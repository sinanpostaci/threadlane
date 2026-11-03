# Coworker Per-Job OS File-Lock Ownership (`coworker_workflow::ownership`)

## 1. Purpose and Confirmed Failure Mode Addressed

A logical lease keyed by a deterministic `instance_id`, a stored PID, or a wall-clock lease
timestamp is **not** process ownership: two processes computing or copying the same deterministic
`instance_id` can both believe they own the same Coworker job and renew the same logical lease.

`crates/threadlane-native/src/coworker_workflow/ownership.rs` provides a self-contained, private
per-job OS kernel file-lock prerequisite (`flock` `LOCK_EX | LOCK_NB` via `fs2::FileExt`) that must
be held for the full driver/step/effect lifetime.

## 2. Core Invariants

1. **Non-blocking kernel lock guard (`!Clone`, non-deserializable)**
   - `CoworkerJobOwnershipStore::try_acquire_job` and `try_acquire_job_option` use non-blocking
     `try_lock_exclusive()` and immediately return `CoworkerOwnershipAcquireOutcome::Busy` /
     `Ok(None)` on contention so a Tokio executor is never blocked.
   - `CoworkerJobOwnershipGuard` holds the open `File` descriptor for the entire ownership
     lifetime, does not implement `Clone`, `Serialize`, or `Deserialize`, and cannot be
     reconstructed from `CoworkerOwnershipSummary` or a copied `instance_id`.
2. **Fresh CSPRNG identity and redacted challenge per acquisition**
   - Every successful acquisition mints a new CSPRNG `instance_id` (`cwo_<64hex>`) and a private
     CSPRNG `challenge_secret` (`cwc_<64hex>`) using `fs_safety::generate_prefixed_random_id`.
   - Only the 64-hex SHA-256 `challenge_digest` is persisted in `CoworkerOwnershipSummary`.
     `CoworkerJobOwnershipGuard` and `CoworkerDriverOwnershipClaim` redact `challenge_secret` as
     `"[REDACTED]"` in `Debug`.
   - `pid_hint` is recorded for diagnostics only and is never trusted for ownership or used for
     process signaling/killing.
3. **Private `0700`/`0600` filesystem and inode continuity**
   - Lock root directories must be canonical, existing, non-symlink `0700` directories owned by the
     current effective UID (`rustix::process::geteuid()`).
   - Per-job lockfiles (`coworker-job-ownership-<32hex_tag>.lock`) are opened with
     `O_NOFOLLOW | O_CLOEXEC` and verified before and after locking to be regular, non-symlink
     `0600` files owned by the current effective UID with `nlink == 1` and matching `lstat`/`fstat`
     `(dev, ino)` identity (`CoworkerLockFileIdentity`).
   - Lockfiles are updated in-place under the exclusive fd lock and are **never** unlinked or
     recreated on release or crash recovery; kernel lock release on guard drop or process exit is
     what permits reacquisition with a new CSPRNG identity.
   - Read-only inspection (`CoworkerJobOwnershipStore::inspect_job_lock` and `lock_path_for_job`)
     never creates directories or files, modifies lockfile bytes, acknowledges events, or
     dispatches work.

## 3. Public API Summary

- **Constants**: `COWORKER_JOB_OWNERSHIP_SCHEMA_VERSION` (`1`),
  `MAX_COWORKER_OWNERSHIP_METADATA_BYTES` (`16 KiB`),
  `OWNERSHIP_GRANTS_NO_EXECUTION_AUTHORITY` (`true`),
  `OWNERSHIP_REQUIRES_RECEIPT_RECONCILIATION_BEFORE_INFERENCE` (`true`),
  `COWORKER_OWNERSHIP_INSTANCE_PREFIX` (`"cwo"`),
  `COWORKER_OWNERSHIP_CHALLENGE_PREFIX` (`"cwc"`).
- **Types**:
  - `CoworkerJobOwnershipKey { binding_id, thread_uuid, job_id }`: `new`, `validate`, `tag`,
    `lock_filename`.
  - `CoworkerLockFileIdentity { dev, ino }`.
  - `CoworkerOwnershipSummary`: `key`, `validate`, `validate_for_key`.
  - `CoworkerJobLockState`: `Absent`, `HeldByActiveProcess`, `UnlockedStaleFile`.
  - `CoworkerDriverOwnershipClaim`: `key`, `instance_id`, `challenge_digest`, `lock_file_identity`.
  - `CoworkerJobOwnershipGuard`: `key`, `binding_id`, `thread_uuid`, `job_id`, `instance_id`,
    `challenge_digest`, `lock_root_dir`, `lock_root_identity`, `lock_path`, `lock_file_identity`,
    `summary`, `issue_driver_claim`, `verify_challenge`, `validate_before_effect`,
    `validate_claim_before_effect`, `validate_driver_claim`, `validate_summary_before_effect`.
  - `CoworkerOwnershipAcquireOutcome`: `Acquired(CoworkerJobOwnershipGuard)`,
    `Busy { observed_holder }`, `into_guard`, `is_acquired`, `is_busy`.
  - `CoworkerJobOwnershipStore`: `new`, `lock_root_dir`, `lock_root_identity`,
    `lock_path_for_job`, `inspect_job_lock`, `try_acquire_job`, `try_acquire_job_option`,
    `try_acquire_job_with_copied_summary`, `verify_live_guard`, `verify_live_claim`.

## 4. Explicit Architectural Boundaries

- **No execution or approval authority**: Job ownership proves only that the holding process holds
  the local per-job kernel file lock. It grants no code write, model selection, `PlanningAdviceGrant`,
  `ApprovalRecord`, or `FrozenControllerCapabilityEnvelope` authority.
- **Receipt reconciliation remains mandatory**: Kernel lock reacquisition after a crash does **not**
  provide exactly-once remote inference; `EffectUnknown` outbox and reservation receipt
  reconciliation remains mandatory in the controller before restarting inference.
- **No transport, scheduler, or PID signaling here**: Driver challenge/reconnect transport,
  scheduler ticks, MCP/CLI wiring, and host supervision belong to subsequent integration phases.
- **Platform support**: Targets Unix (`#[cfg(unix)]`, intended for Linux and macOS) and fails closed
  with `NativeErrorCode::UnsupportedPlatform` on non-Unix targets. Only Linux execution is validated
  in this workspace; macOS and Windows execution are not claimed tested.
