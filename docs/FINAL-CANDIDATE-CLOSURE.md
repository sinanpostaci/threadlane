# Final Combined Candidate Closure Evidence (`threadlane-operations::candidate`)

## 1. Purpose and Architectural Boundary

In the Threadlane Coworker workflow, individual implementation tasks execute inside isolated `WorktreeKind::Task` checkouts, are reviewed and accepted into frozen `ReviewReceipt`s, and are integrated into an owned `WorktreeKind::RunIntegration` checkout (`<work_area_root>/checkouts/runs/<integration_worktree_id>`) with `IntegrationReceipt`s.

Previously, `RoleFlowStore::reserve_closure` in `threadlane-native` only supported `ClosureSubjectSource::ApprovedCycleBase`, which resolves `approved_base_commit^{tree}` and binds `approval_subject_digest`. An `ApprovedCycleBase` reservation proves the initial base tree of the cycle, **not** the final combined candidate tree produced after integrating accepted tasks.

`threadlane-operations::candidate` closes that gap without altering historical receipts or claiming `ApprovedCycleBase` verifies a final candidate:

1. **Read-Only `RunIntegration` Candidate Inspection & Freeze (`inspect_final_candidate_readonly`)**:
   - Inspects the **actual owned `WorktreeKind::RunIntegration` checkout** on disk.
   - Verifies work-area ownership (`.threadlane-work-area.json`), manifest integrity, `threadlane/run/<run_id>/...` branch binding, inactive writer state (`ensure_worktree_job_inactive_readonly` without creating missing `.lock` files), and a clean working tree (`is_worktree_clean`).
   - Reconstructs each selected predecessor task's live `ReviewSubject` via `inspect_review_subject_readonly` (never trusting caller DTOs), verifies its persisted `ReviewReceipt` and `IntegrationReceipt`, ensures neither receipt is superseded, and verifies commit ancestry (`approved_base_commit <= task_base_commit <= accepted_task_head_commit <= integrated_target_commit <= candidate_head_commit`).
   - Verifies that the latest `IntegrationReceipt` targeting the `RunIntegration` worktree matches the live `candidate_head_commit` and `candidate_tree_sha`, and that no persisted `IntegrationReceipt` for the `RunIntegration` worktree was omitted from `predecessor_tasks` (rejecting unreviewed extra commits on the integration branch).
   - Computes and resolves the aggregate allowed write scope (`aggregate_allowed_write_paths`) at `approved_base_commit` via `resolve_allowed_write_scopes_at_base`, and validates all changed paths in `approved_base_commit..candidate_head_commit` against symlink, control-config, path-escape, write-scope, and predecessor-attribution gates.
   - Verifies before and after inspection that the primary source repository `HEAD` and working tree status remain completely untouched.
   - Binds a deterministic 64-hex SHA-256 `subject_digest` over the canonical repository path, `approved_base_commit`, `approved_base_tree_sha`, `candidate_head_commit`, `candidate_tree_sha`, `candidate_diff_digest`, sorted `aggregate_allowed_write_paths`, sorted `changed_paths`, sorted `predecessors` (`by task_id`), `FinalCandidatePolicyBinding`, and `TestEvidenceSummary`.

2. **Bounded Private Cross-Process Locked CAS Closure Ledger (`FinalCandidateClosureStore`)**:
   - Persists closure attempts in `<work_area_root>/manifests/closures/final-candidate-closures.json` (`0600`) protected by `<work_area_root>/manifests/closures/final-candidate-closures.lock` (`0600`) and bounded by `MAX_CLOSURE_LEDGER_BYTES` (`256 KiB`) and `MAX_CLOSURE_ATTEMPTS` (`128`).
   - Tracks explicit lifecycle states (`ClosureAttemptState`):
     - `Reserved`: durable attempt reserved before external closure evaluation is invoked.
     - `Submitted`: external closure evaluation marked in-flight (`external_request_id` bound).
     - `Terminal`: completed with a structured `ClosureVerdict` (`Accepted`, `Rejected`, `Blocked`, `Unclear`).
     - `OutcomeUnknown`: transport, provider, or persistence outcome uncertain; blocks any replay or new attempt across all `command_key`s until explicit receipt-bound reconciliation (`reconcile_closure_attempt`).
   - Returns cache reuse as an explicit decision (`ClosureReservationOutcome::CacheHit`) when an identical live `FinalCandidateSubject` already has a verified `ClosureVerdictKind::Accepted` verdict, avoiding duplicate closure dispatches while guaranteeing that any change to the candidate tree, write scope, policy/scope revision, check evidence, or predecessor receipts produces a new `subject_digest` and never inherits an old verdict.
   - Enforces that a positive verdict (`ClosureVerdictKind::Accepted`) binds actual `ClosureExecutionEvidence` (`grants_merge_or_push_authority: false`, `substitutes_for_required_checks: false`) and passing candidate check evidence.

---

## 2. Exported API Signatures (`threadlane-operations`)

All types, constants, and functions below are re-exported at the `threadlane_operations` crate root and under `threadlane_operations::candidate`:

```rust
pub const FINAL_CANDIDATE_SUBJECT_SCHEMA_VERSION: u32 = 1;
pub const FINAL_CANDIDATE_CLOSURE_SCHEMA_VERSION: u32 = 1;
pub const CLOSURES_SUBDIR: &str = "closures";
pub const CLOSURE_LEDGER_FILENAME: &str = "final-candidate-closures.json";
pub const CLOSURE_LEDGER_LOCK_FILENAME: &str = "final-candidate-closures.lock";
pub const MAX_CLOSURE_LEDGER_BYTES: u64 = 262_144;
pub const MAX_CLOSURE_ATTEMPTS: usize = 128;

pub fn inspect_final_candidate_readonly(
    request: &InspectFinalCandidateRequest,
) -> Result<FinalCandidateSubject, OperationsError>;

pub fn verify_final_candidate_subject_readonly(
    subject: &FinalCandidateSubject,
) -> Result<(), OperationsError>;

pub fn verify_legacy_base_closure_insufficient(
    legacy_base_digest: &str,
    subject: &FinalCandidateSubject,
) -> Result<(), OperationsError>;

impl FinalCandidateClosureStore {
    pub fn open(
        repository_path: &Path,
        work_area_root: &Path,
    ) -> Result<Self, OperationsError>;

    pub fn inspect_ledger_readonly(
        &self,
    ) -> Result<FinalCandidateClosureLedger, OperationsError>;

    pub fn reserve_closure_attempt(
        &self,
        request: &ReserveClosureAttemptRequest,
    ) -> Result<ClosureReservationOutcome, OperationsError>;

    pub fn mark_closure_attempt_submitted(
        &self,
        request: &MarkClosureSubmittedRequest,
    ) -> Result<(u64, ClosureAttemptRecord), OperationsError>;

    pub fn mark_closure_attempt_unknown(
        &self,
        request: &MarkClosureUnknownRequest,
    ) -> Result<(u64, ClosureAttemptRecord), OperationsError>;

    pub fn record_closure_verdict(
        &self,
        request: &RecordClosureVerdictRequest,
    ) -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError>;

    pub fn reconcile_closure_attempt(
        &self,
        request: &ReconcileClosureAttemptRequest,
    ) -> Result<(u64, ClosureAttemptRecord, ClosureVerdict), OperationsError>;

    pub fn verify_accepted_closure_verdict_readonly(
        &self,
        subject: &FinalCandidateSubject,
        verdict: &ClosureVerdict,
    ) -> Result<ClosureAttemptRecord, OperationsError>;
}
```

---

## 3. Native Controller Integration Contract

When wiring final-candidate closure into the native workflow controller (`threadlane-native` / `threadlane-operator`):

1. **Freeze the Live Combined Candidate**:
   - Call `inspect_final_candidate_readonly(&InspectFinalCandidateRequest { ... })` pointing at the owned `WorktreeKind::RunIntegration` worktree and the selected accepted/integrated task receipts.
2. **Reserve or Reuse Closure Attempt Before External Dispatch**:
   - Open `FinalCandidateClosureStore::open(&repository_path, &work_area_root)`.
   - Call `store.reserve_closure_attempt(&ReserveClosureAttemptRequest { expected_ledger_revision, command_key, attempt_id, subject, legacy_base_tree_digest, reserved_at_ms })`.
   - Handle `ClosureReservationOutcome`:
     - `ClosureReservationOutcome::CacheHit { ledger_revision, attempt, verdict }`: reuse `verdict` directly; **do not** invoke an external closure model/provider.
     - `ClosureReservationOutcome::IdempotentReserved { ledger_revision, attempt }` or `ClosureReservationOutcome::Reserved { ledger_revision, attempt }`: proceed to step 3.
3. **Persist `Submitted` Before Invoking External Inference**:
   - Call `store.mark_closure_attempt_submitted(&MarkClosureSubmittedRequest { expected_ledger_revision, attempt_id, command_key, expected_subject_digest, external_request_id, submitted_at_ms })`.
   - If this write fails, **do not** dispatch the external provider call.
4. **Finalize or Quarantine Outcome**:
   - If external execution completes with verified telemetry and a durable handoff/turn receipt, call `store.record_closure_verdict(&RecordClosureVerdictRequest { ... })`.
   - If external transport, timeout, or receipt persistence leaves the outcome uncertain, call `store.mark_closure_attempt_unknown(&MarkClosureUnknownRequest { ... })` and require explicit receipt-bound `store.reconcile_closure_attempt(&ReconcileClosureAttemptRequest { ... })` before any retry.
5. **Respect Authority Boundaries**:
   - A `ClosureVerdictKind::Accepted` verdict proves final-candidate closure review on `subject_digest`; it never grants merge/push authority and never substitutes for required formatting, Clippy, or behavioral test checks.
