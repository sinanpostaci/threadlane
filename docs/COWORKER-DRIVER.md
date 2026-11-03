# Coworker Code Controller Driver (`threadlane-flow code-driver`)

`coworker_driver` (`threadlane-flow code-driver`, alias `coworker-driver`) provides the
independent local process lifecycle and authenticated Unix-domain socket RPC around
`CoworkerControllerService` and physical FD job ownership (`CoworkerJobOwnershipStore` /
`CoworkerJobOwnershipGuard`).

## Subcommands

All subcommands require the trusted CLI scope (`--state-dir`, `--registry-dir`, `--plans-dir`,
`--authority-dir`, `--sessions-dir`, `--binding`, `--thread`) and accept either `--job-id <ID>`
or `--request <ABS_PATH_TO_0600_JSON>`:

- `run-owner`: Acquires the exclusive physical job lock (`CoworkerJobOwnershipGuard`) and
  controller lease (`OwnedCoworkerControllerSession`), verifies authoritative admission
  (planner provenance, `ApprovalRecord`, and `FrozenControllerExecutionEnvelope`) under the held
  guard, reuses an authenticated `Ready` host or starts a fresh host via
  `ApprovedHost::start_fresh_only` when no prior session/launch intent exists, binds a private
  (`0600`) Unix socket under `<plans_dir>/coworker-code-driver`, and drives Phase 1 dispatch and
  receipt reconciliation.
- `ensure`: Verifies and reuses an already active physical owner via the local Unix socket RPC
  handshake (`ReusedLiveOwner`), or validates admission under a short-lived guard and spawns a
  detached `threadlane-flow code-driver run-owner` process (`SpawnedNewOwner`) without duplicate
  process launches or duplicate model calls.
- `status`: Queries a live verified owner over the authenticated Unix socket when held, or
  projects read-only status from scoped workflow, policy, lockfile, and host session state
  without trusting stale crash metadata.
- `reconcile`: Triggers an immediate step on a live verified owner over RPC, or opens a
  short-lived owned session to reconcile existing host receipts without replaying unknown
  effects or restarting stopped/uncertain hosts.
- `cancel`: Persists durable `stop_requested = true` in `ControllerPolicyStore` before any
  local owner RPC or narrow `ApprovedHostClient::cancel_task` call, notifies any live owner to
  cancel active original host tasks and exit cleanly, and reconciles under a short-lived owned
  session.

## Ownership, RPC, and Lifetime Guarantees

1. **Physical FD ownership**: Every authoritative pre-effect check, host startup, and controller
   step requires holding the live `CoworkerJobOwnershipGuard` and verifying `session.verify_admission()`.
   Read-only status inspection uses a narrow, non-authoritative projection and never fabricates
   `VerifiedControllerAdmission`.
2. **Authenticated local Unix RPC**: Client and server verify same-EUID peer credentials via
   `UnixStream::peer_cred()`, fresh 64-hex CSPRNG nonces (`cdn_<64hex>` / `dsn_<64hex>`), and
   live lockfile `(dev, ino)` + `instance_id` + `challenge_digest`. `pid_hint` on
   `CoworkerOwnershipSummary` is diagnostic only and never used as an authority check.
3. **Owner lifetime**: By default (`DEFAULT_DRIVER_MAX_STEPS = None`,
   `DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS = None`), the owner imposes no ad-hoc job duration or
   step limit. It stays alive across long frozen approved worker budgets (`RunningTasks`) while
   preserving active worker output, and keeps its RPC socket and physical ownership available at
   `AwaitingChecksOrReview`, `AwaitingDependencyAcceptanceOrSeal`, and `Held` for later pipeline
   integration. Diagnostic `--max-steps` and `--idle-linger-ms` are strictly opt-in and never
   implicitly cancel or shut down active worker execution.
4. **Phase 1 boundary**: `AwaitingChecksOrReview` is a pipeline checkpoint, not `Completed`.
   Phase 1 dispatches initial `Coder` task attempts and reconciles host receipts to
   `ReadyForReview`; automated checks, reviewer/architect cycles, task/job closure, and running
   derived-attempt cancellation are wired in subsequent slices.
