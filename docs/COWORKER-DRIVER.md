# Coworker Code Controller Driver (`threadlane-flow code-driver`)

`coworker_driver` (`threadlane-flow code-driver`, alias `coworker-driver`) provides the
independent local process lifecycle and authenticated Unix-domain socket RPC around
`CoworkerPipelineService` (`CoworkerPipelineDriver`), `CoworkerControllerService`, and physical FD
job ownership (`CoworkerJobOwnershipStore` / `CoworkerJobOwnershipGuard`).

## Settled attention notices

With verified native notices configured, a settled `BlockedByFailedChecks` or
`AwaitingCorrectionExecution` outcome emits an `Attention` notice before the owner
exits. This includes task/final check failures and corrections blocked by frozen
authority or exhausted budgets. The internal driver path verifies the typed report
against fresh admission, the exact job scope/revision and lease, and the absence of
in-flight stages, active workers, or reserved effects. Running pipeline stages do
not emit these notices. Existing `Complete` eligibility still requires accepted
`Closure`; stop/cancel/draining behavior is unchanged.

The existing private notice store reserves intent before transport, deduplicates
per job/revision, caps attempts at 16, and fences interrupted or unknown possible
sends without automatic replay. Read-only status and duplicate ensure/reconcile
do not create new attempts or provider calls. `Submitted` means transport
acceptance, not reading, acknowledgement, or human approval. Machine wake-ups
contain only the fixed scoped message, never raw worker prose, and never use the
legacy queue as fallback.

## Subcommands

All subcommands require the trusted CLI scope (`--state-dir`, `--registry-dir`, `--plans-dir`,
`--authority-dir`, `--sessions-dir`, `--binding`, `--thread`) and accept either `--job-id <ID>`
or `--request <ABS_PATH_TO_0600_JSON>`:

- `run-owner`: Acquires the exclusive physical job lock (`CoworkerJobOwnershipGuard`) and
  controller/pipeline lease (`OwnedCoworkerControllerSession` / `OwnedCoworkerPipelineSession`),
  verifies authoritative admission (planner provenance, `ApprovalRecord`, and
  `FrozenControllerExecutionEnvelope`) under the held guard, reuses an authenticated `Ready` host
  or starts a fresh host via `ApprovedHost::start_fresh_only` when no prior session/launch intent
  exists, binds a private (`0600`) Unix socket under `<plans_dir>/coworker-code-driver`, and
  drives the selected execution mode (`full_automatic_pipeline` by default when authorized by the
  frozen envelope, or explicit diagnostic `phase1_initial_dispatch_only`).
- `ensure`: Verifies and reuses an already active physical owner via the local Unix socket RPC
  handshake (`ReusedLiveOwner`), returns `AlreadyQuiescentVerified` without starting a host when
  verified persisted evidence shows the job is already terminal/stopped with no unresolved effects,
  or validates admission under a short-lived guard and spawns a detached
  `threadlane-flow code-driver run-owner` process (`SpawnedNewOwner`) without duplicate process
  launches or duplicate model calls.
- `status`: Queries a live verified owner over the authenticated Unix socket when held (returning
  compact cached pipeline/step evidence without triggering new model or check calls), or projects
  read-only status from scoped workflow, policy, lockfile, and host session state without trusting
  stale crash metadata.
- `reconcile`: Triggers an immediate step on a live verified owner over RPC, returns cached
  terminal/stopped status without starting a host when already quiescent, or opens a short-lived
  owned session in the persisted/effective execution mode to reconcile existing receipts and
  pipeline state without replaying unknown effects or restarting stopped/uncertain hosts.
- `cancel`: Persists durable `stop_requested = true` in `ControllerPolicyStore` before any local
  owner RPC, host cancellation, or derived worker signalling, notifies any live owner to stop new
  dispatches, cancel active original host tasks, signal tracked derived `Coder` process groups,
  and drain/fence any finite in-flight check/`Reviewer`/`Closure` futures before releasing the
  guard, and reconciles under a short-lived owned session when no live owner is active.

## Execution Modes and Additive Pipeline Reporting

1. **`full_automatic_pipeline` (default when authorized)**:
   - Selected automatically by `CoworkerCodeDriver::ensure_owner(..., None)` and ordinary
     `threadlane-flow code-driver ensure` / `run-owner` / `reconcile` when the frozen
     `ControllerExecutionEnvelope` authorizes task worktree commits, `RunIntegration`, and
     `Reviewer` / `Closure` role budgets.
   - Creates **one** stateful `CoworkerPipelineDriver` (`FuturesUnordered`) for the entire held
     owner session (`CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure`)
     and retains its in-flight task verification checks, `Reviewer` calls, bounded in-scope
     derived `Coder` corrections, per-task `RunIntegration` + downstream `DependencyInputSeal`
     preparations, and final job aggregation + combined checks + single `Closure` across ticks and
     peer RPC requests.
   - `AllTasksIntegrated` is **not** `Completed`; a job reaches `Completed` only after final job
     aggregation, required combined candidate checks, and a single subject-bound `Closure` verdict
     of `Accepted`.
2. **`phase1_initial_dispatch_only` (explicit diagnostic mode)**:
   - Selected via `--execution-mode phase1_initial_dispatch_only` or `--initial-dispatch-only` (or
     `execution_mode` in the `0600` JSON request file), and automatically selected as a safe
     fallback when a frozen envelope only authorizes Phase 1 `Coder` dispatch without widening
     capabilities.
   - Preserves Phase 1 diagnostic behavior (`CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly`),
     stopping at `AwaitingChecksOrReview` (`ReadyForReview`) without invoking `Reviewer`, derived
     corrections, integration, or `Closure`.
3. **Persisted mode and compact status**:
   - The effective `execution_mode` and compact `last_pipeline_report`
     (`CoworkerDriverPipelineReport`, including `CoworkerDriverCompletionSummary`) are persisted in
     `CoworkerDriverStateRecord` so subsequent `status` and `reconcile` calls preserve the job's
     real mode and completion evidence without resetting approved scope.
   - `CoworkerDriverStatusReport` exposes both legacy Phase 1 `overall_status` / `step_report` and
     additive `pipeline_status` / `pipeline_report` / `stopping_draining_in_flight`.

## Ownership, RPC, Cancellation, and Lifetime Guarantees

1. **Physical FD ownership**: Every authoritative pre-effect check, host startup, and controller or
   pipeline step requires holding the live `CoworkerJobOwnershipGuard` and verifying
   `session.verify_admission()`. Read-only status inspection uses a narrow, non-authoritative
   projection and never fabricates `VerifiedControllerAdmission`.
2. **Authenticated non-blocking local Unix RPC**: Client and server verify same-EUID peer
   credentials via `UnixStream::peer_cred()`, fresh 64-hex CSPRNG nonces (`cdn_<64hex>` /
   `dsn_<64hex>`), and live lockfile `(dev, ino)` + `instance_id` + `challenge_digest`. While
   in-flight `CoworkerPipelineDriver` futures are running, `run-owner` multiplexes peer RPC accepts
   with bounded pipeline ticks (`tokio::select!`) so `status`, duplicate `ensure`, and `cancel`
   remain responsive during long `Reviewer`, check, or `Coder` executions, and `status` returns
   cached compact evidence without model charges.
3. **Owner lifetime**: By default (`DEFAULT_DRIVER_MAX_STEPS = None`,
   `DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS = None`), the owner imposes no ad-hoc 5-minute or 128-tick
   cutoff. In `full_automatic_pipeline` mode it runs until the pipeline reaches a terminal,
   stopped, held, or blocked state with zero in-flight futures, then cleanly shuts down any
   driver-owned disposable host, removes its Unix socket, and releases the physical FD lock.
4. **Truthful cancellation and draining boundary**:
   - `cancel` records durable `stop_requested = true` before any side effect.
   - Active original host tasks are cancelled narrowly via `ApprovedHostClient::cancel_task`, and
     tracked running derived `Coder` correction attempts are signalled via their isolated process
     groups (`signal_running_derived_coder_Execution_for_job`).
   - Direct check subprocesses and headless `Reviewer` / `Closure` calls do not expose mid-flight
     remote cancellation APIs. Rather than dropping their futures or falsely claiming remote
     inference terminated, a live `full_automatic_pipeline` owner holds the physical guard while
     draining those finite frozen-timeout futures (`stopping_draining_in_flight = true`,
     `CoworkerDerivedCancellationBoundary::OriginalAndDerivedSignalledDirectCheckReviewerClosureDrainedFenced`),
     prevents any new stage dispatch, fences late outputs from promoting to receipts or downstream
     stages, and releases the guard and owned host only after in-flight futures reach `0`.
