# Coworker Frozen Check Execution (`bounded_command` & `coworker_workflow::checks`)

This document describes the bounded verification check execution primitive in
`threadlane-workers` and the worktree-scoped frozen check runner in
`threadlane-native`.

## Scope and Service Boundary

- **Internal trusted execution primitive**:
  `threadlane_native::coworker_workflow::checks` (`CHECK_RUNNER_IS_INTERNAL_TRUSTED_PRIMITIVE = true`)
  executes a pre-validated `FrozenCheckCommandSpec` inside a software-owned
  `Task` or `RunIntegration` Git worktree. It is not a standalone public
  authority endpoint and does not implement the full automatic Coworker
  controller/scheduler loop.
- **Ownership guard is liveness, not execution permission**:
  `run_frozen_check`, `run_frozen_check_with_cancel`, and
  `run_frozen_check_with_claim` require a live `&CoworkerJobOwnershipGuard`
  held across the entire `.await` for `(binding_id, thread_uuid, job_id)`.
  Holding the kernel file-lock guard prevents concurrent drivers for the same
  job, but grants no approval, model, or check execution authority
  (`HOLDING_OWNERSHIP_GUARD_ALONE_GRANTS_NO_CHECK_EXECUTION_AUTHORITY = true`).
- **No duplicate ledger**:
  `checks.rs` does not create a separate state store. The caller/controller
  persists `FrozenCheckExecutionReport` and promoted `CandidateCheckEvidence`
  under its existing transactional runtime ledger.

## Worker Primitive (`threadlane_workers::bounded_command`)

- **Types**: `BoundedCommandBudgets`, `BoundedCommandSpec`,
  `BoundedCommandOverflow`, `BoundedCommandStatus`, `BoundedCommandReport`.
- **Entry points**: `run_bounded_command`, `run_bounded_command_with_cancel`.
- **Invariants**:
  - Invokes the canonical non-symlink `executable` directly with exact `argv`
    entries (`stdin` closed). Never invokes a shell; metacharacters remain
    inert literals.
  - Strips inherited `GIT_*` environment variables and applies
    `SANITIZED_GIT_ENV_OVERRIDES` when `strip_git_env` is `true`.
  - Enforces `timeout_ms` (`1..=MAX_TIMEOUT_MS`), `max_stdout_bytes`
    (`1..=MAX_STDOUT_BYTES`), and `max_stderr_bytes` (`1..=MAX_STDERR_BYTES`)
    to the exact millisecond and byte without upward `4096`-byte clamping.
  - Spawns in an isolated Unix process group (`process_group(0)`), holds an
    RAII `ProcessGroupGuard`, reaps lingering descendants after direct child
    exit (`descendants_terminated_after_exit`), and bounds post-exit pipe
    drainage by `POST_EXIT_PIPE_DRAIN_TIMEOUT_MS` (`IncompleteCleanup`).
  - When a direct child exits `0` (`exit_code == Some(0)`) and physical cleanup
    completes (`cleanup_incomplete == false`), but surviving descendant
    processes had to be forcibly terminated after direct child exit
    (`descendants_terminated_after_exit == true`), the report classifies as
    `BoundedCommandStatus::DescendantsTerminatedAfterExit` (`passed() == false`)
    because natural completion of child check work was not proven. Naturally
    waited child subprocesses that finish before parent exit remain `Succeeded`.
  - Cancellation (`cancel_rx`) and timeout signal and reap only the owned
    process group created for that child handle; they never signal arbitrary
    stored PIDs and make no claim that external or remote side effects were
    undone.

## Native Frozen Check Runner (`threadlane_native::coworker_workflow::checks`)

- **Types**: `FrozenCheckTargetKind`, `FrozenCheckSubject`,
  `FrozenCheckExecutionStatus`, `FrozenCheckExecutionReport`.
- **Entry points**:
  - `run_frozen_check`, `run_frozen_check_with_cancel`,
    `run_frozen_check_with_claim`
  - `FrozenCheckExecutionReport::validate_integrity`,
    `FrozenCheckExecutionReport::verify_against_context`,
    `FrozenCheckExecutionReport::to_candidate_check_evidence`
  - `verify_candidate_check_evidence`, `verify_and_promote_check_reports`
- **Pre-effect checks (before spawning)**:
  - Validates the live `CoworkerJobOwnershipGuard` (and optional
    `CoworkerDriverOwnershipClaim` / `CoworkerOwnershipSummary`) for the
    subject's `(binding_id, thread_uuid, job_id)`.
  - Validates `FrozenCheckCommandSpec` and its applicability to
    `FrozenCheckTargetKind` (`TaskWorktree` requires
    `required_for_task_review`; `RunIntegrationWorktree` requires
    `required_for_candidate_closure`).
  - Loads and verifies the private (`0600`) `WorktreeManifest` via
    `load_and_verify_manifest`, rejecting primary/unowned checkouts, active
    worktree/model jobs (`ensure_worktree_job_inactive_readonly`), detached or
    mismatched branches, dirty worktrees (`is_worktree_clean`), mismatched
    `expected_base_commit` / `expected_head_commit` / `expected_tree_sha`, and
    protected or symlink-traversing `repo_relative_cwd` paths.
- **Post-run revalidation and evidence promotion**:
  - Revalidates the live ownership guard and worktree manifest, branch,
    `HEAD` commit, `HEAD^{tree}`, `repo_relative_cwd`, and working-tree
    cleanliness after the command exits.
  - If the check command mutates the worktree or context drifts, all files on
    disk and captured stdout/stderr outputs are preserved without `git reset`
    or `git clean`, the report records `SubjectMutatedOrDrifted` (or
    `OwnershipOrContextDrifted`), and promotion to `CandidateCheckEvidence` is
    refused.
  - `CandidateCheckEvidence` is produced only from a report with status
    `Passed` (`descendants_terminated_after_exit == false`,
    `cleanup_incomplete == false`) whose `evidence_digest` commits to the
    canonical subject digest, `spec_digest`, ownership identity, exit status,
    `stdout_sha256`, `stderr_sha256`, and post-run clean state. If surviving
    descendants had to be force-killed after direct child exit `0`, the report
    preserves `exit_code == Some(0)` and `cleanup_incomplete == false`, records
    `FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit`, refuses
    promotion to `CandidateCheckEvidence`, and rejects tampered serde records
    pairing `Passed` with `descendants_terminated_after_exit == true`. Model
    prose or caller-supplied digests cannot pass
    `verify_candidate_check_evidence`.

## Platform Support and Remaining Coverage

- Validated on **Linux** only; macOS is untested and Windows is unsupported.
- When `allow_check_failure_corrections` is explicitly enabled (`true`) in both
  `FrozenControllerCapabilityEnvelope` and `FrozenControllerExecutionEnvelope`,
  `CoworkerTaskCorrectionService::prepare_task_correction` loads and verifies
  the persisted private (`0600`) `FrozenCheckExecutionReport` artifacts from a
  `BlockedByFailedChecks` task verification pass, requires clean natural
  `NonZeroExit` status (with complete process cleanup, no signal, no output
  overflow, no forced descendants, clean worktree, and matching committed
  `HEAD`/tree), records an unreviewed `TaskCandidateRef` (`review_verdict: None`),
  and reserves a derived `Coder` correction attempt (`DerivedTaskAttemptReceipt`
  with `prior_check_failure_basis: Some(...)` and `prior_review_verdict_id: None`)
  sharing the same hard `<= 2` per-task and job-wide correction budgets without
  pretending a `Reviewer` ran. Actual worker execution of a check-failure-derived
  correction reservation remains an explicit subsequent slice (`execute_task_correction`
  returns `UnsupportedCapability` for that basis).
