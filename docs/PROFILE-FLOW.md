# Profile Selection, Execution Policy, and Durable Role-Flow Handoffs

`crates/threadlane-native/src/profile_flow.rs` (Phase A1) and
`crates/threadlane-native/src/role_flow.rs` (Phase A2) implement Threadlane's
profile-driven role admission, durable workflow cycles, native vs. headless role
handoffs, explicit unknown-outcome reconciliation, and single-closure package
reservations.

For the trusted-local operator CLI wrapper (`threadlane-flow`), see
[`docs/ROLE-FLOW-CLI.md`](ROLE-FLOW-CLI.md).

## Architecture Overview

- **Phase A1 (`profile_flow`) — Pure Admission & Preflight**:
  - Generic deny-by-default [`ExecutionPolicy`] (`schema_version = 1`,
    `policy_id`, and `allow_rules: Vec<PolicyAllowRule>`) with structural
    validation ([`ExecutionPolicy::validate`]) and canonical 64-character
    lowercase hex SHA-256 digest computation ([`ExecutionPolicy::digest`]).
  - Deterministic role/difficulty choice resolution ([`resolve_choice`]) that
    checks an exact `(role, Some(difficulty))` match first and falls back only
    to an explicit role default `(role, None)` without substituting another
    role, difficulty, or unapproved fallback model.
  - Pure, lock-free read-only preflight over a [`RegistrySnapshot`]
    ([`preflight`]), an existing [`RegistryStore`] ([`preflight_from_store`]),
    or a private registry directory ([`preflight_from_dir_readonly`]) via
    `RegistryStore::read_snapshot_readonly_at`.
- **Phase A2 (`role_flow`) — Durable Cycles, Handoffs, Reconciliation & Closure**:
  - Per-binding private (`0600`) role-flow ledgers managed by [`RoleFlowStore`]
    inside an existing private (`0700`) `plans_dir`, protected by cross-process
    file locks (`role-flow-<binding_tag>.lock`), monotonic ledger/cycle
    revisions, and caller `request_key` idempotency.
  - [`RoleFlowStore::create_cycle`] freezes a selected subset of tasks from an
    existing [`ApprovalRecord`] after verifying binding/thread scope, active
    published plan status, canonical repository and base commit/tree SHA,
    registry revision equality, unresolved conversational-change gates, and A1
    preflight admission (deduplicating shared `(role, difficulty)` requests
    across tasks).
  - [`RoleFlowStore::handoff_role`] records a native conversational handoff or
    dispatches a headless worker task through an already-running
    [`ApprovedHostClient`], persisting [`HandoffStatus::IntentRecorded`] before
    crossing the host RPC boundary.
  - [`RoleFlowStore::reconcile_handoff`] inspects [`ApprovedHostClient::get_task`]
    without ever invoking `dispatch_task` or repeating a provider call.
  - [`RoleFlowStore::reserve_closure`] enforces a single closure reservation per
    `(repository_path, candidate_tree_sha)` across cycles in a
    binding.

## Core Invariants

1. **Explicit or real default profile selection only**:
   - When `PreflightRequest::profile_id` is `Some(id)`, preflight selects that
     exact profile ID.
   - When `PreflightRequest::profile_id` is `None`, preflight uses
     `RegistrySnapshot::default_profile_id`. It **never** infers a default from
     a profile whose `id` or `name` happens to be `"default"`. If
     `default_profile_id` is `None`, requested roles report
     `PreflightBlockReason::MissingDefaultProfile`.
2. **Requested roles only & registry-sourced model/effort**:
   - Preflight resolves only the `(role, difficulty)` pairs requested; unrequested
     roles on the same profile never block admission.
   - `SelectedRoleChoice::model_arg` and `SelectedRoleChoice::effort` come
     exclusively from the resolved registry [`ModelDefinition`] and profile
     [`RoleAssignment`]. `RolePreflightRequest` accepts only `role` and optional
     `difficulty` (`#[serde(deny_unknown_fields)]`).
   - For Antigravity and OpenCode v2 executable worker routes, admission runs
     `threadlane_workers::resolve_model_target` over the registry model argument
     and effort. Antigravity requires an explicit effort even when registry
     metadata sets `allow_no_effort`, and an OpenCode inline `#variant` becomes
     the effective effort evaluated against policy. Missing or malformed worker
     targets report `PreflightBlockReason::UnsupportedEffort` before policy
     evaluation and before any model invocation.
3. **Immutable deny-by-default execution policy**:
   - Empty `allow_rules` is a valid policy that denies every role with
     `PreflightBlockReason::PolicyBlocked`.
   - Each [`PolicyAllowRule`] specifies an exact `(tool_id, model_id)` pair,
     allowed `efforts`, explicit `allow_no_effort`, and optional `roles`
     restrictions (`roles: []` applies to all roles).
   - Cycles freeze both the [`ExecutionPolicy`] and its SHA-256 `policy_digest`.
     Any policy or registry drift between cycle creation and `handoff_role`
     is refused before effect.
4. **Native route vs. explicit read-only advisory normalization and executed evidence**:
   - Generic `RoleExecutionRoute::from_adapter(ToolAdapter::CodexCli)` and manual role-flow
     handoffs (`RoleFlowStore::create_cycle` / `handoff_role`) map `ToolAdapter::CodexCli` to
     `RoleExecutionRoute::NativeHandoff` and never dispatch a headless worker on the manual
     role-flow path. The native Coworker remains the human-selected Codex conversation, has no
     model profile slot or lock, and is never a worker dispatch target.
   - [`NativeRoleHandoff`] records the frozen instruction, `requested_model_id`,
     `requested_model_arg`, and `requested_effort` with
     `model_execution_verified: false` and
     `native_per_call_identity_verified: false`. It is a manual native routing
     record, never proof that native Codex executed with that model or effort.
   - `ToolAdapter::Antigravity` and `ToolAdapter::OpencodeV2` map directly to
     `RoleExecutionRoute::HeadlessWorker`. [`RoleEvidenceReference`] records
     actual runtime `run_id`, `worker_id`, `task_worktree_id`,
     `report_artifact_id`, `base_commit`, `head_commit`, `observed_model`, and
     `observed_effort` directly from [`TaskStateSnapshot`], with
     `final_summary_is_complete_evidence: false`.
   - Separate from generic `from_adapter` and manual `RoleFlowStore` handoffs, explicit read-only
     advisory normalization (`normalize_read_only_advisory_choice` /
     `is_read_only_advisory_worker_role`, with binary resolution via
     `resolve_advisory_tool_executable`) runs **after** normal exact `RegistrySnapshot` +
     [`ExecutionPolicy`] preflight succeeds and converts a `ToolAdapter::CodexCli` choice into
     `RoleExecutionRoute::HeadlessWorker` **only** for read-only advisory roles
     (`WorkflowRole::Orchestrator`, `WorkflowRole::Architect`, `WorkflowRole::Reviewer`, and
     `WorkflowRole::Closure`) when an explicit reasoning effort is configured and validated by
     `threadlane_workers::resolve_model_target(WorkerProvider::CodexCli, ...)`.
   - The approved Coworker controller path separately uses `normalize_coder_choice` after exact
     role/policy preflight for the user-facing **Worker** execution role (canonical
     `WorkflowRole::Coder`, wire/storage identifier `"coder"`). User-facing surfaces call this
     role `Worker` because Threadlane can coordinate more than software coding tasks, while
     canonical `WorkflowRole::Coder` (`"coder"`), persisted profile/approval keys, function names
     (`normalize_coder_choice`), and v1 digests are retained for compatibility. Renaming the
     display label does not add arbitrary non-repository jobs: tasks remain bounded to a canonical
     Git repository, base commit, and finite repository-relative write scopes. New Codex Worker
     (`coder`) approvals freeze provider, canonical executable, model/effort, `standalone=false`,
     TaskFiles permissions and finite write scopes into their subject digest. The host accepts only
     these complete frozen contracts; persisted `provider=None` native records retain their
     original digest and manual semantics. `WorkflowRole::Follower` remains native. Saved profiles
     or catalog entries never grant authority without an explicit `ExecutionPolicy` and the
     required `PlanningAdviceGrant` or `FrozenControllerCapabilityEnvelope` gate.
   - Because Linux `codex exec --json` does not emit runtime model or effort telemetry on stdout,
     `CodexCli` Worker (`coder`), review and closure evidence records the verified pinned launch
     configuration explicitly and separately from runtime-observed stream telemetry: absent
     observed model/effort remains `None` with no `requested -> observed` fallback, while legacy
     `Antigravity` and `OpencodeV2` runtime telemetry requirements remain strict. See
     [`docs/CODEX-ADVISORY.md`](CODEX-ADVISORY.md) and [`docs/CODEX-CODER.md`](CODEX-CODER.md).
     Worker (`coder`) execution retains an immutable private source report and a compact sanitized
     launch proof with its digest. The original/dependent task and derived correction paths verify
     the exact launch against frozen authority and owned worktree scope. Passing checks, Reviewer
     receipts, integration and one Closure remain mandatory; a launch pin never accepts work.
5. **Explicit approvals and separately started hosts**:
   - Creating a cycle requires an existing frozen [`ApprovalRecord`] from
     `threadlane-approve`.
   - Headless worker handoffs require `operator_approved = true`, an exact
     `confirm_approval_digest` matching `approval.subject_digest`, and a scoped
     [`ApprovedHostClient`] connected to an already-running `threadlane-host`
     session. Role-flow APIs and CLIs never auto-approve plans or auto-start
     hosts.
6. **Single closure reservation per `(repository_path, candidate_tree_sha)`**:
   - Only `ClosureSubjectSource::ApprovedCycleBase` is supported: the candidate
     tree SHA is resolved via sanitized read-only Git from the cycle's frozen
     approved `base_commit`, and `subject_digest` must equal the cycle's
     `approval_subject_digest`.
   - `WorkflowRole::Closure` handoffs require a matching `closure_reservation_id`
     bound to that exact cycle, approval, repository, base commit, and closure
     task. Missing, foreign, or already-spent reservations are refused before
     recording a native handoff or calling a provider.
   - Re-reserving the same `(repository_path, candidate_tree_sha)`
     with the same `subject_digest` across cycles returns the existing
     reservation; a different `subject_digest` for the same tree conflicts; a
     changed candidate tree requires a new commit, fresh approval, and new cycle.
7. **Uncertain outcomes and explicit reconciliation**:
   - Headless handoffs persist [`HandoffStatus::IntentRecorded`] before calling
     `ApprovedHostClient::dispatch_task`, and transition transport/host errors
     to [`HandoffStatus::OutcomeUnknown`].
   - Ambiguous handoffs (`IntentRecorded` or `OutcomeUnknown`) block blind
     replay with `NativeErrorCode::AmbiguousWakeupPendingReconciliation`.
   - [`RoleFlowStore::reconcile_handoff`] queries `ApprovedHostClient::get_task`
     without dispatching another run. If a sealed run receipt (`run_id` and
     `worker_id`) exists on the host, the handoff recovers to the observed
     status; if the host reports no sealed run (`NotStarted` or `Unknown`), the
     handoff remains `OutcomeUnknown`.
8. **Typed conversation-change gate**:
   - Read-only `question_or_status` observations (`recorded_only`) never revise
     plans, cancel tasks, or block cycle creation or role handoffs.
   - Any unresolved `pending_clarification` or `pending_replan` record (or a
     durably superseded approval) blocks `create_cycle`, `handoff_role`, and
     `reserve_closure` for the affected approval/task scope until resolved.
     Frozen approved prompts and write scopes are never silently reused after a
     scope or model change.

## Honest Limitations

- Native handoff records (`NativeRoleHandoff`) do not switch the active native
  Codex model or prove per-call native chat identity.
- Current headless worker providers (`antigravity`, `opencode_v2`) have no
  verified live in-flight input, pause, or resume channel (`live_input_delivered:
  false`, `process_paused: false`, `provider_resumed: false`).
- `final_summary_preview` in [`RoleEvidenceReference`] is bounded preview text
  and is never complete review evidence; full review and integration still
  require inspecting the task worktree diff and report artifact.
- Linux (`0700`/`0600` private permissions) is the supported and tested
  platform; non-Unix platforms fail closed.

## Public Rust API (`threadlane_native::profile_flow` & `role_flow`)

- **Phase A1 (`profile_flow` / `role_flow` preflight)**:
  - `EXECUTION_POLICY_SCHEMA_VERSION`, `MAX_POLICY_ALLOW_RULES`, `MAX_PREFLIGHT_ROLES`
  - `PolicyAllowRule`, `ExecutionPolicy`, `RoleExecutionRoute`, `PreflightBlockReason`
  - `RolePreflightRequest`, `PreflightRequest`, `SelectedRoleChoice`, `RolePreflightOutcome`, `PreflightReport`
  - `resolve_choice`, `preflight`, `preflight_from_store`, `read_registry_snapshot_readonly`, `preflight_from_dir_readonly`
  - `is_read_only_advisory_worker_role`, `normalize_read_only_advisory_choice`, `resolve_advisory_tool_executable`
- **Phase A2 (`role_flow`)**:
  - `ROLE_FLOW_SCHEMA_VERSION`, `MAX_CYCLES_PER_BINDING`, `MAX_HANDOFFS_PER_CYCLE`, `MAX_CLOSURES_PER_BINDING`, `MAX_ROLE_FLOW_LEDGER_BYTES`
  - `RoleFlowStore`, `RoleFlowBindingSnapshot`, `RoleFlowCycle`, `FrozenCycleTask`
  - `CreateRoleFlowCycleRequest`, `RoleHandoffRequest`, `ReconcileRoleHandoffRequest`, `ReservePackageClosureRequest`
  - `RoleHandoffRecord`, `HandoffStatus`, `NativeRoleHandoff`, `RoleEvidenceReference`
  - `PackageClosureReservation`, `ClosureSubjectSource` (`ApprovedCycleBase`)

## Boundary hardening

- `package_id` is a display/correlation label. Renaming it cannot mint closure
  authority for the same repository and candidate tree. A reservation requires
  an actual frozen Closure task; legacy taskless reservations cannot be spent.
- Handoffs are unique for an approval/task across cycles in the binding. An
  uncertain handoff in another cycle blocks new routing; creating a cycle does
  not reset execution history.
- Native tasks with `depends_on` fail closed as unsupported until accepted
  predecessor input verification exists for that route. Use a separately
  approved base candidate for a manual native review or closing phase.
- Handoff advancement holds the actual conversation-change mutation lock
  through the host reply and durable finalization. A scope/model change and
  dispatch therefore have a defined order instead of a check/start race.
- Only `HostNotReady` and `DependencyInputsUnverified`, which the host guarantees
  before worker effect, become `refused_before_effect`. Exact retries read that
  recorded refusal; a new explicit request key, current cycle revision and
  fresh authorization checks may retry. Previous refusal history is retained.
  Transport uncertainty and `MutationOutcomeUncertain` remain unreplayable;
  observing `NotStarted` without a sealed run never clears an unknown outcome.
- Reconciliation validates approval, task, thread/binding, task digest and sealed
  run/worker continuity before storing evidence. Execution readiness remains
  separate from `task_snapshot.lifecycle` review/integration validity.

A sealed receipt without its corresponding worker snapshot is insufficient for
reconciliation. The handoff remains unresolved until matching worker evidence
becomes available; missing telemetry is never converted into completion.

## Codex Worker (`coder`) acceptance boundary

The frozen controller path supports explicitly selected Codex CLI models and reasoning efforts
(for example `gpt-6-luna` / `max`) for original Worker (`coder`) tasks, dependency-bound tasks and
authorized in-scope corrections. This does not change the native Coworker model, installed profiles
or manual role-flow handoff semantics. Future-call profile adoption is a separate follow-up.

Linux offline fixtures exercise the full pipeline with deterministic subprocesses, including
parallel roots, a rejected original candidate, one correction, dependent input receipts, checks,
Reviewer acceptance, integration and one Closure. Live provider/sandbox qualification and
non-Linux acceptance remain pending. The private launch evidence records actual executable,
exact finite permission/model pins and complete lifecycle evidence separately from absent
runtime model/effort telemetry. Its retained events and prompt/stderr text are removed, and its
source report digest is rechecked before verification, review, integration and correction.
