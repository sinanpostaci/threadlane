# Coworker Workflow Observation and Desktop Projection

This document describes the live observation journal instrumentation (`crates/threadlane-native/src/coworker_observation.rs`) and read-only desktop Activity projection (`apps/desktop/src-tauri/src/observation/coworker_jobs.rs`) for the end-to-end Coworker workflow.

## Authority and Safety Boundaries

- **Shared observation journal, not a new authority**: Coworker workflow stages reuse `threadlane_core::observation` (`ObservationJournalRecord`, `append_journal_record`, `read_journal_records_readonly`) and append bounded records to `<plans_dir>/observation-journal.jsonl` with private permissions (`0700` directory, `0600` file).
- **Non-authoritative telemetry**: Observation hooks never grant execution authority, never alter `CoworkerWorkflowStore` ledger state, `ControllerPolicyStore` runtime envelopes, role budgets, physical job ownership guards, or Git/receipt verification outcomes. If a journal write fails, the underlying workflow stage outcome is preserved unchanged.
- **Strictly read-only desktop refresh**: Desktop snapshot projection reads private ledgers, runtime sidecars, plans, approvals, and `observation-journal.jsonl` without creating directories, files, or lockfiles, without bumping ledger or runtime revisions, and without invoking Git, CLI executables, or model providers.
- **No fabricated legacy session events or callbacks**: Observation projection does not synthesize fake `ApprovedHost` session events, batch IDs, or approval IDs, and does not send native Codex messages or wake-up callbacks.

## Wired Stage Boundaries

Each real service execution boundary emits at most one `ObservationRecordPhase::Entry` record (`ObservationCallState::Running`, bound to the current process's `ObservationProcessInstance { pid, boot_id, start_ticks }`) immediately before crossing the local provider/command/Git boundary and one terminal `ObservationRecordPhase::Finish` record (`Completed`, `Failed`, or `Cancelled`, with measured wall-clock `duration_ms`) when that boundary returns:

| Operation Constant | Operation String | Source Module | Boundary Wrapped | Role / Provider |
| --- | --- | --- | --- | --- |
| `COWORKER_OBS_OP_PLANNING_ORCHESTRATOR` | `coworker.planning.orchestrator` | `coworker_workflow/planning.rs` | `execute_advisory_worker_call` (`WorkflowRole::Orchestrator`) | `orchestrator` / `WorkerProvider` |
| `COWORKER_OBS_OP_PLANNING_ARCHITECT` | `coworker.planning.architect` | `coworker_workflow/planning.rs` | `execute_advisory_worker_call` (`WorkflowRole::Architect`) | `architect` / `WorkerProvider` |
| `COWORKER_OBS_OP_CHECKS_TASK` | `coworker.checks.task` | `coworker_workflow/checks.rs` | `execute_frozen_check_command` (`TaskAttempt` context) | `verification` / `local_check` |
| `COWORKER_OBS_OP_REVIEW_TASK` | `coworker.review.task` | `coworker_workflow/review_execution.rs` | `run_supervised_worker` (read-only task `Reviewer`) | `reviewer` / `ToolAdapter` |
| `COWORKER_OBS_OP_CODER_CORRECTION` | `coworker.coder.correction` | `coworker_workflow/task_correction_execution.rs` | `run_supervised_worker` (derived in-scope `Coder` attempt) | `coder` / `ToolAdapter` |
| `COWORKER_OBS_OP_INTEGRATION_TASK` | `coworker.integration.task` | `coworker_workflow/task_integration.rs` | `integrate_task_worktree_into_run_worktree` | `integration` / `local_git` |
| `COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION` | `coworker.integration.job_aggregation` | `coworker_workflow/job_completion.rs` | `integrate_completed_run_worktrees` | `integration` / `local_git` |
| `COWORKER_OBS_OP_CHECKS_CANDIDATE` | `coworker.checks.candidate` | `coworker_workflow/checks.rs` | `execute_frozen_check_command` (`CombinedCandidate` context) | `verification` / `local_check` |
| `COWORKER_OBS_OP_CLOSURE_CANDIDATE` | `coworker.closure.candidate` | `coworker_workflow/candidate.rs` | `run_supervised_worker` (single combined-candidate `Closure`) | `closure` / `ToolAdapter` |

### Original `Coder` Host Join

Original `ApprovedHost` `Coder` task attempts (`attempt_no == 1`) already record worker execution manifests and `worker.execute` journal entries through `threadlane-host` and `threadlane-workers`. Rather than emitting a duplicate second `Coder` invocation from the controller, `apps/desktop/src-tauri/src/observation/coworker_jobs.rs` correlates each verified `WorkerObservationLink` (`approval_id`, `control_binding_id`, `plan_id`) onto the owning `CoworkerJobRecord` (`job_id`).

### Idempotent Replay and Poll Silence

- Exact dependency-bound `ArchitectAdviceRecord` reuse in `CoworkerPlanningService::obtain_architect_advice` returns before `execute_advisory_worker_call` and appends zero journal records.
- Re-invoking `execute_task_correction`, `review_task_attempt`, `integrate_accepted_task`, `aggregate_integrated_job`, or `complete_job` when a verified artifact/receipt is already recorded returns the idempotent outcome without crossing the execution boundary or appending new journal rows.
- Driver lease heartbeats (`active_driver_lease`), `step_job` status polls, and read-only store inspections do not append journal records.

## Provenance, Redaction, and Liveness Rules

- **Scope correlation**: Every journal record binds the verified `binding_id`, `thread_uuid`, canonical `project_path`, and—when present—`job_id`, `plan_id`, `approval_id`, and `task_id`. Desktop projection validates `coworker.*` journal records against the authoritative `CoworkerBindingLedger` and `ControllerJobRuntimeState` before attaching requests to a projected job; forged cross-binding or mismatched `job_id`/`thread_uuid`/`approval_id`/`plan_id` hints are stripped and surfaced as warnings.
- **Actor attribution and requester `selected_model` / `selected_effort`**: `begin_coworker_call_observation` checks `spec.verified_context` and `current_verified_coworker_observation_context_for_scope` (scoped to a specific async future via `with_verified_coworker_observation_context`). When an explicitly supplied, scope-matching `VerifiedMcpObservationContext` is active, `actor.evidence` is `ObservationActorEvidence::Configured` (`source: "paired_mcp"`, `role: "coworker"`). Otherwise—including production workflow calls without an explicit verified MCP origin, or separately spawned tasks and child processes that do not inherit Tokio task-local scope automatically—`actor.evidence` is `ObservationActorEvidence::Unknown` (`source: "coworker_workflow"`). On `ObservationActor`, `actor.model` and `actor.effort` retain their strict runtime-observed telemetry-only meaning (`None` for paired MCP coworker calls and never inferred from a workflow profile or `Follower` assignment). Optional `actor.selected_model` and `actor.selected_effort` carry the model and reasoning effort configured on the exact verified native Codex thread from metadata-only `thread/read` (`includeTurns: false`), refreshed at most once per paired MCP call through the existing verified managed Linux desktop parent WebSocket endpoint; refresh failures, timeouts, or scope mismatches clear `selected_model` and `selected_effort` to `None` so stale selection is never reported as current, without reading transcripts, acquiring model locks, polling in the background, or elevating identity (`native_per_call_identity_verified == false`). See [`docs/COWORKER-MODEL-OBSERVATION.md`](COWORKER-MODEL-OBSERVATION.md).
- **Independent executor requested vs. observed model and effort**: Executor `requested_model` and `requested_effort` come from the frozen `SelectedRoleChoice` or verified `ResolvedModelTarget` (independent of requester `selected_model` / `selected_effort`). Executor `observed_model` and `observed_effort` are `None` at `Entry` and populated at `Finish` strictly from actual runtime provider stream telemetry (`WorkerExecutionReport` via `extract_observed_model_and_effort`), never copied from requested configuration. Because Linux `codex exec --json` does not emit runtime model or effort telemetry on stdout, `observed_model` and `observed_effort` remain `None` on read-only `codex_cli` advisory runs unless genuinely emitted by the CLI (see [`docs/CODEX-ADVISORY.md`](CODEX-ADVISORY.md)).
- **Honest usage reporting**: `usage` (`ObservationUsage`) is `None` on all Coworker journal records unless genuinely reported by the provider runtime (`extract_observed_usage` with `provenance: "worker_stream"`). Read-only `codex_cli` advisory runs capture genuine token counts reported on terminal `turn.completed` (`input_tokens`, `output_tokens`, optional `cached_input_tokens`, and checked `total_tokens`) without fabricating dollar costs, estimates, or synthetic zero usage.
- **Redaction**: Titles, summaries, and error diagnostics pass through `sanitize_observation_label` and `sanitize_observation_summary`. Raw prompts, full worker stdout/stderr log bodies, structured response bodies, and credentials are never written to `observation-journal.jsonl`.
- **Process-loss degradation**: An unmatched `Entry` record is projected as `ObservationCallState::Running` only while `is_process_instance_alive` confirms the exact local `(pid, boot_id, start_ticks)` is still alive AND the corresponding planning reservation or controller `PendingControllerEffect` is still active (`ReservedBeforeEffect`). If the process exited, crashed, or its PID was reused, or if no active stage exists in the ledger/runtime, the call degrades to `ObservationCallState::Unknown`.

## Desktop Read-Only Job Projection

`apps/desktop/src-tauri/src/observation/coworker_jobs.rs` enumerates `<plans_dir>/coworker-workflow-*.json` (capped at `128` files, skipping symlinks, non-files, oversized files, schema mismatches, and filename/binding mismatches with bounded warnings) and projects each `CoworkerJobRecord` into `ObservationJob`:

1. **Stage mapping (`ObservationJobState`)**:
   - `Planning`: `CoworkerJobPhase::Briefing` or `CoworkerJobPhase::Planning`.
   - `NeedsUser`: `CoworkerJobPhase::AwaitingApproval`, `CoworkerJobPhase::Held`, any task in `Held` / `ReplanRequired` / `EscalatedToUser`, `runtime.stop_requested == true`, or any `PendingControllerEffectStatus::EffectUnknown`.
   - `Running`: `CoworkerJobPhase::Executing` with any task in `Dispatched` / `Running` or an active `CoderCorrection` effect.
   - `Reviewing`: `CoworkerJobPhase::Executing` or `CandidateAssembly` while tasks are in `ReadyForReview`, `UnderReview`, `CorrectInScopeRequired`, `Accepted`, `IntegrationReserved`, or `Integrated` (including when all tasks are `Integrated` or `Closure` is reserved/in-flight).
   - `Completed`: requires `job.phase == CoworkerJobPhase::Completed` **and** a verified `job.combined_candidate` with `status == CombinedCandidateStatus::ClosureAccepted` and `closure_verdict_id.is_some()`. All tasks reaching `Integrated` without `ClosureAccepted` proof remains in `Reviewing`.
   - `Failed` / `Unknown`: `CoworkerJobPhase::Failed` maps to `Failed`; `CoworkerJobPhase::Cancelled` maps to `Unknown`.
2. **Legacy plan/approval deduplication**: Legacy `PlanStore` entries are suppressed only when a `CoworkerJobRecord` in the same `binding_id` has a verified `PlanningResultRef` (`plan_id` + `plan_revision`) or `FrozenApprovalLinkage` (`approval_id` + `plan_id` + `plan_revision`) matching the plan. Unrelated plans—even with identical titles—remain visible as separate jobs.
3. **Heartbeat-stable projection**: `active_driver_lease` renewal timestamps and `runtime_revision`-only changes are excluded from projected `ObservationJob` and `ObservationRequest` fields so background lease renewals do not trigger UI refresh churn.

## Unsupported Boundaries

- Native conversational callbacks and user notifications are handled by separate transport slices and are not emitted by observation hooks.
- Local `antigravity` and `opencode_v2` CLI worker adapters do not currently report token or dollar cost usage in `WorkerExecutionReport`; their `usage` remains `None` (whereas `codex_cli` advisory runs report genuine `turn.completed` token counts only, with no dollar cost estimation).
- Non-Linux process start-tick verification is not claimed by this slice.
