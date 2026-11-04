# Coworker Read-Only Planning and Architectural Advice (`CoworkerPlanningService`)

This document specifies the implemented runtime boundaries for read-only Coworker planning and architectural consultation in `crates/threadlane-native/src/coworker_workflow/planning.rs` (`CoworkerPlanningService`) and `crates/threadlane-workers/src/advisory_response.rs`.

## 1. Service, Grant, and Snapshot Boundaries

- **Standing read-only grant authority (`PlanningAdviceGrant`)**:
  - Every `CoworkerPlanningService::obtain_architect_advice` (`RunArchitectAdviceRequest`) and `CoworkerPlanningService::orchestrate_brief_to_plan` (`OrchestrateWorkBriefPlanRequest`) call requires an active, non-expired, non-revoked `PlanningAdviceGrant` recorded in `CoworkerWorkflowStore`.
  - Grant policies (`ExecutionPolicy`) must restrict allowed roles strictly to `WorkflowRole::Orchestrator` and/or `WorkflowRole::Architect` (`PlanningReadOnlyCapabilities::strict_read_only()`). Implementation roles (`WorkflowRole::Coder`) belong in the workflow profile (`WorkflowProfile`) for compiled task nodes, never in the standing planning grant's `ExecutionPolicy`.
  - Roles and optional difficulty tiers are preflighted against the frozen `RegistrySnapshot` at `grant.registry_revision` and `grant.profile_id` without fallback or model substitution. Native conversational routes (`CodexCli` / `Follower`) and direct API adapters are rejected.
- **Advancing canonical repository base & isolated read-only snapshots**:
  - Standing grants pin `grant.project_base` (`CanonicalProjectBaseRef`). Individual calls may advance to a descendant commit in the same canonical repository (`is_commit_ancestor`) provided the live checkout is clean at `HEAD == call_project_base.base_commit`.
  - Each worker invocation runs inside a disposable detached read-only snapshot under `<artifacts_dir>/snapshots/` (`WorkerFilePermissions::ReadOnly`, empty `allowed_write_paths`). Both the live source repository (allowing ignored local caches, which are excluded from the snapshot clone) and the snapshot (strict cleanliness, rejecting even ignored additions) are verified clean before and after execution; any dirty state or base drift fails closed and records `PlanningReservationStatus::FailedAfterEffect`.

## 2. Bounded Output Capture, Private Artifacts, and Typed Schemas

- **Complete bounded output capture (`PlanningCallBudget.max_output_bytes_per_call` bounded by `MAX_ADVISORY_RESPONSE_BYTES`)**:
  - Worker supervision uses `threadlane_workers::run_advisory_worker_with_structured_result`, extracting full structured JSON (`BoundedAdvisoryResponse`) rather than reconstructing plans from 256-byte diagnostic event summaries. Per-call response capture follows the grant's frozen `PlanningCallBudget.max_output_bytes_per_call` bounded by `MAX_ADVISORY_RESPONSE_BYTES` (`MAX_LINE_BYTES = 4_194_304`), while worker stdout (`MAX_STDOUT_BYTES`), line (`DEFAULT_MAX_LINE_BYTES.min(max_stdout_bytes)`), and persisted artifact (`MAX_PLANNING_ARTIFACT_BYTES = 2 MiB`) limits also apply.
  - Missing, empty, truncated, oversized, malformed, or `deny_unknown_fields`-violating payloads are rejected and finalized as `PlanningReservationStatus::FailedAfterEffect`, consuming call budget honestly.
- **Private (`0600`) call artifacts (`PersistedPlanningCallArtifact`)**:
  - Every call that crosses the execution boundary persists a deterministic `pln_<32hex>.json` artifact in `artifacts_dir` binding `grant_id`, `reservation_id`, `run_id`, `request_key`, `selected_choice`, `context_digest`, `prompt_digest`, `output_digest`, `call_project_base`, and `PlanningArtifactOutcomeKind` (`ArchitectAdvice`, `OrchestratorOutput`, or `FailedAfterEffect`).
- **Source-verified typed parser (`COWORKER_PLANNING_SCHEMA_VERSION = 1`)**:
  - Model output is parsed strictly through private source-verified Rust types (`StructuredArchitectAdviceOutput` and `StructuredOrchestratorOutput::{Plan, RequiresHumanClarification, RequiresArchitectAdvice}`).
  - Model text never grants user/operator authorization, never mutates `PlanningAdviceGrant` or `ExecutionPolicy`, never promotes `WorkBrief.proposed_additions` or `open_questions` into `confirmed_requirements`, and never creates `ApprovalRecord` or `FrozenApprovalLinkage`.
  - Planned task nodes in `StructuredOrchestratorOutput::Plan` must use `WorkflowRole::Coder`, form an acyclic DAG with non-overlapping parallel write scopes at `call_project_base`, declare consistent producer-before-consumer interface ownership, and omit `model_override` / `tool_override`.

## 3. Reuse, Multi-Round Consultation, Idempotent Replay, and Drift Refusal

- **Exact dependency-bound Architect reuse**:
  - `obtain_architect_advice` reuses an existing `ArchitectAdviceRecord` only when `ArchitectContextDependencies` (`project_base`, `work_brief_digest`, `plan_digest`, `policy_digest`, exact `dependency_file_blobs` SHA-256 digests at `base_commit`, `interface_ownership_digests`, `accepted_predecessor_trees`), `question_summary`, `prior_advice_context`, `selected_choice`, and grant provenance all match identically. Changed dependencies require a new call and may pass prior records only as verified `PriorArchitectAdviceContext`.
- **Bounded `Orchestrator -> Architect -> Orchestrator` consultation**:
  - When `StructuredOrchestratorOutput::RequiresArchitectAdvice` is returned and `max_architect_rounds > 0` (`<= MAX_ORCHESTRATOR_ARCHITECT_ROUNDS = 2`), `orchestrate_brief_to_plan` computes exact file blob digests at `call_base`, invokes `obtain_architect_advice` under the same standing grant budget, and re-invokes `Orchestrator` with the resulting `advice_id` context. Exceeding `max_architect_rounds` or call budget fails closed without publishing a plan.
- **Idempotent replay and mid-flight drift refusal**:
  - Repeating an identical completed `OrchestrateWorkBriefPlanRequest` across store/service reopen replays the persisted artifact, deterministic `ScopedWorkflow::save_draft` command journal entry, published plan revision, and `PlanningResultRef` without invoking the worker a second time or creating duplicate plan revisions.
  - Before `ScopedWorkflow::save_draft` / `publish` and `CoworkerWorkflowStore::record_job_planning_result`, the service verifies that the target job's `job_revision`, `work_brief_digest`, active holds, and amendment supersession status have not drifted since the call was reserved. If a brief amendment or hold occurs mid-flight, the call artifact remains on disk for auditability while plan publication and job linkage are refused.

## 4. Non-Claims and Out-of-Scope Boundaries

- This module implements library/service-level read-only planning and advice (`CoworkerPlanningService`). It does **not** expose MCP tools, does **not** run an automatic background task/review/fix scheduler (`controller.rs`), and does **not** assemble combined candidates (`candidate.rs`).
- **Shared manual approval binding vs. automatic controller admission**: `CoworkerWorkflowStore::bind_job_approval` is a shared manual approval-binding helper: when `job.planning_result` is `Some(PlanningResultRef)`, it strictly verifies that the approved plan revision, digest, profile, registry revision, project base, brief digest, and task interfaces match the recorded `PlanningResultRef`; when `job.planning_result` is `None`, it remains compatible with independent explicitly human-approved legacy/manual jobs even if the same chat binding contains `PlanningAdviceGrant` / `Orchestrator` reservations for other planning contexts. Legacy/manual `bind_job_approval` success is **not** automatic controller authority; the next-phase execution controller (`controller.rs`, not yet implemented) must explicitly require a current-job `PlanningResultRef` alongside verified `FrozenApprovalLinkage` and `ExecutionPolicy`.
- Verification in `crates/threadlane-native/tests/coworker_planning.rs` and `tests/coworker_planning_advice.rs` uses disposable local Git repositories and a local fake CLI script supervised by `threadlane-workers`; live paid-provider execution and live provider-side permission-denial acceptance are not claimed.


## Native completion notices

The paired planning gateway adds an operator-only transport extension to this
service-level boundary. On a verified managed Linux desktop session,
`--coworker-native-notices` lets `consult_architect` and
`request_technical_plan` persist an immutable command-scoped native target before
starting the detached CLI. The CLI verifies the durable terminal command and its
completed artifacts before attempting a fixed notice to the same Codex thread.
The Coworker reads the exact command through `planning_command`; a notice never
accepts advice, grants execution approval, or starts coding.

`planning_command` exposes redacted `native_notice` metadata separately from the
planning outcome. `submitted` records backend acceptance only. Interrupted intent
and a possible-send disconnect remain `unknown` and are not automatically replayed,
including after a duplicate CLI invocation. Status reads never send notices. A
notification error does not replace the verified planning result. A crash before
notice reservation can still leave a terminal command without a notice; this is
not an indefinite background delivery service.

Tests use disposable repositories, local fake providers, and private WebSocket
peers. Installed managed enrollment and real desktop delivery remain separate
acceptance gates; no paid-provider or cross-platform acceptance is implied.
