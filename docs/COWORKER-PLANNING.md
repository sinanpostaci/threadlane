# Coworker Read-Only Planning and Architectural Advice (`CoworkerPlanningService`)

This document specifies the implemented runtime boundaries for read-only Coworker planning and architectural consultation in `crates/threadlane-native/src/coworker_workflow/planning.rs` (`CoworkerPlanningService`) and `crates/threadlane-workers/src/advisory_response.rs`.

## 1. Service, Grant, and Snapshot Boundaries

- **Standing read-only grant authority (`PlanningAdviceGrant`)**:
  - Every `CoworkerPlanningService::obtain_architect_advice` (`RunArchitectAdviceRequest`) and `CoworkerPlanningService::orchestrate_brief_to_plan` (`OrchestrateWorkBriefPlanRequest`) call requires an active, non-expired, non-revoked `PlanningAdviceGrant` recorded in `CoworkerWorkflowStore`.
  - Grant policies (`ExecutionPolicy`) must restrict allowed roles strictly to `WorkflowRole::Orchestrator` and/or `WorkflowRole::Architect` (`PlanningReadOnlyCapabilities::strict_read_only()`). Implementation roles (`WorkflowRole::Coder`) belong in the workflow profile (`WorkflowProfile`) for compiled task nodes, never in the standing planning grant's `ExecutionPolicy`.
  - Roles and optional difficulty tiers are preflighted against the frozen `RegistrySnapshot` at `grant.registry_revision` and `grant.profile_id` under the grant's `ExecutionPolicy` without fallback or model substitution, then normalized through `normalize_read_only_advisory_choice`. The native conversational role (`WorkflowRole::Follower`), unnormalized `NativeHandoff` routes, `CodexCli` choices without an explicit reasoning effort, and direct API/auxiliary adapters are rejected; `ToolAdapter::CodexCli` (`WorkerProvider::CodexCli`) is permitted for read-only `Orchestrator` and `Architect` calls when explicitly configured with a model and reasoning effort and allowed by the grant's `ExecutionPolicy` (see [`docs/CODEX-ADVISORY.md`](CODEX-ADVISORY.md)).
  - **Proof-bound selected-profile isolation vs. legacy strict revision checks (`ScopedProfileDependencyProof`)**:
    - New standing grants prepared via `prepare_planning_grant_preview` and recorded via `record_planning_grant` (`create_planning_grant_with_scoped_proof`) bind a canonical `ScopedProfileDependencyProof` into the confirmed preview digest, deterministic `grant_id`, private persisted `PlanningAdviceGrant` record, and `ScopedWorkflow::save_draft_with_scoped_proof` command journal entry (never exposing hidden executable paths in metadata-only `PlanningGrantSummary` DTOs).
    - Before command enqueue/launch, before `Architect` and `Orchestrator` worker effects, before `ScopedWorkflow::save_draft_with_scoped_proof` / `publish` and `record_job_planning_result`, and during ordinary `get_plan`, `list_plans`, and `read_plan_summary_readonly_at` compilation, the proof verifies that the selected profile (`grant.profile_id`) and every referenced model and tool definition in the live `RegistrySnapshot` still match the exact bound dependency specification while preserving original `grant.registry_revision` / `header.profile_revision` provenance. Unrelated profile/model/tool additions, updates, or deletions that leave the selected profile's dependency closure unchanged do not block planning calls, plan publication, or plan retrieval.
    - Any deletion or drift in the selected profile, its role assignments/efforts, or its referenced models/tools (including adapter, `executable_path`, or `enabled` state), tampered proof digests, or stale `RegistryStore` compare-and-swap revisions at preview recording time fail closed before new effects or publication.
    - Legacy grants or journal entries persisted without `scoped_profile_dependency_proof` (`None`) retain strict global `snapshot.revision == grant.registry_revision` (`header.profile_revision`) enforcement.
- **Advancing canonical repository base & isolated read-only snapshots**:
  - Standing grants pin `grant.project_base` (`CanonicalProjectBaseRef`). Individual calls may advance to a descendant commit in the same canonical repository (`is_commit_ancestor`) provided the live checkout is clean at `HEAD == call_project_base.base_commit`.
  - Each worker invocation runs inside a disposable detached read-only snapshot under `<artifacts_dir>/snapshots/` (`WorkerFilePermissions::ReadOnly`, empty `allowed_write_paths`). For `WorkerProvider::CodexCli`, the worker launches `codex --no-daemon exec --json --ephemeral --ignore-user-config` with pinned `--model` and `-c model_reasoning_effort`, `-c approval_policy="never"`, the named `threadlane-advisory` read-only filesystem/network-disabled permission profile, `-c web_search="disabled"`, and `--disable` for all `CODEX_ADVISORY_DISABLED_FEATURES` after verifying that no project-local `.codex`, `codex.toml`, or `.codex.toml` marker exists in or above the snapshot (`validate_codex_advisory_workdir`). Both the live source repository (allowing ignored local caches, which are excluded from the snapshot clone) and the snapshot (strict cleanliness, rejecting even ignored additions) are verified clean before and after execution; any dirty state or base drift fails closed and records `PlanningReservationStatus::FailedAfterEffect`.

## 2. Bounded Output Capture, Private Artifacts, and Typed Schemas

- **Complete bounded output capture (`PlanningCallBudget.max_output_bytes_per_call` bounded by `MAX_ADVISORY_RESPONSE_BYTES`)**:
  - Worker supervision uses `threadlane_workers::run_advisory_worker_with_structured_result`, extracting full structured JSON (`BoundedAdvisoryResponse`) rather than reconstructing plans from 256-byte diagnostic event summaries. For `CodexCli`, the collector accepts only a bounded final `item.completed` (`item.type == "agent_message"`) JSON object sealed inside `thread.started` -> `turn.started` -> `turn.completed` framing (`AdvisoryResponseSource::CodexAgentMessageTurnCompleted`), preserving timeout, cancellation, unknown-outcome, and duplicate recovery. Because Linux `codex exec --json` does not emit runtime model or effort telemetry, `observed_model` and `observed_effort` remain `None` (never copied from the requested target) while completion relies on the verified pinned launch configuration and genuine `turn.completed` token usage is captured when reported. Per-call response capture follows the grant's frozen `PlanningCallBudget.max_output_bytes_per_call` bounded by `MAX_ADVISORY_RESPONSE_BYTES` (`MAX_LINE_BYTES = 4_194_304`), while worker stdout (`MAX_STDOUT_BYTES`), line (`DEFAULT_MAX_LINE_BYTES.min(max_stdout_bytes)`), and persisted artifact (`MAX_PLANNING_ARTIFACT_BYTES = 2 MiB`) limits also apply.
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

### Explicit receipt-only recovery of legacy plan ownership collisions

New requests reject a `plan_id` already linked to another job before enqueue or
provider reservation. Updates to the same job remain supported. An amendment job
must receive its own logical plan; publication never transfers plan ownership.

For a legacy `Unknown` command whose direct Orchestrator call completed before a
cross-job publication/link collision, explicit `execute-command` may recover the
exact persisted Plan without a provider invocation. The driver first acquires
command ownership and refreshes durable state. Recovery requires a Completed
reservation, fully verified bounded artifact, matching original request key,
job/brief/revision, model choice, grant provenance, and context/output digests.
The original requested plan must still be demonstrably owned by another job.
Missing or changed evidence, held/superseded/approved jobs, source drift, dirty
source checkouts, and ambiguous multi-round receipts are refused.

The recovered plan uses a new deterministic identity and draft command key bound
to the original command digest, job, artifact, and output digest. It is published
at revision 1 through the existing command journal and one-job-per-plan linkage
checks. Reopening after publication or linkage resumes the same identity without
a new revision or call. The original payload, artifact, reservation, owner, and
plan history stay intact. Expired or exhausted grants can prove historical output
but grant no new invocation authority. Recovery leaves the job awaiting explicit
execution approval; it creates no tasks, approvals, dispatches, or notice retries.
Status/get/list operations remain read-only and never initiate recovery.

## 4. Non-Claims and Out-of-Scope Boundaries

- This module implements library/service-level read-only planning and advice (`CoworkerPlanningService`). It does **not** expose MCP tools, does **not** run an automatic background task/review/fix scheduler (`controller.rs`), and does **not** assemble combined candidates (`candidate.rs`).
- **Shared manual approval binding vs. automatic controller admission**: `CoworkerWorkflowStore::bind_job_approval` is a shared manual approval-binding helper: when `job.planning_result` is `Some(PlanningResultRef)`, it strictly verifies that the approved plan revision, digest, profile, registry revision, project base, brief digest, and task interfaces match the recorded `PlanningResultRef`; when `job.planning_result` is `None`, it remains compatible with independent explicitly human-approved legacy/manual jobs even if the same chat binding contains `PlanningAdviceGrant` / `Orchestrator` reservations for other planning contexts. Legacy/manual `bind_job_approval` success is **not** automatic controller authority; the separate execution controller requires a current-job `PlanningResultRef` alongside verified `FrozenApprovalLinkage` and `ExecutionPolicy`. The full checks/review/correction/integration/Closure pipeline is documented in [the driver contract](COWORKER-DRIVER.md); its installed end-to-end acceptance is tracked in [MVP readiness](MVP-READINESS.md).
- Verification in `crates/threadlane-native/tests/coworker_planning.rs` and `tests/coworker_planning_advice.rs` uses disposable local Git repositories and a local fake CLI script supervised by `threadlane-workers`; live paid-provider execution and live provider-side permission-denial acceptance are not claimed.
- **Remaining strict approval and execution revision boundary**: `ScopedProfileDependencyProof` currently isolates read-only planning grants (`PlanningGrantPreview` / `PlanningAdviceGrant`), `CoworkerPlanningService`, `PlanningCommandStore` / `PlanningCommandDriver`, and `ScopedWorkflow` plan draft/publish/retrieval. Downstream execution approval and job execution boundaries (`AuthorityStore::approve_plan`, `CoworkerWorkflowStore::bind_job_approval`, `RoleFlowStore`, `ControllerPolicyStore`, `CoworkerController`, and downstream coder/review/correction/integration/closure handoffs) still enforce exact global `registry_revision` equality; full job execution profile isolation and installed end-to-end acceptance are not claimed here.


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
planning outcome. `submitted` records backend acceptance only and carries no
failure diagnostic. When a notice attempt finishes in `failed` or `unknown`,
`PersistedPlanningNoticeAttempt` and `PlanningNoticeReport` (`native_notice.latest`)
record an optional, privacy-safe `diagnostic` stage (`configuration_or_shutdown`,
`endpoint_or_identity`, `connect_or_handshake`, `initialize`, `thread_read`,
`thread_resume`, `status_or_active_turn_lookup`, `delivery`, `timeout`, or
`unclassified`) mapped only from known software-owned wake-up reasons without
persisting raw error text, paths, URLs, prompts, tokens, or hashes. This
metadata is diagnostic only: historical records written without a `diagnostic`
field cannot have their failure reasons reconstructed and remain `None` without
inventing a cause. Interrupted `intent` and post-send `unknown` outcomes remain
uncertain, must be reconciled by inspecting the durable command and plan state,
and are not automatically replayed, including after a duplicate CLI invocation.
Status reads never send notices. Failed or unknown notice delivery does not
invalidate an already verified planning artifact or published plan and never
grants permission to replay a notice or re-run a model call; recording this
diagnostic classification does not by itself claim the observed live notice
failure is fixed. A crash before notice reservation can still leave a terminal
command without a notice; this is not an indefinite background delivery service.

Tests use disposable repositories, local fake providers, and private WebSocket
peers. Installed managed enrollment and real desktop delivery remain separate
acceptance gates; no paid-provider or cross-platform acceptance is implied.


## Live provider diagnostic availability

Owned Orchestrator and Architect calls, including internal consultation rounds,
persist bounded AGY hints per actual reservation before an optional early native
callback. Live eligibility separately requires the actual physical guard, current
Running command, immutable command/grant/role/context, exact still-reserved call,
durable current hint and configured matching native target. The existing terminal
eligibility rule still rejects active owners. Neither route grants model or
execution authority.

The callback uses the fixed software planning text asking for `planning_command`.
It contains no diagnostic text, paths, prompts or arbitrary errors. Read
`command.provider_health_views`: `live` describes the current physical owner and
reservation, while `record.snapshot.current_failure` describes current advisory
attention. `command.provider_health` preserves first/last history. Terminal,
recovered or ownerless observations are not current Needs you signals. Pure reads
perform no send and stale owner metadata alone is insufficient.

The legacy `terminal_kind` field also accepts `diagnostic_available` for the
separate live advisory route; this value is not a terminal planning outcome.
Its fingerprint is per invocation and independent of owner epoch, job revision
and retry count. Health notices deduplicate before the 14-attempt attention
subquota within the shared total of 16 per command; a verified terminal callback
uses its original distinct fingerprint after release. Intent-before-send and
Unknown/no-replay behavior remain unchanged, including ambiguous early sends.
Early notification failures cannot replace a proven planning result.

Coworker fetches the named command once and describes the unverified AGY
stream-idle/internal retry text, observed count, reported high-water ordinal,
elapsed time and diagnostic availability in the human conversation language
(Turkish in this pilot). Running is not
evidence of productive planning. No client/network/upstream cause or permission
to cancel, redispatch, switch models or extend deadlines follows from a hint.
See [provider health](PROVIDER-HEALTH.md) for finite bounds and qualification limits.
