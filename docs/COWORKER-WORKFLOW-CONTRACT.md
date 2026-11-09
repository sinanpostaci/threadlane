# Coworker Workflow Contract

This document defines the shared data contracts, per-binding compare-and-swap ledger store,
trust/provenance boundaries, and frozen Rust module signatures for the end-to-end Threadlane
Coworker workflow package.

The current trusted-local complete-job entry point is
[approval admission](COWORKER-APPROVAL-ADMISSION.md). Sections describing staged
writer ownership and upcoming signatures below preserve the original foundation
contract; they are historical coordination records, not a claim that the now
implemented driver/review/integration/Closure pipeline is unavailable.

## 1. Architectural Boundaries and Core Invariants

1. **Native Coworker is the conversational interface only (`COWORKER_IS_CONVERSATIONAL_INTERFACE_ONLY = true`), while read-only advisory roles may normalize `CodexCli` explicitly**
   - The native Coworker (`WorkflowRole::Follower`) remains the human-selected Codex conversation,
     occupies no model profile slot or lock, and is **never** dispatched as a headless worker.
   - Generic `RoleExecutionRoute::from_adapter(ToolAdapter::CodexCli)` and manual role-flow handoffs
     stay `RoleExecutionRoute::NativeHandoff`.
   - After normal exact `RegistrySnapshot` + `ExecutionPolicy` preflight succeeds, explicit
     read-only normalization (`normalize_read_only_advisory_choice` /
     `is_read_only_advisory_worker_role`) permits `ToolAdapter::CodexCli`
     (`RoleExecutionRoute::HeadlessWorker`) **only** for read-only advisory roles
     (`WorkflowRole::Orchestrator`, `WorkflowRole::Architect`, `WorkflowRole::Reviewer`, and
     `WorkflowRole::Closure`) with an explicit reasoning effort validated via
     `resolve_model_target(WorkerProvider::CodexCli, ...)`. See
     [`docs/CODEX-ADVISORY.md`](CODEX-ADVISORY.md).
   - `validate_non_native_worker_choice` rejects `WorkflowRole::Follower`, any non-`HeadlessWorker`
     route (`NativeHandoff`), unsupported adapters and any `CodexCli` choice lacking an explicit
     validated reasoning effort. Complete newly approved Codex CLI Coder contracts use the
     separate [Codex Coder](CODEX-CODER.md) route; a generic/native or legacy `provider=None`
     approval is not automatically converted to headless execution. Exact admission still checks
     the authority-approved task choice and canonical executable.
2. **Supported trust and provenance model**
   - Grants and execution approvals rely on the existing trusted-local human-confirmation
     attestation (`operator_attested = true` on `PlanningAdviceGrant`; operator token +
     `ApprovalRecord` in `AuthorityStore`).
   - Local grant creation never claims cryptographic per-call native identity verification
     (`native_per_call_identity_verified` must be `false`) and never forces a desktop approval UI.
3. **Structured `WorkBrief`, planner context vs execution approval gates, and job amendments**
   - `WorkBrief` separates `confirmed_requirements`, `proposed_additions`, `assumptions`,
     `open_questions`, and `accepted_architect_decisions`.
   - `WorkBrief::is_ready_for_execution_planning()` requires a valid brief with non-empty
     `confirmed_requirements` and allows technical `open_questions` and unapproved
     `proposed_additions` to reach `CoworkerJobPhase::Planning` as non-authoritative planner
     context so the Coworker is not forced to solve technical planning before the Orchestrator.
   - `proposed_additions` remain explicitly unapproved (never executable scope and never allowed to
     duplicate or silently become `confirmed_requirements`).
   - `WorkBrief::is_ready_for_execution_approval()` (enforced by
     `CoworkerWorkflowStore::bind_job_approval`) strictly requires both `open_questions` and
     `proposed_additions` to be empty so unapproved proposals or unresolved questions never
     authorize writes or worker execution.
   - `WorkBriefKind` distinguishes `NewJob` from `Amendment { target_job_id, target_job_revision, amendment_reason }`.
     Recording an amendment verifies `target_job_revision` (CAS) and places affected tasks on the
     superseded target job into `CoworkerTaskStage::Held` (`WorkflowHoldReason::BriefAmendmentPending`).
4. **Separate read-only `PlanningAdviceGrant` with standing repository scope and durable pre-effect budget reservation**
   - `PlanningAdviceGrant` is strictly separate from code execution `ApprovalRecord`s.
   - Standing grant scope is bound to the canonical repository (`grant.project_base.repository_path`)
     plus frozen explicit models, `ExecutionPolicy` (`policy_digest`), `PlanningReadOnlyCapabilities`,
     finite `PlanningCallBudget`, and `expires_at_ms` (with `operator_attested = true` and policy
     allow rules restricted to `WorkflowRole::Orchestrator` and/or `WorkflowRole::Architect`).
   - `grant.project_base` records the origin base commit/tree metadata when the grant was issued
     and does not force new human approval after every normal commit in the same canonical
     repository. Each individual planning or architect call verifies its actual immutable base
     commit/tree (`call_project_base: Option<CanonicalProjectBaseRef>` on
     `ReservePlanningCallRequest`/`PlanningCallReservation`, and `context_dependencies.project_base`
     on `RecordArchitectAdviceRequest`/`ArchitectAdviceRecord`) and rejects any cross-repository
     context (`NativeErrorCode::SecurityViolation`). Execution approvals (`bind_job_approval`)
     continue to enforce the exact frozen `project_base` (`base_commit` and `base_tree_sha`).
   - `PlanningReadOnlyCapabilities` describes provider-visible capabilities and tool policy enforced
     by the trusted host middleware, not an OS kernel sandbox. The trusted host may prepare an owned
     immutable snapshot or read-only inspection view; the advisory model may not write files, mutate
     Git state, dispatch code execution, or invoke arbitrary mutating tools. Post-run repository
     cleanliness verification is additional post-hoc evidence, not primary prevention.
   - Calls reserve budget durably (`PlanningReservationStatus::ReservedBeforeEffect`) before
     crossing the provider boundary. `RefusedBeforeEffect` releases budget only when no `run_id`
     exists; `EffectUnknown` retains consumed budget and blocks automatic replay with
     `NativeErrorCode::AmbiguousWakeupPendingReconciliation` until reconciled.
5. **Dependency-bound `ArchitectAdviceRecord` reuse**
   - `ArchitectAdviceRecord` binds `ArchitectContextDependencies` (`project_base`,
     `work_brief_digest`, `plan_digest`, `policy_digest`, `dependency_file_blobs`,
     `interface_ownership_digests`, and `accepted_predecessor_trees`).
   - `ArchitectAdviceRecord::can_reuse_for` and `CoworkerWorkflowStore::find_reusable_architect_advice`
     require exact equality of all real context dependencies and their SHA-256 `context_digest`,
     never approximate text similarity.
6. **Immutable `FrozenApprovalLinkage` and explicit `FrozenControllerCapabilityEnvelope`**
   - `CoworkerWorkflowStore::bind_job_approval` verifies the operator `ApprovalRecord` in
     `AuthorityStore`, active published `PlanStore` revision, live `RegistryStore` revision, live
     Git repository/base commit/tree SHA, `WorkBrief::is_ready_for_execution_approval()`, and A1
     `preflight` admission under the supplied `ExecutionPolicy`.
   - `FrozenApprovalLinkage.policy_digest` (and a saved `WorkflowProfile`) by itself is **not**
     execution authority for `Reviewer`, `Closure`, automatic in-scope corrections, verification
     check commands, or local Git integration. Those capabilities require an explicit frozen
     `controller_envelope: Option<FrozenControllerCapabilityEnvelope>` verified via
     `authorizes_reviewer_dispatch`, `authorizes_closure_dispatch`,
     `authorizes_in_scope_correction`, `authorizes_check_failure_correction`,
     `authorizes_check_command`, and `authorizes_local_git_integration`. Automatic check-failure
     corrections default to `allow_check_failure_corrections: false` (omitted on serialization when
     `false` to preserve exact legacy v1 capability and execution envelope digests) and require
     explicit opt-in (`allow_check_failure_corrections: true`, `max_in_scope_correction_attempts >= 1`,
     and non-empty `allowed_check_ids`) before freezing. Existing coder-only or Reviewer-only
     approvals are never broadened or retrofitted.
   - Once bound on a `CoworkerJobRecord`, `approval_linkage` is immutable; any attempt to rebind a
     different approval, plan revision, base commit, write scope, policy, or controller capability
     envelope on the same job fails closed.
   - When `Reviewer` or `Closure` uses a normalized read-only `CodexCli` advisory worker, pinned
     launch configuration evidence is recorded explicitly and kept separate from runtime-observed
     stream telemetry: absent observed model/effort remains `None` with no `requested -> observed`
     fallback, while legacy `Antigravity` and `OpencodeV2` runtime telemetry gates remain strict.
7. **Routine task state machine, reserve-before-effect outbox, and structured review verdicts**
   - `CoworkerTaskStage::can_transition_to` defines the legal transition graph across
     `PendingDependencies`, `ReadyToDispatch`, `DispatchReserved`, `Running`, `ReadyForReview`,
     `ReviewReserved`, `UnderReview`, `CorrectInScopeRequired`, `Accepted`, `IntegrationReserved`,
     `Integrated`, `Held`, `Blocked`, `EffectUnknown`, `Failed`, and `Cancelled`.
   - `OutboxDispatchRecord` reserves durable intent (`ReservedBeforeEffect`) before any external
     worker or Git mutation and never auto-replays `EffectUnknown`.
   - `StructuredReviewVerdict` classifies outcomes as `Accept`, `CorrectInScope`, `ScopeConflict`,
     `Blocked`, or `Unclear`, with `prose_may_advance_acceptance == false`. Positive reviewer prose
     never advances a task to `Accepted`; only `ReviewVerdictOutcome::Accept` with a sealed
     `review_receipt_id` sets `accepted_candidate_id`.
8. **Holds and declared task independence (`prove_task_independence` / `evaluate_declared_task_independence`)**
   - Conversational changes, brief amendments, and review scope conflicts record a
     `WorkflowHoldRecord` and transition non-independent tasks to `CoworkerTaskStage::Held`.
   - `prove_task_independence` (aliased as `evaluate_declared_task_independence`) evaluates
     **declared** dependency closure, write-scope, and interface metadata only; it cannot prove
     undeclared semantic independence. Whenever any hold is active or any sibling task is impacted,
     it requires explicit completeness of declared impact/dependency/interface metadata
     (`complete_declared_evidence == true`) and conservatively falls back to `independent: false`
     when any required declaration is incomplete or unknown:
     1. `complete_declared_evidence`: the candidate task, every active non-job-wide hold, and every
        impacted sibling task have complete write-scope and interface impact declarations (disjoint
        file `write_paths` plus incomplete interface declarations never release a task).
     2. `no_job_wide_hold`: no active job-wide hold and the task is not directly targeted.
     3. `disjoint_dependency_closure`: no task in the transitive `depends_on` closure is held,
        blocked, or in `EffectUnknown`.
     4. `disjoint_write_scopes`: `write_paths` do not overlap (including directory prefix
        containment such as `"src/"` vs `"src/lib.rs"`) with any active hold or impacted task.
     5. `disjoint_interface_ownership`: `owned_interfaces` and `consumed_interfaces` are disjoint
        from all active holds and impacted tasks.
9. **Verified `CombinedCandidateIdentity`**
   - Binds `project_base`, `final_commit_sha`, `final_tree_sha`, `plan_id`, `plan_revision`,
     `plan_digest`, `approval_id`, `approval_subject_digest`, `scope_policy_revision`, `policy_id`,
     `policy_digest`, all tasks' `AcceptedPredecessorReceiptRef`s (`review_receipt_id` +
     `integration_receipt_id`), and passing `CandidateCheckEvidence` (`passed == true` and
     `exit_code == 0`) into a canonical SHA-256 `candidate_digest`.

---

## Complete approval admission contract

`CoworkerAdmissionService` in `coworker_admission.rs` supplies pure `preview`,
explicitly attested `record`, pure `inspect` and persistence-only `reconcile`.
`threadlane-flow coworker-admission` exposes these through the existing private
context `flow` family. No new MCP approval or authority schema is introduced.

`AdmissionRequest` contains the stable `request_key`, nested
`BindJobApprovalRequest`, exact recorded `PlanningResultRef`, `work_brief_digest`,
nested `FreezeControllerExecutionEnvelopeRequest`, and explicit optional
Reviewer/Closure difficulty tiers. Admission requires original job/ledger CAS
revisions, exact existing authority, explicit controller capabilities, exact
profile/task resolution and canonical Worker/Reviewer/Closure executables. The
execution request uses the original source job revision; the service prepares its
envelope against the prospective resulting linked job. Source and resulting
revisions remain separately recorded and must never be substituted on retry.

The order is exact published-plan authority approval, complete admission preview,
presentation of all roles and execution powers, confirmation of that unchanged
scope, record, readiness inspection, then separate `start_execution` with the
returned frozen envelope digest. Record requires `--operator-approved` and
`--confirm-preview`, `--confirm-approval`, `--confirm-envelope`. JSON alone grants
nothing. An identical already-approved complete scope needs no redundant consent;
Worker-only approval implies no Reviewer, Closure, checks, corrections or Git.
Saved models and worker prose never authorize expansion. Local attestation does
not prove cryptographic human identity; `native_per_call_identity_verified` stays
`false`.

`AdmissionPreview` covers the normalized original request, prospective linked
job/ledger revision, exact authority budgets/concurrency, full execution envelope,
canonical executable pins and planning display configuration. The latter includes
all configured Orchestrator/Architect tiers and missing/disabled reasons, actual
profile/models/tools, and original ledger-validated planning grant, policy,
budgets and reservations. Configured planning roles are distinct from historical
Orchestrator identity/Architect references in `PlanningResultRef`. The native
Coworker has no profile execution slot. The domain-separated `preview_digest`
covers this complete subject without changing legacy authority/envelope digests.

`AdmissionReceipt` preserves that preview, original three-digest confirmation,
stage and monotonic `runtime_initialized`. An intent precedes workflow linkage;
linkage and immutable `AdmissionMarker` share a workflow transaction; runtime
initialization and completion are separate persistence boundaries. These are
admission receipts, not provider execution receipts. Request key, original
creation timestamp, request/preview/authority/envelope digests and original/resulting
revisions remain stable through duplicates and recovery. A compact CLI record or
reconcile result returns the original **resulting** linked job revision even when
the live job has advanced.

Inspection independently classifies `absent`, `partially_recorded`, `complete`,
`uncertain` or `conflicting`. Preview/inspect never lock, sync, repair or journal.
All admission CLI commands bypass observation journaling; intent/receipt is the
durable mutation record. A stored Complete receipt observed by a fresh process
may inspect as uncertain because it lacks that process's durability witness.
Deliberate execution gates can establish the witness only for unchanged stored
Complete consent with initialized runtime and matching linkage/marker/envelope:
lock, sync existing workflow/controller ledgers, receipt and directory, reread,
then cache a witness on successful sync returns. This changes no subjects,
bytes/mtime, revisions, counters, leases or provider state. Pure reads and
cancellation never perform it.

Stored Uncertain/Intent/Linked/RuntimeRecorded, missing required receipts or
initialized runtime and conflicts remain fenced. Start never finishes an
incomplete admission. Explicit original-key reconcile completes only permitted
missing persistence backed by original consent; it cannot replace consent or
reset initialized runtime. Complete duplicates and Complete reconcile sync
existing persistence and return the original receipt without rewriting it.
Failures after an intent direct the operator to inspect/reconcile;
`persistence_uncertain` and `outcome_unknown` never justify a new key or blind replay.

Immutable linkage or envelope conflicts require a new job or supported
amendment/replan with exact authority and complete-scope reapproval. Exactly
matching manual linkages may opt in only when all immutable subjects match.
Older valid jobs without admission intent/marker retain prior manual routes and
limitations. Newly admitted Workers, Reviewer and Closure consume canonical pins;
marked jobs have no PATH fallback. Historical inspection after supersession does
not establish current execution eligibility.

This package's regression sources use disposable fixtures/fake providers. Parent
validation owns combined gates; no installed/live-provider, native-chat delivery
or physical power-loss acceptance is inferred from fixture success. Larger runtime
limits, decomposition/language changes, startup ergonomics, native UI status and
observation performance remain separate follow-up work.

---

## 2. Foundation Ownership (Stage 1 — Implemented)

- `crates/threadlane-native/src/lib.rs`: declares `pub mod coworker_workflow;`.
- `crates/threadlane-native/src/coworker_workflow/mod.rs`: exports `contracts` and `store`.
- `crates/threadlane-native/src/coworker_workflow/contracts.rs`: serializable DTOs
  (`#[serde(deny_unknown_fields)]`), finite bounds, validation helpers, deterministic digests,
  state transition graph, and `prove_task_independence`.
- `crates/threadlane-native/src/coworker_workflow/store.rs`: private per-binding ledger
  (`coworker-workflow-<binding_tag>.json` + `.lock` in `plans_dir`, schema version `1`),
  `CoworkerWorkflowStore::transact_binding_ledger`, and transactional store operations:
  - `new`, `plans_dir`, `ledger_path_for_binding`, `lock_path_for_binding`, `read_binding_ledger`
  - `create_planning_grant`, `reserve_planning_call`, `finalize_planning_call`, `revoke_planning_grant`
  - `record_architect_advice`, `find_reusable_architect_advice`
  - `create_or_amend_job`, `bind_job_approval`
  - `reserve_task_outbox`, `reconcile_or_finalize_outbox`
  - `record_task_candidate_and_verdict`
  - `apply_workflow_hold`, `resolve_workflow_hold`
  - `record_combined_candidate`
- `crates/threadlane-native/tests/coworker_workflow_contract.rs`: behavioral and contract tests.

### Storage and Preservation Rules

- Ledger files are stored at `<plans_dir>/coworker-workflow-<binding_tag>.json` (`0600`) with lockfile
  `<plans_dir>/coworker-workflow-<binding_tag>.lock` inside an existing private (`0700`) non-symlink
  `plans_dir`.
- Pure reads (`read_binding_ledger`, `find_reusable_architect_advice`) never create directories,
  ledger files, or lockfiles.
- If an existing ledger file contains invalid JSON, unknown fields, or an unsupported
  `schema_version != 1`, the store fails closed with `NativeErrorCode::ProtocolError` and preserves
  the on-disk file untouched (never overwriting or migrating user state silently).

---

## 3. Frozen Signatures for Upcoming Writers

To prevent cross-writer conflicts, each of the next three writers owns a single new file under
`crates/threadlane-native/src/coworker_workflow/` and must not edit `mod.rs`, `lib.rs`, or
`Cargo.toml` unless coordinated through the Stage 5 integration writer.

### 3.1 Writer 2: `crates/threadlane-native/src/coworker_workflow/planning.rs`

**Responsibility**: Async read-only orchestrator and architect execution over `PlanningAdviceGrant`
reservations, dependency-bound architect advice caching/reuse, and `ScopedWorkflow` plan draft/publish
orchestration.

**Key design rules**:
- Actual async provider execution requires `async` APIs (`pub async fn obtain_architect_advice` and
  `pub async fn orchestrate_brief_to_plan`). Never call `block_on` inside an async MCP runtime.
- `OrchestrateWorkBriefPlanRequest` does **not** require `proposed_tasks: Vec<PlanTask>` as a
  finished technical DAG from the Coworker; validated structured Orchestrator output is the
  authoritative task source. Optional `unapproved_candidate_hints: Vec<String>` (alongside
  `WorkBrief::open_questions` and `WorkBrief::proposed_additions`) may be supplied as unapproved
  planner context only and can never silently become confirmed requirements or executable scope.
- `OrchestrateWorkBriefPlanOutcome` models structured planning outcomes including staged/published
  plans, human-clarification requests, and targeted architect-advice questions.
- Read-only capability enforcement (`PlanningReadOnlyCapabilities`) describes provider-visible
  capabilities and tool policy enforced by the trusted host middleware, not an OS kernel sandbox:
  the trusted host may prepare an owned immutable snapshot for inspection, while the advisory model
  may not write files, mutate Git, dispatch code execution, or invoke arbitrary mutating tools.
  Post-run repository cleanliness is additional post-hoc evidence, not primary prevention.

**Frozen public types and signatures**:

```rust
use std::path::PathBuf;
use threadlane_workflow::{Difficulty, PlanTask, PublishedPlanView, ScopedWorkflow};
use crate::coworker_workflow::{
    ArchitectAdviceRecord, ArchitectContextDependencies, CanonicalProjectBaseRef,
    CoworkerJobRecord, CoworkerWorkflowStore, PlanningAdviceGrant, PlanningCallReservation,
    WorkBrief,
};
use crate::error::NativeError;
use crate::profile_flow::ExecutionPolicy;

/// Request to execute or reuse dependency-bound architectural advice under a `PlanningAdviceGrant`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunArchitectAdviceRequest {
    pub binding_id: String,
    pub thread_uuid: String,
    pub grant_id: String,
    pub request_key: String,
    pub expected_ledger_revision: Option<u64>,
    pub difficulty: Option<Difficulty>,
    pub context_dependencies: ArchitectContextDependencies,
    pub prompt_instruction: String,
    pub current_time_ms: u64,
}

/// Outcome of an architectural advice request (either reused from unchanged dependencies or freshly executed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchitectAdviceOutcome {
    /// Reused an existing `ArchitectAdviceRecord` without consuming grant call budget.
    ReusedExisting(ArchitectAdviceRecord),
    /// Reserved grant budget, executed the read-only headless architect worker, and recorded advice.
    ExecutedNew {
        reservation: PlanningCallReservation,
        advice: ArchitectAdviceRecord,
    },
}

/// Request to run the read-only orchestrator under a `PlanningAdviceGrant` and either stage/publish
/// a validated technical plan or return structured clarification / architect-advice outcomes.
///
/// The Coworker never has to supply a finished technical task DAG; validated structured
/// Orchestrator output is the authoritative source of `PlanTask`s. Any `unapproved_candidate_hints`
/// or `WorkBrief.proposed_additions` are unapproved planner context only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrateWorkBriefPlanRequest {
    pub binding_id: String,
    pub thread_uuid: String,
    pub grant_id: String,
    pub job_id: String,
    pub expected_job_revision: u64,
    pub expected_ledger_revision: Option<u64>,
    pub request_key: String,
    pub plan_id: String,
    pub expected_plan_revision: u64,
    pub call_project_base: CanonicalProjectBaseRef,
    pub difficulty: Option<Difficulty>,
    pub current_time_ms: u64,
    pub unapproved_candidate_hints: Vec<String>,
    pub publish_immediately: bool,
}

/// Structured outcome of running the read-only Orchestrator over a `WorkBrief`.
#[derive(Debug, Clone)]
pub enum OrchestrateWorkBriefPlanOutcome {
    /// Validated structured Orchestrator output produced an executable task DAG from confirmed
    /// requirements and staged (and optionally published) the revision in `PlanStore`.
    PlanStagedOrPublished {
        job: CoworkerJobRecord,
        reservation: PlanningCallReservation,
        orchestrated_tasks: Vec<PlanTask>,
        plan_id: String,
        plan_revision: u64,
        published_view: Option<PublishedPlanView>,
    },
    /// Orchestrator determined that unresolved questions or unconfirmed scope proposals require
    /// human clarification/confirmation before an executable plan can be finalized.
    RequiresHumanClarification {
        job: CoworkerJobRecord,
        reservation: PlanningCallReservation,
        clarification_questions: Vec<String>,
        unconfirmed_scope_proposals: Vec<String>,
    },
    /// Orchestrator requested targeted read-only `Architect` advice before finalizing the task DAG.
    RequiresArchitectAdvice {
        job: CoworkerJobRecord,
        reservation: PlanningCallReservation,
        architect_questions: Vec<String>,
        target_interfaces_or_paths: Vec<String>,
    },
}

/// Stateless service coordinating `PlanningAdviceGrant` reservations, async read-only headless
/// execution, architect advice reuse, and `ScopedWorkflow` plan drafting/publishing.
#[derive(Debug, Clone)]
pub struct CoworkerPlanningService {
    store: CoworkerWorkflowStore,
    registry_dir: PathBuf,
    artifacts_dir: PathBuf,
}

impl CoworkerPlanningService {
    pub fn new(
        store: CoworkerWorkflowStore,
        registry_dir: impl Into<PathBuf>,
        artifacts_dir: impl Into<PathBuf>,
    ) -> Result<Self, NativeError>;

    pub fn store(&self) -> &CoworkerWorkflowStore;

    /// Checks `find_reusable_architect_advice` first; if no exact dependency match exists,
    /// verifies `context_dependencies.project_base` belongs to the standing grant's canonical
    /// repository, resolves the `Architect` model via A1 `preflight`, reserves grant budget before
    /// effect via `reserve_planning_call`, executes the headless worker asynchronously under the
    /// read-only capability policy, verifies post-run repository cleanliness as additional
    /// evidence, finalizes the reservation, and records the `ArchitectAdviceRecord`.
    pub async fn obtain_architect_advice(
        &self,
        request: &RunArchitectAdviceRequest,
    ) -> Result<ArchitectAdviceOutcome, NativeError>;

    /// Verifies `WorkBrief::is_ready_for_execution_planning()` and that `call_project_base`
    /// belongs to the standing grant's canonical repository, reserves an `Orchestrator` call under
    /// `grant_id`, executes the headless Orchestrator asynchronously, validates structured
    /// Orchestrator output against `call_project_base` and confirmed requirements (ensuring
    /// unapproved proposals never become executable scope), and returns a structured
    /// `OrchestrateWorkBriefPlanOutcome`.
    pub async fn orchestrate_brief_to_plan(
        &self,
        workflow: &ScopedWorkflow,
        request: &OrchestrateWorkBriefPlanRequest,
    ) -> Result<OrchestrateWorkBriefPlanOutcome, NativeError>;
}
```

---

### 3.2 Writer 3: `crates/threadlane-native/src/coworker_workflow/controller.rs`

**Responsibility**: Automatic routine task/review/in-scope-fix/dependency scheduling, outbox
receipt reconciliation, and conversation-change hold synchronization, governed by an explicit
`FrozenControllerCapabilityEnvelope` on `FrozenApprovalLinkage` (`policy_digest` or a saved
`WorkflowProfile` by itself never grants authority for `Reviewer`, `Closure`, automatic in-scope
corrections, verification check commands, or local Git integration).

**Frozen public types and signatures**:

```rust
use std::path::PathBuf;
use threadlane_authority::AuthorityStore;
use threadlane_host::ApprovedHostClient;
use crate::conversation_change::ConversationChangeStore;
use crate::coworker_workflow::{
    AcceptedPredecessorReceiptRef, CoworkerJobRecord, CoworkerTaskStage, CoworkerWorkflowStore,
    OutboxDispatchRecord, StructuredReviewVerdict, TaskCandidateRef, WorkflowHoldRecord,
};
use crate::error::NativeError;

/// Single deterministic scheduling action taken or proposed during a controller tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerStepAction {
    /// Synchronized an unresolved blocking `ConversationChangeRecord` into a `WorkflowHoldRecord`.
    AppliedConversationHold { hold_id: String },
    /// Prepared and sealed upstream dependency inputs for a child task.
    PreparedTaskDependencies {
        task_id: String,
        dependency_input_commit: String,
        dependency_input_tree_sha: String,
    },
    /// Reserved outbox and dispatched a routine task coding attempt (`ReadyToDispatch` or `CorrectInScopeRequired`).
    DispatchedTaskAttempt {
        task_id: String,
        attempt_no: u32,
        outbox_id: String,
        run_id: String,
        worker_id: String,
    },
    /// Observed a completed task attempt in `ReadyForReview` and dispatched structured review.
    DispatchedTaskReview {
        task_id: String,
        attempt_no: u32,
        outbox_id: String,
    },
    /// Recorded a `StructuredReviewVerdict` and transitioned the task (`Accepted`, `CorrectInScopeRequired`, `Held`, or `Blocked`).
    RecordedReviewVerdict {
        task_id: String,
        candidate_id: String,
        next_stage: CoworkerTaskStage,
    },
    /// Integrated an `Accepted` task candidate into the run integration worktree.
    IntegratedAcceptedTask {
        task_id: String,
        candidate_id: String,
        integration_receipt_id: String,
    },
    /// Halted scheduling on a task because an outbox record is `EffectUnknown` pending reconciliation.
    BlockedOnUnknownEffect {
        task_id: String,
        outbox_id: String,
    },
}

/// Summary report returned by a single bounded non-polling controller tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerTickReport {
    pub job: CoworkerJobRecord,
    pub actions: Vec<SchedulerStepAction>,
    pub all_tasks_integrated: bool,
}

/// Request to reconcile an `EffectUnknown` outbox record against live host and receipt stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileJobOutboxRequest {
    pub binding_id: String,
    pub thread_uuid: String,
    pub job_id: String,
    pub expected_job_revision: u64,
    pub expected_ledger_revision: Option<u64>,
    pub outbox_id: String,
}

/// Bounded event-driven scheduler controller for routine Coworker task execution, review,
/// in-scope fixes, dependency sealing, and hold synchronization.
#[derive(Debug, Clone)]
pub struct CoworkerSchedulerController {
    store: CoworkerWorkflowStore,
    conversation_changes: ConversationChangeStore,
    sessions_dir: PathBuf,
}

impl CoworkerSchedulerController {
    pub fn new(
        store: CoworkerWorkflowStore,
        conversation_changes: ConversationChangeStore,
        sessions_dir: impl Into<PathBuf>,
    ) -> Result<Self, NativeError>;

    pub fn store(&self) -> &CoworkerWorkflowStore;

    /// Inspects unresolved blocking records in `ConversationChangeStore` for `(binding_id, thread_uuid)`
    /// and records corresponding `WorkflowHoldRecord`s via `apply_workflow_hold` so affected tasks
    /// pause while provably independent tasks (`prove_task_independence`) may continue.
    pub fn sync_conversation_holds(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<Vec<WorkflowHoldRecord>, NativeError>;

    /// Reconciles an uncertain (`EffectUnknown` or `ReservedBeforeEffect`) `OutboxDispatchRecord`
    /// against `ApprovedHostClient` task snapshots and persisted review/integration receipts
    /// without blind redispatch.
    pub async fn reconcile_outbox_record(
        &self,
        host: &ApprovedHostClient,
        request: &ReconcileJobOutboxRequest,
    ) -> Result<(CoworkerJobRecord, OutboxDispatchRecord), NativeError>;

    /// Executes one bounded, non-polling scheduling pass over `job_id`:
    /// 1. Synchronizes conversation holds (`sync_conversation_holds`).
    /// 2. Refuses automatic replay for any task in `EffectUnknown`.
    /// 3. Seals dependency inputs for `PendingDependencies` tasks whose upstream `depends_on`
    ///    tasks are all `Integrated`.
    /// 4. Dispatches `ReadyToDispatch` and `CorrectInScopeRequired` tasks that pass
    ///    `prove_task_independence` via `reserve_task_outbox` + `ApprovedHostClient::start_task`.
    /// 5. Advances `ReadyForReview` tasks through structured review and `Accepted` tasks through
    ///    integration.
    pub async fn step_job(
        &self,
        authority: &AuthorityStore,
        host: &ApprovedHostClient,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<SchedulerTickReport, NativeError>;
}
```

---

### 3.3 Writer 4: `crates/threadlane-native/src/coworker_workflow/candidate.rs`

**Responsibility**: Verified combined-candidate assembly, check evidence verification, and
single-closure package reservation binding over `RoleFlowStore`.

**Frozen public types and signatures**:

```rust
use std::path::PathBuf;
use threadlane_authority::AuthorityStore;
use crate::coworker_workflow::{
    CandidateCheckEvidence, CombinedCandidateIdentity, CoworkerJobRecord, CoworkerWorkflowStore,
};
use crate::error::NativeError;
use crate::role_flow::{PackageClosureReservation, RoleFlowStore};

/// Request to assemble a verified `CombinedCandidateIdentity` and optionally reserve single-closure review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembleVerifiedCandidateRequest {
    pub binding_id: String,
    pub thread_uuid: String,
    pub job_id: String,
    pub expected_job_revision: u64,
    pub expected_ledger_revision: Option<u64>,
    pub request_key: String,
    pub final_commit_sha: Option<String>,
    pub final_tree_sha: String,
    pub check_evidence: Vec<CandidateCheckEvidence>,
    pub reserve_package_closure: bool,
    pub role_flow_cycle_id: Option<String>,
    pub closure_task_id: Option<String>,
    pub closure_summary: Option<String>,
}

/// Result of assembling a verified `CombinedCandidateIdentity` and optional `PackageClosureReservation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssembleVerifiedCandidateOutcome {
    pub job: CoworkerJobRecord,
    pub combined_candidate: CombinedCandidateIdentity,
    pub closure_reservation: Option<PackageClosureReservation>,
}

/// Service assembling verified combined candidates from integrated task receipts and binding
/// single-closure package review reservations.
#[derive(Debug, Clone)]
pub struct CoworkerCandidateService {
    store: CoworkerWorkflowStore,
    role_flow: RoleFlowStore,
    registry_dir: PathBuf,
}

impl CoworkerCandidateService {
    pub fn new(
        store: CoworkerWorkflowStore,
        role_flow: RoleFlowStore,
        registry_dir: impl Into<PathBuf>,
    ) -> Result<Self, NativeError>;

    pub fn store(&self) -> &CoworkerWorkflowStore;

    pub fn role_flow(&self) -> &RoleFlowStore;

    /// Verifies that:
    /// 1. All approved tasks on `job_id` are in `CoworkerTaskStage::Integrated` with sealed
    ///    `review_receipt_id` and `integration_receipt_id`.
    /// 2. `final_tree_sha` resolves in the live canonical repository (and matches `final_commit_sha`
    ///    when supplied).
    /// 3. All `CandidateCheckEvidence` items pass (`passed == true`, `exit_code == 0`).
    /// 4. If `reserve_package_closure` is `true`, reserves a single-closure
    ///    `PackageClosureReservation` in `RoleFlowStore` (`ClosureSubjectSource::ExplicitFinalTree`)
    ///    and binds `closure_reservation_id` into `CombinedCandidateIdentity` via
    ///    `CoworkerWorkflowStore::record_combined_candidate`.
    pub fn assemble_verified_candidate(
        &self,
        authority: &AuthorityStore,
        request: &AssembleVerifiedCandidateRequest,
    ) -> Result<AssembleVerifiedCandidateOutcome, NativeError>;
}
```

---

### 3.4 Stage 5 Integration Writer Ownership and Hooks

The later integration writer owns:
1. `crates/threadlane-native/src/coworker_workflow/mod.rs`: adding `pub mod planning;`,
   `pub mod controller;`, and `pub mod candidate;` plus `pub use` re-exports once Writers 2–4
   complete.
2. `crates/threadlane-native/src/lib.rs`: top-level re-exports of the Coworker workflow types and
   services.
3. `crates/threadlane-native/src/mcp_pair.rs` and CLI/desktop operator glue: wiring scoped MCP tools
   and trusted-local operator CLI subcommands to `CoworkerWorkflowStore`, `CoworkerPlanningService`,
   `CoworkerSchedulerController`, and `CoworkerCandidateService`.
4. `crates/threadlane-native/Cargo.toml`: if `controller.rs` or `candidate.rs` invokes
   `threadlane-operations` Rust functions directly (e.g. `prepare_task_dependencies`,
   `evaluate_review_subject`, `record_review_verdict`, `integrate_accepted_task`) rather than
   delegating through existing host/CLI receipts, add `threadlane-operations.workspace = true` to
   `crates/threadlane-native/Cargo.toml`.

---

## 4. Test Acceptance Cases

### 4.1 Stage 1 Foundation Acceptance Cases (`crates/threadlane-native/tests/coworker_workflow_contract.rs`)

1. **`native_coworker_rejected_as_headless_worker_or_grant_model_and_provenance_honest`**
   - Rejects `WorkflowRole::Follower`, `RoleExecutionRoute::NativeHandoff`, and headless
     `CodexCli` choices without an explicit validated reasoning effort. The original Stage 1
     boundary admitted Codex CLI only for normalized read-only advisory roles (`Orchestrator`,
     `Architect`, `Reviewer`, `Closure`); the later complete Codex Coder contract described above
     adds its separately approved route without widening historical native approvals.
   - Rejects `PlanningAdviceGrant` with `operator_attested: false`, write/mutation capabilities,
     or policies permitting `Coder`/`Reviewer`/`Follower`.
   - Enforces `native_per_call_identity_verified == false`.
2. **`work_brief_validation_digest_readiness_and_amendment_holds_affected_tasks`**
   - Verifies deterministic `WorkBrief::digest()` and separates `is_ready_for_execution_planning()`
     from `is_ready_for_execution_approval()` when `open_questions` or `proposed_additions` are
     present.
   - Verifies `WorkBriefKind::Amendment` rejects stale `target_job_revision` and places affected
     tasks on the target job into `CoworkerTaskStage::Held` while leaving tasks with complete
     disjoint dependency/write/interface declarations in `ReadyToDispatch`.
3. **`planning_grant_reserves_budget_before_effect_and_handles_refusal_uncertainty_expiry_revocation`**
   - Reserves budget durably before effect (`ReservedBeforeEffect`).
   - Releases budget on `RefusedBeforeEffect` only when `run_id` is `None`.
   - Retains consumed budget on `EffectUnknown` and blocks automatic replay with
     `NativeErrorCode::AmbiguousWakeupPendingReconciliation` until reconciled.
   - Enforces per-role and total call budgets, expiry, and explicit revocation.
4. **`architect_advice_reuse_requires_exact_context_dependencies_never_text_similarity`**
   - Reuses `ArchitectAdviceRecord` when `ArchitectContextDependencies` and `context_digest` are
     unchanged.
   - Refuses reuse when any file blob digest, interface ownership digest, predecessor tree, brief
     digest, or policy digest changes even when the decision summary text is identical.
5. **`immutable_approval_linkage_routine_outbox_verdict_and_combined_candidate_lifecycle`**
   - Freezes `FrozenApprovalLinkage` from `AuthorityStore` + `ExecutionPolicy` and rejects any
     attempt to rebind a different approval or policy to the same job.
   - Enforces dependency preparation before dependent task dispatch.
   - Exercises reserve-before-effect outbox, `EffectUnknown` replay blocking, and reconciliation.
   - Verifies `StructuredReviewVerdict` outcomes (`CorrectInScope` vs `Accept`) and proves positive
     reviewer prose never advances acceptance.
   - Verifies `CombinedCandidateIdentity` requires all tasks `Integrated` and all
     `CandidateCheckEvidence` passing (`passed == true`, `exit_code == 0`), with order-invariant
     deterministic `candidate_digest`.
6. **`workflow_holds_and_task_independence_proof_block_overlapping_or_dependent_tasks_only`**
   - Verifies `apply_workflow_hold` and `prove_task_independence` hold directly targeted tasks,
     transitive `depends_on` dependents, overlapping write scopes (including directory prefixes),
     and overlapping `owned_interfaces`/`consumed_interfaces`, while allowing tasks with complete
     disjoint declarations to reserve and dispatch.
   - Verifies `resolve_workflow_hold` restores held tasks to their prior legal stages.
7. **`store_cas_stale_revision_conflicting_duplicate_isolation_and_corrupt_ledger_preservation`**
   - Verifies pure read-only snapshot never creates files or lockfiles.
   - Verifies idempotent retry vs conflicting duplicate `request_key` rejection.
   - Verifies stale `expected_ledger_revision` and cross-thread / cross-binding isolation.
   - Verifies corrupt JSON, unknown fields (`deny_unknown_fields`), and unsupported
     `schema_version` fail closed with `ProtocolError` while preserving on-disk bytes untouched.
8. **`work_brief_allows_open_questions_and_unapproved_proposals_into_planning_while_blocking_execution_approval`**
   - Proves a valid `WorkBrief` with confirmed requirements plus technical `open_questions` and/or
     unapproved `proposed_additions` reaches `CoworkerJobPhase::Planning` as planner context.
   - Proves `proposed_additions` cannot duplicate or silently become `confirmed_requirements` and
     strictly block `bind_job_approval` (`is_ready_for_execution_approval() == false`).
9. **`standing_planning_grant_allows_same_repo_commit_progression_and_rejects_cross_repository_context`**
   - Proves a standing `PlanningAdviceGrant` allows reservations and `ArchitectAdviceRecord`
     recording across normal commit progression in the same canonical repository without requiring
     a new human grant, preserves backwards-compatible `call_project_base: None` defaults, and
     rejects cross-repository context with `NativeErrorCode::SecurityViolation`.
10. **`task_independence_requires_complete_declared_evidence_and_conservatively_holds_when_interface_impact_incomplete`**
    - Proves disjoint file `write_paths` plus incomplete task or hold interface declarations
      (`complete_declared_evidence == false`) conservatively hold the task (`independent == false`)
      and never release it during an active hold.
11. **`frozen_approval_linkage_requires_explicit_controller_capability_envelope_for_reviewer_closure_fixes_checks_and_git`**
    - Proves `FrozenApprovalLinkage.policy_digest` (and a saved `WorkflowProfile`) alone grants no
      authority for `Reviewer`, `Closure`, automatic in-scope corrections, check commands, or local
      Git integration when `controller_envelope` is `None`, and that an explicit
      `FrozenControllerCapabilityEnvelope` grants only the exact frozen capabilities permitted by
      the `ExecutionPolicy`.

### 4.2 Upcoming Writer Acceptance Cases (Stages 2–4)

- **Writer 2 (`planning.rs`)**:
  - `planning_service_reuses_architect_advice_without_spending_grant_budget_when_deps_match`
  - `planning_service_spends_grant_budget_and_rejects_dirty_worktree_or_expired_grant`
  - `planning_service_orchestrates_ready_brief_to_published_plan_and_refuses_unready_brief`
- **Writer 3 (`controller.rs`)**:
  - `controller_steps_root_and_dependent_tasks_through_review_in_scope_fix_and_integration`
  - `controller_halts_on_unknown_outbox_effect_until_receipt_reconciled`
  - `controller_syncs_conversation_changes_into_holds_and_advances_only_independent_tasks`
- **Writer 4 (`candidate.rs`)**:
  - `candidate_service_verifies_live_final_tree_and_binds_single_closure_reservation`
  - `candidate_service_rejects_unintegrated_task_or_failed_check_or_second_closure`
