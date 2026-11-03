//! Call-bound Activity journal observation helpers for the native Coworker workflow runtime.
//!
//! ## Core Invariants
//!
//! - **Reuses `threadlane_core::observation`**: Appends [`ObservationJournalRecord`] entries
//!   directly to the private (`0700`) bounded `observation-journal.jsonl` ledger via
//!   [`append_journal_record`]. Introduces no parallel telemetry store or external provider.
//! - **Real execution boundaries only**: Callers emit one [`ObservationRecordPhase::Entry`]
//!   immediately before a real headless worker, verification command, or Git integration boundary
//!   and one terminal [`ObservationRecordPhase::Finish`] immediately after that boundary resolves.
//!   Read-only status queries, lease heartbeats, dependency-cache hits, and idempotent replays
//!   never emit journal records.
//! - **Deterministic request and call correlation**: `request_id` (`obsreq_<32hex>`), `call_id`
//!   (`obsc_<32hex>`), and per-phase `record_id` (`obsrec_<32hex>`) are derived deterministically
//!   from `(operation, binding_id, thread_uuid, call_key)`. Coalescing guards prevent duplicate
//!   `Entry` or `Finish` rows for the same call identity.
//! - **Truthful actor, model, and usage provenance**:
//!   - `actor` uses a scope-verified [`VerifiedMcpObservationContext`] when available for the
//!     exact `(binding_id, thread_uuid)`; otherwise it records an honest `coworker` actor with
//!     [`ObservationActorEvidence::Unknown`] and never infers the active Codex conversational
//!     model from a saved [` crate::WorkflowProfile`].
//!   - `requested_model` and `requested_effort` come from the frozen role choice or resolved
//!     worker target.
//!   - `observed_model` and `observed_effort` are `None` at `Entry` and populated at `Finish`
//!     exclusively from actual runtime/provider [`WorkerExecutionReport`] evidence.
//!   - `usage` is `None` unless genuinely reported by a runtime/provider; zeroes and estimates
//!     are never fabricated.
//! - **Secret and prompt redaction**: Full worker reports, raw prompts, stdout/stderr log bodies,
//!   and secret tokens are never written to the observation journal.
//! - **Non-authoritative best-effort recording**: Observation journal failures never fabricate
//!   worker success, alter accepted code or receipts, or grant execution authority.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};

use threadlane_core::observation::{
    append_journal_record, current_observation_time_ms, current_process_instance,
    read_journal_records_readonly, sanitize_observation_label, sanitize_observation_summary,
    validate_correlation_id, ObservationActor, ObservationActorEvidence, ObservationCallState,
    ObservationExecutor, ObservationJournalRecord, ObservationProcessInstance,
    ObservationRecordPhase, ObservationUsage, VerifiedMcpObservationContext,
};
use threadlane_workers::{
    FieldEvidence, ResolvedModelTarget, WorkerExecutionReport, WorkerProvider, WorkerRunStatus,
};
use threadlane_workflow::{ToolAdapter, WorkflowRole};

use crate::coworker_workflow::{
    deterministic_prefixed_id, ControllerPolicyStore, CoworkerWorkflowStore,
};
use crate::profile_flow::SelectedRoleChoice;

/// Canonical observation operation name for headless `Orchestrator` technical planning calls.
pub const COWORKER_OBS_OP_PLANNING_ORCHESTRATOR: &str = "coworker.planning.orchestrator";

/// Canonical observation operation name for read-only `Architect` consultation calls.
pub const COWORKER_OBS_OP_PLANNING_ARCHITECT: &str = "coworker.planning.architect";

/// Canonical observation operation name for per-task frozen verification check commands.
pub const COWORKER_OBS_OP_CHECKS_TASK: &str = "coworker.checks.task";

/// Canonical observation operation name for headless `Reviewer` task candidate evaluations.
pub const COWORKER_OBS_OP_REVIEW_TASK: &str = "coworker.review.task";

/// Canonical observation operation name for derived `Coder` in-scope correction attempts.
pub const COWORKER_OBS_OP_CODER_CORRECTION: &str = "coworker.coder.correction";

/// Canonical observation operation name for per-task `RunIntegration` Git operations.
pub const COWORKER_OBS_OP_INTEGRATION_TASK: &str = "coworker.integration.task";

/// Canonical observation operation name for final multi-worker `JobAggregation` Git operations.
pub const COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION: &str =
    "coworker.integration.job_aggregation";

/// Canonical observation operation name for combined-candidate verification check commands.
pub const COWORKER_OBS_OP_CHECKS_CANDIDATE: &str = "coworker.checks.candidate";

/// Canonical observation operation name for final combined-candidate `Closure` evaluations.
pub const COWORKER_OBS_OP_CLOSURE_CANDIDATE: &str = "coworker.closure.candidate";

/// Returns `true` when `operation` is one of the canonical Coworker workflow stage operations.
#[must_use]
pub fn is_coworker_workflow_operation(operation: &str) -> bool {
    matches!(
        operation.trim(),
        COWORKER_OBS_OP_PLANNING_ORCHESTRATOR
            | COWORKER_OBS_OP_PLANNING_ARCHITECT
            | COWORKER_OBS_OP_CHECKS_TASK
            | COWORKER_OBS_OP_REVIEW_TASK
            | COWORKER_OBS_OP_CODER_CORRECTION
            | COWORKER_OBS_OP_INTEGRATION_TASK
            | COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION
            | COWORKER_OBS_OP_CHECKS_CANDIDATE
            | COWORKER_OBS_OP_CLOSURE_CANDIDATE
    )
}

tokio::task_local! {
    static CURRENT_VERIFIED_ENROLLMENT_CONTEXT: Option<VerifiedMcpObservationContext>;
}

/// Scopes an optional [`VerifiedMcpObservationContext`] to the execution of `future` via Tokio
/// task-local storage so async tasks migrating across OS worker threads preserve exact call
/// attribution without cross-task leakage or wrong-thread drop restoration.
///
/// Even when scoped, the context is only used for observation records whose verified
/// `(binding_id, thread_uuid)` matches `(context.binding_id, context.thread_uuid)` exactly.
/// Newly spawned tasks (`tokio::spawn`) and child OS processes do not inherit this task-local
/// scope automatically, and task-local scoping remains separate from actual verified caller
/// origin—production paths that do not pass an explicit verified context continue to record
/// [`ObservationActorEvidence::Unknown`] and gain no authority.
pub async fn with_verified_coworker_observation_context<F: Future>(
    context: Option<VerifiedMcpObservationContext>,
    future: F,
) -> F::Output {
    CURRENT_VERIFIED_ENROLLMENT_CONTEXT
        .scope(context, future)
        .await
}

/// Reads the current Tokio task's [`VerifiedMcpObservationContext`] if and only if one is active
/// in task-local scope and its `(binding_id, thread_uuid)` matches the requested scope.
///
/// Returns `None` without panicking when called outside a
/// [`with_verified_coworker_observation_context`] scope or outside a Tokio runtime.
#[must_use]
pub fn current_verified_coworker_observation_context_for_scope(
    binding_id: &str,
    thread_uuid: &str,
) -> Option<VerifiedMcpObservationContext> {
    CURRENT_VERIFIED_ENROLLMENT_CONTEXT
        .try_with(|slot| {
            slot.as_ref()
                .filter(|ctx| ctx.binding_id == binding_id && ctx.thread_uuid == thread_uuid)
                .cloned()
        })
        .ok()
        .flatten()
}

/// Requested executor metadata frozen prior to a Coworker workflow call boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoworkerObservationExecutorSpec {
    /// Logical tool or check command identifier (e.g. `tool-agy`, `cargo_test`, `git`).
    pub tool: Option<String>,
    /// Execution provider or subsystem label (e.g. `antigravity`, `opencode_v2`, `local_check`, `local_git`).
    pub provider: Option<String>,
    /// Canonical lowercase workflow role or stage role (`orchestrator`, `architect`, `coder`, `reviewer`, `closure`, `verification`, `integration`).
    pub role: Option<String>,
    /// Frozen requested model identifier or CLI model argument (`None` for non-model checks/Git).
    pub requested_model: Option<String>,
    /// Frozen requested effort or variant (`None` when not requested or for non-model stages).
    pub requested_effort: Option<String>,
}

impl CoworkerObservationExecutorSpec {
    /// Builds executor metadata from a frozen [`SelectedRoleChoice`] and [`WorkflowRole`].
    #[must_use]
    pub fn from_selected_role_choice(role: WorkflowRole, choice: &SelectedRoleChoice) -> Self {
        Self {
            tool: sanitize_optional_label(&choice.tool_id),
            provider: Some(adapter_provider_name(choice.adapter).to_string()),
            role: Some(role_name(role).to_string()),
            requested_model: sanitize_optional_label(&choice.model_arg),
            requested_effort: choice.effort.as_deref().and_then(sanitize_optional_label),
        }
    }

    /// Builds executor metadata from a [`WorkerProvider`], [`WorkflowRole`], optional `tool_id`,
    /// and [`ResolvedModelTarget`].
    #[must_use]
    pub fn from_resolved_model_target(
        role: WorkflowRole,
        provider: WorkerProvider,
        tool_id: Option<&str>,
        target: &ResolvedModelTarget,
    ) -> Self {
        Self {
            tool: tool_id.and_then(sanitize_optional_label),
            provider: Some(provider.as_str().to_string()),
            role: Some(role_name(role).to_string()),
            requested_model: sanitize_optional_label(&target.base_model),
            requested_effort: target.effort.as_deref().and_then(sanitize_optional_label),
        }
    }

    /// Builds executor metadata for a frozen local verification check command (`checks.rs`).
    #[must_use]
    pub fn local_verification_check(check_id: &str) -> Self {
        Self {
            tool: sanitize_optional_label(check_id),
            provider: Some("local_check".to_string()),
            role: Some("verification".to_string()),
            requested_model: None,
            requested_effort: None,
        }
    }

    /// Builds executor metadata for a local Git integration stage (`RunIntegration` or `JobAggregation`).
    #[must_use]
    pub fn local_git_integration() -> Self {
        Self {
            tool: Some("git".to_string()),
            provider: Some("local_git".to_string()),
            role: Some("integration".to_string()),
            requested_model: None,
            requested_effort: None,
        }
    }

    /// Converts this spec into an [`ObservationExecutor`] with optional runtime-observed model and effort.
    #[must_use]
    pub fn to_observation_executor(
        &self,
        observed_model: Option<&str>,
        observed_effort: Option<&str>,
    ) -> ObservationExecutor {
        ObservationExecutor {
            tool: self.tool.clone(),
            provider: self.provider.clone(),
            role: self.role.clone(),
            requested_model: self.requested_model.clone(),
            observed_model: observed_model.and_then(sanitize_optional_label),
            requested_effort: self.requested_effort.clone(),
            observed_effort: observed_effort.and_then(sanitize_optional_label),
        }
    }
}

/// Extracts `(observed_model, observed_effort)` exclusively from actual [`WorkerExecutionReport`]
/// runtime/provider evidence.
///
/// Never falls back to `report.requested_model`.
#[must_use]
pub fn extract_observed_model_and_effort(
    report: &WorkerExecutionReport,
) -> (Option<String>, Option<String>) {
    let observed_model = if let Some(meta) = report.validated_opencode_metadata() {
        sanitize_optional_label(&meta.observed_model)
    } else {
        extract_field_evidence_value(&report.evidence.model_evidence)
            .as_deref()
            .and_then(sanitize_optional_label)
    };

    let observed_effort = if let Some(meta) = report.validated_opencode_metadata() {
        meta.observed_effort
            .as_deref()
            .and_then(sanitize_optional_label)
            .or_else(|| {
                extract_field_evidence_value(&report.evidence.effort_evidence)
                    .as_deref()
                    .and_then(sanitize_optional_label)
            })
    } else {
        extract_field_evidence_value(&report.evidence.effort_evidence)
            .as_deref()
            .and_then(sanitize_optional_label)
    };

    (observed_model, observed_effort)
}

/// Maps a [`WorkerRunStatus`] to an [`ObservationCallState`] when a worker call did not complete
/// with a verified accepted artifact.
#[must_use]
pub const fn map_worker_run_status_to_call_state(status: WorkerRunStatus) -> ObservationCallState {
    match status {
        WorkerRunStatus::Succeeded | WorkerRunStatus::MissingEffortEvidence => {
            ObservationCallState::Completed
        }
        WorkerRunStatus::Cancelled => ObservationCallState::Cancelled,
        WorkerRunStatus::IncompleteCleanup => ObservationCallState::Unknown,
        WorkerRunStatus::Blocked
        | WorkerRunStatus::ProviderError
        | WorkerRunStatus::ModelMismatch
        | WorkerRunStatus::MissingModelEvidence
        | WorkerRunStatus::EffortMismatch
        | WorkerRunStatus::SessionMismatch
        | WorkerRunStatus::MissingResultEvidence
        | WorkerRunStatus::MalformedStream
        | WorkerRunStatus::OutputOverflow
        | WorkerRunStatus::TimedOut
        | WorkerRunStatus::NonZeroExit => ObservationCallState::Failed,
    }
}

fn sanitize_optional_label(raw: &str) -> Option<String> {
    let clean = sanitize_observation_label(raw, "");
    if clean.is_empty() {
        None
    } else {
        Some(clean)
    }
}

fn extract_field_evidence_value(evidence: &FieldEvidence) -> Option<String> {
    match evidence {
        FieldEvidence::VerifiedMatch { value } => Some(value.clone()),
        FieldEvidence::Mismatch { observed, .. } => Some(observed.clone()),
        FieldEvidence::Missing { .. } => None,
        FieldEvidence::NotRequested { observed } => observed.clone(),
    }
}

const fn role_name(role: WorkflowRole) -> &'static str {
    match role {
        WorkflowRole::Follower => "follower",
        WorkflowRole::Orchestrator => "orchestrator",
        WorkflowRole::Architect => "architect",
        WorkflowRole::Coder => "coder",
        WorkflowRole::Reviewer => "reviewer",
        WorkflowRole::Closure => "closure",
    }
}

const fn adapter_provider_name(adapter: ToolAdapter) -> &'static str {
    match adapter {
        ToolAdapter::Antigravity => "antigravity",
        ToolAdapter::OpencodeV2 => "opencode_v2",
        ToolAdapter::CodexCli => "codex",
        ToolAdapter::JevApi => "jev_api",
        ToolAdapter::OpenaiDecisionsApi => "openai_decisions_api",
        ToolAdapter::Git => "git",
        ToolAdapter::Tmux => "tmux",
        ToolAdapter::Codegraph => "codegraph",
    }
}

/// Resolves the private (`0700`) directory where `observation-journal.jsonl` should be written for
/// a Coworker workflow call.
///
/// Prefers an explicit `plans_dir` when supplied; otherwise inspects `lock_root_dir` and sibling
/// store directories derived from `lock_root_dir` / `work_area_root` for the binding's workflow
/// ledger (`coworker-workflow-<tag>.json`) or `plans.json`.
#[must_use]
pub fn resolve_coworker_journal_dir(
    explicit_plans_dir: Option<&Path>,
    lock_root_dir: Option<&Path>,
    work_area_root: Option<&Path>,
    binding_id: &str,
) -> Option<PathBuf> {
    if let Some(dir) = explicit_plans_dir {
        if is_existing_private_dir(dir) {
            return Some(dir.to_path_buf());
        }
    }

    let mut base_roots = Vec::new();
    if let Some(lock_dir) = lock_root_dir {
        base_roots.push(lock_dir.to_path_buf());
        if let Some(parent) = lock_dir.parent() {
            base_roots.push(parent.to_path_buf());
        }
    }
    if let Some(work_area) = work_area_root {
        if let Some(session_dir) = work_area.parent() {
            if let Some(sessions_root) = session_dir.parent() {
                base_roots.push(sessions_root.to_path_buf());
                if let Some(workspace_root) = sessions_root.parent() {
                    base_roots.push(workspace_root.to_path_buf());
                }
            }
        }
    }

    let mut candidates = Vec::new();
    for root in &base_roots {
        candidates.push(root.clone());
        candidates.push(root.join("plans"));
        candidates.push(root.join("plans-store"));
    }

    for candidate in &candidates {
        if !is_existing_private_dir(candidate) {
            continue;
        }
        if let Ok(store) = CoworkerWorkflowStore::new(candidate) {
            if let Ok(ledger_path) = store.ledger_path_for_binding(binding_id) {
                if ledger_path.is_file() {
                    return Some(candidate.clone());
                }
            }
        }
        if candidate.join("plans.json").is_file() {
            return Some(candidate.clone());
        }
    }

    candidates
        .into_iter()
        .find(|dir| is_existing_private_dir(dir))
}

fn is_existing_private_dir(path: &Path) -> bool {
    if !path.is_absolute() {
        return false;
    }
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o777) != 0o700 {
            return false;
        }
    }
    true
}

/// Deterministic `(request_id, call_id)` pair for a Coworker workflow stage call.
#[must_use]
pub fn derive_coworker_observation_ids(
    operation: &str,
    binding_id: &str,
    thread_uuid: &str,
    call_key: &str,
) -> (String, String) {
    let request_id = deterministic_prefixed_id(
        "threadlane-native:coworker-obs-request:v1",
        "obsreq",
        &[operation, binding_id, thread_uuid, call_key],
    );
    let call_id = deterministic_prefixed_id(
        "threadlane-native:coworker-obs-call:v1",
        "obsc",
        &[operation, binding_id, thread_uuid, call_key],
    );
    (request_id, call_id)
}

/// Input specification for recording the [`ObservationRecordPhase::Entry`] of a real Coworker
/// workflow execution boundary.
#[derive(Debug, Clone)]
pub struct CoworkerCallObservationSpec {
    /// Private (`0700`) directory where `observation-journal.jsonl` resides (typically `plans_dir`).
    pub journal_dir: PathBuf,
    /// Canonical operation identifier (`coworker.*`).
    pub operation: String,
    /// Human-readable call title (redacted and bounded before storage).
    pub title: String,
    /// Stable reservation, effect, or call key uniquely identifying this execution boundary.
    pub call_key: String,
    /// Verified control-plane chat `binding_id`.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated native Codex `thread_uuid`.
    pub thread_uuid: String,
    /// Verified canonical project repository path.
    pub repository_path: PathBuf,
    /// Verified `CoworkerJobRecord::job_id` when bound to a job.
    pub job_id: Option<String>,
    /// Verified workflow `plan_id` when bound to a plan.
    pub plan_id: Option<String>,
    /// Verified `task_id` when bound to a specific task.
    pub task_id: Option<String>,
    /// Verified `ApprovalRecord::approval_id` when bound to an execution approval.
    pub approval_id: Option<String>,
    /// Verified 1-based task `attempt_no` when applicable.
    pub attempt_no: Option<u32>,
    /// Verified worker `run_id` when applicable.
    pub run_id: Option<String>,
    /// Verified worker `worker_id` when applicable.
    pub worker_id: Option<String>,
    /// Verified `candidate_id` when applicable.
    pub candidate_id: Option<String>,
    /// Frozen executor and requested model/effort metadata.
    pub executor: CoworkerObservationExecutorSpec,
    /// Optional explicit verified MCP enrollment context for `(binding_id, thread_uuid)`.
    pub verified_context: Option<VerifiedMcpObservationContext>,
    /// Optional safe entry summary detail.
    pub entry_summary: Option<String>,
}

/// Input specification for recording the terminal [`ObservationRecordPhase::Finish`] of a
/// Coworker workflow execution boundary.
#[derive(Debug, Clone, Default)]
pub struct CoworkerCallFinishSpec {
    /// Terminal [`ObservationCallState`] (`Completed`, `Failed`, `Cancelled`, or `Unknown`).
    pub state: Option<ObservationCallState>,
    /// Runtime-observed model identifier from [`WorkerExecutionReport`], if genuinely observed.
    pub observed_model: Option<String>,
    /// Runtime-observed effort/variant from [`WorkerExecutionReport`], if genuinely observed.
    pub observed_effort: Option<String>,
    /// Token usage if and only if genuinely reported by the runtime/provider (`None` otherwise).
    pub usage: Option<ObservationUsage>,
    /// Verified `job_id` if resolved or confirmed during execution.
    pub job_id: Option<String>,
    /// Verified `plan_id` if resolved or published during execution.
    pub plan_id: Option<String>,
    /// Verified `task_id` if resolved during execution.
    pub task_id: Option<String>,
    /// Verified `approval_id` if resolved during execution.
    pub approval_id: Option<String>,
    /// Verified `attempt_no` if resolved during execution.
    pub attempt_no: Option<u32>,
    /// Verified `run_id` if resolved during execution.
    pub run_id: Option<String>,
    /// Verified `worker_id` if resolved during execution.
    pub worker_id: Option<String>,
    /// Verified `candidate_id` if resolved during execution.
    pub candidate_id: Option<String>,
    /// Verified receipt identifier (`rev_...`, `int_...`, `cva_...`, ` dtr_...`) when produced.
    pub receipt_id: Option<String>,
    /// Verified persisted report artifact identifier (`pca_...`, `trr_...`, `fcr_...`, `dcr_...`, `cea_...`) when produced.
    pub artifact_id: Option<String>,
    /// Optional safe terminal diagnostic or status code (never raw prompts or log bodies).
    pub diagnostic: Option<String>,
}

/// Summary of a completed `Entry` + `Finish` observation pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoworkerCallObservationSummary {
    /// Deterministic request identifier (`obsreq_<32hex>`).
    pub request_id: String,
    /// Deterministic call identifier (`obsc_<32hex>`).
    pub call_id: String,
    /// Wall-clock start timestamp recorded at `Entry` (ms since Unix epoch).
    pub started_at_ms: i64,
    /// Wall-clock completion timestamp recorded at `Finish` (ms since Unix epoch).
    pub finished_at_ms: i64,
    /// Wall-clock duration (`finished_at_ms - started_at_ms`, `>= 0`).
    pub duration_ms: i64,
    /// Terminal [`ObservationCallState`] recorded at `Finish`.
    pub state: ObservationCallState,
}

/// Active handle returned by [`begin_coworker_call_observation`] prior to an execution boundary.
#[derive(Debug, Clone)]
pub struct CoworkerCallObservationHandle {
    journal_dir: PathBuf,
    request_id: String,
    call_id: String,
    operation: String,
    title: String,
    binding_id: String,
    thread_uuid: String,
    thread_label: Option<String>,
    project_label: Option<String>,
    project_path: Option<String>,
    job_id: Option<String>,
    plan_id: Option<String>,
    task_id: Option<String>,
    approval_id: Option<String>,
    attempt_no: Option<u32>,
    run_id: Option<String>,
    worker_id: Option<String>,
    candidate_id: Option<String>,
    actor: ObservationActor,
    executor: CoworkerObservationExecutorSpec,
    started_at_ms: i64,
    process_instance: Option<ObservationProcessInstance>,
}

impl CoworkerCallObservationHandle {
    /// Returns the deterministic `request_id` (`obsreq_<32hex>`) for this call.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Returns the deterministic `call_id` (`obsc_<32hex>`) for this call.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the wall-clock `Entry` timestamp (ms since Unix epoch).
    #[must_use]
    pub const fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    /// Records the terminal [`ObservationRecordPhase::Finish`] journal entry for this call.
    pub fn finish(self, spec: CoworkerCallFinishSpec) -> CoworkerCallObservationSummary {
        let finished_at_ms = current_observation_time_ms().max(self.started_at_ms);
        let duration_ms = finished_at_ms.saturating_sub(self.started_at_ms);
        let state = spec.state.unwrap_or(ObservationCallState::Completed);

        let job_id = spec
            .job_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.job_id);
        let plan_id = spec
            .plan_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.plan_id);
        let task_id = spec
            .task_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.task_id);
        let approval_id = spec
            .approval_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.approval_id);
        let attempt_no = spec.attempt_no.or(self.attempt_no);
        let run_id = spec
            .run_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.run_id);
        let worker_id = spec
            .worker_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.worker_id);
        let candidate_id = spec
            .candidate_id
            .as_deref()
            .and_then(validate_correlation_id)
            .or(self.candidate_id);
        let receipt_id = spec.receipt_id.as_deref().and_then(validate_correlation_id);
        let artifact_id = spec
            .artifact_id
            .as_deref()
            .and_then(validate_correlation_id);

        let executor = self.executor.to_observation_executor(
            spec.observed_model.as_deref(),
            spec.observed_effort.as_deref(),
        );
        let usage = normalize_genuine_usage(spec.usage);

        let summary = build_safe_scope_summary(
            &self.operation,
            "finish",
            state,
            job_id.as_deref(),
            task_id.as_deref(),
            attempt_no,
            run_id.as_deref(),
            worker_id.as_deref(),
            candidate_id.as_deref(),
            receipt_id.as_deref(),
            artifact_id.as_deref(),
            Some(duration_ms),
            spec.diagnostic.as_deref(),
        );

        let finish_record_id = deterministic_prefixed_id(
            "threadlane-native:coworker-obs-record:v1",
            "obsrec",
            &[&self.request_id, &self.call_id, "finish"],
        );

        let record = ObservationJournalRecord {
            record_id: finish_record_id.clone(),
            request_id: self.request_id.clone(),
            parent_request_id: None,
            call_id: Some(self.call_id.clone()),
            phase: ObservationRecordPhase::Finish,
            at_ms: finished_at_ms,
            pid: Some(std::process::id()),
            process_instance: self.process_instance.or_else(current_process_instance),
            project_label: self.project_label,
            project_path: self.project_path,
            binding_id: Some(self.binding_id),
            thread_uuid: Some(self.thread_uuid),
            thread_label: self.thread_label,
            job_id,
            plan_id,
            task_id,
            approval_id,
            operation: self.operation,
            title: self.title,
            state,
            actor: Some(self.actor),
            executor: Some(executor),
            usage,
            delivery: Vec::new(),
            summary,
        };

        append_coalesced_journal_record(&self.journal_dir, &record);

        CoworkerCallObservationSummary {
            request_id: self.request_id,
            call_id: self.call_id,
            started_at_ms: self.started_at_ms,
            finished_at_ms,
            duration_ms,
            state,
        }
    }
}

/// Records the [`ObservationRecordPhase::Entry`] journal row immediately before a real Coworker
/// workflow execution boundary and returns a [`CoworkerCallObservationHandle`] to complete it.
#[must_use]
pub fn begin_coworker_call_observation(
    spec: CoworkerCallObservationSpec,
) -> CoworkerCallObservationHandle {
    let started_at_ms = current_observation_time_ms();
    let process_instance = current_process_instance();

    let (request_id, call_id) = derive_coworker_observation_ids(
        &spec.operation,
        &spec.binding_id,
        &spec.thread_uuid,
        &spec.call_key,
    );
    let entry_record_id = deterministic_prefixed_id(
        "threadlane-native:coworker-obs-record:v1",
        "obsrec",
        &[&request_id, &call_id, "entry"],
    );

    let (enriched_job_id, enriched_plan_id, enriched_approval_id, enriched_task_id) =
        enrich_scope_from_workflow_store(&spec);

    let (project_path, project_label) = resolve_verified_project_metadata(&spec.repository_path);

    let verified_ctx = spec
        .verified_context
        .filter(|ctx| ctx.binding_id == spec.binding_id && ctx.thread_uuid == spec.thread_uuid)
        .or_else(|| {
            current_verified_coworker_observation_context_for_scope(
                &spec.binding_id,
                &spec.thread_uuid,
            )
        });

    let (actor, thread_label) = if let Some(ctx) = verified_ctx {
        (
            ObservationActor {
                label: ctx.actor.label,
                role: Some("coworker".to_string()),
                source: ctx.actor.source.or_else(|| Some("paired_mcp".to_string())),
                // Never infer the active Codex conversational model or effort from a profile.
                model: None,
                effort: None,
                evidence: ObservationActorEvidence::Configured,
            },
            sanitize_optional_label(&ctx.thread_label),
        )
    } else {
        (
            ObservationActor {
                label: Some("Coworker".to_string()),
                role: Some("coworker".to_string()),
                source: Some("coworker_workflow".to_string()),
                model: None,
                effort: None,
                evidence: ObservationActorEvidence::Unknown,
            },
            None,
        )
    };

    let clean_title = sanitize_observation_label(&spec.title, &spec.operation);
    let task_id = enriched_task_id;
    let run_id = spec.run_id.as_deref().and_then(validate_correlation_id);
    let worker_id = spec.worker_id.as_deref().and_then(validate_correlation_id);
    let candidate_id = spec
        .candidate_id
        .as_deref()
        .and_then(validate_correlation_id);

    let entry_executor = spec.executor.to_observation_executor(None, None);
    let summary = build_safe_scope_summary(
        &spec.operation,
        "entry",
        ObservationCallState::Running,
        enriched_job_id.as_deref(),
        task_id.as_deref(),
        spec.attempt_no,
        run_id.as_deref(),
        worker_id.as_deref(),
        candidate_id.as_deref(),
        None,
        None,
        None,
        spec.entry_summary.as_deref(),
    );

    let entry_record = ObservationJournalRecord {
        record_id: entry_record_id,
        request_id: request_id.clone(),
        parent_request_id: None,
        call_id: Some(call_id.clone()),
        phase: ObservationRecordPhase::Entry,
        at_ms: started_at_ms,
        pid: Some(std::process::id()),
        process_instance: process_instance.clone(),
        project_label: project_label.clone(),
        project_path: project_path.clone(),
        binding_id: Some(spec.binding_id.clone()),
        thread_uuid: Some(spec.thread_uuid.clone()),
        thread_label: thread_label.clone(),
        job_id: enriched_job_id.clone(),
        plan_id: enriched_plan_id.clone(),
        task_id: task_id.clone(),
        approval_id: enriched_approval_id.clone(),
        operation: spec.operation.clone(),
        title: clean_title.clone(),
        state: ObservationCallState::Running,
        actor: Some(actor.clone()),
        executor: Some(entry_executor),
        usage: None,
        delivery: Vec::new(),
        summary,
    };

    append_coalesced_journal_record(&spec.journal_dir, &entry_record);

    CoworkerCallObservationHandle {
        journal_dir: spec.journal_dir,
        request_id,
        call_id,
        operation: spec.operation,
        title: clean_title,
        binding_id: spec.binding_id,
        thread_uuid: spec.thread_uuid,
        thread_label,
        project_label,
        project_path,
        job_id: enriched_job_id,
        plan_id: enriched_plan_id,
        task_id,
        approval_id: enriched_approval_id,
        attempt_no: spec.attempt_no,
        run_id,
        worker_id,
        candidate_id,
        actor,
        executor: spec.executor,
        started_at_ms,
        process_instance,
    }
}

fn append_coalesced_journal_record(journal_dir: &Path, record: &ObservationJournalRecord) {
    if !is_existing_private_dir(journal_dir) {
        return;
    }
    if let Ok((existing, _)) = read_journal_records_readonly(journal_dir) {
        if existing.iter().any(|r| r.record_id == record.record_id) {
            return;
        }
    }
    let _ = append_journal_record(journal_dir, record);
}

fn enrich_scope_from_workflow_store(
    spec: &CoworkerCallObservationSpec,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let journal_dir = spec.journal_dir.as_path();
    let binding_id = spec.binding_id.as_str();
    let thread_uuid = spec.thread_uuid.as_str();
    let explicit_job_id = spec.job_id.as_deref();
    let explicit_plan_id = spec.plan_id.as_deref();
    let explicit_approval_id = spec.approval_id.as_deref();
    let explicit_task_id = spec.task_id.as_deref();
    let explicit_run_id = spec.run_id.as_deref();
    let mut job_id = explicit_job_id.and_then(validate_correlation_id);
    let mut plan_id = explicit_plan_id.and_then(validate_correlation_id);
    let mut approval_id = explicit_approval_id.and_then(validate_correlation_id);
    let mut task_id = explicit_task_id.and_then(validate_correlation_id);

    let Ok(store) = CoworkerWorkflowStore::new(journal_dir) else {
        return (job_id, plan_id, approval_id, task_id);
    };
    let Ok(ledger) = store.read_binding_ledger(binding_id) else {
        return (job_id, plan_id, approval_id, task_id);
    };

    let matched_job = if let Some(ref jid) = job_id {
        ledger.jobs.iter().find(|j| {
            &j.job_id == jid && j.binding_id == binding_id && j.thread_uuid == thread_uuid
        })
    } else if ledger.jobs.len() == 1
        && ledger.jobs[0].binding_id == binding_id
        && ledger.jobs[0].thread_uuid == thread_uuid
    {
        let only_job = &ledger.jobs[0];
        job_id = validate_correlation_id(&only_job.job_id);
        Some(only_job)
    } else {
        None
    };

    if let Some(job) = matched_job {
        if plan_id.is_none() {
            plan_id = job
                .approval_linkage
                .as_ref()
                .map(|l| l.plan_id.as_str())
                .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_id.as_str()))
                .and_then(validate_correlation_id);
        }
        if approval_id.is_none() {
            approval_id = job
                .approval_linkage
                .as_ref()
                .and_then(|l| validate_correlation_id(&l.approval_id));
        }
        if let Some(ref tid) = task_id {
            if !job.tasks.iter().any(|t| &t.task_id == tid) {
                if let Ok(policy_store) = ControllerPolicyStore::new(journal_dir) {
                    if let Ok(Some(rt)) = policy_store.read_job_runtime(binding_id, &job.job_id) {
                        if let Some(art) = rt.attempt_artifacts.values().find(|art| {
                            explicit_run_id.is_some_and(|rid| art.run_id == rid)
                                || art.task_worktree_id.contains(tid)
                        }) {
                            if let Some(valid_tid) = validate_correlation_id(&art.task_id) {
                                task_id = Some(valid_tid);
                            }
                        }
                    }
                }
                if !job
                    .tasks
                    .iter()
                    .any(|t| Some(&t.task_id) == task_id.as_ref())
                    && job.tasks.len() == 1
                {
                    task_id = validate_correlation_id(&job.tasks[0].task_id);
                }
            }
        }
    }

    (job_id, plan_id, approval_id, task_id)
}

fn resolve_verified_project_metadata(repository_path: &Path) -> (Option<String>, Option<String>) {
    if !repository_path.is_absolute() {
        return (None, None);
    }
    let Ok(meta) = fs::symlink_metadata(repository_path) else {
        return (None, None);
    };
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return (None, None);
    }
    let Ok(canonical) = fs::canonicalize(repository_path) else {
        return (None, None);
    };
    if canonical != repository_path {
        return (None, None);
    }
    let path_str = canonical.to_string_lossy().into_owned();
    let label = canonical
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(sanitize_optional_label)
        .or_else(|| sanitize_optional_label(&path_str));
    (Some(path_str), label)
}

fn normalize_genuine_usage(usage: Option<ObservationUsage>) -> Option<ObservationUsage> {
    let u = usage?;
    if u.input_tokens.is_none()
        && u.output_tokens.is_none()
        && u.cache_read_tokens.is_none()
        && u.total_tokens.is_none()
    {
        return None;
    }
    Some(u)
}

#[allow(clippy::too_many_arguments)]
fn build_safe_scope_summary(
    operation: &str,
    phase: &str,
    state: ObservationCallState,
    job_id: Option<&str>,
    task_id: Option<&str>,
    attempt_no: Option<u32>,
    run_id: Option<&str>,
    worker_id: Option<&str>,
    candidate_id: Option<&str>,
    receipt_id: Option<&str>,
    artifact_id: Option<&str>,
    duration_ms: Option<i64>,
    detail: Option<&str>,
) -> Option<String> {
    let mut parts = Vec::with_capacity(10);
    parts.push(format!("op={operation}"));
    parts.push(format!("phase={phase}"));
    parts.push(format!("state={}", state.as_str()));
    if let Some(dur) = duration_ms {
        parts.push(format!("duration_ms={dur}"));
    }
    if let Some(jid) = job_id {
        parts.push(format!("job={jid}"));
    }
    if let Some(tid) = task_id {
        parts.push(format!("task={tid}"));
    }
    if let Some(att) = attempt_no {
        parts.push(format!("attempt={att}"));
    }
    if let Some(rid) = run_id {
        parts.push(format!("run={rid}"));
    }
    if let Some(wid) = worker_id {
        parts.push(format!("worker={wid}"));
    }
    if let Some(cid) = candidate_id {
        parts.push(format!("candidate={cid}"));
    }
    if let Some(rcpt) = receipt_id {
        parts.push(format!("receipt={rcpt}"));
    }
    if let Some(art) = artifact_id {
        parts.push(format!("artifact={art}"));
    }
    if let Some(d) = detail
        .map(sanitize_observation_summary)
        .filter(|s| !s.is_empty())
    {
        parts.push(format!("detail={d}"));
    }
    let out = sanitize_observation_summary(&parts.join(" "));
    (!out.is_empty()).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_local_verified_observation_context_isolates_concurrent_tasks_and_nesting() {
        assert_eq!(
            current_verified_coworker_observation_context_for_scope("binding-a", "thread-a"),
            None
        );

        let ctx_a = VerifiedMcpObservationContext::new(
            "binding-a",
            "thread-a",
            "Observation Native Chat A",
            Path::new("/tmp/threadlane-obs-a"),
            Some("proj-obs-a"),
        );
        let ctx_b = VerifiedMcpObservationContext::new(
            "binding-b",
            "thread-b",
            "Observation Native Chat B",
            Path::new("/tmp/threadlane-obs-b"),
            Some("proj-obs-b"),
        );
        let ctx_a_nested = VerifiedMcpObservationContext::new(
            "binding-a",
            "thread-a",
            "Observation Native Chat A Nested",
            Path::new("/tmp/threadlane-obs-a"),
            Some("proj-obs-a"),
        );

        let barrier = Arc::new(Barrier::new(2));

        let barrier_a = Arc::clone(&barrier);
        let expected_a = ctx_a.clone();
        let expected_a_nested = ctx_a_nested.clone();
        let task_a = tokio::spawn(with_verified_coworker_observation_context(
            Some(ctx_a),
            async move {
                for _ in 0..4 {
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-a",
                            "thread-a",
                        ),
                        Some(expected_a.clone())
                    );
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-b",
                            "thread-b",
                        ),
                        None
                    );
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-a",
                            "thread-b",
                        ),
                        None
                    );
                    tokio::task::yield_now().await;
                    barrier_a.wait().await;
                }

                let unscoped_spawn = tokio::spawn(async {
                    current_verified_coworker_observation_context_for_scope("binding-a", "thread-a")
                })
                .await
                .expect("unscoped spawned task");
                assert_eq!(unscoped_spawn, None);

                with_verified_coworker_observation_context(Some(ctx_a_nested), async {
                    tokio::task::yield_now().await;
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-a",
                            "thread-a",
                        ),
                        Some(expected_a_nested.clone())
                    );
                })
                .await;

                with_verified_coworker_observation_context(None, async {
                    tokio::task::yield_now().await;
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-a",
                            "thread-a",
                        ),
                        None
                    );
                })
                .await;

                assert_eq!(
                    current_verified_coworker_observation_context_for_scope(
                        "binding-a",
                        "thread-a",
                    ),
                    Some(expected_a)
                );
            },
        ));

        let barrier_b = Arc::clone(&barrier);
        let expected_b = ctx_b.clone();
        let task_b = tokio::spawn(with_verified_coworker_observation_context(
            Some(ctx_b),
            async move {
                for _ in 0..4 {
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-b",
                            "thread-b",
                        ),
                        Some(expected_b.clone())
                    );
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-a",
                            "thread-a",
                        ),
                        None
                    );
                    assert_eq!(
                        current_verified_coworker_observation_context_for_scope(
                            "binding-b",
                            "thread-a",
                        ),
                        None
                    );
                    tokio::task::yield_now().await;
                    barrier_b.wait().await;
                }
            },
        ));

        task_a.await.expect("task_a completed");
        task_b.await.expect("task_b completed");

        assert_eq!(
            current_verified_coworker_observation_context_for_scope("binding-a", "thread-a"),
            None
        );
        assert_eq!(
            current_verified_coworker_observation_context_for_scope("binding-b", "thread-b"),
            None
        );
    }
}
