//! Scoped execution handoff gateway bridging verified native Coworker MCP calls to
//! [`CoworkerCodeDriver`], [`CoworkerWorkflowStore`], [`ControllerPolicyStore`], and
//! [`AuthorityStore`].
//!
//! ## Core Invariants
//!
//! - **Caller-supplied scope is never trusted from tool arguments**: `binding_id`, `thread_uuid`,
//!   store directory paths (`state_dir`, `registry_dir`, `plans_dir`, `authority_dir`,
//!   `sessions_dir`), and the canonical sibling `threadlane-flow` binary path are injected
//!   strictly from verified server configuration and operator enrollment.
//! - **No self-approval, grant creation, envelope freeze, or plan publication from MCP**:
//!   [`CoworkerExecutionGateway`] never creates or mutates [`ApprovalRecord`],
//!   [`FrozenApprovalLinkage`], [`FrozenControllerCapabilityEnvelope`], or
//!   [`FrozenControllerExecutionEnvelope`]. Human approval and envelope freezing remain explicit
//!   trusted-local CLI attestation steps via `threadlane-flow` (which records local operator
//!   attestation rather than cryptographic proof of human identity). Saved workflow profiles and
//!   model assignments never grant execution authority on their own.
//! - **Pure read-only `preview_execution`**: `preview_execution` inspects persisted workflow,
//!   controller policy, and authority stores without spawning processes, calling model providers,
//!   creating host session directories, or acquiring physical job locks. It returns the bounded
//!   concrete approval scope without leaking raw task prompts, secret tokens, hidden executable
//!   paths, registry tool configurations, or private logs.
//! - **Exact frozen digest confirmation before launch**: `start_execution` verifies that the job
//!   is in the enrolled `(binding_id, thread_uuid)` scope, has a verified `ApprovalRecord`,
//!   `FrozenApprovalLinkage`, `FrozenControllerCapabilityEnvelope`, and operator-attested
//!   `FrozenControllerExecutionEnvelope`, and that `expected_envelope_digest` matches the frozen
//!   `envelope_digest` in constant time **before** opening `CoworkerCodeDriver` or spawning the
//!   detached canonical sibling `threadlane-flow` owner process.
//! - **Truthful Phase 1 pipeline and cancellation boundaries**: All responses preserve and surface
//!   [`CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly`] (completed `Coder`
//!   runs reach `ReadyForReview` / `AwaitingChecksOrReview` and are never reported as accepted or
//!   integrated) and [`CoworkerDerivedCancellationBoundary::OriginalHostTasksOnlyDerivedIntegratedLater`].

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::schemars;
use serde::{Deserialize, Serialize};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_authority::{ApprovalRecord, AuthorityError, AuthorityStore};
use threadlane_host::HostPhase;
use threadlane_operations::IntegrationStrategy;
use threadlane_workers::validate_sha256_hex_digest;
use threadlane_workflow::{
    compile_plan_revision, CompiledPlan, Difficulty, PlanRevision, WorkflowRole,
};

use crate::coworker_driver::{
    CancelledHostTaskSummary, CoworkerCodeDriver, CoworkerDerivedCancellationBoundary,
    CoworkerDriverCancelReport, CoworkerDriverEnsureDisposition, CoworkerDriverEnsureReport,
    CoworkerDriverHostMode, CoworkerDriverOwnerState, CoworkerDriverPipelineBoundary,
    CoworkerDriverStatusReport,
};
use crate::coworker_workflow::{
    compute_capability_envelope_digest, verify_live_approval_and_registry, CanonicalProjectBaseRef,
    ControllerJobLeaseRecord, ControllerJobRuntimeState, ControllerPolicyStore,
    ControllerRoleExecutionBudget, CoworkerBindingLedger, CoworkerControllerPhase1Status,
    CoworkerControllerStepReport, CoworkerControllerTaskObservation, CoworkerJobOwnershipKey,
    CoworkerJobPhase, CoworkerJobRecord, CoworkerOwnershipSummary, CoworkerTaskRecord,
    CoworkerTaskStage, CoworkerWorkflowStore, FrozenApprovalLinkage, FrozenCheckCommandSpec,
    FrozenCheckKind, MAX_COWORKER_ID_LEN, MAX_COWORKER_SUMMARY_BYTES,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, validate_absolute_no_symlink_components,
    validate_bounded_identifier,
};
use crate::mcp_pair::open_workflow_stores;
use crate::planning_gateway::{
    resolve_sibling_flow_executable, resolve_sibling_flow_executable_from,
};
use crate::profile_flow::SelectedRoleChoice;

/// Canonical human-scope approval route explanation included on execution preview responses.
pub const TRUSTED_LOCAL_HUMAN_APPROVAL_ROUTE: &str =
    "threadlane-flow coworker-admission preview|record|inspect|reconcile after exact published-plan authority approval; trusted-local operator attestation, not cryptographic proof of human identity";

/// Parameters for the `preview_execution` MCP tool.
///
/// Scope (`binding_id`, `thread_uuid`, and store paths) is injected strictly by the server from
/// verified operator enrollment; unknown or caller-supplied scope/model/path fields are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PreviewExecutionParams {
    /// Target Coworker job identifier (`job_<32hex>`) in the enrolled `(binding_id, thread_uuid)`.
    pub job_id: String,
}

/// Parameters for the `start_execution` MCP tool.
///
/// Requires the exact 64-character lowercase hex SHA-256 `expected_envelope_digest` of the job's
/// operator-attested [` crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartExecutionParams {
    /// Target Coworker job identifier (`job_<32hex>`) in the enrolled `(binding_id, thread_uuid)`.
    pub job_id: String,
    /// Exact 64-character lowercase hex SHA-256 `envelope_digest` of the frozen controller execution envelope.
    pub expected_envelope_digest: String,
}

/// Parameters for the `execution_status` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStatusParams {
    /// Target Coworker job identifier (`job_<32hex>`) in the enrolled `(binding_id, thread_uuid)`.
    pub job_id: String,
}

/// Parameters for the `stop_execution` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StopExecutionParams {
    /// Target Coworker job identifier (`job_<32hex>`) in the enrolled `(binding_id, thread_uuid)`.
    pub job_id: String,
    /// Optional bounded single-line reason recorded with the durable stop request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Readiness classification reported by [`CoworkerExecutionGateway::preview_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionReadinessStatus {
    /// Job has no bound [`FrozenApprovalLinkage`] yet (`approved: false`).
    NeedsApproval,
    /// Job has a [`FrozenApprovalLinkage`] but is missing a [`crate::coworker_workflow::FrozenControllerCapabilityEnvelope`]
    /// or a recorded operator-attested [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`] (`approved: false`).
    MissingExecutionEnvelope,
    /// Job has a verified [`ApprovalRecord`], [`FrozenApprovalLinkage`],
    /// [`crate::coworker_workflow::FrozenControllerCapabilityEnvelope`], and operator-attested
    /// [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`] (`approved: true`).
    ApprovedAndFrozen,
}

/// Prompt-redacted summary of a frozen task in [`ExecutionPreviewResponse`].
///
/// Omits raw task instructions and prompts so `preview_execution` never leaks raw prompt text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPreviewTaskSummary {
    /// Logical task identifier within the published plan and job.
    pub task_id: String,
    /// Human-readable task title.
    pub title: String,
    /// Canonical 64-character lowercase hex SHA-256 digest of the published task definition.
    pub task_digest: String,
    /// Assigned workflow role (`coder`).
    pub role: WorkflowRole,
    /// Task difficulty classification.
    pub difficulty: Difficulty,
    /// Exact frozen headless worker tool/model/effort choice for this task.
    pub selected_choice: SelectedRoleChoice,
    /// Upstream task identifiers that must be accepted and sealed before this task can run.
    pub depends_on: Vec<String>,
    /// Approved repository-relative write paths for this task.
    pub write_paths: Vec<String>,
    /// Declared interface identifiers owned by this task.
    pub owned_interfaces: Vec<String>,
    /// Declared interface identifiers consumed by this task.
    pub consumed_interfaces: Vec<String>,
    /// Current persisted task stage in [`CoworkerWorkflowStore`].
    pub stage: CoworkerTaskStage,
}

impl ExecutionPreviewTaskSummary {
    /// Builds a prompt-redacted task summary from a [`CoworkerTaskRecord`] and an optional
    /// frozen plan task title resolved from [`PlanRevision`].
    pub fn from_record(task: &CoworkerTaskRecord, resolved_title: Option<String>) -> Self {
        Self {
            task_id: task.task_id.clone(),
            title: resolved_title.unwrap_or_else(|| task.task_id.clone()),
            task_digest: task.task_digest.clone(),
            role: task.role,
            difficulty: task.difficulty,
            selected_choice: task.selected_choice.clone(),
            depends_on: task.depends_on.clone(),
            write_paths: task.write_paths.clone(),
            owned_interfaces: task.owned_interfaces.clone(),
            consumed_interfaces: task.consumed_interfaces.clone(),
            stage: task.stage,
        }
    }
}

/// Sanitized summary of a frozen verification check command in [`ExecutionPreviewResponse`].
///
/// Exposes the executable basename (`executable_name`) and exact argument vector without leaking
/// the underlying host filesystem path (`FrozenCheckCommandSpec::executable`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPreviewCheckSummary {
    /// Stable check identifier listed in `allowed_check_ids`.
    pub check_id: String,
    /// Verification check category (`formatting`, `clippy`, `tests`, `custom`).
    pub kind: FrozenCheckKind,
    /// Canonical 64-character lowercase hex SHA-256 digest of the full [`FrozenCheckCommandSpec`].
    pub spec_digest: String,
    /// Basename of the canonical check executable (never the full host filesystem path).
    pub executable_name: String,
    /// Exact argument vector passed to the check executable.
    pub argv: Vec<String>,
    /// Compact human-readable command summary (`<executable_name> <argv...>`).
    pub command_summary: String,
    /// Repository-relative working directory inside the worktree (`"."` for repository root).
    pub repo_relative_cwd: String,
    /// Finite wall-clock timeout in milliseconds.
    pub timeout_ms: u64,
    /// Maximum stdout capture budget in bytes.
    pub max_stdout_bytes: usize,
    /// Maximum stderr capture budget in bytes.
    pub max_stderr_bytes: usize,
    /// Whether this check is required before task review acceptance.
    pub required_for_task_review: bool,
    /// Whether this check is required before combined candidate closure.
    pub required_for_candidate_closure: bool,
}

impl ExecutionPreviewCheckSummary {
    /// Builds a path-redacted check summary from a [`FrozenCheckCommandSpec`].
    pub fn from_spec(spec: &FrozenCheckCommandSpec) -> Result<Self, NativeError> {
        let spec_digest = spec.spec_digest()?;
        let executable_name = spec
            .executable
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("check")
            .to_string();
        let command_summary = if spec.argv.is_empty() {
            executable_name.clone()
        } else {
            format!("{executable_name} {}", spec.argv.join(" "))
        };
        Ok(Self {
            check_id: spec.check_id.clone(),
            kind: spec.kind,
            spec_digest,
            executable_name,
            argv: spec.argv.clone(),
            command_summary,
            repo_relative_cwd: spec.repo_relative_cwd.clone(),
            timeout_ms: spec.timeout_ms,
            max_stdout_bytes: spec.max_stdout_bytes,
            max_stderr_bytes: spec.max_stderr_bytes,
            required_for_task_review: spec.required_for_task_review,
            required_for_candidate_closure: spec.required_for_candidate_closure,
        })
    }
}

/// Process-free, provider-free execution scope preview returned by `preview_execution`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPreviewResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Target Coworker job identifier.
    pub job_id: String,
    /// Current monotonic job revision in [`CoworkerWorkflowStore`].
    pub job_revision: u64,
    /// Current persisted job phase in [`CoworkerWorkflowStore`].
    pub job_phase: CoworkerJobPhase,
    /// Whether the job has a complete, verified, operator-attested approval and frozen execution envelope.
    pub approved: bool,
    /// Readiness state (`needs_approval`, `missing_execution_envelope`, or `approved_and_frozen`).
    pub readiness: ExecutionReadinessStatus,
    /// Human-readable explanation of the current readiness state and required next step.
    pub readiness_detail: String,
    /// Canonical repository path, base commit SHA, and base tree SHA for the job.
    pub project_base: CanonicalProjectBaseRef,
    /// Bound [`crate::coworker_workflow::WorkBrief::brief_id`].
    pub work_brief_id: String,
    /// Canonical 64-character lowercase hex SHA-256 digest of the bound [`crate::coworker_workflow::WorkBrief`].
    pub work_brief_digest: String,
    /// Bounded human-readable summary of the bound [`crate::coworker_workflow::WorkBrief`].
    pub work_brief_summary: String,
    /// Bound workflow profile identifier (when planned or approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    /// Bound workflow registry revision (when planned or approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_revision: Option<u64>,
    /// Governing execution policy identifier (when planned or approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_id: Option<String>,
    /// Canonical 64-character lowercase hex SHA-256 digest of the governing execution policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_digest: Option<String>,
    /// Published workflow plan identifier (when planned or approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Published workflow plan revision number (when planned or approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Canonical 64-character lowercase hex SHA-256 digest of the published plan revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    /// Bound operator [`ApprovalRecord::approval_id`] (when approved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Canonical 64-character lowercase hex SHA-256 `subject_digest` of the bound [`ApprovalRecord`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject_digest: Option<String>,
    /// Canonical 64-character lowercase hex SHA-256 digest of the [`crate::coworker_workflow::FrozenControllerCapabilityEnvelope`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_envelope_digest: Option<String>,
    /// Deterministic identifier (`env_<32hex>`) of the recorded [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_id: Option<String>,
    /// Canonical 64-character lowercase hex SHA-256 `envelope_digest` required by `start_execution`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_digest: Option<String>,
    /// Maximum concurrent worker tasks permitted by the published plan / approval record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel: Option<usize>,
    /// Sorted union of approved repository-relative write paths across all tasks in the job.
    pub approved_write_paths: Vec<String>,
    /// Prompt-redacted summaries of the frozen tasks in the job.
    pub tasks: Vec<ExecutionPreviewTaskSummary>,
    /// Frozen per-task [`WorkflowRole::Coder`] tool/model/effort choices keyed by `task_id`.
    pub task_coder_choices: BTreeMap<String, SelectedRoleChoice>,
    /// Frozen headless [`WorkflowRole::Reviewer`] tool/model/effort choice (when envelope is frozen).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_choice: Option<SelectedRoleChoice>,
    /// Frozen headless [`WorkflowRole::Closure`] tool/model/effort choice (when envelope is frozen).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_choice: Option<SelectedRoleChoice>,
    /// Frozen per-role execution budgets (`coder`, `reviewer`, `closure`).
    pub role_budgets: BTreeMap<WorkflowRole, ControllerRoleExecutionBudget>,
    /// Maximum in-scope correction attempts permitted per task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_in_scope_corrections_per_task: Option<u32>,
    /// Maximum total in-scope correction attempts permitted across the entire job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_corrections_per_job: Option<u32>,
    /// Maximum total task execution attempts (initial + corrections) permitted across the job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_task_attempts_per_job: Option<u32>,
    /// Whether the controller is authorized to commit task worktrees after diff-gate verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_task_worktree_commits: Option<bool>,
    /// Whether the controller is authorized to integrate accepted task commits into an owned `RunIntegration` worktree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_run_integration_execution: Option<bool>,
    /// Default Git integration strategy frozen in the execution envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_integration_strategy: Option<IntegrationStrategy>,
    /// Allowed check command identifiers from the capability envelope.
    pub allowed_check_ids: Vec<String>,
    /// Path-redacted summaries of the frozen check command specifications.
    pub check_commands: Vec<ExecutionPreviewCheckSummary>,
    /// Whether local operator attestation (`operator_attested == true`) is recorded on the execution envelope.
    pub operator_attested: bool,
    /// Always `false`: local controller execution envelopes never prove per-call native chat identity.
    pub native_per_call_identity_verified: bool,
    /// Always `false`: saved workflow profiles or model assignments never grant execution authority.
    pub saved_profiles_grant_execution_authority: bool,
    /// Always `false`: trusted-local CLI approval records local operator attestation, not cryptographic proof of human identity.
    pub cryptographic_human_identity_proven: bool,
    /// Explicit description of the trusted-local `threadlane-flow` human approval route.
    pub human_approval_route: String,
    /// Truthful Phase 1 pipeline boundary label (`phase1_initial_dispatch_and_reconcile_only`).
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
}

/// Response returned by `start_execution` after verifying the frozen execution envelope and
/// ensuring a single physical [`CoworkerCodeDriver`] owner for the job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartExecutionResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Target Coworker job identifier.
    pub job_id: String,
    /// Bound operator [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Deterministic identifier (`env_<32hex>`) of the verified [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
    pub envelope_id: String,
    /// Verified 64-character lowercase hex SHA-256 `envelope_digest`.
    pub envelope_digest: String,
    /// Whether `start_execution` spawned a new detached owner, reused a live verified owner, or verified a quiescent job.
    pub disposition: CoworkerDriverEnsureDisposition,
    /// Observed physical owner state after the ensure handoff.
    pub owner_state: CoworkerDriverOwnerState,
    /// Current persisted job phase in [`CoworkerWorkflowStore`].
    pub job_phase: CoworkerJobPhase,
    /// Current overall Phase 1 controller status (`running_tasks`, `awaiting_checks_or_review`, etc.).
    pub overall_status: CoworkerControllerPhase1Status,
    /// Whether durable operator stop (`stop_requested`) is set on the job runtime state.
    pub stop_requested: bool,
    /// Whether the job is blocked on an unresolved or unknown effect (`effect_unknown`).
    pub blocked_on_unknown_effect: bool,
    /// Explicit Phase 1 pipeline boundary (`phase1_initial_dispatch_and_reconcile_only`).
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
    /// Human-readable explanation of the Phase 1 pipeline boundary.
    pub boundary_note: String,
    /// Full verified status report from [`CoworkerCodeDriver::ensure_owner`].
    pub status: CoworkerDriverStatusReport,
}

impl StartExecutionResponse {
    fn from_ensure_report(
        envelope_id: String,
        envelope_digest: String,
        report: CoworkerDriverEnsureReport,
    ) -> Self {
        let status = report.status;
        Self {
            binding_id: status.binding_id.clone(),
            thread_uuid: status.thread_uuid.clone(),
            job_id: status.job_id.clone(),
            approval_id: status.approval_id.clone(),
            envelope_id,
            envelope_digest,
            disposition: report.disposition,
            owner_state: status.owner_state,
            job_phase: status.job_phase,
            overall_status: status.overall_status,
            stop_requested: status.stop_requested,
            blocked_on_unknown_effect: status.blocked_on_unknown_effect,
            pipeline_boundary: status.pipeline_boundary,
            boundary_note: status.boundary_note.clone(),
            status,
        }
    }
}

/// Response returned by `execution_status` for an approved, envelope-frozen Coworker job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStatusResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Target Coworker job identifier.
    pub job_id: String,
    /// Bound operator [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Deterministic identifier (`env_<32hex>`) of the verified [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
    pub envelope_id: String,
    /// Verified 64-character lowercase hex SHA-256 `envelope_digest`.
    pub envelope_digest: String,
    /// Observed physical owner state (`live_verified_owner`, `no_active_owner`, `stale_crash_artifact_detected`, etc.).
    pub owner_state: CoworkerDriverOwnerState,
    /// Durable bounded first fatal owner diagnosis, independent of job completion or stop state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fatal_error: Option<crate::coworker_driver::CoworkerDriverFatalError>,
    /// Verified live ownership summary when a live owner is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_owner_summary: Option<CoworkerOwnershipSummary>,
    /// Scoped unverified diagnostic history with independently checked live owner/effect display.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_health: Vec<crate::provider_health::ProviderHealthView>,
    /// Persisted logical lease record in [`ControllerPolicyStore`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<ControllerJobLeaseRecord>,
    /// Whether the persisted logical lease in [`ControllerPolicyStore`] is currently marked active.
    pub lease_active: bool,
    /// Persisted logical lease owner instance identifier in [`ControllerPolicyStore`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner_instance_id: Option<String>,
    /// Observed host session phase in `sessions_dir/<approval_id>` if a manifest exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_phase: Option<HostPhase>,
    /// Whether `sessions_dir/<approval_id>/host.lock` is currently held by a live process.
    pub host_lock_held: bool,
    /// Host lifecycle mode used for this status inspection.
    pub host_mode: CoworkerDriverHostMode,
    /// Current persisted job phase in [`CoworkerWorkflowStore`].
    pub job_phase: CoworkerJobPhase,
    /// Legacy Phase 1 controller status (`running_tasks`, `awaiting_checks_or_review`, etc.).
    /// Distinct from the full-pipeline `pipeline_status`.
    pub overall_status: CoworkerControllerPhase1Status,
    /// Effective driver execution mode (`full_automatic_pipeline` or `phase1_initial_dispatch_only`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<crate::coworker_driver::CoworkerDriverExecutionMode>,
    /// Truthful full-pipeline status (`running`, `completed`, `stopped`, `held`, etc.) when operating in
    /// `full_automatic_pipeline` mode; distinct from the legacy `overall_status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_status: Option<crate::coworker_workflow::CoworkerPipelineStatus>,
    /// Whether `stop_requested` is set and the live owner is gracefully draining finite in-flight
    /// stage futures before exiting.
    #[serde(default)]
    pub stopping_draining_in_flight: bool,
    /// Stage futures currently retained in-flight by the live owner's pipeline driver.
    #[serde(default)]
    pub in_flight_stages: Vec<crate::coworker_workflow::CoworkerPipelineInFlightStageRef>,
    /// Compact pipeline report when operating in `full_automatic_pipeline` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_report: Option<crate::coworker_driver::CoworkerDriverPipelineReport>,
    /// Whether durable operator stop (`stop_requested`) is set on the job runtime state.
    pub stop_requested: bool,
    /// Whether the job has unresolved pending effects or unknown outbox state blocking replay.
    pub blocked_on_unknown_effect: bool,
    /// Optional stop or block reason recorded in runtime state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_or_block_reason: Option<String>,
    /// Total task execution attempts reserved across the job in [`ControllerJobRuntimeState`].
    pub total_task_attempts_reserved: u32,
    /// Maximum total task attempts permitted across the job by [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
    pub max_total_task_attempts_per_job: u32,
    /// Per-task execution observations (`ReadyForReview` is never reported as accepted or integrated).
    pub tasks: Vec<CoworkerControllerTaskObservation>,
    /// Latest step report when a controller step was executed or recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_report: Option<CoworkerControllerStepReport>,
    /// Explicit Phase 1 pipeline boundary (`phase1_initial_dispatch_and_reconcile_only`).
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
    /// Human-readable boundary explanation.
    pub boundary_note: String,
    /// Read-only coworker native notice metadata when inspection succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice: Option<crate::coworker_notice::NoticeMetadata>,
    /// Stable error code when coworker native notice inspection fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice_error: Option<String>,
}

impl ExecutionStatusResponse {
    fn from_status_report(
        envelope_id: String,
        envelope_digest: String,
        report: CoworkerDriverStatusReport,
    ) -> Self {
        Self {
            binding_id: report.binding_id,
            thread_uuid: report.thread_uuid,
            job_id: report.job_id,
            approval_id: report.approval_id,
            envelope_id,
            envelope_digest,
            owner_state: report.owner_state,
            fatal_error: report.fatal_error,
            verified_owner_summary: report.verified_owner_summary,
            provider_health: report.provider_health,
            lease: report.lease,
            lease_active: report.lease_active,
            lease_owner_instance_id: report.lease_owner_instance_id,
            host_phase: report.host_phase,
            host_lock_held: report.host_lock_held,
            host_mode: report.host_mode,
            job_phase: report.job_phase,
            overall_status: report.overall_status,
            execution_mode: Some(report.execution_mode),
            pipeline_status: report.pipeline_status,
            stopping_draining_in_flight: report.stopping_draining_in_flight,
            in_flight_stages: report.in_flight_stages,
            pipeline_report: report.pipeline_report,
            stop_requested: report.stop_requested,
            blocked_on_unknown_effect: report.blocked_on_unknown_effect,
            stop_or_block_reason: report.stop_or_block_reason,
            total_task_attempts_reserved: report.total_task_attempts_reserved,
            max_total_task_attempts_per_job: report.max_total_task_attempts_per_job,
            tasks: report.tasks,
            step_report: report.step_report,
            pipeline_boundary: report.pipeline_boundary,
            boundary_note: report.boundary_note,
            native_notice: None,
            native_notice_error: None,
        }
    }
}

/// Response returned by `stop_execution` after recording durable `stop_requested` state and
/// performing narrow [`CoworkerCodeDriver::cancel`] cancellation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopExecutionResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical lowercase hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Target Coworker job identifier.
    pub job_id: String,
    /// Bound operator [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Deterministic identifier (`env_<32hex>`) of the verified [`crate::coworker_workflow::FrozenControllerExecutionEnvelope`].
    pub envelope_id: String,
    /// Verified 64-character lowercase hex SHA-256 `envelope_digest`.
    pub envelope_digest: String,
    /// Sanitized reason recorded for the durable stop request.
    pub reason: String,
    /// Confirms that durable `stop_requested = true` was persisted in [`ControllerPolicyStore`]
    /// before any local owner RPC or `ApprovedHostClient::cancel_task` call.
    pub durable_stop_recorded_before_host_cancel: bool,
    /// Whether an active verified owner was notified over local RPC and shut down cleanly.
    pub notified_live_owner: bool,
    /// Whether narrow host task cancellation was attempted against an active `Ready` host.
    pub narrow_host_cancellation_attempted: bool,
    /// Original host tasks cancelled via `ApprovedHostClient::cancel_task`.
    pub cancelled_host_tasks: Vec<CancelledHostTaskSummary>,
    /// Explicit Phase 1 derived-attempt cancellation boundary (`original_host_tasks_only_derived_integrated_later`).
    pub derived_attempt_cancellation_boundary: CoworkerDerivedCancellationBoundary,
    /// Human-readable explanation of the cancellation boundary.
    pub cancellation_boundary_note: String,
    /// Explicit Phase 1 pipeline boundary (`phase1_initial_dispatch_and_reconcile_only`).
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
    /// Human-readable explanation of the Phase 1 pipeline boundary.
    pub boundary_note: String,
    /// Post-cancellation verified status report for the job.
    pub status: CoworkerDriverStatusReport,
}

impl StopExecutionResponse {
    fn from_cancel_report(
        envelope_id: String,
        envelope_digest: String,
        report: CoworkerDriverCancelReport,
    ) -> Self {
        let pipeline_boundary = report.status.pipeline_boundary;
        let boundary_note = report.status.boundary_note.clone();
        Self {
            binding_id: report.binding_id,
            thread_uuid: report.thread_uuid,
            job_id: report.job_id,
            approval_id: report.approval_id,
            envelope_id,
            envelope_digest,
            reason: report.reason,
            durable_stop_recorded_before_host_cancel: report
                .durable_stop_recorded_before_host_cancel,
            notified_live_owner: report.notified_live_owner,
            narrow_host_cancellation_attempted: report.narrow_host_cancellation_attempted,
            cancelled_host_tasks: report.cancelled_host_tasks,
            derived_attempt_cancellation_boundary: report.derived_attempt_cancellation_boundary,
            cancellation_boundary_note: report.cancellation_boundary_note,
            pipeline_boundary,
            boundary_note,
            status: report.status,
        }
    }
}

struct VerifiedScopedExecutionContext {
    #[allow(dead_code)]
    job: CoworkerJobRecord,
    #[allow(dead_code)]
    linkage: FrozenApprovalLinkage,
    runtime_state: ControllerJobRuntimeState,
    #[allow(dead_code)]
    approval: ApprovalRecord,
}

/// Scoped execution handoff gateway for an enrolled `(binding_id, thread_uuid)` native chat.
#[derive(Debug, Clone)]
pub struct CoworkerExecutionGateway {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    binding_id: String,
    thread_uuid: String,
    sibling_anchor_exe: Option<PathBuf>,
    managed_native_notice: Option<(threadlane::native_endpoint::NativeReconnectConfig, u32)>,
}

impl CoworkerExecutionGateway {
    /// Creates a new [`CoworkerExecutionGateway`] for the verified `(binding_id, thread_uuid)`
    /// scope and store directories without creating directories or lockfiles.
    pub fn new(
        state_dir: impl AsRef<Path>,
        registry_dir: impl AsRef<Path>,
        plans_dir: impl AsRef<Path>,
        authority_dir: impl AsRef<Path>,
        sessions_dir: impl AsRef<Path>,
        binding_id: impl Into<String>,
        thread_uuid: impl Into<String>,
    ) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let binding_id = binding_id.into();
        let thread_uuid = thread_uuid.into();
        validate_bounded_identifier(&binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;

        let clean_state_dir =
            validate_absolute_no_symlink_components(state_dir.as_ref(), "state_dir")?;
        let clean_registry_dir =
            validate_absolute_no_symlink_components(registry_dir.as_ref(), "registry_dir")?;
        let clean_plans_dir =
            validate_absolute_no_symlink_components(plans_dir.as_ref(), "plans_dir")?;
        let clean_authority_dir =
            validate_absolute_no_symlink_components(authority_dir.as_ref(), "authority_dir")?;
        let clean_sessions_dir =
            validate_absolute_no_symlink_components(sessions_dir.as_ref(), "sessions_dir")?;

        Ok(Self {
            state_dir: clean_state_dir,
            registry_dir: clean_registry_dir,
            plans_dir: clean_plans_dir,
            authority_dir: clean_authority_dir,
            sessions_dir: clean_sessions_dir,
            binding_id,
            thread_uuid,
            sibling_anchor_exe: None,
            managed_native_notice: None,
        })
    }

    /// Overrides the anchor executable path used to locate the canonical same-package sibling
    /// `threadlane-flow` binary during integration tests.
    pub fn with_sibling_anchor_exe(mut self, anchor_exe: impl Into<PathBuf>) -> Self {
        self.sibling_anchor_exe = Some(anchor_exe.into());
        self
    }

    /// Attaches a verified managed native reconnect configuration and MCP parent backend PID for
    /// configuring coworker milestone notices during `start_execution`.
    pub fn with_managed_native_notice(
        mut self,
        reconnect_config: threadlane::native_endpoint::NativeReconnectConfig,
        backend_pid: u32,
    ) -> Self {
        self.managed_native_notice = Some((reconnect_config, backend_pid));
        self
    }

    /// Resolves and validates the canonical same-package sibling `threadlane-flow` executable.
    pub fn resolve_sibling_flow_executable(&self) -> Result<PathBuf, NativeError> {
        match self.sibling_anchor_exe {
            Some(ref anchor) => resolve_sibling_flow_executable_from(anchor, true),
            None => resolve_sibling_flow_executable(),
        }
    }

    /// Inspects the bounded concrete execution and approval scope for `params.job_id` without
    /// spawning processes, calling model providers, or creating host session directories.
    pub fn preview_execution(
        &self,
        params: &PreviewExecutionParams,
    ) -> Result<ExecutionPreviewResponse, NativeError> {
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;

        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        wf_ledger.validate(&self.binding_id)?;
        verify_ledger_thread_scope(&wf_ledger, &self.binding_id, &self.thread_uuid)?;

        let job = require_scoped_job(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.job_id,
        )?;
        ensure_job_not_superseded(&wf_ledger, job)?;
        verify_no_full_job_external_resolution(&self.plans_dir, &self.binding_id, job)?;

        let policy_store = ControllerPolicyStore::new(&self.plans_dir)?;
        let runtime_state = policy_store.get_job_runtime_readonly(&self.binding_id, &job.job_id)?;
        if let Some(ref rt) = runtime_state {
            if rt.envelope.binding_id != self.binding_id
                || rt.envelope.thread_uuid != self.thread_uuid
            {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "controller_runtime_state envelope scope does not match verified binding_id and thread_uuid",
                ));
            }
        }

        let mut approved = false;
        let readiness;
        let readiness_detail;
        let mut loaded_approval: Option<ApprovalRecord> = None;
        let mut loaded_plan_revision: Option<PlanRevision> = None;
        let mut loaded_compiled_plan: Option<CompiledPlan> = None;

        match (&job.approval_linkage, &runtime_state) {
            (None, None) => {
                readiness = ExecutionReadinessStatus::NeedsApproval;
                readiness_detail = format!(
                    "Job `{}` has no bound FrozenApprovalLinkage (`needs_approval`). Approve the published plan and bind/freeze the execution envelope via `threadlane-flow coworker-admission preview|record` (inspect/reconcile after a lost response) before calling `start_execution`.",
                    job.job_id
                );
            }
            (None, Some(_)) => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "Job `{}` has a recorded ControllerJobRuntimeState without a bound FrozenApprovalLinkage.",
                        job.job_id
                    ),
                ));
            }
            (Some(linkage), None) => {
                linkage.validate()?;
                validate_existing_private_store_dir(&self.registry_dir, "registry_dir")?;
                validate_existing_private_store_dir(&self.authority_dir, "authority_dir")?;
                let (registry_store, plan_store) =
                    open_workflow_stores(Some(&self.registry_dir), Some(&self.plans_dir))?
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::NotConfigured,
                                "execution preview requires configured registry_dir and plans_dir",
                            )
                        })?;
                let authority = AuthorityStore::open(
                    &self.authority_dir,
                    Arc::clone(&registry_store),
                    Arc::clone(&plan_store),
                )
                .map_err(map_authority_error)?;
                let approval = load_scoped_approval(
                    &authority,
                    &linkage.approval_id,
                    &self.binding_id,
                    &self.thread_uuid,
                )?;
                loaded_plan_revision = plan_store
                    .get_revision(&linkage.plan_id, linkage.plan_revision)
                    .ok()
                    .flatten();
                if let Some(ref plan_rev) = loaded_plan_revision {
                    if let Ok(reg_snap) = registry_store.snapshot_readonly() {
                        if reg_snap.revision == linkage.registry_revision {
                            loaded_compiled_plan = compile_plan_revision(plan_rev, &reg_snap).ok();
                        }
                    }
                }
                loaded_approval = Some(approval);
                readiness = ExecutionReadinessStatus::MissingExecutionEnvelope;
                readiness_detail = if linkage.controller_envelope.is_none() {
                    format!(
                        "Job `{}` has a FrozenApprovalLinkage (`{}`) but is missing a FrozenControllerCapabilityEnvelope and recorded FrozenControllerExecutionEnvelope (`missing_execution_envelope`). This linkage is immutable. Create a new job from a replanned, explicitly approved scope with controller capabilities, then use `threadlane-flow coworker-admission preview|record` before calling `start_execution`.",
                        job.job_id, linkage.approval_id
                    )
                } else {
                    format!(
                        "Job `{}` has a FrozenApprovalLinkage (`{}`) and FrozenControllerCapabilityEnvelope but is missing a recorded FrozenControllerExecutionEnvelope (`missing_execution_envelope`). Inspect/reconcile an existing admission receipt after a lost response, or use `threadlane-flow coworker-execution preview|record` for this legacy linked job. A different capability scope requires a new job, replan and explicit reapproval before `start_execution`.",
                        job.job_id, linkage.approval_id
                    )
                };
            }
            (Some(linkage), Some(rt)) => {
                linkage.validate()?;
                if linkage.controller_envelope.is_none() {
                    return Err(NativeError::new(
                        NativeErrorCode::UnauthorizedOperator,
                        format!(
                            "Job `{}` FrozenApprovalLinkage has no FrozenControllerCapabilityEnvelope (`missing_execution_envelope`). This linkage is immutable; create a new job, replan and obtain explicit reapproval with controller capabilities before automatic execution.",
                            job.job_id
                        ),
                    ));
                }
                validate_existing_private_store_dir(&self.registry_dir, "registry_dir")?;
                validate_existing_private_store_dir(&self.authority_dir, "authority_dir")?;
                let (registry_store, plan_store) =
                    open_workflow_stores(Some(&self.registry_dir), Some(&self.plans_dir))?
                        .ok_or_else(|| {
                            NativeError::new(
                                NativeErrorCode::NotConfigured,
                                "execution preview requires configured registry_dir and plans_dir",
                            )
                        })?;
                let authority = AuthorityStore::open(
                    &self.authority_dir,
                    Arc::clone(&registry_store),
                    Arc::clone(&plan_store),
                )
                .map_err(map_authority_error)?;
                let approval = load_scoped_approval(
                    &authority,
                    &linkage.approval_id,
                    &self.binding_id,
                    &self.thread_uuid,
                )?;
                loaded_plan_revision = plan_store
                    .get_revision(&linkage.plan_id, linkage.plan_revision)
                    .ok()
                    .flatten();
                if let Some(ref plan_rev) = loaded_plan_revision {
                    if let Ok(reg_snap) = registry_store.snapshot_readonly() {
                        if reg_snap.revision == linkage.registry_revision {
                            loaded_compiled_plan = compile_plan_revision(plan_rev, &reg_snap).ok();
                        }
                    }
                }
                rt.envelope.policy.validate()?;
                if !rt.envelope.operator_attested {
                    rt.envelope.validate_against_job(job)?;
                    verify_live_approval_and_registry(
                        &approval,
                        job,
                        &rt.envelope.policy,
                        &authority,
                    )?;
                    loaded_approval = Some(approval);
                    readiness = ExecutionReadinessStatus::MissingExecutionEnvelope;
                    readiness_detail = format!(
                        "Job `{}` has an unattested FrozenControllerExecutionEnvelope (`operator_attested == false`, `missing_execution_envelope`). Inspect/reconcile an existing admission receipt after a lost response, or use `threadlane-flow coworker-execution preview|record` for this legacy envelope. Changing this linked job's frozen capability scope requires a new job, replan and explicit reapproval before `start_execution`.",
                        job.job_id
                    );
                } else {
                    verify_job_planning_and_envelope_consistency(
                        job, linkage, rt, &approval, &authority, true,
                    )?;
                    loaded_approval = Some(approval);
                    let admission_ready = if wf_ledger.admission_markers.contains_key(&job.job_id) {
                        let admission = crate::coworker_admission::CoworkerAdmissionService::new(
                            &self.plans_dir,
                            &self.authority_dir,
                            &self.registry_dir,
                        )?
                        .inspect(&self.binding_id, &job.job_id)?;
                        admission.linkage_present
                            && admission.runtime_present
                            && admission.state
                                != crate::coworker_admission::AdmissionInspectionState::Conflicting
                            && admission.receipt.as_ref().is_some_and(|r| {
                                r.stage == crate::coworker_admission::AdmissionStage::Complete
                            })
                    } else {
                        true
                    };
                    approved = admission_ready;
                    readiness = if admission_ready {
                        ExecutionReadinessStatus::ApprovedAndFrozen
                    } else {
                        ExecutionReadinessStatus::MissingExecutionEnvelope
                    };
                    readiness_detail = if admission_ready {
                        format!(
                            "Job `{}` is approved and has a verified operator-attested FrozenControllerExecutionEnvelope (`{}`). Ready for `start_execution` with expected_envelope_digest `{}`.",
                            job.job_id, rt.envelope.envelope_id, rt.envelope.envelope_digest
                        )
                    } else {
                        "Admission is incomplete, uncertain or conflicting. Use threadlane-flow coworker-admission inspect, then explicit reconcile with the original request key; execution cannot finish admission.".into()
                    };
                }
            }
        }

        let plan_id = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.plan_id.clone())
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.plan_id.clone()))
            .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_id.clone()));
        let plan_revision = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.plan_revision)
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.plan_revision))
            .or_else(|| job.planning_result.as_ref().map(|pr| pr.plan_revision));
        let plan_digest = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.plan_digest.clone())
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.plan_digest.clone()))
            .or_else(|| {
                job.planning_result
                    .as_ref()
                    .map(|pr| pr.plan_digest.clone())
            });

        let profile_id = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.profile_id.clone())
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.profile_id.clone()))
            .or_else(|| job.planning_result.as_ref().map(|pr| pr.profile_id.clone()));
        let registry_revision = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.registry_revision)
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.registry_revision))
            .or_else(|| job.planning_result.as_ref().map(|pr| pr.registry_revision));
        let policy_id = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.policy.policy_id.clone())
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.policy_id.clone()))
            .or_else(|| {
                job.planning_result.as_ref().and_then(|pr| {
                    wf_ledger
                        .planning_grants
                        .iter()
                        .find(|grant| {
                            grant.grant_id == pr.grant_id
                                && grant.binding_id == self.binding_id
                                && grant.thread_uuid == self.thread_uuid
                                && grant.profile_id == pr.profile_id
                                && grant.registry_revision == pr.registry_revision
                                && constant_time_str_eq(&grant.policy_digest, &pr.policy_digest)
                                && grant.project_base.repository_path
                                    == pr.project_base.repository_path
                        })
                        .map(|grant| grant.policy.policy_id.clone())
                })
            });
        let policy_digest = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.policy_digest.clone())
            .or_else(|| {
                job.approval_linkage
                    .as_ref()
                    .map(|l| l.policy_digest.clone())
            })
            .or_else(|| {
                job.planning_result
                    .as_ref()
                    .map(|pr| pr.policy_digest.clone())
            });

        let approval_id = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.approval_id.clone())
            .or_else(|| job.approval_linkage.as_ref().map(|l| l.approval_id.clone()));
        let approval_subject_digest = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.approval_subject_digest.clone())
            .or_else(|| {
                job.approval_linkage
                    .as_ref()
                    .map(|l| l.approval_subject_digest.clone())
            });

        let capability_envelope_digest = match &runtime_state {
            Some(rt) => Some(rt.envelope.capability_envelope_digest.clone()),
            None => match job
                .approval_linkage
                .as_ref()
                .and_then(|l| l.controller_envelope.as_ref())
            {
                Some(cap_env) => Some(compute_capability_envelope_digest(cap_env)?),
                None => None,
            },
        };

        let envelope_id = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.envelope_id.clone());
        let envelope_digest = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.envelope_digest.clone());

        let max_parallel = loaded_approval.as_ref().map(|apr| apr.max_parallel);

        let mut write_path_set = BTreeSet::new();
        for task in &job.tasks {
            for path in &task.write_paths {
                write_path_set.insert(path.clone());
            }
        }
        let approved_write_paths: Vec<String> = write_path_set.into_iter().collect();

        let tasks: Vec<ExecutionPreviewTaskSummary> = job
            .tasks
            .iter()
            .map(|task| {
                let resolved_title = resolve_frozen_task_title(
                    task,
                    loaded_compiled_plan.as_ref(),
                    loaded_plan_revision.as_ref(),
                    loaded_approval.as_ref(),
                );
                ExecutionPreviewTaskSummary::from_record(task, resolved_title)
            })
            .collect();

        let task_coder_choices: BTreeMap<String, SelectedRoleChoice> = match &runtime_state {
            Some(rt) => rt.envelope.task_coder_choices.clone(),
            None => job
                .tasks
                .iter()
                .map(|t| (t.task_id.clone(), t.selected_choice.clone()))
                .collect(),
        };

        let reviewer_choice = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.reviewer_choice.clone());
        let closure_choice = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.closure_choice.clone());
        let role_budgets = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.role_budgets.clone())
            .unwrap_or_default();

        let max_in_scope_corrections_per_task = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.max_in_scope_corrections_per_task)
            .or_else(|| {
                job.approval_linkage
                    .as_ref()
                    .and_then(|l| l.controller_envelope.as_ref())
                    .map(|c| c.max_in_scope_correction_attempts)
            });
        let max_total_corrections_per_job = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.max_total_corrections_per_job);
        let max_total_task_attempts_per_job = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.max_total_task_attempts_per_job);
        let allow_task_worktree_commits = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.allow_task_worktree_commits);
        let allow_run_integration_execution = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.allow_run_integration_execution);
        let default_integration_strategy = runtime_state
            .as_ref()
            .map(|rt| rt.envelope.default_integration_strategy);

        let allowed_check_ids = job
            .approval_linkage
            .as_ref()
            .and_then(|l| l.controller_envelope.as_ref())
            .map(|c| c.allowed_check_ids.clone())
            .unwrap_or_default();

        let mut check_commands = Vec::new();
        if let Some(ref rt) = runtime_state {
            for spec in rt.envelope.check_commands.values() {
                check_commands.push(ExecutionPreviewCheckSummary::from_spec(spec)?);
            }
        }

        let operator_attested = runtime_state
            .as_ref()
            .is_some_and(|rt| rt.envelope.operator_attested);
        let pipeline_boundary = match runtime_state.as_ref() {
            Some(rt) => {
                if crate::coworker_driver::envelope_authorizes_full_pipeline(job, rt) {
                    CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
                } else {
                    CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly
                }
            }
            None => CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly,
        };

        Ok(ExecutionPreviewResponse {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: job.job_id.clone(),
            job_revision: job.job_revision,
            job_phase: job.phase,
            approved,
            readiness,
            readiness_detail,
            project_base: job.work_brief.project_base.clone(),
            work_brief_id: job.work_brief.brief_id.clone(),
            work_brief_digest: job.work_brief_digest.clone(),
            work_brief_summary: job.work_brief.summary.clone(),
            profile_id,
            registry_revision,
            policy_id,
            policy_digest,
            plan_id,
            plan_revision,
            plan_digest,
            approval_id,
            approval_subject_digest,
            capability_envelope_digest,
            envelope_id,
            envelope_digest,
            max_parallel,
            approved_write_paths,
            tasks,
            task_coder_choices,
            reviewer_choice,
            closure_choice,
            role_budgets,
            max_in_scope_corrections_per_task,
            max_total_corrections_per_job,
            max_total_task_attempts_per_job,
            allow_task_worktree_commits,
            allow_run_integration_execution,
            default_integration_strategy,
            allowed_check_ids,
            check_commands,
            operator_attested,
            native_per_call_identity_verified: false,
            saved_profiles_grant_execution_authority: false,
            cryptographic_human_identity_proven: false,
            human_approval_route: TRUSTED_LOCAL_HUMAN_APPROVAL_ROUTE.to_string(),
            pipeline_boundary,
        })
    }

    /// Starts or reuses the single verified physical [`CoworkerCodeDriver`] owner for
    /// `params.job_id` after verifying that the job is approved, its execution envelope is frozen
    /// and operator-attested, and `params.expected_envelope_digest` matches the frozen
    /// `envelope_digest`.
    pub async fn start_execution(
        &self,
        params: &StartExecutionParams,
    ) -> Result<StartExecutionResponse, NativeError> {
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(
            &params.expected_envelope_digest,
            "expected_envelope_digest",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_envelope_digest must be a 64-character lowercase hexadecimal SHA-256 digest",
            )
        })?;

        let ctx = self.require_verified_approved_job(&params.job_id)?;
        verify_no_full_job_external_resolution(&self.plans_dir, &self.binding_id, &ctx.job)?;
        let actual_digest = &ctx.runtime_state.envelope.envelope_digest;
        if !constant_time_str_eq(actual_digest, &params.expected_envelope_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "expected_envelope_digest `{}` does not match the job's frozen envelope_digest `{actual_digest}` (`envelope_digest_mismatch`)",
                    params.expected_envelope_digest
                ),
            ));
        }

        // Sync only completed unchanged consent before any driver, notice or owner effect.
        // This cannot finish incomplete or explicitly Uncertain persistence.
        crate::coworker_admission::verify_complete_admission(
            &CoworkerWorkflowStore::new(&self.plans_dir)?,
            &ControllerPolicyStore::new(&self.plans_dir)?,
            &ctx.job,
        )?;

        if let Some((ref reconnect_cfg, backend_pid)) = self.managed_native_notice {
            let notice_key = CoworkerJobOwnershipKey::new(
                self.binding_id.clone(),
                self.thread_uuid.clone(),
                params.job_id.clone(),
            )?;
            let notice_dir = crate::coworker_notice::coworker_notice_dir(&self.plans_dir);
            if !crate::coworker_notice::CoworkerNoticeStore::inspect(
                &notice_dir,
                &notice_key,
                actual_digest,
            )?
            .configured
            {
                let stream = reconnect_cfg
                    .connect_verified_stream_for_backend(Some(backend_pid))
                    .await
                    .map_err(|err| {
                        let code = match err {
                            threadlane::Error::SecurityViolation(_) => {
                                NativeErrorCode::SecurityViolation
                            }
                            _ => NativeErrorCode::NotConfigured,
                        };
                        NativeError::new(code, "failed to verify managed native notice endpoint")
                    })?;
                let socket_path = stream
                    .peer_addr()
                    .map_err(|_| {
                        NativeError::new(
                            NativeErrorCode::IoError,
                            "failed to inspect verified native notice peer address",
                        )
                    })?
                    .as_pathname()
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "verified native notice endpoint socket must have a pathname",
                        )
                    })?
                    .to_path_buf();
                drop(stream);
                let target =
                    threadlane::native_tool_output::NativeToolOutputConfig::new_user_message_steer(
                        socket_path,
                        self.thread_uuid.clone(),
                        ctx.approval.repository_path.clone(),
                    )
                    .with_reconnect(Some(reconnect_cfg.clone()));
                let notice_cfg = crate::coworker_notice::CoworkerNoticeConfig::new(
                    notice_key,
                    actual_digest.clone(),
                    target,
                )?;
                crate::coworker_notice::CoworkerNoticeStore::new(&notice_dir)?
                    .configure(notice_cfg)?;
            }
        }

        let flow_exe = self.resolve_sibling_flow_executable()?;
        let driver = CoworkerCodeDriver::open_from_dirs(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.authority_dir,
            &self.sessions_dir,
        )?
        .with_flow_binary_path(flow_exe);

        let owned_binding = self.binding_id.clone();
        let owned_thread = self.thread_uuid.clone();
        let owned_job = params.job_id.clone();
        // Isolate `ensure_owner` on a dedicated blocking thread with a current-thread runtime
        // because `CoworkerPipelineDriver` holds `!Sync` futures across `.await`, making it `!Send`.
        let report = tokio::task::spawn_blocking(move || -> Result<_, NativeError> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        "failed to build local execution runtime",
                    )
                })?;
            runtime.block_on(driver.ensure_owner(&owned_binding, &owned_thread, &owned_job, None))
        })
        .await
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                "failed to join local execution task",
            )
        })??;

        Ok(StartExecutionResponse::from_ensure_report(
            ctx.runtime_state.envelope.envelope_id,
            ctx.runtime_state.envelope.envelope_digest,
            report,
        ))
    }

    /// Reads the current verified [`CoworkerCodeDriver`] execution status for `params.job_id` in
    /// the enrolled `(binding_id, thread_uuid)` scope, including jobs superseded by a newer
    /// amendment so operators can still observe and drain them safely.
    pub async fn execution_status(
        &self,
        params: &ExecutionStatusParams,
    ) -> Result<ExecutionStatusResponse, NativeError> {
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        let ctx = self.require_verified_approved_job_for_status_or_stop(&params.job_id)?;

        let driver = CoworkerCodeDriver::open_from_dirs(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.authority_dir,
            &self.sessions_dir,
        )?;
        let report = driver
            .status(&self.binding_id, &self.thread_uuid, &params.job_id)
            .await?;

        let mut response = ExecutionStatusResponse::from_status_report(
            ctx.runtime_state.envelope.envelope_id,
            ctx.runtime_state.envelope.envelope_digest,
            report,
        );
        let notice_key = CoworkerJobOwnershipKey::new(
            self.binding_id.clone(),
            self.thread_uuid.clone(),
            params.job_id.clone(),
        )?;
        match crate::coworker_notice::CoworkerNoticeStore::inspect(
            &crate::coworker_notice::coworker_notice_dir(&self.plans_dir),
            &notice_key,
            &response.envelope_digest,
        ) {
            Ok(metadata) => response.native_notice = Some(metadata),
            Err(err) => response.native_notice_error = Some(err.code().to_string()),
        }
        Ok(response)
    }

    /// Records a durable operator stop request first and performs narrow [`CoworkerCodeDriver::cancel`]
    /// cancellation for `params.job_id` in the enrolled `(binding_id, thread_uuid)` scope,
    /// including jobs superseded by a newer amendment while refusing any new execution dispatch.
    pub async fn stop_execution(
        &self,
        params: &StopExecutionParams,
    ) -> Result<StopExecutionResponse, NativeError> {
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if let Some(ref reason) = params.reason {
            let trimmed = reason.trim();
            if trimmed.is_empty()
                || reason.len() > MAX_COWORKER_SUMMARY_BYTES
                || reason.chars().any(char::is_control)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "stop_execution reason must be a non-empty single-line string of at most {MAX_COWORKER_SUMMARY_BYTES} bytes without control characters"
                    ),
                ));
            }
        }

        let ctx = self.require_verified_approved_job_for_status_or_stop(&params.job_id)?;
        let driver = CoworkerCodeDriver::open_from_dirs(
            &self.state_dir,
            &self.registry_dir,
            &self.plans_dir,
            &self.authority_dir,
            &self.sessions_dir,
        )?;
        let report = driver
            .cancel(
                &self.binding_id,
                &self.thread_uuid,
                &params.job_id,
                params.reason.clone(),
            )
            .await?;

        Ok(StopExecutionResponse::from_cancel_report(
            ctx.runtime_state.envelope.envelope_id,
            ctx.runtime_state.envelope.envelope_digest,
            report,
        ))
    }

    fn require_verified_approved_job(
        &self,
        job_id: &str,
    ) -> Result<VerifiedScopedExecutionContext, NativeError> {
        self.verify_approved_job_scope(job_id, true)
    }

    fn require_verified_approved_job_for_status_or_stop(
        &self,
        job_id: &str,
    ) -> Result<VerifiedScopedExecutionContext, NativeError> {
        self.verify_approved_job_scope(job_id, false)
    }

    fn verify_approved_job_scope(
        &self,
        job_id: &str,
        require_not_superseded: bool,
    ) -> Result<VerifiedScopedExecutionContext, NativeError> {
        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        wf_ledger.validate(&self.binding_id)?;
        verify_ledger_thread_scope(&wf_ledger, &self.binding_id, &self.thread_uuid)?;

        let job =
            require_scoped_job(&wf_ledger, &self.binding_id, &self.thread_uuid, job_id)?.clone();
        if require_not_superseded {
            ensure_job_not_superseded(&wf_ledger, &job)?;
        }

        let Some(ref linkage) = job.approval_linkage else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                format!(
                    "Job `{}` needs human approval (`needs_approval`): no FrozenApprovalLinkage is bound. Approve the published plan and bind/freeze the execution envelope via `threadlane-flow coworker-admission preview|record` (inspect/reconcile after a lost response) before starting execution.",
                    job.job_id
                ),
            ));
        };
        let linkage = linkage.clone();
        linkage.validate()?;

        if linkage.controller_envelope.is_none() {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                format!(
                    "Job `{}` is missing a FrozenControllerCapabilityEnvelope (`missing_execution_envelope`): the existing approval linkage is immutable. Create a new job from a replanned, explicitly reapproved scope with controller capabilities, then use `threadlane-flow coworker-admission preview|record` before starting execution.",
                    job.job_id
                ),
            ));
        }

        let policy_store = ControllerPolicyStore::new(&self.plans_dir)?;
        let Some(runtime_state) =
            policy_store.get_job_runtime_readonly(&self.binding_id, &job.job_id)?
        else {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                format!(
                    "Job `{}` is missing a recorded FrozenControllerExecutionEnvelope (`missing_execution_envelope`): inspect/reconcile any existing admission receipt using its original request key, or use `threadlane-flow coworker-execution preview|record` for this legacy linked job. A different capability scope requires a new job, replan and explicit reapproval.",
                    job.job_id
                ),
            ));
        };

        if runtime_state.envelope.binding_id != self.binding_id
            || runtime_state.envelope.thread_uuid != self.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "controller_runtime_state envelope scope does not match verified binding_id and thread_uuid",
            ));
        }

        let (registry_store, plan_store) =
            open_workflow_stores(Some(&self.registry_dir), Some(&self.plans_dir))?.ok_or_else(
                || {
                    NativeError::new(
                        NativeErrorCode::NotConfigured,
                        "Coworker execution requires configured registry_dir and plans_dir",
                    )
                },
            )?;
        let authority = AuthorityStore::open(&self.authority_dir, registry_store, plan_store)
            .map_err(map_authority_error)?;

        let approval = load_scoped_approval(
            &authority,
            &linkage.approval_id,
            &self.binding_id,
            &self.thread_uuid,
        )?;

        verify_job_planning_and_envelope_consistency(
            &job,
            &linkage,
            &runtime_state,
            &approval,
            &authority,
            require_not_superseded,
        )?;

        Ok(VerifiedScopedExecutionContext {
            job,
            linkage,
            runtime_state,
            approval,
        })
    }
}

fn verify_ledger_thread_scope(
    wf_ledger: &CoworkerBindingLedger,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<(), NativeError> {
    if let Some(ref ledger_thread) = wf_ledger.thread_uuid {
        if ledger_thread != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "CoworkerBindingLedger for binding `{binding_id}` has thread_uuid `{ledger_thread}`, which does not match verified thread_uuid `{thread_uuid}`"
                ),
            ));
        }
    }
    Ok(())
}

fn verify_no_full_job_external_resolution(
    plans_dir: &Path,
    binding_id: &str,
    job: &CoworkerJobRecord,
) -> Result<(), NativeError> {
    let store = CoworkerWorkflowStore::new(plans_dir)?;
    let ledger = store.read_binding_ledger(binding_id)?;
    let target_plan_id = job
        .planning_result
        .as_ref()
        .map(|planning| planning.plan_id.as_str())
        .or_else(|| {
            job.approval_linkage
                .as_ref()
                .map(|linkage| linkage.plan_id.as_str())
        });
    let target_approval_id = job
        .approval_linkage
        .as_ref()
        .map(|linkage| linkage.approval_id.as_str());
    if let Some(block) = crate::dispatch_guard::external_resolution_blocks_full_job(
        &ledger,
        Some(&job.job_id),
        target_plan_id,
        target_approval_id,
    ) {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "job `{}` is covered by full-job external resolution receipt `{}`; the receipt is not review, integration, closure, or dependency acceptance, and execution requires an explicit replan and reapproval",
                job.job_id, block.receipt_id
            ),
        ));
    }
    Ok(())
}

fn require_scoped_job<'a>(
    wf_ledger: &'a CoworkerBindingLedger,
    binding_id: &str,
    thread_uuid: &str,
    job_id: &str,
) -> Result<&'a CoworkerJobRecord, NativeError> {
    let job = wf_ledger
        .jobs
        .iter()
        .find(|j| j.job_id == job_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("Coworker job `{job_id}` was not found in binding `{binding_id}`"),
            )
        })?;
    job.validate()?;
    if job.binding_id != binding_id || job.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "CoworkerJobRecord scope does not match verified binding_id and thread_uuid",
        ));
    }
    Ok(job)
}

fn ensure_job_not_superseded(
    wf_ledger: &CoworkerBindingLedger,
    job: &CoworkerJobRecord,
) -> Result<(), NativeError> {
    if wf_ledger
        .jobs
        .iter()
        .any(|other| other.supersedes_job_id.as_deref() == Some(job.job_id.as_str()))
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Coworker job `{}` has been superseded by a newer amendment job and cannot execute",
                job.job_id
            ),
        ));
    }
    Ok(())
}

fn load_scoped_approval(
    authority: &AuthorityStore,
    approval_id: &str,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<ApprovalRecord, NativeError> {
    let approval = authority
        .get_for_operator(approval_id)
        .map_err(map_authority_error)?;
    if approval.binding_id != binding_id || approval.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "ApprovalRecord scope does not match verified binding_id and thread_uuid",
        ));
    }
    Ok(approval)
}

fn verify_job_planning_and_envelope_consistency(
    job: &CoworkerJobRecord,
    linkage: &FrozenApprovalLinkage,
    runtime_state: &ControllerJobRuntimeState,
    approval: &ApprovalRecord,
    authority: &AuthorityStore,
    require_live_launch_eligibility: bool,
) -> Result<(), NativeError> {
    let Some(ref planning_result) = job.planning_result else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "Job `{}` has no Orchestrator PlanningResultRef; automatic execution requires verified planning provenance.",
                job.job_id
            ),
        ));
    };
    planning_result.validate()?;
    if !constant_time_str_eq(&planning_result.work_brief_digest, &job.work_brief_digest) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.planning_result.work_brief_digest does not match job.work_brief_digest",
        ));
    }
    if planning_result.project_base != job.work_brief.project_base {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.planning_result.project_base does not match job.work_brief.project_base",
        ));
    }
    if !job
        .work_brief
        .is_ready_for_execution_approval_with_planning(Some(planning_result))
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.work_brief has unresolved open_questions or unconfirmed proposed_additions not covered by planning_result",
        ));
    }
    if planning_result.plan_id != linkage.plan_id
        || planning_result.plan_revision != linkage.plan_revision
        || !constant_time_str_eq(&planning_result.plan_digest, &linkage.plan_digest)
        || planning_result.profile_id != linkage.profile_id
        || planning_result.registry_revision != linkage.registry_revision
        || planning_result.project_base != linkage.project_base
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.planning_result plan/profile/registry/project_base metadata does not match job.approval_linkage",
        ));
    }

    runtime_state.envelope.policy.validate()?;
    runtime_state.envelope.verify_matches_job(job)?;
    verify_immutable_approval_and_policy_linkage(approval, job, linkage, runtime_state)?;
    if require_live_launch_eligibility {
        verify_live_approval_and_registry(
            approval,
            job,
            &runtime_state.envelope.policy,
            authority,
        )?;
    }

    let coder_budget = runtime_state.envelope.coder_budget()?;
    if approval.budgets.timeout_ms > coder_budget.max_timeout_ms_per_call
        || approval.budgets.max_stdout_bytes > coder_budget.max_stdout_bytes_per_call
        || approval.budgets.max_stderr_bytes > coder_budget.max_stderr_bytes_per_call
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "ApprovalRecord `{}` worker execution budgets exceed frozen Coder role budget caps",
                approval.approval_id
            ),
        ));
    }

    Ok(())
}

fn verify_immutable_approval_and_policy_linkage(
    approval: &ApprovalRecord,
    job: &CoworkerJobRecord,
    linkage: &FrozenApprovalLinkage,
    runtime_state: &ControllerJobRuntimeState,
) -> Result<(), NativeError> {
    let policy = &runtime_state.envelope.policy;
    let computed_policy_digest = policy.digest()?;
    if policy.policy_id != linkage.policy_id
        || !constant_time_str_eq(&computed_policy_digest, &linkage.policy_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ExecutionPolicy does not match the job's FrozenApprovalLinkage policy_id or policy_digest.",
        ));
    }
    if approval.approval_id != linkage.approval_id
        || approval.subject_digest != linkage.approval_subject_digest
        || approval.binding_id != job.binding_id
        || approval.thread_uuid != job.thread_uuid
        || approval.plan_id != linkage.plan_id
        || approval.plan_revision != linkage.plan_revision
        || approval.plan_digest != linkage.plan_digest
        || approval.profile_id != linkage.profile_id
        || approval.registry_revision != linkage.registry_revision
        || approval.repository_path != job.work_brief.project_base.repository_path
        || approval.base_commit != job.work_brief.project_base.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord metadata drifted from the job's FrozenApprovalLinkage.",
        ));
    }
    Ok(())
}

fn resolve_frozen_task_title(
    task: &CoworkerTaskRecord,
    compiled_plan: Option<&CompiledPlan>,
    plan_revision: Option<&PlanRevision>,
    approval: Option<&ApprovalRecord>,
) -> Option<String> {
    if let Some(compiled) = compiled_plan {
        if let Some(ct) = compiled.tasks.iter().find(|ct| {
            ct.id == task.task_id && constant_time_str_eq(&ct.task_digest, &task.task_digest)
        }) {
            return Some(ct.title.clone());
        }
    }
    if let (Some(rev), Some(apr)) = (plan_revision, approval) {
        if apr.tasks.iter().any(|at| {
            at.task_id == task.task_id && constant_time_str_eq(&at.task_digest, &task.task_digest)
        }) {
            if let Some(pt) = rev.tasks.iter().find(|pt| pt.id == task.task_id) {
                return Some(pt.title.clone());
            }
        }
    }
    if let Some(rev) = plan_revision {
        if let Some(pt) = rev.tasks.iter().find(|pt| pt.id == task.task_id) {
            return Some(pt.title.clone());
        }
    }
    None
}

fn validate_existing_private_store_dir(path: &Path, label: &str) -> Result<(), NativeError> {
    validate_absolute_no_symlink_components(path, label)?;
    let meta = fs::symlink_metadata(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!(
                    "Required {label} directory does not exist at {}",
                    path.display()
                ),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to stat {label} at {}: {err}", path.display()),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "{label} at {} must be a non-symlink directory",
                path.display()
            ),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "{label} at {} must have 0700 permissions (found {mode:04o})",
                    path.display()
                ),
            ));
        }
    }
    Ok(())
}

fn map_authority_error(err: AuthorityError) -> NativeError {
    match err {
        AuthorityError::SecurityViolation { .. }
        | AuthorityError::BindingMismatch { .. }
        | AuthorityError::ThreadUuidMismatch { .. } => {
            NativeError::new(NativeErrorCode::SecurityViolation, err.to_string())
        }
        AuthorityError::CorruptStore { .. } => {
            NativeError::new(NativeErrorCode::ProtocolError, err.to_string())
        }
        AuthorityError::Io { .. } => NativeError::new(NativeErrorCode::IoError, err.to_string()),
        AuthorityError::Validation { .. }
        | AuthorityError::PlanNotFound { .. }
        | AuthorityError::ApprovalNotFound { .. }
        | AuthorityError::UnpublishedOrSupersededPlan { .. }
        | AuthorityError::StaleRegistryRevision { .. }
        | AuthorityError::PlanDigestMismatch { .. }
        | AuthorityError::UnknownTask { .. }
        | AuthorityError::UnsupportedTaskAdapter { .. }
        | AuthorityError::InvalidExecutable { .. }
        | AuthorityError::InvalidRepository { .. }
        | AuthorityError::InvalidBaseCommit { .. }
        | AuthorityError::InvalidWriteScope { .. }
        | AuthorityError::UnapprovedTaskWriteScope { .. }
        | AuthorityError::InvalidWorkArea { .. }
        | AuthorityError::WorkersConfigInvalid { .. }
        | AuthorityError::Registry { .. }
        | AuthorityError::Plan { .. } => {
            NativeError::new(NativeErrorCode::ValidationFailed, err.to_string())
        }
    }
}
