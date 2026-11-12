//! Scoped consultation and technical-planning gateway bridging verified native Coworker MCP calls
//! to [`CoworkerWorkflowStore`], [`PlanningCommandStore`], and the canonical same-package sibling
//! `threadlane-flow` CLI.
//!
//! ## Core Invariants
//!
//! - **Caller-supplied scope is never trusted from tool arguments**: `binding_id`, `thread_uuid`,
//!   store directory paths, `project_base`, `profile_id`, `policy`, model choices, and wall-clock
//!   timestamps are injected strictly from verified server configuration, existing operator-attested
//!   standing [`PlanningAdviceGrant`] records, and scoped [`CoworkerJobRecord`] state.
//! - **No grant creation, self-approval, or code execution**: [`CoworkerPlanningGateway`] operates
//!   exclusively under an existing active [`PlanningAdviceGrant`]. `prepare_work` records or amends
//!   a structured [`WorkBrief`] and applies holds on affected tasks without granting approval or
//!   starting worker execution.
//! - **Real dependency file blobs and prior advice records**: `consult_architect` computes exact
//!   64-character SHA-256 file blob digests at `project_base.base_commit` via
//!   [`CoworkerPlanningService::compute_dependency_file_blobs`], requires at least one real file
//!   context path, resolves prior advice identifiers strictly against stored
//!   [`ArchitectAdviceRecord`] entries in the same `(binding_id, thread_uuid)` scope, and always
//!   sets `accept_into_brief: false`.
//! - **Detached execution via canonical sibling `threadlane-flow`**: Because
//!   `start_planning_command_detached` resolves `std::env::current_exe()`, calling it directly from
//!   `threadlane-native-mcp` or a test binary would fork the wrong executable. The gateway instead
//!   validates and invokes only the canonical same-package sibling `threadlane-flow` binary with
//!   fixed global store/binding/thread flags and `coworker-planning start-command --command-id <ID>`,
//!   awaiting only the short bounded CLI launch receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_workflow::{
    ensure_mcp_response_budget, measure_mcp_response_bytes, Difficulty, PlanStatus, ScopedWorkflow,
    ToolAdapter, WorkflowError, WorkflowRole, MAX_MCP_RESPONSE_BYTES,
};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::coworker_grants::{
    list_planning_grant_summaries, PlanningGrantSummary, TrustedGrantScope,
};
use crate::coworker_workflow::{
    deterministic_prefixed_id, validate_bounded_text, validate_non_native_worker_choice,
    validate_unique_id_list, validate_unique_text_list, verify_grant_registry_dependencies,
    ArchitectAdviceRecord, ArchitectConsultationRequestOutcome, ArchitectContextDependencies,
    ArchitectDecisionStatus, CanonicalProjectBaseRef, CoworkerBindingLedger, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerOwnershipSummary, CoworkerPlanningService, CoworkerTaskStage,
    CoworkerWorkflowStore, CreateCoworkerJobRequest, HumanClarificationRequestOutcome,
    OrchestrateWorkBriefPlanRequest, PersistedPlanningCallArtifact, PlanningAdviceGrant,
    PlanningArtifactOutcomeKind, PlanningCallBudget, PlanningGrantStatus, PlanningGrantUsage,
    PlanningReadOnlyCapabilities, PlanningResultRef, PriorArchitectAdviceContext,
    RunArchitectAdviceRequest, StructuredArchitectAdviceOutput, StructuredOrchestratorOutput,
    WorkBrief, WorkBriefKind, MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING, MAX_COWORKER_ID_LEN,
    MAX_ORCHESTRATOR_ARCHITECT_ROUNDS, MAX_PLANNING_ARTIFACT_BYTES, MAX_TASKS_PER_JOB,
    MAX_WORK_BRIEF_ITEMS,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    ensure_platform_supported, prepare_private_dir, read_private_file_bounded,
    validate_absolute_no_symlink_components, validate_bounded_identifier,
};
use crate::planning_commands::{
    current_wall_clock_ms, EnqueuePlanningCommandRequest, PlanningCommandPayload,
    PlanningCommandResultRef, PlanningCommandStatus, PlanningCommandStore,
};
use crate::planning_driver::{
    enqueue_planning_command_summary, get_planning_command_summary_readonly,
    planning_artifacts_dir, planning_commands_dir, read_planning_commands_for_status_readonly,
    PlanningCommandEvidenceStatus, PlanningCommandStartOutcome, PlanningCommandStartStatus,
    PlanningCommandStatusObservation, PlanningCommandSummary, PlanningCommandVerificationIssue,
    PlanningCommandVerificationIssueKind,
};
use crate::profile_flow::{
    preflight, PreflightRequest, RoleExecutionRoute, RolePreflightRequest, SelectedRoleChoice,
};
use crate::role_flow::read_registry_snapshot_readonly;

/// Canonical binary name of the sibling trusted-local role-flow CLI.
pub const DEFAULT_FLOW_EXECUTABLE_NAME: &str = "threadlane-flow";

/// Maximum number of grant, job, advice, or command summary entries returned by `coworker_status`.
pub const MAX_COWORKER_STATUS_LIMIT: usize = 20;

/// Default number of grant, job, advice, or command summary entries returned by `coworker_status`.
pub const DEFAULT_COWORKER_STATUS_LIMIT: usize = 20;

/// Bounded timeout (`15s`) when awaiting the short `threadlane-flow ... start-command` launch receipt.
pub const DEFAULT_FLOW_START_COMMAND_TIMEOUT_MS: u64 = 15_000;

/// Maximum stdout byte budget (`256 KiB`) captured from `threadlane-flow ... start-command`.
const MAX_FLOW_START_STDOUT_BYTES: usize = 256 * 1024;

/// Maximum stderr byte budget (`16 KiB`) captured from `threadlane-flow ... start-command`.
const MAX_FLOW_START_STDERR_BYTES: usize = 16 * 1024;

/// JSON Schema wrapper for [`Difficulty`] (`easy`, `medium`, `hard`, `extreme`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GatewayDifficulty {
    /// Easy difficulty tier.
    Easy,
    /// Medium difficulty tier.
    Medium,
    /// Hard difficulty tier.
    Hard,
    /// Extreme difficulty tier.
    Extreme,
}

impl From<GatewayDifficulty> for Difficulty {
    fn from(value: GatewayDifficulty) -> Self {
        match value {
            GatewayDifficulty::Easy => Self::Easy,
            GatewayDifficulty::Medium => Self::Medium,
            GatewayDifficulty::Hard => Self::Hard,
            GatewayDifficulty::Extreme => Self::Extreme,
        }
    }
}

impl From<Difficulty> for GatewayDifficulty {
    fn from(value: Difficulty) -> Self {
        match value {
            Difficulty::Easy => Self::Easy,
            Difficulty::Medium => Self::Medium,
            Difficulty::Hard => Self::Hard,
            Difficulty::Extreme => Self::Extreme,
        }
    }
}

/// Parameters for the scoped `coworker_status` MCP tool.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusParams {
    /// Optional maximum number of grant, job, advice, and command summary rows (`1..=20`, default `20`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Optional nested amendment target and impact declaration for [`PrepareWorkParams`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrepareWorkAmendmentParams {
    /// Existing [`CoworkerJobRecord::job_id`] being amended.
    pub target_job_id: String,
    /// Expected current `job_revision` of `target_job_id` (`>= 1`).
    #[serde(alias = "expected_job_revision")]
    pub target_job_revision: u64,
    /// Bounded human-readable reason for the brief amendment.
    #[serde(alias = "reason")]
    pub amendment_reason: String,
    /// Directly affected task identifiers on `target_job_id` to place on hold.
    #[serde(default, alias = "affected_task_ids", alias = "affected_tasks")]
    pub amended_target_task_ids: Vec<String>,
    /// Affected repository-relative write paths on `target_job_id`.
    #[serde(default, alias = "affected_write_paths", alias = "affected_paths")]
    pub amended_write_paths: Vec<String>,
    /// Affected logical interface identifiers on `target_job_id`.
    #[serde(default, alias = "affected_interfaces")]
    pub amended_interfaces: Vec<String>,
}

/// Parameters for the scoped `prepare_work` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrepareWorkParams {
    /// Active standing [`PlanningAdviceGrant::grant_id`] in this binding and thread scope.
    pub grant_id: String,
    /// Idempotent caller request key (`1..=128` ASCII identifier characters).
    pub request_key: String,
    /// Bounded summary of the work brief.
    pub summary: String,
    /// Explicitly human-confirmed requirements (`>= 1` required; labels data only, not execution authorization).
    pub confirmed_requirements: Vec<String>,
    /// Model-proposed scope or feature additions not yet confirmed by the human (kept distinct from `confirmed_requirements`).
    #[serde(default)]
    pub proposed_additions: Vec<String>,
    /// Explicit assumptions recorded in the work brief.
    #[serde(default)]
    pub assumptions: Vec<String>,
    /// Unresolved open questions requiring human clarification before execution approval.
    #[serde(default)]
    pub open_questions: Vec<String>,
    /// Accepted [`ArchitectAdviceRecord::advice_id`]s incorporate into the brief.
    #[serde(default)]
    pub accepted_architect_decisions: Vec<String>,
    /// Optional structured amendment target and affected scope declaration.
    #[serde(default)]
    pub amendment: Option<PrepareWorkAmendmentParams>,
    /// Optional flat amendment target job identifier (`job_<32hex>`).
    #[serde(default, alias = "amendment_target_job_id")]
    pub target_job_id: Option<String>,
    /// Optional flat expected current `job_revision` of `target_job_id` (`>= 1`).
    #[serde(default, alias = "expected_job_revision")]
    pub target_job_revision: Option<u64>,
    /// Optional flat bounded reason for amending `target_job_id`.
    #[serde(default, alias = "reason")]
    pub amendment_reason: Option<String>,
    /// Optional flat list of directly affected task identifiers on `target_job_id`.
    #[serde(default, alias = "affected_task_ids", alias = "affected_tasks")]
    pub amended_target_task_ids: Vec<String>,
    /// Optional flat list of affected repository-relative write paths on `target_job_id`.
    #[serde(default, alias = "affected_write_paths", alias = "affected_paths")]
    pub amended_write_paths: Vec<String>,
    /// Optional flat list of affected logical interfaces on `target_job_id`.
    #[serde(default, alias = "affected_interfaces")]
    pub amended_interfaces: Vec<String>,
    /// Optional expected current Coworker workflow binding ledger revision (CAS check).
    #[serde(default)]
    pub expected_ledger_revision: Option<u64>,
}

/// Parameters for the scoped `consult_architect` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConsultArchitectParams {
    /// Active standing [`PlanningAdviceGrant::grant_id`] in this binding and thread scope.
    pub grant_id: String,
    /// Target [`CoworkerJobRecord::job_id`] in this binding and thread scope.
    pub job_id: String,
    /// Idempotent caller request key (`1..=128` ASCII identifier characters).
    pub request_key: String,
    /// Bounded architectural question summary to evaluate.
    #[serde(alias = "question_summary")]
    pub question: String,
    /// Non-empty list of repository-relative file paths (`>= 1` required) at `project_base.base_commit`.
    pub dependency_files: Vec<String>,
    /// Optional difficulty tier (`easy`, `medium`, `hard`, or `extreme`) used to select the Architect worker.
    #[serde(default)]
    #[schemars(with = "Option<GatewayDifficulty>")]
    pub difficulty: Option<Difficulty>,
    /// Optional list of existing verified [`ArchitectAdviceRecord::advice_id`]s in this binding and thread scope.
    #[serde(default, alias = "prior_architect_advice_ids")]
    pub prior_advice_ids: Vec<String>,
    /// Optional expected current Coworker workflow binding ledger revision (CAS check).
    #[serde(default)]
    pub expected_ledger_revision: Option<u64>,
}

/// Parameters for the scoped `request_technical_plan` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestTechnicalPlanParams {
    /// Active standing [`PlanningAdviceGrant::grant_id`] in this binding and thread scope.
    pub grant_id: String,
    /// Target [`CoworkerJobRecord::job_id`] in this binding and thread scope.
    pub job_id: String,
    /// Idempotent caller request key (`1..=128` ASCII identifier characters).
    pub request_key: String,
    /// Optional difficulty tier (`easy`, `medium`, `hard`, or `extreme`) used to select the Orchestrator worker.
    #[serde(default)]
    #[schemars(with = "Option<GatewayDifficulty>")]
    pub difficulty: Option<Difficulty>,
    /// Optional existing workflow `plan_id` to update (`expected_plan_revision` required when updating).
    #[serde(default)]
    pub plan_id: Option<String>,
    /// Optional expected current workflow plan revision (`0` when creating a new plan).
    #[serde(default)]
    pub expected_plan_revision: Option<u64>,
    /// Optional list of verified [`ArchitectAdviceRecord::advice_id`]s to attach to the Orchestrator context.
    #[serde(default, alias = "prior_advice_ids")]
    pub architect_advice_ids: Vec<String>,
    /// Optional bounded maximum automatic Architect consultation rounds (`0..=2`). When omitted, defaults to a bounded internal allowance up to `2` rounds when the active standing grant's policy and remaining call budget permit a headless Architect.
    #[serde(default)]
    pub max_architect_rounds: Option<u32>,
    /// Optional difficulty tier override for automatic Architect consultation rounds.
    #[serde(default)]
    #[schemars(with = "Option<GatewayDifficulty>")]
    pub architect_difficulty: Option<Difficulty>,
    /// Optional expected current Coworker workflow binding ledger revision (CAS check).
    #[serde(default)]
    pub expected_ledger_revision: Option<u64>,
}

/// Parameters for the scoped `planning_command` MCP tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PlanningCommandParams {
    /// Target planning command identifier (`pcm_<32hex>`).
    pub command_id: String,
}

/// Compact, prompt-redacted summary of a [`WorkBrief`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerWorkBriefSummary {
    /// Deterministic brief identifier (`brf_<32hex>`).
    pub brief_id: String,
    /// 64-character lowercase hex SHA-256 digest of the [`WorkBrief`].
    pub work_brief_digest: String,
    /// Whether this brief starts a new job or amends an existing job revision.
    pub kind: WorkBriefKind,
    /// Canonical repository path and immutable base commit/tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Bounded summary of the brief.
    pub summary: String,
    /// Explicitly human-confirmed requirements (data label only; never grants execution approval).
    pub confirmed_requirements: Vec<String>,
    /// Unconfirmed model-proposed additions kept separate from confirmed requirements.
    pub proposed_additions: Vec<String>,
    /// Explicit assumptions recorded for the brief.
    pub assumptions: Vec<String>,
    /// Unresolved open questions requiring human clarification before execution approval.
    pub open_questions: Vec<String>,
    /// Accepted architectural decision identifiers incorporated into the brief.
    pub accepted_architect_decisions: Vec<String>,
    /// Whether the brief satisfies structural readiness for Orchestrator technical planning.
    pub ready_for_execution_planning: bool,
    /// Whether the brief has zero open questions and is structurally ready for human approval after planning.
    pub ready_for_execution_approval: bool,
}

impl CoworkerWorkBriefSummary {
    /// Builds a [`CoworkerWorkBriefSummary`] from a validated [`WorkBrief`] and its digest.
    pub fn from_brief(brief: &WorkBrief, work_brief_digest: &str) -> Self {
        Self {
            brief_id: brief.brief_id.clone(),
            work_brief_digest: work_brief_digest.to_string(),
            kind: brief.kind.clone(),
            project_base: brief.project_base.clone(),
            summary: brief.summary.clone(),
            confirmed_requirements: brief.confirmed_requirements.clone(),
            proposed_additions: brief.proposed_additions.clone(),
            assumptions: brief.assumptions.clone(),
            open_questions: brief.open_questions.clone(),
            accepted_architect_decisions: brief.accepted_architect_decisions.clone(),
            ready_for_execution_planning: brief.is_ready_for_execution_planning(),
            ready_for_execution_approval: brief.is_ready_for_execution_approval(),
        }
    }
}

/// Compact, prompt-redacted summary of a [`CoworkerJobRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobSummary {
    /// Deterministic job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Monotonically increasing 1-based job revision.
    pub job_revision: u64,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Idempotent request key that created this job.
    pub create_request_key: String,
    /// Current lifecycle phase of the job.
    pub phase: CoworkerJobPhase,
    /// Deterministic [`WorkBrief::brief_id`].
    pub brief_id: String,
    /// 64-character lowercase hex SHA-256 digest of the job's [`WorkBrief`].
    pub work_brief_digest: String,
    /// Whether this job is a new job or an amendment of a prior job revision.
    pub kind: WorkBriefKind,
    /// Canonical repository path and base commit/tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Bounded summary of the job's [`WorkBrief`].
    pub summary: String,
    /// Count of confirmed requirements in the brief.
    pub confirmed_requirement_count: usize,
    /// Count of unconfirmed proposed additions in the brief.
    pub proposed_addition_count: usize,
    /// Count of assumptions in the brief.
    pub assumption_count: usize,
    /// Count of unresolved open questions in the brief.
    pub open_question_count: usize,
    /// Count of accepted architect decisions in the brief.
    pub accepted_architect_decision_count: usize,
    /// Whether the brief is ready for Orchestrator technical planning.
    pub ready_for_execution_planning: bool,
    /// Whether the brief has zero open questions and is ready for execution approval once planned.
    pub ready_for_execution_approval: bool,
    /// Superseded job identifier when `kind` is [`WorkBriefKind::Amendment`].
    pub supersedes_job_id: Option<String>,
    /// Linked published [`PlanningResultRef`] when Orchestrator planning has completed.
    pub planning_result: Option<PlanningResultRef>,
    /// Linked operator `approval_id` when execution has been approved.
    pub approval_id: Option<String>,
    /// Total number of task runtime records on the job.
    pub task_count: usize,
    /// Number of currently active [` crate::coworker_workflow::WorkflowHoldRecord`]s on the job.
    pub active_hold_count: usize,
    /// Number of unresolved (`ReservedBeforeEffect` or `EffectUnknown`) outbox records on the job.
    pub unresolved_outbox_count: usize,
}

impl CoworkerJobSummary {
    /// Builds a compact [`CoworkerJobSummary`] from a [`CoworkerJobRecord`].
    pub fn from_record(job: &CoworkerJobRecord) -> Self {
        let active_hold_count = job.holds.iter().filter(|h| h.active).count();
        let unresolved_outbox_count = job
            .outbox
            .iter()
            .filter(|o| o.status.blocks_replay_and_new_dispatch())
            .count();
        Self {
            job_id: job.job_id.clone(),
            job_revision: job.job_revision,
            binding_id: job.binding_id.clone(),
            thread_uuid: job.thread_uuid.clone(),
            create_request_key: job.create_request_key.clone(),
            phase: job.phase,
            brief_id: job.work_brief.brief_id.clone(),
            work_brief_digest: job.work_brief_digest.clone(),
            kind: job.work_brief.kind.clone(),
            project_base: job.work_brief.project_base.clone(),
            summary: job.work_brief.summary.clone(),
            confirmed_requirement_count: job.work_brief.confirmed_requirements.len(),
            proposed_addition_count: job.work_brief.proposed_additions.len(),
            assumption_count: job.work_brief.assumptions.len(),
            open_question_count: job.work_brief.open_questions.len(),
            accepted_architect_decision_count: job.work_brief.accepted_architect_decisions.len(),
            ready_for_execution_planning: job.work_brief.is_ready_for_execution_planning(),
            ready_for_execution_approval: job.work_brief.is_ready_for_execution_approval(),
            supersedes_job_id: job.supersedes_job_id.clone(),
            planning_result: job.planning_result.clone(),
            approval_id: job.approval_linkage.as_ref().map(|a| a.approval_id.clone()),
            task_count: job.tasks.len(),
            active_hold_count,
            unresolved_outbox_count,
        }
    }
}

/// Compact, prompt-redacted summary of a stored [`ArchitectAdviceRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerArchitectAdviceSummary {
    /// Deterministic advice identifier (`adv_<32hex>`).
    pub advice_id: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] that authorized the call.
    pub grant_id: String,
    /// Completed [`crate::coworker_workflow::PlanningCallReservation::reservation_id`].
    pub reservation_id: String,
    /// Idempotent request key for the advice record.
    pub request_key: String,
    /// Headless worker choice that produced the advice.
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the bound [`ArchitectContextDependencies`].
    pub context_digest: String,
    /// Bounded architectural decision summary.
    pub decision_summary: String,
    /// 64-character lowercase hex SHA-256 digest of the structured architectural decision.
    pub decision_digest: String,
    /// Persisted planning artifact identifier (`pln_<32hex>`), if recorded.
    pub report_artifact_id: Option<String>,
    /// Current lifecycle status of the architectural decision (`proposed`, `accepted_into_brief`, etc.).
    pub status: ArchitectDecisionStatus,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: u64,
}

impl CoworkerArchitectAdviceSummary {
    /// Builds a [`CoworkerArchitectAdviceSummary`] from an [`ArchitectAdviceRecord`].
    pub fn from_record(record: &ArchitectAdviceRecord) -> Self {
        Self {
            advice_id: record.advice_id.clone(),
            grant_id: record.grant_id.clone(),
            reservation_id: record.reservation_id.clone(),
            request_key: record.request_key.clone(),
            selected_choice: record.selected_choice.clone(),
            context_digest: record.context_digest.clone(),
            decision_summary: record.decision_summary.clone(),
            decision_digest: record.decision_digest.clone(),
            report_artifact_id: record.report_artifact_id.clone(),
            status: record.status,
            created_at_ms: record.created_at_ms,
        }
    }
}

/// Default dual-envelope (`structuredContent` + escaped `content[0].text`) wire byte budget per
/// free-form text field in `coworker_status` projections before whole-envelope byte fitting.
pub const DEFAULT_COWORKER_STATUS_TEXT_WIRE_BUDGET_BYTES: usize = 512;

/// Decreasing dual-envelope wire byte budgets tried per text field if single-row `coworker_status`
/// projections still approach [`MAX_MCP_RESPONSE_BYTES`].
const STATUS_TEXT_WIRE_BUDGET_STEPS: [usize; 5] = [
    DEFAULT_COWORKER_STATUS_TEXT_WIRE_BUDGET_BYTES,
    256,
    128,
    64,
    0,
];

/// Explicit row-count and MCP wire-byte-budget omission metadata for one `coworker_status` section.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusSectionBounds {
    /// Total matching records present in the scoped binding ledger.
    pub total_count: usize,
    /// Number of summary rows returned in this `coworker_status` response.
    pub returned_count: usize,
    /// `true` when `returned_count < total_count` (from either `limit` or the 256 KiB MCP wire budget).
    pub has_more: bool,
    /// Number of rows omitted because `total_count` exceeded the effective `limit` (`1..=20`).
    pub omitted_by_limit_count: usize,
    /// Number of rows omitted after `limit` slicing to keep the `CallToolResult` envelope `<= 262_144` bytes.
    pub omitted_by_byte_budget_count: usize,
    /// Number of returned rows in this section where at least one text field was truncated.
    pub truncated_text_row_count: usize,
    /// Number of returned rows in this section where nested map/list/health detail was projected to bounded metadata.
    pub projected_nested_detail_row_count: usize,
}

/// Status-specific bounded metadata projection of a [`PlanningGrantSummary`].
///
/// Preserves grant identity, scope, revisions, policy digest, capabilities, call budgets, usage
/// counters (including `unknown_effect_calls`), and status while bounding `revoked_reason` with
/// Unicode-safe, JSON-escape-aware truncation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusGrantSummary {
    /// Deterministic grant identifier (`pgr_<32hex>`).
    pub grant_id: String,
    /// Idempotent request key that created this grant.
    pub create_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Monotonic per-binding Coworker workflow ledger revision (`>= 1`).
    pub ledger_revision: u64,
    /// Canonical repository path and origin base commit/tree metadata.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier.
    pub profile_id: String,
    /// Pinned workflow registry revision.
    pub registry_revision: u64,
    /// Pinned execution policy identifier.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the pinned `ExecutionPolicy`.
    pub policy_digest: String,
    /// Strictly read-only capability envelope.
    pub capabilities: PlanningReadOnlyCapabilities,
    /// Finite call and per-call execution budget.
    pub budget: PlanningCallBudget,
    /// Current durable call usage counters (including `unknown_effect_calls`).
    pub usage: PlanningGrantUsage,
    /// Current persisted lifecycle status (`Active`, `Exhausted`, `Expired`, or `Revoked`).
    pub status: PlanningGrantStatus,
    /// Whether the grant is currently usable at query wall-clock time without pending unknown effects.
    pub currently_active: bool,
    /// Creation timestamp (ms since Unix epoch).
    pub created_at_ms: u64,
    /// Expiry timestamp (ms since Unix epoch).
    pub expires_at_ms: u64,
    /// Optional revocation timestamp (ms since Unix epoch).
    pub revoked_at_ms: Option<u64>,
    /// Optional Unicode-safe bounded revocation reason.
    pub revoked_reason: Option<String>,
    /// `true` when `revoked_reason` was shortened to fit the status text wire budget.
    pub revoked_reason_truncated: bool,
    /// Always `true` on persisted grants: trusted-local operator attestation was supplied.
    pub operator_attested: bool,
    /// Always `false`: local grant records never claim verified per-call native identity.
    pub native_per_call_identity_verified: bool,
    /// Number of durable call reservations recorded under this grant.
    pub reservation_count: usize,
}

impl CoworkerStatusGrantSummary {
    /// Projects a [`PlanningGrantSummary`] into a wire-bounded [`CoworkerStatusGrantSummary`].
    #[must_use]
    pub fn from_summary(
        grant: &PlanningGrantSummary,
        currently_active: bool,
        text_wire_budget: usize,
    ) -> Self {
        let (revoked_reason, revoked_reason_truncated) = match grant.revoked_reason.as_deref() {
            Some(reason) => {
                let (bounded, truncated) =
                    truncate_status_text_unicode_safe(reason, text_wire_budget);
                (Some(bounded), truncated)
            }
            None => (None, false),
        };
        Self {
            grant_id: grant.grant_id.clone(),
            create_request_key: grant.create_request_key.clone(),
            binding_id: grant.binding_id.clone(),
            thread_uuid: grant.thread_uuid.clone(),
            ledger_revision: grant.ledger_revision,
            project_base: grant.project_base.clone(),
            profile_id: grant.profile_id.clone(),
            registry_revision: grant.registry_revision,
            policy_id: grant.policy_id.clone(),
            policy_digest: grant.policy_digest.clone(),
            capabilities: grant.capabilities.clone(),
            budget: grant.budget.clone(),
            usage: grant.usage.clone(),
            status: grant.status,
            currently_active,
            created_at_ms: grant.created_at_ms,
            expires_at_ms: grant.expires_at_ms,
            revoked_at_ms: grant.revoked_at_ms,
            revoked_reason,
            revoked_reason_truncated,
            operator_attested: grant.operator_attested,
            native_per_call_identity_verified: grant.native_per_call_identity_verified,
            reservation_count: grant.reservation_count,
        }
    }
}

/// Non-authoritative, wire-bounded status projection of a [`PlanningResultRef`].
///
/// This type intentionally omits full `task_interfaces`, `architect_advice_ids`,
/// `resolved_open_questions`, and `deferred_proposals` payloads in favor of exact counts and
/// explicit omission flags, and carries `status_projection_only = true` plus
/// `grants_execution_authority = false` so it can never be mistaken for or deserialized as a full
/// [`PlanningResultRef`] or execution approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusPlanResultSummary {
    /// Always `true`: marks this object as a reduced `coworker_status` metadata projection, never a full [`PlanningResultRef`].
    pub status_projection_only: bool,
    /// Always `false`: `coworker_status` projections never grant execution or approval authority.
    pub grants_execution_authority: bool,
    /// Standing [`PlanningAdviceGrant::grant_id`] under which the plan was produced.
    pub grant_id: String,
    /// Completed Orchestrator `reservation_id`.
    pub reservation_id: String,
    /// Idempotent Orchestrator planning `request_key`.
    pub request_key: String,
    /// Supervised read-only worker `run_id` that produced the plan.
    pub run_id: String,
    /// Persisted private report artifact identifier (`pln_<32hex>`).
    pub report_artifact_id: String,
    /// 64-character lowercase hex SHA-256 digest of the verified structured Orchestrator output.
    pub output_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the planned [`WorkBrief`].
    pub work_brief_digest: String,
    /// Canonical repository and immutable base commit/tree SHA inspected during planning.
    pub project_base: CanonicalProjectBaseRef,
    /// Pinned workflow profile identifier from the standing grant.
    pub profile_id: String,
    /// Pinned workflow registry revision (`>= 1`) from the standing grant.
    pub registry_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the standing grant's `ExecutionPolicy`.
    pub policy_digest: String,
    /// Resolved Orchestrator worker choice that produced the plan.
    pub selected_choice: SelectedRoleChoice,
    /// Published workflow `plan_id` in `PlanStore`.
    pub plan_id: String,
    /// Published workflow `plan_revision` (`>= 1`) in `PlanStore`.
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 `plan_digest` of the published plan revision.
    pub plan_digest: String,
    /// Publication-time plan status recorded in [`PlanningResultRef`] (`published`).
    pub plan_status: PlanStatus,
    /// Verified live plan status in `PlanStore` when readable (for example `published` or `superseded`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_plan_status: Option<PlanStatus>,
    /// Explicit non-executable marker (`Some(false)`) when `current_plan_status` is no longer `published`; absence never grants authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    /// Timestamp (ms since Unix epoch) when the planning result was recorded.
    pub recorded_at_ms: u64,
    /// Number of task entries in [`PlanningResultRef::task_interfaces`].
    pub task_interface_task_count: usize,
    /// Total number of owned interface declarations across `task_interfaces`.
    pub owned_interface_count: usize,
    /// Total number of consumed interface declarations across `task_interfaces`.
    pub consumed_interface_count: usize,
    /// Number of [`ArchitectAdviceRecord::advice_id`] references on the result.
    pub architect_advice_id_count: usize,
    /// Number of `resolved_open_questions` entries on the result.
    pub resolved_open_question_count: usize,
    /// Number of `deferred_proposals` entries on the result.
    pub deferred_proposal_count: usize,
    /// `true` when non-empty `task_interfaces` map entries were omitted from this status projection.
    pub omitted_task_interfaces: bool,
    /// `true` when non-empty `architect_advice_ids` entries were omitted from this status projection.
    pub omitted_architect_advice_ids: bool,
    /// `true` when non-empty `resolved_open_questions` strings were omitted from this status projection.
    pub omitted_resolved_open_questions: bool,
    /// `true` when non-empty `deferred_proposals` strings were omitted from this status projection.
    pub omitted_deferred_proposals: bool,
    /// `true` when any nested collection on [`PlanningResultRef`] was omitted in favor of bounded counts.
    pub nested_detail_omitted: bool,
}

impl CoworkerStatusPlanResultSummary {
    /// Projects a full [`PlanningResultRef`] into a non-authoritative, wire-bounded summary.
    #[must_use]
    pub fn from_ref(pr: &PlanningResultRef, current_plan_status: Option<PlanStatus>) -> Self {
        let owned_interface_count = pr
            .task_interfaces
            .values()
            .map(|spec| spec.owned_interfaces.len())
            .sum();
        let consumed_interface_count = pr
            .task_interfaces
            .values()
            .map(|spec| spec.consumed_interfaces.len())
            .sum();
        let omitted_task_interfaces = !pr.task_interfaces.is_empty();
        let omitted_architect_advice_ids = !pr.architect_advice_ids.is_empty();
        let omitted_resolved_open_questions = !pr.resolved_open_questions.is_empty();
        let omitted_deferred_proposals = !pr.deferred_proposals.is_empty();
        let nested_detail_omitted = omitted_task_interfaces
            || omitted_architect_advice_ids
            || omitted_resolved_open_questions
            || omitted_deferred_proposals;
        let executable = current_plan_status
            .filter(|status| *status != PlanStatus::Published)
            .map(|_| false);

        Self {
            status_projection_only: true,
            grants_execution_authority: false,
            grant_id: pr.grant_id.clone(),
            reservation_id: pr.reservation_id.clone(),
            request_key: pr.request_key.clone(),
            run_id: pr.run_id.clone(),
            report_artifact_id: pr.report_artifact_id.clone(),
            output_digest: pr.output_digest.clone(),
            work_brief_digest: pr.work_brief_digest.clone(),
            project_base: pr.project_base.clone(),
            profile_id: pr.profile_id.clone(),
            registry_revision: pr.registry_revision,
            policy_digest: pr.policy_digest.clone(),
            selected_choice: pr.selected_choice.clone(),
            plan_id: pr.plan_id.clone(),
            plan_revision: pr.plan_revision,
            plan_digest: pr.plan_digest.clone(),
            plan_status: pr.plan_status,
            current_plan_status,
            executable,
            recorded_at_ms: pr.recorded_at_ms,
            task_interface_task_count: pr.task_interfaces.len(),
            owned_interface_count,
            consumed_interface_count,
            architect_advice_id_count: pr.architect_advice_ids.len(),
            resolved_open_question_count: pr.resolved_open_questions.len(),
            deferred_proposal_count: pr.deferred_proposals.len(),
            omitted_task_interfaces,
            omitted_architect_advice_ids,
            omitted_resolved_open_questions,
            omitted_deferred_proposals,
            nested_detail_omitted,
        }
    }
}

/// Status-specific bounded projection of [`WorkBriefKind`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoworkerStatusBriefKindSummary {
    /// Starts an independent new Coworker workflow job.
    NewJob,
    /// Amends an existing Coworker workflow job at an exact expected revision with bounded reason text.
    Amendment {
        /// Target job identifier being amended.
        target_job_id: String,
        /// Expected current `job_revision` of `target_job_id` (`>= 1`).
        target_job_revision: u64,
        /// Unicode-safe bounded human-readable reason for the amendment.
        amendment_reason: String,
        /// `true` when `amendment_reason` was shortened to fit the status text wire budget.
        amendment_reason_truncated: bool,
    },
}

impl CoworkerStatusBriefKindSummary {
    /// Projects a [`WorkBriefKind`] into a wire-bounded [`CoworkerStatusBriefKindSummary`].
    #[must_use]
    pub fn from_kind(kind: &WorkBriefKind, text_wire_budget: usize) -> Self {
        match kind {
            WorkBriefKind::NewJob => Self::NewJob,
            WorkBriefKind::Amendment {
                target_job_id,
                target_job_revision,
                amendment_reason,
            } => {
                let (bounded_reason, truncated) =
                    truncate_status_text_unicode_safe(amendment_reason, text_wire_budget);
                Self::Amendment {
                    target_job_id: target_job_id.clone(),
                    target_job_revision: *target_job_revision,
                    amendment_reason: bounded_reason,
                    amendment_reason_truncated: truncated,
                }
            }
        }
    }

    const fn has_truncated_text(&self) -> bool {
        match self {
            Self::NewJob => false,
            Self::Amendment {
                amendment_reason_truncated,
                ..
            } => *amendment_reason_truncated,
        }
    }
}

/// Status-specific, wire-bounded summary of a [`CoworkerJobRecord`].
///
/// Preserves job identity, revision, brief digest, phase, readiness flags, hold/outbox reconciliation
/// counts, and bounded non-authoritative plan linkage while truncating free-form text on UTF-8
/// codepoint boundaries and omitting nested task/interface collections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusJobSummary {
    /// Deterministic job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Monotonically increasing 1-based job revision.
    pub job_revision: u64,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Idempotent request key that created this job.
    pub create_request_key: String,
    /// Current lifecycle phase of the job.
    pub phase: CoworkerJobPhase,
    /// Deterministic [`WorkBrief::brief_id`].
    pub brief_id: String,
    /// 64-character lowercase hex SHA-256 digest of the job's [`WorkBrief`].
    pub work_brief_digest: String,
    /// Bounded new-job or amendment classification.
    pub kind: CoworkerStatusBriefKindSummary,
    /// Canonical repository path and base commit/tree SHA.
    pub project_base: CanonicalProjectBaseRef,
    /// Unicode-safe bounded summary of the job's [`WorkBrief`].
    pub summary: String,
    /// `true` when `summary` was shortened to fit the status text wire budget.
    pub summary_truncated: bool,
    /// Count of confirmed requirements in the brief.
    pub confirmed_requirement_count: usize,
    /// Count of unconfirmed proposed additions in the brief.
    pub proposed_addition_count: usize,
    /// Count of assumptions in the brief.
    pub assumption_count: usize,
    /// Count of unresolved open questions in the brief.
    pub open_question_count: usize,
    /// Count of accepted architect decisions in the brief.
    pub accepted_architect_decision_count: usize,
    /// Whether the brief is ready for Orchestrator technical planning.
    pub ready_for_execution_planning: bool,
    /// Whether the brief alone has zero open questions and zero proposed additions.
    pub ready_for_execution_approval: bool,
    /// Whether the brief plus recorded `planning_result` has resolved/deferred all open items for approval.
    pub ready_for_execution_approval_with_planning: bool,
    /// Superseded job identifier when `kind` is [`CoworkerStatusBriefKindSummary::Amendment`].
    pub supersedes_job_id: Option<String>,
    /// Non-authoritative bounded projection of the job's [`PlanningResultRef`], if planned.
    pub planning_result: Option<CoworkerStatusPlanResultSummary>,
    /// Linked operator `approval_id` when execution has been approved.
    pub approval_id: Option<String>,
    /// Always `false`: status projections never confer execution or controller authority.
    pub grants_execution_authority: bool,
    /// Total number of task runtime records on the job.
    pub task_count: usize,
    /// Number of currently active `WorkflowHoldRecord`s on the job.
    pub active_hold_count: usize,
    /// Number of unresolved (`ReservedBeforeEffect` or `EffectUnknown`) outbox records on the job.
    pub unresolved_outbox_count: usize,
    /// `true` when non-empty [`WorkBrief`] requirement/proposal/question lists were projected to counts.
    pub omitted_brief_item_lists: bool,
    /// `true` when non-empty runtime task, hold, outbox, or combined-candidate records were projected to counts.
    pub omitted_runtime_task_details: bool,
}

impl CoworkerStatusJobSummary {
    /// Projects a [`CoworkerJobRecord`] into a wire-bounded [`CoworkerStatusJobSummary`].
    #[must_use]
    pub fn from_record(
        job: &CoworkerJobRecord,
        current_plan_status: Option<PlanStatus>,
        text_wire_budget: usize,
    ) -> Self {
        let active_hold_count = job.holds.iter().filter(|h| h.active).count();
        let unresolved_outbox_count = job
            .outbox
            .iter()
            .filter(|o| o.status.blocks_replay_and_new_dispatch())
            .count();
        let (summary, summary_truncated) =
            truncate_status_text_unicode_safe(&job.work_brief.summary, text_wire_budget);
        let kind =
            CoworkerStatusBriefKindSummary::from_kind(&job.work_brief.kind, text_wire_budget);
        let omitted_brief_item_lists = !job.work_brief.confirmed_requirements.is_empty()
            || !job.work_brief.proposed_additions.is_empty()
            || !job.work_brief.assumptions.is_empty()
            || !job.work_brief.open_questions.is_empty()
            || !job.work_brief.accepted_architect_decisions.is_empty();
        let omitted_runtime_task_details = !job.tasks.is_empty()
            || !job.holds.is_empty()
            || !job.outbox.is_empty()
            || !job.combined_candidates.is_empty();

        Self {
            job_id: job.job_id.clone(),
            job_revision: job.job_revision,
            binding_id: job.binding_id.clone(),
            thread_uuid: job.thread_uuid.clone(),
            create_request_key: job.create_request_key.clone(),
            phase: job.phase,
            brief_id: job.work_brief.brief_id.clone(),
            work_brief_digest: job.work_brief_digest.clone(),
            kind,
            project_base: job.work_brief.project_base.clone(),
            summary,
            summary_truncated,
            confirmed_requirement_count: job.work_brief.confirmed_requirements.len(),
            proposed_addition_count: job.work_brief.proposed_additions.len(),
            assumption_count: job.work_brief.assumptions.len(),
            open_question_count: job.work_brief.open_questions.len(),
            accepted_architect_decision_count: job.work_brief.accepted_architect_decisions.len(),
            ready_for_execution_planning: job.work_brief.is_ready_for_execution_planning(),
            ready_for_execution_approval: job.work_brief.is_ready_for_execution_approval(),
            ready_for_execution_approval_with_planning: job
                .work_brief
                .is_ready_for_execution_approval_with_planning(job.planning_result.as_ref()),
            supersedes_job_id: job.supersedes_job_id.clone(),
            planning_result: job
                .planning_result
                .as_ref()
                .map(|pr| CoworkerStatusPlanResultSummary::from_ref(pr, current_plan_status)),
            approval_id: job.approval_linkage.as_ref().map(|a| a.approval_id.clone()),
            grants_execution_authority: false,
            task_count: job.tasks.len(),
            active_hold_count,
            unresolved_outbox_count,
            omitted_brief_item_lists,
            omitted_runtime_task_details,
        }
    }

    fn has_truncated_text(&self) -> bool {
        self.summary_truncated || self.kind.has_truncated_text()
    }

    fn has_projected_nested_detail(&self) -> bool {
        self.omitted_brief_item_lists
            || self.omitted_runtime_task_details
            || self
                .planning_result
                .as_ref()
                .is_some_and(|pr| pr.nested_detail_omitted)
    }
}

/// Status-specific, wire-bounded summary of a stored [`ArchitectAdviceRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusArchitectAdviceSummary {
    /// Deterministic advice identifier (`adv_<32hex>`).
    pub advice_id: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] that authorized the call.
    pub grant_id: String,
    /// Completed `reservation_id`.
    pub reservation_id: String,
    /// Idempotent request key for the advice record.
    pub request_key: String,
    /// Headless worker choice that produced the advice.
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the bound [`ArchitectContextDependencies`].
    pub context_digest: String,
    /// Unicode-safe bounded architectural decision summary.
    pub decision_summary: String,
    /// `true` when `decision_summary` was shortened to fit the status text wire budget.
    pub decision_summary_truncated: bool,
    /// 64-character lowercase hex SHA-256 digest of the structured architectural decision.
    pub decision_digest: String,
    /// Persisted planning artifact identifier (`pln_<32hex>`), if recorded.
    pub report_artifact_id: Option<String>,
    /// Current lifecycle status of the architectural decision (`proposed`, `accepted_into_brief`, etc.).
    pub status: ArchitectDecisionStatus,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: u64,
    /// Count of dependency file blob entries bound in `context_dependencies`.
    pub dependency_file_count: usize,
    /// Count of interface ownership digest entries bound in `context_dependencies`.
    pub interface_digest_count: usize,
    /// Count of accepted predecessor tree entries bound in `context_dependencies`.
    pub predecessor_tree_count: usize,
    /// `true` when non-empty `context_dependencies` maps were projected to counts.
    pub omitted_context_dependency_maps: bool,
}

impl CoworkerStatusArchitectAdviceSummary {
    /// Projects an [`ArchitectAdviceRecord`] into a wire-bounded [`CoworkerStatusArchitectAdviceSummary`].
    #[must_use]
    pub fn from_record(record: &ArchitectAdviceRecord, text_wire_budget: usize) -> Self {
        let (decision_summary, decision_summary_truncated) =
            truncate_status_text_unicode_safe(&record.decision_summary, text_wire_budget);
        let dependency_file_count = record.context_dependencies.dependency_file_blobs.len();
        let interface_digest_count = record
            .context_dependencies
            .interface_ownership_digests
            .len();
        let predecessor_tree_count = record.context_dependencies.accepted_predecessor_trees.len();
        let omitted_context_dependency_maps =
            dependency_file_count > 0 || interface_digest_count > 0 || predecessor_tree_count > 0;
        Self {
            advice_id: record.advice_id.clone(),
            grant_id: record.grant_id.clone(),
            reservation_id: record.reservation_id.clone(),
            request_key: record.request_key.clone(),
            selected_choice: record.selected_choice.clone(),
            context_digest: record.context_digest.clone(),
            decision_summary,
            decision_summary_truncated,
            decision_digest: record.decision_digest.clone(),
            report_artifact_id: record.report_artifact_id.clone(),
            status: record.status,
            created_at_ms: record.created_at_ms,
            dependency_file_count,
            interface_digest_count,
            predecessor_tree_count,
            omitted_context_dependency_maps,
        }
    }
}

/// Bounded summary of provider health telemetry attached to a [`CoworkerStatusCommandSummary`].
///
/// Full per-invocation `provider_health` and `provider_health_views` entries remain available via
/// `planning_command(command_id)`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusProviderHealthSummary {
    /// Number of reservation-scoped provider health entries recorded on the command.
    pub invocation_count: usize,
    /// Number of currently live diagnostic sources verified by physical ownership lock inspection.
    pub live_count: usize,
    /// Number of entries that have already observed terminal invocation completion.
    pub terminal_observed_count: usize,
    /// Sum of observed provider retry hints across recorded invocations.
    pub total_retry_count: u64,
    /// `true` when at least one recorded invocation currently reports an unrecovered failure hint.
    pub has_current_failure: bool,
    /// `true` when full `provider_health` / `provider_health_views` vectors were omitted from status.
    pub omitted_records: bool,
}

/// Kind of verified result projected on a completed [`CoworkerStatusCommandSummary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerStatusCommandResultKind {
    /// Completed `Architect` advice result.
    ArchitectAdvice,
    /// Completed `Orchestrator` published plan result.
    OrchestratorPlan,
    /// Completed `Orchestrator` result requesting human clarification.
    OrchestratorRequiresHumanClarification,
    /// Completed `Orchestrator` result requesting Architect consultation.
    OrchestratorRequiresArchitectAdvice,
}

/// Non-authoritative, wire-bounded status projection of a [`PlanningCommandResultRef`].
///
/// Full structured advice, plan task details, clarification question lists, and consultation
/// question lists remain available via `planning_command(command_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusCommandResultSummary {
    /// Always `true`: marks this object as a reduced `coworker_status` projection, never a full [`PlanningCommandResultRef`].
    pub status_projection_only: bool,
    /// Always `false`: status projections never confer execution or approval authority.
    pub grants_execution_authority: bool,
    /// Variant tag of the underlying verified [`PlanningCommandResultRef`].
    pub kind: CoworkerStatusCommandResultKind,
    /// `true` only when the persisted planning report artifact was directly validated on disk.
    pub artifact_verified: bool,
    /// Completed grant `reservation_id`.
    pub reservation_id: String,
    /// Persisted private report artifact identifier (`pln_<32hex>`).
    pub report_artifact_id: String,
    /// Target `job_id` for Orchestrator command outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Recorded `advice_id` for `ArchitectAdvice` outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advice_id: Option<String>,
    /// Whether an existing verified `ArchitectAdviceRecord` was reused without a new call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reused_existing: Option<bool>,
    /// 64-character lowercase hex SHA-256 `context_digest` for `ArchitectAdvice` outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_digest: Option<String>,
    /// 64-character lowercase hex SHA-256 `decision_digest` for `ArchitectAdvice` outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_digest: Option<String>,
    /// 64-character lowercase hex SHA-256 `output_digest` for Orchestrator outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_digest: Option<String>,
    /// Bounded non-authoritative plan result summary when `kind` is [`CoworkerStatusCommandResultKind::OrchestratorPlan`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_result: Option<CoworkerStatusPlanResultSummary>,
    /// Unicode-safe bounded outcome summary for clarification or consultation outcomes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// `true` when `summary` was shortened to fit the status text wire budget.
    pub summary_truncated: bool,
    /// Number of clarification or Architect consultation questions in the outcome.
    pub question_count: usize,
    /// Number of referenced or dependency files across the clarification/consultation outcome.
    pub referenced_file_count: usize,
    /// `true` when nested question lists, file lists, or plan interface/question/proposal lists were omitted.
    pub omitted_nested_detail: bool,
}

impl CoworkerStatusCommandResultSummary {
    /// Projects a verified [`PlanningCommandResultRef`] into a wire-bounded status summary.
    #[must_use]
    pub fn from_ref(
        result_ref: &PlanningCommandResultRef,
        current_plan_status: Option<PlanStatus>,
        text_wire_budget: usize,
    ) -> Self {
        match result_ref {
            PlanningCommandResultRef::ArchitectAdvice {
                advice_id,
                reservation_id,
                report_artifact_id,
                context_digest,
                decision_digest,
                reused_existing,
                artifact_verified,
            } => Self {
                status_projection_only: true,
                grants_execution_authority: false,
                kind: CoworkerStatusCommandResultKind::ArchitectAdvice,
                artifact_verified: *artifact_verified,
                reservation_id: reservation_id.clone(),
                report_artifact_id: report_artifact_id.clone(),
                job_id: None,
                advice_id: Some(advice_id.clone()),
                reused_existing: Some(*reused_existing),
                context_digest: Some(context_digest.clone()),
                decision_digest: Some(decision_digest.clone()),
                output_digest: None,
                plan_result: None,
                summary: None,
                summary_truncated: false,
                question_count: 0,
                referenced_file_count: 0,
                omitted_nested_detail: false,
            },
            PlanningCommandResultRef::OrchestratorPlan {
                job_id,
                planning_result,
                artifact_verified,
            } => {
                let projected_plan =
                    CoworkerStatusPlanResultSummary::from_ref(planning_result, current_plan_status);
                let omitted_nested_detail = projected_plan.nested_detail_omitted;
                Self {
                    status_projection_only: true,
                    grants_execution_authority: false,
                    kind: CoworkerStatusCommandResultKind::OrchestratorPlan,
                    artifact_verified: *artifact_verified,
                    reservation_id: planning_result.reservation_id.clone(),
                    report_artifact_id: planning_result.report_artifact_id.clone(),
                    job_id: Some(job_id.clone()),
                    advice_id: None,
                    reused_existing: None,
                    context_digest: None,
                    decision_digest: None,
                    output_digest: Some(planning_result.output_digest.clone()),
                    plan_result: Some(projected_plan),
                    summary: None,
                    summary_truncated: false,
                    question_count: 0,
                    referenced_file_count: 0,
                    omitted_nested_detail,
                }
            }
            PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
                job_id,
                reservation_id,
                report_artifact_id,
                output_digest,
                clarification,
                artifact_verified,
            } => {
                let (bounded_summary, summary_truncated) =
                    truncate_status_text_unicode_safe(&clarification.summary, text_wire_budget);
                let question_count = clarification.questions.len();
                let referenced_file_count = clarification.referenced_files.len();
                Self {
                    status_projection_only: true,
                    grants_execution_authority: false,
                    kind: CoworkerStatusCommandResultKind::OrchestratorRequiresHumanClarification,
                    artifact_verified: *artifact_verified,
                    reservation_id: reservation_id.clone(),
                    report_artifact_id: report_artifact_id.clone(),
                    job_id: Some(job_id.clone()),
                    advice_id: None,
                    reused_existing: None,
                    context_digest: None,
                    decision_digest: None,
                    output_digest: Some(output_digest.clone()),
                    plan_result: None,
                    summary: Some(bounded_summary),
                    summary_truncated,
                    question_count,
                    referenced_file_count,
                    omitted_nested_detail: question_count > 0 || referenced_file_count > 0,
                }
            }
            PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice {
                job_id,
                reservation_id,
                report_artifact_id,
                output_digest,
                consultation,
                artifact_verified,
            } => {
                let (bounded_summary, summary_truncated) =
                    truncate_status_text_unicode_safe(&consultation.summary, text_wire_budget);
                let question_count = consultation.questions.len();
                let referenced_file_count = consultation
                    .questions
                    .iter()
                    .map(|q| q.dependency_files.len())
                    .sum();
                Self {
                    status_projection_only: true,
                    grants_execution_authority: false,
                    kind: CoworkerStatusCommandResultKind::OrchestratorRequiresArchitectAdvice,
                    artifact_verified: *artifact_verified,
                    reservation_id: reservation_id.clone(),
                    report_artifact_id: report_artifact_id.clone(),
                    job_id: Some(job_id.clone()),
                    advice_id: None,
                    reused_existing: None,
                    context_digest: None,
                    decision_digest: None,
                    output_digest: Some(output_digest.clone()),
                    plan_result: None,
                    summary: Some(bounded_summary),
                    summary_truncated,
                    question_count,
                    referenced_file_count,
                    omitted_nested_detail: question_count > 0,
                }
            }
        }
    }
}

/// Bounded, non-authoritative status diagnostic for a recorded `Completed` planning command whose
/// persisted report artifact could not be proven during read-only status inspection.
///
/// Strictly separate from `Unknown` (`pending_reconciliation`) command state: this diagnostic
/// never relabels a recorded `Completed` command as `Running`, `Unknown`, awaiting approval, or
/// verified success, never exposes raw artifact text or filesystem paths, and cannot dispatch,
/// reconcile, or repair any state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusCommandVerificationIssue {
    /// Always `true`: marks this entry as a status-only read-only diagnostic.
    pub status_diagnostic_only: bool,
    /// Always `false`: degraded historical observations never grant execution or approval authority.
    pub grants_execution_authority: bool,
    /// Evidence verification status (`verification_failed` or `unavailable`), separate from the
    /// command's recorded `status`.
    pub evidence_status: PlanningCommandEvidenceStatus,
    /// Fixed, bounded reason category for the persisted artifact failure.
    pub kind: PlanningCommandVerificationIssueKind,
    /// Underlying strict verifier error code (`validation_failed`, `protocol_error`, or `output_overflow`).
    pub error_code: NativeErrorCode,
    /// Validated grant `reservation_id` referenced by the recorded command outcome.
    pub reservation_id: String,
    /// Validated persisted report artifact identifier (`pln_<32hex>`) referenced by the recorded command outcome.
    pub report_artifact_id: String,
    /// Fixed, software-owned diagnostic description free of untrusted artifact text or private paths.
    pub message: String,
}

impl CoworkerStatusCommandVerificationIssue {
    /// Projects a [`PlanningCommandVerificationIssue`] into a wire-bounded status diagnostic.
    #[must_use]
    pub fn from_issue(issue: &PlanningCommandVerificationIssue) -> Self {
        Self {
            status_diagnostic_only: true,
            grants_execution_authority: false,
            evidence_status: issue.evidence_status,
            kind: issue.kind,
            error_code: issue.error_code,
            reservation_id: issue.reservation_id.clone(),
            report_artifact_id: issue.report_artifact_id.clone(),
            message: issue.message.clone(),
        }
    }
}

/// Status-specific, wire-bounded summary of a [`PlanningCommandSummary`].
///
/// Preserves command identity, scope, digests, `status`, `pending_reconciliation`,
/// `current_plan_status`, `executable`, and `owner_summary`, while bounding `diagnostic` text and
/// projecting `result_ref` and `provider_health` into compact metadata summaries. When a recorded
/// `Completed` command's persisted report artifact fails verification or is unavailable, exposes
/// `verification_issue` with `result_summary == None` and `executable == Some(false)`. Full command
/// details and strict verification remain available via `planning_command(command_id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusCommandSummary {
    /// Deterministic command identifier (`pcm_<32hex>`).
    pub command_id: String,
    /// Idempotent caller request key.
    pub command_key: String,
    /// Frozen internal service request key (`psr_<32hex>`).
    pub service_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] authorizing this command.
    pub grant_id: String,
    /// Target `job_id` when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Target or published workflow `plan_id` when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Primary planning role (`architect` or `orchestrator`).
    pub role: WorkflowRole,
    /// Resolved headless worker choice verified against the grant profile and policy at enqueue.
    pub selected_choice: SelectedRoleChoice,
    /// Canonical repository and immutable base commit/tree verified at enqueue.
    pub call_project_base: CanonicalProjectBaseRef,
    /// 64-character lowercase hex SHA-256 context digest for the command.
    pub context_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the caller's semantic request body.
    pub request_body_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the immutable command fields and frozen payload.
    pub command_digest: String,
    /// Current command status (`queued`, `running`, `completed`, `known_failed`, or `unknown`).
    pub status: PlanningCommandStatus,
    /// Explicit marker that the command outcome is uncertain and requires artifact/receipt reconciliation.
    pub pending_reconciliation: bool,
    /// Public ownership summary bound when claimed by a live `CoworkerJobOwnershipGuard`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_summary: Option<CoworkerOwnershipSummary>,
    /// Bounded provider health count and liveness summary (`planning_command` exposes full records).
    pub provider_health_summary: CoworkerStatusProviderHealthSummary,
    /// Bounded non-authoritative result projection when `status == Completed` and evidence is verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_summary: Option<CoworkerStatusCommandResultSummary>,
    /// Bounded historical artifact verification diagnostic when `status == Completed` but persisted
    /// report evidence failed verification or was unavailable (`result_summary` is `None` and
    /// `executable` is `Some(false)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_issue: Option<CoworkerStatusCommandVerificationIssue>,
    /// Current persisted status of a verified Orchestrator plan, separate from its frozen result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_plan_status: Option<PlanStatus>,
    /// Explicit non-executable marker for a plan that is no longer `Published` or a degraded historical row; absence never grants authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    /// Unicode-safe bounded diagnostic message when `KnownFailed` or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// `true` when `diagnostic` was shortened to fit the status text wire budget.
    pub diagnostic_truncated: bool,
    /// Local wall-clock timestamp (ms since Unix epoch) when the command was enqueued.
    pub enqueued_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when claimed for `Running`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Local wall-clock timestamp (ms since Unix epoch) when last updated.
    pub updated_at_ms: u64,
    /// Local wall-clock timestamp (ms since Unix epoch) when `Completed` or `KnownFailed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

impl CoworkerStatusCommandSummary {
    /// Projects a verified [`PlanningCommandSummary`] into a wire-bounded [`CoworkerStatusCommandSummary`].
    #[must_use]
    pub fn from_summary(summary: &PlanningCommandSummary, text_wire_budget: usize) -> Self {
        let provider_health_summary = CoworkerStatusProviderHealthSummary {
            invocation_count: summary.provider_health.len(),
            live_count: summary
                .provider_health_views
                .iter()
                .filter(|v| v.live)
                .count(),
            terminal_observed_count: summary
                .provider_health
                .iter()
                .filter(|h| h.terminal_observed)
                .count(),
            total_retry_count: summary
                .provider_health
                .iter()
                .map(|h| u64::from(h.snapshot.retry_count))
                .sum(),
            has_current_failure: summary
                .provider_health
                .iter()
                .any(|h| h.snapshot.current_failure.is_some()),
            omitted_records: !summary.provider_health.is_empty(),
        };
        let result_summary = summary.result_ref.as_ref().map(|res| {
            CoworkerStatusCommandResultSummary::from_ref(
                res,
                summary.current_plan_status,
                text_wire_budget,
            )
        });
        let (diagnostic, diagnostic_truncated) = match summary.diagnostic.as_deref() {
            Some(diag) => {
                let (bounded, truncated) =
                    truncate_status_text_unicode_safe(diag, text_wire_budget);
                (Some(bounded), truncated)
            }
            None => (None, false),
        };

        Self {
            command_id: summary.command_id.clone(),
            command_key: summary.command_key.clone(),
            service_request_key: summary.service_request_key.clone(),
            binding_id: summary.binding_id.clone(),
            thread_uuid: summary.thread_uuid.clone(),
            grant_id: summary.grant_id.clone(),
            job_id: summary.job_id.clone(),
            plan_id: summary.plan_id.clone(),
            role: summary.role,
            selected_choice: summary.selected_choice.clone(),
            call_project_base: summary.call_project_base.clone(),
            context_digest: summary.context_digest.clone(),
            request_body_digest: summary.request_body_digest.clone(),
            command_digest: summary.command_digest.clone(),
            status: summary.status,
            pending_reconciliation: summary.pending_reconciliation,
            owner_summary: summary.owner_summary.clone(),
            provider_health_summary,
            result_summary,
            verification_issue: None,
            current_plan_status: summary.current_plan_status,
            executable: summary.executable,
            diagnostic,
            diagnostic_truncated,
            enqueued_at_ms: summary.enqueued_at_ms,
            started_at_ms: summary.started_at_ms,
            updated_at_ms: summary.updated_at_ms,
            completed_at_ms: summary.completed_at_ms,
        }
    }

    /// Projects a status-only [`PlanningCommandStatusObservation`] into a wire-bounded
    /// [`CoworkerStatusCommandSummary`], enforcing non-authoritative fields whenever
    /// `verification_issue` is present.
    #[must_use]
    pub fn from_status_observation(
        observation: &PlanningCommandStatusObservation,
        text_wire_budget: usize,
    ) -> Self {
        let mut projected = Self::from_summary(&observation.summary, text_wire_budget);
        if let Some(ref issue) = observation.verification_issue {
            projected.pending_reconciliation = false;
            projected.result_summary = None;
            projected.current_plan_status = None;
            projected.executable = Some(false);
            projected.verification_issue =
                Some(CoworkerStatusCommandVerificationIssue::from_issue(issue));
        }
        projected
    }

    fn has_truncated_text(&self) -> bool {
        self.diagnostic_truncated
            || self
                .result_summary
                .as_ref()
                .is_some_and(|res| res.summary_truncated)
    }

    fn has_projected_nested_detail(&self) -> bool {
        self.provider_health_summary.omitted_records
            || self
                .result_summary
                .as_ref()
                .is_some_and(|res| res.omitted_nested_detail)
    }
}

/// Bounded, prompt-redacted response returned by `coworker_status`.
///
/// Guaranteed to serialize inside [`MAX_MCP_RESPONSE_BYTES`] (`262_144` bytes) across the full
/// `rmcp` `CallToolResult` envelope (including both `structuredContent` and escaped JSON text in
/// `content`). Status entries are bounded projections and never grant execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerStatusResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical native Codex thread UUID.
    pub thread_uuid: String,
    /// Current revision of the per-binding Coworker workflow ledger (`0` when uninitialized).
    pub workflow_ledger_revision: u64,
    /// Current revision of the per-binding planning command ledger (`0` when uninitialized).
    pub command_ledger_revision: u64,
    /// Effective per-section row limit applied before wire-budget fitting (`1..=20`, default `20`).
    pub effective_limit: usize,
    /// Exact `grant_id` of the latest currently active standing grant, preserved even under extreme byte pressure.
    pub active_grant_id: Option<String>,
    /// Latest currently active, non-expired, non-revoked standing [`CoworkerStatusGrantSummary`], if any.
    pub active_grant: Option<CoworkerStatusGrantSummary>,
    /// `true` only if `active_grant_id` is `Some(_)` and `active_grant` had to be omitted to fit [`MAX_MCP_RESPONSE_BYTES`].
    pub active_grant_omitted_by_byte_budget: bool,
    /// Bounded list (`<= 20`) of currently active standing [`CoworkerStatusGrantSummary`] records.
    pub active_grants: Vec<CoworkerStatusGrantSummary>,
    /// Bounded list (`<= 20`) of standing [`CoworkerStatusGrantSummary`] records in this binding and thread scope.
    pub grants: Vec<CoworkerStatusGrantSummary>,
    /// Bounded list (`<= 20`) of compact [`CoworkerStatusJobSummary`] records in this binding and thread scope.
    pub jobs: Vec<CoworkerStatusJobSummary>,
    /// Bounded list (`<= 20`) of compact [`CoworkerStatusArchitectAdviceSummary`] records in this binding and thread scope.
    pub architect_advice: Vec<CoworkerStatusArchitectAdviceSummary>,
    /// Bounded list (`<= 20`) of prompt-redacted [`CoworkerStatusCommandSummary`] records in this binding and thread scope.
    pub commands: Vec<CoworkerStatusCommandSummary>,
    /// `true` if more currently active grants exist beyond `active_grants` (due to `limit` or byte budget).
    pub has_more_active_grants: bool,
    /// `true` if more grants exist beyond `grants` (due to `limit` or byte budget).
    pub has_more_grants: bool,
    /// `true` if more jobs exist beyond `jobs` (due to `limit` or byte budget).
    pub has_more_jobs: bool,
    /// `true` if more Architect advice records exist beyond `architect_advice` (due to `limit` or byte budget).
    pub has_more_architect_advice: bool,
    /// `true` if more commands exist beyond `commands` (due to `limit` or byte budget).
    pub has_more_commands: bool,
    /// `true` when at least one row (or `active_grant`) was omitted after `limit` slicing to stay within [`MAX_MCP_RESPONSE_BYTES`].
    pub rows_omitted_by_byte_budget: bool,
    /// `true` when any row, text field, or nested detail in this response was omitted or truncated for bounded status projection.
    pub details_omitted_or_truncated_by_byte_budget: bool,
    /// Scope-wide reconciliation flag across all grants, jobs, and commands in `(binding_id, thread_uuid)` (never hidden by pagination).
    pub pending_reconciliation: bool,
    /// Total `unknown_effect_calls` across all standing grants in `(binding_id, thread_uuid)`.
    pub unknown_effect_reservation_count: u32,
    /// Total unresolved (`ReservedBeforeEffect` or `EffectUnknown`) outbox records across all jobs in `(binding_id, thread_uuid)`.
    pub unresolved_job_outbox_count: usize,
    /// Total planning commands with `pending_reconciliation == true` across all commands in `(binding_id, thread_uuid)`.
    pub pending_reconciliation_command_count: usize,
    /// Scope-wide historical command evidence integrity flag across all commands in `(binding_id, thread_uuid)`
    /// (never hidden by `limit` pagination or byte-budget omission, and strictly separate from `pending_reconciliation`).
    pub historical_integrity_attention: bool,
    /// Total recorded `Completed` planning commands in `(binding_id, thread_uuid)` whose persisted report
    /// artifact failed verification or was unavailable (`verification_issue.is_some()`).
    pub degraded_completed_command_count: usize,
    /// Count and byte-budget omission breakdown for `active_grants`.
    pub active_grants_bounds: CoworkerStatusSectionBounds,
    /// Count and byte-budget omission breakdown for `grants`.
    pub grants_bounds: CoworkerStatusSectionBounds,
    /// Count and byte-budget omission breakdown for `jobs`.
    pub jobs_bounds: CoworkerStatusSectionBounds,
    /// Count and byte-budget omission breakdown for `architect_advice`.
    pub architect_advice_bounds: CoworkerStatusSectionBounds,
    /// Count and byte-budget omission breakdown for `commands`.
    pub commands_bounds: CoworkerStatusSectionBounds,
}

/// Compact response returned by `prepare_work`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareWorkResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical native Codex thread UUID.
    pub thread_uuid: String,
    /// Verified active standing [`PlanningAdviceGrant::grant_id`] under which the brief was prepared.
    pub grant_id: String,
    /// Deterministic [`CoworkerJobRecord::job_id`] (`job_<32hex>`).
    pub job_id: String,
    /// Current `job_revision` of `job_id`.
    pub job_revision: u64,
    /// Current [`CoworkerJobPhase`] of `job_id`.
    pub phase: CoworkerJobPhase,
    /// Deterministic [`WorkBrief::brief_id`] (`brf_<32hex>`).
    pub brief_id: String,
    /// 64-character lowercase hex SHA-256 digest of the [`WorkBrief`].
    pub work_brief_digest: String,
    /// Whether this brief starts a new job or amends an existing job revision.
    pub kind: WorkBriefKind,
    /// Canonical repository path and immutable base commit/tree SHA from the standing grant.
    pub project_base: CanonicalProjectBaseRef,
    /// Superseded job identifier when `kind` is [`WorkBriefKind::Amendment`].
    pub supersedes_job_id: Option<String>,
    /// Whether the brief is ready for Orchestrator technical planning.
    pub ready_for_execution_planning: bool,
    /// Whether the brief has zero open questions and is ready for human approval after planning.
    pub ready_for_execution_approval: bool,
    /// Active hold count on the superseded target job (when `kind` is `Amendment`, otherwise `0`).
    pub target_job_active_hold_count: usize,
    /// Compact summary of the created or replayed [`CoworkerJobRecord`].
    pub job: CoworkerJobSummary,
    /// Compact summary of the recorded [`WorkBrief`].
    pub work_brief: CoworkerWorkBriefSummary,
}

/// Honest submission and start status returned by `consult_architect` and `request_technical_plan`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCommandSubmissionResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical native Codex thread UUID.
    pub thread_uuid: String,
    /// Active standing [`PlanningAdviceGrant::grant_id`].
    pub grant_id: String,
    /// Scoped [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Deterministic planning command identifier (`pcm_<32hex>`).
    pub command_id: String,
    /// Target planning role (`architect` or `orchestrator`).
    pub role: WorkflowRole,
    /// Current durable [`PlanningCommandStatus`] observed in the command ledger.
    pub status: PlanningCommandStatus,
    /// High-level launch classification from `threadlane-flow ... start-command`.
    pub submission_status: PlanningCommandStartStatus,
    /// `true` when a new background execution subprocess was requested by this call.
    pub submission_requested: bool,
    /// `true` when the detached `threadlane-flow` execution subprocess was spawned.
    pub launched: bool,
    /// Advisory OS process ID hint for the detached worker driver when freshly spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid_hint: Option<u32>,
    /// Resolved headless worker choice frozen on the command.
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the command's input context.
    pub context_digest: String,
    /// Verified result reference when the command has already completed (e.g. via exact advice reuse or idempotent replay).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<PlanningCommandResultRef>,
    /// Full prompt-redacted [`PlanningCommandSummary`].
    pub command: PlanningCommandSummary,
    /// Read-only planning native notice metadata when inspection succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice: Option<crate::coworker_notice::PlanningNoticeMetadata>,
    /// Stable error code when planning native notice inspection fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice_error: Option<String>,
}

/// Prompt-redacted summary of a published Orchestrator technical plan extracted from a verified artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerOrchestratorPlanDetail {
    /// Published workflow plan identifier.
    pub plan_id: String,
    /// Published workflow plan revision (`>= 1`).
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan revision.
    pub plan_digest: String,
    /// Published workflow plan status (`published`).
    pub plan_status: PlanStatus,
    /// Bounded plan summary produced by the Orchestrator.
    pub summary: String,
    /// Number of implementation tasks in the published plan DAG.
    pub task_count: usize,
    /// Advisory check suggestions extracted from the Orchestrator output.
    #[serde(default)]
    pub check_suggestions: Vec<crate::coworker_workflow::PlannedCheckSuggestion>,
    /// Advisory notes on unconfirmed proposals kept strictly outside confirmed scope.
    #[serde(default)]
    pub unconfirmed_proposal_notes: Vec<String>,
    /// Open questions resolved during planning.
    #[serde(default)]
    pub resolved_open_questions: Vec<String>,
    /// Proposed additions explicitly deferred outside the published plan.
    #[serde(default)]
    pub deferred_proposals: Vec<String>,
}

/// Prompt-redacted status and verified result detail returned by `planning_command`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningCommandDetailResponse {
    /// Verified control-plane chat binding identifier.
    pub binding_id: String,
    /// Verified canonical native Codex thread UUID.
    pub thread_uuid: String,
    /// Deterministic planning command identifier (`pcm_<32hex>`).
    pub command_id: String,
    /// Standing [`PlanningAdviceGrant::grant_id`] that authorized the command.
    pub grant_id: String,
    /// Target [`CoworkerJobRecord::job_id`] when the command is an Orchestrator command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Target planning role (`architect` or `orchestrator`).
    pub role: WorkflowRole,
    /// Current durable [`PlanningCommandStatus`].
    pub status: PlanningCommandStatus,
    /// Whether the command is awaiting explicit reconciliation (`status == Unknown`).
    pub pending_reconciliation: bool,
    /// Resolved headless worker choice frozen on the command.
    pub selected_choice: SelectedRoleChoice,
    /// 64-character lowercase hex SHA-256 digest of the command's input context.
    pub context_digest: String,
    /// Verified result reference when `status` is [`PlanningCommandStatus::Completed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<PlanningCommandResultRef>,
    /// Verified current plan status, separate from the publication-time result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_plan_status: Option<PlanStatus>,
    /// Explicit non-executable marker; absence does not grant execution authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    /// Sanitized diagnostic message when `status` is `KnownFailed` or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    /// Verified structured Architect output (without raw prompts) when `result_ref` is `ArchitectAdvice`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_architect_advice: Option<StructuredArchitectAdviceOutput>,
    /// Verified Orchestrator plan metadata (without raw prompts) when `result_ref` is `OrchestratorPlan`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestrator_plan_summary: Option<CoworkerOrchestratorPlanDetail>,
    /// Verified human clarification request when `result_ref` is `OrchestratorRequiresHumanClarification`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clarification: Option<HumanClarificationRequestOutcome>,
    /// Verified Architect consultation request when `result_ref` is `OrchestratorRequiresArchitectAdvice`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consultation: Option<ArchitectConsultationRequestOutcome>,
    /// Full prompt-redacted [`PlanningCommandSummary`].
    pub command: PlanningCommandSummary,
    /// Read-only planning native notice metadata when inspection succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice: Option<crate::coworker_notice::PlanningNoticeMetadata>,
    /// Stable error code when planning native notice inspection fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FlowCliSuccessEnvelope<T> {
    binding_id: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct FlowCliErrorLine {
    code: String,
    message: String,
}

/// Scoped Coworker consultation and planning gateway bound to verified server store paths and a
/// verified `(binding_id, thread_uuid)` conversation scope.
///
/// Constructing a [`CoworkerPlanningGateway`] does not by itself verify per-call native thread
/// identity or create any grant or execution approval; callers on the paired MCP route must run
/// `verify_enrolled_binding_for_tool` before constructing and invoking the gateway.
#[derive(Debug, Clone)]
pub struct CoworkerPlanningGateway {
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

impl CoworkerPlanningGateway {
    /// Creates a new [`CoworkerPlanningGateway`] for a verified `(binding_id, thread_uuid)` scope
    /// and trusted store directory configuration.
    pub fn new(
        state_dir: impl Into<PathBuf>,
        registry_dir: impl Into<PathBuf>,
        plans_dir: impl Into<PathBuf>,
        authority_dir: impl Into<PathBuf>,
        sessions_dir: impl Into<PathBuf>,
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
        let state_dir = validate_absolute_no_symlink_components(&state_dir.into(), "state_dir")?;
        let registry_dir =
            validate_absolute_no_symlink_components(&registry_dir.into(), "registry_dir")?;
        let plans_dir = validate_absolute_no_symlink_components(&plans_dir.into(), "plans_dir")?;
        let authority_dir =
            validate_absolute_no_symlink_components(&authority_dir.into(), "authority_dir")?;
        let sessions_dir =
            validate_absolute_no_symlink_components(&sessions_dir.into(), "sessions_dir")?;

        Ok(Self {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            binding_id,
            thread_uuid,
            sibling_anchor_exe: None,
            managed_native_notice: None,
        })
    }

    /// Overrides the anchor executable path whose parent directory is searched for the canonical
    /// same-package sibling `threadlane-flow` binary.
    ///
    /// When set, only `<canonical_anchor_parent>/threadlane-flow` is inspected.
    pub fn with_sibling_anchor_exe(mut self, anchor_exe: impl Into<PathBuf>) -> Self {
        self.sibling_anchor_exe = Some(anchor_exe.into());
        self
    }

    /// Attaches a verified managed native reconnect configuration and MCP parent backend PID for
    /// configuring planning command completion notices during `consult_architect` and
    /// `request_technical_plan`.
    pub fn with_managed_native_notice(
        mut self,
        reconnect_config: threadlane::native_endpoint::NativeReconnectConfig,
        backend_pid: u32,
    ) -> Self {
        self.managed_native_notice = Some((reconnect_config, backend_pid));
        self
    }

    /// Returns the verified `binding_id` of this gateway instance.
    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    /// Returns the verified `thread_uuid` of this gateway instance.
    pub fn thread_uuid(&self) -> &str {
        &self.thread_uuid
    }

    /// Executes the read-only `coworker_status` query without creating directories, state files,
    /// or lockfiles, while guaranteeing that the serialized `rmcp` `CallToolResult` envelope stays
    /// within [`MAX_MCP_RESPONSE_BYTES`] (`262_144` bytes).
    pub fn coworker_status(
        &self,
        params: &CoworkerStatusParams,
    ) -> Result<CoworkerStatusResponse, NativeError> {
        let limit = validate_status_limit(params.limit)?;
        let now_ms = current_wall_clock_ms()?;

        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        if wf_ledger.binding_id != self.binding_id {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "Coworker workflow ledger binding_id does not match verified binding_id",
            ));
        }
        ensure_ledger_matches_thread(&wf_ledger, &self.thread_uuid)?;

        let scope = TrustedGrantScope::new(self.binding_id.clone(), self.thread_uuid.clone())?;
        let all_grants: Vec<(PlanningGrantSummary, bool)> =
            list_planning_grant_summaries(&workflow_store, &scope)?
                .into_iter()
                .map(|g| {
                    let active = is_grant_summary_currently_active(&g, now_ms);
                    (g, active)
                })
                .collect();
        let active_grants_all: Vec<(PlanningGrantSummary, bool)> = all_grants
            .iter()
            .filter(|(_, active)| *active)
            .cloned()
            .collect();

        let mut scoped_jobs: Vec<(&CoworkerJobRecord, Option<PlanStatus>)> =
            Vec::with_capacity(wf_ledger.jobs.len());
        for job in &wf_ledger.jobs {
            job.validate()?;
            if job.binding_id != self.binding_id || job.thread_uuid != self.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "CoworkerJobRecord scope does not match verified binding_id and thread_uuid",
                ));
            }
            let current_plan_status = job.planning_result.as_ref().and_then(|pr| {
                ScopedWorkflow::read_plan_summary_readonly_at(
                    &self.registry_dir,
                    &self.plans_dir,
                    &self.binding_id,
                    &pr.plan_id,
                    pr.plan_revision,
                )
                .ok()
                .map(|header| header.status)
            });
            scoped_jobs.push((job, current_plan_status));
        }

        let mut scoped_advice: Vec<&ArchitectAdviceRecord> =
            Vec::with_capacity(wf_ledger.architect_advice.len());
        for advice in &wf_ledger.architect_advice {
            advice.validate()?;
            if advice.binding_id != self.binding_id || advice.thread_uuid != self.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "ArchitectAdviceRecord scope does not match verified binding_id and thread_uuid",
                ));
            }
            scoped_advice.push(advice);
        }

        let cmd_report = read_planning_commands_for_status_readonly(
            &self.plans_dir,
            &self.registry_dir,
            &self.binding_id,
            &self.thread_uuid,
        )?;
        let all_commands = cmd_report.commands;

        let unknown_effect_reservation_count = all_grants.iter().fold(0u32, |acc, (g, _)| {
            acc.saturating_add(g.usage.unknown_effect_calls)
        });
        let unresolved_job_outbox_count: usize = scoped_jobs
            .iter()
            .map(|(job, _)| {
                job.outbox
                    .iter()
                    .filter(|o| o.status.blocks_replay_and_new_dispatch())
                    .count()
            })
            .sum();
        let pending_reconciliation_command_count = all_commands
            .iter()
            .filter(|obs| obs.summary.pending_reconciliation)
            .count();
        let pending_reconciliation = unknown_effect_reservation_count > 0
            || unresolved_job_outbox_count > 0
            || pending_reconciliation_command_count > 0;
        let degraded_completed_command_count = all_commands
            .iter()
            .filter(|obs| obs.verification_issue.is_some())
            .count();
        let historical_integrity_attention = degraded_completed_command_count > 0;

        let latest_active_grant = active_grants_all.last().cloned();
        let active_grant_id = latest_active_grant
            .as_ref()
            .map(|(g, _)| g.grant_id.clone());

        let mut active_grants_cap = active_grants_all.len().min(limit);
        let mut grants_cap = all_grants.len().min(limit);
        let mut jobs_cap = scoped_jobs.len().min(limit);
        let mut advice_cap = scoped_advice.len().min(limit);
        let mut commands_cap = all_commands.len().min(limit);
        let mut text_budget_idx = 0usize;
        let mut include_active_grant = latest_active_grant.is_some();

        loop {
            let text_wire_budget = STATUS_TEXT_WIRE_BUDGET_STEPS[text_budget_idx];
            let selected_active_grants = select_tail_preserving_attention(
                &active_grants_all,
                active_grants_cap,
                |(g, _)| g.usage.unknown_effect_calls > 0,
            );
            let selected_grants =
                select_tail_preserving_attention(&all_grants, grants_cap, |(g, _)| {
                    g.usage.unknown_effect_calls > 0
                });
            let selected_jobs =
                select_tail_preserving_attention(&scoped_jobs, jobs_cap, |(job, _)| {
                    job.outbox
                        .iter()
                        .any(|o| o.status.blocks_replay_and_new_dispatch())
                });
            let selected_advice =
                select_tail_preserving_attention(&scoped_advice, advice_cap, |_| false);
            let selected_commands =
                select_tail_preserving_attention(&all_commands, commands_cap, |obs| {
                    obs.summary.pending_reconciliation
                });

            let active_grant = if include_active_grant {
                latest_active_grant.as_ref().map(|(g, active)| {
                    CoworkerStatusGrantSummary::from_summary(g, *active, text_wire_budget)
                })
            } else {
                None
            };
            let active_grant_omitted_by_byte_budget =
                latest_active_grant.is_some() && !include_active_grant;

            let active_grants: Vec<CoworkerStatusGrantSummary> = selected_active_grants
                .iter()
                .map(|(g, active)| {
                    CoworkerStatusGrantSummary::from_summary(g, *active, text_wire_budget)
                })
                .collect();
            let grants: Vec<CoworkerStatusGrantSummary> = selected_grants
                .iter()
                .map(|(g, active)| {
                    CoworkerStatusGrantSummary::from_summary(g, *active, text_wire_budget)
                })
                .collect();
            let jobs: Vec<CoworkerStatusJobSummary> = selected_jobs
                .iter()
                .map(|(job, plan_status)| {
                    CoworkerStatusJobSummary::from_record(job, *plan_status, text_wire_budget)
                })
                .collect();
            let architect_advice: Vec<CoworkerStatusArchitectAdviceSummary> = selected_advice
                .iter()
                .map(|record| {
                    CoworkerStatusArchitectAdviceSummary::from_record(record, text_wire_budget)
                })
                .collect();
            let commands: Vec<CoworkerStatusCommandSummary> = selected_commands
                .iter()
                .map(|obs| {
                    CoworkerStatusCommandSummary::from_status_observation(obs, text_wire_budget)
                })
                .collect();

            let active_grants_bounds = build_section_bounds(
                active_grants_all.len(),
                limit,
                active_grants.len(),
                active_grants
                    .iter()
                    .filter(|g| g.revoked_reason_truncated)
                    .count(),
                0,
            );
            let grants_bounds = build_section_bounds(
                all_grants.len(),
                limit,
                grants.len(),
                grants.iter().filter(|g| g.revoked_reason_truncated).count(),
                0,
            );
            let jobs_bounds = build_section_bounds(
                scoped_jobs.len(),
                limit,
                jobs.len(),
                jobs.iter().filter(|j| j.has_truncated_text()).count(),
                jobs.iter()
                    .filter(|j| j.has_projected_nested_detail())
                    .count(),
            );
            let architect_advice_bounds = build_section_bounds(
                scoped_advice.len(),
                limit,
                architect_advice.len(),
                architect_advice
                    .iter()
                    .filter(|a| a.decision_summary_truncated)
                    .count(),
                architect_advice
                    .iter()
                    .filter(|a| a.omitted_context_dependency_maps)
                    .count(),
            );
            let commands_bounds = build_section_bounds(
                all_commands.len(),
                limit,
                commands.len(),
                commands.iter().filter(|c| c.has_truncated_text()).count(),
                commands
                    .iter()
                    .filter(|c| c.has_projected_nested_detail())
                    .count(),
            );

            let rows_omitted_by_byte_budget = active_grant_omitted_by_byte_budget
                || active_grants_bounds.omitted_by_byte_budget_count > 0
                || grants_bounds.omitted_by_byte_budget_count > 0
                || jobs_bounds.omitted_by_byte_budget_count > 0
                || architect_advice_bounds.omitted_by_byte_budget_count > 0
                || commands_bounds.omitted_by_byte_budget_count > 0;
            let details_omitted_or_truncated_by_byte_budget = rows_omitted_by_byte_budget
                || text_budget_idx > 0
                || active_grant
                    .as_ref()
                    .is_some_and(|g| g.revoked_reason_truncated)
                || active_grants_bounds.truncated_text_row_count > 0
                || grants_bounds.truncated_text_row_count > 0
                || jobs_bounds.truncated_text_row_count > 0
                || jobs_bounds.projected_nested_detail_row_count > 0
                || architect_advice_bounds.truncated_text_row_count > 0
                || architect_advice_bounds.projected_nested_detail_row_count > 0
                || commands_bounds.truncated_text_row_count > 0
                || commands_bounds.projected_nested_detail_row_count > 0;

            let candidate = CoworkerStatusResponse {
                binding_id: self.binding_id.clone(),
                thread_uuid: self.thread_uuid.clone(),
                workflow_ledger_revision: wf_ledger.ledger_revision,
                command_ledger_revision: cmd_report.ledger_revision,
                effective_limit: limit,
                active_grant_id: active_grant_id.clone(),
                active_grant,
                active_grant_omitted_by_byte_budget,
                has_more_active_grants: active_grants_bounds.has_more,
                has_more_grants: grants_bounds.has_more,
                has_more_jobs: jobs_bounds.has_more,
                has_more_architect_advice: architect_advice_bounds.has_more,
                has_more_commands: commands_bounds.has_more,
                rows_omitted_by_byte_budget,
                details_omitted_or_truncated_by_byte_budget,
                pending_reconciliation,
                unknown_effect_reservation_count,
                unresolved_job_outbox_count,
                pending_reconciliation_command_count,
                historical_integrity_attention,
                degraded_completed_command_count,
                active_grants,
                grants,
                jobs,
                architect_advice,
                commands,
                active_grants_bounds,
                grants_bounds,
                jobs_bounds,
                architect_advice_bounds,
                commands_bounds,
            };

            let wire_bytes =
                measure_mcp_response_bytes(&self.binding_id, "coworker_status", &candidate)
                    .map_err(map_status_wire_budget_error)?;

            if wire_bytes <= MAX_MCP_RESPONSE_BYTES {
                ensure_mcp_response_budget(&self.binding_id, "coworker_status", &candidate)
                    .map_err(map_status_wire_budget_error)?;
                return Ok(candidate);
            }

            let max_cap = active_grants_cap
                .max(grants_cap)
                .max(commands_cap)
                .max(jobs_cap)
                .max(advice_cap);
            if max_cap > 1 {
                if active_grants_cap == max_cap {
                    active_grants_cap -= 1;
                } else if grants_cap == max_cap {
                    grants_cap -= 1;
                } else if commands_cap == max_cap {
                    commands_cap -= 1;
                } else if jobs_cap == max_cap {
                    jobs_cap -= 1;
                } else {
                    advice_cap -= 1;
                }
                continue;
            }

            if text_budget_idx + 1 < STATUS_TEXT_WIRE_BUDGET_STEPS.len() {
                text_budget_idx += 1;
                continue;
            }
            if active_grants_cap > 0 {
                active_grants_cap = 0;
                continue;
            }
            if advice_cap > 0 {
                advice_cap = 0;
                continue;
            }
            if grants_cap > 0 {
                grants_cap = 0;
                continue;
            }
            if jobs_cap > 0 {
                jobs_cap = 0;
                continue;
            }
            if commands_cap > 0 {
                commands_cap = 0;
                continue;
            }
            if include_active_grant {
                include_active_grant = false;
                continue;
            }

            ensure_mcp_response_budget(&self.binding_id, "coworker_status", &candidate)
                .map_err(map_status_wire_budget_error)?;
            return Ok(candidate);
        }
    }

    /// Executes `prepare_work` under an existing active [`PlanningAdviceGrant`], deterministically
    /// deriving `brief_id` from `(binding_id, thread_uuid, request_key)` and recording a new job or
    /// typed amendment job via [`CoworkerWorkflowStore::create_or_amend_job`].
    pub fn prepare_work(
        &self,
        params: &PrepareWorkParams,
    ) -> Result<PrepareWorkResponse, NativeError> {
        validate_bounded_identifier(&params.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&params.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        let now_ms = current_wall_clock_ms()?;

        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        let grant = require_active_grant_for_scope(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.grant_id,
            now_ms,
        )?;
        grant.project_base.verify_live_repository()?;

        let resolved_amendment = resolve_work_brief_kind_and_amendment(params)?;

        let existing_for_key = wf_ledger
            .jobs
            .iter()
            .find(|j| {
                j.binding_id == self.binding_id
                    && j.thread_uuid == self.thread_uuid
                    && j.create_request_key == params.request_key
            })
            .cloned();

        let project_base = if let Some(ref existing_job) = existing_for_key {
            if existing_job.work_brief.project_base.repository_path
                != grant.project_base.repository_path
            {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "existing job project_base.repository_path does not match standing grant canonical repository_path",
                ));
            }
            existing_job
                .work_brief
                .project_base
                .verify_live_repository()?;
            if let WorkBriefKind::Amendment {
                ref target_job_id, ..
            } = resolved_amendment.kind
            {
                let hold_request_key = format!("amend_{}", params.request_key);
                if let Some(target_job) = wf_ledger.jobs.iter().find(|j| &j.job_id == target_job_id)
                {
                    if let Some(existing_hold) = target_job
                        .holds
                        .iter()
                        .find(|h| h.request_key == hold_request_key)
                    {
                        if existing_hold.directly_affected_task_ids
                            != resolved_amendment.amended_target_task_ids
                            || existing_hold.affected_write_paths
                                != resolved_amendment.amended_write_paths
                            || existing_hold.affected_interfaces
                                != resolved_amendment.amended_interfaces
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                "job request_key was already used with different amendment impact parameters",
                            ));
                        }
                    }
                }
            }
            existing_job.work_brief.project_base.clone()
        } else {
            let repo_path = Path::new(&grant.project_base.repository_path);
            let head_commit = threadlane_workers::resolve_exact_commit_sha(
                repo_path,
                "HEAD",
                threadlane_workers::DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "failed to resolve current HEAD commit SHA in standing grant repository",
                )
            })?;
            let current_base =
                CanonicalProjectBaseRef::resolve_and_validate(repo_path, &head_commit)?;
            if current_base.repository_path != grant.project_base.repository_path {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "resolved repository_path does not match standing grant canonical repository_path",
                ));
            }
            current_base
        };

        let brief_id = deterministic_prefixed_id(
            "threadlane-native:coworker-work-brief-id:v1",
            "brf",
            &[&self.binding_id, &self.thread_uuid, &params.request_key],
        );

        let work_brief = WorkBrief {
            brief_id: brief_id.clone(),
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            project_base,
            kind: resolved_amendment.kind,
            summary: params.summary.clone(),
            confirmed_requirements: params.confirmed_requirements.clone(),
            proposed_additions: params.proposed_additions.clone(),
            assumptions: params.assumptions.clone(),
            open_questions: params.open_questions.clone(),
            accepted_architect_decisions: params.accepted_architect_decisions.clone(),
        };
        work_brief.validate()?;

        validate_accepted_architect_decisions(
            &wf_ledger,
            grant,
            &self.binding_id,
            &self.thread_uuid,
            &work_brief,
            existing_for_key.is_some(),
        )?;

        let create_req = CreateCoworkerJobRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            request_key: params.request_key.clone(),
            expected_ledger_revision: params.expected_ledger_revision,
            work_brief,
            amended_target_task_ids: resolved_amendment.amended_target_task_ids,
            amended_write_paths: resolved_amendment.amended_write_paths,
            amended_interfaces: resolved_amendment.amended_interfaces,
        };

        let created_job = workflow_store.create_or_amend_job(&create_req)?;
        let target_job_active_hold_count =
            if let Some(ref target_id) = created_job.supersedes_job_id {
                let updated_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
                updated_ledger
                    .jobs
                    .iter()
                    .find(|j| &j.job_id == target_id)
                    .map(|j| j.holds.iter().filter(|h| h.active).count())
                    .unwrap_or(0)
            } else {
                0
            };

        let job_summary = CoworkerJobSummary::from_record(&created_job);
        let brief_summary = CoworkerWorkBriefSummary::from_brief(
            &created_job.work_brief,
            &created_job.work_brief_digest,
        );

        Ok(PrepareWorkResponse {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            grant_id: params.grant_id.clone(),
            job_id: created_job.job_id.clone(),
            job_revision: created_job.job_revision,
            phase: created_job.phase,
            brief_id: created_job.work_brief.brief_id.clone(),
            work_brief_digest: created_job.work_brief_digest.clone(),
            kind: created_job.work_brief.kind.clone(),
            project_base: created_job.work_brief.project_base.clone(),
            supersedes_job_id: created_job.supersedes_job_id.clone(),
            ready_for_execution_planning: brief_summary.ready_for_execution_planning,
            ready_for_execution_approval: brief_summary.ready_for_execution_approval,
            target_job_active_hold_count,
            job: job_summary,
            work_brief: brief_summary,
        })
    }

    /// Enqueues a dependency-bound Architect consultation command for `(grant_id, job_id)` and
    /// triggers detached execution via the canonical same-package sibling `threadlane-flow` CLI.
    pub async fn consult_architect(
        &self,
        params: &ConsultArchitectParams,
    ) -> Result<PlanningCommandSubmissionResponse, NativeError> {
        let gateway = self.clone();
        let params_cloned = params.clone();
        let (flow_exe, enqueue_req) = tokio::task::spawn_blocking(move || {
            let flow_exe = gateway.resolve_sibling_flow_executable()?;
            let enqueue_req = gateway.build_consult_architect_enqueue_request(&params_cloned)?;
            Ok::<_, NativeError>((flow_exe, enqueue_req))
        })
        .await
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                "consult_architect background preparation task failed to complete",
            )
        })??;
        self.enqueue_and_start_with_flow(&flow_exe, &params.job_id, enqueue_req)
            .await
    }

    /// Enqueues an Orchestrator technical planning command for `(grant_id, job_id)` and triggers
    /// detached execution via the canonical same-package sibling `threadlane-flow` CLI.
    pub async fn request_technical_plan(
        &self,
        params: &RequestTechnicalPlanParams,
    ) -> Result<PlanningCommandSubmissionResponse, NativeError> {
        let gateway = self.clone();
        let params_cloned = params.clone();
        let (flow_exe, enqueue_req) = tokio::task::spawn_blocking(move || {
            let flow_exe = gateway.resolve_sibling_flow_executable()?;
            let enqueue_req =
                gateway.build_request_technical_plan_enqueue_request(&params_cloned)?;
            Ok::<_, NativeError>((flow_exe, enqueue_req))
        })
        .await
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                "request_technical_plan background preparation task failed to complete",
            )
        })??;
        self.enqueue_and_start_with_flow(&flow_exe, &params.job_id, enqueue_req)
            .await
    }

    /// Reads the current status and verified structured outcome (without raw prompts) of a scoped
    /// planning command by `command_id`.
    pub fn planning_command(
        &self,
        params: &PlanningCommandParams,
    ) -> Result<PlanningCommandDetailResponse, NativeError> {
        validate_bounded_identifier(&params.command_id, "command_id", MAX_COWORKER_ID_LEN)?;
        let summary = get_planning_command_summary_readonly(
            &self.plans_dir,
            &self.registry_dir,
            &self.binding_id,
            &self.thread_uuid,
            &params.command_id,
        )?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "planning command `{}` was not found in binding `{}`",
                    params.command_id, self.binding_id
                ),
            )
        })?;

        if summary.binding_id != self.binding_id || summary.thread_uuid != self.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "planning command scope does not match verified binding_id and thread_uuid",
            ));
        }

        let mut structured_architect_advice = None;
        let mut orchestrator_plan_summary = None;
        let mut clarification = None;
        let mut consultation = None;

        if summary.status == PlanningCommandStatus::Completed {
            if let Some(ref result_ref) = summary.result_ref {
                match result_ref {
                    PlanningCommandResultRef::ArchitectAdvice {
                        report_artifact_id, ..
                    } => {
                        let artifact = self.read_verified_artifact_readonly(
                            report_artifact_id,
                            &summary.grant_id,
                        )?;
                        if let PlanningArtifactOutcomeKind::ArchitectAdvice {
                            structured_advice,
                            ..
                        } = artifact.outcome
                        {
                            structured_architect_advice = Some(structured_advice);
                        }
                    }
                    PlanningCommandResultRef::OrchestratorPlan {
                        planning_result, ..
                    } => {
                        let artifact = self.read_verified_artifact_readonly(
                            &planning_result.report_artifact_id,
                            &summary.grant_id,
                        )?;
                        if let PlanningArtifactOutcomeKind::OrchestratorOutput {
                            structured_output:
                                StructuredOrchestratorOutput::Plan {
                                    summary: plan_summary_text,
                                    tasks,
                                    check_suggestions,
                                    unconfirmed_proposal_notes,
                                    resolved_open_questions,
                                    deferred_proposals,
                                    ..
                                },
                            ..
                        } = artifact.outcome
                        {
                            orchestrator_plan_summary = Some(CoworkerOrchestratorPlanDetail {
                                plan_id: planning_result.plan_id.clone(),
                                plan_revision: planning_result.plan_revision,
                                plan_digest: planning_result.plan_digest.clone(),
                                plan_status: planning_result.plan_status,
                                summary: plan_summary_text,
                                task_count: tasks.len(),
                                check_suggestions,
                                unconfirmed_proposal_notes,
                                resolved_open_questions,
                                deferred_proposals,
                            });
                        }
                    }
                    PlanningCommandResultRef::OrchestratorRequiresHumanClarification {
                        clarification: c,
                        ..
                    } => {
                        clarification = Some(c.clone());
                    }
                    PlanningCommandResultRef::OrchestratorRequiresArchitectAdvice {
                        consultation: c,
                        ..
                    } => {
                        consultation = Some(c.clone());
                    }
                }
            }
        }

        let (native_notice, native_notice_error) = inspect_planning_notice_best_effort(
            &self.plans_dir,
            &self.binding_id,
            &self.thread_uuid,
            &summary,
        );

        Ok(PlanningCommandDetailResponse {
            binding_id: summary.binding_id.clone(),
            thread_uuid: summary.thread_uuid.clone(),
            command_id: summary.command_id.clone(),
            grant_id: summary.grant_id.clone(),
            job_id: summary.job_id.clone(),
            role: summary.role,
            status: summary.status,
            pending_reconciliation: summary.pending_reconciliation,
            selected_choice: summary.selected_choice.clone(),
            context_digest: summary.context_digest.clone(),
            result_ref: summary.result_ref.clone(),
            current_plan_status: summary.current_plan_status,
            executable: summary.executable,
            diagnostic: summary.diagnostic.clone(),
            structured_architect_advice,
            orchestrator_plan_summary,
            clarification,
            consultation,
            command: summary,
            native_notice,
            native_notice_error,
        })
    }

    /// Resolves and validates the canonical same-package sibling `threadlane-flow` executable.
    pub fn resolve_sibling_flow_executable(&self) -> Result<PathBuf, NativeError> {
        match &self.sibling_anchor_exe {
            Some(anchor) => resolve_sibling_flow_executable_from(anchor, false),
            None => resolve_sibling_flow_executable(),
        }
    }

    fn build_consult_architect_enqueue_request(
        &self,
        params: &ConsultArchitectParams,
    ) -> Result<EnqueuePlanningCommandRequest, NativeError> {
        validate_bounded_identifier(&params.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&params.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_text(&params.question, "question")?;

        if params.dependency_files.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "consult_architect requires at least one repository-relative path in dependency_files",
            ));
        }
        validate_unique_text_list(
            &params.dependency_files,
            "dependency_files",
            MAX_WORK_BRIEF_ITEMS,
        )?;
        validate_unique_id_list(
            &params.prior_advice_ids,
            "prior_advice_ids",
            MAX_WORK_BRIEF_ITEMS,
        )?;

        let now_ms = current_wall_clock_ms()?;
        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        let grant = require_active_grant_for_scope(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.grant_id,
            now_ms,
        )?;
        let job = require_scoped_job(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.job_id,
        )?;

        let project_base = job.work_brief.project_base.clone();
        if project_base.repository_path != grant.project_base.repository_path {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job project_base.repository_path does not match standing grant canonical repository_path",
            ));
        }

        let dependency_file_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
            &project_base,
            &params.dependency_files,
        )?;
        if dependency_file_blobs.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "consult_architect requires at least one resolved dependency file blob at project_base",
            ));
        }

        let prior_advice_context = resolve_prior_advice_records(
            &wf_ledger,
            grant,
            &self.binding_id,
            &self.thread_uuid,
            &params.prior_advice_ids,
        )?;

        let plan_digest = job
            .approval_linkage
            .as_ref()
            .map(|a| a.plan_digest.clone())
            .or_else(|| {
                job.planning_result
                    .as_ref()
                    .map(|pr| pr.plan_digest.clone())
            });
        let interface_ownership_digests = self.collect_job_interface_ownership_digests(job, grant);
        let accepted_predecessor_trees = collect_job_accepted_predecessor_trees(job);

        let context_dependencies = ArchitectContextDependencies {
            project_base,
            work_brief_digest: job.work_brief_digest.clone(),
            plan_digest,
            policy_digest: grant.policy_digest.clone(),
            dependency_file_blobs,
            interface_ownership_digests,
            accepted_predecessor_trees,
        };
        let _ = context_dependencies.context_digest()?;

        let arch_req = RunArchitectAdviceRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            grant_id: params.grant_id.clone(),
            request_key: params.request_key.clone(),
            expected_ledger_revision: params.expected_ledger_revision,
            difficulty: params.difficulty,
            context_dependencies,
            question_summary: params.question.clone(),
            prior_advice_context,
            accept_into_brief: false,
            current_time_ms: now_ms,
        };

        Ok(EnqueuePlanningCommandRequest::from_architect(arch_req))
    }

    fn build_request_technical_plan_enqueue_request(
        &self,
        params: &RequestTechnicalPlanParams,
    ) -> Result<EnqueuePlanningCommandRequest, NativeError> {
        validate_bounded_identifier(&params.grant_id, "grant_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&params.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&params.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        if let Some(ref plan_id) = params.plan_id {
            validate_bounded_identifier(plan_id, "plan_id", MAX_COWORKER_ID_LEN)?;
        }
        validate_unique_id_list(
            &params.architect_advice_ids,
            "architect_advice_ids",
            MAX_ARCHITECT_ADVICE_RECORDS_PER_BINDING,
        )?;
        if let Some(explicit) = params.max_architect_rounds {
            if explicit > MAX_ORCHESTRATOR_ARCHITECT_ROUNDS {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "max_architect_rounds cannot exceed {MAX_ORCHESTRATOR_ARCHITECT_ROUNDS} (got {explicit})"
                    ),
                ));
            }
        }

        let now_ms = current_wall_clock_ms()?;
        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        let grant = require_active_grant_for_scope(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.grant_id,
            now_ms,
        )?;
        let max_architect_rounds =
            self.resolve_max_architect_rounds_for_request(&workflow_store, grant, params)?;
        let job = require_scoped_job(
            &wf_ledger,
            &self.binding_id,
            &self.thread_uuid,
            &params.job_id,
        )?;

        if job.work_brief.project_base.repository_path != grant.project_base.repository_path {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job project_base.repository_path does not match standing grant canonical repository_path",
            ));
        }

        let (plan_id, expected_plan_revision) =
            match (&params.plan_id, params.expected_plan_revision) {
                (Some(pid), Some(rev)) => (Some(pid.clone()), rev),
                (Some(pid), None) => {
                    let default_rev = job
                        .planning_result
                        .as_ref()
                        .filter(|pr| &pr.plan_id == pid)
                        .map(|pr| pr.plan_revision)
                        .unwrap_or(0);
                    (Some(pid.clone()), default_rev)
                }
                (None, Some(0) | None) => (None, 0),
                (None, Some(_)) => {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "plan_id is required when expected_plan_revision is non-zero",
                    ));
                }
            };

        let mut architect_advice_ids = params.architect_advice_ids.clone();
        if architect_advice_ids.is_empty()
            && !job.work_brief.accepted_architect_decisions.is_empty()
        {
            for decision_id in &job.work_brief.accepted_architect_decisions {
                if wf_ledger
                    .architect_advice
                    .iter()
                    .any(|a| &a.advice_id == decision_id)
                    && !architect_advice_ids.contains(decision_id)
                {
                    architect_advice_ids.push(decision_id.clone());
                }
            }
        }

        let orch_req = OrchestrateWorkBriefPlanRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            grant_id: params.grant_id.clone(),
            job_id: params.job_id.clone(),
            request_key: params.request_key.clone(),
            expected_ledger_revision: params.expected_ledger_revision,
            difficulty: params.difficulty,
            plan_id,
            expected_plan_revision,
            architect_advice_ids,
            max_architect_rounds,
            architect_difficulty: params.architect_difficulty,
            current_time_ms: now_ms,
        };

        Ok(EnqueuePlanningCommandRequest::from_orchestrator(orch_req))
    }

    fn resolve_max_architect_rounds_for_request(
        &self,
        workflow_store: &CoworkerWorkflowStore,
        grant: &PlanningAdviceGrant,
        params: &RequestTechnicalPlanParams,
    ) -> Result<u32, NativeError> {
        if let Some(explicit) = params.max_architect_rounds {
            if explicit > MAX_ORCHESTRATOR_ARCHITECT_ROUNDS {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "max_architect_rounds cannot exceed {MAX_ORCHESTRATOR_ARCHITECT_ROUNDS} (got {explicit})"
                    ),
                ));
            }
            return Ok(explicit);
        }

        let commands_dir = planning_commands_dir(workflow_store.plans_dir());
        if let Ok(command_store) = PlanningCommandStore::open_existing(
            &commands_dir,
            workflow_store.clone(),
            &self.registry_dir,
        ) {
            if let Ok(Some(existing)) = command_store.get_command_by_key(
                &self.binding_id,
                &self.thread_uuid,
                &params.request_key,
            ) {
                if existing.grant_id == params.grant_id {
                    if let PlanningCommandPayload::Orchestrator(ref existing_orch) =
                        existing.payload
                    {
                        return Ok(existing_orch.max_architect_rounds);
                    }
                }
            }
        }

        Ok(compute_default_max_architect_rounds(
            &self.registry_dir,
            grant,
            params.architect_difficulty.or(params.difficulty),
        ))
    }

    async fn enqueue_and_start_with_flow(
        &self,
        flow_exe: &Path,
        job_id: &str,
        enqueue_req: EnqueuePlanningCommandRequest,
    ) -> Result<PlanningCommandSubmissionResponse, NativeError> {
        let gateway = self.clone();
        let (state_dir, registry_dir, plans_dir, authority_dir, sessions_dir, enqueued) =
            tokio::task::spawn_blocking(move || {
                let state_dir = prepare_private_dir(&gateway.state_dir, "state_dir")?;
                let registry_dir = prepare_private_dir(&gateway.registry_dir, "registry_dir")?;
                let plans_dir = prepare_private_dir(&gateway.plans_dir, "plans_dir")?;
                let authority_dir = prepare_private_dir(&gateway.authority_dir, "authority_dir")?;
                let sessions_dir = prepare_private_dir(&gateway.sessions_dir, "sessions_dir")?;

                let enqueued =
                    enqueue_planning_command_summary(&plans_dir, &registry_dir, &enqueue_req)?;
                if enqueued.binding_id != gateway.binding_id
                    || enqueued.thread_uuid != gateway.thread_uuid
                {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "enqueued planning command scope does not match verified binding_id and thread_uuid",
                    ));
                }
                Ok::<_, NativeError>((
                    state_dir,
                    registry_dir,
                    plans_dir,
                    authority_dir,
                    sessions_dir,
                    enqueued,
                ))
            })
            .await
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    "planning command enqueue background task failed to complete",
                )
            })??;

        if let Some((ref reconnect_cfg, backend_pid)) = self.managed_native_notice {
            let notice_key = crate::coworker_workflow::CoworkerJobOwnershipKey::new(
                self.binding_id.clone(),
                self.thread_uuid.clone(),
                enqueued.command_id.clone(),
            )?;
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
                    PathBuf::from(&enqueued.call_project_base.repository_path),
                )
                .with_reconnect(Some(reconnect_cfg.clone()));
            let notice_cfg = crate::coworker_notice::PlanningNoticeConfig::new(
                notice_key,
                enqueued.command_digest.clone(),
                target,
            )?;
            let notice_dir = crate::coworker_notice::planning_notice_dir(&plans_dir);
            crate::coworker_notice::PlanningNoticeStore::new(&notice_dir)?.configure(notice_cfg)?;
        }

        let start_outcome = invoke_sibling_flow_start_command(&FlowStartCommandInvocation {
            flow_exe,
            state_dir: &state_dir,
            registry_dir: &registry_dir,
            plans_dir: &plans_dir,
            authority_dir: &authority_dir,
            sessions_dir: &sessions_dir,
            binding_id: &self.binding_id,
            thread_uuid: &self.thread_uuid,
            command_id: &enqueued.command_id,
        })
        .await?;

        let summary = start_outcome.command;
        if summary.binding_id != self.binding_id
            || summary.thread_uuid != self.thread_uuid
            || summary.command_id != enqueued.command_id
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "threadlane-flow start-command receipt did not match the requested command scope",
            ));
        }

        let (native_notice, native_notice_error) = inspect_planning_notice_best_effort(
            &plans_dir,
            &self.binding_id,
            &self.thread_uuid,
            &summary,
        );

        Ok(PlanningCommandSubmissionResponse {
            binding_id: summary.binding_id.clone(),
            thread_uuid: summary.thread_uuid.clone(),
            grant_id: summary.grant_id.clone(),
            job_id: job_id.to_string(),
            command_id: summary.command_id.clone(),
            role: summary.role,
            status: summary.status,
            submission_status: start_outcome.submission_status,
            submission_requested: start_outcome.submission_requested,
            launched: start_outcome.launched,
            pid_hint: start_outcome.pid_hint,
            selected_choice: summary.selected_choice.clone(),
            context_digest: summary.context_digest.clone(),
            result_ref: summary.result_ref.clone(),
            command: summary,
            native_notice,
            native_notice_error,
        })
    }

    fn collect_job_interface_ownership_digests(
        &self,
        job: &CoworkerJobRecord,
        grant: &PlanningAdviceGrant,
    ) -> BTreeMap<String, String> {
        let mut digests = BTreeMap::new();
        for task in &job.tasks {
            for iface in &task.owned_interfaces {
                if digests.len() >= MAX_WORK_BRIEF_ITEMS {
                    break;
                }
                if validate_bounded_identifier(iface, "interface_id", MAX_COWORKER_ID_LEN).is_ok() {
                    let seed = (
                        "threadlane-native:job-task-interface-ownership:v1",
                        &task.task_id,
                        &task.task_digest,
                        &task.write_paths,
                        &task.owned_interfaces,
                        &task.consumed_interfaces,
                        iface,
                    );
                    if let Ok(bytes) = serde_json::to_vec(&seed) {
                        digests.insert(iface.clone(), sha256_hex_bytes(&bytes));
                    }
                }
            }
        }
        if digests.is_empty() {
            if let Some(ref pr) = job.planning_result {
                if let Ok(artifact) =
                    self.read_verified_artifact_readonly(&pr.report_artifact_id, &grant.grant_id)
                {
                    if let PlanningArtifactOutcomeKind::OrchestratorOutput {
                        structured_output: StructuredOrchestratorOutput::Plan { tasks, .. },
                        ..
                    } = artifact.outcome
                    {
                        for pt in &tasks {
                            for iface in &pt.owned_interfaces {
                                if digests.len() >= MAX_WORK_BRIEF_ITEMS {
                                    break;
                                }
                                if validate_bounded_identifier(
                                    iface,
                                    "interface_id",
                                    MAX_COWORKER_ID_LEN,
                                )
                                .is_ok()
                                {
                                    let seed = (
                                        "threadlane-native:planned-task-interface-ownership:v1",
                                        &pt.task_id,
                                        &pt.allowed_write_paths,
                                        &pt.owned_interfaces,
                                        &pt.consumed_interfaces,
                                        iface,
                                    );
                                    if let Ok(bytes) = serde_json::to_vec(&seed) {
                                        digests.insert(iface.clone(), sha256_hex_bytes(&bytes));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        digests
    }

    fn read_verified_artifact_readonly(
        &self,
        artifact_id: &str,
        grant_id: &str,
    ) -> Result<PersistedPlanningCallArtifact, NativeError> {
        validate_bounded_identifier(artifact_id, "artifact_id", MAX_COWORKER_ID_LEN)?;
        let workflow_store = CoworkerWorkflowStore::new(&self.plans_dir)?;
        let wf_ledger = workflow_store.read_binding_ledger(&self.binding_id)?;
        let grant = wf_ledger
            .planning_grants
            .iter()
            .find(|g| g.grant_id == grant_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "standing grant referenced by completed command was not found",
                )
            })?;
        grant.validate()?;

        let artifacts_dir = planning_artifacts_dir(workflow_store.plans_dir());
        let artifact_path = artifacts_dir.join(format!("{artifact_id}.json"));
        let bytes = read_private_file_bounded(
            &artifact_path,
            MAX_PLANNING_ARTIFACT_BYTES,
            "planning_call_artifact",
        )?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("persisted planning artifact `{artifact_id}` does not exist"),
            )
        })?;
        let artifact: PersistedPlanningCallArtifact =
            serde_json::from_slice(&bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "persisted planning artifact JSON is corrupt or invalid",
                )
            })?;
        artifact.validate_against_grant(artifact_id, grant)?;
        Ok(artifact)
    }
}

fn inspect_planning_notice_best_effort(
    plans_dir: &Path,
    binding_id: &str,
    thread_uuid: &str,
    summary: &PlanningCommandSummary,
) -> (
    Option<crate::coworker_notice::PlanningNoticeMetadata>,
    Option<String>,
) {
    let key = match crate::coworker_workflow::CoworkerJobOwnershipKey::new(
        binding_id,
        thread_uuid,
        &summary.command_id,
    ) {
        Ok(k) => k,
        Err(err) => return (None, Some(err.code().to_string())),
    };
    let notice_dir = crate::coworker_notice::planning_notice_dir(plans_dir);
    match crate::coworker_notice::PlanningNoticeStore::inspect(
        &notice_dir,
        &key,
        &summary.command_digest,
    ) {
        Ok(meta) => (Some(meta), None),
        Err(err) => (None, Some(err.code().to_string())),
    }
}

/// Resolves and validates the canonical same-package sibling `threadlane-flow` binary next to
/// `std::env::current_exe()`.
///
/// Fails closed with [`NativeErrorCode::UnsupportedCapability`] when `threadlane-flow` is missing
/// or not executable, and [`NativeErrorCode::SecurityViolation`] when any symlink, non-regular
/// file, or group/world-writable permission check fails.
pub fn resolve_sibling_flow_executable() -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    let raw_exe = std::env::current_exe().map_err(|_| {
        NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "failed to resolve current executable path to locate sibling threadlane-flow binary",
        )
    })?;
    resolve_sibling_flow_executable_from(&raw_exe, true)
}

/// Resolves and validates the canonical same-package sibling `threadlane-flow` binary relative to
/// `current_exe`.
///
/// When `allow_cargo_deps_parent` is `true` and `current_exe` resides inside a Cargo `<profile>/deps`
/// directory where `<profile>/deps/threadlane-flow` is absent, `<profile>/threadlane-flow` in the
/// same package build output directory is checked. Never searches `PATH`, environment variables,
/// shell aliases, or user-supplied paths.
pub fn resolve_sibling_flow_executable_from(
    current_exe: &Path,
    allow_cargo_deps_parent: bool,
) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if current_exe.as_os_str().is_empty() || !current_exe.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "current executable path must be a non-empty absolute path",
        ));
    }
    let canonical_self = fs::canonicalize(current_exe).map_err(|_| {
        NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "failed to canonicalize current executable path",
        )
    })?;
    let clean_self = validate_absolute_no_symlink_components(&canonical_self, "current_exe")?;
    let parent_dir = clean_self.parent().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            "current executable path has no parent directory",
        )
    })?;

    let direct_candidate = parent_dir.join(DEFAULT_FLOW_EXECUTABLE_NAME);
    match fs::symlink_metadata(&direct_candidate) {
        Ok(_) => return validate_canonical_flow_binary(&direct_candidate),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "failed to inspect sibling threadlane-flow executable metadata",
            ));
        }
    }

    if allow_cargo_deps_parent && parent_dir.file_name() == Some(OsStr::new("deps")) {
        if let Some(profile_dir) = parent_dir.parent() {
            let profile_candidate = profile_dir.join(DEFAULT_FLOW_EXECUTABLE_NAME);
            match fs::symlink_metadata(&profile_candidate) {
                Ok(_) => return validate_canonical_flow_binary(&profile_candidate),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "failed to inspect package sibling threadlane-flow executable metadata",
                    ));
                }
            }
        }
    }

    Err(NativeError::new(
        NativeErrorCode::UnsupportedCapability,
        "canonical same-package sibling `threadlane-flow` executable is unavailable",
    ))
}

fn validate_canonical_flow_binary(candidate: &Path) -> Result<PathBuf, NativeError> {
    let clean = validate_absolute_no_symlink_components(candidate, "threadlane-flow")?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "canonical same-package sibling `threadlane-flow` executable is unavailable",
            )
        } else {
            NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "failed to inspect sibling `threadlane-flow` metadata",
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "sibling `threadlane-flow` must be a regular non-symlink file",
        ));
    }
    let canonical = fs::canonicalize(&clean).map_err(|_| {
        NativeError::new(
            NativeErrorCode::SecurityViolation,
            "failed to canonicalize sibling `threadlane-flow` executable path",
        )
    })?;
    if canonical != clean {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "sibling `threadlane-flow` path must already be canonical without symlinks",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        if (mode & 0o022) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "sibling `threadlane-flow` executable must not be group- or world-writable",
            ));
        }
        if (mode & 0o111) == 0 {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "sibling `threadlane-flow` file is not executable",
            ));
        }
    }
    Ok(clean)
}

struct FlowStartCommandInvocation<'a> {
    flow_exe: &'a Path,
    state_dir: &'a Path,
    registry_dir: &'a Path,
    plans_dir: &'a Path,
    authority_dir: &'a Path,
    sessions_dir: &'a Path,
    binding_id: &'a str,
    thread_uuid: &'a str,
    command_id: &'a str,
}

async fn invoke_sibling_flow_start_command(
    invocation: &FlowStartCommandInvocation<'_>,
) -> Result<PlanningCommandStartOutcome, NativeError> {
    let validated_exe = validate_canonical_flow_binary(invocation.flow_exe)?;

    let mut cmd = Command::new(&validated_exe);
    cmd.arg("--state-dir")
        .arg(invocation.state_dir)
        .arg("--registry-dir")
        .arg(invocation.registry_dir)
        .arg("--plans-dir")
        .arg(invocation.plans_dir)
        .arg("--authority-dir")
        .arg(invocation.authority_dir)
        .arg("--sessions-dir")
        .arg(invocation.sessions_dir)
        .arg("--binding")
        .arg(invocation.binding_id)
        .arg("--thread")
        .arg(invocation.thread_uuid)
        .arg("coworker-planning")
        .arg("start-command")
        .arg("--command-id")
        .arg(invocation.command_id)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!("failed to spawn canonical sibling `threadlane-flow` process: {err}"),
        )
    })?;

    let stdout_pipe = child.stdout.take().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to capture stdout pipe from `threadlane-flow` process",
        )
    })?;
    let stderr_pipe = child.stderr.take().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to capture stderr pipe from `threadlane-flow` process",
        )
    })?;

    let timeout_dur = Duration::from_millis(DEFAULT_FLOW_START_COMMAND_TIMEOUT_MS);
    let (status, stdout_bytes, stderr_bytes) = tokio::time::timeout(timeout_dur, async move {
        let stdout_fut = read_pipe_bounded(stdout_pipe, MAX_FLOW_START_STDOUT_BYTES, "stdout");
        let stderr_fut = read_pipe_bounded(stderr_pipe, MAX_FLOW_START_STDERR_BYTES, "stderr");
        let wait_fut = child.wait();
        let (wait_res, out_res, err_res) = tokio::join!(wait_fut, stdout_fut, stderr_fut);
        let status = wait_res.map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed while awaiting `threadlane-flow` start-command receipt: {err}"),
            )
        })?;
        Ok::<_, NativeError>((status, out_res?, err_res?))
    })
    .await
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::TimedOut,
            "timed out awaiting short launch receipt from sibling `threadlane-flow` start-command",
        )
    })??;

    if !status.success() {
        return Err(parse_flow_cli_error(&stderr_bytes));
    }

    let envelope: FlowCliSuccessEnvelope<PlanningCommandStartOutcome> =
        serde_json::from_slice(&stdout_bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "sibling `threadlane-flow` start-command returned invalid JSON receipt",
            )
        })?;
    if envelope.binding_id != invocation.binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "sibling `threadlane-flow` start-command receipt binding_id mismatch",
        ));
    }
    Ok(envelope.data)
}

async fn read_pipe_bounded<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
    label: &str,
) -> Result<Vec<u8>, NativeError> {
    let limit = max_bytes.saturating_add(1);
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = reader.read(&mut chunk).await.map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to read `threadlane-flow` {label}: {err}"),
            )
        })?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > max_bytes {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("`threadlane-flow` {label} exceeded maximum bound of {max_bytes} bytes"),
            ));
        }
        if buf.len() >= limit {
            break;
        }
    }
    Ok(buf)
}

fn parse_flow_cli_error(stderr_bytes: &[u8]) -> NativeError {
    if let Ok(parsed) = serde_json::from_slice::<FlowCliErrorLine>(stderr_bytes) {
        let code = match parsed.code.as_str() {
            "security_violation" => NativeErrorCode::SecurityViolation,
            "unsupported_platform" => NativeErrorCode::UnsupportedPlatform,
            "unsupported_capability" => NativeErrorCode::UnsupportedCapability,
            "needs_verification" => NativeErrorCode::NeedsVerification,
            "not_configured" => NativeErrorCode::NotConfigured,
            "workflow_not_configured" => NativeErrorCode::WorkflowNotConfigured,
            "host_not_configured" => NativeErrorCode::HostNotConfigured,
            "timed_out" => NativeErrorCode::TimedOut,
            "ambiguous_transport_failure" | "outcome_unknown" => {
                NativeErrorCode::AmbiguousTransportFailure
            }
            "ambiguous_wakeup_pending_reconciliation" => {
                NativeErrorCode::AmbiguousWakeupPendingReconciliation
            }
            "transport_unavailable" => NativeErrorCode::TransportUnavailable,
            "protocol_error" => NativeErrorCode::ProtocolError,
            "rpc_error" => NativeErrorCode::RpcError,
            "mcp_tool_error" => NativeErrorCode::McpToolError,
            "thread_not_found" => NativeErrorCode::ThreadNotFound,
            "unauthorized_operator" => NativeErrorCode::UnauthorizedOperator,
            "rebind_rejected" | "scope_mismatch" | "binding_mismatch" | "thread_uuid_mismatch" => {
                NativeErrorCode::RebindRejected
            }
            "io_error" => NativeErrorCode::IoError,
            "persistence_uncertain" => NativeErrorCode::PersistenceUncertain,
            "output_overflow" => NativeErrorCode::OutputOverflow,
            "invalid_config" => NativeErrorCode::InvalidConfig,
            _ => NativeErrorCode::ValidationFailed,
        };
        return NativeError::new(code, parsed.message);
    }
    NativeError::new(
        NativeErrorCode::ProtocolError,
        "sibling `threadlane-flow` start-command exited with a non-zero status",
    )
}

fn validate_status_limit(limit: Option<usize>) -> Result<usize, NativeError> {
    let resolved = limit.unwrap_or(DEFAULT_COWORKER_STATUS_LIMIT);
    if resolved == 0 || resolved > MAX_COWORKER_STATUS_LIMIT {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("limit must be between 1 and {MAX_COWORKER_STATUS_LIMIT} (got {resolved})"),
        ));
    }
    Ok(resolved)
}

/// Truncates `input` on an exact UTF-8 codepoint boundary so its dual-envelope JSON representation
/// across `CallToolResult.structured_content` and escaped `CallToolResult.content[0].text` uses at
/// most `max_dual_wire_bytes` bytes.
///
/// Returns `(bounded_text, was_truncated)`.
#[must_use]
pub fn truncate_status_text_unicode_safe(
    input: &str,
    max_dual_wire_bytes: usize,
) -> (String, bool) {
    let mut used_wire_bytes = 0usize;
    for (byte_idx, ch) in input.char_indices() {
        let ch_cost = dual_envelope_wire_bytes_for_char(ch);
        if used_wire_bytes.saturating_add(ch_cost) > max_dual_wire_bytes {
            return (input[..byte_idx].to_string(), true);
        }
        used_wire_bytes += ch_cost;
    }
    (input.to_string(), false)
}

const fn dual_envelope_wire_bytes_for_char(ch: char) -> usize {
    match ch {
        // `\"` in structuredContent (2 bytes) + `\\\"` in content[0].text (4 bytes) = 6 wire bytes.
        '"' | '\\' => 6,
        // `\n` in structuredContent (2 bytes) + `\\n` in content[0].text (3 bytes) = 5 wire bytes.
        '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => 5,
        // `\u00XX` in structuredContent (6 bytes) + `\\u00XX` in content[0].text (7 bytes) = 13 wire bytes.
        c if (c as u32) < 0x20 => 13,
        // Unescaped UTF-8 codepoint serialized once in structuredContent and once in content[0].text.
        c => c.len_utf8() * 2,
    }
}

fn select_tail_preserving_attention<T: Clone, F: Fn(&T) -> bool>(
    items: &[T],
    count: usize,
    needs_attention: F,
) -> Vec<T> {
    if count == 0 || items.is_empty() {
        return Vec::new();
    }
    if items.len() <= count {
        return items.to_vec();
    }
    let attention_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| needs_attention(item).then_some(idx))
        .collect();
    if attention_indices.len() >= count {
        return attention_indices[attention_indices.len() - count..]
            .iter()
            .map(|&idx| items[idx].clone())
            .collect();
    }
    let mut selected: BTreeSet<usize> = attention_indices.into_iter().collect();
    for idx in (0..items.len()).rev() {
        if selected.len() == count {
            break;
        }
        selected.insert(idx);
    }
    selected.into_iter().map(|idx| items[idx].clone()).collect()
}

fn build_section_bounds(
    total_count: usize,
    limit: usize,
    returned_count: usize,
    truncated_text_row_count: usize,
    projected_nested_detail_row_count: usize,
) -> CoworkerStatusSectionBounds {
    let after_limit_count = total_count.min(limit);
    let omitted_by_limit_count = total_count.saturating_sub(after_limit_count);
    let omitted_by_byte_budget_count = after_limit_count.saturating_sub(returned_count);
    CoworkerStatusSectionBounds {
        total_count,
        returned_count,
        has_more: returned_count < total_count,
        omitted_by_limit_count,
        omitted_by_byte_budget_count,
        truncated_text_row_count,
        projected_nested_detail_row_count,
    }
}

fn map_status_wire_budget_error(err: WorkflowError) -> NativeError {
    match err {
        WorkflowError::ResponseTooLarge(msg) => {
            NativeError::new(NativeErrorCode::OutputOverflow, msg)
        }
        other => NativeError::new(NativeErrorCode::ProtocolError, other.safe_message()),
    }
}

fn ensure_ledger_matches_thread(
    wf_ledger: &CoworkerBindingLedger,
    thread_uuid: &str,
) -> Result<(), NativeError> {
    if let Some(ref pinned) = wf_ledger.thread_uuid {
        if pinned != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "thread_uuid does not match the thread pinned to this binding's Coworker workflow ledger",
            ));
        }
    }
    Ok(())
}

fn is_grant_summary_currently_active(grant: &PlanningGrantSummary, now_ms: u64) -> bool {
    grant.status == PlanningGrantStatus::Active
        && grant.revoked_at_ms.is_none()
        && grant.revoked_reason.is_none()
        && now_ms >= grant.created_at_ms
        && now_ms < grant.expires_at_ms
        && grant.usage.reserved_total_calls < grant.budget.max_total_calls
        && grant.usage.unknown_effect_calls == 0
}

fn require_active_grant_for_scope<'a>(
    wf_ledger: &'a CoworkerBindingLedger,
    binding_id: &str,
    thread_uuid: &str,
    grant_id: &str,
    now_ms: u64,
) -> Result<&'a PlanningAdviceGrant, NativeError> {
    if wf_ledger.binding_id != binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Coworker workflow ledger binding_id does not match verified binding_id",
        ));
    }
    ensure_ledger_matches_thread(wf_ledger, thread_uuid)?;
    if wf_ledger.thread_uuid.as_deref() != Some(thread_uuid) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "binding `{binding_id}` has no active planning grant for thread `{thread_uuid}`"
            ),
        ));
    }
    if wf_ledger
        .planning_grants
        .iter()
        .any(|g| g.reservations.iter().any(|r| r.status.is_ambiguous()))
    {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            "a planning call reservation on this binding has an uncertain outcome pending reconciliation",
        ));
    }
    let grant = wf_ledger
        .planning_grants
        .iter()
        .find(|g| g.grant_id == grant_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "standing planning grant `{grant_id}` was not found in binding `{binding_id}`"
                ),
            )
        })?;
    grant.validate()?;
    if grant.binding_id != binding_id || grant.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "PlanningAdviceGrant scope does not match verified binding_id and thread_uuid",
        ));
    }
    if grant.status == PlanningGrantStatus::Revoked
        || grant.revoked_at_ms.is_some()
        || grant.revoked_reason.is_some()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "standing planning grant is revoked",
        ));
    }
    if grant.status == PlanningGrantStatus::Expired || now_ms >= grant.expires_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "standing planning grant has expired",
        ));
    }
    if now_ms < grant.created_at_ms {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "local wall clock precedes standing planning grant created_at_ms",
        ));
    }
    if grant.status != PlanningGrantStatus::Active
        || grant.usage.reserved_total_calls >= grant.budget.max_total_calls
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "standing planning grant call budget is exhausted",
        ));
    }
    Ok(grant)
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

struct ResolvedBriefKindAndImpact {
    kind: WorkBriefKind,
    amended_target_task_ids: Vec<String>,
    amended_write_paths: Vec<String>,
    amended_interfaces: Vec<String>,
}

fn resolve_work_brief_kind_and_amendment(
    params: &PrepareWorkParams,
) -> Result<ResolvedBriefKindAndImpact, NativeError> {
    let has_flat_amendment = params.target_job_id.is_some()
        || params.target_job_revision.is_some()
        || params.amendment_reason.is_some()
        || !params.amended_target_task_ids.is_empty()
        || !params.amended_write_paths.is_empty()
        || !params.amended_interfaces.is_empty();

    if let Some(ref nested) = params.amendment {
        if has_flat_amendment {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "specify either nested `amendment` or flat amendment fields in prepare_work, not both",
            ));
        }
        return Ok(ResolvedBriefKindAndImpact {
            kind: WorkBriefKind::Amendment {
                target_job_id: nested.target_job_id.clone(),
                target_job_revision: nested.target_job_revision,
                amendment_reason: nested.amendment_reason.clone(),
            },
            amended_target_task_ids: nested.amended_target_task_ids.clone(),
            amended_write_paths: nested.amended_write_paths.clone(),
            amended_interfaces: nested.amended_interfaces.clone(),
        });
    }

    if has_flat_amendment {
        let target_job_id = params.target_job_id.clone().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "amendment requires target_job_id, target_job_revision, and amendment_reason",
            )
        })?;
        let target_job_revision = params.target_job_revision.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "amendment requires target_job_id, target_job_revision, and amendment_reason",
            )
        })?;
        let amendment_reason = params.amendment_reason.clone().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "amendment requires target_job_id, target_job_revision, and amendment_reason",
            )
        })?;
        return Ok(ResolvedBriefKindAndImpact {
            kind: WorkBriefKind::Amendment {
                target_job_id,
                target_job_revision,
                amendment_reason,
            },
            amended_target_task_ids: params.amended_target_task_ids.clone(),
            amended_write_paths: params.amended_write_paths.clone(),
            amended_interfaces: params.amended_interfaces.clone(),
        });
    }

    Ok(ResolvedBriefKindAndImpact {
        kind: WorkBriefKind::NewJob,
        amended_target_task_ids: Vec::new(),
        amended_write_paths: Vec::new(),
        amended_interfaces: Vec::new(),
    })
}

fn validate_accepted_architect_decisions(
    wf_ledger: &CoworkerBindingLedger,
    grant: &PlanningAdviceGrant,
    binding_id: &str,
    thread_uuid: &str,
    work_brief: &WorkBrief,
    is_existing_request_retry: bool,
) -> Result<(), NativeError> {
    if work_brief.accepted_architect_decisions.is_empty() {
        return Ok(());
    }
    validate_unique_id_list(
        &work_brief.accepted_architect_decisions,
        "accepted_architect_decisions",
        MAX_WORK_BRIEF_ITEMS,
    )?;

    let current_brief_digest = work_brief.digest()?;
    let mut brief_without_decisions = work_brief.clone();
    brief_without_decisions.accepted_architect_decisions.clear();
    let brief_without_decisions_digest = brief_without_decisions.digest()?;

    let target_job = match work_brief.kind {
        WorkBriefKind::NewJob => None,
        WorkBriefKind::Amendment {
            ref target_job_id,
            target_job_revision,
            ..
        } => {
            let found = wf_ledger
                .jobs
                .iter()
                .find(|j| &j.job_id == target_job_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "amendment target_job_id was not found in this binding ledger",
                    )
                })?;
            if !is_existing_request_retry && found.job_revision != target_job_revision {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "amendment target_job_revision does not match current target job_revision",
                ));
            }
            Some(found)
        }
    };

    for id in &work_brief.accepted_architect_decisions {
        let record = wf_ledger
            .architect_advice
            .iter()
            .find(|a| &a.advice_id == id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "accepted_architect_decisions entry `{id}` was not found in binding `{binding_id}`"
                    ),
                )
            })?;
        record.validate()?;
        if record.binding_id != binding_id
            || record.thread_uuid != thread_uuid
            || record.status == ArchitectDecisionStatus::Rejected
            || record.context_dependencies.project_base.repository_path
                != grant.project_base.repository_path
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_architect_decisions entry `{id}` is rejected or outside the current binding/repository scope"
                ),
            ));
        }
        if record.context_dependencies.project_base != work_brief.project_base {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_architect_decisions entry `{id}` project_base does not match the current WorkBrief project_base"
                ),
            ));
        }
        if record.context_dependencies.policy_digest != grant.policy_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_architect_decisions entry `{id}` policy_digest does not match the standing grant policy_digest"
                ),
            ));
        }
        let dep_paths: Vec<String> = record
            .context_dependencies
            .dependency_file_blobs
            .keys()
            .cloned()
            .collect();
        let current_blobs = CoworkerPlanningService::compute_dependency_file_blobs(
            &work_brief.project_base,
            &dep_paths,
        )?;
        if current_blobs != record.context_dependencies.dependency_file_blobs {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_architect_decisions entry `{id}` dependency_file_blobs no longer match current WorkBrief project_base"
                ),
            ));
        }

        let brief_context_matches = match target_job {
            Some(target) => {
                record.context_dependencies.work_brief_digest == target.work_brief_digest
                    || target
                        .work_brief
                        .accepted_architect_decisions
                        .iter()
                        .any(|accepted| accepted == id)
                    || record.context_dependencies.work_brief_digest == current_brief_digest
                    || record.context_dependencies.work_brief_digest
                        == brief_without_decisions_digest
            }
            None => {
                record.context_dependencies.work_brief_digest == current_brief_digest
                    || record.context_dependencies.work_brief_digest
                        == brief_without_decisions_digest
            }
        };
        if !brief_context_matches {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_architect_decisions entry `{id}` work_brief_digest does not match the current or amended WorkBrief context"
                ),
            ));
        }
    }
    Ok(())
}

fn resolve_prior_advice_records(
    wf_ledger: &CoworkerBindingLedger,
    grant: &PlanningAdviceGrant,
    binding_id: &str,
    thread_uuid: &str,
    prior_advice_ids: &[String],
) -> Result<Vec<PriorArchitectAdviceContext>, NativeError> {
    if prior_advice_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_unique_id_list(prior_advice_ids, "prior_advice_ids", MAX_WORK_BRIEF_ITEMS)?;
    let mut seen = BTreeSet::new();
    let mut resolved = Vec::with_capacity(prior_advice_ids.len());
    for advice_id in prior_advice_ids {
        if !seen.insert(advice_id.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("duplicate prior_advice_id `{advice_id}`"),
            ));
        }
        let stored = wf_ledger
            .architect_advice
            .iter()
            .find(|a| &a.advice_id == advice_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "prior_advice_id `{advice_id}` was not found in binding `{binding_id}`"
                    ),
                )
            })?;
        stored.validate()?;
        if stored.binding_id != binding_id
            || stored.thread_uuid != thread_uuid
            || stored.status == ArchitectDecisionStatus::Rejected
            || stored.context_dependencies.project_base.repository_path
                != grant.project_base.repository_path
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "prior_advice_id `{advice_id}` is rejected or does not match current binding and repository scope"
                ),
            ));
        }
        resolved.push(PriorArchitectAdviceContext {
            advice_id: stored.advice_id.clone(),
            context_digest: stored.context_digest.clone(),
            decision_digest: stored.decision_digest.clone(),
            decision_summary: stored.decision_summary.clone(),
        });
    }
    Ok(resolved)
}

fn collect_job_accepted_predecessor_trees(job: &CoworkerJobRecord) -> BTreeMap<String, String> {
    let mut trees = BTreeMap::new();
    for task in &job.tasks {
        for receipt in &task.accepted_predecessor_receipts {
            if trees.len() < MAX_TASKS_PER_JOB {
                trees.insert(receipt.task_id.clone(), receipt.tree_sha.clone());
            }
        }
        if matches!(
            task.stage,
            CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
        ) {
            if let Some(ref cand_id) = task.accepted_candidate_id {
                if let Some(cand) = task.candidates.iter().find(|c| &c.candidate_id == cand_id) {
                    if trees.len() < MAX_TASKS_PER_JOB {
                        trees.insert(task.task_id.clone(), cand.tree_sha.clone());
                    }
                }
            }
        }
    }
    for cmb in &job.combined_candidates {
        for receipt in &cmb.accepted_predecessor_receipts {
            if trees.len() < MAX_TASKS_PER_JOB {
                trees
                    .entry(receipt.task_id.clone())
                    .or_insert_with(|| receipt.tree_sha.clone());
            }
        }
    }
    trees
}

fn compute_default_max_architect_rounds(
    registry_dir: &Path,
    grant: &PlanningAdviceGrant,
    architect_difficulty: Option<Difficulty>,
) -> u32 {
    let remaining_total = grant
        .budget
        .max_total_calls
        .saturating_sub(grant.usage.reserved_total_calls);
    let remaining_orchestrator = grant
        .budget
        .max_orchestrator_calls
        .saturating_sub(grant.usage.reserved_orchestrator_calls);
    let remaining_architect = grant
        .budget
        .max_architect_calls
        .saturating_sub(grant.usage.reserved_architect_calls);

    if remaining_total < 3 || remaining_orchestrator < 2 || remaining_architect < 1 {
        return 0;
    }
    if !can_resolve_headless_architect_for_grant(registry_dir, grant, architect_difficulty) {
        return 0;
    }

    let max_by_architect = remaining_architect;
    let max_by_orchestrator = remaining_orchestrator.saturating_sub(1);
    let max_by_total = remaining_total.saturating_sub(1) / 2;

    max_by_architect
        .min(max_by_orchestrator)
        .min(max_by_total)
        .min(MAX_ORCHESTRATOR_ARCHITECT_ROUNDS)
}

fn can_resolve_headless_architect_for_grant(
    registry_dir: &Path,
    grant: &PlanningAdviceGrant,
    difficulty: Option<Difficulty>,
) -> bool {
    let Ok(live_reg_snapshot) = read_registry_snapshot_readonly(registry_dir) else {
        return false;
    };
    let Ok(reg_snapshot) = verify_grant_registry_dependencies(&live_reg_snapshot, grant) else {
        return false;
    };
    let Ok(report) = preflight(
        &reg_snapshot,
        &PreflightRequest {
            profile_id: Some(grant.profile_id.clone()),
            roles: vec![RolePreflightRequest::new(
                WorkflowRole::Architect,
                difficulty,
            )],
        },
        &grant.policy,
    ) else {
        return false;
    };
    let Some(outcome) = report.roles.first() else {
        return false;
    };
    if !report.ready || !outcome.ready {
        return false;
    }
    let Some(ref raw_selected) = outcome.selected else {
        return false;
    };
    let Ok(selected) = crate::profile_flow::normalize_read_only_advisory_choice(
        WorkflowRole::Architect,
        raw_selected,
    ) else {
        return false;
    };
    if selected.route != RoleExecutionRoute::HeadlessWorker
        || !matches!(
            selected.adapter,
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 | ToolAdapter::CodexCli
        )
    {
        return false;
    }
    if validate_non_native_worker_choice(WorkflowRole::Architect, &selected).is_err() {
        return false;
    }
    let Some(tool_def) = reg_snapshot.tools.iter().find(|t| t.id == selected.tool_id) else {
        return false;
    };
    if !tool_def.enabled {
        return false;
    }
    if tool_def.adapter == ToolAdapter::CodexCli {
        crate::profile_flow::resolve_advisory_tool_executable(tool_def).is_ok()
    } else {
        tool_def.executable_path.is_some()
    }
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}
