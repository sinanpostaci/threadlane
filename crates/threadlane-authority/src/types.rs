use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use threadlane::WorkersConfig;
use threadlane_registry::{Difficulty, ToolAdapter, WorkflowRole};
use threadlane_workers::{ExecutionBudgets, WorkerFilePermissions, WorkerProvider};

/// Current on-disk schema version for `approvals.json`.
pub const AUTHORITY_SCHEMA_VERSION: u32 = 1;

/// Current workflow authority policy version included in every approval subject digest.
pub const AUTHORITY_POLICY_VERSION: u32 = 1;

/// Filename for the persisted authority approval ledger inside the authority directory.
pub const APPROVALS_FILE_NAME: &str = "approvals.json";

/// Filename for the stable cross-process lock file inside the authority directory.
pub const APPROVALS_LOCK_FILE_NAME: &str = "authority.lock";

/// Maximum byte size permitted when reading or writing `approvals.json` (`1 MiB`).
pub const MAX_AUTHORITY_STORE_BYTES: usize = 1_048_576;

/// Maximum number of approval records retained in a single [`crate::AuthorityStore`].
pub const MAX_APPROVALS_IN_STORE: usize = 256;

/// Maximum number of tasks that may be selected in a single [`OperatorApprovalRequest`].
pub const MAX_APPROVED_TASKS: usize = 64;

/// Maximum number of executable headless worker profiles (`antigravity` or `opencode_v2`)
/// permitted in a single approval (`matches threadlane::MAX_WORKER_PROFILES`).
pub const MAX_EXECUTABLE_PROFILES_PER_APPROVAL: usize = 32;

/// Maximum number of operator-approved write path entries in a single [`OperatorApprovalRequest`].
pub const MAX_APPROVED_WRITE_PATHS: usize = 64;

/// Minimum allowed `max_parallel` concurrency limit in an [`OperatorApprovalRequest`].
pub const MIN_APPROVAL_MAX_PARALLEL: usize = 1;

/// Maximum allowed `max_parallel` concurrency limit in an [`OperatorApprovalRequest`].
pub const MAX_APPROVAL_MAX_PARALLEL: usize = 8;

/// Trusted operator-only intent used to prepare and preview a workflow approval before
/// digest-confirmed persistence.
///
/// Carries the same explicit scope and pin fields as [`OperatorApprovalRequest`] except
/// `expected_plan_digest`, which [`crate::AuthorityStore::prepare_for_operator`] computes from
/// the active published plan revision and current registry snapshot without auto-upgrading
/// revisions. Never infers authority from conversation text or extra boolean flags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorApprovalIntent {
    /// Chat binding identifier owning the target plan revision.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Logical plan identifier in [`threadlane_plans::PlanStore`].
    pub plan_id: String,
    /// Published plan revision number (`published`, `received`, or `applied`).
    pub plan_revision: u64,
    /// Exact registry revision expected by the operator (`must equal plan.profile_revision and current registry revision`).
    pub expected_registry_revision: u64,
    /// Existing canonical Git repository path.
    pub repository_path: PathBuf,
    /// Exact 40-character hexadecimal Git commit SHA existing in `repository_path`.
    pub base_commit: String,
    /// Non-empty list of selected task identifiers from the plan revision (`1..=64`).
    pub approved_task_ids: Vec<String>,
    /// Non-empty list of operator-approved repository-relative write scopes (file-exact or trailing-`/` directory prefix).
    pub approved_write_paths: Vec<String>,
    /// Maximum concurrent headless worker executions (`1..=8`).
    pub max_parallel: usize,
    /// Explicit execution timeout and stream capture budgets.
    #[serde(default)]
    pub budgets: ExecutionBudgets,
}

/// Trusted operator-only request to freeze an immutable workflow approval artifact.
///
/// Only the trusted operator CLI or Tauri control panel may submit this request; MCP tools must
/// never expose or call [`crate::AuthorityStore::approve`]. The operator UI is responsible for
/// displaying and confirming every field with the human operator; calling `approve` records the
/// validated artifact and does not by itself prove human interaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorApprovalRequest {
    /// Chat binding identifier owning the target plan revision.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Logical plan identifier in [`threadlane_plans::PlanStore`].
    pub plan_id: String,
    /// Published plan revision number (`published`, `received`, or `applied`).
    pub plan_revision: u64,
    /// Exact registry revision expected by the operator (`must equal plan.profile_revision and current registry revision`).
    pub expected_registry_revision: u64,
    /// Exact 64-character lowercase hex SHA-256 digest of the compiled plan snapshot.
    pub expected_plan_digest: String,
    /// Existing canonical Git repository path.
    pub repository_path: PathBuf,
    /// Exact 40-character hexadecimal Git commit SHA existing in `repository_path`.
    pub base_commit: String,
    /// Non-empty list of selected task identifiers from the plan revision (`1..=64`).
    pub approved_task_ids: Vec<String>,
    /// Non-empty list of operator-approved repository-relative write scopes (file-exact or trailing-`/` directory prefix).
    pub approved_write_paths: Vec<String>,
    /// Maximum concurrent headless worker executions (`1..=8`).
    pub max_parallel: usize,
    /// Explicit execution timeout and stream capture budgets.
    #[serde(default)]
    pub budgets: ExecutionBudgets,
}

/// Immutable frozen task snapshot persisted inside an [`ApprovalRecord`].
///
/// Captures a complete copy of the resolved model, effort, canonical CLI executable path (for
/// headless worker tasks), exact task write scope, dependencies, and instruction prompt at
/// approval time so later edits to the global registry never alter an existing approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedTaskRecord {
    /// Unique task identifier within the plan revision.
    pub task_id: String,
    /// Human-readable task title.
    pub title: String,
    /// Frozen task instruction prompt.
    pub instruction: String,
    /// 64-character lowercase hex SHA-256 digest of `instruction`.
    pub instruction_digest: String,
    /// Workflow role assigned to the task.
    pub role: WorkflowRole,
    /// Task difficulty assigned to the task.
    pub difficulty: Difficulty,
    /// Identifier of the registry tool resolved at approval time.
    pub tool_id: String,
    /// Tool adapter kind (`antigravity`, `opencode_v2`, or `codex_cli`).
    pub adapter: ToolAdapter,
    /// Headless worker provider (`Some(Antigravity | OpenCodeV2)` for worker tasks, `None` for `codex_cli`).
    pub provider: Option<WorkerProvider>,
    /// Canonical non-symlink regular executable path resolved once at approval time for worker tasks (`None` for `codex_cli`).
    pub canonical_executable: Option<PathBuf>,
    /// Registry model definition identifier.
    pub model_id: String,
    /// Raw model argument from the registry model definition at approval time.
    pub model_arg: String,
    /// Validated base model identifier (`gemini-4-argon-high` or `provider/model`).
    pub base_model: String,
    /// Frozen reasoning effort or variant (`Some(...)` or `None`).
    pub effort: Option<String>,
    /// Exact CLI `--model` argument string for headless worker tasks (`None` for `codex_cli`).
    pub cli_model_arg: Option<String>,
    /// Frozen `--standalone` flag for headless worker tasks (`Some(true)` for `opencode_v2`, `Some(false)` for `antigravity`, `None` for `codex_cli`).
    pub standalone: Option<bool>,
    /// Frozen file permission policy for headless worker tasks (`Some(TaskFiles)` for `antigravity` and `opencode_v2`, `None` for `codex_cli`).
    pub file_permissions: Option<WorkerFilePermissions>,
    /// Upstream dependency task identifiers retained as metadata.
    pub depends_on: Vec<String>,
    /// Exact repository-relative write paths permitted for this task.
    pub allowed_write_paths: Vec<String>,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Deterministic worker profile identifier (`<= 64` chars) for headless worker tasks (`None` for `codex_cli`).
    pub profile_id: Option<String>,
    /// Deterministic idempotency command key for headless worker tasks (`None` for `codex_cli`).
    pub command_key: Option<String>,
}

/// Private operator-only preview produced by [`crate::AuthorityStore::prepare_for_operator`]
/// and [`crate::AuthorityStore::preview_for_operator`].
///
/// Includes the complete validated [`OperatorApprovalRequest`] (with compiled
/// `expected_plan_digest` and execution budgets), the 64-character lowercase hex
/// `subject_digest`, and the full frozen [`ApprovedTaskRecord`] snapshots (resolved model,
/// effort, canonical CLI executable paths, write scopes, dependencies, and instructions).
/// This preview is **not** a persisted approval and never modifies `approvals.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorApprovalPreview {
    /// Complete validated [`OperatorApprovalRequest`] to submit to [`crate::AuthorityStore::approve_confirmed`].
    pub request: OperatorApprovalRequest,
    /// 64-character lowercase hex SHA-256 digest over the complete frozen authority subject.
    pub subject_digest: String,
    /// Frozen copies of the selected tasks in deterministic plan order.
    pub tasks: Vec<ApprovedTaskRecord>,
}

/// Complete private operator-only approval record persisted in [`crate::AuthorityStore`].
///
/// Returned only by [`crate::AuthorityStore::get_for_operator`]. Contains the full frozen copy
/// of all approved tasks, resolved canonical executable paths, model/effort pins, scopes, and
/// instructions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRecord {
    /// Deterministic approval identifier derived from `subject_digest`.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest over the complete frozen authority subject.
    pub subject_digest: String,
    /// Workflow authority policy version (`AUTHORITY_POLICY_VERSION`).
    pub policy_version: u32,
    /// Chat binding identifier owning the approved plan revision.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Logical plan identifier.
    pub plan_id: String,
    /// Approved published plan revision number.
    pub plan_revision: u64,
    /// Workflow profile identifier frozen at approval time.
    pub profile_id: String,
    /// Registry snapshot revision frozen at approval time.
    pub registry_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan snapshot.
    pub plan_digest: String,
    /// Canonical Git repository path.
    pub repository_path: PathBuf,
    /// Normalized 40-character lowercase hexadecimal Git commit SHA.
    pub base_commit: String,
    /// Selected task identifiers in deterministic plan order.
    pub approved_task_ids: Vec<String>,
    /// Validated operator-approved repository-relative write scopes.
    pub approved_write_paths: Vec<String>,
    /// Maximum concurrent headless worker executions (`1..=8`).
    pub max_parallel: usize,
    /// Frozen execution and stream capture budgets.
    pub budgets: ExecutionBudgets,
    /// Frozen copies of the selected tasks in deterministic plan order.
    pub tasks: Vec<ApprovedTaskRecord>,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: i64,
}

impl ApprovalRecord {
    /// Projects this full private record into a bounded [`ApprovalSummary`] that omits
    /// executable paths, credentials, and task instructions.
    pub fn to_summary(&self) -> ApprovalSummary {
        let worker_profile_count = self.tasks.iter().filter(|t| t.provider.is_some()).count();
        let native_route_count = self.tasks.iter().filter(|t| t.provider.is_none()).count();
        ApprovalSummary {
            approval_id: self.approval_id.clone(),
            subject_digest: self.subject_digest.clone(),
            policy_version: self.policy_version,
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            plan_id: self.plan_id.clone(),
            plan_revision: self.plan_revision,
            profile_id: self.profile_id.clone(),
            registry_revision: self.registry_revision,
            plan_digest: self.plan_digest.clone(),
            repository_path: self.repository_path.clone(),
            base_commit: self.base_commit.clone(),
            approved_task_ids: self.approved_task_ids.clone(),
            approved_write_paths: self.approved_write_paths.clone(),
            max_parallel: self.max_parallel,
            worker_profile_count,
            native_route_count,
            created_at_ms: self.created_at_ms,
        }
    }
}

/// Bounded summary of an [`ApprovalRecord`] returned by [`crate::AuthorityStore::approve`] and
/// [`crate::AuthorityStore::list_for_binding`].
///
/// Intentionally omits resolved executable paths, credentials, and raw task instruction prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSummary {
    /// Deterministic approval identifier derived from `subject_digest`.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest over the complete frozen authority subject.
    pub subject_digest: String,
    /// Workflow authority policy version (`AUTHORITY_POLICY_VERSION`).
    pub policy_version: u32,
    /// Chat binding identifier owning the approved plan revision.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Logical plan identifier.
    pub plan_id: String,
    /// Approved published plan revision number.
    pub plan_revision: u64,
    /// Workflow profile identifier frozen at approval time.
    pub profile_id: String,
    /// Registry snapshot revision frozen at approval time.
    pub registry_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the compiled plan snapshot.
    pub plan_digest: String,
    /// Canonical Git repository path.
    pub repository_path: PathBuf,
    /// Normalized 40-character lowercase hexadecimal Git commit SHA.
    pub base_commit: String,
    /// Selected task identifiers in deterministic plan order.
    pub approved_task_ids: Vec<String>,
    /// Validated operator-approved repository-relative write scopes.
    pub approved_write_paths: Vec<String>,
    /// Maximum concurrent headless worker executions (`1..=8`).
    pub max_parallel: usize,
    /// Number of headless worker profiles (`antigravity` or `opencode_v2`) in this approval.
    pub worker_profile_count: usize,
    /// Number of manual native routes (`codex_cli`) in this approval.
    pub native_route_count: usize,
    /// Creation timestamp in milliseconds since the Unix epoch.
    pub created_at_ms: i64,
}

/// Frozen dispatch entry for an approved headless worker task (`antigravity` or `opencode_v2`).
///
/// Future MCP dispatch will accept only `task_id` (and an idempotency key) and look up this
/// frozen [`ApprovedTaskDispatch`] without permitting any caller override of `instruction`,
/// model, effort, executable, or write scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedTaskDispatch {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Deterministic worker profile identifier (`<= 64` characters) in `MaterializedPlan::workers`.
    pub profile_id: String,
    /// Deterministic default command key for idempotent dispatch.
    pub command_key: String,
    /// Frozen task instruction prompt from the approved plan revision.
    pub instruction: String,
}

/// Manual native follower/chat routing metadata for an approved `codex_cli` task.
///
/// Native Codex remains the operator's conversational interface; `codex_cli` tasks are never
/// synthesized into fake headless worker profiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoute {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Human-readable task title.
    pub title: String,
    /// Frozen task instruction prompt.
    pub instruction: String,
    /// Workflow role assigned to the task.
    pub role: WorkflowRole,
    /// Task difficulty assigned to the task.
    pub difficulty: Difficulty,
    /// Registry tool identifier.
    pub tool_id: String,
    /// Tool adapter (`ToolAdapter::CodexCli`).
    pub adapter: ToolAdapter,
    /// Registry model definition identifier.
    pub model_id: String,
    /// Model argument string from the registry model definition.
    pub model_arg: String,
    /// Optional reasoning effort from the workflow profile assignment.
    pub effort: Option<String>,
    /// Upstream dependency task identifiers retained as metadata.
    pub depends_on: Vec<String>,
    /// Task repository-relative write paths retained as metadata.
    pub allowed_write_paths: Vec<String>,
    /// Approved canonical native Codex thread UUID.
    pub thread_uuid: String,
    /// Runtime session binding identifier (`session_binding.binding_id`).
    pub session_binding_id: String,
}

/// Materialized runtime execution plan synthesized from a persisted [`ApprovalRecord`] for a
/// specific runtime session [`threadlane_core::Binding`] and private `work_area_root`.
///
/// Synthesizes `workers: Some(WorkersConfig)` only when at least one selected task targets
/// `antigravity` or `opencode_v2`; native-only approvals produce `workers: None`.
/// Materialization is completely side-effect-free and does not start the supervisor, create
/// worktrees, or execute a DAG scheduler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedPlan {
    /// Deterministic approval identifier.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Validated startup worker configuration for selected `antigravity`/`opencode_v2` tasks, or `None` if native-only.
    pub workers: Option<WorkersConfig>,
    /// Frozen task dispatch entries corresponding to the profiles in `workers`.
    pub task_dispatch: Vec<ApprovedTaskDispatch>,
    /// Manual native follower route metadata for selected `codex_cli` tasks.
    pub native_routes: Vec<NativeRoute>,
}
