use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadlane_plans::{PlanError, PlanStatus};
use threadlane_registry::ToolAdapter;

/// Structured domain, validation, persistence, and materialization errors returned by
/// `threadlane-authority`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthorityError {
    /// Input field or collection failed length, count, or format validation.
    #[error("validation failed on `{field}`: {reason}")]
    Validation {
        /// Name of the invalid field or parameter.
        field: String,
        /// Human-readable validation failure reason.
        reason: String,
    },

    /// Requested `(plan_id, revision)` does not exist in the [`threadlane_plans::PlanStore`].
    #[error("plan `{plan_id}` revision {revision} not found")]
    PlanNotFound {
        /// Missing plan identifier.
        plan_id: String,
        /// Missing plan revision number.
        revision: u64,
    },

    /// Requested `approval_id` does not exist in the [`crate::AuthorityStore`].
    #[error("approval `{approval_id}` not found")]
    ApprovalNotFound {
        /// Missing approval identifier.
        approval_id: String,
    },

    /// Plan binding identifier did not match the operator approval request's `binding_id`.
    #[error(
        "binding mismatch for plan `{plan_id}`: plan belongs to binding `{expected_binding_id}`, got `{actual_binding_id}`"
    )]
    BindingMismatch {
        /// Target plan identifier.
        plan_id: String,
        /// Binding identifier recorded on the plan revision.
        expected_binding_id: String,
        /// Conflicting binding identifier supplied in the approval request.
        actual_binding_id: String,
    },

    /// Runtime session binding's `thread_uuid` did not match the approved `thread_uuid`.
    #[error(
        "thread UUID mismatch when materializing approval `{approval_id}`: approved thread `{expected_thread_uuid}`, session binding thread `{actual_thread_uuid}`"
    )]
    ThreadUuidMismatch {
        /// Target approval identifier.
        approval_id: String,
        /// Canonical thread UUID frozen in the approval record.
        expected_thread_uuid: String,
        /// Conflicting thread UUID supplied by the runtime session binding.
        actual_thread_uuid: String,
    },

    /// Target plan revision is `draft` or `superseded` and cannot be approved or materialized.
    #[error(
        "plan `{plan_id}` revision {revision} has status `{status}` and is not an active published revision"
    )]
    UnpublishedOrSupersededPlan {
        /// Target plan identifier.
        plan_id: String,
        /// Target plan revision number.
        revision: u64,
        /// Observed non-active lifecycle status (`draft` or `superseded`).
        status: PlanStatus,
    },

    /// Registry revision pinned by the plan, expected by the operator, or currently stored in
    /// [`threadlane_registry::RegistryStore`] did not match.
    #[error(
        "registry revision mismatch for plan `{plan_id}` revision {plan_revision}: expected {expected_registry_revision}, plan pins {plan_profile_revision}, registry is at {actual_registry_revision}"
    )]
    StaleRegistryRevision {
        /// Target plan identifier.
        plan_id: String,
        /// Target plan revision number.
        plan_revision: u64,
        /// Expected registry revision supplied in the approval request.
        expected_registry_revision: u64,
        /// Registry revision pinned on the plan revision.
        plan_profile_revision: u64,
        /// Current revision of the global registry store.
        actual_registry_revision: u64,
    },

    /// Compiled plan digest did not match the operator's `expected_plan_digest`.
    #[error(
        "compiled plan digest mismatch for plan `{plan_id}` revision {plan_revision}: expected `{expected_plan_digest}`, actual `{actual_plan_digest}`"
    )]
    PlanDigestMismatch {
        /// Target plan identifier.
        plan_id: String,
        /// Target plan revision number.
        plan_revision: u64,
        /// Digest supplied in the operator approval request.
        expected_plan_digest: String,
        /// Digest computed by compiling the plan revision against the registry snapshot.
        actual_plan_digest: String,
    },

    /// An entry in `approved_task_ids` does not exist in the compiled plan revision.
    #[error("unknown task `{task_id}` in plan `{plan_id}` revision {plan_revision}")]
    UnknownTask {
        /// Target plan identifier.
        plan_id: String,
        /// Target plan revision number.
        plan_revision: u64,
        /// Unknown task identifier.
        task_id: String,
    },

    /// Selected task uses an adapter that is unsupported for headless worker execution or
    /// native follower routing.
    #[error("unsupported adapter `{adapter}` on task `{task_id}` in plan `{plan_id}`: {reason}")]
    UnsupportedTaskAdapter {
        /// Target plan identifier.
        plan_id: String,
        /// Task identifier with the unsupported adapter.
        task_id: String,
        /// Rejected tool adapter kind.
        adapter: ToolAdapter,
        /// Explanation of why the adapter is rejected in workflow authority.
        reason: String,
    },

    /// CLI worker executable path could not be resolved to a valid canonical regular executable.
    #[error("invalid executable for tool `{tool_id}` on task `{task_id}`: {reason}")]
    InvalidExecutable {
        /// Registry tool identifier.
        tool_id: String,
        /// Task identifier referencing the tool.
        task_id: String,
        /// Resolution or validation failure reason.
        reason: String,
    },

    /// Target repository path is not an existing, canonical, non-symlink Git repository.
    #[error("invalid repository path `{path}`: {reason}")]
    InvalidRepository {
        /// Rejected repository path.
        path: String,
        /// Validation failure reason.
        reason: String,
    },

    /// Base commit is not an exact 40-hex commit SHA existing in the target Git repository.
    #[error("invalid base commit `{base_commit}`: {reason}")]
    InvalidBaseCommit {
        /// Rejected base commit string.
        base_commit: String,
        /// Validation or Git resolution failure reason.
        reason: String,
    },

    /// An operator-approved or task write path failed format, symlink, ambiguity, or protected
    /// control-path validation.
    #[error("invalid write scope `{path}`: {reason}")]
    InvalidWriteScope {
        /// Rejected write path string.
        path: String,
        /// Validation failure reason.
        reason: String,
    },

    /// A selected task's write path is not contained within the explicit operator allowlist.
    #[error(
        "selected task `{task_id}` write path `{task_path}` is not contained in operator approved_write_paths: {reason}"
    )]
    UnapprovedTaskWriteScope {
        /// Selected task identifier.
        task_id: String,
        /// Uncovered task write path.
        task_path: String,
        /// Explanation of the containment failure.
        reason: String,
    },

    /// Work area path failed private canonical/non-symlink or repository non-overlap validation.
    #[error("invalid work_area_root `{path}`: {reason}")]
    InvalidWorkArea {
        /// Rejected work area path.
        path: String,
        /// Validation failure reason.
        reason: String,
    },

    /// Synthesized `WorkersConfig` failed side-effect-free runtime validation.
    #[error("synthesized workers configuration is invalid: {reason}")]
    WorkersConfigInvalid {
        /// Underlying validation failure message.
        reason: String,
    },

    /// Path traversal, symlink component, or permission violation on the authority store or
    /// target paths.
    #[error("security violation at `{path}`: {reason}")]
    SecurityViolation {
        /// Path that triggered the security violation.
        path: String,
        /// Explanation of the violation.
        reason: String,
    },

    /// On-disk authority ledger is corrupt or fails integrity/subject-digest verification;
    /// the corrupt file is preserved without modification.
    #[error("corrupt authority store at `{path}`: {reason}")]
    CorruptStore {
        /// Path to the corrupt authority store file.
        path: String,
        /// Explanation of the structural or digest corruption.
        reason: String,
    },

    /// Filesystem I/O failure while reading, locking, or persisting the authority store.
    #[error("I/O error during `{operation}`: {reason}")]
    Io {
        /// Name of the operation that failed.
        operation: String,
        /// Underlying OS error message.
        reason: String,
    },

    /// Error returned by the global [`threadlane_registry::RegistryStore`].
    #[error("registry error: {reason}")]
    Registry {
        /// Underlying registry error message.
        reason: String,
    },

    /// Error returned by [`threadlane_plans::PlanStore`] or plan compilation.
    #[error("plan error: {source}")]
    Plan {
        /// Underlying structured [`PlanError`].
        #[from]
        source: PlanError,
    },
}

impl AuthorityError {
    /// Returns a stable `snake_case` error code identifier for wire and diagnostic reporting.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "validation",
            Self::PlanNotFound { .. } => "plan_not_found",
            Self::ApprovalNotFound { .. } => "approval_not_found",
            Self::BindingMismatch { .. } => "binding_mismatch",
            Self::ThreadUuidMismatch { .. } => "thread_uuid_mismatch",
            Self::UnpublishedOrSupersededPlan { .. } => "unpublished_or_superseded_plan",
            Self::StaleRegistryRevision { .. } => "stale_registry_revision",
            Self::PlanDigestMismatch { .. } => "plan_digest_mismatch",
            Self::UnknownTask { .. } => "unknown_task",
            Self::UnsupportedTaskAdapter { .. } => "unsupported_task_adapter",
            Self::InvalidExecutable { .. } => "invalid_executable",
            Self::InvalidRepository { .. } => "invalid_repository",
            Self::InvalidBaseCommit { .. } => "invalid_base_commit",
            Self::InvalidWriteScope { .. } => "invalid_write_scope",
            Self::UnapprovedTaskWriteScope { .. } => "unapproved_task_write_scope",
            Self::InvalidWorkArea { .. } => "invalid_work_area",
            Self::WorkersConfigInvalid { .. } => "workers_config_invalid",
            Self::SecurityViolation { .. } => "security_violation",
            Self::CorruptStore { .. } => "corrupt_store",
            Self::Io { .. } => "io",
            Self::Registry { .. } => "registry",
            Self::Plan { .. } => "plan",
        }
    }

    pub(crate) fn validation(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

impl From<std::io::Error> for AuthorityError {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            operation: "filesystem".to_string(),
            reason: err.to_string(),
        }
    }
}
