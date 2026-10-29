use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadlane_plans::PlanError;
use threadlane_registry::RegistryError;

/// Safe, redacted domain and persistence error returned by [`crate::ScopedWorkflow`].
///
/// Every variant carries a static, user-safe English message and a stable `snake_case`
/// [`WorkflowError::code`]. `Display` and `Debug` representations never expose other
/// bindings' identifiers, cross-binding revision counters, filesystem paths, CLI
/// executable paths, or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub enum WorkflowError {
    /// Input identifier, parameter, summary, task field, or limit failed validation.
    #[error("{0}")]
    InvalidInput(&'static str),

    /// Pagination cursor is malformed, out of bounds, or scoped to a different binding/filter/resource.
    #[error("{0}")]
    InvalidCursor(&'static str),

    /// Requested plan or revision was not found (also returned identically for any cross-binding access).
    #[error("{0}")]
    NotFound(&'static str),

    /// Expected revision, cursor revision watermark, or target revision is stale or superseded.
    #[error("{0}")]
    StaleRevision(&'static str),

    /// Pinned workflow profile revision does not match the current registry snapshot revision.
    #[error("{0}")]
    ProfileRevisionMismatch(&'static str),

    /// Workflow profile, role/difficulty assignment, model, effort, or enabled tool failed resolution.
    #[error("{0}")]
    RegistryResolution(&'static str),

    /// Two or more tasks in the same plan revision share an identical task identifier.
    #[error("{0}")]
    DuplicateTaskId(&'static str),

    /// A task declares a dependency on a task identifier that does not exist in the plan.
    #[error("{0}")]
    UnknownDependency(&'static str),

    /// The plan task dependency graph contains a cycle.
    #[error("{0}")]
    DependencyCycle(&'static str),

    /// A task write scope entry is not a clean, repository-relative, non-protected path.
    #[error("{0}")]
    InvalidWriteScope(&'static str),

    /// Requested plan lifecycle state transition is invalid.
    #[error("{0}")]
    InvalidStateTransition(&'static str),

    /// Idempotent command key or retry conflicted with a different payload or revision target.
    #[error("{0}")]
    IdempotencyConflict(&'static str),

    /// Serialized MCP response envelope exceeds the 256 KiB wire budget.
    #[error("{0}")]
    ResponseTooLarge(&'static str),

    /// Underlying store failed symlink, permission, or file-size security checks.
    #[error("{0}")]
    SecurityViolation(&'static str),

    /// Persisted registry or plan store is corrupt or uses an unsupported schema version.
    #[error("{0}")]
    CorruptStore(&'static str),

    /// Underlying filesystem or lock I/O error.
    #[error("{0}")]
    Io(&'static str),

    /// JSON serialization or deserialization failure.
    #[error("{0}")]
    Serialization(&'static str),

    /// Current operating system does not support required private store permissions.
    #[error("{0}")]
    UnsupportedPlatform(&'static str),
}

impl WorkflowError {
    /// Canonical static `not_found` error used identically for missing plans, missing revisions,
    /// and any cross-binding plan access or race.
    pub const PLAN_NOT_FOUND_MESSAGE: &'static str = "Requested plan was not found.";

    /// Constructs the canonical static `not_found` error for plan lookups and scope guards.
    pub const fn plan_not_found() -> Self {
        Self::NotFound(Self::PLAN_NOT_FOUND_MESSAGE)
    }

    /// Returns a stable `snake_case` machine-readable error code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::InvalidCursor(_) => "invalid_cursor",
            Self::NotFound(_) => "not_found",
            Self::StaleRevision(_) => "stale_revision",
            Self::ProfileRevisionMismatch(_) => "profile_revision_mismatch",
            Self::RegistryResolution(_) => "registry_resolution_failed",
            Self::DuplicateTaskId(_) => "duplicate_task_id",
            Self::UnknownDependency(_) => "unknown_dependency",
            Self::DependencyCycle(_) => "dependency_cycle",
            Self::InvalidWriteScope(_) => "invalid_write_scope",
            Self::InvalidStateTransition(_) => "invalid_state_transition",
            Self::IdempotencyConflict(_) => "idempotency_conflict",
            Self::ResponseTooLarge(_) => "response_too_large",
            Self::SecurityViolation(_) => "security_violation",
            Self::CorruptStore(_) => "corrupt_store",
            Self::Io(_) => "io_error",
            Self::Serialization(_) => "serialization_error",
            Self::UnsupportedPlatform(_) => "unsupported_platform",
        }
    }

    /// Returns the static, user-safe English message for this error.
    pub const fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidInput(msg)
            | Self::InvalidCursor(msg)
            | Self::NotFound(msg)
            | Self::StaleRevision(msg)
            | Self::ProfileRevisionMismatch(msg)
            | Self::RegistryResolution(msg)
            | Self::DuplicateTaskId(msg)
            | Self::UnknownDependency(msg)
            | Self::DependencyCycle(msg)
            | Self::InvalidWriteScope(msg)
            | Self::InvalidStateTransition(msg)
            | Self::IdempotencyConflict(msg)
            | Self::ResponseTooLarge(msg)
            | Self::SecurityViolation(msg)
            | Self::CorruptStore(msg)
            | Self::Io(msg)
            | Self::Serialization(msg)
            | Self::UnsupportedPlatform(msg) => msg,
        }
    }

    /// Returns `true` if this error indicates a stale plan, cursor, or registry revision.
    pub const fn is_stale(&self) -> bool {
        matches!(
            self,
            Self::StaleRevision(_) | Self::ProfileRevisionMismatch(_)
        )
    }

    /// Maps a [`RegistryError`] into a redacted [`WorkflowError`].
    pub const fn from_registry_error(err: &RegistryError) -> Self {
        match err {
            RegistryError::InvalidInput(_) => {
                Self::InvalidInput("Registry input validation failed.")
            }
            RegistryError::InvalidReference(_)
            | RegistryError::DuplicateAssignment(_)
            | RegistryError::UnsupportedEffort(_)
            | RegistryError::ReferenceConflict(_)
            | RegistryError::MissingAssignment(_) => Self::RegistryResolution(
                "Task role and difficulty could not be resolved against the workflow profile.",
            ),
            RegistryError::NotFound(_) => {
                Self::RegistryResolution("Referenced workflow profile was not found in registry.")
            }
            RegistryError::StaleRevision { .. } => {
                Self::StaleRevision("Registry revision changed; reload the latest catalog.")
            }
            RegistryError::CapacityExceeded(_) => {
                Self::InvalidInput("Registry capacity limit exceeded.")
            }
            RegistryError::SecurityViolation(_) => {
                Self::SecurityViolation("Registry store failed security validation.")
            }
            RegistryError::CorruptStore(_) => {
                Self::CorruptStore("Persisted registry store is corrupt or incompatible.")
            }
            RegistryError::Io(_) => Self::Io("Failed to read or lock the registry store."),
        }
    }

    /// Maps a [`PlanError`] into a redacted [`WorkflowError`].
    ///
    /// Note: Cross-binding [`PlanError::BindingMismatch`] is always mapped to the canonical
    /// static [`WorkflowError::plan_not_found`] so cross-binding existence is never leaked.
    pub const fn from_plan_error(err: &PlanError) -> Self {
        match err {
            PlanError::Validation { .. } => Self::InvalidInput("Plan input validation failed."),
            PlanError::DuplicateTaskId { .. } => {
                Self::DuplicateTaskId("Plan contains duplicate task identifiers.")
            }
            PlanError::UnknownDependency { .. } => {
                Self::UnknownDependency("Task references an unknown dependency task.")
            }
            PlanError::DependencyCycle { .. } => {
                Self::DependencyCycle("Task dependency graph contains a cycle.")
            }
            PlanError::InvalidWriteScope { .. } => Self::InvalidWriteScope(
                "Task write scope must use safe repository-relative non-protected paths.",
            ),
            PlanError::PlanNotFound { .. } | PlanError::BindingMismatch { .. } => {
                Self::plan_not_found()
            }
            PlanError::StaleRevision { .. } => {
                Self::StaleRevision("Plan revision is stale; reload the latest plan revision.")
            }
            PlanError::ImmutablePublishedRevision { .. } => {
                Self::StaleRevision("Target plan revision is superseded or immutable.")
            }
            PlanError::InvalidStateTransition { .. }
            | PlanError::UnpublishedPlan { .. }
            | PlanError::UnknownStopBlocksAdmission { .. }
            | PlanError::ReplacementNotStopped { .. }
            | PlanError::InvalidReplacementTransition { .. } => Self::InvalidStateTransition(
                "Requested transition is invalid for the current plan status.",
            ),
            PlanError::ProfileRevisionMismatch { .. } => Self::ProfileRevisionMismatch(
                "Plan profile revision does not match the current registry snapshot.",
            ),
            PlanError::RegistryResolution { .. } => Self::RegistryResolution(
                "Task role and difficulty could not be resolved against the workflow profile.",
            ),
            PlanError::SecurityViolation(_) => {
                Self::SecurityViolation("Plan store failed security validation.")
            }
            PlanError::CorruptStore(_) => {
                Self::CorruptStore("Persisted plan store is corrupt or incompatible.")
            }
            PlanError::Io(_) => Self::Io("Failed to read or write the plan store."),
            PlanError::Serialization(_) => {
                Self::Serialization("Failed to encode or decode workflow data.")
            }
            PlanError::UnsupportedPlatform(_) => Self::UnsupportedPlatform(
                "Current platform does not support required private store permissions.",
            ),
        }
    }
}

impl From<RegistryError> for WorkflowError {
    fn from(err: RegistryError) -> Self {
        Self::from_registry_error(&err)
    }
}

impl From<PlanError> for WorkflowError {
    fn from(err: PlanError) -> Self {
        Self::from_plan_error(&err)
    }
}
