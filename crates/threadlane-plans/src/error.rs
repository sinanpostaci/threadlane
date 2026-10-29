use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadlane_registry::RegistryError;

use crate::types::{PlanStatus, ReplacementState};

/// Structured domain, validation, persistence, compiler, and reconciliation errors returned by
/// `threadlane-plans`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanError {
    /// Input field or collection failed length, count, or format validation.
    #[error("validation failed on `{field}`: {reason}")]
    Validation {
        /// Name of the invalid field or parameter.
        field: String,
        /// Human-readable validation failure reason.
        reason: String,
    },

    /// Two or more tasks within the same plan revision share an identical `id`.
    #[error("duplicate task id `{task_id}` in plan `{plan_id}`")]
    DuplicateTaskId {
        /// Target plan identifier.
        plan_id: String,
        /// Duplicated task identifier.
        task_id: String,
    },

    /// A task declares a `depends_on` reference to a task ID that does not exist in the plan.
    #[error("task `{task_id}` in plan `{plan_id}` depends on unknown task `{dependency_id}`")]
    UnknownDependency {
        /// Target plan identifier.
        plan_id: String,
        /// Task declaring the missing dependency.
        task_id: String,
        /// Referenced task identifier not present in the plan revision.
        dependency_id: String,
    },

    /// The task dependency graph contains a cycle (including self-dependencies).
    #[error("dependency cycle detected in plan `{plan_id}` involving task `{task_id}`")]
    DependencyCycle {
        /// Target plan identifier.
        plan_id: String,
        /// Task identifier participating in the cycle.
        task_id: String,
    },

    /// An entry in `allowed_write_paths` is not a clean repository-relative non-protected path.
    #[error(
        "invalid allowed write path `{path}` on task `{task_id}` in plan `{plan_id}`: {reason}"
    )]
    InvalidWriteScope {
        /// Target plan identifier.
        plan_id: String,
        /// Task declaring the invalid write path.
        task_id: String,
        /// Rejected write path string.
        path: String,
        /// Reason the path was rejected.
        reason: String,
    },

    /// The requested `plan_id` (or specific revision) does not exist in the ledger.
    #[error("plan `{plan_id}` not found")]
    PlanNotFound {
        /// Missing plan identifier.
        plan_id: String,
    },

    /// `expected_revision` (or transition target revision) did not match the plan's current revision.
    #[error(
        "stale revision for plan `{plan_id}`: expected revision {expected_revision}, actual revision is {actual_revision}"
    )]
    StaleRevision {
        /// Target plan identifier.
        plan_id: String,
        /// Revision supplied by the caller.
        expected_revision: u64,
        /// Current persisted revision of the plan (`0` if the plan does not exist).
        actual_revision: u64,
    },

    /// A new draft attempted to change the `binding_id` of an existing plan.
    #[error(
        "binding mismatch for plan `{plan_id}`: expected binding `{expected_binding_id}`, got `{actual_binding_id}`"
    )]
    BindingMismatch {
        /// Target plan identifier.
        plan_id: String,
        /// Binding identifier recorded on the plan's existing revisions.
        expected_binding_id: String,
        /// Conflicting binding identifier supplied in `save_draft`.
        actual_binding_id: String,
    },

    /// Attempted to mutate or revert a revision that has already been published.
    #[error(
        "published revision {revision} of plan `{plan_id}` (status `{status}`) is immutable and cannot perform `{attempted_action}`"
    )]
    ImmutablePublishedRevision {
        /// Target plan identifier.
        plan_id: String,
        /// Immutable published revision number.
        revision: u64,
        /// Current persisted status of the revision.
        status: PlanStatus,
        /// Rejected operation name.
        attempted_action: String,
    },

    /// Requested lifecycle transition is out of order or invalid for the revision's current status.
    #[error(
        "invalid state transition for plan `{plan_id}` revision {revision}: cannot transition from `{current_status}` to `{attempted_status}`"
    )]
    InvalidStateTransition {
        /// Target plan identifier.
        plan_id: String,
        /// Target revision number.
        revision: u64,
        /// Current persisted status of the revision.
        current_status: PlanStatus,
        /// Attempted target status.
        attempted_status: PlanStatus,
    },

    /// Attempted to compile an unpublished (`draft`) or `superseded` plan revision for runtime execution.
    #[error(
        "plan `{plan_id}` revision {revision} has status `{status}` and is not an active published revision"
    )]
    UnpublishedPlan {
        /// Target plan identifier.
        plan_id: String,
        /// Target revision number.
        revision: u64,
        /// Current status of the plan revision.
        status: PlanStatus,
    },

    /// The plan revision's `profile_revision` did not match `RegistrySnapshot::revision`.
    #[error(
        "profile revision mismatch for plan `{plan_id}` revision {plan_revision}: plan pins profile `{profile_id}` at revision {expected_profile_revision}, but registry snapshot is at revision {actual_registry_revision}"
    )]
    ProfileRevisionMismatch {
        /// Target plan identifier.
        plan_id: String,
        /// Plan revision number.
        plan_revision: u64,
        /// Workflow profile identifier referenced by the plan.
        profile_id: String,
        /// Profile revision pinned by the plan revision.
        expected_profile_revision: u64,
        /// Actual revision of the supplied registry snapshot.
        actual_registry_revision: u64,
    },

    /// Role/difficulty resolution or tool/model verification failed against the registry snapshot.
    #[error(
        "registry resolution failed for task `{task_id}` in plan `{plan_id}` (profile `{profile_id}`, code `{registry_code}`): {reason}"
    )]
    RegistryResolution {
        /// Target plan identifier.
        plan_id: String,
        /// Task identifier that failed resolution.
        task_id: String,
        /// Workflow profile identifier.
        profile_id: String,
        /// Error code reported by the registry resolver.
        registry_code: String,
        /// Safe diagnostic description.
        reason: String,
    },

    /// Prior task execution stop state is `unknown`, blocking replacement admission.
    #[error(
        "cannot admit replacement for task `{task_id}` in plan `{plan_id}`: prior stop state is `unknown`"
    )]
    UnknownStopBlocksAdmission {
        /// Target plan identifier.
        plan_id: String,
        /// Task identifier blocked by unknown stop state.
        task_id: String,
    },

    /// Prior task execution has not yet been proven stopped (`pending` or `cancel_requested`).
    #[error(
        "cannot admit replacement for task `{task_id}` in plan `{plan_id}` while replacement state is `{current_state}`"
    )]
    ReplacementNotStopped {
        /// Target plan identifier.
        plan_id: String,
        /// Task identifier awaiting proven stop.
        task_id: String,
        /// Current non-stopped replacement state.
        current_state: ReplacementState,
    },

    /// Invalid state transition in task replacement reconciliation.
    #[error(
        "invalid replacement state transition for task `{task_id}` in plan `{plan_id}` from `{current_state}`: {reason}"
    )]
    InvalidReplacementTransition {
        /// Target plan identifier.
        plan_id: String,
        /// Target task identifier.
        task_id: String,
        /// Current replacement state.
        current_state: ReplacementState,
        /// Explanation of why the transition was rejected.
        reason: String,
    },

    /// Store directory or file failed symlink, permission (`0700`/`0600`), or size cap checks.
    #[error("security violation in plan store: {0}")]
    SecurityViolation(String),

    /// Persisted plan ledger file is corrupt or has an incompatible schema version.
    #[error("corrupt plan store state: {0}")]
    CorruptStore(String),

    /// Underlying filesystem I/O error.
    #[error("plan store I/O error: {0}")]
    Io(String),

    /// JSON serialization or deserialization error.
    #[error("plan store serialization error: {0}")]
    Serialization(String),

    /// Current operating system does not support required private `0700`/`0600` filesystem semantics.
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),
}

impl PlanError {
    /// Returns a stable `snake_case` error code suitable for wire responses and UI diagnostics.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "validation_failed",
            Self::DuplicateTaskId { .. } => "duplicate_task_id",
            Self::UnknownDependency { .. } => "unknown_dependency",
            Self::DependencyCycle { .. } => "dependency_cycle",
            Self::InvalidWriteScope { .. } => "invalid_write_scope",
            Self::PlanNotFound { .. } => "plan_not_found",
            Self::StaleRevision { .. } => "stale_revision",
            Self::BindingMismatch { .. } => "binding_mismatch",
            Self::ImmutablePublishedRevision { .. } => "immutable_published_revision",
            Self::InvalidStateTransition { .. } => "invalid_state_transition",
            Self::UnpublishedPlan { .. } => "unpublished_plan",
            Self::ProfileRevisionMismatch { .. } => "profile_revision_mismatch",
            Self::RegistryResolution { .. } => "registry_resolution_failed",
            Self::UnknownStopBlocksAdmission { .. } => "unknown_stop_blocks_admission",
            Self::ReplacementNotStopped { .. } => "replacement_not_stopped",
            Self::InvalidReplacementTransition { .. } => "invalid_replacement_transition",
            Self::SecurityViolation(_) => "security_violation",
            Self::CorruptStore(_) => "corrupt_store",
            Self::Io(_) => "io_error",
            Self::Serialization(_) => "serialization_error",
            Self::UnsupportedPlatform(_) => "unsupported_platform",
        }
    }

    /// Returns a short, static, user-safe English summary message without filesystem paths or internals.
    pub const fn safe_message(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "Plan validation failed.",
            Self::DuplicateTaskId { .. } => "Plan contains duplicate task identifiers.",
            Self::UnknownDependency { .. } => "Task references an unknown dependency task.",
            Self::DependencyCycle { .. } => "Task dependency graph contains a cycle.",
            Self::InvalidWriteScope { .. } => {
                "Task write scope must use safe repository-relative paths."
            }
            Self::PlanNotFound { .. } => "Requested plan was not found.",
            Self::StaleRevision { .. } => "Plan revision changed; reload the latest plan revision.",
            Self::BindingMismatch { .. } => "Plan binding identifier cannot be changed.",
            Self::ImmutablePublishedRevision { .. } => {
                "Published plan revisions are immutable and cannot be modified."
            }
            Self::InvalidStateTransition { .. } => {
                "Requested transition is invalid for the current plan status."
            }
            Self::UnpublishedPlan { .. } => {
                "Only active published plan revisions can be compiled for runtime execution."
            }
            Self::ProfileRevisionMismatch { .. } => {
                "Plan profile revision does not match the current registry snapshot."
            }
            Self::RegistryResolution { .. } => {
                "Task role and difficulty could not be resolved against the workflow profile."
            }
            Self::UnknownStopBlocksAdmission { .. } => {
                "Prior task stop outcome is unknown; replacement admission is blocked."
            }
            Self::ReplacementNotStopped { .. } => {
                "Prior task execution must be proven stopped before replacement admission."
            }
            Self::InvalidReplacementTransition { .. } => {
                "Requested task replacement state transition is invalid."
            }
            Self::SecurityViolation(_) => {
                "Plan store directory or file failed security validation."
            }
            Self::CorruptStore(_) => "Persisted plan store is corrupt or incompatible.",
            Self::Io(_) => "Failed to read or write the plan store.",
            Self::Serialization(_) => "Failed to encode or decode plan store data.",
            Self::UnsupportedPlatform(_) => {
                "Current platform does not support required private store permissions."
            }
        }
    }

    /// Constructs a [`PlanError::Validation`] error.
    pub fn validation(field: &'static str, reason: impl Into<String>) -> Self {
        Self::Validation {
            field: field.to_string(),
            reason: reason.into(),
        }
    }

    /// Converts a [`RegistryError`] returned during task compilation into a [`PlanError::RegistryResolution`].
    pub fn from_registry_error(
        plan_id: impl Into<String>,
        task_id: impl Into<String>,
        profile_id: impl Into<String>,
        err: &RegistryError,
    ) -> Self {
        Self::RegistryResolution {
            plan_id: plan_id.into(),
            task_id: task_id.into(),
            profile_id: profile_id.into(),
            registry_code: err.code().to_string(),
            reason: err.to_string(),
        }
    }
}

impl From<std::io::Error> for PlanError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<serde_json::Error> for PlanError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}
