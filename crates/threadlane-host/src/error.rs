use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadlane::ErrorCode as RuntimeErrorCode;
use threadlane_authority::AuthorityError;
use threadlane_core::CoreError;
use threadlane_operations::OperationsError;
use threadlane_plans::{PlanError, PlanStatus};
use threadlane_registry::{RegistryError, ToolAdapter};

use crate::types::HostPhase;

/// Structured domain, security, lifecycle, and dispatch errors returned by `threadlane-host`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostError {
    /// Input identifier, UUID, or parameter failed format validation.
    #[error("validation failed on `{field}`: {reason}")]
    Validation {
        /// Name of the invalid field.
        field: String,
        /// Explanation of the validation failure.
        reason: String,
    },

    /// Requested `approval_id` does not exist in [`threadlane_authority::AuthorityStore`].
    #[error("approval `{approval_id}` not found")]
    ApprovalNotFound {
        /// Missing approval identifier.
        approval_id: String,
    },

    /// Requested `task_id` does not exist in the frozen [`threadlane_authority::ApprovalRecord`].
    #[error("unknown task `{task_id}` in approval `{approval_id}`")]
    UnknownTask {
        /// Target approval identifier.
        approval_id: String,
        /// Unknown task identifier.
        task_id: String,
    },

    /// Authoritative caller `control_binding_id`, `native_thread_uuid`, or runtime `session_binding_id`
    /// did not match the persisted approval or host session.
    #[error("scope mismatch on `{field}`: expected `{expected}`, got `{actual}` ({reason})")]
    ScopeMismatch {
        /// Field that failed scope verification (`control_binding_id`, `thread_uuid`, or `session_binding_id`).
        field: String,
        /// Expected authoritative identifier.
        expected: String,
        /// Conflicting identifier observed or supplied.
        actual: String,
        /// Explanation of the scope denial.
        reason: String,
    },

    /// Task has non-empty `depends_on` and cannot be dispatched because static `base_commit`
    /// does not include verified predecessor outputs.
    #[error(
        "cannot dispatch dependent task `{task_id}` in approval `{approval_id}` (depends_on: {depends_on:?}): dependency inputs are unverified on static base_commit"
    )]
    DependencyInputsUnverified {
        /// Target approval identifier.
        approval_id: String,
        /// Dependent task identifier.
        task_id: String,
        /// Upstream task identifiers required before this task could run.
        depends_on: Vec<String>,
    },

    /// Task targets a manual native follower route (`codex_cli`) and cannot be dispatched as a
    /// headless worker.
    #[error(
        "task `{task_id}` in approval `{approval_id}` uses adapter `{adapter}` and requires manual native routing"
    )]
    NativeRouteRequired {
        /// Target approval identifier.
        approval_id: String,
        /// Native route task identifier.
        task_id: String,
        /// Non-worker adapter (`ToolAdapter::CodexCli`).
        adapter: ToolAdapter,
    },

    /// Task has not been started yet (no sealed `task_id -> run_id` receipt exists).
    #[error("task `{task_id}` in approval `{approval_id}` has not been started")]
    TaskNotStarted {
        /// Target approval identifier.
        approval_id: String,
        /// Unstarted task identifier.
        task_id: String,
    },

    /// Source plan revision in [`threadlane_plans::PlanStore`] is `draft` or `superseded` and
    /// cannot start or materialize.
    #[error(
        "plan `{plan_id}` revision {revision} has status `{status}` and cannot be started or materialized"
    )]
    UnpublishedOrSupersededPlan {
        /// Target plan identifier.
        plan_id: String,
        /// Target plan revision number.
        revision: u64,
        /// Non-active lifecycle status (`draft` or `superseded`).
        status: PlanStatus,
    },

    /// Another active host process already holds the per-session exclusive `host.lock`.
    #[error(
        "host session for approval `{approval_id}` at `{session_dir}` is already locked by an active host process"
    )]
    LockBusy {
        /// Contended approval identifier.
        approval_id: String,
        /// Session directory path.
        session_dir: PathBuf,
    },

    /// Host session is not running or has not reached authenticated `Ready` phase.
    #[error("host session for approval `{approval_id}` is not ready (phase: {phase:?}): {reason}")]
    HostNotReady {
        /// Target approval identifier.
        approval_id: String,
        /// Observed session phase, if a manifest exists.
        phase: Option<HostPhase>,
        /// Explanation of why the host session is not ready.
        reason: String,
    },

    /// Host session cannot be stopped because active runs, workers, tasks, or wake-ups remain.
    #[error("host session for approval `{approval_id}` is not idle: {detail}")]
    HostNotIdle {
        /// Target approval identifier.
        approval_id: String,
        /// Sanitized busy breakdown or reason.
        detail: String,
    },

    /// Expected host instance identifier did not match the manifest or runtime instance identifier.
    #[error(
        "host session instance mismatch for approval `{approval_id}`: expected `{expected_instance_id}`, actual `{actual_instance_id:?}`"
    )]
    InstanceMismatch {
        /// Target approval identifier.
        approval_id: String,
        /// Expected host instance identifier (`hst_<32hex>`).
        expected_instance_id: String,
        /// Observed host instance identifier in the manifest or runtime (`None` for legacy manifests).
        actual_instance_id: Option<String>,
    },

    /// Idle-stop request was sent or lost, and durable `Stopped` + released `host.lock` proof was not observed before timeout.
    #[error("host session stop outcome is uncertain for approval `{approval_id}`: {detail}")]
    StopOutcomeUncertain {
        /// Target approval identifier.
        approval_id: String,
        /// Explanation of why durable stop completion could not be confirmed.
        detail: String,
    },

    /// Explicit stopped-session restart was refused because the session is not in a clean `Stopped` state or failed prechecks.
    #[error(
        "host session restart is not allowed for approval `{approval_id}` (phase: {phase:?}): {detail}"
    )]
    RestartNotAllowed {
        /// Target approval identifier.
        approval_id: String,
        /// Observed session phase, if a manifest exists.
        phase: Option<HostPhase>,
        /// Explanation of why restart was refused.
        detail: String,
    },

    /// Revision-checked cancellation failed because `expected_revision` did not match the worker run's revision.
    #[error(
        "stale revision for worker run `{run_id}`: expected revision {expected_revision}, actual revision {actual_revision}"
    )]
    StaleRevision {
        /// Target worker run identifier.
        run_id: String,
        /// Revision expected by the caller.
        expected_revision: u64,
        /// Actual current revision of the run.
        actual_revision: u64,
    },

    /// Deterministic `command_key` was reused with conflicting operation or parameters.
    #[error("idempotency conflict on binding `{binding_id}` for command_key `{command_key}`")]
    IdempotencyConflict {
        /// Target binding identifier.
        binding_id: String,
        /// Conflicting command key.
        command_key: String,
    },

    /// Operation is invalid for the worker run's current lifecycle state.
    #[error("invalid run state for `{run_id}` during `{action}`: {reason}")]
    InvalidRunState {
        /// Target worker run identifier.
        run_id: String,
        /// Attempted operation name.
        action: String,
        /// Explanation of the invalid state transition.
        reason: String,
    },

    /// Path traversal, symlink, foreign file, non-private permission, or directory overlap violation.
    #[error("security violation at `{path}`: {reason}")]
    SecurityViolation {
        /// Path where the security check failed.
        path: String,
        /// Sanitized reason for the refusal.
        reason: String,
    },

    /// On-disk `host-manifest.json` is malformed or fails integrity verification; preserved untouched.
    #[error("corrupt host session manifest at `{path}`: {reason}")]
    CorruptManifest {
        /// Path to the corrupt manifest file.
        path: String,
        /// Explanation of the corruption or invariant mismatch.
        reason: String,
    },

    /// On-disk `task-receipts.json` is malformed or fails integrity verification; preserved untouched.
    #[error("corrupt task receipts ledger at `{path}`: {reason}")]
    CorruptReceipts {
        /// Path to the corrupt task receipts file.
        path: String,
        /// Explanation of the corruption or invariant mismatch.
        reason: String,
    },

    /// Runtime session state violated single-binding or recovery invariants.
    #[error("corrupt host session state: {reason}")]
    CorruptSessionState {
        /// Explanation of the session state violation.
        reason: String,
    },

    /// Underlying authority store or materialization error.
    #[error("authority error [{code}]: {message}")]
    Authority {
        /// Stable authority error code.
        code: String,
        /// Sanitized authority error message.
        message: String,
    },

    /// Underlying runtime supervisor or IPC error.
    #[error("runtime error [{code}]: {message}")]
    Runtime {
        /// Stable runtime error code.
        code: String,
        /// Sanitized runtime error message.
        message: String,
    },

    /// Filesystem or OS I/O error.
    #[error("I/O error during `{operation}`: {reason}")]
    Io {
        /// Operation label.
        operation: String,
        /// Sanitized OS error description.
        reason: String,
    },
}

impl HostError {
    /// Returns a stable `snake_case` error code identifier for wire, CLI, and test assertions.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "validation",
            Self::ApprovalNotFound { .. } => "approval_not_found",
            Self::UnknownTask { .. } => "unknown_task",
            Self::ScopeMismatch { .. } => "scope_mismatch",
            Self::DependencyInputsUnverified { .. } => "dependency_inputs_unverified",
            Self::NativeRouteRequired { .. } => "native_route_required",
            Self::TaskNotStarted { .. } => "not_started",
            Self::UnpublishedOrSupersededPlan { .. } => "unpublished_or_superseded_plan",
            Self::LockBusy { .. } => "lock_busy",
            Self::HostNotReady { .. } => "host_not_ready",
            Self::HostNotIdle { .. } => "host_not_idle",
            Self::InstanceMismatch { .. } => "host_instance_mismatch",
            Self::StopOutcomeUncertain { .. } => "host_stop_outcome_uncertain",
            Self::RestartNotAllowed { .. } => "host_restart_not_allowed",
            Self::StaleRevision { .. } => "stale_revision",
            Self::IdempotencyConflict { .. } => "idempotency_conflict",
            Self::InvalidRunState { .. } => "invalid_run_state",
            Self::SecurityViolation { .. } => "security_violation",
            Self::CorruptManifest { .. } => "corrupt_manifest",
            Self::CorruptReceipts { .. } => "corrupt_receipts",
            Self::CorruptSessionState { .. } => "corrupt_session_state",
            Self::Authority { .. } => "authority_error",
            Self::Runtime { .. } => "runtime_error",
            Self::Io { .. } => "io",
        }
    }

    pub(crate) fn validation(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

impl From<AuthorityError> for HostError {
    fn from(err: AuthorityError) -> Self {
        match err {
            AuthorityError::Validation { field, reason } => Self::Validation { field, reason },
            AuthorityError::ApprovalNotFound { approval_id } => {
                Self::ApprovalNotFound { approval_id }
            }
            AuthorityError::BindingMismatch {
                plan_id,
                expected_binding_id,
                actual_binding_id,
            } => Self::ScopeMismatch {
                field: "control_binding_id".to_string(),
                expected: expected_binding_id,
                actual: actual_binding_id,
                reason: format!("binding mismatch on plan `{plan_id}`"),
            },
            AuthorityError::ThreadUuidMismatch {
                approval_id,
                expected_thread_uuid,
                actual_thread_uuid,
            } => Self::ScopeMismatch {
                field: "thread_uuid".to_string(),
                expected: expected_thread_uuid,
                actual: actual_thread_uuid,
                reason: format!("thread_uuid mismatch on approval `{approval_id}`"),
            },
            AuthorityError::UnpublishedOrSupersededPlan {
                plan_id,
                revision,
                status,
            } => Self::UnpublishedOrSupersededPlan {
                plan_id,
                revision,
                status,
            },
            AuthorityError::SecurityViolation { path, reason } => {
                Self::SecurityViolation { path, reason }
            }
            other => Self::Authority {
                code: other.code().to_string(),
                message: other.to_string(),
            },
        }
    }
}

impl From<PlanError> for HostError {
    fn from(err: PlanError) -> Self {
        Self::Authority {
            code: err.code().to_string(),
            message: err.to_string(),
        }
    }
}

impl From<RegistryError> for HostError {
    fn from(err: RegistryError) -> Self {
        Self::Authority {
            code: err.code().to_string(),
            message: err.to_string(),
        }
    }
}

impl From<threadlane::ApiError> for HostError {
    fn from(err: threadlane::ApiError) -> Self {
        Self::from(threadlane::Error::Remote(err))
    }
}

impl From<threadlane::Error> for HostError {
    fn from(err: threadlane::Error) -> Self {
        match err {
            threadlane::Error::LockBusy { state_dir } => Self::LockBusy {
                approval_id: "runtime-state".to_string(),
                session_dir: state_dir,
            },
            threadlane::Error::SecurityViolation(reason) => Self::SecurityViolation {
                path: "runtime-state".to_string(),
                reason,
            },
            threadlane::Error::Core(core_err) => map_core_err(core_err),
            threadlane::Error::Remote(api_err) => {
                if api_err.code == RuntimeErrorCode::StaleRevision {
                    if let Some(details) = &api_err.details {
                        let run_id = details
                            .get("run_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let expected_revision = details
                            .get("expected_revision")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let actual_revision = details
                            .get("actual_revision")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        return Self::StaleRevision {
                            run_id,
                            expected_revision,
                            actual_revision,
                        };
                    }
                }
                if api_err.code == RuntimeErrorCode::IdempotencyConflict {
                    if let Some(details) = &api_err.details {
                        let binding_id = details
                            .get("binding_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let command_key = details
                            .get("command_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        return Self::IdempotencyConflict {
                            binding_id,
                            command_key,
                        };
                    }
                }
                if api_err.code == RuntimeErrorCode::InvalidRunState {
                    if let Some(details) = &api_err.details {
                        let run_id = details
                            .get("run_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let action = details
                            .get("action")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        return Self::InvalidRunState {
                            run_id,
                            action,
                            reason: api_err.message,
                        };
                    }
                }
                if api_err.code == RuntimeErrorCode::HostScopeMismatch {
                    if let Some(details) = &api_err.details {
                        let field = details
                            .get("field")
                            .and_then(|v| v.as_str())
                            .unwrap_or("scope")
                            .to_string();
                        let expected = details
                            .get("expected")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let actual = details
                            .get("actual")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        return Self::ScopeMismatch {
                            field,
                            expected,
                            actual,
                            reason: api_err.message,
                        };
                    }
                }
                if api_err.code == RuntimeErrorCode::HostInstanceMismatch {
                    if let Some(details) = &api_err.details {
                        let approval_id = details
                            .get("approval_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let expected_instance_id = details
                            .get("expected_instance_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();
                        let actual_instance_id = details
                            .get("actual_instance_id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        return Self::InstanceMismatch {
                            approval_id,
                            expected_instance_id,
                            actual_instance_id,
                        };
                    }
                }
                Self::Runtime {
                    code: runtime_error_code_str(api_err.code),
                    message: api_err.message,
                }
            }
            threadlane::Error::InvalidConfig(reason) => Self::Validation {
                field: "config".to_string(),
                reason,
            },
            other => {
                let api = other.to_api_error();
                Self::Runtime {
                    code: runtime_error_code_str(api.code),
                    message: api.message,
                }
            }
        }
    }
}

fn runtime_error_code_str(code: RuntimeErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{code:?}").to_ascii_lowercase())
}

fn map_core_err(err: CoreError) -> HostError {
    match err {
        CoreError::Validation { field, reason } => HostError::Validation {
            field: field.to_string(),
            reason,
        },
        CoreError::StaleRevision {
            run_id,
            expected_revision,
            actual_revision,
        } => HostError::StaleRevision {
            run_id,
            expected_revision,
            actual_revision,
        },
        CoreError::IdempotencyConflict {
            binding_id,
            command_key,
        } => HostError::IdempotencyConflict {
            binding_id,
            command_key,
        },
        CoreError::InvalidRunState {
            run_id,
            status,
            action,
        } => HostError::InvalidRunState {
            run_id,
            action: action.to_string(),
            reason: format!("run is in status `{status:?}`"),
        },
        other => HostError::Runtime {
            code: "core_error".to_string(),
            message: other.to_string(),
        },
    }
}

impl From<OperationsError> for HostError {
    fn from(err: OperationsError) -> Self {
        match err {
            OperationsError::InvalidInput(reason) => Self::Validation {
                field: "dependency_input".to_string(),
                reason,
            },
            OperationsError::SecurityViolation(reason) => Self::SecurityViolation {
                path: "work-area".to_string(),
                reason,
            },
            OperationsError::Io(io_err) => Self::Io {
                operation: "operations".to_string(),
                reason: io_err.to_string(),
            },
            other => Self::Runtime {
                code: other.code().to_string(),
                message: other.to_string(),
            },
        }
    }
}

impl From<std::io::Error> for HostError {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            operation: "filesystem".to_string(),
            reason: err.to_string(),
        }
    }
}
