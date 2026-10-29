use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadlane_workers::{CleanupRefusalReason, WorkerError};

/// Stable machine-readable error code for `threadlane-operations` callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationsErrorCode {
    /// Current operating system is not supported for verified Unix process-group and permission checks.
    UnsupportedPlatform,
    /// Caller supplied invalid identifiers, SHAs, paths, or parameters.
    InvalidInput,
    /// Path traversal, symlink component, or permission invariant violation.
    SecurityViolation,
    /// Target directory or worktree is not an owned Threadlane worktree checkout.
    WorktreeNotOwned,
    /// An active worker job or lock is held on the target worktree.
    ActiveJob,
    /// Worktree contains uncommitted, staged, untracked, or ignored files.
    DirtyWorktree,
    /// Post-run Git diff gate rejected the worktree changes or exceeded budgets.
    DiffGateRejected,
    /// Worker outcome is uncertain, failed, or lacks required telemetry evidence.
    UncertainWorkerOutcome,
    /// Live worktree state, scope, policy, or evidence does not match the review subject.
    SubjectMismatch,
    /// Review or integration receipt is invalid or has been invalidated by subsequent changes.
    ReceiptInvalidated,
    /// Current repository, integration, or task `HEAD` commit moved from the expected SHA.
    HeadMoved,
    /// Required commit ancestry relationship does not hold.
    AncestryMismatch,
    /// Merge or integration predicted or encountered a conflict.
    MergeConflict,
    /// Worktree cleanup was refused to preserve unreviewed, dirty, or active state.
    CleanupRefused,
    /// Remote branch head does not match the expected lease SHA.
    RemoteLeaseMismatch,
    /// Remote target branch does not contain the accepted task head commit in its ancestry.
    RemoteAncestryMissing,
    /// Requested auxiliary tool is unavailable or invalid.
    ToolUnavailable,
    /// Underlying supervised Git or auxiliary tool command failed.
    GitCommandFailed,
    /// Standard I/O error.
    Io,
    /// JSON serialization or deserialization error.
    Json,
}

impl OperationsErrorCode {
    /// Returns the static snake_case string representation of this error code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::InvalidInput => "invalid_input",
            Self::SecurityViolation => "security_violation",
            Self::WorktreeNotOwned => "worktree_not_owned",
            Self::ActiveJob => "active_job",
            Self::DirtyWorktree => "dirty_worktree",
            Self::DiffGateRejected => "diff_gate_rejected",
            Self::UncertainWorkerOutcome => "uncertain_worker_outcome",
            Self::SubjectMismatch => "subject_mismatch",
            Self::ReceiptInvalidated => "receipt_invalidated",
            Self::HeadMoved => "head_moved",
            Self::AncestryMismatch => "ancestry_mismatch",
            Self::MergeConflict => "merge_conflict",
            Self::CleanupRefused => "cleanup_refused",
            Self::RemoteLeaseMismatch => "remote_lease_mismatch",
            Self::RemoteAncestryMissing => "remote_ancestry_missing",
            Self::ToolUnavailable => "tool_unavailable",
            Self::GitCommandFailed => "git_command_failed",
            Self::Io => "io",
            Self::Json => "json",
        }
    }
}

/// Safe serializable error envelope for UI/IPC/CLI boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationsApiError {
    /// Machine-readable error code.
    pub code: OperationsErrorCode,
    /// Bounded human-readable error message.
    pub message: String,
    /// Optional cleanup refusal reason when `code == CleanupRefused`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_refusal_reason: Option<CleanupRefusalReason>,
}

/// Errors returned by `threadlane-operations` review, integration, cleanup, push, and capability APIs.
#[derive(Debug, Error)]
pub enum OperationsError {
    /// Operating system does not support required Unix process-group and permission checks.
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),

    /// Input parameter, identifier, SHA, or path is invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Security check failed (path traversal, symlink, or non-private permissions).
    #[error("security violation: {0}")]
    SecurityViolation(String),

    /// Path or worktree is not an owned Threadlane worktree checkout.
    #[error("worktree not owned: {0}")]
    WorktreeNotOwned(String),

    /// A worker job or lock is active on the worktree.
    #[error("active job on worktree `{worktree_id}`: {detail}")]
    ActiveJob {
        /// Software-owned worktree identifier.
        worktree_id: String,
        /// Explanation of why the worktree is considered active.
        detail: String,
    },

    /// Worktree has uncommitted, staged, untracked, or ignored modifications.
    #[error("dirty worktree `{worktree_id}`: {detail}")]
    DirtyWorktree {
        /// Software-owned worktree identifier.
        worktree_id: String,
        /// Explanation of dirty state.
        detail: String,
    },

    /// Post-run diff gate rejected the worktree.
    #[error("diff gate rejected worktree `{worktree_id}`: {detail}")]
    DiffGateRejected {
        /// Software-owned worktree identifier.
        worktree_id: String,
        /// Summary of diff violations or incomplete evidence.
        detail: String,
    },

    /// Worker run outcome is uncertain, failed, or missing policy-required evidence.
    #[error("uncertain or unaccepted worker outcome: {0}")]
    UncertainWorkerOutcome(String),

    /// Review subject fields or digests do not match live worktree state.
    #[error("review subject mismatch: {0}")]
    SubjectMismatch(String),

    /// Review or integration receipt is invalid or was invalidated by head/base/scope/policy/evidence changes.
    #[error("receipt invalidated: {0}")]
    ReceiptInvalidated(String),

    /// Current Git HEAD moved from the expected commit SHA.
    #[error("HEAD moved on `{target}`: expected `{expected}`, found `{actual}`")]
    HeadMoved {
        /// Repository or worktree label.
        target: String,
        /// Expected 40-hex commit SHA.
        expected: String,
        /// Actual observed 40-hex commit SHA.
        actual: String,
    },

    /// Required commit ancestry relationship does not hold.
    #[error("commit ancestry mismatch: {0}")]
    AncestryMismatch(String),

    /// Merge between integration branch and task branch has conflicts.
    #[error("merge conflict: {0}")]
    MergeConflict(String),

    /// Worktree cleanup was refused.
    #[error("cleanup refused for worktree `{worktree_id}` ({reason:?}): {detail}")]
    CleanupRefused {
        /// Software-owned worktree identifier.
        worktree_id: String,
        /// Refusal classification.
        reason: CleanupRefusalReason,
        /// Detailed explanation of why cleanup was refused.
        detail: String,
    },

    /// Remote branch head does not match the caller's lease expectation.
    #[error(
        "remote lease mismatch for `{remote}/{branch}`: expected `{expected}`, found `{actual:?}`"
    )]
    RemoteLeaseMismatch {
        /// Remote name.
        remote: String,
        /// Remote branch name.
        branch: String,
        /// Expected 40-hex commit SHA.
        expected: String,
        /// Actual observed remote commit SHA, if the ref exists.
        actual: Option<String>,
    },

    /// Remote target branch does not contain the accepted task head commit in its ancestry.
    #[error("remote target `{remote}/{target_branch}` does not contain accepted head `{accepted_head}`: {detail}")]
    RemoteAncestryMissing {
        /// Remote name.
        remote: String,
        /// Remote target branch name.
        target_branch: String,
        /// Accepted task head commit SHA.
        accepted_head: String,
        /// Diagnostic detail.
        detail: String,
    },

    /// Auxiliary tool is unavailable or invalid.
    #[error("tool unavailable: {0}")]
    ToolUnavailable(String),

    /// Underlying Git command failed.
    #[error("git command `{command}` failed (status: {status:?}): {stderr}")]
    GitCommandFailed {
        /// Git subcommand description.
        command: String,
        /// Exit code if available.
        status: Option<i32>,
        /// Sanitized bounded stderr output.
        stderr: String,
    },

    /// Standard I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl OperationsError {
    /// Returns the typed [`OperationsErrorCode`] for this error.
    pub const fn error_code(&self) -> OperationsErrorCode {
        match self {
            Self::UnsupportedPlatform(_) => OperationsErrorCode::UnsupportedPlatform,
            Self::InvalidInput(_) => OperationsErrorCode::InvalidInput,
            Self::SecurityViolation(_) => OperationsErrorCode::SecurityViolation,
            Self::WorktreeNotOwned(_) => OperationsErrorCode::WorktreeNotOwned,
            Self::ActiveJob { .. } => OperationsErrorCode::ActiveJob,
            Self::DirtyWorktree { .. } => OperationsErrorCode::DirtyWorktree,
            Self::DiffGateRejected { .. } => OperationsErrorCode::DiffGateRejected,
            Self::UncertainWorkerOutcome(_) => OperationsErrorCode::UncertainWorkerOutcome,
            Self::SubjectMismatch(_) => OperationsErrorCode::SubjectMismatch,
            Self::ReceiptInvalidated(_) => OperationsErrorCode::ReceiptInvalidated,
            Self::HeadMoved { .. } => OperationsErrorCode::HeadMoved,
            Self::AncestryMismatch(_) => OperationsErrorCode::AncestryMismatch,
            Self::MergeConflict(_) => OperationsErrorCode::MergeConflict,
            Self::CleanupRefused { .. } => OperationsErrorCode::CleanupRefused,
            Self::RemoteLeaseMismatch { .. } => OperationsErrorCode::RemoteLeaseMismatch,
            Self::RemoteAncestryMissing { .. } => OperationsErrorCode::RemoteAncestryMissing,
            Self::ToolUnavailable(_) => OperationsErrorCode::ToolUnavailable,
            Self::GitCommandFailed { .. } => OperationsErrorCode::GitCommandFailed,
            Self::Io(_) => OperationsErrorCode::Io,
            Self::Json(_) => OperationsErrorCode::Json,
        }
    }

    /// Returns the static snake_case error code string.
    pub const fn code(&self) -> &'static str {
        self.error_code().as_str()
    }

    /// Converts this error into a serializable [`OperationsApiError`].
    pub fn to_api_error(&self) -> OperationsApiError {
        let cleanup_refusal_reason = match self {
            Self::CleanupRefused { reason, .. } => Some(*reason),
            _ => None,
        };
        OperationsApiError {
            code: self.error_code(),
            message: self.to_string(),
            cleanup_refusal_reason,
        }
    }
}

impl From<WorkerError> for OperationsError {
    fn from(err: WorkerError) -> Self {
        match err {
            WorkerError::UnsupportedPlatform(msg) => Self::UnsupportedPlatform(msg),
            WorkerError::InvalidSpec(msg) => Self::InvalidInput(msg),
            WorkerError::DisallowedModel { model } => {
                Self::InvalidInput(format!("disallowed model `{model}`"))
            }
            WorkerError::DisallowedExecutable { executable } => {
                Self::InvalidInput(format!("disallowed executable `{}`", executable.display()))
            }
            WorkerError::SecurityViolation(msg) => Self::SecurityViolation(msg),
            WorkerError::TargetAlreadyExists(path) => {
                Self::InvalidInput(format!("target already exists: `{}`", path.display()))
            }
            WorkerError::GitCommandFailed {
                command,
                status,
                stderr,
            } => Self::GitCommandFailed {
                command,
                status,
                stderr,
            },
            WorkerError::CleanupRefused {
                worktree_id,
                reason,
                detail,
            } => Self::CleanupRefused {
                worktree_id,
                reason,
                detail,
            },
            WorkerError::ExecutionFailed { status, detail } => Self::UncertainWorkerOutcome(
                format!("worker execution failed ({status}): {detail}"),
            ),
            WorkerError::Io(io_err) => Self::Io(io_err),
            WorkerError::Json(json_err) => Self::Json(json_err),
        }
    }
}
