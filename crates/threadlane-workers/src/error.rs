use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Machine-readable reason why explicit worktree cleanup was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupRefusalReason {
    /// A worker job is currently active (or its crash lock/marker has not been reconciled).
    ActiveJob,
    /// The worktree contains modified, staged, untracked, or unmerged commit changes.
    DirtyWorktree,
    /// The worktree HEAD branch or repository owner does not match the private manifest.
    UnexpectedBranchOrOwner,
    /// The private ownership manifest outside the checkout is missing, unreadable, or invalid.
    MissingOrInvalidManifest,
}

/// Stable machine-readable error code for diagnostic CLI and library callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerErrorCode {
    /// Platform does not yet implement verified process-group supervision and permission semantics.
    UnsupportedPlatform,
    /// Requested provider or permission mode does not support the required capability.
    UnsupportedCapability,
    /// Worker specification or argument validation failed before launch.
    InvalidSpec,
    /// Requested model is not permitted by the trusted model allowlist.
    DisallowedModel,
    /// Requested executable is not permitted by the trusted executable allowlist.
    DisallowedExecutable,
    /// Path traversal, symlink component, or permission invariant violation.
    SecurityViolation,
    /// Target worktree directory, manifest, or branch already exists.
    TargetAlreadyExists,
    /// Underlying Git command failed.
    GitCommandFailed,
    /// Explicit worktree cleanup was refused to preserve active or unreviewed state.
    CleanupRefused,
    /// Worker execution or post-run diff gate did not pass.
    ExecutionFailed,
    /// Filesystem or OS I/O error.
    Io,
    /// JSON serialization or deserialization error.
    Json,
}

impl WorkerErrorCode {
    /// Returns the canonical `snake_case` machine code string for this error category.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::UnsupportedCapability => "unsupported_capability",
            Self::InvalidSpec => "invalid_spec",
            Self::DisallowedModel => "disallowed_model",
            Self::DisallowedExecutable => "disallowed_executable",
            Self::SecurityViolation => "security_violation",
            Self::TargetAlreadyExists => "target_already_exists",
            Self::GitCommandFailed => "git_command_failed",
            Self::CleanupRefused => "cleanup_refused",
            Self::ExecutionFailed => "execution_failed",
            Self::Io => "io",
            Self::Json => "json",
        }
    }
}

impl std::fmt::Display for WorkerErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured JSON error envelope emitted by the diagnostic CLI on failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerApiError {
    /// Machine-readable error category.
    pub code: WorkerErrorCode,
    /// Bounded human-readable error message.
    pub message: String,
    /// Optional structured refusal reason when `code == CleanupRefused`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_refusal_reason: Option<CleanupRefusalReason>,
}

/// Errors returned by `threadlane-workers` validation, supervision, Git isolation, and cleanup APIs.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Current operating system is not supported for verified process-group lifecycle supervision.
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),

    /// Requested provider or permission mode does not support the required capability.
    #[error("unsupported capability `{capability}` for provider `{provider}`: {reason}")]
    UnsupportedCapability {
        /// Provider identifier (`antigravity`, `opencode_v2`, etc.).
        provider: String,
        /// Requested capability identifier.
        capability: String,
        /// Detailed reason why the capability is unsupported or refused.
        reason: String,
    },

    /// Worker spec, budget, identifier, or path configuration is invalid.
    #[error("invalid worker spec: {0}")]
    InvalidSpec(String),

    /// Requested model is not present in the trusted allowed-models set.
    #[error("requested model `{model}` is not in the trusted allowed-models set")]
    DisallowedModel {
        /// The rejected model identifier.
        model: String,
    },

    /// Requested executable is not present in the trusted allowed-executables set.
    #[error("requested executable `{executable}` is not in the trusted allowed-executables set")]
    DisallowedExecutable {
        /// The rejected executable path.
        executable: PathBuf,
    },

    /// Security check failed (path traversal, symlink component, or insecure file mode).
    #[error("security violation: {0}")]
    SecurityViolation(String),

    /// Pre-existing worktree checkout directory, manifest, or branch was detected.
    #[error("target already exists and will not be overwritten or reset: `{0}`")]
    TargetAlreadyExists(PathBuf),

    /// A Git command failed or returned unexpected state.
    #[error("git command `{command}` failed (status: {status:?}): {stderr}")]
    GitCommandFailed {
        /// Description of the git subcommand.
        command: String,
        /// Process exit code if available.
        status: Option<i32>,
        /// Sanitized bounded stderr/diagnostic output.
        stderr: String,
    },

    /// Explicit worktree cleanup was refused to protect active or unreviewed work.
    #[error("cleanup refused for worktree `{worktree_id}` ({reason:?}): {detail}")]
    CleanupRefused {
        /// Software-owned worktree identifier.
        worktree_id: String,
        /// Refusal classification.
        reason: CleanupRefusalReason,
        /// Detailed explanation of why cleanup was refused.
        detail: String,
    },

    /// Worker execution or diff-gate validation did not succeed.
    #[error("worker execution did not pass ({status}): {detail}")]
    ExecutionFailed {
        /// Terminal status name.
        status: String,
        /// Diagnostic detail.
        detail: String,
    },

    /// Standard I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl WorkerError {
    /// Returns the stable machine-readable [`WorkerErrorCode`] for this error.
    pub fn code(&self) -> WorkerErrorCode {
        match self {
            Self::UnsupportedPlatform(_) => WorkerErrorCode::UnsupportedPlatform,
            Self::UnsupportedCapability { .. } => WorkerErrorCode::UnsupportedCapability,
            Self::InvalidSpec(_) => WorkerErrorCode::InvalidSpec,
            Self::DisallowedModel { .. } => WorkerErrorCode::DisallowedModel,
            Self::DisallowedExecutable { .. } => WorkerErrorCode::DisallowedExecutable,
            Self::SecurityViolation(_) => WorkerErrorCode::SecurityViolation,
            Self::TargetAlreadyExists(_) => WorkerErrorCode::TargetAlreadyExists,
            Self::GitCommandFailed { .. } => WorkerErrorCode::GitCommandFailed,
            Self::CleanupRefused { .. } => WorkerErrorCode::CleanupRefused,
            Self::ExecutionFailed { .. } => WorkerErrorCode::ExecutionFailed,
            Self::Io(_) => WorkerErrorCode::Io,
            Self::Json(_) => WorkerErrorCode::Json,
        }
    }

    /// Converts this error into a serializable [`WorkerApiError`].
    pub fn to_api_error(&self) -> WorkerApiError {
        let cleanup_refusal_reason = match self {
            Self::CleanupRefused { reason, .. } => Some(*reason),
            Self::UnsupportedPlatform(_)
            | Self::UnsupportedCapability { .. }
            | Self::InvalidSpec(_)
            | Self::DisallowedModel { .. }
            | Self::DisallowedExecutable { .. }
            | Self::SecurityViolation(_)
            | Self::TargetAlreadyExists(_)
            | Self::GitCommandFailed { .. }
            | Self::ExecutionFailed { .. }
            | Self::Io(_)
            | Self::Json(_) => None,
        };
        WorkerApiError {
            code: self.code(),
            message: self.to_string(),
            cleanup_refusal_reason,
        }
    }
}
