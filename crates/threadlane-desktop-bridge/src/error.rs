use thiserror::Error;
use threadlane::ErrorCode;
use threadlane_core::CoreError;

use crate::types::ConnectionState;

/// Safe, user-facing error returned by [`crate::DesktopBridge`] operations.
///
/// `Display` messages are short static English strings and never expose bearer tokens,
/// runtime descriptors, filesystem details, or raw internal errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BridgeError {
    /// No runtime state directory is currently attached.
    #[error("No runtime state directory is attached.")]
    Unconfigured,

    /// Local demo runtime directory was not configured at startup.
    #[error("Local demo runtime is not configured.")]
    DemoUnavailable,

    /// Caller supplied an empty or malformed identifier or path.
    #[error("{0}")]
    InvalidInput(&'static str),

    /// Requested demo action is invalid for the current demo run state.
    #[error("{0}")]
    InvalidState(&'static str),

    /// Expected revision did not match the current run revision.
    #[error("Task revision changed; refresh and try again.")]
    StaleRevision,

    /// Requested binding or task was not found in the active scope.
    #[error("{0}")]
    NotFound(&'static str),

    /// State directory, descriptor, or authentication failed security checks.
    #[error("{0}")]
    SecurityBlocked(&'static str),

    /// Local runtime supervisor is offline or unreachable.
    #[error("Local runtime supervisor is offline or unreachable.")]
    Offline,

    /// Supervisor returned an unexpected protocol or internal error.
    #[error("{0}")]
    Protocol(&'static str),
}

impl BridgeError {
    /// Returns `true` if `err` is specifically a `WorkerNotFound` error from `threadlane-core`
    /// or the remote supervisor API.
    pub fn is_worker_not_found(err: &threadlane::Error) -> bool {
        matches!(
            err,
            threadlane::Error::Remote(api_err) if api_err.code == ErrorCode::WorkerNotFound
        ) || matches!(
            err,
            threadlane::Error::Core(CoreError::WorkerNotFound { .. })
        )
    }

    /// Returns `true` if `err` is a `BindingNotFound` error.
    pub fn is_binding_not_found(err: &threadlane::Error) -> bool {
        matches!(
            err,
            threadlane::Error::Remote(api_err) if api_err.code == ErrorCode::BindingNotFound
        ) || matches!(
            err,
            threadlane::Error::Core(CoreError::BindingNotFound { .. })
        )
    }

    /// Maps a [`threadlane::Error`] to a safe [`BridgeError`].
    pub fn from_threadlane_error(err: &threadlane::Error) -> Self {
        match err {
            threadlane::Error::SecurityViolation(_) => Self::SecurityBlocked(
                "Runtime state directory or descriptor failed security validation.",
            ),
            threadlane::Error::UnsupportedPlatform(_) => Self::SecurityBlocked(
                "Current platform does not support required private runtime permissions.",
            ),
            threadlane::Error::InvalidConfig(_) => {
                Self::InvalidInput("Invalid runtime state directory configuration.")
            }
            threadlane::Error::Transport(_) | threadlane::Error::LockBusy { .. } => Self::Offline,
            threadlane::Error::Io(io_err) => {
                if io_err.kind() == std::io::ErrorKind::PermissionDenied {
                    Self::SecurityBlocked(
                        "Permission denied while accessing runtime state directory.",
                    )
                } else {
                    Self::Offline
                }
            }
            threadlane::Error::Json(_) => {
                Self::Protocol("Runtime supervisor returned a malformed response.")
            }
            threadlane::Error::Core(core_err) => Self::from_core_error(core_err),
            threadlane::Error::Remote(api_err) => Self::from_error_code(api_err.code),
        }
    }

    /// Maps a [`threadlane::Error`] encountered during `snapshot` polling to a
    /// [`ConnectionState`] and short safe diagnostic message.
    pub fn classify_snapshot_error(err: &threadlane::Error) -> (ConnectionState, &'static str) {
        let bridge_err = Self::from_threadlane_error(err);
        Self::classify_bridge_error_for_snapshot(&bridge_err)
    }

    /// Maps a [`BridgeError`] to a [`ConnectionState`] and short safe diagnostic message.
    pub fn classify_bridge_error_for_snapshot(
        err: &BridgeError,
    ) -> (ConnectionState, &'static str) {
        match err {
            Self::Unconfigured => (
                ConnectionState::Unconfigured,
                "No runtime state directory attached.",
            ),
            Self::Offline => (
                ConnectionState::Disconnected,
                "Local runtime supervisor is offline or unreachable.",
            ),
            Self::SecurityBlocked(msg) => (ConnectionState::Blocked, msg),
            Self::InvalidInput(msg) => (ConnectionState::Blocked, msg),
            Self::Protocol(msg) => (ConnectionState::Blocked, msg),
            Self::NotFound(msg) => (ConnectionState::Disconnected, msg),
            Self::InvalidState(msg) => (ConnectionState::Disconnected, msg),
            Self::StaleRevision => (
                ConnectionState::Disconnected,
                "Task revision changed during snapshot.",
            ),
            Self::DemoUnavailable => (
                ConnectionState::Unconfigured,
                "Local demo runtime is not configured.",
            ),
        }
    }

    fn from_core_error(err: &CoreError) -> Self {
        match err {
            CoreError::Validation { .. } => {
                Self::InvalidInput("Request failed supervisor input validation.")
            }
            CoreError::IncompatibleSchemaVersion { .. } | CoreError::CorruptState(_) => {
                Self::SecurityBlocked("Runtime database schema or state is incompatible.")
            }
            CoreError::BindingNotFound { .. } => {
                Self::NotFound("Selected chat binding was not found.")
            }
            CoreError::RunNotFound { .. } | CoreError::WorkerNotFound { .. } => {
                Self::NotFound("Requested task was not found in this chat.")
            }
            CoreError::QuestionNotFound { .. } | CoreError::StaleQuestion { .. } => {
                Self::InvalidState("Local demo question is no longer pending.")
            }
            CoreError::StaleRevision { .. } | CoreError::StaleWorkerClaim { .. } => {
                Self::StaleRevision
            }
            CoreError::InvalidRunState { .. } => {
                Self::InvalidState("Action is not valid for the current task state.")
            }
            CoreError::IdempotencyConflict { .. }
            | CoreError::InvalidAck { .. }
            | CoreError::UnseenEventCursor { .. }
            | CoreError::WakeupClaimNotFound { .. }
            | CoreError::InvalidWakeupState { .. }
            | CoreError::EventPayloadTooLarge { .. }
            | CoreError::ResponseTooLarge { .. }
            | CoreError::Serialization(_) => {
                Self::Protocol("Runtime supervisor rejected the request payload.")
            }
            CoreError::Storage(_) => Self::Offline,
        }
    }

    fn from_error_code(code: ErrorCode) -> Self {
        match code {
            ErrorCode::ValidationFailed | ErrorCode::InvalidRequest | ErrorCode::InvalidConfig => {
                Self::InvalidInput("Request failed supervisor validation.")
            }
            ErrorCode::IncompatibleSchemaVersion | ErrorCode::CorruptState => {
                Self::SecurityBlocked("Runtime database schema or state is incompatible.")
            }
            ErrorCode::Unauthorized => {
                Self::SecurityBlocked("Runtime supervisor rejected authentication.")
            }
            ErrorCode::SecurityViolation => Self::SecurityBlocked(
                "Runtime state directory or descriptor failed security validation.",
            ),
            ErrorCode::UnsupportedPlatform => Self::SecurityBlocked(
                "Current platform does not support required private runtime permissions.",
            ),
            ErrorCode::BindingNotFound => Self::NotFound("Selected chat binding was not found."),
            ErrorCode::RunNotFound | ErrorCode::WorkerNotFound => {
                Self::NotFound("Requested task was not found in this chat.")
            }
            ErrorCode::QuestionNotFound | ErrorCode::StaleQuestion => {
                Self::InvalidState("Local demo question is no longer pending.")
            }
            ErrorCode::StaleRevision | ErrorCode::StaleWorkerClaim => Self::StaleRevision,
            ErrorCode::InvalidRunState => {
                Self::InvalidState("Action is not valid for the current task state.")
            }
            ErrorCode::PayloadTooLarge
            | ErrorCode::EventPayloadTooLarge
            | ErrorCode::SerializationError => {
                Self::Protocol("Runtime supervisor rejected response or payload format.")
            }
            ErrorCode::LockBusy | ErrorCode::TransportError | ErrorCode::StorageError => {
                Self::Offline
            }
            ErrorCode::InternalError
            | ErrorCode::IdempotencyConflict
            | ErrorCode::InvalidAck
            | ErrorCode::UnseenEventCursor
            | ErrorCode::WakeupClaimNotFound
            | ErrorCode::InvalidWakeupState => {
                Self::Protocol("Runtime supervisor reported an internal protocol error.")
            }
        }
    }
}
