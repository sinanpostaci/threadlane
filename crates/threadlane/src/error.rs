use std::path::PathBuf;

use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use threadlane_core::CoreError;

/// Stable machine-readable error code for domain, protocol, and runtime errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Input failed length, count, or format validation.
    ValidationFailed,
    /// Database schema version does not match the engine version.
    IncompatibleSchemaVersion,
    /// The specified `binding_id` does not exist.
    BindingNotFound,
    /// The specified `run_id` does not exist within the configured binding scope.
    RunNotFound,
    /// The specified `question_id` does not exist for `(binding_id, run_id)`.
    QuestionNotFound,
    /// `expected_revision` did not match the run's current revision.
    StaleRevision,
    /// Question was already superseded, answered, cancelled, or belongs to a prior revision.
    StaleQuestion,
    /// Operation is invalid for the run's current lifecycle status.
    InvalidRunState,
    /// An idempotent `command_key` was reused on the same binding with different parameters or operation.
    IdempotencyConflict,
    /// Event batch acknowledgement violated boundary or isolation invariants.
    InvalidAck,
    /// Caller attempted to pull events starting after an unseen sequence number.
    UnseenEventCursor,
    /// Active wake-up claim was not found.
    WakeupClaimNotFound,
    /// Wake-up state transition is invalid for the current status.
    InvalidWakeupState,
    /// Serialized event payload exceeded the maximum bounded size.
    EventPayloadTooLarge,
    /// Underlying SQLite storage error.
    StorageError,
    /// JSON serialization or deserialization failure.
    SerializationError,
    /// Stored database value violated an internal invariant.
    CorruptState,
    /// Missing or invalid bearer token on local supervisor HTTP request.
    Unauthorized,
    /// Malformed request or unknown fields in request payload.
    InvalidRequest,
    /// Request or response body exceeded bounded size limits.
    PayloadTooLarge,
    /// Another supervisor process already holds the state directory lock.
    LockBusy,
    /// State directory, descriptor, database, or lock path violated symlink or permission rules.
    SecurityViolation,
    /// Runtime configuration (such as Codex wake-up flags) is invalid.
    InvalidConfig,
    /// Current operating system does not yet have verified private descriptor ACL enforcement.
    UnsupportedPlatform,
    /// Local IPC transport or connection error.
    TransportError,
    /// Unexpected internal runtime error.
    InternalError,
}

/// Structured wire error returned by the supervisor HTTP API and MCP tool error responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    /// Stable machine-readable error code.
    pub code: ErrorCode,
    /// Human-readable error message (redacted; never contains bearer tokens).
    pub message: String,
    /// Optional structured domain context (such as expected vs actual revision).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ApiError {
    /// Creates a new [`ApiError`] without extra structured details.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    /// Creates a new [`ApiError`] with structured JSON details.
    pub fn with_details(code: ErrorCode, message: impl Into<String>, details: Value) -> Self {
        Self {
            code,
            message: message.into(),
            details: Some(details),
        }
    }

    /// Maps a [`CoreError`] to an [`ApiError`] and HTTP [`StatusCode`].
    pub fn from_core_error(err: &CoreError) -> (StatusCode, Self) {
        let message = err.to_string();
        match err {
            CoreError::Validation { field, reason } => (
                StatusCode::BAD_REQUEST,
                Self::with_details(
                    ErrorCode::ValidationFailed,
                    message,
                    json!({ "field": field, "reason": reason }),
                ),
            ),
            CoreError::IncompatibleSchemaVersion { found, expected } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Self::with_details(
                    ErrorCode::IncompatibleSchemaVersion,
                    message,
                    json!({ "found": found, "expected": expected }),
                ),
            ),
            CoreError::BindingNotFound { binding_id } => (
                StatusCode::NOT_FOUND,
                Self::with_details(
                    ErrorCode::BindingNotFound,
                    message,
                    json!({ "binding_id": binding_id }),
                ),
            ),
            CoreError::RunNotFound { binding_id, run_id } => (
                StatusCode::NOT_FOUND,
                Self::with_details(
                    ErrorCode::RunNotFound,
                    message,
                    json!({ "binding_id": binding_id, "run_id": run_id }),
                ),
            ),
            CoreError::QuestionNotFound {
                binding_id,
                run_id,
                question_id,
            } => (
                StatusCode::NOT_FOUND,
                Self::with_details(
                    ErrorCode::QuestionNotFound,
                    message,
                    json!({
                        "binding_id": binding_id,
                        "run_id": run_id,
                        "question_id": question_id,
                    }),
                ),
            ),
            CoreError::StaleRevision {
                run_id,
                expected_revision,
                actual_revision,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::StaleRevision,
                    message,
                    json!({
                        "run_id": run_id,
                        "expected_revision": expected_revision,
                        "actual_revision": actual_revision,
                    }),
                ),
            ),
            CoreError::StaleQuestion {
                run_id,
                question_id,
                question_status,
                question_revision,
                current_revision,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::StaleQuestion,
                    message,
                    json!({
                        "run_id": run_id,
                        "question_id": question_id,
                        "question_status": question_status,
                        "question_revision": question_revision,
                        "current_revision": current_revision,
                    }),
                ),
            ),
            CoreError::InvalidRunState {
                run_id,
                status,
                action,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::InvalidRunState,
                    message,
                    json!({
                        "run_id": run_id,
                        "status": status,
                        "action": action,
                    }),
                ),
            ),
            CoreError::IdempotencyConflict {
                binding_id,
                command_key,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::IdempotencyConflict,
                    message,
                    json!({
                        "binding_id": binding_id,
                        "command_key": command_key,
                    }),
                ),
            ),
            CoreError::InvalidAck {
                binding_id,
                batch_id,
                reason,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::InvalidAck,
                    message,
                    json!({
                        "binding_id": binding_id,
                        "batch_id": batch_id,
                        "reason": reason,
                    }),
                ),
            ),
            CoreError::UnseenEventCursor {
                binding_id,
                requested_after_seq,
                delivered_seq,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::UnseenEventCursor,
                    message,
                    json!({
                        "binding_id": binding_id,
                        "requested_after_seq": requested_after_seq,
                        "delivered_seq": delivered_seq,
                    }),
                ),
            ),
            CoreError::WakeupClaimNotFound { claim_id } => (
                StatusCode::NOT_FOUND,
                Self::with_details(
                    ErrorCode::WakeupClaimNotFound,
                    message,
                    json!({ "claim_id": claim_id }),
                ),
            ),
            CoreError::InvalidWakeupState {
                binding_id,
                status,
                action,
            } => (
                StatusCode::CONFLICT,
                Self::with_details(
                    ErrorCode::InvalidWakeupState,
                    message,
                    json!({
                        "binding_id": binding_id,
                        "status": status,
                        "action": action,
                    }),
                ),
            ),
            CoreError::EventPayloadTooLarge {
                size_bytes,
                max_bytes,
            } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Self::with_details(
                    ErrorCode::EventPayloadTooLarge,
                    message,
                    json!({ "size_bytes": size_bytes, "max_bytes": max_bytes }),
                ),
            ),
            CoreError::ResponseTooLarge {
                size_bytes,
                max_bytes,
            } => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Self::with_details(
                    ErrorCode::PayloadTooLarge,
                    message,
                    json!({ "size_bytes": size_bytes, "max_bytes": max_bytes }),
                ),
            ),
            CoreError::Storage(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Self::new(ErrorCode::StorageError, message),
            ),
            CoreError::Serialization(_) => (
                StatusCode::BAD_REQUEST,
                Self::new(ErrorCode::SerializationError, message),
            ),
            CoreError::CorruptState(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Self::new(ErrorCode::CorruptState, message),
            ),
        }
    }
}

/// Runtime error type for `threadlane` supervisor, client, CLI, and MCP operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Domain/storage error from `threadlane-core`.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// Structured error returned by the supervisor over local IPC.
    #[error("[{code:?}] {message}", code = .0.code, message = .0.message)]
    Remote(ApiError),

    /// Another process holds the exclusive state directory lock.
    #[error("state directory `{state_dir}` is already locked by another supervisor process")]
    LockBusy {
        /// Contended state directory path.
        state_dir: PathBuf,
    },

    /// Symlink, permission, or descriptor validation violation.
    #[error("security check failed: {0}")]
    SecurityViolation(String),

    /// Invalid CLI or supervisor configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Platform does not support required private file/directory ACL checks.
    #[error("unsupported platform: {0}")]
    UnsupportedPlatform(String),

    /// Local HTTP/IPC transport error (redacted to avoid exposing headers or secrets).
    #[error("local IPC transport error: {0}")]
    Transport(String),

    /// Filesystem I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    /// Converts this runtime [`Error`] into a structured [`ApiError`].
    pub fn to_api_error(&self) -> ApiError {
        match self {
            Self::Core(core_err) => ApiError::from_core_error(core_err).1,
            Self::Remote(api_err) => api_err.clone(),
            Self::LockBusy { state_dir } => ApiError::with_details(
                ErrorCode::LockBusy,
                self.to_string(),
                json!({ "state_dir": state_dir }),
            ),
            Self::SecurityViolation(msg) => ApiError::new(ErrorCode::SecurityViolation, msg),
            Self::InvalidConfig(msg) => ApiError::new(ErrorCode::InvalidConfig, msg),
            Self::UnsupportedPlatform(msg) => ApiError::new(ErrorCode::UnsupportedPlatform, msg),
            Self::Transport(msg) => ApiError::new(ErrorCode::TransportError, msg),
            Self::Io(err) => ApiError::new(ErrorCode::InternalError, err.to_string()),
            Self::Json(err) => ApiError::new(ErrorCode::SerializationError, err.to_string()),
        }
    }
}
