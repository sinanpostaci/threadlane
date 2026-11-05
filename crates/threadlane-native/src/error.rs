use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable machine-readable error codes for `threadlane-native`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeErrorCode {
    /// Native Codex executable or adapter is not configured.
    NotConfigured,
    /// Optional workflow registry and plan stores are not configured on this MCP server instance.
    WorkflowNotConfigured,
    /// Optional approved-task authority store and host sessions root are not configured on this MCP server instance.
    HostNotConfigured,
    /// Native configuration or caller parameter failed validation.
    InvalidConfig,
    /// Input parameter failed format, length, or scope validation.
    ValidationFailed,
    /// Current operating system does not support required private permission or process-group checks.
    UnsupportedPlatform,
    /// Required wire feature or live state is unsupported without inventing undocumented IPC.
    UnsupportedCapability,
    /// Live two-chat or busy-turn capability still requires authorized host verification.
    NeedsVerification,
    /// Path or file permission check failed (symlink, traversal, or non-private mode).
    SecurityViolation,
    /// Spawned `codex app-server proxy` process timed out.
    TimedOut,
    /// Proxy stdout or stderr frame exceeded maximum bounded byte limits.
    OutputOverflow,
    /// Proxy process or transport failed before a request was written to the wire.
    TransportUnavailable,
    /// Proxy process or transport failed or disconnected after a request was written to the wire.
    AmbiguousTransportFailure,
    /// Proxy returned malformed JSON or an unexpected response envelope.
    ProtocolError,
    /// Proxy returned a structured JSON-RPC error response.
    RpcError,
    /// Requested native thread ID was not found or did not match the returned metadata.
    ThreadNotFound,
    /// MCP tool call returned `isError: true` or failed execution.
    McpToolError,
    /// MCP connection is still unbound and has not been enrolled by the trusted operator.
    UnboundConnection,
    /// Connection nonce was not found in the pairing store.
    UnknownConnectionNonce,
    /// Connection epoch has expired (for example after process restart or replacement).
    ExpiredEpoch,
    /// A single MCP connection was observed across two distinct native threads and was quarantined.
    SharedConnectionRejected,
    /// Attempted to rebind an already-enrolled connection or supply untrusted identity parameters.
    RebindRejected,
    /// Operator authentication failed when accessing the private pairing endpoint.
    UnauthorizedOperator,
    /// Attempted to enroll or operate on a thread whose `projectId` did not match the expected project.
    ProjectMismatch,
    /// Active-turn steer requires a verified live `expectedTurnId`.
    MissingLiveTurnId,
    /// Active-turn steer was rejected by the server (e.g. turn no longer active or ID mismatch).
    TurnSteerRejected,
    /// A prior wakeup submission is in `Unknown` or unacknowledged `Submitted` state and cannot be blindly resent.
    AmbiguousWakeupPendingReconciliation,
    /// Underlying filesystem I/O error.
    IoError,
    /// Atomic replacement succeeded on the filesystem path, but post-rename durability confirmation failed.
    PersistenceUncertain,
}

impl NativeErrorCode {
    /// Returns the static `snake_case` string identifier for this error code.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::WorkflowNotConfigured => "workflow_not_configured",
            Self::HostNotConfigured => "host_not_configured",
            Self::InvalidConfig => "invalid_config",
            Self::ValidationFailed => "validation_failed",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::UnsupportedCapability => "unsupported_capability",
            Self::NeedsVerification => "needs_verification",
            Self::SecurityViolation => "security_violation",
            Self::TimedOut => "timed_out",
            Self::OutputOverflow => "output_overflow",
            Self::TransportUnavailable => "transport_unavailable",
            Self::AmbiguousTransportFailure => "ambiguous_transport_failure",
            Self::ProtocolError => "protocol_error",
            Self::RpcError => "rpc_error",
            Self::ThreadNotFound => "thread_not_found",
            Self::McpToolError => "mcp_tool_error",
            Self::UnboundConnection => "unbound_connection",
            Self::UnknownConnectionNonce => "unknown_connection_nonce",
            Self::ExpiredEpoch => "expired_epoch",
            Self::SharedConnectionRejected => "shared_connection_rejected",
            Self::RebindRejected => "rebind_rejected",
            Self::UnauthorizedOperator => "unauthorized_operator",
            Self::ProjectMismatch => "project_mismatch",
            Self::MissingLiveTurnId => "missing_live_turn_id",
            Self::TurnSteerRejected => "turn_steer_rejected",
            Self::AmbiguousWakeupPendingReconciliation => "ambiguous_wakeup_pending_reconciliation",
            Self::IoError => "io_error",
            Self::PersistenceUncertain => "persistence_uncertain",
        }
    }
}

/// Safe, redacted error type for `threadlane-native`.
///
/// Never embeds raw chat transcripts, user prompts, or secret tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{message}")]
pub struct NativeError {
    /// Machine-readable error code.
    pub code: NativeErrorCode,
    /// Safe human-readable diagnostic message.
    pub message: String,
    /// Optional JSON-RPC numeric error code when returned by `codex app-server proxy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_code: Option<i64>,
}

impl NativeError {
    /// Constructs a new [`NativeError`] with `code` and a safe message.
    pub fn new(code: NativeErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            rpc_code: None,
        }
    }

    /// Constructs a new [`NativeError`] when a replacement became visible on disk but durability could not be confirmed.
    pub fn persistence_uncertain(message: impl Into<String>) -> Self {
        Self::new(NativeErrorCode::PersistenceUncertain, message)
    }

    /// Constructs a new [`NativeError`] representing a JSON-RPC error response.
    pub fn rpc(rpc_code: i64, message: impl Into<String>) -> Self {
        Self {
            code: NativeErrorCode::RpcError,
            message: message.into(),
            rpc_code: Some(rpc_code),
        }
    }

    /// Returns the static `snake_case` error code string (matching desktop IPC error conventions).
    pub const fn code(&self) -> &'static str {
        self.code.as_str()
    }

    /// Returns the typed [`NativeErrorCode`].
    pub const fn error_code(&self) -> NativeErrorCode {
        self.code
    }

    /// Returns the safe diagnostic message string.
    pub fn message(&self) -> &str {
        &self.message
    }
}
