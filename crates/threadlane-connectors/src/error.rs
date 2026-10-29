use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Safe, structured error returned by tool probing and credential vault operations.
///
/// All error messages are static English descriptions and never contain secrets,
/// bearer tokens, raw CLI stdout/stderr, or raw OS keychain diagnostic payloads.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConnectorError {
    /// Caller supplied an empty, oversized, or malformed tool or secret input.
    #[error("{0}")]
    InvalidInput(&'static str),

    /// Configured or resolved CLI executable path failed safety or permission checks.
    #[error("{0}")]
    InvalidExecutable(&'static str),

    /// Current operating system does not support required process-group supervision.
    #[error("{0}")]
    UnsupportedPlatform(&'static str),

    /// CLI metadata probe subprocess exceeded the bounded execution timeout.
    #[error("{0}")]
    ProbeTimedOut(&'static str),

    /// CLI metadata probe subprocess exceeded the 1 MiB stdout or stderr stream cap.
    #[error("{0}")]
    ProbeOutputOverflow(&'static str),

    /// CLI metadata probe subprocess failed to spawn, exited non-zero, or left incomplete cleanup.
    #[error("{0}")]
    ProbeFailed(&'static str),

    /// CLI catalog output did not match the expected deterministic schema.
    #[error("{0}")]
    ParseError(&'static str),

    /// OS credential store is currently locked or denied storage access.
    #[error("{0}")]
    CredentialStoreLocked(&'static str),

    /// OS credential store is unavailable on the current host or session.
    #[error("{0}")]
    CredentialStoreUnavailable(&'static str),

    /// OS credential store rejected the operation.
    #[error("{0}")]
    CredentialStoreError(&'static str),
}

impl ConnectorError {
    /// Returns the stable `snake_case` error code for wire envelopes and UI handling.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::InvalidExecutable(_) => "invalid_executable",
            Self::UnsupportedPlatform(_) => "unsupported_platform",
            Self::ProbeTimedOut(_) => "probe_timed_out",
            Self::ProbeOutputOverflow(_) => "probe_output_overflow",
            Self::ProbeFailed(_) => "probe_failed",
            Self::ParseError(_) => "parse_error",
            Self::CredentialStoreLocked(_) => "credential_store_locked",
            Self::CredentialStoreUnavailable(_) => "credential_store_unavailable",
            Self::CredentialStoreError(_) => "credential_store_error",
        }
    }

    /// Returns the safe static user-facing error message.
    pub const fn message(&self) -> &'static str {
        match self {
            Self::InvalidInput(msg)
            | Self::InvalidExecutable(msg)
            | Self::UnsupportedPlatform(msg)
            | Self::ProbeTimedOut(msg)
            | Self::ProbeOutputOverflow(msg)
            | Self::ProbeFailed(msg)
            | Self::ParseError(msg)
            | Self::CredentialStoreLocked(msg)
            | Self::CredentialStoreUnavailable(msg)
            | Self::CredentialStoreError(msg) => msg,
        }
    }
}

impl Serialize for ConnectorError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ConnectorError", 2)?;
        state.serialize_field("code", self.code())?;
        state.serialize_field("message", self.message())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ConnectorError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireError {
            code: String,
        }

        let wire = WireError::deserialize(deserializer)?;
        Ok(match wire.code.as_str() {
            "invalid_input" => Self::InvalidInput("Invalid connector input."),
            "invalid_executable" => Self::InvalidExecutable("Invalid tool executable path."),
            "unsupported_platform" => {
                Self::UnsupportedPlatform("Platform is not supported for CLI process supervision.")
            }
            "probe_timed_out" => Self::ProbeTimedOut("Tool probe timed out."),
            "probe_output_overflow" => {
                Self::ProbeOutputOverflow("Tool probe output exceeded the 1 MiB limit.")
            }
            "probe_failed" => Self::ProbeFailed("Tool probe command failed."),
            "parse_error" => Self::ParseError("Failed to parse tool catalog output."),
            "credential_store_locked" => {
                Self::CredentialStoreLocked("OS credential store is locked.")
            }
            "credential_store_unavailable" => {
                Self::CredentialStoreUnavailable("OS credential store is unavailable.")
            }
            _ => Self::CredentialStoreError("Credential store operation failed."),
        })
    }
}
