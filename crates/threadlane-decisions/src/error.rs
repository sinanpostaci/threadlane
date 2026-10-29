use thiserror::Error;

/// Safe, redacted error returned by [`crate::DecisionClient`] operations.
///
/// `Display` and `Debug` representations never expose API keys, bearer tokens,
/// raw HTTP headers, or unredacted provider error payloads.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecisionError {
    /// API credential for the requested tool is not configured in the vault.
    #[error("API credential is not configured in the system vault.")]
    NotConfigured,

    /// System credential vault is locked or unavailable.
    #[error("System credential vault is unavailable or locked.")]
    VaultUnavailable,

    /// Caller supplied an invalid tool ID, model identifier, or task summary.
    #[error("Decision request parameters are invalid.")]
    InvalidRequest,

    /// Endpoint URL failed fixed HTTPS origin or redirect validation.
    #[error("Decision endpoint must use a fixed HTTPS origin without redirects.")]
    InvalidEndpoint,

    /// Underlying HTTPS client could not be initialized.
    #[error("Failed to initialize HTTPS decision client.")]
    ClientInitFailed,

    /// Provider rejected the API credential (HTTP 401 or 403).
    #[error("Provider rejected the API credential.")]
    Unauthorized,

    /// Provider rate limit or capacity limit was reached (HTTP 429 or 529).
    #[error("Provider rate limit or capacity limit was reached.")]
    RateLimited,

    /// Provider rejected the request payload or model (HTTP 400, 404, or 422).
    #[error("Provider rejected the decision request.")]
    ProviderRejected,

    /// Provider encountered an upstream server failure (HTTP 5xx).
    #[error("Decision provider is temporarily unavailable.")]
    ProviderUnavailable,

    /// Provider HTTP request timed out.
    #[error("Decision provider request timed out.")]
    Timeout,

    /// Network transport failed before a complete response was received.
    #[error("Decision provider transport request failed.")]
    TransportFailed,

    /// Provider response exceeded the bounded payload size limit.
    #[error("Decision provider response exceeded size limits.")]
    ResponseTooLarge,

    /// Provider returned malformed JSON or violated the verified decision schema.
    #[error("Decision provider returned an invalid or unverified response schema.")]
    InvalidResponse,
}

impl DecisionError {
    /// Stable snake_case error code suitable for IPC and structured diagnostics.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::VaultUnavailable => "vault_unavailable",
            Self::InvalidRequest => "invalid_request",
            Self::InvalidEndpoint => "invalid_endpoint",
            Self::ClientInitFailed => "client_init_failed",
            Self::Unauthorized => "unauthorized",
            Self::RateLimited => "rate_limited",
            Self::ProviderRejected => "provider_rejected",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::Timeout => "timeout",
            Self::TransportFailed => "transport_failed",
            Self::ResponseTooLarge => "response_too_large",
            Self::InvalidResponse => "invalid_response",
        }
    }

    /// Static, user-safe message describing the error without leaking secrets or raw payloads.
    pub const fn message(&self) -> &'static str {
        match self {
            Self::NotConfigured => "API credential is not configured in the system vault.",
            Self::VaultUnavailable => "System credential vault is unavailable or locked.",
            Self::InvalidRequest => "Decision request parameters are invalid.",
            Self::InvalidEndpoint => {
                "Decision endpoint must use a fixed HTTPS origin without redirects."
            }
            Self::ClientInitFailed => "Failed to initialize HTTPS decision client.",
            Self::Unauthorized => "Provider rejected the API credential.",
            Self::RateLimited => "Provider rate limit or capacity limit was reached.",
            Self::ProviderRejected => "Provider rejected the decision request.",
            Self::ProviderUnavailable => "Decision provider is temporarily unavailable.",
            Self::Timeout => "Decision provider request timed out.",
            Self::TransportFailed => "Decision provider transport request failed.",
            Self::ResponseTooLarge => "Decision provider response exceeded size limits.",
            Self::InvalidResponse => {
                "Decision provider returned an invalid or unverified response schema."
            }
        }
    }
}
