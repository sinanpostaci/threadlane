use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::DecisionError;
use crate::types::DecisionProvider;

/// Fixed HTTPS endpoint for Jev model catalog discovery.
pub const JEV_MODELS_URL: &str = "https://api.typesafe.ai/v1/models";

/// Fixed HTTPS endpoint for Jev System One classification.
pub const JEV_CLASSIFY_URL: &str = "https://api.typesafe.ai/v1/systemone";

/// Fixed HTTPS endpoint for OpenAI model catalog discovery.
pub const OPENAI_MODELS_URL: &str = "https://api.openai.com/v1/models";

/// Fixed HTTPS endpoint for OpenAI Decisions classification.
pub const OPENAI_CLASSIFY_URL: &str = "https://api.openai.com/v1/decisions";

/// Connection establishment timeout in seconds.
pub const CONNECT_TIMEOUT_SECS: u64 = 5;

/// Total HTTP request timeout in seconds.
pub const REQUEST_TIMEOUT_SECS: u64 = 15;

/// Maximum permitted HTTP response body size in bytes (256 KiB).
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// HTTP method used for a decision provider request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMethod {
    /// HTTP `GET` request (catalog metadata).
    Get,
    /// HTTP `POST` request (decision classification).
    Post,
}

/// Outbound HTTP request dispatched through a [`DecisionTransport`].
///
/// The `Debug` implementation redacts `bearer_token` so secrets never appear in logs or panic output.
#[derive(Clone, PartialEq, Eq)]
pub struct TransportRequest {
    /// Target decision provider.
    pub provider: DecisionProvider,
    /// HTTP method (`GET` or `POST`).
    pub method: TransportMethod,
    /// Full target URL.
    pub url: String,
    /// Bearer API token retrieved from the vault.
    pub bearer_token: String,
    /// Optional serialized JSON payload for `POST` requests.
    pub json_body: Option<String>,
}

impl std::fmt::Debug for TransportRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportRequest")
            .field("provider", &self.provider)
            .field("method", &self.method)
            .field("url", &self.url)
            .field("bearer_token", &"[REDACTED]")
            .field("json_body", &self.json_body)
            .finish()
    }
}

/// Bounded HTTP response returned by a [`DecisionTransport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportResponse {
    /// HTTP status code (e.g. `200`, `401`, `429`, `529`).
    pub status: u16,
    /// Bounded response payload bytes.
    pub body: Vec<u8>,
}

/// Boxed future returned by [`DecisionTransport::execute`].
pub type TransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TransportResponse, DecisionError>> + Send + 'a>>;

/// Injectable transport boundary for decision provider HTTP calls.
pub trait DecisionTransport: Send + Sync {
    /// Executes a single HTTP request without automatic retries.
    fn execute<'a>(&'a self, request: TransportRequest) -> TransportFuture<'a>;
}

/// Production HTTPS transport backed by `reqwest` with `rustls`, redirects disabled,
/// fixed HTTPS endpoints, and bounded response reading.
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
    allow_test_http: bool,
}

impl std::fmt::Debug for ReqwestTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestTransport")
            .field("allow_test_http", &self.allow_test_http)
            .finish()
    }
}

impl ReqwestTransport {
    /// Builds a production HTTPS transport with redirects disabled and strict timeouts.
    pub fn new() -> Result<Self, DecisionError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .map_err(|_| DecisionError::ClientInitFailed)?;

        Ok(Self {
            client,
            allow_test_http: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Result<Self, DecisionError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .map_err(|_| DecisionError::ClientInitFailed)?;

        Ok(Self {
            client,
            allow_test_http: true,
        })
    }

    fn validate_url(
        &self,
        provider: DecisionProvider,
        method: TransportMethod,
        url: &str,
    ) -> Result<(), DecisionError> {
        if !self.allow_test_http {
            let expected = match (provider, method) {
                (DecisionProvider::Jev, TransportMethod::Get) => JEV_MODELS_URL,
                (DecisionProvider::Jev, TransportMethod::Post) => JEV_CLASSIFY_URL,
                (DecisionProvider::Openai, TransportMethod::Get) => OPENAI_MODELS_URL,
                (DecisionProvider::Openai, TransportMethod::Post) => OPENAI_CLASSIFY_URL,
            };
            if url != expected {
                return Err(DecisionError::InvalidEndpoint);
            }
            return Ok(());
        }

        if !(url.starts_with("https://")
            || url.starts_with("http://127.0.0.1:")
            || url.starts_with("http://localhost:"))
        {
            return Err(DecisionError::InvalidEndpoint);
        }
        Ok(())
    }
}

impl DecisionTransport for ReqwestTransport {
    fn execute<'a>(&'a self, request: TransportRequest) -> TransportFuture<'a> {
        Box::pin(async move {
            self.validate_url(request.provider, request.method, &request.url)?;

            let mut builder = match request.method {
                TransportMethod::Get => self.client.get(&request.url),
                TransportMethod::Post => self.client.post(&request.url),
            };

            builder = builder
                .bearer_auth(&request.bearer_token)
                .header(reqwest::header::ACCEPT, "application/json");

            if let Some(json_body) = request.json_body {
                builder = builder
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(json_body);
            }

            let mut response = builder.send().await.map_err(map_reqwest_error)?;
            let status = response.status().as_u16();

            if let Some(len) = response.content_length() {
                if len > MAX_RESPONSE_BYTES as u64 {
                    return Err(DecisionError::ResponseTooLarge);
                }
            }

            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
                if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    return Err(DecisionError::ResponseTooLarge);
                }
                body.extend_from_slice(&chunk);
            }

            Ok(TransportResponse { status, body })
        })
    }
}

fn map_reqwest_error(err: reqwest::Error) -> DecisionError {
    if err.is_timeout() {
        DecisionError::Timeout
    } else if err.is_redirect() {
        DecisionError::InvalidEndpoint
    } else {
        DecisionError::TransportFailed
    }
}

/// Maps HTTP status codes to safe [`DecisionError`] variants without leaking response bodies.
pub fn ensure_success_status(status: u16, body_len: usize) -> Result<(), DecisionError> {
    if body_len > MAX_RESPONSE_BYTES {
        return Err(DecisionError::ResponseTooLarge);
    }
    match status {
        200 => Ok(()),
        401 | 403 => Err(DecisionError::Unauthorized),
        429 | 529 => Err(DecisionError::RateLimited),
        400 | 404 | 422 => Err(DecisionError::ProviderRejected),
        300..=399 => Err(DecisionError::InvalidEndpoint),
        500..=599 => Err(DecisionError::ProviderUnavailable),
        _ => Err(DecisionError::InvalidResponse),
    }
}
