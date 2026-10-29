use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use threadlane_core::{AckResult, Binding, CoreError, EventBatch, Run, MAX_ID_LEN};

use crate::descriptor::{read_and_validate_descriptor, RuntimeDescriptor};
use crate::error::{ApiError, Error};
use crate::protocol::{
    BindingListResponse, CreateBindingRequest, ScopedRequest, ScopedResponse, StatusResponse,
};

/// Maximum response body size accepted by [`SupervisorClient`] (2 MiB).
pub const MAX_CLIENT_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

/// Default HTTP request timeout for local supervisor IPC.
pub const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default TCP connect timeout for local supervisor IPC.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
enum DescriptorSource {
    StateDir(PathBuf),
    Fixed(RuntimeDescriptor),
}

/// Authenticated local HTTP client for communicating with the [`crate::Supervisor`].
///
/// Bypasses environment HTTP proxies, disables redirects, enforces timeouts and response
/// body limits, and validates descriptor security invariants before transmitting the bearer token.
#[derive(Clone)]
pub struct SupervisorClient {
    source: DescriptorSource,
    http: reqwest::Client,
}

impl fmt::Debug for SupervisorClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            DescriptorSource::StateDir(dir) => f
                .debug_struct("SupervisorClient")
                .field("state_dir", dir)
                .finish(),
            DescriptorSource::Fixed(desc) => f
                .debug_struct("SupervisorClient")
                .field("descriptor", desc)
                .finish(),
        }
    }
}

impl SupervisorClient {
    /// Creates a client backed by `<state_dir>/supervisor.json`.
    ///
    /// Validates `state_dir` and `supervisor.json` immediately and re-validates on each
    /// request so supervisor restarts are handled transparently.
    pub fn from_state_dir(state_dir: impl AsRef<Path>) -> Result<Self, Error> {
        let dir = state_dir.as_ref().to_path_buf();
        let _ = read_and_validate_descriptor(&dir)?;
        let http = build_http_client()?;
        Ok(Self {
            source: DescriptorSource::StateDir(dir),
            http,
        })
    }

    /// Creates a client from an explicitly supplied [`RuntimeDescriptor`] after validating it.
    pub fn from_descriptor(descriptor: RuntimeDescriptor) -> Result<Self, Error> {
        descriptor.validate()?;
        let http = build_http_client()?;
        Ok(Self {
            source: DescriptorSource::Fixed(descriptor),
            http,
        })
    }

    /// Creates a [`ScopedClient`] restricted to a single `binding_id`.
    pub fn scoped(self, binding_id: impl Into<String>) -> Result<ScopedClient, Error> {
        let binding_id = validate_client_binding_id(&binding_id.into())?;
        Ok(ScopedClient {
            client: self,
            binding_id,
        })
    }

    /// Creates a new trusted binding (CLI only).
    pub async fn create_binding(&self, thread_uuid: &str, label: &str) -> Result<Binding, Error> {
        let req = CreateBindingRequest {
            thread_uuid: thread_uuid.to_string(),
            label: label.to_string(),
        };
        self.send_json(Method::POST, "/v1/bindings", Some(&req))
            .await
    }

    /// Lists all configured bindings (CLI only).
    pub async fn list_bindings(&self) -> Result<Vec<Binding>, Error> {
        let resp: BindingListResponse = self
            .send_json::<(), _>(Method::GET, "/v1/bindings", None)
            .await?;
        Ok(resp.bindings)
    }

    /// Fetches metadata for a single `binding_id`.
    pub async fn get_binding(&self, binding_id: &str) -> Result<Binding, Error> {
        let clean_id = validate_client_binding_id(binding_id)?;
        let path = format!("/v1/bindings/{clean_id}");
        self.send_json::<(), _>(Method::GET, &path, None).await
    }

    /// Executes a [`ScopedRequest`] against `binding_id`.
    pub async fn execute_scoped(
        &self,
        binding_id: &str,
        request: &ScopedRequest,
    ) -> Result<ScopedResponse, Error> {
        let clean_id = validate_client_binding_id(binding_id)?;
        let path = format!("/v1/bindings/{clean_id}/command");
        self.send_json(Method::POST, &path, Some(request)).await
    }

    /// Fetches the compact [`StatusResponse`] for `binding_id`.
    pub async fn status(&self, binding_id: &str) -> Result<StatusResponse, Error> {
        let resp = self
            .execute_scoped(binding_id, &ScopedRequest::Status)
            .await?;
        resp.into_status()
            .ok_or_else(|| Error::Transport("unexpected response kind for status".to_string()))
    }

    fn resolve_descriptor(&self) -> Result<RuntimeDescriptor, Error> {
        match &self.source {
            DescriptorSource::StateDir(dir) => read_and_validate_descriptor(dir),
            DescriptorSource::Fixed(desc) => {
                desc.validate()?;
                Ok(desc.clone())
            }
        }
    }

    async fn send_json<Req, Resp>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Req>,
    ) -> Result<Resp, Error>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let descriptor = self.resolve_descriptor()?;
        let base_url = descriptor.base_url()?;
        let url = format!("{base_url}{path}");

        let mut req_builder = self.http.request(method, &url).header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", descriptor.token.as_str()),
        );

        if let Some(payload) = body {
            req_builder = req_builder.json(payload);
        }

        let mut response = req_builder
            .send()
            .await
            .map_err(|err| Error::Transport(sanitize_reqwest_error(&err)))?;

        if let Some(content_len) = response.content_length() {
            if content_len > MAX_CLIENT_RESPONSE_BYTES as u64 {
                return Err(Error::Transport(format!(
                    "response content-length {content_len} exceeds limit of {MAX_CLIENT_RESPONSE_BYTES} bytes"
                )));
            }
        }

        let status = response.status();
        let initial_cap = response
            .content_length()
            .and_then(|len| usize::try_from(len).ok())
            .unwrap_or(1024)
            .min(64 * 1024);
        let mut bytes = Vec::with_capacity(initial_cap);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| Error::Transport(sanitize_reqwest_error(&err)))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_CLIENT_RESPONSE_BYTES {
                return Err(Error::Transport(format!(
                    "response body size exceeds limit of {MAX_CLIENT_RESPONSE_BYTES} bytes"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }

        if !status.is_success() {
            if let Ok(api_err) = serde_json::from_slice::<ApiError>(&bytes) {
                return Err(Error::Remote(api_err));
            }
            return Err(Error::Transport(format!(
                "supervisor returned HTTP {}",
                status.as_u16()
            )));
        }

        serde_json::from_slice::<Resp>(&bytes).map_err(Error::Json)
    }
}

/// Client restricted to a single pre-configured `binding_id` (used by the MCP server).
///
/// Does not expose `create_binding`, `list_bindings`, or cross-binding operations.
#[derive(Debug, Clone)]
pub struct ScopedClient {
    client: SupervisorClient,
    binding_id: String,
}

impl ScopedClient {
    /// Connects a [`ScopedClient`] for `binding_id` using `<state_dir>/supervisor.json`.
    pub fn from_state_dir(
        state_dir: impl AsRef<Path>,
        binding_id: impl Into<String>,
    ) -> Result<Self, Error> {
        let client = SupervisorClient::from_state_dir(state_dir)?;
        client.scoped(binding_id)
    }

    /// Returns the fixed `binding_id` for this scoped client.
    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    /// Executes a [`ScopedRequest`] on the fixed binding.
    pub async fn execute(&self, request: &ScopedRequest) -> Result<ScopedResponse, Error> {
        self.client.execute_scoped(&self.binding_id, request).await
    }

    /// Fetches compact status for the fixed binding.
    pub async fn status(&self) -> Result<StatusResponse, Error> {
        self.client.status(&self.binding_id).await
    }

    /// Fetches full details for `run_id` within the fixed binding.
    pub async fn get_run(&self, run_id: String) -> Result<Run, Error> {
        let resp = self.execute(&ScopedRequest::GetRun { run_id }).await?;
        resp.into_run()
            .ok_or_else(|| Error::Transport("unexpected response kind for get_run".to_string()))
    }

    /// Starts a deterministic demo run within the fixed binding.
    pub async fn start_demo(
        &self,
        command_key: String,
        instruction: String,
        step_delay_ms: Option<i64>,
    ) -> Result<Run, Error> {
        let resp = self
            .execute(&ScopedRequest::StartDemo {
                command_key,
                instruction,
                step_delay_ms,
            })
            .await?;
        resp.into_run()
            .ok_or_else(|| Error::Transport("unexpected response kind for start_demo".to_string()))
    }

    /// Pulls a bounded event batch within the fixed binding.
    pub async fn pull_events(
        &self,
        after_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<EventBatch, Error> {
        let resp = self
            .execute(&ScopedRequest::PullEvents { after_seq, limit })
            .await?;
        resp.into_event_batch()
            .ok_or_else(|| Error::Transport("unexpected response kind for pull_events".to_string()))
    }

    /// Acknowledges a delivered event batch within the fixed binding.
    pub async fn ack_events(
        &self,
        command_key: String,
        batch_id: String,
    ) -> Result<AckResult, Error> {
        let resp = self
            .execute(&ScopedRequest::AckEvents {
                command_key,
                batch_id,
            })
            .await?;
        resp.into_ack_result()
            .ok_or_else(|| Error::Transport("unexpected response kind for ack_events".to_string()))
    }

    /// Answers an active question on `run_id` at `expected_revision` within the fixed binding.
    pub async fn answer(
        &self,
        command_key: String,
        run_id: String,
        question_id: String,
        expected_revision: u64,
        answer: String,
    ) -> Result<Run, Error> {
        let resp = self
            .execute(&ScopedRequest::Answer {
                command_key,
                run_id,
                question_id,
                expected_revision,
                answer,
            })
            .await?;
        resp.into_run()
            .ok_or_else(|| Error::Transport("unexpected response kind for answer".to_string()))
    }

    /// Steers `run_id` at `expected_revision` to a new instruction within the fixed binding.
    pub async fn steer(
        &self,
        command_key: String,
        run_id: String,
        expected_revision: u64,
        instruction: String,
    ) -> Result<Run, Error> {
        let resp = self
            .execute(&ScopedRequest::Steer {
                command_key,
                run_id,
                expected_revision,
                instruction,
            })
            .await?;
        resp.into_run()
            .ok_or_else(|| Error::Transport("unexpected response kind for steer".to_string()))
    }

    /// Cancels `run_id` within the fixed binding.
    pub async fn cancel(
        &self,
        command_key: String,
        run_id: String,
        reason: Option<String>,
    ) -> Result<Run, Error> {
        let resp = self
            .execute(&ScopedRequest::Cancel {
                command_key,
                run_id,
                reason,
            })
            .await?;
        resp.into_run()
            .ok_or_else(|| Error::Transport("unexpected response kind for cancel".to_string()))
    }
}

fn build_http_client() -> Result<reqwest::Client, Error> {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(DEFAULT_CLIENT_TIMEOUT)
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .build()
        .map_err(|err| Error::Transport(sanitize_reqwest_error(&err)))
}

fn validate_client_binding_id(raw: &str) -> Result<String, Error> {
    if raw.is_empty()
        || raw.len() > MAX_ID_LEN
        || raw.trim() != raw
        || raw
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#' | '%'))
    {
        return Err(Error::Core(CoreError::Validation {
            field: "binding_id",
            reason: "binding_id is empty, exceeds maximum length, or contains invalid characters"
                .to_string(),
        }));
    }
    Ok(raw.to_string())
}

fn sanitize_reqwest_error(err: &reqwest::Error) -> String {
    if err.is_timeout() {
        "local supervisor request timed out".to_string()
    } else if err.is_connect() {
        "failed to connect to local supervisor on 127.0.0.1".to_string()
    } else if err.is_redirect() {
        "rejected unexpected HTTP redirect from local supervisor".to_string()
    } else {
        "local supervisor HTTP transport error".to_string()
    }
}
