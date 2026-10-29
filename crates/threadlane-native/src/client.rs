use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    sanitize_diagnostic_text, validate_bounded_identifier, validate_executable_path,
    validate_optional_cwd_path, validate_optional_socket_path,
};
use crate::pairing::{
    ConnectionIdentity, EnrollmentRecord, NativeVerificationStatus, OperatorEnrollRequest,
    PairingStore, CONNECTION_IDENTITY_TOOL, MCP_SERVER_NAME,
};
use crate::rpc::{execute_proxy_rpc, DEFAULT_PROXY_TIMEOUT, MAX_RPC_FRAME_BYTES};
use crate::wakeup::{build_turn_steer_params, NativeWakeupRecord, NativeWakeupStore};

const DEFAULT_THREAD_LIST_LIMIT: u32 = 25;
const MAX_ID_CHARS: usize = 128;
const MAX_CURSOR_CHARS: usize = 512;

/// Public configuration for [`NativeClient`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeConfig {
    /// Absolute path to the native `codex` CLI executable.
    pub executable: PathBuf,
    /// Optional operator-selected app-server proxy socket path (`--sock`).
    pub socket: Option<PathBuf>,
}

/// Public read-only metadata for a native Codex thread (`thread.id` is authoritative, never `sessionId`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeThread {
    /// Authoritative native Codex `thread.id` UUIDv7 (never shared-fork `sessionId`).
    pub id: String,
    /// Nullable project identifier reported by `codex app-server`.
    pub project_id: Option<String>,
    /// Optional non-unique thread title (`name` in wire schema).
    pub title: Option<String>,
    /// Canonical working directory associated with the thread.
    pub cwd: String,
    /// Normalized thread status (`idle`, `active`, `not_loaded`, `system_error`).
    pub status: String,
    /// Configured thread model identifier, if reported.
    pub model: Option<String>,
    /// Configured thread reasoning effort, if reported.
    pub effort: Option<String>,
}

/// Paginated response from [`NativeClient::threads`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeThreadPage {
    /// Page of read-only native thread metadata entries.
    pub threads: Vec<NativeThread>,
    /// Opaque forward pagination cursor (`None` when no further pages exist).
    pub next_cursor: Option<String>,
}

/// Summary of a thread-scoped MCP server status entry from `mcpServerStatus/list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeMcpServerStatus {
    /// Configured MCP server name (e.g. `"threadlane"`).
    pub name: String,
    /// Reported authentication status (`unknown`, `unsupported`, `notLoggedIn`, `bearerToken`, `oAuth`).
    pub auth_status: String,
    /// Optional runtime connection status (`notStarted`, `starting`, `connected`, `failed`, etc.).
    pub runtime_status: Option<String>,
    /// Discovered tool names keyed on this server connection.
    pub tool_names: Vec<String>,
    /// Optional discovery error message reported by app-server.
    pub tools_error: Option<String>,
}

/// Result of a thread-scoped `mcpServer/tool/call` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeMcpToolCallResult {
    /// Content blocks returned by the MCP tool.
    pub content: Vec<Value>,
    /// Optional structured JSON payload returned by the MCP tool.
    pub structured_content: Option<Value>,
    /// Whether the tool reported an error (`isError`).
    pub is_error: bool,
}

/// Safe diagnostic report describing the current native Codex adapter configuration and gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeDiagnosticReport {
    /// Whether the native executable and socket paths passed static validation.
    pub configured: bool,
    /// Gate status for two-chat same-project MCP isolation (`needs_verification` until live host test).
    pub two_chat_routing_gate: NativeVerificationStatus,
    /// Gate status for busy-turn `turn/steer` wake-up delivery (`needs_verification` until live host test).
    pub busy_turn_delivery_gate: NativeVerificationStatus,
}

/// Client for the public `codex app-server proxy` stdio JSON-RPC interface.
///
/// Uses only read-only metadata discovery (`thread/list` without experimental `projectId`,
/// `thread/read` with `includeTurns: false`), thread-scoped MCP status/bootstrap calls,
/// and `expectedTurnId`-checked `turn/steer` with [`threadlane_core::FIXED_WAKEUP_MESSAGE`].
/// Never reads transcripts, never calls `thread/resume`, never spawns a new server, and never
/// creates or messages chats with model/permission overrides.
#[derive(Debug, Clone)]
pub struct NativeClient {
    config: NativeConfig,
    cwd_filter: Option<PathBuf>,
    timeout: Duration,
    max_frame_bytes: usize,
}

impl NativeClient {
    /// Validates `config` and constructs a new [`NativeClient`].
    pub fn new(config: NativeConfig) -> Result<Self, NativeError> {
        let executable = validate_executable_path(&config.executable)?;
        let socket = validate_optional_socket_path(config.socket.as_deref())?;
        Ok(Self {
            config: NativeConfig { executable, socket },
            cwd_filter: None,
            timeout: DEFAULT_PROXY_TIMEOUT,
            max_frame_bytes: MAX_RPC_FRAME_BYTES,
        })
    }

    /// Returns a reference to the validated [`NativeConfig`].
    pub fn config(&self) -> &NativeConfig {
        &self.config
    }

    /// Configures a bounded timeout for each `codex app-server proxy` exchange.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, NativeError> {
        if timeout.is_zero() || timeout > Duration::from_secs(120) {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "native proxy timeout must be between 1ms and 120s",
            ));
        }
        self.timeout = timeout;
        Ok(self)
    }

    /// Configures the maximum byte length of a single JSON-RPC frame read from the proxy.
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Result<Self, NativeError> {
        if !(1024..=MAX_RPC_FRAME_BYTES * 4).contains(&max_frame_bytes) {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "max_frame_bytes must be between 1024 and 4 MiB",
            ));
        }
        self.max_frame_bytes = max_frame_bytes;
        Ok(self)
    }

    /// Configures an optional stable `cwd` filter passed to `thread/list`.
    pub fn with_cwd_filter(mut self, cwd: PathBuf) -> Result<Self, NativeError> {
        self.cwd_filter = validate_optional_cwd_path(Some(&cwd))?;
        Ok(self)
    }

    /// Returns honest verification gate diagnostics for this client.
    pub fn diagnostics(&self) -> NativeDiagnosticReport {
        NativeDiagnosticReport {
            configured: true,
            two_chat_routing_gate: NativeVerificationStatus::NeedsVerification,
            busy_turn_delivery_gate: NativeVerificationStatus::NeedsVerification,
        }
    }

    /// Lists a page of native threads using the public `thread/list` RPC.
    ///
    /// Note: `ThreadListParams.projectId` is experimental in `codex app-server` and is **never**
    /// emitted on the wire. Instead, `thread/list` sends stable parameters (including any configured
    /// `cwd` filter) and filters returned thread metadata client-side when `project_id` is `Some`.
    pub async fn threads(
        &self,
        project_id: Option<String>,
        cursor: Option<String>,
    ) -> Result<NativeThreadPage, NativeError> {
        self.threads_in_cwd(self.cwd_filter.as_deref(), project_id, cursor)
            .await
    }

    /// Lists a page of native threads with an explicit optional stable `cwd` wire filter and
    /// optional client-side `project_id` metadata filter.
    pub async fn threads_in_cwd(
        &self,
        cwd: Option<&Path>,
        project_id: Option<String>,
        cursor: Option<String>,
    ) -> Result<NativeThreadPage, NativeError> {
        let clean_cwd = validate_optional_cwd_path(cwd)?;
        let clean_project_id = match project_id {
            Some(ref raw) => Some(validate_bounded_identifier(
                raw,
                "project_id",
                MAX_ID_CHARS,
            )?),
            None => None,
        };
        let clean_cursor = match cursor {
            Some(ref raw) => {
                if raw.is_empty()
                    || raw.len() > MAX_CURSOR_CHARS
                    || raw.trim() != raw
                    || raw.chars().any(char::is_control)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "cursor is empty, exceeds maximum length, or contains control characters",
                    ));
                }
                Some(raw.clone())
            }
            None => None,
        };

        let mut params = serde_json::Map::new();
        params.insert(
            "sourceKinds".to_string(),
            json!(["cli", "vscode", "appServer", "unknown"]),
        );
        params.insert("limit".to_string(), json!(DEFAULT_THREAD_LIST_LIMIT));
        params.insert("sortKey".to_string(), json!("updated_at"));
        params.insert("sortDirection".to_string(), json!("desc"));
        params.insert("useStateDbOnly".to_string(), json!(true));
        if let Some(cwd_path) = clean_cwd {
            params.insert(
                "cwd".to_string(),
                json!(cwd_path.to_string_lossy().into_owned()),
            );
        }
        if let Some(cur) = clean_cursor {
            params.insert("cursor".to_string(), json!(cur));
        }

        let wire_page: WireThreadListResult = execute_proxy_rpc(
            &self.config.executable,
            self.config.socket.as_deref(),
            self.timeout,
            self.max_frame_bytes,
            "thread/list",
            Value::Object(params),
        )
        .await
        .map_err(|e| e.error)?;

        let mut threads = Vec::with_capacity(wire_page.data.len());
        for wire_thread in wire_page.data {
            let mapped = wire_thread.into_native_thread()?;
            if let Some(ref wanted_project) = clean_project_id {
                if mapped.project_id.as_deref() != Some(wanted_project.as_str()) {
                    continue;
                }
            }
            threads.push(mapped);
        }

        let next_cursor = wire_page
            .next_cursor
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty());

        Ok(NativeThreadPage {
            threads,
            next_cursor,
        })
    }

    /// Fetches read-only metadata for a single native thread (`thread/read` with `includeTurns: false`).
    ///
    /// Never requests conversation turns or transcript text, and verifies that the returned
    /// authoritative `thread.id` matches `thread_id`.
    pub async fn thread(&self, thread_id: &str) -> Result<NativeThread, NativeError> {
        let clean_thread_id = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
        let params = json!({
            "threadId": clean_thread_id,
            "includeTurns": false
        });

        let wire_res: WireThreadReadResult = execute_proxy_rpc(
            &self.config.executable,
            self.config.socket.as_deref(),
            self.timeout,
            self.max_frame_bytes,
            "thread/read",
            params,
        )
        .await
        .map_err(|e| e.error)?;

        let mapped = wire_res.thread.into_native_thread()?;
        if mapped.id != clean_thread_id {
            return Err(NativeError::new(
                NativeErrorCode::ThreadNotFound,
                "returned native thread.id did not match requested thread_id",
            ));
        }
        Ok(mapped)
    }

    /// Queries `mcpServerStatus/list` scoped to `thread_id` and `server_name` (`detail: "toolsAndAuthOnly"`).
    pub async fn mcp_server_status(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<Vec<NativeMcpServerStatus>, NativeError> {
        let clean_thread_id = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
        let clean_server = validate_bounded_identifier(server_name, "server_name", MAX_ID_CHARS)?;
        let params = json!({
            "threadId": clean_thread_id,
            "serverName": clean_server,
            "detail": "toolsAndAuthOnly",
            "limit": 20
        });

        let wire_res: WireMcpServerStatusListResult = execute_proxy_rpc(
            &self.config.executable,
            self.config.socket.as_deref(),
            self.timeout,
            self.max_frame_bytes,
            "mcpServerStatus/list",
            params,
        )
        .await
        .map_err(|e| e.error)?;

        let mut out = Vec::with_capacity(wire_res.data.len());
        for entry in wire_res.data {
            let mut tool_names: Vec<String> = entry.tools.into_keys().collect();
            tool_names.sort();
            out.push(NativeMcpServerStatus {
                name: entry.name,
                auth_status: entry.auth_status,
                runtime_status: entry.runtime_status,
                tool_names,
                tools_error: entry.tools_error.map(|e| sanitize_diagnostic_text(&e)),
            });
        }
        Ok(out)
    }

    /// Invokes a thread-scoped MCP tool via `mcpServer/tool/call`, checking both JSON-RPC errors
    /// and `isError: true` in the tool result.
    ///
    /// Never emits `_meta` for identity provenance.
    pub async fn call_mcp_tool(
        &self,
        thread_id: &str,
        server: &str,
        tool: &str,
        arguments: Value,
    ) -> Result<NativeMcpToolCallResult, NativeError> {
        let clean_thread_id = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
        let clean_server = validate_bounded_identifier(server, "server", MAX_ID_CHARS)?;
        let clean_tool = validate_bounded_identifier(tool, "tool", MAX_ID_CHARS)?;
        if !arguments.is_object() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "MCP tool call arguments must be a JSON object",
            ));
        }

        let params = json!({
            "threadId": clean_thread_id,
            "server": clean_server,
            "tool": clean_tool,
            "arguments": arguments
        });

        let wire_res: WireMcpToolCallResult = execute_proxy_rpc(
            &self.config.executable,
            self.config.socket.as_deref(),
            self.timeout,
            self.max_frame_bytes,
            "mcpServer/tool/call",
            params,
        )
        .await
        .map_err(|e| e.error)?;

        let is_error = wire_res.is_error.unwrap_or(false);
        if is_error {
            let detail = wire_res
                .structured_content
                .as_ref()
                .and_then(|v| v.get("error"))
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .map(sanitize_diagnostic_text)
                .unwrap_or_else(|| format!("MCP tool `{clean_tool}` returned isError=true"));
            return Err(NativeError::new(NativeErrorCode::McpToolError, detail));
        }

        Ok(NativeMcpToolCallResult {
            content: wire_res.content,
            structured_content: wire_res.structured_content,
            is_error: false,
        })
    }

    /// Queries the `connection_identity` tool on `server_name` for `thread_id` via `mcpServer/tool/call`.
    pub async fn probe_thread_connection_identity(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<ConnectionIdentity, NativeError> {
        let res = self
            .call_mcp_tool(thread_id, server_name, CONNECTION_IDENTITY_TOOL, json!({}))
            .await?;

        if let Some(ref structured) = res.structured_content {
            if let Some(data_val) = structured.get("data") {
                if let Ok(identity) = serde_json::from_value::<ConnectionIdentity>(data_val.clone())
                {
                    return Ok(identity);
                }
            }
            if let Ok(identity) = serde_json::from_value::<ConnectionIdentity>(structured.clone()) {
                return Ok(identity);
            }
        }

        for block in &res.content {
            if let Some(text) = block.get("text").and_then(Value::as_str) {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    if let Some(data_val) = parsed.get("data") {
                        if let Ok(identity) =
                            serde_json::from_value::<ConnectionIdentity>(data_val.clone())
                        {
                            return Ok(identity);
                        }
                    }
                    if let Ok(identity) = serde_json::from_value::<ConnectionIdentity>(parsed) {
                        return Ok(identity);
                    }
                }
            }
        }

        Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "connection_identity tool response did not contain a valid ConnectionIdentity payload",
        ))
    }

    /// Performs the trusted operator enrollment flow for `thread_id` -> `binding_id`:
    /// 1. Reads metadata for `thread_id` (`thread/read`, `includeTurns: false`) to verify `thread.id`
    ///    and optional `expected_project_id`.
    /// 2. Queries `mcpServer/tool/call` -> `connection_identity` scoped to `thread.id`.
    /// 3. Enrolls `(connection_nonce, epoch_id)` in `pairing_store` using `operator_token`.
    ///    If the MCP child connection is shared with another thread, `pairing_store` quarantines it
    ///    and returns [`NativeErrorCode::SharedConnectionRejected`].
    pub async fn enroll_thread_connection(
        &self,
        pairing_store: &PairingStore,
        operator_token: &str,
        thread_id: &str,
        binding_id: &str,
        expected_project_id: Option<&str>,
    ) -> Result<EnrollmentRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let expected_proj = match expected_project_id {
            Some(p) => Some(validate_bounded_identifier(
                p,
                "expected_project_id",
                MAX_ID_CHARS,
            )?),
            None => None,
        };

        let thread_meta = self.thread(thread_id).await?;
        if let Some(ref wanted_proj) = expected_proj {
            if thread_meta.project_id.as_deref() != Some(wanted_proj.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::ProjectMismatch,
                    "native thread projectId does not match expected project_id",
                ));
            }
        }

        let identity = self
            .probe_thread_connection_identity(&thread_meta.id, MCP_SERVER_NAME)
            .await?;

        pairing_store.enroll(
            operator_token,
            OperatorEnrollRequest {
                connection_nonce: identity.connection_nonce,
                epoch_id: identity.epoch_id,
                binding_id: clean_binding,
                thread_id: thread_meta.id,
                project_id: thread_meta.project_id,
            },
        )
    }

    /// Sends a fixed-message busy-turn wake-up via public `turn/steer` with `expectedTurnId` verification
    /// and durable `Pending` / `Submitted` / `Unknown` / `Received` tracking in `wakeup_store`.
    ///
    /// - Refuses with [`NativeErrorCode::MissingLiveTurnId`] if `expected_turn_id` is `None`
    ///   (never guesses from persisted turns or calls `thread/resume`).
    /// - Refuses with [`NativeErrorCode::AmbiguousWakeupPendingReconciliation`] if a prior submission
    ///   for `binding_id` is in `Unknown` or unacknowledged `Submitted` state.
    /// - Records `Unknown` if the proxy times out, disconnects, or fails after writing the `turn/steer`
    ///   request frame to the transport.
    pub async fn steer_active_turn(
        &self,
        wakeup_store: &NativeWakeupStore,
        binding_id: &str,
        thread_id: &str,
        expected_turn_id: Option<&str>,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let Some(turn_id_raw) = expected_turn_id else {
            return Err(NativeError::new(
                NativeErrorCode::MissingLiveTurnId,
                "active-turn steer requires a server-reported live expectedTurnId; refusing to guess from persisted turns",
            ));
        };
        let clean_turn_id =
            validate_bounded_identifier(turn_id_raw, "expected_turn_id", MAX_ID_CHARS)?;
        let pending = wakeup_store.prepare_pending(binding_id, thread_id, Some(&clean_turn_id))?;
        let params = build_turn_steer_params(&pending.thread_id, &clean_turn_id)?;

        match execute_proxy_rpc::<WireTurnSteerResult>(
            &self.config.executable,
            self.config.socket.as_deref(),
            self.timeout,
            self.max_frame_bytes,
            "turn/steer",
            params,
        )
        .await
        {
            Ok(res) => {
                if res.turn_id != clean_turn_id {
                    let _ = wakeup_store.record_unknown(
                        binding_id,
                        &pending.attempt_id,
                        "turn/steer response turnId did not match expectedTurnId",
                    );
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousTransportFailure,
                        "turn/steer returned mismatched turnId; wakeup marked unknown",
                    ));
                }
                wakeup_store.record_submitted(binding_id, &pending.attempt_id, &res.turn_id)
            }
            Err(call_err) => {
                if call_err.error.code == NativeErrorCode::RpcError {
                    let _ = wakeup_store.record_rejected_pre_delivery(
                        binding_id,
                        &pending.attempt_id,
                        &call_err.error.message,
                    );
                    return Err(NativeError::new(
                        NativeErrorCode::TurnSteerRejected,
                        format!("turn/steer rejected by server: {}", call_err.error.message),
                    ));
                }
                if !call_err.request_written_to_wire {
                    let _ = wakeup_store.record_rejected_pre_delivery(
                        binding_id,
                        &pending.attempt_id,
                        &call_err.error.message,
                    );
                    return Err(call_err.error);
                }

                let _ = wakeup_store.record_unknown(
                    binding_id,
                    &pending.attempt_id,
                    &call_err.error.message,
                );
                Err(NativeError::new(
                    NativeErrorCode::AmbiguousTransportFailure,
                    format!(
                        "ambiguous turn/steer outcome after wire transmission (wakeup marked unknown): {}",
                        call_err.error.message
                    ),
                ))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireThreadListResult {
    data: Vec<WireThread>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireThreadReadResult {
    thread: WireThread,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireThread {
    id: String,
    #[serde(default, rename = "sessionId")]
    _session_id_ignored: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default, alias = "title")]
    name: Option<String>,
    cwd: String,
    status: WireThreadStatusField,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, alias = "effort")]
    reasoning_effort: Option<String>,
}

impl WireThread {
    fn into_native_thread(self) -> Result<NativeThread, NativeError> {
        let clean_id = validate_bounded_identifier(&self.id, "thread.id", MAX_ID_CHARS)?;
        if self.cwd.trim().is_empty() || self.cwd.chars().any(char::is_control) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "native thread metadata contained an empty or invalid cwd",
            ));
        }

        let project_id = self
            .project_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let title = self
            .name
            .map(|s| sanitize_diagnostic_text(&s))
            .filter(|s| !s.is_empty() && s != "native operation error");
        let model = self
            .model
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let effort = self
            .reasoning_effort
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(NativeThread {
            id: clean_id,
            project_id,
            title,
            cwd: self.cwd,
            status: self.status.into_status_string()?,
            model,
            effort,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WireThreadStatusField {
    Tagged {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default, rename = "activeFlags")]
        _active_flags: Vec<String>,
    },
    Plain(String),
}

impl WireThreadStatusField {
    fn into_status_string(self) -> Result<String, NativeError> {
        let raw = match self {
            Self::Tagged { kind, .. } => kind,
            Self::Plain(s) => s,
        };
        let normalized = match raw.trim() {
            "notLoaded" | "not_loaded" => "not_loaded",
            "idle" => "idle",
            "systemError" | "system_error" => "system_error",
            "active" => "active",
            other => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    format!(
                        "unsupported native ThreadStatus variant `{}`",
                        sanitize_diagnostic_text(other)
                    ),
                ));
            }
        };
        Ok(normalized.to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMcpServerStatusListResult {
    data: Vec<WireMcpServerStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMcpServerStatus {
    name: String,
    auth_status: String,
    #[serde(default)]
    runtime_status: Option<String>,
    #[serde(default)]
    tools: BTreeMap<String, Value>,
    #[serde(default)]
    tools_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMcpToolCallResult {
    #[serde(default)]
    content: Vec<Value>,
    #[serde(default)]
    structured_content: Option<Value>,
    #[serde(default)]
    is_error: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTurnSteerResult {
    turn_id: String,
}
