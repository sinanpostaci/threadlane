use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use threadlane::NativeReconnectConfig;

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
use crate::ws_rpc::{execute_ws_rpc_for_backend, map_reconnect_error};

const DEFAULT_THREAD_LIST_LIMIT: u32 = 25;
const MAX_ID_CHARS: usize = 128;
const MAX_CURSOR_CHARS: usize = 512;
const MAX_ACTIVE_FLAGS_COUNT: usize = 32;
const MAX_ACTIVE_FLAG_CHARS: usize = 128;

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
    /// Whether the host allows direct input to this loaded thread, when reported.
    #[serde(default)]
    pub can_accept_direct_input: Option<bool>,
}

/// Structured status detail extracted from a native Codex `ThreadStatus` wire field.
///
/// Preserves whether the backend reported a structured tagged object (`{"type": ...}`) versus a
/// plain string (`"active"`), whether an explicit `activeFlags` array was present on the wire,
/// bounded flag tokens, and whether any malformed or oversized flag entry was encountered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeThreadStatusDetail {
    /// Normalized coarse thread status (`idle`, `active`, `not_loaded`, `system_error`).
    pub status: String,
    /// `true` when the wire status was a structured tagged object (`{"type": ...}`),
    /// `false` when it was a plain string without structured `activeFlags`.
    pub tagged: bool,
    /// `true` when the wire status object explicitly carried a non-null `activeFlags` array.
    #[serde(default)]
    pub active_flags_present: bool,
    /// Sanitized bounded `activeFlags` tokens reported by the backend.
    #[serde(default)]
    pub active_flags: Vec<String>,
    /// `true` if `activeFlags` exceeded count/length bounds or contained non-identifier tokens.
    #[serde(default)]
    pub has_invalid_active_flags: bool,
}

/// Additive read-only thread metadata envelope returned by [`NativeClient::thread_metadata`].
///
/// Preserves existing [`NativeThread`] compatibility while surfacing structured
/// [`NativeThreadStatusDetail`] (`activeFlags` and tagged-vs-plain wire provenance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeThreadMetadata {
    /// Normalized [`NativeThread`] metadata compatible with [`NativeClient::thread`].
    pub thread: NativeThread,
    /// Structured status detail including `activeFlags` provenance.
    pub status_detail: NativeThreadStatusDetail,
}

/// Paginated response from [`NativeClient::threads`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeThreadPage {
    /// Page of read-only native thread metadata entries.
    pub threads: Vec<NativeThread>,
    /// Opaque forward pagination cursor (`None` when no further pages exist).
    pub next_cursor: Option<String>,
}

/// A bounded page of currently loaded native thread identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeLoadedThreadPage {
    /// Canonical thread UUIDs returned by the app-server, without chat content.
    pub thread_ids: Vec<String>,
    /// Opaque continuation cursor, if another page exists.
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

/// Explicit transport selection for [`NativeClient`].
#[derive(Clone, PartialEq, Eq)]
pub enum NativeTransport {
    /// Spawns `codex app-server proxy [--sock <path>]` over stdio in an isolated Unix process group.
    Proxy(NativeConfig),
    /// Connects directly to the verified Linux managed Unix WebSocket endpoint resolved via
    /// [`threadlane::NativeReconnectConfig`] with a strict read-only RPC allowlist.
    ManagedWebSocket(NativeReconnectConfig),
}

impl fmt::Debug for NativeTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Proxy(cfg) => f
                .debug_struct("Proxy")
                .field("executable", &"[REDACTED]")
                .field("socket", &cfg.socket.as_ref().map(|_| "[REDACTED]"))
                .finish(),
            Self::ManagedWebSocket(reconnect) => {
                f.debug_tuple("ManagedWebSocket").field(reconnect).finish()
            }
        }
    }
}

/// Client for the public `codex app-server proxy` stdio JSON-RPC interface or the verified
/// read-only managed Unix WebSocket endpoint.
///
/// Uses only read-only metadata discovery (`thread/list` without experimental `projectId`,
/// `thread/read` with `includeTurns: false`), thread-scoped MCP status/bootstrap calls,
/// and (on the proxy transport only) `expectedTurnId`-checked `turn/steer` with
/// [`threadlane_core::FIXED_WAKEUP_MESSAGE`].
/// Never reads transcripts, never calls `thread/resume`, never spawns a new server, and never
/// creates or messages chats with model/permission overrides.
#[derive(Clone)]
pub struct NativeClient {
    config: NativeConfig,
    transport: NativeTransport,
    cwd_filter: Option<PathBuf>,
    timeout: Duration,
    max_frame_bytes: usize,
    expected_backend_pid: Option<u32>,
}

impl fmt::Debug for NativeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeClient")
            .field("transport", &self.transport)
            .field(
                "cwd_filter",
                &self.cwd_filter.as_ref().map(|_| "[REDACTED]"),
            )
            .field("timeout", &self.timeout)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .finish()
    }
}

impl NativeClient {
    /// Validates `config` and constructs a new proxy-backed [`NativeClient`].
    pub fn new(config: NativeConfig) -> Result<Self, NativeError> {
        let executable = validate_executable_path(&config.executable)?;
        let socket = validate_optional_socket_path(config.socket.as_deref())?;
        let validated = NativeConfig { executable, socket };
        Ok(Self {
            config: validated.clone(),
            transport: NativeTransport::Proxy(validated),
            cwd_filter: None,
            timeout: DEFAULT_PROXY_TIMEOUT,
            max_frame_bytes: MAX_RPC_FRAME_BYTES,
            expected_backend_pid: None,
        })
    }

    /// Validates `reconnect` and constructs a new read-only managed WebSocket [`NativeClient`].
    ///
    /// Requires an explicit [`threadlane::NativeReconnectConfig`] with a private `0700`
    /// `descriptor_root` and canonical `backend_executable` and `application_executable` paths.
    /// Non-Linux platforms fail closed with [`NativeErrorCode::UnsupportedPlatform`].
    pub fn new_managed_websocket(reconnect: NativeReconnectConfig) -> Result<Self, NativeError> {
        reconnect.validate().map_err(map_reconnect_error)?;
        let config = NativeConfig {
            executable: reconnect.backend_executable.clone(),
            socket: None,
        };
        Ok(Self {
            config,
            transport: NativeTransport::ManagedWebSocket(reconnect),
            cwd_filter: None,
            timeout: DEFAULT_PROXY_TIMEOUT,
            max_frame_bytes: MAX_RPC_FRAME_BYTES,
            expected_backend_pid: None,
        })
    }

    /// Validates `transport` and constructs a new [`NativeClient`].
    pub fn with_transport(transport: NativeTransport) -> Result<Self, NativeError> {
        match transport {
            NativeTransport::Proxy(cfg) => Self::new(cfg),
            NativeTransport::ManagedWebSocket(reconnect) => Self::new_managed_websocket(reconnect),
        }
    }

    /// Requires the verified backend to remain this MCP process's discovered parent.
    pub(crate) fn with_expected_backend_pid(mut self, pid: u32) -> Self {
        self.expected_backend_pid = Some(pid);
        self
    }

    /// Switches this client to the verified read-only managed WebSocket transport using `reconnect`.
    pub fn with_managed_websocket(
        mut self,
        reconnect: NativeReconnectConfig,
    ) -> Result<Self, NativeError> {
        reconnect.validate().map_err(map_reconnect_error)?;
        self.config = NativeConfig {
            executable: reconnect.backend_executable.clone(),
            socket: None,
        };
        self.transport = NativeTransport::ManagedWebSocket(reconnect);
        Ok(self)
    }

    /// Returns a reference to the validated [`NativeConfig`].
    pub fn config(&self) -> &NativeConfig {
        &self.config
    }

    /// Returns the active [`NativeTransport`] configured for this client.
    pub fn transport(&self) -> &NativeTransport {
        &self.transport
    }

    /// Returns the configured [`NativeReconnectConfig`] when using the managed WebSocket transport.
    pub fn managed_websocket_config(&self) -> Option<&NativeReconnectConfig> {
        match &self.transport {
            NativeTransport::ManagedWebSocket(reconnect) => Some(reconnect),
            NativeTransport::Proxy(_) => None,
        }
    }

    /// Returns `true` if this client is configured to use the read-only managed WebSocket transport.
    pub fn is_managed_websocket(&self) -> bool {
        matches!(self.transport, NativeTransport::ManagedWebSocket(_))
    }

    /// Configures a bounded timeout for each `codex app-server` RPC exchange.
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

    /// Configures the maximum byte length of a single JSON-RPC frame read from the transport.
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

    async fn execute_readonly_rpc<Resp>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Resp, NativeError>
    where
        Resp: serde::de::DeserializeOwned,
    {
        match &self.transport {
            NativeTransport::Proxy(cfg) => execute_proxy_rpc(
                &cfg.executable,
                cfg.socket.as_deref(),
                self.timeout,
                self.max_frame_bytes,
                method,
                params,
            )
            .await
            .map_err(|e| e.error),
            NativeTransport::ManagedWebSocket(reconnect) => {
                execute_ws_rpc_for_backend(
                    reconnect,
                    self.expected_backend_pid,
                    self.timeout,
                    self.max_frame_bytes,
                    method,
                    params,
                )
                .await
            }
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

    /// Lists currently loaded native thread IDs without reading chat history.
    pub async fn loaded_thread_ids(
        &self,
        cursor: Option<String>,
    ) -> Result<NativeLoadedThreadPage, NativeError> {
        let mut params = json!({"limit": 100});
        if let Some(cursor) = cursor {
            if cursor.is_empty()
                || cursor.len() > MAX_CURSOR_CHARS
                || cursor.trim() != cursor
                || cursor.chars().any(char::is_control)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "invalid loaded-thread cursor",
                ));
            }
            params["cursor"] = json!(cursor);
        }
        let response: WireLoadedThreadListResult = self
            .execute_readonly_rpc("thread/loaded/list", params)
            .await?;
        if response.data.len() > 100 {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "loaded-thread page exceeded limit",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for id in &response.data {
            let parsed = uuid::Uuid::parse_str(id).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "loaded-thread ID is invalid",
                )
            })?;
            if parsed.is_nil() || parsed.hyphenated().to_string() != *id || !seen.insert(id) {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "loaded-thread IDs are invalid or duplicated",
                ));
            }
        }
        let next_cursor = match response.next_cursor {
            Some(cursor)
                if cursor.is_empty()
                    || cursor.len() > MAX_CURSOR_CHARS
                    || cursor.trim() != cursor
                    || cursor.chars().any(char::is_control) =>
            {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "loaded-thread response cursor is invalid",
                ));
            }
            other => other,
        };
        Ok(NativeLoadedThreadPage {
            thread_ids: response.data,
            next_cursor,
        })
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

        let wire_page: WireThreadListResult = self
            .execute_readonly_rpc("thread/list", Value::Object(params))
            .await?;

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
        self.thread_metadata(thread_id)
            .await
            .map(|metadata| metadata.thread)
    }

    /// Fetches additive read-only metadata and structured [`NativeThreadStatusDetail`] (`activeFlags`)
    /// for a single native thread (`thread/read` with `includeTurns: false`).
    ///
    /// Never requests conversation turns or transcript text, and verifies that the returned
    /// authoritative `thread.id` matches `thread_id`.
    pub async fn thread_metadata(
        &self,
        thread_id: &str,
    ) -> Result<NativeThreadMetadata, NativeError> {
        let clean_thread_id = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
        let params = json!({
            "threadId": clean_thread_id,
            "includeTurns": false
        });

        let wire_res: WireThreadReadResult =
            self.execute_readonly_rpc("thread/read", params).await?;

        let mapped = wire_res.thread.into_native_thread_metadata()?;
        if mapped.thread.id != clean_thread_id {
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

        let wire_res: WireMcpServerStatusListResult = self
            .execute_readonly_rpc("mcpServerStatus/list", params)
            .await?;

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
    /// On the managed WebSocket transport, only `server: "threadlane"`,
    /// `tool: "connection_identity"`, and `arguments: {}` are permitted; all other MCP tool calls
    /// are rejected before any frame is sent.
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

        let wire_res: WireMcpToolCallResult = self
            .execute_readonly_rpc("mcpServer/tool/call", params)
            .await?;

        let is_error = wire_res.is_error.unwrap_or(false);
        if is_error {
            if self.is_managed_websocket() {
                return Err(NativeError::new(
                    NativeErrorCode::McpToolError,
                    "Threadlane MCP tool returned an error.",
                ));
            }
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

        let structured_identity = res
            .structured_content
            .as_ref()
            .and_then(parse_connection_identity_value);

        let mut text_identity = None;
        for block in &res.content {
            if let Some(text) = block.get("text").and_then(Value::as_str) {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    if let Some(identity) = parse_connection_identity_value(&parsed) {
                        if text_identity
                            .as_ref()
                            .is_some_and(|previous| previous != &identity)
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ProtocolError,
                                "connection_identity text content blocks disagreed",
                            ));
                        }
                        text_identity = Some(identity);
                    }
                }
            }
        }

        if let (Some(ref s_id), Some(ref t_id)) = (&structured_identity, &text_identity) {
            if s_id != t_id {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "connection_identity structuredContent and text content disagreed",
                ));
            }
        }

        let identity = structured_identity.or(text_identity).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "connection_identity tool response did not contain a valid ConnectionIdentity payload",
            )
        })?;

        if identity.connection_nonce.trim().is_empty()
            || identity.epoch_id.trim().is_empty()
            || identity.server_pid == 0
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "connection_identity tool response contained an empty nonce, epoch_id, or zero server_pid",
            ));
        }

        Ok(identity)
    }

    /// Performs the trusted operator enrollment flow for `thread_id` -> `binding_id`:
    /// 1. Reads metadata for `thread_id` (`thread/read`, `includeTurns: false`) to verify `thread.id`
    ///    and optional `expected_project_id`.
    /// 2. Queries `mcpServer/tool/call` -> `connection_identity` scoped to `thread.id`.
    /// 3. Enrolls `(connection_nonce, epoch_id)` in `pairing_store` using `operator_token`.
    ///    If the MCP child connection is shared with another thread, `pairing_store` quarantines it
    ///    and returns [`NativeErrorCode::SharedConnectionRejected`].
    ///
    /// Note: The managed WebSocket transport is strictly read-only and rejects this method before
    /// any wire frame or store mutation.
    pub async fn enroll_thread_connection(
        &self,
        pairing_store: &PairingStore,
        operator_token: &str,
        thread_id: &str,
        binding_id: &str,
        expected_project_id: Option<&str>,
    ) -> Result<EnrollmentRecord, NativeError> {
        if self.is_managed_websocket() {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "managed WebSocket transport is strictly read-only and does not permit thread connection enrollment",
            ));
        }

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
    /// - Refuses with [`NativeErrorCode::UnsupportedCapability`] when using the read-only managed
    ///   WebSocket transport (before touching `wakeup_store` or the wire).
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
        let NativeTransport::Proxy(ref proxy_cfg) = self.transport else {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "managed WebSocket transport is strictly read-only and does not permit turn/steer",
            ));
        };

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
            &proxy_cfg.executable,
            proxy_cfg.socket.as_deref(),
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

fn parse_connection_identity_value(val: &Value) -> Option<ConnectionIdentity> {
    if let Some(data_val) = val.get("data") {
        if let Ok(identity) = serde_json::from_value::<ConnectionIdentity>(data_val.clone()) {
            return Some(identity);
        }
    }
    serde_json::from_value::<ConnectionIdentity>(val.clone()).ok()
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
struct WireLoadedThreadListResult {
    data: Vec<String>,
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
    #[serde(default)]
    can_accept_direct_input: Option<bool>,
}

impl WireThread {
    fn into_native_thread(self) -> Result<NativeThread, NativeError> {
        self.into_native_thread_metadata()
            .map(|metadata| metadata.thread)
    }

    fn into_native_thread_metadata(self) -> Result<NativeThreadMetadata, NativeError> {
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
        let status_detail = self.status.into_status_detail()?;

        Ok(NativeThreadMetadata {
            thread: NativeThread {
                id: clean_id,
                project_id,
                title,
                cwd: self.cwd,
                status: status_detail.status.clone(),
                model,
                effort,
                can_accept_direct_input: self.can_accept_direct_input,
            },
            status_detail,
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
        active_flags: Option<Vec<String>>,
    },
    Plain(String),
}

impl WireThreadStatusField {
    fn into_status_detail(self) -> Result<NativeThreadStatusDetail, NativeError> {
        let (raw, tagged, raw_flags) = match self {
            Self::Tagged { kind, active_flags } => (kind, true, active_flags),
            Self::Plain(s) => (s, false, None),
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
        let active_flags_present = raw_flags.is_some();
        let mut active_flags = Vec::new();
        let mut has_invalid_active_flags = false;
        if let Some(flags) = raw_flags {
            if flags.len() > MAX_ACTIVE_FLAGS_COUNT {
                has_invalid_active_flags = true;
            }
            for raw_flag in flags.into_iter().take(MAX_ACTIVE_FLAGS_COUNT) {
                let trimmed = raw_flag.trim();
                if trimmed.is_empty()
                    || trimmed.len() > MAX_ACTIVE_FLAG_CHARS
                    || trimmed.chars().any(char::is_control)
                    || !trimmed
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
                {
                    has_invalid_active_flags = true;
                } else {
                    active_flags.push(trimmed.to_string());
                }
            }
        }
        Ok(NativeThreadStatusDetail {
            status: normalized.to_string(),
            tagged,
            active_flags_present,
            active_flags,
            has_invalid_active_flags,
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_thread_preserves_tagged_active_flags_and_legacy_native_thread_contract() {
        let tagged_waiting: WireThread = serde_json::from_value(json!({
            "id": "0196ed00-0000-7000-8000-00000000000a",
            "projectId": "proj_alpha",
            "name": "Architecture Chat",
            "cwd": "/tmp/workspace/alpha",
            "status": {
                "type": "active",
                "activeFlags": ["waitingOnApproval"]
            },
            "model": "gpt-5.4",
            "reasoningEffort": "high",
            "canAcceptDirectInput": true
        }))
        .expect("deserialize tagged active thread");

        let metadata = tagged_waiting
            .into_native_thread_metadata()
            .expect("map metadata");
        assert_eq!(metadata.thread.status, "active");
        assert_eq!(metadata.thread.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(metadata.thread.effort.as_deref(), Some("high"));
        assert_eq!(metadata.status_detail.status, "active");
        assert!(metadata.status_detail.tagged);
        assert!(metadata.status_detail.active_flags_present);
        assert_eq!(
            metadata.status_detail.active_flags,
            vec!["waitingOnApproval"]
        );
        assert!(!metadata.status_detail.has_invalid_active_flags);

        let plain_active: WireThread = serde_json::from_value(json!({
            "id": "0196ed00-0000-7000-8000-00000000000b",
            "cwd": "/tmp/workspace/alpha",
            "status": "active"
        }))
        .expect("deserialize plain active thread");
        let plain_meta = plain_active
            .into_native_thread_metadata()
            .expect("map plain metadata");
        assert_eq!(plain_meta.thread.status, "active");
        assert!(!plain_meta.status_detail.tagged);
        assert!(!plain_meta.status_detail.active_flags_present);
        assert!(plain_meta.status_detail.active_flags.is_empty());

        let missing_flags: WireThread = serde_json::from_value(json!({
            "id": "0196ed00-0000-7000-8000-00000000000c",
            "cwd": "/tmp/workspace/alpha",
            "status": { "type": "active" }
        }))
        .expect("deserialize tagged active without flags");
        let missing_meta = missing_flags
            .into_native_thread_metadata()
            .expect("map missing flags metadata");
        assert!(missing_meta.status_detail.tagged);
        assert!(!missing_meta.status_detail.active_flags_present);
        assert!(missing_meta.status_detail.active_flags.is_empty());

        let invalid_flag: WireThread = serde_json::from_value(json!({
            "id": "0196ed00-0000-7000-8000-00000000000d",
            "cwd": "/tmp/workspace/alpha",
            "status": {
                "type": "active",
                "activeFlags": ["waitingOnApproval", "bad flag\n"]
            }
        }))
        .expect("deserialize invalid flag token");
        let invalid_meta = invalid_flag
            .into_native_thread_metadata()
            .expect("map invalid flag metadata");
        assert!(invalid_meta.status_detail.has_invalid_active_flags);
        assert_eq!(
            invalid_meta.status_detail.active_flags,
            vec!["waitingOnApproval"]
        );
    }
}
