use std::time::Duration;

#[cfg(target_os = "linux")]
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use threadlane::NativeReconnectConfig;
#[cfg(target_os = "linux")]
use tokio_tungstenite::tungstenite::{
    protocol::WebSocketConfig, Error as WsError, Message as WsMessage,
};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{sanitize_diagnostic_text, validate_bounded_identifier};
use crate::pairing::{CONNECTION_IDENTITY_TOOL, MCP_SERVER_NAME};
#[cfg(target_os = "linux")]
use crate::rpc::{
    build_initialize_request, build_initialized_notification, build_rpc_request, InitializeResult,
    MAX_RPC_TOTAL_STDOUT_BYTES, MAX_SKIPPED_NOTIFICATIONS,
};

const MAX_ID_CHARS: usize = 128;

/// Maximum aggregate bytes of interleaved notifications skipped during a single managed WebSocket
/// RPC exchange (`256 KiB`).
pub const MAX_WS_AGGREGATE_NOTIFICATION_BYTES: usize = 256 * 1024;

/// Exact set of JSON-RPC methods permitted on the read-only managed WebSocket transport.
pub const ALLOWED_WS_READONLY_METHODS: [&str; 5] = [
    "thread/list",
    "thread/loaded/list",
    "thread/read",
    "mcpServerStatus/list",
    "mcpServer/tool/call",
];

/// Validates that `(method, params)` satisfies the strict read-only managed WebSocket allowlist
/// before any socket connection or wire frame is sent.
///
/// Permitted requests:
/// - `thread/list` (without experimental `projectId`)
/// - `thread/read` (with `includeTurns: false` and bounded `threadId`)
/// - `mcpServerStatus/list` (with bounded `threadId`, optional bounded `serverName`, and
///   `detail: "toolsAndAuthOnly"`)
/// - `mcpServer/tool/call` only for `server: "threadlane"`, `tool: "connection_identity"`,
///   and empty object `arguments: {}` (no `_meta`)
///
/// All other methods, tools, or arguments (including `turn/start`, `turn/steer`, `thread/resume`,
/// and enrollment/status tools) are rejected with [`NativeErrorCode::UnsupportedCapability`].
pub fn validate_ws_readonly_request(method: &str, params: &Value) -> Result<(), NativeError> {
    let Some(obj) = params.as_object() else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "managed WebSocket JSON-RPC params must be a JSON object",
        ));
    };

    match method {
        "thread/loaded/list" => {
            if obj
                .keys()
                .any(|key| !matches!(key.as_str(), "limit" | "cursor"))
                || !obj
                    .get("limit")
                    .and_then(Value::as_u64)
                    .is_some_and(|value| (1..=100).contains(&value))
            {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "loaded-thread discovery must be bounded",
                ));
            }
            if let Some(cursor) = obj.get("cursor") {
                if !cursor.as_str().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 512
                        && value.trim() == value
                        && !value.chars().any(char::is_control)
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "loaded-thread cursor is invalid",
                    ));
                }
            }
            Ok(())
        }
        "thread/list" => {
            if obj.contains_key("projectId") {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket thread/list prohibits experimental projectId wire parameter",
                ));
            }
            for key in obj.keys() {
                if !matches!(
                    key.as_str(),
                    "sourceKinds"
                        | "limit"
                        | "sortKey"
                        | "sortDirection"
                        | "useStateDbOnly"
                        | "cwd"
                        | "cursor"
                ) {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "managed WebSocket thread/list received an unsupported parameter field",
                    ));
                }
            }
            if obj.get("useStateDbOnly") != Some(&Value::Bool(true))
                || !obj
                    .get("limit")
                    .and_then(Value::as_u64)
                    .is_some_and(|value| (1..=100).contains(&value))
            {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket thread/list requires bounded state-only discovery",
                ));
            }
            if let Some(cursor) = obj.get("cursor") {
                if !cursor.as_str().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 512
                        && value.trim() == value
                        && !value.chars().any(char::is_control)
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "managed WebSocket thread/list cursor is invalid",
                    ));
                }
            }
            Ok(())
        }
        "thread/read" => {
            let Some(tid) = obj.get("threadId").and_then(Value::as_str) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "managed WebSocket thread/read requires a string threadId",
                ));
            };
            let _ = validate_bounded_identifier(tid, "threadId", MAX_ID_CHARS)?;

            if obj.get("includeTurns") != Some(&Value::Bool(false)) {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket thread/read requires includeTurns: false",
                ));
            }
            if obj.len() != 2 {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket thread/read contains unsupported parameters",
                ));
            }
            Ok(())
        }
        "mcpServerStatus/list" => {
            let Some(tid) = obj.get("threadId").and_then(Value::as_str) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "managed WebSocket mcpServerStatus/list requires a string threadId",
                ));
            };
            let _ = validate_bounded_identifier(tid, "threadId", MAX_ID_CHARS)?;

            if obj.get("serverName").and_then(Value::as_str) != Some(MCP_SERVER_NAME)
                || obj.get("detail").and_then(Value::as_str) != Some("toolsAndAuthOnly")
            {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket status is restricted to Threadlane tools and auth",
                ));
            }
            if let Some(limit_val) = obj.get("limit") {
                if !limit_val
                    .as_u64()
                    .is_some_and(|value| (1..=20).contains(&value))
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "managed WebSocket status limit must be between 1 and 20",
                    ));
                }
            }
            if let Some(cursor_val) = obj.get("cursor") {
                if !cursor_val.as_str().is_some_and(|value| {
                    !value.is_empty()
                        && value.len() <= 512
                        && value.trim() == value
                        && !value.chars().any(char::is_control)
                }) {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "managed WebSocket status cursor is invalid",
                    ));
                }
            }
            for key in obj.keys() {
                if !matches!(
                    key.as_str(),
                    "threadId" | "serverName" | "detail" | "limit" | "cursor"
                ) {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "managed WebSocket mcpServerStatus/list contains unsupported parameters",
                    ));
                }
            }
            Ok(())
        }
        "mcpServer/tool/call" => {
            if obj.contains_key("_meta") {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket mcpServer/tool/call prohibits _meta parameter",
                ));
            }
            let Some(tid) = obj.get("threadId").and_then(Value::as_str) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "managed WebSocket mcpServer/tool/call requires a string threadId",
                ));
            };
            let _ = validate_bounded_identifier(tid, "threadId", MAX_ID_CHARS)?;

            let Some(server) = obj.get("server").and_then(Value::as_str) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "managed WebSocket mcpServer/tool/call requires a string server name",
                ));
            };
            let _ = validate_bounded_identifier(server, "server", MAX_ID_CHARS)?;
            if server != MCP_SERVER_NAME {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket mcpServer/tool/call is restricted to server \"threadlane\"",
                ));
            }

            let Some(tool) = obj.get("tool").and_then(Value::as_str) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "managed WebSocket mcpServer/tool/call requires a string tool name",
                ));
            };
            let _ = validate_bounded_identifier(tool, "tool", MAX_ID_CHARS)?;
            if tool != CONNECTION_IDENTITY_TOOL {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket mcpServer/tool/call is restricted to tool \"connection_identity\"",
                ));
            }

            let Some(args_obj) = obj.get("arguments").and_then(Value::as_object) else {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket connection_identity call requires an object arguments field",
                ));
            };
            if !args_obj.is_empty() {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket connection_identity call requires empty object arguments {}",
                ));
            }

            if obj.len() != 4 {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "managed WebSocket mcpServer/tool/call contains unsupported parameter fields",
                ));
            }
            Ok(())
        }
        _ => Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "managed WebSocket transport is strictly read-only and rejects method `{}`",
                sanitize_diagnostic_text(method)
            ),
        )),
    }
}

/// Executes a single read-only initialized JSON-RPC request over the verified managed Unix
/// WebSocket endpoint resolved via [`NativeReconnectConfig`].
///
/// Enforces:
/// 1. Strict read-only method/tool/argument allowlist before opening the socket or sending frames.
/// 2. Linux process-tree/executable/UID/permission/socket/peer-credential (`SO_PEERCRED`) checks.
/// 3. Bounded timeout, single-frame limit, aggregate notification limit, and total byte budget.
/// 4. Strict response ID correlation (`initialize` = `1`, target request = `2`) and clean close.
pub async fn execute_ws_rpc<Resp>(
    reconnect: &NativeReconnectConfig,
    timeout: Duration,
    max_frame_bytes: usize,
    method: &str,
    params: Value,
) -> Result<Resp, NativeError>
where
    Resp: DeserializeOwned,
{
    execute_ws_rpc_for_backend(reconnect, None, timeout, max_frame_bytes, method, params).await
}

/// Executes a read-only request while requiring an optional, trusted MCP-parent backend PID.
pub async fn execute_ws_rpc_for_backend<Resp>(
    reconnect: &NativeReconnectConfig,
    expected_backend_pid: Option<u32>,
    timeout: Duration,
    max_frame_bytes: usize,
    method: &str,
    params: Value,
) -> Result<Resp, NativeError>
where
    Resp: DeserializeOwned,
{
    validate_ws_readonly_request(method, &params)?;
    if timeout.is_zero() || timeout > Duration::from_secs(120) {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "managed WebSocket timeout must be between 1ms and 120s",
        ));
    }
    if !(1024..=4 * 1024 * 1024).contains(&max_frame_bytes) {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "managed WebSocket frame budget must be between 1 KiB and 4 MiB",
        ));
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (reconnect, expected_backend_pid, timeout, max_frame_bytes);
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "managed native WebSocket transport is only supported on Linux",
        ))
    }

    #[cfg(target_os = "linux")]
    {
        let exchange = tokio::time::timeout(timeout, async {
            let stream = reconnect
                .connect_verified_stream_for_backend(expected_backend_pid)
                .await
                .map_err(map_reconnect_error)?;

            let mut ws_config = WebSocketConfig::default();
            ws_config.max_frame_size = Some(max_frame_bytes);
            ws_config.max_message_size = Some(max_frame_bytes);

            let (mut ws, _resp) = tokio_tungstenite::client_async_with_config(
                "ws://localhost/",
                stream,
                Some(ws_config),
            )
            .await
            .map_err(|err| {
                map_ws_error(
                    err,
                    max_frame_bytes,
                    "failed WebSocket handshake on managed native endpoint",
                )
            })?;

            let mut state = WsReaderState::new(max_frame_bytes);

            // 1. Send `initialize` request (id = 1).
            let init_req = build_initialize_request();
            send_ws_json(&mut ws, &init_req, max_frame_bytes).await?;

            // 2. Await `initialize` response (id = 1).
            let init_val = match await_ws_response_by_id(&mut ws, 1, &mut state).await {
                Ok(val) => val,
                Err(err) => {
                    close_ws_cleanly(&mut ws).await;
                    return Err(err);
                }
            };
            let _init_res: InitializeResult = match serde_json::from_value(init_val) {
                Ok(res) => res,
                Err(_) => {
                    close_ws_cleanly(&mut ws).await;
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "invalid initialize response payload from managed native WebSocket",
                    ));
                }
            };

            // 3. Send `initialized` notification.
            let initialized_notif = build_initialized_notification();
            if let Err(err) = send_ws_json(&mut ws, &initialized_notif, max_frame_bytes).await {
                close_ws_cleanly(&mut ws).await;
                return Err(err);
            }

            // 4. Send target read-only request (id = 2).
            let rpc_req = build_rpc_request(2, method, params);
            if let Err(err) = send_ws_json(&mut ws, &rpc_req, max_frame_bytes).await {
                close_ws_cleanly(&mut ws).await;
                return Err(err);
            }

            // 5. Await target response (id = 2) and close transport cleanly.
            let result_val_res = await_ws_response_by_id(&mut ws, 2, &mut state).await;
            close_ws_cleanly(&mut ws).await;
            let mut result_val = result_val_res?;
            strip_thread_previews(method, &mut result_val)?;

            serde_json::from_value::<Resp>(result_val).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    format!(
                        "invalid response payload for `{}` from managed native WebSocket",
                        sanitize_diagnostic_text(method)
                    ),
                )
            })
        })
        .await;

        match exchange {
            Ok(res) => res,
            Err(_elapsed) => Err(NativeError::new(
                NativeErrorCode::TimedOut,
                format!(
                    "managed native WebSocket request `{}` timed out after {} ms",
                    sanitize_diagnostic_text(method),
                    timeout.as_millis()
                ),
            )),
        }
    }
}

fn strip_thread_previews(method: &str, result: &mut Value) -> Result<(), NativeError> {
    fn sanitize_thread(thread: &mut Value) -> Result<(), NativeError> {
        let object = thread.as_object_mut().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "invalid native thread metadata",
            )
        })?;
        if object
            .get("turns")
            .and_then(Value::as_array)
            .is_some_and(|turns| !turns.is_empty())
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "native metadata unexpectedly included conversation turns",
            ));
        }
        object.remove("preview");
        Ok(())
    }

    match method {
        "thread/list" => {
            let threads = result
                .get_mut("data")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| {
                    NativeError::new(NativeErrorCode::ProtocolError, "invalid native thread list")
                })?;
            for thread in threads {
                sanitize_thread(thread)?;
            }
        }
        "thread/read" => {
            let thread = result.get_mut("thread").ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "invalid native thread response",
                )
            })?;
            sanitize_thread(thread)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct WsReaderState {
    max_frame_bytes: usize,
    max_total_bytes: usize,
    max_aggregate_notification_bytes: usize,
    total_bytes: usize,
    aggregate_notification_bytes: usize,
    skipped_notifications: usize,
    control_frames_seen: usize,
}

#[cfg(target_os = "linux")]
impl WsReaderState {
    fn new(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes,
            max_total_bytes: MAX_RPC_TOTAL_STDOUT_BYTES.max(max_frame_bytes),
            max_aggregate_notification_bytes: max_frame_bytes
                .min(MAX_WS_AGGREGATE_NOTIFICATION_BYTES),
            total_bytes: 0,
            aggregate_notification_bytes: 0,
            skipped_notifications: 0,
            control_frames_seen: 0,
        }
    }
}

#[cfg(target_os = "linux")]
async fn send_ws_json(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    payload: &Value,
    max_frame_bytes: usize,
) -> Result<(), NativeError> {
    let serialized = serde_json::to_string(payload).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize managed WebSocket JSON-RPC frame",
        )
    })?;
    if serialized.len() > max_frame_bytes {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "outbound managed WebSocket JSON-RPC frame exceeded max_frame_bytes",
        ));
    }
    ws.send(WsMessage::Text(serialized.into()))
        .await
        .map_err(|err| {
            map_ws_error(
                err,
                max_frame_bytes,
                "failed sending JSON-RPC frame on managed native WebSocket",
            )
        })?;
    ws.flush().await.map_err(|err| {
        map_ws_error(
            err,
            max_frame_bytes,
            "failed flushing JSON-RPC frame on managed native WebSocket",
        )
    })?;
    Ok(())
}

#[cfg(target_os = "linux")]
async fn await_ws_response_by_id(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    expected_id: u64,
    state: &mut WsReaderState,
) -> Result<Value, NativeError> {
    loop {
        let Some(frame_res) = ws.next().await else {
            return Err(NativeError::new(
                NativeErrorCode::TransportUnavailable,
                format!(
                    "managed native WebSocket closed before returning response id {expected_id}"
                ),
            ));
        };

        let msg = frame_res.map_err(|err| {
            map_ws_error(
                err,
                state.max_frame_bytes,
                "failed reading frame from managed native WebSocket",
            )
        })?;

        let text = match msg {
            WsMessage::Text(t) => t,
            WsMessage::Close(_) => {
                return Err(NativeError::new(
                    NativeErrorCode::TransportUnavailable,
                    format!(
                        "managed native WebSocket closed by peer before returning response id {expected_id}"
                    ),
                ));
            }
            WsMessage::Binary(_) => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket received unexpected binary frame",
                ));
            }
            WsMessage::Ping(data) | WsMessage::Pong(data) => {
                state.control_frames_seen = state.control_frames_seen.saturating_add(1);
                if state.control_frames_seen > MAX_SKIPPED_NOTIFICATIONS {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "exceeded maximum control frames on managed native WebSocket",
                    ));
                }
                state.total_bytes = state.total_bytes.saturating_add(data.len());
                if state.total_bytes > state.max_total_bytes {
                    return Err(NativeError::new(
                        NativeErrorCode::OutputOverflow,
                        "managed native WebSocket total bytes exceeded maximum budget",
                    ));
                }
                continue;
            }
            WsMessage::Frame(_) => continue,
        };

        let frame_len = text.len();
        if frame_len > state.max_frame_bytes {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!(
                    "managed native WebSocket frame exceeded maximum size of {} bytes",
                    state.max_frame_bytes
                ),
            ));
        }

        state.total_bytes = state.total_bytes.saturating_add(frame_len);
        if state.total_bytes > state.max_total_bytes {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!(
                    "managed native WebSocket total bytes exceeded maximum budget of {} bytes",
                    state.max_total_bytes
                ),
            ));
        }

        let raw_val: Value = serde_json::from_str(text.as_ref()).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "malformed JSON-RPC frame from managed native WebSocket",
            )
        })?;

        let Some(obj) = raw_val.as_object() else {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket JSON-RPC frame must be a JSON object",
            ));
        };

        if let Some(method_val) = obj.get("method") {
            if obj.contains_key("id") {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket rejected server-initiated request frame",
                ));
            }
            if obj.contains_key("result") || obj.contains_key("error") {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket notification frame must not contain result or error",
                ));
            }
            let Some(method_str) = method_val.as_str() else {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket notification method must be a string",
                ));
            };
            if method_str.trim().is_empty() {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket notification method must not be empty",
                ));
            }

            state.skipped_notifications = state.skipped_notifications.saturating_add(1);
            if state.skipped_notifications > MAX_SKIPPED_NOTIFICATIONS {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "exceeded maximum skipped notification frames while awaiting managed WebSocket response",
                ));
            }

            state.aggregate_notification_bytes =
                state.aggregate_notification_bytes.saturating_add(frame_len);
            if state.aggregate_notification_bytes > state.max_aggregate_notification_bytes {
                return Err(NativeError::new(
                    NativeErrorCode::OutputOverflow,
                    format!(
                        "managed native WebSocket aggregate notification bytes exceeded budget of {} bytes",
                        state.max_aggregate_notification_bytes
                    ),
                ));
            }
            continue;
        }

        let Some(id_val) = obj.get("id") else {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket frame missing both method and response id",
            ));
        };
        let Some(id) = id_val.as_u64() else {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket response frame had non-integer or null id",
            ));
        };
        if id != expected_id {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket returned unexpected response ID",
            ));
        }

        let has_non_null_error = obj.get("error").is_some_and(|v| !v.is_null());
        let has_non_null_result = obj.get("result").is_some_and(|v| !v.is_null());
        if has_non_null_error && has_non_null_result {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket response contained both result and error",
            ));
        }

        if let Some(err_val) = obj.get("error").filter(|v| !v.is_null()) {
            let Some(err_obj) = err_val.as_object() else {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "malformed JSON-RPC error payload from managed native WebSocket",
                ));
            };
            let Some(code) = err_obj.get("code").and_then(Value::as_i64) else {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket JSON-RPC error missing integer code",
                ));
            };
            if err_obj.get("message").and_then(Value::as_str).is_none() {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "managed native WebSocket JSON-RPC error missing string message",
                ));
            }
            return Err(NativeError::rpc(
                code,
                "managed native WebSocket JSON-RPC error response",
            ));
        }

        if let Some(result_val) = obj.get("result") {
            return Ok(result_val.clone());
        }

        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "managed native WebSocket response id {expected_id} contained neither result nor error"
            ),
        ));
    }
}

#[cfg(target_os = "linux")]
async fn close_ws_cleanly(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>) {
    let _ = tokio::time::timeout(Duration::from_millis(150), async {
        let _ = ws.send(WsMessage::Close(None)).await;
        let _ = ws.close(None).await;
        let _ = ws.flush().await;
    })
    .await;
}

/// Maps a [`threadlane::Error`] from [`NativeReconnectConfig`] validation or resolution into a
/// redacted, path-free [`NativeError`].
pub(crate) fn map_reconnect_error(err: threadlane::Error) -> NativeError {
    match err {
        threadlane::Error::UnsupportedPlatform(msg) => NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            sanitize_path_free_reason(
                &msg,
                "managed native WebSocket transport is only supported on Linux",
            ),
        ),
        threadlane::Error::InvalidConfig(msg) => NativeError::new(
            NativeErrorCode::InvalidConfig,
            sanitize_path_free_reason(&msg, "invalid managed native WebSocket configuration"),
        ),
        threadlane::Error::SecurityViolation(msg) => NativeError::new(
            NativeErrorCode::SecurityViolation,
            sanitize_path_free_reason(
                &msg,
                "managed native WebSocket endpoint failed security validation",
            ),
        ),
        threadlane::Error::Transport(msg) => NativeError::new(
            NativeErrorCode::TransportUnavailable,
            sanitize_path_free_reason(&msg, "managed native WebSocket endpoint is unavailable"),
        ),
        _ => NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "managed native WebSocket endpoint error",
        ),
    }
}

fn sanitize_path_free_reason(raw: &str, fallback: &'static str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains('`')
        || trimmed.contains("nce_")
        || trimmed.contains("epc_")
        || trimmed.contains("op_")
        || trimmed.chars().any(char::is_control)
    {
        return fallback.to_string();
    }
    let clean = sanitize_diagnostic_text(trimmed);
    if clean.is_empty() || clean == "native operation error" {
        fallback.to_string()
    } else {
        clean
    }
}

#[cfg(target_os = "linux")]
fn map_ws_error(err: WsError, max_frame_bytes: usize, context: &'static str) -> NativeError {
    match err {
        WsError::Capacity(_) => NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!(
                "managed native WebSocket frame exceeded maximum size of {max_frame_bytes} bytes"
            ),
        ),
        WsError::ConnectionClosed | WsError::AlreadyClosed | WsError::Io(_) => {
            NativeError::new(NativeErrorCode::TransportUnavailable, context)
        }
        WsError::Protocol(_) | WsError::Utf8(_) | WsError::Http(_) | WsError::HttpFormat(_) => {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "managed native WebSocket protocol error",
            )
        }
        _ => NativeError::new(NativeErrorCode::TransportUnavailable, context),
    }
}

#[cfg(test)]
mod privacy_tests {
    use super::strip_thread_previews;
    use serde_json::json;

    #[test]
    fn metadata_responses_do_not_expose_previews_or_nonempty_turns() {
        let mut list = json!({"data": [{"id": "one", "preview": "private text"}]});
        strip_thread_previews("thread/list", &mut list).expect("list is valid");
        assert!(list["data"][0].get("preview").is_none());
        let mut thread = json!({"thread": {"id": "one", "preview": "private", "turns": []}});
        strip_thread_previews("thread/read", &mut thread).expect("thread is valid");
        assert!(thread["thread"].get("preview").is_none());
        let mut unexpected = json!({"thread": {"id": "one", "turns": [{"text": "private"}]}});
        assert!(strip_thread_previews("thread/read", &mut unexpected).is_err());
    }
}
