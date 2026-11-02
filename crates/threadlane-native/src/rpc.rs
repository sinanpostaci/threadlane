use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdout, Command};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{ensure_platform_supported, sanitize_diagnostic_text};

/// Default timeout for a single `codex app-server proxy` JSON-RPC exchange (`10s`).
pub const DEFAULT_PROXY_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum byte length of a single newline-delimited JSON-RPC frame (`1 MiB`).
pub const MAX_RPC_FRAME_BYTES: usize = 1024 * 1024;

/// Maximum total stdout bytes read from a single proxy exchange (`4 MiB`).
pub const MAX_RPC_TOTAL_STDOUT_BYTES: usize = 4 * 1024 * 1024;

/// Maximum stderr bytes drained from a proxy child (`64 KiB`).
pub const MAX_RPC_STDERR_BYTES: usize = 64 * 1024;

/// Maximum number of interleaved notification frames skipped while awaiting a response ID.
pub const MAX_SKIPPED_NOTIFICATIONS: usize = 128;

#[cfg(unix)]
#[allow(unsafe_code)]
mod unix_pgroup {
    use std::fs;
    use std::io;

    pub(crate) fn caller_pgid() -> i32 {
        // SAFETY: `getpgrp` takes no arguments and is always safe to call.
        unsafe { libc::getpgrp() }
    }

    pub(crate) fn signal_process_group(pgid: i32, signal: libc::c_int) -> io::Result<()> {
        if pgid <= 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing to signal non-isolated process group",
            ));
        }
        // SAFETY: `kill(-pgid, signal)` with `pgid > 1` targets the dedicated child process group.
        let rc = unsafe { libc::kill(-pgid, signal) };
        if rc == 0 {
            Ok(())
        } else {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                Err(err)
            }
        }
    }

    pub(crate) fn is_process_group_alive(pgid: i32) -> bool {
        if pgid <= 1 {
            return false;
        }
        // SAFETY: `kill(-pgid, 0)` checks process group existence without delivering a signal.
        let rc = unsafe { libc::kill(-pgid, 0) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ESRCH) {
                return false;
            }
        }
        if let Ok(entries) = fs::read_dir("/proc") {
            let mut saw_any_proc_stat = false;
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name_str) = name.to_str() else {
                    continue;
                };
                if !name_str.bytes().all(|b| b.is_ascii_digit()) {
                    continue;
                }
                let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                    continue;
                };
                saw_any_proc_stat = true;
                let Some(rparen) = stat.rfind(')') else {
                    continue;
                };
                let mut fields = stat[rparen + 1..].split_whitespace();
                let Some(state) = fields.next() else {
                    continue;
                };
                let _ppid = fields.next();
                let Some(pgrp_str) = fields.next() else {
                    continue;
                };
                if pgrp_str.parse::<i32>().ok() == Some(pgid) && !matches!(state, "Z" | "X" | "x") {
                    return true;
                }
            }
            if saw_any_proc_stat {
                return false;
            }
        }
        true
    }

    pub(crate) fn is_pid_alive(pid: u32) -> bool {
        let Ok(pid_i32) = i32::try_from(pid) else {
            return false;
        };
        if pid_i32 <= 1 {
            return false;
        }
        // SAFETY: `kill(pid, 0)` with `pid > 1` checks process existence without sending a signal.
        let rc = unsafe { libc::kill(pid_i32, 0) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ESRCH) {
                return false;
            }
        }
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            let after = stat.rfind(')').map(|idx| stat[idx + 1..].trim_start());
            if after.is_some_and(|s| s.starts_with('Z') || s.starts_with('X') || s.starts_with('x'))
            {
                return false;
            }
        }
        true
    }
}

/// Returns `true` if the OS process `pid` is currently alive (and not a zombie on Linux).
pub fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unix_pgroup::is_pid_alive(pid)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Returns `true` if any process in the Unix process group `pgid` is still alive.
pub fn is_process_group_alive(pgid: i32) -> bool {
    #[cfg(unix)]
    {
        unix_pgroup::is_process_group_alive(pgid)
    }
    #[cfg(not(unix))]
    {
        let _ = pgid;
        false
    }
}

struct ProcessGroupGuard {
    pgid: i32,
    disarmed: bool,
}

impl ProcessGroupGuard {
    fn new(pgid: i32) -> Self {
        Self {
            pgid,
            disarmed: false,
        }
    }

    fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        #[cfg(unix)]
        {
            let _ = unix_pgroup::signal_process_group(self.pgid, libc::SIGTERM);
            let _ = unix_pgroup::signal_process_group(self.pgid, libc::SIGKILL);
        }
    }
}

/// Builds the exact argument vector for `codex app-server proxy [--sock <path>]`.
///
/// Never spawns a new server or passes model/permission overrides.
pub fn build_proxy_args(socket: Option<&Path>) -> Vec<OsString> {
    let mut args = vec![OsString::from("app-server"), OsString::from("proxy")];
    if let Some(sock) = socket {
        args.push(OsString::from("--sock"));
        args.push(sock.as_os_str().to_os_string());
    }
    args
}

/// Builds the standard `initialize` JSON-RPC request frame (`id = 1`, `experimentalApi: false`).
pub(crate) fn build_initialize_request() -> Value {
    json!({
        "id": 1u64,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": "threadlane",
                "title": "Threadlane",
                "version": "0.1.0"
            },
            "capabilities": {
                "experimentalApi": false
            }
        }
    })
}

/// Builds the standard `initialized` JSON-RPC notification frame.
pub(crate) fn build_initialized_notification() -> Value {
    json!({
        "method": "initialized"
    })
}

/// Builds a numbered JSON-RPC request frame (`id`, `method`, `params`).
pub(crate) fn build_rpc_request(id: u64, method: &str, params: Value) -> Value {
    json!({
        "id": id,
        "method": method,
        "params": params
    })
}

/// Detailed failure classification from a proxy JSON-RPC call, distinguishing whether
/// the target request frame had already been written to the proxy transport.
#[derive(Debug, Clone)]
pub struct ProxyCallError {
    /// Underlying structured [`NativeError`].
    pub error: NativeError,
    /// `true` if the target request frame (after `initialize`) was written (or partially written)
    /// to the proxy stdin before the error occurred.
    pub request_written_to_wire: bool,
}

#[derive(Debug, Deserialize)]
struct WireRpcErrorPayload {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct WireRpcEnvelope {
    #[serde(default)]
    id: Option<u64>,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<WireRpcErrorPayload>,
}

/// Result of the `initialize` handshake from `codex app-server proxy`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    /// Server user agent string.
    pub user_agent: String,
    /// Server `codexHome` path.
    #[serde(default)]
    pub codex_home: Option<String>,
    /// Platform family reported by app-server.
    #[serde(default)]
    pub platform_family: Option<String>,
    /// Platform OS reported by app-server.
    #[serde(default)]
    pub platform_os: Option<String>,
}

struct BoundedFrameReader {
    stdout: ChildStdout,
    buf: Vec<u8>,
    total_read: usize,
    max_frame_bytes: usize,
    max_total_bytes: usize,
}

impl BoundedFrameReader {
    fn new(stdout: ChildStdout, max_frame_bytes: usize, max_total_bytes: usize) -> Self {
        Self {
            stdout,
            buf: Vec::with_capacity(4096),
            total_read: 0,
            max_frame_bytes,
            max_total_bytes,
        }
    }

    async fn next_frame(&mut self) -> Result<Option<Vec<u8>>, NativeError> {
        loop {
            if let Some(newline_pos) = self.buf.iter().position(|&b| b == b'\n') {
                if newline_pos > self.max_frame_bytes {
                    return Err(NativeError::new(
                        NativeErrorCode::OutputOverflow,
                        format!(
                            "proxy JSON-RPC frame exceeded maximum size of {} bytes",
                            self.max_frame_bytes
                        ),
                    ));
                }
                let mut line: Vec<u8> = self.buf.drain(..=newline_pos).collect();
                if line.last() == Some(&b'\n') {
                    line.pop();
                }
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                return Ok(Some(line));
            }

            if self.buf.len() > self.max_frame_bytes {
                return Err(NativeError::new(
                    NativeErrorCode::OutputOverflow,
                    format!(
                        "proxy JSON-RPC line exceeded maximum size of {} bytes without newline",
                        self.max_frame_bytes
                    ),
                ));
            }

            let mut chunk = [0u8; 8192];
            let n = self.stdout.read(&mut chunk).await.map_err(|err| {
                NativeError::new(
                    NativeErrorCode::TransportUnavailable,
                    format!(
                        "failed reading from codex app-server proxy stdout: {}",
                        sanitize_diagnostic_text(&err.to_string())
                    ),
                )
            })?;

            if n == 0 {
                if self.buf.is_empty() || self.buf.iter().all(|b| b.is_ascii_whitespace()) {
                    self.buf.clear();
                    return Ok(None);
                }
                if self.buf.len() > self.max_frame_bytes {
                    return Err(NativeError::new(
                        NativeErrorCode::OutputOverflow,
                        format!(
                            "proxy trailing frame exceeded maximum size of {} bytes",
                            self.max_frame_bytes
                        ),
                    ));
                }
                let line = std::mem::take(&mut self.buf);
                return Ok(Some(line));
            }

            self.total_read = self.total_read.saturating_add(n);
            if self.total_read > self.max_total_bytes {
                return Err(NativeError::new(
                    NativeErrorCode::OutputOverflow,
                    format!(
                        "proxy total stdout exceeded maximum budget of {} bytes",
                        self.max_total_bytes
                    ),
                ));
            }
            self.buf.extend_from_slice(&chunk[..n]);
            if let Some(newline_pos) = self.buf.iter().position(|&b| b == b'\n') {
                if newline_pos > self.max_frame_bytes {
                    return Err(NativeError::new(
                        NativeErrorCode::OutputOverflow,
                        format!(
                            "proxy JSON-RPC frame exceeded maximum size of {} bytes",
                            self.max_frame_bytes
                        ),
                    ));
                }
            } else if self.buf.len() > self.max_frame_bytes {
                return Err(NativeError::new(
                    NativeErrorCode::OutputOverflow,
                    format!(
                        "proxy JSON-RPC line exceeded maximum size of {} bytes without newline",
                        self.max_frame_bytes
                    ),
                ));
            }
        }
    }
}

/// Executes a single initialized JSON-RPC request against `codex app-server proxy [--sock ...]`,
/// enforcing bounded frame sizes, timeouts, and process-group cleanup.
pub async fn execute_proxy_rpc<Resp>(
    executable: &Path,
    socket: Option<&Path>,
    timeout: Duration,
    max_frame_bytes: usize,
    method: &str,
    params: Value,
) -> Result<Resp, ProxyCallError>
where
    Resp: DeserializeOwned,
{
    if let Err(err) = ensure_platform_supported() {
        return Err(ProxyCallError {
            error: err,
            request_written_to_wire: false,
        });
    }

    let args = build_proxy_args(socket);
    let mut cmd = Command::new(executable);
    cmd.args(&args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd.spawn().map_err(|err| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            format!(
                "failed to spawn codex app-server proxy: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        ),
        request_written_to_wire: false,
    })?;

    let child_pid = child.id().ok_or_else(|| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "spawned codex app-server proxy had no OS process ID",
        ),
        request_written_to_wire: false,
    })?;

    let pgid = i32::try_from(child_pid).map_err(|_| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "spawned codex app-server proxy PID exceeded i32 range",
        ),
        request_written_to_wire: false,
    })?;

    #[cfg(unix)]
    if pgid <= 1 || pgid == unix_pgroup::caller_pgid() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(ProxyCallError {
            error: NativeError::new(
                NativeErrorCode::SecurityViolation,
                "spawned codex app-server proxy did not enter an isolated process group",
            ),
            request_written_to_wire: false,
        });
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let mut stdin = child.stdin.take().ok_or_else(|| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "failed to capture codex app-server proxy stdin pipe",
        ),
        request_written_to_wire: false,
    })?;
    let stdout = child.stdout.take().ok_or_else(|| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "failed to capture codex app-server proxy stdout pipe",
        ),
        request_written_to_wire: false,
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| ProxyCallError {
        error: NativeError::new(
            NativeErrorCode::TransportUnavailable,
            "failed to capture codex app-server proxy stderr pipe",
        ),
        request_written_to_wire: false,
    })?;

    let stderr_handle = tokio::spawn(async move {
        let mut buf = Vec::with_capacity(1024);
        let mut chunk = [0u8; 4096];
        while buf.len() < MAX_RPC_STDERR_BYTES {
            match stderr.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let take = (MAX_RPC_STDERR_BYTES - buf.len()).min(n);
                    buf.extend_from_slice(&chunk[..take]);
                    if take < n {
                        break;
                    }
                }
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    });

    let mut reader = BoundedFrameReader::new(
        stdout,
        max_frame_bytes,
        MAX_RPC_TOTAL_STDOUT_BYTES.max(max_frame_bytes),
    );
    let mut request_written_to_wire = false;

    let exchange_res = tokio::time::timeout(timeout, async {
        // 1. Send `initialize` request (id = 1, omitting `jsonrpc` per verified proxy schema).
        let init_req = build_initialize_request();
        write_frame(&mut stdin, &init_req).await?;

        // 2. Await `initialize` response (id = 1).
        let init_val = await_response_by_id(&mut reader, 1).await?;
        let _init_res: InitializeResult = serde_json::from_value(init_val).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "invalid initialize response from codex app-server proxy: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;

        // 3. Send `initialized` notification.
        let initialized_notif = build_initialized_notification();
        write_frame(&mut stdin, &initialized_notif).await?;

        // 4. Send target request (id = 2).
        let rpc_req = build_rpc_request(2, method, params);
        let mut line = serde_json::to_vec(&rpc_req).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "failed to serialize proxy request: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;
        line.push(b'\n');

        // Mark wire write before writing the target request bytes so any mid-write or post-write
        // failure is conservatively treated as potentially delivered.
        request_written_to_wire = true;
        stdin.write_all(&line).await.map_err(|err| {
            NativeError::new(
                NativeErrorCode::AmbiguousTransportFailure,
                format!(
                    "failed writing request to codex app-server proxy: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;
        stdin.flush().await.map_err(|err| {
            NativeError::new(
                NativeErrorCode::AmbiguousTransportFailure,
                format!(
                    "failed flushing request to codex app-server proxy: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;

        // 5. Await response (id = 2).
        let result_val = await_response_by_id(&mut reader, 2).await?;
        serde_json::from_value::<Resp>(result_val).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "invalid response payload for `{method}`: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })
    })
    .await;

    // Close stdin and ensure the proxy process group is terminated and reaped.
    drop(stdin);
    terminate_and_reap_group(pgid, &mut child).await;
    if !is_process_group_alive(pgid) {
        pgroup_guard.disarm();
    }

    let stderr_summary = match tokio::time::timeout(Duration::from_millis(100), stderr_handle).await
    {
        Ok(Ok(text)) => sanitize_diagnostic_text(&text),
        _ => String::new(),
    };

    match exchange_res {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(mut err)) => {
            if request_written_to_wire && err.code == NativeErrorCode::TransportUnavailable {
                err.code = NativeErrorCode::AmbiguousTransportFailure;
            }
            if !stderr_summary.is_empty()
                && stderr_summary != "native operation error"
                && matches!(
                    err.code,
                    NativeErrorCode::TransportUnavailable
                        | NativeErrorCode::AmbiguousTransportFailure
                )
            {
                err.message = format!("{} (stderr: {})", err.message, stderr_summary);
            }
            Err(ProxyCallError {
                error: err,
                request_written_to_wire,
            })
        }
        Err(_elapsed) => Err(ProxyCallError {
            error: NativeError::new(
                NativeErrorCode::TimedOut,
                format!(
                    "codex app-server proxy request `{method}` timed out after {} ms",
                    timeout.as_millis()
                ),
            ),
            request_written_to_wire,
        }),
    }
}

async fn write_frame(
    stdin: &mut tokio::process::ChildStdin,
    payload: &Value,
) -> Result<(), NativeError> {
    let mut bytes = serde_json::to_vec(payload).map_err(|err| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "failed to encode JSON-RPC frame: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        )
    })?;
    bytes.push(b'\n');
    stdin.write_all(&bytes).await.map_err(|err| {
        NativeError::new(
            NativeErrorCode::TransportUnavailable,
            format!(
                "failed writing handshake to codex app-server proxy: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        )
    })?;
    stdin.flush().await.map_err(|err| {
        NativeError::new(
            NativeErrorCode::TransportUnavailable,
            format!(
                "failed flushing handshake to codex app-server proxy: {}",
                sanitize_diagnostic_text(&err.to_string())
            ),
        )
    })?;
    Ok(())
}

async fn await_response_by_id(
    reader: &mut BoundedFrameReader,
    expected_id: u64,
) -> Result<Value, NativeError> {
    let mut skipped = 0usize;
    loop {
        let Some(frame) = reader.next_frame().await? else {
            return Err(NativeError::new(
                NativeErrorCode::TransportUnavailable,
                format!(
                    "codex app-server proxy closed stdout before returning response id {expected_id}"
                ),
            ));
        };

        let envelope: WireRpcEnvelope = serde_json::from_slice(&frame).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "malformed JSON-RPC frame from codex app-server proxy: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;

        match envelope.id {
            Some(id) if id == expected_id => {
                if let Some(rpc_err) = envelope.error {
                    return Err(NativeError::rpc(
                        rpc_err.code,
                        sanitize_diagnostic_text(&rpc_err.message),
                    ));
                }
                return envelope.result.ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        format!(
                            "codex app-server proxy response id {expected_id} contained neither result nor error"
                        ),
                    )
                });
            }
            Some(_other_id) => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "codex app-server proxy returned unexpected response ID",
                ));
            }
            None => {
                skipped = skipped.saturating_add(1);
                if skipped > MAX_SKIPPED_NOTIFICATIONS {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "exceeded maximum skipped notification frames while awaiting proxy response",
                    ));
                }
            }
        }
    }
}

async fn terminate_and_reap_group(pgid: i32, child: &mut Child) {
    #[cfg(unix)]
    {
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGTERM);
    }

    for _ in 0..10 {
        if let Ok(Some(_)) = child.try_wait() {
            if !is_process_group_alive(pgid) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    #[cfg(unix)]
    {
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGKILL);
    }

    let _ = child.start_kill();
    let _ = tokio::time::timeout(Duration::from_millis(250), child.wait()).await;

    #[cfg(unix)]
    {
        for _ in 0..20 {
            if !unix_pgroup::is_process_group_alive(pgid) {
                break;
            }
            let _ = unix_pgroup::signal_process_group(pgid, libc::SIGKILL);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
