use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use threadlane_core::{WakeupClaim, WakeupOutcome, MAX_ID_LEN};
use tokio::sync::watch;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use uuid::Uuid;

use crate::codex::{ApprovedSessionWakeupContext, CodexWakeupConfig};
use crate::descriptor::{ensure_platform_supported, validate_no_symlink_components};
use crate::error::Error;

/// Supported schema version for [`NativeToolOutputConfig`].
pub const NATIVE_TOOL_OUTPUT_CONFIG_SCHEMA_VERSION: u32 = 1;

/// Maximum permitted byte size (`16 KiB`) for a private [`NativeToolOutputConfig`] JSON file.
pub const MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES: u64 = 16_384;

/// Maximum permitted byte size (`1 MiB`) for a single WebSocket frame or JSON-RPC message.
pub const MAX_NATIVE_WS_FRAME_BYTES: usize = 1_048_576;

/// Maximum permitted cumulative incoming response byte budget (`4 MiB`) across one native exchange.
pub const MAX_NATIVE_WS_CUMULATIVE_BYTES: usize = 4_194_304;

/// Maximum permitted incoming WebSocket frame count (`512`) across one native exchange.
pub const MAX_NATIVE_WS_FRAMES: usize = 512;

/// Fixed tool name submitted in `turn/start.toolOutput`.
pub const NATIVE_TOOL_OUTPUT_NAME: &str = "threadlane_events_available";

/// Fixed tool namespace submitted in `turn/start.toolOutput`.
pub const NATIVE_TOOL_OUTPUT_NAMESPACE: &str = "threadlane";

const NATIVE_WS_URL: &str = "ws://localhost/";
const MAX_CONTROL_PING_BYTES: usize = 125;

/// Explicit operator-selected native tool-output wake-up transport configuration.
///
/// This configuration is an operator-selected target scope only and is not a per-call
/// native identity proof. Every wake-up attempt revalidates the physical socket path,
/// parent directory ownership/permissions, canonical project root, and target thread identity.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolOutputConfig {
    /// Configuration schema version (must be `1`).
    pub schema_version: u32,
    /// Absolute normalized path to the existing same-UID Unix domain socket in a same-UID `0700` parent directory.
    pub socket_path: PathBuf,
    /// Canonical lowercase hyphenated non-nil target thread UUID.
    pub thread_id: String,
    /// Absolute normalized existing canonical project root directory.
    pub project_root: PathBuf,
    /// Bounded non-empty expected model identifier.
    pub model: String,
    /// Bounded non-empty expected reasoning effort identifier.
    pub effort: String,
}

impl fmt::Debug for NativeToolOutputConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeToolOutputConfig")
            .field("schema_version", &self.schema_version)
            .field("socket_path", &"[REDACTED]")
            .field("thread_id", &"[REDACTED]")
            .field("project_root", &"[REDACTED]")
            .field("model", &"[REDACTED]")
            .field("effort", &"[REDACTED]")
            .finish()
    }
}

impl NativeToolOutputConfig {
    /// Validates all structural, identifier, canonical project root, and physical Unix socket invariants.
    pub fn validate(&self) -> Result<(), Error> {
        ensure_platform_supported()?;
        #[cfg(unix)]
        {
            self.validate_with_euid(current_euid())
        }
        #[cfg(not(unix))]
        {
            unreachable!()
        }
    }

    #[cfg(unix)]
    pub(crate) fn validate_with_euid(&self, expected_uid: u32) -> Result<(), Error> {
        if self.schema_version != NATIVE_TOOL_OUTPUT_CONFIG_SCHEMA_VERSION {
            return Err(Error::InvalidConfig(format!(
                "unsupported native tool-output config schema_version {} (expected {})",
                self.schema_version, NATIVE_TOOL_OUTPUT_CONFIG_SCHEMA_VERSION
            )));
        }

        validate_canonical_non_nil_uuid(&self.thread_id, "native tool-output thread_id")?;
        validate_model_or_effort(&self.model, "model")?;
        validate_model_or_effort(&self.effort, "effort")?;
        validate_canonical_project_root(&self.project_root)?;
        validate_physical_socket_path(&self.socket_path, expected_uid)?;

        Ok(())
    }

    /// Loads and validates a [`NativeToolOutputConfig`] from an existing non-symlink same-UID `0600` regular file (`<= 16 KiB`) inside a same-UID `0700` parent directory.
    pub fn load(path: &Path) -> Result<Self, Error> {
        ensure_platform_supported()?;
        #[cfg(unix)]
        {
            Self::load_with_euid(path, current_euid())
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            unreachable!()
        }
    }

    #[cfg(unix)]
    pub(crate) fn load_with_euid(path: &Path, expected_uid: u32) -> Result<Self, Error> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if path.as_os_str().is_empty() {
            return Err(Error::InvalidConfig(
                "native tool-output config path must not be empty".to_string(),
            ));
        }

        for comp in path.components() {
            if matches!(comp, Component::CurDir | Component::ParentDir) {
                return Err(Error::SecurityViolation(
                    "native tool-output config path must not contain `.` or `..` traversal components"
                        .to_string(),
                ));
            }
        }

        let resolved = validate_no_symlink_components(path, "native tool-output config")?;
        let parent = resolved.parent().ok_or_else(|| {
            Error::InvalidConfig(
                "native tool-output config path must have a direct parent directory".to_string(),
            )
        })?;
        validate_private_parent_dir(
            parent,
            expected_uid,
            "native tool-output config parent directory",
        )?;

        let sym_meta = fs::symlink_metadata(&resolved).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Error::InvalidConfig("native tool-output config file does not exist".to_string())
            } else {
                Error::Io(err)
            }
        })?;
        if sym_meta.file_type().is_symlink() {
            return Err(Error::SecurityViolation(
                "native tool-output config file must not be a symlink".to_string(),
            ));
        }
        if !sym_meta.file_type().is_file() {
            return Err(Error::SecurityViolation(
                "native tool-output config path must be a regular file".to_string(),
            ));
        }
        if sym_meta.uid() != expected_uid {
            return Err(Error::SecurityViolation(
                "native tool-output config file is not owned by the current effective UID"
                    .to_string(),
            ));
        }
        let sym_mode = sym_meta.permissions().mode() & 0o777;
        if sym_mode != 0o600 {
            return Err(Error::SecurityViolation(format!(
                "native tool-output config file has permissions {:04o} (expected 0600)",
                sym_mode
            )));
        }
        if sym_meta.len() > MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES {
            return Err(Error::InvalidConfig(format!(
                "native tool-output config file exceeds maximum size of {} bytes",
                MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES
            )));
        }

        let file = File::open(&resolved)?;
        let open_meta = file.metadata()?;
        if !open_meta.file_type().is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
        {
            return Err(Error::SecurityViolation(
                "native tool-output config file changed during open".to_string(),
            ));
        }
        if open_meta.uid() != expected_uid {
            return Err(Error::SecurityViolation(
                "native tool-output config file is not owned by the current effective UID"
                    .to_string(),
            ));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_mode != 0o600 {
            return Err(Error::SecurityViolation(format!(
                "native tool-output config file has permissions {:04o} (expected 0600)",
                open_mode
            )));
        }
        if open_meta.len() > MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES {
            return Err(Error::InvalidConfig(format!(
                "native tool-output config file exceeds maximum size of {} bytes",
                MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES
            )));
        }

        let mut buf = Vec::with_capacity(open_meta.len() as usize);
        file.take(MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES + 1)
            .read_to_end(&mut buf)?;
        if buf.len() as u64 > MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES {
            return Err(Error::InvalidConfig(format!(
                "native tool-output config file exceeds maximum size of {} bytes",
                MAX_NATIVE_TOOL_OUTPUT_CONFIG_BYTES
            )));
        }

        let config: Self = serde_json::from_slice(&buf).map_err(|_| {
            Error::InvalidConfig("invalid native tool-output config JSON".to_string())
        })?;
        config.validate_with_euid(expected_uid)?;
        Ok(config)
    }

    /// Computes a deterministic 64-character lowercase hex SHA-256 digest of the canonical configuration.
    ///
    /// The returned digest string is path-free and suitable for persistence in summaries.
    pub fn digest(&self) -> String {
        #[derive(Serialize)]
        struct CanonicalConfig<'a> {
            schema_version: u32,
            socket_path: &'a [u8],
            thread_id: &'a str,
            project_root: &'a [u8],
            model: &'a str,
            effort: &'a str,
        }

        let canonical = CanonicalConfig {
            schema_version: self.schema_version,
            socket_path: path_as_canonical_bytes(&self.socket_path),
            thread_id: &self.thread_id,
            project_root: path_as_canonical_bytes(&self.project_root),
            model: &self.model,
            effort: &self.effort,
        };
        let encoded =
            serde_json::to_vec(&canonical).expect("canonical config serialization is infallible");
        let digest = Sha256::digest(&encoded);
        let mut hex = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        hex
    }
}

#[cfg(unix)]
fn path_as_canonical_bytes(path: &Path) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes()
}

#[cfg(not(unix))]
fn path_as_canonical_bytes(path: &Path) -> &[u8] {
    path.to_str().map(str::as_bytes).unwrap_or(&[])
}

#[cfg(unix)]
pub(crate) fn current_euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

pub(crate) fn validate_canonical_non_nil_uuid(raw: &str, label: &str) -> Result<(), Error> {
    if raw.len() != 36 {
        return Err(Error::InvalidConfig(format!(
            "{label} must be a canonical 36-character lowercase hyphenated UUID"
        )));
    }
    let parsed = Uuid::parse_str(raw)
        .map_err(|_| Error::InvalidConfig(format!("{label} is not a valid UUID")))?;
    if parsed.is_nil() {
        return Err(Error::InvalidConfig(format!(
            "{label} must not be the nil UUID"
        )));
    }
    if parsed.hyphenated().to_string() != raw {
        return Err(Error::InvalidConfig(format!(
            "{label} must be formatted in canonical lowercase hyphenated UUID form"
        )));
    }
    Ok(())
}

fn validate_model_or_effort(value: &str, field: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > MAX_ID_LEN
        || value.trim() != value
        || value.contains("..")
        || value.chars().any(|c| {
            c.is_control()
                || c.is_whitespace()
                || !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ':'))
        })
    {
        return Err(Error::InvalidConfig(format!(
            "native tool-output `{field}` must be a bounded non-empty ASCII identifier"
        )));
    }
    Ok(())
}

fn validate_normalized_absolute_path(path: &Path, label: &str) -> Result<(), Error> {
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidConfig(format!("{label} must not be empty")));
    }
    if !path.is_absolute() {
        return Err(Error::InvalidConfig(format!(
            "{label} must be an absolute path"
        )));
    }

    let mut reconstructed = PathBuf::new();
    let mut saw_normal = false;
    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => reconstructed.push(prefix.as_os_str()),
            Component::RootDir => reconstructed.push(comp.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(Error::SecurityViolation(format!(
                    "{label} must not contain `.` or `..` traversal components"
                )));
            }
            Component::Normal(part) => {
                if part.is_empty() {
                    return Err(Error::InvalidConfig(format!(
                        "{label} contains an empty path component"
                    )));
                }
                reconstructed.push(part);
                saw_normal = true;
            }
        }
    }

    if !saw_normal {
        return Err(Error::InvalidConfig(format!(
            "{label} must not be the root directory"
        )));
    }

    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(Error::InvalidConfig(format!(
            "{label} must be normalized without redundant or trailing separators"
        )));
    }

    let _ = validate_no_symlink_components(path, label)?;
    Ok(())
}

fn validate_canonical_project_root(project_root: &Path) -> Result<(), Error> {
    validate_normalized_absolute_path(project_root, "native tool-output project_root")?;

    let meta = fs::symlink_metadata(project_root).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::InvalidConfig(
                "native tool-output project_root directory does not exist".to_string(),
            )
        } else {
            Error::Io(err)
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(Error::SecurityViolation(
            "native tool-output project_root must not be a symlink".to_string(),
        ));
    }
    if !meta.file_type().is_dir() {
        return Err(Error::InvalidConfig(
            "native tool-output project_root must be an existing directory".to_string(),
        ));
    }

    let canonical = fs::canonicalize(project_root).map_err(|_| {
        Error::InvalidConfig("failed to canonicalize native tool-output project_root".to_string())
    })?;
    if canonical != project_root {
        return Err(Error::InvalidConfig(
            "native tool-output project_root must equal its canonical path".to_string(),
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn validate_private_parent_dir(parent: &Path, expected_uid: u32, label: &str) -> Result<(), Error> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let parent_meta = fs::symlink_metadata(parent).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::InvalidConfig(format!("{label} does not exist"))
        } else {
            Error::Io(err)
        }
    })?;
    if parent_meta.file_type().is_symlink() {
        return Err(Error::SecurityViolation(format!(
            "{label} must not be a symlink"
        )));
    }
    if !parent_meta.file_type().is_dir() {
        return Err(Error::SecurityViolation(format!(
            "{label} must be a directory"
        )));
    }
    if parent_meta.uid() != expected_uid {
        return Err(Error::SecurityViolation(format!(
            "{label} is not owned by the current effective UID"
        )));
    }
    let parent_mode = parent_meta.permissions().mode() & 0o777;
    if parent_mode != 0o700 {
        return Err(Error::SecurityViolation(format!(
            "{label} has permissions {:04o} (expected 0700)",
            parent_mode
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_physical_socket_path(socket_path: &Path, expected_uid: u32) -> Result<(), Error> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    validate_normalized_absolute_path(socket_path, "native tool-output socket_path")?;

    let parent = socket_path.parent().ok_or_else(|| {
        Error::InvalidConfig(
            "native tool-output socket_path must have a direct parent directory".to_string(),
        )
    })?;
    validate_private_parent_dir(
        parent,
        expected_uid,
        "native tool-output socket parent directory",
    )?;

    let sock_meta = fs::symlink_metadata(socket_path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Error::InvalidConfig("native tool-output Unix socket does not exist".to_string())
        } else {
            Error::Io(err)
        }
    })?;
    if sock_meta.file_type().is_symlink() {
        return Err(Error::SecurityViolation(
            "native tool-output socket_path must not be a symlink".to_string(),
        ));
    }
    if !sock_meta.file_type().is_socket() {
        return Err(Error::SecurityViolation(
            "native tool-output socket_path is not a Unix domain socket".to_string(),
        ));
    }
    if sock_meta.uid() != expected_uid {
        return Err(Error::SecurityViolation(
            "native tool-output socket_path is not owned by the current effective UID".to_string(),
        ));
    }

    Ok(())
}

#[cfg(unix)]
#[derive(Serialize)]
struct FixedToolOutputPayload<'a> {
    untrusted_data: bool,
    approval_id: &'a str,
    claim_id: &'a str,
    pending_since_seq: u64,
    covered_up_to_seq: u64,
    coalesced_count: u64,
    notice: String,
}

#[cfg(unix)]
struct PreparedExchangeMessages {
    init_req: String,
    initialized_notif: String,
    read_req: String,
    resume_req: String,
    turn_start_req: String,
}

#[cfg(unix)]
fn prepare_exchange_messages(
    claim: &WakeupClaim,
    approved_ctx: &ApprovedSessionWakeupContext,
    tool_output_cfg: &NativeToolOutputConfig,
) -> Result<PreparedExchangeMessages, &'static str> {
    let init_req = serde_json::to_string(&json!({
        "id": 1u64,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": "threadlane",
                "title": "Threadlane",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }
    }))
    .map_err(|_| "native tool-output request serialization failed")?;
    if init_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }

    let initialized_notif = serde_json::to_string(&json!({
        "method": "initialized"
    }))
    .map_err(|_| "native tool-output request serialization failed")?;
    if initialized_notif.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }

    let read_req = serde_json::to_string(&json!({
        "id": 2u64,
        "method": "thread/read",
        "params": {
            "threadId": tool_output_cfg.thread_id,
            "includeTurns": false,
        }
    }))
    .map_err(|_| "native tool-output request serialization failed")?;
    if read_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }

    let resume_req = serde_json::to_string(&json!({
        "id": 3u64,
        "method": "thread/resume",
        "params": {
            "threadId": tool_output_cfg.thread_id,
            "excludeTurns": true,
        }
    }))
    .map_err(|_| "native tool-output request serialization failed")?;
    if resume_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }

    let output_payload = FixedToolOutputPayload {
        untrusted_data: true,
        approval_id: &approved_ctx.approval_id,
        claim_id: &claim.claim_id,
        pending_since_seq: claim.pending_since_seq,
        covered_up_to_seq: claim.covered_up_to_seq,
        coalesced_count: claim.coalesced_count,
        notice: approved_ctx.wakeup_message(),
    };
    let serialized_output = serde_json::to_string(&output_payload)
        .map_err(|_| "native tool-output payload serialization failed")?;
    let turn_start_req = serde_json::to_string(&json!({
        "id": 4u64,
        "method": "turn/start",
        "params": {
            "threadId": tool_output_cfg.thread_id,
            "input": [],
            "toolOutput": {
                "name": NATIVE_TOOL_OUTPUT_NAME,
                "namespace": NATIVE_TOOL_OUTPUT_NAMESPACE,
                "output": serialized_output,
            }
        }
    }))
    .map_err(|_| "native tool-output request serialization failed")?;
    if turn_start_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }

    Ok(PreparedExchangeMessages {
        init_req,
        initialized_notif,
        read_req,
        resume_req,
        turn_start_req,
    })
}

/// Executes one bounded JSON-RPC WebSocket wake-up exchange over the configured Unix domain socket.
///
/// Never spawns a child process, queue command, proxy, or server, and never retries automatically.
pub(crate) async fn invoke_native_tool_output_wakeup(
    config: &CodexWakeupConfig,
    claim: &WakeupClaim,
    approved_ctx: &ApprovedSessionWakeupContext,
    tool_output_cfg: &NativeToolOutputConfig,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> WakeupOutcome {
    #[cfg(not(unix))]
    {
        let _ = (config, claim, approved_ctx, tool_output_cfg, shutdown_rx);
        WakeupOutcome::Failed {
            error: "native tool-output transport is only supported on Unix".to_string(),
        }
    }

    #[cfg(unix)]
    {
        if *shutdown_rx.borrow() {
            return WakeupOutcome::Failed {
                error: "supervisor shutdown before native tool-output wake-up started".to_string(),
            };
        }

        if claim.thread_uuid != tool_output_cfg.thread_id {
            return WakeupOutcome::Failed {
                error: "wake-up claim thread_uuid does not match native tool-output thread_id"
                    .to_string(),
            };
        }

        if tool_output_cfg.validate().is_err() {
            return WakeupOutcome::Failed {
                error: "native tool-output configuration validation failed".to_string(),
            };
        }

        let messages = match prepare_exchange_messages(claim, approved_ctx, tool_output_cfg) {
            Ok(messages) => messages,
            Err(reason) => {
                return WakeupOutcome::Failed {
                    error: reason.to_string(),
                };
            }
        };

        let mut may_have_written = false;
        let shutdown_check_rx = shutdown_rx.clone();

        let exchange_result: Result<Result<(), &'static str>, &'static str> = tokio::select! {
            biased;
            _ = wait_for_shutdown(shutdown_rx) => {
                Err("supervisor shutdown during native tool-output wake-up exchange")
            }
            timed = tokio::time::timeout(
                config.timeout,
                run_bounded_ws_exchange(
                    tool_output_cfg,
                    &messages,
                    &mut may_have_written,
                    &shutdown_check_rx,
                ),
            ) => {
                match timed {
                    Ok(inner) => Ok(inner),
                    Err(_elapsed) => Err("native tool-output wake-up exchange timed out"),
                }
            }
        };

        match exchange_result {
            Ok(Ok(())) => WakeupOutcome::Submitted,
            Ok(Err(reason)) | Err(reason) => {
                if may_have_written {
                    WakeupOutcome::Unknown {
                        reason: reason.to_string(),
                    }
                } else {
                    WakeupOutcome::Failed {
                        error: reason.to_string(),
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow_and_update() {
            return;
        }
        if shutdown_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(unix)]
async fn run_bounded_ws_exchange(
    cfg: &NativeToolOutputConfig,
    messages: &PreparedExchangeMessages,
    may_have_written: &mut bool,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), &'static str> {
    let unix_stream = tokio::net::UnixStream::connect(&cfg.socket_path)
        .await
        .map_err(|_| "failed to connect to native tool-output Unix socket")?;

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(MAX_NATIVE_WS_FRAME_BYTES);
    ws_config.max_frame_size = Some(MAX_NATIVE_WS_FRAME_BYTES);

    let (mut ws, _response) =
        tokio_tungstenite::client_async_with_config(NATIVE_WS_URL, unix_stream, Some(ws_config))
            .await
            .map_err(|_| "native tool-output WebSocket handshake failed")?;

    let mut frame_count = 0usize;
    let mut cumulative_bytes = 0usize;

    // 1. initialize(clientInfo)
    send_prevalidated_text(&mut ws, &messages.init_req).await?;
    let init_result = recv_rpc_response(
        &mut ws,
        1,
        &mut frame_count,
        &mut cumulative_bytes,
        "native tool-output initialize request rejected",
    )
    .await?;
    if !init_result.is_object() {
        return Err("native tool-output initialize response was not an object");
    }

    // 2. initialized notification
    send_prevalidated_text(&mut ws, &messages.initialized_notif).await?;

    // 3. thread/read (includeTurns: false)
    send_prevalidated_text(&mut ws, &messages.read_req).await?;
    let read_result = recv_rpc_response(
        &mut ws,
        2,
        &mut frame_count,
        &mut cumulative_bytes,
        "native tool-output thread/read request rejected",
    )
    .await?;
    validate_thread_read_result(&read_result, cfg)?;

    // 4. thread/resume (threadId, excludeTurns: true, no overrides)
    send_prevalidated_text(&mut ws, &messages.resume_req).await?;
    let resume_result = recv_rpc_response(
        &mut ws,
        3,
        &mut frame_count,
        &mut cumulative_bytes,
        "native tool-output thread/resume request rejected",
    )
    .await?;
    validate_thread_resume_result(&resume_result, cfg)?;

    if *shutdown_rx.borrow() {
        return Err("supervisor shutdown before native tool-output turn/start send");
    }

    // 5. Set uncertainty marker immediately BEFORE attempting turn/start write.
    *may_have_written = true;

    ws.send(Message::Text(messages.turn_start_req.clone().into()))
        .await
        .map_err(|_| "native tool-output turn/start send failed")?;

    // 6. Receive and validate turn/start response.
    let turn_result = recv_rpc_response(
        &mut ws,
        4,
        &mut frame_count,
        &mut cumulative_bytes,
        "native tool-output turn/start request rejected",
    )
    .await?;
    validate_turn_start_result(&turn_result)?;

    Ok(())
}

#[cfg(unix)]
async fn send_prevalidated_text(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    payload: &str,
) -> Result<(), &'static str> {
    if payload.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err("native tool-output request exceeded maximum frame size");
    }
    ws.send(Message::Text(payload.to_owned().into()))
        .await
        .map_err(|_| "failed to send native tool-output WebSocket request")
}

#[cfg(unix)]
async fn recv_rpc_response(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    expected_id: u64,
    frame_count: &mut usize,
    cumulative_bytes: &mut usize,
    rejection_msg: &'static str,
) -> Result<Value, &'static str> {
    loop {
        if *frame_count >= MAX_NATIVE_WS_FRAMES {
            return Err("native tool-output exchange exceeded maximum frame count");
        }

        let Some(frame_res) = ws.next().await else {
            return Err("native tool-output WebSocket closed before response completed");
        };
        let msg = frame_res.map_err(|_| "native tool-output WebSocket receive error")?;
        *frame_count += 1;

        match msg {
            Message::Text(text) => {
                let len = text.len();
                if len > MAX_NATIVE_WS_FRAME_BYTES {
                    return Err("native tool-output WebSocket frame exceeded maximum size");
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    );
                }

                let value: Value = serde_json::from_str(text.as_ref())
                    .map_err(|_| "native tool-output received malformed JSON frame")?;
                let Some(obj) = value.as_object() else {
                    return Err("native tool-output JSON frame must be an object");
                };

                let has_id = obj.get("id").is_some_and(|v| !v.is_null());
                let has_method = obj.contains_key("method");

                if has_method && !has_id {
                    if obj.contains_key("result") || obj.contains_key("error") {
                        return Err("native tool-output received invalid JSON-RPC notification");
                    }
                    // Ignore untrusted server notifications.
                    continue;
                }

                if has_method && has_id {
                    // Fail closed on server-initiated requests; never auto-approve or execute.
                    return Err("native tool-output rejected unexpected server-initiated request");
                }

                let Some(id_val) = obj.get("id").and_then(Value::as_u64) else {
                    return Err("native tool-output response missing expected numeric id");
                };
                if id_val != expected_id {
                    return Err("native tool-output response id did not match expected request id");
                }

                if let Some(err_val) = obj.get("error") {
                    if !err_val.is_null() {
                        return Err(rejection_msg);
                    }
                }

                let Some(result) = obj.get("result") else {
                    return Err("native tool-output response missing result field");
                };
                return Ok(result.clone());
            }
            Message::Ping(payload) => {
                let len = payload.len();
                if len > MAX_CONTROL_PING_BYTES {
                    return Err("native tool-output control ping exceeded maximum size");
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    );
                }
                ws.flush()
                    .await
                    .map_err(|_| "native tool-output failed to flush WebSocket pong")?;
            }
            Message::Pong(payload) => {
                let len = payload.len();
                if len > MAX_CONTROL_PING_BYTES {
                    return Err("native tool-output control pong exceeded maximum size");
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    );
                }
            }
            Message::Binary(_) => {
                return Err("native tool-output rejected unexpected binary WebSocket frame");
            }
            Message::Close(_) => {
                return Err("native tool-output WebSocket closed by peer before response");
            }
            Message::Frame(_) => {
                return Err("native tool-output rejected raw WebSocket frame");
            }
        }
    }
}

#[cfg(unix)]
fn validate_thread_read_result(
    result: &Value,
    cfg: &NativeToolOutputConfig,
) -> Result<(), &'static str> {
    let Some(thread) = result.get("thread").and_then(Value::as_object) else {
        return Err("native tool-output thread/read response missing thread object");
    };

    let Some(id) = thread.get("id").and_then(Value::as_str) else {
        return Err("native tool-output thread/read response missing thread.id");
    };
    if id != cfg.thread_id {
        return Err("native tool-output thread/read thread.id mismatch");
    }

    let Some(cwd) = thread.get("cwd").and_then(Value::as_str) else {
        return Err("native tool-output thread/read response missing thread.cwd");
    };
    if Path::new(cwd) != cfg.project_root.as_path() {
        return Err("native tool-output thread/read thread.cwd mismatch");
    }

    let status_type = match thread.get("status") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Object(map)) => map.get("type").and_then(Value::as_str),
        _ => None,
    };
    match status_type {
        Some("idle" | "active") => {}
        _ => {
            return Err("native tool-output target thread is not loaded in idle or active status");
        }
    }

    if let Some(model_val) = thread.get("model") {
        if !model_val.is_null() {
            let Some(model) = model_val.as_str() else {
                return Err("native tool-output thread/read thread.model is not a string");
            };
            if model != cfg.model {
                return Err("native tool-output thread/read model mismatch");
            }
        }
    }

    if let Some(effort_val) = thread.get("reasoningEffort") {
        if !effort_val.is_null() {
            let Some(effort) = effort_val.as_str() else {
                return Err(
                    "native tool-output thread/read thread.reasoningEffort is not a string",
                );
            };
            if effort != cfg.effort {
                return Err("native tool-output thread/read reasoningEffort mismatch");
            }
        }
    }

    Ok(())
}

#[cfg(unix)]
fn extract_consistent_string_field<'a>(
    outer: &'a serde_json::Map<String, Value>,
    inner: Option<&'a serde_json::Map<String, Value>>,
    field: &str,
) -> Result<&'a str, &'static str> {
    let outer_val = match outer.get(field) {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => return Err("native tool-output thread/resume field has invalid type"),
    };
    let inner_val = match inner.and_then(|m| m.get(field)) {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => return Err("native tool-output thread/resume field has invalid type"),
    };

    match (outer_val, inner_val) {
        (Some(a), Some(b)) => {
            if a != b {
                Err("native tool-output thread/resume outer and inner fields disagreed")
            } else {
                Ok(a)
            }
        }
        (Some(a), None) => Ok(a),
        (None, Some(b)) => Ok(b),
        (None, None) => Err("native tool-output thread/resume missing required field"),
    }
}

#[cfg(unix)]
fn validate_thread_resume_result(
    result: &Value,
    cfg: &NativeToolOutputConfig,
) -> Result<(), &'static str> {
    let Some(result_obj) = result.as_object() else {
        return Err("native tool-output thread/resume result must be an object");
    };
    let thread_obj = match result_obj.get("thread") {
        Some(Value::Object(map)) => Some(map),
        Some(Value::Null) | None => None,
        Some(_) => return Err("native tool-output thread/resume thread field must be an object"),
    };

    let id = extract_consistent_string_field(result_obj, thread_obj, "id")?;
    if id != cfg.thread_id {
        return Err("native tool-output thread/resume thread.id mismatch");
    }

    let cwd = extract_consistent_string_field(result_obj, thread_obj, "cwd")?;
    if Path::new(cwd) != cfg.project_root.as_path() {
        return Err("native tool-output thread/resume cwd mismatch");
    }

    let model = extract_consistent_string_field(result_obj, thread_obj, "model")?;
    if model != cfg.model {
        return Err("native tool-output thread/resume model mismatch");
    }

    let effort = extract_consistent_string_field(result_obj, thread_obj, "reasoningEffort")?;
    if effort != cfg.effort {
        return Err("native tool-output thread/resume reasoningEffort mismatch");
    }

    Ok(())
}

#[cfg(unix)]
fn validate_turn_start_result(result: &Value) -> Result<(), &'static str> {
    let Some(result_obj) = result.as_object() else {
        return Err("native tool-output turn/start result must be an object");
    };

    if let Some(err_val) = result_obj.get("error") {
        if !err_val.is_null() {
            return Err("native tool-output turn/start reported an error");
        }
    }

    let Some(turn_obj) = result_obj.get("turn").and_then(Value::as_object) else {
        return Err("native tool-output turn/start response missing turn object");
    };

    if let Some(turn_err) = turn_obj.get("error") {
        if !turn_err.is_null() {
            return Err("native tool-output turn/start turn reported an error");
        }
    }

    if let Some(status_val) = turn_obj.get("status") {
        let status_str = match status_val {
            Value::String(s) => Some(s.as_str()),
            Value::Object(map) => map.get("type").and_then(Value::as_str),
            Value::Null => None,
            _ => return Err("native tool-output turn/start turn.status has invalid format"),
        };
        if let Some(status) = status_str {
            if matches!(
                status,
                "failed" | "error" | "errored" | "cancelled" | "canceled" | "rejected"
            ) {
                return Err("native tool-output turn/start returned a rejected turn status");
            }
        }
    }

    let Some(turn_id) = turn_obj.get("id").and_then(Value::as_str) else {
        return Err("native tool-output turn/start response missing turn.id");
    };
    validate_canonical_non_nil_uuid(turn_id, "turn.id")
        .map_err(|_| "native tool-output turn/start returned invalid turn.id")?;

    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;

    #[test]
    fn debug_redacts_sensitive_fields_and_digest_is_path_free() {
        let cfg = NativeToolOutputConfig {
            schema_version: 1,
            socket_path: PathBuf::from("/tmp/secret-socket-dir/codex.sock"),
            thread_id: "01968f52-8871-7d51-9914-79d2bc48eb5a".to_string(),
            project_root: PathBuf::from("/tmp/secret-project-root"),
            model: "secret-model-id".to_string(),
            effort: "high".to_string(),
        };

        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("secret-socket-dir"));
        assert!(!dbg.contains("01968f52-8871-7d51-9914-79d2bc48eb5a"));
        assert!(!dbg.contains("secret-project-root"));
        assert!(!dbg.contains("secret-model-id"));
        assert!(dbg.contains("[REDACTED]"));

        let digest = cfg.digest();
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert!(!digest.contains("secret"));
        assert_eq!(digest, cfg.digest());
    }

    #[test]
    fn validate_and_load_reject_mismatched_effective_uid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical tempdir");
        let project_root = root.join("repo");
        fs::create_dir(&project_root).expect("create repo");

        let sock_dir = root.join("sock");
        fs::create_dir(&sock_dir).expect("create sock dir");
        fs::set_permissions(&sock_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let sock_path = sock_dir.join("codex.sock");
        let _listener = UnixListener::bind(&sock_path).expect("bind unix socket");

        let cfg = NativeToolOutputConfig {
            schema_version: 1,
            socket_path: sock_path,
            thread_id: "01968f52-8871-7d51-9914-79d2bc48eb5a".to_string(),
            project_root,
            model: "gpt-5".to_string(),
            effort: "high".to_string(),
        };

        cfg.validate().expect("valid for current euid");
        let wrong_uid = current_euid().wrapping_add(1);
        assert!(cfg.validate_with_euid(wrong_uid).is_err());

        let cfg_dir = root.join("cfg");
        fs::create_dir(&cfg_dir).expect("create cfg dir");
        fs::set_permissions(&cfg_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let cfg_path = cfg_dir.join("context.json");
        fs::write(
            &cfg_path,
            serde_json::to_vec_pretty(&cfg).expect("serialize"),
        )
        .expect("write cfg");
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

        let loaded = NativeToolOutputConfig::load(&cfg_path).expect("load for current euid");
        assert_eq!(loaded, cfg);
        assert!(NativeToolOutputConfig::load_with_euid(&cfg_path, wrong_uid).is_err());
    }
}
