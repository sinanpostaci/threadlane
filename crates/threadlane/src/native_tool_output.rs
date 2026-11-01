use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use std::time::Duration;

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
use crate::native_endpoint::NativeReconnectConfig;
#[cfg(unix)]
use crate::native_endpoint::{EndpointError, ResolvedNativeEndpoint};

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

/// Maximum permitted endpoint resolution and pre-effect connection attempts (`64`) across one wake-up exchange when auto-reconnect is enabled.
pub const MAX_NATIVE_RECONNECT_ATTEMPTS: usize = 64;

/// Fixed tool name submitted in `turn/start.toolOutput`.
pub const NATIVE_TOOL_OUTPUT_NAME: &str = "threadlane_events_available";

/// Fixed tool namespace submitted in `turn/start.toolOutput`.
pub const NATIVE_TOOL_OUTPUT_NAMESPACE: &str = "threadlane";

const NATIVE_WS_URL: &str = "ws://localhost/";
const MAX_CONTROL_PING_BYTES: usize = 125;
#[cfg(unix)]
const INITIAL_RECONNECT_BACKOFF_MS: u64 = 5;
#[cfg(unix)]
const MAX_RECONNECT_BACKOFF_MS: u64 = 50;

/// Explicit operator-selected native tool-output wake-up transport configuration.
///
/// Explicit operator-selected native notification delivery mode over the private Unix WebSocket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeNotificationDelivery {
    /// Default mode: submits `turn/start` with empty `input` and `toolOutput` (`functionCallOutput`).
    #[default]
    ToolOutput,
    /// Opt-in direct user-message mode: submits `turn/start` with the fixed operator notice when
    /// `idle`, or `turn/steer` targeting the verified `inProgress` active turn when `active`.
    UserMessageSteer,
}

impl NativeNotificationDelivery {
    /// Returns `true` when `self` is the default [`Self::ToolOutput`] delivery mode.
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::ToolOutput)
    }

    /// Returns the wire `snake_case` identifier (`"tool_output"` or `"user_message_steer"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ToolOutput => "tool_output",
            Self::UserMessageSteer => "user_message_steer",
        }
    }
}

/// Explicit operator-configured native Unix WebSocket notification target context.
///
/// This configuration is an operator-selected target scope only and is not a per-call
/// native identity proof. Every wake-up attempt revalidates the physical socket path
/// (or resolves and validates the live native endpoint when `reconnect` is configured),
/// parent directory ownership/permissions, canonical project root, and target thread identity.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeToolOutputConfig {
    /// Configuration schema version (must be `1`).
    pub schema_version: u32,
    /// Absolute normalized path to the existing same-UID Unix domain socket in a same-UID `0700` parent directory, or the normalized absolute initial selection when `reconnect` is configured.
    pub socket_path: PathBuf,
    /// Canonical lowercase hyphenated non-nil target thread UUID.
    pub thread_id: String,
    /// Absolute normalized existing canonical project root directory.
    pub project_root: PathBuf,
    /// Bounded non-empty expected model identifier.
    pub model: String,
    /// Bounded non-empty expected reasoning effort identifier.
    pub effort: String,
    /// Optional explicit native notification delivery mode (`tool_output` by default; omitted on legacy wire payloads when default).
    #[serde(
        default,
        skip_serializing_if = "NativeNotificationDelivery::is_default"
    )]
    pub delivery: NativeNotificationDelivery,
    /// Optional opt-in Linux native endpoint auto-reconnect selector configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect: Option<NativeReconnectConfig>,
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
            .field("delivery", &self.delivery)
            .field("reconnect", &self.reconnect)
            .finish()
    }
}

impl NativeToolOutputConfig {
    /// Returns a copy of this configuration with the specified [`NativeNotificationDelivery`] mode.
    pub const fn with_delivery(mut self, delivery: NativeNotificationDelivery) -> Self {
        self.delivery = delivery;
        self
    }
    /// Validates all structural, identifier, canonical project root, and Unix socket or reconnect invariants.
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

        match self.reconnect {
            None => {
                validate_physical_socket_path(&self.socket_path, expected_uid)?;
            }
            Some(ref reconnect) => {
                validate_initial_socket_selection(&self.socket_path, expected_uid)?;
                if expected_uid != current_euid() {
                    return Err(Error::SecurityViolation(
                        "native tool-output reconnect configuration is not owned by the current effective UID"
                            .to_string(),
                    ));
                }
                reconnect.validate()?;
            }
        }

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
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

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

        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32,
            )
            .open(&resolved)?;
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
        struct CanonicalReconnectConfig<'a> {
            descriptor_root: &'a [u8],
            backend_executable: &'a [u8],
            application_executable: &'a [u8],
        }

        #[derive(Serialize)]
        struct CanonicalConfig<'a> {
            schema_version: u32,
            socket_path: &'a [u8],
            thread_id: &'a str,
            project_root: &'a [u8],
            model: &'a str,
            effort: &'a str,
            #[serde(skip_serializing_if = "NativeNotificationDelivery::is_default")]
            delivery: NativeNotificationDelivery,
            #[serde(skip_serializing_if = "Option::is_none")]
            reconnect: Option<CanonicalReconnectConfig<'a>>,
        }

        let canonical = CanonicalConfig {
            schema_version: self.schema_version,
            socket_path: path_as_canonical_bytes(&self.socket_path),
            thread_id: &self.thread_id,
            project_root: path_as_canonical_bytes(&self.project_root),
            model: &self.model,
            effort: &self.effort,
            delivery: self.delivery,
            reconnect: self
                .reconnect
                .as_ref()
                .map(|reconnect| CanonicalReconnectConfig {
                    descriptor_root: path_as_canonical_bytes(&reconnect.descriptor_root),
                    backend_executable: path_as_canonical_bytes(&reconnect.backend_executable),
                    application_executable: path_as_canonical_bytes(
                        &reconnect.application_executable,
                    ),
                }),
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
fn validate_initial_socket_selection(socket_path: &Path, expected_uid: u32) -> Result<(), Error> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    validate_normalized_absolute_path(socket_path, "native tool-output socket_path")?;

    let parent = socket_path.parent().ok_or_else(|| {
        Error::InvalidConfig(
            "native tool-output socket_path must have a direct parent directory".to_string(),
        )
    })?;

    match fs::symlink_metadata(socket_path) {
        Ok(sock_meta) => {
            validate_private_parent_dir(
                parent,
                expected_uid,
                "native tool-output socket parent directory",
            )?;
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
                    "native tool-output socket_path is not owned by the current effective UID"
                        .to_string(),
                ));
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(Error::Io(err)),
    }
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
enum PreparedDeliveryMessages {
    ToolOutput {
        turn_start_req: String,
    },
    UserMessageSteer {
        idle_turn_start_req: String,
        turns_list_req: String,
        notice_text: String,
    },
}

#[cfg(unix)]
struct PreparedExchangeMessages {
    init_req: String,
    initialized_notif: String,
    read_req: String,
    resume_req: String,
    delivery: PreparedDeliveryMessages,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThreadReadStatus {
    Idle,
    Active,
}

#[cfg(unix)]
impl ThreadReadStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Active => "active",
        }
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeError {
    /// Transient pre-effect endpoint unavailability, connection loss, or `notLoaded` thread status.
    Retryable(&'static str),
    /// Terminal configuration, identity, metadata, protocol, budget, or security failure.
    Terminal(&'static str),
}

#[cfg(unix)]
impl ExchangeError {
    const fn reason(self) -> &'static str {
        match self {
            Self::Retryable(reason) | Self::Terminal(reason) => reason,
        }
    }
}

#[cfg(unix)]
fn map_endpoint_error(err: EndpointError) -> ExchangeError {
    match err {
        EndpointError::Unavailable(reason) => ExchangeError::Retryable(reason),
        EndpointError::Refused(reason) => ExchangeError::Terminal(reason),
    }
}

#[cfg(unix)]
fn is_ws_connection_loss(err: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::error::ProtocolError;
    use tokio_tungstenite::tungstenite::Error as WsError;

    matches!(
        err,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Io(_)
            | WsError::Protocol(
                ProtocolError::HandshakeIncomplete | ProtocolError::ResetWithoutClosingHandshake
            )
    )
}

#[cfg(unix)]
fn classify_ws_handshake_error(err: tokio_tungstenite::tungstenite::Error) -> ExchangeError {
    if is_ws_connection_loss(&err) {
        ExchangeError::Retryable("native tool-output WebSocket handshake failed")
    } else {
        ExchangeError::Terminal("native tool-output WebSocket handshake failed")
    }
}

#[cfg(unix)]
fn classify_ws_recv_error(err: tokio_tungstenite::tungstenite::Error) -> ExchangeError {
    if is_ws_connection_loss(&err) {
        ExchangeError::Retryable("native tool-output WebSocket receive error")
    } else {
        ExchangeError::Terminal("native tool-output WebSocket receive error")
    }
}

#[cfg(unix)]
fn prepare_exchange_messages(
    claim: &WakeupClaim,
    approved_ctx: &ApprovedSessionWakeupContext,
    tool_output_cfg: &NativeToolOutputConfig,
) -> Result<PreparedExchangeMessages, &'static str> {
    let init_params = match tool_output_cfg.delivery {
        NativeNotificationDelivery::ToolOutput => json!({
            "clientInfo": {
                "name": "threadlane",
                "title": "Threadlane",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }),
        NativeNotificationDelivery::UserMessageSteer => json!({
            "clientInfo": {
                "name": "threadlane",
                "title": "Threadlane",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {
                "experimentalApi": true,
            }
        }),
    };
    let init_req = serde_json::to_string(&json!({
        "id": 1u64,
        "method": "initialize",
        "params": init_params,
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

    let delivery = match tool_output_cfg.delivery {
        NativeNotificationDelivery::ToolOutput => {
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
            PreparedDeliveryMessages::ToolOutput { turn_start_req }
        }
        NativeNotificationDelivery::UserMessageSteer => {
            let notice_text = approved_ctx.wakeup_message();
            let idle_turn_start_req = serde_json::to_string(&json!({
                "id": 4u64,
                "method": "turn/start",
                "params": {
                    "threadId": tool_output_cfg.thread_id,
                    "input": [
                        {
                            "type": "text",
                            "text": notice_text,
                        }
                    ]
                }
            }))
            .map_err(|_| "native tool-output request serialization failed")?;
            if idle_turn_start_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
                return Err("native tool-output request exceeded maximum frame size");
            }
            let turns_list_req = serde_json::to_string(&json!({
                "id": 4u64,
                "method": "thread/turns/list",
                "params": {
                    "threadId": tool_output_cfg.thread_id,
                    "limit": 1u32,
                    "sortDirection": "desc",
                    "itemsView": "summary",
                }
            }))
            .map_err(|_| "native tool-output request serialization failed")?;
            if turns_list_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
                return Err("native tool-output request exceeded maximum frame size");
            }
            PreparedDeliveryMessages::UserMessageSteer {
                idle_turn_start_req,
                turns_list_req,
                notice_text,
            }
        }
    };

    Ok(PreparedExchangeMessages {
        init_req,
        initialized_notif,
        read_req,
        resume_req,
        delivery,
    })
}

/// Executes one bounded JSON-RPC WebSocket wake-up exchange over the configured or resolved Unix domain socket.
///
/// Never spawns a child process, queue command, proxy, or server. When `reconnect` is `Some`,
/// performs bounded pre-effect endpoint resolution and connection retries under the single overall
/// timeout and shared frame/byte budget; never retries once `turn/start` may have been written.
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
                    config.timeout,
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
    timeout: Duration,
) -> Result<(), &'static str> {
    let deadline = tokio::time::Instant::now() + timeout;
    let max_attempts = if cfg.reconnect.is_some() {
        MAX_NATIVE_RECONNECT_ATTEMPTS
    } else {
        1
    };

    let mut frame_count = 0usize;
    let mut cumulative_bytes = 0usize;

    for attempt in 0..max_attempts {
        if *shutdown_rx.borrow() {
            return Err("supervisor shutdown during native tool-output wake-up exchange");
        }

        match run_single_ws_attempt(
            cfg,
            messages,
            may_have_written,
            shutdown_rx,
            &mut frame_count,
            &mut cumulative_bytes,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(err) => {
                if *may_have_written {
                    return Err(err.reason());
                }
                match err {
                    ExchangeError::Terminal(reason) => return Err(reason),
                    ExchangeError::Retryable(reason) => {
                        if cfg.reconnect.is_none() || attempt + 1 >= max_attempts {
                            return Err(reason);
                        }
                        if *shutdown_rx.borrow() {
                            return Err(
                                "supervisor shutdown during native tool-output wake-up exchange",
                            );
                        }
                        let shift = (attempt as u32).min(4);
                        let delay_ms = INITIAL_RECONNECT_BACKOFF_MS
                            .saturating_mul(1u64 << shift)
                            .min(MAX_RECONNECT_BACKOFF_MS);
                        // Spread the bounded attempt budget over the configured window.
                        // Fast connection refusals must not exhaust it after only a few seconds.
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        let remaining_attempts = (max_attempts - attempt - 1) as u32;
                        let paced_delay = remaining / remaining_attempts;
                        let delay = Duration::from_millis(delay_ms).max(paced_delay);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }

    Err("native tool-output wake-up exchange exhausted retry attempts")
}

#[cfg(unix)]
async fn run_single_ws_attempt(
    cfg: &NativeToolOutputConfig,
    messages: &PreparedExchangeMessages,
    may_have_written: &mut bool,
    shutdown_rx: &watch::Receiver<bool>,
    frame_count: &mut usize,
    cumulative_bytes: &mut usize,
) -> Result<(), ExchangeError> {
    let resolved: Option<ResolvedNativeEndpoint> = match cfg.reconnect {
        Some(ref reconnect_cfg) => Some(reconnect_cfg.resolve().map_err(map_endpoint_error)?),
        None => None,
    };

    let target_socket_path = match resolved {
        Some(ref endpoint) => &endpoint.socket_path,
        None => &cfg.socket_path,
    };

    let unix_stream = tokio::net::UnixStream::connect(target_socket_path)
        .await
        .map_err(|_| {
            ExchangeError::Retryable("failed to connect to native tool-output Unix socket")
        })?;

    if let Some(ref endpoint) = resolved {
        endpoint
            .validate_peer(&unix_stream)
            .map_err(map_endpoint_error)?;
    }

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(MAX_NATIVE_WS_FRAME_BYTES);
    ws_config.max_frame_size = Some(MAX_NATIVE_WS_FRAME_BYTES);

    let (mut ws, _response) =
        tokio_tungstenite::client_async_with_config(NATIVE_WS_URL, unix_stream, Some(ws_config))
            .await
            .map_err(classify_ws_handshake_error)?;

    // 1. initialize(clientInfo)
    send_prevalidated_text(&mut ws, &messages.init_req).await?;
    let init_result = recv_rpc_response(
        &mut ws,
        1,
        frame_count,
        cumulative_bytes,
        "native tool-output initialize request rejected",
    )
    .await?;
    if !init_result.is_object() {
        return Err(ExchangeError::Terminal(
            "native tool-output initialize response was not an object",
        ));
    }

    // 2. initialized notification
    send_prevalidated_text(&mut ws, &messages.initialized_notif).await?;

    // 3. thread/read (includeTurns: false)
    send_prevalidated_text(&mut ws, &messages.read_req).await?;
    let read_result = recv_rpc_response(
        &mut ws,
        2,
        frame_count,
        cumulative_bytes,
        "native tool-output thread/read request rejected",
    )
    .await?;
    let thread_status = validate_thread_read_result(&read_result, cfg)?;

    // 4. thread/resume (threadId, excludeTurns: true, no overrides)
    send_prevalidated_text(&mut ws, &messages.resume_req).await?;
    let resume_result = recv_rpc_response(
        &mut ws,
        3,
        frame_count,
        cumulative_bytes,
        "native tool-output thread/resume request rejected",
    )
    .await?;
    validate_thread_resume_result(&resume_result, cfg, thread_status)?;

    match &messages.delivery {
        PreparedDeliveryMessages::ToolOutput { turn_start_req } => {
            validate_pre_effect_ready(resolved.as_ref(), ws.get_ref(), shutdown_rx)?;

            // 5. Set uncertainty marker immediately BEFORE attempting turn/start write.
            *may_have_written = true;

            ws.send(Message::Text(turn_start_req.clone().into()))
                .await
                .map_err(|_| {
                    ExchangeError::Terminal("native tool-output turn/start send failed")
                })?;

            // 6. Receive and validate turn/start response.
            let turn_result = recv_rpc_response(
                &mut ws,
                4,
                frame_count,
                cumulative_bytes,
                "native tool-output turn/start request rejected",
            )
            .await?;
            validate_turn_start_result(&turn_result)?;
        }
        PreparedDeliveryMessages::UserMessageSteer {
            idle_turn_start_req,
            turns_list_req,
            notice_text,
        } => match thread_status {
            ThreadReadStatus::Idle => {
                validate_pre_effect_ready(resolved.as_ref(), ws.get_ref(), shutdown_rx)?;

                // 5. Set uncertainty marker immediately BEFORE attempting idle turn/start write.
                *may_have_written = true;

                ws.send(Message::Text(idle_turn_start_req.clone().into()))
                    .await
                    .map_err(|_| {
                        ExchangeError::Terminal("native tool-output turn/start send failed")
                    })?;

                // 6. Receive and validate turn/start response.
                let turn_result = recv_rpc_response(
                    &mut ws,
                    4,
                    frame_count,
                    cumulative_bytes,
                    "native tool-output turn/start request rejected",
                )
                .await?;
                validate_turn_start_result(&turn_result)?;
            }
            ThreadReadStatus::Active => {
                // 5. Bounded read-only active turn summary lookup (limit: 1, sortDirection: desc, itemsView: summary).
                send_prevalidated_text(&mut ws, turns_list_req).await?;
                let turns_list_result = recv_rpc_response(
                    &mut ws,
                    4,
                    frame_count,
                    cumulative_bytes,
                    "native user-message thread/turns/list request rejected",
                )
                .await?;
                let expected_turn_id = extract_in_progress_turn_id(&turns_list_result)?;

                let turn_steer_req = serde_json::to_string(&json!({
                    "id": 5u64,
                    "method": "turn/steer",
                    "params": {
                        "threadId": cfg.thread_id,
                        "expectedTurnId": expected_turn_id,
                        "input": [
                            {
                                "type": "text",
                                "text": notice_text,
                            }
                        ]
                    }
                }))
                .map_err(|_| {
                    ExchangeError::Terminal("native user-message turn/steer serialization failed")
                })?;
                if turn_steer_req.len() > MAX_NATIVE_WS_FRAME_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native user-message turn/steer request exceeded maximum frame size",
                    ));
                }

                validate_pre_effect_ready(resolved.as_ref(), ws.get_ref(), shutdown_rx)?;

                // 6. Set uncertainty marker immediately BEFORE attempting turn/steer write.
                *may_have_written = true;

                ws.send(Message::Text(turn_steer_req.into()))
                    .await
                    .map_err(|_| {
                        ExchangeError::Terminal("native user-message turn/steer send failed")
                    })?;

                // 7. Receive and validate turn/steer response against exact expectedTurnId.
                let steer_result = recv_rpc_response(
                    &mut ws,
                    5,
                    frame_count,
                    cumulative_bytes,
                    "native user-message turn/steer request rejected",
                )
                .await?;
                validate_turn_steer_result(&steer_result, &expected_turn_id)?;
            }
        },
    }

    Ok(())
}

#[cfg(unix)]
fn validate_pre_effect_ready(
    resolved: Option<&ResolvedNativeEndpoint>,
    stream: &tokio::net::UnixStream,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), ExchangeError> {
    if *shutdown_rx.borrow() {
        return Err(ExchangeError::Terminal(
            "supervisor shutdown before native tool-output turn/start send",
        ));
    }

    if let Some(endpoint) = resolved {
        endpoint.validate_processes().map_err(map_endpoint_error)?;
        endpoint.validate_peer(stream).map_err(map_endpoint_error)?;
    }

    if *shutdown_rx.borrow() {
        return Err(ExchangeError::Terminal(
            "supervisor shutdown before native tool-output turn/start send",
        ));
    }

    Ok(())
}

#[cfg(unix)]
async fn send_prevalidated_text(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    payload: &str,
) -> Result<(), ExchangeError> {
    if payload.len() > MAX_NATIVE_WS_FRAME_BYTES {
        return Err(ExchangeError::Terminal(
            "native tool-output request exceeded maximum frame size",
        ));
    }
    ws.send(Message::Text(payload.to_owned().into()))
        .await
        .map_err(|_| {
            ExchangeError::Retryable("failed to send native tool-output WebSocket request")
        })
}

#[cfg(unix)]
async fn recv_rpc_response(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    expected_id: u64,
    frame_count: &mut usize,
    cumulative_bytes: &mut usize,
    rejection_msg: &'static str,
) -> Result<Value, ExchangeError> {
    loop {
        if *frame_count >= MAX_NATIVE_WS_FRAMES {
            return Err(ExchangeError::Terminal(
                "native tool-output exchange exceeded maximum frame count",
            ));
        }

        let Some(frame_res) = ws.next().await else {
            return Err(ExchangeError::Retryable(
                "native tool-output WebSocket closed before response completed",
            ));
        };
        let msg = frame_res.map_err(classify_ws_recv_error)?;
        *frame_count += 1;

        match msg {
            Message::Text(text) => {
                let len = text.len();
                if len > MAX_NATIVE_WS_FRAME_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output WebSocket frame exceeded maximum size",
                    ));
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    ));
                }

                let value: Value = serde_json::from_str(text.as_ref()).map_err(|_| {
                    ExchangeError::Terminal("native tool-output received malformed JSON frame")
                })?;
                let Some(obj) = value.as_object() else {
                    return Err(ExchangeError::Terminal(
                        "native tool-output JSON frame must be an object",
                    ));
                };

                let has_id = obj.get("id").is_some_and(|v| !v.is_null());
                let has_method = obj.contains_key("method");

                if has_method && !has_id {
                    if obj.contains_key("result") || obj.contains_key("error") {
                        return Err(ExchangeError::Terminal(
                            "native tool-output received invalid JSON-RPC notification",
                        ));
                    }
                    // Ignore untrusted server notifications.
                    continue;
                }

                if has_method && has_id {
                    // Fail closed on server-initiated requests; never auto-approve or execute.
                    return Err(ExchangeError::Terminal(
                        "native tool-output rejected unexpected server-initiated request",
                    ));
                }

                let Some(id_val) = obj.get("id").and_then(Value::as_u64) else {
                    return Err(ExchangeError::Terminal(
                        "native tool-output response missing expected numeric id",
                    ));
                };
                if id_val != expected_id {
                    return Err(ExchangeError::Terminal(
                        "native tool-output response id did not match expected request id",
                    ));
                }

                if let Some(err_val) = obj.get("error") {
                    if !err_val.is_null() {
                        return Err(ExchangeError::Terminal(rejection_msg));
                    }
                }

                let Some(result) = obj.get("result") else {
                    return Err(ExchangeError::Terminal(
                        "native tool-output response missing result field",
                    ));
                };
                return Ok(result.clone());
            }
            Message::Ping(payload) => {
                let len = payload.len();
                if len > MAX_CONTROL_PING_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output control ping exceeded maximum size",
                    ));
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    ));
                }
                ws.flush().await.map_err(|_| {
                    ExchangeError::Retryable("native tool-output failed to flush WebSocket pong")
                })?;
            }
            Message::Pong(payload) => {
                let len = payload.len();
                if len > MAX_CONTROL_PING_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output control pong exceeded maximum size",
                    ));
                }
                *cumulative_bytes = cumulative_bytes.saturating_add(len);
                if *cumulative_bytes > MAX_NATIVE_WS_CUMULATIVE_BYTES {
                    return Err(ExchangeError::Terminal(
                        "native tool-output exchange exceeded cumulative response byte limit",
                    ));
                }
            }
            Message::Binary(_) => {
                return Err(ExchangeError::Terminal(
                    "native tool-output rejected unexpected binary WebSocket frame",
                ));
            }
            Message::Close(_) => {
                return Err(ExchangeError::Retryable(
                    "native tool-output WebSocket closed by peer before response",
                ));
            }
            Message::Frame(_) => {
                return Err(ExchangeError::Terminal(
                    "native tool-output rejected raw WebSocket frame",
                ));
            }
        }
    }
}

#[cfg(unix)]
fn validate_thread_read_result(
    result: &Value,
    cfg: &NativeToolOutputConfig,
) -> Result<ThreadReadStatus, ExchangeError> {
    let Some(thread) = result.get("thread").and_then(Value::as_object) else {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/read response missing thread object",
        ));
    };

    let Some(id) = thread.get("id").and_then(Value::as_str) else {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/read response missing thread.id",
        ));
    };
    if id != cfg.thread_id {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/read thread.id mismatch",
        ));
    }

    let Some(cwd) = thread.get("cwd").and_then(Value::as_str) else {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/read response missing thread.cwd",
        ));
    };
    if Path::new(cwd) != cfg.project_root.as_path() {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/read thread.cwd mismatch",
        ));
    }

    if let Some(model_val) = thread.get("model") {
        if !model_val.is_null() {
            let Some(model) = model_val.as_str() else {
                return Err(ExchangeError::Terminal(
                    "native tool-output thread/read thread.model is not a string",
                ));
            };
            if model != cfg.model {
                return Err(ExchangeError::Terminal(
                    "native tool-output thread/read model mismatch",
                ));
            }
        }
    }

    if let Some(effort_val) = thread.get("reasoningEffort") {
        if !effort_val.is_null() {
            let Some(effort) = effort_val.as_str() else {
                return Err(ExchangeError::Terminal(
                    "native tool-output thread/read thread.reasoningEffort is not a string",
                ));
            };
            if effort != cfg.effort {
                return Err(ExchangeError::Terminal(
                    "native tool-output thread/read reasoningEffort mismatch",
                ));
            }
        }
    }

    let status_type = match thread.get("status") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Object(map)) => map.get("type").and_then(Value::as_str),
        _ => None,
    };
    match status_type {
        Some("idle") => Ok(ThreadReadStatus::Idle),
        Some("active") => Ok(ThreadReadStatus::Active),
        Some("notLoaded") => Err(ExchangeError::Retryable(
            "native tool-output target thread is not loaded in idle or active status",
        )),
        _ => Err(ExchangeError::Terminal(
            "native tool-output target thread is not loaded in idle or active status",
        )),
    }
}

#[cfg(unix)]
fn extract_consistent_string_field<'a>(
    outer: &'a serde_json::Map<String, Value>,
    inner: Option<&'a serde_json::Map<String, Value>>,
    field: &str,
) -> Result<&'a str, ExchangeError> {
    let outer_val = match outer.get(field) {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => {
            return Err(ExchangeError::Terminal(
                "native tool-output thread/resume field has invalid type",
            ))
        }
    };
    let inner_val = match inner.and_then(|m| m.get(field)) {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) => Some(s.as_str()),
        Some(_) => {
            return Err(ExchangeError::Terminal(
                "native tool-output thread/resume field has invalid type",
            ))
        }
    };

    match (outer_val, inner_val) {
        (Some(a), Some(b)) => {
            if a != b {
                Err(ExchangeError::Terminal(
                    "native tool-output thread/resume outer and inner fields disagreed",
                ))
            } else {
                Ok(a)
            }
        }
        (Some(a), None) => Ok(a),
        (None, Some(b)) => Ok(b),
        (None, None) => Err(ExchangeError::Terminal(
            "native tool-output thread/resume missing required field",
        )),
    }
}

#[cfg(unix)]
fn validate_thread_resume_result(
    result: &Value,
    cfg: &NativeToolOutputConfig,
    expected_status: ThreadReadStatus,
) -> Result<(), ExchangeError> {
    let Some(result_obj) = result.as_object() else {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/resume result must be an object",
        ));
    };
    let thread_obj = match result_obj.get("thread") {
        Some(Value::Object(map)) => Some(map),
        Some(Value::Null) | None => None,
        Some(_) => {
            return Err(ExchangeError::Terminal(
                "native tool-output thread/resume thread field must be an object",
            ))
        }
    };

    let id = extract_consistent_string_field(result_obj, thread_obj, "id")?;
    if id != cfg.thread_id {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/resume thread.id mismatch",
        ));
    }

    let cwd = extract_consistent_string_field(result_obj, thread_obj, "cwd")?;
    if Path::new(cwd) != cfg.project_root.as_path() {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/resume cwd mismatch",
        ));
    }

    let model = extract_consistent_string_field(result_obj, thread_obj, "model")?;
    if model != cfg.model {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/resume model mismatch",
        ));
    }

    let effort = extract_consistent_string_field(result_obj, thread_obj, "reasoningEffort")?;
    if effort != cfg.effort {
        return Err(ExchangeError::Terminal(
            "native tool-output thread/resume reasoningEffort mismatch",
        ));
    }

    if cfg.delivery == NativeNotificationDelivery::UserMessageSteer {
        let status_val = thread_obj.and_then(|thread| thread.get("status"));
        let status = match status_val {
            Some(Value::String(value)) => Some(value.as_str()),
            Some(Value::Object(value)) => value.get("type").and_then(Value::as_str),
            _ => None,
        };
        if status != Some(expected_status.as_str()) {
            return Err(ExchangeError::Terminal(
                "native user-message current thread status missing or changed before send",
            ));
        }
    }

    Ok(())
}

#[cfg(unix)]
fn extract_in_progress_turn_id(result: &Value) -> Result<String, ExchangeError> {
    let Some(result_obj) = result.as_object() else {
        return Err(ExchangeError::Terminal(
            "native user-message thread/turns/list result must be an object",
        ));
    };

    if let Some(err_val) = result_obj.get("error") {
        if !err_val.is_null() {
            return Err(ExchangeError::Terminal(
                "native user-message thread/turns/list reported an error",
            ));
        }
    }

    let Some(data) = result_obj.get("data").and_then(Value::as_array) else {
        return Err(ExchangeError::Terminal(
            "native user-message thread/turns/list response missing data array",
        ));
    };
    if data.len() != 1 {
        return Err(ExchangeError::Terminal(
            "native user-message thread/turns/list did not return exactly one turn summary",
        ));
    }

    let Some(turn_obj) = data[0].as_object() else {
        return Err(ExchangeError::Terminal(
            "native user-message thread/turns/list summary entry must be an object",
        ));
    };

    if let Some(turn_err) = turn_obj.get("error") {
        if !turn_err.is_null() {
            return Err(ExchangeError::Terminal(
                "native user-message latest turn summary reported an error",
            ));
        }
    }

    let status_str = match turn_obj.get("status") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Object(map)) => map.get("type").and_then(Value::as_str),
        _ => None,
    };
    if status_str != Some("inProgress") {
        return Err(ExchangeError::Terminal(
            "native user-message latest turn summary is not inProgress",
        ));
    }

    let Some(turn_id) = turn_obj.get("id").and_then(Value::as_str) else {
        return Err(ExchangeError::Terminal(
            "native user-message latest turn summary missing id",
        ));
    };
    validate_canonical_non_nil_uuid(turn_id, "turn.id").map_err(|_| {
        ExchangeError::Terminal("native user-message latest turn summary has invalid turn.id")
    })?;

    Ok(turn_id.to_string())
}

#[cfg(unix)]
fn validate_turn_steer_result(result: &Value, expected_turn_id: &str) -> Result<(), ExchangeError> {
    let Some(result_obj) = result.as_object() else {
        return Err(ExchangeError::Terminal(
            "native user-message turn/steer result must be an object",
        ));
    };

    if let Some(err_val) = result_obj.get("error") {
        if !err_val.is_null() {
            return Err(ExchangeError::Terminal(
                "native user-message turn/steer reported an error",
            ));
        }
    }

    let outer_turn_id = match result_obj.get("turnId") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Null) | None => None,
        Some(_) => {
            return Err(ExchangeError::Terminal(
                "native user-message turn/steer turnId field must be a string",
            ))
        }
    };

    let inner_turn_id = match result_obj.get("turn") {
        Some(Value::Object(turn_obj)) => {
            if let Some(turn_err) = turn_obj.get("error") {
                if !turn_err.is_null() {
                    return Err(ExchangeError::Terminal(
                        "native user-message turn/steer turn reported an error",
                    ));
                }
            }
            if let Some(status_val) = turn_obj.get("status") {
                let status_str = match status_val {
                    Value::String(s) => Some(s.as_str()),
                    Value::Object(map) => map.get("type").and_then(Value::as_str),
                    Value::Null => None,
                    _ => {
                        return Err(ExchangeError::Terminal(
                            "native user-message turn/steer turn.status has invalid format",
                        ))
                    }
                };
                if let Some(status) = status_str {
                    if matches!(
                        status,
                        "failed" | "error" | "errored" | "cancelled" | "canceled" | "rejected"
                    ) {
                        return Err(ExchangeError::Terminal(
                            "native user-message turn/steer returned a rejected turn status",
                        ));
                    }
                }
            }
            match turn_obj.get("id") {
                Some(Value::String(s)) => Some(s.as_str()),
                Some(Value::Null) | None => None,
                Some(_) => {
                    return Err(ExchangeError::Terminal(
                        "native user-message turn/steer turn.id field must be a string",
                    ))
                }
            }
        }
        Some(Value::Null) | None => None,
        Some(_) => {
            return Err(ExchangeError::Terminal(
                "native user-message turn/steer turn field must be an object",
            ))
        }
    };

    let acked_turn_id = match (outer_turn_id, inner_turn_id) {
        (Some(a), Some(b)) => {
            if a != b {
                return Err(ExchangeError::Terminal(
                    "native user-message turn/steer turnId and turn.id disagreed",
                ));
            }
            a
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => {
            return Err(ExchangeError::Terminal(
                "native user-message turn/steer response missing turnId",
            ))
        }
    };

    validate_canonical_non_nil_uuid(acked_turn_id, "turnId").map_err(|_| {
        ExchangeError::Terminal("native user-message turn/steer returned invalid turnId")
    })?;

    if acked_turn_id != expected_turn_id {
        return Err(ExchangeError::Terminal(
            "native user-message turn/steer turnId did not match expectedTurnId",
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn validate_turn_start_result(result: &Value) -> Result<(), ExchangeError> {
    let Some(result_obj) = result.as_object() else {
        return Err(ExchangeError::Terminal(
            "native tool-output turn/start result must be an object",
        ));
    };

    if let Some(err_val) = result_obj.get("error") {
        if !err_val.is_null() {
            return Err(ExchangeError::Terminal(
                "native tool-output turn/start reported an error",
            ));
        }
    }

    let Some(turn_obj) = result_obj.get("turn").and_then(Value::as_object) else {
        return Err(ExchangeError::Terminal(
            "native tool-output turn/start response missing turn object",
        ));
    };

    if let Some(turn_err) = turn_obj.get("error") {
        if !turn_err.is_null() {
            return Err(ExchangeError::Terminal(
                "native tool-output turn/start turn reported an error",
            ));
        }
    }

    if let Some(status_val) = turn_obj.get("status") {
        let status_str = match status_val {
            Value::String(s) => Some(s.as_str()),
            Value::Object(map) => map.get("type").and_then(Value::as_str),
            Value::Null => None,
            _ => {
                return Err(ExchangeError::Terminal(
                    "native tool-output turn/start turn.status has invalid format",
                ))
            }
        };
        if let Some(status) = status_str {
            if matches!(
                status,
                "failed" | "error" | "errored" | "cancelled" | "canceled" | "rejected"
            ) {
                return Err(ExchangeError::Terminal(
                    "native tool-output turn/start returned a rejected turn status",
                ));
            }
        }
    }

    let Some(turn_id) = turn_obj.get("id").and_then(Value::as_str) else {
        return Err(ExchangeError::Terminal(
            "native tool-output turn/start response missing turn.id",
        ));
    };
    validate_canonical_non_nil_uuid(turn_id, "turn.id").map_err(|_| {
        ExchangeError::Terminal("native tool-output turn/start returned invalid turn.id")
    })?;

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
            delivery: NativeNotificationDelivery::ToolOutput,
            reconnect: None,
        };

        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("secret-socket-dir"));
        assert!(!dbg.contains("01968f52-8871-7d51-9914-79d2bc48eb5a"));
        assert!(!dbg.contains("secret-project-root"));
        assert!(!dbg.contains("secret-model-id"));
        assert!(dbg.contains("[REDACTED]"));

        let digest = cfg.digest();
        assert_eq!(
            digest, "5869029fab09b993d9ccb2f1422ebbc5bf4a1c95406647dd8d87ce8bd961d518",
            "legacy context digest changed"
        );
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert!(!digest.contains("secret"));
        assert_eq!(digest, cfg.digest());

        let legacy_json = serde_json::to_string(&cfg).expect("serialize legacy config");
        assert!(!legacy_json.contains("reconnect"));
        assert!(!legacy_json.contains("delivery"));
        let round_trip: NativeToolOutputConfig =
            serde_json::from_str(&legacy_json).expect("deserialize legacy config");
        assert_eq!(round_trip, cfg);
        assert_eq!(round_trip.digest(), digest);

        let steer_cfg = cfg
            .clone()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let steer_json = serde_json::to_string(&steer_cfg).expect("serialize steer config");
        assert!(steer_json.contains(r#""delivery":"user_message_steer""#));
        let steer_round_trip: NativeToolOutputConfig =
            serde_json::from_str(&steer_json).expect("deserialize steer config");
        assert_eq!(steer_round_trip, steer_cfg);
        assert_ne!(steer_cfg.digest(), digest);

        let mut with_reconnect = cfg.clone();
        with_reconnect.reconnect = Some(NativeReconnectConfig {
            descriptor_root: PathBuf::from("/tmp/secret-descriptor-root"),
            backend_executable: PathBuf::from("/tmp/secret-backend-exe"),
            application_executable: PathBuf::from("/tmp/secret-application-exe"),
        });
        let reconnect_dbg = format!("{with_reconnect:?}");
        assert!(!reconnect_dbg.contains("secret-descriptor-root"));
        assert!(!reconnect_dbg.contains("secret-backend-exe"));
        assert!(!reconnect_dbg.contains("secret-application-exe"));
        assert_ne!(with_reconnect.digest(), digest);
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
            delivery: NativeNotificationDelivery::ToolOutput,
            reconnect: None,
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
