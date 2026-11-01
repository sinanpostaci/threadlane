#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-codex-bridge`: self-contained native Codex `app-server` stdio JSONL to private
//! Unix WebSocket bridge library and CLI shim.
//!
//! ## Contract and Security Invariants
//!
//! - **Single backend launch with opaque argument preservation**: Intercepts only known safe
//!   stdio `app-server` CLI launch forms (`plan_backend_argv`), preserves all whitelisted prefix
//!   and suffix flags and opaque option values in exact order, strips any explicit stdio listener
//!   flag (`--stdio`, `--listen stdio://`, `--listen=stdio://`), and appends a single private
//!   Unix WebSocket listener (`--listen unix://<XDG_RUNTIME_DIR>/tl-codex-<random>/s.sock`).
//! - **Unmodified passthrough for all other invocations**: Any unknown flag, helper subcommand
//!   (`proxy`, `startup-sign-out`, `--print-startup-config`, `--help`, `-h`), non-stdio listener,
//!   duplicate listener flag, or non-`app-server` command execs `THREADLANE_REAL_CODEX` directly
//!   with `argv` preserved byte-for-byte and without creating runtime directories or sockets.
//! - **No raw private diagnostics**: Errors emit only the fixed diagnostic line
//!   ([`FIXED_ERROR_DIAGNOSTIC`]) on `stderr`. Error values carry only static reason strings and
//!   never format protocol payloads, opaque config arguments, paths, environment variables, or
//!   credentials.
//! - **Strict framing and bounded memory**: Bridges newline-delimited UTF-8 JSONL on `stdin`/`stdout`
//!   to RFC 6455 WebSocket text frames over the Unix domain socket with an 8 MiB per-line/per-frame
//!   limit ([`MAX_FRAME_BYTES`]), bounded queue backpressure ([`WS_MAX_QUEUE`]), and fail-closed
//!   rejection of binary frames, invalid UTF-8, and embedded newlines (`\n` or `\r`).
//! - **Private runtime directory and owned cleanup**: Requires a same-UID `0700` non-symlink
//!   `XDG_RUNTIME_DIR`, creates a `0700` `tl-codex-*` session directory, validates the ready socket
//!   (supporting same-UID symlink sockets into a `0700` real parent directory), writes a `0600`
//!   `descriptor.json` only after WebSocket handshake and socket revalidation, and terminates/reaps
//!   the dedicated backend process group while removing only owned session artifacts on exit.

use std::convert::Infallible;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::process::Child;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::tungstenite::Error as WsError;

/// Canonical binary name for the native Rust bridge shim.
pub const BRIDGE_BINARY_NAME: &str = "threadlane-codex-bridge";

/// Package version of `threadlane-codex-bridge`.
pub const BRIDGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Environment variable selecting the absolute executable path of the real Codex binary.
pub const REAL_CODEX_ENV: &str = "THREADLANE_REAL_CODEX";

/// Environment variable selecting the private `0700` runtime directory for per-run session state.
pub const RUNTIME_DIR_ENV: &str = "XDG_RUNTIME_DIR";

/// Directory name prefix for per-run session directories created under `XDG_RUNTIME_DIR`.
pub const SESSION_DIR_PREFIX: &str = "tl-codex-";

/// Advertised Unix domain socket filename inside the per-run session directory.
pub const SOCKET_FILENAME: &str = "s.sock";

/// Per-run descriptor filename written inside the private session directory after handshake.
pub const DESCRIPTOR_FILENAME: &str = "descriptor.json";

/// Schema version recorded in `descriptor.json`.
pub const DESCRIPTOR_SCHEMA_VERSION: u32 = 1;

/// Transport identifier recorded in `descriptor.json`.
pub const DESCRIPTOR_TRANSPORT: &str = "unix_websocket";

/// Prefix written to `stderr` announcing the path to `descriptor.json` once ready.
pub const DESCRIPTOR_STDERR_PREFIX: &str = "threadlane-prototype-descriptor: ";

/// Exclusive upper bound on the byte length of the advertised Unix domain socket path.
pub const MAX_SOCKET_PATH_BYTES: usize = 100;

/// Maximum permitted byte length (`8 MiB`) for a single JSONL line or WebSocket text frame.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Chunk size (`64 KiB`) used for bounded incremental `stdin` reads.
pub const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Maximum queued items (`16`) for bounded backpressure between `stdin` and the WebSocket sink.
pub const WS_MAX_QUEUE: usize = 16;

/// Default timeout (`10s`) while polling for the backend Unix socket to become ready.
pub const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Polling interval (`20ms`) while waiting for the backend Unix socket or process exit.
pub const READY_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Timeout (`5s`) for connecting and completing the RFC 6455 WebSocket handshake.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout (`2s`) for completing a clean WebSocket close handshake after `stdin` EOF.
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Grace period (`2s`) after sending `SIGTERM` to the backend process group before `SIGKILL`.
pub const TERM_TIMEOUT: Duration = Duration::from_secs(2);

/// Wait timeout (`2s`) after sending `SIGKILL` to the backend process group.
pub const KILL_TIMEOUT: Duration = Duration::from_secs(2);

/// Fixed diagnostic line written to `stderr` on any launch, validation, or bridge error.
pub const FIXED_ERROR_DIAGNOSTIC: &str = "threadlane-prototype: backend launch or bridge failed\n";

/// Canonical minimal intercepted `app-server` argument shapes.
pub const INTERCEPTED_ARGVS: &[&[&str]] = &[
    &["app-server"],
    &["app-server", "--listen", "stdio://"],
    &["app-server", "--listen=stdio://"],
    &["app-server", "--stdio"],
];

/// Whitelisted two-token opaque flags accepted before and after `app-server`.
pub const OPAQUE_PAIR_FLAGS: &[&str] = &["-c", "--config", "--enable", "--disable"];

/// Whitelisted boolean flags accepted before `app-server`.
pub const PREFIX_BOOL_FLAGS: &[&str] = &["--strict-config"];

/// Whitelisted boolean flags accepted after `app-server`.
pub const SUFFIX_BOOL_FLAGS: &[&str] = &["--analytics-default-enabled", "--strict-config"];

const VERSION_LINE: &str = concat!("threadlane-codex-bridge ", env!("CARGO_PKG_VERSION"), "\n");

const HELP_TEXT: &str = concat!(
    "threadlane-codex-bridge ",
    env!("CARGO_PKG_VERSION"),
    "\n",
    "Native Codex stdio JSONL to private Unix WebSocket bridge shim.\n\n",
    "USAGE:\n",
    "  threadlane-codex-bridge [CODEX_ARGS...]\n\n",
    "ENVIRONMENT:\n",
    "  THREADLANE_REAL_CODEX  Absolute path to the original Codex executable.\n",
    "  XDG_RUNTIME_DIR        Private (0700) same-UID runtime directory for intercepted sessions.\n"
);

const WS_URI: &str = "ws://localhost/";
const MAX_CONTROL_FRAME_BYTES: usize = 125;

/// Returns the static offline version line (`"threadlane-codex-bridge <version>\n"`).
pub const fn version_text() -> &'static str {
    VERSION_LINE
}

/// Returns the static offline help text for `threadlane-codex-bridge`.
pub const fn help_text() -> &'static str {
    HELP_TEXT
}

/// Checks whether the CLI invocation requests offline bridge help or version output.
///
/// Offline `--help`/`-h` and `--version`/`-V` are handled directly when `THREADLANE_REAL_CODEX`
/// is unset or empty, and `--threadlane-bridge-help` / `--threadlane-bridge-version` are always
/// handled offline without requiring a configured backend. When `THREADLANE_REAL_CODEX` is set,
/// standard `--help` and `--version` pass through to the real Codex binary unchanged.
pub fn check_offline_cli_flag<S: AsRef<OsStr>>(
    args: &[S],
    real_codex_env: Option<&OsStr>,
) -> Option<&'static str> {
    if args.len() != 1 {
        return None;
    }
    let arg = args[0].as_ref().to_str()?;
    match arg {
        "--threadlane-bridge-version" => return Some(version_text()),
        "--threadlane-bridge-help" => return Some(help_text()),
        _ => {}
    }

    let has_configured_backend = real_codex_env.is_some_and(|val| !val.is_empty());
    if has_configured_backend {
        return None;
    }

    match arg {
        "--version" | "-V" => Some(version_text()),
        "--help" | "-h" => Some(help_text()),
        _ => None,
    }
}

/// Raised when a safety precondition, handshake, or bridge invariant fails.
///
/// Contains only a static reason string and process exit code; never stores or formats
/// dynamic paths, CLI arguments, environment variables, or protocol frames.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct BridgeError {
    message: &'static str,
    exit_code: i32,
}

impl BridgeError {
    /// Creates a [`BridgeError`] with exit code `1` and a static, redacted reason string.
    pub const fn new(message: &'static str) -> Self {
        Self {
            message,
            exit_code: 1,
        }
    }

    /// Creates a [`BridgeError`] with an explicit non-zero exit code and static reason string.
    pub const fn with_exit_code(message: &'static str, exit_code: i32) -> Self {
        let code = if exit_code <= 0 { 1 } else { exit_code };
        Self {
            message,
            exit_code: code,
        }
    }

    /// Returns the static, path-free error reason.
    pub const fn message(&self) -> &'static str {
        self.message
    }

    /// Returns the process exit code associated with this failure.
    pub const fn exit_code(&self) -> i32 {
        self.exit_code
    }
}

/// Raised when `SIGTERM`, `SIGINT`, or `SIGHUP` stops the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("signal {signum}")]
pub struct SignalTermination {
    signum: i32,
}

impl SignalTermination {
    /// Creates a [`SignalTermination`] for the given Unix signal number.
    pub const fn new(signum: i32) -> Self {
        Self { signum }
    }

    /// Returns the Unix signal number that stopped the bridge.
    pub const fn signum(&self) -> i32 {
        self.signum
    }

    /// Returns the conventional shell signal exit code (`128 + signum`).
    pub const fn exit_code(&self) -> i32 {
        128 + self.signum
    }
}

/// Terminal non-zero outcome of an intercepted bridge execution.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BridgeRunTermination {
    /// A safety precondition, handshake, framing, or backend lifecycle error occurred.
    #[error(transparent)]
    Error(#[from] BridgeError),
    /// A Unix termination signal stopped the bridge.
    #[error(transparent)]
    Signal(#[from] SignalTermination),
}

impl BridgeRunTermination {
    /// Returns the process exit code for this termination outcome.
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::Error(err) => err.exit_code(),
            Self::Signal(sig) => sig.exit_code(),
        }
    }
}

/// Private per-run `descriptor.json` payload written inside `<XDG_RUNTIME_DIR>/tl-codex-*/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeDescriptor {
    /// Process ID of the spawned real Codex backend.
    pub backend_pid: u32,
    /// Validated path to the real Codex backend executable.
    pub real_codex: String,
    /// Descriptor schema version (`1`).
    pub schema_version: u32,
    /// Advertised Unix domain socket path inside the private session directory.
    pub socket_path: String,
    /// Transport identifier (`"unix_websocket"`).
    pub transport: String,
    /// Process ID of the `threadlane-codex-bridge` wrapper process.
    pub wrapper_pid: u32,
}

/// Offline-constructible configuration for [` threadlane-codex-bridge `](crate).
///
/// Constructing [`BridgeConfig`] performs no filesystem probes or subprocess spawns.
#[derive(Clone, PartialEq, Eq)]
pub struct BridgeConfig {
    /// Candidate path for `THREADLANE_REAL_CODEX`.
    pub real_codex: Option<PathBuf>,
    /// Candidate path for `XDG_RUNTIME_DIR`.
    pub runtime_root: Option<PathBuf>,
    /// Optional path of the invoked wrapper executable (`argv[0]`) for self-target rejection.
    pub launcher_path: Option<PathBuf>,
    /// Maximum duration to wait for the backend Unix socket to become ready.
    pub ready_timeout: Duration,
    /// Maximum byte length for a single JSONL line or WebSocket frame.
    pub max_line_bytes: usize,
}

impl std::fmt::Debug for BridgeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeConfig")
            .field("real_codex", &"[REDACTED]")
            .field("runtime_root", &"[REDACTED]")
            .field("launcher_path", &"[REDACTED]")
            .field("ready_timeout", &self.ready_timeout)
            .field("max_line_bytes", &self.max_line_bytes)
            .finish()
    }
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl BridgeConfig {
    /// Constructs an empty [`BridgeConfig`] offline without inspecting the environment or disk.
    pub const fn new() -> Self {
        Self {
            real_codex: None,
            runtime_root: None,
            launcher_path: None,
            ready_timeout: READY_TIMEOUT,
            max_line_bytes: MAX_FRAME_BYTES,
        }
    }

    /// Constructs a [`BridgeConfig`] populated from `THREADLANE_REAL_CODEX`, `XDG_RUNTIME_DIR`,
    /// and `argv[0]` without touching the filesystem or spawning any process.
    pub fn from_env() -> Self {
        Self {
            real_codex: std::env::var_os(REAL_CODEX_ENV).map(PathBuf::from),
            runtime_root: std::env::var_os(RUNTIME_DIR_ENV).map(PathBuf::from),
            launcher_path: std::env::args_os().next().map(PathBuf::from),
            ready_timeout: READY_TIMEOUT,
            max_line_bytes: MAX_FRAME_BYTES,
        }
    }

    /// Sets the `real_codex` executable path.
    pub fn with_real_codex(mut self, path: impl Into<PathBuf>) -> Self {
        self.real_codex = Some(path.into());
        self
    }

    /// Sets the private `runtime_root` directory path.
    pub fn with_runtime_root(mut self, path: impl Into<PathBuf>) -> Self {
        self.runtime_root = Some(path.into());
        self
    }

    /// Sets the wrapper `launcher_path` (`argv[0]`) used for self-reference checks.
    pub fn with_launcher_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.launcher_path = Some(path.into());
        self
    }

    /// Sets the readiness timeout for the backend Unix domain socket.
    pub const fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }

    /// Sets the maximum byte length for a single JSONL line or WebSocket text frame.
    pub const fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    /// Returns the static offline version string without requiring a configured backend.
    pub const fn version_text(&self) -> &'static str {
        version_text()
    }

    /// Returns the static offline help text without requiring a configured backend.
    pub const fn help_text(&self) -> &'static str {
        help_text()
    }

    /// Validates `self.real_codex` against `self.launcher_path` and the current executable.
    pub fn validate_real_codex(&self) -> Result<PathBuf, BridgeError> {
        validate_real_codex(self.real_codex.as_deref(), self.launcher_path.as_deref())
    }
}

/// Plans the backend `argv` for known safe stdio `app-server` CLI launch shapes, or returns `None`.
///
/// Recognizes leading global opaque flags (`-c VALUE`, `--config VALUE`, `--config=VALUE`,
/// `--enable VALUE`, `--disable VALUE`, `--strict-config`) before `app-server`, and whitelisted
/// `app-server` suffix flags (`--analytics-default-enabled`, `--strict-config`, the same
/// `-c`/`--config`/`--config=..`/`--enable`/`--disable` forms, and at most one explicit stdio
/// listener `--stdio`, `--listen stdio://`, or `--listen=stdio://`).
///
/// Preserves all opaque tokens and flags in exact order, strips any explicit stdio listener
/// flag/pair, and appends `["--listen", "unix://<socket_path>"]`.
pub fn plan_backend_argv<S, P>(args: &[S], socket_path: P) -> Option<Vec<String>>
where
    S: AsRef<str>,
    P: AsRef<Path>,
{
    let socket_str = socket_path.as_ref().to_str()?;
    let n = args.len();
    if n == 0 {
        return None;
    }

    let mut planned: Vec<String> = Vec::with_capacity(n + 2);
    let mut i = 0usize;
    let mut found_command = false;

    while i < n {
        let token = args[i].as_ref();
        if token == "app-server" {
            planned.push(token.to_string());
            i += 1;
            found_command = true;
            break;
        }
        if OPAQUE_PAIR_FLAGS.contains(&token) {
            if i + 1 >= n {
                return None;
            }
            planned.push(token.to_string());
            planned.push(args[i + 1].as_ref().to_string());
            i += 2;
            continue;
        }
        if token.starts_with("--config=") {
            if token.len() <= "--config=".len() {
                return None;
            }
            planned.push(token.to_string());
            i += 1;
            continue;
        }
        if PREFIX_BOOL_FLAGS.contains(&token) {
            planned.push(token.to_string());
            i += 1;
            continue;
        }
        return None;
    }

    if !found_command {
        return None;
    }

    let mut stdio_listener_count = 0usize;
    while i < n {
        let token = args[i].as_ref();
        if SUFFIX_BOOL_FLAGS.contains(&token) {
            planned.push(token.to_string());
            i += 1;
            continue;
        }
        if OPAQUE_PAIR_FLAGS.contains(&token) {
            if i + 1 >= n {
                return None;
            }
            planned.push(token.to_string());
            planned.push(args[i + 1].as_ref().to_string());
            i += 2;
            continue;
        }
        if token.starts_with("--config=") {
            if token.len() <= "--config=".len() {
                return None;
            }
            planned.push(token.to_string());
            i += 1;
            continue;
        }
        if token == "--stdio" {
            stdio_listener_count += 1;
            if stdio_listener_count > 1 {
                return None;
            }
            i += 1;
            continue;
        }
        if token == "--listen" {
            if i + 1 >= n || args[i + 1].as_ref() != "stdio://" {
                return None;
            }
            stdio_listener_count += 1;
            if stdio_listener_count > 1 {
                return None;
            }
            i += 2;
            continue;
        }
        if token.starts_with("--listen=") {
            if token != "--listen=stdio://" {
                return None;
            }
            stdio_listener_count += 1;
            if stdio_listener_count > 1 {
                return None;
            }
            i += 1;
            continue;
        }
        return None;
    }

    planned.push("--listen".to_string());
    planned.push(format!("unix://{socket_str}"));
    Some(planned)
}

/// Plans backend `argv` from OS strings (`OsStr`), returning `None` if any argument is non-UTF-8
/// or does not match a supported stdio `app-server` launch form.
pub fn plan_backend_argv_os<S, P>(args: &[S], socket_path: P) -> Option<Vec<String>>
where
    S: AsRef<OsStr>,
    P: AsRef<Path>,
{
    let mut utf8_args = Vec::with_capacity(args.len());
    for arg in args {
        utf8_args.push(arg.as_ref().to_str()?);
    }
    plan_backend_argv(&utf8_args, socket_path)
}

/// Returns `true` only for supported stdio `app-server` launch forms.
pub fn is_intercepted_argv<S: AsRef<str>>(args: &[S]) -> bool {
    plan_backend_argv(args, SOCKET_FILENAME).is_some()
}

/// Returns `true` only when `args` are valid UTF-8 and match a supported stdio `app-server` launch form.
pub fn is_intercepted_argv_os<S: AsRef<OsStr>>(args: &[S]) -> bool {
    plan_backend_argv_os(args, SOCKET_FILENAME).is_some()
}

/// Validates that `THREADLANE_REAL_CODEX` is an existing, executable, regular absolute file
/// distinct from this bridge binary and `launcher_path`.
pub fn validate_real_codex<P: AsRef<Path>>(
    real_codex: Option<P>,
    launcher_path: Option<&Path>,
) -> Result<PathBuf, BridgeError> {
    let Some(raw_codex) = real_codex else {
        return Err(BridgeError::new("invalid real codex path"));
    };
    let codex_path = raw_codex.as_ref();
    if codex_path.as_os_str().is_empty() || !codex_path.is_absolute() {
        return Err(BridgeError::new("invalid real codex path"));
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        if codex_path.as_os_str().as_bytes().contains(&0) {
            return Err(BridgeError::new("invalid real codex path"));
        }
    }

    let meta = fs::metadata(codex_path).map_err(|_| BridgeError::new("real codex missing"))?;
    if !meta.is_file() {
        return Err(BridgeError::new("real codex is not a regular file"));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0
            || rustix::fs::access(codex_path, rustix::fs::Access::EXEC_OK).is_err()
        {
            return Err(BridgeError::new("real codex is not executable"));
        }
    }

    let real_resolved =
        fs::canonicalize(codex_path).map_err(|_| BridgeError::new("real codex missing"))?;

    let mut self_candidates: Vec<PathBuf> = Vec::new();
    if let Ok(current_exe) = std::env::current_exe() {
        self_candidates.push(current_exe);
    }
    if let Some(lp) = launcher_path {
        if !lp.as_os_str().is_empty() {
            self_candidates.push(lp.to_path_buf());
        }
    }

    for candidate in &self_candidates {
        if candidate == codex_path || candidate == &real_resolved {
            return Err(BridgeError::new("real codex resolves to launcher"));
        }
        if let Ok(cand_resolved) = fs::canonicalize(candidate) {
            if cand_resolved == real_resolved || cand_resolved == codex_path {
                return Err(BridgeError::new("real codex resolves to launcher"));
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if let Ok(cand_meta) = fs::metadata(candidate) {
                if (cand_meta.dev(), cand_meta.ino()) == (meta.dev(), meta.ino()) {
                    return Err(BridgeError::new("real codex resolves to launcher"));
                }
            }
            if let Ok(cand_sym) = fs::symlink_metadata(candidate) {
                if (cand_sym.dev(), cand_sym.ino()) == (meta.dev(), meta.ino()) {
                    return Err(BridgeError::new("real codex resolves to launcher"));
                }
            }
        }
    }

    Ok(codex_path.to_path_buf())
}

/// Replaces the current process image with `real_codex` on Unix, preserving `args` byte-for-byte.
pub fn exec_passthrough<S: AsRef<OsStr>>(
    real_codex: &Path,
    args: &[S],
) -> Result<Infallible, BridgeError> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let _err = std::process::Command::new(real_codex).args(args).exec();
        Err(BridgeError::new("failed to exec passthrough"))
    }
    #[cfg(not(unix))]
    {
        let _ = (real_codex, args);
        Err(BridgeError::new("unix platform required"))
    }
}

#[cfg(unix)]
fn current_uid() -> u32 {
    rustix::process::getuid().as_raw()
}

#[cfg(unix)]
fn normalize_lexical_path(path: &Path) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStrExt;
    if path.as_os_str().is_empty()
        || !path.is_absolute()
        || path.as_os_str().as_bytes().contains(&0)
    {
        return None;
    }
    let mut normalized = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(Component::RootDir.as_os_str());
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Some(normalized)
}

/// Requires `XDG_RUNTIME_DIR` to be an existing, non-symlink `0700` directory owned by `uid`.
pub fn validate_runtime_root<P: AsRef<Path>>(
    runtime_root: Option<P>,
    uid: Option<u32>,
) -> Result<PathBuf, BridgeError> {
    #[cfg(not(unix))]
    {
        let _ = (runtime_root, uid);
        Err(BridgeError::new("unix platform required"))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let expected_uid = uid.unwrap_or_else(current_uid);
        let Some(raw_root) = runtime_root else {
            return Err(BridgeError::new("missing or relative runtime root"));
        };
        let Some(normalized) = normalize_lexical_path(raw_root.as_ref()) else {
            return Err(BridgeError::new("missing or relative runtime root"));
        };

        let st = fs::symlink_metadata(&normalized)
            .map_err(|_| BridgeError::new("runtime root unavailable"))?;
        if st.file_type().is_symlink() || !st.file_type().is_dir() {
            return Err(BridgeError::new("runtime root must be a real directory"));
        }

        let canonical = fs::canonicalize(&normalized)
            .map_err(|_| BridgeError::new("runtime root unavailable"))?;
        if canonical != normalized {
            return Err(BridgeError::new("runtime root must not traverse symlinks"));
        }

        if st.uid() != expected_uid {
            return Err(BridgeError::new("runtime root uid mismatch"));
        }

        if (st.permissions().mode() & 0o7777) != 0o700 {
            return Err(BridgeError::new("runtime root permissions must be 0700"));
        }

        Ok(normalized)
    }
}

/// Tracks owned per-run artifacts by path and `(dev, ino)` for safe non-recursive cleanup.
#[derive(Debug)]
pub struct SessionCleanupTracker {
    session_dir: PathBuf,
    session_dir_id: (u64, u64),
    advertised_socket_path: PathBuf,
    owner_uid: u32,
    descriptor_path: Option<PathBuf>,
    descriptor_id: Option<(u64, u64)>,
}

impl SessionCleanupTracker {
    /// Creates a new [`SessionCleanupTracker`] for an owned session directory.
    pub const fn new(
        session_dir: PathBuf,
        session_dir_id: (u64, u64),
        advertised_socket_path: PathBuf,
        owner_uid: u32,
    ) -> Self {
        Self {
            session_dir,
            session_dir_id,
            advertised_socket_path,
            owner_uid,
            descriptor_path: None,
            descriptor_id: None,
        }
    }

    /// Returns the path of the per-run `0700` session directory.
    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    /// Returns the `(dev, ino)` identity of the session directory at creation time.
    pub const fn session_dir_id(&self) -> (u64, u64) {
        self.session_dir_id
    }

    /// Returns the advertised Unix domain socket path (`<session_dir>/s.sock`).
    pub fn advertised_socket_path(&self) -> &Path {
        &self.advertised_socket_path
    }

    /// Returns the expected owner UID for all session artifacts.
    pub const fn owner_uid(&self) -> u32 {
        self.owner_uid
    }

    /// Returns the recorded `descriptor.json` path if written.
    pub fn descriptor_path(&self) -> Option<&Path> {
        self.descriptor_path.as_deref()
    }

    /// Returns the recorded `(dev, ino)` identity of `descriptor.json` if written.
    pub const fn descriptor_id(&self) -> Option<(u64, u64)> {
        self.descriptor_id
    }

    /// Records the created `descriptor.json` path and `(dev, ino)` identity for owned cleanup.
    pub fn record_descriptor(&mut self, path: PathBuf, file_id: (u64, u64)) {
        self.descriptor_path = Some(path);
        self.descriptor_id = Some(file_id);
    }

    /// Returns `true` if `session_dir` still exists as a non-symlink directory with matching `(dev, ino)` and `owner_uid`.
    pub fn session_dir_still_owned(&self) -> bool {
        #[cfg(not(unix))]
        {
            false
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let Ok(st) = fs::symlink_metadata(&self.session_dir) else {
                return false;
            };
            st.file_type().is_dir()
                && !st.file_type().is_symlink()
                && (st.dev(), st.ino()) == self.session_dir_id
                && st.uid() == self.owner_uid
        }
    }

    /// Removes only the owned `descriptor.json`, advertised socket/symlink, and empty session directory.
    ///
    /// Never performs recursive deletion or removes targets outside `session_dir`.
    pub fn cleanup(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{FileTypeExt, MetadataExt};

            if !self.session_dir_still_owned() {
                return;
            }

            if let (Some(ref desc_path), Some(desc_id)) =
                (&self.descriptor_path, self.descriptor_id)
            {
                if let Ok(dst) = fs::symlink_metadata(desc_path) {
                    if (dst.dev(), dst.ino()) == desc_id
                        && dst.uid() == self.owner_uid
                        && dst.file_type().is_file()
                        && !dst.file_type().is_symlink()
                    {
                        let _ = fs::remove_file(desc_path);
                    }
                }
            }

            if self.advertised_socket_path.parent() == Some(self.session_dir.as_path()) {
                if let Ok(sst) = fs::symlink_metadata(&self.advertised_socket_path) {
                    if sst.uid() == self.owner_uid
                        && (sst.file_type().is_socket() || sst.file_type().is_symlink())
                    {
                        let _ = fs::remove_file(&self.advertised_socket_path);
                    }
                }
            }

            if self.session_dir_still_owned() {
                // Non-recursive rmdir preserves the directory if unexpected files are present.
                let _ = fs::remove_dir(&self.session_dir);
            }
        }
    }
}

impl Drop for SessionCleanupTracker {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Creates a fresh `0700` per-run directory under `runtime_root` and verifies the socket path length limit.
pub fn create_private_session_dir<P: AsRef<Path>>(
    runtime_root: P,
    uid: Option<u32>,
) -> Result<SessionCleanupTracker, BridgeError> {
    #[cfg(not(unix))]
    {
        let _ = (runtime_root, uid);
        Err(BridgeError::new("unix platform required"))
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let expected_uid = uid.unwrap_or_else(current_uid);
        let validated_root =
            validate_runtime_root(Some(runtime_root.as_ref()), Some(expected_uid))?;

        let _ = rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
        let temp_dir = tempfile::Builder::new()
            .prefix(SESSION_DIR_PREFIX)
            .tempdir_in(&validated_root)
            .map_err(|_| BridgeError::new("failed to secure private session dir"))?;
        let session_dir = temp_dir.keep();

        if fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700)).is_err() {
            let _ = fs::remove_dir(&session_dir);
            return Err(BridgeError::new("failed to secure private session dir"));
        }

        let st = match fs::symlink_metadata(&session_dir) {
            Ok(st) => st,
            Err(_) => {
                let _ = fs::remove_dir(&session_dir);
                return Err(BridgeError::new("failed to secure private session dir"));
            }
        };

        if !st.file_type().is_dir()
            || st.file_type().is_symlink()
            || st.uid() != expected_uid
            || (st.permissions().mode() & 0o7777) != 0o700
        {
            let _ = fs::remove_dir(&session_dir);
            return Err(BridgeError::new("failed to secure private session dir"));
        }

        let socket_path = session_dir.join(SOCKET_FILENAME);
        let mut tracker = SessionCleanupTracker::new(
            session_dir,
            (st.dev(), st.ino()),
            socket_path.clone(),
            expected_uid,
        );

        if socket_path.as_os_str().as_bytes().len() >= MAX_SOCKET_PATH_BYTES {
            tracker.cleanup();
            return Err(BridgeError::new(
                "socket path exceeds unix domain socket length limit",
            ));
        }

        Ok(tracker)
    }
}

/// Checks whether the advertised socket exists and satisfies all ownership and permission invariants.
pub fn validate_ready_socket(tracker: &SessionCleanupTracker) -> Result<bool, BridgeError> {
    #[cfg(not(unix))]
    {
        let _ = tracker;
        Err(BridgeError::new("unix platform required"))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

        if !tracker.session_dir_still_owned() {
            return Err(BridgeError::new(
                "private session directory replaced or modified",
            ));
        }

        let socket_path = &tracker.advertised_socket_path;
        let norm_socket = normalize_lexical_path(socket_path).ok_or_else(|| {
            BridgeError::new("advertised socket outside private session directory")
        })?;
        let norm_session = normalize_lexical_path(&tracker.session_dir)
            .ok_or_else(|| BridgeError::new("private session directory replaced or modified"))?;
        if norm_socket.parent() != Some(norm_session.as_path()) {
            return Err(BridgeError::new(
                "advertised socket outside private session directory",
            ));
        }

        let adv_st = match fs::symlink_metadata(socket_path) {
            Ok(st) => st,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(_) => return Err(BridgeError::new("unable to stat advertised socket")),
        };

        if adv_st.uid() != tracker.owner_uid {
            return Err(BridgeError::new("advertised socket owner mismatch"));
        }

        let (resolved_path, target_st) = if adv_st.file_type().is_symlink() {
            let resolved = match fs::canonicalize(socket_path) {
                Ok(p) => p,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(_) => return Err(BridgeError::new("unable to stat resolved socket target")),
            };
            let target_st = match fs::symlink_metadata(&resolved) {
                Ok(st) => st,
                Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(_) => return Err(BridgeError::new("unable to stat resolved socket target")),
            };
            (resolved, target_st)
        } else if adv_st.file_type().is_socket() {
            let resolved = fs::canonicalize(socket_path)
                .map_err(|_| BridgeError::new("unexpected path resolution for direct socket"))?;
            if resolved != norm_socket {
                return Err(BridgeError::new(
                    "unexpected path resolution for direct socket",
                ));
            }
            (resolved, adv_st)
        } else {
            return Err(BridgeError::new(
                "advertised path is neither socket nor symlink",
            ));
        };

        if !target_st.file_type().is_socket() || target_st.file_type().is_symlink() {
            return Err(BridgeError::new(
                "resolved socket target is not a unix socket",
            ));
        }

        if target_st.uid() != tracker.owner_uid {
            return Err(BridgeError::new("resolved socket target owner mismatch"));
        }

        if (target_st.permissions().mode() & 0o077) != 0 {
            return Err(BridgeError::new(
                "resolved socket target has group or world permissions",
            ));
        }

        let Some(target_parent) = resolved_path.parent() else {
            return Err(BridgeError::new("unable to stat resolved socket parent"));
        };
        let parent_st = fs::symlink_metadata(target_parent)
            .map_err(|_| BridgeError::new("unable to stat resolved socket parent"))?;

        if !parent_st.file_type().is_dir() || parent_st.file_type().is_symlink() {
            return Err(BridgeError::new(
                "resolved socket parent is not a real directory",
            ));
        }

        let canonical_parent = fs::canonicalize(target_parent)
            .map_err(|_| BridgeError::new("resolved socket parent traverses symlinks"))?;
        if canonical_parent != target_parent {
            return Err(BridgeError::new(
                "resolved socket parent traverses symlinks",
            ));
        }

        if parent_st.uid() != tracker.owner_uid {
            return Err(BridgeError::new("resolved socket parent owner mismatch"));
        }

        if (parent_st.permissions().mode() & 0o7777) != 0o700 {
            return Err(BridgeError::new(
                "resolved socket parent permissions must be 0700",
            ));
        }

        Ok(true)
    }
}

/// Writes mode `0600` `descriptor.json` inside the private per-run directory.
pub fn write_descriptor(
    tracker: &mut SessionCleanupTracker,
    backend_pid: u32,
    real_codex: &Path,
) -> Result<PathBuf, BridgeError> {
    #[cfg(not(unix))]
    {
        let _ = (tracker, backend_pid, real_codex);
        Err(BridgeError::new("unix platform required"))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

        if !tracker.session_dir_still_owned() {
            return Err(BridgeError::new(
                "session dir invalid before writing descriptor",
            ));
        }

        let socket_path_str = tracker
            .advertised_socket_path
            .to_str()
            .ok_or_else(|| BridgeError::new("session dir invalid before writing descriptor"))?
            .to_string();
        let real_codex_str = real_codex
            .to_str()
            .ok_or_else(|| BridgeError::new("invalid real codex path"))?
            .to_string();

        let descriptor_path = tracker.session_dir.join(DESCRIPTOR_FILENAME);
        let payload = BridgeDescriptor {
            backend_pid,
            real_codex: real_codex_str,
            schema_version: DESCRIPTOR_SCHEMA_VERSION,
            socket_path: socket_path_str,
            transport: DESCRIPTOR_TRANSPORT.to_string(),
            wrapper_pid: std::process::id(),
        };

        let mut encoded = serde_json::to_string_pretty(&payload)
            .map_err(|_| BridgeError::new("session dir invalid before writing descriptor"))?;
        encoded.push('\n');

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(&descriptor_path)
            .map_err(|_| BridgeError::new("session dir invalid before writing descriptor"))?;

        file.write_all(encoded.as_bytes())
            .and_then(|()| file.flush())
            .and_then(|()| file.sync_all())
            .map_err(|_| BridgeError::new("session dir invalid before writing descriptor"))?;
        drop(file);

        fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
            .map_err(|_| BridgeError::new("session dir invalid before writing descriptor"))?;

        let dst = fs::symlink_metadata(&descriptor_path)
            .map_err(|_| BridgeError::new("session dir invalid before writing descriptor"))?;
        if dst.file_type().is_symlink()
            || !dst.file_type().is_file()
            || dst.uid() != tracker.owner_uid
            || (dst.permissions().mode() & 0o7777) != 0o600
        {
            let _ = fs::remove_file(&descriptor_path);
            return Err(BridgeError::new(
                "session dir invalid before writing descriptor",
            ));
        }

        tracker.record_descriptor(descriptor_path.clone(), (dst.dev(), dst.ino()));
        Ok(descriptor_path)
    }
}

/// Maps a child process [`ExitStatus`] to a non-zero exit code matching Python launcher semantics.
pub fn exit_code_from_status(status: Option<ExitStatus>) -> i32 {
    let Some(st) = status else {
        return 1;
    };
    if let Some(code) = st.code() {
        return if code <= 0 { 1 } else { code };
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = st.signal() {
            if sig > 0 {
                return 128 + sig;
            }
        }
    }
    1
}

/// Terminates and reaps only the dedicated backend process group created by this bridge.
pub async fn terminate_backend_process_group(
    child: &mut Child,
    backend_pid: u32,
    term_timeout: Duration,
    kill_timeout: Duration,
) {
    #[cfg(unix)]
    {
        use rustix::process::{kill_process, kill_process_group, Pid, Signal};

        let pgid = i32::try_from(backend_pid)
            .ok()
            .and_then(Pid::from_raw)
            .filter(|p| p.as_raw_nonzero().get() > 1);

        if let Some(pid) = pgid {
            if kill_process_group(pid, Signal::TERM).is_err() {
                let _ = kill_process(pid, Signal::TERM);
            }
        }

        if tokio::time::timeout(term_timeout, child.wait())
            .await
            .is_err()
        {
            if let Some(pid) = pgid {
                if kill_process_group(pid, Signal::KILL).is_err() {
                    let _ = kill_process(pid, Signal::KILL);
                }
            }
            let _ = child.start_kill();
            let _ = tokio::time::timeout(kill_timeout, child.wait()).await;
        }

        if let Some(pid) = pgid {
            let _ = kill_process_group(pid, Signal::KILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (backend_pid, term_timeout);
        let _ = child.start_kill();
        let _ = tokio::time::timeout(kill_timeout, child.wait()).await;
    }
}

/// Reads a single newline-delimited line from `reader` while bounding buffered memory to `max_line_bytes`.
pub async fn read_bounded_line<R>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    max_line_bytes: usize,
    chunk_size: usize,
) -> Result<Option<Vec<u8>>, BridgeError>
where
    R: AsyncRead + Unpin,
{
    let step_chunk = chunk_size.max(1);
    let mut chunk = vec![0u8; step_chunk];

    loop {
        if let Some(nl_pos) = buf.iter().position(|&b| b == b'\n') {
            if nl_pos > max_line_bytes {
                buf.clear();
                return Err(BridgeError::new("stdin line exceeds maximum size"));
            }
            let raw_line = buf[..nl_pos].to_vec();
            buf.drain(..=nl_pos);
            return Ok(Some(raw_line));
        }

        if buf.len() > max_line_bytes {
            buf.clear();
            return Err(BridgeError::new("stdin line exceeds maximum size"));
        }

        let n = reader
            .read(&mut chunk)
            .await
            .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            if buf.len() > max_line_bytes {
                buf.clear();
                return Err(BridgeError::new("stdin line exceeds maximum size"));
            }
            let raw_line = std::mem::take(buf);
            return Ok(Some(raw_line));
        }

        buf.extend_from_slice(&chunk[..n]);
        if !buf.contains(&b'\n') && buf.len() > max_line_bytes {
            buf.clear();
            return Err(BridgeError::new("stdin line exceeds maximum size"));
        }
    }
}

struct ChannelAsyncReader {
    rx: tokio::sync::mpsc::Receiver<io::Result<Vec<u8>>>,
    current: Vec<u8>,
    offset: usize,
    eof: bool,
}

impl ChannelAsyncReader {
    fn new(rx: tokio::sync::mpsc::Receiver<io::Result<Vec<u8>>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            offset: 0,
            eof: false,
        }
    }
}

impl AsyncRead for ChannelAsyncReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.eof || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.offset < self.current.len() {
            let available = &self.current[self.offset..];
            let take = available.len().min(buf.remaining());
            buf.put_slice(&available[..take]);
            self.offset += take;
            if self.offset >= self.current.len() {
                self.current.clear();
                self.offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(chunk))) => {
                if chunk.is_empty() {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
                let take = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..take]);
                if take < chunk.len() {
                    self.current = chunk;
                    self.offset = take;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Err(err)),
        }
    }
}

struct StdStdoutWriter;

impl AsyncWrite for StdStdoutWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle.write_all(buf)?;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle.flush()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        handle.flush()?;
        Poll::Ready(Ok(()))
    }
}

fn spawn_stdin_channel_reader() -> ChannelAsyncReader {
    let (tx, rx) = tokio::sync::mpsc::channel(WS_MAX_QUEUE);
    let _ = std::thread::Builder::new()
        .name("tl-codex-stdin".to_string())
        .spawn(move || {
            let stdin = io::stdin();
            let mut handle = stdin.lock();
            let mut chunk = vec![0u8; READ_CHUNK_BYTES];
            loop {
                match io::Read::read(&mut handle, &mut chunk) {
                    Ok(0) => {
                        let _ = tx.blocking_send(Ok(Vec::new()));
                        break;
                    }
                    Ok(n) => {
                        if tx.blocking_send(Ok(chunk[..n].to_vec())).is_err() {
                            break;
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        let _ = tx.blocking_send(Err(err));
                        break;
                    }
                }
            }
        });
    ChannelAsyncReader::new(rx)
}

#[cfg(unix)]
struct SignalStreams {
    sigterm: tokio::signal::unix::Signal,
    sigint: tokio::signal::unix::Signal,
    sighup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl SignalStreams {
    fn register() -> Result<Self, BridgeError> {
        use tokio::signal::unix::{signal, SignalKind};
        let sigterm = signal(SignalKind::terminate())
            .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
        let sigint = signal(SignalKind::interrupt())
            .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
        let sighup = signal(SignalKind::hangup())
            .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
        Ok(Self {
            sigterm,
            sigint,
            sighup,
        })
    }

    async fn recv(&mut self) -> SignalTermination {
        tokio::select! {
            biased;
            _ = self.sigterm.recv() => SignalTermination::new(15),
            _ = self.sigint.recv() => SignalTermination::new(2),
            _ = self.sighup.recv() => SignalTermination::new(1),
        }
    }
}

#[cfg(unix)]
async fn wait_for_socket_ready(
    child: &mut Child,
    tracker: &SessionCleanupTracker,
    signals: &mut SignalStreams,
    ready_timeout: Duration,
) -> Result<(), BridgeRunTermination> {
    let deadline = Instant::now() + ready_timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(BridgeRunTermination::Error(BridgeError::with_exit_code(
                    "backend exited before socket ready",
                    exit_code_from_status(Some(status)),
                )));
            }
            Ok(None) => {}
            Err(_) => {
                return Err(BridgeRunTermination::Error(BridgeError::new(
                    "backend exited before socket ready",
                )));
            }
        }

        if validate_ready_socket(tracker)? {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(BridgeRunTermination::Error(BridgeError::new(
                "timed out waiting for backend socket",
            )));
        }

        tokio::select! {
            biased;
            sig = signals.recv() => return Err(BridgeRunTermination::Signal(sig)),
            () = tokio::time::sleep(READY_POLL_INTERVAL) => {}
        }
    }
}

#[cfg(unix)]
async fn write_ws_text_to_stdout<W: AsyncWrite + Unpin>(
    message: &str,
    stdout: &mut W,
    max_line_bytes: usize,
) -> Result<(), BridgeError> {
    if message.contains('\n') || message.contains('\r') {
        return Err(BridgeError::new("newline inside websocket frame rejected"));
    }
    let encoded = message.as_bytes();
    if encoded.len() > max_line_bytes {
        return Err(BridgeError::new("websocket frame exceeds maximum size"));
    }

    let mut line = Vec::with_capacity(encoded.len() + 1);
    line.extend_from_slice(encoded);
    line.push(b'\n');

    stdout
        .write_all(&line)
        .await
        .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
    stdout
        .flush()
        .await
        .map_err(|_| BridgeError::new("websocket handshake or bridge error"))?;
    Ok(())
}

/// Runs the intercepted stdio JSONL <-> Unix WebSocket bridge using custom async `stdin` and `stdout` streams.
pub async fn run_intercepted_with_stdio<S, R, W>(
    real_codex: &Path,
    runtime_root: Option<&Path>,
    original_argv: &[S],
    ready_timeout: Duration,
    max_line_bytes: usize,
    stdin_reader: R,
    mut stdout_writer: W,
) -> Result<i32, BridgeRunTermination>
where
    S: AsRef<OsStr>,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    #[cfg(not(unix))]
    {
        let _ = (
            real_codex,
            runtime_root,
            original_argv,
            ready_timeout,
            max_line_bytes,
            stdin_reader,
            stdout_writer,
        );
        Err(BridgeRunTermination::Error(BridgeError::new(
            "unix platform required",
        )))
    }
    #[cfg(unix)]
    {
        let mut signals = SignalStreams::register()?;
        let validated_root = validate_runtime_root(runtime_root, None)?;
        let mut tracker = create_private_session_dir(&validated_root, None)?;

        let Some(backend_argv) =
            plan_backend_argv_os(original_argv, tracker.advertised_socket_path())
        else {
            tracker.cleanup();
            return Err(BridgeRunTermination::Error(BridgeError::new(
                "unsupported app-server argv",
            )));
        };

        let mut cmd = tokio::process::Command::new(real_codex);
        cmd.args(&backend_argv)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .process_group(0);

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(_) => {
                tracker.cleanup();
                return Err(BridgeRunTermination::Error(BridgeError::new(
                    "failed to spawn backend",
                )));
            }
        };

        let Some(backend_pid) = child.id() else {
            terminate_backend_process_group(&mut child, 0, TERM_TIMEOUT, KILL_TIMEOUT).await;
            tracker.cleanup();
            return Err(BridgeRunTermination::Error(BridgeError::new(
                "failed to spawn backend",
            )));
        };

        let outcome = run_spawned_bridge_session(
            real_codex,
            &mut tracker,
            &mut child,
            backend_pid,
            &mut signals,
            ready_timeout,
            max_line_bytes,
            stdin_reader,
            &mut stdout_writer,
        )
        .await;

        terminate_backend_process_group(&mut child, backend_pid, TERM_TIMEOUT, KILL_TIMEOUT).await;
        tracker.cleanup();
        outcome
    }
}

#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
async fn run_spawned_bridge_session<R, W>(
    real_codex: &Path,
    tracker: &mut SessionCleanupTracker,
    child: &mut Child,
    backend_pid: u32,
    signals: &mut SignalStreams,
    ready_timeout: Duration,
    max_line_bytes: usize,
    mut stdin_reader: R,
    stdout_writer: &mut W,
) -> Result<i32, BridgeRunTermination>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    wait_for_socket_ready(child, tracker, signals, ready_timeout).await?;

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(max_line_bytes);
    ws_config.max_frame_size = Some(max_line_bytes);
    ws_config.max_write_buffer_size = max_line_bytes.saturating_add(READ_CHUNK_BYTES);

    let connect_fut = async {
        let stream = tokio::net::UnixStream::connect(tracker.advertised_socket_path()).await?;
        let (ws, _resp) =
            tokio_tungstenite::client_async_with_config(WS_URI, stream, Some(ws_config))
                .await
                .map_err(io::Error::other)?;
        Ok::<_, io::Error>(ws)
    };

    let mut ws = tokio::select! {
        biased;
        sig = signals.recv() => return Err(BridgeRunTermination::Signal(sig)),
        timed = tokio::time::timeout(OPEN_TIMEOUT, connect_fut) => {
            match timed {
                Ok(Ok(ws)) => ws,
                Ok(Err(_)) | Err(_) => {
                    let exit_code = exit_code_from_status(child.try_wait().ok().flatten());
                    return Err(BridgeRunTermination::Error(BridgeError::with_exit_code(
                        "websocket handshake or bridge error",
                        exit_code,
                    )));
                }
            }
        }
    };

    if !validate_ready_socket(tracker)? {
        return Err(BridgeRunTermination::Error(BridgeError::new(
            "socket invalidated during handshake",
        )));
    }

    let descriptor_path = write_descriptor(tracker, backend_pid, real_codex)?;
    let _ = writeln!(
        io::stderr(),
        "{DESCRIPTOR_STDERR_PREFIX}{}",
        descriptor_path.display()
    );
    let _ = io::stderr().flush();

    let (line_tx, mut line_rx) =
        tokio::sync::mpsc::channel::<Result<Option<String>, BridgeError>>(WS_MAX_QUEUE);
    let stdin_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        loop {
            match read_bounded_line(
                &mut stdin_reader,
                &mut buf,
                max_line_bytes,
                READ_CHUNK_BYTES,
            )
            .await
            {
                Ok(Some(raw_line)) => match String::from_utf8(raw_line) {
                    Ok(text) => {
                        if line_tx.send(Ok(Some(text))).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = line_tx
                            .send(Err(BridgeError::new("malformed utf-8 on stdin")))
                            .await;
                        break;
                    }
                },
                Ok(None) => {
                    let _ = line_tx.send(Ok(None)).await;
                    break;
                }
                Err(err) => {
                    let _ = line_tx.send(Err(err)).await;
                    break;
                }
            }
        }
    });

    let bridge_res = run_bidirectional_loop(
        &mut ws,
        &mut line_rx,
        stdout_writer,
        child,
        signals,
        max_line_bytes,
    )
    .await;

    stdin_task.abort();
    bridge_res
}

#[cfg(unix)]
async fn run_bidirectional_loop<W>(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    line_rx: &mut tokio::sync::mpsc::Receiver<Result<Option<String>, BridgeError>>,
    stdout_writer: &mut W,
    child: &mut Child,
    signals: &mut SignalStreams,
    max_line_bytes: usize,
) -> Result<i32, BridgeRunTermination>
where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            sig = signals.recv() => {
                return Err(BridgeRunTermination::Signal(sig));
            }
            ws_item = ws.next() => {
                match ws_item {
                    Some(Ok(Message::Text(text))) => {
                        write_ws_text_to_stdout(text.as_ref(), stdout_writer, max_line_bytes).await?;
                    }
                    Some(Ok(Message::Binary(_))) => {
                        return Err(BridgeRunTermination::Error(BridgeError::new(
                            "binary websocket frame rejected",
                        )));
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if payload.len() > MAX_CONTROL_FRAME_BYTES {
                            return Err(BridgeRunTermination::Error(BridgeError::new(
                                "websocket connection closed abnormally",
                            )));
                        }
                        ws.flush().await.map_err(|_| {
                            BridgeError::new("websocket connection closed abnormally")
                        })?;
                    }
                    Some(Ok(Message::Pong(payload))) => {
                        if payload.len() > MAX_CONTROL_FRAME_BYTES {
                            return Err(BridgeRunTermination::Error(BridgeError::new(
                                "websocket connection closed abnormally",
                            )));
                        }
                    }
                    Some(Ok(Message::Frame(_))) => {
                        return Err(BridgeRunTermination::Error(BridgeError::new(
                            "websocket connection closed abnormally",
                        )));
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        return Err(BridgeRunTermination::Error(
                            backend_closed_before_eof_error(child).await,
                        ));
                    }
                }
            }
            line_item = line_rx.recv() => {
                match line_item {
                    Some(Ok(Some(text_frame))) => {
                        ws.send(Message::Text(text_frame.into())).await.map_err(|_| {
                            BridgeError::new("websocket closed while sending")
                        })?;
                    }
                    Some(Ok(None)) | None => {
                        return finish_after_stdin_eof(
                            ws,
                            stdout_writer,
                            child,
                            signals,
                            max_line_bytes,
                        )
                        .await;
                    }
                    Some(Err(err)) => {
                        return Err(BridgeRunTermination::Error(err));
                    }
                }
            }
            status_res = child.wait() => {
                let code = exit_code_from_status(status_res.ok());
                return Err(BridgeRunTermination::Error(BridgeError::with_exit_code(
                    "backend closed before stdin eof",
                    code,
                )));
            }
        }
    }
}

#[cfg(unix)]
async fn backend_closed_before_eof_error(child: &mut Child) -> BridgeError {
    let status = match child.try_wait() {
        Ok(Some(st)) => Some(st),
        _ => tokio::time::timeout(Duration::from_millis(200), child.wait())
            .await
            .ok()
            .and_then(Result::ok),
    };
    BridgeError::with_exit_code(
        "backend closed before stdin eof",
        exit_code_from_status(status),
    )
}

#[cfg(unix)]
async fn finish_after_stdin_eof<W>(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    stdout_writer: &mut W,
    child: &mut Child,
    signals: &mut SignalStreams,
    max_line_bytes: usize,
) -> Result<i32, BridgeRunTermination>
where
    W: AsyncWrite + Unpin,
{
    let drain_fut = async {
        let _ = ws.send(Message::Close(None)).await;
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    write_ws_text_to_stdout(text.as_ref(), stdout_writer, max_line_bytes).await?;
                }
                Some(Ok(Message::Binary(_))) => {
                    return Err(BridgeError::new("binary websocket frame rejected"));
                }
                Some(Ok(Message::Ping(payload))) => {
                    if payload.len() > MAX_CONTROL_FRAME_BYTES {
                        return Err(BridgeError::new("websocket connection closed abnormally"));
                    }
                    let _ = ws.flush().await;
                }
                Some(Ok(Message::Pong(payload))) => {
                    if payload.len() > MAX_CONTROL_FRAME_BYTES {
                        return Err(BridgeError::new("websocket connection closed abnormally"));
                    }
                }
                Some(Ok(Message::Frame(_))) => {
                    return Err(BridgeError::new("websocket connection closed abnormally"));
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => break,
                Some(Err(WsError::Io(ref io_err)))
                    if matches!(
                        io_err.kind(),
                        io::ErrorKind::BrokenPipe
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    break;
                }
                Some(Err(_)) => {
                    return Err(BridgeError::new("websocket connection closed abnormally"));
                }
            }
        }
        Ok::<(), BridgeError>(())
    };

    tokio::select! {
        biased;
        sig = signals.recv() => return Err(BridgeRunTermination::Signal(sig)),
        timed = tokio::time::timeout(CLOSE_TIMEOUT, drain_fut) => {
            if let Ok(Err(err)) = timed {
                return Err(BridgeRunTermination::Error(err));
            }
        }
    }

    if let Ok(Some(status)) = child.try_wait() {
        if !status.success() {
            return Err(BridgeRunTermination::Error(BridgeError::with_exit_code(
                "backend failed while stdin closed",
                exit_code_from_status(Some(status)),
            )));
        }
    }

    Ok(0)
}

/// Runs the intercepted stdio JSONL <-> Unix WebSocket bridge using process `stdin` and `stdout`.
pub async fn run_intercepted_async<S: AsRef<OsStr>>(
    real_codex: &Path,
    runtime_root: Option<&Path>,
    original_argv: &[S],
    ready_timeout: Duration,
    max_line_bytes: usize,
) -> Result<i32, BridgeRunTermination> {
    let stdin_reader = spawn_stdin_channel_reader();
    let stdout_writer = StdStdoutWriter;
    run_intercepted_with_stdio(
        real_codex,
        runtime_root,
        original_argv,
        ready_timeout,
        max_line_bytes,
        stdin_reader,
        stdout_writer,
    )
    .await
}

fn write_fixed_error_diagnostic() {
    let _ = io::stderr().write_all(FIXED_ERROR_DIAGNOSTIC.as_bytes());
    let _ = io::stderr().flush();
}

/// Entrypoint logic for `threadlane-codex-bridge` with explicit CLI arguments and environment configuration.
pub fn run_with_config_and_args<S: AsRef<OsStr>>(config: &BridgeConfig, args: &[S]) -> i32 {
    if let Some(offline_text) =
        check_offline_cli_flag(args, config.real_codex.as_deref().map(Path::as_os_str))
    {
        let _ = io::stdout().write_all(offline_text.as_bytes());
        let _ = io::stdout().flush();
        return 0;
    }

    let real_codex = match config.validate_real_codex() {
        Ok(path) => path,
        Err(err) => {
            write_fixed_error_diagnostic();
            return err.exit_code();
        }
    };

    if !is_intercepted_argv_os(args) {
        let Err(err) = exec_passthrough(&real_codex, args);
        write_fixed_error_diagnostic();
        return err.exit_code();
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => {
            write_fixed_error_diagnostic();
            return 1;
        }
    };

    match rt.block_on(run_intercepted_async(
        &real_codex,
        config.runtime_root.as_deref(),
        args,
        config.ready_timeout,
        config.max_line_bytes,
    )) {
        Ok(code) => code,
        Err(BridgeRunTermination::Signal(sig)) => sig.exit_code(),
        Err(BridgeRunTermination::Error(err)) => {
            write_fixed_error_diagnostic();
            err.exit_code()
        }
    }
}

/// Standard CLI entrypoint reading `std::env::args_os()`, `THREADLANE_REAL_CODEX`, and `XDG_RUNTIME_DIR`.
pub fn run_from_env() -> i32 {
    let raw_args: Vec<OsString> = std::env::args_os().collect();
    let args = if raw_args.len() > 1 {
        &raw_args[1..]
    } else {
        &[]
    };
    let config = BridgeConfig::from_env();
    run_with_config_and_args(&config, args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_version_and_help_work_without_configured_backend() {
        let cfg = BridgeConfig::new();
        assert!(cfg.version_text().starts_with("threadlane-codex-bridge "));
        assert!(cfg.help_text().contains("THREADLANE_REAL_CODEX"));

        assert_eq!(
            check_offline_cli_flag(&["--version"], None),
            Some(version_text())
        );
        assert_eq!(
            check_offline_cli_flag(&["-V"], Some(OsStr::new(""))),
            Some(version_text())
        );
        assert_eq!(check_offline_cli_flag(&["--help"], None), Some(help_text()));
        assert_eq!(
            check_offline_cli_flag(&["-h"], Some(OsStr::new(""))),
            Some(help_text())
        );

        // When THREADLANE_REAL_CODEX is configured, standard --version and --help pass through.
        assert_eq!(
            check_offline_cli_flag(&["--version"], Some(OsStr::new("/usr/bin/codex"))),
            None
        );
        assert_eq!(
            check_offline_cli_flag(&["--help"], Some(OsStr::new("/usr/bin/codex"))),
            None
        );

        // Explicit bridge flags always work offline.
        assert_eq!(
            check_offline_cli_flag(
                &["--threadlane-bridge-version"],
                Some(OsStr::new("/usr/bin/codex"))
            ),
            Some(version_text())
        );
        assert_eq!(
            check_offline_cli_flag(
                &["--threadlane-bridge-help"],
                Some(OsStr::new("/usr/bin/codex"))
            ),
            Some(help_text())
        );
    }

    #[test]
    fn plan_backend_argv_preserves_opaque_tokens_and_rejects_unknown_forms() {
        let sock = Path::new("/run/user/1000/tl-codex-abc/s.sock");

        assert_eq!(
            plan_backend_argv(
                &[
                    "-c",
                    "app-server",
                    "--config",
                    "opaque_cfg",
                    "--config=opaque_eq",
                    "--enable",
                    "feat_a",
                    "--disable",
                    "feat_b",
                    "--strict-config",
                    "app-server",
                    "--analytics-default-enabled",
                    "--strict-config",
                    "-c",
                    "suffix_cfg",
                    "--listen=stdio://",
                ],
                sock,
            ),
            Some(vec![
                "-c".to_string(),
                "app-server".to_string(),
                "--config".to_string(),
                "opaque_cfg".to_string(),
                "--config=opaque_eq".to_string(),
                "--enable".to_string(),
                "feat_a".to_string(),
                "--disable".to_string(),
                "feat_b".to_string(),
                "--strict-config".to_string(),
                "app-server".to_string(),
                "--analytics-default-enabled".to_string(),
                "--strict-config".to_string(),
                "-c".to_string(),
                "suffix_cfg".to_string(),
                "--listen".to_string(),
                format!("unix://{}", sock.display()),
            ])
        );

        for variant in INTERCEPTED_ARGVS {
            assert!(is_intercepted_argv(variant));
            assert_eq!(
                plan_backend_argv(variant, sock),
                Some(vec![
                    "app-server".to_string(),
                    "--listen".to_string(),
                    format!("unix://{}", sock.display()),
                ])
            );
        }

        let negative_cases: &[&[&str]] = &[
            &[],
            &["-c", "app-server"],
            &["--config", "app-server"],
            &["--enable", "app-server"],
            &["--disable", "app-server"],
            &["-c", "app-server", "exec", "noop"],
            &["-c"],
            &["--config"],
            &["--config="],
            &["--enable"],
            &["--disable"],
            &["app-server", "-c"],
            &["app-server", "--config="],
            &["app-server", "--listen"],
            &["--unknown-global", "app-server"],
            &["-c", "k=v", "app-server", "--unknown-flag"],
            &["app-server", "--model", "o4-mini"],
            &["app-server", "--help"],
            &["app-server", "-h"],
            &["app-server", "--print-startup-config"],
            &["app-server", "startup-sign-out"],
            &["app-server", "proxy", "--sock", "/tmp/neg.sock"],
            &["app-server", "--listen", "unix:///tmp/neg.sock"],
            &["app-server", "--listen=unix:///tmp/neg.sock"],
            &["app-server", "--listen", "ws://127.0.0.1:9999"],
            &["app-server", "--stdio", "--stdio"],
            &["app-server", "--stdio", "--listen", "stdio://"],
            &["app-server", "--listen", "stdio://", "--listen=stdio://"],
        ];

        for &argv in negative_cases {
            assert_eq!(
                plan_backend_argv(argv, sock),
                None,
                "expected None for {argv:?}"
            );
            assert!(!is_intercepted_argv(argv), "expected false for {argv:?}");
        }
    }

    #[tokio::test]
    async fn read_bounded_line_enforces_frame_limit_and_preserves_lines() {
        let input = b"first line\nsecond line\ntrailing";
        let mut cursor = &input[..];
        let mut buf = Vec::new();

        let l1 = read_bounded_line(&mut cursor, &mut buf, 64, 4)
            .await
            .expect("line 1")
            .expect("some line 1");
        assert_eq!(l1, b"first line");

        let l2 = read_bounded_line(&mut cursor, &mut buf, 64, 4)
            .await
            .expect("line 2")
            .expect("some line 2");
        assert_eq!(l2, b"second line");

        let l3 = read_bounded_line(&mut cursor, &mut buf, 64, 4)
            .await
            .expect("line 3")
            .expect("some line 3");
        assert_eq!(l3, b"trailing");

        let eof = read_bounded_line(&mut cursor, &mut buf, 64, 4)
            .await
            .expect("eof");
        assert_eq!(eof, None);

        let oversized = b"0123456789abcdef\n";
        let mut over_cursor = &oversized[..];
        let mut over_buf = Vec::new();
        let err = read_bounded_line(&mut over_cursor, &mut over_buf, 8, 4)
            .await
            .expect_err("oversized line must fail");
        assert_eq!(err.message(), "stdin line exceeds maximum size");
    }
}
