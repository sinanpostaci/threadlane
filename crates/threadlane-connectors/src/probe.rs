use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use threadlane_registry::{ModelDefinition, ToolAdapter, ToolDefinition};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use crate::cli_environment::restore_cli_environment;
use crate::credentials::validate_tool_id;
use crate::error::ConnectorError;
use crate::parsers::{
    parse_antigravity_models, parse_cli_version, parse_codex_bundled_models,
    parse_codex_login_status, parse_opencode_models, AUTH_STATUS_AUTHENTICATED,
    AUTH_STATUS_UNAUTHENTICATED, AUTH_STATUS_UNKNOWN,
};

/// Default wall-clock timeout for a single CLI probe command (10 seconds).
pub const DEFAULT_PROBE_TIMEOUT_MS: u64 = 10_000;

/// Maximum bytes captured from stdout or stderr during a CLI probe (1 MiB).
pub const MAX_PROBE_STREAM_BYTES: usize = 1_048_576;

/// Bounded grace window (ms) for stdout/stderr pipes to reach EOF after process-group exit.
const POST_EXIT_DRAIN_MS: u64 = 200;

/// Availability status when a CLI executable is resolved and verified executable.
pub const AVAILABILITY_FOUND: &str = "found";

/// Availability status when a CLI executable is not found on disk or `PATH`.
pub const AVAILABILITY_NOT_FOUND: &str = "not_found";

/// Availability status for API-only adapters that never spawn a local CLI.
pub const AVAILABILITY_API_ONLY: &str = "api_only";

/// Safe metadata returned from an explicit [`probe_tool`] invocation.
///
/// Contains only validated catalog metadata and static status strings; never contains
/// raw CLI stdout, stderr, stack traces, or credential tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolProbe {
    /// Identifier of the probed tool definition.
    pub tool_id: String,
    /// Executable availability (`found`, `not_found`, or `api_only`).
    pub availability: String,
    /// Parsed CLI version token, if available (`null` in JSON when `None`).
    pub version: Option<String>,
    /// Safe authentication state (`authenticated`, `unauthenticated`, or `unknown`).
    pub auth_status: String,
    /// Discovered catalog models for this tool.
    pub models: Vec<ModelDefinition>,
    /// Short, safe human-readable summary of the probe outcome.
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AdapterKind {
    CodexCli,
    OpencodeV2,
    Antigravity,
    JevApi,
    OpenaiDecisionsApi,
    Git,
    Tmux,
    Codegraph,
}

impl AdapterKind {
    fn from_registry_adapter(adapter: &ToolAdapter) -> Result<Self, ConnectorError> {
        let value = serde_json::to_value(adapter)
            .map_err(|_| ConnectorError::InvalidInput("Tool adapter could not be serialized."))?;
        serde_json::from_value(value)
            .map_err(|_| ConnectorError::InvalidInput("Unsupported tool adapter variant."))
    }

    const fn is_api_adapter(self) -> bool {
        matches!(self, Self::JevApi | Self::OpenaiDecisionsApi)
    }

    const fn default_binaries(self) -> &'static [&'static str] {
        match self {
            Self::CodexCli => &["codex"],
            Self::OpencodeV2 => &["opencode"],
            Self::Antigravity => &["agy", "antigravity"],
            Self::Git => &["git"],
            Self::Tmux => &["tmux"],
            Self::Codegraph => &["codegraph"],
            Self::JevApi | Self::OpenaiDecisionsApi => &[],
        }
    }

    const fn primary_probe_args(self) -> Option<&'static [&'static str]> {
        match self {
            Self::CodexCli => Some(&["debug", "models", "--bundled"]),
            Self::OpencodeV2 => Some(&["api", "--standalone", "model.list"]),
            Self::Antigravity => Some(&["models"]),
            Self::Git => Some(&["--version"]),
            Self::Tmux => Some(&["-V"]),
            Self::Codegraph => Some(&["--version"]),
            Self::JevApi | Self::OpenaiDecisionsApi => None,
        }
    }
}

/// Returns the fixed, shell-free CLI argument template used to probe a [`ToolAdapter`],
/// or `None` for API adapters that never spawn a CLI process.
pub fn probe_command_template(adapter: &ToolAdapter) -> Option<&'static [&'static str]> {
    AdapterKind::from_registry_adapter(adapter)
        .ok()
        .and_then(AdapterKind::primary_probe_args)
}

/// Explicitly probes a [`ToolDefinition`] using fixed, shell-free CLI templates,
/// bounded 10-second timeouts, 1 MiB stdout/stderr caps, and Unix process-group supervision.
///
/// API adapters (`jev_api`, `openai_decisions_api`) never spawn a CLI subprocess.
pub async fn probe_tool(tool: &ToolDefinition) -> Result<ToolProbe, ConnectorError> {
    probe_tool_with_timeout(tool, Duration::from_millis(DEFAULT_PROBE_TIMEOUT_MS)).await
}

/// Explicitly probes a [`ToolDefinition`] with a caller-specified per-command timeout.
pub async fn probe_tool_with_timeout(
    tool: &ToolDefinition,
    timeout: Duration,
) -> Result<ToolProbe, ConnectorError> {
    validate_tool_id(&tool.id)?;
    if timeout.is_zero() {
        return Err(ConnectorError::InvalidInput(
            "Probe timeout must be greater than zero.",
        ));
    }

    let adapter = AdapterKind::from_registry_adapter(&tool.adapter)?;

    // API adapters never spawn a CLI subprocess regardless of executable_path.
    if adapter.is_api_adapter() {
        return Ok(ToolProbe {
            tool_id: tool.id.clone(),
            availability: AVAILABILITY_API_ONLY.to_string(),
            version: None,
            auth_status: AUTH_STATUS_UNKNOWN.to_string(),
            models: Vec::new(),
            message: "API adapter does not use a local CLI executable; check OS credential status or query provider models explicitly.".to_string(),
        });
    }

    ensure_unix_platform()?;

    let resolved_exec = match resolve_and_validate_executable(
        tool.executable_path.as_deref(),
        adapter.default_binaries(),
    )? {
        Some(path) => path,
        None => {
            return Ok(ToolProbe {
                tool_id: tool.id.clone(),
                availability: AVAILABILITY_NOT_FOUND.to_string(),
                version: None,
                auth_status: AUTH_STATUS_UNKNOWN.to_string(),
                models: Vec::new(),
                message:
                    "CLI executable was not found; authentication and catalog status are unknown."
                        .to_string(),
            });
        }
    };

    match adapter {
        AdapterKind::CodexCli => probe_codex_cli(&tool.id, &resolved_exec, timeout).await,
        AdapterKind::OpencodeV2 => probe_opencode_v2(&tool.id, &resolved_exec, timeout).await,
        AdapterKind::Antigravity => probe_antigravity(&tool.id, &resolved_exec, timeout).await,
        AdapterKind::Git | AdapterKind::Tmux | AdapterKind::Codegraph => {
            probe_utility_cli(&tool.id, adapter, &resolved_exec, timeout).await
        }
        AdapterKind::JevApi | AdapterKind::OpenaiDecisionsApi => unreachable!(),
    }
}

async fn probe_codex_cli(
    tool_id: &str,
    executable: &Path,
    timeout: Duration,
) -> Result<ToolProbe, ConnectorError> {
    let catalog_out =
        run_bounded_probe_command(executable, &["debug", "models", "--bundled"], timeout).await?;
    if catalog_out.exit_code != Some(0) {
        return Err(ConnectorError::ProbeFailed(
            "Codex bundled model catalog command exited with a non-zero status.",
        ));
    }
    let models = parse_codex_bundled_models(tool_id, &catalog_out.stdout)?;

    let login_out = run_bounded_probe_command(executable, &["login", "status"], timeout).await?;
    let auth_status =
        parse_codex_login_status(login_out.exit_code, &login_out.stdout, &login_out.stderr);

    let message = match auth_status {
        AUTH_STATUS_AUTHENTICATED => {
            "Codex CLI executable found and bundled catalog metadata parsed; login status is authenticated (does not prove runtime model entitlement)."
        }
        AUTH_STATUS_UNAUTHENTICATED => {
            "Codex CLI executable found and bundled catalog metadata parsed; login status is unauthenticated."
        }
        _ => {
            "Codex CLI executable found and bundled catalog metadata parsed; authentication status and runtime model entitlement are unknown."
        }
    };

    Ok(ToolProbe {
        tool_id: tool_id.to_string(),
        availability: AVAILABILITY_FOUND.to_string(),
        version: None,
        auth_status: auth_status.to_string(),
        models,
        message: message.to_string(),
    })
}

async fn probe_opencode_v2(
    tool_id: &str,
    executable: &Path,
    timeout: Duration,
) -> Result<ToolProbe, ConnectorError> {
    let catalog_out =
        run_bounded_probe_command(executable, &["api", "--standalone", "model.list"], timeout)
            .await?;
    if catalog_out.exit_code != Some(0) {
        return Err(ConnectorError::ProbeFailed(
            "OpenCode standalone model catalog command exited with a non-zero status.",
        ));
    }
    let models = parse_opencode_models(tool_id, &catalog_out.stdout)?;

    Ok(ToolProbe {
        tool_id: tool_id.to_string(),
        availability: AVAILABILITY_FOUND.to_string(),
        version: None,
        auth_status: AUTH_STATUS_UNKNOWN.to_string(),
        models,
        message: "OpenCode CLI executable found and standalone model catalog metadata parsed; authentication status and runtime model entitlement are unknown.".to_string(),
    })
}

async fn probe_antigravity(
    tool_id: &str,
    executable: &Path,
    timeout: Duration,
) -> Result<ToolProbe, ConnectorError> {
    let catalog_out = run_bounded_probe_command(executable, &["models"], timeout).await?;
    if catalog_out.exit_code != Some(0) {
        return Err(ConnectorError::ProbeFailed(
            "Antigravity model catalog command exited with a non-zero status.",
        ));
    }
    let models = parse_antigravity_models(tool_id, &catalog_out.stdout)?;

    Ok(ToolProbe {
        tool_id: tool_id.to_string(),
        availability: AVAILABILITY_FOUND.to_string(),
        version: None,
        auth_status: AUTH_STATUS_UNKNOWN.to_string(),
        models,
        message: "Antigravity CLI executable found and model catalog metadata parsed; authentication status and runtime model entitlement are unknown.".to_string(),
    })
}

async fn probe_utility_cli(
    tool_id: &str,
    adapter: AdapterKind,
    executable: &Path,
    timeout: Duration,
) -> Result<ToolProbe, ConnectorError> {
    let args = adapter.primary_probe_args().unwrap_or(&["--version"]);
    let out = run_bounded_probe_command(executable, args, timeout).await?;
    if out.exit_code != Some(0) {
        return Err(ConnectorError::ProbeFailed(
            "CLI version probe exited with a non-zero status.",
        ));
    }
    let version = parse_cli_version(&out.stdout, &out.stderr);

    Ok(ToolProbe {
        tool_id: tool_id.to_string(),
        availability: AVAILABILITY_FOUND.to_string(),
        version,
        auth_status: AUTH_STATUS_UNKNOWN.to_string(),
        models: Vec::new(),
        message: "CLI executable found; authentication status is unknown.".to_string(),
    })
}

fn ensure_unix_platform() -> Result<(), ConnectorError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(ConnectorError::UnsupportedPlatform(
            "Bounded process-group CLI probing is currently implemented on Unix only.",
        ))
    }
}

fn validate_explicit_executable_path(path: &Path) -> Result<(), ConnectorError> {
    let raw = path.as_os_str().to_string_lossy();
    if raw.is_empty() || raw.contains('\0') || raw.chars().any(char::is_control) {
        return Err(ConnectorError::InvalidExecutable(
            "Configured executable path must not be empty or contain control characters.",
        ));
    }
    if !path.is_absolute() {
        return Err(ConnectorError::InvalidExecutable(
            "Configured executable path must be an absolute path.",
        ));
    }
    for comp in path.components() {
        if matches!(comp, Component::CurDir | Component::ParentDir) {
            return Err(ConnectorError::InvalidExecutable(
                "Configured executable path must not contain '.' or '..' traversal components.",
            ));
        }
    }
    Ok(())
}

fn check_resolved_file_executable(path: &Path) -> Result<PathBuf, ConnectorError> {
    let meta = fs::metadata(path).map_err(|_| {
        ConnectorError::InvalidExecutable(
            "Configured executable path could not be resolved to an accessible target.",
        )
    })?;
    if !meta.is_file() {
        return Err(ConnectorError::InvalidExecutable(
            "Configured executable path must point to a regular file.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(ConnectorError::InvalidExecutable(
                "Configured executable file does not have execute permissions.",
            ));
        }
    }
    let canonical = fs::canonicalize(path).map_err(|_| {
        ConnectorError::InvalidExecutable("Configured executable path could not be canonicalized.")
    })?;
    let canonical_meta = fs::symlink_metadata(&canonical).map_err(|_| {
        ConnectorError::InvalidExecutable(
            "Canonical executable path metadata could not be inspected.",
        )
    })?;
    if !canonical_meta.is_file() {
        return Err(ConnectorError::InvalidExecutable(
            "Canonical executable target is not a regular file.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (canonical_meta.permissions().mode() & 0o111) == 0 {
            return Err(ConnectorError::InvalidExecutable(
                "Canonical executable target does not have execute permissions.",
            ));
        }
    }
    Ok(canonical)
}

fn resolve_and_validate_executable(
    configured: Option<&Path>,
    default_binaries: &[&str],
) -> Result<Option<PathBuf>, ConnectorError> {
    if let Some(path) = configured {
        validate_explicit_executable_path(path)?;
        match fs::symlink_metadata(path) {
            Ok(_) => {
                let canonical = check_resolved_file_executable(path)?;
                return Ok(Some(canonical));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(_) => {
                return Err(ConnectorError::InvalidExecutable(
                    "Configured executable path could not be accessed.",
                ));
            }
        }
    }

    let path_os = std::env::var_os("PATH");
    Ok(find_canonical_executable_in_path(
        path_os.as_deref(),
        default_binaries,
    ))
}

fn find_canonical_executable_in_path(
    path_var: Option<&OsStr>,
    binaries: &[&str],
) -> Option<PathBuf> {
    let raw_path = path_var?;
    for dir in std::env::split_paths(raw_path) {
        if dir.as_os_str().is_empty() || !dir.is_absolute() {
            continue;
        }
        if dir
            .components()
            .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
        {
            continue;
        }
        for bin in binaries {
            let candidate = dir.join(bin);
            if let Ok(canonical) = check_resolved_file_executable(&candidate) {
                return Some(canonical);
            }
        }
    }
    None
}

/// Environments stripped from every CLI probe subprocess to prevent Git repository
/// redirection, hook execution, credential helper hijacking, or interactive prompts.
const SANITIZED_GIT_ENV_VARS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_PREFIX",
    "GIT_SUPER_PREFIX",
    "GIT_EXEC_PATH",
    "GIT_SSH",
    "GIT_SSH_COMMAND",
    "GIT_ASKPASS",
    "SSH_ASKPASS",
    "GIT_EXTERNAL_DIFF",
    "GIT_EDITOR",
    "GIT_PAGER",
    "GIT_CONFIG",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_SYSTEM",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
    "GIT_TEMPLATE_DIR",
];

fn configure_sanitized_probe_env(cmd: &mut Command) {
    for key in SANITIZED_GIT_ENV_VARS {
        cmd.env_remove(key);
    }
    for (k, _) in std::env::vars_os() {
        if let Some(key_str) = k.to_str() {
            if key_str.starts_with("GIT_CONFIG_KEY_") || key_str.starts_with("GIT_CONFIG_VALUE_") {
                cmd.env_remove(&k);
            }
        }
    }
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("NO_COLOR", "1")
        .env("TERM", "dumb")
        .env("CI", "1");
}

#[derive(Debug)]
struct BoundedCommandOutput {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamOverflow {
    Stdout,
    Stderr,
}

#[cfg(unix)]
#[allow(unsafe_code)]
pub(crate) mod unix_pgroup {
    use std::fs;
    use std::io;

    pub(crate) fn caller_pgid() -> i32 {
        // SAFETY: `getpgrp` is a thread-safe POSIX syscall taking no arguments.
        unsafe { libc::getpgrp() }
    }

    pub(crate) fn signal_process_group(pgid: i32, sig: libc::c_int) -> io::Result<()> {
        if pgid <= 1 || pgid == caller_pgid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing to signal invalid or caller process group",
            ));
        }
        // SAFETY: `pgid > 1` is verified above, so `-pgid < -1` targets only the owned child process group.
        let rc = unsafe { libc::kill(-pgid, sig) };
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
        // SAFETY: `kill(-pgid, 0)` with `pgid > 1` checks group existence without sending a signal.
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
}

/// Checks whether any non-zombie process still exists in Unix process group `pgid`.
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
    const fn new(pgid: i32) -> Self {
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
        if !self.disarmed {
            #[cfg(unix)]
            {
                let _ = unix_pgroup::signal_process_group(self.pgid, libc::SIGTERM);
                let _ = unix_pgroup::signal_process_group(self.pgid, libc::SIGKILL);
            }
            #[cfg(not(unix))]
            {
                let _ = self.pgid;
            }
        }
    }
}

async fn wait_for_stop(stop_rx: &mut watch::Receiver<bool>) {
    loop {
        if *stop_rx.borrow_and_update() {
            return;
        }
        if stop_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn drain_stream_bounded<R>(
    mut reader: R,
    max_bytes: usize,
    kind: StreamOverflow,
    overflow_tx: mpsc::UnboundedSender<StreamOverflow>,
    mut stop_rx: watch::Receiver<bool>,
) -> (Vec<u8>, bool)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(4096.min(max_bytes));
    let mut chunk = [0u8; 4096];
    let mut overflowed = false;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_stop(&mut stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            break;
        };
        match read_res {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if buf.len().saturating_add(n) > max_bytes {
                    overflowed = true;
                    let _ = overflow_tx.send(kind);
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        }
    }

    (buf, overflowed)
}

async fn terminate_and_reap_group(pgid: i32, child: &mut Child) -> Option<ExitStatus> {
    #[cfg(unix)]
    {
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGTERM);
    }

    let mut exit_status: Option<ExitStatus> = None;
    for _ in 0..20 {
        if exit_status.is_none() {
            exit_status = child.try_wait().ok().flatten();
        }
        if exit_status.is_some() && !is_process_group_alive(pgid) {
            return exit_status;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    #[cfg(unix)]
    {
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGKILL);
    }

    if exit_status.is_none() {
        let _ = child.start_kill();
        if let Ok(Ok(status)) = tokio::time::timeout(Duration::from_millis(500), child.wait()).await
        {
            exit_status = Some(status);
        }
    }

    terminate_process_group_members(pgid).await;
    exit_status
}

async fn terminate_process_group_members(pgid: i32) {
    #[cfg(unix)]
    {
        if !unix_pgroup::is_process_group_alive(pgid) {
            return;
        }
        let _ = unix_pgroup::signal_process_group(pgid, libc::SIGTERM);
        for _ in 0..10 {
            if !unix_pgroup::is_process_group_alive(pgid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for _ in 0..100 {
            let _ = unix_pgroup::signal_process_group(pgid, libc::SIGKILL);
            if !unix_pgroup::is_process_group_alive(pgid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pgid;
    }
}

async fn finish_or_terminate_surviving_descendants<T1, T2>(
    pgid: i32,
    stdout_handle: &tokio::task::JoinHandle<T1>,
    stderr_handle: &tokio::task::JoinHandle<T2>,
) {
    for _ in 0..10 {
        if stdout_handle.is_finished()
            && stderr_handle.is_finished()
            && !is_process_group_alive(pgid)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if stdout_handle.is_finished() && stderr_handle.is_finished() && !is_process_group_alive(pgid) {
        return;
    }
    terminate_process_group_members(pgid).await;
}

async fn wait_for_readers_or_signal_stop<T1, T2>(
    stdout_handle: &tokio::task::JoinHandle<T1>,
    stderr_handle: &tokio::task::JoinHandle<T2>,
    stop_tx: &watch::Sender<bool>,
) {
    let steps = (POST_EXIT_DRAIN_MS / 5).max(1);
    for _ in 0..steps {
        if stdout_handle.is_finished() && stderr_handle.is_finished() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = stop_tx.send(true);
}

async fn run_bounded_probe_command(
    executable: &Path,
    args: &[&str],
    timeout: Duration,
) -> Result<BoundedCommandOutput, ConnectorError> {
    let mut cmd = Command::new(executable);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    configure_sanitized_probe_env(&mut cmd);
    restore_cli_environment(cmd.as_std_mut());

    let temp_dir = std::env::temp_dir();
    if temp_dir.is_dir() {
        cmd.current_dir(temp_dir);
    }

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd
        .spawn()
        .map_err(|_| ConnectorError::ProbeFailed("Failed to spawn CLI probe subprocess."))?;
    let child_pid = child.id().ok_or(ConnectorError::ProbeFailed(
        "Spawned CLI probe child had no OS process ID.",
    ))?;
    let pgid = i32::try_from(child_pid).map_err(|_| {
        ConnectorError::ProbeFailed("Spawned CLI probe process ID exceeded i32 range.")
    })?;

    #[cfg(unix)]
    if pgid <= 1 || pgid == unix_pgroup::caller_pgid() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(ConnectorError::ProbeFailed(
            "Spawned CLI probe child did not enter an isolated process group.",
        ));
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let stdout_pipe = child.stdout.take().ok_or(ConnectorError::ProbeFailed(
        "Failed to capture CLI probe stdout pipe.",
    ))?;
    let stderr_pipe = child.stderr.take().ok_or(ConnectorError::ProbeFailed(
        "Failed to capture CLI probe stderr pipe.",
    ))?;

    let (overflow_tx, mut overflow_rx) = mpsc::unbounded_channel::<StreamOverflow>();
    let (stop_tx, stop_rx) = watch::channel(false);

    let mut stdout_handle = tokio::spawn(drain_stream_bounded(
        stdout_pipe,
        MAX_PROBE_STREAM_BYTES,
        StreamOverflow::Stdout,
        overflow_tx.clone(),
        stop_rx.clone(),
    ));
    let mut stderr_handle = tokio::spawn(drain_stream_bounded(
        stderr_pipe,
        MAX_PROBE_STREAM_BYTES,
        StreamOverflow::Stderr,
        overflow_tx,
        stop_rx,
    ));

    let timeout_sleep = tokio::time::sleep(timeout);
    tokio::pin!(timeout_sleep);

    let (timed_out, overflowed, exit_status) = tokio::select! {
        wait_res = child.wait() => {
            finish_or_terminate_surviving_descendants(pgid, &stdout_handle, &stderr_handle).await;
            (false, false, wait_res.ok())
        }
        Some(_) = overflow_rx.recv() => {
            let status = terminate_and_reap_group(pgid, &mut child).await;
            (false, true, status)
        }
        () = &mut timeout_sleep => {
            let status = terminate_and_reap_group(pgid, &mut child).await;
            (true, false, status)
        }
    };

    wait_for_readers_or_signal_stop(&stdout_handle, &stderr_handle, &stop_tx).await;

    if !is_process_group_alive(pgid) {
        pgroup_guard.disarm();
    }

    let (stdout_bytes, stdout_overflow) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_DRAIN_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        _ => {
            stdout_handle.abort();
            (Vec::new(), false)
        }
    };

    let (stderr_bytes, stderr_overflow) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_DRAIN_MS),
        &mut stderr_handle,
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        _ => {
            stderr_handle.abort();
            (Vec::new(), false)
        }
    };

    if timed_out {
        return Err(ConnectorError::ProbeTimedOut(
            "CLI metadata probe timed out and its process group was terminated.",
        ));
    }
    if overflowed || stdout_overflow || stderr_overflow {
        return Err(ConnectorError::ProbeOutputOverflow(
            "CLI metadata probe exceeded the 1 MiB stream limit and its process group was terminated.",
        ));
    }
    if is_process_group_alive(pgid) {
        return Err(ConnectorError::ProbeFailed(
            "CLI metadata probe left surviving processes after termination attempt.",
        ));
    }

    let exit_code = exit_status.and_then(|s| s.code());
    let stdout = String::from_utf8_lossy(&stdout_bytes).into_owned();
    let stderr = String::from_utf8_lossy(&stderr_bytes).into_owned();

    Ok(BoundedCommandOutput {
        exit_code,
        stdout,
        stderr,
    })
}
