use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use toml_edit::{Array, DocumentMut, Item, Table, Value};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    ensure_platform_supported, prepare_private_dir, read_private_file_bounded,
    validate_absolute_no_symlink_components, validate_private_file_path, write_private_file_atomic,
    MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::pairing::MCP_SERVER_NAME;

/// Default filename of the Codex configuration file inside `CODEX_HOME`.
pub const CODEX_CONFIG_FILENAME: &str = "config.toml";

/// Default binary name of the unbound paired Threadlane MCP stdio server.
pub const DEFAULT_NATIVE_MCP_EXECUTABLE_NAME: &str = "threadlane-native-mcp";

/// Filename of the private cross-process lock inside `management_dir`.
pub const CODEX_SETUP_LOCK_FILENAME: &str = "codex-setup.lock";

/// Filename of the private ownership receipt inside `management_dir`.
pub const CODEX_SETUP_RECEIPT_FILENAME: &str = "codex-setup-receipt.json";

/// Maximum byte length of a single filesystem path accepted by Codex MCP setup.
pub const MAX_SETUP_PATH_BYTES: usize = 4096;

/// Maximum byte size of the private ownership receipt file (`16 KiB`).
pub const MAX_SETUP_RECEIPT_BYTES: usize = 16 * 1024;

/// Native launcher marker capturing the caller's original `XDG_STATE_HOME`.
pub const THREADLANE_CLI_XDG_STATE_HOME_ENV: &str = "THREADLANE_CLI_XDG_STATE_HOME";

const RECEIPT_SCHEMA_VERSION: u32 = 1;

const MSG_CHECKING: &str = "Checking Codex MCP server registration in config.toml.";
const MSG_REGISTERED_CREATED: &str =
    "Registered mcp_servers.threadlane in Codex config.toml. Restart or refresh Codex MCP connections to load the updated configuration; conversation pairing is still required.";
const MSG_REGISTERED_UPDATED: &str =
    "Updated managed mcp_servers.threadlane registration in Codex config.toml. Restart or refresh Codex MCP connections to load the updated configuration; conversation pairing is still required.";
const MSG_POST_COMMIT_RECEIPT_WARNING: &str =
    "Registered mcp_servers.threadlane in Codex config.toml, but saving the private ownership receipt failed and needs attention. Restart or refresh Codex MCP connections to load the updated configuration; conversation pairing is still required.";
const MSG_POST_COMMIT_SYNC_WARNING: &str =
    "Registered mcp_servers.threadlane in Codex config.toml, but post-write permission or directory sync failed and needs attention. Restart or refresh Codex MCP connections to load the updated configuration; conversation pairing is still required.";
const MSG_ADOPT_RECEIPT_WARNING: &str =
    "Codex mcp_servers.threadlane registration matches the desired configuration, but saving the private ownership receipt failed and needs attention. Conversation pairing is still required.";
const MSG_UNCHANGED: &str =
    "Codex mcp_servers.threadlane registration is already up to date. Conversation pairing is still required before scoped tools can run.";
const MSG_NOT_REGISTERED_ABSENT: &str =
    "Codex mcp_servers.threadlane is not yet registered in config.toml.";
const MSG_NOT_REGISTERED_OUTDATED: &str =
    "Codex mcp_servers.threadlane has a previously managed registration that differs from the current managed binary, store paths or startup flags.";
const MSG_DISABLED: &str =
    "Codex mcp_servers.threadlane is disabled (enabled = false) in config.toml and was left disabled.";
const MSG_CONFLICT: &str =
    "Codex mcp_servers.threadlane has existing manual or unmanaged settings in config.toml and was not modified.";
const MSG_ERROR_INVALID_CONFIG: &str =
    "Codex MCP setup paths or arguments failed validation; config.toml was not modified.";
const MSG_ERROR_EXECUTABLE: &str =
    "Current threadlane-native-mcp executable could not be validated; config.toml was not modified.";
const MSG_ERROR_MALFORMED_CONFIG: &str =
    "Codex config.toml contains invalid TOML syntax and was not modified.";
const MSG_ERROR_CORRUPT_RECEIPT: &str =
    "Threadlane Codex setup ownership receipt is corrupt or invalid; config.toml was not modified.";
const MSG_ERROR_READ_ONLY: &str =
    "Codex config.toml or its parent directory is read-only; config.toml was not modified.";
const MSG_ERROR_SECURITY: &str =
    "Codex config.toml, parent directory, or management store failed symlink or permission security checks.";
const MSG_ERROR_CONCURRENT: &str =
    "Codex config.toml was modified concurrently during setup; changes were not committed.";
const MSG_ERROR_SETUP_BUSY: &str =
    "Another Threadlane process is currently updating Codex MCP setup; config.toml was not modified.";
const MSG_ERROR_IO: &str =
    "Filesystem I/O error occurred while inspecting or updating Codex MCP setup.";

/// Operation mode for the Codex MCP setup helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexSetupMode {
    /// Read-only inspection mode: validates paths, receipt, and `config.toml` without creating
    /// or modifying any directories or files.
    Check,
    /// Atomic setup mode: creates or refreshes a Threadlane-owned `mcp_servers.threadlane` entry
    /// while preserving unrelated settings, comments, disabled entries, and manual edits.
    Apply,
}

impl CodexSetupMode {
    /// Returns the static `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Check => "check",
            Self::Apply => "apply",
        }
    }
}

/// High-level outcome state of Codex MCP setup inspection or application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexSetupState {
    /// Background startup check/apply is currently in progress.
    Checking,
    /// `mcp_servers.threadlane` was newly created or upgraded in `config.toml`.
    Registered,
    /// `mcp_servers.threadlane` already matches the exact desired registration with no config write.
    Unchanged,
    /// `mcp_servers.threadlane` is absent or needs an update (reported in read-only `check` mode).
    NotRegistered,
    /// `mcp_servers.threadlane` has `enabled = false` in `config.toml` and was left disabled.
    Disabled,
    /// `mcp_servers.threadlane` has differing unmanaged or manually edited settings and was not modified.
    Conflict,
    /// Setup failed validation, security, syntax, receipt, or I/O checks and requires operator attention.
    Error,
}

impl CodexSetupState {
    /// Returns the static `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Checking => "checking",
            Self::Registered => "registered",
            Self::Unchanged => "unchanged",
            Self::NotRegistered => "not_registered",
            Self::Disabled => "disabled",
            Self::Conflict => "conflict",
            Self::Error => "error",
        }
    }
}

/// Pure request parameters for checking or applying the unbound `mcp_servers.threadlane` registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexSetupRequest {
    /// Clean absolute path to the user's Codex home directory (`${CODEX_HOME:-$HOME/.codex}`).
    pub codex_home: PathBuf,
    /// Clean absolute path to the current `threadlane-native-mcp` executable.
    pub mcp_executable: PathBuf,
    /// Clean absolute path to the Threadlane runtime supervisor state directory (`--state-dir`).
    pub state_dir: PathBuf,
    /// Clean absolute path to the private native MCP pairing store directory (`--pairing-dir`).
    pub pairing_dir: PathBuf,
    /// Clean absolute path to the workflow registry directory (`--registry-dir`).
    pub registry_dir: PathBuf,
    /// Clean absolute path to the workflow plans store directory (`--plans-dir`).
    pub plans_dir: PathBuf,
    /// Clean absolute path to the operator approval authority directory (`--authority-dir`).
    pub authority_dir: PathBuf,
    /// Clean absolute path to the approved host sessions root directory (`--sessions-dir`).
    pub sessions_dir: PathBuf,
    /// Clean absolute path to the private setup management directory holding the lock and receipt.
    pub management_dir: PathBuf,
}

/// Type alias for [`CodexSetupRequest`].
pub type CodexMcpSetupRequest = CodexSetupRequest;

/// Compact, sanitized, serializable status report returned by Codex MCP setup check/apply operations.
///
/// Never contains raw TOML content, credentials, environment variables, or arbitrary error payloads,
/// and always sets `pairing_required: true` because MCP binary registration alone never enrolls a
/// conversation, verifies native thread identity, grants plan approval, or starts a model run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexSetupReport {
    /// Current setup state (`checking`, `registered`, `unchanged`, `not_registered`, `disabled`, `conflict`, or `error`).
    pub state: CodexSetupState,
    /// Operation mode that produced this report (`check` or `apply`).
    pub mode: CodexSetupMode,
    /// Clean absolute path to `<codex_home>/config.toml` when `codex_home` is known.
    pub config_path: Option<String>,
    /// Whether `config.toml` currently contains the exact desired enabled `mcp_servers.threadlane` registration.
    pub registered: bool,
    /// Whether this operation wrote a new or updated `mcp_servers.threadlane` table to `config.toml`.
    pub config_modified: bool,
    /// Whether Codex must restart or refresh its MCP connection to load a newly written registration.
    pub restart_required: bool,
    /// Always `true`: per-conversation pairing is still required before any scoped MCP tool is permitted.
    pub pairing_required: bool,
    /// Stable `snake_case` error or conflict code when `state` is `conflict` or `error`.
    pub error_code: Option<String>,
    /// Fixed, safe English summary message suitable for CLI and desktop UI display.
    pub message: String,
}

/// Type alias for [`CodexSetupReport`].
pub type CodexMcpSetupReport = CodexSetupReport;

impl CodexSetupReport {
    /// Constructs the initial `checking` report used while the background startup task is running.
    pub fn checking(config_path: Option<String>) -> Self {
        Self {
            state: CodexSetupState::Checking,
            mode: CodexSetupMode::Apply,
            config_path,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: None,
            message: MSG_CHECKING.to_string(),
        }
    }

    /// Constructs a sanitized `error` report with a fixed error code and static message.
    pub fn error(
        mode: CodexSetupMode,
        config_path: Option<String>,
        error_code: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            state: CodexSetupState::Error,
            mode,
            config_path,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: Some(error_code.to_string()),
            message: message.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CodexSetupReceipt {
    schema_version: u32,
    server_name: String,
    config_path: String,
    managed_table_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CanonicalManagedTable<'a> {
    server_name: &'a str,
    command: &'a str,
    args: &'a [String],
}

#[derive(Debug, Clone)]
struct ValidatedSetupInput {
    codex_home: PathBuf,
    config_path: PathBuf,
    config_path_str: String,
    management_dir: PathBuf,
    desired_command: String,
    desired_args: Vec<String>,
    desired_table_sha256: String,
}

#[derive(Debug, Clone)]
enum OriginalConfigSnapshot {
    Absent,
    Present {
        dev: u64,
        ino: u64,
        len: u64,
        mtime_sec: i64,
        mtime_nsec: i64,
        sha256: String,
        raw_text: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupFailureKind {
    InvalidConfig,
    InvalidExecutable,
    MalformedConfig,
    CorruptReceipt,
    ReadOnlyPath,
    SecurityViolation,
    ConcurrentModification,
    SetupBusy,
    IoError,
}

impl SetupFailureKind {
    const fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::InvalidExecutable => "invalid_executable",
            Self::MalformedConfig => "malformed_config",
            Self::CorruptReceipt => "corrupt_receipt",
            Self::ReadOnlyPath => "read_only_path",
            Self::SecurityViolation => "security_violation",
            Self::ConcurrentModification => "concurrent_modification",
            Self::SetupBusy => "setup_busy",
            Self::IoError => "io_error",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::InvalidConfig => MSG_ERROR_INVALID_CONFIG,
            Self::InvalidExecutable => MSG_ERROR_EXECUTABLE,
            Self::MalformedConfig => MSG_ERROR_MALFORMED_CONFIG,
            Self::CorruptReceipt => MSG_ERROR_CORRUPT_RECEIPT,
            Self::ReadOnlyPath => MSG_ERROR_READ_ONLY,
            Self::SecurityViolation => MSG_ERROR_SECURITY,
            Self::ConcurrentModification => MSG_ERROR_CONCURRENT,
            Self::SetupBusy => MSG_ERROR_SETUP_BUSY,
            Self::IoError => MSG_ERROR_IO,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitSuccess {
    Complete,
    PostCommitSyncWarning,
    PostCommitReceiptWarning,
}

#[derive(Debug)]
struct SetupLockGuard {
    file: File,
}

impl Drop for SetupLockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn try_acquire_setup_lock(dir: &Path) -> Result<SetupLockGuard, SetupFailureKind> {
    ensure_platform_supported().map_err(|err| map_native_error_to_failure(&err))?;
    let lock_path = dir.join(CODEX_SETUP_LOCK_FILENAME);
    validate_private_file_path(&lock_path, "codex setup management")
        .map_err(|err| map_native_error_to_failure(&err))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::PermissionDenied {
                    SetupFailureKind::ReadOnlyPath
                } else {
                    SetupFailureKind::IoError
                }
            })?;
        let meta = file.metadata().map_err(|_| SetupFailureKind::IoError)?;
        if !meta.file_type().is_file() {
            return Err(SetupFailureKind::SecurityViolation);
        }
        let sym_meta = fs::symlink_metadata(&lock_path).map_err(|_| SetupFailureKind::IoError)?;
        if sym_meta.file_type().is_symlink() {
            return Err(SetupFailureKind::SecurityViolation);
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| SetupFailureKind::IoError)?;

        if let Err(err) = file.try_lock_exclusive() {
            let is_contended = err.kind() == fs2::lock_contended_error().kind()
                || err.kind() == std::io::ErrorKind::WouldBlock;
            if is_contended {
                return Err(SetupFailureKind::SetupBusy);
            }
            return Err(SetupFailureKind::IoError);
        }

        Ok(SetupLockGuard { file })
    }
}

/// Checks the `mcp_servers.threadlane` registration in `<codex_home>/config.toml` without
/// creating or modifying any files or directories.
pub fn check_codex_mcp_setup(request: &CodexSetupRequest) -> CodexSetupReport {
    run_codex_mcp_setup(request, CodexSetupMode::Check)
}

/// Applies the `mcp_servers.threadlane` registration in `<codex_home>/config.toml` atomically,
/// creating or upgrading only a Threadlane-owned entry while preserving all other settings.
pub fn apply_codex_mcp_setup(request: &CodexSetupRequest) -> CodexSetupReport {
    run_codex_mcp_setup(request, CodexSetupMode::Apply)
}

/// Executes Codex MCP setup in the requested [`CodexSetupMode`].
pub fn run_codex_mcp_setup(request: &CodexSetupRequest, mode: CodexSetupMode) -> CodexSetupReport {
    run_codex_mcp_setup_internal(request, mode, None, None)
}

fn run_codex_mcp_setup_internal(
    request: &CodexSetupRequest,
    mode: CodexSetupMode,
    pre_commit_hook: Option<&dyn Fn()>,
    post_rename_hook: Option<&dyn Fn()>,
) -> CodexSetupReport {
    let fallback_config_path = best_effort_config_path_display(&request.codex_home);

    let validated = match validate_setup_request(request) {
        Ok(v) => v,
        Err(kind) => {
            return CodexSetupReport::error(
                mode,
                fallback_config_path,
                kind.code(),
                kind.message(),
            );
        }
    };

    let config_path_opt = Some(validated.config_path_str.clone());

    match mode {
        CodexSetupMode::Check => execute_check_mode(&validated),
        CodexSetupMode::Apply => {
            match execute_apply_mode(&validated, pre_commit_hook, post_rename_hook) {
                Ok(report) => report,
                Err(kind) => {
                    CodexSetupReport::error(mode, config_path_opt, kind.code(), kind.message())
                }
            }
        }
    }
}

/// Validates and canonicalizes an explicit `threadlane-native-mcp` executable path.
///
/// Requires a clean absolute path (no `.` or `..` segments), canonicalizes symlink chains once,
/// and verifies the resolved target is a regular executable file on Unix (`0o111`).
pub fn validate_mcp_executable_path(executable: &Path) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    let raw_str = validate_clean_utf8_path_str(executable, "threadlane-native-mcp executable")?;
    if !executable.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "threadlane-native-mcp executable path must be an absolute path",
        ));
    }
    for comp in executable.components() {
        match comp {
            Component::CurDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "threadlane-native-mcp executable path must not contain `.` components",
                ));
            }
            Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "threadlane-native-mcp executable path must not contain `..` traversal components",
                ));
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {}
        }
    }
    let _ = raw_str;

    let canonical = match fs::canonicalize(executable) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                "configured threadlane-native-mcp executable does not exist",
            ));
        }
        Err(_) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                "cannot canonicalize threadlane-native-mcp executable path",
            ));
        }
    };

    let _ = validate_clean_utf8_path_str(&canonical, "resolved threadlane-native-mcp executable")?;

    let meta = match fs::symlink_metadata(&canonical) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::NotConfigured,
                "configured threadlane-native-mcp executable does not exist",
            ));
        }
        Err(_) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                "cannot stat resolved threadlane-native-mcp executable",
            ));
        }
    };
    if meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "resolved threadlane-native-mcp executable target must not be a symlink",
        ));
    }
    if !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "threadlane-native-mcp executable path is not a regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 == 0 {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "threadlane-native-mcp executable file is not marked executable",
            ));
        }
    }

    Ok(canonical)
}

/// Resolves the user's Codex home directory following `CODEX_HOME` then `$HOME/.codex`
/// (never using isolated GUI XDG variables).
pub fn resolve_codex_home() -> Result<PathBuf, NativeError> {
    resolve_codex_home_with(|key| std::env::var_os(key))
}

/// Resolves the user's Codex home directory using a custom environment lookup function.
pub fn resolve_codex_home_with<F>(mut get_env: F) -> Result<PathBuf, NativeError>
where
    F: FnMut(&str) -> Option<OsString>,
{
    if let Some(codex_home) = get_env("CODEX_HOME").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(codex_home);
        let clean = validate_absolute_no_symlink_components(&path, "CODEX_HOME")?;
        let _ = validate_clean_utf8_path_str(&clean, "CODEX_HOME")?;
        return Ok(clean);
    }

    if let Some(home) = get_env("HOME").filter(|v| !v.is_empty()) {
        let path = PathBuf::from(home).join(".codex");
        let clean = validate_absolute_no_symlink_components(&path, "default Codex home")?;
        let _ = validate_clean_utf8_path_str(&clean, "default Codex home")?;
        return Ok(clean);
    }

    Err(NativeError::new(
        NativeErrorCode::NotConfigured,
        "neither CODEX_HOME nor HOME is set to resolve the Codex configuration directory",
    ))
}

/// Resolves the Threadlane runtime `state_dir` following supplied `--state-dir`, then original
/// `THREADLANE_CLI_XDG_STATE_HOME` / `XDG_STATE_HOME`, then `$HOME/.local/state/threadlane/runtime`.
pub fn resolve_default_runtime_state_dir(
    supplied_state_dir: Option<&Path>,
) -> Result<PathBuf, NativeError> {
    resolve_default_runtime_state_dir_with(supplied_state_dir, |key| std::env::var_os(key))
}

/// Resolves the Threadlane runtime `state_dir` using a custom environment lookup function.
pub fn resolve_default_runtime_state_dir_with<F>(
    supplied_state_dir: Option<&Path>,
    mut get_env: F,
) -> Result<PathBuf, NativeError>
where
    F: FnMut(&str) -> Option<OsString>,
{
    if let Some(explicit) = supplied_state_dir {
        let clean = validate_absolute_no_symlink_components(explicit, "runtime state_dir")?;
        let _ = validate_clean_utf8_path_str(&clean, "runtime state_dir")?;
        return Ok(clean);
    }

    let state_base = match get_env(THREADLANE_CLI_XDG_STATE_HOME_ENV) {
        Some(marker_val) => {
            if marker_val.is_empty() {
                // Empty marker indicates XDG_STATE_HOME was originally unset before GUI wrapper isolation.
                None
            } else {
                Some(PathBuf::from(marker_val))
            }
        }
        None => get_env("XDG_STATE_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from),
    };

    let runtime_dir = if let Some(base) = state_base {
        base.join("threadlane").join("runtime")
    } else if let Some(home) = get_env("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("threadlane")
            .join("runtime")
    } else {
        return Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "unable to resolve default Threadlane runtime state directory from environment",
        ));
    };

    let clean = validate_absolute_no_symlink_components(&runtime_dir, "runtime state_dir")?;
    let _ = validate_clean_utf8_path_str(&clean, "runtime state_dir")?;
    Ok(clean)
}

/// Resolves and validates the active `threadlane-native-mcp` executable from `PATH` first,
/// falling back to the canonical sibling of `std::env::current_exe()`.
pub fn resolve_native_mcp_executable() -> Result<PathBuf, NativeError> {
    let current_exe = std::env::current_exe().ok();
    resolve_native_mcp_executable_with(std::env::var_os("PATH").as_deref(), current_exe.as_deref())
}

/// Resolves and validates the active `threadlane-native-mcp` executable from an optional `PATH`
/// value and optional `current_exe` path.
pub fn resolve_native_mcp_executable_with(
    path_env: Option<&OsStr>,
    current_exe: Option<&Path>,
) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;

    if let Some(paths) = path_env {
        for dir in std::env::split_paths(paths) {
            if dir.as_os_str().is_empty() || !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join(DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
            if let Ok(validated) = validate_mcp_executable_path(&candidate) {
                return Ok(validated);
            }
        }
    }

    if let Some(exe) = current_exe {
        if let Ok(canonical_self) = fs::canonicalize(exe) {
            if let Some(parent_dir) = canonical_self.parent() {
                let sibling = parent_dir.join(DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
                if let Ok(validated) = validate_mcp_executable_path(&sibling) {
                    return Ok(validated);
                }
            }
        }
    }

    Err(NativeError::new(
        NativeErrorCode::NotConfigured,
        "threadlane-native-mcp executable was not found on PATH or next to the current executable",
    ))
}

/// Builds the exact 14-element argument vector for the standard owned managed
/// `threadlane-native-mcp` entry.
///
/// The managed desktop registration opts into fresh-only approved-host startup and managed
/// native notices. Raw [`crate::NativeMcpCli`] and [`crate::PairedMcpServer`] defaults remain
/// off for legacy and manual callers.
pub fn build_desired_mcp_args(
    state_dir: &str,
    pairing_dir: &str,
    registry_dir: &str,
    plans_dir: &str,
    authority_dir: &str,
    sessions_dir: &str,
) -> Vec<String> {
    vec![
        "--state-dir".to_string(),
        state_dir.to_string(),
        "--pairing-dir".to_string(),
        pairing_dir.to_string(),
        "--registry-dir".to_string(),
        registry_dir.to_string(),
        "--plans-dir".to_string(),
        plans_dir.to_string(),
        "--authority-dir".to_string(),
        authority_dir.to_string(),
        "--sessions-dir".to_string(),
        sessions_dir.to_string(),
        "--coworker-native-notices".to_string(),
        "--auto-start-approved-hosts".to_string(),
    ]
}

fn best_effort_config_path_display(codex_home: &Path) -> Option<String> {
    if codex_home.as_os_str().is_empty() || !codex_home.is_absolute() {
        return None;
    }
    for comp in codex_home.components() {
        if matches!(comp, Component::CurDir | Component::ParentDir) {
            return None;
        }
    }
    let joined = codex_home.join(CODEX_CONFIG_FILENAME);
    let s = joined.to_str()?;
    if s.is_empty() || s.len() > MAX_SETUP_PATH_BYTES || s.chars().any(char::is_control) {
        return None;
    }
    Some(s.to_string())
}

fn validate_clean_utf8_path_str<'a>(path: &'a Path, label: &str) -> Result<&'a str, NativeError> {
    let s = path.to_str().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must be valid UTF-8"),
        )
    })?;
    if s.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty"),
        ));
    }
    if s.len() > MAX_SETUP_PATH_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path exceeds maximum length of {MAX_SETUP_PATH_BYTES} bytes"),
        ));
    }
    if s.chars().any(char::is_control) {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} path must not contain control characters"),
        ));
    }
    Ok(s)
}

fn map_native_error_to_failure(err: &NativeError) -> SetupFailureKind {
    match err.code {
        NativeErrorCode::SecurityViolation => SetupFailureKind::SecurityViolation,
        NativeErrorCode::IoError | NativeErrorCode::OutputOverflow => SetupFailureKind::IoError,
        _ => SetupFailureKind::InvalidConfig,
    }
}

fn validate_directory_path(
    path: &Path,
    label: &str,
) -> Result<(PathBuf, String), SetupFailureKind> {
    let clean = validate_absolute_no_symlink_components(path, label)
        .map_err(|err| map_native_error_to_failure(&err))?;
    let clean_str = validate_clean_utf8_path_str(&clean, label)
        .map_err(|err| map_native_error_to_failure(&err))?
        .to_string();

    match fs::symlink_metadata(&clean) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(SetupFailureKind::SecurityViolation);
            }
            if !meta.file_type().is_dir() {
                return Err(SetupFailureKind::InvalidConfig);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            return Err(SetupFailureKind::ReadOnlyPath);
        }
        Err(_) => return Err(SetupFailureKind::IoError),
    }

    Ok((clean, clean_str))
}

fn validate_setup_request(
    request: &CodexSetupRequest,
) -> Result<ValidatedSetupInput, SetupFailureKind> {
    ensure_platform_supported().map_err(|err| map_native_error_to_failure(&err))?;

    let (codex_home, _) = validate_directory_path(&request.codex_home, "codex_home")?;
    let (state_dir, state_dir_str) = validate_directory_path(&request.state_dir, "state_dir")?;
    let (pairing_dir, pairing_dir_str) =
        validate_directory_path(&request.pairing_dir, "pairing_dir")?;
    let (registry_dir, registry_dir_str) =
        validate_directory_path(&request.registry_dir, "registry_dir")?;
    let (plans_dir, plans_dir_str) = validate_directory_path(&request.plans_dir, "plans_dir")?;
    let (authority_dir, authority_dir_str) =
        validate_directory_path(&request.authority_dir, "authority_dir")?;
    let (sessions_dir, sessions_dir_str) =
        validate_directory_path(&request.sessions_dir, "sessions_dir")?;
    let (management_dir, _) = validate_directory_path(&request.management_dir, "management_dir")?;

    let dirs: [(&str, &Path); 8] = [
        ("codex_home", &codex_home),
        ("state_dir", &state_dir),
        ("pairing_dir", &pairing_dir),
        ("registry_dir", &registry_dir),
        ("plans_dir", &plans_dir),
        ("authority_dir", &authority_dir),
        ("sessions_dir", &sessions_dir),
        ("management_dir", &management_dir),
    ];

    for i in 0..dirs.len() {
        for j in (i + 1)..dirs.len() {
            let (_, a) = dirs[i];
            let (_, b) = dirs[j];
            if a.starts_with(b) || b.starts_with(a) {
                return Err(SetupFailureKind::InvalidConfig);
            }
        }
    }

    let canonical_mcp = match validate_mcp_executable_path(&request.mcp_executable) {
        Ok(p) => p,
        Err(err) if err.code == NativeErrorCode::SecurityViolation => {
            return Err(SetupFailureKind::SecurityViolation);
        }
        Err(_) => return Err(SetupFailureKind::InvalidExecutable),
    };
    let desired_command = validate_clean_utf8_path_str(&canonical_mcp, "mcp_executable")
        .map_err(|_| SetupFailureKind::InvalidExecutable)?
        .to_string();

    for (_, dir_path) in dirs {
        if canonical_mcp.starts_with(dir_path) {
            return Err(SetupFailureKind::InvalidConfig);
        }
    }

    let config_path = codex_home.join(CODEX_CONFIG_FILENAME);
    let config_path_str = validate_clean_utf8_path_str(&config_path, "config.toml")
        .map_err(|_| SetupFailureKind::InvalidConfig)?
        .to_string();

    let desired_args = build_desired_mcp_args(
        &state_dir_str,
        &pairing_dir_str,
        &registry_dir_str,
        &plans_dir_str,
        &authority_dir_str,
        &sessions_dir_str,
    );
    let desired_table_sha256 = compute_managed_table_sha256(&desired_command, &desired_args);

    Ok(ValidatedSetupInput {
        codex_home,
        config_path,
        config_path_str,
        management_dir,
        desired_command,
        desired_args,
        desired_table_sha256,
    })
}

fn compute_managed_table_sha256(command: &str, args: &[String]) -> String {
    let payload = CanonicalManagedTable {
        server_name: MCP_SERVER_NAME,
        command,
        args,
    };
    let serialized =
        serde_json::to_vec(&payload).expect("CanonicalManagedTable serialization is infallible");
    sha256_hex(&serialized)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn is_valid_sha256_hex(raw: &str) -> bool {
    raw.len() == 64 && raw.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'))
}

#[cfg(unix)]
fn check_existing_dir_writable(dir: &Path) -> Result<(), SetupFailureKind> {
    use std::os::unix::fs::PermissionsExt;

    let mut current = Some(dir);
    while let Some(candidate) = current {
        match fs::symlink_metadata(candidate) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(SetupFailureKind::SecurityViolation);
                }
                if !meta.file_type().is_dir() {
                    return Err(SetupFailureKind::InvalidConfig);
                }
                let mode = meta.permissions().mode() & 0o777;
                if meta.permissions().readonly() || (mode & 0o700) != 0o700 {
                    return Err(SetupFailureKind::ReadOnlyPath);
                }
                return Ok(());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                current = candidate.parent();
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(SetupFailureKind::ReadOnlyPath);
            }
            Err(_) => return Err(SetupFailureKind::IoError),
        }
    }
    Ok(())
}

fn read_and_validate_receipt(
    management_dir: &Path,
    expected_config_path: &str,
) -> Result<Option<CodexSetupReceipt>, SetupFailureKind> {
    match fs::symlink_metadata(management_dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(SetupFailureKind::SecurityViolation);
            }
            if !meta.file_type().is_dir() {
                return Err(SetupFailureKind::InvalidConfig);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if (mode & 0o077) != 0 {
                    return Err(SetupFailureKind::SecurityViolation);
                }
                if meta.permissions().readonly() || (mode & 0o700) != 0o700 {
                    return Err(SetupFailureKind::ReadOnlyPath);
                }
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            check_existing_dir_writable(management_dir)?;
            return Ok(None);
        }
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            return Err(SetupFailureKind::ReadOnlyPath);
        }
        Err(_) => return Err(SetupFailureKind::IoError),
    }

    let receipt_path = management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
    let bytes_opt = read_private_file_bounded(
        &receipt_path,
        MAX_SETUP_RECEIPT_BYTES,
        "codex setup receipt",
    )
    .map_err(|err| match err.code {
        NativeErrorCode::SecurityViolation => SetupFailureKind::SecurityViolation,
        NativeErrorCode::OutputOverflow => SetupFailureKind::CorruptReceipt,
        _ => SetupFailureKind::CorruptReceipt,
    })?;

    let Some(bytes) = bytes_opt else {
        return Ok(None);
    };

    let receipt: CodexSetupReceipt =
        serde_json::from_slice(&bytes).map_err(|_| SetupFailureKind::CorruptReceipt)?;

    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.server_name != MCP_SERVER_NAME
        || receipt.config_path != expected_config_path
        || !is_valid_sha256_hex(&receipt.managed_table_sha256)
    {
        return Err(SetupFailureKind::CorruptReceipt);
    }

    Ok(Some(receipt))
}

fn write_receipt(
    management_dir: &Path,
    config_path_str: &str,
    managed_table_sha256: &str,
) -> Result<(), SetupFailureKind> {
    let receipt = CodexSetupReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        server_name: MCP_SERVER_NAME.to_string(),
        config_path: config_path_str.to_string(),
        managed_table_sha256: managed_table_sha256.to_string(),
    };
    let mut bytes = serde_json::to_vec_pretty(&receipt).map_err(|_| SetupFailureKind::IoError)?;
    bytes.push(b'\n');

    write_private_file_atomic(
        management_dir,
        CODEX_SETUP_RECEIPT_FILENAME,
        &bytes,
        "codex setup receipt",
    )
    .map_err(|err| map_native_error_to_failure(&err))?;

    let dir_file = File::open(management_dir).map_err(|_| SetupFailureKind::IoError)?;
    dir_file.sync_all().map_err(|_| SetupFailureKind::IoError)?;

    Ok(())
}

fn read_original_config_snapshot(
    codex_home: &Path,
    config_path: &Path,
) -> Result<OriginalConfigSnapshot, SetupFailureKind> {
    check_existing_dir_writable(codex_home)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let sym_meta = match fs::symlink_metadata(config_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(OriginalConfigSnapshot::Absent);
            }
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(SetupFailureKind::ReadOnlyPath);
            }
            Err(_) => return Err(SetupFailureKind::IoError),
        };

        if sym_meta.file_type().is_symlink() {
            return Err(SetupFailureKind::SecurityViolation);
        }
        if !sym_meta.file_type().is_file() {
            return Err(SetupFailureKind::InvalidConfig);
        }
        let mode = sym_meta.permissions().mode() & 0o777;
        if sym_meta.permissions().readonly() || (mode & 0o600) != 0o600 {
            return Err(SetupFailureKind::ReadOnlyPath);
        }
        if sym_meta.len() > MAX_PRIVATE_STATE_FILE_BYTES as u64 {
            return Err(SetupFailureKind::InvalidConfig);
        }

        let file = File::open(config_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::PermissionDenied {
                SetupFailureKind::ReadOnlyPath
            } else {
                SetupFailureKind::IoError
            }
        })?;
        let open_meta = file.metadata().map_err(|_| SetupFailureKind::IoError)?;
        if !open_meta.file_type().is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
        {
            return Err(SetupFailureKind::ConcurrentModification);
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_meta.permissions().readonly() || (open_mode & 0o600) != 0o600 {
            return Err(SetupFailureKind::ReadOnlyPath);
        }

        let mut buf =
            Vec::with_capacity(open_meta.len().min(MAX_PRIVATE_STATE_FILE_BYTES as u64) as usize);
        file.take((MAX_PRIVATE_STATE_FILE_BYTES as u64).saturating_add(1))
            .read_to_end(&mut buf)
            .map_err(|_| SetupFailureKind::IoError)?;
        if buf.len() > MAX_PRIVATE_STATE_FILE_BYTES || buf.len() as u64 != open_meta.len() {
            return Err(SetupFailureKind::ConcurrentModification);
        }

        let raw_text = String::from_utf8(buf).map_err(|_| SetupFailureKind::MalformedConfig)?;
        let sha256 = sha256_hex(raw_text.as_bytes());

        Ok(OriginalConfigSnapshot::Present {
            dev: open_meta.dev(),
            ino: open_meta.ino(),
            len: open_meta.len(),
            mtime_sec: open_meta.mtime(),
            mtime_nsec: open_meta.mtime_nsec(),
            sha256,
            raw_text,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InspectedTableState {
    Absent,
    Disabled,
    Conflict,
    CandidateManaged { table_sha256: String },
}

fn inspect_threadlane_table(doc: &DocumentMut) -> InspectedTableState {
    let Some(mcp_servers_item) = doc.get("mcp_servers") else {
        return InspectedTableState::Absent;
    };
    let Some(mcp_servers_table) = mcp_servers_item.as_table() else {
        return InspectedTableState::Conflict;
    };
    let Some(threadlane_item) = mcp_servers_table.get(MCP_SERVER_NAME) else {
        return InspectedTableState::Absent;
    };

    let Some(table_like) = threadlane_item.as_table_like() else {
        return InspectedTableState::Conflict;
    };

    if let Some(enabled_item) = table_like.get("enabled") {
        match enabled_item.as_bool() {
            Some(false) => return InspectedTableState::Disabled,
            Some(true) => {}
            None => return InspectedTableState::Conflict,
        }
    }

    let Some(table) = threadlane_item.as_table() else {
        return InspectedTableState::Conflict;
    };

    if table
        .iter()
        .any(|(key, _)| !matches!(key, "command" | "args" | "enabled"))
    {
        return InspectedTableState::Conflict;
    }

    let Some(command_str) = table
        .get("command")
        .and_then(Item::as_value)
        .and_then(Value::as_str)
    else {
        return InspectedTableState::Conflict;
    };

    let Some(args_arr) = table
        .get("args")
        .and_then(Item::as_value)
        .and_then(Value::as_array)
    else {
        return InspectedTableState::Conflict;
    };

    let mut args = Vec::with_capacity(args_arr.len());
    for item in args_arr.iter() {
        let Some(arg_str) = item.as_str() else {
            return InspectedTableState::Conflict;
        };
        args.push(arg_str.to_string());
    }

    if !matches_unbound_paired_args_shape(&args) {
        return InspectedTableState::Conflict;
    }

    let table_sha256 = compute_managed_table_sha256(command_str, &args);
    InspectedTableState::CandidateManaged { table_sha256 }
}

fn matches_unbound_paired_args_shape(args: &[String]) -> bool {
    const EXPECTED_FLAGS: [&str; 6] = [
        "--state-dir",
        "--pairing-dir",
        "--registry-dir",
        "--plans-dir",
        "--authority-dir",
        "--sessions-dir",
    ];
    const COWORKER_NATIVE_NOTICES_FLAG: &str = "--coworker-native-notices";
    const AUTO_START_APPROVED_HOSTS_FLAG: &str = "--auto-start-approved-hosts";

    let legacy_len = EXPECTED_FLAGS.len() * 2;
    let notices_len = legacy_len + 1;
    let managed_len = legacy_len + 2;
    // Accept only the exact historical managed shapes for a receipt-verified upgrade:
    // - legacy 12-element store-only args
    // - 13-element args ending in `--coworker-native-notices`
    // - current 14-element args ending in `--coworker-native-notices --auto-start-approved-hosts`
    let recognized_shape = if args.len() == legacy_len {
        // Legacy store-only shape.
        true
    } else if args.len() == notices_len {
        args[notices_len - 1].as_str() == COWORKER_NATIVE_NOTICES_FLAG
    } else if args.len() == managed_len {
        args[managed_len - 2].as_str() == COWORKER_NATIVE_NOTICES_FLAG
            && args[managed_len - 1].as_str() == AUTO_START_APPROVED_HOSTS_FLAG
    } else {
        false
    };
    if !recognized_shape {
        return false;
    }
    for (idx, expected_flag) in EXPECTED_FLAGS.iter().enumerate() {
        let flag = &args[idx * 2];
        let val = &args[idx * 2 + 1];
        if flag != expected_flag || val.is_empty() || val.chars().any(char::is_control) {
            return false;
        }
        let path = Path::new(val);
        if !path.is_absolute() {
            return false;
        }
        for comp in path.components() {
            if matches!(comp, Component::CurDir | Component::ParentDir) {
                return false;
            }
        }
    }
    true
}

fn apply_desired_table_to_document(
    doc: &mut DocumentMut,
    desired_command: &str,
    desired_args: &[String],
) -> Result<(), SetupFailureKind> {
    let mut args_array = Array::new();
    for arg in desired_args {
        args_array.push(arg.as_str());
    }

    if !doc.contains_key("mcp_servers") {
        let mut mcp_servers = Table::new();
        mcp_servers.set_implicit(true);
        doc.insert("mcp_servers", Item::Table(mcp_servers));
    }

    let mcp_servers = doc
        .get_mut("mcp_servers")
        .and_then(Item::as_table_mut)
        .ok_or(SetupFailureKind::InvalidConfig)?;

    if let Some(existing_threadlane) = mcp_servers
        .get_mut(MCP_SERVER_NAME)
        .and_then(Item::as_table_mut)
    {
        existing_threadlane["command"] = toml_edit::value(desired_command);
        existing_threadlane["args"] = Item::Value(Value::Array(args_array));
    } else {
        let mut threadlane_table = Table::new();
        threadlane_table["command"] = toml_edit::value(desired_command);
        threadlane_table["args"] = Item::Value(Value::Array(args_array));
        mcp_servers.insert(MCP_SERVER_NAME, Item::Table(threadlane_table));
    }

    Ok(())
}

fn execute_check_mode(validated: &ValidatedSetupInput) -> CodexSetupReport {
    let config_path_opt = Some(validated.config_path_str.clone());

    let receipt_opt =
        match read_and_validate_receipt(&validated.management_dir, &validated.config_path_str) {
            Ok(r) => r,
            Err(kind) => {
                return CodexSetupReport::error(
                    CodexSetupMode::Check,
                    config_path_opt,
                    kind.code(),
                    kind.message(),
                );
            }
        };

    let snapshot =
        match read_original_config_snapshot(&validated.codex_home, &validated.config_path) {
            Ok(s) => s,
            Err(kind) => {
                return CodexSetupReport::error(
                    CodexSetupMode::Check,
                    config_path_opt,
                    kind.code(),
                    kind.message(),
                );
            }
        };

    let raw_text = match &snapshot {
        OriginalConfigSnapshot::Absent => String::new(),
        OriginalConfigSnapshot::Present { raw_text, .. } => raw_text.clone(),
    };

    let doc: DocumentMut = match raw_text.parse::<DocumentMut>() {
        Ok(d) => d,
        Err(_) => {
            return CodexSetupReport::error(
                CodexSetupMode::Check,
                config_path_opt,
                SetupFailureKind::MalformedConfig.code(),
                SetupFailureKind::MalformedConfig.message(),
            );
        }
    };

    match inspect_threadlane_table(&doc) {
        InspectedTableState::Absent => CodexSetupReport {
            state: CodexSetupState::NotRegistered,
            mode: CodexSetupMode::Check,
            config_path: config_path_opt,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: None,
            message: MSG_NOT_REGISTERED_ABSENT.to_string(),
        },
        InspectedTableState::Disabled => CodexSetupReport {
            state: CodexSetupState::Disabled,
            mode: CodexSetupMode::Check,
            config_path: config_path_opt,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: None,
            message: MSG_DISABLED.to_string(),
        },
        InspectedTableState::Conflict => CodexSetupReport {
            state: CodexSetupState::Conflict,
            mode: CodexSetupMode::Check,
            config_path: config_path_opt,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: Some("manual_conflict".to_string()),
            message: MSG_CONFLICT.to_string(),
        },
        InspectedTableState::CandidateManaged { table_sha256, .. } => {
            if table_sha256 == validated.desired_table_sha256 {
                CodexSetupReport {
                    state: CodexSetupState::Unchanged,
                    mode: CodexSetupMode::Check,
                    config_path: config_path_opt,
                    registered: true,
                    config_modified: false,
                    restart_required: false,
                    pairing_required: true,
                    error_code: None,
                    message: MSG_UNCHANGED.to_string(),
                }
            } else if let Some(receipt) = receipt_opt {
                if receipt.managed_table_sha256 == table_sha256 {
                    CodexSetupReport {
                        state: CodexSetupState::NotRegistered,
                        mode: CodexSetupMode::Check,
                        config_path: config_path_opt,
                        registered: false,
                        config_modified: false,
                        restart_required: false,
                        pairing_required: true,
                        error_code: None,
                        message: MSG_NOT_REGISTERED_OUTDATED.to_string(),
                    }
                } else {
                    CodexSetupReport {
                        state: CodexSetupState::Conflict,
                        mode: CodexSetupMode::Check,
                        config_path: config_path_opt,
                        registered: false,
                        config_modified: false,
                        restart_required: false,
                        pairing_required: true,
                        error_code: Some("manual_conflict".to_string()),
                        message: MSG_CONFLICT.to_string(),
                    }
                }
            } else {
                CodexSetupReport {
                    state: CodexSetupState::Conflict,
                    mode: CodexSetupMode::Check,
                    config_path: config_path_opt,
                    registered: false,
                    config_modified: false,
                    restart_required: false,
                    pairing_required: true,
                    error_code: Some("manual_conflict".to_string()),
                    message: MSG_CONFLICT.to_string(),
                }
            }
        }
    }
}

fn execute_apply_mode(
    validated: &ValidatedSetupInput,
    pre_commit_hook: Option<&dyn Fn()>,
    post_rename_hook: Option<&dyn Fn()>,
) -> Result<CodexSetupReport, SetupFailureKind> {
    check_existing_dir_writable(&validated.management_dir)?;
    let management_dir = prepare_private_dir(&validated.management_dir, "codex setup management")
        .map_err(|err| map_native_error_to_failure(&err))?;
    let _lock_guard = try_acquire_setup_lock(&management_dir)?;

    let receipt_opt = read_and_validate_receipt(&management_dir, &validated.config_path_str)?;
    let snapshot = read_original_config_snapshot(&validated.codex_home, &validated.config_path)?;

    let raw_text = match &snapshot {
        OriginalConfigSnapshot::Absent => String::new(),
        OriginalConfigSnapshot::Present { raw_text, .. } => raw_text.clone(),
    };

    let mut doc: DocumentMut = raw_text
        .parse::<DocumentMut>()
        .map_err(|_| SetupFailureKind::MalformedConfig)?;

    let config_path_opt = Some(validated.config_path_str.clone());

    match inspect_threadlane_table(&doc) {
        InspectedTableState::Disabled => Ok(CodexSetupReport {
            state: CodexSetupState::Disabled,
            mode: CodexSetupMode::Apply,
            config_path: config_path_opt,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: None,
            message: MSG_DISABLED.to_string(),
        }),
        InspectedTableState::Conflict => Ok(CodexSetupReport {
            state: CodexSetupState::Conflict,
            mode: CodexSetupMode::Apply,
            config_path: config_path_opt,
            registered: false,
            config_modified: false,
            restart_required: false,
            pairing_required: true,
            error_code: Some("manual_conflict".to_string()),
            message: MSG_CONFLICT.to_string(),
        }),
        InspectedTableState::CandidateManaged { table_sha256, .. } => {
            if table_sha256 == validated.desired_table_sha256 {
                // Exact match: adopt/refresh missing or older receipt without rewriting config.toml.
                let needs_receipt_write = match &receipt_opt {
                    Some(r) => r.managed_table_sha256 != validated.desired_table_sha256,
                    None => true,
                };
                if needs_receipt_write
                    && write_receipt(
                        &management_dir,
                        &validated.config_path_str,
                        &validated.desired_table_sha256,
                    )
                    .is_err()
                {
                    return Ok(CodexSetupReport {
                        state: CodexSetupState::Unchanged,
                        mode: CodexSetupMode::Apply,
                        config_path: config_path_opt,
                        registered: true,
                        config_modified: false,
                        restart_required: false,
                        pairing_required: true,
                        error_code: Some("receipt_persistence_failed".to_string()),
                        message: MSG_ADOPT_RECEIPT_WARNING.to_string(),
                    });
                }
                return Ok(CodexSetupReport {
                    state: CodexSetupState::Unchanged,
                    mode: CodexSetupMode::Apply,
                    config_path: config_path_opt,
                    registered: true,
                    config_modified: false,
                    restart_required: false,
                    pairing_required: true,
                    error_code: None,
                    message: MSG_UNCHANGED.to_string(),
                });
            }

            // Differing table: only upgrade if receipt proves this exact table was written by Threadlane.
            let Some(receipt) = receipt_opt else {
                return Ok(CodexSetupReport {
                    state: CodexSetupState::Conflict,
                    mode: CodexSetupMode::Apply,
                    config_path: config_path_opt,
                    registered: false,
                    config_modified: false,
                    restart_required: false,
                    pairing_required: true,
                    error_code: Some("manual_conflict".to_string()),
                    message: MSG_CONFLICT.to_string(),
                });
            };
            if receipt.managed_table_sha256 != table_sha256 {
                return Ok(CodexSetupReport {
                    state: CodexSetupState::Conflict,
                    mode: CodexSetupMode::Apply,
                    config_path: config_path_opt,
                    registered: false,
                    config_modified: false,
                    restart_required: false,
                    pairing_required: true,
                    error_code: Some("manual_conflict".to_string()),
                    message: MSG_CONFLICT.to_string(),
                });
            }

            apply_desired_table_to_document(
                &mut doc,
                &validated.desired_command,
                &validated.desired_args,
            )?;
            let outcome = commit_updated_config_and_receipt(
                validated,
                &management_dir,
                &snapshot,
                &doc,
                pre_commit_hook,
                post_rename_hook,
            )?;

            Ok(build_post_commit_report(
                config_path_opt,
                outcome,
                MSG_REGISTERED_UPDATED,
            ))
        }
        InspectedTableState::Absent => {
            apply_desired_table_to_document(
                &mut doc,
                &validated.desired_command,
                &validated.desired_args,
            )?;
            let outcome = commit_updated_config_and_receipt(
                validated,
                &management_dir,
                &snapshot,
                &doc,
                pre_commit_hook,
                post_rename_hook,
            )?;

            Ok(build_post_commit_report(
                config_path_opt,
                outcome,
                MSG_REGISTERED_CREATED,
            ))
        }
    }
}

fn build_post_commit_report(
    config_path: Option<String>,
    outcome: CommitSuccess,
    clean_message: &'static str,
) -> CodexSetupReport {
    let (error_code, message) = match outcome {
        CommitSuccess::Complete => (None, clean_message.to_string()),
        CommitSuccess::PostCommitReceiptWarning => (
            Some("receipt_persistence_failed".to_string()),
            MSG_POST_COMMIT_RECEIPT_WARNING.to_string(),
        ),
        CommitSuccess::PostCommitSyncWarning => (
            Some("post_commit_sync_failed".to_string()),
            MSG_POST_COMMIT_SYNC_WARNING.to_string(),
        ),
    };
    CodexSetupReport {
        state: CodexSetupState::Registered,
        mode: CodexSetupMode::Apply,
        config_path,
        registered: true,
        config_modified: true,
        restart_required: true,
        pairing_required: true,
        error_code,
        message,
    }
}

fn ensure_codex_home_directory(codex_home: &Path) -> Result<(), SetupFailureKind> {
    check_existing_dir_writable(codex_home)?;
    let _ = validate_absolute_no_symlink_components(codex_home, "Codex home")
        .map_err(|err| map_native_error_to_failure(&err))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

        let mut current = PathBuf::new();
        for comp in codex_home.components() {
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => current.push(comp.as_os_str()),
                Component::CurDir | Component::ParentDir => {
                    return Err(SetupFailureKind::SecurityViolation);
                }
                Component::Normal(part) => {
                    current.push(part);
                    match fs::symlink_metadata(&current) {
                        Ok(meta) => {
                            if meta.file_type().is_symlink() {
                                return Err(SetupFailureKind::SecurityViolation);
                            }
                            if !meta.file_type().is_dir() {
                                return Err(SetupFailureKind::InvalidConfig);
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            builder.create(&current).map_err(|create_err| {
                                if create_err.kind() == std::io::ErrorKind::PermissionDenied {
                                    SetupFailureKind::ReadOnlyPath
                                } else {
                                    SetupFailureKind::IoError
                                }
                            })?;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                            return Err(SetupFailureKind::ReadOnlyPath);
                        }
                        Err(_) => return Err(SetupFailureKind::IoError),
                    }
                }
            }
        }

        let meta = fs::symlink_metadata(codex_home).map_err(|_| SetupFailureKind::IoError)?;
        if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
            return Err(SetupFailureKind::SecurityViolation);
        }
        let mode = meta.permissions().mode() & 0o777;
        if meta.permissions().readonly() || (mode & 0o700) != 0o700 {
            return Err(SetupFailureKind::ReadOnlyPath);
        }
    }

    Ok(())
}

fn verify_snapshot_unchanged(
    config_path: &Path,
    expected: &OriginalConfigSnapshot,
) -> Result<(), SetupFailureKind> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        match expected {
            OriginalConfigSnapshot::Absent => match fs::symlink_metadata(config_path) {
                Ok(_) => Err(SetupFailureKind::ConcurrentModification),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                    Err(SetupFailureKind::ReadOnlyPath)
                }
                Err(_) => Err(SetupFailureKind::IoError),
            },
            OriginalConfigSnapshot::Present {
                dev,
                ino,
                len,
                mtime_sec,
                mtime_nsec,
                sha256,
                ..
            } => {
                let sym_meta = match fs::symlink_metadata(config_path) {
                    Ok(meta) => meta,
                    Err(_) => return Err(SetupFailureKind::ConcurrentModification),
                };
                if sym_meta.file_type().is_symlink() {
                    return Err(SetupFailureKind::SecurityViolation);
                }
                if !sym_meta.file_type().is_file() {
                    return Err(SetupFailureKind::ConcurrentModification);
                }
                let mode = sym_meta.permissions().mode() & 0o777;
                if sym_meta.permissions().readonly() || (mode & 0o600) != 0o600 {
                    return Err(SetupFailureKind::ReadOnlyPath);
                }
                if sym_meta.dev() != *dev
                    || sym_meta.ino() != *ino
                    || sym_meta.len() != *len
                    || sym_meta.mtime() != *mtime_sec
                    || sym_meta.mtime_nsec() != *mtime_nsec
                {
                    return Err(SetupFailureKind::ConcurrentModification);
                }

                let file = File::open(config_path)
                    .map_err(|_| SetupFailureKind::ConcurrentModification)?;
                let open_meta = file
                    .metadata()
                    .map_err(|_| SetupFailureKind::ConcurrentModification)?;
                if !open_meta.file_type().is_file()
                    || open_meta.dev() != *dev
                    || open_meta.ino() != *ino
                    || open_meta.len() != *len
                    || open_meta.mtime() != *mtime_sec
                    || open_meta.mtime_nsec() != *mtime_nsec
                {
                    return Err(SetupFailureKind::ConcurrentModification);
                }

                let mut buf = Vec::with_capacity(*len as usize);
                file.take(len.saturating_add(1))
                    .read_to_end(&mut buf)
                    .map_err(|_| SetupFailureKind::ConcurrentModification)?;
                if buf.len() as u64 != *len || sha256_hex(&buf) != *sha256 {
                    return Err(SetupFailureKind::ConcurrentModification);
                }

                Ok(())
            }
        }
    }
}

fn commit_updated_config_and_receipt(
    validated: &ValidatedSetupInput,
    management_dir: &Path,
    expected_snapshot: &OriginalConfigSnapshot,
    doc: &DocumentMut,
    pre_commit_hook: Option<&dyn Fn()>,
    post_rename_hook: Option<&dyn Fn()>,
) -> Result<CommitSuccess, SetupFailureKind> {
    let mut rendered = doc.to_string();
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    let bytes = rendered.as_bytes();
    if bytes.len() > MAX_PRIVATE_STATE_FILE_BYTES {
        return Err(SetupFailureKind::InvalidConfig);
    }

    ensure_codex_home_directory(&validated.codex_home)?;

    // Validate receipt path is writable/safe before touching config.toml.
    let receipt_path = management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
    validate_private_file_path(&receipt_path, "codex setup receipt")
        .map_err(|err| map_native_error_to_failure(&err))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut temp = tempfile::Builder::new()
            .prefix(".config.toml.threadlane.tmp.")
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(&validated.codex_home)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::PermissionDenied {
                    SetupFailureKind::ReadOnlyPath
                } else {
                    SetupFailureKind::IoError
                }
            })?;

        temp.write_all(bytes)
            .map_err(|_| SetupFailureKind::IoError)?;
        temp.as_file()
            .sync_all()
            .map_err(|_| SetupFailureKind::IoError)?;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| SetupFailureKind::IoError)?;

        if let Some(hook) = pre_commit_hook {
            hook();
        }

        verify_snapshot_unchanged(&validated.config_path, expected_snapshot)?;

        temp.persist(&validated.config_path).map_err(|err| {
            if err.error.kind() == std::io::ErrorKind::PermissionDenied {
                SetupFailureKind::ReadOnlyPath
            } else {
                SetupFailureKind::IoError
            }
        })?;

        if let Some(hook) = post_rename_hook {
            hook();
        }

        let perms_ok =
            fs::set_permissions(&validated.config_path, fs::Permissions::from_mode(0o600)).is_ok();
        let dir_sync_ok = File::open(&validated.codex_home)
            .and_then(|dir_file| dir_file.sync_all())
            .is_ok();
        let receipt_ok = write_receipt(
            management_dir,
            &validated.config_path_str,
            &validated.desired_table_sha256,
        )
        .is_ok();

        if !perms_ok || !dir_sync_ok {
            return Ok(CommitSuccess::PostCommitSyncWarning);
        }
        if !receipt_ok {
            return Ok(CommitSuccess::PostCommitReceiptWarning);
        }
    }

    Ok(CommitSuccess::Complete)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;

    fn write_fake_mcp_binary(dir: &Path, name: &str) -> PathBuf {
        fs::create_dir_all(dir).expect("create bin dir");
        let path = dir.join(name);
        fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write fake binary");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
        path
    }

    fn build_fixture_request(root: &Path) -> CodexSetupRequest {
        let bin_dir = root.join("bin");
        let mcp_executable = write_fake_mcp_binary(&bin_dir, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        CodexSetupRequest {
            codex_home: root.join("codex-home"),
            mcp_executable,
            state_dir: root.join("stores").join("runtime"),
            pairing_dir: root.join("stores").join("pairing"),
            registry_dir: root.join("stores").join("registry"),
            plans_dir: root.join("stores").join("plans"),
            authority_dir: root.join("stores").join("authority"),
            sessions_dir: root.join("stores").join("sessions"),
            management_dir: root.join("stores").join("codex-setup"),
        }
    }

    #[test]
    fn recognizer_accepts_legacy_and_current_args_but_rejects_wrong_final_flag() {
        let legacy = vec![
            "--state-dir".to_string(),
            "/tmp/threadlane/state".to_string(),
            "--pairing-dir".to_string(),
            "/tmp/threadlane/pairing".to_string(),
            "--registry-dir".to_string(),
            "/tmp/threadlane/registry".to_string(),
            "--plans-dir".to_string(),
            "/tmp/threadlane/plans".to_string(),
            "--authority-dir".to_string(),
            "/tmp/threadlane/authority".to_string(),
            "--sessions-dir".to_string(),
            "/tmp/threadlane/sessions".to_string(),
        ];
        assert!(matches_unbound_paired_args_shape(&legacy));

        let mut current = legacy.clone();
        current.push("--coworker-native-notices".to_string());
        assert!(matches_unbound_paired_args_shape(&current));

        let mut managed = current.clone();
        managed.push("--auto-start-approved-hosts".to_string());
        assert!(matches_unbound_paired_args_shape(&managed));

        let mut auto_start_only = legacy.clone();
        auto_start_only.push("--auto-start-approved-hosts".to_string());
        assert!(!matches_unbound_paired_args_shape(&auto_start_only));

        let mut reversed = legacy.clone();
        reversed.push("--auto-start-approved-hosts".to_string());
        reversed.push("--coworker-native-notices".to_string());
        assert!(!matches_unbound_paired_args_shape(&reversed));

        let mut wrong_value = legacy.clone();
        wrong_value.push("--coworker-native-notices=false".to_string());
        assert!(!matches_unbound_paired_args_shape(&wrong_value));

        let mut wrong_flag = legacy.clone();
        wrong_flag.push("--notify".to_string());
        assert!(!matches_unbound_paired_args_shape(&wrong_flag));

        let mut extra_arg = managed;
        extra_arg.push("--unexpected".to_string());
        assert!(!matches_unbound_paired_args_shape(&extra_arg));

        let mut duplicate_auto_start = current;
        duplicate_auto_start.push("--auto-start-approved-hosts".to_string());
        duplicate_auto_start.push("--auto-start-approved-hosts".to_string());
        assert!(!matches_unbound_paired_args_shape(&duplicate_auto_start));
    }

    #[test]
    fn first_absent_table_creates_managed_entry_and_receipt_without_authority_args() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());

        let report = apply_codex_mcp_setup(&req);
        assert_eq!(report.state, CodexSetupState::Registered);
        assert_eq!(report.mode, CodexSetupMode::Apply);
        assert!(report.registered);
        assert!(report.config_modified);
        assert!(report.restart_required);
        assert!(report.pairing_required);
        assert_eq!(report.error_code, None);

        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        assert_eq!(
            report.config_path.as_deref(),
            Some(config_path.to_str().unwrap())
        );

        let config_meta = fs::symlink_metadata(&config_path).expect("stat config.toml");
        assert_eq!(config_meta.permissions().mode() & 0o777, 0o600);

        let content = fs::read_to_string(&config_path).expect("read config.toml");
        let doc: DocumentMut = content.parse().expect("valid toml");
        let table = doc["mcp_servers"]["threadlane"]
            .as_table()
            .expect("threadlane table");
        assert_eq!(table.len(), 2);
        assert_eq!(
            table["command"].as_str(),
            Some(req.mcp_executable.to_str().unwrap())
        );
        let args: Vec<&str> = table["args"]
            .as_array()
            .expect("args array")
            .iter()
            .map(|v| v.as_str().expect("string arg"))
            .collect();
        assert_eq!(
            args,
            vec![
                "--state-dir",
                req.state_dir.to_str().unwrap(),
                "--pairing-dir",
                req.pairing_dir.to_str().unwrap(),
                "--registry-dir",
                req.registry_dir.to_str().unwrap(),
                "--plans-dir",
                req.plans_dir.to_str().unwrap(),
                "--authority-dir",
                req.authority_dir.to_str().unwrap(),
                "--sessions-dir",
                req.sessions_dir.to_str().unwrap(),
                "--coworker-native-notices",
                "--auto-start-approved-hosts",
            ]
        );
        for forbidden in [
            "--binding",
            "--thread",
            "--host-executable",
            "--notify",
            "--env",
        ] {
            assert!(
                !content.contains(forbidden),
                "config.toml must never include authority/binding flag {forbidden}"
            );
        }

        let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        let receipt_meta = fs::symlink_metadata(&receipt_path).expect("stat receipt");
        assert_eq!(receipt_meta.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn preserves_unrelated_settings_comments_and_never_copies_secrets_into_receipt() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).expect("create codex_home");

        let initial_toml = r#"# User top-level configuration comment
model = "gpt-5.4" # inline model comment
approval_policy = "on-request"
api_key_sentinel = "sk-live-super-secret-value-do-not-copy"

# Existing unrelated MCP server must stay untouched
[mcp_servers.other_tool]
command = "/usr/local/bin/other-mcp"
args = ["--stdio"]

[mcp_servers.other_tool.env]
OTHER_TOKEN = "secret-other-token-999"
"#;
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&config_path, initial_toml).expect("write initial config.toml");

        let report = apply_codex_mcp_setup(&req);
        assert_eq!(report.state, CodexSetupState::Registered);
        assert!(report.config_modified);
        assert!(report.restart_required);
        assert!(report.pairing_required);

        let updated = fs::read_to_string(&config_path).expect("read updated config.toml");
        assert!(updated.contains("# User top-level configuration comment"));
        assert!(updated.contains("model = \"gpt-5.4\" # inline model comment"));
        assert!(updated.contains("approval_policy = \"on-request\""));
        assert!(updated.contains("# Existing unrelated MCP server must stay untouched"));
        assert!(updated.contains("[mcp_servers.other_tool]"));
        assert!(updated.contains("OTHER_TOKEN = \"secret-other-token-999\""));
        assert!(updated.contains("[mcp_servers.threadlane]"));

        let receipt_raw = fs::read_to_string(req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME))
            .expect("read receipt");
        assert!(!receipt_raw.contains("sk-live-super-secret-value-do-not-copy"));
        assert!(!receipt_raw.contains("secret-other-token-999"));
        assert!(!receipt_raw.contains("other-mcp"));
    }

    #[test]
    fn exact_matching_table_is_noop_with_unchanged_mtime_and_inode() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());

        let first = apply_codex_mcp_setup(&req);
        assert_eq!(first.state, CodexSetupState::Registered);
        assert!(first.config_modified);
        assert!(first.restart_required);

        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let meta_before = fs::symlink_metadata(&config_path).expect("meta before");

        let second = apply_codex_mcp_setup(&req);
        assert_eq!(second.state, CodexSetupState::Unchanged);
        assert!(second.registered);
        assert!(!second.config_modified);
        assert!(!second.restart_required);
        assert!(second.pairing_required);

        let meta_after = fs::symlink_metadata(&config_path).expect("meta after");
        assert_eq!(meta_before.ino(), meta_after.ino());
        assert_eq!(meta_before.mtime(), meta_after.mtime());
        assert_eq!(meta_before.mtime_nsec(), meta_after.mtime_nsec());
    }

    #[test]
    fn recovery_adopts_exact_desired_table_without_rewriting_config() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());

        // First apply to generate the exact desired config, then remove management_dir to simulate
        // a process crash right after config.toml rename before receipt persistence.
        let initial = apply_codex_mcp_setup(&req);
        assert_eq!(initial.state, CodexSetupState::Registered);
        fs::remove_dir_all(&req.management_dir).expect("remove management_dir");

        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let meta_before = fs::symlink_metadata(&config_path).expect("meta before");

        let recovered = apply_codex_mcp_setup(&req);
        assert_eq!(recovered.state, CodexSetupState::Unchanged);
        assert!(recovered.registered);
        assert!(!recovered.config_modified);
        assert!(!recovered.restart_required);
        assert!(recovered.pairing_required);

        let meta_after = fs::symlink_metadata(&config_path).expect("meta after");
        assert_eq!(meta_before.ino(), meta_after.ino());
        assert_eq!(meta_before.mtime(), meta_after.mtime());
        assert_eq!(meta_before.mtime_nsec(), meta_after.mtime_nsec());
        assert!(req
            .management_dir
            .join(CODEX_SETUP_RECEIPT_FILENAME)
            .exists());
    }

    #[test]
    fn disabled_table_remains_disabled_even_if_previously_managed() {
        let tmp = TempDir::new().expect("tempdir");
        let mut req = build_fixture_request(tmp.path());

        let first = apply_codex_mcp_setup(&req);
        assert_eq!(first.state, CodexSetupState::Registered);

        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let mut content = fs::read_to_string(&config_path).expect("read config");
        content.push_str("enabled = false\n");
        fs::write(&config_path, &content).expect("write disabled config");
        req.mcp_executable = write_fake_mcp_binary(
            &tmp.path().join("bin-v2"),
            DEFAULT_NATIVE_MCP_EXECUTABLE_NAME,
        );

        let check_report = check_codex_mcp_setup(&req);
        assert_eq!(check_report.state, CodexSetupState::Disabled);
        assert!(!check_report.registered);
        assert!(!check_report.config_modified);
        assert!(!check_report.restart_required);
        assert!(check_report.pairing_required);

        let apply_report = apply_codex_mcp_setup(&req);
        assert_eq!(apply_report.state, CodexSetupState::Disabled);
        assert!(!apply_report.registered);
        assert!(!apply_report.config_modified);
        assert!(!apply_report.restart_required);
        assert!(apply_report.pairing_required);
        assert_eq!(
            fs::read_to_string(&config_path).expect("read after"),
            content
        );
    }

    #[test]
    fn unmanaged_and_manually_edited_entries_report_conflict_and_never_overwrite() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).expect("create codex_home");
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);

        // 1. Unmanaged entry (even with threadlane-native-mcp substring in command) without receipt.
        let unmanaged = format!(
            "[mcp_servers.threadlane]\ncommand = {:?}\nargs = [\"--state-dir\", \"/tmp/other\"]\n",
            req.mcp_executable.to_str().unwrap()
        );
        fs::write(&config_path, &unmanaged).expect("write unmanaged");

        let res1 = apply_codex_mcp_setup(&req);
        assert_eq!(res1.state, CodexSetupState::Conflict);
        assert!(!res1.registered);
        assert!(!res1.config_modified);
        assert!(!res1.restart_required);
        assert_eq!(res1.error_code.as_deref(), Some("manual_conflict"));
        assert_eq!(fs::read_to_string(&config_path).unwrap(), unmanaged);

        // 2. Establish a valid managed registration, then manually add env or extra field.
        fs::remove_file(&config_path).unwrap();
        let registered = apply_codex_mcp_setup(&req);
        assert_eq!(registered.state, CodexSetupState::Registered);

        let managed_text = fs::read_to_string(&config_path).unwrap();
        let with_env = format!("{managed_text}env = {{ CUSTOM_VAR = \"1\" }}\n");
        fs::write(&config_path, &with_env).unwrap();

        let res2 = apply_codex_mcp_setup(&req);
        assert_eq!(res2.state, CodexSetupState::Conflict);
        assert!(!res2.registered);
        assert!(!res2.config_modified);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), with_env);

        // 3. Manually edit one store path in args -> receipt hash mismatch -> Conflict.
        let edited_args = managed_text.replace(
            req.state_dir.to_str().unwrap(),
            "/tmp/manually-edited-state-dir",
        );
        fs::write(&config_path, &edited_args).unwrap();

        let res3 = apply_codex_mcp_setup(&req);
        assert_eq!(res3.state, CodexSetupState::Conflict);
        assert!(!res3.registered);
        assert!(!res3.config_modified);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), edited_args);
    }

    #[test]
    fn managed_upgrade_refreshes_untouched_owned_table_to_new_binary_and_store_paths() {
        let tmp = TempDir::new().expect("tempdir");
        let mut req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&config_path, "# keep header comment\nmodel = \"gpt-5.4\"\n").unwrap();

        let first = apply_codex_mcp_setup(&req);
        assert_eq!(first.state, CodexSetupState::Registered);

        // Simulate an application update with a new binary location and updated runtime state_dir.
        let v2_bin_dir = tmp.path().join("bin-v2");
        let v2_mcp = write_fake_mcp_binary(&v2_bin_dir, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        req.mcp_executable = v2_mcp.clone();
        req.state_dir = tmp.path().join("stores").join("runtime-v2");

        // Check mode reports that the owned registration is outdated without writing.
        let check_before = check_codex_mcp_setup(&req);
        assert_eq!(check_before.state, CodexSetupState::NotRegistered);
        assert!(!check_before.registered);
        assert!(!check_before.config_modified);

        // Apply mode upgrades the owned table in-place.
        let upgraded = apply_codex_mcp_setup(&req);
        assert_eq!(upgraded.state, CodexSetupState::Registered);
        assert!(upgraded.registered);
        assert!(upgraded.config_modified);
        assert!(upgraded.restart_required);
        assert!(upgraded.pairing_required);

        let updated_toml = fs::read_to_string(&config_path).unwrap();
        assert!(updated_toml.contains("# keep header comment"));
        assert!(updated_toml.contains("model = \"gpt-5.4\""));
        let doc: DocumentMut = updated_toml.parse().unwrap();
        let table = doc["mcp_servers"]["threadlane"].as_table().unwrap();
        assert_eq!(table["command"].as_str(), Some(v2_mcp.to_str().unwrap()));
        let args: Vec<&str> = table["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(args[1], req.state_dir.to_str().unwrap());
    }

    #[test]
    fn receipt_verified_upgrade_accepts_only_exact_legacy_and_notice_shapes() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).expect("create codex_home");
        assert_eq!(
            apply_codex_mcp_setup(&req).state,
            CodexSetupState::Registered
        );

        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        let command = req.mcp_executable.to_str().expect("utf8 command");
        let config_path_str = config_path.to_str().expect("utf8 config path");
        let base_args = vec![
            "--state-dir".to_string(),
            req.state_dir.to_str().unwrap().to_string(),
            "--pairing-dir".to_string(),
            req.pairing_dir.to_str().unwrap().to_string(),
            "--registry-dir".to_string(),
            req.registry_dir.to_str().unwrap().to_string(),
            "--plans-dir".to_string(),
            req.plans_dir.to_str().unwrap().to_string(),
            "--authority-dir".to_string(),
            req.authority_dir.to_str().unwrap().to_string(),
            "--sessions-dir".to_string(),
            req.sessions_dir.to_str().unwrap().to_string(),
        ];

        for legacy_args in [base_args.clone(), {
            let mut args = base_args.clone();
            args.push("--coworker-native-notices".to_string());
            args
        }] {
            let mut doc = "".parse::<DocumentMut>().expect("empty TOML document");
            apply_desired_table_to_document(&mut doc, command, &legacy_args).unwrap();
            fs::write(&config_path, doc.to_string()).unwrap();
            fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();
            write_receipt(
                &req.management_dir,
                config_path_str,
                &compute_managed_table_sha256(command, &legacy_args),
            )
            .unwrap();

            let upgraded = apply_codex_mcp_setup(&req);
            assert_eq!(upgraded.state, CodexSetupState::Registered);
            assert!(upgraded.config_modified);
            assert!(upgraded.restart_required);

            let updated: DocumentMut = fs::read_to_string(&config_path).unwrap().parse().unwrap();
            let args: Vec<&str> = updated["mcp_servers"]["threadlane"]["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect();
            assert_eq!(args[args.len() - 2], "--coworker-native-notices");
            assert_eq!(args[args.len() - 1], "--auto-start-approved-hosts");
            let receipt: CodexSetupReceipt =
                serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
            assert_eq!(
                inspect_threadlane_table(&updated),
                InspectedTableState::CandidateManaged {
                    table_sha256: receipt.managed_table_sha256
                }
            );
        }

        // A receipt matching an unrecognized shape (auto-start without managed notices) still
        // fails closed: receipts only authorize shapes that Threadlane actually emitted.
        let mut unrecognized = base_args;
        unrecognized.push("--auto-start-approved-hosts".to_string());
        let mut doc = "".parse::<DocumentMut>().expect("empty TOML document");
        apply_desired_table_to_document(&mut doc, command, &unrecognized).unwrap();
        let original = doc.to_string();
        fs::write(&config_path, &original).unwrap();
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();
        write_receipt(
            &req.management_dir,
            config_path_str,
            &compute_managed_table_sha256(command, &unrecognized),
        )
        .unwrap();

        let conflict = apply_codex_mcp_setup(&req);
        assert_eq!(conflict.state, CodexSetupState::Conflict);
        assert_eq!(conflict.error_code.as_deref(), Some("manual_conflict"));
        assert!(!conflict.config_modified);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), original);
    }

    #[test]
    fn explicitly_enabled_owned_upgrade_preserves_enablement_comments_and_receipt_hashes() {
        let tmp = TempDir::new().expect("tempdir");
        let mut req = build_fixture_request(tmp.path());
        assert_eq!(
            apply_codex_mcp_setup(&req).state,
            CodexSetupState::Registered
        );
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        let old_receipt = fs::read(&receipt_path).unwrap();
        let original = fs::read_to_string(&config_path).unwrap();
        let enabled_line = "# Keep explicit enablement\nenabled = true # operator preference\n";
        let enabled_config =
            format!("# Keep header\nmodel = \"gpt-5.4\"\n{original}{enabled_line}");
        fs::write(&config_path, &enabled_config).unwrap();
        let old_doc: DocumentMut = original.parse().unwrap();
        let enabled_doc: DocumentMut = enabled_config.parse().unwrap();
        assert_eq!(
            inspect_threadlane_table(&old_doc),
            inspect_threadlane_table(&enabled_doc)
        );

        req.mcp_executable = write_fake_mcp_binary(
            &tmp.path().join("bin-v2"),
            DEFAULT_NATIVE_MCP_EXECUTABLE_NAME,
        );
        req.state_dir = tmp.path().join("stores/runtime-v2");
        let before_check = fs::metadata(&config_path).unwrap();
        let receipt_before_check = fs::metadata(&receipt_path).unwrap();
        let checked = check_codex_mcp_setup(&req);
        assert_eq!(checked.state, CodexSetupState::NotRegistered);
        assert!(!checked.config_modified);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), enabled_config);
        assert_eq!(fs::read(&receipt_path).unwrap(), old_receipt);
        let after_check = fs::metadata(&config_path).unwrap();
        assert_eq!(before_check.ino(), after_check.ino());
        assert_eq!(
            before_check.modified().unwrap(),
            after_check.modified().unwrap()
        );
        assert_eq!(
            receipt_before_check.modified().unwrap(),
            fs::metadata(&receipt_path).unwrap().modified().unwrap()
        );

        let upgraded = apply_codex_mcp_setup(&req);
        assert_eq!(upgraded.state, CodexSetupState::Registered);
        assert!(upgraded.registered && upgraded.config_modified && upgraded.restart_required);
        let updated = fs::read_to_string(&config_path).unwrap();
        assert!(updated.contains(enabled_line));
        assert!(updated.starts_with("# Keep header\nmodel = \"gpt-5.4\"\n"));
        let doc: DocumentMut = updated.parse().unwrap();
        assert_eq!(
            doc["mcp_servers"]["threadlane"]["enabled"].as_bool(),
            Some(true)
        );
        assert_eq!(
            doc["mcp_servers"]["threadlane"]["command"].as_str(),
            req.mcp_executable.to_str()
        );
        assert_eq!(
            doc["mcp_servers"]["threadlane"]["args"][1].as_str(),
            req.state_dir.to_str()
        );
        let receipt: CodexSetupReceipt =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(receipt.schema_version, 1);
        assert_eq!(
            inspect_threadlane_table(&doc),
            InspectedTableState::CandidateManaged {
                table_sha256: receipt.managed_table_sha256
            }
        );
        let before_reapply = fs::metadata(&config_path).unwrap();
        let reapplied = apply_codex_mcp_setup(&req);
        assert_eq!(reapplied.state, CodexSetupState::Unchanged);
        assert!(!reapplied.config_modified);
        assert!(!reapplied.restart_required);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), updated);
        let after_reapply = fs::metadata(&config_path).unwrap();
        assert_eq!(before_reapply.ino(), after_reapply.ino());
        assert_eq!(
            before_reapply.modified().unwrap(),
            after_reapply.modified().unwrap()
        );
    }

    #[test]
    fn explicitly_enabled_exact_desired_recovery_adopts_receipt_without_rewriting_config() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        assert_eq!(
            apply_codex_mcp_setup(&req).state,
            CodexSetupState::Registered
        );
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        let receipt = fs::read(&receipt_path).unwrap();
        let content = format!(
            "{}enabled = true # preserve\n",
            fs::read_to_string(&config_path).unwrap()
        );
        fs::write(&config_path, &content).unwrap();
        fs::remove_dir_all(&req.management_dir).unwrap();
        let before = fs::metadata(&config_path).unwrap();
        assert_eq!(
            check_codex_mcp_setup(&req).state,
            CodexSetupState::Unchanged
        );
        assert!(!req.management_dir.exists());
        let recovered = apply_codex_mcp_setup(&req);
        assert_eq!(recovered.state, CodexSetupState::Unchanged);
        assert!(recovered.registered);
        assert!(!recovered.config_modified && !recovered.restart_required);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), content);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt);
        let after = fs::metadata(&config_path).unwrap();
        assert_eq!(before.ino(), after.ino());
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
    }

    #[test]
    fn explicitly_enabled_upgrade_rejects_unowned_tampered_and_unknown_settings() {
        for case in [
            "missing_receipt",
            "wrong_digest",
            "wrong_schema",
            "wrong_server",
            "wrong_config",
            "command",
            "args",
            "enabled_string",
            "enabled_integer",
            "env",
            "cwd",
            "timeout",
        ] {
            let tmp = TempDir::new().expect("tempdir");
            let mut req = build_fixture_request(tmp.path());
            assert_eq!(
                apply_codex_mcp_setup(&req).state,
                CodexSetupState::Registered
            );
            let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
            let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
            let mut content = format!(
                "{}enabled = true\n",
                fs::read_to_string(&config_path).unwrap()
            );
            let mut receipt: CodexSetupReceipt =
                serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
            let expected = match case {
                "wrong_schema" | "wrong_server" | "wrong_config" => CodexSetupState::Error,
                _ => CodexSetupState::Conflict,
            };
            match case {
                "missing_receipt" => fs::remove_file(&receipt_path).unwrap(),
                "wrong_digest" => receipt.managed_table_sha256 = "0".repeat(64),
                "wrong_schema" => receipt.schema_version = 99,
                "wrong_server" => receipt.server_name = "foreign".into(),
                "wrong_config" => {
                    receipt.config_path = tmp.path().join("foreign.toml").display().to_string()
                }
                "command" => {
                    content =
                        content.replace(req.mcp_executable.to_str().unwrap(), "/tmp/manual-mcp")
                }
                "args" => {
                    content = content.replace(req.state_dir.to_str().unwrap(), "/tmp/manual-state")
                }
                "enabled_string" => {
                    content = content.replace("enabled = true", "enabled = \"true\"")
                }
                "enabled_integer" => content = content.replace("enabled = true", "enabled = 1"),
                "env" => content.push_str("env = { CUSTOM_VAR = \"1\" }\n"),
                "cwd" => content.push_str("cwd = \"/tmp\"\n"),
                "timeout" => content.push_str("startup_timeout_sec = 10\n"),
                _ => unreachable!(),
            }
            if case != "missing_receipt" {
                fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
            }
            fs::write(&config_path, &content).unwrap();
            let receipt_before = fs::read(&receipt_path).ok();
            req.mcp_executable = write_fake_mcp_binary(
                &tmp.path().join("bin-v2"),
                DEFAULT_NATIVE_MCP_EXECUTABLE_NAME,
            );
            for report in [check_codex_mcp_setup(&req), apply_codex_mcp_setup(&req)] {
                assert_eq!(report.state, expected, "{case}");
                assert!(
                    !report.registered && !report.config_modified && !report.restart_required,
                    "{case}"
                );
            }
            assert_eq!(fs::read_to_string(&config_path).unwrap(), content, "{case}");
            assert_eq!(fs::read(&receipt_path).ok(), receipt_before, "{case}");
        }
    }

    #[test]
    fn paths_with_spaces_are_registered_as_plain_argv_strings() {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().join("Path With Spaces");
        let bin_dir = root.join("App Bin");
        let mcp_exe = write_fake_mcp_binary(&bin_dir, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        let req = CodexSetupRequest {
            codex_home: root.join("Codex Home"),
            mcp_executable: mcp_exe.clone(),
            state_dir: root.join("State Dir"),
            pairing_dir: root.join("Pairing Dir"),
            registry_dir: root.join("Registry Dir"),
            plans_dir: root.join("Plans Dir"),
            authority_dir: root.join("Authority Dir"),
            sessions_dir: root.join("Sessions Dir"),
            management_dir: root.join("Management Dir"),
        };

        let report = apply_codex_mcp_setup(&req);
        assert_eq!(report.state, CodexSetupState::Registered);

        let content = fs::read_to_string(req.codex_home.join(CODEX_CONFIG_FILENAME)).unwrap();
        let doc: DocumentMut = content.parse().unwrap();
        let table = doc["mcp_servers"]["threadlane"].as_table().unwrap();
        assert_eq!(table["command"].as_str(), Some(mcp_exe.to_str().unwrap()));
        let args: Vec<&str> = table["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(args[1], req.state_dir.to_str().unwrap());
        assert!(!args[1].contains("\\ "));
    }

    #[test]
    fn malformed_toml_and_corrupt_receipt_return_clean_error_without_secret_leak_or_repair() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);

        let broken_toml = "secret_token = \"sk-secret-99999\"\n[mcp_servers.threadlane\ncommand = ";
        fs::write(&config_path, broken_toml).unwrap();

        let report = apply_codex_mcp_setup(&req);
        assert_eq!(report.state, CodexSetupState::Error);
        assert_eq!(report.error_code.as_deref(), Some("malformed_config"));
        assert!(!report.message.contains("sk-secret-99999"));
        assert_eq!(fs::read_to_string(&config_path).unwrap(), broken_toml);

        // Fix config.toml, register cleanly, then corrupt the receipt.
        fs::remove_file(&config_path).unwrap();
        assert_eq!(
            apply_codex_mcp_setup(&req).state,
            CodexSetupState::Registered
        );

        let receipt_path = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        fs::write(&receipt_path, b"{not-valid-json").unwrap();
        fs::set_permissions(&receipt_path, fs::Permissions::from_mode(0o600)).unwrap();

        let corrupt_report = apply_codex_mcp_setup(&req);
        assert_eq!(corrupt_report.state, CodexSetupState::Error);
        assert_eq!(
            corrupt_report.error_code.as_deref(),
            Some("corrupt_receipt")
        );
        assert_eq!(
            fs::read_to_string(&receipt_path).unwrap(),
            "{not-valid-json"
        );
    }

    #[test]
    fn symlink_and_read_only_paths_are_rejected_without_forced_repair() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);

        // 1. Symlink config.toml
        let target_file = tmp.path().join("external-config.toml");
        fs::write(&target_file, "model = \"gpt-5.4\"\n").unwrap();
        std::os::unix::fs::symlink(&target_file, &config_path).unwrap();

        let sym_report = apply_codex_mcp_setup(&req);
        assert_eq!(sym_report.state, CodexSetupState::Error);
        assert_eq!(sym_report.error_code.as_deref(), Some("security_violation"));
        assert!(fs::symlink_metadata(&config_path)
            .unwrap()
            .file_type()
            .is_symlink());

        fs::remove_file(&config_path).unwrap();

        // 2. Read-only config.toml (0400)
        fs::write(&config_path, "model = \"gpt-5.4\"\n").unwrap();
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o400)).unwrap();

        let ro_report = apply_codex_mcp_setup(&req);
        assert_eq!(ro_report.state, CodexSetupState::Error);
        assert_eq!(ro_report.error_code.as_deref(), Some("read_only_path"));
        assert_eq!(
            fs::symlink_metadata(&config_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );

        // Restore permissions so cleanup succeeds.
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::remove_file(&config_path).unwrap();

        // 3. Read-only codex_home (0500)
        fs::set_permissions(&req.codex_home, fs::Permissions::from_mode(0o500)).unwrap();
        let ro_dir_report = apply_codex_mcp_setup(&req);
        assert_eq!(ro_dir_report.state, CodexSetupState::Error);
        assert_eq!(ro_dir_report.error_code.as_deref(), Some("read_only_path"));
        fs::set_permissions(&req.codex_home, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn check_mode_creates_no_files_or_directories_and_overlapping_stores_fail() {
        let tmp = TempDir::new().expect("tempdir");
        let mut req = build_fixture_request(tmp.path());

        let check_report = check_codex_mcp_setup(&req);
        assert_eq!(check_report.state, CodexSetupState::NotRegistered);
        assert!(!check_report.registered);
        assert!(!check_report.config_modified);
        assert!(!check_report.restart_required);
        assert!(check_report.pairing_required);
        assert!(!req.codex_home.exists());
        assert!(!req.management_dir.exists());

        // Overlapping store directories must be rejected before any filesystem writes.
        req.pairing_dir = req.state_dir.join("nested-pairing");
        let overlap_report = apply_codex_mcp_setup(&req);
        assert_eq!(overlap_report.state, CodexSetupState::Error);
        assert_eq!(overlap_report.error_code.as_deref(), Some("invalid_config"));
        assert!(!req.codex_home.exists());
        assert!(!req.management_dir.exists());
    }

    #[test]
    fn stale_concurrent_config_modification_is_rejected_and_concurrent_applies_serialize() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&config_path, "model = \"gpt-5.4\"\n").unwrap();

        let hook_config_path = config_path.clone();
        let stale_report = run_codex_mcp_setup_internal(
            &req,
            CodexSetupMode::Apply,
            Some(&move || {
                fs::write(
                    &hook_config_path,
                    "model = \"gpt-5.4\"\n# concurrent external edit\n",
                )
                .expect("concurrent edit");
            }),
            None,
        );
        assert_eq!(stale_report.state, CodexSetupState::Error);
        assert_eq!(
            stale_report.error_code.as_deref(),
            Some("concurrent_modification")
        );
        let preserved = fs::read_to_string(&config_path).unwrap();
        assert!(preserved.contains("# concurrent external edit"));
        assert!(!preserved.contains("[mcp_servers.threadlane]"));

        // Concurrent normal apply launches either serialize or return setup_busy on lock contention,
        // never hanging or corrupting unrelated settings.
        let shared_req = Arc::new(req.clone());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let r = Arc::clone(&shared_req);
            handles.push(std::thread::spawn(move || apply_codex_mcp_setup(&r)));
        }
        let mut registered_count = 0;
        let mut unchanged_count = 0;
        let mut busy_count = 0;
        for h in handles {
            let rep = h.join().expect("thread join");
            match rep.state {
                CodexSetupState::Registered => {
                    assert!(rep.config_modified);
                    assert!(rep.restart_required);
                    registered_count += 1;
                }
                CodexSetupState::Unchanged => {
                    assert!(!rep.config_modified);
                    unchanged_count += 1;
                }
                CodexSetupState::Error if rep.error_code.as_deref() == Some("setup_busy") => {
                    assert!(!rep.config_modified);
                    assert!(!rep.registered);
                    busy_count += 1;
                }
                other => panic!("unexpected concurrent state: {other:?} ({rep:?})"),
            }
        }
        assert_eq!(registered_count, 1);
        assert_eq!(registered_count + unchanged_count + busy_count, 4);
        let final_config = fs::read_to_string(&config_path).unwrap();
        assert!(final_config.contains("model = \"gpt-5.4\""));
        assert!(final_config.contains("# concurrent external edit"));
        assert!(final_config.contains("[mcp_servers.threadlane]"));
    }

    #[test]
    fn lock_contention_returns_setup_busy_promptly_without_modifying_config() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        fs::create_dir_all(&req.management_dir).unwrap();
        fs::set_permissions(&req.management_dir, fs::Permissions::from_mode(0o700)).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        let initial_content = "model = \"gpt-5.4\"\n";
        fs::write(&config_path, initial_content).unwrap();

        let lock_path = req.management_dir.join(CODEX_SETUP_LOCK_FILENAME);
        let held_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)
            .expect("open lock file");
        held_lock
            .try_lock_exclusive()
            .expect("acquire exclusive lock in test");

        let busy_report = apply_codex_mcp_setup(&req);
        assert_eq!(busy_report.state, CodexSetupState::Error);
        assert_eq!(busy_report.error_code.as_deref(), Some("setup_busy"));
        assert!(!busy_report.registered);
        assert!(!busy_report.config_modified);
        assert!(!busy_report.restart_required);
        assert!(busy_report.pairing_required);
        assert_eq!(fs::read_to_string(&config_path).unwrap(), initial_content);
        assert!(!req
            .management_dir
            .join(CODEX_SETUP_RECEIPT_FILENAME)
            .exists());

        // Explicitly unlock the raw test FD before closing it (matching SetupLockGuard::drop)
        // so an inherited open-file description in a concurrent pre-exec child cannot retain flock.
        fs2::FileExt::unlock(&held_lock).expect("unlock exclusive lock in test");
        drop(held_lock);

        let after_release = apply_codex_mcp_setup(&req);
        assert_eq!(
            after_release.state,
            CodexSetupState::Registered,
            "setup after releasing the held lock returned: {after_release:?}"
        );
        assert!(after_release.registered);
        assert!(after_release.config_modified);
    }

    #[test]
    fn post_commit_receipt_failure_preserves_truthful_config_modified_and_restart_flags() {
        let tmp = TempDir::new().expect("tempdir");
        let req = build_fixture_request(tmp.path());
        fs::create_dir_all(&req.codex_home).unwrap();
        let config_path = req.codex_home.join(CODEX_CONFIG_FILENAME);
        fs::write(&config_path, "model = \"gpt-5.4\"\n").unwrap();

        let receipt_blocker = req.management_dir.join(CODEX_SETUP_RECEIPT_FILENAME);
        let report = run_codex_mcp_setup_internal(
            &req,
            CodexSetupMode::Apply,
            None,
            Some(&move || {
                // Create a directory at the target receipt path after config.toml rename
                // so persisting the receipt temp file fails deterministically with EISDIR.
                fs::create_dir_all(&receipt_blocker).expect("create receipt blocker dir");
            }),
        );

        assert_eq!(report.state, CodexSetupState::Registered);
        assert!(report.registered);
        assert!(report.config_modified);
        assert!(report.restart_required);
        assert!(report.pairing_required);
        assert_eq!(
            report.error_code.as_deref(),
            Some("receipt_persistence_failed")
        );
        assert!(report.message.contains("ownership receipt"));

        let written_config = fs::read_to_string(&config_path).unwrap();
        assert!(written_config.contains("model = \"gpt-5.4\""));
        assert!(written_config.contains("[mcp_servers.threadlane]"));

        // While the receipt path is invalid, check/apply refuse to force-repair it.
        let blocked_check = check_codex_mcp_setup(&req);
        assert_eq!(blocked_check.state, CodexSetupState::Error);
        assert_eq!(
            blocked_check.error_code.as_deref(),
            Some("security_violation")
        );
        assert!(req
            .management_dir
            .join(CODEX_SETUP_RECEIPT_FILENAME)
            .is_dir());
        assert_eq!(fs::read_to_string(&config_path).unwrap(), written_config);
    }

    #[test]
    fn environment_and_executable_resolution_helpers_follow_contract() {
        let tmp = TempDir::new().expect("tempdir");
        let home_dir = tmp.path().join("home");
        let custom_codex = tmp.path().join("custom-codex");

        let mut env = BTreeMap::new();
        env.insert("HOME".to_string(), OsString::from(home_dir.as_os_str()));

        assert_eq!(
            resolve_codex_home_with(|k| env.get(k).cloned()).unwrap(),
            home_dir.join(".codex")
        );

        env.insert(
            "CODEX_HOME".to_string(),
            OsString::from(custom_codex.as_os_str()),
        );
        assert_eq!(
            resolve_codex_home_with(|k| env.get(k).cloned()).unwrap(),
            custom_codex
        );

        // Runtime state_dir precedence:
        // 1. Explicit --state-dir
        let explicit_state = tmp.path().join("explicit-state");
        assert_eq!(
            resolve_default_runtime_state_dir_with(Some(&explicit_state), |k| env.get(k).cloned())
                .unwrap(),
            explicit_state
        );

        // 2. Empty THREADLANE_CLI_XDG_STATE_HOME marker ignores isolated XDG_STATE_HOME and uses HOME.
        env.insert(
            "XDG_STATE_HOME".to_string(),
            OsString::from(tmp.path().join("isolated-gui-xdg").as_os_str()),
        );
        env.insert(
            THREADLANE_CLI_XDG_STATE_HOME_ENV.to_string(),
            OsString::new(),
        );
        assert_eq!(
            resolve_default_runtime_state_dir_with(None, |k| env.get(k).cloned()).unwrap(),
            home_dir
                .join(".local")
                .join("state")
                .join("threadlane")
                .join("runtime")
        );

        // 3. Non-empty THREADLANE_CLI_XDG_STATE_HOME marker wins over isolated XDG_STATE_HOME.
        let orig_xdg = tmp.path().join("orig-xdg-state");
        env.insert(
            THREADLANE_CLI_XDG_STATE_HOME_ENV.to_string(),
            OsString::from(orig_xdg.as_os_str()),
        );
        assert_eq!(
            resolve_default_runtime_state_dir_with(None, |k| env.get(k).cloned()).unwrap(),
            orig_xdg.join("threadlane").join("runtime")
        );

        // Executable resolution: PATH first, then canonical sibling of current_exe.
        let path_bin_dir = tmp.path().join("path-bin");
        let path_mcp = write_fake_mcp_binary(&path_bin_dir, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);
        let sibling_dir = tmp.path().join("bundle-bin");
        let fake_desktop = write_fake_mcp_binary(&sibling_dir, "threadlane-desktop");
        let sibling_mcp = write_fake_mcp_binary(&sibling_dir, DEFAULT_NATIVE_MCP_EXECUTABLE_NAME);

        let resolved_from_path =
            resolve_native_mcp_executable_with(Some(path_bin_dir.as_os_str()), Some(&fake_desktop))
                .unwrap();
        assert_eq!(resolved_from_path, path_mcp);

        let resolved_from_sibling =
            resolve_native_mcp_executable_with(None, Some(&fake_desktop)).unwrap();
        assert_eq!(resolved_from_sibling, sibling_mcp);
    }
}
