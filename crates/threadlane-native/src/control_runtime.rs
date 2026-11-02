use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use threadlane::descriptor::{
    RuntimeDescriptor, DATABASE_FILENAME, DESCRIPTOR_FILENAME, LOCK_FILENAME, MAX_DESCRIPTOR_BYTES,
};
use threadlane::{Error as ThreadlaneError, ErrorCode as ThreadlaneErrorCode, SupervisorClient};
use tokio::process::{Child, Command};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::sanitize_diagnostic_text;
use crate::rpc::is_process_alive;

/// Default bounded timeout (`5s`) for [`ensure_control_runtime`].
pub const DEFAULT_CONTROL_RUNTIME_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Minimum allowed timeout (`1ms`) for [`ensure_control_runtime`].
pub const MIN_CONTROL_RUNTIME_STARTUP_TIMEOUT: Duration = Duration::from_millis(1);

/// Maximum allowed timeout (`60s`) for [`ensure_control_runtime`].
pub const MAX_CONTROL_RUNTIME_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Polling interval (`20ms`) while awaiting control-plane supervisor startup or winner readiness.
pub const CONTROL_RUNTIME_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Maximum duration (`500ms`) for a single HTTP readiness probe attempt within the outer deadline.
const MAX_SINGLE_PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// Explicit statement of how normal `Supervisor` startup handles pre-existing in-flight worker runs.
pub const CONTROL_RUNTIME_IN_FLIGHT_RECOVERY_POLICY: &str =
    "Normal Supervisor startup marks pre-existing non-terminal worker runs Unknown in state.db and never silently re-dispatches work.";

/// Explicit statement of the started control-plane supervisor's observed process lifetime.
///
/// The helper spawns `<executable> serve --state-dir <dir>` on Linux with `process_group(0)`,
/// `kill_on_drop(false)`, `Stdio::null()`, and a cleared environment (`env_clear()`). This allows
/// the spawned supervisor process to survive normal exit of the direct caller process when the
/// enclosing OS session/cgroup remains active, but does not install an OS service, does not escape
/// cgroup/scope termination, does not guarantee automatic termination at logout, and does not
/// act as a subreaper for detached descendants.
pub const CONTROL_RUNTIME_LIFETIME_POLICY: &str =
    "Linux background process spawned with process_group(0), kill_on_drop(false), Stdio::null(), and env_clear(); survives direct caller process exit while the enclosing OS session/cgroup remains active, but is not an OS service, does not escape cgroup termination or guarantee logout stop, and does not reap detached descendants.";

/// Outcome status of [`ensure_control_runtime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlRuntimeEnsureStatus {
    /// A healthy authenticated control-plane supervisor was already running and holding
    /// `supervisor.lock`; it was reused without spawning another process.
    AlreadyRunning,
    /// No healthy control-plane supervisor was running; a new detached
    /// `<executable> serve --state-dir <dir>` process was started and verified via
    /// authenticated readiness.
    Started,
}

impl ControlRuntimeEnsureStatus {
    /// Stable snake_case identifier for this status (`already_running` or `started`).
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyRunning => "already_running",
            Self::Started => "started",
        }
    }
}

impl fmt::Display for ControlRuntimeEnsureStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured outcome returned by [`ensure_control_runtime`].
///
/// Uses owned wire-safe fields so it can be deserialized from arbitrary temporary buffers, and
/// never contains bearer tokens or secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRuntimeEnsureOutcome {
    /// Whether an existing authenticated supervisor was reused (`already_running`) or a new
    /// supervisor process was launched (`started`).
    pub status: ControlRuntimeEnsureStatus,
    /// Canonical private runtime state directory (`0700`).
    pub state_dir: PathBuf,
    /// OS process identifier of the verified running supervisor holding `supervisor.lock`.
    pub pid: u32,
    /// Loopback TCP port (`127.0.0.1:<port>`) from the verified `supervisor.json` descriptor.
    pub port: u16,
    /// Count of persisted bindings reported by the authenticated `GET /v1/bindings` readiness check.
    pub bindings_count: usize,
    /// `true` if a provably stale `supervisor.json` (dead PID and unlocked `supervisor.lock`)
    /// was present before startup and was replaced through normal locked `Supervisor` startup
    /// without deleting `supervisor.json`, `state.db`, or `supervisor.lock`.
    pub stale_descriptor_recovered: bool,
    /// `Some(false)` when this call launched the supervisor (`started`, which omits `--codex` and
    /// `--notify-binding`); `None` when reusing an existing supervisor (`already_running`) whose
    /// startup flags were not verified by `GET /v1/bindings`.
    pub notifications_enabled: Option<bool>,
    /// `Some(false)` when this call launched the supervisor (`started`, which omits
    /// `--workers-config`); `None` when reusing an existing supervisor (`already_running`) whose
    /// worker configuration was not verified by `GET /v1/bindings`.
    pub workers_enabled: Option<bool>,
    /// Always `false`: approved per-session hosts are never started or auto-resumed by this helper.
    pub approved_hosts_resumed: bool,
    /// Explicit statement of how normal `Supervisor` startup handles old in-flight worker runs.
    pub in_flight_runs_recovery_policy: String,
    /// Explicit statement of the started control process's observed background lifetime and limits.
    pub process_lifetime_policy: String,
}

impl ControlRuntimeEnsureOutcome {
    fn already_running(
        state_dir: PathBuf,
        pid: u32,
        port: u16,
        bindings_count: usize,
        stale_descriptor_recovered: bool,
    ) -> Self {
        Self {
            status: ControlRuntimeEnsureStatus::AlreadyRunning,
            state_dir,
            pid,
            port,
            bindings_count,
            stale_descriptor_recovered,
            notifications_enabled: None,
            workers_enabled: None,
            approved_hosts_resumed: false,
            in_flight_runs_recovery_policy: CONTROL_RUNTIME_IN_FLIGHT_RECOVERY_POLICY.to_string(),
            process_lifetime_policy: CONTROL_RUNTIME_LIFETIME_POLICY.to_string(),
        }
    }

    fn started(
        state_dir: PathBuf,
        pid: u32,
        port: u16,
        bindings_count: usize,
        stale_descriptor_recovered: bool,
    ) -> Self {
        Self {
            status: ControlRuntimeEnsureStatus::Started,
            state_dir,
            pid,
            port,
            bindings_count,
            stale_descriptor_recovered,
            notifications_enabled: Some(false),
            workers_enabled: Some(false),
            approved_hosts_resumed: false,
            in_flight_runs_recovery_policy: CONTROL_RUNTIME_IN_FLIGHT_RECOVERY_POLICY.to_string(),
            process_lifetime_policy: CONTROL_RUNTIME_LIFETIME_POLICY.to_string(),
        }
    }
}

/// Machine-readable error code for [`ControlRuntimeError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlRuntimeErrorCode {
    /// The current operating system is not supported (`ensure_control_runtime` requires Linux).
    UnsupportedPlatform,
    /// Caller-supplied path or timeout configuration is invalid.
    InvalidConfig,
    /// `state_dir`, ancestor directories, `supervisor.json`, `supervisor.lock`, `state.db`, or the
    /// executable violated private-directory, trusted-ancestor, symlink, ownership, or permission
    /// invariants.
    SecurityViolation,
    /// `supervisor.json` is malformed or fails descriptor schema validation.
    InvalidDescriptor,
    /// A live supervisor rejected the bearer token in `supervisor.json` with `401 Unauthorized`.
    UnauthorizedSupervisor,
    /// `supervisor.lock` is held by another process that did not publish a verified ready
    /// supervisor within the caller's timeout.
    LockBusy,
    /// `supervisor.json` references a live process that does not hold `supervisor.lock` or
    /// did not pass authenticated readiness verification; takeover is refused.
    AmbiguousLiveSupervisor,
    /// Runtime directory, executable, or descriptor identity changed unexpectedly during
    /// verification or startup.
    StateChanged,
    /// Spawning `<executable> serve --state-dir <dir>` failed at the OS level.
    SpawnFailed,
    /// The spawned `<executable> serve --state-dir <dir>` process exited before becoming ready,
    /// and no concurrent winner holds `supervisor.lock`.
    StartupProcessExited,
    /// The caller timeout expired before a spawned supervisor published a verified ready endpoint.
    StartupTimedOut,
    /// An underlying filesystem or I/O operation failed.
    IoError,
}

impl ControlRuntimeErrorCode {
    /// Stable snake_case identifier for this error code.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::InvalidConfig => "invalid_config",
            Self::SecurityViolation => "security_violation",
            Self::InvalidDescriptor => "invalid_descriptor",
            Self::UnauthorizedSupervisor => "unauthorized_supervisor",
            Self::LockBusy => "lock_busy",
            Self::AmbiguousLiveSupervisor => "ambiguous_live_supervisor",
            Self::StateChanged => "state_changed",
            Self::SpawnFailed => "spawn_failed",
            Self::StartupProcessExited => "startup_process_exited",
            Self::StartupTimedOut => "startup_timed_out",
            Self::IoError => "io_error",
        }
    }
}

impl fmt::Display for ControlRuntimeErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured, redacted error returned by [`ensure_control_runtime`] and [`ControlRuntimePolicy`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRuntimeError {
    code: ControlRuntimeErrorCode,
    message: String,
}

impl ControlRuntimeError {
    /// Constructs a new [`ControlRuntimeError`] with a sanitized diagnostic message.
    #[must_use]
    pub fn new(code: ControlRuntimeErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: sanitize_diagnostic_text(&message.into()),
        }
    }

    /// Returns the typed [`ControlRuntimeErrorCode`].
    #[must_use]
    pub const fn error_code(&self) -> ControlRuntimeErrorCode {
        self.code
    }

    /// Returns the stable snake_case error code string.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code.as_str()
    }

    /// Returns the sanitized, human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    fn with_cleanup_outcome(mut self, cleanup: &CleanupOutcome) -> Self {
        if let Some(ref note) = cleanup.uncertainty_note {
            self.message = sanitize_diagnostic_text(&format!("{} ({note})", self.message));
        }
        self
    }
}

impl fmt::Display for ControlRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for ControlRuntimeError {}

impl From<ControlRuntimeError> for NativeError {
    fn from(err: ControlRuntimeError) -> Self {
        let mapped = match err.code {
            ControlRuntimeErrorCode::UnsupportedPlatform => NativeErrorCode::UnsupportedPlatform,
            ControlRuntimeErrorCode::InvalidConfig => NativeErrorCode::InvalidConfig,
            ControlRuntimeErrorCode::SecurityViolation
            | ControlRuntimeErrorCode::InvalidDescriptor => NativeErrorCode::SecurityViolation,
            ControlRuntimeErrorCode::UnauthorizedSupervisor
            | ControlRuntimeErrorCode::LockBusy
            | ControlRuntimeErrorCode::AmbiguousLiveSupervisor
            | ControlRuntimeErrorCode::StateChanged
            | ControlRuntimeErrorCode::SpawnFailed
            | ControlRuntimeErrorCode::StartupProcessExited => {
                NativeErrorCode::TransportUnavailable
            }
            ControlRuntimeErrorCode::StartupTimedOut => NativeErrorCode::TimedOut,
            ControlRuntimeErrorCode::IoError => NativeErrorCode::IoError,
        };
        Self::new(mapped, format!("control runtime error ({})", err.code()))
    }
}

/// Validated, opt-in configuration for ensuring a single local control-plane supervisor is running.
///
/// Launches only `<threadlane_executable> serve --state-dir <state_dir>` with a cleared
/// environment (`env_clear()`) and notifications, worker profiles, and approved-host auto-start
/// disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlRuntimePolicy {
    state_dir: PathBuf,
    threadlane_executable: PathBuf,
    timeout: Duration,
}

impl ControlRuntimePolicy {
    /// Validates `state_dir`, `threadlane_executable`, and `timeout` and constructs a
    /// [`ControlRuntimePolicy`].
    pub fn new(
        state_dir: impl AsRef<Path>,
        threadlane_executable: impl AsRef<Path>,
        timeout: Duration,
    ) -> Result<Self, ControlRuntimeError> {
        ensure_linux_supported()?;
        let validated_timeout = validate_control_runtime_timeout(timeout)?;
        let (validated_executable, _) =
            validate_executable_with_identity(threadlane_executable.as_ref(), None)?;
        let validated_state_dir = validate_control_runtime_state_dir(state_dir.as_ref())?;
        Ok(Self {
            state_dir: validated_state_dir,
            threadlane_executable: validated_executable,
            timeout: validated_timeout,
        })
    }

    /// Returns the validated canonical private `state_dir` (`0700`).
    #[must_use]
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Returns the validated canonical `threadlane` executable path.
    #[must_use]
    pub fn threadlane_executable(&self) -> &Path {
        &self.threadlane_executable
    }

    /// Returns the bounded caller timeout (`1ms..=60s`).
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the exact argument vector passed to `threadlane`:
    /// `["serve", "--state-dir", "<state_dir>"]`.
    #[must_use]
    pub fn serve_args(&self) -> Vec<OsString> {
        build_control_runtime_serve_args(&self.state_dir)
    }

    /// Ensures the dedicated control-plane supervisor for `self.state_dir()` is running and
    /// responds to an authenticated readiness check within `self.timeout()`.
    pub async fn ensure(&self) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
        ensure_control_runtime(&self.state_dir, &self.threadlane_executable, self.timeout).await
    }
}

/// Validates the bounded caller timeout (`1ms..=60s`) for [`ensure_control_runtime`].
pub fn validate_control_runtime_timeout(
    timeout: Duration,
) -> Result<Duration, ControlRuntimeError> {
    if timeout < MIN_CONTROL_RUNTIME_STARTUP_TIMEOUT
        || timeout > MAX_CONTROL_RUNTIME_STARTUP_TIMEOUT
    {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::InvalidConfig,
            format!(
                "control runtime timeout must be between {}ms and {}s (got {:?})",
                MIN_CONTROL_RUNTIME_STARTUP_TIMEOUT.as_millis(),
                MAX_CONTROL_RUNTIME_STARTUP_TIMEOUT.as_secs(),
                timeout
            ),
        ));
    }
    Ok(timeout)
}

/// Validates an explicitly supplied absolute `threadlane` executable path.
///
/// Enforces:
/// 1. Linux platform support (`UnsupportedPlatform` on other platforms).
/// 2. Non-empty absolute path with no `.` or `..` traversal components.
/// 3. No symlink components on the executable path or any ancestor directory.
/// 4. Every ancestor directory is owned by the current effective UID or `root` (`uid == 0`) and is
///    not group- or world-writable (except sticky directories like `/tmp` when the immediate child
///    already exists, is owned by the current effective UID or `root`, and is not group/world-writable).
/// 5. Target exists, is a regular file owned by the current effective UID (or `root`), has at
///    least one execute bit set (`mode & 0o111 != 0`), and is not group- or world-writable
///    (`mode & 0o022 == 0`).
pub fn validate_control_runtime_executable_path(
    executable: &Path,
) -> Result<PathBuf, ControlRuntimeError> {
    let (canonical, _) = validate_executable_with_identity(executable, None)?;
    Ok(canonical)
}

fn validate_executable_with_identity(
    executable: &Path,
    expected_identity: Option<FileIdentity>,
) -> Result<(PathBuf, FileIdentity), ControlRuntimeError> {
    ensure_linux_supported()?;

    if executable.as_os_str().is_empty() || !executable.is_absolute() {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::InvalidConfig,
            format!(
                "control runtime executable path `{}` must be a non-empty absolute path",
                executable.display()
            ),
        ));
    }

    for comp in executable.components() {
        if matches!(comp, Component::ParentDir | Component::CurDir) {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime executable path `{}` must not contain `.` or `..` traversal components",
                    executable.display()
                ),
            ));
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let euid = current_euid();
        let components: Vec<Component<'_>> = executable.components().collect();
        let mut current = PathBuf::new();
        let mut prev_was_sticky_writable = false;

        for (idx, comp) in components.iter().enumerate() {
            let is_leaf = idx + 1 == components.len();
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => {
                    current.push(comp.as_os_str());
                    let root_meta = fs::symlink_metadata(&current).map_err(|err| {
                        ControlRuntimeError::new(
                            ControlRuntimeErrorCode::IoError,
                            format!(
                                "failed to stat root directory `{}`: {err}",
                                current.display()
                            ),
                        )
                    })?;
                    prev_was_sticky_writable =
                        validate_trusted_ancestor_meta(&current, &root_meta, euid, false)?;
                }
                Component::Normal(part) => {
                    current.push(part);
                    let meta = fs::symlink_metadata(&current).map_err(|err| {
                        let code = if expected_identity.is_some()
                            && err.kind() == std::io::ErrorKind::NotFound
                        {
                            ControlRuntimeErrorCode::StateChanged
                        } else if err.kind() == std::io::ErrorKind::NotFound {
                            ControlRuntimeErrorCode::InvalidConfig
                        } else {
                            ControlRuntimeErrorCode::IoError
                        };
                        ControlRuntimeError::new(
                            code,
                            format!(
                                "failed to stat control runtime executable path `{}` at `{}`: {err}",
                                executable.display(),
                                current.display()
                            ),
                        )
                    })?;
                    if meta.file_type().is_symlink() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "control runtime executable path `{}` must not traverse or be a symlink (`{}`)",
                                executable.display(),
                                current.display()
                            ),
                        ));
                    }
                    if !is_leaf {
                        prev_was_sticky_writable = validate_trusted_ancestor_meta(
                            &current,
                            &meta,
                            euid,
                            prev_was_sticky_writable,
                        )?;
                    }
                }
                Component::CurDir | Component::ParentDir => unreachable!(),
            }
        }

        let meta = fs::symlink_metadata(executable).map_err(|err| {
            let code = if expected_identity.is_some() && err.kind() == std::io::ErrorKind::NotFound
            {
                ControlRuntimeErrorCode::StateChanged
            } else if err.kind() == std::io::ErrorKind::NotFound {
                ControlRuntimeErrorCode::InvalidConfig
            } else {
                ControlRuntimeErrorCode::IoError
            };
            ControlRuntimeError::new(
                code,
                format!(
                    "failed to stat control runtime executable `{}`: {err}",
                    executable.display()
                ),
            )
        })?;

        if meta.file_type().is_symlink() {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime executable `{}` must not be a symlink",
                    executable.display()
                ),
            ));
        }
        if !meta.file_type().is_file() {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidConfig,
                format!(
                    "control runtime executable `{}` must be a regular file",
                    executable.display()
                ),
            ));
        }

        if meta.uid() != euid && meta.uid() != 0 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime executable `{}` is owned by uid {} (expected {} or 0)",
                    executable.display(),
                    meta.uid(),
                    euid
                ),
            ));
        }

        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o111 == 0 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidConfig,
                format!(
                    "control runtime executable `{}` is not executable (mode {:04o})",
                    executable.display(),
                    mode
                ),
            ));
        }
        if mode & 0o022 != 0 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime executable `{}` must not be group- or world-writable (mode {:04o})",
                    executable.display(),
                    mode
                ),
            ));
        }

        let identity = FileIdentity {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        if let Some(expected) = expected_identity {
            if identity != expected {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StateChanged,
                    format!(
                        "control runtime executable `{}` identity changed unexpectedly during operation",
                        executable.display()
                    ),
                ));
            }
        }

        let canonical = fs::canonicalize(executable).map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!(
                    "failed to canonicalize control runtime executable `{}`: {err}",
                    executable.display()
                ),
            )
        })?;
        if canonical != executable {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime executable `{}` resolved through a symlink to `{}`",
                    executable.display(),
                    canonical.display()
                ),
            ));
        }
        Ok((canonical, identity))
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = expected_identity;
        unreachable!()
    }
}

/// Validates and prepares the private runtime `state_dir` (`0700`) for [`ensure_control_runtime`].
///
/// Enforces:
/// 1. Linux platform support (`UnsupportedPlatform` on other platforms).
/// 2. Non-empty absolute path with no `.` or `..` components and no symlink components.
/// 3. Every ancestor directory must be owned by the current effective UID or `root` (`uid == 0`)
///    and must not be group- or world-writable (`mode & 0o022 == 0`), except for standard sticky
///    directories (`mode & 0o1000 != 0`, such as `/tmp`) when the immediate child inside the
///    sticky directory already exists, is owned by the current effective UID, and is not
///    group- or world-writable.
/// 4. If `state_dir` already exists (or is concurrently created during a benign `AlreadyExists`
///    first-use race), it must be a non-symlink directory owned by the current effective UID with
///    exact `0700` permissions; if non-empty, it must contain at least one Threadlane runtime
///    state entry (`supervisor.lock`, `state.db`, `supervisor.json`, `state.db-wal`,
///    `state.db-shm`) and no symlinks.
/// 5. If `state_dir` does not exist yet (under a non-writable private parent), it is created with
///    mode `0700`.
/// 6. Any pre-existing `supervisor.lock`, `state.db`, `state.db-wal`, `state.db-shm`, or
///    `supervisor.json` must be a non-symlink regular file owned by the current effective UID
///    with private permissions (`0600` for `supervisor.json`, `mode & 0o077 == 0` for lock/db).
pub fn validate_control_runtime_state_dir(
    state_dir: &Path,
) -> Result<PathBuf, ControlRuntimeError> {
    ensure_linux_supported()?;

    if state_dir.as_os_str().is_empty() || !state_dir.is_absolute() {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::InvalidConfig,
            format!(
                "control runtime state_dir `{}` must be a non-empty absolute path",
                state_dir.display()
            ),
        ));
    }

    for comp in state_dir.components() {
        if matches!(comp, Component::ParentDir | Component::CurDir) {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime state_dir `{}` must not contain `.` or `..` traversal components",
                    state_dir.display()
                ),
            ));
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};

        let euid = current_euid();
        let components: Vec<Component<'_>> = state_dir.components().collect();
        let mut current = PathBuf::new();
        let mut prev_was_sticky_writable = false;

        for (idx, comp) in components.iter().enumerate() {
            let is_leaf = idx + 1 == components.len();
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => {
                    current.push(comp.as_os_str());
                    let root_meta = fs::symlink_metadata(&current).map_err(|err| {
                        ControlRuntimeError::new(
                            ControlRuntimeErrorCode::IoError,
                            format!(
                                "failed to stat root directory `{}`: {err}",
                                current.display()
                            ),
                        )
                    })?;
                    prev_was_sticky_writable =
                        validate_trusted_ancestor_meta(&current, &root_meta, euid, false)?;
                }
                Component::Normal(part) => {
                    current.push(part);
                    let meta = match fs::symlink_metadata(&current) {
                        Ok(meta) => meta,
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            if prev_was_sticky_writable {
                                return Err(ControlRuntimeError::new(
                                    ControlRuntimeErrorCode::SecurityViolation,
                                    format!(
                                        "refusing to create missing directory `{}` directly inside sticky world/group-writable parent; parent must be a pre-existing private directory owned by uid {}",
                                        current.display(),
                                        euid
                                    ),
                                ));
                            }
                            let mut builder = fs::DirBuilder::new();
                            builder.mode(0o700);
                            match builder.create(&current) {
                                Ok(()) => {}
                                // Benign concurrent first-use race: another caller created the
                                // directory between `symlink_metadata` and `DirBuilder::create`.
                                // Fall through to re-stat and strictly validate ownership/mode.
                                Err(create_err)
                                    if create_err.kind() == std::io::ErrorKind::AlreadyExists => {}
                                Err(create_err) => {
                                    return Err(ControlRuntimeError::new(
                                        ControlRuntimeErrorCode::IoError,
                                        format!(
                                            "failed to create control runtime directory `{}`: {create_err}",
                                            current.display()
                                        ),
                                    ));
                                }
                            }
                            fs::symlink_metadata(&current).map_err(|stat_err| {
                                ControlRuntimeError::new(
                                    ControlRuntimeErrorCode::IoError,
                                    format!(
                                        "failed to stat control runtime directory `{}` after creation: {stat_err}",
                                        current.display()
                                    ),
                                )
                            })?
                        }
                        Err(err) => {
                            return Err(ControlRuntimeError::new(
                                ControlRuntimeErrorCode::IoError,
                                format!(
                                    "failed to stat control runtime directory component `{}`: {err}",
                                    current.display()
                                ),
                            ));
                        }
                    };

                    if meta.file_type().is_symlink() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "control runtime state_dir `{}` traverses symlink `{}`",
                                state_dir.display(),
                                current.display()
                            ),
                        ));
                    }
                    if !meta.file_type().is_dir() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "control runtime state_dir component `{}` is not a directory",
                                current.display()
                            ),
                        ));
                    }

                    if is_leaf {
                        if meta.uid() != euid {
                            return Err(ControlRuntimeError::new(
                                ControlRuntimeErrorCode::SecurityViolation,
                                format!(
                                    "control runtime state_dir `{}` is owned by uid {} (expected {})",
                                    state_dir.display(),
                                    meta.uid(),
                                    euid
                                ),
                            ));
                        }
                        let mode = meta.permissions().mode() & 0o777;
                        if mode != 0o700 {
                            return Err(ControlRuntimeError::new(
                                ControlRuntimeErrorCode::SecurityViolation,
                                format!(
                                    "control runtime state_dir `{}` has insecure permissions {:04o} (expected 0700)",
                                    state_dir.display(),
                                    mode
                                ),
                            ));
                        }
                    } else {
                        prev_was_sticky_writable = validate_trusted_ancestor_meta(
                            &current,
                            &meta,
                            euid,
                            prev_was_sticky_writable,
                        )?;
                    }
                }
                Component::CurDir | Component::ParentDir => unreachable!(),
            }
        }

        validate_state_dir_contents(state_dir)?;

        let canonical = fs::canonicalize(state_dir).map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!(
                    "failed to canonicalize control runtime state_dir `{}`: {err}",
                    state_dir.display()
                ),
            )
        })?;
        if canonical != state_dir {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime state_dir `{}` resolved through a symlink to `{}`",
                    state_dir.display(),
                    canonical.display()
                ),
            ));
        }

        Ok(canonical)
    }

    #[cfg(not(target_os = "linux"))]
    {
        unreachable!()
    }
}

#[cfg(target_os = "linux")]
fn validate_trusted_ancestor_meta(
    path: &Path,
    meta: &fs::Metadata,
    euid: u32,
    parent_was_sticky_writable: bool,
) -> Result<bool, ControlRuntimeError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    if meta.file_type().is_symlink() {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::SecurityViolation,
            format!(
                "ancestor directory `{}` must not be a symlink",
                path.display()
            ),
        ));
    }
    if !meta.file_type().is_dir() {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::SecurityViolation,
            format!("ancestor component `{}` is not a directory", path.display()),
        ));
    }

    let uid = meta.uid();
    if uid != euid && uid != 0 {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::SecurityViolation,
            format!(
                "ancestor directory `{}` is owned by untrusted uid {} (expected {} or 0)",
                path.display(),
                uid,
                euid
            ),
        ));
    }

    let mode = meta.permissions().mode() & 0o7777;
    let is_group_or_world_writable = (mode & 0o022) != 0;
    let has_sticky_bit = (mode & 0o1000) != 0;

    if parent_was_sticky_writable {
        // An immediate child directory inside a sticky shared parent (like `/tmp/.tmpXXXX`) must be
        // owned by the current user (or root) and must NOT itself be group- or world-writable.
        if is_group_or_world_writable {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "directory `{}` inside sticky parent has insecure writable permissions {:04o}",
                    path.display(),
                    mode & 0o777
                ),
            ));
        }
    }

    if is_group_or_world_writable {
        if !has_sticky_bit {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "ancestor directory `{}` has untrusted group/world-writable permissions {:04o} without sticky bit",
                    path.display(),
                    mode
                ),
            ));
        }
        return Ok(true);
    }

    Ok(false)
}

/// Builds the exact command-line arguments for launching the dedicated control-plane supervisor:
/// `["serve", "--state-dir", "<state_dir>"]`.
///
/// Notifications (`--codex`, `--notify-binding`), worker profiles (`--workers-config`), and
/// approved-host flags are unconditionally omitted.
#[must_use]
pub fn build_control_runtime_serve_args(state_dir: &Path) -> Vec<OsString> {
    vec![
        OsString::from("serve"),
        OsString::from("--state-dir"),
        state_dir.as_os_str().to_os_string(),
    ]
}

/// Ensures a single dedicated Threadlane control-plane supervisor is running for `state_dir`.
///
/// - Reuses an existing healthy authenticated supervisor (`already_running`) without spawning
///   another process; reports `notifications_enabled: None` and `workers_enabled: None` because
///   `GET /v1/bindings` does not verify how a pre-existing process was configured.
/// - Launches only `<threadlane_executable> serve --state-dir <state_dir>` on Linux with
///   `process_group(0)`, `kill_on_drop(false)`, `Stdio::null()`, and a cleared environment
///   (`env_clear()`) so ambient provider credentials are never inherited (`started`, reporting
///   `notifications_enabled: Some(false)` and `workers_enabled: Some(false)`).
/// - Recovers from a provably stale `supervisor.json` (dead PID and unlocked `supervisor.lock`)
///   exclusively through normal locked `Supervisor` startup without deleting `supervisor.json`,
///   `state.db`, or `supervisor.lock`.
/// - Fails closed on unsupported platforms, untrusted writable ancestors, invalid/insecure
///   permissions or symlinks, malformed descriptors, `401 Unauthorized`, ambiguous live processes,
///   occupied locks that do not become ready within `timeout`, changed directory/executable/descriptor
///   identity, or startup failure/timeout.
pub async fn ensure_control_runtime(
    state_dir: impl AsRef<Path>,
    threadlane_executable: impl AsRef<Path>,
    timeout: Duration,
) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
    let policy = ControlRuntimePolicy::new(state_dir, threadlane_executable, timeout)?;
    let deadline = Instant::now() + policy.timeout;

    let in_process_mutex = state_dir_coordinator_mutex(&policy.state_dir);
    let _in_process_guard = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        in_process_mutex.lock(),
    )
    .await
    {
        Ok(guard) => guard,
        Err(_) => {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::LockBusy,
                format!(
                    "timed out after {:?} waiting for concurrent in-process control runtime ensure on `{}`",
                    policy.timeout,
                    policy.state_dir.display()
                ),
            ));
        }
    };

    ensure_control_runtime_locked(&policy, deadline).await
}

fn state_dir_coordinator_mutex(canonical_state_dir: &Path) -> Arc<tokio::sync::Mutex<()>> {
    static COORDINATORS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> =
        OnceLock::new();
    let map = COORDINATORS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = map.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(
        guard
            .entry(canonical_state_dir.to_path_buf())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LockHolderInfo {
    pid: u32,
}

#[derive(Debug, Clone)]
struct DescriptorSnapshot {
    descriptor: RuntimeDescriptor,
    identity: FileIdentity,
}

impl DescriptorSnapshot {
    fn same_instance(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.descriptor.pid == other.descriptor.pid
            && self.descriptor.port == other.descriptor.port
            && self
                .descriptor
                .token
                .constant_time_eq(other.descriptor.token.as_str())
    }
}

#[derive(Debug, Clone)]
struct RuntimeStateInspection {
    lock_holder: Option<LockHolderInfo>,
    descriptor: Option<DescriptorSnapshot>,
}

enum ProbeFailure {
    Unauthorized,
    TimedOut,
    Transport(String),
}

async fn ensure_control_runtime_locked(
    policy: &ControlRuntimePolicy,
    deadline: Instant,
) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
    let state_dir = &policy.state_dir;
    let initial_dir_identity = verify_state_dir_identity(state_dir, None)?;
    let (_, initial_exe_identity) =
        validate_executable_with_identity(&policy.threadlane_executable, None)?;

    let initial_state = inspect_runtime_state(state_dir, initial_dir_identity)?;

    match initial_state.descriptor {
        Some(ref snap) if is_process_alive(snap.descriptor.pid) => {
            return handle_existing_live_descriptor(
                state_dir,
                initial_dir_identity,
                snap,
                initial_state.lock_holder,
                deadline,
            )
            .await;
        }
        Some(ref stale_snap) => {
            if initial_state.lock_holder.is_some() {
                return wait_for_running_winner(
                    state_dir,
                    initial_dir_identity,
                    Some(stale_snap),
                    deadline,
                )
                .await;
            }
            spawn_and_await_control_runtime(
                policy,
                initial_dir_identity,
                initial_exe_identity,
                Some(stale_snap.clone()),
                deadline,
            )
            .await
        }
        None => {
            if initial_state.lock_holder.is_some() {
                return wait_for_running_winner(state_dir, initial_dir_identity, None, deadline)
                    .await;
            }
            spawn_and_await_control_runtime(
                policy,
                initial_dir_identity,
                initial_exe_identity,
                None,
                deadline,
            )
            .await
        }
    }
}

async fn handle_existing_live_descriptor(
    state_dir: &Path,
    initial_dir_identity: FileIdentity,
    snap: &DescriptorSnapshot,
    lock_holder: Option<LockHolderInfo>,
    deadline: Instant,
) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
    let Some(holder) = lock_holder else {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
            format!(
                "refusing takeover of `{}`: supervisor.json references live pid {} on port {}, but `{}` is not held",
                state_dir.display(),
                snap.descriptor.pid,
                snap.descriptor.port,
                LOCK_FILENAME
            ),
        ));
    };

    if holder.pid != snap.descriptor.pid {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
            format!(
                "refusing takeover of `{}`: supervisor.json references live pid {}, but `{}` is held by pid {}",
                state_dir.display(),
                snap.descriptor.pid,
                LOCK_FILENAME,
                holder.pid
            ),
        ));
    }

    match probe_authenticated_supervisor(&snap.descriptor, deadline).await {
        Ok(bindings_count) => {
            let post_state = inspect_runtime_state(state_dir, initial_dir_identity)?;
            let Some(post_snap) = post_state.descriptor else {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StateChanged,
                    format!(
                        "supervisor.json in `{}` disappeared during authenticated readiness verification",
                        state_dir.display()
                    ),
                ));
            };
            if !post_snap.same_instance(snap)
                || post_state.lock_holder != Some(LockHolderInfo { pid: snap.descriptor.pid })
                || !is_process_alive(snap.descriptor.pid)
            {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StateChanged,
                    format!(
                        "supervisor state in `{}` changed during authenticated readiness verification",
                        state_dir.display()
                    ),
                ));
            }
            Ok(ControlRuntimeEnsureOutcome::already_running(
                state_dir.to_path_buf(),
                snap.descriptor.pid,
                snap.descriptor.port,
                bindings_count,
                false,
            ))
        }
        Err(ProbeFailure::Unauthorized) => Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::UnauthorizedSupervisor,
            format!(
                "live supervisor (pid {}, port {}) in `{}` rejected bearer token authentication (401 Unauthorized); refusing takeover",
                snap.descriptor.pid,
                snap.descriptor.port,
                state_dir.display()
            ),
        )),
        Err(ProbeFailure::TimedOut) => {
            let _ = verify_state_dir_identity(state_dir, Some(initial_dir_identity))?;
            Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
                format!(
                    "live supervisor (pid {}, port {}) in `{}` timed out during authenticated readiness verification; refusing takeover",
                    snap.descriptor.pid,
                    snap.descriptor.port,
                    state_dir.display()
                ),
            ))
        }
        Err(ProbeFailure::Transport(reason)) => {
            let _ = verify_state_dir_identity(state_dir, Some(initial_dir_identity))?;
            if is_process_alive(snap.descriptor.pid) {
                Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
                    format!(
                        "live supervisor (pid {}, port {}) in `{}` failed authenticated readiness verification ({reason}); refusing takeover",
                        snap.descriptor.pid,
                        snap.descriptor.port,
                        state_dir.display()
                    ),
                ))
            } else {
                Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StateChanged,
                    format!(
                        "supervisor (pid {}, port {}) in `{}` exited during authenticated readiness verification ({reason}); refusing automatic takeover",
                        snap.descriptor.pid,
                        snap.descriptor.port,
                        state_dir.display()
                    ),
                ))
            }
        }
    }
}

async fn wait_for_running_winner(
    state_dir: &Path,
    initial_dir_identity: FileIdentity,
    stale_before: Option<&DescriptorSnapshot>,
    deadline: Instant,
) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
    loop {
        let state = inspect_runtime_state(state_dir, initial_dir_identity)?;
        let Some(holder) = state.lock_holder else {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::LockBusy,
                format!(
                    "concurrent holder of `{}/{}` exited without publishing a verified running supervisor; refusing blind takeover",
                    state_dir.display(),
                    LOCK_FILENAME
                ),
            ));
        };

        if let Some(ref snap) = state.descriptor {
            let is_stale_unreplaced = stale_before.is_some_and(|stale| snap.same_instance(stale));
            if !is_stale_unreplaced && is_process_alive(snap.descriptor.pid) {
                if holder.pid != snap.descriptor.pid {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
                        format!(
                            "concurrent startup in `{}` published supervisor.json for live pid {} while `{}` is held by pid {}",
                            state_dir.display(),
                            snap.descriptor.pid,
                            LOCK_FILENAME,
                            holder.pid
                        ),
                    ));
                }
                match probe_authenticated_supervisor(&snap.descriptor, deadline).await {
                    Ok(bindings_count) => {
                        let post_state = inspect_runtime_state(state_dir, initial_dir_identity)?;
                        if let Some(post_snap) = post_state.descriptor {
                            if post_snap.same_instance(snap)
                                && post_state.lock_holder
                                    == Some(LockHolderInfo {
                                        pid: snap.descriptor.pid,
                                    })
                                && is_process_alive(snap.descriptor.pid)
                            {
                                return Ok(ControlRuntimeEnsureOutcome::already_running(
                                    state_dir.to_path_buf(),
                                    snap.descriptor.pid,
                                    snap.descriptor.port,
                                    bindings_count,
                                    stale_before.is_some(),
                                ));
                            }
                        }
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::StateChanged,
                            format!(
                                "concurrent winner in `{}` changed state during readiness verification",
                                state_dir.display()
                            ),
                        ));
                    }
                    Err(ProbeFailure::Unauthorized) => {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::UnauthorizedSupervisor,
                            format!(
                                "concurrent supervisor (pid {}, port {}) in `{}` rejected bearer token authentication (401 Unauthorized)",
                                snap.descriptor.pid,
                                snap.descriptor.port,
                                state_dir.display()
                            ),
                        ));
                    }
                    Err(ProbeFailure::TimedOut | ProbeFailure::Transport(_)) => {}
                }
            }
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::LockBusy,
                format!(
                    "`{}/{}` is held by pid {}, which did not publish a verified ready supervisor before timeout",
                    state_dir.display(),
                    LOCK_FILENAME,
                    holder.pid
                ),
            ));
        }

        let sleep_dur = CONTROL_RUNTIME_POLL_INTERVAL.min(deadline.saturating_duration_since(now));
        tokio::time::sleep(sleep_dur).await;
    }
}

#[derive(Debug, Clone)]
struct CleanupOutcome {
    uncertainty_note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcStatIdentity {
    state: char,
    pgrp: i32,
    start_ticks: u64,
}

/// Cancellation-safe guard for a child process spawned by [`spawn_and_await_control_runtime`].
///
/// Retains `/proc/<pid>/stat` `starttime` (`child_start_ticks`) and tracks whether the leader
/// child has been reaped (`leader_reaped`) so cleanup never signals a recycled PID or unrelated
/// process group. `self.disarmed` remains `false` throughout `terminate_and_reap` until cleanup
/// completes, so dropping the caller future mid-cleanup still triggers `Drop` cleanup.
struct SpawnedChildGuard {
    child: Option<Child>,
    child_pid: u32,
    child_start_ticks: Option<u64>,
    owned_members: BTreeMap<u32, u64>,
    leader_reaped: bool,
    disarmed: bool,
}

impl SpawnedChildGuard {
    fn new(child: Child, child_pid: u32) -> Self {
        let child_start_ticks = read_proc_stat_identity(child_pid).map(|id| id.start_ticks);
        Self {
            child: Some(child),
            child_pid,
            child_start_ticks,
            owned_members: BTreeMap::new(),
            leader_reaped: false,
            disarmed: false,
        }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        // Capture identities before wait can reap the group leader. A group ID can be reused
        // after its last member exits, so discovering new members after reap is unsafe.
        self.capture_owned_members();
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.leader_reaped = true;
                self.child = None;
                Ok(Some(status))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn pid_belongs_to_spawned(&self, pid: u32) -> bool {
        let Some(pgid) = i32::try_from(self.child_pid).ok().filter(|&g| g > 1) else {
            return false;
        };
        if !proc_entry_owned_by_current_euid(pid) {
            return false;
        }
        let Some(id) = read_proc_stat_identity(pid) else {
            return false;
        };
        if pid == self.child_pid {
            if self.leader_reaped {
                return false;
            }
            id.pgrp == pgid && self.child_start_ticks == Some(id.start_ticks)
        } else {
            id.pgrp == pgid && self.owned_members.get(&pid) == Some(&id.start_ticks)
        }
    }

    fn leader_is_verified_live(&self) -> bool {
        if self.leader_reaped || !proc_entry_owned_by_current_euid(self.child_pid) {
            return false;
        }
        let Some(pgid) = i32::try_from(self.child_pid).ok().filter(|&g| g > 1) else {
            return false;
        };
        let Some(id) = read_proc_stat_identity(self.child_pid) else {
            return false;
        };
        !matches!(id.state, 'Z' | 'X' | 'x')
            && id.pgrp == pgid
            && self.child_start_ticks == Some(id.start_ticks)
    }

    fn capture_owned_members(&mut self) {
        if self.leader_reaped {
            return;
        }
        let Some(pgid) = i32::try_from(self.child_pid).ok().filter(|&g| g > 1) else {
            return;
        };
        let Some(leader_start) = self.child_start_ticks else {
            return;
        };
        let Some(leader) = read_proc_stat_identity(self.child_pid) else {
            return;
        };
        if leader.start_ticks != leader_start
            || leader.pgrp != pgid
            || !proc_entry_owned_by_current_euid(self.child_pid)
        {
            return;
        };
        let Ok(entries) = fs::read_dir("/proc") else {
            return;
        };
        let caller_pid = std::process::id();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if !name_str.bytes().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let Ok(pid) = name_str.parse::<u32>() else {
                continue;
            };
            if pid <= 1 || pid == caller_pid {
                continue;
            }
            if !proc_entry_owned_by_current_euid(pid) {
                continue;
            }
            let Some(id) = read_proc_stat_identity(pid) else {
                continue;
            };
            if matches!(id.state, 'Z' | 'X' | 'x') || id.pgrp != pgid {
                continue;
            }
            let ticks_match = if pid == self.child_pid {
                id.start_ticks == leader_start
            } else {
                id.start_ticks >= leader_start
            };
            if ticks_match {
                self.owned_members.entry(pid).or_insert(id.start_ticks);
            }
        }
    }

    fn verified_group_member_pids(&self) -> Vec<(u32, u64)> {
        let Some(pgid) = i32::try_from(self.child_pid).ok().filter(|&g| g > 1) else {
            return Vec::new();
        };
        self.owned_members
            .iter()
            .filter_map(|(&pid, &start)| {
                let id = read_proc_stat_identity(pid)?;
                (id.pgrp == pgid
                    && id.start_ticks == start
                    && !matches!(id.state, 'Z' | 'X' | 'x')
                    && proc_entry_owned_by_current_euid(pid))
                .then_some((pid, start))
            })
            .collect()
    }

    fn signal_owned_group(&self, signal: libc::c_int) {
        let Some(pgid) = i32::try_from(self.child_pid).ok().filter(|&g| g > 1) else {
            return;
        };
        // Only signal `-pgid` directly while the group leader `self.child_pid` has NOT been reaped
        // and still matches its original `/proc/<pid>/stat` `starttime`. Once the leader has
        // exited or been reaped, signal only identities observed while the original leader
        // still occupied its PID, and recheck each member's start time before signaling.
        if !self.leader_reaped && self.leader_is_verified_live() {
            signal_process_group(pgid, signal);
            return;
        }
        for (member_pid, expected_ticks) in self.verified_group_member_pids() {
            if let Some(recheck) = read_proc_stat_identity(member_pid) {
                if !matches!(recheck.state, 'Z' | 'X' | 'x')
                    && recheck.pgrp == pgid
                    && recheck.start_ticks == expected_ticks
                    && proc_entry_owned_by_current_euid(member_pid)
                {
                    signal_single_pid(member_pid, signal);
                }
            }
        }
    }

    fn disarm_and_reap_in_background(&mut self) {
        self.disarmed = true;
        if let Some(mut child) = self.child.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }

    async fn terminate_and_reap(&mut self) -> CleanupOutcome {
        // NOTE: Do NOT set `self.disarmed = true` before `.await` points; if the caller future is
        // cancelled mid-cleanup, `Drop` must still run to escalate to SIGKILL and reap `self.child`.
        self.capture_owned_members();
        self.signal_owned_group(libc::SIGTERM);

        for _ in 0..30 {
            let _ = self.try_wait();
            if self.leader_reaped && self.verified_group_member_pids().is_empty() {
                self.disarmed = true;
                return CleanupOutcome {
                    uncertainty_note: None,
                };
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        self.signal_owned_group(libc::SIGKILL);

        if let Some(mut child) = self.child.take() {
            if !self.leader_reaped {
                let _ = child.start_kill();
            }
            match tokio::time::timeout(Duration::from_millis(250), child.wait()).await {
                Ok(Ok(_)) => {
                    self.leader_reaped = true;
                }
                Ok(Err(_)) | Err(_) => {
                    if let Ok(Some(_)) = child.try_wait() {
                        self.leader_reaped = true;
                    } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
                        handle.spawn(async move {
                            let _ = child.wait().await;
                        });
                    }
                }
            }
        }

        for _ in 0..20 {
            if self.verified_group_member_pids().is_empty() {
                break;
            }
            self.signal_owned_group(libc::SIGKILL);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        self.disarmed = true;
        let remaining = self.verified_group_member_pids().len();
        let uncertainty_note = if self.child_start_ticks.is_none()
            || !self.leader_reaped
            || remaining > 0
        {
            Some(format!(
                "cleanup uncertainty: leader_reaped={}, remaining_verified_group_members={remaining}",
                self.leader_reaped
            ))
        } else {
            None
        };
        CleanupOutcome { uncertainty_note }
    }
}

impl Drop for SpawnedChildGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        self.disarmed = true;
        self.capture_owned_members();
        self.signal_owned_group(libc::SIGKILL);
        if let Some(mut child) = self.child.take() {
            if !self.leader_reaped {
                let _ = child.start_kill();
            }
            if let Ok(Some(_)) = child.try_wait() {
                self.leader_reaped = true;
            } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

async fn spawn_and_await_control_runtime(
    policy: &ControlRuntimePolicy,
    initial_dir_identity: FileIdentity,
    initial_exe_identity: FileIdentity,
    stale_before: Option<DescriptorSnapshot>,
    deadline: Instant,
) -> Result<ControlRuntimeEnsureOutcome, ControlRuntimeError> {
    if Instant::now() >= deadline {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::StartupTimedOut,
            format!(
                "control runtime startup deadline expired before spawning `threadlane serve` for `{}`",
                policy.state_dir.display()
            ),
        ));
    }

    let _ = verify_state_dir_identity(&policy.state_dir, Some(initial_dir_identity))?;
    let _ = validate_executable_with_identity(
        &policy.threadlane_executable,
        Some(initial_exe_identity),
    )?;

    let serve_args = policy.serve_args();
    let mut cmd = Command::new(&policy.threadlane_executable);
    // Clear the environment completely so ambient provider API keys, tokens, proxy variables, or
    // path overrides are never inherited by the dedicated control-plane supervisor.
    cmd.env_clear()
        .args(&serve_args)
        .current_dir(&policy.state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false);

    #[cfg(target_os = "linux")]
    {
        cmd.process_group(0);
    }

    let child = cmd.spawn().map_err(|err| {
        ControlRuntimeError::new(
            ControlRuntimeErrorCode::SpawnFailed,
            format!(
                "failed to spawn `{}` with `serve --state-dir {}`: {err}",
                policy.threadlane_executable.display(),
                policy.state_dir.display()
            ),
        )
    })?;

    let Some(child_pid) = child.id() else {
        return Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::SpawnFailed,
            format!(
                "spawned `{}` process had no OS process identifier",
                policy.threadlane_executable.display()
            ),
        ));
    };

    let mut guard = SpawnedChildGuard::new(child, child_pid);

    // Re-verify executable and state_dir identities immediately after spawn to detect replacement races.
    if let Err(err) =
        validate_executable_with_identity(&policy.threadlane_executable, Some(initial_exe_identity))
    {
        let cleanup = guard.terminate_and_reap().await;
        return Err(err.with_cleanup_outcome(&cleanup));
    }

    loop {
        guard.capture_owned_members();
        let state = match inspect_runtime_state(&policy.state_dir, initial_dir_identity) {
            Ok(state) => state,
            Err(err) => {
                let cleanup = guard.terminate_and_reap().await;
                return Err(err.with_cleanup_outcome(&cleanup));
            }
        };

        if let Some(holder) = state.lock_holder {
            if !guard.pid_belongs_to_spawned(holder.pid) {
                let _cleanup = guard.terminate_and_reap().await;
                return wait_for_running_winner(
                    &policy.state_dir,
                    initial_dir_identity,
                    stale_before.as_ref(),
                    deadline,
                )
                .await;
            }
        }

        if let Some(ref snap) = state.descriptor {
            let is_stale_unreplaced = stale_before
                .as_ref()
                .is_some_and(|stale| snap.same_instance(stale));
            if !is_stale_unreplaced {
                if guard.pid_belongs_to_spawned(snap.descriptor.pid) {
                    if !is_process_alive(snap.descriptor.pid) {
                        let cleanup = guard.terminate_and_reap().await;
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::StartupProcessExited,
                            format!(
                                "spawned supervisor (pid {}) in `{}` wrote supervisor.json and exited before readiness verification",
                                snap.descriptor.pid,
                                policy.state_dir.display()
                            ),
                        )
                        .with_cleanup_outcome(&cleanup));
                    }
                    if state.lock_holder
                        != Some(LockHolderInfo {
                            pid: snap.descriptor.pid,
                        })
                    {
                        let cleanup = guard.terminate_and_reap().await;
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::AmbiguousLiveSupervisor,
                            format!(
                                "spawned process (pid {}) in `{}` wrote supervisor.json without holding `{}`",
                                snap.descriptor.pid,
                                policy.state_dir.display(),
                                LOCK_FILENAME
                            ),
                        )
                        .with_cleanup_outcome(&cleanup));
                    }

                    match probe_authenticated_supervisor(&snap.descriptor, deadline).await {
                        Ok(bindings_count) => {
                            let post_state = match inspect_runtime_state(
                                &policy.state_dir,
                                initial_dir_identity,
                            ) {
                                Ok(s) => s,
                                Err(err) => {
                                    let cleanup = guard.terminate_and_reap().await;
                                    return Err(err.with_cleanup_outcome(&cleanup));
                                }
                            };
                            let Some(post_snap) = post_state.descriptor else {
                                let cleanup = guard.terminate_and_reap().await;
                                return Err(ControlRuntimeError::new(
                                    ControlRuntimeErrorCode::StateChanged,
                                    format!(
                                        "supervisor.json in `{}` disappeared immediately after startup readiness probe",
                                        policy.state_dir.display()
                                    ),
                                )
                                .with_cleanup_outcome(&cleanup));
                            };
                            if !post_snap.same_instance(snap)
                                || post_state.lock_holder
                                    != Some(LockHolderInfo {
                                        pid: snap.descriptor.pid,
                                    })
                                || !is_process_alive(snap.descriptor.pid)
                            {
                                let cleanup = guard.terminate_and_reap().await;
                                return Err(ControlRuntimeError::new(
                                    ControlRuntimeErrorCode::StateChanged,
                                    format!(
                                        "supervisor state in `{}` changed immediately after startup readiness probe",
                                        policy.state_dir.display()
                                    ),
                                )
                                .with_cleanup_outcome(&cleanup));
                            }

                            guard.disarm_and_reap_in_background();
                            return Ok(ControlRuntimeEnsureOutcome::started(
                                policy.state_dir.clone(),
                                snap.descriptor.pid,
                                snap.descriptor.port,
                                bindings_count,
                                stale_before.is_some(),
                            ));
                        }
                        Err(ProbeFailure::Unauthorized) => {
                            let cleanup = guard.terminate_and_reap().await;
                            return Err(ControlRuntimeError::new(
                                ControlRuntimeErrorCode::UnauthorizedSupervisor,
                                format!(
                                    "spawned supervisor (pid {}, port {}) in `{}` rejected bearer token authentication (401 Unauthorized)",
                                    snap.descriptor.pid,
                                    snap.descriptor.port,
                                    policy.state_dir.display()
                                ),
                            )
                            .with_cleanup_outcome(&cleanup));
                        }
                        Err(ProbeFailure::TimedOut | ProbeFailure::Transport(_)) => {}
                    }
                } else {
                    let cleanup = guard.terminate_and_reap().await;
                    let code = if is_process_alive(snap.descriptor.pid) {
                        ControlRuntimeErrorCode::AmbiguousLiveSupervisor
                    } else {
                        ControlRuntimeErrorCode::StateChanged
                    };
                    return Err(ControlRuntimeError::new(
                        code,
                        format!(
                            "unexpected unlocked supervisor.json for pid {} appeared in `{}` while awaiting spawned pid {}",
                            snap.descriptor.pid,
                            policy.state_dir.display(),
                            child_pid
                        ),
                    )
                    .with_cleanup_outcome(&cleanup));
                }
            }
        }

        match guard.try_wait() {
            Ok(Some(status)) => {
                let post_exit_state =
                    inspect_runtime_state(&policy.state_dir, initial_dir_identity).ok();
                let external_winner_holds_lock = post_exit_state
                    .as_ref()
                    .and_then(|s| s.lock_holder)
                    .is_some_and(|holder| !guard.pid_belongs_to_spawned(holder.pid));

                let cleanup = guard.terminate_and_reap().await;

                if external_winner_holds_lock {
                    return wait_for_running_winner(
                        &policy.state_dir,
                        initial_dir_identity,
                        stale_before.as_ref(),
                        deadline,
                    )
                    .await;
                }

                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StartupProcessExited,
                    format!(
                        "spawned `threadlane serve` process (pid {child_pid}) for `{}` exited prematurely with {status} before becoming ready",
                        policy.state_dir.display()
                    ),
                )
                .with_cleanup_outcome(&cleanup));
            }
            Ok(None) => {}
            Err(err) => {
                let cleanup = guard.terminate_and_reap().await;
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!("failed polling spawned `threadlane serve` process {child_pid}: {err}"),
                )
                .with_cleanup_outcome(&cleanup));
            }
        }

        let now = Instant::now();
        if now >= deadline {
            let cleanup = guard.terminate_and_reap().await;
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::StartupTimedOut,
                format!(
                    "timed out after {:?} waiting for spawned `threadlane serve` (pid {child_pid}) in `{}` to publish a verified ready endpoint",
                    policy.timeout,
                    policy.state_dir.display()
                ),
            )
            .with_cleanup_outcome(&cleanup));
        }

        let sleep_dur = CONTROL_RUNTIME_POLL_INTERVAL.min(deadline.saturating_duration_since(now));
        tokio::time::sleep(sleep_dur).await;
    }
}

async fn probe_authenticated_supervisor(
    descriptor: &RuntimeDescriptor,
    deadline: Instant,
) -> Result<usize, ProbeFailure> {
    let now = Instant::now();
    if now >= deadline {
        return Err(ProbeFailure::TimedOut);
    }
    let budget = deadline
        .saturating_duration_since(now)
        .min(MAX_SINGLE_PROBE_TIMEOUT);

    let client = SupervisorClient::from_descriptor(descriptor.clone())
        .map_err(|err| ProbeFailure::Transport(sanitize_diagnostic_text(&err.to_string())))?;

    match tokio::time::timeout(budget, client.list_bindings()).await {
        Ok(Ok(bindings)) => Ok(bindings.len()),
        Ok(Err(ThreadlaneError::Remote(api_err)))
            if api_err.code == ThreadlaneErrorCode::Unauthorized =>
        {
            Err(ProbeFailure::Unauthorized)
        }
        Ok(Err(err)) => Err(ProbeFailure::Transport(sanitize_diagnostic_text(
            &err.to_string(),
        ))),
        Err(_) => Err(ProbeFailure::TimedOut),
    }
}

fn inspect_runtime_state(
    state_dir: &Path,
    expected_dir_identity: FileIdentity,
) -> Result<RuntimeStateInspection, ControlRuntimeError> {
    let _ = verify_state_dir_identity(state_dir, Some(expected_dir_identity))?;
    validate_state_dir_contents(state_dir)?;
    let lock_holder = inspect_supervisor_lock_holder(state_dir)?;
    let descriptor = read_descriptor_snapshot_optional(state_dir, expected_dir_identity)?;
    Ok(RuntimeStateInspection {
        lock_holder,
        descriptor,
    })
}

fn verify_state_dir_identity(
    state_dir: &Path,
    expected: Option<FileIdentity>,
) -> Result<FileIdentity, ControlRuntimeError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        for comp in state_dir.components() {
            if matches!(comp, Component::ParentDir | Component::CurDir) {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::SecurityViolation,
                    format!(
                        "control runtime state_dir `{}` contains traversal components",
                        state_dir.display()
                    ),
                ));
            }
        }

        let euid = current_euid();
        let components: Vec<Component<'_>> = state_dir.components().collect();
        let mut current = PathBuf::new();
        let mut prev_was_sticky_writable = false;

        for (idx, comp) in components.iter().enumerate() {
            let is_leaf = idx + 1 == components.len();
            match comp {
                Component::Prefix(p) => current.push(p.as_os_str()),
                Component::RootDir => {
                    current.push(comp.as_os_str());
                    let root_meta = fs::symlink_metadata(&current).map_err(|err| {
                        ControlRuntimeError::new(
                            ControlRuntimeErrorCode::IoError,
                            format!(
                                "failed to stat root directory `{}`: {err}",
                                current.display()
                            ),
                        )
                    })?;
                    prev_was_sticky_writable =
                        validate_trusted_ancestor_meta(&current, &root_meta, euid, false)?;
                }
                Component::Normal(part) => {
                    current.push(part);
                    let meta = fs::symlink_metadata(&current).map_err(|err| {
                        let code =
                            if expected.is_some() && err.kind() == std::io::ErrorKind::NotFound {
                                ControlRuntimeErrorCode::StateChanged
                            } else {
                                ControlRuntimeErrorCode::IoError
                            };
                        ControlRuntimeError::new(
                            code,
                            format!(
                                "failed to stat control runtime directory component `{}`: {err}",
                                current.display()
                            ),
                        )
                    })?;
                    if meta.file_type().is_symlink() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "control runtime state_dir `{}` traverses symlink `{}`",
                                state_dir.display(),
                                current.display()
                            ),
                        ));
                    }
                    if !meta.file_type().is_dir() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "control runtime state_dir component `{}` is not a directory",
                                current.display()
                            ),
                        ));
                    }
                    if !is_leaf {
                        prev_was_sticky_writable = validate_trusted_ancestor_meta(
                            &current,
                            &meta,
                            euid,
                            prev_was_sticky_writable,
                        )?;
                    }
                }
                Component::CurDir | Component::ParentDir => unreachable!(),
            }
        }

        let meta = fs::symlink_metadata(state_dir).map_err(|err| {
            let code = if expected.is_some() && err.kind() == std::io::ErrorKind::NotFound {
                ControlRuntimeErrorCode::StateChanged
            } else {
                ControlRuntimeErrorCode::IoError
            };
            ControlRuntimeError::new(
                code,
                format!(
                    "failed to stat control runtime state_dir `{}`: {err}",
                    state_dir.display()
                ),
            )
        })?;

        if meta.uid() != euid {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime state_dir `{}` is owned by uid {} (expected {})",
                    state_dir.display(),
                    meta.uid(),
                    euid
                ),
            ));
        }

        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime state_dir `{}` has insecure permissions {:04o} (expected 0700)",
                    state_dir.display(),
                    mode
                ),
            ));
        }

        let current_identity = FileIdentity {
            dev: meta.dev(),
            ino: meta.ino(),
        };
        if let Some(expected_id) = expected {
            if current_identity != expected_id {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::StateChanged,
                    format!(
                        "control runtime state_dir `{}` identity changed unexpectedly during operation",
                        state_dir.display()
                    ),
                ));
            }
        }
        Ok(current_identity)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (state_dir, expected);
        ensure_linux_supported()?;
        unreachable!()
    }
}

fn validate_state_dir_contents(state_dir: &Path) -> Result<(), ControlRuntimeError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let entries = fs::read_dir(state_dir).map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!(
                    "failed to read control runtime state_dir `{}`: {err}",
                    state_dir.display()
                ),
            )
        })?;

        let mut is_non_empty = false;
        for entry_res in entries {
            let entry = entry_res.map_err(|err| {
                ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!(
                        "failed to inspect entries in control runtime state_dir `{}`: {err}",
                        state_dir.display()
                    ),
                )
            })?;
            is_non_empty = true;
            let entry_path = entry.path();
            let entry_meta = fs::symlink_metadata(&entry_path).map_err(|err| {
                ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!(
                        "failed to stat entry `{}` in control runtime state_dir: {err}",
                        entry_path.display()
                    ),
                )
            })?;
            if entry_meta.file_type().is_symlink() {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::SecurityViolation,
                    format!(
                        "control runtime state_dir `{}` must not contain symlink entry `{}`",
                        state_dir.display(),
                        entry_path.display()
                    ),
                ));
            }
        }

        let known_entries = [
            LOCK_FILENAME,
            DATABASE_FILENAME,
            DESCRIPTOR_FILENAME,
            "state.db-wal",
            "state.db-shm",
        ];
        let has_known_entry = known_entries
            .iter()
            .any(|name| fs::symlink_metadata(state_dir.join(name)).is_ok());

        if is_non_empty && !has_known_entry {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "control runtime state_dir `{}` is a pre-existing non-empty directory with no Threadlane runtime state entries",
                    state_dir.display()
                ),
            ));
        }

        let euid = current_euid();
        for name in [
            LOCK_FILENAME,
            DATABASE_FILENAME,
            "state.db-wal",
            "state.db-shm",
        ] {
            let path = state_dir.join(name);
            match fs::symlink_metadata(&path) {
                Ok(meta) => {
                    if meta.file_type().is_symlink() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "runtime state file `{}` must not be a symlink",
                                path.display()
                            ),
                        ));
                    }
                    if !meta.file_type().is_file() {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "runtime state file `{}` must be a regular file",
                                path.display()
                            ),
                        ));
                    }
                    if meta.uid() != euid {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "runtime state file `{}` is owned by uid {} (expected {})",
                                path.display(),
                                meta.uid(),
                                euid
                            ),
                        ));
                    }
                    let mode = meta.permissions().mode() & 0o777;
                    if mode & 0o077 != 0 {
                        return Err(ControlRuntimeError::new(
                            ControlRuntimeErrorCode::SecurityViolation,
                            format!(
                                "runtime state file `{}` has insecure permissions {:04o} (expected 0600)",
                                path.display(),
                                mode
                            ),
                        ));
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::IoError,
                        format!(
                            "failed to stat runtime state file `{}`: {err}",
                            path.display()
                        ),
                    ));
                }
            }
        }

        let descriptor_path = state_dir.join(DESCRIPTOR_FILENAME);
        match fs::symlink_metadata(&descriptor_path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::SecurityViolation,
                        format!(
                            "runtime descriptor `{}` must not be a symlink",
                            descriptor_path.display()
                        ),
                    ));
                }
                if !meta.file_type().is_file() {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::SecurityViolation,
                        format!(
                            "runtime descriptor `{}` must be a regular file",
                            descriptor_path.display()
                        ),
                    ));
                }
                if meta.uid() != euid {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::SecurityViolation,
                        format!(
                            "runtime descriptor `{}` is owned by uid {} (expected {})",
                            descriptor_path.display(),
                            meta.uid(),
                            euid
                        ),
                    ));
                }
                let mode = meta.permissions().mode() & 0o777;
                if mode != 0o600 {
                    return Err(ControlRuntimeError::new(
                        ControlRuntimeErrorCode::SecurityViolation,
                        format!(
                            "runtime descriptor `{}` has insecure permissions {:04o} (expected 0600)",
                            descriptor_path.display(),
                            mode
                        ),
                    ));
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!(
                        "failed to stat runtime descriptor `{}`: {err}",
                        descriptor_path.display()
                    ),
                ));
            }
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = state_dir;
        ensure_linux_supported()?;
        unreachable!()
    }
}

fn read_descriptor_snapshot_optional(
    state_dir: &Path,
    expected_dir_identity: FileIdentity,
) -> Result<Option<DescriptorSnapshot>, ControlRuntimeError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let _ = verify_state_dir_identity(state_dir, Some(expected_dir_identity))?;
        let descriptor_path = state_dir.join(DESCRIPTOR_FILENAME);
        let sym_meta = match fs::symlink_metadata(&descriptor_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!(
                        "failed to stat runtime descriptor `{}`: {err}",
                        descriptor_path.display()
                    ),
                ));
            }
        };

        if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "runtime descriptor `{}` must be a regular non-symlink file",
                    descriptor_path.display()
                ),
            ));
        }

        let euid = current_euid();
        if sym_meta.uid() != euid {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "runtime descriptor `{}` is owned by uid {} (expected {})",
                    descriptor_path.display(),
                    sym_meta.uid(),
                    euid
                ),
            ));
        }

        let sym_mode = sym_meta.permissions().mode() & 0o777;
        if sym_mode != 0o600 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "runtime descriptor `{}` has insecure permissions {:04o} (expected 0600)",
                    descriptor_path.display(),
                    sym_mode
                ),
            ));
        }

        if sym_meta.len() > MAX_DESCRIPTOR_BYTES {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidDescriptor,
                format!(
                    "runtime descriptor `{}` exceeds maximum size of {} bytes",
                    descriptor_path.display(),
                    MAX_DESCRIPTOR_BYTES
                ),
            ));
        }

        let file = File::open(&descriptor_path).map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!(
                    "failed to open runtime descriptor `{}`: {err}",
                    descriptor_path.display()
                ),
            )
        })?;
        let open_meta = file.metadata().map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!(
                    "failed to stat open runtime descriptor `{}`: {err}",
                    descriptor_path.display()
                ),
            )
        })?;

        if !open_meta.file_type().is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
            || open_meta.uid() != euid
        {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::StateChanged,
                format!(
                    "runtime descriptor `{}` changed while opening",
                    descriptor_path.display()
                ),
            ));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if open_mode != 0o600 {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::SecurityViolation,
                format!(
                    "runtime descriptor `{}` has insecure permissions {:04o} (expected 0600)",
                    descriptor_path.display(),
                    open_mode
                ),
            ));
        }

        let mut buf = Vec::with_capacity(open_meta.len() as usize);
        file.take(MAX_DESCRIPTOR_BYTES + 1)
            .read_to_end(&mut buf)
            .map_err(|err| {
                ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!(
                        "failed reading runtime descriptor `{}`: {err}",
                        descriptor_path.display()
                    ),
                )
            })?;
        if buf.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidDescriptor,
                format!(
                    "runtime descriptor `{}` exceeds maximum size of {} bytes",
                    descriptor_path.display(),
                    MAX_DESCRIPTOR_BYTES
                ),
            ));
        }

        // Re-verify state_dir identity after reading descriptor bytes.
        let _ = verify_state_dir_identity(state_dir, Some(expected_dir_identity))?;

        let descriptor: RuntimeDescriptor = serde_json::from_slice(&buf).map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidDescriptor,
                format!(
                    "invalid runtime descriptor JSON in `{}`: {err}",
                    descriptor_path.display()
                ),
            )
        })?;
        descriptor.validate().map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::InvalidDescriptor,
                format!(
                    "runtime descriptor `{}` failed schema validation: {err}",
                    descriptor_path.display()
                ),
            )
        })?;

        Ok(Some(DescriptorSnapshot {
            descriptor,
            identity: FileIdentity {
                dev: open_meta.dev(),
                ino: open_meta.ino(),
            },
        }))
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (state_dir, expected_dir_identity);
        ensure_linux_supported()?;
        unreachable!()
    }
}

/// Non-intrusively inspects `/proc/locks` and `/proc/<pid>/fd` on Linux to determine if a live
/// process holds an exclusive lock on `<state_dir>/supervisor.lock`, without calling `flock()`
/// and risking transient lock contention against a starting `threadlane serve` child.
fn inspect_supervisor_lock_holder(
    state_dir: &Path,
) -> Result<Option<LockHolderInfo>, ControlRuntimeError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        let lock_path = state_dir.join(LOCK_FILENAME);
        let lock_meta = match fs::symlink_metadata(&lock_path) {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(ControlRuntimeError::new(
                    ControlRuntimeErrorCode::IoError,
                    format!("failed to stat `{}`: {err}", lock_path.display()),
                ));
            }
        };

        let lock_dev = lock_meta.dev();
        let lock_ino = lock_meta.ino();
        let expected_major = linux_dev_major(lock_dev);
        let expected_minor = linux_dev_minor(lock_dev);

        let locks_text = fs::read_to_string("/proc/locks").map_err(|err| {
            ControlRuntimeError::new(
                ControlRuntimeErrorCode::IoError,
                format!("failed to read `/proc/locks`: {err}"),
            )
        })?;

        for line in locks_text.lines() {
            let tokens: Vec<&str> = line.split_ascii_whitespace().collect();
            if tokens.len() < 6 {
                continue;
            }
            // Skip blocked lock requests (`1: -> FLOCK ...`).
            if tokens[1] == "->" {
                continue;
            }
            let kind = tokens[1];
            let access = tokens[3];
            let pid_str = tokens[4];
            let dev_ino_str = tokens[5];

            if !matches!(kind, "FLOCK" | "POSIX" | "OFD") || access != "WRITE" {
                continue;
            }

            let mut parts = dev_ino_str.split(':');
            let (Some(maj_hex), Some(min_hex), Some(ino_dec), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };

            let Ok(ino) = ino_dec.parse::<u64>() else {
                continue;
            };
            if ino != lock_ino {
                continue;
            }

            let Ok(pid_i32) = pid_str.parse::<i32>() else {
                continue;
            };
            if pid_i32 <= 0 {
                continue;
            }
            let pid = pid_i32 as u32;
            if !is_process_alive(pid) {
                continue;
            }

            let dev_matches_proc_locks = u64::from_str_radix(maj_hex, 16).ok()
                == Some(expected_major)
                && u64::from_str_radix(min_hex, 16).ok() == Some(expected_minor);

            if dev_matches_proc_locks || process_holds_file_open(pid, lock_dev, lock_ino) {
                return Ok(Some(LockHolderInfo { pid }));
            }
        }

        Ok(None)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = state_dir;
        ensure_linux_supported()?;
        unreachable!()
    }
}

#[cfg(target_os = "linux")]
const fn linux_dev_major(dev: u64) -> u64 {
    ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff)
}

#[cfg(target_os = "linux")]
const fn linux_dev_minor(dev: u64) -> u64 {
    (dev & 0xff) | ((dev >> 12) & !0xff)
}

#[cfg(target_os = "linux")]
fn process_holds_file_open(pid: u32, expected_dev: u64, expected_ino: u64) -> bool {
    use std::os::unix::fs::MetadataExt;

    let Ok(entries) = fs::read_dir(format!("/proc/{pid}/fd")) else {
        return false;
    };
    for entry in entries.flatten() {
        if let Ok(meta) = fs::metadata(entry.path()) {
            if meta.dev() == expected_dev && meta.ino() == expected_ino {
                return true;
            }
        }
    }
    false
}

fn read_proc_stat_identity(pid: u32) -> Option<ProcStatIdentity> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let rparen = stat.rfind(')')?;
        let fields: Vec<&str> = stat[rparen + 1..].split_whitespace().collect();
        if fields.len() < 20 {
            return None;
        }
        let state = fields[0].chars().next()?;
        let pgrp = fields[2].parse::<i32>().ok()?;
        let start_ticks = fields[19].parse::<u64>().ok()?;
        Some(ProcStatIdentity {
            state,
            pgrp,
            start_ticks,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

fn proc_entry_owned_by_current_euid(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        fs::symlink_metadata(format!("/proc/{pid}"))
            .map(|meta| meta.uid() == current_euid())
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

fn ensure_linux_supported() -> Result<(), ControlRuntimeError> {
    #[cfg(target_os = "linux")]
    {
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(ControlRuntimeError::new(
            ControlRuntimeErrorCode::UnsupportedPlatform,
            "ensure_control_runtime is only implemented on Linux; failing closed on unsupported platform",
        ))
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn current_euid() -> u32 {
    // SAFETY: `geteuid` takes no arguments and is always safe to call on Linux/Unix.
    unsafe { libc::geteuid() }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn signal_process_group(pgid: i32, signal: libc::c_int) {
    if pgid > 1 {
        // SAFETY: `kill(-pgid, signal)` with `pgid > 1` signals only the isolated child process group.
        let _ = unsafe { libc::kill(-pgid, signal) };
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn signal_single_pid(pid: u32, signal: libc::c_int) {
    if let Ok(pid_i32) = i32::try_from(pid) {
        if pid_i32 > 1 && pid != std::process::id() {
            // SAFETY: `kill(pid_i32, signal)` targets a single verified PID owned by `current_euid()`.
            let _ = unsafe { libc::kill(pid_i32, signal) };
        }
    }
}
