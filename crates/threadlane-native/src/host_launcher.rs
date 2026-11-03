use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rmcp::model::CallToolResult;
use threadlane_authority::{ApprovalRecord, AuthorityStore};
use threadlane_core::Binding;
use threadlane_host::{
    has_prepared_dependency_input_or_ledger_entry, is_host_lock_held,
    read_host_launch_intent_optional, read_host_manifest_optional, validate_approval_id,
    validate_canonical_thread_uuid, validate_identifier, validate_sessions_root,
    verify_existing_session_dir, write_host_launch_intent, ApprovedHostClient,
    HostControlRuntimeState, HostError, HostLaunchIntent, HostLaunchIntentLock,
    HostLaunchIntentStatus, HostPhase, HOST_LOCK_FILENAME, HOST_MANIFEST_FILENAME,
    WORK_AREA_SUBDIR,
};
use threadlane_workflow::ToolAdapter;
use tokio::process::{Child, Command};
use tokio::sync::Mutex as AsyncMutex;

use crate::approved_mcp::{
    execute_start_approved_task, format_approved_scoped_error, map_host_error_to_mcp,
    open_host_stores_with_known_dirs, StartApprovedTaskParams,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{ensure_platform_supported, sanitize_diagnostic_text, sanitize_io_error};
use crate::mcp_pair::{open_workflow_stores, PairedMcpServer};
use crate::pairing::PairingStore;
use crate::rpc::is_process_alive;

/// Maximum wall-clock time (`5s`) `start_approved_task` waits for an initial or concurrently
/// bootstrapping approved host session to reach live authenticated `Ready` state.
pub const APPROVED_HOST_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Nonblocking poll interval (`20ms`) used while waiting for an initial approved host session to
/// reach live authenticated `Ready` state.
pub const APPROVED_HOST_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Maximum number of live child process handles retained for non-killing opportunistic reaping.
const MAX_RETAINED_CHILD_HANDLES: usize = 64;

/// Bounded wall-clock grace (`50ms`) for best-effort `Unknown` launch-intent bookkeeping after a
/// startup timeout or failure so post-timeout persistence never extends responses indefinitely.
const POST_TIMEOUT_INTENT_BOOKKEEPING_TIMEOUT: Duration = Duration::from_millis(50);

/// Default binary name of the sibling approved-host executable.
pub const DEFAULT_HOST_EXECUTABLE_NAME: &str = "threadlane-host";

/// Command-line interface for the `threadlane-native-mcp` paired stdio server.
///
/// Auto-starting initial approved hosts is **disabled by default** (`--auto-start-approved-hosts`
/// is `false`). Operators must explicitly pass `--auto-start-approved-hosts` together with
/// `--registry-dir`, `--plans-dir`, `--authority-dir`, and `--sessions-dir` to allow
/// `start_approved_task(approval_id, task_id)` to launch a brand-new `threadlane-host` session
/// when no prior session state exists.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "threadlane-native-mcp",
    about = "Unbound connection-epoch paired MCP stdio server for Threadlane native Codex integration"
)]
pub struct NativeMcpCli {
    /// Path to the private Threadlane supervisor state directory (`supervisor.json`).
    #[arg(long, value_name = "DIR")]
    pub state_dir: PathBuf,

    /// Path to the private (`0700`) native MCP pairing store directory.
    #[arg(long, value_name = "DIR")]
    pub pairing_dir: PathBuf,

    /// Optional path to the private (`0700`) global workflow registry directory (`registry.json`).
    /// Must be provided together with `--plans-dir`.
    #[arg(long, value_name = "DIR", requires = "plans_dir")]
    pub registry_dir: Option<PathBuf>,

    /// Optional path to the private (`0700`) versioned workflow plan store directory (`plans.json`).
    /// Must be provided together with `--registry-dir`.
    #[arg(long, value_name = "DIR", requires = "registry_dir")]
    pub plans_dir: Option<PathBuf>,

    /// Optional path to the private (`0700`) global operator approval authority directory (`approvals.json`).
    /// Must be provided together with `--sessions-dir`, `--registry-dir`, and `--plans-dir`.
    #[arg(
        long,
        value_name = "DIR",
        requires_all = ["sessions_dir", "registry_dir", "plans_dir"]
    )]
    pub authority_dir: Option<PathBuf>,

    /// Optional path to the private (`0700`) operator-selected approved host sessions root directory.
    /// Must be provided together with `--authority-dir`, `--registry-dir`, and `--plans-dir`.
    #[arg(
        long,
        value_name = "DIR",
        requires_all = ["authority_dir", "registry_dir", "plans_dir"]
    )]
    pub sessions_dir: Option<PathBuf>,

    /// Explicit operator-only opt-in (disabled by default) allowing `start_approved_task(approval_id, task_id)`
    /// to auto-start an initial fresh `threadlane-host` process (`serve --start-fresh-only`) when
    /// no prior session state exists. Requires `--registry-dir`, `--plans-dir`, `--authority-dir`,
    /// and `--sessions-dir`. Never grants approval, never resumes stopped/failed/corrupt sessions,
    /// and never enables notifications.
    #[arg(
        long = "auto-start-approved-hosts",
        default_value_t = false,
        requires_all = ["registry_dir", "plans_dir", "authority_dir", "sessions_dir"]
    )]
    pub auto_start_approved_hosts: bool,

    /// Optional explicit operator-only override for the absolute `threadlane-host` executable path.
    /// Requires `--auto-start-approved-hosts` (which is off by default) and the four store
    /// directory arguments. When omitted, resolves the canonical `threadlane-host` sibling next to
    /// the current native executable.
    #[arg(
        long = "host-executable",
        value_name = "ABS",
        requires_all = [
            "auto_start_approved_hosts",
            "registry_dir",
            "plans_dir",
            "authority_dir",
            "sessions_dir"
        ]
    )]
    pub host_executable: Option<PathBuf>,

    /// Explicit operator-only opt-in (disabled by default) allowing `start_execution` to configure
    /// scoped native coworker milestone notices for the verified MCP parent session. Requires
    /// `--registry-dir`, `--plans-dir`, `--authority-dir`, and `--sessions-dir`.
    #[arg(
        long = "coworker-native-notices",
        default_value_t = false,
        requires_all = ["registry_dir", "plans_dir", "authority_dir", "sessions_dir"]
    )]
    pub coworker_native_notices: bool,
}

#[cfg(test)]
mod native_notice_cli_flag_tests {
    use super::*;

    fn base_args() -> Vec<&'static str> {
        vec![
            "threadlane-native-mcp",
            "--state-dir",
            "/tmp/state",
            "--pairing-dir",
            "/tmp/pairing",
        ]
    }

    fn store_args() -> [(&'static str, &'static str); 4] {
        [
            ("--registry-dir", "/tmp/registry"),
            ("--plans-dir", "/tmp/plans"),
            ("--authority-dir", "/tmp/authority"),
            ("--sessions-dir", "/tmp/sessions"),
        ]
    }

    #[test]
    fn coworker_native_notices_requires_all_stores_and_is_not_enabled_by_auto_start() {
        assert!(
            !NativeMcpCli::try_parse_from(base_args())
                .unwrap()
                .coworker_native_notices
        );

        for omitted in 0..4 {
            let mut args = base_args();
            for (index, (flag, value)) in store_args().into_iter().enumerate() {
                if index != omitted {
                    args.push(flag);
                    args.push(value);
                }
            }
            args.push("--coworker-native-notices");
            assert!(NativeMcpCli::try_parse_from(args).is_err());
        }

        let mut auto_start_args = base_args();
        for (flag, value) in store_args() {
            auto_start_args.push(flag);
            auto_start_args.push(value);
        }
        auto_start_args.push("--auto-start-approved-hosts");
        let auto_start = NativeMcpCli::try_parse_from(auto_start_args).unwrap();
        assert!(auto_start.auto_start_approved_hosts);
        assert!(!auto_start.coworker_native_notices);

        let mut enabled_args = base_args();
        for (flag, value) in store_args() {
            enabled_args.push(flag);
            enabled_args.push(value);
        }
        enabled_args.push("--coworker-native-notices");
        assert!(
            NativeMcpCli::try_parse_from(enabled_args)
                .unwrap()
                .coworker_native_notices
        );
    }
}

impl NativeMcpCli {
    /// Validates CLI flag prerequisites, opens the configured stores, and constructs a
    /// [`PairedMcpServer`] (attaching the validated [`ApprovedHostLauncherPolicy`] only when
    /// `--auto-start-approved-hosts` is explicitly enabled by the operator).
    pub fn into_server(self) -> Result<PairedMcpServer, NativeError> {
        if !self.auto_start_approved_hosts && self.host_executable.is_some() {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "--host-executable requires --auto-start-approved-hosts",
            ));
        }
        if self.auto_start_approved_hosts
            && (self.registry_dir.is_none()
                || self.plans_dir.is_none()
                || self.authority_dir.is_none()
                || self.sessions_dir.is_none())
        {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "--auto-start-approved-hosts requires --registry-dir, --plans-dir, --authority-dir, and --sessions-dir",
            ));
        }
        if self.coworker_native_notices
            && (self.registry_dir.is_none()
                || self.plans_dir.is_none()
                || self.authority_dir.is_none()
                || self.sessions_dir.is_none())
        {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "--coworker-native-notices requires --registry-dir, --plans-dir, --authority-dir, and --sessions-dir",
            ));
        }

        let workflow_stores =
            open_workflow_stores(self.registry_dir.as_deref(), self.plans_dir.as_deref())?;
        let pairing_store = PairingStore::open(&self.pairing_dir)?;
        let mut server = PairedMcpServer::new(&self.state_dir, pairing_store)?;

        let host_stores = open_host_stores_with_known_dirs(
            self.authority_dir.as_deref(),
            self.sessions_dir.as_deref(),
            workflow_stores.as_ref(),
            &[
                ("state_dir", &self.state_dir),
                ("pairing_dir", server.pairing_store().dir()),
            ],
        )?;

        if let Some((registry, plans)) = workflow_stores {
            server = server.with_workflow_stores(registry, plans);
        }

        if let Some((authority, sessions_dir)) = host_stores {
            server = server.with_host_stores(authority, sessions_dir);
        }

        if let Some(policy) = ApprovedHostLauncherPolicy::from_operator_cli(
            self.auto_start_approved_hosts,
            self.host_executable.as_deref(),
        )? {
            server = server.with_host_launcher_policy(policy)?;
        }
        if self.coworker_native_notices {
            server = server.with_coworker_native_notices(true);
        }

        Ok(server)
    }
}

/// Validates an explicit or sibling-resolved `threadlane-host` executable path.
///
/// Requires a non-empty clean absolute path without `.` or `..` segments, canonicalizes symlink
/// chains once to their final target, and verifies that the resolved target is a non-symlink
/// regular file with execute permissions on Unix (`0o111`). Never searches `PATH` and never
/// invokes a shell.
pub fn validate_host_executable_path(executable: &Path) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if executable.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "threadlane-host executable path must not be empty",
        ));
    }
    if !executable.is_absolute() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "threadlane-host executable path must be an absolute path",
        ));
    }
    for comp in executable.components() {
        match comp {
            Component::CurDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "threadlane-host executable path must not contain `.` components",
                ));
            }
            Component::ParentDir => {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "threadlane-host executable path must not contain `..` traversal components",
                ));
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {}
        }
    }

    let canonical = match fs::canonicalize(executable) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "configured threadlane-host executable does not exist",
            ));
        }
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot canonicalize threadlane-host executable: {}",
                    sanitize_io_error(&err)
                ),
            ));
        }
    };

    let meta = match fs::symlink_metadata(&canonical) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "configured threadlane-host executable does not exist",
            ));
        }
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot stat threadlane-host executable: {}",
                    sanitize_io_error(&err)
                ),
            ));
        }
    };
    if meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "resolved threadlane-host executable target must not be a symlink",
        ));
    }
    if !meta.file_type().is_file() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "threadlane-host executable path is not a regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o111 == 0 {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "threadlane-host executable file is not marked executable",
            ));
        }
    }

    Ok(canonical)
}

/// Validated operator policy for auto-starting initial fresh `threadlane-host` sessions on
/// `start_approved_task(approval_id, task_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedHostLauncherPolicy {
    executable: PathBuf,
    startup_timeout: Duration,
}

/// Type alias for [`ApprovedHostLauncherPolicy`].
pub type HostLauncherPolicy = ApprovedHostLauncherPolicy;

impl ApprovedHostLauncherPolicy {
    /// Validates and canonicalizes an explicit operator-configured `threadlane-host` executable path.
    pub fn new(executable: impl AsRef<Path>) -> Result<Self, NativeError> {
        let canonical = validate_host_executable_path(executable.as_ref())?;
        Ok(Self {
            executable: canonical,
            startup_timeout: APPROVED_HOST_STARTUP_TIMEOUT,
        })
    }

    /// Resolves and validates the `threadlane-host` sibling of the canonical current native executable.
    pub fn from_current_exe_sibling() -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let current_exe = std::env::current_exe().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "cannot determine current native executable path: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        Self::from_sibling_of_executable(&current_exe)
    }

    /// Resolves and validates the `threadlane-host` sibling of the canonical target of `native_executable`.
    pub fn from_sibling_of_executable(
        native_executable: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let native_path = native_executable.as_ref();
        if native_path.as_os_str().is_empty() || !native_path.is_absolute() {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "current native executable path must be a non-empty absolute path",
            ));
        }
        let canonical_native = fs::canonicalize(native_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                format!(
                    "cannot canonicalize current native executable path: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        let parent_dir = canonical_native.parent().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::InvalidConfig,
                "canonical native executable path has no parent directory",
            )
        })?;
        let sibling_candidate = parent_dir.join(DEFAULT_HOST_EXECUTABLE_NAME);
        Self::new(sibling_candidate)
    }

    /// Constructs an optional [`ApprovedHostLauncherPolicy`] from operator CLI arguments.
    ///
    /// Returns `Ok(None)` when `auto_start_approved_hosts` is `false` and `host_executable` is `None`.
    /// Fails with [`NativeErrorCode::InvalidConfig`] if `host_executable` is provided while
    /// `auto_start_approved_hosts` is `false`.
    pub fn from_operator_cli(
        auto_start_approved_hosts: bool,
        host_executable: Option<&Path>,
    ) -> Result<Option<Self>, NativeError> {
        if !auto_start_approved_hosts {
            if host_executable.is_some() {
                return Err(NativeError::new(
                    NativeErrorCode::InvalidConfig,
                    "--host-executable requires --auto-start-approved-hosts",
                ));
            }
            return Ok(None);
        }

        match host_executable {
            Some(exec) => Self::new(exec).map(Some),
            None => Self::from_current_exe_sibling().map(Some),
        }
    }

    /// Returns the canonical validated `threadlane-host` executable path.
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    /// Returns the bounded wall-clock timeout (`5s` by default) for initial approved-host startup
    /// and authenticated readiness verification.
    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Overrides the bounded wall-clock timeout for initial approved-host startup and
    /// authenticated readiness verification (`1ms..=60s`).
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Result<Self, NativeError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(NativeError::new(
                NativeErrorCode::InvalidConfig,
                "approved host startup_timeout must be between 1ms and 60s",
            ));
        }
        self.startup_timeout = timeout;
        Ok(self)
    }

    /// Builds the exact direct argument vector for launching a brand-new `threadlane-host`
    /// session (`serve --registry-dir ... --plans-dir ... --authority-dir ... --sessions-dir ... --approval-id ... --start-fresh-only`).
    ///
    /// Never includes `--notify-codex`, `--notify-timeout-ms`, or `--restart-from-instance-id`.
    pub fn build_fresh_host_serve_args(
        registry_dir: &Path,
        plans_dir: &Path,
        authority_dir: &Path,
        sessions_dir: &Path,
        approval_id: &str,
    ) -> Vec<OsString> {
        vec![
            OsString::from("serve"),
            OsString::from("--registry-dir"),
            registry_dir.as_os_str().to_os_string(),
            OsString::from("--plans-dir"),
            plans_dir.as_os_str().to_os_string(),
            OsString::from("--authority-dir"),
            authority_dir.as_os_str().to_os_string(),
            OsString::from("--sessions-dir"),
            sessions_dir.as_os_str().to_os_string(),
            OsString::from("--approval-id"),
            OsString::from(approval_id),
            OsString::from("--start-fresh-only"),
        ]
    }
}

#[derive(Debug)]
struct TrackedChild {
    id: u64,
    approval_id: String,
    child: Child,
}

#[derive(Debug, Clone, Copy)]
enum FreshLaunchDecision {
    AlreadyReady,
    WaitExisting {
        spawned_child_id: Option<u64>,
        tracked_pid: Option<u32>,
    },
    Spawned {
        spawned_child_id: u64,
        tracked_pid: Option<u32>,
    },
}

#[derive(Debug)]
struct PreparedFreshLaunchPermit {
    intent_lock: HostLaunchIntentLock,
    executable: PathBuf,
    args: Vec<OsString>,
}

#[derive(Debug)]
enum TryPrepareFreshLaunch {
    LockBusy,
    AlreadyReady,
    WaitExisting {
        spawned_child_id: Option<u64>,
        tracked_pid: Option<u32>,
    },
    ReadyToSpawn(PreparedFreshLaunchPermit),
}

/// Per-server coordinator that serializes initial approved-host startup per `approval_id`,
/// retains and opportunistically reaps spawned child handles without killing live hosts on drop,
/// and enforces bounded authenticated readiness polling.
#[derive(Debug)]
pub(crate) struct ApprovedHostLauncher {
    policy: ApprovedHostLauncherPolicy,
    approval_locks: Mutex<BTreeMap<String, Arc<AsyncMutex<()>>>>,
    children: Mutex<(u64, Vec<TrackedChild>)>,
}

impl ApprovedHostLauncher {
    pub(crate) fn new(policy: ApprovedHostLauncherPolicy) -> Self {
        Self {
            policy,
            approval_locks: Mutex::new(BTreeMap::new()),
            children: Mutex::new((0, Vec::new())),
        }
    }

    pub(crate) fn policy(&self) -> &ApprovedHostLauncherPolicy {
        &self.policy
    }

    fn approval_mutex(&self, approval_id: &str) -> Arc<AsyncMutex<()>> {
        let mut guard = self
            .approval_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.len() >= 128 {
            guard.retain(|_, arc| Arc::strong_count(arc) > 1);
        }
        Arc::clone(
            guard
                .entry(approval_id.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }

    fn reap_children_and_check_exited(
        &self,
        child_id: Option<u64>,
        approval_id: Option<&str>,
    ) -> (bool, bool) {
        let mut guard = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut target_still_running = false;
        let mut approval_still_running = false;
        guard.1.retain_mut(|entry| match entry.child.try_wait() {
            Ok(Some(_status)) => false,
            Ok(None) => {
                if Some(entry.id) == child_id {
                    target_still_running = true;
                }
                if Some(entry.approval_id.as_str()) == approval_id {
                    approval_still_running = true;
                }
                true
            }
            Err(_) => false,
        });
        let target_exited = match child_id {
            Some(_) => !target_still_running,
            None => false,
        };
        (target_exited, approval_still_running)
    }

    fn register_child(&self, approval_id: &str, child: Child) -> u64 {
        let mut guard = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .1
            .retain_mut(|entry| matches!(entry.child.try_wait(), Ok(None)));
        if guard.1.len() >= MAX_RETAINED_CHILD_HANDLES {
            guard.1.remove(0);
        }
        guard.0 = guard.0.wrapping_add(1);
        let id = guard.0;
        guard.1.push(TrackedChild {
            id,
            approval_id: approval_id.to_string(),
            child,
        });
        id
    }

    fn try_prepare_fresh_launch(
        &self,
        authority: &AuthorityStore,
        canonical_sessions_root: &Path,
        record: &ApprovalRecord,
        trusted_binding_id: &str,
        trusted_thread_uuid: &str,
    ) -> Result<TryPrepareFreshLaunch, HostError> {
        let Some(intent_lock) =
            HostLaunchIntentLock::try_acquire(canonical_sessions_root, &record.approval_id)?
        else {
            return Ok(TryPrepareFreshLaunch::LockBusy);
        };
        let (_, approval_child_running) =
            self.reap_children_and_check_exited(None, Some(&record.approval_id));

        match inspect_existing_session_disposition(
            canonical_sessions_root,
            record,
            trusted_binding_id,
            trusted_thread_uuid,
            PreManifestProcessContext {
                known_pid: None,
                tracked_in_launcher: approval_child_running,
                child_exited_known: false,
                intent_lock_held: true,
            },
        ) {
            ExistingSessionDisposition::AttachReady => {
                return Ok(TryPrepareFreshLaunch::AlreadyReady);
            }
            ExistingSessionDisposition::WaitBootstrapping { tracked_pid } => {
                return Ok(TryPrepareFreshLaunch::WaitExisting {
                    spawned_child_id: None,
                    tracked_pid,
                });
            }
            ExistingSessionDisposition::Refuse(err) => {
                return Err(err);
            }
            ExistingSessionDisposition::EligibleForFreshLaunch => {}
        }

        let executable =
            validate_host_executable_path(self.policy.executable()).map_err(|err| {
                HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason: format!("threadlane-host launcher executable validation failed: {err}"),
                }
            })?;
        let args = ApprovedHostLauncherPolicy::build_fresh_host_serve_args(
            authority.registry().config_dir(),
            authority.plans().dir(),
            authority.dir(),
            canonical_sessions_root,
            &record.approval_id,
        );

        Ok(TryPrepareFreshLaunch::ReadyToSpawn(
            PreparedFreshLaunchPermit {
                intent_lock,
                executable,
                args,
            },
        ))
    }

    async fn spawn_prepared_host_after_intent_locked(
        &self,
        startup_deadline: Instant,
        canonical_sessions_root: &Path,
        record: &ApprovalRecord,
        permit: PreparedFreshLaunchPermit,
        mut intent: HostLaunchIntent,
    ) -> Result<FreshLaunchDecision, HostError> {
        // Final synchronous deadline check at the actual process-spawn boundary, with no
        // intervening `.await` before `command.spawn()`.
        if Instant::now() >= startup_deadline {
            mark_launch_intent_unknown_with_permit_best_effort(
                canonical_sessions_root,
                record,
                permit,
                intent,
                "startup deadline expired at process spawn boundary",
            )
            .await;
            return Err(startup_timeout_error(&record.approval_id, None));
        }

        let mut command = Command::new(&permit.executable);
        command
            .args(&permit.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);

        #[cfg(unix)]
        {
            command.process_group(0);
        }

        let child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                let sanitized = sanitize_io_error(&err);
                let diag = format!("spawn(threadlane-host) failed: {sanitized}");
                mark_launch_intent_unknown_with_permit_best_effort(
                    canonical_sessions_root,
                    record,
                    permit,
                    intent,
                    &diag,
                )
                .await;
                return Err(HostError::Io {
                    operation: "spawn(threadlane-host)".to_string(),
                    reason: sanitized,
                });
            }
        };

        let tracked_pid = child.id().filter(|&pid| pid != 0);
        let spawned_child_id = self.register_child(&record.approval_id, child);
        intent.spawn_pid = tracked_pid;
        intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);

        let aborted = Arc::new(AtomicBool::new(false));
        let mut abort_guard = IntentPersistenceAbortGuard::new(Arc::clone(&aborted));
        let roots = canonical_sessions_root.to_path_buf();
        let rec = record.clone();

        let pid_write_res = run_within_startup_deadline(
            startup_deadline,
            &record.approval_id,
            None,
            async move {
                tokio::task::spawn_blocking(move || {
                    let _intent_lock = permit.intent_lock;
                    if aborted.load(Ordering::SeqCst) || Instant::now() >= startup_deadline {
                        intent.status = HostLaunchIntentStatus::Unknown;
                        intent.last_error = Some(sanitize_intent_diagnostic(
                            "timed out waiting for approved host session to reach live authenticated Ready state; startup outcome is uncertain",
                        ));
                    }
                    write_host_launch_intent(&roots, &rec, &intent)?;
                    if aborted.load(Ordering::SeqCst) || Instant::now() >= startup_deadline {
                        if intent.status != HostLaunchIntentStatus::Unknown {
                            intent.status = HostLaunchIntentStatus::Unknown;
                            intent.last_error = Some(sanitize_intent_diagnostic(
                                "timed out waiting for approved host session to reach live authenticated Ready state; startup outcome is uncertain",
                            ));
                            intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);
                            let _ = write_host_launch_intent(&roots, &rec, &intent);
                        }
                        return Err(startup_timeout_error(&rec.approval_id, None));
                    }
                    Ok::<_, HostError>(())
                })
                .await
                .map_err(|err| HostError::Io {
                    operation: "spawn_blocking(write_host_launch_intent_pid)".to_string(),
                    reason: err.to_string(),
                })?
            },
        )
        .await;

        if pid_write_res.is_ok() {
            abort_guard.disarm();
        }
        pid_write_res?;

        Ok(FreshLaunchDecision::Spawned {
            spawned_child_id,
            tracked_pid,
        })
    }
}

struct IntentPersistenceAbortGuard {
    aborted: Arc<AtomicBool>,
    armed: bool,
}

impl IntentPersistenceAbortGuard {
    fn new(aborted: Arc<AtomicBool>) -> Self {
        Self {
            aborted,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for IntentPersistenceAbortGuard {
    fn drop(&mut self) {
        if self.armed {
            self.aborted.store(true, Ordering::SeqCst);
        }
    }
}

async fn persist_pending_launch_intent_within_deadline(
    startup_deadline: Instant,
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    permit: PreparedFreshLaunchPermit,
) -> Result<(PreparedFreshLaunchPermit, HostLaunchIntent), HostError> {
    if Instant::now() >= startup_deadline {
        drop(permit);
        return Err(startup_timeout_error(&record.approval_id, None));
    }

    let aborted = Arc::new(AtomicBool::new(false));
    let mut abort_guard = IntentPersistenceAbortGuard::new(Arc::clone(&aborted));
    let roots = canonical_sessions_root.to_path_buf();
    let rec = record.clone();

    let res = run_within_startup_deadline(
        startup_deadline,
        &record.approval_id,
        None,
        async move {
            tokio::task::spawn_blocking(move || {
                if aborted.load(Ordering::SeqCst) || Instant::now() >= startup_deadline {
                    drop(permit);
                    return Err(startup_timeout_error(&rec.approval_id, None));
                }

                let now_ms = current_epoch_ms();
                let mut intent =
                    HostLaunchIntent::new_pending(&rec, permit.executable.clone(), now_ms);
                write_host_launch_intent(&roots, &rec, &intent)?;

                if aborted.load(Ordering::SeqCst) || Instant::now() >= startup_deadline {
                    intent.status = HostLaunchIntentStatus::Unknown;
                    intent.last_error = Some(sanitize_intent_diagnostic(
                        "timed out or cancelled after persisting pending launch intent before process spawn",
                    ));
                    intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);
                    let _ = write_host_launch_intent(&roots, &rec, &intent);
                    drop(permit);
                    return Err(startup_timeout_error(&rec.approval_id, None));
                }

                Ok::<_, HostError>((permit, intent))
            })
            .await
            .map_err(|err| HostError::Io {
                operation: "spawn_blocking(persist_pending_launch_intent)".to_string(),
                reason: err.to_string(),
            })?
        },
    )
    .await;

    if res.is_ok() {
        abort_guard.disarm();
    }
    res
}

fn current_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(1)
        .max(1)
}

fn sanitize_intent_diagnostic(raw: &str) -> String {
    let sanitized = sanitize_diagnostic_text(raw);
    let mut out = String::new();
    for ch in sanitized.chars() {
        if out.len() + ch.len_utf8() > 256 {
            break;
        }
        out.push(ch);
    }
    if out.is_empty() {
        "host launch outcome uncertain".to_string()
    } else {
        out
    }
}

fn format_start_host_error(binding_id: &str, err: &HostError) -> CallToolResult {
    let (code, message) = map_host_error_to_mcp(err);
    format_approved_scoped_error(binding_id, "start_approved_task", code, message)
}

fn startup_timeout_error(approval_id: &str, last_observed_phase: Option<HostPhase>) -> HostError {
    HostError::HostNotReady {
        approval_id: approval_id.to_string(),
        phase: last_observed_phase,
        reason:
            "timed out waiting for approved host session to reach live authenticated Ready state; startup outcome is uncertain"
                .to_string(),
    }
}

fn try_reconcile_launch_intent_ready_blocking(
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
) -> Result<bool, HostError> {
    let Some(_intent_lock) =
        HostLaunchIntentLock::try_acquire(canonical_sessions_root, &record.approval_id)?
    else {
        return Ok(false);
    };
    if let Some(mut intent) = read_host_launch_intent_optional(canonical_sessions_root, record)? {
        if intent.status != HostLaunchIntentStatus::ReconciledReady || intent.last_error.is_some() {
            intent.status = HostLaunchIntentStatus::ReconciledReady;
            intent.last_error = None;
            intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);
            write_host_launch_intent(canonical_sessions_root, record, &intent)?;
        }
    }
    Ok(true)
}

fn mark_launch_intent_unknown_blocking(
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    diagnostic: &str,
) {
    let Ok(Some(_intent_lock)) =
        HostLaunchIntentLock::try_acquire(canonical_sessions_root, &record.approval_id)
    else {
        return;
    };
    let Ok(Some(mut intent)) = read_host_launch_intent_optional(canonical_sessions_root, record)
    else {
        return;
    };
    if intent.status != HostLaunchIntentStatus::ReconciledReady {
        intent.status = HostLaunchIntentStatus::Unknown;
        intent.last_error = Some(sanitize_intent_diagnostic(diagnostic));
        intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);
        let _ = write_host_launch_intent(canonical_sessions_root, record, &intent);
    }
}

async fn mark_launch_intent_unknown_best_effort(
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    diagnostic: &str,
) {
    let roots = canonical_sessions_root.to_path_buf();
    let rec = record.clone();
    let diag = diagnostic.to_string();
    let _ = tokio::time::timeout(
        POST_TIMEOUT_INTENT_BOOKKEEPING_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            mark_launch_intent_unknown_blocking(&roots, &rec, &diag);
        }),
    )
    .await;
}

async fn mark_launch_intent_unknown_with_permit_best_effort(
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    permit: PreparedFreshLaunchPermit,
    mut intent: HostLaunchIntent,
    diagnostic: &str,
) {
    let roots = canonical_sessions_root.to_path_buf();
    let rec = record.clone();
    let diag = sanitize_intent_diagnostic(diagnostic);
    let _ = tokio::time::timeout(
        POST_TIMEOUT_INTENT_BOOKKEEPING_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            let _intent_lock = permit.intent_lock;
            if intent.status != HostLaunchIntentStatus::ReconciledReady {
                intent.status = HostLaunchIntentStatus::Unknown;
                intent.last_error = Some(diag);
                intent.updated_at_ms = current_epoch_ms().max(intent.created_at_ms);
                let _ = write_host_launch_intent(&roots, &rec, &intent);
            }
        }),
    )
    .await;
}

#[derive(Debug)]
enum ExistingSessionDisposition {
    AttachReady,
    WaitBootstrapping { tracked_pid: Option<u32> },
    EligibleForFreshLaunch,
    Refuse(HostError),
}

fn validate_task_and_scope_before_launch(
    authority: &AuthorityStore,
    sessions_dir: &Path,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
    approval_id: &str,
    task_id: &str,
) -> Result<(ApprovalRecord, PathBuf), HostError> {
    validate_approval_id(approval_id)?;
    validate_identifier("task_id", task_id)?;
    validate_identifier("control_binding_id", trusted_binding_id)?;
    validate_canonical_thread_uuid("native_thread_uuid", trusted_thread_uuid)?;

    let record = authority.get_for_operator(approval_id)?;
    if record.binding_id != trusted_binding_id {
        return Err(HostError::ScopeMismatch {
            field: "control_binding_id".to_string(),
            expected: record.binding_id,
            actual: trusted_binding_id.to_string(),
            reason: "approval belongs to a different control_binding_id".to_string(),
        });
    }
    if record.thread_uuid != trusted_thread_uuid {
        return Err(HostError::ScopeMismatch {
            field: "thread_uuid".to_string(),
            expected: record.thread_uuid,
            actual: trusted_thread_uuid.to_string(),
            reason: "approval belongs to a different native_thread_uuid".to_string(),
        });
    }

    let canonical_sessions_root = validate_sessions_root(
        sessions_dir,
        &[
            ("registry_dir", authority.registry().config_dir()),
            ("plans_dir", authority.plans().dir()),
            ("authority_dir", authority.dir()),
            ("repository_path", record.repository_path.as_path()),
        ],
    )?;

    let task = record
        .tasks
        .iter()
        .find(|t| t.task_id == task_id)
        .ok_or_else(|| HostError::UnknownTask {
            approval_id: record.approval_id.clone(),
            task_id: task_id.to_string(),
        })?;

    if task.provider.is_none()
        || task.adapter == ToolAdapter::CodexCli
        || task.profile_id.is_none()
        || task.command_key.is_none()
    {
        return Err(HostError::NativeRouteRequired {
            approval_id: record.approval_id.clone(),
            task_id: task.task_id.clone(),
            adapter: task.adapter,
        });
    }

    if !task.depends_on.is_empty()
        && !has_prepared_dependency_input_or_ledger_entry(
            &canonical_sessions_root,
            &record,
            &task.task_id,
        )?
    {
        return Err(HostError::DependencyInputsUnverified {
            approval_id: record.approval_id.clone(),
            task_id: task.task_id.clone(),
            depends_on: task.depends_on.clone(),
        });
    }

    Ok((record, canonical_sessions_root))
}

fn inspect_manifest_disposition(
    session_dir: &Path,
    manifest: threadlane_host::HostSessionManifest,
    record: &ApprovalRecord,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
    tracked_pid: Option<u32>,
) -> ExistingSessionDisposition {
    if manifest.approval_id != record.approval_id
        || manifest.subject_digest != record.subject_digest
        || manifest.control_binding_id != trusted_binding_id
        || manifest.thread_uuid != trusted_thread_uuid
        || manifest.plan_id != record.plan_id
        || manifest.plan_revision != record.plan_revision
    {
        return ExistingSessionDisposition::Refuse(HostError::CorruptManifest {
            path: session_dir
                .join(HOST_MANIFEST_FILENAME)
                .display()
                .to_string(),
            reason: "manifest ownership fields do not match approved scope".to_string(),
        });
    }

    match manifest.phase {
        HostPhase::Ready => {
            let lock_held = match is_host_lock_held(session_dir) {
                Ok(held) => held,
                Err(err) => return ExistingSessionDisposition::Refuse(err),
            };
            if !lock_held {
                ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(HostPhase::Ready),
                    reason:
                        "host manifest is Ready, but host.lock is not held by an active process"
                            .to_string(),
                })
            } else if !manifest.gates.supervisor_authenticated_ready {
                ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(HostPhase::Ready),
                    reason: "host manifest Ready gate is false".to_string(),
                })
            } else {
                ExistingSessionDisposition::AttachReady
            }
        }
        HostPhase::Bootstrapping => {
            let lock_held = match is_host_lock_held(session_dir) {
                Ok(held) => held,
                Err(err) => return ExistingSessionDisposition::Refuse(err),
            };
            if lock_held {
                ExistingSessionDisposition::WaitBootstrapping { tracked_pid }
            } else {
                ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(HostPhase::Bootstrapping),
                    reason:
                        "host session left an unlocked Bootstrapping manifest and cannot be auto-started"
                            .to_string(),
                })
            }
        }
        HostPhase::Stopping | HostPhase::Stopped | HostPhase::Failed => {
            ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: Some(manifest.phase),
                reason: manifest.last_error.unwrap_or_else(|| {
                    format!(
                        "host session is in `{}` phase and cannot be auto-started",
                        manifest.phase
                    )
                }),
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PreManifestProcessContext {
    known_pid: Option<u32>,
    tracked_in_launcher: bool,
    child_exited_known: bool,
    intent_lock_held: bool,
}

fn inspect_existing_session_disposition(
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
    process_ctx: PreManifestProcessContext,
) -> ExistingSessionDisposition {
    let PreManifestProcessContext {
        known_pid,
        tracked_in_launcher,
        child_exited_known,
        intent_lock_held,
    } = process_ctx;
    let intent_opt = match read_host_launch_intent_optional(canonical_sessions_root, record) {
        Ok(opt) => opt,
        Err(err) => return ExistingSessionDisposition::Refuse(err),
    };

    let candidate = canonical_sessions_root.join(&record.approval_id);
    let session_dir_opt = match fs::symlink_metadata(&candidate) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return ExistingSessionDisposition::Refuse(HostError::Io {
                operation: "symlink_metadata(session_dir)".to_string(),
                reason: sanitize_io_error(&err),
            });
        }
        Ok(_) => match verify_existing_session_dir(canonical_sessions_root, &record.approval_id) {
            Ok(dir) => Some(dir),
            Err(err) => return ExistingSessionDisposition::Refuse(err),
        },
    };

    let mut pre_manifest_dir: Option<(PathBuf, Vec<String>)> = None;
    if let Some(session_dir) = session_dir_opt {
        let manifest_opt = match read_host_manifest_optional(&session_dir, record) {
            Ok(opt) => opt,
            Err(err) => return ExistingSessionDisposition::Refuse(err),
        };

        if let Some(manifest) = manifest_opt {
            return inspect_manifest_disposition(
                &session_dir,
                manifest,
                record,
                trusted_binding_id,
                trusted_thread_uuid,
                known_pid.or_else(|| intent_opt.as_ref().and_then(|i| i.spawn_pid)),
            );
        }

        let entries = match fs::read_dir(&session_dir) {
            Ok(rd) => rd,
            Err(err) => {
                return ExistingSessionDisposition::Refuse(HostError::Io {
                    operation: "read_dir(session_dir)".to_string(),
                    reason: sanitize_io_error(&err),
                });
            }
        };

        let mut non_lock_entries = Vec::new();
        for entry_res in entries {
            let entry = match entry_res {
                Ok(e) => e,
                Err(err) => {
                    return ExistingSessionDisposition::Refuse(HostError::Io {
                        operation: "read_dir_entry(session_dir)".to_string(),
                        reason: sanitize_io_error(&err),
                    });
                }
            };
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                return ExistingSessionDisposition::Refuse(HostError::SecurityViolation {
                    path: entry.path().display().to_string(),
                    reason: "non-UTF-8 entry in session directory is not permitted".to_string(),
                });
            };
            if name == HOST_LOCK_FILENAME {
                let meta = match fs::symlink_metadata(entry.path()) {
                    Ok(m) => m,
                    Err(err) => {
                        return ExistingSessionDisposition::Refuse(HostError::Io {
                            operation: "symlink_metadata(host.lock)".to_string(),
                            reason: sanitize_io_error(&err),
                        });
                    }
                };
                if meta.len() != 0 {
                    return ExistingSessionDisposition::Refuse(HostError::SecurityViolation {
                        path: entry.path().display().to_string(),
                        reason: "host.lock must be a 0-byte lock file".to_string(),
                    });
                }
                continue;
            }
            non_lock_entries.push(name.to_string());
        }

        if !non_lock_entries.is_empty()
            && !non_lock_entries
                .iter()
                .all(|name| name.starts_with(".host-manifest.json.tmp."))
        {
            non_lock_entries.sort();
            return ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                approval_id: record.approval_id.clone(),
                phase: None,
                reason: format!(
                    "pre-existing session state ({}) without a valid manifest cannot be auto-started",
                    non_lock_entries.join(", ")
                ),
            });
        }

        pre_manifest_dir = Some((session_dir, non_lock_entries));
    }

    // Apply the live launch-intent guard BEFORE probing `host.lock` on a pre-manifest session so
    // an observer never steals the exclusive lock from a starting host between `open(host.lock)`
    // and `HostSessionLock::acquire`.
    if let Some(mut existing_intent) = intent_opt {
        match existing_intent.status {
            HostLaunchIntentStatus::ReconciledReady => {
                return ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: None,
                    reason:
                        "prior launch intent was already reconciled Ready, but active host session state is missing"
                            .to_string(),
                });
            }
            HostLaunchIntentStatus::Pending | HostLaunchIntentStatus::Unknown => {
                let pid_to_check = known_pid.or(existing_intent.spawn_pid);
                if let Some(pid) = pid_to_check {
                    let pid_alive = !child_exited_known && is_process_alive(pid);
                    if tracked_in_launcher || pid_alive {
                        return ExistingSessionDisposition::WaitBootstrapping {
                            tracked_pid: Some(pid),
                        };
                    }

                    // The recorded process is no longer alive. Re-check whether it committed
                    // `host-manifest.json` right before exiting.
                    if let Ok(dir) =
                        verify_existing_session_dir(canonical_sessions_root, &record.approval_id)
                    {
                        match read_host_manifest_optional(&dir, record) {
                            Ok(Some(manifest)) => {
                                return inspect_manifest_disposition(
                                    &dir,
                                    manifest,
                                    record,
                                    trusted_binding_id,
                                    trusted_thread_uuid,
                                    Some(pid),
                                );
                            }
                            Ok(None) => {}
                            Err(err) => return ExistingSessionDisposition::Refuse(err),
                        }
                    }

                    if existing_intent.status != HostLaunchIntentStatus::Unknown {
                        let diag =
                            "spawned threadlane-host process exited before initializing session state";
                        if intent_lock_held {
                            existing_intent.status = HostLaunchIntentStatus::Unknown;
                            existing_intent.last_error = Some(sanitize_intent_diagnostic(diag));
                            existing_intent.updated_at_ms =
                                current_epoch_ms().max(existing_intent.created_at_ms);
                            let _ = write_host_launch_intent(
                                canonical_sessions_root,
                                record,
                                &existing_intent,
                            );
                        } else {
                            mark_launch_intent_unknown_blocking(
                                canonical_sessions_root,
                                record,
                                diag,
                            );
                        }
                    }

                    return ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                        approval_id: record.approval_id.clone(),
                        phase: None,
                        reason:
                            "prior spawned threadlane-host process exited without initializing session state; refusing duplicate auto-start"
                                .to_string(),
                    });
                }

                if tracked_in_launcher {
                    return ExistingSessionDisposition::WaitBootstrapping { tracked_pid: None };
                }

                if existing_intent.status == HostLaunchIntentStatus::Unknown || child_exited_known {
                    if existing_intent.status != HostLaunchIntentStatus::Unknown {
                        let diag =
                            "spawned threadlane-host process exited before initializing session state";
                        if intent_lock_held {
                            existing_intent.status = HostLaunchIntentStatus::Unknown;
                            existing_intent.last_error = Some(sanitize_intent_diagnostic(diag));
                            existing_intent.updated_at_ms =
                                current_epoch_ms().max(existing_intent.created_at_ms);
                            let _ = write_host_launch_intent(
                                canonical_sessions_root,
                                record,
                                &existing_intent,
                            );
                        } else {
                            mark_launch_intent_unknown_blocking(
                                canonical_sessions_root,
                                record,
                                diag,
                            );
                        }
                    }
                    return ExistingSessionDisposition::Refuse(HostError::HostNotReady {
                        approval_id: record.approval_id.clone(),
                        phase: None,
                        reason:
                            "prior launch intent is in unknown state without an active child; refusing duplicate auto-start"
                                .to_string(),
                    });
                }

                // `Pending` intent while process spawn / PID persistence is still in flight.
                return ExistingSessionDisposition::WaitBootstrapping { tracked_pid: None };
            }
        }
    }

    if tracked_in_launcher {
        return ExistingSessionDisposition::WaitBootstrapping {
            tracked_pid: known_pid,
        };
    }
    if child_exited_known {
        return ExistingSessionDisposition::Refuse(HostError::HostNotReady {
            approval_id: record.approval_id.clone(),
            phase: None,
            reason: "spawned threadlane-host process exited before initializing session state"
                .to_string(),
        });
    }

    let Some((session_dir, mut non_lock_entries)) = pre_manifest_dir else {
        return ExistingSessionDisposition::EligibleForFreshLaunch;
    };

    let lock_held = match is_host_lock_held(&session_dir) {
        Ok(held) => held,
        Err(err) => return ExistingSessionDisposition::Refuse(err),
    };

    if non_lock_entries.is_empty() {
        if lock_held {
            ExistingSessionDisposition::WaitBootstrapping { tracked_pid: None }
        } else {
            ExistingSessionDisposition::EligibleForFreshLaunch
        }
    } else if lock_held {
        ExistingSessionDisposition::WaitBootstrapping { tracked_pid: None }
    } else {
        non_lock_entries.sort();
        ExistingSessionDisposition::Refuse(HostError::HostNotReady {
            approval_id: record.approval_id.clone(),
            phase: None,
            reason: format!(
                "pre-existing session state ({}) without a valid manifest cannot be auto-started",
                non_lock_entries.join(", ")
            ),
        })
    }
}

async fn run_within_startup_deadline<T, Fut>(
    startup_deadline: Instant,
    approval_id: &str,
    last_observed_phase: Option<HostPhase>,
    fut: Fut,
) -> Result<T, HostError>
where
    Fut: std::future::Future<Output = Result<T, HostError>>,
{
    let Some(remaining) = startup_deadline.checked_duration_since(Instant::now()) else {
        return Err(startup_timeout_error(approval_id, last_observed_phase));
    };
    match tokio::time::timeout(remaining, fut).await {
        Ok(res) => res,
        Err(_elapsed) => Err(startup_timeout_error(approval_id, last_observed_phase)),
    }
}

async fn run_enrollment_recheck_within_deadline<F>(
    startup_deadline: Instant,
    trusted_binding_id: &str,
    approval_id: &str,
    last_observed_phase: Option<HostPhase>,
    recheck_enrollment: &Arc<F>,
) -> Result<(), CallToolResult>
where
    F: Fn() -> Result<(), CallToolResult> + Send + Sync + 'static,
{
    let Some(remaining) = startup_deadline.checked_duration_since(Instant::now()) else {
        return Err(format_start_host_error(
            trusted_binding_id,
            &startup_timeout_error(approval_id, last_observed_phase),
        ));
    };
    let recheck = Arc::clone(recheck_enrollment);
    let check_res =
        match tokio::time::timeout(remaining, tokio::task::spawn_blocking(move || recheck())).await
        {
            Ok(Ok(res)) => res,
            Ok(Err(join_err)) => {
                return Err(format_start_host_error(
                    trusted_binding_id,
                    &HostError::Io {
                        operation: "spawn_blocking(recheck_enrollment)".to_string(),
                        reason: join_err.to_string(),
                    },
                ));
            }
            Err(_elapsed) => {
                return Err(format_start_host_error(
                    trusted_binding_id,
                    &startup_timeout_error(approval_id, last_observed_phase),
                ));
            }
        };
    check_res?;
    if Instant::now() >= startup_deadline {
        return Err(format_start_host_error(
            trusted_binding_id,
            &startup_timeout_error(approval_id, last_observed_phase),
        ));
    }
    Ok(())
}

async fn verify_live_authenticated_ready(
    authority: &Arc<AuthorityStore>,
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
) -> Result<(), HostError> {
    let auth_clone = Arc::clone(authority);
    let sessions_clone = canonical_sessions_root.to_path_buf();
    let rec_clone = record.clone();
    let binding_clone = trusted_binding_id.to_string();
    let thread_clone = trusted_thread_uuid.to_string();
    let approval_id = record.approval_id.clone();

    let (client, summary) = tokio::task::spawn_blocking(move || {
        let _ = read_host_launch_intent_optional(&sessions_clone, &rec_clone)?;
        let client =
            ApprovedHostClient::new(auth_clone, &sessions_clone, binding_clone, thread_clone)?;
        let summary = client.session_summary(&approval_id)?;
        Ok::<_, HostError>((client, summary))
    })
    .await
    .map_err(|err| HostError::Io {
        operation: "spawn_blocking(verify_live_authenticated_ready)".to_string(),
        reason: err.to_string(),
    })??;

    if summary.approval_id != record.approval_id
        || summary.subject_digest != record.subject_digest
        || summary.binding_id != trusted_binding_id
        || summary.thread_uuid != trusted_thread_uuid
        || summary.plan_id != record.plan_id
        || summary.plan_revision != record.plan_revision
    {
        return Err(HostError::ScopeMismatch {
            field: "approval_id".to_string(),
            expected: record.approval_id.clone(),
            actual: summary.approval_id,
            reason: "host session summary ownership did not match frozen approval record"
                .to_string(),
        });
    }

    if summary.phase != HostPhase::Ready || !summary.gates.supervisor_authenticated_ready {
        return Err(HostError::HostNotReady {
            approval_id: record.approval_id.clone(),
            phase: Some(summary.phase),
            reason: "host session is not in authenticated Ready state".to_string(),
        });
    }

    let expected_session_binding_id =
        summary
            .session_binding_id
            .clone()
            .ok_or_else(|| HostError::CorruptManifest {
                path: canonical_sessions_root
                    .join(&record.approval_id)
                    .join(HOST_MANIFEST_FILENAME)
                    .display()
                    .to_string(),
                reason: "Ready manifest is missing session_binding_id".to_string(),
            })?;

    if expected_session_binding_id == trusted_binding_id {
        return Err(HostError::ScopeMismatch {
            field: "session_binding_id".to_string(),
            expected: expected_session_binding_id,
            actual: trusted_binding_id.to_string(),
            reason: "approved host session_binding_id must be distinct from control_binding_id"
                .to_string(),
        });
    }

    let activity_page = client
        .list_activity(&record.approval_id, None, None, Some(1), None)
        .await?;
    if activity_page.binding_id != expected_session_binding_id
        || activity_page.binding_id == trusted_binding_id
    {
        return Err(HostError::ScopeMismatch {
            field: "session_binding_id".to_string(),
            expected: expected_session_binding_id,
            actual: activity_page.binding_id,
            reason:
                "live supervisor activity binding did not match Ready manifest session_binding_id"
                    .to_string(),
        });
    }

    if let Some(ref expected_instance_id) = summary.instance_id {
        if let Some(control_info) = client.host_control_status(&record.approval_id).await? {
            if control_info.approval_id != record.approval_id
                || control_info.subject_digest != record.subject_digest
                || control_info.control_binding_id != trusted_binding_id
                || control_info.thread_uuid != trusted_thread_uuid
                || control_info.session_binding_id != expected_session_binding_id
                || &control_info.instance_id != expected_instance_id
            {
                return Err(HostError::CorruptSessionState {
                    reason:
                        "live supervisor host_control_status did not match frozen approval ownership"
                            .to_string(),
                });
            }
            if control_info.state != HostControlRuntimeState::Ready {
                return Err(HostError::HostNotReady {
                    approval_id: record.approval_id.clone(),
                    phase: Some(HostPhase::Stopping),
                    reason: "live supervisor is no longer in Ready state".to_string(),
                });
            }
        }
    }

    loop {
        let sessions_reconcile = canonical_sessions_root.to_path_buf();
        let rec_reconcile = record.clone();
        let reconciled = tokio::task::spawn_blocking(move || {
            try_reconcile_launch_intent_ready_blocking(&sessions_reconcile, &rec_reconcile)
        })
        .await
        .map_err(|err| HostError::Io {
            operation: "spawn_blocking(reconcile_launch_intent_ready)".to_string(),
            reason: err.to_string(),
        })??;
        if reconciled {
            break;
        }
        tokio::time::sleep(APPROVED_HOST_STARTUP_POLL_INTERVAL).await;
    }

    Ok(())
}

struct AuthenticatedReadyWait {
    startup_deadline: Instant,
    spawned_child_id: Option<u64>,
    tracked_pid: Option<u32>,
}

async fn wait_for_authenticated_ready(
    launcher: &ApprovedHostLauncher,
    authority: &Arc<AuthorityStore>,
    canonical_sessions_root: &Path,
    record: &ApprovalRecord,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
    wait: AuthenticatedReadyWait,
) -> Result<(), HostError> {
    let AuthenticatedReadyWait {
        startup_deadline,
        spawned_child_id,
        mut tracked_pid,
    } = wait;
    let mut last_observed_phase: Option<HostPhase> = None;

    loop {
        if Instant::now() >= startup_deadline {
            return Err(startup_timeout_error(
                &record.approval_id,
                last_observed_phase,
            ));
        }

        let (child_exited, approval_child_running) =
            launcher.reap_children_and_check_exited(spawned_child_id, Some(&record.approval_id));
        let disp = {
            let roots = canonical_sessions_root.to_path_buf();
            let rec = record.clone();
            let b_id = trusted_binding_id.to_string();
            let t_id = trusted_thread_uuid.to_string();
            let known_pid = tracked_pid;
            let tracked_in_launcher =
                (spawned_child_id.is_some() && !child_exited) || approval_child_running;
            let child_exited_known = spawned_child_id.is_some() && child_exited;
            tokio::task::spawn_blocking(move || {
                inspect_existing_session_disposition(
                    &roots,
                    &rec,
                    &b_id,
                    &t_id,
                    PreManifestProcessContext {
                        known_pid,
                        tracked_in_launcher,
                        child_exited_known,
                        intent_lock_held: false,
                    },
                )
            })
            .await
            .map_err(|err| HostError::Io {
                operation: "spawn_blocking(inspect_existing_session_disposition)".to_string(),
                reason: err.to_string(),
            })?
        };

        match disp {
            ExistingSessionDisposition::AttachReady => {
                return run_within_startup_deadline(
                    startup_deadline,
                    &record.approval_id,
                    Some(HostPhase::Ready),
                    verify_live_authenticated_ready(
                        authority,
                        canonical_sessions_root,
                        record,
                        trusted_binding_id,
                        trusted_thread_uuid,
                    ),
                )
                .await;
            }
            ExistingSessionDisposition::WaitBootstrapping {
                tracked_pid: observed_pid,
            } => {
                if tracked_pid.is_none() {
                    tracked_pid = observed_pid;
                }
                last_observed_phase = Some(HostPhase::Bootstrapping);
            }
            ExistingSessionDisposition::EligibleForFreshLaunch => {}
            ExistingSessionDisposition::Refuse(err) => {
                return Err(err);
            }
        }

        let Some(remaining) = startup_deadline.checked_duration_since(Instant::now()) else {
            return Err(startup_timeout_error(
                &record.approval_id,
                last_observed_phase,
            ));
        };

        tokio::time::sleep(remaining.min(APPROVED_HOST_STARTUP_POLL_INTERVAL)).await;
    }
}

/// Executes `start_approved_task` with optional initial fresh-only host auto-start when
/// `launcher` is configured.
#[cfg(test)]
pub(crate) async fn execute_start_approved_task_with_launcher<F>(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: StartApprovedTaskParams,
    launcher: Option<Arc<ApprovedHostLauncher>>,
    recheck_enrollment: F,
) -> Result<CallToolResult, CallToolResult>
where
    F: Fn() -> Result<(), CallToolResult> + Send + Sync + 'static,
{
    execute_start_approved_task_with_launcher_until(
        authority,
        sessions_dir,
        trusted_binding_id,
        trusted_thread_uuid,
        params,
        launcher,
        None,
        recheck_enrollment,
    )
    .await
}

/// Executes `start_approved_task` with optional initial fresh-only host auto-start bounded by
/// `startup_deadline` (or `Instant::now() + launcher.policy().startup_timeout()` when `None`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_start_approved_task_with_launcher_until<F>(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: StartApprovedTaskParams,
    launcher: Option<Arc<ApprovedHostLauncher>>,
    startup_deadline: Option<Instant>,
    recheck_enrollment: F,
) -> Result<CallToolResult, CallToolResult>
where
    F: Fn() -> Result<(), CallToolResult> + Send + Sync + 'static,
{
    let Some(launcher) = launcher else {
        return execute_start_approved_task(
            authority,
            sessions_dir,
            trusted_binding_id,
            trusted_thread_uuid,
            params,
        )
        .await;
    };

    let startup_deadline =
        startup_deadline.unwrap_or_else(|| Instant::now() + launcher.policy().startup_timeout());
    let recheck_enrollment = Arc::new(recheck_enrollment);

    // 1. Validate approval, caller scope (binding_id + thread_uuid), sessions_dir, and that the
    //    requested task exists, is a root worker task, and is not a manual Codex route BEFORE
    //    touching the session directory or spawning any process, bounded by `startup_deadline`.
    validate_approval_id(&params.approval_id)
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
    validate_identifier("task_id", &params.task_id)
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
    validate_identifier("control_binding_id", &trusted_binding_id)
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
    validate_canonical_thread_uuid("native_thread_uuid", &trusted_thread_uuid)
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;

    let (record, canonical_sessions_root) = {
        let auth = Arc::clone(&authority);
        let sess = sessions_dir.clone();
        let b_id = trusted_binding_id.clone();
        let t_id = trusted_thread_uuid.clone();
        let a_id = params.approval_id.clone();
        let task_id = params.task_id.clone();
        run_within_startup_deadline(startup_deadline, &params.approval_id, None, async move {
            tokio::task::spawn_blocking(move || {
                validate_task_and_scope_before_launch(&auth, &sess, &b_id, &t_id, &a_id, &task_id)
            })
            .await
            .map_err(|err| HostError::Io {
                operation: "spawn_blocking(validate_task_and_scope_before_launch)".to_string(),
                reason: err.to_string(),
            })?
        })
        .await
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?
    };

    // 2. Serialize initial host startup attempts for this `approval_id` across concurrent callers
    //    within this MCP server without holding a std::sync::Mutex across `.await`, bounded by
    //    `startup_deadline`.
    let approval_mutex = launcher.approval_mutex(&record.approval_id);
    let _approval_guard =
        run_within_startup_deadline(startup_deadline, &record.approval_id, None, async {
            Ok(approval_mutex.lock().await)
        })
        .await
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;

    run_enrollment_recheck_within_deadline(
        startup_deadline,
        &trusted_binding_id,
        &record.approval_id,
        None,
        &recheck_enrollment,
    )
    .await?;

    let (_, approval_child_running) =
        launcher.reap_children_and_check_exited(None, Some(&record.approval_id));

    let initial_disposition = {
        let roots = canonical_sessions_root.clone();
        let rec = record.clone();
        let b_id = trusted_binding_id.clone();
        let t_id = trusted_thread_uuid.clone();
        run_within_startup_deadline(startup_deadline, &record.approval_id, None, async move {
            tokio::task::spawn_blocking(move || {
                Ok::<_, HostError>(inspect_existing_session_disposition(
                    &roots,
                    &rec,
                    &b_id,
                    &t_id,
                    PreManifestProcessContext {
                        known_pid: None,
                        tracked_in_launcher: approval_child_running,
                        child_exited_known: false,
                        intent_lock_held: false,
                    },
                ))
            })
            .await
            .map_err(|err| HostError::Io {
                operation: "spawn_blocking(inspect_existing_session_disposition)".to_string(),
                reason: err.to_string(),
            })?
        })
        .await
        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?
    };

    match initial_disposition {
        ExistingSessionDisposition::Refuse(err) => {
            return Err(format_start_host_error(&trusted_binding_id, &err));
        }
        ExistingSessionDisposition::AttachReady => {
            run_within_startup_deadline(
                startup_deadline,
                &record.approval_id,
                Some(HostPhase::Ready),
                verify_live_authenticated_ready(
                    &authority,
                    &canonical_sessions_root,
                    &record,
                    &trusted_binding_id,
                    &trusted_thread_uuid,
                ),
            )
            .await
            .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
        }
        ExistingSessionDisposition::WaitBootstrapping { tracked_pid } => {
            let wait_res = run_within_startup_deadline(
                startup_deadline,
                &record.approval_id,
                Some(HostPhase::Bootstrapping),
                wait_for_authenticated_ready(
                    &launcher,
                    &authority,
                    &canonical_sessions_root,
                    &record,
                    &trusted_binding_id,
                    &trusted_thread_uuid,
                    AuthenticatedReadyWait {
                        startup_deadline,
                        spawned_child_id: None,
                        tracked_pid,
                    },
                ),
            )
            .await;
            if let Err(err) = wait_res {
                let diag = err.to_string();
                mark_launch_intent_unknown_best_effort(&canonical_sessions_root, &record, &diag)
                    .await;
                return Err(format_start_host_error(&trusted_binding_id, &err));
            }
        }
        ExistingSessionDisposition::EligibleForFreshLaunch => {
            // Verify source plan publication status, repository/base_commit, write scopes, and
            // frozen worker profiles BEFORE spawning so superseded plans or invalid repos cause
            // zero process effects and zero directory creation.
            {
                let auth = Arc::clone(&authority);
                let rec = record.clone();
                let preview_work_area = canonical_sessions_root
                    .join(&record.approval_id)
                    .join(WORK_AREA_SUBDIR);
                run_within_startup_deadline(
                    startup_deadline,
                    &record.approval_id,
                    None,
                    async move {
                        tokio::task::spawn_blocking(move || {
                            let preview_binding = Binding {
                                binding_id: rec.binding_id.clone(),
                                thread_uuid: rec.thread_uuid.clone(),
                                label: "launch-precheck".to_string(),
                                delivered_seq: 0,
                                acked_seq: 0,
                            };
                            auth.materialize(
                                &rec.approval_id,
                                &preview_binding,
                                &preview_work_area,
                            )
                            .map_err(HostError::from)?;
                            Ok::<_, HostError>(())
                        })
                        .await
                        .map_err(|err| HostError::Io {
                            operation: "spawn_blocking(materialize_precheck)".to_string(),
                            reason: err.to_string(),
                        })?
                    },
                )
                .await
                .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
            }

            let decision = loop {
                run_enrollment_recheck_within_deadline(
                    startup_deadline,
                    &trusted_binding_id,
                    &record.approval_id,
                    None,
                    &recheck_enrollment,
                )
                .await?;

                let prep = {
                    let launcher_clone = Arc::clone(&launcher);
                    let auth = Arc::clone(&authority);
                    let roots = canonical_sessions_root.clone();
                    let rec = record.clone();
                    let b_id = trusted_binding_id.clone();
                    let t_id = trusted_thread_uuid.clone();
                    run_within_startup_deadline(
                        startup_deadline,
                        &record.approval_id,
                        None,
                        async move {
                            tokio::task::spawn_blocking(move || {
                                launcher_clone
                                    .try_prepare_fresh_launch(&auth, &roots, &rec, &b_id, &t_id)
                            })
                            .await
                            .map_err(|err| HostError::Io {
                                operation: "spawn_blocking(try_prepare_fresh_launch)".to_string(),
                                reason: err.to_string(),
                            })?
                        },
                    )
                    .await
                    .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?
                };

                match prep {
                    TryPrepareFreshLaunch::LockBusy => {
                        run_enrollment_recheck_within_deadline(
                            startup_deadline,
                            &trusted_binding_id,
                            &record.approval_id,
                            None,
                            &recheck_enrollment,
                        )
                        .await?;
                        let Some(remaining) =
                            startup_deadline.checked_duration_since(Instant::now())
                        else {
                            return Err(format_start_host_error(
                                &trusted_binding_id,
                                &startup_timeout_error(&record.approval_id, None),
                            ));
                        };
                        tokio::time::sleep(remaining.min(APPROVED_HOST_STARTUP_POLL_INTERVAL))
                            .await;
                    }
                    TryPrepareFreshLaunch::AlreadyReady => {
                        break FreshLaunchDecision::AlreadyReady;
                    }
                    TryPrepareFreshLaunch::WaitExisting {
                        spawned_child_id,
                        tracked_pid,
                    } => {
                        break FreshLaunchDecision::WaitExisting {
                            spawned_child_id,
                            tracked_pid,
                        };
                    }
                    TryPrepareFreshLaunch::ReadyToSpawn(permit) => {
                        // 1. Re-verify `startup_deadline` and caller enrollment off the async
                        //    executor (bounded by `startup_deadline`) while the live request owns
                        //    `permit` without moving `permit` into the background task.
                        if Instant::now() >= startup_deadline {
                            drop(permit);
                            return Err(format_start_host_error(
                                &trusted_binding_id,
                                &startup_timeout_error(&record.approval_id, None),
                            ));
                        }
                        if let Err(enroll_err) = run_enrollment_recheck_within_deadline(
                            startup_deadline,
                            &trusted_binding_id,
                            &record.approval_id,
                            None,
                            &recheck_enrollment,
                        )
                        .await
                        {
                            drop(permit);
                            return Err(enroll_err);
                        }

                        // 2. Persist the durable `Pending` launch intent off-thread bounded by
                        //    `startup_deadline` with zero process-spawn capability in the
                        //    background task, then revalidate deadline and enrollment on the live
                        //    request before `command.spawn()`.
                        let (permit, intent) = persist_pending_launch_intent_within_deadline(
                            startup_deadline,
                            &canonical_sessions_root,
                            &record,
                            permit,
                        )
                        .await
                        .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;

                        if Instant::now() >= startup_deadline {
                            mark_launch_intent_unknown_with_permit_best_effort(
                                &canonical_sessions_root,
                                &record,
                                permit,
                                intent,
                                "startup deadline expired after persisting pending launch intent before process spawn",
                            )
                            .await;
                            return Err(format_start_host_error(
                                &trusted_binding_id,
                                &startup_timeout_error(&record.approval_id, None),
                            ));
                        }
                        if let Err(enroll_err) = run_enrollment_recheck_within_deadline(
                            startup_deadline,
                            &trusted_binding_id,
                            &record.approval_id,
                            None,
                            &recheck_enrollment,
                        )
                        .await
                        {
                            mark_launch_intent_unknown_with_permit_best_effort(
                                &canonical_sessions_root,
                                &record,
                                permit,
                                intent,
                                "enrollment recheck failed or timed out after persisting pending launch intent before process spawn",
                            )
                            .await;
                            return Err(enroll_err);
                        }

                        let spawned = launcher
                            .spawn_prepared_host_after_intent_locked(
                                startup_deadline,
                                &canonical_sessions_root,
                                &record,
                                permit,
                                intent,
                            )
                            .await
                            .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
                        break spawned;
                    }
                }
            };

            match decision {
                FreshLaunchDecision::AlreadyReady => {
                    run_within_startup_deadline(
                        startup_deadline,
                        &record.approval_id,
                        Some(HostPhase::Ready),
                        verify_live_authenticated_ready(
                            &authority,
                            &canonical_sessions_root,
                            &record,
                            &trusted_binding_id,
                            &trusted_thread_uuid,
                        ),
                    )
                    .await
                    .map_err(|err| format_start_host_error(&trusted_binding_id, &err))?;
                }
                FreshLaunchDecision::WaitExisting {
                    spawned_child_id,
                    tracked_pid,
                } => {
                    let wait_res = run_within_startup_deadline(
                        startup_deadline,
                        &record.approval_id,
                        None,
                        wait_for_authenticated_ready(
                            &launcher,
                            &authority,
                            &canonical_sessions_root,
                            &record,
                            &trusted_binding_id,
                            &trusted_thread_uuid,
                            AuthenticatedReadyWait {
                                startup_deadline,
                                spawned_child_id,
                                tracked_pid,
                            },
                        ),
                    )
                    .await;
                    if let Err(err) = wait_res {
                        let diag = err.to_string();
                        mark_launch_intent_unknown_best_effort(
                            &canonical_sessions_root,
                            &record,
                            &diag,
                        )
                        .await;
                        return Err(format_start_host_error(&trusted_binding_id, &err));
                    }
                }
                FreshLaunchDecision::Spawned {
                    spawned_child_id,
                    tracked_pid,
                } => {
                    let wait_res = run_within_startup_deadline(
                        startup_deadline,
                        &record.approval_id,
                        None,
                        wait_for_authenticated_ready(
                            &launcher,
                            &authority,
                            &canonical_sessions_root,
                            &record,
                            &trusted_binding_id,
                            &trusted_thread_uuid,
                            AuthenticatedReadyWait {
                                startup_deadline,
                                spawned_child_id: Some(spawned_child_id),
                                tracked_pid,
                            },
                        ),
                    )
                    .await;
                    if let Err(err) = wait_res {
                        let diag = err.to_string();
                        mark_launch_intent_unknown_best_effort(
                            &canonical_sessions_root,
                            &record,
                            &diag,
                        )
                        .await;
                        return Err(format_start_host_error(&trusted_binding_id, &err));
                    }
                }
            }
        }
    }

    drop(_approval_guard);

    // 3. Final enrollment recheck immediately before dispatching the task after any startup await.
    run_enrollment_recheck_within_deadline(
        startup_deadline,
        &trusted_binding_id,
        &record.approval_id,
        Some(HostPhase::Ready),
        &recheck_enrollment,
    )
    .await?;

    execute_start_approved_task(
        authority,
        canonical_sessions_root,
        trusted_binding_id,
        trusted_thread_uuid,
        params,
    )
    .await
}
