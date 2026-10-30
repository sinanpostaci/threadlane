use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use threadlane_authority::{ApprovalRecord, AuthorityStore};
use threadlane_desktop_bridge::{ActivityPage, DesktopBridge};
use threadlane_host::{
    is_host_lock_held, read_host_manifest_optional, validate_approval_id, validate_identifier,
    validate_sessions_root, verify_existing_session_dir, ApprovedHostClient, HostError, HostPhase,
    HostSessionSummary, TaskStateSnapshot, TASK_RECEIPTS_FILENAME, WORK_AREA_SUBDIR,
};
use tokio::process::{Child, Command};

use crate::{
    verify_snapshot_binding_for_approval, verify_snapshot_recheck, AppError, DesktopControlState,
    VerifiedBindingContext,
};

/// Canonical binary name for the separately owned per-approval local execution host.
pub const HOST_BINARY_NAME: &str = "threadlane-host";

/// Default bounded readiness poll timeout (`15s`) when launching or attaching to a starting host.
pub const DEFAULT_HOST_READINESS_TIMEOUT: Duration = Duration::from_secs(15);

/// Maximum permitted readiness poll timeout (`15s`).
pub const MAX_HOST_READINESS_TIMEOUT: Duration = Duration::from_secs(15);

/// Default sleep interval (`50ms`) between readiness poll iterations.
pub const DEFAULT_HOST_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Default activity page size (`20`) for `desktop_approved_activity`.
pub const DEFAULT_APPROVED_ACTIVITY_LIMIT: usize = 20;

/// Maximum permitted activity page size (`20`) for `desktop_approved_activity`.
pub const MAX_APPROVED_ACTIVITY_LIMIT: usize = 20;

/// Maximum permitted byte length for an optional pagination cursor string.
const MAX_ACTIVITY_CURSOR_LEN: usize = 256;

/// Non-blocking child exit status observed by [`HostControlHub::poll_child_reaper`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReapedChildState {
    /// No child process has been spawned for this approval by the current desktop instance.
    NotTracked,
    /// Spawned child process is still running (`try_wait` returned `Ok(None)`).
    Running {
        /// OS process ID of the running child.
        pid: u32,
    },
    /// Spawned child process has exited and was reaped via non-blocking `try_wait`.
    Exited {
        /// OS process ID of the exited child.
        pid: u32,
        /// Exit status reported by the OS.
        status: ExitStatus,
    },
    /// Non-blocking `try_wait` encountered an OS error while probing the child.
    ReapError {
        /// OS process ID of the tracked child.
        pid: u32,
    },
}

struct ChildReaperEntry {
    child: Option<Child>,
    pid: u32,
    last_exit_status: Option<ExitStatus>,
}

/// Process-local coordinator for per-approval async start serialization and non-blocking
/// child-process exit reaping.
///
/// Never holds a [`threadlane::SupervisorHandle`] or `host.lock`; dropping [`HostControlHub`]
/// or closing the desktop GUI leaves any running `threadlane-host` child process alive in its
/// own Unix process group (`kill_on_drop(false)`).
pub struct HostControlHub {
    start_locks: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
    children: Mutex<BTreeMap<String, ChildReaperEntry>>,
    readiness_timeout: Duration,
    poll_interval: Duration,
}

impl std::fmt::Debug for HostControlHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tracked_count = self
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        f.debug_struct("HostControlHub")
            .field("tracked_children", &tracked_count)
            .field("readiness_timeout", &self.readiness_timeout)
            .field("poll_interval", &self.poll_interval)
            .finish()
    }
}

impl Default for HostControlHub {
    fn default() -> Self {
        Self::new()
    }
}

impl HostControlHub {
    /// Constructs a new [`HostControlHub`] with [`DEFAULT_HOST_READINESS_TIMEOUT`] (`15s`) and
    /// [`DEFAULT_HOST_POLL_INTERVAL`] (`50ms`).
    pub fn new() -> Self {
        Self {
            start_locks: Mutex::new(BTreeMap::new()),
            children: Mutex::new(BTreeMap::new()),
            readiness_timeout: DEFAULT_HOST_READINESS_TIMEOUT,
            poll_interval: DEFAULT_HOST_POLL_INTERVAL,
        }
    }

    /// Constructs a [`HostControlHub`] with custom poll timing, clamping `readiness_timeout`
    /// to at most [`MAX_HOST_READINESS_TIMEOUT`] (`15s`).
    #[cfg(test)]
    pub fn with_poll_timing(readiness_timeout: Duration, poll_interval: Duration) -> Self {
        let clamped_timeout = readiness_timeout
            .min(MAX_HOST_READINESS_TIMEOUT)
            .max(Duration::from_millis(10));
        let clamped_interval = poll_interval
            .min(clamped_timeout)
            .max(Duration::from_millis(5));
        Self {
            start_locks: Mutex::new(BTreeMap::new()),
            children: Mutex::new(BTreeMap::new()),
            readiness_timeout: clamped_timeout,
            poll_interval: clamped_interval,
        }
    }

    /// Returns the per-approval async mutex used to serialize `desktop_start_approved_session`
    /// calls within this desktop process.
    pub fn approval_start_mutex(&self, approval_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.start_locks.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(
            guard
                .entry(approval_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Non-blockingly polls `try_wait()` across all tracked child handles to reap exited
    /// processes without blocking or killing running hosts.
    pub fn reap_all_finished(&self) {
        let mut guard = self.children.lock().unwrap_or_else(|e| e.into_inner());
        for entry in guard.values_mut() {
            if let Some(ref mut child) = entry.child {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        entry.last_exit_status = Some(status);
                        entry.child = None;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        entry.child = None;
                    }
                }
            }
        }
    }

    /// Non-blockingly polls `try_wait()` for `approval_id` and returns its [`ReapedChildState`].
    pub fn poll_child_reaper(&self, approval_id: &str) -> ReapedChildState {
        let mut guard = self.children.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = guard.get_mut(approval_id) else {
            return ReapedChildState::NotTracked;
        };
        let pid = entry.pid;
        if let Some(ref mut child) = entry.child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    entry.last_exit_status = Some(status);
                    entry.child = None;
                    ReapedChildState::Exited { pid, status }
                }
                Ok(None) => ReapedChildState::Running { pid },
                Err(_) => {
                    entry.child = None;
                    ReapedChildState::ReapError { pid }
                }
            }
        } else if let Some(status) = entry.last_exit_status {
            ReapedChildState::Exited { pid, status }
        } else {
            ReapedChildState::ReapError { pid }
        }
    }

    /// Spawns a detached `threadlane-host` process in its own Unix process group with
    /// `stdin`, `stdout`, and `stderr` attached to `/dev/null` and `kill_on_drop(false)`,
    /// registering the child handle in the non-blocking reaper table.
    pub fn spawn_detached_host_process(
        &self,
        approval_id: &str,
        canonical_executable: &Path,
        serve_args: &[OsString],
    ) -> Result<u32, AppError> {
        ensure_host_platform_supported()?;
        validate_approval_id(approval_id).map_err(AppError::from_host_error)?;
        validate_canonical_regular_executable(canonical_executable)?;

        #[cfg(unix)]
        {
            let mut cmd = Command::new(canonical_executable);
            cmd.args(serve_args)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(false)
                .process_group(0);

            let child = cmd.spawn().map_err(|_| {
                AppError::new(
                    "host_spawn_failed",
                    "Failed to spawn detached threadlane-host process.",
                )
            })?;
            let pid = child.id().ok_or_else(|| {
                AppError::new(
                    "host_spawn_failed",
                    "Spawned threadlane-host process did not report a valid process identifier.",
                )
            })?;

            let mut guard = self.children.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(mut previous) = guard.remove(approval_id) {
                if let Some(ref mut old_child) = previous.child {
                    let _ = old_child.try_wait();
                }
            }
            guard.insert(
                approval_id.to_string(),
                ChildReaperEntry {
                    child: Some(child),
                    pid,
                    last_exit_status: None,
                },
            );
            Ok(pid)
        }

        #[cfg(not(unix))]
        {
            let _ = (approval_id, canonical_executable, serve_args);
            Err(AppError::new(
                "unsupported_platform",
                "Detached per-approval host process lifecycle is not yet supported on this platform.",
            ))
        }
    }
}

impl AppError {
    /// Maps a [`HostError`] to a bounded static [`AppError`] without leaking raw error `Debug`
    /// strings, filesystem paths, executable paths, supervisor bearer tokens, or raw stderr.
    pub(crate) fn from_host_error(err: HostError) -> Self {
        let (code, message) = match err {
            HostError::Validation { field, .. } => {
                let msg = match field.as_str() {
                    "approval_id" => {
                        "Approval identifier must match apr_<32 lowercase hex characters>."
                    }
                    "task_id" => {
                        "Task identifier is required and must be a valid bounded identifier."
                    }
                    "control_binding_id" | "session_binding_id" => {
                        "Conversation binding identifier is invalid."
                    }
                    "thread_uuid" | "native_thread_uuid" => {
                        "Thread UUID must be a canonical lowercase non-nil UUID."
                    }
                    "expected_revision" => "Expected revision must be a positive integer.",
                    _ => "Host session request failed validation.",
                };
                ("validation", msg)
            }
            HostError::ApprovalNotFound { .. } => (
                "approval_not_found",
                "Requested approval artifact was not found.",
            ),
            HostError::UnknownTask { .. } => (
                "unknown_task",
                "Requested task identifier was not found in the approved plan.",
            ),
            HostError::ScopeMismatch { field, .. } => match field.as_str() {
                "thread_uuid" => (
                    "scope_mismatch",
                    "Runtime binding thread UUID does not match the approved thread UUID.",
                ),
                "control_binding_id" => (
                    "scope_mismatch",
                    "Approved plan binding does not match the active runtime conversation binding.",
                ),
                _ => (
                    "scope_mismatch",
                    "Host session binding scope did not match the approved execution context.",
                ),
            },
            HostError::DependencyInputsUnverified { .. } => (
                "dependency_inputs_unverified",
                "Dependent task cannot run until upstream task outputs are verified.",
            ),
            HostError::NativeRouteRequired { .. } => (
                "native_route_required",
                "Task targets manual native Codex routing and is not a headless worker task.",
            ),
            HostError::TaskNotStarted { .. } => (
                "not_started",
                "Requested approved task has not been started yet.",
            ),
            HostError::UnpublishedOrSupersededPlan { .. } => (
                "unpublished_or_superseded_plan",
                "Approved source plan revision is no longer active published and cannot be executed.",
            ),
            HostError::LockBusy { .. } => (
                "lock_busy",
                "Approved execution host session is already locked by an active host process.",
            ),
            HostError::HostNotReady { .. } => (
                "host_not_ready",
                "Approved execution host session is not running or has not reached authenticated ready state.",
            ),
            HostError::StaleRevision { .. } => (
                "stale_revision",
                "Worker task revision changed since this request was prepared; refresh task state.",
            ),
            HostError::IdempotencyConflict { .. } => (
                "idempotency_conflict",
                "Task command key conflicted with an existing session operation.",
            ),
            HostError::InvalidRunState { .. } => (
                "invalid_run_state",
                "Operation is not valid for the task's current execution state.",
            ),
            HostError::SecurityViolation { .. } => (
                "security_violation",
                "Host session directory or artifact failed security validation.",
            ),
            HostError::CorruptManifest { .. } => (
                "corrupt_manifest",
                "Persisted host session manifest failed integrity verification.",
            ),
            HostError::CorruptReceipts { .. } => (
                "corrupt_receipts",
                "Persisted host task receipts ledger failed integrity verification.",
            ),
            HostError::CorruptSessionState { .. } => (
                "corrupt_session_state",
                "Host session runtime state failed integrity verification.",
            ),
            HostError::Authority { code, .. } => map_authority_code_static(&code),
            HostError::Runtime { .. } => (
                "runtime_error",
                "Host session runtime supervisor operation failed.",
            ),
            HostError::Io { .. } => ("io_error", "Failed to read or write host session state."),
        };
        Self {
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

fn map_authority_code_static(code: &str) -> (&'static str, &'static str) {
    match code {
        "validation" => ("validation", "Workflow authority validation failed."),
        "plan_not_found" => (
            "plan_not_found",
            "Requested plan revision was not found in the plan ledger.",
        ),
        "approval_not_found" => (
            "approval_not_found",
            "Requested approval artifact was not found.",
        ),
        "binding_mismatch" => (
            "binding_mismatch",
            "Plan binding identifier does not match the approval binding.",
        ),
        "thread_uuid_mismatch" => (
            "thread_uuid_mismatch",
            "Runtime binding thread UUID does not match the approved thread UUID.",
        ),
        "unpublished_or_superseded_plan" => (
            "unpublished_or_superseded_plan",
            "Only active published plan revisions can be executed.",
        ),
        "stale_registry_revision" => (
            "stale_registry_revision",
            "Registry revision changed since this plan was approved.",
        ),
        "plan_digest_mismatch" => (
            "plan_digest_mismatch",
            "Compiled plan digest does not match the approved plan snapshot.",
        ),
        "unknown_task" => (
            "unknown_task",
            "Requested task identifier does not exist in the approved plan.",
        ),
        "unsupported_task_adapter" => (
            "unsupported_task_adapter",
            "Selected task uses an adapter that cannot be executed by a worker host.",
        ),
        "invalid_executable" => (
            "invalid_executable",
            "Approved worker tool executable is no longer a valid local binary.",
        ),
        "invalid_repository" => (
            "invalid_repository",
            "Approved repository path is no longer a valid canonical Git repository.",
        ),
        "invalid_base_commit" => (
            "invalid_base_commit",
            "Approved base commit could not be verified in the target repository.",
        ),
        "invalid_write_scope" | "unapproved_task_write_scope" => (
            "invalid_write_scope",
            "Approved task write scope failed validation.",
        ),
        "invalid_work_area" => (
            "invalid_work_area",
            "Host session work area directory failed security or non-overlap validation.",
        ),
        "workers_config_invalid" => (
            "workers_config_invalid",
            "Synthesized worker configuration failed validation.",
        ),
        "security_violation" => (
            "security_violation",
            "Authority store or session target path failed security validation.",
        ),
        "corrupt_store" => (
            "corrupt_store",
            "Persisted authority store failed integrity verification.",
        ),
        _ => (
            "authority_error",
            "Workflow authority verification failed for this host session.",
        ),
    }
}

/// Verifies that the current platform supports detached Unix process-group lifecycle management.
pub fn ensure_host_platform_supported() -> Result<(), AppError> {
    #[cfg(unix)]
    {
        Ok(())
    }
    #[cfg(not(unix))]
    {
        Err(AppError::new(
            "unsupported_platform",
            "Detached per-approval host process lifecycle is not yet supported on this platform.",
        ))
    }
}

/// Constructs the exact argument list for `threadlane-host serve`:
/// `["serve", "--registry-dir", <global>, "--plans-dir", <global>, "--authority-dir", <global>, "--sessions-dir", <private>, "--approval-id", <validated>]`.
///
/// Rejects non-canonical `approval_id` values and relative, empty, or traversal directory paths.
pub fn build_host_serve_args(
    registry_dir: &Path,
    plans_dir: &Path,
    authority_dir: &Path,
    sessions_dir: &Path,
    approval_id: &str,
) -> Result<Vec<OsString>, AppError> {
    if approval_id.trim() != approval_id {
        return Err(AppError::new(
            "validation",
            "Approval identifier must match apr_<32 lowercase hex characters>.",
        ));
    }
    validate_approval_id(approval_id).map_err(AppError::from_host_error)?;

    for dir in [registry_dir, plans_dir, authority_dir, sessions_dir] {
        validate_clean_absolute_dir_arg(dir)?;
    }

    Ok(vec![
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
    ])
}

fn validate_clean_absolute_dir_arg(dir: &Path) -> Result<(), AppError> {
    if dir.as_os_str().is_empty() || !dir.is_absolute() {
        return Err(AppError::new(
            "security_violation",
            "Host store directory arguments must be absolute paths.",
        ));
    }
    if dir
        .as_os_str()
        .to_string_lossy()
        .split(std::path::is_separator)
        .any(|component| component == ".")
    {
        return Err(AppError::new(
            "security_violation",
            "Host store directory arguments must not contain `.` path components.",
        ));
    }
    for comp in dir.components() {
        if matches!(comp, Component::CurDir | Component::ParentDir) {
            return Err(AppError::new(
                "security_violation",
                "Host store directory arguments must not contain traversal components.",
            ));
        }
    }
    Ok(())
}

/// Resolves the `threadlane-host` binary from the trusted native CLI `--host-executable` option,
/// or falls back to a packaged sibling binary next to `current_exe()` or `PATH` lookup.
///
/// Canonicalizes symlinks (such as NixOS store wrappers) and verifies that the final resolved
/// target is an existing, non-symlink regular executable file.
pub fn resolve_host_executable(
    configured_host_executable: Option<&Path>,
) -> Result<PathBuf, AppError> {
    let sibling_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let path_var = std::env::var_os("PATH");
    resolve_host_executable_with_env(
        configured_host_executable,
        sibling_dir.as_deref(),
        path_var.as_deref(),
    )
}

/// Testable resolver for `threadlane-host` using explicit sibling and `PATH` environment inputs.
pub fn resolve_host_executable_with_env(
    configured_host_executable: Option<&Path>,
    sibling_dir: Option<&Path>,
    path_var: Option<&OsStr>,
) -> Result<PathBuf, AppError> {
    if let Some(explicit_path) = configured_host_executable {
        if explicit_path.as_os_str().is_empty() || !explicit_path.is_absolute() {
            return Err(AppError::new(
                "invalid_host_executable",
                "Configured threadlane-host executable must be an absolute path to a regular executable binary.",
            ));
        }
        for comp in explicit_path.components() {
            if matches!(comp, Component::CurDir | Component::ParentDir) {
                return Err(AppError::new(
                    "invalid_host_executable",
                    "Configured threadlane-host executable path must not contain traversal components.",
                ));
            }
        }
        return canonicalize_and_validate_regular_executable(explicit_path);
    }

    if let Some(dir) = sibling_dir {
        if dir.is_absolute()
            && !dir
                .components()
                .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
        {
            let sibling_candidate = dir.join(HOST_BINARY_NAME);
            if let Ok(resolved) = canonicalize_and_validate_regular_executable(&sibling_candidate) {
                return Ok(resolved);
            }
        }
    }

    if let Some(paths) = path_var {
        for dir in std::env::split_paths(paths) {
            if dir.as_os_str().is_empty() || !dir.is_absolute() {
                continue;
            }
            if dir
                .components()
                .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
            {
                continue;
            }
            let candidate = dir.join(HOST_BINARY_NAME);
            if let Ok(resolved) = canonicalize_and_validate_regular_executable(&candidate) {
                return Ok(resolved);
            }
        }
    }

    Err(AppError::new(
        "host_executable_unavailable",
        "Unable to locate a valid canonical threadlane-host executable.",
    ))
}

fn canonicalize_and_validate_regular_executable(candidate: &Path) -> Result<PathBuf, AppError> {
    let canonical = fs::canonicalize(candidate).map_err(|_| {
        AppError::new(
            "invalid_host_executable",
            "Configured threadlane-host executable could not be resolved to a valid canonical regular executable.",
        )
    })?;
    validate_canonical_regular_executable(&canonical)?;
    Ok(canonical)
}

fn validate_canonical_regular_executable(canonical: &Path) -> Result<(), AppError> {
    if !canonical.is_absolute() {
        return Err(AppError::new(
            "invalid_host_executable",
            "Resolved threadlane-host executable path must be absolute.",
        ));
    }
    let meta = fs::symlink_metadata(canonical).map_err(|_| {
        AppError::new(
            "invalid_host_executable",
            "Configured threadlane-host executable could not be inspected.",
        )
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(AppError::new(
            "invalid_host_executable",
            "Resolved threadlane-host executable must be a regular non-symlink file.",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(AppError::new(
                "invalid_host_executable",
                "Resolved threadlane-host file does not have executable permissions.",
            ));
        }
    }
    Ok(())
}

fn sanitize_session_summary(mut summary: HostSessionSummary) -> HostSessionSummary {
    if let Some(ref raw_err) = summary.last_error {
        summary.last_error = Some(sanitize_manifest_last_error(raw_err));
    }
    summary
}

fn sanitize_manifest_last_error(raw: &str) -> String {
    let code_prefix = raw
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']'))
        .map(|(code, _)| code.trim())
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 64
                && code
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        });

    if let Some(code) = code_prefix {
        let (_, static_msg) = map_authority_or_host_code_static(code);
        format!("[{code}] {static_msg}")
    } else {
        "Approved execution host session recorded a startup or runtime error.".to_string()
    }
}

fn map_authority_or_host_code_static(code: &str) -> (&'static str, &'static str) {
    match code {
        "lock_busy" => (
            "lock_busy",
            "Approved execution host session is already locked by an active host process.",
        ),
        "host_not_ready" => (
            "host_not_ready",
            "Approved execution host session is not running or has not reached authenticated ready state.",
        ),
        "corrupt_manifest" => (
            "corrupt_manifest",
            "Persisted host session manifest failed integrity verification.",
        ),
        "corrupt_receipts" => (
            "corrupt_receipts",
            "Persisted host task receipts ledger failed integrity verification.",
        ),
        "corrupt_session_state" => (
            "corrupt_session_state",
            "Host session runtime state failed integrity verification.",
        ),
        "scope_mismatch" => (
            "scope_mismatch",
            "Host session binding scope did not match the approved execution context.",
        ),
        "runtime_error" => (
            "runtime_error",
            "Host session runtime supervisor operation failed.",
        ),
        "io" | "io_error" => ("io_error", "Failed to read or write host session state."),
        other => map_authority_code_static(other),
    }
}

fn verify_materialization_for_record(
    authority: &AuthorityStore,
    sessions_dir: &Path,
    record: &ApprovalRecord,
) -> Result<(), AppError> {
    let session_dir = sessions_dir.join(&record.approval_id);
    let existing_binding_id = if session_dir.exists() {
        let verified_dir = verify_existing_session_dir(sessions_dir, &record.approval_id)
            .map_err(AppError::from_host_error)?;
        read_host_manifest_optional(&verified_dir, record)
            .map_err(AppError::from_host_error)?
            .and_then(|m| m.session_binding_id)
    } else {
        None
    };

    let preview_binding_id =
        existing_binding_id.unwrap_or_else(|| "bnd_bootstrap_preview".to_string());
    let preview_binding = serde_json::from_value(serde_json::json!({
        "binding_id": preview_binding_id,
        "thread_uuid": record.thread_uuid,
        "label": "bootstrap-preview",
        "delivered_seq": 0,
        "acked_seq": 0,
    }))
    .map_err(|_| {
        AppError::new(
            "internal_task_error",
            "Failed to construct session binding preview for materialization verification.",
        )
    })?;

    let preview_work_area = session_dir.join(WORK_AREA_SUBDIR);
    let materialized = authority
        .materialize(&record.approval_id, &preview_binding, &preview_work_area)
        .map_err(AppError::from_authority_error)?;

    if materialized.approval_id != record.approval_id
        || materialized.subject_digest != record.subject_digest
    {
        return Err(AppError::new(
            "corrupt_session_state",
            "Materialized plan identity did not match the persisted approval record.",
        ));
    }

    Ok(())
}

async fn verify_caller_and_load_approval(
    bridge: &DesktopBridge,
    control: &DesktopControlState,
    approval_id: &str,
    check_materialization: bool,
) -> Result<(ApprovalRecord, VerifiedBindingContext), AppError> {
    if approval_id.trim() != approval_id {
        return Err(AppError::new(
            "validation",
            "Approval identifier must match apr_<32 lowercase hex characters>.",
        ));
    }
    validate_approval_id(approval_id).map_err(AppError::from_host_error)?;

    let initial_snapshot = bridge.snapshot(None).await.map_err(|_| {
        AppError::new(
            "bridge_unavailable",
            "Unable to inspect active runtime attachment snapshot.",
        )
    })?;

    let authority = Arc::clone(&control.authority);
    let registry_dir = control.registry.config_dir().to_path_buf();
    let plans_dir = control.plans.dir().to_path_buf();
    let sessions_dir = control.sessions_dir.clone();
    let lookup_id = approval_id.to_string();

    let record =
        tauri::async_runtime::spawn_blocking(move || -> Result<ApprovalRecord, AppError> {
            let record = authority
                .get_for_operator(&lookup_id)
                .map_err(AppError::from_authority_error)?;

            validate_sessions_root(
                &sessions_dir,
                &[
                    ("registry_dir", registry_dir.as_path()),
                    ("plans_dir", plans_dir.as_path()),
                    ("authority_dir", authority.dir()),
                    ("repository_path", record.repository_path.as_path()),
                ],
            )
            .map_err(AppError::from_host_error)?;

            if check_materialization {
                verify_materialization_for_record(&authority, &sessions_dir, &record)?;
            }

            Ok(record)
        })
        .await
        .map_err(|_| AppError::internal_join_error())??;

    let captured_ctx = verify_snapshot_binding_for_approval(
        &initial_snapshot,
        &record.binding_id,
        &record.thread_uuid,
    )?;

    Ok((record, captured_ctx))
}

async fn recheck_caller_bridge_snapshot(
    bridge: &DesktopBridge,
    captured_ctx: &VerifiedBindingContext,
) -> Result<(), AppError> {
    let rechecked_snapshot = bridge.snapshot(None).await.map_err(|_| {
        AppError::new(
            "bridge_unavailable",
            "Unable to re-verify active runtime attachment snapshot.",
        )
    })?;
    verify_snapshot_recheck(captured_ctx, &rechecked_snapshot)
}

async fn verify_authenticated_ready_summary(
    client: &ApprovedHostClient,
    approval_id: &str,
) -> Result<HostSessionSummary, AppError> {
    let summary = client
        .session_summary(approval_id)
        .map_err(AppError::from_host_error)?;

    if summary.phase != HostPhase::Ready || !summary.gates.supervisor_authenticated_ready {
        return Err(AppError::new(
            "host_not_ready",
            "Approved execution host session is not running or has not reached authenticated ready state.",
        ));
    }

    let page = client
        .list_activity(approval_id, None, None, Some(1), None)
        .await
        .map_err(AppError::from_host_error)?;

    if summary.session_binding_id.as_deref() != Some(page.binding_id.as_str()) {
        return Err(AppError::new(
            "scope_mismatch",
            "Host session binding scope did not match the approved execution context.",
        ));
    }

    Ok(sanitize_session_summary(summary))
}

async fn poll_until_authenticated_ready(
    control: &DesktopControlState,
    client: &ApprovedHostClient,
    record: &ApprovalRecord,
    spawned_by_this_call: bool,
) -> Result<HostSessionSummary, AppError> {
    let timeout = control
        .host_control
        .readiness_timeout
        .min(MAX_HOST_READINESS_TIMEOUT);
    let interval = control.host_control.poll_interval;
    let deadline = Instant::now() + timeout;
    let session_dir = control.sessions_dir.join(&record.approval_id);

    loop {
        let child_state = control.host_control.poll_child_reaper(&record.approval_id);
        let child_terminated = match child_state {
            ReapedChildState::NotTracked => false,
            ReapedChildState::Running { pid } => pid == 0,
            ReapedChildState::Exited { pid, status } => pid > 0 || !status.success(),
            ReapedChildState::ReapError { pid } => pid > 0,
        };

        if session_dir.exists() {
            let verified_dir =
                verify_existing_session_dir(&control.sessions_dir, &record.approval_id)
                    .map_err(AppError::from_host_error)?;
            let manifest_opt = read_host_manifest_optional(&verified_dir, record)
                .map_err(AppError::from_host_error)?;
            let lock_held = is_host_lock_held(&verified_dir).map_err(AppError::from_host_error)?;

            if let Some(ref manifest) = manifest_opt {
                if manifest.phase == HostPhase::Failed && !lock_held {
                    return Err(AppError::new(
                        "host_startup_failed",
                        "Approved execution host session failed during startup.",
                    ));
                }
                if manifest.phase == HostPhase::Stopped && !lock_held {
                    return Err(AppError::new(
                        "host_not_ready",
                        "Approved execution host session stopped before reaching ready state.",
                    ));
                }
            }

            if lock_held {
                if let Ok(ready_summary) =
                    verify_authenticated_ready_summary(client, &record.approval_id).await
                {
                    return Ok(ready_summary);
                }
            } else if (spawned_by_this_call && child_terminated) || !spawned_by_this_call {
                return Err(AppError::new(
                    "host_startup_failed",
                    "Approved execution host process exited before reaching authenticated ready state.",
                ));
            }
        } else if spawned_by_this_call && child_terminated {
            return Err(AppError::new(
                "host_startup_failed",
                "Approved execution host process exited before initializing session state.",
            ));
        }

        if Instant::now() >= deadline {
            return Err(AppError::new(
                "host_not_ready",
                "Timed out waiting for approved execution host to reach authenticated ready state.",
            ));
        }

        tokio::time::sleep(interval).await;
    }
}

/// Operator-only desktop command implementation for `desktop_start_approved_session`.
///
/// - Verifies fresh connected runtime attachment and non-demo binding matching `record.binding_id`
///   and `record.thread_uuid` (initial check + recheck before action).
/// - Verifies source plan publication and side-effect-free materialization via `AuthorityStore::materialize`.
/// - Serializes per-approval starts via an async mutex and OS-level `host.lock`.
/// - Attaches without spawning if an active authenticated host is already `Ready` (proven via
///   `ApprovedHostClient::list_activity(limit = 1)`).
/// - Otherwise spawns `threadlane-host serve` in its own Unix process group with stdio null and
///   `kill_on_drop(false)` and polls bounded readiness (`<= 15s`).
pub(crate) async fn start_approved_session(
    bridge: &DesktopBridge,
    control: &DesktopControlState,
    approval_id: String,
) -> Result<HostSessionSummary, AppError> {
    ensure_host_platform_supported()?;
    control.host_control.reap_all_finished();

    let (record, captured_ctx) =
        verify_caller_and_load_approval(bridge, control, &approval_id, true).await?;

    let start_mutex = control
        .host_control
        .approval_start_mutex(&record.approval_id);
    let _start_guard = start_mutex.lock().await;

    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
    control.host_control.reap_all_finished();

    let client = ApprovedHostClient::new(
        Arc::clone(&control.authority),
        &control.sessions_dir,
        &record.binding_id,
        &record.thread_uuid,
    )
    .map_err(AppError::from_host_error)?;

    match verify_existing_session_dir(&control.sessions_dir, &record.approval_id) {
        Ok(session_dir) => {
            let manifest_opt = read_host_manifest_optional(&session_dir, &record)
                .map_err(AppError::from_host_error)?;
            let lock_held = is_host_lock_held(&session_dir).map_err(AppError::from_host_error)?;
            let has_receipts = session_dir.join(TASK_RECEIPTS_FILENAME).exists();

            if lock_held {
                if manifest_opt.as_ref().is_some_and(|m| {
                    m.phase == HostPhase::Ready && m.gates.supervisor_authenticated_ready
                }) {
                    if let Ok(ready_summary) =
                        verify_authenticated_ready_summary(&client, &record.approval_id).await
                    {
                        recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
                        return Ok(ready_summary);
                    }
                }
                let ready_summary =
                    poll_until_authenticated_ready(control, &client, &record, false).await?;
                recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
                return Ok(ready_summary);
            }

            if has_receipts {
                return Err(AppError::new(
                    "host_not_ready",
                    "Approved execution host session has recorded task receipts and is offline; automatic restart of prior work is disabled.",
                ));
            }

            if let Some(manifest) = manifest_opt {
                match manifest.phase {
                    HostPhase::Ready | HostPhase::Bootstrapping => {
                        return Err(AppError::new(
                            "host_not_ready",
                            "Existing host session manifest is not backed by an active authenticated host process.",
                        ));
                    }
                    HostPhase::Failed => {
                        return Err(AppError::new(
                            "host_startup_failed",
                            "Approved execution host session previously failed startup.",
                        ));
                    }
                    HostPhase::Stopped => {}
                }
            }
        }
        Err(HostError::HostNotReady { phase: None, .. }) => {}
        Err(other_err) => return Err(AppError::from_host_error(other_err)),
    }

    let canonical_exe = resolve_host_executable(control.host_executable.as_deref())?;
    let serve_args = build_host_serve_args(
        control.registry.config_dir(),
        control.plans.dir(),
        control.authority.dir(),
        &control.sessions_dir,
        &record.approval_id,
    )?;

    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;

    control.host_control.spawn_detached_host_process(
        &record.approval_id,
        &canonical_exe,
        &serve_args,
    )?;

    let ready_summary = poll_until_authenticated_ready(control, &client, &record, true).await?;
    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
    Ok(ready_summary)
}

/// Operator-only read-only desktop command implementation for `desktop_approved_session`.
///
/// Verifies caller runtime attachment and binding context, reads the session summary via
/// [`ApprovedHostClient`], and requires authenticated supervisor readiness via
/// `ApprovedHostClient::list_activity(limit = 1)` before returning `HostPhase::Ready`.
/// Stopped or offline manifests never report `Ready`.
pub(crate) async fn approved_session(
    bridge: &DesktopBridge,
    control: &DesktopControlState,
    approval_id: String,
) -> Result<HostSessionSummary, AppError> {
    control.host_control.reap_all_finished();

    let (record, captured_ctx) =
        verify_caller_and_load_approval(bridge, control, &approval_id, false).await?;
    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;

    let client = ApprovedHostClient::new(
        Arc::clone(&control.authority),
        &control.sessions_dir,
        &record.binding_id,
        &record.thread_uuid,
    )
    .map_err(AppError::from_host_error)?;

    let mut summary = client
        .session_summary(&record.approval_id)
        .map_err(AppError::from_host_error)?;

    match summary.phase {
        HostPhase::Ready => {
            let verified = verify_authenticated_ready_summary(&client, &record.approval_id).await?;
            recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
            Ok(verified)
        }
        HostPhase::Bootstrapping => {
            summary.gates.supervisor_authenticated_ready = false;
            let session_dir =
                verify_existing_session_dir(&control.sessions_dir, &record.approval_id)
                    .map_err(AppError::from_host_error)?;
            let lock_held = is_host_lock_held(&session_dir).map_err(AppError::from_host_error)?;
            if !lock_held {
                return Err(AppError::new(
                    "host_not_ready",
                    "Approved execution host session is not running or has not reached authenticated ready state.",
                ));
            }
            recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
            Ok(sanitize_session_summary(summary))
        }
        HostPhase::Stopped | HostPhase::Failed => {
            summary.gates.supervisor_authenticated_ready = false;
            recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
            Ok(sanitize_session_summary(summary))
        }
    }
}

/// Operator-only read-only desktop command implementation for `desktop_approved_task`.
///
/// Uses the frozen approval and task IDs and the verified authoritative control binding and
/// thread UUID to query task execution status without starting or mutating any worker or plan.
pub(crate) async fn approved_task(
    bridge: &DesktopBridge,
    control: &DesktopControlState,
    approval_id: String,
    task_id: String,
) -> Result<TaskStateSnapshot, AppError> {
    control.host_control.reap_all_finished();

    if task_id.trim() != task_id {
        return Err(AppError::new(
            "validation",
            "Task identifier is required and must be a valid bounded identifier.",
        ));
    }
    validate_identifier("task_id", &task_id).map_err(AppError::from_host_error)?;

    let (record, captured_ctx) =
        verify_caller_and_load_approval(bridge, control, &approval_id, false).await?;
    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;

    let client = ApprovedHostClient::new(
        Arc::clone(&control.authority),
        &control.sessions_dir,
        &record.binding_id,
        &record.thread_uuid,
    )
    .map_err(AppError::from_host_error)?;

    let snapshot = client
        .get_task(&record.approval_id, &task_id)
        .await
        .map_err(AppError::from_host_error)?;

    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
    Ok(snapshot)
}

/// Operator-only read-only desktop command implementation for `desktop_approved_activity`.
///
/// Enforces `limit <= 20` and queries durable host activity via [`ApprovedHostClient::list_activity`]
/// without calling `pull_events` or `ack_events`.
pub(crate) async fn approved_activity(
    bridge: &DesktopBridge,
    control: &DesktopControlState,
    approval_id: String,
    after_seq: Option<u64>,
    cursor: Option<String>,
    limit: Option<usize>,
) -> Result<ActivityPage, AppError> {
    control.host_control.reap_all_finished();

    let clean_cursor = match cursor {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else if trimmed.len() > MAX_ACTIVITY_CURSOR_LEN
                || trimmed.chars().any(char::is_control)
            {
                return Err(AppError::new(
                    "invalid_input",
                    "Activity pagination cursor is invalid.",
                ));
            } else {
                Some(trimmed.to_string())
            }
        }
        None => None,
    };

    if after_seq.is_some() && clean_cursor.is_some() {
        return Err(AppError::new(
            "invalid_input",
            "Cannot specify both after_seq and cursor.",
        ));
    }

    let effective_limit = match limit {
        None => DEFAULT_APPROVED_ACTIVITY_LIMIT,
        Some(lim) if (1..=MAX_APPROVED_ACTIVITY_LIMIT).contains(&lim) => lim,
        Some(_) => {
            return Err(AppError::new(
                "invalid_input",
                "Approved session activity limit must be between 1 and 20.",
            ));
        }
    };

    let (record, captured_ctx) =
        verify_caller_and_load_approval(bridge, control, &approval_id, false).await?;
    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;

    let client = ApprovedHostClient::new(
        Arc::clone(&control.authority),
        &control.sessions_dir,
        &record.binding_id,
        &record.thread_uuid,
    )
    .map_err(AppError::from_host_error)?;

    let page = client
        .list_activity(
            &record.approval_id,
            after_seq,
            clean_cursor,
            Some(effective_limit),
            None,
        )
        .await
        .map_err(AppError::from_host_error)?;

    recheck_caller_bridge_snapshot(bridge, &captured_ctx).await?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    use clap::Parser;
    use threadlane_authority::{
        compute_authority_core_digest, compute_record_subject_digest, derive_approval_id,
        derive_command_key, derive_profile_id, verify_approval_record_integrity,
        AUTHORITY_POLICY_VERSION, AUTHORITY_SCHEMA_VERSION,
    };
    use threadlane_desktop_bridge::{AttachmentMode, ConnectionState};
    use threadlane_host::{
        prepare_session_dir, write_host_manifest, ApprovedHost, ApprovedHostConfig, HostCli,
        HostCommand, HostSessionLock, HostSessionManifest, TaskExecutionStatus,
        HOST_MANIFEST_FILENAME, RUNTIME_STATE_SUBDIR,
    };
    use threadlane_plans::{
        compile_plan_revision, Difficulty, PlanChange, PlanStatus, PlanTask, WorkflowRole,
    };
    use threadlane_registry::{
        ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition,
        WorkflowProfile,
    };
    use threadlane_workers::{
        resolve_exact_commit_sha, run_sanitized_git, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    };

    const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
    const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    static HOST_TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct ScopedHostTestDir {
        path: PathBuf,
    }

    impl ScopedHostTestDir {
        fn new(label: &str) -> Self {
            let seq = HOST_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let raw_base =
                fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
            let path = raw_base.join(format!(
                "threadlane-desktop-host-ctrl-{label}-{}-{seq}",
                std::process::id()
            ));
            if path.exists() {
                let _ = fs::remove_dir_all(&path);
            }
            fs::create_dir_all(&path).expect("create test temp dir");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 test temp dir");
            Self { path }
        }
    }

    impl Drop for ScopedHostTestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct DesktopHostFixture {
        tmp: ScopedHostTestDir,
        repo_dir: PathBuf,
        base_commit: String,
        fake_agy_exe: PathBuf,
        agy_count_file: PathBuf,
        control_sessions_dir: PathBuf,
        control: DesktopControlState,
    }

    fn write_executable_script(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create script parent");
        }
        let mut file = File::create(path).expect("create script file");
        file.write_all(body.as_bytes()).expect("write script body");
        file.sync_all().expect("sync script file");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 script file");
    }

    fn init_disposable_git_repo(repo_dir: &Path) -> String {
        fs::create_dir_all(repo_dir.join("src")).expect("create repo/src");
        fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
            .expect("write src/lib.rs");

        run_sanitized_git(
            repo_dir,
            &["init", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git init");
        run_sanitized_git(
            repo_dir,
            &["config", "user.email", "test@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config email");
        run_sanitized_git(
            repo_dir,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config name");
        run_sanitized_git(repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            repo_dir,
            &["commit", "-m", "initial commit", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD")
    }

    fn setup_desktop_host_fixture(label: &str) -> DesktopHostFixture {
        let tmp = ScopedHostTestDir::new(label);
        let root = &tmp.path;

        let repo_dir = root.join("repo");
        let base_commit = init_disposable_git_repo(&repo_dir);

        let bin_dir = root.join("bin");
        let fake_agy_exe = bin_dir.join("fake-agy");
        let agy_count_file = root.join("agy-count.txt");
        let script = format!(
            "#!/bin/sh\nset -eu\nc=0\nif [ -f \"{count}\" ]; then c=$(cat \"{count}\"); fi\nc=$((c + 1))\nprintf \"%s\" \"$c\" > \"{count}\"\nexit 0\n",
            count = agy_count_file.display()
        );
        write_executable_script(&fake_agy_exe, &script);

        let registry_dir = root.join("registry-store");
        let plans_dir = root.join("plans-store");
        let authority_dir = root.join("authority-store");
        let sessions_dir = root.join("sessions-store");
        let control_sessions_dir = root.join("control-runtime-sessions");
        fs::create_dir_all(&control_sessions_dir).expect("create control_sessions_dir");
        fs::set_permissions(&control_sessions_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 control_sessions_dir");

        let control =
            DesktopControlState::open(registry_dir, plans_dir, authority_dir, sessions_dir, None)
                .expect("open DesktopControlState");

        control
            .registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity CLI".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fake_agy_exe.clone()),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool-agy");

        control
            .registry
            .apply(
                1,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-codex".to_string(),
                        label: "Native Codex".to_string(),
                        adapter: ToolAdapter::CodexCli,
                        executable_path: None,
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool-codex");

        control
            .registry
            .apply(
                2,
                RegistryChange::UpsertModels {
                    models: vec![
                        ModelDefinition {
                            id: "model-argon".to_string(),
                            tool_id: "tool-agy".to_string(),
                            label: "Gemini 4 Argon High".to_string(),
                            model_arg: "gemini-4-argon-high".to_string(),
                            efforts: vec!["high".to_string()],
                            allow_no_effort: false,
                        },
                        ModelDefinition {
                            id: "model-codex-native".to_string(),
                            tool_id: "tool-codex".to_string(),
                            label: "Native Codex Follower".to_string(),
                            model_arg: "gpt-5-codex".to_string(),
                            efforts: vec!["high".to_string()],
                            allow_no_effort: true,
                        },
                    ],
                },
            )
            .expect("upsert models");

        control
            .registry
            .apply(
                3,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: "profile-main".to_string(),
                        name: "Main M4 Profile".to_string(),
                        assignments: vec![
                            RoleAssignment {
                                role: WorkflowRole::Coder,
                                difficulty: Some(Difficulty::Hard),
                                choice: ModelChoice {
                                    model_id: "model-argon".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                            RoleAssignment {
                                role: WorkflowRole::Follower,
                                difficulty: None,
                                choice: ModelChoice {
                                    model_id: "model-codex-native".to_string(),
                                    effort: Some("high".to_string()),
                                },
                            },
                        ],
                    },
                },
            )
            .expect("upsert profile");

        DesktopHostFixture {
            tmp,
            repo_dir,
            base_commit,
            fake_agy_exe,
            agy_count_file,
            control_sessions_dir,
            control,
        }
    }

    fn default_plan_tasks() -> Vec<PlanTask> {
        vec![
            PlanTask {
                id: "task-root-agy".to_string(),
                title: "Root AGY implementation".to_string(),
                instruction: "Implement root AGY task in src/lib.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-agy".to_string(),
                title: "Dependent AGY task".to_string(),
                instruction: "Follow up after task-root-agy".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-agy".to_string()],
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
            PlanTask {
                id: "task-native-codex".to_string(),
                title: "Manual Codex task".to_string(),
                instruction: "Coordinate in native Codex conversation".to_string(),
                role: WorkflowRole::Follower,
                difficulty: Difficulty::Medium,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/lib.rs".to_string()],
            },
        ]
    }

    fn seed_published_plan_and_approval(
        fixture: &DesktopHostFixture,
        plan_id: &str,
        binding_id: &str,
        thread_uuid: &str,
    ) -> ApprovalRecord {
        let reg_snap = fixture
            .control
            .registry
            .snapshot()
            .expect("registry snapshot");
        let tasks = default_plan_tasks();

        let latest_rev = fixture
            .control
            .plans
            .latest_revision(plan_id)
            .expect("latest plan revision")
            .map(|revision| revision.revision)
            .unwrap_or(0);

        let draft = fixture
            .control
            .plans
            .change_at(
                latest_rev,
                PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: binding_id.to_string(),
                    profile_id: "profile-main".to_string(),
                    profile_revision: reg_snap.revision,
                    summary: format!("Plan {plan_id}"),
                    tasks: tasks.clone(),
                },
                1_700_000_000_000,
            )
            .expect("save draft");

        let published = fixture
            .control
            .plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: plan_id.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish plan");

        let compiled =
            compile_plan_revision(&published, &reg_snap).expect("compile published plan");
        let canonical_repo = fs::canonicalize(&fixture.repo_dir).expect("canonical repo");
        let canonical_agy = fs::canonicalize(&fixture.fake_agy_exe).expect("canonical agy");

        let tasks_json: Vec<serde_json::Value> = compiled
            .tasks
            .iter()
            .map(|ct| {
                if ct.adapter == ToolAdapter::Antigravity {
                    serde_json::json!({
                        "task_id": ct.id,
                        "title": ct.title,
                        "instruction": ct.instruction,
                        "instruction_digest": ct.instruction_digest,
                        "role": ct.role,
                        "difficulty": ct.difficulty,
                        "tool_id": ct.tool_id,
                        "adapter": ct.adapter,
                        "provider": "antigravity",
                        "canonical_executable": canonical_agy,
                        "model_id": ct.choice.model_id,
                        "model_arg": ct.model_arg,
                        "base_model": ct.model_arg,
                        "effort": ct.choice.effort,
                        "cli_model_arg": ct.model_arg,
                        "standalone": false,
                        "file_permissions": "task_files",
                        "depends_on": ct.depends_on,
                        "allowed_write_paths": ct.allowed_write_paths,
                        "task_digest": ct.task_digest,
                        "profile_id": null,
                        "command_key": null,
                    })
                } else {
                    serde_json::json!({
                        "task_id": ct.id,
                        "title": ct.title,
                        "instruction": ct.instruction,
                        "instruction_digest": ct.instruction_digest,
                        "role": ct.role,
                        "difficulty": ct.difficulty,
                        "tool_id": ct.tool_id,
                        "adapter": ct.adapter,
                        "provider": null,
                        "canonical_executable": null,
                        "model_id": ct.choice.model_id,
                        "model_arg": ct.model_arg,
                        "base_model": ct.model_arg,
                        "effort": ct.choice.effort,
                        "cli_model_arg": null,
                        "standalone": null,
                        "file_permissions": null,
                        "depends_on": ct.depends_on,
                        "allowed_write_paths": ct.allowed_write_paths,
                        "task_digest": ct.task_digest,
                        "profile_id": null,
                        "command_key": null,
                    })
                }
            })
            .collect();

        let approved_task_ids: Vec<String> = compiled.tasks.iter().map(|t| t.id.clone()).collect();
        let mut record: ApprovalRecord = serde_json::from_value(serde_json::json!({
            "approval_id": "",
            "subject_digest": "",
            "policy_version": AUTHORITY_POLICY_VERSION,
            "binding_id": binding_id,
            "thread_uuid": thread_uuid,
            "plan_id": compiled.plan_id,
            "plan_revision": compiled.plan_revision,
            "profile_id": compiled.profile_id,
            "registry_revision": compiled.profile_revision,
            "plan_digest": compiled.plan_digest,
            "repository_path": canonical_repo,
            "base_commit": fixture.base_commit.clone(),
            "approved_task_ids": approved_task_ids,
            "approved_write_paths": ["src/"],
            "max_parallel": 2,
            "budgets": {
                "timeout_ms": 30_000,
                "max_line_bytes": 65_536,
                "max_stdout_bytes": 524_288,
                "max_stderr_bytes": 262_144,
                "max_events": 512
            },
            "tasks": tasks_json,
            "created_at_ms": 1_700_000_001_000i64
        }))
        .expect("deserialize preliminary ApprovalRecord");

        let core_digest = compute_authority_core_digest(&record).expect("compute core digest");
        for task in &mut record.tasks {
            if task.provider.is_some() {
                task.profile_id = Some(derive_profile_id(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
                task.command_key = Some(derive_command_key(
                    &core_digest,
                    &task.task_id,
                    &task.task_digest,
                ));
            }
        }
        record.subject_digest =
            compute_record_subject_digest(&record).expect("compute subject digest");
        record.approval_id = derive_approval_id(&record.subject_digest);

        let approvals_path = fixture.control.authority.approvals_path();
        verify_approval_record_integrity(&record, approvals_path)
            .expect("verify constructed ApprovalRecord");

        let mut existing_records: Vec<ApprovalRecord> = if approvals_path.exists() {
            let raw = fs::read_to_string(approvals_path).expect("read existing approvals.json");
            let parsed: serde_json::Value =
                serde_json::from_str(&raw).expect("parse existing approvals.json");
            serde_json::from_value(parsed["approvals"].clone()).expect("extract approvals array")
        } else {
            Vec::new()
        };
        existing_records.push(record.clone());

        let ledger_bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": AUTHORITY_SCHEMA_VERSION,
            "approvals": existing_records,
        }))
        .expect("serialize approvals ledger");
        fs::write(approvals_path, ledger_bytes).expect("write approvals.json");
        fs::set_permissions(approvals_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 approvals.json");

        fixture
            .control
            .authority
            .get_for_operator(&record.approval_id)
            .expect("reload verified record from AuthorityStore")
    }

    fn read_count_file(path: &Path) -> u32 {
        fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(0)
    }

    fn is_pid_alive_in_proc(pid: u32) -> bool {
        let stat_path = PathBuf::from(format!("/proc/{pid}/stat"));
        let Ok(stat) = fs::read_to_string(stat_path) else {
            return false;
        };
        let Some(rparen) = stat.rfind(')') else {
            return false;
        };
        let mut fields = stat[rparen + 1..].split_whitespace();
        let Some(state) = fields.next() else {
            return false;
        };
        !matches!(state, "Z" | "X" | "x")
    }

    #[test]
    fn exact_serve_argv_construction_and_no_arbitrary_parameters() {
        let valid_approval = "apr_0123456789abcdef0123456789abcdef";
        let reg = Path::new("/tmp/tl-reg");
        let plans = Path::new("/tmp/tl-plans");
        let auth = Path::new("/tmp/tl-auth");
        let sess = Path::new("/tmp/tl-sess");

        let args = build_host_serve_args(reg, plans, auth, sess, valid_approval)
            .expect("valid serve args");

        let expected: Vec<OsString> = vec![
            OsString::from("serve"),
            OsString::from("--registry-dir"),
            OsString::from("/tmp/tl-reg"),
            OsString::from("--plans-dir"),
            OsString::from("/tmp/tl-plans"),
            OsString::from("--authority-dir"),
            OsString::from("/tmp/tl-auth"),
            OsString::from("--sessions-dir"),
            OsString::from("/tmp/tl-sess"),
            OsString::from("--approval-id"),
            OsString::from(valid_approval),
        ];
        assert_eq!(args, expected);

        let full_cli_iter = std::iter::once(OsString::from(HOST_BINARY_NAME)).chain(args.clone());
        let parsed = HostCli::try_parse_from(full_cli_iter).expect("HostCli must parse exact argv");
        match parsed.command {
            HostCommand::Serve(serve) => {
                assert_eq!(serve.registry_dir, reg);
                assert_eq!(serve.plans_dir, plans);
                assert_eq!(serve.authority_dir, auth);
                assert_eq!(serve.sessions_dir, sess);
                assert_eq!(serve.approval_id, valid_approval);
            }
        }

        // Reject malformed, whitespace-padded, or flag-injected approval_id inputs.
        for bad_id in [
            "",
            " apr_0123456789abcdef0123456789abcdef",
            "apr_0123456789abcdef0123456789abcdef ",
            "apr_0123456789ABCDEF0123456789abcdef",
            "apr_short",
            "--model=override",
            "apr_0123456789abcdef0123456789abcdef --auto",
        ] {
            let err = build_host_serve_args(reg, plans, auth, sess, bad_id)
                .expect_err("invalid approval_id must be rejected");
            assert_eq!(err.code, "validation");
            if !bad_id.is_empty() {
                assert!(!err.message.contains(bad_id));
            }
        }

        // Reject relative or traversal store directory arguments.
        for bad_dir in [
            Path::new(""),
            Path::new("relative/store"),
            Path::new("/tmp/tl-sess/../escape"),
            Path::new("/tmp/./tl-sess"),
        ] {
            let err = build_host_serve_args(reg, plans, auth, bad_dir, valid_approval)
                .expect_err("non-canonical directory arg must fail");
            assert_eq!(err.code, "security_violation");
        }
    }

    #[test]
    fn host_executable_resolution_missing_nonregular_symlink_canonicalized_and_static_errors() {
        let tmp = ScopedHostTestDir::new("exe-resolve");
        let secret_dir = tmp.path.join("private-secret-location");
        fs::create_dir_all(&secret_dir).expect("create secret_dir");

        // 1. Missing explicit executable -> static error, no path leak.
        let missing_exe = secret_dir.join("missing-threadlane-host");
        let err_missing =
            resolve_host_executable_with_env(Some(&missing_exe), None, None).unwrap_err();
        assert_eq!(err_missing.code, "invalid_host_executable");
        assert!(!err_missing.message.contains("private-secret-location"));
        assert!(!err_missing.message.contains("missing-threadlane-host"));

        // 2. Relative or traversal explicit path -> static error, no path leak.
        let err_rel = resolve_host_executable_with_env(
            Some(Path::new("relative/threadlane-host")),
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err_rel.code, "invalid_host_executable");
        assert!(!err_rel.message.contains("relative"));

        let traversal_exe = secret_dir.join("..").join("threadlane-host");
        let err_trav =
            resolve_host_executable_with_env(Some(&traversal_exe), None, None).unwrap_err();
        assert_eq!(err_trav.code, "invalid_host_executable");
        assert!(!err_trav.message.contains("private-secret-location"));

        // 3. Directory (non-regular file) -> static error, no path leak.
        let dir_as_exe = secret_dir.join("dir-host");
        fs::create_dir_all(&dir_as_exe).expect("create dir_as_exe");
        let err_dir = resolve_host_executable_with_env(Some(&dir_as_exe), None, None).unwrap_err();
        assert_eq!(err_dir.code, "invalid_host_executable");
        assert!(!err_dir.message.contains("dir-host"));

        // 4. Non-executable regular file (mode 0600) -> static error, no path leak.
        let non_exec_file = secret_dir.join("non-exec-host");
        fs::write(&non_exec_file, b"#!/bin/sh\nexit 0\n").expect("write non_exec_file");
        fs::set_permissions(&non_exec_file, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 non_exec_file");
        let err_non_exec =
            resolve_host_executable_with_env(Some(&non_exec_file), None, None).unwrap_err();
        assert_eq!(err_non_exec.code, "invalid_host_executable");
        assert!(!err_non_exec.message.contains("non-exec-host"));

        // 5. Symlink to valid regular executable -> canonicalizes to real target (not symlink).
        let real_bin = secret_dir.join("real-threadlane-host-bin");
        write_executable_script(&real_bin, "#!/bin/sh\nexit 0\n");
        let symlink_bin = tmp.path.join("symlink-threadlane-host");
        symlink(&real_bin, &symlink_bin).expect("create symlink to real_bin");

        let resolved = resolve_host_executable_with_env(Some(&symlink_bin), None, None)
            .expect("symlink executable must canonicalize to real target");
        assert_eq!(resolved, fs::canonicalize(&real_bin).unwrap());
        assert!(
            !fs::symlink_metadata(&resolved)
                .expect("stat resolved")
                .file_type()
                .is_symlink(),
            "resolved canonical path must not be a symlink"
        );

        // 6. Fallback to sibling_dir and PATH when explicit CLI option is None.
        let sibling_dir = tmp.path.join("sibling-bin");
        fs::create_dir_all(&sibling_dir).expect("create sibling_dir");
        let path_dir = tmp.path.join("path-bin");
        fs::create_dir_all(&path_dir).expect("create path_dir");

        // When neither sibling_dir nor PATH has `threadlane-host` -> static error.
        let err_unavail =
            resolve_host_executable_with_env(None, Some(&sibling_dir), Some(path_dir.as_os_str()))
                .unwrap_err();
        assert_eq!(err_unavail.code, "host_executable_unavailable");
        assert!(!err_unavail.message.contains("sibling-bin"));
        assert!(!err_unavail.message.contains("path-bin"));

        // Populate PATH with a symlink named `threadlane-host` -> resolves to canonical target.
        let path_host_link = path_dir.join(HOST_BINARY_NAME);
        symlink(&real_bin, &path_host_link).expect("symlink in PATH dir");
        let from_path =
            resolve_host_executable_with_env(None, Some(&sibling_dir), Some(path_dir.as_os_str()))
                .expect("resolve from PATH");
        assert_eq!(from_path, fs::canonicalize(&real_bin).unwrap());

        // Populate sibling_dir with its own binary -> sibling takes priority over PATH.
        let sibling_real = sibling_dir.join(HOST_BINARY_NAME);
        write_executable_script(&sibling_real, "#!/bin/sh\nexit 0\n");
        let from_sibling =
            resolve_host_executable_with_env(None, Some(&sibling_dir), Some(path_dir.as_os_str()))
                .expect("resolve from sibling_dir");
        assert_eq!(from_sibling, fs::canonicalize(&sibling_real).unwrap());
    }

    #[test]
    fn static_host_error_conversion_redacts_paths_executables_tokens_and_stderr() {
        let cases = vec![
            (
                HostError::SecurityViolation {
                    path: "/home/secret-operator/.local/share/sessions/apr_123".to_string(),
                    reason: "symlink detected targeting /etc/passwd with token tl_secret_999"
                        .to_string(),
                },
                "security_violation",
            ),
            (
                HostError::CorruptManifest {
                    path: "/private/sessions/host-manifest.json".to_string(),
                    reason: "unexpected token tl_bearer_xyz".to_string(),
                },
                "corrupt_manifest",
            ),
            (
                HostError::HostNotReady {
                    approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
                    phase: Some(HostPhase::Failed),
                    reason: "stderr: /nix/store/secret-agy/bin/agy failed with sk-live-secret"
                        .to_string(),
                },
                "host_not_ready",
            ),
            (
                HostError::Runtime {
                    code: "internal".to_string(),
                    message: "Bearer tl_super_secret_token failed on /tmp/socket".to_string(),
                },
                "runtime_error",
            ),
            (
                HostError::Io {
                    operation: "open(/home/secret/file)".to_string(),
                    reason: "permission denied on /home/secret/file".to_string(),
                },
                "io_error",
            ),
        ];

        for (host_err, expected_code) in cases {
            let app_err = AppError::from_host_error(host_err);
            assert_eq!(app_err.code, expected_code);
            assert!(!app_err.message.contains("/home/"));
            assert!(!app_err.message.contains("/private/"));
            assert!(!app_err.message.contains("/nix/store"));
            assert!(!app_err.message.contains("/etc/passwd"));
            assert!(!app_err.message.contains("tl_"));
            assert!(!app_err.message.contains("sk-live"));
            assert!(!app_err.message.contains("stderr:"));
        }
    }

    #[test]
    fn caller_binding_verification_denies_unconfigured_offline_demo_and_cross_binding() {
        tauri::async_runtime::block_on(async {
            let fixture = setup_desktop_host_fixture("caller-deny");

            // Bootstrap a real control supervisor in `control_sessions_dir` to get a live control binding.
            let bootstrap_record = seed_published_plan_and_approval(
                &fixture,
                "plan-control-bootstrap",
                "bnd_bootstrap_placeholder",
                THREAD_UUID_A,
            );
            let control_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control_sessions_dir,
                    &bootstrap_record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start control runtime host");
            let live_control_binding_id = control_host.session_binding_id().to_string();

            // Create an approval for `(live_control_binding_id, THREAD_UUID_A)` and start its real host session.
            let target_record = seed_published_plan_and_approval(
                &fixture,
                "plan-target-a",
                &live_control_binding_id,
                THREAD_UUID_A,
            );
            let target_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control.sessions_dir,
                    &target_record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start target session host");

            // 1. Unconfigured DesktopBridge -> runtime_not_attached.
            let unconfigured_bridge =
                DesktopBridge::new(None, None).expect("create unconfigured bridge");
            for code in [
                start_approved_session(
                    &unconfigured_bridge,
                    &fixture.control,
                    target_record.approval_id.clone(),
                )
                .await
                .unwrap_err()
                .code,
                approved_session(
                    &unconfigured_bridge,
                    &fixture.control,
                    target_record.approval_id.clone(),
                )
                .await
                .unwrap_err()
                .code,
                approved_task(
                    &unconfigured_bridge,
                    &fixture.control,
                    target_record.approval_id.clone(),
                    "task-root-agy".to_string(),
                )
                .await
                .unwrap_err()
                .code,
                approved_activity(
                    &unconfigured_bridge,
                    &fixture.control,
                    target_record.approval_id.clone(),
                    None,
                    None,
                    Some(5),
                )
                .await
                .unwrap_err()
                .code,
            ] {
                assert_eq!(code, "runtime_not_attached");
            }

            // 2. Offline / Disconnected DesktopBridge -> runtime_offline.
            let offline_bridge =
                DesktopBridge::new(Some(fixture.tmp.path.join("nonexistent-runtime")), None)
                    .expect("create offline bridge");
            assert_eq!(
                start_approved_session(
                    &offline_bridge,
                    &fixture.control,
                    target_record.approval_id.clone()
                )
                .await
                .unwrap_err()
                .code,
                "runtime_offline"
            );
            assert_eq!(
                approved_session(
                    &offline_bridge,
                    &fixture.control,
                    target_record.approval_id.clone()
                )
                .await
                .unwrap_err()
                .code,
                "runtime_offline"
            );

            // 3. Demo mode DesktopBridge -> demo_binding_forbidden.
            let demo_bridge =
                DesktopBridge::new(None, Some(control_host.runtime_state_dir().to_path_buf()))
                    .expect("create demo mode bridge");
            assert_eq!(
                start_approved_session(
                    &demo_bridge,
                    &fixture.control,
                    target_record.approval_id.clone()
                )
                .await
                .unwrap_err()
                .code,
                "demo_binding_forbidden"
            );
            assert_eq!(
                approved_task(
                    &demo_bridge,
                    &fixture.control,
                    target_record.approval_id.clone(),
                    "task-root-agy".to_string()
                )
                .await
                .unwrap_err()
                .code,
                "demo_binding_forbidden"
            );

            // 4. Connected DesktopBridge, but approval belongs to a different binding (`binding_not_found`)
            //    or mismatched thread_uuid (`thread_uuid_mismatch`).
            let connected_bridge =
                DesktopBridge::new(Some(control_host.runtime_state_dir().to_path_buf()), None)
                    .expect("create connected bridge");

            let wrong_binding_record = seed_published_plan_and_approval(
                &fixture,
                "plan-wrong-binding",
                "bnd_other_unattached_chat",
                THREAD_UUID_A,
            );
            assert_eq!(
                start_approved_session(
                    &connected_bridge,
                    &fixture.control,
                    wrong_binding_record.approval_id.clone()
                )
                .await
                .unwrap_err()
                .code,
                "binding_not_found"
            );
            assert_eq!(
                approved_session(
                    &connected_bridge,
                    &fixture.control,
                    wrong_binding_record.approval_id
                )
                .await
                .unwrap_err()
                .code,
                "binding_not_found"
            );

            let wrong_thread_record = seed_published_plan_and_approval(
                &fixture,
                "plan-wrong-thread",
                &live_control_binding_id,
                THREAD_UUID_B,
            );
            assert_eq!(
                start_approved_session(
                    &connected_bridge,
                    &fixture.control,
                    wrong_thread_record.approval_id.clone()
                )
                .await
                .unwrap_err()
                .code,
                "thread_uuid_mismatch"
            );
            assert_eq!(
                approved_task(
                    &connected_bridge,
                    &fixture.control,
                    wrong_thread_record.approval_id,
                    "task-root-agy".to_string()
                )
                .await
                .unwrap_err()
                .code,
                "thread_uuid_mismatch"
            );

            target_host.shutdown().await.expect("shutdown target_host");
            control_host
                .shutdown()
                .await
                .expect("shutdown control_host");
        });
    }

    #[test]
    fn duplicate_existing_ready_does_not_launch_and_preserves_control_bridge_identity() {
        tauri::async_runtime::block_on(async {
            let mut fixture = setup_desktop_host_fixture("dup-ready");

            // Configure a fake host executable that increments a counter file if ever spawned.
            let host_spawn_count_file = fixture.tmp.path.join("host-spawn-count.txt");
            let fake_host_exe = fixture.tmp.path.join("bin/fake-threadlane-host");
            let host_script = format!(
                "#!/bin/sh\nset -eu\nc=0\nif [ -f \"{count}\" ]; then c=$(cat \"{count}\"); fi\nc=$((c + 1))\nprintf \"%s\" \"$c\" > \"{count}\"\nexit 0\n",
                count = host_spawn_count_file.display()
            );
            write_executable_script(&fake_host_exe, &host_script);
            fixture.control.host_executable = Some(fake_host_exe);

            // Start a real control supervisor in `control_sessions_dir`.
            let bootstrap_record = seed_published_plan_and_approval(
                &fixture,
                "plan-ctrl",
                "bnd_ctrl_init",
                THREAD_UUID_A,
            );
            let control_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control_sessions_dir,
                    &bootstrap_record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start control_host");
            let control_binding_id = control_host.session_binding_id().to_string();

            let connected_bridge =
                DesktopBridge::new(Some(control_host.runtime_state_dir().to_path_buf()), None)
                    .expect("create connected_bridge");

            let record = seed_published_plan_and_approval(
                &fixture,
                "plan-ready-session",
                &control_binding_id,
                THREAD_UUID_A,
            );

            // Opening stores, saving/publishing plans, and seeding approvals never spawns a host.
            assert_eq!(read_count_file(&host_spawn_count_file), 0);

            // Start the real ApprovedHost for `record.approval_id`.
            let running_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control.sessions_dir,
                    &record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start running_host");
            let host_session_binding_id = running_host.session_binding_id().to_string();
            assert_ne!(host_session_binding_id, control_binding_id);

            // Concurrent double-click on `start_approved_session` attaches to existing Ready host
            // without spawning `fake_host_exe`.
            let (res1, res2) = tokio::join!(
                start_approved_session(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone()
                ),
                start_approved_session(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone()
                )
            );
            let summary1 = res1.expect("first start_approved_session");
            let summary2 = res2.expect("second concurrent start_approved_session");
            assert_eq!(summary1, summary2);
            assert_eq!(summary1.phase, HostPhase::Ready);
            assert!(summary1.gates.supervisor_authenticated_ready);
            assert!(!summary1.gates.workers_auto_started_on_boot);
            assert_eq!(summary1.binding_id, control_binding_id);
            assert_eq!(
                summary1.session_binding_id.as_deref(),
                Some(host_session_binding_id.as_str())
            );
            assert_eq!(
                read_count_file(&host_spawn_count_file),
                0,
                "existing Ready session must never spawn a duplicate host process"
            );
            assert_eq!(
                read_count_file(&fixture.agy_count_file),
                0,
                "host startup/attach must never auto-start any worker"
            );

            // Verify DesktopBridge remained attached to the original control runtime, NOT switched to the host session.
            let bridge_snap = connected_bridge
                .snapshot(None)
                .await
                .expect("bridge snapshot");
            assert_eq!(bridge_snap.attachment.mode, AttachmentMode::Runtime);
            assert_eq!(
                bridge_snap.attachment.connection,
                ConnectionState::Connected
            );
            assert_eq!(
                bridge_snap.selected_binding_id.as_deref(),
                Some(control_binding_id.as_str())
            );

            // Verify `approved_session`, `approved_task`, and `approved_activity` read-only observation.
            let observed = approved_session(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
            )
            .await
            .expect("approved_session");
            assert_eq!(observed, summary1);

            let root_task = approved_task(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
                "task-root-agy".to_string(),
            )
            .await
            .expect("approved_task root");
            assert_eq!(root_task.status, TaskExecutionStatus::NotStarted);
            assert!(!root_task.accepted);

            let dep_task = approved_task(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
                "task-dep-agy".to_string(),
            )
            .await
            .expect("approved_task dependent");
            assert_eq!(
                dep_task.status,
                TaskExecutionStatus::DependencyInputsUnverified
            );

            let native_task = approved_task(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
                "task-native-codex".to_string(),
            )
            .await
            .expect("approved_task native");
            assert_eq!(native_task.status, TaskExecutionStatus::NativeRouteRequired);

            let unknown_task_err = approved_task(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
                "task-nonexistent".to_string(),
            )
            .await
            .expect_err("unknown task must fail");
            assert_eq!(unknown_task_err.code, "unknown_task");

            // Activity limit <= 20 enforcement.
            assert_eq!(
                approved_activity(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone(),
                    None,
                    None,
                    Some(0)
                )
                .await
                .unwrap_err()
                .code,
                "invalid_input"
            );
            assert_eq!(
                approved_activity(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone(),
                    None,
                    None,
                    Some(21)
                )
                .await
                .unwrap_err()
                .code,
                "invalid_input"
            );
            let activity_page = approved_activity(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
                None,
                None,
                Some(5),
            )
            .await
            .expect("valid approved_activity");
            assert_eq!(activity_page.binding_id, host_session_binding_id);

            // Plan status remains Published (never mutated to Received or Applied).
            let plan_rev = fixture
                .control
                .plans
                .get_revision(&record.plan_id, record.plan_revision)
                .expect("get plan")
                .expect("plan exists");
            assert_eq!(plan_rev.status, PlanStatus::Published);

            running_host
                .shutdown()
                .await
                .expect("shutdown running_host");
            control_host
                .shutdown()
                .await
                .expect("shutdown control_host");
        });
    }

    #[test]
    fn readiness_not_proven_by_pid_or_manifest_alone_and_stopped_or_offline_not_ready() {
        tauri::async_runtime::block_on(async {
            let mut fixture = setup_desktop_host_fixture("pid-manifest-alone");
            fixture.control.host_control = Arc::new(HostControlHub::with_poll_timing(
                Duration::from_millis(120),
                Duration::from_millis(20),
            ));

            let bootstrap_record = seed_published_plan_and_approval(
                &fixture,
                "plan-ctrl-2",
                "bnd_ctrl_init_2",
                THREAD_UUID_A,
            );
            let control_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control_sessions_dir,
                    &bootstrap_record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start control_host");
            let control_binding_id = control_host.session_binding_id().to_string();

            let connected_bridge =
                DesktopBridge::new(Some(control_host.runtime_state_dir().to_path_buf()), None)
                    .expect("create connected_bridge");

            let record = seed_published_plan_and_approval(
                &fixture,
                "plan-fake-ready",
                &control_binding_id,
                THREAD_UUID_A,
            );

            let session_dir =
                prepare_session_dir(&fixture.control.sessions_dir, &record.approval_id)
                    .expect("prepare session_dir");
            threadlane_host::ensure_private_dir(&session_dir.join(RUNTIME_STATE_SUBDIR))
                .expect("create empty runtime-state");
            threadlane_host::ensure_private_dir(&session_dir.join(WORK_AREA_SUBDIR))
                .expect("create empty work-area");

            // Case 1: Manifest claims `Ready` with a live PID (`std::process::id()`), AND `host.lock`
            // is held, so `ApprovedHostClient::session_summary` alone reports `Ready` +
            // `supervisor_authenticated_ready: true`, but NO authenticated supervisor is running!
            let ready_manifest = HostSessionManifest::from_approval_record(
                &record,
                Some("bnd_unverified_session".to_string()),
                HostPhase::Ready,
                Some(std::process::id()),
                None,
                1_700_000_000_000,
                1_700_000_005_000,
            );
            write_host_manifest(&session_dir, &ready_manifest).expect("write fake ready manifest");

            {
                let _held_lock = HostSessionLock::acquire(&session_dir, &record.approval_id)
                    .expect("acquire host.lock without supervisor");

                // Confirm that `session_summary` alone would have reported Ready + gate true:
                let raw_client = ApprovedHostClient::new(
                    Arc::clone(&fixture.control.authority),
                    &fixture.control.sessions_dir,
                    &record.binding_id,
                    &record.thread_uuid,
                )
                .expect("client");
                let raw_summary = raw_client
                    .session_summary(&record.approval_id)
                    .expect("raw session_summary");
                assert_eq!(raw_summary.phase, HostPhase::Ready);
                assert!(raw_summary.gates.supervisor_authenticated_ready);

                // Both `approved_session` and `start_approved_session` must reject the unverified Ready claim!
                let obs_err = approved_session(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone(),
                )
                .await
                .expect_err(
                    "PID + manifest + lock without authenticated supervisor must not report Ready",
                );
                assert_eq!(obs_err.code, "host_not_ready");

                let start_err = start_approved_session(
                    &connected_bridge,
                    &fixture.control,
                    record.approval_id.clone(),
                )
                .await
                .expect_err(
                    "start_approved_session must not accept unauthenticated Ready manifest",
                );
                assert_eq!(start_err.code, "host_not_ready");
            }

            // Case 2: Lock is released while `host-manifest.json` on disk still says `Ready` (offline manifest).
            let offline_obs_err = approved_session(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
            )
            .await
            .expect_err("offline Ready manifest must not report Ready");
            assert_eq!(offline_obs_err.code, "host_not_ready");

            let offline_start_err = start_approved_session(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
            )
            .await
            .expect_err("offline uncertain Ready manifest must not auto-restart");
            assert_eq!(offline_start_err.code, "host_not_ready");
            assert!(
                session_dir.join(HOST_MANIFEST_FILENAME).exists(),
                "existing manifest must be preserved on disk"
            );

            // Case 3: Stopped manifest on disk reports `HostPhase::Stopped` (never `Ready`).
            let stopped_manifest = HostSessionManifest::from_approval_record(
                &record,
                Some("bnd_unverified_session".to_string()),
                HostPhase::Stopped,
                None,
                None,
                1_700_000_000_000,
                1_700_000_010_000,
            );
            write_host_manifest(&session_dir, &stopped_manifest).expect("write stopped manifest");
            let stopped_summary = approved_session(
                &connected_bridge,
                &fixture.control,
                record.approval_id.clone(),
            )
            .await
            .expect("stopped manifest observation");
            assert_eq!(stopped_summary.phase, HostPhase::Stopped);
            assert!(!stopped_summary.gates.supervisor_authenticated_ready);

            control_host
                .shutdown()
                .await
                .expect("shutdown control_host");
        });
    }

    #[test]
    fn child_handle_drop_keeps_own_fake_process_alive_and_bounded_cleanup() {
        tauri::async_runtime::block_on(async {
            let tmp = ScopedHostTestDir::new("child-drop");
            let keep_alive_file = tmp.path.join("keep-alive");
            let pid_file = tmp.path.join("child-pid.txt");
            let ping_file = tmp.path.join("ping");
            let pong_file = tmp.path.join("pong");
            fs::write(&keep_alive_file, "1").expect("create keep-alive sentinel");

            let fake_host_exe = tmp.path.join("fake-detached-host");
            let script = format!(
                r#"#!/bin/sh
set -eu
printf "%s" "$$" > "{pid_file}"
i=0
while [ "$i" -lt 250 ] && [ -f "{keep_alive}" ]; do
    if [ -f "{ping}" ] && [ ! -f "{pong}" ]; then
        printf "alive:%s" "$$" > "{pong}"
    fi
    sleep 0.02
    i=$((i + 1))
done
exit 0
"#,
                pid_file = pid_file.display(),
                keep_alive = keep_alive_file.display(),
                ping = ping_file.display(),
                pong = pong_file.display(),
            );
            write_executable_script(&fake_host_exe, &script);
            let canonical_exe =
                fs::canonicalize(&fake_host_exe).expect("canonicalize fake_host_exe");

            let approval_id = "apr_0123456789abcdef0123456789abcdef";
            let serve_args = build_host_serve_args(
                &tmp.path.join("reg"),
                &tmp.path.join("plans"),
                &tmp.path.join("auth"),
                &tmp.path.join("sess"),
                approval_id,
            )
            .expect("build serve args");

            let spawned_pid = {
                let hub = HostControlHub::new();
                let pid = hub
                    .spawn_detached_host_process(approval_id, &canonical_exe, &serve_args)
                    .expect("spawn detached fake host");
                assert_eq!(
                    hub.poll_child_reaper(approval_id),
                    ReapedChildState::Running { pid }
                );
                // Drop `hub` (and its internal `tokio::process::Child` handle) at the end of this scope!
                pid
            };

            // Wait for the child to write its PID file.
            let wait_deadline = Instant::now() + Duration::from_secs(2);
            while !pid_file.exists() && Instant::now() < wait_deadline {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            assert!(pid_file.exists(), "spawned child must write pid_file");

            // After `HostControlHub` and `tokio::process::Child` were dropped, challenge the child
            // with `ping` and verify it actively writes `pong`.
            fs::write(&ping_file, "ping").expect("write ping trigger after handle drop");
            let pong_deadline = Instant::now() + Duration::from_secs(2);
            while !pong_file.exists() && Instant::now() < pong_deadline {
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            assert!(
                pong_file.exists(),
                "child process must remain alive and responsive after Child handle Drop"
            );
            assert!(is_pid_alive_in_proc(spawned_pid));

            // Bounded cleanup of only our own spawned test child by removing `keep_alive_file`.
            let _ = fs::remove_file(&keep_alive_file);
            let exit_deadline = Instant::now() + Duration::from_secs(2);
            while is_pid_alive_in_proc(spawned_pid) && Instant::now() < exit_deadline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                !is_pid_alive_in_proc(spawned_pid),
                "own fake child process must exit cleanly within bounded cleanup window"
            );
        });
    }

    #[test]
    fn timeout_and_failure_never_invent_ready_and_spawned_host_reaches_ready_when_authenticated() {
        tauri::async_runtime::block_on(async {
            let mut fixture = setup_desktop_host_fixture("timeout-and-boot");
            fixture.control.host_control = Arc::new(HostControlHub::with_poll_timing(
                Duration::from_millis(180),
                Duration::from_millis(20),
            ));

            let bootstrap_record = seed_published_plan_and_approval(
                &fixture,
                "plan-ctrl-3",
                "bnd_ctrl_init_3",
                THREAD_UUID_A,
            );
            let control_host = ApprovedHost::start(
                ApprovedHostConfig::new(
                    fixture.control.registry.config_dir(),
                    fixture.control.plans.dir(),
                    fixture.control.authority.dir(),
                    &fixture.control_sessions_dir,
                    &bootstrap_record.approval_id,
                )
                .with_tick_interval(Duration::from_millis(20)),
            )
            .await
            .expect("start control_host");
            let control_binding_id = control_host.session_binding_id().to_string();

            let connected_bridge =
                DesktopBridge::new(Some(control_host.runtime_state_dir().to_path_buf()), None)
                    .expect("create connected_bridge");

            let record_fail = seed_published_plan_and_approval(
                &fixture,
                "plan-boot-fail",
                &control_binding_id,
                THREAD_UUID_A,
            );

            // 1. Spawned host exits non-zero immediately -> returns static `host_startup_failed`, no fake Ready.
            let failing_host_exe = fixture.tmp.path.join("bin/failing-host");
            write_executable_script(
                &failing_host_exe,
                "#!/bin/sh\necho 'secret stderr /home/user/token tl_123' >&2\nexit 1\n",
            );
            fixture.control.host_executable = Some(failing_host_exe);

            let fail_err = start_approved_session(
                &connected_bridge,
                &fixture.control,
                record_fail.approval_id.clone(),
            )
            .await
            .expect_err("exiting host must fail startup");
            assert_eq!(fail_err.code, "host_startup_failed");
            assert!(!fail_err.message.contains("tl_123"));
            assert!(!fail_err.message.contains("/home/user"));

            // Child was reaped by non-blocking reaper.
            assert!(matches!(
                fixture
                    .control
                    .host_control
                    .poll_child_reaper(&record_fail.approval_id),
                ReapedChildState::Exited { .. }
            ));

            // 2. Spawned host hangs without becoming ready -> times out with `host_not_ready`, no fake Ready.
            let keep_sleeping = fixture.tmp.path.join("keep-sleeping");
            fs::write(&keep_sleeping, "1").expect("write keep_sleeping");
            let sleeping_pid_file = fixture.tmp.path.join("sleeping-pid.txt");
            let sleeping_host_exe = fixture.tmp.path.join("bin/sleeping-host");
            let sleep_script = format!(
                "#!/bin/sh\nset -eu\nprintf \"%s\" \"$$\" > \"{pid}\"\ni=0\nwhile [ \"$i\" -lt 200 ] && [ -f \"{keep}\" ]; do sleep 0.02; i=$((i + 1)); done\nexit 0\n",
                pid = sleeping_pid_file.display(),
                keep = keep_sleeping.display(),
            );
            write_executable_script(&sleeping_host_exe, &sleep_script);
            fixture.control.host_executable = Some(sleeping_host_exe);

            let timeout_err = start_approved_session(
                &connected_bridge,
                &fixture.control,
                record_fail.approval_id.clone(),
            )
            .await
            .expect_err("hanging host must time out without fake Ready");
            assert_eq!(timeout_err.code, "host_not_ready");

            // Clean up our own sleeping fake child boundedly.
            let _ = fs::remove_file(&keep_sleeping);
            if let Ok(pid_str) = fs::read_to_string(&sleeping_pid_file) {
                if let Ok(pid) = pid_str.trim().parse::<u32>() {
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while is_pid_alive_in_proc(pid) && Instant::now() < deadline {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            fixture.control.host_control.reap_all_finished();

            // 3. End-to-end spawn where fake host records exact argv and an in-process ApprovedHost
            //    starts from those exact parsed `ServeArgs` during the readiness poll.
            fixture.control.host_control = Arc::new(HostControlHub::with_poll_timing(
                Duration::from_secs(5),
                Duration::from_millis(25),
            ));
            let record_ok = seed_published_plan_and_approval(
                &fixture,
                "plan-boot-ok",
                &control_binding_id,
                THREAD_UUID_A,
            );

            let argv_file = fixture.tmp.path.join("captured-serve-argv.txt");
            let hold_ok_file = fixture.tmp.path.join("hold-ok-host");
            fs::write(&hold_ok_file, "1").expect("write hold_ok_file");
            let ok_host_exe = fixture.tmp.path.join("bin/ok-host-wrapper");
            let ok_script = format!(
                r#"#!/bin/sh
set -eu
: > "{argv}.tmp"
for arg in "$@"; do
    printf "%s\n" "$arg" >> "{argv}.tmp"
done
mv "{argv}.tmp" "{argv}"
i=0
while [ "$i" -lt 250 ] && [ -f "{hold}" ]; do
    sleep 0.02
    i=$((i + 1))
done
exit 0
"#,
                argv = argv_file.display(),
                hold = hold_ok_file.display(),
            );
            write_executable_script(&ok_host_exe, &ok_script);
            fixture.control.host_executable = Some(ok_host_exe);

            let argv_file_bg = argv_file.clone();
            let bg_host_task = tauri::async_runtime::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(3);
                while !argv_file_bg.exists() && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
                let raw_lines = fs::read_to_string(&argv_file_bg).expect("read captured argv");
                let mut cli_args = vec![HOST_BINARY_NAME.to_string()];
                cli_args.extend(raw_lines.lines().map(str::to_string));
                let parsed = HostCli::try_parse_from(&cli_args).expect("parse captured argv");
                let HostCommand::Serve(serve_args) = parsed.command;
                ApprovedHost::start(
                    ApprovedHostConfig::new(
                        serve_args.registry_dir,
                        serve_args.plans_dir,
                        serve_args.authority_dir,
                        serve_args.sessions_dir,
                        serve_args.approval_id,
                    )
                    .with_tick_interval(Duration::from_millis(20)),
                )
                .await
                .expect("start real ApprovedHost from captured CLI args")
            });

            let started_summary = start_approved_session(
                &connected_bridge,
                &fixture.control,
                record_ok.approval_id.clone(),
            )
            .await
            .expect("start_approved_session must succeed once authenticated host is ready");
            assert_eq!(started_summary.approval_id, record_ok.approval_id);
            assert_eq!(started_summary.phase, HostPhase::Ready);
            assert!(started_summary.gates.supervisor_authenticated_ready);
            assert!(!started_summary.gates.workers_auto_started_on_boot);

            let spawned_host_handle = bg_host_task.await.expect("join bg_host_task");
            let _ = fs::remove_file(&hold_ok_file);
            spawned_host_handle
                .shutdown()
                .await
                .expect("shutdown spawned_host_handle");
            fixture.control.host_control.reap_all_finished();

            control_host
                .shutdown()
                .await
                .expect("shutdown control_host");
        });
    }
}
