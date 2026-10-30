//! Crate-private owned OpenCode v2 session allocation and post-run metadata collection.
//!
//! This module supervises the fixed public OpenCode v2 CLI commands required to allocate a
//! software-owned session before a worker run and to collect authoritative session metadata after
//! the worker exits:
//!
//! 1. **Allocation (`allocate`)**:
//!    `api --standalone session.create --data <JSON>`
//!    Parses only `{ "data": { "id", "location": { "directory" }, "time": { "created" } } }` and
//!    validates a safe bounded non-empty session identifier, an exact canonical working directory
//!    match, and a creation timestamp at or after `started_at_ms` (and not unreasonably in the
//!    future).
//! 2. **Collection (`collect`)**:
//!    - Phase 1: `api --standalone session.get --param sessionID=<exact software-owned ID>`
//!    - Phase 2: `session export --standalone --sanitize <exact software-owned ID>`
//!    Feeds the captured in-memory byte buffers directly to [`validate_opencode_metadata`],
//!    preserving finite static error codes without persisting or logging raw export content.
//!
//! Every metadata command runs without a shell in its own Unix process group (`process_group(0)`),
//! with `stdin` set to null, `kill_on_drop(true)`, the exact canonical working directory, and the
//! exact per-child environment overrides used by the model run. Both `stdout` and `stderr` are
//! drained concurrently under strict byte caps (`64 KiB` for `session.create`/`session.get`,
//! `1 MiB` for `session export`, and `context.max_stderr_bytes` for `stderr` which is discarded).
//! Cancellation, timeouts (`<= 10s` per command and bounded by the overall run deadline), or
//! output overflows immediately terminate and reap the entire owned Unix process group. Any
//! lingering descendant process or cleanup uncertainty prevents evidence acceptance.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::fs_safety::ensure_platform_supported;
use crate::opencode_metadata::{
    validate_opencode_metadata, OpenCodeMetadataError, OpenCodeMetadataEvidence,
    OwnedOpenCodeSession, MAX_OPENCODE_EXPORT_BYTES, MAX_OPENCODE_INFO_BYTES,
};
use crate::spec::{ResolvedModelTarget, MAX_IDENTIFIER_LEN, MAX_STDERR_BYTES};
use crate::supervisor::{
    finish_or_terminate_surviving_descendants, is_isolated_process_group, is_process_group_alive,
    terminate_and_reap_group, terminate_process_group_members, wait_for_cancellation,
    wait_for_readers_or_signal_stop, ProcessGroupGuard, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};

/// Maximum per-command timeout for OpenCode metadata commands (`10 seconds`).
pub(crate) const MAX_METADATA_COMMAND_TIMEOUT_MS: u64 = 10_000;

/// Maximum accepted byte length of the `session.create` stdout payload (`64 KiB`).
pub(crate) const MAX_OPENCODE_CREATE_BYTES: usize = MAX_OPENCODE_INFO_BYTES;

/// Maximum permitted forward clock drift above `started_at_ms` for `session.create` (`5 minutes`).
pub(crate) const MAX_SESSION_CREATE_FUTURE_DRIFT_MS: u64 = 300_000;

/// Pre-validated execution context shared across OpenCode session allocation and collection.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeCollectorContext {
    /// Validated canonical executable path for the OpenCode CLI.
    pub(crate) executable: PathBuf,
    /// Validated canonical working directory for the worker run.
    pub(crate) canonical_workdir: PathBuf,
    /// Exact per-child environment overrides used by the worker model run.
    pub(crate) child_env: Vec<(String, String)>,
    /// Absolute overall deadline shared across the worker run and metadata commands.
    pub(crate) overall_deadline: Instant,
    /// Trusted wallclock timestamp (milliseconds since Unix epoch) when the run started.
    pub(crate) started_at_ms: u64,
    /// Maximum stderr bytes permitted per metadata command before overflow (`stderr` is dropped).
    pub(crate) max_stderr_bytes: usize,
}

/// Finite static error code returned by OpenCode session allocation or metadata collection.
///
/// Variants carry no raw output, filesystem paths, provider error prose, tokens, or JSON values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OpenCodeCollectorError {
    /// Current host platform does not support Unix process-group supervision.
    UnsupportedPlatform,
    /// Collector context or requested model target failed pre-launch validation.
    InvalidContext,
    /// Spawning the CLI process or isolating its Unix process group failed.
    SpawnFailed,
    /// Operation was cancelled via the cancellation channel before or during command execution.
    Cancelled,
    /// Per-command timeout (`<= 10s`) or overall run deadline expired.
    TimedOut,
    /// Command stdout/stderr byte cap or metadata message cap was exceeded.
    OutputOverflow,
    /// Lingering descendants or pipe-drain uncertainty prevented clean process-group exit.
    CleanupIncomplete,
    /// Metadata command exited with a non-zero exit code or termination signal.
    NonZeroExit,
    /// Required JSON shape, identifier, timestamp type, or field type was malformed.
    InvalidMetadata,
    /// Session identifier, working directory, or message-session scope did not match owned state.
    OwnershipMismatch,
    /// Session or assistant timestamp predates `started_at_ms` or is unreasonably in the future.
    StaleSession,
    /// No assistant model evidence was available in the sanitized session export.
    MissingModel,
    /// Assistant provider or model did not match the immutable requested target.
    ModelMismatch,
    /// Requested effort had no matching per-assistant variant evidence.
    MissingEffort,
    /// Observed assistant variant differed from the requested effort.
    EffortMismatch,
    /// Succeeded session outcome, assistant completion, final `stop`, or idle time was missing.
    MissingCompletion,
    /// Assistant message carried a non-null provider error.
    ProviderError,
}

impl OpenCodeCollectorError {
    /// Returns the finite `snake_case` error code.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::InvalidContext => "invalid_context",
            Self::SpawnFailed => "spawn_failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::OutputOverflow => "output_overflow",
            Self::CleanupIncomplete => "cleanup_incomplete",
            Self::NonZeroExit => "nonzero_exit",
            Self::InvalidMetadata => "invalid_metadata",
            Self::OwnershipMismatch => "ownership_mismatch",
            Self::StaleSession => "stale_session",
            Self::MissingModel => "missing_model",
            Self::ModelMismatch => "model_mismatch",
            Self::MissingEffort => "missing_effort",
            Self::EffortMismatch => "effort_mismatch",
            Self::MissingCompletion => "missing_completion",
            Self::ProviderError => "provider_error",
        }
    }
}

impl fmt::Display for OpenCodeCollectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for OpenCodeCollectorError {}

impl From<OpenCodeMetadataError> for OpenCodeCollectorError {
    fn from(error: OpenCodeMetadataError) -> Self {
        match error {
            OpenCodeMetadataError::OutputOverflow => Self::OutputOverflow,
            OpenCodeMetadataError::InvalidMetadata => Self::InvalidMetadata,
            OpenCodeMetadataError::OwnershipMismatch => Self::OwnershipMismatch,
            OpenCodeMetadataError::StaleSession => Self::StaleSession,
            OpenCodeMetadataError::MissingModel => Self::MissingModel,
            OpenCodeMetadataError::ModelMismatch => Self::ModelMismatch,
            OpenCodeMetadataError::MissingEffort => Self::MissingEffort,
            OpenCodeMetadataError::EffortMismatch => Self::EffortMismatch,
            OpenCodeMetadataError::MissingCompletion => Self::MissingCompletion,
            OpenCodeMetadataError::ProviderError => Self::ProviderError,
        }
    }
}

#[derive(Deserialize)]
struct SessionCreateEnvelope {
    data: SessionCreateData,
}

#[derive(Deserialize)]
struct SessionCreateData {
    id: String,
    location: SessionCreateLocation,
    time: SessionCreateTime,
}

#[derive(Deserialize)]
struct SessionCreateLocation {
    directory: String,
}

#[derive(Deserialize)]
struct SessionCreateTime {
    created: u64,
}

/// Allocates a fresh software-owned OpenCode v2 session before launching the worker run.
///
/// Executes `api --standalone session.create --data <JSON>` under Unix process-group supervision
/// and validates the returned session identifier, canonical working directory, and creation time.
pub(crate) async fn allocate(
    context: &OpenCodeCollectorContext,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OwnedOpenCodeSession, OpenCodeCollectorError> {
    let workdir_str = validate_context(context)?;
    let argv = build_session_create_argv(workdir_str, resolved_model)?;

    let create_stdout =
        run_metadata_command(context, &argv, MAX_OPENCODE_CREATE_BYTES, cancel_rx).await?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;

    let owned = parse_and_validate_created_session(&create_stdout, context, workdir_str)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;
    Ok(owned)
}

/// Collects and validates authoritative OpenCode v2 session metadata for an [`OwnedOpenCodeSession`].
///
/// Executes `api --standalone session.get --param sessionID=<ID>` followed by
/// `session export --standalone --sanitize <ID>`, checking cancellation and the overall deadline
/// before each phase, and validates both responses in memory via [`validate_opencode_metadata`].
pub(crate) async fn collect(
    context: &OpenCodeCollectorContext,
    owned: &OwnedOpenCodeSession,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OpenCodeMetadataEvidence, OpenCodeCollectorError> {
    let workdir_str = validate_context(context)?;
    let _ = split_and_validate_model_target(resolved_model)?;
    validate_owned_against_context(owned, context, workdir_str)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;
    let get_argv = build_session_get_argv(&owned.session_id)?;
    let info_bytes =
        run_metadata_command(context, &get_argv, MAX_OPENCODE_INFO_BYTES, cancel_rx).await?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;
    let export_argv = build_session_export_argv(&owned.session_id)?;
    let export_bytes =
        run_metadata_command(context, &export_argv, MAX_OPENCODE_EXPORT_BYTES, cancel_rx).await?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;
    let evidence = validate_opencode_metadata(owned, &info_bytes, &export_bytes, resolved_model)
        .map_err(OpenCodeCollectorError::from)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;
    Ok(evidence)
}

/// Builds the fixed public argv for `api --standalone session.create --data <JSON>`.
pub(crate) fn build_session_create_argv(
    canonical_workdir: &str,
    resolved_model: &ResolvedModelTarget,
) -> Result<Vec<String>, OpenCodeCollectorError> {
    if !is_clean_absolute_path(canonical_workdir) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    let (provider_id, model_id) = split_and_validate_model_target(resolved_model)?;

    let mut model_map = Map::new();
    model_map.insert(
        "providerID".to_string(),
        Value::String(provider_id.to_string()),
    );
    model_map.insert("id".to_string(), Value::String(model_id.to_string()));
    if let Some(variant) = resolved_model.effort.as_deref() {
        model_map.insert("variant".to_string(), Value::String(variant.to_string()));
    }

    let mut location_map = Map::new();
    location_map.insert(
        "directory".to_string(),
        Value::String(canonical_workdir.to_string()),
    );

    let mut root_map = Map::new();
    root_map.insert("agent".to_string(), Value::String("build".to_string()));
    root_map.insert("model".to_string(), Value::Object(model_map));
    root_map.insert("location".to_string(), Value::Object(location_map));

    let data_json = serde_json::to_string(&Value::Object(root_map))
        .map_err(|_| OpenCodeCollectorError::InvalidContext)?;

    Ok(vec![
        "api".to_string(),
        "--standalone".to_string(),
        "session.create".to_string(),
        "--data".to_string(),
        data_json,
    ])
}

/// Builds the fixed public argv for `api --standalone session.get --param sessionID=<ID>`.
pub(crate) fn build_session_get_argv(
    session_id: &str,
) -> Result<Vec<String>, OpenCodeCollectorError> {
    if !is_safe_identifier(session_id) {
        return Err(OpenCodeCollectorError::InvalidMetadata);
    }
    Ok(vec![
        "api".to_string(),
        "--standalone".to_string(),
        "session.get".to_string(),
        "--param".to_string(),
        format!("sessionID={session_id}"),
    ])
}

/// Builds the fixed public argv for `session export --standalone --sanitize <ID>`.
pub(crate) fn build_session_export_argv(
    session_id: &str,
) -> Result<Vec<String>, OpenCodeCollectorError> {
    if !is_safe_identifier(session_id) {
        return Err(OpenCodeCollectorError::InvalidMetadata);
    }
    Ok(vec![
        "session".to_string(),
        "export".to_string(),
        "--standalone".to_string(),
        "--sanitize".to_string(),
        session_id.to_string(),
    ])
}

fn parse_and_validate_created_session(
    bytes: &[u8],
    context: &OpenCodeCollectorContext,
    expected_workdir: &str,
) -> Result<OwnedOpenCodeSession, OpenCodeCollectorError> {
    if bytes.len() > MAX_OPENCODE_CREATE_BYTES {
        return Err(OpenCodeCollectorError::OutputOverflow);
    }

    let envelope: SessionCreateEnvelope =
        serde_json::from_slice(bytes).map_err(|_| OpenCodeCollectorError::InvalidMetadata)?;

    if !is_safe_identifier(&envelope.data.id) {
        return Err(OpenCodeCollectorError::InvalidMetadata);
    }

    if !is_clean_absolute_path(&envelope.data.location.directory)
        || envelope.data.location.directory != expected_workdir
    {
        return Err(OpenCodeCollectorError::OwnershipMismatch);
    }

    let created_ms = envelope.data.time.created;
    let max_allowed_created_ms = context
        .started_at_ms
        .saturating_add(MAX_SESSION_CREATE_FUTURE_DRIFT_MS);
    if created_ms < context.started_at_ms
        || created_ms > max_allowed_created_ms
        || created_ms > i64::MAX as u64
    {
        return Err(OpenCodeCollectorError::StaleSession);
    }

    Ok(OwnedOpenCodeSession {
        session_id: envelope.data.id,
        canonical_workdir: context.canonical_workdir.clone(),
        started_at_ms: context.started_at_ms,
    })
}

fn validate_context(context: &OpenCodeCollectorContext) -> Result<&str, OpenCodeCollectorError> {
    ensure_platform_supported().map_err(|_| OpenCodeCollectorError::UnsupportedPlatform)?;

    let exec_str = context
        .executable
        .to_str()
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_clean_absolute_path(exec_str) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    let workdir_str = context
        .canonical_workdir
        .to_str()
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_clean_absolute_path(workdir_str) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    if context.max_stderr_bytes == 0 || context.max_stderr_bytes > MAX_STDERR_BYTES {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    if context.started_at_ms > i64::MAX as u64 {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    for (key, value) in &context.child_env {
        if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
            return Err(OpenCodeCollectorError::InvalidContext);
        }
    }

    Ok(workdir_str)
}

fn validate_owned_against_context(
    owned: &OwnedOpenCodeSession,
    context: &OpenCodeCollectorContext,
    expected_workdir: &str,
) -> Result<(), OpenCodeCollectorError> {
    if !is_safe_identifier(&owned.session_id) {
        return Err(OpenCodeCollectorError::InvalidMetadata);
    }
    let owned_workdir = owned
        .canonical_workdir
        .to_str()
        .ok_or(OpenCodeCollectorError::InvalidMetadata)?;
    if !is_clean_absolute_path(owned_workdir) || owned_workdir != expected_workdir {
        return Err(OpenCodeCollectorError::OwnershipMismatch);
    }
    if owned.started_at_ms < context.started_at_ms || owned.started_at_ms > i64::MAX as u64 {
        return Err(OpenCodeCollectorError::StaleSession);
    }
    Ok(())
}

fn split_and_validate_model_target(
    resolved_model: &ResolvedModelTarget,
) -> Result<(&str, &str), OpenCodeCollectorError> {
    if resolved_model.base_model.len() > MAX_IDENTIFIER_LEN {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    let (provider_id, model_id) = resolved_model
        .base_model
        .split_once('/')
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    if resolved_model
        .effort
        .as_deref()
        .is_some_and(|effort| !is_safe_identifier(effort))
    {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    Ok((provider_id, model_id))
}

fn check_not_cancelled_or_expired(
    overall_deadline: Instant,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Instant, OpenCodeCollectorError> {
    if *cancel_rx.borrow_and_update() {
        return Err(OpenCodeCollectorError::Cancelled);
    }
    let now = Instant::now();
    if now >= overall_deadline {
        return Err(OpenCodeCollectorError::TimedOut);
    }
    let max_phase_deadline = now + Duration::from_millis(MAX_METADATA_COMMAND_TIMEOUT_MS);
    Ok(overall_deadline.min(max_phase_deadline))
}

async fn run_metadata_command(
    context: &OpenCodeCollectorContext,
    argv: &[String],
    max_stdout_bytes: usize,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Vec<u8>, OpenCodeCollectorError> {
    let command_deadline = check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;

    let mut child = spawn_isolated_child(context, argv).await?;
    let child_pid = match child.id() {
        Some(pid) => pid,
        None => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    };
    let pgid = match i32::try_from(child_pid) {
        Ok(id) => id,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    };

    if !is_isolated_process_group(pgid) {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let Some(stdout_pipe) = child.stdout.take() else {
        let _ = terminate_and_reap_group(pgid, &mut child).await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    };
    let Some(stderr_pipe) = child.stderr.take() else {
        let _ = terminate_and_reap_group(pgid, &mut child).await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    };

    let (overflow_tx, mut overflow_rx) = mpsc::unbounded_channel::<()>();
    let (drain_stop_tx, drain_stop_rx) = watch::channel(false);

    let mut stdout_handle = tokio::spawn(drain_stdout_bounded(
        stdout_pipe,
        max_stdout_bytes,
        overflow_tx.clone(),
        drain_stop_rx.clone(),
    ));
    let mut stderr_handle = tokio::spawn(drain_stderr_bounded(
        stderr_pipe,
        context.max_stderr_bytes,
        overflow_tx,
        drain_stop_rx,
    ));

    let mut timed_out = false;
    let mut cancelled = false;
    let mut overflow_signaled = false;
    let mut descendants_terminated_after_exit = false;
    let exit_status: Option<ExitStatus>;

    if *cancel_rx.borrow_and_update() {
        cancelled = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else if Instant::now() >= command_deadline {
        timed_out = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else {
        let timeout_sleep = tokio::time::sleep_until(command_deadline);
        tokio::pin!(timeout_sleep);

        tokio::select! {
            wait_res = child.wait() => {
                exit_status = wait_res.ok();
                descendants_terminated_after_exit =
                    finish_or_terminate_surviving_descendants(
                        pgid,
                        &stdout_handle,
                        &stderr_handle,
                    ).await;
            }
            Some(()) = overflow_rx.recv() => {
                overflow_signaled = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
            () = &mut timeout_sleep => {
                timed_out = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
            () = wait_for_cancellation(cancel_rx) => {
                cancelled = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
        }
    }

    let pipe_drain_timed_out =
        wait_for_readers_or_signal_stop(&stdout_handle, &stderr_handle, &drain_stop_tx).await;

    if is_process_group_alive(pgid) {
        terminate_process_group_members(pgid).await;
    }

    let group_still_alive = is_process_group_alive(pgid);
    if !group_still_alive {
        pgroup_guard.disarm();
    }
    let mut cleanup_incomplete =
        pipe_drain_timed_out || group_still_alive || descendants_terminated_after_exit;

    let (stdout_bytes, stdout_overflow, stdout_read_error) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(triple)) => triple,
        Ok(Err(_)) => {
            cleanup_incomplete = true;
            (Vec::new(), false, true)
        }
        Err(_) => {
            stdout_handle.abort();
            cleanup_incomplete = true;
            (Vec::new(), false, true)
        }
    };

    let (stderr_overflow, stderr_read_error) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stderr_handle,
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(_)) => {
            cleanup_incomplete = true;
            (false, true)
        }
        Err(_) => {
            stderr_handle.abort();
            cleanup_incomplete = true;
            (false, true)
        }
    };

    if stdout_read_error || stderr_read_error {
        cleanup_incomplete = true;
    }

    let final_overflow = overflow_signaled || stdout_overflow || stderr_overflow;

    if group_still_alive {
        return Err(OpenCodeCollectorError::CleanupIncomplete);
    }
    if cancelled {
        return Err(OpenCodeCollectorError::Cancelled);
    }
    if timed_out {
        return Err(OpenCodeCollectorError::TimedOut);
    }
    if final_overflow {
        return Err(OpenCodeCollectorError::OutputOverflow);
    }
    if cleanup_incomplete {
        return Err(OpenCodeCollectorError::CleanupIncomplete);
    }

    match exit_status {
        Some(status) if status.success() => Ok(stdout_bytes),
        _ => Err(OpenCodeCollectorError::NonZeroExit),
    }
}

async fn spawn_isolated_child(
    context: &OpenCodeCollectorContext,
    argv: &[String],
) -> Result<tokio::process::Child, OpenCodeCollectorError> {
    let mut cmd = Command::new(&context.executable);
    cmd.args(argv)
        .envs(
            context
                .child_env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .current_dir(&context.canonical_workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    cmd.spawn().map_err(|_| OpenCodeCollectorError::SpawnFailed)
}

async fn drain_stdout_bounded(
    mut reader: tokio::process::ChildStdout,
    max_stdout_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<()>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (Vec<u8>, bool, bool) {
    let mut buf: Vec<u8> = Vec::with_capacity(4096.min(max_stdout_bytes));
    let mut chunk = [0u8; 4096];
    let mut overflow = false;
    let mut read_error = false;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_cancellation(&mut drain_stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            break;
        };
        match read_res {
            Ok(0) => break,
            Ok(n) => {
                if buf.len().saturating_add(n) > max_stdout_bytes {
                    buf.clear();
                    overflow = true;
                    let _ = overflow_tx.send(());
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => {
                buf.clear();
                read_error = true;
                break;
            }
        }
    }

    (buf, overflow, read_error)
}

async fn drain_stderr_bounded(
    mut reader: tokio::process::ChildStderr,
    max_stderr_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<()>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (bool, bool) {
    let mut total_bytes: usize = 0;
    let mut chunk = [0u8; 4096];
    let mut overflow = false;
    let mut read_error = false;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_cancellation(&mut drain_stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            break;
        };
        match read_res {
            Ok(0) => break,
            Ok(n) => {
                if total_bytes.saturating_add(n) > max_stderr_bytes {
                    overflow = true;
                    let _ = overflow_tx.send(());
                    break;
                }
                total_bytes += n;
            }
            Err(_) => {
                read_error = true;
                break;
            }
        }
    }

    (overflow, read_error)
}

fn is_clean_absolute_path(value: &str) -> bool {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
        || value.contains("//")
        || (value != "/" && value.ends_with('/'))
    {
        return false;
    }
    Path::new(value)
        .components()
        .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

fn is_safe_model_identifier(value: &str) -> bool {
    !value.is_empty() && value.split('/').all(is_safe_identifier)
}

fn is_safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_LEN
        && value.trim() == value
        && !value.starts_with('-')
        && !value.starts_with('.')
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::supervisor::is_process_alive;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TestFixture {
        root_dir: PathBuf,
        workdir: PathBuf,
        bin_path: PathBuf,
        log_path: PathBuf,
    }

    impl TestFixture {
        fn new(script_body: &str) -> Self {
            let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let base = std::env::temp_dir()
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from("/tmp"));
            let root_dir = base.join(format!("threadlane-opencode-collector-test-{pid}-{id}"));
            let workdir = root_dir.join("work");
            fs::create_dir_all(&workdir).expect("create test workdir");
            fs::set_permissions(&root_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod root_dir");
            fs::set_permissions(&workdir, fs::Permissions::from_mode(0o700))
                .expect("chmod workdir");

            let staging_path = root_dir.join("fake-opencode.tmp");
            let bin_path = root_dir.join("fake-opencode");
            let log_path = root_dir.join("invocations.log");

            {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .mode(0o700)
                    .open(&staging_path)
                    .expect("open staging fake script");
                file.write_all(script_body.as_bytes())
                    .expect("write fake script");
                file.sync_all().expect("sync fake script");
            }
            fs::rename(&staging_path, &bin_path).expect("rename fake script into place");

            Self {
                root_dir,
                workdir,
                bin_path,
                log_path,
            }
        }

        fn context(&self, timeout: Duration, started_at_ms: u64) -> OpenCodeCollectorContext {
            OpenCodeCollectorContext {
                executable: self.bin_path.clone(),
                canonical_workdir: self.workdir.clone(),
                child_env: vec![(
                    "OPENCODE_CONFIG_CONTENT".to_string(),
                    r#"{"model":"opencode-go/deepseek-v4.1-flash"}"#.to_string(),
                )],
                overall_deadline: Instant::now() + timeout,
                started_at_ms,
                max_stderr_bytes: 4096,
            }
        }

        fn invocations(&self) -> Vec<String> {
            match fs::read_to_string(&self.log_path) {
                Ok(text) => text.lines().map(str::to_string).collect(),
                Err(_) => Vec::new(),
            }
        }
    }

    impl Drop for TestFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }

    fn expected_model_target() -> ResolvedModelTarget {
        ResolvedModelTarget {
            base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
            cli_model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        }
    }

    #[tokio::test]
    async fn valid_create_info_and_sanitized_export_proves_exact_model_and_max_effort() {
        let script = r#"#!/bin/sh
set -eu
LOG_FILE="$(dirname "$0")/invocations.log"
printf '%s\n' "$*" >> "$LOG_FILE"

if [ "${OPENCODE_CONFIG_CONTENT:-}" != '{"model":"opencode-go/deepseek-v4.1-flash"}' ]; then
    echo "missing or mismatched OPENCODE_CONFIG_CONTENT" >&2
    exit 12
fi

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.create" ] && [ "$4" = "--data" ]; then
    printf '%s\n' "$5" > "$(dirname "$0")/create_payload.json"
    printf 'bounded stderr create noise\n' >&2
    cat <<EOF
{"data":{"id":"ses_owned_123","title":"ignored title","location":{"directory":"$(pwd)"},"time":{"created":1050}}}
EOF
    exit 0
fi

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.get" ] && [ "$4" = "--param" ] && [ "$5" = "sessionID=ses_owned_123" ]; then
    printf 'bounded stderr get noise\n' >&2
    cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
    exit 0
fi

if [ "$1" = "session" ] && [ "$2" = "export" ] && [ "$3" = "--standalone" ] && [ "$4" = "--sanitize" ] && [ "$5" = "ses_owned_123" ]; then
    printf 'bounded stderr export noise\n' >&2
    cat <<EOF
{"info":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}},"messages":[{"id":"msg_user_1","type":"user","sessionID":"ses_owned_123"},{"id":"msg_asst_1","type":"assistant","sessionID":"ses_owned_123","finish":"stop","time":{"created":1060,"completed":1150},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}
EOF
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        let owned = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect("allocate should succeed");
        assert_eq!(owned.session_id, "ses_owned_123");
        assert_eq!(owned.canonical_workdir, fixture.workdir);
        assert_eq!(owned.started_at_ms, 1_000);

        let create_payload_raw = fs::read_to_string(fixture.root_dir.join("create_payload.json"))
            .expect("read captured session.create payload");
        let create_payload: Value =
            serde_json::from_str(&create_payload_raw).expect("parse session.create payload");
        assert_eq!(create_payload["agent"], "build");
        assert_eq!(create_payload["model"]["providerID"], "opencode-go");
        assert_eq!(create_payload["model"]["id"], "deepseek-v4.1-flash");
        assert_eq!(create_payload["model"]["variant"], "max");
        assert_eq!(
            create_payload["location"]["directory"],
            fixture.workdir.to_str().unwrap()
        );

        let evidence = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect("collect should succeed");
        assert_eq!(evidence.source, "opencode_sanitized_session_export");
        assert_eq!(evidence.strength, "cli_reported_per_assistant");
        assert_eq!(evidence.session_id, "ses_owned_123");
        assert_eq!(evidence.observed_model, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(evidence.observed_effort.as_deref(), Some("max"));
        assert_eq!(evidence.assistant_message_count, 1);
        assert_eq!(evidence.terminal_message_id, "msg_asst_1");

        let invocations = fixture.invocations();
        assert_eq!(invocations.len(), 3);
        assert!(invocations[0].starts_with("api --standalone session.create --data "));
        assert_eq!(
            invocations[1],
            "api --standalone session.get --param sessionID=ses_owned_123"
        );
        assert_eq!(
            invocations[2],
            "session export --standalone --sanitize ses_owned_123"
        );
    }

    #[tokio::test]
    async fn wrong_create_identity_time_or_cwd_refuses_before_metadata() {
        let script = r#"#!/bin/sh
set -eu
MODE_FILE="$(dirname "$0")/mode.txt"
LOG_FILE="$(dirname "$0")/invocations.log"
printf '%s\n' "$*" >> "$LOG_FILE"
MODE="$(cat "$MODE_FILE")"

if [ "$1" = "api" ] && [ "$3" = "session.create" ]; then
    case "$MODE" in
        bad_id)
            printf '{"data":{"id":"../escaped","location":{"directory":"%s"},"time":{"created":1050}}}\n' "$(pwd)"
            ;;
        stale_time)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":999}}}\n' "$(pwd)"
            ;;
        future_time)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":999999999}}}\n' "$(pwd)"
            ;;
        wrong_cwd)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"/tmp/unrelated-workdir"},"time":{"created":1050}}}\n'
            ;;
    esac
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        fs::write(&mode_path, "bad_id").expect("write bad_id mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("unsafe session ID must fail");
        assert_eq!(err, OpenCodeCollectorError::InvalidMetadata);

        fs::write(&mode_path, "stale_time").expect("write stale_time mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("stale session creation timestamp must fail");
        assert_eq!(err, OpenCodeCollectorError::StaleSession);

        fs::write(&mode_path, "future_time").expect("write future_time mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("unreasonably future session creation timestamp must fail");
        assert_eq!(err, OpenCodeCollectorError::StaleSession);

        fs::write(&mode_path, "wrong_cwd").expect("write wrong_cwd mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("mismatched session directory must fail");
        assert_eq!(err, OpenCodeCollectorError::OwnershipMismatch);

        let before_collect_count = fixture.invocations().len();
        let mismatched_owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: PathBuf::from("/tmp/different-owned-workdir"),
            started_at_ms: 1_000,
        };
        let collect_err = collect(&context, &mismatched_owned, &target, &mut cancel_rx)
            .await
            .expect_err("collect with wrong owned cwd must refuse before spawning");
        assert_eq!(collect_err, OpenCodeCollectorError::OwnershipMismatch);
        assert_eq!(
            fixture.invocations().len(),
            before_collect_count,
            "collect must not spawn any CLI command when owned session state mismatches context"
        );
    }

    #[tokio::test]
    async fn metadata_error_never_echoes_sentinel() {
        let sentinel = "SENTINEL_SECRET_TOKEN_DO_NOT_ECHO_999";
        let script = r#"#!/bin/sh
set -eu
MODE="$(cat "$(dirname "$0")/mode.txt")"
SENTINEL="SENTINEL_SECRET_TOKEN_DO_NOT_ECHO_999"

if [ "$1" = "api" ] && [ "$3" = "session.create" ]; then
    printf 'stderr leak %s\n' "$SENTINEL" >&2
    printf 'not-json-%s\n' "$SENTINEL"
    exit 0
fi

if [ "$1" = "api" ] && [ "$3" = "session.get" ]; then
    if [ "$MODE" = "nonzero" ]; then
        printf 'stderr failure %s\n' "$SENTINEL" >&2
        printf '{"error":"%s"}\n' "$SENTINEL"
        exit 7
    fi
    cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
    exit 0
fi

if [ "$1" = "session" ] && [ "$2" = "export" ]; then
    cat <<EOF
{"info":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}},"messages":[{"id":"msg_asst_1","type":"assistant","sessionID":"ses_owned_123","finish":"stop","error":{"message":"$SENTINEL"},"time":{"created":1060,"completed":1150},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}
EOF
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        fs::write(&mode_path, "malformed_create").expect("write mode");
        let create_err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("malformed create output must fail");
        assert_eq!(create_err, OpenCodeCollectorError::InvalidMetadata);
        assert!(!create_err.to_string().contains(sentinel));
        assert!(!format!("{create_err:?}").contains(sentinel));

        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };

        fs::write(&mode_path, "nonzero").expect("write mode");
        let nonzero_err = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("nonzero session.get must fail");
        assert_eq!(nonzero_err, OpenCodeCollectorError::NonZeroExit);
        assert!(!nonzero_err.to_string().contains(sentinel));
        assert!(!format!("{nonzero_err:?}").contains(sentinel));

        fs::write(&mode_path, "provider_error").expect("write mode");
        let provider_err = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("assistant provider error must fail");
        assert_eq!(provider_err, OpenCodeCollectorError::ProviderError);
        assert!(!provider_err.to_string().contains(sentinel));
        assert!(!format!("{provider_err:?}").contains(sentinel));
    }

    #[tokio::test]
    async fn timeout_cancel_overflow_and_lingering_descendant_kill_reap_with_no_later_phase() {
        let script = r#"#!/bin/sh
set -eu
ROOT_DIR="$(dirname "$0")"
MODE="$(cat "$ROOT_DIR/mode.txt")"
LOG_FILE="$ROOT_DIR/invocations.log"
PID_FILE="$ROOT_DIR/descendant.pid"
printf '%s\n' "$*" >> "$LOG_FILE"

if [ "$1" = "api" ] && [ "$3" = "session.get" ]; then
    case "$MODE" in
        timeout)
            sleep 60 &
            echo $! > "$PID_FILE"
            wait
            ;;
        cancel)
            sleep 60 &
            echo $! > "$PID_FILE"
            wait
            ;;
        overflow)
            sleep 60 &
            echo $! > "$PID_FILE"
            dd if=/dev/zero bs=1024 count=80 2>/dev/null
            wait
            ;;
        lingering)
            sleep 60 >/dev/null 2>&1 &
            echo $! > "$PID_FILE"
            cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
            exit 0
            ;;
    esac
fi

if [ "$1" = "session" ] && [ "$2" = "export" ]; then
    printf 'EXPORT_SHOULD_NOT_RUN\n' >> "$LOG_FILE"
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let pid_path = fixture.root_dir.join("descendant.pid");
        let target = expected_model_target();
        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };

        // 1. Timeout kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "timeout").expect("write timeout mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let timeout_ctx = fixture.context(Duration::from_millis(120), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&timeout_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("timeout must fail");
        assert_eq!(err, OpenCodeCollectorError::TimedOut);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on timeout"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 2. Cancellation kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "cancel").expect("write cancel mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let cancel_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let cancel_trigger = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = cancel_tx.send(true);
        });
        let err = collect(&cancel_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("cancellation must fail");
        let _ = cancel_trigger.await;
        assert_eq!(err, OpenCodeCollectorError::Cancelled);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on cancellation"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 3. Stdout overflow (>64KiB) kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "overflow").expect("write overflow mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let overflow_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&overflow_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("stdout overflow must fail");
        assert_eq!(err, OpenCodeCollectorError::OutputOverflow);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on overflow"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 4. Lingering descendant after direct child exit 0 is terminated/reaped and prevents success.
        fs::write(&mode_path, "lingering").expect("write lingering mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let lingering_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&lingering_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("lingering descendant after exit 0 must fail cleanup check");
        assert_eq!(err, OpenCodeCollectorError::CleanupIncomplete);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "lingering descendant must be terminated and reaped"
        );
        assert_eq!(fixture.invocations().len(), 1);
    }
}
