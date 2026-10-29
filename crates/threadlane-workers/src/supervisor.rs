use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use crate::error::WorkerError;
use crate::fs_safety::{
    ensure_platform_supported, sanitize_and_redact, validate_existing_canonical_dir,
};
use crate::spec::{ExecutionBudgets, ResolvedModelTarget, WorkerProvider, WorkerSpec};
use crate::stream::{FieldEvidence, NdjsonStreamParser, StreamEvidenceSummary};

/// Maximum byte length of the sanitized stderr diagnostic summary in [`WorkerExecutionReport`].
pub const MAX_STDERR_SUMMARY_BYTES: usize = 512;

/// Bounded grace window (ms) for stdout/stderr reader tasks to reach EOF after process-group termination.
pub const POST_EXIT_PIPE_DRAIN_TIMEOUT_MS: u64 = 250;

/// Specific stream budget that was exceeded when a worker run terminates with [`WorkerRunStatus::OutputOverflow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputOverflowKind {
    /// A single line on stdout exceeded `budgets.max_line_bytes`.
    StdoutLineExceeded {
        /// Configured per-line byte limit.
        max_line_bytes: usize,
    },
    /// Total bytes emitted on stdout exceeded `budgets.max_stdout_bytes`.
    StdoutTotalExceeded {
        /// Configured total stdout byte limit.
        max_stdout_bytes: usize,
    },
    /// Total bytes emitted on stderr exceeded `budgets.max_stderr_bytes`.
    StderrTotalExceeded {
        /// Configured total stderr byte limit.
        max_stderr_bytes: usize,
    },
}

/// Terminal outcome of a supervised worker process invocation.
///
/// Note that [`WorkerRunStatus::Succeeded`] means only that the worker process exited cleanly
/// with verified stream evidence (matching model, effort/variant, and terminal result event).
/// It is **not** accepted work until the post-run Git diff gate and primary review succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRunStatus {
    /// Exited 0 with verified model, effort/variant, and completion result evidence.
    Succeeded,
    /// Blocked on tool/edit permission or interactive user input.
    Blocked,
    /// Provider NDJSON stream reported an error event (even if process exit code was 0).
    ProviderError,
    /// Observed model in stream did not match the requested model or conflicted across events.
    ModelMismatch,
    /// Stream did not emit any model evidence to verify the requested model.
    MissingModelEvidence,
    /// Observed effort/variant in stream did not match the requested effort/variant.
    EffortMismatch,
    /// Stream did not emit effort/variant evidence when an explicit effort/variant was requested.
    MissingEffortEvidence,
    /// Observed session ID in stream did not match the requested session ID.
    SessionMismatch,
    /// Process exited 0 without emitting a terminal result/completion event.
    MissingResultEvidence,
    /// Stdout contained one or more malformed (non-JSON or non-object) lines.
    MalformedStream,
    /// Stdout line, stdout total, or stderr total exceeded configured byte budgets.
    OutputOverflow,
    /// Execution exceeded `budgets.timeout_ms` and the process group was terminated.
    TimedOut,
    /// Execution was explicitly cancelled by the caller and the process group was terminated.
    Cancelled,
    /// Process exited, but post-exit process-group or pipe-drain cleanup remained incomplete
    /// (for example, an escaped/reparented descendant outside the process group held stdout/stderr pipes open).
    IncompleteCleanup,
    /// Process exited with a non-zero exit code or was terminated by a signal.
    NonZeroExit,
}

impl WorkerRunStatus {
    /// Returns the canonical snake_case status name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Blocked => "blocked",
            Self::ProviderError => "provider_error",
            Self::ModelMismatch => "model_mismatch",
            Self::MissingModelEvidence => "missing_model_evidence",
            Self::EffortMismatch => "effort_mismatch",
            Self::MissingEffortEvidence => "missing_effort_evidence",
            Self::SessionMismatch => "session_mismatch",
            Self::MissingResultEvidence => "missing_result_evidence",
            Self::MalformedStream => "malformed_stream",
            Self::OutputOverflow => "output_overflow",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::IncompleteCleanup => "incomplete_cleanup",
            Self::NonZeroExit => "non_zero_exit",
        }
    }

    /// Returns `true` when the worker exited 0 with verified model, session, and terminal result
    /// evidence, and only lacked effort evidence because the provider stream omitted it.
    pub fn is_completed_pending_effort_evidence(self) -> bool {
        self == Self::MissingEffortEvidence
    }
}

/// Bounded structured report of a single supervised worker process execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerExecutionReport {
    /// Provider executed.
    pub provider: WorkerProvider,
    /// Requested model and effort/variant target.
    pub requested_model: ResolvedModelTarget,
    /// Requested session ID, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_session_id: Option<String>,
    /// Exact argument vector passed to the worker executable.
    pub constructed_argv: Vec<String>,
    /// Canonical working directory where the worker ran.
    pub working_directory: PathBuf,
    /// Verified execution outcome.
    pub status: WorkerRunStatus,
    /// Process exit status code, if exited normally.
    pub exit_code: Option<i32>,
    /// Termination signal number, if terminated by a Unix signal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Owned Unix process group ID (`pgid`) created for this run.
    pub process_group_id: Option<i32>,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Total stdout bytes processed within budget.
    pub stdout_bytes_captured: usize,
    /// Total stderr bytes processed within budget.
    pub stderr_bytes_captured: usize,
    /// Overflow details when `status == OutputOverflow`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub overflow: Option<OutputOverflowKind>,
    /// True if lingering descendant processes in the worker's process group survived the
    /// direct child's exit and had to be forcefully terminated and reaped.
    pub descendants_terminated_after_exit: bool,
    /// True if stdout/stderr pipes remained held open after process-group termination
    /// (e.g. by an escaped/reparented descendant) or if process-group termination remained incomplete.
    #[serde(default)]
    pub cleanup_incomplete: bool,
    /// Bounded, secret-redacted summary of stderr output (if non-empty).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redacted_stderr_summary: Option<String>,
    /// Parsed NDJSON stream evidence and bounded normalized events.
    pub evidence: StreamEvidenceSummary,
}

#[cfg(unix)]
#[allow(unsafe_code)]
pub(crate) mod unix_pgroup {
    use std::fs;
    use std::io;

    /// Returns the caller's own process group ID.
    pub(crate) fn caller_pgid() -> i32 {
        // SAFETY: `getpgrp` is a thread-safe POSIX syscall taking no arguments.
        unsafe { libc::getpgrp() }
    }

    /// Sends `sig` to the entire Unix process group `-pgid` after verifying `pgid > 1`
    /// and `pgid != caller_pgid()`.
    pub(crate) fn signal_process_group(pgid: i32, sig: libc::c_int) -> io::Result<()> {
        if pgid <= 1 || pgid == caller_pgid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("refusing to signal invalid or unowned process group {pgid}"),
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

    /// Checks whether any non-zombie process still exists in process group `pgid`.
    pub(crate) fn is_process_group_alive(pgid: i32) -> bool {
        if pgid <= 1 {
            return false;
        }
        // SAFETY: `kill(-pgid, 0)` with `pgid > 1` performs an existence check without sending a signal.
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

    /// Checks whether a specific PID is alive (and not a zombie on Linux).
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
            if after.is_some_and(|s| s.starts_with('Z') || s.starts_with('X')) {
                return false;
            }
        }
        true
    }

    /// Sets `O_NONBLOCK` on a raw pipe file descriptor.
    pub(crate) fn set_fd_nonblocking(fd: libc::c_int) -> io::Result<()> {
        // SAFETY: `fcntl` with `F_GETFL`/`F_SETFL` on a valid open pipe fd is safe.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Polls up to two pipe file descriptors for readability or hangup up to `timeout_ms`.
    pub(crate) fn poll_pipe_fds(
        fd_a: Option<libc::c_int>,
        fd_b: Option<libc::c_int>,
        timeout_ms: libc::c_int,
    ) -> io::Result<()> {
        let mut fds = [
            libc::pollfd {
                fd: fd_a.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: fd_b.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: `fds` is a valid stack array of 2 `pollfd` structs; negative `fd` entries are ignored by `poll`.
        let rc = unsafe { libc::poll(fds.as_mut_ptr(), 2, timeout_ms) };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }
}

/// Returns `true` if a process with `pid` is currently alive (and not a zombie).
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

/// RAII guard that ensures an owned worker or Git Unix process group (`-pgid`) is signaled with
/// `SIGTERM` and `SIGKILL` if the supervising scope exits or is dropped/aborted early.
pub(crate) struct ProcessGroupGuard {
    pgid: i32,
    disarmed: bool,
}

impl ProcessGroupGuard {
    pub(crate) fn new(pgid: i32) -> Self {
        Self {
            pgid,
            disarmed: false,
        }
    }

    pub(crate) fn disarm(&mut self) {
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

async fn wait_for_cancellation(cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        if *cancel_rx.borrow_and_update() {
            return;
        }
        if cancel_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Runs a pre-validated [`WorkerSpec`] inside `working_directory` with a no-op cancellation channel.
pub async fn run_worker(
    spec: &WorkerSpec,
    working_directory: &Path,
) -> Result<WorkerExecutionReport, WorkerError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_worker_with_cancel(spec, working_directory, &mut cancel_rx).await
}

/// Runs a [`WorkerSpec`] inside `working_directory` under Unix process-group supervision,
/// concurrent incremental stdout/stderr drainage, and timeout/cancel/overflow enforcement.
///
/// Never performs automatic replay if execution crashes or times out.
pub async fn run_worker_with_cancel(
    spec: &WorkerSpec,
    working_directory: &Path,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<WorkerExecutionReport, WorkerError> {
    ensure_platform_supported()?;
    let validation = spec.validate()?;
    let canonical_workdir =
        validate_existing_canonical_dir(working_directory, "working_directory")?;

    let start_instant = Instant::now();

    let mut cmd = Command::new(&validation.canonical_executable);
    cmd.args(&validation.constructed_argv)
        .current_dir(&canonical_workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd.spawn()?;
    let child_pid = child.id().ok_or_else(|| {
        WorkerError::Io(std::io::Error::other(
            "spawned worker child had no OS process ID",
        ))
    })?;
    let pgid = i32::try_from(child_pid).map_err(|_| {
        WorkerError::Io(std::io::Error::other(format!(
            "worker PID {child_pid} exceeds i32 range"
        )))
    })?;

    #[cfg(unix)]
    if pgid <= 1 || pgid == unix_pgroup::caller_pgid() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(WorkerError::SecurityViolation(format!(
            "spawned worker child did not enter an isolated process group (pgid={pgid})"
        )));
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let stdout_pipe = child.stdout.take().ok_or_else(|| {
        WorkerError::Io(std::io::Error::other(
            "failed to capture worker stdout pipe",
        ))
    })?;
    let stderr_pipe = child.stderr.take().ok_or_else(|| {
        WorkerError::Io(std::io::Error::other(
            "failed to capture worker stderr pipe",
        ))
    })?;

    let (overflow_tx, mut overflow_rx) = mpsc::unbounded_channel::<OutputOverflowKind>();
    let (drain_stop_tx, drain_stop_rx) = watch::channel(false);

    let mut stdout_handle = tokio::spawn(drain_stdout_incremental(
        stdout_pipe,
        validation.resolved_model.clone(),
        spec.session_id.clone(),
        spec.budgets.clone(),
        overflow_tx.clone(),
        drain_stop_rx.clone(),
    ));

    let mut stderr_handle = tokio::spawn(drain_stderr_incremental(
        stderr_pipe,
        spec.budgets.max_stderr_bytes,
        overflow_tx,
        drain_stop_rx,
    ));

    let mut timed_out = false;
    let mut cancelled = false;
    let mut recorded_overflow: Option<OutputOverflowKind> = None;
    let mut descendants_terminated_after_exit = false;
    let exit_status: Option<ExitStatus>;

    if *cancel_rx.borrow_and_update() {
        cancelled = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else {
        let timeout_sleep = tokio::time::sleep(spec.budgets.timeout_duration());
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
            Some(overflow_kind) = overflow_rx.recv() => {
                recorded_overflow = Some(overflow_kind);
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

    let group_still_alive = is_process_group_alive(pgid);
    if !group_still_alive {
        pgroup_guard.disarm();
    }
    let mut cleanup_incomplete = pipe_drain_timed_out || group_still_alive;

    let (stdout_bytes_captured, evidence, stdout_overflow) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(tuple)) => tuple,
        Ok(Err(err)) => {
            return Err(WorkerError::Io(std::io::Error::other(format!(
                "stdout task join error: {err}"
            ))));
        }
        Err(_) => {
            stdout_handle.abort();
            cleanup_incomplete = true;
            let empty_parser = NdjsonStreamParser::new(
                validation.resolved_model.clone(),
                spec.session_id.clone(),
                spec.budgets.max_events,
            );
            (0, empty_parser.finalize(), None)
        }
    };

    let (stderr_bytes_captured, redacted_stderr_summary, stderr_overflow) =
        match tokio::time::timeout(
            Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
            &mut stderr_handle,
        )
        .await
        {
            Ok(Ok(tuple)) => tuple,
            Ok(Err(err)) => {
                return Err(WorkerError::Io(std::io::Error::other(format!(
                    "stderr task join error: {err}"
                ))));
            }
            Err(_) => {
                stderr_handle.abort();
                cleanup_incomplete = true;
                (0, None, None)
            }
        };

    let final_overflow = recorded_overflow.or(stdout_overflow).or(stderr_overflow);
    let (exit_code, signal) = extract_exit_code_and_signal(exit_status);

    let status = classify_run_status(
        cancelled,
        timed_out,
        final_overflow,
        cleanup_incomplete,
        exit_code,
        signal,
        &evidence,
    );

    let duration_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);

    Ok(WorkerExecutionReport {
        provider: spec.provider,
        requested_model: validation.resolved_model,
        requested_session_id: spec.session_id.clone(),
        constructed_argv: validation.constructed_argv,
        working_directory: canonical_workdir,
        status,
        exit_code,
        signal,
        process_group_id: Some(pgid),
        duration_ms,
        stdout_bytes_captured,
        stderr_bytes_captured,
        overflow: final_overflow,
        descendants_terminated_after_exit,
        cleanup_incomplete,
        redacted_stderr_summary,
        evidence,
    })
}

async fn wait_for_readers_or_signal_stop<T1, T2>(
    stdout_handle: &tokio::task::JoinHandle<T1>,
    stderr_handle: &tokio::task::JoinHandle<T2>,
    drain_stop_tx: &watch::Sender<bool>,
) -> bool {
    let steps = (POST_EXIT_PIPE_DRAIN_TIMEOUT_MS / 5).max(1);
    for _ in 0..steps {
        if stdout_handle.is_finished() && stderr_handle.is_finished() {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if stdout_handle.is_finished() && stderr_handle.is_finished() {
        return false;
    }
    let _ = drain_stop_tx.send(true);
    true
}

async fn drain_stdout_incremental(
    mut reader: tokio::process::ChildStdout,
    requested_model: ResolvedModelTarget,
    requested_session_id: Option<String>,
    budgets: ExecutionBudgets,
    overflow_tx: mpsc::UnboundedSender<OutputOverflowKind>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (usize, StreamEvidenceSummary, Option<OutputOverflowKind>) {
    let max_line_bytes = budgets.max_line_bytes;
    let max_stdout_bytes = budgets.max_stdout_bytes;
    let mut parser =
        NdjsonStreamParser::new(requested_model, requested_session_id, budgets.max_events);
    let mut total_bytes: usize = 0;
    let mut line_buf: Vec<u8> = Vec::with_capacity(4096.min(max_line_bytes));
    let mut chunk = [0u8; 4096];
    let mut overflow: Option<OutputOverflowKind> = None;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_cancellation(&mut drain_stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            if overflow.is_none() && !line_buf.is_empty() {
                parser.ingest_line(&line_buf);
                line_buf.clear();
            }
            break;
        };
        match read_res {
            Ok(0) => {
                if overflow.is_none() && !line_buf.is_empty() {
                    parser.ingest_line(&line_buf);
                    line_buf.clear();
                }
                break;
            }
            Ok(n) => {
                let mut slice = &chunk[..n];
                while !slice.is_empty() {
                    let newline_pos = slice.iter().position(|&b| b == b'\n');
                    let (segment, has_newline, rest) = match newline_pos {
                        Some(pos) => (&slice[..pos], true, &slice[pos + 1..]),
                        None => (slice, false, &[][..]),
                    };

                    let consumed = segment.len() + usize::from(has_newline);
                    if total_bytes.saturating_add(consumed) > max_stdout_bytes {
                        let kind = OutputOverflowKind::StdoutTotalExceeded { max_stdout_bytes };
                        overflow = Some(kind);
                        let _ = overflow_tx.send(kind);
                        break;
                    }
                    total_bytes += consumed;

                    if line_buf.len().saturating_add(segment.len()) > max_line_bytes {
                        let kind = OutputOverflowKind::StdoutLineExceeded { max_line_bytes };
                        overflow = Some(kind);
                        let _ = overflow_tx.send(kind);
                        break;
                    }
                    line_buf.extend_from_slice(segment);

                    if has_newline {
                        parser.ingest_line(&line_buf);
                        line_buf.clear();
                    }

                    slice = rest;
                }

                if overflow.is_some() {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    (total_bytes, parser.finalize(), overflow)
}

async fn drain_stderr_incremental(
    mut reader: tokio::process::ChildStderr,
    max_stderr_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<OutputOverflowKind>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (usize, Option<String>, Option<OutputOverflowKind>) {
    let mut total_bytes: usize = 0;
    let mut buf: Vec<u8> = Vec::with_capacity(4096.min(max_stderr_bytes));
    let mut chunk = [0u8; 4096];
    let mut overflow: Option<OutputOverflowKind> = None;

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
                    let remaining = max_stderr_bytes.saturating_sub(buf.len());
                    if remaining > 0 {
                        buf.extend_from_slice(&chunk[..remaining.min(n)]);
                    }
                    total_bytes = total_bytes.saturating_add(n);
                    let kind = OutputOverflowKind::StderrTotalExceeded { max_stderr_bytes };
                    overflow = Some(kind);
                    let _ = overflow_tx.send(kind);
                    break;
                }
                total_bytes += n;
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => break,
        }
    }

    let summary = if buf.is_empty() {
        None
    } else {
        let raw = String::from_utf8_lossy(&buf);
        let sanitized = sanitize_and_redact(&raw, MAX_STDERR_SUMMARY_BYTES);
        if sanitized.trim().is_empty() {
            None
        } else {
            Some(sanitized)
        }
    };

    (total_bytes, summary, overflow)
}

async fn finish_or_terminate_surviving_descendants<T1, T2>(
    pgid: i32,
    stdout_handle: &tokio::task::JoinHandle<T1>,
    stderr_handle: &tokio::task::JoinHandle<T2>,
) -> bool {
    for _ in 0..10 {
        if stdout_handle.is_finished()
            && stderr_handle.is_finished()
            && !is_process_group_alive(pgid)
        {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let all_done =
        stdout_handle.is_finished() && stderr_handle.is_finished() && !is_process_group_alive(pgid);
    if all_done {
        return false;
    }

    terminate_process_group_members(pgid).await;
    true
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

fn extract_exit_code_and_signal(status: Option<ExitStatus>) -> (Option<i32>, Option<i32>) {
    let Some(st) = status else {
        return (None, None);
    };
    let code = st.code();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        (code, st.signal())
    }
    #[cfg(not(unix))]
    {
        (code, None)
    }
}

fn classify_run_status(
    cancelled: bool,
    timed_out: bool,
    overflow: Option<OutputOverflowKind>,
    cleanup_incomplete: bool,
    exit_code: Option<i32>,
    signal: Option<i32>,
    evidence: &StreamEvidenceSummary,
) -> WorkerRunStatus {
    if cancelled {
        return WorkerRunStatus::Cancelled;
    }
    if timed_out {
        return WorkerRunStatus::TimedOut;
    }
    if overflow.is_some() {
        return WorkerRunStatus::OutputOverflow;
    }
    if cleanup_incomplete {
        return WorkerRunStatus::IncompleteCleanup;
    }
    if !evidence.blocked_events.is_empty() {
        return WorkerRunStatus::Blocked;
    }
    if !evidence.provider_errors.is_empty() {
        return WorkerRunStatus::ProviderError;
    }
    if evidence.malformed_lines_count > 0 {
        return WorkerRunStatus::MalformedStream;
    }
    if exit_code != Some(0) || signal.is_some() {
        return WorkerRunStatus::NonZeroExit;
    }
    if matches!(evidence.model_evidence, FieldEvidence::Mismatch { .. }) {
        return WorkerRunStatus::ModelMismatch;
    }
    if matches!(evidence.model_evidence, FieldEvidence::Missing { .. }) {
        return WorkerRunStatus::MissingModelEvidence;
    }
    if matches!(evidence.effort_evidence, FieldEvidence::Mismatch { .. }) {
        return WorkerRunStatus::EffortMismatch;
    }
    if !evidence.session_evidence.is_satisfied() {
        return WorkerRunStatus::SessionMismatch;
    }
    if !evidence.saw_result_event {
        return WorkerRunStatus::MissingResultEvidence;
    }
    if matches!(evidence.effort_evidence, FieldEvidence::Missing { .. }) {
        return WorkerRunStatus::MissingEffortEvidence;
    }
    WorkerRunStatus::Succeeded
}
