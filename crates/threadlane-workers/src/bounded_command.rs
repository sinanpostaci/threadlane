//! Generic bounded-command execution primitive under isolated Unix process-group supervision.
//!
//! Unlike [`crate::supervisor::run_worker`] and [`crate::supervisor::run_advisory_worker`], which
//! require provider-specific NDJSON stream evidence (`antigravity` or `opencode_v2`) and enforce a
//! 4096-byte minimum stream floor, [`run_bounded_command`] and [`run_bounded_command_with_cancel`]
//! execute an arbitrary validated canonical executable and exact `argv` array inside a canonical
//! working directory while honoring finite `timeout_ms`, `max_stdout_bytes`, and `max_stderr_bytes`
//! budgets to the exact byte (`1..=MAX_*`).
//!
//! ## Key Invariants
//!
//! - **No shell composition**: Spawns `spec.executable` directly via [`tokio::process::Command`]
//!   with `spec.argv` passed as literal OS arguments. Shell metacharacters (`;`, `|`, `&&`, `$()`,
//!   backticks, globs, redirections) remain inert literal strings.
//! - **Owned process-group lifecycle**: Spawns the child in a dedicated Unix process group
//!   (`process_group(0)`), verifies that `pgid > 1` and `pgid != caller_pgid()`, holds an RAII
//!   [`ProcessGroupGuard`] for the lifetime of supervision, and signals only `-pgid` combined with
//!   the live [`tokio::process::Child`] handle. Never signals arbitrary stored PIDs.
//! - **Exact byte caps and concurrent pipe drainage**: Drains stdout and stderr concurrently
//!   without clamping small byte budgets up to `4096`. Any single byte beyond `max_stdout_bytes`
//!   or `max_stderr_bytes` immediately triggers process-group termination and records
//!   [`BoundedCommandStatus::OutputOverflow`].
//! - **Truthful terminal status and bounded post-exit drain**: Lingering descendants in the
//!   owned process group are terminated and reaped after direct child exit
//!   (`descendants_terminated_after_exit`). If an escaped descendant outside the process group
//!   holds stdout/stderr pipes open beyond [`POST_EXIT_PIPE_DRAIN_TIMEOUT_MS`], supervision stops
//!   waiting and records [`BoundedCommandStatus::IncompleteCleanup`].
//! - **Cancellation boundary**: Explicit cancellation terminates and reaps only the owned local
//!   process group created for this command; it makes no claim that external or remote side
//!   effects were rolled back.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::{mpsc, watch};

use crate::error::WorkerError;
use crate::fs_safety::{
    ensure_platform_supported, sanitize_and_redact, validate_executable_file,
    validate_existing_canonical_dir,
};
use crate::git::SANITIZED_GIT_ENV_OVERRIDES;
use crate::spec::{MAX_STDERR_BYTES, MAX_STDOUT_BYTES, MAX_TIMEOUT_MS};
use crate::supervisor::{
    extract_exit_code_and_signal, finish_or_terminate_surviving_descendants,
    is_isolated_process_group, is_process_group_alive, terminate_and_reap_group,
    wait_for_cancellation, wait_for_readers_or_signal_stop, ProcessGroupGuard,
    MAX_STDERR_SUMMARY_BYTES, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};

/// Maximum number of arguments permitted in a [`BoundedCommandSpec::argv`].
pub const MAX_BOUNDED_COMMAND_ARGV_LEN: usize = 64;

/// Maximum UTF-8 byte length of a single argument in [`BoundedCommandSpec::argv`].
pub const MAX_BOUNDED_COMMAND_ARG_BYTES: usize = 4096;

/// Maximum number of explicit environment variable overrides in [`BoundedCommandSpec::env_overrides`].
pub const MAX_BOUNDED_COMMAND_ENV_OVERRIDES: usize = 64;

/// Exact finite execution budgets for a [`BoundedCommandSpec`].
///
/// All caps are honored to the exact millisecond or byte without upward clamping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedCommandBudgets {
    /// Finite wall-clock timeout in milliseconds (`1..=MAX_TIMEOUT_MS`).
    pub timeout_ms: u64,
    /// Maximum stdout bytes captured (`1..=MAX_STDOUT_BYTES`).
    pub max_stdout_bytes: usize,
    /// Maximum stderr bytes captured (`1..=MAX_STDERR_BYTES`).
    pub max_stderr_bytes: usize,
}

impl BoundedCommandBudgets {
    /// Validates that `timeout_ms`, `max_stdout_bytes`, and `max_stderr_bytes` are non-zero and
    /// within hard upper bounds.
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.timeout_ms == 0 || self.timeout_ms > MAX_TIMEOUT_MS {
            return Err(WorkerError::InvalidSpec(format!(
                "BoundedCommandBudgets.timeout_ms must be in 1..={MAX_TIMEOUT_MS} (got {})",
                self.timeout_ms
            )));
        }
        if self.max_stdout_bytes == 0 || self.max_stdout_bytes > MAX_STDOUT_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "BoundedCommandBudgets.max_stdout_bytes must be in 1..={MAX_STDOUT_BYTES} (got {})",
                self.max_stdout_bytes
            )));
        }
        if self.max_stderr_bytes == 0 || self.max_stderr_bytes > MAX_STDERR_BYTES {
            return Err(WorkerError::InvalidSpec(format!(
                "BoundedCommandBudgets.max_stderr_bytes must be in 1..={MAX_STDERR_BYTES} (got {})",
                self.max_stderr_bytes
            )));
        }
        Ok(())
    }

    /// Returns the configured wall-clock timeout as a [`Duration`].
    pub const fn timeout_duration(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }
}

/// Validated specification for a generic bounded child process invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedCommandSpec {
    /// Canonical absolute path to a non-symlink executable regular file.
    pub executable: PathBuf,
    /// Exact argument vector passed to `executable` (excluding `argv[0]`).
    #[serde(default)]
    pub argv: Vec<String>,
    /// Canonical absolute path to an existing non-symlink working directory.
    pub working_directory: PathBuf,
    /// Optional explicit environment variable `(key, value)` overrides applied to the child.
    #[serde(default)]
    pub env_overrides: Vec<(String, String)>,
    /// When `true` (recommended), strips all inherited `GIT_*` environment variables and applies
    /// [`SANITIZED_GIT_ENV_OVERRIDES`] so ambient Git variables cannot redirect repository operations.
    #[serde(default = "default_strip_git_env")]
    pub strip_git_env: bool,
    /// Finite wall-clock timeout and exact stdout/stderr byte caps.
    pub budgets: BoundedCommandBudgets,
}

const fn default_strip_git_env() -> bool {
    true
}

impl BoundedCommandSpec {
    /// Validates executable safety, argument bounds, canonical working directory, environment
    /// overrides, and finite execution budgets.
    pub fn validate(&self) -> Result<(), WorkerError> {
        let canonical_exe = validate_executable_file(&self.executable)?;
        if canonical_exe != self.executable {
            return Err(WorkerError::SecurityViolation(format!(
                "BoundedCommandSpec.executable `{}` must match its canonical path `{}`",
                self.executable.display(),
                canonical_exe.display()
            )));
        }

        if self.argv.len() > MAX_BOUNDED_COMMAND_ARGV_LEN {
            return Err(WorkerError::InvalidSpec(format!(
                "BoundedCommandSpec.argv length {} exceeds maximum {MAX_BOUNDED_COMMAND_ARGV_LEN}",
                self.argv.len()
            )));
        }
        for (idx, arg) in self.argv.iter().enumerate() {
            if arg.is_empty() || arg.len() > MAX_BOUNDED_COMMAND_ARG_BYTES {
                return Err(WorkerError::InvalidSpec(format!(
                    "BoundedCommandSpec.argv[{idx}] length {} must be in 1..={MAX_BOUNDED_COMMAND_ARG_BYTES}",
                    arg.len()
                )));
            }
            if arg.as_bytes().contains(&0) {
                return Err(WorkerError::InvalidSpec(format!(
                    "BoundedCommandSpec.argv[{idx}] must not contain NUL bytes"
                )));
            }
        }

        let canonical_workdir =
            validate_existing_canonical_dir(&self.working_directory, "working_directory")?;
        if canonical_workdir != self.working_directory {
            return Err(WorkerError::SecurityViolation(format!(
                "BoundedCommandSpec.working_directory `{}` must match its canonical path `{}`",
                self.working_directory.display(),
                canonical_workdir.display()
            )));
        }

        if self.env_overrides.len() > MAX_BOUNDED_COMMAND_ENV_OVERRIDES {
            return Err(WorkerError::InvalidSpec(format!(
                "BoundedCommandSpec.env_overrides length {} exceeds maximum {MAX_BOUNDED_COMMAND_ENV_OVERRIDES}",
                self.env_overrides.len()
            )));
        }
        for (key, val) in &self.env_overrides {
            if key.is_empty()
                || key.len() > MAX_BOUNDED_COMMAND_ARG_BYTES
                || key.contains('=')
                || key.as_bytes().contains(&0)
            {
                return Err(WorkerError::InvalidSpec(format!(
                    "BoundedCommandSpec.env_overrides key `{key}` is invalid"
                )));
            }
            if val.len() > MAX_BOUNDED_COMMAND_ARG_BYTES || val.as_bytes().contains(&0) {
                return Err(WorkerError::InvalidSpec(format!(
                    "BoundedCommandSpec.env_overrides value for `{key}` is invalid"
                )));
            }
            if self.strip_git_env && key.to_ascii_uppercase().starts_with("GIT_") {
                return Err(WorkerError::SecurityViolation(format!(
                    "BoundedCommandSpec.env_overrides cannot set `{key}` when strip_git_env is true"
                )));
            }
        }

        self.budgets.validate()?;
        Ok(())
    }
}

/// Specific stream budget exceeded when a bounded command terminates with
/// [`BoundedCommandStatus::OutputOverflow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoundedCommandOverflow {
    /// Total bytes emitted on stdout exceeded `budgets.max_stdout_bytes`.
    StdoutExceeded {
        /// Configured stdout byte cap.
        max_stdout_bytes: usize,
    },
    /// Total bytes emitted on stderr exceeded `budgets.max_stderr_bytes`.
    StderrExceeded {
        /// Configured stderr byte cap.
        max_stderr_bytes: usize,
    },
}

/// Terminal outcome of a supervised [`BoundedCommandSpec`] execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundedCommandStatus {
    /// Exited with code `0`, no signal, no timeout, no cancellation, no stream overflow, no
    /// force-terminated surviving descendants, and complete process-group/pipe cleanup.
    Succeeded,
    /// Exited with a non-zero exit code or was terminated by a signal.
    NonZeroExit,
    /// Stdout or stderr exceeded the configured byte cap and the process group was terminated.
    OutputOverflow,
    /// Execution exceeded `budgets.timeout_ms` and the process group was terminated.
    TimedOut,
    /// Execution was explicitly cancelled by the caller and the owned process group was terminated.
    Cancelled,
    /// Direct child exited `0` and physical process-group cleanup completed (`cleanup_incomplete == false`),
    /// but surviving descendant processes in the process group had to be forcibly terminated after
    /// direct child exit (`descendants_terminated_after_exit == true`), so complete natural
    /// execution is not proven.
    DescendantsTerminatedAfterExit,
    /// Process exited, but post-exit pipe drainage timed out or process-group termination remained
    /// incomplete.
    IncompleteCleanup,
}

impl BoundedCommandStatus {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::NonZeroExit => "non_zero_exit",
            Self::OutputOverflow => "output_overflow",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::DescendantsTerminatedAfterExit => "descendants_terminated_after_exit",
            Self::IncompleteCleanup => "incomplete_cleanup",
        }
    }

    /// Returns `true` only when the status is [`BoundedCommandStatus::Succeeded`].
    pub const fn is_succeeded(self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

/// Structured execution report returned by [`run_bounded_command`] and
/// [`run_bounded_command_with_cancel`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedCommandReport {
    /// Canonical executable path that was invoked.
    pub executable: PathBuf,
    /// Exact argument vector passed to `executable` (excluding `argv[0]`).
    pub argv: Vec<String>,
    /// Canonical working directory where the command ran.
    pub working_directory: PathBuf,
    /// Configured execution budgets enforced for this run.
    pub budgets: BoundedCommandBudgets,
    /// Terminal execution status.
    pub status: BoundedCommandStatus,
    /// Process exit code, if exited normally.
    pub exit_code: Option<i32>,
    /// Termination signal number, if terminated by a Unix signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Owned Unix process group ID (`pgid`) created for this run.
    pub process_group_id: Option<i32>,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Raw stdout bytes captured within `budgets.max_stdout_bytes`.
    pub stdout_bytes: Vec<u8>,
    /// Raw stderr bytes captured within `budgets.max_stderr_bytes`.
    pub stderr_bytes: Vec<u8>,
    /// Number of stdout bytes retained in `stdout_bytes` (`<= budgets.max_stdout_bytes`).
    pub stdout_bytes_captured: usize,
    /// Number of stderr bytes retained in `stderr_bytes` (`<= budgets.max_stderr_bytes`).
    pub stderr_bytes_captured: usize,
    /// True if the child emitted more than `budgets.max_stdout_bytes` on stdout.
    pub stdout_overflowed: bool,
    /// True if the child emitted more than `budgets.max_stderr_bytes` on stderr.
    pub stderr_overflowed: bool,
    /// Primary overflow classification when `stdout_overflowed` or `stderr_overflowed` is true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overflow: Option<BoundedCommandOverflow>,
    /// True if lingering descendant processes in the owned process group survived the direct
    /// child's exit and had to be terminated and reaped.
    pub descendants_terminated_after_exit: bool,
    /// True if stdout/stderr pipes remained held open past [`POST_EXIT_PIPE_DRAIN_TIMEOUT_MS`]
    /// or if the owned process group remained alive after termination attempts.
    pub cleanup_incomplete: bool,
    /// Bounded, secret-redacted UTF-8 summary of captured stdout (if non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted_stdout_summary: Option<String>,
    /// Bounded, secret-redacted UTF-8 summary of captured stderr (if non-empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted_stderr_summary: Option<String>,
}

impl BoundedCommandReport {
    /// Returns `true` if and only if the command exited `0` cleanly without signal, timeout,
    /// cancellation, stdout/stderr overflow, force-terminated surviving descendants, or incomplete
    /// cleanup, and captured buffers are internally consistent with configured caps.
    pub fn passed(&self) -> bool {
        self.status == BoundedCommandStatus::Succeeded
            && self.exit_code == Some(0)
            && self.signal.is_none()
            && !self.stdout_overflowed
            && !self.stderr_overflowed
            && self.overflow.is_none()
            && !self.descendants_terminated_after_exit
            && !self.cleanup_incomplete
            && self.stdout_bytes.len() == self.stdout_bytes_captured
            && self.stderr_bytes.len() == self.stderr_bytes_captured
            && self.stdout_bytes_captured <= self.budgets.max_stdout_bytes
            && self.stderr_bytes_captured <= self.budgets.max_stderr_bytes
    }
}

/// Runs a validated [`BoundedCommandSpec`] with a no-op cancellation channel.
pub async fn run_bounded_command(
    spec: &BoundedCommandSpec,
) -> Result<BoundedCommandReport, WorkerError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_bounded_command_with_cancel(spec, &mut cancel_rx).await
}

/// Runs a validated [`BoundedCommandSpec`] under isolated Unix process-group supervision,
/// concurrent exact-cap stdout/stderr pipe drainage, and timeout/cancellation/overflow enforcement.
///
/// Cancellation (`cancel_rx`) and timeout terminate and reap only the owned process group created
/// for this invocation; they never signal arbitrary stored PIDs and never claim external or remote
/// effects were undone.
pub async fn run_bounded_command_with_cancel(
    spec: &BoundedCommandSpec,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<BoundedCommandReport, WorkerError> {
    ensure_platform_supported()?;
    spec.validate()?;

    let start_instant = Instant::now();
    let overall_deadline =
        tokio::time::Instant::from_std(start_instant + spec.budgets.timeout_duration());

    let mut cmd = Command::new(&spec.executable);
    if spec.strip_git_env {
        for (key, _) in std::env::vars_os() {
            if key
                .to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("GIT_")
            {
                cmd.env_remove(&key);
            }
        }
        for &(k, v) in SANITIZED_GIT_ENV_OVERRIDES {
            cmd.env(k, v);
        }
    }
    cmd.args(&spec.argv)
        .envs(spec.env_overrides.iter().map(|(k, v)| (k, v)))
        .current_dir(&spec.working_directory)
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
            "spawned bounded command child had no OS process ID",
        ))
    })?;
    let pgid = i32::try_from(child_pid).map_err(|_| {
        WorkerError::Io(std::io::Error::other(format!(
            "bounded command PID {child_pid} exceeds i32 range"
        )))
    })?;

    if !is_isolated_process_group(pgid) {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(WorkerError::SecurityViolation(format!(
            "spawned bounded command child did not enter an isolated process group (pgid={pgid})"
        )));
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let stdout_pipe = child.stdout.take().ok_or_else(|| {
        WorkerError::Io(std::io::Error::other(
            "failed to capture bounded command stdout pipe",
        ))
    })?;
    let stderr_pipe = child.stderr.take().ok_or_else(|| {
        WorkerError::Io(std::io::Error::other(
            "failed to capture bounded command stderr pipe",
        ))
    })?;

    let (overflow_tx, mut overflow_rx) = mpsc::unbounded_channel::<BoundedCommandOverflow>();
    let (drain_stop_tx, drain_stop_rx) = watch::channel(false);

    let max_stdout_bytes = spec.budgets.max_stdout_bytes;
    let max_stderr_bytes = spec.budgets.max_stderr_bytes;

    let mut stdout_handle = tokio::spawn(drain_pipe_exact(
        stdout_pipe,
        max_stdout_bytes,
        BoundedCommandOverflow::StdoutExceeded { max_stdout_bytes },
        overflow_tx.clone(),
        drain_stop_rx.clone(),
    ));
    let mut stderr_handle = tokio::spawn(drain_pipe_exact(
        stderr_pipe,
        max_stderr_bytes,
        BoundedCommandOverflow::StderrExceeded { max_stderr_bytes },
        overflow_tx,
        drain_stop_rx,
    ));

    let mut timed_out = false;
    let mut cancelled = false;
    let mut recorded_overflow: Option<BoundedCommandOverflow> = None;
    let mut descendants_terminated_after_exit = false;
    let exit_status: Option<ExitStatus>;

    if *cancel_rx.borrow_and_update() {
        cancelled = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else if tokio::time::Instant::now() >= overall_deadline {
        timed_out = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else {
        let timeout_sleep = tokio::time::sleep_until(overall_deadline);
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

    let PipeDrainOutput {
        bytes: stdout_bytes,
        summary: redacted_stdout_summary,
        overflow: stdout_overflow,
    } = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(err)) => {
            return Err(WorkerError::Io(std::io::Error::other(format!(
                "bounded command stdout task join error: {err}"
            ))));
        }
        Err(_) => {
            stdout_handle.abort();
            cleanup_incomplete = true;
            PipeDrainOutput {
                bytes: Vec::new(),
                summary: None,
                overflow: None,
            }
        }
    };

    let PipeDrainOutput {
        bytes: stderr_bytes,
        summary: redacted_stderr_summary,
        overflow: stderr_overflow,
    } = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stderr_handle,
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(err)) => {
            return Err(WorkerError::Io(std::io::Error::other(format!(
                "bounded command stderr task join error: {err}"
            ))));
        }
        Err(_) => {
            stderr_handle.abort();
            cleanup_incomplete = true;
            PipeDrainOutput {
                bytes: Vec::new(),
                summary: None,
                overflow: None,
            }
        }
    };

    let stdout_overflowed = stdout_overflow.is_some()
        || matches!(
            recorded_overflow,
            Some(BoundedCommandOverflow::StdoutExceeded { .. })
        );
    let stderr_overflowed = stderr_overflow.is_some()
        || matches!(
            recorded_overflow,
            Some(BoundedCommandOverflow::StderrExceeded { .. })
        );
    let final_overflow = recorded_overflow.or(stdout_overflow).or(stderr_overflow);
    let (exit_code, signal) = extract_exit_code_and_signal(exit_status);

    let status = classify_bounded_status(
        cancelled,
        timed_out,
        final_overflow.is_some(),
        cleanup_incomplete,
        exit_code,
        signal,
        descendants_terminated_after_exit,
    );

    let duration_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stdout_bytes_captured = stdout_bytes.len();
    let stderr_bytes_captured = stderr_bytes.len();

    Ok(BoundedCommandReport {
        executable: spec.executable.clone(),
        argv: spec.argv.clone(),
        working_directory: spec.working_directory.clone(),
        budgets: spec.budgets,
        status,
        exit_code,
        signal,
        process_group_id: Some(pgid),
        duration_ms,
        stdout_bytes,
        stderr_bytes,
        stdout_bytes_captured,
        stderr_bytes_captured,
        stdout_overflowed,
        stderr_overflowed,
        overflow: final_overflow,
        descendants_terminated_after_exit,
        cleanup_incomplete,
        redacted_stdout_summary,
        redacted_stderr_summary,
    })
}

struct PipeDrainOutput {
    bytes: Vec<u8>,
    summary: Option<String>,
    overflow: Option<BoundedCommandOverflow>,
}

async fn drain_pipe_exact<R>(
    mut reader: R,
    max_bytes: usize,
    overflow_kind: BoundedCommandOverflow,
    overflow_tx: mpsc::UnboundedSender<BoundedCommandOverflow>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> PipeDrainOutput
where
    R: AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::with_capacity(max_bytes.min(4096));
    let mut chunk = [0u8; 4096];
    let mut overflow: Option<BoundedCommandOverflow> = None;

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
                let remaining = max_bytes.saturating_sub(buf.len());
                if n > remaining {
                    if remaining > 0 {
                        buf.extend_from_slice(&chunk[..remaining]);
                    }
                    overflow = Some(overflow_kind);
                    let _ = overflow_tx.send(overflow_kind);
                    break;
                }
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

    PipeDrainOutput {
        bytes: buf,
        summary,
        overflow,
    }
}

fn classify_bounded_status(
    cancelled: bool,
    timed_out: bool,
    overflowed: bool,
    cleanup_incomplete: bool,
    exit_code: Option<i32>,
    signal: Option<i32>,
    descendants_terminated_after_exit: bool,
) -> BoundedCommandStatus {
    if cancelled {
        return BoundedCommandStatus::Cancelled;
    }
    if timed_out {
        return BoundedCommandStatus::TimedOut;
    }
    if overflowed {
        return BoundedCommandStatus::OutputOverflow;
    }
    if cleanup_incomplete {
        return BoundedCommandStatus::IncompleteCleanup;
    }
    if exit_code != Some(0) || signal.is_some() {
        return BoundedCommandStatus::NonZeroExit;
    }
    if descendants_terminated_after_exit {
        return BoundedCommandStatus::DescendantsTerminatedAfterExit;
    }
    BoundedCommandStatus::Succeeded
}
