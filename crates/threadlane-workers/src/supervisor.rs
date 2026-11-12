use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use crate::advisory_response::{
    AdvisoryResponseBudget, AdvisoryResponseCollector, AdvisoryStructuredExecution,
};
use crate::antigravity_diagnostics::start_owned_capture;
use crate::error::WorkerError;
#[cfg(target_os = "linux")]
use crate::fs_safety::validate_executable_file;
use crate::fs_safety::{
    ensure_platform_supported, sanitize_and_redact, validate_existing_canonical_dir,
};
use crate::opencode_collector::{
    self, OpenCodeCollectorContext, OpenCodeCollectorDiagnostic, OpenCodeCollectorError,
};
use crate::opencode_metadata::{
    OpenCodeMetadataDiagnostic, OpenCodeMetadataEvidence, OpenCodeMetadataStage,
    OwnedOpenCodeSession,
};
use crate::progress_watchdog::{ProgressWatchdog, ProgressWatchdogSnapshot};
use crate::provider_health::{HealthPublisher, ProviderHealthObserver, ProviderHealthSnapshot};
use crate::spec::{
    extract_pinned_codex_read_only_output_last_message_path, is_pinned_codex_read_only_argv,
    is_pinned_codex_task_files_argv, splice_codex_output_last_message_arg, AdvisoryWorkerSpec,
    ExecutionBudgets, ResolvedModelTarget, WorkerProvider, WorkerSpec,
};
use crate::stream::{
    FieldEvidence, NdjsonStreamParser, NormalizedEventKind, NormalizedWorkerEvent,
    StreamEvidenceSummary,
};

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
    /// Canonical executable actually selected for the supervised launch (absent in legacy reports).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executed_executable: Option<PathBuf>,
    /// Requested model and effort/variant target.
    pub requested_model: ResolvedModelTarget,
    /// Requested session ID, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_session_id: Option<String>,
    /// Software-allocated session ID created for a new OpenCode v2 run, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_session_id: Option<String>,
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
    /// Bounded, secret-redacted stderr summary; suppressed for health-aware AGY invocations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redacted_stderr_summary: Option<String>,
    /// Number of live normalized events that could not be delivered to the optional
    /// `progress_tx` observer channel because it was full or closed (`0` when `progress_tx` is `None`).
    #[serde(default)]
    pub dropped_live_events: usize,
    /// Parsed NDJSON stream evidence and bounded normalized events.
    pub evidence: StreamEvidenceSummary,
    /// Compact allowlisted OpenCode v2 session metadata evidence, when collected and validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode_metadata: Option<OpenCodeMetadataEvidence>,
    /// Finite static error code (or stage-qualified `invalid_metadata:<stage>` code) when
    /// OpenCode v2 session allocation or metadata collection failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opencode_metadata_error: Option<String>,
    /// Advisory tool-hint supervision, independent of evidence verification and acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_watchdog: Option<ProgressWatchdogSnapshot>,
    /// Bounded unverified diagnostic hints, independent of execution/acceptance evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_health: Option<ProviderHealthSnapshot>,
}

impl WorkerExecutionReport {
    /// Returns the validated OpenCode v2 metadata evidence only when this report represents a
    /// clean, non-contradictory, software-allocated OpenCode v2 run whose stream and process
    /// evidence do not conflict with the metadata.
    pub fn validated_opencode_metadata(&self) -> Option<&OpenCodeMetadataEvidence> {
        if self.provider != WorkerProvider::OpenCodeV2
            || self.requested_session_id.is_some()
            || self.opencode_metadata_error.is_some()
            || self.exit_code != Some(0)
            || self.signal.is_some()
            || self.cleanup_incomplete
            || self.overflow.is_some()
            || !self.evidence.blocked_events.is_empty()
            || !self.evidence.provider_errors.is_empty()
            || self.evidence.malformed_lines_count > 0
            || matches!(
                self.status,
                WorkerRunStatus::Cancelled
                    | WorkerRunStatus::TimedOut
                    | WorkerRunStatus::OutputOverflow
                    | WorkerRunStatus::IncompleteCleanup
                    | WorkerRunStatus::Blocked
                    | WorkerRunStatus::ProviderError
                    | WorkerRunStatus::MalformedStream
                    | WorkerRunStatus::NonZeroExit
                    | WorkerRunStatus::ModelMismatch
                    | WorkerRunStatus::EffortMismatch
                    | WorkerRunStatus::SessionMismatch
            )
            || matches!(self.evidence.model_evidence, FieldEvidence::Mismatch { .. })
            || matches!(
                self.evidence.effort_evidence,
                FieldEvidence::Mismatch { .. }
            )
            || !self.evidence.session_evidence.is_satisfied()
        {
            return None;
        }

        let allocated_id = self.allocated_session_id.as_deref()?;
        if allocated_id.is_empty() {
            return None;
        }
        let meta = self.opencode_metadata.as_ref()?;
        if meta.session_id != allocated_id
            || meta.source != "opencode_sanitized_session_export"
            || meta.strength != "cli_reported_per_assistant"
            || meta.assistant_message_count == 0
            || meta.terminal_message_id.trim().is_empty()
        {
            return None;
        }

        if let Some(obs_session) = self.evidence.observed_session_id.as_deref() {
            if obs_session != allocated_id {
                return None;
            }
        }
        if let FieldEvidence::VerifiedMatch { ref value } = self.evidence.session_evidence {
            if value != allocated_id {
                return None;
            }
        }

        if meta.observed_model != self.requested_model.base_model
            && meta.observed_model != self.requested_model.cli_model_arg
        {
            return None;
        }
        if let FieldEvidence::VerifiedMatch { ref value } = self.evidence.model_evidence {
            if value != &meta.observed_model && value != &self.requested_model.base_model {
                return None;
            }
        }

        if let Some(req_effort) = self.requested_model.effort.as_deref() {
            if meta.observed_effort.as_deref() != Some(req_effort) {
                return None;
            }
        }
        match &self.evidence.effort_evidence {
            FieldEvidence::VerifiedMatch { value } => {
                if meta.observed_effort.as_deref() != Some(value.as_str()) {
                    return None;
                }
            }
            FieldEvidence::NotRequested {
                observed: Some(value),
            } => {
                if let Some(meta_eff) = meta.observed_effort.as_deref() {
                    if meta_eff != value.as_str() {
                        return None;
                    }
                }
            }
            _ => {}
        }

        Some(meta)
    }

    /// Returns the effective observed model from the NDJSON stream or, when the stream omitted
    /// model evidence on a clean owned OpenCode v2 run, from validated session metadata.
    pub fn effective_observed_model(&self) -> Option<&str> {
        let stream_model = match &self.evidence.model_evidence {
            FieldEvidence::VerifiedMatch { value } => Some(value.as_str()),
            FieldEvidence::Mismatch { observed, .. } => Some(observed.as_str()),
            FieldEvidence::NotRequested { observed } => observed.as_deref(),
            FieldEvidence::Missing { .. } => None,
        };
        stream_model.or_else(|| {
            self.validated_opencode_metadata()
                .map(|m| m.observed_model.as_str())
        })
    }

    /// Returns the effective observed effort/variant from the NDJSON stream or, when the stream
    /// omitted effort evidence on a clean owned OpenCode v2 run, from validated session metadata.
    pub fn effective_observed_effort(&self) -> Option<&str> {
        let stream_effort = match &self.evidence.effort_evidence {
            FieldEvidence::VerifiedMatch { value } => Some(value.as_str()),
            FieldEvidence::Mismatch { observed, .. } => Some(observed.as_str()),
            FieldEvidence::NotRequested { observed } => observed.as_deref(),
            FieldEvidence::Missing { .. } => None,
        };
        stream_effort.or_else(|| {
            self.validated_opencode_metadata()
                .and_then(|m| m.observed_effort.as_deref())
        })
    }

    /// Returns `true` when this report is for a [`WorkerProvider::CodexCli`] read-only advisory
    /// invocation whose `constructed_argv` matches the exact pinned launch configuration for
    /// `self.requested_model` (`--no-daemon exec --json --ephemeral --ignore-user-config --model`,
    /// `-c` overrides for `model_reasoning_effort`, `approval_policy`, `model_provider`,
    /// `default_permissions`, `permissions.threadlane-advisory`, `web_search`, and all disabled
    /// capability flags).
    ///
    /// Because Codex CLI `exec --json` does not emit runtime model or reasoning-effort telemetry
    /// in its JSONL event stream, this launch-pinned configuration check provides requested-only
    /// Returns `true` if `self` is a [`WorkerProvider::CodexCli`] run whose constructed launch
    /// argument vector matches the exact read-only advisory launch configuration pinned to
    /// `self.requested_model` (`--no-daemon exec --json --ephemeral --ignore-user-config --model`,
    /// `-c` overrides for `model_reasoning_effort`, `approval_policy="never"`,
    /// `model_provider="openai"`, `default_permissions="threadlane-advisory"`,
    /// `permissions.threadlane-advisory={...}`, `web_search="disabled"`, and all disabled feature
    /// flags).
    ///
    /// Note: `codex exec --json` does not emit runtime model or effort strings in its JSONL event
    /// stream. This helper exposes the verified launch-configuration pin separately from stream
    /// observations so callers can verify launch evidence for `CodexCli` without fabricating
    /// `effective_observed_model()` or `effective_observed_effort()` and without relaxing strict
    /// observed evidence rules for [`WorkerProvider::Antigravity`] or [`WorkerProvider::OpenCodeV2`].
    #[must_use]
    pub fn has_pinned_codex_read_only_launch_config(&self) -> bool {
        self.provider == WorkerProvider::CodexCli
            && self.requested_session_id.is_none()
            && self.allocated_session_id.is_none()
            && self.opencode_metadata.is_none()
            && self.opencode_metadata_error.is_none()
            && is_pinned_codex_read_only_argv(&self.constructed_argv, &self.requested_model)
    }

    /// Returns `true` if `self` is a [`WorkerProvider::CodexCli`] mutable task-worktree run whose
    /// constructed launch argument vector matches the exact `TaskFiles` launch configuration
    /// pinned to `self.requested_model` (`--no-daemon exec --json --ephemeral --ignore-user-config --model`,
    /// `-c` overrides for `model_reasoning_effort`, `approval_policy="never"`,
    /// `model_provider="openai"`, `default_permissions="threadlane-task-files"`, a validated
    /// `permissions.threadlane-task-files={...}` inline profile with finite write scopes and all
    /// protected deny rules, `web_search="disabled"`, and all disabled feature flags).
    ///
    /// Like [`Self::has_pinned_codex_read_only_launch_config`], this verifies the exact launch pin
    /// separately from runtime stream observations (`effective_observed_model()` and
    /// `effective_observed_effort()` remain `None` when absent from the JSONL stream).
    #[must_use]
    pub fn has_pinned_codex_task_files_launch_config(&self) -> bool {
        self.provider == WorkerProvider::CodexCli
            && self.requested_session_id.is_none()
            && self.allocated_session_id.is_none()
            && self.opencode_metadata.is_none()
            && self.opencode_metadata_error.is_none()
            && is_pinned_codex_task_files_argv(&self.constructed_argv, &self.requested_model)
    }

    /// Returns `true` if `self` has a verified [`WorkerProvider::CodexCli`] launch-configuration
    /// pin (either read-only advisory `threadlane-advisory` or mutable task-files
    /// `threadlane-task-files`).
    #[must_use]
    pub fn has_pinned_codex_launch_config(&self) -> bool {
        self.has_pinned_codex_read_only_launch_config()
            || self.has_pinned_codex_task_files_launch_config()
    }

    /// Returns the explicit launch-pin evidence label when `self` is a launch-pinned
    /// [`WorkerProvider::CodexCli`] run (`"codex_read_only_launch_pin"` or
    /// `"codex_task_files_launch_pin"`), or `None` otherwise.
    #[must_use]
    pub fn codex_launch_pin_evidence_mode(&self) -> Option<&'static str> {
        if self.has_pinned_codex_read_only_launch_config() {
            Some("codex_read_only_launch_pin")
        } else if self.has_pinned_codex_task_files_launch_config() {
            Some("codex_task_files_launch_pin")
        } else {
            None
        }
    }

    /// Returns `true` if model evidence is satisfied by the stream, by validated OpenCode v2
    /// session metadata, or (for [`WorkerProvider::CodexCli`] runs) by an exact pinned launch
    /// configuration (`threadlane-advisory` or `threadlane-task-files`) without any stream model
    /// conflict.
    pub fn has_satisfied_model_evidence(&self) -> bool {
        if matches!(self.evidence.model_evidence, FieldEvidence::Mismatch { .. })
            || matches!(
                self.status,
                WorkerRunStatus::ModelMismatch | WorkerRunStatus::MissingModelEvidence
            )
            || matches!(
                self.opencode_metadata_error.as_deref(),
                Some("model_mismatch" | "missing_model")
            )
        {
            return false;
        }
        if let Some(meta) = self.opencode_metadata.as_ref() {
            let model_matches_requested = meta.observed_model == self.requested_model.base_model
                || meta.observed_model == self.requested_model.cli_model_arg;
            let model_matches_stream = match &self.evidence.model_evidence {
                FieldEvidence::VerifiedMatch { value } => {
                    value == &meta.observed_model || value == &self.requested_model.base_model
                }
                _ => true,
            };
            if !model_matches_requested || !model_matches_stream {
                return false;
            }
        }
        if self.evidence.model_evidence.is_satisfied() {
            return true;
        }
        if self.provider == WorkerProvider::CodexCli {
            return self.has_pinned_codex_launch_config();
        }
        self.validated_opencode_metadata().is_some()
    }

    /// Returns `true` if effort/variant evidence is satisfied by the stream, by validated
    /// OpenCode v2 session metadata, or (for [`WorkerProvider::CodexCli`] runs) by an exact
    /// pinned launch configuration (`threadlane-advisory` or `threadlane-task-files`) without any
    /// stream effort conflict.
    pub fn has_satisfied_effort_evidence(&self) -> bool {
        if matches!(
            self.evidence.effort_evidence,
            FieldEvidence::Mismatch { .. }
        ) || matches!(
            self.status,
            WorkerRunStatus::EffortMismatch | WorkerRunStatus::MissingEffortEvidence
        ) || matches!(
            self.opencode_metadata_error.as_deref(),
            Some("effort_mismatch" | "missing_effort")
        ) {
            return false;
        }
        if let Some(meta) = self.opencode_metadata.as_ref() {
            if let Some(req_effort) = self.requested_model.effort.as_deref() {
                if meta.observed_effort.as_deref() != Some(req_effort) {
                    return false;
                }
            }
            match &self.evidence.effort_evidence {
                FieldEvidence::VerifiedMatch { value } => {
                    if meta.observed_effort.as_deref() != Some(value.as_str()) {
                        return false;
                    }
                }
                FieldEvidence::NotRequested {
                    observed: Some(value),
                } => {
                    if let Some(meta_eff) = meta.observed_effort.as_deref() {
                        if meta_eff != value.as_str() {
                            return false;
                        }
                    }
                }
                _ => {}
            }
        }
        if self.evidence.effort_evidence.is_satisfied() {
            return true;
        }
        if self.provider == WorkerProvider::CodexCli {
            return self.has_pinned_codex_launch_config();
        }
        self.validated_opencode_metadata().is_some()
    }

    /// Returns `true` if session identity evidence is satisfied by the stream (and matches any
    /// software-allocated session ID without metadata ownership conflict).
    pub fn has_satisfied_session_evidence(&self) -> bool {
        if self.status == WorkerRunStatus::SessionMismatch
            || self.opencode_metadata_error.as_deref() == Some("ownership_mismatch")
            || !self.evidence.session_evidence.is_satisfied()
        {
            return false;
        }
        if self.provider == WorkerProvider::CodexCli
            && (!self.evidence.saw_init_event
                || self.evidence.observed_session_id.is_none()
                || !self
                    .evidence
                    .codex_turn_lifecycle
                    .as_ref()
                    .is_some_and(|lc| lc.thread_started_count == 1))
        {
            return false;
        }
        if let Some(allocated_id) = self.allocated_session_id.as_deref() {
            if self.evidence.observed_session_id.as_deref() != Some(allocated_id) {
                return false;
            }
            if let Some(meta) = self.opencode_metadata.as_ref() {
                if meta.session_id != allocated_id {
                    return false;
                }
            }
        }
        true
    }

    /// Returns `true` if terminal completion/result evidence is satisfied by the stream or by
    /// validated OpenCode v2 session metadata.
    pub fn has_satisfied_result_evidence(&self) -> bool {
        if self.validated_opencode_metadata().is_some() {
            return true;
        }
        if self.provider == WorkerProvider::CodexCli
            && !self.evidence.has_valid_codex_turn_lifecycle()
        {
            return false;
        }
        self.evidence.saw_result_event
            && self.status != WorkerRunStatus::MissingResultEvidence
            && self.opencode_metadata_error.is_none()
    }

    /// Returns `true` if the worker completed a legacy stream-only run cleanly with verified
    /// model, session, and terminal result evidence, no OpenCode session allocation or metadata
    /// error, and only effort/variant evidence missing (`WorkerRunStatus::MissingEffortEvidence`).
    pub fn is_completed_pending_effort_evidence(&self) -> bool {
        self.status.is_completed_pending_effort_evidence()
            && self.allocated_session_id.is_none()
            && self.opencode_metadata.is_none()
            && self.opencode_metadata_error.is_none()
            && self.exit_code == Some(0)
            && self.signal.is_none()
            && !self.cleanup_incomplete
            && self.overflow.is_none()
            && self.evidence.blocked_events.is_empty()
            && self.evidence.provider_errors.is_empty()
            && self.evidence.malformed_lines_count == 0
            && self.evidence.model_evidence.is_satisfied()
            && self.evidence.session_evidence.is_satisfied()
            && self.evidence.saw_result_event
            && matches!(self.evidence.effort_evidence, FieldEvidence::Missing { .. })
    }

    /// Checks historical read-only Codex advisory completion for persisted artifact validation.
    ///
    /// This compatibility predicate requires the exact read-only launch pin and the absence of
    /// both current execution markers (`executed_executable` and `codex_turn_lifecycle`). It
    /// preserves clean process, model/effort, session and terminal-result checks without inventing
    /// lifecycle or observed telemetry. It never admits mutable `TaskFiles` launches. Callers
    /// must establish persisted artifact integrity separately and must not use this predicate to
    /// collect fresh execution evidence; current runs require
    /// [`Self::has_complete_natural_completion_evidence`].
    #[must_use]
    pub fn has_complete_legacy_codex_advisory_evidence(&self) -> bool {
        self.executed_executable.is_none()
            && self.evidence.codex_turn_lifecycle.is_none()
            && self.has_pinned_codex_read_only_launch_config()
            && extract_pinned_codex_read_only_output_last_message_path(
                &self.constructed_argv,
                &self.requested_model,
            )
            .is_none()
            && self.status == WorkerRunStatus::Succeeded
            && self.exit_code == Some(0)
            && self.signal.is_none()
            && !self.cleanup_incomplete
            && self.overflow.is_none()
            && self.evidence.blocked_events.is_empty()
            && self.evidence.provider_errors.is_empty()
            && self.evidence.malformed_lines_count == 0
            && !matches!(self.evidence.model_evidence, FieldEvidence::Mismatch { .. })
            && !matches!(
                self.evidence.effort_evidence,
                FieldEvidence::Mismatch { .. }
            )
            && self.evidence.saw_init_event
            && self
                .evidence
                .observed_session_id
                .as_deref()
                .is_some_and(|id| !id.trim().is_empty())
            && self.evidence.session_evidence.is_satisfied()
            && self.evidence.saw_result_event
    }

    /// Returns `true` if the worker exited 0 cleanly without timeout, overflow, cleanup issues,
    /// blocked events, provider errors, malformed lines, or metadata errors, and has complete
    /// model, effort, session, and completion evidence from the stream, from validated
    /// OpenCode v2 session metadata on an owned session run, or from a launch-pinned
    /// [`WorkerProvider::CodexCli`] scoped-task or read-only advisory run with verified thread and turn
    /// completion evidence.
    pub fn has_complete_natural_completion_evidence(&self) -> bool {
        if self.exit_code != Some(0)
            || self.signal.is_some()
            || self.status == WorkerRunStatus::TimedOut
            || self.cleanup_incomplete
            || self.overflow.is_some()
            || !self.evidence.blocked_events.is_empty()
            || !self.evidence.provider_errors.is_empty()
            || self.evidence.malformed_lines_count > 0
            || self.opencode_metadata_error.is_some()
        {
            return false;
        }

        if self.provider == WorkerProvider::CodexCli {
            return self.status == WorkerRunStatus::Succeeded
                && self.has_pinned_codex_launch_config()
                && !matches!(self.evidence.model_evidence, FieldEvidence::Mismatch { .. })
                && !matches!(
                    self.evidence.effort_evidence,
                    FieldEvidence::Mismatch { .. }
                )
                && self.evidence.saw_init_event
                && self.evidence.observed_session_id.is_some()
                && self.evidence.session_evidence.is_satisfied()
                && self.evidence.saw_result_event
                && self.evidence.has_valid_codex_turn_lifecycle();
        }

        let stream_complete = self.evidence.model_evidence.is_satisfied()
            && self.evidence.effort_evidence.is_satisfied()
            && self.evidence.session_evidence.is_satisfied()
            && self.evidence.saw_result_event
            && self.allocated_session_id.is_none()
            && self.opencode_metadata.is_none();

        stream_complete || self.validated_opencode_metadata().is_some()
    }

    /// Parses [`Self::opencode_metadata_error`] into a typed [`OpenCodeMetadataDiagnostic`] when
    /// it matches a known metadata validator error code or stage-qualified
    /// `invalid_metadata:<stage>` code.
    ///
    /// Legacy `"invalid_metadata"` values decode with `stage: None` (unknown/unavailable detail),
    /// never an invented stage.
    #[must_use]
    pub fn opencode_metadata_diagnostic(&self) -> Option<OpenCodeMetadataDiagnostic> {
        let raw = self.opencode_metadata_error.as_deref()?;
        OpenCodeMetadataDiagnostic::from_error_code(raw)
    }

    /// Returns the finite [`OpenCodeMetadataStage`] when [`Self::opencode_metadata_error`] encodes
    /// a stage-qualified `invalid_metadata:<stage>` diagnostic.
    ///
    /// Returns `None` for legacy undifferentiated `"invalid_metadata"` reports and for semantic
    /// error codes that already identify the failure cause without a stage suffix.
    #[must_use]
    pub fn opencode_metadata_stage(&self) -> Option<OpenCodeMetadataStage> {
        let raw = self.opencode_metadata_error.as_deref()?;
        OpenCodeCollectorDiagnostic::from_error_code(raw).and_then(|diag| diag.stage)
    }

    /// Returns the verified finite `'static` error literal for [`Self::opencode_metadata_error`].
    ///
    /// If a deserialized report contains an unrecognized string in `opencode_metadata_error`, this
    /// method falls back to `"invalid_metadata"` so untrusted or arbitrary strings never escape.
    #[must_use]
    pub fn sanitized_opencode_metadata_error(&self) -> Option<&'static str> {
        let raw = self.opencode_metadata_error.as_deref()?;
        Some(
            OpenCodeCollectorDiagnostic::from_error_code(raw)
                .map_or("invalid_metadata", |diag| diag.as_str()),
        )
    }

    /// Formats a privacy-safe failure reason from [`Self::sanitized_opencode_metadata_error`] when
    /// OpenCode v2 session allocation or metadata verification failed.
    #[must_use]
    pub fn opencode_metadata_failure_reason(&self) -> Option<String> {
        self.sanitized_opencode_metadata_error()
            .map(|code| format!("OpenCode session metadata verification failed ({code})"))
    }
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

/// Returns `true` if `pgid` is a valid isolated Unix process group distinct from the caller's.
pub(crate) fn is_isolated_process_group(pgid: i32) -> bool {
    #[cfg(unix)]
    {
        pgid > 1 && pgid != unix_pgroup::caller_pgid()
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

pub(crate) async fn wait_for_cancellation(cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        if *cancel_rx.borrow_and_update() {
            return;
        }
        if cancel_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Runs a pre-validated [`WorkerSpec`] inside `working_directory` with a no-op cancellation channel
/// and no live event observer (`progress_tx = None`).
pub async fn run_worker(
    spec: &WorkerSpec,
    working_directory: &Path,
) -> Result<WorkerExecutionReport, WorkerError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_worker_with_observer(spec, working_directory, &mut cancel_rx, None).await
}

/// Runs a [`WorkerSpec`] inside `working_directory` under Unix process-group supervision,
/// concurrent incremental stdout/stderr drainage, and timeout/cancel/overflow enforcement,
/// with no live event observer (`progress_tx = None`).
///
/// Never performs automatic replay if execution crashes or times out.
pub async fn run_worker_with_cancel(
    spec: &WorkerSpec,
    working_directory: &Path,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<WorkerExecutionReport, WorkerError> {
    run_worker_with_observer(spec, working_directory, cancel_rx, None).await
}

/// Runs a [`WorkerSpec`] inside `working_directory` under Unix process-group supervision,
/// concurrent incremental stdout/stderr drainage, timeout/cancel/overflow enforcement, and
/// optional nonblocking live event observation via `progress_tx`.
///
/// When `progress_tx` is `Some`, each sanitized, bounded [`NormalizedWorkerEvent`] parsed from
/// stdout is emitted incrementally before process exit using [`mpsc::Sender::try_send`]. A full
/// or closed observer channel never blocks pipe drainage, overflow enforcement, cancellation,
/// timeout handling, or final evidence verification; undelivered live events are counted in
/// [`WorkerExecutionReport::dropped_live_events`].
///
/// Never performs automatic replay if execution crashes or times out.
pub async fn run_worker_with_observer(
    spec: &WorkerSpec,
    working_directory: &Path,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
) -> Result<WorkerExecutionReport, WorkerError> {
    run_worker_with_health_observer(spec, working_directory, cancel_rx, progress_tx, None).await
}

/// Runs with an independent cumulative health watch observer; existing progress semantics are preserved.
/// Use one health channel per invocation. Watch data grants no persistence or callback authority.
pub async fn run_worker_with_health_observer(
    spec: &WorkerSpec,
    working_directory: &Path,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
) -> Result<WorkerExecutionReport, WorkerError> {
    ensure_platform_supported()?;
    let validation = spec.validate()?;
    let canonical_workdir =
        validate_existing_canonical_dir(working_directory, "working_directory")?;
    let initial_argv = spec.build_argv_for_workdir(&canonical_workdir)?;
    let child_env = spec.build_child_env_for_workdir(&canonical_workdir)?;

    let (report, _) = run_supervised_process_group(
        SupervisedLaunchInputs {
            provider: spec.provider,
            canonical_executable: validation.canonical_executable,
            canonical_workdir: canonical_workdir.clone(),
            resolved_model: validation.resolved_model,
            requested_session_id: spec.session_id.clone(),
            initial_argv,
            child_env,
            budgets: spec.budgets.clone(),
        },
        |allocated_session_id| {
            let mut effective_spec = spec.clone();
            effective_spec.session_id = Some(allocated_session_id.to_string());
            effective_spec.build_argv_for_workdir(&canonical_workdir)
        },
        None,
        cancel_rx,
        progress_tx,
        health_observer,
    )
    .await?;
    Ok(report)
}

/// Runs a read-only [`AdvisoryWorkerSpec`] inside `spec.working_directory` under Unix
/// process-group supervision, concurrent incremental stdout/stderr drainage, and
/// timeout/overflow enforcement.
///
/// Fails closed with [`WorkerError::UnsupportedCapability`] before spawning if
/// `spec.file_permissions` is not [`crate::permissions::WorkerFilePermissions::ReadOnly`].
/// Never performs automatic replay if execution crashes or times out.
pub async fn run_advisory_worker(
    spec: &AdvisoryWorkerSpec,
) -> Result<WorkerExecutionReport, WorkerError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_advisory_worker_with_observer(spec, &mut cancel_rx, None).await
}

/// Runs a read-only [`AdvisoryWorkerSpec`] inside `spec.working_directory` under Unix
/// process-group supervision, concurrent incremental stdout/stderr drainage, and
/// timeout/cancel/overflow enforcement, with no live event observer (`progress_tx = None`).
///
/// Never performs automatic replay if execution crashes or times out.
pub async fn run_advisory_worker_with_cancel(
    spec: &AdvisoryWorkerSpec,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<WorkerExecutionReport, WorkerError> {
    run_advisory_worker_with_observer(spec, cancel_rx, None).await
}

/// Runs a read-only [`AdvisoryWorkerSpec`] inside `spec.working_directory` under Unix
/// process-group supervision, concurrent incremental stdout/stderr drainage,
/// timeout/cancel/overflow enforcement, and optional nonblocking live event observation via
/// `progress_tx`.
///
/// Distinguishes intentional provider-private logging/state outside `working_directory` and
/// bounded stderr diagnostics from repository writes: stderr and owned OpenCode session
/// metadata are captured within configured budgets while pipe-holding descendants are reaped
/// within [`POST_EXIT_PIPE_DRAIN_TIMEOUT_MS`].
pub async fn run_advisory_worker_with_observer(
    spec: &AdvisoryWorkerSpec,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
) -> Result<WorkerExecutionReport, WorkerError> {
    run_advisory_worker_with_health_observer(spec, cancel_rx, progress_tx, None).await
}

/// Runs with an independent cumulative health watch observer; existing progress semantics are preserved.
/// Use one health channel per invocation. Watch data grants no persistence or callback authority.
pub async fn run_advisory_worker_with_health_observer(
    spec: &AdvisoryWorkerSpec,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
) -> Result<WorkerExecutionReport, WorkerError> {
    ensure_platform_supported()?;
    let validation = spec.validate()?;
    let canonical_workdir = validation.canonical_working_directory.clone();

    let (report, _) = run_supervised_process_group(
        SupervisedLaunchInputs {
            provider: spec.provider,
            canonical_executable: validation.canonical_executable,
            canonical_workdir: canonical_workdir.clone(),
            resolved_model: validation.resolved_model,
            requested_session_id: spec.session_id.clone(),
            initial_argv: validation.constructed_argv,
            child_env: validation.child_env,
            budgets: spec.budgets.clone(),
        },
        |allocated_session_id| {
            let mut effective_spec = spec.clone();
            effective_spec.session_id = Some(allocated_session_id.to_string());
            effective_spec.build_argv_for_workdir(&canonical_workdir)
        },
        None,
        cancel_rx,
        progress_tx,
        health_observer,
    )
    .await?;
    Ok(report)
}

/// Runs a read-only [`AdvisoryWorkerSpec`] with opt-in complete structured advisory response
/// capture under `response_budget`.
///
/// Pre-launch configuration errors (including invalid `response_budget` or non-`read_only` mode)
/// fail closed with [`WorkerError`] before spawning. Once spawned, the underlying
/// [`WorkerExecutionReport`] is always preserved in [`AdvisoryStructuredExecution::execution`]
/// even if the provider completed (`WorkerRunStatus::Succeeded`) with missing, oversized,
/// truncated, non-JSON, spoofed, or conflicting output.
pub async fn run_advisory_worker_with_structured_result(
    spec: &AdvisoryWorkerSpec,
    response_budget: AdvisoryResponseBudget,
) -> Result<AdvisoryStructuredExecution, WorkerError> {
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    run_advisory_worker_with_structured_result_and_observer(
        spec,
        response_budget,
        &mut cancel_rx,
        None,
    )
    .await
}

/// Runs a read-only [`AdvisoryWorkerSpec`] with opt-in complete structured advisory response
/// capture under `response_budget` and cancellation via `cancel_rx`, with no live event observer
/// (`progress_tx = None`).
pub async fn run_advisory_worker_with_structured_result_and_cancel(
    spec: &AdvisoryWorkerSpec,
    response_budget: AdvisoryResponseBudget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<AdvisoryStructuredExecution, WorkerError> {
    run_advisory_worker_with_structured_result_and_observer(spec, response_budget, cancel_rx, None)
        .await
}

/// Runs a read-only [`AdvisoryWorkerSpec`] with opt-in complete structured advisory response
/// capture under `response_budget`, cancellation via `cancel_rx`, and optional nonblocking live
/// event observation via `progress_tx`.
pub async fn run_advisory_worker_with_structured_result_and_observer(
    spec: &AdvisoryWorkerSpec,
    response_budget: AdvisoryResponseBudget,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
) -> Result<AdvisoryStructuredExecution, WorkerError> {
    run_advisory_worker_with_structured_result_and_health_observer(
        spec,
        response_budget,
        cancel_rx,
        progress_tx,
        None,
    )
    .await
}

/// Runs with an independent cumulative health watch observer; existing progress semantics are preserved.
/// Use one health channel per invocation. Watch data grants no persistence or callback authority.
pub async fn run_advisory_worker_with_structured_result_and_health_observer(
    spec: &AdvisoryWorkerSpec,
    response_budget: AdvisoryResponseBudget,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
) -> Result<AdvisoryStructuredExecution, WorkerError> {
    ensure_platform_supported()?;
    response_budget.validate()?;
    let validation = spec.validate()?;
    let canonical_workdir = validation.canonical_working_directory.clone();
    let resolved_model = validation.resolved_model.clone();

    let (execution, response_collector) = run_supervised_process_group(
        SupervisedLaunchInputs {
            provider: spec.provider,
            canonical_executable: validation.canonical_executable,
            canonical_workdir: canonical_workdir.clone(),
            resolved_model: validation.resolved_model,
            requested_session_id: spec.session_id.clone(),
            initial_argv: validation.constructed_argv,
            child_env: validation.child_env,
            budgets: spec.budgets.clone(),
        },
        |allocated_session_id| {
            let mut effective_spec = spec.clone();
            effective_spec.session_id = Some(allocated_session_id.to_string());
            effective_spec.build_argv_for_workdir(&canonical_workdir)
        },
        Some(response_budget),
        cancel_rx,
        progress_tx,
        health_observer,
    )
    .await?;

    let collector = response_collector.unwrap_or_else(|| {
        let effective_session_id = execution
            .allocated_session_id
            .clone()
            .or_else(|| spec.session_id.clone());
        AdvisoryResponseCollector::new(
            spec.provider,
            resolved_model,
            effective_session_id,
            response_budget,
        )
    });

    Ok(collector.finalize(execution))
}

struct SupervisedLaunchInputs {
    provider: WorkerProvider,
    canonical_executable: PathBuf,
    canonical_workdir: PathBuf,
    resolved_model: ResolvedModelTarget,
    requested_session_id: Option<String>,
    initial_argv: Vec<String>,
    child_env: Vec<(String, String)>,
    budgets: ExecutionBudgets,
}

/// Total Linux spawn attempts, including the first, for the exact transient exec refusal.
#[cfg(target_os = "linux")]
const ETXTBSY_MAX_SPAWN_ATTEMPTS: u32 = 3;

/// Short async backoffs between the bounded Linux `ETXTBSY` spawn attempts.
#[cfg(target_os = "linux")]
const ETXTBSY_RETRY_BACKOFF_MS: [u64; 2] = [10, 20];

enum LaunchAttemptOutcome {
    Spawned(Child),
    Aborted(WorkerRunStatus),
    Failed(WorkerError),
}

fn launch_abort_status(
    cancel_rx: &watch::Receiver<bool>,
    overall_deadline: tokio::time::Instant,
) -> Option<WorkerRunStatus> {
    if *cancel_rx.borrow() {
        Some(WorkerRunStatus::Cancelled)
    } else if tokio::time::Instant::now() >= overall_deadline {
        Some(WorkerRunStatus::TimedOut)
    } else {
        None
    }
}

async fn wait_for_launch_retry(
    cancel_rx: &mut watch::Receiver<bool>,
    overall_deadline: tokio::time::Instant,
    backoff: Duration,
) -> Option<WorkerRunStatus> {
    let retry_sleep = tokio::time::sleep(backoff);
    tokio::pin!(retry_sleep);
    let deadline_sleep = tokio::time::sleep_until(overall_deadline);
    tokio::pin!(deadline_sleep);
    tokio::select! {
        biased;
        () = wait_for_cancellation(cancel_rx) => Some(WorkerRunStatus::Cancelled),
        () = &mut deadline_sleep => Some(WorkerRunStatus::TimedOut),
        () = &mut retry_sleep => None,
    }
}

/// Returns a bounded retry delay only for Linux's exact numeric exec refusal.
///
/// Message text, broad `Io`, and `ErrorKind` alone are never sufficient. Every other error is
/// returned to the caller immediately, and no retry is attempted after a child is obtained.
#[cfg(target_os = "linux")]
fn etxtbsy_retry_delay(attempts: u32, err: &std::io::Error) -> Option<Duration> {
    if err.raw_os_error() != Some(libc::ETXTBSY) || attempts >= ETXTBSY_MAX_SPAWN_ATTEMPTS {
        return None;
    }
    let index = usize::try_from(attempts.saturating_sub(1)).ok()?;
    ETXTBSY_RETRY_BACKOFF_MS
        .get(index)
        .copied()
        .map(Duration::from_millis)
}

#[cfg(not(target_os = "linux"))]
fn etxtbsy_retry_delay(_attempts: u32, _err: &std::io::Error) -> Option<Duration> {
    None
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, PartialEq, Eq)]
struct ExecutableIdentity {
    dev: u64,
    ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    mtime: i64,
    mtime_nsec: i64,
    ctime: i64,
    ctime_nsec: i64,
}

#[cfg(target_os = "linux")]
fn executable_identity(path: &Path) -> Result<ExecutableIdentity, WorkerError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::symlink_metadata(path).map_err(WorkerError::Io)?;
    if metadata.file_type().is_symlink() {
        return Err(WorkerError::SecurityViolation(format!(
            "worker executable `{}` must not be a symlink",
            path.display()
        )));
    }
    if !metadata.file_type().is_file() {
        return Err(WorkerError::InvalidSpec(format!(
            "worker executable `{}` must be a regular file",
            path.display()
        )));
    }
    Ok(ExecutableIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        size: metadata.size(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        ctime: metadata.ctime(),
        ctime_nsec: metadata.ctime_nsec(),
    })
}

/// Conservative retry-time guard against executable replacement or permission drift.
///
/// This metadata comparison is a race check, not a descriptor-atomic exec identity proof.
#[cfg(target_os = "linux")]
fn revalidate_launch_executable(
    path: &Path,
    expected: ExecutableIdentity,
) -> Result<(), WorkerError> {
    let canonical = validate_executable_file(path)?;
    if canonical != path {
        return Err(WorkerError::SecurityViolation(format!(
            "worker executable `{}` changed canonical path before retry",
            path.display()
        )));
    }
    if executable_identity(path)? != expected {
        return Err(WorkerError::SecurityViolation(format!(
            "worker executable `{}` changed identity before retry",
            path.display()
        )));
    }
    Ok(())
}

async fn launch_with_bounded_etxtbsy_retry<S>(
    mut spawn: S,
    canonical_executable: &Path,
    cancel_rx: &mut watch::Receiver<bool>,
    overall_deadline: tokio::time::Instant,
    mut on_spawn_error: Option<&mut (dyn FnMut(u32, &std::io::Error) + Send)>,
) -> LaunchAttemptOutcome
where
    S: FnMut() -> std::io::Result<Child>,
{
    if let Some(status) = launch_abort_status(cancel_rx, overall_deadline) {
        return LaunchAttemptOutcome::Aborted(status);
    }
    #[cfg(target_os = "linux")]
    let expected_identity = match executable_identity(canonical_executable) {
        Ok(identity) => identity,
        Err(err) => return LaunchAttemptOutcome::Failed(err),
    };
    #[cfg(not(target_os = "linux"))]
    let _ = canonical_executable;

    let mut attempts = 0u32;
    loop {
        if let Some(status) = launch_abort_status(cancel_rx, overall_deadline) {
            return LaunchAttemptOutcome::Aborted(status);
        }
        attempts = attempts.saturating_add(1);
        #[cfg(target_os = "linux")]
        if attempts > 1 {
            if let Err(err) = revalidate_launch_executable(canonical_executable, expected_identity)
            {
                return LaunchAttemptOutcome::Failed(err);
            }
        }
        if let Some(status) = launch_abort_status(cancel_rx, overall_deadline) {
            return LaunchAttemptOutcome::Aborted(status);
        }
        match spawn() {
            Ok(child) => return LaunchAttemptOutcome::Spawned(child),
            Err(err) => {
                if let Some(observer) = on_spawn_error.as_deref_mut() {
                    observer(attempts, &err);
                }
                let Some(backoff) = etxtbsy_retry_delay(attempts, &err) else {
                    return LaunchAttemptOutcome::Failed(WorkerError::Io(err));
                };
                if let Some(status) =
                    wait_for_launch_retry(cancel_rx, overall_deadline, backoff).await
                {
                    return LaunchAttemptOutcome::Aborted(status);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn no_child_launch_report(
    provider: WorkerProvider,
    canonical_executable: &Path,
    canonical_workdir: &Path,
    resolved_model: &ResolvedModelTarget,
    requested_session_id: Option<String>,
    allocated_session_id: Option<String>,
    status: WorkerRunStatus,
    budgets: &ExecutionBudgets,
    watchdog: &Arc<Mutex<ProgressWatchdog>>,
    health: &HealthPublisher,
    health_requested: bool,
    start_instant: Instant,
) -> WorkerExecutionReport {
    if status == WorkerRunStatus::TimedOut {
        watchdog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .poll(budgets.timeout_ms);
    }
    let effective_session_id = allocated_session_id
        .clone()
        .or_else(|| requested_session_id.clone());
    let empty_parser = NdjsonStreamParser::new_for_provider(
        provider,
        resolved_model.clone(),
        effective_session_id,
        budgets.max_events,
    );
    let duration_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
    WorkerExecutionReport {
        provider,
        executed_executable: Some(canonical_executable.to_path_buf()),
        requested_model: resolved_model.clone(),
        requested_session_id,
        allocated_session_id,
        constructed_argv: Vec::new(),
        working_directory: canonical_workdir.to_path_buf(),
        status,
        exit_code: None,
        signal: None,
        process_group_id: None,
        duration_ms,
        stdout_bytes_captured: 0,
        stderr_bytes_captured: 0,
        overflow: None,
        descendants_terminated_after_exit: false,
        cleanup_incomplete: false,
        redacted_stderr_summary: None,
        dropped_live_events: 0,
        evidence: empty_parser.finalize(),
        opencode_metadata: None,
        opencode_metadata_error: (provider == WorkerProvider::OpenCodeV2)
            .then(|| status.as_str().to_string()),
        provider_health: health_requested.then(|| health.finish(duration_ms, false)),
        progress_watchdog: Some(
            watchdog
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .snapshot(duration_ms),
        ),
    }
}

async fn finish_no_child_launch_setup(
    diagnostic_task: &mut Option<crate::antigravity_diagnostics::DiagnosticTask>,
    codex_last_message_capture: &mut Option<
        codex_last_message_capture::OwnedCodexLastMessageCapture,
    >,
    start_instant: Instant,
) {
    if let Some(task) = diagnostic_task.as_mut() {
        let elapsed_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
        task.finish(elapsed_ms).await;
    }
    drop(codex_last_message_capture.take());
}

async fn run_supervised_process_group<F>(
    inputs: SupervisedLaunchInputs,
    rebuild_argv_with_session: F,
    response_budget: Option<AdvisoryResponseBudget>,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
) -> Result<(WorkerExecutionReport, Option<AdvisoryResponseCollector>), WorkerError>
where
    F: FnOnce(&str) -> Result<Vec<String>, WorkerError>,
{
    run_supervised_process_group_with_launch_observer(
        inputs,
        rebuild_argv_with_session,
        response_budget,
        cancel_rx,
        progress_tx,
        health_observer,
        None,
        None,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn run_supervised_process_group_for_test<F>(
    inputs: SupervisedLaunchInputs,
    rebuild_argv_with_session: F,
    response_budget: Option<AdvisoryResponseBudget>,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
    on_launch_setup: Option<&mut (dyn FnMut(&[String]) + Send)>,
    on_spawn_error: &mut (dyn FnMut(u32, &std::io::Error) + Send),
) -> Result<(WorkerExecutionReport, Option<AdvisoryResponseCollector>), WorkerError>
where
    F: FnOnce(&str) -> Result<Vec<String>, WorkerError>,
{
    run_supervised_process_group_with_launch_observer(
        inputs,
        rebuild_argv_with_session,
        response_budget,
        cancel_rx,
        progress_tx,
        health_observer,
        on_launch_setup,
        Some(on_spawn_error),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_supervised_process_group_with_launch_observer<F>(
    inputs: SupervisedLaunchInputs,
    rebuild_argv_with_session: F,
    response_budget: Option<AdvisoryResponseBudget>,
    cancel_rx: &mut watch::Receiver<bool>,
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    health_observer: Option<ProviderHealthObserver>,
    on_launch_setup: Option<&mut (dyn FnMut(&[String]) + Send)>,
    on_spawn_error: Option<&mut (dyn FnMut(u32, &std::io::Error) + Send)>,
) -> Result<(WorkerExecutionReport, Option<AdvisoryResponseCollector>), WorkerError>
where
    F: FnOnce(&str) -> Result<Vec<String>, WorkerError>,
{
    let SupervisedLaunchInputs {
        provider,
        canonical_executable,
        canonical_workdir,
        resolved_model,
        requested_session_id,
        initial_argv,
        child_env,
        budgets,
    } = inputs;

    let start_instant = Instant::now();
    // None is the explicit legacy opt-out. Freeze this invocation-level choice before
    // capture setup: failed/unavailable capture and closed observers must not expose stderr.
    let health_requested = health_observer.is_some();
    let health = HealthPublisher::new(provider == WorkerProvider::Antigravity, health_observer);
    let watchdog = Arc::new(Mutex::new(ProgressWatchdog::new(
        budgets.timeout_ms,
        budgets.progress_watchdog.as_ref(),
    )?));
    let overall_deadline =
        tokio::time::Instant::from_std(start_instant + budgets.timeout_duration());
    let started_at_ms = current_unix_epoch_ms()?;

    let is_new_opencode_run =
        provider == WorkerProvider::OpenCodeV2 && requested_session_id.is_none();

    let mut collector_context: Option<OpenCodeCollectorContext> = None;
    let mut owned_session: Option<OwnedOpenCodeSession> = None;
    let mut allocated_session_id: Option<String> = None;
    let mut effective_session_id = requested_session_id.clone();
    let mut constructed_argv = initial_argv;

    if is_new_opencode_run {
        let ctx = OpenCodeCollectorContext {
            executable: canonical_executable.clone(),
            canonical_workdir: canonical_workdir.clone(),
            child_env: child_env.clone(),
            overall_deadline,
            started_at_ms,
            max_stderr_bytes: budgets.max_stderr_bytes,
        };
        match opencode_collector::allocate_with_diagnostic(&ctx, &resolved_model, cancel_rx).await {
            Ok(owned) => {
                constructed_argv = rebuild_argv_with_session(&owned.session_id)?;
                effective_session_id = Some(owned.session_id.clone());
                allocated_session_id = Some(owned.session_id.clone());
                owned_session = Some(owned);
                collector_context = Some(ctx);

                if let Some(abort_status) = launch_abort_status(cancel_rx, overall_deadline) {
                    return Ok((
                        no_child_launch_report(
                            provider,
                            &canonical_executable,
                            &canonical_workdir,
                            &resolved_model,
                            requested_session_id.clone(),
                            allocated_session_id.clone(),
                            abort_status,
                            &budgets,
                            &watchdog,
                            &health,
                            health_requested,
                            start_instant,
                        ),
                        None,
                    ));
                }
            }
            Err(alloc_diag) => {
                let alloc_err = alloc_diag.error;
                let status = map_collector_error_to_run_status(alloc_err);
                if status == WorkerRunStatus::TimedOut {
                    watchdog
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .poll(budgets.timeout_ms);
                }
                let cleanup_incomplete =
                    matches!(alloc_err, OpenCodeCollectorError::CleanupIncomplete);
                let empty_parser = NdjsonStreamParser::new_for_provider(
                    provider,
                    resolved_model.clone(),
                    requested_session_id.clone(),
                    budgets.max_events,
                );
                let duration_ms =
                    u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
                return Ok((
                    WorkerExecutionReport {
                        provider,
                        executed_executable: Some(canonical_executable.clone()),
                        requested_model: resolved_model,
                        requested_session_id,
                        allocated_session_id: None,
                        constructed_argv: Vec::new(),
                        working_directory: canonical_workdir,
                        status,
                        exit_code: None,
                        signal: None,
                        process_group_id: None,
                        duration_ms,
                        stdout_bytes_captured: 0,
                        stderr_bytes_captured: 0,
                        overflow: None,
                        descendants_terminated_after_exit: false,
                        cleanup_incomplete,
                        redacted_stderr_summary: None,
                        dropped_live_events: 0,
                        evidence: empty_parser.finalize(),
                        opencode_metadata: None,
                        opencode_metadata_error: Some(alloc_diag.as_str().to_string()),
                        provider_health: health_requested
                            .then(|| health.finish(duration_ms, false)),
                        progress_watchdog: Some(
                            watchdog
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .snapshot(duration_ms),
                        ),
                    },
                    None,
                ));
            }
        }
    }

    let mut codex_last_message_capture = if provider == WorkerProvider::CodexCli
        && response_budget.is_some()
    {
        let capture =
            codex_last_message_capture::OwnedCodexLastMessageCapture::create(&canonical_workdir)?;
        splice_codex_output_last_message_arg(
            &mut constructed_argv,
            &resolved_model,
            capture.output_path(),
        )?;
        Some(capture)
    } else {
        None
    };

    let mut diagnostic_task = start_owned_capture(
        health_requested && provider == WorkerProvider::Antigravity,
        &mut constructed_argv,
        health.clone(),
        start_instant,
    );

    if let Some(abort_status) = launch_abort_status(cancel_rx, overall_deadline) {
        finish_no_child_launch_setup(
            &mut diagnostic_task,
            &mut codex_last_message_capture,
            start_instant,
        )
        .await;
        return Ok((
            no_child_launch_report(
                provider,
                &canonical_executable,
                &canonical_workdir,
                &resolved_model,
                requested_session_id,
                allocated_session_id,
                abort_status,
                &budgets,
                &watchdog,
                &health,
                health_requested,
                start_instant,
            ),
            None,
        ));
    }

    // Test-only seam: expose the exact capture-bearing argv once, before any spawn attempt.
    // Production always passes `None`, so the runtime launch policy is unchanged.
    if let Some(observer) = on_launch_setup {
        observer(&constructed_argv);
    }

    let mut cmd = Command::new(&canonical_executable);
    cmd.args(&constructed_argv)
        .envs(child_env)
        .current_dir(&canonical_workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    // Only the immediate Linux exec refusal is retried here. Once a Child exists, every failure
    // remains terminal; no provider, session, model, argv, or capture setup is recreated.
    let launch_outcome = launch_with_bounded_etxtbsy_retry(
        || cmd.spawn(),
        &canonical_executable,
        cancel_rx,
        overall_deadline,
        on_spawn_error,
    )
    .await;

    let mut child = match launch_outcome {
        LaunchAttemptOutcome::Spawned(child) => child,
        LaunchAttemptOutcome::Aborted(abort_status) => {
            finish_no_child_launch_setup(
                &mut diagnostic_task,
                &mut codex_last_message_capture,
                start_instant,
            )
            .await;
            return Ok((
                no_child_launch_report(
                    provider,
                    &canonical_executable,
                    &canonical_workdir,
                    &resolved_model,
                    requested_session_id,
                    allocated_session_id,
                    abort_status,
                    &budgets,
                    &watchdog,
                    &health,
                    health_requested,
                    start_instant,
                ),
                None,
            ));
        }
        LaunchAttemptOutcome::Failed(err) => {
            finish_no_child_launch_setup(
                &mut diagnostic_task,
                &mut codex_last_message_capture,
                start_instant,
            )
            .await;
            return Err(err);
        }
    };
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

    let response_collector = response_budget.map(|budget| {
        AdvisoryResponseCollector::new(
            provider,
            resolved_model.clone(),
            effective_session_id.clone(),
            budget,
        )
    });

    let mut stdout_handle = tokio::spawn(drain_stdout_incremental(
        stdout_pipe,
        NdjsonStreamParser::new_for_provider(
            provider,
            resolved_model.clone(),
            effective_session_id.clone(),
            budgets.max_events,
        ),
        response_collector,
        budgets.clone(),
        overflow_tx.clone(),
        drain_stop_rx.clone(),
        LiveProgressObserver {
            progress_tx: progress_tx.clone(),
            watchdog: Arc::clone(&watchdog),
            start_instant,
            dropped_live_events: 0,
        },
    ));

    let mut stderr_handle = tokio::spawn(drain_stderr_incremental(
        stderr_pipe,
        budgets.max_stderr_bytes,
        overflow_tx,
        drain_stop_rx,
        health_requested && provider == WorkerProvider::Antigravity,
    ));

    let mut timed_out = false;
    let mut cancelled = false;
    let mut recorded_overflow: Option<OutputOverflowKind> = None;
    let mut descendants_terminated_after_exit = false;
    let mut dropped_supervisor_events = 0usize;
    let exit_status: Option<ExitStatus>;

    if *cancel_rx.borrow_and_update() {
        cancelled = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else if tokio::time::Instant::now() >= overall_deadline {
        timed_out = true;
        watchdog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .poll(budgets.timeout_ms);
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else {
        let timeout_sleep = tokio::time::sleep_until(overall_deadline);
        tokio::pin!(timeout_sleep);

        loop {
            let next_soft_ms = watchdog
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .next_deadline_ms();
            let soft_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(
                start_instant + Duration::from_millis(next_soft_ms),
            ));
            tokio::pin!(soft_sleep);
            tokio::select! {
            biased;
            () = &mut timeout_sleep => {
                timed_out = true;
                watchdog.lock().unwrap_or_else(|p| p.into_inner()).poll(budgets.timeout_ms);
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
                break;
            }
            () = wait_for_cancellation(cancel_rx) => {
                cancelled = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
                break;
            }
            wait_res = child.wait() => {
                exit_status = wait_res.ok();
                descendants_terminated_after_exit =
                    finish_or_terminate_surviving_descendants(
                        pgid,
                        &stdout_handle,
                        &stderr_handle,
                    ).await;
                break;
            }
            Some(overflow_kind) = overflow_rx.recv() => {
                recorded_overflow = Some(overflow_kind);
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
                break;
            }
            () = &mut soft_sleep => {
                let elapsed_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
                let attention = watchdog.lock().unwrap_or_else(|p| p.into_inner()).poll(elapsed_ms);
                if let (Some(snapshot), Some(tx)) = (attention, progress_tx.as_ref()) {
                    let event = NormalizedWorkerEvent {
                        line_number: 0,
                        kind: NormalizedEventKind::SupervisorAttention,
                        raw_type: "threadlane.progress_watchdog".to_string(),
                        session_id: None,
                        model: None,
                        effort: None,
                        tool_name: None,
                        summary: serde_json::to_string(&snapshot).ok(),
                    };
                    if tx.try_send(event).is_err() {
                        dropped_supervisor_events = dropped_supervisor_events.saturating_add(1);
                    }
                }
            }
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

    let StdoutDrainOutput {
        bytes_captured: stdout_bytes_captured,
        evidence,
        mut response_collector,
        overflow: stdout_overflow,
        dropped_live_events,
    } = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => {
            return Err(WorkerError::Io(std::io::Error::other(format!(
                "stdout task join error: {err}"
            ))));
        }
        Err(_) => {
            stdout_handle.abort();
            cleanup_incomplete = true;
            let empty_parser = NdjsonStreamParser::new_for_provider(
                provider,
                resolved_model.clone(),
                effective_session_id,
                budgets.max_events,
            );
            StdoutDrainOutput {
                bytes_captured: 0,
                evidence: empty_parser.finalize(),
                response_collector: None,
                overflow: None,
                dropped_live_events: 0,
            }
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

    if let Some(task) = diagnostic_task.as_mut() {
        let elapsed_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
        task.finish(elapsed_ms).await;
    }

    let final_overflow = recorded_overflow.or(stdout_overflow).or(stderr_overflow);
    let (exit_code, signal) = extract_exit_code_and_signal(exit_status);

    if let (Some(capture), Some(budget), Some(collector)) = (
        codex_last_message_capture.as_ref(),
        response_budget,
        response_collector.as_mut(),
    ) {
        if !cancelled && !timed_out && final_overflow.is_none() && !cleanup_incomplete {
            let outcome = capture.read_outcome(budget.max_response_bytes);
            collector.set_codex_last_message_capture(outcome);
        }
    }

    let mut status = classify_run_status(
        provider,
        &resolved_model,
        &constructed_argv,
        cancelled,
        timed_out,
        final_overflow,
        cleanup_incomplete,
        exit_code,
        signal,
        &evidence,
    );

    let mut opencode_metadata: Option<OpenCodeMetadataEvidence> = None;
    let mut opencode_metadata_error: Option<String> = None;

    if let (Some(ctx), Some(owned)) = (collector_context.as_ref(), owned_session.as_ref()) {
        if can_collect_opencode_metadata(
            cancelled,
            timed_out,
            final_overflow,
            cleanup_incomplete,
            exit_code,
            signal,
            &evidence,
        ) {
            match opencode_collector::collect_with_diagnostic(
                ctx,
                owned,
                &resolved_model,
                cancel_rx,
            )
            .await
            {
                Ok(meta) => {
                    opencode_metadata = Some(meta);
                    status = WorkerRunStatus::Succeeded;
                }
                Err(collect_diag) => {
                    let collect_err = collect_diag.error;
                    if matches!(collect_err, OpenCodeCollectorError::CleanupIncomplete) {
                        cleanup_incomplete = true;
                    }
                    status = map_collector_error_to_run_status(collect_err);
                    opencode_metadata_error = Some(collect_diag.as_str().to_string());
                }
            }
        }
    }

    let duration_ms = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
    if status == WorkerRunStatus::TimedOut {
        watchdog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .poll(budgets.timeout_ms);
    }
    let final_watchdog = watchdog
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .snapshot(duration_ms);

    Ok((
        WorkerExecutionReport {
            provider,
            executed_executable: Some(canonical_executable.clone()),
            requested_model: resolved_model,
            requested_session_id,
            allocated_session_id,
            constructed_argv,
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
            dropped_live_events: dropped_live_events.saturating_add(dropped_supervisor_events),
            evidence,
            opencode_metadata,
            opencode_metadata_error,
            progress_watchdog: Some(final_watchdog),
            provider_health: health_requested
                .then(|| health.finish(duration_ms, status == WorkerRunStatus::Succeeded)),
        },
        response_collector,
    ))
}

pub(crate) async fn wait_for_readers_or_signal_stop<T1, T2>(
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

struct StdoutDrainOutput {
    bytes_captured: usize,
    evidence: StreamEvidenceSummary,
    response_collector: Option<AdvisoryResponseCollector>,
    overflow: Option<OutputOverflowKind>,
    dropped_live_events: usize,
}

struct LiveProgressObserver {
    progress_tx: Option<mpsc::Sender<NormalizedWorkerEvent>>,
    watchdog: Arc<Mutex<ProgressWatchdog>>,
    start_instant: Instant,
    dropped_live_events: usize,
}

fn ingest_and_observe_line(
    parser: &mut NdjsonStreamParser,
    response_collector: Option<&mut AdvisoryResponseCollector>,
    raw_line: &[u8],
    observer: &mut LiveProgressObserver,
) {
    if let Some(collector) = response_collector {
        collector.ingest_line(raw_line);
    }
    if let Some(event) = parser.ingest_line_with_event(raw_line) {
        let elapsed_ms =
            u64::try_from(observer.start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
        observer
            .watchdog
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .observe(&event, elapsed_ms);
        if let Some(tx) = &observer.progress_tx {
            if tx.try_send(event).is_err() {
                observer.dropped_live_events = observer.dropped_live_events.saturating_add(1);
            }
        }
    }
}

async fn drain_stdout_incremental(
    mut reader: tokio::process::ChildStdout,
    mut parser: NdjsonStreamParser,
    mut response_collector: Option<AdvisoryResponseCollector>,
    budgets: ExecutionBudgets,
    overflow_tx: mpsc::UnboundedSender<OutputOverflowKind>,
    mut drain_stop_rx: watch::Receiver<bool>,
    mut observer: LiveProgressObserver,
) -> StdoutDrainOutput {
    let max_line_bytes = budgets.max_line_bytes;
    let max_stdout_bytes = budgets.max_stdout_bytes;
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
                ingest_and_observe_line(
                    &mut parser,
                    response_collector.as_mut(),
                    &line_buf,
                    &mut observer,
                );
                line_buf.clear();
            }
            break;
        };
        match read_res {
            Ok(0) => {
                if overflow.is_none() && !line_buf.is_empty() {
                    ingest_and_observe_line(
                        &mut parser,
                        response_collector.as_mut(),
                        &line_buf,
                        &mut observer,
                    );
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
                        ingest_and_observe_line(
                            &mut parser,
                            response_collector.as_mut(),
                            &line_buf,
                            &mut observer,
                        );
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

    StdoutDrainOutput {
        bytes_captured: total_bytes,
        evidence: parser.finalize(),
        response_collector,
        overflow,
        dropped_live_events: observer.dropped_live_events,
    }
}

async fn drain_stderr_incremental(
    mut reader: tokio::process::ChildStderr,
    max_stderr_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<OutputOverflowKind>,
    mut drain_stop_rx: watch::Receiver<bool>,
    suppress_summary: bool,
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
                    if !suppress_summary && remaining > 0 {
                        buf.extend_from_slice(&chunk[..remaining.min(n)]);
                    }
                    total_bytes = total_bytes.saturating_add(n);
                    let kind = OutputOverflowKind::StderrTotalExceeded { max_stderr_bytes };
                    overflow = Some(kind);
                    let _ = overflow_tx.send(kind);
                    break;
                }
                total_bytes += n;
                if !suppress_summary {
                    buf.extend_from_slice(&chunk[..n]);
                }
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

pub(crate) async fn finish_or_terminate_surviving_descendants<T1, T2>(
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

pub(crate) async fn terminate_and_reap_group(pgid: i32, child: &mut Child) -> Option<ExitStatus> {
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

pub(crate) async fn terminate_process_group_members(pgid: i32) {
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

pub(crate) fn extract_exit_code_and_signal(
    status: Option<ExitStatus>,
) -> (Option<i32>, Option<i32>) {
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

#[allow(clippy::too_many_arguments)]
fn classify_run_status(
    provider: WorkerProvider,
    resolved_model: &ResolvedModelTarget,
    constructed_argv: &[String],
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
    if matches!(evidence.effort_evidence, FieldEvidence::Mismatch { .. }) {
        return WorkerRunStatus::EffortMismatch;
    }
    if matches!(evidence.session_evidence, FieldEvidence::Mismatch { .. }) {
        return WorkerRunStatus::SessionMismatch;
    }
    if provider == WorkerProvider::CodexCli {
        if !(is_pinned_codex_read_only_argv(constructed_argv, resolved_model)
            || is_pinned_codex_task_files_argv(constructed_argv, resolved_model))
        {
            return WorkerRunStatus::MissingModelEvidence;
        }
        if !evidence.saw_result_event || !evidence.has_valid_codex_turn_lifecycle() {
            if !evidence.saw_init_event
                || evidence.observed_session_id.is_none()
                || !evidence.session_evidence.is_satisfied()
            {
                return WorkerRunStatus::SessionMismatch;
            }
            return WorkerRunStatus::MissingResultEvidence;
        }
        if !evidence.saw_init_event
            || evidence.observed_session_id.is_none()
            || !evidence.session_evidence.is_satisfied()
        {
            return WorkerRunStatus::SessionMismatch;
        }
        return WorkerRunStatus::Succeeded;
    }
    if matches!(evidence.model_evidence, FieldEvidence::Missing { .. }) {
        return WorkerRunStatus::MissingModelEvidence;
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

fn current_unix_epoch_ms() -> Result<u64, WorkerError> {
    let duration =
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WorkerError::ExecutionFailed {
                status: "invalid_system_clock".to_string(),
                detail: "system clock predates Unix epoch".to_string(),
            })?;
    u64::try_from(duration.as_millis()).map_err(|_| WorkerError::ExecutionFailed {
        status: "invalid_system_clock".to_string(),
        detail: "system clock exceeds u64 millisecond range".to_string(),
    })
}

fn can_collect_opencode_metadata(
    cancelled: bool,
    timed_out: bool,
    overflow: Option<OutputOverflowKind>,
    cleanup_incomplete: bool,
    exit_code: Option<i32>,
    signal: Option<i32>,
    evidence: &StreamEvidenceSummary,
) -> bool {
    !cancelled
        && !timed_out
        && overflow.is_none()
        && !cleanup_incomplete
        && exit_code == Some(0)
        && signal.is_none()
        && evidence.blocked_events.is_empty()
        && evidence.provider_errors.is_empty()
        && evidence.malformed_lines_count == 0
        && !matches!(evidence.model_evidence, FieldEvidence::Mismatch { .. })
        && !matches!(evidence.effort_evidence, FieldEvidence::Mismatch { .. })
        && evidence.session_evidence.is_satisfied()
}

fn map_collector_error_to_run_status(err: OpenCodeCollectorError) -> WorkerRunStatus {
    match err {
        OpenCodeCollectorError::Cancelled => WorkerRunStatus::Cancelled,
        OpenCodeCollectorError::TimedOut => WorkerRunStatus::TimedOut,
        OpenCodeCollectorError::OutputOverflow => WorkerRunStatus::OutputOverflow,
        OpenCodeCollectorError::CleanupIncomplete => WorkerRunStatus::IncompleteCleanup,
        OpenCodeCollectorError::ProviderError => WorkerRunStatus::ProviderError,
        OpenCodeCollectorError::ModelMismatch => WorkerRunStatus::ModelMismatch,
        OpenCodeCollectorError::MissingModel => WorkerRunStatus::MissingModelEvidence,
        OpenCodeCollectorError::EffortMismatch => WorkerRunStatus::EffortMismatch,
        OpenCodeCollectorError::MissingEffort => WorkerRunStatus::MissingEffortEvidence,
        OpenCodeCollectorError::OwnershipMismatch => WorkerRunStatus::SessionMismatch,
        OpenCodeCollectorError::MissingCompletion => WorkerRunStatus::MissingResultEvidence,
        OpenCodeCollectorError::UnsupportedPlatform
        | OpenCodeCollectorError::InvalidContext
        | OpenCodeCollectorError::SpawnFailed
        | OpenCodeCollectorError::NonZeroExit
        | OpenCodeCollectorError::InvalidMetadata
        | OpenCodeCollectorError::StaleSession => WorkerRunStatus::NonZeroExit,
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
mod codex_last_message_capture {
    use std::ffi::CString;
    use std::fs::{self, File};
    use std::io::{self, Read};
    use std::mem::MaybeUninit;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    use crate::advisory_response::CodexLastMessageCaptureOutcome;
    use crate::error::WorkerError;
    use crate::fs_safety::validate_existing_canonical_dir;
    use crate::spec::CODEX_ADVISORY_LAST_MESSAGE_FILENAME;

    pub(super) struct OwnedCodexLastMessageCapture {
        directory: PathBuf,
        directory_name_cstr: CString,
        output_path: PathBuf,
        parent_file: File,
        directory_file: File,
        dir_dev: libc::dev_t,
        dir_ino: libc::ino_t,
        expected_uid: libc::uid_t,
    }

    fn fstat_fd(fd: libc::c_int) -> io::Result<libc::stat> {
        let mut st = MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: `fd` is a valid open file descriptor and `st` points to writable `libc::stat` storage.
        let rc = unsafe { libc::fstat(fd, st.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fstat` succeeded and initialized `st`.
        Ok(unsafe { st.assume_init() })
    }

    fn fstatat_nofollow(dir_fd: libc::c_int, name: &CString) -> io::Result<libc::stat> {
        let mut st = MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: `dir_fd` is a valid open directory fd, `name` is a NUL-terminated relative entry name,
        // and `st` points to writable `libc::stat` storage. `AT_SYMLINK_NOFOLLOW` never follows symlinks.
        let rc = unsafe {
            libc::fstatat(
                dir_fd,
                name.as_ptr(),
                st.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fstatat` succeeded and initialized `st`.
        Ok(unsafe { st.assume_init() })
    }

    fn lstat_path(path: &Path) -> io::Result<libc::stat> {
        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "path contains interior NUL")
        })?;
        let mut st = MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: `c_path` is a valid NUL-terminated string and `st` points to writable `libc::stat` storage.
        let rc = unsafe { libc::lstat(c_path.as_ptr(), st.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `lstat` succeeded and initialized `st`.
        Ok(unsafe { st.assume_init() })
    }

    fn open_dir_nofollow(path: &Path) -> io::Result<File> {
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }

    fn entry_cstr() -> CString {
        CString::new(CODEX_ADVISORY_LAST_MESSAGE_FILENAME)
            .expect("CODEX_ADVISORY_LAST_MESSAGE_FILENAME is static NUL-free ASCII")
    }

    impl OwnedCodexLastMessageCapture {
        pub(super) fn create(canonical_workdir: &Path) -> Result<Self, WorkerError> {
            let tempdir = tempfile::Builder::new()
                .prefix("threadlane-codex-last-msg-")
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()
                .map_err(WorkerError::Io)?;
            let leaf_stat = lstat_path(tempdir.path()).map_err(WorkerError::Io)?;
            if (leaf_stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
                return Err(WorkerError::SecurityViolation(format!(
                    "codex output-last-message capture tempdir leaf is not a directory: {}",
                    tempdir.path().display()
                )));
            }
            let canonical_candidate = fs::canonicalize(tempdir.path()).map_err(WorkerError::Io)?;
            let canonical_dir = validate_existing_canonical_dir(
                &canonical_candidate,
                "codex output-last-message capture directory",
            )?;
            if canonical_dir.starts_with(canonical_workdir)
                || canonical_workdir.starts_with(&canonical_dir)
            {
                return Err(WorkerError::SecurityViolation(format!(
                    "codex output-last-message capture directory must not overlap working directory: {}",
                    canonical_dir.display()
                )));
            }
            let Some(parent_dir) = canonical_dir.parent() else {
                return Err(WorkerError::SecurityViolation(format!(
                    "codex output-last-message capture directory has no parent: {}",
                    canonical_dir.display()
                )));
            };
            let Some(dir_name) = canonical_dir.file_name() else {
                return Err(WorkerError::SecurityViolation(format!(
                    "codex output-last-message capture directory has no leaf name: {}",
                    canonical_dir.display()
                )));
            };
            let directory_name_cstr = CString::new(dir_name.as_bytes()).map_err(|_| {
                WorkerError::InvalidSpec(
                    "codex output-last-message directory name contains interior NUL".to_string(),
                )
            })?;

            let parent_file = open_dir_nofollow(parent_dir).map_err(WorkerError::Io)?;
            let directory_file = open_dir_nofollow(&canonical_dir).map_err(WorkerError::Io)?;

            // SAFETY: `geteuid` has no preconditions and cannot fail.
            let expected_uid = unsafe { libc::geteuid() };
            let dir_stat = fstat_fd(directory_file.as_raw_fd()).map_err(WorkerError::Io)?;
            let path_stat = lstat_path(&canonical_dir).map_err(WorkerError::Io)?;
            if (path_stat.st_mode & libc::S_IFMT) != libc::S_IFDIR
                || path_stat.st_dev != dir_stat.st_dev
                || path_stat.st_ino != dir_stat.st_ino
                || leaf_stat.st_dev != dir_stat.st_dev
                || leaf_stat.st_ino != dir_stat.st_ino
                || dir_stat.st_uid != expected_uid
                || (dir_stat.st_mode & 0o7777) != 0o700
            {
                return Err(WorkerError::SecurityViolation(format!(
                    "codex output-last-message capture directory failed ownership or 0700 mode check: {}",
                    canonical_dir.display()
                )));
            }

            if fs::read_dir(&canonical_dir)
                .map_err(WorkerError::Io)?
                .next()
                .is_some()
            {
                return Err(WorkerError::SecurityViolation(format!(
                    "fresh codex output-last-message capture directory was not empty: {}",
                    canonical_dir.display()
                )));
            }

            let output_path = canonical_dir.join(CODEX_ADVISORY_LAST_MESSAGE_FILENAME);
            match fs::symlink_metadata(&output_path) {
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Ok(_) => {
                    return Err(WorkerError::SecurityViolation(format!(
                        "fresh codex output-last-message capture file unexpectedly existed before launch: {}",
                        output_path.display()
                    )));
                }
                Err(err) => return Err(WorkerError::Io(err)),
            }

            // Disarm `TempDir` recursive drop now that our descriptor-pinned `Drop` owns cleanup.
            let _ = tempdir.keep();

            Ok(Self {
                directory: canonical_dir,
                directory_name_cstr,
                output_path,
                parent_file,
                directory_file,
                dir_dev: dir_stat.st_dev,
                dir_ino: dir_stat.st_ino,
                expected_uid,
            })
        }

        pub(super) fn output_path(&self) -> &Path {
            &self.output_path
        }

        pub(super) fn read_outcome(
            &self,
            max_response_bytes: usize,
        ) -> CodexLastMessageCaptureOutcome {
            let Ok(path_stat) = lstat_path(&self.directory) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture directory disappeared or failed lstat".to_string(),
                );
            };
            let Ok(fd_stat) = fstat_fd(self.directory_file.as_raw_fd()) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture directory descriptor failed fstat".to_string(),
                );
            };
            if (path_stat.st_mode & libc::S_IFMT) != libc::S_IFDIR
                || path_stat.st_dev != self.dir_dev
                || path_stat.st_ino != self.dir_ino
                || fd_stat.st_dev != self.dir_dev
                || fd_stat.st_ino != self.dir_ino
                || fd_stat.st_uid != self.expected_uid
                || (fd_stat.st_mode & 0o7777) != 0o700
            {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture directory identity, ownership, or 0700 permissions changed"
                        .to_string(),
                );
            }

            let Ok(entries) = fs::read_dir(&self.directory) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "failed to enumerate capture directory".to_string(),
                );
            };
            for entry_res in entries {
                let Ok(entry) = entry_res else {
                    return CodexLastMessageCaptureOutcome::Unsafe(
                        "failed to read capture directory entry".to_string(),
                    );
                };
                if entry.file_name().to_str() != Some(CODEX_ADVISORY_LAST_MESSAGE_FILENAME) {
                    return CodexLastMessageCaptureOutcome::AmbiguousExtraEntries;
                }
            }

            let file_cstr = entry_cstr();
            let entry_stat = match fstatat_nofollow(self.directory_file.as_raw_fd(), &file_cstr) {
                Ok(st) => st,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return CodexLastMessageCaptureOutcome::Missing;
                }
                Err(err) => {
                    return CodexLastMessageCaptureOutcome::Unsafe(format!(
                        "failed to stat capture file: {err}"
                    ));
                }
            };

            let file_type = entry_stat.st_mode & libc::S_IFMT;
            if file_type == libc::S_IFLNK {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture path is a symbolic link".to_string(),
                );
            }
            if file_type != libc::S_IFREG {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture path is not a regular file".to_string(),
                );
            }
            if entry_stat.st_uid != self.expected_uid {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture file UID does not match supervisor EUID".to_string(),
                );
            }
            if entry_stat.st_nlink != 1 {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture file has unexpected hard-link count".to_string(),
                );
            }
            if (entry_stat.st_mode & 0o7113) != 0 || (entry_stat.st_mode & 0o0400) == 0 {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture file has unsafe permissions".to_string(),
                );
            }

            // SAFETY: `self.directory_file` is a valid open directory fd and `file_cstr` is a
            // NUL-terminated relative filename. `O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC` prevents
            // symlink traversal, blocking on FIFOs if raced, and descriptor leakage.
            let raw_fd = unsafe {
                libc::openat(
                    self.directory_file.as_raw_fd(),
                    file_cstr.as_ptr(),
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
                )
            };
            if raw_fd < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::NotFound {
                    return CodexLastMessageCaptureOutcome::Missing;
                }
                return CodexLastMessageCaptureOutcome::Unsafe(format!(
                    "openat failed on capture file: {err}"
                ));
            }
            // SAFETY: `raw_fd` is a freshly opened valid file descriptor owned exclusively here.
            let file = unsafe { File::from_raw_fd(raw_fd) };

            let Ok(opened_stat) = fstat_fd(file.as_raw_fd()) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "fstat failed on opened capture file".to_string(),
                );
            };
            if (opened_stat.st_mode & libc::S_IFMT) != libc::S_IFREG
                || opened_stat.st_dev != entry_stat.st_dev
                || opened_stat.st_ino != entry_stat.st_ino
                || opened_stat.st_uid != self.expected_uid
                || opened_stat.st_nlink != 1
                || (opened_stat.st_mode & 0o7113) != 0
                || (opened_stat.st_mode & 0o0400) == 0
            {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "opened capture file failed post-open identity, ownership, or mode check"
                        .to_string(),
                );
            }

            if opened_stat.st_size < 0 {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "opened capture file reported negative st_size".to_string(),
                );
            }
            let initial_size = usize::try_from(opened_stat.st_size).unwrap_or(usize::MAX);
            if initial_size > max_response_bytes {
                return CodexLastMessageCaptureOutcome::TooLarge {
                    observed_bytes: initial_size,
                };
            }

            let read_cap = max_response_bytes.saturating_add(1);
            let mut bytes = Vec::with_capacity(initial_size.min(read_cap));
            let Ok(limit_u64) = u64::try_from(read_cap) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "response byte cap overflowed u64".to_string(),
                );
            };
            if let Err(err) = (&file).take(limit_u64).read_to_end(&mut bytes) {
                return CodexLastMessageCaptureOutcome::Unsafe(format!(
                    "failed reading capture file: {err}"
                ));
            }
            if bytes.len() > max_response_bytes {
                return CodexLastMessageCaptureOutcome::TooLarge {
                    observed_bytes: bytes.len(),
                };
            }

            let Ok(post_stat) = fstat_fd(file.as_raw_fd()) else {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "post-read fstat failed on capture file".to_string(),
                );
            };
            let post_size = usize::try_from(post_stat.st_size).unwrap_or(usize::MAX);
            if post_stat.st_dev != opened_stat.st_dev
                || post_stat.st_ino != opened_stat.st_ino
                || post_size != bytes.len()
            {
                return CodexLastMessageCaptureOutcome::Unsafe(
                    "capture file mutated concurrently during read".to_string(),
                );
            }

            if bytes.is_empty() {
                return CodexLastMessageCaptureOutcome::Empty;
            }
            let Ok(text) = String::from_utf8(bytes) else {
                return CodexLastMessageCaptureOutcome::InvalidUtf8;
            };
            if text.trim().is_empty() {
                return CodexLastMessageCaptureOutcome::Empty;
            }

            CodexLastMessageCaptureOutcome::Verified(text)
        }
    }

    impl Drop for OwnedCodexLastMessageCapture {
        fn drop(&mut self) {
            let Ok(dir_stat) = fstat_fd(self.directory_file.as_raw_fd()) else {
                return;
            };
            if dir_stat.st_dev != self.dir_dev
                || dir_stat.st_ino != self.dir_ino
                || dir_stat.st_uid != self.expected_uid
            {
                return;
            }

            let file_cstr = entry_cstr();
            if let Ok(entry_stat) = fstatat_nofollow(self.directory_file.as_raw_fd(), &file_cstr) {
                if (entry_stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
                    // SAFETY: `self.directory_file` is our verified directory fd and `unlinkat`
                    // with flags `0` removes only the named non-directory entry inside that
                    // directory without following symlinks.
                    let _ = unsafe {
                        libc::unlinkat(self.directory_file.as_raw_fd(), file_cstr.as_ptr(), 0)
                    };
                }
            }

            if let Ok(parent_entry_stat) =
                fstatat_nofollow(self.parent_file.as_raw_fd(), &self.directory_name_cstr)
            {
                if (parent_entry_stat.st_mode & libc::S_IFMT) == libc::S_IFDIR
                    && parent_entry_stat.st_dev == self.dir_dev
                    && parent_entry_stat.st_ino == self.dir_ino
                    && parent_entry_stat.st_uid == self.expected_uid
                {
                    // SAFETY: `self.parent_file` is an open parent directory fd and `AT_REMOVEDIR`
                    // removes only an empty directory matching our verified `(dev, ino, uid)`.
                    let _ = unsafe {
                        libc::unlinkat(
                            self.parent_file.as_raw_fd(),
                            self.directory_name_cstr.as_ptr(),
                            libc::AT_REMOVEDIR,
                        )
                    };
                }
            }
        }
    }
}

#[cfg(not(unix))]
mod codex_last_message_capture {
    use std::path::Path;

    use crate::advisory_response::CodexLastMessageCaptureOutcome;
    use crate::error::WorkerError;
    use crate::fs_safety::ensure_platform_supported;

    pub(super) struct OwnedCodexLastMessageCapture;

    impl OwnedCodexLastMessageCapture {
        pub(super) fn create(_canonical_workdir: &Path) -> Result<Self, WorkerError> {
            ensure_platform_supported()?;
            unreachable!("non-Unix platforms are rejected by ensure_platform_supported")
        }

        pub(super) fn output_path(&self) -> &Path {
            Path::new("")
        }

        pub(super) fn read_outcome(
            &self,
            _max_response_bytes: usize,
        ) -> CodexLastMessageCaptureOutcome {
            CodexLastMessageCaptureOutcome::Missing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use crate::opencode_metadata::OpenCodeMetadataError;
    use crate::permissions::WorkerFilePermissions;
    use crate::spec::ExecutionBudgets;

    #[cfg(target_os = "linux")]
    static CAPTURE_TEMP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn sample_report_with_metadata_error(
        opencode_metadata_error: Option<&str>,
    ) -> WorkerExecutionReport {
        let target = ResolvedModelTarget {
            base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
            cli_model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        };
        let parser = NdjsonStreamParser::new_for_provider(
            WorkerProvider::OpenCodeV2,
            target.clone(),
            Some("ses_owned_123".to_string()),
            16,
        );
        WorkerExecutionReport {
            provider: WorkerProvider::OpenCodeV2,
            executed_executable: None,
            requested_model: target,
            requested_session_id: None,
            allocated_session_id: Some("ses_owned_123".to_string()),
            constructed_argv: Vec::new(),
            working_directory: PathBuf::from("/tmp"),
            status: WorkerRunStatus::NonZeroExit,
            exit_code: Some(0),
            signal: None,
            process_group_id: Some(42),
            duration_ms: 10,
            stdout_bytes_captured: 0,
            stderr_bytes_captured: 0,
            overflow: None,
            descendants_terminated_after_exit: false,
            cleanup_incomplete: false,
            redacted_stderr_summary: None,
            dropped_live_events: 0,
            evidence: parser.finalize(),
            opencode_metadata: None,
            opencode_metadata_error: opencode_metadata_error.map(str::to_string),
            progress_watchdog: None,
            provider_health: None,
        }
    }

    #[test]
    fn report_opencode_metadata_diagnostic_helpers_handle_legacy_staged_semantic_and_untrusted_values(
    ) {
        // 1. None when no metadata error is recorded.
        let clean = sample_report_with_metadata_error(None);
        assert_eq!(clean.opencode_metadata_diagnostic(), None);
        assert_eq!(clean.opencode_metadata_stage(), None);
        assert_eq!(clean.sanitized_opencode_metadata_error(), None);
        assert_eq!(clean.opencode_metadata_failure_reason(), None);

        // 2. Legacy undifferentiated `invalid_metadata` remains readable with `stage: None`.
        let legacy = sample_report_with_metadata_error(Some("invalid_metadata"));
        assert_eq!(
            legacy.opencode_metadata_diagnostic(),
            Some(OpenCodeMetadataDiagnostic {
                error: OpenCodeMetadataError::InvalidMetadata,
                stage: None,
            })
        );
        assert_eq!(legacy.opencode_metadata_stage(), None);
        assert_eq!(
            legacy.sanitized_opencode_metadata_error(),
            Some("invalid_metadata")
        );
        assert_eq!(
            legacy.opencode_metadata_failure_reason().as_deref(),
            Some("OpenCode session metadata verification failed (invalid_metadata)")
        );
        assert!(legacy.validated_opencode_metadata().is_none());
        assert!(!legacy.has_complete_natural_completion_evidence());

        // 3. Every finite stage round-trips through `WorkerExecutionReport`.
        for stage in OpenCodeMetadataStage::ALL {
            let code = stage.qualified_error_code();
            let staged = sample_report_with_metadata_error(Some(code));
            assert_eq!(
                staged.opencode_metadata_diagnostic(),
                Some(OpenCodeMetadataDiagnostic {
                    error: OpenCodeMetadataError::InvalidMetadata,
                    stage: Some(stage),
                })
            );
            assert_eq!(staged.opencode_metadata_stage(), Some(stage));
            assert_eq!(staged.sanitized_opencode_metadata_error(), Some(code));
            assert_eq!(
                staged.opencode_metadata_failure_reason(),
                Some(format!(
                    "OpenCode session metadata verification failed ({code})"
                ))
            );
            assert!(staged.validated_opencode_metadata().is_none());
            assert!(!staged.has_complete_natural_completion_evidence());
        }

        // 4. Semantic validator/collector codes preserve their exact code with `stage: None`.
        for semantic in [
            "output_overflow",
            "ownership_mismatch",
            "stale_session",
            "missing_model",
            "model_mismatch",
            "missing_effort",
            "effort_mismatch",
            "missing_completion",
            "provider_error",
            "cleanup_incomplete",
            "timed_out",
            "cancelled",
        ] {
            let report = sample_report_with_metadata_error(Some(semantic));
            assert_eq!(report.opencode_metadata_stage(), None);
            assert_eq!(report.sanitized_opencode_metadata_error(), Some(semantic));
            assert_eq!(
                report.opencode_metadata_failure_reason(),
                Some(format!(
                    "OpenCode session metadata verification failed ({semantic})"
                ))
            );
        }

        // 5. Untrusted/unrecognized strings never escape through sanitized helpers.
        for untrusted in [
            "invalid_metadata:SENTINEL_UNTRUSTED_STAGE",
            "raw_payload:SENTINEL_SECRET_TOKEN",
            "ownership_mismatch:info_response_json",
        ] {
            let tampered = sample_report_with_metadata_error(Some(untrusted));
            assert_eq!(tampered.opencode_metadata_diagnostic(), None);
            assert_eq!(tampered.opencode_metadata_stage(), None);
            assert_eq!(
                tampered.sanitized_opencode_metadata_error(),
                Some("invalid_metadata")
            );
            let reason = tampered
                .opencode_metadata_failure_reason()
                .expect("failure reason");
            assert_eq!(
                reason,
                "OpenCode session metadata verification failed (invalid_metadata)"
            );
            assert!(!reason.contains("SENTINEL"));
        }
    }

    #[tokio::test]
    async fn run_worker_propagates_staged_opencode_metadata_diagnostics_and_preserves_semantic_failures(
    ) {
        let temp = tempfile::Builder::new()
            .prefix("tl-sup-oc-diag-")
            .tempdir()
            .expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let bin_dir = root.join("bin");
        let repo_dir = root.join("repo");
        let work_area = root.join("work-area");
        fs::create_dir_all(&bin_dir).expect("create bin");
        fs::create_dir_all(&repo_dir).expect("create repo");
        fs::create_dir_all(&work_area).expect("create work area");
        #[cfg(unix)]
        {
            for dir in [&root, &bin_dir, &repo_dir, &work_area] {
                fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
                    .expect("chmod test dir");
            }
        }

        let mode_path = root.join("mode.txt");
        let created_ms_path = root.join("created_ms.txt");
        let staging_bin_path = bin_dir.join("fake-opencode.tmp");
        let bin_path = bin_dir.join("fake-opencode");
        let script = format!(
            r#"#!/bin/sh
set -eu
MODE="$(cat "{}")"
CREATED_MS_FILE="{}"

if [ "${{1:-}}" = "api" ] && [ "${{2:-}}" = "--standalone" ] && [ "${{3:-}}" = "session.create" ]; then
    if [ "$MODE" = "create_bad_json" ]; then
        printf 'corrupt-create SENTINEL_SUP_CREATE\n'
        exit 0
    fi
    NOW_S="$(date +%s)"
    CREATED_MS=$((NOW_S * 1000 + 2000))
    printf '%s\n' "$CREATED_MS" > "$CREATED_MS_FILE"
    printf '{{"data":{{"id":"ses_owned_123","location":{{"directory":"%s"}},"time":{{"created":%s}}}}}}\n' "$(pwd)" "$CREATED_MS"
    exit 0
fi

if [ "${{1:-}}" = "--standalone" ] && [ "${{2:-}}" = "run" ]; then
    printf '{{"type":"step_start","sessionID":"ses_owned_123"}}\n'
    exit 0
fi

CREATED_MS="$(cat "$CREATED_MS_FILE")"
MSG_CREATED_MS=$((CREATED_MS + 20))
MSG_COMPLETED_MS=$((CREATED_MS + 30))
IDLE_MS=$((CREATED_MS + 40))

if [ "${{1:-}}" = "api" ] && [ "${{2:-}}" = "--standalone" ] && [ "${{3:-}}" = "session.get" ]; then
    case "$MODE" in
        info_bad_json)
            printf 'corrupt-info SENTINEL_SUP_INFO\n'
            exit 0
            ;;
        bad_timestamps)
            printf '{{"data":{{"id":"ses_owned_123","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":"SENTINEL_BAD_IDLE"}}}}}}\n' "$(pwd)" "$CREATED_MS" "$IDLE_MS"
            exit 0
            ;;
        *)
            printf '{{"data":{{"id":"ses_owned_123","location":{{"directory":"%s"}},"outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}}}}\n' "$(pwd)" "$CREATED_MS" "$IDLE_MS" "$IDLE_MS"
            exit 0
            ;;
    esac
fi

if [ "${{1:-}}" = "session" ] && [ "${{2:-}}" = "export" ]; then
    case "$MODE" in
        valid_baseline|info_bad_json|bad_timestamps)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":"stop","model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}},"time":{{"created":%s,"completed":%s}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS" "$MSG_CREATED_MS" "$MSG_COMPLETED_MS"
            exit 0
            ;;
        export_bad_json)
            printf '{{corrupt-export SENTINEL_SUP_EXPORT\n'
            exit 0
            ;;
        bad_message_envelope)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","info":{{"role":"assistant","content":"SENTINEL_NESTED_CONTENT"}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS"
            exit 0
            ;;
        bad_assistant_target)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":"stop","model":{{"providerID":"opencode-go","id":"deepseek SENTINEL_UNSAFE_MODEL","variant":"max"}},"time":{{"created":%s,"completed":%s}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS" "$MSG_CREATED_MS" "$MSG_COMPLETED_MS"
            exit 0
            ;;
        bad_terminal_completion)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":{{"reason":"SENTINEL_BAD_FINISH"}},"model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}},"time":{{"created":%s,"completed":%s}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS" "$MSG_CREATED_MS" "$MSG_COMPLETED_MS"
            exit 0
            ;;
        semantic_model_mismatch)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":"stop","model":{{"providerID":"opencode-go","id":"other-model-SENTINEL","variant":"max"}},"time":{{"created":%s,"completed":%s}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS" "$MSG_CREATED_MS" "$MSG_COMPLETED_MS"
            exit 0
            ;;
        semantic_missing_completion)
            printf '{{"info":{{"id":"ses_owned_123","outcome":"succeeded","time":{{"created":%s,"updated":%s,"idle":%s}}}},"messages":[{{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":"length","model":{{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}},"time":{{"created":%s,"completed":%s}}}}]}}\n' "$CREATED_MS" "$IDLE_MS" "$IDLE_MS" "$MSG_CREATED_MS" "$MSG_COMPLETED_MS"
            exit 0
            ;;
    esac
fi

exit 99
"#,
            mode_path.display(),
            created_ms_path.display()
        );
        {
            use std::io::Write;
            let mut file = fs::File::create(&staging_bin_path).expect("create script");
            #[cfg(unix)]
            {
                file.set_permissions(fs::Permissions::from_mode(0o700))
                    .expect("set script permissions");
            }
            file.write_all(script.as_bytes()).expect("write script");
            file.sync_all().expect("sync script");
        }
        fs::rename(&staging_bin_path, &bin_path).expect("rename script into place");

        let mut allowed_write_paths = BTreeSet::new();
        allowed_write_paths.insert("docs/ROADMAP.md".to_string());
        let spec = WorkerSpec {
            provider: WorkerProvider::OpenCodeV2,
            executable: bin_path,
            model: "opencode-go/deepseek-v4.1-flash#max".to_string(),
            effort: Some("max".to_string()),
            allowed_models: BTreeSet::new(),
            session_id: None,
            standalone: true,
            repository_path: repo_dir.clone(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
            work_area_root: work_area,
            allowed_write_paths,
            prompt: "diagnostic test".to_string(),
            file_permissions: WorkerFilePermissions::Inherit,
            budgets: ExecutionBudgets::default(),
        };

        fs::write(&mode_path, "valid_baseline").expect("write baseline mode");
        let baseline_report = run_worker(&spec, &repo_dir)
            .await
            .expect("run_worker baseline");
        assert_eq!(baseline_report.status, WorkerRunStatus::Succeeded);
        assert!(baseline_report.opencode_metadata_error.is_none());
        assert!(baseline_report.validated_opencode_metadata().is_some());
        assert!(baseline_report.has_complete_natural_completion_evidence());

        let cases = [
            (
                "create_bad_json",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:create_response_json",
                Some(OpenCodeMetadataStage::CreateResponseJson),
            ),
            (
                "info_bad_json",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:info_response_json",
                Some(OpenCodeMetadataStage::InfoResponseJson),
            ),
            (
                "export_bad_json",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:export_json",
                Some(OpenCodeMetadataStage::ExportJson),
            ),
            (
                "bad_timestamps",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:timestamps",
                Some(OpenCodeMetadataStage::Timestamps),
            ),
            (
                "bad_message_envelope",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:message_envelope",
                Some(OpenCodeMetadataStage::MessageEnvelope),
            ),
            (
                "bad_assistant_target",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:assistant_target",
                Some(OpenCodeMetadataStage::AssistantTarget),
            ),
            (
                "bad_terminal_completion",
                WorkerRunStatus::NonZeroExit,
                "invalid_metadata:terminal_completion",
                Some(OpenCodeMetadataStage::TerminalCompletion),
            ),
            (
                "semantic_model_mismatch",
                WorkerRunStatus::ModelMismatch,
                "model_mismatch",
                None,
            ),
            (
                "semantic_missing_completion",
                WorkerRunStatus::MissingResultEvidence,
                "missing_completion",
                None,
            ),
        ];

        for (mode, expected_status, expected_code, expected_stage) in cases {
            fs::write(&mode_path, mode).expect("write mode");
            let report = run_worker(&spec, &repo_dir).await.expect("run_worker");
            assert_eq!(report.status, expected_status, "mode={mode}");
            assert_eq!(
                report.opencode_metadata_error.as_deref(),
                Some(expected_code),
                "mode={mode}"
            );
            assert_eq!(
                report.opencode_metadata_stage(),
                expected_stage,
                "mode={mode}"
            );
            assert_eq!(
                report.sanitized_opencode_metadata_error(),
                Some(expected_code),
                "mode={mode}"
            );
            let reason = report
                .opencode_metadata_failure_reason()
                .expect("failure reason");
            assert_eq!(
                reason,
                format!("OpenCode session metadata verification failed ({expected_code})"),
                "mode={mode}"
            );
            assert!(!reason.contains("SENTINEL"), "mode={mode}");
            assert!(
                report.validated_opencode_metadata().is_none(),
                "mode={mode}"
            );
            assert!(
                !report.has_complete_natural_completion_evidence(),
                "mode={mode}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_etxtbsy_retry_releases_on_first_exact_refusal_and_executes_once() {
        let _capture_temp_guard = CAPTURE_TEMP_TEST_LOCK.lock().await;
        let temp = tempfile::Builder::new()
            .prefix("tl-sup-etxtbsy-kernel-")
            .tempdir()
            .expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let workdir = root.join("work");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&workdir).expect("create workdir");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        let launch_log = root.join("launch.log");
        let script_path = bin_dir.join("fake-agy-etxtbsy");
        let script = format!(
            r#"#!/bin/sh
set -eu
{{
  printf 'BEGIN\n'
  printf 'cwd=%s\n' "$PWD"
  for arg in "$@"; do printf 'arg=%s\n' "$arg"; done
  printf 'env=%s\n' "${{TL_TEST_ENV:-}}"
}} >> "{}"
printf '%s\n' '{{"event":"init","init":{{"session_id":"agy-etxtbsy-1","model":"gemini-4-argon-high","effort":"high"}}}}'
printf '%s\n' '{{"event":"result","result":{{"status":"SUCCESS","response":"{{\"ok\":true}}"}}}}'
"#,
            launch_log.display()
        );
        fs::write(&script_path, script).expect("write script");
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o700)).expect("chmod script");
        let mut held_writer = Some(
            fs::OpenOptions::new()
                .write(true)
                .open(&script_path)
                .expect("hold writable executable descriptor"),
        );
        let mut first_refusal_observed = false;
        let mut refusal_attempts = Vec::new();

        let (health_tx, _health_rx) = watch::channel(None);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let inputs = SupervisedLaunchInputs {
            provider: WorkerProvider::Antigravity,
            canonical_executable: script_path.clone(),
            canonical_workdir: workdir.clone(),
            resolved_model: ResolvedModelTarget {
                base_model: "gemini-4-argon-high".to_string(),
                effort: Some("high".to_string()),
                cli_model_arg: "gemini-4-argon-high".to_string(),
            },
            requested_session_id: None,
            initial_argv: vec!["alpha".to_string(), "beta".to_string()],
            child_env: vec![
                ("TL_TEST_ENV".to_string(), "stable".to_string()),
                (
                    "TL_LAUNCH_LOG".to_string(),
                    launch_log.display().to_string(),
                ),
            ],
            budgets: ExecutionBudgets::default(),
        };
        let mut on_spawn_error = |attempt: u32, err: &std::io::Error| {
            assert_eq!(attempt, 1, "only the first attempt may hit the held writer");
            assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
            refusal_attempts.push(attempt);
            first_refusal_observed = true;
            drop(held_writer.take());
        };
        let (report, collector) = run_supervised_process_group_for_test(
            inputs,
            |_| Ok(Vec::new()),
            None,
            &mut cancel_rx,
            None,
            Some(health_tx),
            None,
            &mut on_spawn_error,
        )
        .await
        .expect("retry after synchronized writer release");

        assert!(first_refusal_observed);
        assert_eq!(refusal_attempts, vec![1]);
        assert!(collector.is_none());
        assert_eq!(report.status, WorkerRunStatus::Succeeded);
        assert_eq!(report.constructed_argv.len(), 4);
        assert_eq!(report.constructed_argv[0], "--log-file");
        assert_eq!(report.constructed_argv[2], "alpha");
        assert_eq!(report.constructed_argv[3], "beta");
        assert_eq!(
            report
                .constructed_argv
                .iter()
                .filter(|arg| arg.as_str() == "--log-file")
                .count(),
            1,
            "AGY diagnostic capture setup must not be repeated by retries"
        );
        let observed = fs::read_to_string(&launch_log).expect("read launch log");
        assert_eq!(observed.matches("BEGIN").count(), 1);
        assert!(observed.contains(&format!("cwd={}", workdir.display())));
        assert!(observed.contains("arg=alpha"));
        assert!(observed.contains("arg=beta"));
        assert!(observed.contains("env=stable"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_codex_capture_setup_survives_etxtbsy_retry_without_duplication() {
        let _capture_temp_guard = CAPTURE_TEMP_TEST_LOCK.lock().await;
        let temp = tempfile::Builder::new()
            .prefix("tl-sup-codex-retry-")
            .tempdir()
            .expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let workdir = root.join("work");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&workdir).expect("create workdir");
        fs::create_dir_all(workdir.join(".git")).expect("create .git");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        let launch_log = root.join("launch.log");
        let script_path = bin_dir.join("fake-codex-etxtbsy");
        let payload = serde_json::json!({"ok": true}).to_string();
        let terminal_event = serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "msg_1",
                "type": "agent_message",
                "text": payload
            }
        })
        .to_string();
        let script = format!(
            r#"#!/usr/bin/env sh
set -eu
LOG="{}"
printf 'BEGIN\n' >> "$LOG"
OUT=""
for arg in "$@"; do
  printf 'arg=%s\n' "$arg" >> "$LOG"
done
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-last-message|-o) OUT="$2"; shift 2 ;;
    --) shift; break ;;
    *) shift 1 ;;
  esac
done
printf '%s\n' '{{"type":"thread.started","thread_id":"thread-etxtbsy-codex"}}'
printf '%s\n' '{{"type":"turn.started"}}'
printf '%s\n' '{}'
printf '%s\n' '{{"type":"turn.completed","usage":{{"input_tokens":10,"output_tokens":5}}}}'
printf '%s' '{}' > "$OUT"
"#,
            launch_log.display(),
            terminal_event,
            payload
        );
        fs::write(&script_path, script).expect("write script");
        fs::set_permissions(&script_path, fs::Permissions::from_mode(0o700)).expect("chmod script");
        let mut held_writer = Some(
            fs::OpenOptions::new()
                .write(true)
                .open(&script_path)
                .expect("hold writable executable descriptor"),
        );
        let spec = crate::spec::AdvisoryWorkerSpec {
            provider: WorkerProvider::CodexCli,
            executable: script_path,
            model: "gpt-6.1-sol".to_string(),
            effort: Some("xhigh".to_string()),
            allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
            session_id: None,
            standalone: false,
            working_directory: workdir,
            allowed_write_paths: BTreeSet::new(),
            prompt: "retry Codex capture".to_string(),
            file_permissions: WorkerFilePermissions::ReadOnly,
            budgets: ExecutionBudgets::default(),
        };
        let validation = spec.validate().expect("validate Codex spec");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let mut refused_attempts = Vec::new();
        let mut on_spawn_error = |attempt: u32, err: &std::io::Error| {
            assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
            refused_attempts.push(attempt);
            drop(held_writer.take());
        };
        let inputs = SupervisedLaunchInputs {
            provider: validation.provider,
            canonical_executable: validation.canonical_executable,
            canonical_workdir: validation.canonical_working_directory,
            resolved_model: validation.resolved_model,
            requested_session_id: None,
            initial_argv: validation.constructed_argv,
            child_env: validation.child_env,
            budgets: validation.budgets,
        };
        let (report, collector) = run_supervised_process_group_for_test(
            inputs,
            |_| Ok(Vec::new()),
            Some(AdvisoryResponseBudget::default()),
            &mut cancel_rx,
            None,
            None,
            None,
            &mut on_spawn_error,
        )
        .await
        .expect("Codex retry after synchronized writer release");
        assert_eq!(refused_attempts, vec![1]);
        let structured = collector.expect("Codex capture collector").finalize(report);
        assert_eq!(structured.execution.status, WorkerRunStatus::Succeeded);
        assert!(
            structured.has_verified_response(),
            "retried Codex capture must retain verified structured output"
        );
        assert_eq!(
            structured
                .execution
                .constructed_argv
                .iter()
                .filter(|arg| arg.as_str() == "--output-last-message")
                .count(),
            1,
            "Codex capture arguments must not be duplicated by retries"
        );
        let observed = fs::read_to_string(&launch_log).expect("read launch log");
        assert_eq!(observed.matches("BEGIN").count(), 1);
    }

    #[cfg(target_os = "linux")]
    fn test_executable_file(root: &Path, name: &str, body: &str) -> PathBuf {
        let path = root.join(name);
        fs::write(&path, body).expect("write test executable");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("chmod test executable");
        path
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_spawn_retry_policy_is_exact_bounded_and_abortable() {
        let temp = tempfile::tempdir().expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let executable = test_executable_file(&root, "executable", "#!/bin/sh\nexit 0\n");
        let far_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::EACCES))
            },
            &executable,
            &mut cancel_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(attempts, 1, "non-ETXTBSY errors must not retry");
        match outcome {
            LaunchAttemptOutcome::Failed(WorkerError::Io(err)) => {
                assert_eq!(err.raw_os_error(), Some(libc::EACCES));
            }
            _ => panic!("non-ETXTBSY error must surface immediately"),
        }

        let mut attempts = 0u32;
        let mut retry_errors = Vec::new();
        let started = Instant::now();
        let mut retry_observer = |attempt: u32, err: &std::io::Error| {
            retry_errors.push((attempt, err.raw_os_error()));
        };
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut cancel_rx,
            far_deadline,
            Some(&mut retry_observer),
        )
        .await;
        assert_eq!(attempts, ETXTBSY_MAX_SPAWN_ATTEMPTS);
        assert_eq!(
            retry_errors,
            vec![
                (1, Some(libc::ETXTBSY)),
                (2, Some(libc::ETXTBSY)),
                (3, Some(libc::ETXTBSY))
            ]
        );
        assert!(started.elapsed() >= Duration::from_millis(30));
        match outcome {
            LaunchAttemptOutcome::Failed(WorkerError::Io(err)) => {
                assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
            }
            _ => panic!("exhaustion must surface the final exact IO refusal"),
        }

        let (_cancel_tx, mut cancelled_rx) = watch::channel(true);
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut cancelled_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(attempts, 0, "already-cancelled launches must not spawn");
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Aborted(WorkerRunStatus::Cancelled)
        ));

        let (_cancel_tx, mut expired_rx) = watch::channel(false);
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut expired_rx,
            tokio::time::Instant::now(),
            None,
        )
        .await;
        assert_eq!(attempts, 0, "expired launches must not spawn");
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Aborted(WorkerRunStatus::TimedOut)
        ));

        let (cancel_tx, mut cancel_during_backoff_rx) = watch::channel(false);
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                let _ = cancel_tx.send(true);
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut cancel_during_backoff_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(
            attempts, 1,
            "backoff cancellation must prevent another spawn"
        );
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Aborted(WorkerRunStatus::Cancelled)
        ));

        let (_cancel_tx, mut deadline_during_backoff_rx) = watch::channel(false);
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut deadline_during_backoff_rx,
            tokio::time::Instant::now() + Duration::from_millis(9),
            None,
        )
        .await;
        assert!(attempts <= 1, "deadline backoff must prevent another spawn");
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Aborted(WorkerRunStatus::TimedOut)
        ));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_executable_replacement_symlink_and_permission_drift_are_refused() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let far_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        let executable = test_executable_file(&root, "replace-target", "#!/bin/sh\nexit 0\n");
        let replacement = test_executable_file(&root, "replace-source", "#!/bin/sh\nexit 1\n");
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                fs::rename(&replacement, &executable).expect("replace executable");
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &executable,
            &mut cancel_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(attempts, 1);
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Failed(WorkerError::SecurityViolation(_))
        ));

        let symlink_target = test_executable_file(&root, "symlink-target", "#!/bin/sh\nexit 0\n");
        let symlink_path = test_executable_file(&root, "symlink-executable", "#!/bin/sh\nexit 0\n");
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                fs::remove_file(&symlink_path).expect("remove regular path");
                symlink(&symlink_target, &symlink_path).expect("replace with symlink");
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &symlink_path,
            &mut cancel_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(attempts, 1);
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Failed(WorkerError::SecurityViolation(_))
        ));

        let mode_path = test_executable_file(&root, "mode-executable", "#!/bin/sh\nexit 0\n");
        let mut attempts = 0u32;
        let outcome = launch_with_bounded_etxtbsy_retry(
            || {
                attempts += 1;
                fs::set_permissions(&mode_path, fs::Permissions::from_mode(0o600))
                    .expect("remove execute permission");
                Err(std::io::Error::from_raw_os_error(libc::ETXTBSY))
            },
            &mode_path,
            &mut cancel_rx,
            far_deadline,
            None,
        )
        .await;
        assert_eq!(attempts, 1);
        assert!(matches!(
            outcome,
            LaunchAttemptOutcome::Failed(WorkerError::InvalidSpec(_))
        ));
    }

    // Capture identity comes from the exact argv built for this invocation; shared temp prefixes
    // are never used to attribute ownership or cleanup failures.
    #[cfg(target_os = "linux")]
    fn argv_option_path(argv: &[String], option: &str) -> PathBuf {
        argv.windows(2)
            .find(|window| window[0].as_str() == option)
            .map(|window| PathBuf::from(window[1].as_str()))
            .unwrap_or_else(|| panic!("argv must contain {option} capture path"))
    }

    #[cfg(target_os = "linux")]
    async fn assert_owned_capture_paths_removed(paths: &[PathBuf]) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            let remaining = paths
                .iter()
                .filter(|path| match fs::symlink_metadata(path) {
                    Ok(_) => true,
                    Err(err) => err.kind() != std::io::ErrorKind::NotFound,
                })
                .collect::<Vec<_>>();
            if remaining.is_empty() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no-child cleanup left exact owned capture paths: {remaining:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_codex_capture_and_agy_diagnostic_zero_child_exhaustion_cleanup_is_bounded() {
        let temp = tempfile::Builder::new()
            .prefix("tl-sup-zero-child-")
            .tempdir()
            .expect("create tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");
        let workdir = root.join("work");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&workdir).expect("create workdir");
        fs::create_dir_all(workdir.join(".git")).expect("create .git");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        let sentinel = workdir.join("unrelated-sentinel.txt");
        fs::write(&sentinel, b"preserve unrelated data").expect("write sentinel");

        let codex_executable = test_executable_file(
            &bin_dir,
            "fake-codex-held",
            "#!/bin/sh\nprintf 'executed\\n' >> codex-executed.txt\nexit 0\n",
        );
        let _codex_writer = fs::OpenOptions::new()
            .write(true)
            .open(&codex_executable)
            .expect("hold Codex executable open for write");
        let codex_spec = crate::spec::AdvisoryWorkerSpec {
            provider: WorkerProvider::CodexCli,
            executable: codex_executable,
            model: "gpt-6.1-sol".to_string(),
            effort: Some("xhigh".to_string()),
            allowed_models: BTreeSet::from(["gpt-6.1-sol".to_string()]),
            session_id: None,
            standalone: false,
            working_directory: workdir.clone(),
            allowed_write_paths: BTreeSet::new(),
            prompt: "zero-child Codex cleanup".to_string(),
            file_permissions: WorkerFilePermissions::ReadOnly,
            budgets: ExecutionBudgets::default(),
        };
        let codex_validation = codex_spec.validate().expect("validate Codex spec");
        let (_cancel_tx, mut codex_cancel_rx) = watch::channel(false);
        let mut codex_attempts = Vec::new();
        let codex_inputs = SupervisedLaunchInputs {
            provider: codex_validation.provider,
            canonical_executable: codex_validation.canonical_executable,
            canonical_workdir: codex_validation.canonical_working_directory,
            resolved_model: codex_validation.resolved_model,
            requested_session_id: None,
            initial_argv: codex_validation.constructed_argv,
            child_env: codex_validation.child_env,
            budgets: codex_validation.budgets,
        };
        let mut codex_capture_paths = Vec::new();
        let codex_err = {
            let mut codex_observer = |attempt: u32, err: &std::io::Error| {
                assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
                codex_attempts.push(attempt);
            };
            let mut codex_launch_observer = |argv: &[String]| {
                let output = argv_option_path(argv, "--output-last-message");
                let directory = output
                    .parent()
                    .expect("Codex capture output path must have a parent")
                    .to_path_buf();
                assert!(
                    fs::symlink_metadata(&directory).is_ok(),
                    "Codex capture directory must exist before launch"
                );
                codex_capture_paths.push((output, directory));
            };
            match run_supervised_process_group_for_test(
                codex_inputs,
                |_| Ok(Vec::new()),
                Some(AdvisoryResponseBudget::default()),
                &mut codex_cancel_rx,
                None,
                None,
                Some(&mut codex_launch_observer),
                &mut codex_observer,
            )
            .await
            {
                Ok(_) => panic!("held Codex executable must exhaust retries"),
                Err(err) => err,
            }
        };
        match codex_err {
            WorkerError::Io(err) => assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY)),
            other => panic!("unexpected Codex exhaustion error: {other}"),
        }
        assert_eq!(codex_attempts, vec![1, 2, 3]);
        assert!(!workdir.join("codex-executed.txt").exists());
        assert_eq!(
            fs::read(&sentinel).expect("read sentinel"),
            b"preserve unrelated data"
        );
        assert_eq!(
            codex_capture_paths.len(),
            1,
            "Codex capture setup must run exactly once"
        );
        let (codex_capture_output, codex_capture_directory) = &codex_capture_paths[0];
        assert_owned_capture_paths_removed(&[
            codex_capture_output.to_path_buf(),
            codex_capture_directory.to_path_buf(),
        ])
        .await;

        let agy_executable = test_executable_file(
            &bin_dir,
            "fake-agy-held",
            "#!/bin/sh\nprintf 'executed\\n' >> agy-executed.txt\nexit 0\n",
        );
        let _agy_writer = fs::OpenOptions::new()
            .write(true)
            .open(&agy_executable)
            .expect("hold AGY executable open for write");
        let (health_tx, _health_rx) = watch::channel(None);
        let (_cancel_tx, mut agy_cancel_rx) = watch::channel(false);
        let mut agy_attempts = Vec::new();
        let agy_inputs = SupervisedLaunchInputs {
            provider: WorkerProvider::Antigravity,
            canonical_executable: agy_executable,
            canonical_workdir: workdir.clone(),
            resolved_model: ResolvedModelTarget {
                base_model: "gemini-4-argon-high".to_string(),
                effort: Some("high".to_string()),
                cli_model_arg: "gemini-4-argon-high".to_string(),
            },
            requested_session_id: None,
            initial_argv: vec!["--model".to_string(), "gemini-4-argon-high".to_string()],
            child_env: Vec::new(),
            budgets: ExecutionBudgets::default(),
        };
        let mut agy_capture_paths = Vec::new();
        let agy_err = {
            let mut agy_observer = |attempt: u32, err: &std::io::Error| {
                assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY));
                agy_attempts.push(attempt);
            };
            let mut agy_launch_observer = |argv: &[String]| {
                let log = argv_option_path(argv, "--log-file");
                let directory = log
                    .parent()
                    .expect("AGY capture log path must have a parent")
                    .to_path_buf();
                assert!(
                    fs::symlink_metadata(&log).is_ok(),
                    "AGY capture log FIFO must exist before launch"
                );
                assert!(
                    fs::symlink_metadata(&directory).is_ok(),
                    "AGY capture directory must exist before launch"
                );
                agy_capture_paths.push((log, directory));
            };
            match run_supervised_process_group_for_test(
                agy_inputs,
                |_| Ok(Vec::new()),
                None,
                &mut agy_cancel_rx,
                None,
                Some(health_tx),
                Some(&mut agy_launch_observer),
                &mut agy_observer,
            )
            .await
            {
                Ok(_) => panic!("held AGY executable must exhaust retries"),
                Err(err) => err,
            }
        };
        match agy_err {
            WorkerError::Io(err) => assert_eq!(err.raw_os_error(), Some(libc::ETXTBSY)),
            other => panic!("unexpected AGY exhaustion error: {other}"),
        }
        assert_eq!(agy_attempts, vec![1, 2, 3]);
        assert!(!workdir.join("agy-executed.txt").exists());
        assert_eq!(
            fs::read(&sentinel).expect("read sentinel"),
            b"preserve unrelated data"
        );
        assert_eq!(
            agy_capture_paths.len(),
            1,
            "AGY capture setup must run exactly once"
        );
        let (agy_capture_log, agy_capture_directory) = &agy_capture_paths[0];
        assert_owned_capture_paths_removed(&[
            agy_capture_log.to_path_buf(),
            agy_capture_directory.to_path_buf(),
        ])
        .await;
    }
}
