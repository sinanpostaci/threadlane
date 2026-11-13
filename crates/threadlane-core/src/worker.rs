use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::engine::{
    enforce_compact_response_budget, generate_id, hash_dedup_payload, i64_to_u64, u64_to_i64, Core,
};
use crate::error::CoreError;
use crate::limits::{
    validate_bounded_text, validate_command_key, validate_identifier, validate_optional_text,
    validate_thread_uuid, validate_timestamp_ms, MAX_CANCEL_REASON_LEN, MAX_ID_LEN,
    MAX_INSTRUCTION_LEN, MAX_WORKER_CHANGED_PATHS_PREVIEW, MAX_WORKER_DIGEST_LEN,
    MAX_WORKER_PATH_BYTES, MAX_WORKER_PROFILE_ID_LEN, MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT,
    MAX_WORKER_SUMMARY_TEXT_BYTES, MAX_WORKER_VIOLATION_SUMMARIES,
};
use crate::types::{CommandOutput, EventKind, EventPayload, RunStatus};

/// Schema version for [`WorkerExecutionBinding`] records.
pub const WORKER_EXECUTION_BINDING_SCHEMA_VERSION: u32 = 1;

const WORKER_EXECUTION_BINDING_DOMAIN: &str = "threadlane.worker_execution_binding.v1";

/// Durable execution binding persisted on dependent worker runs to bind the static
/// `profile_digest`, exact instruction, sealed dependency input digest, on-disk seal file digest,
/// and synthesized `execution_base_commit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionBinding {
    /// Execution binding schema version (`1`).
    pub schema_version: u32,
    /// Lowercase 64-character SHA-256 hex `seal_digest` from the verified [`DependencyInputSeal`].
    pub input_digest: String,
    /// Lowercase 64-character SHA-256 hex digest of the raw on-disk dependency seal file bytes.
    pub seal_file_digest: String,
    /// Exact lowercase 40-character hexadecimal Git commit SHA used as the execution base.
    pub execution_base_commit: String,
    /// Canonical lowercase 64-character SHA-256 hex digest binding all execution inputs.
    pub execution_digest: String,
}

impl WorkerExecutionBinding {
    /// Validates all execution binding inputs and computes the canonical `execution_digest`.
    pub fn new(
        profile_digest: &str,
        instruction: &str,
        input_digest: &str,
        seal_file_digest: &str,
        execution_base_commit: &str,
    ) -> Result<Self, CoreError> {
        let clean_profile_digest = validate_profile_digest(profile_digest)?;
        let clean_instruction =
            validate_bounded_text("instruction", instruction, MAX_INSTRUCTION_LEN)?;
        let clean_input_digest = validate_sha256_digest("input_digest", input_digest)?;
        let clean_seal_file_digest = validate_sha256_digest("seal_file_digest", seal_file_digest)?;
        let clean_base_commit =
            validate_exact_40_hex_commit("execution_base_commit", execution_base_commit)?;

        let instruction_hash = sha256_hex_str(&clean_instruction);
        let execution_digest = hash_dedup_payload(&(
            WORKER_EXECUTION_BINDING_DOMAIN,
            WORKER_EXECUTION_BINDING_SCHEMA_VERSION,
            &clean_profile_digest,
            &instruction_hash,
            &clean_input_digest,
            &clean_seal_file_digest,
            &clean_base_commit,
        ))?;

        Ok(Self {
            schema_version: WORKER_EXECUTION_BINDING_SCHEMA_VERSION,
            input_digest: clean_input_digest,
            seal_file_digest: clean_seal_file_digest,
            execution_base_commit: clean_base_commit,
            execution_digest,
        })
    }

    /// Verifies that this execution binding is structurally valid and matches `profile_digest`,
    /// `instruction`, and `base_commit`.
    pub fn verify_against(
        &self,
        profile_digest: &str,
        instruction: &str,
        base_commit: &str,
    ) -> Result<(), CoreError> {
        if self.schema_version != WORKER_EXECUTION_BINDING_SCHEMA_VERSION {
            return Err(CoreError::Validation {
                field: "execution_binding",
                reason: format!(
                    "unsupported execution_binding schema_version {} (expected {WORKER_EXECUTION_BINDING_SCHEMA_VERSION})",
                    self.schema_version
                ),
            });
        }
        if self.execution_base_commit != base_commit {
            return Err(CoreError::Validation {
                field: "execution_binding",
                reason: format!(
                    "execution_binding.execution_base_commit `{}` does not match base_commit `{base_commit}`",
                    self.execution_base_commit
                ),
            });
        }
        let expected = Self::new(
            profile_digest,
            instruction,
            &self.input_digest,
            &self.seal_file_digest,
            base_commit,
        )?;
        if self != &expected {
            return Err(CoreError::Validation {
                field: "execution_binding",
                reason: "execution_binding.execution_digest does not match expected binding digest"
                    .to_string(),
            });
        }
        Ok(())
    }
}

const fn default_untrusted_data() -> bool {
    true
}

/// Lifecycle status of a durable worker execution record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    /// Worker run has been admitted and is waiting in the durable queue for a dispatch slot.
    Queued,
    /// Worker run has been claimed by the dispatcher prior to external worktree/process launch.
    Claimed,
    /// Worker process is actively executing inside its isolated task worktree.
    Running,
    /// Caller requested revision-checked cancellation; waiting for worker termination proof.
    CancelRequested,
    /// Worker process exited cleanly and passed provider/diff-gate checks (`ReadyForReview`).
    Completed,
    /// Worker failed execution, provider evidence checks, or diff-gate validation.
    Failed,
    /// Worker was proven stopped after cancellation.
    Cancelled,
    /// Worker exceeded its configured execution timeout budget.
    TimedOut,
    /// Worker was in an uncertain non-terminal state across restart or shutdown; never auto-retried.
    Unknown,
}

impl WorkerStatus {
    /// Returns `true` if this worker status is terminal (`Completed`, `Failed`, `Cancelled`, `TimedOut`, or `Unknown`).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Unknown
        )
    }

    /// Maps this worker status to the corresponding [`RunStatus`].
    pub fn to_run_status(self) -> RunStatus {
        match self {
            Self::Queued => RunStatus::Queued,
            Self::Claimed => RunStatus::Claimed,
            Self::Running => RunStatus::Running,
            Self::CancelRequested => RunStatus::CancelRequested,
            Self::Completed => RunStatus::Completed,
            Self::Failed => RunStatus::Failed,
            Self::Cancelled => RunStatus::Cancelled,
            Self::TimedOut => RunStatus::TimedOut,
            Self::Unknown => RunStatus::Unknown,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "claimed" => Some(Self::Claimed),
            "running" => Some(Self::Running),
            "cancel_requested" => Some(Self::CancelRequested),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "timed_out" => Some(Self::TimedOut),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Review and acceptance state of a worker execution attempt.
///
/// Process exit zero or passing the diff gate never automatically grants final acceptance;
/// successful runs reach at most [`WorkerAcceptanceState::ReadyForReview`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerAcceptanceState {
    /// Worker is still queued or running and has not produced a final diff/outcome.
    PendingReview,
    /// Worker exited cleanly, satisfied model evidence, and passed the diff gate; ready for review.
    ReadyForReview,
    /// Worker modified forbidden paths, created symlinks, or otherwise failed the diff gate.
    RejectedByDiffGate,
    /// Worker process failed, timed out, was cancelled, or lacked required model/cleanup evidence.
    ExecutionFailed,
    /// Worker outcome is uncertain due to restart or supervisor shutdown before completion proof.
    Unknown,
}

/// Compact owned worktree references associated with a worker run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerWorktreeSummary {
    /// Owned per-run integration worktree identifier (`run_<run_id>`).
    pub run_worktree_id: Option<String>,
    /// Owned per-run integration worktree path inside the private work area.
    pub run_worktree_path: Option<String>,
    /// Branch name of the per-run integration worktree.
    pub run_branch_name: Option<String>,
    /// Owned per-task worktree identifier (`task_<run_id>_<worker_id>`).
    pub task_worktree_id: Option<String>,
    /// Owned per-task worktree path inside the private work area.
    pub task_worktree_path: Option<String>,
    /// Branch name of the per-task worktree.
    pub task_branch_name: Option<String>,
    /// Resolved base commit hash or ref from the trusted profile.
    pub base_commit: String,
    /// Head commit hash in the task worktree after execution, if available.
    pub head_commit: Option<String>,
}

/// Maximum JSON byte length accepted for a software-supervision attention snapshot.
pub const MAX_WORKER_WATCHDOG_SNAPSHOT_BYTES: usize = 2048;

/// Fixed software-supervision state, independent of provider claims or task acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerProgressWatchdogStatus {
    /// The initial advisory deadline has not been reached.
    Observing,
    /// A supported recent tool hint extended the advisory deadline.
    SoftDeadlineExtended,
    /// Progress evidence is uncertain; execution continues within the hard ceiling.
    AttentionRequired,
    /// The existing hard runtime ceiling was reached.
    HardLimitReached,
}

/// Fixed advisory reason; no reason proves that an executor is stuck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerProgressAttentionReason {
    /// No supported advancement hint was observed.
    NoAdvancementHints,
    /// Read-only tool hints were observed without supported advancement hints.
    ReadOnlyActivity,
    /// Earlier advancement hints are outside the recent window.
    AdvancementNotRecent,
}

/// Core-owned bounded DTO for supervisor-generated progress attention.
///
/// Contains no provider prose, paths, prompts, model claims, or execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerProgressWatchdogSnapshot {
    /// Advisory software-supervision state at emission.
    pub status: WorkerProgressWatchdogStatus,
    /// Fixed reason for the advisory attention.
    pub attention_reason: Option<WorkerProgressAttentionReason>,
    /// Elapsed monotonic invocation time at emission.
    pub elapsed_ms: u64,
    /// Advisory deadline within the existing hard ceiling.
    pub soft_deadline_ms: u64,
    /// Existing absolute runtime ceiling, never increased by the watchdog.
    pub hard_timeout_ms: u64,
    /// Last supported advancement hint time, if observed.
    pub last_advancement_ms: Option<u64>,
    /// Count of normalized provider events observed.
    pub events_observed: u64,
    /// Supported write, edit, or check hints; not verified file changes or checks.
    pub advancement_hints: u64,
    /// Read-only tool hints.
    pub read_hints: u64,
    /// Provider output hints, which never extend a deadline.
    pub output_hints: u64,
    /// Other tool hints, which never extend a deadline.
    pub other_tool_hints: u64,
    /// Number of bounded advisory deadline extensions.
    pub extensions: u64,
    /// Whether the invocation attempted its single advisory attention event.
    pub attention_emitted: bool,
}

impl WorkerProgressWatchdogSnapshot {
    /// Validates an attention emitted while execution remains below its hard ceiling.
    ///
    /// Final report states are not accepted as live attention events.
    pub fn validate_attention(&self) -> Result<(), CoreError> {
        let total_hints = self
            .advancement_hints
            .saturating_add(self.read_hints)
            .saturating_add(self.output_hints)
            .saturating_add(self.other_tool_hints);
        let reason_matches = match self.attention_reason {
            Some(WorkerProgressAttentionReason::NoAdvancementHints) => {
                self.advancement_hints == 0 && self.read_hints == 0
            }
            Some(WorkerProgressAttentionReason::ReadOnlyActivity) => {
                self.advancement_hints == 0 && self.read_hints > 0
            }
            Some(WorkerProgressAttentionReason::AdvancementNotRecent) => self.advancement_hints > 0,
            None => false,
        };
        if self.status != WorkerProgressWatchdogStatus::AttentionRequired
            || !self.attention_emitted
            || !reason_matches
            || self.soft_deadline_ms == 0
            || self.soft_deadline_ms > self.elapsed_ms
            || self.elapsed_ms >= self.hard_timeout_ms
            || self
                .last_advancement_ms
                .is_some_and(|last| last > self.elapsed_ms)
            || (self.advancement_hints > 0) != self.last_advancement_ms.is_some()
            || total_hints > self.events_observed
        {
            return Err(CoreError::Validation {
                field: "watchdog_attention",
                reason: "invalid bounded software-supervision attention snapshot".to_string(),
            });
        }
        Ok(())
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Validates the bounded diagnostic allowlist without granting source or execution authority.
fn validate_provider_health(
    snapshot: &crate::provider_health::ProviderHealthSnapshot,
) -> Result<(), CoreError> {
    crate::provider_health::decode_provider_health_snapshot(&serde_json::to_vec(snapshot)?)?;
    Ok(())
}

fn merge_provider_health(
    previous: &mut Option<crate::provider_health::ProviderHealthSnapshot>,
    previous_high_water: &mut Option<u32>,
    incoming: &crate::provider_health::ProviderHealthSnapshot,
    incoming_high_water: Option<u32>,
) -> Result<bool, CoreError> {
    validate_provider_health(incoming)?;
    if incoming_high_water.is_some_and(|value| {
        value == 0
            || value > crate::provider_health::MAX_PROVIDER_HEALTH_RECORDS
            || incoming.retry_count == 0
    }) {
        return Err(CoreError::Validation {
            field: "provider_retry_high_water",
            reason: "invalid bounded unverified retry ordinal".to_owned(),
        });
    }
    if let Some(prior) = previous.as_ref() {
        // Delayed cumulative observations cannot rewind durable history or add counters.
        if incoming.update_sequence < prior.update_sequence {
            return Ok(false);
        }
        if incoming == prior
            && (incoming_high_water.is_none() || incoming_high_water == *previous_high_water)
        {
            return Ok(false);
        }
        if !incoming.is_valid_successor_of(prior)
            || incoming.update_sequence == prior.update_sequence
            || (incoming.retry_count - prior.retry_count
                > incoming.update_sequence - prior.update_sequence)
            || (incoming.retry_count == prior.retry_count
                && prior.recovery.is_some()
                && incoming.recovery.is_none())
            || match (*previous_high_water, incoming_high_water) {
                (Some(prior_high), Some(next)) => {
                    next < prior_high
                        || (next > prior_high && incoming.retry_count == prior.retry_count)
                }
                (Some(_), None) => true,
                _ => false,
            }
            || (incoming.retry_count > prior.retry_count
                && prior.last_failure.is_some_and(|last| {
                    incoming.last_failure.is_none_or(|next| {
                        next.update_sequence <= prior.update_sequence
                            || next.elapsed_ms < prior.elapsed_ms
                            || next.elapsed_ms < last.elapsed_ms
                    })
                }))
            || (prior.first_failure.is_none()
                && incoming.first_failure.is_some_and(|first| {
                    first.update_sequence <= prior.update_sequence
                        || first.elapsed_ms < prior.elapsed_ms
                }))
        {
            return Err(CoreError::Validation {
                field: "provider_health",
                reason: "inconsistent cumulative unverified diagnostic history".to_string(),
            });
        }
    }
    *previous = Some(incoming.clone());
    *previous_high_water = incoming_high_water;
    Ok(true)
}

/// Bounded live progress counters and latest redacted summary for a worker attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerProgressSummary {
    /// Highest retry ordinal reported by unverified diagnostic text, distinct from record count.
    /// Absent means unknown, never zero dispatch attempts or proven healthy transport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_retry_high_water: Option<u32>,
    /// A hint was durably observed on the live diagnostic channel. Terminal merging never
    /// sets this flag. Callbacks still require verified scope, owner and durable deduplication.
    #[serde(default, skip_serializing_if = "is_false")]
    pub provider_health_live_observed: bool,
    /// Cumulative unverified diagnostic hints; absent on legacy records with unknown coverage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_health: Option<crate::provider_health::ProviderHealthSnapshot>,
    /// Count of durable `WorkerProgress` events emitted to the binding event stream (`<= MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT`).
    pub durable_progress_events: u32,
    /// Count of additional live progress updates coalesced into this snapshot after reaching the durable event cap.
    pub coalesced_progress_events: u32,
    /// Count of live observer events dropped by the nonblocking worker channel during high-volume output.
    pub dropped_live_events: u32,
    /// Kind of the most recent normalized live worker event (`system_init`, `assistant_message`, `tool_use`, etc.).
    pub last_event_kind: Option<String>,
    /// Bounded, sanitized summary of the most recent live worker event.
    pub last_summary: Option<String>,
    /// Timestamp (ms) of the most recent live progress update.
    pub last_event_at_ms: Option<i64>,
    /// First validated software-supervision attention for this attempt. Later provider events
    /// cannot overwrite or clear it; this remains advisory, not a worker outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog_attention: Option<WorkerProgressWatchdogSnapshot>,
}

/// Compact, bounded terminal outcome evidence for a worker attempt.
///
/// Contains no raw stdout, raw prompts, argv, or credentials. No `accepted` boolean is ever set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerOutcomeSummary {
    /// Highest retry ordinal reported by unverified diagnostic text, distinct from record count.
    /// Absent means unknown, never zero dispatch attempts or proven healthy transport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_retry_high_water: Option<u32>,
    /// Cumulative unverified diagnostic hints; absent on legacy records with unknown coverage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_health: Option<crate::provider_health::ProviderHealthSnapshot>,
    /// Process exit code when the worker exited normally.
    pub exit_code: Option<i32>,
    /// Terminating signal number when the worker was killed by a signal.
    pub signal: Option<i32>,
    /// Total execution wall-clock duration in milliseconds.
    pub duration_ms: Option<u64>,
    /// Whether the worker exceeded its configured timeout budget.
    pub timed_out: bool,
    /// Whether the worker was terminated due to cancellation.
    pub cancelled: bool,
    /// Whether cancellation was requested before the worker finished (preserved even when natural completion wins a race).
    pub cancel_requested_before_completion: bool,
    /// Whether the provider failed to emit verifiable model telemetry matching the requested model.
    pub missing_model_evidence: bool,
    /// Whether the provider failed to emit verifiable reasoning effort telemetry.
    pub missing_effort_evidence: bool,
    /// Whether the provider emitted a structured error event or adapter failure.
    pub provider_error: bool,
    /// Whether descendant processes survived after the leader exited (`incomplete_cleanup`).
    pub incomplete_cleanup: bool,
    /// Whether the post-run Git diff gate rejected the task worktree changes.
    pub rejected_by_diff_gate: bool,
    /// Whether raw stdout exceeded the profile's byte budget and was truncated.
    pub stdout_truncated: bool,
    /// Whether raw stderr exceeded the profile's byte budget and was truncated.
    pub stderr_truncated: bool,
    /// Resulting review acceptance state (never automatically accepted).
    pub acceptance_state: WorkerAcceptanceState,
    /// Total number of changed paths detected in the task worktree.
    pub changed_paths_count: u32,
    /// Bounded preview of relative changed file paths (`<= MAX_WORKER_CHANGED_PATHS_PREVIEW`).
    pub changed_paths_preview: Vec<String>,
    /// Total number of diff-gate scope violations detected.
    pub violation_count: u32,
    /// Bounded list of sanitized diff-gate violation summaries (`<= MAX_WORKER_VIOLATION_SUMMARIES`).
    pub violation_summaries: Vec<String>,
    /// Bounded, sanitized final summary text from the worker (`<= MAX_WORKER_SUMMARY_TEXT_BYTES`).
    pub final_summary: Option<String>,
    /// Bounded, sanitized diagnostic or failure reason (`<= MAX_WORKER_SUMMARY_TEXT_BYTES`).
    pub failure_reason: Option<String>,
    /// Optional identifier of a private finite-size local report artifact stored outside MCP responses.
    pub report_artifact_id: Option<String>,
}

/// Compact durable snapshot of a worker execution record (`start_worker`, `get_worker`, `cancel_worker`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSnapshot {
    /// Explicit marker indicating that instruction and worker summary fields are untrusted data.
    #[serde(default = "default_untrusted_data")]
    pub untrusted_data: bool,
    /// Software-generated worker identifier (`wrk_<uuid>`).
    pub worker_id: String,
    /// Linked run identifier (`run_<uuid>`) in the binding's run/event scope.
    pub run_id: String,
    /// Owning binding ID (`bnd_<uuid>`).
    pub binding_id: String,
    /// Current 1-based revision of the worker run.
    pub revision: u64,
    /// Current worker lifecycle status.
    pub status: WorkerStatus,
    /// Trusted startup profile identifier selected for this worker.
    pub profile_id: String,
    /// Immutable SHA-256 hex digest of the complete trusted execution profile at admission time.
    pub profile_digest: String,
    /// Target provider identifier fixed by the trusted runtime profile.
    pub provider: String,
    /// Exact model requested by the trusted profile.
    pub requested_model: String,
    /// Exact reasoning effort requested by the trusted profile.
    pub requested_effort: String,
    /// Model identifier observed in provider telemetry, if reported.
    pub observed_model: Option<String>,
    /// Reasoning effort observed in provider telemetry, if reported.
    pub observed_effort: Option<String>,
    /// Bounded task instruction supplied when the worker was started.
    pub instruction: String,
    /// Active or most recent dispatcher claim identifier (`wclm_<uuid>`).
    pub claim_id: Option<String>,
    /// Number of dispatch claims attempted (at most `1`; never automatically retried).
    pub attempt_count: u32,
    /// Optional durable execution binding when started with a verified dependency input seal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_binding: Option<WorkerExecutionBinding>,
    /// Owned per-run and per-task worktree references.
    pub worktree: WorkerWorktreeSummary,
    /// Bounded live progress counters and latest summary.
    pub progress: WorkerProgressSummary,
    /// Compact terminal or unknown outcome summary once resolved.
    pub outcome: Option<WorkerOutcomeSummary>,
    /// Timestamp (ms) when the worker was admitted and queued.
    pub created_at_ms: i64,
    /// Timestamp (ms) when the worker record was last updated.
    pub updated_at_ms: i64,
    /// Timestamp (ms) when the worker transitioned to `Running`.
    pub started_at_ms: Option<i64>,
    /// Timestamp (ms) when the worker reached a terminal or `Unknown` state.
    pub completed_at_ms: Option<i64>,
    /// Timestamp (ms) when cancellation was requested, if any.
    pub cancel_requested_at_ms: Option<i64>,
    /// Optional bounded cancellation reason supplied by the caller.
    pub cancel_reason: Option<String>,
}

impl WorkerSnapshot {
    /// Whether a live invocation has a current unverified diagnostic advisory. Terminal
    /// history never supplies current-running attention or callback/replay authority.
    pub fn provider_health_attention_required(&self) -> bool {
        matches!(
            self.status,
            WorkerStatus::Running | WorkerStatus::CancelRequested
        ) && self.progress.provider_health_live_observed
            && self
                .progress
                .provider_health
                .as_ref()
                .is_some_and(|health| {
                    health.current_failure.is_some() && health.recovery.is_none()
                        && health.unavailable_reason != Some(crate::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed)
                })
    }
}

/// Persisted dispatcher claim binding a single execution attempt to `(binding_id, run_id, worker_id, revision, claim_id, profile_digest)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerClaim {
    /// Software-generated worker claim identifier (`wclm_<uuid>`).
    pub claim_id: String,
    /// Target worker identifier (`wrk_<uuid>`).
    pub worker_id: String,
    /// Target run identifier (`run_<uuid>`).
    pub run_id: String,
    /// Owning binding identifier (`bnd_<uuid>`).
    pub binding_id: String,
    /// Claimed run revision.
    pub revision: u64,
    /// Claimed 1-based attempt number.
    pub attempt_count: u32,
    /// Trusted profile identifier.
    pub profile_id: String,
    /// Immutable SHA-256 hex digest of the complete trusted execution profile.
    pub profile_digest: String,
    /// Provider name fixed by the profile.
    pub provider: String,
    /// Requested model fixed by the profile.
    pub requested_model: String,
    /// Requested effort fixed by the profile.
    pub requested_effort: String,
    /// Base commit fixed by the profile or pinned dependency seal.
    pub base_commit: String,
    /// Optional durable execution binding when started with a verified dependency input seal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_binding: Option<WorkerExecutionBinding>,
    /// Bounded task instruction.
    pub instruction: String,
    /// Timestamp (ms) when the claim was persisted.
    pub claimed_at_ms: i64,
}

/// Owned worktree metadata recorded when a claimed worker transitions to `Running`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerWorktreeUpdate {
    /// Owned per-run integration worktree identifier.
    pub run_worktree_id: String,
    /// Owned per-run integration worktree filesystem path.
    pub run_worktree_path: String,
    /// Branch name of the per-run integration worktree.
    pub run_branch_name: String,
    /// Owned per-task worktree identifier.
    pub task_worktree_id: String,
    /// Owned per-task worktree filesystem path.
    pub task_worktree_path: String,
    /// Branch name of the per-task worktree.
    pub task_branch_name: String,
    /// Resolved base commit hash in the worktrees.
    pub base_commit: String,
}

/// Single live progress item forwarded from the worker observer channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerProgressUpdate {
    /// Highest retry ordinal reported by unverified diagnostic text, distinct from record count.
    /// Absent means unknown, never zero dispatch attempts or proven healthy transport.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_retry_high_water: Option<u32>,
    /// Cumulative unverified diagnostic hints; absent on legacy records with unknown coverage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_health: Option<crate::provider_health::ProviderHealthSnapshot>,
    /// Normalized event kind label (`system_init`, `assistant_message`, `tool_use`, `tool_result`, etc.).
    pub event_kind: String,
    /// Sanitized summary text for the event.
    pub summary: String,
    /// Typed software-supervision attention, supplied only by the supervisor event mapper.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog_attention: Option<WorkerProgressWatchdogSnapshot>,
}

/// Terminal completion payload submitted by the dispatcher for an active [`WorkerClaim`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCompletionRecord {
    /// Proposed terminal worker status (`Completed`, `Failed`, `Cancelled`, `TimedOut`, or `Unknown`).
    pub status: WorkerStatus,
    /// Model observed in provider telemetry, if any.
    pub observed_model: Option<String>,
    /// Reasoning effort observed in provider telemetry, if any.
    pub observed_effort: Option<String>,
    /// Head commit hash in the task worktree after execution, if any.
    pub head_commit: Option<String>,
    /// Count of live events dropped by the bounded observer channel.
    pub dropped_live_events: u32,
    /// Compact outcome summary.
    pub outcome: WorkerOutcomeSummary,
}

pub(crate) fn validate_profile_id(raw: &str) -> Result<String, CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "profile_id",
            reason: "profile_id must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_WORKER_PROFILE_ID_LEN {
        return Err(CoreError::Validation {
            field: "profile_id",
            reason: format!(
                "profile_id exceeds maximum length of {MAX_WORKER_PROFILE_ID_LEN} bytes"
            ),
        });
    }
    if raw.starts_with('-')
        || !raw
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(CoreError::Validation {
            field: "profile_id",
            reason:
                "profile_id must contain only ASCII alphanumeric, '.', '_', or '-' characters and not start with '-'"
                    .to_string(),
        });
    }
    Ok(raw.to_string())
}

pub(crate) fn validate_profile_digest(raw: &str) -> Result<String, CoreError> {
    validate_sha256_digest("profile_digest", raw)
}

fn validate_sha256_digest(field: &'static str, raw: &str) -> Result<String, CoreError> {
    if raw.len() != MAX_WORKER_DIGEST_LEN
        || !raw
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(CoreError::Validation {
            field,
            reason: format!(
                "{field} must be a {MAX_WORKER_DIGEST_LEN}-character lowercase hex SHA-256 digest"
            ),
        });
    }
    Ok(raw.to_string())
}

fn validate_exact_40_hex_commit(field: &'static str, raw: &str) -> Result<String, CoreError> {
    if raw.len() != 40
        || !raw
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(CoreError::Validation {
            field,
            reason: format!("{field} must be an exact 40-character lowercase hexadecimal SHA"),
        });
    }
    Ok(raw.to_string())
}

fn sha256_hex_str(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

/// Sanitizes and bounds a compact diagnostic or progress string for durable storage and MCP output.
pub fn sanitize_worker_text(raw: &str, max_bytes: usize) -> String {
    let cleaned: String = raw
        .chars()
        .map(|ch| {
            if ch == '\n' || ch == '\r' || ch == '\t' {
                ' '
            } else {
                ch
            }
        })
        .filter(|ch| !ch.is_control())
        .collect();

    let collapsed = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");
    let redacted = redact_secret_tokens(&collapsed);

    if redacted.len() <= max_bytes {
        return redacted;
    }

    let suffix = "...";
    if max_bytes <= suffix.len() {
        return suffix[..max_bytes].to_string();
    }
    let target = max_bytes - suffix.len();
    let mut end = target;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{suffix}", &redacted[..end])
}

fn redact_secret_tokens(input: &str) -> String {
    let mut words = Vec::new();
    let mut redact_next = false;
    for token in input.split(' ') {
        if redact_next {
            words.push("[REDACTED]".to_string());
            redact_next = false;
            continue;
        }
        let lower = token.to_ascii_lowercase();
        if lower == "bearer" {
            words.push(token.to_string());
            redact_next = true;
            continue;
        }
        if lower.starts_with("sk-")
            || lower.starts_with("ghp_")
            || lower.starts_with("github_pat_")
            || lower.starts_with("xoxb-")
            || lower.starts_with("ai_key_")
        {
            words.push("[REDACTED]".to_string());
            continue;
        }
        if let Some((key, _val)) = token.split_once('=') {
            let key_lower = key.to_ascii_lowercase();
            if key_lower.contains("key")
                || key_lower.contains("token")
                || key_lower.contains("secret")
                || key_lower.contains("password")
                || key_lower.contains("credential")
                || key_lower.contains("auth")
            {
                words.push(format!("{key}=[REDACTED]"));
                continue;
            }
        }
        if let Some((key, _val)) = token.split_once(':') {
            let key_lower = key.to_ascii_lowercase();
            if key_lower.contains("key")
                || key_lower.contains("token")
                || key_lower.contains("secret")
                || key_lower.contains("password")
                || key_lower.contains("authorization")
            {
                words.push(format!("{key}:[REDACTED]"));
                continue;
            }
        }
        words.push(token.to_string());
    }
    words.join(" ")
}

fn sanitize_outcome(mut outcome: WorkerOutcomeSummary) -> WorkerOutcomeSummary {
    outcome.final_summary = outcome
        .final_summary
        .take()
        .map(|s| sanitize_worker_text(&s, MAX_WORKER_SUMMARY_TEXT_BYTES))
        .filter(|s| !s.is_empty());
    outcome.failure_reason = outcome
        .failure_reason
        .take()
        .map(|s| sanitize_worker_text(&s, MAX_WORKER_SUMMARY_TEXT_BYTES))
        .filter(|s| !s.is_empty());
    outcome.report_artifact_id = outcome
        .report_artifact_id
        .take()
        .map(|s| sanitize_worker_text(&s, MAX_ID_LEN))
        .filter(|s| !s.is_empty());
    outcome.changed_paths_preview = outcome
        .changed_paths_preview
        .into_iter()
        .take(MAX_WORKER_CHANGED_PATHS_PREVIEW)
        .map(|p| sanitize_worker_text(&p, MAX_WORKER_PATH_BYTES))
        .collect();
    outcome.violation_summaries = outcome
        .violation_summaries
        .into_iter()
        .take(MAX_WORKER_VIOLATION_SUMMARIES)
        .map(|v| sanitize_worker_text(&v, MAX_WORKER_SUMMARY_TEXT_BYTES))
        .collect();
    outcome
}

impl Core {
    /// Fetches a compact [`WorkerSnapshot`] scoped to `binding_id` by `run_id` or `worker_id`.
    pub fn get_worker(
        &self,
        binding_id: &str,
        run_or_worker_id: &str,
    ) -> Result<WorkerSnapshot, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_target_id = validate_identifier("run_id", run_or_worker_id)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        let snapshot = Self::load_worker_conn(&self.conn, &clean_binding_id, &clean_target_id)?;
        enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
        Ok(snapshot)
    }

    /// Fetches one exact [`WorkerSnapshot`] from an existing, checkpointed `state.db` without
    /// opening a mutating [`Core`], running migrations or recovery, creating directories or
    /// sidecar files, or writing any rows.
    ///
    /// The transaction requires exactly one stored session binding and validates its
    /// `binding_id` and `thread_uuid` before the exact `run_id`/`worker_id` lookup. This is a
    /// frozen stopped-session read path, not an alternative live-worker or cross-binding lookup.
    pub fn get_worker_readonly(
        database_path: impl AsRef<Path>,
        binding_id: &str,
        expected_thread_uuid: &str,
        run_or_worker_id: &str,
    ) -> Result<WorkerSnapshot, crate::observation::ReadonlySqliteError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_thread_uuid = validate_thread_uuid(expected_thread_uuid)?;
        let clean_target_id = validate_identifier("run_id", run_or_worker_id)?;

        crate::observation::with_existing_readonly_sqlite_tx(database_path.as_ref(), |tx| {
            // A stopped host session owns exactly one runtime binding. Count first so an
            // unexpected multi-binding database is rejected without materializing its rows.
            let binding_count: i64 =
                tx.query_row("SELECT COUNT(*) FROM bindings", [], |row| row.get(0))?;
            if binding_count != 1 {
                return Err(CoreError::CorruptState(format!(
                    "expected 1 binding in stopped session state.db, found {binding_count}"
                )));
            }

            let stored: Option<(String, String)> = tx
                .query_row(
                    "SELECT binding_id, thread_uuid FROM bindings LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((stored_binding_id, stored_thread_uuid)) = stored else {
                return Err(CoreError::CorruptState(
                    "stopped session state.db contains no binding".to_string(),
                ));
            };
            if stored_binding_id != clean_binding_id {
                return Err(CoreError::BindingNotFound {
                    binding_id: stored_binding_id,
                });
            }
            if stored_thread_uuid != clean_thread_uuid {
                return Err(CoreError::Validation {
                        field: "thread_uuid",
                        reason: format!(
                            "stored binding thread_uuid `{stored_thread_uuid}` does not match expected `{clean_thread_uuid}`"
                        ),
                    });
            }
            let snapshot = Self::load_worker_conn(tx, &clean_binding_id, &clean_target_id)?;
            enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
            Ok(snapshot)
        })
    }

    /// Lists up to `limit` queued worker snapshots across all bindings in FIFO admission order.
    pub fn list_queued_workers(&self, limit: usize) -> Result<Vec<WorkerSnapshot>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut stmt = self.conn.prepare(
            "SELECT binding_id, run_id
             FROM workers
             WHERE status = 'queued'
             ORDER BY rowid ASC
             LIMIT ?1",
        )?;
        let mut rows = stmt.query(params![u64_to_i64("limit", limit as u64)?])?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            ids.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
        }
        let mut snapshots = Vec::with_capacity(ids.len());
        for (binding_id, run_id) in ids {
            snapshots.push(Self::load_worker_conn(&self.conn, &binding_id, &run_id)?);
        }
        Ok(snapshots)
    }

    /// Atomically claims a `Queued` worker for dispatch after verifying `(binding_id, run_id, expected_revision, expected_profile_digest)`
    /// and that `execution_binding` is `None`.
    ///
    /// Persists `WorkerStatus::Claimed`, increments `attempt_count`, and records a fresh `claim_id`
    /// before the caller performs any worktree or process side effects.
    pub fn claim_worker(
        &mut self,
        binding_id: &str,
        run_id: &str,
        expected_revision: u64,
        expected_profile_digest: &str,
        now_ms: i64,
    ) -> Result<WorkerClaim, CoreError> {
        self.claim_worker_with_execution_binding(
            binding_id,
            run_id,
            expected_revision,
            expected_profile_digest,
            None,
            now_ms,
        )
    }

    /// Atomically claims a `Queued` worker for dispatch after verifying `(binding_id, run_id, expected_revision, expected_profile_digest)`
    /// and `expected_execution_binding`.
    ///
    /// Persists `WorkerStatus::Claimed`, increments `attempt_count`, and records a fresh `claim_id`
    /// before the caller performs any worktree or process side effects.
    pub fn claim_worker_with_execution_binding(
        &mut self,
        binding_id: &str,
        run_id: &str,
        expected_revision: u64,
        expected_profile_digest: &str,
        expected_execution_binding: Option<&WorkerExecutionBinding>,
        now_ms: i64,
    ) -> Result<WorkerClaim, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_run_id = validate_identifier("run_id", run_id)?;
        let clean_digest = validate_profile_digest(expected_profile_digest)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;

        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        let worker = Self::load_worker_tx(&tx, &clean_binding_id, &clean_run_id)?;

        if worker.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: worker.run_id,
                expected_revision,
                actual_revision: worker.revision,
            });
        }

        if worker.profile_digest != clean_digest {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: clean_binding_id,
                run_id: worker.run_id,
                worker_id: worker.worker_id,
                claim_id: worker.claim_id.unwrap_or_else(|| "unclaimed".to_string()),
                reason: format!(
                    "profile_digest mismatch: queued with `{}`, attempted claim with `{clean_digest}`",
                    worker.profile_digest
                ),
            });
        }

        if worker.execution_binding.as_ref() != expected_execution_binding {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: clean_binding_id,
                run_id: worker.run_id,
                worker_id: worker.worker_id,
                claim_id: worker.claim_id.unwrap_or_else(|| "unclaimed".to_string()),
                reason: "execution_binding mismatch between queued worker and claim request"
                    .to_string(),
            });
        }

        if let Some(ref binding) = worker.execution_binding {
            binding
                .verify_against(
                    &worker.profile_digest,
                    &worker.instruction,
                    &worker.worktree.base_commit,
                )
                .map_err(|err| CoreError::StaleWorkerClaim {
                    binding_id: clean_binding_id.clone(),
                    run_id: worker.run_id.clone(),
                    worker_id: worker.worker_id.clone(),
                    claim_id: worker
                        .claim_id
                        .clone()
                        .unwrap_or_else(|| "unclaimed".to_string()),
                    reason: format!("stored execution_binding verification failed: {err}"),
                })?;
        }

        if worker.status != WorkerStatus::Queued {
            return Err(CoreError::InvalidRunState {
                run_id: worker.run_id,
                status: worker.status.to_run_status(),
                action: "claim_worker",
            });
        }

        let claim_id = generate_id("wclm");
        let next_attempt = worker.attempt_count.saturating_add(1);

        tx.execute(
            "UPDATE workers
             SET status = ?1,
                 claim_id = ?2,
                 attempt_count = ?3,
                 updated_at_ms = ?4
             WHERE worker_id = ?5 AND binding_id = ?6",
            params![
                WorkerStatus::Claimed.as_str(),
                claim_id,
                i64::from(next_attempt),
                valid_now,
                worker.worker_id,
                clean_binding_id
            ],
        )?;

        tx.execute(
            "UPDATE runs
             SET status = ?1,
                 updated_at_ms = ?2
             WHERE run_id = ?3 AND binding_id = ?4",
            params![
                RunStatus::Claimed.as_str(),
                valid_now,
                worker.run_id,
                clean_binding_id
            ],
        )?;

        tx.commit()?;

        Ok(WorkerClaim {
            claim_id,
            worker_id: worker.worker_id,
            run_id: worker.run_id,
            binding_id: clean_binding_id,
            revision: worker.revision,
            attempt_count: next_attempt,
            profile_id: worker.profile_id,
            profile_digest: worker.profile_digest,
            provider: worker.provider,
            requested_model: worker.requested_model,
            requested_effort: worker.requested_effort,
            base_commit: worker.worktree.base_commit,
            execution_binding: worker.execution_binding,
            instruction: worker.instruction,
            claimed_at_ms: valid_now,
        })
    }

    /// Rejects an unclaimed `Queued` worker (`attempt_count == 0`, `claim_id == None`) before dispatch,
    /// transitioning both `workers` and `runs` to `Failed` (`WorkerAcceptanceState::ExecutionFailed`),
    /// emitting an important `EventKind::WorkerFailed` event, and coalescing a binding wake-up notice.
    pub fn reject_queued_worker(
        &mut self,
        binding_id: &str,
        run_id: &str,
        expected_revision: u64,
        expected_profile_digest: &str,
        failure_reason: &str,
        now_ms: i64,
    ) -> Result<WorkerSnapshot, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_run_id = validate_identifier("run_id", run_id)?;
        let clean_digest = validate_profile_digest(expected_profile_digest)?;
        if failure_reason.trim().is_empty() {
            return Err(CoreError::Validation {
                field: "failure_reason",
                reason: "failure_reason must not be empty".to_string(),
            });
        }
        let clean_reason = sanitize_worker_text(failure_reason, MAX_WORKER_SUMMARY_TEXT_BYTES);
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;

        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        let worker = Self::load_worker_tx(&tx, &clean_binding_id, &clean_run_id)?;

        if worker.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: worker.run_id,
                expected_revision,
                actual_revision: worker.revision,
            });
        }

        if worker.profile_digest != clean_digest {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: clean_binding_id,
                run_id: worker.run_id,
                worker_id: worker.worker_id,
                claim_id: worker.claim_id.unwrap_or_else(|| "unclaimed".to_string()),
                reason: format!(
                    "profile_digest mismatch: queued with `{}`, attempted rejection with `{clean_digest}`",
                    worker.profile_digest
                ),
            });
        }

        if worker.status != WorkerStatus::Queued
            || worker.attempt_count != 0
            || worker.claim_id.is_some()
        {
            return Err(CoreError::InvalidRunState {
                run_id: worker.run_id,
                status: worker.status.to_run_status(),
                action: "reject_queued_worker",
            });
        }

        let outcome = WorkerOutcomeSummary {
            provider_retry_high_water: None,
            provider_health: None,
            exit_code: None,
            signal: None,
            duration_ms: Some(0),
            timed_out: false,
            cancelled: false,
            cancel_requested_before_completion: false,
            missing_model_evidence: false,
            missing_effort_evidence: false,
            provider_error: false,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: WorkerAcceptanceState::ExecutionFailed,
            changed_paths_count: 0,
            changed_paths_preview: Vec::new(),
            violation_count: 0,
            violation_summaries: Vec::new(),
            final_summary: None,
            failure_reason: Some(clean_reason.clone()),
            report_artifact_id: None,
        };
        let outcome_json = serde_json::to_string(&outcome)?;

        tx.execute(
            "UPDATE workers
             SET status = ?1,
                 outcome_json = ?2,
                 completed_at_ms = ?3,
                 updated_at_ms = ?3
             WHERE worker_id = ?4 AND binding_id = ?5",
            params![
                WorkerStatus::Failed.as_str(),
                outcome_json,
                valid_now,
                worker.worker_id,
                clean_binding_id
            ],
        )?;

        let next_step_seq = Self::increment_run_step_tx(
            &tx,
            &clean_binding_id,
            &worker.run_id,
            RunStatus::Failed,
            valid_now,
        )?;

        let summary = sanitize_worker_text(
            &format!(
                "Worker {} on run {} rejected before dispatch: {}",
                worker.worker_id, worker.run_id, clean_reason
            ),
            MAX_WORKER_SUMMARY_TEXT_BYTES,
        );

        Self::emit_event_tx(
            &tx,
            &clean_binding_id,
            &worker.run_id,
            worker.revision,
            next_step_seq,
            EventKind::WorkerFailed,
            EventPayload {
                untrusted_data: true,
                summary,
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: None,
                cancel_reason: None,
            },
            valid_now,
        )?;

        let snapshot = Self::load_worker_tx(&tx, &clean_binding_id, &worker.run_id)?;
        enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
        tx.commit()?;
        Ok(snapshot)
    }

    /// Records that a claimed worker has created its owned worktrees and started execution.
    ///
    /// Rejects stale or mismatched claims without mutating state. If cancellation was already
    /// requested while `Claimed`, preserves `WorkerStatus::CancelRequested` while recording the
    /// owned worktree references.
    pub fn record_worker_running(
        &mut self,
        claim: &WorkerClaim,
        worktree_update: WorkerWorktreeUpdate,
        now_ms: i64,
    ) -> Result<WorkerSnapshot, CoreError> {
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;
        let worker = Self::verify_active_claim_tx(&tx, claim)?;

        let clean_update_base = sanitize_worker_text(&worktree_update.base_commit, MAX_ID_LEN * 2);
        if clean_update_base != claim.base_commit
            || clean_update_base != worker.worktree.base_commit
        {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "worktree base_commit `{clean_update_base}` does not match claimed base_commit `{}`",
                    claim.base_commit
                ),
            });
        }

        let worktree = WorkerWorktreeSummary {
            run_worktree_id: Some(sanitize_worker_text(
                &worktree_update.run_worktree_id,
                MAX_ID_LEN * 2,
            )),
            run_worktree_path: Some(sanitize_worker_text(
                &worktree_update.run_worktree_path,
                MAX_WORKER_PATH_BYTES,
            )),
            run_branch_name: Some(sanitize_worker_text(
                &worktree_update.run_branch_name,
                MAX_WORKER_PATH_BYTES,
            )),
            task_worktree_id: Some(sanitize_worker_text(
                &worktree_update.task_worktree_id,
                MAX_ID_LEN * 2,
            )),
            task_worktree_path: Some(sanitize_worker_text(
                &worktree_update.task_worktree_path,
                MAX_WORKER_PATH_BYTES,
            )),
            task_branch_name: Some(sanitize_worker_text(
                &worktree_update.task_branch_name,
                MAX_WORKER_PATH_BYTES,
            )),
            base_commit: clean_update_base,
            head_commit: worker.worktree.head_commit,
        };
        let worktree_json = serde_json::to_string(&worktree)?;

        let next_status = if worker.status == WorkerStatus::CancelRequested {
            WorkerStatus::CancelRequested
        } else {
            WorkerStatus::Running
        };
        let started_at_ms = worker.started_at_ms.unwrap_or(valid_now);

        tx.execute(
            "UPDATE workers
             SET status = ?1,
                 worktree_json = ?2,
                 started_at_ms = ?3,
                 updated_at_ms = ?4
             WHERE worker_id = ?5 AND binding_id = ?6",
            params![
                next_status.as_str(),
                worktree_json,
                started_at_ms,
                valid_now,
                worker.worker_id,
                claim.binding_id
            ],
        )?;

        let next_step_seq = Self::increment_run_step_tx(
            &tx,
            &claim.binding_id,
            &claim.run_id,
            next_status.to_run_status(),
            valid_now,
        )?;

        if worker.status == WorkerStatus::Claimed {
            Self::emit_event_tx(
                &tx,
                &claim.binding_id,
                &claim.run_id,
                claim.revision,
                next_step_seq,
                EventKind::WorkerStarted,
                EventPayload {
                    untrusted_data: true,
                    summary: format!(
                        "Worker {} started on profile {} (run {}, revision {})",
                        worker.worker_id, worker.profile_id, worker.run_id, worker.revision
                    ),
                    question_id: None,
                    question_prompt: None,
                    answer: None,
                    instruction: None,
                    cancel_reason: None,
                },
                valid_now,
            )?;
        }

        let snapshot = Self::load_worker_tx(&tx, &claim.binding_id, &claim.run_id)?;
        enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
        tx.commit()?;
        Ok(snapshot)
    }

    /// Records a batch of live, bounded, sanitized worker progress updates for an active [`WorkerClaim`].
    ///
    /// Caps durable `EventKind::WorkerProgress` rows in `events` at
    /// [`MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT`] per attempt; additional live updates increment
    /// `coalesced_progress_events` and update the latest summary without growing the `events` table.
    pub fn record_worker_progress(
        &mut self,
        claim: &WorkerClaim,
        updates: &[WorkerProgressUpdate],
        now_ms: i64,
    ) -> Result<WorkerSnapshot, CoreError> {
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;
        let worker = Self::verify_active_claim_tx(&tx, claim)?;

        if updates.is_empty() {
            tx.commit()?;
            return Ok(worker);
        }

        let mut progress = worker.progress;
        let has_ordinary_progress = updates.iter().any(|item| item.provider_health.is_none());
        let next_status = if worker.status == WorkerStatus::Claimed && has_ordinary_progress {
            WorkerStatus::Running
        } else {
            worker.status
        };
        let started_at_ms = worker
            .started_at_ms
            .or_else(|| has_ordinary_progress.then_some(valid_now));

        for item in updates {
            if let Some(health) = &item.provider_health {
                if item.event_kind != "provider_diagnostic_hint"
                    || item.watchdog_attention.is_some()
                    || (health.recovery == Some(crate::provider_health::ProviderHealthRecovery::VerifiedTerminalOutcome)
                        && health.unavailable_reason != Some(crate::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed))
                {
                    return Err(CoreError::Validation {
                        field: "provider_health",
                        reason: "diagnostics require the separate live snapshot channel"
                            .to_string(),
                    });
                }
                if merge_provider_health(
                    &mut progress.provider_health, &mut progress.provider_retry_high_water,
                    health, item.provider_retry_high_water,
                )?
                    && health.current_failure.is_some()
                    && health.unavailable_reason != Some(crate::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed)
                {
                    progress.provider_health_live_observed = true;
                }
                // Retain no caller text; spend no ordinary event capacity or lifecycle authority.
                continue;
            }
            if item.provider_retry_high_water.is_some() {
                return Err(CoreError::Validation {
                    field: "provider_retry_high_water",
                    reason: "retry ordinals require diagnostic snapshot metadata".to_owned(),
                });
            }
            if let Some(attention) = &item.watchdog_attention {
                attention.validate_attention()?;
                if item.event_kind != "supervisor_attention" {
                    return Err(CoreError::Validation {
                        field: "watchdog_attention",
                        reason: "software-supervision metadata requires a typed supervisor event"
                            .to_string(),
                    });
                }
                if progress.watchdog_attention.is_none() {
                    progress.watchdog_attention = Some(attention.clone());
                }
            }
            let clean_kind = sanitize_worker_text(&item.event_kind, MAX_ID_LEN);
            let clean_summary = sanitize_worker_text(&item.summary, MAX_WORKER_SUMMARY_TEXT_BYTES);
            if clean_summary.is_empty() {
                continue;
            }

            progress.last_event_kind = Some(clean_kind.clone());
            progress.last_summary = Some(clean_summary.clone());
            progress.last_event_at_ms = Some(valid_now);

            if progress.durable_progress_events < MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT {
                progress.durable_progress_events += 1;
                let step_seq = Self::increment_run_step_tx(
                    &tx,
                    &claim.binding_id,
                    &claim.run_id,
                    next_status.to_run_status(),
                    valid_now,
                )?;
                let event_summary = sanitize_worker_text(
                    &format!(
                        "Worker {} [{}]: {}",
                        worker.worker_id, clean_kind, clean_summary
                    ),
                    MAX_WORKER_SUMMARY_TEXT_BYTES,
                );
                Self::emit_event_tx(
                    &tx,
                    &claim.binding_id,
                    &claim.run_id,
                    claim.revision,
                    step_seq,
                    EventKind::WorkerProgress,
                    EventPayload {
                        untrusted_data: true,
                        summary: event_summary,
                        question_id: None,
                        question_prompt: None,
                        answer: None,
                        instruction: None,
                        cancel_reason: None,
                    },
                    valid_now,
                )?;
            } else {
                progress.coalesced_progress_events =
                    progress.coalesced_progress_events.saturating_add(1);
            }
        }

        let progress_json = serde_json::to_string(&progress)?;
        tx.execute(
            "UPDATE workers
             SET status = ?1,
                 progress_json = ?2,
                 started_at_ms = ?3,
                 updated_at_ms = ?4
             WHERE worker_id = ?5 AND binding_id = ?6",
            params![
                next_status.as_str(),
                progress_json,
                started_at_ms,
                valid_now,
                worker.worker_id,
                claim.binding_id
            ],
        )?;

        tx.execute(
            "UPDATE runs
             SET status = ?1,
                 updated_at_ms = ?2
             WHERE run_id = ?3 AND binding_id = ?4",
            params![
                next_status.to_run_status().as_str(),
                valid_now,
                claim.run_id,
                claim.binding_id
            ],
        )?;

        let snapshot = Self::load_worker_tx(&tx, &claim.binding_id, &claim.run_id)?;
        enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
        tx.commit()?;
        Ok(snapshot)
    }

    /// Atomically records the terminal outcome of an active [`WorkerClaim`].
    ///
    /// - Rejects any callback whose `(binding_id, run_id, worker_id, revision, claim_id, attempt_count, profile_digest)`
    ///   does not match the live non-terminal worker state.
    /// - Preserves genuine completion status and stop-request evidence (`cancel_requested_before_completion`,
    ///   `cancel_requested_at_ms`, `cancel_reason`) when natural completion races with cancellation.
    /// - Emits the corresponding `important = true` terminal worker event and schedules/coalesces a
    ///   binding wake-up notice.
    pub fn record_worker_outcome(
        &mut self,
        claim: &WorkerClaim,
        completion: WorkerCompletionRecord,
        now_ms: i64,
    ) -> Result<WorkerSnapshot, CoreError> {
        if !completion.status.is_terminal() {
            return Err(CoreError::Validation {
                field: "status",
                reason: format!(
                    "worker completion status `{}` must be terminal",
                    completion.status.as_str()
                ),
            });
        }
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;
        let worker = Self::verify_active_claim_tx(&tx, claim)?;

        let cancel_was_requested = worker.status == WorkerStatus::CancelRequested
            || worker.cancel_requested_at_ms.is_some();

        let mut outcome = sanitize_outcome(completion.outcome);
        outcome.cancel_requested_before_completion = cancel_was_requested;

        let final_status = if outcome.cancelled || completion.status == WorkerStatus::Cancelled {
            outcome.cancelled = true;
            WorkerStatus::Cancelled
        } else {
            completion.status
        };

        let mut worktree = worker.worktree;
        if let Some(head) = completion.head_commit {
            let clean_head = sanitize_worker_text(&head, MAX_ID_LEN * 2);
            if !clean_head.is_empty() {
                worktree.head_commit = Some(clean_head);
            }
        }
        let worktree_json = serde_json::to_string(&worktree)?;

        let mut progress = worker.progress;
        if let Some(health) = &outcome.provider_health {
            // Final evidence completes history even when all live receivers were closed.
            // Reject contradictory diagnostic metadata without allowing it to prevent the
            // independently established terminal outcome or manufacture a new live notice.
            let duration_consistent = outcome
                .duration_ms
                .is_none_or(|duration| health.elapsed_ms <= duration);
            let recovery_consistent = health.recovery
                != Some(crate::provider_health::ProviderHealthRecovery::VerifiedTerminalOutcome)
                || (outcome.exit_code == Some(0)
                    && outcome.signal.is_none()
                    && !outcome.timed_out
                    && !outcome.cancelled
                    && !outcome.provider_error
                    && !outcome.incomplete_cleanup
                    && !outcome.stdout_truncated
                    && !outcome.stderr_truncated
                    && !outcome.missing_model_evidence
                    && !outcome.missing_effort_evidence);
            if duration_consistent && recovery_consistent {
                match merge_provider_health(
                    &mut progress.provider_health,
                    &mut progress.provider_retry_high_water,
                    health,
                    outcome.provider_retry_high_water,
                ) {
                    Ok(_) | Err(CoreError::Validation { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        outcome.provider_health = progress.provider_health.clone();
        outcome.provider_retry_high_water = progress.provider_retry_high_water;
        progress.dropped_live_events = progress
            .dropped_live_events
            .saturating_add(completion.dropped_live_events);
        let progress_json = serde_json::to_string(&progress)?;
        let outcome_json = serde_json::to_string(&outcome)?;

        let clean_observed_model = completion
            .observed_model
            .map(|m| sanitize_worker_text(&m, MAX_ID_LEN * 2))
            .filter(|m| !m.is_empty());
        let clean_observed_effort = completion
            .observed_effort
            .map(|e| sanitize_worker_text(&e, MAX_ID_LEN))
            .filter(|e| !e.is_empty());

        tx.execute(
            "UPDATE workers
             SET status = ?1,
                 observed_model = ?2,
                 observed_effort = ?3,
                 worktree_json = ?4,
                 progress_json = ?5,
                 outcome_json = ?6,
                 completed_at_ms = ?7,
                 updated_at_ms = ?7
             WHERE worker_id = ?8 AND binding_id = ?9",
            params![
                final_status.as_str(),
                clean_observed_model,
                clean_observed_effort,
                worktree_json,
                progress_json,
                outcome_json,
                valid_now,
                worker.worker_id,
                claim.binding_id
            ],
        )?;

        let run_status = final_status.to_run_status();
        let next_step_seq = Self::increment_run_step_tx(
            &tx,
            &claim.binding_id,
            &claim.run_id,
            run_status,
            valid_now,
        )?;

        if final_status == WorkerStatus::Cancelled {
            tx.execute(
                "UPDATE runs
                 SET cancelled_at_ms = ?1,
                     cancel_reason = COALESCE(cancel_reason, ?2)
                 WHERE run_id = ?3 AND binding_id = ?4",
                params![
                    valid_now,
                    worker.cancel_reason,
                    claim.run_id,
                    claim.binding_id
                ],
            )?;
        } else if final_status == WorkerStatus::Completed {
            tx.execute(
                "UPDATE runs
                 SET completed_at_ms = ?1
                 WHERE run_id = ?2 AND binding_id = ?3",
                params![valid_now, claim.run_id, claim.binding_id],
            )?;
        }

        let (event_kind, summary) = match final_status {
            WorkerStatus::Completed => (
                EventKind::WorkerCompleted,
                format!(
                    "Worker {} on run {} completed (ready_for_review)",
                    worker.worker_id, worker.run_id
                ),
            ),
            WorkerStatus::Failed => (
                EventKind::WorkerFailed,
                format!(
                    "Worker {} on run {} failed ({:?})",
                    worker.worker_id, worker.run_id, outcome.acceptance_state
                ),
            ),
            WorkerStatus::Cancelled => (
                EventKind::WorkerCancelled,
                format!(
                    "Worker {} on run {} cancelled",
                    worker.worker_id, worker.run_id
                ),
            ),
            WorkerStatus::TimedOut => (
                EventKind::WorkerTimedOut,
                format!(
                    "Worker {} on run {} timed out",
                    worker.worker_id, worker.run_id
                ),
            ),
            WorkerStatus::Unknown => (
                EventKind::WorkerUnknown,
                format!(
                    "Worker {} on run {} ended in unknown state",
                    worker.worker_id, worker.run_id
                ),
            ),
            _ => unreachable!("checked terminal status above"),
        };

        Self::emit_event_tx(
            &tx,
            &claim.binding_id,
            &claim.run_id,
            claim.revision,
            next_step_seq,
            event_kind,
            EventPayload {
                untrusted_data: true,
                summary: sanitize_worker_text(&summary, MAX_WORKER_SUMMARY_TEXT_BYTES),
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: None,
                cancel_reason: if final_status == WorkerStatus::Cancelled {
                    worker.cancel_reason.clone()
                } else {
                    None
                },
            },
            valid_now,
        )?;

        let snapshot = Self::load_worker_tx(&tx, &claim.binding_id, &claim.run_id)?;
        enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone())))?;
        tx.commit()?;
        Ok(snapshot)
    }

    /// Transitions any non-terminal worker records (`Queued`, `Claimed`, `Running`, `CancelRequested`)
    /// to `WorkerStatus::Unknown` with a bounded important event and wake-up notice, without
    /// automatic retry.
    pub fn mark_active_workers_unknown(
        &mut self,
        now_ms: i64,
        reason: &str,
    ) -> Result<usize, CoreError> {
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let clean_reason = sanitize_worker_text(reason, MAX_WORKER_SUMMARY_TEXT_BYTES);
        let tx = self.conn.transaction()?;
        let count = Self::recover_in_flight_workers_with_reason_tx(&tx, valid_now, &clean_reason)?;
        tx.commit()?;
        Ok(count)
    }

    pub(crate) fn recover_in_flight_workers_tx(tx: &Transaction<'_>) -> Result<usize, CoreError> {
        Self::recover_in_flight_workers_with_reason_tx(
            tx,
            0,
            "worker was in flight when database reopened; marked unknown without automatic retry",
        )
    }

    fn recover_in_flight_workers_with_reason_tx(
        tx: &Transaction<'_>,
        now_ms: i64,
        reason: &str,
    ) -> Result<usize, CoreError> {
        let in_flight: Vec<(String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT binding_id, run_id
                 FROM workers
                 WHERE status IN ('queued', 'claimed', 'running', 'cancel_requested')
                 ORDER BY rowid ASC",
            )?;
            let mut rows = stmt.query([])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                items.push((row.get(0)?, row.get(1)?));
            }
            items
        };

        let mut recovered = 0usize;
        for (binding_id, run_id) in in_flight {
            let worker = Self::load_worker_tx(tx, &binding_id, &run_id)?;
            let resolved_at_ms = now_ms.max(worker.updated_at_ms);
            let prior_status = worker.status.as_str();
            let cancel_was_requested = worker.status == WorkerStatus::CancelRequested
                || worker.cancel_requested_at_ms.is_some();

            let outcome = WorkerOutcomeSummary {
                provider_retry_high_water: None,
                provider_health: None,
                exit_code: None,
                signal: None,
                duration_ms: None,
                timed_out: false,
                cancelled: false,
                cancel_requested_before_completion: cancel_was_requested,
                missing_model_evidence: false,
                missing_effort_evidence: false,
                provider_error: false,
                incomplete_cleanup: false,
                rejected_by_diff_gate: false,
                stdout_truncated: false,
                stderr_truncated: false,
                acceptance_state: WorkerAcceptanceState::Unknown,
                changed_paths_count: 0,
                changed_paths_preview: Vec::new(),
                violation_count: 0,
                violation_summaries: Vec::new(),
                final_summary: None,
                failure_reason: Some(sanitize_worker_text(
                    &format!("prior status `{prior_status}`: {reason}"),
                    MAX_WORKER_SUMMARY_TEXT_BYTES,
                )),
                report_artifact_id: None,
            };
            let outcome_json = serde_json::to_string(&outcome)?;

            tx.execute(
                "UPDATE workers
                 SET status = ?1,
                     outcome_json = ?2,
                     completed_at_ms = ?3,
                     updated_at_ms = ?3
                 WHERE worker_id = ?4 AND binding_id = ?5",
                params![
                    WorkerStatus::Unknown.as_str(),
                    outcome_json,
                    resolved_at_ms,
                    worker.worker_id,
                    binding_id
                ],
            )?;

            let step_seq = Self::increment_run_step_tx(
                tx,
                &binding_id,
                &run_id,
                RunStatus::Unknown,
                resolved_at_ms,
            )?;

            Self::emit_event_tx(
                tx,
                &binding_id,
                &run_id,
                worker.revision,
                step_seq,
                EventKind::WorkerUnknown,
                EventPayload {
                    untrusted_data: true,
                    summary: sanitize_worker_text(
                        &format!(
                            "Worker {} on run {} marked unknown from `{prior_status}` without automatic retry",
                            worker.worker_id, run_id
                        ),
                        MAX_WORKER_SUMMARY_TEXT_BYTES,
                    ),
                    question_id: None,
                    question_prompt: None,
                    answer: None,
                    instruction: None,
                    cancel_reason: None,
                },
                resolved_at_ms,
            )?;

            recovered += 1;
        }

        Ok(recovered)
    }

    pub(crate) fn is_worker_run_tx(tx: &Transaction<'_>, run_id: &str) -> Result<bool, CoreError> {
        let exists: Option<i64> = tx
            .query_row(
                "SELECT 1 FROM workers WHERE run_id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(exists.is_some())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_start_worker(
        &mut self,
        binding_id: &str,
        command_key: String,
        profile_id: String,
        profile_digest: String,
        provider: String,
        requested_model: String,
        requested_effort: String,
        base_commit: String,
        execution_binding: Option<WorkerExecutionBinding>,
        instruction: String,
        now_ms: i64,
    ) -> Result<CommandOutput, CoreError> {
        let clean_key = validate_command_key(&command_key)?;
        let clean_profile_id = validate_profile_id(&profile_id)?;
        let clean_digest = validate_profile_digest(&profile_digest)?;
        let clean_provider = validate_bounded_text("provider", &provider, MAX_ID_LEN)?;
        let clean_model =
            validate_bounded_text("requested_model", &requested_model, MAX_ID_LEN * 2)?;
        let clean_effort =
            validate_bounded_text("requested_effort", &requested_effort, MAX_ID_LEN)?;
        let clean_base = validate_bounded_text("base_commit", &base_commit, MAX_ID_LEN * 2)?;
        let clean_instruction =
            validate_bounded_text("instruction", &instruction, MAX_INSTRUCTION_LEN)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;

        if let Some(ref binding) = execution_binding {
            binding.verify_against(&clean_digest, &clean_instruction, &clean_base)?;
        }

        // Include `clean_profile_id`, `clean_digest`, `clean_instruction` (and `execution_binding` when present)
        // in the dedup hash so:
        // - Identical retries with the same profile digest and execution binding return the original snapshot regardless of queue load.
        // - Reusing the same `command_key` after a profile's configuration digest or sealed execution binding changed is rejected with `IdempotencyConflict`.
        let payload_hash = match execution_binding {
            Some(ref binding) => hash_dedup_payload(&(
                "start_worker",
                &clean_profile_id,
                &clean_digest,
                &clean_instruction,
                binding,
            ))?,
            None => hash_dedup_payload(&(
                "start_worker",
                &clean_profile_id,
                &clean_digest,
                &clean_instruction,
            ))?,
        };

        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, binding_id)?;
        if let Some(existing) =
            Self::check_dedup(&tx, binding_id, &clean_key, "start_worker", &payload_hash)?
        {
            return Ok(existing);
        }

        let run_id = generate_id("run");
        let worker_id = generate_id("wrk");
        let revision = 1u64;
        let step_seq = 1u64;

        tx.execute(
            "INSERT INTO runs (
                run_id, binding_id, revision, status, instruction, step_seq,
                step_delay_ms, next_step_at_ms, active_question_id, started_at_ms,
                updated_at_ms, completed_at_ms, cancelled_at_ms, cancel_reason
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, NULL, NULL, ?7, ?7, NULL, NULL, NULL)",
            params![
                run_id,
                binding_id,
                u64_to_i64("revision", revision)?,
                RunStatus::Queued.as_str(),
                clean_instruction,
                u64_to_i64("step_seq", step_seq)?,
                valid_now
            ],
        )?;

        tx.execute(
            "INSERT INTO run_revisions (run_id, revision, instruction, created_at_ms, superseded_at_ms)
             VALUES (?1, ?2, ?3, ?4, NULL)",
            params![
                run_id,
                u64_to_i64("revision", revision)?,
                clean_instruction,
                valid_now
            ],
        )?;

        let worktree = WorkerWorktreeSummary {
            run_worktree_id: None,
            run_worktree_path: None,
            run_branch_name: None,
            task_worktree_id: None,
            task_worktree_path: None,
            task_branch_name: None,
            base_commit: clean_base,
            head_commit: None,
        };
        let progress = WorkerProgressSummary::default();
        let worktree_json = serde_json::to_string(&worktree)?;
        let progress_json = serde_json::to_string(&progress)?;
        let execution_binding_json = match execution_binding {
            Some(ref binding) => Some(serde_json::to_string(binding)?),
            None => None,
        };

        tx.execute(
            "INSERT INTO workers (
                worker_id, run_id, binding_id, revision, status, profile_id,
                profile_digest, provider, requested_model, requested_effort,
                observed_model, observed_effort, instruction, claim_id, attempt_count,
                execution_binding_json, worktree_json, progress_json, outcome_json,
                created_at_ms, updated_at_ms, started_at_ms, completed_at_ms,
                cancel_requested_at_ms, cancel_reason
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9, ?10,
                NULL, NULL, ?11, NULL, 0,
                ?12, ?13, ?14, NULL,
                ?15, ?15, NULL, NULL,
                NULL, NULL
             )",
            params![
                worker_id,
                run_id,
                binding_id,
                u64_to_i64("revision", revision)?,
                WorkerStatus::Queued.as_str(),
                clean_profile_id,
                clean_digest,
                clean_provider,
                clean_model,
                clean_effort,
                clean_instruction,
                execution_binding_json,
                worktree_json,
                progress_json,
                valid_now
            ],
        )?;

        Self::emit_event_tx(
            &tx,
            binding_id,
            &run_id,
            revision,
            step_seq,
            EventKind::WorkerQueued,
            EventPayload {
                untrusted_data: true,
                summary: format!(
                    "Worker {worker_id} queued on profile {clean_profile_id} (run {run_id}, revision 1)"
                ),
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: None,
                cancel_reason: None,
            },
            valid_now,
        )?;

        let snapshot = Self::load_worker_tx(&tx, binding_id, &run_id)?;
        let output = CommandOutput::Worker(Box::new(snapshot));
        Self::record_dedup(
            &tx,
            binding_id,
            &clean_key,
            "start_worker",
            &payload_hash,
            &output,
            valid_now,
        )?;
        tx.commit()?;
        Ok(output)
    }

    pub(crate) fn execute_get_worker(
        &mut self,
        binding_id: &str,
        run_id: String,
    ) -> Result<CommandOutput, CoreError> {
        let clean_target_id = validate_identifier("run_id", &run_id)?;
        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, binding_id)?;
        let worker = Self::load_worker_tx(&tx, binding_id, &clean_target_id)?;
        let output = CommandOutput::Worker(Box::new(worker));
        enforce_compact_response_budget(&output)?;
        tx.commit()?;
        Ok(output)
    }

    pub(crate) fn execute_cancel_worker(
        &mut self,
        binding_id: &str,
        command_key: String,
        run_id: String,
        expected_revision: u64,
        reason: Option<String>,
        now_ms: i64,
    ) -> Result<CommandOutput, CoreError> {
        let clean_key = validate_command_key(&command_key)?;
        let clean_target_id = validate_identifier("run_id", &run_id)?;
        if expected_revision == 0 {
            return Err(CoreError::Validation {
                field: "expected_revision",
                reason: "expected_revision must be >= 1".to_string(),
            });
        }
        let clean_reason =
            validate_optional_text("reason", reason.as_deref(), MAX_CANCEL_REASON_LEN)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            "cancel_worker",
            &clean_target_id,
            expected_revision,
            &clean_reason,
        ))?;

        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, binding_id)?;
        if let Some(existing) =
            Self::check_dedup(&tx, binding_id, &clean_key, "cancel_worker", &payload_hash)?
        {
            return Ok(existing);
        }

        let worker = Self::load_worker_for_cancel_tx(&tx, binding_id, &clean_target_id)?;

        if worker.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: worker.run_id,
                expected_revision,
                actual_revision: worker.revision,
            });
        }

        if worker.status.is_terminal() || worker.status == WorkerStatus::CancelRequested {
            return Err(CoreError::InvalidRunState {
                run_id: worker.run_id,
                status: worker.status.to_run_status(),
                action: "cancel_worker",
            });
        }

        let sanitized_reason = clean_reason
            .as_deref()
            .map(|r| sanitize_worker_text(r, MAX_WORKER_SUMMARY_TEXT_BYTES));

        if worker.status == WorkerStatus::Queued && worker.claim_id.is_none() {
            // Unclaimed queued worker has no external process or worktree: transition directly to Cancelled.
            let outcome = WorkerOutcomeSummary {
                provider_retry_high_water: None,
                provider_health: None,
                exit_code: None,
                signal: None,
                duration_ms: Some(0),
                timed_out: false,
                cancelled: true,
                cancel_requested_before_completion: true,
                missing_model_evidence: false,
                missing_effort_evidence: false,
                provider_error: false,
                incomplete_cleanup: false,
                rejected_by_diff_gate: false,
                stdout_truncated: false,
                stderr_truncated: false,
                acceptance_state: WorkerAcceptanceState::ExecutionFailed,
                changed_paths_count: 0,
                changed_paths_preview: Vec::new(),
                violation_count: 0,
                violation_summaries: Vec::new(),
                final_summary: None,
                failure_reason: Some("cancelled while queued before dispatch".to_string()),
                report_artifact_id: None,
            };
            let outcome_json = serde_json::to_string(&outcome)?;

            tx.execute(
                "UPDATE workers
                 SET status = ?1,
                     outcome_json = ?2,
                     cancel_requested_at_ms = ?3,
                     cancel_reason = ?4,
                     completed_at_ms = ?3,
                     updated_at_ms = ?3
                 WHERE worker_id = ?5 AND binding_id = ?6",
                params![
                    WorkerStatus::Cancelled.as_str(),
                    outcome_json,
                    valid_now,
                    sanitized_reason,
                    worker.worker_id,
                    binding_id
                ],
            )?;

            let step_seq = Self::increment_run_step_tx(
                &tx,
                binding_id,
                &worker.run_id,
                RunStatus::Cancelled,
                valid_now,
            )?;
            tx.execute(
                "UPDATE runs
                 SET cancelled_at_ms = ?1,
                     cancel_reason = ?2
                 WHERE run_id = ?3 AND binding_id = ?4",
                params![valid_now, sanitized_reason, worker.run_id, binding_id],
            )?;

            Self::emit_event_tx(
                &tx,
                binding_id,
                &worker.run_id,
                worker.revision,
                step_seq,
                EventKind::WorkerCancelled,
                EventPayload {
                    untrusted_data: true,
                    summary: format!(
                        "Worker {} on run {} cancelled before dispatch",
                        worker.worker_id, worker.run_id
                    ),
                    question_id: None,
                    question_prompt: None,
                    answer: None,
                    instruction: None,
                    cancel_reason: sanitized_reason,
                },
                valid_now,
            )?;
        } else {
            // Claimed or Running worker: record CancelRequested and wait for supervisor stop proof.
            tx.execute(
                "UPDATE workers
                 SET status = ?1,
                     cancel_requested_at_ms = ?2,
                     cancel_reason = ?3,
                     updated_at_ms = ?2
                 WHERE worker_id = ?4 AND binding_id = ?5",
                params![
                    WorkerStatus::CancelRequested.as_str(),
                    valid_now,
                    sanitized_reason,
                    worker.worker_id,
                    binding_id
                ],
            )?;

            let step_seq = Self::increment_run_step_tx(
                &tx,
                binding_id,
                &worker.run_id,
                RunStatus::CancelRequested,
                valid_now,
            )?;
            tx.execute(
                "UPDATE runs
                 SET cancel_reason = ?1
                 WHERE run_id = ?2 AND binding_id = ?3",
                params![sanitized_reason, worker.run_id, binding_id],
            )?;

            Self::emit_event_tx(
                &tx,
                binding_id,
                &worker.run_id,
                worker.revision,
                step_seq,
                EventKind::WorkerCancelRequested,
                EventPayload {
                    untrusted_data: true,
                    summary: format!(
                        "Cancellation requested for worker {} on run {} at revision {}",
                        worker.worker_id, worker.run_id, worker.revision
                    ),
                    question_id: None,
                    question_prompt: None,
                    answer: None,
                    instruction: None,
                    cancel_reason: sanitized_reason,
                },
                valid_now,
            )?;
        }

        let snapshot = Self::load_worker_tx(&tx, binding_id, &worker.run_id)?;
        let output = CommandOutput::Worker(Box::new(snapshot));
        Self::record_dedup(
            &tx,
            binding_id,
            &clean_key,
            "cancel_worker",
            &payload_hash,
            &output,
            valid_now,
        )?;
        tx.commit()?;
        Ok(output)
    }

    fn verify_active_claim_tx(
        tx: &Transaction<'_>,
        claim: &WorkerClaim,
    ) -> Result<WorkerSnapshot, CoreError> {
        let worker =
            Self::load_worker_tx(tx, &claim.binding_id, &claim.run_id).map_err(
                |err| match err {
                    CoreError::WorkerNotFound { .. } | CoreError::RunNotFound { .. } => {
                        CoreError::StaleWorkerClaim {
                            binding_id: claim.binding_id.clone(),
                            run_id: claim.run_id.clone(),
                            worker_id: claim.worker_id.clone(),
                            claim_id: claim.claim_id.clone(),
                            reason: "worker record does not exist for binding and run".to_string(),
                        }
                    }
                    other => other,
                },
            )?;

        if worker.worker_id != claim.worker_id {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "worker_id mismatch: expected `{}`, actual `{}`",
                    claim.worker_id, worker.worker_id
                ),
            });
        }

        if worker.revision != claim.revision {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "revision mismatch: claim revision {}, actual revision {}",
                    claim.revision, worker.revision
                ),
            });
        }

        if worker.claim_id.as_deref() != Some(claim.claim_id.as_str()) {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "claim_id mismatch: claim `{}`, actual `{:?}`",
                    claim.claim_id, worker.claim_id
                ),
            });
        }

        if worker.attempt_count != claim.attempt_count {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "attempt_count mismatch: claim {}, actual {}",
                    claim.attempt_count, worker.attempt_count
                ),
            });
        }

        if worker.profile_id != claim.profile_id || worker.profile_digest != claim.profile_digest {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: "profile_id or profile_digest mismatch".to_string(),
            });
        }

        if worker.execution_binding != claim.execution_binding {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: "execution_binding mismatch".to_string(),
            });
        }

        if worker.worktree.base_commit != claim.base_commit {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "base_commit mismatch: claim `{}`, actual `{}`",
                    claim.base_commit, worker.worktree.base_commit
                ),
            });
        }

        if worker.instruction != claim.instruction {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: "instruction mismatch".to_string(),
            });
        }

        if worker.status.is_terminal() || worker.status == WorkerStatus::Queued {
            return Err(CoreError::StaleWorkerClaim {
                binding_id: claim.binding_id.clone(),
                run_id: claim.run_id.clone(),
                worker_id: claim.worker_id.clone(),
                claim_id: claim.claim_id.clone(),
                reason: format!(
                    "worker is in status `{}` and cannot accept claim callbacks",
                    worker.status.as_str()
                ),
            });
        }

        Ok(worker)
    }

    fn increment_run_step_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        new_status: RunStatus,
        now_ms: i64,
    ) -> Result<u64, CoreError> {
        let raw_step: i64 = tx.query_row(
            "SELECT step_seq FROM runs WHERE run_id = ?1 AND binding_id = ?2",
            params![run_id, binding_id],
            |row| row.get(0),
        )?;
        let next_step = i64_to_u64("step_seq", raw_step)? + 1;
        tx.execute(
            "UPDATE runs
             SET status = ?1,
                 step_seq = ?2,
                 next_step_at_ms = NULL,
                 updated_at_ms = ?3
             WHERE run_id = ?4 AND binding_id = ?5",
            params![
                new_status.as_str(),
                u64_to_i64("step_seq", next_step)?,
                now_ms,
                run_id,
                binding_id
            ],
        )?;
        Ok(next_step)
    }

    fn load_worker_for_cancel_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        target_id: &str,
    ) -> Result<WorkerSnapshot, CoreError> {
        match Self::load_worker_conn(tx, binding_id, target_id) {
            Ok(worker) => Ok(worker),
            Err(CoreError::WorkerNotFound { .. }) => {
                // If target_id is a valid demo run on this binding, report InvalidRunState rather than not found.
                if let Ok(demo_run) = Self::load_run_tx(tx, binding_id, target_id) {
                    return Err(CoreError::InvalidRunState {
                        run_id: demo_run.run_id,
                        status: demo_run.status,
                        action: "cancel_worker",
                    });
                }
                Err(CoreError::WorkerNotFound {
                    binding_id: binding_id.to_string(),
                    worker_id: target_id.to_string(),
                })
            }
            Err(other) => Err(other),
        }
    }

    pub(crate) fn load_worker_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        target_id: &str,
    ) -> Result<WorkerSnapshot, CoreError> {
        Self::load_worker_conn(tx, binding_id, target_id)
    }

    pub(crate) fn load_worker_conn(
        conn: &Connection,
        binding_id: &str,
        target_id: &str,
    ) -> Result<WorkerSnapshot, CoreError> {
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            i64,
            Option<String>,
            String,
            String,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
        )> = conn
            .query_row(
                "SELECT worker_id, run_id, binding_id, revision, status, profile_id,
                        profile_digest, provider, requested_model, requested_effort,
                        observed_model, observed_effort, instruction, claim_id, attempt_count,
                        execution_binding_json, worktree_json, progress_json, outcome_json,
                        created_at_ms, updated_at_ms, started_at_ms, completed_at_ms,
                        cancel_requested_at_ms, cancel_reason
                 FROM workers
                 WHERE binding_id = ?1 AND (run_id = ?2 OR worker_id = ?2)",
                params![binding_id, target_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                        r.get(11)?,
                        r.get(12)?,
                        r.get(13)?,
                        r.get(14)?,
                        r.get(15)?,
                        r.get(16)?,
                        r.get(17)?,
                        r.get(18)?,
                        r.get(19)?,
                        r.get(20)?,
                        r.get(21)?,
                        r.get(22)?,
                        r.get(23)?,
                        r.get(24)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            worker_id,
            run_id,
            b_id,
            rev_raw,
            status_raw,
            profile_id,
            profile_digest,
            provider,
            requested_model,
            requested_effort,
            observed_model,
            observed_effort,
            instruction,
            claim_id,
            attempt_raw,
            execution_binding_json,
            worktree_json,
            progress_json,
            outcome_json,
            created_at_ms,
            updated_at_ms,
            started_at_ms,
            completed_at_ms,
            cancel_requested_at_ms,
            cancel_reason,
        )) = row
        else {
            return Err(CoreError::WorkerNotFound {
                binding_id: binding_id.to_string(),
                worker_id: target_id.to_string(),
            });
        };

        let status = WorkerStatus::from_str(&status_raw).ok_or_else(|| {
            CoreError::CorruptState(format!("invalid worker status `{status_raw}`"))
        })?;
        let attempt_count =
            u32::try_from(i64_to_u64("attempt_count", attempt_raw)?).map_err(|_| {
                CoreError::CorruptState(format!("attempt_count overflow: {attempt_raw}"))
            })?;
        let execution_binding: Option<WorkerExecutionBinding> = match execution_binding_json {
            Some(json) => Some(serde_json::from_str(&json)?),
            None => None,
        };
        let worktree: WorkerWorktreeSummary = serde_json::from_str(&worktree_json)?;
        let progress: WorkerProgressSummary = serde_json::from_str(&progress_json)?;
        let outcome: Option<WorkerOutcomeSummary> = match outcome_json {
            Some(json) => Some(serde_json::from_str(&json)?),
            None => None,
        };

        let mut snapshot = WorkerSnapshot {
            untrusted_data: true,
            worker_id,
            run_id,
            binding_id: b_id,
            revision: i64_to_u64("revision", rev_raw)?,
            status,
            profile_id,
            profile_digest,
            provider,
            requested_model,
            requested_effort,
            observed_model,
            observed_effort,
            instruction,
            claim_id,
            attempt_count,
            execution_binding,
            worktree,
            progress,
            outcome,
            created_at_ms,
            updated_at_ms,
            started_at_ms,
            completed_at_ms,
            cancel_requested_at_ms,
            cancel_reason,
        };
        fit_worker_snapshot_to_budget(&mut snapshot)?;
        Ok(snapshot)
    }
}

fn fit_worker_snapshot_to_budget(snapshot: &mut WorkerSnapshot) -> Result<(), CoreError> {
    loop {
        match enforce_compact_response_budget(&CommandOutput::Worker(Box::new(snapshot.clone()))) {
            Ok(_) => return Ok(()),
            Err(err @ CoreError::ResponseTooLarge { .. }) => {
                let Some(outcome) = snapshot.outcome.as_mut() else {
                    return Err(err);
                };
                if outcome.changed_paths_preview.pop().is_some()
                    || outcome.violation_summaries.pop().is_some()
                    || outcome.final_summary.take().is_some()
                    || outcome.failure_reason.take().is_some()
                {
                    continue;
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        }
    }
}
