use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane::coworker_planning_wakeup::{
    validate_coworker_planning_command_id, CoworkerPlanningWakeupAttempt,
    CoworkerPlanningWakeupContext,
};
use threadlane::coworker_wakeup::{CoworkerJobWakeupAttempt, CoworkerJobWakeupContext};
use threadlane::native_tool_output::NativeToolOutputConfig;
use threadlane_core::WakeupOutcome;
use tokio::sync::watch;

use crate::coworker_driver::{CoworkerDriverFatalError, CoworkerDriverPipelineReport};
use crate::coworker_workflow::{
    sha256_hex, CombinedCandidateStatus, CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey,
    CoworkerJobPhase, CoworkerPipelineStatus, CoworkerTaskStage, OwnedCoworkerControllerSession,
    PendingControllerEffectStatus, VerifiedControllerAdmission,
};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, prepare_private_dir,
    read_private_file_bounded, validate_bounded_identifier, write_private_file_atomic,
};
use crate::planning_commands::PlanningCommandStatus;
use crate::{NativeError, NativeErrorCode};

/// Maximum number of persisted notice delivery attempts allowed per job scope.
pub const MAX_NOTICE_ATTEMPTS: usize = 16;
// Advisory progress cannot consume the final two shared slots for terminal/fatal notices.
const MAX_PROGRESS_NOTICE_ATTEMPTS: usize = MAX_NOTICE_ATTEMPTS - 2;
/// Timeout in milliseconds applied when invoking a coworker notice wakeup.
pub const NOTICE_TIMEOUT_MS: u64 = 2_000;
const MAX_NOTICE_STATE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Classifies the coworker milestone notification being delivered.
pub enum NoticeKind {
    /// Signals completion of a job with an accepted closure candidate.
    Complete,
    /// Signals that the job was cancelled or a stop was requested.
    Stopped,
    /// Signals that the job is held, has a rejected closure candidate, or has a failed, blocked, or effect-unknown task or runtime effect.
    Attention,
}

impl NoticeKind {
    /// Returns the stable snake_case string representation of this notice kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Stopped => "stopped",
            Self::Attention => "attention",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Durable status of a persisted notice delivery attempt.
pub enum PersistedAttemptStatus {
    /// Marks an attempt reserved before transport, which becomes unknown on reopen and is never retried automatically.
    Intent,
    /// Records that the transport accepted the wakeup, not that the notification was read.
    Submitted,
    /// Records that the wakeup transport reported a failure.
    Failed,
    /// Records an indeterminate delivery outcome that is not retried automatically.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Delivery status exposed by notice reservation and finalization reports.
pub enum NoticeDeliveryStatus {
    /// Indicates transport acceptance only, not proof that the notification was read.
    Submitted,
    /// Indicates that the wakeup transport reported a failure.
    Failed,
    /// Indicates an indeterminate delivery outcome that is not retried automatically.
    Unknown,
    /// Indicates that the maximum persisted attempt count prevented a new attempt.
    CapReached,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Immutable validated configuration binding a job to its notice envelope and native wakeup target.
pub struct CoworkerNoticeConfig {
    /// Ownership key identifying the binding, thread, and coworker job for this notice.
    pub key: CoworkerJobOwnershipKey,
    /// Lowercase 64-character SHA-256 digest of the verified notice envelope.
    pub envelope_digest: String,
    /// Native tool output target bound to the ownership thread and repository root.
    pub target: NativeToolOutputConfig,
}

impl CoworkerNoticeConfig {
    /// Creates a validated configuration for trusted native callers only, never for MCP self-approval.
    pub fn new(
        key: CoworkerJobOwnershipKey,
        envelope_digest: impl Into<String>,
        target: NativeToolOutputConfig,
    ) -> Result<Self, NativeError> {
        let cfg = Self {
            key,
            envelope_digest: envelope_digest.into(),
            target,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the ownership key, envelope digest, target compatibility, and derived wakeup context.
    pub fn validate(&self) -> Result<(), NativeError> {
        self.key.validate()?;
        validate_bounded_identifier(&self.envelope_digest, "envelope_digest", 128)?;
        let normalized_envelope_digest = threadlane_workers::validate_sha256_hex_digest(
            &self.envelope_digest,
            "envelope_digest",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "envelope_digest must be 64 lowercase hex characters",
            )
        })?;
        if normalized_envelope_digest != self.envelope_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "envelope_digest must be 64 lowercase hex characters",
            ));
        }
        if self.target.has_legacy_model_metadata() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "coworker notice target must not specify model or effort overrides",
            ));
        }
        if self.target.thread_id != self.key.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker notice target thread_id must match ownership thread_uuid",
            ));
        }
        self.build_wakeup_context().map(|_| ())
    }

    fn build_wakeup_context(&self) -> Result<CoworkerJobWakeupContext, NativeError> {
        CoworkerJobWakeupContext::new(self.key.job_id.clone(), self.target.clone())
            .and_then(|ctx| ctx.with_timeout_ms(NOTICE_TIMEOUT_MS))
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// A durable record of one reserved notice delivery attempt.
pub struct PersistedNoticeAttempt {
    /// Unique identifier assigned to the persisted attempt.
    pub attempt_id: String,
    /// Milestone kind that this attempt intends to deliver.
    pub kind: NoticeKind,
    /// Job revision captured when the attempt was reserved.
    pub job_revision: u64,
    /// Deterministic fingerprint used to detect duplicate notices.
    pub fingerprint: String,
    /// Current durable status of the attempted delivery.
    pub status: PersistedAttemptStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredNoticeState {
    config: CoworkerNoticeConfig,
    attempts: Vec<PersistedNoticeAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Report returned when a notice attempt is reserved, finalized, or deduplicated.
pub struct NoticeReport {
    /// Persisted attempt identifier when an attempt exists, or None when the cap was reached.
    pub attempt_id: Option<String>,
    /// Milestone kind associated with this report.
    pub kind: NoticeKind,
    /// Job revision associated with this report.
    pub job_revision: u64,
    /// Deterministic fingerprint identifying this notice.
    pub fingerprint: String,
    /// Delivery status reported for this notice attempt.
    pub status: NoticeDeliveryStatus,
    /// Indicates whether an existing attempt was found instead of a new reservation.
    pub was_duplicate: bool,
    /// Indicates whether the maximum persisted attempt count was reached.
    pub cap_reached: bool,
    /// Total number of persisted attempts for the job scope.
    pub attempt_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
/// Read-only inspection summary of persisted coworker notice state for a job scope.
pub struct NoticeMetadata {
    /// Indicates whether a notice configuration is persisted for the job scope.
    pub configured: bool,
    /// Total number of persisted notice delivery attempts for the job scope.
    pub attempt_count: usize,
    /// Report describing the latest persisted notice delivery attempt, if any.
    pub latest: Option<NoticeReport>,
}

/// Returns the private coworker native notice store directory inside the driver directory.
pub fn coworker_notice_dir(plans_dir: &Path) -> PathBuf {
    crate::coworker_driver::coworker_driver_dir(plans_dir).join("native-notices")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Snapshot of job state used to decide whether a milestone notice is eligible.
pub struct NoticeMilestoneSnapshot {
    /// Current lifecycle phase of the coworker job.
    pub phase: CoworkerJobPhase,
    /// Indicates whether any combined candidate has an accepted closure.
    pub has_closure_accepted: bool,
    /// Indicates whether any combined candidate has a rejected closure.
    pub has_rejected_closure: bool,
    /// Indicates whether the job currently has an active hold.
    pub has_active_hold: bool,
    /// Indicates whether any task is failed, blocked, or has an unknown effect.
    pub has_attention_task: bool,
    /// Indicates whether the controller runtime has any pending effect with unknown status.
    pub has_unknown_effect: bool,
    /// Indicates whether a stop has been requested for the job runtime.
    pub stop_requested: bool,
    /// Settled pipeline block verified against fresh admission, never inferred from worker prose.
    pub settled_pipeline_block: Option<CoworkerPipelineStatus>,
}

/// Returns whether the snapshot satisfies the eligibility milestone for the given notice kind.
pub fn satisfies_notice_milestone(kind: NoticeKind, snapshot: NoticeMilestoneSnapshot) -> bool {
    match kind {
        NoticeKind::Complete => {
            snapshot.phase == CoworkerJobPhase::Completed && snapshot.has_closure_accepted
        }
        NoticeKind::Stopped => {
            snapshot.phase == CoworkerJobPhase::Cancelled || snapshot.stop_requested
        }
        NoticeKind::Attention => {
            snapshot.phase == CoworkerJobPhase::Held
                || snapshot.has_active_hold
                || snapshot.has_attention_task
                || snapshot.has_rejected_closure
                || snapshot.has_unknown_effect
                || matches!(
                    snapshot.settled_pipeline_block,
                    Some(
                        CoworkerPipelineStatus::BlockedByFailedChecks
                            | CoworkerPipelineStatus::AwaitingCorrectionExecution
                    )
                )
        }
    }
}

fn snapshot_from_admission(admission: &VerifiedControllerAdmission) -> NoticeMilestoneSnapshot {
    NoticeMilestoneSnapshot {
        phase: admission.job.phase,
        has_closure_accepted: admission
            .job
            .combined_candidates
            .iter()
            .any(|c| c.status == CombinedCandidateStatus::ClosureAccepted),
        has_rejected_closure: admission
            .job
            .combined_candidates
            .iter()
            .any(|c| c.status == CombinedCandidateStatus::Rejected),
        has_active_hold: admission.job.holds.iter().any(|h| h.active),
        has_attention_task: admission.job.tasks.iter().any(|t| {
            matches!(
                t.stage,
                crate::coworker_workflow::CoworkerTaskStage::Failed
                    | crate::coworker_workflow::CoworkerTaskStage::Blocked
                    | crate::coworker_workflow::CoworkerTaskStage::EffectUnknown
            )
        }),
        has_unknown_effect: admission
            .runtime_state
            .pending_effects
            .values()
            .any(|e| e.status == PendingControllerEffectStatus::EffectUnknown),
        stop_requested: admission.runtime_state.stop_requested,
        settled_pipeline_block: None,
    }
}

fn validate_settled_report_scope(
    report: &CoworkerDriverPipelineReport,
    key: &CoworkerJobOwnershipKey,
    approval_id: &str,
    job_revision: u64,
) -> Result<(), NativeError> {
    if report.binding_id != key.binding_id
        || report.thread_uuid != key.thread_uuid
        || report.job_id != key.job_id
        || report.approval_id != approval_id
        || report.job_revision != job_revision
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "settled notice report does not match current admission scope or revision",
        ));
    }
    if !matches!(
        report.status,
        CoworkerPipelineStatus::BlockedByFailedChecks
            | CoworkerPipelineStatus::AwaitingCorrectionExecution
    ) || report.draining_in_flight_after_stop
        || !report.in_flight_stages.is_empty()
        || report.tasks.iter().any(|task| {
            task.in_flight_stage.is_some()
                || matches!(
                    task.host_status,
                    threadlane_host::TaskExecutionStatus::Queued
                        | threadlane_host::TaskExecutionStatus::Claimed
                        | threadlane_host::TaskExecutionStatus::Running
                        | threadlane_host::TaskExecutionStatus::CancelRequested
                )
        })
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "attention notice requires a settled actionable pipeline block",
        ));
    }
    Ok(())
}

fn snapshot_from_settled_pipeline(
    admission: &VerifiedControllerAdmission,
    key: &CoworkerJobOwnershipKey,
    report: &CoworkerDriverPipelineReport,
) -> Result<NoticeMilestoneSnapshot, NativeError> {
    validate_settled_report_scope(
        report,
        key,
        &admission.approval.approval_id,
        admission.job.job_revision,
    )?;
    let runtime = &admission.runtime_state;
    if report.job_phase != admission.job.phase
        || runtime.stop_requested
        || !runtime.lease.as_ref().is_some_and(|lease| {
            lease.active
                && lease.lease_epoch == report.lease_epoch
                && lease.instance_id == report.controller_instance_id
        })
        || runtime
            .pending_effects
            .values()
            .any(|effect| effect.status == PendingControllerEffectStatus::ReservedBeforeEffect)
        || report.tasks.len() != admission.job.tasks.len()
        || report
            .tasks
            .iter()
            .zip(&admission.job.tasks)
            .any(|(snapshot, task)| {
                snapshot.task_id != task.task_id
                    || snapshot.stage != task.stage
                    || snapshot.attempt_no != task.current_attempt_no.max(1)
                    || matches!(
                        task.stage,
                        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
                    )
            })
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "settled notice report is stale or controller work remains in flight",
        ));
    }
    // The driver supplies a typed settled outcome, but a status label alone is insufficient:
    // require the corresponding current task boundary or frozen final-check outcome as well.
    let has_block_evidence = match report.status {
        CoworkerPipelineStatus::AwaitingCorrectionExecution => report.tasks.iter().any(|task| {
            task.stage == CoworkerTaskStage::CorrectInScopeRequired
                && task.awaiting_correction_execution
        }),
        CoworkerPipelineStatus::BlockedByFailedChecks => {
            report.tasks.iter().any(|task| {
                task.stage == CoworkerTaskStage::ReadyForReview
                    && task.failed_check_ids.iter().any(|id| {
                        runtime
                            .envelope
                            .check_commands
                            .get(id)
                            .is_some_and(|spec| spec.required_for_task_review)
                    })
            }) || report.completion_summary.as_ref().is_some_and(|summary| {
                summary.status
                    == crate::coworker_workflow::CoworkerJobCompletionStatus::BlockedByFailedChecks
                    && summary.failed_check_id.as_ref().is_some_and(|id| {
                        runtime
                            .envelope
                            .check_commands
                            .get(id)
                            .is_some_and(|spec| spec.required_for_candidate_closure)
                    })
            })
        }
        _ => false,
    };
    if !has_block_evidence {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "settled notice report has no typed actionable block evidence",
        ));
    }
    Ok(NoticeMilestoneSnapshot {
        settled_pipeline_block: Some(report.status),
        ..snapshot_from_admission(admission)
    })
}

/// Derives the deterministic SHA-256 fingerprint for a validated key, notice kind, and nonzero job revision.
pub fn derive_notice_fingerprint(
    key: &CoworkerJobOwnershipKey,
    kind: NoticeKind,
    job_revision: u64,
) -> Result<String, NativeError> {
    key.validate()?;
    if job_revision == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "coworker job_revision must be >= 1",
        ));
    }
    Ok(sha256_hex(
        format!(
            "coworker_notice_fp\0{}\0{}\0{}\0{}\0{}",
            key.binding_id,
            key.thread_uuid,
            key.job_id,
            kind.as_str(),
            job_revision
        )
        .as_bytes(),
    ))
}

/// Derives a separate deterministic SHA-256 fingerprint for a fatal owner error notice.
///
/// Keyed by `(binding_id, thread_uuid, job_id)` under a distinct
/// domain prefix so fatal owner failure notices are one-per-fatal-fingerprint and never collide
/// with normal milestone fingerprints.
pub fn derive_fatal_notice_fingerprint(
    key: &CoworkerJobOwnershipKey,
) -> Result<String, NativeError> {
    key.validate()?;
    Ok(sha256_hex(
        format!(
            "coworker_fatal_notice_fp\0{}\0{}\0{}",
            key.binding_id, key.thread_uuid, key.job_id
        )
        .as_bytes(),
    ))
}

// This accepts genuine host snapshots only after their caller used ApprovedHostClient.
// Scope, immutable approval identity, actual receipt IDs, active state, and the fixed core DTO
// are checked again here; provider prose and cached controller reports never grant notice authority.
pub(crate) fn verified_progress_attention(
    admission: &VerifiedControllerAdmission,
    snapshot: &threadlane_host::TaskStateSnapshot,
) -> Option<threadlane_core::WorkerProgressWatchdogSnapshot> {
    let task = admission
        .approval
        .tasks
        .iter()
        .find(|task| task.task_id == snapshot.task_id)?;
    let worker = snapshot.worker.as_ref()?;
    let attention = worker.progress.watchdog_attention.as_ref()?;
    attention.validate_attention().ok()?;
    if admission.runtime_state.stop_requested
        || snapshot.binding_id != admission.job.binding_id
        || snapshot.thread_uuid != admission.job.thread_uuid
        || snapshot.approval_id != admission.approval.approval_id
        || snapshot.subject_digest != admission.approval.subject_digest
        || snapshot.task_digest != task.task_digest
        || snapshot.command_key != task.command_key
        || snapshot.profile_id != task.profile_id
        || snapshot.run_id.as_deref() != Some(worker.run_id.as_str())
        || snapshot.worker_id.as_deref() != Some(worker.worker_id.as_str())
        || worker.binding_id != snapshot.session_binding_id
        || !matches!(
            snapshot.status,
            threadlane_host::TaskExecutionStatus::Running
                | threadlane_host::TaskExecutionStatus::CancelRequested
        )
        || !matches!(
            worker.status,
            threadlane_core::WorkerStatus::Running | threadlane_core::WorkerStatus::CancelRequested
        )
        || attention.hard_timeout_ms != admission.approval.budgets.timeout_ms
    {
        return None;
    }
    Some(attention.clone())
}

fn derive_progress_notice_fingerprint(
    key: &CoworkerJobOwnershipKey,
    task_id: &str,
    run_id: &str,
    worker_id: &str,
) -> Result<String, NativeError> {
    key.validate()?;
    for (name, value) in [
        ("task_id", task_id),
        ("run_id", run_id),
        ("worker_id", worker_id),
    ] {
        validate_bounded_identifier(value, name, 128)?;
    }
    Ok(sha256_hex(
        format!(
            "coworker_progress_notice_fp\0{}\0{}\0{}\0{task_id}\0{run_id}\0{worker_id}",
            key.binding_id, key.thread_uuid, key.job_id
        )
        .as_bytes(),
    ))
}

/// Verifies that a notice configuration matches the verified key, envelope digest, and repository path.
pub fn validate_config_identity(
    config: &CoworkerNoticeConfig,
    key: &CoworkerJobOwnershipKey,
    envelope_digest: &str,
    repository_path: &Path,
) -> Result<(), NativeError> {
    config.validate()?;
    if &config.key != key
        || config.target.thread_id != key.thread_uuid
        || config.envelope_digest != envelope_digest
        || config.target.project_root.as_path() != repository_path
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "coworker notice config identity does not match verified session admission",
        ));
    }
    Ok(())
}

enum ReserveOutcome {
    MissingConfig,
    Completed(NoticeReport),
    Reserved {
        context: Box<CoworkerJobWakeupContext>,
        attempt: CoworkerJobWakeupAttempt,
        fingerprint: String,
        job_revision: u64,
    },
}

#[derive(Debug, Clone)]
/// Durable, cross-process-locked store for pending and completed notice delivery attempts.
pub struct CoworkerNoticeStore {
    root_dir: PathBuf,
}

impl CoworkerNoticeStore {
    /// Creates the store root as a private directory and returns a store handle.
    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self, NativeError> {
        let root_dir = root_dir.into();
        prepare_private_dir(&root_dir, "coworker_notice_dir")?;
        Ok(Self { root_dir })
    }

    /// Inspects persisted notice metadata for `key` and `envelope_digest` without creating directories or locks.
    pub fn inspect(
        root_dir: &Path,
        key: &CoworkerJobOwnershipKey,
        envelope_digest: &str,
    ) -> Result<NoticeMetadata, NativeError> {
        let (_, state_name) = scope_filenames(key)?;
        validate_bounded_identifier(envelope_digest, "envelope_digest", 128)?;
        let normalized_digest =
            threadlane_workers::validate_sha256_hex_digest(envelope_digest, "envelope_digest")
                .map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "envelope_digest must be 64 lowercase hex characters",
                    )
                })?;
        if normalized_digest != envelope_digest {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "envelope_digest must be 64 lowercase hex characters",
            ));
        }
        let store = Self {
            root_dir: root_dir.to_path_buf(),
        };
        let Some(state) = store.read_state_locked(&state_name)? else {
            return Ok(NoticeMetadata::default());
        };
        state.config.key.validate()?;
        if &state.config.key != key
            || state.config.envelope_digest != envelope_digest
            || state.config.target.thread_id != key.thread_uuid
            || state.config.target.has_legacy_model_metadata()
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker notice config identity does not match inspected job scope",
            ));
        }
        let attempt_count = state.attempts.len();
        let latest = state.attempts.last().map(|attempt| NoticeReport {
            attempt_id: Some(attempt.attempt_id.clone()),
            kind: attempt.kind,
            job_revision: attempt.job_revision,
            fingerprint: attempt.fingerprint.clone(),
            status: map_persisted_status(attempt.status),
            was_duplicate: false,
            cap_reached: false,
            attempt_count,
        });
        Ok(NoticeMetadata {
            configured: true,
            attempt_count,
            latest,
        })
    }

    /// Persists an immutable notice configuration for its ownership scope.
    pub fn configure(&self, config: CoworkerNoticeConfig) -> Result<(), NativeError> {
        config.validate()?;
        let (lock_name, state_name) = scope_filenames(&config.key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "coworker_notice_lock")?;
        if let Some(existing) = self.read_state_locked(&state_name)? {
            if existing.config != config {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "coworker notice config is immutable once configured",
                ));
            }
            return Ok(());
        }
        self.write_state_locked(
            &state_name,
            &StoredNoticeState {
                config,
                attempts: Vec::new(),
            },
        )
    }

    /// Reserves and submits one milestone notice, returning None when no configuration is stored.
    pub async fn notify(
        &self,
        session: &OwnedCoworkerControllerSession,
        kind: NoticeKind,
    ) -> Result<Option<NoticeReport>, NativeError> {
        self.notify_inner(session, kind, None).await
    }

    /// Internal driver-only path for settled check/correction blocks absent from job phase.
    pub(crate) async fn notify_settled_attention(
        &self,
        session: &OwnedCoworkerControllerSession,
        report: &CoworkerDriverPipelineReport,
    ) -> Result<Option<NoticeReport>, NativeError> {
        self.notify_inner(session, NoticeKind::Attention, Some(report))
            .await
    }

    /// Reserves and submits at most one `Attention` wakeup for a verified fatal owner error.
    ///
    /// Uses a separate deterministic fatal-error fingerprint and reserve-before-send semantics.
    /// The wakeup payload remains fixed to the software-owned `job_id` and never embeds error
    /// or provider prose as instructions.
    pub async fn notify_fatal_error(
        &self,
        session: &OwnedCoworkerControllerSession,
        fatal_error: &CoworkerDriverFatalError,
    ) -> Result<Option<NoticeReport>, NativeError> {
        fatal_error.validate()?;
        let key = session.guard().key().clone();
        session.guard().validate_before_effect(&key)?;
        let admission = session.verify_admission()?;
        // A stop request or terminal task state does not erase an actual owner error.
        // This attention signal makes no completion or remote-termination claim.
        let reserved = self.reserve_fatal_attempt(
            &key,
            &admission.runtime_state.envelope.envelope_digest,
            Path::new(&admission.approval.repository_path),
            fatal_error.job_revision,
            Some(session.guard()),
        )?;
        let (context, attempt, fingerprint, job_revision) = match reserved {
            ReserveOutcome::MissingConfig => return Ok(None),
            ReserveOutcome::Completed(report) => return Ok(Some(report)),
            ReserveOutcome::Reserved {
                context,
                attempt,
                fingerprint,
                job_revision,
            } => (context, attempt, fingerprint, job_revision),
        };
        session.guard().validate_before_effect(&key)?;
        let fresh_admission = session.verify_admission()?;
        if fresh_admission.runtime_state.envelope.envelope_digest
            != admission.runtime_state.envelope.envelope_digest
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "fatal owner notice admission changed before wakeup invocation",
            ));
        }
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let outcome = context.invoke_wakeup(&attempt, &mut shutdown_rx).await;
        session.guard().validate_before_effect(&key)?;
        let report = self.finalize_attempt(
            &key,
            NoticeKind::Attention,
            job_revision,
            &fingerprint,
            &outcome,
        )?;
        Ok(Some(report))
    }

    /// Submits one bounded fixed wakeup for validated progress attention on an active host
    /// invocation. This crate-private route is called only with ApprovedHostClient snapshots.
    pub(crate) async fn notify_progress_attention(
        &self,
        session: &OwnedCoworkerControllerSession,
        snapshot: &threadlane_host::TaskStateSnapshot,
    ) -> Result<Option<NoticeReport>, NativeError> {
        let key = session.guard().key().clone();
        session.guard().validate_before_effect(&key)?;
        let admission = session.verify_admission()?;
        if verified_progress_attention(&admission, snapshot).is_none() {
            return Ok(None);
        }
        let fingerprint = derive_progress_notice_fingerprint(
            &key,
            &snapshot.task_id,
            snapshot.run_id.as_deref().expect("verified run"),
            snapshot.worker_id.as_deref().expect("verified worker"),
        )?;
        let (lock_name, state_name) = scope_filenames(&key)?;
        let reserved = {
            let _lock =
                acquire_cross_process_lock(&self.root_dir, &lock_name, "coworker_notice_lock")?;
            let Some(mut state) = self.read_state_locked(&state_name)? else {
                return Ok(None);
            };
            validate_config_identity(
                &state.config,
                &key,
                &admission.runtime_state.envelope.envelope_digest,
                &admission.approval.repository_path,
            )?;
            self.reserve_progress_for_fingerprint_locked(
                &state_name,
                &mut state,
                &key,
                admission.job.job_revision,
                fingerprint,
                Some(session.guard()),
            )?
        };
        let (context, attempt, fingerprint, job_revision) = match reserved {
            ReserveOutcome::MissingConfig => return Ok(None),
            ReserveOutcome::Completed(report) => return Ok(Some(report)),
            ReserveOutcome::Reserved {
                context,
                attempt,
                fingerprint,
                job_revision,
            } => (context, attempt, fingerprint, job_revision),
        };
        session.guard().validate_before_effect(&key)?;
        let fresh = session.verify_admission()?;
        if fresh.runtime_state.envelope.envelope_digest
            != admission.runtime_state.envelope.envelope_digest
            || verified_progress_attention(&fresh, snapshot).is_none()
        {
            // The reserved intent remains uncertain and cannot be replayed after scope drift.
            return Ok(None);
        }
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let outcome = context.invoke_wakeup(&attempt, &mut shutdown_rx).await;
        session.guard().validate_before_effect(&key)?;
        self.finalize_attempt(
            &key,
            NoticeKind::Attention,
            job_revision,
            &fingerprint,
            &outcome,
        )
        .map(Some)
    }

    async fn notify_inner(
        &self,
        session: &OwnedCoworkerControllerSession,
        kind: NoticeKind,
        settled_report: Option<&CoworkerDriverPipelineReport>,
    ) -> Result<Option<NoticeReport>, NativeError> {
        let key = session.guard().key().clone();
        session.guard().validate_before_effect(&key)?;
        let admission = session.verify_admission()?;
        let snapshot = match settled_report {
            Some(report) => snapshot_from_settled_pipeline(&admission, &key, report)?,
            None => snapshot_from_admission(&admission),
        };
        let reserved = self.reserve_attempt(
            &key,
            &admission.runtime_state.envelope.envelope_digest,
            Path::new(&admission.approval.repository_path),
            kind,
            admission.job.job_revision,
            snapshot,
            Some(session.guard()),
        )?;
        let (context, attempt, fingerprint, job_revision) = match reserved {
            ReserveOutcome::MissingConfig => return Ok(None),
            ReserveOutcome::Completed(report) => return Ok(Some(report)),
            ReserveOutcome::Reserved {
                context,
                attempt,
                fingerprint,
                job_revision,
            } => (context, attempt, fingerprint, job_revision),
        };
        session.guard().validate_before_effect(&key)?;
        if let Some(report) = settled_report {
            snapshot_from_settled_pipeline(&session.verify_admission()?, &key, report)?;
        }
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let outcome = context.invoke_wakeup(&attempt, &mut shutdown_rx).await;
        session.guard().validate_before_effect(&key)?;
        let report = self.finalize_attempt(&key, kind, job_revision, &fingerprint, &outcome)?;
        Ok(Some(report))
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_attempt(
        &self,
        key: &CoworkerJobOwnershipKey,
        envelope_digest: &str,
        repository_path: &Path,
        kind: NoticeKind,
        job_revision: u64,
        snapshot: NoticeMilestoneSnapshot,
        guard: Option<&CoworkerJobOwnershipGuard>,
    ) -> Result<ReserveOutcome, NativeError> {
        let (lock_name, state_name) = scope_filenames(key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "coworker_notice_lock")?;
        let Some(mut state) = self.read_state_locked(&state_name)? else {
            return Ok(ReserveOutcome::MissingConfig);
        };
        validate_config_identity(&state.config, key, envelope_digest, repository_path)?;
        if !satisfies_notice_milestone(kind, snapshot) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "coworker job state does not satisfy notice milestone",
            ));
        }
        let fingerprint = derive_notice_fingerprint(key, kind, job_revision)?;
        self.reserve_for_fingerprint_locked(
            &state_name,
            &mut state,
            key,
            kind,
            job_revision,
            fingerprint,
            guard,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_fatal_attempt(
        &self,
        key: &CoworkerJobOwnershipKey,
        envelope_digest: &str,
        repository_path: &Path,
        job_revision: u64,
        guard: Option<&CoworkerJobOwnershipGuard>,
    ) -> Result<ReserveOutcome, NativeError> {
        let (lock_name, state_name) = scope_filenames(key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "coworker_notice_lock")?;
        let Some(mut state) = self.read_state_locked(&state_name)? else {
            return Ok(ReserveOutcome::MissingConfig);
        };
        validate_config_identity(&state.config, key, envelope_digest, repository_path)?;
        let fingerprint = derive_fatal_notice_fingerprint(key)?;
        self.reserve_for_fingerprint_locked(
            &state_name,
            &mut state,
            key,
            NoticeKind::Attention,
            job_revision,
            fingerprint,
            guard,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_progress_for_fingerprint_locked(
        &self,
        state_name: &str,
        state: &mut StoredNoticeState,
        key: &CoworkerJobOwnershipKey,
        job_revision: u64,
        fingerprint: String,
        guard: Option<&CoworkerJobOwnershipGuard>,
    ) -> Result<ReserveOutcome, NativeError> {
        // Existing fingerprints retain the common Intent -> Unknown reconciliation and
        // deduplication path, including when the progress or global cap is already reached.
        if state.attempts.len() >= MAX_PROGRESS_NOTICE_ATTEMPTS
            && !state
                .attempts
                .iter()
                .any(|attempt| attempt.fingerprint == fingerprint)
        {
            return Ok(ReserveOutcome::Completed(NoticeReport {
                attempt_id: None,
                kind: NoticeKind::Attention,
                job_revision,
                fingerprint,
                status: NoticeDeliveryStatus::CapReached,
                was_duplicate: false,
                cap_reached: true,
                attempt_count: state.attempts.len(),
            }));
        }
        self.reserve_for_fingerprint_locked(
            state_name,
            state,
            key,
            NoticeKind::Attention,
            job_revision,
            fingerprint,
            guard,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_for_fingerprint_locked(
        &self,
        state_name: &str,
        state: &mut StoredNoticeState,
        key: &CoworkerJobOwnershipKey,
        kind: NoticeKind,
        job_revision: u64,
        fingerprint: String,
        guard: Option<&CoworkerJobOwnershipGuard>,
    ) -> Result<ReserveOutcome, NativeError> {
        let mut dirty = false;
        for entry in &mut state.attempts {
            if entry.status == PersistedAttemptStatus::Intent {
                entry.status = PersistedAttemptStatus::Unknown;
                dirty = true;
            }
        }
        if dirty {
            self.write_state_locked(state_name, state)?;
        }
        let count = state.attempts.len();
        if let Some(existing) = state.attempts.iter().find(|a| a.fingerprint == fingerprint) {
            return Ok(ReserveOutcome::Completed(NoticeReport {
                attempt_id: Some(existing.attempt_id.clone()),
                kind,
                job_revision,
                fingerprint,
                status: map_persisted_status(existing.status),
                was_duplicate: true,
                cap_reached: false,
                attempt_count: count,
            }));
        }
        if count >= MAX_NOTICE_ATTEMPTS {
            return Ok(ReserveOutcome::Completed(NoticeReport {
                attempt_id: None,
                kind,
                job_revision,
                fingerprint,
                status: NoticeDeliveryStatus::CapReached,
                was_duplicate: false,
                cap_reached: true,
                attempt_count: count,
            }));
        }
        let context = state.config.build_wakeup_context()?;
        let attempt_id = format!("notice_{}_{}", count + 1, &fingerprint[..16]);
        let attempt = CoworkerJobWakeupAttempt::new(key.job_id.clone(), attempt_id.clone())
            .and_then(|a| a.with_status_revision(job_revision))
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        if let Some(g) = guard {
            g.validate_before_effect(key)?;
        }
        state.attempts.push(PersistedNoticeAttempt {
            attempt_id,
            kind,
            job_revision,
            fingerprint: fingerprint.clone(),
            status: PersistedAttemptStatus::Intent,
        });
        self.write_state_locked(state_name, state)?;
        Ok(ReserveOutcome::Reserved {
            context: Box::new(context),
            attempt,
            fingerprint,
            job_revision,
        })
    }

    fn finalize_attempt(
        &self,
        key: &CoworkerJobOwnershipKey,
        kind: NoticeKind,
        job_revision: u64,
        fingerprint: &str,
        outcome: &WakeupOutcome,
    ) -> Result<NoticeReport, NativeError> {
        let (lock_name, state_name) = scope_filenames(key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "coworker_notice_lock")?;
        let mut state = self.read_state_locked(&state_name)?.ok_or_else(|| {
            NativeError::new(NativeErrorCode::IoError, "missing notice state on finalize")
        })?;
        let persisted = match outcome {
            WakeupOutcome::Submitted => PersistedAttemptStatus::Submitted,
            WakeupOutcome::Failed { .. } => PersistedAttemptStatus::Failed,
            WakeupOutcome::Unknown { .. } => PersistedAttemptStatus::Unknown,
        };
        let entry = state
            .attempts
            .iter_mut()
            .find(|a| a.fingerprint == fingerprint)
            .ok_or_else(|| {
                NativeError::new(NativeErrorCode::IoError, "missing reserved notice intent")
            })?;
        entry.status = persisted;
        let attempt_id = entry.attempt_id.clone();
        let attempt_count = state.attempts.len();
        self.write_state_locked(&state_name, &state)?;
        Ok(NoticeReport {
            attempt_id: Some(attempt_id),
            kind,
            job_revision,
            fingerprint: fingerprint.to_string(),
            status: map_persisted_status(persisted),
            was_duplicate: false,
            cap_reached: false,
            attempt_count,
        })
    }

    fn read_state_locked(&self, filename: &str) -> Result<Option<StoredNoticeState>, NativeError> {
        let path = self.root_dir.join(filename);
        let Some(bytes) =
            read_private_file_bounded(&path, MAX_NOTICE_STATE_BYTES, "coworker_notice_state")?
        else {
            return Ok(None);
        };
        let state: StoredNoticeState = serde_json::from_slice(&bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("invalid coworker notice state JSON: {err}"),
            )
        })?;
        if state.attempts.len() > MAX_NOTICE_ATTEMPTS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "persisted coworker notice attempts exceed cap",
            ));
        }
        Ok(Some(state))
    }

    fn write_state_locked(
        &self,
        filename: &str,
        state: &StoredNoticeState,
    ) -> Result<(), NativeError> {
        let bytes = serde_json::to_vec(state)
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        write_private_file_atomic(&self.root_dir, filename, &bytes, "coworker_notice_state")?;
        Ok(())
    }
}

fn map_persisted_status(status: PersistedAttemptStatus) -> NoticeDeliveryStatus {
    match status {
        PersistedAttemptStatus::Submitted => NoticeDeliveryStatus::Submitted,
        PersistedAttemptStatus::Failed => NoticeDeliveryStatus::Failed,
        PersistedAttemptStatus::Intent | PersistedAttemptStatus::Unknown => {
            NoticeDeliveryStatus::Unknown
        }
    }
}

fn scope_filenames(key: &CoworkerJobOwnershipKey) -> Result<(String, String), NativeError> {
    key.validate()?;
    let digest = sha256_hex(
        format!(
            "coworker_notice_scope\0{}\0{}\0{}",
            key.binding_id, key.thread_uuid, key.job_id
        )
        .as_bytes(),
    );
    Ok((
        format!("notice-{digest}.lock"),
        format!("notice-{digest}.json"),
    ))
}

fn validate_sha256_digest_str(digest: &str, label: &'static str) -> Result<(), NativeError> {
    validate_bounded_identifier(digest, label, 128)?;
    let normalized =
        threadlane_workers::validate_sha256_hex_digest(digest, label).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("{label} must be 64 lowercase hex characters"),
            )
        })?;
    if normalized != digest {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{label} must be 64 lowercase hex characters"),
        ));
    }
    Ok(())
}

/// Returns the private planning command native notice store directory inside the planning commands directory.
pub fn planning_notice_dir(plans_dir: &Path) -> PathBuf {
    crate::planning_driver::planning_commands_dir(plans_dir).join("native-notices")
}

/// Eligible terminal planning command state for a completion or attention native notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningNoticeTerminalKind {
    /// Command completed with a verified planning artifact and applicable published plan.
    Completed,
    /// Command terminated in a known failure.
    KnownFailed,
    /// Command terminated with an uncertain provider outcome (`Unknown` / `pending_reconciliation`).
    Unknown,
}

impl PlanningNoticeTerminalKind {
    /// Returns the stable `snake_case` string representation of this terminal kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::KnownFailed => "known_failed",
            Self::Unknown => "unknown",
        }
    }
}

/// Snapshot of persisted planning command state used to verify terminal notice eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanningNoticeSnapshot {
    /// Authoritative persisted planning command status.
    pub status: PlanningCommandStatus,
    /// Whether the command requires reconciliation due to an uncertain provider outcome.
    pub pending_reconciliation: bool,
    /// Whether an active process currently holds the command ownership lock.
    pub has_active_owner_lock: bool,
    /// Whether the completed artifact (and published plan, when applicable) has been verified.
    pub verified_completed_outcome: bool,
}

/// Validates whether a [`PlanningNoticeSnapshot`] satisfies terminal planning notice eligibility.
///
/// Only terminal states (`Completed` with verified artifact outcome and no pending reconciliation,
/// `KnownFailed` without pending reconciliation, or `Unknown` with `pending_reconciliation`)
/// without an active ownership lock are eligible. `Queued` and live `Running` commands are rejected.
pub fn validate_planning_notice_eligibility(
    snapshot: PlanningNoticeSnapshot,
) -> Result<PlanningNoticeTerminalKind, NativeError> {
    if snapshot.has_active_owner_lock {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning command still holds an active ownership lock",
        ));
    }
    match snapshot.status {
        PlanningCommandStatus::Queued | PlanningCommandStatus::Running => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "planning command state does not satisfy terminal notice milestone",
        )),
        PlanningCommandStatus::Completed => {
            if snapshot.pending_reconciliation || !snapshot.verified_completed_outcome {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "completed planning command requires a verified artifact outcome and no pending reconciliation",
                ));
            }
            Ok(PlanningNoticeTerminalKind::Completed)
        }
        PlanningCommandStatus::KnownFailed => {
            if snapshot.pending_reconciliation {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "known_failed planning command must not have pending reconciliation",
                ));
            }
            Ok(PlanningNoticeTerminalKind::KnownFailed)
        }
        PlanningCommandStatus::Unknown => {
            if !snapshot.pending_reconciliation {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "unknown planning command must have pending_reconciliation set",
                ));
            }
            Ok(PlanningNoticeTerminalKind::Unknown)
        }
    }
}

/// Immutable validated configuration binding a planning command to its command digest and native wakeup target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningNoticeConfig {
    /// Ownership key identifying the binding, thread, and planning command (`pcm_<32 lowercase hex>`).
    pub key: CoworkerJobOwnershipKey,
    /// Lowercase 64-character SHA-256 digest of the immutable planning command payload.
    pub command_digest: String,
    /// Native tool output target bound to the command thread and repository root.
    pub target: NativeToolOutputConfig,
}

impl PlanningNoticeConfig {
    /// Creates a validated planning notice configuration for trusted native callers only.
    pub fn new(
        key: CoworkerJobOwnershipKey,
        command_digest: impl Into<String>,
        target: NativeToolOutputConfig,
    ) -> Result<Self, NativeError> {
        let cfg = Self {
            key,
            command_digest: command_digest.into(),
            target,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates the ownership key, `pcm_<32hex>` command identifier, command digest, target compatibility, and wakeup context.
    pub fn validate(&self) -> Result<(), NativeError> {
        self.key.validate()?;
        validate_coworker_planning_command_id(&self.key.job_id)
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        validate_sha256_digest_str(&self.command_digest, "command_digest")?;
        if self.target.has_legacy_model_metadata() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning notice target must not specify model or effort overrides",
            ));
        }
        if self.target.thread_id != self.key.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "planning notice target thread_id must match ownership thread_uuid",
            ));
        }
        self.build_wakeup_context().map(|_| ())
    }

    /// Returns whether `other` binds the same immutable command and target identity.
    pub fn matches_identity(&self, other: &Self) -> bool {
        self.key == other.key
            && constant_time_str_eq(&self.command_digest, &other.command_digest)
            && self.target.delivery == other.target.delivery
            && self.target.thread_id == other.target.thread_id
            && self.target.project_root == other.target.project_root
            && self.target.reconnect == other.target.reconnect
            && !self.target.has_legacy_model_metadata()
            && !other.target.has_legacy_model_metadata()
            && (self.target.socket_path == other.target.socket_path
                || self.target.reconnect.is_some())
    }

    fn build_wakeup_context(&self) -> Result<CoworkerPlanningWakeupContext, NativeError> {
        CoworkerPlanningWakeupContext::new(self.key.job_id.clone(), self.target.clone())
            .and_then(|ctx| ctx.with_timeout_ms(NOTICE_TIMEOUT_MS))
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))
    }
}

/// A durable record of one reserved planning command notice delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedPlanningNoticeAttempt {
    /// Unique deterministic identifier assigned to the persisted attempt.
    pub attempt_id: String,
    /// Terminal planning command state associated with this attempt.
    pub terminal_kind: PlanningNoticeTerminalKind,
    /// Deterministic fingerprint used to enforce at-most-once completion/attention notice per command.
    pub fingerprint: String,
    /// Current durable status of the attempted delivery.
    pub status: PersistedAttemptStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPlanningNoticeState {
    config: PlanningNoticeConfig,
    attempts: Vec<PersistedPlanningNoticeAttempt>,
}

/// Report returned when a planning notice attempt is reserved, finalized, or deduplicated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningNoticeReport {
    /// Persisted attempt identifier when an attempt exists, or `None` when the cap was reached.
    pub attempt_id: Option<String>,
    /// Validated planning command identifier (`pcm_<32 lowercase hex>`).
    pub command_id: String,
    /// Terminal planning command state associated with this report.
    pub terminal_kind: PlanningNoticeTerminalKind,
    /// Deterministic fingerprint identifying this planning command notice.
    pub fingerprint: String,
    /// Delivery status reported for this notice attempt.
    pub status: NoticeDeliveryStatus,
    /// Indicates whether an existing attempt was found instead of a new reservation.
    pub was_duplicate: bool,
    /// Indicates whether the maximum persisted attempt count was reached.
    pub cap_reached: bool,
    /// Total number of persisted attempts for the planning command scope.
    pub attempt_count: usize,
}

/// Read-only inspection summary of persisted planning command notice state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningNoticeMetadata {
    /// Indicates whether a native notice target is configured for the planning command scope.
    pub configured: bool,
    /// Total number of persisted notice delivery attempts for the planning command scope.
    pub attempt_count: usize,
    /// Report describing the latest persisted notice delivery attempt, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<PlanningNoticeReport>,
}

/// Derives the deterministic SHA-256 fingerprint for a validated planning command key and immutable command digest.
///
/// Never keys on evolving timestamps, mutable status revisions, or random retry identifiers.
pub fn derive_planning_notice_fingerprint(
    key: &CoworkerJobOwnershipKey,
    command_digest: &str,
) -> Result<String, NativeError> {
    key.validate()?;
    validate_coworker_planning_command_id(&key.job_id)
        .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
    validate_sha256_digest_str(command_digest, "command_digest")?;
    Ok(sha256_hex(
        format!(
            "planning_notice_fp\0{}\0{}\0{}\0{}",
            key.binding_id, key.thread_uuid, key.job_id, command_digest
        )
        .as_bytes(),
    ))
}

/// Verifies that a [`PlanningNoticeConfig`] matches the verified ownership key, command digest, and repository path.
pub fn validate_planning_config_identity(
    config: &PlanningNoticeConfig,
    key: &CoworkerJobOwnershipKey,
    command_digest: &str,
    repository_path: &Path,
) -> Result<(), NativeError> {
    config.validate()?;
    if &config.key != key
        || config.target.thread_id != key.thread_uuid
        || !constant_time_str_eq(&config.command_digest, command_digest)
        || config.target.project_root.as_path() != repository_path
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "planning notice config identity does not match verified command scope",
        ));
    }
    Ok(())
}

enum PlanningReserveOutcome {
    MissingConfig,
    Completed(PlanningNoticeReport),
    Reserved {
        context: Box<CoworkerPlanningWakeupContext>,
        attempt: CoworkerPlanningWakeupAttempt,
        fingerprint: String,
        terminal_kind: PlanningNoticeTerminalKind,
    },
}

/// Durable, cross-process-locked store for planning command native notice configuration and attempts.
#[derive(Debug, Clone)]
pub struct PlanningNoticeStore {
    root_dir: PathBuf,
}

impl PlanningNoticeStore {
    /// Creates the store root as a private `0700` directory and returns a store handle.
    pub fn new(root_dir: impl Into<PathBuf>) -> Result<Self, NativeError> {
        let root_dir = root_dir.into();
        prepare_private_dir(&root_dir, "planning_notice_dir")?;
        Ok(Self { root_dir })
    }

    /// Inspects persisted planning notice metadata for `key` and `command_digest` without creating directories or locks.
    pub fn inspect(
        root_dir: &Path,
        key: &CoworkerJobOwnershipKey,
        command_digest: &str,
    ) -> Result<PlanningNoticeMetadata, NativeError> {
        let (_, state_name) = planning_scope_filenames(key)?;
        validate_sha256_digest_str(command_digest, "command_digest")?;
        let store = Self {
            root_dir: root_dir.to_path_buf(),
        };
        let Some(state) = store.read_state_locked(&state_name)? else {
            return Ok(PlanningNoticeMetadata::default());
        };
        state.config.key.validate()?;
        if validate_coworker_planning_command_id(&state.config.key.job_id).is_err()
            || &state.config.key != key
            || !constant_time_str_eq(&state.config.command_digest, command_digest)
            || state.config.target.thread_id != key.thread_uuid
            || state.config.target.has_legacy_model_metadata()
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "planning notice config identity does not match inspected command scope",
            ));
        }
        let attempt_count = state.attempts.len();
        let latest = state.attempts.last().map(|attempt| PlanningNoticeReport {
            attempt_id: Some(attempt.attempt_id.clone()),
            command_id: key.job_id.clone(),
            terminal_kind: attempt.terminal_kind,
            fingerprint: attempt.fingerprint.clone(),
            status: map_persisted_status(attempt.status),
            was_duplicate: false,
            cap_reached: false,
            attempt_count,
        });
        Ok(PlanningNoticeMetadata {
            configured: true,
            attempt_count,
            latest,
        })
    }

    /// Persists an immutable planning notice configuration for its command ownership scope.
    ///
    /// On duplicate submission for the same command scope, verifies identical command and target
    /// identity without overwriting or resetting existing intent or attempts.
    pub fn configure(&self, config: PlanningNoticeConfig) -> Result<(), NativeError> {
        config.validate()?;
        let (lock_name, state_name) = planning_scope_filenames(&config.key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "planning_notice_lock")?;
        if let Some(existing) = self.read_state_locked(&state_name)? {
            if !existing.config.matches_identity(&config) {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "planning notice config is immutable once configured",
                ));
            }
            return Ok(());
        }
        self.write_state_locked(
            &state_name,
            &StoredPlanningNoticeState {
                config,
                attempts: Vec::new(),
            },
        )
    }

    /// Reserves and submits at most one terminal completion/attention notice for a configured planning command.
    ///
    /// Returns `Ok(None)` quietly when no notice configuration exists for `key`.
    pub async fn notify_if_configured(
        &self,
        key: &CoworkerJobOwnershipKey,
        command_digest: &str,
        repository_path: &Path,
        snapshot: PlanningNoticeSnapshot,
    ) -> Result<Option<PlanningNoticeReport>, NativeError> {
        let reserved = self.reserve_attempt(key, command_digest, repository_path, snapshot)?;
        let (context, attempt, fingerprint, terminal_kind) = match reserved {
            PlanningReserveOutcome::MissingConfig => return Ok(None),
            PlanningReserveOutcome::Completed(report) => return Ok(Some(report)),
            PlanningReserveOutcome::Reserved {
                context,
                attempt,
                fingerprint,
                terminal_kind,
            } => (context, attempt, fingerprint, terminal_kind),
        };
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let outcome = context.invoke_wakeup(&attempt, &mut shutdown_rx).await;
        let report = self.finalize_attempt(key, terminal_kind, &fingerprint, &outcome)?;
        Ok(Some(report))
    }

    fn reserve_attempt(
        &self,
        key: &CoworkerJobOwnershipKey,
        command_digest: &str,
        repository_path: &Path,
        snapshot: PlanningNoticeSnapshot,
    ) -> Result<PlanningReserveOutcome, NativeError> {
        let (lock_name, state_name) = planning_scope_filenames(key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "planning_notice_lock")?;
        let Some(mut state) = self.read_state_locked(&state_name)? else {
            return Ok(PlanningReserveOutcome::MissingConfig);
        };
        validate_planning_config_identity(&state.config, key, command_digest, repository_path)?;
        let terminal_kind = validate_planning_notice_eligibility(snapshot)?;
        let mut dirty = false;
        for entry in &mut state.attempts {
            if entry.status == PersistedAttemptStatus::Intent {
                entry.status = PersistedAttemptStatus::Unknown;
                dirty = true;
            }
        }
        if dirty {
            self.write_state_locked(&state_name, &state)?;
        }
        let fingerprint = derive_planning_notice_fingerprint(key, command_digest)?;
        let count = state.attempts.len();
        if let Some(existing) = state.attempts.iter().find(|a| a.fingerprint == fingerprint) {
            return Ok(PlanningReserveOutcome::Completed(PlanningNoticeReport {
                attempt_id: Some(existing.attempt_id.clone()),
                command_id: key.job_id.clone(),
                terminal_kind: existing.terminal_kind,
                fingerprint,
                status: map_persisted_status(existing.status),
                was_duplicate: true,
                cap_reached: false,
                attempt_count: count,
            }));
        }
        if count >= MAX_NOTICE_ATTEMPTS {
            return Ok(PlanningReserveOutcome::Completed(PlanningNoticeReport {
                attempt_id: None,
                command_id: key.job_id.clone(),
                terminal_kind,
                fingerprint,
                status: NoticeDeliveryStatus::CapReached,
                was_duplicate: false,
                cap_reached: true,
                attempt_count: count,
            }));
        }
        let context = state.config.build_wakeup_context()?;
        let attempt_id = format!("pnotice_{}_{}", count + 1, &fingerprint[..16]);
        let attempt = CoworkerPlanningWakeupAttempt::new(key.job_id.clone(), attempt_id.clone())
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        state.attempts.push(PersistedPlanningNoticeAttempt {
            attempt_id,
            terminal_kind,
            fingerprint: fingerprint.clone(),
            status: PersistedAttemptStatus::Intent,
        });
        self.write_state_locked(&state_name, &state)?;
        Ok(PlanningReserveOutcome::Reserved {
            context: Box::new(context),
            attempt,
            fingerprint,
            terminal_kind,
        })
    }

    fn finalize_attempt(
        &self,
        key: &CoworkerJobOwnershipKey,
        terminal_kind: PlanningNoticeTerminalKind,
        fingerprint: &str,
        outcome: &WakeupOutcome,
    ) -> Result<PlanningNoticeReport, NativeError> {
        let (lock_name, state_name) = planning_scope_filenames(key)?;
        let _lock = acquire_cross_process_lock(&self.root_dir, &lock_name, "planning_notice_lock")?;
        let mut state = self.read_state_locked(&state_name)?.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::IoError,
                "missing planning notice state on finalize",
            )
        })?;
        let persisted = match outcome {
            WakeupOutcome::Submitted => PersistedAttemptStatus::Submitted,
            WakeupOutcome::Failed { .. } => PersistedAttemptStatus::Failed,
            WakeupOutcome::Unknown { .. } => PersistedAttemptStatus::Unknown,
        };
        let entry = state
            .attempts
            .iter_mut()
            .find(|a| a.fingerprint == fingerprint)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    "missing reserved planning notice intent",
                )
            })?;
        entry.status = persisted;
        let attempt_id = entry.attempt_id.clone();
        let attempt_count = state.attempts.len();
        self.write_state_locked(&state_name, &state)?;
        Ok(PlanningNoticeReport {
            attempt_id: Some(attempt_id),
            command_id: key.job_id.clone(),
            terminal_kind,
            fingerprint: fingerprint.to_string(),
            status: map_persisted_status(persisted),
            was_duplicate: false,
            cap_reached: false,
            attempt_count,
        })
    }

    fn read_state_locked(
        &self,
        filename: &str,
    ) -> Result<Option<StoredPlanningNoticeState>, NativeError> {
        let path = self.root_dir.join(filename);
        let Some(bytes) =
            read_private_file_bounded(&path, MAX_NOTICE_STATE_BYTES, "planning_notice_state")?
        else {
            return Ok(None);
        };
        let state: StoredPlanningNoticeState = serde_json::from_slice(&bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("invalid planning notice state JSON: {err}"),
            )
        })?;
        if state.attempts.len() > MAX_NOTICE_ATTEMPTS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "persisted planning notice attempts exceed cap",
            ));
        }
        Ok(Some(state))
    }

    fn write_state_locked(
        &self,
        filename: &str,
        state: &StoredPlanningNoticeState,
    ) -> Result<(), NativeError> {
        let bytes = serde_json::to_vec(state)
            .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
        write_private_file_atomic(&self.root_dir, filename, &bytes, "planning_notice_state")?;
        Ok(())
    }
}

fn planning_scope_filenames(
    key: &CoworkerJobOwnershipKey,
) -> Result<(String, String), NativeError> {
    key.validate()?;
    validate_coworker_planning_command_id(&key.job_id)
        .map_err(|err| NativeError::new(NativeErrorCode::ValidationFailed, err.to_string()))?;
    let digest = sha256_hex(
        format!(
            "planning_notice_scope\0{}\0{}\0{}",
            key.binding_id, key.thread_uuid, key.job_id
        )
        .as_bytes(),
    );
    Ok((
        format!("planning-notice-{digest}.lock"),
        format!("planning-notice-{digest}.json"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn fixture(
        dir: &Path,
    ) -> (
        CoworkerNoticeStore,
        CoworkerNoticeConfig,
        NoticeMilestoneSnapshot,
    ) {
        prepare_private_dir(dir, "test_root").unwrap();
        let sock = dir.join("codex.sock");
        if !sock.exists() {
            let _ = UnixListener::bind(&sock).unwrap();
        }
        let key = CoworkerJobOwnershipKey::new(
            "binding_1",
            "550e8400-e29b-41d4-a716-446655440000",
            "job_0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let target = NativeToolOutputConfig::new_user_message_steer(
            sock,
            key.thread_uuid.clone(),
            dir.to_path_buf(),
        );
        let cfg = CoworkerNoticeConfig::new(key, "a".repeat(64), target).unwrap();
        let store = CoworkerNoticeStore::new(dir.join("store")).unwrap();
        store.configure(cfg.clone()).unwrap();
        let snap = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::Completed,
            has_closure_accepted: true,
            has_rejected_closure: false,
            has_active_hold: false,
            has_attention_task: false,
            has_unknown_effect: false,
            stop_requested: false,
            settled_pipeline_block: None,
        };
        (store, cfg, snap)
    }

    #[test]
    fn milestone_predicate_complete_vs_accepted_only() {
        let accepted_only = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::Executing,
            has_closure_accepted: true,
            has_rejected_closure: false,
            has_active_hold: false,
            has_attention_task: false,
            has_unknown_effect: false,
            stop_requested: false,
            settled_pipeline_block: None,
        };
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            accepted_only
        ));
        assert!(satisfies_notice_milestone(
            NoticeKind::Complete,
            NoticeMilestoneSnapshot {
                phase: CoworkerJobPhase::Completed,
                ..accepted_only
            }
        ));
    }

    #[test]
    fn milestone_predicate_rejected_closure_and_unknown_effect_attention_with_negative_controls() {
        let closure_review_clean = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::ClosureReview,
            has_closure_accepted: false,
            has_rejected_closure: false,
            has_active_hold: false,
            has_attention_task: false,
            has_unknown_effect: false,
            stop_requested: false,
            settled_pipeline_block: None,
        };
        assert!(!satisfies_notice_milestone(
            NoticeKind::Attention,
            closure_review_clean
        ));
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            closure_review_clean
        ));

        let rejected_closure = NoticeMilestoneSnapshot {
            has_rejected_closure: true,
            ..closure_review_clean
        };
        assert!(satisfies_notice_milestone(
            NoticeKind::Attention,
            rejected_closure
        ));
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            rejected_closure
        ));
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            NoticeMilestoneSnapshot {
                phase: CoworkerJobPhase::Completed,
                ..rejected_closure
            }
        ));

        let unknown_effect = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::Executing,
            has_unknown_effect: true,
            ..closure_review_clean
        };
        assert!(satisfies_notice_milestone(
            NoticeKind::Attention,
            unknown_effect
        ));
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            unknown_effect
        ));
        assert!(!satisfies_notice_milestone(
            NoticeKind::Complete,
            NoticeMilestoneSnapshot {
                phase: CoworkerJobPhase::Completed,
                ..unknown_effect
            }
        ));
    }

    #[test]
    fn reserve_attention_for_rejected_closure_and_unknown_effect_deduplicates_on_reopen() {
        let clean_closure_review = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::ClosureReview,
            has_closure_accepted: false,
            has_rejected_closure: false,
            has_active_hold: false,
            has_attention_task: false,
            has_unknown_effect: false,
            stop_requested: false,
            settled_pipeline_block: None,
        };
        let rejected_closure = NoticeMilestoneSnapshot {
            has_rejected_closure: true,
            ..clean_closure_review
        };
        let unknown_effect = NoticeMilestoneSnapshot {
            phase: CoworkerJobPhase::Executing,
            has_unknown_effect: true,
            ..clean_closure_review
        };

        for eligible_snap in [rejected_closure, unknown_effect] {
            let tmp = tempfile::tempdir().unwrap();
            let (store, cfg, _) = fixture(tmp.path());
            assert!(store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Attention,
                    1,
                    clean_closure_review,
                    None,
                )
                .is_err());
            let reserved = store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Attention,
                    1,
                    eligible_snap,
                    None,
                )
                .unwrap();
            assert!(matches!(reserved, ReserveOutcome::Reserved { .. }));

            let reopened = CoworkerNoticeStore::new(tmp.path().join("store")).unwrap();
            let dup = reopened
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Attention,
                    1,
                    eligible_snap,
                    None,
                )
                .unwrap();
            assert!(matches!(
                dup,
                ReserveOutcome::Completed(NoticeReport {
                    kind: NoticeKind::Attention,
                    status: NoticeDeliveryStatus::Unknown,
                    was_duplicate: true,
                    cap_reached: false,
                    attempt_count: 1,
                    ..
                })
            ));
        }
    }

    #[test]
    fn fatal_notice_fingerprint_is_separate_and_deduplicates_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, _) = fixture(tmp.path());
        let normal_fp = derive_notice_fingerprint(&cfg.key, NoticeKind::Attention, 3).unwrap();
        let fatal_fp = derive_fatal_notice_fingerprint(&cfg.key).unwrap();
        assert_ne!(normal_fp, fatal_fp);

        let reserved = store
            .reserve_fatal_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                3,
                None,
            )
            .unwrap();
        let ReserveOutcome::Reserved { fingerprint, .. } = reserved else {
            panic!("expected reserved fatal attempt");
        };
        assert_eq!(fingerprint, fatal_fp);

        // Reopen promotes Intent -> Unknown and refuses automatic replay.
        let reopened = CoworkerNoticeStore::new(tmp.path().join("store")).unwrap();
        let dup = reopened
            .reserve_fatal_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                3,
                None,
            )
            .unwrap();
        assert!(matches!(
            dup,
            ReserveOutcome::Completed(NoticeReport {
                kind: NoticeKind::Attention,
                status: NoticeDeliveryStatus::Unknown,
                was_duplicate: true,
                cap_reached: false,
                attempt_count: 1,
                ..
            })
        ));
    }

    #[test]
    fn fatal_notice_rejects_wrong_envelope_or_repository_and_respects_shared_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, snap) = fixture(tmp.path());
        let wrong_repo = tmp.path().join("other-repository");
        for (digest, repo) in [
            ("b".repeat(64), cfg.target.project_root.clone()),
            (cfg.envelope_digest.clone(), wrong_repo),
        ] {
            assert_eq!(
                store
                    .reserve_fatal_attempt(&cfg.key, &digest, &repo, 1, None)
                    .err()
                    .unwrap()
                    .code,
                NativeErrorCode::SecurityViolation
            );
        }
        assert_eq!(
            CoworkerNoticeStore::inspect(&store.root_dir, &cfg.key, &cfg.envelope_digest)
                .unwrap()
                .attempt_count,
            0
        );
        for revision in 1..=MAX_NOTICE_ATTEMPTS as u64 {
            assert!(matches!(
                store
                    .reserve_attempt(
                        &cfg.key,
                        &cfg.envelope_digest,
                        &cfg.target.project_root,
                        NoticeKind::Complete,
                        revision,
                        snap,
                        None
                    )
                    .unwrap(),
                ReserveOutcome::Reserved { .. }
            ));
        }
        assert!(matches!(
            store
                .reserve_fatal_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    99,
                    None
                )
                .unwrap(),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::CapReached,
                attempt_count: MAX_NOTICE_ATTEMPTS,
                ..
            })
        ));
    }

    fn settled_report(
        cfg: &CoworkerNoticeConfig,
        status: CoworkerPipelineStatus,
    ) -> CoworkerDriverPipelineReport {
        serde_json::from_value(serde_json::json!({
            "binding_id": cfg.key.binding_id,
            "thread_uuid": cfg.key.thread_uuid,
            "job_id": cfg.key.job_id,
            "approval_id": "approval-current",
            "job_phase": "executing",
            "job_revision": 7,
            "lease_epoch": 1,
            "controller_instance_id": "controller-current",
            "status": status
        }))
        .unwrap()
    }

    #[test]
    fn settled_block_scope_and_running_controls() {
        let tmp = tempfile::tempdir().unwrap();
        let (_, cfg, _) = fixture(tmp.path());
        for status in [
            CoworkerPipelineStatus::BlockedByFailedChecks,
            CoworkerPipelineStatus::AwaitingCorrectionExecution,
        ] {
            let report = settled_report(&cfg, status);
            assert!(
                validate_settled_report_scope(&report, &cfg.key, "approval-current", 7).is_ok()
            );
            for field in [
                "binding_id",
                "thread_uuid",
                "job_id",
                "approval_id",
                "job_revision",
            ] {
                let mut value = serde_json::to_value(&report).unwrap();
                value[field] = if field == "job_revision" {
                    serde_json::json!(8)
                } else {
                    serde_json::json!("other-scope")
                };
                let forged = serde_json::from_value(value).unwrap();
                assert!(
                    validate_settled_report_scope(&forged, &cfg.key, "approval-current", 7)
                        .is_err(),
                    "{field}"
                );
            }
            let mut running = report.clone();
            running.status = CoworkerPipelineStatus::Running;
            assert!(
                validate_settled_report_scope(&running, &cfg.key, "approval-current", 7).is_err()
            );
            running = report.clone();
            running.in_flight_stages.push(
                crate::coworker_workflow::CoworkerPipelineInFlightStageRef {
                    task_id: None,
                    attempt_no: None,
                    kind:
                        crate::coworker_workflow::CoworkerPipelineInFlightStageKind::JobCompletion,
                },
            );
            assert!(
                validate_settled_report_scope(&running, &cfg.key, "approval-current", 7).is_err()
            );
        }
    }

    #[test]
    fn settled_block_attention_deduplicates_after_submit_and_interrupted_intent() {
        for status in [
            CoworkerPipelineStatus::BlockedByFailedChecks,
            CoworkerPipelineStatus::AwaitingCorrectionExecution,
        ] {
            for submitted in [false, true] {
                let tmp = tempfile::tempdir().unwrap();
                let (store, cfg, baseline) = fixture(tmp.path());
                let clean = NoticeMilestoneSnapshot {
                    phase: CoworkerJobPhase::Executing,
                    has_closure_accepted: false,
                    ..baseline
                };
                assert!(!satisfies_notice_milestone(NoticeKind::Attention, clean));
                assert!(!satisfies_notice_milestone(
                    NoticeKind::Attention,
                    NoticeMilestoneSnapshot {
                        settled_pipeline_block: Some(CoworkerPipelineStatus::Running),
                        ..clean
                    }
                ));
                let snapshot = NoticeMilestoneSnapshot {
                    settled_pipeline_block: Some(status),
                    ..clean
                };
                assert!(!satisfies_notice_milestone(NoticeKind::Complete, snapshot));
                assert!(!satisfies_notice_milestone(NoticeKind::Stopped, snapshot));
                let reserved = store
                    .reserve_attempt(
                        &cfg.key,
                        &cfg.envelope_digest,
                        &cfg.target.project_root,
                        NoticeKind::Attention,
                        7,
                        snapshot,
                        None,
                    )
                    .unwrap();
                let ReserveOutcome::Reserved { fingerprint, .. } = reserved else {
                    panic!("expected reserved intent")
                };
                if submitted {
                    store
                        .finalize_attempt(
                            &cfg.key,
                            NoticeKind::Attention,
                            7,
                            &fingerprint,
                            &WakeupOutcome::Submitted,
                        )
                        .unwrap();
                }
                let reopened = CoworkerNoticeStore::new(tmp.path().join("store")).unwrap();
                for _ in 0..2 {
                    let ReserveOutcome::Completed(duplicate) = reopened
                        .reserve_attempt(
                            &cfg.key,
                            &cfg.envelope_digest,
                            &cfg.target.project_root,
                            NoticeKind::Attention,
                            7,
                            snapshot,
                            None,
                        )
                        .unwrap()
                    else {
                        panic!("must not resend")
                    };
                    assert!(duplicate.was_duplicate);
                    assert_eq!(duplicate.attempt_count, 1);
                    assert_eq!(
                        duplicate.status,
                        if submitted {
                            NoticeDeliveryStatus::Submitted
                        } else {
                            NoticeDeliveryStatus::Unknown
                        }
                    );
                }
            }
        }
    }

    #[test]
    fn intent_survives_reopen_as_unknown_and_failed_never_replays() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, snap) = fixture(tmp.path());
        let res = store
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                1,
                snap,
                None,
            )
            .unwrap();
        assert!(matches!(res, ReserveOutcome::Reserved { .. }));
        let reopened = CoworkerNoticeStore::new(tmp.path().join("store")).unwrap();
        let dup = reopened
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                1,
                snap,
                None,
            )
            .unwrap();
        assert!(matches!(
            dup,
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::Unknown,
                was_duplicate: true,
                ..
            })
        ));
        if let ReserveOutcome::Reserved { fingerprint, .. } = reopened
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                2,
                snap,
                None,
            )
            .unwrap()
        {
            reopened
                .finalize_attempt(
                    &cfg.key,
                    NoticeKind::Complete,
                    2,
                    &fingerprint,
                    &WakeupOutcome::Failed {
                        error: "boom".into(),
                    },
                )
                .unwrap();
        }
        let failed_dup = reopened
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                2,
                snap,
                None,
            )
            .unwrap();
        assert!(matches!(
            failed_dup,
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::Failed,
                was_duplicate: true,
                ..
            })
        ));
    }

    #[test]
    fn progress_subcap_preserves_fatal_and_terminal_slots_and_unknown_deduplication() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, complete) = fixture(tmp.path());
        let reserve_progress = |store: &CoworkerNoticeStore, invocation: usize| {
            let (lock_name, state_name) = scope_filenames(&cfg.key).unwrap();
            let _lock =
                acquire_cross_process_lock(&store.root_dir, &lock_name, "coworker_notice_lock")
                    .unwrap();
            let mut state = store.read_state_locked(&state_name).unwrap().unwrap();
            let fingerprint = derive_progress_notice_fingerprint(
                &cfg.key,
                "task_1",
                &format!("run_{invocation}"),
                &format!("worker_{invocation}"),
            )
            .unwrap();
            store
                .reserve_progress_for_fingerprint_locked(
                    &state_name,
                    &mut state,
                    &cfg.key,
                    1,
                    fingerprint,
                    None,
                )
                .unwrap()
        };
        for invocation in 0..MAX_PROGRESS_NOTICE_ATTEMPTS {
            assert!(matches!(
                reserve_progress(&store, invocation),
                ReserveOutcome::Reserved { .. }
            ));
        }
        assert!(matches!(
            reserve_progress(&store, 14),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::CapReached,
                attempt_count: 14,
                cap_reached: true,
                was_duplicate: false,
                ..
            })
        ));
        // The last progress intent is reconciled as Unknown, not retried at the subcap.
        drop(store);
        let store = CoworkerNoticeStore::new(tmp.path().join("store")).unwrap();
        assert!(matches!(
            reserve_progress(&store, 13),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::Unknown,
                attempt_count: 14,
                was_duplicate: true,
                cap_reached: false,
                ..
            })
        ));
        assert!(matches!(
            store
                .reserve_fatal_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    2,
                    None
                )
                .unwrap(),
            ReserveOutcome::Reserved { .. }
        ));
        assert!(matches!(
            store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Complete,
                    2,
                    complete,
                    None
                )
                .unwrap(),
            ReserveOutcome::Reserved { .. }
        ));
        assert!(matches!(
            store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Complete,
                    3,
                    complete,
                    None
                )
                .unwrap(),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::CapReached,
                attempt_count: 16,
                cap_reached: true,
                ..
            })
        ));
        assert!(matches!(
            reserve_progress(&store, 13),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::Unknown,
                attempt_count: 16,
                was_duplicate: true,
                cap_reached: false,
                ..
            })
        ));
        assert!(matches!(
            reserve_progress(&store, 14),
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::CapReached,
                attempt_count: 16,
                cap_reached: true,
                was_duplicate: false,
                ..
            })
        ));
    }

    #[test]
    fn enforces_16_attempt_cap_and_rejects_identity_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, snap) = fixture(tmp.path());
        assert!(
            validate_config_identity(&cfg, &cfg.key, "wrong_digest", &cfg.target.project_root)
                .is_err()
        );
        for rev in 1..=(MAX_NOTICE_ATTEMPTS as u64) {
            if let ReserveOutcome::Reserved { fingerprint, .. } = store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Complete,
                    rev,
                    snap,
                    None,
                )
                .unwrap()
            {
                store
                    .finalize_attempt(
                        &cfg.key,
                        NoticeKind::Complete,
                        rev,
                        &fingerprint,
                        &WakeupOutcome::Submitted,
                    )
                    .unwrap();
            }
        }
        let capped = store
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                17,
                snap,
                None,
            )
            .unwrap();
        assert!(matches!(
            capped,
            ReserveOutcome::Completed(NoticeReport {
                status: NoticeDeliveryStatus::CapReached,
                cap_reached: true,
                attempt_count: 16,
                ..
            })
        ));
    }

    #[test]
    fn inspect_missing_root_returns_default_and_creates_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("missing");
        let key = CoworkerJobOwnershipKey::new(
            "binding_1",
            "550e8400-e29b-41d4-a716-446655440000",
            "job_0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let digest = "c".repeat(64);
        assert_eq!(
            CoworkerNoticeStore::inspect(&root, &key, &digest).unwrap(),
            NoticeMetadata::default()
        );
        assert!(!root.exists());
    }

    #[test]
    fn inspect_reserved_intent_unknown_and_bytes_mtime_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, snap) = fixture(tmp.path());
        let root = tmp.path().join("store");
        assert!(matches!(
            store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.envelope_digest,
                    &cfg.target.project_root,
                    NoticeKind::Complete,
                    1,
                    snap,
                    None,
                )
                .unwrap(),
            ReserveOutcome::Reserved { .. }
        ));
        let (_, state_name) = scope_filenames(&cfg.key).unwrap();
        let state_path = root.join(state_name);
        let bytes = std::fs::read(&state_path).unwrap();
        let mtime = std::fs::metadata(&state_path).unwrap().modified().unwrap();
        for _ in 0..2 {
            let meta = CoworkerNoticeStore::inspect(&root, &cfg.key, &cfg.envelope_digest).unwrap();
            assert!(meta.configured);
            assert_eq!(meta.attempt_count, 1);
            assert_eq!(
                meta.latest.map(|report| report.status),
                Some(NoticeDeliveryStatus::Unknown)
            );
        }
        assert_eq!(std::fs::read(&state_path).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(&state_path).unwrap().modified().unwrap(),
            mtime
        );
    }

    #[test]
    fn inspect_digest_and_reserve_scope_mismatch_are_security_violations_without_mutation() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, cfg, snap) = fixture(tmp.path());
        let root = tmp.path().join("store");
        store
            .reserve_attempt(
                &cfg.key,
                &cfg.envelope_digest,
                &cfg.target.project_root,
                NoticeKind::Complete,
                1,
                snap,
                None,
            )
            .unwrap();
        let (_, state_name) = scope_filenames(&cfg.key).unwrap();
        let state_path = root.join(state_name);
        let bytes = std::fs::read(&state_path).unwrap();
        let mtime = std::fs::metadata(&state_path).unwrap().modified().unwrap();
        let wrong_digest = "d".repeat(64);
        let inspect_err = match CoworkerNoticeStore::inspect(&root, &cfg.key, &wrong_digest) {
            Err(err) => err,
            Ok(_) => panic!("expected digest mismatch"),
        };
        assert!(format!("{inspect_err:?}").contains("SecurityViolation"));
        let wrong_root = tmp.path().join("wrong-root");
        let reserve_err = match store.reserve_attempt(
            &cfg.key,
            &cfg.envelope_digest,
            &wrong_root,
            NoticeKind::Complete,
            2,
            snap,
            None,
        ) {
            Err(err) => err,
            Ok(_) => panic!("expected scope mismatch"),
        };
        assert!(format!("{reserve_err:?}").contains("SecurityViolation"));
        assert_eq!(std::fs::read(&state_path).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(&state_path).unwrap().modified().unwrap(),
            mtime
        );
    }

    #[test]
    fn planning_notice_eligibility_and_reserve_intent_reopen_no_replay() {
        let tmp = tempfile::tempdir().unwrap();
        prepare_private_dir(tmp.path(), "test_root").unwrap();
        let sock = tmp.path().join("codex.sock");
        let _listener = UnixListener::bind(&sock).unwrap();
        let key = CoworkerJobOwnershipKey::new(
            "binding_1",
            "550e8400-e29b-41d4-a716-446655440000",
            "pcm_0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let target = NativeToolOutputConfig::new_user_message_steer(
            sock,
            key.thread_uuid.clone(),
            tmp.path().to_path_buf(),
        );
        let cfg = PlanningNoticeConfig::new(key, "b".repeat(64), target).unwrap();
        let store_root = tmp.path().join("planning-notices");

        // Pure read-only inspect on missing root creates nothing.
        let unconfigured =
            PlanningNoticeStore::inspect(&store_root, &cfg.key, &cfg.command_digest).unwrap();
        assert!(!unconfigured.configured);
        assert!(!store_root.exists());

        let store = PlanningNoticeStore::new(&store_root).unwrap();
        store.configure(cfg.clone()).unwrap();
        // Idempotent duplicate configure succeeds without resetting.
        store.configure(cfg.clone()).unwrap();

        // Queued, Running, active lock, and unverified Completed are rejected.
        for bad_snap in [
            PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Queued,
                pending_reconciliation: false,
                has_active_owner_lock: false,
                verified_completed_outcome: false,
            },
            PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Running,
                pending_reconciliation: false,
                has_active_owner_lock: false,
                verified_completed_outcome: false,
            },
            PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Completed,
                pending_reconciliation: false,
                has_active_owner_lock: true,
                verified_completed_outcome: true,
            },
            PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Completed,
                pending_reconciliation: false,
                has_active_owner_lock: false,
                verified_completed_outcome: false,
            },
            PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Completed,
                pending_reconciliation: true,
                has_active_owner_lock: false,
                verified_completed_outcome: true,
            },
        ] {
            assert!(validate_planning_notice_eligibility(bad_snap).is_err());
        }

        // Eligible terminal states: Completed, KnownFailed, Unknown.
        assert_eq!(
            validate_planning_notice_eligibility(PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Completed,
                pending_reconciliation: false,
                has_active_owner_lock: false,
                verified_completed_outcome: true,
            })
            .unwrap(),
            PlanningNoticeTerminalKind::Completed
        );
        assert_eq!(
            validate_planning_notice_eligibility(PlanningNoticeSnapshot {
                status: PlanningCommandStatus::KnownFailed,
                pending_reconciliation: false,
                has_active_owner_lock: false,
                verified_completed_outcome: false,
            })
            .unwrap(),
            PlanningNoticeTerminalKind::KnownFailed
        );
        assert_eq!(
            validate_planning_notice_eligibility(PlanningNoticeSnapshot {
                status: PlanningCommandStatus::Unknown,
                pending_reconciliation: true,
                has_active_owner_lock: false,
                verified_completed_outcome: false,
            })
            .unwrap(),
            PlanningNoticeTerminalKind::Unknown
        );

        let completed_snap = PlanningNoticeSnapshot {
            status: PlanningCommandStatus::Completed,
            pending_reconciliation: false,
            has_active_owner_lock: false,
            verified_completed_outcome: true,
        };
        assert!(matches!(
            store
                .reserve_attempt(
                    &cfg.key,
                    &cfg.command_digest,
                    &cfg.target.project_root,
                    completed_snap
                )
                .unwrap(),
            PlanningReserveOutcome::Reserved { .. }
        ));

        // Duplicate configure does not reset reserved intent.
        store.configure(cfg.clone()).unwrap();

        // Inspect maps Intent to Unknown without mutating bytes or mtime.
        let meta =
            PlanningNoticeStore::inspect(&store_root, &cfg.key, &cfg.command_digest).unwrap();
        assert!(meta.configured);
        assert_eq!(meta.attempt_count, 1);
        assert_eq!(
            meta.latest.as_ref().map(|r| r.status),
            Some(NoticeDeliveryStatus::Unknown)
        );

        // Reopen / second reserve promotes Intent to Unknown and deduplicates without replaying.
        let reopened = PlanningNoticeStore::new(&store_root).unwrap();
        match reopened
            .reserve_attempt(
                &cfg.key,
                &cfg.command_digest,
                &cfg.target.project_root,
                completed_snap,
            )
            .unwrap()
        {
            PlanningReserveOutcome::Completed(report) => {
                assert!(report.was_duplicate);
                assert_eq!(report.status, NoticeDeliveryStatus::Unknown);
                assert_eq!(report.attempt_count, 1);
            }
            _ => panic!("expected duplicate Unknown completion without replay"),
        }
    }
}
