use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use threadlane::coworker_wakeup::{CoworkerJobWakeupAttempt, CoworkerJobWakeupContext};
use threadlane::native_tool_output::NativeToolOutputConfig;
use threadlane_core::WakeupOutcome;
use tokio::sync::watch;

use crate::coworker_workflow::{
    sha256_hex, CombinedCandidateStatus, CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey,
    CoworkerJobPhase, OwnedCoworkerControllerSession, PendingControllerEffectStatus,
    VerifiedControllerAdmission,
};
use crate::fs_safety::{
    acquire_cross_process_lock, prepare_private_dir, read_private_file_bounded,
    validate_bounded_identifier, write_private_file_atomic,
};
use crate::{NativeError, NativeErrorCode};

/// Maximum number of persisted notice delivery attempts allowed per job scope.
pub const MAX_NOTICE_ATTEMPTS: usize = 16;
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
    }
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
        let key = session.guard().key().clone();
        session.guard().validate_before_effect(&key)?;
        let admission = session.verify_admission()?;
        let reserved = self.reserve_attempt(
            &key,
            &admission.runtime_state.envelope.envelope_digest,
            Path::new(&admission.approval.repository_path),
            kind,
            admission.job.job_revision,
            snapshot_from_admission(&admission),
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
        let fingerprint = derive_notice_fingerprint(key, kind, job_revision)?;
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
        self.write_state_locked(&state_name, &state)?;
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
}
