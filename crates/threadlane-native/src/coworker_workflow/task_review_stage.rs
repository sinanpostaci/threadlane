use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use threadlane_authority::{validation::validate_canonical_thread_uuid, ApprovedTaskRecord};
use threadlane_operations::{
    accept_review_subject, inspect_review_subject_readonly, load_review_receipt,
    verify_review_receipt, verify_review_receipt_readonly, InspectReviewSubjectRequest,
    OperationsError, ReviewAcceptanceRequest, ReviewReceipt, ReviewSubject,
};
use threadlane_workers::{
    is_worktree_clean, load_and_verify_manifest, validate_sha256_hex_digest, ExecutionBudgets,
    WorkerProvider, WorkerRunStatus, WorktreeKind, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{ToolAdapter, WorkflowRole};

use super::checks::{FrozenCheckSubject, FrozenCheckTargetKind};
use super::contracts::{
    deterministic_prefixed_id, validate_non_native_worker_choice, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage, OutboxDispatchKind,
    OutboxDispatchRecord, OutboxDispatchStatus, ReviewVerdictOutcome, StructuredReviewVerdict,
    TaskCandidateRef, WorkflowHoldReason, MAX_COWORKER_ID_LEN, MAX_COWORKER_LEDGER_BYTES,
};
use super::controller::{
    verify_task_fenced_for_advancement, CoworkerControllerService, VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, prove_registry_snapshot_equivalence_to_frozen_authority,
    ControllerJobRuntimeState, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, TaskAttemptExecutionArtifacts, DEFAULT_CONTROLLER_LEASE_TTL_MS,
};
use super::ownership::CoworkerJobOwnershipGuard;
use super::review::{ExpectedTaskReviewIdentity, MAX_TASK_REVIEW_RESPONSE_BYTES};
use super::review_execution::{
    run_task_review_with_claim_and_health, TaskReviewExecutionReport, TaskReviewExecutionRequest,
    TaskReviewExecutionStatus,
};
use super::store::{
    FinalizeTaskOutboxRequest, RecordTaskReviewVerdictRequest, ReserveTaskOutboxRequest,
};
use super::task_verification::read_persisted_check_report;
use crate::conversation_change::ConversationChangeStore;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, read_private_file_bounded, sanitize_diagnostic_text,
    validate_bounded_identifier, write_private_file_atomic,
};
use crate::profile_flow::{
    normalize_read_only_advisory_choice, preflight, resolve_advisory_tool_executable,
    PreflightBlockReason, PreflightRequest, RoleExecutionRoute, RolePreflightRequest,
    SelectedRoleChoice,
};
use crate::role_flow::read_registry_snapshot_readonly;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InFlightTaskReviewKey {
    plans_dir: PathBuf,
    binding_id: String,
    job_id: String,
    task_id: String,
    attempt_no: u32,
}

fn in_flight_review_set() -> &'static Mutex<BTreeSet<InFlightTaskReviewKey>> {
    static SET: OnceLock<Mutex<BTreeSet<InFlightTaskReviewKey>>> = OnceLock::new();
    SET.get_or_init(|| Mutex::new(BTreeSet::new()))
}

struct InFlightTaskReviewGuard {
    key: InFlightTaskReviewKey,
}

impl InFlightTaskReviewGuard {
    fn acquire(
        plans_dir: &Path,
        binding_id: &str,
        job_id: &str,
        task_id: &str,
        attempt_no: u32,
    ) -> Result<Self, NativeError> {
        let key = InFlightTaskReviewKey {
            plans_dir: plans_dir.to_path_buf(),
            binding_id: binding_id.to_string(),
            job_id: job_id.to_string(),
            task_id: task_id.to_string(),
            attempt_no,
        };
        let mut guard = in_flight_review_set().lock().map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                "in-flight task review stage mutex poisoned",
            )
        })?;
        if !guard.insert(key.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "concurrent task review stage call already in flight for job `{job_id}` task `{task_id}` attempt `{attempt_no}`"
                ),
            ));
        }
        Ok(Self { key })
    }
}

impl Drop for InFlightTaskReviewGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = in_flight_review_set().lock() {
            guard.remove(&self.key);
        }
    }
}

/// Request to execute the supervised read-only `Reviewer` stage for a verified task attempt,
/// record its durable [`TaskCandidateRef`] and [`StructuredReviewVerdict`], and (on `Accept`)
/// mint and persist a real [`ReviewReceipt`].
///
/// Never accepts caller-supplied model, provider, executable, effort, or verdict overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskReviewStageRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Target task identifier in `job.tasks` and `approval.tasks`.
    pub task_id: String,
    /// 1-based task attempt number (`>= 1`).
    pub attempt_no: u32,
    /// Current wall-clock timestamp (ms since Unix epoch).
    pub now_ms: u64,
    /// Optional bounded untrusted candidate notes labelled as untrusted in the Reviewer prompt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub untrusted_candidate_context: Option<String>,
}

/// High-level stage disposition produced by [`execute_task_review_stage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerTaskReviewStageStatus {
    /// Reviewer returned `Accept`, a real [`ReviewReceipt`] was minted and saved, and the task
    /// advanced to [`CoworkerTaskStage::Accepted`].
    Accepted,
    /// Reviewer returned `CorrectInScope`; the durable verdict was recorded and the task advanced
    /// to [`CoworkerTaskStage::CorrectInScopeRequired`] without automatic Coder retry.
    CorrectInScopeRequired,
    /// Reviewer returned `ScopeConflict`; the durable verdict and a [`super::contracts::WorkflowHoldRecord`]
    /// were recorded and the task advanced to [`CoworkerTaskStage::Held`].
    HeldByScopeConflict,
    /// Reviewer returned `Blocked` or `Unclear`; the durable verdict was recorded and the task
    /// advanced to [`CoworkerTaskStage::Blocked`].
    BlockedByVerdict,
}

/// Typed outcome of [`execute_task_review_stage`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskReviewStageOutcome {
    /// Owning [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Reviewed task identifier.
    pub task_id: String,
    /// Reviewed 1-based attempt number.
    pub attempt_no: u32,
    /// Resulting durable [`CoworkerTaskStage`].
    pub task_stage: CoworkerTaskStage,
    /// High-level stage disposition.
    pub status: CoworkerTaskReviewStageStatus,
    /// Deterministic candidate identifier (`tcd_<32hex>`).
    pub candidate_id: String,
    /// Semantic derived review request key (`trk_<32hex>`).
    pub request_key: String,
    /// Deterministic controller effect identifier (`eff_<32hex>`).
    pub effect_id: String,
    /// Durable [`OutboxDispatchRecord::outbox_id`].
    pub outbox_id: String,
    /// Local report artifact identifier (`trr_<32hex>`).
    pub report_artifact_id: String,
    /// Absolute path to the private (`0600`) persisted [`TaskReviewExecutionReport`] file.
    pub report_path: String,
    /// Full bounded [`TaskReviewExecutionReport`] persisted on disk.
    pub report: TaskReviewExecutionReport,
    /// 64-character lowercase hex SHA-256 digest of the structured Reviewer response.
    pub reviewer_response_digest: String,
    /// Promoted [`StructuredReviewVerdict`] recorded on the candidate.
    pub verdict: StructuredReviewVerdict,
    /// Recorded [`TaskCandidateRef`] in the binding ledger.
    pub candidate: TaskCandidateRef,
    /// Real [`ReviewReceipt`] minted and persisted when `status == Accepted`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_receipt: Option<ReviewReceipt>,
    /// Active [`super::contracts::WorkflowHoldRecord::hold_id`] when `status == HeldByScopeConflict`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_id: Option<String>,
    /// Truthful blocking diagnostic when the verdict did not accept the candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_error: Option<String>,
    /// `true` when the completed review artifact and ledger state were verified and returned idempotently.
    pub reused_existing: bool,
}

impl CoworkerTaskReviewStageOutcome {
    /// Returns `Ok(&ReviewReceipt)` when the candidate was accepted with a verified receipt, or a
    /// truthful [`NativeError`] when rejected, held, or blocked.
    pub fn require_accepted(&self) -> Result<&ReviewReceipt, NativeError> {
        if self.status == CoworkerTaskReviewStageStatus::Accepted
            && self.task_stage == CoworkerTaskStage::Accepted
        {
            if let Some(ref receipt) = self.review_receipt {
                return Ok(receipt);
            }
        }
        Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            self.blocking_error.clone().unwrap_or_else(|| {
                format!(
                    "task `{}` attempt `{}` was not accepted (outcome: `{}`)",
                    self.task_id,
                    self.attempt_no,
                    self.verdict.outcome.as_str()
                )
            }),
        ))
    }
}

/// Service wrapper for executing the supervised `Reviewer` stage on a verified task attempt.
#[derive(Debug, Clone)]
pub struct CoworkerTaskReviewStageService {
    controller: CoworkerControllerService,
}

impl CoworkerTaskReviewStageService {
    /// Creates a new [`CoworkerTaskReviewStageService`] wrapping `controller`.
    pub fn new(controller: CoworkerControllerService) -> Self {
        Self { controller }
    }

    /// Borrows the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Executes or idempotently reopens the supervised `Reviewer` stage for a verified task attempt.
    pub async fn review_task_attempt(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskReviewStageRequest,
    ) -> Result<CoworkerTaskReviewStageOutcome, NativeError> {
        execute_task_review_stage(&self.controller, guard, request).await
    }
}

/// Executes the supervised read-only `Reviewer` stage for a verified task attempt, persists the
/// full [`TaskReviewExecutionReport`] in a private (`0600`) file outside the 1 MiB binding ledger,
/// advances `ReviewReserved -> UnderReview` via [`super::store::CoworkerWorkflowStore::reconcile_or_finalize_outbox`],
/// records [`TaskCandidateRef`] and [`StructuredReviewVerdict`] via
/// [`super::store::CoworkerWorkflowStore::record_task_candidate_and_verdict`], and on `Accept`
/// issues a real [`ReviewReceipt`] via [`accept_review_subject`] and stores it in
/// [`TaskAttemptExecutionArtifacts::review_receipt`].
pub async fn execute_task_review_stage(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
) -> Result<CoworkerTaskReviewStageOutcome, NativeError> {
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
    validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
        )
    })?;
    if request.attempt_no == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "attempt_no must be >= 1",
        ));
    }

    let plans_dir = controller.workflow_store().plans_dir().to_path_buf();
    let _in_flight = InFlightTaskReviewGuard::acquire(
        &plans_dir,
        &request.binding_id,
        &request.job_id,
        &request.task_id,
        request.attempt_no,
    )?;

    let (
        adm,
        controller_instance_id,
        lease_epoch,
        candidate_task,
        approved_task,
        artifacts,
        candidate_id,
        review_subject,
        reviewer_choice,
        provider,
        executable,
        budgets,
        max_response_bytes,
    ) = verify_live_task_review_gate(controller, guard, request, None)?;

    let expected_identity = ExpectedTaskReviewIdentity::from_subject(
        &review_subject,
        &request.task_id,
        request.attempt_no,
        &candidate_id,
        &adm.linkage.policy_digest,
    );

    let request_key = compute_task_review_request_key(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &candidate_id,
        &review_subject.subject_digest,
        &adm.linkage.policy_digest,
        &adm.runtime_state.envelope.envelope_digest,
        &reviewer_choice,
    );
    let effect_id = compute_task_review_effect_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &request_key,
    );
    let report_artifact_id = compute_task_review_report_artifact_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &request_key,
    );

    verify_no_conflicting_review_key(
        &adm.job,
        &adm.runtime_state,
        &candidate_task,
        request.attempt_no,
        &candidate_id,
        &request_key,
        &effect_id,
        &review_subject.subject_digest,
    )?;

    if let Some(reopened) = try_reopen_completed_review_stage(
        &plans_dir,
        request,
        &adm.job,
        &candidate_task,
        &artifacts,
        &candidate_id,
        &review_subject,
        &expected_identity,
        &request_key,
        &effect_id,
        &report_artifact_id,
    )? {
        return Ok(reopened);
    }

    let existing_effect = adm.runtime_state.pending_effects.get(&effect_id).cloned();
    let existing_outbox = find_task_review_outbox(&adm.job, &request.task_id, &request_key);
    let persisted_report = read_persisted_task_review_report(&plans_dir, &effect_id)?;

    let (report, report_path, outbox_record) = if existing_effect.is_some()
        || existing_outbox.is_some()
        || persisted_report.is_some()
        || candidate_task.stage != CoworkerTaskStage::ReadyForReview
    {
        recover_existing_review_invocation(
            controller,
            guard,
            request,
            &plans_dir,
            &adm,
            &controller_instance_id,
            lease_epoch,
            &candidate_task,
            &review_subject,
            &expected_identity,
            &request_key,
            &effect_id,
            &report_artifact_id,
            existing_effect.as_ref(),
            existing_outbox,
            persisted_report,
        )?
    } else {
        verify_task_fenced_for_advancement(
            &candidate_task,
            &adm.job,
            &adm.runtime_state,
            &adm.approval.approval_id,
            &plans_dir,
        )?;

        let exec_req = TaskReviewExecutionRequest {
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            subject: review_subject.clone(),
            expected_identity: expected_identity.clone(),
            provider,
            executable,
            model_arg: reviewer_choice.model_arg.clone(),
            effort: reviewer_choice.effort.clone(),
            budgets,
            max_response_bytes,
            frozen_instructions: Some(approved_task.instruction.clone()),
            untrusted_candidate_context: request.untrusted_candidate_context.clone(),
        };
        let _ = exec_req.validate()?;
        let held_report_fd = open_private_report_staging_fd(&plans_dir, &effect_id)?;

        let outbox_record = reserve_review_effect_and_outbox(
            controller,
            guard,
            request,
            &adm,
            &controller_instance_id,
            lease_epoch,
            &review_subject,
            &request_key,
            &effect_id,
        )?;

        let health_sink = crate::provider_health::ScopedHealthSink::controller(
            controller,
            guard,
            &effect_id,
            WorkflowRole::Reviewer,
            Some(&request.task_id),
            Some(request.attempt_no),
            &review_subject.subject_digest,
        )?;
        let (_cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let run_res = run_task_review_with_claim_and_health(
            &exec_req,
            guard,
            None,
            None,
            &mut cancel_rx,
            Some(&health_sink),
        )
        .await;

        controller.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let report = match run_res {
            Ok(rep) => rep,
            Err(err) => {
                let diag = sanitize_diagnostic_text(err.message());
                mark_review_effect_and_outbox_unknown(
                    controller,
                    request,
                    &controller_instance_id,
                    lease_epoch,
                    &effect_id,
                    &outbox_record.outbox_id,
                    None,
                    None,
                    None,
                    &diag,
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "Reviewer invocation for task `{}` attempt `{}` failed with uncertain outcome: {diag}",
                        request.task_id, request.attempt_no
                    ),
                ));
            }
        };

        report.validate_integrity()?;
        let report_path =
            persist_held_task_review_report(&plans_dir, &effect_id, held_report_fd, &report)?;
        (report, report_path, outbox_record)
    };

    finalize_and_promote_review_report(
        controller,
        guard,
        request,
        &plans_dir,
        &controller_instance_id,
        lease_epoch,
        &candidate_id,
        &review_subject,
        &expected_identity,
        &reviewer_choice,
        &request_key,
        &effect_id,
        &report_artifact_id,
        &outbox_record,
        report,
        report_path,
    )
}

/// Alias for [`execute_task_review_stage`].
pub async fn review_verified_task_attempt(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
) -> Result<CoworkerTaskReviewStageOutcome, NativeError> {
    execute_task_review_stage(controller, guard, request).await
}

/// Computes the deterministic semantic request key (`trk_<32hex>`) for a task review invocation.
#[allow(clippy::too_many_arguments)]
pub fn compute_task_review_request_key(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    candidate_id: &str,
    subject_digest: &str,
    policy_digest: &str,
    envelope_digest: &str,
    reviewer_choice: &SelectedRoleChoice,
) -> String {
    let attempt_str = attempt_no.to_string();
    let effort_str = reviewer_choice.effort.as_deref().unwrap_or("");
    let diff_str = reviewer_choice
        .matched_difficulty
        .map(|d| d.to_string())
        .unwrap_or_default();
    deterministic_prefixed_id(
        "threadlane-native:coworker-task-review-request:v1",
        "trk",
        &[
            job_id,
            task_id,
            &attempt_str,
            candidate_id,
            subject_digest,
            policy_digest,
            envelope_digest,
            &reviewer_choice.tool_id,
            &reviewer_choice.model_id,
            &reviewer_choice.model_arg,
            effort_str,
            &diff_str,
        ],
    )
}

/// Computes the deterministic [`PendingControllerEffect::effect_id`] (`eff_<32hex>`) for a task review invocation.
pub fn compute_task_review_effect_id(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    request_key: &str,
) -> String {
    deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[job_id, task_id, &attempt_no.to_string(), request_key],
    )
}

fn compute_task_review_report_artifact_id(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    request_key: &str,
) -> String {
    deterministic_prefixed_id(
        "threadlane-native:coworker-task-review-report:v1",
        "trr",
        &[job_id, task_id, &attempt_no.to_string(), request_key],
    )
}

fn review_report_filename(effect_id: &str) -> String {
    format!("coworker-task-review-report-{effect_id}.json")
}

fn review_report_file_path(plans_dir: &Path, effect_id: &str) -> PathBuf {
    plans_dir.join(review_report_filename(effect_id))
}

fn open_private_report_staging_fd(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<tempfile::NamedTempFile, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let prefix = format!(".{}.tmp.", review_report_filename(effect_id));
        tempfile::Builder::new()
            .prefix(&prefix)
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(plans_dir)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to open private 0600 task review report staging file: {}",
                        sanitize_diagnostic_text(&err.to_string())
                    ),
                )
            })
    }
    #[cfg(not(unix))]
    {
        let _ = (plans_dir, effect_id);
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "task review stage requires Unix 0600 file permissions",
        ))
    }
}

fn persist_held_task_review_report(
    plans_dir: &Path,
    effect_id: &str,
    mut held_temp: tempfile::NamedTempFile,
    report: &TaskReviewExecutionReport,
) -> Result<PathBuf, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let bytes = serde_json::to_vec_pretty(report).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize TaskReviewExecutionReport JSON",
        )
    })?;
    if bytes.len() > MAX_COWORKER_LEDGER_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "TaskReviewExecutionReport exceeds maximum private artifact file budget",
        ));
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let target_path = review_report_file_path(plans_dir, effect_id);
        crate::fs_safety::validate_private_file_path(&target_path, "task_review_execution_report")?;
        held_temp.write_all(&bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to write held 0600 task review report file: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;
        held_temp.as_file().sync_all().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to sync held 0600 task review report file: {}",
                    sanitize_diagnostic_text(&err.to_string())
                ),
            )
        })?;
        held_temp
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to enforce 0600 permissions on held task review report file: {}",
                        sanitize_diagnostic_text(&err.to_string())
                    ),
                )
            })?;
        if held_temp.persist(&target_path).is_err() {
            return write_private_file_atomic(
                plans_dir,
                &review_report_filename(effect_id),
                &bytes,
                "task_review_execution_report",
            );
        }
        if let Ok(dir_file) = fs::File::open(plans_dir) {
            let _ = dir_file.sync_all();
        }
        Ok(target_path)
    }
    #[cfg(not(unix))]
    {
        let _ = (plans_dir, effect_id, held_temp, bytes);
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "task review stage requires Unix 0600 file permissions",
        ))
    }
}

/// Reads and validates integrity of a persisted private (`0600`) [`TaskReviewExecutionReport`] by `effect_id`.
pub fn read_persisted_task_review_report(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<Option<TaskReviewExecutionReport>, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let path = review_report_file_path(plans_dir, effect_id);
    let Some(bytes) = read_private_file_bounded(
        &path,
        MAX_COWORKER_LEDGER_BYTES,
        "task_review_execution_report",
    )?
    else {
        return Ok(None);
    };
    let report: TaskReviewExecutionReport = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted TaskReviewExecutionReport JSON",
        )
    })?;
    report.validate_integrity()?;
    Ok(Some(report))
}

#[allow(clippy::type_complexity)]
fn verify_live_task_review_gate(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    expected_lease: Option<(&str, u64)>,
) -> Result<
    (
        VerifiedControllerAdmission,
        String,
        u64,
        CoworkerTaskRecord,
        ApprovedTaskRecord,
        TaskAttemptExecutionArtifacts,
        String,
        ReviewSubject,
        SelectedRoleChoice,
        WorkerProvider,
        PathBuf,
        ExecutionBudgets,
        usize,
    ),
    NativeError,
> {
    let plans_dir = controller.workflow_store().plans_dir();
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let mut adm = controller.verify_job_admission(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let own_instance_id = deterministic_controller_instance_id(
        &request.binding_id,
        &request.job_id,
        guard.instance_id(),
    );

    if expected_lease.is_none() {
        let needs_lease_acquire = match adm.runtime_state.lease.as_ref() {
            Some(lease) => {
                !lease.active
                    || lease.instance_id != own_instance_id
                    || lease.lease_epoch == 0
                    || lease.expires_at_ms <= lease.acquired_at_ms
            }
            None => true,
        };
        if needs_lease_acquire {
            if let Some(ref prior_lease) = adm.runtime_state.lease {
                if prior_lease.active && prior_lease.instance_id != own_instance_id {
                    controller.verify_physical_ownership(
                        guard,
                        &request.binding_id,
                        &request.thread_uuid,
                        &request.job_id,
                    )?;
                    controller.policy_store().release_lease(
                        &request.binding_id,
                        &request.job_id,
                        &prior_lease.instance_id,
                        prior_lease.lease_epoch,
                    )?;
                }
            }
            controller.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;
            let _ = controller.policy_store().acquire_or_renew_lease(
                &request.binding_id,
                &request.job_id,
                &own_instance_id,
                request.now_ms.max(1),
                DEFAULT_CONTROLLER_LEASE_TTL_MS,
            )?;
            adm = controller.verify_job_admission(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;
        }
    }

    let lease = adm.runtime_state.lease.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ControllerJobRuntimeState has no active driver lease",
        )
    })?;
    if !lease.active
        || lease.instance_id != own_instance_id
        || lease.lease_epoch == 0
        || lease.expires_at_ms <= lease.acquired_at_ms
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ControllerJobRuntimeState driver lease is not active for the current physical owner",
        ));
    }
    let controller_instance_id = lease.instance_id.clone();
    let lease_epoch = lease.lease_epoch;
    if let Some((exp_inst, exp_epoch)) = expected_lease {
        if controller_instance_id != exp_inst || lease_epoch != exp_epoch {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller driver lease changed during task review stage",
            ));
        }
    }

    if adm.runtime_state.stop_requested {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "controller job has stop_requested == true; task review stage is fenced",
        ));
    }

    let change_summary = ConversationChangeStore::open_readonly(plans_dir)
        .and_then(|store| {
            store.unresolved_blocking_changes(
                &request.binding_id,
                Some(&adm.approval.approval_id),
                None,
            )
        })
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to read ConversationChangeStore during task review stage: {err}"),
            )
        })?;
    if change_summary.current_change_revision > adm.runtime_state.synced_change_revision {
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            &controller_instance_id,
            lease_epoch,
            |state| {
                if change_summary.current_change_revision > state.synced_change_revision {
                    state.synced_change_revision = change_summary.current_change_revision;
                }
                Ok(())
            },
        )?;
        adm.runtime_state.synced_change_revision = change_summary.current_change_revision;
    }
    if change_summary.approval_superseded {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "approval `{}` is superseded in ConversationChangeStore; task review stage is fenced",
                adm.approval.approval_id
            ),
        ));
    }

    if adm.job.phase != CoworkerJobPhase::Executing {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "job `{}` is in phase `{:?}` (expected `executing`)",
                adm.job.job_id, adm.job.phase
            ),
        ));
    }

    let candidate_task = adm
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` was not found in job `{}`",
                    request.task_id, request.job_id
                ),
            )
        })?;
    let approved_task = adm
        .approval
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` was not found in ApprovalRecord `{}`",
                    request.task_id, adm.approval.approval_id
                ),
            )
        })?;

    if candidate_task.current_attempt_no != request.attempt_no {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` current_attempt_no `{}` does not match requested attempt_no `{}`",
                request.task_id, candidate_task.current_attempt_no, request.attempt_no
            ),
        ));
    }

    let (reviewer_choice, provider, executable, budgets, max_response_bytes) =
        resolve_frozen_reviewer_route(controller, &adm)?;

    let art_key = ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
    let artifacts = adm
        .runtime_state
        .attempt_artifacts
        .get(&art_key)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing TaskAttemptExecutionArtifacts for task `{}` attempt `{}`",
                    request.task_id, request.attempt_no
                ),
            )
        })?;

    let (candidate_id, review_subject) = verify_preserved_candidate_and_live_subject(
        plans_dir,
        request,
        &adm,
        &candidate_task,
        &approved_task,
        &artifacts,
    )?;

    Ok((
        adm,
        controller_instance_id,
        lease_epoch,
        candidate_task,
        approved_task,
        artifacts,
        candidate_id,
        review_subject,
        reviewer_choice,
        provider,
        executable,
        budgets,
        max_response_bytes,
    ))
}

fn resolve_frozen_reviewer_route(
    controller: &CoworkerControllerService,
    adm: &VerifiedControllerAdmission,
) -> Result<
    (
        SelectedRoleChoice,
        WorkerProvider,
        PathBuf,
        ExecutionBudgets,
        usize,
    ),
    NativeError,
> {
    let reviewer_choice = adm.runtime_state.envelope.reviewer_choice.clone();
    if reviewer_choice.route != RoleExecutionRoute::HeadlessWorker
        || !matches!(
            reviewer_choice.adapter,
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 | ToolAdapter::CodexCli
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "frozen reviewer_choice route `{}` (`{:?}`) is not a supported headless worker route",
                reviewer_choice.route, reviewer_choice.adapter
            ),
        ));
    }
    validate_non_native_worker_choice(WorkflowRole::Reviewer, &reviewer_choice)?;

    if !adm.linkage.authorizes_reviewer_dispatch(&reviewer_choice) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen approval linkage does not authorize the envelope reviewer_choice",
        ));
    }
    if !adm.runtime_state.envelope.policy.allows(
        WorkflowRole::Reviewer,
        &reviewer_choice.tool_id,
        &reviewer_choice.model_id,
        reviewer_choice.effort.as_deref(),
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen execution policy denies the envelope reviewer_choice",
        ));
    }

    let snapshot = read_registry_snapshot_readonly(controller.authority().registry().config_dir())?;
    if snapshot.revision != adm.linkage.registry_revision
        && !prove_registry_snapshot_equivalence_to_frozen_authority(
            &adm.approval,
            &adm.job,
            &adm.runtime_state.envelope,
            &snapshot,
        )?
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "live registry revision `{}` drifted from frozen linkage revision `{}`",
                snapshot.revision, adm.linkage.registry_revision
            ),
        ));
    }

    let pf = preflight(
        &snapshot,
        &PreflightRequest {
            profile_id: Some(adm.linkage.profile_id.clone()),
            roles: vec![RolePreflightRequest::new(
                WorkflowRole::Reviewer,
                reviewer_choice.matched_difficulty,
            )],
        },
        &adm.runtime_state.envelope.policy,
    )?;
    let outcome = pf.roles.first().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Reviewer preflight returned no role outcome",
        )
    })?;
    if !pf.ready || !outcome.ready {
        let code = match outcome.block_reason {
            Some(PreflightBlockReason::UnsupportedRoute) => NativeErrorCode::UnsupportedCapability,
            _ => NativeErrorCode::ValidationFailed,
        };
        return Err(NativeError::new(
            code,
            format!("Reviewer preflight blocked: {}", outcome.detail),
        ));
    }
    let raw_live_choice = outcome.selected.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Reviewer preflight marked ready without selected choice",
        )
    })?;
    let live_reviewer_choice =
        normalize_read_only_advisory_choice(WorkflowRole::Reviewer, raw_live_choice)?;
    if live_reviewer_choice != reviewer_choice {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live registry Reviewer resolution does not match frozen envelope reviewer_choice",
        ));
    }

    let tool_def = snapshot
        .tools
        .iter()
        .find(|t| t.id == reviewer_choice.tool_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                format!(
                    "frozen Reviewer tool `{}` is missing from registry snapshot",
                    reviewer_choice.tool_id
                ),
            )
        })?;
    if !tool_def.enabled || tool_def.adapter != reviewer_choice.adapter {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "frozen Reviewer tool `{}` is disabled or adapter mismatched",
                reviewer_choice.tool_id
            ),
        ));
    }
    let executable = match crate::coworker_admission::complete_admission_pins(
        controller.workflow_store(),
        controller.policy_store(),
        &adm.job,
    )? {
        Some(pins) => {
            if pins.reviewer.choice != reviewer_choice {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Admission Reviewer pin differs from frozen envelope",
                ));
            }
            pins.reviewer.executable()?.to_path_buf()
        }
        None => resolve_advisory_tool_executable(tool_def)?,
    };

    let provider = match reviewer_choice.adapter {
        ToolAdapter::Antigravity => WorkerProvider::Antigravity,
        ToolAdapter::OpencodeV2 => WorkerProvider::OpenCodeV2,
        ToolAdapter::CodexCli => WorkerProvider::CodexCli,
        _ => {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "unsupported adapter for headless Reviewer execution",
            ));
        }
    };

    let reviewer_budget = adm
        .runtime_state
        .envelope
        .role_budgets
        .get(&WorkflowRole::Reviewer)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope is missing Reviewer role budget",
            )
        })?;
    reviewer_budget.validate()?;
    let budgets = ExecutionBudgets {
        timeout_ms: reviewer_budget.max_timeout_ms_per_call,
        max_stdout_bytes: reviewer_budget.max_stdout_bytes_per_call,
        max_stderr_bytes: reviewer_budget.max_stderr_bytes_per_call,
        ..ExecutionBudgets::default()
    };
    let max_response_bytes = reviewer_budget
        .max_structured_response_bytes
        .min(MAX_TASK_REVIEW_RESPONSE_BYTES);

    Ok((
        reviewer_choice,
        provider,
        executable,
        budgets,
        max_response_bytes,
    ))
}

fn verify_preserved_candidate_and_live_subject(
    plans_dir: &Path,
    request: &CoworkerTaskReviewStageRequest,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
) -> Result<(String, ReviewSubject), NativeError> {
    let attempt_no = request.attempt_no;
    if artifacts.task_id != candidate_task.task_id
        || artifacts.attempt_no != attempt_no
        || artifacts.run_id.trim().is_empty()
        || artifacts.worker_id.trim().is_empty()
        || artifacts.task_worktree_id.trim().is_empty()
        || artifacts.integration_worktree_id.trim().is_empty()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts has invalid task_id, attempt_no, run_id, worker_id, or worktree_id",
        ));
    }
    if let Some(attempt_ref) = candidate_task
        .attempts
        .iter()
        .find(|a| a.attempt_no == attempt_no)
    {
        if attempt_ref.run_id.as_deref() != Some(artifacts.run_id.as_str())
            || attempt_ref.worker_id.as_deref() != Some(artifacts.worker_id.as_str())
            || attempt_ref.task_worktree_id.as_deref() != Some(artifacts.task_worktree_id.as_str())
            || attempt_ref.base_commit != artifacts.execution_base_commit
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptRef on CoworkerTaskRecord does not match TaskAttemptExecutionArtifacts",
            ));
        }
    }
    if !artifacts.has_matching_coder_evidence(approved_task, &candidate_task.selected_choice) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Coder launch or observed evidence does not match the frozen task",
        ));
    }
    let expected_base_commit = if candidate_task.depends_on.is_empty() {
        adm.linkage.project_base.base_commit.clone()
    } else {
        candidate_task
            .dependency_input_commit
            .clone()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "dependent task is missing sealed dependency_input_commit",
                )
            })?
    };
    if artifacts.execution_base_commit != expected_base_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts.execution_base_commit does not match expected task base_commit",
        ));
    }

    let committed_head = artifacts.committed_head_commit.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts is missing committed_head_commit; run task verification first",
        )
    })?;
    let committed_tree = artifacts.committed_tree_sha.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts is missing committed_tree_sha; run task verification first",
        )
    })?;
    let candidate_id = artifacts.candidate_id.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts is missing candidate_id; run task verification first",
        )
    })?;
    if artifacts.check_evidence.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts has no passing check_evidence; Reviewer is blocked when checks fail or are unverified",
        ));
    }

    let test_summary = artifacts.test_evidence_summary.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts is missing test_evidence_summary",
        )
    })?;
    if !test_summary.all_checks_passed() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts test_evidence_summary does not report all checks passed",
        ));
    }

    let review_subject = artifacts.review_subject.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts is missing promoted ReviewSubject",
        )
    })?;
    review_subject.verify_integrity().map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("preserved ReviewSubject failed integrity validation: {err}"),
        )
    })?;

    let expected_write_paths: BTreeSet<String> =
        candidate_task.write_paths.iter().cloned().collect();
    if review_subject.allowed_write_paths != expected_write_paths
        || approved_task.allowed_write_paths != candidate_task.write_paths
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ReviewSubject allowed_write_paths does not match approved task write scope",
        ));
    }

    adm.linkage.project_base.verify_live_repository()?;
    let primary_repo = Path::new(&adm.linkage.project_base.repository_path);
    if review_subject.repository_path != primary_repo {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "ReviewSubject repository_path does not match job project_base.repository_path",
        ));
    }
    let primary_clean =
        is_worktree_clean(primary_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "failed to inspect primary repository cleanliness before Reviewer stage",
            )
        })?;
    if !primary_clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "primary repository has uncommitted or untracked changes",
        ));
    }

    let manifest =
        load_and_verify_manifest(&review_subject.work_area_root, &artifacts.task_worktree_id)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to verify owned task worktree manifest: {err}"),
                )
            })?;
    let int_manifest = load_and_verify_manifest(
        &review_subject.work_area_root,
        &artifacts.integration_worktree_id,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to verify owned integration worktree manifest: {err}"),
        )
    })?;
    if int_manifest.kind != WorktreeKind::RunIntegration
        || int_manifest.worktree_id != artifacts.integration_worktree_id
        || int_manifest.run_id != manifest.run_id
        || int_manifest.repository_path != manifest.repository_path
        || int_manifest.work_area_root != manifest.work_area_root
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "integration WorktreeManifest does not match task WorktreeManifest ownership/run scope",
        ));
    }
    if manifest.kind != WorktreeKind::Task
        || manifest.worktree_id != artifacts.task_worktree_id
        || manifest.work_area_root != review_subject.work_area_root
        || manifest.repository_path != primary_repo
        || manifest.repository_path != adm.approval.repository_path
        || manifest.base_commit != artifacts.execution_base_commit
        || manifest.allowed_write_paths != expected_write_paths
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "owned task worktree manifest does not match TaskAttemptExecutionArtifacts or frozen task scope",
        ));
    }

    artifacts.verify_coder_manifest_evidence(approved_task, &manifest, plans_dir)?;

    let expected_check_subject = FrozenCheckSubject {
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        job_id: request.job_id.clone(),
        target_kind: FrozenCheckTargetKind::TaskWorktree,
        repository_path: PathBuf::from(&adm.linkage.project_base.repository_path),
        work_area_root: manifest.work_area_root.clone(),
        worktree_id: manifest.worktree_id.clone(),
        expected_run_id: manifest.run_id.clone(),
        expected_task_id: manifest.task_id.clone(),
        expected_base_commit: manifest.base_commit.clone(),
        expected_head_commit: committed_head.clone(),
        expected_tree_sha: committed_tree.clone(),
    };
    expected_check_subject.validate()?;

    for ev in &artifacts.check_evidence {
        ev.validate_passed()?;
        let spec = adm
            .runtime_state
            .envelope
            .check_commands
            .get(&ev.check_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "CandidateCheckEvidence check `{}` is not present in frozen envelope check_commands",
                        ev.check_id
                    ),
                )
            })?;
        if !spec.required_for_task_review {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "CandidateCheckEvidence check `{}` is not configured with required_for_task_review",
                    ev.check_id
                ),
            ));
        }
        let check_effect = adm
            .runtime_state
            .pending_effects
            .values()
            .find(|eff| {
                eff.kind == PendingControllerEffectKind::CheckExecution
                    && eff.job_id == request.job_id
                    && eff.task_id.as_deref() == Some(candidate_task.task_id.as_str())
                    && eff.attempt_no == Some(attempt_no)
                    && eff.status == PendingControllerEffectStatus::EffectConfirmed
                    && eff.external_correlation_id.as_deref() == Some(ev.check_id.as_str())
            })
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "missing confirmed CheckExecution PendingControllerEffect for check `{}` on task `{}` attempt `{attempt_no}`",
                        ev.check_id, candidate_task.task_id
                    ),
                )
            })?;
        let check_report = read_persisted_check_report(plans_dir, &check_effect.effect_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    format!(
                        "missing persisted FrozenCheckExecutionReport for check `{}` effect `{}`",
                        ev.check_id, check_effect.effect_id
                    ),
                )
            })?;
        check_report.verify_against_context(&expected_check_subject, spec)?;
        let derived_ev = check_report.to_candidate_check_evidence(&expected_check_subject, spec)?;
        if &derived_ev != ev {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "persisted FrozenCheckExecutionReport for check `{}` does not match promoted CandidateCheckEvidence",
                    ev.check_id
                ),
            ));
        }
    }

    for (check_id, spec) in &adm.runtime_state.envelope.check_commands {
        if spec.required_for_task_review
            && !artifacts
                .check_evidence
                .iter()
                .any(|ev| &ev.check_id == check_id && ev.passed)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "required task review check `{check_id}` is missing from verified check_evidence"
                ),
            ));
        }
    }

    if &review_subject.task_head_commit != committed_head
        || &review_subject.task_tree_sha != committed_tree
        || review_subject.base_commit != artifacts.execution_base_commit
        || review_subject.base_commit != manifest.base_commit
        || review_subject.task_worktree_id != artifacts.task_worktree_id
        || review_subject.task_worktree_id != manifest.worktree_id
        || review_subject.run_id != manifest.run_id
        || review_subject.task_id.trim().is_empty()
        || manifest.task_id.as_deref() != Some(review_subject.task_id.as_str())
        || &review_subject.test_evidence != test_summary
        || review_subject.policy.policy_id != adm.runtime_state.envelope.policy.policy_id
        || review_subject.policy.requested_provider.as_deref()
            != approved_task.provider.map(WorkerProvider::as_str)
        || review_subject.policy.requested_model.as_deref()
            != Some(candidate_task.selected_choice.model_arg.as_str())
        || review_subject.policy.requested_effort != candidate_task.selected_choice.effort
        || review_subject.policy.observed_model.as_deref() != artifacts.observed_model.as_deref()
        || review_subject.policy.observed_effort != artifacts.observed_effort
        || !review_subject.policy.require_passing_tests
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preserved ReviewSubject does not match owned WorktreeManifest, TaskAttemptExecutionArtifacts commits/worktree/test evidence, or frozen policy binding",
        ));
    }

    let live_subject = inspect_review_subject_readonly(&InspectReviewSubjectRequest {
        work_area_root: review_subject.work_area_root.clone(),
        task_worktree_id: review_subject.task_worktree_id.clone(),
        policy: review_subject.policy.clone(),
        test_evidence: review_subject.test_evidence.clone(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("live task worktree failed read-only ReviewSubject inspection: {err}"),
        )
    })?;
    if &live_subject != review_subject {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live task worktree drifted from preserved ReviewSubject",
        ));
    }

    Ok((candidate_id.clone(), review_subject.clone()))
}

#[allow(clippy::too_many_arguments)]
fn verify_no_conflicting_review_key(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    task: &CoworkerTaskRecord,
    attempt_no: u32,
    candidate_id: &str,
    request_key: &str,
    effect_id: &str,
    subject_digest: &str,
) -> Result<(), NativeError> {
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::ReviewerInvocation
            && eff.task_id.as_deref() == Some(task.task_id.as_str())
            && eff.attempt_no == Some(attempt_no)
            && (eff.effect_id != effect_id
                || eff.request_key != request_key
                || !constant_time_str_eq(&eff.subject_digest, subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "conflicting ReviewerInvocation PendingControllerEffect `{}` already exists for task `{}` attempt `{attempt_no}`",
                    eff.effect_id, task.task_id
                ),
            ));
        }
    }

    for outbox in &job.outbox {
        if outbox.kind == OutboxDispatchKind::TaskReviewDispatch
            && outbox.task_id == task.task_id
            && outbox.attempt_no == attempt_no
            && (outbox.request_key != request_key
                || !constant_time_str_eq(&outbox.subject_digest, subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "conflicting TaskReviewDispatch outbox `{}` already exists for task `{}` attempt `{attempt_no}`",
                    outbox.outbox_id, task.task_id
                ),
            ));
        }
    }

    for cand in &task.candidates {
        if (cand.attempt_no == attempt_no || cand.candidate_id == candidate_id)
            && (cand.candidate_id != candidate_id
                || cand.review_subject_digest.as_deref() != Some(subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "conflicting TaskCandidateRef `{}` already recorded for task `{}` attempt `{attempt_no}`",
                    cand.candidate_id, task.task_id
                ),
            ));
        }
    }

    Ok(())
}

fn find_task_review_outbox(
    job: &CoworkerJobRecord,
    task_id: &str,
    request_key: &str,
) -> Option<OutboxDispatchRecord> {
    job.outbox
        .iter()
        .find(|o| {
            o.task_id == task_id
                && o.kind == OutboxDispatchKind::TaskReviewDispatch
                && o.request_key == request_key
        })
        .cloned()
}

#[allow(clippy::too_many_arguments)]
fn try_reopen_completed_review_stage(
    plans_dir: &Path,
    request: &CoworkerTaskReviewStageRequest,
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
    candidate_id: &str,
    review_subject: &ReviewSubject,
    expected_identity: &ExpectedTaskReviewIdentity,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
) -> Result<Option<CoworkerTaskReviewStageOutcome>, NativeError> {
    let Some(recorded_candidate) = task
        .candidates
        .iter()
        .find(|c| c.candidate_id == candidate_id && c.attempt_no == request.attempt_no)
        .cloned()
    else {
        return Ok(None);
    };
    let Some(recorded_verdict) = recorded_candidate.review_verdict.clone() else {
        return Ok(None);
    };

    if task.unresolved_outbox_id.is_some() {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` has an unresolved outbox while candidate `{candidate_id}` is recorded",
                task.task_id
            ),
        ));
    }

    let outbox = find_task_review_outbox(job, &request.task_id, request_key).ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "recorded TaskCandidateRef is missing its corresponding TaskReviewDispatch outbox record",
        )
    })?;
    if !matches!(
        outbox.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed | OutboxDispatchStatus::ReconciledConfirmed
    ) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "recorded TaskCandidateRef has an unconfirmed TaskReviewDispatch outbox status",
        ));
    }

    let report = read_persisted_task_review_report(plans_dir, effect_id)?.ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "missing persisted private 0600 TaskReviewExecutionReport for completed effect `{effect_id}`"
            ),
        )
    })?;
    let promoted = report.promote_verified_verdict(review_subject, expected_identity)?;
    if promoted != recorded_verdict {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "persisted TaskReviewExecutionReport verdict does not match ledger StructuredReviewVerdict",
        ));
    }

    let response_digest = report
        .advisory_response()
        .map(|r| r.sha256_digest.clone())
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "persisted TaskReviewExecutionReport is missing bounded advisory_response digest",
            )
        })?;
    if artifacts.reviewer_response_digest.as_deref() != Some(response_digest.as_str()) {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "TaskAttemptExecutionArtifacts.reviewer_response_digest does not match persisted report digest",
        ));
    }

    let expected_stage = recorded_verdict.target_task_stage();
    if task.stage != expected_stage {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` stage `{}` does not match recorded verdict target stage `{expected_stage}`",
                task.task_id, task.stage
            ),
        ));
    }

    let verified_receipt = if recorded_verdict.outcome == ReviewVerdictOutcome::Accept {
        let receipt_id = recorded_candidate
            .review_receipt_id
            .as_ref()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "accepted TaskCandidateRef is missing review_receipt_id",
                )
            })?;
        let art_receipt = artifacts.review_receipt.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts is missing review_receipt for accepted candidate",
            )
        })?;
        let disk_receipt = load_review_receipt(&review_subject.work_area_root, receipt_id)
            .map_err(|err| {
                let code = match err {
                    OperationsError::SecurityViolation(_) => NativeErrorCode::SecurityViolation,
                    OperationsError::Json(_) | OperationsError::ReceiptInvalidated(_) => {
                        NativeErrorCode::ProtocolError
                    }
                    _ => NativeErrorCode::ValidationFailed,
                };
                NativeError::new(
                    code,
                    format!("failed to load persisted ReviewReceipt `{receipt_id}`: {err}"),
                )
            })?;
        if art_receipt != &disk_receipt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts.review_receipt does not match on-disk ReviewReceipt",
            ));
        }
        verify_review_receipt_readonly(review_subject, &disk_receipt).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("ReviewReceipt `{receipt_id}` failed live subject verification: {err}"),
            )
        })?;
        Some(disk_receipt)
    } else {
        if recorded_candidate.review_receipt_id.is_some() || artifacts.review_receipt.is_some() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "non-Accept candidate must not carry a ReviewReceipt",
            ));
        }
        None
    };

    let (status, hold_id, blocking_error) =
        classify_verdict_stage_disposition(job, &recorded_verdict);

    Ok(Some(CoworkerTaskReviewStageOutcome {
        job_id: request.job_id.clone(),
        task_id: request.task_id.clone(),
        attempt_no: request.attempt_no,
        task_stage: task.stage,
        status,
        candidate_id: candidate_id.to_string(),
        request_key: request_key.to_string(),
        effect_id: effect_id.to_string(),
        outbox_id: outbox.outbox_id,
        report_artifact_id: report_artifact_id.to_string(),
        report_path: review_report_file_path(plans_dir, effect_id)
            .to_string_lossy()
            .to_string(),
        report,
        reviewer_response_digest: response_digest,
        verdict: recorded_verdict,
        candidate: recorded_candidate,
        review_receipt: verified_receipt,
        hold_id,
        blocking_error,
        reused_existing: true,
    }))
}

#[allow(clippy::too_many_arguments)]
fn recover_existing_review_invocation(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    plans_dir: &Path,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    candidate_task: &CoworkerTaskRecord,
    review_subject: &ReviewSubject,
    expected_identity: &ExpectedTaskReviewIdentity,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
    existing_effect: Option<&PendingControllerEffect>,
    existing_outbox: Option<OutboxDispatchRecord>,
    persisted_report: Option<TaskReviewExecutionReport>,
) -> Result<(TaskReviewExecutionReport, PathBuf, OutboxDispatchRecord), NativeError> {
    let Some(report) = persisted_report else {
        if let Some(ref obx) = existing_outbox {
            if obx.status.blocks_replay_and_new_dispatch() {
                mark_review_effect_and_outbox_unknown(
                    controller,
                    request,
                    controller_instance_id,
                    lease_epoch,
                    effect_id,
                    &obx.outbox_id,
                    None,
                    None,
                    None,
                    "unresolved TaskReviewDispatch outbox has no persisted private report; blind replay refused",
                );
            }
        } else if existing_effect.is_some() {
            let _ = controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectUnknown;
                        eff.diagnostic = Some(
                            "PendingControllerEffect has no persisted private report; blind replay refused"
                                .to_string(),
                        );
                        eff.updated_at_ms = request.now_ms;
                    }
                    Ok(())
                },
            );
        }
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` attempt `{}` has an unresolved Reviewer reservation without a verified persisted report; blind replay is forbidden",
                request.task_id, request.attempt_no
            ),
        ));
    };

    if report.binding_id != request.binding_id
        || report.thread_uuid != request.thread_uuid
        || report.job_id != request.job_id
        || &report.subject != review_subject
        || &report.expected_identity != expected_identity
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "persisted TaskReviewExecutionReport identity or subject does not match current review request",
        ));
    }

    let outbox_record = if let Some(obx) = existing_outbox {
        obx
    } else if candidate_task.stage == CoworkerTaskStage::ReadyForReview {
        reserve_review_effect_and_outbox(
            controller,
            guard,
            request,
            adm,
            controller_instance_id,
            lease_epoch,
            review_subject,
            request_key,
            effect_id,
        )?
    } else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is in stage `{}` without a matching TaskReviewDispatch outbox record",
                candidate_task.task_id, candidate_task.stage
            ),
        ));
    };

    let _ = report_artifact_id;
    Ok((
        report,
        review_report_file_path(plans_dir, effect_id),
        outbox_record,
    ))
}

#[allow(clippy::too_many_arguments)]
fn reserve_review_effect_and_outbox(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    review_subject: &ReviewSubject,
    request_key: &str,
    effect_id: &str,
) -> Result<OutboxDispatchRecord, NativeError> {
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if !state.pending_effects.contains_key(effect_id) {
                state.reserve_role_call(WorkflowRole::Reviewer)?;
                state.pending_effects.insert(
                    effect_id.to_string(),
                    PendingControllerEffect {
                        effect_id: effect_id.to_string(),
                        request_key: request_key.to_string(),
                        job_id: request.job_id.clone(),
                        task_id: Some(request.task_id.clone()),
                        attempt_no: Some(request.attempt_no),
                        kind: PendingControllerEffectKind::ReviewerInvocation,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: review_subject.subject_digest.clone(),
                        external_correlation_id: None,
                        created_at_ms: request.now_ms,
                        updated_at_ms: request.now_ms,
                        diagnostic: None,
                    },
                );
            }
            Ok(())
        },
    )?;

    if let Some(existing) = find_task_review_outbox(&adm.job, &request.task_id, request_key) {
        return Ok(existing);
    }

    let (_, outbox) =
        controller
            .workflow_store()
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: adm.job.job_revision,
                expected_ledger_revision: None,
                request_key: request_key.to_string(),
                kind: OutboxDispatchKind::TaskReviewDispatch,
                task_id: request.task_id.clone(),
                role: WorkflowRole::Reviewer,
                subject_digest: review_subject.subject_digest.clone(),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
            })?;
    Ok(outbox)
}

#[allow(clippy::too_many_arguments)]
fn finalize_and_promote_review_report(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    plans_dir: &Path,
    controller_instance_id: &str,
    lease_epoch: u64,
    candidate_id: &str,
    review_subject: &ReviewSubject,
    expected_identity: &ExpectedTaskReviewIdentity,
    reviewer_choice: &SelectedRoleChoice,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
    outbox_record: &OutboxDispatchRecord,
    report: TaskReviewExecutionReport,
    report_path: PathBuf,
) -> Result<CoworkerTaskReviewStageOutcome, NativeError> {
    let response_digest_opt = report.advisory_response().map(|r| r.sha256_digest.clone());
    let duration_ms = report.execution().duration_ms;

    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    match report.status {
        TaskReviewExecutionStatus::UnknownOutcome => {
            let diag = report
                .diagnostic
                .clone()
                .unwrap_or_else(|| "Reviewer execution finished with unknown outcome".to_string());
            mark_review_effect_and_outbox_unknown(
                controller,
                request,
                controller_instance_id,
                lease_epoch,
                effect_id,
                &outbox_record.outbox_id,
                None,
                None,
                Some(report_artifact_id.to_string()),
                &diag,
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
        TaskReviewExecutionStatus::KnownTerminalFailure
        | TaskReviewExecutionStatus::SubjectMutatedOrDrifted
        | TaskReviewExecutionStatus::OwnershipLostOrDrifted => {
            let diag = sanitize_diagnostic_text(&report.diagnostic.clone().unwrap_or_else(|| {
                format!(
                    "Reviewer execution failed with status `{}`",
                    report.status.as_str()
                )
            }));
            controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    let should_record_completion =
                        state.pending_effects.get(effect_id).is_some_and(|eff| {
                            matches!(
                                eff.status,
                                PendingControllerEffectStatus::ReservedBeforeEffect
                                    | PendingControllerEffectStatus::EffectUnknown
                            )
                        });
                    if should_record_completion {
                        state.record_role_completion(WorkflowRole::Reviewer, duration_ms);
                    }
                    if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectFailedTerminal;
                        eff.external_correlation_id = Some(report_artifact_id.to_string());
                        eff.diagnostic = Some(diag.clone());
                        eff.updated_at_ms = request.now_ms;
                    }
                    let art_key = ControllerJobRuntimeState::artifact_key(
                        &request.task_id,
                        request.attempt_no,
                    );
                    if let (Some(art), Some(ref resp_dig)) = (
                        state.attempt_artifacts.get_mut(&art_key),
                        &response_digest_opt,
                    ) {
                        art.reviewer_response_digest = Some(resp_dig.clone());
                    }
                    Ok(())
                },
            )?;

            let current_ledger = controller
                .workflow_store()
                .read_binding_ledger(&request.binding_id)?;
            let current_job = current_ledger
                .jobs
                .iter()
                .find(|j| j.job_id == request.job_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!("job `{}` was not found in binding ledger", request.job_id),
                    )
                })?;
            let terminal_outbox_status =
                if outbox_record.status == OutboxDispatchStatus::EffectUnknown {
                    OutboxDispatchStatus::ReconciledConfirmed
                } else {
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                };
            let _ = controller.workflow_store().reconcile_or_finalize_outbox(
                &FinalizeTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: current_job.job_revision,
                    expected_ledger_revision: None,
                    outbox_id: outbox_record.outbox_id.clone(),
                    status: terminal_outbox_status,
                    next_task_stage: CoworkerTaskStage::Blocked,
                    run_id: None,
                    worker_id: None,
                    task_worktree_id: Some(review_subject.task_worktree_id.clone()),
                    head_commit: Some(review_subject.task_head_commit.clone()),
                    tree_sha: Some(review_subject.task_tree_sha.clone()),
                    diff_digest: Some(review_subject.diff_digest.clone()),
                    report_artifact_id: Some(report_artifact_id.to_string()),
                    observed_model: report.observed_model.clone(),
                    observed_effort: report.observed_effort.clone(),
                    receipt_id: None,
                    diagnostic: Some(diag.clone()),
                },
            )?;

            let code = if report.status == TaskReviewExecutionStatus::SubjectMutatedOrDrifted
                || report.status == TaskReviewExecutionStatus::OwnershipLostOrDrifted
            {
                NativeErrorCode::SecurityViolation
            } else {
                NativeErrorCode::ValidationFailed
            };
            return Err(NativeError::new(code, diag));
        }
        TaskReviewExecutionStatus::VerifiedVerdict => {}
    }

    let response_digest = response_digest_opt.ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "VerifiedVerdict report is missing bounded advisory_response digest",
        )
    })?;
    validate_sha256_hex_digest(&response_digest, "reviewer_response_digest").map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "reviewer_response_digest must be a 64-character lowercase hex SHA-256",
        )
    })?;

    // Preserve late response digest evidence before checking advancement fence.
    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            let should_record_completion =
                state.pending_effects.get(effect_id).is_some_and(|eff| {
                    matches!(
                        eff.status,
                        PendingControllerEffectStatus::ReservedBeforeEffect
                            | PendingControllerEffectStatus::EffectUnknown
                    )
                });
            if should_record_completion {
                state.record_role_completion(WorkflowRole::Reviewer, duration_ms);
            }
            if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                eff.status = PendingControllerEffectStatus::EffectConfirmed;
                eff.external_correlation_id = Some(report_artifact_id.to_string());
                eff.updated_at_ms = request.now_ms;
            }
            let art_key =
                ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
            if let Some(art) = state.attempt_artifacts.get_mut(&art_key) {
                art.reviewer_response_digest = Some(response_digest.clone());
            }
            Ok(())
        },
    )?;

    if let Some(fenced_err) = reconcile_verified_review_outbox_under_registry_hold(
        controller,
        guard,
        request,
        plans_dir,
        controller_instance_id,
        lease_epoch,
        candidate_id,
        review_subject,
        expected_identity,
        reviewer_choice,
        request_key,
        effect_id,
        report_artifact_id,
        outbox_record,
        &report,
        &response_digest,
    )? {
        return Err(fenced_err);
    }

    let (post_adm, _, _, post_task, _, post_artifacts, _, _, _, _, _, _, _) =
        verify_live_task_review_gate(
            controller,
            guard,
            request,
            Some((controller_instance_id, lease_epoch)),
        )?;

    verify_task_fenced_for_advancement(
        &post_task,
        &post_adm.job,
        &post_adm.runtime_state,
        &post_adm.approval.approval_id,
        plans_dir,
    )?;

    let verdict = report.promote_verified_verdict(review_subject, expected_identity)?;

    let job_in_under_review = if post_task.stage == CoworkerTaskStage::ReviewReserved
        || post_task.stage == CoworkerTaskStage::EffectUnknown
    {
        let outbox_status = if outbox_record.status == OutboxDispatchStatus::EffectUnknown {
            OutboxDispatchStatus::ReconciledConfirmed
        } else {
            OutboxDispatchStatus::DispatchedEffectConfirmed
        };
        let (updated_job, _) = controller.workflow_store().reconcile_or_finalize_outbox(
            &FinalizeTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: post_adm.job.job_revision,
                expected_ledger_revision: None,
                outbox_id: outbox_record.outbox_id.clone(),
                status: outbox_status,
                next_task_stage: CoworkerTaskStage::UnderReview,
                run_id: None,
                worker_id: None,
                task_worktree_id: Some(review_subject.task_worktree_id.clone()),
                head_commit: Some(review_subject.task_head_commit.clone()),
                tree_sha: Some(review_subject.task_tree_sha.clone()),
                diff_digest: Some(review_subject.diff_digest.clone()),
                report_artifact_id: Some(report_artifact_id.to_string()),
                observed_model: report.observed_model.clone(),
                observed_effort: report.observed_effort.clone(),
                receipt_id: None,
                diagnostic: None,
            },
        )?;
        updated_job
    } else if post_task.stage == CoworkerTaskStage::UnderReview {
        post_adm.job.clone()
    } else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is in unexpected stage `{}` before verdict recording",
                post_task.task_id, post_task.stage
            ),
        ));
    };

    // Re-verify fence and physical ownership right before minting receipt and recording verdict.
    let (pre_verdict_adm, _, _, pre_verdict_task, _, _, _, _, _, _, _, _, _) =
        verify_live_task_review_gate(
            controller,
            guard,
            request,
            Some((controller_instance_id, lease_epoch)),
        )?;
    verify_task_fenced_for_advancement(
        &pre_verdict_task,
        &pre_verdict_adm.job,
        &pre_verdict_adm.runtime_state,
        &pre_verdict_adm.approval.approval_id,
        plans_dir,
    )?;

    let review_receipt = if verdict.outcome == ReviewVerdictOutcome::Accept {
        let receipt = mint_or_load_verified_review_receipt(
            &job_in_under_review.job_id,
            &request.task_id,
            request.attempt_no,
            candidate_id,
            &verdict,
            review_subject,
            &reviewer_choice.model_id,
            request.now_ms,
        )?;
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            |state| {
                let art_key =
                    ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
                let art = state.attempt_artifacts.get_mut(&art_key).ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskAttemptExecutionArtifacts missing during ReviewReceipt persistence",
                    )
                })?;
                art.reviewer_response_digest = Some(response_digest.clone());
                art.review_receipt = Some(receipt.clone());
                Ok(())
            },
        )?;
        Some(receipt)
    } else {
        None
    };

    let candidate_ref = TaskCandidateRef {
        candidate_id: candidate_id.to_string(),
        task_id: request.task_id.clone(),
        attempt_no: request.attempt_no,
        base_commit: review_subject.base_commit.clone(),
        head_commit: review_subject.task_head_commit.clone(),
        tree_sha: review_subject.task_tree_sha.clone(),
        diff_digest: review_subject.diff_digest.clone(),
        write_paths: review_subject.allowed_write_paths.iter().cloned().collect(),
        check_evidence_digests: post_artifacts
            .check_evidence
            .iter()
            .map(|ev| ev.evidence_digest.clone())
            .collect(),
        review_subject_digest: Some(review_subject.subject_digest.clone()),
        review_verdict: Some(verdict.clone()),
        review_receipt_id: review_receipt.as_ref().map(|r| r.receipt_id.clone()),
        integration_receipt_id: None,
    };

    let (final_job, recorded_candidate) = controller
        .workflow_store()
        .record_task_candidate_and_verdict(&RecordTaskReviewVerdictRequest {
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            expected_job_revision: pre_verdict_adm.job.job_revision,
            expected_ledger_revision: None,
            candidate: candidate_ref,
        })?;

    let final_task = final_job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "reviewed task missing after record_task_candidate_and_verdict",
            )
        })?;
    let (status, hold_id, blocking_error) =
        classify_verdict_stage_disposition(&final_job, &verdict);

    Ok(CoworkerTaskReviewStageOutcome {
        job_id: request.job_id.clone(),
        task_id: request.task_id.clone(),
        attempt_no: request.attempt_no,
        task_stage: final_task.stage,
        status,
        candidate_id: candidate_id.to_string(),
        request_key: request_key.to_string(),
        effect_id: effect_id.to_string(),
        outbox_id: outbox_record.outbox_id.clone(),
        report_artifact_id: report_artifact_id.to_string(),
        report_path: report_path.to_string_lossy().to_string(),
        report,
        reviewer_response_digest: response_digest,
        verdict,
        candidate: recorded_candidate,
        review_receipt,
        hold_id,
        blocking_error,
        reused_existing: false,
    })
}

const MAX_REVIEW_OUTBOX_HOLD_RECONCILE_ATTEMPTS: usize = 4;

fn retry_on_stale_job_revision_cas<T, F>(mut op: F) -> Result<T, NativeError>
where
    F: FnMut() -> Result<T, NativeError>,
{
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match op() {
            Err(err)
                if attempt < MAX_REVIEW_OUTBOX_HOLD_RECONCILE_ATTEMPTS
                    && err.code == NativeErrorCode::ValidationFailed
                    && err.message
                        == "expected_job_revision does not match current job_revision" =>
            {
                continue;
            }
            other => return other,
        }
    }
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn reconcile_verified_review_outbox_under_registry_hold(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    plans_dir: &Path,
    controller_instance_id: &str,
    lease_epoch: u64,
    candidate_id: &str,
    review_subject: &ReviewSubject,
    expected_identity: &ExpectedTaskReviewIdentity,
    reviewer_choice: &SelectedRoleChoice,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
    outbox_record: &OutboxDispatchRecord,
    report: &TaskReviewExecutionReport,
    response_digest: &str,
) -> Result<Option<NativeError>, NativeError> {
    if report.status != TaskReviewExecutionStatus::VerifiedVerdict {
        return Ok(None);
    }
    retry_on_stale_job_revision_cas(|| {
        reconcile_verified_review_outbox_under_registry_hold_once(
            controller,
            guard,
            request,
            plans_dir,
            controller_instance_id,
            lease_epoch,
            candidate_id,
            review_subject,
            expected_identity,
            reviewer_choice,
            request_key,
            effect_id,
            report_artifact_id,
            outbox_record,
            report,
            response_digest,
        )
    })
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn reconcile_verified_review_outbox_under_registry_hold_once(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskReviewStageRequest,
    plans_dir: &Path,
    controller_instance_id: &str,
    lease_epoch: u64,
    candidate_id: &str,
    review_subject: &ReviewSubject,
    expected_identity: &ExpectedTaskReviewIdentity,
    reviewer_choice: &SelectedRoleChoice,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
    outbox_record: &OutboxDispatchRecord,
    report: &TaskReviewExecutionReport,
    response_digest: &str,
) -> Result<Option<NativeError>, NativeError> {
    if report.status != TaskReviewExecutionStatus::VerifiedVerdict {
        return Ok(None);
    }

    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let mut adm = Box::new(controller.verify_job_admission(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?);

    let own_instance_id = deterministic_controller_instance_id(
        &request.binding_id,
        &request.job_id,
        guard.instance_id(),
    );
    let lease = adm.runtime_state.lease.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ControllerJobRuntimeState has no active driver lease",
        )
    })?;
    if !lease.active
        || lease.instance_id != own_instance_id
        || lease.lease_epoch == 0
        || lease.expires_at_ms <= lease.acquired_at_ms
        || lease.instance_id != controller_instance_id
        || lease.lease_epoch != lease_epoch
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "controller driver lease changed during task review stage",
        ));
    }

    if adm.runtime_state.stop_requested {
        return Ok(None);
    }

    let change_summary = ConversationChangeStore::open_readonly(plans_dir)
        .and_then(|store| {
            store.unresolved_blocking_changes(
                &request.binding_id,
                Some(&adm.approval.approval_id),
                None,
            )
        })
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to read ConversationChangeStore during task review stage: {err}"),
            )
        })?;
    if change_summary.current_change_revision > adm.runtime_state.synced_change_revision {
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            |state| {
                if change_summary.current_change_revision > state.synced_change_revision {
                    state.synced_change_revision = change_summary.current_change_revision;
                }
                Ok(())
            },
        )?;
        adm.runtime_state.synced_change_revision = change_summary.current_change_revision;
    }
    if change_summary.approval_superseded || !change_summary.blocking_changes.is_empty() {
        return Ok(None);
    }

    let has_active_registry_hold = adm.job.holds.iter().any(|h| {
        h.active
            && h.reason == WorkflowHoldReason::RegistryConfigurationChanged
            && (h.holds_entire_job
                || h.directly_affected_task_ids
                    .iter()
                    .any(|id| id == &request.task_id))
    });
    let has_other_active_hold = adm
        .job
        .holds
        .iter()
        .any(|h| h.active && h.reason != WorkflowHoldReason::RegistryConfigurationChanged);
    if !has_active_registry_hold || has_other_active_hold || adm.job.phase != CoworkerJobPhase::Held
    {
        return Ok(None);
    }

    let candidate_task = adm
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` was not found in job `{}`",
                    request.task_id, request.job_id
                ),
            )
        })?;
    let approved_task = adm
        .approval
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` was not found in ApprovalRecord `{}`",
                    request.task_id, adm.approval.approval_id
                ),
            )
        })?;
    if candidate_task.current_attempt_no != request.attempt_no {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` current_attempt_no `{}` does not match requested attempt_no `{}`",
                request.task_id, candidate_task.current_attempt_no, request.attempt_no
            ),
        ));
    }
    if candidate_task.stage != CoworkerTaskStage::Held
        || candidate_task.active_hold_ids.is_empty()
        || candidate_task.stage_before_hold.is_none()
    {
        return Ok(None);
    }

    if &adm.runtime_state.envelope.reviewer_choice != reviewer_choice {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "reviewer_choice does not match frozen execution envelope",
        ));
    }
    validate_non_native_worker_choice(WorkflowRole::Reviewer, reviewer_choice)?;
    if !adm.linkage.authorizes_reviewer_dispatch(reviewer_choice) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen approval linkage does not authorize the envelope reviewer_choice",
        ));
    }
    if !adm.runtime_state.envelope.policy.allows(
        WorkflowRole::Reviewer,
        &reviewer_choice.tool_id,
        &reviewer_choice.model_id,
        reviewer_choice.effort.as_deref(),
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen execution policy denies the envelope reviewer_choice",
        ));
    }

    let art_key = ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
    let artifacts = adm
        .runtime_state
        .attempt_artifacts
        .get(&art_key)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing TaskAttemptExecutionArtifacts for task `{}` attempt `{}`",
                    request.task_id, request.attempt_no
                ),
            )
        })?;
    let (verified_candidate_id, verified_review_subject) =
        verify_preserved_candidate_and_live_subject(
            plans_dir,
            request,
            &adm,
            &candidate_task,
            &approved_task,
            &artifacts,
        )?;
    if verified_candidate_id != candidate_id || &verified_review_subject != review_subject {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preserved candidate_id or ReviewSubject drifted from review invocation context",
        ));
    }

    let recomputed_identity = ExpectedTaskReviewIdentity::from_subject(
        review_subject,
        &request.task_id,
        request.attempt_no,
        candidate_id,
        &adm.linkage.policy_digest,
    );
    let recomputed_request_key = compute_task_review_request_key(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        candidate_id,
        &review_subject.subject_digest,
        &adm.linkage.policy_digest,
        &adm.runtime_state.envelope.envelope_digest,
        reviewer_choice,
    );
    let recomputed_effect_id = compute_task_review_effect_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &recomputed_request_key,
    );
    let recomputed_report_artifact_id = compute_task_review_report_artifact_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &recomputed_request_key,
    );
    if &recomputed_identity != expected_identity
        || recomputed_request_key != request_key
        || recomputed_effect_id != effect_id
        || recomputed_report_artifact_id != report_artifact_id
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "review identity, request_key, effect_id, or report_artifact_id does not match frozen authority",
        ));
    }

    verify_no_conflicting_review_key(
        &adm.job,
        &adm.runtime_state,
        &candidate_task,
        request.attempt_no,
        candidate_id,
        request_key,
        effect_id,
        &review_subject.subject_digest,
    )?;

    report.validate_integrity()?;
    if report.binding_id != request.binding_id
        || report.thread_uuid != request.thread_uuid
        || report.job_id != request.job_id
        || &report.subject != review_subject
        || &report.expected_identity != expected_identity
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskReviewExecutionReport identity or subject does not match current review request",
        ));
    }
    let persisted_report = read_persisted_task_review_report(plans_dir, effect_id)?.ok_or_else(
        || {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "missing persisted private 0600 TaskReviewExecutionReport for effect `{effect_id}`"
                ),
            )
        },
    )?;
    if &persisted_report != report {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "persisted TaskReviewExecutionReport does not match VerifiedVerdict report",
        ));
    }
    let persisted_digest = persisted_report
        .advisory_response()
        .map(|r| r.sha256_digest.as_str())
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "persisted VerifiedVerdict report is missing bounded advisory_response digest",
            )
        })?;
    if !constant_time_str_eq(persisted_digest, response_digest)
        || artifacts.reviewer_response_digest.as_deref() != Some(response_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "persisted reviewer_response_digest does not match VerifiedVerdict report digest",
        ));
    }

    let _validated_verdict = report.promote_verified_verdict(review_subject, expected_identity)?;

    let confirmed_effect = adm
        .runtime_state
        .pending_effects
        .get(effect_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing ReviewerInvocation PendingControllerEffect `{effect_id}` in runtime state"
                ),
            )
        })?;
    if confirmed_effect.kind != PendingControllerEffectKind::ReviewerInvocation
        || confirmed_effect.job_id != request.job_id
        || confirmed_effect.task_id.as_deref() != Some(request.task_id.as_str())
        || confirmed_effect.attempt_no != Some(request.attempt_no)
        || confirmed_effect.request_key != request_key
        || !constant_time_str_eq(
            &confirmed_effect.subject_digest,
            &review_subject.subject_digest,
        )
        || confirmed_effect.status != PendingControllerEffectStatus::EffectConfirmed
        || confirmed_effect.external_correlation_id.as_deref() != Some(report_artifact_id)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "ReviewerInvocation PendingControllerEffect `{effect_id}` does not match confirmed VerifiedVerdict identity"
            ),
        ));
    }

    let live_outbox =
        find_task_review_outbox(&adm.job, &request.task_id, request_key).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "missing matching TaskReviewDispatch outbox record for held review invocation",
            )
        })?;
    if live_outbox.outbox_id != outbox_record.outbox_id
        || live_outbox.task_id != request.task_id
        || live_outbox.attempt_no != request.attempt_no
        || live_outbox.kind != OutboxDispatchKind::TaskReviewDispatch
        || live_outbox.role != WorkflowRole::Reviewer
        || live_outbox.request_key != request_key
        || !constant_time_str_eq(&live_outbox.subject_digest, &review_subject.subject_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskReviewDispatch outbox identity does not match VerifiedVerdict review invocation",
        ));
    }

    if outbox_record.status == OutboxDispatchStatus::EffectUnknown
        || live_outbox.status == OutboxDispatchStatus::EffectUnknown
    {
        return Ok(None);
    }

    let final_job = match live_outbox.status {
        OutboxDispatchStatus::ReservedBeforeEffect => {
            if outbox_record.status != OutboxDispatchStatus::ReservedBeforeEffect {
                return Ok(None);
            }
            if candidate_task.unresolved_outbox_id.as_deref()
                != Some(live_outbox.outbox_id.as_str())
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "held task `{}` unresolved_outbox_id `{:?}` does not match ReservedBeforeEffect outbox `{}`",
                        candidate_task.task_id,
                        candidate_task.unresolved_outbox_id,
                        live_outbox.outbox_id
                    ),
                ));
            }
            controller.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;
            let (updated_job, finalized_outbox) = controller
                .workflow_store()
                .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: adm.job.job_revision,
                    expected_ledger_revision: None,
                    outbox_id: live_outbox.outbox_id.clone(),
                    status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                    next_task_stage: CoworkerTaskStage::Held,
                    run_id: None,
                    worker_id: None,
                    task_worktree_id: Some(review_subject.task_worktree_id.clone()),
                    head_commit: Some(review_subject.task_head_commit.clone()),
                    tree_sha: Some(review_subject.task_tree_sha.clone()),
                    diff_digest: Some(review_subject.diff_digest.clone()),
                    report_artifact_id: Some(report_artifact_id.to_string()),
                    observed_model: report.observed_model.clone(),
                    observed_effort: report.observed_effort.clone(),
                    receipt_id: None,
                    diagnostic: None,
                })?;
            if finalized_outbox.status != OutboxDispatchStatus::DispatchedEffectConfirmed {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "finalized TaskReviewDispatch outbox did not transition to DispatchedEffectConfirmed",
                ));
            }
            updated_job
        }
        OutboxDispatchStatus::DispatchedEffectConfirmed => {
            if candidate_task.unresolved_outbox_id.is_some()
                || live_outbox.run_id.is_some()
                || live_outbox.worker_id.is_some()
                || live_outbox.receipt_id.is_some()
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "already-confirmed TaskReviewDispatch outbox has inconsistent held task or outbox fields",
                ));
            }
            adm.job.clone()
        }
        _ => return Ok(None),
    };

    let final_task = final_job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "held task missing after TaskReviewDispatch outbox reconciliation",
            )
        })?;
    if final_task.stage != CoworkerTaskStage::Held
        || final_task.stage_before_hold != candidate_task.stage_before_hold
        || final_task.unresolved_outbox_id.is_some()
        || final_job.holds != adm.job.holds
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "held task stage, stage_before_hold, or workflow holds drifted during outbox finalization",
        ));
    }

    match verify_task_fenced_for_advancement(
        final_task,
        &final_job,
        &adm.runtime_state,
        &adm.approval.approval_id,
        plans_dir,
    ) {
        Err(fenced_err) => Ok(Some(fenced_err)),
        Ok(()) => Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is held under RegistryConfigurationChanged; review promotion is fenced",
                request.task_id
            ),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn mint_or_load_verified_review_receipt(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    candidate_id: &str,
    verdict: &StructuredReviewVerdict,
    review_subject: &ReviewSubject,
    reviewer_model_id: &str,
    now_ms: u64,
) -> Result<ReviewReceipt, NativeError> {
    let receipt_id = deterministic_prefixed_id(
        "threadlane-native:coworker-review-receipt:v1",
        "rev",
        &[
            job_id,
            task_id,
            &attempt_no.to_string(),
            candidate_id,
            &verdict.verdict_id,
            &review_subject.subject_digest,
        ],
    );
    match load_review_receipt(&review_subject.work_area_root, &receipt_id) {
        Ok(existing) => {
            verify_review_receipt(review_subject, &existing).map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    format!("existing ReviewReceipt `{receipt_id}` failed verification: {err}"),
                )
            })?;
            return Ok(existing);
        }
        Err(OperationsError::Io(ref io_err)) if io_err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            let code = match err {
                OperationsError::SecurityViolation(_) => NativeErrorCode::SecurityViolation,
                OperationsError::Json(_) | OperationsError::ReceiptInvalidated(_) => {
                    NativeErrorCode::ProtocolError
                }
                _ => NativeErrorCode::ValidationFailed,
            };
            return Err(NativeError::new(
                code,
                format!(
                    "existing ReviewReceipt `{receipt_id}` is corrupt or invalid and cannot be overwritten: {err}"
                ),
            ));
        }
    }

    let review_note = verdict
        .prose_summary
        .clone()
        .unwrap_or_else(|| format!("Accepted candidate {candidate_id} ({})", verdict.verdict_id));
    let receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: review_subject.clone(),
        expected_subject_digest: review_subject.subject_digest.clone(),
        receipt_id: Some(receipt_id),
        reviewer_id: reviewer_model_id.to_string(),
        review_note,
        approved_at_ms: i64::try_from(now_ms).unwrap_or(i64::MAX),
    })
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to mint ReviewReceipt via accept_review_subject: {err}"),
        )
    })?;
    Ok(receipt)
}

fn classify_verdict_stage_disposition(
    job: &CoworkerJobRecord,
    verdict: &StructuredReviewVerdict,
) -> (
    CoworkerTaskReviewStageStatus,
    Option<String>,
    Option<String>,
) {
    match verdict.outcome {
        ReviewVerdictOutcome::Accept => (CoworkerTaskReviewStageStatus::Accepted, None, None),
        ReviewVerdictOutcome::CorrectInScope => (
            CoworkerTaskReviewStageStatus::CorrectInScopeRequired,
            None,
            Some(format!(
                "Reviewer required in-scope fixes for task `{}` candidate `{}`: {}",
                verdict.task_id,
                verdict.candidate_id,
                verdict.required_in_scope_fixes.join("; ")
            )),
        ),
        ReviewVerdictOutcome::ScopeConflict => {
            let hold_id = job
                .holds
                .iter()
                .rev()
                .find(|h| {
                    h.active
                        && h.directly_affected_task_ids
                            .iter()
                            .any(|id| id == &verdict.task_id)
                })
                .map(|h| h.hold_id.clone());
            (
                CoworkerTaskReviewStageStatus::HeldByScopeConflict,
                hold_id,
                Some(format!(
                    "Reviewer reported scope conflict on task `{}` candidate `{}`: {}",
                    verdict.task_id,
                    verdict.candidate_id,
                    verdict.conflicting_paths_or_interfaces.join("; ")
                )),
            )
        }
        ReviewVerdictOutcome::Blocked | ReviewVerdictOutcome::Unclear => (
            CoworkerTaskReviewStageStatus::BlockedByVerdict,
            None,
            Some(format!(
                "Reviewer {} task `{}` candidate `{}`: {}",
                verdict.outcome.as_str(),
                verdict.task_id,
                verdict.candidate_id,
                verdict.findings.join("; ")
            )),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn mark_review_effect_and_outbox_unknown(
    controller: &CoworkerControllerService,
    request: &CoworkerTaskReviewStageRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    effect_id: &str,
    outbox_id: &str,
    run_id: Option<String>,
    worker_id: Option<String>,
    report_artifact_id: Option<String>,
    diag: &str,
) {
    let clean_diag = sanitize_diagnostic_text(diag);
    let _ = controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                eff.status = PendingControllerEffectStatus::EffectUnknown;
                if let Some(ref art_id) = report_artifact_id {
                    eff.external_correlation_id = Some(art_id.clone());
                }
                eff.diagnostic = Some(clean_diag.clone());
                eff.updated_at_ms = request.now_ms;
            }
            Ok(())
        },
    );
    if let Ok(ledger) = controller
        .workflow_store()
        .read_binding_ledger(&request.binding_id)
    {
        if let Some(job) = ledger.jobs.iter().find(|j| j.job_id == request.job_id) {
            let _ = controller.workflow_store().reconcile_or_finalize_outbox(
                &FinalizeTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    outbox_id: outbox_id.to_string(),
                    status: OutboxDispatchStatus::EffectUnknown,
                    next_task_stage: CoworkerTaskStage::EffectUnknown,
                    run_id,
                    worker_id,
                    task_worktree_id: None,
                    head_commit: None,
                    tree_sha: None,
                    diff_digest: None,
                    report_artifact_id,
                    observed_model: None,
                    observed_effort: None,
                    receipt_id: None,
                    diagnostic: Some(clean_diag),
                },
            );
        }
    }
}

#[cfg(test)]
mod stale_job_revision_cas_retry_unit_tests {
    use super::{
        retry_on_stale_job_revision_cas, NativeError, NativeErrorCode,
        MAX_REVIEW_OUTBOX_HOLD_RECONCILE_ATTEMPTS,
    };

    #[test]
    fn retries_exact_stale_job_revision_cas_and_returns_success_or_none_without_extra_calls() {
        let mut attempts = 0usize;
        let outcome: Result<Option<&'static str>, NativeError> =
            retry_on_stale_job_revision_cas(|| {
                attempts += 1;
                if attempts < 4 {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_job_revision does not match current job_revision",
                    ));
                }
                Ok(Some("fenced_after_reconcile"))
            });
        assert_eq!(attempts, 4);
        assert_eq!(
            outcome.expect("succeeds on 4th attempt"),
            Some("fenced_after_reconcile")
        );

        let mut none_attempts = 0usize;
        let none_outcome: Result<Option<&'static str>, NativeError> =
            retry_on_stale_job_revision_cas(|| {
                none_attempts += 1;
                Ok(None)
            });
        assert_eq!(none_attempts, 1);
        assert_eq!(none_outcome.expect("Ok(None) returns immediately"), None);
    }

    #[test]
    fn exhausts_after_four_stale_job_revision_cas_conflicts() {
        let mut attempts = 0usize;
        let err = retry_on_stale_job_revision_cas::<(), _>(|| {
            attempts += 1;
            Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_job_revision does not match current job_revision",
            ))
        })
        .expect_err("4 consecutive CAS conflicts must fail closed");
        assert_eq!(attempts, MAX_REVIEW_OUTBOX_HOLD_RECONCILE_ATTEMPTS);
        assert_eq!(err.code, NativeErrorCode::ValidationFailed);
        assert_eq!(
            err.message,
            "expected_job_revision does not match current job_revision"
        );
    }

    #[test]
    fn non_cas_errors_propagate_immediately_on_first_attempt() {
        for (code, msg) in [
            (
                NativeErrorCode::ValidationFailed,
                "persisted reviewer_response_digest does not match VerifiedVerdict report digest",
            ),
            (NativeErrorCode::IoError, "failed to read report file"),
            (
                NativeErrorCode::PersistenceUncertain,
                "ledger write durability uncertain",
            ),
        ] {
            let mut attempts = 0usize;
            let err = retry_on_stale_job_revision_cas::<(), _>(|| {
                attempts += 1;
                Err(NativeError::new(code, msg))
            })
            .expect_err("non-CAS error must fail immediately");
            assert_eq!(attempts, 1);
            assert_eq!(err.code, code);
            assert_eq!(err.message, msg);
        }
    }
}
