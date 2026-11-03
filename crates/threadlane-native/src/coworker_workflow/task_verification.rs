use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use threadlane_authority::{validation::validate_canonical_thread_uuid, ApprovedTaskRecord};
use threadlane_host::WORK_AREA_SUBDIR;
use threadlane_operations::{
    commit_task_worktree_changes, inspect_review_subject_readonly, CommitTaskChangesRequest,
    InspectReviewSubjectRequest, ReviewPolicyBinding, ReviewSubject, TestEvidenceSummary,
};
use threadlane_workers::{
    evaluate_diff_gate, is_commit_ancestor, is_worktree_clean, load_and_verify_manifest,
    resolve_exact_commit_sha, resolve_exact_tree_sha, resolve_model_target, ChangedPathEntry,
    WorkerFilePermissions, WorkerProvider, WorkerRunStatus, WorktreeKind, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::ToolAdapter;

use crate::conversation_change::ConversationChangeStore;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, read_private_file_bounded, sanitize_diagnostic_text,
    validate_bounded_identifier, write_private_file_atomic,
};

use super::checks::{
    run_frozen_check, verify_and_promote_check_reports, FrozenCheckExecutionReport,
    FrozenCheckExecutionStatus, FrozenCheckSubject, FrozenCheckTargetKind,
};
use super::contracts::{
    deterministic_prefixed_id, sha256_hex, CandidateCheckEvidence, CoworkerTaskRecord,
    CoworkerTaskStage, OutboxDispatchKind, OutboxDispatchStatus, ReviewVerdictOutcome,
    MAX_COWORKER_ID_LEN, MAX_COWORKER_LEDGER_BYTES,
};
use super::controller::{
    is_task_fenced_by_hold_or_sibling, CoworkerControllerService, VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, ControllerJobRuntimeState, FrozenCheckCommandSpec,
    PendingControllerEffect, PendingControllerEffectKind, PendingControllerEffectStatus,
    TaskAttemptExecutionArtifacts, DEFAULT_CONTROLLER_LEASE_TTL_MS,
};
use super::ownership::CoworkerJobOwnershipGuard;
use super::task_correction::{compute_derived_coder_effect_id, compute_derived_coder_request_key};
use super::task_correction_execution::{
    compute_derived_worktree_change_set_digest, read_persisted_derived_task_execution_report,
    PersistedDerivedTaskExecutionArtifact,
};

static IN_FLIGHT_TASK_VERIFICATIONS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

fn in_flight_set() -> &'static Mutex<BTreeSet<String>> {
    IN_FLIGHT_TASK_VERIFICATIONS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

struct InFlightTaskVerificationGuard {
    key: String,
}

impl InFlightTaskVerificationGuard {
    fn acquire(
        plans_dir: &Path,
        binding_id: &str,
        job_id: &str,
        task_id: &str,
        attempt_no: u32,
    ) -> Result<Self, NativeError> {
        let key = format!(
            "{}:{binding_id}:{job_id}:{task_id}:{attempt_no}",
            plans_dir.display()
        );
        let mut guard = in_flight_set().lock().map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "in-flight task verification registry lock poisoned",
            )
        })?;
        if !guard.insert(key.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "concurrent task verification call already in flight for job `{job_id}` task `{task_id}` attempt `{attempt_no}`"
                ),
            ));
        }
        Ok(Self { key })
    }
}

impl Drop for InFlightTaskVerificationGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = in_flight_set().lock() {
            guard.remove(&self.key);
        }
    }
}

/// Request to commit completed coder task worktree output, execute required frozen verification
/// checks, and prepare a verified [`ReviewSubject`] for a task attempt in
/// [`CoworkerTaskStage::ReadyForReview`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskVerificationRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`super::contracts::CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Target task identifier in `job.tasks` and `approval.tasks`.
    pub task_id: String,
    /// 1-based task attempt number (`>= 1`).
    pub attempt_no: u32,
    /// Current wall-clock timestamp (ms since Unix epoch).
    pub now_ms: u64,
}

/// Persisted private (`0600`) [`FrozenCheckExecutionReport`] reference bound to a semantic
/// [`PendingControllerEffectKind::CheckExecution`] effect identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedCheckReportRef {
    /// Stable check identifier (`FrozenCheckCommandSpec::check_id`).
    pub check_id: String,
    /// Deterministic controller effect identifier (`eff_<32hex>`).
    pub effect_id: String,
    /// Absolute path to the private (`0600`) JSON report file on disk.
    pub report_path: String,
    /// Full bounded [`FrozenCheckExecutionReport`] loaded/persisted for this check execution.
    pub report: FrozenCheckExecutionReport,
}

/// Truthful verification status after coder output commit and required frozen check execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerTaskVerificationStatus {
    /// Coder changes were committed, all `required_for_task_review` checks passed without
    /// mutating the subject or leaving descendant processes, and [`ReviewSubject`] was promoted.
    ReadyForReviewer,
    /// One or more required frozen checks failed, mutated the worktree, or had forced descendants;
    /// `Reviewer` invocation is blocked while retaining committed and report artifacts.
    BlockedByFailedChecks,
}

/// Typed outcome of [`prepare_task_attempt_for_review`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskVerificationOutcome {
    /// Owning [`super::contracts::CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Verified task identifier.
    pub task_id: String,
    /// Verified 1-based attempt number.
    pub attempt_no: u32,
    /// Current durable task stage (`CoworkerTaskStage::ReadyForReview`).
    pub task_stage: CoworkerTaskStage,
    /// Verification outcome status (`ReadyForReviewer` or `BlockedByFailedChecks`).
    pub status: CoworkerTaskVerificationStatus,
    /// Deterministic task candidate identifier (`tcd_<32hex>`).
    pub candidate_id: String,
    /// Exact 40-character lowercase hex committed task worktree `HEAD` SHA-1.
    pub committed_head_commit: String,
    /// Exact 40-character lowercase hex committed task worktree Git tree SHA-1.
    pub committed_tree_sha: String,
    /// Exact [`FrozenCheckSubject`] inspected by the required frozen checks.
    pub check_subject: FrozenCheckSubject,
    /// Full persisted check report references for the executed/reused required checks.
    pub check_reports: Vec<PersistedCheckReportRef>,
    /// Promoted [`CandidateCheckEvidence`] entries (empty when `status == BlockedByFailedChecks`).
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Aggregated [`TestEvidenceSummary`] bound to the candidate (present when `ReadyForReviewer`).
    pub test_evidence_summary: Option<TestEvidenceSummary>,
    /// Sealed read-only [`ReviewSubject`] (present when `ReadyForReviewer`).
    pub review_subject: Option<ReviewSubject>,
}

/// Service wrapper for committing completed coder task worktree changes and executing frozen
/// verification checks prior to `Reviewer` dispatch.
#[derive(Debug, Clone)]
pub struct CoworkerTaskVerificationService {
    controller: CoworkerControllerService,
}

impl CoworkerTaskVerificationService {
    /// Creates a new [`CoworkerTaskVerificationService`] wrapping `controller`.
    pub fn new(controller: CoworkerControllerService) -> Self {
        Self { controller }
    }

    /// Borrows the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Commits in-scope coder changes on the owned task worktree (under explicit
    /// `allow_task_worktree_commits`), runs all `required_for_task_review` frozen checks, persists
    /// private `0600` reports, and promotes verified check evidence and [`ReviewSubject`] into
    /// [`TaskAttemptExecutionArtifacts`].
    pub async fn verify_and_prepare_task_attempt(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskVerificationRequest,
    ) -> Result<CoworkerTaskVerificationOutcome, NativeError> {
        prepare_task_attempt_for_review(&self.controller, guard, request).await
    }
}

/// Commits completed coder task worktree output (when permitted by the frozen execution envelope),
/// executes all `required_for_task_review` frozen check commands via [`run_frozen_check`],
/// persists private (`0600`) [`FrozenCheckExecutionReport`] artifacts per semantic check effect ID,
/// and promotes verified check evidence, [`TestEvidenceSummary`], and [`ReviewSubject`] into the
/// existing [`TaskAttemptExecutionArtifacts`] record under a fresh verified lease.
///
/// This primitive never dispatches `Reviewer`, never mints a review acceptance receipt, never
/// transitions a task to `UnderReview`, `Accepted`, or `Integrated`, and never invokes `Closure`.
pub async fn prepare_task_attempt_for_review(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskVerificationRequest,
) -> Result<CoworkerTaskVerificationOutcome, NativeError> {
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

    let plans_dir = controller.workflow_store().plans_dir();
    let _in_flight = InFlightTaskVerificationGuard::acquire(
        plans_dir,
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
        derived_artifact,
    ) = verify_live_task_verification_gate(controller, guard, request, None)?;

    let work_area_root = controller
        .sessions_dir()
        .join(&adm.approval.approval_id)
        .join(WORK_AREA_SUBDIR);
    let manifest = load_and_verify_owned_task_manifest(
        &work_area_root,
        &adm,
        &candidate_task,
        &approved_task,
        &artifacts,
    )?;

    let (committed_head_commit, committed_tree_sha, candidate_id) =
        reconcile_or_commit_task_worktree(
            controller,
            guard,
            request,
            &adm,
            &candidate_task,
            &artifacts,
            derived_artifact.as_ref(),
            &manifest,
            &controller_instance_id,
            lease_epoch,
        )?;

    let check_subject = FrozenCheckSubject {
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
        expected_head_commit: committed_head_commit.clone(),
        expected_tree_sha: committed_tree_sha.clone(),
    };
    check_subject.validate()?;
    let subject_digest = check_subject.subject_digest()?;

    let required_specs: Vec<&FrozenCheckCommandSpec> = adm
        .runtime_state
        .envelope
        .check_commands
        .values()
        .filter(|spec| spec.required_for_task_review)
        .collect();
    if required_specs.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "FrozenControllerExecutionEnvelope has no required_for_task_review check commands",
        ));
    }
    let cap_env = adm.linkage.controller_envelope.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "FrozenApprovalLinkage is missing controller_envelope",
        )
    })?;
    for spec in &required_specs {
        if !cap_env
            .allowed_check_ids
            .iter()
            .any(|id| id == &spec.check_id)
        {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                format!(
                    "check `{}` is not permitted by FrozenControllerCapabilityEnvelope.allowed_check_ids",
                    spec.check_id
                ),
            ));
        }
    }

    let mut check_reports = Vec::with_capacity(required_specs.len());
    let mut raw_reports = Vec::with_capacity(required_specs.len());
    let mut any_check_failed = false;

    for spec in &required_specs {
        let report_ref = execute_or_reconcile_single_check(
            controller,
            guard,
            request,
            &check_subject,
            &subject_digest,
            spec,
            &controller_instance_id,
            lease_epoch,
        )
        .await?;
        let passed = report_ref.report.passed();
        raw_reports.push(report_ref.report.clone());
        check_reports.push(report_ref);
        if !passed {
            any_check_failed = true;
            break;
        }
    }

    // Re-verify physical ownership, admission, lease epoch, ConversationChangeStore, and hold/stop
    // fences after all `.await` points and before promoting any check evidence or ReviewSubject.
    let (post_adm, _, _, post_task, post_approved_task, post_artifacts, _) =
        verify_live_task_verification_gate(
            controller,
            guard,
            request,
            Some((&controller_instance_id, lease_epoch)),
        )?;

    if any_check_failed {
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            &controller_instance_id,
            lease_epoch,
            |state| {
                let art_key =
                    ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
                let art = state.attempt_artifacts.get_mut(&art_key).ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskAttemptExecutionArtifacts missing during failed check recording",
                    )
                })?;
                art.committed_head_commit = Some(committed_head_commit.clone());
                art.committed_tree_sha = Some(committed_tree_sha.clone());
                art.candidate_id = Some(candidate_id.clone());
                art.check_evidence.clear();
                art.test_evidence_summary = None;
                art.review_subject = None;
                Ok(())
            },
        )?;

        return Ok(CoworkerTaskVerificationOutcome {
            job_id: request.job_id.clone(),
            task_id: request.task_id.clone(),
            attempt_no: request.attempt_no,
            task_stage: post_task.stage,
            status: CoworkerTaskVerificationStatus::BlockedByFailedChecks,
            candidate_id,
            committed_head_commit,
            committed_tree_sha,
            check_subject,
            check_reports,
            check_evidence: Vec::new(),
            test_evidence_summary: None,
            review_subject: None,
        });
    }

    let promoted_evidence = verify_and_promote_check_reports(
        &check_subject,
        &post_adm.runtime_state.envelope.check_commands,
        &raw_reports,
    )?;

    let test_evidence_summary =
        build_verified_test_evidence_summary(&candidate_id, &subject_digest, &promoted_evidence)?;

    let review_policy = ReviewPolicyBinding {
        policy_id: post_adm.runtime_state.envelope.policy.policy_id.clone(),
        requested_provider: post_approved_task.provider.map(|p| p.as_str().to_string()),
        requested_model: Some(post_approved_task.base_model.clone()),
        requested_effort: post_task.selected_choice.effort.clone(),
        observed_model: Some(post_artifacts.observed_model.clone()),
        observed_effort: post_artifacts.observed_effort.clone(),
        file_permissions: post_approved_task
            .file_permissions
            .unwrap_or(WorkerFilePermissions::TaskFiles),
        require_effort_evidence: post_task.selected_choice.effort.is_some(),
        require_passing_tests: true,
    };
    let review_subject = inspect_review_subject_readonly(&InspectReviewSubjectRequest {
        work_area_root: manifest.work_area_root.clone(),
        task_worktree_id: manifest.worktree_id.clone(),
        policy: review_policy,
        test_evidence: test_evidence_summary.clone(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to construct verified ReviewSubject: {err}"),
        )
    })?;

    if review_subject.task_head_commit != committed_head_commit
        || review_subject.task_tree_sha != committed_tree_sha
        || review_subject.base_commit != manifest.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ReviewSubject head_commit, tree_sha, or base_commit drifted from verified check subject",
        ));
    }

    // Final fence and ownership check immediately before durable artifact promotion.
    let _ = verify_live_task_verification_gate(
        controller,
        guard,
        request,
        Some((&controller_instance_id, lease_epoch)),
    )?;

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        &controller_instance_id,
        lease_epoch,
        |state| {
            let art_key =
                ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
            let art = state.attempt_artifacts.get_mut(&art_key).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "TaskAttemptExecutionArtifacts missing during ReviewSubject promotion",
                )
            })?;
            if let Some(ref existing_subject) = art.review_subject {
                if existing_subject != &review_subject
                    || art.candidate_id.as_deref() != Some(candidate_id.as_str())
                    || art.committed_head_commit.as_deref() != Some(committed_head_commit.as_str())
                    || art.committed_tree_sha.as_deref() != Some(committed_tree_sha.as_str())
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskAttemptExecutionArtifacts already has a different promoted ReviewSubject",
                    ));
                }
                return Ok(());
            }
            art.committed_head_commit = Some(committed_head_commit.clone());
            art.committed_tree_sha = Some(committed_tree_sha.clone());
            art.candidate_id = Some(candidate_id.clone());
            art.check_evidence = promoted_evidence.clone();
            art.test_evidence_summary = Some(test_evidence_summary.clone());
            art.review_subject = Some(review_subject.clone());
            Ok(())
        },
    )?;

    Ok(CoworkerTaskVerificationOutcome {
        job_id: request.job_id.clone(),
        task_id: request.task_id.clone(),
        attempt_no: request.attempt_no,
        task_stage: post_task.stage,
        status: CoworkerTaskVerificationStatus::ReadyForReviewer,
        candidate_id,
        committed_head_commit,
        committed_tree_sha,
        check_subject,
        check_reports,
        check_evidence: promoted_evidence,
        test_evidence_summary: Some(test_evidence_summary),
        review_subject: Some(review_subject),
    })
}

#[allow(clippy::type_complexity)]
fn verify_live_task_verification_gate(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskVerificationRequest,
    expected_lease: Option<(&str, u64)>,
) -> Result<
    (
        VerifiedControllerAdmission,
        String,
        u64,
        CoworkerTaskRecord,
        ApprovedTaskRecord,
        TaskAttemptExecutionArtifacts,
        Option<PersistedDerivedTaskExecutionArtifact>,
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
                "controller driver lease changed during task verification",
            ));
        }
    }

    if adm.runtime_state.stop_requested {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "controller job has stop_requested == true; task verification is fenced",
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
                format!("failed to read ConversationChangeStore during task verification: {err}"),
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
                    "task `{}` not found in CoworkerJobRecord.tasks",
                    request.task_id
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
                    "task `{}` not found in ApprovalRecord.tasks",
                    request.task_id
                ),
            )
        })?;

    if is_task_fenced_by_hold_or_sibling(
        &candidate_task,
        &adm.job,
        &adm.runtime_state,
        &adm.approval.approval_id,
        plans_dir,
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is fenced by an active hold, conversational change, stop request, or impacted sibling",
                request.task_id
            ),
        ));
    }

    if candidate_task.stage == CoworkerTaskStage::EffectUnknown
        || candidate_task.unresolved_outbox_id.is_some()
    {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` has an unresolved outbox or EffectUnknown state",
                request.task_id
            ),
        ));
    }
    if candidate_task.stage != CoworkerTaskStage::ReadyForReview {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` must be in `ready_for_review` stage for task verification (current: `{}`)",
                request.task_id, candidate_task.stage
            ),
        ));
    }
    if candidate_task.current_attempt_no != request.attempt_no {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` current_attempt_no `{}` does not match requested attempt_no `{}`",
                request.task_id, candidate_task.current_attempt_no, request.attempt_no
            ),
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
    if artifacts.task_id != request.task_id
        || artifacts.attempt_no != request.attempt_no
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

    if (request.attempt_no >= 2) != artifacts.is_derived_attempt {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` attempt `{}` is_derived_attempt `{}` does not match attempt_no",
                request.task_id, request.attempt_no, artifacts.is_derived_attempt
            ),
        ));
    }

    let expected_coder_provider = match candidate_task.selected_choice.adapter {
        ToolAdapter::Antigravity => WorkerProvider::Antigravity,
        ToolAdapter::OpencodeV2 => WorkerProvider::OpenCodeV2,
        _ => {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "unsupported Coder adapter on candidate_task.selected_choice",
            ));
        }
    };
    let resolved_coder_target = resolve_model_target(
        expected_coder_provider,
        &candidate_task.selected_choice.model_arg,
        candidate_task.selected_choice.effort.as_deref(),
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("invalid frozen Coder model target: {err}"),
        )
    })?;
    if (artifacts.observed_model != resolved_coder_target.base_model
        && artifacts.observed_model != candidate_task.selected_choice.model_arg)
        || resolved_coder_target.base_model != approved_task.base_model
        || resolved_coder_target.effort != approved_task.effort
        || approved_task.provider != Some(expected_coder_provider)
        || artifacts.observed_provider != expected_coder_provider.as_str()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` attempt `{}` observed provider/model does not match pinned coder choice",
                request.task_id, request.attempt_no
            ),
        ));
    }
    if let Some(ref exp_effort) = resolved_coder_target.effort {
        if artifacts.observed_effort.as_deref() != Some(exp_effort.as_str()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` attempt `{}` observed_effort does not match pinned effort `{exp_effort}`",
                    request.task_id, request.attempt_no
                ),
            ));
        }
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

    if let Some(attempt_ref) = candidate_task
        .attempts
        .iter()
        .find(|a| a.attempt_no == request.attempt_no)
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

    let derived_artifact = if request.attempt_no >= 2 {
        Some(verify_derived_attempt_for_task_verification(
            plans_dir,
            request,
            &adm,
            &candidate_task,
            &approved_task,
            &artifacts,
        )?)
    } else {
        None
    };

    Ok((
        adm,
        controller_instance_id,
        lease_epoch,
        candidate_task,
        approved_task,
        artifacts,
        derived_artifact,
    ))
}

fn verify_derived_attempt_for_task_verification(
    plans_dir: &Path,
    request: &CoworkerTaskVerificationRequest,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
) -> Result<PersistedDerivedTaskExecutionArtifact, NativeError> {
    if !adm
        .linkage
        .authorizes_in_scope_correction(request.attempt_no)
    {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            format!(
                "frozen approval linkage does not authorize derived correction attempt `{}` for task `{}`",
                request.attempt_no, request.task_id
            ),
        ));
    }
    let receipt = adm
        .runtime_state
        .derived_attempts
        .iter()
        .find(|d| d.task_id == request.task_id && d.attempt_no == request.attempt_no)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing DerivedTaskAttemptReceipt for task `{}` derived attempt `{}`",
                    request.task_id, request.attempt_no
                ),
            )
        })?;
    receipt.validate_against_task(candidate_task, &adm.runtime_state.envelope)?;

    let prior_attempt_no = request.attempt_no - 1;
    let prior_candidate = candidate_task
        .candidates
        .iter()
        .find(|c| c.attempt_no == prior_attempt_no)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing prior rejected candidate for task `{}` attempt `{prior_attempt_no}`",
                    request.task_id
                ),
            )
        })?;
    let prior_verdict = prior_candidate.review_verdict.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "prior attempt candidate has no recorded Reviewer verdict",
        )
    })?;
    if prior_verdict.outcome != ReviewVerdictOutcome::CorrectInScope
        || receipt.prior_rejected_candidate_id.as_deref()
            != Some(prior_candidate.candidate_id.as_str())
        || receipt.prior_review_verdict_id.as_deref() != Some(prior_verdict.verdict_id.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "DerivedTaskAttemptReceipt does not match prior CorrectInScope candidate/verdict",
        ));
    }
    let Some(ref prior_subject_digest) = receipt.prior_review_subject_digest else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "DerivedTaskAttemptReceipt is missing prior_review_subject_digest",
        ));
    };

    let request_key = compute_derived_coder_request_key(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &prior_candidate.candidate_id,
        &prior_verdict.verdict_id,
        prior_subject_digest,
        &adm.runtime_state.envelope.envelope_digest,
        &candidate_task.selected_choice,
    );
    let effect_id = compute_derived_coder_effect_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &request_key,
    );

    let coder_eff = adm
        .runtime_state
        .pending_effects
        .get(&effect_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing CoderDispatch PendingControllerEffect `{effect_id}` for derived attempt `{}`",
                    request.attempt_no
                ),
            )
        })?;
    if coder_eff.kind != PendingControllerEffectKind::CoderDispatch
        || coder_eff.status != PendingControllerEffectStatus::EffectConfirmed
        || coder_eff.request_key != request_key
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "CoderDispatch PendingControllerEffect `{effect_id}` is not EffectConfirmed (`{:?}`)",
                coder_eff.status
            ),
        ));
    }

    let outbox = adm
        .job
        .outbox
        .iter()
        .find(|o| {
            o.task_id == request.task_id
                && o.kind == OutboxDispatchKind::TaskAttemptDispatch
                && o.attempt_no == request.attempt_no
                && o.request_key == request_key
        })
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing TaskAttemptDispatch outbox for task `{}` derived attempt `{}`",
                    request.task_id, request.attempt_no
                ),
            )
        })?;
    if !matches!(
        outbox.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed | OutboxDispatchStatus::ReconciledConfirmed
    ) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "derived TaskAttemptDispatch outbox is not confirmed",
        ));
    }

    let persisted_derived = read_persisted_derived_task_execution_report(plans_dir, &effect_id)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing persisted private 0600 PersistedDerivedTaskExecutionArtifact for derived attempt `{}` (`{effect_id}`)",
                    request.attempt_no
                ),
            )
        })?;
    persisted_derived.validate_for_task_verification(receipt, candidate_task, approved_task)?;

    if persisted_derived.pre_execution_head_commit != prior_candidate.head_commit
        || persisted_derived.pre_execution_tree_sha != prior_candidate.tree_sha
        || artifacts.run_id != receipt.run_id
        || artifacts.worker_id != receipt.worker_id
        || artifacts.task_worktree_id != receipt.task_worktree_id
        || artifacts.integration_worktree_id != receipt.integration_worktree_id
        || artifacts.execution_base_commit != receipt.execution_base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "PersistedDerivedTaskExecutionArtifact or TaskAttemptExecutionArtifacts does not match prior candidate HEAD/tree or receipt",
        ));
    }

    Ok(persisted_derived)
}

fn load_and_verify_owned_task_manifest(
    work_area_root: &Path,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
) -> Result<WorktreeManifest, NativeError> {
    let manifest =
        load_and_verify_manifest(work_area_root, &artifacts.task_worktree_id).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to load and verify owned task worktree manifest `{}`: {err}",
                    artifacts.task_worktree_id
                ),
            )
        })?;
    if manifest.kind != WorktreeKind::Task {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task_worktree_id manifest is not WorktreeKind::Task",
        ));
    }
    let int_manifest = load_and_verify_manifest(work_area_root, &artifacts.integration_worktree_id)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to load and verify owned integration worktree manifest `{}`: {err}",
                    artifacts.integration_worktree_id
                ),
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

    let expected_write_paths: BTreeSet<String> =
        candidate_task.write_paths.iter().cloned().collect();
    if manifest.worktree_id != artifacts.task_worktree_id
        || manifest
            .task_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        || manifest.repository_path != Path::new(&adm.linkage.project_base.repository_path)
        || manifest.repository_path != adm.approval.repository_path
        || manifest.base_commit != artifacts.execution_base_commit
        || candidate_task.write_paths != approved_task.allowed_write_paths
        || manifest.allowed_write_paths != expected_write_paths
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task WorktreeManifest ownership, repository, base_commit, or allowed_write_paths drifted from frozen task scope",
        ));
    }
    let primary_clean =
        is_worktree_clean(&manifest.repository_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to inspect primary repository cleanliness: {err}"),
                )
            },
        )?;
    if !primary_clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "primary repository checkout is dirty; refusing task verification",
        ));
    }
    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
fn reconcile_or_commit_task_worktree(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskVerificationRequest,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
    derived_artifact: Option<&PersistedDerivedTaskExecutionArtifact>,
    manifest: &WorktreeManifest,
    controller_instance_id: &str,
    lease_epoch: u64,
) -> Result<(String, String, String), NativeError> {
    if !adm.runtime_state.envelope.allow_task_worktree_commits {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "FrozenControllerExecutionEnvelope.allow_task_worktree_commits is false; task worktree commits are not permitted",
        ));
    }

    let expected_pre_commit_head = derived_artifact
        .map(|d| d.pre_execution_head_commit.as_str())
        .unwrap_or(manifest.base_commit.as_str());

    let work_area_root = &manifest.work_area_root;
    let current_head = resolve_exact_commit_sha(
        &manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve task worktree HEAD: {err}"),
        )
    })?;
    let current_tree = resolve_exact_tree_sha(
        &manifest.checkout_path,
        &current_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve task worktree tree SHA: {err}"),
        )
    })?;
    let worktree_clean = is_worktree_clean(&manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to inspect task worktree cleanliness: {err}"),
            )
        })?;

    // Check if TaskCommit was already confirmed for this attempt and a mutating check left the worktree dirty.
    if !worktree_clean && current_head != expected_pre_commit_head {
        if let Some(existing_commit_eff) = adm.runtime_state.pending_effects.values().find(|e| {
            e.kind == PendingControllerEffectKind::TaskCommit
                && e.task_id.as_deref() == Some(request.task_id.as_str())
                && e.attempt_no == Some(request.attempt_no)
                && e.status == PendingControllerEffectStatus::EffectConfirmed
                && e.external_correlation_id.as_deref() == Some(current_head.as_str())
        }) {
            let has_mutated_check_report = adm
                .runtime_state
                .pending_effects
                .values()
                .filter(|e| {
                    e.kind == PendingControllerEffectKind::CheckExecution
                        && e.task_id.as_deref() == Some(request.task_id.as_str())
                        && e.attempt_no == Some(request.attempt_no)
                })
                .any(|chk_eff| {
                    read_persisted_check_report(
                        controller.workflow_store().plans_dir(),
                        &chk_eff.effect_id,
                    )
                    .ok()
                    .flatten()
                    .is_some_and(|rep| {
                        rep.status == FrozenCheckExecutionStatus::SubjectMutatedOrDrifted
                            && rep.subject.expected_head_commit == current_head
                    })
                });
            if has_mutated_check_report && !existing_commit_eff.effect_id.is_empty() {
                let candidate_id = compute_task_candidate_id(
                    &request.job_id,
                    &request.task_id,
                    request.attempt_no,
                    &current_head,
                    &current_tree,
                );
                return Ok((current_head, current_tree, candidate_id));
            }
        }
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task worktree has uncommitted modifications on top of an unexpected HEAD commit",
        ));
    }

    if worktree_clean && current_head == expected_pre_commit_head {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task worktree has no uncommitted changes to commit at expected pre-commit HEAD",
        ));
    }

    let pre_diff_gate =
        evaluate_diff_gate(work_area_root, &manifest.worktree_id).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to evaluate task worktree diff gate: {err}"),
            )
        })?;
    if !pre_diff_gate.evidence_complete
        || !pre_diff_gate.passed
        || pre_diff_gate.changed_paths.is_empty()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task worktree failed pre-commit diff gate or has no in-scope changes",
        ));
    }

    if !worktree_clean {
        if let Some(derived) = derived_artifact {
            let (live_derived_digest, live_has_changes) =
                compute_derived_worktree_change_set_digest(
                    &manifest.checkout_path,
                    expected_pre_commit_head,
                    &pre_diff_gate.changed_paths,
                )?;
            if !live_has_changes
                || !constant_time_str_eq(&live_derived_digest, &derived.worktree_change_set_digest)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "live task worktree change-set drifted from persisted PersistedDerivedTaskExecutionArtifact before commit",
                ));
            }
        }
    }

    let diff_digest =
        compute_worktree_change_set_digest(&manifest.checkout_path, &pre_diff_gate.changed_paths)?;
    let scope_bytes = serde_json::to_vec(&candidate_task.write_paths).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize task write scope for commit digest",
        )
    })?;
    let scope_digest = sha256_hex(&scope_bytes);
    let commit_subject_digest = sha256_hex(
        format!(
            "threadlane-native:task-commit-subject:v1:{}:{}:{}:{}:{}:{}:{}",
            request.job_id,
            request.task_id,
            request.attempt_no,
            manifest.worktree_id,
            manifest.base_commit,
            scope_digest,
            diff_digest,
        )
        .as_bytes(),
    );
    let commit_request_key = deterministic_prefixed_id(
        "threadlane-native:coworker-task-commit-key:v1",
        "tck",
        &[
            &request.job_id,
            &request.task_id,
            &request.attempt_no.to_string(),
            &manifest.worktree_id,
            &manifest.base_commit,
            &diff_digest,
        ],
    );
    let commit_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&request.job_id, &request.task_id, &commit_request_key],
    );

    for eff in adm.runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::TaskCommit
            && eff.task_id.as_deref() == Some(request.task_id.as_str())
            && eff.attempt_no == Some(request.attempt_no)
            && (eff.effect_id != commit_effect_id
                || eff.request_key != commit_request_key
                || !constant_time_str_eq(&eff.subject_digest, &commit_subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "task `{}` attempt `{}` already has a conflicting TaskCommit effect `{}`",
                    request.task_id, request.attempt_no, eff.effect_id
                ),
            ));
        }
    }

    let existing_commit_effect = adm
        .runtime_state
        .pending_effects
        .get(&commit_effect_id)
        .cloned();

    // Case 1: Task worktree is already clean at a committed HEAD != expected_pre_commit_head.
    if worktree_clean && current_head != expected_pre_commit_head {
        let Some(existing_eff) = existing_commit_effect else {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task worktree HEAD advanced past expected pre-commit HEAD without a recorded TaskCommit effect",
            ));
        };
        if matches!(
            existing_eff.status,
            PendingControllerEffectStatus::RefusedBeforeEffect
                | PendingControllerEffectStatus::EffectFailedTerminal
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskCommit effect was previously refused or failed terminally",
            ));
        }
        let is_anc_base = is_commit_ancestor(
            &manifest.checkout_path,
            &manifest.base_commit,
            &current_head,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify task commit ancestry from base_commit: {err}"),
            )
        })?;
        let is_anc_pre = is_commit_ancestor(
            &manifest.checkout_path,
            expected_pre_commit_head,
            &current_head,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to verify task commit ancestry from expected pre-commit HEAD: {err}"
                ),
            )
        })?;
        if !is_anc_base || !is_anc_pre || current_head == manifest.base_commit {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task worktree HEAD does not descend from manifest.base_commit and expected pre-commit HEAD",
            ));
        }
        if let Some(ref prev_head) = artifacts.committed_head_commit {
            if prev_head != &manifest.base_commit
                && prev_head != expected_pre_commit_head
                && prev_head != &current_head
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "task worktree HEAD drifted from previously recorded committed_head_commit",
                ));
            }
        }
        let candidate_id = compute_task_candidate_id(
            &request.job_id,
            &request.task_id,
            request.attempt_no,
            &current_head,
            &current_tree,
        );
        if existing_eff.status != PendingControllerEffectStatus::EffectConfirmed
            || artifacts.candidate_id.as_deref() != Some(candidate_id.as_str())
            || artifacts.committed_head_commit.as_deref() != Some(current_head.as_str())
        {
            controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state.pending_effects.get_mut(&commit_effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectConfirmed;
                        eff.external_correlation_id = Some(current_head.clone());
                        eff.updated_at_ms = request.now_ms;
                    }
                    let art_key = ControllerJobRuntimeState::artifact_key(
                        &request.task_id,
                        request.attempt_no,
                    );
                    if let Some(art) = state.attempt_artifacts.get_mut(&art_key) {
                        art.committed_head_commit = Some(current_head.clone());
                        art.committed_tree_sha = Some(current_tree.clone());
                        art.candidate_id = Some(candidate_id.clone());
                    }
                    Ok(())
                },
            )?;
        }
        return Ok((current_head, current_tree, candidate_id));
    }

    // Case 2: Worktree is dirty at expected_pre_commit_head; check for unconfirmed/unknown prior commit effect.
    if let Some(existing_eff) = existing_commit_effect {
        if existing_eff.status == PendingControllerEffectStatus::ReservedBeforeEffect {
            let _ = controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state.pending_effects.get_mut(&commit_effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectUnknown;
                        eff.diagnostic = Some(
                            "TaskCommit reservation found with uncommitted worktree; refusing blind replay"
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
                "TaskCommit effect `{commit_effect_id}` is in `{:?}` state while worktree is uncommitted; refusing blind replay or reset",
                existing_eff.status
            ),
        ));
    }

    // Re-verify live ownership and fencing right before reserving and executing the Git commit.
    let _ = verify_live_task_verification_gate(
        controller,
        guard,
        request,
        Some((controller_instance_id, lease_epoch)),
    )?;

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if state.pending_effects.contains_key(&commit_effect_id) {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "TaskCommit effect was concurrently reserved",
                ));
            }
            state.pending_effects.insert(
                commit_effect_id.clone(),
                PendingControllerEffect {
                    effect_id: commit_effect_id.clone(),
                    request_key: commit_request_key.clone(),
                    job_id: request.job_id.clone(),
                    task_id: Some(request.task_id.clone()),
                    attempt_no: Some(request.attempt_no),
                    kind: PendingControllerEffectKind::TaskCommit,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: commit_subject_digest.clone(),
                    external_correlation_id: None,
                    created_at_ms: request.now_ms,
                    updated_at_ms: request.now_ms,
                    diagnostic: None,
                },
            );
            Ok(())
        },
    )?;

    let commit_msg = format!(
        "threadlane({}): commit task {} attempt {}",
        request.job_id, request.task_id, request.attempt_no
    );
    let commit_res = commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area_root.to_path_buf(),
        task_worktree_id: manifest.worktree_id.clone(),
        commit_message: commit_msg,
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    });

    let commit_report = match commit_res {
        Ok(rep) => rep,
        Err(err) => {
            let diag = sanitize_diagnostic_text(&err.to_string());
            let _ = controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state.pending_effects.get_mut(&commit_effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectUnknown;
                        eff.diagnostic = Some(diag.clone());
                        eff.updated_at_ms = request.now_ms;
                    }
                    Ok(())
                },
            );
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("commit_task_worktree_changes failed: {err}"),
            ));
        }
    };

    let post_clean = is_worktree_clean(&manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify post-commit worktree cleanliness: {err}"),
            )
        })?;
    let post_diff_gate =
        evaluate_diff_gate(work_area_root, &manifest.worktree_id).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to evaluate post-commit diff gate: {err}"),
            )
        })?;
    if !post_clean
        || commit_report.task_head_commit == manifest.base_commit
        || commit_report.task_head_commit == expected_pre_commit_head
        || !post_diff_gate.passed
        || !post_diff_gate.evidence_complete
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "post-commit task worktree verification failed",
        ));
    }

    let candidate_id = compute_task_candidate_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &commit_report.task_head_commit,
        &commit_report.task_tree_sha,
    );

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(&commit_effect_id) {
                eff.status = PendingControllerEffectStatus::EffectConfirmed;
                eff.external_correlation_id = Some(commit_report.task_head_commit.clone());
                eff.updated_at_ms = request.now_ms;
            }
            let art_key =
                ControllerJobRuntimeState::artifact_key(&request.task_id, request.attempt_no);
            let art = state.attempt_artifacts.get_mut(&art_key).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "TaskAttemptExecutionArtifacts missing after task commit",
                )
            })?;
            art.committed_head_commit = Some(commit_report.task_head_commit.clone());
            art.committed_tree_sha = Some(commit_report.task_tree_sha.clone());
            art.candidate_id = Some(candidate_id.clone());
            Ok(())
        },
    )?;

    Ok((
        commit_report.task_head_commit,
        commit_report.task_tree_sha,
        candidate_id,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_or_reconcile_single_check(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskVerificationRequest,
    check_subject: &FrozenCheckSubject,
    subject_digest: &str,
    spec: &FrozenCheckCommandSpec,
    controller_instance_id: &str,
    lease_epoch: u64,
) -> Result<PersistedCheckReportRef, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    let spec_digest = spec.spec_digest()?;
    let check_effect_subject_digest = sha256_hex(
        format!(
            "threadlane-native:task-check-effect-subject:v1:{}:{}:{}:{}:{}:{}",
            request.job_id,
            request.task_id,
            request.attempt_no,
            subject_digest,
            spec.check_id,
            spec_digest,
        )
        .as_bytes(),
    );
    let check_request_key = deterministic_prefixed_id(
        "threadlane-native:coworker-task-check-key:v1",
        "chk",
        &[
            &request.job_id,
            &request.task_id,
            &request.attempt_no.to_string(),
            subject_digest,
            &spec.check_id,
            &spec_digest,
        ],
    );
    let check_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&request.job_id, &request.task_id, &check_request_key],
    );

    let (adm, _, _, _, _, _, _) = verify_live_task_verification_gate(
        controller,
        guard,
        request,
        Some((controller_instance_id, lease_epoch)),
    )?;

    // Reject any conflicting CheckExecution effect for the same (task_id, attempt_no, check_id).
    for eff in adm.runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::CheckExecution
            && eff.task_id.as_deref() == Some(request.task_id.as_str())
            && eff.attempt_no == Some(request.attempt_no)
            && eff.external_correlation_id.as_deref() == Some(spec.check_id.as_str())
            && (eff.effect_id != check_effect_id
                || eff.request_key != check_request_key
                || !constant_time_str_eq(&eff.subject_digest, &check_effect_subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "task `{}` attempt `{}` check `{}` already has a conflicting CheckExecution effect `{}`",
                    request.task_id, request.attempt_no, spec.check_id, eff.effect_id
                ),
            ));
        }
    }

    if let Some(existing_eff) = adm
        .runtime_state
        .pending_effects
        .get(&check_effect_id)
        .cloned()
    {
        let persisted = read_persisted_check_report(plans_dir, &check_effect_id)?;
        match existing_eff.status {
            PendingControllerEffectStatus::EffectConfirmed
            | PendingControllerEffectStatus::EffectFailedTerminal => {
                let report = persisted.ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        format!(
                            "persisted FrozenCheckExecutionReport missing for finalized effect `{check_effect_id}`"
                        ),
                    )
                })?;
                report.verify_against_context(check_subject, spec)?;
                return Ok(PersistedCheckReportRef {
                    check_id: spec.check_id.clone(),
                    effect_id: check_effect_id.clone(),
                    report_path: check_report_file_path(plans_dir, &check_effect_id)
                        .to_string_lossy()
                        .to_string(),
                    report,
                });
            }
            PendingControllerEffectStatus::ReservedBeforeEffect
            | PendingControllerEffectStatus::EffectUnknown => {
                if let Some(report) = persisted {
                    report.verify_against_context(check_subject, spec)?;
                    let next_status = if report.passed() {
                        PendingControllerEffectStatus::EffectConfirmed
                    } else {
                        PendingControllerEffectStatus::EffectFailedTerminal
                    };
                    controller.policy_store().mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        |state| {
                            if let Some(eff) = state.pending_effects.get_mut(&check_effect_id) {
                                eff.status = next_status;
                                eff.updated_at_ms = request.now_ms;
                            }
                            Ok(())
                        },
                    )?;
                    return Ok(PersistedCheckReportRef {
                        check_id: spec.check_id.clone(),
                        effect_id: check_effect_id.clone(),
                        report_path: check_report_file_path(plans_dir, &check_effect_id)
                            .to_string_lossy()
                            .to_string(),
                        report,
                    });
                }

                if existing_eff.status == PendingControllerEffectStatus::ReservedBeforeEffect {
                    let _ = controller.policy_store().mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        |state| {
                            if let Some(eff) = state.pending_effects.get_mut(&check_effect_id) {
                                eff.status = PendingControllerEffectStatus::EffectUnknown;
                                eff.diagnostic = Some(
                                    "CheckExecution reservation found without a persisted report; refusing blind rerun"
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
                        "check `{}` effect `{check_effect_id}` is in `{:?}` state without a verified persisted report; refusing to rerun unknown check",
                        spec.check_id, existing_eff.status
                    ),
                ));
            }
            PendingControllerEffectStatus::RefusedBeforeEffect => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "check `{}` effect `{check_effect_id}` was refused before effect",
                        spec.check_id
                    ),
                ));
            }
        }
    }

    // Reserve CheckExecution intent before invoking `run_frozen_check`.
    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if state.pending_effects.contains_key(&check_effect_id) {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "CheckExecution effect was concurrently reserved",
                ));
            }
            state.pending_effects.insert(
                check_effect_id.clone(),
                PendingControllerEffect {
                    effect_id: check_effect_id.clone(),
                    request_key: check_request_key.clone(),
                    job_id: request.job_id.clone(),
                    task_id: Some(request.task_id.clone()),
                    attempt_no: Some(request.attempt_no),
                    kind: PendingControllerEffectKind::CheckExecution,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: check_effect_subject_digest.clone(),
                    external_correlation_id: Some(spec.check_id.clone()),
                    created_at_ms: request.now_ms,
                    updated_at_ms: request.now_ms,
                    diagnostic: None,
                },
            );
            Ok(())
        },
    )?;

    let run_res = run_frozen_check(guard, check_subject, spec).await;

    // Immediately re-verify physical ownership after `.await`.
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let report = match run_res {
        Ok(rep) => rep,
        Err(err) => {
            let diag = sanitize_diagnostic_text(&err.to_string());
            let _ = controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state.pending_effects.get_mut(&check_effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectUnknown;
                        eff.diagnostic = Some(diag.clone());
                        eff.updated_at_ms = request.now_ms;
                    }
                    Ok(())
                },
            );
            return Err(err);
        }
    };

    report.verify_against_context(check_subject, spec)?;
    let report_path = write_persisted_check_report(plans_dir, &check_effect_id, &report)?;

    let next_status = if report.passed() {
        PendingControllerEffectStatus::EffectConfirmed
    } else {
        PendingControllerEffectStatus::EffectFailedTerminal
    };
    let diag = if report.passed() {
        None
    } else {
        Some(format!(
            "check `{}` finished with status `{}`",
            spec.check_id,
            report.status.as_str()
        ))
    };

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(&check_effect_id) {
                eff.status = next_status;
                eff.diagnostic = diag.clone();
                eff.updated_at_ms = request.now_ms;
            }
            Ok(())
        },
    )?;

    Ok(PersistedCheckReportRef {
        check_id: spec.check_id.clone(),
        effect_id: check_effect_id,
        report_path: report_path.to_string_lossy().to_string(),
        report,
    })
}

fn compute_worktree_change_set_digest(
    checkout_path: &Path,
    changed_paths: &[ChangedPathEntry],
) -> Result<String, NativeError> {
    let mut entries: BTreeMap<String, Option<String>> = BTreeMap::new();
    for entry in changed_paths {
        let full_path = checkout_path.join(&entry.path);
        let content_digest = match fs::read(&full_path) {
            Ok(bytes) => Some(sha256_hex(&bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                return Err(NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to read task worktree changed path `{}`: {}",
                        entry.path,
                        sanitize_diagnostic_text(&err.to_string())
                    ),
                ));
            }
        };
        entries.insert(entry.path.clone(), content_digest);
    }
    let bytes = serde_json::to_vec(&entries).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize worktree change set for digest calculation",
        )
    })?;
    Ok(sha256_hex(&bytes))
}

fn compute_task_candidate_id(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    head_commit: &str,
    tree_sha: &str,
) -> String {
    deterministic_prefixed_id(
        "threadlane-native:coworker-task-candidate:v1",
        "tcd",
        &[
            job_id,
            task_id,
            &attempt_no.to_string(),
            head_commit,
            tree_sha,
        ],
    )
}

fn build_verified_test_evidence_summary(
    candidate_id: &str,
    subject_digest: &str,
    check_evidence: &[CandidateCheckEvidence],
) -> Result<TestEvidenceSummary, NativeError> {
    if check_evidence.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "cannot build TestEvidenceSummary from empty check_evidence",
        ));
    }
    for ev in check_evidence {
        ev.validate_passed()?;
    }
    let seed = (
        "threadlane-native:task-test-evidence-summary:v1",
        candidate_id,
        subject_digest,
        check_evidence,
    );
    let bytes = serde_json::to_vec(&seed).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize check_evidence for TestEvidenceSummary digest",
        )
    })?;
    let evidence_digest = sha256_hex(&bytes);
    let parts: Vec<String> = check_evidence
        .iter()
        .map(|ev| {
            format!(
                "{}:exit={}:digest={}",
                ev.check_id,
                ev.exit_code,
                &ev.evidence_digest[..16]
            )
        })
        .collect();
    let evidence_summary = format!(
        "candidate={candidate_id} digest={} checks=[{}]",
        &evidence_digest[..16],
        parts.join(", ")
    );
    Ok(TestEvidenceSummary {
        formatting_passed: true,
        clippy_passed: true,
        tests_passed: true,
        passed_test_count: check_evidence.len() as u32,
        failed_test_count: 0,
        evidence_summary,
    })
}

fn check_report_filename(effect_id: &str) -> String {
    format!("coworker-check-report-{effect_id}.json")
}

fn check_report_file_path(plans_dir: &Path, effect_id: &str) -> PathBuf {
    plans_dir.join(check_report_filename(effect_id))
}

fn write_persisted_check_report(
    plans_dir: &Path,
    effect_id: &str,
    report: &FrozenCheckExecutionReport,
) -> Result<PathBuf, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let bytes = serde_json::to_vec_pretty(report).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize FrozenCheckExecutionReport JSON",
        )
    })?;
    if bytes.len() > MAX_COWORKER_LEDGER_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "FrozenCheckExecutionReport exceeds maximum file budget",
        ));
    }
    write_private_file_atomic(
        plans_dir,
        &check_report_filename(effect_id),
        &bytes,
        "frozen_check_execution_report",
    )
}

/// Reads a persisted private (`0600`) [`FrozenCheckExecutionReport`] by semantic `effect_id`.
pub fn read_persisted_check_report(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<Option<FrozenCheckExecutionReport>, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let path = check_report_file_path(plans_dir, effect_id);
    let Some(bytes) = read_private_file_bounded(
        &path,
        MAX_COWORKER_LEDGER_BYTES,
        "frozen_check_execution_report",
    )?
    else {
        return Ok(None);
    };
    let report: FrozenCheckExecutionReport = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted FrozenCheckExecutionReport JSON",
        )
    })?;
    Ok(Some(report))
}
