use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use threadlane_authority::{validation::validate_canonical_thread_uuid, ApprovedTaskRecord};
use threadlane_host::WORK_AREA_SUBDIR;
use threadlane_operations::{
    execute_integration, load_integration_receipt, load_review_receipt, propose_integration,
    verify_review_receipt_readonly, ExecuteIntegrationRequest, IntegrationProposalRequest,
    IntegrationReceipt, IntegrationStrategy, ReviewReceipt, ReviewSubject,
};
use threadlane_workers::{
    is_commit_ancestor, is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha,
    resolve_exact_tree_sha, WorkerFilePermissions, WorkerProvider, WorktreeKind, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

use crate::conversation_change::ConversationChangeStore;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, sanitize_diagnostic_text, validate_bounded_identifier,
};

use super::candidate::map_operations_error;
use super::contracts::{
    deterministic_prefixed_id, sha256_hex, CoworkerJobPhase, CoworkerTaskRecord, CoworkerTaskStage,
    OutboxDispatchKind, OutboxDispatchRecord, OutboxDispatchStatus, TaskCandidateRef,
    MAX_ATTEMPTS_PER_TASK, MAX_COWORKER_ID_LEN,
};
use super::controller::{
    is_task_fenced_by_hold_or_sibling, CoworkerControllerService, VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, ControllerJobRuntimeState, PendingControllerEffect,
    PendingControllerEffectKind, PendingControllerEffectStatus, TaskAttemptExecutionArtifacts,
    DEFAULT_CONTROLLER_LEASE_TTL_MS,
};
use super::ownership::CoworkerJobOwnershipGuard;
use super::store::{FinalizeTaskOutboxRequest, ReserveTaskOutboxRequest};

static IN_FLIGHT_TASK_INTEGRATIONS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

fn in_flight_set() -> &'static Mutex<BTreeSet<String>> {
    IN_FLIGHT_TASK_INTEGRATIONS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

struct InFlightTaskIntegrationGuard {
    key: String,
}

impl InFlightTaskIntegrationGuard {
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
                "in-flight task integration registry lock poisoned",
            )
        })?;
        if !guard.insert(key.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "concurrent task integration call already in flight for job `{job_id}` task `{task_id}` attempt `{attempt_no}`"
                ),
            ));
        }
        Ok(Self { key })
    }
}

impl Drop for InFlightTaskIntegrationGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = in_flight_set().lock() {
            guard.remove(&self.key);
        }
    }
}

/// Request to integrate an accepted task attempt into its original owned `WorktreeKind::RunIntegration`
/// worktree under frozen controller policy and approval scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskIntegrationRequest {
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

impl CoworkerTaskIntegrationRequest {
    /// Validates request identifiers, canonical thread UUID, attempt bounds, and timestamp.
    pub fn validate(&self) -> Result<(), NativeError> {
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
            )
        })?;
        if self.attempt_no == 0 || (self.attempt_no as usize) > MAX_ATTEMPTS_PER_TASK {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "attempt_no must be between 1 and MAX_ATTEMPTS_PER_TASK",
            ));
        }
        if self.now_ms == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "now_ms must be > 0",
            ));
        }
        Ok(())
    }
}

/// Truthful execution status of [`integrate_accepted_task`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerTaskIntegrationStatus {
    /// Freshly executed `execute_integration` into the attempt's owned `RunIntegration` worktree
    /// and finalized the task to [`CoworkerTaskStage::Integrated`].
    Integrated,
    /// Reopened and independently verified an already persisted original [`IntegrationReceipt`]
    /// and promoted/confirmed [`CoworkerTaskStage::Integrated`] idempotently without re-integrating.
    ReopenedIntegrated,
    /// The verified original [`IntegrationReceipt`] was retained in runtime `attempt_artifacts` and
    /// the [`PendingControllerEffect`] was confirmed, but advancing the task stage to `Integrated`
    /// (and releasing downstream dependencies) was fenced by a hold, stop request, or change.
    FencedAfterEffect,
}

/// Typed outcome of [`integrate_accepted_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskIntegrationOutcome {
    /// Owning [`super::contracts::CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Resulting `job_revision` after integration or reconciliation.
    pub job_revision: u64,
    /// Target task identifier.
    pub task_id: String,
    /// Verified 1-based attempt number.
    pub attempt_no: u32,
    /// Current durable task stage (`Integrated` or fenced stage).
    pub task_stage: CoworkerTaskStage,
    /// Execution or reopen status.
    pub status: CoworkerTaskIntegrationStatus,
    /// Accepted [`TaskCandidateRef::candidate_id`].
    pub candidate_id: String,
    /// Bound 64-hex [`ReviewSubject::subject_digest`].
    pub review_subject_digest: String,
    /// Verified [`ReviewReceipt::receipt_id`].
    pub review_receipt_id: String,
    /// Deterministic original [`IntegrationReceipt::receipt_id`].
    pub integration_receipt_id: String,
    /// Owned `WorktreeKind::RunIntegration` worktree identifier.
    pub integration_worktree_id: String,
    /// Owned `WorktreeKind::Task` worktree identifier.
    pub task_worktree_id: String,
    /// Integration worktree 40-hex `HEAD` commit SHA prior to integration.
    pub previous_integration_head_commit: String,
    /// Resulting 40-hex target commit SHA on the integration worktree branch.
    pub integrated_target_commit: String,
    /// Resulting 40-hex `HEAD^{tree}` SHA on the integration worktree branch.
    pub integrated_tree_sha: String,
    /// Frozen integration strategy executed.
    pub strategy: IntegrationStrategy,
    /// Bound [`OutboxDispatchRecord::outbox_id`].
    pub outbox_id: String,
    /// Bound [`PendingControllerEffect::effect_id`].
    pub effect_id: String,
    /// Full verified original [`IntegrationReceipt`].
    pub integration_receipt: IntegrationReceipt,
}

/// Service wrapper over [`CoworkerControllerService`] for executing or reconciling the original
/// per-task `RunIntegration` stage.
#[derive(Debug, Clone)]
pub struct CoworkerTaskIntegrationService {
    controller: CoworkerControllerService,
}

impl CoworkerTaskIntegrationService {
    /// Creates a new [`CoworkerTaskIntegrationService`] wrapping `controller`.
    pub fn new(controller: CoworkerControllerService) -> Self {
        Self { controller }
    }

    /// Borrows the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Integrates an accepted task attempt into its original owned `RunIntegration` worktree or
    /// reopens an already-persisted original [`IntegrationReceipt`] idempotently.
    pub fn integrate_accepted_task(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskIntegrationRequest,
    ) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
        integrate_accepted_task(&self.controller, guard, request)
    }

    /// Alias for [`Self::integrate_accepted_task`].
    pub fn integrate_accepted_task_attempt(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskIntegrationRequest,
    ) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
        integrate_accepted_task(&self.controller, guard, request)
    }
}

/// Alias for [`integrate_accepted_task`].
pub fn integrate_accepted_task_attempt(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskIntegrationRequest,
) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
    integrate_accepted_task(controller, guard, request)
}

/// Integrates an accepted task attempt into its original owned `WorktreeKind::RunIntegration`
/// worktree under frozen controller admission, strategy, and scope, or reopens and verifies an
/// existing persisted original [`IntegrationReceipt`] idempotently without re-executing Git
/// mutations.
pub fn integrate_accepted_task(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskIntegrationRequest,
) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
    request.validate()?;

    let plans_dir = controller.workflow_store().plans_dir();
    let _in_flight = InFlightTaskIntegrationGuard::acquire(
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
        entry_change_revision,
        candidate_task,
        approved_task,
        accepted_candidate,
        artifacts,
        expected_strategy,
    ) = verify_live_task_integration_admission(controller, guard, request, None)?;

    let work_area_root = controller
        .sessions_dir()
        .join(&adm.approval.approval_id)
        .join(WORK_AREA_SUBDIR);

    let (task_manifest, int_manifest) = load_and_verify_owned_manifests(
        &work_area_root,
        &adm,
        &candidate_task,
        &approved_task,
        &artifacts,
    )?;

    let (review_subject, disk_review_receipt) = load_and_verify_accepted_review_proofs(
        &work_area_root,
        &adm,
        &candidate_task,
        &approved_task,
        &accepted_candidate,
        &artifacts,
        &task_manifest,
    )?;

    let source_head = verify_clean_primary_and_task_worktrees(
        &adm,
        &accepted_candidate,
        &task_manifest,
        &int_manifest,
    )?;

    let expected_subject_digest = compute_task_integration_subject_digest(
        &adm,
        request,
        &accepted_candidate,
        &artifacts,
        &task_manifest,
        &int_manifest,
        &review_subject,
        &disk_review_receipt,
        expected_strategy,
    );
    let expected_receipt_id = deterministic_task_integration_receipt_id(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &int_manifest.worktree_id,
        &expected_subject_digest,
    );
    let expected_request_key = deterministic_task_integration_request_key(
        &request.job_id,
        &request.task_id,
        request.attempt_no,
        &int_manifest.worktree_id,
        &expected_subject_digest,
    );
    let expected_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&request.job_id, &request.task_id, &expected_request_key],
    );

    let (existing_outbox, existing_effect) = inspect_existing_integration_intent(
        controller,
        request,
        &adm,
        &controller_instance_id,
        lease_epoch,
        &expected_request_key,
        &expected_effect_id,
        &expected_subject_digest,
        &expected_receipt_id,
    )?;

    let has_prior_intent_or_completion = existing_outbox.is_some()
        || existing_effect.is_some()
        || candidate_task.stage == CoworkerTaskStage::Integrated
        || accepted_candidate.integration_receipt_id.is_some()
        || artifacts.integration_receipt.is_some();

    if has_prior_intent_or_completion {
        return reconcile_or_reopen_prior_integration(
            controller,
            guard,
            request,
            &adm,
            &controller_instance_id,
            lease_epoch,
            entry_change_revision,
            &candidate_task,
            &accepted_candidate,
            &artifacts,
            &work_area_root,
            &task_manifest,
            &int_manifest,
            &review_subject,
            &disk_review_receipt,
            expected_strategy,
            &expected_subject_digest,
            &expected_receipt_id,
            &expected_request_key,
            &expected_effect_id,
            existing_outbox,
            existing_effect,
        );
    }

    // Fresh integration path: require Accepted stage with no unresolved outbox and no active fences.
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
    if candidate_task.stage != CoworkerTaskStage::Accepted {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` must be in `accepted` stage for fresh RunIntegration (current: `{}`)",
                request.task_id, candidate_task.stage
            ),
        ));
    }
    if adm.runtime_state.stop_requested
        || is_task_fenced_by_hold_or_sibling(
            &candidate_task,
            &adm.job,
            &adm.runtime_state,
            &adm.approval.approval_id,
            plans_dir,
        )?
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is fenced by an active hold, conversational change, stop request, or impacted sibling",
                request.task_id
            ),
        ));
    }

    let current_int_head = resolve_exact_commit_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve integration worktree HEAD: {err}"),
        )
    })?;
    if current_int_head != int_manifest.base_commit {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "integration worktree HEAD advanced past base_commit without a recorded TaskIntegration reservation",
        ));
    }
    if load_integration_receipt(&work_area_root, &expected_receipt_id).is_ok() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "IntegrationReceipt already exists on disk without a recorded TaskIntegration reservation",
        ));
    }

    // Read-only pre-flight check before reserving outbox or controller effect.
    let proposal = propose_integration(&IntegrationProposalRequest {
        work_area_root: work_area_root.clone(),
        integration_worktree_id: int_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject: review_subject.clone(),
        review_receipt: disk_review_receipt.clone(),
        strategy: expected_strategy,
    })
    .map_err(map_operations_error)?;
    if !proposal.clean_merge_predicted
        || proposal.integration_head_commit != current_int_head
        || proposal.task_head_commit != accepted_candidate.head_commit
        || proposal.source_head_commit != source_head
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "RunIntegration pre-check refused integration for task `{}` (clean_merge_predicted={}, can_fast_forward={}, already_integrated={})",
                request.task_id,
                proposal.clean_merge_predicted,
                proposal.can_fast_forward,
                proposal.already_integrated
            ),
        ));
    }

    // Re-verify ownership and lease immediately before persisting intent reservations.
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let (_, reserved_outbox) =
        controller
            .workflow_store()
            .reserve_task_outbox(&ReserveTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: adm.job.job_revision,
                expected_ledger_revision: None,
                request_key: expected_request_key.clone(),
                kind: OutboxDispatchKind::TaskIntegration,
                task_id: request.task_id.clone(),
                role: candidate_task.role,
                subject_digest: expected_subject_digest.clone(),
                dependency_input_commit: None,
                dependency_input_tree_sha: None,
                accepted_predecessor_receipts: Vec::new(),
            })?;

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        &controller_instance_id,
        lease_epoch,
        |state| {
            state.pending_effects.insert(
                expected_effect_id.clone(),
                PendingControllerEffect {
                    effect_id: expected_effect_id.clone(),
                    request_key: expected_request_key.clone(),
                    job_id: request.job_id.clone(),
                    task_id: Some(request.task_id.clone()),
                    attempt_no: Some(request.attempt_no),
                    kind: PendingControllerEffectKind::TaskIntegration,
                    status: PendingControllerEffectStatus::ReservedBeforeEffect,
                    subject_digest: expected_subject_digest.clone(),
                    external_correlation_id: Some(expected_receipt_id.clone()),
                    created_at_ms: request.now_ms,
                    updated_at_ms: request.now_ms,
                    diagnostic: None,
                },
            );
            Ok(())
        },
    )?;

    let operator_id = format!("ctrl_{controller_instance_id}");
    let integrated_at_ms = i64::try_from(request.now_ms).unwrap_or(i64::MAX);

    let exec_result = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area_root.clone(),
        integration_worktree_id: int_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject: review_subject.clone(),
        review_receipt: disk_review_receipt.clone(),
        expected_integration_head_commit: current_int_head.clone(),
        expected_task_head_commit: accepted_candidate.head_commit.clone(),
        expected_source_head_commit: source_head.clone(),
        strategy: expected_strategy,
        receipt_id: Some(expected_receipt_id.clone()),
        operator_id,
        integrated_at_ms,
    });

    let executed_receipt = match exec_result {
        Ok(receipt) => receipt,
        Err(op_err) => {
            let diag = sanitize_diagnostic_text(&op_err.to_string());
            mark_integration_intent_unknown(
                controller,
                request,
                &controller_instance_id,
                lease_epoch,
                Some(&reserved_outbox),
                &expected_effect_id,
                &diag,
            )?;
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "RunIntegration execution failed or became uncertain after reservation for task `{}`: {diag}",
                    request.task_id
                ),
            ));
        }
    };

    if let Err(verify_err) = verify_persisted_integration_receipt(
        &work_area_root,
        &adm,
        &accepted_candidate,
        &artifacts,
        &task_manifest,
        &int_manifest,
        &review_subject,
        &disk_review_receipt,
        expected_strategy,
        &expected_receipt_id,
        Some(&executed_receipt),
    ) {
        let diag = sanitize_diagnostic_text(verify_err.message());
        mark_integration_intent_unknown(
            controller,
            request,
            &controller_instance_id,
            lease_epoch,
            Some(&reserved_outbox),
            &expected_effect_id,
            &diag,
        )?;
        return Err(verify_err);
    }

    finalize_verified_integration_after_effect(
        controller,
        guard,
        request,
        &controller_instance_id,
        lease_epoch,
        entry_change_revision,
        &accepted_candidate,
        &artifacts,
        &disk_review_receipt,
        &executed_receipt,
        &reserved_outbox,
        &expected_effect_id,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn reconcile_or_reopen_prior_integration(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskIntegrationRequest,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    entry_change_revision: u64,
    candidate_task: &CoworkerTaskRecord,
    accepted_candidate: &TaskCandidateRef,
    artifacts: &TaskAttemptExecutionArtifacts,
    work_area_root: &Path,
    task_manifest: &WorktreeManifest,
    int_manifest: &WorktreeManifest,
    review_subject: &ReviewSubject,
    disk_review_receipt: &ReviewReceipt,
    expected_strategy: IntegrationStrategy,
    expected_subject_digest: &str,
    expected_receipt_id: &str,
    expected_request_key: &str,
    expected_effect_id: &str,
    existing_outbox: Option<OutboxDispatchRecord>,
    existing_effect: Option<PendingControllerEffect>,
) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
    if (candidate_task.stage == CoworkerTaskStage::Integrated
        || accepted_candidate.integration_receipt_id.is_some()
        || artifacts.integration_receipt.is_some())
        && (existing_outbox.is_none() || existing_effect.is_none())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task or runtime artifacts claim integration completion without both a recorded TaskIntegration outbox and PendingControllerEffect",
        ));
    }

    let disk_int_receipt = match load_integration_receipt(work_area_root, expected_receipt_id) {
        Ok(receipt) => receipt,
        Err(err) => {
            let diag = format!(
                "persisted IntegrationReceipt `{expected_receipt_id}` is missing or unreadable after TaskIntegration reservation: {}",
                sanitize_diagnostic_text(&err.to_string())
            );
            mark_integration_intent_unknown(
                controller,
                request,
                controller_instance_id,
                lease_epoch,
                existing_outbox.as_ref(),
                expected_effect_id,
                &diag,
            )?;
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
    };

    if let Err(verify_err) = verify_persisted_integration_receipt(
        work_area_root,
        adm,
        accepted_candidate,
        artifacts,
        task_manifest,
        int_manifest,
        review_subject,
        disk_review_receipt,
        expected_strategy,
        expected_receipt_id,
        Some(&disk_int_receipt),
    ) {
        let diag = sanitize_diagnostic_text(verify_err.message());
        mark_integration_intent_unknown(
            controller,
            request,
            controller_instance_id,
            lease_epoch,
            existing_outbox.as_ref(),
            expected_effect_id,
            &diag,
        )?;
        return Err(verify_err);
    }

    // Ensure PendingControllerEffect exists in runtime state if outbox was reserved first.
    if existing_effect.is_none() {
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            |state| {
                state
                    .pending_effects
                    .entry(expected_effect_id.to_string())
                    .or_insert_with(|| PendingControllerEffect {
                        effect_id: expected_effect_id.to_string(),
                        request_key: expected_request_key.to_string(),
                        job_id: request.job_id.clone(),
                        task_id: Some(request.task_id.clone()),
                        attempt_no: Some(request.attempt_no),
                        kind: PendingControllerEffectKind::TaskIntegration,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: expected_subject_digest.to_string(),
                        external_correlation_id: Some(expected_receipt_id.to_string()),
                        created_at_ms: request.now_ms,
                        updated_at_ms: request.now_ms,
                        diagnostic: None,
                    });
                Ok(())
            },
        )?;
    }

    let outbox_record = match existing_outbox {
        Some(obx) => obx,
        None => {
            if candidate_task.stage != CoworkerTaskStage::Accepted {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "TaskIntegration effect `{expected_effect_id}` has no matching outbox record and task is in stage `{}`",
                        candidate_task.stage
                    ),
                ));
            }
            let (_, reserved) =
                controller
                    .workflow_store()
                    .reserve_task_outbox(&ReserveTaskOutboxRequest {
                        binding_id: request.binding_id.clone(),
                        thread_uuid: request.thread_uuid.clone(),
                        job_id: request.job_id.clone(),
                        expected_job_revision: adm.job.job_revision,
                        expected_ledger_revision: None,
                        request_key: expected_request_key.to_string(),
                        kind: OutboxDispatchKind::TaskIntegration,
                        task_id: request.task_id.clone(),
                        role: candidate_task.role,
                        subject_digest: expected_subject_digest.to_string(),
                        dependency_input_commit: None,
                        dependency_input_tree_sha: None,
                        accepted_predecessor_receipts: Vec::new(),
                    })?;
            reserved
        }
    };

    finalize_verified_integration_after_effect(
        controller,
        guard,
        request,
        controller_instance_id,
        lease_epoch,
        entry_change_revision,
        accepted_candidate,
        artifacts,
        disk_review_receipt,
        &disk_int_receipt,
        &outbox_record,
        expected_effect_id,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_verified_integration_after_effect(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskIntegrationRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    entry_change_revision: u64,
    accepted_candidate: &TaskCandidateRef,
    artifacts: &TaskAttemptExecutionArtifacts,
    disk_review_receipt: &ReviewReceipt,
    verified_int_receipt: &IntegrationReceipt,
    outbox_record: &OutboxDispatchRecord,
    expected_effect_id: &str,
    is_reopen: bool,
) -> Result<CoworkerTaskIntegrationOutcome, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    // Always retain the verified original IntegrationReceipt and confirm the PendingControllerEffect
    // in runtime state, even if post-effect task stage advancement is subsequently fenced.
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
                    "TaskAttemptExecutionArtifacts missing during IntegrationReceipt retention",
                )
            })?;
            if let Some(ref existing_int) = art.integration_receipt {
                if existing_int != verified_int_receipt || existing_int.job_aggregation.is_some() {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskAttemptExecutionArtifacts already has a different or non-original IntegrationReceipt",
                    ));
                }
            } else {
                art.integration_receipt = Some(verified_int_receipt.clone());
            }
            if let Some(ref existing_rev) = art.review_receipt {
                if existing_rev != disk_review_receipt {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "TaskAttemptExecutionArtifacts.review_receipt drifted from verified ReviewReceipt",
                    ));
                }
            } else {
                art.review_receipt = Some(disk_review_receipt.clone());
            }

            let eff = state
                .pending_effects
                .get_mut(expected_effect_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "PendingControllerEffect missing during IntegrationReceipt confirmation",
                    )
                })?;
            eff.status = PendingControllerEffectStatus::EffectConfirmed;
            eff.external_correlation_id = Some(verified_int_receipt.receipt_id.clone());
            eff.updated_at_ms = request.now_ms;
            eff.diagnostic = None;
            Ok(())
        },
    )?;

    let (post_adm, _, _, post_change_rev, post_task, _, post_cand, _, _) =
        verify_live_task_integration_admission(
            controller,
            guard,
            request,
            Some((controller_instance_id, lease_epoch)),
        )?;

    // If already finalized to Integrated, verify idempotent state and return ReopenedIntegrated.
    if post_task.stage == CoworkerTaskStage::Integrated
        && matches!(
            outbox_record.status,
            OutboxDispatchStatus::DispatchedEffectConfirmed
                | OutboxDispatchStatus::ReconciledConfirmed
        )
    {
        if post_cand.integration_receipt_id.as_deref()
            != Some(verified_int_receipt.receipt_id.as_str())
            || outbox_record.receipt_id.as_deref() != Some(verified_int_receipt.receipt_id.as_str())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Integrated task candidate or outbox receipt_id does not match verified original IntegrationReceipt",
            ));
        }
        return Ok(CoworkerTaskIntegrationOutcome {
            job_id: request.job_id.clone(),
            job_revision: post_adm.job.job_revision,
            task_id: request.task_id.clone(),
            attempt_no: request.attempt_no,
            task_stage: post_task.stage,
            status: CoworkerTaskIntegrationStatus::ReopenedIntegrated,
            candidate_id: accepted_candidate.candidate_id.clone(),
            review_subject_digest: verified_int_receipt.review_subject_digest.clone(),
            review_receipt_id: verified_int_receipt.review_receipt_id.clone(),
            integration_receipt_id: verified_int_receipt.receipt_id.clone(),
            integration_worktree_id: verified_int_receipt.integration_worktree_id.clone(),
            task_worktree_id: verified_int_receipt.task_worktree_id.clone(),
            previous_integration_head_commit: verified_int_receipt
                .previous_integration_head_commit
                .clone(),
            integrated_target_commit: verified_int_receipt.integrated_target_commit.clone(),
            integrated_tree_sha: verified_int_receipt.integrated_tree_sha.clone(),
            strategy: verified_int_receipt.strategy,
            outbox_id: outbox_record.outbox_id.clone(),
            effect_id: expected_effect_id.to_string(),
            integration_receipt: verified_int_receipt.clone(),
        });
    }

    // Scope/hold/stop/revision fence check AFTER effect and BEFORE advancing task stage to Integrated.
    let is_fenced = post_adm.runtime_state.stop_requested
        || post_change_rev > entry_change_revision
        || post_adm.job.phase != CoworkerJobPhase::Executing
        || is_task_fenced_by_hold_or_sibling(
            &post_task,
            &post_adm.job,
            &post_adm.runtime_state,
            &post_adm.approval.approval_id,
            plans_dir,
        )?;

    if is_fenced {
        return Ok(CoworkerTaskIntegrationOutcome {
            job_id: request.job_id.clone(),
            job_revision: post_adm.job.job_revision,
            task_id: request.task_id.clone(),
            attempt_no: request.attempt_no,
            task_stage: post_task.stage,
            status: CoworkerTaskIntegrationStatus::FencedAfterEffect,
            candidate_id: accepted_candidate.candidate_id.clone(),
            review_subject_digest: verified_int_receipt.review_subject_digest.clone(),
            review_receipt_id: verified_int_receipt.review_receipt_id.clone(),
            integration_receipt_id: verified_int_receipt.receipt_id.clone(),
            integration_worktree_id: verified_int_receipt.integration_worktree_id.clone(),
            task_worktree_id: verified_int_receipt.task_worktree_id.clone(),
            previous_integration_head_commit: verified_int_receipt
                .previous_integration_head_commit
                .clone(),
            integrated_target_commit: verified_int_receipt.integrated_target_commit.clone(),
            integrated_tree_sha: verified_int_receipt.integrated_tree_sha.clone(),
            strategy: verified_int_receipt.strategy,
            outbox_id: outbox_record.outbox_id.clone(),
            effect_id: expected_effect_id.to_string(),
            integration_receipt: verified_int_receipt.clone(),
        });
    }

    let finalize_status = if outbox_record.status == OutboxDispatchStatus::EffectUnknown {
        OutboxDispatchStatus::ReconciledConfirmed
    } else {
        OutboxDispatchStatus::DispatchedEffectConfirmed
    };

    let (final_job, final_outbox) =
        controller
            .workflow_store()
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: post_adm.job.job_revision,
                expected_ledger_revision: None,
                outbox_id: outbox_record.outbox_id.clone(),
                status: finalize_status,
                next_task_stage: CoworkerTaskStage::Integrated,
                run_id: Some(artifacts.run_id.clone()),
                worker_id: Some(artifacts.worker_id.clone()),
                task_worktree_id: Some(artifacts.task_worktree_id.clone()),
                head_commit: Some(verified_int_receipt.integrated_target_commit.clone()),
                tree_sha: Some(verified_int_receipt.integrated_tree_sha.clone()),
                diff_digest: Some(accepted_candidate.diff_digest.clone()),
                report_artifact_id: None,
                observed_model: Some(artifacts.observed_model.clone()),
                observed_effort: artifacts.observed_effort.clone(),
                receipt_id: Some(verified_int_receipt.receipt_id.clone()),
                diagnostic: None,
            })?;

    let status = if is_reopen {
        CoworkerTaskIntegrationStatus::ReopenedIntegrated
    } else {
        CoworkerTaskIntegrationStatus::Integrated
    };

    Ok(CoworkerTaskIntegrationOutcome {
        job_id: request.job_id.clone(),
        job_revision: final_job.job_revision,
        task_id: request.task_id.clone(),
        attempt_no: request.attempt_no,
        task_stage: CoworkerTaskStage::Integrated,
        status,
        candidate_id: accepted_candidate.candidate_id.clone(),
        review_subject_digest: verified_int_receipt.review_subject_digest.clone(),
        review_receipt_id: verified_int_receipt.review_receipt_id.clone(),
        integration_receipt_id: verified_int_receipt.receipt_id.clone(),
        integration_worktree_id: verified_int_receipt.integration_worktree_id.clone(),
        task_worktree_id: verified_int_receipt.task_worktree_id.clone(),
        previous_integration_head_commit: verified_int_receipt
            .previous_integration_head_commit
            .clone(),
        integrated_target_commit: verified_int_receipt.integrated_target_commit.clone(),
        integrated_tree_sha: verified_int_receipt.integrated_tree_sha.clone(),
        strategy: verified_int_receipt.strategy,
        outbox_id: final_outbox.outbox_id,
        effect_id: expected_effect_id.to_string(),
        integration_receipt: verified_int_receipt.clone(),
    })
}

#[allow(clippy::type_complexity)]
fn verify_live_task_integration_admission(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskIntegrationRequest,
    expected_lease: Option<(&str, u64)>,
) -> Result<
    (
        VerifiedControllerAdmission,
        String,
        u64,
        u64,
        CoworkerTaskRecord,
        ApprovedTaskRecord,
        TaskCandidateRef,
        TaskAttemptExecutionArtifacts,
        IntegrationStrategy,
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
                "controller driver lease changed during task integration",
            ));
        }
    }

    if !adm.linkage.authorizes_local_git_integration()
        || !adm.runtime_state.envelope.allow_run_integration_execution
    {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "local RunIntegration execution is not authorized by FrozenApprovalLinkage and FrozenControllerExecutionEnvelope",
        ));
    }
    let expected_strategy = adm.runtime_state.envelope.default_integration_strategy;

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
                format!("failed to read ConversationChangeStore during task integration: {err}"),
            )
        })?;
    if expected_lease.is_none()
        && change_summary.current_change_revision > adm.runtime_state.synced_change_revision
    {
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

    if candidate_task.current_attempt_no != request.attempt_no {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` current_attempt_no `{}` does not match requested attempt_no `{}`",
                request.task_id, candidate_task.current_attempt_no, request.attempt_no
            ),
        ));
    }

    let accepted_candidate_id = candidate_task
        .accepted_candidate_id
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` has no accepted_candidate_id; refusing RunIntegration without accepted proof",
                    request.task_id
                ),
            )
        })?;
    let accepted_candidate = candidate_task
        .candidates
        .iter()
        .find(|c| &c.candidate_id == accepted_candidate_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "accepted_candidate_id `{accepted_candidate_id}` not found in task `{}` candidates",
                    request.task_id
                ),
            )
        })?;
    accepted_candidate.validate()?;
    if accepted_candidate.task_id != request.task_id
        || accepted_candidate.attempt_no != request.attempt_no
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "accepted TaskCandidateRef task_id or attempt_no does not match requested task attempt",
        ));
    }
    let verdict = accepted_candidate.review_verdict.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "accepted TaskCandidateRef is missing StructuredReviewVerdict",
        )
    })?;
    if !verdict.allows_acceptance()
        || !constant_time_str_eq(&verdict.policy_digest, &adm.linkage.policy_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "accepted TaskCandidateRef review_verdict is not ReviewVerdictOutcome::Accept under the frozen policy_digest",
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
    if artifacts.observed_model != candidate_task.selected_choice.model_arg
        || artifacts.observed_model != approved_task.base_model
        || approved_task.provider.map(WorkerProvider::as_str)
            != Some(artifacts.observed_provider.as_str())
        || artifacts.observed_effort != candidate_task.selected_choice.effort
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts observed provider/model/effort does not match frozen coder choice",
        ));
    }
    if artifacts.committed_head_commit.as_deref() != Some(accepted_candidate.head_commit.as_str())
        || artifacts.committed_tree_sha.as_deref() != Some(accepted_candidate.tree_sha.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "TaskAttemptExecutionArtifacts committed_head_commit or committed_tree_sha does not match accepted candidate",
        ));
    }
    if let Some(ref art_cand_id) = artifacts.candidate_id {
        if art_cand_id != &accepted_candidate.candidate_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts.candidate_id does not match accepted_candidate_id",
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
    if artifacts.execution_base_commit != expected_base_commit
        || accepted_candidate.base_commit != expected_base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "execution_base_commit or accepted candidate base_commit does not match expected task base_commit",
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

    Ok((
        adm,
        controller_instance_id,
        lease_epoch,
        change_summary.current_change_revision,
        candidate_task,
        approved_task,
        accepted_candidate,
        artifacts,
        expected_strategy,
    ))
}

fn load_and_verify_owned_manifests(
    work_area_root: &Path,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    artifacts: &TaskAttemptExecutionArtifacts,
) -> Result<(WorktreeManifest, WorktreeManifest), NativeError> {
    let task_manifest = load_and_verify_manifest(work_area_root, &artifacts.task_worktree_id)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to load and verify owned task worktree manifest `{}`: {err}",
                    artifacts.task_worktree_id
                ),
            )
        })?;
    if task_manifest.kind != WorktreeKind::Task {
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
        || int_manifest.run_id != task_manifest.run_id
        || int_manifest.repository_path != task_manifest.repository_path
        || int_manifest.work_area_root != task_manifest.work_area_root
        || int_manifest.base_commit != task_manifest.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "integration WorktreeManifest does not match task WorktreeManifest ownership, run_id, repository, or base_commit",
        ));
    }

    let expected_write_paths: BTreeSet<String> =
        candidate_task.write_paths.iter().cloned().collect();
    if task_manifest.worktree_id != artifacts.task_worktree_id
        || task_manifest
            .task_id
            .as_deref()
            .is_none_or(|id| id.trim().is_empty())
        || task_manifest.repository_path != Path::new(&adm.linkage.project_base.repository_path)
        || task_manifest.repository_path != adm.approval.repository_path
        || task_manifest.base_commit != artifacts.execution_base_commit
        || candidate_task.write_paths != approved_task.allowed_write_paths
        || task_manifest.allowed_write_paths != expected_write_paths
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task WorktreeManifest ownership, repository, base_commit, or allowed_write_paths drifted from frozen task scope",
        ));
    }

    Ok((task_manifest, int_manifest))
}

fn load_and_verify_accepted_review_proofs(
    work_area_root: &Path,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    accepted_candidate: &TaskCandidateRef,
    artifacts: &TaskAttemptExecutionArtifacts,
    task_manifest: &WorktreeManifest,
) -> Result<(ReviewSubject, ReviewReceipt), NativeError> {
    let review_subject_digest = accepted_candidate
        .review_subject_digest
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "accepted TaskCandidateRef is missing review_subject_digest",
            )
        })?;
    let review_receipt_id = accepted_candidate
        .review_receipt_id
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "accepted TaskCandidateRef is missing review_receipt_id",
            )
        })?;
    let runtime_subject = artifacts
        .review_subject
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts is missing ReviewSubject",
            )
        })?
        .clone();

    let disk_review_receipt =
        load_review_receipt(work_area_root, review_receipt_id).map_err(map_operations_error)?;

    if let Some(ref rt_receipt) = artifacts.review_receipt {
        if rt_receipt != &disk_review_receipt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts.review_receipt does not match persisted ReviewReceipt on disk",
            ));
        }
    }

    verify_review_receipt_readonly(&runtime_subject, &disk_review_receipt)
        .map_err(map_operations_error)?;

    if disk_review_receipt.receipt_id != *review_receipt_id
        || !constant_time_str_eq(&runtime_subject.subject_digest, review_subject_digest)
        || !constant_time_str_eq(&disk_review_receipt.subject_digest, review_subject_digest)
        || runtime_subject.run_id != task_manifest.run_id
        || Some(runtime_subject.task_id.as_str()) != task_manifest.task_id.as_deref()
        || runtime_subject.task_worktree_id != task_manifest.worktree_id
        || runtime_subject.work_area_root != task_manifest.work_area_root
        || runtime_subject.repository_path != task_manifest.repository_path
        || runtime_subject.base_commit != artifacts.execution_base_commit
        || runtime_subject.task_head_commit != accepted_candidate.head_commit
        || runtime_subject.task_tree_sha != accepted_candidate.tree_sha
        || !constant_time_str_eq(
            &runtime_subject.diff_digest,
            &accepted_candidate.diff_digest,
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ReviewSubject or persisted ReviewReceipt does not match accepted candidate and owned task manifest",
        ));
    }

    let expected_file_perms = approved_task
        .file_permissions
        .unwrap_or(WorkerFilePermissions::TaskFiles);
    if runtime_subject.policy.policy_id != adm.runtime_state.envelope.policy.policy_id
        || runtime_subject.policy.requested_provider
            != approved_task.provider.map(|p| p.as_str().to_string())
        || runtime_subject.policy.requested_model.as_deref()
            != Some(candidate_task.selected_choice.model_arg.as_str())
        || runtime_subject.policy.requested_effort != candidate_task.selected_choice.effort
        || runtime_subject.policy.observed_model.as_deref()
            != Some(artifacts.observed_model.as_str())
        || runtime_subject.policy.observed_effort != artifacts.observed_effort
        || runtime_subject.policy.file_permissions != expected_file_perms
        || runtime_subject.policy.require_effort_evidence
            != candidate_task.selected_choice.effort.is_some()
        || !runtime_subject.policy.require_passing_tests
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ReviewSubject.policy does not match frozen execution policy and pinned task model/effort",
        ));
    }

    if let Some(ref rt_test_summary) = artifacts.test_evidence_summary {
        if rt_test_summary != &runtime_subject.test_evidence {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts.test_evidence_summary does not match ReviewSubject.test_evidence",
            ));
        }
    }

    Ok((runtime_subject, disk_review_receipt))
}

fn verify_clean_primary_and_task_worktrees(
    adm: &VerifiedControllerAdmission,
    accepted_candidate: &TaskCandidateRef,
    task_manifest: &WorktreeManifest,
    int_manifest: &WorktreeManifest,
) -> Result<String, NativeError> {
    let primary_clean = is_worktree_clean(
        &task_manifest.repository_path,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to inspect primary repository cleanliness: {err}"),
        )
    })?;
    if !primary_clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "primary source repository checkout is dirty; refusing RunIntegration",
        ));
    }

    let source_head = resolve_exact_commit_sha(
        &task_manifest.repository_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve primary source repository HEAD: {err}"),
        )
    })?;
    let source_tree = resolve_exact_tree_sha(
        &task_manifest.repository_path,
        &source_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve primary source repository tree SHA: {err}"),
        )
    })?;
    if source_head != adm.linkage.project_base.base_commit
        || source_head != adm.approval.base_commit
        || source_tree != adm.linkage.project_base.base_tree_sha
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "primary source repository HEAD or tree drifted from frozen project_base",
        ));
    }

    let task_clean =
        is_worktree_clean(&task_manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to inspect task worktree cleanliness: {err}"),
                )
            },
        )?;
    if !task_clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "owned task worktree is dirty; refusing RunIntegration",
        ));
    }
    let task_head = resolve_exact_commit_sha(
        &task_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve task worktree HEAD: {err}"),
        )
    })?;
    let task_tree = resolve_exact_tree_sha(
        &task_manifest.checkout_path,
        &task_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve task worktree tree SHA: {err}"),
        )
    })?;
    if task_head != accepted_candidate.head_commit || task_tree != accepted_candidate.tree_sha {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live task worktree HEAD or tree SHA drifted from accepted candidate",
        ));
    }

    let int_clean = is_worktree_clean(&int_manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to inspect integration worktree cleanliness: {err}"),
            )
        })?;
    if !int_clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "owned RunIntegration worktree is dirty; refusing RunIntegration",
        ));
    }

    Ok(source_head)
}

#[allow(clippy::too_many_arguments)]
fn verify_persisted_integration_receipt(
    work_area_root: &Path,
    adm: &VerifiedControllerAdmission,
    accepted_candidate: &TaskCandidateRef,
    artifacts: &TaskAttemptExecutionArtifacts,
    task_manifest: &WorktreeManifest,
    int_manifest: &WorktreeManifest,
    review_subject: &ReviewSubject,
    disk_review_receipt: &ReviewReceipt,
    expected_strategy: IntegrationStrategy,
    expected_receipt_id: &str,
    in_memory_receipt: Option<&IntegrationReceipt>,
) -> Result<IntegrationReceipt, NativeError> {
    let disk_receipt = load_integration_receipt(work_area_root, expected_receipt_id)
        .map_err(map_operations_error)?;
    if let Some(mem_rec) = in_memory_receipt {
        if mem_rec != &disk_receipt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "in-memory IntegrationReceipt does not match persisted IntegrationReceipt on disk",
            ));
        }
    }

    if disk_receipt.job_aggregation.is_some() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "per-task RunIntegration receipt must be an original receipt (job_aggregation == None)",
        ));
    }

    disk_receipt
        .verify_against_review(review_subject, disk_review_receipt)
        .map_err(map_operations_error)?;

    if disk_receipt.receipt_id != expected_receipt_id
        || disk_receipt.run_id != task_manifest.run_id
        || disk_receipt.task_id != review_subject.task_id
        || disk_receipt.integration_worktree_id != int_manifest.worktree_id
        || disk_receipt.task_worktree_id != task_manifest.worktree_id
        || disk_receipt.repository_path != task_manifest.repository_path
        || disk_receipt.work_area_root != task_manifest.work_area_root
        || disk_receipt.base_commit != artifacts.execution_base_commit
        || disk_receipt.previous_integration_head_commit != int_manifest.base_commit
        || disk_receipt.accepted_task_head_commit != accepted_candidate.head_commit
        || disk_receipt.source_head_commit != adm.linkage.project_base.base_commit
        || disk_receipt.strategy != expected_strategy
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "persisted IntegrationReceipt fields do not match expected task attempt, manifests, start head, or strategy",
        ));
    }

    if let Some(ref existing_art_int) = artifacts.integration_receipt {
        if existing_art_int != &disk_receipt || existing_art_int.job_aggregation.is_some() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskAttemptExecutionArtifacts.integration_receipt was overwritten or does not match persisted original IntegrationReceipt",
            ));
        }
    }
    if let Some(ref existing_cand_int_id) = accepted_candidate.integration_receipt_id {
        if existing_cand_int_id != &disk_receipt.receipt_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "accepted TaskCandidateRef.integration_receipt_id does not match persisted original IntegrationReceipt",
            ));
        }
    }

    let _ = verify_clean_primary_and_task_worktrees(
        adm,
        accepted_candidate,
        task_manifest,
        int_manifest,
    )?;

    let live_int_head = resolve_exact_commit_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve integration worktree HEAD after integration: {err}"),
        )
    })?;
    let live_int_tree = resolve_exact_tree_sha(
        &int_manifest.checkout_path,
        &live_int_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve integration worktree tree SHA after integration: {err}"),
        )
    })?;
    if live_int_head != disk_receipt.integrated_target_commit
        || live_int_tree != disk_receipt.integrated_tree_sha
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live RunIntegration worktree HEAD or tree SHA does not match IntegrationReceipt",
        ));
    }

    if expected_strategy == IntegrationStrategy::FastForwardOnly
        && (disk_receipt.integrated_target_commit != accepted_candidate.head_commit
            || disk_receipt.integrated_tree_sha != accepted_candidate.tree_sha)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "FastForwardOnly IntegrationReceipt target commit or tree SHA does not match accepted candidate",
        ));
    }

    let base_is_anc = is_commit_ancestor(
        &int_manifest.checkout_path,
        &int_manifest.base_commit,
        &live_int_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to verify integration base ancestry: {err}"),
        )
    })?;
    let task_is_anc = is_commit_ancestor(
        &int_manifest.checkout_path,
        &accepted_candidate.head_commit,
        &live_int_head,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to verify accepted task commit ancestry in integration worktree: {err}"
            ),
        )
    })?;
    if !base_is_anc || !task_is_anc {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live RunIntegration worktree HEAD does not contain base_commit and accepted_task_head_commit",
        ));
    }

    Ok(disk_receipt)
}

#[allow(clippy::too_many_arguments)]
fn inspect_existing_integration_intent(
    controller: &CoworkerControllerService,
    request: &CoworkerTaskIntegrationRequest,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    expected_request_key: &str,
    expected_effect_id: &str,
    expected_subject_digest: &str,
    expected_receipt_id: &str,
) -> Result<
    (
        Option<OutboxDispatchRecord>,
        Option<PendingControllerEffect>,
    ),
    NativeError,
> {
    let task_int_outboxes: Vec<&OutboxDispatchRecord> = adm
        .job
        .outbox
        .iter()
        .filter(|o| o.task_id == request.task_id && o.kind == OutboxDispatchKind::TaskIntegration)
        .collect();
    if task_int_outboxes.len() > 1 {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` has multiple TaskIntegration outbox records",
                request.task_id
            ),
        ));
    }
    let existing_outbox = task_int_outboxes.first().map(|o| (*o).clone());

    let task_int_effects: Vec<&PendingControllerEffect> = adm
        .runtime_state
        .pending_effects
        .values()
        .filter(|e| {
            e.kind == PendingControllerEffectKind::TaskIntegration
                && e.task_id.as_deref() == Some(request.task_id.as_str())
        })
        .collect();
    if task_int_effects.len() > 1 {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` has multiple TaskIntegration pending controller effects",
                request.task_id
            ),
        ));
    }
    let existing_effect = task_int_effects.first().map(|e| (*e).clone());

    if let Some(ref obx) = existing_outbox {
        if obx.attempt_no != request.attempt_no
            || obx.request_key != expected_request_key
            || !constant_time_str_eq(&obx.subject_digest, expected_subject_digest)
            || obx
                .receipt_id
                .as_deref()
                .is_some_and(|r| r != expected_receipt_id)
        {
            let diag = format!(
                "task `{}` has a conflicting TaskIntegration outbox `{}` (`{}`)",
                request.task_id, obx.outbox_id, obx.request_key
            );
            mark_integration_intent_unknown(
                controller,
                request,
                controller_instance_id,
                lease_epoch,
                Some(obx),
                existing_effect
                    .as_ref()
                    .map(|e| e.effect_id.as_str())
                    .unwrap_or(expected_effect_id),
                &diag,
            )?;
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
        if matches!(
            obx.status,
            OutboxDispatchStatus::RefusedBeforeEffect | OutboxDispatchStatus::ReconciledNotExecuted
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskIntegration outbox was previously refused or reconciled as not executed; blind retry is forbidden",
            ));
        }
    }

    if let Some(ref eff) = existing_effect {
        if eff.attempt_no != Some(request.attempt_no)
            || eff.effect_id != expected_effect_id
            || eff.request_key != expected_request_key
            || !constant_time_str_eq(&eff.subject_digest, expected_subject_digest)
            || eff.external_correlation_id.as_deref() != Some(expected_receipt_id)
        {
            let diag = format!(
                "task `{}` has a conflicting TaskIntegration effect `{}` (`{}`)",
                request.task_id, eff.effect_id, eff.request_key
            );
            mark_integration_intent_unknown(
                controller,
                request,
                controller_instance_id,
                lease_epoch,
                existing_outbox.as_ref(),
                &eff.effect_id,
                &diag,
            )?;
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
        if matches!(
            eff.status,
            PendingControllerEffectStatus::RefusedBeforeEffect
                | PendingControllerEffectStatus::EffectFailedTerminal
        ) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "TaskIntegration pending effect was previously refused or failed terminally; blind retry is forbidden",
            ));
        }
    }

    Ok((existing_outbox, existing_effect))
}

fn mark_integration_intent_unknown(
    controller: &CoworkerControllerService,
    request: &CoworkerTaskIntegrationRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    outbox: Option<&OutboxDispatchRecord>,
    effect_id: &str,
    diagnostic: &str,
) -> Result<(), NativeError> {
    if let Some(obx) = outbox {
        if obx.status == OutboxDispatchStatus::ReservedBeforeEffect {
            if let Ok(ledger) = controller
                .workflow_store()
                .read_binding_ledger(&request.binding_id)
            {
                if let Some(live_job) = ledger.jobs.iter().find(|j| j.job_id == request.job_id) {
                    let _ = controller.workflow_store().reconcile_or_finalize_outbox(
                        &FinalizeTaskOutboxRequest {
                            binding_id: request.binding_id.clone(),
                            thread_uuid: request.thread_uuid.clone(),
                            job_id: request.job_id.clone(),
                            expected_job_revision: live_job.job_revision,
                            expected_ledger_revision: None,
                            outbox_id: obx.outbox_id.clone(),
                            status: OutboxDispatchStatus::EffectUnknown,
                            next_task_stage: CoworkerTaskStage::EffectUnknown,
                            run_id: None,
                            worker_id: None,
                            task_worktree_id: None,
                            head_commit: None,
                            tree_sha: None,
                            diff_digest: None,
                            report_artifact_id: None,
                            observed_model: None,
                            observed_effort: None,
                            receipt_id: None,
                            diagnostic: Some(diagnostic.to_string()),
                        },
                    );
                }
            }
        }
    }

    let _ = controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                if eff.status == PendingControllerEffectStatus::ReservedBeforeEffect {
                    eff.status = PendingControllerEffectStatus::EffectUnknown;
                    eff.updated_at_ms = request.now_ms;
                    eff.diagnostic = Some(sanitize_diagnostic_text(diagnostic));
                }
            }
            Ok(())
        },
    );

    Ok(())
}

#[derive(Serialize)]
struct TaskIntegrationSubjectSeed<'a> {
    domain: &'static str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    job_id: &'a str,
    approval_id: &'a str,
    approval_subject_digest: &'a str,
    linkage_policy_digest: &'a str,
    envelope_approval_subject_digest: &'a str,
    envelope_policy_digest: &'a str,
    allow_run_integration_execution: bool,
    strategy: &'static str,
    task_id: &'a str,
    attempt_no: u32,
    candidate_id: &'a str,
    run_id: &'a str,
    worker_id: &'a str,
    manifest_run_id: &'a str,
    manifest_task_id: &'a str,
    task_worktree_id: &'a str,
    integration_worktree_id: &'a str,
    expected_integration_start_head: &'a str,
    accepted_task_head_commit: &'a str,
    accepted_task_tree_sha: &'a str,
    diff_digest: &'a str,
    source_base_commit: &'a str,
    review_subject_digest: &'a str,
    review_receipt_id: &'a str,
    review_receipt_digest: &'a str,
}

/// Computes the canonical 64-character lowercase hex SHA-256 subject digest for a per-task
/// `RunIntegration` controller effect and outbox reservation.
#[allow(clippy::too_many_arguments)]
pub fn compute_task_integration_subject_digest(
    adm: &VerifiedControllerAdmission,
    request: &CoworkerTaskIntegrationRequest,
    accepted_candidate: &TaskCandidateRef,
    artifacts: &TaskAttemptExecutionArtifacts,
    task_manifest: &WorktreeManifest,
    int_manifest: &WorktreeManifest,
    review_subject: &ReviewSubject,
    review_receipt: &ReviewReceipt,
    strategy: IntegrationStrategy,
) -> String {
    let strategy_label = match strategy {
        IntegrationStrategy::FastForwardOnly => "fast_forward_only",
        IntegrationStrategy::MergeNoFf => "merge_no_ff",
    };
    let seed = TaskIntegrationSubjectSeed {
        domain: "threadlane-native:coworker-task-integration-subject:v1",
        binding_id: &request.binding_id,
        thread_uuid: &request.thread_uuid,
        job_id: &request.job_id,
        approval_id: &adm.linkage.approval_id,
        approval_subject_digest: &adm.linkage.approval_subject_digest,
        linkage_policy_digest: &adm.linkage.policy_digest,
        envelope_approval_subject_digest: &adm.runtime_state.envelope.approval_subject_digest,
        envelope_policy_digest: &adm.runtime_state.envelope.policy_digest,
        allow_run_integration_execution: adm.runtime_state.envelope.allow_run_integration_execution,
        strategy: strategy_label,
        task_id: &request.task_id,
        attempt_no: request.attempt_no,
        candidate_id: &accepted_candidate.candidate_id,
        run_id: &artifacts.run_id,
        worker_id: &artifacts.worker_id,
        manifest_run_id: &task_manifest.run_id,
        manifest_task_id: &review_subject.task_id,
        task_worktree_id: &task_manifest.worktree_id,
        integration_worktree_id: &int_manifest.worktree_id,
        expected_integration_start_head: &int_manifest.base_commit,
        accepted_task_head_commit: &accepted_candidate.head_commit,
        accepted_task_tree_sha: &accepted_candidate.tree_sha,
        diff_digest: &accepted_candidate.diff_digest,
        source_base_commit: &adm.linkage.project_base.base_commit,
        review_subject_digest: &review_subject.subject_digest,
        review_receipt_id: &review_receipt.receipt_id,
        review_receipt_digest: &review_receipt.receipt_digest,
    };
    let bytes =
        serde_json::to_vec(&seed).expect("task integration subject seed serialization cannot fail");
    sha256_hex(&bytes)
}

/// Computes the deterministic stable `int_<32hex>` receipt identifier for a task attempt's
/// original `RunIntegration` execution.
pub fn deterministic_task_integration_receipt_id(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    integration_worktree_id: &str,
    integration_subject_digest: &str,
) -> String {
    let attempt_str = attempt_no.to_string();
    deterministic_prefixed_id(
        "threadlane-native:coworker-task-integration-receipt:v1",
        "int",
        &[
            job_id,
            task_id,
            &attempt_str,
            integration_worktree_id,
            integration_subject_digest,
        ],
    )
}

/// Computes the deterministic stable `tik_<32hex>` request key for a task attempt's original
/// `RunIntegration` outbox and controller effect reservation.
pub fn deterministic_task_integration_request_key(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    integration_worktree_id: &str,
    integration_subject_digest: &str,
) -> String {
    let attempt_str = attempt_no.to_string();
    deterministic_prefixed_id(
        "threadlane-native:coworker-task-integration-key:v1",
        "tik",
        &[
            job_id,
            task_id,
            &attempt_str,
            integration_worktree_id,
            integration_subject_digest,
        ],
    )
}
