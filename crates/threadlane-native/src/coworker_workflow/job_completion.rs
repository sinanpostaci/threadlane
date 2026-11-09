//! Trusted controller stage for final multi-worker job Git aggregation, frozen combined
//! candidate check execution, candidate assembly, and single candidate-bound `Closure` evaluation.
//!
//! ## Core Invariants
//!
//! - **Held physical OS ownership**: Every entry point requires a live [`CoworkerJobOwnershipGuard`]
//!   held across all `.await` points and verified before and after every effect or promotion.
//! - **No caller overrides**: [`CoworkerJobCompletionRequest`] accepts only `binding_id`,
//!   `thread_uuid`, `job_id`, and `now_ms`. All repository paths, base commits, write scopes,
//!   integration strategies, check command specs, and `Closure` model/effort/budget selections are
//!   derived exclusively from the verified frozen [`threadlane_authority::ApprovalRecord`] and
//!   [`super::controller_policy::FrozenControllerExecutionEnvelope`].
//! - **Durable intent before Git creation or mutation**: A [`PendingControllerEffectKind::JobAggregation`]
//!   effect is reserved in [`super::controller_policy::ControllerPolicyStore`] before creating or
//!   mutating the final owned `RunIntegration` worktree, and a [`JobIntegrationIntent`] is persisted
//!   before executing multi-worker Git merges.
//! - **Distinct logical vs. compact manifest/runtime identifiers**: Logical task IDs
//!   ([`CoworkerTaskRecord::task_id`]) remain distinct from original host run UUIDs and compact
//!   manifest `run_id` / `task_id` tags. Original per-worker [`IntegrationReceipt`] records and
//!   candidate `integration_receipt_id` references are preserved untouched; final job-aggregation
//!   receipts use distinct deterministic receipt IDs.
//! - **Separate bounded `0600` check report persistence**: Full [`FrozenCheckExecutionReport`]
//!   artifacts (supporting full `MAX_STDOUT_BYTES` and `MAX_STDERR_BYTES` byte captures) and
//!   persisted [`JobIntegrationReport`] artifacts are stored in private `0600` files separate from
//!   the 1 MiB workflow/policy ledgers.
//! - **Truthful completion & preservation**: A job transitions to [`CoworkerJobPhase::Completed`]
//!   if and only if all tasks are integrated, all required frozen candidate checks pass, the
//!   combined candidate is assembled, and `Closure` returns [`ClosureVerdictKind::Accepted`].
//!   The owned `RunIntegration` worktree is preserved on disk for human inspection; this stage
//!   never merges into `main`, pushes, or cleans up worktrees.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use serde::{Deserialize, Serialize};
use threadlane_authority::validation::validate_canonical_thread_uuid;
use threadlane_authority::{build_approved_worker_profile_config, ApprovedTaskRecord};
use threadlane_host::WORK_AREA_SUBDIR;
use threadlane_operations::{
    execute_job_integration, inspect_job_integration_intent_readonly, load_integration_receipt,
    load_review_receipt, propose_job_integration, verify_review_receipt_readonly,
    ClosureReservationOutcome, ClosureVerdict, ClosureVerdictKind, ExecuteJobIntegrationRequest,
    FinalCandidateClosureStore, FinalCandidateSubject, IntegrationReceipt, IntegrationStrategy,
    JobIntegrationIntent, JobIntegrationIntentState, JobIntegrationReport,
    JobIntegrationScopeProvenance, JobIntegrationTaskInput, JobTaskDependencyExecutionProof,
    OperationsError, ProposeJobIntegrationRequest, ReserveClosureAttemptRequest, ReviewReceipt,
};
use threadlane_workers::{
    ensure_worktree_job_inactive_readonly, is_commit_ancestor, is_worktree_clean,
    load_and_verify_manifest, resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    WorkerRunStatus, WorktreeKind, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR, MAX_STDERR_BYTES, MAX_STDOUT_BYTES,
};

use crate::conversation_change::ConversationChangeStore;
use crate::coworker_observation::{
    begin_coworker_call_observation, CoworkerCallFinishSpec, CoworkerCallObservationSpec,
    CoworkerObservationExecutorSpec, COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, read_private_file_bounded,
    sanitize_diagnostic_text, sanitize_io_error, validate_bounded_identifier,
    validate_private_file_path,
};
use crate::role_flow::RoleFlowStore;
use threadlane_core::observation::ObservationCallState;

use super::candidate::{
    AssembleJobAggregateCandidateRequest, AssembleOperationsFinalCandidateOutcome,
    AssembleOperationsFinalCandidateRequest, CoworkerCandidateService,
    ExecuteFinalCandidateClosureOutcome, ExecuteFinalCandidateClosureRequest,
    StructuredClosureModelResponse,
};
use super::checks::{
    run_frozen_check, FrozenCheckExecutionReport, FrozenCheckExecutionStatus, FrozenCheckSubject,
    FrozenCheckTargetKind,
};
use super::contracts::{
    deterministic_prefixed_id, sha256_hex, CandidateCheckEvidence, CombinedCandidateIdentity,
    CombinedCandidateStatus, CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord,
    CoworkerTaskStage, TaskCandidateRef, MAX_COWORKER_ID_LEN,
};
use super::controller::{
    CoworkerControllerService, OwnedCoworkerControllerSession, VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, FrozenCheckCommandSpec, PendingControllerEffect,
    PendingControllerEffectKind, PendingControllerEffectStatus, TaskAttemptExecutionArtifacts,
    DEFAULT_CONTROLLER_LEASE_TTL_MS,
};
use super::ownership::CoworkerJobOwnershipGuard;

/// Maximum serialized byte size for a private (`0600`) persisted [`FrozenCheckExecutionReport`]
/// or [`JobIntegrationReport`] file (`~220 MiB`), conservatively sized to hold full
/// `MAX_STDOUT_BYTES` (`32 MiB`) and `MAX_STDERR_BYTES` (`4 MiB`) JSON byte arrays without silent
/// truncation or ledger bloat.
pub const MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES: usize =
    (MAX_STDOUT_BYTES + MAX_STDERR_BYTES) * 6 + 4 * 1024 * 1024;

static IN_FLIGHT_JOB_COMPLETIONS: LazyLock<Mutex<BTreeSet<String>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

struct InFlightJobCompletionGuard {
    key: String,
}

impl InFlightJobCompletionGuard {
    fn try_acquire(plans_dir: &Path, binding_id: &str, job_id: &str) -> Result<Self, NativeError> {
        let key = format!("{}:{binding_id}:{job_id}", plans_dir.display());
        let mut set = IN_FLIGHT_JOB_COMPLETIONS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !set.insert(key.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "job completion is already in flight in this process for binding `{binding_id}` job `{job_id}`"
                ),
            ));
        }
        Ok(Self { key })
    }
}

impl Drop for InFlightJobCompletionGuard {
    fn drop(&mut self) {
        let mut set = IN_FLIGHT_JOB_COMPLETIONS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        set.remove(&self.key);
    }
}

/// Narrow, override-free request to execute the final job completion controller stage.
///
/// Contains only scope identifiers (`binding_id`, `thread_uuid`, `job_id`) and `now_ms`.
/// No model, path, strategy, check command, or closure verdict overrides are accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobCompletionRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Monotonic wall-clock timestamp (ms since Unix epoch).
    pub now_ms: u64,
}

/// High-level outcome status of [`CoworkerJobCompletionService::complete_job`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerJobCompletionStatus {
    /// Final Git aggregation, all required frozen candidate checks, candidate assembly, and
    /// `Closure` evaluation succeeded with [`ClosureVerdictKind::Accepted`]; the job is now
    /// [`CoworkerJobPhase::Completed`].
    Completed,
    /// Final Git aggregation succeeded and persisted its [`JobIntegrationReport`] (used by the
    /// standalone aggregation entry point before combined candidate checks and `Closure`).
    AggregatedReadyForCandidateChecks,
    /// One or more required frozen combined candidate checks failed cleanly on the final
    /// `RunIntegration` worktree; full private `0600` check reports were persisted and candidate
    /// promotion / `Closure` invocation was refused.
    BlockedByFailedChecks,
    /// Combined candidate was assembled and `Closure` returned a non-accepting structured verdict
    /// (`CorrectInScope`, `ReplanRequired`, `Blocked`, or `Unclear`); the job was not marked
    /// `Completed`.
    ClosureNotAccepted,
}

/// Compact reference to a private (`0600`) persisted [`FrozenCheckExecutionReport`] produced on
/// the final owned `RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedCandidateCheckReportRef {
    /// Frozen check command identifier (`check_id`).
    pub check_id: String,
    /// Deterministic controller effect identifier (`eff_<32hex>`).
    pub effect_id: String,
    /// Validated absolute path to the private (`0600`) persisted [`FrozenCheckExecutionReport`] JSON file.
    pub report_path: PathBuf,
    /// Full verified [`FrozenCheckExecutionReport`].
    pub report: FrozenCheckExecutionReport,
}

/// Outcome of the final multi-worker Git aggregation stage on the owned `RunIntegration` worktree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobAggregationOutcome {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Bound [`threadlane_authority::ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Deterministic `JobAggregation` controller effect identifier (`eff_<32hex>`).
    pub aggregation_effect_id: String,
    /// Deterministic software run identifier (`rjob_<16hex>`) for the final `RunIntegration` worktree.
    pub target_integration_run_id: String,
    /// Deterministic worktree token (`jagg_<16hex>`) for the final `RunIntegration` worktree.
    pub target_integration_token: String,
    /// Owned `RunIntegration` worktree identifier (`wt-run-<run_id>-<token>`).
    pub integration_worktree_id: String,
    /// Canonical filesystem path of the owned `RunIntegration` checkout.
    pub integration_checkout_path: PathBuf,
    /// Software-owned Git branch name (`threadlane/run/<run_id>/<token>`).
    pub integration_branch_name: String,
    /// Exact 40-hex approved base commit SHA.
    pub approved_base_commit: String,
    /// Exact 40-hex final integrated commit SHA on the `RunIntegration` worktree.
    pub final_integrated_target_commit: String,
    /// Exact 40-hex final integrated Git tree SHA on the `RunIntegration` worktree.
    pub final_integrated_tree_sha: String,
    /// Whether an existing completed [`JobIntegrationIntent`] and receipt chain were verified and reused.
    pub reused_existing_receipts: bool,
    /// Validated absolute path to the private (`0600`) persisted [`JobIntegrationReport`] JSON artifact.
    pub report_artifact_path: PathBuf,
    /// Canonical 64-hex SHA-256 digest of the persisted [`JobIntegrationReport`].
    pub report_artifact_digest: String,
    /// Full verified [`JobIntegrationReport`].
    pub job_integration_report: JobIntegrationReport,
}

/// Complete outcome of [`CoworkerJobCompletionService::complete_job`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerJobCompletionOutcome {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Bound [`threadlane_authority::ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// High-level completion status.
    pub status: CoworkerJobCompletionStatus,
    /// Verified multi-worker Git aggregation outcome.
    pub aggregation: CoworkerJobAggregationOutcome,
    /// Frozen check subject for the final `RunIntegration` worktree.
    pub check_subject: FrozenCheckSubject,
    /// Persisted private (`0600`) check report references in deterministic execution order.
    pub check_reports: Vec<PersistedCandidateCheckReportRef>,
    /// Compact check evidence records promoted into the combined candidate when all checks pass.
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Identifier of the first failed check command when `status == BlockedByFailedChecks`.
    pub failed_check_id: Option<String>,
    /// Assembled [`CombinedCandidateIdentity`] when all required candidate checks passed.
    pub combined_candidate: Option<CombinedCandidateIdentity>,
    /// Self-contained [`FinalCandidateSubject`] when all required candidate checks passed.
    pub final_candidate_subject: Option<FinalCandidateSubject>,
    /// Full `Closure` execution outcome when candidate checks passed and `Closure` completed.
    pub closure_outcome: Option<ExecuteFinalCandidateClosureOutcome>,
    /// Final structured [`ClosureVerdict`] when `Closure` completed.
    pub closure_verdict: Option<ClosureVerdict>,
    /// Updated [`CoworkerJobRecord`] snapshot after the stage finished.
    pub job: CoworkerJobRecord,
}

impl CoworkerJobCompletionOutcome {
    /// Returns `true` if and only if the job reached [`CoworkerJobCompletionStatus::Completed`]
    /// with an accepted `Closure` verdict and [`CoworkerJobPhase::Completed`].
    pub const fn is_completed(&self) -> bool {
        matches!(self.status, CoworkerJobCompletionStatus::Completed)
            && matches!(self.job.phase, CoworkerJobPhase::Completed)
    }
}

/// Type alias for callers referring to the completion outcome as a report.
pub type CoworkerJobCompletionReport = CoworkerJobCompletionOutcome;

/// Trusted service wrapping [`CoworkerControllerService`] and [`CoworkerCandidateService`] to drive
/// final multi-worker Git aggregation, frozen combined candidate checks, candidate assembly, and
/// single candidate-bound `Closure` evaluation.
#[derive(Debug, Clone)]
pub struct CoworkerJobCompletionService {
    controller: CoworkerControllerService,
    candidate_service: CoworkerCandidateService,
}

impl CoworkerJobCompletionService {
    /// Constructs a [`CoworkerJobCompletionService`] after verifying that `controller` and
    /// `candidate_service` share the exact same `plans_dir` and `registry_dir`.
    pub fn new(
        controller: CoworkerControllerService,
        candidate_service: CoworkerCandidateService,
    ) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let controller_plans_dir = controller.workflow_store().plans_dir();
        if controller.policy_store().plans_dir() != controller_plans_dir {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerControllerService workflow_store and policy_store plans_dir do not match",
            ));
        }
        if candidate_service.store().plans_dir() != controller_plans_dir
            || candidate_service.role_flow().plans_dir() != controller_plans_dir
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerCandidateService plans_dir does not match CoworkerControllerService plans_dir",
            ));
        }
        let authority_registry_path = controller.authority().registry().registry_path();
        let authority_registry_dir = authority_registry_path.parent().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "AuthorityStore registry_path has no parent directory",
            )
        })?;
        if candidate_service.registry_dir() != authority_registry_dir {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerCandidateService registry_dir does not match AuthorityStore registry directory",
            ));
        }
        Ok(Self {
            controller,
            candidate_service,
        })
    }

    /// Constructs a [`CoworkerJobCompletionService`] directly from a [`CoworkerControllerService`],
    /// deriving the matching [`CoworkerCandidateService`] from the controller's configured stores.
    pub fn from_controller(controller: CoworkerControllerService) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let plans_dir = controller.workflow_store().plans_dir().to_path_buf();
        let role_flow = RoleFlowStore::new(&plans_dir)?;
        let authority_registry_path = controller.authority().registry().registry_path();
        let registry_dir = authority_registry_path
            .parent()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "AuthorityStore registry_path has no parent directory",
                )
            })?
            .to_path_buf();
        let candidate_service = CoworkerCandidateService::new(
            controller.workflow_store().clone(),
            role_flow,
            registry_dir,
        )?;
        Self::new(controller, candidate_service)
    }

    /// Returns a reference to the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Returns a reference to the underlying [`CoworkerCandidateService`].
    pub fn candidate_service(&self) -> &CoworkerCandidateService {
        &self.candidate_service
    }

    /// Executes only the final multi-worker Git aggregation stage into the owned `RunIntegration`
    /// worktree while holding `guard`, persisting the [`JobIntegrationReport`] artifact on disk.
    pub async fn aggregate_integrated_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerJobCompletionRequest,
    ) -> Result<CoworkerJobAggregationOutcome, NativeError> {
        validate_completion_request(request)?;
        let plans_dir = self.controller.workflow_store().plans_dir();
        let _in_flight = InFlightJobCompletionGuard::try_acquire(
            plans_dir,
            &request.binding_id,
            &request.job_id,
        )?;

        let (adm, controller_instance_id, lease_epoch, entry_change_revision) =
            verify_live_job_completion_gate(&self.controller, guard, request, None, None, false)?;

        execute_or_reuse_final_job_aggregation(
            &self.controller,
            guard,
            request,
            &adm,
            &controller_instance_id,
            lease_epoch,
            entry_change_revision,
        )
    }

    /// Executes the full final job completion stage:
    /// 1. Verifies live physical ownership, automatic controller admission, and that all frozen
    ///    job tasks are [`CoworkerTaskStage::Integrated`] with verified on-disk [`ReviewReceipt`]
    ///    and original per-worker [`IntegrationReceipt`] proofs.
    /// 2. Creates or reopens the deterministic final owned `RunIntegration` worktree at the
    ///    approved base commit after recording durable intent before effect, and runs
    ///    `propose_job_integration` + `record_job_integration_intent_before_effect` +
    ///    `execute_job_integration` (or reuses a verified `Completed` intent and receipt chain).
    /// 3. Runs all required frozen combined candidate checks (`required_for_candidate_closure`)
    ///    on the `RunIntegration` worktree with `guard` held across `.await`, persisting full
    ///    bounded private `0600` [`FrozenCheckExecutionReport`] files outside the 1 MiB ledger.
    /// 4. Assembles and inspects the combined candidate via
    ///    [`CoworkerCandidateService::assemble_and_inspect_job_aggregate_candidate`] and executes
    ///    the single candidate-bound `Closure` evaluation via
    ///    [`CoworkerCandidateService::execute_final_candidate_closure`], marking the job
    ///    [`CoworkerJobPhase::Completed`] only when `Closure` returns [`ClosureVerdictKind::Accepted`].
    pub async fn complete_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerJobCompletionRequest,
    ) -> Result<CoworkerJobCompletionOutcome, NativeError> {
        validate_completion_request(request)?;
        let plans_dir = self.controller.workflow_store().plans_dir();
        let _in_flight = InFlightJobCompletionGuard::try_acquire(
            plans_dir,
            &request.binding_id,
            &request.job_id,
        )?;

        let (adm, controller_instance_id, lease_epoch, entry_change_revision) =
            verify_live_job_completion_gate(&self.controller, guard, request, None, None, true)?;

        let aggregation = execute_or_reuse_final_job_aggregation(
            &self.controller,
            guard,
            request,
            &adm,
            &controller_instance_id,
            lease_epoch,
            entry_change_revision,
        )?;

        // Re-verify live ownership, admission, lease, and hold/stop/change fences after Git aggregation.
        let (post_agg_adm, _, _, _) = verify_live_job_completion_gate(
            &self.controller,
            guard,
            request,
            Some((&controller_instance_id, lease_epoch)),
            Some(entry_change_revision),
            true,
        )?;

        let check_subject = FrozenCheckSubject {
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            target_kind: FrozenCheckTargetKind::RunIntegrationWorktree,
            repository_path: aggregation.job_integration_report.repository_path.clone(),
            work_area_root: aggregation.job_integration_report.work_area_root.clone(),
            worktree_id: aggregation.integration_worktree_id.clone(),
            expected_run_id: aggregation.target_integration_run_id.clone(),
            expected_task_id: None,
            expected_base_commit: aggregation.approved_base_commit.clone(),
            expected_head_commit: aggregation.final_integrated_target_commit.clone(),
            expected_tree_sha: aggregation.final_integrated_tree_sha.clone(),
        };
        check_subject.validate()?;
        let check_subject_digest = check_subject.subject_digest()?;

        let required_specs: Vec<&FrozenCheckCommandSpec> = post_agg_adm
            .runtime_state
            .envelope
            .check_commands
            .values()
            .filter(|spec| spec.required_for_candidate_closure)
            .collect();
        if required_specs.is_empty() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope has no required_for_candidate_closure check commands",
            ));
        }

        let cap_env = post_agg_adm
            .linkage
            .controller_envelope
            .as_ref()
            .ok_or_else(|| {
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
                        "candidate check `{}` is not permitted by FrozenControllerCapabilityEnvelope.allowed_check_ids",
                        spec.check_id
                    ),
                ));
            }
        }

        let mut check_reports = Vec::with_capacity(required_specs.len());
        let mut raw_reports = Vec::with_capacity(required_specs.len());
        let mut failed_check_id: Option<String> = None;

        for spec in &required_specs {
            let report_ref = execute_or_reconcile_single_candidate_check(
                &self.controller,
                guard,
                request,
                &check_subject,
                &check_subject_digest,
                spec,
                &controller_instance_id,
                lease_epoch,
                entry_change_revision,
            )
            .await?;
            let passed = report_ref.report.passed();
            raw_reports.push(report_ref.report.clone());
            check_reports.push(report_ref);
            if !passed {
                failed_check_id = Some(spec.check_id.clone());
                break;
            }
        }

        // Re-verify live ownership, admission, lease, and hold/stop/change fences after all check `.await` points
        // and before promoting check evidence or assembling the combined candidate.
        let (post_chk_adm, _, _, _) = verify_live_job_completion_gate(
            &self.controller,
            guard,
            request,
            Some((&controller_instance_id, lease_epoch)),
            Some(entry_change_revision),
            false,
        )?;

        if let Some(failed_id) = failed_check_id {
            return Ok(CoworkerJobCompletionOutcome {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                approval_id: post_chk_adm.approval.approval_id,
                status: CoworkerJobCompletionStatus::BlockedByFailedChecks,
                aggregation,
                check_subject,
                check_reports,
                check_evidence: Vec::new(),
                failed_check_id: Some(failed_id),
                combined_candidate: None,
                final_candidate_subject: None,
                closure_outcome: None,
                closure_verdict: None,
                job: post_chk_adm.job,
            });
        }

        let check_evidence = verify_and_promote_candidate_check_reports(
            &check_subject,
            &post_chk_adm.runtime_state.envelope.check_commands,
            &raw_reports,
        )?;

        let candidate_request_key = deterministic_prefixed_id(
            "threadlane-native:coworker-job-candidate-request:v1",
            "jcr",
            &[
                &request.job_id,
                &post_chk_adm.approval.approval_id,
                &aggregation.integration_worktree_id,
                &aggregation.final_integrated_target_commit,
                &aggregation.final_integrated_tree_sha,
            ],
        );
        let closure_task_id = format!("closure_{}", post_chk_adm.job.job_id);
        let now_ms_i64 = request.now_ms.min(i64::MAX as u64) as i64;

        let assemble_req = AssembleJobAggregateCandidateRequest {
            candidate: AssembleOperationsFinalCandidateRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: post_chk_adm.job.job_revision,
                expected_ledger_revision: None,
                request_key: candidate_request_key.clone(),
                work_area_root: aggregation.job_integration_report.work_area_root.clone(),
                run_id: aggregation.target_integration_run_id.clone(),
                integration_worktree_id: aggregation.integration_worktree_id.clone(),
                check_evidence: check_evidence.clone(),
                closure_task_id,
                reserve_closure_attempt: false,
                reserved_at_ms: now_ms_i64,
            },
            job_integration_report: aggregation.job_integration_report.clone(),
        };

        let AssembleOperationsFinalCandidateOutcome {
            job: assembled_job,
            combined_candidate,
            final_subject: final_candidate_subject,
            ..
        } = self
            .candidate_service
            .assemble_and_inspect_job_aggregate_candidate(
                self.controller.authority(),
                self.controller.policy_store(),
                &assemble_req,
            )?;

        // Re-verify live ownership and hold/stop/change fences right before invoking `Closure`.
        let (pre_closure_adm, _, _, _) = verify_live_job_completion_gate(
            &self.controller,
            guard,
            request,
            Some((&controller_instance_id, lease_epoch)),
            Some(entry_change_revision),
            false,
        )?;

        let closure_command_key = deterministic_prefixed_id(
            "threadlane-native:coworker-job-closure-key:v1",
            "clk",
            &[
                &request.job_id,
                &combined_candidate.combined_candidate_id,
                &final_candidate_subject.subject_digest,
            ],
        );

        for eff in pre_closure_adm.runtime_state.pending_effects.values() {
            if eff.kind == PendingControllerEffectKind::ClosureInvocation
                && eff.status == PendingControllerEffectStatus::ReservedBeforeEffect
            {
                let eff_id_to_mark = eff.effect_id.clone();
                let _ = self.controller.policy_store().mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    &controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(e) = state.pending_effects.get_mut(&eff_id_to_mark) {
                            e.status = PendingControllerEffectStatus::EffectUnknown;
                            e.diagnostic = Some(
                                "unresolved ClosureInvocation ReservedBeforeEffect found; refusing blind replay"
                                    .to_string(),
                            );
                            e.updated_at_ms = request.now_ms;
                        }
                        Ok(())
                    },
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "job `{}` has an unresolved ClosureInvocation effect `{eff_id_to_mark}` in ReservedBeforeEffect state",
                        request.job_id
                    ),
                ));
            }
        }

        let closure_outcome = if assembled_job.phase == CoworkerJobPhase::Completed
            && combined_candidate.status == CombinedCandidateStatus::ClosureAccepted
        {
            let closure_store = FinalCandidateClosureStore::open(
                &final_candidate_subject.repository_path,
                &final_candidate_subject.work_area_root,
            )
            .map_err(map_operations_error)?;
            let current_closure_ledger = closure_store
                .inspect_ledger_readonly()
                .map_err(map_operations_error)?;
            let reservation = closure_store
                .reserve_closure_attempt(&ReserveClosureAttemptRequest {
                    expected_ledger_revision: current_closure_ledger.ledger_revision,
                    command_key: closure_command_key.clone(),
                    attempt_id: None,
                    subject: final_candidate_subject.clone(),
                    legacy_base_tree_digest: Some(
                        final_candidate_subject.approved_base_tree_sha.clone(),
                    ),
                    reserved_at_ms: now_ms_i64,
                })
                .map_err(map_operations_error)?;
            let ClosureReservationOutcome::CacheHit {
                ledger_revision,
                attempt,
                verdict,
            } = reservation
            else {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "completed job is missing its verified Accepted ClosureVerdict in FinalCandidateClosureStore",
                ));
            };
            ExecuteFinalCandidateClosureOutcome {
                job: assembled_job,
                closure_ledger_revision: ledger_revision,
                attempt,
                verdict: Some(*verdict),
                reused_cached_verdict: true,
            }
        } else {
            let closure_req = ExecuteFinalCandidateClosureRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: assembled_job.job_revision,
                expected_ledger_revision: None,
                command_key: closure_command_key.clone(),
                combined_candidate_id: combined_candidate.combined_candidate_id.clone(),
                final_subject: final_candidate_subject.clone(),
                current_time_ms: now_ms_i64,
            };

            let closure_res = self
                .candidate_service
                .execute_final_candidate_closure(
                    self.controller.authority(),
                    self.controller.policy_store(),
                    &closure_req,
                )
                .await;

            // Immediately re-verify physical ownership after `.await`.
            self.controller.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;

            closure_res?
        };

        if closure_outcome
            .verdict
            .as_ref()
            .is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
        {
            let latest_runtime = self
                .controller
                .policy_store()
                .read_job_runtime(&request.binding_id, &request.job_id)?
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "ControllerJobRuntimeState missing after Closure evaluation",
                    )
                })?;
            let closure_budget = latest_runtime.envelope.closure_budget()?;
            let closure_art = latest_runtime
                .closure_artifacts
                .get(&closure_outcome.attempt.attempt_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        format!(
                            "persisted ClosureExecutionArtifactRecord missing for accepted attempt `{}`",
                            closure_outcome.attempt.attempt_id
                        ),
                    )
                })?;
            if closure_art.fenced_from_promotion
                || closure_art.command_key != closure_command_key
                || !constant_time_str_eq(
                    &closure_art.subject_digest,
                    &final_candidate_subject.subject_digest,
                )
                || !constant_time_str_eq(
                    &closure_art.candidate_tree_sha,
                    &final_candidate_subject.candidate_tree_sha,
                )
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "persisted ClosureExecutionArtifactRecord metadata does not match accepted FinalCandidateSubject",
                ));
            }
            let loaded_closure_art = self
                .controller
                .policy_store()
                .load_closure_execution_artifact(
                    &request.binding_id,
                    &request.thread_uuid,
                    &request.job_id,
                    &closure_outcome.attempt.attempt_id,
                )?;
            if loaded_closure_art.fenced_from_promotion
                || loaded_closure_art.command_key != closure_command_key
                || !constant_time_str_eq(
                    &loaded_closure_art.subject_digest,
                    &final_candidate_subject.subject_digest,
                )
                || !constant_time_str_eq(
                    &loaded_closure_art.candidate_tree_sha,
                    &final_candidate_subject.candidate_tree_sha,
                )
                || loaded_closure_art.execution_report.status != WorkerRunStatus::Succeeded
                || loaded_closure_art.execution_report.exit_code != Some(0)
                || !(loaded_closure_art
                    .execution_report
                    .has_complete_natural_completion_evidence()
                    || loaded_closure_art
                        .execution_report
                        .has_complete_legacy_codex_advisory_evidence())
                || !loaded_closure_art
                    .execution_report
                    .has_satisfied_model_evidence()
                || !loaded_closure_art
                    .execution_report
                    .has_satisfied_effort_evidence()
                || loaded_closure_art.execution_report.cleanup_incomplete
                || loaded_closure_art.response_error.is_some()
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "loaded Closure execution artifact does not match accepted FinalCandidateSubject or natural Succeeded state",
                ));
            }
            let adv_resp = loaded_closure_art
                .advisory_response
                .as_ref()
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "loaded Closure execution artifact is missing its BoundedAdvisoryResponse",
                    )
                })?;
            adv_resp
                .validate_integrity(closure_budget.max_structured_response_bytes)
                .map_err(|e| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!("persisted Closure BoundedAdvisoryResponse integrity failed: {e}"),
                    )
                })?;
            let parsed = StructuredClosureModelResponse::parse_and_verify(
                &adv_resp.text,
                &final_candidate_subject,
            )?;
            if parsed.outcome != ClosureVerdictKind::Accepted {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "persisted StructuredClosureModelResponse outcome is not Accepted",
                ));
            }
        }

        // Verify post-closure target worktree preservation (`RunIntegration` checkout remains clean at the
        // integrated head/tree and primary source repository remains untouched at `approved_base_commit`).
        verify_preserved_run_integration_and_source(
            &aggregation.job_integration_report.repository_path,
            &aggregation.approved_base_commit,
            &post_chk_adm.linkage.project_base.base_tree_sha,
            &aggregation.job_integration_report.work_area_root,
            &aggregation.integration_worktree_id,
            &aggregation.target_integration_run_id,
            &aggregation.integration_branch_name,
            &aggregation.final_integrated_target_commit,
            &aggregation.final_integrated_tree_sha,
        )?;

        let final_verdict = closure_outcome.verdict.clone();
        let final_candidate = closure_outcome
            .job
            .combined_candidates
            .iter()
            .find(|c| c.combined_candidate_id == combined_candidate.combined_candidate_id)
            .cloned()
            .unwrap_or(combined_candidate);
        let status = if final_verdict
            .as_ref()
            .is_some_and(|v| v.kind == ClosureVerdictKind::Accepted)
            && closure_outcome.job.phase == CoworkerJobPhase::Completed
        {
            CoworkerJobCompletionStatus::Completed
        } else {
            CoworkerJobCompletionStatus::ClosureNotAccepted
        };

        Ok(CoworkerJobCompletionOutcome {
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            approval_id: post_chk_adm.approval.approval_id,
            status,
            aggregation,
            check_subject,
            check_reports,
            check_evidence,
            failed_check_id: None,
            combined_candidate: Some(final_candidate),
            final_candidate_subject: Some(final_candidate_subject),
            closure_outcome: Some(closure_outcome.clone()),
            closure_verdict: final_verdict,
            job: closure_outcome.job,
        })
    }

    /// Alias for [`Self::complete_job`].
    pub async fn execute_job_completion(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerJobCompletionRequest,
    ) -> Result<CoworkerJobCompletionOutcome, NativeError> {
        self.complete_job(guard, request).await
    }

    /// Alias for [`Self::complete_job`].
    pub async fn complete_integrated_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerJobCompletionRequest,
    ) -> Result<CoworkerJobCompletionOutcome, NativeError> {
        self.complete_job(guard, request).await
    }

    /// Executes [`Self::complete_job`] using the held guard from an [`OwnedCoworkerControllerSession`].
    pub async fn complete_owned_session(
        &self,
        session: &OwnedCoworkerControllerSession,
        now_ms: u64,
    ) -> Result<CoworkerJobCompletionOutcome, NativeError> {
        let key = session.guard().key();
        self.complete_job(
            session.guard(),
            &CoworkerJobCompletionRequest {
                binding_id: key.binding_id.clone(),
                thread_uuid: key.thread_uuid.clone(),
                job_id: key.job_id.clone(),
                now_ms,
            },
        )
        .await
    }
}

/// Derives the deterministic software `run_id` (`rjob_<16hex>`), worktree `token` (`jagg_<16hex>`),
/// `integration_worktree_id` (`wt-run-<run_id>-<token>`), and `integration_branch_name`
/// (`threadlane/run/<run_id>/<token>`) for a job's final owned `RunIntegration` worktree.
pub fn derive_final_job_integration_identifiers(
    job_id: &str,
    approval_id: &str,
    approval_subject_digest: &str,
) -> (String, String, String, String) {
    let run_seed = format!(
        "threadlane-native:job-completion-run:v1:{job_id}:{approval_id}:{approval_subject_digest}"
    );
    let token_seed = format!(
        "threadlane-native:job-completion-token:v1:{job_id}:{approval_id}:{approval_subject_digest}"
    );
    let run_id = format!("rjob_{}", &sha256_hex(run_seed.as_bytes())[..16]);
    let token = format!("jagg_{}", &sha256_hex(token_seed.as_bytes())[..16]);
    let worktree_id = format!("wt-run-{run_id}-{token}");
    let branch_name = format!("threadlane/run/{run_id}/{token}");
    (run_id, token, worktree_id, branch_name)
}

/// Derives the deterministic job-aggregation [`IntegrationReceipt::receipt_id`] for a task step,
/// guaranteed to be distinct from the task's original per-worker `integration_receipt_id`.
pub fn derive_job_aggregation_receipt_id(
    job_id: &str,
    target_integration_run_id: &str,
    integration_worktree_id: &str,
    logical_task_id: &str,
    review_receipt_id: &str,
) -> String {
    let seed = format!(
        "threadlane-native:job-aggregation-receipt:v1:{job_id}:{target_integration_run_id}:{integration_worktree_id}:{logical_task_id}:{review_receipt_id}"
    );
    format!("int_jagg_{}", &sha256_hex(seed.as_bytes())[..24])
}

fn validate_completion_request(request: &CoworkerJobCompletionRequest) -> Result<(), NativeError> {
    ensure_platform_supported()?;
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
        )
    })?;
    validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
    Ok(())
}

#[allow(clippy::type_complexity)]
fn verify_live_job_completion_gate(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerJobCompletionRequest,
    expected_lease: Option<(&str, u64)>,
    expected_change_revision: Option<u64>,
    allow_reconcilable_candidate_check_unknown: bool,
) -> Result<(VerifiedControllerAdmission, String, u64, u64), NativeError> {
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
                "controller driver lease changed during job completion",
            ));
        }
    }

    if !adm.linkage.authorizes_local_git_integration()
        || !adm.runtime_state.envelope.allow_run_integration_execution
    {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "job FrozenApprovalLinkage or FrozenControllerExecutionEnvelope does not authorize local RunIntegration execution",
        ));
    }

    if adm.runtime_state.stop_requested {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "controller job has stop_requested == true; job completion is fenced",
        ));
    }

    if adm.job.holds.iter().any(|h| h.active) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job has an active workflow hold; job completion is fenced",
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
                format!("failed to read ConversationChangeStore during job completion: {err}"),
            )
        })?;
    if change_summary.approval_superseded || !change_summary.blocking_changes.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job has unresolved blocking conversation changes or superseded approval; job completion is fenced",
        ));
    }
    if let Some(exp_rev) = expected_change_revision {
        if change_summary.current_change_revision != exp_rev {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "ConversationChangeStore revision drifted from {exp_rev} to {} during job completion",
                    change_summary.current_change_revision
                ),
            ));
        }
    }
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
        adm = controller.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;
    }

    if !matches!(
        adm.job.phase,
        CoworkerJobPhase::Executing
            | CoworkerJobPhase::AssemblingCandidate
            | CoworkerJobPhase::ClosureReview
            | CoworkerJobPhase::Completed
    ) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "job `{}` is in phase `{:?}`; expected `Executing`, `AssemblingCandidate`, `ClosureReview`, or `Completed`",
                adm.job.job_id, adm.job.phase
            ),
        ));
    }

    // Refuse if any effect on the job is in EffectUnknown state (unless it is a candidate
    // CheckExecution effect with a valid non-uncertain persisted report on disk that can be
    // verified, made durable, and reconciled in `execute_or_reconcile_single_candidate_check`).
    for eff in adm.runtime_state.pending_effects.values() {
        if eff.status == PendingControllerEffectStatus::EffectUnknown {
            if allow_reconcilable_candidate_check_unknown
                && is_reconcilable_candidate_check_unknown(plans_dir, &adm, eff)
            {
                continue;
            }
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "job `{}` has unresolved EffectUnknown effect `{}` (`{}`)",
                    adm.job.job_id,
                    eff.effect_id,
                    eff.kind.as_str()
                ),
            ));
        }
        if eff.task_id.is_some()
            && eff.status == PendingControllerEffectStatus::ReservedBeforeEffect
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "job `{}` has unresolved task effect `{}` (`{}`) in ReservedBeforeEffect state",
                    adm.job.job_id,
                    eff.effect_id,
                    eff.kind.as_str()
                ),
            ));
        }
    }

    // Verify primary source repository checkout is clean and at the exact approved base commit and tree.
    verify_primary_source_repository_clean_at_base(&adm)?;

    Ok((
        adm,
        controller_instance_id,
        lease_epoch,
        change_summary.current_change_revision,
    ))
}

fn is_reconcilable_candidate_check_unknown(
    plans_dir: &Path,
    adm: &VerifiedControllerAdmission,
    eff: &PendingControllerEffect,
) -> bool {
    if eff.kind != PendingControllerEffectKind::CheckExecution
        || eff.task_id.is_some()
        || eff.attempt_no.is_some()
    {
        return false;
    }
    let Some(check_id) = eff.external_correlation_id.as_deref() else {
        return false;
    };
    let Some(spec) = adm.runtime_state.envelope.check_commands.get(check_id) else {
        return false;
    };
    if !spec.required_for_candidate_closure {
        return false;
    }
    let Ok(Some(report)) = read_persisted_candidate_check_report(plans_dir, &eff.effect_id) else {
        return false;
    };
    report.check_id == spec.check_id && !is_uncertain_check_report(&report)
}

fn verify_primary_source_repository_clean_at_base(
    adm: &VerifiedControllerAdmission,
) -> Result<PathBuf, NativeError> {
    let repo_path = PathBuf::from(&adm.linkage.project_base.repository_path);
    let canonical_repo = fs::canonicalize(&repo_path).map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to canonicalize repository_path `{}`: {}",
                repo_path.display(),
                sanitize_io_error(&err)
            ),
        )
    })?;
    if canonical_repo != repo_path || canonical_repo != adm.approval.repository_path {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "repository_path does not match canonical path or ApprovalRecord.repository_path",
        ));
    }
    let clean =
        is_worktree_clean(&canonical_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify primary source repository cleanliness: {e}"),
            )
        })?;
    if !clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "primary source repository checkout is dirty; refusing job completion",
        ));
    }
    let live_head =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |e| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to resolve primary source repository HEAD: {e}"),
                )
            },
        )?;
    let live_tree = resolve_exact_tree_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to resolve primary source repository HEAD tree: {e}"),
            )
        })?;
    if !constant_time_str_eq(&live_head, &adm.linkage.project_base.base_commit)
        || !constant_time_str_eq(&live_head, &adm.approval.base_commit)
        || !constant_time_str_eq(&live_tree, &adm.linkage.project_base.base_tree_sha)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "primary source repository HEAD (`{live_head}`, tree `{live_tree}`) drifted from frozen base (`{}`, tree `{}`)",
                adm.linkage.project_base.base_commit, adm.linkage.project_base.base_tree_sha
            ),
        ));
    }
    Ok(canonical_repo)
}

struct VerifiedIntegratedJobTask {
    logical_task_id: String,
    task_digest: String,
    original_run_id: String,
    task_worktree_id: String,
    expected_task_head_commit: String,
    review_subject: threadlane_operations::ReviewSubject,
    review_receipt: ReviewReceipt,
    original_integration_receipt: IntegrationReceipt,
    dependency_input: Option<JobTaskDependencyExecutionProof>,
}

fn verify_all_tasks_integrated_and_build_inputs(
    adm: &VerifiedControllerAdmission,
    work_area_root: &Path,
    canonical_repo: &Path,
) -> Result<(Vec<VerifiedIntegratedJobTask>, BTreeSet<String>), NativeError> {
    if adm.job.tasks.is_empty() || adm.job.tasks.len() != adm.approval.tasks.len() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.tasks must be non-empty and match ApprovalRecord.tasks length",
        ));
    }

    let ordered_task_ids = compute_deterministic_task_integration_order(&adm.job.tasks)?;
    if ordered_task_ids.len() != adm.job.tasks.len() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "topological task integration order length does not match job.tasks length",
        ));
    }

    let mut aggregate_allowed_write_paths = BTreeSet::new();
    for approved_task in &adm.approval.tasks {
        for p in &approved_task.allowed_write_paths {
            aggregate_allowed_write_paths.insert(p.clone());
        }
    }

    let mut verified_tasks = Vec::with_capacity(ordered_task_ids.len());

    for logical_task_id in &ordered_task_ids {
        let job_task = adm
            .job
            .tasks
            .iter()
            .find(|t| &t.task_id == logical_task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{logical_task_id}` not found in job.tasks"),
                )
            })?;
        let approved_task = adm
            .approval
            .tasks
            .iter()
            .find(|t| &t.task_id == logical_task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{logical_task_id}` not found in ApprovalRecord.tasks"),
                )
            })?;

        verify_single_integrated_task(
            adm,
            job_task,
            approved_task,
            work_area_root,
            canonical_repo,
            &mut verified_tasks,
        )?;
    }

    Ok((verified_tasks, aggregate_allowed_write_paths))
}

fn verify_single_integrated_task(
    adm: &VerifiedControllerAdmission,
    job_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    work_area_root: &Path,
    canonical_repo: &Path,
    out: &mut Vec<VerifiedIntegratedJobTask>,
) -> Result<(), NativeError> {
    if job_task.stage != CoworkerTaskStage::Integrated {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is in stage `{}`; all tasks must be in `integrated` stage before job completion",
                job_task.task_id, job_task.stage
            ),
        ));
    }
    if job_task.unresolved_outbox_id.is_some() || !job_task.active_hold_ids.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` has an unresolved outbox or active hold",
                job_task.task_id
            ),
        ));
    }
    if !constant_time_str_eq(&job_task.task_digest, &approved_task.task_digest)
        || job_task.write_paths != approved_task.allowed_write_paths
        || job_task.depends_on != approved_task.depends_on
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` digest, write_paths, or depends_on does not match ApprovalRecord",
                job_task.task_id
            ),
        ));
    }

    let acc_cand_id = job_task.accepted_candidate_id.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "integrated task `{}` is missing accepted_candidate_id",
                job_task.task_id
            ),
        )
    })?;
    if job_task.active_candidate_id.as_ref() != Some(acc_cand_id) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "integrated task `{}` active_candidate_id does not match accepted_candidate_id",
                job_task.task_id
            ),
        ));
    }
    let accepted_candidate: &TaskCandidateRef = job_task
        .candidates
        .iter()
        .find(|c| &c.candidate_id == acc_cand_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "integrated task `{}` accepted_candidate_id `{acc_cand_id}` not found in task.candidates",
                    job_task.task_id
                ),
            )
        })?;
    if accepted_candidate.attempt_no == 0
        || accepted_candidate.attempt_no != job_task.current_attempt_no
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "integrated task `{}` accepted candidate attempt_no `{}` does not match current_attempt_no `{}`",
                job_task.task_id, accepted_candidate.attempt_no, job_task.current_attempt_no
            ),
        ));
    }
    let verdict = accepted_candidate.review_verdict.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "integrated task `{}` accepted candidate `{}` has no StructuredReviewVerdict",
                job_task.task_id, accepted_candidate.candidate_id
            ),
        )
    })?;
    if !verdict.allows_acceptance()
        || !constant_time_str_eq(&verdict.policy_digest, &adm.linkage.policy_digest)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "integrated task `{}` accepted candidate verdict does not allow acceptance or has mismatched policy_digest",
                job_task.task_id
            ),
        ));
    }

    let review_receipt_id = accepted_candidate
        .review_receipt_id
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "integrated task `{}` accepted candidate is missing review_receipt_id",
                    job_task.task_id
                ),
            )
        })?;
    let orig_integration_receipt_id = accepted_candidate
        .integration_receipt_id
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "integrated task `{}` accepted candidate is missing integration_receipt_id",
                    job_task.task_id
                ),
            )
        })?;
    let cand_subject_digest = accepted_candidate
        .review_subject_digest
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "integrated task `{}` accepted candidate is missing review_subject_digest",
                    job_task.task_id
                ),
            )
        })?;

    let artifacts: &TaskAttemptExecutionArtifacts = adm
        .runtime_state
        .get_attempt_artifacts(&job_task.task_id, accepted_candidate.attempt_no)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "ControllerJobRuntimeState is missing TaskAttemptExecutionArtifacts for task `{}` attempt `{}`",
                    job_task.task_id, accepted_candidate.attempt_no
                ),
            )
        })?;
    let runtime_subject = artifacts.review_subject.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "TaskAttemptExecutionArtifacts for task `{}` attempt `{}` is missing review_subject",
                job_task.task_id, accepted_candidate.attempt_no
            ),
        )
    })?;
    let runtime_review_receipt = artifacts.review_receipt.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "TaskAttemptExecutionArtifacts for task `{}` attempt `{}` is missing review_receipt",
                job_task.task_id, accepted_candidate.attempt_no
            ),
        )
    })?;
    let runtime_orig_int_receipt = artifacts.integration_receipt.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "TaskAttemptExecutionArtifacts for task `{}` attempt `{}` is missing original integration_receipt",
                job_task.task_id, accepted_candidate.attempt_no
            ),
        )
    })?;

    let disk_review_receipt =
        load_review_receipt(work_area_root, review_receipt_id).map_err(map_operations_error)?;
    if &disk_review_receipt != runtime_review_receipt {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "on-disk ReviewReceipt `{review_receipt_id}` does not match runtime_state.attempt_artifacts for task `{}`",
                job_task.task_id
            ),
        ));
    }
    verify_review_receipt_readonly(runtime_subject, &disk_review_receipt)
        .map_err(map_operations_error)?;

    let disk_orig_int_receipt =
        load_integration_receipt(work_area_root, orig_integration_receipt_id)
            .map_err(map_operations_error)?;
    if &disk_orig_int_receipt != runtime_orig_int_receipt {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "on-disk IntegrationReceipt `{orig_integration_receipt_id}` does not match runtime_state.attempt_artifacts for task `{}`",
                job_task.task_id
            ),
        ));
    }
    if disk_orig_int_receipt.job_aggregation.is_some() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` original IntegrationReceipt `{orig_integration_receipt_id}` must not have job_aggregation provenance",
                job_task.task_id
            ),
        ));
    }
    disk_orig_int_receipt
        .verify_against_review(runtime_subject, &disk_review_receipt)
        .map_err(map_operations_error)?;

    let task_manifest =
        load_and_verify_manifest(work_area_root, &disk_review_receipt.task_worktree_id).map_err(
            |e| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "failed to load and verify task worktree manifest `{}` for task `{}`: {e}",
                        disk_review_receipt.task_worktree_id, job_task.task_id
                    ),
                )
            },
        )?;
    if task_manifest.kind != WorktreeKind::Task
        || task_manifest.repository_path != canonical_repo
        || task_manifest.work_area_root != work_area_root
        || task_manifest.worktree_id != artifacts.task_worktree_id
        || task_manifest.run_id != disk_review_receipt.run_id
        || task_manifest.run_id != runtime_subject.run_id
        || task_manifest.run_id != disk_orig_int_receipt.run_id
        || task_manifest.task_id.as_deref() != Some(disk_review_receipt.task_id.as_str())
        || task_manifest.task_id.as_deref() != Some(runtime_subject.task_id.as_str())
        || task_manifest.task_id.as_deref() != Some(disk_orig_int_receipt.task_id.as_str())
        || !constant_time_str_eq(&runtime_subject.subject_digest, cand_subject_digest)
        || !constant_time_str_eq(
            &disk_review_receipt.task_head_commit,
            &accepted_candidate.head_commit,
        )
        || !constant_time_str_eq(
            &disk_orig_int_receipt.accepted_task_head_commit,
            &accepted_candidate.head_commit,
        )
        || !constant_time_str_eq(
            &disk_review_receipt.task_tree_sha,
            &accepted_candidate.tree_sha,
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` manifest, ReviewSubject, ReviewReceipt, or original IntegrationReceipt fields are inconsistent",
                job_task.task_id
            ),
        ));
    }

    let dependency_input = if job_task.depends_on.is_empty() {
        if !constant_time_str_eq(&task_manifest.base_commit, &adm.approval.base_commit) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "root task `{}` manifest base_commit `{}` does not match approved base_commit `{}`",
                    job_task.task_id, task_manifest.base_commit, adm.approval.base_commit
                ),
            ));
        }
        None
    } else {
        let profile_cfg = build_approved_worker_profile_config(
            &adm.approval,
            approved_task,
            &adm.job.binding_id,
            work_area_root,
            work_area_root,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to synthesize approved worker profile config for dependent task `{}`: {err}",
                    job_task.task_id
                ),
            )
        })?;
        let policy = profile_cfg.dependency_input.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "dependent task `{}` synthesized profile config is missing DependencyInputPolicy",
                    job_task.task_id
                ),
            )
        })?;
        Some(JobTaskDependencyExecutionProof {
            policy,
            instruction: approved_task.instruction.clone(),
        })
    };

    out.push(VerifiedIntegratedJobTask {
        logical_task_id: job_task.task_id.clone(),
        task_digest: job_task.task_digest.clone(),
        original_run_id: task_manifest.run_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        expected_task_head_commit: accepted_candidate.head_commit.clone(),
        review_subject: runtime_subject.clone(),
        review_receipt: disk_review_receipt,
        original_integration_receipt: disk_orig_int_receipt,
        dependency_input,
    });

    Ok(())
}

fn verify_strategy_compatible_before_effect(
    canonical_repo: &Path,
    approved_base_commit: &str,
    strategy: IntegrationStrategy,
    verified_tasks: &[VerifiedIntegratedJobTask],
) -> Result<(), NativeError> {
    if strategy == IntegrationStrategy::FastForwardOnly {
        let mut cur_head = approved_base_commit.to_string();
        for (idx, vt) in verified_tasks.iter().enumerate() {
            let can_ff = is_commit_ancestor(
                canonical_repo,
                &cur_head,
                &vt.expected_task_head_commit,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .map_err(|e| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "failed to verify FastForwardOnly ancestry for task `{}` at step {idx}: {e}",
                        vt.logical_task_id
                    ),
                )
            })?;
            if !can_ff {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "frozen IntegrationStrategy::FastForwardOnly is incompatible with task `{}` commit `{}` from current head `{cur_head}` at step {idx}; refusing silent fallback to MergeNoFf",
                        vt.logical_task_id, vt.expected_task_head_commit
                    ),
                ));
            }
            cur_head = vt.expected_task_head_commit.clone();
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_or_reuse_final_job_aggregation(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerJobCompletionRequest,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    entry_change_revision: u64,
) -> Result<CoworkerJobAggregationOutcome, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    let canonical_repo = verify_primary_source_repository_clean_at_base(adm)?;
    let raw_work_area = controller
        .sessions_dir()
        .join(&adm.approval.approval_id)
        .join(WORK_AREA_SUBDIR);
    let canonical_work_area = fs::canonicalize(&raw_work_area).map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to canonicalize shared work_area_root `{}`: {}",
                raw_work_area.display(),
                sanitize_io_error(&err)
            ),
        )
    })?;
    if canonical_work_area != raw_work_area {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "shared work_area_root does not match its canonical path",
        ));
    }

    let (verified_tasks, aggregate_allowed_write_paths) =
        verify_all_tasks_integrated_and_build_inputs(adm, &canonical_work_area, &canonical_repo)?;

    let frozen_strategy = adm.runtime_state.envelope.default_integration_strategy;
    verify_strategy_compatible_before_effect(
        &canonical_repo,
        &adm.approval.base_commit,
        frozen_strategy,
        &verified_tasks,
    )?;

    let (
        target_integration_run_id,
        target_integration_token,
        integration_worktree_id,
        expected_branch_name,
    ) = derive_final_job_integration_identifiers(
        &adm.job.job_id,
        &adm.approval.approval_id,
        &adm.approval.subject_digest,
    );

    let task_commit_pairs: Vec<(&str, &str, &str)> = verified_tasks
        .iter()
        .map(|vt| {
            (
                vt.logical_task_id.as_str(),
                vt.expected_task_head_commit.as_str(),
                vt.original_integration_receipt.receipt_id.as_str(),
            )
        })
        .collect();
    let frozen_strategy_label = match frozen_strategy {
        IntegrationStrategy::FastForwardOnly => "fast_forward_only",
        IntegrationStrategy::MergeNoFf => "merge_no_ff",
    };
    let agg_subject_bytes = serde_json::to_vec(&(
        "threadlane-native:job-aggregation-effect-subject:v1",
        adm.job.job_id.as_str(),
        adm.approval.approval_id.as_str(),
        adm.approval.subject_digest.as_str(),
        adm.approval.base_commit.as_str(),
        target_integration_run_id.as_str(),
        integration_worktree_id.as_str(),
        frozen_strategy_label,
        &aggregate_allowed_write_paths,
        &task_commit_pairs,
    ))
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize JobAggregation subject seed",
        )
    })?;
    let agg_subject_digest = sha256_hex(&agg_subject_bytes);
    let agg_request_key = deterministic_prefixed_id(
        "threadlane-native:coworker-job-aggregation-key:v1",
        "jak",
        &[
            &adm.job.job_id,
            &adm.approval.approval_id,
            &adm.approval.subject_digest,
            &integration_worktree_id,
            &agg_subject_digest,
        ],
    );
    let agg_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&adm.job.job_id, &agg_request_key],
    );

    // Reject any conflicting JobAggregation effect on the job.
    for eff in adm.runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::JobAggregation
            && (eff.effect_id != agg_effect_id
                || eff.request_key != agg_request_key
                || !constant_time_str_eq(&eff.subject_digest, &agg_subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "job `{}` already has a conflicting JobAggregation effect `{}`",
                    adm.job.job_id, eff.effect_id
                ),
            ));
        }
    }

    let existing_agg_effect = adm
        .runtime_state
        .pending_effects
        .get(&agg_effect_id)
        .cloned();
    if let Some(ref eff) = existing_agg_effect {
        match eff.status {
            PendingControllerEffectStatus::EffectUnknown => {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "JobAggregation effect `{agg_effect_id}` is in EffectUnknown state; operator inspection required"
                    ),
                ));
            }
            PendingControllerEffectStatus::RefusedBeforeEffect => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "JobAggregation effect `{agg_effect_id}` was refused before effect: {}",
                        eff.diagnostic.as_deref().unwrap_or("unspecified refusal")
                    ),
                ));
            }
            _ => {}
        }
    }

    // Inspect any pre-existing JobIntegrationIntent on disk before touching or creating worktrees.
    let pre_existing_intent =
        inspect_job_integration_intent_readonly(&canonical_work_area, &integration_worktree_id)
            .map_err(|err| {
                let message = format!(
                    "failed to inspect JobIntegrationIntent for `{integration_worktree_id}`: {err}"
                );
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    &format!("corrupt or unreadable JobIntegrationIntent: {err}"),
                );
                NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    message,
                )
            })?;

    if let Some(ref intent) = pre_existing_intent {
        if let Err(err) = intent.verify_integrity() {
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                &agg_effect_id,
                &agg_request_key,
                &agg_subject_digest,
                &integration_worktree_id,
                request.now_ms,
                &format!("JobIntegrationIntent integrity failed: {err}"),
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!("JobIntegrationIntent integrity verification failed: {err}"),
            ));
        }
        if intent.state == JobIntegrationIntentState::OutcomeUnknown {
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                &agg_effect_id,
                &agg_request_key,
                &agg_subject_digest,
                &integration_worktree_id,
                request.now_ms,
                "JobIntegrationIntent is in OutcomeUnknown state",
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "JobIntegrationIntent `{}` on `{integration_worktree_id}` is in OutcomeUnknown state",
                    intent.intent_id
                ),
            ));
        }
    }

    let existing_agg_effect_confirmed = existing_agg_effect
        .as_ref()
        .is_some_and(|e| e.status == PendingControllerEffectStatus::EffectConfirmed);

    let existing_persisted_report = match existing_agg_effect.as_ref().map(|e| e.status) {
        Some(PendingControllerEffectStatus::EffectConfirmed) => {
            if !pre_existing_intent
                .as_ref()
                .is_some_and(|i| i.state == JobIntegrationIntentState::Completed)
            {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    "confirmed JobAggregation effect is missing its Completed JobIntegrationIntent on disk",
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "confirmed JobAggregation effect is missing its Completed JobIntegrationIntent on disk",
                ));
            }
            let persisted = read_persisted_job_integration_report(plans_dir, &agg_effect_id)
                .map_err(|err| {
                    mark_effect_unknown_best_effort(
                        controller,
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        &agg_effect_id,
                        &agg_request_key,
                        &agg_subject_digest,
                        &integration_worktree_id,
                        request.now_ms,
                        &format!("corrupt or unreadable persisted JobIntegrationReport: {err}"),
                    );
                    NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        format!(
                            "failed to read persisted JobIntegrationReport for `{agg_effect_id}`: {err}"
                        ),
                    )
                })?;
            let Some(rep) = persisted else {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    "persisted JobIntegrationReport missing for confirmed JobAggregation effect",
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "persisted JobIntegrationReport missing for confirmed JobAggregation effect `{agg_effect_id}`"
                    ),
                ));
            };
            Some(rep)
        }
        Some(PendingControllerEffectStatus::ReservedBeforeEffect) => {
            let persisted = read_persisted_job_integration_report(plans_dir, &agg_effect_id)
                .map_err(|err| {
                    mark_effect_unknown_best_effort(
                        controller,
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        &agg_effect_id,
                        &agg_request_key,
                        &agg_subject_digest,
                        &integration_worktree_id,
                        request.now_ms,
                        &format!("corrupt or unreadable persisted JobIntegrationReport: {err}"),
                    );
                    NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        format!(
                            "failed to read persisted JobIntegrationReport for `{agg_effect_id}`: {err}"
                        ),
                    )
                })?;
            if persisted.is_some()
                && !pre_existing_intent
                    .as_ref()
                    .is_some_and(|i| i.state == JobIntegrationIntentState::Completed)
            {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    "persisted JobIntegrationReport found without a Completed JobIntegrationIntent on disk",
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "persisted JobIntegrationReport found without a Completed JobIntegrationIntent on disk",
                ));
            }
            persisted
        }
        _ => None,
    };

    // Capture one frozen JobIntegrationScopeProvenance for the final batch.
    // On reopen with an existing verified JobIntegrationIntent, preserve the frozen job_revision
    // from that intent so idempotent reuse succeeds even after candidate assembly/Closure bumped job_revision.
    let batch_job_revision = match pre_existing_intent.as_ref() {
        Some(intent) => {
            if intent.job_scope.job_id != adm.job.job_id
                || intent.job_scope.approval_id != adm.approval.approval_id
                || !constant_time_str_eq(
                    &intent.job_scope.approval_subject_digest,
                    &adm.approval.subject_digest,
                )
                || !constant_time_str_eq(&intent.approved_base_commit, &adm.approval.base_commit)
                || !constant_time_str_eq(
                    &intent.initial_integration_head_commit,
                    &adm.approval.base_commit,
                )
                || intent.job_scope.target_integration_run_id != target_integration_run_id
                || intent.job_scope.aggregate_allowed_write_paths != aggregate_allowed_write_paths
                || intent.job_scope.job_revision == 0
                || intent.job_scope.job_revision > adm.job.job_revision
            {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    "persisted JobIntegrationIntent job_scope does not match current job/approval",
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "persisted JobIntegrationIntent job_scope does not match current job or approval",
                ));
            }
            intent.job_scope.job_revision
        }
        None => adm.job.job_revision,
    };

    let job_scope = JobIntegrationScopeProvenance {
        job_id: adm.job.job_id.clone(),
        job_revision: batch_job_revision,
        approval_id: adm.approval.approval_id.clone(),
        approval_subject_digest: adm.approval.subject_digest.clone(),
        target_integration_run_id: target_integration_run_id.clone(),
        aggregate_allowed_write_paths: aggregate_allowed_write_paths.clone(),
    };

    let task_inputs: Vec<JobIntegrationTaskInput> = verified_tasks
        .iter()
        .map(|vt| {
            let planned_receipt_id = derive_job_aggregation_receipt_id(
                &adm.job.job_id,
                &target_integration_run_id,
                &integration_worktree_id,
                &vt.logical_task_id,
                &vt.review_receipt.receipt_id,
            );
            JobIntegrationTaskInput {
                original_run_id: vt.original_run_id.clone(),
                task_id: vt.logical_task_id.clone(),
                task_digest: vt.task_digest.clone(),
                task_worktree_id: vt.task_worktree_id.clone(),
                subject: vt.review_subject.clone(),
                review_receipt: vt.review_receipt.clone(),
                expected_task_head_commit: vt.expected_task_head_commit.clone(),
                dependency_input: vt.dependency_input.clone(),
                strategy: frozen_strategy,
                receipt_id: Some(planned_receipt_id),
            }
        })
        .collect();

    // Reserve durable JobAggregation PendingControllerEffect BEFORE creating or mutating the Git RunIntegration worktree.
    if existing_agg_effect.is_none() {
        let _ = verify_live_job_completion_gate(
            controller,
            guard,
            request,
            Some((controller_instance_id, lease_epoch)),
            Some(entry_change_revision),
            true,
        )?;
        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            |state| {
                if state.pending_effects.contains_key(&agg_effect_id) {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "JobAggregation effect was concurrently reserved",
                    ));
                }
                state.pending_effects.insert(
                    agg_effect_id.clone(),
                    PendingControllerEffect {
                        effect_id: agg_effect_id.clone(),
                        request_key: agg_request_key.clone(),
                        job_id: request.job_id.clone(),
                        task_id: None,
                        attempt_no: None,
                        kind: PendingControllerEffectKind::JobAggregation,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: agg_subject_digest.clone(),
                        external_correlation_id: Some(integration_worktree_id.clone()),
                        created_at_ms: request.now_ms,
                        updated_at_ms: request.now_ms,
                        diagnostic: None,
                    },
                );
                Ok(())
            },
        )?;
    }

    // Create or reopen and verify the owned final RunIntegration worktree.
    let int_manifest = create_or_reopen_final_run_integration_worktree(
        controller,
        request,
        controller_instance_id,
        lease_epoch,
        &agg_effect_id,
        &agg_request_key,
        &agg_subject_digest,
        &canonical_repo,
        &canonical_work_area,
        &adm.approval.base_commit,
        &target_integration_run_id,
        &target_integration_token,
        &integration_worktree_id,
        &expected_branch_name,
        existing_agg_effect.as_ref(),
        pre_existing_intent.as_ref(),
    )?;

    let live_int_head = resolve_exact_commit_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        mark_effect_unknown_best_effort(
            controller,
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            &agg_effect_id,
            &agg_request_key,
            &agg_subject_digest,
            &integration_worktree_id,
            request.now_ms,
            &format!("failed to resolve RunIntegration HEAD: {err}"),
        );
        NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!("failed to resolve RunIntegration HEAD: {err}"),
        )
    })?;

    // If the target RunIntegration worktree HEAD has already moved past base_commit without any
    // persisted JobIntegrationIntent on disk, treat as an unverified partial target worktree and mark EffectUnknown.
    if live_int_head != adm.approval.base_commit && pre_existing_intent.is_none() {
        mark_effect_unknown_best_effort(
            controller,
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            &agg_effect_id,
            &agg_request_key,
            &agg_subject_digest,
            &integration_worktree_id,
            request.now_ms,
            "RunIntegration worktree HEAD advanced past base_commit without a persisted JobIntegrationIntent",
        );
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "RunIntegration worktree `{integration_worktree_id}` HEAD `{live_int_head}` advanced past approved base `{}` without a persisted JobIntegrationIntent",
                adm.approval.base_commit
            ),
        ));
    }

    let ops_command_key = deterministic_prefixed_id(
        "threadlane-native:coworker-job-int-ops-key:v1",
        "jok",
        &[
            &adm.job.job_id,
            &adm.approval.approval_id,
            &integration_worktree_id,
            &agg_subject_digest,
        ],
    );
    let ops_intent_id = format!(
        "jint_{}",
        &sha256_hex(
            format!(
                "threadlane-native:job-int-intent-id:v1:{}:{}:{}",
                adm.job.job_id, integration_worktree_id, ops_command_key
            )
            .as_bytes()
        )[..16]
    );

    // If the worktree is still at base_commit and no intent exists yet,
    // run `propose_job_integration` and `record_job_integration_intent_before_effect`.
    if pre_existing_intent.is_none() {
        let proposal = propose_job_integration(&ProposeJobIntegrationRequest {
            repository_path: canonical_repo.clone(),
            work_area_root: canonical_work_area.clone(),
            integration_worktree_id: integration_worktree_id.clone(),
            approved_base_commit: adm.approval.base_commit.clone(),
            expected_source_head_commit: adm.approval.base_commit.clone(),
            expected_integration_head_commit: adm.approval.base_commit.clone(),
            job_scope: job_scope.clone(),
            tasks: task_inputs.clone(),
        })
        .map_err(|err| {
            let diag = sanitize_diagnostic_text(&err.to_string());
            if pre_existing_intent.is_none() && live_int_head == adm.approval.base_commit {
                let _ = controller.policy_store().mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state.pending_effects.get_mut(&agg_effect_id) {
                            eff.status = PendingControllerEffectStatus::RefusedBeforeEffect;
                            eff.diagnostic = Some(diag.clone());
                            eff.updated_at_ms = request.now_ms;
                        }
                        Ok(())
                    },
                );
            }
            map_operations_error(err)
        })?;

        if !proposal.all_clean_merges_predicted {
            let diag =
                "propose_job_integration predicted merge conflict or non-fast-forward incompatibility"
                    .to_string();
            if pre_existing_intent.is_none() && live_int_head == adm.approval.base_commit {
                let _ = controller.policy_store().mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state.pending_effects.get_mut(&agg_effect_id) {
                            eff.status = PendingControllerEffectStatus::RefusedBeforeEffect;
                            eff.diagnostic = Some(diag.clone());
                            eff.updated_at_ms = request.now_ms;
                        }
                        Ok(())
                    },
                );
            }
            return Err(NativeError::new(NativeErrorCode::ValidationFailed, diag));
        }
    }

    let expected_int_head_for_exec = match pre_existing_intent.as_ref() {
        Some(intent) => intent.initial_integration_head_commit.clone(),
        None => adm.approval.base_commit.clone(),
    };
    let exec_req = ExecuteJobIntegrationRequest {
        repository_path: canonical_repo.clone(),
        work_area_root: canonical_work_area.clone(),
        integration_worktree_id: integration_worktree_id.clone(),
        approved_base_commit: adm.approval.base_commit.clone(),
        expected_source_head_commit: adm.approval.base_commit.clone(),
        expected_integration_head_commit: expected_int_head_for_exec,
        job_scope,
        tasks: task_inputs,
        command_key: ops_command_key,
        intent_id: Some(ops_intent_id),
        operator_id: controller_instance_id.to_string(),
        integrated_at_ms: request.now_ms.min(i64::MAX as u64) as i64,
    };

    let _ = verify_live_job_completion_gate(
        controller,
        guard,
        request,
        Some((controller_instance_id, lease_epoch)),
        Some(entry_change_revision),
        true,
    )?;

    let obs_handle = (!existing_agg_effect_confirmed).then(|| {
        begin_coworker_call_observation(CoworkerCallObservationSpec {
            journal_dir: plans_dir.to_path_buf(),
            operation: COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION.to_string(),
            title: format!("Job integration aggregation `{}`", request.job_id),
            call_key: agg_effect_id.clone(),
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            repository_path: canonical_repo.clone(),
            job_id: Some(request.job_id.clone()),
            plan_id: None,
            task_id: None,
            approval_id: Some(adm.approval.approval_id.clone()),
            attempt_no: None,
            run_id: Some(target_integration_run_id.clone()),
            worker_id: None,
            candidate_id: None,
            executor: CoworkerObservationExecutorSpec::local_git_integration(),
            verified_context: None,
            entry_summary: Some(format!("worktree_id={integration_worktree_id}")),
        })
    });

    let report = match execute_job_integration(&exec_req) {
        Ok(rep) => rep,
        Err(err) => {
            let diag = sanitize_diagnostic_text(&err.to_string());
            if let Some(obs) = obs_handle {
                let _ = obs.finish(CoworkerCallFinishSpec {
                    state: Some(ObservationCallState::Unknown),
                    diagnostic: Some(diag.clone()),
                    ..CoworkerCallFinishSpec::default()
                });
            }
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                &agg_effect_id,
                &agg_request_key,
                &agg_subject_digest,
                &integration_worktree_id,
                request.now_ms,
                &diag,
            );
            return Err(map_operations_error_after_effect(err));
        }
    };

    // Verify original per-worker receipts on disk and in runtime_state were preserved untouched.
    for vt in &verified_tasks {
        let disk_orig = load_integration_receipt(
            &canonical_work_area,
            &vt.original_integration_receipt.receipt_id,
        )
        .map_err(map_operations_error)?;
        if disk_orig != vt.original_integration_receipt || disk_orig.job_aggregation.is_some() {
            if let Some(obs) = obs_handle {
                let _ = obs.finish(CoworkerCallFinishSpec {
                    state: Some(ObservationCallState::Failed),
                    diagnostic: Some(
                        "original per-worker IntegrationReceipt was overwritten or mutated"
                            .to_string(),
                    ),
                    ..CoworkerCallFinishSpec::default()
                });
            }
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "original per-worker IntegrationReceipt `{}` for task `{}` was overwritten or mutated during job aggregation",
                    vt.original_integration_receipt.receipt_id, vt.logical_task_id
                ),
            ));
        }
    }

    let (report, report_artifact_path, report_artifact_digest) = match existing_persisted_report {
        Some(prev_report) => {
            if prev_report.job_scope != report.job_scope
                || prev_report.integration_worktree_id != report.integration_worktree_id
                || prev_report.integration_checkout_path != report.integration_checkout_path
                || prev_report.integration_branch_name != report.integration_branch_name
                || prev_report.repository_path != report.repository_path
                || prev_report.work_area_root != report.work_area_root
                || prev_report.approved_base_commit != report.approved_base_commit
                || prev_report.initial_integration_head_commit
                    != report.initial_integration_head_commit
                || prev_report.final_integrated_target_commit
                    != report.final_integrated_target_commit
                || prev_report.final_integrated_tree_sha != report.final_integrated_tree_sha
                || prev_report.source_head_commit != report.source_head_commit
                || prev_report.intent != report.intent
                || prev_report.receipts != report.receipts
            {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    &agg_effect_id,
                    &agg_request_key,
                    &agg_subject_digest,
                    &integration_worktree_id,
                    request.now_ms,
                    "persisted JobIntegrationReport drifted from live verified job integration state",
                );
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "persisted JobIntegrationReport drifted from live verified job integration state",
                ));
            }
            if existing_agg_effect_confirmed {
                let path = job_integration_report_file_path(plans_dir, &agg_effect_id);
                let bytes = read_private_file_bounded(
                    &path,
                    MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES,
                    "job_integration_report",
                )?
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "persisted JobIntegrationReport disappeared during verification",
                    )
                })?;
                (prev_report, path, sha256_hex(&bytes))
            } else {
                // Before promoting an already-visible uncertain JobIntegrationReport from
                // ReservedBeforeEffect to EffectConfirmed, explicitly establish file and parent-dir
                // durability under bounded/private-file checks without overwriting the file.
                let (durable_report, path, digest) =
                    ensure_persisted_job_integration_report_durable(plans_dir, &agg_effect_id)?;
                if durable_report != prev_report {
                    mark_effect_unknown_best_effort(
                        controller,
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        &agg_effect_id,
                        &agg_request_key,
                        &agg_subject_digest,
                        &integration_worktree_id,
                        request.now_ms,
                        "persisted JobIntegrationReport mutated during durability reconciliation",
                    );
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "persisted JobIntegrationReport mutated during durability reconciliation",
                    ));
                }
                (durable_report, path, digest)
            }
        }
        None => {
            // Persist the full bounded JobIntegrationReport to a private 0600 file separate from the 1 MiB ledger.
            let (path, digest) =
                write_persisted_job_integration_report(plans_dir, &agg_effect_id, &report)?;
            (report, path, digest)
        }
    };

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(&agg_effect_id) {
                eff.status = PendingControllerEffectStatus::EffectConfirmed;
                eff.external_correlation_id = Some(report.final_integrated_target_commit.clone());
                eff.diagnostic = None;
                eff.updated_at_ms = request.now_ms;
            }
            Ok(())
        },
    )?;

    if let Some(obs) = obs_handle {
        let _ = obs.finish(CoworkerCallFinishSpec {
            state: Some(ObservationCallState::Completed),
            receipt_id: report.receipts.last().map(|r| r.receipt_id.clone()),
            artifact_id: Some(format!("jagg_{}", &report_artifact_digest[..16])),
            ..CoworkerCallFinishSpec::default()
        });
    }

    Ok(CoworkerJobAggregationOutcome {
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        job_id: request.job_id.clone(),
        approval_id: adm.approval.approval_id.clone(),
        aggregation_effect_id: agg_effect_id,
        target_integration_run_id,
        target_integration_token,
        integration_worktree_id: report.integration_worktree_id.clone(),
        integration_checkout_path: report.integration_checkout_path.clone(),
        integration_branch_name: report.integration_branch_name.clone(),
        approved_base_commit: report.approved_base_commit.clone(),
        final_integrated_target_commit: report.final_integrated_target_commit.clone(),
        final_integrated_tree_sha: report.final_integrated_tree_sha.clone(),
        reused_existing_receipts: report.reused_existing_receipts,
        report_artifact_path,
        report_artifact_digest,
        job_integration_report: report,
    })
}

#[allow(clippy::too_many_arguments)]
fn create_or_reopen_final_run_integration_worktree(
    controller: &CoworkerControllerService,
    request: &CoworkerJobCompletionRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    agg_effect_id: &str,
    agg_request_key: &str,
    agg_subject_digest: &str,
    canonical_repo: &Path,
    canonical_work_area: &Path,
    approved_base_commit: &str,
    target_integration_run_id: &str,
    target_integration_token: &str,
    integration_worktree_id: &str,
    expected_branch_name: &str,
    existing_agg_effect: Option<&PendingControllerEffect>,
    pre_existing_intent: Option<&JobIntegrationIntent>,
) -> Result<WorktreeManifest, NativeError> {
    let manifest_path = canonical_work_area
        .join(MANIFESTS_SUBDIR)
        .join(format!("{integration_worktree_id}.json"));
    let expected_checkout_path = canonical_work_area
        .join("checkouts")
        .join("runs")
        .join(integration_worktree_id);

    let manifest_exists = fs::symlink_metadata(&manifest_path).is_ok();
    let checkout_exists = fs::symlink_metadata(&expected_checkout_path).is_ok();

    if manifest_exists || checkout_exists {
        let manifest = load_and_verify_manifest(canonical_work_area, integration_worktree_id)
            .map_err(|err| {
                mark_effect_unknown_best_effort(
                    controller,
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    agg_effect_id,
                    agg_request_key,
                    agg_subject_digest,
                    integration_worktree_id,
                    request.now_ms,
                    &format!("partial or corrupt RunIntegration worktree manifest: {err}"),
                );
                NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "failed to verify existing RunIntegration manifest `{integration_worktree_id}`: {err}"
                    ),
                )
            })?;

        if manifest.kind != WorktreeKind::RunIntegration
            || manifest.repository_path != canonical_repo
            || manifest.work_area_root != canonical_work_area
            || manifest.run_id != target_integration_run_id
            || manifest.task_id.is_some()
            || !constant_time_str_eq(&manifest.base_commit, approved_base_commit)
            || manifest.branch_name != expected_branch_name
            || manifest.partial_cleanup_accepted_head.is_some()
        {
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                agg_effect_id,
                agg_request_key,
                agg_subject_digest,
                integration_worktree_id,
                request.now_ms,
                "existing RunIntegration manifest metadata mismatch",
            );
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "existing RunIntegration manifest `{integration_worktree_id}` does not match expected owned job integration target"
                ),
            ));
        }

        ensure_worktree_job_inactive_readonly(&manifest).map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "RunIntegration worktree `{integration_worktree_id}` has an active lock: {err}"
                ),
            )
        })?;

        let clean = is_worktree_clean(&manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to inspect RunIntegration worktree cleanliness: {err}"),
                )
            })?;
        if !clean {
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                agg_effect_id,
                agg_request_key,
                agg_subject_digest,
                integration_worktree_id,
                request.now_ms,
                "existing RunIntegration worktree is dirty",
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "existing RunIntegration worktree `{integration_worktree_id}` is dirty; preserving for operator inspection"
                ),
            ));
        }

        if pre_existing_intent.is_none() {
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                agg_effect_id,
                agg_request_key,
                agg_subject_digest,
                integration_worktree_id,
                request.now_ms,
                "existing RunIntegration worktree found on disk without a persisted JobIntegrationIntent",
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "existing RunIntegration worktree `{integration_worktree_id}` found on disk without a persisted JobIntegrationIntent; preserving for operator inspection"
                ),
            ));
        }

        return Ok(manifest);
    }

    // Neither manifest nor checkout exists yet. If a prior effect was already confirmed or an intent
    // was already recorded on disk, the worktree disappeared unexpectedly -> EffectUnknown.
    if pre_existing_intent.is_some()
        || existing_agg_effect.is_some_and(|e| {
            matches!(
                e.status,
                PendingControllerEffectStatus::EffectConfirmed
                    | PendingControllerEffectStatus::EffectUnknown
            )
        })
    {
        mark_effect_unknown_best_effort(
            controller,
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            agg_effect_id,
            agg_request_key,
            agg_subject_digest,
            integration_worktree_id,
            request.now_ms,
            "RunIntegration manifest missing despite prior intent or confirmed effect",
        );
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "RunIntegration worktree `{integration_worktree_id}` is missing on disk despite prior intent or confirmed effect"
            ),
        ));
    }

    let manager = WorktreeManager::new(canonical_repo, canonical_work_area, approved_base_commit)
        .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to initialize WorktreeManager for final RunIntegration: {err}"),
        )
    })?;

    let manifest = manager
        .create_run_worktree_with_token(target_integration_run_id, target_integration_token)
        .map_err(|err| {
            let diag = sanitize_diagnostic_text(&err.to_string());
            mark_effect_unknown_best_effort(
                controller,
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                agg_effect_id,
                agg_request_key,
                agg_subject_digest,
                integration_worktree_id,
                request.now_ms,
                &diag,
            );
            NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!("failed to create final RunIntegration worktree `{integration_worktree_id}`: {err}"),
            )
        })?;

    Ok(manifest)
}

#[allow(clippy::too_many_arguments)]
async fn execute_or_reconcile_single_candidate_check(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerJobCompletionRequest,
    check_subject: &FrozenCheckSubject,
    check_subject_digest: &str,
    spec: &FrozenCheckCommandSpec,
    controller_instance_id: &str,
    lease_epoch: u64,
    entry_change_revision: u64,
) -> Result<PersistedCandidateCheckReportRef, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    let spec_digest = spec.spec_digest()?;
    let check_effect_subject_digest = sha256_hex(
        format!(
            "threadlane-native:candidate-check-effect-subject:v1:{}:{}:{}:{}:{}",
            request.job_id,
            check_subject.worktree_id,
            check_subject_digest,
            spec.check_id,
            spec_digest,
        )
        .as_bytes(),
    );
    let check_request_key = deterministic_prefixed_id(
        "threadlane-native:coworker-candidate-check-key:v1",
        "cck",
        &[
            &request.job_id,
            &check_subject.worktree_id,
            check_subject_digest,
            &spec.check_id,
            &spec_digest,
        ],
    );
    let check_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&request.job_id, &check_request_key],
    );

    let (adm, _, _, _) = verify_live_job_completion_gate(
        controller,
        guard,
        request,
        Some((controller_instance_id, lease_epoch)),
        Some(entry_change_revision),
        true,
    )?;

    // Reject any conflicting candidate CheckExecution effect for the same check_id.
    for eff in adm.runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::CheckExecution
            && eff.task_id.is_none()
            && eff.attempt_no.is_none()
            && eff.external_correlation_id.as_deref() == Some(spec.check_id.as_str())
            && (eff.effect_id != check_effect_id
                || eff.request_key != check_request_key
                || !constant_time_str_eq(&eff.subject_digest, &check_effect_subject_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "job `{}` candidate check `{}` already has a conflicting CheckExecution effect `{}`",
                    request.job_id, spec.check_id, eff.effect_id
                ),
            ));
        }
    }

    // Verify the live RunIntegration worktree is still clean and at the exact expected commit and tree
    // before reusing any cached check report or running a new check.
    verify_live_run_integration_matches_check_subject(check_subject)?;

    if let Some(existing_eff) = adm
        .runtime_state
        .pending_effects
        .get(&check_effect_id)
        .cloned()
    {
        let persisted = read_persisted_candidate_check_report(plans_dir, &check_effect_id)?;
        match existing_eff.status {
            PendingControllerEffectStatus::EffectConfirmed
            | PendingControllerEffectStatus::EffectFailedTerminal => {
                let report = persisted.ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        format!(
                            "persisted FrozenCheckExecutionReport missing for finalized candidate check effect `{check_effect_id}`"
                        ),
                    )
                })?;
                verify_cached_candidate_check_report(&report, check_subject, spec)?;
                return Ok(PersistedCandidateCheckReportRef {
                    check_id: spec.check_id.clone(),
                    effect_id: check_effect_id.clone(),
                    report_path: candidate_check_report_file_path(plans_dir, &check_effect_id),
                    report,
                });
            }
            PendingControllerEffectStatus::ReservedBeforeEffect
            | PendingControllerEffectStatus::EffectUnknown => {
                if let Some(report) = persisted {
                    verify_cached_candidate_check_report(&report, check_subject, spec)?;
                    if is_uncertain_check_report(&report) {
                        let _ = controller.policy_store().mutate_job_runtime(
                            &request.binding_id,
                            &request.job_id,
                            controller_instance_id,
                            lease_epoch,
                            |state| {
                                if let Some(eff) = state.pending_effects.get_mut(&check_effect_id) {
                                    eff.status = PendingControllerEffectStatus::EffectUnknown;
                                    eff.updated_at_ms = request.now_ms;
                                }
                                Ok(())
                            },
                        );
                        return Err(NativeError::new(
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                            format!(
                                "candidate check `{}` report indicates incomplete cleanup or ownership drift (`{}`)",
                                spec.check_id,
                                report.status.as_str()
                            ),
                        ));
                    }
                    // Before promoting an already-visible uncertain candidate check report from
                    // ReservedBeforeEffect / EffectUnknown, explicitly establish file and parent-dir
                    // durability under bounded/private-file checks without overwriting the file.
                    let durable_report = ensure_persisted_candidate_check_report_durable(
                        plans_dir,
                        &check_effect_id,
                    )?;
                    if durable_report != report {
                        return Err(NativeError::new(
                            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                            format!(
                                "persisted candidate check `{}` report mutated during durability reconciliation",
                                spec.check_id
                            ),
                        ));
                    }
                    let next_status = if durable_report.passed() {
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
                    return Ok(PersistedCandidateCheckReportRef {
                        check_id: spec.check_id.clone(),
                        effect_id: check_effect_id.clone(),
                        report_path: candidate_check_report_file_path(plans_dir, &check_effect_id),
                        report: durable_report,
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
                                    "candidate CheckExecution reservation found without a persisted report; refusing blind rerun"
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
                        "candidate check `{}` effect `{check_effect_id}` is in `{:?}` state without a verified persisted report; refusing blind rerun",
                        spec.check_id, existing_eff.status
                    ),
                ));
            }
            PendingControllerEffectStatus::RefusedBeforeEffect => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "candidate check `{}` effect `{check_effect_id}` was refused before effect",
                        spec.check_id
                    ),
                ));
            }
        }
    }

    // Pre-validate spec applicability before reserving the check effect.
    spec.validate()?;
    if !check_subject.target_kind.is_spec_applicable(spec) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "FrozenCheckCommandSpec `{}` is not enabled for RunIntegrationWorktree",
                spec.check_id
            ),
        ));
    }

    // Reserve candidate CheckExecution intent before invoking `run_frozen_check`.
    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if state.pending_effects.contains_key(&check_effect_id) {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    "candidate CheckExecution effect was concurrently reserved",
                ));
            }
            state.pending_effects.insert(
                check_effect_id.clone(),
                PendingControllerEffect {
                    effect_id: check_effect_id.clone(),
                    request_key: check_request_key.clone(),
                    job_id: request.job_id.clone(),
                    task_id: None,
                    attempt_no: None,
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
    let report_path = write_persisted_candidate_check_report(plans_dir, &check_effect_id, &report)?;

    if is_uncertain_check_report(&report) {
        let diag = format!(
            "candidate check `{}` ended with uncertain status `{}`",
            spec.check_id,
            report.status.as_str()
        );
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
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            diag,
        ));
    }

    let next_status = if report.passed() {
        PendingControllerEffectStatus::EffectConfirmed
    } else {
        PendingControllerEffectStatus::EffectFailedTerminal
    };
    let diag = if report.passed() {
        None
    } else {
        Some(format!(
            "candidate check `{}` finished with status `{}`",
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

    Ok(PersistedCandidateCheckReportRef {
        check_id: spec.check_id.clone(),
        effect_id: check_effect_id,
        report_path,
        report,
    })
}

fn is_uncertain_check_report(report: &FrozenCheckExecutionReport) -> bool {
    report.cleanup_incomplete
        || matches!(
            report.status,
            FrozenCheckExecutionStatus::IncompleteCleanup
                | FrozenCheckExecutionStatus::OwnershipOrContextDrifted
        )
}

fn verify_live_run_integration_matches_check_subject(
    subject: &FrozenCheckSubject,
) -> Result<(), NativeError> {
    let manifest = load_and_verify_manifest(&subject.work_area_root, &subject.worktree_id)
        .map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to load RunIntegration manifest `{}` before check: {e}",
                    subject.worktree_id
                ),
            )
        })?;
    if manifest.kind != WorktreeKind::RunIntegration
        || manifest.repository_path != subject.repository_path
        || manifest.work_area_root != subject.work_area_root
        || manifest.run_id != subject.expected_run_id
        || manifest.task_id != subject.expected_task_id
        || !constant_time_str_eq(&manifest.base_commit, &subject.expected_base_commit)
        || manifest.partial_cleanup_accepted_head.is_some()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "RunIntegration manifest does not match FrozenCheckSubject",
        ));
    }
    ensure_worktree_job_inactive_readonly(&manifest).map_err(|e| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("RunIntegration worktree has an active lock: {e}"),
        )
    })?;
    let clean = is_worktree_clean(&manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify RunIntegration cleanliness: {e}"),
            )
        })?;
    if !clean {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "RunIntegration worktree is dirty; refusing candidate check execution or cache reuse",
        ));
    }
    let live_head = resolve_exact_commit_sha(
        &manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|e| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve RunIntegration HEAD commit: {e}"),
        )
    })?;
    let live_tree = resolve_exact_tree_sha(
        &manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|e| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve RunIntegration HEAD tree: {e}"),
        )
    })?;
    if !constant_time_str_eq(&live_head, &subject.expected_head_commit)
        || !constant_time_str_eq(&live_tree, &subject.expected_tree_sha)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "RunIntegration worktree HEAD (`{live_head}`, tree `{live_tree}`) does not match FrozenCheckSubject (`{}`, tree `{}`)",
                subject.expected_head_commit, subject.expected_tree_sha
            ),
        ));
    }
    Ok(())
}

fn verify_cached_candidate_check_report(
    report: &FrozenCheckExecutionReport,
    check_subject: &FrozenCheckSubject,
    spec: &FrozenCheckCommandSpec,
) -> Result<(), NativeError> {
    report.verify_against_context(check_subject, spec)?;
    verify_live_run_integration_matches_check_subject(check_subject)?;
    if report.passed() {
        let _ = report.to_candidate_check_evidence(check_subject, spec)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn verify_preserved_run_integration_and_source(
    canonical_repo: &Path,
    approved_base_commit: &str,
    approved_base_tree_sha: &str,
    canonical_work_area: &Path,
    integration_worktree_id: &str,
    target_integration_run_id: &str,
    expected_branch_name: &str,
    expected_head_commit: &str,
    expected_tree_sha: &str,
) -> Result<(), NativeError> {
    let source_clean =
        is_worktree_clean(canonical_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify primary source repository cleanliness post-closure: {e}"),
            )
        })?;
    let source_head =
        resolve_exact_commit_sha(canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |e| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to resolve primary source repository HEAD post-closure: {e}"),
                )
            },
        )?;
    let source_tree =
        resolve_exact_tree_sha(canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |e| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to resolve primary source repository tree post-closure: {e}"),
                )
            },
        )?;
    if !source_clean
        || !constant_time_str_eq(&source_head, approved_base_commit)
        || !constant_time_str_eq(&source_tree, approved_base_tree_sha)
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "primary source repository was modified or moved during job completion",
        ));
    }

    let int_manifest = load_and_verify_manifest(canonical_work_area, integration_worktree_id)
        .map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify preserved RunIntegration manifest post-closure: {e}"),
            )
        })?;
    if int_manifest.kind != WorktreeKind::RunIntegration
        || int_manifest.run_id != target_integration_run_id
        || int_manifest.branch_name != expected_branch_name
        || int_manifest.partial_cleanup_accepted_head.is_some()
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "preserved RunIntegration manifest was altered or partially cleaned during job completion",
        ));
    }
    let current_branch = run_sanitized_git(
        &int_manifest.checkout_path,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "preserved RunIntegration worktree HEAD is detached",
        )
    })?;
    if current_branch.trim() != expected_branch_name {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "preserved RunIntegration worktree branch changed",
        ));
    }
    let int_clean = is_worktree_clean(&int_manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .map_err(|e| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to verify preserved RunIntegration cleanliness: {e}"),
            )
        })?;
    let int_head = resolve_exact_commit_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|e| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve preserved RunIntegration HEAD: {e}"),
        )
    })?;
    let int_tree = resolve_exact_tree_sha(
        &int_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|e| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve preserved RunIntegration tree: {e}"),
        )
    })?;
    if !int_clean
        || !constant_time_str_eq(&int_head, expected_head_commit)
        || !constant_time_str_eq(&int_tree, expected_tree_sha)
    {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "preserved RunIntegration worktree was dirtied or moved during Closure evaluation",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn mark_effect_unknown_best_effort(
    controller: &CoworkerControllerService,
    binding_id: &str,
    job_id: &str,
    controller_instance_id: &str,
    lease_epoch: u64,
    effect_id: &str,
    request_key: &str,
    subject_digest: &str,
    external_correlation_id: &str,
    now_ms: u64,
    reason: &str,
) {
    let diag = sanitize_diagnostic_text(reason);
    let _ = controller.policy_store().mutate_job_runtime(
        binding_id,
        job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                eff.status = PendingControllerEffectStatus::EffectUnknown;
                eff.diagnostic = Some(diag.clone());
                eff.updated_at_ms = now_ms;
            } else {
                state.pending_effects.insert(
                    effect_id.to_string(),
                    PendingControllerEffect {
                        effect_id: effect_id.to_string(),
                        request_key: request_key.to_string(),
                        job_id: job_id.to_string(),
                        task_id: None,
                        attempt_no: None,
                        kind: PendingControllerEffectKind::JobAggregation,
                        status: PendingControllerEffectStatus::EffectUnknown,
                        subject_digest: subject_digest.to_string(),
                        external_correlation_id: Some(external_correlation_id.to_string()),
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        diagnostic: Some(diag.clone()),
                    },
                );
            }
            Ok(())
        },
    );
}

/// Kind of private (`0600`) completion artifact persisted in `plans_dir` during job completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompletionArtifactKind {
    /// Persisted [`JobIntegrationReport`] artifact (`coworker-job-integration-report-<eff>.json`).
    JobIntegrationReport,
    /// Persisted candidate [`FrozenCheckExecutionReport`] artifact (`coworker-candidate-check-report-<eff>.json`).
    CandidateCheckExecutionReport,
}

/// Deterministic fault-injection step for [`ScopedCompletionArtifactFaultGuard`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionArtifactFaultStep {
    /// No fault for this step (allow the step to succeed normally).
    None,
    /// Fail `temp.as_file().sync_all()` before renaming the temporary file into place (`IoError`).
    PreRenameTempFileSync,
    /// Fail `File::open(dir)` immediately after renaming the temporary file into place (`PersistenceUncertain`).
    PostRenameParentOpen,
    /// Fail `dir_file.sync_all()` immediately after renaming the temporary file into place (`PersistenceUncertain`).
    PostRenameParentSync,
    /// Fail `file.sync_all()` while establishing durability on a visible uncertain artifact during recovery (`PersistenceUncertain`).
    RecoveryFileSync,
    /// Fail `File::open(dir)` while establishing durability on a visible uncertain artifact during recovery (`PersistenceUncertain`).
    RecoveryParentOpen,
    /// Fail `dir_file.sync_all()` while establishing durability on a visible uncertain artifact during recovery (`PersistenceUncertain`).
    RecoveryParentSync,
}

type CompletionArtifactFaults =
    BTreeMap<(PathBuf, CompletionArtifactKind), VecDeque<CompletionArtifactFaultStep>>;

static SCOPED_COMPLETION_ARTIFACT_FAULTS: LazyLock<Mutex<CompletionArtifactFaults>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// Scoped deterministic fault-injection guard for private completion artifact persistence and
/// recovery durability establishment, keyed by `(plans_dir, artifact_kind)`.
#[derive(Debug)]
pub struct ScopedCompletionArtifactFaultGuard {
    plans_dir: PathBuf,
    artifact_kind: CompletionArtifactKind,
}

impl ScopedCompletionArtifactFaultGuard {
    /// Installs a FIFO sequence of [`CompletionArtifactFaultStep`]s for `(plans_dir, artifact_kind)`.
    #[must_use]
    pub fn install(
        plans_dir: impl AsRef<Path>,
        artifact_kind: CompletionArtifactKind,
        steps: impl IntoIterator<Item = CompletionArtifactFaultStep>,
    ) -> Self {
        let plans_dir = plans_dir.as_ref().to_path_buf();
        let queue: VecDeque<CompletionArtifactFaultStep> = steps.into_iter().collect();
        let mut map = SCOPED_COMPLETION_ARTIFACT_FAULTS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.insert((plans_dir.clone(), artifact_kind), queue);
        Self {
            plans_dir,
            artifact_kind,
        }
    }

    /// Returns the number of unconsumed fault steps remaining for this guard's scope.
    #[must_use]
    pub fn remaining_steps(&self) -> usize {
        let map = SCOPED_COMPLETION_ARTIFACT_FAULTS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.get(&(self.plans_dir.clone(), self.artifact_kind))
            .map_or(0, VecDeque::len)
    }
}

impl Drop for ScopedCompletionArtifactFaultGuard {
    fn drop(&mut self) {
        let mut map = SCOPED_COMPLETION_ARTIFACT_FAULTS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        map.remove(&(self.plans_dir.clone(), self.artifact_kind));
    }
}

fn take_scoped_completion_artifact_fault(
    dir: &Path,
    artifact_kind: CompletionArtifactKind,
    current_step: CompletionArtifactFaultStep,
) -> bool {
    let mut map = SCOPED_COMPLETION_ARTIFACT_FAULTS
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let Some(queue) = map.get_mut(&(dir.to_path_buf(), artifact_kind)) else {
        return false;
    };
    while matches!(queue.front(), Some(CompletionArtifactFaultStep::None)) {
        queue.pop_front();
    }
    if queue.front() == Some(&current_step) {
        queue.pop_front();
        return true;
    }
    false
}

fn candidate_check_report_filename(effect_id: &str) -> String {
    format!("coworker-candidate-check-report-{effect_id}.json")
}

/// Returns the deterministic private (`0600`) file path for a persisted candidate
/// [`FrozenCheckExecutionReport`] by `effect_id`.
pub fn candidate_check_report_file_path(plans_dir: &Path, effect_id: &str) -> PathBuf {
    plans_dir.join(candidate_check_report_filename(effect_id))
}

fn write_persisted_candidate_check_report(
    plans_dir: &Path,
    effect_id: &str,
    report: &FrozenCheckExecutionReport,
) -> Result<PathBuf, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let bytes = serde_json::to_vec(report).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize candidate FrozenCheckExecutionReport JSON",
        )
    })?;
    write_private_completion_artifact_bounded(
        plans_dir,
        &candidate_check_report_filename(effect_id),
        &bytes,
        "candidate_frozen_check_execution_report",
        CompletionArtifactKind::CandidateCheckExecutionReport,
    )
}

/// Reads a persisted private (`0600`) candidate [`FrozenCheckExecutionReport`] by `effect_id`.
pub fn read_persisted_candidate_check_report(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<Option<FrozenCheckExecutionReport>, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let path = candidate_check_report_file_path(plans_dir, effect_id);
    let Some(bytes) = read_private_file_bounded(
        &path,
        MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES,
        "candidate_frozen_check_execution_report",
    )?
    else {
        return Ok(None);
    };
    let report: FrozenCheckExecutionReport = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted candidate FrozenCheckExecutionReport JSON",
        )
    })?;
    Ok(Some(report))
}

fn ensure_persisted_candidate_check_report_durable(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<FrozenCheckExecutionReport, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let bytes = ensure_private_completion_artifact_durable(
        plans_dir,
        &candidate_check_report_filename(effect_id),
        MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES,
        "candidate_frozen_check_execution_report",
        CompletionArtifactKind::CandidateCheckExecutionReport,
    )?;
    serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted candidate FrozenCheckExecutionReport JSON during durability check",
        )
    })
}

fn job_integration_report_filename(effect_id: &str) -> String {
    format!("coworker-job-integration-report-{effect_id}.json")
}

/// Returns the deterministic private (`0600`) file path for a persisted [`JobIntegrationReport`]
/// by `effect_id`.
pub fn job_integration_report_file_path(plans_dir: &Path, effect_id: &str) -> PathBuf {
    plans_dir.join(job_integration_report_filename(effect_id))
}

fn write_persisted_job_integration_report(
    plans_dir: &Path,
    effect_id: &str,
    report: &JobIntegrationReport,
) -> Result<(PathBuf, String), NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let bytes = serde_json::to_vec(report).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize JobIntegrationReport JSON",
        )
    })?;
    let digest = sha256_hex(&bytes);
    let path = write_private_completion_artifact_bounded(
        plans_dir,
        &job_integration_report_filename(effect_id),
        &bytes,
        "job_integration_report",
        CompletionArtifactKind::JobIntegrationReport,
    )?;
    Ok((path, digest))
}

/// Reads a persisted private (`0600`) [`JobIntegrationReport`] by `effect_id`.
pub fn read_persisted_job_integration_report(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<Option<JobIntegrationReport>, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let path = job_integration_report_file_path(plans_dir, effect_id);
    let Some(bytes) = read_private_file_bounded(
        &path,
        MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES,
        "job_integration_report",
    )?
    else {
        return Ok(None);
    };
    let report: JobIntegrationReport = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted JobIntegrationReport JSON",
        )
    })?;
    Ok(Some(report))
}

fn ensure_persisted_job_integration_report_durable(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<(JobIntegrationReport, PathBuf, String), NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let filename = job_integration_report_filename(effect_id);
    let path = plans_dir.join(&filename);
    let bytes = ensure_private_completion_artifact_durable(
        plans_dir,
        &filename,
        MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES,
        "job_integration_report",
        CompletionArtifactKind::JobIntegrationReport,
    )?;
    let digest = sha256_hex(&bytes);
    let report: JobIntegrationReport = serde_json::from_slice(&bytes).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to parse persisted JobIntegrationReport JSON during durability check",
        )
    })?;
    Ok((report, path, digest))
}

fn sync_completion_temp_file(
    dir: &Path,
    file: &File,
    artifact_kind: CompletionArtifactKind,
    label: &str,
) -> Result<(), NativeError> {
    if take_scoped_completion_artifact_fault(
        dir,
        artifact_kind,
        CompletionArtifactFaultStep::PreRenameTempFileSync,
    ) {
        return Err(NativeError::new(
            NativeErrorCode::IoError,
            format!("failed to sync temporary {label} file: injected pre-rename file sync failure"),
        ));
    }
    file.sync_all().map_err(|err| {
        NativeError::new(
            NativeErrorCode::IoError,
            format!(
                "failed to sync temporary {label} file: {}",
                sanitize_io_error(&err)
            ),
        )
    })
}

fn open_completion_parent_dir(
    dir: &Path,
    artifact_kind: CompletionArtifactKind,
    fault_step: CompletionArtifactFaultStep,
    label: &str,
) -> Result<File, NativeError> {
    if take_scoped_completion_artifact_fault(dir, artifact_kind, fault_step) {
        return Err(NativeError::persistence_uncertain(format!(
            "failed to open parent directory for {label} durability sync: injected parent directory open failure"
        )));
    }
    File::open(dir).map_err(|err| {
        NativeError::persistence_uncertain(format!(
            "failed to open parent directory for {label} durability sync: {}",
            sanitize_io_error(&err)
        ))
    })
}

fn sync_completion_parent_dir(
    dir: &Path,
    dir_file: &File,
    artifact_kind: CompletionArtifactKind,
    fault_step: CompletionArtifactFaultStep,
    label: &str,
) -> Result<(), NativeError> {
    if take_scoped_completion_artifact_fault(dir, artifact_kind, fault_step) {
        return Err(NativeError::persistence_uncertain(format!(
            "failed to sync parent directory for {label} durability: injected parent directory sync failure"
        )));
    }
    dir_file.sync_all().map_err(|err| {
        NativeError::persistence_uncertain(format!(
            "failed to sync parent directory for {label} durability: {}",
            sanitize_io_error(&err)
        ))
    })
}

fn sync_completion_recovery_file(
    dir: &Path,
    file: &File,
    artifact_kind: CompletionArtifactKind,
    label: &str,
) -> Result<(), NativeError> {
    if take_scoped_completion_artifact_fault(
        dir,
        artifact_kind,
        CompletionArtifactFaultStep::RecoveryFileSync,
    ) {
        return Err(NativeError::persistence_uncertain(format!(
            "failed to sync existing {label} file during durability recovery: injected recovery file sync failure"
        )));
    }
    file.sync_all().map_err(|err| {
        NativeError::persistence_uncertain(format!(
            "failed to sync existing {label} file during durability recovery: {}",
            sanitize_io_error(&err)
        ))
    })
}

fn write_private_completion_artifact_bounded(
    dir: &Path,
    filename: &str,
    bytes: &[u8],
    label: &str,
    artifact_kind: CompletionArtifactKind,
) -> Result<PathBuf, NativeError> {
    ensure_platform_supported()?;
    if bytes.len() > MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            format!(
                "{label} payload exceeds maximum size of {MAX_PERSISTED_COMPLETION_ARTIFACT_BYTES} bytes"
            ),
        ));
    }
    let target_path = dir.join(filename);
    validate_private_file_path(&target_path, label)?;

    match fs::symlink_metadata(&target_path) {
        Ok(_) => {
            return Err(NativeError::persistence_uncertain(format!(
                "refusing to overwrite existing {label} file"
            )));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to inspect target path for {label} file: {}",
                    sanitize_io_error(&err)
                ),
            ));
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp_prefix = format!(".{filename}.tmp.");
        let mut temp = tempfile::Builder::new()
            .prefix(&temp_prefix)
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(dir)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to create temporary {label} file: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;

        temp.write_all(bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to write temporary {label} file: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to set permissions on {label} file: {}",
                        sanitize_io_error(&err)
                    ),
                )
            })?;
        sync_completion_temp_file(dir, temp.as_file(), artifact_kind, label)?;

        temp.persist(&target_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to persist {label} file: {}",
                    sanitize_io_error(&err.error)
                ),
            )
        })?;
        fs::set_permissions(&target_path, fs::Permissions::from_mode(0o600)).map_err(|err| {
            NativeError::persistence_uncertain(format!(
                "failed to enforce 0600 on persisted {label} file after rename: {}",
                sanitize_io_error(&err)
            ))
        })?;
        let dir_file = open_completion_parent_dir(
            dir,
            artifact_kind,
            CompletionArtifactFaultStep::PostRenameParentOpen,
            label,
        )?;
        sync_completion_parent_dir(
            dir,
            &dir_file,
            artifact_kind,
            CompletionArtifactFaultStep::PostRenameParentSync,
            label,
        )?;
    }

    Ok(target_path)
}

fn ensure_private_completion_artifact_durable(
    dir: &Path,
    filename: &str,
    max_bytes: usize,
    label: &str,
    artifact_kind: CompletionArtifactKind,
) -> Result<Vec<u8>, NativeError> {
    ensure_platform_supported()?;
    let path = dir.join(filename);
    validate_private_file_path(&path, label)?;

    let sym_meta = fs::symlink_metadata(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::persistence_uncertain(format!(
                "persisted {label} file is missing during durability reconciliation"
            ))
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to stat {label}: {}", sanitize_io_error(&err)),
            )
        }
    })?;

    if sym_meta.file_type().is_symlink() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must not be a symlink"),
        ));
    }
    if !sym_meta.is_file() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be a regular file"),
        ));
    }

    #[cfg(unix)]
    {
        use std::io::Read;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let mode = sym_meta.permissions().mode() & 0o777;
        if (mode & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} has insecure file permissions {mode:04o} (expected 0600)"),
            ));
        }

        if sym_meta.len() > max_bytes as u64 {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("{label} exceeds maximum size of {max_bytes} bytes"),
            ));
        }

        let file = File::open(&path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to open {label}: {}", sanitize_io_error(&err)),
            )
        })?;
        let open_meta = file.metadata().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to stat open {label} descriptor: {}",
                    sanitize_io_error(&err)
                ),
            )
        })?;
        if !open_meta.is_file()
            || open_meta.dev() != sym_meta.dev()
            || open_meta.ino() != sym_meta.ino()
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} changed between metadata check and file open"),
            ));
        }
        let open_mode = open_meta.permissions().mode() & 0o777;
        if (open_mode & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "{label} open descriptor has insecure permissions {open_mode:04o} (expected 0600)"
                ),
            ));
        }
        if open_meta.len() > max_bytes as u64 {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("{label} exceeds maximum size of {max_bytes} bytes"),
            ));
        }

        let mut buf = Vec::with_capacity(open_meta.len() as usize);
        let limit = (max_bytes as u64).saturating_add(1);
        (&file).take(limit).read_to_end(&mut buf).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("failed to read {label}: {}", sanitize_io_error(&err)),
            )
        })?;
        if buf.len() > max_bytes {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                format!("{label} exceeds maximum size of {max_bytes} bytes"),
            ));
        }

        sync_completion_recovery_file(dir, &file, artifact_kind, label)?;
        let dir_file = open_completion_parent_dir(
            dir,
            artifact_kind,
            CompletionArtifactFaultStep::RecoveryParentOpen,
            label,
        )?;
        sync_completion_parent_dir(
            dir,
            &dir_file,
            artifact_kind,
            CompletionArtifactFaultStep::RecoveryParentSync,
            label,
        )?;

        Ok(buf)
    }
}

fn compute_deterministic_task_integration_order(
    tasks: &[CoworkerTaskRecord],
) -> Result<Vec<String>, NativeError> {
    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    let mut dependents: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for task in tasks {
        if in_degree.insert(task.task_id.clone(), 0).is_some() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("duplicate task_id `{}` in job.tasks", task.task_id),
            ));
        }
        dependents.insert(task.task_id.clone(), Vec::new());
    }
    for task in tasks {
        let mut seen_deps = BTreeSet::new();
        for dep in &task.depends_on {
            if !seen_deps.insert(dep.as_str()) {
                continue;
            }
            let Some(children) = dependents.get_mut(dep) else {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{}` depends on unknown task `{dep}`", task.task_id),
                ));
            };
            children.push(task.task_id.clone());
            if let Some(deg) = in_degree.get_mut(&task.task_id) {
                *deg += 1;
            }
        }
    }

    let mut ready: BTreeSet<String> = in_degree
        .iter()
        .filter_map(|(tid, &deg)| if deg == 0 { Some(tid.clone()) } else { None })
        .collect();
    let mut ordered = Vec::with_capacity(tasks.len());

    while let Some(next_id) = ready.pop_first() {
        ordered.push(next_id.clone());
        if let Some(children) = dependents.get(&next_id) {
            for child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        ready.insert(child.clone());
                    }
                }
            }
        }
    }

    if ordered.len() != tasks.len() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "job.tasks dependency graph contains a cycle",
        ));
    }
    Ok(ordered)
}

fn verify_and_promote_candidate_check_reports(
    check_subject: &FrozenCheckSubject,
    check_commands: &BTreeMap<String, FrozenCheckCommandSpec>,
    reports: &[FrozenCheckExecutionReport],
) -> Result<Vec<CandidateCheckEvidence>, NativeError> {
    let required_specs: Vec<&FrozenCheckCommandSpec> = check_commands
        .values()
        .filter(|spec| spec.required_for_candidate_closure)
        .collect();
    if required_specs.is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "check_commands has no required_for_candidate_closure specs",
        ));
    }
    if reports.len() != required_specs.len() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "candidate check report count does not match required_for_candidate_closure spec count",
        ));
    }

    let mut evidence = Vec::with_capacity(required_specs.len());
    for spec in required_specs {
        let report = reports
            .iter()
            .find(|r| r.check_id == spec.check_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "missing FrozenCheckExecutionReport for required candidate check `{}`",
                        spec.check_id
                    ),
                )
            })?;
        report.verify_against_context(check_subject, spec)?;
        let ev = report.to_candidate_check_evidence(check_subject, spec)?;
        ev.validate_passed()?;
        evidence.push(ev);
    }
    Ok(evidence)
}

fn map_operations_error(err: OperationsError) -> NativeError {
    let code = match &err {
        OperationsError::SecurityViolation(_) => NativeErrorCode::SecurityViolation,
        OperationsError::UncertainWorkerOutcome(_) => {
            NativeErrorCode::AmbiguousWakeupPendingReconciliation
        }
        _ => NativeErrorCode::ValidationFailed,
    };
    NativeError::new(code, sanitize_diagnostic_text(&err.to_string()))
}

fn map_operations_error_after_effect(err: OperationsError) -> NativeError {
    let code = match &err {
        OperationsError::SecurityViolation(_) => NativeErrorCode::SecurityViolation,
        OperationsError::UncertainWorkerOutcome(_)
        | OperationsError::GitCommandFailed { .. }
        | OperationsError::Io(_)
        | OperationsError::HeadMoved { .. }
        | OperationsError::DirtyWorktree { .. }
        | OperationsError::ReceiptInvalidated(_) => {
            NativeErrorCode::AmbiguousWakeupPendingReconciliation
        }
        _ => NativeErrorCode::ValidationFailed,
    };
    NativeError::new(code, sanitize_diagnostic_text(&err.to_string()))
}
