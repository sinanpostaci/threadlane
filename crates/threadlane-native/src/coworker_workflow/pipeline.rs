use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use threadlane_host::{
    ApprovedHostClient, DependencyPredecessorSelection, HostError, PrepareDependencyInputsRequest,
    TaskExecutionStatus, WORK_AREA_SUBDIR,
};
use threadlane_operations::load_integration_receipt;
use threadlane_workers::{
    is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::WorkflowRole;
use tokio::sync::watch;

use crate::conversation_change::ConversationChangeStore;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, sanitize_diagnostic_text, validate_bounded_identifier,
};

use super::contracts::{
    CoworkerJobPhase, CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage, OutboxDispatchKind,
    OutboxDispatchStatus, ReviewVerdictOutcome, MAX_ATTEMPTS_PER_TASK, MAX_COWORKER_ID_LEN,
};
use super::controller::{
    count_active_coder_executions, has_reconciled_terminal_coder_attempt,
    is_task_coder_execution_active, is_task_fenced_by_hold_or_sibling,
    task_has_derived_attempt_or_post_initial_state, CoworkerControllerService,
    CoworkerControllerStepReport, CoworkerControllerStepRequest, OwnedCoworkerControllerSession,
    VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, ControllerJobRuntimeState, PendingControllerEffectKind,
    PendingControllerEffectStatus, DEFAULT_CONTROLLER_LEASE_TTL_MS,
    MAX_IN_SCOPE_CORRECTION_ATTEMPTS,
};
use super::job_completion::{
    CoworkerJobCompletionOutcome, CoworkerJobCompletionRequest, CoworkerJobCompletionService,
    CoworkerJobCompletionStatus,
};
use super::ownership::CoworkerJobOwnershipGuard;
use super::task_correction_execution::{
    read_persisted_derived_task_execution_report, CoworkerTaskCorrectionExecuteOutcome,
    CoworkerTaskCorrectionExecuteRequest, CoworkerTaskCorrectionExecuteStatus,
    CoworkerTaskCorrectionExecutionService,
};
use super::task_integration::{
    CoworkerTaskIntegrationOutcome, CoworkerTaskIntegrationRequest, CoworkerTaskIntegrationService,
    CoworkerTaskIntegrationStatus,
};
use super::task_review_stage::{
    read_persisted_task_review_report, CoworkerTaskReviewStageOutcome,
    CoworkerTaskReviewStageRequest, CoworkerTaskReviewStageService, CoworkerTaskReviewStageStatus,
};
use super::task_verification::{
    read_persisted_check_report, CoworkerTaskVerificationOutcome, CoworkerTaskVerificationRequest,
    CoworkerTaskVerificationService, CoworkerTaskVerificationStatus,
};

/// Default bounded poll interval in milliseconds between pipeline driver iterations when waiting
/// for host worker completion or in-flight stage futures.
pub const DEFAULT_PIPELINE_POLL_INTERVAL_MS: u64 = 15;

/// Default bounded non-blocking window in milliseconds used during a single [`CoworkerPipelineDriver::tick`]
/// to drain fast ready stage completions while keeping slower stage futures in-flight across ticks.
pub const DEFAULT_PIPELINE_TICK_DRAIN_WINDOW_MS: u64 = 25;

/// Identity-only request for driving an admitted Coworker workflow job through the automatic
/// pipeline.
///
/// The request carries only scope identity and a caller-supplied wall-clock timestamp; it never
/// accepts raw prompts, model/tool overrides, synthetic verdicts, or check command overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerPipelineRequest {
    /// Scoped workflow binding identifier.
    pub binding_id: String,
    /// Owning native conversation thread UUID.
    pub thread_uuid: String,
    /// Admitted Coworker workflow job identifier.
    pub job_id: String,
    /// Caller-supplied monotonic wall-clock timestamp in milliseconds since the Unix epoch.
    pub now_ms: u64,
}

/// High-level truthful disposition of a Coworker job after a pipeline tick or settled drive loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerPipelineStatus {
    /// All frozen tasks are integrated, combined candidate checks passed, and `Closure` returned
    /// `Accepted` once (`CoworkerJobPhase::Completed`).
    Completed,
    /// Work is actively executing on the host, in-flight inside a pipeline stage future, or ready
    /// to advance on the next tick.
    Running,
    /// One or more tasks received a verified `Reviewer` `CorrectInScope` verdict
    /// (`CoworkerTaskStage::CorrectInScopeRequired`), cannot dispatch another derived correction
    /// under the frozen correction caps/budgets, and no other independent tasks can advance.
    AwaitingCorrectionExecution,
    /// One or more tasks failed required frozen task-review checks, or final combined candidate
    /// checks failed, and no other independent tasks can advance.
    BlockedByFailedChecks,
    /// Remaining tasks or job completion are fenced by an active `WorkflowHoldRecord` (including
    /// `ScopeConflict`), unresolved blocking conversation change, or impacted sibling without a
    /// complete operator-attested `DeclaredIndependenceCompletenessReceipt`.
    Held,
    /// `runtime_state.stop_requested == true` fences all new dispatches, promotions, and completions.
    Stopped,
    /// One or more tasks or `PendingControllerEffect`s are in `EffectUnknown` pending receipt
    /// reconciliation.
    BlockedOnUnknownEffect,
    /// One or more tasks are in `Blocked`, `Failed`, or `Cancelled`, or `Closure` returned a
    /// non-`Accepted` verdict, and no other independent tasks can advance.
    BlockedOrFailed,
}

/// Stage kind for an in-flight pipeline future held in [`CoworkerPipelineDriver`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerPipelineInFlightStageKind {
    /// Derived in-scope `Coder` correction execution in the reused owned task worktree
    /// (`CoworkerTaskCorrectionExecutionService`).
    TaskCorrection,
    /// Per-task deterministic git commit, scope validation, frozen check execution, and review
    /// subject promotion (`CoworkerTaskVerificationService`).
    TaskVerification,
    /// Supervised read-only `Reviewer` evaluation and durable `ReviewDecisionReceipt` persistence
    /// (`CoworkerTaskReviewStageService`).
    TaskReview,
    /// Original per-task `RunIntegration` of a `Reviewer`-accepted task candidate into the job
    /// integration worktree (`CoworkerTaskIntegrationService`).
    TaskIntegration,
    /// Final job aggregation, combined candidate check execution, and single `Closure` evaluation
    /// (`CoworkerJobCompletionService`).
    JobCompletion,
}

/// Compact descriptor of an in-flight stage future retained across [`CoworkerPipelineDriver`] ticks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerPipelineInFlightStageRef {
    /// Task identifier for task-scoped in-flight stages, or `None` for job-level completion.
    pub task_id: Option<String>,
    /// Task attempt number for task-scoped in-flight stages, or `None` for job-level completion.
    pub attempt_no: Option<u32>,
    /// Kind of pipeline stage future currently retained in `FuturesUnordered`.
    pub kind: CoworkerPipelineInFlightStageKind,
}

/// Per-task truthful snapshot reported by [`CoworkerPipelineTickReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerPipelineTaskSnapshot {
    /// Frozen task identifier within the Coworker job.
    pub task_id: String,
    /// Current 1-based task execution attempt number.
    pub attempt_no: u32,
    /// Durable workflow stage recorded on the `CoworkerTaskRecord`.
    pub stage: CoworkerTaskStage,
    /// Live host execution status reported by the approved session host for this task.
    pub host_status: TaskExecutionStatus,
    /// Whether this task is currently fenced by a hold, stop request, blocking conversation
    /// change, or unindependent sibling failure/unknown state.
    pub fenced: bool,
    /// Active pipeline stage future retained by [`CoworkerPipelineDriver`] for this task attempt,
    /// if any.
    pub in_flight_stage: Option<CoworkerPipelineInFlightStageKind>,
    /// Deterministic candidate identifier recorded for this task attempt, if promoted.
    pub candidate_id: Option<String>,
    /// Whether a verified immutable `TaskReviewSubject` has been promoted for this task attempt.
    pub review_subject_promoted: bool,
    /// Parsed `Reviewer` verdict outcome recorded for the current candidate, if evaluated.
    pub review_verdict_outcome: Option<ReviewVerdictOutcome>,
    /// Durable `ReviewDecisionReceipt` identifier bound to the current candidate, if persisted.
    pub review_receipt_id: Option<String>,
    /// Durable per-task `IntegrationReceipt` identifier bound to the current candidate, if
    /// integrated.
    pub integration_receipt_id: Option<String>,
    /// True when the task is at the truthful `CorrectInScopeRequired` boundary awaiting a future
    /// derived correction execution slice.
    pub awaiting_correction_execution: bool,
    /// Frozen task-review check identifiers that failed on the current committed attempt tree.
    pub failed_check_ids: Vec<String>,
    /// Sanitized actionable diagnostic explaining why this task is blocked, failed, or awaiting
    /// correction, if applicable.
    pub blocking_diagnostic: Option<String>,
}

/// Compact truthful outcome of a stateful [`CoworkerPipelineDriver::tick`] or settled drive loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerPipelineTickReport {
    /// Scoped workflow binding identifier.
    pub binding_id: String,
    /// Owning native conversation thread UUID.
    pub thread_uuid: String,
    /// Admitted Coworker workflow job identifier.
    pub job_id: String,
    /// Exact human execution approval identifier governing this job.
    pub approval_id: String,
    /// Current durable `CoworkerJobPhase` recorded in the workflow store.
    pub job_phase: CoworkerJobPhase,
    /// Current optimistic concurrency revision of the `CoworkerJobRecord`.
    pub job_revision: u64,
    /// Current logical controller lease epoch held in `ControllerJobRuntimeState`.
    pub lease_epoch: u64,
    /// Deterministic controller instance identifier bound to the held physical ownership guard.
    pub controller_instance_id: String,
    /// High-level truthful pipeline status after this tick or settled drive loop.
    pub status: CoworkerPipelineStatus,
    /// Descriptors of stage futures still retained in-flight by the [`CoworkerPipelineDriver`].
    pub in_flight_stages: Vec<CoworkerPipelineInFlightStageRef>,
    /// Task identifiers dispatched to the approved host as `Coder` workers during the run.
    pub dispatched_task_ids: Vec<String>,
    /// Task identifiers reconciled from completed host `Coder` runs into `ReadyForReview`.
    pub reconciled_ready_for_review_task_ids: Vec<String>,
    /// Task identifiers whose attempt committed cleanly, passed frozen checks, and promoted a
    /// `TaskReviewSubject`.
    pub verified_task_ids: Vec<String>,
    /// Task identifiers evaluated by a supervised `Reviewer` with a persisted
    /// `ReviewDecisionReceipt`.
    pub reviewed_task_ids: Vec<String>,
    /// Task identifiers integrated into the job integration worktree with a persisted
    /// `IntegrationReceipt`.
    pub integrated_task_ids: Vec<String>,
    /// Downstream task identifiers whose sealed `DependencyInputSeal` was prepared from integrated
    /// predecessors.
    pub prepared_dependency_task_ids: Vec<String>,
    /// Downstream task identifiers promoted from `PendingDependencies` to `ReadyToDispatch`.
    pub promoted_dependency_ready_task_ids: Vec<String>,
    /// Task identifiers currently in `CorrectInScopeRequired` awaiting derived correction
    /// execution.
    pub awaiting_correction_task_ids: Vec<String>,
    /// Task identifiers currently blocked by failed required frozen task-review checks.
    pub failed_check_task_ids: Vec<String>,
    /// Task identifiers fenced by an active hold, stop request, blocking conversation change, or
    /// unindependent sibling.
    pub fenced_task_ids: Vec<String>,
    /// Task identifiers blocked by an unreconciled `EffectUnknown` stage or pending effect.
    pub unknown_effect_task_ids: Vec<String>,
    /// Task identifiers in a terminal or non-recoverable `Blocked`, `Failed`, or `Cancelled`
    /// stage.
    pub blocked_or_failed_task_ids: Vec<String>,
    /// Per-task truthful state snapshots in job task order.
    pub tasks: Vec<CoworkerPipelineTaskSnapshot>,
    /// Final job completion outcome (`CoworkerJobCompletionOutcome`) when job aggregation,
    /// combined candidate checks, or `Closure` evaluation has run.
    pub completion_outcome: Option<CoworkerJobCompletionOutcome>,
}

#[derive(Debug)]
enum InFlightStageCompletion {
    TaskCorrection {
        task_id: String,
        attempt_no: u32,
        result: Result<Box<CoworkerTaskCorrectionExecuteOutcome>, NativeError>,
    },
    TaskVerification {
        task_id: String,
        attempt_no: u32,
        result: Result<Box<CoworkerTaskVerificationOutcome>, NativeError>,
    },
    TaskReview {
        task_id: String,
        attempt_no: u32,
        result: Result<Box<CoworkerTaskReviewStageOutcome>, NativeError>,
    },
    TaskIntegration {
        task_id: String,
        attempt_no: u32,
        result: Result<Box<CoworkerTaskIntegrationOutcome>, NativeError>,
    },
    JobCompletion {
        result: Result<Box<CoworkerJobCompletionOutcome>, NativeError>,
    },
}

type BoxStageFuture<'g> = Pin<Box<dyn Future<Output = InFlightStageCompletion> + Send + 'g>>;

/// Automatic Coworker workflow pipeline service connecting Phase 1 host reconciliation/dispatch,
/// derived in-scope `Coder` correction execution, frozen task verification, supervised read-only
/// `Reviewer` evaluation, original per-task `RunIntegration`, downstream dependency input seal
/// preparation, and final job aggregation + combined candidate checks + single `Closure`
/// evaluation.
#[derive(Clone)]
pub struct CoworkerPipelineService {
    controller: CoworkerControllerService,
    correction_service: CoworkerTaskCorrectionExecutionService,
    verification_service: CoworkerTaskVerificationService,
    review_service: CoworkerTaskReviewStageService,
    integration_service: CoworkerTaskIntegrationService,
    completion_service: CoworkerJobCompletionService,
}

impl std::fmt::Debug for CoworkerPipelineService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoworkerPipelineService")
            .field("controller", &self.controller)
            .finish_non_exhaustive()
    }
}

impl CoworkerPipelineService {
    /// Constructs a [`CoworkerPipelineService`] from an existing [`CoworkerControllerService`].
    pub fn new(controller: CoworkerControllerService) -> Result<Self, NativeError> {
        let correction_service = CoworkerTaskCorrectionExecutionService::new(controller.clone());
        let verification_service = CoworkerTaskVerificationService::new(controller.clone());
        let review_service = CoworkerTaskReviewStageService::new(controller.clone());
        let integration_service = CoworkerTaskIntegrationService::new(controller.clone());
        let completion_service = CoworkerJobCompletionService::from_controller(controller.clone())?;
        Ok(Self {
            controller,
            correction_service,
            verification_service,
            review_service,
            integration_service,
            completion_service,
        })
    }

    /// Alias for [`Self::new`].
    pub fn from_controller(controller: CoworkerControllerService) -> Result<Self, NativeError> {
        Self::new(controller)
    }

    /// Returns a reference to the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Returns a reference to the underlying [`CoworkerTaskCorrectionExecutionService`].
    pub fn correction_service(&self) -> &CoworkerTaskCorrectionExecutionService {
        &self.correction_service
    }

    /// Returns a reference to the underlying [`CoworkerTaskVerificationService`].
    pub fn verification_service(&self) -> &CoworkerTaskVerificationService {
        &self.verification_service
    }

    /// Returns a reference to the underlying [`CoworkerTaskReviewStageService`].
    pub fn review_service(&self) -> &CoworkerTaskReviewStageService {
        &self.review_service
    }

    /// Returns a reference to the underlying [`CoworkerTaskIntegrationService`].
    pub fn integration_service(&self) -> &CoworkerTaskIntegrationService {
        &self.integration_service
    }

    /// Returns a reference to the underlying [`CoworkerJobCompletionService`].
    pub fn completion_service(&self) -> &CoworkerJobCompletionService {
        &self.completion_service
    }

    /// Creates a stateful [`CoworkerPipelineDriver`] borrowing `guard` that preserves in-flight
    /// per-task/attempt stage futures across [`CoworkerPipelineDriver::tick`] iterations.
    pub fn driver<'g>(
        &self,
        guard: &'g CoworkerJobOwnershipGuard,
        binding_id: impl Into<String>,
        thread_uuid: impl Into<String>,
        job_id: impl Into<String>,
    ) -> Result<CoworkerPipelineDriver<'g>, NativeError> {
        CoworkerPipelineDriver::new(
            self.clone(),
            guard,
            binding_id.into(),
            thread_uuid.into(),
            job_id.into(),
        )
    }

    /// Acquires physical OS job ownership, verifies automatic controller admission, and opens an
    /// [`OwnedCoworkerPipelineSession`] suitable for an independent controller process loop.
    pub fn open_owned_session(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        now_ms: u64,
    ) -> Result<OwnedCoworkerPipelineSession, NativeError> {
        let controller_session =
            self.controller
                .open_owned_session(binding_id, thread_uuid, job_id, now_ms)?;
        OwnedCoworkerPipelineSession::from_controller_session(self.clone(), controller_session)
    }

    /// Drives an admitted job until it reaches a settled state (`Completed`,
    /// `AwaitingCorrectionExecution`, `BlockedByFailedChecks`, `Held`, `Stopped`,
    /// `BlockedOnUnknownEffect`, or `BlockedOrFailed`) and drains all in-flight stage futures
    /// while borrowing `guard`.
    ///
    /// Keeps in-flight stage futures in `FuturesUnordered` across driver iterations so one slow
    /// `Reviewer` or check subprocess never blocks independent tasks from verifying, reviewing,
    /// integrating, preparing downstream dependency seals, or dispatching ready dependents.
    pub async fn drive_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerPipelineRequest,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        validate_pipeline_request(request)?;
        let mut driver = self.driver(
            guard,
            request.binding_id.clone(),
            request.thread_uuid.clone(),
            request.job_id.clone(),
        )?;
        Box::pin(driver.drive_until_settled(request.now_ms)).await
    }
}

/// Stateful async pipeline driver that borrows a held [`CoworkerJobOwnershipGuard`] and retains
/// in-flight per-task/attempt stage futures in [`FuturesUnordered`] across iterations.
///
/// Callers driving a job incrementally via [`Self::tick`] must keep the [`CoworkerPipelineDriver`]
/// instance alive across ticks until [`Self::in_flight_count`] is `0` (or call
/// [`Self::drain_in_flight`] / [`Self::drive_until_settled`]) so active check, `Reviewer`, or
/// `Closure` futures finish their bounded execution and persist full private `0600` reports.
pub struct CoworkerPipelineDriver<'g> {
    service: CoworkerPipelineService,
    guard: &'g CoworkerJobOwnershipGuard,
    binding_id: String,
    thread_uuid: String,
    job_id: String,
    in_flight: FuturesUnordered<BoxStageFuture<'g>>,
    active_task_stages: BTreeMap<(String, u32), CoworkerPipelineInFlightStageKind>,
    job_completion_in_flight: bool,
    in_flight_stages_tx: watch::Sender<Vec<CoworkerPipelineInFlightStageRef>>,
    failed_check_tasks: BTreeMap<(String, u32), Vec<String>>,
    last_completion_outcome: Option<Box<CoworkerJobCompletionOutcome>>,
    drain_window_ms: u64,
}

impl<'g> std::fmt::Debug for CoworkerPipelineDriver<'g> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoworkerPipelineDriver")
            .field("binding_id", &self.binding_id)
            .field("thread_uuid", &self.thread_uuid)
            .field("job_id", &self.job_id)
            .field("active_task_stages", &self.active_task_stages)
            .field("job_completion_in_flight", &self.job_completion_in_flight)
            .finish_non_exhaustive()
    }
}

impl<'g> CoworkerPipelineDriver<'g> {
    /// Creates a new [`CoworkerPipelineDriver`] borrowing `guard`.
    pub fn new(
        service: CoworkerPipelineService,
        guard: &'g CoworkerJobOwnershipGuard,
        binding_id: String,
        thread_uuid: String,
        job_id: String,
    ) -> Result<Self, NativeError> {
        validate_bounded_identifier(&binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        service
            .controller
            .verify_physical_ownership(guard, &binding_id, &thread_uuid, &job_id)?;
        let (in_flight_stages_tx, _) = watch::channel(Vec::new());
        Ok(Self {
            service,
            guard,
            binding_id,
            thread_uuid,
            job_id,
            in_flight: FuturesUnordered::new(),
            active_task_stages: BTreeMap::new(),
            job_completion_in_flight: false,
            in_flight_stages_tx,
            failed_check_tasks: BTreeMap::new(),
            last_completion_outcome: None,
            drain_window_ms: DEFAULT_PIPELINE_TICK_DRAIN_WINDOW_MS,
        })
    }

    /// Configures the per-tick non-blocking drain window in milliseconds used to collect ready
    /// stage completions while leaving slower stage futures in-flight across ticks.
    pub fn with_drain_window_ms(mut self, drain_window_ms: u64) -> Self {
        self.drain_window_ms = drain_window_ms.clamp(1, 1_000);
        self
    }

    /// Returns a reference to the borrowed [`CoworkerJobOwnershipGuard`].
    pub fn guard(&self) -> &'g CoworkerJobOwnershipGuard {
        self.guard
    }

    /// Returns the number of currently in-flight stage futures retained by this driver.
    pub fn in_flight_count(&self) -> usize {
        self.active_task_stages.len() + usize::from(self.job_completion_in_flight)
    }

    /// Returns the list of currently in-flight stage descriptors.
    pub fn in_flight_stages(&self) -> Vec<CoworkerPipelineInFlightStageRef> {
        let mut refs: Vec<CoworkerPipelineInFlightStageRef> = self
            .active_task_stages
            .iter()
            .map(
                |((task_id, attempt_no), &kind)| CoworkerPipelineInFlightStageRef {
                    task_id: Some(task_id.clone()),
                    attempt_no: Some(*attempt_no),
                    kind,
                },
            )
            .collect();
        if self.job_completion_in_flight {
            refs.push(CoworkerPipelineInFlightStageRef {
                task_id: None,
                attempt_no: None,
                kind: CoworkerPipelineInFlightStageKind::JobCompletion,
            });
        }
        refs
    }

    /// Subscribes to a cloneable read-only live view of currently in-flight stage descriptors.
    ///
    /// This snapshot is diagnostic metadata only and carries no execution or lease authority. A
    /// closed (or open) receiver must never be treated as proof that an owner remains alive or
    /// holds authority.
    pub fn subscribe_in_flight_stages(
        &self,
    ) -> watch::Receiver<Vec<CoworkerPipelineInFlightStageRef>> {
        self.in_flight_stages_tx.subscribe()
    }

    fn publish_in_flight_stages(&self) {
        let _ = self
            .in_flight_stages_tx
            .send_replace(self.in_flight_stages());
    }

    /// Drains all currently in-flight stage futures to completion without starting any new stages
    /// or dispatching new tasks, ensuring every active check, `Reviewer`, or `Closure` subprocess
    /// finishes within its frozen per-call budget and persists its full private `0600` report.
    pub async fn drain_in_flight(
        &mut self,
        now_ms: u64,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        let result = Box::pin(self.drain_in_flight_inner(now_ms)).await;
        self.publish_in_flight_stages();
        result
    }

    async fn drain_in_flight_inner(
        &mut self,
        now_ms: u64,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        if now_ms == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "pipeline drain_in_flight now_ms must be > 0",
            ));
        }
        let mut acc = TickAccumulator::default();
        Box::pin(self.drain_all_in_flight_to_completion(now_ms, &mut acc)).await?;
        Box::pin(self.build_tick_report(now_ms, acc)).await
    }

    /// Executes one bounded pipeline tick at `now_ms`, advancing ready tasks and draining any
    /// stage futures that complete within the tick's bounded window while preserving slower stage
    /// futures in `self.in_flight` for subsequent ticks on this same driver instance.
    pub async fn tick(&mut self, now_ms: u64) -> Result<CoworkerPipelineTickReport, NativeError> {
        let result = Box::pin(self.tick_inner(now_ms)).await;
        self.publish_in_flight_stages();
        result
    }

    async fn tick_inner(&mut self, now_ms: u64) -> Result<CoworkerPipelineTickReport, NativeError> {
        if now_ms == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "pipeline tick now_ms must be > 0",
            ));
        }

        let mut acc = TickAccumulator::default();
        let mut attempted_corrections: BTreeSet<(String, u32)> = BTreeSet::new();
        let mut attempted_verifications: BTreeSet<(String, u32)> = BTreeSet::new();
        let mut attempted_reviews: BTreeSet<(String, u32)> = BTreeSet::new();
        let mut attempted_integrations: BTreeSet<(String, u32)> = BTreeSet::new();
        let mut attempted_completion = false;

        // 1. Short admission & lease verification under step_mutex (never held across provider/check await).
        let (initial_phase, task_count) =
            match Box::pin(self.acquire_or_verify_short_admission(now_ms)).await {
                Ok(adm) => (adm.job.phase, adm.job.tasks.len()),
                Err(err) => {
                    Box::pin(self.best_effort_drain_on_error(now_ms, &mut acc)).await;
                    return Err(err);
                }
            };

        // 2. If the job is already Completed and nothing is in flight, re-verify completed state via
        //    completion_service (which reuses cached check reports and cached Closure verdict without
        //    spawning any new check or model calls) so tampering is caught and no extra Closure runs.
        if initial_phase == CoworkerJobPhase::Completed && self.in_flight.is_empty() {
            let req = CoworkerJobCompletionRequest {
                binding_id: self.binding_id.clone(),
                thread_uuid: self.thread_uuid.clone(),
                job_id: self.job_id.clone(),
                now_ms,
            };
            let outcome = Box::pin(
                self.service
                    .completion_service
                    .complete_job(self.guard, &req),
            )
            .await
            .map(Box::new)?;
            self.last_completion_outcome = Some(outcome.clone());
            acc.completion_outcome = Some(outcome);
            return Box::pin(self.build_tick_report(now_ms, acc)).await;
        }

        // 3. Initial Phase 1 dependency seal preparation and host reconciliation/dispatch pass.
        if let Err(err) = Box::pin(self.run_phase1_and_dependency_prep(now_ms, &mut acc)).await {
            Box::pin(self.best_effort_drain_on_error(now_ms, &mut acc)).await;
            return Err(err);
        }

        // 4. Iteratively schedule eligible per-task stages into `self.in_flight` and drain any
        //    stage futures that finish within the bounded tick drain window.
        let max_inner_rounds = task_count.saturating_mul(10).max(16);
        for _round in 0..max_inner_rounds {
            let scheduled = match Box::pin(self.schedule_eligible_stages(
                now_ms,
                &mut attempted_corrections,
                &mut attempted_verifications,
                &mut attempted_reviews,
                &mut attempted_integrations,
                &mut attempted_completion,
            ))
            .await
            {
                Ok(s) => s,
                Err(err) => {
                    Box::pin(self.best_effort_drain_on_error(now_ms, &mut acc)).await;
                    return Err(err);
                }
            };

            if self.in_flight.is_empty() {
                if !scheduled {
                    break;
                }
                continue;
            }

            let drained_any = match Box::pin(self.drain_ready_in_flight(now_ms, &mut acc)).await {
                Ok(d) => d,
                Err(err) => {
                    Box::pin(self.best_effort_drain_on_error(now_ms, &mut acc)).await;
                    return Err(err);
                }
            };
            if !drained_any {
                // Remaining futures in `self.in_flight` are still awaiting slow child processes;
                // keep them alive in `self.in_flight` across ticks and return the current snapshot.
                break;
            }

            // After draining one or more completed stage futures (e.g., task integration), run
            // dependency seal preparation and Phase 1 reconciliation/dispatch so newly unblocked
            // dependent tasks can be released without waiting for a slow sibling Reviewer.
            if let Err(err) = Box::pin(self.run_phase1_and_dependency_prep(now_ms, &mut acc)).await
            {
                Box::pin(self.best_effort_drain_on_error(now_ms, &mut acc)).await;
                return Err(err);
            }
        }

        Box::pin(self.build_tick_report(now_ms, acc)).await
    }

    /// Drives the job until it settles into a non-`Running` [`CoworkerPipelineStatus`] and all
    /// in-flight stage futures have completed.
    ///
    /// Execution is bounded strictly by the job's frozen per-call worker/check/Reviewer/Closure
    /// timeouts, frozen role call caps, frozen attempt caps, and durable `stop_requested` / hold
    /// state rather than an arbitrary global job wall-clock cutoff. When a stop, hold, or blocking
    /// state occurs while stage futures are in-flight, existing stage futures are drained to
    /// completion so full private `0600` reports are saved while promotion is fenced.
    pub async fn drive_until_settled(
        &mut self,
        initial_now_ms: u64,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        Box::pin(self.drive_until_settled_inner(initial_now_ms)).await
    }

    async fn drive_until_settled_inner(
        &mut self,
        initial_now_ms: u64,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        if initial_now_ms == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "pipeline drive initial_now_ms must be > 0",
            ));
        }

        let start_instant = tokio::time::Instant::now();
        let mut cumulative_acc = TickAccumulator::default();

        loop {
            let elapsed_ms = start_instant.elapsed().as_millis() as u64;
            let now_ms = initial_now_ms.saturating_add(elapsed_ms);

            let tick_report = Box::pin(self.tick(now_ms)).await?;
            cumulative_acc.merge_from_report(&tick_report);

            if tick_report.status != CoworkerPipelineStatus::Running {
                if !self.in_flight.is_empty() {
                    let drain_now_ms =
                        initial_now_ms.saturating_add(start_instant.elapsed().as_millis() as u64);
                    Box::pin(
                        self.drain_all_in_flight_to_completion(drain_now_ms, &mut cumulative_acc),
                    )
                    .await?;
                    let settled_now_ms =
                        initial_now_ms.saturating_add(start_instant.elapsed().as_millis() as u64);
                    let final_snapshot = Box::pin(
                        self.build_tick_report(settled_now_ms, TickAccumulator::default()),
                    )
                    .await?;
                    cumulative_acc.merge_from_report(&final_snapshot);
                    return Ok(cumulative_acc.into_final_report(final_snapshot));
                }
                return Ok(cumulative_acc.into_final_report(tick_report));
            }

            if !self.in_flight.is_empty() {
                // Wait up to DEFAULT_PIPELINE_POLL_INTERVAL_MS for the next in-flight stage future
                // to complete before the next host reconciliation / scheduling tick.
                let completed_opt = tokio::time::timeout(
                    Duration::from_millis(DEFAULT_PIPELINE_POLL_INTERVAL_MS),
                    self.in_flight.next(),
                )
                .await;
                if let Ok(Some(done)) = completed_opt {
                    let elapsed_after = start_instant.elapsed().as_millis() as u64;
                    let handle_now_ms = initial_now_ms.saturating_add(elapsed_after);
                    if let Err(err) = Box::pin(self.handle_stage_completion(
                        done,
                        handle_now_ms,
                        &mut cumulative_acc,
                    ))
                    .await
                    {
                        Box::pin(
                            self.best_effort_drain_on_error(handle_now_ms, &mut cumulative_acc),
                        )
                        .await;
                        return Err(err);
                    }
                }
            } else {
                tokio::time::sleep(Duration::from_millis(DEFAULT_PIPELINE_POLL_INTERVAL_MS)).await;
            }
        }
    }

    async fn drain_all_in_flight_to_completion(
        &mut self,
        now_ms: u64,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        let mut first_err: Option<NativeError> = None;
        while let Some(completion) = self.in_flight.next().await {
            if let Err(err) = Box::pin(self.handle_stage_completion(completion, now_ms, acc)).await
            {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    async fn best_effort_drain_on_error(&mut self, now_ms: u64, acc: &mut TickAccumulator) {
        if self.in_flight.is_empty() {
            return;
        }
        if self
            .service
            .controller
            .verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )
            .is_err()
        {
            return;
        }
        while let Some(completion) = self.in_flight.next().await {
            let _ = Box::pin(self.handle_stage_completion(completion, now_ms, acc)).await;
        }
    }

    async fn acquire_or_verify_short_admission(
        &self,
        now_ms: u64,
    ) -> Result<Box<VerifiedControllerAdmission>, NativeError> {
        let _guard_lock = self.service.controller.step_mutex().lock().await;
        self.service.controller.verify_physical_ownership(
            self.guard,
            &self.binding_id,
            &self.thread_uuid,
            &self.job_id,
        )?;

        let mut adm = Box::new(self.service.controller.verify_job_admission(
            self.guard,
            &self.binding_id,
            &self.thread_uuid,
            &self.job_id,
        )?);

        let own_instance_id = deterministic_controller_instance_id(
            &self.binding_id,
            &self.job_id,
            self.guard.instance_id(),
        );

        let needs_lease = match adm.runtime_state.lease.as_ref() {
            Some(lease) => {
                !lease.active
                    || lease.instance_id != own_instance_id
                    || lease.lease_epoch == 0
                    || lease.expires_at_ms <= now_ms
            }
            None => true,
        };

        if needs_lease {
            if let Some(ref prior_lease) = adm.runtime_state.lease {
                if prior_lease.active && prior_lease.instance_id != own_instance_id {
                    self.service.controller.verify_physical_ownership(
                        self.guard,
                        &self.binding_id,
                        &self.thread_uuid,
                        &self.job_id,
                    )?;
                    self.service.controller.policy_store().release_lease(
                        &self.binding_id,
                        &self.job_id,
                        &prior_lease.instance_id,
                        prior_lease.lease_epoch,
                    )?;
                }
            }
            self.service.controller.verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;
            let _ = self
                .service
                .controller
                .policy_store()
                .acquire_or_renew_lease(
                    &self.binding_id,
                    &self.job_id,
                    &own_instance_id,
                    now_ms.max(1),
                    DEFAULT_CONTROLLER_LEASE_TTL_MS,
                )?;
            *adm = self.service.controller.verify_job_admission(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;
        }

        let lease_epoch = adm
            .runtime_state
            .lease
            .as_ref()
            .filter(|l| l.active && l.instance_id == own_instance_id && l.lease_epoch > 0)
            .map(|l| l.lease_epoch)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "ControllerJobRuntimeState has no active driver lease for current physical owner",
                )
            })?;

        let plans_dir = self.service.controller.workflow_store().plans_dir();
        if let Ok(change_store) = ConversationChangeStore::open_readonly(plans_dir) {
            if let Ok(change_rev) = change_store.current_revision(&self.binding_id) {
                if change_rev > adm.runtime_state.synced_change_revision {
                    self.service.controller.policy_store().mutate_job_runtime(
                        &self.binding_id,
                        &self.job_id,
                        &own_instance_id,
                        lease_epoch,
                        |state| {
                            if change_rev > state.synced_change_revision {
                                state.synced_change_revision = change_rev;
                            }
                            Ok(())
                        },
                    )?;
                    *adm = self.service.controller.verify_job_admission(
                        self.guard,
                        &self.binding_id,
                        &self.thread_uuid,
                        &self.job_id,
                    )?;
                }
            }
        }

        verify_no_conflicting_effect_keys(&adm.job, &adm.runtime_state)?;

        Ok(adm)
    }

    async fn run_phase1_and_dependency_prep(
        &mut self,
        now_ms: u64,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        let adm = Box::pin(self.acquire_or_verify_short_admission(now_ms)).await?;
        if adm.job.phase != CoworkerJobPhase::Executing {
            return Ok(());
        }

        let step_req = CoworkerControllerStepRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            now_ms,
            lease_ttl_ms: None,
        };

        if has_global_unknown_effect(&adm.runtime_state) || adm.runtime_state.stop_requested {
            drop(adm);
            // Still run step_job so live host snapshots and fenced/unknown statuses are reconciled
            // without dispatching new tasks or preparing new dependency seals.
            let step_report =
                Box::pin(self.service.controller.step_job(self.guard, &step_req)).await?;
            acc.record_step_report(&step_report);
            return Ok(());
        }

        // Prepare dependency input seals on the host for any `PendingDependencies` tasks whose
        // predecessors are all `Integrated` with verified review + integration receipts and that
        // are not fenced by hold/stop/change/sibling/unknown.
        let ready_candidates = self.collect_ready_dependency_candidates(&adm, acc)?;
        let approval_id = adm.approval.approval_id.clone();
        let subject_digest = adm.approval.subject_digest.clone();
        drop(adm);

        if !ready_candidates.is_empty() {
            Box::pin(self.prepare_ready_dependency_inputs(
                &approval_id,
                &subject_digest,
                ready_candidates,
                acc,
            ))
            .await?;
        }

        // Run Phase 1 reconciliation and bounded initial Coder dispatch (`max_parallel`).
        let step_report = Box::pin(self.service.controller.step_job(self.guard, &step_req)).await?;
        acc.record_step_report(&step_report);

        Ok(())
    }

    fn collect_ready_dependency_candidates(
        &self,
        adm: &VerifiedControllerAdmission,
        acc: &mut TickAccumulator,
    ) -> Result<Vec<(String, Vec<DependencyPredecessorSelection>)>, NativeError> {
        if adm.runtime_state.stop_requested
            || adm.job.phase != CoworkerJobPhase::Executing
            || has_global_unknown_effect(&adm.runtime_state)
        {
            return Ok(Vec::new());
        }

        let plans_dir = self.service.controller.workflow_store().plans_dir();
        let mut ready_candidates: Vec<(String, Vec<DependencyPredecessorSelection>)> = Vec::new();
        for task in &adm.job.tasks {
            if task.stage != CoworkerTaskStage::PendingDependencies || task.depends_on.is_empty() {
                continue;
            }
            if is_task_fenced_in_pipeline(
                task,
                &adm.job,
                &adm.runtime_state,
                &adm.approval.approval_id,
                plans_dir,
            )? {
                push_unique(&mut acc.fenced_task_ids, &task.task_id);
                continue;
            }

            if let Some(selections) = collect_integrated_predecessor_selections(task, &adm.job) {
                ready_candidates.push((task.task_id.clone(), selections));
            }
        }

        Ok(ready_candidates)
    }

    async fn prepare_ready_dependency_inputs(
        &self,
        approval_id: &str,
        subject_digest: &str,
        ready_candidates: Vec<(String, Vec<DependencyPredecessorSelection>)>,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        let host_client = ApprovedHostClient::new(
            std::sync::Arc::clone(self.service.controller.authority()),
            self.service.controller.sessions_dir(),
            &self.binding_id,
            &self.thread_uuid,
        )
        .map_err(map_pipeline_host_error)?;

        for (task_id, selections) in ready_candidates {
            self.service.controller.verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;

            let snap = host_client
                .get_task(approval_id, &task_id)
                .await
                .map_err(map_pipeline_host_error)?;

            self.service.controller.verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;

            if snap.status == TaskExecutionStatus::DependencyInputsUnverified {
                host_client
                    .prepare_dependency_inputs(PrepareDependencyInputsRequest {
                        approval_id: approval_id.to_string(),
                        expected_approval_digest: subject_digest.to_string(),
                        task_id: task_id.clone(),
                        predecessors: selections,
                    })
                    .await
                    .map_err(map_pipeline_host_error)?;

                self.service.controller.verify_physical_ownership(
                    self.guard,
                    &self.binding_id,
                    &self.thread_uuid,
                    &self.job_id,
                )?;

                push_unique(&mut acc.prepared_dependency_task_ids, &task_id);
            }
        }

        Ok(())
    }

    fn has_active_stage_for_task(&self, task_id: &str) -> bool {
        self.active_task_stages
            .keys()
            .any(|(tid, _)| tid == task_id)
    }

    fn other_active_coder_count(
        &self,
        adm: &VerifiedControllerAdmission,
        candidate_task_id: &str,
    ) -> usize {
        adm.job
            .tasks
            .iter()
            .filter(|t| {
                t.task_id != candidate_task_id
                    && (is_task_coder_execution_active(t, &adm.job, &adm.runtime_state)
                        || self.active_task_stages.iter().any(|((tid, _), kind)| {
                            tid == &t.task_id
                                && *kind == CoworkerPipelineInFlightStageKind::TaskCorrection
                        }))
            })
            .count()
    }

    async fn schedule_eligible_stages(
        &mut self,
        now_ms: u64,
        attempted_corrections: &mut BTreeSet<(String, u32)>,
        attempted_verifications: &mut BTreeSet<(String, u32)>,
        attempted_reviews: &mut BTreeSet<(String, u32)>,
        attempted_integrations: &mut BTreeSet<(String, u32)>,
        attempted_completion: &mut bool,
    ) -> Result<bool, NativeError> {
        let adm = Box::pin(self.acquire_or_verify_short_admission(now_ms)).await?;
        if adm.runtime_state.stop_requested || has_global_unknown_effect(&adm.runtime_state) {
            return Ok(false);
        }

        let plans_dir = self
            .service
            .controller
            .workflow_store()
            .plans_dir()
            .to_path_buf();
        let mut scheduled_any = false;

        if adm.job.phase == CoworkerJobPhase::Executing {
            let mut simulated_job = Box::new(adm.job.clone());
            for ((active_tid, _), active_kind) in &self.active_task_stages {
                if matches!(
                    active_kind,
                    CoworkerPipelineInFlightStageKind::TaskCorrection
                        | CoworkerPipelineInFlightStageKind::TaskReview
                        | CoworkerPipelineInFlightStageKind::TaskIntegration
                ) {
                    if let Some(t) = simulated_job
                        .tasks
                        .iter_mut()
                        .find(|t| &t.task_id == active_tid)
                    {
                        if t.unresolved_outbox_id.is_none() {
                            t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                        }
                    }
                }
            }

            for task in &adm.job.tasks {
                let attempt_no = task.current_attempt_no.max(1);
                let key = (task.task_id.clone(), attempt_no);

                // Never schedule more than one stage future per task at a time.
                if self.has_active_stage_for_task(&task.task_id) {
                    continue;
                }

                if is_task_fenced_in_pipeline(
                    task,
                    &simulated_job,
                    &adm.runtime_state,
                    &adm.approval.approval_id,
                    &plans_dir,
                )? {
                    continue;
                }

                let art_key = ControllerJobRuntimeState::artifact_key(&task.task_id, attempt_no);
                let artifacts_opt = adm.runtime_state.attempt_artifacts.get(&art_key);

                match task.stage {
                    CoworkerTaskStage::CorrectInScopeRequired => {
                        let next_attempt_no = attempt_no.saturating_add(1);
                        let corr_key = (task.task_id.clone(), next_attempt_no);
                        if !attempted_corrections.contains(&corr_key)
                            && can_task_dispatch_derived_correction(&adm, task, attempt_no)
                            && self.other_active_coder_count(&adm, &task.task_id)
                                < adm.approval.max_parallel
                        {
                            attempted_corrections.insert(corr_key);
                            self.spawn_task_correction_future(
                                task.task_id.clone(),
                                attempt_no,
                                next_attempt_no,
                                now_ms,
                            );
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
                        if attempt_no >= 2 =>
                    {
                        let prior_attempt_no = attempt_no - 1;
                        let corr_key = (task.task_id.clone(), attempt_no);
                        let has_derived_receipt = adm
                            .runtime_state
                            .derived_attempts
                            .iter()
                            .any(|d| d.task_id == task.task_id && d.attempt_no == attempt_no);
                        let is_fenced_late = has_fenced_late_derived_report(
                            &plans_dir,
                            &adm.runtime_state,
                            &task.task_id,
                            attempt_no,
                        )?;
                        let capacity_ok = task.stage == CoworkerTaskStage::Running
                            || self.other_active_coder_count(&adm, &task.task_id)
                                < adm.approval.max_parallel;
                        if has_derived_receipt
                            && !is_fenced_late
                            && capacity_ok
                            && !attempted_corrections.contains(&corr_key)
                        {
                            attempted_corrections.insert(corr_key);
                            self.spawn_task_correction_future(
                                task.task_id.clone(),
                                prior_attempt_no,
                                attempt_no,
                                now_ms,
                            );
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    CoworkerTaskStage::ReadyForReview => {
                        let has_review_subject =
                            artifacts_opt.is_some_and(|a| a.review_subject.is_some());
                        if !has_review_subject {
                            if self.failed_check_tasks.contains_key(&key)
                                || attempted_verifications.contains(&key)
                            {
                                continue;
                            }
                            if let Some(failed_ids) = detect_persisted_failed_task_checks(
                                &plans_dir,
                                &adm.runtime_state,
                                &task.task_id,
                                attempt_no,
                            )? {
                                self.failed_check_tasks.insert(key, failed_ids);
                                continue;
                            }
                            if has_unreconcilable_task_verification_unknown_effect(
                                &plans_dir,
                                self.service.controller.sessions_dir(),
                                &adm,
                                &task.task_id,
                                attempt_no,
                            )? {
                                continue;
                            }

                            attempted_verifications.insert(key.clone());
                            self.spawn_task_verification_future(
                                task.task_id.clone(),
                                attempt_no,
                                now_ms,
                            );
                            scheduled_any = true;
                        } else if !attempted_reviews.contains(&key) {
                            attempted_reviews.insert(key.clone());
                            self.spawn_task_review_future(task.task_id.clone(), attempt_no, now_ms);
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    CoworkerTaskStage::ReviewReserved | CoworkerTaskStage::UnderReview => {
                        if !attempted_reviews.contains(&key) {
                            attempted_reviews.insert(key.clone());
                            self.spawn_task_review_future(task.task_id.clone(), attempt_no, now_ms);
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    CoworkerTaskStage::Accepted | CoworkerTaskStage::IntegrationReserved => {
                        if !attempted_integrations.contains(&key) {
                            attempted_integrations.insert(key.clone());
                            self.spawn_task_integration_future(
                                task.task_id.clone(),
                                attempt_no,
                                now_ms,
                            );
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    CoworkerTaskStage::EffectUnknown => {
                        // After crash/reopen, only reconcile an EffectUnknown stage if its full
                        // report or receipt is actually present on disk; never blind-replay.
                        if can_reconcile_effect_unknown_review(
                            &plans_dir,
                            &adm.job,
                            &adm.runtime_state,
                            &task.task_id,
                            attempt_no,
                        )? && !attempted_reviews.contains(&key)
                        {
                            attempted_reviews.insert(key.clone());
                            self.spawn_task_review_future(task.task_id.clone(), attempt_no, now_ms);
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        } else if can_reconcile_effect_unknown_integration(
                            self.service.controller.sessions_dir(),
                            &adm,
                            task,
                            attempt_no,
                        )? && !attempted_integrations.contains(&key)
                        {
                            attempted_integrations.insert(key.clone());
                            self.spawn_task_integration_future(
                                task.task_id.clone(),
                                attempt_no,
                                now_ms,
                            );
                            if let Some(t) = simulated_job
                                .tasks
                                .iter_mut()
                                .find(|t| t.task_id == task.task_id)
                            {
                                t.unresolved_outbox_id = Some("in_flight_outbox".to_string());
                            }
                            scheduled_any = true;
                        }
                    }
                    _ => {}
                }
            }
        }

        // Schedule final job completion once ALL tasks are Integrated, no task stage futures are
        // in-flight, and no active hold/stop/change/unknown fence exists.
        if !self.job_completion_in_flight
            && !*attempted_completion
            && self.active_task_stages.is_empty()
            && !adm.job.tasks.is_empty()
            && adm
                .job
                .tasks
                .iter()
                .all(|t| t.stage == CoworkerTaskStage::Integrated)
            && matches!(
                adm.job.phase,
                CoworkerJobPhase::Executing
                    | CoworkerJobPhase::AssemblingCandidate
                    | CoworkerJobPhase::ClosureReview
            )
            && can_advance_to_job_completion(&adm, &plans_dir)?
        {
            *attempted_completion = true;
            self.spawn_job_completion_future(now_ms);
            scheduled_any = true;
        }

        Ok(scheduled_any)
    }

    fn spawn_task_correction_future(
        &mut self,
        task_id: String,
        prior_attempt_no: u32,
        next_attempt_no: u32,
        now_ms: u64,
    ) {
        let correction_service = self.service.correction_service.clone();
        let guard = self.guard;
        let req = CoworkerTaskCorrectionExecuteRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            task_id: task_id.clone(),
            prior_attempt_no,
            now_ms,
        };
        self.active_task_stages.insert(
            (task_id.clone(), next_attempt_no),
            CoworkerPipelineInFlightStageKind::TaskCorrection,
        );
        self.in_flight.push(Box::pin(async move {
            let result = Box::pin(correction_service.execute_task_correction(guard, &req))
                .await
                .map(Box::new);
            InFlightStageCompletion::TaskCorrection {
                task_id,
                attempt_no: next_attempt_no,
                result,
            }
        }));
        self.publish_in_flight_stages();
    }

    fn spawn_task_verification_future(&mut self, task_id: String, attempt_no: u32, now_ms: u64) {
        let verification_service = self.service.verification_service.clone();
        let guard = self.guard;
        let req = CoworkerTaskVerificationRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            now_ms,
        };
        self.active_task_stages.insert(
            (task_id.clone(), attempt_no),
            CoworkerPipelineInFlightStageKind::TaskVerification,
        );
        self.in_flight.push(Box::pin(async move {
            let result =
                Box::pin(verification_service.verify_and_prepare_task_attempt(guard, &req))
                    .await
                    .map(Box::new);
            InFlightStageCompletion::TaskVerification {
                task_id,
                attempt_no,
                result,
            }
        }));
        self.publish_in_flight_stages();
    }

    fn spawn_task_review_future(&mut self, task_id: String, attempt_no: u32, now_ms: u64) {
        let review_service = self.service.review_service.clone();
        let guard = self.guard;
        let req = CoworkerTaskReviewStageRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            now_ms,
            untrusted_candidate_context: None,
        };
        self.active_task_stages.insert(
            (task_id.clone(), attempt_no),
            CoworkerPipelineInFlightStageKind::TaskReview,
        );
        self.in_flight.push(Box::pin(async move {
            let result = Box::pin(review_service.review_task_attempt(guard, &req))
                .await
                .map(Box::new);
            InFlightStageCompletion::TaskReview {
                task_id,
                attempt_no,
                result,
            }
        }));
        self.publish_in_flight_stages();
    }

    fn spawn_task_integration_future(&mut self, task_id: String, attempt_no: u32, now_ms: u64) {
        let integration_service = self.service.integration_service.clone();
        let controller = self.service.controller.clone();
        let guard = self.guard;
        let req = CoworkerTaskIntegrationRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            task_id: task_id.clone(),
            attempt_no,
            now_ms,
        };
        self.active_task_stages.insert(
            (task_id.clone(), attempt_no),
            CoworkerPipelineInFlightStageKind::TaskIntegration,
        );
        self.in_flight.push(Box::pin(async move {
            let _short_cas_lock = controller.step_mutex().lock().await;
            let result = integration_service
                .integrate_accepted_task(guard, &req)
                .map(Box::new);
            InFlightStageCompletion::TaskIntegration {
                task_id,
                attempt_no,
                result,
            }
        }));
        self.publish_in_flight_stages();
    }

    fn spawn_job_completion_future(&mut self, now_ms: u64) {
        let completion_service = self.service.completion_service.clone();
        let guard = self.guard;
        let req = CoworkerJobCompletionRequest {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            now_ms,
        };
        self.job_completion_in_flight = true;
        self.in_flight.push(Box::pin(async move {
            let result = Box::pin(completion_service.complete_job(guard, &req))
                .await
                .map(Box::new);
            InFlightStageCompletion::JobCompletion { result }
        }));
        self.publish_in_flight_stages();
    }

    async fn drain_ready_in_flight(
        &mut self,
        now_ms: u64,
        acc: &mut TickAccumulator,
    ) -> Result<bool, NativeError> {
        let mut drained_any = false;
        while !self.in_flight.is_empty() {
            let window = Duration::from_millis(self.drain_window_ms);
            match tokio::time::timeout(window, self.in_flight.next()).await {
                Ok(Some(completion)) => {
                    drained_any = true;
                    Box::pin(self.handle_stage_completion(completion, now_ms, acc)).await?;
                }
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }
        Ok(drained_any)
    }

    async fn handle_stage_completion(
        &mut self,
        completion: InFlightStageCompletion,
        now_ms: u64,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        let result = Box::pin(self.handle_stage_completion_inner(completion, now_ms, acc)).await;
        self.publish_in_flight_stages();
        result
    }

    async fn handle_stage_completion_inner(
        &mut self,
        completion: InFlightStageCompletion,
        now_ms: u64,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        self.service.controller.verify_physical_ownership(
            self.guard,
            &self.binding_id,
            &self.thread_uuid,
            &self.job_id,
        )?;

        match completion {
            InFlightStageCompletion::TaskCorrection {
                task_id,
                attempt_no,
                result,
            } => {
                self.active_task_stages
                    .remove(&(task_id.clone(), attempt_no));
                self.active_task_stages
                    .retain(|(tid, _), _| tid != &task_id);
                match result {
                    Ok(outcome) => match outcome.status {
                        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification => {
                            acc.awaiting_correction_task_ids.retain(|id| id != &task_id);
                            push_unique(&mut acc.dispatched_task_ids, &task_id);
                            push_unique(&mut acc.reconciled_ready_for_review_task_ids, &task_id);
                        }
                        CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity => {}
                        CoworkerTaskCorrectionExecuteStatus::FailedTerminal
                        | CoworkerTaskCorrectionExecuteStatus::CancelledTerminal => {
                            push_unique(&mut acc.blocked_or_failed_task_ids, &task_id);
                        }
                        CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved => {
                            push_unique(&mut acc.fenced_task_ids, &task_id);
                        }
                    },
                    Err(err) => {
                        Box::pin(
                            self.classify_task_stage_error(&task_id, attempt_no, now_ms, err, acc),
                        )
                        .await?;
                    }
                }
            }
            InFlightStageCompletion::TaskVerification {
                task_id,
                attempt_no,
                result,
            } => {
                self.active_task_stages
                    .remove(&(task_id.clone(), attempt_no));
                match result {
                    Ok(outcome) => match outcome.status {
                        CoworkerTaskVerificationStatus::ReadyForReviewer => {
                            self.failed_check_tasks
                                .remove(&(task_id.clone(), attempt_no));
                            push_unique(&mut acc.verified_task_ids, &task_id);
                        }
                        CoworkerTaskVerificationStatus::BlockedByFailedChecks => {
                            let failed_ids: Vec<String> = outcome
                                .check_reports
                                .iter()
                                .filter(|r| !r.report.passed())
                                .map(|r| r.check_id.clone())
                                .collect();
                            self.failed_check_tasks
                                .insert((task_id.clone(), attempt_no), failed_ids);
                            push_unique(&mut acc.failed_check_task_ids, &task_id);
                        }
                    },
                    Err(err) => {
                        Box::pin(
                            self.classify_task_stage_error(&task_id, attempt_no, now_ms, err, acc),
                        )
                        .await?;
                    }
                }
            }
            InFlightStageCompletion::TaskReview {
                task_id,
                attempt_no,
                result,
            } => {
                self.active_task_stages
                    .remove(&(task_id.clone(), attempt_no));
                match result {
                    Ok(outcome) => {
                        push_unique(&mut acc.reviewed_task_ids, &task_id);
                        match outcome.status {
                            CoworkerTaskReviewStageStatus::Accepted => {}
                            CoworkerTaskReviewStageStatus::CorrectInScopeRequired => {}
                            CoworkerTaskReviewStageStatus::HeldByScopeConflict => {
                                push_unique(&mut acc.fenced_task_ids, &task_id);
                            }
                            CoworkerTaskReviewStageStatus::BlockedByVerdict => {
                                push_unique(&mut acc.blocked_or_failed_task_ids, &task_id);
                            }
                        }
                    }
                    Err(err) => {
                        Box::pin(
                            self.classify_task_stage_error(&task_id, attempt_no, now_ms, err, acc),
                        )
                        .await?;
                    }
                }
            }
            InFlightStageCompletion::TaskIntegration {
                task_id,
                attempt_no,
                result,
            } => {
                self.active_task_stages
                    .remove(&(task_id.clone(), attempt_no));
                match result {
                    Ok(outcome) => match outcome.status {
                        CoworkerTaskIntegrationStatus::Integrated
                        | CoworkerTaskIntegrationStatus::ReopenedIntegrated => {
                            push_unique(&mut acc.integrated_task_ids, &task_id);
                        }
                        CoworkerTaskIntegrationStatus::FencedAfterEffect => {
                            push_unique(&mut acc.fenced_task_ids, &task_id);
                        }
                    },
                    Err(err) => {
                        Box::pin(
                            self.classify_task_stage_error(&task_id, attempt_no, now_ms, err, acc),
                        )
                        .await?;
                    }
                }
            }
            InFlightStageCompletion::JobCompletion { result } => {
                self.job_completion_in_flight = false;
                match result {
                    Ok(outcome) => {
                        self.last_completion_outcome = Some(outcome.clone());
                        acc.completion_outcome = Some(outcome);
                    }
                    Err(err) => {
                        Box::pin(self.classify_job_completion_error(now_ms, err, acc)).await?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn classify_task_stage_error(
        &self,
        task_id: &str,
        attempt_no: u32,
        now_ms: u64,
        err: NativeError,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        if err.code() == NativeErrorCode::SecurityViolation.as_str() {
            return Err(err);
        }

        let adm = Box::pin(self.acquire_or_verify_short_admission(now_ms)).await?;
        let plans_dir = self.service.controller.workflow_store().plans_dir();
        let Some(task) = adm.job.tasks.iter().find(|t| t.task_id == task_id) else {
            return Err(err);
        };

        // 1. If the stage service transitioned the task to `Blocked` (e.g. malformed/prose/non-zero
        //    Reviewer output recorded as EffectFailedTerminal), expose the actionable Blocked state
        //    without blind retry.
        if matches!(
            task.stage,
            CoworkerTaskStage::Blocked | CoworkerTaskStage::Failed | CoworkerTaskStage::Cancelled
        ) {
            push_unique(&mut acc.blocked_or_failed_task_ids, task_id);
            return Ok(());
        }

        // 2. If the task or one of its effects transitioned to `EffectUnknown`, expose actionable
        //    BlockedOnUnknownEffect state.
        let has_task_unknown_eff = adm.runtime_state.pending_effects.values().any(|e| {
            e.task_id.as_deref() == Some(task_id)
                && e.attempt_no.unwrap_or(attempt_no) == attempt_no
                && e.status == PendingControllerEffectStatus::EffectUnknown
        });
        if task.stage == CoworkerTaskStage::EffectUnknown || has_task_unknown_eff {
            push_unique(&mut acc.unknown_effect_task_ids, task_id);
            return Ok(());
        }

        // 3. If a hold, stop_requested, blocking conversation change, or non-independent sibling
        //    fenced the task while its check/review was in-flight (preserving late artifacts on disk),
        //    record it as fenced rather than failing the whole pipeline tick.
        if is_task_fenced_in_pipeline(
            task,
            &adm.job,
            &adm.runtime_state,
            &adm.approval.approval_id,
            plans_dir,
        )? {
            push_unique(&mut acc.fenced_task_ids, task_id);
            return Ok(());
        }

        Err(err)
    }

    async fn classify_job_completion_error(
        &self,
        now_ms: u64,
        err: NativeError,
        acc: &mut TickAccumulator,
    ) -> Result<(), NativeError> {
        if err.code() == NativeErrorCode::SecurityViolation.as_str() {
            return Err(err);
        }

        let adm = Box::pin(self.acquire_or_verify_short_admission(now_ms)).await?;
        if has_global_unknown_effect(&adm.runtime_state)
            || err.code() == NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
        {
            return Ok(());
        }

        let plans_dir = self.service.controller.workflow_store().plans_dir();
        if !can_advance_to_job_completion(&adm, plans_dir)? {
            for t in &adm.job.tasks {
                push_unique(&mut acc.fenced_task_ids, &t.task_id);
            }
            return Ok(());
        }

        Err(err)
    }

    async fn build_tick_report(
        &mut self,
        now_ms: u64,
        mut acc: TickAccumulator,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        let adm = Box::pin(self.acquire_or_verify_short_admission(now_ms)).await?;
        let plans_dir = self
            .service
            .controller
            .workflow_store()
            .plans_dir()
            .to_path_buf();

        let controller_instance_id = deterministic_controller_instance_id(
            &self.binding_id,
            &self.job_id,
            self.guard.instance_id(),
        );
        let lease_epoch = adm
            .runtime_state
            .lease
            .as_ref()
            .map(|l| l.lease_epoch)
            .unwrap_or(0);

        let host_client = ApprovedHostClient::new(
            std::sync::Arc::clone(self.service.controller.authority()),
            self.service.controller.sessions_dir(),
            &self.binding_id,
            &self.thread_uuid,
        )
        .map_err(map_pipeline_host_error)?;

        let mut job_for_fence = Box::new(adm.job.clone());
        for t in &mut job_for_fence.tasks {
            if self.has_active_stage_for_task(&t.task_id)
                && t.stage != CoworkerTaskStage::EffectUnknown
            {
                t.unresolved_outbox_id = None;
            }
        }

        let mut task_snapshots = Vec::with_capacity(adm.job.tasks.len());
        let mut active_host_coder_count = 0usize;

        for task in &adm.job.tasks {
            self.service.controller.verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;

            let snap = host_client
                .get_task(&adm.approval.approval_id, &task.task_id)
                .await
                .map_err(map_pipeline_host_error)?;

            self.service.controller.verify_physical_ownership(
                self.guard,
                &self.binding_id,
                &self.thread_uuid,
                &self.job_id,
            )?;

            let ledger_attempt_no = task.current_attempt_no.max(1);
            let (in_flight_stage, active_stage_attempt_no) = self
                .active_task_stages
                .get(&(task.task_id.clone(), ledger_attempt_no))
                .map(|&k| (Some(k), Some(ledger_attempt_no)))
                .or_else(|| {
                    self.active_task_stages
                        .iter()
                        .find(|((tid, _), _)| tid == &task.task_id)
                        .map(|((_, att), &k)| (Some(k), Some(*att)))
                })
                .unwrap_or((None, None));
            let attempt_no = active_stage_attempt_no.unwrap_or(ledger_attempt_no);
            let key = (task.task_id.clone(), attempt_no);

            let has_task_unknown_eff = adm.runtime_state.pending_effects.values().any(|e| {
                e.task_id.as_deref() == Some(task.task_id.as_str())
                    && e.status == PendingControllerEffectStatus::EffectUnknown
            });

            let has_fenced_late_derived = has_fenced_late_derived_report(
                &plans_dir,
                &adm.runtime_state,
                &task.task_id,
                attempt_no,
            )?;
            let has_reconciled_terminal_coder =
                has_reconciled_terminal_coder_attempt(task, &adm.job, &adm.runtime_state);
            let fenced_initial_coder = has_reconciled_terminal_coder
                && matches!(
                    task.stage,
                    CoworkerTaskStage::DispatchReserved
                        | CoworkerTaskStage::Running
                        | CoworkerTaskStage::Held
                );

            let fenced = if task.stage == CoworkerTaskStage::Integrated {
                adm.runtime_state.stop_requested
                    || has_global_unknown_effect(&adm.runtime_state)
                    || has_task_unknown_eff
                    || task.unresolved_outbox_id.is_some()
                    || !task.active_hold_ids.is_empty()
                    || adm.job.holds.iter().any(|h| {
                        h.active
                            && (h.holds_entire_job
                                || h.directly_affected_task_ids.contains(&task.task_id))
                    })
            } else {
                has_fenced_late_derived
                    || fenced_initial_coder
                    || is_task_fenced_in_pipeline(
                        task,
                        &job_for_fence,
                        &adm.runtime_state,
                        &adm.approval.approval_id,
                        &plans_dir,
                    )?
            };
            if fenced && task.stage != CoworkerTaskStage::Integrated {
                push_unique(&mut acc.fenced_task_ids, &task.task_id);
            }

            if task.stage == CoworkerTaskStage::EffectUnknown
                || snap.status == TaskExecutionStatus::Unknown
                || has_task_unknown_eff
            {
                push_unique(&mut acc.unknown_effect_task_ids, &task.task_id);
            }

            let awaiting_correction_execution = task.stage
                == CoworkerTaskStage::CorrectInScopeRequired
                && in_flight_stage != Some(CoworkerPipelineInFlightStageKind::TaskCorrection)
                && !can_task_dispatch_derived_correction(&adm, task, attempt_no);
            if awaiting_correction_execution {
                push_unique(&mut acc.awaiting_correction_task_ids, &task.task_id);
            }

            if matches!(
                task.stage,
                CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Cancelled
            ) {
                push_unique(&mut acc.blocked_or_failed_task_ids, &task.task_id);
            }

            let mut failed_check_ids = self
                .failed_check_tasks
                .get(&key)
                .cloned()
                .unwrap_or_default();
            if failed_check_ids.is_empty() && task.stage == CoworkerTaskStage::ReadyForReview {
                if let Some(detected) = detect_persisted_failed_task_checks(
                    &plans_dir,
                    &adm.runtime_state,
                    &task.task_id,
                    attempt_no,
                )? {
                    failed_check_ids = detected.clone();
                    self.failed_check_tasks.insert(key.clone(), detected);
                }
            }
            if !failed_check_ids.is_empty() {
                push_unique(&mut acc.failed_check_task_ids, &task.task_id);
            }

            let has_derived_state =
                task_has_derived_attempt_or_post_initial_state(task, &adm.runtime_state);
            if !has_derived_state
                && (is_task_coder_execution_active(task, &adm.job, &adm.runtime_state)
                    || matches!(
                        snap.status,
                        TaskExecutionStatus::Queued
                            | TaskExecutionStatus::Claimed
                            | TaskExecutionStatus::Running
                            | TaskExecutionStatus::CancelRequested
                    ))
            {
                active_host_coder_count += 1;
            }

            let art_key = ControllerJobRuntimeState::artifact_key(&task.task_id, attempt_no);
            let artifacts = adm.runtime_state.attempt_artifacts.get(&art_key);
            let latest_cand = task
                .candidates
                .iter()
                .rfind(|c| c.attempt_no == attempt_no)
                .or_else(|| task.candidates.last());

            let candidate_id = latest_cand
                .map(|c| c.candidate_id.clone())
                .or_else(|| artifacts.and_then(|a| a.candidate_id.clone()));
            let review_subject_promoted = artifacts.is_some_and(|a| a.review_subject.is_some());
            let review_verdict_outcome = latest_cand
                .and_then(|c| c.review_verdict.as_ref())
                .map(|v| v.outcome);
            let review_receipt_id = latest_cand
                .and_then(|c| c.review_receipt_id.clone())
                .or_else(|| {
                    artifacts
                        .and_then(|a| a.review_receipt.as_ref())
                        .map(|r| r.receipt_id.clone())
                });
            let integration_receipt_id = latest_cand
                .and_then(|c| c.integration_receipt_id.clone())
                .or_else(|| {
                    artifacts
                        .and_then(|a| a.integration_receipt.as_ref())
                        .map(|r| r.receipt_id.clone())
                });

            let blocking_diagnostic = extract_task_blocking_diagnostic(
                task,
                &adm.job,
                &adm.runtime_state,
                attempt_no,
                &failed_check_ids,
            );

            task_snapshots.push(CoworkerPipelineTaskSnapshot {
                task_id: task.task_id.clone(),
                attempt_no,
                stage: task.stage,
                host_status: snap.status,
                fenced,
                in_flight_stage,
                candidate_id,
                review_subject_promoted,
                review_verdict_outcome,
                review_receipt_id,
                integration_receipt_id,
                awaiting_correction_execution,
                failed_check_ids,
                blocking_diagnostic,
            });
        }

        let in_flight_stages = self.in_flight_stages();
        let completion_outcome = acc
            .completion_outcome
            .as_deref()
            .cloned()
            .or_else(|| self.last_completion_outcome.as_deref().cloned());

        let status = classify_pipeline_status(
            &adm,
            &plans_dir,
            active_host_coder_count,
            &in_flight_stages,
            &task_snapshots,
            &acc,
            completion_outcome.as_ref(),
        )?;

        Ok(CoworkerPipelineTickReport {
            binding_id: self.binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            job_id: self.job_id.clone(),
            approval_id: adm.approval.approval_id,
            job_phase: adm.job.phase,
            job_revision: adm.job.job_revision,
            lease_epoch,
            controller_instance_id,
            status,
            in_flight_stages,
            dispatched_task_ids: acc.dispatched_task_ids,
            reconciled_ready_for_review_task_ids: acc.reconciled_ready_for_review_task_ids,
            verified_task_ids: acc.verified_task_ids,
            reviewed_task_ids: acc.reviewed_task_ids,
            integrated_task_ids: acc.integrated_task_ids,
            prepared_dependency_task_ids: acc.prepared_dependency_task_ids,
            promoted_dependency_ready_task_ids: acc.promoted_dependency_ready_task_ids,
            awaiting_correction_task_ids: acc.awaiting_correction_task_ids,
            failed_check_task_ids: acc.failed_check_task_ids,
            fenced_task_ids: acc.fenced_task_ids,
            unknown_effect_task_ids: acc.unknown_effect_task_ids,
            blocked_or_failed_task_ids: acc.blocked_or_failed_task_ids,
            tasks: task_snapshots,
            completion_outcome,
        })
    }
}

/// Owned pipeline session holding physical OS job ownership ([`OwnedCoworkerControllerSession`])
/// for an independent controller process adapter.
#[derive(Debug)]
pub struct OwnedCoworkerPipelineSession {
    service: CoworkerPipelineService,
    controller_session: OwnedCoworkerControllerSession,
}

impl OwnedCoworkerPipelineSession {
    /// Creates an [`OwnedCoworkerPipelineSession`] from an already-opened
    /// [`OwnedCoworkerControllerSession`].
    pub fn from_controller_session(
        service: CoworkerPipelineService,
        controller_session: OwnedCoworkerControllerSession,
    ) -> Result<Self, NativeError> {
        let key = controller_session.guard().key();
        service.controller.verify_physical_ownership(
            controller_session.guard(),
            &key.binding_id,
            &key.thread_uuid,
            &key.job_id,
        )?;
        Ok(Self {
            service,
            controller_session,
        })
    }

    /// Returns a reference to the held [`CoworkerJobOwnershipGuard`].
    pub fn guard(&self) -> &CoworkerJobOwnershipGuard {
        self.controller_session.guard()
    }

    /// Returns a reference to the underlying [`OwnedCoworkerControllerSession`].
    pub fn controller_session(&self) -> &OwnedCoworkerControllerSession {
        &self.controller_session
    }

    /// Returns a reference to the underlying [`CoworkerPipelineService`].
    pub fn service(&self) -> &CoworkerPipelineService {
        &self.service
    }

    /// Creates a stateful [`CoworkerPipelineDriver`] borrowing this session's held guard so
    /// in-flight stage futures in `FuturesUnordered` are retained across ticks.
    pub fn driver(&self) -> Result<CoworkerPipelineDriver<'_>, NativeError> {
        let key = self.controller_session.guard().key();
        self.service.driver(
            self.controller_session.guard(),
            key.binding_id.clone(),
            key.thread_uuid.clone(),
            key.job_id.clone(),
        )
    }

    /// Drives the owned session until the job settles into a non-`Running` state and all
    /// in-flight stage futures have drained.
    pub async fn drive_until_settled(
        &self,
        initial_now_ms: u64,
    ) -> Result<CoworkerPipelineTickReport, NativeError> {
        let key = self.controller_session.guard().key();
        let req = CoworkerPipelineRequest {
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            now_ms: initial_now_ms,
        };
        Box::pin(
            self.service
                .drive_job(self.controller_session.guard(), &req),
        )
        .await
    }

    /// Explicitly releases any active logical lease held by this session's instance and drops the
    /// physical OS ownership lock.
    pub fn release(self) -> Result<(), NativeError> {
        self.controller_session.release()
    }
}

#[derive(Debug, Default, Clone)]
struct TickAccumulator {
    dispatched_task_ids: Vec<String>,
    reconciled_ready_for_review_task_ids: Vec<String>,
    verified_task_ids: Vec<String>,
    reviewed_task_ids: Vec<String>,
    integrated_task_ids: Vec<String>,
    prepared_dependency_task_ids: Vec<String>,
    promoted_dependency_ready_task_ids: Vec<String>,
    awaiting_correction_task_ids: Vec<String>,
    failed_check_task_ids: Vec<String>,
    fenced_task_ids: Vec<String>,
    unknown_effect_task_ids: Vec<String>,
    blocked_or_failed_task_ids: Vec<String>,
    completion_outcome: Option<Box<CoworkerJobCompletionOutcome>>,
}

impl TickAccumulator {
    fn record_step_report(&mut self, report: &CoworkerControllerStepReport) {
        for id in &report.dispatched_task_ids {
            push_unique(&mut self.dispatched_task_ids, id);
        }
        for id in &report.reconciled_ready_for_review_task_ids {
            push_unique(&mut self.reconciled_ready_for_review_task_ids, id);
        }
        for id in &report.promoted_dependency_ready_task_ids {
            push_unique(&mut self.promoted_dependency_ready_task_ids, id);
        }
        for id in &report.unknown_effect_task_ids {
            push_unique(&mut self.unknown_effect_task_ids, id);
        }
    }

    fn merge_from_report(&mut self, report: &CoworkerPipelineTickReport) {
        for id in &report.dispatched_task_ids {
            push_unique(&mut self.dispatched_task_ids, id);
        }
        for id in &report.reconciled_ready_for_review_task_ids {
            push_unique(&mut self.reconciled_ready_for_review_task_ids, id);
        }
        for id in &report.verified_task_ids {
            push_unique(&mut self.verified_task_ids, id);
        }
        for id in &report.reviewed_task_ids {
            push_unique(&mut self.reviewed_task_ids, id);
        }
        for id in &report.integrated_task_ids {
            push_unique(&mut self.integrated_task_ids, id);
        }
        for id in &report.prepared_dependency_task_ids {
            push_unique(&mut self.prepared_dependency_task_ids, id);
        }
        for id in &report.promoted_dependency_ready_task_ids {
            push_unique(&mut self.promoted_dependency_ready_task_ids, id);
        }
        if let Some(ref outcome) = report.completion_outcome {
            self.completion_outcome = Some(Box::new(outcome.clone()));
        }
    }

    fn into_final_report(
        self,
        mut final_tick: CoworkerPipelineTickReport,
    ) -> CoworkerPipelineTickReport {
        final_tick.dispatched_task_ids = self.dispatched_task_ids;
        final_tick.reconciled_ready_for_review_task_ids = self.reconciled_ready_for_review_task_ids;
        final_tick.verified_task_ids = self.verified_task_ids;
        final_tick.reviewed_task_ids = self.reviewed_task_ids;
        final_tick.integrated_task_ids = self.integrated_task_ids;
        final_tick.prepared_dependency_task_ids = self.prepared_dependency_task_ids;
        final_tick.promoted_dependency_ready_task_ids = self.promoted_dependency_ready_task_ids;
        if let (None, Some(outcome)) = (&final_tick.completion_outcome, self.completion_outcome) {
            final_tick.completion_outcome = Some(*outcome);
        }
        final_tick
    }
}

fn validate_pipeline_request(request: &CoworkerPipelineRequest) -> Result<(), NativeError> {
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
    if request.thread_uuid.trim().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "CoworkerPipelineRequest.thread_uuid must not be empty",
        ));
    }
    if request.now_ms == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "CoworkerPipelineRequest.now_ms must be > 0",
        ));
    }
    Ok(())
}

fn push_unique(vec: &mut Vec<String>, val: &str) {
    if !vec.iter().any(|existing| existing == val) {
        vec.push(val.to_string());
    }
}

fn has_global_unknown_effect(runtime_state: &ControllerJobRuntimeState) -> bool {
    runtime_state.pending_effects.values().any(|eff| {
        eff.task_id.is_none() && eff.status == PendingControllerEffectStatus::EffectUnknown
    })
}

fn verify_no_conflicting_effect_keys(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> Result<(), NativeError> {
    // 1. Check job-level and task-level effects: no slot may have multiple distinct request_keys
    //    (e.g. attempting to bypass an EffectUnknown or ReservedBeforeEffect effect with a new key).
    let mut slot_keys: BTreeMap<String, String> = BTreeMap::new();
    for eff in runtime_state.pending_effects.values() {
        let slot_key = match eff.kind {
            PendingControllerEffectKind::JobAggregation => "job_aggregation".to_string(),
            PendingControllerEffectKind::ClosureInvocation => "closure_invocation".to_string(),
            PendingControllerEffectKind::CheckExecution if eff.task_id.is_none() => {
                let check_id = eff.external_correlation_id.as_deref().unwrap_or("unknown");
                format!("candidate_check:{check_id}")
            }
            PendingControllerEffectKind::TaskCommit
            | PendingControllerEffectKind::CoderDispatch
            | PendingControllerEffectKind::ReviewerInvocation
            | PendingControllerEffectKind::TaskIntegration => {
                let tid = eff.task_id.as_deref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "task-scoped PendingControllerEffect `{}` (`{}`) is missing task_id",
                            eff.effect_id,
                            eff.kind.as_str()
                        ),
                    )
                })?;
                let att = eff.attempt_no.unwrap_or(1);
                format!("task:{}:{tid}:{att}", eff.kind.as_str())
            }
            PendingControllerEffectKind::CheckExecution => {
                let tid = eff.task_id.as_deref().unwrap_or("");
                let att = eff.attempt_no.unwrap_or(1);
                let check_id = eff.external_correlation_id.as_deref().unwrap_or("unknown");
                format!("task_check:{tid}:{att}:{check_id}")
            }
        };
        if let Some(existing_req_key) = slot_keys.get(&slot_key) {
            if existing_req_key != &eff.request_key {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "conflicting PendingControllerEffect request_key `{}` vs `{existing_req_key}` for slot `{slot_key}` on job `{}`; unknown or active effects cannot be bypassed by a new request_key",
                        eff.request_key, job.job_id
                    ),
                ));
            }
        } else {
            slot_keys.insert(slot_key, eff.request_key.clone());
        }
    }

    // 2. Also verify task outbox records against conflicting request_keys for the same
    //    (task_id, attempt_no, kind).
    let mut outbox_keys: BTreeMap<(String, u32, String), String> = BTreeMap::new();
    for obx in &job.outbox {
        if obx.kind == OutboxDispatchKind::DependencyPreparation {
            continue;
        }
        let kind_tag = format!("{:?}", obx.kind);
        let slot = (obx.task_id.clone(), obx.attempt_no, kind_tag.clone());
        if let Some(prev_key) = outbox_keys.get(&slot) {
            if prev_key != &obx.request_key {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "conflicting OutboxDispatchRecord request_key `{}` vs `{prev_key}` for task `{}` attempt `{}` kind `{kind_tag}`",
                        obx.request_key, obx.task_id, obx.attempt_no
                    ),
                ));
            }
        } else {
            outbox_keys.insert(slot, obx.request_key.clone());
        }
    }

    Ok(())
}

fn is_task_fenced_in_pipeline(
    candidate_task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    approval_id: &str,
    plans_dir: &Path,
) -> Result<bool, NativeError> {
    if has_global_unknown_effect(runtime_state) {
        return Ok(true);
    }
    if is_task_fenced_by_hold_or_sibling(
        candidate_task,
        job,
        runtime_state,
        approval_id,
        plans_dir,
    )? {
        return Ok(true);
    }

    // Also check if any sibling task has an EffectUnknown entry in `runtime_state.pending_effects`
    // even if that sibling's `CoworkerTaskStage` was not yet flipped to `EffectUnknown`.
    let sibling_with_unknown_effect = runtime_state.pending_effects.values().any(|eff| {
        eff.status == PendingControllerEffectStatus::EffectUnknown
            && eff
                .task_id
                .as_deref()
                .is_some_and(|tid| tid != candidate_task.task_id)
    });
    if sibling_with_unknown_effect {
        let Some(ref completeness_receipt) =
            runtime_state.envelope.independence_completeness_receipt
        else {
            return Ok(true);
        };
        let change_summary = match ConversationChangeStore::open_readonly(plans_dir)
            .and_then(|s| s.unresolved_blocking_changes(&job.binding_id, Some(approval_id), None))
        {
            Ok(sum) => sum,
            Err(_) => return Ok(true),
        };
        match completeness_receipt.prove_branch_continuation_allowed(
            candidate_task,
            &job.tasks,
            &job.holds,
            &change_summary.blocking_changes,
        ) {
            Ok(proof) => {
                if !proof.independent {
                    return Ok(true);
                }
            }
            Err(_) => return Ok(true),
        }
    }

    Ok(false)
}

fn can_advance_to_job_completion(
    adm: &VerifiedControllerAdmission,
    plans_dir: &Path,
) -> Result<bool, NativeError> {
    if adm.runtime_state.stop_requested {
        return Ok(false);
    }
    if adm.job.holds.iter().any(|h| h.active) {
        return Ok(false);
    }
    if adm.runtime_state.pending_effects.values().any(|eff| {
        eff.status == PendingControllerEffectStatus::EffectUnknown
            || (eff.task_id.is_some()
                && eff.status == PendingControllerEffectStatus::ReservedBeforeEffect)
    }) {
        return Ok(false);
    }
    let change_summary = ConversationChangeStore::open_readonly(plans_dir)
        .and_then(|store| {
            store.unresolved_blocking_changes(
                &adm.job.binding_id,
                Some(&adm.approval.approval_id),
                None,
            )
        })
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to read ConversationChangeStore: {err}"),
            )
        })?;
    if change_summary.approval_superseded || !change_summary.blocking_changes.is_empty() {
        return Ok(false);
    }
    Ok(true)
}

fn collect_integrated_predecessor_selections(
    task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
) -> Option<Vec<DependencyPredecessorSelection>> {
    let mut selections = Vec::with_capacity(task.depends_on.len());
    for pred_id in &task.depends_on {
        let pred_task = job.tasks.iter().find(|t| &t.task_id == pred_id)?;
        if pred_task.stage != CoworkerTaskStage::Integrated {
            return None;
        }
        let acc_cand_id = pred_task.accepted_candidate_id.as_ref()?;
        let cand = pred_task
            .candidates
            .iter()
            .find(|c| &c.candidate_id == acc_cand_id)?;
        let verdict = cand.review_verdict.as_ref()?;
        if !verdict.allows_acceptance() {
            return None;
        }
        let review_receipt_id = cand.review_receipt_id.clone()?;
        let integration_receipt_id = cand.integration_receipt_id.clone()?;
        selections.push(DependencyPredecessorSelection {
            task_id: pred_task.task_id.clone(),
            review_receipt_id,
            integration_receipt_id,
        });
    }
    Some(selections)
}

fn detect_persisted_failed_task_checks(
    plans_dir: &Path,
    runtime_state: &ControllerJobRuntimeState,
    task_id: &str,
    attempt_no: u32,
) -> Result<Option<Vec<String>>, NativeError> {
    let art_key = ControllerJobRuntimeState::artifact_key(task_id, attempt_no);
    let Some(artifacts) = runtime_state.attempt_artifacts.get(&art_key) else {
        return Ok(None);
    };
    if artifacts.review_subject.is_some() {
        return Ok(None);
    }
    let Some(ref committed_head) = artifacts.committed_head_commit else {
        return Ok(None);
    };
    let Some(ref committed_tree) = artifacts.committed_tree_sha else {
        return Ok(None);
    };

    let mut failed_ids = Vec::new();
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::CheckExecution
            && eff.task_id.as_deref() == Some(task_id)
            && eff.attempt_no == Some(attempt_no)
            && eff.status == PendingControllerEffectStatus::EffectFailedTerminal
        {
            if let Some(report) = read_persisted_check_report(plans_dir, &eff.effect_id)? {
                if constant_time_str_eq(&report.subject.expected_head_commit, committed_head)
                    && constant_time_str_eq(&report.subject.expected_tree_sha, committed_tree)
                    && !report.passed()
                {
                    push_unique(&mut failed_ids, &report.check_id);
                }
            }
        }
    }

    if failed_ids.is_empty() {
        Ok(None)
    } else {
        Ok(Some(failed_ids))
    }
}

fn has_unreconcilable_task_verification_unknown_effect(
    plans_dir: &Path,
    sessions_dir: &Path,
    adm: &VerifiedControllerAdmission,
    task_id: &str,
    attempt_no: u32,
) -> Result<bool, NativeError> {
    let art_key = ControllerJobRuntimeState::artifact_key(task_id, attempt_no);
    let artifacts_opt = adm.runtime_state.attempt_artifacts.get(&art_key);
    let work_area_root = sessions_dir
        .join(&adm.approval.approval_id)
        .join(WORK_AREA_SUBDIR);

    for eff in adm.runtime_state.pending_effects.values() {
        if eff.task_id.as_deref() != Some(task_id) || eff.attempt_no != Some(attempt_no) {
            continue;
        }
        if eff.status != PendingControllerEffectStatus::EffectUnknown {
            continue;
        }
        match eff.kind {
            PendingControllerEffectKind::TaskCommit => {
                let Some(artifacts) = artifacts_opt else {
                    return Ok(true);
                };
                let Ok(manifest) =
                    load_and_verify_manifest(&work_area_root, &artifacts.task_worktree_id)
                else {
                    return Ok(true);
                };
                let Ok(current_head) = resolve_exact_commit_sha(
                    &manifest.checkout_path,
                    "HEAD",
                    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
                ) else {
                    return Ok(true);
                };
                let Ok(clean) =
                    is_worktree_clean(&manifest.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                else {
                    return Ok(true);
                };
                if !clean || current_head == manifest.base_commit {
                    return Ok(true);
                }
            }
            PendingControllerEffectKind::CheckExecution => {
                if read_persisted_check_report(plans_dir, &eff.effect_id)?.is_none() {
                    return Ok(true);
                }
            }
            _ => return Ok(true),
        }
    }

    Ok(false)
}

fn can_reconcile_effect_unknown_review(
    plans_dir: &Path,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    task_id: &str,
    attempt_no: u32,
) -> Result<bool, NativeError> {
    let has_review_outbox = job.outbox.iter().any(|o| {
        o.task_id == task_id
            && o.attempt_no == attempt_no
            && o.kind == OutboxDispatchKind::TaskReviewDispatch
            && matches!(
                o.status,
                OutboxDispatchStatus::ReservedBeforeEffect | OutboxDispatchStatus::EffectUnknown
            )
    });
    if !has_review_outbox {
        return Ok(false);
    }
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::ReviewerInvocation
            && eff.task_id.as_deref() == Some(task_id)
            && eff.attempt_no == Some(attempt_no)
            && read_persisted_task_review_report(plans_dir, &eff.effect_id)?.is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn can_reconcile_effect_unknown_integration(
    sessions_dir: &Path,
    adm: &VerifiedControllerAdmission,
    task: &CoworkerTaskRecord,
    attempt_no: u32,
) -> Result<bool, NativeError> {
    let has_int_outbox = adm.job.outbox.iter().any(|o| {
        o.task_id == task.task_id
            && o.attempt_no == attempt_no
            && o.kind == OutboxDispatchKind::TaskIntegration
            && matches!(
                o.status,
                OutboxDispatchStatus::ReservedBeforeEffect | OutboxDispatchStatus::EffectUnknown
            )
    });
    if !has_int_outbox {
        return Ok(false);
    }
    let work_area_root = sessions_dir
        .join(&adm.approval.approval_id)
        .join(WORK_AREA_SUBDIR);
    for eff in adm.runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::TaskIntegration
            && eff.task_id.as_deref() == Some(task.task_id.as_str())
            && eff.attempt_no == Some(attempt_no)
        {
            if let Some(ref receipt_id) = eff.external_correlation_id {
                if load_integration_receipt(&work_area_root, receipt_id).is_ok() {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn can_task_dispatch_derived_correction(
    adm: &VerifiedControllerAdmission,
    task: &CoworkerTaskRecord,
    prior_attempt_no: u32,
) -> bool {
    if task.stage != CoworkerTaskStage::CorrectInScopeRequired {
        return false;
    }
    let Some(ref cap_env) = adm.linkage.controller_envelope else {
        return false;
    };
    if cap_env.max_in_scope_correction_attempts == 0
        || adm.runtime_state.envelope.max_in_scope_corrections_per_task == 0
        || adm.runtime_state.envelope.max_total_corrections_per_job == 0
    {
        return false;
    }
    let Some(next_attempt_no) = prior_attempt_no.checked_add(1) else {
        return false;
    };
    if prior_attempt_no == 0
        || prior_attempt_no > MAX_IN_SCOPE_CORRECTION_ATTEMPTS
        || usize::try_from(next_attempt_no)
            .map(|n| n > MAX_ATTEMPTS_PER_TASK)
            .unwrap_or(true)
        || prior_attempt_no > cap_env.max_in_scope_correction_attempts
        || prior_attempt_no > adm.runtime_state.envelope.max_in_scope_corrections_per_task
        || !adm.linkage.authorizes_in_scope_correction(next_attempt_no)
    {
        return false;
    }

    let Some(prior_cand) = task
        .candidates
        .iter()
        .find(|c| c.attempt_no == prior_attempt_no)
    else {
        return false;
    };
    let Some(ref verdict) = prior_cand.review_verdict else {
        return false;
    };
    if verdict.outcome != ReviewVerdictOutcome::CorrectInScope
        || verdict.required_in_scope_fixes.is_empty()
        || prior_cand.review_receipt_id.is_some()
        || prior_cand.integration_receipt_id.is_some()
        || task.accepted_candidate_id.is_some()
    {
        return false;
    }
    let art_key = ControllerJobRuntimeState::artifact_key(&task.task_id, prior_attempt_no);
    let Some(prior_artifacts) = adm.runtime_state.attempt_artifacts.get(&art_key) else {
        return false;
    };
    if prior_artifacts.review_subject.is_none()
        || prior_artifacts.reviewer_response_digest.is_none()
        || prior_artifacts.review_receipt.is_some()
        || prior_artifacts.integration_receipt.is_some()
    {
        return false;
    }

    let already_reserved = adm
        .runtime_state
        .derived_attempts
        .iter()
        .any(|d| d.task_id == task.task_id && d.attempt_no == next_attempt_no);
    if !already_reserved {
        let task_corrections_reserved = adm
            .runtime_state
            .derived_attempts
            .iter()
            .filter(|d| d.task_id == task.task_id && d.attempt_no >= 2)
            .count() as u32;
        if task_corrections_reserved >= MAX_IN_SCOPE_CORRECTION_ATTEMPTS
            || task_corrections_reserved >= cap_env.max_in_scope_correction_attempts
            || task_corrections_reserved
                >= adm.runtime_state.envelope.max_in_scope_corrections_per_task
        {
            return false;
        }
        if adm.runtime_state.total_corrections_reserved
            >= adm.runtime_state.envelope.max_total_corrections_per_job
        {
            return false;
        }
        if adm.runtime_state.total_task_attempts_reserved
            >= adm.runtime_state.envelope.max_total_task_attempts_per_job
        {
            return false;
        }
        if adm
            .runtime_state
            .ensure_role_budget_available(WorkflowRole::Coder)
            .is_err()
        {
            return false;
        }
    }

    true
}

fn has_fenced_late_derived_report(
    plans_dir: &Path,
    runtime_state: &ControllerJobRuntimeState,
    task_id: &str,
    attempt_no: u32,
) -> Result<bool, NativeError> {
    if attempt_no < 2 {
        return Ok(false);
    }
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::CoderDispatch
            && eff.task_id.as_deref() == Some(task_id)
            && eff.attempt_no == Some(attempt_no)
        {
            if let Some(report) =
                read_persisted_derived_task_execution_report(plans_dir, &eff.effect_id)?
            {
                if report.status == CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn extract_task_blocking_diagnostic(
    task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    attempt_no: u32,
    failed_check_ids: &[String],
) -> Option<String> {
    if !failed_check_ids.is_empty() {
        return Some(format!(
            "required frozen task check(s) failed: {}",
            failed_check_ids.join(", ")
        ));
    }
    if let Some(cand) = task.candidates.iter().rfind(|c| c.attempt_no == attempt_no) {
        if let Some(ref verdict) = cand.review_verdict {
            if !verdict.allows_acceptance() {
                return verdict
                    .prose_summary
                    .clone()
                    .or_else(|| Some(format!("Reviewer outcome `{}`", verdict.outcome.as_str())));
            }
        }
    }
    for eff in runtime_state.pending_effects.values() {
        if eff.task_id.as_deref() == Some(task.task_id.as_str())
            && eff.attempt_no.unwrap_or(attempt_no) == attempt_no
            && eff.diagnostic.is_some()
        {
            return eff.diagnostic.clone();
        }
    }
    for obx in job.outbox.iter().rev() {
        if obx.task_id == task.task_id && obx.attempt_no == attempt_no && obx.diagnostic.is_some() {
            return obx.diagnostic.clone();
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn classify_pipeline_status(
    adm: &VerifiedControllerAdmission,
    plans_dir: &Path,
    active_host_coder_count: usize,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
    tasks: &[CoworkerPipelineTaskSnapshot],
    acc: &TickAccumulator,
    completion_outcome: Option<&CoworkerJobCompletionOutcome>,
) -> Result<CoworkerPipelineStatus, NativeError> {
    if adm.runtime_state.stop_requested {
        return Ok(CoworkerPipelineStatus::Stopped);
    }

    if adm.job.phase == CoworkerJobPhase::Completed {
        return Ok(CoworkerPipelineStatus::Completed);
    }

    if let Some(comp) = completion_outcome {
        match comp.status {
            CoworkerJobCompletionStatus::Completed => {
                return Ok(CoworkerPipelineStatus::Completed);
            }
            CoworkerJobCompletionStatus::BlockedByFailedChecks => {
                return Ok(CoworkerPipelineStatus::BlockedByFailedChecks);
            }
            CoworkerJobCompletionStatus::ClosureNotAccepted => {
                return Ok(CoworkerPipelineStatus::BlockedOrFailed);
            }
            CoworkerJobCompletionStatus::AggregatedReadyForCandidateChecks => {}
        }
    }

    if has_global_unknown_effect(&adm.runtime_state) {
        return Ok(CoworkerPipelineStatus::BlockedOnUnknownEffect);
    }

    // Check if any task is still actively running or in-flight.
    if active_host_coder_count > 0 || !in_flight_stages.is_empty() {
        return Ok(CoworkerPipelineStatus::Running);
    }

    let active_in_flight_coders = count_active_coder_executions(&adm.job, &adm.runtime_state);

    let any_task_can_advance = tasks.iter().any(|snap| {
        if snap.fenced || acc.unknown_effect_task_ids.contains(&snap.task_id) {
            return false;
        }
        match snap.stage {
            CoworkerTaskStage::ReadyToDispatch => {
                active_in_flight_coders < adm.approval.max_parallel
            }
            CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running => true,
            CoworkerTaskStage::ReadyForReview => snap.failed_check_ids.is_empty(),
            CoworkerTaskStage::ReviewReserved
            | CoworkerTaskStage::UnderReview
            | CoworkerTaskStage::Accepted
            | CoworkerTaskStage::IntegrationReserved => true,
            CoworkerTaskStage::CorrectInScopeRequired => {
                if let Some(job_task) = adm.job.tasks.iter().find(|t| t.task_id == snap.task_id) {
                    active_in_flight_coders < adm.approval.max_parallel
                        && can_task_dispatch_derived_correction(adm, job_task, snap.attempt_no)
                } else {
                    false
                }
            }
            CoworkerTaskStage::PendingDependencies => {
                if let Some(job_task) = adm.job.tasks.iter().find(|t| t.task_id == snap.task_id) {
                    collect_integrated_predecessor_selections(job_task, &adm.job).is_some()
                } else {
                    false
                }
            }
            _ => false,
        }
    });

    if any_task_can_advance {
        return Ok(CoworkerPipelineStatus::Running);
    }

    if !adm.job.tasks.is_empty()
        && adm
            .job
            .tasks
            .iter()
            .all(|t| t.stage == CoworkerTaskStage::Integrated)
        && can_advance_to_job_completion(adm, plans_dir)?
        && completion_outcome.is_none()
    {
        return Ok(CoworkerPipelineStatus::Running);
    }

    if !acc.unknown_effect_task_ids.is_empty()
        || adm
            .runtime_state
            .pending_effects
            .values()
            .any(|e| e.status == PendingControllerEffectStatus::EffectUnknown)
    {
        return Ok(CoworkerPipelineStatus::BlockedOnUnknownEffect);
    }

    if !acc.awaiting_correction_task_ids.is_empty()
        || tasks.iter().any(|t| t.awaiting_correction_execution)
    {
        return Ok(CoworkerPipelineStatus::AwaitingCorrectionExecution);
    }

    if !acc.failed_check_task_ids.is_empty() || tasks.iter().any(|t| !t.failed_check_ids.is_empty())
    {
        return Ok(CoworkerPipelineStatus::BlockedByFailedChecks);
    }

    if adm.job.holds.iter().any(|h| h.active)
        || tasks.iter().any(|t| t.stage == CoworkerTaskStage::Held)
        || (!acc.fenced_task_ids.is_empty() && acc.blocked_or_failed_task_ids.is_empty())
    {
        return Ok(CoworkerPipelineStatus::Held);
    }

    if !acc.blocked_or_failed_task_ids.is_empty()
        || tasks.iter().any(|t| {
            matches!(
                t.stage,
                CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Cancelled
            )
        })
    {
        return Ok(CoworkerPipelineStatus::BlockedOrFailed);
    }

    if !acc.fenced_task_ids.is_empty() {
        return Ok(CoworkerPipelineStatus::Held);
    }

    Ok(CoworkerPipelineStatus::Running)
}

fn map_pipeline_host_error(err: HostError) -> NativeError {
    let code = match &err {
        HostError::ScopeMismatch { .. } | HostError::SecurityViolation { .. } => {
            NativeErrorCode::SecurityViolation
        }
        HostError::NativeRouteRequired { .. } => NativeErrorCode::UnsupportedCapability,
        HostError::MutationOutcomeUncertain { .. } | HostError::StopOutcomeUncertain { .. } => {
            NativeErrorCode::AmbiguousWakeupPendingReconciliation
        }
        _ => NativeErrorCode::ValidationFailed,
    };
    NativeError::new(code, sanitize_diagnostic_text(&err.to_string()))
}
