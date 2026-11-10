//! Automatic Coworker workflow controller (Phase 1: admission, physical ownership, original
//! `Coder` host task reconciliation, dependency-seal gating, and bounded parallel dispatch).
//!
//! ## Phase 1 Scope and Truthfulness Invariants
//!
//! - **Admission requires planner provenance**: Automatic controller admission requires an
//!   existing [`PlanningResultRef`] on the [`CoworkerJobRecord`] bound to the exact current
//!   [`WorkBrief`] digest, [`CanonicalProjectBaseRef`], published plan revision/digest,
//!   [`WorkflowProfile`], registry revision, and a completed `WorkflowRole::Orchestrator`
//!   reservation + verified [`PersistedPlanningCallArtifact`]. Manual/legacy jobs without
//!   `planning_result` remain compatible with [`CoworkerWorkflowStore::bind_job_approval`] for
//!   manual operation, but are refused by automatic controller admission before any effect.
//! - **Physical OS ownership**: Every controller admission, reconciliation, and dispatch step
//!   requires a live, non-clonable [`CoworkerJobOwnershipGuard`] backed by an active OS file
//!   descriptor lock (`flock`). The guard is re-verified before every effect and immediately after
//!   every `.await` point. Synchronous store locks are never held across `.await`.
//! - **Intent-before-effect and frozen command keys**: Initial `WorkflowRole::Coder` task attempts
//!   (`attempt_no == 1`) reserve their role call cap, job attempt cap, and task outbox record
//!   keyed by the immutable `command_key` from [`ApprovalRecord`] before invoking
//!   [`ApprovedHostClient::dispatch_task`]. Repeated ticks or controller reopens reconcile existing
//!   host receipts without re-charging budgets or re-launching workers.
//! - **Dependency gating**: Dependent tasks (`!depends_on.is_empty()`) remain in
//!   [`CoworkerTaskStage::PendingDependencies`] until every predecessor has reached
//!   [`CoworkerTaskStage::Accepted`] or [`CoworkerTaskStage::Integrated`] with a sealed review
//!   receipt and integration receipt **and** the host reports a verified
//!   [`TaskExecutionStatus::DependencyInputsReady`] seal. Merely completed (`ReadyForReview`)
//!   predecessor workers or prose never unblock dependents.
//! - **Truthful Phase 1 boundary**: Completed `Coder` tasks reconcile to
//!   [`CoworkerTaskStage::ReadyForReview`] and report
//!   [`CoworkerControllerPhase1Status::AwaitingChecksOrReview`]. Phase 1 does not execute
//!   verification check commands, dispatch `Reviewer` or `Closure` workers, synthesize in-scope
//!   correction attempts, or run cross-task Git integration.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use threadlane_authority::{
    build_approved_worker_profile_config, ApprovalRecord, ApprovedTaskRecord, AuthorityStore,
};
use threadlane_host::{
    ApprovedHostClient, HostError, TaskExecutionStatus, TaskStateSnapshot, WORK_AREA_SUBDIR,
};
use threadlane_operations::inspect_dependency_input_seal_readonly;
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::WorkflowRole;

use crate::conversation_change::ConversationChangeStore;
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, sanitize_diagnostic_text, validate_bounded_identifier,
};
use crate::profile_flow::{RoleExecutionRoute, SelectedRoleChoice};

use super::contracts::{
    deterministic_prefixed_id, prove_task_independence, sha256_hex,
    validate_non_native_worker_choice, AcceptedPredecessorReceiptRef, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage, FrozenApprovalLinkage,
    OutboxDispatchKind, OutboxDispatchStatus, PlanningReservationStatus, PlanningResultRef,
    MAX_COWORKER_ID_LEN,
};
use super::controller_policy::{
    deterministic_controller_instance_id, verify_approval_and_plan_evidence,
    verify_frozen_runtime_registry_or_apply_config_hold, ControllerJobRuntimeState,
    ControllerPolicyStore, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, TaskAttemptExecutionArtifacts, DEFAULT_CONTROLLER_LEASE_TTL_MS,
};
use super::ownership::{
    CoworkerJobOwnershipGuard, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore,
    CoworkerOwnershipAcquireOutcome,
};
use super::planning::{
    CoworkerPlanningService, PlanningArtifactOutcomeKind, StructuredOrchestratorOutput,
};
use super::store::{
    CoworkerBindingLedger, CoworkerWorkflowStore, FinalizeTaskOutboxRequest,
    ReserveTaskOutboxRequest,
};

/// High-level truthful Phase 1 status reported by [`CoworkerControllerService::step_job`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerControllerPhase1Status {
    /// One or more original `Coder` tasks are actively queued, claimed, or running on the host.
    RunningTasks,
    /// Reachable `Coder` tasks have finished worker execution (`ReadyForReview`) and are waiting
    /// for verification checks, `Reviewer` evaluation, or acceptance/integration.
    AwaitingChecksOrReview,
    /// Remaining tasks have unfulfilled dependencies (`PendingDependencies`) waiting for upstream
    /// task acceptance, integration, and a verified host dependency input seal.
    AwaitingDependencyAcceptanceOrSeal,
    /// Every task in the job has reached `Accepted` or `Integrated`.
    AllTasksIntegrated,
    /// Scheduling or output promotion is fenced by an active [` super::contracts::WorkflowHoldRecord`]
    /// or impacted sibling without a complete attested [`super::controller_policy::DeclaredIndependenceCompletenessReceipt`].
    Held,
    /// One or more tasks or controller effects are in `EffectUnknown` pending receipt reconciliation.
    BlockedOnUnknownEffect,
    /// `stop_requested` is set on the job's controller runtime state; new dispatches and promotions are fenced.
    Stopped,
    /// One or more tasks reached a terminal `Failed`, `Cancelled`, or `Blocked` stage.
    BlockedOrFailed,
}

/// Request to execute a single bounded Phase 1 controller reconciliation and scheduling step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerControllerStepRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Current wall-clock timestamp (ms since Unix epoch).
    pub now_ms: u64,
    /// Optional logical lease TTL override (defaults to [`DEFAULT_CONTROLLER_LEASE_TTL_MS`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ttl_ms: Option<u64>,
}

/// Per-task status and genuine host receipt telemetry observed during a Phase 1 controller step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerControllerTaskObservation {
    /// Task identifier (`task_id`).
    pub task_id: String,
    /// Current durable [`CoworkerTaskStage`] in [`CoworkerWorkflowStore`].
    pub stage: CoworkerTaskStage,
    /// Live host [`TaskExecutionStatus`] from [`ApprovedHostClient::get_task`].
    pub host_status: TaskExecutionStatus,
    /// Frozen idempotent `command_key` from the [`ApprovalRecord`].
    pub command_key: String,
    /// Genuine runtime `run_id` from the sealed host receipt, if started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Genuine runtime `worker_id` from the sealed host receipt, if started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Genuine `task_worktree_id` reported by the host worker snapshot, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_worktree_id: Option<String>,
    /// Provider telemetry model observed on the host worker snapshot, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Provider telemetry effort observed on the host worker snapshot, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// Sticky software-supervision attention from the genuine host worker snapshot, when
    /// available. This is advisory progress evidence, never an outcome or model metric.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog_attention: Option<threadlane_core::WorkerProgressWatchdogSnapshot>,
    /// Genuine original host diagnostic history, explicitly separate from the current derived attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_worker_provider_health:
        Option<crate::provider_health::OriginalWorkerProviderHealth>,
    /// Whether this task was fenced by a hold, stop request, superseding brief, or sibling impact.
    pub fenced: bool,
}

/// Truthful outcome report returned by [`CoworkerControllerService::step_job`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerControllerStepReport {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target [`CoworkerJobRecord::job_id`].
    pub job_id: String,
    /// Pinned [`ApprovalRecord::approval_id`].
    pub approval_id: String,
    /// Current [`CoworkerJobRecord::job_revision`] after the step.
    pub job_revision: u64,
    /// Current [`ControllerJobRuntimeState::lease_epoch`] after the step.
    pub lease_epoch: u64,
    /// Deterministic logical controller instance identifier (`ctl_<32hex>`).
    pub controller_instance_id: String,
    /// Truthful high-level Phase 1 status.
    pub status: CoworkerControllerPhase1Status,
    /// Frozen `max_parallel` limit from the [`ApprovalRecord`].
    pub max_parallel: usize,
    /// Number of currently active/in-flight tasks on the host after this step.
    pub active_running_count: usize,
    /// Task IDs newly dispatched to [`ApprovedHostClient::dispatch_task`] during this step.
    pub dispatched_task_ids: Vec<String>,
    /// Task IDs reconciled into [`CoworkerTaskStage::ReadyForReview`] during this step.
    pub reconciled_ready_for_review_task_ids: Vec<String>,
    /// Dependent task IDs promoted from [`CoworkerTaskStage::PendingDependencies`] to
    /// [`CoworkerTaskStage::ReadyToDispatch`] after verifying accepted predecessor receipts and
    /// the host's [`TaskExecutionStatus::DependencyInputsReady`] seal.
    pub promoted_dependency_ready_task_ids: Vec<String>,
    /// Task IDs fenced from dispatch or output promotion during this step.
    pub fenced_task_ids: Vec<String>,
    /// Task IDs currently blocked in `EffectUnknown`.
    pub unknown_effect_task_ids: Vec<String>,
    /// Ordered per-task observations.
    pub tasks: Vec<CoworkerControllerTaskObservation>,
}

/// Verified admission bundle produced by [`CoworkerControllerService::verify_job_admission`].
#[derive(Debug, Clone)]
pub struct VerifiedControllerAdmission {
    /// Current validated [`CoworkerJobRecord`].
    pub job: CoworkerJobRecord,
    /// Verified Orchestrator [`PlanningResultRef`] linked to `job`.
    pub planning_result: PlanningResultRef,
    /// Frozen approval linkage on `job`.
    pub linkage: FrozenApprovalLinkage,
    /// Live [`ApprovalRecord`] loaded from [`AuthorityStore`].
    pub approval: ApprovalRecord,
    /// Current [`ControllerJobRuntimeState`] loaded from [`ControllerPolicyStore`].
    pub runtime_state: ControllerJobRuntimeState,
}

/// Automatic Coworker workflow controller service (Phase 1).
#[derive(Clone)]
pub struct CoworkerControllerService {
    workflow_store: CoworkerWorkflowStore,
    policy_store: ControllerPolicyStore,
    ownership_store: CoworkerJobOwnershipStore,
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    planning_artifacts_dir: Option<PathBuf>,
    step_mutex: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for CoworkerControllerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoworkerControllerService")
            .field("workflow_store", &self.workflow_store)
            .field("policy_store", &self.policy_store)
            .field("ownership_store", &self.ownership_store)
            .finish()
    }
}

/// Owned controller session that holds the non-clonable physical [`CoworkerJobOwnershipGuard`]
/// across its lifetime, steps, and `.await` points.
#[derive(Debug)]
pub struct OwnedCoworkerControllerSession {
    service: CoworkerControllerService,
    guard: CoworkerJobOwnershipGuard,
}

impl OwnedCoworkerControllerSession {
    /// Returns a reference to the held physical [`CoworkerJobOwnershipGuard`].
    pub fn guard(&self) -> &CoworkerJobOwnershipGuard {
        &self.guard
    }

    /// Verifies live physical ownership and full automatic controller admission for the owned job.
    pub fn verify_admission(&self) -> Result<VerifiedControllerAdmission, NativeError> {
        let key = self.guard.key();
        self.service.verify_job_admission(
            &self.guard,
            &key.binding_id,
            &key.thread_uuid,
            &key.job_id,
        )
    }

    /// Executes a single Phase 1 reconciliation and scheduling step while holding physical OS ownership.
    pub async fn step(&self, now_ms: u64) -> Result<CoworkerControllerStepReport, NativeError> {
        let key = self.guard.key();
        self.service
            .step_job(
                &self.guard,
                &CoworkerControllerStepRequest {
                    binding_id: key.binding_id.clone(),
                    thread_uuid: key.thread_uuid.clone(),
                    job_id: key.job_id.clone(),
                    now_ms,
                    lease_ttl_ms: None,
                },
            )
            .await
    }

    /// Explicitly releases any active logical lease held by this session's instance and drops the
    /// physical OS ownership lock.
    pub fn release(self) -> Result<(), NativeError> {
        let key = self.guard.key().clone();
        self.service.verify_physical_ownership(
            &self.guard,
            &key.binding_id,
            &key.thread_uuid,
            &key.job_id,
        )?;
        let own_instance_id = deterministic_controller_instance_id(
            &key.binding_id,
            &key.job_id,
            self.guard.instance_id(),
        );
        if let Some(runtime_state) = self
            .service
            .policy_store
            .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
        {
            if let Some(ref lease) = runtime_state.lease {
                if lease.active && lease.instance_id == own_instance_id {
                    self.service.verify_physical_ownership(
                        &self.guard,
                        &key.binding_id,
                        &key.thread_uuid,
                        &key.job_id,
                    )?;
                    self.service.policy_store.release_lease(
                        &key.binding_id,
                        &key.job_id,
                        &lease.instance_id,
                        lease.lease_epoch,
                    )?;
                }
            }
        }
        drop(self.guard);
        Ok(())
    }
}

impl CoworkerControllerService {
    /// Constructs a [`CoworkerControllerService`] from opened workflow, policy, ownership, and
    /// authority stores plus the host `sessions_dir`.
    pub fn new(
        workflow_store: CoworkerWorkflowStore,
        policy_store: ControllerPolicyStore,
        ownership_store: CoworkerJobOwnershipStore,
        authority: Arc<AuthorityStore>,
        sessions_dir: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        let sessions_path = sessions_dir.as_ref();
        if !sessions_path.is_absolute() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller sessions_dir must be an absolute path",
            ));
        }
        Ok(Self {
            workflow_store,
            policy_store,
            ownership_store,
            authority,
            sessions_dir: sessions_path.to_path_buf(),
            planning_artifacts_dir: None,
            step_mutex: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Attaches a private planning report artifacts directory so admission also verifies the
    /// persisted [`super::planning::PersistedPlanningCallArtifact`] on disk.
    pub fn with_planning_artifacts_dir(
        mut self,
        planning_artifacts_dir: impl AsRef<Path>,
    ) -> Result<Self, NativeError> {
        let p = planning_artifacts_dir.as_ref();
        if !p.is_absolute() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "planning_artifacts_dir must be an absolute path",
            ));
        }
        self.planning_artifacts_dir = Some(p.to_path_buf());
        Ok(self)
    }

    /// Returns a reference to the underlying [`CoworkerWorkflowStore`].
    pub fn workflow_store(&self) -> &CoworkerWorkflowStore {
        &self.workflow_store
    }

    /// Returns a reference to the underlying [`ControllerPolicyStore`].
    pub fn policy_store(&self) -> &ControllerPolicyStore {
        &self.policy_store
    }

    /// Returns a reference to the underlying [`CoworkerJobOwnershipStore`].
    pub fn ownership_store(&self) -> &CoworkerJobOwnershipStore {
        &self.ownership_store
    }

    /// Returns a reference to the underlying [`AuthorityStore`].
    pub fn authority(&self) -> &Arc<AuthorityStore> {
        &self.authority
    }

    /// Returns the configured host sessions root directory.
    pub fn sessions_dir(&self) -> &Path {
        &self.sessions_dir
    }

    /// Returns the shared short-lived controller dispatch/reservation serialization mutex.
    pub(crate) fn step_mutex(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.step_mutex
    }

    /// Acquires physical OS job ownership, verifies automatic controller admission, and returns an
    /// [`OwnedCoworkerControllerSession`] holding the live [`CoworkerJobOwnershipGuard`].
    pub fn open_owned_session(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        now_ms: u64,
    ) -> Result<OwnedCoworkerControllerSession, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        let guard = match self.ownership_store.try_acquire_job(&key, now_ms)? {
            CoworkerOwnershipAcquireOutcome::Acquired(guard) => guard,
            CoworkerOwnershipAcquireOutcome::Busy { observed_holder } => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "job `{job_id}` in binding `{binding_id}` is already physically owned by another live controller instance: {observed_holder:?}"
                    ),
                ));
            }
        };
        self.verify_job_admission(&guard, binding_id, thread_uuid, job_id)?;
        self.reconcile_stale_logical_lease_under_physical_guard(
            &guard,
            binding_id,
            thread_uuid,
            job_id,
        )?;
        Ok(OwnedCoworkerControllerSession {
            service: self.clone(),
            guard,
        })
    }

    pub(crate) fn reconcile_stale_logical_lease_under_physical_guard(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<(), NativeError> {
        self.verify_physical_ownership(guard, binding_id, thread_uuid, job_id)?;
        let own_instance_id =
            deterministic_controller_instance_id(binding_id, job_id, guard.instance_id());
        if let Some(runtime_state) = self
            .policy_store
            .get_job_runtime_readonly(binding_id, job_id)?
        {
            if let Some(ref prior_lease) = runtime_state.lease {
                if prior_lease.active && prior_lease.instance_id != own_instance_id {
                    self.verify_physical_ownership(guard, binding_id, thread_uuid, job_id)?;
                    self.policy_store.release_lease(
                        binding_id,
                        job_id,
                        &prior_lease.instance_id,
                        prior_lease.lease_epoch,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Verifies that `guard` is a live physical OS lock held by this process for the exact
    /// `(binding_id, thread_uuid, job_id)` scope.
    pub fn verify_physical_ownership(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<CoworkerJobOwnershipKey, NativeError> {
        let expected_key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        self.ownership_store
            .verify_live_guard(guard, &expected_key)?;
        Ok(expected_key)
    }

    /// Verifies complete automatic controller admission for `(binding_id, thread_uuid, job_id)`.
    ///
    /// Enforces:
    /// 1. Live physical OS ownership guard for `(binding_id, thread_uuid, job_id)`.
    /// 2. Full binding ledger validation (`ledger.validate(binding_id)`) and non-superseded job state.
    /// 3. Non-empty [`PlanningResultRef`] on the job matching the current [`WorkBrief`] digest,
    ///    [`CanonicalProjectBaseRef`], published plan revision/digest, profile, registry revision,
    ///    completed `WorkflowRole::Orchestrator` reservation, and persisted planning report artifact.
    ///    Unlinked manual jobs (`planning_result == None`) are rejected before any effect.
    /// 4. Operator-attested [`FrozenControllerExecutionEnvelope`] in [`ControllerPolicyStore`]
    ///    matching the job's [`FrozenApprovalLinkage`] and live [`ApprovalRecord`].
    /// 5. Approved task worker budgets (`timeout_ms`, `max_stdout_bytes`, `max_stderr_bytes`)
    ///    within the frozen `WorkflowRole::Coder` role budget caps (never silently clamped).
    /// 6. Every task is a non-native headless `WorkflowRole::Coder` task matching its frozen
    ///    [`SelectedRoleChoice`] and [`ApprovedTaskRecord`].
    #[inline(never)]
    pub fn verify_job_admission(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<VerifiedControllerAdmission, NativeError> {
        self.verify_physical_ownership(guard, binding_id, thread_uuid, job_id)?;

        let (mut job, planning_result) =
            self.load_and_verify_admitted_job_from_ledger(binding_id, thread_uuid, job_id)?;

        let linkage = job
            .approval_linkage
            .as_ref()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("job `{}` has no bound FrozenApprovalLinkage", job.job_id),
                )
            })?
            .clone();
        linkage.validate()?;

        if linkage.controller_envelope.is_none() {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                format!(
                    "job `{}` FrozenApprovalLinkage has no FrozenControllerCapabilityEnvelope",
                    job.job_id
                ),
            ));
        }

        if planning_result.plan_id != linkage.plan_id
            || planning_result.plan_revision != linkage.plan_revision
            || !constant_time_str_eq(&planning_result.plan_digest, &linkage.plan_digest)
            || planning_result.profile_id != linkage.profile_id
            || planning_result.registry_revision != linkage.registry_revision
            || planning_result.project_base != linkage.project_base
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result plan/profile/registry/project_base metadata does not match job.approval_linkage",
            ));
        }

        for task in &job.tasks {
            let expected_ifaces = planning_result.task_interfaces.get(&task.task_id);
            let expected_owned = expected_ifaces
                .map(|s| s.owned_interfaces.as_slice())
                .unwrap_or(&[]);
            let expected_consumed = expected_ifaces
                .map(|s| s.consumed_interfaces.as_slice())
                .unwrap_or(&[]);
            if task.owned_interfaces != expected_owned
                || task.consumed_interfaces != expected_consumed
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "job task `{}` interface declarations do not match Orchestrator planning_result.task_interfaces",
                        task.task_id
                    ),
                ));
            }
        }

        let mut runtime_state = self
            .policy_store
            .get_job_runtime_readonly(binding_id, job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::UnauthorizedOperator,
                    format!(
                        "no ControllerJobRuntimeState configured in ControllerPolicyStore for job `{job_id}`"
                    ),
                )
            })?;
        runtime_state.envelope.policy.validate()?;
        runtime_state.envelope.verify_matches_job(&job)?;

        let approval = self
            .authority
            .get_for_operator(&linkage.approval_id)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "failed to load live ApprovalRecord `{}`: {err}",
                        linkage.approval_id
                    ),
                )
            })?;

        verify_approval_and_plan_evidence(
            &approval,
            &job,
            &runtime_state.envelope.policy,
            &self.authority,
        )?;

        let coder_budget = runtime_state.envelope.coder_budget()?;
        if approval.budgets.timeout_ms > coder_budget.max_timeout_ms_per_call
            || approval.budgets.max_stdout_bytes > coder_budget.max_stdout_bytes_per_call
            || approval.budgets.max_stderr_bytes > coder_budget.max_stderr_bytes_per_call
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "ApprovalRecord `{}` worker execution budgets (timeout_ms={}, max_stdout_bytes={}, max_stderr_bytes={}) exceed frozen Coder role budget caps (max_timeout_ms_per_call={}, max_stdout_bytes_per_call={}, max_stderr_bytes_per_call={})",
                    approval.approval_id,
                    approval.budgets.timeout_ms,
                    approval.budgets.max_stdout_bytes,
                    approval.budgets.max_stderr_bytes,
                    coder_budget.max_timeout_ms_per_call,
                    coder_budget.max_stdout_bytes_per_call,
                    coder_budget.max_stderr_bytes_per_call,
                ),
            ));
        }

        if approval.tasks.len() != job.tasks.len() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ApprovalRecord task count does not match job.tasks length",
            ));
        }

        for job_task in &job.tasks {
            if job_task.role != WorkflowRole::Coder {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    format!(
                        "job task `{}` has role `{:?}`; only WorkflowRole::Coder tasks are valid execution DAG nodes",
                        job_task.task_id, job_task.role
                    ),
                ));
            }
            validate_non_native_worker_choice(WorkflowRole::Coder, &job_task.selected_choice)?;

            let env_choice = runtime_state
                .envelope
                .task_coder_choices
                .get(&job_task.task_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "FrozenControllerExecutionEnvelope is missing task_coder_choices entry for `{}`",
                            job_task.task_id
                        ),
                    )
                })?;
            if env_choice != &job_task.selected_choice {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` selected_choice does not match FrozenControllerExecutionEnvelope.task_coder_choices",
                        job_task.task_id
                    ),
                ));
            }

            let approved_task = approval
                .tasks
                .iter()
                .find(|t| t.task_id == job_task.task_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "job task `{}` was not found in ApprovalRecord `{}`",
                            job_task.task_id, approval.approval_id
                        ),
                    )
                })?;

            verify_approved_task_matches_coder_choice(approved_task, job_task)?;
        }

        verify_frozen_runtime_registry_or_apply_config_hold(
            &approval,
            &mut job,
            &mut runtime_state,
            &self.authority,
            &self.workflow_store,
            &self.policy_store,
        )?;

        Ok(VerifiedControllerAdmission {
            job,
            planning_result,
            linkage,
            approval,
            runtime_state,
        })
    }

    #[inline(never)]
    fn load_and_verify_admitted_job_from_ledger(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<(CoworkerJobRecord, PlanningResultRef), NativeError> {
        let ledger = self.workflow_store.read_binding_ledger(binding_id)?;
        ledger.validate(binding_id)?;
        if ledger.thread_uuid.as_deref() != Some(thread_uuid) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "binding `{binding_id}` ledger thread_uuid `{:?}` does not match requested thread_uuid `{thread_uuid}`",
                    ledger.thread_uuid
                ),
            ));
        }

        let job = ledger
            .jobs
            .iter()
            .find(|j| j.job_id == job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("job `{job_id}` was not found in binding `{binding_id}`"),
                )
            })?
            .clone();

        if job.binding_id != binding_id || job.thread_uuid != thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "job binding_id or thread_uuid does not match requested controller scope",
            ));
        }

        if ledger
            .jobs
            .iter()
            .any(|other| other.supersedes_job_id.as_deref() == Some(job.job_id.as_str()))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "job `{}` has been superseded by a newer amendment job and cannot run automatically",
                    job.job_id
                ),
            ));
        }

        let planning_result = job
            .planning_result
            .as_ref()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "job `{}` has no Orchestrator PlanningResultRef; manual unlinked jobs are refused by automatic controller admission",
                        job.job_id
                    ),
                )
            })?
            .clone();
        planning_result.validate()?;

        if !constant_time_str_eq(&planning_result.work_brief_digest, &job.work_brief_digest) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result.work_brief_digest does not match current job.work_brief_digest",
            ));
        }
        if planning_result.project_base != job.work_brief.project_base {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result.project_base does not match current job.work_brief.project_base",
            ));
        }
        if !job
            .work_brief
            .is_ready_for_execution_approval_with_planning(Some(&planning_result))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.work_brief has unresolved open_questions or unconfirmed proposed_additions not covered by planning_result",
            ));
        }

        self.verify_planning_provenance_in_ledger(&ledger, &job, &planning_result)?;
        Ok((job, planning_result))
    }

    #[inline(never)]
    fn verify_planning_provenance_in_ledger(
        &self,
        ledger: &CoworkerBindingLedger,
        job: &CoworkerJobRecord,
        pr: &PlanningResultRef,
    ) -> Result<(), NativeError> {
        let grant = ledger
            .planning_grants
            .iter()
            .find(|g| g.grant_id == pr.grant_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "job.planning_result references unknown grant_id `{}`",
                        pr.grant_id
                    ),
                )
            })?;

        let reservation = grant
            .reservations
            .iter()
            .find(|r| r.reservation_id == pr.reservation_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "job.planning_result references unknown reservation_id `{}`",
                        pr.reservation_id
                    ),
                )
            })?;

        if grant.binding_id != job.binding_id
            || grant.thread_uuid != job.thread_uuid
            || grant.profile_id != pr.profile_id
            || grant.registry_revision != pr.registry_revision
            || !constant_time_str_eq(&grant.policy_digest, &pr.policy_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result grant scope/profile/registry/policy does not match PlanningAdviceGrant in binding ledger",
            ));
        }

        let effective_call_base = reservation
            .call_project_base
            .as_ref()
            .unwrap_or(&grant.project_base);
        if effective_call_base != &pr.project_base {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result.project_base does not match Orchestrator reservation project_base",
            ));
        }

        if reservation.role != WorkflowRole::Orchestrator
            || reservation.status != PlanningReservationStatus::Completed
            || reservation.request_key != pr.request_key
            || reservation.selected_choice != pr.selected_choice
            || reservation.run_id.as_deref() != Some(pr.run_id.as_str())
            || reservation.report_artifact_id.as_deref() != Some(pr.report_artifact_id.as_str())
            || reservation
                .output_digest
                .as_deref()
                .is_none_or(|d| !constant_time_str_eq(d, &pr.output_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "job.planning_result does not match completed Orchestrator reservation in binding ledger",
            ));
        }

        let artifacts_dir = self.planning_artifacts_dir.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "controller planning_artifacts_dir is not configured; automatic admission requires a verified on-disk PersistedPlanningCallArtifact for job `{}`",
                    job.job_id
                ),
            )
        })?;
        let planning_service = CoworkerPlanningService::new(
            self.workflow_store.clone(),
            self.authority.registry().config_dir(),
            artifacts_dir,
        )?;
        let artifact = planning_service
            .read_planning_artifact(&pr.report_artifact_id)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "failed to load verified PersistedPlanningCallArtifact `{}` for job `{}`: {}",
                        pr.report_artifact_id,
                        job.job_id,
                        err.message()
                    ),
                )
            })?;
        artifact
            .validate_against_grant(&pr.report_artifact_id, grant)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "PersistedPlanningCallArtifact `{}` failed grant validation for job `{}`: {}",
                        pr.report_artifact_id,
                        job.job_id,
                        err.message()
                    ),
                )
            })?;

        if artifact.binding_id != job.binding_id
            || artifact.thread_uuid != job.thread_uuid
            || artifact.grant_id != pr.grant_id
            || artifact.reservation_id != pr.reservation_id
            || artifact.run_id != pr.run_id
            || artifact.request_key != pr.request_key
            || artifact.role != WorkflowRole::Orchestrator
            || artifact.profile_id != pr.profile_id
            || artifact.registry_revision != pr.registry_revision
            || !constant_time_str_eq(&artifact.policy_digest, &pr.policy_digest)
            || artifact.selected_choice != pr.selected_choice
            || artifact.call_project_base != pr.project_base
            || artifact
                .output_digest
                .as_deref()
                .is_none_or(|d| !constant_time_str_eq(d, &pr.output_digest))
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PersistedPlanningCallArtifact metadata does not match job.planning_result",
            ));
        }

        match &artifact.outcome {
            PlanningArtifactOutcomeKind::OrchestratorOutput {
                job_id: art_job_id,
                work_brief_digest: art_brief_digest,
                structured_output: StructuredOrchestratorOutput::Plan { .. },
                ..
            } => {
                if art_job_id != &job.job_id
                    || !constant_time_str_eq(art_brief_digest, &job.work_brief_digest)
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "PersistedPlanningCallArtifact job_id or work_brief_digest does not match job",
                    ));
                }
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "PersistedPlanningCallArtifact is not a completed Orchestrator Plan outcome",
                ));
            }
        }

        Ok(())
    }

    /// Executes a single bounded Phase 1 reconciliation and scheduling step for an admitted job.
    ///
    /// - Re-verifies live physical OS ownership (`guard`) before every effect and after every
    ///   `.await` point.
    /// - Never holds synchronous file locks across `.await`.
    /// - Reconciles existing `Coder` host tasks by frozen `command_key` without double-charging
    ///   budgets or re-launching workers.
    /// - Promotes `PendingDependencies` tasks to `ReadyToDispatch` only when all predecessor tasks
    ///   are `Accepted` or `Integrated` with verified review + integration receipts and the host
    ///   reports a verified `DependencyInputsReady` seal.
    /// - Dispatches ready `Coder` tasks (`attempt_no == 1`) up to `approval.max_parallel` using
    ///   intent-before-effect reservation.
    pub async fn step_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
    ) -> Result<CoworkerControllerStepReport, NativeError> {
        let _step_serial_guard = self.step_mutex.lock().await;
        let (controller_instance_id, lease_epoch, approval, approved_by_id, task_ids) =
            self.prepare_step_job(guard, request)?;

        let host_client = ApprovedHostClient::new(
            Arc::clone(&self.authority),
            &self.sessions_dir,
            &request.binding_id,
            &request.thread_uuid,
        )
        .map_err(map_host_error)?;

        let mut promoted_dependency_ready_task_ids = Vec::new();
        let mut reconciled_ready_for_review_task_ids = Vec::new();
        let mut dispatched_task_ids = Vec::new();
        let mut fenced_task_ids = Vec::new();

        // Pass 1: Query live host task snapshots and reconcile existing attempts / dependency seals.
        for task_id in &task_ids {
            self.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;

            let snapshot = host_client
                .get_task(&approval.approval_id, task_id)
                .await
                .map_err(map_host_error)?;

            self.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;

            let approved_task = approved_by_id.get(task_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{task_id}` missing from ApprovalRecord"),
                )
            })?;
            verify_snapshot_matches_approved_task(&snapshot, approved_task)?;

            self.reconcile_single_task_snapshot(
                guard,
                request,
                &approval,
                approved_task,
                &snapshot,
                &controller_instance_id,
                lease_epoch,
                &mut promoted_dependency_ready_task_ids,
                &mut reconciled_ready_for_review_task_ids,
                &mut fenced_task_ids,
            )?;
        }

        // Pass 2: Bounded parallel dispatch of ready initial Coder tasks (`attempt_no == 1`).
        while let Some(candidate_task) = self.select_next_dispatch_candidate(
            guard,
            request,
            &approval,
            &controller_instance_id,
            lease_epoch,
            &mut fenced_task_ids,
        )? {
            let approved_task = approved_by_id.get(&candidate_task.task_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` missing from ApprovalRecord",
                        candidate_task.task_id
                    ),
                )
            })?;

            // Re-check live host snapshot before reserving/dispatching so we never double-launch a
            // task that already has a host intent or receipt.
            let pre_snapshot = host_client
                .get_task(&approval.approval_id, &candidate_task.task_id)
                .await
                .map_err(map_host_error)?;

            self.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;
            verify_snapshot_matches_approved_task(&pre_snapshot, approved_task)?;

            let expected_pre_status = if candidate_task.depends_on.is_empty() {
                TaskExecutionStatus::NotStarted
            } else {
                TaskExecutionStatus::DependencyInputsReady
            };

            if pre_snapshot.status != expected_pre_status {
                // Host already has state for this task; reconcile it instead of dispatching.
                self.reconcile_single_task_snapshot(
                    guard,
                    request,
                    &approval,
                    approved_task,
                    &pre_snapshot,
                    &controller_instance_id,
                    lease_epoch,
                    &mut promoted_dependency_ready_task_ids,
                    &mut reconciled_ready_for_review_task_ids,
                    &mut fenced_task_ids,
                )?;
                continue;
            }

            self.dispatch_and_reconcile_initial_coder_task(
                guard,
                request,
                &host_client,
                &approval,
                approved_task,
                &candidate_task,
                &controller_instance_id,
                lease_epoch,
                &mut dispatched_task_ids,
                &mut reconciled_ready_for_review_task_ids,
                &mut fenced_task_ids,
            )
            .await?;
        }

        // Final observation pass to build truthful step report.
        self.build_step_report(
            guard,
            request,
            &host_client,
            &approval,
            &approved_by_id,
            lease_epoch,
            controller_instance_id,
            dispatched_task_ids,
            reconciled_ready_for_review_task_ids,
            promoted_dependency_ready_task_ids,
            fenced_task_ids,
        )
        .await
    }

    #[inline(never)]
    #[allow(clippy::type_complexity)]
    fn prepare_step_job(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
    ) -> Result<
        (
            String,
            u64,
            ApprovalRecord,
            BTreeMap<String, ApprovedTaskRecord>,
            Vec<String>,
        ),
        NativeError,
    > {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        if request.now_ms == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CoworkerControllerStepRequest.now_ms must be > 0",
            ));
        }

        let admission = self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let lease_ttl_ms = request
            .lease_ttl_ms
            .unwrap_or(DEFAULT_CONTROLLER_LEASE_TTL_MS);
        let controller_instance_id = deterministic_controller_instance_id(
            &request.binding_id,
            &request.job_id,
            guard.instance_id(),
        );

        self.reconcile_stale_logical_lease_under_physical_guard(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let lease_record = self.policy_store.acquire_or_renew_lease(
            &request.binding_id,
            &request.job_id,
            &controller_instance_id,
            request.now_ms,
            lease_ttl_ms,
        )?;
        let runtime_after_lease = self
            .policy_store
            .get_job_runtime_readonly(&request.binding_id, &request.job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::UnauthorizedOperator,
                    format!(
                        "no ControllerJobRuntimeState configured in ControllerPolicyStore for job `{}`",
                        request.job_id
                    ),
                )
            })?;
        let lease_epoch =
            require_active_lease_epoch(&runtime_after_lease, &controller_instance_id)?;
        if lease_record.lease_epoch != lease_epoch {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller lease_epoch drifted immediately after acquire_or_renew_lease",
            ));
        }

        if let Ok(change_store) =
            ConversationChangeStore::open_readonly(self.workflow_store.plans_dir())
        {
            if let Ok(change_rev) = change_store.current_revision(&request.binding_id) {
                if change_rev > runtime_after_lease.synced_change_revision {
                    self.policy_store.mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        &controller_instance_id,
                        lease_epoch,
                        |state| {
                            if change_rev > state.synced_change_revision {
                                state.synced_change_revision = change_rev;
                            }
                            Ok(())
                        },
                    )?;
                }
            }
        }

        let approval = admission.approval;
        let approved_by_id: BTreeMap<String, ApprovedTaskRecord> = approval
            .tasks
            .iter()
            .cloned()
            .map(|t| (t.task_id.clone(), t))
            .collect();
        let task_ids: Vec<String> = admission
            .job
            .tasks
            .iter()
            .map(|t| t.task_id.clone())
            .collect();

        Ok((
            controller_instance_id,
            lease_epoch,
            approval,
            approved_by_id,
            task_ids,
        ))
    }

    #[inline(never)]
    fn select_next_dispatch_candidate(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        approval: &ApprovalRecord,
        controller_instance_id: &str,
        lease_epoch: u64,
        fenced_task_ids: &mut Vec<String>,
    ) -> Result<Option<CoworkerTaskRecord>, NativeError> {
        self.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let current_adm = self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        if require_active_lease_epoch(&current_adm.runtime_state, controller_instance_id)?
            != lease_epoch
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller lease_epoch changed concurrently during step_job",
            ));
        }

        if current_adm.runtime_state.stop_requested
            || current_adm.job.phase != CoworkerJobPhase::Executing
        {
            for t in &current_adm.job.tasks {
                if t.stage == CoworkerTaskStage::ReadyToDispatch
                    && !fenced_task_ids.contains(&t.task_id)
                {
                    fenced_task_ids.push(t.task_id.clone());
                }
            }
            return Ok(None);
        }

        let active_in_flight =
            count_active_coder_executions(&current_adm.job, &current_adm.runtime_state);
        if active_in_flight >= approval.max_parallel {
            return Ok(None);
        }

        for task in &current_adm.job.tasks {
            if task.stage != CoworkerTaskStage::ReadyToDispatch {
                continue;
            }
            if is_task_fenced_by_hold_or_sibling(
                task,
                &current_adm.job,
                &current_adm.runtime_state,
                &approval.approval_id,
                self.workflow_store.plans_dir(),
            )? {
                if !fenced_task_ids.contains(&task.task_id) {
                    fenced_task_ids.push(task.task_id.clone());
                }
                continue;
            }
            return Ok(Some(task.clone()));
        }

        Ok(None)
    }

    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    fn reconcile_single_task_snapshot(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        approval: &ApprovalRecord,
        approved_task: &ApprovedTaskRecord,
        snapshot: &TaskStateSnapshot,
        controller_instance_id: &str,
        lease_epoch: u64,
        promoted_dependency_ready_task_ids: &mut Vec<String>,
        reconciled_ready_for_review_task_ids: &mut Vec<String>,
        fenced_task_ids: &mut Vec<String>,
    ) -> Result<(), NativeError> {
        self.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let current_adm = self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;
        if require_active_lease_epoch(&current_adm.runtime_state, controller_instance_id)?
            != lease_epoch
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller lease_epoch changed concurrently during reconciliation",
            ));
        }

        // Optional host diagnostics never replace authoritative admission or provider results.
        if let Some(health) = crate::provider_health::OriginalWorkerProviderHealth::from_snapshot(
            snapshot,
            approval,
            approved_task,
        ) {
            let _ = self.policy_store.mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    guard.validate_before_effect(&guard.key())?;
                    if state.envelope.approval_id != approval.approval_id
                        || state.envelope.thread_uuid != request.thread_uuid
                    {
                        return Err(crate::provider_health::rejected());
                    }
                    if let Some(prior) = state.original_worker_provider_health.get(&health.task_id)
                    {
                        if !health.is_valid_successor_of(prior) {
                            return Err(crate::provider_health::rejected());
                        }
                    }
                    state
                        .original_worker_provider_health
                        .insert(health.task_id.clone(), health);
                    Ok(())
                },
            );
        }

        let task = current_adm
            .job
            .tasks
            .iter()
            .find(|t| t.task_id == approved_task.task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{}` not found in job", approved_task.task_id),
                )
            })?
            .clone();

        let command_key = approved_task_command_key(approved_task)?;

        // Verify no conflicting request_key exists for `(task_id, attempt_no = 1)` in runtime_state or job.outbox.
        verify_no_conflicting_initial_coder_key(
            &current_adm.job,
            &current_adm.runtime_state,
            &task.task_id,
            &command_key,
        )?;

        // Once a derived attempt receipt, subsequent attempt, or post-initial candidate/review
        // artifact exists for this task, initial attempt 1 reconciliation is complete and
        // immutable. Skip initial host snapshot reconciliation so attempt 1 artifacts and
        // derived task stages are never overwritten by the old attempt 1 host snapshot.
        if task_has_derived_attempt_or_post_initial_state(&task, &current_adm.runtime_state) {
            return Ok(());
        }

        // 1. Check if `PendingDependencies` can be promoted to `ReadyToDispatch`.
        if task.stage == CoworkerTaskStage::PendingDependencies {
            if let Some(accepted_pred_receipts) = verify_predecessors_accepted_and_sealed(
                approval,
                approved_task,
                &task,
                &current_adm.job,
                snapshot,
                &self.sessions_dir,
            )? {
                if is_task_fenced_by_hold_or_sibling(
                    &task,
                    &current_adm.job,
                    &current_adm.runtime_state,
                    &approval.approval_id,
                    self.workflow_store.plans_dir(),
                )? {
                    if !fenced_task_ids.contains(&task.task_id) {
                        fenced_task_ids.push(task.task_id.clone());
                    }
                    return Ok(());
                }

                let dep_summary = snapshot.dependency_input.as_ref().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "DependencyInputsReady snapshot is missing DependencyInputSummary",
                    )
                })?;

                let dep_req_key = deterministic_prefixed_id(
                    "threadlane-native:coworker-dep-prep:v1",
                    "dep",
                    &[
                        &current_adm.job.job_id,
                        &task.task_id,
                        &dep_summary.input_digest,
                    ],
                );
                let _ = self
                    .workflow_store
                    .reserve_task_outbox(&ReserveTaskOutboxRequest {
                        binding_id: request.binding_id.clone(),
                        thread_uuid: request.thread_uuid.clone(),
                        job_id: request.job_id.clone(),
                        expected_job_revision: current_adm.job.job_revision,
                        expected_ledger_revision: None,
                        request_key: dep_req_key,
                        kind: OutboxDispatchKind::DependencyPreparation,
                        task_id: task.task_id.clone(),
                        role: WorkflowRole::Coder,
                        subject_digest: dep_summary.input_digest.clone(),
                        dependency_input_commit: Some(dep_summary.base_commit.clone()),
                        dependency_input_tree_sha: Some(dep_summary.tree_sha.clone()),
                        accepted_predecessor_receipts: accepted_pred_receipts,
                    })?;

                if !promoted_dependency_ready_task_ids.contains(&task.task_id) {
                    promoted_dependency_ready_task_ids.push(task.task_id.clone());
                }
            }
            return Ok(());
        }

        // 2. Reconcile based on live host TaskExecutionStatus.
        match snapshot.status {
            TaskExecutionStatus::NotStarted
            | TaskExecutionStatus::DependencyInputsUnverified
            | TaskExecutionStatus::DependencyInputsReady => {
                // Host has not started this task and has no unresolved dispatch intent.
                // If an outbox was previously left in `ReservedBeforeEffect` (or `EffectUnknown`) before
                // crossing into host execution, reconcile it back to `ReadyToDispatch` without
                // charging another attempt.
                if let Some(existing_outbox) =
                    find_task_attempt_outbox(&current_adm.job, &task.task_id, &command_key)
                {
                    if matches!(
                        existing_outbox.status,
                        OutboxDispatchStatus::ReservedBeforeEffect
                            | OutboxDispatchStatus::EffectUnknown
                    ) {
                        self.workflow_store.reconcile_or_finalize_outbox(
                            &FinalizeTaskOutboxRequest {
                                binding_id: request.binding_id.clone(),
                                thread_uuid: request.thread_uuid.clone(),
                                job_id: request.job_id.clone(),
                                expected_job_revision: current_adm.job.job_revision,
                                expected_ledger_revision: None,
                                outbox_id: existing_outbox.outbox_id.clone(),
                                status: OutboxDispatchStatus::ReconciledNotExecuted,
                                next_task_stage: CoworkerTaskStage::ReadyToDispatch,
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
                                diagnostic: Some(
                                    "host confirmed no dispatch intent or receipt was recorded"
                                        .to_string(),
                                ),
                            },
                        )?;
                    }
                }
                Ok(())
            }
            TaskExecutionStatus::NativeRouteRequired => Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                format!(
                    "task `{}` requires a native handoff route, which is unsupported for headless controller execution",
                    task.task_id
                ),
            )),
            TaskExecutionStatus::Unknown => {
                // Host has an unresolved dispatch intent or missing worker snapshot for a sealed receipt.
                // Transition outbox and pending effect to `EffectUnknown`; never replay or reset caps.
                let (job_with_outbox, outbox) = self.ensure_initial_coder_outbox_and_effect(
                    request,
                    &current_adm,
                    approved_task,
                    &task,
                    &command_key,
                    controller_instance_id,
                    lease_epoch,
                )?;
                if outbox.status == OutboxDispatchStatus::ReservedBeforeEffect {
                    self.workflow_store
                        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                            binding_id: request.binding_id.clone(),
                            thread_uuid: request.thread_uuid.clone(),
                            job_id: request.job_id.clone(),
                            expected_job_revision: job_with_outbox.job_revision,
                            expected_ledger_revision: None,
                            outbox_id: outbox.outbox_id,
                            status: OutboxDispatchStatus::EffectUnknown,
                            next_task_stage: CoworkerTaskStage::EffectUnknown,
                            run_id: snapshot.run_id.clone(),
                            worker_id: snapshot.worker_id.clone(),
                            task_worktree_id: snapshot
                                .worker
                                .as_ref()
                                .and_then(|w| w.worktree.task_worktree_id.clone()),
                            head_commit: None,
                            tree_sha: None,
                            diff_digest: None,
                            report_artifact_id: None,
                            observed_model: None,
                            observed_effort: None,
                            receipt_id: None,
                            diagnostic: Some(
                                "host reported TaskExecutionStatus::Unknown; awaiting receipt reconciliation"
                                    .to_string(),
                            ),
                        })?;
                }
                self.policy_store.mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state
                            .pending_effects
                            .values_mut()
                            .find(|e| e.request_key == command_key)
                        {
                            eff.status = PendingControllerEffectStatus::EffectUnknown;
                            eff.updated_at_ms = request.now_ms;
                            if let Some(ref r_id) = snapshot.run_id {
                                eff.external_correlation_id = Some(r_id.clone());
                            }
                        }
                        Ok(())
                    },
                )?;
                Ok(())
            }
            TaskExecutionStatus::Queued
            | TaskExecutionStatus::Claimed
            | TaskExecutionStatus::Running
            | TaskExecutionStatus::CancelRequested => {
                let run_id = snapshot.run_id.clone().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "active host task snapshot is missing run_id",
                    )
                })?;
                let worker_id = snapshot.worker_id.clone().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "active host task snapshot is missing worker_id",
                    )
                })?;
                let task_worktree_id = snapshot
                    .worker
                    .as_ref()
                    .and_then(|w| w.worktree.task_worktree_id.clone());

                let (job_with_outbox, outbox) = self.ensure_initial_coder_outbox_and_effect(
                    request,
                    &current_adm,
                    approved_task,
                    &task,
                    &command_key,
                    controller_instance_id,
                    lease_epoch,
                )?;

                if matches!(
                    outbox.status,
                    OutboxDispatchStatus::ReservedBeforeEffect
                        | OutboxDispatchStatus::EffectUnknown
                ) {
                    let next_status = if outbox.status == OutboxDispatchStatus::EffectUnknown {
                        OutboxDispatchStatus::ReconciledConfirmed
                    } else {
                        OutboxDispatchStatus::DispatchedEffectConfirmed
                    };
                    self.workflow_store
                        .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                            binding_id: request.binding_id.clone(),
                            thread_uuid: request.thread_uuid.clone(),
                            job_id: request.job_id.clone(),
                            expected_job_revision: job_with_outbox.job_revision,
                            expected_ledger_revision: None,
                            outbox_id: outbox.outbox_id,
                            status: next_status,
                            next_task_stage: CoworkerTaskStage::Running,
                            run_id: Some(run_id.clone()),
                            worker_id: Some(worker_id.clone()),
                            task_worktree_id: task_worktree_id.clone(),
                            head_commit: None,
                            tree_sha: None,
                            diff_digest: None,
                            report_artifact_id: None,
                            observed_model: None,
                            observed_effort: None,
                            receipt_id: None,
                            diagnostic: None,
                        })?;
                }

                self.policy_store.mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state
                            .pending_effects
                            .values_mut()
                            .find(|e| e.request_key == command_key)
                        {
                            eff.external_correlation_id = Some(run_id.clone());
                            eff.updated_at_ms = request.now_ms;
                        }
                        Ok(())
                    },
                )?;
                Ok(())
            }
            TaskExecutionStatus::ReadyForReview => {
                // If already in ReadyForReview or later stage, ensure pending effect is marked EffectConfirmed
                // idempotently without re-charging or re-mutating the task stage.
                if matches!(
                    task.stage,
                    CoworkerTaskStage::ReadyForReview
                        | CoworkerTaskStage::UnderReview
                        | CoworkerTaskStage::Accepted
                        | CoworkerTaskStage::Integrated
                ) {
                    let duration_ms = snapshot
                        .worker
                        .as_ref()
                        .and_then(|w| match (w.started_at_ms, w.completed_at_ms) {
                            (Some(s), Some(f)) => Some(f.saturating_sub(s).max(0) as u64),
                            _ => None,
                        })
                        .unwrap_or(0);
                    self.policy_store.mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        |state| {
                            if let Some(eff) = state
                                .pending_effects
                                .values_mut()
                                .find(|e| e.request_key == command_key)
                            {
                                if eff.status != PendingControllerEffectStatus::EffectConfirmed {
                                    eff.status = PendingControllerEffectStatus::EffectConfirmed;
                                    eff.external_correlation_id = snapshot.run_id.clone();
                                    eff.updated_at_ms = request.now_ms;
                                    state.record_role_completion(WorkflowRole::Coder, duration_ms);
                                }
                            }
                            Ok(())
                        },
                    )?;
                    return Ok(());
                }

                // Check fencing BEFORE promoting worker output to ReadyForReview!
                if is_task_fenced_by_hold_or_sibling(
                    &task,
                    &current_adm.job,
                    &current_adm.runtime_state,
                    &approval.approval_id,
                    self.workflow_store.plans_dir(),
                )? {
                    if !fenced_task_ids.contains(&task.task_id) {
                        fenced_task_ids.push(task.task_id.clone());
                    }
                    return self.reconcile_fenced_ready_for_review_snapshot(
                        request,
                        &current_adm,
                        approval,
                        approved_task,
                        &task,
                        &command_key,
                        snapshot,
                        controller_instance_id,
                        lease_epoch,
                    );
                }

                let evidence = extract_verified_ready_for_review_evidence(
                    &self.sessions_dir,
                    approval,
                    approved_task,
                    &task.selected_choice,
                    snapshot,
                )?;

                let (job_with_outbox, outbox) = self.ensure_initial_coder_outbox_and_effect(
                    request,
                    &current_adm,
                    approved_task,
                    &task,
                    &command_key,
                    controller_instance_id,
                    lease_epoch,
                )?;

                let resolved_status = if outbox.status == OutboxDispatchStatus::EffectUnknown {
                    OutboxDispatchStatus::ReconciledConfirmed
                } else {
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                };
                self.workflow_store
                    .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                        binding_id: request.binding_id.clone(),
                        thread_uuid: request.thread_uuid.clone(),
                        job_id: request.job_id.clone(),
                        expected_job_revision: job_with_outbox.job_revision,
                        expected_ledger_revision: None,
                        outbox_id: outbox.outbox_id,
                        status: resolved_status,
                        next_task_stage: CoworkerTaskStage::ReadyForReview,
                        run_id: Some(evidence.run_id.clone()),
                        worker_id: Some(evidence.worker_id.clone()),
                        task_worktree_id: Some(evidence.task_worktree_id.clone()),
                        head_commit: Some(evidence.head_commit.clone()),
                        tree_sha: Some(evidence.tree_sha.clone()),
                        diff_digest: Some(evidence.diff_digest.clone()),
                        report_artifact_id: Some(evidence.report_artifact_id.clone()),
                        observed_model: evidence.observed_model.clone(),
                        observed_effort: evidence.observed_effort.clone(),
                        receipt_id: None,
                        diagnostic: None,
                    })?;

                self.policy_store.mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state
                            .pending_effects
                            .values_mut()
                            .find(|e| e.request_key == command_key)
                        {
                            if eff.status != PendingControllerEffectStatus::EffectConfirmed {
                                eff.status = PendingControllerEffectStatus::EffectConfirmed;
                                eff.external_correlation_id = Some(evidence.run_id.clone());
                                eff.updated_at_ms = request.now_ms;
                                state.record_role_completion(
                                    WorkflowRole::Coder,
                                    evidence.duration_ms,
                                );
                            }
                        }
                        let artifact_rec = TaskAttemptExecutionArtifacts {
                            task_id: task.task_id.clone(),
                            attempt_no: 1,
                            run_id: evidence.run_id.clone(),
                            worker_id: evidence.worker_id.clone(),
                            task_worktree_id: evidence.task_worktree_id.clone(),
                            integration_worktree_id: evidence.integration_worktree_id.clone(),
                            execution_base_commit: evidence.execution_base_commit.clone(),
                            observed_provider: evidence.observed_provider.clone(),
                            observed_model: evidence.observed_model.clone(),
                            codex_launch_report: evidence.codex_launch_report.clone(),
                            codex_source_report_sha256: evidence.codex_source_report_sha256.clone(),
                            codex_source_effect_id: None,
                            observed_effort: evidence.observed_effort.clone(),
                            committed_head_commit: Some(evidence.head_commit.clone()),
                            committed_tree_sha: Some(evidence.tree_sha.clone()),
                            candidate_id: None,
                            check_evidence: Vec::new(),
                            test_evidence_summary: None,
                            review_subject: None,
                            reviewer_response_digest: None,
                            review_receipt: None,
                            integration_receipt: None,
                            is_derived_attempt: false,
                        };
                        state.attempt_artifacts.insert(
                            ControllerJobRuntimeState::artifact_key(&task.task_id, 1),
                            artifact_rec,
                        );
                        Ok(())
                    },
                )?;

                if !reconciled_ready_for_review_task_ids.contains(&task.task_id) {
                    reconciled_ready_for_review_task_ids.push(task.task_id.clone());
                }
                Ok(())
            }
            TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut => {
                if matches!(
                    task.stage,
                    CoworkerTaskStage::Failed | CoworkerTaskStage::Cancelled
                ) {
                    return Ok(());
                }

                let next_stage = if snapshot.status == TaskExecutionStatus::Cancelled {
                    CoworkerTaskStage::Cancelled
                } else {
                    CoworkerTaskStage::Failed
                };
                let (job_with_outbox, outbox) = self.ensure_initial_coder_outbox_and_effect(
                    request,
                    &current_adm,
                    approved_task,
                    &task,
                    &command_key,
                    controller_instance_id,
                    lease_epoch,
                )?;
                let diag = format!(
                    "host worker finished in terminal status `{:?}`",
                    snapshot.status
                );
                let resolved_status = if outbox.status == OutboxDispatchStatus::EffectUnknown {
                    OutboxDispatchStatus::ReconciledConfirmed
                } else {
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                };
                self.workflow_store
                    .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                        binding_id: request.binding_id.clone(),
                        thread_uuid: request.thread_uuid.clone(),
                        job_id: request.job_id.clone(),
                        expected_job_revision: job_with_outbox.job_revision,
                        expected_ledger_revision: None,
                        outbox_id: outbox.outbox_id,
                        status: resolved_status,
                        next_task_stage: next_stage,
                        run_id: snapshot.run_id.clone(),
                        worker_id: snapshot.worker_id.clone(),
                        task_worktree_id: snapshot
                            .worker
                            .as_ref()
                            .and_then(|w| w.worktree.task_worktree_id.clone()),
                        head_commit: None,
                        tree_sha: None,
                        diff_digest: None,
                        report_artifact_id: None,
                        observed_model: snapshot
                            .worker
                            .as_ref()
                            .and_then(|w| w.observed_model.clone()),
                        observed_effort: snapshot
                            .worker
                            .as_ref()
                            .and_then(|w| w.observed_effort.clone()),
                        receipt_id: None,
                        diagnostic: Some(diag.clone()),
                    })?;

                self.policy_store.mutate_job_runtime(
                    &request.binding_id,
                    &request.job_id,
                    controller_instance_id,
                    lease_epoch,
                    |state| {
                        if let Some(eff) = state
                            .pending_effects
                            .values_mut()
                            .find(|e| e.request_key == command_key)
                        {
                            eff.status = PendingControllerEffectStatus::EffectFailedTerminal;
                            eff.external_correlation_id = snapshot.run_id.clone();
                            eff.diagnostic = Some(diag.clone());
                            eff.updated_at_ms = request.now_ms;
                        }
                        Ok(())
                    },
                )?;
                Ok(())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_fenced_ready_for_review_snapshot(
        &self,
        request: &CoworkerControllerStepRequest,
        current_adm: &VerifiedControllerAdmission,
        approval: &ApprovalRecord,
        approved_task: &ApprovedTaskRecord,
        task: &CoworkerTaskRecord,
        command_key: &str,
        snapshot: &TaskStateSnapshot,
        controller_instance_id: &str,
        lease_epoch: u64,
    ) -> Result<(), NativeError> {
        let existing_outbox =
            find_task_attempt_outbox(&current_adm.job, &task.task_id, command_key);
        let has_pending_effect = current_adm
            .runtime_state
            .pending_effects
            .values()
            .any(|e| e.request_key == command_key);
        let was_controller_dispatched = existing_outbox.is_some()
            || has_pending_effect
            || matches!(
                task.stage,
                CoworkerTaskStage::DispatchReserved
                    | CoworkerTaskStage::Running
                    | CoworkerTaskStage::Held
                    | CoworkerTaskStage::EffectUnknown
            );
        if !was_controller_dispatched {
            return Ok(());
        }

        let evidence = extract_verified_ready_for_review_evidence(
            &self.sessions_dir,
            approval,
            approved_task,
            &task.selected_choice,
            snapshot,
        )?;

        if let Some(outbox) = existing_outbox {
            let fenced_stage = if matches!(
                task.stage,
                CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::EffectUnknown
            ) {
                CoworkerTaskStage::Running
            } else {
                task.stage
            };
            if matches!(
                outbox.status,
                OutboxDispatchStatus::ReservedBeforeEffect | OutboxDispatchStatus::EffectUnknown
            ) || task.stage != fenced_stage
                || task.unresolved_outbox_id.is_some()
                || outbox.run_id.as_deref() != Some(evidence.run_id.as_str())
                || outbox.worker_id.as_deref() != Some(evidence.worker_id.as_str())
            {
                let resolved_status = if outbox.status == OutboxDispatchStatus::EffectUnknown {
                    OutboxDispatchStatus::ReconciledConfirmed
                } else {
                    OutboxDispatchStatus::DispatchedEffectConfirmed
                };
                self.workflow_store
                    .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                        binding_id: request.binding_id.clone(),
                        thread_uuid: request.thread_uuid.clone(),
                        job_id: request.job_id.clone(),
                        expected_job_revision: current_adm.job.job_revision,
                        expected_ledger_revision: None,
                        outbox_id: outbox.outbox_id,
                        status: resolved_status,
                        next_task_stage: fenced_stage,
                        run_id: Some(evidence.run_id.clone()),
                        worker_id: Some(evidence.worker_id.clone()),
                        task_worktree_id: Some(evidence.task_worktree_id.clone()),
                        head_commit: Some(evidence.head_commit.clone()),
                        tree_sha: Some(evidence.tree_sha.clone()),
                        diff_digest: Some(evidence.diff_digest.clone()),
                        report_artifact_id: Some(evidence.report_artifact_id.clone()),
                        observed_model: evidence.observed_model.clone(),
                        observed_effort: evidence.observed_effort.clone(),
                        receipt_id: None,
                        diagnostic: None,
                    })?;
            }
        }

        let effect_already_confirmed =
            current_adm.runtime_state.pending_effects.values().any(|e| {
                e.request_key == command_key
                    && e.status == PendingControllerEffectStatus::EffectConfirmed
                    && e.external_correlation_id.as_deref() == Some(evidence.run_id.as_str())
            });
        let artifacts_already_recorded = current_adm
            .runtime_state
            .get_attempt_artifacts(&task.task_id, 1)
            .is_some_and(|a| {
                a.run_id == evidence.run_id
                    && a.worker_id == evidence.worker_id
                    && a.task_worktree_id == evidence.task_worktree_id
                    && a.observed_model == evidence.observed_model
                    && a.observed_effort == evidence.observed_effort
                    && a.committed_head_commit.as_deref() == Some(evidence.head_commit.as_str())
                    && a.committed_tree_sha.as_deref() == Some(evidence.tree_sha.as_str())
            });

        if !effect_already_confirmed || !artifacts_already_recorded {
            self.policy_store.mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    if let Some(eff) = state
                        .pending_effects
                        .values_mut()
                        .find(|e| e.request_key == command_key)
                    {
                        if eff.status != PendingControllerEffectStatus::EffectConfirmed {
                            eff.status = PendingControllerEffectStatus::EffectConfirmed;
                            eff.external_correlation_id = Some(evidence.run_id.clone());
                            eff.updated_at_ms = request.now_ms;
                            state.record_role_completion(WorkflowRole::Coder, evidence.duration_ms);
                        }
                    } else {
                        if state.total_task_attempts_reserved
                            >= state.envelope.max_total_task_attempts_per_job
                        {
                            return Err(NativeError::new(
                                NativeErrorCode::ValidationFailed,
                                format!(
                                    "job `{}` exhausted max_total_task_attempts_per_job ({})",
                                    state.job_id, state.envelope.max_total_task_attempts_per_job
                                ),
                            ));
                        }
                        state.reserve_role_call(WorkflowRole::Coder)?;
                        state.total_task_attempts_reserved =
                            state.total_task_attempts_reserved.saturating_add(1);
                        state.record_role_completion(WorkflowRole::Coder, evidence.duration_ms);
                        let effect_id = deterministic_prefixed_id(
                            "threadlane-native:coworker-controller-effect:v1",
                            "eff",
                            &[&state.job_id, &task.task_id, command_key],
                        );
                        state.pending_effects.insert(
                            effect_id.clone(),
                            PendingControllerEffect {
                                effect_id,
                                request_key: command_key.to_string(),
                                job_id: state.job_id.clone(),
                                task_id: Some(task.task_id.clone()),
                                attempt_no: Some(1),
                                kind: PendingControllerEffectKind::CoderDispatch,
                                status: PendingControllerEffectStatus::EffectConfirmed,
                                subject_digest: approved_task.task_digest.clone(),
                                external_correlation_id: Some(evidence.run_id.clone()),
                                created_at_ms: request.now_ms,
                                updated_at_ms: request.now_ms,
                                diagnostic: None,
                            },
                        );
                    }
                    let artifact_rec = TaskAttemptExecutionArtifacts {
                        task_id: task.task_id.clone(),
                        attempt_no: 1,
                        run_id: evidence.run_id.clone(),
                        worker_id: evidence.worker_id.clone(),
                        task_worktree_id: evidence.task_worktree_id.clone(),
                        integration_worktree_id: evidence.integration_worktree_id.clone(),
                        execution_base_commit: evidence.execution_base_commit.clone(),
                        observed_provider: evidence.observed_provider.clone(),
                        observed_model: evidence.observed_model.clone(),
                        codex_launch_report: evidence.codex_launch_report.clone(),
                        codex_source_report_sha256: evidence.codex_source_report_sha256.clone(),
                        codex_source_effect_id: None,
                        observed_effort: evidence.observed_effort.clone(),
                        committed_head_commit: Some(evidence.head_commit.clone()),
                        committed_tree_sha: Some(evidence.tree_sha.clone()),
                        candidate_id: None,
                        check_evidence: Vec::new(),
                        test_evidence_summary: None,
                        review_subject: None,
                        reviewer_response_digest: None,
                        review_receipt: None,
                        integration_receipt: None,
                        is_derived_attempt: false,
                    };
                    state.attempt_artifacts.insert(
                        ControllerJobRuntimeState::artifact_key(&task.task_id, 1),
                        artifact_rec,
                    );
                    Ok(())
                },
            )?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn ensure_initial_coder_outbox_and_effect(
        &self,
        request: &CoworkerControllerStepRequest,
        current_adm: &VerifiedControllerAdmission,
        approved_task: &ApprovedTaskRecord,
        task: &CoworkerTaskRecord,
        command_key: &str,
        controller_instance_id: &str,
        lease_epoch: u64,
    ) -> Result<(CoworkerJobRecord, super::contracts::OutboxDispatchRecord), NativeError> {
        verify_no_conflicting_initial_coder_key(
            &current_adm.job,
            &current_adm.runtime_state,
            &task.task_id,
            command_key,
        )?;

        let (job_with_outbox, outbox) = if let Some(existing) =
            find_task_attempt_outbox(&current_adm.job, &task.task_id, command_key)
        {
            (current_adm.job.clone(), existing)
        } else {
            self.workflow_store
                .reserve_task_outbox(&ReserveTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: current_adm.job.job_revision,
                    expected_ledger_revision: None,
                    request_key: command_key.to_string(),
                    kind: OutboxDispatchKind::TaskAttemptDispatch,
                    task_id: task.task_id.clone(),
                    role: WorkflowRole::Coder,
                    subject_digest: approved_task.task_digest.clone(),
                    dependency_input_commit: None,
                    dependency_input_tree_sha: None,
                    accepted_predecessor_receipts: Vec::new(),
                })?
        };

        // Reserve role call & job attempt budget in ControllerPolicyStore only if not already reserved.
        self.policy_store.mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            controller_instance_id,
            lease_epoch,
            |state| {
                let already_reserved = state
                    .pending_effects
                    .values()
                    .any(|e| e.request_key == command_key);
                if !already_reserved {
                    if state.total_task_attempts_reserved
                        >= state.envelope.max_total_task_attempts_per_job
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            format!(
                                "job `{}` exhausted max_total_task_attempts_per_job ({})",
                                state.job_id, state.envelope.max_total_task_attempts_per_job
                            ),
                        ));
                    }
                    state.reserve_role_call(WorkflowRole::Coder)?;
                    state.total_task_attempts_reserved =
                        state.total_task_attempts_reserved.saturating_add(1);
                    let effect_id = deterministic_prefixed_id(
                        "threadlane-native:coworker-controller-effect:v1",
                        "eff",
                        &[&state.job_id, &task.task_id, command_key],
                    );
                    state.pending_effects.insert(
                        effect_id.clone(),
                        PendingControllerEffect {
                            effect_id,
                            request_key: command_key.to_string(),
                            job_id: state.job_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            attempt_no: Some(1),
                            kind: PendingControllerEffectKind::CoderDispatch,
                            status: PendingControllerEffectStatus::ReservedBeforeEffect,
                            subject_digest: approved_task.task_digest.clone(),
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

        Ok((job_with_outbox, outbox))
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_and_reconcile_initial_coder_task(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        host_client: &ApprovedHostClient,
        approval: &ApprovalRecord,
        approved_task: &ApprovedTaskRecord,
        candidate_task: &CoworkerTaskRecord,
        controller_instance_id: &str,
        lease_epoch: u64,
        dispatched_task_ids: &mut Vec<String>,
        reconciled_ready_for_review_task_ids: &mut Vec<String>,
        fenced_task_ids: &mut Vec<String>,
    ) -> Result<(), NativeError> {
        let Some((reserved_job_revision, command_key, outbox_id)) = self
            .reserve_initial_coder_dispatch_before_effect(
                guard,
                request,
                approved_task,
                candidate_task,
                controller_instance_id,
                lease_epoch,
            )?
        else {
            return Ok(());
        };

        let dispatch_res = host_client
            .dispatch_task(&approval.approval_id, &candidate_task.task_id)
            .await;

        // Re-verify physical OS ownership immediately after `.await`.
        self.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        match dispatch_res {
            Ok(post_snapshot) => {
                verify_snapshot_matches_approved_task(&post_snapshot, approved_task)?;
                if !dispatched_task_ids.contains(&candidate_task.task_id) {
                    dispatched_task_ids.push(candidate_task.task_id.clone());
                }
                self.reconcile_single_task_snapshot(
                    guard,
                    request,
                    approval,
                    approved_task,
                    &post_snapshot,
                    controller_instance_id,
                    lease_epoch,
                    &mut Vec::new(),
                    reconciled_ready_for_review_task_ids,
                    fenced_task_ids,
                )?;
                Ok(())
            }
            Err(host_err) => {
                if is_host_error_pre_effect_refusal(&host_err) {
                    let diag = sanitize_diagnostic_text(&host_err.to_string());
                    self.workflow_store.reconcile_or_finalize_outbox(
                        &FinalizeTaskOutboxRequest {
                            binding_id: request.binding_id.clone(),
                            thread_uuid: request.thread_uuid.clone(),
                            job_id: request.job_id.clone(),
                            expected_job_revision: reserved_job_revision,
                            expected_ledger_revision: None,
                            outbox_id,
                            status: OutboxDispatchStatus::ReconciledNotExecuted,
                            next_task_stage: CoworkerTaskStage::ReadyToDispatch,
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
                            diagnostic: Some(diag.clone()),
                        },
                    )?;
                    self.policy_store.mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        |state| {
                            if let Some(eff) = state
                                .pending_effects
                                .values_mut()
                                .find(|e| e.request_key == command_key)
                            {
                                eff.status = PendingControllerEffectStatus::RefusedBeforeEffect;
                                eff.diagnostic = Some(diag.clone());
                                eff.updated_at_ms = request.now_ms;
                            }
                            Ok(())
                        },
                    )?;
                    Err(map_host_error(host_err))
                } else {
                    let diag = sanitize_diagnostic_text(&host_err.to_string());
                    let _ = self.workflow_store.reconcile_or_finalize_outbox(
                        &FinalizeTaskOutboxRequest {
                            binding_id: request.binding_id.clone(),
                            thread_uuid: request.thread_uuid.clone(),
                            job_id: request.job_id.clone(),
                            expected_job_revision: reserved_job_revision,
                            expected_ledger_revision: None,
                            outbox_id,
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
                            diagnostic: Some(diag.clone()),
                        },
                    );
                    let _ = self.policy_store.mutate_job_runtime(
                        &request.binding_id,
                        &request.job_id,
                        controller_instance_id,
                        lease_epoch,
                        |state| {
                            if let Some(eff) = state
                                .pending_effects
                                .values_mut()
                                .find(|e| e.request_key == command_key)
                            {
                                eff.status = PendingControllerEffectStatus::EffectUnknown;
                                eff.diagnostic = Some(diag.clone());
                                eff.updated_at_ms = request.now_ms;
                            }
                            Ok(())
                        },
                    );
                    Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        format!(
                            "dispatch_task outcome uncertain for `{}`: {diag}",
                            candidate_task.task_id
                        ),
                    ))
                }
            }
        }
    }

    #[inline(never)]
    fn reserve_initial_coder_dispatch_before_effect(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        approved_task: &ApprovedTaskRecord,
        candidate_task: &CoworkerTaskRecord,
        controller_instance_id: &str,
        lease_epoch: u64,
    ) -> Result<Option<(u64, String, String)>, NativeError> {
        self.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        let current_adm = self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;
        if require_active_lease_epoch(&current_adm.runtime_state, controller_instance_id)?
            != lease_epoch
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller lease_epoch changed before task dispatch",
            ));
        }

        if candidate_task.task_id != approved_task.task_id {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate_task.task_id does not match approved_task.task_id",
            ));
        }
        verify_approved_task_matches_coder_choice(approved_task, candidate_task)?;
        let command_key = approved_task_command_key(approved_task)?;
        verify_no_conflicting_initial_coder_key(
            &current_adm.job,
            &current_adm.runtime_state,
            &candidate_task.task_id,
            &command_key,
        )?;

        let current_task = current_adm
            .job
            .tasks
            .iter()
            .find(|t| t.task_id == candidate_task.task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{}` not found in job", candidate_task.task_id),
                )
            })?;
        if !constant_time_str_eq(&current_task.task_digest, &candidate_task.task_digest)
            || current_task.role != candidate_task.role
            || current_task.difficulty != candidate_task.difficulty
            || current_task.selected_choice != candidate_task.selected_choice
            || current_task.depends_on != candidate_task.depends_on
            || current_task.write_paths != candidate_task.write_paths
            || current_task.owned_interfaces != candidate_task.owned_interfaces
            || current_task.consumed_interfaces != candidate_task.consumed_interfaces
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` immutable specification drifted before dispatch",
                    candidate_task.task_id
                ),
            ));
        }

        if current_adm.runtime_state.stop_requested
            || current_adm.job.phase != CoworkerJobPhase::Executing
            || current_task.stage != CoworkerTaskStage::ReadyToDispatch
            || current_task != candidate_task
            || current_task.unresolved_outbox_id.is_some()
            || task_has_derived_attempt_or_post_initial_state(
                current_task,
                &current_adm.runtime_state,
            )
            || count_active_coder_executions(&current_adm.job, &current_adm.runtime_state)
                >= current_adm.approval.max_parallel
            || is_task_fenced_by_hold_or_sibling(
                current_task,
                &current_adm.job,
                &current_adm.runtime_state,
                &current_adm.approval.approval_id,
                self.workflow_store.plans_dir(),
            )?
        {
            return Ok(None);
        }

        // Intent-before-host-dispatch: reserve role call, job attempt cap, and task outbox.
        let (job_after_reserve, outbox) = self.ensure_initial_coder_outbox_and_effect(
            request,
            &current_adm,
            approved_task,
            current_task,
            &command_key,
            controller_instance_id,
            lease_epoch,
        )?;
        if outbox.status != OutboxDispatchStatus::ReservedBeforeEffect {
            return Ok(None);
        }

        self.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        Ok(Some((
            job_after_reserve.job_revision,
            command_key,
            outbox.outbox_id,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_step_report(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        host_client: &ApprovedHostClient,
        approval: &ApprovalRecord,
        approved_by_id: &BTreeMap<String, ApprovedTaskRecord>,
        lease_epoch: u64,
        controller_instance_id: String,
        dispatched_task_ids: Vec<String>,
        reconciled_ready_for_review_task_ids: Vec<String>,
        promoted_dependency_ready_task_ids: Vec<String>,
        mut fenced_task_ids: Vec<String>,
    ) -> Result<CoworkerControllerStepReport, NativeError> {
        let mut observations = Vec::new();
        let mut unknown_effect_task_ids = Vec::new();
        let mut active_running_count = 0usize;

        let final_adm = Box::new(self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?);
        if require_active_lease_epoch(&final_adm.runtime_state, &controller_instance_id)?
            != lease_epoch
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "controller lease_epoch drifted before completing step report",
            ));
        }

        for task in &final_adm.job.tasks {
            self.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;

            let mut snap = host_client
                .get_task(&approval.approval_id, &task.task_id)
                .await
                .map_err(map_host_error)?;

            self.verify_physical_ownership(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;

            let approved_task = approved_by_id.get(&task.task_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("task `{}` missing from ApprovalRecord", task.task_id),
                )
            })?;
            verify_snapshot_matches_approved_task(&snap, approved_task)?;

            // If a newly dispatched initial task finished during the loop, reconcile it so the final report
            // reflects its terminal ReadyForReview state.
            if !task_has_derived_attempt_or_post_initial_state(task, &final_adm.runtime_state)
                && matches!(
                    task.stage,
                    CoworkerTaskStage::DispatchReserved
                        | CoworkerTaskStage::Running
                        | CoworkerTaskStage::Held
                )
                && matches!(
                    snap.status,
                    TaskExecutionStatus::ReadyForReview
                        | TaskExecutionStatus::Failed
                        | TaskExecutionStatus::Cancelled
                        | TaskExecutionStatus::TimedOut
                )
            {
                self.reconcile_single_task_snapshot(
                    guard,
                    request,
                    approval,
                    approved_task,
                    &snap,
                    &controller_instance_id,
                    lease_epoch,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &mut fenced_task_ids,
                )?;
                snap = host_client
                    .get_task(&approval.approval_id, &task.task_id)
                    .await
                    .map_err(map_host_error)?;
                self.verify_physical_ownership(
                    guard,
                    &request.binding_id,
                    &request.thread_uuid,
                    &request.job_id,
                )?;
            }

            self.record_task_step_observation(
                request,
                &final_adm.runtime_state,
                &task.task_id,
                approved_task,
                &snap,
                &mut fenced_task_ids,
                &mut unknown_effect_task_ids,
                &mut active_running_count,
                &mut observations,
            )?;
        }

        drop(final_adm);
        self.finalize_step_report(
            guard,
            request,
            approval,
            controller_instance_id,
            dispatched_task_ids,
            reconciled_ready_for_review_task_ids,
            promoted_dependency_ready_task_ids,
            fenced_task_ids,
            unknown_effect_task_ids,
            active_running_count,
            observations,
        )
    }

    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    fn record_task_step_observation(
        &self,
        request: &CoworkerControllerStepRequest,
        fallback_runtime: &ControllerJobRuntimeState,
        task_id: &str,
        approved_task: &ApprovedTaskRecord,
        snap: &TaskStateSnapshot,
        fenced_task_ids: &mut Vec<String>,
        unknown_effect_task_ids: &mut Vec<String>,
        active_running_count: &mut usize,
        observations: &mut Vec<CoworkerControllerTaskObservation>,
    ) -> Result<(), NativeError> {
        let refreshed_ledger = self
            .workflow_store
            .read_binding_ledger(&request.binding_id)?;
        let refreshed_job = refreshed_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "job disappeared during step report",
                )
            })?;
        let refreshed_task = refreshed_job
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "task disappeared during step report",
                )
            })?;
        let refreshed_runtime_opt = self
            .policy_store
            .get_job_runtime_readonly(&request.binding_id, &request.job_id)?;
        let refreshed_runtime = refreshed_runtime_opt.as_ref().unwrap_or(fallback_runtime);

        let fenced = fenced_task_ids.iter().any(|id| id == task_id)
            || refreshed_task.stage == CoworkerTaskStage::Held
            || !refreshed_task.active_hold_ids.is_empty();
        if fenced && !fenced_task_ids.iter().any(|id| id == task_id) {
            fenced_task_ids.push(task_id.to_string());
        }

        if (refreshed_task.stage == CoworkerTaskStage::EffectUnknown
            || snap.status == TaskExecutionStatus::Unknown)
            && !unknown_effect_task_ids.iter().any(|id| id == task_id)
        {
            unknown_effect_task_ids.push(task_id.to_string());
        }

        let has_derived_state =
            task_has_derived_attempt_or_post_initial_state(refreshed_task, refreshed_runtime);
        if is_task_coder_execution_active(refreshed_task, refreshed_job, refreshed_runtime)
            || (!has_derived_state
                && matches!(
                    snap.status,
                    TaskExecutionStatus::Queued
                        | TaskExecutionStatus::Claimed
                        | TaskExecutionStatus::Running
                        | TaskExecutionStatus::CancelRequested
                ))
        {
            *active_running_count += 1;
        }

        let command_key = approved_task_command_key(approved_task)?;
        observations.push(CoworkerControllerTaskObservation {
            task_id: task_id.to_string(),
            stage: refreshed_task.stage,
            host_status: snap.status,
            command_key,
            run_id: snap.run_id.clone(),
            worker_id: snap.worker_id.clone(),
            task_worktree_id: snap
                .worker
                .as_ref()
                .and_then(|w| w.worktree.task_worktree_id.clone()),
            observed_model: snap.worker.as_ref().and_then(|w| w.observed_model.clone()),
            observed_effort: snap.worker.as_ref().and_then(|w| w.observed_effort.clone()),
            original_worker_provider_health: refreshed_runtime
                .original_worker_provider_health
                .get(task_id)
                .cloned(),
            watchdog_attention: snap
                .worker
                .as_ref()
                .and_then(|w| w.progress.watchdog_attention.clone()),
            fenced,
        });
        Ok(())
    }

    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    fn finalize_step_report(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerControllerStepRequest,
        approval: &ApprovalRecord,
        controller_instance_id: String,
        dispatched_task_ids: Vec<String>,
        reconciled_ready_for_review_task_ids: Vec<String>,
        promoted_dependency_ready_task_ids: Vec<String>,
        fenced_task_ids: Vec<String>,
        unknown_effect_task_ids: Vec<String>,
        active_running_count: usize,
        observations: Vec<CoworkerControllerTaskObservation>,
    ) -> Result<CoworkerControllerStepReport, NativeError> {
        let refreshed_adm = self.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;
        let final_lease_epoch =
            require_active_lease_epoch(&refreshed_adm.runtime_state, &controller_instance_id)?;

        let status = classify_phase1_status(
            &refreshed_adm.job,
            &refreshed_adm.runtime_state,
            active_running_count,
            &fenced_task_ids,
            &unknown_effect_task_ids,
        );

        Ok(CoworkerControllerStepReport {
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            job_id: request.job_id.clone(),
            approval_id: approval.approval_id.clone(),
            job_revision: refreshed_adm.job.job_revision,
            lease_epoch: final_lease_epoch,
            controller_instance_id,
            status,
            max_parallel: approval.max_parallel,
            active_running_count,
            dispatched_task_ids,
            reconciled_ready_for_review_task_ids,
            promoted_dependency_ready_task_ids,
            fenced_task_ids,
            unknown_effect_task_ids,
            tasks: observations,
        })
    }
}

fn require_active_lease_epoch(
    state: &ControllerJobRuntimeState,
    expected_instance_id: &str,
) -> Result<u64, NativeError> {
    let lease = state.lease.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "controller runtime state for job `{}` has no active lease record",
                state.job_id
            ),
        )
    })?;
    if !lease.active
        || lease.instance_id != expected_instance_id
        || lease.lease_epoch == 0
        || lease.expires_at_ms <= lease.acquired_at_ms
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "controller lease for job `{}` is not active for instance `{expected_instance_id}`",
                state.job_id
            ),
        ));
    }
    Ok(lease.lease_epoch)
}

struct VerifiedReadyForReviewEvidence {
    run_id: String,
    worker_id: String,
    task_worktree_id: String,
    integration_worktree_id: String,
    execution_base_commit: String,
    observed_provider: String,
    head_commit: String,
    tree_sha: String,
    diff_digest: String,
    report_artifact_id: String,
    observed_model: Option<String>,
    codex_launch_report: Option<threadlane_workers::WorkerExecutionReport>,
    codex_source_report_sha256: Option<String>,
    observed_effort: Option<String>,
    duration_ms: u64,
}

fn extract_verified_ready_for_review_evidence(
    sessions_dir: &Path,
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    selected_choice: &SelectedRoleChoice,
    snapshot: &TaskStateSnapshot,
) -> Result<VerifiedReadyForReviewEvidence, NativeError> {
    let run_id = snapshot.run_id.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "ReadyForReview snapshot for task `{}` is missing run_id",
                approved_task.task_id
            ),
        )
    })?;
    let worker_id = snapshot.worker_id.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "ReadyForReview snapshot for task `{}` is missing worker_id",
                approved_task.task_id
            ),
        )
    })?;
    let worker = snapshot.worker.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "ReadyForReview snapshot for task `{}` is missing WorkerSnapshot",
                approved_task.task_id
            ),
        )
    })?;

    if worker.run_id != run_id || worker.worker_id != worker_id {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "WorkerSnapshot run_id or worker_id does not match TaskStateSnapshot receipt",
        ));
    }

    let source_report =
        if approved_task.provider == Some(threadlane_workers::WorkerProvider::CodexCli) {
            Some(
                threadlane_host::load_verified_codex_task_report(
                    approval,
                    &sessions_dir.join(&approval.approval_id),
                    approved_task,
                    worker,
                )
                .map_err(|e| NativeError::new(NativeErrorCode::ValidationFailed, e))?,
            )
        } else {
            None
        };
    let codex_source_report_sha256 = source_report
        .as_ref()
        .map(|r| serde_json::to_vec(r).map(|bytes| sha256_hex(&bytes)))
        .transpose()
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to digest immutable worker report",
            )
        })?;
    let codex_launch_report = source_report
        .as_ref()
        .map(|r| super::controller_policy::compact_codex_launch_report(&r.worker_report));
    let observed_model = worker.observed_model.clone();
    if selected_choice.model_arg != approved_task.base_model
        || (codex_launch_report.is_none()
            && observed_model.as_deref() != Some(approved_task.base_model.as_str()))
        || observed_model
            .as_deref()
            .is_some_and(|v| v != approved_task.base_model)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "worker model evidence does not match the frozen Coder choice",
        ));
    }
    if let Some(ref expected_effort) = selected_choice.effort {
        if (codex_launch_report.is_none()
            && worker.observed_effort.as_deref() != Some(expected_effort.as_str()))
            || worker
                .observed_effort
                .as_deref()
                .is_some_and(|v| v != expected_effort)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "worker effort evidence does not match the frozen Coder choice",
            ));
        }
    }

    let task_worktree_id = worker.worktree.task_worktree_id.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!("ReadyForReview worker `{worker_id}` is missing task_worktree_id",),
        )
    })?;
    let integration_worktree_id = worker.worktree.run_worktree_id.clone().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!("ReadyForReview worker `{worker_id}` is missing run_worktree_id",),
        )
    })?;
    let task_worktree_path_str = worker.worktree.task_worktree_path.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            format!("ReadyForReview worker `{worker_id}` is missing task_worktree_path",),
        )
    })?;
    let checkout_path = Path::new(task_worktree_path_str);
    if !checkout_path.exists() {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task worktree checkout path for `{}` does not exist on disk",
                approved_task.task_id
            ),
        ));
    }

    let head_commit =
        resolve_exact_commit_sha(checkout_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).map_err(
            |err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to resolve task worktree HEAD commit: {err}"),
                )
            },
        )?;
    let tree_sha =
        resolve_exact_tree_sha(checkout_path, &head_commit, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to resolve task worktree HEAD tree SHA: {err}"),
                )
            })?;

    let diff_out = run_sanitized_git(
        checkout_path,
        &["diff", "--binary", &worker.worktree.base_commit],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to compute git diff in task worktree: {err}"),
        )
    })?;
    let diff_digest = sha256_hex(diff_out.as_bytes());

    let report_artifact_id = deterministic_prefixed_id(
        "threadlane-native:coworker-coder-attempt-report:v1",
        "art",
        &[
            &approval.approval_id,
            &approved_task.task_id,
            &run_id,
            &worker_id,
            &task_worktree_id,
            &diff_digest,
        ],
    );
    let duration_ms = match (worker.started_at_ms, worker.completed_at_ms) {
        (Some(s), Some(f)) => f.saturating_sub(s).max(0) as u64,
        _ => 0,
    };

    Ok(VerifiedReadyForReviewEvidence {
        run_id,
        worker_id,
        task_worktree_id,
        integration_worktree_id,
        execution_base_commit: worker.worktree.base_commit.clone(),
        observed_provider: worker.provider.clone(),
        head_commit,
        tree_sha,
        diff_digest,
        report_artifact_id,
        observed_model,
        codex_launch_report,
        codex_source_report_sha256,
        observed_effort: worker.observed_effort.clone(),
        duration_ms,
    })
}

fn verify_approved_task_matches_coder_choice(
    approved_task: &ApprovedTaskRecord,
    job_task: &CoworkerTaskRecord,
) -> Result<(), NativeError> {
    if approved_task.role != WorkflowRole::Coder {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "approved task `{}` has role `{:?}` (only WorkflowRole::Coder is supported)",
                approved_task.task_id, approved_task.role
            ),
        ));
    }
    if !approved_task.is_frozen_headless_worker() {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "approved task `{}` uses native Codex route (`{:?}`), which is unsupported for headless controller execution",
                approved_task.task_id, approved_task.adapter
            ),
        ));
    }
    if job_task.selected_choice.route != RoleExecutionRoute::HeadlessWorker {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "job task `{}` route `{:?}` is not HeadlessWorker",
                job_task.task_id, job_task.selected_choice.route
            ),
        ));
    }
    if approved_task.tool_id != job_task.selected_choice.tool_id
        || approved_task.adapter != job_task.selected_choice.adapter
        || approved_task.base_model != job_task.selected_choice.model_arg
        || approved_task.effort != job_task.selected_choice.effort
        || approved_task.allowed_write_paths != job_task.write_paths
        || approved_task.depends_on != job_task.depends_on
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "approved task `{}` model/tool/effort/scope pin does not match job task",
                approved_task.task_id
            ),
        ));
    }
    let _ = approved_task_command_key(approved_task)?;
    if approved_task
        .profile_id
        .as_deref()
        .is_none_or(|p| p.trim().is_empty())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "approved task `{}` is missing frozen profile_id",
                approved_task.task_id
            ),
        ));
    }
    Ok(())
}

fn approved_task_command_key(approved_task: &ApprovedTaskRecord) -> Result<String, NativeError> {
    let key = approved_task
        .command_key
        .as_deref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "approved task `{}` is missing frozen command_key",
                    approved_task.task_id
                ),
            )
        })?
        .to_string();
    validate_bounded_identifier(&key, "approved_task.command_key", MAX_COWORKER_ID_LEN)?;
    Ok(key)
}

fn verify_snapshot_matches_approved_task(
    snapshot: &TaskStateSnapshot,
    approved_task: &ApprovedTaskRecord,
) -> Result<(), NativeError> {
    if snapshot.task_id != approved_task.task_id
        || !constant_time_str_eq(&snapshot.task_digest, &approved_task.task_digest)
        || snapshot.command_key != approved_task.command_key
        || snapshot.profile_id != approved_task.profile_id
        || snapshot.adapter != approved_task.adapter
        || snapshot.provider != approved_task.provider
        || snapshot.depends_on != approved_task.depends_on
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "live host TaskStateSnapshot for `{}` does not match frozen ApprovedTaskRecord",
                approved_task.task_id
            ),
        ));
    }
    Ok(())
}

fn verify_no_conflicting_initial_coder_key(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    task_id: &str,
    expected_command_key: &str,
) -> Result<(), NativeError> {
    for outbox in &job.outbox {
        if outbox.task_id == task_id
            && outbox.kind == OutboxDispatchKind::TaskAttemptDispatch
            && outbox.attempt_no == 1
            && outbox.request_key != expected_command_key
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{task_id}` already has a TaskAttemptDispatch outbox with request_key `{}` != frozen command_key `{expected_command_key}`; refusing to replay or mint a new attempt reservation",
                    outbox.request_key
                ),
            ));
        }
    }
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::CoderDispatch
            && eff.task_id.as_deref() == Some(task_id)
            && eff.attempt_no == Some(1)
            && eff.request_key != expected_command_key
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{task_id}` already has an initial CoderDispatch effect with request_key `{}` != frozen command_key `{expected_command_key}`",
                    eff.request_key
                ),
            ));
        }
    }
    Ok(())
}

fn find_task_attempt_outbox(
    job: &CoworkerJobRecord,
    task_id: &str,
    command_key: &str,
) -> Option<super::contracts::OutboxDispatchRecord> {
    job.outbox
        .iter()
        .find(|o| {
            o.task_id == task_id
                && o.kind == OutboxDispatchKind::TaskAttemptDispatch
                && o.request_key == command_key
        })
        .cloned()
}

pub(crate) fn task_has_derived_attempt_or_post_initial_state(
    task: &CoworkerTaskRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    task.current_attempt_no > 1
        || task.attempts.iter().any(|a| a.attempt_no > 1)
        || !task.candidates.is_empty()
        || matches!(
            task.stage,
            CoworkerTaskStage::ReviewReserved
                | CoworkerTaskStage::UnderReview
                | CoworkerTaskStage::CorrectInScopeRequired
                | CoworkerTaskStage::Accepted
                | CoworkerTaskStage::IntegrationReserved
                | CoworkerTaskStage::Integrated
                | CoworkerTaskStage::Blocked
        )
        || runtime_state
            .derived_attempts
            .iter()
            .any(|d| d.task_id == task.task_id)
        || runtime_state.attempt_artifacts.values().any(|a| {
            a.task_id == task.task_id
                && (a.attempt_no > 1
                    || a.is_derived_attempt
                    || a.candidate_id.is_some()
                    || a.review_subject.is_some())
        })
}

/// Returns `true` when the task's current `Coder` attempt has already completed and reconciled its
/// verified terminal execution evidence into [`ControllerJobRuntimeState`]. Cached Coder stages
/// require unpromoted initial evidence; a hold after Coder completion retains its post-Coder stage.
pub(crate) fn has_reconciled_terminal_coder_attempt(
    task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    let attempt_no = task.current_attempt_no;
    if attempt_no == 0 || task.unresolved_outbox_id.is_some() {
        return false;
    }
    let stage_before_hold = if task.stage == CoworkerTaskStage::Held {
        task.stage_before_hold
    } else {
        Some(task.stage)
    };
    let post_coder_stage = matches!(
        stage_before_hold,
        Some(
            CoworkerTaskStage::ReadyForReview
                | CoworkerTaskStage::ReviewReserved
                | CoworkerTaskStage::UnderReview
                | CoworkerTaskStage::CorrectInScopeRequired
                | CoworkerTaskStage::Accepted
                | CoworkerTaskStage::IntegrationReserved
                | CoworkerTaskStage::Integrated
                | CoworkerTaskStage::Blocked
        )
    );
    // Promoted artifacts cannot rehabilitate a stale Running (or Held-from-Running) stage.
    // Verification records runtime candidates before Reviewer adds ledger candidates.
    if !post_coder_stage
        && (attempt_no != 1
            || task.active_candidate_id.is_some()
            || task.accepted_candidate_id.is_some()
            || task_has_derived_attempt_or_post_initial_state(task, runtime_state)
            || job.outbox.iter().any(|o| {
                // Dependency preparation is retained before the first Coder reservation.
                // Its producer confirms it synchronously and creates no controller effect.
                let completed_pre_coder_dependency = o.kind
                    == OutboxDispatchKind::DependencyPreparation
                    && o.attempt_no == 1
                    && o.role == WorkflowRole::Coder
                    && o.status == OutboxDispatchStatus::DispatchedEffectConfirmed
                    && !task.depends_on.is_empty()
                    && task.dependency_input_commit.is_some()
                    && task.dependency_input_tree_sha.is_some()
                    && job.outbox.iter().any(|coder| {
                        coder.task_id == task.task_id
                            && coder.attempt_no == 1
                            && coder.kind == OutboxDispatchKind::TaskAttemptDispatch
                            && o.updated_at_job_revision < coder.reserved_at_job_revision
                    });
                o.task_id == task.task_id
                    && (o.attempt_no > attempt_no
                        || (o.kind != OutboxDispatchKind::TaskAttemptDispatch
                            && !completed_pre_coder_dependency))
            })
            || runtime_state.pending_effects.values().any(|eff| {
                eff.task_id.as_deref() == Some(task.task_id.as_str())
                    && (eff.attempt_no != Some(attempt_no)
                        || eff.kind != PendingControllerEffectKind::CoderDispatch)
            }))
    {
        return false;
    }
    let has_blocking_coder_outbox = job.outbox.iter().any(|o| {
        o.task_id == task.task_id
            && o.attempt_no == attempt_no
            && o.kind == OutboxDispatchKind::TaskAttemptDispatch
            && o.status.blocks_replay_and_new_dispatch()
    });
    if has_blocking_coder_outbox {
        return false;
    }
    let Some(artifacts) = runtime_state.get_attempt_artifacts(&task.task_id, attempt_no) else {
        return false;
    };
    if (!artifacts.is_derived_attempt
        && (artifacts.committed_head_commit.is_none() || artifacts.committed_tree_sha.is_none()))
        || (!post_coder_stage
            && (artifacts.is_derived_attempt
                || !artifacts.check_evidence.is_empty()
                || artifacts.test_evidence_summary.is_some()
                || artifacts.reviewer_response_digest.is_some()
                || artifacts.review_receipt.is_some()
                || artifacts.integration_receipt.is_some()))
    {
        return false;
    }
    let Some(attempt_record) = task
        .attempts
        .iter()
        .rfind(|a| a.attempt_no == attempt_no && a.role == WorkflowRole::Coder)
    else {
        return false;
    };
    if attempt_record.run_id.as_deref() != Some(artifacts.run_id.as_str())
        || attempt_record.worker_id.as_deref() != Some(artifacts.worker_id.as_str())
        || (artifacts.is_derived_attempt
            && (attempt_record.head_commit.is_none() || attempt_record.tree_sha.is_none()))
    {
        return false;
    }
    let derived_receipt = if artifacts.is_derived_attempt {
        let Some(receipt) = runtime_state.derived_attempts.iter().find(|receipt| {
            receipt.task_id == task.task_id
                && receipt.attempt_no == attempt_no
                && receipt.run_id == artifacts.run_id
                && receipt.worker_id == artifacts.worker_id
                && receipt.task_worktree_id == artifacts.task_worktree_id
        }) else {
            return false;
        };
        Some(receipt)
    } else {
        None
    };
    let terminal_correlation = if derived_receipt.is_some() {
        let Some(report_id) = attempt_record.report_artifact_id.as_deref() else {
            return false;
        };
        report_id
    } else {
        artifacts.run_id.as_str()
    };
    let has_confirmed_matching_outbox = job.outbox.iter().any(|o| {
        o.outbox_id == attempt_record.outbox_id
            && o.request_key == attempt_record.request_key
            && o.task_id == task.task_id
            && o.attempt_no == attempt_no
            && o.kind == OutboxDispatchKind::TaskAttemptDispatch
            && matches!(
                o.status,
                OutboxDispatchStatus::DispatchedEffectConfirmed
                    | OutboxDispatchStatus::ReconciledConfirmed
            )
            && o.run_id.as_deref() == Some(artifacts.run_id.as_str())
            && o.worker_id.as_deref() == Some(artifacts.worker_id.as_str())
            && o.receipt_id.as_deref() == derived_receipt.map(|r| r.receipt_id.as_str())
            && derived_receipt.is_none_or(|r| o.subject_digest == r.receipt_digest)
    });
    if !has_confirmed_matching_outbox {
        return false;
    }
    let has_blocking_coder_effect = runtime_state.pending_effects.values().any(|eff| {
        eff.kind == PendingControllerEffectKind::CoderDispatch
            && eff.task_id.as_deref() == Some(task.task_id.as_str())
            && eff.attempt_no == Some(attempt_no)
            && eff.status.blocks_replay()
    });
    if has_blocking_coder_effect {
        return false;
    }
    runtime_state.pending_effects.values().any(|eff| {
        eff.kind == PendingControllerEffectKind::CoderDispatch
            && eff.task_id.as_deref() == Some(task.task_id.as_str())
            && eff.attempt_no == Some(attempt_no)
            && eff.request_key == attempt_record.request_key
            && eff.status == PendingControllerEffectStatus::EffectConfirmed
            && eff.external_correlation_id.as_deref() == Some(terminal_correlation)
            && derived_receipt.is_none_or(|r| eff.subject_digest == r.receipt_digest)
    })
}

pub(crate) fn is_task_coder_execution_active(
    task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    let held_coder_attempt_started = task.stage == CoworkerTaskStage::Held
        && (matches!(
            task.stage_before_hold,
            Some(
                CoworkerTaskStage::DispatchReserved
                    | CoworkerTaskStage::Running
                    | CoworkerTaskStage::EffectUnknown
            )
        ) || ((task.current_attempt_no > 0 || !task.attempts.is_empty())
            && !task
                .candidates
                .iter()
                .any(|c| c.attempt_no == task.current_attempt_no)));
    if matches!(
        task.stage,
        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
    ) || held_coder_attempt_started
    {
        return !has_reconciled_terminal_coder_attempt(task, job, runtime_state);
    }
    let has_active_coder_outbox = job.outbox.iter().any(|o| {
        o.task_id == task.task_id
            && o.kind == OutboxDispatchKind::TaskAttemptDispatch
            && matches!(
                o.status,
                OutboxDispatchStatus::ReservedBeforeEffect | OutboxDispatchStatus::EffectUnknown
            )
    });
    if has_active_coder_outbox {
        return true;
    }
    let has_active_coder_effect = runtime_state.pending_effects.values().any(|eff| {
        eff.kind == PendingControllerEffectKind::CoderDispatch
            && eff.task_id.as_deref() == Some(task.task_id.as_str())
            && matches!(
                eff.status,
                PendingControllerEffectStatus::ReservedBeforeEffect
                    | PendingControllerEffectStatus::EffectUnknown
            )
    });
    if has_active_coder_effect {
        return true;
    }
    false
}

pub(crate) fn count_active_coder_executions(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> usize {
    job.tasks
        .iter()
        .filter(|task| is_task_coder_execution_active(task, job, runtime_state))
        .count()
}

pub(crate) fn is_task_fenced_by_hold_or_sibling(
    candidate_task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    approval_id: &str,
    plans_dir: &Path,
) -> Result<bool, NativeError> {
    if runtime_state.stop_requested || job.phase != CoworkerJobPhase::Executing {
        return Ok(true);
    }
    if candidate_task.stage == CoworkerTaskStage::Held || !candidate_task.active_hold_ids.is_empty()
    {
        return Ok(true);
    }

    let change_summary = match ConversationChangeStore::open_readonly(plans_dir).and_then(|store| {
        store.unresolved_blocking_changes(&job.binding_id, Some(approval_id), None)
    }) {
        Ok(summary) => summary,
        Err(_) => return Ok(true),
    };
    if change_summary.approval_superseded {
        return Ok(true);
    }
    let unresolved_changes = change_summary.blocking_changes;

    let has_active_hold = job.holds.iter().any(|h| h.active);
    let has_impacted_sibling = job
        .tasks
        .iter()
        .filter(|t| t.task_id != candidate_task.task_id)
        .any(|other| {
            matches!(
                other.stage,
                CoworkerTaskStage::Held
                    | CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::EffectUnknown
                    | CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Cancelled
            ) || !other.active_hold_ids.is_empty()
                || other.unresolved_outbox_id.is_some()
        });

    if !has_active_hold && unresolved_changes.is_empty() && !has_impacted_sibling {
        return Ok(false);
    }

    let foundation_proof = prove_task_independence(candidate_task, &job.tasks, &job.holds)?;
    if !foundation_proof.independent {
        return Ok(true);
    }

    let Some(ref completeness_receipt) = runtime_state.envelope.independence_completeness_receipt
    else {
        return Ok(true);
    };

    match completeness_receipt.prove_branch_continuation_allowed(
        candidate_task,
        &job.tasks,
        &job.holds,
        &unresolved_changes,
    ) {
        Ok(proof) => Ok(!proof.independent),
        Err(_) => Ok(true),
    }
}

pub(crate) fn verify_task_fenced_for_advancement(
    candidate_task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    approval_id: &str,
    plans_dir: &Path,
) -> Result<(), NativeError> {
    if is_task_fenced_by_hold_or_sibling(
        candidate_task,
        job,
        runtime_state,
        approval_id,
        plans_dir,
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is fenced by stop_requested, active hold, unresolved conversation change, or non-independent impacted sibling",
                candidate_task.task_id
            ),
        ));
    }
    Ok(())
}

fn verify_predecessors_accepted_and_sealed(
    approval: &ApprovalRecord,
    approved_task: &ApprovedTaskRecord,
    task: &CoworkerTaskRecord,
    job: &CoworkerJobRecord,
    snapshot: &TaskStateSnapshot,
    sessions_dir: &Path,
) -> Result<Option<Vec<AcceptedPredecessorReceiptRef>>, NativeError> {
    if task.depends_on.is_empty() {
        return Ok(None);
    }

    let mut receipts = Vec::with_capacity(task.depends_on.len());
    for pred_id in &task.depends_on {
        let Some(pred_task) = job.tasks.iter().find(|t| &t.task_id == pred_id) else {
            return Ok(None);
        };
        if !matches!(
            pred_task.stage,
            CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
        ) {
            return Ok(None);
        }
        let Some(ref acc_cand_id) = pred_task.accepted_candidate_id else {
            return Ok(None);
        };
        let Some(cand) = pred_task
            .candidates
            .iter()
            .find(|c| &c.candidate_id == acc_cand_id)
        else {
            return Ok(None);
        };
        let Some(ref verdict) = cand.review_verdict else {
            return Ok(None);
        };
        if !verdict.allows_acceptance() {
            return Ok(None);
        }
        let (
            Some(ref review_subject_digest),
            Some(ref review_receipt_id),
            Some(ref integration_receipt_id),
        ) = (
            &cand.review_subject_digest,
            &cand.review_receipt_id,
            &cand.integration_receipt_id,
        )
        else {
            return Ok(None);
        };

        receipts.push(AcceptedPredecessorReceiptRef {
            task_id: pred_task.task_id.clone(),
            candidate_id: cand.candidate_id.clone(),
            head_commit: cand.head_commit.clone(),
            tree_sha: cand.tree_sha.clone(),
            review_subject_digest: review_subject_digest.clone(),
            review_receipt_id: review_receipt_id.clone(),
            integration_receipt_id: integration_receipt_id.clone(),
        });
    }

    if snapshot.status != TaskExecutionStatus::DependencyInputsReady {
        return Ok(None);
    }
    let Some(ref dep_summary) = snapshot.dependency_input else {
        return Ok(None);
    };
    if dep_summary.predecessor_task_ids.len() != receipts.len() {
        return Ok(None);
    }
    for rec in &receipts {
        if !dep_summary
            .predecessor_task_ids
            .iter()
            .any(|pid| pid == &rec.task_id)
        {
            return Ok(None);
        }
    }

    let work_area_dir = sessions_dir
        .join(&approval.approval_id)
        .join(WORK_AREA_SUBDIR);
    let profile_cfg = build_approved_worker_profile_config(
        approval,
        approved_task,
        &snapshot.session_binding_id,
        &work_area_dir,
        &work_area_dir,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to build approved worker profile config for dependent task `{}`: {err}",
                task.task_id
            ),
        )
    })?;
    let Some(ref dep_policy) = profile_cfg.dependency_input else {
        return Ok(None);
    };
    let Some(seal) = inspect_dependency_input_seal_readonly(
        dep_policy,
        &approval.repository_path,
        &work_area_dir,
        &approval.base_commit,
        &approved_task.instruction,
        &dep_policy.command_key,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to verify on-disk DependencyInputSeal for dependent task `{}`: {err}",
                task.task_id
            ),
        )
    })?
    else {
        return Ok(None);
    };

    if !constant_time_str_eq(&seal.seal_digest, &dep_summary.input_digest)
        || seal.execution_base_commit != dep_summary.base_commit
        || seal.execution_tree_sha != dep_summary.tree_sha
        || seal.predecessors.len() != receipts.len()
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "on-disk DependencyInputSeal for task `{}` does not match host DependencyInputSummary",
                task.task_id
            ),
        ));
    }

    for rec in &receipts {
        let Some(host_pred) = seal.predecessors.iter().find(|p| p.task_id == rec.task_id) else {
            return Ok(None);
        };
        let sealed_head_tree_sha = resolve_exact_tree_sha(
            &approval.repository_path,
            &host_pred.accepted_task_head_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to resolve Git tree SHA for predecessor `{}` accepted commit `{}`: {err}",
                    rec.task_id, host_pred.accepted_task_head_commit
                ),
            )
        })?;
        if host_pred.review_receipt_id != rec.review_receipt_id
            || host_pred.integration_receipt_id != rec.integration_receipt_id
            || !constant_time_str_eq(&host_pred.review_subject_digest, &rec.review_subject_digest)
            || host_pred.accepted_task_head_commit != rec.head_commit
            || sealed_head_tree_sha != rec.tree_sha
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "on-disk DependencyInputSeal for predecessor `{}` does not match accepted candidate `{}` on job",
                    rec.task_id, rec.candidate_id
                ),
            ));
        }
    }

    Ok(Some(receipts))
}

fn classify_phase1_status(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    active_running_count: usize,
    fenced_task_ids: &[String],
    unknown_effect_task_ids: &[String],
) -> CoworkerControllerPhase1Status {
    if runtime_state.stop_requested {
        return CoworkerControllerPhase1Status::Stopped;
    }
    if !unknown_effect_task_ids.is_empty()
        || runtime_state
            .pending_effects
            .values()
            .any(|e| e.status == PendingControllerEffectStatus::EffectUnknown)
    {
        return CoworkerControllerPhase1Status::BlockedOnUnknownEffect;
    }
    if active_running_count > 0 {
        return CoworkerControllerPhase1Status::RunningTasks;
    }
    if job.tasks.iter().all(|t| {
        matches!(
            t.stage,
            CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
        )
    }) {
        return CoworkerControllerPhase1Status::AllTasksIntegrated;
    }
    let has_unfenced_ready_for_review = job.tasks.iter().any(|t| {
        !fenced_task_ids.contains(&t.task_id) && t.stage == CoworkerTaskStage::ReadyForReview
    });
    let has_active_hold = job.holds.iter().any(|h| h.active)
        || job.phase == CoworkerJobPhase::Held
        || job
            .tasks
            .iter()
            .any(|t| t.stage == CoworkerTaskStage::Held || !t.active_hold_ids.is_empty());
    if job.tasks.iter().any(|t| {
        matches!(
            t.stage,
            CoworkerTaskStage::Failed | CoworkerTaskStage::Cancelled | CoworkerTaskStage::Blocked
        )
    }) && !has_unfenced_ready_for_review
        && !has_active_hold
    {
        return CoworkerControllerPhase1Status::BlockedOrFailed;
    }
    if !fenced_task_ids.is_empty() || has_active_hold {
        if has_unfenced_ready_for_review {
            return CoworkerControllerPhase1Status::AwaitingChecksOrReview;
        }
        return CoworkerControllerPhase1Status::Held;
    }
    if job
        .tasks
        .iter()
        .any(|t| t.stage == CoworkerTaskStage::ReadyForReview)
    {
        return CoworkerControllerPhase1Status::AwaitingChecksOrReview;
    }
    if job
        .tasks
        .iter()
        .any(|t| t.stage == CoworkerTaskStage::PendingDependencies)
    {
        return CoworkerControllerPhase1Status::AwaitingDependencyAcceptanceOrSeal;
    }
    CoworkerControllerPhase1Status::AwaitingChecksOrReview
}

fn is_host_error_pre_effect_refusal(err: &HostError) -> bool {
    matches!(
        err,
        HostError::Validation { .. }
            | HostError::ApprovalNotFound { .. }
            | HostError::UnknownTask { .. }
            | HostError::ScopeMismatch { .. }
            | HostError::InstanceMismatch { .. }
            | HostError::HostNotReady { .. }
            | HostError::NativeRouteRequired { .. }
            | HostError::DependencyInputsUnverified { .. }
            | HostError::TaskNotStarted { .. }
            | HostError::SecurityViolation { .. }
    )
}

fn map_host_error(err: HostError) -> NativeError {
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
