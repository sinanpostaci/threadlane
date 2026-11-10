//! Derived `Coder` task correction execution (`attempt_no >= 2`) for Reviewer-authorized
//! `correct_in_scope` outcomes.
//!
//! ## Key Invariants
//!
//! - **Physical OS ownership + short-lived step serialization**:
//!   [`execute_task_correction`] requires a live held [`CoworkerJobOwnershipGuard`]. It acquires
//!   `controller.step_mutex()` only during the short pre-execution admission/reservation/claim step
//!   (`DispatchReserved -> Running`) and the short post-execution artifact persistence/promotion step,
//!   releasing `controller.step_mutex()` while awaiting the isolated headless `Coder` provider
//!   subprocess.
//! - **Single-reservation claim without double-charging**:
//!   If the derived attempt (`prior_attempt_no + 1`) was not yet prepared, [`execute_task_correction`]
//!   prepares it first (returning [`CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity`]
//!   without reserving or launching when `approval.max_parallel` is full). If already prepared in
//!   [`CoworkerTaskStage::DispatchReserved`], it claims the existing [`PendingControllerEffect`]
//!   (`CoderDispatch`) and [`OutboxDispatchRecord`] (`TaskAttemptDispatch`) into `Running` with
//!   **no** second `Coder` role reservation or attempt/correction counter charge.
//! - **No blind replay on `Running` or `EffectUnknown`**:
//!   Concurrent duplicate claims against an in-flight `Running` attempt or an `EffectUnknown`
//!   effect/outbox are refused with [`NativeErrorCode::AmbiguousWakeupPendingReconciliation`] without
//!   launching a second worker or minting a fresh request key. Reopen after a crash honors the
//!   persisted private `0600` [`PersistedDerivedTaskExecutionArtifact`] on disk.
//! - **Reuse of owned task/integration worktrees and frozen base/scope/model**:
//!   Derived `Coder` execution runs directly in the existing owned `WorktreeKind::Task` checkout
//!   (`receipt.task_worktree_id`) paired with `receipt.integration_worktree_id` and original
//!   `execution_base_commit`, preserving the original host `run_id` and `worker_id` UUIDs on
//!   controller records while passing the compact owned manifest identifiers (`manifest.run_id`,
//!   `manifest.task_id`) to [`execute_worker_task_with_health_observer`]. It never resets the prior
//!   attempt's committed `HEAD`, never alters base/scope/model, and never reuses a provider session
//!   (`session_id = None`).
//! - **Private `0600` execution report artifact before promotion**:
//!   The full [`PersistedDerivedTaskExecutionArtifact`] (wrapping [`TaskExecutionReport`]) is
//!   atomically persisted as a `0600` regular file in `plans_dir` outside the 1 MiB workflow/policy
//!   ledgers before promoting the task to [`CoworkerTaskStage::ReadyForReview`] (or recording a
//!   truthful terminal `Failed` / `Cancelled` or fenced outcome).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use threadlane_authority::{validation::validate_canonical_thread_uuid, ApprovedTaskRecord};
use threadlane_operations::ReviewSubject;
use threadlane_workers::{
    ensure_worktree_job_inactive_readonly, execute_worker_task_with_health_observer,
    is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha, resolve_exact_tree_sha,
    resolve_model_target, run_sanitized_git, validate_exact_40_hex_sha, validate_sha256_hex_digest,
    ChangedPathEntry, ExecutionBudgets, ResolvedModelTarget, TaskExecutionReport,
    WorkAcceptanceState, WorkerFilePermissions, WorkerProvider, WorkerRunStatus, WorkerSpec,
    WorktreeKind, WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS, MAX_STDERR_BYTES,
    MAX_STDOUT_BYTES,
};
use threadlane_workflow::{ToolAdapter, WorkflowRole};

use crate::conversation_change::ConversationChangeStore;
use crate::coworker_observation::{
    begin_coworker_call_observation, extract_observed_model_and_effort, CoworkerCallFinishSpec,
    CoworkerCallObservationSpec, CoworkerObservationExecutorSpec, COWORKER_OBS_OP_CODER_CORRECTION,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, read_private_file_bounded, validate_bounded_identifier,
};
use crate::profile_flow::{
    preflight, PreflightBlockReason, PreflightRequest, RoleExecutionRoute, RolePreflightRequest,
    SelectedRoleChoice,
};
use crate::role_flow::read_registry_snapshot_readonly;
use threadlane_core::observation::ObservationCallState;

use super::contracts::{
    deterministic_prefixed_id, sha256_hex, validate_non_native_worker_choice, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerTaskRecord, CoworkerTaskStage, OutboxDispatchKind,
    OutboxDispatchRecord, OutboxDispatchStatus, StructuredReviewVerdict, TaskCandidateRef,
    MAX_ATTEMPTS_PER_TASK, MAX_COWORKER_ID_LEN,
};
use super::controller::{
    count_active_coder_executions, is_task_coder_execution_active,
    is_task_fenced_by_hold_or_sibling, task_has_derived_attempt_or_post_initial_state,
    verify_task_fenced_for_advancement, CoworkerControllerService, VerifiedControllerAdmission,
};
use super::controller_policy::{
    deterministic_controller_instance_id, prove_registry_snapshot_equivalence_to_frozen_authority,
    sanitize_controller_diagnostic, CheckFailureCorrectionBasis, ControllerJobRuntimeState,
    DerivedTaskAttemptReceipt, PendingControllerEffect, PendingControllerEffectKind,
    PendingControllerEffectStatus, TaskAttemptExecutionArtifacts, DEFAULT_CONTROLLER_LEASE_TTL_MS,
    MAX_IN_SCOPE_CORRECTION_ATTEMPTS,
};
use super::ownership::CoworkerJobOwnershipGuard;
use super::store::{FinalizeTaskOutboxRequest, ReserveTaskOutboxRequest};
use super::task_correction::{
    compute_derived_coder_effect_id, compute_derived_coder_request_key,
    compute_derived_task_attempt_receipt_id, construct_verified_derived_attempt_receipt,
    try_reopen_existing_correction_reservation, verify_original_approval_unchanged,
    verify_prior_attempt_and_persisted_reviewer_report_with_live_check,
    CoworkerTaskCorrectionPrepareRequest, CoworkerTaskCorrectionPrepareStatus,
};

/// Schema version for [`PersistedDerivedTaskExecutionArtifact`].
pub const DERIVED_TASK_EXECUTION_ARTIFACT_SCHEMA_VERSION: u32 = 1;

/// Finite maximum byte size for a persisted private (`0600`)
/// [`PersistedDerivedTaskExecutionArtifact`] file on disk.
///
/// Sized above `MAX_STDOUT_BYTES + MAX_STDERR_BYTES` plus structured JSON report and diff-gate
/// metadata overhead so valid full worker execution reports never hit the 1 MiB ledger cap.
pub const MAX_DERIVED_EXECUTION_REPORT_ARTIFACT_BYTES: usize =
    MAX_STDOUT_BYTES + MAX_STDERR_BYTES + (2 * 1024 * 1024);

static IN_FLIGHT_DERIVED_CORRECTIONS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();

struct InFlightDerivedCorrectionGuard {
    key: String,
}

impl InFlightDerivedCorrectionGuard {
    fn key_for(
        plans_dir: &Path,
        binding_id: &str,
        job_id: &str,
        task_id: &str,
        attempt_no: u32,
    ) -> String {
        format!(
            "{}:{binding_id}:{job_id}:{task_id}:{attempt_no}",
            plans_dir.display()
        )
    }

    fn is_in_flight(
        plans_dir: &Path,
        binding_id: &str,
        job_id: &str,
        task_id: &str,
        attempt_no: u32,
    ) -> Result<bool, NativeError> {
        let key = Self::key_for(plans_dir, binding_id, job_id, task_id, attempt_no);
        let set = IN_FLIGHT_DERIVED_CORRECTIONS
            .get_or_init(|| Mutex::new(BTreeSet::new()))
            .lock()
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "in-flight derived correction registry mutex poisoned",
                )
            })?;
        Ok(set.contains(&key))
    }

    fn acquire(
        plans_dir: &Path,
        binding_id: &str,
        job_id: &str,
        task_id: &str,
        attempt_no: u32,
    ) -> Result<Self, NativeError> {
        let key = Self::key_for(plans_dir, binding_id, job_id, task_id, attempt_no);
        let mut set = IN_FLIGHT_DERIVED_CORRECTIONS
            .get_or_init(|| Mutex::new(BTreeSet::new()))
            .lock()
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "in-flight derived correction registry mutex poisoned",
                )
            })?;
        if !set.insert(key.clone()) {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "task `{task_id}` derived attempt `{attempt_no}` is currently Running in-flight; duplicate claim refused"
                ),
            ));
        }
        Ok(Self { key })
    }
}

impl Drop for InFlightDerivedCorrectionGuard {
    fn drop(&mut self) {
        if let Some(mutex) = IN_FLIGHT_DERIVED_CORRECTIONS.get() {
            if let Ok(mut set) = mutex.lock() {
                set.remove(&self.key);
            }
        }
    }
}

/// Request to execute (or idempotently reopen) a Reviewer-authorized in-scope `Coder` correction
/// attempt (`attempt_no = prior_attempt_no + 1 >= 2`).
///
/// Rejects any caller-supplied prompt, instruction override, write-path override, model override,
/// or synthetic verdict/report fields via `#[serde(deny_unknown_fields)]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskCorrectionExecuteRequest {
    /// Bound conversation binding identifier (`bnd_<32hex>`).
    pub binding_id: String,
    /// Pinned canonical lowercase hyphenated non-nil thread UUID.
    pub thread_uuid: String,
    /// Target job identifier (`job_<32hex>`).
    pub job_id: String,
    /// Target task identifier within the job.
    pub task_id: String,
    /// Attempt number (`>= 1`) whose verified `Reviewer` verdict returned `correct_in_scope`.
    pub prior_attempt_no: u32,
    /// Monotonic wall-clock timestamp in milliseconds since Unix epoch (`> 0`).
    pub now_ms: u64,
}

/// Type alias for [`CoworkerTaskCorrectionExecuteRequest`].
pub type CoworkerTaskCorrectionExecutionRequest = CoworkerTaskCorrectionExecuteRequest;

/// High-level status of [`CoworkerTaskCorrectionExecuteOutcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerTaskCorrectionExecuteStatus {
    /// Derived `Coder` execution succeeded, passed the post-run diff gate and model/effort checks,
    /// persisted its private `0600` execution report artifact, and promoted the task to
    /// [`CoworkerTaskStage::ReadyForReview`] for ordinary task verification.
    ReadyForVerification,
    /// `approval.max_parallel` is currently saturated by active `Coder` executions; no worker was
    /// launched and no new counters were charged.
    WaitingForParallelCapacity,
    /// Derived `Coder` execution finished with a truthful terminal failure (non-zero exit, timeout,
    /// output overflow, provider block, model/effort mismatch, incomplete cleanup, or diff gate
    /// rejection) and transitioned the task to [`CoworkerTaskStage::Failed`].
    FailedTerminal,
    /// Derived `Coder` execution was cancelled and transitioned the task to
    /// [`CoworkerTaskStage::Cancelled`].
    CancelledTerminal,
    /// A hold, conversational change, or `stop_requested` fence became active while the derived
    /// `Coder` worker was running; the valid worker output was preserved on disk in the private
    /// `0600` report artifact without advancing the task stage to `ReadyForReview`.
    FencedLateOutputPreserved,
}

impl CoworkerTaskCorrectionExecuteStatus {
    /// Returns the canonical snake_case string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadyForVerification => "ready_for_verification",
            Self::WaitingForParallelCapacity => "waiting_for_parallel_capacity",
            Self::FailedTerminal => "failed_terminal",
            Self::CancelledTerminal => "cancelled_terminal",
            Self::FencedLateOutputPreserved => "fenced_late_output_preserved",
        }
    }
}

impl std::fmt::Display for CoworkerTaskCorrectionExecuteStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Persisted private (`0600`) artifact wrapping the full [`TaskExecutionReport`] and cryptographic
/// lineage of a derived `Coder` correction attempt (`attempt_no >= 2`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedDerivedTaskExecutionArtifact {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Deterministic artifact identifier (`dcr_<32hex>`).
    pub report_artifact_id: String,
    /// Bound [`DerivedTaskAttemptReceipt::receipt_id`] (`dar_<32hex>`).
    pub receipt_id: String,
    /// Bound [`DerivedTaskAttemptReceipt::receipt_digest`] (64-hex SHA-256).
    pub receipt_digest: String,
    /// Deterministic derived `Coder` request key (`dck_<32hex>`).
    pub request_key: String,
    /// Deterministic [`PendingControllerEffect::effect_id`] (`eff_<32hex>`).
    pub effect_id: String,
    /// Bound [`OutboxDispatchRecord::outbox_id`] (`obx_<32hex>`).
    pub outbox_id: String,
    /// Bound conversation binding identifier.
    pub binding_id: String,
    /// Pinned thread UUID.
    pub thread_uuid: String,
    /// Bound job identifier.
    pub job_id: String,
    /// Bound original immutable `ApprovalRecord.approval_id`.
    pub approval_id: String,
    /// Bound `FrozenControllerExecutionEnvelope.envelope_id`.
    pub envelope_id: String,
    /// Bound `FrozenControllerExecutionEnvelope.envelope_digest`.
    pub envelope_digest: String,
    /// Target task identifier.
    pub task_id: String,
    /// Frozen task digest.
    pub task_digest: String,
    /// Prior attempt number (`attempt_no - 1`).
    pub prior_attempt_no: u32,
    /// Derived attempt number (`>= 2`).
    pub attempt_no: u32,
    /// Prior rejected candidate identifier (`cand_<32hex>`).
    pub prior_candidate_id: String,
    /// Prior verified Reviewer verdict identifier (`vrd_<32hex>`), or `None` when authorized by
    /// `prior_check_failure_basis`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_review_verdict_id: Option<String>,
    /// Prior verified failed-check correction basis when authorized without a Reviewer verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_check_failure_basis: Option<CheckFailureCorrectionBasis>,
    /// Prior verified `ReviewSubject.subject_digest` (64-hex SHA-256).
    pub prior_review_subject_digest: String,
    /// Original frozen host `run_id` UUID from Attempt 1.
    pub original_run_id: String,
    /// Original frozen host `worker_id` UUID from Attempt 1.
    pub original_worker_id: String,
    /// Compact owned worktree manifest `run_id` passed to `execute_worker_task_with_health_observer`.
    pub manifest_run_id: String,
    /// Compact owned worktree manifest `task_id` passed to `execute_worker_task_with_health_observer`.
    pub manifest_task_id: String,
    /// Reused owned `WorktreeKind::Task` identifier.
    pub task_worktree_id: String,
    /// Reused owned `WorktreeKind::RunIntegration` identifier.
    pub integration_worktree_id: String,
    /// Frozen task execution base commit (40-hex SHA-1).
    pub execution_base_commit: String,
    /// Task worktree `HEAD` commit immediately before derived `Coder` execution (must equal the
    /// prior rejected candidate's committed `head_commit`).
    pub pre_execution_head_commit: String,
    /// Task worktree `HEAD` tree SHA immediately before derived `Coder` execution.
    pub pre_execution_tree_sha: String,
    /// Task worktree `HEAD` commit immediately after derived `Coder` execution (before verification
    /// commits the new changes).
    pub post_execution_head_commit: String,
    /// Task worktree `HEAD` tree SHA immediately after derived `Coder` execution.
    pub post_execution_tree_sha: String,
    /// Canonical 64-hex SHA-256 digest of the post-execution working tree changes relative to
    /// `pre_execution_head_commit`.
    pub worktree_change_set_digest: String,
    /// Canonical 64-hex SHA-256 digest of the post-execution working tree changes relative to
    /// `execution_base_commit`.
    pub base_diff_digest: String,
    /// Whether the derived `Coder` actually modified the working tree relative to
    /// `pre_execution_head_commit`.
    pub has_changes_from_prior_head: bool,
    /// Frozen `Coder` role choice used for this execution.
    pub coder_choice: SelectedRoleChoice,
    /// Full bounded [`TaskExecutionReport`] from `threadlane_workers`.
    pub execution_report: TaskExecutionReport,
    /// Observed provider string (`antigravity` or `opencode_v2`).
    pub observed_provider: String,
    /// Effective observed model reported by the worker stream (`None` if unobserved).
    pub observed_model: Option<String>,
    /// Effective observed effort reported by the worker stream (`None` if unobserved).
    pub observed_effort: Option<String>,
    /// Final execution status recorded for this derived attempt.
    pub status: CoworkerTaskCorrectionExecuteStatus,
    /// True if a hold, conversational change, or `stop_requested` fence prevented promotion to
    /// `ReadyForReview` after the worker finished.
    pub fenced_from_promotion: bool,
    /// Optional sanitized diagnostic when execution failed, was cancelled, or was fenced.
    pub diagnostic: Option<String>,
    /// Timestamp when this artifact was persisted.
    pub recorded_at_ms: u64,
    /// Canonical 64-hex SHA-256 digest over all fields of this artifact.
    pub artifact_digest: String,
}

#[derive(Serialize)]
struct PersistedDerivedTaskExecutionArtifactDigestPayload<'a> {
    schema_version: u32,
    report_artifact_id: &'a str,
    receipt_id: &'a str,
    receipt_digest: &'a str,
    request_key: &'a str,
    effect_id: &'a str,
    outbox_id: &'a str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    job_id: &'a str,
    approval_id: &'a str,
    envelope_id: &'a str,
    envelope_digest: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    prior_attempt_no: u32,
    attempt_no: u32,
    prior_candidate_id: &'a str,
    prior_review_verdict_id: &'a str,
    prior_review_subject_digest: &'a str,
    original_run_id: &'a str,
    original_worker_id: &'a str,
    manifest_run_id: &'a str,
    manifest_task_id: &'a str,
    task_worktree_id: &'a str,
    integration_worktree_id: &'a str,
    execution_base_commit: &'a str,
    pre_execution_head_commit: &'a str,
    pre_execution_tree_sha: &'a str,
    post_execution_head_commit: &'a str,
    post_execution_tree_sha: &'a str,
    worktree_change_set_digest: &'a str,
    base_diff_digest: &'a str,
    has_changes_from_prior_head: bool,
    coder_choice: &'a SelectedRoleChoice,
    execution_report: &'a TaskExecutionReport,
    observed_provider: &'a str,
    observed_model: &'a Option<String>,
    observed_effort: &'a Option<String>,
    status: CoworkerTaskCorrectionExecuteStatus,
    fenced_from_promotion: bool,
    diagnostic: &'a Option<String>,
    recorded_at_ms: u64,
}

#[derive(Serialize)]
struct PersistedCheckFailureDerivedTaskExecutionArtifactDigestPayload<'a> {
    domain: &'static str,
    schema_version: u32,
    report_artifact_id: &'a str,
    receipt_id: &'a str,
    receipt_digest: &'a str,
    request_key: &'a str,
    effect_id: &'a str,
    outbox_id: &'a str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    job_id: &'a str,
    approval_id: &'a str,
    envelope_id: &'a str,
    envelope_digest: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    prior_attempt_no: u32,
    attempt_no: u32,
    prior_candidate_id: &'a str,
    prior_check_failure_basis: &'a CheckFailureCorrectionBasis,
    prior_check_failure_basis_digest: &'a str,
    prior_review_subject_digest: &'a str,
    original_run_id: &'a str,
    original_worker_id: &'a str,
    manifest_run_id: &'a str,
    manifest_task_id: &'a str,
    task_worktree_id: &'a str,
    integration_worktree_id: &'a str,
    execution_base_commit: &'a str,
    pre_execution_head_commit: &'a str,
    pre_execution_tree_sha: &'a str,
    post_execution_head_commit: &'a str,
    post_execution_tree_sha: &'a str,
    worktree_change_set_digest: &'a str,
    base_diff_digest: &'a str,
    has_changes_from_prior_head: bool,
    coder_choice: &'a SelectedRoleChoice,
    execution_report: &'a TaskExecutionReport,
    observed_provider: &'a str,
    observed_model: &'a Option<String>,
    observed_effort: &'a Option<String>,
    status: CoworkerTaskCorrectionExecuteStatus,
    fenced_from_promotion: bool,
    diagnostic: &'a Option<String>,
    recorded_at_ms: u64,
}

impl PersistedDerivedTaskExecutionArtifact {
    /// Computes the canonical 64-character lowercase hexadecimal SHA-256 digest of this artifact.
    pub fn compute_digest(&self) -> Result<String, NativeError> {
        let encoded = match (
            &self.prior_review_verdict_id,
            &self.prior_check_failure_basis,
        ) {
            (Some(verdict_id), None) => {
                let payload = PersistedDerivedTaskExecutionArtifactDigestPayload {
                    schema_version: self.schema_version,
                    report_artifact_id: &self.report_artifact_id,
                    receipt_id: &self.receipt_id,
                    receipt_digest: &self.receipt_digest,
                    request_key: &self.request_key,
                    effect_id: &self.effect_id,
                    outbox_id: &self.outbox_id,
                    binding_id: &self.binding_id,
                    thread_uuid: &self.thread_uuid,
                    job_id: &self.job_id,
                    approval_id: &self.approval_id,
                    envelope_id: &self.envelope_id,
                    envelope_digest: &self.envelope_digest,
                    task_id: &self.task_id,
                    task_digest: &self.task_digest,
                    prior_attempt_no: self.prior_attempt_no,
                    attempt_no: self.attempt_no,
                    prior_candidate_id: &self.prior_candidate_id,
                    prior_review_verdict_id: verdict_id.as_str(),
                    prior_review_subject_digest: &self.prior_review_subject_digest,
                    original_run_id: &self.original_run_id,
                    original_worker_id: &self.original_worker_id,
                    manifest_run_id: &self.manifest_run_id,
                    manifest_task_id: &self.manifest_task_id,
                    task_worktree_id: &self.task_worktree_id,
                    integration_worktree_id: &self.integration_worktree_id,
                    execution_base_commit: &self.execution_base_commit,
                    pre_execution_head_commit: &self.pre_execution_head_commit,
                    pre_execution_tree_sha: &self.pre_execution_tree_sha,
                    post_execution_head_commit: &self.post_execution_head_commit,
                    post_execution_tree_sha: &self.post_execution_tree_sha,
                    worktree_change_set_digest: &self.worktree_change_set_digest,
                    base_diff_digest: &self.base_diff_digest,
                    has_changes_from_prior_head: self.has_changes_from_prior_head,
                    coder_choice: &self.coder_choice,
                    execution_report: &self.execution_report,
                    observed_provider: &self.observed_provider,
                    observed_model: &self.observed_model,
                    observed_effort: &self.observed_effort,
                    status: self.status,
                    fenced_from_promotion: self.fenced_from_promotion,
                    diagnostic: &self.diagnostic,
                    recorded_at_ms: self.recorded_at_ms,
                };
                serde_json::to_vec(&payload).map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "failed to serialize PersistedDerivedTaskExecutionArtifact digest payload",
                    )
                })?
            }
            (None, Some(basis)) => {
                let basis_digest = basis.compute_digest()?;
                let payload = PersistedCheckFailureDerivedTaskExecutionArtifactDigestPayload {
                    domain: "threadlane-native:derived_task_execution_artifact:check_failure:v2",
                    schema_version: self.schema_version,
                    report_artifact_id: &self.report_artifact_id,
                    receipt_id: &self.receipt_id,
                    receipt_digest: &self.receipt_digest,
                    request_key: &self.request_key,
                    effect_id: &self.effect_id,
                    outbox_id: &self.outbox_id,
                    binding_id: &self.binding_id,
                    thread_uuid: &self.thread_uuid,
                    job_id: &self.job_id,
                    approval_id: &self.approval_id,
                    envelope_id: &self.envelope_id,
                    envelope_digest: &self.envelope_digest,
                    task_id: &self.task_id,
                    task_digest: &self.task_digest,
                    prior_attempt_no: self.prior_attempt_no,
                    attempt_no: self.attempt_no,
                    prior_candidate_id: &self.prior_candidate_id,
                    prior_check_failure_basis: basis,
                    prior_check_failure_basis_digest: &basis_digest,
                    prior_review_subject_digest: &self.prior_review_subject_digest,
                    original_run_id: &self.original_run_id,
                    original_worker_id: &self.original_worker_id,
                    manifest_run_id: &self.manifest_run_id,
                    manifest_task_id: &self.manifest_task_id,
                    task_worktree_id: &self.task_worktree_id,
                    integration_worktree_id: &self.integration_worktree_id,
                    execution_base_commit: &self.execution_base_commit,
                    pre_execution_head_commit: &self.pre_execution_head_commit,
                    pre_execution_tree_sha: &self.pre_execution_tree_sha,
                    post_execution_head_commit: &self.post_execution_head_commit,
                    post_execution_tree_sha: &self.post_execution_tree_sha,
                    worktree_change_set_digest: &self.worktree_change_set_digest,
                    base_diff_digest: &self.base_diff_digest,
                    has_changes_from_prior_head: self.has_changes_from_prior_head,
                    coder_choice: &self.coder_choice,
                    execution_report: &self.execution_report,
                    observed_provider: &self.observed_provider,
                    observed_model: &self.observed_model,
                    observed_effort: &self.observed_effort,
                    status: self.status,
                    fenced_from_promotion: self.fenced_from_promotion,
                    diagnostic: &self.diagnostic,
                    recorded_at_ms: self.recorded_at_ms,
                };
                serde_json::to_vec(&payload).map_err(|_| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "failed to serialize PersistedCheckFailureDerivedTaskExecutionArtifact digest payload",
                    )
                })?
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "PersistedDerivedTaskExecutionArtifact must have either prior_review_verdict_id (without prior_check_failure_basis) or prior_check_failure_basis (without prior_review_verdict_id)",
                ));
            }
        };
        Ok(sha256_hex(&encoded))
    }

    /// Validates structural invariants, embedded [`WorkerExecutionReport`] integrity, and the
    /// cryptographic `artifact_digest` of this artifact.
    pub fn validate_integrity(&self) -> Result<(), NativeError> {
        if self.schema_version != DERIVED_TASK_EXECUTION_ARTIFACT_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported PersistedDerivedTaskExecutionArtifact schema_version `{}` (expected `{DERIVED_TASK_EXECUTION_ARTIFACT_SCHEMA_VERSION}`)",
                    self.schema_version
                ),
            ));
        }
        validate_bounded_identifier(
            &self.report_artifact_id,
            "report_artifact_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(&self.receipt_id, "receipt_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.receipt_digest, "receipt_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "receipt_digest must be a 64-hex SHA-256",
            )
        })?;
        validate_bounded_identifier(&self.request_key, "request_key", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.outbox_id, "outbox_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &self.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "PersistedDerivedTaskExecutionArtifact.thread_uuid is not a canonical UUID",
            )
        })?;
        validate_bounded_identifier(&self.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.approval_id, "approval_id", MAX_COWORKER_ID_LEN)?;
        validate_bounded_identifier(&self.envelope_id, "envelope_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.envelope_digest, "envelope_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "envelope_digest must be a 64-hex SHA-256",
            )
        })?;
        validate_bounded_identifier(&self.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
        validate_sha256_hex_digest(&self.task_digest, "task_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "task_digest must be a 64-hex SHA-256",
            )
        })?;
        if self.prior_attempt_no == 0
            || self.attempt_no != self.prior_attempt_no.saturating_add(1)
            || self.attempt_no < 2
        {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PersistedDerivedTaskExecutionArtifact has invalid prior_attempt_no or attempt_no",
            ));
        }
        validate_bounded_identifier(
            &self.prior_candidate_id,
            "prior_candidate_id",
            MAX_COWORKER_ID_LEN,
        )?;
        match (
            &self.prior_review_verdict_id,
            &self.prior_check_failure_basis,
        ) {
            (Some(verdict_id), None) => {
                validate_bounded_identifier(
                    verdict_id,
                    "prior_review_verdict_id",
                    MAX_COWORKER_ID_LEN,
                )?;
            }
            (None, Some(basis)) => {
                basis.validate()?;
                if basis.task_id != self.task_id
                    || basis.task_manifest_id != self.manifest_task_id
                    || basis.prior_attempt_no != self.prior_attempt_no
                    || basis.prior_candidate_id != self.prior_candidate_id
                    || basis.execution_base_commit != self.execution_base_commit
                    || basis.committed_head_commit != self.pre_execution_head_commit
                    || basis.committed_tree_sha != self.pre_execution_tree_sha
                    || !constant_time_str_eq(
                        &basis.prior_review_subject_digest,
                        &self.prior_review_subject_digest,
                    )
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "PersistedDerivedTaskExecutionArtifact.prior_check_failure_basis does not match artifact lineage",
                    ));
                }
            }
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "PersistedDerivedTaskExecutionArtifact must have either prior_review_verdict_id or prior_check_failure_basis (not both or neither)",
                ));
            }
        }
        validate_sha256_hex_digest(
            &self.prior_review_subject_digest,
            "prior_review_subject_digest",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "prior_review_subject_digest must be a 64-hex SHA-256",
            )
        })?;
        validate_bounded_identifier(
            &self.original_run_id,
            "original_run_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.original_worker_id,
            "original_worker_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.manifest_run_id,
            "manifest_run_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.manifest_task_id,
            "manifest_task_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.task_worktree_id,
            "task_worktree_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_bounded_identifier(
            &self.integration_worktree_id,
            "integration_worktree_id",
            MAX_COWORKER_ID_LEN,
        )?;
        validate_exact_40_hex_sha(&self.execution_base_commit, "execution_base_commit").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "execution_base_commit must be a 40-hex SHA-1",
                )
            },
        )?;
        validate_exact_40_hex_sha(&self.pre_execution_head_commit, "pre_execution_head_commit")
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "pre_execution_head_commit must be a 40-hex SHA-1",
                )
            })?;
        validate_exact_40_hex_sha(&self.pre_execution_tree_sha, "pre_execution_tree_sha").map_err(
            |_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "pre_execution_tree_sha must be a 40-hex SHA-1",
                )
            },
        )?;
        validate_exact_40_hex_sha(
            &self.post_execution_head_commit,
            "post_execution_head_commit",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "post_execution_head_commit must be a 40-hex SHA-1",
            )
        })?;
        validate_exact_40_hex_sha(&self.post_execution_tree_sha, "post_execution_tree_sha")
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "post_execution_tree_sha must be a 40-hex SHA-1",
                )
            })?;
        validate_sha256_hex_digest(
            &self.worktree_change_set_digest,
            "worktree_change_set_digest",
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "worktree_change_set_digest must be a 64-hex SHA-256",
            )
        })?;
        validate_sha256_hex_digest(&self.base_diff_digest, "base_diff_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "base_diff_digest must be a 64-hex SHA-256",
            )
        })?;
        validate_sha256_hex_digest(&self.artifact_digest, "artifact_digest").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "artifact_digest must be a 64-hex SHA-256",
            )
        })?;

        let expected_artifact_id = compute_derived_coder_report_artifact_id(
            &self.job_id,
            &self.task_id,
            self.attempt_no,
            &self.request_key,
        );
        if self.report_artifact_id != expected_artifact_id {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "PersistedDerivedTaskExecutionArtifact.report_artifact_id `{}` does not match expected `{expected_artifact_id}`",
                    self.report_artifact_id
                ),
            ));
        }

        let computed = self.compute_digest()?;
        if !constant_time_str_eq(&self.artifact_digest, &computed) {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "PersistedDerivedTaskExecutionArtifact digest mismatch: stored `{}`, computed `{computed}`",
                    self.artifact_digest
                ),
            ));
        }

        Ok(())
    }

    /// Validates that this persisted artifact is bound to `receipt` and satisfies all `Coder`
    /// execution, diff-gate, model/effort, and natural-completion requirements for task
    /// verification (`attempt_no >= 2`).
    pub fn validate_for_task_verification(
        &self,
        receipt: &DerivedTaskAttemptReceipt,
        candidate_task: &CoworkerTaskRecord,
        approved_task: &ApprovedTaskRecord,
    ) -> Result<(), NativeError> {
        self.validate_integrity()?;
        if self.receipt_id != receipt.receipt_id
            || !constant_time_str_eq(&self.receipt_digest, &receipt.receipt_digest)
            || self.binding_id != receipt.binding_id
            || self.thread_uuid != receipt.thread_uuid
            || self.job_id != receipt.job_id
            || self.approval_id != receipt.approval_id
            || self.envelope_id != receipt.envelope_id
            || !constant_time_str_eq(&self.envelope_digest, &receipt.envelope_digest)
            || self.task_id != receipt.task_id
            || !constant_time_str_eq(&self.task_digest, &receipt.task_digest)
            || self.attempt_no != receipt.attempt_no
            || Some(self.prior_candidate_id.as_str())
                != receipt.prior_rejected_candidate_id.as_deref()
            || self.prior_review_verdict_id != receipt.prior_review_verdict_id
            || self.prior_check_failure_basis != receipt.prior_check_failure_basis
            || Some(self.prior_review_subject_digest.as_str())
                != receipt.prior_review_subject_digest.as_deref()
            || self.original_run_id != receipt.run_id
            || self.original_worker_id != receipt.worker_id
            || self.task_worktree_id != receipt.task_worktree_id
            || self.integration_worktree_id != receipt.integration_worktree_id
            || self.execution_base_commit != receipt.execution_base_commit
            || self.coder_choice != receipt.coder_choice
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PersistedDerivedTaskExecutionArtifact does not match DerivedTaskAttemptReceipt",
            ));
        }

        if self.status != CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
            || self.fenced_from_promotion
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "PersistedDerivedTaskExecutionArtifact status is `{}` (fenced={}); not eligible for task verification",
                    self.status, self.fenced_from_promotion
                ),
            ));
        }

        if !self.has_changes_from_prior_head
            || self.pre_execution_head_commit != self.post_execution_head_commit
            || self.pre_execution_tree_sha != self.post_execution_tree_sha
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "PersistedDerivedTaskExecutionArtifact has no changes from prior HEAD or modified HEAD directly before verification",
            ));
        }

        if self.execution_report.worker_report.cleanup_incomplete {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "derived WorkerExecutionReport has cleanup_incomplete == true",
            ));
        }

        if self.execution_report.worker_report.status != WorkerRunStatus::Succeeded
            || !self
                .execution_report
                .worker_report
                .has_complete_natural_completion_evidence()
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "derived WorkerExecutionReport did not succeed with complete natural completion evidence",
            ));
        }

        if self.execution_report.overall_acceptance_state
            != WorkAcceptanceState::DiffGatePassedPendingReview
            || !self.execution_report.diff_gate.passed
            || !self.execution_report.diff_gate.evidence_complete
            || self.execution_report.diff_gate.changed_paths.is_empty()
            || !self.execution_report.diff_gate.violations.is_empty()
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "derived TaskExecutionReport did not pass the post-run diff gate",
            ));
        }

        let expected_provider = match receipt.coder_choice.adapter {
            ToolAdapter::Antigravity => WorkerProvider::Antigravity,
            ToolAdapter::OpencodeV2 => WorkerProvider::OpenCodeV2,
            ToolAdapter::CodexCli => WorkerProvider::CodexCli,
            _ => {
                return Err(NativeError::new(
                    NativeErrorCode::UnsupportedCapability,
                    "unsupported Coder adapter on DerivedTaskAttemptReceipt",
                ));
            }
        };
        let resolved_target = resolve_model_target(
            expected_provider,
            &receipt.coder_choice.model_arg,
            receipt.coder_choice.effort.as_deref(),
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("invalid frozen Coder model target: {err}"),
            )
        })?;

        if self.execution_report.worker_report.provider != expected_provider
            || self.observed_provider != expected_provider.as_str()
            || approved_task.provider != Some(expected_provider)
            || (expected_provider != WorkerProvider::CodexCli
                && self
                    .execution_report
                    .worker_report
                    .effective_observed_model()
                    != Some(resolved_target.base_model.as_str()))
            || self.observed_model.as_deref()
                != self
                    .execution_report
                    .worker_report
                    .effective_observed_model()
            || (expected_provider == WorkerProvider::CodexCli
                && !approved_task.matches_codex_task_launch_report(
                    &self.execution_report.worker_report,
                    &self.execution_report.manifest.checkout_path,
                    &receipt.execution_base_commit,
                ))
            || resolved_target.base_model != approved_task.base_model
            || resolved_target.effort != approved_task.effort
            || approved_task.cli_model_arg.as_deref()
                != Some(resolved_target.cli_model_arg.as_str())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "derived TaskExecutionReport observed provider/model does not match frozen Coder choice",
            ));
        }

        if let Some(ref exp_effort) = resolved_target.effort {
            if (expected_provider != WorkerProvider::CodexCli
                && self
                    .execution_report
                    .worker_report
                    .effective_observed_effort()
                    != Some(exp_effort.as_str()))
                || self.observed_effort.as_deref()
                    != self
                        .execution_report
                        .worker_report
                        .effective_observed_effort()
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "derived TaskExecutionReport observed_effort does not match frozen Coder effort",
                ));
            }
        }

        let expected_write_paths: BTreeSet<String> =
            candidate_task.write_paths.iter().cloned().collect();
        if self.execution_report.manifest.kind != WorktreeKind::Task
            || self.execution_report.manifest.worktree_id != receipt.task_worktree_id
            || self.execution_report.manifest.base_commit != receipt.execution_base_commit
            || self.execution_report.manifest.allowed_write_paths != expected_write_paths
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "derived TaskExecutionReport worktree/base/write-scope metadata does not match receipt",
            ));
        }
        let int_manifest = load_and_verify_manifest(
            &self.execution_report.manifest.work_area_root,
            &receipt.integration_worktree_id,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "failed to load and verify owned integration worktree manifest during derived artifact verification: {err}"
                ),
            )
        })?;
        if int_manifest.kind != WorktreeKind::RunIntegration
            || int_manifest.worktree_id != receipt.integration_worktree_id
            || int_manifest.run_id != self.execution_report.manifest.run_id
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "derived TaskExecutionReport integration worktree manifest does not match receipt",
            ));
        }

        Ok(())
    }
}

/// Outcome of [`execute_task_correction`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerTaskCorrectionExecuteOutcome {
    /// Bound conversation binding identifier.
    pub binding_id: String,
    /// Pinned thread UUID.
    pub thread_uuid: String,
    /// Target job identifier.
    pub job_id: String,
    /// Original immutable `ApprovalRecord.approval_id`.
    pub approval_id: String,
    /// Target task identifier.
    pub task_id: String,
    /// Prior attempt number (`>= 1`).
    pub prior_attempt_no: u32,
    /// Derived attempt number (`prior_attempt_no + 1 >= 2`).
    pub attempt_no: u32,
    /// High-level execution outcome status.
    pub status: CoworkerTaskCorrectionExecuteStatus,
    /// Resulting task stage in the binding workflow ledger.
    pub task_stage: CoworkerTaskStage,
    /// Frozen `ApprovalRecord.max_parallel` ceiling.
    pub max_parallel: usize,
    /// Active `Coder` executions after this call.
    pub active_coder_executions: usize,
    /// Prior rejected candidate identifier (`cand_<32hex>`).
    pub prior_candidate_id: String,
    /// Prior verified Reviewer verdict identifier (`vrd_<32hex>`), or `None` when authorized by
    /// `prior_check_failure_basis`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_review_verdict_id: Option<String>,
    /// Prior verified failed-check correction basis when authorized without a Reviewer verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_check_failure_basis: Option<CheckFailureCorrectionBasis>,
    /// Prior verified `ReviewSubject.subject_digest`.
    pub prior_review_subject_digest: String,
    /// Deterministic derived `Coder` request key (`None` when `WaitingForParallelCapacity` before reservation).
    pub request_key: Option<String>,
    /// Deterministic [`PendingControllerEffect::effect_id`] (`None` when `WaitingForParallelCapacity` before reservation).
    pub effect_id: Option<String>,
    /// Durable [`OutboxDispatchRecord::outbox_id`] (`None` when `WaitingForParallelCapacity` before reservation).
    pub outbox_id: Option<String>,
    /// Deterministic report artifact identifier (`dcr_<32hex>`), populated when the worker ran.
    pub report_artifact_id: Option<String>,
    /// Absolute path to the private `0600` persisted report artifact file on disk, populated when the worker ran.
    pub report_path: Option<String>,
    /// Bound [`DerivedTaskAttemptReceipt`], populated once reserved.
    pub receipt: Option<DerivedTaskAttemptReceipt>,
    /// Full persisted [`PersistedDerivedTaskExecutionArtifact`], populated when the worker ran.
    pub report: Option<PersistedDerivedTaskExecutionArtifact>,
    /// Optional sanitized diagnostic when execution failed, was cancelled, or was fenced.
    pub diagnostic: Option<String>,
    /// True if this call idempotently reopened an already-completed derived execution from disk.
    pub reused_existing: bool,
}

/// Service wrapper for executing Reviewer-authorized in-scope `Coder` correction attempts.
#[derive(Debug, Clone)]
pub struct CoworkerTaskCorrectionExecutionService {
    controller: CoworkerControllerService,
}

impl CoworkerTaskCorrectionExecutionService {
    /// Creates a new [`CoworkerTaskCorrectionExecutionService`] wrapping `controller`.
    pub fn new(controller: CoworkerControllerService) -> Self {
        Self { controller }
    }

    /// Borrows the underlying [`CoworkerControllerService`].
    pub fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Executes (or idempotently reopens) an in-scope `Coder` correction attempt
    /// (`prior_attempt_no + 1`) while holding physical OS job ownership.
    pub async fn execute_task_correction(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskCorrectionExecuteRequest,
    ) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
        execute_task_correction(&self.controller, guard, request).await
    }

    /// Alias for [`Self::execute_task_correction`].
    pub async fn execute_or_reopen_task_correction(
        &self,
        guard: &CoworkerJobOwnershipGuard,
        request: &CoworkerTaskCorrectionExecuteRequest,
    ) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
        execute_task_correction(&self.controller, guard, request).await
    }
}

/// Computes the deterministic [`PersistedDerivedTaskExecutionArtifact::report_artifact_id`]
/// (`dcr_<32hex>`) for a derived `Coder` correction attempt.
pub fn compute_derived_coder_report_artifact_id(
    job_id: &str,
    task_id: &str,
    attempt_no: u32,
    request_key: &str,
) -> String {
    let attempt_str = attempt_no.to_string();
    deterministic_prefixed_id(
        "threadlane-native:coworker-derived-coder-report:v1",
        "dcr",
        &[job_id, task_id, &attempt_str, request_key],
    )
}

fn derived_execution_report_filename(effect_id: &str) -> String {
    format!("coworker-derived-task-execution-report-{effect_id}.json")
}

/// Returns the canonical path in `plans_dir` for the private `0600` derived execution report file
/// corresponding to `effect_id`.
pub fn derived_execution_report_file_path(plans_dir: &Path, effect_id: &str) -> PathBuf {
    plans_dir.join(derived_execution_report_filename(effect_id))
}

fn open_private_derived_report_staging_fd(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<tempfile::NamedTempFile, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let prefix = format!(".{}.tmp.", derived_execution_report_filename(effect_id));
        tempfile::Builder::new()
            .prefix(&prefix)
            .permissions(fs::Permissions::from_mode(0o600))
            .tempfile_in(plans_dir)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::IoError,
                    format!(
                        "failed to open private 0600 derived task execution report staging file: {}",
                        sanitize_controller_diagnostic(&err.to_string())
                    ),
                )
            })
    }
    #[cfg(not(unix))]
    {
        let _ = (plans_dir, effect_id);
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "derived task correction execution requires Unix 0600 file permissions",
        ))
    }
}

fn persist_held_derived_task_execution_report(
    plans_dir: &Path,
    effect_id: &str,
    mut held_temp: tempfile::NamedTempFile,
    artifact: &PersistedDerivedTaskExecutionArtifact,
) -> Result<PathBuf, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    artifact.validate_integrity()?;
    let bytes = serde_json::to_vec_pretty(artifact).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize PersistedDerivedTaskExecutionArtifact JSON",
        )
    })?;
    if bytes.len() > MAX_DERIVED_EXECUTION_REPORT_ARTIFACT_BYTES {
        return Err(NativeError::new(
            NativeErrorCode::OutputOverflow,
            "PersistedDerivedTaskExecutionArtifact exceeds maximum private artifact file budget",
        ));
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let target_path = derived_execution_report_file_path(plans_dir, effect_id);
        if let Ok(meta) = fs::symlink_metadata(&target_path) {
            let code = if meta.file_type().is_symlink() {
                NativeErrorCode::SecurityViolation
            } else {
                NativeErrorCode::ProtocolError
            };
            return Err(NativeError::new(
                code,
                format!(
                    "derived task execution report `{}` already exists on disk and cannot be overwritten",
                    target_path.display()
                ),
            ));
        }
        crate::fs_safety::validate_private_file_path(
            &target_path,
            "derived_task_execution_report",
        )?;
        held_temp.write_all(&bytes).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to write held 0600 derived task execution report file: {}",
                    sanitize_controller_diagnostic(&err.to_string())
                ),
            )
        })?;
        held_temp.as_file().sync_all().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to sync held 0600 derived task execution report file: {}",
                    sanitize_controller_diagnostic(&err.to_string())
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
                        "failed to enforce 0600 permissions on held derived task execution report file: {}",
                        sanitize_controller_diagnostic(&err.to_string())
                    ),
                )
            })?;
        held_temp.persist_noclobber(&target_path).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to atomically link held 0600 derived task execution report file `{}` without clobber: {}",
                    target_path.display(),
                    sanitize_controller_diagnostic(&err.to_string())
                ),
            )
        })?;
        let dir_file = fs::File::open(plans_dir).map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to open plans_dir `{}` for directory sync after derived report persistence: {}",
                    plans_dir.display(),
                    sanitize_controller_diagnostic(&err.to_string())
                ),
            )
        })?;
        dir_file.sync_all().map_err(|err| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!(
                    "failed to sync plans_dir `{}` after derived report persistence: {}",
                    plans_dir.display(),
                    sanitize_controller_diagnostic(&err.to_string())
                ),
            )
        })?;
        Ok(target_path)
    }
    #[cfg(not(unix))]
    {
        let _ = (plans_dir, effect_id, held_temp, bytes);
        Err(NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "derived task correction execution requires Unix 0600 file permissions",
        ))
    }
}

/// Reads and validates integrity of a persisted private (`0600`)
/// [`PersistedDerivedTaskExecutionArtifact`] by `effect_id`.
pub fn read_persisted_derived_task_execution_report(
    plans_dir: &Path,
    effect_id: &str,
) -> Result<Option<PersistedDerivedTaskExecutionArtifact>, NativeError> {
    validate_bounded_identifier(effect_id, "effect_id", MAX_COWORKER_ID_LEN)?;
    let path = derived_execution_report_file_path(plans_dir, effect_id);
    let Some(bytes) = read_private_file_bounded(
        &path,
        MAX_DERIVED_EXECUTION_REPORT_ARTIFACT_BYTES,
        "derived_task_execution_report",
    )?
    else {
        return Ok(None);
    };
    let artifact: PersistedDerivedTaskExecutionArtifact =
        serde_json::from_slice(&bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to parse persisted PersistedDerivedTaskExecutionArtifact JSON",
            )
        })?;
    artifact.validate_integrity()?;
    Ok(Some(artifact))
}

#[derive(Serialize)]
struct WorktreeChangeSetDigestEntry {
    path: String,
    exists: bool,
    content_sha256: Option<String>,
}

#[derive(Serialize)]
struct WorktreeChangeSetDigestPayload<'a> {
    reference_commit: &'a str,
    git_diff_sha256: String,
    entries: Vec<WorktreeChangeSetDigestEntry>,
}

/// Computes a deterministic 64-hex SHA-256 digest of the working tree changes in `checkout_path`
/// relative to `reference_commit`, covering both tracked `git diff --binary <reference_commit>`
/// output and the current regular file contents of `changed_paths`.
pub fn compute_derived_worktree_change_set_digest(
    checkout_path: &Path,
    reference_commit: &str,
    changed_paths: &[ChangedPathEntry],
) -> Result<(String, bool), NativeError> {
    let diff_out = run_sanitized_git(
        checkout_path,
        &["diff", "--binary", reference_commit, "--"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to compute git diff against `{reference_commit}`: {err}"),
        )
    })?;
    let status_out = run_sanitized_git(
        checkout_path,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to inspect git status in task worktree: {err}"),
        )
    })?;

    let mut entries = Vec::with_capacity(changed_paths.len());
    for entry in changed_paths {
        let rel_path = &entry.path;
        let full_path = checkout_path.join(rel_path);
        match fs::symlink_metadata(&full_path) {
            Ok(meta) if meta.is_file() => {
                let bytes = fs::read(&full_path).map_err(|err| {
                    NativeError::new(
                        NativeErrorCode::IoError,
                        format!(
                            "failed to read modified task worktree file `{rel_path}`: {}",
                            sanitize_controller_diagnostic(&err.to_string())
                        ),
                    )
                })?;
                entries.push(WorktreeChangeSetDigestEntry {
                    path: rel_path.clone(),
                    exists: true,
                    content_sha256: Some(sha256_hex(&bytes)),
                });
            }
            Ok(_) => {
                entries.push(WorktreeChangeSetDigestEntry {
                    path: rel_path.clone(),
                    exists: true,
                    content_sha256: None,
                });
            }
            Err(_) => {
                entries.push(WorktreeChangeSetDigestEntry {
                    path: rel_path.clone(),
                    exists: false,
                    content_sha256: None,
                });
            }
        }
    }

    let has_changes_from_ref = !diff_out.is_empty() || !status_out.trim().is_empty();
    let payload = WorktreeChangeSetDigestPayload {
        reference_commit,
        git_diff_sha256: sha256_hex(diff_out.as_bytes()),
        entries,
    };
    let encoded = serde_json::to_vec(&payload).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "failed to serialize WorktreeChangeSetDigestPayload",
        )
    })?;
    Ok((sha256_hex(&encoded), has_changes_from_ref))
}

struct ClaimedDerivedExecutionContext {
    _in_flight_guard: InFlightDerivedCorrectionGuard,
    controller_instance_id: String,
    lease_epoch: u64,
    approval_id: String,
    next_attempt_no: u32,
    receipt: DerivedTaskAttemptReceipt,
    request_key: String,
    effect_id: String,
    outbox_record: OutboxDispatchRecord,
    report_artifact_id: String,
    prior_candidate: TaskCandidateRef,
    review_subject: ReviewSubject,
    recorded_verdict: StructuredReviewVerdict,
    task_manifest: WorktreeManifest,
    expected_provider: WorkerProvider,
    approved_task: ApprovedTaskRecord,
    resolved_model_target: ResolvedModelTarget,
    worker_spec: WorkerSpec,
    pre_execution_head_commit: String,
    pre_execution_tree_sha: String,
    held_temp: tempfile::NamedTempFile,
}

/// Executes (or idempotently reopens) an in-scope `Coder` correction attempt
/// (`attempt_no = prior_attempt_no + 1 >= 2`) while holding physical OS job ownership.
pub async fn execute_task_correction(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskCorrectionExecuteRequest,
) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
    validate_bounded_identifier(&request.binding_id, "binding_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.job_id, "job_id", MAX_COWORKER_ID_LEN)?;
    validate_bounded_identifier(&request.task_id, "task_id", MAX_COWORKER_ID_LEN)?;
    validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "thread_uuid must be a canonical lowercase hyphenated non-nil UUID",
        )
    })?;
    if request.prior_attempt_no == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "prior_attempt_no must be >= 1",
        ));
    }
    if request.now_ms == 0 {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "now_ms must be > 0",
        ));
    }
    let next_attempt_no = request.prior_attempt_no.checked_add(1).ok_or_else(|| {
        NativeError::new(NativeErrorCode::ValidationFailed, "attempt_no overflow")
    })?;
    if request.prior_attempt_no > MAX_IN_SCOPE_CORRECTION_ATTEMPTS
        || next_attempt_no > 1 + MAX_IN_SCOPE_CORRECTION_ATTEMPTS
        || (next_attempt_no as usize) > MAX_ATTEMPTS_PER_TASK
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` exhausted per-task in-scope correction limit (prior_attempt_no={}, max_in_scope_corrections={MAX_IN_SCOPE_CORRECTION_ATTEMPTS})",
                request.task_id, request.prior_attempt_no
            ),
        ));
    }

    // Phase 1: Short serialized admission, reopen check, reservation, and single claim (`DispatchReserved -> Running`).
    let claimed_ctx = {
        let _step_serial_guard = controller.step_mutex().lock().await;
        match prepare_or_claim_derived_coder_execution(controller, guard, request, next_attempt_no)?
        {
            PrepareOrClaimDisposition::CompletedOrWaiting(outcome) => return Ok(*outcome),
            PrepareOrClaimDisposition::ClaimedForExecution(ctx) => *ctx,
        }
    };

    // Phase 2: Execute the isolated headless Coder provider process outside `controller.step_mutex()`
    // while keeping the physical OS ownership FD (`guard`) held and supervising live stop/cancel state.
    let health_sink = crate::provider_health::ScopedHealthSink::controller(
        controller,
        guard,
        &claimed_ctx.effect_id,
        WorkflowRole::Coder,
        Some(&request.task_id),
        Some(claimed_ctx.next_attempt_no),
        &claimed_ctx.receipt.receipt_digest,
    )?;
    let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
    let manifest_task_id = claimed_ctx
        .task_manifest
        .task_id
        .clone()
        .unwrap_or_else(|| request.task_id.clone());
    let watcher_controller = controller.clone();
    let watcher_binding_id = request.binding_id.clone();
    let watcher_job_id = request.job_id.clone();
    let watcher_task_id = request.task_id.clone();
    let cancel_watcher = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(40));
        loop {
            interval.tick().await;
            let stop_signaled = watcher_controller
                .policy_store()
                .get_job_runtime_readonly(&watcher_binding_id, &watcher_job_id)
                .ok()
                .flatten()
                .is_some_and(|rt| rt.stop_requested);
            let job_or_task_cancelled = watcher_controller
                .workflow_store()
                .read_binding_ledger(&watcher_binding_id)
                .ok()
                .and_then(|ledger| ledger.jobs.into_iter().find(|j| j.job_id == watcher_job_id))
                .is_some_and(|job| {
                    job.phase != CoworkerJobPhase::Executing
                        || job.tasks.iter().any(|t| {
                            t.task_id == watcher_task_id && t.stage == CoworkerTaskStage::Cancelled
                        })
                });
            if stop_signaled || job_or_task_cancelled {
                let _ = cancel_tx.send(true);
                break;
            }
        }
    });
    let obs_handle = begin_coworker_call_observation(CoworkerCallObservationSpec {
        journal_dir: controller.workflow_store().plans_dir().to_path_buf(),
        operation: COWORKER_OBS_OP_CODER_CORRECTION.to_string(),
        title: format!(
            "Coder correction `{}` (attempt {})",
            request.task_id, claimed_ctx.next_attempt_no
        ),
        call_key: claimed_ctx.effect_id.clone(),
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        repository_path: claimed_ctx.task_manifest.repository_path.clone(),
        job_id: Some(request.job_id.clone()),
        plan_id: None,
        task_id: Some(request.task_id.clone()),
        approval_id: Some(claimed_ctx.approval_id.clone()),
        attempt_no: Some(claimed_ctx.next_attempt_no),
        run_id: Some(claimed_ctx.receipt.run_id.clone()),
        worker_id: Some(claimed_ctx.receipt.worker_id.clone()),
        candidate_id: None,
        executor: CoworkerObservationExecutorSpec::from_selected_role_choice(
            WorkflowRole::Coder,
            &claimed_ctx.receipt.coder_choice,
        ),
        verified_context: None,
        entry_summary: None,
    });
    let obs_receipt_id = claimed_ctx.receipt.receipt_id.clone();
    let obs_artifact_id = claimed_ctx.report_artifact_id.clone();

    let exec_result = crate::provider_health::supervise_health(
        Some(&health_sink),
        |observer| {
            execute_worker_task_with_health_observer(
                &claimed_ctx.worker_spec,
                &claimed_ctx.task_manifest.run_id,
                manifest_task_id.as_str(),
                Some(&claimed_ctx.receipt.task_worktree_id),
                &mut cancel_rx,
                None,
                Some(observer),
            )
        },
        |report| report.worker_report.provider_health.as_ref(),
    )
    .await;
    cancel_watcher.abort();

    let (genuine_obs_model, genuine_obs_effort) = match &exec_result {
        Ok(rep) => extract_observed_model_and_effort(&rep.worker_report),
        Err(_) => (None, None),
    };

    // Phase 3: Re-acquire `controller.step_mutex()`, re-verify physical ownership and lease, persist
    // the private `0600` execution artifact, and promote or record terminal/fenced state.
    let _step_serial_guard = controller.step_mutex().lock().await;
    let finalize_res = finalize_claimed_derived_coder_execution(
        controller,
        guard,
        request,
        claimed_ctx,
        &manifest_task_id,
        exec_result,
    );
    match &finalize_res {
        Ok(outcome) => {
            let obs_state = match outcome.status {
                CoworkerTaskCorrectionExecuteStatus::ReadyForVerification => {
                    ObservationCallState::Completed
                }
                CoworkerTaskCorrectionExecuteStatus::CancelledTerminal => {
                    ObservationCallState::Cancelled
                }
                CoworkerTaskCorrectionExecuteStatus::FailedTerminal
                | CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved
                | CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity => {
                    ObservationCallState::Failed
                }
            };
            let _ = obs_handle.finish(CoworkerCallFinishSpec {
                state: Some(obs_state),
                observed_model: genuine_obs_model,
                observed_effort: genuine_obs_effort,
                receipt_id: Some(obs_receipt_id),
                artifact_id: Some(obs_artifact_id),
                diagnostic: outcome.diagnostic.clone(),
                ..CoworkerCallFinishSpec::default()
            });
        }
        Err(err) => {
            let obs_state = if err.code == NativeErrorCode::AmbiguousWakeupPendingReconciliation {
                ObservationCallState::Unknown
            } else {
                ObservationCallState::Failed
            };
            let _ = obs_handle.finish(CoworkerCallFinishSpec {
                state: Some(obs_state),
                observed_model: genuine_obs_model,
                observed_effort: genuine_obs_effort,
                receipt_id: Some(obs_receipt_id),
                artifact_id: Some(obs_artifact_id),
                diagnostic: Some(err.message.clone()),
                ..CoworkerCallFinishSpec::default()
            });
        }
    }
    finalize_res
}

enum PrepareOrClaimDisposition {
    CompletedOrWaiting(Box<CoworkerTaskCorrectionExecuteOutcome>),
    ClaimedForExecution(Box<ClaimedDerivedExecutionContext>),
}

fn prepare_or_claim_derived_coder_execution(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskCorrectionExecuteRequest,
    next_attempt_no: u32,
) -> Result<PrepareOrClaimDisposition, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    controller.reconcile_stale_logical_lease_under_physical_guard(
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
            request.now_ms,
            DEFAULT_CONTROLLER_LEASE_TTL_MS,
        )?;
        adm = controller.verify_job_admission(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;
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

    if adm.runtime_state.stop_requested {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "controller job `{}` has stop_requested == true; derived task correction execution is fenced",
                adm.job.job_id
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
                format!(
                    "failed to read ConversationChangeStore during derived task correction execution: {err}"
                ),
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
                "approval `{}` is superseded in ConversationChangeStore; derived task correction execution is fenced",
                adm.approval.approval_id
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

    if !task_has_derived_attempt_or_post_initial_state(&candidate_task, &adm.runtime_state) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` has not completed an initial candidate/review stage and is not eligible for correction",
                request.task_id
            ),
        ));
    }

    if is_task_fenced_by_hold_or_sibling(
        &candidate_task,
        &adm.job,
        &adm.runtime_state,
        &adm.approval.approval_id,
        plans_dir,
    )? {
        verify_task_fenced_for_advancement(
            &candidate_task,
            &adm.job,
            &adm.runtime_state,
            &adm.approval.approval_id,
            plans_dir,
        )?;
    }

    let cap_env = adm
        .linkage
        .controller_envelope
        .as_ref()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "job approval_linkage has no FrozenControllerCapabilityEnvelope authorizing correction attempts",
            )
        })?;
    if cap_env.max_in_scope_correction_attempts == 0
        || adm.runtime_state.envelope.max_in_scope_corrections_per_task == 0
    {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            format!(
                "frozen controller capability/execution envelope for job `{}` disables in-scope task corrections (max_in_scope_correction_attempts={}, max_in_scope_corrections_per_task={})",
                request.job_id,
                cap_env.max_in_scope_correction_attempts,
                adm.runtime_state.envelope.max_in_scope_corrections_per_task
            ),
        ));
    }
    if request.prior_attempt_no > cap_env.max_in_scope_correction_attempts
        || request.prior_attempt_no > adm.runtime_state.envelope.max_in_scope_corrections_per_task
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` prior_attempt_no `{}` exceeds frozen per-task correction cap (capability={}, envelope={})",
                request.task_id,
                request.prior_attempt_no,
                cap_env.max_in_scope_correction_attempts,
                adm.runtime_state.envelope.max_in_scope_corrections_per_task
            ),
        ));
    }

    if candidate_task.accepted_candidate_id.is_some()
        || matches!(
            candidate_task.stage,
            CoworkerTaskStage::Accepted
                | CoworkerTaskStage::IntegrationReserved
                | CoworkerTaskStage::Integrated
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "task `{}` is already accepted/integrated (`{}`) and cannot execute a correction attempt",
                request.task_id, candidate_task.stage
            ),
        ));
    }
    if candidate_task.stage == CoworkerTaskStage::EffectUnknown {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` is in EffectUnknown state; refusing to launch or mint a fresh key before reconciliation",
                request.task_id
            ),
        ));
    }
    if candidate_task.current_attempt_no < request.prior_attempt_no
        || candidate_task.current_attempt_no > next_attempt_no
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "stale or out-of-sequence prior_attempt_no `{}` for task `{}` (current_attempt_no `{}`)",
                request.prior_attempt_no, request.task_id, candidate_task.current_attempt_no
            ),
        ));
    }
    if candidate_task
        .candidates
        .iter()
        .any(|c| c.attempt_no > request.prior_attempt_no)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "stale prior_attempt_no `{}` for task `{}`: a newer candidate already exists",
                request.prior_attempt_no, request.task_id
            ),
        ));
    }

    if adm.runtime_state.derived_attempts.iter().any(|d| {
        d.task_id == request.task_id
            && d.attempt_no == next_attempt_no
            && d.prior_check_failure_basis.is_some()
    }) {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "task `{}` derived attempt `{next_attempt_no}` was prepared from a check-failure basis without a Reviewer verdict; execution of check-failure derived correction attempts is not enabled in this slice and refuses to launch a provider worker",
                request.task_id
            ),
        ));
    }

    let prep_request = CoworkerTaskCorrectionPrepareRequest {
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        job_id: request.job_id.clone(),
        task_id: request.task_id.clone(),
        prior_attempt_no: request.prior_attempt_no,
        now_ms: request.now_ms,
    };

    // First verify the prior attempt's ledger, artifacts, and persisted Reviewer report without
    // requiring the live worktree to be unmodified yet (in case we are reopening an already-executed
    // derived attempt whose worker already edited the worktree).
    let (prior_artifacts, prior_candidate, review_subject, recorded_verdict, _) =
        verify_prior_attempt_and_persisted_reviewer_report_with_live_check(
            controller,
            plans_dir,
            &prep_request,
            &adm,
            &candidate_task,
            &approved_task,
            false,
        )?;

    let receipt_id = compute_derived_task_attempt_receipt_id(
        &request.job_id,
        &request.task_id,
        next_attempt_no,
        &prior_candidate.candidate_id,
        &recorded_verdict.verdict_id,
        &review_subject.subject_digest,
        &adm.runtime_state.envelope.envelope_digest,
    );
    let request_key = compute_derived_coder_request_key(
        &request.job_id,
        &request.task_id,
        next_attempt_no,
        &prior_candidate.candidate_id,
        &recorded_verdict.verdict_id,
        &review_subject.subject_digest,
        &adm.runtime_state.envelope.envelope_digest,
        &candidate_task.selected_choice,
    );
    let effect_id = compute_derived_coder_effect_id(
        &request.job_id,
        &request.task_id,
        next_attempt_no,
        &request_key,
    );
    let report_artifact_id = compute_derived_coder_report_artifact_id(
        &request.job_id,
        &request.task_id,
        next_attempt_no,
        &request_key,
    );

    // Reject any EffectUnknown or conflicting state on this task.
    verify_no_unknown_or_conflicting_execution_state(
        &adm.job,
        &adm.runtime_state,
        &candidate_task,
        next_attempt_no,
        &receipt_id,
        &request_key,
        &effect_id,
    )?;

    // Reject any concurrent duplicate claim while an in-process derived execution is actively Running.
    if InFlightDerivedCorrectionGuard::is_in_flight(
        plans_dir,
        &request.binding_id,
        &request.job_id,
        &request.task_id,
        next_attempt_no,
    )? {
        return Err(NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "task `{}` derived attempt `{next_attempt_no}` (`{effect_id}`) is currently Running in-flight; duplicate claim refused",
                request.task_id
            ),
        ));
    }

    // Check if a persisted derived execution report already exists on disk or if the effect/task has
    // already moved past `DispatchReserved` (`Running`, `ReadyForReview`, `Failed`, `Cancelled`).
    let report_file_exists =
        fs::symlink_metadata(derived_execution_report_file_path(plans_dir, &effect_id)).is_ok();
    let existing_effect = adm.runtime_state.pending_effects.get(&effect_id).cloned();
    let effect_past_reserved = existing_effect
        .as_ref()
        .is_some_and(|eff| eff.status != PendingControllerEffectStatus::ReservedBeforeEffect);
    let task_past_reserved = candidate_task.current_attempt_no == next_attempt_no
        && candidate_task.stage != CoworkerTaskStage::DispatchReserved;

    if report_file_exists || effect_past_reserved || task_past_reserved {
        let outcome = reopen_or_reconcile_existing_derived_execution(
            controller,
            guard,
            request,
            plans_dir,
            &adm,
            &controller_instance_id,
            lease_epoch,
            &candidate_task,
            &approved_task,
            &prior_artifacts,
            &prior_candidate,
            &review_subject,
            &recorded_verdict,
            next_attempt_no,
            &receipt_id,
            &request_key,
            &effect_id,
            &report_artifact_id,
        )?;
        verify_original_approval_unchanged(controller, &adm)?;
        return Ok(PrepareOrClaimDisposition::CompletedOrWaiting(Box::new(
            outcome,
        )));
    }

    // Fresh execution or pre-execution `DispatchReserved` claim path: the live task worktree MUST
    // still match the reviewed candidate's ReviewSubject before the derived Coder runs.
    let _ = verify_prior_attempt_and_persisted_reviewer_report_with_live_check(
        controller,
        plans_dir,
        &prep_request,
        &adm,
        &candidate_task,
        &approved_task,
        true,
    )?;

    // Check parallel Coder capacity on OTHER tasks first (`other_active_coder_executions`).
    let other_active_coder_executions = adm
        .job
        .tasks
        .iter()
        .filter(|t| {
            t.task_id != request.task_id
                && is_task_coder_execution_active(t, &adm.job, &adm.runtime_state)
        })
        .count();
    let total_active_coder_executions = count_active_coder_executions(&adm.job, &adm.runtime_state);

    // Check if an existing `DispatchReserved` reservation can be reopened without charging a second time.
    let (receipt, outbox_record) = if let Some(reopened) =
        try_reopen_existing_correction_reservation(
            controller,
            guard,
            &prep_request,
            &adm,
            &controller_instance_id,
            lease_epoch,
            &candidate_task,
            &approved_task,
            &prior_artifacts,
            &prior_candidate,
            &review_subject,
            &recorded_verdict,
            next_attempt_no,
            &receipt_id,
            &request_key,
            &effect_id,
        )? {
        if other_active_coder_executions >= adm.approval.max_parallel {
            verify_original_approval_unchanged(controller, &adm)?;
            return Ok(PrepareOrClaimDisposition::CompletedOrWaiting(Box::new(
                CoworkerTaskCorrectionExecuteOutcome {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    approval_id: adm.approval.approval_id.clone(),
                    task_id: request.task_id.clone(),
                    prior_attempt_no: request.prior_attempt_no,
                    attempt_no: next_attempt_no,
                    status: CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity,
                    task_stage: reopened.task_stage,
                    max_parallel: adm.approval.max_parallel,
                    active_coder_executions: reopened.active_coder_executions,
                    prior_candidate_id: prior_candidate.candidate_id,
                    prior_review_verdict_id: Some(recorded_verdict.verdict_id),
                    prior_check_failure_basis: None,
                    prior_review_subject_digest: review_subject.subject_digest,
                    request_key: reopened.request_key,
                    effect_id: reopened.effect_id,
                    outbox_id: reopened.outbox_id,
                    report_artifact_id: None,
                    report_path: None,
                    receipt: reopened.receipt,
                    report: None,
                    diagnostic: None,
                    reused_existing: true,
                },
            )));
        }
        let rec = reopened.receipt.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "reopened correction reservation is missing DerivedTaskAttemptReceipt",
            )
        })?;
        let obx_id = reopened.outbox_id.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "reopened correction reservation is missing outbox_id",
            )
        })?;
        let fresh_ledger = controller
            .workflow_store()
            .read_binding_ledger(&request.binding_id)?;
        let fresh_job = fresh_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == request.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "job missing after reopening correction reservation",
                )
            })?;
        let obx = fresh_job
            .outbox
            .iter()
            .find(|o| o.outbox_id == obx_id)
            .cloned()
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "reopened outbox_id missing from job.outbox",
                )
            })?;
        (rec, obx)
    } else {
        // Fresh reservation path from `CorrectInScopeRequired` at `prior_attempt_no`.
        if candidate_task.current_attempt_no != request.prior_attempt_no
            || candidate_task.stage != CoworkerTaskStage::CorrectInScopeRequired
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` is in stage `{}` (attempt `{}`) without a reopenable reservation",
                    request.task_id, candidate_task.stage, candidate_task.current_attempt_no
                ),
            ));
        }
        if candidate_task.unresolved_outbox_id.is_some()
            || is_task_coder_execution_active(&candidate_task, &adm.job, &adm.runtime_state)
        {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                format!(
                    "task `{}` has an unresolved outbox or active Coder execution",
                    request.task_id
                ),
            ));
        }

        let task_corrections_reserved = adm
            .runtime_state
            .derived_attempts
            .iter()
            .filter(|d| d.task_id == request.task_id && d.attempt_no >= 2)
            .count() as u32;
        if task_corrections_reserved >= MAX_IN_SCOPE_CORRECTION_ATTEMPTS
            || task_corrections_reserved >= cap_env.max_in_scope_correction_attempts
            || task_corrections_reserved
                >= adm.runtime_state.envelope.max_in_scope_corrections_per_task
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` has exhausted its per-task in-scope correction budget ({} reserved)",
                    request.task_id, task_corrections_reserved
                ),
            ));
        }
        if adm.runtime_state.total_corrections_reserved
            >= adm.runtime_state.envelope.max_total_corrections_per_job
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "job `{}` has exhausted max_total_corrections_per_job ({} of {} reserved)",
                    request.job_id,
                    adm.runtime_state.total_corrections_reserved,
                    adm.runtime_state.envelope.max_total_corrections_per_job
                ),
            ));
        }
        if adm.runtime_state.total_task_attempts_reserved
            >= adm.runtime_state.envelope.max_total_task_attempts_per_job
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "job `{}` has exhausted max_total_task_attempts_per_job ({} of {} reserved)",
                    request.job_id,
                    adm.runtime_state.total_task_attempts_reserved,
                    adm.runtime_state.envelope.max_total_task_attempts_per_job
                ),
            ));
        }
        adm.runtime_state
            .ensure_role_budget_available(WorkflowRole::Coder)?;

        let rec = construct_verified_derived_attempt_receipt(
            &adm,
            &candidate_task,
            &approved_task,
            &prior_artifacts,
            &prior_candidate,
            &review_subject,
            &recorded_verdict,
            next_attempt_no,
            &receipt_id,
            request.now_ms,
        )?;

        if total_active_coder_executions >= adm.approval.max_parallel
            || other_active_coder_executions >= adm.approval.max_parallel
        {
            let _ = CoworkerTaskCorrectionPrepareStatus::WaitingForParallelCapacity;
            verify_original_approval_unchanged(controller, &adm)?;
            return Ok(PrepareOrClaimDisposition::CompletedOrWaiting(Box::new(
                CoworkerTaskCorrectionExecuteOutcome {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    approval_id: adm.approval.approval_id.clone(),
                    task_id: request.task_id.clone(),
                    prior_attempt_no: request.prior_attempt_no,
                    attempt_no: next_attempt_no,
                    status: CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity,
                    task_stage: candidate_task.stage,
                    max_parallel: adm.approval.max_parallel,
                    active_coder_executions: total_active_coder_executions,
                    prior_candidate_id: prior_candidate.candidate_id,
                    prior_review_verdict_id: Some(recorded_verdict.verdict_id),
                    prior_check_failure_basis: None,
                    prior_review_subject_digest: review_subject.subject_digest,
                    request_key: None,
                    effect_id: None,
                    outbox_id: None,
                    report_artifact_id: None,
                    report_path: None,
                    receipt: None,
                    report: None,
                    diagnostic: None,
                    reused_existing: false,
                },
            )));
        }

        controller.verify_physical_ownership(
            guard,
            &request.binding_id,
            &request.thread_uuid,
            &request.job_id,
        )?;

        controller.policy_store().mutate_job_runtime(
            &request.binding_id,
            &request.job_id,
            &controller_instance_id,
            lease_epoch,
            |state| {
                state.reserve_role_call(WorkflowRole::Coder)?;
                state.total_task_attempts_reserved = state
                    .total_task_attempts_reserved
                    .checked_add(1)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "total_task_attempts_reserved overflow",
                        )
                    })?;
                state.total_corrections_reserved = state
                    .total_corrections_reserved
                    .checked_add(1)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "total_corrections_reserved overflow",
                        )
                    })?;
                state.derived_attempts.push(rec.clone());
                state.pending_effects.insert(
                    effect_id.clone(),
                    PendingControllerEffect {
                        effect_id: effect_id.clone(),
                        request_key: request_key.clone(),
                        job_id: request.job_id.clone(),
                        task_id: Some(request.task_id.clone()),
                        attempt_no: Some(next_attempt_no),
                        kind: PendingControllerEffectKind::CoderDispatch,
                        status: PendingControllerEffectStatus::ReservedBeforeEffect,
                        subject_digest: rec.receipt_digest.clone(),
                        external_correlation_id: Some(rec.receipt_id.clone()),
                        created_at_ms: request.now_ms,
                        updated_at_ms: request.now_ms,
                        diagnostic: None,
                    },
                );
                Ok(())
            },
        )?;

        let (_, obx) =
            controller
                .workflow_store()
                .reserve_task_outbox(&ReserveTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: adm.job.job_revision,
                    expected_ledger_revision: None,
                    request_key: request_key.clone(),
                    kind: OutboxDispatchKind::TaskAttemptDispatch,
                    task_id: request.task_id.clone(),
                    role: WorkflowRole::Coder,
                    subject_digest: rec.receipt_digest.clone(),
                    dependency_input_commit: None,
                    dependency_input_tree_sha: None,
                    accepted_predecessor_receipts: Vec::new(),
                })?;
        (rec, obx)
    };

    // Validate live Coder tool/model against registry snapshot, linkage, policy, and owned worktree
    // manifests BEFORE claiming `DispatchReserved -> Running`.
    let (task_manifest, expected_provider, resolved_model_target, worker_spec) =
        build_and_validate_derived_coder_worker_spec(
            controller,
            &adm,
            &candidate_task,
            &approved_task,
            &receipt,
            &review_subject,
        )?;

    let pre_execution_head_commit = resolve_exact_commit_sha(
        &task_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("failed to resolve task worktree HEAD before derived Coder execution: {err}"),
        )
    })?;
    let pre_execution_tree_sha = resolve_exact_tree_sha(
        &task_manifest.checkout_path,
        &pre_execution_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "failed to resolve task worktree tree SHA before derived Coder execution: {err}"
            ),
        )
    })?;
    if pre_execution_head_commit != prior_candidate.head_commit
        || pre_execution_tree_sha != prior_candidate.tree_sha
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "task worktree HEAD/tree before derived Coder execution does not match prior rejected candidate",
        ));
    }

    let held_temp = open_private_derived_report_staging_fd(plans_dir, &effect_id)?;
    let in_flight_guard = InFlightDerivedCorrectionGuard::acquire(
        plans_dir,
        &request.binding_id,
        &request.job_id,
        &request.task_id,
        next_attempt_no,
    )?;

    // Claim the single existing reservation (`DispatchReserved -> Running`) with NO second Coder
    // role reservation or counter increment.
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        &controller_instance_id,
        lease_epoch,
        |state| {
            let eff = state.pending_effects.get_mut(&effect_id).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "missing ReservedBeforeEffect CoderDispatch effect `{effect_id}` during claim"
                    ),
                )
            })?;
            if eff.status != PendingControllerEffectStatus::ReservedBeforeEffect {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "CoderDispatch effect `{effect_id}` is in status `{:?}` (expected `ReservedBeforeEffect`)",
                        eff.status
                    ),
                ));
            }
            eff.external_correlation_id = Some(receipt.receipt_id.clone());
            eff.updated_at_ms = request.now_ms;
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
                "job missing from binding ledger before claiming outbox for Running",
            )
        })?;
    let (_, running_outbox) =
        controller
            .workflow_store()
            .reconcile_or_finalize_outbox(&FinalizeTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: current_job.job_revision,
                expected_ledger_revision: None,
                outbox_id: outbox_record.outbox_id.clone(),
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                next_task_stage: CoworkerTaskStage::Running,
                run_id: Some(receipt.run_id.clone()),
                worker_id: Some(receipt.worker_id.clone()),
                task_worktree_id: Some(receipt.task_worktree_id.clone()),
                head_commit: None,
                tree_sha: None,
                diff_digest: None,
                report_artifact_id: None,
                observed_model: None,
                observed_effort: None,
                receipt_id: Some(receipt.receipt_id.clone()),
                diagnostic: None,
            })?;

    Ok(PrepareOrClaimDisposition::ClaimedForExecution(Box::new(
        ClaimedDerivedExecutionContext {
            _in_flight_guard: in_flight_guard,
            controller_instance_id,
            lease_epoch,
            approval_id: adm.approval.approval_id,
            next_attempt_no,
            receipt,
            request_key,
            effect_id,
            outbox_record: running_outbox,
            report_artifact_id,
            prior_candidate,
            review_subject,
            recorded_verdict,
            task_manifest,
            expected_provider,
            approved_task: approved_task.clone(),
            resolved_model_target,
            worker_spec,
            pre_execution_head_commit,
            pre_execution_tree_sha,
            held_temp,
        },
    )))
}

fn verify_no_unknown_or_conflicting_execution_state(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    task: &CoworkerTaskRecord,
    next_attempt_no: u32,
    expected_receipt_id: &str,
    expected_request_key: &str,
    expected_effect_id: &str,
) -> Result<(), NativeError> {
    for eff in runtime_state.pending_effects.values() {
        if eff.task_id.as_deref() == Some(task.task_id.as_str()) {
            if eff.status == PendingControllerEffectStatus::EffectUnknown {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "task `{}` has an EffectUnknown pending effect `{}`; refusing to launch or mint a fresh key",
                        task.task_id, eff.effect_id
                    ),
                ));
            }
            if eff.kind == PendingControllerEffectKind::CoderDispatch
                && eff.attempt_no == Some(next_attempt_no)
                && (eff.effect_id != expected_effect_id || eff.request_key != expected_request_key)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` attempt `{next_attempt_no}` already has a conflicting CoderDispatch effect (`{}` / `{}`)",
                        task.task_id, eff.effect_id, eff.request_key
                    ),
                ));
            }
            if eff.kind == PendingControllerEffectKind::CoderDispatch
                && eff.attempt_no.is_some_and(|no| no > next_attempt_no)
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` already has a newer CoderDispatch effect for attempt `{:?}`",
                        task.task_id, eff.attempt_no
                    ),
                ));
            }
        }
    }

    for outbox in &job.outbox {
        if outbox.task_id == task.task_id {
            if outbox.status == OutboxDispatchStatus::EffectUnknown {
                return Err(NativeError::new(
                    NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                    format!(
                        "task `{}` has an EffectUnknown outbox `{}`; refusing to launch or mint a fresh key",
                        task.task_id, outbox.outbox_id
                    ),
                ));
            }
            if outbox.kind == OutboxDispatchKind::TaskAttemptDispatch
                && outbox.attempt_no == next_attempt_no
                && outbox.request_key != expected_request_key
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` attempt `{next_attempt_no}` already has a TaskAttemptDispatch outbox with conflicting request_key `{}` != `{expected_request_key}`",
                        task.task_id, outbox.request_key
                    ),
                ));
            }
            if outbox.kind == OutboxDispatchKind::TaskAttemptDispatch
                && outbox.attempt_no > next_attempt_no
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` already has a newer TaskAttemptDispatch outbox for attempt `{}`",
                        task.task_id, outbox.attempt_no
                    ),
                ));
            }
        }
    }

    for derived in &runtime_state.derived_attempts {
        if derived.task_id == task.task_id {
            if derived.attempt_no > next_attempt_no {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` already has a newer DerivedTaskAttemptReceipt for attempt `{}`",
                        task.task_id, derived.attempt_no
                    ),
                ));
            }
            if derived.attempt_no == next_attempt_no && derived.receipt_id != expected_receipt_id {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "task `{}` attempt `{next_attempt_no}` already has a conflicting DerivedTaskAttemptReceipt `{}` != `{expected_receipt_id}`",
                        task.task_id, derived.receipt_id
                    ),
                ));
            }
        }
    }

    Ok(())
}

fn build_and_validate_derived_coder_worker_spec(
    controller: &CoworkerControllerService,
    adm: &VerifiedControllerAdmission,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    receipt: &DerivedTaskAttemptReceipt,
    review_subject: &ReviewSubject,
) -> Result<
    (
        WorktreeManifest,
        WorkerProvider,
        ResolvedModelTarget,
        WorkerSpec,
    ),
    NativeError,
> {
    let coder_choice = &receipt.coder_choice;
    if coder_choice != &candidate_task.selected_choice {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "DerivedTaskAttemptReceipt.coder_choice does not match task selected_choice",
        ));
    }
    if coder_choice.route != RoleExecutionRoute::HeadlessWorker
        || !matches!(
            coder_choice.adapter,
            ToolAdapter::Antigravity | ToolAdapter::OpencodeV2 | ToolAdapter::CodexCli
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "frozen coder_choice route `{}` (`{:?}`) is not a supported headless worker route",
                coder_choice.route, coder_choice.adapter
            ),
        ));
    }
    validate_non_native_worker_choice(WorkflowRole::Coder, coder_choice)?;

    if !adm
        .linkage
        .authorizes_in_scope_correction(receipt.attempt_no)
        || !adm
            .linkage
            .approved_task_ids
            .iter()
            .any(|id| id == &candidate_task.task_id)
        || adm
            .runtime_state
            .envelope
            .task_coder_choices
            .get(&candidate_task.task_id)
            != Some(coder_choice)
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen approval linkage or envelope does not authorize the task coder_choice",
        ));
    }
    if !adm.runtime_state.envelope.policy.allows(
        WorkflowRole::Coder,
        &coder_choice.tool_id,
        &coder_choice.model_id,
        coder_choice.effort.as_deref(),
    )? {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "frozen execution policy denies the task coder_choice",
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
                WorkflowRole::Coder,
                Some(candidate_task.difficulty),
            )],
        },
        &adm.runtime_state.envelope.policy,
    )?;
    let outcome = pf.roles.first().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Coder preflight returned no role outcome",
        )
    })?;
    if !pf.ready || !outcome.ready {
        let code = match outcome.block_reason {
            Some(PreflightBlockReason::UnsupportedRoute) => NativeErrorCode::UnsupportedCapability,
            _ => NativeErrorCode::ValidationFailed,
        };
        return Err(NativeError::new(
            code,
            format!("Coder preflight blocked: {}", outcome.detail),
        ));
    }
    let live_choice = outcome
        .selected
        .as_ref()
        .map(crate::profile_flow::normalize_coder_choice)
        .transpose()?;
    if live_choice.as_ref() != Some(coder_choice) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live registry Coder resolution does not match frozen task coder_choice",
        ));
    }

    let tool_def = snapshot
        .tools
        .iter()
        .find(|t| t.id == coder_choice.tool_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                format!(
                    "frozen Coder tool `{}` is missing from registry snapshot",
                    coder_choice.tool_id
                ),
            )
        })?;
    if !tool_def.enabled || tool_def.adapter != coder_choice.adapter {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "frozen Coder tool `{}` is disabled or adapter mismatched",
                coder_choice.tool_id
            ),
        ));
    }
    let (_, executable) = threadlane_authority::resolve_and_validate_worker_executable(
        &tool_def.id,
        &candidate_task.task_id,
        tool_def.adapter,
        tool_def.executable_path.as_deref(),
    )
    .map_err(|e| NativeError::new(NativeErrorCode::ValidationFailed, e.to_string()))?;
    let Some(ref approved_exec) = approved_task.canonical_executable else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovedTaskRecord.canonical_executable is missing for headless Coder task",
        ));
    };
    if &executable != approved_exec {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "live Coder executable_path drifted from frozen ApprovedTaskRecord.canonical_executable",
        ));
    }

    let provider = match coder_choice.adapter {
        ToolAdapter::Antigravity => WorkerProvider::Antigravity,
        ToolAdapter::OpencodeV2 => WorkerProvider::OpenCodeV2,
        ToolAdapter::CodexCli => WorkerProvider::CodexCli,
        _ => {
            return Err(NativeError::new(
                NativeErrorCode::UnsupportedCapability,
                "unsupported adapter for headless Coder execution",
            ));
        }
    };
    let resolved_model_target = resolve_model_target(
        provider,
        &coder_choice.model_arg,
        coder_choice.effort.as_deref(),
    )
    .map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("invalid frozen Coder model target: {err}"),
        )
    })?;
    if approved_task.provider != Some(provider)
        || approved_task.base_model != resolved_model_target.base_model
        || approved_task.effort != resolved_model_target.effort
        || approved_task.cli_model_arg.as_deref()
            != Some(resolved_model_target.cli_model_arg.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovedTaskRecord provider/model/effort does not match frozen coder_choice",
        ));
    }

    let coder_budget = adm
        .runtime_state
        .envelope
        .role_budgets
        .get(&WorkflowRole::Coder)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "FrozenControllerExecutionEnvelope is missing Coder role budget",
            )
        })?;
    coder_budget.validate()?;
    adm.approval.budgets.validate().map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("ApprovalRecord.budgets is invalid: {err}"),
        )
    })?;
    let budgets = ExecutionBudgets {
        progress_watchdog: adm.approval.budgets.progress_watchdog.clone(),
        timeout_ms: adm
            .approval
            .budgets
            .timeout_ms
            .min(coder_budget.max_timeout_ms_per_call),
        max_line_bytes: adm.approval.budgets.max_line_bytes,
        max_stdout_bytes: adm
            .approval
            .budgets
            .max_stdout_bytes
            .min(coder_budget.max_stdout_bytes_per_call),
        max_stderr_bytes: adm
            .approval
            .budgets
            .max_stderr_bytes
            .min(coder_budget.max_stderr_bytes_per_call),
        max_events: adm.approval.budgets.max_events,
    };
    budgets.validate().map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("derived Coder ExecutionBudgets is invalid: {err}"),
        )
    })?;

    let expected_work_area_root = controller
        .sessions_dir()
        .join(&adm.approval.approval_id)
        .join("work-area");
    if review_subject.work_area_root != expected_work_area_root {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "ReviewSubject work_area_root does not match controller sessions_dir/<approval_id>/work-area",
        ));
    }
    let task_manifest =
        load_and_verify_manifest(&expected_work_area_root, &receipt.task_worktree_id).map_err(
            |err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to load and verify owned task worktree manifest: {err}"),
                )
            },
        )?;
    let int_manifest =
        load_and_verify_manifest(&expected_work_area_root, &receipt.integration_worktree_id)
            .map_err(|err| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("failed to load and verify owned integration worktree manifest: {err}"),
                )
            })?;

    let expected_write_paths: BTreeSet<String> = receipt.write_paths.iter().cloned().collect();
    if task_manifest.kind != WorktreeKind::Task
        || task_manifest.worktree_id != receipt.task_worktree_id
        || task_manifest.base_commit != receipt.execution_base_commit
        || task_manifest.allowed_write_paths != expected_write_paths
        || int_manifest.kind != WorktreeKind::RunIntegration
        || int_manifest.worktree_id != receipt.integration_worktree_id
        || int_manifest.run_id != task_manifest.run_id
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "owned task/integration worktree manifests do not match DerivedTaskAttemptReceipt",
        ));
    }
    ensure_worktree_job_inactive_readonly(&task_manifest).map_err(|err| {
        NativeError::new(
            NativeErrorCode::AmbiguousWakeupPendingReconciliation,
            format!(
                "owned task worktree `{}` has an active job lock before derived Coder launch: {err}",
                task_manifest.worktree_id
            ),
        )
    })?;

    let file_permissions = match approved_task.file_permissions {
        Some(WorkerFilePermissions::TaskFiles) => WorkerFilePermissions::TaskFiles,
        _ => {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ApprovedTaskRecord.file_permissions must be Some(TaskFiles) for headless Coder task",
            ));
        }
    };

    let mut allowed_models = BTreeSet::new();
    allowed_models.insert(coder_choice.model_arg.clone());
    allowed_models.insert(resolved_model_target.cli_model_arg.clone());
    allowed_models.insert(resolved_model_target.base_model.clone());

    let worker_spec = WorkerSpec {
        provider,
        executable,
        model: coder_choice.model_arg.clone(),
        effort: coder_choice.effort.clone(),
        allowed_models,
        session_id: None,
        standalone: approved_task
            .standalone
            .unwrap_or(matches!(provider, WorkerProvider::OpenCodeV2)),
        repository_path: PathBuf::from(&adm.linkage.project_base.repository_path),
        base_commit: receipt.execution_base_commit.clone(),
        work_area_root: task_manifest.work_area_root.clone(),
        allowed_write_paths: expected_write_paths,
        prompt: receipt.derived_prompt.clone(),
        file_permissions,
        budgets,
    };
    let _ = worker_spec.validate().map_err(|err| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("derived Coder WorkerSpec static validation failed: {err}"),
        )
    })?;

    Ok((task_manifest, provider, resolved_model_target, worker_spec))
}

#[allow(clippy::too_many_arguments)]
fn reopen_or_reconcile_existing_derived_execution(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskCorrectionExecuteRequest,
    plans_dir: &Path,
    adm: &VerifiedControllerAdmission,
    controller_instance_id: &str,
    lease_epoch: u64,
    candidate_task: &CoworkerTaskRecord,
    approved_task: &ApprovedTaskRecord,
    prior_artifacts: &TaskAttemptExecutionArtifacts,
    prior_candidate: &TaskCandidateRef,
    review_subject: &ReviewSubject,
    recorded_verdict: &StructuredReviewVerdict,
    next_attempt_no: u32,
    receipt_id: &str,
    request_key: &str,
    effect_id: &str,
    report_artifact_id: &str,
) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
    let existing_receipt = adm
        .runtime_state
        .derived_attempts
        .iter()
        .find(|d| d.task_id == request.task_id && d.attempt_no == next_attempt_no)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "task `{}` attempt `{next_attempt_no}` has execution state without a persisted DerivedTaskAttemptReceipt",
                    request.task_id
                ),
            )
        })?;
    existing_receipt.validate_against_task(candidate_task, &adm.runtime_state.envelope)?;
    let expected_receipt = construct_verified_derived_attempt_receipt(
        adm,
        candidate_task,
        approved_task,
        prior_artifacts,
        prior_candidate,
        review_subject,
        recorded_verdict,
        next_attempt_no,
        receipt_id,
        existing_receipt.recorded_at_ms,
    )?;
    if existing_receipt != expected_receipt
        || !constant_time_str_eq(
            &existing_receipt.receipt_digest,
            &expected_receipt.receipt_digest,
        )
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "existing DerivedTaskAttemptReceipt does not match canonical reconstructed receipt",
        ));
    }

    let existing_effect = adm
        .runtime_state
        .pending_effects
        .get(effect_id)
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing PendingControllerEffect `{effect_id}` for derived attempt `{next_attempt_no}`"
                ),
            )
        })?;
    let existing_outbox = adm
        .job
        .outbox
        .iter()
        .find(|o| {
            o.task_id == request.task_id
                && o.kind == OutboxDispatchKind::TaskAttemptDispatch
                && o.attempt_no == next_attempt_no
                && o.request_key == request_key
        })
        .cloned()
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "missing TaskAttemptDispatch outbox for task `{}` attempt `{next_attempt_no}`",
                    request.task_id
                ),
            )
        })?;

    // Load the persisted private 0600 execution report artifact from disk.
    let Some(persisted_report) =
        read_persisted_derived_task_execution_report(plans_dir, effect_id)?
    else {
        // Since `InFlightDerivedCorrectionGuard::is_in_flight` was already checked and is false,
        // if the task is in `Running` (or its outbox is `DispatchedEffectConfirmed` while `Running`)
        // without a persisted report artifact on disk, the previous owner crashed after claiming
        // `Running` before persisting the report. Transition to `EffectUnknown` and refuse replay!
        if candidate_task.stage == CoworkerTaskStage::Running
            || existing_effect.status == PendingControllerEffectStatus::EffectUnknown
        {
            let diag = format!(
                "task `{}` attempt `{next_attempt_no}` (`{effect_id}`) was left in Running without an in-flight owner or persisted report artifact; fenced to EffectUnknown",
                request.task_id
            );
            mark_derived_coder_effect_and_outbox_unknown(
                controller,
                request,
                controller_instance_id,
                lease_epoch,
                effect_id,
                &existing_outbox.outbox_id,
                Some(existing_receipt.run_id.clone()),
                Some(existing_receipt.worker_id.clone()),
                Some(existing_receipt.task_worktree_id.clone()),
                Some(existing_receipt.receipt_id.clone()),
                &diag,
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            format!(
                "missing persisted private 0600 PersistedDerivedTaskExecutionArtifact for effect `{effect_id}`"
            ),
        ));
    };

    if persisted_report.report_artifact_id != report_artifact_id
        || persisted_report.receipt_id != existing_receipt.receipt_id
        || !constant_time_str_eq(
            &persisted_report.receipt_digest,
            &existing_receipt.receipt_digest,
        )
        || persisted_report.request_key != request_key
        || persisted_report.effect_id != effect_id
        || persisted_report.outbox_id != existing_outbox.outbox_id
        || persisted_report.binding_id != request.binding_id
        || persisted_report.thread_uuid != request.thread_uuid
        || persisted_report.job_id != request.job_id
        || persisted_report.task_id != request.task_id
        || persisted_report.prior_attempt_no != request.prior_attempt_no
        || persisted_report.attempt_no != next_attempt_no
    {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "persisted PersistedDerivedTaskExecutionArtifact identity does not match receipt/effect/outbox",
        ));
    }

    // Apply or verify idempotent store reconciliation from the verified persisted report.
    apply_persisted_derived_report_to_stores(
        controller,
        guard,
        request,
        controller_instance_id,
        lease_epoch,
        &existing_receipt,
        &existing_outbox,
        &persisted_report,
        true,
    )
}

fn finalize_claimed_derived_coder_execution(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskCorrectionExecuteRequest,
    ctx: ClaimedDerivedExecutionContext,
    manifest_task_id: &str,
    exec_result: Result<TaskExecutionReport, threadlane_workers::WorkerError>,
) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    let adm = controller.verify_job_admission(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;
    let lease = adm.runtime_state.lease.as_ref().ok_or_else(|| {
        NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ControllerJobRuntimeState lost active driver lease during derived Coder execution",
        )
    })?;
    if !lease.active
        || lease.instance_id != ctx.controller_instance_id
        || lease.lease_epoch != ctx.lease_epoch
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ControllerJobRuntimeState driver lease changed during derived Coder execution",
        ));
    }

    let execution_report = match exec_result {
        Ok(report) => report,
        Err(worker_err) => {
            let diag = sanitize_controller_diagnostic(&format!(
                "derived Coder execution returned uncertain error before producing TaskExecutionReport: {worker_err}"
            ));
            mark_derived_coder_effect_and_outbox_unknown(
                controller,
                request,
                &ctx.controller_instance_id,
                ctx.lease_epoch,
                &ctx.effect_id,
                &ctx.outbox_record.outbox_id,
                Some(ctx.receipt.run_id.clone()),
                Some(ctx.receipt.worker_id.clone()),
                Some(ctx.receipt.task_worktree_id.clone()),
                Some(ctx.receipt.receipt_id.clone()),
                &diag,
            );
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                diag,
            ));
        }
    };

    let post_execution_head_commit = resolve_exact_commit_sha(
        &ctx.task_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .unwrap_or_else(|_| ctx.pre_execution_head_commit.clone());
    let post_execution_tree_sha = resolve_exact_tree_sha(
        &ctx.task_manifest.checkout_path,
        &post_execution_head_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .unwrap_or_else(|_| ctx.pre_execution_tree_sha.clone());

    let (worktree_change_set_digest, has_changes_from_prior_head) =
        compute_derived_worktree_change_set_digest(
            &ctx.task_manifest.checkout_path,
            &ctx.pre_execution_head_commit,
            &execution_report.diff_gate.changed_paths,
        )?;
    let (base_diff_digest, _) = compute_derived_worktree_change_set_digest(
        &ctx.task_manifest.checkout_path,
        &ctx.receipt.execution_base_commit,
        &execution_report.diff_gate.changed_paths,
    )?;

    let observed_model = execution_report
        .worker_report
        .effective_observed_model()
        .map(ToString::to_string);
    let observed_effort = execution_report
        .worker_report
        .effective_observed_effort()
        .map(ToString::to_string);

    let post_task = adm
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task missing from job after derived Coder execution",
            )
        })?;
    let fenced_after_execution = is_task_fenced_by_hold_or_sibling(
        post_task,
        &adm.job,
        &adm.runtime_state,
        &adm.approval.approval_id,
        plans_dir,
    )?;

    let (status, fenced_from_promotion, diagnostic) = classify_derived_execution_outcome(
        &adm,
        &ctx,
        &execution_report,
        &post_execution_head_commit,
        &post_execution_tree_sha,
        has_changes_from_prior_head,
        fenced_after_execution,
    );

    let mut artifact = PersistedDerivedTaskExecutionArtifact {
        schema_version: DERIVED_TASK_EXECUTION_ARTIFACT_SCHEMA_VERSION,
        report_artifact_id: ctx.report_artifact_id.clone(),
        receipt_id: ctx.receipt.receipt_id.clone(),
        receipt_digest: ctx.receipt.receipt_digest.clone(),
        request_key: ctx.request_key.clone(),
        effect_id: ctx.effect_id.clone(),
        outbox_id: ctx.outbox_record.outbox_id.clone(),
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        job_id: request.job_id.clone(),
        approval_id: ctx.approval_id.clone(),
        envelope_id: ctx.receipt.envelope_id.clone(),
        envelope_digest: ctx.receipt.envelope_digest.clone(),
        task_id: request.task_id.clone(),
        task_digest: ctx.receipt.task_digest.clone(),
        prior_attempt_no: request.prior_attempt_no,
        attempt_no: ctx.next_attempt_no,
        prior_candidate_id: ctx.prior_candidate.candidate_id.clone(),
        prior_review_verdict_id: Some(ctx.recorded_verdict.verdict_id.clone()),
        prior_check_failure_basis: None,
        prior_review_subject_digest: ctx.review_subject.subject_digest.clone(),
        original_run_id: ctx.receipt.run_id.clone(),
        original_worker_id: ctx.receipt.worker_id.clone(),
        manifest_run_id: ctx.task_manifest.run_id.clone(),
        manifest_task_id: manifest_task_id.to_string(),
        task_worktree_id: ctx.receipt.task_worktree_id.clone(),
        integration_worktree_id: ctx.receipt.integration_worktree_id.clone(),
        execution_base_commit: ctx.receipt.execution_base_commit.clone(),
        pre_execution_head_commit: ctx.pre_execution_head_commit.clone(),
        pre_execution_tree_sha: ctx.pre_execution_tree_sha.clone(),
        post_execution_head_commit,
        post_execution_tree_sha,
        worktree_change_set_digest,
        base_diff_digest,
        has_changes_from_prior_head,
        coder_choice: ctx.receipt.coder_choice.clone(),
        execution_report,
        observed_provider: ctx.expected_provider.as_str().to_string(),
        observed_model,
        observed_effort,
        status,
        fenced_from_promotion,
        diagnostic,
        recorded_at_ms: request.now_ms,
        artifact_digest: String::new(),
    };
    artifact.artifact_digest = artifact.compute_digest()?;
    artifact.validate_integrity()?;

    if let Err(err) = persist_held_derived_task_execution_report(
        plans_dir,
        &ctx.effect_id,
        ctx.held_temp,
        &artifact,
    ) {
        let diag = sanitize_controller_diagnostic(&format!(
            "failed to persist derived task execution report for effect `{}` after worker execution: {}",
            ctx.effect_id,
            err.message()
        ));
        mark_derived_coder_effect_and_outbox_unknown(
            controller,
            request,
            &ctx.controller_instance_id,
            ctx.lease_epoch,
            &ctx.effect_id,
            &ctx.outbox_record.outbox_id,
            Some(ctx.receipt.run_id.clone()),
            Some(ctx.receipt.worker_id.clone()),
            Some(ctx.receipt.task_worktree_id.clone()),
            Some(ctx.receipt.receipt_id.clone()),
            &diag,
        );
        return Err(err);
    }

    let outcome = apply_persisted_derived_report_to_stores(
        controller,
        guard,
        request,
        &ctx.controller_instance_id,
        ctx.lease_epoch,
        &ctx.receipt,
        &ctx.outbox_record,
        &artifact,
        false,
    )?;
    verify_original_approval_unchanged(controller, &adm)?;
    Ok(outcome)
}

fn classify_derived_execution_outcome(
    adm: &VerifiedControllerAdmission,
    ctx: &ClaimedDerivedExecutionContext,
    execution_report: &TaskExecutionReport,
    post_execution_head_commit: &str,
    post_execution_tree_sha: &str,
    has_changes_from_prior_head: bool,
    fenced_after_execution: bool,
) -> (CoworkerTaskCorrectionExecuteStatus, bool, Option<String>) {
    let worker_report = &execution_report.worker_report;
    if worker_report.status == WorkerRunStatus::Cancelled {
        let diag = sanitize_controller_diagnostic(
            worker_report
                .redacted_stderr_summary
                .as_deref()
                .unwrap_or("derived Coder execution was cancelled"),
        );
        return (
            CoworkerTaskCorrectionExecuteStatus::CancelledTerminal,
            false,
            Some(diag),
        );
    }

    if worker_report.cleanup_incomplete {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(
                "derived WorkerExecutionReport has cleanup_incomplete == true",
            )),
        );
    }

    if worker_report.status != WorkerRunStatus::Succeeded
        || !worker_report.has_complete_natural_completion_evidence()
    {
        let detail = worker_report
            .redacted_stderr_summary
            .as_deref()
            .unwrap_or(worker_report.status.as_str());
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(&format!(
                "derived Coder worker did not complete with Succeeded natural completion evidence (status=`{}`: {detail})",
                worker_report.status.as_str()
            ))),
        );
    }

    if worker_report.provider != ctx.expected_provider
        || (ctx.expected_provider != WorkerProvider::CodexCli
            && worker_report.effective_observed_model()
                != Some(ctx.resolved_model_target.base_model.as_str()))
        || (ctx.expected_provider == WorkerProvider::CodexCli
            && !ctx.approved_task.matches_codex_task_launch_report(
                worker_report,
                &ctx.task_manifest.checkout_path,
                &ctx.receipt.execution_base_commit,
            ))
    {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(&format!(
                "derived Coder observed model `{:?}` does not match frozen model `{}`",
                worker_report.effective_observed_model(),
                ctx.resolved_model_target.base_model
            ))),
        );
    }

    if let Some(ref exp_effort) = ctx.resolved_model_target.effort {
        if ctx.expected_provider != WorkerProvider::CodexCli
            && worker_report.effective_observed_effort() != Some(exp_effort.as_str())
        {
            return (
                CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
                false,
                Some(sanitize_controller_diagnostic(&format!(
                    "derived Coder observed effort `{:?}` does not match frozen effort `{exp_effort}`",
                    worker_report.effective_observed_effort()
                ))),
            );
        }
    }

    if !execution_report.diff_gate.passed
        || !execution_report.diff_gate.evidence_complete
        || execution_report.diff_gate.changed_paths.is_empty()
        || !execution_report.diff_gate.violations.is_empty()
        || execution_report.overall_acceptance_state
            != WorkAcceptanceState::DiffGatePassedPendingReview
    {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(&format!(
                "derived Coder failed post-run diff gate (passed={}, evidence_complete={}, changed_paths={}, violations={})",
                execution_report.diff_gate.passed,
                execution_report.diff_gate.evidence_complete,
                execution_report.diff_gate.changed_paths.len(),
                execution_report.diff_gate.violations.len()
            ))),
        );
    }

    if post_execution_head_commit != ctx.pre_execution_head_commit
        || post_execution_tree_sha != ctx.pre_execution_tree_sha
    {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(
                "derived Coder altered git HEAD directly instead of leaving working tree edits for task verification",
            )),
        );
    }

    if !has_changes_from_prior_head {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(
                "derived Coder made no modifications relative to the prior attempt's committed HEAD",
            )),
        );
    }

    let primary_repo = Path::new(&adm.linkage.project_base.repository_path);
    if !is_worktree_clean(primary_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS).unwrap_or(false) {
        return (
            CoworkerTaskCorrectionExecuteStatus::FailedTerminal,
            false,
            Some(sanitize_controller_diagnostic(
                "primary repository checkout became dirty during derived Coder execution",
            )),
        );
    }

    if fenced_after_execution {
        return (
            CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved,
            true,
            Some(sanitize_controller_diagnostic(
                "task became fenced by hold, conversational change, or stop_requested during derived Coder execution; late output preserved on disk without advancing to ReadyForReview",
            )),
        );
    }

    (
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification,
        false,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_persisted_derived_report_to_stores(
    controller: &CoworkerControllerService,
    guard: &CoworkerJobOwnershipGuard,
    request: &CoworkerTaskCorrectionExecuteRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    receipt: &DerivedTaskAttemptReceipt,
    outbox_record: &OutboxDispatchRecord,
    artifact: &PersistedDerivedTaskExecutionArtifact,
    reused_existing: bool,
) -> Result<CoworkerTaskCorrectionExecuteOutcome, NativeError> {
    let plans_dir = controller.workflow_store().plans_dir();
    let report_path_str = derived_execution_report_file_path(plans_dir, &artifact.effect_id)
        .to_string_lossy()
        .to_string();
    let duration_ms = artifact.execution_report.worker_report.duration_ms;

    controller.verify_physical_ownership(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;

    match artifact.status {
        CoworkerTaskCorrectionExecuteStatus::WaitingForParallelCapacity => {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "PersistedDerivedTaskExecutionArtifact cannot have status WaitingForParallelCapacity",
            ));
        }
        CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved => {
            controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    let should_record_completion = state
                        .pending_effects
                        .get(&artifact.effect_id)
                        .is_some_and(|eff| {
                            eff.status == PendingControllerEffectStatus::ReservedBeforeEffect
                        });
                    if should_record_completion {
                        state.record_role_completion(WorkflowRole::Coder, duration_ms);
                    }
                    if let Some(eff) = state.pending_effects.get_mut(&artifact.effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectConfirmed;
                        eff.external_correlation_id = Some(artifact.report_artifact_id.clone());
                        eff.diagnostic = artifact.diagnostic.clone();
                        eff.updated_at_ms = request.now_ms;
                    }
                    Ok(())
                },
            )?;

            let fresh_adm = controller.verify_job_admission(
                guard,
                &request.binding_id,
                &request.thread_uuid,
                &request.job_id,
            )?;
            let fresh_task = fresh_adm
                .job
                .tasks
                .iter()
                .find(|t| t.task_id == request.task_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "task missing after fenced derived execution preservation",
                    )
                })?;
            let active_count =
                count_active_coder_executions(&fresh_adm.job, &fresh_adm.runtime_state);

            return Ok(CoworkerTaskCorrectionExecuteOutcome {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                approval_id: artifact.approval_id.clone(),
                task_id: request.task_id.clone(),
                prior_attempt_no: request.prior_attempt_no,
                attempt_no: artifact.attempt_no,
                status: CoworkerTaskCorrectionExecuteStatus::FencedLateOutputPreserved,
                task_stage: fresh_task.stage,
                max_parallel: fresh_adm.approval.max_parallel,
                active_coder_executions: active_count,
                prior_candidate_id: artifact.prior_candidate_id.clone(),
                prior_review_verdict_id: artifact.prior_review_verdict_id.clone(),
                prior_check_failure_basis: artifact.prior_check_failure_basis.clone(),
                prior_review_subject_digest: artifact.prior_review_subject_digest.clone(),
                request_key: Some(artifact.request_key.clone()),
                effect_id: Some(artifact.effect_id.clone()),
                outbox_id: Some(outbox_record.outbox_id.clone()),
                report_artifact_id: Some(artifact.report_artifact_id.clone()),
                report_path: Some(report_path_str),
                receipt: Some(receipt.clone()),
                report: Some(artifact.clone()),
                diagnostic: artifact.diagnostic.clone(),
                reused_existing,
            });
        }
        CoworkerTaskCorrectionExecuteStatus::FailedTerminal
        | CoworkerTaskCorrectionExecuteStatus::CancelledTerminal => {
            let next_stage =
                if artifact.status == CoworkerTaskCorrectionExecuteStatus::CancelledTerminal {
                    CoworkerTaskStage::Cancelled
                } else {
                    CoworkerTaskStage::Failed
                };
            let diag = artifact.diagnostic.clone().unwrap_or_else(|| {
                format!(
                    "derived Coder execution ended with `{}`",
                    artifact.status.as_str()
                )
            });

            controller.policy_store().mutate_job_runtime(
                &request.binding_id,
                &request.job_id,
                controller_instance_id,
                lease_epoch,
                |state| {
                    let should_record_completion = state
                        .pending_effects
                        .get(&artifact.effect_id)
                        .is_some_and(|eff| {
                            eff.status == PendingControllerEffectStatus::ReservedBeforeEffect
                        });
                    if should_record_completion {
                        state.record_role_completion(WorkflowRole::Coder, duration_ms);
                    }
                    if let Some(eff) = state.pending_effects.get_mut(&artifact.effect_id) {
                        eff.status = PendingControllerEffectStatus::EffectFailedTerminal;
                        eff.external_correlation_id = Some(artifact.report_artifact_id.clone());
                        eff.diagnostic = Some(diag.clone());
                        eff.updated_at_ms = request.now_ms;
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
                        "job missing before finalizing failed/cancelled derived outbox",
                    )
                })?;
            let (updated_job, _) = controller.workflow_store().reconcile_or_finalize_outbox(
                &FinalizeTaskOutboxRequest {
                    binding_id: request.binding_id.clone(),
                    thread_uuid: request.thread_uuid.clone(),
                    job_id: request.job_id.clone(),
                    expected_job_revision: current_job.job_revision,
                    expected_ledger_revision: None,
                    outbox_id: outbox_record.outbox_id.clone(),
                    status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                    next_task_stage: next_stage,
                    run_id: Some(receipt.run_id.clone()),
                    worker_id: Some(receipt.worker_id.clone()),
                    task_worktree_id: Some(receipt.task_worktree_id.clone()),
                    head_commit: Some(artifact.post_execution_head_commit.clone()),
                    tree_sha: Some(artifact.post_execution_tree_sha.clone()),
                    diff_digest: Some(artifact.base_diff_digest.clone()),
                    report_artifact_id: Some(artifact.report_artifact_id.clone()),
                    observed_model: artifact.observed_model.clone(),
                    observed_effort: artifact.observed_effort.clone(),
                    receipt_id: Some(receipt.receipt_id.clone()),
                    diagnostic: Some(diag.clone()),
                },
            )?;

            let fresh_runtime = controller
                .policy_store()
                .get_job_runtime_readonly(&request.binding_id, &request.job_id)?
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "runtime state missing after terminal derived outbox finalization",
                    )
                })?;
            let updated_task = updated_job
                .tasks
                .iter()
                .find(|t| t.task_id == request.task_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "task missing after terminal derived outbox finalization",
                    )
                })?;
            let active_count = count_active_coder_executions(&updated_job, &fresh_runtime);
            let max_parallel = controller
                .authority()
                .get_for_operator(&artifact.approval_id)
                .map(|a| a.max_parallel)
                .unwrap_or(1);

            return Ok(CoworkerTaskCorrectionExecuteOutcome {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                approval_id: artifact.approval_id.clone(),
                task_id: request.task_id.clone(),
                prior_attempt_no: request.prior_attempt_no,
                attempt_no: artifact.attempt_no,
                status: artifact.status,
                task_stage: updated_task.stage,
                max_parallel,
                active_coder_executions: active_count,
                prior_candidate_id: artifact.prior_candidate_id.clone(),
                prior_review_verdict_id: artifact.prior_review_verdict_id.clone(),
                prior_check_failure_basis: artifact.prior_check_failure_basis.clone(),
                prior_review_subject_digest: artifact.prior_review_subject_digest.clone(),
                request_key: Some(artifact.request_key.clone()),
                effect_id: Some(artifact.effect_id.clone()),
                outbox_id: Some(outbox_record.outbox_id.clone()),
                report_artifact_id: Some(artifact.report_artifact_id.clone()),
                report_path: Some(report_path_str),
                receipt: Some(receipt.clone()),
                report: Some(artifact.clone()),
                diagnostic: Some(diag),
                reused_existing,
            });
        }
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification => {}
    }

    // ReadyForVerification path: validate the artifact against the receipt and task, record
    // `TaskAttemptExecutionArtifacts` with `is_derived_attempt: true`, confirm the effect, and
    // promote the task stage to `ReadyForReview`.
    let adm = controller.verify_job_admission(
        guard,
        &request.binding_id,
        &request.thread_uuid,
        &request.job_id,
    )?;
    let candidate_task = adm
        .job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task missing before ReadyForVerification promotion",
            )
        })?;
    let approved_task = adm
        .approval
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "approved task missing before ReadyForVerification promotion",
            )
        })?;

    artifact.validate_for_task_verification(receipt, candidate_task, approved_task)?;

    let art_key = ControllerJobRuntimeState::artifact_key(&request.task_id, artifact.attempt_no);
    let existing_art = adm.runtime_state.attempt_artifacts.get(&art_key).cloned();
    if let Some(ref existing) = existing_art {
        if !existing.is_derived_attempt
            || existing.task_id != request.task_id
            || existing.attempt_no != artifact.attempt_no
            || existing.run_id != receipt.run_id
            || existing.worker_id != receipt.worker_id
            || existing.task_worktree_id != receipt.task_worktree_id
            || existing.integration_worktree_id != receipt.integration_worktree_id
            || existing.execution_base_commit != receipt.execution_base_commit
            || existing.observed_provider != artifact.observed_provider
            || existing.observed_model != artifact.observed_model
            || existing.observed_effort != artifact.observed_effort
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "existing TaskAttemptExecutionArtifacts for derived attempt does not match PersistedDerivedTaskExecutionArtifact",
            ));
        }
    }

    if existing_art
        .as_ref()
        .is_none_or(|a| a.committed_head_commit.is_none())
    {
        let live_head = resolve_exact_commit_sha(
            &artifact.execution_report.manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to resolve live task worktree HEAD for derived report verification: {err}"),
            )
        })?;
        let live_tree = resolve_exact_tree_sha(
            &artifact.execution_report.manifest.checkout_path,
            &live_head,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|err| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("failed to resolve live task worktree tree SHA for derived report verification: {err}"),
            )
        })?;
        if live_head != artifact.post_execution_head_commit
            || live_tree != artifact.post_execution_tree_sha
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "live task worktree HEAD/tree drifted from PersistedDerivedTaskExecutionArtifact",
            ));
        }
        let (live_change_digest, live_has_changes) = compute_derived_worktree_change_set_digest(
            &artifact.execution_report.manifest.checkout_path,
            &artifact.pre_execution_head_commit,
            &artifact.execution_report.diff_gate.changed_paths,
        )?;
        let (live_base_digest, _) = compute_derived_worktree_change_set_digest(
            &artifact.execution_report.manifest.checkout_path,
            &artifact.execution_base_commit,
            &artifact.execution_report.diff_gate.changed_paths,
        )?;
        if !live_has_changes
            || !constant_time_str_eq(&live_change_digest, &artifact.worktree_change_set_digest)
            || !constant_time_str_eq(&live_base_digest, &artifact.base_diff_digest)
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "live task worktree change-set does not match PersistedDerivedTaskExecutionArtifact",
            ));
        }
    }

    let observed_model = artifact.observed_model.clone();
    let codex_launch_report =
        (artifact.execution_report.worker_report.provider == WorkerProvider::CodexCli).then(|| {
            super::controller_policy::compact_codex_launch_report(
                &artifact.execution_report.worker_report,
            )
        });

    let derived_attempt_artifacts = TaskAttemptExecutionArtifacts {
        task_id: request.task_id.clone(),
        attempt_no: artifact.attempt_no,
        is_derived_attempt: true,
        run_id: receipt.run_id.clone(),
        worker_id: receipt.worker_id.clone(),
        task_worktree_id: receipt.task_worktree_id.clone(),
        integration_worktree_id: receipt.integration_worktree_id.clone(),
        execution_base_commit: receipt.execution_base_commit.clone(),
        observed_provider: artifact.observed_provider.clone(),
        observed_model: observed_model.clone(),
        codex_source_report_sha256: codex_launch_report
            .as_ref()
            .map(|_| artifact.artifact_digest.clone()),
        codex_source_effect_id: codex_launch_report
            .as_ref()
            .map(|_| artifact.effect_id.clone()),
        codex_launch_report,
        observed_effort: artifact.observed_effort.clone(),
        committed_head_commit: None,
        committed_tree_sha: None,
        candidate_id: None,
        check_evidence: Vec::new(),
        test_evidence_summary: None,
        review_subject: None,
        reviewer_response_digest: None,
        review_receipt: None,
        integration_receipt: None,
    };

    controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            let should_record_completion = state
                .pending_effects
                .get(&artifact.effect_id)
                .is_some_and(|eff| {
                    eff.status == PendingControllerEffectStatus::ReservedBeforeEffect
                });
            if should_record_completion {
                state.record_role_completion(WorkflowRole::Coder, duration_ms);
            }
            if let Some(eff) = state.pending_effects.get_mut(&artifact.effect_id) {
                eff.status = PendingControllerEffectStatus::EffectConfirmed;
                eff.external_correlation_id = Some(artifact.report_artifact_id.clone());
                eff.updated_at_ms = request.now_ms;
            }
            let art_key =
                ControllerJobRuntimeState::artifact_key(&request.task_id, artifact.attempt_no);
            state
                .attempt_artifacts
                .entry(art_key)
                .or_insert(derived_attempt_artifacts);
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
                "job missing before promoting derived attempt to ReadyForReview",
            )
        })?;
    let current_task = current_job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task missing before promoting derived attempt to ReadyForReview",
            )
        })?;

    let updated_job = if matches!(
        current_task.stage,
        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
    ) {
        let (job_after, _) = controller.workflow_store().reconcile_or_finalize_outbox(
            &FinalizeTaskOutboxRequest {
                binding_id: request.binding_id.clone(),
                thread_uuid: request.thread_uuid.clone(),
                job_id: request.job_id.clone(),
                expected_job_revision: current_job.job_revision,
                expected_ledger_revision: None,
                outbox_id: outbox_record.outbox_id.clone(),
                status: OutboxDispatchStatus::DispatchedEffectConfirmed,
                next_task_stage: CoworkerTaskStage::ReadyForReview,
                run_id: Some(receipt.run_id.clone()),
                worker_id: Some(receipt.worker_id.clone()),
                task_worktree_id: Some(receipt.task_worktree_id.clone()),
                head_commit: Some(artifact.post_execution_head_commit.clone()),
                tree_sha: Some(artifact.post_execution_tree_sha.clone()),
                diff_digest: Some(artifact.base_diff_digest.clone()),
                report_artifact_id: Some(artifact.report_artifact_id.clone()),
                observed_model,
                observed_effort: artifact.observed_effort.clone(),
                receipt_id: Some(receipt.receipt_id.clone()),
                diagnostic: None,
            },
        )?;
        job_after
    } else {
        current_job.clone()
    };

    let fresh_runtime = controller
        .policy_store()
        .get_job_runtime_readonly(&request.binding_id, &request.job_id)?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "runtime state missing after promoting derived attempt",
            )
        })?;
    let updated_task = updated_job
        .tasks
        .iter()
        .find(|t| t.task_id == request.task_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task missing after promoting derived attempt",
            )
        })?;
    let active_count = count_active_coder_executions(&updated_job, &fresh_runtime);

    Ok(CoworkerTaskCorrectionExecuteOutcome {
        binding_id: request.binding_id.clone(),
        thread_uuid: request.thread_uuid.clone(),
        job_id: request.job_id.clone(),
        approval_id: adm.approval.approval_id.clone(),
        task_id: request.task_id.clone(),
        prior_attempt_no: request.prior_attempt_no,
        attempt_no: artifact.attempt_no,
        status: CoworkerTaskCorrectionExecuteStatus::ReadyForVerification,
        task_stage: updated_task.stage,
        max_parallel: adm.approval.max_parallel,
        active_coder_executions: active_count,
        prior_candidate_id: artifact.prior_candidate_id.clone(),
        prior_review_verdict_id: artifact.prior_review_verdict_id.clone(),
        prior_check_failure_basis: artifact.prior_check_failure_basis.clone(),
        prior_review_subject_digest: artifact.prior_review_subject_digest.clone(),
        request_key: Some(artifact.request_key.clone()),
        effect_id: Some(artifact.effect_id.clone()),
        outbox_id: Some(outbox_record.outbox_id.clone()),
        report_artifact_id: Some(artifact.report_artifact_id.clone()),
        report_path: Some(report_path_str),
        receipt: Some(receipt.clone()),
        report: Some(artifact.clone()),
        diagnostic: None,
        reused_existing,
    })
}

#[allow(clippy::too_many_arguments)]
fn mark_derived_coder_effect_and_outbox_unknown(
    controller: &CoworkerControllerService,
    request: &CoworkerTaskCorrectionExecuteRequest,
    controller_instance_id: &str,
    lease_epoch: u64,
    effect_id: &str,
    outbox_id: &str,
    run_id: Option<String>,
    worker_id: Option<String>,
    task_worktree_id: Option<String>,
    receipt_id: Option<String>,
    diag: &str,
) {
    let clean_diag = sanitize_controller_diagnostic(diag);
    let _ = controller.policy_store().mutate_job_runtime(
        &request.binding_id,
        &request.job_id,
        controller_instance_id,
        lease_epoch,
        |state| {
            if let Some(eff) = state.pending_effects.get_mut(effect_id) {
                eff.status = PendingControllerEffectStatus::EffectUnknown;
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
                    task_worktree_id,
                    head_commit: None,
                    tree_sha: None,
                    diff_digest: None,
                    report_artifact_id: None,
                    observed_model: None,
                    observed_effort: None,
                    receipt_id,
                    diagnostic: Some(clean_diag),
                },
            );
        }
    }
}
