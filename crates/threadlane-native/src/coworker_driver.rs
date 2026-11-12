//! Independent code controller process lifecycle and authenticated local driver RPC.
//!
//! This module implements the independent code controller process lifecycle
//! (`run-owner`, `ensure`, `status`, `reconcile`, and `cancel`) around
//! [`CoworkerPipelineService`] / [`CoworkerPipelineDriver`], [`CoworkerControllerService`],
//! and physical FD job ownership ([`CoworkerJobOwnershipStore`] / [`CoworkerJobOwnershipGuard`]).
//!
//! Key guarantees:
//! - One live physical FD owner per `(binding_id, thread_uuid, job_id)`.
//! - Private `0700` driver directory and `0600` state and Unix domain socket files.
//! - Same-EUID peer credential checks and fresh CSPRNG random nonce handshakes on local RPC.
//! - Server validates the actual held [`CoworkerJobOwnershipGuard`] and live lockfile identity
//!   before performing any action or replying; client verifies the live lock summary, instance,
//!   challenge digest, and peer credentials rather than trusting PID hints or copied metadata alone.
//! - Selects the full automatic workflow (`FullAutomaticPipeline`) by default when the frozen
//!   controller capability and execution envelopes authorize it, retaining ONE mutable
//!   [`CoworkerPipelineDriver`] (`FuturesUnordered`) across ticks and RPC connections through
//!   per-task verification, `Reviewer`, bounded in-scope `Coder` corrections, original per-task
//!   `RunIntegration`, downstream dependency seals, final multi-worker Git aggregation, combined
//!   candidate checks, and a single subject-bound `Closure` evaluation.
//! - Preserves an explicit diagnostic `Phase1InitialDispatchOnly` mode for initial-dispatch-only
//!   tests and coder-only envelopes without widening capabilities.
//! - Reconciles existing receipts without replaying unknown effects and never automatically
//!   restarts a human-stopped, failed, or uncertain host session.
//! - Persists durable `stop_requested` state before attempting narrow [`ApprovedHostClient`]
//!   task cancellation, signals active derived `Coder` correction runs via their `stop_requested`
//!   process-group watcher, and gracefully drains finite in-flight check/`Reviewer`/`Closure`
//!   calls under the held physical guard while fencing stage promotion.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::{
    validate_approved_executable_task_instructions, ApprovalRecord, AuthorityError, AuthorityStore,
};
use threadlane_core::WorkerStatus;
use threadlane_host::{
    is_host_lock_held, read_host_launch_intent_optional, read_host_manifest_optional,
    read_session_task_ledger_readonly, ApprovedHost, ApprovedHostClient, ApprovedHostConfig,
    ApprovedHostHandle, HostError, HostPhase, TaskDispatchIntent, TaskExecutionStatus,
    TaskRunReceipt,
};
use threadlane_operations::{ClosureVerdict, ClosureVerdictKind};
use threadlane_workers::validate_sha256_hex_digest;
use threadlane_workflow::WorkflowRole;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::coworker_workflow::pipeline::LiveCompletionEffects;
use crate::coworker_workflow::{
    compute_derived_coder_effect_id, derived_execution_report_file_path,
    deterministic_controller_instance_id, deterministic_prefixed_id, sha256_hex,
    CandidateCheckEvidence, CombinedCandidateStatus, ControllerJobLeaseRecord,
    ControllerJobRuntimeState, ControllerPolicyStore, CoworkerControllerPhase1Status,
    CoworkerControllerService, CoworkerControllerStepReport, CoworkerControllerTaskObservation,
    CoworkerJobCompletionOutcome, CoworkerJobCompletionRequest, CoworkerJobCompletionStatus,
    CoworkerJobLockState, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, CoworkerJobPhase,
    CoworkerJobRecord, CoworkerLockFileIdentity, CoworkerOwnershipAcquireOutcome,
    CoworkerOwnershipSummary, CoworkerPipelineDriver, CoworkerPipelineInFlightStageKind,
    CoworkerPipelineInFlightStageRef, CoworkerPipelineService, CoworkerPipelineStatus,
    CoworkerPipelineTaskSnapshot, CoworkerPipelineTickReport, CoworkerTaskRecord,
    CoworkerTaskStage, CoworkerWorkflowStore, OutboxDispatchKind, OutboxDispatchRecord,
    OutboxDispatchStatus, OwnedCoworkerControllerSession, PendingControllerEffect,
    PendingControllerEffectKind, PendingControllerEffectStatus, VerifiedControllerAdmission,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, generate_prefixed_random_id, prepare_private_dir,
    read_private_file_bounded, write_private_file_atomic,
};
use crate::mcp_pair::open_workflow_stores;
use crate::planning_driver::planning_artifacts_dir;

/// Canonical directory name under `plans_dir` for code-driver lockfiles, sockets, and state.
pub const COWORKER_DRIVER_DIR_NAME: &str = "coworker-code-driver";

/// Schema version for persisted code-driver state records.
pub const COWORKER_DRIVER_STATE_SCHEMA_VERSION: u32 = 1;

/// Schema version for local Unix domain socket RPC envelopes.
pub const COWORKER_DRIVER_RPC_SCHEMA_VERSION: u32 = 1;

/// Schema version for identity-only code-driver CLI request files.
pub const COWORKER_DRIVER_REQUEST_SCHEMA_VERSION: u32 = 1;

/// Default finite cadence between controller step ticks inside `run-owner`.
pub const DEFAULT_DRIVER_POLL_INTERVAL_MS: u64 = 50;

/// Default step limit in normal operation (`None`: unbounded step iterations; owner lifetime
/// follows real job terminal/stopped/unknown/checkpoint policy and frozen worker budgets).
pub const DEFAULT_DRIVER_MAX_STEPS: Option<u32> = None;

/// Default idle linger limit when `run-owner` is spawned by `ensure` (`None`: owner keeps RPC
/// and physical ownership available while awaiting checks/review/dependencies/hold).
pub const DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS: Option<u64> = None;

/// Default finite timeout (in milliseconds) for a single local Unix socket RPC call.
pub const DEFAULT_DRIVER_RPC_TIMEOUT_MS: u64 = 5_000;

/// Default finite timeout (in milliseconds) waiting for a spawned owner to become ready.
pub const DEFAULT_DRIVER_STARTUP_TIMEOUT_MS: u64 = 10_000;

/// Maximum byte size for a single local Unix socket RPC message line.
const MAX_DRIVER_RPC_MESSAGE_BYTES: u64 = 64 * 1024;

/// Maximum byte size for an identity-only code-driver CLI request JSON file.
const MAX_DRIVER_REQUEST_BYTES: usize = 64 * 1024;

/// Maximum byte size for a persisted code-driver state JSON file.
const MAX_DRIVER_STATE_BYTES: usize = 256 * 1024;

/// Maximum direct socket path length before using `/proc/self/fd/<dir_fd>/<name>`.
const MAX_DIRECT_UNIX_SOCKET_PATH_LEN: usize = 96;

/// Lease renewal safety margin (in milliseconds) when the owner is waiting at a pipeline checkpoint.
const LEASE_RENEWAL_MARGIN_MS: u64 = 10_000;

/// Returns the canonical private directory path under `plans_dir` used by the code driver.
#[must_use]
pub fn coworker_driver_dir(plans_dir: &Path) -> PathBuf {
    plans_dir.join(COWORKER_DRIVER_DIR_NAME)
}

/// Execution mode for the independent code controller driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverExecutionMode {
    /// Complete automatic workflow: Phase 1 host dispatch/reconcile, per-task verification checks,
    /// `Reviewer` evaluation, bounded in-scope `Coder` corrections, original per-task
    /// `RunIntegration`, downstream dependency seal preparation, final multi-worker Git
    /// aggregation, combined candidate checks, and single candidate-bound `Closure`.
    #[default]
    FullAutomaticPipeline,
    /// Explicit diagnostic mode preserving legacy Phase 1 initial `Coder` dispatch and host
    /// receipt reconciliation only (`AwaitingChecksOrReview`).
    Phase1InitialDispatchOnly,
}

impl CoworkerDriverExecutionMode {
    /// Returns the canonical `snake_case` wire identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FullAutomaticPipeline => "full_automatic_pipeline",
            Self::Phase1InitialDispatchOnly => "phase1_initial_dispatch_only",
        }
    }
}

impl std::str::FromStr for CoworkerDriverExecutionMode {
    type Err = NativeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full_automatic_pipeline" => Ok(Self::FullAutomaticPipeline),
            "phase1_initial_dispatch_only" => Ok(Self::Phase1InitialDispatchOnly),
            _ => Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!("unknown coworker driver execution mode `{value}`"),
            )),
        }
    }
}

/// Identity-only request payload for `threadlane-flow coworker-driver` subcommands.
///
/// Store directories (`state_dir`, `registry_dir`, `plans_dir`, `authority_dir`,
/// `sessions_dir`) always come from the trusted CLI context rather than the request payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerDriverRequest {
    /// Schema version (must equal [`COWORKER_DRIVER_REQUEST_SCHEMA_VERSION`]).
    #[serde(default = "default_driver_request_schema_version")]
    pub schema_version: u32,
    /// Scoped control binding identifier.
    pub binding_id: String,
    /// Scoped native thread UUID.
    pub thread_uuid: String,
    /// Target coworker job identifier.
    pub job_id: String,
    /// Optional explicit reason used by `cancel`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Optional diagnostic maximum controller step iterations for `run-owner` / `ensure`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<u32>,
    /// Optional finite cadence interval (in milliseconds) between steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Optional bounded linger window (in milliseconds) after reaching a pipeline await state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_linger_ms: Option<u64>,
    /// Optional execution mode (`full_automatic_pipeline` by default when envelope authorizes it,
    /// or `phase1_initial_dispatch_only` for diagnostic Phase 1 runs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<CoworkerDriverExecutionMode>,
}

const fn default_driver_request_schema_version() -> u32 {
    COWORKER_DRIVER_REQUEST_SCHEMA_VERSION
}

impl CoworkerDriverRequest {
    /// Loads and validates an identity-only code-driver request from a private `0600` JSON file.
    ///
    /// # Errors
    /// Returns [`NativeError`] if the file fails private-file checks, cannot be parsed, or
    /// does not match the caller's `(binding_id, thread_uuid)` scope.
    pub fn load_from_private_file(
        path: &Path,
        expected_binding_id: &str,
        expected_thread_uuid: &str,
    ) -> Result<Self, NativeError> {
        let raw =
            read_private_file_bounded(path, MAX_DRIVER_REQUEST_BYTES, "coworker_driver_request")?
                .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "coworker_driver_request file does not exist.",
                )
            })?;
        let request: Self = serde_json::from_slice(&raw)
            .map_err(|err| map_json_error("coworker_driver_request", err))?;
        request.validate_for_scope(expected_binding_id, expected_thread_uuid)?;
        Ok(request)
    }

    /// Validates this request against the trusted CLI `(binding_id, thread_uuid)` scope.
    ///
    /// # Errors
    /// Returns [`NativeError`] if the schema version, scope, or job identifier is invalid.
    pub fn validate_for_scope(
        &self,
        expected_binding_id: &str,
        expected_thread_uuid: &str,
    ) -> Result<(), NativeError> {
        if self.schema_version != COWORKER_DRIVER_REQUEST_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "unsupported coworker_driver_request schema_version {}; expected {}",
                    self.schema_version, COWORKER_DRIVER_REQUEST_SCHEMA_VERSION
                ),
            ));
        }
        if self.binding_id.trim().is_empty() || self.binding_id != expected_binding_id {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker_driver_request.binding_id does not match expected binding scope.",
            ));
        }
        if self.thread_uuid.trim().is_empty() || self.thread_uuid != expected_thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker_driver_request.thread_uuid does not match expected thread scope.",
            ));
        }
        let _ = CoworkerJobOwnershipKey::new(&self.binding_id, &self.thread_uuid, &self.job_id)?;
        if let Some(max_steps) = self.max_steps {
            if max_steps == 0 || max_steps > 4_096 {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "coworker_driver_request.max_steps must be between 1 and 4096.",
                ));
            }
        }
        if let Some(poll_ms) = self.poll_interval_ms {
            if poll_ms == 0 || poll_ms > 60_000 {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "coworker_driver_request.poll_interval_ms must be between 1 and 60000.",
                ));
            }
        }
        if let Some(linger_ms) = self.idle_linger_ms {
            if linger_ms > 60_000 {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "coworker_driver_request.idle_linger_ms must be <= 60000.",
                ));
            }
        }
        Ok(())
    }
}

/// Host lifecycle mode used by the code controller owner or reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverHostMode {
    /// Reused an already running and verified `Ready` host session.
    ReusedVerifiedReadyHost,
    /// Started a brand-new host session via [`ApprovedHost::start_fresh_only`].
    StartedFreshHost,
    /// No live host interaction was required because all reachable tasks were already reconciled.
    QuiescentNoHostRequired,
    /// Host session was not running during a read-only status inspection or offline cancel.
    NotRunningOrInspectedReadonly,
}

/// Live or historical owner state observed for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverOwnerState {
    /// Physical lock is held by a live owner verified via Unix socket RPC handshake.
    LiveVerifiedOwner,
    /// Physical lock is held by a process whose RPC socket is still initializing or unreachable.
    LockHeldRpcUnavailable,
    /// No active physical lock holder exists and any previous owner exited cleanly.
    NoActiveOwner,
    /// An unlocked lockfile or ungraceful driver state from a crashed owner was detected.
    /// Stale metadata never authorizes effects.
    StaleCrashArtifactDetected,
}

/// Disposition of an `ensure` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverEnsureDisposition {
    /// Verified and reused an already running live physical owner via local Unix socket RPC.
    ReusedLiveOwner,
    /// Spawned a new detached `threadlane-flow` owner process and verified it via RPC or exit state.
    SpawnedNewOwner,
    /// Job had already reached a quiescent state and was verified without spawning a worker.
    AlreadyQuiescentVerified,
}

/// Explicit pipeline boundary label included on all driver reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverPipelineBoundary {
    /// Full automatic pipeline through per-task verification, `Reviewer`, bounded in-scope
    /// corrections, original per-task `RunIntegration`, dependency seal preparation, combined
    /// candidate checks, and single subject-bound `Closure`.
    FullAutomaticPipelineThroughSingleClosure,
    /// Phase 1 initial task dispatch and receipt reconciliation only; task checks, reviewer/architect
    /// cycles, and task/job closure are not executed in this diagnostic mode.
    Phase1InitialDispatchAndReconcileOnly,
}

/// Explicit cancellation boundary label included on cancel reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDerivedCancellationBoundary {
    /// Durable `stop_requested` persisted before effects; active original host tasks cancelled via
    /// `ApprovedHostClient::cancel_task` and active derived `Coder` correction runs terminated via
    /// their process-group watcher, while finite in-flight check/`Reviewer`/`TaskIntegration`/`Closure`
    /// calls drain under the held ownership guard and fence stage promotion without claiming remote
    /// inference termination.
    OriginalAndDerivedSignalledDirectCheckReviewerClosureDrainedFenced,
    /// Narrow cancellation targets original `ApprovedHostClient` tasks (`task_id -> run_id`) in
    /// diagnostic Phase 1 mode.
    OriginalHostTasksOnlyDerivedIntegratedLater,
}

/// Summary of an individual original host task cancelled via [`ApprovedHostClient::cancel_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelledHostTaskSummary {
    /// Cancelled task identifier.
    pub task_id: String,
    /// Sealed host run identifier from the task receipt.
    pub run_id: String,
    /// Worker lifecycle state after the narrow cancellation call.
    pub worker_state: WorkerStatus,
    /// Worker revision after the narrow cancellation call.
    pub worker_revision: u64,
}

/// Compact summary of final job aggregation, combined candidate check verification, and single
/// candidate-bound `Closure` evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverCompletionSummary {
    /// High-level completion status (`completed`, `blocked_by_failed_checks`, `closure_not_accepted`, etc.).
    pub status: CoworkerJobCompletionStatus,
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
    /// Whether existing completed job integration receipts were verified and reused.
    pub reused_existing_receipts: bool,
    /// Compact check evidence records promoted into the combined candidate when all checks pass.
    #[serde(default)]
    pub check_evidence: Vec<CandidateCheckEvidence>,
    /// Identifier of the first failed check command when `status == BlockedByFailedChecks`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_check_id: Option<String>,
    /// Assembled `CombinedCandidateIdentity::combined_candidate_id` when candidate checks passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined_candidate_id: Option<String>,
    /// Assembled `CombinedCandidateIdentity::status` when candidate checks passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined_candidate_status: Option<CombinedCandidateStatus>,
    /// Verified `FinalCandidateSubject::subject_digest` when candidate checks passed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_candidate_subject_digest: Option<String>,
    /// Bound `PackageClosureReservation::reservation_id` (`cls_<32hex>`), if reserved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_reservation_id: Option<String>,
    /// Persisted `ClosureAttemptRecord::attempt_id` (`cat_<32hex>`), if executed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_attempt_id: Option<String>,
    /// Whether an existing verified `Accepted` closure verdict for the identical subject was reused.
    #[serde(default)]
    pub reused_cached_closure_verdict: bool,
    /// Final structured `ClosureVerdictKind` (`accepted`, `rejected`, `blocked`, `unclear`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_verdict_kind: Option<ClosureVerdictKind>,
    /// Final structured `ClosureVerdict` when `Closure` completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure_verdict: Option<ClosureVerdict>,
}

impl CoworkerDriverCompletionSummary {
    /// Builds a compact completion summary from a full [`CoworkerJobCompletionOutcome`].
    #[must_use]
    pub fn from_outcome(outcome: &CoworkerJobCompletionOutcome) -> Self {
        Self {
            status: outcome.status,
            integration_worktree_id: outcome.aggregation.integration_worktree_id.clone(),
            integration_checkout_path: outcome.aggregation.integration_checkout_path.clone(),
            integration_branch_name: outcome.aggregation.integration_branch_name.clone(),
            approved_base_commit: outcome.aggregation.approved_base_commit.clone(),
            final_integrated_target_commit: outcome
                .aggregation
                .final_integrated_target_commit
                .clone(),
            final_integrated_tree_sha: outcome.aggregation.final_integrated_tree_sha.clone(),
            reused_existing_receipts: outcome.aggregation.reused_existing_receipts,
            check_evidence: outcome.check_evidence.clone(),
            failed_check_id: outcome.failed_check_id.clone(),
            combined_candidate_id: outcome
                .combined_candidate
                .as_ref()
                .map(|c| c.combined_candidate_id.clone()),
            combined_candidate_status: outcome.combined_candidate.as_ref().map(|c| c.status),
            final_candidate_subject_digest: outcome
                .final_candidate_subject
                .as_ref()
                .map(|s| s.subject_digest.clone()),
            closure_reservation_id: outcome
                .combined_candidate
                .as_ref()
                .and_then(|c| c.closure_reservation_id.clone()),
            closure_attempt_id: outcome
                .closure_outcome
                .as_ref()
                .map(|o| o.attempt.attempt_id.clone()),
            reused_cached_closure_verdict: outcome
                .closure_outcome
                .as_ref()
                .is_some_and(|o| o.reused_cached_verdict),
            closure_verdict_kind: outcome.closure_verdict.as_ref().map(|v| v.kind),
            closure_verdict: outcome.closure_verdict.clone(),
        }
    }
}

/// Compact, cumulative report of the retained [`CoworkerPipelineDriver`] persisted in driver state
/// and returned over local status RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverPipelineReport {
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
    /// High-level truthful pipeline status (`running`, `completed`, `stopped`, `held`, etc.).
    pub status: CoworkerPipelineStatus,
    /// True when `stop_requested` is recorded and the owner is gracefully draining finite
    /// in-flight stage futures (`TaskVerification`, `TaskReview`, `TaskIntegration`, `JobCompletion`)
    /// under the held physical guard before releasing ownership.
    #[serde(default)]
    pub draining_in_flight_after_stop: bool,
    /// Descriptors of stage futures currently retained in-flight by the [`CoworkerPipelineDriver`].
    #[serde(default)]
    pub in_flight_stages: Vec<CoworkerPipelineInFlightStageRef>,
    /// Cumulative task identifiers dispatched to the approved host or derived runner as `Coder` workers.
    #[serde(default)]
    pub dispatched_task_ids: Vec<String>,
    /// Cumulative task identifiers reconciled from completed `Coder` runs into `ReadyForReview`.
    #[serde(default)]
    pub reconciled_ready_for_review_task_ids: Vec<String>,
    /// Cumulative task identifiers whose attempt committed cleanly, passed frozen checks, and promoted a `TaskReviewSubject`.
    #[serde(default)]
    pub verified_task_ids: Vec<String>,
    /// Cumulative task identifiers evaluated by a supervised `Reviewer` with a persisted `ReviewDecisionReceipt`.
    #[serde(default)]
    pub reviewed_task_ids: Vec<String>,
    /// Cumulative task identifiers integrated into the job integration worktree with a persisted `IntegrationReceipt`.
    #[serde(default)]
    pub integrated_task_ids: Vec<String>,
    /// Cumulative downstream task identifiers whose sealed `DependencyInputSeal` was prepared from integrated predecessors.
    #[serde(default)]
    pub prepared_dependency_task_ids: Vec<String>,
    /// Cumulative downstream task identifiers promoted from `PendingDependencies` to `ReadyToDispatch`.
    #[serde(default)]
    pub promoted_dependency_ready_task_ids: Vec<String>,
    /// Task identifiers currently in `CorrectInScopeRequired` awaiting derived correction execution.
    #[serde(default)]
    pub awaiting_correction_task_ids: Vec<String>,
    /// Task identifiers currently blocked by failed required frozen task-review checks.
    #[serde(default)]
    pub failed_check_task_ids: Vec<String>,
    /// Task identifiers currently fenced by an active hold, stop request, blocking conversation change, or unindependent sibling.
    #[serde(default)]
    pub fenced_task_ids: Vec<String>,
    /// Task identifiers currently blocked by an unreconciled `EffectUnknown` stage or pending effect.
    #[serde(default)]
    pub unknown_effect_task_ids: Vec<String>,
    /// Task identifiers currently in a terminal or non-recoverable `Blocked`, `Failed`, or `Cancelled` stage.
    #[serde(default)]
    pub blocked_or_failed_task_ids: Vec<String>,
    /// Per-task truthful state snapshots in job task order.
    #[serde(default)]
    pub tasks: Vec<CoworkerPipelineTaskSnapshot>,
    /// Compact final job completion summary when job aggregation, combined candidate checks, or `Closure` has run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_summary: Option<CoworkerDriverCompletionSummary>,
}

impl CoworkerDriverPipelineReport {
    /// Creates a new compact pipeline report from a [`CoworkerPipelineTickReport`].
    #[must_use]
    pub fn from_tick(tick: &CoworkerPipelineTickReport, draining_after_stop: bool) -> Self {
        let mut report = Self {
            binding_id: tick.binding_id.clone(),
            thread_uuid: tick.thread_uuid.clone(),
            job_id: tick.job_id.clone(),
            approval_id: tick.approval_id.clone(),
            job_phase: tick.job_phase,
            job_revision: tick.job_revision,
            lease_epoch: tick.lease_epoch,
            controller_instance_id: tick.controller_instance_id.clone(),
            status: tick.status,
            draining_in_flight_after_stop: draining_after_stop && !tick.in_flight_stages.is_empty(),
            in_flight_stages: tick.in_flight_stages.clone(),
            dispatched_task_ids: Vec::new(),
            reconciled_ready_for_review_task_ids: Vec::new(),
            verified_task_ids: Vec::new(),
            reviewed_task_ids: Vec::new(),
            integrated_task_ids: Vec::new(),
            prepared_dependency_task_ids: Vec::new(),
            promoted_dependency_ready_task_ids: Vec::new(),
            awaiting_correction_task_ids: tick.awaiting_correction_task_ids.clone(),
            failed_check_task_ids: tick.failed_check_task_ids.clone(),
            fenced_task_ids: tick.fenced_task_ids.clone(),
            unknown_effect_task_ids: tick.unknown_effect_task_ids.clone(),
            blocked_or_failed_task_ids: tick.blocked_or_failed_task_ids.clone(),
            tasks: tick.tasks.clone(),
            completion_summary: tick
                .completion_outcome
                .as_ref()
                .map(CoworkerDriverCompletionSummary::from_outcome),
        };
        report.merge_from_tick(tick, draining_after_stop);
        report
    }

    /// Merges a subsequent [`CoworkerPipelineTickReport`] into this cumulative report while
    /// preserving cumulative task progression lists across ticks.
    pub fn merge_from_tick(
        &mut self,
        tick: &CoworkerPipelineTickReport,
        draining_after_stop: bool,
    ) {
        self.job_phase = tick.job_phase;
        self.job_revision = tick.job_revision;
        self.lease_epoch = tick.lease_epoch;
        self.controller_instance_id = tick.controller_instance_id.clone();
        self.status = tick.status;
        self.draining_in_flight_after_stop =
            draining_after_stop && !tick.in_flight_stages.is_empty();
        self.in_flight_stages = tick.in_flight_stages.clone();
        for id in &tick.dispatched_task_ids {
            push_unique_id(&mut self.dispatched_task_ids, id);
        }
        for id in &tick.reconciled_ready_for_review_task_ids {
            push_unique_id(&mut self.reconciled_ready_for_review_task_ids, id);
        }
        for id in &tick.verified_task_ids {
            push_unique_id(&mut self.verified_task_ids, id);
        }
        for id in &tick.reviewed_task_ids {
            push_unique_id(&mut self.reviewed_task_ids, id);
        }
        for id in &tick.integrated_task_ids {
            push_unique_id(&mut self.integrated_task_ids, id);
        }
        for id in &tick.prepared_dependency_task_ids {
            push_unique_id(&mut self.prepared_dependency_task_ids, id);
        }
        for id in &tick.promoted_dependency_ready_task_ids {
            push_unique_id(&mut self.promoted_dependency_ready_task_ids, id);
        }
        for snap in &tick.tasks {
            if snap.attempt_no >= 1
                && !matches!(
                    snap.stage,
                    CoworkerTaskStage::PendingDependencies | CoworkerTaskStage::ReadyToDispatch
                )
            {
                push_unique_id(&mut self.dispatched_task_ids, &snap.task_id);
            }
            if snap.review_subject_promoted {
                push_unique_id(
                    &mut self.reconciled_ready_for_review_task_ids,
                    &snap.task_id,
                );
                push_unique_id(&mut self.verified_task_ids, &snap.task_id);
            }
            if snap.review_verdict_outcome.is_some() {
                push_unique_id(&mut self.reviewed_task_ids, &snap.task_id);
            }
            if snap.stage == CoworkerTaskStage::Integrated {
                push_unique_id(&mut self.integrated_task_ids, &snap.task_id);
            }
        }
        self.awaiting_correction_task_ids = tick.awaiting_correction_task_ids.clone();
        self.failed_check_task_ids = tick.failed_check_task_ids.clone();
        self.fenced_task_ids = tick.fenced_task_ids.clone();
        self.unknown_effect_task_ids = tick.unknown_effect_task_ids.clone();
        self.blocked_or_failed_task_ids = tick.blocked_or_failed_task_ids.clone();
        self.tasks = tick.tasks.clone();
        if let Some(ref outcome) = tick.completion_outcome {
            self.completion_summary = Some(CoworkerDriverCompletionSummary::from_outcome(outcome));
        }
    }
}

/// Persisted private (`0600`) code-driver state record for a single job.
///
/// This file is informational and diagnostic only; it never substitutes for holding the live
/// physical [`crate::coworker_workflow::CoworkerJobOwnershipGuard`] file descriptor or passing
/// the live RPC handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverStateRecord {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Scoped control binding identifier.
    pub binding_id: String,
    /// Scoped native thread UUID.
    pub thread_uuid: String,
    /// Target coworker job identifier.
    pub job_id: String,
    /// Ownership instance identifier (`cwo_<64hex>`) of the most recent physical owner.
    pub owner_instance_id: String,
    /// Challenge digest (`<64hex>`) of the most recent physical owner.
    pub owner_challenge_digest: String,
    /// Diagnostic OS process identifier hint recorded when the owner acquired the lock.
    pub owner_pid_hint: u32,
    /// Effective UID recorded when the owner acquired the lock.
    pub owner_uid: u32,
    /// Lockfile `(dev, ino)` identity recorded when the owner acquired the lock.
    pub lock_file_identity: CoworkerLockFileIdentity,
    /// Unix socket path used while the owner was active.
    pub socket_path: String,
    /// Host lifecycle mode used by the owner.
    pub host_mode: CoworkerDriverHostMode,
    /// Whether the owner is still marked active in persisted metadata (if the lock is unlocked while
    /// this flag is `true`, the previous owner crashed without graceful teardown).
    pub owner_active: bool,
    /// Whether the owner completed graceful exit and released both lease and FD lock.
    pub graceful_exit_completed: bool,
    /// Post-shutdown `HostSessionManifest::updated_at_ms` recorded when the owner cleanly shut down
    /// a self-started fresh host upon reaching a quiescent state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_quiescent_host_shutdown_updated_at_ms: Option<i64>,
    /// Post-shutdown `HostSessionManifest::instance_id` recorded when the owner cleanly shut down
    /// a self-started fresh host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_quiescent_host_instance_id: Option<String>,
    /// Number of controller step iterations executed by this owner instance.
    pub steps_completed: u32,
    /// Last controller overall status observed by this owner.
    pub last_overall_status: CoworkerControllerPhase1Status,
    /// Last full controller step report recorded by this owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_step_report: Option<CoworkerControllerStepReport>,
    /// Engine execution mode persisted by the owner.
    #[serde(default)]
    pub execution_mode: Option<CoworkerDriverExecutionMode>,
    /// Last pipeline status observed when running in `FullAutomaticPipeline` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pipeline_status: Option<CoworkerPipelineStatus>,
    /// Compact last pipeline report recorded when running in `FullAutomaticPipeline` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_pipeline_report: Option<CoworkerDriverPipelineReport>,
    /// Explicit pipeline boundary label.
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
    /// Timestamp (milliseconds since Unix epoch) when the owner started.
    pub started_at_ms: u64,
    /// Timestamp (milliseconds since Unix epoch) when this record was last updated.
    pub updated_at_ms: u64,
}

impl CoworkerDriverStateRecord {
    /// Resolves the effective execution mode recorded in this persisted state record, falling back
    /// to `pipeline_boundary` for backward compatibility with older state files.
    #[must_use]
    pub const fn effective_execution_mode(&self) -> CoworkerDriverExecutionMode {
        if let Some(mode) = self.execution_mode {
            return mode;
        }
        match self.pipeline_boundary {
            CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure => {
                CoworkerDriverExecutionMode::FullAutomaticPipeline
            }
            CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly => {
                CoworkerDriverExecutionMode::Phase1InitialDispatchOnly
            }
        }
    }
}

/// Fixed owner lifecycle boundary at which a fatal error was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoworkerDriverFailureStage {
    /// Host, socket, or pipeline setup after physical admission.
    OwnerSetup,
    /// Owner RPC handling while retaining physical ownership.
    OwnerRpc,
    /// Quiescent admission and readiness inspection.
    QuiescentCheck,
    /// Controller or full-pipeline step, including result persistence.
    PipelineTick,
    /// Graceful shutdown, final state persistence, or status validation while ownership remains held.
    OwnerTeardown,
}

/// Bounded software-owned diagnostic for the first fatal owner error.
/// Raw errors may contain provider output, paths, or credentials and are never persisted here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerDriverFatalError {
    /// Stable native error code.
    pub code: NativeErrorCode,
    /// Owner boundary, not an inferred provider failure stage.
    pub stage: CoworkerDriverFailureStage,
    /// Fixed safe diagnostic, never raw provider or error prose.
    pub message: String,
    /// First observation time in milliseconds since Unix epoch.
    pub recorded_at_ms: u64,
    /// Admitted job revision retained for the stable failure notice.
    pub job_revision: u64,
}

impl CoworkerDriverFatalError {
    fn from_error(
        error: &NativeError,
        stage: CoworkerDriverFailureStage,
        job_revision: u64,
    ) -> Self {
        Self {
            code: error.code,
            stage,
            message: safe_owner_failure_message(error).to_string(),
            recorded_at_ms: current_time_ms().max(1),
            job_revision,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), NativeError> {
        if self.recorded_at_ms == 0
            || self.job_revision == 0
            || !SAFE_OWNER_FAILURE_MESSAGES.contains(&self.message.as_str())
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "invalid bounded owner failure diagnostic",
            ));
        }
        Ok(())
    }
}

const SAFE_OWNER_FAILURE_MESSAGES: &[&str] = &[
    "Owner execution failed; inspect the recorded error code and stage.",
    "Task verification rejected a worktree with no changes at the expected commit.",
    "Task verification rejected the pre-commit diff gate or found no in-scope changes.",
    "Task worktree manifest does not match the frozen task scope.",
    "Integration worktree manifest does not match task ownership or run scope.",
    "Controller lease is inactive or belongs to another physical owner.",
    "Controller lease changed during task verification.",
    "Controller lease changed during task review.",
    "Controller lease changed during job completion.",
    "Post-commit task worktree verification failed.",
];

fn safe_owner_failure_message(error: &NativeError) -> &'static str {
    if error.code != NativeErrorCode::ValidationFailed {
        return SAFE_OWNER_FAILURE_MESSAGES[0];
    }
    match error.message.as_str() {
        "task worktree has no uncommitted changes to commit at expected pre-commit HEAD" => SAFE_OWNER_FAILURE_MESSAGES[1],
        "task worktree failed pre-commit diff gate or has no in-scope changes" => SAFE_OWNER_FAILURE_MESSAGES[2],
        "owned task worktree manifest does not match TaskAttemptExecutionArtifacts or frozen task scope"
        | "task WorktreeManifest ownership, repository, base_commit, or allowed_write_paths drifted from frozen task scope" => SAFE_OWNER_FAILURE_MESSAGES[3],
        "integration WorktreeManifest does not match task WorktreeManifest ownership/run scope" => SAFE_OWNER_FAILURE_MESSAGES[4],
        "ControllerJobRuntimeState driver lease is not active for the current physical owner"
        | "Controller lease ownership mismatch or lease is no longer active."
        | "Lease instance_id or expected_lease_epoch does not match current lease owner." => SAFE_OWNER_FAILURE_MESSAGES[5],
        "controller driver lease changed during task verification" => SAFE_OWNER_FAILURE_MESSAGES[6],
        "controller driver lease changed during task review stage" => SAFE_OWNER_FAILURE_MESSAGES[7],
        "controller driver lease changed during job completion" => SAFE_OWNER_FAILURE_MESSAGES[8],
        "post-commit task worktree verification failed" => SAFE_OWNER_FAILURE_MESSAGES[9],
        _ => SAFE_OWNER_FAILURE_MESSAGES[0],
    }
}

/// Separate from replaceable lifecycle snapshots so setup failures and later stop/reopen
/// projections cannot erase the first diagnosis. Scope is checked on every read.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredOwnerFailure {
    key: CoworkerJobOwnershipKey,
    owner_instance_id: String,
    fatal_error: CoworkerDriverFatalError,
}

/// Status and step report returned by `status`, `reconcile`, and `run-owner`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverStatusReport {
    /// Scoped control binding identifier.
    pub binding_id: String,
    /// Scoped native thread UUID.
    pub thread_uuid: String,
    /// Target coworker job identifier.
    pub job_id: String,
    /// Admitted approval identifier from the controller execution envelope.
    pub approval_id: String,
    /// Observed owner state (`live_verified_owner`, `no_active_owner`, `stale_crash_artifact_detected`, etc.).
    pub owner_state: CoworkerDriverOwnerState,
    /// Durable first fatal owner diagnosis, independent of task completion and stop state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fatal_error: Option<CoworkerDriverFatalError>,
    /// Verified live ownership summary when a live owner is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_owner_summary: Option<CoworkerOwnershipSummary>,
    /// Durable scoped role diagnostics; only exact current verified owner/effect pairs are live.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_health: Vec<crate::provider_health::ProviderHealthView>,
    /// Persisted lease record in `ControllerPolicyStore`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<ControllerJobLeaseRecord>,
    /// Whether the persisted lease in `ControllerPolicyStore` is currently marked active.
    pub lease_active: bool,
    /// Persisted lease owner instance identifier in `ControllerPolicyStore`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_owner_instance_id: Option<String>,
    /// Observed host session phase in `sessions_dir/<approval_id>` if a manifest exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_phase: Option<HostPhase>,
    /// Whether `sessions_dir/<approval_id>/host.lock` is currently held by a live process.
    pub host_lock_held: bool,
    /// Host lifecycle mode used for this operation.
    pub host_mode: CoworkerDriverHostMode,
    /// Current coworker job phase in `CoworkerWorkflowStore`.
    pub job_phase: CoworkerJobPhase,
    /// Legacy Phase 1 controller status observation (`AwaitingChecksOrReview`, `RunningTasks`, `AllTasksIntegrated`, `Stopped`, etc.).
    /// Kept separate and additive from `pipeline_status`; `AllTasksIntegrated` is NOT job `Completed`
    /// until `job_phase == CoworkerJobPhase::Completed` and `pipeline_status == Some(CoworkerPipelineStatus::Completed)`.
    pub overall_status: CoworkerControllerPhase1Status,
    /// Effective driver execution mode (`full_automatic_pipeline` or `phase1_initial_dispatch_only`).
    #[serde(default)]
    pub execution_mode: CoworkerDriverExecutionMode,
    /// Truthful full-pipeline status (`running`, `completed`, `stopped`, `held`, etc.) when operating in
    /// `FullAutomaticPipeline` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_status: Option<CoworkerPipelineStatus>,
    /// True when `stop_requested` is set and the live owner is gracefully draining finite in-flight
    /// stage futures under the held physical guard before exiting.
    #[serde(default)]
    pub stopping_draining_in_flight: bool,
    /// Stage futures currently retained in-flight by the live owner's [`CoworkerPipelineDriver`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub in_flight_stages: Vec<CoworkerPipelineInFlightStageRef>,
    /// Compact pipeline report when operating in `FullAutomaticPipeline` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_report: Option<CoworkerDriverPipelineReport>,
    /// Whether durable operator stop (`stop_requested`) is set on the job runtime state.
    pub stop_requested: bool,
    /// Whether the job has unresolved pending effects or unknown outbox state blocking replay.
    pub blocked_on_unknown_effect: bool,
    /// Optional stop or block reason recorded in runtime state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_or_block_reason: Option<String>,
    /// Total task execution attempts reserved across the job in `ControllerJobRuntimeState`.
    pub total_task_attempts_reserved: u32,
    /// Maximum total task attempts permitted across the job by `FrozenControllerExecutionEnvelope`.
    pub max_total_task_attempts_per_job: u32,
    /// Per-task Phase 1 observations.
    pub tasks: Vec<CoworkerControllerTaskObservation>,
    /// Latest recorded Phase 1 step report. When the owner is inactive this is historical;
    /// its past running count is not current host evidence. Use `tasks` and `pipeline_report`
    /// for the current read-only projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_report: Option<CoworkerControllerStepReport>,
    /// Explicit pipeline boundary.
    pub pipeline_boundary: CoworkerDriverPipelineBoundary,
    /// Human-readable boundary explanation.
    pub boundary_note: String,
}

/// Report returned by `ensure`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverEnsureReport {
    /// Whether `ensure` reused an active verified owner, spawned a new owner, or verified a quiescent job.
    pub disposition: CoworkerDriverEnsureDisposition,
    /// Current verified status report for the job.
    pub status: CoworkerDriverStatusReport,
}

/// Report returned by `cancel`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverCancelReport {
    /// Scoped control binding identifier.
    pub binding_id: String,
    /// Scoped native thread UUID.
    pub thread_uuid: String,
    /// Target coworker job identifier.
    pub job_id: String,
    /// Admitted approval identifier.
    pub approval_id: String,
    /// Reason recorded for the cancellation request.
    pub reason: String,
    /// Confirms that durable `stop_requested = true` was persisted in `ControllerPolicyStore`
    /// before any local owner RPC or `ApprovedHostClient::cancel_task` call.
    pub durable_stop_recorded_before_host_cancel: bool,
    /// Whether an active verified owner was notified over local RPC and shut down cleanly (or began draining).
    pub notified_live_owner: bool,
    /// Whether narrow host task cancellation was attempted against an active `Ready` host.
    pub narrow_host_cancellation_attempted: bool,
    /// Original host tasks cancelled via `ApprovedHostClient::cancel_task`.
    pub cancelled_host_tasks: Vec<CancelledHostTaskSummary>,
    /// Explicit derived-attempt and in-flight stage cancellation boundary.
    pub derived_attempt_cancellation_boundary: CoworkerDerivedCancellationBoundary,
    /// Human-readable explanation of the cancellation boundary.
    pub cancellation_boundary_note: String,
    /// Post-cancellation status report for the job.
    pub status: CoworkerDriverStatusReport,
}

/// Options controlling `run-owner` execution cadence, mode, and optional diagnostic bounds.
///
/// By default (`max_steps: None, idle_linger_ms: None, execution_mode: None`), the owner selects
/// `FullAutomaticPipeline` when the frozen controller envelope authorizes it and drives the single
/// retained [`CoworkerPipelineDriver`] through verification, `Reviewer`, bounded corrections,
/// per-task integration, dependency seals, combined candidate checks, and single `Closure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoworkerDriverRunOptions {
    /// Optional diagnostic maximum number of controller step iterations (`None` = unbounded normal lifetime).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<u32>,
    /// Finite cadence interval (in milliseconds) between steps while tasks are running.
    pub poll_interval_ms: u64,
    /// Optional diagnostic linger window (in milliseconds) before exiting a pipeline await state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_linger_ms: Option<u64>,
    /// Optional explicit execution mode (`None` = select `FullAutomaticPipeline` by default when
    /// the frozen controller envelope authorizes it, preserving persisted diagnostic mode on reopen).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<CoworkerDriverExecutionMode>,
}

impl Default for CoworkerDriverRunOptions {
    fn default() -> Self {
        Self {
            max_steps: DEFAULT_DRIVER_MAX_STEPS,
            poll_interval_ms: DEFAULT_DRIVER_POLL_INTERVAL_MS,
            idle_linger_ms: DEFAULT_ENSURE_OWNER_IDLE_LINGER_MS,
            execution_mode: None,
        }
    }
}

/// Local Unix socket RPC operation requested by a same-EUID client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DriverRpcOp {
    /// Read-only status and live-owner verification handshake.
    Status,
    /// Trigger an immediate controller step/reconcile under the owner's held physical guard.
    Reconcile,
    /// Record durable stop, cancel active original host tasks, reconcile, and exit owner cleanly.
    Cancel {
        /// Cancellation reason.
        reason: String,
    },
}

/// Local Unix socket RPC request message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverRpcRequest {
    schema_version: u32,
    binding_id: String,
    thread_uuid: String,
    job_id: String,
    expected_instance_id: String,
    expected_challenge_digest: String,
    client_nonce: String,
    op: DriverRpcOp,
}

/// Local Unix socket RPC response message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DriverRpcResponse {
    schema_version: u32,
    echoed_client_nonce: String,
    server_nonce: String,
    owner_summary: CoworkerOwnershipSummary,
    status: Option<CoworkerDriverStatusReport>,
    cancelled_host_tasks: Vec<CancelledHostTaskSummary>,
    narrow_host_cancellation_attempted: bool,
    error: Option<String>,
}

/// Active host session state resolved before stepping a job.
struct ActiveHostSession {
    mode: CoworkerDriverHostMode,
    owned_handle: Option<ApprovedHostHandle>,
}

/// Narrow, non-authoritative read-only scoped job inspection bundle used ONLY for read-only
/// status projection and pre-cancel scope checks when not holding the physical ownership guard.
///
/// Authoritative pre-effect validation ALWAYS uses [`OwnedCoworkerControllerSession::verify_admission`]
/// under the held [`crate::coworker_workflow::CoworkerJobOwnershipGuard`].
struct ReadonlyScopedJobInspection {
    job: CoworkerJobRecord,
    runtime_state: ControllerJobRuntimeState,
    approval: ApprovalRecord,
    // Ephemeral exact task/attempt proof, minted only by the authenticated live-derived
    // matcher below. Never deserialized from cached state or inferred from owner presence.
    verified_live_derived_attempts: std::collections::BTreeMap<String, u32>,
}

/// Independent code controller process lifecycle driver.
#[derive(Clone)]
pub struct CoworkerCodeDriver {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    authority: Arc<AuthorityStore>,
    controller: CoworkerControllerService,
    flow_binary_override: Option<PathBuf>,
}

impl std::fmt::Debug for CoworkerCodeDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoworkerCodeDriver")
            .field("state_dir", &self.state_dir)
            .field("registry_dir", &self.registry_dir)
            .field("plans_dir", &self.plans_dir)
            .field("authority_dir", &self.authority_dir)
            .field("sessions_dir", &self.sessions_dir)
            .field(
                "driver_dir",
                &self.controller.ownership_store().lock_root_dir(),
            )
            .finish_non_exhaustive()
    }
}

impl CoworkerCodeDriver {
    /// Opens a [`CoworkerCodeDriver`] from the canonical workspace store directories.
    ///
    /// Uses [`coworker_driver_dir`]`(plans_dir)` (`plans_dir/coworker-code-driver`, mode `0700`)
    /// as the physical ownership and driver state root directory, and attaches
    /// [`planning_artifacts_dir`]`(plans_dir)` for planner provenance verification.
    ///
    /// # Errors
    /// Returns [`NativeError`] if any store directory or authority store cannot be opened.
    pub fn open_from_dirs(
        state_dir: impl Into<PathBuf>,
        registry_dir: impl Into<PathBuf>,
        plans_dir: impl Into<PathBuf>,
        authority_dir: impl Into<PathBuf>,
        sessions_dir: impl Into<PathBuf>,
    ) -> Result<Self, NativeError> {
        let state_dir = state_dir.into();
        let registry_dir = registry_dir.into();
        let plans_dir = plans_dir.into();
        let authority_dir = authority_dir.into();
        let sessions_dir = sessions_dir.into();

        prepare_private_dir(&state_dir, "state_dir")?;
        prepare_private_dir(&registry_dir, "registry_dir")?;
        prepare_private_dir(&plans_dir, "plans_dir")?;
        prepare_private_dir(&authority_dir, "authority_dir")?;
        prepare_private_dir(&sessions_dir, "sessions_dir")?;

        let (registry, plans) = open_workflow_stores(Some(&registry_dir), Some(&plans_dir))?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "coworker code driver requires registry_dir and plans_dir.",
                )
            })?;
        let authority = Arc::new(
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .map_err(map_authority_error)?,
        );
        let workflow_store = CoworkerWorkflowStore::new(&plans_dir)?;
        let policy_store = ControllerPolicyStore::new(&plans_dir)?;
        let driver_root = coworker_driver_dir(&plans_dir);
        prepare_private_dir(&driver_root, "coworker_driver_dir")?;
        let ownership_store = CoworkerJobOwnershipStore::new(&driver_root)?;
        let artifacts_dir = planning_artifacts_dir(&plans_dir);
        let controller = CoworkerControllerService::new(
            workflow_store,
            policy_store,
            ownership_store,
            Arc::clone(&authority),
            &sessions_dir,
        )?
        .with_planning_artifacts_dir(&artifacts_dir)?;

        Ok(Self {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            authority,
            controller,
            flow_binary_override: None,
        })
    }

    /// Constructs a [`CoworkerCodeDriver`] around an already-configured [`CoworkerControllerService`].
    ///
    /// # Errors
    /// Returns [`NativeError`] if any directory fails private-directory validation.
    pub fn from_controller(
        state_dir: impl Into<PathBuf>,
        registry_dir: impl Into<PathBuf>,
        plans_dir: impl Into<PathBuf>,
        authority_dir: impl Into<PathBuf>,
        sessions_dir: impl Into<PathBuf>,
        authority: Arc<AuthorityStore>,
        controller: CoworkerControllerService,
    ) -> Result<Self, NativeError> {
        let state_dir = state_dir.into();
        let registry_dir = registry_dir.into();
        let plans_dir = plans_dir.into();
        let authority_dir = authority_dir.into();
        let sessions_dir = sessions_dir.into();

        prepare_private_dir(&state_dir, "state_dir")?;
        prepare_private_dir(&registry_dir, "registry_dir")?;
        prepare_private_dir(&plans_dir, "plans_dir")?;
        prepare_private_dir(&authority_dir, "authority_dir")?;
        prepare_private_dir(&sessions_dir, "sessions_dir")?;
        prepare_private_dir(
            controller.ownership_store().lock_root_dir(),
            "coworker_driver_dir",
        )?;

        Ok(Self {
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            authority,
            controller,
            flow_binary_override: None,
        })
    }

    /// Overrides the `threadlane-flow` executable path used when `ensure` spawns a detached owner.
    #[must_use]
    pub fn with_flow_binary_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.flow_binary_override = Some(path.into());
        self
    }

    /// Returns a reference to the underlying [`CoworkerControllerService`].
    #[must_use]
    pub const fn controller(&self) -> &CoworkerControllerService {
        &self.controller
    }

    /// Returns the private driver directory used for lockfiles, Unix sockets, and state files.
    #[must_use]
    pub fn driver_dir(&self) -> &Path {
        self.controller.ownership_store().lock_root_dir()
    }

    /// Returns the deterministic private state file path for `(binding_id, thread_uuid, job_id)`.
    ///
    /// # Errors
    /// Returns [`NativeError`] if the ownership key is invalid or the driver directory is unsafe.
    pub fn state_file_path_for_key(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<PathBuf, NativeError> {
        prepare_private_dir(self.driver_dir(), "coworker_driver_dir")?;
        Ok(self.driver_dir().join(state_file_name_for_key(key)))
    }

    /// Returns the deterministic Unix domain socket path for `(binding_id, thread_uuid, job_id)`.
    ///
    /// # Errors
    /// Returns [`NativeError`] if the ownership key is invalid or the driver directory is unsafe.
    pub fn socket_file_path_for_key(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<PathBuf, NativeError> {
        prepare_private_dir(self.driver_dir(), "coworker_driver_dir")?;
        Ok(self.driver_dir().join(socket_file_name_for_key(key)))
    }

    /// Reads the persisted [`CoworkerDriverStateRecord`] for `key`, if one exists.
    ///
    /// # Errors
    /// Returns [`NativeError`] if the file fails `0600` private-file checks or does not match `key`.
    pub fn read_driver_state_optional(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<Option<CoworkerDriverStateRecord>, NativeError> {
        let path = self.state_file_path_for_key(key)?;
        let Some(raw) =
            read_private_file_bounded(&path, MAX_DRIVER_STATE_BYTES, "coworker_driver_state")?
        else {
            return Ok(None);
        };
        let record: CoworkerDriverStateRecord = serde_json::from_slice(&raw)
            .map_err(|err| map_json_error("coworker_driver_state", err))?;
        validate_driver_state_record(&record, key)?;
        Ok(Some(record))
    }

    /// Reads the bounded first fatal diagnosis without advancing execution or replaying notices.
    pub fn read_owner_failure_optional(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<Option<CoworkerDriverFatalError>, NativeError> {
        read_owner_failure(self.driver_dir(), key)
    }

    // Diagnostics must never replace the primary error. Persist before the bounded existing
    // bridge notice and before draining or releasing physical ownership.
    async fn report_owner_failure(
        &self,
        session: &OwnedCoworkerControllerSession,
        admitted_job_revision: u64,
        stage: CoworkerDriverFailureStage,
        error: NativeError,
    ) -> NativeError {
        let revision = session
            .verify_admission()
            .map_or(admitted_job_revision, |a| a.job.job_revision);
        let failure = CoworkerDriverFatalError::from_error(&error, stage, revision);
        // A failed persistence attempt still permits one bounded wakeup, never a replay.
        let failure = persist_owner_failure(self.driver_dir(), session.guard(), failure.clone())
            .unwrap_or(failure);
        if let Ok(store) = crate::coworker_notice::CoworkerNoticeStore::new(
            crate::coworker_notice::coworker_notice_dir(
                self.controller.workflow_store().plans_dir(),
            ),
        ) {
            let _ = store.notify_fatal_error(session, &failure).await;
        }
        error
    }

    /// Runs the physical owner lifecycle (`run-owner`) for `(binding_id, thread_uuid, job_id)`.
    ///
    /// Acquires the exclusive physical [`OwnedCoworkerControllerSession`], verifies the full
    /// planner provenance + approval + controller envelope admission under the held physical
    /// guard, binds the private `0600` Unix domain socket for same-EUID + fresh-nonce RPCs,
    /// reuses a verified `Ready` host or starts a fresh host via [`ApprovedHost::start_fresh_only`]
    /// when permitted, retains ONE mutable [`CoworkerPipelineDriver`] across ticks and RPCs when
    /// running in `FullAutomaticPipeline` mode, and upon graceful exit releases the logical lease,
    /// shuts down any self-started host handle when no worker is actively running, unlinks the
    /// socket, and drops the physical FD lock.
    ///
    /// # Errors
    /// Returns [`NativeError`] if another live owner already holds the job, if admission or host
    /// verification fails, or if stepping fails.
    pub async fn run_owner(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        options: CoworkerDriverRunOptions,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        verify_no_full_job_external_resolution(
            self.controller.workflow_store(),
            binding_id,
            job_id,
        )?;
        let max_steps = options.max_steps.filter(|&n| n > 0).map(|n| n.min(4_096));
        let poll_interval_ms = options.poll_interval_ms.clamp(1, 60_000);
        let idle_linger_ms = options.idle_linger_ms.map(|ms| ms.min(60_000));

        let started_at_ms = current_time_ms();
        let session =
            self.controller
                .open_owned_session(binding_id, thread_uuid, job_id, started_at_ms)?;
        let admission = match session.verify_admission() {
            Ok(adm) => adm,
            Err(err) => {
                let _ = session.release();
                return Err(err);
            }
        };

        let initial_prev_state = match self.read_driver_state_optional(&key) {
            Ok(st) => st,
            Err(err) => {
                let err = self
                    .report_owner_failure(
                        &session,
                        admission.job.job_revision,
                        CoworkerDriverFailureStage::OwnerSetup,
                        err,
                    )
                    .await;
                let _ = session.release();
                return Err(err);
            }
        };
        let effective_mode = match resolve_effective_execution_mode(
            options.execution_mode,
            initial_prev_state.as_ref(),
            &admission.job,
            &admission.runtime_state,
        ) {
            Ok(mode) => mode,
            Err(err) => {
                // An explicitly requested mode outside the frozen envelope is a caller
                // refusal, not a new fatal failure of the admitted execution.
                let _ = session.release();
                return Err(err);
            }
        };

        // Read-only host gates are admission refusals even for direct run_owner callers.
        // Keep their original error and do not turn an existing stopped/uncertain host into
        // a new fatal owner diagnosis. Actual startup/socket/runtime faults remain below.
        if let Err(err) = self
            .preflight_host_for_admission(&key, &admission, effective_mode)
            .await
        {
            let _ = session.release();
            return Err(err);
        }

        // Ensure host readiness before entering the step/RPC loop. If host acquisition fails,
        // release the owned session cleanly before returning the error.
        let mut host_session = match self
            .ensure_host_for_admission(&key, &admission, effective_mode)
            .await
        {
            Ok(host) => host,
            Err(err) => {
                let err = self
                    .report_owner_failure(
                        &session,
                        admission.job.job_revision,
                        CoworkerDriverFailureStage::OwnerSetup,
                        err,
                    )
                    .await;
                let _ = session.release();
                return Err(err);
            }
        };

        let sock_file_name = socket_file_name_for_key(&key);
        let canonical_socket_path = self.socket_file_path_for_key(&key)?;
        let listener = match bind_private_unix_listener(self.driver_dir(), &sock_file_name) {
            Ok(listener) => listener,
            Err(err) => {
                let err = self
                    .report_owner_failure(
                        &session,
                        admission.job.job_revision,
                        CoworkerDriverFailureStage::OwnerSetup,
                        err,
                    )
                    .await;
                if let Some(handle) = host_session.owned_handle.take() {
                    let _ = handle.shutdown().await;
                }
                let _ = session.release();
                return Err(err);
            }
        };

        let pipeline_service =
            if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
                match CoworkerPipelineService::new(self.controller.clone()) {
                    Ok(svc) => Some(svc),
                    Err(err) => {
                        let err = self
                            .report_owner_failure(
                                &session,
                                admission.job.job_revision,
                                CoworkerDriverFailureStage::OwnerSetup,
                                err,
                            )
                            .await;
                        let _ = remove_socket_if_present(&canonical_socket_path);
                        if let Some(handle) = host_session.owned_handle.take() {
                            let _ = handle.shutdown().await;
                        }
                        let _ = session.release();
                        return Err(err);
                    }
                }
            } else {
                None
            };

        // Create ONE mutable pipeline driver for the entire held owner session so FuturesUnordered
        // and its in-flight stage executions are retained across ticks and RPC connections.
        let mut pipeline_driver: Option<CoworkerPipelineDriver<'_>> = None;
        let mut pipeline_driver_init_err: Option<NativeError> = None;
        if let Some(ref svc) = pipeline_service {
            match svc.driver(
                session.guard(),
                key.binding_id.clone(),
                key.thread_uuid.clone(),
                key.job_id.clone(),
            ) {
                Ok(drv) => {
                    pipeline_driver = Some(drv);
                }
                Err(err) => {
                    pipeline_driver_init_err = Some(err);
                }
            }
        }
        if let Some(err) = pipeline_driver_init_err {
            let err = self
                .report_owner_failure(
                    &session,
                    admission.job.job_revision,
                    CoworkerDriverFailureStage::OwnerSetup,
                    err,
                )
                .await;
            drop(pipeline_driver);
            let _ = remove_socket_if_present(&canonical_socket_path);
            if let Some(handle) = host_session.owned_handle.take() {
                let _ = handle.shutdown().await;
            }
            let _ = session.release();
            return Err(err);
        }
        let in_flight_stages_rx = pipeline_driver
            .as_ref()
            .map(CoworkerPipelineDriver::subscribe_in_flight_stages);

        let live_completion_effects_rx = pipeline_driver
            .as_ref()
            .map(CoworkerPipelineDriver::subscribe_live_completion_effects);

        let mut steps_completed = 0u32;
        let mut last_step_report: Option<CoworkerControllerStepReport> = None;
        let mut last_pipeline_report: Option<CoworkerDriverPipelineReport> = initial_prev_state
            .as_ref()
            .and_then(|s| s.last_pipeline_report.clone());
        let mut draining_after_stop = admission.runtime_state.stop_requested;

        let initial_state_res: Result<CoworkerControllerPhase1Status, NativeError> = (|| {
            let owner_summary = session.guard().validate_before_effect(&key)?.clone();
            let inspection = self.inspect_scoped_job_readonly(&key)?;
            let (initial_overall_status, _) = self.synthesize_status_and_tasks(&inspection)?;
            let initial_state = CoworkerDriverStateRecord {
                schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
                binding_id: key.binding_id.clone(),
                thread_uuid: key.thread_uuid.clone(),
                job_id: key.job_id.clone(),
                owner_instance_id: owner_summary.instance_id,
                owner_challenge_digest: owner_summary.challenge_digest,
                owner_pid_hint: owner_summary.pid_hint,
                owner_uid: current_effective_uid(),
                lock_file_identity: owner_summary.lock_file_identity,
                socket_path: canonical_socket_path.display().to_string(),
                host_mode: host_session.mode,
                owner_active: true,
                graceful_exit_completed: false,
                owner_quiescent_host_shutdown_updated_at_ms: initial_prev_state
                    .as_ref()
                    .and_then(|s| s.owner_quiescent_host_shutdown_updated_at_ms),
                owner_quiescent_host_instance_id: initial_prev_state
                    .as_ref()
                    .and_then(|s| s.owner_quiescent_host_instance_id.clone()),
                steps_completed,
                last_overall_status: initial_overall_status,
                last_step_report: None,
                execution_mode: Some(effective_mode),
                last_pipeline_status: last_pipeline_report.as_ref().map(|r| r.status),
                last_pipeline_report: last_pipeline_report.clone(),
                pipeline_boundary: boundary_for_mode(effective_mode),
                started_at_ms: owner_summary.acquired_at_ms,
                updated_at_ms: current_time_ms(),
            };
            self.write_driver_state(&key, &initial_state)?;
            Ok(initial_overall_status)
        })();

        let initial_overall_status = match initial_state_res {
            Ok(status) => status,
            Err(err) => {
                let err = self
                    .report_owner_failure(
                        &session,
                        admission.job.job_revision,
                        CoworkerDriverFailureStage::OwnerSetup,
                        err,
                    )
                    .await;
                drop(pipeline_driver);
                let _ = remove_socket_if_present(&canonical_socket_path);
                if let Some(handle) = host_session.owned_handle.take() {
                    let _ = handle.shutdown().await;
                }
                let _ = session.release();
                return Err(err);
            }
        };

        let mut quiescent_since: Option<Instant> = None;

        let mut last_tick_at = Instant::now();
        let mut immediate_step_requested = false;
        let mut stop_loop = false;
        let mut loop_err: Option<NativeError> = None;
        let mut failure_stage = CoworkerDriverFailureStage::PipelineTick;
        while !stop_loop {
            let in_flight_count = pipeline_driver
                .as_ref()
                .map_or(0, CoworkerPipelineDriver::in_flight_count);
            let initial_step_pending = steps_completed == 0;
            let active_work = initial_step_pending
                || owner_has_active_work(
                    effective_mode,
                    last_step_report.as_ref(),
                    last_pipeline_report.as_ref(),
                    in_flight_count,
                );
            let terminal_or_stopped = !initial_step_pending
                && owner_is_terminal_or_stopped(
                    effective_mode,
                    last_step_report.as_ref(),
                    last_pipeline_report.as_ref(),
                    in_flight_count,
                );
            let pipeline_await = !initial_step_pending
                && owner_is_pipeline_await(
                    effective_mode,
                    last_step_report.as_ref(),
                    last_pipeline_report.as_ref(),
                    in_flight_count,
                );

            if active_work {
                quiescent_since = None;
                // Explicit diagnostic max_steps never cancels or shuts down active work on an owned host
                // or drops in-flight pipeline stage futures.
                if let Some(limit) = max_steps {
                    if steps_completed >= limit
                        && host_session.owned_handle.is_none()
                        && in_flight_count == 0
                    {
                        break;
                    }
                }
            } else if !immediate_step_requested {
                let since = *quiescent_since.get_or_insert_with(Instant::now);
                let elapsed_ms = u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX);
                let max_steps_reached = max_steps.is_some_and(|limit| steps_completed >= limit);

                let should_exit_now = if terminal_or_stopped {
                    let linger = idle_linger_ms.unwrap_or(0);
                    linger == 0 || elapsed_ms >= linger
                } else if pipeline_await {
                    // Normal Phase 1 owner lifetime keeps RPC and physical ownership available at
                    // AwaitingChecksOrReview / AwaitingDependencyAcceptanceOrSeal / Held unless
                    // an explicit diagnostic max_steps or idle_linger_ms was supplied.
                    max_steps_reached || idle_linger_ms.is_some_and(|linger| elapsed_ms >= linger)
                } else {
                    max_steps_reached
                };

                if should_exit_now {
                    while let Ok(Some(Ok((stream, _)))) =
                        tokio::time::timeout(Duration::from_millis(5), async {
                            Some(listener.accept().await)
                        })
                        .await
                    {
                        let _ = Box::pin(self.handle_owner_rpc_connection(
                            stream,
                            &key,
                            &session,
                            host_session.mode,
                            effective_mode,
                            in_flight_stages_rx.as_ref(),
                            live_completion_effects_rx.as_ref(),
                            false,
                            &last_step_report,
                            &last_pipeline_report,
                            &mut draining_after_stop,
                            &mut immediate_step_requested,
                            &mut stop_loop,
                        ))
                        .await;
                        if immediate_step_requested {
                            break;
                        }
                    }
                    if !immediate_step_requested {
                        break;
                    }
                }
            }

            let target_wait_ms = if !active_work {
                if let Some(linger) = idle_linger_ms {
                    let since = quiescent_since.unwrap_or_else(Instant::now);
                    let elapsed_ms = u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX);
                    linger.saturating_sub(elapsed_ms).clamp(1, poll_interval_ms)
                } else {
                    poll_interval_ms
                }
            } else if initial_step_pending {
                poll_interval_ms.clamp(100, 500)
            } else if in_flight_count > 0 {
                poll_interval_ms.min(15)
            } else {
                poll_interval_ms
            };

            let elapsed_since_tick_ms =
                u64::try_from(last_tick_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            let remaining_wait_ms = if immediate_step_requested {
                0
            } else {
                target_wait_ms.saturating_sub(elapsed_since_tick_ms)
            };

            let mut run_periodic_tick = false;
            if immediate_step_requested {
                run_periodic_tick = true;
            } else {
                tokio::select! {
                    biased;
                    accept_res = listener.accept() => {
                        if let Ok((stream, _)) = accept_res {
                            if let Err(err) = Box::pin(self.handle_owner_rpc_connection(
                                stream,
                                &key,
                                &session,
                                host_session.mode,
                                effective_mode,
                                in_flight_stages_rx.as_ref(),
                                live_completion_effects_rx.as_ref(),
                                false,
                                &last_step_report,
                                &last_pipeline_report,
                                &mut draining_after_stop,
                                &mut immediate_step_requested,
                                &mut stop_loop,
                            ))
                            .await
                            {
                                failure_stage = CoworkerDriverFailureStage::OwnerRpc;
                                loop_err = Some(err);
                                break;
                            }
                        }
                        if immediate_step_requested
                            || steps_completed == 0
                            || u64::try_from(last_tick_at.elapsed().as_millis()).unwrap_or(u64::MAX) >= target_wait_ms
                        {
                            run_periodic_tick = true;
                        }
                    }
                    () = tokio::time::sleep(Duration::from_millis(remaining_wait_ms)) => {
                        run_periodic_tick = true;
                    }
                }
            }

            if run_periodic_tick && !stop_loop {
                last_tick_at = Instant::now();
                let now_ms = current_time_ms();
                let should_step_on_tick =
                    if steps_completed == 0 || active_work || immediate_step_requested {
                        true
                    } else {
                        match self.should_step_while_quiescent(&key, now_ms) {
                            Ok(should_step) => should_step,
                            Err(err) => {
                                failure_stage = CoworkerDriverFailureStage::QuiescentCheck;
                                loop_err = Some(err);
                                break;
                            }
                        }
                    };

                if should_step_on_tick {
                    immediate_step_requested = false;
                    let step_prev_pipeline_report = last_pipeline_report.clone();
                    let mut step_fut = Box::pin(self.execute_owner_step_or_quiescent_check(
                        &key,
                        &session,
                        effective_mode,
                        pipeline_service.as_ref(),
                        &mut pipeline_driver,
                        host_session.mode,
                        steps_completed,
                        step_prev_pipeline_report,
                        now_ms,
                        &canonical_socket_path,
                    ));
                    let step_outcome = loop {
                        tokio::select! {
                            biased;
                            step_res = &mut step_fut => {
                                break step_res;
                            }
                            accept_res = listener.accept() => {
                                if let Ok((stream, _)) = accept_res {
                                    if let Err(err) = Box::pin(self.handle_owner_rpc_connection(
                                        stream,
                                        &key,
                                        &session,
                                        host_session.mode,
                                        effective_mode,
                                        in_flight_stages_rx.as_ref(),
                                        live_completion_effects_rx.as_ref(),
                                        true,
                                        &last_step_report,
                                        &last_pipeline_report,
                                        &mut draining_after_stop,
                                        &mut immediate_step_requested,
                                        &mut stop_loop,
                                    ))
                                    .await
                                    {
                                        let _ = self
                                            .controller
                                            .policy_store()
                                            .mutate_job_runtime_for_operator(
                                                &key.binding_id,
                                                &key.job_id,
                                                |runtime_state| {
                                                    runtime_state.stop_requested = true;
                                                    if runtime_state.stop_or_block_reason.is_none() {
                                                        runtime_state.stop_or_block_reason = Some(
                                                            "owner RPC handler failed; inspect execution_status fatal_error".to_string(),
                                                        );
                                                    }
                                                    Ok(())
                                                },
                                            );
                                        draining_after_stop = true;
                                        if loop_err.is_none() {
                                            failure_stage = CoworkerDriverFailureStage::OwnerRpc;
                                            // The retained tick can finish before this RPC error
                                            // unwinds. Persist now so it cannot emit a normal
                                            // milestone notice for the same fatal owner path.
                                            let failure = CoworkerDriverFatalError::from_error(
                                                &err, failure_stage, admission.job.job_revision);
                                            let _ = persist_owner_failure(self.driver_dir(), session.guard(), failure);
                                            loop_err = Some(err);
                                        }
                                    }
                                }
                            }
                        }
                    };
                    drop(step_fut);
                    match step_outcome {
                        Ok((
                            next_steps_completed,
                            next_step_report,
                            next_pipeline_report,
                            next_draining_after_stop,
                        )) => {
                            steps_completed = next_steps_completed;
                            last_step_report = Some(next_step_report);
                            last_pipeline_report = next_pipeline_report;
                            draining_after_stop = next_draining_after_stop;
                        }
                        Err(err) => {
                            if loop_err.is_none() {
                                failure_stage = CoworkerDriverFailureStage::PipelineTick;
                                loop_err = Some(err);
                            }
                            break;
                        }
                    }
                    if loop_err.is_some() {
                        break;
                    }
                    let current_in_flight = pipeline_driver
                        .as_ref()
                        .map_or(0, CoworkerPipelineDriver::in_flight_count);
                    let still_active = owner_has_active_work(
                        effective_mode,
                        last_step_report.as_ref(),
                        last_pipeline_report.as_ref(),
                        current_in_flight,
                    );
                    if !still_active && quiescent_since.is_none() {
                        quiescent_since = Some(Instant::now());
                    } else if still_active {
                        quiescent_since = None;
                    }
                }
            }
        }

        if let Some(err) = loop_err {
            let err = self
                .report_owner_failure(&session, admission.job.job_revision, failure_stage, err)
                .await;
            if let Some(ref mut drv) = pipeline_driver {
                if drv.in_flight_count() > 0 {
                    let _ = self
                        .controller
                        .policy_store()
                        .mutate_job_runtime_for_operator(
                            &key.binding_id,
                            &key.job_id,
                            |runtime_state| {
                                runtime_state.stop_requested = true;
                                Ok(())
                            },
                        );
                    let _ = drv.drain_in_flight(current_time_ms().max(1)).await;
                }
            }
            drop(pipeline_driver);
            let _ = remove_socket_if_present(&canonical_socket_path);
            if let Some(handle) = host_session.owned_handle.take() {
                let _ = handle.shutdown().await;
            }
            let _ = session.release();
            return Err(err);
        }

        // Drop retained pipeline driver before releasing session guard.
        drop(pipeline_driver);

        // Build final status report while still holding the guard, then perform graceful teardown.
        let _ = remove_socket_if_present(&canonical_socket_path);
        drop(listener);

        let teardown: Result<(), NativeError> = async {
            let prev_state_before_shutdown = self.read_driver_state_optional(&key)?;
            let mut shutdown_manifest_updated_at_ms: Option<i64> = prev_state_before_shutdown
                .as_ref()
                .and_then(|s| s.owner_quiescent_host_shutdown_updated_at_ms);
            let mut shutdown_host_instance_id: Option<String> = prev_state_before_shutdown
                .as_ref()
                .and_then(|s| s.owner_quiescent_host_instance_id.clone());
            if let Some(handle) = host_session.owned_handle.take() {
                let instance_id = handle.instance_id().to_owned();
                handle.shutdown().await.map_err(map_host_error)?;
                let session_dir = self.sessions_dir.join(&admission.approval.approval_id);
                if let Some(post_manifest) =
                    read_host_manifest_optional(&session_dir, &admission.approval)
                        .map_err(map_host_error)?
                {
                    if post_manifest.phase == HostPhase::Stopped
                        && last_step_report.as_ref().is_some_and(|r| {
                            matches!(
                            r.status,
                            CoworkerControllerPhase1Status::AwaitingChecksOrReview
                                | CoworkerControllerPhase1Status::AwaitingDependencyAcceptanceOrSeal
                                | CoworkerControllerPhase1Status::AllTasksIntegrated
                                | CoworkerControllerPhase1Status::Stopped
                                | CoworkerControllerPhase1Status::BlockedOrFailed
                                | CoworkerControllerPhase1Status::Held
                        )
                        })
                    {
                        shutdown_manifest_updated_at_ms = Some(post_manifest.updated_at_ms);
                        shutdown_host_instance_id = post_manifest.instance_id.or(Some(instance_id));
                    }
                }
            }

            let finished_at_ms = current_time_ms();
            let owner_summary = session.guard().validate_before_effect(&key)?.clone();
            if let Some(ref mut pipe_rep) = last_pipeline_report {
                pipe_rep.draining_in_flight_after_stop = false;
                pipe_rep.in_flight_stages.clear();
            }
            let final_overall_status = last_step_report
                .as_ref()
                .map_or(initial_overall_status, |r| r.status);
            let final_state = CoworkerDriverStateRecord {
                schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
                binding_id: key.binding_id.clone(),
                thread_uuid: key.thread_uuid.clone(),
                job_id: key.job_id.clone(),
                owner_instance_id: owner_summary.instance_id.clone(),
                owner_challenge_digest: owner_summary.challenge_digest.clone(),
                owner_pid_hint: owner_summary.pid_hint,
                owner_uid: current_effective_uid(),
                lock_file_identity: owner_summary.lock_file_identity,
                socket_path: canonical_socket_path.display().to_string(),
                host_mode: host_session.mode,
                owner_active: false,
                graceful_exit_completed: true,
                owner_quiescent_host_shutdown_updated_at_ms: shutdown_manifest_updated_at_ms,
                owner_quiescent_host_instance_id: shutdown_host_instance_id,
                steps_completed,
                last_overall_status: final_overall_status,
                last_step_report: last_step_report.clone(),
                execution_mode: Some(effective_mode),
                last_pipeline_status: last_pipeline_report.as_ref().map(|r| r.status),
                last_pipeline_report: last_pipeline_report.clone(),
                pipeline_boundary: boundary_for_mode(effective_mode),
                started_at_ms,
                updated_at_ms: finished_at_ms,
            };
            self.write_driver_state(&key, &final_state)?;

            // Validate final status inputs before releasing the guard. A later concurrent failure
            // after release cannot safely send a notice from this former owner.
            self.build_readonly_status_report(
                &key,
                Some(host_session.mode),
                last_step_report.clone(),
            )?;
            Ok(())
        }
        .await;
        if let Err(error) = teardown {
            let error = self
                .report_owner_failure(
                    &session,
                    admission.job.job_revision,
                    CoworkerDriverFailureStage::OwnerTeardown,
                    error,
                )
                .await;
            if let Some(handle) = host_session.owned_handle.take() {
                let _ = handle.shutdown().await;
            }
            let _ = session.release();
            return Err(error);
        }

        session.release()?;
        self.build_readonly_status_report(&key, Some(host_session.mode), last_step_report)
    }

    /// Ensures a single verified physical owner for `(binding_id, thread_uuid, job_id)`.
    ///
    /// - If a live physical owner already holds the job lock, verifies it over the local Unix
    ///   socket RPC (same-EUID peer credentials + fresh CSPRNG nonces + live lock summary/instance)
    ///   and reuses it without spawning a second process or making a second model call.
    /// - Otherwise, validates authoritative admission under a short-lived owned session before
    ///   spawning a detached `threadlane-flow ... code-driver run-owner` process and waiting for
    ///   bounded readiness verification.
    ///
    /// # Errors
    /// Returns [`NativeError`] if admission validation fails, if a live lock holder fails RPC
    /// verification, or if the spawned owner fails.
    pub async fn ensure_owner(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        options: Option<CoworkerDriverRunOptions>,
    ) -> Result<CoworkerDriverEnsureReport, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        verify_no_full_job_external_resolution(
            self.controller.workflow_store(),
            binding_id,
            job_id,
        )?;

        // 1. Check if a live owner is already holding the physical lock.
        if let Some(status) = self
            .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Status)
            .await?
        {
            return Ok(CoworkerDriverEnsureReport {
                disposition: CoworkerDriverEnsureDisposition::ReusedLiveOwner,
                status,
            });
        }

        let requested_mode = options.and_then(|o| o.execution_mode);
        let resolved_spawn_mode: CoworkerDriverExecutionMode;

        // 2. Validate authoritative admission and host state under a short-lived physical guard
        // before spawning a detached owner process. If another `ensure` or `run-owner` process is
        // concurrently acquiring or initializing the lock, wait for its RPC readiness.
        {
            let precheck_deadline =
                Instant::now() + Duration::from_millis(DEFAULT_DRIVER_STARTUP_TIMEOUT_MS);
            let session = loop {
                if let Some(status) = self
                    .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Status)
                    .await?
                {
                    return Ok(CoworkerDriverEnsureReport {
                        disposition: CoworkerDriverEnsureDisposition::ReusedLiveOwner,
                        status,
                    });
                }
                let attempt_now_ms = current_time_ms();
                match self.controller.open_owned_session(
                    binding_id,
                    thread_uuid,
                    job_id,
                    attempt_now_ms,
                ) {
                    Ok(sess) => break sess,
                    Err(err) => {
                        let lock_held = matches!(
                            self.controller.ownership_store().inspect_job_lock(&key)?,
                            CoworkerJobLockState::HeldByActiveProcess { .. }
                        );
                        if lock_held && Instant::now() < precheck_deadline {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            continue;
                        }
                        return Err(err);
                    }
                }
            };
            let precheck_now_ms = current_time_ms();
            type OwnerPrecheckResult = Result<
                (
                    CoworkerDriverExecutionMode,
                    Option<(CoworkerDriverHostMode, Option<CoworkerControllerStepReport>)>,
                ),
                NativeError,
            >;
            let precheck_res: OwnerPrecheckResult = async {
                let admission = session.verify_admission()?;
                let prev_state = self.read_driver_state_optional(&key)?;
                let effective_mode = resolve_effective_execution_mode(
                    requested_mode,
                    prev_state.as_ref(),
                    &admission.job,
                    &admission.runtime_state,
                )?;
                if is_terminal_or_stopped_job(
                    &admission.job,
                    &admission.runtime_state,
                    &admission.approval.approval_id,
                    self.controller.workflow_store().plans_dir(),
                    effective_mode,
                ) {
                    if self
                        .is_verified_ready_host_active(&admission.approval)
                        .await?
                    {
                        let active_host = ActiveHostSession {
                            mode: CoworkerDriverHostMode::ReusedVerifiedReadyHost,
                            owned_handle: None,
                        };
                        let pipe_svc = if effective_mode
                            == CoworkerDriverExecutionMode::FullAutomaticPipeline
                        {
                            Some(CoworkerPipelineService::new(self.controller.clone())?)
                        } else {
                            None
                        };
                        let mut pipe_drv = if let Some(ref svc) = pipe_svc {
                            Some(svc.driver(
                                session.guard(),
                                key.binding_id.clone(),
                                key.thread_uuid.clone(),
                                key.job_id.clone(),
                            )?)
                        } else {
                            None
                        };
                        let mut last_pipe = prev_state
                            .as_ref()
                            .and_then(|s| s.last_pipeline_report.clone());
                        let sock_path = self.socket_file_path_for_key(&key)?;
                        let (_, step_report, pipe_report, _) =
                            Box::pin(self.execute_owner_step_or_quiescent_check(
                                &key,
                                &session,
                                effective_mode,
                                pipe_svc.as_ref(),
                                &mut pipe_drv,
                                active_host.mode,
                                0u32,
                                last_pipe,
                                current_time_ms(),
                                &sock_path,
                            ))
                            .await?;
                        let last_step = Some(step_report);
                        last_pipe = pipe_report;
                        drop(pipe_drv);
                        let has_active_work = owner_has_active_work(
                            effective_mode,
                            last_step.as_ref(),
                            last_pipe.as_ref(),
                            0,
                        );
                        if !has_active_work {
                            let owner_summary =
                                session.guard().validate_before_effect(&key)?.clone();
                            let state_record = CoworkerDriverStateRecord {
                                schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
                                binding_id: key.binding_id.clone(),
                                thread_uuid: key.thread_uuid.clone(),
                                job_id: key.job_id.clone(),
                                owner_instance_id: owner_summary.instance_id,
                                owner_challenge_digest: owner_summary.challenge_digest,
                                owner_pid_hint: owner_summary.pid_hint,
                                owner_uid: current_effective_uid(),
                                lock_file_identity: owner_summary.lock_file_identity,
                                socket_path: sock_path.display().to_string(),
                                host_mode: CoworkerDriverHostMode::ReusedVerifiedReadyHost,
                                owner_active: false,
                                graceful_exit_completed: true,
                                owner_quiescent_host_shutdown_updated_at_ms: prev_state
                                    .as_ref()
                                    .and_then(|s| s.owner_quiescent_host_shutdown_updated_at_ms),
                                owner_quiescent_host_instance_id: prev_state
                                    .and_then(|s| s.owner_quiescent_host_instance_id),
                                steps_completed: 1,
                                last_overall_status: last_step
                                    .as_ref()
                                    .map_or(CoworkerControllerPhase1Status::Stopped, |r| r.status),
                                last_step_report: last_step.clone(),
                                execution_mode: Some(effective_mode),
                                last_pipeline_status: last_pipe.as_ref().map(|r| r.status),
                                last_pipeline_report: last_pipe,
                                pipeline_boundary: boundary_for_mode(effective_mode),
                                started_at_ms: precheck_now_ms,
                                updated_at_ms: current_time_ms(),
                            };
                            self.write_driver_state(&key, &state_record)?;
                            return Ok((
                                effective_mode,
                                Some((CoworkerDriverHostMode::ReusedVerifiedReadyHost, last_step)),
                            ));
                        }
                    } else if self.can_skip_host_for_quiescent_job(
                        &key,
                        &admission,
                        effective_mode,
                    )? {
                        let active_host = ActiveHostSession {
                            mode: CoworkerDriverHostMode::QuiescentNoHostRequired,
                            owned_handle: None,
                        };
                        let pipe_svc = if effective_mode
                            == CoworkerDriverExecutionMode::FullAutomaticPipeline
                        {
                            Some(CoworkerPipelineService::new(self.controller.clone())?)
                        } else {
                            None
                        };
                        let mut pipe_drv = None;
                        let mut last_pipe = prev_state
                            .as_ref()
                            .and_then(|s| s.last_pipeline_report.clone());
                        let sock_path = self.socket_file_path_for_key(&key)?;
                        let (_, step_report, pipe_report, _) =
                            Box::pin(self.execute_owner_step_or_quiescent_check(
                                &key,
                                &session,
                                effective_mode,
                                pipe_svc.as_ref(),
                                &mut pipe_drv,
                                active_host.mode,
                                0u32,
                                last_pipe,
                                current_time_ms(),
                                &sock_path,
                            ))
                            .await?;
                        let last_step = Some(step_report);
                        last_pipe = pipe_report;
                        let owner_summary = session.guard().validate_before_effect(&key)?.clone();
                        let state_record = CoworkerDriverStateRecord {
                            schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
                            binding_id: key.binding_id.clone(),
                            thread_uuid: key.thread_uuid.clone(),
                            job_id: key.job_id.clone(),
                            owner_instance_id: owner_summary.instance_id,
                            owner_challenge_digest: owner_summary.challenge_digest,
                            owner_pid_hint: owner_summary.pid_hint,
                            owner_uid: current_effective_uid(),
                            lock_file_identity: owner_summary.lock_file_identity,
                            socket_path: sock_path.display().to_string(),
                            host_mode: CoworkerDriverHostMode::QuiescentNoHostRequired,
                            owner_active: false,
                            graceful_exit_completed: true,
                            owner_quiescent_host_shutdown_updated_at_ms: prev_state
                                .as_ref()
                                .and_then(|s| s.owner_quiescent_host_shutdown_updated_at_ms),
                            owner_quiescent_host_instance_id: prev_state
                                .and_then(|s| s.owner_quiescent_host_instance_id),
                            steps_completed: 1,
                            last_overall_status: last_step
                                .as_ref()
                                .map_or(CoworkerControllerPhase1Status::Stopped, |r| r.status),
                            last_step_report: last_step.clone(),
                            execution_mode: Some(effective_mode),
                            last_pipeline_status: last_pipe.as_ref().map(|r| r.status),
                            last_pipeline_report: last_pipe,
                            pipeline_boundary: boundary_for_mode(effective_mode),
                            started_at_ms: precheck_now_ms,
                            updated_at_ms: current_time_ms(),
                        };
                        self.write_driver_state(&key, &state_record)?;
                        return Ok((
                            effective_mode,
                            Some((CoworkerDriverHostMode::QuiescentNoHostRequired, last_step)),
                        ));
                    }
                }
                self.preflight_host_for_admission(&key, &admission, effective_mode)
                    .await?;
                Ok((effective_mode, None))
            }
            .await;

            match precheck_res {
                Ok((_mode, Some((quiescent_host_mode, quiescent_report)))) => {
                    session.release()?;
                    let quiescent_status = self.build_readonly_status_report(
                        &key,
                        Some(quiescent_host_mode),
                        quiescent_report,
                    )?;
                    return Ok(CoworkerDriverEnsureReport {
                        disposition: CoworkerDriverEnsureDisposition::AlreadyQuiescentVerified,
                        status: quiescent_status,
                    });
                }
                Ok((mode, None)) => {
                    resolved_spawn_mode = mode;
                    session.release()?;
                }
                Err(err) => {
                    let _ = session.release();
                    return Err(err);
                }
            }
        }

        // 3. Spawn a detached `threadlane-flow ... code-driver run-owner` process.
        let run_opts = options.unwrap_or_default();
        let flow_bin = self.resolve_flow_binary()?;
        let mut cmd = Command::new(&flow_bin);
        cmd.arg("--state-dir")
            .arg(&self.state_dir)
            .arg("--registry-dir")
            .arg(&self.registry_dir)
            .arg("--plans-dir")
            .arg(&self.plans_dir)
            .arg("--authority-dir")
            .arg(&self.authority_dir)
            .arg("--sessions-dir")
            .arg(&self.sessions_dir)
            .arg("--binding")
            .arg(binding_id)
            .arg("--thread")
            .arg(thread_uuid)
            .arg("code-driver")
            .arg("run-owner")
            .arg("--job-id")
            .arg(job_id)
            .arg("--poll-interval-ms")
            .arg(run_opts.poll_interval_ms.clamp(1, 60_000).to_string())
            .arg("--execution-mode")
            .arg(resolved_spawn_mode.as_str());

        if let Some(max_steps) = run_opts.max_steps.filter(|&n| n > 0) {
            cmd.arg("--max-steps").arg(max_steps.min(4_096).to_string());
        }
        if let Some(idle_linger_ms) = run_opts.idle_linger_ms {
            cmd.arg("--idle-linger-ms")
                .arg(idle_linger_ms.min(60_000).to_string());
        }

        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);

        let mut child = cmd
            .spawn()
            .map_err(|err| map_io_error("failed to spawn detached code-driver owner", err))?;
        let deadline = Instant::now() + Duration::from_millis(DEFAULT_DRIVER_STARTUP_TIMEOUT_MS);

        loop {
            if let Some(status) = self
                .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Status)
                .await?
            {
                return Ok(CoworkerDriverEnsureReport {
                    disposition: CoworkerDriverEnsureDisposition::SpawnedNewOwner,
                    status,
                });
            }

            if let Some(exit_status) = child
                .try_wait()
                .map_err(|err| map_io_error("failed to poll spawned code-driver owner", err))?
            {
                if exit_status.success() {
                    let status = self.status(binding_id, thread_uuid, job_id).await?;
                    return Ok(CoworkerDriverEnsureReport {
                        disposition: CoworkerDriverEnsureDisposition::SpawnedNewOwner,
                        status,
                    });
                }
                // If a concurrent `ensure` won the physical lock race, wait for the winning live owner's RPC.
                while Instant::now() < deadline {
                    if let Some(status) = self
                        .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Status)
                        .await?
                    {
                        return Ok(CoworkerDriverEnsureReport {
                            disposition: CoworkerDriverEnsureDisposition::ReusedLiveOwner,
                            status,
                        });
                    }
                    if !matches!(
                        self.controller.ownership_store().inspect_job_lock(&key)?,
                        CoworkerJobLockState::HeldByActiveProcess { .. }
                    ) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "spawned code-driver run-owner process exited with non-zero status: {exit_status}"
                    ),
                ));
            }

            if Instant::now() >= deadline {
                return Err(NativeError::new(
                    NativeErrorCode::TimedOut,
                    format!(
                        "timed out after {DEFAULT_DRIVER_STARTUP_TIMEOUT_MS}ms waiting for code-driver owner for job '{}'",
                        key.job_id
                    ),
                ));
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Returns the current verified status for `(binding_id, thread_uuid, job_id)`.
    ///
    /// If a live physical owner holds the lock, verifies it via the local Unix socket RPC
    /// handshake without calling models or advancing pipeline state. Otherwise, inspects the
    /// persisted workflow, controller policy, lockfile, and host session state read-only without
    /// mutating state or trusting stale crash metadata.
    ///
    /// # Errors
    /// Returns [`NativeError`] if scope or store validation fails.
    pub async fn status(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        if let Some(live_report) = self
            .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Status)
            .await?
        {
            return Ok(live_report);
        }
        self.build_readonly_status_report(&key, None, None)
    }

    /// Reconciles task receipts and advances state for `(binding_id, thread_uuid, job_id)`.
    ///
    /// - If a live physical owner is active, delegates reconciliation over the authenticated local
    ///   Unix socket RPC so the live owner steps under its held physical guard.
    /// - Otherwise, opens a short-lived [`OwnedCoworkerControllerSession`], reconciles any stale
    ///   lease, ensures host readiness (without restarting a stopped or uncertain host), steps the
    ///   controller/pipeline once, and releases the session cleanly.
    ///
    /// # Errors
    /// Returns [`NativeError`] if admission, host verification, or receipt reconciliation fails.
    pub async fn reconcile(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        self.reconcile_with_options(binding_id, thread_uuid, job_id, None)
            .await
    }

    /// Reconciles task receipts and advances state for `(binding_id, thread_uuid, job_id)` with
    /// optional [`CoworkerDriverRunOptions`] (such as an explicit [`CoworkerDriverExecutionMode`]).
    ///
    /// # Errors
    /// Returns [`NativeError`] if admission, host verification, or receipt reconciliation fails.
    pub async fn reconcile_with_options(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        options: Option<CoworkerDriverRunOptions>,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        if let Some(live_report) = self
            .try_verify_live_owner_via_rpc(&key, DriverRpcOp::Reconcile)
            .await?
        {
            return Ok(live_report);
        }
        let execution_mode = options.and_then(|o| o.execution_mode);
        // Match ensure's read-only refusal boundary before entering the owner lifecycle.
        // run_owner reacquires ownership and verifies admission/readiness again, so this
        // preflight grants no authority across the release/reacquisition boundary.
        let session = self.controller.open_owned_session(
            binding_id,
            thread_uuid,
            job_id,
            current_time_ms(),
        )?;
        let preflight: Result<(), NativeError> = async {
            let admission = session.verify_admission()?;
            let previous = self.read_driver_state_optional(&key)?;
            let mode = resolve_effective_execution_mode(
                execution_mode,
                previous.as_ref(),
                &admission.job,
                &admission.runtime_state,
            )?;
            self.preflight_host_for_admission(&key, &admission, mode)
                .await
        }
        .await;
        let release = session.release();
        preflight?;
        release?;
        self.run_owner(
            binding_id,
            thread_uuid,
            job_id,
            CoworkerDriverRunOptions {
                max_steps: Some(1),
                poll_interval_ms: DEFAULT_DRIVER_POLL_INTERVAL_MS,
                idle_linger_ms: Some(0),
                execution_mode,
            },
        )
        .await
    }

    /// Requests durable stop, narrow host task cancellation, and in-flight stage draining for
    /// `(binding_id, thread_uuid, job_id)`.
    ///
    /// Ordering invariants:
    /// 1. Validates scope and read-only job/envelope/approval linkage.
    /// 2. Persists `stop_requested = true` durably in [`ControllerPolicyStore`] BEFORE any local
    ///    socket RPC or [`ApprovedHostClient::cancel_task`] call (which also signals any active
    ///    derived `Coder` correction attempt via its process-group `stop_requested` watcher).
    /// 3. If a live physical owner is active, notifies it via authenticated Unix socket RPC so it
    ///    performs narrow host cancellation, prevents new dispatches, and either exits immediately
    ///    or gracefully drains any finite in-flight direct `Reviewer` / check / `Closure` call
    ///    under its held physical guard while fencing stage promotion.
    /// 4. If the lock is free, performs/verifies narrow [`ApprovedHostClient::cancel_task`] on any
    ///    remaining active original host tasks when a verified `Ready` host is available, reconciles
    ///    under a short-lived owned session, and releases the lease and FD lock.
    ///
    /// # Errors
    /// Returns [`NativeError`] if scope/admission validation or durable state persistence fails.
    pub async fn cancel(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
        reason: Option<String>,
    ) -> Result<CoworkerDriverCancelReport, NativeError> {
        let key = CoworkerJobOwnershipKey::new(binding_id, thread_uuid, job_id)?;
        let inspection = self.inspect_scoped_job_readonly(&key)?;
        let sanitized_reason = reason
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("operator_requested_cancel")
            .to_owned();

        // STEP 1: Persist durable `stop_requested = true` BEFORE any socket RPC or host cancel call.
        self.controller
            .policy_store()
            .mutate_job_runtime_for_operator(binding_id, job_id, |runtime_state| {
                if runtime_state.envelope.binding_id != binding_id
                    || runtime_state.envelope.thread_uuid != thread_uuid
                {
                    return Err(NativeError::new(
                        NativeErrorCode::SecurityViolation,
                        "controller_runtime_state envelope scope does not match requested binding/thread.",
                    ));
                }
                runtime_state.stop_requested = true;
                runtime_state.stop_or_block_reason = Some(sanitized_reason.clone());
                Ok(())
            })?;
        let durable_stop_recorded_before_host_cancel = true;

        let mut notified_live_owner = false;
        let mut narrow_host_cancellation_attempted = false;
        let mut cancelled_host_tasks = Vec::new();
        let mut owner_draining_in_flight = false;

        // STEP 2: If a live physical owner is holding the lock, notify it over authenticated RPC.
        if let Some(rpc_resp) = self
            .try_send_rpc_to_live_owner(
                &key,
                DriverRpcOp::Cancel {
                    reason: sanitized_reason.clone(),
                },
            )
            .await?
        {
            notified_live_owner = true;
            narrow_host_cancellation_attempted = rpc_resp.narrow_host_cancellation_attempted;
            cancelled_host_tasks = rpc_resp.cancelled_host_tasks;
            owner_draining_in_flight = rpc_resp
                .status
                .as_ref()
                .is_some_and(|s| s.stopping_draining_in_flight);

            // Wait bounded time for the notified live owner to finish graceful teardown and drop FD.
            // If the owner is actively draining a finite in-flight direct check/Reviewer/Closure call
            // that lacks a cancellation API, wait briefly in case it finishes immediately, then
            // return truthful draining status without blocking RPC callers.
            let wait_window_ms = if owner_draining_in_flight { 120 } else { 5_000 };
            let deadline = Instant::now() + Duration::from_millis(wait_window_ms);
            while Instant::now() < deadline {
                if !matches!(
                    self.controller.ownership_store().inspect_job_lock(&key)?,
                    CoworkerJobLockState::HeldByActiveProcess { .. }
                ) {
                    owner_draining_in_flight = false;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // STEP 3: If the lock is now free, acquire the physical ownership guard directly, verify
        // immutable job/envelope/approval identity under the held guard, release any stale logical
        // lease, and cancel any remaining active original host tasks without opening execution
        // admission or running a new controller step.
        if !owner_draining_in_flight
            && !matches!(
                self.controller.ownership_store().inspect_job_lock(&key)?,
                CoworkerJobLockState::HeldByActiveProcess { .. }
            )
        {
            let cancel_now_ms = current_time_ms();
            if let CoworkerOwnershipAcquireOutcome::Acquired(guard) = self
                .controller
                .ownership_store()
                .try_acquire_job(&key, cancel_now_ms)?
            {
                self.controller.verify_physical_ownership(
                    &guard,
                    binding_id,
                    thread_uuid,
                    job_id,
                )?;
                let owner_summary = guard.validate_before_effect(&key)?.clone();
                let verified_inspection = self.inspect_scoped_job_readonly(&key)?;
                self.controller
                    .reconcile_stale_logical_lease_under_physical_guard(
                        &guard,
                        binding_id,
                        thread_uuid,
                        job_id,
                    )?;
                if self
                    .is_verified_ready_host_active(&verified_inspection.approval)
                    .await?
                {
                    narrow_host_cancellation_attempted = true;
                    let newly_cancelled = self
                        .cancel_active_original_host_tasks(
                            binding_id,
                            thread_uuid,
                            &verified_inspection.job,
                            &verified_inspection.approval.approval_id,
                            &sanitized_reason,
                        )
                        .await?;
                    for item in newly_cancelled {
                        if !cancelled_host_tasks
                            .iter()
                            .any(|existing| existing.task_id == item.task_id)
                        {
                            cancelled_host_tasks.push(item);
                        }
                    }
                }

                let previous_state = self.read_driver_state_optional(&key)?;
                let effective_mode = resolve_effective_execution_mode(
                    None,
                    previous_state.as_ref(),
                    &verified_inspection.job,
                    &verified_inspection.runtime_state,
                )
                .unwrap_or(CoworkerDriverExecutionMode::FullAutomaticPipeline);
                let effective_host_mode = if narrow_host_cancellation_attempted {
                    CoworkerDriverHostMode::ReusedVerifiedReadyHost
                } else {
                    previous_state
                        .as_ref()
                        .map_or(CoworkerDriverHostMode::NotRunningOrInspectedReadonly, |s| {
                            s.host_mode
                        })
                };
                let effective_step_report = previous_state
                    .as_ref()
                    .and_then(|s| s.last_step_report.clone());
                let mut updated_pipe_report = previous_state
                    .as_ref()
                    .and_then(|s| s.last_pipeline_report.clone());
                if let Some(ref mut pipe_rep) = updated_pipe_report {
                    pipe_rep.status = CoworkerPipelineStatus::Stopped;
                    pipe_rep.draining_in_flight_after_stop = false;
                    pipe_rep.in_flight_stages.clear();
                }
                let updated_state = CoworkerDriverStateRecord {
                    schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
                    binding_id: key.binding_id.clone(),
                    thread_uuid: key.thread_uuid.clone(),
                    job_id: key.job_id.clone(),
                    owner_instance_id: owner_summary.instance_id.clone(),
                    owner_challenge_digest: owner_summary.challenge_digest.clone(),
                    owner_pid_hint: owner_summary.pid_hint,
                    owner_uid: current_effective_uid(),
                    lock_file_identity: owner_summary.lock_file_identity,
                    socket_path: self.socket_file_path_for_key(&key)?.display().to_string(),
                    host_mode: effective_host_mode,
                    owner_active: false,
                    graceful_exit_completed: true,
                    owner_quiescent_host_shutdown_updated_at_ms: previous_state
                        .as_ref()
                        .and_then(|state| state.owner_quiescent_host_shutdown_updated_at_ms),
                    owner_quiescent_host_instance_id: previous_state
                        .and_then(|state| state.owner_quiescent_host_instance_id),
                    steps_completed: 1,
                    last_overall_status: CoworkerControllerPhase1Status::Stopped,
                    last_step_report: effective_step_report,
                    execution_mode: Some(effective_mode),
                    last_pipeline_status: if effective_mode
                        == CoworkerDriverExecutionMode::FullAutomaticPipeline
                    {
                        Some(CoworkerPipelineStatus::Stopped)
                    } else {
                        None
                    },
                    last_pipeline_report: updated_pipe_report,
                    pipeline_boundary: boundary_for_mode(effective_mode),
                    started_at_ms: cancel_now_ms,
                    updated_at_ms: current_time_ms(),
                };
                self.write_driver_state(&key, &updated_state)?;
                drop(guard);
            }
        }

        let status = self.status(binding_id, thread_uuid, job_id).await?;
        let (cancel_boundary, cancel_note) = match status.execution_mode {
            CoworkerDriverExecutionMode::FullAutomaticPipeline => (
                CoworkerDerivedCancellationBoundary::OriginalAndDerivedSignalledDirectCheckReviewerClosureDrainedFenced,
                "Durable stop_requested persisted before narrow ApprovedHostClient::cancel_task \
                 calls on original host tasks (task_id -> run_id) and process-group stop signalling \
                 for active derived Coder correction runs; finite in-flight check, Reviewer, \
                 TaskIntegration, and Closure calls that lack cancellation APIs drain under the held \
                 ownership guard and fence stage promotion without claiming remote inference termination."
                    .to_owned(),
            ),
            CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => (
                CoworkerDerivedCancellationBoundary::OriginalHostTasksOnlyDerivedIntegratedLater,
                "Durable stop_requested persisted before narrow ApprovedHostClient::cancel_task \
                 calls on original host tasks (task_id -> run_id); no PID or process-name kills \
                 are used. Running derived-attempt cancellation will be integrated in a later \
                 milestone."
                    .to_owned(),
            ),
        };
        Ok(CoworkerDriverCancelReport {
            binding_id: binding_id.to_owned(),
            thread_uuid: thread_uuid.to_owned(),
            job_id: job_id.to_owned(),
            approval_id: inspection.approval.approval_id,
            reason: sanitized_reason,
            durable_stop_recorded_before_host_cancel,
            notified_live_owner,
            narrow_host_cancellation_attempted,
            cancelled_host_tasks,
            derived_attempt_cancellation_boundary: cancel_boundary,
            cancellation_boundary_note: cancel_note,
            status,
        })
    }

    fn should_step_while_quiescent(
        &self,
        key: &CoworkerJobOwnershipKey,
        now_ms: u64,
    ) -> Result<bool, NativeError> {
        let Some(runtime_state) = self
            .controller
            .policy_store()
            .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
        else {
            return Ok(true);
        };
        if runtime_state.stop_requested {
            return Ok(true);
        }
        let Some(ref lease) = runtime_state.lease else {
            return Ok(true);
        };
        if !lease.active || now_ms.saturating_add(LEASE_RENEWAL_MARGIN_MS) >= lease.expires_at_ms {
            return Ok(true);
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_owner_step_or_quiescent_check<'a>(
        &self,
        key: &CoworkerJobOwnershipKey,
        session: &'a OwnedCoworkerControllerSession,
        effective_mode: CoworkerDriverExecutionMode,
        pipeline_service: Option<&CoworkerPipelineService>,
        pipeline_driver: &mut Option<CoworkerPipelineDriver<'a>>,
        host_mode: CoworkerDriverHostMode,
        steps_completed: u32,
        mut last_pipeline_report: Option<CoworkerDriverPipelineReport>,
        now_ms: u64,
        canonical_socket_path: &Path,
    ) -> Result<
        (
            u32,
            CoworkerControllerStepReport,
            Option<CoworkerDriverPipelineReport>,
            bool,
        ),
        NativeError,
    > {
        let owner_summary = session.guard().validate_before_effect(key)?.clone();
        let verified_summary = self
            .controller
            .ownership_store()
            .verify_live_guard(session.guard(), key)?;
        if !constant_time_str_eq(&owner_summary.instance_id, &verified_summary.instance_id) {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "live ownership guard instance mismatch before controller step.",
            ));
        }

        let prev_state = self.read_driver_state_optional(key)?;
        let mut draining_after_stop = false;
        let mut step_report = if host_mode == CoworkerDriverHostMode::QuiescentNoHostRequired {
            let admission = session.verify_admission()?;
            let lease = admission
                .runtime_state
                .lease
                .as_ref()
                .filter(|l| l.lease_epoch > 0)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "quiescent job is missing a valid persisted controller lease record.",
                    )
                })?;
            let lease_epoch = lease.lease_epoch;
            let controller_instance_id = lease.instance_id.clone();

            if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
                let mut completion_summary = last_pipeline_report
                    .as_ref()
                    .and_then(|r| r.completion_summary.clone());
                if admission.job.phase == CoworkerJobPhase::Completed {
                    if let Some(svc) = pipeline_service {
                        let comp_req = CoworkerJobCompletionRequest {
                            binding_id: key.binding_id.clone(),
                            thread_uuid: key.thread_uuid.clone(),
                            job_id: key.job_id.clone(),
                            now_ms: now_ms.max(1),
                        };
                        let comp_outcome = Box::pin(
                            svc.completion_service()
                                .complete_job(session.guard(), &comp_req),
                        )
                        .await?;
                        completion_summary =
                            Some(CoworkerDriverCompletionSummary::from_outcome(&comp_outcome));
                    }
                }
                let post_inspection = self.inspect_scoped_job_readonly(key)?;
                let (_, mut synth_pipe) = self.synthesize_readonly_pipeline_report(
                    &post_inspection,
                    prev_state.as_ref(),
                    Vec::new(),
                    false,
                )?;
                if let Some(comp) = completion_summary {
                    synth_pipe.completion_summary = Some(comp);
                }
                last_pipeline_report = Some(synth_pipe);
            }

            let inspection = self.inspect_scoped_job_readonly(key)?;
            let (status, tasks) = self.synthesize_status_and_tasks(&inspection)?;
            let fenced_task_ids = tasks
                .iter()
                .filter(|t| t.fenced)
                .map(|t| t.task_id.clone())
                .collect();
            let unknown_effect_task_ids = tasks
                .iter()
                .filter(|t| {
                    t.stage == CoworkerTaskStage::EffectUnknown
                        || t.host_status == TaskExecutionStatus::Unknown
                })
                .map(|t| t.task_id.clone())
                .collect();
            CoworkerControllerStepReport {
                binding_id: key.binding_id.clone(),
                thread_uuid: key.thread_uuid.clone(),
                job_id: key.job_id.clone(),
                approval_id: inspection.approval.approval_id,
                job_revision: inspection.job.job_revision,
                lease_epoch,
                controller_instance_id,
                status,
                max_parallel: inspection.approval.max_parallel,
                active_running_count: 0,
                dispatched_task_ids: last_pipeline_report
                    .as_ref()
                    .map_or_else(Vec::new, |r| r.dispatched_task_ids.clone()),
                reconciled_ready_for_review_task_ids: last_pipeline_report
                    .as_ref()
                    .map_or_else(Vec::new, |r| r.reconciled_ready_for_review_task_ids.clone()),
                promoted_dependency_ready_task_ids: last_pipeline_report
                    .as_ref()
                    .map_or_else(Vec::new, |r| r.promoted_dependency_ready_task_ids.clone()),
                fenced_task_ids,
                unknown_effect_task_ids,
                tasks,
            }
        } else if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
            let drv = pipeline_driver.as_mut().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "FullAutomaticPipeline mode requires an initialized CoworkerPipelineDriver.",
                )
            })?;
            // Once every task is integrated, continuously poll the retained completion
            // future through subprocess exit, private artifact persistence and post-effect
            // fencing. Generic pipeline ticks remain bounded. The owner RPC select keeps
            // status and durable stop reachable throughout this frozen-budget drain.
            let completing = drv
                .in_flight_stages()
                .iter()
                .any(|stage| stage.kind == CoworkerPipelineInFlightStageKind::JobCompletion);
            let tick_report = if completing {
                let lease = session
                    .verify_admission()?
                    .runtime_state
                    .lease
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "missing completion lease",
                        )
                    })?;
                let ttl_ms = crate::coworker_workflow::DEFAULT_CONTROLLER_LEASE_TTL_MS;
                let mut drain = Box::pin(drv.drain_in_flight(now_ms.max(1)));
                loop {
                    tokio::select! {
                        biased;
                        report = &mut drain => break report?,
                        () = tokio::time::sleep(Duration::from_millis((ttl_ms / 4).max(1))) => {
                            // Renew only the same still-active lease under the actual held
                            // physical guard. Never reacquire an expired/released epoch while
                            // a post-effect completion is awaiting its frozen-budget result.
                            let owner = session.guard().validate_before_effect(key)?;
                            let renewal_ms = current_time_ms();
                            self.controller.policy_store().mutate_job_runtime(
                                &key.binding_id, &key.job_id, &lease.instance_id, lease.lease_epoch,
                                |runtime| {
                                    let current = runtime.lease.as_mut().ok_or_else(|| {
                                        NativeError::new(NativeErrorCode::ValidationFailed, "missing completion lease")
                                    })?;
                                    if !completion_lease_matches_live_owner(current, key, owner, renewal_ms) {
                                        return Err(NativeError::new(
                                            NativeErrorCode::ValidationFailed,
                                            "completion lease is stale; refusing epoch replacement",
                                        ));
                                    }
                                    current.acquired_at_ms = renewal_ms;
                                    current.expires_at_ms = renewal_ms.saturating_add(ttl_ms);
                                    Ok(())
                                },
                            )?;
                        }
                    }
                }
            } else {
                Box::pin(drv.tick(now_ms.max(1))).await?
            };
            if tick_report.status == CoworkerPipelineStatus::Stopped {
                draining_after_stop = !tick_report.in_flight_stages.is_empty();
            } else {
                draining_after_stop = false;
            }
            if let Some(ref mut existing) = last_pipeline_report {
                existing.merge_from_tick(&tick_report, draining_after_stop);
            } else {
                last_pipeline_report = Some(CoworkerDriverPipelineReport::from_tick(
                    &tick_report,
                    draining_after_stop,
                ));
            }
            let inspection = self.inspect_scoped_job_readonly(key)?;
            let (status, tasks) = self.synthesize_status_and_tasks(&inspection)?;
            let active_running_count = tasks
                .iter()
                .filter(|t| {
                    matches!(
                        t.host_status,
                        TaskExecutionStatus::Queued
                            | TaskExecutionStatus::Claimed
                            | TaskExecutionStatus::Running
                            | TaskExecutionStatus::CancelRequested
                    ) || (matches!(
                        t.stage,
                        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
                    ) && t.host_status != TaskExecutionStatus::ReadyForReview)
                })
                .count();
            let cumulative = last_pipeline_report.as_ref();
            CoworkerControllerStepReport {
                binding_id: key.binding_id.clone(),
                thread_uuid: key.thread_uuid.clone(),
                job_id: key.job_id.clone(),
                approval_id: tick_report.approval_id,
                job_revision: tick_report.job_revision,
                lease_epoch: tick_report.lease_epoch,
                controller_instance_id: tick_report.controller_instance_id,
                status,
                max_parallel: inspection.approval.max_parallel,
                active_running_count,
                dispatched_task_ids: cumulative
                    .map_or_else(Vec::new, |r| r.dispatched_task_ids.clone()),
                reconciled_ready_for_review_task_ids: cumulative
                    .map_or_else(Vec::new, |r| r.reconciled_ready_for_review_task_ids.clone()),
                promoted_dependency_ready_task_ids: cumulative
                    .map_or_else(Vec::new, |r| r.promoted_dependency_ready_task_ids.clone()),
                fenced_task_ids: tick_report.fenced_task_ids,
                unknown_effect_task_ids: tick_report.unknown_effect_task_ids,
                tasks,
            }
        } else {
            session.step(now_ms).await?
        };
        let next_steps_completed = steps_completed.saturating_add(1);
        let mut progress_snapshots = Vec::new();
        let mut failed_task_snapshots = Vec::new();
        let admission = session.verify_admission()?;
        if !admission.runtime_state.stop_requested
            && host_mode != CoworkerDriverHostMode::QuiescentNoHostRequired
        {
            let client = ApprovedHostClient::new(
                Arc::clone(&self.authority),
                &self.sessions_dir,
                &key.binding_id,
                &key.thread_uuid,
            )
            .map_err(map_host_error)?;
            for observation in &mut step_report.tasks {
                if matches!(
                    observation.host_status,
                    TaskExecutionStatus::Running | TaskExecutionStatus::CancelRequested
                ) {
                    let snapshot = client
                        .get_task(&admission.approval.approval_id, &observation.task_id)
                        .await;
                    session.guard().validate_before_effect(key)?;
                    if let Ok(snapshot) = snapshot {
                        // Independent of soft tool-progress attention. Only already durable,
                        // verified original-host hints are eligible for this fixed wakeup.
                        if snapshot
                            .worker
                            .as_ref()
                            .is_some_and(|worker| worker.progress.provider_health.is_some())
                        {
                            let directory = crate::coworker_notice::coworker_notice_dir(
                                self.controller.workflow_store().plans_dir(),
                            );
                            if crate::coworker_notice::CoworkerNoticeStore::inspect(
                                &directory,
                                key,
                                &admission.runtime_state.envelope.envelope_digest,
                            )
                            .is_ok_and(|metadata| metadata.configured)
                            {
                                if let Ok(store) =
                                    crate::coworker_notice::CoworkerNoticeStore::new(directory)
                                {
                                    let _ = store
                                        .notify_original_provider_health(
                                            session, &client, &snapshot,
                                        )
                                        .await;
                                }
                            }
                        }
                        if let Some(attention) = crate::coworker_notice::verified_progress_attention(
                            &admission, &snapshot,
                        ) {
                            observation.run_id = snapshot.run_id.clone();
                            observation.worker_id = snapshot.worker_id.clone();
                            observation.watchdog_attention = Some(attention);
                            progress_snapshots.push(snapshot);
                        }
                    }
                } else if matches!(
                    observation.stage,
                    CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked
                ) && matches!(
                    observation.host_status,
                    TaskExecutionStatus::Failed | TaskExecutionStatus::TimedOut
                ) {
                    let snapshot = client
                        .get_task(&admission.approval.approval_id, &observation.task_id)
                        .await;
                    session.guard().validate_before_effect(key)?;
                    if let Ok(snapshot) = snapshot {
                        if crate::coworker_notice::verified_failed_task_attention(
                            &admission, &snapshot,
                        )
                        .is_some()
                        {
                            failed_task_snapshots.push(snapshot);
                        }
                    }
                }
            }
        }

        let state_record = CoworkerDriverStateRecord {
            schema_version: COWORKER_DRIVER_STATE_SCHEMA_VERSION,
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            owner_instance_id: owner_summary.instance_id,
            owner_challenge_digest: owner_summary.challenge_digest,
            owner_pid_hint: owner_summary.pid_hint,
            owner_uid: current_effective_uid(),
            lock_file_identity: owner_summary.lock_file_identity,
            socket_path: canonical_socket_path.display().to_string(),
            host_mode,
            owner_active: true,
            graceful_exit_completed: false,
            owner_quiescent_host_shutdown_updated_at_ms: prev_state
                .as_ref()
                .and_then(|s| s.owner_quiescent_host_shutdown_updated_at_ms),
            owner_quiescent_host_instance_id: prev_state
                .and_then(|s| s.owner_quiescent_host_instance_id),
            steps_completed: next_steps_completed,
            last_overall_status: step_report.status,
            last_step_report: Some(step_report.clone()),
            execution_mode: Some(effective_mode),
            last_pipeline_status: last_pipeline_report.as_ref().map(|r| r.status),
            last_pipeline_report: last_pipeline_report.clone(),
            pipeline_boundary: boundary_for_mode(effective_mode),
            started_at_ms: owner_summary.acquired_at_ms,
            updated_at_ms: current_time_ms(),
        };
        self.write_driver_state(key, &state_record)?;
        let failed_during_this_owner = read_owner_failure_record(self.driver_dir(), key)?
            .is_some_and(|failure| failure.owner_instance_id == state_record.owner_instance_id);
        if !failed_during_this_owner && !progress_snapshots.is_empty() {
            if let Ok(store) = crate::coworker_notice::CoworkerNoticeStore::new(
                crate::coworker_notice::coworker_notice_dir(
                    self.controller.workflow_store().plans_dir(),
                ),
            ) {
                for snapshot in &progress_snapshots {
                    let _ = store.notify_progress_attention(session, snapshot).await;
                }
            }
        }
        let settled_notice_kind = if failed_during_this_owner {
            None
        } else if let Some(ref pipe_report) = last_pipeline_report {
            match pipe_report.status {
                CoworkerPipelineStatus::Completed => {
                    Some(crate::coworker_notice::NoticeKind::Complete)
                }
                CoworkerPipelineStatus::Stopped => {
                    Some(crate::coworker_notice::NoticeKind::Stopped)
                }
                CoworkerPipelineStatus::Held
                | CoworkerPipelineStatus::BlockedOnUnknownEffect
                | CoworkerPipelineStatus::BlockedOrFailed
                | CoworkerPipelineStatus::BlockedByFailedChecks
                | CoworkerPipelineStatus::AwaitingCorrectionExecution => {
                    Some(crate::coworker_notice::NoticeKind::Attention)
                }
                CoworkerPipelineStatus::Running => None,
            }
        } else {
            match step_report.status {
                CoworkerControllerPhase1Status::Stopped => {
                    Some(crate::coworker_notice::NoticeKind::Stopped)
                }
                CoworkerControllerPhase1Status::Held
                | CoworkerControllerPhase1Status::BlockedOnUnknownEffect
                | CoworkerControllerPhase1Status::BlockedOrFailed => {
                    Some(crate::coworker_notice::NoticeKind::Attention)
                }
                _ => None,
            }
        };
        if settled_notice_kind.is_none()
            && !failed_during_this_owner
            && !failed_task_snapshots.is_empty()
        {
            if let Ok(store) = crate::coworker_notice::CoworkerNoticeStore::new(
                crate::coworker_notice::coworker_notice_dir(
                    self.controller.workflow_store().plans_dir(),
                ),
            ) {
                for snapshot in &failed_task_snapshots {
                    let _ = store
                        .notify_worker_failure_attention(session, snapshot)
                        .await;
                }
            }
        }
        if let Some(notice_kind) = settled_notice_kind {
            if let Ok(store) = crate::coworker_notice::CoworkerNoticeStore::new(
                crate::coworker_notice::coworker_notice_dir(
                    self.controller.workflow_store().plans_dir(),
                ),
            ) {
                if let Some(report) = last_pipeline_report.as_ref().filter(|report| {
                    matches!(
                        report.status,
                        CoworkerPipelineStatus::BlockedByFailedChecks
                            | CoworkerPipelineStatus::AwaitingCorrectionExecution
                    )
                }) {
                    if pipeline_driver
                        .as_ref()
                        .is_some_and(|driver| driver.in_flight_count() == 0)
                    {
                        let _ = store.notify_settled_attention(session, report).await;
                    }
                } else {
                    let _ = store.notify(session, notice_kind).await;
                }
            }
        }
        Ok((
            next_steps_completed,
            step_report,
            last_pipeline_report,
            draining_after_stop,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_owner_rpc_connection(
        &self,
        stream: UnixStream,
        key: &CoworkerJobOwnershipKey,
        session: &OwnedCoworkerControllerSession,
        host_mode: CoworkerDriverHostMode,
        effective_mode: CoworkerDriverExecutionMode,
        in_flight_stages_rx: Option<
            &tokio::sync::watch::Receiver<Vec<CoworkerPipelineInFlightStageRef>>,
        >,
        live_completion_effects_rx: Option<&tokio::sync::watch::Receiver<LiveCompletionEffects>>,
        step_in_progress: bool,
        last_step_report: &Option<CoworkerControllerStepReport>,
        last_pipeline_report: &Option<CoworkerDriverPipelineReport>,
        draining_after_stop: &mut bool,
        immediate_step_requested: &mut bool,
        stop_loop: &mut bool,
    ) -> Result<(), NativeError> {
        let peer = stream
            .peer_cred()
            .map_err(|err| map_io_error("failed to inspect RPC socket peer credentials", err))?;
        let expected_uid = current_effective_uid();
        if peer.uid() != expected_uid {
            return Ok(());
        }

        let (reader_half, mut writer_half) = stream.into_split();
        let mut limited_reader = BufReader::new(reader_half.take(MAX_DRIVER_RPC_MESSAGE_BYTES));
        let mut line = String::new();
        let read_res = tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            limited_reader.read_line(&mut line),
        )
        .await;
        let Ok(Ok(bytes_read)) = read_res else {
            return Ok(());
        };
        if bytes_read == 0 {
            return Ok(());
        }
        let Ok(request) = serde_json::from_str::<DriverRpcRequest>(line.trim()) else {
            return Ok(());
        };

        // Server MUST validate actual held guard, current instance, challenge digest, and scope
        // BEFORE performing any action or replying.
        let live_summary = match session.guard().validate_before_effect(key) {
            Ok(summary) => summary.clone(),
            Err(_) => return Ok(()),
        };
        let verified_summary = match self
            .controller
            .ownership_store()
            .verify_live_guard(session.guard(), key)
        {
            Ok(summary) => summary,
            Err(_) => return Ok(()),
        };
        if !constant_time_str_eq(&live_summary.instance_id, &verified_summary.instance_id)
            || !constant_time_str_eq(
                &live_summary.challenge_digest,
                &verified_summary.challenge_digest,
            )
            || live_summary.lock_file_identity != verified_summary.lock_file_identity
        {
            return Ok(());
        }

        if request.schema_version != COWORKER_DRIVER_RPC_SCHEMA_VERSION
            || request.binding_id != key.binding_id
            || request.thread_uuid != key.thread_uuid
            || request.job_id != key.job_id
            || validate_prefixed_random_id(&request.client_nonce, "cdn", "client_nonce").is_err()
            || !constant_time_str_eq(&live_summary.instance_id, &request.expected_instance_id)
            || !constant_time_str_eq(
                &live_summary.challenge_digest,
                &request.expected_challenge_digest,
            )
        {
            return Ok(());
        }

        let server_nonce = generate_prefixed_random_id("dsn");
        let mut cancelled_host_tasks = Vec::new();
        let mut narrow_host_cancellation_attempted = false;
        let mut op_error: Option<String> = None;

        match request.op {
            DriverRpcOp::Status => {
                // Status RPC never invokes models or advances pipeline state; it returns cached
                // compact evidence combined with live read-only store/in-flight inspection.
            }
            DriverRpcOp::Reconcile => {
                if let Err(err) = session.verify_admission() {
                    op_error = Some(err.to_string());
                } else if !step_in_progress {
                    *immediate_step_requested = true;
                }
            }
            DriverRpcOp::Cancel { reason } => {
                let _ = self
                    .controller
                    .policy_store()
                    .mutate_job_runtime_for_operator(
                        &key.binding_id,
                        &key.job_id,
                        |runtime_state| {
                            runtime_state.stop_requested = true;
                            runtime_state.stop_or_block_reason = Some(reason.clone());
                            Ok(())
                        },
                    );
                let admission_valid = session.verify_admission().is_ok();
                *draining_after_stop = true;
                *immediate_step_requested = admission_valid;
                if self
                    .controller
                    .verify_physical_ownership(
                        session.guard(),
                        &key.binding_id,
                        &key.thread_uuid,
                        &key.job_id,
                    )
                    .is_ok()
                {
                    if let Ok(inspection) = self.inspect_scoped_job_readonly(key) {
                        if self
                            .is_verified_ready_host_active(&inspection.approval)
                            .await
                            .unwrap_or(false)
                        {
                            narrow_host_cancellation_attempted = true;
                            if let Ok(cancelled) = self
                                .cancel_active_original_host_tasks(
                                    &key.binding_id,
                                    &key.thread_uuid,
                                    &inspection.job,
                                    &inspection.approval.approval_id,
                                    &reason,
                                )
                                .await
                            {
                                cancelled_host_tasks = cancelled;
                            }
                        }
                    }
                }
                let live_in_flight_len = in_flight_stages_rx.map_or(0, |rx| rx.borrow().len());
                if !step_in_progress && live_in_flight_len == 0 && !*immediate_step_requested {
                    *draining_after_stop = false;
                    *stop_loop = true;
                }
            }
        }

        // Re-validate held guard immediately before serializing response.
        let post_summary = session.guard().validate_before_effect(key)?.clone();
        let live_in_flight_stages =
            in_flight_stages_rx.map_or_else(Vec::new, |rx| rx.borrow().clone());
        let status_report = self
            .build_live_owner_status_report(
                key,
                &post_summary,
                host_mode,
                effective_mode,
                last_step_report.clone(),
                last_pipeline_report.clone(),
                live_in_flight_stages,
                live_completion_effects_rx.map_or_else(BTreeMap::new, |rx| rx.borrow().clone()),
                *draining_after_stop,
            )
            .ok();

        let response = DriverRpcResponse {
            schema_version: COWORKER_DRIVER_RPC_SCHEMA_VERSION,
            echoed_client_nonce: request.client_nonce,
            server_nonce,
            owner_summary: post_summary,
            status: status_report,
            cancelled_host_tasks,
            narrow_host_cancellation_attempted,
            error: op_error,
        };
        let mut encoded = serde_json::to_vec(&response)
            .map_err(|err| map_json_error("driver_rpc_response", err))?;
        encoded.push(b'\n');
        let _ = tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            writer_half.write_all(&encoded),
        )
        .await;
        let _ = writer_half.flush().await;
        Ok(())
    }

    async fn try_verify_live_owner_via_rpc(
        &self,
        key: &CoworkerJobOwnershipKey,
        op: DriverRpcOp,
    ) -> Result<Option<CoworkerDriverStatusReport>, NativeError> {
        let Some(resp) = self.try_send_rpc_to_live_owner(key, op).await? else {
            return Ok(None);
        };
        if let Some(err_msg) = resp.error {
            return Err(NativeError::new(NativeErrorCode::ValidationFailed, err_msg));
        }
        resp.status.map(Some).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "live owner RPC response omitted status report.",
            )
        })
    }

    async fn try_send_rpc_to_live_owner(
        &self,
        key: &CoworkerJobOwnershipKey,
        op: DriverRpcOp,
    ) -> Result<Option<DriverRpcResponse>, NativeError> {
        let lock_state = self.controller.ownership_store().inspect_job_lock(key)?;
        let CoworkerJobLockState::HeldByActiveProcess {
            recorded_summary: Some(lock_summary),
            lock_file_identity,
            ..
        } = lock_state
        else {
            return Ok(None);
        };

        lock_summary.validate_for_key(key)?;
        if lock_summary.lock_file_identity != lock_file_identity {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "active job lock summary lock_file_identity does not match physical lockfile.",
            ));
        }

        let sock_file_name = socket_file_name_for_key(key);
        let canonical_socket_path = self.socket_file_path_for_key(key)?;
        if !validate_existing_private_socket(&canonical_socket_path)? {
            return Ok(None);
        }

        let is_cancel = matches!(op, DriverRpcOp::Cancel { .. });
        let client_nonce = generate_prefixed_random_id("cdn");
        let request = DriverRpcRequest {
            schema_version: COWORKER_DRIVER_RPC_SCHEMA_VERSION,
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            expected_instance_id: lock_summary.instance_id.clone(),
            expected_challenge_digest: lock_summary.challenge_digest.clone(),
            client_nonce: client_nonce.clone(),
            op,
        };

        let Some(stream) = connect_private_unix_socket(self.driver_dir(), &sock_file_name).await?
        else {
            return Ok(None);
        };

        let peer = stream
            .peer_cred()
            .map_err(|err| map_io_error("failed to inspect RPC socket peer credentials", err))?;
        let expected_uid = current_effective_uid();
        if peer.uid() != expected_uid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!(
                    "coworker driver socket peer uid {} does not match effective uid {expected_uid}.",
                    peer.uid()
                ),
            ));
        }

        let (reader_half, mut writer_half) = stream.into_split();
        let mut encoded_req = serde_json::to_vec(&request)
            .map_err(|err| map_json_error("driver_rpc_request", err))?;
        encoded_req.push(b'\n');

        let write_res = tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            writer_half.write_all(&encoded_req),
        )
        .await;
        if write_res.is_err() || writer_half.flush().await.is_err() {
            return Ok(None);
        }

        let mut limited_reader = BufReader::new(reader_half.take(MAX_DRIVER_RPC_MESSAGE_BYTES));
        let mut resp_line = String::new();
        let read_res = tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            limited_reader.read_line(&mut resp_line),
        )
        .await;
        let Ok(Ok(bytes_read)) = read_res else {
            return Ok(None);
        };
        if bytes_read == 0 {
            return Ok(None);
        }

        let response: DriverRpcResponse = serde_json::from_str(resp_line.trim())
            .map_err(|err| map_json_error("driver_rpc_response", err))?;
        if response.schema_version != COWORKER_DRIVER_RPC_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported coworker driver RPC response schema_version {}.",
                    response.schema_version
                ),
            ));
        }
        if !constant_time_str_eq(&response.echoed_client_nonce, &client_nonce) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker driver RPC response client_nonce mismatch.",
            ));
        }
        validate_prefixed_random_id(&response.server_nonce, "dsn", "server_nonce")?;
        response.owner_summary.validate_for_key(key)?;
        if !constant_time_str_eq(
            &response.owner_summary.instance_id,
            &lock_summary.instance_id,
        ) || !constant_time_str_eq(
            &response.owner_summary.challenge_digest,
            &lock_summary.challenge_digest,
        ) || response.owner_summary.lock_file_identity != lock_file_identity
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker driver RPC response owner_summary does not match live physical lock.",
            ));
        }

        if !is_cancel {
            // Re-inspect the physical lock to confirm the same instance still holds the lock.
            let post_lock = self.controller.ownership_store().inspect_job_lock(key)?;
            match post_lock {
                CoworkerJobLockState::HeldByActiveProcess {
                    recorded_summary: Some(post_summary),
                    lock_file_identity: post_identity,
                    ..
                } => {
                    if !constant_time_str_eq(&post_summary.instance_id, &lock_summary.instance_id)
                        || post_identity != lock_file_identity
                    {
                        return Err(NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "coworker job ownership lock changed during RPC verification.",
                        ));
                    }
                }
                _ => {
                    return Ok(None);
                }
            }
        }

        Ok(Some(response))
    }

    fn inspect_scoped_job_readonly(
        &self,
        key: &CoworkerJobOwnershipKey,
    ) -> Result<ReadonlyScopedJobInspection, NativeError> {
        let ledger = self
            .controller
            .workflow_store()
            .read_binding_ledger(&key.binding_id)?;
        if let Some(ref ledger_thread) = ledger.thread_uuid {
            if ledger_thread != &key.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "coworker workflow ledger thread_uuid does not match requested thread scope.",
                ));
            }
        }
        let job = ledger
            .jobs
            .into_iter()
            .find(|item| item.job_id == key.job_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "coworker job '{}' was not found in binding '{}'.",
                        key.job_id, key.binding_id
                    ),
                )
            })?;
        if job.binding_id != key.binding_id || job.thread_uuid != key.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "coworker job scope does not match requested binding/thread.",
            ));
        }

        let runtime_state = self
            .controller
            .policy_store()
            .get_job_runtime_readonly(&key.binding_id, &key.job_id)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::NotConfigured,
                    format!(
                        "controller runtime state missing for job '{}' in binding '{}'.",
                        key.job_id, key.binding_id
                    ),
                )
            })?;
        if runtime_state.envelope.binding_id != key.binding_id
            || runtime_state.envelope.thread_uuid != key.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "controller runtime state envelope scope does not match requested binding/thread.",
            ));
        }
        runtime_state.envelope.verify_matches_job(&job)?;

        let approval = self
            .authority
            .get_for_operator(&runtime_state.envelope.approval_id)
            .map_err(map_authority_error)?;
        if approval.binding_id != key.binding_id || approval.thread_uuid != key.thread_uuid {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "approval record scope does not match requested binding/thread.",
            ));
        }
        if !constant_time_str_eq(
            &approval.subject_digest,
            &runtime_state.envelope.approval_subject_digest,
        ) {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                "approval subject_digest does not match frozen controller execution envelope.",
            ));
        }

        Ok(ReadonlyScopedJobInspection {
            job,
            runtime_state,
            approval,
            verified_live_derived_attempts: Default::default(),
        })
    }

    fn can_skip_host_for_quiescent_job(
        &self,
        key: &CoworkerJobOwnershipKey,
        admission: &VerifiedControllerAdmission,
        execution_mode: CoworkerDriverExecutionMode,
    ) -> Result<bool, NativeError> {
        if !admission.runtime_state.unresolved_effects().is_empty()
            || admission.job.outbox.iter().any(|o| {
                o.status.blocks_replay_and_new_dispatch()
                    && !is_outbox_confirmed_but_fenced_by_stop(
                        &admission.job,
                        o,
                        &admission.runtime_state,
                    )
            })
        {
            return Ok(false);
        }

        let job_stopped = admission.runtime_state.stop_requested
            || admission.job.phase == CoworkerJobPhase::Cancelled;
        let job_settled_blocked_or_failed = !job_stopped
            && is_settled_blocked_or_failed_job(
                &admission.job,
                &admission.runtime_state,
                &admission.approval.approval_id,
                self.controller.workflow_store().plans_dir(),
            );

        if execution_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline
            && !job_stopped
            && !job_settled_blocked_or_failed
            && (admission.job.phase != CoworkerJobPhase::Completed
                || admission
                    .job
                    .tasks
                    .iter()
                    .any(|t| t.stage != CoworkerTaskStage::Integrated))
        {
            return Ok(false);
        }

        for task in &admission.job.tasks {
            if task.unresolved_outbox_id.is_some()
                && !is_review_reserved_task_confirmed_but_fenced_by_stop(
                    &admission.job,
                    task,
                    &admission.runtime_state,
                )
            {
                return Ok(false);
            }
            match task.stage {
                CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running => {
                    if !job_settled_blocked_or_failed
                        || !crate::coworker_workflow::has_reconciled_terminal_coder_attempt(
                            task,
                            &admission.job,
                            &admission.runtime_state,
                        )
                    {
                        return Ok(false);
                    }
                }
                CoworkerTaskStage::EffectUnknown => return Ok(false),
                CoworkerTaskStage::ReadyToDispatch => {
                    if !job_stopped && !job_settled_blocked_or_failed {
                        return Ok(false);
                    }
                }
                CoworkerTaskStage::ReadyForReview => {
                    if task.current_attempt_no == 0
                        || admission
                            .runtime_state
                            .get_attempt_artifacts(&task.task_id, task.current_attempt_no)
                            .is_none()
                    {
                        return Ok(false);
                    }
                }
                CoworkerTaskStage::ReviewReserved => {
                    if !job_stopped
                        || !is_review_reserved_task_confirmed_but_fenced_by_stop(
                            &admission.job,
                            task,
                            &admission.runtime_state,
                        )
                    {
                        return Ok(false);
                    }
                }
                CoworkerTaskStage::Held => {
                    if crate::coworker_workflow::is_task_coder_execution_active(
                        task,
                        &admission.job,
                        &admission.runtime_state,
                    ) || ((task.current_attempt_no > 0 || !task.attempts.is_empty())
                        && !crate::coworker_workflow::has_reconciled_terminal_coder_attempt(
                            task,
                            &admission.job,
                            &admission.runtime_state,
                        ))
                    {
                        return Ok(false);
                    }
                }
                CoworkerTaskStage::PendingDependencies
                | CoworkerTaskStage::UnderReview
                | CoworkerTaskStage::CorrectInScopeRequired
                | CoworkerTaskStage::Accepted
                | CoworkerTaskStage::IntegrationReserved
                | CoworkerTaskStage::Integrated
                | CoworkerTaskStage::Blocked
                | CoworkerTaskStage::Failed
                | CoworkerTaskStage::Cancelled => {}
            }
        }

        if !admission
            .runtime_state
            .lease
            .as_ref()
            .is_some_and(|l| l.lease_epoch > 0)
        {
            return Ok(false);
        }

        let Some(driver_state) = self.read_driver_state_optional(key)? else {
            return Ok(false);
        };
        if !driver_state.graceful_exit_completed {
            return Ok(false);
        }

        let session_dir = self.sessions_dir.join(&admission.approval.approval_id);
        let Some(manifest) = read_host_manifest_optional(&session_dir, &admission.approval)
            .map_err(map_host_error)?
        else {
            return Ok(false);
        };
        let Some(expected_updated_at_ms) = driver_state.owner_quiescent_host_shutdown_updated_at_ms
        else {
            if admission.job.phase == CoworkerJobPhase::Completed
                && manifest.phase == HostPhase::Stopped
                && !is_host_lock_held(&session_dir).unwrap_or(true)
            {
                return Ok(true);
            }
            return Ok(false);
        };
        let instance_matches = match (
            &driver_state.owner_quiescent_host_instance_id,
            &manifest.instance_id,
        ) {
            (Some(exp), Some(act)) => constant_time_str_eq(exp, act),
            (None, None) => true,
            _ => false,
        };
        Ok(manifest.phase == HostPhase::Stopped
            && manifest.updated_at_ms == expected_updated_at_ms
            && instance_matches)
    }

    async fn preflight_host_for_admission(
        &self,
        key: &CoworkerJobOwnershipKey,
        admission: &VerifiedControllerAdmission,
        execution_mode: CoworkerDriverExecutionMode,
    ) -> Result<(), NativeError> {
        if self.can_skip_host_for_quiescent_job(key, admission, execution_mode)? {
            return Ok(());
        }
        let session_dir = self.sessions_dir.join(&admission.approval.approval_id);
        match fs::symlink_metadata(&session_dir) {
            Ok(_) => {
                let Some(manifest) = read_host_manifest_optional(&session_dir, &admission.approval)
                    .map_err(map_host_error)?
                else {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "uncertain host session directory '{}' has no valid host-manifest.json; \
                             code driver refuses to auto-restart an uncertain host.",
                            session_dir.display()
                        ),
                    ));
                };
                if manifest.phase != HostPhase::Ready {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "existing host session for approval '{}' is in phase {:?}; \
                             code driver never automatically restarts a stopped or non-ready host.",
                            admission.approval.approval_id, manifest.phase
                        ),
                    ));
                }
                if !is_host_lock_held(&session_dir).map_err(map_host_error)? {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "existing host session for approval '{}' is marked Ready but host.lock is \
                             not held; code driver refuses to auto-restart an uncertain host.",
                            admission.approval.approval_id
                        ),
                    ));
                }
                let client = ApprovedHostClient::new(
                    Arc::clone(&self.authority),
                    &self.sessions_dir,
                    &key.binding_id,
                    &key.thread_uuid,
                )
                .map_err(map_host_error)?;
                let summary = client
                    .session_summary(&admission.approval.approval_id)
                    .map_err(map_host_error)?;
                if summary.phase != HostPhase::Ready
                    || !summary.gates.supervisor_authenticated_ready
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "host session for approval '{}' is not authenticated Ready.",
                            admission.approval.approval_id
                        ),
                    ));
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if read_host_launch_intent_optional(&self.sessions_dir, &admission.approval)
                    .map_err(map_host_error)?
                    .is_some()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "prior host launch intent exists for approval '{}' without an active \
                             Ready session; code driver refuses to auto-restart an uncertain host.",
                            admission.approval.approval_id
                        ),
                    ));
                }
            }
            Err(err) => {
                return Err(map_io_error(
                    "failed to inspect host session directory",
                    err,
                ))
            }
        }
        if let Err(err) = validate_approved_executable_task_instructions(&admission.approval.tasks)
        {
            if self
                .controller
                .job_may_dispatch_new_executable_tasks(admission)?
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    crate::fs_safety::sanitize_diagnostic_text(&err.to_string()),
                ));
            }
        }
        Ok(())
    }

    async fn ensure_host_for_admission(
        &self,
        key: &CoworkerJobOwnershipKey,
        admission: &VerifiedControllerAdmission,
        execution_mode: CoworkerDriverExecutionMode,
    ) -> Result<ActiveHostSession, NativeError> {
        let session_dir = self.sessions_dir.join(&admission.approval.approval_id);
        match fs::symlink_metadata(&session_dir) {
            Ok(_) => {
                let Some(manifest) = read_host_manifest_optional(&session_dir, &admission.approval)
                    .map_err(map_host_error)?
                else {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "uncertain host session directory '{}' has no valid host-manifest.json; \
                             code driver refuses to auto-restart an uncertain host.",
                            session_dir.display()
                        ),
                    ));
                };
                if manifest.phase == HostPhase::Ready
                    && is_host_lock_held(&session_dir).map_err(map_host_error)?
                {
                    let client = ApprovedHostClient::new(
                        Arc::clone(&self.authority),
                        &self.sessions_dir,
                        &key.binding_id,
                        &key.thread_uuid,
                    )
                    .map_err(map_host_error)?;
                    let summary = client
                        .session_summary(&admission.approval.approval_id)
                        .map_err(map_host_error)?;
                    if summary.phase == HostPhase::Ready
                        && summary.gates.supervisor_authenticated_ready
                    {
                        return Ok(ActiveHostSession {
                            mode: CoworkerDriverHostMode::ReusedVerifiedReadyHost,
                            owned_handle: None,
                        });
                    }
                }
                if self.can_skip_host_for_quiescent_job(key, admission, execution_mode)? {
                    return Ok(ActiveHostSession {
                        mode: CoworkerDriverHostMode::QuiescentNoHostRequired,
                        owned_handle: None,
                    });
                }
                Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!(
                        "existing host session for approval '{}' is in phase {:?} (lock_held={}); \
                         code driver never automatically restarts a stopped, failed, or uncertain host.",
                        admission.approval.approval_id,
                        manifest.phase,
                        is_host_lock_held(&session_dir).unwrap_or(false)
                    ),
                ))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if read_host_launch_intent_optional(&self.sessions_dir, &admission.approval)
                    .map_err(map_host_error)?
                    .is_some()
                {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        format!(
                            "prior host launch intent exists for approval '{}' without a Ready \
                             session; code driver refuses to auto-restart an uncertain host.",
                            admission.approval.approval_id
                        ),
                    ));
                }
                if self.can_skip_host_for_quiescent_job(key, admission, execution_mode)? {
                    return Ok(ActiveHostSession {
                        mode: CoworkerDriverHostMode::QuiescentNoHostRequired,
                        owned_handle: None,
                    });
                }
                let config = ApprovedHostConfig::new(
                    &self.registry_dir,
                    &self.plans_dir,
                    &self.authority_dir,
                    &self.sessions_dir,
                    &admission.approval.approval_id,
                );
                let handle = ApprovedHost::start_fresh_only(config)
                    .await
                    .map_err(map_host_error)?;
                Ok(ActiveHostSession {
                    mode: CoworkerDriverHostMode::StartedFreshHost,
                    owned_handle: Some(handle),
                })
            }
            Err(err) => Err(map_io_error(
                "failed to inspect host session directory",
                err,
            )),
        }
    }

    async fn is_verified_ready_host_active(
        &self,
        approval: &ApprovalRecord,
    ) -> Result<bool, NativeError> {
        let session_dir = self.sessions_dir.join(&approval.approval_id);
        match fs::symlink_metadata(&session_dir) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
            Err(err) => {
                return Err(map_io_error(
                    "failed to inspect host session directory",
                    err,
                ))
            }
        }
        let Some(manifest) =
            read_host_manifest_optional(&session_dir, approval).map_err(map_host_error)?
        else {
            return Ok(false);
        };
        if manifest.phase != HostPhase::Ready || !manifest.gates.supervisor_authenticated_ready {
            return Ok(false);
        }
        is_host_lock_held(&session_dir).map_err(map_host_error)
    }

    async fn cancel_active_original_host_tasks(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        job: &CoworkerJobRecord,
        approval_id: &str,
        reason: &str,
    ) -> Result<Vec<CancelledHostTaskSummary>, NativeError> {
        let host_client = ApprovedHostClient::new(
            Arc::clone(&self.authority),
            &self.sessions_dir,
            binding_id,
            thread_uuid,
        )
        .map_err(map_host_error)?;
        let mut cancelled = Vec::new();
        for task in &job.tasks {
            if let Ok(snapshot) = host_client.get_task(approval_id, &task.task_id).await {
                if let Some(ref worker) = snapshot.worker {
                    if matches!(
                        worker.status,
                        WorkerStatus::Queued
                            | WorkerStatus::Claimed
                            | WorkerStatus::Running
                            | WorkerStatus::CancelRequested
                    ) {
                        let initial_updated = if matches!(
                            worker.status,
                            WorkerStatus::Queued | WorkerStatus::Claimed | WorkerStatus::Running
                        ) {
                            host_client
                                .cancel_task(
                                    approval_id,
                                    &task.task_id,
                                    worker.revision,
                                    Some(reason.to_owned()),
                                )
                                .await
                                .ok()
                        } else {
                            Some(snapshot.clone())
                        };
                        if let Some(mut latest) = initial_updated {
                            let wait_deadline = Instant::now() + Duration::from_millis(3_000);
                            while Instant::now() < wait_deadline {
                                let still_in_flight =
                                    latest.worker.as_ref().is_some_and(|polled_worker| {
                                        matches!(
                                            polled_worker.status,
                                            WorkerStatus::Queued
                                                | WorkerStatus::Claimed
                                                | WorkerStatus::Running
                                                | WorkerStatus::CancelRequested
                                        )
                                    });
                                if !still_in_flight {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(15)).await;
                                if let Ok(polled) =
                                    host_client.get_task(approval_id, &task.task_id).await
                                {
                                    latest = polled;
                                } else {
                                    break;
                                }
                            }
                            let (updated_state, updated_rev) = latest
                                .worker
                                .as_ref()
                                .map_or((worker.status, worker.revision), |w| {
                                    (w.status, w.revision)
                                });
                            cancelled.push(CancelledHostTaskSummary {
                                task_id: task.task_id.clone(),
                                run_id: latest.run_id.unwrap_or_else(|| worker.run_id.clone()),
                                worker_state: updated_state,
                                worker_revision: updated_rev,
                            });
                        }
                    }
                }
            }
        }
        Ok(cancelled)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_live_owner_status_report(
        &self,
        key: &CoworkerJobOwnershipKey,
        verified_owner_summary: &CoworkerOwnershipSummary,
        host_mode: CoworkerDriverHostMode,
        effective_mode: CoworkerDriverExecutionMode,
        step_report: Option<CoworkerControllerStepReport>,
        mut pipeline_report: Option<CoworkerDriverPipelineReport>,
        live_in_flight_stages: Vec<CoworkerPipelineInFlightStageRef>,
        live_completion_effects: LiveCompletionEffects,
        draining_after_stop: bool,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        let step_report = step_report.or_else(|| {
            self.read_driver_state_optional(key)
                .ok()
                .flatten()
                .and_then(|state| state.last_step_report)
        });
        let mut inspection = self.inspect_scoped_job_readonly(key)?;
        if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
            filter_live_owned_completion_reservations(
                &mut inspection,
                key,
                verified_owner_summary,
                &live_in_flight_stages,
                &live_completion_effects,
            );
            filter_live_owned_in_flight_review_reservations(
                &mut inspection,
                &live_in_flight_stages,
            );
            filter_live_owned_in_flight_check_reservations(&mut inspection, &live_in_flight_stages);
        }
        let lease = inspection.runtime_state.lease.clone();
        let lease_active = lease.as_ref().is_some_and(|l| l.active);
        let lease_owner_instance_id = lease.as_ref().map(|l| l.instance_id.clone());

        let session_dir = self.sessions_dir.join(&inspection.approval.approval_id);
        let (host_phase, host_lock_held) =
            self.inspect_host_phase_and_lock(&session_dir, &inspection.approval)?;
        let (session_intents, session_receipts) = if host_phase == Some(HostPhase::Ready)
            && host_lock_held
        {
            self.read_live_ready_session_task_ledger_optional(&session_dir, &inspection.approval)?
        } else {
            (Vec::new(), Vec::new())
        };
        filter_live_owned_in_flight_coder_reservations(
            &mut inspection,
            key,
            verified_owner_summary,
            effective_mode,
            host_phase,
            host_lock_held,
            &session_intents,
            &session_receipts,
            self.controller.workflow_store().plans_dir(),
            step_report.as_ref(),
            &live_in_flight_stages,
        );

        let (overall_status, mut tasks) = match (&effective_mode, step_report.as_ref()) {
            (CoworkerDriverExecutionMode::Phase1InitialDispatchOnly, Some(report)) => {
                (report.status, report.tasks.clone())
            }
            _ => self.synthesize_status_and_tasks(&inspection)?,
        };
        if let Some(report) = step_report.as_ref() {
            for task in &mut tasks {
                task.watchdog_attention = report
                    .tasks
                    .iter()
                    .find(|prior| {
                        prior.task_id == task.task_id
                            && prior.run_id.is_some()
                            && prior.run_id == task.run_id
                            && prior.worker_id == task.worker_id
                    })
                    .and_then(|prior| prior.watchdog_attention.clone())
                    .filter(|attention| attention.validate_attention().is_ok());
            }
        }
        let blocked_on_unknown = has_unresolved_or_unknown_effect(
            &inspection.job,
            &inspection.runtime_state,
            overall_status,
        );

        let stopping_draining = draining_after_stop && !live_in_flight_stages.is_empty();
        if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
            let persisted_state = self.read_driver_state_optional(key).ok().flatten();
            let (_, mut synth_rep) = self.synthesize_readonly_pipeline_report(
                &inspection,
                persisted_state.as_ref(),
                live_in_flight_stages.clone(),
                stopping_draining,
            )?;
            if let Some(prev_rep) = pipeline_report {
                for id in prev_rep.dispatched_task_ids {
                    push_unique_id(&mut synth_rep.dispatched_task_ids, &id);
                }
                for id in prev_rep.reconciled_ready_for_review_task_ids {
                    push_unique_id(&mut synth_rep.reconciled_ready_for_review_task_ids, &id);
                }
                for id in prev_rep.verified_task_ids {
                    push_unique_id(&mut synth_rep.verified_task_ids, &id);
                }
                for id in prev_rep.reviewed_task_ids {
                    push_unique_id(&mut synth_rep.reviewed_task_ids, &id);
                }
                for id in prev_rep.integrated_task_ids {
                    push_unique_id(&mut synth_rep.integrated_task_ids, &id);
                }
                for id in prev_rep.prepared_dependency_task_ids {
                    push_unique_id(&mut synth_rep.prepared_dependency_task_ids, &id);
                }
                for id in prev_rep.promoted_dependency_ready_task_ids {
                    push_unique_id(&mut synth_rep.promoted_dependency_ready_task_ids, &id);
                }
                if synth_rep.completion_summary.is_none()
                    && !live_in_flight_stages
                        .iter()
                        .any(|stage| stage.kind == CoworkerPipelineInFlightStageKind::JobCompletion)
                    && inspection
                        .job
                        .combined_candidates
                        .last()
                        .is_none_or(|candidate| {
                            matches!(
                                candidate.status,
                                CombinedCandidateStatus::ClosureAccepted
                                    | CombinedCandidateStatus::Rejected
                            )
                        })
                {
                    synth_rep.completion_summary = prev_rep.completion_summary;
                }
            }
            pipeline_report = Some(synth_rep);
        }
        let pipeline_status = pipeline_report.as_ref().map(|r| r.status);

        let provider_health = crate::provider_health::controller_health_views(
            &inspection.runtime_state,
            Some(verified_owner_summary),
        );
        Ok(CoworkerDriverStatusReport {
            provider_health,
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            approval_id: inspection.approval.approval_id,
            owner_state: CoworkerDriverOwnerState::LiveVerifiedOwner,
            fatal_error: self.read_owner_failure_optional(key)?,
            verified_owner_summary: Some(verified_owner_summary.clone()),
            lease,
            lease_active,
            lease_owner_instance_id,
            host_phase,
            host_lock_held,
            host_mode,
            job_phase: inspection.job.phase,
            overall_status,
            execution_mode: effective_mode,
            pipeline_status,
            stopping_draining_in_flight: stopping_draining,
            in_flight_stages: live_in_flight_stages,
            pipeline_report,
            stop_requested: inspection.runtime_state.stop_requested,
            blocked_on_unknown_effect: blocked_on_unknown,
            stop_or_block_reason: inspection.runtime_state.stop_or_block_reason,
            total_task_attempts_reserved: inspection.runtime_state.total_task_attempts_reserved,
            max_total_task_attempts_per_job: inspection
                .runtime_state
                .envelope
                .max_total_task_attempts_per_job,
            tasks,
            step_report,
            pipeline_boundary: boundary_for_mode(effective_mode),
            boundary_note: boundary_note_for_mode(effective_mode),
        })
    }

    fn build_readonly_status_report(
        &self,
        key: &CoworkerJobOwnershipKey,
        override_host_mode: Option<CoworkerDriverHostMode>,
        override_step_report: Option<CoworkerControllerStepReport>,
    ) -> Result<CoworkerDriverStatusReport, NativeError> {
        let inspection = self.inspect_scoped_job_readonly(key)?;
        let lock_state = self.controller.ownership_store().inspect_job_lock(key)?;
        let persisted_driver_state = self.read_driver_state_optional(key)?;
        let lease = inspection.runtime_state.lease.clone();
        let lease_active = lease.as_ref().is_some_and(|l| l.active);
        let lease_owner_instance_id = lease.as_ref().map(|l| l.instance_id.clone());

        let owner_state = match &lock_state {
            CoworkerJobLockState::HeldByActiveProcess { .. } => {
                CoworkerDriverOwnerState::LockHeldRpcUnavailable
            }
            CoworkerJobLockState::UnlockedStaleFile {
                recorded_summary,
                lock_file_identity,
            } => {
                if is_gracefully_released_unlocked_lock(
                    key,
                    recorded_summary.as_ref(),
                    *lock_file_identity,
                    persisted_driver_state.as_ref(),
                    lease_active,
                ) {
                    CoworkerDriverOwnerState::NoActiveOwner
                } else {
                    CoworkerDriverOwnerState::StaleCrashArtifactDetected
                }
            }
            CoworkerJobLockState::Absent => {
                let stale_driver = persisted_driver_state
                    .as_ref()
                    .is_some_and(|s| s.owner_active || !s.graceful_exit_completed);
                if stale_driver || lease_active {
                    CoworkerDriverOwnerState::StaleCrashArtifactDetected
                } else {
                    CoworkerDriverOwnerState::NoActiveOwner
                }
            }
        };

        let session_dir = self.sessions_dir.join(&inspection.approval.approval_id);
        let (host_phase, host_lock_held) =
            self.inspect_host_phase_and_lock(&session_dir, &inspection.approval)?;

        let effective_mode = resolve_effective_execution_mode(
            None,
            persisted_driver_state.as_ref(),
            &inspection.job,
            &inspection.runtime_state,
        )
        .unwrap_or_else(|_| {
            if envelope_authorizes_full_pipeline(&inspection.job, &inspection.runtime_state) {
                CoworkerDriverExecutionMode::FullAutomaticPipeline
            } else {
                CoworkerDriverExecutionMode::Phase1InitialDispatchOnly
            }
        });

        let step_report = override_step_report.or_else(|| {
            if owner_state == CoworkerDriverOwnerState::NoActiveOwner {
                persisted_driver_state
                    .as_ref()
                    .and_then(|s| s.last_step_report.clone())
            } else {
                None
            }
        });

        let (overall_status, mut tasks) = self.synthesize_status_and_tasks(&inspection)?;
        if let Some(report) = step_report.as_ref() {
            for task in &mut tasks {
                task.watchdog_attention = report
                    .tasks
                    .iter()
                    .find(|prior| {
                        prior.task_id == task.task_id
                            && prior.run_id.is_some()
                            && prior.run_id == task.run_id
                            && prior.worker_id == task.worker_id
                    })
                    .and_then(|prior| prior.watchdog_attention.clone())
                    .filter(|attention| attention.validate_attention().is_ok());
            }
        }
        let blocked_on_unknown = has_unresolved_or_unknown_effect(
            &inspection.job,
            &inspection.runtime_state,
            overall_status,
        );

        let (pipeline_status, pipeline_report) =
            if effective_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline {
                let (pipe_status, pipe_report) = self.synthesize_readonly_pipeline_report(
                    &inspection,
                    persisted_driver_state.as_ref(),
                    Vec::new(),
                    false,
                )?;
                (Some(pipe_status), Some(pipe_report))
            } else {
                (None, None)
            };

        let host_mode = override_host_mode
            .or_else(|| {
                if owner_state == CoworkerDriverOwnerState::NoActiveOwner {
                    persisted_driver_state.map(|s| s.host_mode)
                } else {
                    None
                }
            })
            .unwrap_or(CoworkerDriverHostMode::NotRunningOrInspectedReadonly);

        let provider_health =
            crate::provider_health::controller_health_views(&inspection.runtime_state, None);
        Ok(CoworkerDriverStatusReport {
            provider_health,
            binding_id: key.binding_id.clone(),
            thread_uuid: key.thread_uuid.clone(),
            job_id: key.job_id.clone(),
            approval_id: inspection.approval.approval_id,
            owner_state,
            fatal_error: self.read_owner_failure_optional(key)?,
            verified_owner_summary: None,
            lease,
            lease_active,
            lease_owner_instance_id,
            host_phase,
            host_lock_held,
            host_mode,
            job_phase: inspection.job.phase,
            overall_status,
            execution_mode: effective_mode,
            pipeline_status,
            stopping_draining_in_flight: false,
            in_flight_stages: Vec::new(),
            pipeline_report,
            stop_requested: inspection.runtime_state.stop_requested,
            blocked_on_unknown_effect: blocked_on_unknown,
            stop_or_block_reason: inspection.runtime_state.stop_or_block_reason,
            total_task_attempts_reserved: inspection.runtime_state.total_task_attempts_reserved,
            max_total_task_attempts_per_job: inspection
                .runtime_state
                .envelope
                .max_total_task_attempts_per_job,
            tasks,
            step_report,
            pipeline_boundary: boundary_for_mode(effective_mode),
            boundary_note: boundary_note_for_mode(effective_mode),
        })
    }

    fn synthesize_readonly_pipeline_report(
        &self,
        inspection: &ReadonlyScopedJobInspection,
        persisted_driver_state: Option<&CoworkerDriverStateRecord>,
        in_flight_stages: Vec<CoworkerPipelineInFlightStageRef>,
        draining_in_flight_after_stop: bool,
    ) -> Result<(CoworkerPipelineStatus, CoworkerDriverPipelineReport), NativeError> {
        let (_, phase1_obs) = self.synthesize_status_and_tasks(inspection)?;
        let lease_epoch = inspection
            .runtime_state
            .lease
            .as_ref()
            .map_or(0, |l| l.lease_epoch);
        let controller_instance_id = inspection
            .runtime_state
            .lease
            .as_ref()
            .map(|l| l.instance_id.clone())
            .or_else(|| persisted_driver_state.map(|s| s.owner_instance_id.clone()))
            .unwrap_or_default();

        let mut base = persisted_driver_state
            .and_then(|s| s.last_pipeline_report.clone())
            .unwrap_or_else(|| CoworkerDriverPipelineReport {
                binding_id: inspection.job.binding_id.clone(),
                thread_uuid: inspection.job.thread_uuid.clone(),
                job_id: inspection.job.job_id.clone(),
                approval_id: inspection.approval.approval_id.clone(),
                job_phase: inspection.job.phase,
                job_revision: inspection.job.job_revision,
                lease_epoch,
                controller_instance_id: controller_instance_id.clone(),
                status: CoworkerPipelineStatus::Running,
                draining_in_flight_after_stop: false,
                in_flight_stages: Vec::new(),
                dispatched_task_ids: Vec::new(),
                reconciled_ready_for_review_task_ids: Vec::new(),
                verified_task_ids: Vec::new(),
                reviewed_task_ids: Vec::new(),
                integrated_task_ids: Vec::new(),
                prepared_dependency_task_ids: Vec::new(),
                promoted_dependency_ready_task_ids: Vec::new(),
                awaiting_correction_task_ids: Vec::new(),
                failed_check_task_ids: Vec::new(),
                fenced_task_ids: Vec::new(),
                unknown_effect_task_ids: Vec::new(),
                blocked_or_failed_task_ids: Vec::new(),
                tasks: Vec::new(),
                completion_summary: None,
            });

        base.job_phase = inspection.job.phase;
        base.job_revision = inspection.job.job_revision;
        base.lease_epoch = lease_epoch;
        if !controller_instance_id.is_empty() {
            base.controller_instance_id = controller_instance_id;
        }
        base.in_flight_stages = in_flight_stages.clone();
        base.draining_in_flight_after_stop = draining_in_flight_after_stop;

        let mut awaiting_correction_task_ids = Vec::new();
        let mut fenced_task_ids = Vec::new();
        let mut unknown_effect_task_ids = Vec::new();
        let mut blocked_or_failed_task_ids = Vec::new();
        let mut snapshots = Vec::with_capacity(inspection.job.tasks.len());

        for task in &inspection.job.tasks {
            let obs = phase1_obs.iter().find(|o| o.task_id == task.task_id);
            let host_status = obs.map_or(TaskExecutionStatus::NotStarted, |o| o.host_status);
            let fenced = obs.is_some_and(|o| o.fenced)
                || inspection.runtime_state.stop_requested
                || inspection.job.phase == CoworkerJobPhase::Cancelled;
            if fenced {
                push_unique_id(&mut fenced_task_ids, &task.task_id);
            }
            if task.current_attempt_no >= 1
                && !matches!(
                    task.stage,
                    CoworkerTaskStage::PendingDependencies | CoworkerTaskStage::ReadyToDispatch
                )
            {
                push_unique_id(&mut base.dispatched_task_ids, &task.task_id);
            }
            let latest_candidate = task.candidates.last();
            let review_subject_promoted = task.current_attempt_no >= 1
                && inspection
                    .runtime_state
                    .get_attempt_artifacts(&task.task_id, task.current_attempt_no)
                    .is_some_and(|a| a.review_subject.is_some());
            if review_subject_promoted
                || matches!(
                    task.stage,
                    CoworkerTaskStage::ReviewReserved
                        | CoworkerTaskStage::UnderReview
                        | CoworkerTaskStage::CorrectInScopeRequired
                        | CoworkerTaskStage::Accepted
                        | CoworkerTaskStage::IntegrationReserved
                        | CoworkerTaskStage::Integrated
                )
            {
                push_unique_id(
                    &mut base.reconciled_ready_for_review_task_ids,
                    &task.task_id,
                );
                push_unique_id(&mut base.verified_task_ids, &task.task_id);
            }
            let review_verdict_outcome = latest_candidate
                .and_then(|c| c.review_verdict.as_ref())
                .map(|v| v.outcome);
            let review_receipt_id = latest_candidate.and_then(|c| c.review_receipt_id.clone());
            let integration_receipt_id =
                latest_candidate.and_then(|c| c.integration_receipt_id.clone());
            if review_verdict_outcome.is_some()
                || matches!(
                    task.stage,
                    CoworkerTaskStage::CorrectInScopeRequired
                        | CoworkerTaskStage::Accepted
                        | CoworkerTaskStage::IntegrationReserved
                        | CoworkerTaskStage::Integrated
                )
            {
                push_unique_id(&mut base.reviewed_task_ids, &task.task_id);
            }
            if task.stage == CoworkerTaskStage::Integrated {
                push_unique_id(&mut base.integrated_task_ids, &task.task_id);
            }
            if task.stage == CoworkerTaskStage::CorrectInScopeRequired {
                push_unique_id(&mut awaiting_correction_task_ids, &task.task_id);
            }
            if host_status == TaskExecutionStatus::Unknown
                || task.stage == CoworkerTaskStage::EffectUnknown
                || (task.unresolved_outbox_id.is_some()
                    && !is_review_reserved_task_confirmed_but_fenced_by_stop(
                        &inspection.job,
                        task,
                        &inspection.runtime_state,
                    )
                    && !is_live_owned_in_flight_review_task(
                        &inspection.job,
                        task,
                        &inspection.runtime_state,
                        &in_flight_stages,
                    ))
            {
                push_unique_id(&mut unknown_effect_task_ids, &task.task_id);
            }
            if matches!(
                task.stage,
                CoworkerTaskStage::Blocked
                    | CoworkerTaskStage::Failed
                    | CoworkerTaskStage::Cancelled
            ) {
                push_unique_id(&mut blocked_or_failed_task_ids, &task.task_id);
            }
            let in_flight_stage = in_flight_stages
                .iter()
                .find(|s| s.task_id.as_deref() == Some(task.task_id.as_str()))
                .map(|s| s.kind);

            snapshots.push(CoworkerPipelineTaskSnapshot {
                task_id: task.task_id.clone(),
                attempt_no: task.current_attempt_no,
                stage: task.stage,
                host_status,
                fenced,
                in_flight_stage,
                candidate_id: latest_candidate.map(|c| c.candidate_id.clone()),
                review_subject_promoted,
                review_verdict_outcome,
                review_receipt_id,
                integration_receipt_id,
                awaiting_correction_execution: task.stage
                    == CoworkerTaskStage::CorrectInScopeRequired,
                failed_check_ids: Vec::new(),
                blocking_diagnostic: None,
            });
        }

        base.awaiting_correction_task_ids = awaiting_correction_task_ids;
        base.fenced_task_ids = fenced_task_ids;
        base.unknown_effect_task_ids = unknown_effect_task_ids;
        base.blocked_or_failed_task_ids = blocked_or_failed_task_ids;
        base.tasks = snapshots;

        let has_unknown = inspection
            .runtime_state
            .unresolved_effects()
            .into_iter()
            .any(|eff| {
                !is_live_owned_in_flight_review_effect(
                    &inspection.job,
                    eff,
                    &inspection.runtime_state,
                    &in_flight_stages,
                )
            })
            || inspection.job.outbox.iter().any(|o| {
                o.status.blocks_replay_and_new_dispatch()
                    && !is_outbox_confirmed_but_fenced_by_stop(
                        &inspection.job,
                        o,
                        &inspection.runtime_state,
                    )
                    && !is_live_owned_in_flight_review_outbox(
                        &inspection.job,
                        o,
                        &inspection.runtime_state,
                        &in_flight_stages,
                    )
            })
            || !base.unknown_effect_task_ids.is_empty();

        let accepted_candidate = inspection
            .job
            .combined_candidates
            .iter()
            .find(|c| c.status == CombinedCandidateStatus::ClosureAccepted);

        let completion_in_flight = in_flight_stages
            .iter()
            .any(|stage| stage.kind == CoworkerPipelineInFlightStageKind::JobCompletion);
        let terminal_candidate = accepted_candidate
            .filter(|_| inspection.job.phase == CoworkerJobPhase::Completed)
            .or_else(|| {
                inspection
                    .job
                    .combined_candidates
                    .last()
                    .filter(|candidate| candidate.status == CombinedCandidateStatus::Rejected)
            });
        if completion_in_flight
            || inspection
                .job
                .combined_candidates
                .last()
                .is_some_and(|candidate| {
                    matches!(
                        candidate.status,
                        CombinedCandidateStatus::ChecksVerified
                            | CombinedCandidateStatus::ClosureReserved
                    )
                })
        {
            base.completion_summary = None;
        } else if let Some(cand) = terminal_candidate {
            if base.completion_summary.is_none() {
                base.completion_summary = Some(CoworkerDriverCompletionSummary {
                    status: if inspection.job.phase == CoworkerJobPhase::Completed
                        && cand.status == CombinedCandidateStatus::ClosureAccepted
                    {
                        CoworkerJobCompletionStatus::Completed
                    } else {
                        CoworkerJobCompletionStatus::ClosureNotAccepted
                    },
                    integration_worktree_id: String::new(),
                    integration_checkout_path: PathBuf::new(),
                    integration_branch_name: String::new(),
                    approved_base_commit: cand.project_base.base_commit.clone(),
                    final_integrated_target_commit: cand
                        .final_commit_sha
                        .clone()
                        .unwrap_or_default(),
                    final_integrated_tree_sha: cand.final_tree_sha.clone(),
                    reused_existing_receipts: true,
                    check_evidence: cand.check_evidence.clone(),
                    failed_check_id: None,
                    combined_candidate_id: Some(cand.combined_candidate_id.clone()),
                    combined_candidate_status: Some(cand.status),
                    final_candidate_subject_digest: None,
                    closure_reservation_id: cand.closure_reservation_id.clone(),
                    closure_attempt_id: None,
                    // Read-only projection observes accepted/rejected state; it did not
                    // execute a cache lookup. Only an actual completion outcome proves reuse.
                    reused_cached_closure_verdict: false,
                    closure_verdict_kind: if cand.status == CombinedCandidateStatus::ClosureAccepted
                    {
                        Some(ClosureVerdictKind::Accepted)
                    } else {
                        None
                    },
                    closure_verdict: None,
                });
            }
        }

        let has_active_coder = phase1_obs.iter().any(|obs| {
            matches!(
                obs.host_status,
                TaskExecutionStatus::Queued
                    | TaskExecutionStatus::Claimed
                    | TaskExecutionStatus::Running
                    | TaskExecutionStatus::CancelRequested
            ) || (matches!(
                obs.stage,
                CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
            ) && obs.host_status != TaskExecutionStatus::ReadyForReview)
        });

        // AllTasksIntegrated is NOT Completed until real subject-bound Closure is accepted and
        // job.phase == CoworkerJobPhase::Completed.
        let status = if inspection.runtime_state.stop_requested
            || inspection.job.phase == CoworkerJobPhase::Cancelled
        {
            CoworkerPipelineStatus::Stopped
        } else if has_unknown {
            CoworkerPipelineStatus::BlockedOnUnknownEffect
        } else if inspection.job.phase == CoworkerJobPhase::Completed
            && accepted_candidate.is_some()
        {
            CoworkerPipelineStatus::Completed
        } else if has_active_coder || !in_flight_stages.is_empty() {
            CoworkerPipelineStatus::Running
        } else if !base.failed_check_task_ids.is_empty()
            || base
                .completion_summary
                .as_ref()
                .is_some_and(|c| c.status == CoworkerJobCompletionStatus::BlockedByFailedChecks)
        {
            CoworkerPipelineStatus::BlockedByFailedChecks
        } else if inspection.job.phase == CoworkerJobPhase::Held
            || inspection.job.holds.iter().any(|h| h.active)
            || inspection
                .job
                .tasks
                .iter()
                .any(|t| t.stage == CoworkerTaskStage::Held || !t.active_hold_ids.is_empty())
        {
            CoworkerPipelineStatus::Held
        } else if !base.blocked_or_failed_task_ids.is_empty()
            || base
                .completion_summary
                .as_ref()
                .is_some_and(|c| c.status == CoworkerJobCompletionStatus::ClosureNotAccepted)
        {
            CoworkerPipelineStatus::BlockedOrFailed
        } else if !base.awaiting_correction_task_ids.is_empty() && in_flight_stages.is_empty() {
            CoworkerPipelineStatus::AwaitingCorrectionExecution
        } else {
            CoworkerPipelineStatus::Running
        };

        base.status = status;
        Ok((status, base))
    }

    fn inspect_host_phase_and_lock(
        &self,
        session_dir: &Path,
        approval: &ApprovalRecord,
    ) -> Result<(Option<HostPhase>, bool), NativeError> {
        match fs::symlink_metadata(session_dir) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok((None, false)),
            Err(err) => {
                return Err(map_io_error(
                    "failed to inspect host session directory",
                    err,
                ))
            }
        }
        let manifest =
            read_host_manifest_optional(session_dir, approval).map_err(map_host_error)?;
        let lock_held = is_host_lock_held(session_dir).unwrap_or(false);
        Ok((manifest.map(|m| m.phase), lock_held))
    }

    fn read_live_ready_session_task_ledger_optional(
        &self,
        session_dir: &Path,
        approval: &ApprovalRecord,
    ) -> Result<(Vec<TaskDispatchIntent>, Vec<TaskRunReceipt>), NativeError> {
        match fs::symlink_metadata(session_dir) {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok((Vec::new(), Vec::new())),
            Err(err) => {
                return Err(map_io_error(
                    "failed to inspect host session directory",
                    err,
                ))
            }
        }
        if !is_host_lock_held(session_dir).unwrap_or(false) {
            return Ok((Vec::new(), Vec::new()));
        }
        let Some(manifest) =
            read_host_manifest_optional(session_dir, approval).map_err(map_host_error)?
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        if manifest.phase != HostPhase::Ready
            || !manifest.gates.supervisor_authenticated_ready
            || manifest.approval_id != approval.approval_id
            || manifest.control_binding_id != approval.binding_id
            || manifest.thread_uuid != approval.thread_uuid
            || !constant_time_str_eq(&manifest.subject_digest, &approval.subject_digest)
        {
            return Ok((Vec::new(), Vec::new()));
        }
        let Some(ref session_binding_id) = manifest.session_binding_id else {
            return Ok((Vec::new(), Vec::new()));
        };
        Ok(
            read_session_task_ledger_readonly(session_dir, approval, session_binding_id)
                .map_err(map_host_error)?
                .unwrap_or_default(),
        )
    }

    fn synthesize_status_and_tasks(
        &self,
        inspection: &ReadonlyScopedJobInspection,
    ) -> Result<
        (
            CoworkerControllerPhase1Status,
            Vec<CoworkerControllerTaskObservation>,
        ),
        NativeError,
    > {
        let session_dir = self.sessions_dir.join(&inspection.approval.approval_id);
        let mut stopped_host = false;
        let (intents, receipts) = match fs::symlink_metadata(&session_dir) {
            Ok(_) => {
                if let Some(manifest) =
                    read_host_manifest_optional(&session_dir, &inspection.approval)
                        .map_err(map_host_error)?
                {
                    // A known stopped host with no physical host lock cannot prove that a
                    // historical Running/DispatchReserved stage still has an active worker.
                    // Missing manifests and transient RPC failures do not establish this.
                    stopped_host = manifest.phase == HostPhase::Stopped
                        && !is_host_lock_held(&session_dir).map_err(map_host_error)?;
                    if let Some(ref session_binding_id) = manifest.session_binding_id {
                        read_session_task_ledger_readonly(
                            &session_dir,
                            &inspection.approval,
                            session_binding_id,
                        )
                        .map_err(map_host_error)?
                        .unwrap_or_default()
                    } else {
                        (Vec::new(), Vec::new())
                    }
                } else {
                    (Vec::new(), Vec::new())
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => (Vec::new(), Vec::new()),
            Err(err) => {
                return Err(map_io_error(
                    "failed to inspect host session directory",
                    err,
                ))
            }
        };

        let mut observations = Vec::with_capacity(inspection.job.tasks.len());
        let mut active_running_count = 0usize;
        let mut fenced_task_ids = Vec::new();
        let mut unknown_effect_task_ids = Vec::new();

        for task in &inspection.job.tasks {
            let approved_task = inspection
                .approval
                .tasks
                .iter()
                .find(|t| t.task_id == task.task_id);
            let command_key = approved_task
                .and_then(|t| t.command_key.clone())
                .unwrap_or_default();

            let artifact = if task.current_attempt_no >= 1 {
                inspection
                    .runtime_state
                    .get_attempt_artifacts(&task.task_id, task.current_attempt_no)
            } else {
                None
            };
            let latest_attempt = task.attempts.last();
            let receipt = receipts.iter().find(|r| r.task_id == task.task_id);
            let intent = intents.iter().find(|i| i.task_id == task.task_id);

            let run_id = artifact
                .map(|a| a.run_id.clone())
                .or_else(|| latest_attempt.and_then(|a| a.run_id.clone()))
                .or_else(|| receipt.map(|r| r.run_id.clone()));
            let worker_id = artifact
                .map(|a| a.worker_id.clone())
                .or_else(|| latest_attempt.and_then(|a| a.worker_id.clone()))
                .or_else(|| receipt.map(|r| r.worker_id.clone()));
            let task_worktree_id = artifact
                .map(|a| a.task_worktree_id.clone())
                .or_else(|| latest_attempt.and_then(|a| a.task_worktree_id.clone()));
            let observed_model = artifact
                .and_then(|a| a.observed_model.clone())
                .or_else(|| latest_attempt.and_then(|a| a.observed_model.clone()));
            let observed_effort = artifact
                .and_then(|a| a.observed_effort.clone())
                .or_else(|| latest_attempt.and_then(|a| a.observed_effort.clone()));

            let reconciled_terminal_coder =
                crate::coworker_workflow::has_reconciled_terminal_coder_attempt(
                    task,
                    &inspection.job,
                    &inspection.runtime_state,
                );
            let fenced_terminal_coder = reconciled_terminal_coder
                && matches!(
                    task.stage,
                    CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
                );
            let fenced = task.stage == CoworkerTaskStage::Held
                || !task.active_hold_ids.is_empty()
                || inspection.job.phase == CoworkerJobPhase::Held
                || fenced_terminal_coder;
            if fenced {
                fenced_task_ids.push(task.task_id.clone());
            }

            let host_status = match task.stage {
                CoworkerTaskStage::PendingDependencies => {
                    TaskExecutionStatus::DependencyInputsUnverified
                }
                CoworkerTaskStage::ReadyToDispatch => {
                    if !task.depends_on.is_empty() {
                        TaskExecutionStatus::DependencyInputsReady
                    } else {
                        TaskExecutionStatus::NotStarted
                    }
                }
                CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running => {
                    if reconciled_terminal_coder {
                        TaskExecutionStatus::ReadyForReview
                    } else {
                        let status = project_cached_running_host_status(
                            stopped_host,
                            &task.task_id,
                            task.current_attempt_no,
                            &inspection.verified_live_derived_attempts,
                        );
                        if status == TaskExecutionStatus::Unknown {
                            unknown_effect_task_ids.push(task.task_id.clone());
                        } else {
                            active_running_count += 1;
                        }
                        status
                    }
                }
                CoworkerTaskStage::ReadyForReview
                | CoworkerTaskStage::ReviewReserved
                | CoworkerTaskStage::UnderReview
                | CoworkerTaskStage::CorrectInScopeRequired
                | CoworkerTaskStage::Accepted
                | CoworkerTaskStage::IntegrationReserved
                | CoworkerTaskStage::Integrated => TaskExecutionStatus::ReadyForReview,
                CoworkerTaskStage::Held => {
                    if crate::coworker_workflow::is_task_coder_execution_active(
                        task,
                        &inspection.job,
                        &inspection.runtime_state,
                    ) {
                        let status = project_cached_running_host_status(
                            stopped_host,
                            &task.task_id,
                            task.current_attempt_no,
                            &inspection.verified_live_derived_attempts,
                        );
                        if status == TaskExecutionStatus::Unknown {
                            unknown_effect_task_ids.push(task.task_id.clone());
                        } else {
                            active_running_count += 1;
                        }
                        status
                    } else if run_id.is_some() {
                        TaskExecutionStatus::ReadyForReview
                    } else if !task.depends_on.is_empty() {
                        TaskExecutionStatus::DependencyInputsUnverified
                    } else {
                        TaskExecutionStatus::NotStarted
                    }
                }
                CoworkerTaskStage::EffectUnknown => {
                    unknown_effect_task_ids.push(task.task_id.clone());
                    TaskExecutionStatus::Unknown
                }
                CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked => {
                    if intent.is_some() && receipt.is_none() {
                        unknown_effect_task_ids.push(task.task_id.clone());
                        TaskExecutionStatus::Unknown
                    } else {
                        TaskExecutionStatus::Failed
                    }
                }
                CoworkerTaskStage::Cancelled => TaskExecutionStatus::Cancelled,
            };

            observations.push(CoworkerControllerTaskObservation {
                task_id: task.task_id.clone(),
                stage: task.stage,
                host_status,
                command_key,
                run_id,
                worker_id,
                task_worktree_id,
                observed_model,
                observed_effort,
                watchdog_attention: None,
                original_worker_provider_health: inspection
                    .runtime_state
                    .original_worker_provider_health
                    .get(&task.task_id)
                    .cloned(),
                fenced,
            });
        }

        let overall = classify_readonly_phase1_status(
            &inspection.job,
            &inspection.runtime_state,
            active_running_count,
            &fenced_task_ids,
            &unknown_effect_task_ids,
        );

        Ok((overall, observations))
    }

    fn write_driver_state(
        &self,
        key: &CoworkerJobOwnershipKey,
        record: &CoworkerDriverStateRecord,
    ) -> Result<(), NativeError> {
        validate_driver_state_record(record, key)?;
        prepare_private_dir(self.driver_dir(), "coworker_driver_dir")?;
        let filename = state_file_name_for_key(key);
        let bytes = serde_json::to_vec_pretty(record)
            .map_err(|err| map_json_error("coworker_driver_state", err))?;
        let _ = write_private_file_atomic(
            self.driver_dir(),
            &filename,
            &bytes,
            "coworker_driver_state",
        )?;
        Ok(())
    }

    fn resolve_flow_binary(&self) -> Result<PathBuf, NativeError> {
        if let Some(override_path) = &self.flow_binary_override {
            return Ok(override_path.clone());
        }
        if let Ok(current_exe) = std::env::current_exe() {
            if current_exe
                .file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(|stem| stem == "threadlane-flow")
            {
                return Ok(current_exe);
            }
            if let Some(bin_from_env) = std::env::var_os("CARGO_BIN_EXE_threadlane-flow") {
                let candidate = PathBuf::from(bin_from_env);
                if candidate.is_file() {
                    return Ok(candidate);
                }
            }
            if let Some(parent) = current_exe.parent() {
                let direct_sibling = parent.join("threadlane-flow");
                if direct_sibling.is_file() {
                    return Ok(direct_sibling);
                }
                if let Some(grandparent) = parent.parent() {
                    let target_bin = grandparent.join("threadlane-flow");
                    if target_bin.is_file() {
                        return Ok(target_bin);
                    }
                }
            }
        }
        Err(NativeError::new(
            NativeErrorCode::NotConfigured,
            "unable to locate same-package threadlane-flow binary for detached owner launch.",
        ))
    }
}

fn owner_failure_filename(key: &CoworkerJobOwnershipKey) -> String {
    format!("coworker-owner-failure-{}.json", key_digest_tag(key, 32))
}

fn read_owner_failure(
    root: &Path,
    key: &CoworkerJobOwnershipKey,
) -> Result<Option<CoworkerDriverFatalError>, NativeError> {
    Ok(read_owner_failure_record(root, key)?.map(|record| record.fatal_error))
}

fn read_owner_failure_record(
    root: &Path,
    key: &CoworkerJobOwnershipKey,
) -> Result<Option<StoredOwnerFailure>, NativeError> {
    key.validate()?;
    let Some(raw) = read_private_file_bounded(
        &root.join(owner_failure_filename(key)),
        4_096,
        "coworker_owner_failure",
    )?
    else {
        return Ok(None);
    };
    let record: StoredOwnerFailure = serde_json::from_slice(&raw)
        .map_err(|err| map_json_error("coworker_owner_failure", err))?;
    if record.key != *key {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "owner failure diagnostic scope mismatch",
        ));
    }
    record.fatal_error.validate()?;
    validate_prefixed_random_id(&record.owner_instance_id, "cwo", "owner failure instance")?;
    Ok(Some(record))
}

fn persist_owner_failure(
    root: &Path,
    guard: &crate::coworker_workflow::CoworkerJobOwnershipGuard,
    failure: CoworkerDriverFatalError,
) -> Result<CoworkerDriverFatalError, NativeError> {
    let key = guard.key();
    guard.validate_before_effect(&key)?;
    // Keep the first diagnosis, while correlating the current failed physical owner so its
    // retained tick cannot emit a normal milestone notice during fatal-error unwinding.
    let failure = read_owner_failure(root, &key)?.unwrap_or(failure);
    failure.validate()?;
    prepare_private_dir(root, "coworker_driver_dir")?;
    let record = StoredOwnerFailure {
        key: key.clone(),
        owner_instance_id: guard.validate_before_effect(&key)?.instance_id.clone(),
        fatal_error: failure.clone(),
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|err| map_json_error("coworker_owner_failure", err))?;
    guard.validate_before_effect(&key)?;
    write_private_file_atomic(
        root,
        &owner_failure_filename(&key),
        &bytes,
        "coworker_owner_failure",
    )?;
    Ok(failure)
}

fn validate_driver_state_record(
    record: &CoworkerDriverStateRecord,
    expected_key: &CoworkerJobOwnershipKey,
) -> Result<(), NativeError> {
    if record.schema_version != COWORKER_DRIVER_STATE_SCHEMA_VERSION {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!(
                "unsupported coworker_driver_state schema_version {}.",
                record.schema_version
            ),
        ));
    }
    if record.binding_id != expected_key.binding_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "coworker_driver_state.binding_id does not match expected binding scope.",
        ));
    }
    if record.thread_uuid != expected_key.thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "coworker_driver_state.thread_uuid does not match expected thread scope.",
        ));
    }
    if record.job_id != expected_key.job_id {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            "coworker_driver_state.job_id does not match expected job scope.",
        ));
    }
    validate_prefixed_random_id(
        &record.owner_instance_id,
        "cwo",
        "coworker_driver_state.owner_instance_id",
    )?;
    Ok(())
}

fn is_gracefully_released_unlocked_lock(
    key: &CoworkerJobOwnershipKey,
    recorded_summary: Option<&CoworkerOwnershipSummary>,
    lock_file_identity: CoworkerLockFileIdentity,
    persisted_driver_state: Option<&CoworkerDriverStateRecord>,
    lease_active: bool,
) -> bool {
    if lease_active {
        return false;
    }
    let Some(summary) = recorded_summary else {
        return false;
    };
    if summary.validate_for_key(key).is_err() || summary.lock_file_identity != lock_file_identity {
        return false;
    }
    let Some(state) = persisted_driver_state else {
        return false;
    };
    if validate_driver_state_record(state, key).is_err() {
        return false;
    }
    !state.owner_active
        && state.graceful_exit_completed
        && state.owner_uid == current_effective_uid()
        && state.lock_file_identity == lock_file_identity
        && state.owner_pid_hint == summary.pid_hint
        && constant_time_str_eq(&state.owner_instance_id, &summary.instance_id)
        && constant_time_str_eq(&state.owner_challenge_digest, &summary.challenge_digest)
}

fn key_digest_tag(key: &CoworkerJobOwnershipKey, hex_chars: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-coworker-driver-v1\0");
    hasher.update(key.binding_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(key.thread_uuid.as_bytes());
    hasher.update(b"\0");
    hasher.update(key.job_id.as_bytes());
    let full = sha256_hex(&hasher.finalize());
    full[..hex_chars.min(full.len())].to_owned()
}

fn state_file_name_for_key(key: &CoworkerJobOwnershipKey) -> String {
    let tag = key_digest_tag(key, 32);
    format!("coworker-driver-state-{tag}.json")
}

fn socket_file_name_for_key(key: &CoworkerJobOwnershipKey) -> String {
    let short_tag = key_digest_tag(key, 20);
    format!("drv-{short_tag}.sock")
}

fn bind_private_unix_listener(
    driver_dir: &Path,
    sock_file_name: &str,
) -> Result<UnixListener, NativeError> {
    prepare_private_dir(driver_dir, "coworker_driver_dir")?;
    let canonical_path = driver_dir.join(sock_file_name);
    remove_socket_if_present(&canonical_path)?;

    let listener = if canonical_path.as_os_str().len() <= MAX_DIRECT_UNIX_SOCKET_PATH_LEN {
        UnixListener::bind(&canonical_path)
            .map_err(|err| map_io_error("failed to bind coworker driver Unix socket", err))?
    } else {
        let dir_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(driver_dir)
            .map_err(|err| map_io_error("failed to open coworker driver directory FD", err))?;
        let proc_fd_path = {
            use std::os::unix::io::AsRawFd;
            PathBuf::from(format!(
                "/proc/self/fd/{}/{sock_file_name}",
                dir_file.as_raw_fd()
            ))
        };
        UnixListener::bind(&proc_fd_path)
            .map_err(|err| map_io_error("failed to bind coworker driver Unix socket via fd", err))?
    };

    fs::set_permissions(&canonical_path, fs::Permissions::from_mode(0o600))
        .map_err(|err| map_io_error("failed to set 0600 permissions on driver socket", err))?;
    if !validate_existing_private_socket(&canonical_path)? {
        let _ = remove_socket_if_present(&canonical_path);
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "bound coworker driver socket '{}' failed private 0600 socket verification.",
                canonical_path.display()
            ),
        ));
    }
    Ok(listener)
}

async fn connect_private_unix_socket(
    driver_dir: &Path,
    sock_file_name: &str,
) -> Result<Option<UnixStream>, NativeError> {
    prepare_private_dir(driver_dir, "coworker_driver_dir")?;
    let canonical_path = driver_dir.join(sock_file_name);
    if !validate_existing_private_socket(&canonical_path)? {
        return Ok(None);
    }

    let connect_res = if canonical_path.as_os_str().len() <= MAX_DIRECT_UNIX_SOCKET_PATH_LEN {
        tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            UnixStream::connect(&canonical_path),
        )
        .await
    } else {
        let dir_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(driver_dir)
            .map_err(|err| map_io_error("failed to open coworker driver directory FD", err))?;
        let proc_fd_path = {
            use std::os::unix::io::AsRawFd;
            PathBuf::from(format!(
                "/proc/self/fd/{}/{sock_file_name}",
                dir_file.as_raw_fd()
            ))
        };
        tokio::time::timeout(
            Duration::from_millis(DEFAULT_DRIVER_RPC_TIMEOUT_MS),
            UnixStream::connect(&proc_fd_path),
        )
        .await
    };

    match connect_res {
        Ok(Ok(stream)) => Ok(Some(stream)),
        Ok(Err(err))
            if matches!(
                err.kind(),
                ErrorKind::NotFound
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::BrokenPipe
            ) =>
        {
            Ok(None)
        }
        Ok(Err(err)) => Err(map_io_error(
            "failed to connect to coworker driver Unix socket",
            err,
        )),
        Err(_) => Ok(None),
    }
}

fn validate_existing_private_socket(path: &Path) -> Result<bool, NativeError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(map_io_error(
                "failed to inspect coworker driver socket",
                err,
            ))
        }
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_socket() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "coworker driver socket path '{}' is not a regular Unix domain socket.",
                path.display()
            ),
        ));
    }
    let expected_uid = current_effective_uid();
    if meta.uid() != expected_uid {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "coworker driver socket '{}' uid {} does not match effective uid {expected_uid}.",
                path.display(),
                meta.uid()
            ),
        ));
    }
    let mode = meta.mode() & 0o777;
    if mode != 0o600 {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!(
                "coworker driver socket '{}' must have 0600 permissions, got {:04o}.",
                path.display(),
                mode
            ),
        ));
    }
    Ok(true)
}

fn remove_socket_if_present(path: &Path) -> Result<(), NativeError> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_socket() {
                return Err(NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    format!("refusing to unlink non-socket path '{}'.", path.display()),
                ));
            }
            match fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(map_io_error("failed to remove coworker driver socket", err)),
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(map_io_error(
            "failed to inspect coworker driver socket",
            err,
        )),
    }
}

fn push_unique_id(list: &mut Vec<String>, id: &str) {
    if !list.iter().any(|existing| existing == id) {
        list.push(id.to_owned());
    }
}

const fn boundary_for_mode(mode: CoworkerDriverExecutionMode) -> CoworkerDriverPipelineBoundary {
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => {
            CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
        }
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => {
            CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly
        }
    }
}

fn boundary_note_for_mode(mode: CoworkerDriverExecutionMode) -> String {
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => {
            "Full automatic code-driver workflow retains ONE CoworkerPipelineDriver across ticks \
             and RPCs, running initial/derived Coder attempts, task verification checks, Reviewer \
             evaluation, bounded in-scope corrections, original per-task RunIntegration, downstream \
             dependency seals, final combined candidate checks, and ONE Closure evaluation. \
             Cancellation records stop_requested before effects, signals original/derived workers \
             where supported, fences late promotion, and drains finite in-flight check/Reviewer/Closure \
             calls under the held guard without claiming remote inference termination."
                .to_owned()
        }
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => phase1_boundary_note(),
    }
}

pub(crate) fn envelope_authorizes_full_pipeline(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    let env = &runtime_state.envelope;
    if !env.operator_attested
        || !env.allow_task_worktree_commits
        || !env.allow_run_integration_execution
        || env.check_commands.is_empty()
        || !env.role_budgets.contains_key(&WorkflowRole::Reviewer)
        || !env.role_budgets.contains_key(&WorkflowRole::Closure)
    {
        return false;
    }
    job.approval_linkage.as_ref().is_some_and(|linkage| {
        linkage.authorizes_local_git_integration()
            && linkage.authorizes_reviewer_dispatch(&env.reviewer_choice)
            && linkage.authorizes_closure_dispatch(&env.closure_choice)
    })
}

fn resolve_effective_execution_mode(
    requested_mode: Option<CoworkerDriverExecutionMode>,
    persisted_state: Option<&CoworkerDriverStateRecord>,
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> Result<CoworkerDriverExecutionMode, NativeError> {
    let envelope_allows_full = envelope_authorizes_full_pipeline(job, runtime_state);
    match requested_mode {
        Some(CoworkerDriverExecutionMode::FullAutomaticPipeline) => {
            if !envelope_allows_full {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "frozen controller execution envelope does not authorize full_automatic_pipeline \
                     (requires frozen Reviewer/Closure choices and budgets, non-empty check_commands, \
                      allow_task_worktree_commits, allow_run_integration_execution, and local_git_integration_allowed).",
                ));
            }
            Ok(CoworkerDriverExecutionMode::FullAutomaticPipeline)
        }
        Some(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly) => {
            Ok(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly)
        }
        None => {
            if let Some(prev_mode) = persisted_state.and_then(|s| s.execution_mode) {
                if prev_mode == CoworkerDriverExecutionMode::Phase1InitialDispatchOnly {
                    return Ok(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly);
                }
                if prev_mode == CoworkerDriverExecutionMode::FullAutomaticPipeline
                    && envelope_allows_full
                {
                    return Ok(CoworkerDriverExecutionMode::FullAutomaticPipeline);
                }
            }
            if persisted_state.is_some_and(|s| {
                s.execution_mode.is_none()
                    && s.pipeline_boundary
                        == CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly
            }) {
                return Ok(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly);
            }
            if envelope_allows_full {
                Ok(CoworkerDriverExecutionMode::FullAutomaticPipeline)
            } else {
                Ok(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly)
            }
        }
    }
}

fn owner_has_active_work(
    mode: CoworkerDriverExecutionMode,
    step_report: Option<&CoworkerControllerStepReport>,
    pipeline_report: Option<&CoworkerDriverPipelineReport>,
    in_flight_count: usize,
) -> bool {
    if in_flight_count > 0 {
        return true;
    }
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => {
            if let Some(pipe) = pipeline_report {
                if pipe.status == CoworkerPipelineStatus::Running
                    || !pipe.in_flight_stages.is_empty()
                    || pipe.draining_in_flight_after_stop
                {
                    return true;
                }
                false
            } else {
                step_report.is_some_and(report_has_active_work)
            }
        }
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => {
            step_report.is_some_and(report_has_active_work)
        }
    }
}

fn owner_is_terminal_or_stopped(
    mode: CoworkerDriverExecutionMode,
    step_report: Option<&CoworkerControllerStepReport>,
    pipeline_report: Option<&CoworkerDriverPipelineReport>,
    in_flight_count: usize,
) -> bool {
    if in_flight_count > 0 {
        return false;
    }
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => pipeline_report.is_some_and(|pipe| {
            !pipe.draining_in_flight_after_stop
                && pipe.in_flight_stages.is_empty()
                && matches!(
                    pipe.status,
                    CoworkerPipelineStatus::Completed
                        | CoworkerPipelineStatus::AwaitingCorrectionExecution
                        | CoworkerPipelineStatus::BlockedByFailedChecks
                        | CoworkerPipelineStatus::Held
                        | CoworkerPipelineStatus::BlockedOrFailed
                        | CoworkerPipelineStatus::BlockedOnUnknownEffect
                        | CoworkerPipelineStatus::Stopped
                )
        }),
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => {
            step_report.is_some_and(|r| is_terminal_or_stopped_status(r.status))
        }
    }
}

fn owner_is_pipeline_await(
    mode: CoworkerDriverExecutionMode,
    step_report: Option<&CoworkerControllerStepReport>,
    _pipeline_report: Option<&CoworkerDriverPipelineReport>,
    in_flight_count: usize,
) -> bool {
    if in_flight_count > 0 {
        return false;
    }
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => false,
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => {
            step_report.is_some_and(|r| is_pipeline_await_status(r.status))
        }
    }
}

fn report_has_active_work(report: &CoworkerControllerStepReport) -> bool {
    if report.status == CoworkerControllerPhase1Status::RunningTasks
        || report.active_running_count > 0
    {
        return true;
    }
    report.tasks.iter().any(|obs| {
        matches!(
            obs.host_status,
            TaskExecutionStatus::Queued
                | TaskExecutionStatus::Claimed
                | TaskExecutionStatus::Running
                | TaskExecutionStatus::CancelRequested
        ) || (matches!(
            obs.stage,
            CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running
        ) && obs.host_status != TaskExecutionStatus::ReadyForReview)
    })
}

const fn is_terminal_or_stopped_status(status: CoworkerControllerPhase1Status) -> bool {
    matches!(
        status,
        CoworkerControllerPhase1Status::AllTasksIntegrated
            | CoworkerControllerPhase1Status::BlockedOnUnknownEffect
            | CoworkerControllerPhase1Status::Stopped
            | CoworkerControllerPhase1Status::BlockedOrFailed
    )
}

const fn is_pipeline_await_status(status: CoworkerControllerPhase1Status) -> bool {
    matches!(
        status,
        CoworkerControllerPhase1Status::AwaitingChecksOrReview
            | CoworkerControllerPhase1Status::AwaitingDependencyAcceptanceOrSeal
            | CoworkerControllerPhase1Status::Held
    )
}

fn is_settled_blocked_or_failed_job(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    approval_id: &str,
    plans_dir: &Path,
) -> bool {
    if job.tasks.is_empty()
        || !runtime_state.unresolved_effects().is_empty()
        || job
            .outbox
            .iter()
            .any(|o| o.status.blocks_replay_and_new_dispatch())
        || job.tasks.iter().any(|t| t.unresolved_outbox_id.is_some())
    {
        return false;
    }
    job.tasks.iter().any(|t| {
        matches!(
            t.stage,
            CoworkerTaskStage::Failed | CoworkerTaskStage::Blocked | CoworkerTaskStage::Cancelled
        )
    }) && job.tasks.iter().all(|t| match t.stage {
        CoworkerTaskStage::Failed
        | CoworkerTaskStage::Blocked
        | CoworkerTaskStage::Cancelled
        | CoworkerTaskStage::Accepted
        | CoworkerTaskStage::Integrated => true,
        CoworkerTaskStage::Held => {
            if t.current_attempt_no == 0 && t.attempts.is_empty() {
                !crate::coworker_workflow::is_task_coder_execution_active(t, job, runtime_state)
            } else {
                crate::coworker_workflow::has_reconciled_terminal_coder_attempt(
                    t,
                    job,
                    runtime_state,
                ) && !crate::coworker_workflow::is_task_coder_execution_active(
                    t,
                    job,
                    runtime_state,
                )
            }
        }
        CoworkerTaskStage::DispatchReserved | CoworkerTaskStage::Running => {
            crate::coworker_workflow::has_reconciled_terminal_coder_attempt(t, job, runtime_state)
                && crate::coworker_workflow::is_task_fenced_by_hold_or_sibling(
                    t,
                    job,
                    runtime_state,
                    approval_id,
                    plans_dir,
                )
                .unwrap_or(false)
        }
        CoworkerTaskStage::PendingDependencies | CoworkerTaskStage::ReadyToDispatch => {
            crate::coworker_workflow::is_task_fenced_by_hold_or_sibling(
                t,
                job,
                runtime_state,
                approval_id,
                plans_dir,
            )
            .unwrap_or(false)
        }
        _ => false,
    })
}

fn is_terminal_or_stopped_job(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    approval_id: &str,
    plans_dir: &Path,
    mode: CoworkerDriverExecutionMode,
) -> bool {
    if runtime_state.stop_requested
        || matches!(
            job.phase,
            CoworkerJobPhase::Completed | CoworkerJobPhase::Cancelled
        )
        || is_settled_blocked_or_failed_job(job, runtime_state, approval_id, plans_dir)
    {
        return true;
    }
    match mode {
        CoworkerDriverExecutionMode::FullAutomaticPipeline => false,
        CoworkerDriverExecutionMode::Phase1InitialDispatchOnly => {
            !job.tasks.is_empty()
                && job.tasks.iter().all(|t| {
                    matches!(
                        t.stage,
                        CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
                    )
                })
        }
    }
}

fn is_outbox_confirmed_but_fenced_by_stop(
    job: &CoworkerJobRecord,
    outbox: &OutboxDispatchRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    if !runtime_state.stop_requested {
        return false;
    }
    if matches!(
        outbox.status,
        OutboxDispatchStatus::DispatchedEffectConfirmed | OutboxDispatchStatus::ReconciledConfirmed
    ) {
        return runtime_state
            .pending_effects
            .get(&outbox.outbox_id)
            .is_some_and(|eff| {
                matches!(
                    eff.status,
                    PendingControllerEffectStatus::EffectConfirmed
                        | PendingControllerEffectStatus::EffectFailedTerminal
                )
            });
    }
    is_reserved_review_outbox_confirmed_but_fenced_by_stop(job, outbox, runtime_state)
}

fn is_reserved_review_outbox_confirmed_but_fenced_by_stop(
    job: &CoworkerJobRecord,
    outbox: &OutboxDispatchRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    if !runtime_state.stop_requested
        || runtime_state.job_id != job.job_id
        || !runtime_state.unresolved_effects().is_empty()
        || runtime_state
            .pending_effects
            .values()
            .any(|eff| eff.status == PendingControllerEffectStatus::EffectUnknown)
        || outbox.status != OutboxDispatchStatus::ReservedBeforeEffect
        || outbox.kind != OutboxDispatchKind::TaskReviewDispatch
        || outbox.role != WorkflowRole::Reviewer
        || outbox.attempt_no == 0
        || outbox.request_key.is_empty()
        || outbox.subject_digest.is_empty()
    {
        return false;
    }
    let Some(task) = job.tasks.iter().find(|t| t.task_id == outbox.task_id) else {
        return false;
    };
    if task.stage != CoworkerTaskStage::ReviewReserved
        || task.current_attempt_no != outbox.attempt_no
        || task.unresolved_outbox_id.as_deref() != Some(outbox.outbox_id.as_str())
        || job.outbox.iter().any(|o| {
            o.kind == OutboxDispatchKind::TaskReviewDispatch
                && o.task_id == outbox.task_id
                && o.attempt_no == outbox.attempt_no
                && (o.outbox_id != outbox.outbox_id
                    || o.request_key != outbox.request_key
                    || o.subject_digest != outbox.subject_digest)
        })
    {
        return false;
    }
    let Some(artifacts) = runtime_state.get_attempt_artifacts(&outbox.task_id, outbox.attempt_no)
    else {
        return false;
    };
    if artifacts.task_id != outbox.task_id
        || artifacts.attempt_no != outbox.attempt_no
        || artifacts
            .review_subject
            .as_ref()
            .is_none_or(|s| s.subject_digest != outbox.subject_digest)
    {
        return false;
    }
    let mut matched = false;
    for eff in runtime_state.pending_effects.values() {
        if eff.kind == PendingControllerEffectKind::ReviewerInvocation
            && ((eff.task_id.as_deref() == Some(outbox.task_id.as_str())
                && eff.attempt_no == Some(outbox.attempt_no))
                || eff.request_key == outbox.request_key)
        {
            if eff.job_id != job.job_id
                || eff.task_id.as_deref() != Some(outbox.task_id.as_str())
                || eff.attempt_no != Some(outbox.attempt_no)
                || eff.request_key != outbox.request_key
                || eff.status != PendingControllerEffectStatus::EffectConfirmed
                || eff.subject_digest != outbox.subject_digest
                || matched
            {
                return false;
            }
            matched = true;
        }
    }
    matched
}

fn is_review_reserved_task_confirmed_but_fenced_by_stop(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime_state: &ControllerJobRuntimeState,
) -> bool {
    task.stage == CoworkerTaskStage::ReviewReserved
        && task
            .unresolved_outbox_id
            .as_deref()
            .and_then(|id| job.outbox.iter().find(|o| o.outbox_id == id))
            .is_some_and(|outbox| {
                outbox.task_id == task.task_id
                    && outbox.attempt_no == task.current_attempt_no
                    && is_reserved_review_outbox_confirmed_but_fenced_by_stop(
                        job,
                        outbox,
                        runtime_state,
                    )
            })
}

// Shared current-status projection for historical Running/DispatchReserved task stages.
fn project_cached_running_host_status(
    stopped_host: bool,
    task_id: &str,
    attempt_no: u32,
    verified_live_derived_attempts: &std::collections::BTreeMap<String, u32>,
) -> TaskExecutionStatus {
    let verified_direct_attempt =
        attempt_no >= 2 && verified_live_derived_attempts.get(task_id) == Some(&attempt_no);
    if stopped_host && !verified_direct_attempt {
        TaskExecutionStatus::Unknown
    } else {
        TaskExecutionStatus::Running
    }
}

fn has_unresolved_or_unknown_effect(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    overall_status: CoworkerControllerPhase1Status,
) -> bool {
    overall_status == CoworkerControllerPhase1Status::BlockedOnUnknownEffect
        || !runtime_state.unresolved_effects().is_empty()
        || job.outbox.iter().any(|o| {
            o.status.blocks_replay_and_new_dispatch()
                && !is_outbox_confirmed_but_fenced_by_stop(job, o, runtime_state)
        })
        || job.tasks.iter().any(|t| {
            t.stage == CoworkerTaskStage::EffectUnknown
                || (t.unresolved_outbox_id.is_some()
                    && !is_review_reserved_task_confirmed_but_fenced_by_stop(job, t, runtime_state))
        })
}

fn classify_readonly_phase1_status(
    job: &CoworkerJobRecord,
    runtime_state: &ControllerJobRuntimeState,
    active_running_count: usize,
    fenced_task_ids: &[String],
    unknown_effect_task_ids: &[String],
) -> CoworkerControllerPhase1Status {
    if runtime_state.stop_requested || job.phase == CoworkerJobPhase::Cancelled {
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
    if active_running_count > 0
        || job.tasks.iter().any(|t| {
            t.stage == CoworkerTaskStage::ReadyToDispatch && !fenced_task_ids.contains(&t.task_id)
        })
    {
        return CoworkerControllerPhase1Status::RunningTasks;
    }
    if (!job.tasks.is_empty()
        && job.tasks.iter().all(|t| {
            matches!(
                t.stage,
                CoworkerTaskStage::Accepted | CoworkerTaskStage::Integrated
            )
        }))
        || job.phase == CoworkerJobPhase::Completed
    {
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

#[cfg(unix)]
fn current_effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

#[cfg(not(unix))]
fn current_effective_uid() -> u32 {
    0
}

fn validate_prefixed_random_id(
    raw: &str,
    expected_prefix: &str,
    field: &'static str,
) -> Result<(), NativeError> {
    let clean_prefix = expected_prefix.trim_end_matches('_');
    let Some(rest) = raw.strip_prefix(clean_prefix) else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must begin with `{clean_prefix}_`."),
        ));
    };
    let Some(hex_part) = rest.strip_prefix('_') else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("{field} must begin with `{clean_prefix}_`."),
        ));
    };
    validate_sha256_hex_digest(hex_part, field)
        .map(|_| ())
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                format!(
                    "{field} must consist of `{clean_prefix}_` followed by 64 lowercase hex characters."
                ),
            )
        })
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
    NativeError::new(code, sanitize_message(&err.to_string()))
}

fn map_authority_error(err: AuthorityError) -> NativeError {
    let code = match &err {
        AuthorityError::SecurityViolation { .. }
        | AuthorityError::BindingMismatch { .. }
        | AuthorityError::ThreadUuidMismatch { .. } => NativeErrorCode::SecurityViolation,
        AuthorityError::Io { .. } => NativeErrorCode::IoError,
        _ => NativeErrorCode::ValidationFailed,
    };
    NativeError::new(code, sanitize_message(&err.to_string()))
}

fn map_io_error(context: &str, err: std::io::Error) -> NativeError {
    let code = match err.kind() {
        ErrorKind::PermissionDenied => NativeErrorCode::SecurityViolation,
        ErrorKind::TimedOut | ErrorKind::WouldBlock => NativeErrorCode::TimedOut,
        _ => NativeErrorCode::IoError,
    };
    NativeError::new(code, format!("{context}: {}", err.kind()))
}

fn map_json_error(context: &str, _err: serde_json::Error) -> NativeError {
    NativeError::new(
        NativeErrorCode::ProtocolError,
        format!("{context}: malformed JSON payload."),
    )
}

fn sanitize_message(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(256)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        "coworker driver operation failed.".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn phase1_boundary_note() -> String {
    "Phase 1 code controller dispatches initial task attempts and reconciles host receipts to \
     AwaitingChecksOrReview; automated checks, reviewer/architect cycles, task/job closure, and \
     running derived-attempt cancellation are wired in subsequent milestones."
        .to_owned()
}

fn verify_no_full_job_external_resolution(
    workflow_store: &CoworkerWorkflowStore,
    binding_id: &str,
    job_id: &str,
) -> Result<(), NativeError> {
    let ledger = workflow_store.read_binding_ledger(binding_id)?;
    let Some(job) = ledger.jobs.iter().find(|job| job.job_id == job_id) else {
        return Ok(());
    };
    let target_plan_id = job
        .planning_result
        .as_ref()
        .map(|planning| planning.plan_id.as_str())
        .or_else(|| {
            job.approval_linkage
                .as_ref()
                .map(|linkage| linkage.plan_id.as_str())
        });
    let target_approval_id = job
        .approval_linkage
        .as_ref()
        .map(|linkage| linkage.approval_id.as_str());
    if let Some(block) = crate::dispatch_guard::external_resolution_blocks_full_job(
        &ledger,
        Some(job_id),
        target_plan_id,
        target_approval_id,
    ) {
        return Err(NativeError::new(
            NativeErrorCode::UnsupportedCapability,
            format!(
                "job `{job_id}` is covered by full-job external resolution receipt `{}`; a receipt is not review, integration, closure, or dependency acceptance, and controller advancement requires an explicit replan and reapproval",
                block.receipt_id
            ),
        ));
    }
    Ok(())
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn completion_reservation_matches_live_effect(
    effect: &PendingControllerEffect,
    live_effects: &LiveCompletionEffects,
    lease: &ControllerJobLeaseRecord,
) -> bool {
    effect.status == PendingControllerEffectStatus::ReservedBeforeEffect
        && effect.task_id.is_none()
        && effect.attempt_no.is_none()
        && matches!(
            effect.kind,
            PendingControllerEffectKind::ClosureInvocation
                | PendingControllerEffectKind::JobAggregation
                | PendingControllerEffectKind::CheckExecution
        )
        && live_effects
            .get(&effect.effect_id)
            .is_some_and(|(owned, instance_id, epoch)| {
                owned == effect && instance_id == &lease.instance_id && *epoch == lease.lease_epoch
            })
}

fn completion_lease_matches_live_owner(
    lease: &ControllerJobLeaseRecord,
    key: &CoworkerJobOwnershipKey,
    verified_owner: &CoworkerOwnershipSummary,
    now_ms: u64,
) -> bool {
    verified_owner.validate_for_key(key).is_ok()
        && lease.is_active_at(now_ms)
        && lease.lease_epoch > 0
        && lease.expires_at_ms > lease.acquired_at_ms
        && lease.job_id == key.job_id
        && lease.binding_id == key.binding_id
        && lease.instance_id
            == deterministic_controller_instance_id(
                &key.binding_id,
                &key.job_id,
                &verified_owner.instance_id,
            )
}

fn filter_live_owned_completion_reservations(
    inspection: &mut ReadonlyScopedJobInspection,
    key: &CoworkerJobOwnershipKey,
    verified_owner: &CoworkerOwnershipSummary,
    stages: &[CoworkerPipelineInFlightStageRef],
    live_effects: &LiveCompletionEffects,
) {
    let job = &inspection.job;
    let runtime = &inspection.runtime_state;
    if verified_owner.validate_for_key(key).is_err()
        || job.binding_id != key.binding_id
        || job.thread_uuid != key.thread_uuid
        || job.job_id != key.job_id
        || runtime.job_id != key.job_id
        || runtime.envelope.binding_id != key.binding_id
        || runtime.envelope.thread_uuid != key.thread_uuid
        || job.tasks.is_empty()
        || job
            .tasks
            .iter()
            .any(|task| task.stage != CoworkerTaskStage::Integrated)
        || !stages.iter().any(|stage| {
            stage.kind == CoworkerPipelineInFlightStageKind::JobCompletion
                && stage.task_id.is_none()
                && stage.attempt_no.is_none()
        })
        || !runtime.lease.as_ref().is_some_and(|lease| {
            completion_lease_matches_live_owner(lease, key, verified_owner, current_time_ms())
        })
        || runtime
            .pending_effects
            .values()
            .any(|effect| effect.status == PendingControllerEffectStatus::EffectUnknown)
        || job
            .outbox
            .iter()
            .any(|outbox| outbox.status == OutboxDispatchStatus::EffectUnknown)
    {
        return;
    }
    // This is a copy used only by the authenticated held owner's RPC response. The watch
    // contains only effects newly persisted inside its current completion future. Metadata,
    // a matching stage kind, a recovered reservation, or a stale owner cannot populate it.
    let lease = runtime
        .lease
        .as_ref()
        .expect("verified completion lease")
        .clone();
    inspection
        .runtime_state
        .pending_effects
        .retain(|_, effect| {
            effect.job_id != key.job_id
                || !completion_reservation_matches_live_effect(effect, live_effects, &lease)
        });
}

fn reserved_check_matches_live_stage(
    effect: &crate::coworker_workflow::PendingControllerEffect,
    stage: &CoworkerPipelineInFlightStageRef,
) -> bool {
    use crate::coworker_workflow::CoworkerPipelineInFlightStageKind;
    effect.kind == PendingControllerEffectKind::CheckExecution
        && effect.status == PendingControllerEffectStatus::ReservedBeforeEffect
        && effect.task_id == stage.task_id
        && effect.attempt_no == stage.attempt_no
        && match stage.kind {
            CoworkerPipelineInFlightStageKind::TaskVerification => {
                stage.task_id.is_some() && stage.attempt_no.is_some_and(|n| n > 0)
            }
            CoworkerPipelineInFlightStageKind::JobCompletion => {
                stage.task_id.is_none() && stage.attempt_no.is_none()
            }
            _ => false,
        }
}

fn filter_live_owned_in_flight_check_reservations(
    inspection: &mut ReadonlyScopedJobInspection,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) {
    let job = &inspection.job;
    let runtime = &inspection.runtime_state;
    if runtime.job_id != job.job_id
        || !runtime.lease.as_ref().is_some_and(|lease| {
            lease.active && lease.job_id == job.job_id && lease.binding_id == job.binding_id
        })
    {
        return;
    }
    let matched: Vec<String> = runtime
        .pending_effects
        .values()
        .filter(|effect| {
            if effect.job_id != job.job_id
                || !in_flight_stages
                    .iter()
                    .any(|stage| reserved_check_matches_live_stage(effect, stage))
            {
                return false;
            }
            let Some(spec) = effect
                .external_correlation_id
                .as_ref()
                .and_then(|id| runtime.envelope.check_commands.get(id))
            else {
                return false;
            };
            match effect.task_id.as_deref() {
                Some(task_id) => {
                    spec.required_for_task_review
                        && job.tasks.iter().any(|task| {
                            task.task_id == task_id
                                && Some(task.current_attempt_no) == effect.attempt_no
                                && task.stage == CoworkerTaskStage::ReadyForReview
                        })
                }
                None => false,
            }
        })
        .map(|effect| effect.effect_id.clone())
        .collect();
    // Only the authenticated live owner's read-only copy is filtered. Persisted intent,
    // true EffectUnknown, and reservations without an exactly matching live future stay fenced.
    inspection
        .runtime_state
        .pending_effects
        .retain(|_, effect| !matched.contains(&effect.effect_id));
}

fn filter_live_owned_in_flight_review_reservations(
    inspection: &mut ReadonlyScopedJobInspection,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) {
    let matched: Vec<(String, String, String)> = inspection
        .job
        .outbox
        .iter()
        .filter_map(|o| {
            live_owned_in_flight_review_effect_id(
                &inspection.job,
                o,
                &inspection.runtime_state,
                in_flight_stages,
            )
            .map(|eff_id| (o.task_id.clone(), o.outbox_id.clone(), eff_id))
        })
        .collect();
    for (task_id, outbox_id, effect_id) in matched {
        for task in &mut inspection.job.tasks {
            if task.task_id == task_id
                && task.unresolved_outbox_id.as_deref() == Some(outbox_id.as_str())
            {
                task.unresolved_outbox_id = None;
            }
        }
        inspection.job.outbox.retain(|o| o.outbox_id != outbox_id);
        inspection
            .runtime_state
            .pending_effects
            .retain(|_, eff| eff.effect_id != effect_id);
    }
}

fn live_owned_in_flight_review_effect_id(
    job: &CoworkerJobRecord,
    outbox: &OutboxDispatchRecord,
    runtime_state: &ControllerJobRuntimeState,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> Option<String> {
    if runtime_state.job_id != job.job_id
        || runtime_state
            .pending_effects
            .values()
            .any(|eff| eff.status == PendingControllerEffectStatus::EffectUnknown)
        || outbox.status != OutboxDispatchStatus::ReservedBeforeEffect
        || outbox.kind != OutboxDispatchKind::TaskReviewDispatch
        || outbox.role != WorkflowRole::Reviewer
        || outbox.attempt_no == 0
        || outbox.request_key.is_empty()
        || outbox.subject_digest.is_empty()
        || !in_flight_stages.iter().any(|s| {
            s.kind == crate::coworker_workflow::CoworkerPipelineInFlightStageKind::TaskReview
                && s.task_id.as_deref() == Some(outbox.task_id.as_str())
                && s.attempt_no == Some(outbox.attempt_no)
        })
        || in_flight_stages.iter().any(|s| {
            s.task_id.as_deref() == Some(outbox.task_id.as_str())
                && (s.kind
                    != crate::coworker_workflow::CoworkerPipelineInFlightStageKind::TaskReview
                    || s.attempt_no != Some(outbox.attempt_no))
        })
    {
        return None;
    }
    let task = job.tasks.iter().find(|t| t.task_id == outbox.task_id)?;
    if task.stage != CoworkerTaskStage::ReviewReserved
        || task.current_attempt_no != outbox.attempt_no
        || task.unresolved_outbox_id.as_deref() != Some(outbox.outbox_id.as_str())
        || job.outbox.iter().any(|o| {
            o.task_id == outbox.task_id
                && ((o.kind == OutboxDispatchKind::TaskReviewDispatch
                    && o.attempt_no == outbox.attempt_no
                    && (o.outbox_id != outbox.outbox_id
                        || o.request_key != outbox.request_key
                        || o.subject_digest != outbox.subject_digest))
                    || (o.outbox_id != outbox.outbox_id
                        && o.status.blocks_replay_and_new_dispatch()))
        })
    {
        return None;
    }
    let artifacts = runtime_state.get_attempt_artifacts(&outbox.task_id, outbox.attempt_no)?;
    if artifacts.task_id != outbox.task_id
        || artifacts.attempt_no != outbox.attempt_no
        || artifacts
            .review_subject
            .as_ref()
            .is_none_or(|s| s.subject_digest != outbox.subject_digest)
    {
        return None;
    }
    let mut matched_effect_id = None;
    for eff in runtime_state.pending_effects.values() {
        let same_task_or_request = eff.task_id.as_deref() == Some(outbox.task_id.as_str())
            || eff.request_key == outbox.request_key;
        if !same_task_or_request {
            continue;
        }
        if eff.kind != PendingControllerEffectKind::ReviewerInvocation
            || eff.attempt_no != Some(outbox.attempt_no)
        {
            if eff.status.blocks_replay() || eff.request_key == outbox.request_key {
                return None;
            }
            continue;
        }
        if eff.job_id != job.job_id
            || eff.task_id.as_deref() != Some(outbox.task_id.as_str())
            || eff.request_key != outbox.request_key
            || !matches!(
                eff.status,
                PendingControllerEffectStatus::ReservedBeforeEffect
                    | PendingControllerEffectStatus::EffectConfirmed
            )
            || eff.subject_digest != outbox.subject_digest
            || matched_effect_id.is_some()
        {
            return None;
        }
        matched_effect_id = Some(eff.effect_id.clone());
    }
    matched_effect_id
}

fn is_live_owned_in_flight_review_outbox(
    job: &CoworkerJobRecord,
    outbox: &OutboxDispatchRecord,
    runtime_state: &ControllerJobRuntimeState,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    live_owned_in_flight_review_effect_id(job, outbox, runtime_state, in_flight_stages).is_some()
}

fn is_live_owned_in_flight_review_task(
    job: &CoworkerJobRecord,
    task: &CoworkerTaskRecord,
    runtime_state: &ControllerJobRuntimeState,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    task.stage == CoworkerTaskStage::ReviewReserved
        && task
            .unresolved_outbox_id
            .as_deref()
            .and_then(|id| job.outbox.iter().find(|o| o.outbox_id == id))
            .is_some_and(|outbox| {
                outbox.task_id == task.task_id
                    && outbox.attempt_no == task.current_attempt_no
                    && is_live_owned_in_flight_review_outbox(
                        job,
                        outbox,
                        runtime_state,
                        in_flight_stages,
                    )
            })
}

fn is_live_owned_in_flight_review_effect(
    job: &CoworkerJobRecord,
    effect: &crate::coworker_workflow::PendingControllerEffect,
    runtime_state: &ControllerJobRuntimeState,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    job.outbox.iter().any(|outbox| {
        live_owned_in_flight_review_effect_id(job, outbox, runtime_state, in_flight_stages)
            .as_deref()
            == Some(effect.effect_id.as_str())
    })
}

#[allow(clippy::too_many_arguments)]
fn filter_live_owned_in_flight_coder_reservations(
    inspection: &mut ReadonlyScopedJobInspection,
    key: &CoworkerJobOwnershipKey,
    verified_owner_summary: &CoworkerOwnershipSummary,
    effective_mode: CoworkerDriverExecutionMode,
    host_phase: Option<HostPhase>,
    host_lock_held: bool,
    session_intents: &[TaskDispatchIntent],
    session_receipts: &[TaskRunReceipt],
    plans_dir: &Path,
    step_report: Option<&CoworkerControllerStepReport>,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) {
    inspection.verified_live_derived_attempts.clear();
    if verified_owner_summary.validate_for_key(key).is_err() {
        return;
    }
    let job = &inspection.job;
    let runtime = &inspection.runtime_state;
    let approval = &inspection.approval;
    if job.binding_id != key.binding_id
        || job.thread_uuid != key.thread_uuid
        || job.job_id != key.job_id
        || approval.binding_id != key.binding_id
        || approval.thread_uuid != key.thread_uuid
        || runtime.envelope.binding_id != key.binding_id
        || runtime.envelope.thread_uuid != key.thread_uuid
        || runtime.job_id != key.job_id
        || runtime.envelope.approval_id != approval.approval_id
        || !matches!(
            job.phase,
            CoworkerJobPhase::Executing | CoworkerJobPhase::Held
        )
        || runtime.stop_requested
    {
        return;
    }
    let expected_controller_instance_id = deterministic_controller_instance_id(
        &job.binding_id,
        &job.job_id,
        &verified_owner_summary.instance_id,
    );
    let Some(ref lease) = runtime.lease else {
        return;
    };
    if !lease.is_active_at(current_time_ms())
        || lease.job_id != job.job_id
        || lease.binding_id != job.binding_id
        || lease.lease_epoch == 0
        || lease.expires_at_ms <= lease.acquired_at_ms
        || !constant_time_str_eq(&lease.instance_id, &expected_controller_instance_id)
    {
        return;
    }
    if runtime
        .pending_effects
        .values()
        .any(|eff| eff.status == PendingControllerEffectStatus::EffectUnknown)
        || job
            .outbox
            .iter()
            .any(|o| o.status == OutboxDispatchStatus::EffectUnknown)
        || job
            .tasks
            .iter()
            .any(|t| t.stage == CoworkerTaskStage::EffectUnknown)
    {
        return;
    }

    let matched: Vec<String> = runtime
        .pending_effects
        .values()
        .filter(|effect| {
            live_owned_running_coder_effect_matches(
                inspection,
                effect,
                effective_mode,
                host_phase,
                host_lock_held,
                session_intents,
                session_receipts,
                plans_dir,
                step_report,
                in_flight_stages,
            )
        })
        .map(|effect| effect.effect_id.clone())
        .collect();

    inspection.verified_live_derived_attempts = runtime
        .pending_effects
        .values()
        .filter(|effect| matched.contains(&effect.effect_id))
        .filter_map(|effect| {
            let attempt = effect.attempt_no.filter(|&attempt| attempt >= 2)?;
            Some((effect.task_id.clone()?, attempt))
        })
        .collect();

    // Only the authenticated live owner's read-only status inspection copy is filtered.
    // Durable PendingControllerEffect records on disk, true EffectUnknown, DispatchReserved
    // unstarted/reopened reservations, and stale or mismatched correlations remain untouched.
    inspection
        .runtime_state
        .pending_effects
        .retain(|_, effect| !matched.contains(&effect.effect_id));
}

#[allow(clippy::too_many_arguments)]
fn live_owned_running_coder_effect_matches(
    inspection: &ReadonlyScopedJobInspection,
    effect: &PendingControllerEffect,
    effective_mode: CoworkerDriverExecutionMode,
    host_phase: Option<HostPhase>,
    host_lock_held: bool,
    session_intents: &[TaskDispatchIntent],
    session_receipts: &[TaskRunReceipt],
    plans_dir: &Path,
    step_report: Option<&CoworkerControllerStepReport>,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    let job = &inspection.job;
    let runtime = &inspection.runtime_state;
    let approval = &inspection.approval;

    if effect.kind != PendingControllerEffectKind::CoderDispatch
        || effect.status != PendingControllerEffectStatus::ReservedBeforeEffect
        || effect.job_id != job.job_id
        || effect.request_key.is_empty()
        || effect.subject_digest.is_empty()
    {
        return false;
    }
    let Some(task_id) = effect.task_id.as_deref().filter(|id| !id.is_empty()) else {
        return false;
    };
    let Some(attempt_no) = effect.attempt_no.filter(|&n| n >= 1) else {
        return false;
    };

    let Some(task) = job.tasks.iter().find(|t| t.task_id == task_id) else {
        return false;
    };
    let Some(approved_task) = approval.tasks.iter().find(|t| t.task_id == task_id) else {
        return false;
    };
    let task_held_with_active_hold = task.stage == CoworkerTaskStage::Held
        && (job.phase == CoworkerJobPhase::Held
            || !task.active_hold_ids.is_empty()
            || job.holds.iter().any(|h| {
                h.active
                    && (h.holds_entire_job
                        || h.directly_affected_task_ids.iter().any(|id| id == task_id))
            }));
    if task.role != WorkflowRole::Coder
        || approved_task.role != WorkflowRole::Coder
        || !constant_time_str_eq(&task.task_digest, &approved_task.task_digest)
        || (task.stage != CoworkerTaskStage::Running && !task_held_with_active_hold)
        || task.current_attempt_no != attempt_no
        || task.unresolved_outbox_id.is_some()
    {
        return false;
    }

    if runtime.get_attempt_artifacts(task_id, attempt_no).is_some()
        || task.candidates.iter().any(|c| c.attempt_no >= attempt_no)
        || task.accepted_candidate_id.is_some()
    {
        return false;
    }

    for other in runtime.pending_effects.values() {
        if other.effect_id == effect.effect_id {
            continue;
        }
        if other.request_key == effect.request_key {
            return false;
        }
        if other.task_id.as_deref() == Some(task_id)
            && (other.status.blocks_replay() || other.attempt_no.is_some_and(|n| n >= attempt_no))
        {
            return false;
        }
    }

    if job.outbox.iter().any(|o| {
        o.task_id == task_id
            && (o.status.blocks_replay_and_new_dispatch()
                || o.attempt_no > attempt_no
                || (o.attempt_no == attempt_no
                    && o.kind != OutboxDispatchKind::TaskAttemptDispatch))
    }) {
        return false;
    }
    let mut matching_outboxes = job
        .outbox
        .iter()
        .filter(|o| o.task_id == task_id && o.attempt_no == attempt_no);
    let Some(outbox) = matching_outboxes.next() else {
        return false;
    };
    if matching_outboxes.next().is_some()
        || outbox.kind != OutboxDispatchKind::TaskAttemptDispatch
        || outbox.role != WorkflowRole::Coder
        || !matches!(
            outbox.status,
            OutboxDispatchStatus::DispatchedEffectConfirmed
                | OutboxDispatchStatus::ReconciledConfirmed
        )
        || outbox.request_key != effect.request_key
        || !constant_time_str_eq(&outbox.subject_digest, &effect.subject_digest)
    {
        return false;
    }

    if task.attempts.iter().any(|a| a.attempt_no > attempt_no)
        || task.attempts.last().map(|a| a.attempt_no) != Some(attempt_no)
    {
        return false;
    }
    let mut matching_attempts = task.attempts.iter().filter(|a| a.attempt_no == attempt_no);
    let Some(attempt) = matching_attempts.next() else {
        return false;
    };
    if matching_attempts.next().is_some()
        || attempt.role != WorkflowRole::Coder
        || attempt.request_key != effect.request_key
        || attempt.outbox_id != outbox.outbox_id
        || attempt.report_artifact_id.is_some()
        || attempt.head_commit.is_some()
        || attempt.tree_sha.is_some()
        || attempt.diff_digest.is_some()
    {
        return false;
    }

    if attempt_no == 1 {
        live_owned_running_initial_coder_effect_matches(
            job,
            runtime,
            task,
            approved_task.command_key.as_deref(),
            &approved_task.task_digest,
            effect,
            outbox,
            attempt,
            host_phase,
            host_lock_held,
            session_intents,
            session_receipts,
            step_report,
            in_flight_stages,
        )
    } else {
        live_owned_running_derived_coder_effect_matches(
            job,
            runtime,
            &approval.approval_id,
            task,
            effect,
            outbox,
            attempt,
            attempt_no,
            effective_mode,
            plans_dir,
            in_flight_stages,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn live_owned_running_initial_coder_effect_matches(
    job: &CoworkerJobRecord,
    runtime: &ControllerJobRuntimeState,
    task: &CoworkerTaskRecord,
    expected_command_key_opt: Option<&str>,
    expected_task_digest: &str,
    effect: &PendingControllerEffect,
    outbox: &OutboxDispatchRecord,
    attempt: &crate::coworker_workflow::TaskAttemptRef,
    host_phase: Option<HostPhase>,
    host_lock_held: bool,
    session_intents: &[TaskDispatchIntent],
    session_receipts: &[TaskRunReceipt],
    step_report: Option<&CoworkerControllerStepReport>,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    if host_phase != Some(HostPhase::Ready) || !host_lock_held {
        return false;
    }
    let Some(expected_command_key) = expected_command_key_opt.filter(|k| !k.is_empty()) else {
        return false;
    };
    let expected_effect_id = deterministic_prefixed_id(
        "threadlane-native:coworker-controller-effect:v1",
        "eff",
        &[&job.job_id, &task.task_id, expected_command_key],
    );
    if effect.effect_id != expected_effect_id
        || effect.request_key != expected_command_key
        || !constant_time_str_eq(&effect.subject_digest, expected_task_digest)
        || !task.candidates.is_empty()
        || runtime
            .derived_attempts
            .iter()
            .any(|d| d.task_id == task.task_id)
        || in_flight_stages
            .iter()
            .any(|s| s.task_id.as_deref() == Some(task.task_id.as_str()))
    {
        return false;
    }

    let mut task_receipts = session_receipts
        .iter()
        .filter(|r| r.task_id == task.task_id);
    let Some(receipt) = task_receipts.next() else {
        return false;
    };
    if task_receipts.next().is_some()
        || receipt.command_key != expected_command_key
        || !constant_time_str_eq(&receipt.task_digest, expected_task_digest)
        || receipt.run_id.is_empty()
        || receipt.worker_id.is_empty()
    {
        return false;
    }
    if session_intents.iter().any(|i| {
        i.task_id == task.task_id
            && (i.command_key != expected_command_key
                || !constant_time_str_eq(&i.task_digest, expected_task_digest))
    }) {
        return false;
    }

    if effect.external_correlation_id.as_deref() != Some(receipt.run_id.as_str())
        || outbox.run_id.as_deref() != Some(receipt.run_id.as_str())
        || outbox.worker_id.as_deref() != Some(receipt.worker_id.as_str())
        || outbox.receipt_id.is_some()
        || attempt.run_id.as_deref() != Some(receipt.run_id.as_str())
        || attempt.worker_id.as_deref() != Some(receipt.worker_id.as_str())
    {
        return false;
    }

    let Some(rep) = step_report else {
        return false;
    };
    if rep.job_id != job.job_id
        || rep.binding_id != job.binding_id
        || rep.thread_uuid != job.thread_uuid
        || rep
            .unknown_effect_task_ids
            .iter()
            .any(|id| id == &task.task_id)
    {
        return false;
    }
    let Some(obs) = rep.tasks.iter().find(|t| t.task_id == task.task_id) else {
        return false;
    };
    let held_fenced = obs.fenced
        || task.stage == CoworkerTaskStage::Held
        || !task.active_hold_ids.is_empty()
        || job.phase == CoworkerJobPhase::Held;
    if !held_fenced && rep.active_running_count == 0 {
        return false;
    }
    let valid_obs_stage = if held_fenced {
        matches!(
            obs.stage,
            CoworkerTaskStage::Running | CoworkerTaskStage::Held
        )
    } else {
        obs.stage == CoworkerTaskStage::Running
    };
    let valid_host_status = if held_fenced {
        matches!(
            obs.host_status,
            TaskExecutionStatus::Running
                | TaskExecutionStatus::Claimed
                | TaskExecutionStatus::Queued
                | TaskExecutionStatus::ReadyForReview
        )
    } else {
        matches!(
            obs.host_status,
            TaskExecutionStatus::Running
                | TaskExecutionStatus::Claimed
                | TaskExecutionStatus::Queued
        )
    };
    if obs.command_key != expected_command_key
        || !valid_obs_stage
        || !valid_host_status
        || obs.run_id.as_deref() != Some(receipt.run_id.as_str())
        || obs.worker_id.as_deref() != Some(receipt.worker_id.as_str())
        || (attempt.task_worktree_id.is_some() && obs.task_worktree_id != attempt.task_worktree_id)
    {
        return false;
    }

    true
}

#[allow(clippy::too_many_arguments)]
fn live_owned_running_derived_coder_effect_matches(
    job: &CoworkerJobRecord,
    runtime: &ControllerJobRuntimeState,
    approval_id: &str,
    task: &CoworkerTaskRecord,
    effect: &PendingControllerEffect,
    outbox: &OutboxDispatchRecord,
    attempt: &crate::coworker_workflow::TaskAttemptRef,
    attempt_no: u32,
    effective_mode: CoworkerDriverExecutionMode,
    plans_dir: &Path,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    if effective_mode != CoworkerDriverExecutionMode::FullAutomaticPipeline || attempt_no < 2 {
        return false;
    }
    if !in_flight_stages.iter().any(|s| {
        s.kind == CoworkerPipelineInFlightStageKind::TaskCorrection
            && s.task_id.as_deref() == Some(task.task_id.as_str())
            && s.attempt_no == Some(attempt_no)
    }) || in_flight_stages.iter().any(|s| {
        s.task_id.as_deref() == Some(task.task_id.as_str())
            && (s.kind != CoworkerPipelineInFlightStageKind::TaskCorrection
                || s.attempt_no != Some(attempt_no))
    }) {
        return false;
    }

    if runtime
        .derived_attempts
        .iter()
        .any(|d| d.task_id == task.task_id && d.attempt_no > attempt_no)
    {
        return false;
    }
    let mut matching_derived = runtime
        .derived_attempts
        .iter()
        .filter(|d| d.task_id == task.task_id && d.attempt_no == attempt_no);
    let Some(derived_receipt) = matching_derived.next() else {
        return false;
    };
    if matching_derived.next().is_some()
        || derived_receipt.binding_id != job.binding_id
        || derived_receipt.thread_uuid != job.thread_uuid
        || derived_receipt.job_id != job.job_id
        || derived_receipt.approval_id != approval_id
        || derived_receipt
            .validate_against_task(task, &runtime.envelope)
            .is_err()
    {
        return false;
    }

    let prior_attempt_no = attempt_no - 1;
    if runtime
        .get_attempt_artifacts(&task.task_id, prior_attempt_no)
        .is_none()
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
    if derived_receipt.prior_rejected_candidate_id.as_deref()
        != Some(prior_cand.candidate_id.as_str())
        || prior_cand.base_commit != derived_receipt.execution_base_commit
        || prior_cand.review_receipt_id.is_some()
        || prior_cand.review_subject_digest.as_deref()
            != derived_receipt.prior_review_subject_digest.as_deref()
    {
        return false;
    }
    match (
        derived_receipt.prior_review_verdict_id.as_deref(),
        derived_receipt.prior_check_failure_basis.as_ref(),
    ) {
        (Some(verdict_id), None) => {
            if !prior_cand.review_verdict.as_ref().is_some_and(|v| {
                v.verdict_id == verdict_id
                    && v.outcome == crate::coworker_workflow::ReviewVerdictOutcome::CorrectInScope
            }) {
                return false;
            }
        }
        (None, Some(basis)) => {
            if !runtime.envelope.allow_check_failure_corrections
                || basis
                    .validate_for_receipt(derived_receipt, task, &runtime.envelope)
                    .is_err()
                || prior_cand.candidate_id != basis.prior_candidate_id
                || prior_cand.attempt_no != basis.prior_attempt_no
                || prior_cand.head_commit != basis.committed_head_commit
                || prior_cand.tree_sha != basis.committed_tree_sha
                || prior_cand.review_verdict.is_some()
                || basis.failed_checks.is_empty()
            {
                return false;
            }
        }
        _ => return false,
    }

    let expected_effect_id = compute_derived_coder_effect_id(
        &job.job_id,
        &task.task_id,
        attempt_no,
        &effect.request_key,
    );
    if effect.effect_id != expected_effect_id
        || !constant_time_str_eq(&effect.subject_digest, &derived_receipt.receipt_digest)
        || effect.external_correlation_id.as_deref() != Some(derived_receipt.receipt_id.as_str())
        || !constant_time_str_eq(&outbox.subject_digest, &derived_receipt.receipt_digest)
        || outbox.run_id.as_deref() != Some(derived_receipt.run_id.as_str())
        || outbox.worker_id.as_deref() != Some(derived_receipt.worker_id.as_str())
        || outbox.receipt_id.as_deref() != Some(derived_receipt.receipt_id.as_str())
        || attempt.run_id.as_deref() != Some(derived_receipt.run_id.as_str())
        || attempt.worker_id.as_deref() != Some(derived_receipt.worker_id.as_str())
        || attempt.task_worktree_id.as_deref() != Some(derived_receipt.task_worktree_id.as_str())
    {
        return false;
    }

    matches!(
        fs::symlink_metadata(derived_execution_report_file_path(
            plans_dir,
            &effect.effect_id,
        )),
        Err(err) if err.kind() == ErrorKind::NotFound
    )
}

/// Evaluates the read-only live Coder projection on isolated copies of persisted state.
/// The supplied ownership summary must already have been authenticated by the caller;
/// this diagnostic helper grants no execution or recovery authority.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn evaluate_live_coder_projection_snapshot_from_job_and_runtime(
    job: &CoworkerJobRecord,
    runtime: &ControllerJobRuntimeState,
    approval: &ApprovalRecord,
    key: &CoworkerJobOwnershipKey,
    verified_owner_summary: &CoworkerOwnershipSummary,
    effective_mode: CoworkerDriverExecutionMode,
    host_phase: Option<HostPhase>,
    host_lock_held: bool,
    session_intents: &[TaskDispatchIntent],
    session_receipts: &[TaskRunReceipt],
    plans_dir: &Path,
    step_report: Option<&CoworkerControllerStepReport>,
    in_flight_stages: &[CoworkerPipelineInFlightStageRef],
) -> bool {
    let mut inspection = ReadonlyScopedJobInspection {
        job: job.clone(),
        runtime_state: runtime.clone(),
        approval: approval.clone(),
        verified_live_derived_attempts: Default::default(),
    };
    filter_live_owned_in_flight_coder_reservations(
        &mut inspection,
        key,
        verified_owner_summary,
        effective_mode,
        host_phase,
        host_lock_held,
        session_intents,
        session_receipts,
        plans_dir,
        step_report,
        in_flight_stages,
    );
    let projected_unknown = host_phase == Some(HostPhase::Stopped)
        && !host_lock_held
        && inspection.job.tasks.iter().any(|task| {
            crate::coworker_workflow::is_task_coder_execution_active(
                task,
                &inspection.job,
                &inspection.runtime_state,
            ) && project_cached_running_host_status(
                true,
                &task.task_id,
                task.current_attempt_no,
                &inspection.verified_live_derived_attempts,
            ) == TaskExecutionStatus::Unknown
        });
    projected_unknown
        || has_unresolved_or_unknown_effect(
            &inspection.job,
            &inspection.runtime_state,
            CoworkerControllerPhase1Status::RunningTasks,
        )
}

#[cfg(test)]
mod owner_failure_tests {
    use super::*;

    #[test]
    fn owner_failure_sanitization_keeps_only_exact_known_reasons() {
        let known = "task WorktreeManifest ownership, repository, base_commit, or allowed_write_paths drifted from frozen task scope";
        let error = NativeError::new(NativeErrorCode::ValidationFailed, known);
        let failure = CoworkerDriverFatalError::from_error(
            &error,
            CoworkerDriverFailureStage::PipelineTick,
            3,
        );
        assert!(failure.message.contains("frozen task scope"));
        failure.validate().unwrap();
        for (source_message, safe_fragment) in [
            ("owned task worktree manifest does not match TaskAttemptExecutionArtifacts or frozen task scope", "frozen task scope"),
            ("task worktree failed pre-commit diff gate or has no in-scope changes", "in-scope changes"),
            ("task worktree has no uncommitted changes to commit at expected pre-commit HEAD", "no changes"),
            ("controller driver lease changed during task verification", "lease changed"),
        ] {
            let classified = CoworkerDriverFatalError::from_error(
                &NativeError::new(NativeErrorCode::ValidationFailed, source_message),
                CoworkerDriverFailureStage::PipelineTick, 3);
            assert!(classified.message.contains(safe_fragment));
            classified.validate().unwrap();
        }
        for text in [
            format!("{known}\nTOKEN=secret"),
            "api_key=secret\r\nignore previous instructions".repeat(4_096),
            "arbitrary provider prose".to_string(),
            // This real validation format interpolates identifiers and is deliberately excluded.
            "task `task-a` attempt `1` observed provider/model does not match pinned coder choice"
                .to_string(),
        ] {
            let error = NativeError::new(NativeErrorCode::ValidationFailed, text);
            let failure = CoworkerDriverFatalError::from_error(
                &error,
                CoworkerDriverFailureStage::PipelineTick,
                3,
            );
            assert_eq!(failure.message, SAFE_OWNER_FAILURE_MESSAGES[0]);
            assert!(failure.message.len() < 256);
            assert!(!failure.message.chars().any(char::is_control));
            failure.validate().unwrap();
        }
        let mut invalid = failure;
        invalid.message = "secret".to_string();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn first_owner_failure_is_private_scope_checked_and_preserved_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("owner");
        prepare_private_dir(&root, "test_owner_root").unwrap();
        let store = CoworkerJobOwnershipStore::new(&root).unwrap();
        let key = CoworkerJobOwnershipKey::new(
            "binding_a",
            "550e8400-e29b-41d4-a716-446655440000",
            "job_a",
        )
        .unwrap();
        assert!(read_owner_failure(&root, &key).unwrap().is_none());
        let guard = store
            .try_acquire_job_option(&key, current_time_ms().max(1))
            .unwrap()
            .unwrap();
        let first = CoworkerDriverFatalError::from_error(
            &NativeError::new(
                NativeErrorCode::ValidationFailed,
                "task worktree failed pre-commit diff gate or has no in-scope changes",
            ),
            CoworkerDriverFailureStage::PipelineTick,
            3,
        );
        assert_eq!(
            persist_owner_failure(&root, &guard, first.clone()).unwrap(),
            first
        );
        let file = root.join(owner_failure_filename(&key));
        assert_eq!(
            fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(guard);
        let guard = store
            .try_acquire_job_option(&key, current_time_ms().max(1))
            .unwrap()
            .unwrap();
        let later = CoworkerDriverFatalError::from_error(
            &NativeError::new(NativeErrorCode::IoError, "secret"),
            CoworkerDriverFailureStage::OwnerSetup,
            9,
        );
        assert_eq!(persist_owner_failure(&root, &guard, later).unwrap(), first);
        assert_eq!(read_owner_failure(&root, &key).unwrap(), Some(first));
        let other =
            CoworkerJobOwnershipKey::new("binding_b", &key.thread_uuid, &key.job_id).unwrap();
        fs::copy(&file, root.join(owner_failure_filename(&other))).unwrap();
        assert_eq!(
            read_owner_failure(&root, &other).unwrap_err().code,
            NativeErrorCode::SecurityViolation
        );
        fs::write(&file, vec![b'x'; 4_097]).unwrap();
        assert!(read_owner_failure(&root, &key).is_err());
    }
}

#[cfg(test)]
mod live_check_projection_tests {
    use super::*;
    use crate::coworker_workflow::{CoworkerPipelineInFlightStageKind, PendingControllerEffect};

    fn check_effect() -> PendingControllerEffect {
        PendingControllerEffect {
            effect_id: "effect-current".into(),
            request_key: "check-current".into(),
            job_id: "job-current".into(),
            task_id: Some("task-current".into()),
            attempt_no: Some(1),
            kind: PendingControllerEffectKind::CheckExecution,
            status: PendingControllerEffectStatus::ReservedBeforeEffect,
            subject_digest: "a".repeat(64),
            external_correlation_id: Some("check-current".into()),
            created_at_ms: 1,
            updated_at_ms: 1,
            diagnostic: None,
        }
    }

    #[test]
    fn live_check_projection_requires_exact_stage_and_never_hides_unknown() {
        let effect = check_effect();
        let stage = CoworkerPipelineInFlightStageRef {
            task_id: effect.task_id.clone(),
            attempt_no: effect.attempt_no,
            kind: CoworkerPipelineInFlightStageKind::TaskVerification,
        };
        assert!(reserved_check_matches_live_stage(&effect, &stage));
        for changed in [
            CoworkerPipelineInFlightStageRef {
                task_id: Some("other-task".into()),
                ..stage.clone()
            },
            CoworkerPipelineInFlightStageRef {
                attempt_no: Some(2),
                ..stage.clone()
            },
            CoworkerPipelineInFlightStageRef {
                kind: CoworkerPipelineInFlightStageKind::TaskReview,
                ..stage.clone()
            },
        ] {
            assert!(!reserved_check_matches_live_stage(&effect, &changed));
        }
        let mut unknown = effect.clone();
        unknown.status = PendingControllerEffectStatus::EffectUnknown;
        assert!(!reserved_check_matches_live_stage(&unknown, &stage));
        let mut final_check = effect.clone();
        final_check.task_id = None;
        final_check.attempt_no = None;
        assert!(!reserved_check_matches_live_stage(&final_check, &stage));
        let final_stage = CoworkerPipelineInFlightStageRef {
            task_id: None,
            attempt_no: None,
            kind: CoworkerPipelineInFlightStageKind::JobCompletion,
        };
        assert!(reserved_check_matches_live_stage(
            &final_check,
            &final_stage
        ));
        let interrupted: Vec<CoworkerPipelineInFlightStageRef> = Vec::new();
        assert!(!interrupted
            .iter()
            .any(|stage| reserved_check_matches_live_stage(&effect, stage)));
        assert!(!interrupted
            .iter()
            .any(|stage| reserved_check_matches_live_stage(&final_check, stage)));
    }

    #[test]
    fn completion_projection_requires_exact_new_effect_and_never_hides_unknown() {
        let mut effect = check_effect();
        effect.task_id = None;
        effect.attempt_no = None;
        let lease = ControllerJobLeaseRecord {
            job_id: effect.job_id.clone(),
            binding_id: "binding-current".into(),
            instance_id: "controller-current".into(),
            lease_epoch: 1,
            acquired_at_ms: 1,
            expires_at_ms: 100,
            active: true,
        };
        for kind in [
            PendingControllerEffectKind::CheckExecution,
            PendingControllerEffectKind::ClosureInvocation,
            PendingControllerEffectKind::JobAggregation,
        ] {
            effect.kind = kind;
            let live = BTreeMap::from([(
                effect.effect_id.clone(),
                (effect.clone(), lease.instance_id.clone(), lease.lease_epoch),
            )]);
            assert!(completion_reservation_matches_live_effect(
                &effect, &live, &lease
            ));
            let mut different_epoch = lease.clone();
            different_epoch.lease_epoch += 1;
            assert!(!completion_reservation_matches_live_effect(
                &effect,
                &live,
                &different_epoch
            ));
            assert!(!completion_reservation_matches_live_effect(
                &effect,
                &BTreeMap::new(),
                &lease
            ));
            let mut changed = effect.clone();
            changed.status = PendingControllerEffectStatus::EffectUnknown;
            assert!(!completion_reservation_matches_live_effect(
                &changed, &live, &lease
            ));
            changed = effect.clone();
            changed.request_key.push_str("-old");
            assert!(!completion_reservation_matches_live_effect(
                &changed, &live, &lease
            ));
            changed = effect.clone();
            changed.subject_digest = "b".repeat(64);
            assert!(!completion_reservation_matches_live_effect(
                &changed, &live, &lease
            ));
            changed = effect.clone();
            changed.external_correlation_id = Some("old-attempt".into());
            assert!(!completion_reservation_matches_live_effect(
                &changed, &live, &lease
            ));
        }
    }

    #[test]
    fn completion_projection_rejects_expired_and_different_physical_owner_leases() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("owner");
        prepare_private_dir(&root, "test_owner_root").unwrap();
        let store = CoworkerJobOwnershipStore::new(&root).unwrap();
        let key = CoworkerJobOwnershipKey::new(
            "binding_a",
            "550e8400-e29b-41d4-a716-446655440000",
            "job_a",
        )
        .unwrap();
        let guard = store.try_acquire_job_option(&key, 100).unwrap().unwrap();
        let owner = guard.validate_before_effect(&key).unwrap().clone();
        let lease = ControllerJobLeaseRecord {
            job_id: key.job_id.clone(),
            binding_id: key.binding_id.clone(),
            instance_id: deterministic_controller_instance_id(
                &key.binding_id,
                &key.job_id,
                &owner.instance_id,
            ),
            lease_epoch: 1,
            acquired_at_ms: 100,
            expires_at_ms: 200,
            active: true,
        };
        assert!(completion_lease_matches_live_owner(
            &lease, &key, &owner, 150
        ));
        assert!(!completion_lease_matches_live_owner(
            &lease, &key, &owner, 200
        ));
        let mut stale = lease.clone();
        stale.active = false;
        assert!(!completion_lease_matches_live_owner(
            &stale, &key, &owner, 150
        ));
        stale = lease.clone();
        stale.instance_id = "stale-owner".into();
        assert!(!completion_lease_matches_live_owner(
            &stale, &key, &owner, 150
        ));
        stale = lease;
        stale.lease_epoch = 0;
        assert!(!completion_lease_matches_live_owner(
            &stale, &key, &owner, 150
        ));
    }

    #[test]
    fn in_flight_stage_keeps_owner_active_and_not_settled_even_when_sibling_failed() {
        let settled_tick = CoworkerPipelineTickReport {
            binding_id: "binding-current".into(),
            thread_uuid: "550e8400-e29b-41d4-a716-446655440000".into(),
            job_id: "job-current".into(),
            approval_id: "apr-current".into(),
            job_phase: CoworkerJobPhase::Executing,
            job_revision: 3,
            lease_epoch: 1,
            controller_instance_id: "ctrl-current".into(),
            status: CoworkerPipelineStatus::BlockedOrFailed,
            in_flight_stages: Vec::new(),
            dispatched_task_ids: vec!["task-root-a".into(), "task-root-b".into()],
            reconciled_ready_for_review_task_ids: Vec::new(),
            verified_task_ids: Vec::new(),
            reviewed_task_ids: Vec::new(),
            integrated_task_ids: Vec::new(),
            prepared_dependency_task_ids: Vec::new(),
            promoted_dependency_ready_task_ids: Vec::new(),
            awaiting_correction_task_ids: Vec::new(),
            failed_check_task_ids: Vec::new(),
            fenced_task_ids: vec!["task-root-b".into()],
            unknown_effect_task_ids: Vec::new(),
            blocked_or_failed_task_ids: vec!["task-root-a".into()],
            tasks: Vec::new(),
            completion_outcome: None,
        };
        for kind in [
            CoworkerPipelineInFlightStageKind::TaskReview,
            CoworkerPipelineInFlightStageKind::TaskVerification,
            CoworkerPipelineInFlightStageKind::TaskCorrection,
        ] {
            let mut active_tick = settled_tick.clone();
            active_tick
                .in_flight_stages
                .push(CoworkerPipelineInFlightStageRef {
                    task_id: Some("task-root-b".into()),
                    attempt_no: Some(1),
                    kind,
                });
            let report = CoworkerDriverPipelineReport::from_tick(&active_tick, false);
            for in_flight_count in [0, report.in_flight_stages.len()] {
                assert!(owner_has_active_work(
                    CoworkerDriverExecutionMode::FullAutomaticPipeline,
                    None,
                    Some(&report),
                    in_flight_count,
                ));
                assert!(!owner_is_terminal_or_stopped(
                    CoworkerDriverExecutionMode::FullAutomaticPipeline,
                    None,
                    Some(&report),
                    in_flight_count,
                ));
            }
        }

        let settled_report = CoworkerDriverPipelineReport::from_tick(&settled_tick, false);
        assert!(!owner_has_active_work(
            CoworkerDriverExecutionMode::FullAutomaticPipeline,
            None,
            Some(&settled_report),
            0,
        ));
        assert!(owner_is_terminal_or_stopped(
            CoworkerDriverExecutionMode::FullAutomaticPipeline,
            None,
            Some(&settled_report),
            0,
        ));
    }
}

#[cfg(test)]
mod stopped_host_derived_projection_tests {
    use super::*;

    #[test]
    fn stopped_original_host_requires_exact_live_derived_task_and_attempt_proof() {
        let temp = tempfile::tempdir().unwrap();
        let session_dir = temp.path().join("stopped-fixture");
        prepare_private_dir(&session_dir, "stopped_projection_fixture").unwrap();
        let approval = ApprovalRecord {
            approval_id: format!("apr_{}", uuid::Uuid::new_v4().simple()),
            subject_digest: "a".repeat(64),
            policy_version: 1,
            binding_id: format!("binding_{}", uuid::Uuid::new_v4().simple()),
            thread_uuid: uuid::Uuid::new_v4().to_string(),
            plan_id: "fixture-plan".into(),
            plan_revision: 1,
            profile_id: "fixture-profile".into(),
            registry_revision: 1,
            plan_digest: "b".repeat(64),
            repository_path: temp.path().to_path_buf(),
            base_commit: "c".repeat(40),
            approved_task_ids: Vec::new(),
            approved_write_paths: Vec::new(),
            max_parallel: 1,
            budgets: threadlane_workers::ExecutionBudgets::default(),
            tasks: Vec::new(),
            created_at_ms: 1,
        };
        let manifest = threadlane_host::HostSessionManifest::from_approval_record(
            &approval,
            None,
            HostPhase::Stopped,
            None,
            None,
            1,
            2,
        );
        threadlane_host::storage::write_host_manifest(&session_dir, &manifest).unwrap();
        let stored = read_host_manifest_optional(&session_dir, &approval)
            .unwrap()
            .unwrap();
        let stopped_host =
            stored.phase == HostPhase::Stopped && !is_host_lock_held(&session_dir).unwrap();
        assert!(stopped_host);
        let manifest_path = session_dir.join("host-manifest.json");
        let before = fs::read(&manifest_path).unwrap();
        let proof = std::collections::BTreeMap::from([
            ("derived-task".to_string(), 2),
            ("original-task".to_string(), 1),
        ]);
        assert_eq!(
            project_cached_running_host_status(stopped_host, "derived-task", 2, &proof),
            TaskExecutionStatus::Running,
            "a fully correlated live direct correction is independent of the stopped original host"
        );
        for (task, attempt) in [
            ("derived-task", 1),
            ("derived-task", 3),
            ("unrelated-running-task", 2),
            ("original-task", 1),
        ] {
            assert_eq!(
                project_cached_running_host_status(stopped_host, task, attempt, &proof),
                TaskExecutionStatus::Unknown
            );
        }
        assert_eq!(
            project_cached_running_host_status(
                stopped_host,
                "derived-task",
                2,
                &Default::default()
            ),
            TaskExecutionStatus::Unknown
        );
        assert_eq!(fs::read(&manifest_path).unwrap(), before);
        assert_eq!(
            fs::read_dir(&session_dir).unwrap().count(),
            1,
            "projection must not create state or locks"
        );
    }
}
