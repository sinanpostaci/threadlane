//! Durable per-binding role-flow cycle store, native vs. headless role handoff controller,
//! explicit unknown-outcome reconciliation, and cross-cycle package closure reservation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_repository_and_base_commit,
};
use threadlane_authority::{ApprovalRecord, AuthorityError, AuthorityStore};
use threadlane_host::{ApprovedHostClient, HostError, TaskExecutionStatus, TaskStateSnapshot};
use threadlane_workers::{
    resolve_exact_tree_sha, validate_exact_40_hex_sha, validate_sha256_hex_digest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{Difficulty, RegistrySnapshot, ToolAdapter, WorkflowRole};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    read_private_file_bounded, sanitize_diagnostic_text, validate_absolute_no_symlink_components,
    validate_bounded_identifier, write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::profile_flow::{
    preflight, ExecutionPolicy, PreflightReport, PreflightRequest, RoleExecutionRoute,
    RolePreflightRequest,
};
use crate::{inspect_unresolved_blocking_changes, UnresolvedChangeBlockSummary};

/// Current wire and on-disk schema version for the durable role-flow ledger and cycles.
pub const ROLE_FLOW_SCHEMA_VERSION: u32 = 1;

/// Maximum number of frozen role-flow cycles retained per binding ledger.
pub const MAX_CYCLES_PER_BINDING: usize = 128;

/// Maximum number of role handoff records retained inside a single [`RoleFlowCycle`].
pub const MAX_HANDOFFS_PER_CYCLE: usize = 64;

/// Maximum number of package closure reservations retained across cycles in a binding ledger.
pub const MAX_CLOSURES_PER_BINDING: usize = 256;

/// Maximum byte size permitted for a per-binding role-flow ledger file (`1 MiB`).
pub const MAX_ROLE_FLOW_LEDGER_BYTES: usize = MAX_PRIVATE_STATE_FILE_BYTES;

const MAX_ID_LEN: usize = 128;

/// Immutable task selection frozen into a [`RoleFlowCycle`] from an [`ApprovalRecord`] and A1 preflight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCycleTask {
    /// Approved task identifier within the frozen plan revision.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Human-readable task title from the frozen approval.
    pub title: String,
    /// Full frozen task instruction from the approved plan revision.
    pub instruction: String,
    /// Workflow role assigned to the task.
    pub role: WorkflowRole,
    /// Difficulty tier assigned to the task in the plan.
    pub difficulty: Difficulty,
    /// Resolved registry tool identifier.
    pub tool_id: String,
    /// Resolved registry tool adapter.
    pub adapter: ToolAdapter,
    /// Execution route derived from `adapter` (`native_handoff` or `headless_worker`).
    pub route: RoleExecutionRoute,
    /// Resolved registry model identifier.
    pub model_id: String,
    /// Exact provider/CLI model argument frozen in both the registry and approval.
    pub model_arg: String,
    /// Resolved reasoning/effort level (`Some(...)` or `None`).
    pub effort: Option<String>,
    /// Upstream dependency task identifiers frozen in the approval record.
    pub depends_on: Vec<String>,
    /// Repository-relative write paths permitted for this task.
    pub write_paths: Vec<String>,
}

/// Durable lifecycle status of a [`RoleHandoffRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffStatus {
    /// Headless dispatch intent was persisted before calling `ApprovedHostClient::dispatch_task`.
    IntentRecorded,
    /// Native Codex conversational handoff was recorded (makes no model execution claim).
    NativeHandoffRecorded,
    /// Headless worker task was dispatched and sealed a `TaskRunReceipt` (`Queued`, `Claimed`, `Running`, or `CancelRequested`).
    Dispatched,
    /// Headless worker task completed and passed the post-run diff gate (`ReadyForReview`, `accepted: false`).
    ReadyForReview,
    /// Headless worker task failed execution, telemetry verification, or diff-gate checks.
    Failed,
    /// Headless worker task was proven stopped after cancellation.
    Cancelled,
    /// Headless worker task exceeded its execution timeout budget.
    TimedOut,
    /// Dispatch outcome is uncertain after persisting intent; cannot be blindly replayed and requires explicit reconciliation.
    OutcomeUnknown,
}

impl HandoffStatus {
    /// Returns the canonical `snake_case` wire identifier for this handoff status.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntentRecorded => "intent_recorded",
            Self::NativeHandoffRecorded => "native_handoff_recorded",
            Self::Dispatched => "dispatched",
            Self::ReadyForReview => "ready_for_review",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    /// Returns `true` if the handoff is in an uncertain state (`IntentRecorded` or `OutcomeUnknown`) that forbids blind replay.
    pub const fn is_ambiguous(self) -> bool {
        matches!(self, Self::IntentRecorded | Self::OutcomeUnknown)
    }
}

impl fmt::Display for HandoffStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Explicit native Codex conversational handoff payload for a `ToolAdapter::CodexCli` task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRoleHandoff {
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Frozen approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Human-readable task title.
    pub title: String,
    /// Full frozen task instruction from the approved plan revision.
    pub frozen_instruction: String,
    /// Requested registry model identifier.
    pub requested_model_id: String,
    /// Exact requested model argument from the registry definition.
    pub requested_model_arg: String,
    /// Optional requested reasoning effort from the profile assignment.
    pub requested_effort: Option<String>,
    /// Always `false`: native handoff records the requested model and instruction without proving native model execution.
    pub model_execution_verified: bool,
    /// Always `false`: local context/handoff records do not prove per-call native conversation identity.
    pub native_per_call_identity_verified: bool,
}

/// Complete run, worktree, and report artifact reference for a dispatched headless role handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleEvidenceReference {
    /// Frozen operator approval identifier owning the session.
    pub approval_id: String,
    /// Approved task identifier.
    pub task_id: String,
    /// Sealed runtime `run_id` (`run_<uuid>`), if a task run receipt exists.
    pub run_id: Option<String>,
    /// Sealed runtime `worker_id` (`wrk_<uuid>`), if a task run receipt exists.
    pub worker_id: Option<String>,
    /// Private local report artifact identifier from `WorkerOutcomeSummary::report_artifact_id`, if present.
    pub report_artifact_id: Option<String>,
    /// Actual per-task worktree identifier from `WorkerSnapshot::worktree::task_worktree_id`, if reported.
    pub task_worktree_id: Option<String>,
    /// Frozen repository base commit SHA for this task run.
    pub base_commit: String,
    /// Head commit SHA in the task worktree after execution, if available.
    pub head_commit: Option<String>,
    /// Model identifier observed in verified worker stream telemetry, if reported.
    pub observed_model: Option<String>,
    /// Reasoning effort observed in verified worker stream telemetry, if reported.
    pub observed_effort: Option<String>,
    /// Bounded, sanitized preview from `WorkerOutcomeSummary::final_summary` (never complete review evidence).
    pub final_summary_preview: Option<String>,
    /// Always `false`: `final_summary_preview` is bounded/truncated and is never complete review evidence.
    pub final_summary_is_complete_evidence: bool,
}

/// Durable record of a role handoff within a [`RoleFlowCycle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleHandoffRecord {
    /// Deterministic handoff identifier (`hnd_<32hex>`).
    pub handoff_id: String,
    /// Idempotent caller request key bound to this handoff.
    pub request_key: String,
    /// Owning role-flow cycle identifier (`flw_<32hex>`).
    pub cycle_id: String,
    /// Frozen operator approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Frozen approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Workflow role assigned to the task.
    pub role: WorkflowRole,
    /// Difficulty tier assigned to the task.
    pub difficulty: Difficulty,
    /// Frozen registry tool identifier.
    pub tool_id: String,
    /// Frozen registry tool adapter.
    pub adapter: ToolAdapter,
    /// Derived execution route (`native_handoff` or `headless_worker`).
    pub route: RoleExecutionRoute,
    /// Frozen registry model identifier.
    pub model_id: String,
    /// Exact frozen model argument string.
    pub model_arg: String,
    /// Frozen reasoning effort (`Some(...)` or `None`).
    pub effort: Option<String>,
    /// Frozen execution policy identifier.
    pub policy_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen [`ExecutionPolicy`].
    pub policy_digest: String,
    /// Bound package closure reservation identifier (`cls_<32hex>`) required when `role == WorkflowRole::Closure`.
    #[serde(default)]
    pub closure_reservation_id: Option<String>,
    /// Current durable handoff lifecycle status.
    pub status: HandoffStatus,
    /// 1-based cycle revision when this handoff record was first persisted.
    pub created_at_cycle_revision: u64,
    /// 1-based cycle revision when this handoff record was most recently updated.
    pub updated_at_cycle_revision: u64,
    /// Sealed runtime `run_id` (`run_<uuid>`) when a worker receipt has been observed.
    pub run_id: Option<String>,
    /// Sealed runtime `worker_id` (`wrk_<uuid>`) when a worker receipt has been observed.
    pub worker_id: Option<String>,
    /// Most recently observed [`TaskExecutionStatus`] from [`ApprovedHostClient`], if any.
    pub observed_task_status: Option<TaskExecutionStatus>,
    /// Explicit native handoff payload when `route == RoleExecutionRoute::NativeHandoff`.
    pub native_handoff: Option<NativeRoleHandoff>,
    /// Full run/worktree/artifact evidence reference when `route == RoleExecutionRoute::HeadlessWorker`.
    pub evidence_ref: Option<RoleEvidenceReference>,
    /// Most recently observed [`TaskStateSnapshot`] from [`ApprovedHostClient`], if any.
    pub task_snapshot: Option<TaskStateSnapshot>,
    /// Sanitized diagnostic message when `status` is [`HandoffStatus::OutcomeUnknown`] or failed.
    pub last_error: Option<String>,
}

/// Durable role-flow execution cycle bound to a single frozen [`ApprovalRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleFlowCycle {
    /// Deterministic cycle identifier (`flw_<32hex>`).
    pub cycle_id: String,
    /// Monotonically increasing 1-based revision of this cycle.
    pub cycle_revision: u64,
    /// Idempotent request key that created this cycle.
    pub create_request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Frozen operator approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 subject digest from [`ApprovalRecord::subject_digest`].
    pub approval_subject_digest: String,
    /// Frozen plan identifier.
    pub plan_id: String,
    /// Frozen published plan revision number.
    pub plan_revision: u64,
    /// 64-character lowercase hex SHA-256 digest of the published plan revision.
    pub plan_digest: String,
    /// Canonical absolute repository path frozen by the approval.
    pub repository_path: String,
    /// Full 40-character lowercase hex base commit SHA frozen by the approval.
    pub base_commit: String,
    /// Full 40-character lowercase hex Git tree SHA resolved from `base_commit`.
    pub base_tree_sha: String,
    /// Frozen profile identifier selected for this approval.
    pub profile_id: String,
    /// Frozen registry revision when the approval and cycle were validated.
    pub registry_revision: u64,
    /// Frozen immutable execution policy snapshot.
    pub policy: ExecutionPolicy,
    /// 64-character lowercase hex SHA-256 digest of `policy`.
    pub policy_digest: String,
    /// Selected subset of approved tasks frozen into this cycle.
    pub tasks: Vec<FrozenCycleTask>,
    /// Recorded role handoffs within this cycle, ordered by creation.
    pub handoffs: Vec<RoleHandoffRecord>,
}

/// Verified source from which a package closure reservation derives its candidate tree SHA.
///
/// Only the cycle's frozen approved base commit is accepted as a closure candidate source; its tree
/// SHA is derived directly from the repository via sanitized read-only Git (`git rev-parse <base_commit>^{tree}`)
/// and `subject_digest` must equal the cycle's software-validated `approval_subject_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClosureSubjectSource {
    /// Derive the candidate tree directly from the cycle's frozen `repository_path` and `base_commit`
    /// via read-only Git resolution (`git rev-parse <base_commit>^{tree}`).
    ApprovedCycleBase {
        /// Concrete 64-character lowercase hex SHA-256 digest of the closure review subject (must equal the cycle's `approval_subject_digest`).
        subject_digest: String,
        /// Optional expected 40-character lowercase hex tree SHA; if supplied, must match the resolved tree.
        expected_candidate_tree_sha: Option<String>,
    },
}

/// Cross-cycle single-closure reservation for a `(repository_path, package_id, candidate_tree_sha)` tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageClosureReservation {
    /// Deterministic closure reservation identifier (`cls_<32hex>`).
    pub reservation_id: String,
    /// Idempotent request key that first created this reservation.
    pub request_key: String,
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Role-flow cycle identifier (`flw_<32hex>`) that reserved this closure.
    pub cycle_id: String,
    /// Frozen operator approval identifier (`apr_<32hex>`) of the reserving cycle.
    pub approval_id: String,
    /// Canonical absolute repository path.
    pub repository_path: String,
    /// Package or milestone closure identifier within the repository.
    pub package_id: String,
    /// Frozen base commit SHA of the cycle that reserved the closure.
    pub base_commit: String,
    /// Full 40-character lowercase hex Git tree SHA of the candidate tree being closed.
    pub candidate_tree_sha: String,
    /// Concrete 64-character lowercase hex SHA-256 digest of the closure review subject.
    pub subject_digest: String,
    /// Verified source used to derive `candidate_tree_sha`.
    pub source: ClosureSubjectSource,
    /// Optional closure reviewer task identifier within the cycle, if bound.
    pub task_id: Option<String>,
    /// Ledger revision when this closure reservation was recorded.
    pub reserved_at_ledger_revision: u64,
}

/// Complete view of role-flow cycles and closure reservations for a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleFlowBindingSnapshot {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Current monotonic revision of the binding's role-flow ledger (`0` if uninitialized).
    pub ledger_revision: u64,
    /// Cycles recorded for this binding, ordered by creation.
    pub cycles: Vec<RoleFlowCycle>,
    /// Cross-cycle package closure reservations recorded for this binding.
    pub closures: Vec<PackageClosureReservation>,
}

/// Request to create a new [`RoleFlowCycle`] from an existing frozen [`ApprovalRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRoleFlowCycleRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Caller-supplied idempotency key for cycle creation.
    pub request_key: String,
    /// Expected current ledger revision (`None` to skip ledger-level CAS check).
    pub expected_ledger_revision: Option<u64>,
    /// Existing operator approval identifier (`apr_<32hex>`) in [`AuthorityStore`].
    pub approval_id: String,
    /// Explicit non-empty list of approved task identifiers to freeze into this cycle.
    pub task_ids: Vec<String>,
    /// Immutable execution policy to freeze into this cycle.
    pub policy: ExecutionPolicy,
}

/// Request to execute or record a role handoff for a single frozen task in a [`RoleFlowCycle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleHandoffRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target role-flow cycle identifier (`flw_<32hex>`).
    pub cycle_id: String,
    /// Expected current `cycle_revision` of the target [`RoleFlowCycle`] (required CAS check).
    pub expected_cycle_revision: u64,
    /// Caller-supplied idempotency key for this handoff.
    pub request_key: String,
    /// Target approved task identifier within the cycle.
    pub task_id: String,
    /// Current [`ExecutionPolicy`] at dispatch time (must match the cycle's frozen policy & digest).
    pub current_policy: ExecutionPolicy,
    /// Explicit operator confirmation flag required before any headless worker effect.
    pub operator_approved: bool,
    /// Exact `approval_subject_digest` confirmation required before any headless worker effect.
    pub confirm_approval_digest: Option<String>,
    /// Required package closure reservation identifier (`cls_<32hex>`) when dispatching a `WorkflowRole::Closure` task.
    #[serde(default)]
    pub closure_reservation_id: Option<String>,
}

/// Request to reconcile a previously recorded [`RoleHandoffRecord`] against [`ApprovedHostClient::get_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileRoleHandoffRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target role-flow cycle identifier (`flw_<32hex>`).
    pub cycle_id: String,
    /// Expected current `cycle_revision` of the target [`RoleFlowCycle`] (required CAS check).
    pub expected_cycle_revision: u64,
    /// Target handoff identifier (`hnd_<32hex>`) within the cycle.
    pub handoff_id: String,
}

/// Request to reserve single-closure authority for `(repository_path, package_id, candidate_tree_sha)` across cycles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservePackageClosureRequest {
    /// Control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Target role-flow cycle identifier (`flw_<32hex>`).
    pub cycle_id: String,
    /// Expected current `cycle_revision` of the target [`RoleFlowCycle`] (required CAS check).
    pub expected_cycle_revision: u64,
    /// Caller-supplied idempotency key for this reservation.
    pub request_key: String,
    /// Package or milestone closure identifier within the repository.
    pub package_id: String,
    /// Verified source from which the concrete `candidate_tree_sha` and `subject_digest` are derived.
    pub source: ClosureSubjectSource,
    /// Optional closure reviewer task identifier within the cycle.
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedRoleFlowLedger {
    schema_version: u32,
    binding_id: String,
    ledger_revision: u64,
    cycles: Vec<RoleFlowCycle>,
    closures: Vec<PackageClosureReservation>,
}

/// Reads a [`RegistrySnapshot`] from an existing private (`0700`) registry directory without
/// creating the directory or `registry.lock`.
pub fn read_registry_snapshot_readonly(
    registry_dir: impl AsRef<Path>,
) -> Result<RegistrySnapshot, NativeError> {
    threadlane_workflow::RegistryStore::read_snapshot_readonly_at(registry_dir).map_err(|err| {
        let code = match err {
            threadlane_workflow::RegistryError::SecurityViolation(_) => {
                NativeErrorCode::SecurityViolation
            }
            threadlane_workflow::RegistryError::Io(_) => NativeErrorCode::IoError,
            _ => NativeErrorCode::ProtocolError,
        };
        NativeError::new(
            code,
            "Registry snapshot failed canonical read-only validation.",
        )
    })
}

/// Evaluates [`preflight`] against an existing private (`0700`) registry directory without
/// creating `registry.lock` or any directories.
pub fn preflight_from_dir_readonly(
    registry_dir: impl AsRef<Path>,
    request: &PreflightRequest,
    policy: &ExecutionPolicy,
) -> Result<PreflightReport, NativeError> {
    let snapshot = read_registry_snapshot_readonly(registry_dir)?;
    preflight(&snapshot, request, policy)
}

/// Durable per-binding role-flow ledger store backed by private (`0600`) files inside an
/// existing private (`0700`) `plans_dir`.
#[derive(Debug, Clone)]
pub struct RoleFlowStore {
    plans_dir: PathBuf,
}

impl RoleFlowStore {
    /// Opens a [`RoleFlowStore`] handle over an existing private (`0700`) non-symlink `plans_dir`
    /// without creating any directory, state file, or lockfile.
    pub fn new(plans_dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        ensure_platform_supported()?;
        let clean = validate_existing_private_store_dir(plans_dir.as_ref(), "plans_dir")?;
        Ok(Self { plans_dir: clean })
    }

    /// Returns the canonical validated `plans_dir` path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }

    /// Returns the deterministic private ledger file path (`role-flow-<binding_tag>.json`) for `binding_id`.
    pub fn ledger_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_ID_LEN)?;
        Ok(self.plans_dir.join(ledger_filename_for_binding(binding_id)))
    }

    /// Returns the deterministic cross-process lockfile path (`role-flow-<binding_tag>.lock`) for `binding_id`.
    pub fn lock_path_for_binding(&self, binding_id: &str) -> Result<PathBuf, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_ID_LEN)?;
        Ok(self.plans_dir.join(lock_filename_for_binding(binding_id)))
    }

    /// Explicitly initializes the durable role-flow ledger for `binding_id` on disk if it does
    /// not yet exist, returning the current [`RoleFlowBindingSnapshot`].
    pub fn initialize_binding(
        &self,
        binding_id: &str,
    ) -> Result<RoleFlowBindingSnapshot, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(binding_id);
        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_filename, "role_flow_ledger_lock")?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        let ledger = match self.read_ledger_unlocked(binding_id, &ledger_path)? {
            Some(existing) => existing,
            None => {
                let fresh = PersistedRoleFlowLedger {
                    schema_version: ROLE_FLOW_SCHEMA_VERSION,
                    binding_id: binding_id.to_string(),
                    ledger_revision: 0,
                    cycles: Vec::new(),
                    closures: Vec::new(),
                };
                self.write_ledger_unlocked(binding_id, &fresh)?;
                fresh
            }
        };
        Ok(RoleFlowBindingSnapshot {
            binding_id: ledger.binding_id,
            ledger_revision: ledger.ledger_revision,
            cycles: ledger.cycles,
            closures: ledger.closures,
        })
    }

    /// Pure read-only snapshot of `binding_id`'s role-flow ledger.
    ///
    /// Never creates a directory, state file, or lockfile. Returns an empty revision-0 snapshot
    /// when no ledger file exists yet for `binding_id`.
    pub fn get_binding_snapshot(
        &self,
        binding_id: &str,
    ) -> Result<RoleFlowBindingSnapshot, NativeError> {
        validate_bounded_identifier(binding_id, "binding_id", MAX_ID_LEN)?;
        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        match self.read_ledger_unlocked(binding_id, &ledger_path)? {
            Some(ledger) => Ok(RoleFlowBindingSnapshot {
                binding_id: ledger.binding_id,
                ledger_revision: ledger.ledger_revision,
                cycles: ledger.cycles,
                closures: ledger.closures,
            }),
            None => Ok(RoleFlowBindingSnapshot {
                binding_id: binding_id.to_string(),
                ledger_revision: 0,
                cycles: Vec::new(),
                closures: Vec::new(),
            }),
        }
    }

    /// Pure read-only list of all [`RoleFlowCycle`] records for `binding_id` without creating
    /// any directory, state file, or lockfile.
    pub fn list_cycles(&self, binding_id: &str) -> Result<Vec<RoleFlowCycle>, NativeError> {
        Ok(self.get_binding_snapshot(binding_id)?.cycles)
    }

    /// Pure read-only lookup of a single [`RoleFlowCycle`] by `(binding_id, cycle_id)` without
    /// creating any directory, state file, or lockfile.
    pub fn get_cycle(
        &self,
        binding_id: &str,
        cycle_id: &str,
    ) -> Result<Option<RoleFlowCycle>, NativeError> {
        validate_bounded_identifier(cycle_id, "cycle_id", MAX_ID_LEN)?;
        let snapshot = self.get_binding_snapshot(binding_id)?;
        Ok(snapshot
            .cycles
            .into_iter()
            .find(|cycle| cycle.cycle_id == cycle_id))
    }

    /// Pure read-only list of all cross-cycle [`PackageClosureReservation`] records for `binding_id`
    /// without creating any directory, state file, or lockfile.
    pub fn list_closure_reservations(
        &self,
        binding_id: &str,
    ) -> Result<Vec<PackageClosureReservation>, NativeError> {
        Ok(self.get_binding_snapshot(binding_id)?.closures)
    }

    /// Pure read-only lookup of a [`PackageClosureReservation`] by `(binding_id, repository_path, package_id, candidate_tree_sha)`
    /// without creating any directory, state file, or lockfile.
    pub fn get_closure_reservation(
        &self,
        binding_id: &str,
        repository_path: &str,
        package_id: &str,
        candidate_tree_sha: &str,
    ) -> Result<Option<PackageClosureReservation>, NativeError> {
        validate_bounded_identifier(package_id, "package_id", MAX_ID_LEN)?;
        validate_exact_40_hex_sha(candidate_tree_sha, "candidate_tree_sha").map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "candidate_tree_sha must be a 40-character lowercase hex SHA-1.",
            )
        })?;
        let snapshot = self.get_binding_snapshot(binding_id)?;
        Ok(snapshot.closures.into_iter().find(|res| {
            res.repository_path == repository_path
                && res.package_id == package_id
                && res.candidate_tree_sha == candidate_tree_sha
        }))
    }

    /// Creates and persists a new [`RoleFlowCycle`] bound to an existing [`ApprovalRecord`] after
    /// verifying scope, active plan status, repository/base commit & tree, registry revision,
    /// conversation-change gates, and exact A1 [`preflight`] equality for all requested tasks.
    pub fn create_cycle(
        &self,
        request: &CreateRoleFlowCycleRequest,
        authority: &AuthorityStore,
    ) -> Result<RoleFlowCycle, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.approval_id, "approval_id", MAX_ID_LEN)?;
        request.policy.validate()?;
        let policy_digest = request.policy.digest()?;

        if request.task_ids.is_empty() || request.task_ids.len() > MAX_HANDOFFS_PER_CYCLE {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "CreateRoleFlowCycleRequest.task_ids must contain between 1 and 64 task IDs.",
            ));
        }
        let mut seen_tasks = BTreeSet::new();
        for task_id in &request.task_ids {
            validate_bounded_identifier(task_id, "task_id", MAX_ID_LEN)?;
            if !seen_tasks.insert(task_id.as_str()) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "CreateRoleFlowCycleRequest.task_ids contains duplicate task IDs.",
                ));
            }
        }

        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(&request.binding_id);
        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_filename, "role_flow_ledger_lock")?;
        let ledger_path = self.ledger_path_for_binding(&request.binding_id)?;
        let mut ledger = self
            .read_ledger_unlocked(&request.binding_id, &ledger_path)?
            .unwrap_or_else(|| PersistedRoleFlowLedger {
                schema_version: ROLE_FLOW_SCHEMA_VERSION,
                binding_id: request.binding_id.clone(),
                ledger_revision: 0,
                cycles: Vec::new(),
                closures: Vec::new(),
            });

        if let Some(existing) = ledger
            .cycles
            .iter()
            .find(|c| c.create_request_key == request.request_key)
        {
            let existing_task_ids: Vec<String> =
                existing.tasks.iter().map(|t| t.task_id.clone()).collect();
            if existing.binding_id == request.binding_id
                && existing.thread_uuid == request.thread_uuid
                && existing.approval_id == request.approval_id
                && existing.policy_digest == policy_digest
                && existing.policy == request.policy
                && existing_task_ids == request.task_ids
            {
                return Ok(existing.clone());
            }
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "request_key was already used to create a cycle with different parameters.",
            ));
        }

        if let Some(expected_rev) = request.expected_ledger_revision {
            if expected_rev != ledger.ledger_revision {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "expected_ledger_revision does not match current role-flow ledger revision.",
                ));
            }
        }
        if ledger.cycles.len() >= MAX_CYCLES_PER_BINDING {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Role-flow ledger reached maximum cycle capacity for this binding.",
            ));
        }

        let approval = authority
            .get_for_operator(&request.approval_id)
            .map_err(map_authority_error)?;
        verify_approval_scope_and_plan(
            &approval,
            &request.binding_id,
            &request.thread_uuid,
            authority,
        )?;

        let blocker = inspect_unresolved_blocking_changes(
            &self.plans_dir,
            &request.binding_id,
            Some(&request.approval_id),
            None,
        )?;
        if blocker.blocked {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Approval cannot start a cycle while blocked by an unresolved change or superseded replan.",
            ));
        }

        let (canonical_repo, normalized_base) =
            validate_repository_and_base_commit(&approval.repository_path, &approval.base_commit)
                .map_err(map_authority_error)?;
        if canonical_repo != approval.repository_path || normalized_base != approval.base_commit {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Frozen approval repository_path or base_commit does not match canonical repository state.",
            ));
        }
        let base_tree_sha = resolve_exact_tree_sha(
            &canonical_repo,
            &normalized_base,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Failed to resolve Git tree SHA for the frozen approval base_commit.",
            )
        })?;

        let registry_snapshot = authority.registry().snapshot().map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read workflow registry snapshot.",
            )
        })?;
        if registry_snapshot.revision != approval.registry_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Current registry revision drifted from the frozen approval registry_revision.",
            ));
        }

        let frozen_tasks = verify_and_freeze_cycle_tasks(
            &approval,
            &request.task_ids,
            &registry_snapshot,
            &request.policy,
        )?;

        let cycle_id = deterministic_cycle_id(
            &request.binding_id,
            &request.approval_id,
            &request.request_key,
            &policy_digest,
            &request.task_ids,
        );
        let next_ledger_rev = ledger.ledger_revision.checked_add(1).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Role-flow ledger revision overflow.",
            )
        })?;

        let cycle = RoleFlowCycle {
            cycle_id,
            cycle_revision: 1,
            create_request_key: request.request_key.clone(),
            binding_id: request.binding_id.clone(),
            thread_uuid: request.thread_uuid.clone(),
            approval_id: approval.approval_id.clone(),
            approval_subject_digest: approval.subject_digest.clone(),
            plan_id: approval.plan_id.clone(),
            plan_revision: approval.plan_revision,
            plan_digest: approval.plan_digest.clone(),
            repository_path: canonical_repo.to_string_lossy().into_owned(),
            base_commit: normalized_base,
            base_tree_sha,
            profile_id: approval.profile_id.clone(),
            registry_revision: approval.registry_revision,
            policy: request.policy.clone(),
            policy_digest,
            tasks: frozen_tasks,
            handoffs: Vec::new(),
        };

        ledger.ledger_revision = next_ledger_rev;
        ledger.cycles.push(cycle.clone());
        self.write_ledger_unlocked(&request.binding_id, &ledger)?;
        Ok(cycle)
    }

    /// Executes or records a role handoff for a frozen task in `cycle_id`.
    ///
    /// Rechecks the current policy digest, frozen/current registry revision and A1 preflight choice,
    /// active published plan revision, closure reservation binding (for `WorkflowRole::Closure`),
    /// and `inspect_unresolved_blocking_changes` before effect. Persists [`HandoffStatus::IntentRecorded`]
    /// before any external [`ApprovedHostClient::dispatch_task`] call, returns recorded state on
    /// duplicates without repeating the provider call, and marks transport/host errors as
    /// [`HandoffStatus::OutcomeUnknown`] (which cannot be replayed).
    pub async fn handoff_role(
        &self,
        request: &RoleHandoffRequest,
        authority: &AuthorityStore,
        host_client: Option<&ApprovedHostClient>,
    ) -> Result<RoleHandoffRecord, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;
        validate_bounded_identifier(&request.cycle_id, "cycle_id", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.task_id, "task_id", MAX_ID_LEN)?;
        if let Some(ref res_id) = request.closure_reservation_id {
            validate_bounded_identifier(res_id, "closure_reservation_id", MAX_ID_LEN)?;
        }
        if request.expected_cycle_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_cycle_revision must be >= 1.",
            ));
        }
        request.current_policy.validate()?;
        let current_policy_digest = request.current_policy.digest()?;

        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(&request.binding_id);
        let ledger_path = self.ledger_path_for_binding(&request.binding_id)?;

        let (cycle_snapshot, handoff_id, is_native_completed) = {
            let _guard = acquire_cross_process_lock(
                &self.plans_dir,
                &lock_filename,
                "role_flow_ledger_lock",
            )?;
            let mut ledger = self
                .read_ledger_unlocked(&request.binding_id, &ledger_path)?
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Role-flow ledger is not initialized for this binding.",
                    )
                })?;

            let cycle_idx = ledger
                .cycles
                .iter()
                .position(|c| c.cycle_id == request.cycle_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Requested role-flow cycle_id was not found in this binding.",
                    )
                })?;

            if ledger.cycles[cycle_idx].binding_id != request.binding_id
                || ledger.cycles[cycle_idx].thread_uuid != request.thread_uuid
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Role-flow cycle does not belong to the requested binding and thread scope.",
                ));
            }
            if current_policy_digest != ledger.cycles[cycle_idx].policy_digest
                || request.current_policy != ledger.cycles[cycle_idx].policy
            {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Current execution policy or digest does not match the cycle's frozen policy.",
                ));
            }

            if let Some(existing) = ledger.cycles[cycle_idx]
                .handoffs
                .iter()
                .find(|h| h.request_key == request.request_key)
                .cloned()
            {
                verify_existing_handoff_match(
                    &existing,
                    request,
                    &ledger.cycles[cycle_idx],
                    &current_policy_digest,
                )?;
                if existing.status.is_ambiguous() {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "Handoff outcome is unknown and cannot be replayed; reconcile first.",
                    ));
                }
                return Ok(existing);
            }

            if let Some(existing_task) = ledger.cycles[cycle_idx]
                .handoffs
                .iter()
                .find(|h| h.task_id == request.task_id)
                .cloned()
            {
                verify_existing_handoff_match(
                    &existing_task,
                    request,
                    &ledger.cycles[cycle_idx],
                    &current_policy_digest,
                )?;
                if existing_task.status.is_ambiguous() {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "Task handoff has an unknown outcome pending reconciliation; replay is forbidden.",
                    ));
                }
                return Ok(existing_task);
            }

            let cycle = &ledger.cycles[cycle_idx];
            if request.expected_cycle_revision != cycle.cycle_revision {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "expected_cycle_revision does not match current role-flow cycle_revision.",
                ));
            }
            if cycle.handoffs.len() >= MAX_HANDOFFS_PER_CYCLE {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Role-flow cycle reached maximum handoff capacity.",
                ));
            }

            let frozen_task = cycle
                .tasks
                .iter()
                .find(|t| t.task_id == request.task_id)
                .cloned()
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Requested task_id is not frozen in this role-flow cycle.",
                    )
                })?;

            recheck_live_dispatch_gates(
                &self.plans_dir,
                cycle,
                &frozen_task,
                &request.current_policy,
                authority,
            )?;

            let verified_closure_id = verify_closure_handoff_reservation(
                request,
                cycle,
                &frozen_task,
                &ledger.cycles,
                &ledger.closures,
            )?;

            let handoff_id =
                deterministic_handoff_id(&cycle.cycle_id, &request.task_id, &request.request_key);
            let next_cycle_rev = cycle.cycle_revision.checked_add(1).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Cycle revision overflow.",
                )
            })?;
            let next_ledger_rev = ledger.ledger_revision.checked_add(1).ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Ledger revision overflow.",
                )
            })?;

            match frozen_task.route {
                RoleExecutionRoute::Unsupported => {
                    return Err(NativeError::new(
                        NativeErrorCode::UnsupportedCapability,
                        "Frozen task route cannot execute or produce a native handoff.",
                    ));
                }
                RoleExecutionRoute::NativeHandoff => {
                    let record = build_native_handoff_record(
                        &handoff_id,
                        &request.request_key,
                        cycle,
                        &frozen_task,
                        verified_closure_id,
                        next_cycle_rev,
                    );
                    let cycle_mut = &mut ledger.cycles[cycle_idx];
                    cycle_mut.cycle_revision = next_cycle_rev;
                    cycle_mut.handoffs.push(record.clone());
                    let cycle_snapshot = cycle_mut.clone();
                    ledger.ledger_revision = next_ledger_rev;
                    self.write_ledger_unlocked(&request.binding_id, &ledger)?;
                    (cycle_snapshot, handoff_id, Some(record))
                }
                RoleExecutionRoute::HeadlessWorker => {
                    verify_headless_dispatch_authorization(request, cycle, host_client)?;
                    let intent_record = build_headless_intent_record(
                        &handoff_id,
                        &request.request_key,
                        cycle,
                        &frozen_task,
                        verified_closure_id,
                        next_cycle_rev,
                    );
                    let cycle_mut = &mut ledger.cycles[cycle_idx];
                    cycle_mut.cycle_revision = next_cycle_rev;
                    cycle_mut.handoffs.push(intent_record);
                    let cycle_snapshot = cycle_mut.clone();
                    ledger.ledger_revision = next_ledger_rev;
                    self.write_ledger_unlocked(&request.binding_id, &ledger)?;
                    (cycle_snapshot, handoff_id, None)
                }
            }
        };

        if let Some(native_record) = is_native_completed {
            return Ok(native_record);
        }

        let client = host_client.ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::HostNotConfigured,
                "ApprovedHostClient is required for headless worker dispatch.",
            )
        })?;
        let dispatch_res = client
            .dispatch_task(&cycle_snapshot.approval_id, &request.task_id)
            .await;

        self.finalize_dispatched_handoff(
            &request.binding_id,
            &request.cycle_id,
            &handoff_id,
            dispatch_res,
        )
    }

    /// Explicitly reconciles a headless worker handoff against [`ApprovedHostClient::get_task`]
    /// and stores the observed host task snapshot and run/artifact evidence.
    ///
    /// Never invokes [`ApprovedHostClient::dispatch_task`]. When the host reports a sealed run
    /// receipt (`run_id` and `worker_id`), transitions [`HandoffStatus`] to the observed state;
    /// when no sealed receipt exists (`NotStarted`, `DependencyInputs*`, or `Unknown`), records
    /// the observed state while keeping [`HandoffStatus::OutcomeUnknown`] so unknown outcomes
    /// can never replay.
    pub async fn reconcile_handoff(
        &self,
        request: &ReconcileRoleHandoffRequest,
        host_client: &ApprovedHostClient,
    ) -> Result<RoleHandoffRecord, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;
        validate_bounded_identifier(&request.cycle_id, "cycle_id", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.handoff_id, "handoff_id", MAX_ID_LEN)?;
        if request.expected_cycle_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_cycle_revision must be >= 1.",
            ));
        }
        if host_client.control_binding_id() != request.binding_id
            || host_client.native_thread_uuid() != request.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "ApprovedHostClient control_binding_id or native_thread_uuid does not match reconcile request.",
            ));
        }

        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(&request.binding_id);
        let ledger_path = self.ledger_path_for_binding(&request.binding_id)?;

        let (approval_id, task_id) = {
            let _guard = acquire_cross_process_lock(
                &self.plans_dir,
                &lock_filename,
                "role_flow_ledger_lock",
            )?;
            let ledger = self
                .read_ledger_unlocked(&request.binding_id, &ledger_path)?
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Role-flow ledger is not initialized for this binding.",
                    )
                })?;
            let cycle = ledger
                .cycles
                .iter()
                .find(|c| c.cycle_id == request.cycle_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Requested role-flow cycle_id was not found in this binding.",
                    )
                })?;
            if cycle.binding_id != request.binding_id || cycle.thread_uuid != request.thread_uuid {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Role-flow cycle does not belong to the requested binding and thread scope.",
                ));
            }
            if request.expected_cycle_revision != cycle.cycle_revision {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "expected_cycle_revision does not match current role-flow cycle_revision.",
                ));
            }
            let handoff = cycle
                .handoffs
                .iter()
                .find(|h| h.handoff_id == request.handoff_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Requested handoff_id was not found in this role-flow cycle.",
                    )
                })?;
            if handoff.route != RoleExecutionRoute::HeadlessWorker {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Only headless_worker handoffs can be reconciled via ApprovedHostClient::get_task.",
                ));
            }
            (cycle.approval_id.clone(), handoff.task_id.clone())
        };

        let snapshot = host_client
            .get_task(&approval_id, &task_id)
            .await
            .map_err(map_host_error)?;

        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_filename, "role_flow_ledger_lock")?;
        let mut ledger = self
            .read_ledger_unlocked(&request.binding_id, &ledger_path)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Role-flow ledger disappeared during handoff reconciliation.",
                )
            })?;
        let cycle_idx = ledger
            .cycles
            .iter()
            .position(|c| c.cycle_id == request.cycle_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Role-flow cycle disappeared during handoff reconciliation.",
                )
            })?;
        if ledger.cycles[cycle_idx].cycle_revision != request.expected_cycle_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "cycle_revision changed concurrently during handoff reconciliation.",
            ));
        }
        let next_cycle_rev = ledger.cycles[cycle_idx]
            .cycle_revision
            .checked_add(1)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Cycle revision overflow.",
                )
            })?;
        let next_ledger_rev = ledger.ledger_revision.checked_add(1).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Ledger revision overflow.",
            )
        })?;
        let evidence = build_evidence_ref(&ledger.cycles[cycle_idx], &snapshot);
        let has_sealed_run = snapshot.run_id.is_some() && snapshot.worker_id.is_some();
        let cycle_mut = &mut ledger.cycles[cycle_idx];
        let handoff_mut = cycle_mut
            .handoffs
            .iter_mut()
            .find(|h| h.handoff_id == request.handoff_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Role handoff record disappeared during reconciliation.",
                )
            })?;

        handoff_mut.observed_task_status = Some(snapshot.status);
        handoff_mut.run_id = snapshot.run_id.clone();
        handoff_mut.worker_id = snapshot.worker_id.clone();
        handoff_mut.evidence_ref = Some(evidence);
        handoff_mut.task_snapshot = Some(snapshot.clone());
        handoff_mut.updated_at_cycle_revision = next_cycle_rev;
        handoff_mut.status = map_task_status_to_handoff_status(snapshot.status, has_sealed_run);
        if !handoff_mut.status.is_ambiguous() {
            handoff_mut.last_error = None;
        }

        let updated = handoff_mut.clone();
        cycle_mut.cycle_revision = next_cycle_rev;
        ledger.ledger_revision = next_ledger_rev;
        self.write_ledger_unlocked(&request.binding_id, &ledger)?;
        Ok(updated)
    }

    /// Reserves single-closure authority once per `(repository_path, package_id, candidate_tree_sha)`
    /// across all cycles in `binding_id`.
    ///
    /// Derives the concrete `candidate_tree_sha` from the cycle's approved base commit via
    /// controlled read-only Git and requires `subject_digest` to equal the cycle's software-validated
    /// `approval_subject_digest`. If the same `(repository_path, package_id, candidate_tree_sha)`
    /// was already reserved with the same `subject_digest` (even in an earlier cycle), returns the
    /// existing reservation without spending another closure call. If `subject_digest` differs for
    /// the same tree key, fails with [`NativeErrorCode::ValidationFailed`].
    pub fn reserve_closure(
        &self,
        request: &ReservePackageClosureRequest,
    ) -> Result<PackageClosureReservation, NativeError> {
        validate_bounded_identifier(&request.binding_id, "binding_id", MAX_ID_LEN)?;
        validate_canonical_thread_uuid("thread_uuid", &request.thread_uuid).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
            )
        })?;
        validate_bounded_identifier(&request.cycle_id, "cycle_id", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.request_key, "request_key", MAX_ID_LEN)?;
        validate_bounded_identifier(&request.package_id, "package_id", MAX_ID_LEN)?;
        if request.expected_cycle_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_cycle_revision must be >= 1.",
            ));
        }
        if let Some(ref task_id) = request.task_id {
            validate_bounded_identifier(task_id, "task_id", MAX_ID_LEN)?;
        }

        let _ = validate_existing_private_store_dir(&self.plans_dir, "plans_dir")?;
        let lock_filename = lock_filename_for_binding(&request.binding_id);
        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_filename, "role_flow_ledger_lock")?;
        let ledger_path = self.ledger_path_for_binding(&request.binding_id)?;
        let mut ledger = self
            .read_ledger_unlocked(&request.binding_id, &ledger_path)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Role-flow ledger is not initialized for this binding.",
                )
            })?;

        let cycle_idx = ledger
            .cycles
            .iter()
            .position(|c| c.cycle_id == request.cycle_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Requested role-flow cycle_id was not found in this binding.",
                )
            })?;
        if ledger.cycles[cycle_idx].binding_id != request.binding_id
            || ledger.cycles[cycle_idx].thread_uuid != request.thread_uuid
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Role-flow cycle does not belong to the requested binding and thread scope.",
            ));
        }

        if let Some(ref task_id) = request.task_id {
            let task = ledger.cycles[cycle_idx]
                .tasks
                .iter()
                .find(|t| &t.task_id == task_id)
                .ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "Closure task_id is not frozen in this role-flow cycle.",
                    )
                })?;
            if task.role != WorkflowRole::Closure {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Only tasks with WorkflowRole::Closure may reserve a package closure.",
                ));
            }
        }

        let (candidate_tree_sha, subject_digest) =
            derive_and_verify_closure_subject(&ledger.cycles[cycle_idx], &request.source)?;

        if let Some(existing_by_req) = ledger
            .closures
            .iter()
            .find(|res| res.request_key == request.request_key)
        {
            if existing_by_req.repository_path == ledger.cycles[cycle_idx].repository_path
                && existing_by_req.package_id == request.package_id
                && existing_by_req.candidate_tree_sha == candidate_tree_sha
                && existing_by_req.subject_digest == subject_digest
                && existing_by_req.task_id == request.task_id
            {
                return Ok(existing_by_req.clone());
            }
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Closure request_key was already used with different parameters.",
            ));
        }

        if let Some(existing_by_key) = ledger.closures.iter().find(|res| {
            res.repository_path == ledger.cycles[cycle_idx].repository_path
                && res.package_id == request.package_id
                && res.candidate_tree_sha == candidate_tree_sha
        }) {
            if existing_by_key.subject_digest == subject_digest {
                return Ok(existing_by_key.clone());
            }
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Conflicting closure subject_digest for an already-reserved (repository, package, candidate_tree_sha).",
            ));
        }

        let cycle = &ledger.cycles[cycle_idx];
        if request.expected_cycle_revision != cycle.cycle_revision {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "expected_cycle_revision does not match current role-flow cycle_revision.",
            ));
        }
        if ledger.closures.len() >= MAX_CLOSURES_PER_BINDING {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Role-flow ledger reached maximum closure reservation capacity.",
            ));
        }

        let blocker = inspect_unresolved_blocking_changes(
            &self.plans_dir,
            &cycle.binding_id,
            Some(&cycle.approval_id),
            request.task_id.as_deref(),
        )?;
        if blocker.blocked {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Package closure reservation is blocked by an unresolved change or superseded approval.",
            ));
        }

        let next_cycle_rev = cycle.cycle_revision.checked_add(1).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Cycle revision overflow.",
            )
        })?;
        let next_ledger_rev = ledger.ledger_revision.checked_add(1).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Ledger revision overflow.",
            )
        })?;

        let reservation_id = deterministic_closure_id(
            &cycle.binding_id,
            &cycle.repository_path,
            &request.package_id,
            &candidate_tree_sha,
        );
        let reservation = PackageClosureReservation {
            reservation_id,
            request_key: request.request_key.clone(),
            binding_id: cycle.binding_id.clone(),
            thread_uuid: cycle.thread_uuid.clone(),
            cycle_id: cycle.cycle_id.clone(),
            approval_id: cycle.approval_id.clone(),
            repository_path: cycle.repository_path.clone(),
            package_id: request.package_id.clone(),
            base_commit: cycle.base_commit.clone(),
            candidate_tree_sha,
            subject_digest,
            source: request.source.clone(),
            task_id: request.task_id.clone(),
            reserved_at_ledger_revision: next_ledger_rev,
        };

        ledger.cycles[cycle_idx].cycle_revision = next_cycle_rev;
        ledger.ledger_revision = next_ledger_rev;
        ledger.closures.push(reservation.clone());
        self.write_ledger_unlocked(&request.binding_id, &ledger)?;
        Ok(reservation)
    }

    fn finalize_dispatched_handoff(
        &self,
        binding_id: &str,
        cycle_id: &str,
        handoff_id: &str,
        dispatch_res: Result<TaskStateSnapshot, HostError>,
    ) -> Result<RoleHandoffRecord, NativeError> {
        let lock_filename = lock_filename_for_binding(binding_id);
        let _guard =
            acquire_cross_process_lock(&self.plans_dir, &lock_filename, "role_flow_ledger_lock")?;
        let ledger_path = self.ledger_path_for_binding(binding_id)?;
        let mut ledger = self
            .read_ledger_unlocked(binding_id, &ledger_path)?
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Role-flow ledger disappeared after headless dispatch.",
                )
            })?;
        let cycle_idx = ledger
            .cycles
            .iter()
            .position(|c| c.cycle_id == cycle_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "Role-flow cycle disappeared after headless dispatch.",
                )
            })?;
        let next_cycle_rev = ledger.cycles[cycle_idx]
            .cycle_revision
            .checked_add(1)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Cycle revision overflow.",
                )
            })?;
        let next_ledger_rev = ledger.ledger_revision.checked_add(1).ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Ledger revision overflow.",
            )
        })?;

        match dispatch_res {
            Ok(snapshot) => {
                let evidence = build_evidence_ref(&ledger.cycles[cycle_idx], &snapshot);
                let has_sealed_run = snapshot.run_id.is_some() && snapshot.worker_id.is_some();
                let cycle_mut = &mut ledger.cycles[cycle_idx];
                let handoff_mut = cycle_mut
                    .handoffs
                    .iter_mut()
                    .find(|h| h.handoff_id == handoff_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "Persisted handoff intent disappeared after headless dispatch.",
                        )
                    })?;
                handoff_mut.status =
                    map_task_status_to_handoff_status(snapshot.status, has_sealed_run);
                handoff_mut.observed_task_status = Some(snapshot.status);
                handoff_mut.run_id = snapshot.run_id.clone();
                handoff_mut.worker_id = snapshot.worker_id.clone();
                handoff_mut.evidence_ref = Some(evidence);
                handoff_mut.task_snapshot = Some(snapshot);
                handoff_mut.updated_at_cycle_revision = next_cycle_rev;
                if !handoff_mut.status.is_ambiguous() {
                    handoff_mut.last_error = None;
                }
                let updated = handoff_mut.clone();
                cycle_mut.cycle_revision = next_cycle_rev;
                ledger.ledger_revision = next_ledger_rev;
                self.write_ledger_unlocked(binding_id, &ledger)?;
                Ok(updated)
            }
            Err(host_err) => {
                let sanitized = sanitize_diagnostic_text(host_err.code());
                let cycle_mut = &mut ledger.cycles[cycle_idx];
                let handoff_mut = cycle_mut
                    .handoffs
                    .iter_mut()
                    .find(|h| h.handoff_id == handoff_id)
                    .ok_or_else(|| {
                        NativeError::new(
                            NativeErrorCode::ProtocolError,
                            "Persisted handoff intent disappeared after failed headless dispatch.",
                        )
                    })?;
                handoff_mut.status = HandoffStatus::OutcomeUnknown;
                handoff_mut.updated_at_cycle_revision = next_cycle_rev;
                handoff_mut.last_error = Some(sanitized.clone());
                cycle_mut.cycle_revision = next_cycle_rev;
                ledger.ledger_revision = next_ledger_rev;
                self.write_ledger_unlocked(binding_id, &ledger)?;
                Err(NativeError::new(
                    NativeErrorCode::AmbiguousTransportFailure,
                    format!(
                        "Headless dispatch outcome is unknown after durable intent ({sanitized}); reconcile before acting."
                    ),
                ))
            }
        }
    }

    fn read_ledger_unlocked(
        &self,
        expected_binding_id: &str,
        ledger_path: &Path,
    ) -> Result<Option<PersistedRoleFlowLedger>, NativeError> {
        let Some(bytes) =
            read_private_file_bounded(ledger_path, MAX_ROLE_FLOW_LEDGER_BYTES, "role_flow_ledger")?
        else {
            return Ok(None);
        };
        let ledger: PersistedRoleFlowLedger = serde_json::from_slice(&bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to parse role-flow ledger JSON.",
            )
        })?;
        validate_persisted_ledger(&ledger, expected_binding_id)?;
        Ok(Some(ledger))
    }

    fn write_ledger_unlocked(
        &self,
        binding_id: &str,
        ledger: &PersistedRoleFlowLedger,
    ) -> Result<(), NativeError> {
        validate_persisted_ledger(ledger, binding_id)?;
        let bytes = serde_json::to_vec_pretty(ledger).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to serialize role-flow ledger JSON.",
            )
        })?;
        if bytes.len() > MAX_ROLE_FLOW_LEDGER_BYTES {
            return Err(NativeError::new(
                NativeErrorCode::OutputOverflow,
                "Role-flow ledger exceeds maximum 1 MiB file budget.",
            ));
        }
        let filename = ledger_filename_for_binding(binding_id);
        write_private_file_atomic(&self.plans_dir, &filename, &bytes, "role_flow_ledger")
            .map(|_| ())
    }
}

fn verify_approval_scope_and_plan(
    approval: &ApprovalRecord,
    binding_id: &str,
    thread_uuid: &str,
    authority: &AuthorityStore,
) -> Result<(), NativeError> {
    if approval.binding_id != binding_id || approval.thread_uuid != thread_uuid {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovalRecord does not belong to the requested binding and thread scope.",
        ));
    }
    let plan_rev = authority
        .plans()
        .get_revision(&approval.plan_id, approval.plan_revision)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read workflow plan revision from PlanStore.",
            )
        })?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced plan revision was not found in PlanStore.",
            )
        })?;
    if plan_rev.binding_id != binding_id {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Referenced workflow plan does not belong to the requested binding.",
        ));
    }
    let latest_rev = authority
        .plans()
        .latest_revision(&approval.plan_id)
        .map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to read latest workflow plan revision from PlanStore.",
            )
        })?
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced workflow plan has no revisions in PlanStore.",
            )
        })?;
    if !plan_rev.status.is_active_published() || latest_rev.revision != approval.plan_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Approval plan_revision is no longer the active published revision.",
        ));
    }
    Ok(())
}

fn verify_and_freeze_cycle_tasks(
    approval: &ApprovalRecord,
    task_ids: &[String],
    registry_snapshot: &RegistrySnapshot,
    policy: &ExecutionPolicy,
) -> Result<Vec<FrozenCycleTask>, NativeError> {
    let mut approved_tasks = Vec::with_capacity(task_ids.len());
    let mut unique_roles = BTreeSet::new();
    let mut role_requests = Vec::new();
    for task_id in task_ids {
        let task = approval
            .tasks
            .iter()
            .find(|t| &t.task_id == task_id)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    format!("Requested task_id '{task_id}' does not exist in the frozen approval."),
                )
            })?;
        approved_tasks.push(task);
        if unique_roles.insert((task.role, task.difficulty)) {
            role_requests.push(RolePreflightRequest {
                role: task.role,
                difficulty: Some(task.difficulty),
            });
        }
    }

    let report = preflight(
        registry_snapshot,
        &PreflightRequest {
            profile_id: Some(approval.profile_id.clone()),
            roles: role_requests,
        },
        policy,
    )?;
    if !report.ready {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "A1 preflight blocked one or more requested tasks under the execution policy.",
        ));
    }
    if report.profile_id.as_deref() != Some(approval.profile_id.as_str()) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "A1 preflight resolved profile_id does not match the frozen approval profile_id.",
        ));
    }

    let mut outcome_by_role = BTreeMap::new();
    for outcome in &report.roles {
        if let Some(diff) = outcome.requested_difficulty {
            outcome_by_role.insert((outcome.role, diff), outcome);
        }
    }

    let mut frozen_tasks = Vec::with_capacity(approved_tasks.len());
    for task in approved_tasks {
        let outcome = outcome_by_role
            .get(&(task.role, task.difficulty))
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "A1 preflight did not return an outcome for a requested task role/difficulty.",
                )
            })?;
        if !outcome.ready {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "A1 preflight rejected the requested task role/difficulty.",
            ));
        }
        let selected = outcome.selected.as_ref().ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "A1 preflight did not return a selected role choice for an approved task.",
            )
        })?;
        let expected_route = RoleExecutionRoute::from_adapter(task.adapter);
        if selected.tool_id != task.tool_id
            || selected.adapter != task.adapter
            || selected.route != expected_route
            || selected.model_id != task.model_id
            || selected.model_arg != task.model_arg
            || selected.effort != task.effort
        {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Frozen approval task assignment does not match current A1 preflight resolution.",
            ));
        }
        frozen_tasks.push(FrozenCycleTask {
            task_id: task.task_id.clone(),
            task_digest: task.task_digest.clone(),
            title: task.title.clone(),
            instruction: task.instruction.clone(),
            role: task.role,
            difficulty: task.difficulty,
            tool_id: task.tool_id.clone(),
            adapter: task.adapter,
            route: selected.route,
            model_id: task.model_id.clone(),
            model_arg: task.model_arg.clone(),
            effort: task.effort.clone(),
            depends_on: task.depends_on.clone(),
            write_paths: task.allowed_write_paths.clone(),
        });
    }
    Ok(frozen_tasks)
}

fn recheck_live_dispatch_gates(
    plans_dir: &Path,
    cycle: &RoleFlowCycle,
    frozen_task: &FrozenCycleTask,
    current_policy: &ExecutionPolicy,
    authority: &AuthorityStore,
) -> Result<(), NativeError> {
    let approval = authority
        .get_for_operator(&cycle.approval_id)
        .map_err(map_authority_error)?;
    verify_approval_scope_and_plan(&approval, &cycle.binding_id, &cycle.thread_uuid, authority)?;
    if approval.subject_digest != cycle.approval_subject_digest
        || approval.registry_revision != cycle.registry_revision
        || approval.profile_id != cycle.profile_id
        || approval.repository_path.to_string_lossy() != cycle.repository_path
        || approval.base_commit != cycle.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Frozen approval metadata drifted from the role-flow cycle snapshot.",
        ));
    }

    let registry_snapshot = authority.registry().snapshot().map_err(|_| {
        NativeError::new(
            NativeErrorCode::ProtocolError,
            "Failed to read workflow registry snapshot during dispatch recheck.",
        )
    })?;
    if registry_snapshot.revision != cycle.registry_revision {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Registry revision drifted since the role-flow cycle was frozen.",
        ));
    }

    let rechecked = verify_and_freeze_cycle_tasks(
        &approval,
        std::slice::from_ref(&frozen_task.task_id),
        &registry_snapshot,
        current_policy,
    )?;
    if rechecked.first() != Some(frozen_task) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Rechecked task choice at dispatch drifted from the cycle's frozen task choice.",
        ));
    }

    let blocker: UnresolvedChangeBlockSummary = inspect_unresolved_blocking_changes(
        plans_dir,
        &cycle.binding_id,
        Some(&cycle.approval_id),
        Some(&frozen_task.task_id),
    )?;
    if blocker.blocked {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Task handoff is blocked by an unresolved conversational change or superseded approval.",
        ));
    }
    Ok(())
}

fn verify_existing_handoff_match(
    existing: &RoleHandoffRecord,
    request: &RoleHandoffRequest,
    cycle: &RoleFlowCycle,
    current_policy_digest: &str,
) -> Result<(), NativeError> {
    if existing.binding_id != request.binding_id
        || existing.thread_uuid != request.thread_uuid
        || existing.cycle_id != request.cycle_id
        || existing.task_id != request.task_id
        || existing.policy_digest != current_policy_digest
        || existing.closure_reservation_id != request.closure_reservation_id
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Handoff request parameters do not match the previously recorded handoff for this key or task.",
        ));
    }
    if existing.route == RoleExecutionRoute::HeadlessWorker {
        if !request.operator_approved {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Headless worker handoff requires explicit operator_approved = true.",
            ));
        }
        let Some(ref confirm_digest) = request.confirm_approval_digest else {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "Headless worker handoff requires confirm_approval_digest matching the frozen approval.",
            ));
        };
        if !constant_time_str_eq(confirm_digest, &cycle.approval_subject_digest) {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "confirm_approval_digest does not match the cycle's frozen approval_subject_digest.",
            ));
        }
    }
    Ok(())
}

fn verify_closure_handoff_reservation(
    request: &RoleHandoffRequest,
    cycle: &RoleFlowCycle,
    frozen_task: &FrozenCycleTask,
    all_cycles: &[RoleFlowCycle],
    all_closures: &[PackageClosureReservation],
) -> Result<Option<String>, NativeError> {
    if frozen_task.role != WorkflowRole::Closure {
        if request.closure_reservation_id.is_some() {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Non-closure roles cannot bind or spend a closure_reservation_id.",
            ));
        }
        return Ok(None);
    }

    let Some(ref reservation_id) = request.closure_reservation_id else {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "WorkflowRole::Closure handoff requires an explicit closure_reservation_id reserved for this cycle and task.",
        ));
    };

    let reservation = all_closures
        .iter()
        .find(|res| &res.reservation_id == reservation_id)
        .ok_or_else(|| {
            NativeError::new(
                NativeErrorCode::ValidationFailed,
                "Referenced closure_reservation_id was not found in this binding.",
            )
        })?;

    if reservation.binding_id != cycle.binding_id
        || reservation.thread_uuid != cycle.thread_uuid
        || reservation.cycle_id != cycle.cycle_id
        || reservation.approval_id != cycle.approval_id
        || reservation.repository_path != cycle.repository_path
        || reservation.base_commit != cycle.base_commit
        || reservation.task_id.as_deref() != Some(request.task_id.as_str())
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Closure reservation does not belong to this exact cycle, approval, repository, base_commit, and closure task.",
        ));
    }

    if all_cycles.iter().any(|c| {
        c.handoffs
            .iter()
            .any(|h| h.closure_reservation_id.as_deref() == Some(reservation_id.as_str()))
    }) {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Closure reservation was already spent by an existing closure handoff.",
        ));
    }

    let (actual_tree_sha, actual_subject_digest) =
        derive_and_verify_closure_subject(cycle, &reservation.source)?;
    if actual_tree_sha != reservation.candidate_tree_sha
        || actual_subject_digest != reservation.subject_digest
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Candidate tree SHA or subject digest drifted from the bound closure reservation.",
        ));
    }

    Ok(Some(reservation_id.clone()))
}

fn verify_headless_dispatch_authorization(
    request: &RoleHandoffRequest,
    cycle: &RoleFlowCycle,
    host_client: Option<&ApprovedHostClient>,
) -> Result<(), NativeError> {
    if !request.operator_approved {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "Headless worker handoff requires explicit operator_approved = true.",
        ));
    }
    let Some(ref confirm_digest) = request.confirm_approval_digest else {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "Headless worker handoff requires confirm_approval_digest matching the frozen approval.",
        ));
    };
    if !constant_time_str_eq(confirm_digest, &cycle.approval_subject_digest) {
        return Err(NativeError::new(
            NativeErrorCode::UnauthorizedOperator,
            "confirm_approval_digest does not match the cycle's frozen approval_subject_digest.",
        ));
    }
    let Some(client) = host_client else {
        return Err(NativeError::new(
            NativeErrorCode::HostNotConfigured,
            "ApprovedHostClient is required for headless worker dispatch.",
        ));
    };
    if client.control_binding_id() != cycle.binding_id
        || client.native_thread_uuid() != cycle.thread_uuid
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "ApprovedHostClient control_binding_id or native_thread_uuid does not match the role-flow cycle.",
        ));
    }
    Ok(())
}

fn build_native_handoff_record(
    handoff_id: &str,
    request_key: &str,
    cycle: &RoleFlowCycle,
    frozen_task: &FrozenCycleTask,
    closure_reservation_id: Option<String>,
    cycle_revision: u64,
) -> RoleHandoffRecord {
    RoleHandoffRecord {
        handoff_id: handoff_id.to_string(),
        request_key: request_key.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        approval_id: cycle.approval_id.clone(),
        binding_id: cycle.binding_id.clone(),
        thread_uuid: cycle.thread_uuid.clone(),
        task_id: frozen_task.task_id.clone(),
        task_digest: frozen_task.task_digest.clone(),
        role: frozen_task.role,
        difficulty: frozen_task.difficulty,
        tool_id: frozen_task.tool_id.clone(),
        adapter: frozen_task.adapter,
        route: RoleExecutionRoute::NativeHandoff,
        model_id: frozen_task.model_id.clone(),
        model_arg: frozen_task.model_arg.clone(),
        effort: frozen_task.effort.clone(),
        policy_id: cycle.policy.policy_id.clone(),
        policy_digest: cycle.policy_digest.clone(),
        closure_reservation_id,
        status: HandoffStatus::NativeHandoffRecorded,
        created_at_cycle_revision: cycle_revision,
        updated_at_cycle_revision: cycle_revision,
        run_id: None,
        worker_id: None,
        observed_task_status: None,
        native_handoff: Some(NativeRoleHandoff {
            thread_uuid: cycle.thread_uuid.clone(),
            task_id: frozen_task.task_id.clone(),
            task_digest: frozen_task.task_digest.clone(),
            title: frozen_task.title.clone(),
            frozen_instruction: frozen_task.instruction.clone(),
            requested_model_id: frozen_task.model_id.clone(),
            requested_model_arg: frozen_task.model_arg.clone(),
            requested_effort: frozen_task.effort.clone(),
            model_execution_verified: false,
            native_per_call_identity_verified: false,
        }),
        evidence_ref: None,
        task_snapshot: None,
        last_error: None,
    }
}

fn build_headless_intent_record(
    handoff_id: &str,
    request_key: &str,
    cycle: &RoleFlowCycle,
    frozen_task: &FrozenCycleTask,
    closure_reservation_id: Option<String>,
    cycle_revision: u64,
) -> RoleHandoffRecord {
    RoleHandoffRecord {
        handoff_id: handoff_id.to_string(),
        request_key: request_key.to_string(),
        cycle_id: cycle.cycle_id.clone(),
        approval_id: cycle.approval_id.clone(),
        binding_id: cycle.binding_id.clone(),
        thread_uuid: cycle.thread_uuid.clone(),
        task_id: frozen_task.task_id.clone(),
        task_digest: frozen_task.task_digest.clone(),
        role: frozen_task.role,
        difficulty: frozen_task.difficulty,
        tool_id: frozen_task.tool_id.clone(),
        adapter: frozen_task.adapter,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: frozen_task.model_id.clone(),
        model_arg: frozen_task.model_arg.clone(),
        effort: frozen_task.effort.clone(),
        policy_id: cycle.policy.policy_id.clone(),
        policy_digest: cycle.policy_digest.clone(),
        closure_reservation_id,
        status: HandoffStatus::IntentRecorded,
        created_at_cycle_revision: cycle_revision,
        updated_at_cycle_revision: cycle_revision,
        run_id: None,
        worker_id: None,
        observed_task_status: None,
        native_handoff: None,
        evidence_ref: None,
        task_snapshot: None,
        last_error: None,
    }
}

fn build_evidence_ref(
    cycle: &RoleFlowCycle,
    snapshot: &TaskStateSnapshot,
) -> RoleEvidenceReference {
    let task_worktree_id = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.worktree.task_worktree_id.clone());
    let head_commit = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.worktree.head_commit.clone());
    let observed_model = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.observed_model.clone());
    let observed_effort = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.observed_effort.clone());
    let report_artifact_id = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.outcome.as_ref())
        .and_then(|o| o.report_artifact_id.clone());
    let final_summary_preview = snapshot
        .worker
        .as_ref()
        .and_then(|w| w.outcome.as_ref())
        .and_then(|o| o.final_summary.clone());

    RoleEvidenceReference {
        approval_id: cycle.approval_id.clone(),
        task_id: snapshot.task_id.clone(),
        run_id: snapshot.run_id.clone(),
        worker_id: snapshot.worker_id.clone(),
        report_artifact_id,
        task_worktree_id,
        base_commit: cycle.base_commit.clone(),
        head_commit,
        observed_model,
        observed_effort,
        final_summary_preview,
        final_summary_is_complete_evidence: false,
    }
}

fn map_task_status_to_handoff_status(
    status: TaskExecutionStatus,
    has_sealed_run: bool,
) -> HandoffStatus {
    if !has_sealed_run {
        return HandoffStatus::OutcomeUnknown;
    }
    match status {
        TaskExecutionStatus::Queued
        | TaskExecutionStatus::Claimed
        | TaskExecutionStatus::Running
        | TaskExecutionStatus::CancelRequested => HandoffStatus::Dispatched,
        TaskExecutionStatus::ReadyForReview => HandoffStatus::ReadyForReview,
        TaskExecutionStatus::Failed => HandoffStatus::Failed,
        TaskExecutionStatus::Cancelled => HandoffStatus::Cancelled,
        TaskExecutionStatus::TimedOut => HandoffStatus::TimedOut,
        TaskExecutionStatus::NotStarted
        | TaskExecutionStatus::DependencyInputsUnverified
        | TaskExecutionStatus::DependencyInputsReady
        | TaskExecutionStatus::NativeRouteRequired
        | TaskExecutionStatus::Unknown => HandoffStatus::OutcomeUnknown,
    }
}

fn derive_and_verify_closure_subject(
    cycle: &RoleFlowCycle,
    source: &ClosureSubjectSource,
) -> Result<(String, String), NativeError> {
    let (canonical_repo, normalized_base) =
        validate_repository_and_base_commit(Path::new(&cycle.repository_path), &cycle.base_commit)
            .map_err(map_authority_error)?;
    if canonical_repo.to_string_lossy() != cycle.repository_path
        || normalized_base != cycle.base_commit
    {
        return Err(NativeError::new(
            NativeErrorCode::ValidationFailed,
            "Cycle repository_path or base_commit does not match canonical repository state.",
        ));
    }

    match source {
        ClosureSubjectSource::ApprovedCycleBase {
            subject_digest,
            expected_candidate_tree_sha,
        } => {
            validate_sha256_hex_digest(subject_digest, "subject_digest").map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "subject_digest must be a 64-character lowercase hex SHA-256 digest.",
                )
            })?;
            if !constant_time_str_eq(subject_digest, &cycle.approval_subject_digest) {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Closure subject_digest must equal the cycle's software-validated approval_subject_digest.",
                ));
            }
            let tree_sha = resolve_exact_tree_sha(
                &canonical_repo,
                &normalized_base,
                DEFAULT_GIT_COMMAND_TIMEOUT_MS,
            )
            .map_err(|_| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Failed to resolve Git tree SHA from cycle base_commit.",
                )
            })?;
            if tree_sha != cycle.base_tree_sha {
                return Err(NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "Resolved Git tree SHA for base_commit drifted from cycle base_tree_sha.",
                ));
            }
            if let Some(ref expected_tree) = expected_candidate_tree_sha {
                validate_exact_40_hex_sha(expected_tree, "expected_candidate_tree_sha").map_err(
                    |_| {
                        NativeError::new(
                            NativeErrorCode::ValidationFailed,
                            "expected_candidate_tree_sha must be a 40-character lowercase hex SHA-1.",
                        )
                    },
                )?;
                if expected_tree != &tree_sha {
                    return Err(NativeError::new(
                        NativeErrorCode::ValidationFailed,
                        "expected_candidate_tree_sha does not match the resolved base_commit tree SHA.",
                    ));
                }
            }
            Ok((tree_sha, subject_digest.clone()))
        }
    }
}

fn validate_existing_private_store_dir(dir: &Path, label: &str) -> Result<PathBuf, NativeError> {
    if dir.as_os_str().is_empty() {
        return Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            format!("{label} path must not be empty."),
        ));
    }
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|_| {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to resolve current working directory for {label}."),
            )
        })?;
        cwd.join(dir)
    };
    let clean = validate_absolute_no_symlink_components(&resolved, label)?;
    let meta = fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            NativeError::new(
                NativeErrorCode::NotConfigured,
                format!("Required {label} directory does not exist."),
            )
        } else {
            NativeError::new(
                NativeErrorCode::IoError,
                format!("Failed to inspect {label} directory metadata."),
            )
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(NativeError::new(
            NativeErrorCode::SecurityViolation,
            format!("{label} must be an existing private non-symlink directory."),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(NativeError::new(
                NativeErrorCode::SecurityViolation,
                format!("{label} directory has insecure permissions (expected 0700)."),
            ));
        }
    }
    Ok(clean)
}

fn validate_persisted_ledger(
    ledger: &PersistedRoleFlowLedger,
    expected_binding_id: &str,
) -> Result<(), NativeError> {
    if ledger.schema_version != ROLE_FLOW_SCHEMA_VERSION {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "Unsupported role-flow ledger schema_version.",
        ));
    }
    validate_bounded_identifier(&ledger.binding_id, "ledger.binding_id", MAX_ID_LEN)?;
    if ledger.binding_id != expected_binding_id {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "Role-flow ledger binding_id does not match the expected binding file.",
        ));
    }
    if ledger.cycles.len() > MAX_CYCLES_PER_BINDING {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "Role-flow ledger exceeds maximum cycle count.",
        ));
    }
    if ledger.closures.len() > MAX_CLOSURES_PER_BINDING {
        return Err(NativeError::new(
            NativeErrorCode::ProtocolError,
            "Role-flow ledger exceeds maximum closure reservation count.",
        ));
    }
    for cycle in &ledger.cycles {
        if cycle.binding_id != expected_binding_id || cycle.cycle_revision == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "Role-flow cycle has invalid binding_id or cycle_revision.",
            ));
        }
        cycle.policy.validate().map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Persisted role-flow cycle contains an invalid ExecutionPolicy.",
            )
        })?;
        let computed_digest = cycle.policy.digest().map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "Failed to compute persisted cycle policy digest.",
            )
        })?;
        if computed_digest != cycle.policy_digest {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "Persisted role-flow cycle policy_digest does not match its policy.",
            ));
        }
        if cycle.handoffs.len() > MAX_HANDOFFS_PER_CYCLE {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                "Persisted role-flow cycle exceeds maximum handoff count.",
            ));
        }
    }
    Ok(())
}

fn map_authority_error(err: AuthorityError) -> NativeError {
    match err.code() {
        "security_violation" => NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Authority store reported a security violation.",
        ),
        "corrupt_store" => NativeError::new(
            NativeErrorCode::ProtocolError,
            "Authority store reported corrupt state.",
        ),
        "io_error" => NativeError::new(
            NativeErrorCode::IoError,
            "Authority store reported an I/O failure.",
        ),
        "unsupported_platform" => NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "Authority store reported an unsupported platform.",
        ),
        code => NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("Authority validation failed ({code})."),
        ),
    }
}

fn map_host_error(err: HostError) -> NativeError {
    match err.code() {
        "security_violation" => NativeError::new(
            NativeErrorCode::SecurityViolation,
            "Approved host reported a security violation.",
        ),
        "corrupt_manifest" | "corrupt_receipts" => NativeError::new(
            NativeErrorCode::ProtocolError,
            "Approved host reported corrupt manifest or receipt state.",
        ),
        "io_error" => NativeError::new(
            NativeErrorCode::IoError,
            "Approved host reported an I/O failure.",
        ),
        "unsupported_platform" => NativeError::new(
            NativeErrorCode::UnsupportedPlatform,
            "Approved host reported an unsupported platform.",
        ),
        code => NativeError::new(
            NativeErrorCode::ValidationFailed,
            format!("Approved host request failed ({code})."),
        ),
    }
}

fn binding_ledger_tag(binding_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:role-flow-ledger:v1\0");
    hasher.update((binding_id.len() as u64).to_le_bytes());
    hasher.update(binding_id.as_bytes());
    let digest = hasher.finalize();
    encode_lower_hex_16(&digest[..16])
}

fn ledger_filename_for_binding(binding_id: &str) -> String {
    let tag = binding_ledger_tag(binding_id);
    format!("role-flow-{tag}.json")
}

fn lock_filename_for_binding(binding_id: &str) -> String {
    let tag = binding_ledger_tag(binding_id);
    format!("role-flow-{tag}.lock")
}

fn deterministic_cycle_id(
    binding_id: &str,
    approval_id: &str,
    request_key: &str,
    policy_digest: &str,
    task_ids: &[String],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:role-flow-cycle:v1\0");
    for field in [binding_id, approval_id, request_key, policy_digest] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    hasher.update((task_ids.len() as u64).to_le_bytes());
    for tid in task_ids {
        hasher.update((tid.len() as u64).to_le_bytes());
        hasher.update(tid.as_bytes());
    }
    let digest = hasher.finalize();
    format!("flw_{}", encode_lower_hex_16(&digest[..16]))
}

fn deterministic_handoff_id(cycle_id: &str, task_id: &str, request_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:role-flow-handoff:v1\0");
    for field in [cycle_id, task_id, request_key] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    format!("hnd_{}", encode_lower_hex_16(&digest[..16]))
}

fn deterministic_closure_id(
    binding_id: &str,
    repository_path: &str,
    package_id: &str,
    candidate_tree_sha: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:role-flow-closure:v1\0");
    for field in [binding_id, repository_path, package_id, candidate_tree_sha] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    format!("cls_{}", encode_lower_hex_16(&digest[..16]))
}

fn encode_lower_hex_16(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}
