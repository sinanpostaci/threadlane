use serde::{Deserialize, Serialize};
use threadlane::{WorkerAcceptanceState, WorkerSnapshot, WorkerStatus};
use threadlane_authority::ApprovalRecord;
use threadlane_registry::ToolAdapter;
use threadlane_workers::WorkerProvider;

/// Current on-disk schema version for `host-manifest.json`.
pub const HOST_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Current on-disk schema version for `task-receipts.json`.
pub const TASK_RECEIPTS_SCHEMA_VERSION: u32 = 1;

/// Filename for the exclusive process-lifetime lock inside a host session directory.
pub const HOST_LOCK_FILENAME: &str = "host.lock";

/// Filename for the bounded atomic host session manifest inside a host session directory.
pub const HOST_MANIFEST_FILENAME: &str = "host-manifest.json";

/// Filename for the private sealed task-to-run mapping receipts inside a host session directory.
pub const TASK_RECEIPTS_FILENAME: &str = "task-receipts.json";

/// Filename for the cross-process lock protecting `task-receipts.json`.
pub const TASK_RECEIPTS_LOCK_FILENAME: &str = "task-receipts.lock";

/// Subdirectory name for the per-session `threadlane::Supervisor` SQLite and descriptor state.
pub const RUNTIME_STATE_SUBDIR: &str = "runtime-state";

/// Subdirectory name for the per-session isolated Git worktrees and manifests.
pub const WORK_AREA_SUBDIR: &str = "work-area";

/// Maximum permitted byte size when reading or writing `host-manifest.json` (`64 KiB`).
pub const MAX_HOST_MANIFEST_BYTES: usize = 65_536;

/// Maximum permitted byte size when reading or writing `task-receipts.json` (`64 KiB`).
pub const MAX_TASK_RECEIPTS_BYTES: usize = 65_536;

/// Lifecycle phase recorded in a per-session [`HostSessionManifest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostPhase {
    /// Host holds `host.lock` and is executing the two-phase supervisor bootstrap and binding recovery.
    Bootstrapping,
    /// Final supervisor is running with frozen `WorkersConfig` and authenticated readiness was verified.
    Ready,
    /// Host startup or runtime faulted; preserved for diagnostics without claiming readiness.
    Failed,
    /// Host completed a clean graceful shutdown.
    Stopped,
}

impl HostPhase {
    /// Returns the canonical `snake_case` string label for this phase.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrapping => "bootstrapping",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for HostPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Explicit execution safety and lifecycle gates reported by a host session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostExecutionGates {
    /// True only after the final supervisor is running and authenticated `status` readiness succeeded.
    pub supervisor_authenticated_ready: bool,
    /// Always `false`: no provider or worker is ever started automatically on host startup or restart.
    pub workers_auto_started_on_boot: bool,
    /// Always `true`: only root tasks (`depends_on.is_empty()`) can be dispatched on static `base_commit`.
    pub root_tasks_only: bool,
    /// Always `true`: tasks with non-empty `depends_on` are rejected with `dependency_inputs_unverified`.
    pub dependent_tasks_blocked: bool,
    /// Always `true`: `codex_cli` native routes are manual-only and rejected for worker dispatch with `native_route_required`.
    pub native_routes_manual_only: bool,
    /// Always `true`: worker `Completed` maps at most to `ReadyForReview`.
    pub worker_completed_means_ready_for_review: bool,
    /// Always `false`: host and workers never automatically accept, merge, push, or mark plans `Received`/`Applied`.
    pub auto_accept_or_merge_enabled: bool,
}

impl HostExecutionGates {
    /// Constructs the invariant execution gates for the given readiness state.
    pub const fn for_ready_state(supervisor_authenticated_ready: bool) -> Self {
        Self {
            supervisor_authenticated_ready,
            workers_auto_started_on_boot: false,
            root_tasks_only: true,
            dependent_tasks_blocked: true,
            native_routes_manual_only: true,
            worker_completed_means_ready_for_review: true,
            auto_accept_or_merge_enabled: false,
        }
    }
}

/// Durable, bounded (`<= 64 KiB`), private (`0600`) manifest persisted atomically inside
/// `<sessions_dir>/<approval_id>/host-manifest.json`.
///
/// Never contains supervisor bearer tokens, provider credentials, or raw task prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionManifest {
    /// On-disk manifest schema version (`HOST_MANIFEST_SCHEMA_VERSION`).
    pub schema_version: u32,
    /// Deterministic approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Workflow authority policy version frozen in the approval record.
    pub policy_version: u32,
    /// Authoritative control-plane chat binding identifier (`ApprovalRecord::binding_id`).
    pub control_binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Approved plan identifier.
    pub plan_id: String,
    /// Approved published plan revision number.
    pub plan_revision: u64,
    /// Single software-generated runtime session binding ID in `runtime-state`, once created or recovered.
    pub session_binding_id: Option<String>,
    /// Current lifecycle phase of the host session.
    pub phase: HostPhase,
    /// Process ID of the owning `threadlane-host` instance while bootstrapping or ready.
    pub pid: Option<u32>,
    /// Count of executable headless worker profiles (`antigravity` or `opencode_v2`) in this approval.
    pub worker_profile_count: usize,
    /// Count of manual native routes (`codex_cli`) in this approval.
    pub native_route_count: usize,
    /// Root headless worker task IDs (`depends_on.is_empty()`) eligible for dispatch.
    pub root_worker_task_ids: Vec<String>,
    /// Dependent task IDs (`!depends_on.is_empty()`) blocked until receipt-bound DAG inputs exist.
    pub dependent_task_ids: Vec<String>,
    /// Manual native route task IDs (`codex_cli`) blocked from headless worker dispatch.
    pub native_route_task_ids: Vec<String>,
    /// Explicit execution and review gates enforced by this host session.
    pub gates: HostExecutionGates,
    /// Sanitized, bounded failure diagnostic when `phase` is [`HostPhase::Failed`].
    pub last_error: Option<String>,
    /// Timestamp (ms since Unix epoch) when the session manifest was first created.
    pub created_at_ms: i64,
    /// Timestamp (ms since Unix epoch) when the session manifest was last updated.
    pub updated_at_ms: i64,
}

impl HostSessionManifest {
    /// Builds a new [`HostSessionManifest`] from a validated [`ApprovalRecord`].
    pub fn from_approval_record(
        record: &ApprovalRecord,
        session_binding_id: Option<String>,
        phase: HostPhase,
        pid: Option<u32>,
        last_error: Option<String>,
        created_at_ms: i64,
        updated_at_ms: i64,
    ) -> Self {
        let mut worker_profile_count = 0usize;
        let mut native_route_count = 0usize;
        let mut root_worker_task_ids = Vec::new();
        let mut dependent_task_ids = Vec::new();
        let mut native_route_task_ids = Vec::new();

        for task in &record.tasks {
            if task.provider.is_some() {
                worker_profile_count += 1;
                if task.depends_on.is_empty() {
                    root_worker_task_ids.push(task.task_id.clone());
                } else {
                    dependent_task_ids.push(task.task_id.clone());
                }
            } else {
                native_route_count += 1;
                native_route_task_ids.push(task.task_id.clone());
                if !task.depends_on.is_empty() {
                    dependent_task_ids.push(task.task_id.clone());
                }
            }
        }

        let ready = phase == HostPhase::Ready;
        Self {
            schema_version: HOST_MANIFEST_SCHEMA_VERSION,
            approval_id: record.approval_id.clone(),
            subject_digest: record.subject_digest.clone(),
            policy_version: record.policy_version,
            control_binding_id: record.binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            plan_id: record.plan_id.clone(),
            plan_revision: record.plan_revision,
            session_binding_id,
            phase,
            pid,
            worker_profile_count,
            native_route_count,
            root_worker_task_ids,
            dependent_task_ids,
            native_route_task_ids,
            gates: HostExecutionGates::for_ready_state(ready),
            last_error,
            created_at_ms,
            updated_at_ms,
        }
    }

    /// Projects this manifest into a safe [`HostSessionSummary`] omitting any local path material or PIDs.
    pub fn to_summary(&self) -> HostSessionSummary {
        HostSessionSummary {
            approval_id: self.approval_id.clone(),
            subject_digest: self.subject_digest.clone(),
            binding_id: self.control_binding_id.clone(),
            thread_uuid: self.thread_uuid.clone(),
            plan_id: self.plan_id.clone(),
            plan_revision: self.plan_revision,
            session_binding_id: self.session_binding_id.clone(),
            phase: self.phase,
            worker_profile_count: self.worker_profile_count,
            native_route_count: self.native_route_count,
            root_worker_task_ids: self.root_worker_task_ids.clone(),
            dependent_task_ids: self.dependent_task_ids.clone(),
            native_route_task_ids: self.native_route_task_ids.clone(),
            gates: self.gates.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

/// Safe, token-free, path-free summary of a host session suitable for operator CLI, desktop, or MCP inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostSessionSummary {
    /// Deterministic approval identifier.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Authoritative control-plane chat binding identifier (`ApprovalRecord::binding_id`).
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Approved plan identifier.
    pub plan_id: String,
    /// Approved published plan revision number.
    pub plan_revision: u64,
    /// Single software-generated runtime session binding ID inside the host's runtime supervisor.
    pub session_binding_id: Option<String>,
    /// Current host session phase.
    pub phase: HostPhase,
    /// Number of headless worker profiles (`antigravity` or `opencode_v2`).
    pub worker_profile_count: usize,
    /// Number of manual native routes (`codex_cli`).
    pub native_route_count: usize,
    /// Root headless worker task IDs (`depends_on.is_empty()`).
    pub root_worker_task_ids: Vec<String>,
    /// Dependent task IDs (`!depends_on.is_empty()`) blocked from dispatch.
    pub dependent_task_ids: Vec<String>,
    /// Manual native route task IDs (`codex_cli`) blocked from headless dispatch.
    pub native_route_task_ids: Vec<String>,
    /// Explicit execution and review gates enforced by this host session.
    pub gates: HostExecutionGates,
    /// Sanitized failure diagnostic when `phase` is [`HostPhase::Failed`].
    pub last_error: Option<String>,
}

/// Private persisted receipt sealing the mapping from an approved `task_id` and deterministic
/// `command_key` to the actual runtime `WorkerSnapshot::run_id` and `worker_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunReceipt {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Deterministic worker profile identifier.
    pub profile_id: String,
    /// Deterministic idempotency command key used with `ScopedClient::start_worker`.
    pub command_key: String,
    /// Actual software-generated `run_id` (`run_<uuid>`) returned by `ScopedClient::start_worker`.
    pub run_id: String,
    /// Actual software-generated `worker_id` (`wrk_<uuid>`) returned by `ScopedClient::start_worker`.
    pub worker_id: String,
    /// Timestamp (ms since Unix epoch) when the receipt was recorded.
    pub recorded_at_ms: i64,
}

/// High-level task execution status reported by [`crate::ApprovedHostClient`].
///
/// Successful worker runs report [`TaskExecutionStatus::ReadyForReview`]; no automatic acceptance,
/// merge, or plan `Received`/`Applied` mutation ever occurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionStatus {
    /// Root worker task has not been dispatched yet (no sealed task-to-run receipt exists).
    NotStarted,
    /// Task has non-empty `depends_on` and is blocked because predecessor code is not verified in static `base_commit`.
    DependencyInputsUnverified,
    /// Task targets `codex_cli` and requires manual native conversation routing rather than headless worker dispatch.
    NativeRouteRequired,
    /// Worker run is queued in the session supervisor.
    Queued,
    /// Worker run has been claimed by the session supervisor prior to process spawn.
    Claimed,
    /// Worker process is actively executing in its isolated task worktree.
    Running,
    /// Revision-checked cancellation has been requested; waiting for worker termination proof.
    CancelRequested,
    /// Worker exited cleanly, satisfied model/effort telemetry, and passed the diff gate (`ReadyForReview`, never auto-accepted).
    ReadyForReview,
    /// Worker failed execution, telemetry verification, or the post-run diff gate.
    Failed,
    /// Worker was proven stopped after cancellation.
    Cancelled,
    /// Worker exceeded its execution timeout budget.
    TimedOut,
    /// Worker outcome or run mapping is in an uncertain `Unknown` state (e.g. across supervisor restart) and is never auto-replayed.
    Unknown,
}

impl TaskExecutionStatus {
    /// Maps a runtime [`WorkerSnapshot`] to its corresponding [`TaskExecutionStatus`].
    pub fn from_worker_snapshot(worker: &WorkerSnapshot) -> Self {
        match worker.status {
            WorkerStatus::Queued => Self::Queued,
            WorkerStatus::Claimed => Self::Claimed,
            WorkerStatus::Running => Self::Running,
            WorkerStatus::CancelRequested => Self::CancelRequested,
            WorkerStatus::Completed => {
                let acceptance = worker
                    .outcome
                    .as_ref()
                    .map(|o| o.acceptance_state)
                    .unwrap_or(WorkerAcceptanceState::ReadyForReview);
                if acceptance == WorkerAcceptanceState::ReadyForReview {
                    Self::ReadyForReview
                } else {
                    Self::Failed
                }
            }
            WorkerStatus::Failed => Self::Failed,
            WorkerStatus::Cancelled => Self::Cancelled,
            WorkerStatus::TimedOut => Self::TimedOut,
            WorkerStatus::Unknown => Self::Unknown,
        }
    }
}

/// Scoped, token-free snapshot of an approved task's execution state returned by
/// [`crate::ApprovedHostClient::dispatch_task`], [`crate::ApprovedHostClient::get_task`], and
/// [`crate::ApprovedHostClient::cancel_task`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStateSnapshot {
    /// Deterministic approval identifier.
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Authoritative control-plane chat binding identifier.
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Runtime session binding identifier owned by the host.
    pub session_binding_id: String,
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Tool adapter frozen for this task (`antigravity`, `opencode_v2`, or `codex_cli`).
    pub adapter: ToolAdapter,
    /// Headless worker provider (`Some` for worker tasks, `None` for `codex_cli`).
    pub provider: Option<WorkerProvider>,
    /// Upstream dependency task IDs frozen in the approval record.
    pub depends_on: Vec<String>,
    /// Deterministic worker profile identifier (`Some` for worker tasks, `None` for `codex_cli`).
    pub profile_id: Option<String>,
    /// Deterministic idempotency command key (`Some` for worker tasks, `None` for `codex_cli`).
    pub command_key: Option<String>,
    /// Current task execution status (`Completed` workers map to `ReadyForReview`).
    pub status: TaskExecutionStatus,
    /// Actual software-generated `run_id` (`run_<uuid>`) from the sealed receipt, if started.
    pub run_id: Option<String>,
    /// Actual software-generated `worker_id` (`wrk_<uuid>`) from the sealed receipt, if started.
    pub worker_id: Option<String>,
    /// Compact runtime [`WorkerSnapshot`] from the session supervisor, if available.
    pub worker: Option<WorkerSnapshot>,
    /// Always `false`: worker completion means `ReadyForReview` and is never automatically accepted/merged/applied.
    pub accepted: bool,
}
