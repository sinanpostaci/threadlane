use std::fs;
use std::path::{Component, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane::{WorkerAcceptanceState, WorkerSnapshot, WorkerStatus, MAX_CODEX_TIMEOUT_MS};
use threadlane_authority::ApprovalRecord;
use threadlane_core::WorkerExecutionBinding;
use threadlane_registry::ToolAdapter;
use threadlane_workers::{validate_executable_file, DependencyInputSeal, WorkerProvider};

use crate::error::HostError;
use crate::storage::{ensure_no_symlink_components, validate_hex_digest};

/// Current on-disk schema version for `host-manifest.json`.
pub const HOST_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Current on-disk schema version for `task-receipts.json`.
pub const TASK_RECEIPTS_SCHEMA_VERSION: u32 = 1;

/// Current on-disk schema version for `<sessions_dir>/.launch-intents/<approval_id>.json`.
pub const HOST_LAUNCH_INTENT_SCHEMA_VERSION: u32 = 1;

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

/// Subdirectory name for private (`0700`) durable per-approval initial host launch intents inside `sessions_dir`.
pub const LAUNCH_INTENTS_SUBDIR: &str = ".launch-intents";

/// Maximum permitted byte size when reading or writing `host-manifest.json` (`64 KiB`).
pub const MAX_HOST_MANIFEST_BYTES: usize = 65_536;

/// Maximum permitted byte size when reading or writing `task-receipts.json` (`64 KiB`).
pub const MAX_TASK_RECEIPTS_BYTES: usize = 65_536;

/// Maximum permitted byte size when reading or writing `<sessions_dir>/.launch-intents/<approval_id>.json` (`16 KiB`).
pub const MAX_LAUNCH_INTENT_BYTES: usize = 16_384;

/// Default and maximum event batch count (`20`) for [`crate::ApprovedHostClient::pull_events`].
pub const APPROVED_EVENT_BATCH_MAX_EVENTS: usize = 20;

/// Maximum serialized JSON byte budget (`49_152` bytes / `48 KiB`) for [`crate::ApprovedHostClient::pull_events`].
pub const APPROVED_EVENT_BATCH_MAX_BYTES: usize = 49_152;

/// Explicit opt-in configuration for approved-session Codex wake-up notifications on [`crate::ApprovedHost`].
///
/// Disabled by default. When configured, the executable path is canonicalized and validated as an
/// absolute regular executable file and `timeout_ms` is bounded to `1..=MAX_CODEX_TIMEOUT_MS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedHostNotificationConfig {
    /// Canonical absolute path to the validated Codex executable.
    pub executable: PathBuf,
    /// Bounded timeout in milliseconds (`1..=MAX_CODEX_TIMEOUT_MS`) for each Codex queue invocation.
    pub timeout_ms: u64,
}

impl ApprovedHostNotificationConfig {
    /// Canonicalizes trusted launcher symlinks (such as `/bin/sh` on NixOS) and validates that the
    /// resulting canonical path is an absolute non-symlink regular executable file and that
    /// `timeout_ms` is within `1..=MAX_CODEX_TIMEOUT_MS`.
    pub fn new(executable: PathBuf, timeout_ms: u64) -> Result<Self, HostError> {
        if executable.as_os_str().is_empty() {
            return Err(HostError::validation(
                "executable",
                "codex executable path must not be empty",
            ));
        }
        for comp in executable.components() {
            if matches!(comp, Component::CurDir | Component::ParentDir) {
                return Err(HostError::validation(
                    "executable",
                    format!(
                        "codex executable path `{}` must not contain `.` or `..` traversal components",
                        executable.display()
                    ),
                ));
            }
        }
        if !executable.is_absolute() {
            return Err(HostError::validation(
                "executable",
                format!(
                    "codex executable path `{}` must be an absolute path",
                    executable.display()
                ),
            ));
        }
        if timeout_ms == 0 || timeout_ms > MAX_CODEX_TIMEOUT_MS {
            return Err(HostError::validation(
                "timeout_ms",
                format!("notify timeout_ms must be between 1 and {MAX_CODEX_TIMEOUT_MS} ms"),
            ));
        }

        let canonical = fs::canonicalize(&executable).map_err(|err| {
            HostError::validation(
                "executable",
                format!(
                    "failed to canonicalize codex executable `{}`: {err}",
                    executable.display()
                ),
            )
        })?;
        let checked = ensure_no_symlink_components(&canonical)?;
        let validated = validate_executable_file(&checked).map_err(|err| {
            HostError::validation(
                "executable",
                format!(
                    "canonical target `{}` (resolved from `{}`) is not a valid regular executable: {err}",
                    checked.display(),
                    executable.display()
                ),
            )
        })?;

        Ok(Self {
            executable: validated,
            timeout_ms,
        })
    }

    /// Returns a safe, path-free [`HostNotificationSummary`] where `executable_digest` is the
    /// 64-character lowercase hex SHA-256 of the canonical executable path string.
    pub fn summary(&self) -> HostNotificationSummary {
        let canonical_str = fs::canonicalize(&self.executable)
            .unwrap_or_else(|_| self.executable.clone())
            .to_string_lossy()
            .into_owned();
        let mut hasher = Sha256::new();
        hasher.update(canonical_str.as_bytes());
        let digest = hasher.finalize();
        let mut hex = String::with_capacity(64);
        for byte in &digest {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{byte:02x}");
        }
        HostNotificationSummary {
            enabled: true,
            executable_digest: Some(hex),
            timeout_ms: Some(self.timeout_ms),
        }
    }
}

/// Safe, path-free notification policy summary recorded in [`HostSessionManifest`] and
/// [`HostSessionSummary`].
///
/// Reports only whether host notification policy is configured for the session (never delivery
/// proof) and never contains raw local filesystem paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HostNotificationSummary {
    /// Whether optional Codex wake-up notifications are enabled for this host session.
    pub enabled: bool,
    /// 64-character lowercase hex SHA-256 digest of the canonical Codex executable path when `enabled` is `true`; `None` when disabled.
    pub executable_digest: Option<String>,
    /// Bounded timeout in milliseconds (`1..=MAX_CODEX_TIMEOUT_MS`) when `enabled` is `true`; `None` when disabled.
    pub timeout_ms: Option<u64>,
}

impl HostNotificationSummary {
    /// Returns the default disabled notification summary (`enabled: false`, no digest or timeout).
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            executable_digest: None,
            timeout_ms: None,
        }
    }

    /// Validates internal field consistency of this [`HostNotificationSummary`]:
    /// - When `enabled == false`, both `executable_digest` and `timeout_ms` must be `None`.
    /// - When `enabled == true`, `executable_digest` must be a valid 64-char lowercase hex SHA-256
    ///   digest and `timeout_ms` must be in `1..=MAX_CODEX_TIMEOUT_MS`.
    pub fn validate(&self) -> Result<(), HostError> {
        if !self.enabled {
            if self.executable_digest.is_some() || self.timeout_ms.is_some() {
                return Err(HostError::validation(
                    "notifications",
                    "disabled notifications summary must have executable_digest=None and timeout_ms=None",
                ));
            }
            return Ok(());
        }

        let Some(ref digest) = self.executable_digest else {
            return Err(HostError::validation(
                "notifications.executable_digest",
                "enabled notifications summary requires executable_digest",
            ));
        };
        validate_hex_digest("notifications.executable_digest", digest)?;

        let Some(timeout_ms) = self.timeout_ms else {
            return Err(HostError::validation(
                "notifications.timeout_ms",
                "enabled notifications summary requires timeout_ms",
            ));
        };
        if timeout_ms == 0 || timeout_ms > MAX_CODEX_TIMEOUT_MS {
            return Err(HostError::validation(
                "notifications.timeout_ms",
                format!("notifications timeout_ms must be between 1 and {MAX_CODEX_TIMEOUT_MS} ms"),
            ));
        }

        Ok(())
    }
}

/// Lifecycle phase recorded in a per-session [`HostSessionManifest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostPhase {
    /// Host holds `host.lock` and is executing the two-phase supervisor bootstrap and binding recovery.
    Bootstrapping,
    /// Final supervisor is running with frozen `WorkersConfig` and authenticated readiness was verified.
    Ready,
    /// Host has accepted an idle stop request or initiated graceful shutdown and is draining before writing `Stopped`.
    Stopping,
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
            Self::Stopping => "stopping",
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
    /// `false` on M5B sessions supporting receipt-sealed dependent tasks; `true` on legacy root-only sessions.
    pub root_tasks_only: bool,
    /// `false` on M5B sessions supporting receipt-sealed dependent tasks; `true` on legacy root-only sessions.
    pub dependent_tasks_blocked: bool,
    /// `true` on M5B sessions: dependent tasks require a verified receipt-proven `DependencyInputSeal` before dispatch.
    #[serde(default)]
    pub dependency_inputs_required: bool,
    /// Always `true`: `codex_cli` native routes are manual-only and rejected for worker dispatch with `native_route_required`.
    pub native_routes_manual_only: bool,
    /// Always `true`: worker `Completed` maps at most to `ReadyForReview`.
    pub worker_completed_means_ready_for_review: bool,
    /// Always `false`: host and workers never automatically accept, merge, push, or mark plans `Received`/`Applied`.
    pub auto_accept_or_merge_enabled: bool,
}

impl HostExecutionGates {
    /// Constructs the M5B execution gates for the given readiness state.
    pub const fn for_ready_state(supervisor_authenticated_ready: bool) -> Self {
        Self {
            supervisor_authenticated_ready,
            workers_auto_started_on_boot: false,
            root_tasks_only: false,
            dependent_tasks_blocked: false,
            dependency_inputs_required: true,
            native_routes_manual_only: true,
            worker_completed_means_ready_for_review: true,
            auto_accept_or_merge_enabled: false,
        }
    }

    /// Constructs the legacy root-only execution gates for backward compatibility with older manifests.
    pub const fn legacy_for_ready_state(supervisor_authenticated_ready: bool) -> Self {
        Self {
            supervisor_authenticated_ready,
            workers_auto_started_on_boot: false,
            root_tasks_only: true,
            dependent_tasks_blocked: true,
            dependency_inputs_required: false,
            native_routes_manual_only: true,
            worker_completed_means_ready_for_review: true,
            auto_accept_or_merge_enabled: false,
        }
    }

    /// Returns `true` if this gate tuple represents a legacy root-only session that does not support
    /// prepared dependency inputs.
    pub const fn is_legacy_root_only(&self) -> bool {
        !self.dependency_inputs_required || self.root_tasks_only || self.dependent_tasks_blocked
    }
}

/// Operator selection of an integrated predecessor task and its review/integration receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyPredecessorSelection {
    /// Approved predecessor task identifier (`depends_on` entry).
    pub task_id: String,
    /// Review receipt ID (`rev_<16hex>` or validated identifier) under `manifests/receipts/review-<id>.json`.
    pub review_receipt_id: String,
    /// Integration receipt ID (`int_<16hex>` or validated identifier) under `manifests/receipts/integration-<id>.json`.
    pub integration_receipt_id: String,
}

/// Explicit trusted-local request to verify predecessor proof and seal a dependent task's input commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareDependencyInputsRequest {
    /// Approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// Dependent task identifier (`!depends_on.is_empty()`).
    pub task_id: String,
    /// Expected 64-character lowercase hex SHA-256 subject digest of the frozen approval.
    pub expected_approval_digest: String,
    /// Exact set of direct predecessor selections matching `task.depends_on`.
    pub predecessors: Vec<DependencyPredecessorSelection>,
}

/// Path-free, token-free summary of a sealed [`DependencyInputSeal`] for operator and task snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyInputSummary {
    /// 64-character lowercase hex SHA-256 digest of the sealed dependency input (`seal_digest`).
    pub input_digest: String,
    /// Canonical 40-char lowercase hex SHA-1 of the composed dependency input commit (`execution_base_commit`).
    pub base_commit: String,
    /// Canonical 40-char lowercase hex SHA-1 of the root tree of `base_commit` (`execution_tree_sha`).
    pub tree_sha: String,
    /// Sorted direct predecessor task IDs included in this dependency input.
    pub predecessor_task_ids: Vec<String>,
}

impl DependencyInputSummary {
    /// Projects a verified [`DependencyInputSeal`] into a safe, path-free [`DependencyInputSummary`].
    pub fn from_seal(seal: &DependencyInputSeal) -> Self {
        Self {
            input_digest: seal.seal_digest.clone(),
            base_commit: seal.execution_base_commit.clone(),
            tree_sha: seal.execution_tree_sha.clone(),
            predecessor_task_ids: seal
                .predecessors
                .iter()
                .map(|p| p.task_id.clone())
                .collect(),
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
    /// Public per-start host instance identifier (`hst_<32hex>`), or `None` for legacy manifests.
    #[serde(default)]
    pub instance_id: Option<String>,
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
    /// Configured host notification policy summary (disabled by default; never implies delivery proof).
    #[serde(default)]
    pub notifications: HostNotificationSummary,
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
            instance_id: None,
            phase,
            pid,
            worker_profile_count,
            native_route_count,
            root_worker_task_ids,
            dependent_task_ids,
            native_route_task_ids,
            gates: HostExecutionGates::for_ready_state(ready),
            notifications: HostNotificationSummary::disabled(),
            last_error,
            created_at_ms,
            updated_at_ms,
        }
    }

    /// Sets the configured [`HostNotificationSummary`] on this manifest.
    pub fn with_notifications(mut self, notifications: HostNotificationSummary) -> Self {
        self.notifications = notifications;
        self
    }

    /// Sets the public per-start `instance_id` (`hst_<32hex>`) on this manifest.
    pub fn with_instance_id(mut self, instance_id: Option<String>) -> Self {
        self.instance_id = instance_id;
        self
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
            instance_id: self.instance_id.clone(),
            phase: self.phase,
            worker_profile_count: self.worker_profile_count,
            native_route_count: self.native_route_count,
            root_worker_task_ids: self.root_worker_task_ids.clone(),
            dependent_task_ids: self.dependent_task_ids.clone(),
            native_route_task_ids: self.native_route_task_ids.clone(),
            gates: self.gates.clone(),
            notifications: self.notifications.clone(),
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
    /// Public per-start host instance identifier (`hst_<32hex>`), or `None` for legacy manifests.
    #[serde(default)]
    pub instance_id: Option<String>,
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
    /// Configured host notification policy summary (disabled by default; never implies delivery proof).
    #[serde(default)]
    pub notifications: HostNotificationSummary,
    /// Sanitized failure diagnostic when `phase` is [`HostPhase::Failed`].
    pub last_error: Option<String>,
}

/// Private persisted dispatch intent recorded in `task-receipts.json` before invoking
/// `ScopedClient::start_worker` for an approved root worker task.
///
/// Committing this intent before the runtime `start_worker` effect ensures that if worker
/// startup or receipt sealing is interrupted, task inspection reports
/// [`TaskExecutionStatus::Unknown`] rather than [`TaskExecutionStatus::NotStarted`], and
/// ambiguous cancellation is refused until an explicit idempotent `dispatch_task` retry seals
/// the `task_id -> run_id` mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDispatchIntent {
    /// Approved task identifier.
    pub task_id: String,
    /// 64-character lowercase hex SHA-256 digest of the compiled task specification.
    pub task_digest: String,
    /// Deterministic worker profile identifier.
    pub profile_id: String,
    /// Deterministic idempotency command key used with `ScopedClient::start_worker`.
    pub command_key: String,
    /// Sealed execution binding (`Some` for dependent worker tasks, `None` for root worker tasks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_binding: Option<WorkerExecutionBinding>,
    /// Timestamp (ms since Unix epoch) when the dispatch intent was recorded.
    pub recorded_at_ms: i64,
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
    /// Sealed execution binding (`Some` for dependent worker tasks, `None` for root worker tasks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_binding: Option<WorkerExecutionBinding>,
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
    /// Root worker task has not been dispatched yet (no durable dispatch intent or sealed task-to-run receipt exists).
    NotStarted,
    /// Task has non-empty `depends_on` and is blocked because predecessor code is not verified in a sealed dependency input.
    DependencyInputsUnverified,
    /// Dependent worker task has a verified `DependencyInputSeal` on disk and is ready for dispatch.
    DependencyInputsReady,
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
    /// Worker outcome or run mapping is in an uncertain `Unknown` state (e.g. across supervisor restart or when a durable dispatch intent exists without a sealed receipt) and is never auto-replayed.
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
    /// Path-free summary of the verified dependency input seal for dependent tasks, if prepared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_input: Option<DependencyInputSummary>,
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

/// Lifecycle status of a durable per-approval initial host launch intent under
/// `<sessions_dir>/.launch-intents/<approval_id>.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostLaunchIntentStatus {
    /// Launch intent was persisted before spawning `threadlane-host`; startup is in progress.
    Pending,
    /// Startup timed out or exited before live authenticated `Ready` could be proven; outcome is
    /// uncertain and duplicate fresh spawns are refused.
    Unknown,
    /// The launched host session was verified to have reached live authenticated `Ready` for the
    /// exact frozen approval identity.
    ReconciledReady,
}

impl std::fmt::Display for HostLaunchIntentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Unknown => f.write_str("unknown"),
            Self::ReconciledReady => f.write_str("reconciled_ready"),
        }
    }
}

/// Bounded, private (`0600`) durable per-approval initial host launch intent persisted at
/// `<sessions_dir>/.launch-intents/<approval_id>.json` before spawning a fresh `threadlane-host`
/// process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostLaunchIntent {
    /// Schema version (`1`).
    pub schema_version: u32,
    /// Canonical approval ID (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Frozen 64-character lowercase hex SHA-256 subject digest.
    pub subject_digest: String,
    /// Frozen policy version of the approval record.
    pub policy_version: u32,
    /// Frozen control-plane `binding_id`.
    pub control_binding_id: String,
    /// Frozen canonical lowercase hyphenated `thread_uuid`.
    pub thread_uuid: String,
    /// Frozen workflow `plan_id`.
    pub plan_id: String,
    /// Frozen workflow `plan_revision` (`>= 1`).
    pub plan_revision: u64,
    /// Canonical absolute `threadlane-host` executable path used for this launch attempt.
    pub executable_path: PathBuf,
    /// Current launch intent lifecycle status.
    pub status: HostLaunchIntentStatus,
    /// Optional OS process ID of the spawned `threadlane-host` child once `spawn()` returns.
    pub spawn_pid: Option<u32>,
    /// Optional sanitized diagnostic when `status` transitions to `unknown`.
    pub last_error: Option<String>,
    /// Creation timestamp in milliseconds since Unix epoch.
    pub created_at_ms: i64,
    /// Last update timestamp in milliseconds since Unix epoch.
    pub updated_at_ms: i64,
}

impl HostLaunchIntent {
    /// Constructs a new [`HostLaunchIntentStatus::Pending`] launch intent from the frozen
    /// [`ApprovalRecord`] and canonical host executable path before spawning `threadlane-host`.
    pub fn new_pending(record: &ApprovalRecord, executable_path: PathBuf, now_ms: i64) -> Self {
        Self {
            schema_version: HOST_LAUNCH_INTENT_SCHEMA_VERSION,
            approval_id: record.approval_id.clone(),
            subject_digest: record.subject_digest.clone(),
            policy_version: record.policy_version,
            control_binding_id: record.binding_id.clone(),
            thread_uuid: record.thread_uuid.clone(),
            plan_id: record.plan_id.clone(),
            plan_revision: record.plan_revision,
            executable_path,
            status: HostLaunchIntentStatus::Pending,
            spawn_pid: None,
            last_error: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        }
    }
}
