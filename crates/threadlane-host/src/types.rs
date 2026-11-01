use std::fs;
use std::path::{Component, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane::{
    NativeToolOutputConfig, WorkerAcceptanceState, WorkerSnapshot, WorkerStatus,
    MAX_CODEX_TIMEOUT_MS,
};
use threadlane_authority::ApprovalRecord;
use threadlane_core::{Binding, WakeupNotice, WakeupStatus, WorkerExecutionBinding};
use threadlane_operations::IntegrationStrategy;
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

/// Route for software-owned approved-session wake-up notification messages.
///
/// Controls only the fixed software-owned guidance text passed to the configured Codex
/// executable (`queue --thread <THREAD_UUID> --message <FIXED_WAKEUP_TEXT>`) when
/// approved-session notifications are explicitly enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HostNotificationRoute {
    /// Default MCP-oriented wake-up message (`approved_pull_events` / `approved_ack_events`).
    #[default]
    Mcp,
    /// Native operator CLI wake-up message (`threadlane-session pull` / `threadlane-session ack`).
    OperatorCli,
}

impl HostNotificationRoute {
    /// Returns the canonical `snake_case` wire identifier (`"mcp"` or `"operator_cli"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mcp => "mcp",
            Self::OperatorCli => "operator_cli",
        }
    }

    /// Returns `true` when this route is the default [`HostNotificationRoute::Mcp`] variant.
    ///
    /// Used by `#[serde(skip_serializing_if = ...)]` on [`HostNotificationSummary::route`] so
    /// disabled and default-MCP summaries remain byte-for-byte compatible with legacy manifests.
    pub const fn is_default_mcp(&self) -> bool {
        matches!(self, Self::Mcp)
    }
}

impl std::fmt::Display for HostNotificationRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for HostNotificationRoute {
    type Err = HostError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "mcp" => Ok(Self::Mcp),
            "operator-cli" | "operator_cli" => Ok(Self::OperatorCli),
            _ => Err(HostError::validation(
                "notify_route",
                format!("invalid notification route `{raw}`: expected `mcp` or `operator-cli`"),
            )),
        }
    }
}

/// Transport mechanism for approved-session wake-up notifications recorded in
/// [`HostNotificationSummary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HostNotificationTransport {
    /// Legacy Codex CLI `queue --thread <THREAD_UUID> --message <FIXED_WAKEUP_TEXT>` subprocess transport.
    #[default]
    Queue,
    /// Native Unix WebSocket JSON-RPC `turn/start` `toolOutput` transport.
    UnixToolOutput,
}

impl HostNotificationTransport {
    /// Returns the canonical `snake_case` wire identifier (`"queue"` or `"unix_tool_output"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::UnixToolOutput => "unix_tool_output",
        }
    }

    /// Returns `true` when this transport is the default [`HostNotificationTransport::Queue`] variant.
    ///
    /// Used by `#[serde(skip_serializing_if = ...)]` on [`HostNotificationSummary::transport`] so
    /// disabled and default-queue summaries remain byte-for-byte compatible with legacy manifests.
    pub const fn is_default_queue(&self) -> bool {
        matches!(self, Self::Queue)
    }
}

impl std::fmt::Display for HostNotificationTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Explicit opt-in configuration for approved-session Codex wake-up notifications on [`crate::ApprovedHost`].
///
/// Disabled by default. When configured, the executable path is canonicalized and validated as an
/// absolute regular executable file and `timeout_ms` is bounded to `1..=MAX_CODEX_TIMEOUT_MS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedHostNotificationConfig {
    /// Canonical absolute path to the validated Codex executable.
    pub executable: PathBuf,
    /// Bounded timeout in milliseconds (`1..=MAX_CODEX_TIMEOUT_MS`) for each Codex wake-up invocation.
    pub timeout_ms: u64,
    /// Fixed software-owned notification message route (defaults to [`HostNotificationRoute::Mcp`]).
    pub route: HostNotificationRoute,
    /// Optional explicit native Unix WebSocket `turn/start.toolOutput` transport configuration (`None` / queue by default).
    pub tool_output: Option<NativeToolOutputConfig>,
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
            route: HostNotificationRoute::Mcp,
            tool_output: None,
        })
    }

    /// Sets the [`HostNotificationRoute`] for this notification configuration.
    pub fn with_route(mut self, route: HostNotificationRoute) -> Self {
        self.route = route;
        self
    }

    /// Validates and attaches an explicit [`NativeToolOutputConfig`] so wake-ups use the native
    /// Unix WebSocket `turn/start.toolOutput` transport instead of spawning the legacy queue CLI.
    pub fn with_tool_output(mut self, config: NativeToolOutputConfig) -> Result<Self, HostError> {
        config.validate()?;
        self.tool_output = Some(config);
        Ok(self)
    }

    /// Returns a safe, path-free [`HostNotificationSummary`] where `executable_digest` is the
    /// 64-character lowercase hex SHA-256 of the canonical executable path string and, when native
    /// tool-output transport is configured, `native_context_digest` is `config.digest()`.
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
        let (transport, native_context_digest) = match self.tool_output {
            Some(ref cfg) => (
                HostNotificationTransport::UnixToolOutput,
                Some(cfg.digest()),
            ),
            None => (HostNotificationTransport::Queue, None),
        };
        HostNotificationSummary {
            enabled: true,
            executable_digest: Some(hex),
            timeout_ms: Some(self.timeout_ms),
            route: self.route,
            transport,
            native_context_digest,
        }
    }
}

/// Safe, path-free notification policy summary recorded in [`HostSessionManifest`] and
/// [`HostSessionSummary`].
///
/// Reports only whether host notification policy is configured for the session (never delivery
/// proof) and never contains raw local filesystem paths, model names, effort levels, or thread UUIDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HostNotificationSummary {
    /// Whether optional Codex wake-up notifications are enabled for this host session.
    pub enabled: bool,
    /// 64-character lowercase hex SHA-256 digest of the canonical Codex executable path when `enabled` is `true`; `None` when disabled.
    pub executable_digest: Option<String>,
    /// Bounded timeout in milliseconds (`1..=MAX_CODEX_TIMEOUT_MS`) when `enabled` is `true`; `None` when disabled.
    pub timeout_ms: Option<u64>,
    /// Notification message route (`mcp` by default; omitted on serialization when `mcp` for legacy wire compatibility).
    #[serde(default, skip_serializing_if = "HostNotificationRoute::is_default_mcp")]
    pub route: HostNotificationRoute,
    /// Notification transport (`queue` by default; omitted on serialization when `queue` for legacy wire compatibility).
    #[serde(
        default,
        skip_serializing_if = "HostNotificationTransport::is_default_queue"
    )]
    pub transport: HostNotificationTransport,
    /// 64-character lowercase hex SHA-256 digest of the canonical native tool-output context when `transport == UnixToolOutput`; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_context_digest: Option<String>,
}

impl HostNotificationSummary {
    /// Returns the default disabled notification summary (`enabled: false`, no digests or timeout, `route: Mcp`, `transport: Queue`).
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            executable_digest: None,
            timeout_ms: None,
            route: HostNotificationRoute::Mcp,
            transport: HostNotificationTransport::Queue,
            native_context_digest: None,
        }
    }

    /// Validates internal field consistency of this [`HostNotificationSummary`]:
    /// - When `enabled == false`, `executable_digest`, `timeout_ms`, and `native_context_digest`
    ///   must be `None`, `route` must be [`HostNotificationRoute::Mcp`], and `transport` must be
    ///   [`HostNotificationTransport::Queue`].
    /// - When `enabled == true`, `executable_digest` must be a valid 64-char lowercase hex SHA-256
    ///   digest and `timeout_ms` must be in `1..=MAX_CODEX_TIMEOUT_MS`.
    /// - When `transport == Queue`, `native_context_digest` must be `None`.
    /// - When `transport == UnixToolOutput`, `native_context_digest` must be a valid 64-char
    ///   lowercase hex SHA-256 digest.
    pub fn validate(&self) -> Result<(), HostError> {
        if !self.enabled {
            if self.executable_digest.is_some()
                || self.timeout_ms.is_some()
                || self.route != HostNotificationRoute::Mcp
                || self.transport != HostNotificationTransport::Queue
                || self.native_context_digest.is_some()
            {
                return Err(HostError::validation(
                    "notifications",
                    "disabled notifications summary must have executable_digest=None, timeout_ms=None, route=mcp, transport=queue, and native_context_digest=None",
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

        match self.transport {
            HostNotificationTransport::Queue => {
                if self.native_context_digest.is_some() {
                    return Err(HostError::validation(
                        "notifications.native_context_digest",
                        "queue notifications summary must not include native_context_digest",
                    ));
                }
            }
            HostNotificationTransport::UnixToolOutput => {
                let Some(ref native_digest) = self.native_context_digest else {
                    return Err(HostError::validation(
                        "notifications.native_context_digest",
                        "unix_tool_output notifications summary requires native_context_digest",
                    ));
                };
                validate_hex_digest("notifications.native_context_digest", native_digest)?;
            }
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
            notification_delivery: None,
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
    /// Optional bounded, path-free live notification delivery status when queried against the host runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification_delivery: Option<HostNotificationDeliveryStatus>,
    /// Sanitized failure diagnostic when `phase` is [`HostPhase::Failed`].
    pub last_error: Option<String>,
}

/// Availability of live runtime notification state for an approved host session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostNotificationAvailability {
    /// Authenticated runtime supervisor is live, holds `host.lock`, and matched the manifest's `instance_id` and owner scope.
    Live,
    /// Host session is stopped, failed, bootstrapping, or its runtime supervisor is not reachable; live wake-up state is unavailable.
    Unavailable,
}

impl HostNotificationAvailability {
    /// Returns the canonical `snake_case` wire label (`"live"` or `"unavailable"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Sanitized wake-up delivery status reported for an approved host session.
///
/// Never fakes `Idle` when the host runtime is stopped or unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostNotificationWakeupStatus {
    /// Live runtime verified: no unacknowledged important events are waiting (`WakeupStatus::Idle`).
    Idle,
    /// Live runtime verified: important events are queued for wake-up delivery (`WakeupStatus::Pending`).
    Pending,
    /// Live runtime verified: a wake-up claim is currently in flight (`WakeupStatus::InFlight`).
    InFlight,
    /// Live runtime verified: a wake-up claim was submitted and awaits event acknowledgement (`WakeupStatus::Submitted`).
    Submitted,
    /// Live runtime verified: pre-send wake-up check or spawn failed before crossing the delivery boundary (`WakeupStatus::Failed`).
    Failed,
    /// Live runtime verified: wake-up delivery outcome is unknown after crossing the send boundary (`WakeupStatus::Unknown`).
    Unknown,
    /// Host runtime is stopped or unavailable; live wake-up status cannot be determined.
    Unavailable,
}

impl HostNotificationWakeupStatus {
    /// Converts a core [`WakeupStatus`] observed on a live runtime supervisor.
    pub const fn from_core(status: WakeupStatus) -> Self {
        match status {
            WakeupStatus::Idle => Self::Idle,
            WakeupStatus::Pending => Self::Pending,
            WakeupStatus::InFlight => Self::InFlight,
            WakeupStatus::Submitted => Self::Submitted,
            WakeupStatus::Failed => Self::Failed,
            WakeupStatus::Unknown => Self::Unknown,
        }
    }

    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Submitted => "submitted",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Sanitized, fixed machine-readable error class describing a wake-up failure or uncertainty.
///
/// Raw error text, filesystem paths, and process outputs are never exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostNotificationErrorClass {
    /// Wake-up executable or tool-output configuration failed validation before send.
    ConfigurationInvalid,
    /// Wake-up executable could not be stat'd, was not a regular executable file, or was a symlink before spawn.
    ExecutableValidationFailed,
    /// Wake-up child process failed to spawn before any notification was sent.
    SpawnFailed,
    /// Native tool-output socket or peer validation failed before send.
    TransportValidationFailed,
    /// Native tool-output socket connection failed before send.
    TransportConnectFailed,
    /// Native tool-output pre-send protocol check (`thread/read`) was rejected before send.
    TransportProtocolRejected,
    /// Pre-send wake-up failure not matching a more specific pre-send class.
    PreSendFailure,
    /// Wake-up process or transport exchange timed out after crossing the send boundary (`Unknown`).
    DeliveryTimedOut,
    /// Wake-up process exited with a non-zero or ambiguous status after spawn (`Unknown`).
    ProcessTerminatedAmbiguously,
    /// An in-flight wake-up claim was marked `Unknown` during runtime reopen recovery.
    InFlightInterruptedOnReopen,
    /// Wake-up delivery outcome is unknown after crossing the send boundary (`Unknown`).
    DeliveryOutcomeUnknown,
    /// Host session is stopped or not running, so live runtime wake-up state is unavailable.
    HostUnavailable,
}

impl HostNotificationErrorClass {
    /// Returns the canonical `snake_case` wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigurationInvalid => "configuration_invalid",
            Self::ExecutableValidationFailed => "executable_validation_failed",
            Self::SpawnFailed => "spawn_failed",
            Self::TransportValidationFailed => "transport_validation_failed",
            Self::TransportConnectFailed => "transport_connect_failed",
            Self::TransportProtocolRejected => "transport_protocol_rejected",
            Self::PreSendFailure => "pre_send_failure",
            Self::DeliveryTimedOut => "delivery_timed_out",
            Self::ProcessTerminatedAmbiguously => "process_terminated_ambiguously",
            Self::InFlightInterruptedOnReopen => "in_flight_interrupted_on_reopen",
            Self::DeliveryOutcomeUnknown => "delivery_outcome_unknown",
            Self::HostUnavailable => "host_unavailable",
        }
    }

    /// Classifies a core [`WakeupStatus`] and optional raw `last_error` string into a fixed,
    /// path-free, secret-free [`HostNotificationErrorClass`].
    pub fn from_wakeup(status: WakeupStatus, last_error: Option<&str>) -> Option<Self> {
        let raw = last_error.unwrap_or_default().to_ascii_lowercase();
        match status {
            WakeupStatus::Idle
            | WakeupStatus::Pending
            | WakeupStatus::InFlight
            | WakeupStatus::Submitted => None,
            WakeupStatus::Failed => {
                if raw.contains("configuration validation failed")
                    || raw.contains("project_root validation failed")
                {
                    Some(Self::ConfigurationInvalid)
                } else if raw.contains("executable")
                    || raw.contains("symlink")
                    || raw.contains("regular file")
                    || raw.contains("permission")
                {
                    if raw.contains("failed to spawn") {
                        Some(Self::SpawnFailed)
                    } else {
                        Some(Self::ExecutableValidationFailed)
                    }
                } else if raw.contains("failed to spawn") {
                    Some(Self::SpawnFailed)
                } else if raw.contains("socket validation failed")
                    || raw.contains("peer verification failed")
                {
                    Some(Self::TransportValidationFailed)
                } else if raw.contains("failed to connect") {
                    Some(Self::TransportConnectFailed)
                } else if raw.contains("thread/read") || raw.contains("rejected") {
                    Some(Self::TransportProtocolRejected)
                } else {
                    Some(Self::PreSendFailure)
                }
            }
            WakeupStatus::Unknown => {
                if raw.contains("timed out") {
                    Some(Self::DeliveryTimedOut)
                } else if raw.contains("reopen") {
                    Some(Self::InFlightInterruptedOnReopen)
                } else if raw.contains("exited with failure")
                    || raw.contains("terminated with ambiguous")
                    || raw.contains("failed to wait")
                {
                    Some(Self::ProcessTerminatedAmbiguously)
                } else {
                    Some(Self::DeliveryOutcomeUnknown)
                }
            }
        }
    }
}

/// Bounded, path-free, secret-free live notification delivery status and cursor state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostNotificationDeliveryStatus {
    /// Whether live authenticated runtime status is available (`live` or `unavailable`).
    pub availability: HostNotificationAvailability,
    /// Current wake-up delivery status (`idle`, `pending`, `in_flight`, `submitted`, `failed`, `unknown`, or `unavailable`).
    pub wakeup_status: HostNotificationWakeupStatus,
    /// Active or retained wake-up claim identifier (`wcl_<uuid>`) when present on the runtime notice.
    pub claim_id: Option<String>,
    /// Highest event sequence number delivered via `pull_events` (`None` when runtime is unavailable).
    pub delivered_seq: Option<u64>,
    /// Highest event sequence number acknowledged via `ack_events` (`None` when runtime is unavailable).
    pub acked_seq: Option<u64>,
    /// First unacknowledged important event sequence number (`0` when none; `None` when runtime is unavailable).
    pub pending_since_seq: Option<u64>,
    /// Latest important event sequence number recorded on the binding (`0` when none; `None` when runtime is unavailable).
    pub latest_important_seq: Option<u64>,
    /// Highest important event sequence number covered by the current/retained claim (`0` when none; `None` when runtime is unavailable).
    pub covered_up_to_seq: Option<u64>,
    /// Number of important events coalesced into the current/retained notice (`None` when runtime is unavailable).
    pub coalesced_count: Option<u64>,
    /// Cumulative wake-up claim attempt count on this binding (`None` when runtime is unavailable).
    pub attempt_count: Option<u64>,
    /// Timestamp (ms since Unix epoch) when the current/retained claim was acquired (`None` when none or unavailable).
    pub claimed_at_ms: Option<i64>,
    /// Timestamp (ms since Unix epoch) when the current/retained claim resolved (`None` when none or unavailable).
    pub resolved_at_ms: Option<i64>,
    /// Whether unacknowledged important events exist (`latest_important_seq > acked_seq`; `None` when runtime is unavailable).
    pub unacked_important_events: Option<bool>,
    /// Whether this notice is eligible for explicit `rearm-notification` (`true` only when `availability == Live`, `wakeup_status == Failed`, `claim_id.is_some()`, and `latest_important_seq > acked_seq`).
    pub rearm_eligible: bool,
    /// Whether operator reconciliation via `pull` + `ack` is needed (`true` when `wakeup_status` is `Unknown` or `Submitted`, or unacknowledged important events exist).
    pub reconcile_by_pull_ack_required: bool,
    /// Sanitized fixed error classification when `wakeup_status` is `Failed`, `Unknown`, or `Unavailable` (`None` otherwise).
    pub error_class: Option<HostNotificationErrorClass>,
}

impl HostNotificationDeliveryStatus {
    /// Projects a verified live runtime [`Binding`] and [`WakeupNotice`] into a bounded, path-free [`HostNotificationDeliveryStatus`].
    pub fn from_live(binding: &Binding, wakeup: &WakeupNotice) -> Self {
        let unacked_important = wakeup.latest_important_seq > binding.acked_seq;
        let rearm_eligible =
            wakeup.status == WakeupStatus::Failed && wakeup.claim_id.is_some() && unacked_important;
        let reconcile_by_pull_ack_required = matches!(
            wakeup.status,
            WakeupStatus::Unknown | WakeupStatus::Submitted
        ) || unacked_important;
        Self {
            availability: HostNotificationAvailability::Live,
            wakeup_status: HostNotificationWakeupStatus::from_core(wakeup.status),
            claim_id: wakeup.claim_id.clone(),
            delivered_seq: Some(binding.delivered_seq),
            acked_seq: Some(binding.acked_seq),
            pending_since_seq: wakeup.pending_since_seq,
            latest_important_seq: Some(wakeup.latest_important_seq),
            covered_up_to_seq: Some(wakeup.covered_up_to_seq),
            coalesced_count: Some(wakeup.coalesced_count),
            attempt_count: Some(wakeup.attempt_count),
            claimed_at_ms: wakeup.claimed_at_ms,
            resolved_at_ms: wakeup.resolved_at_ms,
            unacked_important_events: Some(unacked_important),
            rearm_eligible,
            reconcile_by_pull_ack_required,
            error_class: HostNotificationErrorClass::from_wakeup(
                wakeup.status,
                wakeup.last_error.as_deref(),
            ),
        }
    }

    /// Constructs the truthful unavailable status reported when the host session is stopped,
    /// failed, bootstrapping, or its runtime supervisor is unreachable. Never fakes `Idle`.
    pub const fn unavailable() -> Self {
        Self {
            availability: HostNotificationAvailability::Unavailable,
            wakeup_status: HostNotificationWakeupStatus::Unavailable,
            claim_id: None,
            delivered_seq: None,
            acked_seq: None,
            pending_since_seq: None,
            latest_important_seq: None,
            covered_up_to_seq: None,
            coalesced_count: None,
            attempt_count: None,
            claimed_at_ms: None,
            resolved_at_ms: None,
            unacked_important_events: None,
            rearm_eligible: false,
            reconcile_by_pull_ack_required: false,
            error_class: Some(HostNotificationErrorClass::HostUnavailable),
        }
    }
}

/// Safe, token-free, path-free notification delivery status report for an approved host session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostNotificationStatusReport {
    /// Deterministic approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Authoritative control-plane chat binding identifier (`ApprovalRecord::binding_id`).
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Single software-generated runtime session binding ID inside the host's runtime supervisor.
    pub session_binding_id: Option<String>,
    /// Public per-start host instance identifier (`hst_<32hex>`), or `None` for legacy manifests.
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Current host session phase recorded in `host-manifest.json`.
    pub phase: HostPhase,
    /// Configured host notification policy summary (`enabled`, digests, timeout, route, transport).
    pub notifications: HostNotificationSummary,
    /// Live or unavailable notification delivery state and reconciliation cursors.
    pub delivery: HostNotificationDeliveryStatus,
}

/// Safe, token-free, path-free report returned after explicitly re-arming a `Failed` notification claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostNotificationRearmReport {
    /// Deterministic approval identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// 64-character lowercase hex SHA-256 digest of the frozen approval subject.
    pub subject_digest: String,
    /// Authoritative control-plane chat binding identifier (`ApprovalRecord::binding_id`).
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native Codex thread UUID.
    pub thread_uuid: String,
    /// Single software-generated runtime session binding ID inside the host's runtime supervisor.
    pub session_binding_id: String,
    /// Verified live host instance identifier (`hst_<32hex>`).
    pub instance_id: String,
    /// Current host session phase (`Ready`).
    pub phase: HostPhase,
    /// Configured host notification policy summary.
    pub notifications: HostNotificationSummary,
    /// The `Failed` claim identifier (`wcl_<uuid>`) that was atomically verified and cleared.
    pub rearmed_from_claim_id: String,
    /// Post-rearm notification delivery state (`Pending`, `claim_id: None`) immediately committed by the core transaction.
    pub delivery: HostNotificationDeliveryStatus,
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

/// Maximum directory entries inspected when scanning `<work_area_dir>/manifests/receipts` during
/// read-only task lifecycle projection.
pub const MAX_RECEIPT_SCAN_ENTRIES: usize = 256;

/// Maximum UTF-8 byte length for [`TaskLifecycleProjection::verification_detail`].
pub const MAX_LIFECYCLE_DETAIL_BYTES: usize = 256;

/// Read-only review lifecycle stage projected for an approved task.
///
/// Separate from [`TaskExecutionStatus`], which preserves `ReadyForReview` after worker completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskReviewLifecycleStatus {
    /// Task has not yet reached [`TaskExecutionStatus::ReadyForReview`].
    NotReady,
    /// Worker completed at [`TaskExecutionStatus::ReadyForReview`] and the live task checkout is
    /// awaiting an operator review receipt.
    PendingReview,
    /// A valid [`threadlane_operations::ReviewReceipt`] is verified against the live task checkout
    /// subject and frozen task/run/policy identity.
    Accepted,
    /// A valid [`threadlane_operations::ReviewReceipt`] and Git commit/tree ancestry are verified
    /// in the repository, but the task checkout directory was cleaned up and is no longer present
    /// for live working-tree inspection.
    HistoricalAccepted,
    /// Review receipt or task worktree evidence failed verification (wrong scope/digest/model/policy,
    /// dirty worktree or moved `HEAD` after review, missing commit, corrupt/symlink/foreign receipt
    /// file, or missing checkout before review).
    Invalidated,
}

/// Read-only branch integration lifecycle stage projected for an approved task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskIntegrationLifecycleStatus {
    /// Task is not yet reviewed or has only historical review without integration.
    NotIntegrated,
    /// Review is [`TaskReviewLifecycleStatus::Accepted`] on a live task checkout and ready for
    /// operator integration into the run integration branch.
    ReadyToIntegrate,
    /// A valid [`threadlane_operations::IntegrationReceipt`] is verified against the accepted
    /// review receipt, live task checkout, and integration ancestry.
    Integrated,
    /// A valid [`threadlane_operations::IntegrationReceipt`] is verified against the accepted
    /// review receipt and repository/integration ancestry after task checkout cleanup.
    HistoricalIntegrated,
    /// Integration receipt, target worktree, or underlying review evidence failed verification.
    Invalidated,
}

/// Read-only evidence verification status projected for an approved task's review and integration
/// lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskEvidenceVerificationStatus {
    /// Task has not yet reached [`TaskExecutionStatus::ReadyForReview`].
    NotApplicable,
    /// Live task worktree checkout is present and passes post-run diff gate checks; awaiting an
    /// operator review receipt.
    PendingReview,
    /// Active receipts and worktree checkouts are present on disk and verified live against Git
    /// state and frozen task policy.
    VerifiedLive,
    /// Task worktree checkout was cleaned up; receipts and Git commit/tree ancestry are verified
    /// from recorded history in the repository, with explicit historical verification limits.
    RecordedHistoricalOnly,
    /// Evidence is stale, mismatched, tampered, dirty, foreign, symlinked, or exceeded scan bounds.
    Invalidated,
}

/// Path-free, token-free summary of a verified [`threadlane_operations::ReviewReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedReviewReceiptSummary {
    /// Unique software-assigned review receipt identifier (`review-<uuid>`).
    pub receipt_id: String,
    /// 64-character lowercase hex SHA-256 digest of the canonical [`threadlane_operations::ReviewSubject`].
    pub subject_digest: String,
    /// 64-character lowercase hex SHA-256 digest of the [`threadlane_operations::ReviewReceipt`].
    pub receipt_digest: String,
    /// Validated task worktree identifier (`wt-task-...`).
    pub task_worktree_id: String,
    /// Exact 40-character lowercase hex base commit SHA of the task worktree.
    pub base_commit: String,
    /// Exact 40-character lowercase hex `HEAD` commit SHA accepted in the review subject.
    pub task_head_commit: String,
    /// Exact 40-character lowercase hex root tree SHA of `task_head_commit`.
    pub task_tree_sha: String,
    /// 64-character lowercase hex SHA-256 digest of the task's verified diff.
    pub diff_digest: String,
    /// Non-empty identifier of the human reviewer or operator that accepted the subject.
    pub reviewer_id: String,
    /// Unix epoch timestamp in milliseconds when the review receipt was issued.
    pub approved_at_ms: i64,
}

/// Path-free, token-free summary of a verified [`threadlane_operations::IntegrationReceipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedIntegrationReceiptSummary {
    /// Unique software-assigned integration receipt identifier (`integration-<uuid>`).
    pub receipt_id: String,
    /// 64-character lowercase hex SHA-256 digest of the [`threadlane_operations::IntegrationReceipt`].
    pub receipt_digest: String,
    /// Exact `receipt_id` of the bound [`threadlane_operations::ReviewReceipt`].
    pub review_receipt_id: String,
    /// Validated run-level integration worktree identifier (`wt-run-...`).
    pub integration_worktree_id: String,
    /// Deterministic merge strategy used (`fast_forward_only` or `merge_no_ff`).
    pub strategy: IntegrationStrategy,
    /// Exact 40-character lowercase hex accepted task `HEAD` commit SHA that was integrated.
    pub accepted_task_head_commit: String,
    /// Exact 40-character lowercase hex `HEAD` commit SHA of the integration branch after integration.
    pub integrated_target_commit: String,
    /// Exact 40-character lowercase hex root tree SHA of `integrated_target_commit`.
    pub integrated_tree_sha: String,
    /// Non-empty identifier of the operator that executed the integration.
    pub operator_id: String,
    /// Unix epoch timestamp in milliseconds when the integration completed.
    pub integrated_at_ms: i64,
}

/// Truthful, read-only review and integration lifecycle projection joined from software-owned
/// session, run, worktree, subject, and receipt identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskLifecycleProjection {
    /// Current review lifecycle stage for this task.
    pub review_status: TaskReviewLifecycleStatus,
    /// Current branch integration lifecycle stage for this task.
    pub integration_status: TaskIntegrationLifecycleStatus,
    /// Current evidence verification status for this task.
    pub evidence_status: TaskEvidenceVerificationStatus,
    /// Whether the isolated task worktree checkout directory is currently present on disk.
    pub task_checkout_present: bool,
    /// Whether the run-level integration worktree checkout directory is currently present on disk.
    pub integration_checkout_present: bool,
    /// Verified review receipt summary when a review receipt was verified (live or historical).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_receipt: Option<VerifiedReviewReceiptSummary>,
    /// Verified integration receipt summary when an integration receipt was verified (live or historical).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_receipt: Option<VerifiedIntegrationReceiptSummary>,
    /// Sanitized single-line explanation of verification limits (such as post-cleanup historical
    /// state) or evidence invalidation reasons.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_detail: Option<String>,
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
    /// Separate read-only review and integration lifecycle projection joined from verified
    /// worktree/subject/receipt state (`None` when omitted on legacy payloads).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<TaskLifecycleProjection>,
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
