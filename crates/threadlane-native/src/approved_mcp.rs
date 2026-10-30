use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::{model::CallToolResult, schemars};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use threadlane::ThreadlaneMcpServer;
use threadlane_authority::{ApprovalSummary, AuthorityError, AuthorityStore};
use threadlane_core::{AckResult, ActivityPage, EventBatch, EventKind};
use threadlane_host::{
    validate_sessions_root, ApprovedHostClient, HostError, HostPhase, HostSessionSummary,
};
pub use threadlane_host::{APPROVED_EVENT_BATCH_MAX_BYTES, APPROVED_EVENT_BATCH_MAX_EVENTS};
use threadlane_workflow::ensure_mcp_response_budget;

use crate::error::{NativeError, NativeErrorCode};
use crate::mcp_pair::WorkflowStoreHandles;

/// Safe static message returned when approved-task authority and host session stores are not configured.
pub const HOST_NOT_CONFIGURED_MESSAGE: &str =
    "Approved-task authority store and host sessions directory are not configured for this MCP server.";

/// Default number of approval summaries returned per `list_approvals` page.
pub const DEFAULT_APPROVAL_LIST_LIMIT: usize = 20;

/// Maximum number of approval summaries returned per `list_approvals` page.
pub const MAX_APPROVAL_LIST_LIMIT: usize = 20;

/// Default number of activity events returned per `approved_activity` page.
pub const DEFAULT_APPROVED_ACTIVITY_LIMIT: usize = 20;

/// Maximum number of activity events returned per `approved_activity` page.
pub const MAX_APPROVED_ACTIVITY_LIMIT: usize = 20;

/// Default number of events returned per `approved_pull_events` batch.
pub const DEFAULT_APPROVED_EVENT_BATCH_LIMIT: usize = APPROVED_EVENT_BATCH_MAX_EVENTS;

/// Maximum number of events returned per `approved_pull_events` batch.
pub const MAX_APPROVED_EVENT_BATCH_LIMIT: usize = APPROVED_EVENT_BATCH_MAX_EVENTS;

/// Maximum length in bytes of an optional `cancel_approved_task` cancellation reason.
pub const MAX_APPROVED_CANCEL_REASON_LEN: usize = 512;

/// Maximum length in bytes of a pagination cursor accepted by `list_approvals` or `approved_activity`.
pub const MAX_APPROVAL_CURSOR_LEN: usize = 256;

const APPROVAL_CURSOR_PREFIX: &str = "tlapr1";

const INVALID_CURSOR_MESSAGE: &str =
    "Pagination cursor is invalid or does not belong to this chat binding.";

/// Shared operator-selected [`AuthorityStore`] handle and canonical sessions root directory.
pub type HostStoreHandles = (Arc<AuthorityStore>, PathBuf);

/// Parameters for the `list_approvals` MCP tool.
///
/// Lists a bounded page of safe operator approval summaries belonging exclusively to the enrolled
/// `(control_binding_id, native_thread_uuid)`. Rejects caller-supplied identity fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListApprovalsParams {
    /// Optional opaque pagination cursor returned by a previous `list_approvals` page (or an `approval_id` belonging to this chat binding).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional `approval_id` anchor belonging to this chat binding's own result set to start after (mutually exclusive with `cursor`).
    #[serde(default)]
    pub after_id: Option<String>,
    /// Optional maximum number of approval summaries to return (`1..=20`, defaults to `20`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Parameters for the `approved_session` MCP tool.
///
/// Queries live authenticated host readiness and session metadata for an operator approval record
/// owned by the enrolled `(control_binding_id, native_thread_uuid)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovedSessionParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
}

/// Parameters for the `start_approved_task` MCP tool.
///
/// Accepts only `approval_id` and `task_id`. Frozen task instructions, worker profile IDs, CLI
/// executables, model assignments, write scopes, and deterministic idempotency keys are loaded
/// exclusively from the operator-approved [`AuthorityStore`] record inside [`ApprovedHostClient`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StartApprovedTaskParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Approved root worker task identifier within `approval_id`.
    pub task_id: String,
}

/// Parameters for the `approved_task` MCP tool.
///
/// Reads the current execution status of an approved task within `approval_id` for the enrolled
/// `(control_binding_id, native_thread_uuid)`. Completed workers report `ready_for_review` (`accepted: false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovedTaskParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Approved task identifier within `approval_id`.
    pub task_id: String,
}

/// Parameters for the `cancel_approved_task` MCP tool.
///
/// Requests revision-checked cancellation of an active approved root worker task for the enrolled
/// `(control_binding_id, native_thread_uuid)` without granting any new execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelApprovedTaskParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Approved root worker task identifier within `approval_id`.
    pub task_id: String,
    /// Expected current 1-based revision of the worker run.
    pub expected_revision: u64,
    /// Optional bounded cancellation reason (`<= 512` bytes).
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpApprovedEventKind {
    RunStarted,
    RunProgress,
    QuestionAsked,
    QuestionAnswered,
    QuestionSuperseded,
    RunSteered,
    RunCompleted,
    RunCancelled,
    WorkerQueued,
    WorkerStarted,
    WorkerProgress,
    WorkerCancelRequested,
    WorkerCompleted,
    WorkerFailed,
    WorkerCancelled,
    WorkerTimedOut,
    WorkerUnknown,
}

/// Parameters for the `approved_activity` MCP tool.
///
/// Lists a read-only bounded page (`<= 20`) of durable activity events from the separate approved
/// host session for `approval_id`, optionally filtered to a sealed `task_id` and [`EventKind`].
/// Never accepts caller-supplied `run_id`, `worker_id`, `binding_id`, `session_binding_id`,
/// `thread_id`, or filesystem paths.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovedActivityParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Optional approved root worker task identifier filter (resolved to its sealed `run_id` by the host client).
    #[serde(default)]
    pub task_id: Option<String>,
    /// Optional sequence position (`seq`) to start after on the first page (mutually exclusive with `cursor`).
    #[serde(default)]
    pub after_seq: Option<u64>,
    /// Optional opaque keyset cursor returned by a previous `approved_activity` page (mutually exclusive with `after_seq`).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional maximum number of activity events to return (`1..=20`, defaults to `20`).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Optional durable event classification filter.
    #[serde(default)]
    #[schemars(with = "Option<McpApprovedEventKind>")]
    pub kind: Option<EventKind>,
}

/// Parameters for the `approved_pull_events` MCP tool.
///
/// Pulls a bounded ordered batch (`<= 20` events, `<= 49_152` serialized bytes) of durable
/// events from the separate approved host session for `approval_id` owned by the enrolled
/// `(control_binding_id, native_thread_uuid)`. Advances `delivered_seq` on the approved host's
/// internal `session_binding_id` only after the prefix fits within the byte and count budgets.
/// Never accepts caller-supplied identity, path, run, task, kind, model, policy, or key fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovedPullEventsParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Optional sequence number (`<= delivered_seq`) to pull events after (`None` defaults to `acked_seq`).
    #[serde(default)]
    pub after_seq: Option<u64>,
    /// Optional maximum number of events to pull (`1..=20`, defaults to `20`).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Parameters for the `approved_ack_events` MCP tool.
///
/// Explicitly acknowledges receipt and processing of a previously delivered `batch_id` from
/// the separate approved host session for `approval_id` owned by the enrolled
/// `(control_binding_id, native_thread_uuid)`. Deterministic idempotency keys are derived
/// internally by [`ApprovedHostClient::ack_events`]; caller-supplied `command_key`, identity,
/// path, run, task, kind, model, or policy fields are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovedAckEventsParams {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Previously delivered event batch identifier returned by `approved_pull_events`.
    pub batch_id: String,
}

/// Bounded page of safe operator approval summaries returned by `list_approvals`.
///
/// Every entry is filtered to the exact enrolled `(binding_id, thread_uuid)` and omits CLI
/// executable paths, secrets, and raw task instructions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalSummaryPage {
    /// Authoritative control-plane chat binding identifier of the enrolled native thread.
    pub binding_id: String,
    /// Authoritative native Codex thread UUID (`thread.id`) of the enrolled native thread.
    pub thread_uuid: String,
    /// Bounded page (`<= 20`) of safe approval summaries owned by `(binding_id, thread_uuid)`.
    pub approvals: Vec<ApprovalSummary>,
    /// Opaque pagination cursor for the next page when `has_more` is `true`.
    pub next_cursor: Option<String>,
    /// Whether additional approval summaries remain after this page.
    pub has_more: bool,
}

/// Read-only bounded activity page returned by `approved_activity`.
///
/// Explicitly distinguishes the enrolled chat's `control_binding_id` (matching the outer MCP
/// envelope's `binding_id`) from the separate approved host's software-generated runtime
/// `session_binding_id` (which is also carried by the flattened inner [`ActivityPage::binding_id`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovedActivityPage {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Authoritative control-plane chat binding identifier of the enrolled native thread.
    pub control_binding_id: String,
    /// Distinct software-generated runtime session binding identifier inside the approved host.
    pub session_binding_id: String,
    /// Optional approved root worker task identifier filter applied to this page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Inner read-only host runtime activity page (`page.binding_id` equals `session_binding_id`).
    #[serde(flatten)]
    pub page: ActivityPage,
}

/// Bounded event delivery batch returned by `approved_pull_events`.
///
/// Wraps the inner [`EventBatch`] in an explicit `batch` field (without `#[serde(flatten)]`)
/// so the enrolled chat's `control_binding_id` and the separate approved host's runtime
/// `session_binding_id` (`batch.binding_id`) are never conflated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEventBatch {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Authoritative control-plane chat binding identifier of the enrolled native thread.
    pub control_binding_id: String,
    /// Distinct software-generated runtime session binding identifier inside the approved host.
    pub session_binding_id: String,
    /// Inner bounded event batch from the approved host runtime (`batch.binding_id == session_binding_id`).
    pub batch: EventBatch,
}

/// Explicit event batch acknowledgement result returned by `approved_ack_events`.
///
/// Wraps the inner [`AckResult`] in an explicit `ack` field (without `#[serde(flatten)]`)
/// so the enrolled chat's `control_binding_id` and the separate approved host's runtime
/// `session_binding_id` (`ack.binding_id`) are never conflated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedEventAck {
    /// Target operator approval identifier (`apr_<32 lowercase hex>`).
    pub approval_id: String,
    /// Authoritative control-plane chat binding identifier of the enrolled native thread.
    pub control_binding_id: String,
    /// Distinct software-generated runtime session binding identifier inside the approved host.
    pub session_binding_id: String,
    /// Inner acknowledgement result from the approved host runtime (`ack.binding_id == session_binding_id`).
    pub ack: AckResult,
}

/// Opens optional private operator [`AuthorityStore`] and host `sessions_dir` handles.
///
/// Requires both `authority_dir` and `sessions_dir` to be provided together, and requires
/// `workflow_stores` (`--registry-dir` and `--plans-dir`) so the global [`AuthorityStore`] shares
/// the exact configured `Arc<RegistryStore>` and `Arc<PlanStore>` handles.
pub fn open_host_stores(
    authority_dir: Option<&Path>,
    sessions_dir: Option<&Path>,
    workflow_stores: Option<&WorkflowStoreHandles>,
) -> Result<Option<HostStoreHandles>, NativeError> {
    open_host_stores_with_known_dirs(authority_dir, sessions_dir, workflow_stores, &[])
}

/// Opens optional private operator [`AuthorityStore`] and host `sessions_dir` handles while
/// verifying non-overlap against `known_dirs`.
pub fn open_host_stores_with_known_dirs(
    authority_dir: Option<&Path>,
    sessions_dir: Option<&Path>,
    workflow_stores: Option<&WorkflowStoreHandles>,
    known_dirs: &[(&str, &Path)],
) -> Result<Option<HostStoreHandles>, NativeError> {
    match (authority_dir, sessions_dir) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(NativeError::new(
            NativeErrorCode::InvalidConfig,
            "both --authority-dir and --sessions-dir must be provided together to enable approved-task host tools",
        )),
        (Some(auth_path), Some(sess_path)) => {
            let Some((registry, plans)) = workflow_stores else {
                return Err(NativeError::new(
                    NativeErrorCode::InvalidConfig,
                    "--authority-dir and --sessions-dir require --registry-dir and --plans-dir to be configured",
                ));
            };

            let authority =
                AuthorityStore::open(auth_path, Arc::clone(registry), Arc::clone(plans))
                    .map_err(map_authority_store_open_error)?;

            let mut overlap_checks: Vec<(&str, &Path)> = Vec::with_capacity(known_dirs.len() + 3);
            overlap_checks.extend_from_slice(known_dirs);
            overlap_checks.push(("registry_dir", registry.config_dir()));
            overlap_checks.push(("plans_dir", plans.dir()));
            overlap_checks.push(("authority_dir", authority.dir()));

            let canonical_sessions_dir = validate_sessions_root(sess_path, &overlap_checks)
                .map_err(map_host_store_open_error)?;

            Ok(Some((Arc::new(authority), canonical_sessions_dir)))
        }
    }
}

fn map_authority_store_open_error(err: AuthorityError) -> NativeError {
    let (code, message) = match err {
        AuthorityError::SecurityViolation { .. } => (
            NativeErrorCode::SecurityViolation,
            "Authority store path failed private directory or symlink security checks.",
        ),
        AuthorityError::Io { .. } => (
            NativeErrorCode::IoError,
            "Filesystem I/O error while opening the authority store.",
        ),
        _ => (
            NativeErrorCode::InvalidConfig,
            "Authority store configuration or state is invalid.",
        ),
    };
    NativeError::new(code, message)
}

fn map_host_store_open_error(err: HostError) -> NativeError {
    let (code, message) = match err {
        HostError::SecurityViolation { .. } => (
            NativeErrorCode::SecurityViolation,
            "Host sessions directory failed private directory, symlink, or non-overlap security checks.",
        ),
        HostError::Io { .. } => (
            NativeErrorCode::IoError,
            "Filesystem I/O error while validating the host sessions directory.",
        ),
        _ => (
            NativeErrorCode::InvalidConfig,
            "Host sessions directory configuration is invalid.",
        ),
    };
    NativeError::new(code, message)
}

/// Maps any [`AuthorityError`] to a static bounded `(code, message)` pair safe for MCP responses.
pub fn map_authority_error_to_mcp(err: &AuthorityError) -> (&'static str, &'static str) {
    match err {
        AuthorityError::Validation { .. } => {
            ("validation_failed", "Request parameter failed validation.")
        }
        AuthorityError::PlanNotFound { .. } => (
            "plan_not_found",
            "Referenced workflow plan revision was not found.",
        ),
        AuthorityError::ApprovalNotFound { .. } => (
            "approval_not_found",
            "Requested approval was not found for this chat binding.",
        ),
        AuthorityError::BindingMismatch { .. } | AuthorityError::ThreadUuidMismatch { .. } => (
            "scope_mismatch",
            "Approval record does not belong to the enrolled chat binding and native thread.",
        ),
        AuthorityError::UnpublishedOrSupersededPlan { .. } => (
            "unpublished_or_superseded_plan",
            "Source workflow plan revision is not an active published revision.",
        ),
        AuthorityError::StaleRegistryRevision { .. } => (
            "stale_registry_revision",
            "Workflow registry revision no longer matches the approved snapshot.",
        ),
        AuthorityError::PlanDigestMismatch { .. } => (
            "plan_digest_mismatch",
            "Compiled workflow plan digest does not match the approved snapshot.",
        ),
        AuthorityError::UnknownTask { .. } => (
            "unknown_task",
            "Requested task was not found in the frozen approval record.",
        ),
        AuthorityError::UnsupportedTaskAdapter { .. } => (
            "unsupported_task_adapter",
            "Selected task uses an unsupported execution adapter.",
        ),
        AuthorityError::InvalidExecutable { .. } => (
            "invalid_executable",
            "Approved worker tool executable could not be validated.",
        ),
        AuthorityError::InvalidRepository { .. } => (
            "invalid_repository",
            "Approved target repository could not be validated.",
        ),
        AuthorityError::InvalidBaseCommit { .. } => (
            "invalid_base_commit",
            "Approved base commit could not be validated.",
        ),
        AuthorityError::InvalidWriteScope { .. }
        | AuthorityError::UnapprovedTaskWriteScope { .. } => (
            "invalid_write_scope",
            "Task write scope is invalid or not covered by operator approval.",
        ),
        AuthorityError::InvalidWorkArea { .. } => (
            "invalid_work_area",
            "Approved work area directory failed validation.",
        ),
        AuthorityError::WorkersConfigInvalid { .. } => (
            "workers_config_invalid",
            "Synthesized worker configuration failed validation.",
        ),
        AuthorityError::SecurityViolation { .. } => (
            "security_violation",
            "Security policy or private directory invariant was violated.",
        ),
        AuthorityError::CorruptStore { .. } => (
            "corrupt_store",
            "Authority store failed integrity verification.",
        ),
        AuthorityError::Io { .. } => (
            "io_error",
            "Filesystem I/O error occurred while accessing authority state.",
        ),
        AuthorityError::Registry { .. } => (
            "registry_error",
            "Workflow registry rejected the authority operation.",
        ),
        AuthorityError::Plan { .. } => (
            "plan_error",
            "Workflow plan store rejected the authority operation.",
        ),
    }
}

/// Maps any [`HostError`] to a static bounded `(code, message)` pair safe for MCP responses.
///
/// Never formats raw filesystem paths, runtime tokens, or [`HostError`] `Debug`/`Display` text.
pub fn map_host_error_to_mcp(err: &HostError) -> (&'static str, &'static str) {
    match err {
        HostError::Validation { field, .. } => {
            if field == "cursor" || field == "after_seq" {
                ("invalid_cursor", INVALID_CURSOR_MESSAGE)
            } else if field == "limit" {
                (
                    "invalid_input",
                    "Requested page limit must be between 1 and 20.",
                )
            } else if field == "batch_id" {
                (
                    "invalid_ack",
                    "Event batch acknowledgement was rejected because the batch ID is unknown, out of order, or does not belong to this approved host session.",
                )
            } else if field == "max_bytes" {
                (
                    "response_too_large",
                    "Serialized event batch or response exceeds the configured byte budget.",
                )
            } else {
                ("validation_failed", "Request parameter failed validation.")
            }
        }
        HostError::ApprovalNotFound { .. } => (
            "approval_not_found",
            "Requested approval was not found for this chat binding.",
        ),
        HostError::UnknownTask { .. } => (
            "unknown_task",
            "Requested task was not found in the frozen approval record.",
        ),
        HostError::ScopeMismatch { .. } => (
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ),
        HostError::DependencyInputsUnverified { .. } => (
            "dependency_inputs_unverified",
            "Dependent task cannot be dispatched because upstream task outputs are unverified on the static base commit.",
        ),
        HostError::NativeRouteRequired { .. } => (
            "native_route_required",
            "Task targets a manual native follower route (codex_cli) and cannot be dispatched as a headless worker.",
        ),
        HostError::TaskNotStarted { .. } => (
            "not_started",
            "Requested approved task has not been started in this host session.",
        ),
        HostError::UnpublishedOrSupersededPlan { .. } => (
            "unpublished_or_superseded_plan",
            "Source workflow plan revision is not an active published revision.",
        ),
        HostError::LockBusy { .. } => (
            "lock_busy",
            "Approved host session lock is currently busy.",
        ),
        HostError::HostNotReady { .. } => (
            "host_not_ready",
            "Approved host session is not running or has not reached authenticated ready state.",
        ),
        HostError::StaleRevision { .. } => (
            "stale_revision",
            "Expected worker run revision does not match the current revision.",
        ),
        HostError::IdempotencyConflict { .. } => (
            "idempotency_conflict",
            "Deterministic command key conflicted with an existing operation.",
        ),
        HostError::InvalidRunState { .. } => (
            "invalid_run_state",
            "Operation is not valid for the worker run's current lifecycle state.",
        ),
        HostError::SecurityViolation { .. } => (
            "security_violation",
            "Security policy or private directory invariant was violated.",
        ),
        HostError::CorruptManifest { .. } => (
            "corrupt_manifest",
            "Approved host session manifest failed integrity verification.",
        ),
        HostError::CorruptReceipts { .. } => (
            "corrupt_receipts",
            "Approved host task receipt ledger failed integrity verification.",
        ),
        HostError::CorruptSessionState { .. } => (
            "corrupt_session_state",
            "Approved host runtime session violated integrity invariants.",
        ),
        HostError::Authority { code, .. } => match code.as_str() {
            "approval_not_found" => (
                "approval_not_found",
                "Requested approval was not found for this chat binding.",
            ),
            "binding_mismatch" | "thread_uuid_mismatch" => (
                "scope_mismatch",
                "Approval record does not belong to the enrolled chat binding and native thread.",
            ),
            "unpublished_or_superseded_plan" => (
                "unpublished_or_superseded_plan",
                "Source workflow plan revision is not an active published revision.",
            ),
            "unknown_task" => (
                "unknown_task",
                "Requested task was not found in the frozen approval record.",
            ),
            "security_violation" => (
                "security_violation",
                "Security policy or private directory invariant was violated.",
            ),
            "corrupt_store" => (
                "corrupt_store",
                "Authority store failed integrity verification.",
            ),
            _ => (
                "authority_error",
                "Authority store rejected the approved task operation.",
            ),
        },
        HostError::Runtime { code, .. } => match code.as_str() {
            "stalerevision" | "stale_revision" => (
                "stale_revision",
                "Expected worker run revision does not match the current revision.",
            ),
            "idempotencyconflict" | "idempotency_conflict" => (
                "idempotency_conflict",
                "Deterministic command key conflicted with an existing operation.",
            ),
            "invalidrunstate" | "invalid_run_state" => (
                "invalid_run_state",
                "Operation is not valid for the worker run's current lifecycle state.",
            ),
            "invalidack" | "invalid_ack" => (
                "invalid_ack",
                "Event batch acknowledgement was rejected because the batch ID is unknown, out of order, or does not belong to this approved host session.",
            ),
            "unseeneventcursor" | "unseen_event_cursor" => (
                "unseen_event_cursor",
                "Cannot pull events after an unseen sequence number beyond delivered_seq.",
            ),
            "payloadtoolarge"
            | "payload_too_large"
            | "responsetoolarge"
            | "response_too_large"
            | "eventpayloadtoolarge"
            | "event_payload_too_large" => (
                "response_too_large",
                "Serialized event batch or response exceeds the configured byte budget.",
            ),
            "validation" | "validation_failed" | "validationfailed" => {
                ("invalid_cursor", INVALID_CURSOR_MESSAGE)
            }
            _ => (
                "runtime_error",
                "Approved host runtime rejected the operation.",
            ),
        },
        HostError::Io { .. } => (
            "io_error",
            "Filesystem I/O error occurred while accessing approved host state.",
        ),
    }
}

pub(crate) fn format_approved_scoped_error(
    binding_id: &str,
    tool_name: &str,
    code: &'static str,
    message: &'static str,
) -> CallToolResult {
    let payload = json!({
        "untrusted_data": true,
        "demo_only": false,
        "binding_id": binding_id,
        "tool": tool_name,
        "error": {
            "code": code,
            "message": message,
        }
    });
    CallToolResult::structured_error(payload)
}

fn format_approved_result<T: Serialize>(
    binding_id: &str,
    tool_name: &'static str,
    data: T,
) -> Result<CallToolResult, CallToolResult> {
    if let Err(err) = ensure_mcp_response_budget(binding_id, tool_name, &data) {
        return Err(format_approved_scoped_error(
            binding_id,
            tool_name,
            err.code(),
            err.safe_message(),
        ));
    }
    Ok(ThreadlaneMcpServer::build_structured_tool_result(
        binding_id, tool_name, data,
    ))
}

fn short_tag<T: Serialize + ?Sized>(
    domain: &str,
    value: &T,
) -> Result<String, (&'static str, &'static str)> {
    let Ok(encoded) = serde_json::to_vec(&(domain, value)) else {
        return Err((
            "serialization_error",
            "Failed to encode pagination digest tag.",
        ));
    };
    let digest = Sha256::digest(&encoded);
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(&mut out, "{byte:02x}");
    }
    Ok(out)
}

fn is_hex_16(s: &str) -> bool {
    s.len() == 16
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn encode_approval_cursor(
    binding_id: &str,
    thread_uuid: &str,
    state_tag: &str,
    next_offset: usize,
    anchor_approval_id: &str,
) -> Result<String, (&'static str, &'static str)> {
    if next_offset == 0 {
        return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
    }
    let binding_tag = short_tag("approval_binding", binding_id)?;
    let thread_tag = short_tag("approval_thread", thread_uuid)?;
    let anchor_tag = short_tag("approval_anchor", anchor_approval_id)?;
    let token = format!(
        "{APPROVAL_CURSOR_PREFIX}:list_approvals:{binding_tag}:{thread_tag}:{state_tag}:{next_offset}:{anchor_tag}"
    );
    if token.len() > MAX_APPROVAL_CURSOR_LEN {
        return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
    }
    Ok(token)
}

fn resolve_approval_page_start(
    cursor: Option<&str>,
    after_id: Option<&str>,
    binding_id: &str,
    thread_uuid: &str,
    state_tag: &str,
    own_approvals: &[ApprovalSummary],
) -> Result<usize, (&'static str, &'static str)> {
    if cursor.is_some() && after_id.is_some() {
        return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
    }

    let Some(raw) = cursor.or(after_id) else {
        return Ok(0);
    };

    if raw.is_empty()
        || raw.len() > MAX_APPROVAL_CURSOR_LEN
        || raw.trim() != raw
        || raw.chars().any(char::is_control)
    {
        return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
    }

    if raw.starts_with(APPROVAL_CURSOR_PREFIX) {
        let parts: Vec<&str> = raw.split(':').collect();
        if parts.len() != 7 || parts[0] != APPROVAL_CURSOR_PREFIX || parts[1] != "list_approvals" {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        }

        let cursor_binding_tag = parts[2];
        let cursor_thread_tag = parts[3];
        let cursor_state_tag = parts[4];
        let cursor_offset_raw = parts[5];
        let cursor_anchor_tag = parts[6];

        if !is_hex_16(cursor_binding_tag)
            || !is_hex_16(cursor_thread_tag)
            || !is_hex_16(cursor_state_tag)
            || !is_hex_16(cursor_anchor_tag)
        {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        }

        let expected_binding_tag = short_tag("approval_binding", binding_id)?;
        let expected_thread_tag = short_tag("approval_thread", thread_uuid)?;
        if cursor_binding_tag != expected_binding_tag
            || cursor_thread_tag != expected_thread_tag
            || cursor_state_tag != state_tag
        {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        }

        let Ok(offset) = cursor_offset_raw.parse::<usize>() else {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        };
        if offset == 0 || offset >= own_approvals.len() {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        }

        let Some(anchor) = own_approvals.get(offset - 1) else {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        };
        let expected_anchor_tag = short_tag("approval_anchor", &anchor.approval_id)?;
        if cursor_anchor_tag != expected_anchor_tag {
            return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
        }

        return Ok(offset);
    }

    // `after_id` (or `cursor` carrying an `approval_id`) must belong to the caller's own
    // `(binding_id, thread_uuid)` filtered result set. Any foreign, nonexistent, or malformed
    // identifier fails with static `invalid_cursor` without leaking other chat data.
    let Some(idx) = own_approvals
        .iter()
        .position(|item| item.approval_id == raw)
    else {
        return Err(("invalid_cursor", INVALID_CURSOR_MESSAGE));
    };

    Ok(idx + 1)
}

fn validate_cancel_reason(
    reason: Option<String>,
) -> Result<Option<String>, (&'static str, &'static str)> {
    let Some(raw) = reason else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_APPROVED_CANCEL_REASON_LEN
        || trimmed.chars().any(char::is_control)
    {
        return Err((
            "validation_failed",
            "Cancellation reason must be non-empty, at most 512 bytes, and free of control characters.",
        ));
    }
    Ok(Some(trimmed.to_string()))
}

/// Executes the `list_approvals` tool in `spawn_blocking` after enrollment verification.
pub(crate) async fn execute_list_approvals(
    authority: Arc<AuthorityStore>,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ListApprovalsParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "list_approvals";

    let limit = match params.limit {
        None => DEFAULT_APPROVAL_LIST_LIMIT,
        Some(value) if (1..=MAX_APPROVAL_LIST_LIMIT).contains(&value) => value,
        Some(_) => {
            return Err(format_approved_scoped_error(
                &trusted_binding_id,
                TOOL,
                "invalid_input",
                "Requested page limit must be between 1 and 20.",
            ));
        }
    };

    let binding_for_task = trusted_binding_id.clone();
    let thread_for_task = trusted_thread_uuid.clone();
    let cursor = params.cursor;
    let after_id = params.after_id;

    let outcome = tokio::task::spawn_blocking(
        move || -> Result<ApprovalSummaryPage, (&'static str, &'static str)> {
            let binding_records = authority
                .list_for_binding(&binding_for_task)
                .map_err(|err| map_authority_error_to_mcp(&err))?;

            let own_approvals: Vec<ApprovalSummary> = binding_records
                .into_iter()
                .filter(|item| {
                    item.binding_id == binding_for_task && item.thread_uuid == thread_for_task
                })
                .collect();

            let state_fingerprints: Vec<(&str, &str, i64)> = own_approvals
                .iter()
                .map(|a| {
                    (
                        a.approval_id.as_str(),
                        a.subject_digest.as_str(),
                        a.created_at_ms,
                    )
                })
                .collect();
            let state_tag = short_tag("list_approvals_state", &state_fingerprints)?;

            let start = resolve_approval_page_start(
                cursor.as_deref(),
                after_id.as_deref(),
                &binding_for_task,
                &thread_for_task,
                &state_tag,
                &own_approvals,
            )?;

            let total = own_approvals.len();
            if start >= total {
                let empty_page = ApprovalSummaryPage {
                    binding_id: binding_for_task.clone(),
                    thread_uuid: thread_for_task,
                    approvals: Vec::new(),
                    next_cursor: None,
                    has_more: false,
                };
                ensure_mcp_response_budget(&binding_for_task, TOOL, &empty_page)
                    .map_err(|err| (err.code(), err.safe_message()))?;
                return Ok(empty_page);
            }

            let max_take = limit.min(total - start);
            let mut take_count = max_take;

            loop {
                let end = start + take_count;
                let page_items = own_approvals[start..end].to_vec();
                let has_more = end < total;
                let next_cursor = if has_more {
                    let anchor_id = &page_items
                        .last()
                        .expect("non-empty page slice must have last item")
                        .approval_id;
                    Some(encode_approval_cursor(
                        &binding_for_task,
                        &thread_for_task,
                        &state_tag,
                        end,
                        anchor_id,
                    )?)
                } else {
                    None
                };

                let candidate = ApprovalSummaryPage {
                    binding_id: binding_for_task.clone(),
                    thread_uuid: thread_for_task.clone(),
                    approvals: page_items,
                    next_cursor,
                    has_more,
                };

                match ensure_mcp_response_budget(&binding_for_task, TOOL, &candidate) {
                    Ok(_) => return Ok(candidate),
                    Err(err) => {
                        if take_count <= 1 {
                            return Err((err.code(), err.safe_message()));
                        }
                        take_count -= 1;
                    }
                }
            }
        },
    )
    .await
    .map_err(|_| {
        format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "io_error",
            "Approved task background operation failed to complete.",
        )
    })?;

    match outcome {
        Ok(page) => format_approved_result(&trusted_binding_id, TOOL, page),
        Err((code, message)) => Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            code,
            message,
        )),
    }
}

/// Executes the `approved_session` tool after enrollment verification.
///
/// Constructs [`ApprovedHostClient`] and loads [`HostSessionSummary`] inside `spawn_blocking`,
/// then verifies live authenticated host readiness via a read-only `list_activity(limit = 1)`
/// probe before reporting `Ready`. Manifest and lock presence alone are never accepted as proof
/// of live authentication.
pub(crate) async fn execute_approved_session(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ApprovedSessionParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "approved_session";

    let approval_id = params.approval_id;
    let binding_for_task = trusted_binding_id.clone();
    let thread_for_task = trusted_thread_uuid.clone();
    let approval_for_task = approval_id.clone();

    let blocking_outcome = tokio::task::spawn_blocking(
        move || -> Result<(ApprovedHostClient, HostSessionSummary), (&'static str, &'static str)> {
            let client = ApprovedHostClient::new(
                authority,
                sessions_dir,
                binding_for_task,
                thread_for_task,
            )
            .map_err(|err| map_host_error_to_mcp(&err))?;

            let summary = client
                .session_summary(&approval_for_task)
                .map_err(|err| map_host_error_to_mcp(&err))?;

            Ok((client, summary))
        },
    )
    .await
    .map_err(|_| {
        format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "io_error",
            "Approved task background operation failed to complete.",
        )
    })?;

    let (client, mut summary) = match blocking_outcome {
        Ok(pair) => pair,
        Err((code, message)) => {
            return Err(format_approved_scoped_error(
                &trusted_binding_id,
                TOOL,
                code,
                message,
            ));
        }
    };

    // Never expose raw error strings from `host-manifest.json` (which might contain filesystem paths).
    if summary.last_error.is_some() {
        summary.last_error =
            Some("Approved host session reported an execution failure.".to_string());
    }

    if summary.binding_id != trusted_binding_id || summary.thread_uuid != trusted_thread_uuid {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    if summary.phase != HostPhase::Ready
        || !summary.gates.supervisor_authenticated_ready
        || summary.session_binding_id.is_none()
    {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "host_not_ready",
            "Approved host session is not running or has not reached authenticated ready state.",
        ));
    }

    // Verify live authenticated host supervisor via read-only `list_activity(limit = 1)`.
    // This never calls `pull_events` or `ack_events` and never consumes or advances cursors.
    let activity_probe = client
        .list_activity(&approval_id, None, None, Some(1), None)
        .await
        .map_err(|err| {
            let (code, message) = match err {
                HostError::HostNotReady { .. } | HostError::Runtime { .. } | HostError::Io { .. } => (
                    "host_not_ready",
                    "Approved host session is not running or has not reached authenticated ready state.",
                ),
                other => map_host_error_to_mcp(&other),
            };
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if Some(activity_probe.binding_id.as_str()) != summary.session_binding_id.as_deref()
        || activity_probe.binding_id == trusted_binding_id
    {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "corrupt_session_state",
            "Approved host runtime session binding did not match the sealed session manifest.",
        ));
    }

    format_approved_result(&trusted_binding_id, TOOL, summary)
}

async fn build_host_client_blocking(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: &str,
    trusted_thread_uuid: &str,
    tool_name: &'static str,
) -> Result<ApprovedHostClient, CallToolResult> {
    let binding_for_task = trusted_binding_id.to_string();
    let thread_for_task = trusted_thread_uuid.to_string();

    let outcome = tokio::task::spawn_blocking(move || {
        ApprovedHostClient::new(authority, sessions_dir, binding_for_task, thread_for_task)
            .map_err(|err| map_host_error_to_mcp(&err))
    })
    .await
    .map_err(|_| {
        format_approved_scoped_error(
            trusted_binding_id,
            tool_name,
            "io_error",
            "Approved task background operation failed to complete.",
        )
    })?;

    outcome.map_err(|(code, message)| {
        format_approved_scoped_error(trusted_binding_id, tool_name, code, message)
    })
}

/// Executes the `start_approved_task` tool after enrollment verification.
pub(crate) async fn execute_start_approved_task(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: StartApprovedTaskParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "start_approved_task";

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let snapshot = client
        .dispatch_task(&params.approval_id, &params.task_id)
        .await
        .map_err(|err| {
            let (code, message) = map_host_error_to_mcp(&err);
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if snapshot.binding_id != trusted_binding_id || snapshot.thread_uuid != trusted_thread_uuid {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    format_approved_result(&trusted_binding_id, TOOL, snapshot)
}

/// Executes the `approved_task` tool after enrollment verification.
pub(crate) async fn execute_approved_task(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ApprovedTaskParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "approved_task";

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let snapshot = client
        .get_task(&params.approval_id, &params.task_id)
        .await
        .map_err(|err| {
            let (code, message) = map_host_error_to_mcp(&err);
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if snapshot.binding_id != trusted_binding_id || snapshot.thread_uuid != trusted_thread_uuid {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    format_approved_result(&trusted_binding_id, TOOL, snapshot)
}

/// Executes the `cancel_approved_task` tool after enrollment verification.
pub(crate) async fn execute_cancel_approved_task(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: CancelApprovedTaskParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "cancel_approved_task";

    if params.expected_revision == 0 {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "validation_failed",
            "Expected worker run revision must be at least 1.",
        ));
    }

    let clean_reason = validate_cancel_reason(params.reason).map_err(|(code, message)| {
        format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
    })?;

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let snapshot = client
        .cancel_task(
            &params.approval_id,
            &params.task_id,
            params.expected_revision,
            clean_reason,
        )
        .await
        .map_err(|err| {
            let (code, message) = map_host_error_to_mcp(&err);
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if snapshot.binding_id != trusted_binding_id || snapshot.thread_uuid != trusted_thread_uuid {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "scope_mismatch",
            "Approval record or host session does not belong to the enrolled chat binding and native thread.",
        ));
    }

    format_approved_result(&trusted_binding_id, TOOL, snapshot)
}

/// Executes the `approved_activity` tool after enrollment verification.
pub(crate) async fn execute_approved_activity(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ApprovedActivityParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "approved_activity";

    let effective_limit = match params.limit {
        None => DEFAULT_APPROVED_ACTIVITY_LIMIT,
        Some(value) if (1..=MAX_APPROVED_ACTIVITY_LIMIT).contains(&value) => value,
        Some(_) => {
            return Err(format_approved_scoped_error(
                &trusted_binding_id,
                TOOL,
                "invalid_input",
                "Requested page limit must be between 1 and 20.",
            ));
        }
    };

    if params.after_seq.is_some() && params.cursor.is_some() {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "invalid_cursor",
            INVALID_CURSOR_MESSAGE,
        ));
    }

    if let Some(ref cursor) = params.cursor {
        if cursor.is_empty()
            || cursor.len() > MAX_APPROVAL_CURSOR_LEN
            || cursor.trim() != cursor
            || cursor.chars().any(char::is_control)
        {
            return Err(format_approved_scoped_error(
                &trusted_binding_id,
                TOOL,
                "invalid_cursor",
                INVALID_CURSOR_MESSAGE,
            ));
        }
    }

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let page = match params.task_id.as_deref() {
        Some(task_id) => {
            client
                .list_task_activity(
                    &params.approval_id,
                    task_id,
                    params.after_seq,
                    params.cursor,
                    Some(effective_limit),
                    params.kind,
                )
                .await
        }
        None => {
            client
                .list_activity(
                    &params.approval_id,
                    params.after_seq,
                    params.cursor,
                    Some(effective_limit),
                    params.kind,
                )
                .await
        }
    }
    .map_err(|err| {
        let (code, message) = map_host_error_to_mcp(&err);
        format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
    })?;

    if page.binding_id.is_empty() || page.binding_id == trusted_binding_id {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "corrupt_session_state",
            "Approved host runtime session binding must be distinct from the control-plane chat binding.",
        ));
    }

    let session_binding_id = page.binding_id.clone();
    let view = ApprovedActivityPage {
        approval_id: params.approval_id,
        control_binding_id: trusted_binding_id.clone(),
        session_binding_id,
        task_id: params.task_id,
        page,
    };

    format_approved_result(&trusted_binding_id, TOOL, view)
}

/// Executes the `approved_pull_events` tool after enrollment verification.
///
/// Validates `limit` (`1..=20`, default `20`) before opening the host client or touching
/// runtime state, pulls a byte-bounded (`<= 49_152` bytes) and count-bounded (`<= 20` events)
/// batch from the separate approved host session via [`ApprovedHostClient::pull_events`],
/// marks all event payloads as untrusted (`untrusted_data: true`), and verifies the 256 KiB
/// MCP response envelope budget.
pub(crate) async fn execute_approved_pull_events(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ApprovedPullEventsParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "approved_pull_events";

    let effective_limit = match params.limit {
        None => DEFAULT_APPROVED_EVENT_BATCH_LIMIT,
        Some(value) if (1..=MAX_APPROVED_EVENT_BATCH_LIMIT).contains(&value) => value,
        Some(_) => {
            return Err(format_approved_scoped_error(
                &trusted_binding_id,
                TOOL,
                "invalid_input",
                "Requested page limit must be between 1 and 20.",
            ));
        }
    };

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let mut batch = client
        .pull_events(&params.approval_id, params.after_seq, Some(effective_limit))
        .await
        .map_err(|err| {
            let (code, message) = map_host_error_to_mcp(&err);
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if batch.binding_id.is_empty() || batch.binding_id == trusted_binding_id {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "corrupt_session_state",
            "Approved host runtime session binding must be distinct from the control-plane chat binding.",
        ));
    }

    for event in &mut batch.events {
        event.payload.untrusted_data = true;
    }

    let session_binding_id = batch.binding_id.clone();
    let view = ApprovedEventBatch {
        approval_id: params.approval_id,
        control_binding_id: trusted_binding_id.clone(),
        session_binding_id,
        batch,
    };

    format_approved_result(&trusted_binding_id, TOOL, view)
}

/// Executes the `approved_ack_events` tool after enrollment verification.
///
/// Acknowledges the delivered event batch on the separate approved host session via
/// [`ApprovedHostClient::ack_events`] (which derives the deterministic idempotency key
/// internally), and returns a typed [`ApprovedEventAck`] within the 256 KiB MCP response
/// envelope budget.
pub(crate) async fn execute_approved_ack_events(
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    trusted_binding_id: String,
    trusted_thread_uuid: String,
    params: ApprovedAckEventsParams,
) -> Result<CallToolResult, CallToolResult> {
    const TOOL: &str = "approved_ack_events";

    let client = build_host_client_blocking(
        authority,
        sessions_dir,
        &trusted_binding_id,
        &trusted_thread_uuid,
        TOOL,
    )
    .await?;

    let ack = client
        .ack_events(&params.approval_id, params.batch_id)
        .await
        .map_err(|err| {
            let (code, message) = map_host_error_to_mcp(&err);
            format_approved_scoped_error(&trusted_binding_id, TOOL, code, message)
        })?;

    if ack.binding_id.is_empty() || ack.binding_id == trusted_binding_id {
        return Err(format_approved_scoped_error(
            &trusted_binding_id,
            TOOL,
            "corrupt_session_state",
            "Approved host runtime session binding must be distinct from the control-plane chat binding.",
        ));
    }

    let session_binding_id = ack.binding_id.clone();
    let view = ApprovedEventAck {
        approval_id: params.approval_id,
        control_binding_id: trusted_binding_id.clone(),
        session_binding_id,
        ack,
    };

    format_approved_result(&trusted_binding_id, TOOL, view)
}
