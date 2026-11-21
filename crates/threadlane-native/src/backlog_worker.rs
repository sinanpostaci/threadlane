//! Trusted-local service for guarded Worker backlog execution.
//!
//! This module is the typed boundary between the paired MCP server and the existing
//! trusted-local supervisor transport. It reserves a Worker backlog item before any
//! host/provider effect, reconciles the original approved task and invocation, reads
//! genuine live or stopped host snapshots, records only verified terminal outcomes,
//! commits separate Coworker acceptance completion, and records a failed-acceptance
//! rejection only after rechecking the same physically verified delivery. It never
//! creates a job, controller, reviewer, closure stage, or automatic acceptance decision.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane::operator_backlog::{OperatorWorkerBacklogRequest, OperatorWorkerBacklogResponse};
use threadlane::protocol::{ScopedRequest, ScopedResponse};
use threadlane::{ErrorCode as RuntimeErrorCode, SupervisorClient};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AuthorityError, AuthorityStore};
use threadlane_core::{
    BacklogItem, BacklogItemKind, BacklogStatus, WorkerAcceptanceState,
    WorkerBacklogAcceptanceRejection, WorkerBacklogAcceptanceRejectionReceipt,
    WorkerBacklogAuthority, WorkerBacklogCompletion, WorkerBacklogCompletionReceipt,
    WorkerBacklogCompletionRecord, WorkerBacklogDispatchReceipt, WorkerBacklogExecution,
    WorkerBacklogNoticeSnapshot, WorkerBacklogOutcome, WorkerBacklogOutcomeReport,
    WorkerEvidenceFailure, WorkerStatus,
};
use threadlane_host::{
    ApprovedHostClient, DeliveryInspection, HostError, TaskExecutionStatus, TaskStateSnapshot,
};

use crate::approved_mcp::StartApprovedTaskParams;
use crate::host_launcher::{
    execute_start_approved_task_with_launcher_until, ApprovedHostLauncher, ManagedHostNoticeRequest,
};

const START_TOOL: &str = "backlog_worker_start";
const RESULT_TOOL: &str = "backlog_worker_result";
const NOTICE_CONTEXT_TOOL: &str = "backlog_worker_notice_context";
const COMPLETE_TOOL: &str = "backlog_worker_complete";
const REJECT_TOOL: &str = "backlog_worker_reject";
const FAILURE_RESULT_DIGEST_SCHEMA: &str = "threadlane-worker-backlog-failure-result-v1";
const DERIVED_COMMAND_DOMAIN: &str = "threadlane-worker-backlog-command-v1";

const OUTCOME_NOTE_SUCCESS: &str = "verified terminal worker success";
const OUTCOME_NOTE_UNKNOWN: &str = "worker dispatch or effect remains unknown";

/// Strict MCP parameters for explicitly reserving and starting one Worker backlog item.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogStartParams {
    /// Software-generated backlog item identifier.
    pub item_id: String,
    /// Expected current backlog item revision before reservation.
    pub expected_revision: u64,
    /// Caller-supplied idempotency key scoped to the enrolled chat binding.
    pub command_key: String,
    /// Existing durable operator approval identifier.
    pub approval_id: String,
    /// Exact approved task identifier within that approval.
    pub task_id: String,
}

/// Strict MCP parameters for reading and reconciling one stored Worker execution.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogResultParams {
    /// Software-generated backlog item identifier.
    pub item_id: String,
    /// Exact stored Worker execution reference.
    pub execution_ref: String,
}

/// Strict parameters for recovering one original Worker notice context.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogNoticeContextParams {
    /// Exact durable approval identifier carried by the native notice.
    pub approval_id: String,
}

/// Strict MCP parameters for separate Coworker acceptance completion.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogCompleteParams {
    /// Software-generated backlog item identifier.
    pub item_id: String,
    /// Exact stored successful Worker execution reference.
    pub execution_ref: String,
    /// Caller-supplied idempotency key scoped to the enrolled chat binding.
    pub command_key: String,
    /// Expected current backlog item revision.
    pub expected_revision: u64,
    /// Exact successful result digest previously returned by the result tool.
    pub expected_result_digest: String,
    /// Exact physical delivery digest previously returned by the result tool.
    pub expected_delivery_digest: String,
    /// Bounded Coworker acceptance note.
    pub acceptance_note: String,
    /// Bounded Coworker acceptance evidence attestation.
    pub acceptance_evidence: String,
}

/// Strict MCP parameters for rejecting a technically successful delivery.
///
/// The caller must already have read the exact stored success and physical
/// delivery, and independently decided that the delivered result does not meet
/// the item's acceptance condition. The request carries no outcome override and
/// cannot claim a provider failure or completion.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogRejectParams {
    /// Caller-supplied idempotency key scoped to the enrolled chat binding.
    pub command_key: String,
    /// Software-generated backlog item identifier.
    pub item_id: String,
    /// Expected current backlog item revision.
    pub expected_revision: u64,
    /// Exact stored successful Worker execution reference.
    pub execution_ref: String,
    /// Exact successful result digest previously returned by the result tool.
    pub expected_result_digest: String,
    /// Exact physical delivery digest previously returned by the result tool.
    pub expected_delivery_digest: String,
    /// Bounded Coworker reason the delivered result failed acceptance.
    pub rejection_reason: String,
    /// Bounded Coworker evidence checked during failed acceptance.
    pub rejection_evidence: String,
}

/// Compact successful start/reservation response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogStartOutcome {
    /// Exact stored execution after reservation or idempotent replay.
    pub execution: WorkerBacklogExecution,
    /// Genuine host status when independently observed; never proof by itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_status: Option<TaskExecutionStatus>,
}

/// Compact current backlog item snapshot read from scoped core after reconciliation.
///
/// This is an observed projection, never a computed or caller-supplied value. It lets an
/// ordinary Coworker complete or reject with the actual current revision without a separate
/// `backlog_get` or `approved_task` read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogItemSnapshot {
    /// Actual software-generated backlog item identifier.
    pub item_id: String,
    /// Actual current backlog item revision after reconciliation.
    pub revision: u64,
    /// Actual current backlog item status after reconciliation.
    pub status: BacklogStatus,
    /// Actual current Worker execution reference, when one holds the item.
    pub current_execution_ref: Option<String>,
}

/// Frozen approval/task facts needed for independent delivery inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogNoticeScope {
    /// Canonical repository path frozen in the scoped approval record.
    pub repository_path: PathBuf,
    /// Approved repository base commit.
    pub base_commit: String,
    /// Frozen requested model.
    pub requested_model: String,
    /// Frozen requested reasoning effort.
    pub requested_effort: String,
    /// Exact approved task write paths.
    pub write_paths: Vec<String>,
    /// Optional frozen subset of exact write paths that must change; `None` requires every write path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_changed_paths: Option<Vec<String>>,
}

/// Read-only durable recovery context; no field grants new execution authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogNoticeContextOutcome {
    /// Current durable execution including the original immutable dispatch identity.
    pub execution: WorkerBacklogExecution,
    /// Narrow projection of immutable approval scope.
    pub scope: WorkerBacklogNoticeScope,
    /// Untrusted item acceptance condition, verified against its reservation digest.
    pub acceptance_condition: String,
    /// Untrusted intended delivery description, verified against its reservation digest.
    pub delivery_description: String,
    /// Atomic current item identity, status, and revision.
    pub item_snapshot: WorkerBacklogItemSnapshot,
    /// Canonical completion receipt when this same execution is already Done.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<WorkerBacklogCompletionReceipt>,
}

/// Compact Worker evidence derived from the same verified host task snapshot used for
/// reconciliation.
///
/// It intentionally excludes the raw instruction, worker summaries, provider logs, environment,
/// credentials, and arbitrary captured text. Optional fields stay absent when the verified
/// snapshot does not carry them; they are never synthesized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogWorkerEvidence {
    /// Frozen approval identifier verified by the snapshot.
    pub approval_id: String,
    /// Frozen approved task identifier verified by the snapshot.
    pub task_id: String,
    /// Genuine runtime session binding identifier.
    pub session_binding_id: String,
    /// Actual software-generated run identifier.
    pub run_id: String,
    /// Actual software-generated worker identifier.
    pub worker_id: String,
    /// Frozen trusted execution profile identifier.
    pub profile_id: String,
    /// Provider fixed by the trusted execution profile.
    pub provider: String,
    /// Exact model requested by the trusted execution profile.
    pub requested_model: String,
    /// Exact reasoning effort requested by the trusted execution profile.
    pub requested_effort: String,
    /// Model identifier observed in provider telemetry, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_model: Option<String>,
    /// Reasoning effort observed in provider telemetry, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_effort: Option<String>,
    /// Verified runtime Worker status from the same snapshot.
    pub worker_status: WorkerStatus,
    /// Current worker run revision.
    pub worker_revision: u64,
    /// Number of dispatch claims attempted.
    pub attempt_count: u32,
    /// Actual runtime start timestamp in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<i64>,
    /// Actual runtime completion timestamp in milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<i64>,
    /// Actual measured terminal duration in milliseconds when proven.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Process exit code when the worker exited normally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Terminating signal number when the worker was killed by a signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    /// Whether the worker exceeded its configured timeout budget, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_out: Option<bool>,
    /// Whether the worker was terminated due to cancellation, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled: Option<bool>,
    /// Whether cancellation was requested before completion, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_requested_before_completion: Option<bool>,
    /// Whether verifiable model telemetry was missing, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_model_evidence: Option<bool>,
    /// Whether verifiable reasoning effort telemetry was missing, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_effort_evidence: Option<bool>,
    /// Whether the provider emitted a structured error event, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_error: Option<bool>,
    /// Whether descendant processes survived after the leader exited, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_cleanup: Option<bool>,
    /// Whether the post-run Git diff gate rejected the worktree changes, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_by_diff_gate: Option<bool>,
    /// Total diff-gate scope violations, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub violation_count: Option<u32>,
    /// Whether stdout exceeded its byte budget, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_truncated: Option<bool>,
    /// Whether stderr exceeded its byte budget, when terminal evidence exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_truncated: Option<bool>,
    /// Actual scoped task worktree path from the verified Worker snapshot, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_worktree_path: Option<String>,
}

/// Result and reconciliation response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogResultOutcome {
    /// Exact stored execution after reconciliation.
    pub execution: WorkerBacklogExecution,
    /// Genuine host status when independently observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_status: Option<TaskExecutionStatus>,
    /// Path-free physical delivery inspection for exact Coworker acceptance binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryInspection>,
    /// Compact current item snapshot read from scoped core after reconciliation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_snapshot: Option<WorkerBacklogItemSnapshot>,
    /// Compact Worker evidence from the same verified host snapshot, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_evidence: Option<WorkerBacklogWorkerEvidence>,
}

/// Separate completion response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogCompleteOutcome {
    /// Durable core completion receipt.
    pub completion: WorkerBacklogCompletionReceipt,
    /// Path-free delivery inspection observed for a first completion; absent on durable replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryInspection>,
}

/// Separate failed-acceptance rejection response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogRejectOutcome {
    /// Durable core rejection receipt preserving the original successful outcome.
    pub rejection: WorkerBacklogAcceptanceRejectionReceipt,
    /// Path-free delivery inspection observed for a first rejection; absent on durable replay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryInspection>,
}

/// Bounded static error returned to the MCP adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerBacklogError {
    code: &'static str,
    message: &'static str,
}

impl WorkerBacklogError {
    /// Constructs a bounded static error.
    pub(crate) const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    /// Stable machine-readable error code.
    pub(crate) const fn code(self) -> &'static str {
        self.code
    }

    /// Bounded sanitized message without private paths or agent logs.
    pub(crate) const fn message(self) -> &'static str {
        self.message
    }

    /// Converts a structured tool error without copying its potentially dynamic message.
    pub(crate) fn from_tool_result(result: CallToolResult) -> Self {
        let code = result
            .structured_content
            .as_ref()
            .and_then(|value| value.get("error"))
            .and_then(|error| error.get("code"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("worker_backlog_failed");
        Self::from_code(code)
    }

    fn from_code(code: &str) -> Self {
        match code {
            "host_not_ready" => Self::new(
                "host_not_ready",
                "The approved host session is not ready for this Worker backlog operation.",
            ),
            "scope_mismatch" => Self::new(
                "scope_mismatch",
                "The stored approval, task, item, or host state does not match the enrolled chat scope.",
            ),
            "validation_failed" | "validation" | "invalid_input" => Self::new(
                "validation_failed",
                "Worker backlog arguments or stored state failed validation.",
            ),
            "stale_revision" => Self::new(
                "stale_revision",
                "The Worker backlog item revision is stale; refresh it before retrying.",
            ),
            "idempotency_conflict" => Self::new(
                "idempotency_conflict",
                "This command key was already used with different Worker backlog arguments.",
            ),
            "corrupt_manifest" => Self::new(
                "corrupt_manifest",
                "The approved host manifest is corrupt and cannot be treated as absent.",
            ),
            "corrupt_receipts" => Self::new(
                "corrupt_receipts",
                "The approved host task receipts are corrupt and cannot be treated as absent.",
            ),
            "corrupt_session_state" => Self::new(
                "corrupt_session_state",
                "The approved host session state is corrupt and cannot be treated as absent.",
            ),
            "corrupt_state" => Self::new(
                "corrupt_state",
                "Worker backlog state is corrupt and cannot be treated as absent.",
            ),
            "security_violation" => Self::new(
                "security_violation",
                "Worker backlog state failed private-path or symlink security checks.",
            ),
            "outcome_unknown" => Self::new(
                "worker_effect_unknown",
                "The Worker dispatch or external effect remains unknown and was not replayed.",
            ),
            "not_started" => Self::new(
                "not_started",
                "The approved task has not been started in the observed host state.",
            ),
            "task_not_started" => Self::new(
                "task_not_started",
                "The approved task has not been started in the observed host state.",
            ),
            "host_not_configured" => Self::new(
                "host_not_configured",
                "Approved host stores are not configured for this MCP server.",
            ),
            "rebind_rejected" => Self::new(
                "rebind_rejected",
                "The verified operator enrollment identity cannot be used for this Worker backlog operation.",
            ),
            "unbound_connection" | "expired_epoch" | "shared_connection_rejected" => Self::new(
                "unbound_connection",
                "The MCP connection is not bound to the expected verified operator enrollment.",
            ),
            "approval_not_found" => Self::new(
                "approval_not_found",
                "The requested operator approval record was not found.",
            ),
            "unknown_task" => Self::new(
                "unknown_task",
                "The requested task is not part of the frozen approval record.",
            ),
            "native_route_required" => Self::new(
                "native_route_required",
                "The requested task requires manual native routing and cannot run as a Worker.",
            ),
            "dependency_inputs_unverified" => Self::new(
                "dependency_inputs_unverified",
                "The requested task requires verified dependency inputs before dispatch.",
            ),
            "payload_too_large" => Self::new(
                "payload_too_large",
                "The Worker backlog result exceeds the bounded MCP response budget.",
            ),
            _ => Self::new(
                "worker_backlog_unavailable",
                "The Worker backlog operation could not be completed from verified local state.",
            ),
        }
    }

    fn from_runtime_error(error: &threadlane::Error) -> Self {
        let code = error.to_api_error().code;
        match code {
            RuntimeErrorCode::ValidationFailed => Self::from_code("validation_failed"),
            RuntimeErrorCode::StaleRevision => Self::from_code("stale_revision"),
            RuntimeErrorCode::IdempotencyConflict => Self::from_code("idempotency_conflict"),
            RuntimeErrorCode::CorruptState => Self::from_code("corrupt_state"),
            RuntimeErrorCode::BindingNotFound => Self::new(
                "binding_not_found",
                "The enrolled chat binding was not found by the control supervisor.",
            ),
            RuntimeErrorCode::SecurityViolation => Self::from_code("security_violation"),
            RuntimeErrorCode::PayloadTooLarge | RuntimeErrorCode::EventPayloadTooLarge => {
                Self::from_code("payload_too_large")
            }
            RuntimeErrorCode::TransportError
            | RuntimeErrorCode::LockBusy
            | RuntimeErrorCode::InternalError
            | RuntimeErrorCode::StorageError => Self::new(
                "supervisor_unavailable",
                "The trusted-local control supervisor is unavailable or could not complete the operation.",
            ),
            _ => Self::from_code("worker_backlog_unavailable"),
        }
    }

    fn from_authority_error(error: &AuthorityError) -> Self {
        Self::from_code(error.code())
    }

    fn from_host_error(error: &HostError) -> Self {
        Self::from_code(error.code())
    }
}

/// Typed Worker backlog service built from already verified operator configuration.
#[derive(Clone)]
pub(crate) struct WorkerBacklogService {
    supervisor: SupervisorClient,
    authority: Arc<AuthorityStore>,
    sessions_dir: PathBuf,
    host_launcher: Option<Arc<ApprovedHostLauncher>>,
}

/// Validates that a repository-relative path uses clean literal exact-file spelling.
pub(crate) fn is_clean_exact_relative_path(path: &str) -> bool {
    if path.is_empty()
        || path.trim() != path
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains("//")
        || path.contains('\0')
    {
        return false;
    }
    path.split('/').all(|component| {
        !component.is_empty()
            && component != "."
            && component != ".."
            && !component.chars().all(|ch| ch == '.')
            && !component.chars().any(char::is_control)
            && !component
                .chars()
                .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}'))
    })
}

/// Validates frozen `required_changed_paths` for exact-file ordinary review (`R subset A`).
pub(crate) fn valid_exact_required_changed_paths(
    write_paths: &[String],
    required_changed_paths: Option<&[String]>,
) -> bool {
    if write_paths.is_empty() {
        return false;
    }
    let Some(required) = required_changed_paths else {
        return true;
    };
    if required.is_empty() {
        return false;
    }
    let mut allowed = BTreeSet::new();
    for path in write_paths {
        if !is_clean_exact_relative_path(path) || !allowed.insert(path.as_str()) {
            return false;
        }
    }
    let mut seen_required = BTreeSet::new();
    for path in required {
        if !is_clean_exact_relative_path(path)
            || !allowed.contains(path.as_str())
            || !seen_required.insert(path.as_str())
        {
            return false;
        }
    }
    true
}

fn valid_notice_required_changed_paths(
    write_paths: &[String],
    required_changed_paths: Option<&[String]>,
) -> bool {
    if write_paths.is_empty() {
        return false;
    }
    let Some(required) = required_changed_paths else {
        return true;
    };
    let mut seen_required = BTreeSet::new();
    for path in required {
        if !is_clean_exact_relative_path(path)
            || !seen_required.insert(path.as_str())
            || !threadlane_authority::is_task_scope_contained_in_operator_allowlist(
                path,
                write_paths,
            )
        {
            return false;
        }
    }
    true
}

impl WorkerBacklogService {
    /// Validates the frozen approval and an unstarted Worker item while the caller
    /// holds the same per-binding dispatch guard as guarded Worker start.
    pub(crate) async fn review_scope_before_start(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        approval_id: &str,
        task_id: &str,
        item_id: &str,
    ) -> Result<(PathBuf, String, Option<Vec<String>>), WorkerBacklogError> {
        let record = self
            .load_record_until(binding_id, thread_uuid, approval_id, None)
            .await?;
        let task = resolve_approved_task(&record, task_id)?;
        let item = self.load_item(binding_id, item_id).await?;
        if item.kind != BacklogItemKind::Worker
            || !matches!(item.status, BacklogStatus::Todo | BacklogStatus::Blocked)
            || item.current_execution_ref.is_some()
            || task.allowed_write_paths.is_empty()
            || !valid_exact_required_changed_paths(
                &task.allowed_write_paths,
                task.required_changed_paths.as_deref(),
            )
            || self
                .get_notice_snapshot(binding_id, thread_uuid, approval_id)
                .await?
                .is_some()
        {
            return Err(WorkerBacklogError::new(
                "invalid_state",
                "Review source scope requires the original unstarted Worker item and approval.",
            ));
        }
        let required_changed_paths = task.required_changed_paths.clone();
        Ok((
            record.repository_path,
            record.base_commit,
            required_changed_paths,
        ))
    }

    /// Resolve a native notice from durable scoped records without consulting or starting a host.
    pub(crate) async fn notice_context<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogNoticeContextParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogNoticeContextOutcome, WorkerBacklogError>
    where
        F: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        // Resolve the approval first so an identifier from another chat cannot be used to
        // enumerate that chat's execution state.
        let record = self
            .load_record_until(binding_id, thread_uuid, &params.approval_id, None)
            .await?;
        let maybe_snapshot = self
            .get_notice_snapshot(binding_id, thread_uuid, &params.approval_id)
            .await?;
        let Some(snapshot) = maybe_snapshot else {
            final_recheck()?;
            return Err(WorkerBacklogError::new(
                "no_guarded_execution",
                "This verified approval has no guarded Worker execution.",
            ));
        };
        let execution = &snapshot.execution;
        validate_execution_identity(execution, binding_id, &snapshot.item.item_id)?;
        let task = resolve_approved_task(&record, &execution.authority.task_id)?;
        let dispatch = execution.dispatch_receipt.as_ref().ok_or_else(|| {
            WorkerBacklogError::new(
                "worker_start_unconfirmed",
                "The original dispatch receipt is unavailable.",
            )
        })?;
        if execution.authority != authority_for_task(&record, task)
            || task.profile_id.as_deref() != Some(dispatch.profile_id.as_str())
            // Dependent tasks use a separately sealed execution base; the compact
            // dispatch receipt stores only its digest, not that base commit.
            || !task.depends_on.is_empty()
            || task.allowed_write_paths.is_empty()
            || !valid_notice_required_changed_paths(
                &task.allowed_write_paths,
                task.required_changed_paths.as_deref(),
            )
        {
            return Err(WorkerBacklogError::new(
                "authority_mismatch",
                "The current execution does not match an unambiguous frozen Worker scope.",
            ));
        }
        // Match the host's runtime normalization of an explicitly absent effort.
        let effort = match task.effort.as_deref() {
            Some("") => {
                return Err(WorkerBacklogError::new(
                    "authority_mismatch",
                    "The frozen Worker effort is invalid.",
                ));
            }
            Some(value) => value.to_string(),
            None => "default".to_string(),
        };
        // Detect a superseding attempt or a concurrent completion while resolving authority.
        // The actual result and completion operations independently recheck current state.
        let verified = self
            .get_notice_snapshot(binding_id, thread_uuid, &params.approval_id)
            .await?;
        if verified.as_ref() != Some(&snapshot) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "The durable Worker state changed during notice recovery.",
            ));
        }
        final_recheck()?;
        if snapshot.item.status == BacklogStatus::Blocked
            && execution
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.outcome == WorkerBacklogOutcome::Unknown)
        {
            return Err(WorkerBacklogError::new(
                "worker_effect_unknown",
                "The original Worker effect remains unknown and cannot be recovered as a successful closure.",
            ));
        }
        let completion = if snapshot.item.status == BacklogStatus::Done {
            Some(WorkerBacklogCompletionReceipt {
                item: snapshot.item.clone(),
                execution: snapshot.execution.clone(),
            })
        } else {
            None
        };
        Ok(WorkerBacklogNoticeContextOutcome {
            acceptance_condition: snapshot.item.acceptance_condition,
            delivery_description: snapshot.item.delivery_description,
            item_snapshot: WorkerBacklogItemSnapshot {
                item_id: snapshot.item.item_id,
                revision: snapshot.item.revision,
                status: snapshot.item.status,
                current_execution_ref: snapshot.item.current_execution_ref,
            },
            execution: snapshot.execution,
            scope: WorkerBacklogNoticeScope {
                repository_path: record.repository_path.clone(),
                base_commit: record.base_commit.clone(),
                requested_model: task.base_model.clone(),
                requested_effort: effort,
                write_paths: task.allowed_write_paths.clone(),
                required_changed_paths: task.required_changed_paths.clone(),
            },
            completion,
        })
    }

    async fn get_notice_snapshot(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        approval_id: &str,
    ) -> Result<Option<WorkerBacklogNoticeSnapshot>, WorkerBacklogError> {
        let request = OperatorWorkerBacklogRequest::GetNoticeContext {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            approval_id: approval_id.to_string(),
        };
        match self.call(request, None).await? {
            OperatorWorkerBacklogResponse::NoticeContext { snapshot } => Ok(snapshot),
            _ => Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The control supervisor returned an unexpected Worker notice snapshot.",
            )),
        }
    }

    /// Builds a lazy trusted-local supervisor client around the existing operator stores.
    pub(crate) fn new(
        state_dir: impl AsRef<std::path::Path>,
        authority: Arc<AuthorityStore>,
        sessions_dir: PathBuf,
        host_launcher: Option<Arc<ApprovedHostLauncher>>,
    ) -> Result<Self, WorkerBacklogError> {
        let supervisor = SupervisorClient::from_state_dir_lazy(state_dir)
            .map_err(|error| WorkerBacklogError::from_runtime_error(&error))?;
        Ok(Self {
            supervisor,
            authority,
            sessions_dir,
            host_launcher,
        })
    }

    /// Reserves at most one logical Worker execution before any host/provider effect and
    /// reconciles the original approved task/invocation. A pre-effect start refusal leaves the
    /// reservation recoverable for an exact retry.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start<T, B, V, R>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogStartParams,
        startup_deadline: Option<Instant>,
        managed_host_notice: ManagedHostNoticeRequest,
        blocking_recheck: B,
        verify_on_task: V,
        post_dispatch_recheck: R,
    ) -> Result<WorkerBacklogStartOutcome, WorkerBacklogError>
    where
        T: Send + 'static,
        B: Fn() -> Result<T, CallToolResult> + Send + Sync + 'static,
        V: Fn(T) -> Result<(), CallToolResult>,
        R: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        let item = self
            .load_item_until(binding_id, &params.item_id, startup_deadline)
            .await?;
        if item.kind != BacklogItemKind::Worker {
            return Err(WorkerBacklogError::new(
                "validation_failed",
                "Worker backlog start requires a Worker item.",
            ));
        }

        let record = self
            .load_record_until(
                binding_id,
                thread_uuid,
                &params.approval_id,
                startup_deadline,
            )
            .await?;
        let verified_thread_uuid = record.thread_uuid.clone();
        let task = resolve_approved_task(&record, &params.task_id)?;
        let recheck_value = blocking_recheck().map_err(WorkerBacklogError::from_tool_result)?;
        verify_on_task(recheck_value).map_err(WorkerBacklogError::from_tool_result)?;
        let reservation = self
            .reserve_until(
                binding_id,
                &verified_thread_uuid,
                &params,
                authority_for_task(&record, task),
                startup_deadline,
            )
            .await?;
        if reservation.authority != authority_for_task(&record, task) {
            return Err(WorkerBacklogError::new(
                "authority_mismatch",
                "The stored Worker reservation does not match the frozen approval and task authority.",
            ));
        }
        // Core intentionally returns the original reservation serialization for an exact reserve
        // replay, before any later attach/outcome/completion. Always reconcile the durable current
        // execution instead of assuming the command response is current.
        let execution = self
            .load_current_reserved_execution(
                binding_id,
                &verified_thread_uuid,
                &params.item_id,
                &record,
                task,
                &reservation,
            )
            .await?;
        self.reconcile_start(
            binding_id,
            &verified_thread_uuid,
            &params,
            &record,
            task,
            execution,
            startup_deadline,
            managed_host_notice,
            blocking_recheck,
            verify_on_task,
            post_dispatch_recheck,
        )
        .await
    }

    /// Reads the exact stored item/execution first, reconciles genuine live or stopped host
    /// state, and attaches/records only evidence that is actually present. Known outcomes are
    /// re-read rather than overwritten.
    pub(crate) async fn result<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogResultParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogResultOutcome, WorkerBacklogError>
    where
        F: Fn() -> Result<(), WorkerBacklogError> + Send,
    {
        let mut final_recheck = Some(final_recheck);
        let item = self.load_item(binding_id, &params.item_id).await?;
        let execution = self
            .get_execution(
                binding_id,
                thread_uuid,
                &params.item_id,
                &params.execution_ref,
            )
            .await?;
        validate_execution_identity(&execution, binding_id, &params.item_id)?;
        let (record, task) = self
            .load_execution_authority(binding_id, thread_uuid, &execution)
            .await?;
        if item.current_execution_ref.as_deref() != Some(execution.execution_ref.as_str()) {
            return self
                .finalize_result(binding_id, &params.item_id, execution, None, None, None)
                .await;
        }

        let client = self.host_client(binding_id, &record.thread_uuid)?;
        let snapshot_result = client
            .get_task_live_or_stopped(&record.approval_id, &task.task_id)
            .await;
        let snapshot = match snapshot_result {
            Ok(snapshot) => {
                verify_snapshot_identity(&snapshot, &record, &task)?;
                Some(snapshot)
            }
            Err(error) if is_missing_host_session(&error) => None,
            Err(error) => {
                let known_outcome = execution
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.outcome != WorkerBacklogOutcome::Unknown);
                if known_outcome || execution.completion.is_some() {
                    return self
                        .finalize_result(binding_id, &params.item_id, execution, None, None, None)
                        .await;
                }
                return Err(WorkerBacklogError::from_host_error(&error));
            }
        };

        let Some(snapshot) = snapshot else {
            if execution.outcome.is_some() || execution.completion.is_some() {
                return self
                    .finalize_result(binding_id, &params.item_id, execution, None, None, None)
                    .await;
            }
            if execution.dispatch_receipt.is_some() {
                return Err(WorkerBacklogError::new(
                    "host_result_unavailable",
                    "The stored Worker invocation has no readable live or stopped host result.",
                ));
            }
            return self
                .finalize_result(binding_id, &params.item_id, execution, None, None, None)
                .await;
        };

        if let Some(receipt) = execution.dispatch_receipt.as_ref() {
            if snapshot.run_id.is_some() || snapshot.worker_id.is_some() {
                verify_receipt_matches_snapshot(receipt, &snapshot)?;
            } else {
                return Err(WorkerBacklogError::new(
                    "host_state_mismatch",
                    "The stored Worker dispatch receipt has no matching genuine host invocation.",
                ));
            }
        }

        if execution.completion.is_some() {
            return self
                .finalize_result(
                    binding_id,
                    &params.item_id,
                    execution,
                    Some(snapshot.status),
                    None,
                    Some(&snapshot),
                )
                .await;
        }

        let previous_kind = execution.outcome.as_ref().map(|outcome| outcome.outcome);
        let previous_result_digest = execution
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.result_digest.clone());
        if let Some(previous_kind) = previous_kind {
            if previous_kind == WorkerBacklogOutcome::Unknown {
                if snapshot.status != TaskExecutionStatus::Unknown {
                    let outcome = self
                        .reconcile_known_outcome(
                            &client,
                            binding_id,
                            &item,
                            execution,
                            &snapshot,
                            &record,
                            &task,
                            final_recheck
                                .take()
                                .expect("result recheck closure must be available"),
                        )
                        .await?;
                    return self
                        .finalize_result(
                            binding_id,
                            &params.item_id,
                            outcome.execution,
                            outcome.host_status,
                            outcome.delivery,
                            Some(&snapshot),
                        )
                        .await;
                }
                final_recheck
                    .as_ref()
                    .expect("result recheck closure must be available")()?;
                let execution = self
                    .ensure_snapshot_receipt(
                        binding_id,
                        &record.thread_uuid,
                        &item,
                        execution,
                        &snapshot,
                    )
                    .await?;
                return self
                    .finalize_result(
                        binding_id,
                        &params.item_id,
                        execution,
                        Some(snapshot.status),
                        None,
                        Some(&snapshot),
                    )
                    .await;
            }
            if previous_kind == WorkerBacklogOutcome::Success
                && snapshot.status == TaskExecutionStatus::ReadyForReview
            {
                if let Ok(inspection) = client
                    .inspect_task_delivery(&record.approval_id, &task.task_id)
                    .await
                {
                    verify_delivery_identity(&inspection, &record, &task, &execution, &snapshot)?;
                    let stored_digest = previous_result_digest.as_deref().ok_or_else(|| {
                        WorkerBacklogError::new(
                            "corrupt_worker_state",
                            "A successful Worker outcome is missing its stored result digest.",
                        )
                    })?;
                    if inspection.result_digest != stored_digest {
                        return Err(WorkerBacklogError::new(
                            "result_digest_mismatch",
                            "The current Worker result digest differs from the stored successful outcome.",
                        ));
                    }
                    return self
                        .finalize_result(
                            binding_id,
                            &params.item_id,
                            execution,
                            Some(snapshot.status),
                            Some(inspection),
                            Some(&snapshot),
                        )
                        .await;
                }
            }
            return self
                .finalize_result(
                    binding_id,
                    &params.item_id,
                    execution,
                    Some(snapshot.status),
                    None,
                    Some(&snapshot),
                )
                .await;
        }

        match snapshot.status {
            TaskExecutionStatus::Queued
            | TaskExecutionStatus::Claimed
            | TaskExecutionStatus::Running
            | TaskExecutionStatus::CancelRequested
            | TaskExecutionStatus::NotStarted
            | TaskExecutionStatus::DependencyInputsUnverified
            | TaskExecutionStatus::DependencyInputsReady
            | TaskExecutionStatus::NativeRouteRequired => {
                self.finalize_result(
                    binding_id,
                    &params.item_id,
                    execution,
                    Some(snapshot.status),
                    None,
                    Some(&snapshot),
                )
                .await
            }
            TaskExecutionStatus::Unknown => {
                final_recheck
                    .as_ref()
                    .expect("result recheck closure must be available")()?;
                let execution = self
                    .ensure_snapshot_receipt(
                        binding_id,
                        &record.thread_uuid,
                        &item,
                        execution,
                        &snapshot,
                    )
                    .await?;
                final_recheck
                    .as_ref()
                    .expect("result recheck closure must be available")()?;
                let execution = self
                    .record_unknown(binding_id, &record.thread_uuid, execution)
                    .await?;
                self.finalize_result(
                    binding_id,
                    &params.item_id,
                    execution,
                    Some(snapshot.status),
                    None,
                    Some(&snapshot),
                )
                .await
            }
            TaskExecutionStatus::ReadyForReview
            | TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut => {
                let outcome = self
                    .reconcile_known_outcome(
                        &client,
                        binding_id,
                        &item,
                        execution,
                        &snapshot,
                        &record,
                        &task,
                        final_recheck
                            .take()
                            .expect("result recheck closure must be available"),
                    )
                    .await?;
                self.finalize_result(
                    binding_id,
                    &params.item_id,
                    outcome.execution,
                    outcome.host_status,
                    outcome.delivery,
                    Some(&snapshot),
                )
                .await
            }
        }
    }

    /// Reads the current scoped item and attaches the compact observed projection and the
    /// optional Worker evidence from the same already verified host snapshot.
    async fn finalize_result(
        &self,
        binding_id: &str,
        item_id: &str,
        execution: WorkerBacklogExecution,
        host_status: Option<TaskExecutionStatus>,
        delivery: Option<DeliveryInspection>,
        snapshot: Option<&TaskStateSnapshot>,
    ) -> Result<WorkerBacklogResultOutcome, WorkerBacklogError> {
        let item = self.load_item(binding_id, item_id).await?;
        Ok(WorkerBacklogResultOutcome {
            execution,
            host_status,
            delivery,
            item_snapshot: Some(WorkerBacklogItemSnapshot {
                item_id: item.item_id,
                revision: item.revision,
                status: item.status,
                current_execution_ref: item.current_execution_ref,
            }),
            worker_evidence: snapshot.and_then(worker_evidence_from_snapshot),
        })
    }

    /// Commits separate Coworker acceptance completion only after a fresh independent host
    /// inspection/recheck and the caller's final enrollment recheck. An exact lost completion
    /// reply is served from durable core completion deduplication without reading a checkout.
    pub(crate) async fn complete<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogCompleteParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogCompleteOutcome, WorkerBacklogError>
    where
        F: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        let mut final_recheck = Some(final_recheck);
        let item = self.load_item(binding_id, &params.item_id).await?;
        let execution = self
            .get_execution(
                binding_id,
                thread_uuid,
                &params.item_id,
                &params.execution_ref,
            )
            .await?;
        validate_execution_identity(&execution, binding_id, &params.item_id)?;
        let (record, task) = self
            .load_execution_authority(binding_id, thread_uuid, &execution)
            .await?;
        if item.current_execution_ref.as_deref() != Some(execution.execution_ref.as_str()) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "The stored Worker execution is not the current execution for this backlog item.",
            ));
        }

        if let Some(stored) = execution.completion.as_ref() {
            return self
                .replay_committed_completion(
                    binding_id,
                    &record.thread_uuid,
                    item,
                    &execution,
                    stored,
                    &params,
                    final_recheck
                        .take()
                        .expect("completion recheck closure must be available"),
                )
                .await;
        }

        let outcome = execution.outcome.as_ref().ok_or_else(|| {
            WorkerBacklogError::new(
                "outcome_required",
                "Worker completion requires a stored successful outcome.",
            )
        })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(WorkerBacklogError::new(
                "outcome_required",
                "Worker completion requires a stored successful outcome.",
            ));
        }
        if item.status != BacklogStatus::Doing {
            return Err(WorkerBacklogError::new(
                "invalid_state",
                "Worker completion requires the backlog item to still be in Doing.",
            ));
        }
        let stored_result_digest = outcome.result_digest.as_deref().ok_or_else(|| {
            WorkerBacklogError::new(
                "corrupt_worker_state",
                "A successful Worker outcome is missing its stored result digest.",
            )
        })?;
        if stored_result_digest != params.expected_result_digest {
            return Err(WorkerBacklogError::new(
                "result_digest_mismatch",
                "The expected result digest does not match the stored successful outcome.",
            ));
        }

        let client = self.host_client(binding_id, &record.thread_uuid)?;
        let inspection = client
            .inspect_task_delivery(&record.approval_id, &task.task_id)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        let snapshot = client
            .get_task_live_or_stopped(&record.approval_id, &task.task_id)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        verify_snapshot_identity(&snapshot, &record, &task)?;
        verify_delivery_identity(&inspection, &record, &task, &execution, &snapshot)?;
        if inspection.result_digest != params.expected_result_digest {
            return Err(WorkerBacklogError::new(
                "result_digest_mismatch",
                "The actual Worker result digest changed since the stored successful outcome.",
            ));
        }
        if inspection.delivery_digest != params.expected_delivery_digest {
            return Err(WorkerBacklogError::new(
                "delivery_digest_mismatch",
                "The actual delivery bytes no longer match the expected delivery digest.",
            ));
        }
        client
            .recheck_task_delivery(&record.approval_id, &task.task_id, &inspection)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        final_recheck
            .take()
            .expect("completion recheck closure must be available")()?;

        let completion = self
            .complete_core(binding_id, &record.thread_uuid, &params)
            .await?;
        validate_completion_receipt(&completion, binding_id, &params)?;
        Ok(WorkerBacklogCompleteOutcome {
            completion,
            delivery: Some(inspection),
        })
    }

    /// Verifies that an already committed durable `Done` receipt was recorded in core under
    /// the exact caller `command_key`, `expected_revision`, execution reference, and completion
    /// payload. This never issues a new `Doing -> Done` transition and never reads a mutable
    /// checkout.
    pub(crate) async fn verify_committed_completion<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogCompleteParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogCompletionReceipt, WorkerBacklogError>
    where
        F: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        let item = self.load_item(binding_id, &params.item_id).await?;
        let execution = self
            .get_execution(
                binding_id,
                thread_uuid,
                &params.item_id,
                &params.execution_ref,
            )
            .await?;
        validate_execution_identity(&execution, binding_id, &params.item_id)?;
        let (record, _task) = self
            .load_execution_authority(binding_id, thread_uuid, &execution)
            .await?;
        if item.current_execution_ref.as_deref() != Some(execution.execution_ref.as_str()) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "The stored Worker execution is not the current execution for this backlog item.",
            ));
        }
        let Some(stored) = execution.completion.as_ref() else {
            return Err(WorkerBacklogError::new(
                "completion_conflict",
                "The backlog item does not have a matching durable completion.",
            ));
        };
        let outcome = self
            .replay_committed_completion(
                binding_id,
                &record.thread_uuid,
                item,
                &execution,
                stored,
                &params,
                final_recheck,
            )
            .await?;
        Ok(outcome.completion)
    }

    #[allow(clippy::too_many_arguments)]
    async fn replay_committed_completion<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        item: BacklogItem,
        execution: &WorkerBacklogExecution,
        stored: &WorkerBacklogCompletionRecord,
        params: &WorkerBacklogCompleteParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogCompleteOutcome, WorkerBacklogError>
    where
        F: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        // `load_item` and `get_execution` are separate reads; if a concurrent commit completed
        // the item between them, refresh the item once before treating the mismatch as corrupt.
        let item = if item.status != BacklogStatus::Done {
            self.load_item(binding_id, &params.item_id).await?
        } else {
            item
        };
        if item.status != BacklogStatus::Done {
            return Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "A stored Worker completion does not have a Done backlog item.",
            ));
        }
        if item.current_execution_ref.as_deref() != Some(execution.execution_ref.as_str()) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "The stored Worker execution is not the current execution for this backlog item.",
            ));
        }
        if params.expected_revision.checked_add(1) != Some(item.revision)
            || stored.result_digest != params.expected_result_digest
            || stored.delivery_digest != params.expected_delivery_digest
            || stored.acceptance_note != params.acceptance_note
            || stored.acceptance_evidence != params.acceptance_evidence
        {
            return Err(WorkerBacklogError::new(
                "completion_conflict",
                "The backlog item is already completed with a different durable payload.",
            ));
        }
        final_recheck()?;
        let completion = self
            .complete_core(binding_id, thread_uuid, params)
            .await
            .map_err(|error| match error.code {
                "worker_backlog_unavailable"
                | "supervisor_unavailable"
                | "corrupt_state"
                | "security_violation" => error,
                _ => WorkerBacklogError::new(
                    "completion_conflict",
                    "The durable completion cannot be replayed with this command key or payload.",
                ),
            })?;
        validate_completion_receipt(&completion, binding_id, params)?;
        if completion.item != item || completion.execution != *execution {
            return Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The durable Worker completion replay does not match the current Done state.",
            ));
        }
        Ok(WorkerBacklogCompleteOutcome {
            completion,
            delivery: None,
        })
    }

    /// Rejects Coworker acceptance of a technically successful Worker delivery.
    ///
    /// A first rejection re-reads the exact current item/execution, the stored successful
    /// terminal result, and the genuine physical delivery, then requires the caller's exact
    /// expected digests before committing a separate durable rejection. The original
    /// successful outcome is never rewritten as a failure and the item never becomes `Done`.
    /// A missing or changed checkout is a refusal, never a rejection by absence. An exact
    /// durable replay returns the original rejection receipt without reading a mutable
    /// checkout, even after a newer authorized attempt exists. Changed command keys,
    /// payloads, execution references, or revisions are refused rather than reinterpreted.
    pub(crate) async fn reject<F>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: WorkerBacklogRejectParams,
        final_recheck: F,
    ) -> Result<WorkerBacklogRejectOutcome, WorkerBacklogError>
    where
        F: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        let mut final_recheck = Some(final_recheck);
        let item = self.load_item(binding_id, &params.item_id).await?;
        let execution = self
            .get_execution(
                binding_id,
                thread_uuid,
                &params.item_id,
                &params.execution_ref,
            )
            .await?;
        validate_execution_identity(&execution, binding_id, &params.item_id)?;
        let (record, task) = self
            .load_execution_authority(binding_id, thread_uuid, &execution)
            .await?;

        // Durable replay is resolved before current-item or physical-checkout checks: the
        // original rejection remains the immutable receipt even after a newer attempt exists.
        if let Some(stored) = execution.acceptance_rejection.as_ref() {
            if stored.result_digest != params.expected_result_digest
                || stored.delivery_digest != params.expected_delivery_digest
                || stored.reason != params.rejection_reason
                || stored.evidence != params.rejection_evidence
            {
                return Err(WorkerBacklogError::new(
                    "rejection_conflict",
                    "The backlog item already has a different durable acceptance rejection.",
                ));
            }
            final_recheck
                .take()
                .expect("rejection recheck closure must be available")()?;
            let rejection = self
                .reject_core(binding_id, &record.thread_uuid, &params)
                .await
                .map_err(|error| match error.code {
                    "worker_backlog_unavailable" | "supervisor_unavailable" => error,
                    _ => WorkerBacklogError::new(
                        "rejection_conflict",
                        "The durable acceptance rejection cannot be replayed with this command key, payload, execution reference, or revision.",
                    ),
                })?;
            validate_rejection_receipt(
                &rejection,
                binding_id,
                &params.item_id,
                &params.execution_ref,
                &params.expected_result_digest,
                &params.expected_delivery_digest,
                &params.rejection_reason,
                &params.rejection_evidence,
            )?;
            return Ok(WorkerBacklogRejectOutcome {
                rejection,
                delivery: None,
            });
        }

        if item.current_execution_ref.as_deref() != Some(execution.execution_ref.as_str()) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "The stored Worker execution is not the current execution for this backlog item.",
            ));
        }
        if execution.completion.is_some() {
            return Err(WorkerBacklogError::new(
                "invalid_state",
                "A completed Worker execution cannot be rejected.",
            ));
        }
        let outcome = execution.outcome.as_ref().ok_or_else(|| {
            WorkerBacklogError::new(
                "outcome_required",
                "Worker acceptance rejection requires a stored successful outcome.",
            )
        })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(WorkerBacklogError::new(
                "outcome_required",
                "Worker acceptance rejection requires a stored successful outcome.",
            ));
        }
        if item.status != BacklogStatus::Doing {
            return Err(WorkerBacklogError::new(
                "invalid_state",
                "Worker acceptance rejection requires the backlog item to still be in Doing.",
            ));
        }
        let stored_result_digest = outcome.result_digest.as_deref().ok_or_else(|| {
            WorkerBacklogError::new(
                "corrupt_worker_state",
                "A successful Worker outcome is missing its stored result digest.",
            )
        })?;
        if stored_result_digest != params.expected_result_digest {
            return Err(WorkerBacklogError::new(
                "result_digest_mismatch",
                "The expected result digest does not match the stored successful outcome.",
            ));
        }

        let client = self.host_client(binding_id, &record.thread_uuid)?;
        let inspection = client
            .inspect_task_delivery(&record.approval_id, &task.task_id)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        let snapshot = client
            .get_task_live_or_stopped(&record.approval_id, &task.task_id)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        verify_snapshot_identity(&snapshot, &record, &task)?;
        verify_delivery_identity(&inspection, &record, &task, &execution, &snapshot)?;
        if inspection.result_digest != params.expected_result_digest {
            return Err(WorkerBacklogError::new(
                "result_digest_mismatch",
                "The actual Worker result digest changed since the stored successful outcome.",
            ));
        }
        if inspection.delivery_digest != params.expected_delivery_digest {
            return Err(WorkerBacklogError::new(
                "delivery_digest_mismatch",
                "The actual delivery bytes no longer match the expected delivery digest.",
            ));
        }
        client
            .recheck_task_delivery(&record.approval_id, &task.task_id, &inspection)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        final_recheck
            .take()
            .expect("rejection recheck closure must be available")()?;

        let rejection = self
            .reject_core(binding_id, &record.thread_uuid, &params)
            .await?;
        validate_rejection_receipt(
            &rejection,
            binding_id,
            &params.item_id,
            &params.execution_ref,
            &params.expected_result_digest,
            &params.expected_delivery_digest,
            &params.rejection_reason,
            &params.rejection_evidence,
        )?;
        Ok(WorkerBacklogRejectOutcome {
            rejection,
            delivery: Some(inspection),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn reconcile_start<T, B, V, R>(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: &WorkerBacklogStartParams,
        record: &ApprovalRecord,
        task: &ApprovedTaskRecord,
        execution: WorkerBacklogExecution,
        startup_deadline: Option<Instant>,
        managed_host_notice: ManagedHostNoticeRequest,
        blocking_recheck: B,
        verify_on_task: V,
        post_dispatch_recheck: R,
    ) -> Result<WorkerBacklogStartOutcome, WorkerBacklogError>
    where
        T: Send + 'static,
        B: Fn() -> Result<T, CallToolResult> + Send + Sync + 'static,
        V: Fn(T) -> Result<(), CallToolResult>,
        R: FnOnce() -> Result<(), WorkerBacklogError> + Send,
    {
        let mut post_dispatch_recheck = Some(post_dispatch_recheck);
        if execution.outcome.is_some() || execution.completion.is_some() {
            let host_status = self
                .observed_host_status(
                    binding_id,
                    thread_uuid,
                    record,
                    task,
                    &execution,
                    startup_deadline,
                )
                .await?;
            return Ok(WorkerBacklogStartOutcome {
                execution,
                host_status,
            });
        }

        if execution.dispatch_receipt.is_some() {
            let host_status = self
                .observed_host_status(
                    binding_id,
                    thread_uuid,
                    record,
                    task,
                    &execution,
                    startup_deadline,
                )
                .await?;
            return Ok(WorkerBacklogStartOutcome {
                execution,
                host_status,
            });
        }

        let client = self.host_client(binding_id, thread_uuid)?;
        let observed = client
            .get_task_live_or_stopped(&record.approval_id, &task.task_id)
            .await;
        match observed {
            Ok(snapshot) => {
                verify_snapshot_identity(&snapshot, record, task)?;
                if snapshot.run_id.is_some() || snapshot.worker_id.is_some() {
                    let item = self.load_item(binding_id, &params.item_id).await?;
                    post_dispatch_recheck
                        .take()
                        .expect("post-dispatch recheck must be available")()?;
                    let execution = self
                        .ensure_snapshot_receipt(
                            binding_id,
                            thread_uuid,
                            &item,
                            execution,
                            &snapshot,
                        )
                        .await?;
                    return Ok(WorkerBacklogStartOutcome {
                        execution,
                        host_status: Some(snapshot.status),
                    });
                }
                if snapshot.status == TaskExecutionStatus::Unknown {
                    return Err(WorkerBacklogError::new(
                        "worker_effect_unknown",
                        "An unresolved dispatch intent or unknown Worker effect exists; no new dispatch was attempted.",
                    ));
                }
                if !matches!(
                    snapshot.status,
                    TaskExecutionStatus::NotStarted | TaskExecutionStatus::DependencyInputsReady
                ) {
                    return Err(WorkerBacklogError::new(
                        "host_state_mismatch",
                        "The host task state is not a demonstrably unstarted original task.",
                    ));
                }
            }
            Err(error) if is_missing_host_session(&error) => {}
            Err(error) => {
                return Err(WorkerBacklogError::from_host_error(&error));
            }
        }

        if startup_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(WorkerBacklogError::new(
                "host_not_ready",
                "The approved host startup deadline expired before Worker dispatch.",
            ));
        }
        let recheck_value = blocking_recheck().map_err(WorkerBacklogError::from_tool_result)?;
        verify_on_task(recheck_value).map_err(WorkerBacklogError::from_tool_result)?;

        let start_result = execute_start_approved_task_with_launcher_until(
            Arc::clone(&self.authority),
            self.sessions_dir.clone(),
            binding_id.to_string(),
            thread_uuid.to_string(),
            StartApprovedTaskParams {
                approval_id: record.approval_id.clone(),
                task_id: task.task_id.clone(),
            },
            self.host_launcher.clone(),
            managed_host_notice,
            startup_deadline,
            blocking_recheck,
            verify_on_task,
        )
        .await;
        if let Err(result) = start_result {
            return Err(WorkerBacklogError::from_tool_result(result));
        }
        post_dispatch_recheck
            .take()
            .expect("post-dispatch recheck must be available")()?;

        let snapshot = client
            .get_task_live_or_stopped(&record.approval_id, &task.task_id)
            .await
            .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
        verify_snapshot_identity(&snapshot, record, task)?;
        let item = self.load_item(binding_id, &params.item_id).await?;
        let execution = self
            .ensure_snapshot_receipt(binding_id, thread_uuid, &item, execution, &snapshot)
            .await?;
        if execution.dispatch_receipt.is_none() {
            return Err(WorkerBacklogError::new(
                "worker_start_unconfirmed",
                "The approved start returned without a reconcilable sealed Worker receipt.",
            ));
        }
        Ok(WorkerBacklogStartOutcome {
            execution,
            host_status: Some(snapshot.status),
        })
    }

    async fn load_current_reserved_execution(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        item_id: &str,
        record: &ApprovalRecord,
        task: &ApprovedTaskRecord,
        reservation: &WorkerBacklogExecution,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        let current = self
            .get_execution(binding_id, thread_uuid, item_id, &reservation.execution_ref)
            .await?;
        validate_execution_identity(&current, binding_id, item_id)?;
        verify_execution_correlation(reservation, &current)?;
        if current.authority != authority_for_task(record, task) {
            return Err(WorkerBacklogError::new(
                "authority_mismatch",
                "The durable Worker execution does not match the frozen approval and task authority.",
            ));
        }
        // Resolve the exact execution authority again under the independently verified thread
        // scope. This rejects a metadata-only or wrong-thread read before any later effect.
        let (current_record, current_task) = self
            .load_execution_authority(binding_id, thread_uuid, &current)
            .await?;
        if current_record.approval_id != record.approval_id || current_task.task_id != task.task_id
        {
            return Err(WorkerBacklogError::new(
                "scope_mismatch",
                "The durable Worker execution authority does not match the reserved approval and task.",
            ));
        }
        Ok(current)
    }

    async fn observed_host_status(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        record: &ApprovalRecord,
        task: &ApprovedTaskRecord,
        execution: &WorkerBacklogExecution,
        deadline: Option<Instant>,
    ) -> Result<Option<TaskExecutionStatus>, WorkerBacklogError> {
        let client = self.host_client(binding_id, thread_uuid)?;
        let snapshot_result = match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before reading Worker state.",
                    ));
                };
                match tokio::time::timeout(
                    remaining,
                    client.get_task_live_or_stopped(&record.approval_id, &task.task_id),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => return Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before reading Worker state.",
                    )),
                }
            }
            None => {
                client
                    .get_task_live_or_stopped(&record.approval_id, &task.task_id)
                    .await
            }
        };
        match snapshot_result {
            Ok(snapshot) => {
                verify_snapshot_identity(&snapshot, record, task)?;
                if let Some(receipt) = execution.dispatch_receipt.as_ref() {
                    if snapshot.run_id.is_some() || snapshot.worker_id.is_some() {
                        verify_receipt_matches_snapshot(receipt, &snapshot)?;
                    } else {
                        return Err(WorkerBacklogError::new(
                            "host_state_mismatch",
                            "The stored Worker dispatch receipt has no matching genuine host invocation.",
                        ));
                    }
                }
                Ok(Some(snapshot.status))
            }
            Err(error) if is_missing_host_session(&error) => Ok(None),
            Err(error) => Err(WorkerBacklogError::from_host_error(&error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn reconcile_known_outcome<F>(
        &self,
        client: &ApprovedHostClient,
        binding_id: &str,
        item: &BacklogItem,
        execution: WorkerBacklogExecution,
        snapshot: &TaskStateSnapshot,
        record: &ApprovalRecord,
        task: &ApprovedTaskRecord,
        final_recheck: F,
    ) -> Result<WorkerBacklogResultOutcome, WorkerBacklogError>
    where
        F: Fn() -> Result<(), WorkerBacklogError> + Send,
    {
        match snapshot.status {
            TaskExecutionStatus::ReadyForReview => {
                final_recheck()?;
                let execution = self
                    .ensure_snapshot_receipt(
                        binding_id,
                        &record.thread_uuid,
                        item,
                        execution,
                        snapshot,
                    )
                    .await?;
                let inspection = client
                    .inspect_task_delivery(&record.approval_id, &task.task_id)
                    .await
                    .map_err(|error| WorkerBacklogError::from_host_error(&error))?;
                verify_delivery_identity(&inspection, record, task, &execution, snapshot)?;
                final_recheck()?;
                let execution = self
                    .record_outcome(
                        binding_id,
                        &record.thread_uuid,
                        execution,
                        WorkerBacklogOutcome::Success,
                        Some(inspection.result_digest.clone()),
                        OUTCOME_NOTE_SUCCESS,
                    )
                    .await?;
                Ok(WorkerBacklogResultOutcome {
                    execution,
                    host_status: Some(snapshot.status),
                    delivery: Some(inspection),
                    item_snapshot: None,
                    worker_evidence: None,
                })
            }
            TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut => {
                final_recheck()?;
                let execution = self
                    .ensure_snapshot_receipt(
                        binding_id,
                        &record.thread_uuid,
                        item,
                        execution,
                        snapshot,
                    )
                    .await?;
                let digest = failure_result_digest(record, task, &execution, snapshot)?;
                let failure_note = failure_outcome_note(snapshot)?;
                final_recheck()?;
                let execution = self
                    .record_outcome(
                        binding_id,
                        &record.thread_uuid,
                        execution,
                        WorkerBacklogOutcome::Failure,
                        Some(digest),
                        failure_note,
                    )
                    .await?;
                Ok(WorkerBacklogResultOutcome {
                    execution,
                    host_status: Some(snapshot.status),
                    delivery: None,
                    item_snapshot: None,
                    worker_evidence: None,
                })
            }
            _ => Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: Some(snapshot.status),
                delivery: None,
                item_snapshot: None,
                worker_evidence: None,
            }),
        }
    }

    async fn ensure_snapshot_receipt(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        item: &BacklogItem,
        execution: WorkerBacklogExecution,
        snapshot: &TaskStateSnapshot,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        // Reconcile the durable current execution before building any mutation payload. A prior
        // attach reply may have been lost, or another exact operation may have advanced the item
        // revision after the caller read it.
        let current = self
            .get_execution(
                binding_id,
                thread_uuid,
                &item.item_id,
                &execution.execution_ref,
            )
            .await?;
        validate_execution_identity(&current, binding_id, &item.item_id)?;
        verify_execution_correlation(&execution, &current)?;

        let Some(receipt) = receipt_from_snapshot(snapshot)? else {
            if current.dispatch_receipt.is_some() {
                return Err(WorkerBacklogError::new(
                    "host_state_mismatch",
                    "The stored Worker dispatch receipt has no matching genuine host invocation.",
                ));
            }
            return Ok(current);
        };
        if let Some(existing) = current.dispatch_receipt.as_ref() {
            if existing != &receipt {
                return Err(WorkerBacklogError::new(
                    "receipt_mismatch",
                    "The stored Worker dispatch receipt does not match the genuine host invocation.",
                ));
            }
            return Ok(current);
        }
        if current
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.outcome != WorkerBacklogOutcome::Unknown)
        {
            return Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "A known Worker outcome exists without its exact dispatch receipt.",
            ));
        }
        let current_item = self.load_item(binding_id, &item.item_id).await?;
        if current_item.current_execution_ref.as_deref() != Some(current.execution_ref.as_str()) {
            return Err(WorkerBacklogError::new(
                "execution_not_current",
                "Only the current Worker reservation may attach its genuine dispatch receipt.",
            ));
        }
        let request = OperatorWorkerBacklogRequest::AttachDispatchReceipt {
            binding_id: current.binding_id.clone(),
            thread_uuid: thread_uuid.to_string(),
            command_key: derived_command_key("attach", &current.execution_ref),
            item_id: current.item_id.clone(),
            execution_ref: current.execution_ref.clone(),
            expected_revision: current_item.revision,
            receipt: receipt.clone(),
        };
        match self.call_execution(request, None).await {
            Ok(attached) => {
                validate_execution_identity(&attached, binding_id, &item.item_id)?;
                verify_execution_correlation(&current, &attached)?;
                Ok(attached)
            }
            Err(error) if error.code() == "idempotency_conflict" => {
                // A concurrent exact attach can advance the item revision while producing the
                // same durable receipt. Re-read and accept only the exact expected receipt.
                let reconciled = self
                    .get_execution(
                        binding_id,
                        thread_uuid,
                        &item.item_id,
                        &current.execution_ref,
                    )
                    .await?;
                validate_execution_identity(&reconciled, binding_id, &item.item_id)?;
                verify_execution_correlation(&current, &reconciled)?;
                match reconciled.dispatch_receipt.as_ref() {
                    Some(existing) if existing == &receipt => Ok(reconciled),
                    Some(_) => Err(WorkerBacklogError::new(
                        "receipt_mismatch",
                        "The stored Worker dispatch receipt does not match the genuine host invocation.",
                    )),
                    None => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn record_unknown(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        execution: WorkerBacklogExecution,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        self.record_outcome(
            binding_id,
            thread_uuid,
            execution,
            WorkerBacklogOutcome::Unknown,
            None,
            OUTCOME_NOTE_UNKNOWN,
        )
        .await
    }

    async fn record_outcome(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        execution: WorkerBacklogExecution,
        outcome: WorkerBacklogOutcome,
        result_digest: Option<String>,
        note: &'static str,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        let current = self
            .get_execution(
                binding_id,
                thread_uuid,
                &execution.item_id,
                &execution.execution_ref,
            )
            .await?;
        validate_execution_identity(&current, binding_id, &execution.item_id)?;
        verify_execution_correlation(&execution, &current)?;
        if let Some(existing) = current.outcome.as_ref() {
            if existing.outcome == outcome && existing.result_digest == result_digest {
                return Ok(current);
            }
            let reconciling_unknown = existing.outcome == WorkerBacklogOutcome::Unknown
                && outcome != WorkerBacklogOutcome::Unknown;
            if !reconciling_unknown {
                return Err(WorkerBacklogError::new(
                    "outcome_conflict",
                    "The stored Worker execution already has a different recorded outcome.",
                ));
            }
        }
        if outcome != WorkerBacklogOutcome::Unknown && current.dispatch_receipt.is_none() {
            return Err(WorkerBacklogError::new(
                "receipt_required",
                "A known Worker outcome requires the attached genuine dispatch receipt.",
            ));
        }
        let current_item = self.load_item(binding_id, &current.item_id).await?;
        let operation = match outcome {
            WorkerBacklogOutcome::Success => "outcome-success",
            WorkerBacklogOutcome::Failure => "outcome-failure",
            WorkerBacklogOutcome::Unknown => "outcome-unknown",
        };
        let request = OperatorWorkerBacklogRequest::RecordOutcome {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            command_key: derived_command_key(operation, &current.execution_ref),
            item_id: current.item_id.clone(),
            execution_ref: current.execution_ref.clone(),
            expected_revision: current_item.revision,
            report: WorkerBacklogOutcomeReport {
                outcome,
                note: note.to_string(),
                result_digest: result_digest.clone(),
            },
        };
        match self.call_execution(request, None).await {
            Ok(recorded) => {
                validate_execution_identity(&recorded, binding_id, &current.item_id)?;
                verify_execution_correlation(&current, &recorded)?;
                let exact_outcome = recorded.outcome.as_ref().is_some_and(|stored| {
                    stored.outcome == outcome && stored.result_digest == result_digest
                });
                if !exact_outcome {
                    return Err(WorkerBacklogError::new(
                        "corrupt_worker_state",
                        "Worker outcome recording returned a different durable outcome.",
                    ));
                }
                Ok(recorded)
            }
            Err(error) if error.code() == "idempotency_conflict" => {
                // Another exact operation may have recorded the same durable outcome after our
                // read. Accept only the identical stored outcome; never suppress a real conflict.
                let reconciled = self
                    .get_execution(
                        binding_id,
                        thread_uuid,
                        &current.item_id,
                        &current.execution_ref,
                    )
                    .await?;
                validate_execution_identity(&reconciled, binding_id, &current.item_id)?;
                verify_execution_correlation(&current, &reconciled)?;
                let exact_outcome = reconciled.outcome.as_ref().is_some_and(|stored| {
                    stored.outcome == outcome && stored.result_digest == result_digest
                });
                if exact_outcome {
                    Ok(reconciled)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn reserve_until(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: &WorkerBacklogStartParams,
        authority: WorkerBacklogAuthority,
        deadline: Option<Instant>,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        let request = OperatorWorkerBacklogRequest::Reserve {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            command_key: params.command_key.clone(),
            item_id: params.item_id.clone(),
            expected_revision: params.expected_revision,
            authority,
        };
        let execution = self.call_execution(request, deadline).await?;
        validate_execution_identity(&execution, binding_id, &params.item_id)?;
        Ok(execution)
    }

    async fn complete_core(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: &WorkerBacklogCompleteParams,
    ) -> Result<WorkerBacklogCompletionReceipt, WorkerBacklogError> {
        let request = OperatorWorkerBacklogRequest::Complete {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            command_key: params.command_key.clone(),
            item_id: params.item_id.clone(),
            expected_revision: params.expected_revision,
            completion: WorkerBacklogCompletion {
                execution_ref: params.execution_ref.clone(),
                result_digest: params.expected_result_digest.clone(),
                acceptance_note: params.acceptance_note.clone(),
                acceptance_evidence: params.acceptance_evidence.clone(),
                delivery_digest: params.expected_delivery_digest.clone(),
            },
        };
        match self.call(request, None).await? {
            OperatorWorkerBacklogResponse::Completion { completion } => Ok(completion),
            _ => Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The control supervisor returned an unexpected completion response.",
            )),
        }
    }

    async fn reject_core(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        params: &WorkerBacklogRejectParams,
    ) -> Result<WorkerBacklogAcceptanceRejectionReceipt, WorkerBacklogError> {
        let request = OperatorWorkerBacklogRequest::RejectAcceptance {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            command_key: params.command_key.clone(),
            item_id: params.item_id.clone(),
            expected_revision: params.expected_revision,
            rejection: WorkerBacklogAcceptanceRejection {
                execution_ref: params.execution_ref.clone(),
                result_digest: params.expected_result_digest.clone(),
                delivery_digest: params.expected_delivery_digest.clone(),
                reason: params.rejection_reason.clone(),
                evidence: params.rejection_evidence.clone(),
            },
        };
        match self.call(request, None).await? {
            OperatorWorkerBacklogResponse::AcceptanceRejection { rejection } => Ok(rejection),
            _ => Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The control supervisor returned an unexpected acceptance rejection response.",
            )),
        }
    }

    async fn get_execution(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        item_id: &str,
        execution_ref: &str,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        let request = OperatorWorkerBacklogRequest::GetExecution {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            item_id: item_id.to_string(),
            execution_ref: execution_ref.to_string(),
        };
        self.call_execution(request, None).await
    }

    async fn call_execution(
        &self,
        request: OperatorWorkerBacklogRequest,
        deadline: Option<Instant>,
    ) -> Result<WorkerBacklogExecution, WorkerBacklogError> {
        match self.call(request, deadline).await? {
            OperatorWorkerBacklogResponse::Execution { execution } => Ok(execution),
            _ => Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The control supervisor returned an unexpected Worker execution response.",
            )),
        }
    }

    async fn call(
        &self,
        request: OperatorWorkerBacklogRequest,
        deadline: Option<Instant>,
    ) -> Result<OperatorWorkerBacklogResponse, WorkerBacklogError> {
        let fut = self.supervisor.worker_backlog_operator_attested(&request);
        match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before the Worker backlog operation.",
                    ));
                };
                match tokio::time::timeout(remaining, fut).await {
                    Ok(result) => result.map_err(|error| {
                        WorkerBacklogError::from_runtime_error(&error)
                    }),
                    Err(_) => Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before the Worker backlog operation.",
                    )),
                }
            }
            None => fut
                .await
                .map_err(|error| WorkerBacklogError::from_runtime_error(&error)),
        }
    }

    async fn load_item(
        &self,
        binding_id: &str,
        item_id: &str,
    ) -> Result<BacklogItem, WorkerBacklogError> {
        let response = self
            .supervisor
            .execute_scoped(
                binding_id,
                &ScopedRequest::BacklogGet {
                    item_id: item_id.to_string(),
                },
            )
            .await
            .map_err(|error| WorkerBacklogError::from_runtime_error(&error))?;
        let item = match response {
            ScopedResponse::BacklogItem(item) => item,
            _ => {
                return Err(WorkerBacklogError::new(
                    "corrupt_worker_state",
                    "The control supervisor returned an unexpected backlog item response.",
                ))
            }
        };
        if item.binding_id != binding_id || item.item_id != item_id {
            return Err(WorkerBacklogError::new(
                "scope_mismatch",
                "The backlog item does not belong to the enrolled chat binding.",
            ));
        }
        Ok(item)
    }

    async fn load_item_until(
        &self,
        binding_id: &str,
        item_id: &str,
        deadline: Option<Instant>,
    ) -> Result<BacklogItem, WorkerBacklogError> {
        match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before reading the backlog item.",
                    ));
                };
                match tokio::time::timeout(remaining, self.load_item(binding_id, item_id)).await {
                    Ok(result) => result,
                    Err(_) => Err(WorkerBacklogError::new(
                        "host_not_ready",
                        "The approved host startup deadline expired before reading the backlog item.",
                    )),
                }
            }
            None => self.load_item(binding_id, item_id).await,
        }
    }

    async fn load_record_until(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        approval_id: &str,
        deadline: Option<Instant>,
    ) -> Result<ApprovalRecord, WorkerBacklogError> {
        let authority = Arc::clone(&self.authority);
        let approval_id = approval_id.to_string();
        let load = async move {
            tokio::task::spawn_blocking(move || authority.get_for_operator(&approval_id))
                .await
                .map_err(|_| {
                    WorkerBacklogError::new(
                        "supervisor_unavailable",
                        "The authority store read task failed to complete.",
                    )
                })?
                .map_err(|error| WorkerBacklogError::from_authority_error(&error))
        };
        let record =
            match deadline {
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(WorkerBacklogError::new(
                            "host_not_ready",
                            "The approved host startup deadline expired before reading authority.",
                        ));
                    };
                    match tokio::time::timeout(remaining, load).await {
                        Ok(result) => result?,
                        Err(_) => return Err(WorkerBacklogError::new(
                            "host_not_ready",
                            "The approved host startup deadline expired before reading authority.",
                        )),
                    }
                }
                None => load.await?,
            };
        if record.binding_id != binding_id || record.thread_uuid != thread_uuid {
            return Err(WorkerBacklogError::new(
                "scope_mismatch",
                "The approval record does not belong to the enrolled chat binding and native thread.",
            ));
        }
        Ok(record)
    }

    async fn load_execution_authority(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        execution: &WorkerBacklogExecution,
    ) -> Result<(ApprovalRecord, ApprovedTaskRecord), WorkerBacklogError> {
        let record = self
            .load_record_until(
                binding_id,
                thread_uuid,
                &execution.authority.approval_id,
                None,
            )
            .await?;
        let task = resolve_approved_task(&record, &execution.authority.task_id)?.clone();
        let expected = authority_for_task(&record, &task);
        if execution.authority != expected
            || execution.acceptance_condition_digest.is_empty()
            || execution.delivery_description_digest.is_empty()
        {
            return Err(WorkerBacklogError::new(
                "authority_mismatch",
                "The stored Worker execution does not match the frozen approval authority.",
            ));
        }
        Ok((record, task))
    }

    fn host_client(
        &self,
        binding_id: &str,
        thread_uuid: &str,
    ) -> Result<ApprovedHostClient, WorkerBacklogError> {
        ApprovedHostClient::new(
            Arc::clone(&self.authority),
            &self.sessions_dir,
            binding_id,
            thread_uuid,
        )
        .map_err(|error| WorkerBacklogError::from_host_error(&error))
    }
}

fn resolve_approved_task<'a>(
    record: &'a ApprovalRecord,
    task_id: &str,
) -> Result<&'a ApprovedTaskRecord, WorkerBacklogError> {
    let task = record
        .tasks
        .iter()
        .find(|task| task.task_id == task_id)
        .ok_or_else(|| {
            WorkerBacklogError::new(
                "unknown_task",
                "The requested task is not part of the frozen approval record.",
            )
        })?;
    if !task.is_frozen_headless_worker() {
        return Err(WorkerBacklogError::new(
            "native_route_required",
            "The requested task requires manual native routing or lacks a complete frozen Worker contract.",
        ));
    }
    Ok(task)
}

fn authority_for_task(
    record: &ApprovalRecord,
    task: &ApprovedTaskRecord,
) -> WorkerBacklogAuthority {
    WorkerBacklogAuthority {
        approval_id: record.approval_id.clone(),
        subject_digest: record.subject_digest.clone(),
        task_id: task.task_id.clone(),
        task_digest: task.task_digest.clone(),
    }
}

fn validate_execution_identity(
    execution: &WorkerBacklogExecution,
    binding_id: &str,
    item_id: &str,
) -> Result<(), WorkerBacklogError> {
    if execution.binding_id != binding_id
        || execution.item_id != item_id
        || execution.authority.approval_id.is_empty()
        || execution.authority.task_id.is_empty()
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The stored Worker execution identity is inconsistent with the enrolled binding and item.",
        ));
    }
    Ok(())
}

fn verify_execution_correlation(
    expected: &WorkerBacklogExecution,
    current: &WorkerBacklogExecution,
) -> Result<(), WorkerBacklogError> {
    if expected.execution_ref != current.execution_ref
        || expected.binding_id != current.binding_id
        || expected.item_id != current.item_id
        || expected.authority != current.authority
        || expected.acceptance_condition_digest != current.acceptance_condition_digest
        || expected.delivery_description_digest != current.delivery_description_digest
        || expected.reservation_revision != current.reservation_revision
        || expected.reserved_at_ms != current.reserved_at_ms
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The current Worker execution does not preserve its original immutable correlation.",
        ));
    }
    Ok(())
}

fn verify_snapshot_identity(
    snapshot: &TaskStateSnapshot,
    record: &ApprovalRecord,
    task: &ApprovedTaskRecord,
) -> Result<(), WorkerBacklogError> {
    if snapshot.approval_id != record.approval_id
        || snapshot.subject_digest != record.subject_digest
        || snapshot.binding_id != record.binding_id
        || snapshot.thread_uuid != record.thread_uuid
        || snapshot.task_id != task.task_id
        || snapshot.task_digest != task.task_digest
        || snapshot.profile_id.as_deref() != task.profile_id.as_deref()
        || snapshot.command_key.as_deref() != task.command_key.as_deref()
    {
        return Err(WorkerBacklogError::new(
            "scope_mismatch",
            "The host task snapshot does not match the frozen approval and enrolled chat scope.",
        ));
    }
    if snapshot.accepted {
        return Err(WorkerBacklogError::new(
            "host_state_mismatch",
            "A Worker result must remain unaccepted at the backlog evidence boundary.",
        ));
    }
    Ok(())
}

fn worker_evidence_from_snapshot(
    snapshot: &TaskStateSnapshot,
) -> Option<WorkerBacklogWorkerEvidence> {
    let worker = snapshot.worker.as_ref()?;
    let outcome = worker.outcome.as_ref();
    Some(WorkerBacklogWorkerEvidence {
        approval_id: snapshot.approval_id.clone(),
        task_id: snapshot.task_id.clone(),
        session_binding_id: snapshot.session_binding_id.clone(),
        run_id: worker.run_id.clone(),
        worker_id: worker.worker_id.clone(),
        profile_id: worker.profile_id.clone(),
        provider: worker.provider.clone(),
        requested_model: worker.requested_model.clone(),
        requested_effort: worker.requested_effort.clone(),
        observed_model: worker.observed_model.clone(),
        observed_effort: worker.observed_effort.clone(),
        worker_status: worker.status,
        worker_revision: worker.revision,
        attempt_count: worker.attempt_count,
        started_at_ms: worker.started_at_ms,
        completed_at_ms: worker.completed_at_ms,
        duration_ms: outcome.and_then(|outcome| outcome.duration_ms),
        exit_code: outcome.and_then(|outcome| outcome.exit_code),
        signal: outcome.and_then(|outcome| outcome.signal),
        timed_out: outcome.map(|outcome| outcome.timed_out),
        cancelled: outcome.map(|outcome| outcome.cancelled),
        cancel_requested_before_completion: outcome
            .map(|outcome| outcome.cancel_requested_before_completion),
        missing_model_evidence: outcome.map(|outcome| outcome.model_evidence_rejected()),
        missing_effort_evidence: outcome.map(|outcome| outcome.effort_evidence_rejected()),
        provider_error: outcome.map(|outcome| outcome.provider_error),
        incomplete_cleanup: outcome.map(|outcome| outcome.incomplete_cleanup),
        rejected_by_diff_gate: outcome.map(|outcome| outcome.rejected_by_diff_gate),
        violation_count: outcome.map(|outcome| outcome.violation_count),
        stdout_truncated: outcome.map(|outcome| outcome.stdout_truncated),
        stderr_truncated: outcome.map(|outcome| outcome.stderr_truncated),
        task_worktree_path: worker.worktree.task_worktree_path.clone(),
    })
}

fn receipt_from_snapshot(
    snapshot: &TaskStateSnapshot,
) -> Result<Option<WorkerBacklogDispatchReceipt>, WorkerBacklogError> {
    match (&snapshot.run_id, &snapshot.worker_id) {
        (None, None) => Ok(None),
        (Some(run_id), Some(worker_id)) => {
            let profile_id = snapshot
                .profile_id
                .clone()
                .or_else(|| {
                    snapshot
                        .worker
                        .as_ref()
                        .map(|worker| worker.profile_id.clone())
                })
                .ok_or_else(|| {
                    WorkerBacklogError::new(
                        "corrupt_worker_state",
                        "A sealed Worker receipt is missing its frozen profile identity.",
                    )
                })?;
            let execution_binding_digest = match snapshot.worker.as_ref() {
                Some(worker) => {
                    if worker.run_id != *run_id
                        || worker.worker_id != *worker_id
                        || worker.profile_id != profile_id
                        || worker.binding_id != snapshot.session_binding_id
                    {
                        return Err(WorkerBacklogError::new(
                            "scope_mismatch",
                            "The Worker snapshot does not match its sealed task receipt.",
                        ));
                    }
                    worker
                        .execution_binding
                        .as_ref()
                        .map(|binding| binding.execution_digest.clone())
                }
                None => None,
            };
            Ok(Some(WorkerBacklogDispatchReceipt {
                session_binding_id: snapshot.session_binding_id.clone(),
                run_id: run_id.clone(),
                worker_id: worker_id.clone(),
                profile_id,
                execution_binding_digest,
            }))
        }
        _ => Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The host task snapshot has a partial sealed Worker invocation identity.",
        )),
    }
}

fn verify_receipt_matches_snapshot(
    receipt: &WorkerBacklogDispatchReceipt,
    snapshot: &TaskStateSnapshot,
) -> Result<(), WorkerBacklogError> {
    if receipt.session_binding_id != snapshot.session_binding_id
        || snapshot.run_id.as_deref() != Some(receipt.run_id.as_str())
        || snapshot.worker_id.as_deref() != Some(receipt.worker_id.as_str())
        || snapshot.profile_id.as_deref() != Some(receipt.profile_id.as_str())
    {
        return Err(WorkerBacklogError::new(
            "receipt_mismatch",
            "The stored Worker dispatch receipt does not match the genuine host invocation.",
        ));
    }
    if let Some(worker) = snapshot.worker.as_ref() {
        if worker.run_id != receipt.run_id
            || worker.worker_id != receipt.worker_id
            || worker.profile_id != receipt.profile_id
            || worker.binding_id != snapshot.session_binding_id
        {
            return Err(WorkerBacklogError::new(
                "receipt_mismatch",
                "The host Worker snapshot identity does not match the stored dispatch receipt.",
            ));
        }
    }
    Ok(())
}

fn verify_delivery_identity(
    inspection: &DeliveryInspection,
    record: &ApprovalRecord,
    task: &ApprovedTaskRecord,
    execution: &WorkerBacklogExecution,
    snapshot: &TaskStateSnapshot,
) -> Result<(), WorkerBacklogError> {
    let receipt = execution.dispatch_receipt.as_ref().ok_or_else(|| {
        WorkerBacklogError::new(
            "receipt_required",
            "Physical delivery inspection requires an attached genuine dispatch receipt.",
        )
    })?;
    verify_receipt_matches_snapshot(receipt, snapshot)?;
    if inspection.approval_id != record.approval_id
        || inspection.subject_digest != record.subject_digest
        || inspection.binding_id != record.binding_id
        || inspection.thread_uuid != record.thread_uuid
        || inspection.task_id != task.task_id
        || inspection.task_digest != task.task_digest
        || inspection.profile_id != receipt.profile_id
        || inspection.command_key != task.command_key.clone().unwrap_or_default()
        || inspection.run_id != receipt.run_id
        || inspection.worker_id != receipt.worker_id
        || inspection.session_binding_id != receipt.session_binding_id
    {
        return Err(WorkerBacklogError::new(
            "scope_mismatch",
            "The physical delivery inspection does not match the frozen task and dispatch identity.",
        ));
    }
    Ok(())
}

fn validate_completion_receipt(
    receipt: &WorkerBacklogCompletionReceipt,
    binding_id: &str,
    params: &WorkerBacklogCompleteParams,
) -> Result<(), WorkerBacklogError> {
    validate_execution_identity(&receipt.execution, binding_id, &params.item_id)?;
    if receipt.item.binding_id != binding_id
        || receipt.item.item_id != params.item_id
        || receipt.item.kind != BacklogItemKind::Worker
        || receipt.item.status != BacklogStatus::Done
        || receipt.item.blocked_reason.is_some()
        || params.expected_revision.checked_add(1) != Some(receipt.item.revision)
        || receipt.item.current_execution_ref.as_deref() != Some(params.execution_ref.as_str())
        || receipt.item.completion_note.as_deref() != Some(params.acceptance_note.as_str())
        || receipt.item.completion_evidence.as_deref() != Some(params.acceptance_evidence.as_str())
        || receipt.execution.execution_ref != params.execution_ref
        || receipt.execution.acceptance_rejection.is_some()
        || receipt
            .execution
            .outcome
            .as_ref()
            .map(|outcome| outcome.outcome)
            != Some(WorkerBacklogOutcome::Success)
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The durable Worker completion receipt did not match the requested completion.",
        ));
    }
    let stored = receipt.execution.completion.as_ref().ok_or_else(|| {
        WorkerBacklogError::new(
            "corrupt_worker_state",
            "The durable Worker completion receipt has no completion record.",
        )
    })?;
    if stored.result_digest != params.expected_result_digest
        || stored.delivery_digest != params.expected_delivery_digest
        || stored.acceptance_note != params.acceptance_note
        || stored.acceptance_evidence != params.acceptance_evidence
        || receipt.item.completed_at_ms != Some(stored.completed_at_ms)
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The durable Worker completion receipt did not match the requested completion.",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_rejection_receipt(
    receipt: &WorkerBacklogAcceptanceRejectionReceipt,
    binding_id: &str,
    item_id: &str,
    execution_ref: &str,
    result_digest: &str,
    delivery_digest: &str,
    reason: &str,
    evidence: &str,
) -> Result<(), WorkerBacklogError> {
    validate_execution_identity(&receipt.execution, binding_id, item_id)?;
    if receipt.item.binding_id != binding_id
        || receipt.item.item_id != item_id
        || receipt.item.status != BacklogStatus::Blocked
        || receipt.item.current_execution_ref.as_deref() != Some(execution_ref)
        || receipt.execution.execution_ref != execution_ref
        || receipt.execution.completion.is_some()
        || receipt
            .execution
            .outcome
            .as_ref()
            .map(|outcome| outcome.outcome)
            != Some(WorkerBacklogOutcome::Success)
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The durable Worker acceptance rejection receipt did not preserve the expected execution state.",
        ));
    }
    let stored = receipt
        .execution
        .acceptance_rejection
        .as_ref()
        .ok_or_else(|| {
            WorkerBacklogError::new(
                "corrupt_worker_state",
                "The durable Worker acceptance rejection receipt has no rejection record.",
            )
        })?;
    if stored.result_digest != result_digest
        || stored.delivery_digest != delivery_digest
        || stored.reason != reason
        || stored.evidence != evidence
    {
        return Err(WorkerBacklogError::new(
            "corrupt_worker_state",
            "The durable Worker acceptance rejection receipt did not match the requested rejection.",
        ));
    }
    Ok(())
}

fn failure_result_digest(
    record: &ApprovalRecord,
    task: &ApprovedTaskRecord,
    execution: &WorkerBacklogExecution,
    snapshot: &TaskStateSnapshot,
) -> Result<String, WorkerBacklogError> {
    let receipt = execution.dispatch_receipt.as_ref().ok_or_else(|| {
        WorkerBacklogError::new(
            "receipt_required",
            "A failed Worker outcome requires an attached genuine dispatch receipt.",
        )
    })?;
    verify_receipt_matches_snapshot(receipt, snapshot)?;
    let worker = snapshot.worker.as_ref().ok_or_else(|| {
        WorkerBacklogError::new(
            "corrupt_worker_state",
            "A failed Worker outcome is missing its verified worker snapshot.",
        )
    })?;
    if TaskExecutionStatus::from_worker_snapshot(worker) != snapshot.status {
        return Err(WorkerBacklogError::new(
            "host_state_mismatch",
            "The host task status does not match its verified terminal Worker snapshot.",
        ));
    }
    if !matches!(
        snapshot.status,
        TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut
    ) {
        return Err(WorkerBacklogError::new(
            "host_state_mismatch",
            "The host task status is not a verified terminal Worker failure.",
        ));
    }
    let outcome = worker.outcome.as_ref();
    let encoding = FailureResultEncoding {
        schema: FAILURE_RESULT_DIGEST_SCHEMA,
        approval_id: &record.approval_id,
        subject_digest: &record.subject_digest,
        binding_id: &record.binding_id,
        thread_uuid: &record.thread_uuid,
        session_binding_id: &snapshot.session_binding_id,
        task_id: &task.task_id,
        task_digest: &task.task_digest,
        profile_id: &receipt.profile_id,
        profile_digest: &worker.profile_digest,
        command_key: task.command_key.as_deref().unwrap_or_default(),
        run_id: &receipt.run_id,
        worker_id: &receipt.worker_id,
        worker_revision: worker.revision,
        attempt_count: worker.attempt_count,
        provider: &worker.provider,
        requested_model: &worker.requested_model,
        requested_effort: &worker.requested_effort,
        observed_model: worker.observed_model.as_deref(),
        observed_effort: worker.observed_effort.as_deref(),
        execution_binding_digest: receipt.execution_binding_digest.as_deref(),
        base_commit: &worker.worktree.base_commit,
        head_commit: worker.worktree.head_commit.as_deref(),
        worker_status: worker.status,
        task_status: snapshot.status,
        outcome: outcome.map(FailureOutcomeEncoding::from),
    };
    let bytes = serde_json::to_vec(&encoding).map_err(|_| {
        WorkerBacklogError::new(
            "corrupt_worker_state",
            "The verified terminal Worker identity could not be encoded for its result digest.",
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

/// Software-owned failure categories derived only from structured terminal Worker evidence.
///
/// This never copies provider prose, raw stderr, paths, or arbitrary failure text into the
/// persisted outcome note or blocked reason.
#[derive(Clone, Copy, Default)]
struct FailureNoteEvidence {
    evidence_failure: Option<WorkerEvidenceFailure>,
    timed_out: bool,
    cancelled: bool,
    rejected_by_diff_gate: bool,
    missing_model_evidence: bool,
    missing_effort_evidence: bool,
    incomplete_cleanup: bool,
    provider_error: bool,
    exit_code: Option<i32>,
    signal: Option<i32>,
}

impl FailureNoteEvidence {
    fn from_snapshot(snapshot: &TaskStateSnapshot) -> Result<Self, WorkerBacklogError> {
        let worker = snapshot.worker.as_ref().ok_or_else(|| {
            WorkerBacklogError::new(
                "corrupt_worker_state",
                "A failed Worker outcome is missing its verified worker snapshot.",
            )
        })?;
        if TaskExecutionStatus::from_worker_snapshot(worker) != snapshot.status {
            return Err(WorkerBacklogError::new(
                "host_state_mismatch",
                "The host task status does not match its verified terminal Worker snapshot.",
            ));
        }
        let outcome = worker.outcome.as_ref();
        Ok(Self {
            evidence_failure: outcome.and_then(|outcome| outcome.evidence_failure),
            timed_out: outcome.is_some_and(|outcome| outcome.timed_out),
            cancelled: outcome.is_some_and(|outcome| outcome.cancelled),
            rejected_by_diff_gate: outcome.is_some_and(|outcome| outcome.rejected_by_diff_gate),
            missing_model_evidence: outcome.is_some_and(|outcome| outcome.missing_model_evidence),
            missing_effort_evidence: outcome.is_some_and(|outcome| outcome.missing_effort_evidence),
            incomplete_cleanup: outcome.is_some_and(|outcome| outcome.incomplete_cleanup),
            provider_error: outcome.is_some_and(|outcome| outcome.provider_error),
            exit_code: outcome.and_then(|outcome| outcome.exit_code),
            signal: outcome.and_then(|outcome| outcome.signal),
        })
    }
}

fn failure_outcome_note(snapshot: &TaskStateSnapshot) -> Result<&'static str, WorkerBacklogError> {
    Ok(failure_note_for(
        snapshot.status,
        FailureNoteEvidence::from_snapshot(snapshot)?,
    ))
}

fn failure_note_for(status: TaskExecutionStatus, evidence: FailureNoteEvidence) -> &'static str {
    match status {
        TaskExecutionStatus::TimedOut => "verified worker timeout",
        TaskExecutionStatus::Cancelled => "verified worker cancellation",
        TaskExecutionStatus::Failed => {
            if evidence.timed_out {
                "verified worker timeout"
            } else if evidence.cancelled {
                "verified worker cancellation"
            } else if evidence.rejected_by_diff_gate {
                "verified worker diff scope rejection"
            } else if let Some(reason) = evidence.evidence_failure {
                match reason {
                    WorkerEvidenceFailure::MissingModel => "verified worker missing model evidence",
                    WorkerEvidenceFailure::ModelMismatch => "verified worker model mismatch",
                    WorkerEvidenceFailure::MissingEffort => {
                        "verified worker missing effort evidence"
                    }
                    WorkerEvidenceFailure::EffortMismatch => "verified worker effort mismatch",
                }
            } else if evidence.missing_model_evidence {
                "worker model evidence unverified"
            } else if evidence.missing_effort_evidence {
                "worker effort evidence unverified"
            } else if evidence.incomplete_cleanup {
                "verified worker cleanup incomplete"
            } else if evidence.exit_code.is_some_and(|code| code != 0) {
                "verified worker nonzero exit"
            } else if evidence.signal.is_some() {
                "verified worker signal termination"
            } else if evidence.provider_error {
                "verified worker provider error"
            } else {
                "verified worker terminal failure"
            }
        }
        _ => "verified worker terminal failure",
    }
}

#[derive(Serialize)]
struct FailureResultEncoding<'a> {
    schema: &'static str,
    approval_id: &'a str,
    subject_digest: &'a str,
    binding_id: &'a str,
    thread_uuid: &'a str,
    session_binding_id: &'a str,
    task_id: &'a str,
    task_digest: &'a str,
    profile_id: &'a str,
    profile_digest: &'a str,
    command_key: &'a str,
    run_id: &'a str,
    worker_id: &'a str,
    worker_revision: u64,
    attempt_count: u32,
    provider: &'a str,
    requested_model: &'a str,
    requested_effort: &'a str,
    observed_model: Option<&'a str>,
    observed_effort: Option<&'a str>,
    execution_binding_digest: Option<&'a str>,
    base_commit: &'a str,
    head_commit: Option<&'a str>,
    worker_status: WorkerStatus,
    task_status: TaskExecutionStatus,
    outcome: Option<FailureOutcomeEncoding>,
}

#[derive(Serialize)]
struct FailureOutcomeEncoding {
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence_failure: Option<WorkerEvidenceFailure>,
    acceptance_state: WorkerAcceptanceState,
    exit_code: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    cancelled: bool,
    cancel_requested_before_completion: bool,
    missing_model_evidence: bool,
    missing_effort_evidence: bool,
    provider_error: bool,
    incomplete_cleanup: bool,
    rejected_by_diff_gate: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    changed_paths_count: u32,
    violation_count: u32,
}

impl From<&threadlane_core::WorkerOutcomeSummary> for FailureOutcomeEncoding {
    fn from(outcome: &threadlane_core::WorkerOutcomeSummary) -> Self {
        Self {
            evidence_failure: outcome.evidence_failure,
            acceptance_state: outcome.acceptance_state,
            exit_code: outcome.exit_code,
            signal: outcome.signal,
            timed_out: outcome.timed_out,
            cancelled: outcome.cancelled,
            cancel_requested_before_completion: outcome.cancel_requested_before_completion,
            missing_model_evidence: outcome.missing_model_evidence,
            missing_effort_evidence: outcome.missing_effort_evidence,
            provider_error: outcome.provider_error,
            incomplete_cleanup: outcome.incomplete_cleanup,
            rejected_by_diff_gate: outcome.rejected_by_diff_gate,
            stdout_truncated: outcome.stdout_truncated,
            stderr_truncated: outcome.stderr_truncated,
            changed_paths_count: outcome.changed_paths_count,
            violation_count: outcome.violation_count,
        }
    }
}

fn is_missing_host_session(error: &HostError) -> bool {
    matches!(
        error,
        HostError::HostNotReady {
            phase: None,
            reason,
            ..
        } if reason.as_str() == "host session directory does not exist"
            || reason.as_str() == "host-manifest.json does not exist"
    )
}

fn derived_command_key(operation: &str, execution_ref: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(DERIVED_COMMAND_DOMAIN.as_bytes());
    hasher.update([0]);
    hasher.update(operation.as_bytes());
    hasher.update([0]);
    hasher.update(execution_ref.as_bytes());
    format!("wb-{:x}", hasher.finalize())
}

/// Stable tool names exposed by this typed service.
pub(crate) const WORKER_BACKLOG_START_TOOL: &str = START_TOOL;
/// Stable result tool name.
pub(crate) const WORKER_BACKLOG_RESULT_TOOL: &str = RESULT_TOOL;
/// Stable read-only notice recovery tool name.
pub(crate) const WORKER_BACKLOG_NOTICE_CONTEXT_TOOL: &str = NOTICE_CONTEXT_TOOL;
/// Stable completion tool name.
pub(crate) const WORKER_BACKLOG_COMPLETE_TOOL: &str = COMPLETE_TOOL;
/// Stable failed-acceptance rejection tool name.
pub(crate) const WORKER_BACKLOG_REJECT_TOOL: &str = REJECT_TOOL;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        failure_note_for, valid_exact_required_changed_paths, valid_notice_required_changed_paths,
        worker_evidence_from_snapshot, FailureNoteEvidence, WorkerBacklogNoticeScope,
        WorkerBacklogResultOutcome, WorkerBacklogWorkerEvidence,
    };
    use serde_json::json;
    use threadlane_core::WorkerProgressSummary;
    use threadlane_host::{TaskExecutionStatus, TaskStateSnapshot};

    fn task_snapshot_with_worker(outcome: Option<serde_json::Value>) -> TaskStateSnapshot {
        serde_json::from_value(json!({
            "approval_id": "apr_test",
            "subject_digest": "a".repeat(64),
            "binding_id": "bnd_test",
            "thread_uuid": "0196ed00-0000-7000-8000-000000000001",
            "session_binding_id": "bnd_session",
            "task_id": "task-root",
            "task_digest": "b".repeat(64),
            "adapter": "opencode_v2",
            "provider": "opencode_v2",
            "depends_on": [],
            "profile_id": "profile-main",
            "command_key": "command-key",
            "status": if outcome.is_some() { "ready_for_review" } else { "running" },
            "run_id": "run_test",
            "worker_id": "wrk_test",
            "worker": {
                "worker_id": "wrk_test",
                "run_id": "run_test",
                "binding_id": "bnd_session",
                "revision": 3,
                "status": if outcome.is_some() { "completed" } else { "running" },
                "profile_id": "profile-main",
                "profile_digest": "c".repeat(64),
                "provider": "opencode-go",
                "requested_model": "deepseek-v4.1-flash",
                "requested_effort": "max",
                "instruction": "Fixture instruction must not enter compact evidence.",
                "attempt_count": 1,
                "worktree": { "base_commit": "d".repeat(40) },
                "progress": WorkerProgressSummary::default(),
                "outcome": outcome,
                "created_at_ms": 1_700_000_000_000_i64,
                "updated_at_ms": 1_700_000_000_001_i64
            },
            "accepted": false
        }))
        .expect("decode fixture task and Worker evidence")
    }

    #[test]
    fn terminal_gate_evidence_preserves_observed_values() {
        for (violation_count, stdout_truncated, stderr_truncated) in
            [(0, false, false), (2, true, false), (0, false, true)]
        {
            let snapshot = task_snapshot_with_worker(Some(json!({
                "exit_code": 0,
                "timed_out": false,
                "cancelled": false,
                "cancel_requested_before_completion": false,
                "missing_model_evidence": false,
                "missing_effort_evidence": false,
                "provider_error": false,
                "incomplete_cleanup": false,
                "rejected_by_diff_gate": false,
                "stdout_truncated": stdout_truncated,
                "stderr_truncated": stderr_truncated,
                "acceptance_state": "ready_for_review",
                "changed_paths_count": 0,
                "changed_paths_preview": [],
                "violation_count": violation_count,
                "violation_summaries": []
            })));
            let evidence = worker_evidence_from_snapshot(&snapshot).expect("Worker evidence");
            assert_eq!(evidence.violation_count, Some(violation_count));
            assert_eq!(evidence.stdout_truncated, Some(stdout_truncated));
            assert_eq!(evidence.stderr_truncated, Some(stderr_truncated));
            let encoded = serde_json::to_value(evidence).expect("serialize evidence");
            assert_eq!(encoded["violation_count"], violation_count);
            assert_eq!(encoded["stdout_truncated"], stdout_truncated);
            assert_eq!(encoded["stderr_truncated"], stderr_truncated);
        }
    }

    #[test]
    fn running_worker_without_outcome_does_not_invent_terminal_evidence() {
        let snapshot = task_snapshot_with_worker(None);
        let evidence = worker_evidence_from_snapshot(&snapshot).expect("running Worker evidence");
        assert_eq!(evidence.attempt_count, 1);
        assert!(evidence.violation_count.is_none());
        assert!(evidence.stdout_truncated.is_none());
        assert!(evidence.stderr_truncated.is_none());
        let encoded = serde_json::to_value(evidence).expect("serialize running evidence");
        for field in ["violation_count", "stdout_truncated", "stderr_truncated"] {
            assert!(
                encoded.get(field).is_none(),
                "absent terminal field {field}"
            );
        }
    }

    #[test]
    fn legacy_worker_evidence_without_gate_fields_remains_unknown() {
        let snapshot = task_snapshot_with_worker(None);
        let evidence = worker_evidence_from_snapshot(&snapshot).expect("Worker evidence");
        let encoded = serde_json::to_value(evidence).expect("serialize legacy-shaped evidence");
        let decoded: WorkerBacklogWorkerEvidence =
            serde_json::from_value(encoded).expect("decode older optional evidence fields");
        assert!(decoded.violation_count.is_none());
        assert!(decoded.stdout_truncated.is_none());
        assert!(decoded.stderr_truncated.is_none());
    }

    #[test]
    fn absent_worker_snapshot_stays_honestly_absent() {
        let snapshot: TaskStateSnapshot = serde_json::from_value(json!({
            "approval_id": "apr_0196ed00-0000-7000-8000-000000000001",
            "subject_digest": "a".repeat(64),
            "binding_id": "bnd_0196ed00-0000-7000-8000-000000000001",
            "thread_uuid": "0196ed00-0000-7000-8000-000000000001",
            "session_binding_id": "sbnd_0196ed00-0000-7000-8000-000000000001",
            "task_id": "task-root",
            "task_digest": "b".repeat(64),
            "adapter": "opencode_v2",
            "provider": "opencode_v2",
            "depends_on": [],
            "profile_id": "profile-main",
            "command_key": "command-key",
            "status": "not_started",
            "run_id": null,
            "worker_id": null,
            "worker": null,
            "accepted": false
        }))
        .expect("decode task snapshot without a Worker record");
        assert!(worker_evidence_from_snapshot(&snapshot).is_none());
    }

    #[test]
    fn legacy_result_payload_without_observed_projection_still_deserializes() {
        let legacy = json!({
            "execution": {
                "execution_ref": "wbe_0196ed00-0000-7000-8000-000000000001",
                "binding_id": "bnd_0196ed00-0000-7000-8000-000000000001",
                "item_id": "blg_0196ed00-0000-7000-8000-000000000001",
                "authority": {
                    "approval_id": "apr_0196ed00-0000-7000-8000-000000000001",
                    "subject_digest": "a".repeat(64),
                    "task_id": "task-root",
                    "task_digest": "b".repeat(64)
                },
                "acceptance_condition_digest": "c".repeat(64),
                "delivery_description_digest": "d".repeat(64),
                "reservation_revision": 2,
                "reserved_at_ms": 1_700_000_000_000_i64,
                "dispatch_receipt": null,
                "attached_at_ms": null,
                "outcome": null,
                "completion": null
            },
            "host_status": "ready_for_review"
        });
        let outcome: WorkerBacklogResultOutcome =
            serde_json::from_value(legacy).expect("legacy result payload must deserialize");
        assert!(outcome.item_snapshot.is_none());
        assert!(outcome.worker_evidence.is_none());
        assert_eq!(
            outcome.host_status,
            Some(TaskExecutionStatus::ReadyForReview)
        );
        let encoded = serde_json::to_value(&outcome).expect("serialize legacy-compatible result");
        assert!(encoded.get("item_snapshot").is_none());
        assert!(encoded.get("worker_evidence").is_none());
    }

    fn failed_snapshot(
        verdict: Option<&str>,
        model_flag: bool,
        effort_flag: bool,
    ) -> TaskStateSnapshot {
        let mut wire = json!({
            "exit_code": 0, "timed_out": false, "cancelled": false,
            "cancel_requested_before_completion": false, "provider_error": false,
            "missing_model_evidence": model_flag, "missing_effort_evidence": effort_flag,
            "incomplete_cleanup": false, "rejected_by_diff_gate": false,
            "stdout_truncated": false, "stderr_truncated": false,
            "acceptance_state": "execution_failed", "changed_paths_count": 0,
            "changed_paths_preview": [], "violation_count": 0, "violation_summaries": [],
            "failure_reason": "arbitrary text about a model mismatch"
        });
        if let Some(verdict) = verdict {
            wire["evidence_failure"] = json!(verdict);
        }
        let mut snapshot = task_snapshot_with_worker(Some(wire));
        snapshot.status = TaskExecutionStatus::Failed;
        let worker = snapshot.worker.as_mut().unwrap();
        worker.status = threadlane_core::WorkerStatus::Failed;
        // A conflict may include the requested value; aliases may look different.
        worker.observed_model = Some("display-alias".into());
        worker.observed_effort = Some(worker.requested_effort.clone());
        snapshot
    }

    #[test]
    fn failure_notes_preserve_exact_verdict_without_guessing_from_identifiers() {
        let cases = [
            (
                Some("model_mismatch"),
                true,
                true,
                "verified worker model mismatch",
            ),
            (
                Some("effort_mismatch"),
                false,
                true,
                "verified worker effort mismatch",
            ),
            (
                Some("missing_model"),
                true,
                false,
                "verified worker missing model evidence",
            ),
            (
                Some("missing_effort"),
                false,
                true,
                "verified worker missing effort evidence",
            ),
            (None, true, false, "worker model evidence unverified"),
            (None, false, true, "worker effort evidence unverified"),
            (None, false, false, "verified worker terminal failure"),
        ];
        for (verdict, model_flag, effort_flag, expected) in cases {
            let snapshot = failed_snapshot(verdict, model_flag, effort_flag);
            assert_eq!(super::failure_outcome_note(&snapshot).unwrap(), expected);
            let outcome = snapshot.worker.unwrap().outcome.unwrap();
            let stored = serde_json::to_value(&outcome).unwrap();
            let encoded =
                serde_json::to_value(super::FailureOutcomeEncoding::from(&outcome)).unwrap();
            let expected_field = verdict.map(|value| json!(value));
            assert_eq!(stored.get("evidence_failure"), expected_field.as_ref());
            assert_eq!(encoded.get("evidence_failure"), expected_field.as_ref());
        }
    }

    #[test]
    fn legacy_failure_digest_input_preserves_order_and_bytes() {
        let snapshot = failed_snapshot(None, false, false);
        let outcome = snapshot.worker.unwrap().outcome.unwrap();
        let encoded = super::FailureOutcomeEncoding::from(&outcome);
        // The v1 digest is over ordered JSON bytes, including explicit nulls.
        assert_eq!(
            serde_json::to_string(&encoded).unwrap(),
            concat!(
                r#"{"acceptance_state":"execution_failed","exit_code":0,"signal":null,"#,
                r#""timed_out":false,"cancelled":false,"cancel_requested_before_completion":false,"#,
                r#""missing_model_evidence":false,"missing_effort_evidence":false,"#,
                r#""provider_error":false,"incomplete_cleanup":false,"rejected_by_diff_gate":false,"#,
                r#""stdout_truncated":false,"stderr_truncated":false,"changed_paths_count":0,"violation_count":0}"#,
            )
        );
    }

    #[test]
    fn typed_rejection_blocks_review_with_clear_legacy_flags() {
        for (verdict, reject_model) in [
            ("missing_model", true),
            ("model_mismatch", true),
            ("missing_effort", false),
            ("effort_mismatch", false),
        ] {
            let mut snapshot = failed_snapshot(Some(verdict), false, false);
            let completed = snapshot.worker.as_mut().unwrap();
            completed.status = threadlane_core::WorkerStatus::Completed;
            completed.outcome.as_mut().unwrap().acceptance_state =
                threadlane_core::WorkerAcceptanceState::ReadyForReview;
            assert_eq!(
                TaskExecutionStatus::from_worker_snapshot(completed),
                TaskExecutionStatus::Failed
            );
            let projected = worker_evidence_from_snapshot(&snapshot).unwrap();
            assert_eq!(projected.missing_model_evidence, Some(reject_model));
            assert_eq!(projected.missing_effort_evidence, Some(!reject_model));
        }
    }

    #[test]
    fn failure_notes_distinguish_structured_terminal_categories() {
        let conflicting_evidence = FailureNoteEvidence {
            evidence_failure: Some(super::WorkerEvidenceFailure::EffortMismatch),
            ..FailureNoteEvidence::default()
        };
        let cases = [
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    timed_out: true,
                    cancelled: true,
                    ..conflicting_evidence
                },
                "verified worker timeout",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    cancelled: true,
                    rejected_by_diff_gate: true,
                    ..conflicting_evidence
                },
                "verified worker cancellation",
            ),
            (
                TaskExecutionStatus::TimedOut,
                conflicting_evidence,
                "verified worker timeout",
            ),
            (
                TaskExecutionStatus::Cancelled,
                conflicting_evidence,
                "verified worker cancellation",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    rejected_by_diff_gate: true,
                    ..conflicting_evidence
                },
                "verified worker diff scope rejection",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    missing_model_evidence: true,
                    ..FailureNoteEvidence::default()
                },
                "worker model evidence unverified",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    missing_effort_evidence: true,
                    ..FailureNoteEvidence::default()
                },
                "worker effort evidence unverified",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    incomplete_cleanup: true,
                    ..FailureNoteEvidence::default()
                },
                "verified worker cleanup incomplete",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    exit_code: Some(1),
                    ..FailureNoteEvidence::default()
                },
                "verified worker nonzero exit",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    signal: Some(9),
                    ..FailureNoteEvidence::default()
                },
                "verified worker signal termination",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    provider_error: true,
                    ..FailureNoteEvidence::default()
                },
                "verified worker provider error",
            ),
        ];
        for (status, evidence, expected) in cases {
            assert_eq!(failure_note_for(status, evidence), expected);
        }
    }

    #[test]
    fn notice_scope_omits_none_required_changed_paths_and_round_trips_some() {
        let legacy = WorkerBacklogNoticeScope {
            repository_path: PathBuf::from("/repo"),
            base_commit: "a".repeat(40),
            requested_model: "model-a".to_string(),
            requested_effort: "high".to_string(),
            write_paths: vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()],
            required_changed_paths: None,
        };
        let encoded = serde_json::to_value(&legacy).expect("serialize legacy notice scope");
        let object = encoded.as_object().expect("notice scope object");
        assert!(
            !object.contains_key("required_changed_paths"),
            "None required_changed_paths must be omitted to preserve legacy bytes/digests"
        );
        let decoded: WorkerBacklogNoticeScope =
            serde_json::from_value(encoded).expect("deserialize legacy notice scope without field");
        assert_eq!(decoded, legacy);

        let with_required = WorkerBacklogNoticeScope {
            required_changed_paths: Some(vec!["src/lib.rs".to_string()]),
            ..legacy
        };
        let encoded_some =
            serde_json::to_value(&with_required).expect("serialize notice scope with required");
        assert_eq!(
            encoded_some["required_changed_paths"],
            serde_json::json!(["src/lib.rs"])
        );
        let decoded_some: WorkerBacklogNoticeScope =
            serde_json::from_value(encoded_some).expect("round-trip notice scope with required");
        assert_eq!(decoded_some, with_required);
    }

    #[test]
    fn required_changed_paths_validation_enforces_exact_subsets() {
        let exact_allowed = vec!["src/lib.rs".to_string(), "src/extra.rs".to_string()];
        assert!(valid_exact_required_changed_paths(&exact_allowed, None));
        assert!(valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&["src/lib.rs".to_string()])
        ));
        assert!(!valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&[])
        ));
        assert!(!valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&["src/other.rs".to_string()])
        ));
        assert!(!valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&["src/lib.rs".to_string(), "src/lib.rs".to_string()])
        ));
        assert!(!valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&["src/".to_string()])
        ));
        assert!(!valid_exact_required_changed_paths(
            &exact_allowed,
            Some(&["../escape.rs".to_string()])
        ));

        let dir_allowed = vec!["src/".to_string()];
        assert!(!valid_exact_required_changed_paths(
            &dir_allowed,
            Some(&["src/lib.rs".to_string()])
        ));
        assert!(valid_notice_required_changed_paths(
            &dir_allowed,
            Some(&["src/lib.rs".to_string()])
        ));
        assert!(!valid_notice_required_changed_paths(
            &dir_allowed,
            Some(&["other/lib.rs".to_string()])
        ));
    }
}
