//! Trusted-local service for guarded Worker backlog execution.
//!
//! This module is the typed boundary between the paired MCP server and the existing
//! trusted-local supervisor transport. It reserves a Worker backlog item before any
//! host/provider effect, reconciles the original approved task and invocation, reads
//! genuine live or stopped host snapshots, records only verified terminal outcomes, and
//! commits separate Coworker acceptance completion. It never creates a job, controller,
//! reviewer, closure stage, or automatic acceptance decision.

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
    BacklogItem, BacklogItemKind, BacklogStatus, WorkerAcceptanceState, WorkerBacklogAuthority,
    WorkerBacklogCompletion, WorkerBacklogCompletionReceipt, WorkerBacklogDispatchReceipt,
    WorkerBacklogExecution, WorkerBacklogOutcome, WorkerBacklogOutcomeReport, WorkerStatus,
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
const COMPLETE_TOOL: &str = "backlog_worker_complete";
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

/// Compact successful start/reservation response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBacklogStartOutcome {
    /// Exact stored execution after reservation or idempotent replay.
    pub execution: WorkerBacklogExecution,
    /// Genuine host status when independently observed; never proof by itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_status: Option<TaskExecutionStatus>,
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

impl WorkerBacklogService {
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
            return Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: None,
                delivery: None,
            });
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
                    return Ok(WorkerBacklogResultOutcome {
                        execution,
                        host_status: None,
                        delivery: None,
                    });
                }
                return Err(WorkerBacklogError::from_host_error(&error));
            }
        };

        let Some(snapshot) = snapshot else {
            if execution.outcome.is_some() || execution.completion.is_some() {
                return Ok(WorkerBacklogResultOutcome {
                    execution,
                    host_status: None,
                    delivery: None,
                });
            }
            if execution.dispatch_receipt.is_some() {
                return Err(WorkerBacklogError::new(
                    "host_result_unavailable",
                    "The stored Worker invocation has no readable live or stopped host result.",
                ));
            }
            return Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: None,
                delivery: None,
            });
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
            return Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: Some(snapshot.status),
                delivery: None,
            });
        }

        let previous_kind = execution.outcome.as_ref().map(|outcome| outcome.outcome);
        let previous_result_digest = execution
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.result_digest.clone());
        if let Some(previous_kind) = previous_kind {
            if previous_kind == WorkerBacklogOutcome::Unknown {
                if snapshot.status != TaskExecutionStatus::Unknown {
                    return self
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
                return Ok(WorkerBacklogResultOutcome {
                    execution,
                    host_status: Some(snapshot.status),
                    delivery: None,
                });
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
                    return Ok(WorkerBacklogResultOutcome {
                        execution,
                        host_status: Some(snapshot.status),
                        delivery: Some(inspection),
                    });
                }
            }
            return Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: Some(snapshot.status),
                delivery: None,
            });
        }

        match snapshot.status {
            TaskExecutionStatus::Queued
            | TaskExecutionStatus::Claimed
            | TaskExecutionStatus::Running
            | TaskExecutionStatus::CancelRequested
            | TaskExecutionStatus::NotStarted
            | TaskExecutionStatus::DependencyInputsUnverified
            | TaskExecutionStatus::DependencyInputsReady
            | TaskExecutionStatus::NativeRouteRequired => Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: Some(snapshot.status),
                delivery: None,
            }),
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
                Ok(WorkerBacklogResultOutcome {
                    execution,
                    host_status: Some(snapshot.status),
                    delivery: None,
                })
            }
            TaskExecutionStatus::ReadyForReview
            | TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut => {
                self.reconcile_known_outcome(
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
                .await
            }
        }
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
            if item.status != BacklogStatus::Done {
                return Err(WorkerBacklogError::new(
                    "corrupt_worker_state",
                    "A stored Worker completion does not have a Done backlog item.",
                ));
            }
            if stored.result_digest != params.expected_result_digest
                || stored.delivery_digest != params.expected_delivery_digest
                || stored.acceptance_note != params.acceptance_note
                || stored.acceptance_evidence != params.acceptance_evidence
            {
                return Err(WorkerBacklogError::new(
                    "completion_conflict",
                    "The backlog item is already completed with a different durable payload.",
                ));
            }
            final_recheck
                .take()
                .expect("completion recheck closure must be available")()?;
            let completion = self
                .complete_core(binding_id, &record.thread_uuid, &params)
                .await
                .map_err(|error| match error.code {
                    "worker_backlog_unavailable" | "supervisor_unavailable" => error,
                    _ => WorkerBacklogError::new(
                        "completion_conflict",
                        "The durable completion cannot be replayed with this command key or payload.",
                    ),
                })?;
            return Ok(WorkerBacklogCompleteOutcome {
                completion,
                delivery: None,
            });
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
        let completion_matches = matches!(
            completion.execution.completion.as_ref(),
            Some(stored)
                if stored.delivery_digest == params.expected_delivery_digest
                    && stored.result_digest == params.expected_result_digest
        );
        if completion.item.status != BacklogStatus::Done
            || completion.execution.execution_ref != params.execution_ref
            || !completion_matches
        {
            return Err(WorkerBacklogError::new(
                "corrupt_worker_state",
                "The durable Worker completion receipt did not match the requested completion.",
            ));
        }
        Ok(WorkerBacklogCompleteOutcome {
            completion,
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
                })
            }
            _ => Ok(WorkerBacklogResultOutcome {
                execution,
                host_status: Some(snapshot.status),
                delivery: None,
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
        outcome: outcome.map(|outcome| FailureOutcomeEncoding {
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
        }),
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
            } else if evidence.missing_model_evidence {
                "verified worker missing model evidence"
            } else if evidence.missing_effort_evidence {
                "verified worker missing effort evidence"
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
/// Stable completion tool name.
pub(crate) const WORKER_BACKLOG_COMPLETE_TOOL: &str = COMPLETE_TOOL;

#[cfg(test)]
mod tests {
    use super::{failure_note_for, FailureNoteEvidence};
    use threadlane_host::TaskExecutionStatus;

    #[test]
    fn failure_notes_distinguish_structured_terminal_categories() {
        let cases = [
            (
                TaskExecutionStatus::TimedOut,
                FailureNoteEvidence::default(),
                "verified worker timeout",
            ),
            (
                TaskExecutionStatus::Cancelled,
                FailureNoteEvidence::default(),
                "verified worker cancellation",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    rejected_by_diff_gate: true,
                    ..FailureNoteEvidence::default()
                },
                "verified worker diff scope rejection",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    missing_model_evidence: true,
                    ..FailureNoteEvidence::default()
                },
                "verified worker missing model evidence",
            ),
            (
                TaskExecutionStatus::Failed,
                FailureNoteEvidence {
                    missing_effort_evidence: true,
                    ..FailureNoteEvidence::default()
                },
                "verified worker missing effort evidence",
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
}
