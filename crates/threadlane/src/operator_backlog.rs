//! Trusted-local operator transport for durable Worker backlog execution state.
//!
//! This module is storage/control plumbing only. It exposes the seven operator-attested
//! Worker backlog operations over the existing private loopback HTTP endpoint and
//! deliberately performs no execution, scheduling, wake-up notification, host start,
//! or provider dispatch.
//!
//! The endpoint has the same trust as the trusted-local
//! [`SupervisorClient::get_or_create_binding`](crate::SupervisorClient::get_or_create_binding)
//! route: possession of the private runtime descriptor and bearer token is the boundary.
//! It is not a new cryptographic separation from another same-UID caller that can read
//! the descriptor. Only the trusted native facade resolves the real approval, terminal
//! result, and delivery evidence before calling these operations. A raw outcome report
//! is an operator attestation, not runtime proof, and this transport does not verify
//! approval authority on its own.
//!
//! Every request carries the exact `binding_id` and canonical `thread_uuid`. The active
//! [`Core`] binding is rechecked while the serialized core guard is held before any
//! replay, read, or mutation. Mutations carry an idempotency `command_key` and the
//! current backlog item `expected_revision`; the server injects wall-clock time and
//! rejects caller-supplied `now_ms`, identity, or success fields. Requests use the
//! existing 64 KiB supervisor body limit, and replies use the existing 2 MiB client
//! response cap. The core compact or page serialized response budget applies to the
//! inner typed payload, matching the core's transactional precommit check; the small
//! fixed operator response enum envelope remains well below the client response cap.

use serde::{Deserialize, Serialize};
use threadlane_core::{
    estimate_wire_response_bytes, Core, CoreError, WorkerBacklogAcceptanceRejection,
    WorkerBacklogAcceptanceRejectionReceipt, WorkerBacklogAttemptPage, WorkerBacklogAuthority,
    WorkerBacklogCompletion, WorkerBacklogCompletionReceipt, WorkerBacklogDispatchReceipt,
    WorkerBacklogExecution, WorkerBacklogOutcomeReport, MAX_COMPACT_RESPONSE_BYTES,
    MAX_PAGE_RESPONSE_BYTES,
};

/// Trusted-local HTTP path for the operator Worker backlog operations.
pub const OPERATOR_WORKER_BACKLOG_PATH: &str = "/v1/operator/worker-backlog";

/// One trusted-local operator Worker backlog request.
///
/// The enum is internally tagged by `op` and rejects unknown fields in every variant.
/// Requests never carry caller wall-clock time or a caller-supplied success flag.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorWorkerBacklogRequest {
    /// Reserve a Worker backlog item for one operator-attested execution.
    Reserve {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Expected current backlog item revision.
        expected_revision: u64,
        /// Compact trusted-upstream approval/scope evidence.
        authority: WorkerBacklogAuthority,
    },
    /// Attach one operator-attested runtime dispatch receipt to a reserved execution.
    AttachDispatchReceipt {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Reserved execution reference.
        execution_ref: String,
        /// Expected current backlog item revision.
        expected_revision: u64,
        /// Compact trusted-upstream dispatch receipt.
        receipt: WorkerBacklogDispatchReceipt,
    },
    /// Record one operator-attested terminal outcome for a dispatched execution.
    RecordOutcome {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Reserved execution reference.
        execution_ref: String,
        /// Expected current backlog item revision.
        expected_revision: u64,
        /// Compact trusted-upstream terminal outcome report.
        report: WorkerBacklogOutcomeReport,
    },
    /// Complete a Worker backlog item after a verified terminal result was recorded.
    Complete {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Expected current backlog item revision.
        expected_revision: u64,
        /// Bounded completion attestation.
        completion: WorkerBacklogCompletion,
    },
    /// Reject Coworker acceptance of a technically successful Worker execution.
    RejectAcceptance {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Expected current backlog item revision.
        expected_revision: u64,
        /// Bounded failed-acceptance rejection attestation.
        rejection: WorkerBacklogAcceptanceRejection,
    },
    /// Read one durable Worker backlog execution by reference.
    GetExecution {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Reserved execution reference.
        execution_ref: String,
    },
    /// List a bounded page of durable execution attempts for one Worker backlog item.
    ListAttempts {
        /// Owning binding identifier.
        binding_id: String,
        /// Canonical hyphenated native Codex thread UUID bound to `binding_id`.
        thread_uuid: String,
        /// Target Worker backlog item identifier.
        item_id: String,
        /// Optional opaque keyset cursor returned by a previous attempt page.
        #[serde(default)]
        cursor: Option<String>,
        /// Page size limit validated by the core page bound.
        limit: usize,
    },
}

impl OperatorWorkerBacklogRequest {
    /// Returns the request binding identifier.
    pub fn binding_id(&self) -> &str {
        match self {
            Self::Reserve { binding_id, .. }
            | Self::AttachDispatchReceipt { binding_id, .. }
            | Self::RecordOutcome { binding_id, .. }
            | Self::Complete { binding_id, .. }
            | Self::RejectAcceptance { binding_id, .. }
            | Self::GetExecution { binding_id, .. }
            | Self::ListAttempts { binding_id, .. } => binding_id,
        }
    }

    /// Returns the canonical thread UUID supplied with the request.
    pub fn thread_uuid(&self) -> &str {
        match self {
            Self::Reserve { thread_uuid, .. }
            | Self::AttachDispatchReceipt { thread_uuid, .. }
            | Self::RecordOutcome { thread_uuid, .. }
            | Self::Complete { thread_uuid, .. }
            | Self::RejectAcceptance { thread_uuid, .. }
            | Self::GetExecution { thread_uuid, .. }
            | Self::ListAttempts { thread_uuid, .. } => thread_uuid,
        }
    }

    /// Returns `true` when the operation mutates durable backlog or execution state.
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Self::Reserve { .. }
                | Self::AttachDispatchReceipt { .. }
                | Self::RecordOutcome { .. }
                | Self::Complete { .. }
                | Self::RejectAcceptance { .. }
        )
    }
}

/// One trusted-local operator Worker backlog response.
///
/// The enum is internally tagged by `kind`, rejects unknown fields, and never adds a
/// caller-supplied identity, timestamp, or success flag.
// Bounded transport DTO: keep variants unboxed so the serialized wire and public
// Rust API stay stable for native consumers, matching the `ScopedResponse` pattern.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorWorkerBacklogResponse {
    /// Durable execution state returned by reserve, attach, record, or get.
    Execution {
        /// Compact durable execution record.
        execution: WorkerBacklogExecution,
    },
    /// Durable completion receipt returned by complete.
    Completion {
        /// Compact completion receipt.
        completion: WorkerBacklogCompletionReceipt,
    },
    /// Durable acceptance-rejection receipt returned by reject.
    AcceptanceRejection {
        /// Compact acceptance-rejection receipt.
        rejection: WorkerBacklogAcceptanceRejectionReceipt,
    },
    /// Bounded attempt page returned by list.
    AttemptPage {
        /// Compact attempt page.
        page: WorkerBacklogAttemptPage,
    },
}

/// Executes one operator Worker backlog operation against the active core.
///
/// The caller must hold the supervisor's serialized core access guard. This function
/// revalidates the canonical thread identity and the exact current binding before it
/// invokes any replay, read, or mutation. It does not tick the core, notify wake-ups,
/// start a host, or dispatch a provider.
pub(crate) fn execute_operator_worker_backlog(
    core: &mut Core,
    now_ms: i64,
    request: OperatorWorkerBacklogRequest,
) -> Result<OperatorWorkerBacklogResponse, CoreError> {
    let binding_id = request.binding_id().to_string();
    let thread_uuid = request.thread_uuid().to_string();
    validate_canonical_thread_uuid(&thread_uuid)?;

    let binding = core.get_binding(&binding_id)?;
    if binding.thread_uuid != thread_uuid {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: "thread_uuid does not match the current binding identity".to_string(),
        });
    }

    let response = match request {
        OperatorWorkerBacklogRequest::Reserve {
            command_key,
            item_id,
            expected_revision,
            authority,
            ..
        } => OperatorWorkerBacklogResponse::Execution {
            execution: core.reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                &command_key,
                &item_id,
                expected_revision,
                authority,
                now_ms,
            )?,
        },
        OperatorWorkerBacklogRequest::AttachDispatchReceipt {
            command_key,
            item_id,
            execution_ref,
            expected_revision,
            receipt,
            ..
        } => OperatorWorkerBacklogResponse::Execution {
            execution: core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                &command_key,
                &item_id,
                &execution_ref,
                expected_revision,
                receipt,
                now_ms,
            )?,
        },
        OperatorWorkerBacklogRequest::RecordOutcome {
            command_key,
            item_id,
            execution_ref,
            expected_revision,
            report,
            ..
        } => OperatorWorkerBacklogResponse::Execution {
            execution: core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                &command_key,
                &item_id,
                &execution_ref,
                expected_revision,
                report,
                now_ms,
            )?,
        },
        OperatorWorkerBacklogRequest::Complete {
            command_key,
            item_id,
            expected_revision,
            completion,
            ..
        } => OperatorWorkerBacklogResponse::Completion {
            completion: core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                &command_key,
                &item_id,
                expected_revision,
                completion,
                now_ms,
            )?,
        },
        OperatorWorkerBacklogRequest::RejectAcceptance {
            command_key,
            item_id,
            expected_revision,
            rejection,
            ..
        } => OperatorWorkerBacklogResponse::AcceptanceRejection {
            rejection: core.reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                &command_key,
                &item_id,
                expected_revision,
                rejection,
                now_ms,
            )?,
        },
        OperatorWorkerBacklogRequest::GetExecution {
            item_id,
            execution_ref,
            ..
        } => OperatorWorkerBacklogResponse::Execution {
            execution: core.get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item_id,
                &execution_ref,
            )?,
        },
        OperatorWorkerBacklogRequest::ListAttempts {
            item_id,
            cursor,
            limit,
            ..
        } => OperatorWorkerBacklogResponse::AttemptPage {
            page: core.list_worker_backlog_attempts_operator_attested(
                &binding_id,
                &item_id,
                cursor.as_deref(),
                limit,
            )?,
        },
    };

    enforce_operator_worker_backlog_response_budget(&response)?;
    Ok(response)
}

/// Enforces the existing compact/page serialized response budgets on the inner typed
/// operator payload (`execution`, `completion`, `rejection`, or `page`), matching the
/// core's transactional precommit guarantee. The fixed operator response enum
/// envelope is not included; the independent HTTP/client response cap already bounds
/// the actual serialized reply well above the inner payload plus that small fixed
/// wrapper.
pub(crate) fn enforce_operator_worker_backlog_response_budget(
    response: &OperatorWorkerBacklogResponse,
) -> Result<(), CoreError> {
    match response {
        OperatorWorkerBacklogResponse::Execution { execution } => {
            enforce_inner_response_budget(execution, MAX_COMPACT_RESPONSE_BYTES)
        }
        OperatorWorkerBacklogResponse::Completion { completion } => {
            enforce_inner_response_budget(completion, MAX_COMPACT_RESPONSE_BYTES)
        }
        OperatorWorkerBacklogResponse::AcceptanceRejection { rejection } => {
            enforce_inner_response_budget(rejection, MAX_COMPACT_RESPONSE_BYTES)
        }
        OperatorWorkerBacklogResponse::AttemptPage { page } => {
            enforce_inner_response_budget(page, MAX_PAGE_RESPONSE_BYTES)
        }
    }
}

fn enforce_inner_response_budget<T: Serialize>(
    payload: &T,
    max_bytes: usize,
) -> Result<(), CoreError> {
    let size_bytes = estimate_wire_response_bytes(payload)?;
    if size_bytes > max_bytes {
        return Err(CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes,
        });
    }
    Ok(())
}

fn validate_canonical_thread_uuid(raw: &str) -> Result<(), CoreError> {
    let parsed = uuid::Uuid::parse_str(raw).map_err(|_| CoreError::Validation {
        field: "thread_uuid",
        reason: "thread_uuid must be a canonical hyphenated UUID".to_string(),
    })?;
    if parsed.is_nil() || parsed.to_string() != raw {
        return Err(CoreError::Validation {
            field: "thread_uuid",
            reason: "thread_uuid must be a canonical lowercase hyphenated non-nil UUID".to_string(),
        });
    }
    Ok(())
}
