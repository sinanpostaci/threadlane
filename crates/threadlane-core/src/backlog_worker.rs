//! Operator-attested Worker execution linkage for durable backlog items.
//!
//! This module connects `Worker` backlog items to the existing approved
//! single-task execution route without creating a `Run`, `Job`, worker, event, or
//! human request itself. Every mutating method is explicitly named
//! `*_operator_attested` and is intended only for the trusted local native
//! facade. These methods are not scoped MCP operations and must not be added to
//! `ScopedRequest`, exposed as public MCP tools, or accepted by public raw-result
//! import.
//!
//! The core validates identifier and digest shape, binding ownership, item
//! revisions, allowed transitions, approval/task uniqueness, and transactional
//! atomicity. It does not prove human authority, cryptographically authenticate
//! a dispatch receipt, inspect owned files, or interpret arbitrary prose. The
//! trusted local caller must independently resolve the approval record, the
//! exact live or stopped task state, the physically owned delivery, and the
//! Coworker acceptance attestation before calling into this module. For known
//! outcomes the trusted caller also supplies the final result digest computed
//! from the exact verified terminal result; core checks only digest structure
//! and correlation with the resolved execution.
//!
//! A reservation is not proof that a process is running. Recording a successful
//! outcome leaves the item in `Doing` until a separate completion receipt is
//! committed. `Failure` and `Unknown` block the item while preserving the
//! execution identity and evidence. A technically successful delivery that
//! fails Coworker acceptance can be separately rejected, which blocks the item
//! without rewriting the immutable `Success` outcome. A later reservation after
//! a known failure or acceptance rejection requires a different fresh
//! approval/task identity and a new revision check.

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::backlog::{
    check_backlog_dedup_raw, invalid_backlog_transition, load_backlog_item_conn,
    next_backlog_revision, parse_backlog_cursor_position, record_backlog_dedup_raw,
    record_backlog_history_tx, update_backlog_item_tx, validate_backlog_cursor_text,
    validate_backlog_item_id, validate_expected_revision, BacklogHistoryAction, BacklogItem,
    BacklogItemKind, BacklogStatus, MAX_BACKLOG_ACCEPTANCE_LEN, MAX_BACKLOG_BLOCKED_REASON_LEN,
    MAX_BACKLOG_COMPLETION_EVIDENCE_LEN, MAX_BACKLOG_COMPLETION_NOTE_LEN, MAX_BACKLOG_DELIVERY_LEN,
};
use crate::engine::{generate_id, hash_dedup_payload, i64_to_u64, u64_to_i64, Core};
use crate::error::CoreError;
use crate::limits::{
    estimate_wire_response_bytes, validate_bounded_text, validate_command_key, validate_identifier,
    validate_page_limit, validate_timestamp_ms, MAX_COMPACT_RESPONSE_BYTES,
    MAX_PAGE_RESPONSE_BYTES, MAX_WORKER_DIGEST_LEN,
};

/// Maximum byte length for one recorded Worker outcome note.
///
/// The note is also used as the basis of the bounded blocked reason, so it stays
/// smaller than [`MAX_BACKLOG_BLOCKED_REASON_LEN`] after the fixed prefix.
pub const MAX_WORKER_BACKLOG_OUTCOME_NOTE_LEN: usize = 768;

/// Maximum byte length for one failed-acceptance rejection reason.
///
/// The reason is also embedded in the bounded blocked reason, so it stays
/// smaller than [`MAX_BACKLOG_BLOCKED_REASON_LEN`] after the fixed prefix.
pub const MAX_WORKER_BACKLOG_REJECTION_REASON_LEN: usize = 768;

/// Maximum byte length for one failed-acceptance rejection evidence
/// attestation.
pub const MAX_WORKER_BACKLOG_REJECTION_EVIDENCE_LEN: usize = 2_048;

const RESERVE_OPERATION: &str = "backlog_worker_reserve";
const ATTACH_OPERATION: &str = "backlog_worker_attach";
const OUTCOME_OPERATION: &str = "backlog_worker_outcome";
const COMPLETE_OPERATION: &str = "backlog_worker_complete";
const REJECT_OPERATION: &str = "backlog_worker_acceptance_reject";
const WORKER_CURSOR_VERSION: &str = "tlb1";
const WORKER_ATTEMPTS_CURSOR_KIND: &str = "worker_attempts";

/// Frozen concrete authority tuple for one Worker execution reservation.
///
/// The tuple is supplied by the trusted local caller after it has resolved the
/// approval record and the exact approved task. Core validates only lexical
/// shape and uniqueness; it does not authenticate the approval or re-derive the
/// digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogAuthority {
    /// Durable approval identifier.
    pub approval_id: String,
    /// Exact 64-character lowercase hex approval subject digest.
    pub subject_digest: String,
    /// Approved task identifier within the approval record.
    pub task_id: String,
    /// Exact 64-character lowercase hex approved task digest.
    pub task_digest: String,
}

/// Exact dispatch receipt attached once the trusted upstream caller knows the
/// concrete execution identity.
///
/// Core does not verify that these identifiers belong to a live worker or that
/// the digest was computed from real process state. Upstream must reconcile that
/// independently and attach only the real receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogDispatchReceipt {
    /// Session binding that owns the dispatch attempt.
    pub session_binding_id: String,
    /// Concrete run identifier created by the existing execution route.
    pub run_id: String,
    /// Concrete worker identifier created by the existing execution route.
    pub worker_id: String,
    /// Exact execution profile identifier used for dispatch.
    pub profile_id: String,
    /// Optional lowercase hex execution-binding digest when the route provides one.
    pub execution_binding_digest: Option<String>,
}

/// Small operator-verified Worker outcome classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerBacklogOutcome {
    /// The trusted caller verified successful execution; the item remains
    /// `Doing` until separate Coworker acceptance completion or a stored
    /// acceptance rejection.
    Success,
    /// The trusted caller verified failure.
    Failure,
    /// The effect remains ambiguous and must be reconciled by the original
    /// invocation identity.
    Unknown,
}

impl WorkerBacklogOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "failure" => Some(Self::Failure),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    fn is_known(self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

/// Revision-checked trusted-operator-only outcome report.
///
/// This is a trusted-local operator input, never a scoped MCP request. Known
/// outcomes require `result_digest` as the canonical 64-character lowercase hex
/// SHA-256 digest that the trusted upstream facade computed from the exact
/// verified terminal result evidence; `Unknown` must send `None`. Core validates
/// only digest structure and correlation with the resolved execution. The
/// upstream facade must verify the actual terminal result, and this raw
/// outcome/digest import must never be exposed through scoped MCP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogOutcomeReport {
    /// Verified outcome classification.
    pub outcome: WorkerBacklogOutcome,
    /// Bounded concrete diagnostic note.
    pub note: String,
    /// Canonical lowercase hex digest of the exact verified terminal result for
    /// known outcomes; `None` for `Unknown`.
    pub result_digest: Option<String>,
}

/// Separate completion attestation supplied only after Coworker acceptance and
/// upstream delivery verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogCompletion {
    /// Exact current execution reference being completed.
    pub execution_ref: String,
    /// Exact stored successful `result_digest`.
    pub result_digest: String,
    /// Bounded Coworker acceptance note.
    pub acceptance_note: String,
    /// Bounded Coworker evidence attestation.
    pub acceptance_evidence: String,
    /// Lowercase hex delivery digest computed by the trusted upstream delivery
    /// checker. Core validates shape only and does not inspect disk state.
    pub delivery_digest: String,
}

/// Revision-checked trusted-operator-only acceptance rejection request.
///
/// This is a trusted-local operator input, never a scoped MCP request. It is
/// only valid for the exact current execution of a `Doing` Worker item whose
/// immutable technical outcome is `Success`. The request must echo the stored
/// successful result digest and carry the upstream-checked delivery digest plus
/// bounded concrete failed-acceptance reason/evidence. The upstream guarded
/// facade must independently re-read the real terminal result and delivery
/// before calling this core operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerBacklogAcceptanceRejection {
    /// Exact current execution reference being rejected.
    pub execution_ref: String,
    /// Exact stored successful `result_digest`.
    pub result_digest: String,
    /// Lowercase hex delivery digest computed by the trusted upstream delivery
    /// checker. Core validates shape only and does not inspect disk state.
    pub delivery_digest: String,
    /// Bounded concrete reason the delivered result failed acceptance.
    pub reason: String,
    /// Bounded concrete evidence checked during failed acceptance.
    pub evidence: String,
}

/// Durable recorded outcome for one execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogRecordedOutcome {
    /// Verified outcome classification.
    pub outcome: WorkerBacklogOutcome,
    /// Bounded concrete note.
    pub note: String,
    /// Final result digest supplied by the trusted upstream operator report for
    /// known outcomes.
    ///
    /// `None` for `Unknown` so transient diagnostic evidence is never frozen as
    /// a final result.
    pub result_digest: Option<String>,
    /// Timestamp (ms) when the outcome was recorded.
    pub recorded_at_ms: i64,
}

/// Durable completion receipt fields stored with the execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogCompletionRecord {
    /// Successful result digest copied from the recorded outcome.
    pub result_digest: String,
    /// Upstream-computed delivery digest.
    pub delivery_digest: String,
    /// Bounded Coworker acceptance note.
    pub acceptance_note: String,
    /// Bounded Coworker acceptance evidence.
    pub acceptance_evidence: String,
    /// Timestamp (ms) when completion was recorded.
    pub completed_at_ms: i64,
}

/// Durable failed-acceptance rejection stored separately from the immutable
/// technical outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogAcceptanceRejectionRecord {
    /// Successful result digest copied from the recorded outcome.
    pub result_digest: String,
    /// Upstream-checked delivery digest.
    pub delivery_digest: String,
    /// Bounded concrete reason the delivered result failed acceptance.
    pub reason: String,
    /// Bounded concrete evidence checked during failed acceptance.
    pub evidence: String,
    /// Timestamp (ms) when the rejection was recorded.
    pub rejected_at_ms: i64,
}

/// Durable execution attempt linked to one Worker backlog item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogExecution {
    /// Software-generated execution reference (`wbe_<uuid>`).
    pub execution_ref: String,
    /// Owning binding/chat identifier.
    pub binding_id: String,
    /// Backlog item that reserved this attempt.
    pub item_id: String,
    /// Frozen concrete authority tuple.
    pub authority: WorkerBacklogAuthority,
    /// Software-computed digest of the acceptance condition at reservation.
    pub acceptance_condition_digest: String,
    /// Software-computed digest of the delivery description at reservation.
    pub delivery_description_digest: String,
    /// Item revision produced by the reservation.
    pub reservation_revision: u64,
    /// Timestamp (ms) when the reservation was recorded.
    pub reserved_at_ms: i64,
    /// Exact dispatch receipt when attached.
    pub dispatch_receipt: Option<WorkerBacklogDispatchReceipt>,
    /// Timestamp (ms) when the dispatch receipt was attached.
    pub attached_at_ms: Option<i64>,
    /// Operator-verified outcome when recorded.
    pub outcome: Option<WorkerBacklogRecordedOutcome>,
    /// Separate Coworker acceptance and delivery completion when recorded.
    pub completion: Option<WorkerBacklogCompletionRecord>,
    /// Separate failed-acceptance rejection when the Coworker rejected a
    /// technically successful delivery.
    ///
    /// Absent on legacy records and on attempts that were not rejected. The
    /// original successful outcome remains unchanged when this is present, and
    /// it is mutually exclusive with [`Self::completion`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_rejection: Option<WorkerBacklogAcceptanceRejectionRecord>,
}

/// Durable completion receipt returned by the separate completion commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogCompletionReceipt {
    /// Final item snapshot after completion.
    pub item: BacklogItem,
    /// Final execution snapshot containing the authority tuple, dispatch
    /// identity, result digest, delivery digest, and acceptance attestation.
    pub execution: WorkerBacklogExecution,
}

/// Durable rejection receipt returned by the acceptance-rejection commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogAcceptanceRejectionReceipt {
    /// Final item snapshot after rejection.
    pub item: BacklogItem,
    /// Final execution snapshot preserving the original successful outcome and
    /// carrying the separate rejection record.
    pub execution: WorkerBacklogExecution,
}

/// Compact identity of one historical Worker attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogAttemptIdentity {
    /// Software-generated execution reference.
    pub execution_ref: String,
    /// Frozen approval identifier.
    pub approval_id: String,
    /// Frozen approval subject digest.
    pub subject_digest: String,
    /// Frozen approved task identifier.
    pub task_id: String,
    /// Frozen approved task digest.
    pub task_digest: String,
    /// Timestamp (ms) when the reservation was recorded.
    pub reserved_at_ms: i64,
    /// Whether a dispatch receipt has been attached.
    pub dispatch_attached: bool,
    /// Recorded outcome classification, when one exists.
    pub outcome: Option<WorkerBacklogOutcome>,
    /// Whether the separate completion receipt has been committed.
    pub completed: bool,
}

/// Bounded stable-order page of Worker attempt identities for one item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerBacklogAttemptPage {
    /// Binding/chat that owns the item.
    pub binding_id: String,
    /// Item whose attempts are being read.
    pub item_id: String,
    /// Attempt identities in stable reservation order within the page.
    pub attempts: Vec<WorkerBacklogAttemptIdentity>,
    /// Opaque continuation cursor when `has_more` is true.
    pub next_cursor: Option<String>,
    /// Whether more attempts remain inside this pagination window.
    pub has_more: bool,
    /// Reservation-order high-water mark anchoring this pagination window.
    pub high_water_mark_rowid: u64,
}

#[allow(clippy::type_complexity)]
type WorkerExecutionRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

type WorkerAttemptRow = (
    i64,
    String,
    String,
    String,
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
);

impl Core {
    /// Reserves an existing `Worker` backlog item before upstream dispatch.
    ///
    /// This is a trusted-local operator-attested method, not a scoped MCP
    /// operation. Core generates `execution_ref`, freezes the supplied authority
    /// tuple and the item's current acceptance/delivery condition digests, moves
    /// the item to `Doing`, records history, and inserts both the execution row
    /// and the command deduplication record in one transaction. It does not
    /// create a run, job, worker, process, or proof that execution started.
    ///
    /// A `Todo` item may reserve directly. A manually `Blocked` item with no
    /// execution may reserve from `Blocked`. After a known `Failure` or a stored
    /// acceptance rejection of a technical `Success`, a new reservation is
    /// allowed only with a different fresh approval/task pair and the current
    /// revision. An `Unknown` or successful attempt pending acceptance cannot
    /// reserve a replacement.
    #[allow(clippy::too_many_arguments)]
    pub fn reserve_worker_backlog_execution_operator_attested(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        authority: WorkerBacklogAuthority,
        now_ms: i64,
    ) -> Result<WorkerBacklogExecution, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let clean_authority = validate_authority(authority)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            RESERVE_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_authority,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_worker_execution_replay(
            &tx,
            &clean_binding_id,
            &clean_key,
            RESERVE_OPERATION,
            &payload_hash,
            &clean_item_id,
            None,
        )? {
            return Ok(existing);
        }

        let mut backlog_item = load_backlog_item_conn(&tx, &clean_binding_id, &clean_item_id)?;
        if backlog_item.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: clean_item_id,
                expected_revision,
                actual_revision: backlog_item.revision,
            });
        }
        if backlog_item.kind != BacklogItemKind::Worker {
            return Err(worker_api_requires_worker(&backlog_item, "reserve"));
        }
        match backlog_item.status {
            BacklogStatus::Todo => {}
            BacklogStatus::Blocked => {
                if let Some(latest) =
                    latest_worker_execution_conn(&tx, &clean_binding_id, &clean_item_id)?
                {
                    let failed = latest
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| outcome.outcome == WorkerBacklogOutcome::Failure);
                    let rejected = latest.acceptance_rejection.is_some();
                    if !failed && !rejected {
                        return Err(invalid_worker_transition(
                            &backlog_item,
                            "reserve",
                            "a previous attempt is unresolved or successful pending acceptance rejection",
                        ));
                    }
                } else if backlog_item.current_execution_ref.is_some() {
                    return Err(CoreError::CorruptState(format!(
                        "blocked Worker item `{clean_item_id}` references an execution with no ledger row"
                    )));
                }
            }
            BacklogStatus::Doing | BacklogStatus::Done => {
                return Err(invalid_worker_transition(
                    &backlog_item,
                    "reserve",
                    "a current reservation, unknown attempt, or successful pending item cannot reserve a replacement",
                ));
            }
        }

        if let Some(existing_ref) = find_worker_execution_by_authority_conn(&tx, &clean_authority)?
        {
            return Err(CoreError::Validation {
                field: "authority",
                reason: format!(
                    "approval `{}` and task `{}` are already attached to execution `{existing_ref}`",
                    clean_authority.approval_id, clean_authority.task_id
                ),
            });
        }

        let (acceptance_condition_digest, delivery_description_digest) =
            condition_digests(&backlog_item)?;
        let previous_revision = backlog_item.revision;
        let reservation_revision = next_backlog_revision(&backlog_item)?;
        let execution = WorkerBacklogExecution {
            execution_ref: generate_id("wbe"),
            binding_id: clean_binding_id.clone(),
            item_id: clean_item_id.clone(),
            authority: clean_authority,
            acceptance_condition_digest,
            delivery_description_digest,
            reservation_revision,
            reserved_at_ms: valid_now,
            dispatch_receipt: None,
            attached_at_ms: None,
            outcome: None,
            completion: None,
            acceptance_rejection: None,
        };
        validate_worker_execution(&execution)?;
        insert_worker_execution_tx(&tx, &execution)?;

        backlog_item.status = BacklogStatus::Doing;
        backlog_item.blocked_reason = None;
        backlog_item.current_execution_ref = Some(execution.execution_ref.clone());
        backlog_item.revision = reservation_revision;
        backlog_item.updated_at_ms = valid_now;
        crate::backlog::validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        record_backlog_history_tx(
            &tx,
            &backlog_item,
            BacklogHistoryAction::WorkerReserved,
            valid_now,
        )?;
        let response_json = serialize_worker_response(&execution)?;
        record_backlog_dedup_raw(
            &tx,
            &clean_binding_id,
            &clean_key,
            RESERVE_OPERATION,
            &payload_hash,
            &response_json,
            valid_now,
        )?;
        tx.commit()?;
        Ok(execution)
    }

    /// Attaches the real exact dispatch receipt once the upstream caller knows it.
    ///
    /// Trusted-local operator-attested method. An execution accepts only one
    /// dispatch identity. Exact command replay is harmless, identical duplicate
    /// attachment adds no revision or history, and a different receipt is
    /// rejected. Attachment is allowed after an `Unknown` outcome so the original
    /// invocation can still be reconciled.
    #[allow(clippy::too_many_arguments)]
    pub fn attach_worker_backlog_dispatch_receipt_operator_attested(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        execution_ref: &str,
        expected_revision: u64,
        receipt: WorkerBacklogDispatchReceipt,
        now_ms: i64,
    ) -> Result<WorkerBacklogExecution, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        let clean_execution_ref = validate_execution_ref(execution_ref)?;
        validate_expected_revision(expected_revision)?;
        let clean_receipt = validate_dispatch_receipt(receipt)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            ATTACH_OPERATION,
            &clean_item_id,
            &clean_execution_ref,
            expected_revision,
            &clean_receipt,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_worker_execution_replay(
            &tx,
            &clean_binding_id,
            &clean_key,
            ATTACH_OPERATION,
            &payload_hash,
            &clean_item_id,
            Some(&clean_execution_ref),
        )? {
            return Ok(existing);
        }

        let mut backlog_item = load_backlog_item_conn(&tx, &clean_binding_id, &clean_item_id)?;
        if backlog_item.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: clean_item_id,
                expected_revision,
                actual_revision: backlog_item.revision,
            });
        }
        if backlog_item.kind != BacklogItemKind::Worker {
            return Err(worker_api_requires_worker(&backlog_item, "attach receipt"));
        }
        if backlog_item.current_execution_ref.as_deref() != Some(clean_execution_ref.as_str()) {
            return Err(CoreError::Validation {
                field: "execution_ref",
                reason: format!(
                    "execution `{clean_execution_ref}` is not the current execution for backlog item `{clean_item_id}`"
                ),
            });
        }
        if matches!(
            backlog_item.status,
            BacklogStatus::Todo | BacklogStatus::Done
        ) {
            return Err(invalid_worker_transition(
                &backlog_item,
                "attach receipt",
                "only a reserved or unknown-blocked Worker item can attach a dispatch receipt",
            ));
        }

        let mut execution = load_worker_execution_conn(
            &tx,
            &clean_binding_id,
            &clean_item_id,
            &clean_execution_ref,
        )?;
        if let Some(existing_receipt) = execution.dispatch_receipt.as_ref() {
            if existing_receipt != &clean_receipt {
                return Err(CoreError::Validation {
                    field: "receipt",
                    reason: "the execution already has a different dispatch receipt".to_string(),
                });
            }
            let response_json = serialize_worker_response(&execution)?;
            record_backlog_dedup_raw(
                &tx,
                &clean_binding_id,
                &clean_key,
                ATTACH_OPERATION,
                &payload_hash,
                &response_json,
                valid_now,
            )?;
            tx.commit()?;
            return Ok(execution);
        }
        if let Some(outcome) = execution.outcome.as_ref() {
            if outcome.outcome.is_known() {
                return Err(CoreError::CorruptState(format!(
                    "worker execution `{clean_execution_ref}` has a known outcome without a dispatch receipt"
                )));
            }
        }
        if backlog_item.status == BacklogStatus::Blocked {
            let unknown = execution
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.outcome == WorkerBacklogOutcome::Unknown);
            if !unknown {
                return Err(blocked_without_unknown_corrupt(&backlog_item));
            }
        }

        execution.dispatch_receipt = Some(clean_receipt);
        execution.attached_at_ms = Some(valid_now);
        validate_worker_execution(&execution)?;

        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        crate::backlog::validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        update_worker_execution_tx(&tx, &execution)?;
        record_backlog_history_tx(
            &tx,
            &backlog_item,
            BacklogHistoryAction::WorkerExecutionAttached,
            valid_now,
        )?;
        let response_json = serialize_worker_response(&execution)?;
        record_backlog_dedup_raw(
            &tx,
            &clean_binding_id,
            &clean_key,
            ATTACH_OPERATION,
            &payload_hash,
            &response_json,
            valid_now,
        )?;
        tx.commit()?;
        Ok(execution)
    }

    /// Records one operator-verified outcome for the exact current execution.
    ///
    /// Trusted-local operator-attested method, never part of scoped MCP. `Success`
    /// and `Failure` require an exact attached dispatch receipt and the canonical
    /// lowercase hex `result_digest` computed upstream from the exact verified
    /// terminal result evidence; `Unknown` requires no digest. Core checks only
    /// digest structure and correlation with the resolved execution; the trusted
    /// upstream facade verifies the actual terminal result. The supplied digest is
    /// persisted unchanged for the later exact completion check. `Unknown` may be
    /// recorded for an unattached ambiguous start using the frozen authority
    /// tuple; it stores no final result digest and creates no run. `Success`
    /// leaves the item in `Doing`; `Failure` and `Unknown` block it. Once a known
    /// outcome is stored, a contradictory outcome cannot overwrite it. An
    /// `Unknown` outcome may be reconciled once with a known outcome for the same
    /// execution reference.
    #[allow(clippy::too_many_arguments)]
    pub fn record_worker_backlog_outcome_operator_attested(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        execution_ref: &str,
        expected_revision: u64,
        report: WorkerBacklogOutcomeReport,
        now_ms: i64,
    ) -> Result<WorkerBacklogExecution, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        let clean_execution_ref = validate_execution_ref(execution_ref)?;
        validate_expected_revision(expected_revision)?;
        let clean_note =
            validate_bounded_text("note", &report.note, MAX_WORKER_BACKLOG_OUTCOME_NOTE_LEN)?;
        let clean_result_digest =
            validate_outcome_report_digest(report.outcome, report.result_digest.as_deref())?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            OUTCOME_OPERATION,
            &clean_item_id,
            &clean_execution_ref,
            expected_revision,
            report.outcome,
            &clean_note,
            clean_result_digest.as_deref(),
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_worker_execution_replay(
            &tx,
            &clean_binding_id,
            &clean_key,
            OUTCOME_OPERATION,
            &payload_hash,
            &clean_item_id,
            Some(&clean_execution_ref),
        )? {
            return Ok(existing);
        }

        let mut backlog_item = load_backlog_item_conn(&tx, &clean_binding_id, &clean_item_id)?;
        if backlog_item.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: clean_item_id,
                expected_revision,
                actual_revision: backlog_item.revision,
            });
        }
        if backlog_item.kind != BacklogItemKind::Worker {
            return Err(worker_api_requires_worker(&backlog_item, "record outcome"));
        }
        if backlog_item.current_execution_ref.as_deref() != Some(clean_execution_ref.as_str()) {
            return Err(CoreError::Validation {
                field: "execution_ref",
                reason: format!(
                    "execution `{clean_execution_ref}` is not the current execution for backlog item `{clean_item_id}`"
                ),
            });
        }
        if !matches!(
            backlog_item.status,
            BacklogStatus::Doing | BacklogStatus::Blocked
        ) {
            return Err(invalid_worker_transition(
                &backlog_item,
                "record outcome",
                "only a reserved or unknown-blocked Worker item can record an outcome",
            ));
        }

        let mut execution = load_worker_execution_conn(
            &tx,
            &clean_binding_id,
            &clean_item_id,
            &clean_execution_ref,
        )?;
        if execution.completion.is_some() {
            return Err(CoreError::Validation {
                field: "outcome",
                reason: "a completed Worker execution cannot record another outcome".to_string(),
            });
        }
        if execution.acceptance_rejection.is_some() {
            return Err(CoreError::Validation {
                field: "acceptance_rejection",
                reason: "a rejected Worker execution cannot record another outcome".to_string(),
            });
        }
        if let Some(previous) = execution.outcome.as_ref() {
            let reconciling_unknown =
                previous.outcome == WorkerBacklogOutcome::Unknown && report.outcome.is_known();
            if !reconciling_unknown {
                return Err(CoreError::Validation {
                    field: "outcome",
                    reason: format!(
                        "execution `{clean_execution_ref}` already has recorded outcome `{}`",
                        previous.outcome.as_str()
                    ),
                });
            }
        }
        if report.outcome.is_known() && execution.dispatch_receipt.is_none() {
            return Err(CoreError::Validation {
                field: "dispatch_receipt",
                reason: "a known outcome requires the exact attached dispatch receipt".to_string(),
            });
        }

        let recorded_outcome = WorkerBacklogRecordedOutcome {
            outcome: report.outcome,
            note: clean_note,
            result_digest: clean_result_digest,
            recorded_at_ms: valid_now,
        };
        execution.outcome = Some(recorded_outcome.clone());

        match report.outcome {
            WorkerBacklogOutcome::Success => {
                backlog_item.status = BacklogStatus::Doing;
                backlog_item.blocked_reason = None;
            }
            WorkerBacklogOutcome::Failure | WorkerBacklogOutcome::Unknown => {
                backlog_item.status = BacklogStatus::Blocked;
                backlog_item.blocked_reason = Some(blocked_reason_for_outcome(
                    report.outcome,
                    &recorded_outcome.note,
                )?);
            }
        }

        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        crate::backlog::validate_backlog_item(&backlog_item, &clean_binding_id)?;
        validate_worker_execution(&execution)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        update_worker_execution_tx(&tx, &execution)?;
        record_backlog_history_tx(
            &tx,
            &backlog_item,
            BacklogHistoryAction::WorkerOutcomeRecorded,
            valid_now,
        )?;
        let response_json = serialize_worker_response(&execution)?;
        record_backlog_dedup_raw(
            &tx,
            &clean_binding_id,
            &clean_key,
            OUTCOME_OPERATION,
            &payload_hash,
            &response_json,
            valid_now,
        )?;
        tx.commit()?;
        Ok(execution)
    }

    /// Commits separate Coworker acceptance and delivery completion for a
    /// successful Worker execution.
    ///
    /// Trusted-local operator-attested method. The request must name the exact
    /// current execution, echo the stored successful `result_digest`, and carry
    /// bounded acceptance note/evidence plus the upstream-computed
    /// `delivery_digest`. Core checks the reservation's frozen condition digests,
    /// stores the durable completion receipt together with `Done`, clears any
    /// blocked reason, appends history, and records command deduplication in one
    /// transaction. It does not inspect owned files or claim that delivery was
    /// independently verified.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_worker_backlog_item_operator_attested(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        completion: WorkerBacklogCompletion,
        now_ms: i64,
    ) -> Result<WorkerBacklogCompletionReceipt, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let clean_completion = validate_completion(completion)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            COMPLETE_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_completion,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_worker_completion_replay(
            &tx,
            &clean_binding_id,
            &clean_key,
            COMPLETE_OPERATION,
            &payload_hash,
            &clean_item_id,
            &clean_completion.execution_ref,
        )? {
            return Ok(existing);
        }

        let mut backlog_item = load_backlog_item_conn(&tx, &clean_binding_id, &clean_item_id)?;
        if backlog_item.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: clean_item_id,
                expected_revision,
                actual_revision: backlog_item.revision,
            });
        }
        if backlog_item.kind != BacklogItemKind::Worker {
            return Err(worker_api_requires_worker(
                &backlog_item,
                "complete Worker item",
            ));
        }
        if backlog_item.status != BacklogStatus::Doing {
            return Err(invalid_backlog_transition(&backlog_item, "complete Worker"));
        }
        if backlog_item.current_execution_ref.as_deref()
            != Some(clean_completion.execution_ref.as_str())
        {
            return Err(CoreError::Validation {
                field: "execution_ref",
                reason: format!(
                    "execution `{}` is not the current execution for backlog item `{clean_item_id}`",
                    clean_completion.execution_ref
                ),
            });
        }

        let mut execution = load_worker_execution_conn(
            &tx,
            &clean_binding_id,
            &clean_item_id,
            &clean_completion.execution_ref,
        )?;
        if execution.acceptance_rejection.is_some() {
            return Err(CoreError::Validation {
                field: "acceptance_rejection",
                reason: "a rejected Worker execution cannot be completed".to_string(),
            });
        }
        if execution.dispatch_receipt.is_none() {
            return Err(CoreError::Validation {
                field: "dispatch_receipt",
                reason: "completion requires an attached dispatch receipt".to_string(),
            });
        }
        let outcome = execution
            .outcome
            .as_ref()
            .ok_or_else(|| CoreError::Validation {
                field: "outcome",
                reason: "completion requires a recorded successful outcome".to_string(),
            })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(CoreError::Validation {
                field: "outcome",
                reason: "completion requires a recorded successful outcome".to_string(),
            });
        }
        let stored_result_digest = outcome.result_digest.as_deref().ok_or_else(|| {
            CoreError::CorruptState(format!(
                "successful execution `{}` has no stored result_digest",
                clean_completion.execution_ref
            ))
        })?;
        if stored_result_digest != clean_completion.result_digest {
            return Err(CoreError::Validation {
                field: "result_digest",
                reason: "result_digest does not match the stored successful outcome".to_string(),
            });
        }
        if execution.completion.is_some() {
            return Err(CoreError::Validation {
                field: "completion",
                reason: "the execution already has a completion receipt".to_string(),
            });
        }
        let (acceptance_condition_digest, delivery_description_digest) =
            condition_digests(&backlog_item)?;
        if acceptance_condition_digest != execution.acceptance_condition_digest {
            return Err(CoreError::Validation {
                field: "acceptance_condition",
                reason: "acceptance condition no longer matches the reservation digest".to_string(),
            });
        }
        if delivery_description_digest != execution.delivery_description_digest {
            return Err(CoreError::Validation {
                field: "delivery_description",
                reason: "delivery description no longer matches the reservation digest".to_string(),
            });
        }

        let completion_record = WorkerBacklogCompletionRecord {
            result_digest: clean_completion.result_digest.clone(),
            delivery_digest: clean_completion.delivery_digest,
            acceptance_note: clean_completion.acceptance_note.clone(),
            acceptance_evidence: clean_completion.acceptance_evidence.clone(),
            completed_at_ms: valid_now,
        };
        execution.completion = Some(completion_record);

        backlog_item.status = BacklogStatus::Done;
        backlog_item.blocked_reason = None;
        backlog_item.completion_note = Some(clean_completion.acceptance_note);
        backlog_item.completion_evidence = Some(clean_completion.acceptance_evidence);
        backlog_item.completed_at_ms = Some(valid_now);
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        crate::backlog::validate_backlog_item(&backlog_item, &clean_binding_id)?;
        validate_worker_execution(&execution)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        update_worker_execution_tx(&tx, &execution)?;
        record_backlog_history_tx(
            &tx,
            &backlog_item,
            BacklogHistoryAction::Completed,
            valid_now,
        )?;

        let receipt = WorkerBacklogCompletionReceipt {
            item: backlog_item,
            execution,
        };
        let response_json = serialize_worker_response(&receipt)?;
        record_backlog_dedup_raw(
            &tx,
            &clean_binding_id,
            &clean_key,
            COMPLETE_OPERATION,
            &payload_hash,
            &response_json,
            valid_now,
        )?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Rejects the Coworker acceptance of a technically successful Worker
    /// execution and blocks the item for a fresh authorized attempt.
    ///
    /// Trusted-local operator-attested method, never part of scoped MCP. The
    /// request must name the exact current execution and item revision, echo the
    /// stored successful `result_digest`, carry the upstream-checked
    /// `delivery_digest`, and include bounded concrete failed-acceptance
    /// reason/evidence. Core checks the reservation's frozen condition digests,
    /// persists the rejection in a separate durable record, stores `Blocked` with
    /// the bounded reason, appends one history entry, and records command
    /// deduplication in one transaction. The immutable technical `Success`
    /// outcome is never overwritten, and no run, job, worker, event, question, or
    /// human request is created. Replaying the exact request returns the original
    /// receipt without another revision or history entry.
    #[allow(clippy::too_many_arguments)]
    pub fn reject_worker_backlog_acceptance_operator_attested(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        rejection: WorkerBacklogAcceptanceRejection,
        now_ms: i64,
    ) -> Result<WorkerBacklogAcceptanceRejectionReceipt, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let clean_rejection = validate_acceptance_rejection(rejection)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            REJECT_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_rejection,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_worker_acceptance_rejection_replay(
            &tx,
            &clean_binding_id,
            &clean_key,
            REJECT_OPERATION,
            &payload_hash,
            &clean_item_id,
            &clean_rejection.execution_ref,
        )? {
            return Ok(existing);
        }

        let mut backlog_item = load_backlog_item_conn(&tx, &clean_binding_id, &clean_item_id)?;
        if backlog_item.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: clean_item_id,
                expected_revision,
                actual_revision: backlog_item.revision,
            });
        }
        if backlog_item.kind != BacklogItemKind::Worker {
            return Err(worker_api_requires_worker(
                &backlog_item,
                "reject Worker acceptance",
            ));
        }
        if backlog_item.status != BacklogStatus::Doing {
            return Err(invalid_worker_transition(
                &backlog_item,
                "reject Worker acceptance",
                "a technically successful execution pending acceptance is required",
            ));
        }
        if backlog_item.current_execution_ref.as_deref()
            != Some(clean_rejection.execution_ref.as_str())
        {
            return Err(CoreError::Validation {
                field: "execution_ref",
                reason: format!(
                    "execution `{}` is not the current execution for backlog item `{clean_item_id}`",
                    clean_rejection.execution_ref
                ),
            });
        }

        let mut execution = load_worker_execution_conn(
            &tx,
            &clean_binding_id,
            &clean_item_id,
            &clean_rejection.execution_ref,
        )?;
        if execution.completion.is_some() {
            return Err(CoreError::Validation {
                field: "completion",
                reason: "a completed Worker execution cannot be rejected".to_string(),
            });
        }
        if execution.acceptance_rejection.is_some() {
            return Err(CoreError::Validation {
                field: "acceptance_rejection",
                reason: "the execution already has an acceptance rejection record".to_string(),
            });
        }
        let outcome = execution
            .outcome
            .as_ref()
            .ok_or_else(|| CoreError::Validation {
                field: "outcome",
                reason: "acceptance rejection requires a recorded successful outcome".to_string(),
            })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(CoreError::Validation {
                field: "outcome",
                reason: "acceptance rejection requires a recorded successful outcome".to_string(),
            });
        }
        if execution.dispatch_receipt.is_none() {
            return Err(CoreError::Validation {
                field: "dispatch_receipt",
                reason: "acceptance rejection requires an attached dispatch receipt".to_string(),
            });
        }
        let stored_result_digest = outcome.result_digest.as_deref().ok_or_else(|| {
            CoreError::CorruptState(format!(
                "successful execution `{}` has no stored result_digest",
                clean_rejection.execution_ref
            ))
        })?;
        if stored_result_digest != clean_rejection.result_digest {
            return Err(CoreError::Validation {
                field: "result_digest",
                reason: "result_digest does not match the stored successful outcome".to_string(),
            });
        }
        let (acceptance_condition_digest, delivery_description_digest) =
            condition_digests(&backlog_item)?;
        if acceptance_condition_digest != execution.acceptance_condition_digest {
            return Err(CoreError::Validation {
                field: "acceptance_condition",
                reason: "acceptance condition no longer matches the reservation digest".to_string(),
            });
        }
        if delivery_description_digest != execution.delivery_description_digest {
            return Err(CoreError::Validation {
                field: "delivery_description",
                reason: "delivery description no longer matches the reservation digest".to_string(),
            });
        }

        let rejection_record = WorkerBacklogAcceptanceRejectionRecord {
            result_digest: clean_rejection.result_digest.clone(),
            delivery_digest: clean_rejection.delivery_digest.clone(),
            reason: clean_rejection.reason.clone(),
            evidence: clean_rejection.evidence.clone(),
            rejected_at_ms: valid_now,
        };
        execution.acceptance_rejection = Some(rejection_record);

        backlog_item.status = BacklogStatus::Blocked;
        backlog_item.blocked_reason = Some(blocked_reason_for_rejection(&clean_rejection.reason)?);
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        crate::backlog::validate_backlog_item(&backlog_item, &clean_binding_id)?;
        validate_worker_execution(&execution)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        insert_worker_acceptance_rejection_tx(&tx, &execution)?;
        record_backlog_history_tx(&tx, &backlog_item, BacklogHistoryAction::Blocked, valid_now)?;

        let receipt = WorkerBacklogAcceptanceRejectionReceipt {
            item: backlog_item,
            execution,
        };
        let response_json = serialize_worker_response(&receipt)?;
        record_backlog_dedup_raw(
            &tx,
            &clean_binding_id,
            &clean_key,
            REJECT_OPERATION,
            &payload_hash,
            &response_json,
            valid_now,
        )?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Reads one exact Worker execution scoped by binding and item.
    ///
    /// Trusted-local operator-attested read. This returns no paths or private raw
    /// provider output and is not part of the public scoped MCP surface.
    pub fn get_worker_backlog_execution_operator_attested(
        &self,
        binding_id: &str,
        item_id: &str,
        execution_ref: &str,
    ) -> Result<WorkerBacklogExecution, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        let clean_execution_ref = validate_execution_ref(execution_ref)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        let execution = load_worker_execution_conn(
            &self.conn,
            &clean_binding_id,
            &clean_item_id,
            &clean_execution_ref,
        )?;
        enforce_worker_response_budget(&execution, MAX_COMPACT_RESPONSE_BYTES)?;
        Ok(execution)
    }

    /// Lists one bounded stable-order page of historical Worker attempt
    /// identities for an item.
    ///
    /// Trusted-local operator-attested read. The page includes frozen authority
    /// identities and outcome/completion flags but no private raw output.
    pub fn list_worker_backlog_attempts_operator_attested(
        &self,
        binding_id: &str,
        item_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<WorkerBacklogAttemptPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        load_backlog_item_conn(&self.conn, &clean_binding_id, &clean_item_id)?;
        list_worker_attempts_conn(
            &self.conn,
            &clean_binding_id,
            &clean_item_id,
            cursor,
            valid_limit,
        )
    }
}

fn validate_authority(
    authority: WorkerBacklogAuthority,
) -> Result<WorkerBacklogAuthority, CoreError> {
    Ok(WorkerBacklogAuthority {
        approval_id: validate_identifier("approval_id", &authority.approval_id)?,
        subject_digest: validate_worker_digest("subject_digest", &authority.subject_digest)?,
        task_id: validate_identifier("task_id", &authority.task_id)?,
        task_digest: validate_worker_digest("task_digest", &authority.task_digest)?,
    })
}

fn validate_dispatch_receipt(
    receipt: WorkerBacklogDispatchReceipt,
) -> Result<WorkerBacklogDispatchReceipt, CoreError> {
    Ok(WorkerBacklogDispatchReceipt {
        session_binding_id: validate_identifier("session_binding_id", &receipt.session_binding_id)?,
        run_id: validate_identifier("run_id", &receipt.run_id)?,
        worker_id: validate_identifier("worker_id", &receipt.worker_id)?,
        profile_id: validate_identifier("profile_id", &receipt.profile_id)?,
        execution_binding_digest: receipt
            .execution_binding_digest
            .as_deref()
            .map(|digest| validate_worker_digest("execution_binding_digest", digest))
            .transpose()?,
    })
}

fn validate_completion(
    completion: WorkerBacklogCompletion,
) -> Result<WorkerBacklogCompletion, CoreError> {
    Ok(WorkerBacklogCompletion {
        execution_ref: validate_execution_ref(&completion.execution_ref)?,
        result_digest: validate_worker_digest("result_digest", &completion.result_digest)?,
        acceptance_note: validate_bounded_text(
            "acceptance_note",
            &completion.acceptance_note,
            MAX_BACKLOG_COMPLETION_NOTE_LEN,
        )?,
        acceptance_evidence: validate_bounded_text(
            "acceptance_evidence",
            &completion.acceptance_evidence,
            MAX_BACKLOG_COMPLETION_EVIDENCE_LEN,
        )?,
        delivery_digest: validate_worker_digest("delivery_digest", &completion.delivery_digest)?,
    })
}

fn validate_acceptance_rejection(
    rejection: WorkerBacklogAcceptanceRejection,
) -> Result<WorkerBacklogAcceptanceRejection, CoreError> {
    Ok(WorkerBacklogAcceptanceRejection {
        execution_ref: validate_execution_ref(&rejection.execution_ref)?,
        result_digest: validate_worker_digest("result_digest", &rejection.result_digest)?,
        delivery_digest: validate_worker_digest("delivery_digest", &rejection.delivery_digest)?,
        reason: validate_bounded_text(
            "reason",
            &rejection.reason,
            MAX_WORKER_BACKLOG_REJECTION_REASON_LEN,
        )?,
        evidence: validate_bounded_text(
            "evidence",
            &rejection.evidence,
            MAX_WORKER_BACKLOG_REJECTION_EVIDENCE_LEN,
        )?,
    })
}

fn validate_execution_ref(raw: &str) -> Result<String, CoreError> {
    let clean = validate_identifier("execution_ref", raw)?;
    if clean.contains(':') {
        return Err(CoreError::Validation {
            field: "execution_ref",
            reason: "execution_ref contains invalid characters".to_string(),
        });
    }
    Ok(clean)
}

fn validate_worker_digest(field: &'static str, raw: &str) -> Result<String, CoreError> {
    if raw.len() != MAX_WORKER_DIGEST_LEN
        || !raw
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        return Err(CoreError::Validation {
            field,
            reason: format!(
                "{field} must be a {MAX_WORKER_DIGEST_LEN}-character lowercase hex SHA-256 digest"
            ),
        });
    }
    Ok(raw.to_string())
}

fn condition_digests(item: &BacklogItem) -> Result<(String, String), CoreError> {
    validate_bounded_text(
        "acceptance_condition",
        &item.acceptance_condition,
        MAX_BACKLOG_ACCEPTANCE_LEN,
    )?;
    validate_bounded_text(
        "delivery_description",
        &item.delivery_description,
        MAX_BACKLOG_DELIVERY_LEN,
    )?;
    Ok((
        hash_dedup_payload(&(
            "backlog_worker_acceptance_condition",
            &item.acceptance_condition,
        ))?,
        hash_dedup_payload(&(
            "backlog_worker_delivery_description",
            &item.delivery_description,
        ))?,
    ))
}

fn validate_outcome_report_digest(
    outcome: WorkerBacklogOutcome,
    raw: Option<&str>,
) -> Result<Option<String>, CoreError> {
    match (outcome, raw) {
        (WorkerBacklogOutcome::Unknown, None) => Ok(None),
        (WorkerBacklogOutcome::Unknown, Some(_)) => Err(CoreError::Validation {
            field: "result_digest",
            reason: "an unknown worker outcome must not carry a final result digest".to_string(),
        }),
        (_, Some(digest)) => Ok(Some(validate_worker_digest("result_digest", digest)?)),
        (_, None) => Err(CoreError::Validation {
            field: "result_digest",
            reason: "a known worker outcome requires the upstream-verified terminal result digest"
                .to_string(),
        }),
    }
}

fn blocked_reason_for_outcome(
    outcome: WorkerBacklogOutcome,
    note: &str,
) -> Result<String, CoreError> {
    let reason = format!("worker {}: {note}", outcome.as_str());
    validate_bounded_text("blocked_reason", &reason, MAX_BACKLOG_BLOCKED_REASON_LEN)?;
    Ok(reason)
}

fn blocked_reason_for_rejection(reason: &str) -> Result<String, CoreError> {
    let blocked_reason = format!("worker acceptance rejected: {reason}");
    validate_bounded_text(
        "blocked_reason",
        &blocked_reason,
        MAX_BACKLOG_BLOCKED_REASON_LEN,
    )?;
    Ok(blocked_reason)
}

fn worker_api_requires_worker(item: &BacklogItem, action: &'static str) -> CoreError {
    CoreError::Validation {
        field: "kind",
        reason: format!(
            "backlog item `{}` is `{}`; `{action}` requires a Worker item",
            item.item_id,
            item.kind.as_str()
        ),
    }
}

fn invalid_worker_transition(item: &BacklogItem, action: &'static str, reason: &str) -> CoreError {
    CoreError::Validation {
        field: "status",
        reason: format!(
            "cannot perform `{action}` on backlog item `{}` while in status `{}`: {reason}",
            item.item_id,
            item.status.as_str()
        ),
    }
}

fn blocked_without_unknown_corrupt(item: &BacklogItem) -> CoreError {
    CoreError::CorruptState(format!(
        "blocked Worker item `{}` has an execution without a recorded unknown outcome",
        item.item_id
    ))
}

fn validate_worker_execution(execution: &WorkerBacklogExecution) -> Result<(), CoreError> {
    validate_execution_ref(&execution.execution_ref)?;
    validate_identifier("binding_id", &execution.binding_id)?;
    validate_backlog_item_id(&execution.item_id)?;
    validate_authority(execution.authority.clone())?;
    validate_worker_digest(
        "acceptance_condition_digest",
        &execution.acceptance_condition_digest,
    )?;
    validate_worker_digest(
        "delivery_description_digest",
        &execution.delivery_description_digest,
    )?;
    if execution.reservation_revision == 0 {
        return Err(CoreError::CorruptState(format!(
            "worker execution `{}` has reservation revision 0",
            execution.execution_ref
        )));
    }
    if execution.reserved_at_ms < 0 {
        return Err(CoreError::CorruptState(format!(
            "worker execution `{}` has a negative reservation timestamp",
            execution.execution_ref
        )));
    }
    if execution.dispatch_receipt.is_some() != execution.attached_at_ms.is_some() {
        return Err(CoreError::CorruptState(format!(
            "worker execution `{}` has a partial dispatch receipt",
            execution.execution_ref
        )));
    }
    if let Some(receipt) = execution.dispatch_receipt.as_ref() {
        validate_dispatch_receipt(receipt.clone())?;
        if execution.attached_at_ms.is_some_and(|value| value < 0) {
            return Err(CoreError::CorruptState(format!(
                "worker execution `{}` has a negative attachment timestamp",
                execution.execution_ref
            )));
        }
    }
    if let Some(outcome) = execution.outcome.as_ref() {
        validate_bounded_text(
            "outcome_note",
            &outcome.note,
            MAX_WORKER_BACKLOG_OUTCOME_NOTE_LEN,
        )?;
        if outcome.recorded_at_ms < 0 {
            return Err(CoreError::CorruptState(format!(
                "worker execution `{}` has a negative outcome timestamp",
                execution.execution_ref
            )));
        }
        match (outcome.outcome, outcome.result_digest.as_deref()) {
            (WorkerBacklogOutcome::Unknown, None) => {}
            (WorkerBacklogOutcome::Unknown, Some(_)) => {
                return Err(CoreError::CorruptState(format!(
                    "unknown worker outcome for `{}` carries a final result digest",
                    execution.execution_ref
                )));
            }
            (_, Some(digest)) => {
                validate_worker_digest("result_digest", digest)?;
            }
            (_, None) => {
                return Err(CoreError::CorruptState(format!(
                    "known worker outcome for `{}` has no result digest",
                    execution.execution_ref
                )));
            }
        }
        if outcome.outcome.is_known() && execution.dispatch_receipt.is_none() {
            return Err(CoreError::CorruptState(format!(
                "known worker outcome for `{}` has no dispatch receipt",
                execution.execution_ref
            )));
        }
    }
    if let Some(completion) = execution.completion.as_ref() {
        if execution.dispatch_receipt.is_none() {
            return Err(CoreError::CorruptState(format!(
                "completed worker execution `{}` has no dispatch receipt",
                execution.execution_ref
            )));
        }
        let outcome = execution.outcome.as_ref().ok_or_else(|| {
            CoreError::CorruptState(format!(
                "completed worker execution `{}` has no outcome",
                execution.execution_ref
            ))
        })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(CoreError::CorruptState(format!(
                "completed worker execution `{}` does not have a successful outcome",
                execution.execution_ref
            )));
        }
        if outcome.result_digest.as_deref() != Some(completion.result_digest.as_str()) {
            return Err(CoreError::CorruptState(format!(
                "completed worker execution `{}` result digest does not match its outcome",
                execution.execution_ref
            )));
        }
        validate_worker_digest("completion_result_digest", &completion.result_digest)?;
        validate_worker_digest("completion_delivery_digest", &completion.delivery_digest)?;
        validate_bounded_text(
            "completion_acceptance_note",
            &completion.acceptance_note,
            MAX_BACKLOG_COMPLETION_NOTE_LEN,
        )?;
        validate_bounded_text(
            "completion_acceptance_evidence",
            &completion.acceptance_evidence,
            MAX_BACKLOG_COMPLETION_EVIDENCE_LEN,
        )?;
        if completion.completed_at_ms < 0 {
            return Err(CoreError::CorruptState(format!(
                "completed worker execution `{}` has a negative completion timestamp",
                execution.execution_ref
            )));
        }
    }
    if let Some(rejection) = execution.acceptance_rejection.as_ref() {
        if execution.completion.is_some() {
            return Err(CoreError::CorruptState(format!(
                "worker execution `{}` has both completion and acceptance rejection data",
                execution.execution_ref
            )));
        }
        let outcome = execution.outcome.as_ref().ok_or_else(|| {
            CoreError::CorruptState(format!(
                "rejected worker execution `{}` has no outcome",
                execution.execution_ref
            ))
        })?;
        if outcome.outcome != WorkerBacklogOutcome::Success {
            return Err(CoreError::CorruptState(format!(
                "rejected worker execution `{}` does not have a successful outcome",
                execution.execution_ref
            )));
        }
        if outcome.result_digest.as_deref() != Some(rejection.result_digest.as_str()) {
            return Err(CoreError::CorruptState(format!(
                "rejected worker execution `{}` result digest does not match its outcome",
                execution.execution_ref
            )));
        }
        validate_worker_digest("rejection_result_digest", &rejection.result_digest)?;
        validate_worker_digest("rejection_delivery_digest", &rejection.delivery_digest)?;
        validate_bounded_text(
            "rejection_reason",
            &rejection.reason,
            MAX_WORKER_BACKLOG_REJECTION_REASON_LEN,
        )?;
        validate_bounded_text(
            "rejection_evidence",
            &rejection.evidence,
            MAX_WORKER_BACKLOG_REJECTION_EVIDENCE_LEN,
        )?;
        if rejection.rejected_at_ms < 0 {
            return Err(CoreError::CorruptState(format!(
                "rejected worker execution `{}` has a negative rejection timestamp",
                execution.execution_ref
            )));
        }
    }
    Ok(())
}

fn insert_worker_execution_tx(
    tx: &Transaction<'_>,
    execution: &WorkerBacklogExecution,
) -> Result<(), CoreError> {
    tx.execute(
        "INSERT INTO backlog_worker_executions (
            execution_ref, binding_id, item_id, approval_id, subject_digest,
            task_id, task_digest, acceptance_condition_digest,
            delivery_description_digest, reservation_revision, reserved_at_ms,
            session_binding_id, run_id, worker_id, profile_id,
            execution_binding_digest, attached_at_ms, outcome, outcome_note,
            outcome_result_digest, outcome_recorded_at_ms, completion_result_digest,
            completion_delivery_digest, completion_note, completion_evidence,
            completed_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                   ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)",
        params![
            execution.execution_ref,
            execution.binding_id,
            execution.item_id,
            execution.authority.approval_id,
            execution.authority.subject_digest,
            execution.authority.task_id,
            execution.authority.task_digest,
            execution.acceptance_condition_digest,
            execution.delivery_description_digest,
            u64_to_i64("reservation_revision", execution.reservation_revision)?,
            execution.reserved_at_ms,
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.session_binding_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.run_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.worker_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.profile_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .and_then(|receipt| receipt.execution_binding_digest.as_deref()),
            execution.attached_at_ms,
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.outcome.as_str()),
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.note.as_str()),
            execution
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.recorded_at_ms),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.result_digest.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.delivery_digest.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.acceptance_note.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.acceptance_evidence.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.completed_at_ms),
        ],
    )?;
    Ok(())
}

fn update_worker_execution_tx(
    tx: &Transaction<'_>,
    execution: &WorkerBacklogExecution,
) -> Result<(), CoreError> {
    let updated = tx.execute(
        "UPDATE backlog_worker_executions
         SET session_binding_id = ?1,
             run_id = ?2,
             worker_id = ?3,
             profile_id = ?4,
             execution_binding_digest = ?5,
             attached_at_ms = ?6,
             outcome = ?7,
             outcome_note = ?8,
             outcome_result_digest = ?9,
             outcome_recorded_at_ms = ?10,
             completion_result_digest = ?11,
             completion_delivery_digest = ?12,
             completion_note = ?13,
             completion_evidence = ?14,
             completed_at_ms = ?15
         WHERE execution_ref = ?16",
        params![
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.session_binding_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.run_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.worker_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .map(|receipt| receipt.profile_id.as_str()),
            execution
                .dispatch_receipt
                .as_ref()
                .and_then(|receipt| receipt.execution_binding_digest.as_deref()),
            execution.attached_at_ms,
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.outcome.as_str()),
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.note.as_str()),
            execution
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            execution
                .outcome
                .as_ref()
                .map(|outcome| outcome.recorded_at_ms),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.result_digest.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.delivery_digest.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.acceptance_note.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.acceptance_evidence.as_str()),
            execution
                .completion
                .as_ref()
                .map(|completion| completion.completed_at_ms),
            execution.execution_ref,
        ],
    )?;
    if updated != 1 {
        return Err(CoreError::CorruptState(format!(
            "worker execution `{}` disappeared during an exclusive mutation",
            execution.execution_ref
        )));
    }
    Ok(())
}

fn insert_worker_acceptance_rejection_tx(
    tx: &Transaction<'_>,
    execution: &WorkerBacklogExecution,
) -> Result<(), CoreError> {
    let rejection = execution.acceptance_rejection.as_ref().ok_or_else(|| {
        CoreError::CorruptState(format!(
            "worker execution `{}` has no acceptance rejection to persist",
            execution.execution_ref
        ))
    })?;
    tx.execute(
        "INSERT INTO backlog_worker_acceptance_rejections (
            execution_ref, binding_id, item_id, result_digest, delivery_digest,
            reason, evidence, rejected_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            execution.execution_ref,
            execution.binding_id,
            execution.item_id,
            rejection.result_digest,
            rejection.delivery_digest,
            rejection.reason,
            rejection.evidence,
            rejection.rejected_at_ms,
        ],
    )?;
    Ok(())
}

fn load_worker_acceptance_rejection_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
    execution_ref: &str,
) -> Result<Option<WorkerBacklogAcceptanceRejectionRecord>, CoreError> {
    let row: Option<(String, String, String, String, String, String, i64)> = conn
        .query_row(
            "SELECT binding_id, item_id, result_digest, delivery_digest, reason,
                    evidence, rejected_at_ms
             FROM backlog_worker_acceptance_rejections
             WHERE execution_ref = ?1",
            params![execution_ref],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        stored_binding_id,
        stored_item_id,
        result_digest,
        delivery_digest,
        reason,
        evidence,
        rejected_at_ms,
    )) = row
    else {
        return Ok(None);
    };
    if stored_binding_id != binding_id || stored_item_id != item_id {
        return Err(CoreError::CorruptState(format!(
            "acceptance rejection for execution `{execution_ref}` belongs to `{stored_binding_id}`/`{stored_item_id}` instead of `{binding_id}`/`{item_id}`"
        )));
    }
    Ok(Some(WorkerBacklogAcceptanceRejectionRecord {
        result_digest,
        delivery_digest,
        reason,
        evidence,
        rejected_at_ms,
    }))
}

fn load_worker_execution_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
    execution_ref: &str,
) -> Result<WorkerBacklogExecution, CoreError> {
    let row: Option<WorkerExecutionRow> = conn
        .query_row(
            "SELECT execution_ref, binding_id, item_id, approval_id,
                    subject_digest, task_id, task_digest,
                    acceptance_condition_digest, delivery_description_digest,
                    reservation_revision, reserved_at_ms, session_binding_id,
                    run_id, worker_id, profile_id, execution_binding_digest,
                    attached_at_ms, outcome, outcome_note, outcome_result_digest,
                    outcome_recorded_at_ms, completion_result_digest,
                    completion_delivery_digest, completion_note,
                    completion_evidence, completed_at_ms
             FROM backlog_worker_executions
             WHERE binding_id = ?1 AND item_id = ?2 AND execution_ref = ?3",
            params![binding_id, item_id, execution_ref],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                    row.get(16)?,
                    row.get(17)?,
                    row.get(18)?,
                    row.get(19)?,
                    row.get(20)?,
                    row.get(21)?,
                    row.get(22)?,
                    row.get(23)?,
                    row.get(24)?,
                    row.get(25)?,
                ))
            },
        )
        .optional()?;

    let Some((
        stored_execution_ref,
        stored_binding_id,
        stored_item_id,
        approval_id,
        subject_digest,
        task_id,
        task_digest,
        acceptance_condition_digest,
        delivery_description_digest,
        reservation_revision,
        reserved_at_ms,
        session_binding_id,
        run_id,
        worker_id,
        profile_id,
        execution_binding_digest,
        attached_at_ms,
        outcome_raw,
        outcome_note,
        outcome_result_digest,
        outcome_recorded_at_ms,
        completion_result_digest,
        completion_delivery_digest,
        completion_note,
        completion_evidence,
        completed_at_ms,
    )) = row
    else {
        return Err(CoreError::Validation {
            field: "execution_ref",
            reason: format!(
                "worker execution `{execution_ref}` not found for item `{item_id}` on binding `{binding_id}`"
            ),
        });
    };

    let dispatch_receipt = match (session_binding_id, run_id, worker_id, profile_id) {
        (None, None, None, None) => {
            if execution_binding_digest.is_some() || attached_at_ms.is_some() {
                return Err(CoreError::CorruptState(format!(
                    "worker execution `{stored_execution_ref}` has receipt fields without a receipt"
                )));
            }
            None
        }
        (Some(session_binding_id), Some(run_id), Some(worker_id), Some(profile_id)) => {
            Some(WorkerBacklogDispatchReceipt {
                session_binding_id,
                run_id,
                worker_id,
                profile_id,
                execution_binding_digest,
            })
        }
        _ => {
            return Err(CoreError::CorruptState(format!(
                "worker execution `{stored_execution_ref}` has a partial dispatch receipt"
            )));
        }
    };

    let outcome = match outcome_raw {
        None => {
            if outcome_note.is_some()
                || outcome_result_digest.is_some()
                || outcome_recorded_at_ms.is_some()
            {
                return Err(CoreError::CorruptState(format!(
                    "worker execution `{stored_execution_ref}` has partial outcome fields"
                )));
            }
            None
        }
        Some(raw_outcome) => {
            let outcome = WorkerBacklogOutcome::from_str(&raw_outcome).ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "worker execution `{stored_execution_ref}` has invalid outcome `{raw_outcome}`"
                ))
            })?;
            let note = outcome_note.ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "worker execution `{stored_execution_ref}` has no outcome note"
                ))
            })?;
            let recorded_at_ms = outcome_recorded_at_ms.ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "worker execution `{stored_execution_ref}` has no outcome timestamp"
                ))
            })?;
            let result_digest = if outcome == WorkerBacklogOutcome::Unknown {
                if outcome_result_digest.is_some() {
                    return Err(CoreError::CorruptState(format!(
                        "unknown worker execution `{stored_execution_ref}` carries a final result digest"
                    )));
                }
                None
            } else {
                Some(outcome_result_digest.ok_or_else(|| {
                    CoreError::CorruptState(format!(
                        "known worker execution `{stored_execution_ref}` has no result digest"
                    ))
                })?)
            };
            Some(WorkerBacklogRecordedOutcome {
                outcome,
                note,
                result_digest,
                recorded_at_ms,
            })
        }
    };

    let completion = match (
        completion_result_digest,
        completion_delivery_digest,
        completion_note,
        completion_evidence,
        completed_at_ms,
    ) {
        (None, None, None, None, None) => None,
        (
            Some(result_digest),
            Some(delivery_digest),
            Some(acceptance_note),
            Some(acceptance_evidence),
            Some(completed_at_ms),
        ) => Some(WorkerBacklogCompletionRecord {
            result_digest,
            delivery_digest,
            acceptance_note,
            acceptance_evidence,
            completed_at_ms,
        }),
        _ => {
            return Err(CoreError::CorruptState(format!(
                "worker execution `{stored_execution_ref}` has partial completion fields"
            )));
        }
    };

    let acceptance_rejection = load_worker_acceptance_rejection_conn(
        conn,
        &stored_binding_id,
        &stored_item_id,
        &stored_execution_ref,
    )?;

    let execution = WorkerBacklogExecution {
        execution_ref: stored_execution_ref,
        binding_id: stored_binding_id,
        item_id: stored_item_id,
        authority: WorkerBacklogAuthority {
            approval_id,
            subject_digest,
            task_id,
            task_digest,
        },
        acceptance_condition_digest,
        delivery_description_digest,
        reservation_revision: i64_to_u64("reservation_revision", reservation_revision)?,
        reserved_at_ms,
        dispatch_receipt,
        attached_at_ms,
        outcome,
        completion,
        acceptance_rejection,
    };
    if execution.binding_id != binding_id || execution.item_id != item_id {
        return Err(CoreError::CorruptState(format!(
            "worker execution `{execution_ref}` belongs to `{}`/`{}` instead of `{binding_id}`/`{item_id}`",
            execution.binding_id, execution.item_id
        )));
    }
    validate_worker_execution(&execution)?;
    Ok(execution)
}

fn latest_worker_execution_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
) -> Result<Option<WorkerBacklogExecution>, CoreError> {
    let execution_ref: Option<String> = conn
        .query_row(
            "SELECT execution_ref
             FROM backlog_worker_executions
             WHERE binding_id = ?1 AND item_id = ?2
             ORDER BY rowid DESC
             LIMIT 1",
            params![binding_id, item_id],
            |row| row.get(0),
        )
        .optional()?;
    execution_ref
        .map(|execution_ref| load_worker_execution_conn(conn, binding_id, item_id, &execution_ref))
        .transpose()
}

fn find_worker_execution_by_authority_conn(
    conn: &Connection,
    authority: &WorkerBacklogAuthority,
) -> Result<Option<String>, CoreError> {
    let execution_ref: Option<String> = conn
        .query_row(
            "SELECT execution_ref
             FROM backlog_worker_executions
             WHERE approval_id = ?1 AND task_id = ?2
             LIMIT 1",
            params![authority.approval_id, authority.task_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(execution_ref)
}

fn check_worker_execution_replay(
    tx: &Transaction<'_>,
    binding_id: &str,
    command_key: &str,
    operation: &str,
    payload_hash: &str,
    expected_item_id: &str,
    expected_execution_ref: Option<&str>,
) -> Result<Option<WorkerBacklogExecution>, CoreError> {
    let Some((stored_operation, stored_hash, response_json)) =
        check_backlog_dedup_raw(tx, binding_id, command_key)?
    else {
        return Ok(None);
    };
    if stored_operation != operation || stored_hash != payload_hash {
        return Err(CoreError::IdempotencyConflict {
            binding_id: binding_id.to_string(),
            command_key: command_key.to_string(),
        });
    }
    let execution: WorkerBacklogExecution = serde_json::from_str(&response_json)?;
    validate_worker_execution(&execution)?;
    if execution.binding_id != binding_id || execution.item_id != expected_item_id {
        return Err(CoreError::CorruptState(format!(
            "Worker idempotency record for command `{command_key}` on binding `{binding_id}` returned execution `{}` for item `{}`",
            execution.execution_ref, execution.item_id
        )));
    }
    if let Some(expected_execution_ref) = expected_execution_ref {
        if execution.execution_ref != expected_execution_ref {
            return Err(CoreError::CorruptState(format!(
                "Worker idempotency record for command `{command_key}` returned execution `{}` instead of `{expected_execution_ref}`",
                execution.execution_ref
            )));
        }
    }
    enforce_worker_response_budget(&execution, MAX_COMPACT_RESPONSE_BYTES)?;
    Ok(Some(execution))
}

fn check_worker_completion_replay(
    tx: &Transaction<'_>,
    binding_id: &str,
    command_key: &str,
    operation: &str,
    payload_hash: &str,
    expected_item_id: &str,
    expected_execution_ref: &str,
) -> Result<Option<WorkerBacklogCompletionReceipt>, CoreError> {
    let Some((stored_operation, stored_hash, response_json)) =
        check_backlog_dedup_raw(tx, binding_id, command_key)?
    else {
        return Ok(None);
    };
    if stored_operation != operation || stored_hash != payload_hash {
        return Err(CoreError::IdempotencyConflict {
            binding_id: binding_id.to_string(),
            command_key: command_key.to_string(),
        });
    }
    let receipt: WorkerBacklogCompletionReceipt = serde_json::from_str(&response_json)?;
    if receipt.item.binding_id != binding_id || receipt.item.item_id != expected_item_id {
        return Err(CoreError::CorruptState(format!(
            "Worker completion idempotency record for command `{command_key}` returned item `{}` on binding `{}`",
            receipt.item.item_id, receipt.item.binding_id
        )));
    }
    if receipt.execution.execution_ref != expected_execution_ref
        || receipt.execution.binding_id != binding_id
        || receipt.execution.item_id != expected_item_id
    {
        return Err(CoreError::CorruptState(format!(
            "Worker completion idempotency record for command `{command_key}` returned execution `{}`",
            receipt.execution.execution_ref
        )));
    }
    crate::backlog::validate_backlog_item(&receipt.item, binding_id)?;
    validate_worker_execution(&receipt.execution)?;
    enforce_worker_response_budget(&receipt, MAX_COMPACT_RESPONSE_BYTES)?;
    Ok(Some(receipt))
}

fn check_worker_acceptance_rejection_replay(
    tx: &Transaction<'_>,
    binding_id: &str,
    command_key: &str,
    operation: &str,
    payload_hash: &str,
    expected_item_id: &str,
    expected_execution_ref: &str,
) -> Result<Option<WorkerBacklogAcceptanceRejectionReceipt>, CoreError> {
    let Some((stored_operation, stored_hash, response_json)) =
        check_backlog_dedup_raw(tx, binding_id, command_key)?
    else {
        return Ok(None);
    };
    if stored_operation != operation || stored_hash != payload_hash {
        return Err(CoreError::IdempotencyConflict {
            binding_id: binding_id.to_string(),
            command_key: command_key.to_string(),
        });
    }
    let receipt: WorkerBacklogAcceptanceRejectionReceipt = serde_json::from_str(&response_json)?;
    if receipt.item.binding_id != binding_id || receipt.item.item_id != expected_item_id {
        return Err(CoreError::CorruptState(format!(
            "Worker acceptance rejection idempotency record for command `{command_key}` returned item `{}` on binding `{}`",
            receipt.item.item_id, receipt.item.binding_id
        )));
    }
    if receipt.execution.execution_ref != expected_execution_ref
        || receipt.execution.binding_id != binding_id
        || receipt.execution.item_id != expected_item_id
    {
        return Err(CoreError::CorruptState(format!(
            "Worker acceptance rejection idempotency record for command `{command_key}` returned execution `{}`",
            receipt.execution.execution_ref
        )));
    }
    if receipt.execution.acceptance_rejection.is_none() {
        return Err(CoreError::CorruptState(format!(
            "Worker acceptance rejection idempotency record for command `{command_key}` has no rejection data"
        )));
    }
    crate::backlog::validate_backlog_item(&receipt.item, binding_id)?;
    validate_worker_execution(&receipt.execution)?;
    enforce_worker_response_budget(&receipt, MAX_COMPACT_RESPONSE_BYTES)?;
    Ok(Some(receipt))
}

fn serialize_worker_response<T: Serialize>(value: &T) -> Result<String, CoreError> {
    enforce_worker_response_budget(value, MAX_COMPACT_RESPONSE_BYTES)?;
    Ok(serde_json::to_string(value)?)
}

fn enforce_worker_response_budget<T: Serialize>(
    value: &T,
    max_bytes: usize,
) -> Result<(), CoreError> {
    let size_bytes = estimate_wire_response_bytes(value)?;
    if size_bytes > max_bytes {
        return Err(CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes,
        });
    }
    Ok(())
}

fn list_worker_attempts_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<WorkerBacklogAttemptPage, CoreError> {
    let (after_rowid, high_water_mark_rowid) = match cursor {
        Some(raw_cursor) => {
            let (after, high_water_mark) =
                parse_worker_attempts_cursor(raw_cursor, binding_id, item_id)?;
            let anchor_count: i64 = conn.query_row(
                "SELECT COUNT(*)
                 FROM backlog_worker_executions
                 WHERE binding_id = ?1 AND item_id = ?2 AND rowid IN (?3, ?4)",
                params![
                    binding_id,
                    item_id,
                    u64_to_i64("cursor", after)?,
                    u64_to_i64("cursor", high_water_mark)?,
                ],
                |row| row.get(0),
            )?;
            if anchor_count != 2 {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "cursor references unknown or out-of-range Worker attempt position for this item"
                        .to_string(),
                });
            }
            (after, high_water_mark)
        }
        None => {
            let max_rowid: Option<i64> = conn.query_row(
                "SELECT MAX(rowid)
                 FROM backlog_worker_executions
                 WHERE binding_id = ?1 AND item_id = ?2",
                params![binding_id, item_id],
                |row| row.get(0),
            )?;
            let Some(max_raw) = max_rowid else {
                return Ok(WorkerBacklogAttemptPage {
                    binding_id: binding_id.to_string(),
                    item_id: item_id.to_string(),
                    attempts: Vec::new(),
                    next_cursor: None,
                    has_more: false,
                    high_water_mark_rowid: 0,
                });
            };
            (0, i64_to_u64("high_water_mark_rowid", max_raw)?)
        }
    };

    let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
    let candidate_attempts: Vec<WorkerAttemptRow> = {
        let mut stmt = conn.prepare(
            "SELECT rowid, execution_ref, approval_id, subject_digest, task_id,
                    task_digest, reserved_at_ms, attached_at_ms, outcome,
                    completion_result_digest
             FROM backlog_worker_executions
             WHERE binding_id = ?1 AND item_id = ?2 AND rowid > ?3 AND rowid <= ?4
             ORDER BY rowid ASC
             LIMIT ?5",
        )?;
        let mut rows = stmt.query(params![
            binding_id,
            item_id,
            u64_to_i64("after_rowid", after_rowid)?,
            u64_to_i64("high_water_mark_rowid", high_water_mark_rowid)?,
            fetch_limit,
        ])?;
        let mut attempts = Vec::new();
        while let Some(row) = rows.next()? {
            attempts.push((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
                row.get(9)?,
            ));
        }
        attempts
    };

    let db_has_more = candidate_attempts.len() > limit;
    let mut included_attempts = Vec::with_capacity(candidate_attempts.len().min(limit));
    let mut last_included_rowid = after_rowid;
    let mut stopped_by_budget = false;

    for (
        rowid,
        execution_ref,
        approval_id,
        subject_digest,
        task_id,
        task_digest,
        reserved_at_ms,
        attached_at_ms,
        outcome_raw,
        completion_result_digest,
    ) in candidate_attempts.into_iter().take(limit)
    {
        let attempt_rowid = i64_to_u64("rowid", rowid)?;
        let outcome = match outcome_raw {
            None => None,
            Some(raw) => Some(WorkerBacklogOutcome::from_str(&raw).ok_or_else(|| {
                CoreError::CorruptState(format!("invalid Worker attempt outcome `{raw}`"))
            })?),
        };
        included_attempts.push(WorkerBacklogAttemptIdentity {
            execution_ref,
            approval_id,
            subject_digest,
            task_id,
            task_digest,
            reserved_at_ms,
            dispatch_attached: attached_at_ms.is_some(),
            outcome,
            completed: completion_result_digest.is_some(),
        });

        let probe_output = WorkerBacklogAttemptPage {
            binding_id: binding_id.to_string(),
            item_id: item_id.to_string(),
            attempts: included_attempts.clone(),
            next_cursor: Some(encode_worker_attempts_cursor(
                binding_id,
                item_id,
                attempt_rowid,
                high_water_mark_rowid,
            )),
            has_more: true,
            high_water_mark_rowid,
        };
        let size_bytes = estimate_wire_response_bytes(&probe_output)?;
        if size_bytes > MAX_PAGE_RESPONSE_BYTES {
            if included_attempts.len() == 1 {
                return Err(CoreError::ResponseTooLarge {
                    size_bytes,
                    max_bytes: MAX_PAGE_RESPONSE_BYTES,
                });
            }
            included_attempts.pop();
            stopped_by_budget = true;
            break;
        }
        last_included_rowid = attempt_rowid;
    }

    let has_more = stopped_by_budget || db_has_more;
    let next_cursor = if has_more {
        Some(encode_worker_attempts_cursor(
            binding_id,
            item_id,
            last_included_rowid,
            high_water_mark_rowid,
        ))
    } else {
        None
    };

    Ok(WorkerBacklogAttemptPage {
        binding_id: binding_id.to_string(),
        item_id: item_id.to_string(),
        attempts: included_attempts,
        next_cursor,
        has_more,
        high_water_mark_rowid,
    })
}

fn encode_worker_attempts_cursor(
    binding_id: &str,
    item_id: &str,
    after_rowid: u64,
    hwm_rowid: u64,
) -> String {
    format!(
        "{WORKER_CURSOR_VERSION}:{WORKER_ATTEMPTS_CURSOR_KIND}:{binding_id}:{item_id}:{after_rowid}:{hwm_rowid}"
    )
}

fn parse_worker_attempts_cursor(
    raw: &str,
    expected_binding_id: &str,
    expected_item_id: &str,
) -> Result<(u64, u64), CoreError> {
    validate_backlog_cursor_text(raw)?;
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 6
        || parts[0] != WORKER_CURSOR_VERSION
        || parts[1] != WORKER_ATTEMPTS_CURSOR_KIND
    {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "malformed Worker attempts cursor structure".to_string(),
        });
    }
    if parts[2] != expected_binding_id {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor binding_id does not match request scope".to_string(),
        });
    }
    if parts[3] != expected_item_id {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor item_id does not match requested item".to_string(),
        });
    }
    let after = parse_backlog_cursor_position("position", parts[4])?;
    let high_water_mark = parse_backlog_cursor_position("high_water_mark", parts[5])?;
    if after >= high_water_mark {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!(
                "cursor position ({after}) must be strictly less than high_water_mark ({high_water_mark})"
            ),
        });
    }
    Ok((after, high_water_mark))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;
    use crate::backlog::{BacklogHistoryEntry, BacklogItemEdit, NewBacklogItem};
    use crate::limits::MAX_PAGE_LIMIT;

    const THREAD_A: &str = "11111111-1111-4111-8111-111111111111";
    const THREAD_B: &str = "22222222-2222-4222-8222-222222222222";

    fn open_temp_core() -> (TempDir, PathBuf, Core) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("threadlane.db");
        let core = Core::open(&db_path).expect("open core");
        (dir, db_path, core)
    }

    fn create_binding(core: &mut Core, thread_uuid: &str, label: &str) -> String {
        core.create_binding(thread_uuid, label)
            .expect("create binding")
            .binding_id
    }

    fn worker_item(title: &str) -> NewBacklogItem {
        NewBacklogItem {
            title: title.to_string(),
            acceptance_condition: format!("{title} satisfies its acceptance condition"),
            delivery_description: format!("Deliver {title}"),
            kind: BacklogItemKind::Worker,
        }
    }

    fn coworker_item(title: &str) -> NewBacklogItem {
        NewBacklogItem {
            title: title.to_string(),
            acceptance_condition: format!("{title} satisfies its acceptance condition"),
            delivery_description: format!("Deliver {title}"),
            kind: BacklogItemKind::Coworker,
        }
    }

    fn authority(tag: &str) -> WorkerBacklogAuthority {
        WorkerBacklogAuthority {
            approval_id: format!("apr_{tag}"),
            subject_digest: "a".repeat(64),
            task_id: format!("tsk_{tag}"),
            task_digest: "b".repeat(64),
        }
    }

    fn receipt(tag: &str) -> WorkerBacklogDispatchReceipt {
        WorkerBacklogDispatchReceipt {
            session_binding_id: format!("sbx_{tag}"),
            run_id: format!("run_{tag}"),
            worker_id: format!("wrk_{tag}"),
            profile_id: format!("profile_{tag}"),
            execution_binding_digest: Some("c".repeat(64)),
        }
    }

    fn completion(
        execution_ref: &str,
        result_digest: &str,
        delivery_digest: &str,
    ) -> WorkerBacklogCompletion {
        WorkerBacklogCompletion {
            execution_ref: execution_ref.to_string(),
            result_digest: result_digest.to_string(),
            acceptance_note: "Coworker accepted the delivered result".to_string(),
            acceptance_evidence: "Reviewed the bounded delivery and acceptance checks".to_string(),
            delivery_digest: delivery_digest.to_string(),
        }
    }

    fn acceptance_rejection(
        execution_ref: &str,
        result_digest: &str,
        delivery_digest: &str,
    ) -> WorkerBacklogAcceptanceRejection {
        WorkerBacklogAcceptanceRejection {
            execution_ref: execution_ref.to_string(),
            result_digest: result_digest.to_string(),
            delivery_digest: delivery_digest.to_string(),
            reason: "The delivered result does not satisfy the acceptance condition".to_string(),
            evidence: "Compared the bounded delivery with the frozen acceptance condition"
                .to_string(),
        }
    }

    fn reserve_attach_success(
        core: &mut Core,
        binding_id: &str,
        item_id: &str,
        tag: &str,
        result_digest: &str,
    ) -> WorkerBacklogExecution {
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                binding_id,
                &format!("reserve-{tag}"),
                item_id,
                1,
                authority(tag),
                1_010,
            )
            .expect("reserve Worker execution");
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            binding_id,
            &format!("attach-{tag}"),
            item_id,
            &reserved.execution_ref,
            2,
            receipt(tag),
            1_020,
        )
        .expect("attach dispatch receipt");
        core.record_worker_backlog_outcome_operator_attested(
            binding_id,
            &format!("success-{tag}"),
            item_id,
            &reserved.execution_ref,
            3,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified technical success".to_string(),
                result_digest: Some(result_digest.to_string()),
            },
            1_030,
        )
        .expect("record successful outcome")
    }

    fn history_entries(core: &Core, binding_id: &str, item_id: &str) -> Vec<BacklogHistoryEntry> {
        let mut all_entries = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = core
                .list_backlog_history(binding_id, item_id, cursor.as_deref(), MAX_PAGE_LIMIT)
                .expect("list backlog history");
            all_entries.extend(page.entries);
            if !page.has_more {
                break;
            }
            cursor = page.next_cursor;
        }
        all_entries
    }

    fn attempts(core: &Core, binding_id: &str, item_id: &str) -> Vec<WorkerBacklogAttemptIdentity> {
        core.list_worker_backlog_attempts_operator_attested(
            binding_id,
            item_id,
            None,
            MAX_PAGE_LIMIT,
        )
        .expect("list Worker attempts")
        .attempts
    }

    fn table_counts(core: &Core) -> (i64, i64, i64, i64) {
        let items = core
            .conn
            .query_row("SELECT COUNT(*) FROM backlog_items", [], |row| row.get(0))
            .expect("count items");
        let history = core
            .conn
            .query_row("SELECT COUNT(*) FROM backlog_history", [], |row| row.get(0))
            .expect("count history");
        let dedup = core
            .conn
            .query_row("SELECT COUNT(*) FROM backlog_command_dedup", [], |row| {
                row.get(0)
            })
            .expect("count dedup");
        let executions = core
            .conn
            .query_row(
                "SELECT COUNT(*) FROM backlog_worker_executions",
                [],
                |row| row.get(0),
            )
            .expect("count executions");
        (items, history, dedup, executions)
    }

    fn rejection_count(core: &Core) -> i64 {
        core.conn
            .query_row(
                "SELECT COUNT(*) FROM backlog_worker_acceptance_rejections",
                [],
                |row| row.get(0),
            )
            .expect("count acceptance rejections")
    }

    fn assert_validation(err: CoreError, expected_field: &str) {
        match err {
            CoreError::Validation { field, reason } => {
                assert_eq!(
                    field, expected_field,
                    "unexpected validation field (reason: {reason})"
                );
            }
            other => panic!("expected validation error for `{expected_field}`, got {other:?}"),
        }
    }

    fn assert_stale_revision(err: CoreError, expected_revision: u64, actual_revision: u64) {
        match err {
            CoreError::StaleRevision {
                expected_revision: expected,
                actual_revision: actual,
                ..
            } => {
                assert_eq!(expected, expected_revision);
                assert_eq!(actual, actual_revision);
            }
            other => panic!("expected stale revision error, got {other:?}"),
        }
    }

    fn assert_idempotency_conflict(err: CoreError) {
        match err {
            CoreError::IdempotencyConflict { .. } => {}
            other => panic!("expected idempotency conflict, got {other:?}"),
        }
    }

    #[test]
    fn reserve_persists_before_attach_and_duplicate_returns_original_snapshot() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "reserve persistence");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create-item",
                worker_item("Durable reservation"),
                1_000,
            )
            .expect("create Worker item");
        let frozen_authority = authority("reserve_persist");

        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                frozen_authority.clone(),
                1_010,
            )
            .expect("reserve Worker execution");
        assert!(reserved.execution_ref.starts_with("wbe_"));
        assert_eq!(reserved.binding_id, binding_id);
        assert_eq!(reserved.item_id, item.item_id);
        assert_eq!(reserved.authority, frozen_authority);
        assert_eq!(reserved.reservation_revision, 2);
        assert!(reserved.dispatch_receipt.is_none());
        assert!(reserved.outcome.is_none());
        assert!(reserved.completion.is_none());

        let reserved_item = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("read reserved item");
        assert_eq!(reserved_item.status, BacklogStatus::Doing);
        assert_eq!(reserved_item.revision, 2);
        assert_eq!(
            reserved_item.current_execution_ref.as_deref(),
            Some(reserved.execution_ref.as_str())
        );

        let entries = history_entries(&core, &binding_id, &item.item_id);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].action, BacklogHistoryAction::WorkerReserved);
        assert_eq!(entries[1].revision, 2);
        assert_eq!(entries[1].item, reserved_item);

        let replayed = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                frozen_authority,
                9_999,
            )
            .expect("replay reservation");
        assert_eq!(replayed, reserved);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after replay"),
            reserved_item
        );
        assert_eq!(history_entries(&core, &binding_id, &item.item_id).len(), 2);
        assert!(core
            .list_runs(&binding_id, None, MAX_PAGE_LIMIT)
            .expect("list runs")
            .runs
            .is_empty());
        assert!(core
            .list_activity(&binding_id, None, None, MAX_PAGE_LIMIT, None, None,)
            .expect("list activity")
            .events
            .is_empty());
        let worker_count: i64 = core
            .conn
            .query_row("SELECT COUNT(*) FROM workers", [], |row| row.get(0))
            .expect("count workers");
        assert_eq!(worker_count, 0);

        drop(core);
        let reopened = Core::open(&db_path).expect("reopen core");
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("read item after reopen"),
            reserved_item
        );
        assert_eq!(
            reopened
                .get_worker_backlog_execution_operator_attested(
                    &binding_id,
                    &item.item_id,
                    &reserved.execution_ref,
                )
                .expect("read execution after reopen"),
            reserved
        );
        let reopened_attempts = attempts(&reopened, &binding_id, &item.item_id);
        assert_eq!(reopened_attempts.len(), 1);
        assert_eq!(reopened_attempts[0].execution_ref, reserved.execution_ref);
        assert!(reopened_attempts[0].outcome.is_none());
        assert!(!reopened_attempts[0].dispatch_attached);
    }

    #[test]
    fn reserve_rejects_changed_payload_stale_revision_and_cross_binding_without_mutation() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_a = create_binding(&mut core, THREAD_A, "binding A");
        let binding_b = create_binding(&mut core, THREAD_B, "binding B");
        let item_a = core
            .create_backlog_item(&binding_a, "create-a", worker_item("Item A"), 1_000)
            .expect("create item A");
        let item_b = core
            .create_backlog_item(&binding_b, "create-b", worker_item("Item B"), 1_000)
            .expect("create item B");
        let auth_a = authority("scope_a");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "reserve",
                &item_a.item_id,
                1,
                auth_a.clone(),
                1_010,
            )
            .expect("reserve item A");

        let before_item = core
            .get_backlog_item(&binding_a, &item_a.item_id)
            .expect("read item A");
        let before_counts = table_counts(&core);
        let before_attempts = attempts(&core, &binding_a, &item_a.item_id);

        assert_idempotency_conflict(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "reserve",
                &item_a.item_id,
                2,
                auth_a.clone(),
                1_020,
            )
            .expect_err("changed payload must conflict"),
        );
        assert_idempotency_conflict(
            core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_a,
                "reserve",
                &item_a.item_id,
                &reserved.execution_ref,
                2,
                receipt("changed-operation"),
                1_030,
            )
            .expect_err("changed operation must conflict"),
        );
        assert_stale_revision(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "stale-reserve",
                &item_a.item_id,
                1,
                authority("stale_scope"),
                1_040,
            )
            .expect_err("stale reservation must fail"),
            1,
            2,
        );
        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_b,
                "cross-binding",
                &item_a.item_id,
                1,
                authority("cross_scope"),
                1_050,
            )
            .expect_err("item A is not scoped to binding B"),
            "item_id",
        );

        assert_eq!(
            core.get_backlog_item(&binding_a, &item_a.item_id)
                .expect("item A unchanged"),
            before_item
        );
        assert_eq!(table_counts(&core), before_counts);
        assert_eq!(
            attempts(&core, &binding_a, &item_a.item_id),
            before_attempts
        );
        assert_eq!(
            core.get_backlog_item(&binding_b, &item_b.item_id)
                .expect("item B unchanged"),
            item_b
        );

        let independent = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_b,
                "reserve",
                &item_b.item_id,
                1,
                authority("scope_b"),
                1_060,
            )
            .expect("same command key in another binding is independent");
        assert_ne!(independent.execution_ref, reserved.execution_ref);
        assert_eq!(
            core.get_backlog_item(&binding_b, &item_b.item_id)
                .expect("item B reserved")
                .status,
            BacklogStatus::Doing
        );
    }

    #[test]
    fn same_approval_task_cannot_reserve_a_second_item_across_bindings() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_a = create_binding(&mut core, THREAD_A, "binding A");
        let binding_b = create_binding(&mut core, THREAD_B, "binding B");
        let item_a = core
            .create_backlog_item(&binding_a, "create-a", worker_item("First item"), 1_000)
            .expect("create item A");
        let item_b = core
            .create_backlog_item(&binding_b, "create-b", worker_item("Second item"), 1_000)
            .expect("create item B");
        let shared_authority = authority("shared_scope");

        core.reserve_worker_backlog_execution_operator_attested(
            &binding_a,
            "reserve-a",
            &item_a.item_id,
            1,
            shared_authority.clone(),
            1_010,
        )
        .expect("reserve first item");

        let before_counts = table_counts(&core);
        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_b,
                "reserve-b",
                &item_b.item_id,
                1,
                shared_authority,
                1_020,
            )
            .expect_err("same concrete task must not attach two items"),
            "authority",
        );
        assert_eq!(
            core.get_backlog_item(&binding_b, &item_b.item_id)
                .expect("second item unchanged"),
            item_b
        );
        assert_eq!(history_entries(&core, &binding_b, &item_b.item_id).len(), 1);
        assert!(attempts(&core, &binding_b, &item_b.item_id).is_empty());
        assert_eq!(table_counts(&core), before_counts);
    }

    #[test]
    fn independent_items_reserve_without_a_global_worker_lock() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "parallel reservations");
        let first = core
            .create_backlog_item(&binding_id, "create-first", worker_item("First"), 1_000)
            .expect("create first");
        let second = core
            .create_backlog_item(&binding_id, "create-second", worker_item("Second"), 1_000)
            .expect("create second");

        let first_execution = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-first",
                &first.item_id,
                1,
                authority("parallel_first"),
                1_010,
            )
            .expect("reserve first");
        let second_execution = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-second",
                &second.item_id,
                1,
                authority("parallel_second"),
                1_020,
            )
            .expect("reserve second without a global Worker cap");

        assert_ne!(
            first_execution.execution_ref,
            second_execution.execution_ref
        );
        assert_eq!(
            core.get_backlog_item(&binding_id, &first.item_id)
                .expect("first item")
                .status,
            BacklogStatus::Doing
        );
        assert_eq!(
            core.get_backlog_item(&binding_id, &second.item_id)
                .expect("second item")
                .status,
            BacklogStatus::Doing
        );
        assert_eq!(attempts(&core, &binding_id, &first.item_id).len(), 1);
        assert_eq!(attempts(&core, &binding_id, &second.item_id).len(), 1);
    }

    #[test]
    fn manually_blocked_item_can_reserve_but_a_reserved_item_cannot_be_replaced() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "blocked reservation");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Blocked item"), 1_000)
            .expect("create item");
        let blocked = core
            .block_backlog_item(
                &binding_id,
                "block",
                &item.item_id,
                1,
                "operator paused the item",
                1_010,
            )
            .expect("block Worker item while Todo");
        assert_eq!(blocked.status, BacklogStatus::Blocked);

        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-from-blocked",
                &item.item_id,
                2,
                authority("blocked_scope"),
                1_020,
            )
            .expect("reserve manually blocked item");
        let reserved_item = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("reserved item");
        assert_eq!(reserved_item.status, BacklogStatus::Doing);
        assert_eq!(reserved_item.revision, 3);
        assert!(reserved_item.blocked_reason.is_none());

        let before_counts = table_counts(&core);
        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "replacement",
                &item.item_id,
                3,
                authority("replacement_scope"),
                1_030,
            )
            .expect_err("a reserved Doing item cannot reserve a replacement"),
            "status",
        );
        assert_validation(
            core.block_backlog_item(
                &binding_id,
                "manual-block",
                &item.item_id,
                3,
                "manual block must not override the reservation",
                1_040,
            )
            .expect_err("manual Worker block cannot override a reservation"),
            "kind",
        );
        assert_eq!(table_counts(&core), before_counts);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("reserved item unchanged"),
            reserved_item
        );
        assert_eq!(
            core.get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("execution still readable"),
            reserved
        );
    }

    #[test]
    fn attach_requires_exact_current_reference_and_one_receipt() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "attach receipt");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Attach receipt"), 1_000)
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("attach_scope"),
                1_010,
            )
            .expect("reserve");

        let before_counts = table_counts(&core);
        assert_validation(
            core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach-wrong-ref",
                &item.item_id,
                "wbe_missing",
                2,
                receipt("wrong_ref"),
                1_020,
            )
            .expect_err("wrong execution reference must fail"),
            "execution_ref",
        );
        assert_eq!(table_counts(&core), before_counts);

        let actual_receipt = receipt("actual");
        let attached = core
            .attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach",
                &item.item_id,
                &reserved.execution_ref,
                2,
                actual_receipt.clone(),
                1_030,
            )
            .expect("attach receipt");
        assert_eq!(attached.dispatch_receipt, Some(actual_receipt.clone()));
        assert_eq!(attached.attached_at_ms, Some(1_030));
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after attach")
                .revision,
            3
        );
        assert_eq!(
            history_entries(&core, &binding_id, &item.item_id)
                .last()
                .expect("history")
                .action,
            BacklogHistoryAction::WorkerExecutionAttached
        );

        let replay = core
            .attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach",
                &item.item_id,
                &reserved.execution_ref,
                2,
                actual_receipt.clone(),
                9_999,
            )
            .expect("replay attach");
        assert_eq!(replay, attached);
        let after_replay_counts = table_counts(&core);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after replay")
                .revision,
            3
        );
        assert_eq!(history_entries(&core, &binding_id, &item.item_id).len(), 3);

        assert_validation(
            core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach-mismatch",
                &item.item_id,
                &reserved.execution_ref,
                3,
                receipt("mismatch"),
                1_040,
            )
            .expect_err("a different receipt must be rejected"),
            "receipt",
        );
        let mut malformed_receipt = receipt("malformed");
        malformed_receipt.execution_binding_digest = Some("C".repeat(64));
        assert_validation(
            core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach-malformed",
                &item.item_id,
                &reserved.execution_ref,
                3,
                malformed_receipt,
                1_050,
            )
            .expect_err("uppercase digest must be rejected"),
            "execution_binding_digest",
        );
        assert_eq!(table_counts(&core), after_replay_counts);
    }

    #[test]
    fn successful_outcome_stays_doing_and_cannot_be_overwritten() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "successful outcome");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Outcome"), 1_000)
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("outcome_scope"),
                1_010,
            )
            .expect("reserve");
        let before_counts = table_counts(&core);

        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "success-without-receipt",
                &item.item_id,
                &reserved.execution_ref,
                2,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "claims success before dispatch receipt".to_string(),
                    result_digest: Some("1".repeat(64)),
                },
                1_020,
            )
            .expect_err("known outcome requires attached receipt"),
            "dispatch_receipt",
        );
        assert_eq!(table_counts(&core), before_counts);

        let attached = core
            .attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach",
                &item.item_id,
                &reserved.execution_ref,
                2,
                receipt("success"),
                1_030,
            )
            .expect("attach receipt");

        let success = core
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "success",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "the bounded checks passed".to_string(),
                    result_digest: Some("1".repeat(64)),
                },
                1_040,
            )
            .expect("record success");
        let recorded = success.outcome.as_ref().expect("recorded outcome");
        assert_eq!(recorded.outcome, WorkerBacklogOutcome::Success);
        assert_eq!(recorded.result_digest, Some("1".repeat(64)));
        assert!(success.completion.is_none());

        let item_after_success = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("item after success");
        assert_eq!(item_after_success.status, BacklogStatus::Doing);
        assert_eq!(item_after_success.revision, 4);
        assert!(item_after_success.blocked_reason.is_none());
        assert!(item_after_success.completion_note.is_none());

        let counts_after_success = table_counts(&core);
        let reserve_replay = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("outcome_scope"),
                9_998,
            )
            .expect("replay original reservation after later transitions");
        assert_eq!(reserve_replay, reserved);
        assert!(reserve_replay.dispatch_receipt.is_none());
        assert!(reserve_replay.outcome.is_none());
        let attach_replay = core
            .attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach",
                &item.item_id,
                &reserved.execution_ref,
                2,
                receipt("success"),
                9_999,
            )
            .expect("replay original attachment after later transitions");
        assert_eq!(attach_replay, attached);
        assert!(attach_replay.outcome.is_none());
        assert_eq!(table_counts(&core), counts_after_success);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after reserve and attach replay"),
            item_after_success
        );

        let before_overwrite = table_counts(&core);
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "contradictory-failure",
                &item.item_id,
                &reserved.execution_ref,
                4,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Failure,
                    note: "contradictory result".to_string(),
                    result_digest: Some("2".repeat(64)),
                },
                1_050,
            )
            .expect_err("successful result cannot be overwritten"),
            "outcome",
        );
        assert_eq!(table_counts(&core), before_overwrite);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item unchanged"),
            item_after_success
        );

        assert_validation(
            core.complete_backlog_item(
                &binding_id,
                "manual-complete",
                &item.item_id,
                4,
                "manual Worker success",
                "manual Worker evidence",
                1_060,
            )
            .expect_err("ordinary completion must reject Worker"),
            "kind",
        );
        assert_validation(
            core.start_backlog_item(&binding_id, "manual-start", &item.item_id, 4, 1_070)
                .expect_err("ordinary start must reject Worker"),
            "kind",
        );
    }

    #[test]
    fn outcome_report_requires_canonical_digest_before_mutation() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "outcome digest validation");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Digest validation"),
                1_000,
            )
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("digest_validation"),
                1_010,
            )
            .expect("reserve");
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_id,
            "attach",
            &item.item_id,
            &reserved.execution_ref,
            2,
            receipt("digest_validation"),
            1_020,
        )
        .expect("attach receipt");
        let before_counts = table_counts(&core);
        let before_item = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("item before rejected reports");
        let before_execution = core
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("execution before rejected reports");

        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "missing-digest",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "known outcome without a digest".to_string(),
                    result_digest: None,
                },
                1_030,
            )
            .expect_err("known outcome without a result digest must fail"),
            "result_digest",
        );
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "uppercase-digest",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Failure,
                    note: "known outcome with an uppercase digest".to_string(),
                    result_digest: Some("A".repeat(64)),
                },
                1_040,
            )
            .expect_err("uppercase result digest must fail"),
            "result_digest",
        );
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "short-digest",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "known outcome with a short digest".to_string(),
                    result_digest: Some("a".repeat(63)),
                },
                1_050,
            )
            .expect_err("short result digest must fail"),
            "result_digest",
        );
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "unknown-with-digest",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Unknown,
                    note: "unknown outcome with a final digest".to_string(),
                    result_digest: Some("a".repeat(64)),
                },
                1_060,
            )
            .expect_err("unknown outcome with a result digest must fail"),
            "result_digest",
        );

        assert_eq!(table_counts(&core), before_counts);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after rejected reports"),
            before_item
        );
        assert_eq!(
            core.get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("execution after rejected reports"),
            before_execution
        );
    }

    #[test]
    fn outcome_command_key_changed_result_digest_conflicts() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "outcome digest idempotency");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Digest idempotency"),
                1_000,
            )
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("digest_idempotency"),
                1_010,
            )
            .expect("reserve");
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_id,
            "attach",
            &item.item_id,
            &reserved.execution_ref,
            2,
            receipt("digest_idempotency"),
            1_020,
        )
        .expect("attach receipt");

        let supplied_digest = "a".repeat(64);
        let success = core
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "bound-outcome",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "verified terminal result".to_string(),
                    result_digest: Some(supplied_digest.clone()),
                },
                1_030,
            )
            .expect("record success");
        assert_eq!(
            success
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(supplied_digest.as_str())
        );

        let after_success = table_counts(&core);
        assert_idempotency_conflict(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "bound-outcome",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "verified terminal result".to_string(),
                    result_digest: Some("b".repeat(64)),
                },
                9_999,
            )
            .expect_err("same command key with a changed result digest must conflict"),
        );
        assert_eq!(table_counts(&core), after_success);
        assert_eq!(
            core.get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("execution after idempotency conflict"),
            success
        );
    }

    #[test]
    fn reconciled_unknown_keeps_upstream_digest_through_completion() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "reconciled digest");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Reconciled digest"),
                1_000,
            )
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("reconciled_digest"),
                1_010,
            )
            .expect("reserve");
        core.record_worker_backlog_outcome_operator_attested(
            &binding_id,
            "unknown",
            &item.item_id,
            &reserved.execution_ref,
            2,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Unknown,
                note: "ambiguous before reconciliation".to_string(),
                result_digest: None,
            },
            1_020,
        )
        .expect("record unknown");
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_id,
            "attach-original",
            &item.item_id,
            &reserved.execution_ref,
            3,
            receipt("reconciled"),
            1_030,
        )
        .expect("attach original receipt");

        drop(core);
        let mut reopened = Core::open(&db_path).expect("reopen before reconciliation");
        let supplied_digest = "e".repeat(64);
        let success = reopened
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "reconcile",
                &item.item_id,
                &reserved.execution_ref,
                4,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "verified after reconciliation".to_string(),
                    result_digest: Some(supplied_digest.clone()),
                },
                1_040,
            )
            .expect("reconcile unknown to success");
        assert_eq!(
            success
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(supplied_digest.as_str())
        );
        assert!(success.completion.is_none());

        drop(reopened);
        let mut reopened = Core::open(&db_path).expect("reopen after reconciliation");
        let persisted = reopened
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("reconciled execution after reopen");
        assert_eq!(
            persisted
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(supplied_digest.as_str())
        );

        let delivery_digest = "d".repeat(64);
        let before_wrong_completion = table_counts(&reopened);
        assert_validation(
            reopened
                .complete_worker_backlog_item_operator_attested(
                    &binding_id,
                    "wrong-result",
                    &item.item_id,
                    5,
                    completion(&reserved.execution_ref, &"f".repeat(64), &delivery_digest),
                    1_050,
                )
                .expect_err("wrong completion result digest must fail"),
            "result_digest",
        );
        assert_eq!(table_counts(&reopened), before_wrong_completion);

        let completed = reopened
            .complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete",
                &item.item_id,
                5,
                completion(&reserved.execution_ref, &supplied_digest, &delivery_digest),
                1_060,
            )
            .expect("complete with the supplied result digest");
        assert_eq!(
            completed
                .execution
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(supplied_digest.as_str())
        );
        assert_eq!(
            completed
                .execution
                .completion
                .as_ref()
                .expect("completion record")
                .result_digest,
            supplied_digest
        );

        drop(reopened);
        let reopened = Core::open(&db_path).expect("reopen after completion");
        let completed_after_reopen = reopened
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &reserved.execution_ref,
            )
            .expect("completed execution after reopen");
        assert_eq!(
            completed_after_reopen
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(supplied_digest.as_str())
        );
        assert_eq!(
            completed_after_reopen
                .completion
                .as_ref()
                .expect("persisted completion record")
                .result_digest,
            supplied_digest
        );
    }

    #[test]
    fn completion_requires_success_exact_evidence_and_survives_reopen() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "completion evidence");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Completion"), 1_000)
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("completion_scope"),
                1_010,
            )
            .expect("reserve");

        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete-without-attachment",
                &item.item_id,
                2,
                completion(&reserved.execution_ref, &"d".repeat(64), &"e".repeat(64)),
                1_020,
            )
            .expect_err("completion before attachment must fail"),
            "dispatch_receipt",
        );

        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_id,
            "attach",
            &item.item_id,
            &reserved.execution_ref,
            2,
            receipt("completion"),
            1_030,
        )
        .expect("attach receipt");

        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete-without-success",
                &item.item_id,
                3,
                completion(&reserved.execution_ref, &"d".repeat(64), &"e".repeat(64)),
                1_040,
            )
            .expect_err("completion without successful outcome must fail"),
            "outcome",
        );

        let success = core
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "success",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "verified success".to_string(),
                    result_digest: Some("c".repeat(64)),
                },
                1_050,
            )
            .expect("record success");
        let result_digest = success
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.result_digest.clone())
            .expect("successful result digest");
        let delivery_digest = "d".repeat(64);

        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "wrong-result",
                &item.item_id,
                4,
                completion(&reserved.execution_ref, &"f".repeat(64), &delivery_digest),
                1_060,
            )
            .expect_err("wrong result digest must fail"),
            "result_digest",
        );
        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "malformed-delivery",
                &item.item_id,
                4,
                completion(&reserved.execution_ref, &result_digest, &"D".repeat(64)),
                1_070,
            )
            .expect_err("malformed delivery digest must fail"),
            "delivery_digest",
        );
        let mut blank_note = completion(&reserved.execution_ref, &result_digest, &delivery_digest);
        blank_note.acceptance_note = "   ".to_string();
        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "blank-note",
                &item.item_id,
                4,
                blank_note,
                1_080,
            )
            .expect_err("blank acceptance note must fail"),
            "acceptance_note",
        );
        let mut blank_evidence =
            completion(&reserved.execution_ref, &result_digest, &delivery_digest);
        blank_evidence.acceptance_evidence = "".to_string();
        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "blank-evidence",
                &item.item_id,
                4,
                blank_evidence,
                1_090,
            )
            .expect_err("blank acceptance evidence must fail"),
            "acceptance_evidence",
        );

        let completed = core
            .complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete",
                &item.item_id,
                4,
                completion(&reserved.execution_ref, &result_digest, &delivery_digest),
                1_100,
            )
            .expect("complete Worker item");
        assert_eq!(completed.item.status, BacklogStatus::Done);
        assert_eq!(completed.item.revision, 5);
        assert!(completed.item.blocked_reason.is_none());
        assert_eq!(
            completed.item.completion_note.as_deref(),
            Some("Coworker accepted the delivered result")
        );
        assert_eq!(completed.item.completed_at_ms, Some(1_100));
        let completion_record = completed
            .execution
            .completion
            .as_ref()
            .expect("completion record");
        assert_eq!(completion_record.result_digest, result_digest);
        assert_eq!(completion_record.delivery_digest, delivery_digest);
        assert_eq!(
            history_entries(&core, &binding_id, &item.item_id)
                .last()
                .expect("history")
                .action,
            BacklogHistoryAction::Completed
        );
        let history_count = history_entries(&core, &binding_id, &item.item_id).len();
        assert_eq!(history_count, 5);
        let after_completion_counts = table_counts(&core);
        let outcome_replay = core
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "success",
                &item.item_id,
                &reserved.execution_ref,
                3,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "verified success".to_string(),
                    result_digest: Some("c".repeat(64)),
                },
                9_998,
            )
            .expect("replay original outcome after completion");
        assert_eq!(outcome_replay, success);
        assert!(outcome_replay.completion.is_none());
        let reserve_replay_after_done = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("completion_scope"),
                9_997,
            )
            .expect("replay original reservation after Done");
        assert_eq!(reserve_replay_after_done, reserved);
        assert_eq!(table_counts(&core), after_completion_counts);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("completed item after outcome and reservation replay"),
            completed.item
        );

        drop(core);
        let mut reopened = Core::open(&db_path).expect("reopen core");
        let replayed = reopened
            .complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete",
                &item.item_id,
                4,
                completion(&reserved.execution_ref, &result_digest, &delivery_digest),
                9_999,
            )
            .expect("replay completion after reopen");
        assert_eq!(replayed, completed);
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("completed item after reopen"),
            completed.item
        );
        assert_eq!(
            history_entries(&reopened, &binding_id, &item.item_id).len(),
            history_count
        );
        assert_eq!(table_counts(&reopened), after_completion_counts);
    }

    #[test]
    fn acceptance_rejection_preserves_success_blocks_item_and_replays_exactly() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "acceptance rejection");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Rejected acceptance"),
                1_000,
            )
            .expect("create Worker item");
        let result_digest = "a".repeat(64);
        let delivery_digest = "b".repeat(64);
        let success = reserve_attach_success(
            &mut core,
            &binding_id,
            &item.item_id,
            "reject_success",
            &result_digest,
        );
        let before_history = history_entries(&core, &binding_id, &item.item_id);

        let request =
            acceptance_rejection(&success.execution_ref, &result_digest, &delivery_digest);
        let rejected = core
            .reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                "reject-acceptance",
                &item.item_id,
                4,
                request.clone(),
                1_040,
            )
            .expect("reject Worker acceptance");

        assert_eq!(rejected.item.status, BacklogStatus::Blocked);
        assert_eq!(rejected.item.revision, 5);
        assert!(rejected.item.completion_note.is_none());
        assert!(rejected.item.completed_at_ms.is_none());
        assert_eq!(
            rejected.item.current_execution_ref.as_deref(),
            Some(success.execution_ref.as_str())
        );
        assert!(rejected
            .item
            .blocked_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.starts_with("worker acceptance rejected: ")
                    && reason.contains(&request.reason)
            }));
        assert_eq!(
            rejected
                .execution
                .outcome
                .as_ref()
                .expect("original success")
                .outcome,
            WorkerBacklogOutcome::Success
        );
        assert_eq!(
            rejected
                .execution
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.result_digest.as_deref()),
            Some(result_digest.as_str())
        );
        let rejection = rejected
            .execution
            .acceptance_rejection
            .as_ref()
            .expect("acceptance rejection record");
        assert_eq!(rejection.result_digest, result_digest);
        assert_eq!(rejection.delivery_digest, delivery_digest);
        assert_eq!(rejection.reason, request.reason);
        assert_eq!(rejection.evidence, request.evidence);
        assert_eq!(rejection.rejected_at_ms, 1_040);
        assert!(rejected.execution.completion.is_none());
        assert!(
            crate::limits::estimate_wire_response_bytes(&rejected)
                .expect("rejection response bytes")
                <= crate::limits::MAX_COMPACT_RESPONSE_BYTES
        );

        let after_rejection_counts = table_counts(&core);
        let after_rejection_history = history_entries(&core, &binding_id, &item.item_id);
        assert_eq!(after_rejection_history.len(), before_history.len() + 1);
        assert_eq!(
            after_rejection_history
                .last()
                .expect("rejection history")
                .action,
            BacklogHistoryAction::Blocked
        );
        assert_eq!(
            after_rejection_history
                .last()
                .expect("rejection history")
                .revision,
            5
        );
        assert_eq!(rejection_count(&core), 1);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("blocked item"),
            rejected.item
        );
        assert_eq!(
            core.get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &success.execution_ref,
            )
            .expect("rejected execution"),
            rejected.execution
        );

        assert!(core
            .list_runs(&binding_id, None, MAX_PAGE_LIMIT)
            .expect("list runs")
            .runs
            .is_empty());
        assert!(core
            .list_activity(&binding_id, None, None, MAX_PAGE_LIMIT, None, None)
            .expect("list activity")
            .events
            .is_empty());
        let worker_count: i64 = core
            .conn
            .query_row("SELECT COUNT(*) FROM workers", [], |row| row.get(0))
            .expect("count workers");
        assert_eq!(worker_count, 0);

        let replay = core
            .reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                "reject-acceptance",
                &item.item_id,
                4,
                request.clone(),
                9_999,
            )
            .expect("replay acceptance rejection");
        assert_eq!(replay, rejected);
        assert_eq!(table_counts(&core), after_rejection_counts);
        assert_eq!(rejection_count(&core), 1);
        assert_eq!(
            history_entries(&core, &binding_id, &item.item_id),
            after_rejection_history
        );

        let mut changed_payload = request.clone();
        changed_payload.reason = "A different failed-acceptance reason".to_string();
        assert_idempotency_conflict(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                "reject-acceptance",
                &item.item_id,
                4,
                changed_payload,
                9_998,
            )
            .expect_err("same command key with changed payload must conflict"),
        );
        assert_stale_revision(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                "stale-after-rejection",
                &item.item_id,
                4,
                request.clone(),
                9_997,
            )
            .expect_err("stale rejection revision must fail after the commit"),
            4,
            5,
        );
        assert_eq!(table_counts(&core), after_rejection_counts);
        assert_eq!(rejection_count(&core), 1);

        let before_reread = table_counts(&core);
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "post-rejection-result-reread",
                &item.item_id,
                &success.execution_ref,
                5,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Success,
                    note: "reread of the original successful result".to_string(),
                    result_digest: Some(result_digest.clone()),
                },
                1_050,
            )
            .expect_err("post-rejection result reread must not mutate"),
            "acceptance_rejection",
        );
        assert_eq!(table_counts(&core), before_reread);
        assert_eq!(rejection_count(&core), 1);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after result reread"),
            rejected.item
        );
        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "post-rejection-completion",
                &item.item_id,
                5,
                completion(&success.execution_ref, &result_digest, &delivery_digest),
                1_060,
            )
            .expect_err("a rejected execution cannot complete"),
            "status",
        );
        assert_validation(
            core.block_backlog_item(
                &binding_id,
                "post-rejection-block",
                &item.item_id,
                5,
                "must not overwrite the rejection reason",
                1_070,
            )
            .expect_err("blocked item cannot be blocked again"),
            "status",
        );

        drop(core);
        let reopened = Core::open(&db_path).expect("reopen core");
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("reopened blocked item"),
            rejected.item
        );
        assert_eq!(
            reopened
                .get_worker_backlog_execution_operator_attested(
                    &binding_id,
                    &item.item_id,
                    &success.execution_ref,
                )
                .expect("reopened rejected execution"),
            rejected.execution
        );
        assert_eq!(rejection_count(&reopened), 1);
    }

    #[test]
    fn acceptance_rejection_rejects_bad_scope_revision_ref_digest_and_non_success() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_a = create_binding(&mut core, THREAD_A, "rejection scope A");
        let binding_b = create_binding(&mut core, THREAD_B, "rejection scope B");
        let item = core
            .create_backlog_item(
                &binding_a,
                "create",
                worker_item("Rejection validation"),
                1_000,
            )
            .expect("create Worker item");
        let result_digest = "a".repeat(64);
        let delivery_digest = "b".repeat(64);
        let success = reserve_attach_success(
            &mut core,
            &binding_a,
            &item.item_id,
            "reject_validation",
            &result_digest,
        );
        let request =
            acceptance_rejection(&success.execution_ref, &result_digest, &delivery_digest);
        let before_counts = table_counts(&core);
        let before_item = core
            .get_backlog_item(&binding_a, &item.item_id)
            .expect("item before rejected requests");
        let before_execution = core
            .get_worker_backlog_execution_operator_attested(
                &binding_a,
                &item.item_id,
                &success.execution_ref,
            )
            .expect("execution before rejected requests");

        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_b,
                "wrong-binding",
                &item.item_id,
                4,
                request.clone(),
                1_040,
            )
            .expect_err("cross-binding rejection must fail"),
            "item_id",
        );
        assert_stale_revision(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "stale-revision",
                &item.item_id,
                3,
                request.clone(),
                1_050,
            )
            .expect_err("stale rejection must fail"),
            3,
            4,
        );
        let mut wrong_ref = request.clone();
        wrong_ref.execution_ref = "wbe_missing".to_string();
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "wrong-ref",
                &item.item_id,
                4,
                wrong_ref,
                1_060,
            )
            .expect_err("wrong execution reference must fail"),
            "execution_ref",
        );
        let mut wrong_result = request.clone();
        wrong_result.result_digest = "c".repeat(64);
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "wrong-result",
                &item.item_id,
                4,
                wrong_result,
                1_070,
            )
            .expect_err("wrong result digest must fail"),
            "result_digest",
        );
        let mut wrong_delivery = request.clone();
        wrong_delivery.delivery_digest = "B".repeat(64);
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "wrong-delivery",
                &item.item_id,
                4,
                wrong_delivery,
                1_080,
            )
            .expect_err("malformed delivery digest must fail"),
            "delivery_digest",
        );
        let mut blank_reason = request.clone();
        blank_reason.reason = "   ".to_string();
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "blank-reason",
                &item.item_id,
                4,
                blank_reason,
                1_090,
            )
            .expect_err("blank rejection reason must fail"),
            "reason",
        );
        let mut oversized_reason = request.clone();
        oversized_reason.reason = "r".repeat(MAX_WORKER_BACKLOG_REJECTION_REASON_LEN + 1);
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "oversized-reason",
                &item.item_id,
                4,
                oversized_reason,
                1_100,
            )
            .expect_err("oversized rejection reason must fail"),
            "reason",
        );
        let mut oversized_evidence = request.clone();
        oversized_evidence.evidence = "e".repeat(MAX_WORKER_BACKLOG_REJECTION_EVIDENCE_LEN + 1);
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "oversized-evidence",
                &item.item_id,
                4,
                oversized_evidence,
                1_110,
            )
            .expect_err("oversized rejection evidence must fail"),
            "evidence",
        );

        assert_eq!(table_counts(&core), before_counts);
        assert_eq!(rejection_count(&core), 0);
        assert_eq!(
            core.get_backlog_item(&binding_a, &item.item_id)
                .expect("item after rejected requests"),
            before_item
        );
        assert_eq!(
            core.get_worker_backlog_execution_operator_attested(
                &binding_a,
                &item.item_id,
                &success.execution_ref,
            )
            .expect("execution after rejected requests"),
            before_execution
        );

        let unstarted_item = core
            .create_backlog_item(
                &binding_a,
                "create-unstarted",
                worker_item("Unstarted rejection"),
                1_200,
            )
            .expect("create unstarted Worker item");
        let unstarted = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "reserve-unstarted",
                &unstarted_item.item_id,
                1,
                authority("reject_unstarted"),
                1_210,
            )
            .expect("reserve without success");
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "reject-unstarted",
                &unstarted_item.item_id,
                2,
                acceptance_rejection(&unstarted.execution_ref, &result_digest, &delivery_digest),
                1_220,
            )
            .expect_err("unstarted execution cannot be rejected"),
            "outcome",
        );

        let failed_item = core
            .create_backlog_item(
                &binding_a,
                "create-failed",
                worker_item("Failed rejection"),
                1_300,
            )
            .expect("create failed Worker item");
        let failed = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "reserve-failed",
                &failed_item.item_id,
                1,
                authority("reject_failed"),
                1_310,
            )
            .expect("reserve failed Worker item");
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_a,
            "attach-failed",
            &failed_item.item_id,
            &failed.execution_ref,
            2,
            receipt("reject_failed"),
            1_320,
        )
        .expect("attach failed receipt");
        core.record_worker_backlog_outcome_operator_attested(
            &binding_a,
            "failure",
            &failed_item.item_id,
            &failed.execution_ref,
            3,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Failure,
                note: "verified failure".to_string(),
                result_digest: Some("d".repeat(64)),
            },
            1_330,
        )
        .expect("record failure");
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "reject-failed",
                &failed_item.item_id,
                4,
                acceptance_rejection(&failed.execution_ref, &result_digest, &delivery_digest),
                1_340,
            )
            .expect_err("failed execution cannot use acceptance rejection"),
            "status",
        );

        let unknown_item = core
            .create_backlog_item(
                &binding_a,
                "create-unknown",
                worker_item("Unknown rejection"),
                1_400,
            )
            .expect("create unknown Worker item");
        let unknown = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_a,
                "reserve-unknown",
                &unknown_item.item_id,
                1,
                authority("reject_unknown"),
                1_410,
            )
            .expect("reserve unknown Worker item");
        core.record_worker_backlog_outcome_operator_attested(
            &binding_a,
            "unknown",
            &unknown_item.item_id,
            &unknown.execution_ref,
            2,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Unknown,
                note: "ambiguous process state".to_string(),
                result_digest: None,
            },
            1_420,
        )
        .expect("record unknown");
        assert_validation(
            core.reject_worker_backlog_acceptance_operator_attested(
                &binding_a,
                "reject-unknown",
                &unknown_item.item_id,
                3,
                acceptance_rejection(&unknown.execution_ref, &result_digest, &delivery_digest),
                1_430,
            )
            .expect_err("unknown execution cannot use acceptance rejection"),
            "status",
        );
    }

    #[test]
    fn acceptance_rejection_allows_only_fresh_authority_correction_and_preserves_history() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "rejection retry");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Rejection retry"), 1_000)
            .expect("create Worker item");
        let old_result_digest = "a".repeat(64);
        let old_delivery_digest = "b".repeat(64);
        let old_success = reserve_attach_success(
            &mut core,
            &binding_id,
            &item.item_id,
            "retry_old",
            &old_result_digest,
        );
        let old_request = acceptance_rejection(
            &old_success.execution_ref,
            &old_result_digest,
            &old_delivery_digest,
        );
        let rejected = core
            .reject_worker_backlog_acceptance_operator_attested(
                &binding_id,
                "reject-old",
                &item.item_id,
                4,
                old_request.clone(),
                1_040,
            )
            .expect("reject old acceptance");
        let rejected_item = rejected.item.clone();

        let before_old_authority = table_counts(&core);
        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reuse-old-authority",
                &item.item_id,
                rejected_item.revision,
                authority("retry_old"),
                1_050,
            )
            .expect_err("a rejected approval/task pair must not be reused"),
            "authority",
        );
        assert_eq!(table_counts(&core), before_old_authority);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("item after refused old authority"),
            rejected_item
        );

        let replacement = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-correction",
                &item.item_id,
                rejected_item.revision,
                authority("retry_fresh"),
                1_060,
            )
            .expect("a genuine rejection permits a fresh authority");
        assert_ne!(replacement.execution_ref, old_success.execution_ref);
        let replacement_item = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("replacement item");
        assert_eq!(replacement_item.status, BacklogStatus::Doing);
        assert_eq!(
            replacement_item.current_execution_ref.as_deref(),
            Some(replacement.execution_ref.as_str())
        );
        assert!(replacement_item.blocked_reason.is_none());
        let old_execution_after_retry = core
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &old_success.execution_ref,
            )
            .expect("old execution after retry");
        assert_eq!(
            old_execution_after_retry
                .outcome
                .as_ref()
                .expect("old success")
                .outcome,
            WorkerBacklogOutcome::Success
        );
        assert_eq!(
            old_execution_after_retry.acceptance_rejection.as_ref(),
            rejected.execution.acceptance_rejection.as_ref()
        );
        assert!(old_execution_after_retry.completion.is_none());
        let attempt_identities = attempts(&core, &binding_id, &item.item_id);
        assert_eq!(attempt_identities.len(), 2);
        assert_eq!(
            attempt_identities[0].execution_ref,
            old_success.execution_ref
        );
        assert_eq!(
            attempt_identities[1].execution_ref,
            replacement.execution_ref
        );
        let retry_history = history_entries(&core, &binding_id, &item.item_id);
        assert!(retry_history.iter().any(|entry| {
            entry.item.current_execution_ref.as_deref() == Some(old_success.execution_ref.as_str())
                && entry.item.blocked_reason.is_some()
        }));

        let new_result_digest = "d".repeat(64);
        let new_delivery_digest = "e".repeat(64);
        core.attach_worker_backlog_dispatch_receipt_operator_attested(
            &binding_id,
            "attach-correction",
            &item.item_id,
            &replacement.execution_ref,
            6,
            receipt("retry_fresh"),
            1_070,
        )
        .expect("attach correction receipt");
        core.record_worker_backlog_outcome_operator_attested(
            &binding_id,
            "success-correction",
            &item.item_id,
            &replacement.execution_ref,
            7,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified corrected technical success".to_string(),
                result_digest: Some(new_result_digest.clone()),
            },
            1_080,
        )
        .expect("record corrected success");
        let completed = core
            .complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete-correction",
                &item.item_id,
                8,
                completion(
                    &replacement.execution_ref,
                    &new_result_digest,
                    &new_delivery_digest,
                ),
                1_090,
            )
            .expect("complete corrected attempt");
        assert_eq!(completed.item.status, BacklogStatus::Done);
        assert_eq!(completed.item.revision, 9);
        assert!(completed.item.blocked_reason.is_none());
        assert_eq!(
            completed
                .execution
                .completion
                .as_ref()
                .expect("corrected completion")
                .result_digest,
            new_result_digest
        );
        let old_execution_after_done = core
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &old_success.execution_ref,
            )
            .expect("old execution after corrected completion");
        assert_eq!(
            old_execution_after_done.acceptance_rejection.as_ref(),
            rejected.execution.acceptance_rejection.as_ref()
        );
        assert!(old_execution_after_done.completion.is_none());
        let final_history = history_entries(&core, &binding_id, &item.item_id);
        assert!(final_history
            .iter()
            .any(|entry| entry.action == BacklogHistoryAction::Blocked));
        assert_eq!(
            final_history.last().expect("final history").action,
            BacklogHistoryAction::Completed
        );

        drop(core);
        let reopened = Core::open(&db_path).expect("reopen corrected store");
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("reopened completed item"),
            completed.item
        );
        let reopened_old = reopened
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &old_success.execution_ref,
            )
            .expect("reopened old rejected execution");
        assert_eq!(
            reopened_old.acceptance_rejection.as_ref(),
            rejected.execution.acceptance_rejection.as_ref()
        );
        assert!(reopened_old.completion.is_none());
    }

    #[test]
    fn legacy_execution_without_rejection_data_reads_none_after_additive_schema_open() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "legacy rejection compatibility");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Legacy rejection compatibility"),
                1_000,
            )
            .expect("create Worker item");
        let result_digest = "a".repeat(64);
        let success = reserve_attach_success(
            &mut core,
            &binding_id,
            &item.item_id,
            "legacy_rejection",
            &result_digest,
        );
        assert!(success.acceptance_rejection.is_none());

        drop(core);
        {
            // Simulate an older store that predates the additive rejection table.
            let conn = rusqlite::Connection::open(&db_path).expect("open raw legacy database");
            conn.execute("DROP TABLE backlog_worker_acceptance_rejections", [])
                .expect("drop additive rejection table");
        }

        let mut reopened = Core::open(&db_path).expect("reopen legacy store");
        let loaded = reopened
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &success.execution_ref,
            )
            .expect("legacy execution read");
        assert!(loaded.acceptance_rejection.is_none());
        assert_eq!(loaded, success);
        let mut legacy_value = serde_json::to_value(&loaded).expect("serialize current execution");
        let removed_rejection = legacy_value
            .as_object_mut()
            .expect("execution object")
            .remove("acceptance_rejection");
        assert!(removed_rejection.is_none());
        let decoded: WorkerBacklogExecution =
            serde_json::from_value(legacy_value).expect("deserialize legacy execution");
        assert!(decoded.acceptance_rejection.is_none());
        assert_eq!(decoded, loaded);
        assert_eq!(rejection_count(&reopened), 0);
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("legacy success item")
                .status,
            BacklogStatus::Doing
        );
        assert_validation(
            reopened
                .reserve_worker_backlog_execution_operator_attested(
                    &binding_id,
                    "legacy-success-is-not-a-rejection",
                    &item.item_id,
                    4,
                    authority("legacy_replacement"),
                    1_040,
                )
                .expect_err("missing legacy rejection data must never infer rejection"),
            "status",
        );
    }

    #[test]
    fn reopened_rejection_must_match_the_original_success_result() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "rejection correlation");
        let item = core
            .create_backlog_item(
                &binding_id,
                "create",
                worker_item("Rejection correlation"),
                1_000,
            )
            .expect("create Worker item");
        let result_digest = "a".repeat(64);
        let success = reserve_attach_success(
            &mut core,
            &binding_id,
            &item.item_id,
            "reject_correlation",
            &result_digest,
        );
        let request = acceptance_rejection(&success.execution_ref, &result_digest, &"b".repeat(64));
        core.reject_worker_backlog_acceptance_operator_attested(
            &binding_id,
            "reject-correlation",
            &item.item_id,
            4,
            request,
            1_040,
        )
        .expect("reject acceptance");

        drop(core);
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open raw database");
            conn.execute(
                "UPDATE backlog_worker_acceptance_rejections
                 SET result_digest = ?1
                 WHERE execution_ref = ?2",
                rusqlite::params!["f".repeat(64), success.execution_ref],
            )
            .expect("tamper rejection result digest");
        }

        let reopened = Core::open(&db_path).expect("reopen corrupted rejection store");
        let err = reopened
            .get_worker_backlog_execution_operator_attested(
                &binding_id,
                &item.item_id,
                &success.execution_ref,
            )
            .expect_err("reopened rejection correlation mismatch must fail closed");
        assert!(matches!(err, CoreError::CorruptState(_)));
    }

    #[test]
    fn unknown_blocks_new_reservation_and_failure_reconciles_before_replacement() {
        let (_dir, db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "unknown reconciliation");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Unknown attempt"), 1_000)
            .expect("create item");
        let old_authority = authority("old_scope");
        let old_execution = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-old",
                &item.item_id,
                1,
                old_authority.clone(),
                1_010,
            )
            .expect("reserve original attempt");

        let unknown = core
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "unknown",
                &item.item_id,
                &old_execution.execution_ref,
                2,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Unknown,
                    note: "process state is ambiguous".to_string(),
                    result_digest: None,
                },
                1_020,
            )
            .expect("record unknown without a receipt");
        let unknown_outcome = unknown.outcome.as_ref().expect("unknown outcome");
        assert_eq!(unknown_outcome.outcome, WorkerBacklogOutcome::Unknown);
        assert!(unknown_outcome.result_digest.is_none());
        assert!(unknown.dispatch_receipt.is_none());
        let blocked = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("blocked item");
        assert_eq!(blocked.status, BacklogStatus::Blocked);
        assert_eq!(blocked.revision, 3);
        assert!(blocked
            .blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("worker unknown:")));

        let blocked_counts = table_counts(&core);
        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "replacement-while-unknown",
                &item.item_id,
                3,
                authority("premature_replacement"),
                1_030,
            )
            .expect_err("unknown outcome must not permit a new reservation"),
            "status",
        );
        assert_eq!(table_counts(&core), blocked_counts);

        drop(core);
        let mut reopened = Core::open(&db_path).expect("reopen core");
        assert_validation(
            reopened
                .reserve_worker_backlog_execution_operator_attested(
                    &binding_id,
                    "replacement-after-reopen",
                    &item.item_id,
                    3,
                    authority("still_premature"),
                    1_040,
                )
                .expect_err("reopened unknown must still block replacement"),
            "status",
        );

        let attached = reopened
            .attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach-original",
                &item.item_id,
                &old_execution.execution_ref,
                3,
                receipt("original"),
                1_050,
            )
            .expect("attach receipt to the original invocation");
        assert_eq!(attached.dispatch_receipt, Some(receipt("original")));
        assert_eq!(
            reopened
                .get_backlog_item(&binding_id, &item.item_id)
                .expect("attached unknown item")
                .status,
            BacklogStatus::Blocked
        );
        assert_validation(
            reopened
                .record_worker_backlog_outcome_operator_attested(
                    &binding_id,
                    "second-unknown",
                    &item.item_id,
                    &old_execution.execution_ref,
                    4,
                    WorkerBacklogOutcomeReport {
                        outcome: WorkerBacklogOutcome::Unknown,
                        note: "duplicate unknown".to_string(),
                        result_digest: None,
                    },
                    1_060,
                )
                .expect_err("unknown cannot be recorded twice"),
            "outcome",
        );

        let failure = reopened
            .record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "failure",
                &item.item_id,
                &old_execution.execution_ref,
                4,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Failure,
                    note: "verified failure".to_string(),
                    result_digest: Some("3".repeat(64)),
                },
                1_070,
            )
            .expect("reconcile unknown to failure");
        let failure_outcome = failure.outcome.as_ref().expect("failed outcome");
        assert_eq!(failure_outcome.outcome, WorkerBacklogOutcome::Failure);
        assert_eq!(failure_outcome.result_digest, Some("3".repeat(64)));
        let blocked_after_failure = reopened
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("blocked after failure");
        assert_eq!(blocked_after_failure.status, BacklogStatus::Blocked);
        assert_eq!(blocked_after_failure.revision, 5);

        let before_reused_authority = table_counts(&reopened);
        assert_validation(
            reopened
                .reserve_worker_backlog_execution_operator_attested(
                    &binding_id,
                    "reuse-old-authority",
                    &item.item_id,
                    5,
                    old_authority,
                    1_080,
                )
                .expect_err("the failed approval/task pair must not be reused"),
            "authority",
        );
        assert_eq!(table_counts(&reopened), before_reused_authority);

        let replacement = reopened
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve-replacement",
                &item.item_id,
                5,
                authority("fresh_scope"),
                1_090,
            )
            .expect("known failure permits a distinct fresh reservation");
        assert_ne!(replacement.execution_ref, old_execution.execution_ref);
        let replacement_item = reopened
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("replacement item");
        assert_eq!(replacement_item.status, BacklogStatus::Doing);
        assert_eq!(
            replacement_item.current_execution_ref.as_deref(),
            Some(replacement.execution_ref.as_str())
        );
        assert_eq!(
            reopened
                .get_worker_backlog_execution_operator_attested(
                    &binding_id,
                    &item.item_id,
                    &old_execution.execution_ref,
                )
                .expect("old execution preserved")
                .outcome
                .as_ref()
                .expect("old outcome")
                .outcome,
            WorkerBacklogOutcome::Failure
        );
        let attempt_identities = attempts(&reopened, &binding_id, &item.item_id);
        assert_eq!(attempt_identities.len(), 2);
        assert_eq!(
            attempt_identities[0].execution_ref,
            old_execution.execution_ref
        );
        assert_eq!(
            attempt_identities[1].execution_ref,
            replacement.execution_ref
        );
        let history = history_entries(&reopened, &binding_id, &item.item_id);
        assert!(history.iter().any(|entry| {
            entry.item.current_execution_ref.as_deref()
                == Some(old_execution.execution_ref.as_str())
        }));
        assert_eq!(
            history
                .last()
                .expect("replacement history")
                .item
                .current_execution_ref
                .as_deref(),
            Some(replacement.execution_ref.as_str())
        );
        let first_page = reopened
            .list_worker_backlog_attempts_operator_attested(&binding_id, &item.item_id, None, 1)
            .expect("first bounded attempt page");
        assert_eq!(first_page.attempts.len(), 1);
        assert!(first_page.has_more);
        assert_eq!(
            first_page.attempts[0].execution_ref,
            old_execution.execution_ref
        );
        let second_page = reopened
            .list_worker_backlog_attempts_operator_attested(
                &binding_id,
                &item.item_id,
                first_page.next_cursor.as_deref(),
                1,
            )
            .expect("second bounded attempt page");
        assert_eq!(second_page.attempts.len(), 1);
        assert!(!second_page.has_more);
        assert!(second_page.next_cursor.is_none());
        assert_eq!(
            second_page.attempts[0].execution_ref,
            replacement.execution_ref
        );

        assert_validation(
            reopened
                .complete_worker_backlog_item_operator_attested(
                    &binding_id,
                    "old-result-completion",
                    &item.item_id,
                    6,
                    completion(
                        &old_execution.execution_ref,
                        &"d".repeat(64),
                        &"e".repeat(64),
                    ),
                    1_100,
                )
                .expect_err("an old attempt cannot finish its replacement"),
            "execution_ref",
        );
    }

    #[test]
    fn reserved_worker_conditions_are_frozen_even_when_blocked() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "frozen conditions");
        let item = core
            .create_backlog_item(&binding_id, "create", worker_item("Frozen"), 1_000)
            .expect("create item");
        let reserved = core
            .reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                1,
                authority("frozen_scope"),
                1_010,
            )
            .expect("reserve");

        assert_validation(
            core.edit_backlog_item(
                &binding_id,
                "edit-acceptance",
                &item.item_id,
                2,
                BacklogItemEdit {
                    acceptance_condition: Some("changed acceptance".to_string()),
                    ..BacklogItemEdit::default()
                },
                1_020,
            )
            .expect_err("acceptance must stay frozen"),
            "acceptance_condition",
        );
        assert_validation(
            core.edit_backlog_item(
                &binding_id,
                "edit-delivery",
                &item.item_id,
                2,
                BacklogItemEdit {
                    delivery_description: Some("changed delivery".to_string()),
                    ..BacklogItemEdit::default()
                },
                1_030,
            )
            .expect_err("delivery must stay frozen"),
            "delivery_description",
        );
        let title_clarified = core
            .edit_backlog_item(
                &binding_id,
                "clarify-title",
                &item.item_id,
                2,
                BacklogItemEdit {
                    title: Some("Frozen conditions clarified".to_string()),
                    ..BacklogItemEdit::default()
                },
                1_040,
            )
            .expect("title clarification while Doing is allowed");
        assert_eq!(title_clarified.revision, 3);
        assert_eq!(title_clarified.title, "Frozen conditions clarified");

        core.record_worker_backlog_outcome_operator_attested(
            &binding_id,
            "unknown",
            &item.item_id,
            &reserved.execution_ref,
            3,
            WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Unknown,
                note: "ambiguous after title clarification".to_string(),
                result_digest: None,
            },
            1_050,
        )
        .expect("block item with unknown outcome");
        let blocked = core
            .get_backlog_item(&binding_id, &item.item_id)
            .expect("blocked item");
        assert_eq!(blocked.status, BacklogStatus::Blocked);
        let before_edit = table_counts(&core);
        assert_validation(
            core.edit_backlog_item(
                &binding_id,
                "edit-blocked-acceptance",
                &item.item_id,
                4,
                BacklogItemEdit {
                    acceptance_condition: Some("blocked change".to_string()),
                    ..BacklogItemEdit::default()
                },
                1_060,
            )
            .expect_err("blocked Worker conditions must stay frozen"),
            "status",
        );
        assert_validation(
            core.edit_backlog_item(
                &binding_id,
                "edit-blocked-title",
                &item.item_id,
                4,
                BacklogItemEdit {
                    title: Some("must not apply".to_string()),
                    ..BacklogItemEdit::default()
                },
                1_070,
            )
            .expect_err("ordinary edits of blocked items stay rejected"),
            "status",
        );
        assert_eq!(table_counts(&core), before_edit);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("blocked item unchanged"),
            blocked
        );
    }

    #[test]
    fn coworker_items_cannot_use_operator_worker_apis() {
        let (_dir, _db_path, mut core) = open_temp_core();
        let binding_id = create_binding(&mut core, THREAD_A, "Coworker isolation");
        let item = core
            .create_backlog_item(&binding_id, "create", coworker_item("Coworker work"), 1_000)
            .expect("create Coworker item");
        let started = core
            .start_backlog_item(&binding_id, "start", &item.item_id, 1, 1_010)
            .expect("start Coworker item");
        assert_eq!(started.status, BacklogStatus::Doing);
        let fake_ref = "wbe_coworker";
        let before_counts = table_counts(&core);

        assert_validation(
            core.reserve_worker_backlog_execution_operator_attested(
                &binding_id,
                "reserve",
                &item.item_id,
                2,
                authority("coworker_scope"),
                1_020,
            )
            .expect_err("Coworker cannot reserve Worker execution"),
            "kind",
        );
        assert_validation(
            core.attach_worker_backlog_dispatch_receipt_operator_attested(
                &binding_id,
                "attach",
                &item.item_id,
                fake_ref,
                2,
                receipt("coworker"),
                1_030,
            )
            .expect_err("Coworker cannot attach a Worker receipt"),
            "kind",
        );
        assert_validation(
            core.record_worker_backlog_outcome_operator_attested(
                &binding_id,
                "outcome",
                &item.item_id,
                fake_ref,
                2,
                WorkerBacklogOutcomeReport {
                    outcome: WorkerBacklogOutcome::Unknown,
                    note: "fake Coworker Worker outcome".to_string(),
                    result_digest: None,
                },
                1_040,
            )
            .expect_err("Coworker cannot record a Worker outcome"),
            "kind",
        );
        assert_validation(
            core.complete_worker_backlog_item_operator_attested(
                &binding_id,
                "complete",
                &item.item_id,
                2,
                completion(fake_ref, &"d".repeat(64), &"e".repeat(64)),
                1_050,
            )
            .expect_err("Coworker cannot complete a Worker execution"),
            "kind",
        );
        assert_eq!(table_counts(&core), before_counts);
        assert_eq!(
            core.get_backlog_item(&binding_id, &item.item_id)
                .expect("Coworker item unchanged"),
            started
        );
        assert_eq!(history_entries(&core, &binding_id, &item.item_id).len(), 2);
    }
}
