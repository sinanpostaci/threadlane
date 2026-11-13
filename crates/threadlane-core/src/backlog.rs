//! Durable per-chat backlog storage for the minimal chat-centered MVP.
//!
//! The backlog is deliberately independent of runs, workers, generic events, and
//! approval ledgers. A backlog item records intent, delivery conditions, and a
//! bounded change history; it never fabricates a `Run` or `Job`. Actual Coworker
//! and Worker liveness stays outside this module.
//!
//! This foundation slice implements creation, reads, bounded listing, ordinary
//! edits, blocking, and Coworker-only start/completion attestation. Worker
//! items can be created, edited while `Todo`, and blocked, but guarded Worker
//! reservation and verified terminal completion belong to the next slice.
//!
//! Schema initialization is additive and transactional through
//! [`crate::Core::open`]. The existing schema version remains unchanged because
//! the new tables are created idempotently without altering existing tables,
//! recovery behavior, bindings, runs, workers, events, or approvals.
//!
//! Revision conflicts reuse the existing [`CoreError::StaleRevision`] shape; its
//! legacy `run_id` field carries the backlog item identifier for these
//! operations so callers can keep one conflict mapping.

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::engine::{generate_id, hash_dedup_payload, i64_to_u64, u64_to_i64, Core};
use crate::error::CoreError;
use crate::limits::{
    estimate_wire_response_bytes, validate_bounded_text, validate_command_key, validate_identifier,
    validate_page_limit, validate_timestamp_ms, MAX_COMPACT_RESPONSE_BYTES, MAX_CURSOR_LEN,
    MAX_PAGE_RESPONSE_BYTES,
};

/// Maximum byte length for a backlog item title.
pub const MAX_BACKLOG_TITLE_LEN: usize = 256;

/// Maximum byte length for a backlog acceptance condition.
pub const MAX_BACKLOG_ACCEPTANCE_LEN: usize = 2_048;

/// Maximum byte length for a backlog delivery description.
pub const MAX_BACKLOG_DELIVERY_LEN: usize = 2_048;

/// Maximum byte length for a backlog blocked reason.
pub const MAX_BACKLOG_BLOCKED_REASON_LEN: usize = 1_024;

/// Maximum byte length for a Coworker completion note.
pub const MAX_BACKLOG_COMPLETION_NOTE_LEN: usize = 2_048;

/// Maximum byte length for a Coworker completion evidence attestation.
pub const MAX_BACKLOG_COMPLETION_EVIDENCE_LEN: usize = 2_048;

/// Maximum byte length for a future Worker execution reference.
pub const MAX_BACKLOG_EXECUTION_REF_LEN: usize = 512;

const BACKLOG_CURSOR_VERSION: &str = "tlb1";
const BACKLOG_ITEMS_CURSOR_KIND: &str = "items";
const BACKLOG_HISTORY_CURSOR_KIND: &str = "history";
const CREATE_OPERATION: &str = "backlog_create";
const EDIT_OPERATION: &str = "backlog_edit";
const BLOCK_OPERATION: &str = "backlog_block";
const START_OPERATION: &str = "backlog_start";
const COMPLETE_OPERATION: &str = "backlog_complete";

/// Whether a backlog item is Coworker-attested conversational work or intended
/// for the existing single-task Worker execution route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BacklogItemKind {
    /// Work intended for the guarded Worker reservation/completion path.
    ///
    /// In this foundation slice Worker items can be created, edited while
    /// `Todo`, and blocked only. They cannot be started or completed manually.
    Worker,
    /// Work the native Codex Coworker performs in the human conversation.
    ///
    /// Completion is an explicit attestation recorded with bounded evidence,
    /// not proof that a Worker process ran.
    Coworker,
}

impl BacklogItemKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Coworker => "coworker",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "worker" => Some(Self::Worker),
            "coworker" => Some(Self::Coworker),
            _ => None,
        }
    }
}

/// Committed backlog state, intentionally separate from live Coworker/Worker
/// liveness and from the `NeedsYou` attention derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BacklogStatus {
    /// Not started.
    Todo,
    /// Coworker is actively working, or a future Worker reservation holds it.
    Doing,
    /// Accepted delivery was durably recorded.
    Done,
    /// Explicitly blocked with a reason.
    Blocked,
}

impl BacklogStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Todo => "todo",
            Self::Doing => "doing",
            Self::Done => "done",
            Self::Blocked => "blocked",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "todo" => Some(Self::Todo),
            "doing" => Some(Self::Doing),
            "done" => Some(Self::Done),
            "blocked" => Some(Self::Blocked),
            _ => None,
        }
    }
}

/// Classifies one durable backlog history entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BacklogHistoryAction {
    /// The item was created in `Todo`.
    Created,
    /// Ordinary item content was edited.
    Edited,
    /// The item moved to `Doing`.
    Started,
    /// The item moved to `Blocked`.
    Blocked,
    /// The item moved to `Done` with completion evidence.
    Completed,
}

impl BacklogHistoryAction {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Edited => "edited",
            Self::Started => "started",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "edited" => Some(Self::Edited),
            "started" => Some(Self::Started),
            "blocked" => Some(Self::Blocked),
            "completed" => Some(Self::Completed),
            _ => None,
        }
    }
}

/// Input for creating one backlog item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewBacklogItem {
    /// Bounded human-readable title.
    pub title: String,
    /// Bounded condition that must be checked before `Done`.
    pub acceptance_condition: String,
    /// Bounded description of the intended delivery.
    pub delivery_description: String,
    /// Coworker-attested or future Worker-backed work.
    pub kind: BacklogItemKind,
}

/// Revision-checked ordinary content edit.
///
/// The request deliberately has no `kind`, `status`, completion, or execution
/// fields. It cannot mark an item `Done`, bypass a guarded Worker completion, or
/// convert a Worker item to Coworker.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacklogItemEdit {
    /// Replacement title when `Some`.
    pub title: Option<String>,
    /// Replacement acceptance condition when `Some`.
    pub acceptance_condition: Option<String>,
    /// Replacement delivery description when `Some`.
    pub delivery_description: Option<String>,
}

/// Current durable snapshot of one backlog item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacklogItem {
    /// Software-generated item identifier (`blg_<uuid>`).
    pub item_id: String,
    /// Owning binding/chat identifier.
    pub binding_id: String,
    /// Monotonically increasing 1-based mutation revision.
    pub revision: u64,
    /// Bounded human-readable title.
    pub title: String,
    /// Bounded condition that must be checked before `Done`.
    pub acceptance_condition: String,
    /// Bounded description of the intended delivery.
    pub delivery_description: String,
    /// Coworker-attested or future Worker-backed work.
    pub kind: BacklogItemKind,
    /// Current committed backlog state.
    pub status: BacklogStatus,
    /// Bounded reason when `status == BacklogStatus::Blocked`.
    pub blocked_reason: Option<String>,
    /// Reserved for a future Worker reservation.
    ///
    /// Remains `None` throughout this foundation slice. Coworker completion
    /// never attaches a Worker invocation here.
    pub current_execution_ref: Option<String>,
    /// Bounded Coworker attestation note recorded on completion.
    pub completion_note: Option<String>,
    /// Bounded Coworker evidence attestation recorded on completion.
    pub completion_evidence: Option<String>,
    /// Timestamp (ms) when completion was durably recorded.
    pub completed_at_ms: Option<i64>,
    /// Timestamp (ms) when the item was created.
    pub created_at_ms: i64,
    /// Timestamp (ms) when the item revision was last mutated.
    pub updated_at_ms: i64,
}

/// Bounded creation-order page of backlog items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacklogItemPage {
    /// Binding/chat that owns every item in this page.
    pub binding_id: String,
    /// Items in stable creation order within the page.
    pub items: Vec<BacklogItem>,
    /// Opaque continuation cursor when `has_more` is true.
    pub next_cursor: Option<String>,
    /// Whether more items remain inside this pagination window.
    pub has_more: bool,
    /// Creation-order high-water mark anchoring this pagination window.
    pub high_water_mark_rowid: u64,
}

/// One immutable backlog history entry containing the item snapshot produced by
/// the mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacklogHistoryEntry {
    /// Binding/chat that owns the item.
    pub binding_id: String,
    /// Item that was mutated.
    pub item_id: String,
    /// Resulting item revision for this history entry.
    pub revision: u64,
    /// Mutation classification.
    pub action: BacklogHistoryAction,
    /// Full item snapshot after the mutation.
    pub item: BacklogItem,
    /// Timestamp (ms) when the history entry was written.
    pub recorded_at_ms: i64,
}

/// Bounded stable-order page of backlog history entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacklogHistoryPage {
    /// Binding/chat that owns the item.
    pub binding_id: String,
    /// Item whose history is being read.
    pub item_id: String,
    /// History entries in stable write order within the page.
    pub entries: Vec<BacklogHistoryEntry>,
    /// Opaque continuation cursor when `has_more` is true.
    pub next_cursor: Option<String>,
    /// Whether more history entries remain inside this pagination window.
    pub has_more: bool,
    /// Write-order high-water mark anchoring this pagination window.
    pub high_water_mark_rowid: u64,
}

type BacklogItemRow = (
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    i64,
    i64,
);

type BacklogHistoryRow = (i64, i64, String, String, i64);

impl Core {
    /// Creates one backlog item in `Todo` without creating a run, job, worker,
    /// event, wake-up, approval, or human request.
    ///
    /// The item, its initial history entry, and the binding-scoped idempotency
    /// record commit atomically. Binding validation happens before idempotency
    /// replay. An exact retry returns the original item snapshot without adding
    /// a revision or history entry; reusing the same command key with a changed
    /// payload returns [`CoreError::IdempotencyConflict`].
    pub fn create_backlog_item(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item: NewBacklogItem,
        now_ms: i64,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let NewBacklogItem {
            title,
            acceptance_condition,
            delivery_description,
            kind,
        } = item;
        let clean_title = validate_bounded_text("title", &title, MAX_BACKLOG_TITLE_LEN)?;
        let clean_acceptance = validate_bounded_text(
            "acceptance_condition",
            &acceptance_condition,
            MAX_BACKLOG_ACCEPTANCE_LEN,
        )?;
        let clean_delivery = validate_bounded_text(
            "delivery_description",
            &delivery_description,
            MAX_BACKLOG_DELIVERY_LEN,
        )?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            CREATE_OPERATION,
            &clean_title,
            &clean_acceptance,
            &clean_delivery,
            kind,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            CREATE_OPERATION,
            &payload_hash,
            None,
        )? {
            return Ok(existing);
        }

        let backlog_item = BacklogItem {
            item_id: generate_id("blg"),
            binding_id: clean_binding_id.clone(),
            revision: 1,
            title: clean_title,
            acceptance_condition: clean_acceptance,
            delivery_description: clean_delivery,
            kind,
            status: BacklogStatus::Todo,
            blocked_reason: None,
            current_execution_ref: None,
            completion_note: None,
            completion_evidence: None,
            completed_at_ms: None,
            created_at_ms: valid_now,
            updated_at_ms: valid_now,
        };
        validate_backlog_item(&backlog_item, &clean_binding_id)?;
        insert_backlog_item_tx(&tx, &backlog_item)?;
        record_backlog_history_tx(&tx, &backlog_item, BacklogHistoryAction::Created, valid_now)?;
        record_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            CREATE_OPERATION,
            &payload_hash,
            &backlog_item,
            valid_now,
        )?;
        tx.commit()?;
        Ok(backlog_item)
    }

    /// Reads one item scoped to `binding_id`.
    ///
    /// An item that exists only under another binding is never returned.
    pub fn get_backlog_item(
        &self,
        binding_id: &str,
        item_id: &str,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        load_backlog_item_conn(&self.conn, &clean_binding_id, &clean_item_id)
    }

    /// Lists one bounded, keyset-paginated page of backlog items for a binding.
    ///
    /// The first page anchors a creation-order high-water mark. New items created
    /// after that anchor do not shift or duplicate continuation pages.
    pub fn list_backlog_items(
        &self,
        binding_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<BacklogItemPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        list_backlog_items_conn(&self.conn, &clean_binding_id, cursor, valid_limit)
    }

    /// Applies a revision-checked ordinary content edit.
    ///
    /// Editing is allowed in `Todo`. In `Doing`, only the title may be clarified;
    /// the acceptance condition and delivery description are immutable. `Blocked`
    /// and `Done` items reject ordinary edits. Worker items are only editable
    /// while `Todo`. This method cannot change kind, status, completion, or
    /// execution references.
    pub fn edit_backlog_item(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        edit: BacklogItemEdit,
        now_ms: i64,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let BacklogItemEdit {
            title,
            acceptance_condition,
            delivery_description,
        } = edit;
        let clean_title = title
            .as_deref()
            .map(|value| validate_bounded_text("title", value, MAX_BACKLOG_TITLE_LEN))
            .transpose()?;
        let clean_acceptance = acceptance_condition
            .as_deref()
            .map(|value| {
                validate_bounded_text("acceptance_condition", value, MAX_BACKLOG_ACCEPTANCE_LEN)
            })
            .transpose()?;
        let clean_delivery = delivery_description
            .as_deref()
            .map(|value| {
                validate_bounded_text("delivery_description", value, MAX_BACKLOG_DELIVERY_LEN)
            })
            .transpose()?;
        if clean_title.is_none() && clean_acceptance.is_none() && clean_delivery.is_none() {
            return Err(CoreError::Validation {
                field: "edit",
                reason: "edit must change at least one of title, acceptance_condition, or delivery_description"
                    .to_string(),
            });
        }
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            EDIT_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_title,
            &clean_acceptance,
            &clean_delivery,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            EDIT_OPERATION,
            &payload_hash,
            Some(clean_item_id.as_str()),
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
        match backlog_item.status {
            BacklogStatus::Todo => {}
            BacklogStatus::Doing => {
                if clean_acceptance.is_some() {
                    return Err(CoreError::Validation {
                        field: "acceptance_condition",
                        reason: "acceptance_condition cannot change while the item is Doing"
                            .to_string(),
                    });
                }
                if clean_delivery.is_some() {
                    return Err(CoreError::Validation {
                        field: "delivery_description",
                        reason: "delivery_description cannot change while the item is Doing"
                            .to_string(),
                    });
                }
            }
            BacklogStatus::Blocked | BacklogStatus::Done => {
                return Err(invalid_backlog_transition(&backlog_item, "edit"));
            }
        }
        if backlog_item.kind == BacklogItemKind::Worker
            && backlog_item.status != BacklogStatus::Todo
        {
            return Err(worker_manual_transition(&backlog_item, "edited"));
        }
        if let Some(title) = clean_title {
            backlog_item.title = title;
        }
        if let Some(acceptance_condition) = clean_acceptance {
            backlog_item.acceptance_condition = acceptance_condition;
        }
        if let Some(delivery_description) = clean_delivery {
            backlog_item.delivery_description = delivery_description;
        }
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        record_backlog_history_tx(&tx, &backlog_item, BacklogHistoryAction::Edited, valid_now)?;
        record_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            EDIT_OPERATION,
            &payload_hash,
            &backlog_item,
            valid_now,
        )?;
        tx.commit()?;
        Ok(backlog_item)
    }

    /// Blocks a `Todo` or `Doing` item with a bounded reason.
    ///
    /// Worker items may be blocked while `Todo`; they cannot enter `Doing` in
    /// this slice, so Worker blocking never implies a running process.
    pub fn block_backlog_item(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        reason: &str,
        now_ms: i64,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let clean_reason = validate_bounded_text("reason", reason, MAX_BACKLOG_BLOCKED_REASON_LEN)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            BLOCK_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_reason,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            BLOCK_OPERATION,
            &payload_hash,
            Some(clean_item_id.as_str()),
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
        match backlog_item.status {
            BacklogStatus::Todo | BacklogStatus::Doing => {}
            BacklogStatus::Blocked | BacklogStatus::Done => {
                return Err(invalid_backlog_transition(&backlog_item, "block"));
            }
        }
        if backlog_item.kind == BacklogItemKind::Worker
            && backlog_item.status != BacklogStatus::Todo
        {
            return Err(worker_manual_transition(&backlog_item, "blocked"));
        }
        backlog_item.status = BacklogStatus::Blocked;
        backlog_item.blocked_reason = Some(clean_reason);
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        record_backlog_history_tx(&tx, &backlog_item, BacklogHistoryAction::Blocked, valid_now)?;
        record_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            BLOCK_OPERATION,
            &payload_hash,
            &backlog_item,
            valid_now,
        )?;
        tx.commit()?;
        Ok(backlog_item)
    }

    /// Starts a Coworker item from `Todo` or `Blocked` with a revision check.
    ///
    /// Worker items are rejected: guarded Worker reservation is not part of this
    /// foundation slice. Starting clears a prior blocked reason and never
    /// attaches a Worker execution reference.
    pub fn start_backlog_item(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        now_ms: i64,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash =
            hash_dedup_payload(&(START_OPERATION, &clean_item_id, expected_revision))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            START_OPERATION,
            &payload_hash,
            Some(clean_item_id.as_str()),
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
        if backlog_item.kind != BacklogItemKind::Coworker {
            return Err(worker_manual_transition(&backlog_item, "started"));
        }
        match backlog_item.status {
            BacklogStatus::Todo | BacklogStatus::Blocked => {}
            BacklogStatus::Doing | BacklogStatus::Done => {
                return Err(invalid_backlog_transition(&backlog_item, "start"));
            }
        }
        backlog_item.status = BacklogStatus::Doing;
        backlog_item.blocked_reason = None;
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        record_backlog_history_tx(&tx, &backlog_item, BacklogHistoryAction::Started, valid_now)?;
        record_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            START_OPERATION,
            &payload_hash,
            &backlog_item,
            valid_now,
        )?;
        tx.commit()?;
        Ok(backlog_item)
    }

    /// Completes a Coworker item from `Doing` with bounded delivery note and
    /// evidence.
    ///
    /// This is an explicit attestation that the Coworker checked the acceptance
    /// condition and delivered the described result. It is not proof that a real
    /// Worker ran, does not accept a caller-provided success flag, and never
    /// attaches a Worker invocation. Completion is rejected from `Todo`,
    /// `Blocked`, or `Done`, and completed evidence is immutable.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_backlog_item(
        &mut self,
        binding_id: &str,
        command_key: &str,
        item_id: &str,
        expected_revision: u64,
        note: &str,
        evidence: &str,
        now_ms: i64,
    ) -> Result<BacklogItem, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_key = validate_command_key(command_key)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        validate_expected_revision(expected_revision)?;
        let clean_note = validate_bounded_text("note", note, MAX_BACKLOG_COMPLETION_NOTE_LEN)?;
        let clean_evidence =
            validate_bounded_text("evidence", evidence, MAX_BACKLOG_COMPLETION_EVIDENCE_LEN)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let payload_hash = hash_dedup_payload(&(
            COMPLETE_OPERATION,
            &clean_item_id,
            expected_revision,
            &clean_note,
            &clean_evidence,
        ))?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        if let Some(existing) = check_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            COMPLETE_OPERATION,
            &payload_hash,
            Some(clean_item_id.as_str()),
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
        if backlog_item.kind != BacklogItemKind::Coworker {
            return Err(worker_manual_transition(&backlog_item, "completed"));
        }
        if backlog_item.status != BacklogStatus::Doing {
            return Err(invalid_backlog_transition(&backlog_item, "complete"));
        }
        if backlog_item.current_execution_ref.is_some() {
            return Err(CoreError::Validation {
                field: "current_execution_ref",
                reason: "Coworker completion must not attach a Worker execution reference"
                    .to_string(),
            });
        }
        backlog_item.status = BacklogStatus::Done;
        backlog_item.blocked_reason = None;
        backlog_item.completion_note = Some(clean_note);
        backlog_item.completion_evidence = Some(clean_evidence);
        backlog_item.completed_at_ms = Some(valid_now);
        let previous_revision = backlog_item.revision;
        backlog_item.revision = next_backlog_revision(&backlog_item)?;
        backlog_item.updated_at_ms = valid_now;
        validate_backlog_item(&backlog_item, &clean_binding_id)?;
        update_backlog_item_tx(&tx, &backlog_item, previous_revision)?;
        record_backlog_history_tx(
            &tx,
            &backlog_item,
            BacklogHistoryAction::Completed,
            valid_now,
        )?;
        record_backlog_dedup(
            &tx,
            &clean_binding_id,
            &clean_key,
            COMPLETE_OPERATION,
            &payload_hash,
            &backlog_item,
            valid_now,
        )?;
        tx.commit()?;
        Ok(backlog_item)
    }

    /// Lists one bounded, stable-order page of an item's backlog history.
    ///
    /// History entries are independent of generic events and runs. The first
    /// page anchors a write-order high-water mark so later mutations do not shift
    /// continuation pages.
    pub fn list_backlog_history(
        &self,
        binding_id: &str,
        item_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<BacklogHistoryPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_item_id = validate_backlog_item_id(item_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        list_backlog_history_conn(
            &self.conn,
            &clean_binding_id,
            &clean_item_id,
            cursor,
            valid_limit,
        )
    }
}

fn validate_backlog_item_id(raw: &str) -> Result<String, CoreError> {
    let clean = validate_identifier("item_id", raw)?;
    if clean.contains(':') {
        return Err(CoreError::Validation {
            field: "item_id",
            reason: "item_id contains invalid characters".to_string(),
        });
    }
    Ok(clean)
}

fn validate_expected_revision(expected_revision: u64) -> Result<(), CoreError> {
    if expected_revision == 0 {
        return Err(CoreError::Validation {
            field: "expected_revision",
            reason: "expected_revision must be >= 1".to_string(),
        });
    }
    Ok(())
}

fn next_backlog_revision(item: &BacklogItem) -> Result<u64, CoreError> {
    item.revision.checked_add(1).ok_or_else(|| {
        CoreError::CorruptState(format!(
            "backlog item `{}` revision overflowed u64",
            item.item_id
        ))
    })
}

fn backlog_item_not_found(binding_id: &str, item_id: &str) -> CoreError {
    CoreError::Validation {
        field: "item_id",
        reason: format!("backlog item `{item_id}` not found for binding `{binding_id}`"),
    }
}

fn invalid_backlog_transition(item: &BacklogItem, action: &'static str) -> CoreError {
    CoreError::Validation {
        field: "status",
        reason: format!(
            "cannot perform `{action}` on backlog item `{}` while in status `{}`",
            item.item_id,
            item.status.as_str()
        ),
    }
}

fn worker_manual_transition(item: &BacklogItem, action: &'static str) -> CoreError {
    CoreError::Validation {
        field: "kind",
        reason: format!(
            "Worker backlog item `{}` cannot be `{action}` manually; guarded Worker reservation and verified terminal completion are not implemented in this foundation slice",
            item.item_id
        ),
    }
}

fn validate_backlog_item(item: &BacklogItem, expected_binding_id: &str) -> Result<(), CoreError> {
    if item.binding_id != expected_binding_id {
        return Err(CoreError::CorruptState(format!(
            "backlog item `{}` belongs to binding `{}` instead of requested binding `{expected_binding_id}`",
            item.item_id, item.binding_id
        )));
    }
    validate_backlog_item_id(&item.item_id)?;
    if item.revision == 0 {
        return Err(CoreError::CorruptState(format!(
            "backlog item `{}` has invalid revision 0",
            item.item_id
        )));
    }
    validate_bounded_text("title", &item.title, MAX_BACKLOG_TITLE_LEN)?;
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
    match item.status {
        BacklogStatus::Blocked => {
            let reason = item.blocked_reason.as_deref().ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "blocked backlog item `{}` has no blocked reason",
                    item.item_id
                ))
            })?;
            validate_bounded_text("blocked_reason", reason, MAX_BACKLOG_BLOCKED_REASON_LEN)?;
        }
        BacklogStatus::Todo | BacklogStatus::Doing | BacklogStatus::Done => {
            if item.blocked_reason.is_some() {
                return Err(CoreError::CorruptState(format!(
                    "non-blocked backlog item `{}` carries a blocked reason",
                    item.item_id
                )));
            }
        }
    }
    match item.status {
        BacklogStatus::Done => {
            let note = item.completion_note.as_deref().ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "done backlog item `{}` has no completion note",
                    item.item_id
                ))
            })?;
            let evidence = item.completion_evidence.as_deref().ok_or_else(|| {
                CoreError::CorruptState(format!(
                    "done backlog item `{}` has no completion evidence",
                    item.item_id
                ))
            })?;
            if item.completed_at_ms.is_none() {
                return Err(CoreError::CorruptState(format!(
                    "done backlog item `{}` has no completed_at_ms",
                    item.item_id
                )));
            }
            validate_bounded_text("completion_note", note, MAX_BACKLOG_COMPLETION_NOTE_LEN)?;
            validate_bounded_text(
                "completion_evidence",
                evidence,
                MAX_BACKLOG_COMPLETION_EVIDENCE_LEN,
            )?;
        }
        BacklogStatus::Todo | BacklogStatus::Doing | BacklogStatus::Blocked => {
            if item.completion_note.is_some()
                || item.completion_evidence.is_some()
                || item.completed_at_ms.is_some()
            {
                return Err(CoreError::CorruptState(format!(
                    "unfinished backlog item `{}` carries completion data",
                    item.item_id
                )));
            }
        }
    }
    if let Some(execution_ref) = item.current_execution_ref.as_deref() {
        validate_bounded_text(
            "current_execution_ref",
            execution_ref,
            MAX_BACKLOG_EXECUTION_REF_LEN,
        )?;
    }
    Ok(())
}

fn load_backlog_item_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
) -> Result<BacklogItem, CoreError> {
    let row: Option<BacklogItemRow> = conn
        .query_row(
            "SELECT item_id, binding_id, revision, title, acceptance_condition,
                    delivery_description, kind, status, blocked_reason,
                    current_execution_ref, completion_note, completion_evidence,
                    completed_at_ms, created_at_ms, updated_at_ms
             FROM backlog_items
             WHERE binding_id = ?1 AND item_id = ?2",
            params![binding_id, item_id],
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
                ))
            },
        )
        .optional()?;

    let Some((
        stored_item_id,
        stored_binding_id,
        revision,
        title,
        acceptance_condition,
        delivery_description,
        kind_raw,
        status_raw,
        blocked_reason,
        current_execution_ref,
        completion_note,
        completion_evidence,
        completed_at_ms,
        created_at_ms,
        updated_at_ms,
    )) = row
    else {
        return Err(backlog_item_not_found(binding_id, item_id));
    };

    let kind = BacklogItemKind::from_str(&kind_raw)
        .ok_or_else(|| CoreError::CorruptState(format!("invalid backlog kind `{kind_raw}`")))?;
    let status = BacklogStatus::from_str(&status_raw)
        .ok_or_else(|| CoreError::CorruptState(format!("invalid backlog status `{status_raw}`")))?;
    let item = BacklogItem {
        item_id: stored_item_id,
        binding_id: stored_binding_id,
        revision: i64_to_u64("revision", revision)?,
        title,
        acceptance_condition,
        delivery_description,
        kind,
        status,
        blocked_reason,
        current_execution_ref,
        completion_note,
        completion_evidence,
        completed_at_ms,
        created_at_ms,
        updated_at_ms,
    };
    validate_backlog_item(&item, binding_id)?;
    Ok(item)
}

fn insert_backlog_item_tx(tx: &Transaction<'_>, item: &BacklogItem) -> Result<(), CoreError> {
    tx.execute(
        "INSERT INTO backlog_items (
            item_id, binding_id, revision, title, acceptance_condition,
            delivery_description, kind, status, blocked_reason,
            current_execution_ref, completion_note, completion_evidence,
            completed_at_ms, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            item.item_id,
            item.binding_id,
            u64_to_i64("revision", item.revision)?,
            item.title,
            item.acceptance_condition,
            item.delivery_description,
            item.kind.as_str(),
            item.status.as_str(),
            item.blocked_reason.as_deref(),
            item.current_execution_ref.as_deref(),
            item.completion_note.as_deref(),
            item.completion_evidence.as_deref(),
            item.completed_at_ms,
            item.created_at_ms,
            item.updated_at_ms,
        ],
    )?;
    Ok(())
}

fn update_backlog_item_tx(
    tx: &Transaction<'_>,
    item: &BacklogItem,
    expected_revision: u64,
) -> Result<(), CoreError> {
    let updated = tx.execute(
        "UPDATE backlog_items
         SET revision = ?1,
             title = ?2,
             acceptance_condition = ?3,
             delivery_description = ?4,
             kind = ?5,
             status = ?6,
             blocked_reason = ?7,
             current_execution_ref = ?8,
             completion_note = ?9,
             completion_evidence = ?10,
             completed_at_ms = ?11,
             updated_at_ms = ?12
         WHERE binding_id = ?13 AND item_id = ?14 AND revision = ?15",
        params![
            u64_to_i64("revision", item.revision)?,
            item.title,
            item.acceptance_condition,
            item.delivery_description,
            item.kind.as_str(),
            item.status.as_str(),
            item.blocked_reason.as_deref(),
            item.current_execution_ref.as_deref(),
            item.completion_note.as_deref(),
            item.completion_evidence.as_deref(),
            item.completed_at_ms,
            item.updated_at_ms,
            item.binding_id,
            item.item_id,
            u64_to_i64("expected_revision", expected_revision)?,
        ],
    )?;
    if updated != 1 {
        return Err(CoreError::CorruptState(format!(
            "backlog item `{}` revision changed during an exclusive mutation",
            item.item_id
        )));
    }
    Ok(())
}

fn record_backlog_history_tx(
    tx: &Transaction<'_>,
    item: &BacklogItem,
    action: BacklogHistoryAction,
    now_ms: i64,
) -> Result<(), CoreError> {
    let item_json = serialize_backlog_item(item)?;
    tx.execute(
        "INSERT INTO backlog_history (
            history_id, binding_id, item_id, revision, action, item_json, recorded_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            generate_id("blh"),
            item.binding_id,
            item.item_id,
            u64_to_i64("revision", item.revision)?,
            action.as_str(),
            item_json,
            now_ms,
        ],
    )?;
    Ok(())
}

fn check_backlog_dedup(
    tx: &Transaction<'_>,
    binding_id: &str,
    command_key: &str,
    operation: &str,
    payload_hash: &str,
    expected_item_id: Option<&str>,
) -> Result<Option<BacklogItem>, CoreError> {
    let existing: Option<(String, String, String)> = tx
        .query_row(
            "SELECT operation, payload_hash, response_json
             FROM backlog_command_dedup
             WHERE binding_id = ?1 AND command_key = ?2",
            params![binding_id, command_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    let Some((stored_operation, stored_hash, response_json)) = existing else {
        return Ok(None);
    };
    if stored_operation != operation || stored_hash != payload_hash {
        return Err(CoreError::IdempotencyConflict {
            binding_id: binding_id.to_string(),
            command_key: command_key.to_string(),
        });
    }
    let item: BacklogItem = serde_json::from_str(&response_json)?;
    validate_backlog_item(&item, binding_id)?;
    if let Some(expected_item_id) = expected_item_id {
        if item.item_id != expected_item_id {
            return Err(CoreError::CorruptState(format!(
                "idempotency record for command `{command_key}` on binding `{binding_id}` returned item `{}` instead of `{expected_item_id}`",
                item.item_id
            )));
        }
    }
    let size_bytes = estimate_wire_response_bytes(&item)?;
    if size_bytes > MAX_COMPACT_RESPONSE_BYTES {
        return Err(CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes: MAX_COMPACT_RESPONSE_BYTES,
        });
    }
    Ok(Some(item))
}

fn record_backlog_dedup(
    tx: &Transaction<'_>,
    binding_id: &str,
    command_key: &str,
    operation: &str,
    payload_hash: &str,
    item: &BacklogItem,
    now_ms: i64,
) -> Result<(), CoreError> {
    let response_json = serialize_backlog_item(item)?;
    tx.execute(
        "INSERT INTO backlog_command_dedup (
            binding_id, command_key, operation, payload_hash, response_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            binding_id,
            command_key,
            operation,
            payload_hash,
            response_json,
            now_ms,
        ],
    )?;
    Ok(())
}

fn serialize_backlog_item(item: &BacklogItem) -> Result<String, CoreError> {
    let json = serde_json::to_string(item)?;
    let size_bytes = estimate_wire_response_bytes(item)?;
    if size_bytes > MAX_COMPACT_RESPONSE_BYTES {
        return Err(CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes: MAX_COMPACT_RESPONSE_BYTES,
        });
    }
    Ok(json)
}

fn list_backlog_items_conn(
    conn: &Connection,
    binding_id: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<BacklogItemPage, CoreError> {
    let (after_rowid, high_water_mark_rowid) = match cursor {
        Some(raw_cursor) => {
            let (after, high_water_mark) = parse_backlog_items_cursor(raw_cursor, binding_id)?;
            let anchor_count: i64 = conn.query_row(
                "SELECT COUNT(*)
                 FROM backlog_items
                 WHERE binding_id = ?1 AND rowid IN (?2, ?3)",
                params![
                    binding_id,
                    u64_to_i64("cursor", after)?,
                    u64_to_i64("cursor", high_water_mark)?,
                ],
                |row| row.get(0),
            )?;
            if anchor_count != 2 {
                return Err(CoreError::Validation {
                    field: "cursor",
                    reason: "cursor references unknown or out-of-range backlog item position for this binding"
                        .to_string(),
                });
            }
            (after, high_water_mark)
        }
        None => {
            let max_rowid: Option<i64> = conn.query_row(
                "SELECT MAX(rowid) FROM backlog_items WHERE binding_id = ?1",
                params![binding_id],
                |row| row.get(0),
            )?;
            let Some(max_raw) = max_rowid else {
                return Ok(BacklogItemPage {
                    binding_id: binding_id.to_string(),
                    items: Vec::new(),
                    next_cursor: None,
                    has_more: false,
                    high_water_mark_rowid: 0,
                });
            };
            (0, i64_to_u64("high_water_mark_rowid", max_raw)?)
        }
    };

    let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
    let candidate_items: Vec<(u64, String)> = {
        let mut stmt = conn.prepare(
            "SELECT rowid, item_id
             FROM backlog_items
             WHERE binding_id = ?1 AND rowid > ?2 AND rowid <= ?3
             ORDER BY rowid ASC
             LIMIT ?4",
        )?;
        let mut rows = stmt.query(params![
            binding_id,
            u64_to_i64("after_rowid", after_rowid)?,
            u64_to_i64("high_water_mark_rowid", high_water_mark_rowid)?,
            fetch_limit,
        ])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push((i64_to_u64("rowid", row.get(0)?)?, row.get(1)?));
        }
        items
    };

    let db_has_more = candidate_items.len() > limit;
    let mut included_items = Vec::with_capacity(candidate_items.len().min(limit));
    let mut last_included_rowid = after_rowid;
    let mut stopped_by_budget = false;

    for (rowid, item_id) in candidate_items.into_iter().take(limit) {
        let item = load_backlog_item_conn(conn, binding_id, &item_id)?;
        included_items.push(item);

        let probe_output = BacklogItemPage {
            binding_id: binding_id.to_string(),
            items: included_items.clone(),
            next_cursor: Some(encode_backlog_items_cursor(
                binding_id,
                rowid,
                high_water_mark_rowid,
            )),
            has_more: true,
            high_water_mark_rowid,
        };
        let size_bytes = estimate_wire_response_bytes(&probe_output)?;
        if size_bytes > MAX_PAGE_RESPONSE_BYTES {
            if included_items.len() == 1 {
                return Err(CoreError::ResponseTooLarge {
                    size_bytes,
                    max_bytes: MAX_PAGE_RESPONSE_BYTES,
                });
            }
            included_items.pop();
            stopped_by_budget = true;
            break;
        }
        last_included_rowid = rowid;
    }

    let has_more = stopped_by_budget || db_has_more;
    let next_cursor = if has_more {
        Some(encode_backlog_items_cursor(
            binding_id,
            last_included_rowid,
            high_water_mark_rowid,
        ))
    } else {
        None
    };

    Ok(BacklogItemPage {
        binding_id: binding_id.to_string(),
        items: included_items,
        next_cursor,
        has_more,
        high_water_mark_rowid,
    })
}

fn list_backlog_history_conn(
    conn: &Connection,
    binding_id: &str,
    item_id: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<BacklogHistoryPage, CoreError> {
    load_backlog_item_conn(conn, binding_id, item_id)?;

    let (after_rowid, high_water_mark_rowid) = match cursor {
        Some(raw_cursor) => {
            let (after, high_water_mark) =
                parse_backlog_history_cursor(raw_cursor, binding_id, item_id)?;
            let anchor_count: i64 = conn.query_row(
                "SELECT COUNT(*)
                 FROM backlog_history
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
                    reason: "cursor references unknown or out-of-range backlog history position for this item"
                        .to_string(),
                });
            }
            (after, high_water_mark)
        }
        None => {
            let max_rowid: Option<i64> = conn.query_row(
                "SELECT MAX(rowid)
                 FROM backlog_history
                 WHERE binding_id = ?1 AND item_id = ?2",
                params![binding_id, item_id],
                |row| row.get(0),
            )?;
            let Some(max_raw) = max_rowid else {
                return Ok(BacklogHistoryPage {
                    binding_id: binding_id.to_string(),
                    item_id: item_id.to_string(),
                    entries: Vec::new(),
                    next_cursor: None,
                    has_more: false,
                    high_water_mark_rowid: 0,
                });
            };
            (0, i64_to_u64("high_water_mark_rowid", max_raw)?)
        }
    };

    let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
    let candidate_entries: Vec<BacklogHistoryRow> = {
        let mut stmt = conn.prepare(
            "SELECT rowid, revision, action, item_json, recorded_at_ms
             FROM backlog_history
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
        let mut entries = Vec::new();
        while let Some(row) = rows.next()? {
            entries.push((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ));
        }
        entries
    };

    let db_has_more = candidate_entries.len() > limit;
    let mut included_entries = Vec::with_capacity(candidate_entries.len().min(limit));
    let mut last_included_rowid = after_rowid;
    let mut stopped_by_budget = false;

    for (rowid, revision, action_raw, item_json, recorded_at_ms) in
        candidate_entries.into_iter().take(limit)
    {
        let history_rowid = i64_to_u64("rowid", rowid)?;
        let action = BacklogHistoryAction::from_str(&action_raw).ok_or_else(|| {
            CoreError::CorruptState(format!("invalid backlog history action `{action_raw}`"))
        })?;
        let snapshot: BacklogItem = serde_json::from_str(&item_json)?;
        validate_backlog_item(&snapshot, binding_id)?;
        if snapshot.item_id != item_id {
            return Err(CoreError::CorruptState(format!(
                "backlog history for item `{item_id}` contains snapshot for `{}`",
                snapshot.item_id
            )));
        }
        let entry_revision = i64_to_u64("revision", revision)?;
        if snapshot.revision != entry_revision {
            return Err(CoreError::CorruptState(format!(
                "backlog history revision {entry_revision} does not match snapshot revision {}",
                snapshot.revision
            )));
        }
        included_entries.push(BacklogHistoryEntry {
            binding_id: binding_id.to_string(),
            item_id: item_id.to_string(),
            revision: entry_revision,
            action,
            item: snapshot,
            recorded_at_ms,
        });

        let probe_output = BacklogHistoryPage {
            binding_id: binding_id.to_string(),
            item_id: item_id.to_string(),
            entries: included_entries.clone(),
            next_cursor: Some(encode_backlog_history_cursor(
                binding_id,
                item_id,
                history_rowid,
                high_water_mark_rowid,
            )),
            has_more: true,
            high_water_mark_rowid,
        };
        let size_bytes = estimate_wire_response_bytes(&probe_output)?;
        if size_bytes > MAX_PAGE_RESPONSE_BYTES {
            if included_entries.len() == 1 {
                return Err(CoreError::ResponseTooLarge {
                    size_bytes,
                    max_bytes: MAX_PAGE_RESPONSE_BYTES,
                });
            }
            included_entries.pop();
            stopped_by_budget = true;
            break;
        }
        last_included_rowid = history_rowid;
    }

    let has_more = stopped_by_budget || db_has_more;
    let next_cursor = if has_more {
        Some(encode_backlog_history_cursor(
            binding_id,
            item_id,
            last_included_rowid,
            high_water_mark_rowid,
        ))
    } else {
        None
    };

    Ok(BacklogHistoryPage {
        binding_id: binding_id.to_string(),
        item_id: item_id.to_string(),
        entries: included_entries,
        next_cursor,
        has_more,
        high_water_mark_rowid,
    })
}

fn encode_backlog_items_cursor(binding_id: &str, after_rowid: u64, hwm_rowid: u64) -> String {
    format!(
        "{BACKLOG_CURSOR_VERSION}:{BACKLOG_ITEMS_CURSOR_KIND}:{binding_id}:{after_rowid}:{hwm_rowid}"
    )
}

fn encode_backlog_history_cursor(
    binding_id: &str,
    item_id: &str,
    after_rowid: u64,
    hwm_rowid: u64,
) -> String {
    format!(
        "{BACKLOG_CURSOR_VERSION}:{BACKLOG_HISTORY_CURSOR_KIND}:{binding_id}:{item_id}:{after_rowid}:{hwm_rowid}"
    )
}

fn parse_backlog_items_cursor(
    raw: &str,
    expected_binding_id: &str,
) -> Result<(u64, u64), CoreError> {
    validate_backlog_cursor_text(raw)?;
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 5
        || parts[0] != BACKLOG_CURSOR_VERSION
        || parts[1] != BACKLOG_ITEMS_CURSOR_KIND
    {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "malformed backlog items cursor structure".to_string(),
        });
    }
    if parts[2] != expected_binding_id {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor binding_id does not match request scope".to_string(),
        });
    }
    let after = parse_backlog_cursor_position("position", parts[3])?;
    let high_water_mark = parse_backlog_cursor_position("high_water_mark", parts[4])?;
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

fn parse_backlog_history_cursor(
    raw: &str,
    expected_binding_id: &str,
    expected_item_id: &str,
) -> Result<(u64, u64), CoreError> {
    validate_backlog_cursor_text(raw)?;
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 6
        || parts[0] != BACKLOG_CURSOR_VERSION
        || parts[1] != BACKLOG_HISTORY_CURSOR_KIND
    {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "malformed backlog history cursor structure".to_string(),
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

fn validate_backlog_cursor_text(raw: &str) -> Result<(), CoreError> {
    if raw.is_empty() {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not be empty".to_string(),
        });
    }
    if raw.len() > MAX_CURSOR_LEN {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor exceeds maximum length of {MAX_CURSOR_LEN} bytes"),
        });
    }
    if raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: "cursor must not contain control characters or surrounding whitespace"
                .to_string(),
        });
    }
    Ok(())
}

fn parse_backlog_cursor_position(label: &str, raw: &str) -> Result<u64, CoreError> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) || raw.starts_with('0') {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor {label} must be a canonical positive integer"),
        });
    }
    let value = raw.parse::<u64>().map_err(|_| CoreError::Validation {
        field: "cursor",
        reason: format!("cursor {label} overflows 64-bit integer range"),
    })?;
    if value == 0 || value > i64::MAX as u64 {
        return Err(CoreError::Validation {
            field: "cursor",
            reason: format!("cursor {label} exceeds valid signed 64-bit integer key range"),
        });
    }
    Ok(value)
}
