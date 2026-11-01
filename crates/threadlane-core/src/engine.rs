use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{AckErrorReason, CoreError};
use crate::limits::{
    encode_activity_cursor, encode_questions_cursor, encode_revisions_cursor, encode_runs_cursor,
    estimate_wire_response_bytes, parse_and_validate_activity_cursor, parse_and_validate_cursor,
    truncate_preview, validate_activity_run_id, validate_batch_byte_budget, validate_batch_limit,
    validate_bounded_text, validate_command_key, validate_identifier, validate_label,
    validate_optional_text, validate_page_limit, validate_step_delay_ms, validate_thread_uuid,
    validate_timestamp_ms, CursorKind, FIXED_WAKEUP_MESSAGE, LEGACY_SCHEMA_VERSION_V1,
    LEGACY_SCHEMA_VERSION_V2, LEGACY_SCHEMA_VERSION_V3, MAX_ANSWER_LEN, MAX_CANCEL_REASON_LEN,
    MAX_COMPACT_RESPONSE_BYTES, MAX_EVENT_PAYLOAD_BYTES, MAX_INSTRUCTION_LEN,
    MAX_PAGE_RESPONSE_BYTES, MAX_WAKEUP_ERROR_LEN, RESPONSE_WRAPPER_OVERHEAD_BYTES, SCHEMA_VERSION,
    WAKEUP_NOTICE_STATUS_MESSAGE,
};
use crate::types::{
    AckResult, ActivityPage, Binding, Command, CommandOutput, Event, EventBatch, EventKind,
    EventPayload, Question, QuestionHistoryPage, QuestionStatus, RevisionHistoryPage, Run,
    RunListPage, RunRevision, RunStatus, RunSummary, RuntimeIdleInspection, TickReport,
    WakeupClaim, WakeupNotice, WakeupOutcome, WakeupRetryRecord, WakeupStatus,
};
use crate::worker::WorkerSnapshot;

/// Transactional, persistent SQLite domain engine for Threadlane.
///
/// Owned by a single supervisor process; all mutating operations execute inside atomic
/// SQLite transactions.
#[derive(Debug)]
pub struct Core {
    pub(crate) conn: Connection,
}

impl Core {
    /// Opens or initializes the Threadlane SQLite state database at `database_path`.
    ///
    /// Verifies the schema version and atomically recovers any `in_flight` wake-up claim
    /// left behind by a prior supervisor crash or restart to `WakeupStatus::Unknown`
    /// without automatic retry.
    pub fn open(database_path: impl AsRef<Path>) -> Result<Self, CoreError> {
        let mut conn = Connection::open(database_path.as_ref())?;
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;",
        )?;

        {
            let tx = conn.transaction()?;
            Self::init_schema_and_recover(&tx)?;
            tx.commit()?;
        }

        Ok(Self { conn })
    }

    /// Creates a new trusted binding for a validated native Codex `thread_uuid` and human-readable `label`.
    ///
    /// Generates a fresh software `binding_id` (`bnd_<uuid>`) and initializes its isolated
    /// cursor and wake-up state.
    pub fn create_binding(
        &mut self,
        validated_thread_uuid: &str,
        label: &str,
    ) -> Result<Binding, CoreError> {
        let thread_uuid = validate_thread_uuid(validated_thread_uuid)?;
        let clean_label = validate_label(label)?;
        let binding_id = generate_id("bnd");

        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO bindings (binding_id, thread_uuid, label, delivered_seq, acked_seq, next_event_seq)
             VALUES (?1, ?2, ?3, 0, 0, 1)",
            params![binding_id, thread_uuid, clean_label],
        )?;
        tx.execute(
            "INSERT INTO wakeups (
                binding_id, thread_uuid, status, claim_id, pending_since_seq,
                latest_important_seq, covered_up_to_seq, coalesced_count, attempt_count,
                last_error, claimed_at_ms, resolved_at_ms
             ) VALUES (?1, ?2, ?3, NULL, NULL, 0, 0, 0, 0, NULL, NULL, NULL)",
            params![binding_id, thread_uuid, WakeupStatus::Idle.as_str()],
        )?;
        let binding = Self::load_binding_tx(&tx, &binding_id)?;
        tx.commit()?;
        Ok(binding)
    }

    /// Fetches a configured [`Binding`] by its `binding_id`.
    pub fn get_binding(&self, binding_id: &str) -> Result<Binding, CoreError> {
        let clean_id = validate_identifier("binding_id", binding_id)?;
        Self::load_binding_conn(&self.conn, &clean_id)
    }

    /// Lists all configured bindings ordered by `binding_id`.
    pub fn list_bindings(&self) -> Result<Vec<Binding>, CoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT binding_id, thread_uuid, label, delivered_seq, acked_seq
             FROM bindings
             ORDER BY rowid ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut bindings = Vec::new();
        while let Some(row) = rows.next()? {
            bindings.push(Binding {
                binding_id: row.get(0)?,
                thread_uuid: row.get(1)?,
                label: row.get(2)?,
                delivered_seq: i64_to_u64("delivered_seq", row.get(3)?)?,
                acked_seq: i64_to_u64("acked_seq", row.get(4)?)?,
            });
        }
        Ok(bindings)
    }

    /// Executes a typed [`Command`] scoped to `binding_id`.
    ///
    /// Mutating commands enforce per-binding idempotency via `command_key`:
    /// - An identical retry returns the original [`CommandOutput`] (including after reopen).
    /// - Reusing a `command_key` on the same binding with different parameters or a different
    ///   operation returns [`CoreError::IdempotencyConflict`].
    /// - State transitions, emitted events, wake-up coalescing, and deduplication records commit
    ///   atomically in a single SQLite transaction.
    pub fn execute(
        &mut self,
        binding_id: &str,
        command: Command,
    ) -> Result<CommandOutput, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;

        match command {
            Command::GetRun { run_id } => {
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                let run = Self::load_run_tx(&tx, &clean_binding_id, &clean_run_id)?;
                let output = CommandOutput::Run(Box::new(run));
                enforce_compact_response_budget(&output)?;
                tx.commit()?;
                Ok(output)
            }
            Command::ListRuns { cursor, limit } => {
                let valid_limit = validate_page_limit(limit)?;
                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                let page =
                    Self::list_runs_tx(&tx, &clean_binding_id, cursor.as_deref(), valid_limit)?;
                tx.commit()?;
                Ok(CommandOutput::RunList(page))
            }
            Command::ListRunRevisions {
                run_id,
                cursor,
                limit,
            } => {
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                let valid_limit = validate_page_limit(limit)?;
                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                let page = Self::list_run_revisions_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_run_id,
                    cursor.as_deref(),
                    valid_limit,
                )?;
                tx.commit()?;
                Ok(CommandOutput::RevisionHistory(page))
            }
            Command::ListRunQuestions {
                run_id,
                cursor,
                limit,
            } => {
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                let valid_limit = validate_page_limit(limit)?;
                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                let page = Self::list_run_questions_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_run_id,
                    cursor.as_deref(),
                    valid_limit,
                )?;
                tx.commit()?;
                Ok(CommandOutput::QuestionHistory(page))
            }
            Command::ListActivity {
                after_seq,
                cursor,
                limit,
                run_id,
                kind,
            } => {
                let page = self.list_activity(
                    &clean_binding_id,
                    after_seq,
                    cursor.as_deref(),
                    limit,
                    run_id.as_deref(),
                    kind,
                )?;
                Ok(CommandOutput::Activity(page))
            }
            Command::PullEvents {
                after_seq,
                limit,
                now_ms,
            } => {
                let valid_limit = validate_batch_limit(limit)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let tx = self.conn.transaction()?;
                let batch = Self::pull_events_tx(
                    &tx,
                    &clean_binding_id,
                    after_seq,
                    valid_limit,
                    None,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(CommandOutput::EventBatch(batch))
            }
            Command::PullEventsBounded {
                after_seq,
                limit,
                max_batch_bytes,
                now_ms,
            } => {
                let valid_limit = validate_batch_limit(limit)?;
                let valid_budget = validate_batch_byte_budget(max_batch_bytes)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let tx = self.conn.transaction()?;
                let batch = Self::pull_events_tx(
                    &tx,
                    &clean_binding_id,
                    after_seq,
                    valid_limit,
                    Some(valid_budget),
                    valid_now,
                )?;
                tx.commit()?;
                Ok(CommandOutput::EventBatch(batch))
            }
            Command::StartDemo {
                command_key,
                instruction,
                step_delay_ms,
                now_ms,
            } => {
                let clean_key = validate_command_key(&command_key)?;
                let clean_instruction =
                    validate_bounded_text("instruction", &instruction, MAX_INSTRUCTION_LEN)?;
                let valid_delay = validate_step_delay_ms(step_delay_ms)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let payload_hash =
                    hash_dedup_payload(&("start_demo", &clean_instruction, valid_delay))?;

                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                if let Some(existing) = Self::check_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "start_demo",
                    &payload_hash,
                )? {
                    return Ok(existing);
                }

                let run = Self::start_demo_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_instruction,
                    valid_delay,
                    valid_now,
                )?;
                let output = CommandOutput::Run(Box::new(run));
                Self::record_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "start_demo",
                    &payload_hash,
                    &output,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(output)
            }
            Command::Answer {
                command_key,
                run_id,
                question_id,
                expected_revision,
                answer,
                now_ms,
            } => {
                let clean_key = validate_command_key(&command_key)?;
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                let clean_question_id = validate_identifier("question_id", &question_id)?;
                if expected_revision == 0 {
                    return Err(CoreError::Validation {
                        field: "expected_revision",
                        reason: "expected_revision must be >= 1".to_string(),
                    });
                }
                let clean_answer = validate_bounded_text("answer", &answer, MAX_ANSWER_LEN)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let payload_hash = hash_dedup_payload(&(
                    "answer",
                    &clean_run_id,
                    &clean_question_id,
                    expected_revision,
                    &clean_answer,
                ))?;

                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                if let Some(existing) =
                    Self::check_dedup(&tx, &clean_binding_id, &clean_key, "answer", &payload_hash)?
                {
                    return Ok(existing);
                }

                let run = Self::answer_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_run_id,
                    &clean_question_id,
                    expected_revision,
                    &clean_answer,
                    valid_now,
                )?;
                let output = CommandOutput::Run(Box::new(run));
                Self::record_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "answer",
                    &payload_hash,
                    &output,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(output)
            }
            Command::Steer {
                command_key,
                run_id,
                expected_revision,
                instruction,
                now_ms,
            } => {
                let clean_key = validate_command_key(&command_key)?;
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                if expected_revision == 0 {
                    return Err(CoreError::Validation {
                        field: "expected_revision",
                        reason: "expected_revision must be >= 1".to_string(),
                    });
                }
                let clean_instruction =
                    validate_bounded_text("instruction", &instruction, MAX_INSTRUCTION_LEN)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let payload_hash = hash_dedup_payload(&(
                    "steer",
                    &clean_run_id,
                    expected_revision,
                    &clean_instruction,
                ))?;

                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                if let Some(existing) =
                    Self::check_dedup(&tx, &clean_binding_id, &clean_key, "steer", &payload_hash)?
                {
                    return Ok(existing);
                }

                let run = Self::steer_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_run_id,
                    expected_revision,
                    &clean_instruction,
                    valid_now,
                )?;
                let output = CommandOutput::Run(Box::new(run));
                Self::record_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "steer",
                    &payload_hash,
                    &output,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(output)
            }
            Command::Cancel {
                command_key,
                run_id,
                reason,
                now_ms,
            } => {
                let clean_key = validate_command_key(&command_key)?;
                let clean_run_id = validate_identifier("run_id", &run_id)?;
                let clean_reason =
                    validate_optional_text("reason", reason.as_deref(), MAX_CANCEL_REASON_LEN)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let payload_hash = hash_dedup_payload(&("cancel", &clean_run_id, &clean_reason))?;

                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                if let Some(existing) =
                    Self::check_dedup(&tx, &clean_binding_id, &clean_key, "cancel", &payload_hash)?
                {
                    return Ok(existing);
                }

                let run = Self::cancel_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_run_id,
                    clean_reason.as_deref(),
                    valid_now,
                )?;
                let output = CommandOutput::Run(Box::new(run));
                Self::record_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "cancel",
                    &payload_hash,
                    &output,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(output)
            }
            Command::AcknowledgeEvents {
                command_key,
                batch_id,
                now_ms,
            } => {
                let clean_key = validate_command_key(&command_key)?;
                let clean_batch_id = validate_identifier("batch_id", &batch_id)?;
                let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
                let payload_hash = hash_dedup_payload(&("acknowledge_events", &clean_batch_id))?;

                let tx = self.conn.transaction()?;
                Self::ensure_binding_exists(&tx, &clean_binding_id)?;
                if let Some(existing) = Self::check_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "acknowledge_events",
                    &payload_hash,
                )? {
                    return Ok(existing);
                }

                let ack = Self::acknowledge_events_tx(
                    &tx,
                    &clean_binding_id,
                    &clean_batch_id,
                    valid_now,
                )?;
                let output = CommandOutput::AckResult(ack);
                Self::record_dedup(
                    &tx,
                    &clean_binding_id,
                    &clean_key,
                    "acknowledge_events",
                    &payload_hash,
                    &output,
                    valid_now,
                )?;
                tx.commit()?;
                Ok(output)
            }
            Command::StartWorker {
                command_key,
                profile_id,
                profile_digest,
                provider,
                requested_model,
                requested_effort,
                base_commit,
                execution_binding,
                instruction,
                now_ms,
            } => self.execute_start_worker(
                &clean_binding_id,
                command_key,
                profile_id,
                profile_digest,
                provider,
                requested_model,
                requested_effort,
                base_commit,
                execution_binding,
                instruction,
                now_ms,
            ),
            Command::GetWorker { run_id } => self.execute_get_worker(&clean_binding_id, run_id),
            Command::CancelWorker {
                command_key,
                run_id,
                expected_revision,
                reason,
                now_ms,
            } => self.execute_cancel_worker(
                &clean_binding_id,
                command_key,
                run_id,
                expected_revision,
                reason,
                now_ms,
            ),
        }
    }

    /// Advances all deterministic demo runs whose scheduled `next_step_at_ms <= now_ms`.
    ///
    /// - Runs in `RunStatus::Running` emit `RunProgress` (`instruction: None`), pose a compact
    ///   revision-scoped demo question referencing `(run_id, revision)` and `get_run` (`QuestionAsked`
    ///   with `instruction: None`), transition to `RunStatus::WaitingInput`, and schedule/coalesce a
    ///   pending wake-up notice.
    /// - Runs in `RunStatus::Completing` transition to `RunStatus::Completed`, emit `RunCompleted`
    ///   (`instruction: None`), and schedule/coalesce a pending wake-up notice.
    /// - Timed/derived events never duplicate the full instruction text; full original instruction
    ///   and revision history remain in [`Run`] (`get_run`).
    /// - Cancelled, superseded, or worker-backed runs are never advanced by `tick`.
    pub fn tick(&mut self, now_ms: i64) -> Result<TickReport, CoreError> {
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;

        let due_runs = {
            let mut stmt = tx.prepare(
                "SELECT run_id, binding_id, revision, status, step_seq
                 FROM runs
                 WHERE next_step_at_ms IS NOT NULL
                   AND next_step_at_ms <= ?1
                   AND status IN ('running', 'completing')
                   AND NOT EXISTS (SELECT 1 FROM workers w WHERE w.run_id = runs.run_id)
                 ORDER BY next_step_at_ms ASC, rowid ASC",
            )?;
            let mut rows = stmt.query(params![valid_now])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let status_raw: String = row.get(3)?;
                let status = RunStatus::from_str(&status_raw).ok_or_else(|| {
                    CoreError::CorruptState(format!("invalid run status `{status_raw}`"))
                })?;
                items.push((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    i64_to_u64("revision", row.get(2)?)?,
                    status,
                    i64_to_u64("step_seq", row.get(4)?)?,
                ));
            }
            items
        };

        let mut advanced_runs = Vec::with_capacity(due_runs.len());
        let mut emitted_events = 0usize;

        for (run_id, binding_id, revision, status, step_seq) in due_runs {
            match status {
                RunStatus::Running => {
                    let progress_step = step_seq + 1;
                    Self::emit_event_tx(
                        &tx,
                        &binding_id,
                        &run_id,
                        revision,
                        progress_step,
                        EventKind::RunProgress,
                        EventPayload {
                            untrusted_data: true,
                            summary: format!(
                                "Demo run progressing on revision {revision} before input gate"
                            ),
                            question_id: None,
                            question_prompt: None,
                            answer: None,
                            instruction: None,
                            cancel_reason: None,
                        },
                        valid_now,
                    )?;
                    emitted_events += 1;

                    let question_step = progress_step + 1;
                    let question_id = generate_id("qst");
                    let prompt = format!(
                        "Demo confirmation for run {run_id} (revision {revision}): confirm the current instruction (retrieve get_run for full text)."
                    );
                    tx.execute(
                        "INSERT INTO questions (
                            question_id, binding_id, run_id, revision, status, prompt,
                            answer, superseded_by_revision, created_at_ms, resolved_at_ms
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, NULL)",
                        params![
                            question_id,
                            binding_id,
                            run_id,
                            u64_to_i64("revision", revision)?,
                            QuestionStatus::Pending.as_str(),
                            prompt,
                            valid_now
                        ],
                    )?;

                    tx.execute(
                        "UPDATE runs
                         SET status = ?1,
                             step_seq = ?2,
                             next_step_at_ms = NULL,
                             active_question_id = ?3,
                             updated_at_ms = ?4
                         WHERE run_id = ?5 AND binding_id = ?6",
                        params![
                            RunStatus::WaitingInput.as_str(),
                            u64_to_i64("step_seq", question_step)?,
                            question_id,
                            valid_now,
                            run_id,
                            binding_id
                        ],
                    )?;

                    Self::emit_event_tx(
                        &tx,
                        &binding_id,
                        &run_id,
                        revision,
                        question_step,
                        EventKind::QuestionAsked,
                        EventPayload {
                            untrusted_data: true,
                            summary: format!(
                                "Question asked on run {run_id} (revision {revision})"
                            ),
                            question_id: Some(question_id),
                            question_prompt: Some(prompt),
                            answer: None,
                            instruction: None,
                            cancel_reason: None,
                        },
                        valid_now,
                    )?;
                    emitted_events += 1;
                    advanced_runs.push(run_id);
                }
                RunStatus::Completing => {
                    let next_step = step_seq + 1;
                    tx.execute(
                        "UPDATE runs
                         SET status = ?1,
                             step_seq = ?2,
                             next_step_at_ms = NULL,
                             active_question_id = NULL,
                             completed_at_ms = ?3,
                             updated_at_ms = ?3
                         WHERE run_id = ?4 AND binding_id = ?5",
                        params![
                            RunStatus::Completed.as_str(),
                            u64_to_i64("step_seq", next_step)?,
                            valid_now,
                            run_id,
                            binding_id
                        ],
                    )?;

                    Self::emit_event_tx(
                        &tx,
                        &binding_id,
                        &run_id,
                        revision,
                        next_step,
                        EventKind::RunCompleted,
                        EventPayload {
                            untrusted_data: true,
                            summary: format!("Demo run {run_id} completed at revision {revision}"),
                            question_id: None,
                            question_prompt: None,
                            answer: None,
                            instruction: None,
                            cancel_reason: None,
                        },
                        valid_now,
                    )?;
                    emitted_events += 1;
                    advanced_runs.push(run_id);
                }
                _ => {}
            }
        }

        tx.commit()?;
        Ok(TickReport {
            advanced_runs,
            emitted_events,
        })
    }

    /// Claims the next pending wake-up notice across configured bindings, persisting `InFlight`
    /// intent before the caller invokes any external Codex queue command.
    ///
    /// Only notices in `WakeupStatus::Pending` are claimed. Notices in `Idle`, `InFlight`,
    /// `Submitted`, `Failed`, or `Unknown` are never claimed or automatically retried.
    pub fn claim_wakeup(&mut self, now_ms: i64) -> Result<Option<WakeupClaim>, CoreError> {
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;
        let next_binding: Option<String> = tx
            .query_row(
                "SELECT binding_id
                 FROM wakeups
                 WHERE status = 'pending'
                 ORDER BY COALESCE(pending_since_seq, 0) ASC, binding_id ASC
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        let Some(binding_id) = next_binding else {
            tx.commit()?;
            return Ok(None);
        };

        let claim = Self::claim_wakeup_for_binding_tx(&tx, &binding_id, valid_now)?;
        tx.commit()?;
        Ok(claim)
    }

    /// Claims a pending wake-up notice for a specific `binding_id`, persisting `InFlight` intent.
    pub fn claim_wakeup_for_binding(
        &mut self,
        binding_id: &str,
        now_ms: i64,
    ) -> Result<Option<WakeupClaim>, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;
        let tx = self.conn.transaction()?;
        Self::ensure_binding_exists(&tx, &clean_binding_id)?;
        let claim = Self::claim_wakeup_for_binding_tx(&tx, &clean_binding_id, valid_now)?;
        tx.commit()?;
        Ok(claim)
    }

    /// Records the explicit outcome (`Submitted`, `Failed`, or `Unknown`) of an `InFlight` wake-up claim.
    pub fn record_wakeup_result(
        &mut self,
        claim_id: &str,
        outcome: WakeupOutcome,
        now_ms: i64,
    ) -> Result<WakeupNotice, CoreError> {
        let clean_claim_id = validate_identifier("claim_id", claim_id)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;

        let (target_status, error_detail) = match outcome {
            WakeupOutcome::Submitted => (WakeupStatus::Submitted, None),
            WakeupOutcome::Failed { error } => {
                let clean_err = validate_bounded_text("error", &error, MAX_WAKEUP_ERROR_LEN)?;
                (WakeupStatus::Failed, Some(clean_err))
            }
            WakeupOutcome::Unknown { reason } => {
                let clean_reason = validate_bounded_text("reason", &reason, MAX_WAKEUP_ERROR_LEN)?;
                (WakeupStatus::Unknown, Some(clean_reason))
            }
        };

        let tx = self.conn.transaction()?;
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT binding_id, status FROM wakeups WHERE claim_id = ?1",
                params![clean_claim_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;

        let Some((binding_id, current_status_str)) = row else {
            return Err(CoreError::WakeupClaimNotFound {
                claim_id: clean_claim_id,
            });
        };

        let current_status = WakeupStatus::from_str(&current_status_str).ok_or_else(|| {
            CoreError::CorruptState(format!("invalid wakeup status `{current_status_str}`"))
        })?;

        if current_status != WakeupStatus::InFlight {
            return Err(CoreError::InvalidWakeupState {
                binding_id,
                status: current_status,
                action: "record_wakeup_result",
            });
        }

        tx.execute(
            "UPDATE wakeups
             SET status = ?1,
                 last_error = ?2,
                 resolved_at_ms = ?3
             WHERE binding_id = ?4 AND claim_id = ?5",
            params![
                target_status.as_str(),
                error_detail,
                valid_now,
                binding_id,
                clean_claim_id
            ],
        )?;

        tx.execute(
            "UPDATE wakeup_claims
             SET status = ?1,
                 error_detail = ?2,
                 resolved_at_ms = ?3
             WHERE claim_id = ?4",
            params![
                target_status.as_str(),
                error_detail,
                valid_now,
                clean_claim_id
            ],
        )?;

        let binding = Self::load_binding_tx(&tx, &binding_id)?;
        Self::reconcile_wakeup_on_ack_tx(&tx, &binding_id, binding.acked_seq)?;

        let notice = Self::load_wakeup_tx(&tx, &binding_id)?;
        tx.commit()?;
        Ok(notice)
    }

    /// Explicitly rearms a `Failed` (or `Unknown` when `allow_unknown == true`) wake-up notice
    /// back to `WakeupStatus::Pending` when `expected_claim_id` matches the binding's current live claim.
    ///
    /// - Rejects `Idle`, `Pending`, `InFlight`, and `Submitted` notices with [`CoreError::InvalidWakeupState`].
    /// - Rejects `Unknown` notices when `allow_unknown == false` with [`CoreError::InvalidWakeupState`].
    /// - Rejects stale, repeated, or cross-binding claim IDs without releasing any newer claim.
    /// - Preserves `delivered_seq`, `acked_seq`, `pending_since_seq`, `latest_important_seq`,
    ///   `coalesced_count`, and cumulative `attempt_count`.
    /// - Clears active claim fields (`claim_id`, `covered_up_to_seq`, `last_error`, `claimed_at_ms`,
    ///   `resolved_at_ms`) on the live notice, keeps historical `wakeup_claims` outcomes immutable,
    ///   and records a minimal persisted audit row in `wakeup_retries`.
    pub fn retry_wakeup(
        &mut self,
        binding_id: &str,
        expected_claim_id: &str,
        allow_unknown: bool,
        now_ms: i64,
    ) -> Result<WakeupNotice, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_expected_claim_id = validate_identifier("expected_claim_id", expected_claim_id)?;
        let valid_now = validate_timestamp_ms("now_ms", now_ms)?;

        let tx = self.conn.transaction()?;
        let wakeup = Self::load_wakeup_tx(&tx, &clean_binding_id)?;

        // Reject cross-binding claim IDs regardless of the target binding's current status.
        let claim_owner: Option<String> = tx
            .query_row(
                "SELECT binding_id FROM wakeup_claims WHERE claim_id = ?1",
                params![clean_expected_claim_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(owner_binding_id) = claim_owner {
            if owner_binding_id != clean_binding_id {
                return Err(CoreError::WakeupClaimNotFound {
                    claim_id: clean_expected_claim_id,
                });
            }
        }

        // If this binding currently has a live claim that differs from `expected_claim_id`,
        // reject as stale/unmatched without releasing the newer claim.
        if wakeup.claim_id.is_some()
            && wakeup.claim_id.as_deref() != Some(clean_expected_claim_id.as_str())
        {
            return Err(CoreError::WakeupClaimNotFound {
                claim_id: clean_expected_claim_id,
            });
        }

        match wakeup.status {
            WakeupStatus::Idle
            | WakeupStatus::Pending
            | WakeupStatus::InFlight
            | WakeupStatus::Submitted => {
                return Err(CoreError::InvalidWakeupState {
                    binding_id: clean_binding_id,
                    status: wakeup.status,
                    action: "retry_wakeup",
                });
            }
            WakeupStatus::Unknown if !allow_unknown => {
                return Err(CoreError::InvalidWakeupState {
                    binding_id: clean_binding_id,
                    status: WakeupStatus::Unknown,
                    action: "retry_wakeup",
                });
            }
            WakeupStatus::Failed | WakeupStatus::Unknown => {}
        }

        if wakeup.claim_id.as_deref() != Some(clean_expected_claim_id.as_str()) {
            return Err(CoreError::WakeupClaimNotFound {
                claim_id: clean_expected_claim_id,
            });
        }

        if wakeup.pending_since_seq.is_none() || wakeup.coalesced_count == 0 {
            return Err(CoreError::CorruptState(format!(
                "wakeup for binding `{clean_binding_id}` in status `{:?}` has no pending range",
                wakeup.status
            )));
        }

        let retry_id = generate_id("wrt");
        tx.execute(
            "INSERT INTO wakeup_retries (
                retry_id, binding_id, prior_claim_id, prior_status, allow_unknown, retried_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                retry_id,
                clean_binding_id,
                clean_expected_claim_id,
                wakeup.status.as_str(),
                i64::from(allow_unknown),
                valid_now
            ],
        )?;

        let updated = tx.execute(
            "UPDATE wakeups
             SET status = ?1,
                 claim_id = NULL,
                 covered_up_to_seq = 0,
                 last_error = NULL,
                 claimed_at_ms = NULL,
                 resolved_at_ms = NULL
             WHERE binding_id = ?2 AND claim_id = ?3 AND status = ?4",
            params![
                WakeupStatus::Pending.as_str(),
                clean_binding_id,
                clean_expected_claim_id,
                wakeup.status.as_str()
            ],
        )?;
        if updated != 1 {
            return Err(CoreError::WakeupClaimNotFound {
                claim_id: clean_expected_claim_id,
            });
        }

        let notice = Self::load_wakeup_tx(&tx, &clean_binding_id)?;
        tx.commit()?;
        Ok(notice)
    }

    /// Atomically rearms a `WakeupStatus::Failed` notice back to `WakeupStatus::Pending` when
    /// `expected_claim_id` matches the binding's current live claim inside a single SQLite
    /// compare-and-swap transaction.
    ///
    /// Never rearms `WakeupStatus::Unknown` (which must be reconciled via `pull_events` and
    /// `ack_events`) or any non-`Failed` state.
    pub fn rearm_failed_wakeup(
        &mut self,
        binding_id: &str,
        expected_claim_id: &str,
        now_ms: i64,
    ) -> Result<WakeupNotice, CoreError> {
        self.retry_wakeup(binding_id, expected_claim_id, false, now_ms)
    }

    /// Lists persisted manual wake-up retry audit records for `binding_id` in chronological order.
    pub fn list_wakeup_retries(
        &self,
        binding_id: &str,
    ) -> Result<Vec<WakeupRetryRecord>, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        let mut stmt = self.conn.prepare(
            "SELECT retry_id, binding_id, prior_claim_id, prior_status, allow_unknown, retried_at_ms
             FROM wakeup_retries
             WHERE binding_id = ?1
             ORDER BY rowid ASC",
        )?;
        let mut rows = stmt.query(params![clean_binding_id])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let status_raw: String = row.get(3)?;
            let prior_status = WakeupStatus::from_str(&status_raw).ok_or_else(|| {
                CoreError::CorruptState(format!("invalid wakeup retry prior_status `{status_raw}`"))
            })?;
            records.push(WakeupRetryRecord {
                retry_id: row.get(0)?,
                binding_id: row.get(1)?,
                prior_claim_id: row.get(2)?,
                prior_status,
                allow_unknown: row.get::<_, i64>(4)? != 0,
                retried_at_ms: row.get(5)?,
            });
        }
        Ok(records)
    }

    /// Inspects the current persisted [`WakeupNotice`] state for `binding_id`.
    pub fn get_wakeup(&self, binding_id: &str) -> Result<WakeupNotice, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        Self::load_wakeup_conn(&self.conn, &clean_binding_id)
    }

    /// Convenience helper to fetch a constant-size [`Run`] snapshot scoped to `binding_id`.
    pub fn get_run(&self, binding_id: &str, run_id: &str) -> Result<Run, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_run_id = validate_identifier("run_id", run_id)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        let run = Self::load_run_conn(&self.conn, &clean_binding_id, &clean_run_id)?;
        enforce_compact_response_budget(&CommandOutput::Run(Box::new(run.clone())))?;
        Ok(run)
    }

    /// Convenience helper to fetch a bounded keyset-paginated [`RunListPage`] scoped to `binding_id`.
    pub fn list_runs(
        &self,
        binding_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RunListPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        Self::list_runs_conn(&self.conn, &clean_binding_id, cursor, valid_limit)
    }

    /// Convenience helper to fetch a bounded keyset-paginated [`RevisionHistoryPage`] for `(binding_id, run_id)`.
    pub fn list_run_revisions(
        &self,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RevisionHistoryPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_run_id = validate_identifier("run_id", run_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        Self::list_run_revisions_conn(
            &self.conn,
            &clean_binding_id,
            &clean_run_id,
            cursor,
            valid_limit,
        )
    }

    /// Convenience helper to fetch a bounded keyset-paginated [`QuestionHistoryPage`] for `(binding_id, run_id)`.
    pub fn list_run_questions(
        &self,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<QuestionHistoryPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let clean_run_id = validate_identifier("run_id", run_id)?;
        let valid_limit = validate_page_limit(limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        Self::list_run_questions_conn(
            &self.conn,
            &clean_binding_id,
            &clean_run_id,
            cursor,
            valid_limit,
        )
    }

    /// Convenience helper to fetch a bounded, read-only keyset-paginated [`ActivityPage`] of durable events for `binding_id`.
    ///
    /// Never modifies `delivered_seq`, `acked_seq`, `event_batches`, or `wakeups`.
    pub fn list_activity(
        &self,
        binding_id: &str,
        after_seq: Option<u64>,
        cursor: Option<&str>,
        limit: usize,
        run_id: Option<&str>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        if cursor.is_some() && after_seq.is_some() {
            return Err(CoreError::Validation {
                field: "cursor",
                reason: "cursor and after_seq cannot be specified simultaneously".to_string(),
            });
        }
        let valid_limit = validate_page_limit(limit)?;
        let clean_run_id = match run_id {
            Some(raw_run_id) => Some(validate_activity_run_id(raw_run_id)?),
            None => None,
        };
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;
        Self::list_activity_conn(
            &self.conn,
            &clean_binding_id,
            after_seq,
            cursor,
            valid_limit,
            clean_run_id.as_deref(),
            kind,
        )
    }

    /// Computes SQL aggregate counts (`total_runs`, `active_runs`) and returns at most `recent_limit`
    /// recent [`RunSummary`] records for `binding_id` without materializing all runs, revisions, or questions.
    pub fn get_status_run_summaries(
        &self,
        binding_id: &str,
        recent_limit: usize,
    ) -> Result<(usize, usize, Vec<RunSummary>), CoreError> {
        let clean_binding_id = validate_identifier("binding_id", binding_id)?;
        let valid_limit = validate_page_limit(recent_limit)?;
        Self::ensure_binding_exists_conn(&self.conn, &clean_binding_id)?;

        let (total_raw, active_raw): (i64, i64) = self.conn.query_row(
            "SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN status NOT IN ('completed', 'failed', 'cancelled', 'timed_out', 'unknown') THEN 1 ELSE 0 END), 0)
             FROM runs
             WHERE binding_id = ?1",
            params![clean_binding_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        let total_runs = i64_to_u64("total_runs", total_raw)? as usize;
        let active_runs = i64_to_u64("active_runs", active_raw)? as usize;

        let mut stmt = self.conn.prepare(
            "SELECT run_id, revision, status, instruction, active_question_id, started_at_ms, updated_at_ms
             FROM runs
             WHERE binding_id = ?1
             ORDER BY rowid DESC
             LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![
            clean_binding_id,
            u64_to_i64("limit", valid_limit as u64)?
        ])?;
        let mut recent_runs = Vec::with_capacity(valid_limit.min(total_runs));
        while let Some(row) = rows.next()? {
            let status_raw: String = row.get(2)?;
            let status = RunStatus::from_str(&status_raw).ok_or_else(|| {
                CoreError::CorruptState(format!("invalid run status `{status_raw}`"))
            })?;
            let instruction: String = row.get(3)?;
            let (instruction_preview, instruction_truncated) = truncate_preview(&instruction);
            recent_runs.push(RunSummary {
                untrusted_data: true,
                run_id: row.get(0)?,
                binding_id: clean_binding_id.clone(),
                revision: i64_to_u64("revision", row.get(1)?)?,
                status,
                instruction_preview,
                instruction_truncated,
                active_question_id: row.get(4)?,
                started_at_ms: row.get(5)?,
                updated_at_ms: row.get(6)?,
            });
        }

        Ok((total_runs, active_runs, recent_runs))
    }

    /// Computes consistent runtime-wide aggregate counts of active runs, workers, and wake-ups
    /// across all bindings in a single read query.
    pub fn inspect_runtime_idle(&self) -> Result<RuntimeIdleInspection, CoreError> {
        let (runs_raw, workers_raw, in_flight_raw, pending_raw): (i64, i64, i64, i64) =
            self.conn.query_row(
                "SELECT
                    (SELECT COUNT(*) FROM runs WHERE status NOT IN ('completed', 'failed', 'cancelled', 'timed_out', 'unknown')),
                    (SELECT COUNT(*) FROM workers WHERE status NOT IN ('completed', 'failed', 'cancelled', 'timed_out', 'unknown')),
                    (SELECT COUNT(*) FROM wakeups WHERE status = 'in_flight'),
                    (SELECT COUNT(*) FROM wakeups WHERE status = 'pending')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;

        Ok(RuntimeIdleInspection {
            non_terminal_runs: i64_to_u64("non_terminal_runs", runs_raw)? as usize,
            non_terminal_workers: i64_to_u64("non_terminal_workers", workers_raw)? as usize,
            in_flight_wakeups: i64_to_u64("in_flight_wakeups", in_flight_raw)? as usize,
            pending_wakeups: i64_to_u64("pending_wakeups", pending_raw)? as usize,
        })
    }

    fn init_schema_and_recover(tx: &Transaction<'_>) -> Result<(), CoreError> {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bindings (
                binding_id TEXT PRIMARY KEY,
                thread_uuid TEXT NOT NULL,
                label TEXT NOT NULL,
                delivered_seq INTEGER NOT NULL DEFAULT 0,
                acked_seq INTEGER NOT NULL DEFAULT 0,
                next_event_seq INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                instruction TEXT NOT NULL,
                step_seq INTEGER NOT NULL,
                step_delay_ms INTEGER NOT NULL,
                next_step_at_ms INTEGER,
                active_question_id TEXT,
                started_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                completed_at_ms INTEGER,
                cancelled_at_ms INTEGER,
                cancel_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_runs_binding
                ON runs(binding_id, started_at_ms, run_id);
            CREATE INDEX IF NOT EXISTS idx_runs_binding_order
                ON runs(binding_id);

            CREATE TABLE IF NOT EXISTS run_revisions (
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                revision INTEGER NOT NULL,
                instruction TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                superseded_at_ms INTEGER,
                PRIMARY KEY (run_id, revision)
            );

            CREATE TABLE IF NOT EXISTS questions (
                question_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                prompt TEXT NOT NULL,
                answer TEXT,
                superseded_by_revision INTEGER,
                created_at_ms INTEGER NOT NULL,
                resolved_at_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_questions_run
                ON questions(run_id, revision, question_id);
            CREATE INDEX IF NOT EXISTS idx_questions_binding_run_order
                ON questions(binding_id, run_id);

            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                seq INTEGER NOT NULL,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                revision INTEGER NOT NULL,
                step_seq INTEGER NOT NULL,
                kind TEXT NOT NULL,
                important INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                UNIQUE (binding_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_events_binding_seq
                ON events(binding_id, seq);

            CREATE TABLE IF NOT EXISTS event_batches (
                batch_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                from_seq INTEGER NOT NULL,
                to_seq INTEGER NOT NULL,
                event_count INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                acked_at_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS command_dedup (
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                command_key TEXT NOT NULL,
                operation TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (binding_id, command_key)
            );

            CREATE TABLE IF NOT EXISTS wakeups (
                binding_id TEXT PRIMARY KEY REFERENCES bindings(binding_id),
                thread_uuid TEXT NOT NULL,
                status TEXT NOT NULL,
                claim_id TEXT,
                pending_since_seq INTEGER,
                latest_important_seq INTEGER NOT NULL DEFAULT 0,
                covered_up_to_seq INTEGER NOT NULL DEFAULT 0,
                coalesced_count INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                claimed_at_ms INTEGER,
                resolved_at_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS wakeup_claims (
                claim_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                thread_uuid TEXT NOT NULL,
                pending_since_seq INTEGER NOT NULL,
                covered_up_to_seq INTEGER NOT NULL,
                coalesced_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                error_detail TEXT,
                claimed_at_ms INTEGER NOT NULL,
                resolved_at_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS wakeup_retries (
                retry_id TEXT PRIMARY KEY,
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                prior_claim_id TEXT NOT NULL REFERENCES wakeup_claims(claim_id),
                prior_status TEXT NOT NULL,
                allow_unknown INTEGER NOT NULL,
                retried_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_wakeup_retries_binding
                ON wakeup_retries(binding_id, retried_at_ms, retry_id);

            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id),
                binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                profile_digest TEXT NOT NULL,
                provider TEXT NOT NULL,
                requested_model TEXT NOT NULL,
                requested_effort TEXT NOT NULL,
                observed_model TEXT,
                observed_effort TEXT,
                instruction TEXT NOT NULL,
                claim_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                execution_binding_json TEXT,
                worktree_json TEXT NOT NULL,
                progress_json TEXT NOT NULL,
                outcome_json TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                started_at_ms INTEGER,
                completed_at_ms INTEGER,
                cancel_requested_at_ms INTEGER,
                cancel_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_workers_binding_run
                ON workers(binding_id, run_id);
            CREATE INDEX IF NOT EXISTS idx_workers_status_order
                ON workers(status);",
        )?;

        let existing_version: Option<u32> = tx
            .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;

        match existing_version {
            None => {
                tx.execute(
                    "INSERT INTO schema_meta (id, version) VALUES (1, ?1)",
                    params![SCHEMA_VERSION],
                )?;
            }
            Some(version) if version == SCHEMA_VERSION => {}
            Some(version)
                if version == LEGACY_SCHEMA_VERSION_V3 || version == LEGACY_SCHEMA_VERSION_V2 =>
            {
                Self::ensure_workers_execution_binding_column_tx(tx)?;
                tx.execute(
                    "UPDATE schema_meta SET version = ?1 WHERE id = 1",
                    params![SCHEMA_VERSION],
                )?;
            }
            Some(version) if version == LEGACY_SCHEMA_VERSION_V1 => {
                Self::ensure_workers_execution_binding_column_tx(tx)?;
                Self::migrate_legacy_command_dedup_tx(tx)?;
                tx.execute(
                    "UPDATE schema_meta SET version = ?1 WHERE id = 1",
                    params![SCHEMA_VERSION],
                )?;
            }
            Some(found) => {
                return Err(CoreError::IncompatibleSchemaVersion {
                    found,
                    expected: SCHEMA_VERSION,
                });
            }
        }

        // Any wake-up claim that was in_flight when the previous owner exited has an
        // ambiguous external delivery state and must transition to `unknown` without
        // automatic retry, then reconcile against durable event acknowledgements.
        tx.execute(
            "UPDATE wakeups
             SET status = 'unknown',
                 last_error = COALESCE(last_error, 'in-flight wake-up claim became unknown on reopen')
             WHERE status = 'in_flight'",
            [],
        )?;
        tx.execute(
            "UPDATE wakeup_claims
             SET status = 'unknown',
                 error_detail = COALESCE(error_detail, 'in-flight wake-up claim became unknown on reopen')
             WHERE status = 'in_flight'",
            [],
        )?;

        // Any worker that was queued, claimed, running, or cancel_requested when the previous
        // owner exited has an uncertain execution state and must transition to `unknown` with
        // an important event and wake-up notice, without automatic retry or PID kills.
        Self::recover_in_flight_workers_tx(tx)?;

        let bindings_acked: Vec<(String, u64)> = {
            let mut stmt = tx.prepare("SELECT binding_id, acked_seq FROM bindings")?;
            let mut rows = stmt.query([])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                items.push((
                    row.get::<_, String>(0)?,
                    i64_to_u64("acked_seq", row.get(1)?)?,
                ));
            }
            items
        };

        for (binding_id, acked_seq) in bindings_acked {
            Self::reconcile_wakeup_on_ack_tx(tx, &binding_id, acked_seq)?;
        }

        Ok(())
    }

    fn ensure_workers_execution_binding_column_tx(tx: &Transaction<'_>) -> Result<(), CoreError> {
        let has_column = {
            let mut stmt = tx.prepare("PRAGMA table_info(workers)")?;
            let mut rows = stmt.query([])?;
            let mut found = false;
            while let Some(row) = rows.next()? {
                let col_name: String = row.get(1)?;
                if col_name == "execution_binding_json" {
                    found = true;
                    break;
                }
            }
            found
        };
        if !has_column {
            tx.execute(
                "ALTER TABLE workers ADD COLUMN execution_binding_json TEXT",
                [],
            )?;
        }
        Ok(())
    }

    fn start_demo_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        instruction: &str,
        step_delay_ms: i64,
        now_ms: i64,
    ) -> Result<Run, CoreError> {
        let run_id = generate_id("run");
        let revision = 1u64;
        let step_seq = 1u64;
        let next_step_at_ms = now_ms.saturating_add(step_delay_ms);

        tx.execute(
            "INSERT INTO runs (
                run_id, binding_id, revision, status, instruction, step_seq,
                step_delay_ms, next_step_at_ms, active_question_id, started_at_ms,
                updated_at_ms, completed_at_ms, cancelled_at_ms, cancel_reason
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?9, NULL, NULL, NULL)",
            params![
                run_id,
                binding_id,
                u64_to_i64("revision", revision)?,
                RunStatus::Running.as_str(),
                instruction,
                u64_to_i64("step_seq", step_seq)?,
                step_delay_ms,
                next_step_at_ms,
                now_ms
            ],
        )?;

        tx.execute(
            "INSERT INTO run_revisions (run_id, revision, instruction, created_at_ms, superseded_at_ms)
             VALUES (?1, ?2, ?3, ?4, NULL)",
            params![run_id, u64_to_i64("revision", revision)?, instruction, now_ms],
        )?;

        Self::emit_event_tx(
            tx,
            binding_id,
            &run_id,
            revision,
            step_seq,
            EventKind::RunStarted,
            EventPayload {
                untrusted_data: true,
                summary: format!("Demo run {run_id} started at revision 1"),
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: Some(instruction.to_string()),
                cancel_reason: None,
            },
            now_ms,
        )?;

        Self::load_run_tx(tx, binding_id, &run_id)
    }

    fn answer_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        question_id: &str,
        expected_revision: u64,
        answer: &str,
        now_ms: i64,
    ) -> Result<Run, CoreError> {
        let run = Self::load_run_tx(tx, binding_id, run_id)?;
        if Self::is_worker_run_tx(tx, run_id)? {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "answer",
            });
        }
        if run.status.is_terminal() {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "answer",
            });
        }

        // Look up the target question within (binding_id, run_id) first so we can report
        // stale/superseded questions and revision mismatches accurately.
        let question = Self::load_question_tx(tx, binding_id, run_id, question_id)?;

        if run.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: run_id.to_string(),
                expected_revision,
                actual_revision: run.revision,
            });
        }

        if question.status != QuestionStatus::Pending
            || question.revision != run.revision
            || run.active_question_id.as_deref() != Some(question_id)
        {
            return Err(CoreError::StaleQuestion {
                run_id: run_id.to_string(),
                question_id: question_id.to_string(),
                question_status: question.status,
                question_revision: question.revision,
                current_revision: run.revision,
            });
        }

        if run.status != RunStatus::WaitingInput {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "answer",
            });
        }

        tx.execute(
            "UPDATE questions
             SET status = ?1,
                 answer = ?2,
                 resolved_at_ms = ?3
             WHERE question_id = ?4 AND run_id = ?5 AND binding_id = ?6",
            params![
                QuestionStatus::Answered.as_str(),
                answer,
                now_ms,
                question_id,
                run_id,
                binding_id
            ],
        )?;

        let next_step_seq = run.step_seq + 1;
        let next_step_at_ms = now_ms.saturating_add(run.step_delay_ms);
        tx.execute(
            "UPDATE runs
             SET status = ?1,
                 step_seq = ?2,
                 next_step_at_ms = ?3,
                 active_question_id = NULL,
                 updated_at_ms = ?4
             WHERE run_id = ?5 AND binding_id = ?6",
            params![
                RunStatus::Completing.as_str(),
                u64_to_i64("step_seq", next_step_seq)?,
                next_step_at_ms,
                now_ms,
                run_id,
                binding_id
            ],
        )?;

        Self::emit_event_tx(
            tx,
            binding_id,
            run_id,
            run.revision,
            next_step_seq,
            EventKind::QuestionAnswered,
            EventPayload {
                untrusted_data: true,
                summary: format!(
                    "Question {question_id} answered on run {run_id} (revision {})",
                    run.revision
                ),
                question_id: Some(question_id.to_string()),
                question_prompt: None,
                answer: Some(answer.to_string()),
                instruction: None,
                cancel_reason: None,
            },
            now_ms,
        )?;

        Self::load_run_tx(tx, binding_id, run_id)
    }

    fn steer_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        expected_revision: u64,
        instruction: &str,
        now_ms: i64,
    ) -> Result<Run, CoreError> {
        let run = Self::load_run_tx(tx, binding_id, run_id)?;
        if Self::is_worker_run_tx(tx, run_id)? {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "steer",
            });
        }
        if run.status.is_terminal() {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "steer",
            });
        }

        if run.revision != expected_revision {
            return Err(CoreError::StaleRevision {
                run_id: run_id.to_string(),
                expected_revision,
                actual_revision: run.revision,
            });
        }

        let new_revision = run.revision + 1;
        let mut step_seq = run.step_seq;

        // Supersede any pending question(s) on this run.
        let pending_questions: Vec<String> = {
            let mut stmt = tx.prepare(
                "SELECT question_id
                 FROM questions
                 WHERE run_id = ?1 AND binding_id = ?2 AND status = 'pending'
                 ORDER BY rowid ASC",
            )?;
            let mut rows = stmt.query(params![run_id, binding_id])?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next()? {
                ids.push(row.get(0)?);
            }
            ids
        };

        for qid in &pending_questions {
            tx.execute(
                "UPDATE questions
                 SET status = ?1,
                     superseded_by_revision = ?2,
                     resolved_at_ms = ?3
                 WHERE question_id = ?4 AND run_id = ?5 AND binding_id = ?6",
                params![
                    QuestionStatus::Superseded.as_str(),
                    u64_to_i64("superseded_by_revision", new_revision)?,
                    now_ms,
                    qid,
                    run_id,
                    binding_id
                ],
            )?;
            step_seq += 1;
            Self::emit_event_tx(
                tx,
                binding_id,
                run_id,
                run.revision,
                step_seq,
                EventKind::QuestionSuperseded,
                EventPayload {
                    untrusted_data: true,
                    summary: format!(
                        "Question {qid} from revision {} superseded by revision {new_revision}",
                        run.revision
                    ),
                    question_id: Some(qid.clone()),
                    question_prompt: None,
                    answer: None,
                    instruction: None,
                    cancel_reason: None,
                },
                now_ms,
            )?;
        }

        // Mark previous revision superseded and append the new revision entry.
        tx.execute(
            "UPDATE run_revisions
             SET superseded_at_ms = ?1
             WHERE run_id = ?2 AND revision = ?3",
            params![now_ms, run_id, u64_to_i64("revision", run.revision)?],
        )?;
        tx.execute(
            "INSERT INTO run_revisions (run_id, revision, instruction, created_at_ms, superseded_at_ms)
             VALUES (?1, ?2, ?3, ?4, NULL)",
            params![
                run_id,
                u64_to_i64("revision", new_revision)?,
                instruction,
                now_ms
            ],
        )?;

        step_seq += 1;
        let next_step_at_ms = now_ms.saturating_add(run.step_delay_ms);
        tx.execute(
            "UPDATE runs
             SET revision = ?1,
                 status = ?2,
                 instruction = ?3,
                 step_seq = ?4,
                 next_step_at_ms = ?5,
                 active_question_id = NULL,
                 updated_at_ms = ?6
             WHERE run_id = ?7 AND binding_id = ?8",
            params![
                u64_to_i64("revision", new_revision)?,
                RunStatus::Running.as_str(),
                instruction,
                u64_to_i64("step_seq", step_seq)?,
                next_step_at_ms,
                now_ms,
                run_id,
                binding_id
            ],
        )?;

        Self::emit_event_tx(
            tx,
            binding_id,
            run_id,
            new_revision,
            step_seq,
            EventKind::RunSteered,
            EventPayload {
                untrusted_data: true,
                summary: format!("Run {run_id} steered to revision {new_revision}"),
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: Some(instruction.to_string()),
                cancel_reason: None,
            },
            now_ms,
        )?;

        Self::load_run_tx(tx, binding_id, run_id)
    }

    fn cancel_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> Result<Run, CoreError> {
        let run = Self::load_run_tx(tx, binding_id, run_id)?;
        if Self::is_worker_run_tx(tx, run_id)? {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "cancel",
            });
        }
        if run.status.is_terminal() {
            return Err(CoreError::InvalidRunState {
                run_id: run_id.to_string(),
                status: run.status,
                action: "cancel",
            });
        }

        tx.execute(
            "UPDATE questions
             SET status = ?1,
                 resolved_at_ms = ?2
             WHERE run_id = ?3 AND binding_id = ?4 AND status = 'pending'",
            params![
                QuestionStatus::Cancelled.as_str(),
                now_ms,
                run_id,
                binding_id
            ],
        )?;

        let next_step_seq = run.step_seq + 1;
        tx.execute(
            "UPDATE runs
             SET status = ?1,
                 step_seq = ?2,
                 next_step_at_ms = NULL,
                 active_question_id = NULL,
                 cancelled_at_ms = ?3,
                 cancel_reason = ?4,
                 updated_at_ms = ?3
             WHERE run_id = ?5 AND binding_id = ?6",
            params![
                RunStatus::Cancelled.as_str(),
                u64_to_i64("step_seq", next_step_seq)?,
                now_ms,
                reason,
                run_id,
                binding_id
            ],
        )?;

        Self::emit_event_tx(
            tx,
            binding_id,
            run_id,
            run.revision,
            next_step_seq,
            EventKind::RunCancelled,
            EventPayload {
                untrusted_data: true,
                summary: format!("Run {run_id} cancelled at revision {}", run.revision),
                question_id: None,
                question_prompt: None,
                answer: None,
                instruction: None,
                cancel_reason: reason.map(ToOwned::to_owned),
            },
            now_ms,
        )?;

        Self::load_run_tx(tx, binding_id, run_id)
    }

    fn pull_events_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        after_seq: Option<u64>,
        limit: usize,
        max_batch_bytes: Option<usize>,
        now_ms: i64,
    ) -> Result<EventBatch, CoreError> {
        let valid_budget = match max_batch_bytes {
            Some(budget) => Some(validate_batch_byte_budget(budget)?),
            None => None,
        };
        let binding = Self::load_binding_tx(tx, binding_id)?;
        let start_after = match after_seq {
            None => binding.acked_seq,
            Some(cursor) => {
                if cursor > binding.delivered_seq {
                    return Err(CoreError::UnseenEventCursor {
                        binding_id: binding_id.to_string(),
                        requested_after_seq: cursor,
                        delivered_seq: binding.delivered_seq,
                    });
                }
                cursor
            }
        };

        let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
        let mut events = {
            let mut stmt = tx.prepare(
                "SELECT event_id, binding_id, seq, run_id, revision, step_seq,
                        kind, important, payload_json, created_at_ms
                 FROM events
                 WHERE binding_id = ?1 AND seq > ?2
                 ORDER BY seq ASC
                 LIMIT ?3",
            )?;
            let mut rows = stmt.query(params![
                binding_id,
                u64_to_i64("after_seq", start_after)?,
                fetch_limit
            ])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let kind_str: String = row.get(6)?;
                let kind = EventKind::from_str(&kind_str).ok_or_else(|| {
                    CoreError::CorruptState(format!("invalid event kind `{kind_str}`"))
                })?;
                let payload_json: String = row.get(8)?;
                let payload: EventPayload = serde_json::from_str(&payload_json)?;
                items.push(Event {
                    event_id: row.get(0)?,
                    binding_id: row.get(1)?,
                    seq: i64_to_u64("seq", row.get(2)?)?,
                    run_id: row.get(3)?,
                    revision: i64_to_u64("revision", row.get(4)?)?,
                    step_seq: i64_to_u64("step_seq", row.get(5)?)?,
                    kind,
                    important: row.get::<_, i64>(7)? != 0,
                    payload,
                    created_at_ms: row.get(9)?,
                });
            }
            items
        };

        let count_trimmed = events.len() > limit;
        if count_trimmed {
            events.truncate(limit);
        }

        let batch_id = generate_id("bat");
        let mut byte_trimmed = false;

        let candidate = loop {
            let (from_seq, to_seq) = match (events.first(), events.last()) {
                (Some(first), Some(last)) => (first.seq, last.seq),
                _ => (start_after, start_after),
            };
            let new_delivered_seq = binding.delivered_seq.max(to_seq);
            let probe = EventBatch {
                batch_id: batch_id.clone(),
                binding_id: binding_id.to_string(),
                events,
                from_seq,
                to_seq,
                delivered_seq: new_delivered_seq,
                acked_seq: binding.acked_seq,
                has_more: count_trimmed || byte_trimmed,
            };

            if let Some(budget) = valid_budget {
                let size_bytes = serde_json::to_vec(&probe)?.len();
                if size_bytes > budget {
                    if probe.events.len() <= 1 {
                        return Err(CoreError::ResponseTooLarge {
                            size_bytes,
                            max_bytes: budget,
                        });
                    }
                    events = probe.events;
                    events.pop();
                    byte_trimmed = true;
                    continue;
                }
            }

            break probe;
        };

        if candidate.delivered_seq != binding.delivered_seq {
            tx.execute(
                "UPDATE bindings SET delivered_seq = ?1 WHERE binding_id = ?2",
                params![
                    u64_to_i64("delivered_seq", candidate.delivered_seq)?,
                    binding_id
                ],
            )?;
        }

        tx.execute(
            "INSERT INTO event_batches (
                batch_id, binding_id, from_seq, to_seq, event_count, created_at_ms, acked_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
            params![
                candidate.batch_id,
                binding_id,
                u64_to_i64("from_seq", candidate.from_seq)?,
                u64_to_i64("to_seq", candidate.to_seq)?,
                u64_to_i64("event_count", candidate.events.len() as u64)?,
                now_ms
            ],
        )?;

        Ok(candidate)
    }

    fn acknowledge_events_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        batch_id: &str,
        now_ms: i64,
    ) -> Result<AckResult, CoreError> {
        let binding = Self::load_binding_tx(tx, binding_id)?;

        let batch_row: Option<(String, i64, i64, i64)> = tx
            .query_row(
                "SELECT binding_id, from_seq, to_seq, event_count
                 FROM event_batches
                 WHERE batch_id = ?1",
                params![batch_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        let Some((batch_binding_id, raw_from_seq, raw_to_seq, raw_event_count)) = batch_row else {
            return Err(CoreError::InvalidAck {
                binding_id: binding_id.to_string(),
                batch_id: batch_id.to_string(),
                reason: AckErrorReason::UnknownBatch,
            });
        };

        if batch_binding_id != binding_id {
            return Err(CoreError::InvalidAck {
                binding_id: binding_id.to_string(),
                batch_id: batch_id.to_string(),
                reason: AckErrorReason::CrossBindingBatch,
            });
        }

        let batch_from_seq = i64_to_u64("from_seq", raw_from_seq)?;
        let batch_to_seq = i64_to_u64("to_seq", raw_to_seq)?;
        let event_count = i64_to_u64("event_count", raw_event_count)?;

        if batch_to_seq > binding.delivered_seq {
            return Err(CoreError::InvalidAck {
                binding_id: binding_id.to_string(),
                batch_id: batch_id.to_string(),
                reason: AckErrorReason::UnseenOrFutureEvents {
                    batch_to_seq,
                    delivered_seq: binding.delivered_seq,
                },
            });
        }

        if event_count == 0 || batch_to_seq <= binding.acked_seq {
            tx.execute(
                "UPDATE event_batches
                 SET acked_at_ms = COALESCE(acked_at_ms, ?1)
                 WHERE batch_id = ?2",
                params![now_ms, batch_id],
            )?;
            let wakeup = Self::load_wakeup_tx(tx, binding_id)?;
            return Ok(AckResult {
                binding_id: binding_id.to_string(),
                batch_id: batch_id.to_string(),
                acked_seq: binding.acked_seq,
                advanced: false,
                wakeup_status: wakeup.status,
            });
        }

        if batch_from_seq > binding.acked_seq + 1 {
            return Err(CoreError::InvalidAck {
                binding_id: binding_id.to_string(),
                batch_id: batch_id.to_string(),
                reason: AckErrorReason::SkippedEvents {
                    acked_seq: binding.acked_seq,
                    batch_from_seq,
                },
            });
        }

        let new_acked_seq = batch_to_seq;
        tx.execute(
            "UPDATE bindings SET acked_seq = ?1 WHERE binding_id = ?2",
            params![u64_to_i64("acked_seq", new_acked_seq)?, binding_id],
        )?;
        tx.execute(
            "UPDATE event_batches
             SET acked_at_ms = COALESCE(acked_at_ms, ?1)
             WHERE batch_id = ?2",
            params![now_ms, batch_id],
        )?;

        Self::reconcile_wakeup_on_ack_tx(tx, binding_id, new_acked_seq)?;
        let wakeup = Self::load_wakeup_tx(tx, binding_id)?;

        Ok(AckResult {
            binding_id: binding_id.to_string(),
            batch_id: batch_id.to_string(),
            acked_seq: new_acked_seq,
            advanced: true,
            wakeup_status: wakeup.status,
        })
    }

    fn reconcile_wakeup_on_ack_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        new_acked_seq: u64,
    ) -> Result<(), CoreError> {
        let wakeup = Self::load_wakeup_tx(tx, binding_id)?;

        let (min_unacked_important, remaining_important_count): (Option<i64>, i64) = tx.query_row(
            "SELECT MIN(seq), COUNT(*)
             FROM events
             WHERE binding_id = ?1 AND important = 1 AND seq > ?2",
            params![binding_id, u64_to_i64("acked_seq", new_acked_seq)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        let remaining_count = i64_to_u64("remaining_important_count", remaining_important_count)?;

        if remaining_count == 0 {
            if wakeup.status != WakeupStatus::Idle && wakeup.status != WakeupStatus::InFlight {
                tx.execute(
                    "UPDATE wakeups
                     SET status = ?1,
                         claim_id = NULL,
                         pending_since_seq = NULL,
                         covered_up_to_seq = ?2,
                         coalesced_count = 0,
                         last_error = NULL,
                         claimed_at_ms = NULL,
                         resolved_at_ms = NULL
                     WHERE binding_id = ?3",
                    params![
                        WakeupStatus::Idle.as_str(),
                        u64_to_i64("covered_up_to_seq", new_acked_seq)?,
                        binding_id
                    ],
                )?;
            }
            return Ok(());
        }

        let next_pending_seq = match min_unacked_important {
            Some(seq) => i64_to_u64("min_unacked_important", seq)?,
            None => return Ok(()),
        };

        match wakeup.status {
            WakeupStatus::Pending => {
                tx.execute(
                    "UPDATE wakeups
                     SET pending_since_seq = ?1,
                         coalesced_count = ?2
                     WHERE binding_id = ?3",
                    params![
                        u64_to_i64("pending_since_seq", next_pending_seq)?,
                        u64_to_i64("coalesced_count", remaining_count)?,
                        binding_id
                    ],
                )?;
            }
            WakeupStatus::Submitted | WakeupStatus::Failed | WakeupStatus::Unknown => {
                // Only release the current notice coverage once the caller has acknowledged
                // at least up to `covered_up_to_seq`. Any new unread/unacknowledged important
                // events after `new_acked_seq` then transition to `Pending`.
                if wakeup.covered_up_to_seq > 0 && new_acked_seq >= wakeup.covered_up_to_seq {
                    tx.execute(
                        "UPDATE wakeups
                         SET status = ?1,
                             claim_id = NULL,
                             pending_since_seq = ?2,
                             covered_up_to_seq = 0,
                             coalesced_count = ?3,
                             last_error = NULL,
                             claimed_at_ms = NULL,
                             resolved_at_ms = NULL
                         WHERE binding_id = ?4",
                        params![
                            WakeupStatus::Pending.as_str(),
                            u64_to_i64("pending_since_seq", next_pending_seq)?,
                            u64_to_i64("coalesced_count", remaining_count)?,
                            binding_id
                        ],
                    )?;
                } else {
                    tx.execute(
                        "UPDATE wakeups
                         SET pending_since_seq = ?1,
                             coalesced_count = ?2
                         WHERE binding_id = ?3",
                        params![
                            u64_to_i64("pending_since_seq", next_pending_seq)?,
                            u64_to_i64("coalesced_count", remaining_count)?,
                            binding_id
                        ],
                    )?;
                }
            }
            WakeupStatus::Idle | WakeupStatus::InFlight => {}
        }

        Ok(())
    }

    fn claim_wakeup_for_binding_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        now_ms: i64,
    ) -> Result<Option<WakeupClaim>, CoreError> {
        let wakeup = Self::load_wakeup_tx(tx, binding_id)?;
        if wakeup.status != WakeupStatus::Pending {
            return Ok(None);
        }

        let pending_since_seq = wakeup
            .pending_since_seq
            .unwrap_or(wakeup.latest_important_seq);
        let covered_up_to_seq = wakeup.latest_important_seq;
        let coalesced_count = wakeup.coalesced_count;
        let next_attempt_count = wakeup.attempt_count + 1;
        let claim_id = generate_id("wcl");

        tx.execute(
            "UPDATE wakeups
             SET status = ?1,
                 claim_id = ?2,
                 covered_up_to_seq = ?3,
                 attempt_count = ?4,
                 last_error = NULL,
                 claimed_at_ms = ?5,
                 resolved_at_ms = NULL
             WHERE binding_id = ?6 AND status = 'pending'",
            params![
                WakeupStatus::InFlight.as_str(),
                claim_id,
                u64_to_i64("covered_up_to_seq", covered_up_to_seq)?,
                u64_to_i64("attempt_count", next_attempt_count)?,
                now_ms,
                binding_id
            ],
        )?;

        tx.execute(
            "INSERT INTO wakeup_claims (
                claim_id, binding_id, thread_uuid, pending_since_seq, covered_up_to_seq,
                coalesced_count, status, error_detail, claimed_at_ms, resolved_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, NULL)",
            params![
                claim_id,
                binding_id,
                wakeup.thread_uuid,
                u64_to_i64("pending_since_seq", pending_since_seq)?,
                u64_to_i64("covered_up_to_seq", covered_up_to_seq)?,
                u64_to_i64("coalesced_count", coalesced_count)?,
                WakeupStatus::InFlight.as_str(),
                now_ms
            ],
        )?;

        Ok(Some(WakeupClaim {
            claim_id,
            binding_id: binding_id.to_string(),
            thread_uuid: wakeup.thread_uuid,
            message: FIXED_WAKEUP_MESSAGE.to_string(),
            pending_since_seq,
            covered_up_to_seq,
            coalesced_count,
            claimed_at_ms: now_ms,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_event_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        revision: u64,
        step_seq: u64,
        kind: EventKind,
        payload: EventPayload,
        now_ms: i64,
    ) -> Result<Event, CoreError> {
        let payload_json = serde_json::to_string(&payload)?;
        if payload_json.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Err(CoreError::EventPayloadTooLarge {
                size_bytes: payload_json.len(),
                max_bytes: MAX_EVENT_PAYLOAD_BYTES,
            });
        }

        let raw_next_seq: i64 = tx.query_row(
            "SELECT next_event_seq FROM bindings WHERE binding_id = ?1",
            params![binding_id],
            |row| row.get(0),
        )?;
        let seq = i64_to_u64("next_event_seq", raw_next_seq)?;
        let updated_next_seq = seq + 1;

        tx.execute(
            "UPDATE bindings SET next_event_seq = ?1 WHERE binding_id = ?2",
            params![u64_to_i64("next_event_seq", updated_next_seq)?, binding_id],
        )?;

        let event_id = generate_id("evt");
        let important = kind.is_important();

        tx.execute(
            "INSERT INTO events (
                event_id, binding_id, seq, run_id, revision, step_seq,
                kind, important, payload_json, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                event_id,
                binding_id,
                u64_to_i64("seq", seq)?,
                run_id,
                u64_to_i64("revision", revision)?,
                u64_to_i64("step_seq", step_seq)?,
                kind.as_str(),
                i64::from(important),
                payload_json,
                now_ms
            ],
        )?;

        if important {
            Self::record_important_event_wakeup_tx(tx, binding_id, seq)?;
        }

        Ok(Event {
            event_id,
            binding_id: binding_id.to_string(),
            seq,
            run_id: run_id.to_string(),
            revision,
            step_seq,
            kind,
            important,
            payload,
            created_at_ms: now_ms,
        })
    }

    fn record_important_event_wakeup_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        event_seq: u64,
    ) -> Result<(), CoreError> {
        let wakeup = Self::load_wakeup_tx(tx, binding_id)?;
        let next_status = match wakeup.status {
            WakeupStatus::Idle => WakeupStatus::Pending,
            other => other,
        };
        let pending_since_seq = wakeup.pending_since_seq.unwrap_or(event_seq);
        let coalesced_count = wakeup.coalesced_count + 1;

        tx.execute(
            "UPDATE wakeups
             SET status = ?1,
                 pending_since_seq = ?2,
                 latest_important_seq = ?3,
                 coalesced_count = ?4
             WHERE binding_id = ?5",
            params![
                next_status.as_str(),
                u64_to_i64("pending_since_seq", pending_since_seq)?,
                u64_to_i64("latest_important_seq", event_seq)?,
                u64_to_i64("coalesced_count", coalesced_count)?,
                binding_id
            ],
        )?;
        Ok(())
    }

    fn migrate_legacy_command_dedup_tx(tx: &Transaction<'_>) -> Result<(), CoreError> {
        let mut select_stmt = tx.prepare(
            "SELECT rowid, binding_id, command_key, response_json
             FROM command_dedup
             WHERE rowid > ?1
             ORDER BY rowid ASC
             LIMIT 1",
        )?;
        let mut update_stmt = tx.prepare(
            "UPDATE command_dedup
             SET response_json = ?1
             WHERE rowid = ?2 AND binding_id = ?3 AND command_key = ?4",
        )?;

        let mut after_rowid: i64 = 0;
        loop {
            let next_row: Option<(i64, String, String, String)> = select_stmt
                .query_row(params![after_rowid], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .optional()?;

            let Some((rowid, binding_id, command_key, response_json)) = next_row else {
                break;
            };

            let (compact_output, needs_rewrite) = parse_dedup_command_output(&response_json)?;
            let compact_json = enforce_compact_response_budget(&compact_output)?;
            if needs_rewrite {
                update_stmt.execute(params![compact_json, rowid, binding_id, command_key])?;
            }
            after_rowid = rowid;
        }

        Ok(())
    }

    pub(crate) fn check_dedup(
        tx: &Transaction<'_>,
        binding_id: &str,
        command_key: &str,
        operation: &str,
        payload_hash: &str,
    ) -> Result<Option<CommandOutput>, CoreError> {
        let existing: Option<(String, String, String)> = tx
            .query_row(
                "SELECT operation, payload_hash, response_json
                 FROM command_dedup
                 WHERE binding_id = ?1 AND command_key = ?2",
                params![binding_id, command_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        let Some((stored_op, stored_hash, response_json)) = existing else {
            return Ok(None);
        };

        if stored_op != operation || stored_hash != payload_hash {
            return Err(CoreError::IdempotencyConflict {
                binding_id: binding_id.to_string(),
                command_key: command_key.to_string(),
            });
        }

        let (output, _) = parse_dedup_command_output(&response_json)?;
        enforce_compact_response_budget(&output)?;
        Ok(Some(output))
    }

    pub(crate) fn record_dedup(
        tx: &Transaction<'_>,
        binding_id: &str,
        command_key: &str,
        operation: &str,
        payload_hash: &str,
        output: &CommandOutput,
        now_ms: i64,
    ) -> Result<(), CoreError> {
        let response_json = enforce_compact_response_budget(output)?;
        tx.execute(
            "INSERT INTO command_dedup (
                binding_id, command_key, operation, payload_hash, response_json, created_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                binding_id,
                command_key,
                operation,
                payload_hash,
                response_json,
                now_ms
            ],
        )?;
        Ok(())
    }

    pub(crate) fn ensure_binding_exists(
        tx: &Transaction<'_>,
        binding_id: &str,
    ) -> Result<(), CoreError> {
        Self::ensure_binding_exists_conn(tx, binding_id)
    }

    pub(crate) fn ensure_binding_exists_conn(
        conn: &Connection,
        binding_id: &str,
    ) -> Result<(), CoreError> {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM bindings WHERE binding_id = ?1",
                params![binding_id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(CoreError::BindingNotFound {
                binding_id: binding_id.to_string(),
            });
        }
        Ok(())
    }

    fn load_binding_tx(tx: &Transaction<'_>, binding_id: &str) -> Result<Binding, CoreError> {
        Self::load_binding_conn(tx, binding_id)
    }

    fn load_binding_conn(conn: &Connection, binding_id: &str) -> Result<Binding, CoreError> {
        let row: Option<(String, String, String, i64, i64)> = conn
            .query_row(
                "SELECT binding_id, thread_uuid, label, delivered_seq, acked_seq
                 FROM bindings
                 WHERE binding_id = ?1",
                params![binding_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;

        let Some((id, thread_uuid, label, delivered, acked)) = row else {
            return Err(CoreError::BindingNotFound {
                binding_id: binding_id.to_string(),
            });
        };

        Ok(Binding {
            binding_id: id,
            thread_uuid,
            label,
            delivered_seq: i64_to_u64("delivered_seq", delivered)?,
            acked_seq: i64_to_u64("acked_seq", acked)?,
        })
    }

    fn load_wakeup_tx(tx: &Transaction<'_>, binding_id: &str) -> Result<WakeupNotice, CoreError> {
        Self::load_wakeup_conn(tx, binding_id)
    }

    fn load_wakeup_conn(conn: &Connection, binding_id: &str) -> Result<WakeupNotice, CoreError> {
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            String,
            Option<String>,
            Option<i64>,
            i64,
            i64,
            i64,
            i64,
            Option<String>,
            Option<i64>,
            Option<i64>,
        )> = conn
            .query_row(
                "SELECT binding_id, thread_uuid, status, claim_id, pending_since_seq,
                        latest_important_seq, covered_up_to_seq, coalesced_count, attempt_count,
                        last_error, claimed_at_ms, resolved_at_ms
                 FROM wakeups
                 WHERE binding_id = ?1",
                params![binding_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                        r.get(11)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            b_id,
            thread_uuid,
            status_raw,
            claim_id,
            pending_since_raw,
            latest_imp_raw,
            covered_raw,
            coalesced_raw,
            attempt_raw,
            last_error,
            claimed_at_ms,
            resolved_at_ms,
        )) = row
        else {
            return Err(CoreError::BindingNotFound {
                binding_id: binding_id.to_string(),
            });
        };

        let status = WakeupStatus::from_str(&status_raw).ok_or_else(|| {
            CoreError::CorruptState(format!("invalid wakeup status `{status_raw}`"))
        })?;
        let pending_since_seq = match pending_since_raw {
            Some(v) => Some(i64_to_u64("pending_since_seq", v)?),
            None => None,
        };

        Ok(WakeupNotice {
            binding_id: b_id,
            thread_uuid,
            status,
            claim_id,
            message: WAKEUP_NOTICE_STATUS_MESSAGE.to_string(),
            pending_since_seq,
            latest_important_seq: i64_to_u64("latest_important_seq", latest_imp_raw)?,
            covered_up_to_seq: i64_to_u64("covered_up_to_seq", covered_raw)?,
            coalesced_count: i64_to_u64("coalesced_count", coalesced_raw)?,
            attempt_count: i64_to_u64("attempt_count", attempt_raw)?,
            last_error,
            claimed_at_ms,
            resolved_at_ms,
        })
    }

    fn load_question_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        question_id: &str,
    ) -> Result<Question, CoreError> {
        Self::load_question_conn(tx, binding_id, run_id, question_id)
    }

    fn load_question_conn(
        conn: &Connection,
        binding_id: &str,
        run_id: &str,
        question_id: &str,
    ) -> Result<Question, CoreError> {
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            String,
            i64,
            String,
            String,
            Option<String>,
            Option<i64>,
            i64,
            Option<i64>,
        )> = conn
            .query_row(
                "SELECT question_id, binding_id, run_id, revision, status, prompt,
                        answer, superseded_by_revision, created_at_ms, resolved_at_ms
                 FROM questions
                 WHERE question_id = ?1 AND run_id = ?2 AND binding_id = ?3",
                params![question_id, run_id, binding_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            q_id,
            b_id,
            r_id,
            rev_raw,
            status_raw,
            prompt,
            answer,
            superseded_raw,
            created_at_ms,
            resolved_at_ms,
        )) = row
        else {
            return Err(CoreError::QuestionNotFound {
                binding_id: binding_id.to_string(),
                run_id: run_id.to_string(),
                question_id: question_id.to_string(),
            });
        };

        let status = QuestionStatus::from_str(&status_raw).ok_or_else(|| {
            CoreError::CorruptState(format!("invalid question status `{status_raw}`"))
        })?;
        let superseded_by_revision = match superseded_raw {
            Some(v) => Some(i64_to_u64("superseded_by_revision", v)?),
            None => None,
        };

        Ok(Question {
            untrusted_data: true,
            question_id: q_id,
            binding_id: b_id,
            run_id: r_id,
            revision: i64_to_u64("revision", rev_raw)?,
            status,
            prompt,
            answer,
            superseded_by_revision,
            created_at_ms,
            resolved_at_ms,
        })
    }

    pub(crate) fn load_run_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
    ) -> Result<Run, CoreError> {
        Self::load_run_conn(tx, binding_id, run_id)
    }

    fn load_run_conn(conn: &Connection, binding_id: &str, run_id: &str) -> Result<Run, CoreError> {
        #[allow(clippy::type_complexity)]
        let row: Option<(
            String,
            String,
            i64,
            String,
            String,
            i64,
            i64,
            Option<i64>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            Option<String>,
        )> = conn
            .query_row(
                "SELECT run_id, binding_id, revision, status, instruction, step_seq,
                        step_delay_ms, next_step_at_ms, active_question_id, started_at_ms,
                        updated_at_ms, completed_at_ms, cancelled_at_ms, cancel_reason
                 FROM runs
                 WHERE run_id = ?1 AND binding_id = ?2",
                params![run_id, binding_id],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                        r.get(11)?,
                        r.get(12)?,
                        r.get(13)?,
                    ))
                },
            )
            .optional()?;

        let Some((
            r_id,
            b_id,
            rev_raw,
            status_raw,
            instruction,
            step_raw,
            step_delay_ms,
            next_step_at_ms,
            active_question_id,
            started_at_ms,
            updated_at_ms,
            completed_at_ms,
            cancelled_at_ms,
            cancel_reason,
        )) = row
        else {
            return Err(CoreError::RunNotFound {
                binding_id: binding_id.to_string(),
                run_id: run_id.to_string(),
            });
        };

        let status = RunStatus::from_str(&status_raw)
            .ok_or_else(|| CoreError::CorruptState(format!("invalid run status `{status_raw}`")))?;

        let active_question = match active_question_id.as_deref() {
            Some(qid) => Some(Self::load_question_conn(conn, binding_id, &r_id, qid)?),
            None => None,
        };

        Ok(Run {
            untrusted_data: true,
            run_id: r_id,
            binding_id: b_id,
            revision: i64_to_u64("revision", rev_raw)?,
            status,
            instruction,
            step_seq: i64_to_u64("step_seq", step_raw)?,
            step_delay_ms,
            next_step_at_ms,
            active_question_id,
            active_question,
            started_at_ms,
            updated_at_ms,
            completed_at_ms,
            cancelled_at_ms,
            cancel_reason,
        })
    }

    fn load_run_current_revision_conn(
        conn: &Connection,
        binding_id: &str,
        run_id: &str,
    ) -> Result<u64, CoreError> {
        let rev_raw: Option<i64> = conn
            .query_row(
                "SELECT revision FROM runs WHERE run_id = ?1 AND binding_id = ?2",
                params![run_id, binding_id],
                |row| row.get(0),
            )
            .optional()?;
        match rev_raw {
            Some(rev) => i64_to_u64("revision", rev),
            None => Err(CoreError::RunNotFound {
                binding_id: binding_id.to_string(),
                run_id: run_id.to_string(),
            }),
        }
    }

    fn list_runs_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RunListPage, CoreError> {
        Self::list_runs_conn(tx, binding_id, cursor, limit)
    }

    fn list_runs_conn(
        conn: &Connection,
        binding_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RunListPage, CoreError> {
        let (after_rowid, hwm_rowid) = match cursor {
            Some(raw_cursor) => {
                let parsed =
                    parse_and_validate_cursor(raw_cursor, CursorKind::Runs, binding_id, None)?;
                let anchor_count: i64 = conn.query_row(
                    "SELECT COUNT(*)
                     FROM runs
                     WHERE binding_id = ?1 AND rowid IN (?2, ?3)",
                    params![
                        binding_id,
                        u64_to_i64("cursor", parsed.after)?,
                        u64_to_i64("cursor", parsed.high_water_mark)?
                    ],
                    |row| row.get(0),
                )?;
                if anchor_count != 2 {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason: "cursor references unknown or out-of-range run position for this binding".to_string(),
                    });
                }
                (parsed.after, parsed.high_water_mark)
            }
            None => {
                let max_rowid: Option<i64> = conn.query_row(
                    "SELECT MAX(rowid) FROM runs WHERE binding_id = ?1",
                    params![binding_id],
                    |row| row.get(0),
                )?;
                let Some(max_raw) = max_rowid else {
                    return Ok(RunListPage {
                        binding_id: binding_id.to_string(),
                        runs: Vec::new(),
                        next_cursor: None,
                        has_more: false,
                        high_water_mark_rowid: 0,
                    });
                };
                (0, i64_to_u64("high_water_mark_rowid", max_raw)?)
            }
        };

        let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
        let candidate_rows: Vec<(u64, String)> = {
            let mut stmt = conn.prepare(
                "SELECT rowid, run_id
                 FROM runs
                 WHERE binding_id = ?1 AND rowid > ?2 AND rowid <= ?3
                 ORDER BY rowid ASC
                 LIMIT ?4",
            )?;
            let mut rows = stmt.query(params![
                binding_id,
                u64_to_i64("after_rowid", after_rowid)?,
                u64_to_i64("hwm_rowid", hwm_rowid)?,
                fetch_limit
            ])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                items.push((i64_to_u64("rowid", row.get(0)?)?, row.get(1)?));
            }
            items
        };

        let db_has_more = candidate_rows.len() > limit;
        let mut included_runs = Vec::with_capacity(candidate_rows.len().min(limit));
        let mut last_included_rowid = after_rowid;
        let mut stopped_by_budget = false;

        for (rowid, run_id) in candidate_rows.into_iter().take(limit) {
            let run = Self::load_run_conn(conn, binding_id, &run_id)?;
            included_runs.push(run);

            let probe_cursor = Some(encode_runs_cursor(binding_id, rowid, hwm_rowid));
            let probe_output = CommandOutput::RunList(RunListPage {
                binding_id: binding_id.to_string(),
                runs: included_runs.clone(),
                next_cursor: probe_cursor,
                has_more: true,
                high_water_mark_rowid: hwm_rowid,
            });
            let size_bytes = serialized_output_budget_bytes(&probe_output)?;
            if size_bytes > MAX_PAGE_RESPONSE_BYTES {
                if included_runs.len() == 1 {
                    return Err(CoreError::ResponseTooLarge {
                        size_bytes,
                        max_bytes: MAX_PAGE_RESPONSE_BYTES,
                    });
                }
                included_runs.pop();
                stopped_by_budget = true;
                break;
            }
            last_included_rowid = rowid;
        }

        let has_more = stopped_by_budget || db_has_more;
        let next_cursor = if has_more {
            Some(encode_runs_cursor(
                binding_id,
                last_included_rowid,
                hwm_rowid,
            ))
        } else {
            None
        };

        Ok(RunListPage {
            binding_id: binding_id.to_string(),
            runs: included_runs,
            next_cursor,
            has_more,
            high_water_mark_rowid: hwm_rowid,
        })
    }

    fn list_run_revisions_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RevisionHistoryPage, CoreError> {
        Self::list_run_revisions_conn(tx, binding_id, run_id, cursor, limit)
    }

    fn list_run_revisions_conn(
        conn: &Connection,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RevisionHistoryPage, CoreError> {
        let current_revision = Self::load_run_current_revision_conn(conn, binding_id, run_id)?;
        let (after_revision, hwm_revision) = match cursor {
            Some(raw_cursor) => {
                let parsed = parse_and_validate_cursor(
                    raw_cursor,
                    CursorKind::Revisions,
                    binding_id,
                    Some(run_id),
                )?;
                if parsed.high_water_mark > current_revision {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason: format!(
                            "cursor high_water_mark ({}) exceeds run current_revision ({current_revision})",
                            parsed.high_water_mark
                        ),
                    });
                }
                let anchor_count: i64 = conn.query_row(
                    "SELECT COUNT(*)
                     FROM run_revisions
                     WHERE run_id = ?1 AND revision IN (?2, ?3)",
                    params![
                        run_id,
                        u64_to_i64("cursor", parsed.after)?,
                        u64_to_i64("cursor", parsed.high_water_mark)?
                    ],
                    |row| row.get(0),
                )?;
                if anchor_count != 2 {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason: "cursor references unknown revision position for this run"
                            .to_string(),
                    });
                }
                (parsed.after, parsed.high_water_mark)
            }
            None => (0, current_revision),
        };

        let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
        let candidate_revisions: Vec<RunRevision> = {
            let mut stmt = conn.prepare(
                "SELECT revision, instruction, created_at_ms, superseded_at_ms
                 FROM run_revisions
                 WHERE run_id = ?1 AND revision > ?2 AND revision <= ?3
                 ORDER BY revision ASC
                 LIMIT ?4",
            )?;
            let mut rows = stmt.query(params![
                run_id,
                u64_to_i64("after_revision", after_revision)?,
                u64_to_i64("hwm_revision", hwm_revision)?,
                fetch_limit
            ])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                items.push(RunRevision {
                    untrusted_data: true,
                    revision: i64_to_u64("revision", row.get(0)?)?,
                    instruction: row.get(1)?,
                    created_at_ms: row.get(2)?,
                    superseded_at_ms: row.get(3)?,
                });
            }
            items
        };

        let db_has_more = candidate_revisions.len() > limit;
        let mut included_revisions = Vec::with_capacity(candidate_revisions.len().min(limit));
        let mut last_included_revision = after_revision;
        let mut stopped_by_budget = false;

        for rev in candidate_revisions.into_iter().take(limit) {
            let rev_num = rev.revision;
            included_revisions.push(rev);

            let probe_cursor = Some(encode_revisions_cursor(
                binding_id,
                run_id,
                rev_num,
                hwm_revision,
            ));
            let probe_output = CommandOutput::RevisionHistory(RevisionHistoryPage {
                binding_id: binding_id.to_string(),
                run_id: run_id.to_string(),
                current_revision,
                high_water_mark_revision: hwm_revision,
                revisions: included_revisions.clone(),
                next_cursor: probe_cursor,
                has_more: true,
            });
            let size_bytes = serialized_output_budget_bytes(&probe_output)?;
            if size_bytes > MAX_PAGE_RESPONSE_BYTES {
                if included_revisions.len() == 1 {
                    return Err(CoreError::ResponseTooLarge {
                        size_bytes,
                        max_bytes: MAX_PAGE_RESPONSE_BYTES,
                    });
                }
                included_revisions.pop();
                stopped_by_budget = true;
                break;
            }
            last_included_revision = rev_num;
        }

        let has_more = stopped_by_budget || db_has_more;
        let next_cursor = if has_more {
            Some(encode_revisions_cursor(
                binding_id,
                run_id,
                last_included_revision,
                hwm_revision,
            ))
        } else {
            None
        };

        Ok(RevisionHistoryPage {
            binding_id: binding_id.to_string(),
            run_id: run_id.to_string(),
            current_revision,
            high_water_mark_revision: hwm_revision,
            revisions: included_revisions,
            next_cursor,
            has_more,
        })
    }

    fn list_run_questions_tx(
        tx: &Transaction<'_>,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<QuestionHistoryPage, CoreError> {
        Self::list_run_questions_conn(tx, binding_id, run_id, cursor, limit)
    }

    fn list_run_questions_conn(
        conn: &Connection,
        binding_id: &str,
        run_id: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<QuestionHistoryPage, CoreError> {
        let current_revision = Self::load_run_current_revision_conn(conn, binding_id, run_id)?;
        let (after_rowid, hwm_rowid) = match cursor {
            Some(raw_cursor) => {
                let parsed = parse_and_validate_cursor(
                    raw_cursor,
                    CursorKind::Questions,
                    binding_id,
                    Some(run_id),
                )?;
                let anchor_count: i64 = conn.query_row(
                    "SELECT COUNT(*)
                     FROM questions
                     WHERE binding_id = ?1 AND run_id = ?2 AND rowid IN (?3, ?4)",
                    params![
                        binding_id,
                        run_id,
                        u64_to_i64("cursor", parsed.after)?,
                        u64_to_i64("cursor", parsed.high_water_mark)?
                    ],
                    |row| row.get(0),
                )?;
                if anchor_count != 2 {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason: "cursor references unknown or out-of-range question position for this run".to_string(),
                    });
                }
                (parsed.after, parsed.high_water_mark)
            }
            None => {
                let max_rowid: Option<i64> = conn.query_row(
                    "SELECT MAX(rowid)
                     FROM questions
                     WHERE binding_id = ?1 AND run_id = ?2",
                    params![binding_id, run_id],
                    |row| row.get(0),
                )?;
                let Some(max_raw) = max_rowid else {
                    return Ok(QuestionHistoryPage {
                        binding_id: binding_id.to_string(),
                        run_id: run_id.to_string(),
                        current_revision,
                        high_water_mark_rowid: 0,
                        questions: Vec::new(),
                        next_cursor: None,
                        has_more: false,
                    });
                };
                (0, i64_to_u64("high_water_mark_rowid", max_raw)?)
            }
        };

        let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
        let candidate_questions: Vec<(u64, Question)> = {
            let mut stmt = conn.prepare(
                "SELECT rowid, question_id, binding_id, run_id, revision, status, prompt,
                        answer, superseded_by_revision, created_at_ms, resolved_at_ms
                 FROM questions
                 WHERE binding_id = ?1 AND run_id = ?2 AND rowid > ?3 AND rowid <= ?4
                 ORDER BY rowid ASC
                 LIMIT ?5",
            )?;
            let mut rows = stmt.query(params![
                binding_id,
                run_id,
                u64_to_i64("after_rowid", after_rowid)?,
                u64_to_i64("hwm_rowid", hwm_rowid)?,
                fetch_limit
            ])?;
            let mut items = Vec::new();
            while let Some(r) = rows.next()? {
                let rowid = i64_to_u64("rowid", r.get(0)?)?;
                let q_status_raw: String = r.get(5)?;
                let q_status = QuestionStatus::from_str(&q_status_raw).ok_or_else(|| {
                    CoreError::CorruptState(format!("invalid question status `{q_status_raw}`"))
                })?;
                let superseded_raw: Option<i64> = r.get(8)?;
                let superseded_by_revision = match superseded_raw {
                    Some(v) => Some(i64_to_u64("superseded_by_revision", v)?),
                    None => None,
                };
                items.push((
                    rowid,
                    Question {
                        untrusted_data: true,
                        question_id: r.get(1)?,
                        binding_id: r.get(2)?,
                        run_id: r.get(3)?,
                        revision: i64_to_u64("revision", r.get(4)?)?,
                        status: q_status,
                        prompt: r.get(6)?,
                        answer: r.get(7)?,
                        superseded_by_revision,
                        created_at_ms: r.get(9)?,
                        resolved_at_ms: r.get(10)?,
                    },
                ));
            }
            items
        };

        let db_has_more = candidate_questions.len() > limit;
        let mut included_questions = Vec::with_capacity(candidate_questions.len().min(limit));
        let mut last_included_rowid = after_rowid;
        let mut stopped_by_budget = false;

        for (rowid, question) in candidate_questions.into_iter().take(limit) {
            included_questions.push(question);

            let probe_cursor = Some(encode_questions_cursor(
                binding_id, run_id, rowid, hwm_rowid,
            ));
            let probe_output = CommandOutput::QuestionHistory(QuestionHistoryPage {
                binding_id: binding_id.to_string(),
                run_id: run_id.to_string(),
                current_revision,
                high_water_mark_rowid: hwm_rowid,
                questions: included_questions.clone(),
                next_cursor: probe_cursor,
                has_more: true,
            });
            let size_bytes = serialized_output_budget_bytes(&probe_output)?;
            if size_bytes > MAX_PAGE_RESPONSE_BYTES {
                if included_questions.len() == 1 {
                    return Err(CoreError::ResponseTooLarge {
                        size_bytes,
                        max_bytes: MAX_PAGE_RESPONSE_BYTES,
                    });
                }
                included_questions.pop();
                stopped_by_budget = true;
                break;
            }
            last_included_rowid = rowid;
        }

        let has_more = stopped_by_budget || db_has_more;
        let next_cursor = if has_more {
            Some(encode_questions_cursor(
                binding_id,
                run_id,
                last_included_rowid,
                hwm_rowid,
            ))
        } else {
            None
        };

        Ok(QuestionHistoryPage {
            binding_id: binding_id.to_string(),
            run_id: run_id.to_string(),
            current_revision,
            high_water_mark_rowid: hwm_rowid,
            questions: included_questions,
            next_cursor,
            has_more,
        })
    }

    fn list_activity_conn(
        conn: &Connection,
        binding_id: &str,
        after_seq: Option<u64>,
        cursor: Option<&str>,
        limit: usize,
        run_id: Option<&str>,
        kind: Option<EventKind>,
    ) -> Result<ActivityPage, CoreError> {
        let max_seq_raw: Option<i64> = conn.query_row(
            "SELECT MAX(seq) FROM events WHERE binding_id = ?1",
            params![binding_id],
            |row| row.get(0),
        )?;
        let current_max_seq = match max_seq_raw {
            Some(raw) => i64_to_u64("high_water_mark_seq", raw)?,
            None => 0,
        };

        let (effective_after_seq, hwm_seq) = match cursor {
            Some(raw_cursor) => {
                let parsed =
                    parse_and_validate_activity_cursor(raw_cursor, binding_id, run_id, kind)?;
                if parsed.high_water_mark > current_max_seq {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason: format!(
                            "cursor high_water_mark ({}) exceeds binding high_water_mark_seq ({current_max_seq})",
                            parsed.high_water_mark
                        ),
                    });
                }
                let anchor_count: i64 = conn.query_row(
                    "SELECT COUNT(*)
                     FROM events
                     WHERE binding_id = ?1 AND seq IN (?2, ?3)",
                    params![
                        binding_id,
                        u64_to_i64("cursor", parsed.after)?,
                        u64_to_i64("cursor", parsed.high_water_mark)?
                    ],
                    |row| row.get(0),
                )?;
                if anchor_count != 2 {
                    return Err(CoreError::Validation {
                        field: "cursor",
                        reason:
                            "cursor references unknown or out-of-range event sequence for this binding"
                                .to_string(),
                    });
                }
                (parsed.after, parsed.high_water_mark)
            }
            None => {
                let start_after = after_seq.unwrap_or(0);
                let _ = u64_to_i64("after_seq", start_after)?;
                if start_after > current_max_seq {
                    return Err(CoreError::Validation {
                        field: "after_seq",
                        reason: format!(
                            "after_seq ({start_after}) exceeds binding high_water_mark_seq ({current_max_seq})"
                        ),
                    });
                }
                if current_max_seq == 0 || start_after == current_max_seq {
                    return Ok(ActivityPage {
                        binding_id: binding_id.to_string(),
                        events: Vec::new(),
                        next_cursor: None,
                        has_more: false,
                        high_water_mark_seq: current_max_seq,
                        after_seq: start_after,
                    });
                }
                (start_after, current_max_seq)
            }
        };

        let fetch_limit = u64_to_i64("limit", (limit + 1) as u64)?;
        let kind_filter = kind.map(EventKind::as_str);
        let candidate_events: Vec<Event> = {
            let mut stmt = conn.prepare(
                "SELECT event_id, binding_id, seq, run_id, revision, step_seq,
                        kind, important, payload_json, created_at_ms
                 FROM events
                 WHERE binding_id = ?1
                   AND seq > ?2
                   AND seq <= ?3
                   AND (?4 IS NULL OR run_id = ?4)
                   AND (?5 IS NULL OR kind = ?5)
                 ORDER BY seq ASC
                 LIMIT ?6",
            )?;
            let mut rows = stmt.query(params![
                binding_id,
                u64_to_i64("after_seq", effective_after_seq)?,
                u64_to_i64("hwm_seq", hwm_seq)?,
                run_id,
                kind_filter,
                fetch_limit
            ])?;
            let mut items = Vec::new();
            while let Some(row) = rows.next()? {
                let kind_str: String = row.get(6)?;
                let event_kind = EventKind::from_str(&kind_str).ok_or_else(|| {
                    CoreError::CorruptState(format!("invalid event kind `{kind_str}`"))
                })?;
                let payload_json: String = row.get(8)?;
                let payload: EventPayload = serde_json::from_str(&payload_json)?;
                items.push(Event {
                    event_id: row.get(0)?,
                    binding_id: row.get(1)?,
                    seq: i64_to_u64("seq", row.get(2)?)?,
                    run_id: row.get(3)?,
                    revision: i64_to_u64("revision", row.get(4)?)?,
                    step_seq: i64_to_u64("step_seq", row.get(5)?)?,
                    kind: event_kind,
                    important: row.get::<_, i64>(7)? != 0,
                    payload,
                    created_at_ms: row.get(9)?,
                });
            }
            items
        };

        let db_has_more = candidate_events.len() > limit;
        let mut included_events = Vec::with_capacity(candidate_events.len().min(limit));
        let mut last_included_seq = effective_after_seq;
        let mut stopped_by_budget = false;

        for event in candidate_events.into_iter().take(limit) {
            let event_seq = event.seq;
            included_events.push(event);

            let probe_cursor = Some(encode_activity_cursor(
                binding_id, run_id, kind, event_seq, hwm_seq,
            ));
            let probe_output = CommandOutput::Activity(ActivityPage {
                binding_id: binding_id.to_string(),
                events: included_events.clone(),
                next_cursor: probe_cursor,
                has_more: true,
                high_water_mark_seq: hwm_seq,
                after_seq: effective_after_seq,
            });
            let size_bytes = serialized_output_budget_bytes(&probe_output)?;
            if size_bytes > MAX_PAGE_RESPONSE_BYTES {
                if included_events.len() == 1 {
                    return Err(CoreError::ResponseTooLarge {
                        size_bytes,
                        max_bytes: MAX_PAGE_RESPONSE_BYTES,
                    });
                }
                included_events.pop();
                stopped_by_budget = true;
                break;
            }
            last_included_seq = event_seq;
        }

        let has_more = stopped_by_budget || db_has_more;
        let next_cursor = if has_more {
            Some(encode_activity_cursor(
                binding_id,
                run_id,
                kind,
                last_included_seq,
                hwm_seq,
            ))
        } else {
            None
        };

        Ok(ActivityPage {
            binding_id: binding_id.to_string(),
            events: included_events,
            next_cursor,
            has_more,
            high_water_mark_seq: hwm_seq,
            after_seq: effective_after_seq,
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
enum StoredDedupOutput {
    Run(Box<LegacyStoredRun>),
    Worker(Box<WorkerSnapshot>),
    AckResult(AckResult),
}

#[derive(Default)]
struct IgnoredFieldPresence(bool);

impl<'de> serde::Deserialize<'de> for IgnoredFieldPresence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Self(true))
    }
}

#[derive(Default)]
struct LegacyQuestionsField {
    present: bool,
    pending_question: Option<Question>,
    last_question: Option<Question>,
}

impl<'de> serde::Deserialize<'de> for LegacyQuestionsField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct QuestionsVisitor;

        impl<'de> serde::de::Visitor<'de> for QuestionsVisitor {
            type Value = LegacyQuestionsField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an optional array of questions")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(LegacyQuestionsField {
                    present: true,
                    pending_question: None,
                    last_question: None,
                })
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(LegacyQuestionsField {
                    present: true,
                    pending_question: None,
                    last_question: None,
                })
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut pending_question = None;
                let mut last_question = None;
                while let Some(q) = seq.next_element::<Question>()? {
                    if q.status == QuestionStatus::Pending {
                        pending_question = Some(q.clone());
                    }
                    last_question = Some(q);
                }
                Ok(LegacyQuestionsField {
                    present: true,
                    pending_question,
                    last_question,
                })
            }
        }

        deserializer.deserialize_option(QuestionsVisitor)
    }
}

#[derive(serde::Deserialize)]
struct LegacyStoredRun {
    #[serde(default)]
    untrusted_data: Option<bool>,
    run_id: String,
    binding_id: String,
    revision: u64,
    status: RunStatus,
    instruction: String,
    step_seq: u64,
    step_delay_ms: i64,
    next_step_at_ms: Option<i64>,
    active_question_id: Option<String>,
    #[serde(default)]
    active_question: Option<Question>,
    started_at_ms: i64,
    updated_at_ms: i64,
    completed_at_ms: Option<i64>,
    cancelled_at_ms: Option<i64>,
    cancel_reason: Option<String>,
    #[serde(default)]
    revision_history: IgnoredFieldPresence,
    #[serde(default)]
    questions: LegacyQuestionsField,
}

fn parse_dedup_command_output(response_json: &str) -> Result<(CommandOutput, bool), CoreError> {
    let parsed: StoredDedupOutput = serde_json::from_str(response_json)?;
    match parsed {
        StoredDedupOutput::AckResult(ack) => Ok((CommandOutput::AckResult(ack), false)),
        StoredDedupOutput::Worker(mut worker) => {
            let needs_rewrite = !worker.untrusted_data;
            worker.untrusted_data = true;
            Ok((CommandOutput::Worker(worker), needs_rewrite))
        }
        StoredDedupOutput::Run(legacy) => {
            let legacy = *legacy;
            let needs_rewrite = legacy.revision_history.0
                || legacy.questions.present
                || legacy.untrusted_data != Some(true);
            let LegacyQuestionsField {
                pending_question,
                last_question,
                ..
            } = legacy.questions;
            let active_question = legacy.active_question.or_else(|| {
                let target_id = legacy.active_question_id.as_deref()?;
                pending_question
                    .filter(|q| q.question_id == target_id)
                    .or_else(|| last_question.filter(|q| q.question_id == target_id))
            });
            let compact_run = Run {
                untrusted_data: true,
                run_id: legacy.run_id,
                binding_id: legacy.binding_id,
                revision: legacy.revision,
                status: legacy.status,
                instruction: legacy.instruction,
                step_seq: legacy.step_seq,
                step_delay_ms: legacy.step_delay_ms,
                next_step_at_ms: legacy.next_step_at_ms,
                active_question_id: legacy.active_question_id,
                active_question,
                started_at_ms: legacy.started_at_ms,
                updated_at_ms: legacy.updated_at_ms,
                completed_at_ms: legacy.completed_at_ms,
                cancelled_at_ms: legacy.cancelled_at_ms,
                cancel_reason: legacy.cancel_reason,
            };
            Ok((CommandOutput::Run(Box::new(compact_run)), needs_rewrite))
        }
    }
}

fn serialized_output_budget_bytes(output: &CommandOutput) -> Result<usize, CoreError> {
    estimate_wire_response_bytes(output)
}

pub(crate) fn enforce_compact_response_budget(output: &CommandOutput) -> Result<String, CoreError> {
    let json = serde_json::to_string(output)?;
    let second_level_string = serde_json::to_string(&json)?;
    let size_bytes = json
        .len()
        .saturating_add(second_level_string.len())
        .saturating_add(RESPONSE_WRAPPER_OVERHEAD_BYTES);
    if size_bytes > MAX_COMPACT_RESPONSE_BYTES {
        return Err(CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes: MAX_COMPACT_RESPONSE_BYTES,
        });
    }
    Ok(json)
}

pub(crate) fn generate_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub(crate) fn hash_dedup_payload<T: serde::Serialize>(value: &T) -> Result<String, CoreError> {
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    Ok(hex)
}

pub(crate) fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, CoreError> {
    i64::try_from(value).map_err(|_| CoreError::Validation {
        field,
        reason: format!("{field} value {value} exceeds i64::MAX"),
    })
}

pub(crate) fn i64_to_u64(field: &'static str, value: i64) -> Result<u64, CoreError> {
    u64::try_from(value).map_err(|_| {
        CoreError::CorruptState(format!("negative integer {value} stored in `{field}`"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{
        WorkerAcceptanceState, WorkerCompletionRecord, WorkerOutcomeSummary, WorkerStatus,
        WorkerWorktreeUpdate,
    };

    const THREAD_A: &str = "11111111-1111-4111-8111-111111111111";
    const THREAD_B: &str = "22222222-2222-4222-8222-222222222222";
    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn open_temp_core() -> (tempfile::TempDir, Core) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("threadlane.db");
        let core = Core::open(&db).expect("open core");
        (dir, core)
    }

    fn default_outcome(cancelled: bool) -> WorkerOutcomeSummary {
        WorkerOutcomeSummary {
            exit_code: Some(if cancelled { 130 } else { 0 }),
            signal: None,
            duration_ms: Some(15),
            timed_out: false,
            cancelled,
            cancel_requested_before_completion: cancelled,
            missing_model_evidence: false,
            missing_effort_evidence: false,
            provider_error: false,
            incomplete_cleanup: false,
            rejected_by_diff_gate: false,
            stdout_truncated: false,
            stderr_truncated: false,
            acceptance_state: if cancelled {
                WorkerAcceptanceState::ExecutionFailed
            } else {
                WorkerAcceptanceState::ReadyForReview
            },
            changed_paths_count: 0,
            changed_paths_preview: Vec::new(),
            violation_count: 0,
            violation_summaries: Vec::new(),
            final_summary: Some("ok".to_string()),
            failure_reason: None,
            report_artifact_id: None,
        }
    }

    #[test]
    fn inspect_runtime_idle_counts_all_bindings_runs_workers_and_wakeups() {
        let (_dir, mut core) = open_temp_core();
        assert_eq!(
            core.inspect_runtime_idle().expect("empty idle"),
            RuntimeIdleInspection {
                non_terminal_runs: 0,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        let b1 = core.create_binding(THREAD_A, "control").expect("b1");
        let b2 = core.create_binding(THREAD_B, "session").expect("b2");

        // Start a demo run on b1 -> Running
        let demo_run = core
            .execute(
                &b1.binding_id,
                Command::StartDemo {
                    command_key: "demo-1".to_string(),
                    instruction: "demo task".to_string(),
                    step_delay_ms: Some(100),
                    now_ms: 1_000,
                },
            )
            .expect("start demo")
            .into_run()
            .expect("demo run");
        assert_eq!(demo_run.status, RunStatus::Running);
        assert_eq!(
            core.inspect_runtime_idle().expect("running demo"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Advance demo run to WaitingInput -> emits important question_opened -> wakeup Pending on b1
        core.tick(1_200).expect("tick waiting_input");
        assert_eq!(
            core.inspect_runtime_idle()
                .expect("waiting_input + pending"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 1,
            }
        );

        // Claim wakeup on b1 -> InFlight
        let claim_w1 = core
            .claim_wakeup_for_binding(&b1.binding_id, 1_210)
            .expect("claim wakeup")
            .expect("wakeup claim");
        assert_eq!(
            core.inspect_runtime_idle().expect("in_flight wakeup"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 0,
                in_flight_wakeups: 1,
                pending_wakeups: 0,
            }
        );

        // Record wakeup Submitted -> in_flight_wakeups drops to 0
        core.record_wakeup_result(&claim_w1.claim_id, WakeupOutcome::Submitted, 1_220)
            .expect("record submitted");
        assert_eq!(
            core.inspect_runtime_idle().expect("submitted wakeup"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Acknowledge the delivered question events on b1 -> Submitted wakeup returns to Idle.
        let question_batch = core
            .execute(
                &b1.binding_id,
                Command::PullEvents {
                    after_seq: None,
                    limit: 50,
                    now_ms: 1_230,
                },
            )
            .expect("pull b1 question")
            .into_event_batch()
            .expect("batch b1 question");
        core.execute(
            &b1.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b1-question".to_string(),
                batch_id: question_batch.batch_id,
                now_ms: 1_240,
            },
        )
        .expect("ack b1 question");
        assert_eq!(
            core.inspect_runtime_idle().expect("after b1 question ack"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Complete the demo run -> terminal Completed, emits important run_completed -> wakeup Pending again.
        let waiting_run = core
            .get_run(&b1.binding_id, &demo_run.run_id)
            .expect("get waiting demo run");
        let question = waiting_run
            .active_question
            .as_ref()
            .expect("active demo question");
        core.execute(
            &b1.binding_id,
            Command::Answer {
                command_key: "answer-demo-1".to_string(),
                run_id: waiting_run.run_id.clone(),
                question_id: question.question_id.clone(),
                expected_revision: waiting_run.revision,
                answer: "confirmed".to_string(),
                now_ms: 1_250,
            },
        )
        .expect("answer demo");
        core.tick(1_360).expect("tick demo completion");
        assert_eq!(
            core.inspect_runtime_idle().expect("completed demo"),
            RuntimeIdleInspection {
                non_terminal_runs: 0,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 1,
            }
        );

        // Pull and ack events on b1 -> wakeup returns to Idle
        let batch = core
            .execute(
                &b1.binding_id,
                Command::PullEvents {
                    after_seq: None,
                    limit: 50,
                    now_ms: 1_370,
                },
            )
            .expect("pull")
            .into_event_batch()
            .expect("batch");
        core.execute(
            &b1.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b1".to_string(),
                batch_id: batch.batch_id,
                now_ms: 1_380,
            },
        )
        .expect("ack");
        assert_eq!(
            core.inspect_runtime_idle().expect("after ack"),
            RuntimeIdleInspection {
                non_terminal_runs: 0,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Start a worker on b2 -> Queued (both runs and workers tables have 1 non-terminal row)
        let w1 = core
            .execute(
                &b2.binding_id,
                Command::StartWorker {
                    command_key: "start-w1".to_string(),
                    profile_id: "verify".to_string(),
                    profile_digest: DIGEST_A.to_string(),
                    provider: "codex_cli".to_string(),
                    requested_model: "gpt-5.4".to_string(),
                    requested_effort: "high".to_string(),
                    base_commit: BASE_COMMIT.to_string(),
                    execution_binding: None,
                    instruction: "verify package".to_string(),
                    now_ms: 2_000,
                },
            )
            .expect("start worker")
            .into_worker()
            .expect("worker");
        assert_eq!(w1.status, WorkerStatus::Queued);
        assert_eq!(
            core.inspect_runtime_idle().expect("queued worker"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 1,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Claim worker -> Claimed
        let claim = core
            .claim_worker(
                &b2.binding_id,
                &w1.run_id,
                w1.revision,
                &w1.profile_digest,
                2_010,
            )
            .expect("claim worker");
        assert_eq!(
            core.inspect_runtime_idle().expect("claimed worker"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 1,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Record worker running -> Running
        core.record_worker_running(
            &claim,
            WorkerWorktreeUpdate {
                run_worktree_id: "rwt_1".to_string(),
                run_worktree_path: "/tmp/rwt_1".to_string(),
                run_branch_name: "tl/run/1".to_string(),
                task_worktree_id: "twt_1".to_string(),
                task_worktree_path: "/tmp/twt_1".to_string(),
                task_branch_name: "tl/task/1".to_string(),
                base_commit: BASE_COMMIT.to_string(),
            },
            2_020,
        )
        .expect("record running");
        assert_eq!(
            core.inspect_runtime_idle().expect("running worker"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 1,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Request cancel while running -> CancelRequested (still non-terminal!)
        core.execute(
            &b2.binding_id,
            Command::CancelWorker {
                command_key: "cancel-w1".to_string(),
                run_id: w1.run_id.clone(),
                expected_revision: claim.revision,
                reason: Some("operator cancel".to_string()),
                now_ms: 2_030,
            },
        )
        .expect("cancel worker");
        assert_eq!(
            core.inspect_runtime_idle()
                .expect("cancel_requested worker"),
            RuntimeIdleInspection {
                non_terminal_runs: 1,
                non_terminal_workers: 1,
                in_flight_wakeups: 0,
                pending_wakeups: 0,
            }
        );

        // Record terminal Cancelled outcome -> non-terminal runs/workers drop to 0, wakeup Pending on b2
        core.record_worker_outcome(
            &claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Cancelled,
                observed_model: None,
                observed_effort: None,
                head_commit: None,
                dropped_live_events: 0,
                outcome: default_outcome(true),
            },
            2_040,
        )
        .expect("record outcome");
        assert_eq!(
            core.inspect_runtime_idle().expect("after worker cancelled"),
            RuntimeIdleInspection {
                non_terminal_runs: 0,
                non_terminal_workers: 0,
                in_flight_wakeups: 0,
                pending_wakeups: 1,
            }
        );

        // serde deny_unknown_fields check
        let err = serde_json::from_str::<RuntimeIdleInspection>(
            r#"{"non_terminal_runs":0,"non_terminal_workers":0,"in_flight_wakeups":0,"pending_wakeups":0,"extra":1}"#,
        );
        assert!(err.is_err());
    }
}
