# `threadlane-core`

Transactional, persistent SQLite domain engine for Threadlane Milestone 1.

`threadlane-core` is independent of Tauri, HTTP, MCP transport, and external CLI invocation. A single supervisor process owns the `Core` instance and executes all state transitions, event reads/acknowledgements, and wake-up claim transitions through short SQLite transactions.

## Public API Signatures

```rust
use std::path::Path;
use threadlane_core::{
    Binding, Command, CommandOutput, Core, CoreError, Run, TickReport,
    WakeupClaim, WakeupNotice, WakeupOutcome, WakeupRetryRecord,
};

impl Core {
    /// Opens or initializes the SQLite state database and atomically recovers
    /// any `in_flight` wake-up claims from a prior crash/reopen to `WakeupStatus::Unknown`.
    pub fn open(database_path: impl AsRef<Path>) -> Result<Self, CoreError>;

    /// Creates a trusted binding for a validated non-nil Codex thread UUID and display label.
    pub fn create_binding(
        &mut self,
        validated_thread_uuid: &str,
        label: &str,
    ) -> Result<Binding, CoreError>;

    /// Inspects a single configured binding by `binding_id`.
    pub fn get_binding(&self, binding_id: &str) -> Result<Binding, CoreError>;

    /// Lists all configured bindings in creation order.
    pub fn list_bindings(&self) -> Result<Vec<Binding>, CoreError>;

    /// Executes a binding-scoped command (mutations commit state, events, wake-ups, and
    /// idempotency records atomically).
    pub fn execute(
        &mut self,
        binding_id: &str,
        command: Command,
    ) -> Result<CommandOutput, CoreError>;

    /// Advances all deterministic demo runs whose scheduled step time `<= now_ms`.
    pub fn tick(&mut self, now_ms: i64) -> Result<TickReport, CoreError>;

    /// Claims the oldest `Pending` wake-up notice across bindings and persists `InFlight`
    /// intent prior to an external queue invocation.
    pub fn claim_wakeup(&mut self, now_ms: i64) -> Result<Option<WakeupClaim>, CoreError>;

    /// Claims a `Pending` wake-up notice for a specific `binding_id`.
    pub fn claim_wakeup_for_binding(
        &mut self,
        binding_id: &str,
        now_ms: i64,
    ) -> Result<Option<WakeupClaim>, CoreError>;

    /// Records the explicit outcome (`Submitted`, `Failed`, or `Unknown`) of an `InFlight` claim.
    pub fn record_wakeup_result(
        &mut self,
        claim_id: &str,
        outcome: WakeupOutcome,
        now_ms: i64,
    ) -> Result<WakeupNotice, CoreError>;

    /// Explicitly rearms a `Failed` (or `Unknown` when `allow_unknown == true`) wake-up notice
    /// back to `WakeupStatus::Pending` when `expected_claim_id` matches the live claim.
    pub fn retry_wakeup(
        &mut self,
        binding_id: &str,
        expected_claim_id: &str,
        allow_unknown: bool,
        now_ms: i64,
    ) -> Result<WakeupNotice, CoreError>;

    /// Lists persisted manual wake-up retry audit records (`wakeup_retries`) for `binding_id`.
    pub fn list_wakeup_retries(
        &self,
        binding_id: &str,
    ) -> Result<Vec<WakeupRetryRecord>, CoreError>;

    /// Returns the current persistent wake-up notice state for `binding_id`.
    pub fn get_wakeup(&self, binding_id: &str) -> Result<WakeupNotice, CoreError>;

    /// Convenience helper to fetch a single `Run` scoped to `binding_id`.
    pub fn get_run(&self, binding_id: &str, run_id: &str) -> Result<Run, CoreError>;
}
```

## Typed Commands and Outputs

```rust
pub enum Command {
    StartDemo {
        command_key: String,
        instruction: String,
        step_delay_ms: Option<i64>,
        now_ms: i64,
    },
    GetRun {
        run_id: String,
    },
    ListRuns,
    PullEvents {
        after_seq: Option<u64>,
        limit: usize,
        now_ms: i64,
    },
    AcknowledgeEvents {
        command_key: String,
        batch_id: String,
        now_ms: i64,
    },
    Answer {
        command_key: String,
        run_id: String,
        question_id: String,
        expected_revision: u64,
        answer: String,
        now_ms: i64,
    },
    Steer {
        command_key: String,
        run_id: String,
        expected_revision: u64,
        instruction: String,
        now_ms: i64,
    },
    Cancel {
        command_key: String,
        run_id: String,
        reason: Option<String>,
        now_ms: i64,
    },
}

pub enum CommandOutput {
    Run(Run),
    RunList(Vec<Run>),
    EventBatch(EventBatch),
    AckResult(AckResult),
}
```

## Key Invariants

1. **Per-Binding Isolation**: Every run, question, event cursor, delivery batch, deduplication key, and wake-up notice is strictly scoped to its `binding_id`. Cross-binding access or acknowledgement is rejected.
2. **Deterministic Demo Lifecycle & Bounded Event Payloads**:
   - `StartDemo` creates a run in `RunStatus::Running` at `revision = 1` with `next_step_at_ms = now_ms + step_delay_ms` and emits `EventKind::RunStarted` carrying `summary` and `instruction: Some(instruction)`. Immediate event payload size (`<= MAX_EVENT_PAYLOAD_BYTES`) is validated transactionally before commit; oversized payloads fail with `CoreError::EventPayloadTooLarge` with no partial write.
   - `tick(now_ms)` advances due `Running` runs to `RunStatus::WaitingInput`, creates a `Pending` demo question with a compact prompt referencing `(run_id, revision)` and `get_run`, emits `RunProgress` (`summary` only, `instruction: None`) and `QuestionAsked` (`important = true`, `summary`, `question_id`, compact `question_prompt`, `instruction: None`), and creates/coalesces a pending wake-up notice. Timed/derived events never duplicate the full instruction, so any accepted or previously persisted instruction is safe across all future deterministic ticks. Full original instruction and revision history remain in `Run` (`get_run`).
   - `Answer` validates `expected_revision` and active `question_id`, transitions the run to `RunStatus::Completing` with `next_step_at_ms = now_ms + step_delay_ms`, and emits `QuestionAnswered` (`summary`, `question_id`, `answer: Some(answer)`, `instruction: None`, validated transactionally against `MAX_EVENT_PAYLOAD_BYTES`).
   - `tick(now_ms)` advances due `Completing` runs to `RunStatus::Completed`, emits `RunCompleted` (`important = true`, `summary` only, `instruction: None`, `answer: None`), and creates/coalesces a pending wake-up notice.
   - `Steer` validates `expected_revision`, increments `revision`, marks any pending question `QuestionStatus::Superseded` (emitting `QuestionSuperseded` with `question_id` and `instruction: None`), preserves revision and question history, resets the run to `RunStatus::Running` for the new revision, and emits `RunSteered` (`summary`, `instruction: Some(instruction)`, validated transactionally against `MAX_EVENT_PAYLOAD_BYTES`). Superseded questions and steps can never be answered or completed.
   - `Cancel` transitions a non-terminal run to `RunStatus::Cancelled` (terminal), emits `RunCancelled` (`summary`, `cancel_reason`, `instruction: None`), and prevents any future completion or mutation.
3. **Idempotent Mutations**: Mutating commands (`StartDemo`, `Answer`, `Steer`, `Cancel`, `AcknowledgeEvents`) record `(binding_id, command_key, operation, payload_hash, response_json)` in the same SQLite transaction as state and event mutations. Identical retries return the original `CommandOutput` (even across restarts and advancing `now_ms`); reusing a key on the same binding with different parameters or a different operation fails with `CoreError::IdempotencyConflict`.
4. **Ordered Events & Acknowledgement High-Water Marks**:
   - Events receive contiguous 1-based per-binding sequence numbers (`seq`).
   - `PullEvents` returns a bounded batch (`1..=MAX_EVENT_BATCH_SIZE`), assigns a software-generated `batch_id`, and advances `delivered_seq`. Reading events does not acknowledge them.
   - `AcknowledgeEvents` validates that `batch_id` exists, belongs to `binding_id`, does not exceed `delivered_seq`, and does not skip unacknowledged events (`batch.from_seq <= acked_seq + 1`). Repeated/stale acknowledgements (`batch.to_seq <= acked_seq`) succeed harmlessly without moving `acked_seq` backwards.
5. **Wake-up Persistence, Coalescing, Recovery & Manual Retry**:
   - At most one unacknowledged wake-up notice exists per binding. Multiple important events (`QuestionAsked`, `RunCompleted`) coalesce into the single notice.
   - `claim_wakeup` persists `WakeupStatus::InFlight` and `covered_up_to_seq` before the supervisor invokes the external queue command with `FIXED_WAKEUP_MESSAGE`.
   - Reopening the database (`Core::open`) recovers any `InFlight` notice to `WakeupStatus::Unknown`.
   - `Submitted`, `Failed`, and `Unknown` notices are never automatically retried. Only a valid `AcknowledgeEvents` covering `covered_up_to_seq` releases the prior notification coverage; if newer unread/unacknowledged important events exist (`seq > acked_seq`), the notice transitions to `WakeupStatus::Pending` for those remaining events (or `WakeupStatus::Idle` when none remain).
   - Trusted operator code can explicitly rearm a `Failed` notice (or an `Unknown` notice when `allow_unknown == true`) via `Core::retry_wakeup(binding_id, expected_claim_id, allow_unknown, now_ms)`:
     - Rejects `Idle`, `Pending`, `InFlight`, and `Submitted` notices (`CoreError::InvalidWakeupState`), and rejects `Unknown` notices when `allow_unknown == false`.
     - Requires `expected_claim_id` to match the binding's current live `claim_id`; stale, repeated, or cross-binding retries are rejected without releasing any newer claim.
     - Atomically returns the notice to `WakeupStatus::Pending` without altering `delivered_seq` or `acked_seq`, preserving `pending_since_seq`, `latest_important_seq`, `coalesced_count`, and cumulative `attempt_count`, and clearing active claim fields (`claim_id`, `covered_up_to_seq`, `last_error`, `claimed_at_ms`, `resolved_at_ms`).
     - Preserves historical `wakeup_claims` rows immutably and appends a minimal audit row in `wakeup_retries` (`retry_id`, `binding_id`, `prior_claim_id`, `prior_status`, `allow_unknown`, `retried_at_ms`, with no tokens or chat data). The additive `wakeup_retries` table is initialized with `CREATE TABLE IF NOT EXISTS` at `SCHEMA_VERSION = 1`, maintaining backward/forward compatibility with existing v1 state databases.
