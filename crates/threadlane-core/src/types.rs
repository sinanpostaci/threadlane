use serde::{Deserialize, Serialize};

/// Trusted binding between a Threadlane scope and a native Codex thread UUID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    /// Software-generated binding identifier (`bnd_<uuid>`).
    pub binding_id: String,
    /// Validated canonical hyphenated native Codex thread UUID.
    pub thread_uuid: String,
    /// Validated human-readable display label.
    pub label: String,
    /// Highest event sequence number delivered via `PullEvents` for this binding.
    pub delivered_seq: u64,
    /// Highest event sequence number processed/acknowledged via `AcknowledgeEvents`.
    pub acked_seq: u64,
}

/// Lifecycle state of a deterministic demo run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Run has started (or been steered to a new revision) and is progressing toward a question.
    Running,
    /// Run is paused waiting for an answer to its active question.
    WaitingInput,
    /// Run received a valid answer and is finishing its bounded completion step.
    Completing,
    /// Run finished normally (terminal).
    Completed,
    /// Run was cancelled by the caller (terminal).
    Cancelled,
}

impl RunStatus {
    /// Returns `true` if the run is in a terminal state (`Completed` or `Cancelled`).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::WaitingInput => "waiting_input",
            Self::Completing => "completing",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "waiting_input" => Some(Self::WaitingInput),
            "completing" => Some(Self::Completing),
            "completed" => Some(Self::Completed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Lifecycle state of a question emitted by a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionStatus {
    /// Waiting for an answer on the current run revision.
    Pending,
    /// Answered by the caller on the matching revision.
    Answered,
    /// Superseded by a `Steer` command that advanced the run revision.
    Superseded,
    /// Cancelled because the parent run was cancelled.
    Cancelled,
}

impl QuestionStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Answered => "answered",
            Self::Superseded => "superseded",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "answered" => Some(Self::Answered),
            "superseded" => Some(Self::Superseded),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Persisted question associated with a specific `(binding_id, run_id, revision)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    /// Software-generated question identifier (`qst_<uuid>`).
    pub question_id: String,
    /// Owning binding ID.
    pub binding_id: String,
    /// Owning run ID.
    pub run_id: String,
    /// Task revision that produced this question.
    pub revision: u64,
    /// Current status of the question.
    pub status: QuestionStatus,
    /// Question prompt text.
    pub prompt: String,
    /// Recorded answer text when `status == QuestionStatus::Answered`.
    pub answer: Option<String>,
    /// Revision that superseded this question when `status == QuestionStatus::Superseded`.
    pub superseded_by_revision: Option<u64>,
    /// Timestamp (ms) when the question was created.
    pub created_at_ms: i64,
    /// Timestamp (ms) when the question was answered, superseded, or cancelled.
    pub resolved_at_ms: Option<i64>,
}

/// Historical record of a task instruction at a specific revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRevision {
    /// 1-based revision number.
    pub revision: u64,
    /// Task instruction active during this revision.
    pub instruction: String,
    /// Timestamp (ms) when this revision was created.
    pub created_at_ms: i64,
    /// Timestamp (ms) when this revision was superseded by a subsequent `Steer`.
    pub superseded_at_ms: Option<i64>,
}

/// Full persisted snapshot of a deterministic demo run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    /// Software-generated run identifier (`run_<uuid>`).
    pub run_id: String,
    /// Owning binding ID.
    pub binding_id: String,
    /// Current 1-based task revision.
    pub revision: u64,
    /// Current lifecycle status (`Running`, `WaitingInput`, `Completing`, `Completed`, `Cancelled`).
    pub status: RunStatus,
    /// Current active instruction for `revision`.
    pub instruction: String,
    /// Monotonically increasing step counter persisted across transitions.
    pub step_seq: u64,
    /// Configured delay (ms) between timed progression steps.
    pub step_delay_ms: i64,
    /// Scheduled timestamp (ms) for the next `tick` transition (`None` when waiting for input or terminal).
    pub next_step_at_ms: Option<i64>,
    /// Currently pending question ID when `status == RunStatus::WaitingInput`.
    pub active_question_id: Option<String>,
    /// Timestamp (ms) when the run was started.
    pub started_at_ms: i64,
    /// Timestamp (ms) when the run was last updated.
    pub updated_at_ms: i64,
    /// Timestamp (ms) when the run reached `Completed`.
    pub completed_at_ms: Option<i64>,
    /// Timestamp (ms) when the run reached `Cancelled`.
    pub cancelled_at_ms: Option<i64>,
    /// Optional cancellation reason when `status == RunStatus::Cancelled`.
    pub cancel_reason: Option<String>,
    /// Ordered revision history (`1..=revision`).
    pub revision_history: Vec<RunRevision>,
    /// Ordered history of questions emitted across revisions of this run.
    pub questions: Vec<Question>,
}

/// Classification of a durable binding event.
///
/// Timed and derived events (`RunProgress`, `QuestionAsked`, `QuestionSuperseded`, `RunCompleted`)
/// never duplicate the full task instruction; callers retrieve full instruction and revision
/// history via `get_run` (`Command::GetRun`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// Demo run started (`Start -> Running`). Carries `summary` and `instruction: Some(instruction)`.
    RunStarted,
    /// Demo run emitted progress before requesting input. Carries `summary` only (`instruction: None`).
    RunProgress,
    /// Demo run transitioned to `WaitingInput` and posed a compact demo question (`important = true`).
    /// Carries `summary`, `question_id: Some(question_id)`, and `question_prompt: Some(prompt)` (`instruction: None`).
    QuestionAsked,
    /// Caller answered the active question (`WaitingInput -> Completing`).
    /// Carries `summary`, `question_id: Some(question_id)`, and `answer: Some(answer)` (`instruction: None`).
    QuestionAnswered,
    /// Prior pending question was superseded by `Steer`.
    /// Carries `summary` and `question_id: Some(question_id)` (`instruction: None`).
    QuestionSuperseded,
    /// Caller steered the run to a new instruction and revision.
    /// Carries `summary` and `instruction: Some(instruction)`.
    RunSteered,
    /// Demo run finished (`Completing -> Completed`, `important = true`).
    /// Carries `summary` only (`instruction: None`, `answer: None`).
    RunCompleted,
    /// Demo run was cancelled (`-> Cancelled`).
    /// Carries `summary` and optional `cancel_reason: Option<String>` (`instruction: None`).
    RunCancelled,
}

impl EventKind {
    /// Returns `true` if this event kind triggers or coalesces into a wake-up notice.
    pub fn is_important(self) -> bool {
        matches!(self, Self::QuestionAsked | Self::RunCompleted)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RunStarted => "run_started",
            Self::RunProgress => "run_progress",
            Self::QuestionAsked => "question_asked",
            Self::QuestionAnswered => "question_answered",
            Self::QuestionSuperseded => "question_superseded",
            Self::RunSteered => "run_steered",
            Self::RunCompleted => "run_completed",
            Self::RunCancelled => "run_cancelled",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "run_started" => Some(Self::RunStarted),
            "run_progress" => Some(Self::RunProgress),
            "question_asked" => Some(Self::QuestionAsked),
            "question_answered" => Some(Self::QuestionAnswered),
            "question_superseded" => Some(Self::QuestionSuperseded),
            "run_steered" => Some(Self::RunSteered),
            "run_completed" => Some(Self::RunCompleted),
            "run_cancelled" => Some(Self::RunCancelled),
            _ => None,
        }
    }
}

/// Structured, bounded payload carried by a durable [`Event`].
///
/// Always tagged with `untrusted_data: true` so downstream MCP/CLI consumers treat worker/event text as untrusted data.
///
/// Payload field population by [`EventKind`]:
/// - [`EventKind::RunStarted`]: `summary`, `instruction: Some(instruction)`.
/// - [`EventKind::RunProgress`]: `summary` only (`instruction: None`).
/// - [`EventKind::QuestionAsked`]: `summary`, `question_id: Some(question_id)`, and compact `question_prompt: Some(prompt)` referencing `(run_id, revision)` and `get_run` (`instruction: None`).
/// - [`EventKind::QuestionAnswered`]: `summary`, `question_id: Some(question_id)`, `answer: Some(answer)` (`instruction: None`).
/// - [`EventKind::QuestionSuperseded`]: `summary`, `question_id: Some(question_id)` (`instruction: None`).
/// - [`EventKind::RunSteered`]: `summary`, `instruction: Some(instruction)`.
/// - [`EventKind::RunCompleted`]: `summary` only (`instruction: None`, `answer: None`).
/// - [`EventKind::RunCancelled`]: `summary`, `cancel_reason: Option<String>` (`instruction: None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventPayload {
    /// Explicit marker indicating that text fields inside event payloads are untrusted data.
    pub untrusted_data: bool,
    /// Human-readable summary of the transition or step (present on all event kinds).
    pub summary: String,
    /// Associated question ID when `kind` is `QuestionAsked`, `QuestionAnswered`, or `QuestionSuperseded` (`None` otherwise).
    pub question_id: Option<String>,
    /// Compact demo question prompt referencing `(run_id, revision)` and `get_run` when `kind == EventKind::QuestionAsked` (`None` otherwise).
    pub question_prompt: Option<String>,
    /// Answer text when `kind == EventKind::QuestionAnswered` (`None` otherwise).
    pub answer: Option<String>,
    /// Full task instruction when `kind == EventKind::RunStarted` or `EventKind::RunSteered` (`None` on `RunProgress`, `QuestionAsked`, `RunCompleted`, and all other events).
    pub instruction: Option<String>,
    /// Cancellation reason when `kind == EventKind::RunCancelled` (`None` otherwise).
    pub cancel_reason: Option<String>,
}

/// Ordered durable event scoped to a single binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Software-generated event ID (`evt_<uuid>`).
    pub event_id: String,
    /// Owning binding ID.
    pub binding_id: String,
    /// Monotonically increasing 1-based per-binding event sequence number.
    pub seq: u64,
    /// Associated run ID.
    pub run_id: String,
    /// Run revision at the time the event was emitted.
    pub revision: u64,
    /// Persisted run step sequence at the time the event was emitted.
    pub step_seq: u64,
    /// Event classification.
    pub kind: EventKind,
    /// Whether this event triggers/coalesces into a binding wake-up notice.
    pub important: bool,
    /// Bounded, structured event payload.
    pub payload: EventPayload,
    /// Timestamp (ms) when the event was recorded.
    pub created_at_ms: i64,
}

/// Bounded batch of ordered events returned by `PullEvents`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventBatch {
    /// Opaque software-generated delivery batch identifier (`bat_<uuid>`).
    pub batch_id: String,
    /// Binding that owns this batch.
    pub binding_id: String,
    /// Ordered events in this delivery batch.
    pub events: Vec<Event>,
    /// First event sequence number in this batch (`acked_seq` when `events` is empty).
    pub from_seq: u64,
    /// Last event sequence number in this batch (`acked_seq` when `events` is empty).
    pub to_seq: u64,
    /// Highest event sequence number delivered so far for this binding.
    pub delivered_seq: u64,
    /// Highest event sequence number acknowledged so far for this binding.
    pub acked_seq: u64,
    /// `true` if additional unread events remain after `to_seq`.
    pub has_more: bool,
}

/// Result of acknowledging a delivered [`EventBatch`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckResult {
    /// Binding ID.
    pub binding_id: String,
    /// Acknowledged batch ID.
    pub batch_id: String,
    /// Resulting acknowledged high-water mark (`acked_seq`). Never moves backwards.
    pub acked_seq: u64,
    /// `true` if `acked_seq` advanced, `false` if this was a harmless old/repeated acknowledgement.
    pub advanced: bool,
    /// Updated wake-up status for the binding after applying acknowledgement coverage rules.
    pub wakeup_status: WakeupStatus,
}

/// Public typed command executed against a specific `binding_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// Start a new deterministic demo run (`Start -> Running`).
    StartDemo {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Initial task instruction.
        instruction: String,
        /// Optional step delay in milliseconds (defaults to [`crate::limits::DEFAULT_DEMO_STEP_DELAY_MS`]).
        step_delay_ms: Option<i64>,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
    /// Fetch a run and its full revision/question history scoped to the binding.
    GetRun {
        /// Run ID to inspect.
        run_id: String,
    },
    /// List all runs belonging to the binding in creation order.
    ListRuns,
    /// Pull a bounded batch of ordered events for the binding and record a delivery batch ID.
    PullEvents {
        /// Optional sequence cursor to read after. Defaults to the binding's `acked_seq`.
        /// Must not exceed `delivered_seq` (cannot skip unseen events).
        after_seq: Option<u64>,
        /// Maximum number of events to return (`1..=MAX_EVENT_BATCH_SIZE`).
        limit: usize,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
    /// Explicitly acknowledge processing of a previously delivered event batch.
    AcknowledgeEvents {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Opaque software-generated `batch_id` returned by `PullEvents`.
        batch_id: String,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
    /// Provide an answer to the active question on a run at `expected_revision` (`WaitingInput -> Completing`).
    Answer {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run ID.
        run_id: String,
        /// Target question ID.
        question_id: String,
        /// Expected current revision of the run.
        expected_revision: u64,
        /// Answer text.
        answer: String,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
    /// Steer a non-terminal run at `expected_revision`: updates instruction, increments revision,
    /// supersedes any pending question, and restarts progression for the new revision.
    Steer {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run ID.
        run_id: String,
        /// Expected current revision of the run.
        expected_revision: u64,
        /// Updated task instruction for `expected_revision + 1`.
        instruction: String,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
    /// Terminally cancel a non-terminal run (`-> Cancelled`).
    Cancel {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run ID.
        run_id: String,
        /// Optional cancellation reason.
        reason: Option<String>,
        /// Injected current timestamp in milliseconds.
        now_ms: i64,
    },
}

/// Serializable output returned by [` crate::Core::execute `].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CommandOutput {
    /// Single run snapshot (returned by `StartDemo`, `GetRun`, `Answer`, `Steer`, `Cancel`).
    Run(Run),
    /// List of runs on the binding (returned by `ListRuns`).
    RunList(Vec<Run>),
    /// Delivered event batch (returned by `PullEvents`).
    EventBatch(EventBatch),
    /// Event batch acknowledgement result (returned by `AcknowledgeEvents`).
    AckResult(AckResult),
}

impl CommandOutput {
    /// Extracts the inner [`Run`] if this output is `CommandOutput::Run`.
    pub fn into_run(self) -> Option<Run> {
        match self {
            Self::Run(run) => Some(run),
            _ => None,
        }
    }

    /// Borrows the inner [`Run`] if this output is `CommandOutput::Run`.
    pub fn as_run(&self) -> Option<&Run> {
        match self {
            Self::Run(run) => Some(run),
            _ => None,
        }
    }

    /// Extracts the inner `Vec<Run>` if this output is `CommandOutput::RunList`.
    pub fn into_run_list(self) -> Option<Vec<Run>> {
        match self {
            Self::RunList(runs) => Some(runs),
            _ => None,
        }
    }

    /// Extracts the inner [`EventBatch`] if this output is `CommandOutput::EventBatch`.
    pub fn into_event_batch(self) -> Option<EventBatch> {
        match self {
            Self::EventBatch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Extracts the inner [`AckResult`] if this output is `CommandOutput::AckResult`.
    pub fn into_ack_result(self) -> Option<AckResult> {
        match self {
            Self::AckResult(ack) => Some(ack),
            _ => None,
        }
    }
}

/// Summary of deterministic run transitions performed by [`crate::Core::tick`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickReport {
    /// Run IDs that advanced a step during this tick (`Running -> WaitingInput` or `Completing -> Completed`).
    pub advanced_runs: Vec<String>,
    /// Total number of durable events emitted during this tick.
    pub emitted_events: usize,
}

/// Persistent wake-up status for a binding.
///
/// At most one unacknowledged notice exists per binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeupStatus {
    /// No unacknowledged important events are awaiting notification.
    Idle,
    /// One or more important events are pending wake-up dispatch.
    Pending,
    /// Wake-up claim has been durably persisted prior to external queue invocation.
    InFlight,
    /// External queue invocation succeeded; waiting for valid event acknowledgement covering `covered_up_to_seq`.
    Submitted,
    /// External queue invocation failed; never automatically retried (may be explicitly rearmed via `Core::retry_wakeup` or released by acknowledging `covered_up_to_seq`).
    Failed,
    /// External queue invocation timed out or supervisor reopened while `InFlight`; never automatically retried (may be explicitly rearmed via `Core::retry_wakeup` with `allow_unknown == true` or released by acknowledging `covered_up_to_seq`).
    Unknown,
}

impl WakeupStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Submitted => "submitted",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "pending" => Some(Self::Pending),
            "in_flight" => Some(Self::InFlight),
            "submitted" => Some(Self::Submitted),
            "failed" => Some(Self::Failed),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// Explicit outcome recorded after attempting an external wake-up queue invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WakeupOutcome {
    /// External queue command completed successfully.
    Submitted,
    /// External queue command definitively failed before submission.
    Failed {
        /// Diagnostic error detail.
        error: String,
    },
    /// External queue command timed out or terminated with unknown delivery status.
    Unknown {
        /// Diagnostic reason detail.
        reason: String,
    },
}

/// Claimed wake-up intent persisted before invoking the external Codex queue bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeupClaim {
    /// Software-generated claim identifier (`wcl_<uuid>`).
    pub claim_id: String,
    /// Target binding ID.
    pub binding_id: String,
    /// Validated native Codex thread UUID for `queue --thread <UUID>`.
    pub thread_uuid: String,
    /// Fixed machine-generated message (`FIXED_WAKEUP_MESSAGE`) containing no worker prose or labels.
    pub message: String,
    /// Earliest unacknowledged important event sequence number in this notice.
    pub pending_since_seq: u64,
    /// Highest important event sequence number covered by this claim.
    pub covered_up_to_seq: u64,
    /// Number of important events coalesced into this notice at claim time.
    pub coalesced_count: u64,
    /// Timestamp (ms) when the claim intent was persisted.
    pub claimed_at_ms: i64,
}

/// Full persistent wake-up state for a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeupNotice {
    /// Binding ID.
    pub binding_id: String,
    /// Validated native Codex thread UUID.
    pub thread_uuid: String,
    /// Current wake-up status.
    pub status: WakeupStatus,
    /// Active or most recent claim ID for the current unacknowledged notice cycle.
    pub claim_id: Option<String>,
    /// Fixed wake-up message (`FIXED_WAKEUP_MESSAGE`).
    pub message: String,
    /// Earliest unacknowledged important event sequence number in the current notice cycle.
    pub pending_since_seq: Option<u64>,
    /// Highest important event sequence number emitted on this binding.
    pub latest_important_seq: u64,
    /// Highest important event sequence number covered by the current/last claim in this cycle.
    pub covered_up_to_seq: u64,
    /// Count of unacknowledged important events coalesced into the current notice.
    pub coalesced_count: u64,
    /// Total number of claims attempted across the lifetime of this binding.
    pub attempt_count: u64,
    /// Diagnostic detail when `status` is `Failed` or `Unknown`.
    pub last_error: Option<String>,
    /// Timestamp (ms) when the current claim was persisted.
    pub claimed_at_ms: Option<i64>,
    /// Timestamp (ms) when the most recent claim outcome was recorded.
    pub resolved_at_ms: Option<i64>,
}

/// Minimal persisted audit record for an explicit manual wake-up retry ([`crate::Core::retry_wakeup`]).
///
/// Contains only binding and claim metadata (`binding_id`, `prior_claim_id`, `prior_status`,
/// `allow_unknown`, `retried_at_ms`) and never stores bearer tokens or raw chat/task text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WakeupRetryRecord {
    /// Software-generated retry audit identifier (`wrt_<uuid>`).
    pub retry_id: String,
    /// Binding whose wake-up notice was manually rearmed.
    pub binding_id: String,
    /// Claim identifier that was active on the notice prior to the manual retry.
    pub prior_claim_id: String,
    /// Wake-up status (`Failed` or `Unknown`) prior to the manual retry.
    pub prior_status: WakeupStatus,
    /// Whether the caller explicitly opted into retrying an `Unknown` wake-up claim.
    pub allow_unknown: bool,
    /// Timestamp (ms) when the manual retry was recorded.
    pub retried_at_ms: i64,
}
