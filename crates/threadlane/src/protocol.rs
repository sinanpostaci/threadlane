use serde::{Deserialize, Serialize};
use threadlane_core::{
    AckResult, Binding, Command, CommandOutput, EventBatch, Run, RunStatus, WakeupNotice,
    DEFAULT_EVENT_BATCH_SIZE,
};

/// Maximum number of recent runs included in a compact [`StatusResponse`].
pub const MAX_STATUS_RECENT_RUNS: usize = 10;

/// Maximum character count included in `RunSummary::instruction_preview`.
pub const MAX_SUMMARY_TEXT_PREVIEW_CHARS: usize = 120;

/// Trusted CLI request to bind a validated native Codex thread UUID and display label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBindingRequest {
    /// Canonical hyphenated non-nil Codex thread UUID.
    pub thread_uuid: String,
    /// Human-readable display label.
    pub label: String,
}

/// Response listing all configured bindings (trusted CLI only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingListResponse {
    /// Configured bindings in creation order.
    pub bindings: Vec<Binding>,
}

/// Compact summary of a single run for [`StatusResponse`].
///
/// Omits historical `revision_history` and `questions` lists to keep `status` compact;
/// callers use `get_run` to inspect full run detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    /// Explicit marker indicating that `instruction_preview` is untrusted data.
    pub untrusted_data: bool,
    /// Software-generated run identifier (`run_<uuid>`).
    pub run_id: String,
    /// Owning binding ID.
    pub binding_id: String,
    /// Current 1-based task revision.
    pub revision: u64,
    /// Current lifecycle status.
    pub status: RunStatus,
    /// Truncated preview of the active instruction.
    pub instruction_preview: String,
    /// Currently pending question ID when `status == RunStatus::WaitingInput`.
    pub active_question_id: Option<String>,
    /// Timestamp (ms) when the run was started.
    pub started_at_ms: i64,
    /// Timestamp (ms) when the run was last updated.
    pub updated_at_ms: i64,
}

impl RunSummary {
    /// Builds a compact [`RunSummary`] from a full [`Run`].
    pub fn from_run(run: &Run) -> Self {
        Self {
            untrusted_data: true,
            run_id: run.run_id.clone(),
            binding_id: run.binding_id.clone(),
            revision: run.revision,
            status: run.status,
            instruction_preview: truncate_preview(&run.instruction, MAX_SUMMARY_TEXT_PREVIEW_CHARS),
            active_question_id: run.active_question_id.clone(),
            started_at_ms: run.started_at_ms,
            updated_at_ms: run.updated_at_ms,
        }
    }
}

fn truncate_preview(input: &str, max_chars: usize) -> String {
    let mut chars = input.chars();
    let preview: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

/// Compact binding-scoped status snapshot returned by `status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Explicit marker indicating that text fields in status/summaries are untrusted data.
    pub untrusted_data: bool,
    /// Bound chat metadata and durable event cursors.
    pub binding: Binding,
    /// Current wake-up notice state for this binding.
    pub wakeup: WakeupNotice,
    /// Total number of runs recorded for this binding.
    pub total_runs: usize,
    /// Number of non-terminal runs (`running`, `waiting_input`, `completing`).
    pub active_runs: usize,
    /// Compact summaries of the most recent runs (up to [`MAX_STATUS_RECENT_RUNS`], newest first).
    pub recent_runs: Vec<RunSummary>,
}

impl StatusResponse {
    /// Constructs a compact [`StatusResponse`] from binding metadata, wake-up state, and runs.
    pub fn build(binding: Binding, wakeup: WakeupNotice, runs: &[Run]) -> Self {
        let total_runs = runs.len();
        let active_runs = runs.iter().filter(|r| !r.status.is_terminal()).count();
        let recent_runs = runs
            .iter()
            .rev()
            .take(MAX_STATUS_RECENT_RUNS)
            .map(RunSummary::from_run)
            .collect();

        Self {
            untrusted_data: true,
            binding,
            wakeup,
            total_runs,
            active_runs,
            recent_runs,
        }
    }
}

/// Binding-scoped operation request accepted by the supervisor for CLI `call` and MCP tools.
///
/// Does not accept `binding_id`, `thread_uuid`, or `now_ms` in the payload: the target binding
/// is fixed by the endpoint/scope and the supervisor injects current wall-clock `now_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScopedRequest {
    /// Return a compact status snapshot for the binding.
    Status,
    /// Fetch full run detail (including revision history and questions) for `run_id`.
    GetRun {
        /// Target run identifier.
        run_id: String,
    },
    /// List all runs belonging to the binding.
    ListRuns,
    /// Start a deterministic demo run (`Start -> Running`).
    StartDemo {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Initial task instruction.
        instruction: String,
        /// Optional step delay in milliseconds.
        #[serde(default)]
        step_delay_ms: Option<i64>,
    },
    /// Pull a bounded batch of ordered durable events for the binding.
    PullEvents {
        /// Optional sequence cursor to read after (defaults to `acked_seq`).
        #[serde(default)]
        after_seq: Option<u64>,
        /// Optional batch size limit (`1..=MAX_EVENT_BATCH_SIZE`, defaults to `DEFAULT_EVENT_BATCH_SIZE`).
        #[serde(default)]
        limit: Option<usize>,
    },
    /// Acknowledge processing of a previously delivered event batch.
    #[serde(alias = "acknowledge_events")]
    AckEvents {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Opaque software-generated `batch_id` returned by `PullEvents`.
        batch_id: String,
    },
    /// Provide an answer to the active question on a run at `expected_revision`.
    Answer {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run identifier.
        run_id: String,
        /// Target question identifier.
        question_id: String,
        /// Expected current 1-based revision of the run.
        expected_revision: u64,
        /// Answer text.
        answer: String,
    },
    /// Steer a non-terminal run at `expected_revision` to a new instruction and revision.
    Steer {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run identifier.
        run_id: String,
        /// Expected current 1-based revision of the run.
        expected_revision: u64,
        /// Updated task instruction for `expected_revision + 1`.
        instruction: String,
    },
    /// Terminally cancel a non-terminal run.
    Cancel {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target run identifier.
        run_id: String,
        /// Optional cancellation reason.
        #[serde(default)]
        reason: Option<String>,
    },
}

impl ScopedRequest {
    /// Converts this [`ScopedRequest`] into a [`threadlane_core::Command`] using supervisor-injected `now_ms`.
    ///
    /// Returns `None` for [`ScopedRequest::Status`], which aggregates binding, wake-up, and run summaries.
    pub fn into_core_command(self, now_ms: i64) -> Option<Command> {
        match self {
            Self::Status => None,
            Self::GetRun { run_id } => Some(Command::GetRun { run_id }),
            Self::ListRuns => Some(Command::ListRuns),
            Self::StartDemo {
                command_key,
                instruction,
                step_delay_ms,
            } => Some(Command::StartDemo {
                command_key,
                instruction,
                step_delay_ms,
                now_ms,
            }),
            Self::PullEvents { after_seq, limit } => Some(Command::PullEvents {
                after_seq,
                limit: limit.unwrap_or(DEFAULT_EVENT_BATCH_SIZE),
                now_ms,
            }),
            Self::AckEvents {
                command_key,
                batch_id,
            } => Some(Command::AcknowledgeEvents {
                command_key,
                batch_id,
                now_ms,
            }),
            Self::Answer {
                command_key,
                run_id,
                question_id,
                expected_revision,
                answer,
            } => Some(Command::Answer {
                command_key,
                run_id,
                question_id,
                expected_revision,
                answer,
                now_ms,
            }),
            Self::Steer {
                command_key,
                run_id,
                expected_revision,
                instruction,
            } => Some(Command::Steer {
                command_key,
                run_id,
                expected_revision,
                instruction,
                now_ms,
            }),
            Self::Cancel {
                command_key,
                run_id,
                reason,
            } => Some(Command::Cancel {
                command_key,
                run_id,
                reason,
                now_ms,
            }),
        }
    }

    /// Returns `true` if executing this request can create or release wake-up work.
    pub fn may_affect_scheduling(&self) -> bool {
        matches!(
            self,
            Self::StartDemo { .. }
                | Self::AckEvents { .. }
                | Self::Answer { .. }
                | Self::Steer { .. }
                | Self::Cancel { .. }
        )
    }
}

/// Serializable response returned by the supervisor for a [`ScopedRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ScopedResponse {
    /// Compact binding status (`Status`).
    Status(StatusResponse),
    /// Full run snapshot (`GetRun`, `StartDemo`, `Answer`, `Steer`, `Cancel`).
    Run(Run),
    /// Full run list (`ListRuns`).
    RunList(Vec<Run>),
    /// Ordered event delivery batch (`PullEvents`).
    EventBatch(EventBatch),
    /// Event batch acknowledgement result (`AckEvents`).
    AckResult(AckResult),
}

impl From<CommandOutput> for ScopedResponse {
    fn from(output: CommandOutput) -> Self {
        match output {
            CommandOutput::Run(run) => Self::Run(run),
            CommandOutput::RunList(runs) => Self::RunList(runs),
            CommandOutput::EventBatch(batch) => Self::EventBatch(batch),
            CommandOutput::AckResult(ack) => Self::AckResult(ack),
        }
    }
}

impl ScopedResponse {
    /// Extracts the inner [`StatusResponse`] if this is `ScopedResponse::Status`.
    pub fn into_status(self) -> Option<StatusResponse> {
        match self {
            Self::Status(status) => Some(status),
            _ => None,
        }
    }

    /// Extracts the inner [`Run`] if this is `ScopedResponse::Run`.
    pub fn into_run(self) -> Option<Run> {
        match self {
            Self::Run(run) => Some(run),
            _ => None,
        }
    }

    /// Extracts the inner `Vec<Run>` if this is `ScopedResponse::RunList`.
    pub fn into_run_list(self) -> Option<Vec<Run>> {
        match self {
            Self::RunList(runs) => Some(runs),
            _ => None,
        }
    }

    /// Extracts the inner [`EventBatch`] if this is `ScopedResponse::EventBatch`.
    pub fn into_event_batch(self) -> Option<EventBatch> {
        match self {
            Self::EventBatch(batch) => Some(batch),
            _ => None,
        }
    }

    /// Extracts the inner [`AckResult`] if this is `ScopedResponse::AckResult`.
    pub fn into_ack_result(self) -> Option<AckResult> {
        match self {
            Self::AckResult(ack) => Some(ack),
            _ => None,
        }
    }
}
