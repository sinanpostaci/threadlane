use serde::{Deserialize, Serialize};
use threadlane_core::{
    AckResult, Binding, Command, CommandOutput, EventBatch, QuestionHistoryPage,
    RevisionHistoryPage, Run, RunListPage, WakeupNotice, DEFAULT_EVENT_BATCH_SIZE,
    DEFAULT_PAGE_LIMIT,
};
pub use threadlane_core::{
    ActivityPage, Event, EventKind, EventPayload, RunSummary, WorkerAcceptanceState, WorkerClaim,
    WorkerCompletionRecord, WorkerOutcomeSummary, WorkerProgressSummary, WorkerProgressUpdate,
    WorkerSnapshot, WorkerStatus, WorkerWorktreeSummary, WorkerWorktreeUpdate,
    MAX_STATUS_RECENT_RUNS, MAX_SUMMARY_TEXT_PREVIEW_CHARS,
};

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

/// Trusted CLI request to manually re-arm a `Failed` (or explicitly `Unknown`) wake-up claim.
///
/// Explicitly excluded from [`ScopedRequest`] and MCP tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryWakeupRequest {
    /// Expected claim identifier (`wcl_<uuid>`) currently recorded on the binding's wake-up notice.
    pub expected_claim_id: String,
    /// Explicitly allow retrying a wake-up claim in `WakeupStatus::Unknown` (may cause duplicate wake-up delivery).
    #[serde(default)]
    pub allow_unknown: bool,
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
    /// Constructs a compact [`StatusResponse`] from binding metadata, wake-up state, and SQL summary counts.
    pub fn from_summary_counts(
        binding: Binding,
        wakeup: WakeupNotice,
        total_runs: usize,
        active_runs: usize,
        recent_runs: Vec<RunSummary>,
    ) -> Self {
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
    /// Fetch a constant-size current run snapshot (including `active_question` when waiting for input) for `run_id`.
    GetRun {
        /// Target run identifier.
        run_id: String,
    },
    /// List a bounded page of runs belonging to the binding in creation order.
    ListRuns {
        /// Optional opaque keyset cursor returned by a previous `ListRuns` page.
        #[serde(default)]
        cursor: Option<String>,
        /// Optional page size limit (`1..=MAX_PAGE_LIMIT`, defaults to `DEFAULT_PAGE_LIMIT`).
        #[serde(default)]
        limit: Option<usize>,
    },
    /// List a bounded page of revision history records for `run_id`.
    ListRunRevisions {
        /// Target run identifier.
        run_id: String,
        /// Optional opaque keyset cursor returned by a previous `ListRunRevisions` page.
        #[serde(default)]
        cursor: Option<String>,
        /// Optional page size limit (`1..=MAX_PAGE_LIMIT`, defaults to `DEFAULT_PAGE_LIMIT`).
        #[serde(default)]
        limit: Option<usize>,
    },
    /// List a bounded page of question history records for `run_id`.
    ListRunQuestions {
        /// Target run identifier.
        run_id: String,
        /// Optional opaque keyset cursor returned by a previous `ListRunQuestions` page.
        #[serde(default)]
        cursor: Option<String>,
        /// Optional page size limit (`1..=MAX_PAGE_LIMIT`, defaults to `DEFAULT_PAGE_LIMIT`).
        #[serde(default)]
        limit: Option<usize>,
    },
    /// List a bounded read-only page of durable activity events for the binding without mutating delivery or acknowledgement cursors.
    ListActivity {
        /// Optional sequence position (`seq`) to start after on the first page (defaults to `0`; mutually exclusive with `cursor`).
        #[serde(default)]
        after_seq: Option<u64>,
        /// Optional opaque keyset cursor returned by a previous `ListActivity` page (mutually exclusive with `after_seq`).
        #[serde(default)]
        cursor: Option<String>,
        /// Optional page size limit (`1..=MAX_PAGE_LIMIT`, defaults to `DEFAULT_PAGE_LIMIT`).
        #[serde(default)]
        limit: Option<usize>,
        /// Optional run identifier filter (`run_<uuid>`).
        #[serde(default)]
        run_id: Option<String>,
        /// Optional event kind filter.
        #[serde(default)]
        kind: Option<EventKind>,
    },
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
    /// Pull an ordered batch of durable events for the binding bounded by both event count and serialized JSON byte budget.
    PullEventsBounded {
        /// Optional sequence cursor to read after (defaults to `acked_seq`).
        #[serde(default)]
        after_seq: Option<u64>,
        /// Optional batch size limit (`1..=MAX_EVENT_BATCH_SIZE`, defaults to `DEFAULT_EVENT_BATCH_SIZE`).
        #[serde(default)]
        limit: Option<usize>,
        /// Maximum serialized JSON byte size for the returned [`EventBatch`] (`MIN_EVENT_BATCH_BYTES..=MAX_EVENT_BATCH_BYTES`).
        max_batch_bytes: usize,
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
    /// Admit and queue a new worker run using a startup-configured profile (`-> Queued`).
    StartWorker {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Startup-configured worker profile identifier.
        profile_id: String,
        /// Bounded task instruction for the worker.
        instruction: String,
    },
    /// Fetch a compact durable [`WorkerSnapshot`] for `run_id` (or `worker_id`) within the binding.
    GetWorker {
        /// Target worker run identifier (`run_<uuid>`) or worker identifier (`wrk_<uuid>`).
        #[serde(alias = "worker_id")]
        run_id: String,
    },
    /// Request revision-checked cancellation of a worker run within the binding.
    CancelWorker {
        /// Caller-supplied idempotency key scoped to the binding.
        command_key: String,
        /// Target worker run identifier (`run_<uuid>`) or worker identifier (`wrk_<uuid>`).
        #[serde(alias = "worker_id")]
        run_id: String,
        /// Expected current 1-based revision of the worker run.
        expected_revision: u64,
        /// Optional cancellation reason.
        #[serde(default)]
        reason: Option<String>,
    },
}

impl ScopedRequest {
    /// Converts this [`ScopedRequest`] into a [`threadlane_core::Command`] using supervisor-injected `now_ms`.
    ///
    /// Returns `None` for [`ScopedRequest::Status`] and [`ScopedRequest::StartWorker`], which require
    /// supervisor-level aggregation or startup profile resolution prior to core execution.
    pub fn into_core_command(self, now_ms: i64) -> Option<Command> {
        match self {
            Self::Status | Self::StartWorker { .. } => None,
            Self::GetRun { run_id } => Some(Command::GetRun { run_id }),
            Self::ListRuns { cursor, limit } => Some(Command::ListRuns {
                cursor,
                limit: limit.unwrap_or(DEFAULT_PAGE_LIMIT),
            }),
            Self::ListRunRevisions {
                run_id,
                cursor,
                limit,
            } => Some(Command::ListRunRevisions {
                run_id,
                cursor,
                limit: limit.unwrap_or(DEFAULT_PAGE_LIMIT),
            }),
            Self::ListRunQuestions {
                run_id,
                cursor,
                limit,
            } => Some(Command::ListRunQuestions {
                run_id,
                cursor,
                limit: limit.unwrap_or(DEFAULT_PAGE_LIMIT),
            }),
            Self::ListActivity {
                after_seq,
                cursor,
                limit,
                run_id,
                kind,
            } => Some(Command::ListActivity {
                after_seq,
                cursor,
                limit: limit.unwrap_or(DEFAULT_PAGE_LIMIT),
                run_id,
                kind,
            }),
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
            Self::PullEventsBounded {
                after_seq,
                limit,
                max_batch_bytes,
            } => Some(Command::PullEventsBounded {
                after_seq,
                limit: limit.unwrap_or(DEFAULT_EVENT_BATCH_SIZE),
                max_batch_bytes,
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
            Self::GetWorker { run_id } => Some(Command::GetWorker { run_id }),
            Self::CancelWorker {
                command_key,
                run_id,
                expected_revision,
                reason,
            } => Some(Command::CancelWorker {
                command_key,
                run_id,
                expected_revision,
                reason,
                now_ms,
            }),
        }
    }

    /// Returns `true` if executing this request can create or release wake-up or worker dispatch work.
    pub fn may_affect_scheduling(&self) -> bool {
        matches!(
            self,
            Self::StartDemo { .. }
                | Self::AckEvents { .. }
                | Self::Answer { .. }
                | Self::Steer { .. }
                | Self::Cancel { .. }
                | Self::StartWorker { .. }
                | Self::CancelWorker { .. }
        )
    }

    /// Returns `true` if executing this request mutates persistent binding, run, worker, or cursor state.
    pub const fn is_mutating(&self) -> bool {
        matches!(
            self,
            Self::StartDemo { .. }
                | Self::PullEvents { .. }
                | Self::PullEventsBounded { .. }
                | Self::AckEvents { .. }
                | Self::Answer { .. }
                | Self::Steer { .. }
                | Self::Cancel { .. }
                | Self::StartWorker { .. }
                | Self::CancelWorker { .. }
        )
    }
}

/// Frozen owner-scoped configuration installed on a final `ApprovedHost` runtime supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlConfig {
    /// Approved session authority identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// Immutable SHA-256 hex digest of the approved subject.
    pub subject_digest: String,
    /// Originating control binding identifier (`bnd_<uuid>`).
    pub control_binding_id: String,
    /// Originating native Codex thread UUID.
    pub thread_uuid: String,
    /// Dedicated approved host session binding identifier (`bnd_<uuid>`).
    pub session_binding_id: String,
    /// Public per-start host instance identifier (`hst_<32hex>`).
    pub instance_id: String,
}

/// Owner-scoped verification and compare-and-swap request payload for host control endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlOwnerScope {
    /// Approved session authority identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// Immutable SHA-256 hex digest of the approved subject.
    pub subject_digest: String,
    /// Originating control binding identifier (`bnd_<uuid>`).
    pub control_binding_id: String,
    /// Originating native Codex thread UUID.
    pub thread_uuid: String,
    /// Dedicated approved host session binding identifier (`bnd_<uuid>`).
    pub session_binding_id: String,
    /// Expected live host instance identifier (`hst_<32hex>`).
    pub expected_instance_id: String,
}

/// Request payload for `POST /v1/host-control/info`.
pub type HostControlInfoRequest = HostControlOwnerScope;

/// Request payload for `POST /v1/host-control/stop`.
pub type StopIdleHostRequest = HostControlOwnerScope;

/// Runtime host control lifecycle state reported by a host-control-enabled supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostControlRuntimeState {
    /// Supervisor is ready and accepting requests.
    Ready,
    /// Supervisor has latched an idle-stop request and is draining.
    Stopping,
}

/// Aggregate busy counts across all bindings and live worker tasks on the supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlBusyCounts {
    /// Count of non-terminal runs across all bindings.
    pub non_terminal_runs: usize,
    /// Count of non-terminal workers across all bindings.
    pub non_terminal_workers: usize,
    /// Count of live spawned worker tasks tracked by the supervisor.
    pub live_worker_tasks: usize,
    /// Count of wake-up notices currently in `WakeupStatus::InFlight`.
    pub in_flight_wakeups: usize,
    /// Count of wake-up notices in `WakeupStatus::Pending` when the Codex wake-up sender is enabled (`0` when disabled).
    pub pending_enabled_wakeups: usize,
}

impl HostControlBusyCounts {
    /// Returns `true` if all five busy counters are zero.
    pub const fn is_idle(&self) -> bool {
        self.non_terminal_runs == 0
            && self.non_terminal_workers == 0
            && self.live_worker_tasks == 0
            && self.in_flight_wakeups == 0
            && self.pending_enabled_wakeups == 0
    }
}

/// Authenticated status and idle-stop response returned by `/v1/host-control/info` and `/v1/host-control/stop`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlStatusResponse {
    /// Approved session authority identifier (`apr_<32hex>`).
    pub approval_id: String,
    /// Immutable SHA-256 hex digest of the approved subject.
    pub subject_digest: String,
    /// Originating control binding identifier (`bnd_<uuid>`).
    pub control_binding_id: String,
    /// Originating native Codex thread UUID.
    pub thread_uuid: String,
    /// Dedicated approved host session binding identifier (`bnd_<uuid>`).
    pub session_binding_id: String,
    /// Live host instance identifier (`hst_<32hex>`).
    pub instance_id: String,
    /// Current runtime host control state (`Ready` or `Stopping`).
    pub state: HostControlRuntimeState,
    /// Whether all busy counters in `busy` are zero.
    pub idle: bool,
    /// Detailed breakdown of runtime busy counters.
    pub busy: HostControlBusyCounts,
}

trait IntoWorkerSnapshot {
    fn into_worker_snapshot(self) -> WorkerSnapshot;
}

impl IntoWorkerSnapshot for WorkerSnapshot {
    fn into_worker_snapshot(self) -> WorkerSnapshot {
        self
    }
}

impl IntoWorkerSnapshot for Box<WorkerSnapshot> {
    fn into_worker_snapshot(self) -> WorkerSnapshot {
        *self
    }
}

/// Serializable response returned by the supervisor for a [`ScopedRequest`].
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ScopedResponse {
    /// Compact binding status (`Status`).
    Status(StatusResponse),
    /// Constant-size current run snapshot (`GetRun`, `StartDemo`, `Answer`, `Steer`, `Cancel`).
    Run(Run),
    /// Compact durable worker snapshot (`StartWorker`, `GetWorker`, `CancelWorker`).
    Worker(WorkerSnapshot),
    /// Bounded keyset-paginated run list page (`ListRuns`).
    RunList(RunListPage),
    /// Bounded keyset-paginated run revision history page (`ListRunRevisions`).
    RevisionHistory(RevisionHistoryPage),
    /// Bounded keyset-paginated run question history page (`ListRunQuestions`).
    QuestionHistory(QuestionHistoryPage),
    /// Bounded keyset-paginated read-only activity event page (`ListActivity`).
    #[serde(alias = "activity_page", alias = "activity_history")]
    Activity(ActivityPage),
    /// Ordered event delivery batch (`PullEvents`).
    EventBatch(EventBatch),
    /// Event batch acknowledgement result (`AckEvents`).
    AckResult(AckResult),
}

impl From<CommandOutput> for ScopedResponse {
    fn from(output: CommandOutput) -> Self {
        match output {
            CommandOutput::Run(run) => Self::Run(*run),
            CommandOutput::Worker(worker) => Self::Worker(worker.into_worker_snapshot()),
            CommandOutput::RunList(page) => Self::RunList(page),
            CommandOutput::RevisionHistory(page) => Self::RevisionHistory(page),
            CommandOutput::QuestionHistory(page) => Self::QuestionHistory(page),
            CommandOutput::Activity(page) => Self::Activity(page),
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

    /// Extracts the inner [`WorkerSnapshot`] if this is `ScopedResponse::Worker`.
    pub fn into_worker(self) -> Option<WorkerSnapshot> {
        match self {
            Self::Worker(worker) => Some(worker),
            _ => None,
        }
    }

    /// Extracts the inner [`RunListPage`] if this is `ScopedResponse::RunList`.
    pub fn into_run_list(self) -> Option<RunListPage> {
        match self {
            Self::RunList(page) => Some(page),
            _ => None,
        }
    }

    /// Extracts the inner [`RevisionHistoryPage`] if this is `ScopedResponse::RevisionHistory`.
    pub fn into_revision_history(self) -> Option<RevisionHistoryPage> {
        match self {
            Self::RevisionHistory(page) => Some(page),
            _ => None,
        }
    }

    /// Extracts the inner [`QuestionHistoryPage`] if this is `ScopedResponse::QuestionHistory`.
    pub fn into_question_history(self) -> Option<QuestionHistoryPage> {
        match self {
            Self::QuestionHistory(page) => Some(page),
            _ => None,
        }
    }

    /// Extracts the inner [`ActivityPage`] if this is `ScopedResponse::Activity`.
    pub fn into_activity(self) -> Option<ActivityPage> {
        match self {
            Self::Activity(page) => Some(page),
            _ => None,
        }
    }

    /// Extracts the inner [`ActivityPage`] if this is `ScopedResponse::Activity`.
    pub fn into_activity_page(self) -> Option<ActivityPage> {
        self.into_activity()
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
