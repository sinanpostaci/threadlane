use serde::{Deserialize, Serialize};
use threadlane_core::{
    QuestionStatus, Run, RunStatus, WorkerAcceptanceState, WorkerSnapshot, WorkerStatus,
    MAX_SUMMARY_TEXT_PREVIEW_CHARS,
};

/// Active observer attachment mode of the desktop panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentMode {
    /// Observing the dedicated owned local demo runtime.
    Demo,
    /// Observing an existing external Threadlane runtime.
    Runtime,
    /// No runtime state directory is currently attached.
    Unconfigured,
}

/// Live connection state for the currently attached state directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// Descriptor validated and supervisor responded over authenticated loopback HTTP.
    Connected,
    /// State directory, descriptor, or loopback supervisor is currently offline/unreachable.
    Disconnected,
    /// State directory, descriptor, authentication, or response failed security/protocol checks.
    Blocked,
    /// No state directory is configured.
    Unconfigured,
}

/// Attachment status included in every [`DesktopSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// Active observer mode (`demo`, `runtime`, or `unconfigured`).
    pub mode: AttachmentMode,
    /// Current connection status (`connected`, `disconnected`, `blocked`, or `unconfigured`).
    pub connection: ConnectionState,
    /// Configured state directory path for the active observer, if any.
    pub state_dir: Option<String>,
    /// Short, safe diagnostic message when disconnected, blocked, or unconfigured (`None` when connected).
    pub message: Option<String>,
    /// Wall-clock timestamp (ms since Unix epoch) when this attachment state was produced.
    pub updated_at_ms: i64,
}

/// Compact summary of a configured chat binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingSummary {
    /// Software-generated binding identifier (`bnd_<uuid>`).
    pub binding_id: String,
    /// Canonical hyphenated thread UUID.
    pub thread_uuid: String,
    /// Configured display label for the binding.
    pub label: String,
    /// `true` if this binding is the local deterministic demo binding.
    pub is_local_demo: bool,
}

/// Classification of a task run in the desktop panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    /// Durable isolated worker execution run.
    Worker,
    /// Deterministic local demo state-machine run.
    Demo,
    /// Unclassified run kind.
    Unknown,
}

/// Enriched summary of a recent task run scoped to a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSummary {
    /// Software-generated run identifier (`run_<uuid>`).
    pub run_id: String,
    /// Owning binding identifier (`bnd_<uuid>`).
    pub binding_id: String,
    /// Current 1-based task revision.
    pub revision: u64,
    /// Current lifecycle status in snake_case (`running`, `waiting_input`, `completed`, `failed`, etc.).
    pub status: String,
    /// Bounded single-line task title derived from the instruction.
    pub title: String,
    /// Target provider identifier (`antigravity`, `opencode_v2`, etc.) for worker runs.
    pub provider: Option<String>,
    /// Requested model identifier from the trusted profile for worker runs.
    pub requested_model: Option<String>,
    /// Requested reasoning effort from the trusted profile for worker runs.
    pub requested_effort: Option<String>,
    /// Observed model identifier from provider telemetry, if reported.
    pub observed_model: Option<String>,
    /// Observed reasoning effort from provider telemetry, if reported.
    pub observed_effort: Option<String>,
    /// Latest bounded progress, question, outcome, or failure summary.
    pub last_summary: Option<String>,
    /// Timestamp (ms) when the run was last updated.
    pub updated_at_ms: i64,
    /// `true` if the worker run is missing required model or effort telemetry evidence.
    pub needs_evidence: bool,
    /// Run classification (`worker`, `demo`, or `unknown`).
    pub kind: TaskKind,
    /// Trusted worker profile identifier (`None` for local demo runs).
    pub profile_id: Option<String>,
}

impl TaskSummary {
    /// Constructs a [`TaskSummary`] from a durable [`WorkerSnapshot`], enforcing the invariant
    /// that missing model or effort evidence keeps the status as `failed`.
    pub fn from_worker(worker: &WorkerSnapshot) -> Self {
        let missing_model = worker
            .outcome
            .as_ref()
            .is_some_and(|o| o.missing_model_evidence);
        let missing_effort = worker
            .outcome
            .as_ref()
            .is_some_and(|o| o.missing_effort_evidence);
        let needs_evidence = missing_model || missing_effort;

        let effective_status = if needs_evidence && worker.status == WorkerStatus::Completed {
            WorkerStatus::Failed
        } else {
            worker.status
        };

        let last_summary = match effective_status {
            WorkerStatus::Failed | WorkerStatus::TimedOut | WorkerStatus::Unknown => worker
                .outcome
                .as_ref()
                .and_then(|o| o.failure_reason.clone().or_else(|| o.final_summary.clone()))
                .or_else(|| worker.progress.last_summary.clone())
                .or_else(|| worker.cancel_reason.clone()),
            _ => worker
                .outcome
                .as_ref()
                .and_then(|o| o.final_summary.clone().or_else(|| o.failure_reason.clone()))
                .or_else(|| worker.progress.last_summary.clone())
                .or_else(|| worker.cancel_reason.clone()),
        };

        Self {
            run_id: worker.run_id.clone(),
            binding_id: worker.binding_id.clone(),
            revision: worker.revision,
            status: worker_status_str(effective_status).to_string(),
            title: format_task_title(&worker.instruction),
            provider: Some(worker.provider.clone()),
            requested_model: Some(worker.requested_model.clone()),
            requested_effort: Some(worker.requested_effort.clone()),
            observed_model: worker.observed_model.clone(),
            observed_effort: worker.observed_effort.clone(),
            last_summary,
            updated_at_ms: worker.updated_at_ms,
            needs_evidence,
            kind: TaskKind::Worker,
            profile_id: Some(worker.profile_id.clone()),
        }
    }

    /// Constructs a [`TaskSummary`] from a deterministic local demo [`Run`].
    pub fn from_demo_run(run: &Run) -> Self {
        let last_summary = match run.status {
            RunStatus::WaitingInput => run
                .active_question
                .as_ref()
                .filter(|q| q.status == QuestionStatus::Pending && q.revision == run.revision)
                .map(|q| q.prompt.clone())
                .or_else(|| Some("Waiting for local demo confirmation.".to_string())),
            RunStatus::Completing => Some("Completing local demo run.".to_string()),
            RunStatus::Completed => Some("Local demo run completed.".to_string()),
            RunStatus::Cancelled => run
                .cancel_reason
                .clone()
                .or_else(|| Some("Local demo run cancelled.".to_string())),
            RunStatus::Running => Some("Local demo run in progress.".to_string()),
            _ => None,
        };

        Self {
            run_id: run.run_id.clone(),
            binding_id: run.binding_id.clone(),
            revision: run.revision,
            status: run_status_str(run.status).to_string(),
            title: format_task_title(&run.instruction),
            provider: None,
            requested_model: None,
            requested_effort: None,
            observed_model: None,
            observed_effort: None,
            last_summary,
            updated_at_ms: run.updated_at_ms,
            needs_evidence: false,
            kind: TaskKind::Demo,
            profile_id: None,
        }
    }
}

/// Availability and state of the owned local deterministic demo controls.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoControls {
    /// `true` if a dedicated local demo runtime directory is configured and reachable.
    pub available: bool,
    /// Run ID of the currently owned demo run, if one has been started in this session.
    pub run_id: Option<String>,
    /// Binding ID of the currently owned demo run, if one has been started in this session.
    pub binding_id: Option<String>,
    /// `true` if the owned demo run is currently waiting for an answer on `revision`.
    pub can_answer: bool,
    /// `true` if the owned demo run is currently non-terminal and can be cancelled on `revision`.
    pub can_cancel: bool,
    /// Current revision of the owned demo run, if any.
    pub revision: Option<u64>,
}

/// Complete polling snapshot returned to the desktop UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesktopSnapshot {
    /// Current attachment mode and connection state.
    pub attachment: Attachment,
    /// Configured bindings on the attached runtime (capped at 100).
    pub bindings: Vec<BindingSummary>,
    /// Currently selected binding ID (always scoped to `bindings`, or `None` if `bindings` is empty).
    pub selected_binding_id: Option<String>,
    /// Enriched recent tasks (up to 10) for `selected_binding_id`.
    pub tasks: Vec<TaskSummary>,
    /// Total number of runs recorded for `selected_binding_id`.
    pub total_runs: usize,
    /// Number of active non-terminal runs recorded for `selected_binding_id`.
    pub active_runs: usize,
    /// `true` if `total_runs > tasks.len()`.
    pub has_more: bool,
    /// Owned local demo control state.
    pub demo_controls: DemoControls,
}

/// Active question detail embedded in [`RunDetail`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunQuestion {
    /// Question identifier (`qst_<uuid>`).
    pub question_id: String,
    /// Untrusted question prompt text.
    pub prompt: String,
}

/// Bounded progress event counters embedded in [`RunDetail`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEventCounts {
    /// Count of durable progress events recorded in the event stream.
    pub durable_progress_events: u32,
    /// Count of live progress updates coalesced after reaching the durable cap.
    pub coalesced_progress_events: u32,
    /// Count of live progress updates dropped by the nonblocking observer channel.
    pub dropped_live_events: u32,
}

/// Detailed view of a single worker or local demo run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDetail {
    /// Enriched task summary.
    pub task: TaskSummary,
    /// Full task instruction for the current revision.
    pub instruction: String,
    /// Bounded final outcome summary, if available.
    pub outcome_summary: Option<String>,
    /// Bounded failure or cancellation reason, if available.
    pub failure_reason: Option<String>,
    /// Worker review acceptance state (`pending_review`, `ready_for_review`, `rejected_by_diff_gate`, `execution_failed`, `unknown`), or `None` for demo runs.
    pub acceptance_state: Option<String>,
    /// Worker process exit code, if exited normally.
    pub exit_code: Option<i32>,
    /// Task or run worktree branch name, if available.
    pub branch_name: Option<String>,
    /// Task or run worktree filesystem path, if available.
    pub worktree_path: Option<String>,
    /// Latest bounded activity or progress summary.
    pub latest_activity: Option<String>,
    /// Pending question on the current revision for a demo run, if any.
    pub question: Option<RunQuestion>,
    /// Progress event counters.
    pub counts: RunEventCounts,
    /// `true` if the worker run lacked verifiable model telemetry.
    pub missing_model_evidence: bool,
    /// `true` if the worker run lacked verifiable effort telemetry.
    pub missing_effort_evidence: bool,
    /// Demo controls scoped to this run (only enabled if this run is the active owned demo tuple).
    pub demo_controls: DemoControls,
}

impl RunDetail {
    /// Constructs a [`RunDetail`] from a [`WorkerSnapshot`], preserving `Failed`/`ExecutionFailed`
    /// when model or effort evidence is missing.
    pub fn from_worker(worker: &WorkerSnapshot, demo_controls: DemoControls) -> Self {
        let missing_model_evidence = worker
            .outcome
            .as_ref()
            .is_some_and(|o| o.missing_model_evidence);
        let missing_effort_evidence = worker
            .outcome
            .as_ref()
            .is_some_and(|o| o.missing_effort_evidence);
        let needs_evidence = missing_model_evidence || missing_effort_evidence;

        let task = TaskSummary::from_worker(worker);

        let raw_acceptance = worker
            .outcome
            .as_ref()
            .map(|o| o.acceptance_state)
            .unwrap_or_else(|| {
                if worker.status.is_terminal() {
                    WorkerAcceptanceState::Unknown
                } else {
                    WorkerAcceptanceState::PendingReview
                }
            });
        let effective_acceptance =
            if needs_evidence && raw_acceptance == WorkerAcceptanceState::ReadyForReview {
                WorkerAcceptanceState::ExecutionFailed
            } else {
                raw_acceptance
            };

        let outcome_summary = worker
            .outcome
            .as_ref()
            .and_then(|o| o.final_summary.clone());
        let failure_reason = worker
            .outcome
            .as_ref()
            .and_then(|o| o.failure_reason.clone())
            .or_else(|| worker.cancel_reason.clone());
        let exit_code = worker.outcome.as_ref().and_then(|o| o.exit_code);
        let branch_name = worker
            .worktree
            .task_branch_name
            .clone()
            .or_else(|| worker.worktree.run_branch_name.clone());
        let worktree_path = worker
            .worktree
            .task_worktree_path
            .clone()
            .or_else(|| worker.worktree.run_worktree_path.clone());
        let latest_activity = worker
            .progress
            .last_summary
            .clone()
            .or_else(|| outcome_summary.clone())
            .or_else(|| failure_reason.clone());

        let scoped_controls = DemoControls {
            available: demo_controls.available,
            run_id: demo_controls.run_id,
            binding_id: demo_controls.binding_id,
            can_answer: false,
            can_cancel: false,
            revision: demo_controls.revision,
        };

        Self {
            task,
            instruction: worker.instruction.clone(),
            outcome_summary,
            failure_reason,
            acceptance_state: Some(acceptance_state_str(effective_acceptance).to_string()),
            exit_code,
            branch_name,
            worktree_path,
            latest_activity,
            question: None,
            counts: RunEventCounts {
                durable_progress_events: worker.progress.durable_progress_events,
                coalesced_progress_events: worker.progress.coalesced_progress_events,
                dropped_live_events: worker.progress.dropped_live_events,
            },
            missing_model_evidence,
            missing_effort_evidence,
            demo_controls: scoped_controls,
        }
    }

    /// Constructs a [`RunDetail`] from a deterministic local demo [`Run`].
    pub fn from_demo_run(run: &Run, demo_controls: DemoControls) -> Self {
        let task = TaskSummary::from_demo_run(run);
        let is_owned_run = demo_controls.binding_id.as_deref() == Some(run.binding_id.as_str())
            && demo_controls.run_id.as_deref() == Some(run.run_id.as_str());

        let scoped_controls = if is_owned_run {
            demo_controls
        } else {
            DemoControls {
                available: demo_controls.available,
                run_id: demo_controls.run_id,
                binding_id: demo_controls.binding_id,
                can_answer: false,
                can_cancel: false,
                revision: demo_controls.revision,
            }
        };

        let question = run
            .active_question
            .as_ref()
            .filter(|q| q.status == QuestionStatus::Pending && q.revision == run.revision)
            .map(|q| RunQuestion {
                question_id: q.question_id.clone(),
                prompt: q.prompt.clone(),
            });

        let outcome_summary = if run.status == RunStatus::Completed {
            Some("Local demo run completed.".to_string())
        } else {
            None
        };
        let failure_reason = if run.status == RunStatus::Cancelled {
            run.cancel_reason
                .clone()
                .or_else(|| Some("Local demo run cancelled.".to_string()))
        } else {
            None
        };

        Self {
            latest_activity: task.last_summary.clone(),
            task,
            instruction: run.instruction.clone(),
            outcome_summary,
            failure_reason,
            acceptance_state: None,
            exit_code: None,
            branch_name: None,
            worktree_path: None,
            question,
            counts: RunEventCounts::default(),
            missing_model_evidence: false,
            missing_effort_evidence: false,
            demo_controls: scoped_controls,
        }
    }
}

/// Discovery state of a local tool or integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAvailability {
    /// Executable was found on `PATH`.
    Found,
    /// Executable was not found on `PATH`.
    NotFound,
    /// Integration is not configured in this milestone (e.g., Jev).
    NotConfigured,
}

/// Passive `PATH` discovery status for a local tool (never spawns processes or checks auth).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolStatus {
    /// Stable tool identifier (`codex`, `opencode`, `antigravity`, `git`, `jev`).
    pub id: String,
    /// Human-readable display label.
    pub label: String,
    /// Resolved executable path on `PATH` when `availability == ToolAvailability::Found`.
    pub executable_path: Option<String>,
    /// Tool availability state (`found`, `not_found`, or `not_configured`).
    pub availability: ToolAvailability,
    /// Honest note explaining discovery scope.
    pub note: String,
}

/// Window management action requested by the desktop UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowAction {
    /// Close the desktop panel window without stopping external runtimes.
    Close,
    /// Minimize the desktop panel window.
    Minimize,
}

pub(crate) fn format_task_title(instruction: &str) -> String {
    let collapsed = instruction.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return "Untitled task".to_string();
    }
    let char_count = collapsed.chars().count();
    if char_count <= MAX_SUMMARY_TEXT_PREVIEW_CHARS {
        return collapsed;
    }
    let preview: String = collapsed
        .chars()
        .take(MAX_SUMMARY_TEXT_PREVIEW_CHARS)
        .collect();
    format!("{preview}...")
}

pub(crate) fn run_status_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Claimed => "claimed",
        RunStatus::Running => "running",
        RunStatus::WaitingInput => "waiting_input",
        RunStatus::Completing => "completing",
        RunStatus::CancelRequested => "cancel_requested",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::TimedOut => "timed_out",
        RunStatus::Unknown => "unknown",
    }
}

pub(crate) fn worker_status_str(status: WorkerStatus) -> &'static str {
    match status {
        WorkerStatus::Queued => "queued",
        WorkerStatus::Claimed => "claimed",
        WorkerStatus::Running => "running",
        WorkerStatus::CancelRequested => "cancel_requested",
        WorkerStatus::Completed => "completed",
        WorkerStatus::Failed => "failed",
        WorkerStatus::Cancelled => "cancelled",
        WorkerStatus::TimedOut => "timed_out",
        WorkerStatus::Unknown => "unknown",
    }
}

pub(crate) fn acceptance_state_str(state: WorkerAcceptanceState) -> &'static str {
    match state {
        WorkerAcceptanceState::PendingReview => "pending_review",
        WorkerAcceptanceState::ReadyForReview => "ready_for_review",
        WorkerAcceptanceState::RejectedByDiffGate => "rejected_by_diff_gate",
        WorkerAcceptanceState::ExecutionFailed => "execution_failed",
        WorkerAcceptanceState::Unknown => "unknown",
    }
}
