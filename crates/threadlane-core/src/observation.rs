use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Schema version for [`ObservationSnapshot`] payloads.
pub const OBSERVATION_SCHEMA_VERSION: u32 = 1;

/// File name for the append-only per-store observation journal.
pub const OBSERVATION_JOURNAL_FILE_NAME: &str = "observation-journal.jsonl";

/// Maximum byte size (`16 MiB`) permitted when reading an observation journal file.
pub const MAX_OBSERVATION_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum byte length (`8 KiB`) of a single serialized journal record line.
pub const MAX_OBSERVATION_RECORD_LINE_BYTES: usize = 8 * 1024;

/// Maximum length in characters of a sanitized summary or message field.
pub const MAX_OBSERVATION_SUMMARY_CHARS: usize = 240;

/// Optional environment variable used only for parent-child observation request correlation.
///
/// This variable is untrusted observational metadata and grants no execution, scope, or binding
/// authority.
pub const ENV_OBSERVATION_PARENT_REQUEST_ID: &str = "THREADLANE_OBSERVATION_PARENT_REQUEST_ID";

/// Optional environment variable used only for observational plan correlation.
///
/// This variable is untrusted observational metadata and grants no execution, scope, or binding
/// authority.
pub const ENV_OBSERVATION_PLAN_ID: &str = "THREADLANE_OBSERVATION_PLAN_ID";

/// Availability state of an [`ObservationSource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationSourceAvailability {
    /// The observation source is readable and passed validation.
    Available,
    /// The observation source is missing, unreadable, or failed validation.
    Unavailable,
}

impl ObservationSourceAvailability {
    /// Returns the canonical `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Configured or linked store source surfaced in the observation feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSource {
    /// Stable source identifier (`default` or `ctx_<hash>`).
    pub id: String,
    /// Human-readable label for the source or project.
    pub label: String,
    /// Canonical project path when known.
    pub project_path: Option<String>,
    /// Current read availability of the source.
    pub availability: ObservationSourceAvailability,
    /// Diagnostic or status message when unavailable or degraded.
    pub message: Option<String>,
}

/// Evidence classification for an [`ObservationActor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationActorEvidence {
    /// Actor identity is not proven by trusted correlation, binding, or handoff receipts.
    Unknown,
    /// Actor attributes reflect explicit configuration or a declared conversation role on a
    /// verified connection, not observed model execution.
    Configured,
    /// Actor attributes were directly observed from a verified runtime or handoff receipt.
    Observed,
    /// Request arrived over the trusted local operator boundary or enrolled operator session.
    Operator,
}

impl ObservationActorEvidence {
    /// Returns the canonical `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Configured => "configured",
            Self::Observed => "observed",
            Self::Operator => "operator",
        }
    }
}

/// Requester / actor metadata attached to an [`ObservationRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationActor {
    /// Human-readable actor label when verified or configured; `None` when unknown.
    pub label: Option<String>,
    /// Workflow or declared conversation role when proven by a role-flow handoff, receipt, or
    /// verified connection context (for example `"coworker"`).
    pub role: Option<String>,
    /// Boundary or transport source (for example `trusted_local_operator` or `paired_mcp`).
    pub source: Option<String>,
    /// Actor model only when proven by runtime receipt; never inferred from profile defaults or thread configuration.
    pub model: Option<String>,
    /// Actor reasoning effort only when proven by runtime receipt; never inferred from profile defaults or thread configuration.
    pub effort: Option<String>,
    /// Provenance level of the actor metadata.
    pub evidence: ObservationActorEvidence,
}

impl ObservationActor {
    /// Returns an actor record with `unknown` evidence and all optional fields `None`.
    pub const fn unknown(source: Option<String>) -> Self {
        Self {
            label: None,
            role: None,
            source,
            model: None,
            effort: None,
            evidence: ObservationActorEvidence::Unknown,
        }
    }

    /// Returns a verified paired-MCP caller actor record with display label `"Codex · <thread_label>"`,
    /// declared conversation role `"coworker"`, source `"paired_mcp"`, `Configured` evidence, and
    /// `None` for `model` and `effort` (which are never inferred from profile defaults or thread configuration).
    pub fn paired_coworker(thread_label: &str) -> Self {
        let sanitized = sanitize_observation_label(thread_label, "Codex chat");
        Self {
            label: Some(format!("Codex · {sanitized}")),
            role: Some("coworker".to_string()),
            source: Some("paired_mcp".to_string()),
            model: None,
            effort: None,
            evidence: ObservationActorEvidence::Configured,
        }
    }

    /// Compatibility alias for [`Self::paired_coworker`].
    pub fn paired_follower(thread_label: &str) -> Self {
        Self::paired_coworker(thread_label)
    }
}

/// Tool, provider, role, and separated requested vs. observed model/effort metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationExecutor {
    /// Tool identifier or adapter name.
    pub tool: Option<String>,
    /// Provider identifier.
    pub provider: Option<String>,
    /// Assigned workflow role when applicable.
    pub role: Option<String>,
    /// Model requested by policy, profile, or approved plan.
    pub requested_model: Option<String>,
    /// Model actually reported by the runtime or provider receipt.
    pub observed_model: Option<String>,
    /// Reasoning effort requested by policy, profile, or approved plan.
    pub requested_effort: Option<String>,
    /// Reasoning effort actually reported by the runtime or provider receipt.
    pub observed_effort: Option<String>,
}

impl ObservationExecutor {
    /// Returns the local MCP handler executor record (`tool: "threadlane_mcp"`, `provider: "local"`,
    /// and `None` for role, requested/observed model, and requested/observed effort).
    ///
    /// Describes only the local Threadlane MCP handler itself, never its caller or worker children.
    pub fn local_mcp() -> Self {
        Self {
            tool: Some("threadlane_mcp".to_string()),
            provider: Some("local".to_string()),
            role: None,
            requested_model: None,
            observed_model: None,
            requested_effort: None,
            observed_effort: None,
        }
    }
}

/// Sanitizes a short display label (such as a chat title or project name), stripping control
/// characters and bounding length to [`crate::MAX_LABEL_LEN`] bytes.
pub fn sanitize_observation_label(raw: &str, fallback: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if !ch.is_control() && out.len() + ch.len_utf8() <= crate::MAX_LABEL_LEN {
            out.push(ch);
        }
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

/// Display-only observation enrichment context cached after independent native identity
/// verification and pairing enrollment.
///
/// Carries no execution, approval, or binding authority and is never persisted into approval files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedMcpObservationContext {
    /// Exact enrolled software `binding_id` that owns this context.
    pub binding_id: String,
    /// Exact verified native `thread_uuid` that owns this context.
    pub thread_uuid: String,
    /// Sanitized human-readable chat title from verified native thread metadata.
    pub thread_label: String,
    /// Sanitized human-readable project label derived from verified canonical `cwd` / project identity.
    pub project_label: String,
    /// Canonical project directory path from verified `MatchedThreadCandidate`.
    pub project_path: Option<String>,
    /// Verified caller actor record (`Codex · <thread_label>`, role `coworker`, source `paired_mcp`, evidence `Configured`).
    pub actor: ObservationActor,
}

impl VerifiedMcpObservationContext {
    /// Constructs a display-only observation context from verified binding/thread scope,
    /// native thread title, and canonical project directory / project identifier.
    pub fn new(
        binding_id: impl Into<String>,
        thread_uuid: impl Into<String>,
        thread_title: &str,
        canonical_cwd: &Path,
        project_id: Option<&str>,
    ) -> Self {
        let thread_label = sanitize_observation_label(thread_title, "Codex chat");
        let cwd_name = canonical_cwd
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let raw_project_label = if !cwd_name.trim().is_empty() {
            cwd_name
        } else {
            project_id.unwrap_or("Project")
        };
        let project_label = sanitize_observation_label(raw_project_label, "Project");
        let project_path = canonical_cwd
            .to_str()
            .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
            .map(str::to_string);
        let actor = ObservationActor::paired_coworker(&thread_label);
        Self {
            binding_id: binding_id.into(),
            thread_uuid: thread_uuid.into(),
            thread_label,
            project_label,
            project_path,
            actor,
        }
    }

    /// Returns `Some(self)` only when both `binding_id` and `thread_uuid` match the active call's
    /// verified scope.
    pub fn matching_scope(&self, binding_id: &str, thread_uuid: &str) -> Option<&Self> {
        if self.binding_id == binding_id && self.thread_uuid == thread_uuid {
            Some(self)
        } else {
            None
        }
    }
}

/// Per-call token usage and provenance metadata.
///
/// Values are populated only when actually reported by the provider or cache metadata; never
/// inferred or summed across parent and child calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationUsage {
    /// Reported input tokens, or `None` when unknown.
    pub input_tokens: Option<u64>,
    /// Reported output tokens, or `None` when unknown.
    pub output_tokens: Option<u64>,
    /// Reported cache-read tokens, or `None` when unknown.
    pub cache_read_tokens: Option<u64>,
    /// Reported total tokens, or `None` when unknown.
    pub total_tokens: Option<u64>,
    /// Provenance description (for example `cache_hit`, `network_response`, or `unreported`).
    pub provenance: String,
}

/// Delivery lifecycle stage for an [`ObservationDelivery`] record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationDeliveryStage {
    /// A worker or operation produced a durable result ready for delivery.
    ResultReady,
    /// A wake-up notice was submitted to the native notification channel.
    NotificationSubmitted,
    /// An event batch was pulled/delivered over a scoped binding.
    BatchDelivered,
    /// An event batch was durably acknowledged up to a covered sequence number.
    BatchAcknowledged,
    /// A synchronous CLI, MCP, or Jev API response was returned to the caller.
    ResponseReturned,
    /// Delivery state is uncertain after process loss or in-flight interruption.
    Unknown,
    /// Delivery or notification submission failed.
    Failed,
}

impl ObservationDeliveryStage {
    /// Returns the canonical `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ResultReady => "result_ready",
            Self::NotificationSubmitted => "notification_submitted",
            Self::BatchDelivered => "batch_delivered",
            Self::BatchAcknowledged => "batch_acknowledged",
            Self::ResponseReturned => "response_returned",
            Self::Unknown => "unknown",
            Self::Failed => "failed",
        }
    }
}

/// Separate delivery/return lifecycle event attached to an [`ObservationRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationDelivery {
    /// Delivery stage.
    pub stage: ObservationDeliveryStage,
    /// Timestamp in milliseconds since the Unix epoch when known.
    pub at_ms: Option<i64>,
    /// Sanitized human-readable summary of the delivery event.
    pub summary: String,
    /// Recipient identifier (such as `binding_id`), never claiming an unverified human/chat reader.
    pub recipient: Option<String>,
}

/// Execution state for an [`ObservationCall`] or [`ObservationRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationCallState {
    /// Queued and awaiting execution.
    Queued,
    /// Currently executing with a live process or active claim.
    Running,
    /// Waiting for user input or approval.
    WaitingForUser,
    /// Execution finished with a zero exit code or completed receipt (not equivalent to review acceptance).
    Completed,
    /// Output is ready for review and awaiting a valid current-candidate review receipt.
    ReadyForReview,
    /// Execution or validation failed.
    Failed,
    /// Execution was explicitly cancelled.
    Cancelled,
    /// Execution outcome is unknown (e.g., process loss without a completion receipt).
    Unknown,
}

impl ObservationCallState {
    /// Returns the canonical `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingForUser => "waiting_for_user",
            Self::Completed => "completed",
            Self::ReadyForReview => "ready_for_review",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    /// Returns `true` if the state represents an active (non-terminal) call.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::WaitingForUser)
    }
}

/// Child tool, role, or worker call under an [`ObservationRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationCall {
    /// Stable call identifier within the source.
    pub id: String,
    /// Human-readable call title.
    pub title: String,
    /// Current state of the child call.
    pub state: ObservationCallState,
    /// Executor metadata when applicable.
    pub executor: Option<ObservationExecutor>,
    /// Measured start timestamp in milliseconds since the Unix epoch.
    pub started_at_ms: Option<i64>,
    /// Measured finish timestamp in milliseconds since the Unix epoch.
    pub finished_at_ms: Option<i64>,
    /// Measured duration in milliseconds (`finished_at_ms - started_at_ms` when both are known).
    pub duration_ms: Option<i64>,
    /// Reported per-call token usage, or `None` when unknown.
    pub usage: Option<ObservationUsage>,
    /// Sanitized call summary.
    pub summary: Option<String>,
}

/// Durable parent request entry in the observation feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationRequest {
    /// Source identifier scoping `id` and `job_id`.
    pub source_id: String,
    /// Request identifier within `source_id`.
    pub id: String,
    /// Associated job identifier within `source_id` when correlated; `None` when unlinked.
    pub job_id: Option<String>,
    /// Optional parent request identifier when correlated.
    pub parent_request_id: Option<String>,
    /// Human-readable request title.
    pub title: String,
    /// Canonical safe operation label.
    pub operation: String,
    /// Project label for display.
    pub project_label: String,
    /// Scoped software `binding_id` when bound.
    pub binding_id: Option<String>,
    /// Validated `thread_uuid` when bound.
    pub thread_uuid: Option<String>,
    /// Human-readable thread/binding label when known.
    pub thread_label: Option<String>,
    /// Current state of the request.
    pub state: ObservationCallState,
    /// Measured start timestamp in milliseconds since the Unix epoch.
    pub started_at_ms: Option<i64>,
    /// Measured finish timestamp in milliseconds since the Unix epoch.
    pub finished_at_ms: Option<i64>,
    /// Latest update timestamp in milliseconds since the Unix epoch.
    pub updated_at_ms: i64,
    /// Measured duration in milliseconds when both start and finish timestamps are known.
    pub duration_ms: Option<i64>,
    /// Requester / actor metadata and provenance.
    pub actor: ObservationActor,
    /// Executor metadata when applicable.
    pub executor: Option<ObservationExecutor>,
    /// Direct request token usage (never summing child usage twice).
    pub usage: Option<ObservationUsage>,
    /// Ordered delivery stages recorded for this request.
    pub delivery: Vec<ObservationDelivery>,
    /// Causal child calls (tools, role steps, worker runs).
    pub children: Vec<ObservationCall>,
    /// Total count of causal child calls.
    pub children_count: usize,
    /// Sanitized request summary.
    pub summary: Option<String>,
}

/// Task summary within an [`ObservationJob`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationTask {
    /// Task identifier within the plan/job.
    pub id: String,
    /// Human-readable task title.
    pub title: String,
    /// Truthful task status (`pending`, `running`, `ready_for_review`, `completed`, `accepted`, `failed`, `unknown`, etc.).
    pub status: String,
    /// Task IDs that this task depends on.
    pub depends_on: Vec<String>,
}

/// High-level lifecycle state of an [`ObservationJob`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationJobState {
    /// Plan is being drafted or proposed.
    Planning,
    /// Plan is approved and ready for execution.
    Ready,
    /// One or more tasks or role steps are actively running.
    Running,
    /// Work is awaiting review or in an active review cycle.
    Reviewing,
    /// Job is blocked on user approval, decision, or question response.
    NeedsUser,
    /// Job has completed with verified acceptance/review evidence where applicable.
    Completed,
    /// Job execution or review failed or was rejected.
    Failed,
    /// Job state is uncertain (for example after host/worker loss).
    Unknown,
}

impl ObservationJobState {
    /// Returns the canonical `snake_case` wire representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Reviewing => "reviewing",
            Self::NeedsUser => "needs_user",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

/// Durable job projection scoped by `(source_id, id)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationJob {
    /// Source identifier scoping `id`.
    pub source_id: String,
    /// Job identifier within `source_id` (typically the durable `plan_id`).
    pub id: String,
    /// Human-readable project label.
    pub project_label: String,
    /// Canonical project path when known.
    pub project_path: Option<String>,
    /// Human-readable job/plan title.
    pub title: String,
    /// Bound `thread_uuid` when known.
    pub thread_uuid: Option<String>,
    /// Bound human-readable thread label when known.
    pub thread_label: Option<String>,
    /// Scoped `binding_id` when known.
    pub binding_id: Option<String>,
    /// Durable `plan_id` when backed by a plan.
    pub plan_id: Option<String>,
    /// Latest or active plan revision number when backed by a plan.
    pub plan_revision: Option<u64>,
    /// High-level job state.
    pub state: ObservationJobState,
    /// Truthful approval status (`draft`, `pending_approval`, `approved`, `superseded`, `revoked`, `expired`, `rejected`, `unapproved`).
    pub approval_status: String,
    /// Truthful execution status (`not_started`, `running`, `ready_for_review`, `completed`, `accepted`, `failed`, `stopped`, `unknown`).
    pub execution_status: String,
    /// Truthful review state (`pending_review`, `review_passed`, `changes_requested`, `unresolved`, `invalidated`, `unknown`, or `None`).
    pub review_state: Option<String>,
    /// Latest update timestamp in milliseconds since the Unix epoch.
    pub updated_at_ms: Option<i64>,
    /// Count of currently active calls for this job.
    pub active_call_count: usize,
    /// Tasks belonging to this job.
    pub tasks: Vec<ObservationTask>,
    /// Sanitized job summary.
    pub summary: Option<String>,
}

/// Read-only point-in-time observation snapshot returned to the desktop UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSnapshot {
    /// Wire schema version (`1`).
    pub schema_version: u32,
    /// Snapshot generation timestamp in milliseconds since the Unix epoch.
    pub generated_at_ms: i64,
    /// Configured and linked observation sources.
    pub sources: Vec<ObservationSource>,
    /// Projected jobs across available sources (filtered by `source_id` / `job_id` when requested).
    pub jobs: Vec<ObservationJob>,
    /// Paginated requests across available sources (filtered by `source_id` / `job_id` when requested).
    pub requests: Vec<ObservationRequest>,
    /// Opaque pagination cursor for the next page of requests, or `None` when at the end.
    pub next_cursor: Option<String>,
    /// Whether additional request pages exist beyond `requests`.
    pub has_more: bool,
    /// Whether additional jobs exist beyond the bounded `jobs` cap.
    pub jobs_has_more: bool,
    /// Per-source or journal warnings encountered during read-only projection.
    pub warnings: Vec<String>,
}

/// Optional filter and pagination arguments for `ipcObservationSnapshot`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSnapshotRequest {
    /// Optional source filter (`source_id`).
    #[serde(default)]
    pub source_id: Option<String>,
    /// Optional job filter (`job_id`).
    #[serde(default)]
    pub job_id: Option<String>,
    /// Optional pagination cursor from a previous [`ObservationSnapshot::next_cursor`].
    #[serde(default)]
    pub cursor: Option<String>,
    /// Optional maximum number of requests to return in this page.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Lifecycle phase of an append-only [`ObservationJournalRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationRecordPhase {
    /// Recorded immediately at request/call entry or spawn before execution completes.
    Entry,
    /// Recorded on intermediate progress, child call updates, or delivery events.
    Progress,
    /// Recorded when the request/call finishes (completed, failed, cancelled, or unknown).
    Finish,
}

/// OS process instance identity proof combining PID, kernel boot ID, and process start ticks.
///
/// Used to verify that an in-flight journal record still belongs to the exact same running OS
/// process rather than a reused PID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationProcessInstance {
    /// Operating system process identifier (`PID`).
    pub pid: u32,
    /// Kernel boot identifier from `/proc/sys/kernel/random/boot_id`.
    pub boot_id: String,
    /// Process start time in clock ticks since boot (field 22 of `/proc/<pid>/stat`).
    pub start_ticks: u64,
}

/// Append-only journal record persisted in `observation-journal.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationJournalRecord {
    /// Unique record identifier (`obsrec_<uuid>`).
    pub record_id: String,
    /// Stable parent or standalone request identifier (`obsreq_<uuid>`).
    pub request_id: String,
    /// Optional parent request identifier when this request was spawned inside another request.
    ///
    /// Untrusted correlation hint only; never proves execution, actor, binding, thread, or job authority.
    #[serde(default)]
    pub parent_request_id: Option<String>,
    /// Optional child call identifier (`obsc_<uuid>`) when this record updates a child [`ObservationCall`].
    #[serde(default)]
    pub call_id: Option<String>,
    /// Entry, progress, or finish phase.
    pub phase: ObservationRecordPhase,
    /// Timestamp in milliseconds since the Unix epoch.
    pub at_ms: i64,
    /// OS process ID that wrote the record (diagnostic only; never sufficient alone to claim `Running`).
    #[serde(default)]
    pub pid: Option<u32>,
    /// Optional OS process-instance proof (`pid`, `boot_id`, and `start_ticks`) used to detect PID reuse and process loss.
    #[serde(default)]
    pub process_instance: Option<ObservationProcessInstance>,
    /// Optional project label known at the boundary.
    #[serde(default)]
    pub project_label: Option<String>,
    /// Optional canonical project path known at the boundary.
    #[serde(default)]
    pub project_path: Option<String>,
    /// Optional scoped `binding_id`.
    #[serde(default)]
    pub binding_id: Option<String>,
    /// Optional validated `thread_uuid`.
    #[serde(default)]
    pub thread_uuid: Option<String>,
    /// Optional human-readable thread label.
    #[serde(default)]
    pub thread_label: Option<String>,
    /// Optional correlated `job_id`.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Optional correlated `plan_id`.
    ///
    /// Untrusted correlation hint unless verified against a real plan/binding/receipt.
    #[serde(default)]
    pub plan_id: Option<String>,
    /// Optional correlated `task_id`.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Optional correlated `approval_id`.
    #[serde(default)]
    pub approval_id: Option<String>,
    /// Canonical safe operation name (e.g. `context.workflow.propose`).
    pub operation: String,
    /// Safe human-readable title.
    pub title: String,
    /// Observed state at this phase.
    pub state: ObservationCallState,
    /// Optional actor metadata.
    #[serde(default)]
    pub actor: Option<ObservationActor>,
    /// Optional executor metadata.
    #[serde(default)]
    pub executor: Option<ObservationExecutor>,
    /// Optional actual token usage metadata.
    #[serde(default)]
    pub usage: Option<ObservationUsage>,
    /// Delivery stages recorded at this phase.
    #[serde(default)]
    pub delivery: Vec<ObservationDelivery>,
    /// Sanitized summary (never raw prompts, secrets, or conversation bodies).
    #[serde(default)]
    pub summary: Option<String>,
}

impl ObservationJournalRecord {
    /// Returns `true` only when this record carries a valid [`ObservationProcessInstance`] proof
    /// that matches any recorded `pid` and is currently alive on this host.
    ///
    /// A bare `pid` without `process_instance` proof never returns `true`.
    pub fn has_live_process_instance(&self) -> bool {
        let Some(instance) = &self.process_instance else {
            return false;
        };
        if let Some(pid) = self.pid {
            if pid != instance.pid {
                return false;
            }
        }
        is_process_instance_alive(instance)
    }

    /// Resolves the truthful state for an in-flight (non-finish) journal record.
    ///
    /// If the record state is active (`Queued`, `Running`, or `WaitingForUser`) and the record
    /// phase is not `Finish`, the state remains active only when backed by either a live
    /// [`ObservationProcessInstance`] proof or an independent active worker DB receipt.
    /// Otherwise, it degrades to [`ObservationCallState::Unknown`].
    pub fn resolve_in_flight_state(&self, has_active_worker_receipt: bool) -> ObservationCallState {
        if self.phase != ObservationRecordPhase::Finish && self.state.is_active() {
            // A long-lived MCP server can outlive a cancelled request future.
            let one_shot_process_is_live =
                !self.operation.starts_with("mcp.") && self.has_live_process_instance();
            if one_shot_process_is_live || has_active_worker_receipt {
                self.state
            } else {
                ObservationCallState::Unknown
            }
        } else {
            self.state
        }
    }
}

/// Errors produced when reading or appending to the observation journal.
#[derive(Debug, Error)]
pub enum ObservationJournalError {
    /// Target directory or journal file failed private non-symlink validation.
    #[error("observation journal path failed security validation: {0}")]
    SecurityViolation(String),
    /// Journal file exceeds the bounded read limit.
    #[error("observation journal exceeds maximum size limit")]
    JournalTooLarge,
    /// Serialized journal line exceeds the bounded line limit.
    #[error("observation journal record exceeds maximum line size")]
    RecordTooLarge,
    /// Underlying filesystem I/O error.
    #[error("observation journal I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization error.
    #[error("observation journal serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Generates a fresh prefixed observation identifier (`<prefix>_<uuid>`).
pub fn generate_observation_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

/// Returns the current system clock timestamp in milliseconds since the Unix epoch.
pub fn current_observation_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn contains_sk_prefix_marker(lower: &str) -> bool {
    let bytes = lower.as_bytes();
    bytes.windows(3).enumerate().any(|(idx, window)| {
        window == b"sk-" && (idx == 0 || !bytes[idx - 1].is_ascii_alphanumeric())
    })
}

fn contains_sensitive_marker(lower: &str) -> bool {
    contains_sk_prefix_marker(lower)
        || lower.contains("bearer")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("authorization")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("prompt")
        || lower.contains("instruction")
}

/// Validates an optional correlation identifier (`plan_id`, `parent_request_id`, `job_id`, etc.).
///
/// Permits only bounded ASCII identifiers (`1..=128` chars in `[A-Za-z0-9_.:-]`) that do not
/// contain sensitive or secret-like markers. Returns `None` if the input is empty or invalid.
pub fn validate_correlation_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return None;
    }
    if !trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
    {
        return None;
    }
    if contains_sensitive_marker(&trimmed.to_ascii_lowercase()) {
        return None;
    }
    Some(trimmed.to_string())
}

#[cfg(test)]
mod correlation_marker_regression_tests {
    use super::{contains_sensitive_marker, sanitize_observation_summary, validate_correlation_id};

    #[test]
    fn preserves_ordinary_task_correlation_ids_while_rejecting_sensitive_markers() {
        for valid in [
            "task-obs-main",
            "task-root-a",
            "task-0123456789abcdef0123456789abcdef",
            "plan_01:step.2",
        ] {
            assert_eq!(
                validate_correlation_id(valid),
                Some(valid.to_string()),
                "expected valid correlation id to be preserved: {valid}"
            );
        }

        for sk_secret in [
            "sk-1234567890abcdef",
            "SK-UPPER-123456",
            "prefix:sk-123456",
            "task-sk-123456",
            "scope_sk-123456",
            "nested.sk-123456",
        ] {
            assert_eq!(
                validate_correlation_id(sk_secret),
                None,
                "expected sk- token-boundary marker to be rejected: {sk_secret}"
            );
        }

        assert!(contains_sensitive_marker("sk-live-key"));
        assert!(contains_sensitive_marker("prefix:sk-live-key"));
        assert!(contains_sensitive_marker("task-sk-live-key"));
        assert!(contains_sensitive_marker("env_sk-live-key"));
        assert!(!contains_sensitive_marker("task-obs-main"));
        assert!(!contains_sensitive_marker("task-root-a"));
        assert_eq!(
            sanitize_observation_summary("task=task-obs-main duration_ms=7"),
            "task=task-obs-main duration_ms=7"
        );
        assert_eq!(
            sanitize_observation_summary("value=sk-1234567890abcdef duration_ms=7"),
            "[redacted sensitive metadata]"
        );

        for other_sensitive in [
            "bearer-xyz",
            "my_api_key_1",
            "apikey:123",
            "authorization-1",
            "task-secret-1",
            "password-1",
            "task-token-1",
            "prompt-1",
            "instruction-1",
        ] {
            assert_eq!(
                validate_correlation_id(other_sensitive),
                None,
                "expected sensitive marker to be rejected: {other_sensitive}"
            );
        }

        let max_len_id = "a".repeat(128);
        let over_len_id = "a".repeat(129);
        assert_eq!(validate_correlation_id(&max_len_id), Some(max_len_id));
        assert_eq!(validate_correlation_id(&over_len_id), None);
        assert_eq!(validate_correlation_id(""), None);
        assert_eq!(validate_correlation_id("   "), None);
        assert_eq!(validate_correlation_id("task/obs"), None);
        assert_eq!(validate_correlation_id("task obs"), None);
    }
}

/// Reads optional untrusted parent-request correlation from the process environment.
pub fn read_env_parent_request_id() -> Option<String> {
    std::env::var(ENV_OBSERVATION_PARENT_REQUEST_ID)
        .ok()
        .and_then(|v| validate_correlation_id(&v))
}

/// Reads optional untrusted plan correlation from the process environment.
pub fn read_env_plan_id() -> Option<String> {
    std::env::var(ENV_OBSERVATION_PLAN_ID)
        .ok()
        .and_then(|v| validate_correlation_id(&v))
}

/// Sanitizes a short metadata summary, stripping control characters, newlines, and secret-like
/// patterns and truncating to [`MAX_OBSERVATION_SUMMARY_CHARS`].
pub fn sanitize_observation_summary(raw: &str) -> String {
    let collapsed: String = raw
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let lower = collapsed.to_ascii_lowercase();
    if contains_sk_prefix_marker(&lower)
        || lower.contains("bearer ")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("authorization:")
        || lower.contains("secret")
        || lower.contains("password")
    {
        return "[redacted sensitive metadata]".to_string();
    }

    if collapsed.chars().count() <= MAX_OBSERVATION_SUMMARY_CHARS {
        collapsed
    } else {
        let mut truncated: String = collapsed
            .chars()
            .take(MAX_OBSERVATION_SUMMARY_CHARS)
            .collect();
        truncated.push_str("...");
        truncated
    }
}

/// Extracted safe metadata from a CLI invocation argv slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeArgvMetadata {
    /// Canonical operation name (e.g. `context.workflow.propose`).
    pub operation: String,
    /// Human-readable title for the operation.
    pub title: String,
    /// Optional validated `plan_id` extracted from `--plan-id` or environment correlation.
    pub plan_id: Option<String>,
    /// Optional validated `approval_id` extracted from `--approval-id`.
    pub approval_id: Option<String>,
    /// Optional validated `task_id` extracted from `--task-id`.
    pub task_id: Option<String>,
    /// Optional validated `binding_id` extracted from `--binding-id`.
    pub binding_id: Option<String>,
    /// Optional validated `parent_request_id` extracted from `--parent-request-id` or environment.
    pub parent_request_id: Option<String>,
    /// Optional workflow role extracted from `--role`.
    pub role: Option<String>,
    /// Optional difficulty extracted from `--difficulty`.
    pub difficulty: Option<String>,
    /// Safe summary describing only the operation and safe flags (never prompts or free-text values).
    pub safe_summary: String,
}

fn is_safe_cli_subcommand(s: &str) -> bool {
    matches!(
        s,
        "ack"
            | "activity"
            | "admit"
            | "advance"
            | "approve"
            | "cancel"
            | "catalog"
            | "change-preview"
            | "change-record"
            | "change-resolve"
            | "changes"
            | "check"
            | "claim"
            | "classify"
            | "complete"
            | "create"
            | "dispatch"
            | "doctor"
            | "fail"
            | "get"
            | "get-plan"
            | "guide"
            | "handoff"
            | "heartbeat"
            | "help"
            | "inspect"
            | "integrate"
            | "invoke"
            | "list"
            | "list-changes"
            | "list-plans"
            | "models"
            | "notification-status"
            | "poll"
            | "preflight"
            | "preview"
            | "preview-change"
            | "progress"
            | "propose"
            | "publish"
            | "pull"
            | "rearm-notification"
            | "reconcile"
            | "record"
            | "record-change"
            | "reject"
            | "replan"
            | "reserve-closure"
            | "resolve-change"
            | "review"
            | "revise"
            | "run"
            | "save-draft"
            | "session"
            | "start"
            | "status"
            | "stop"
            | "submit"
            | "task"
            | "verify"
            | "version"
    )
}

/// Extracts only safe operation labels and structural ID flags from an operator CLI `argv` slice.
///
/// Explicitly ignores and never logs `--request` / `--request-file` paths or contents, `--summary`,
/// `--reason`, `--instruction`, `--prompt`, `--answer`, secrets, unknown flags/values, or free-text
/// positional arguments.
pub fn sanitize_cli_argv_metadata(
    boundary_prefix: &str,
    subcommand_group: &str,
    forwarded_args: &[String],
) -> SafeArgvMetadata {
    let matched_subcommand = forwarded_args.first().and_then(|s| {
        let clean = s.trim();
        if is_safe_cli_subcommand(clean) {
            Some(clean.to_string())
        } else {
            None
        }
    });
    let mut idx = if matched_subcommand.is_some() { 1 } else { 0 };
    let subcommand = matched_subcommand.unwrap_or_else(|| "invoke".to_string());

    let operation = if boundary_prefix.is_empty() {
        format!("{subcommand_group}.{subcommand}")
    } else {
        format!("{boundary_prefix}.{subcommand_group}.{subcommand}")
    };

    let title = format!(
        "{} {}",
        subcommand_group.replace('_', " "),
        subcommand.replace('-', " ")
    );

    let mut plan_id = read_env_plan_id();
    let mut approval_id = None;
    let mut task_id = None;
    let mut binding_id = None;
    let mut parent_request_id = read_env_parent_request_id();
    let mut role = None;
    let mut difficulty = None;
    let mut safe_flags = Vec::new();

    while idx < forwarded_args.len() {
        let token = forwarded_args[idx].trim();
        let (flag, inline_val) = match token.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f, Some(v)),
            _ => (token, None),
        };
        let step = if inline_val.is_some() { 1 } else { 2 };
        let next_val = || -> Option<&str> {
            match inline_val {
                Some(v) => Some(v),
                None => forwarded_args.get(idx + 1).map(String::as_str),
            }
        };

        match flag {
            "--plan-id" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("plan_id={val}"));
                    plan_id = Some(val);
                }
                idx += step;
            }
            "--approval-id" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("approval_id={val}"));
                    approval_id = Some(val);
                }
                idx += step;
            }
            "--task-id" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("task_id={val}"));
                    task_id = Some(val);
                }
                idx += step;
            }
            "--binding-id" | "--binding" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("binding_id={val}"));
                    binding_id = Some(val);
                }
                idx += step;
            }
            "--parent-request-id" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    parent_request_id = Some(val);
                }
                idx += step;
            }
            "--cycle-id" | "--handoff-id" | "--batch-id" | "--worktree-id" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    let key = flag.trim_start_matches("--").replace('-', "_");
                    safe_flags.push(format!("{key}={val}"));
                }
                idx += step;
            }
            "--role" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("role={val}"));
                    role = Some(val);
                }
                idx += step;
            }
            "--difficulty" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    safe_flags.push(format!("difficulty={val}"));
                    difficulty = Some(val);
                }
                idx += step;
            }
            "--outcome" | "--verdict" | "--kind" => {
                if let Some(val) = next_val().and_then(validate_correlation_id) {
                    let key = flag.trim_start_matches("--").replace('-', "_");
                    safe_flags.push(format!("{key}={val}"));
                }
                idx += step;
            }
            "--expected-revision"
            | "--revision"
            | "--after-seq"
            | "--up-to-seq"
            | "--limit"
            | "--max-batch-bytes" => {
                if let Some(val) = next_val() {
                    let clean_val = val.trim();
                    if !clean_val.is_empty()
                        && clean_val.len() <= 20
                        && clean_val.bytes().all(|b| b.is_ascii_digit())
                    {
                        let key = flag.trim_start_matches("--").replace('-', "_");
                        safe_flags.push(format!("{key}={clean_val}"));
                    }
                }
                idx += step;
            }
            "--request-file" | "--request" => {
                safe_flags.push("request_file=[redacted_path]".to_string());
                idx += step;
            }
            "--summary" | "--reason" | "--instruction" | "--prompt" | "--answer" | "--context"
            | "--policy" | "--task-summary" => {
                // Never log free-text user/worker prose or file paths from argv.
                idx += step;
            }
            _ if flag.starts_with("--")
                && inline_val.is_none()
                && forwarded_args
                    .get(idx + 1)
                    .is_some_and(|next| !next.trim().starts_with('-')) =>
            {
                // Skip an unknown flag's possible value rather than logging it.
                idx += 2;
            }
            _ => {
                idx += 1;
            }
        }
    }

    let safe_summary = if safe_flags.is_empty() {
        format!("Operation {operation}")
    } else {
        format!("Operation {operation} ({})", safe_flags.join(", "))
    };

    SafeArgvMetadata {
        operation,
        title,
        plan_id,
        approval_id,
        task_id,
        binding_id,
        parent_request_id,
        role,
        difficulty,
        safe_summary,
    }
}

fn read_linux_process_instance(pid: u32) -> Option<ObservationProcessInstance> {
    if pid == 0 {
        return None;
    }
    let proc_root = Path::new("/proc");
    if !proc_root.is_dir() {
        return None;
    }
    let boot_id = fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))
        .ok()
        .map(|s| s.trim().to_string())?;
    if boot_id.is_empty() || boot_id.len() > 128 || !boot_id.bytes().all(|b| b.is_ascii_graphic()) {
        return None;
    }

    let stat = fs::read_to_string(proc_root.join(pid.to_string()).join("stat")).ok()?;
    let rparen = stat.rfind(')')?;
    let after = stat.get(rparen + 1..)?.trim_start();
    let mut fields = after.split_whitespace();
    let state = fields.next()?;
    if state.starts_with('Z') || state.starts_with('X') || state.starts_with('x') {
        return None;
    }
    // Field 3 is `state` (consumed above). Field 22 is `starttime`, 18 fields after `state`.
    let start_ticks = fields.nth(18)?.parse::<u64>().ok()?;

    Some(ObservationProcessInstance {
        pid,
        boot_id,
        start_ticks,
    })
}

/// Captures the current OS process instance proof (`pid`, kernel `boot_id`, and Linux start ticks)
/// when available on the host platform.
pub fn current_process_instance() -> Option<ObservationProcessInstance> {
    read_linux_process_instance(std::process::id())
}

/// Verifies whether `expected` matches the currently running OS process instance on this host.
///
/// Returns `true` only when `/proc/<pid>` is alive (not zombie/dead) and both the kernel
/// `boot_id` and process `start_ticks` match `expected`.
pub fn is_process_instance_alive(expected: &ObservationProcessInstance) -> bool {
    if expected.pid == 0 || expected.boot_id.trim().is_empty() {
        return false;
    }
    match read_linux_process_instance(expected.pid) {
        Some(actual) => actual == *expected,
        None => false,
    }
}

/// Diagnostic helper checking whether a local OS process ID exists in `/proc/<pid>` and is not a
/// zombie.
///
/// Because OS PIDs can be reused, callers must never use `is_local_process_alive` alone to claim a
/// call or job is `Running`; use [`is_process_instance_alive`] instead.
pub fn is_local_process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let proc_root = Path::new("/proc");
    if !proc_root.is_dir() {
        return false;
    }
    let proc_pid = proc_root.join(pid.to_string());
    if !proc_pid.exists() {
        return false;
    }
    if let Ok(stat) = fs::read_to_string(proc_pid.join("stat")) {
        if let Some(rparen) = stat.rfind(')') {
            let after = stat[rparen + 1..].trim_start();
            if after.starts_with('Z') || after.starts_with('X') || after.starts_with('x') {
                return false;
            }
        }
    }
    true
}

fn validate_private_dir_metadata(dir: &Path) -> Result<fs::Metadata, ObservationJournalError> {
    if !dir.is_absolute() {
        return Err(ObservationJournalError::SecurityViolation(
            "directory path must be absolute".to_string(),
        ));
    }
    let mut current = std::path::PathBuf::new();
    for component in dir.components() {
        use std::path::Component;
        match component {
            Component::RootDir => current.push("/"),
            Component::Normal(part) => {
                current.push(part);
                let meta = fs::symlink_metadata(&current)?;
                if meta.file_type().is_symlink() {
                    return Err(ObservationJournalError::SecurityViolation(
                        "directory path must not contain symlinks".to_string(),
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(ObservationJournalError::SecurityViolation(
                        "path component is not a directory".to_string(),
                    ));
                }
            }
            _ => {
                return Err(ObservationJournalError::SecurityViolation(
                    "directory path must not contain relative or traversal components".to_string(),
                ));
            }
        }
    }

    let meta = fs::symlink_metadata(dir)?;
    verify_private_dir_metadata(&meta)?;

    Ok(meta)
}

fn verify_private_dir_metadata(meta: &fs::Metadata) -> Result<(), ObservationJournalError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ObservationJournalError::SecurityViolation(
            "directory path must reference a non-symlink directory".to_string(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o700 {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "directory must have 0700 permissions, found {mode:04o}"
            )));
        }
        if let Ok(self_meta) = fs::metadata("/proc/self") {
            if meta.uid() != self_meta.uid() {
                return Err(ObservationJournalError::SecurityViolation(
                    "directory owner does not match current process owner".to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn open_verified_private_dir(
    dir: &Path,
    expected_meta: &fs::Metadata,
) -> Result<File, ObservationJournalError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc_o_nofollow());
    let dir_file = options
        .open(dir)
        .map_err(|err| map_open_error(err, "observation journal directory"))?;
    let post_meta = dir_file.metadata()?;
    verify_private_dir_metadata(&post_meta)?;
    verify_same_file_identity(expected_meta, &post_meta, "observation journal directory")?;
    Ok(dir_file)
}

fn revalidate_private_dir_identity(
    dir: &Path,
    expected_dir_meta: &fs::Metadata,
    dir_file: Option<&File>,
) -> Result<(), ObservationJournalError> {
    let current_dir_meta = validate_private_dir_metadata(dir)?;
    verify_same_file_identity(
        expected_dir_meta,
        &current_dir_meta,
        "observation journal directory",
    )?;
    if let Some(dir_file) = dir_file {
        let handle_meta = dir_file.metadata()?;
        verify_private_dir_metadata(&handle_meta)?;
        verify_same_file_identity(
            expected_dir_meta,
            &handle_meta,
            "observation journal directory",
        )?;
    }
    Ok(())
}

fn verify_private_file_metadata(
    meta: &fs::Metadata,
    parent_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(ObservationJournalError::SecurityViolation(format!(
            "{label} must be a non-symlink regular file"
        )));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode() & 0o777;
        if mode != 0o600 {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "{label} must have 0600 permissions, found {mode:04o}"
            )));
        }
        if meta.uid() != parent_meta.uid() {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "{label} owner does not match private parent directory owner"
            )));
        }
        if let Ok(self_meta) = fs::metadata("/proc/self") {
            if meta.uid() != self_meta.uid() {
                return Err(ObservationJournalError::SecurityViolation(format!(
                    "{label} owner does not match current process owner"
                )));
            }
        }
    }

    Ok(())
}

fn verify_same_file_identity(
    pre_meta: &fs::Metadata,
    post_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if pre_meta.dev() != post_meta.dev() || pre_meta.ino() != post_meta.ino() {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "{label} dev/inode changed during open"
            )));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (pre_meta, post_meta, label);
    }
    Ok(())
}

fn map_open_error(err: std::io::Error, label: &str) -> ObservationJournalError {
    #[cfg(unix)]
    {
        if err.raw_os_error() == Some(libc_eloop()) {
            return ObservationJournalError::SecurityViolation(format!(
                "{label} must not be a symlink"
            ));
        }
    }
    ObservationJournalError::Io(err)
}

fn open_verified_private_file_readonly(
    file_path: &Path,
    label: &str,
) -> Result<(File, fs::Metadata), ObservationJournalError> {
    let parent = file_path.parent().ok_or_else(|| {
        ObservationJournalError::SecurityViolation("file path has no parent directory".to_string())
    })?;
    let parent_meta = validate_private_dir_metadata(parent)?;
    let pre_meta = fs::symlink_metadata(file_path)?;
    verify_private_file_metadata(&pre_meta, &parent_meta, label)?;

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc_o_nofollow());
    }

    let file = options
        .open(file_path)
        .map_err(|err| map_open_error(err, label))?;
    let post_meta = file.metadata()?;
    verify_private_file_metadata(&post_meta, &parent_meta, label)?;
    verify_same_file_identity(&pre_meta, &post_meta, label)?;

    Ok((file, post_meta))
}

/// Validates that `dir` is an existing, non-symlink private (`0700` on Unix) directory without
/// creating or modifying it.
pub fn validate_existing_private_dir_readonly(dir: &Path) -> Result<(), ObservationJournalError> {
    validate_private_dir_metadata(dir).map(|_| ())
}

/// Validates that `file_path` is an existing, non-symlink regular file with `0600` permissions on
/// Unix, matching private owner, and size at most `max_bytes`.
pub fn validate_existing_private_file_readonly(
    file_path: &Path,
    max_bytes: u64,
) -> Result<fs::Metadata, ObservationJournalError> {
    let (_file, post_meta) = open_verified_private_file_readonly(file_path, "file")?;
    if post_meta.len() > max_bytes {
        return Err(ObservationJournalError::JournalTooLarge);
    }
    Ok(post_meta)
}

static JOURNAL_APPEND_LOCK: Mutex<()> = Mutex::new(());

/// Appends a single JSONL [`ObservationJournalRecord`] to `<dir>/observation-journal.jsonl`.
///
/// Requires `dir` to be an existing `0700` non-symlink directory. On Unix, opens a verified
/// parent-directory handle with `O_NOFOLLOW`, keeps it alive across the append, and revalidates
/// the directory path and handle identity (`dev+ino`, owner, and `0700` mode) before writing and
/// before completion so directory substitution is rejected. Serializes the complete JSONL line
/// first, opens or creates the journal file with `0600` permissions and `O_APPEND | O_NOFOLLOW`,
/// verifies `fstat` regular file / owner / `0600` mode / matching `dev+ino`, acquires an exclusive
/// cross-process lock, repairs any missing trailing newline from an interrupted prior write,
/// writes the complete record line, flushes, syncs file data (`sync_data`), and on Unix syncs
/// parent directory metadata (`sync_all`, even when the journal preexisted) before releasing the
/// lock and returning `Ok(())`. Non-Unix targets preserve file-data sync without directory-handle
/// sync. These OS sync primitives provide software-level crash/power-loss durability guarantees
/// where honored by the underlying kernel and storage stack (not verified by hardware power-cut
/// testing).
pub fn append_journal_record(
    dir: &Path,
    record: &ObservationJournalRecord,
) -> Result<(), ObservationJournalError> {
    let dir_meta = validate_private_dir_metadata(dir)?;
    #[cfg(unix)]
    let dir_file = Some(open_verified_private_dir(dir, &dir_meta)?);
    #[cfg(not(unix))]
    let dir_file: Option<File> = None;

    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    if line.len() > MAX_OBSERVATION_RECORD_LINE_BYTES {
        return Err(ObservationJournalError::RecordTooLarge);
    }

    let _in_process_guard = JOURNAL_APPEND_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    revalidate_private_dir_identity(dir, &dir_meta, dir_file.as_ref())?;

    let journal_path = dir.join(OBSERVATION_JOURNAL_FILE_NAME);
    let pre_meta = match fs::symlink_metadata(&journal_path) {
        Ok(meta) => {
            verify_private_file_metadata(&meta, &dir_meta, "observation journal")?;
            Some(meta)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => return Err(ObservationJournalError::Io(err)),
    };

    let mut options = OpenOptions::new();
    options.read(true).create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc_o_nofollow());
    }

    let mut file = options
        .open(&journal_path)
        .map_err(|err| map_open_error(err, "observation journal"))?;

    let post_meta = file.metadata()?;
    verify_private_file_metadata(&post_meta, &dir_meta, "observation journal")?;
    if let Some(pre) = &pre_meta {
        verify_same_file_identity(pre, &post_meta, "observation journal")?;
    }

    file.lock_exclusive()?;

    revalidate_private_dir_identity(dir, &dir_meta, dir_file.as_ref())?;
    let locked_meta = file.metadata()?;
    verify_private_file_metadata(&locked_meta, &dir_meta, "observation journal")?;
    verify_same_file_identity(&post_meta, &locked_meta, "observation journal")?;
    let path_meta = fs::symlink_metadata(&journal_path)?;
    verify_private_file_metadata(&path_meta, &dir_meta, "observation journal")?;
    verify_same_file_identity(&post_meta, &path_meta, "observation journal")?;

    if locked_meta.len() > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last_byte = [0u8; 1];
        file.read_exact(&mut last_byte)?;
        if last_byte[0] != b'\n' {
            let mut repaired = Vec::with_capacity(line.len() + 1);
            repaired.push(b'\n');
            repaired.extend_from_slice(&line);
            line = repaired;
        }
    }

    file.write_all(&line)?;
    file.flush()?;
    file.sync_data()?;

    revalidate_private_dir_identity(dir, &dir_meta, dir_file.as_ref())?;
    let committed_path_meta = fs::symlink_metadata(&journal_path)?;
    verify_private_file_metadata(&committed_path_meta, &dir_meta, "observation journal")?;
    verify_same_file_identity(&post_meta, &committed_path_meta, "observation journal")?;
    if let Some(dir_file) = &dir_file {
        dir_file.sync_all()?;
    }

    let _ = file.unlock();
    Ok(())
}

#[cfg(unix)]
const fn libc_o_nofollow() -> i32 {
    #[cfg(target_os = "linux")]
    {
        0o400000
    }
    #[cfg(target_os = "macos")]
    {
        0x0100
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

#[cfg(unix)]
const fn libc_eloop() -> i32 {
    #[cfg(target_os = "linux")]
    {
        40
    }
    #[cfg(target_os = "macos")]
    {
        62
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        40
    }
}

/// Reads valid [`ObservationJournalRecord`] entries from `<dir>/observation-journal.jsonl`
/// in strictly read-only mode.
///
/// Never creates directories or files, never acquires write locks, and tolerates a truncated or
/// malformed line by recording a warning while preserving all valid records. When the journal file
/// exceeds [`MAX_OBSERVATION_JOURNAL_BYTES`], reads the newest complete records from the trailing
/// [`MAX_OBSERVATION_JOURNAL_BYTES`] window and emits an explicit partial-history warning.
pub fn read_journal_records_readonly(
    dir: &Path,
) -> Result<(Vec<ObservationJournalRecord>, Vec<String>), ObservationJournalError> {
    validate_private_dir_metadata(dir)?;

    let journal_path = dir.join(OBSERVATION_JOURNAL_FILE_NAME);
    match fs::symlink_metadata(&journal_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), Vec::new()));
        }
        Err(err) => return Err(ObservationJournalError::Io(err)),
        Ok(_) => {}
    }

    let (mut file, post_meta) =
        open_verified_private_file_readonly(&journal_path, "observation journal")?;

    let file_len = post_meta.len();
    let mut records = Vec::new();
    let mut warnings = Vec::new();

    let discard_partial_leading_line = if file_len > MAX_OBSERVATION_JOURNAL_BYTES {
        let start_offset = file_len - MAX_OBSERVATION_JOURNAL_BYTES;
        warnings.push(format!(
            "Observation journal size ({file_len} bytes) exceeds maximum read window ({MAX_OBSERVATION_JOURNAL_BYTES} bytes); earlier history was omitted and only the newest complete records from the trailing {MAX_OBSERVATION_JOURNAL_BYTES} bytes are shown"
        ));
        file.seek(SeekFrom::Start(start_offset - 1))?;
        let mut prev_byte = [0u8; 1];
        file.read_exact(&mut prev_byte)?;
        prev_byte[0] != b'\n'
    } else {
        false
    };

    let bounded = file.take(MAX_OBSERVATION_JOURNAL_BYTES);
    let mut reader = BufReader::new(bounded);

    if discard_partial_leading_line {
        let mut discarded = Vec::new();
        let _ = reader.read_until(b'\n', &mut discarded)?;
    }

    let mut line_buf = Vec::new();
    let mut line_idx = 0usize;
    loop {
        line_buf.clear();
        let bytes_read = reader.read_until(b'\n', &mut line_buf)?;
        if bytes_read == 0 {
            break;
        }
        line_idx += 1;
        let Ok(line_str) = std::str::from_utf8(&line_buf) else {
            warnings.push(format!(
                "Skipped malformed or interrupted observation journal line {line_idx}"
            ));
            continue;
        };
        let trimmed = line_str.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > MAX_OBSERVATION_RECORD_LINE_BYTES {
            warnings.push(format!(
                "Skipped oversized observation journal line {line_idx}"
            ));
            continue;
        }
        match serde_json::from_str::<ObservationJournalRecord>(trimmed) {
            Ok(record) => records.push(record),
            Err(_) => {
                warnings.push(format!(
                    "Skipped malformed or interrupted observation journal line {line_idx}"
                ));
            }
        }
    }

    Ok((records, warnings))
}

/// Structural binding record read from `state.db` in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyBindingRecord {
    /// Scoped software `binding_id`.
    pub binding_id: String,
    /// Validated `thread_uuid`.
    pub thread_uuid: String,
    /// Human-readable binding label.
    pub label: String,
    /// Highest event sequence delivered in a batch.
    pub delivered_seq: u64,
    /// Highest event sequence durably acknowledged.
    pub acked_seq: u64,
}

/// Structural run record read from `state.db` in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyRunRecord {
    /// Run identifier (`run_<uuid>`).
    pub run_id: String,
    /// Owning `binding_id`.
    pub binding_id: String,
    /// Current revision number.
    pub revision: u64,
    /// Run status string (`running`, `waiting_input`, `completing`, `completed`, `failed`, `cancelled`, `unknown`).
    pub status: String,
    /// Active question ID when waiting for user input.
    pub active_question_id: Option<String>,
    /// Start timestamp in milliseconds.
    pub started_at_ms: i64,
    /// Latest update timestamp in milliseconds.
    pub updated_at_ms: i64,
    /// Completion timestamp in milliseconds.
    pub completed_at_ms: Option<i64>,
    /// Cancellation timestamp in milliseconds.
    pub cancelled_at_ms: Option<i64>,
}

/// Structural worker record read from `state.db` in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyWorkerRecord {
    /// Worker identifier (`wrk_<uuid>`).
    pub worker_id: String,
    /// Associated `run_id`.
    pub run_id: String,
    /// Owning `binding_id`.
    pub binding_id: String,
    /// Worker revision number.
    pub revision: u64,
    /// Worker status string (`queued`, `claimed`, `running`, `cancel_requested`, `completed`, `failed`, `cancelled`, `unknown`).
    pub status: String,
    /// Worker profile identifier.
    pub profile_id: String,
    profile_digest: String,
    /// Provider name (`codex`, `opencode`, `antigravity`, etc.).
    pub provider: String,
    /// Requested model identifier.
    pub requested_model: String,
    /// Requested reasoning effort.
    pub requested_effort: String,
    /// Observed model reported by the worker runtime, if any.
    pub observed_model: Option<String>,
    /// Observed reasoning effort reported by the worker runtime, if any.
    pub observed_effort: Option<String>,
    instruction: String,
    claim_id: Option<String>,
    /// Dispatch attempt count.
    pub attempt_count: u32,
    /// Raw JSON for `WorkerExecutionBinding` if bound to an approved plan task.
    pub execution_binding_json: Option<String>,
    worktree_json: String,
    progress_json: String,
    /// Raw JSON for `WorkerOutcome` if finished.
    pub outcome_json: Option<String>,
    /// Creation timestamp in milliseconds.
    pub created_at_ms: i64,
    /// Latest update timestamp in milliseconds.
    pub updated_at_ms: i64,
    /// Execution start timestamp in milliseconds.
    pub started_at_ms: Option<i64>,
    /// Execution completion timestamp in milliseconds.
    pub completed_at_ms: Option<i64>,
    cancel_requested_at_ms: Option<i64>,
    cancel_reason: Option<String>,
}

impl ReadonlyWorkerRecord {
    /// Converts this read-only worker record into a [`crate::WorkerSnapshot`] for read-only
    /// task lifecycle and review verification.
    pub fn to_worker_snapshot(&self) -> Option<crate::WorkerSnapshot> {
        let status = crate::WorkerStatus::from_str(&self.status)?;
        let execution_binding = match &self.execution_binding_json {
            Some(json) => Some(serde_json::from_str(json).ok()?),
            None => None,
        };
        let worktree = serde_json::from_str(&self.worktree_json).ok()?;
        let progress = serde_json::from_str(&self.progress_json).ok()?;
        let outcome = match &self.outcome_json {
            Some(json) => Some(serde_json::from_str(json).ok()?),
            None => None,
        };
        Some(crate::WorkerSnapshot {
            untrusted_data: true,
            worker_id: self.worker_id.clone(),
            run_id: self.run_id.clone(),
            binding_id: self.binding_id.clone(),
            revision: self.revision,
            status,
            profile_id: self.profile_id.clone(),
            profile_digest: self.profile_digest.clone(),
            provider: self.provider.clone(),
            requested_model: self.requested_model.clone(),
            requested_effort: self.requested_effort.clone(),
            observed_model: self.observed_model.clone(),
            observed_effort: self.observed_effort.clone(),
            instruction: self.instruction.clone(),
            claim_id: self.claim_id.clone(),
            attempt_count: self.attempt_count,
            execution_binding,
            worktree,
            progress,
            outcome,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            started_at_ms: self.started_at_ms,
            completed_at_ms: self.completed_at_ms,
            cancel_requested_at_ms: self.cancel_requested_at_ms,
            cancel_reason: self.cancel_reason.clone(),
        })
    }
}

/// Structural event record read from `state.db` in read-only mode (without untrusted prose payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyEventRecord {
    /// Event identifier (`evt_<uuid>`).
    pub event_id: String,
    /// Owning `binding_id`.
    pub binding_id: String,
    /// Monotonic per-binding sequence number.
    pub seq: u64,
    /// Associated `run_id`.
    pub run_id: String,
    /// Run revision number.
    pub revision: u64,
    /// Canonical `snake_case` event kind.
    pub kind: String,
    /// Whether the event triggers wake-up coalescing.
    pub important: bool,
    /// Event creation timestamp in milliseconds.
    pub created_at_ms: i64,
}

/// Structural event batch record read from `state.db` in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyEventBatchRecord {
    /// Batch identifier (`bat_<uuid>`).
    pub batch_id: String,
    /// Owning `binding_id`.
    pub binding_id: String,
    /// Inclusive starting sequence number.
    pub from_seq: u64,
    /// Inclusive ending sequence number.
    pub to_seq: u64,
    /// Count of events in the batch.
    pub event_count: usize,
    /// Batch creation/delivery timestamp in milliseconds.
    pub created_at_ms: i64,
    /// Batch acknowledgement timestamp in milliseconds, if acknowledged.
    pub acked_at_ms: Option<i64>,
}

/// Structural wake-up notice record read from `state.db` in read-only mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadonlyWakeupRecord {
    /// Owning `binding_id`.
    pub binding_id: String,
    /// Bound `thread_uuid`.
    pub thread_uuid: String,
    /// Wake-up status (`idle`, `pending`, `in_flight`, `succeeded`, `satisfied_by_ack`, `failed`, `unknown`).
    pub status: String,
    /// Active or most recent claim ID.
    pub claim_id: Option<String>,
    /// Earliest unacknowledged important sequence number.
    pub pending_since_seq: Option<u64>,
    /// Latest important sequence number.
    pub latest_important_seq: u64,
    /// Highest sequence covered by the notice/claim.
    pub covered_up_to_seq: u64,
    /// Claim timestamp in milliseconds.
    pub claimed_at_ms: Option<i64>,
    /// Resolution timestamp in milliseconds.
    pub resolved_at_ms: Option<i64>,
}

/// Complete read-only structural snapshot of `state.db`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadonlyCoreStateSnapshot {
    /// Configured bindings.
    pub bindings: Vec<ReadonlyBindingRecord>,
    /// Runs across bindings.
    pub runs: Vec<ReadonlyRunRecord>,
    /// Workers across bindings.
    pub workers: Vec<ReadonlyWorkerRecord>,
    /// Structural events across bindings.
    pub events: Vec<ReadonlyEventRecord>,
    /// Delivered event batches across bindings.
    pub event_batches: Vec<ReadonlyEventBatchRecord>,
    /// Wake-up state per binding.
    pub wakeups: Vec<ReadonlyWakeupRecord>,
}

fn validate_readonly_sqlite_file(
    file_path: &Path,
    parent_meta: &fs::Metadata,
    max_bytes: u64,
    label: &str,
) -> Result<fs::Metadata, ObservationJournalError> {
    let pre_meta = fs::symlink_metadata(file_path)?;
    verify_readonly_sqlite_file_metadata(&pre_meta, parent_meta, label)?;

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc_o_nofollow());
    }

    let file = options
        .open(file_path)
        .map_err(|err| map_open_error(err, label))?;
    let post_meta = file.metadata()?;
    verify_readonly_sqlite_file_metadata(&post_meta, parent_meta, label)?;
    verify_same_file_identity(&pre_meta, &post_meta, label)?;
    if post_meta.len() > max_bytes {
        return Err(ObservationJournalError::JournalTooLarge);
    }
    Ok(post_meta)
}

fn verify_readonly_sqlite_file_metadata(
    meta: &fs::Metadata,
    parent_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return Err(ObservationJournalError::SecurityViolation(format!(
            "{label} must be a non-symlink regular file"
        )));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = meta.permissions().mode() & 0o777;
        if (mode & 0o133) != 0 {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "{label} must not be group/world-writable or executable, found {mode:04o}"
            )));
        }
        if meta.uid() != parent_meta.uid() {
            return Err(ObservationJournalError::SecurityViolation(format!(
                "{label} owner does not match private parent directory owner"
            )));
        }
        if let Ok(self_meta) = fs::metadata("/proc/self") {
            if meta.uid() != self_meta.uid() {
                return Err(ObservationJournalError::SecurityViolation(format!(
                    "{label} owner does not match current process owner"
                )));
            }
        }
    }

    Ok(())
}

fn sqlite_readonly_error(context: &str, err: rusqlite::Error) -> ObservationJournalError {
    ObservationJournalError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("sqlite read-only {context}: {err}"),
    ))
}

fn convert_u64_column(
    table: &str,
    column: &str,
    value: i64,
) -> Result<u64, ObservationJournalError> {
    u64::try_from(value).map_err(|_| {
        ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only invalid integer in {table}.{column}: {value}"),
        ))
    })
}

fn convert_u32_column(
    table: &str,
    column: &str,
    value: i64,
) -> Result<u32, ObservationJournalError> {
    u32::try_from(value).map_err(|_| {
        ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only invalid integer in {table}.{column}: {value}"),
        ))
    })
}

fn convert_usize_column(
    table: &str,
    column: &str,
    value: i64,
) -> Result<usize, ObservationJournalError> {
    usize::try_from(value).map_err(|_| {
        ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only invalid integer in {table}.{column}: {value}"),
        ))
    })
}

fn sqlite_sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar_os = database_path.as_os_str().to_os_string();
    sidecar_os.push(suffix);
    PathBuf::from(sidecar_os)
}

fn inspect_readonly_sqlite_sidecar(
    sidecar_path: &Path,
    parent_meta: &fs::Metadata,
    max_bytes: u64,
    label: &str,
) -> Result<Option<fs::Metadata>, ObservationJournalError> {
    match fs::symlink_metadata(sidecar_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ObservationJournalError::Io(err)),
        Ok(_) => {
            let meta = validate_readonly_sqlite_file(sidecar_path, parent_meta, max_bytes, label)?;
            Ok(Some(meta))
        }
    }
}

fn verify_sqlite_sidecar_absent(
    sidecar_path: &Path,
    label: &str,
) -> Result<(), ObservationJournalError> {
    match fs::symlink_metadata(sidecar_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ObservationJournalError::Io(err)),
        Ok(_) => Err(ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only detected concurrent creation of {label}"),
        ))),
    }
}

fn verify_unchanged_file_metadata(
    pre_meta: &fs::Metadata,
    post_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    verify_same_file_identity(pre_meta, post_meta, label)?;
    if pre_meta.len() != post_meta.len() {
        return Err(ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only detected concurrent size change of {label}"),
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let pre_mode = pre_meta.permissions().mode() & 0o777;
        let post_mode = post_meta.permissions().mode() & 0o777;
        if pre_mode != post_mode
            || pre_meta.uid() != post_meta.uid()
            || pre_meta.gid() != post_meta.gid()
            || pre_meta.mtime() != post_meta.mtime()
            || pre_meta.mtime_nsec() != post_meta.mtime_nsec()
            || pre_meta.ctime() != post_meta.ctime()
            || pre_meta.ctime_nsec() != post_meta.ctime_nsec()
        {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sqlite read-only detected concurrent modification of {label}"),
            )));
        }
    }
    #[cfg(not(unix))]
    {
        if pre_meta.modified().ok() != post_meta.modified().ok() {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sqlite read-only detected concurrent modification of {label}"),
            )));
        }
    }
    Ok(())
}

fn verify_unchanged_shm_metadata(
    pre_meta: &fs::Metadata,
    post_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    verify_same_file_identity(pre_meta, post_meta, label)?;
    if pre_meta.len() != post_meta.len() {
        return Err(ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("sqlite read-only detected concurrent size change of {label}"),
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let pre_mode = pre_meta.permissions().mode() & 0o777;
        let post_mode = post_meta.permissions().mode() & 0o777;
        if pre_mode != post_mode
            || pre_meta.uid() != post_meta.uid()
            || pre_meta.gid() != post_meta.gid()
        {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sqlite read-only detected concurrent metadata change of {label}"),
            )));
        }
    }
    Ok(())
}

fn verify_unchanged_dir_metadata(
    pre_meta: &fs::Metadata,
    post_meta: &fs::Metadata,
    label: &str,
) -> Result<(), ObservationJournalError> {
    verify_same_file_identity(pre_meta, post_meta, label)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let pre_mode = pre_meta.permissions().mode() & 0o777;
        let post_mode = post_meta.permissions().mode() & 0o777;
        if pre_mode != post_mode
            || pre_meta.uid() != post_meta.uid()
            || pre_meta.gid() != post_meta.gid()
            || pre_meta.mtime() != post_meta.mtime()
            || pre_meta.mtime_nsec() != post_meta.mtime_nsec()
            || pre_meta.ctime() != post_meta.ctime()
            || pre_meta.ctime_nsec() != post_meta.ctime_nsec()
        {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sqlite read-only detected concurrent directory change in {label}"),
            )));
        }
    }
    #[cfg(not(unix))]
    {
        if pre_meta.modified().ok() != post_meta.modified().ok() {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("sqlite read-only detected concurrent directory change in {label}"),
            )));
        }
    }
    Ok(())
}

fn build_sqlite_readonly_immutable_uri(
    database_path: &Path,
) -> Result<String, ObservationJournalError> {
    if !database_path.is_absolute() {
        return Err(ObservationJournalError::SecurityViolation(
            "database path must be absolute".to_string(),
        ));
    }

    #[cfg(unix)]
    let raw_bytes: &[u8] = {
        use std::os::unix::ffi::OsStrExt;
        database_path.as_os_str().as_bytes()
    };
    #[cfg(not(unix))]
    let raw_bytes: &[u8] = {
        let s = database_path.to_str().ok_or_else(|| {
            ObservationJournalError::SecurityViolation(
                "database path must be valid UTF-8 on non-Unix platforms".to_string(),
            )
        })?;
        s.as_bytes()
    };

    if raw_bytes.is_empty() || raw_bytes.contains(&0) {
        return Err(ObservationJournalError::SecurityViolation(
            "database path must not be empty or contain NUL bytes".to_string(),
        ));
    }

    const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";
    let mut uri = String::with_capacity(raw_bytes.len() + 32);
    uri.push_str("file://");
    if raw_bytes[0] != b'/' {
        uri.push('/');
    }
    for &b in raw_bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.' | b'~') {
            uri.push(char::from(b));
        } else {
            uri.push('%');
            uri.push(char::from(HEX_UPPER[usize::from(b >> 4)]));
            uri.push(char::from(HEX_UPPER[usize::from(b & 0x0f)]));
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    Ok(uri)
}

/// Reads `state.db` at `database_path` using `SQLITE_OPEN_READ_ONLY` without creating directories,
/// creating database files, running schema migrations, recovering in-flight state, or modifying
/// any rows.
pub fn read_core_state_readonly(
    database_path: &Path,
) -> Result<ReadonlyCoreStateSnapshot, ObservationJournalError> {
    const MAX_CORE_DB_BYTES: u64 = 64 * 1024 * 1024;

    let parent = database_path.parent().ok_or_else(|| {
        ObservationJournalError::SecurityViolation(
            "database path has no parent directory".to_string(),
        )
    })?;
    match fs::symlink_metadata(parent) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReadonlyCoreStateSnapshot::default());
        }
        Err(err) => return Err(ObservationJournalError::Io(err)),
        Ok(_) => {}
    }
    let parent_pre_meta = validate_private_dir_metadata(parent)?;

    match fs::symlink_metadata(database_path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReadonlyCoreStateSnapshot::default());
        }
        Err(err) => return Err(ObservationJournalError::Io(err)),
        Ok(_) => {}
    }
    let db_pre_meta = validate_readonly_sqlite_file(
        database_path,
        &parent_pre_meta,
        MAX_CORE_DB_BYTES,
        "state.db",
    )?;

    let wal_path = sqlite_sidecar_path(database_path, "-wal");
    let shm_path = sqlite_sidecar_path(database_path, "-shm");
    let wal_pre_meta = inspect_readonly_sqlite_sidecar(
        &wal_path,
        &parent_pre_meta,
        MAX_CORE_DB_BYTES,
        "state.db-wal sidecar",
    )?;
    let shm_pre_meta = inspect_readonly_sqlite_sidecar(
        &shm_path,
        &parent_pre_meta,
        MAX_CORE_DB_BYTES,
        "state.db-shm sidecar",
    )?;

    let mut conn = match (&wal_pre_meta, &shm_pre_meta) {
        (Some(_), Some(_)) => rusqlite::Connection::open_with_flags(
            database_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|err| sqlite_readonly_error("open failed", err))?,
        (None, None) => {
            let uri = build_sqlite_readonly_immutable_uri(database_path)?;
            rusqlite::Connection::open_with_flags(
                &uri,
                rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|err| sqlite_readonly_error("immutable open failed", err))?
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(ObservationJournalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "sqlite read-only inconsistent sidecar state: expected both -wal and -shm or neither",
            )));
        }
    };

    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
        .map_err(|err| sqlite_readonly_error("begin read transaction failed", err))?;

    let schema_version: i64 = tx
        .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|err| sqlite_readonly_error("schema_meta query failed", err))?;
    if schema_version != i64::from(crate::SCHEMA_VERSION) {
        return Err(ObservationJournalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "unsupported state.db schema_meta version {schema_version} (expected {})",
                crate::SCHEMA_VERSION
            ),
        )));
    }

    let mut snap = ReadonlyCoreStateSnapshot::default();

    {
        let mut stmt = tx
            .prepare(
                "SELECT binding_id, thread_uuid, label, delivered_seq, acked_seq
                 FROM bindings
                 ORDER BY rowid ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare bindings failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query bindings failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step bindings failed", err))?
        {
            let binding_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read bindings.binding_id failed", err))?;
            let thread_uuid: String = row
                .get(1)
                .map_err(|err| sqlite_readonly_error("read bindings.thread_uuid failed", err))?;
            let label: String = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read bindings.label failed", err))?;
            let delivered_seq_raw: i64 = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read bindings.delivered_seq failed", err))?;
            let acked_seq_raw: i64 = row
                .get(4)
                .map_err(|err| sqlite_readonly_error("read bindings.acked_seq failed", err))?;

            snap.bindings.push(ReadonlyBindingRecord {
                binding_id,
                thread_uuid,
                label,
                delivered_seq: convert_u64_column("bindings", "delivered_seq", delivered_seq_raw)?,
                acked_seq: convert_u64_column("bindings", "acked_seq", acked_seq_raw)?,
            });
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT run_id, binding_id, revision, status, active_question_id,
                        started_at_ms, updated_at_ms, completed_at_ms, cancelled_at_ms
                 FROM runs
                 ORDER BY started_at_ms DESC, run_id ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare runs failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query runs failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step runs failed", err))?
        {
            let run_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read runs.run_id failed", err))?;
            let binding_id: String = row
                .get(1)
                .map_err(|err| sqlite_readonly_error("read runs.binding_id failed", err))?;
            let revision_raw: i64 = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read runs.revision failed", err))?;
            let status: String = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read runs.status failed", err))?;
            let active_question_id: Option<String> = row
                .get(4)
                .map_err(|err| sqlite_readonly_error("read runs.active_question_id failed", err))?;
            let started_at_ms: i64 = row
                .get(5)
                .map_err(|err| sqlite_readonly_error("read runs.started_at_ms failed", err))?;
            let updated_at_ms: i64 = row
                .get(6)
                .map_err(|err| sqlite_readonly_error("read runs.updated_at_ms failed", err))?;
            let completed_at_ms: Option<i64> = row
                .get(7)
                .map_err(|err| sqlite_readonly_error("read runs.completed_at_ms failed", err))?;
            let cancelled_at_ms: Option<i64> = row
                .get(8)
                .map_err(|err| sqlite_readonly_error("read runs.cancelled_at_ms failed", err))?;

            snap.runs.push(ReadonlyRunRecord {
                run_id,
                binding_id,
                revision: convert_u64_column("runs", "revision", revision_raw)?,
                status,
                active_question_id,
                started_at_ms,
                updated_at_ms,
                completed_at_ms,
                cancelled_at_ms,
            });
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT worker_id, run_id, binding_id, revision, status, profile_id, profile_digest, provider,
                        requested_model, requested_effort, observed_model, observed_effort,
                        instruction, claim_id, attempt_count, execution_binding_json,
                        worktree_json, progress_json, outcome_json,
                        created_at_ms, updated_at_ms, started_at_ms, completed_at_ms,
                        cancel_requested_at_ms, cancel_reason
                 FROM workers
                 ORDER BY created_at_ms DESC, worker_id ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare workers failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query workers failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step workers failed", err))?
        {
            let worker_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read workers.worker_id failed", err))?;
            let run_id: String = row
                .get(1)
                .map_err(|err| sqlite_readonly_error("read workers.run_id failed", err))?;
            let binding_id: String = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read workers.binding_id failed", err))?;
            let revision_raw: i64 = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read workers.revision failed", err))?;
            let status: String = row
                .get(4)
                .map_err(|err| sqlite_readonly_error("read workers.status failed", err))?;
            let profile_id: String = row
                .get(5)
                .map_err(|err| sqlite_readonly_error("read workers.profile_id failed", err))?;
            let profile_digest: String = row
                .get(6)
                .map_err(|err| sqlite_readonly_error("read workers.profile_digest failed", err))?;
            let provider: String = row
                .get(7)
                .map_err(|err| sqlite_readonly_error("read workers.provider failed", err))?;
            let requested_model: String = row
                .get(8)
                .map_err(|err| sqlite_readonly_error("read workers.requested_model failed", err))?;
            let requested_effort: String = row.get(9).map_err(|err| {
                sqlite_readonly_error("read workers.requested_effort failed", err)
            })?;
            let observed_model: Option<String> = row
                .get(10)
                .map_err(|err| sqlite_readonly_error("read workers.observed_model failed", err))?;
            let observed_effort: Option<String> = row
                .get(11)
                .map_err(|err| sqlite_readonly_error("read workers.observed_effort failed", err))?;
            let instruction: String = row
                .get(12)
                .map_err(|err| sqlite_readonly_error("read workers.instruction failed", err))?;
            let claim_id: Option<String> = row
                .get(13)
                .map_err(|err| sqlite_readonly_error("read workers.claim_id failed", err))?;
            let attempt_count_raw: i64 = row
                .get(14)
                .map_err(|err| sqlite_readonly_error("read workers.attempt_count failed", err))?;
            let execution_binding_json: Option<String> = row.get(15).map_err(|err| {
                sqlite_readonly_error("read workers.execution_binding_json failed", err)
            })?;
            let worktree_json: String = row
                .get(16)
                .map_err(|err| sqlite_readonly_error("read workers.worktree_json failed", err))?;
            let progress_json: String = row
                .get(17)
                .map_err(|err| sqlite_readonly_error("read workers.progress_json failed", err))?;
            let outcome_json: Option<String> = row
                .get(18)
                .map_err(|err| sqlite_readonly_error("read workers.outcome_json failed", err))?;
            let created_at_ms: i64 = row
                .get(19)
                .map_err(|err| sqlite_readonly_error("read workers.created_at_ms failed", err))?;
            let updated_at_ms: i64 = row
                .get(20)
                .map_err(|err| sqlite_readonly_error("read workers.updated_at_ms failed", err))?;
            let started_at_ms: Option<i64> = row
                .get(21)
                .map_err(|err| sqlite_readonly_error("read workers.started_at_ms failed", err))?;
            let completed_at_ms: Option<i64> = row
                .get(22)
                .map_err(|err| sqlite_readonly_error("read workers.completed_at_ms failed", err))?;
            let cancel_requested_at_ms: Option<i64> = row.get(23).map_err(|err| {
                sqlite_readonly_error("read workers.cancel_requested_at_ms failed", err)
            })?;
            let cancel_reason: Option<String> = row
                .get(24)
                .map_err(|err| sqlite_readonly_error("read workers.cancel_reason failed", err))?;

            snap.workers.push(ReadonlyWorkerRecord {
                worker_id,
                run_id,
                binding_id,
                revision: convert_u64_column("workers", "revision", revision_raw)?,
                status,
                profile_id,
                profile_digest,
                provider,
                requested_model,
                requested_effort,
                observed_model,
                observed_effort,
                instruction,
                claim_id,
                attempt_count: convert_u32_column("workers", "attempt_count", attempt_count_raw)?,
                execution_binding_json,
                worktree_json,
                progress_json,
                outcome_json,
                created_at_ms,
                updated_at_ms,
                started_at_ms,
                completed_at_ms,
                cancel_requested_at_ms,
                cancel_reason,
            });
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT event_id, binding_id, seq, run_id, revision, kind, important, created_at_ms
                 FROM events
                 ORDER BY binding_id ASC, seq ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare events failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query events failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step events failed", err))?
        {
            let event_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read events.event_id failed", err))?;
            let binding_id: String = row
                .get(1)
                .map_err(|err| sqlite_readonly_error("read events.binding_id failed", err))?;
            let seq_raw: i64 = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read events.seq failed", err))?;
            let run_id: String = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read events.run_id failed", err))?;
            let revision_raw: i64 = row
                .get(4)
                .map_err(|err| sqlite_readonly_error("read events.revision failed", err))?;
            let kind: String = row
                .get(5)
                .map_err(|err| sqlite_readonly_error("read events.kind failed", err))?;
            let important_raw: i64 = row
                .get(6)
                .map_err(|err| sqlite_readonly_error("read events.important failed", err))?;
            let created_at_ms: i64 = row
                .get(7)
                .map_err(|err| sqlite_readonly_error("read events.created_at_ms failed", err))?;

            snap.events.push(ReadonlyEventRecord {
                event_id,
                binding_id,
                seq: convert_u64_column("events", "seq", seq_raw)?,
                run_id,
                revision: convert_u64_column("events", "revision", revision_raw)?,
                kind,
                important: important_raw != 0,
                created_at_ms,
            });
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT batch_id, binding_id, from_seq, to_seq, event_count, created_at_ms, acked_at_ms
                 FROM event_batches
                 ORDER BY created_at_ms ASC, batch_id ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare event_batches failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query event_batches failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step event_batches failed", err))?
        {
            let batch_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read event_batches.batch_id failed", err))?;
            let binding_id: String = row.get(1).map_err(|err| {
                sqlite_readonly_error("read event_batches.binding_id failed", err)
            })?;
            let from_seq_raw: i64 = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read event_batches.from_seq failed", err))?;
            let to_seq_raw: i64 = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read event_batches.to_seq failed", err))?;
            let event_count_raw: i64 = row.get(4).map_err(|err| {
                sqlite_readonly_error("read event_batches.event_count failed", err)
            })?;
            let created_at_ms: i64 = row.get(5).map_err(|err| {
                sqlite_readonly_error("read event_batches.created_at_ms failed", err)
            })?;
            let acked_at_ms: Option<i64> = row.get(6).map_err(|err| {
                sqlite_readonly_error("read event_batches.acked_at_ms failed", err)
            })?;

            snap.event_batches.push(ReadonlyEventBatchRecord {
                batch_id,
                binding_id,
                from_seq: convert_u64_column("event_batches", "from_seq", from_seq_raw)?,
                to_seq: convert_u64_column("event_batches", "to_seq", to_seq_raw)?,
                event_count: convert_usize_column("event_batches", "event_count", event_count_raw)?,
                created_at_ms,
                acked_at_ms,
            });
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT binding_id, thread_uuid, status, claim_id, pending_since_seq,
                        latest_important_seq, covered_up_to_seq, claimed_at_ms, resolved_at_ms
                 FROM wakeups
                 ORDER BY binding_id ASC",
            )
            .map_err(|err| sqlite_readonly_error("prepare wakeups failed", err))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| sqlite_readonly_error("query wakeups failed", err))?;
        while let Some(row) = rows
            .next()
            .map_err(|err| sqlite_readonly_error("step wakeups failed", err))?
        {
            let binding_id: String = row
                .get(0)
                .map_err(|err| sqlite_readonly_error("read wakeups.binding_id failed", err))?;
            let thread_uuid: String = row
                .get(1)
                .map_err(|err| sqlite_readonly_error("read wakeups.thread_uuid failed", err))?;
            let status: String = row
                .get(2)
                .map_err(|err| sqlite_readonly_error("read wakeups.status failed", err))?;
            let claim_id: Option<String> = row
                .get(3)
                .map_err(|err| sqlite_readonly_error("read wakeups.claim_id failed", err))?;
            let pending_since_seq_raw: Option<i64> = row.get(4).map_err(|err| {
                sqlite_readonly_error("read wakeups.pending_since_seq failed", err)
            })?;
            let latest_important_seq_raw: i64 = row.get(5).map_err(|err| {
                sqlite_readonly_error("read wakeups.latest_important_seq failed", err)
            })?;
            let covered_up_to_seq_raw: i64 = row.get(6).map_err(|err| {
                sqlite_readonly_error("read wakeups.covered_up_to_seq failed", err)
            })?;
            let claimed_at_ms: Option<i64> = row
                .get(7)
                .map_err(|err| sqlite_readonly_error("read wakeups.claimed_at_ms failed", err))?;
            let resolved_at_ms: Option<i64> = row
                .get(8)
                .map_err(|err| sqlite_readonly_error("read wakeups.resolved_at_ms failed", err))?;

            let pending_since_seq = match pending_since_seq_raw {
                Some(val) => Some(convert_u64_column("wakeups", "pending_since_seq", val)?),
                None => None,
            };

            snap.wakeups.push(ReadonlyWakeupRecord {
                binding_id,
                thread_uuid,
                status,
                claim_id,
                pending_since_seq,
                latest_important_seq: convert_u64_column(
                    "wakeups",
                    "latest_important_seq",
                    latest_important_seq_raw,
                )?,
                covered_up_to_seq: convert_u64_column(
                    "wakeups",
                    "covered_up_to_seq",
                    covered_up_to_seq_raw,
                )?,
                claimed_at_ms,
                resolved_at_ms,
            });
        }
    }

    drop(tx);
    drop(conn);

    let parent_post_meta = validate_private_dir_metadata(parent)?;
    verify_unchanged_dir_metadata(
        &parent_pre_meta,
        &parent_post_meta,
        "state.db parent directory",
    )?;
    let db_post_meta = validate_readonly_sqlite_file(
        database_path,
        &parent_post_meta,
        MAX_CORE_DB_BYTES,
        "state.db",
    )?;
    verify_unchanged_file_metadata(&db_pre_meta, &db_post_meta, "state.db")?;

    match (wal_pre_meta, shm_pre_meta) {
        (Some(wal_pre), Some(shm_pre)) => {
            let wal_post = inspect_readonly_sqlite_sidecar(
                &wal_path,
                &parent_post_meta,
                MAX_CORE_DB_BYTES,
                "state.db-wal sidecar",
            )?
            .ok_or_else(|| {
                ObservationJournalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "sqlite read-only state.db-wal sidecar disappeared during read",
                ))
            })?;
            verify_unchanged_file_metadata(&wal_pre, &wal_post, "state.db-wal sidecar")?;

            let shm_post = inspect_readonly_sqlite_sidecar(
                &shm_path,
                &parent_post_meta,
                MAX_CORE_DB_BYTES,
                "state.db-shm sidecar",
            )?
            .ok_or_else(|| {
                ObservationJournalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "sqlite read-only state.db-shm sidecar disappeared during read",
                ))
            })?;
            verify_unchanged_shm_metadata(&shm_pre, &shm_post, "state.db-shm sidecar")?;
        }
        (None, None) => {
            verify_sqlite_sidecar_absent(&wal_path, "state.db-wal sidecar")?;
            verify_sqlite_sidecar_absent(&shm_path, "state.db-shm sidecar")?;
        }
        (Some(_), None) | (None, Some(_)) => unreachable!(),
    }

    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    static TEST_SEQ: AtomicU64 = AtomicU64::new(1);

    struct ScopedPrivateDir {
        path: PathBuf,
    }

    impl ScopedPrivateDir {
        fn new(label: &str) -> Self {
            let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
            let base =
                fs::canonicalize(std::env::temp_dir()).unwrap_or_else(|_| std::env::temp_dir());
            let path = base.join(format!(
                "threadlane-obs-test-{label}-{}-{seq}",
                std::process::id()
            ));
            if path.exists() {
                let _ = fs::remove_dir_all(&path);
            }
            fs::create_dir_all(&path).expect("create test dir");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .expect("chmod 0700 test dir");
            }
            Self { path }
        }
    }

    impl Drop for ScopedPrivateDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct FileStateFingerprint {
        exists: bool,
        bytes: Vec<u8>,
        len: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        dev: u64,
        ino: u64,
        mtime_sec: i64,
        mtime_nsec: i64,
        ctime_sec: i64,
        ctime_nsec: i64,
    }

    fn capture_file_fingerprint(path: &Path) -> FileStateFingerprint {
        match fs::symlink_metadata(path) {
            Ok(meta) => {
                let bytes = fs::read(path).expect("read file bytes for fingerprint");
                #[cfg(unix)]
                {
                    use std::os::unix::fs::{MetadataExt, PermissionsExt};
                    FileStateFingerprint {
                        exists: true,
                        bytes,
                        len: meta.len(),
                        mode: meta.permissions().mode() & 0o777,
                        uid: meta.uid(),
                        gid: meta.gid(),
                        dev: meta.dev(),
                        ino: meta.ino(),
                        mtime_sec: meta.mtime(),
                        mtime_nsec: meta.mtime_nsec(),
                        ctime_sec: meta.ctime(),
                        ctime_nsec: meta.ctime_nsec(),
                    }
                }
                #[cfg(not(unix))]
                {
                    FileStateFingerprint {
                        exists: true,
                        bytes,
                        len: meta.len(),
                        mode: 0,
                        uid: 0,
                        gid: 0,
                        dev: 0,
                        ino: 0,
                        mtime_sec: 0,
                        mtime_nsec: 0,
                        ctime_sec: 0,
                        ctime_nsec: 0,
                    }
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => FileStateFingerprint {
                exists: false,
                bytes: Vec::new(),
                len: 0,
                mode: 0,
                uid: 0,
                gid: 0,
                dev: 0,
                ino: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
                ctime_sec: 0,
                ctime_nsec: 0,
            },
            Err(err) => panic!("unexpected metadata error for {}: {err}", path.display()),
        }
    }

    fn list_dir_names(dir: &Path) -> BTreeSet<String> {
        fs::read_dir(dir)
            .expect("read_dir")
            .map(|entry| {
                entry
                    .expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    }

    fn sample_journal_record(
        record_id: &str,
        request_id: &str,
        phase: ObservationRecordPhase,
        state: ObservationCallState,
    ) -> ObservationJournalRecord {
        ObservationJournalRecord {
            record_id: record_id.to_string(),
            request_id: request_id.to_string(),
            parent_request_id: None,
            call_id: Some(format!("obsc_{record_id}")),
            phase,
            at_ms: 1_700_000_000_000,
            pid: Some(std::process::id()),
            process_instance: current_process_instance(),
            project_label: Some("threadlane".to_string()),
            project_path: None,
            binding_id: Some("bnd_test_1".to_string()),
            thread_uuid: Some("0196ed00-0000-7000-8000-000000000001".to_string()),
            thread_label: None,
            job_id: None,
            plan_id: Some("plan_test_1".to_string()),
            task_id: Some("task_1".to_string()),
            approval_id: None,
            operation: "context.workflow.propose".to_string(),
            title: "workflow propose".to_string(),
            state,
            actor: Some(ObservationActor::unknown(Some(
                "trusted_local_operator".to_string(),
            ))),
            executor: None,
            usage: None,
            delivery: Vec::new(),
            summary: Some("Safe structural summary".to_string()),
        }
    }

    #[test]
    fn read_core_state_readonly_missing_dir_or_db_returns_empty_without_creating_files() {
        let dir = ScopedPrivateDir::new("missing-db");
        let missing_parent_db = dir.path.join("nonexistent-subdir").join("state.db");
        let snap1 = read_core_state_readonly(&missing_parent_db)
            .expect("missing parent dir should return empty snapshot");
        assert_eq!(snap1, ReadonlyCoreStateSnapshot::default());
        assert!(!dir.path.join("nonexistent-subdir").exists());

        let missing_db = dir.path.join("state.db");
        let snap2 = read_core_state_readonly(&missing_db)
            .expect("missing database file should return empty snapshot");
        assert_eq!(snap2, ReadonlyCoreStateSnapshot::default());
        assert!(list_dir_names(&dir.path).is_empty());
    }

    #[test]
    fn read_core_state_readonly_reads_live_wal_and_closed_core_db_without_mutating_files_or_metadata(
    ) {
        let dir = ScopedPrivateDir::new("core-live-wal");
        let db_path = dir.path.join("state.db");
        let wal_path = dir.path.join("state.db-wal");
        let shm_path = dir.path.join("state.db-shm");
        let valid_uuid = "0196ed00-0000-7000-8000-000000000042";

        let mut core = crate::Core::open(&db_path).expect("open Core");
        let binding = core
            .create_binding(valid_uuid, "Primary Thread")
            .expect("create binding");
        let worker = core
            .execute(
                &binding.binding_id,
                crate::Command::StartWorker {
                    command_key: "seed_worker".to_string(),
                    profile_id: "profile_main".to_string(),
                    profile_digest: "a".repeat(64),
                    provider: "antigravity".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    base_commit: "a".repeat(40),
                    execution_binding: None,
                    instruction: "Implement test coverage".to_string(),
                    now_ms: 1_700_000_000_000,
                },
            )
            .expect("queue worker")
            .into_worker()
            .expect("worker output");
        let pulled = core
            .execute(
                &binding.binding_id,
                crate::Command::PullEvents {
                    after_seq: None,
                    limit: 20,
                    now_ms: 1_700_000_000_001,
                },
            )
            .expect("pull events")
            .into_event_batch()
            .expect("batch output");
        assert!(!pulled.batch_id.is_empty());

        // Perform a read on `core` so the WAL-index read mark in `-shm` already reflects the
        // latest WAL frame before taking the pre-read file fingerprint.
        let bindings = core.list_bindings().expect("list bindings");
        assert_eq!(bindings.len(), 1);
        assert!(
            wal_path.exists(),
            "live WAL file should exist while Core is open"
        );

        let dir_entries_before_live = list_dir_names(&dir.path);
        let db_before_live = capture_file_fingerprint(&db_path);
        let wal_before_live = capture_file_fingerprint(&wal_path);
        let shm_before_live = capture_file_fingerprint(&shm_path);

        let snap_live =
            read_core_state_readonly(&db_path).expect("read_core_state_readonly with live WAL");
        assert_eq!(snap_live.bindings.len(), 1);
        assert_eq!(snap_live.bindings[0].binding_id, binding.binding_id);
        assert_eq!(snap_live.bindings[0].thread_uuid, valid_uuid);
        assert_eq!(snap_live.runs.len(), 1);
        assert_eq!(snap_live.runs[0].run_id, worker.run_id);
        assert_eq!(snap_live.workers.len(), 1);
        assert_eq!(snap_live.workers[0].worker_id, worker.worker_id);
        assert!(snap_live.workers[0].to_worker_snapshot().is_some());
        assert!(!snap_live.events.is_empty());
        assert_eq!(snap_live.event_batches.len(), 1);
        assert_eq!(snap_live.wakeups.len(), 1);
        assert_eq!(snap_live.wakeups[0].binding_id, binding.binding_id);

        assert_eq!(list_dir_names(&dir.path), dir_entries_before_live);
        assert_eq!(capture_file_fingerprint(&db_path), db_before_live);
        assert_eq!(capture_file_fingerprint(&wal_path), wal_before_live);
        assert_eq!(capture_file_fingerprint(&shm_path), shm_before_live);

        // Close Core and verify reading the closed database also causes zero file or metadata changes.
        drop(core);

        let dir_entries_before_closed = list_dir_names(&dir.path);
        let db_before_closed = capture_file_fingerprint(&db_path);
        let wal_before_closed = capture_file_fingerprint(&wal_path);
        let shm_before_closed = capture_file_fingerprint(&shm_path);
        assert!(
            !wal_before_closed.exists && !shm_before_closed.exists,
            "WAL and SHM sidecars must be absent after Core drop"
        );

        let snap_closed =
            read_core_state_readonly(&db_path).expect("read_core_state_readonly after Core drop");
        assert_eq!(snap_closed, snap_live);
        assert_eq!(list_dir_names(&dir.path), dir_entries_before_closed);
        assert_eq!(capture_file_fingerprint(&db_path), db_before_closed);
        assert_eq!(capture_file_fingerprint(&wal_path), wal_before_closed);
        assert_eq!(capture_file_fingerprint(&shm_path), shm_before_closed);
        assert!(
            !wal_path.exists() && !shm_path.exists(),
            "WAL and SHM sidecars must remain absent after read_core_state_readonly"
        );
    }

    #[test]
    fn read_core_state_readonly_escapes_reserved_uri_characters_and_rejects_partial_sidecars() {
        let dir = ScopedPrivateDir::new("core-uri-and-sidecars");
        let reserved_subdir = dir.path.join("sub dir %25 ? # & = + ~");
        fs::create_dir(&reserved_subdir).expect("create reserved subdir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&reserved_subdir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 reserved subdir");
        }

        let db_path = reserved_subdir.join("state.db");
        let wal_path = reserved_subdir.join("state.db-wal");
        let shm_path = reserved_subdir.join("state.db-shm");
        let valid_uuid = "0196ed00-0000-7000-8000-000000000077";

        let uri = build_sqlite_readonly_immutable_uri(&db_path).expect("build escaped URI");
        assert!(uri.starts_with("file:///"));
        assert!(uri.ends_with(
            "/sub%20dir%20%2525%20%3F%20%23%20%26%20%3D%20%2B%20~/state.db?mode=ro&immutable=1"
        ));

        let mut core = crate::Core::open(&db_path).expect("open Core in reserved subdir");
        let binding = core
            .create_binding(valid_uuid, "Reserved URI Thread")
            .expect("create binding");
        drop(core);

        let dir_entries_before = list_dir_names(&reserved_subdir);
        let db_before = capture_file_fingerprint(&db_path);
        assert!(!wal_path.exists() && !shm_path.exists());

        let snap = read_core_state_readonly(&db_path)
            .expect("read_core_state_readonly on closed DB with reserved URI chars");
        assert_eq!(snap.bindings.len(), 1);
        assert_eq!(snap.bindings[0].binding_id, binding.binding_id);
        assert_eq!(list_dir_names(&reserved_subdir), dir_entries_before);
        assert_eq!(capture_file_fingerprint(&db_path), db_before);
        assert!(!wal_path.exists() && !shm_path.exists());

        // Exactly one sidecar present must fail closed without creating the missing sidecar.
        fs::write(&wal_path, b"").expect("create lone wal sidecar");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&wal_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 lone wal");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "lone -wal sidecar without -shm must fail closed"
        );
        assert!(!shm_path.exists(), "must not create missing -shm sidecar");
        fs::remove_file(&wal_path).expect("remove lone wal");

        fs::write(&shm_path, b"").expect("create lone shm sidecar");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&shm_path, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 lone shm");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "lone -shm sidecar without -wal must fail closed"
        );
        assert!(!wal_path.exists(), "must not create missing -wal sidecar");
        fs::remove_file(&shm_path).expect("remove lone shm");
    }

    #[test]
    fn read_core_state_readonly_validates_schema_version_and_propagates_prepare_query_row_and_conversion_errors(
    ) {
        let dir = ScopedPrivateDir::new("core-schema-and-errors");
        let db_path = dir.path.join("state.db");
        let valid_uuid = "0196ed00-0000-7000-8000-000000000099";

        // 1. Database without schema_meta must fail rather than returning an empty snapshot.
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open raw sqlite");
            conn.execute_batch("CREATE TABLE dummy (id INTEGER PRIMARY KEY);")
                .expect("create dummy");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "missing schema_meta table must fail"
        );
        let _ = fs::remove_file(&db_path);

        // 2. Seed a real Core database.
        {
            let mut core = crate::Core::open(&db_path).expect("open Core");
            let binding = core
                .create_binding(valid_uuid, "Error Test")
                .expect("create binding");
            core.execute(
                &binding.binding_id,
                crate::Command::StartWorker {
                    command_key: "seed_error_worker".to_string(),
                    profile_id: "profile_1".to_string(),
                    profile_digest: "b".repeat(64),
                    provider: "antigravity".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    base_commit: "b".repeat(40),
                    execution_binding: None,
                    instruction: "Check error propagation".to_string(),
                    now_ms: 1_700_000_000_000,
                },
            )
            .expect("queue worker");
        }

        // 3. Stale or future schema_meta.version must be rejected before querying domain tables.
        for bad_version in [
            0_i64,
            i64::from(crate::SCHEMA_VERSION) - 1,
            i64::from(crate::SCHEMA_VERSION) + 1,
            99,
        ] {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute(
                "UPDATE schema_meta SET version = ?1 WHERE id = 1",
                [bad_version],
            )
            .expect("set bad schema version");
            drop(conn);

            let err = read_core_state_readonly(&db_path)
                .expect_err("mismatched schema_meta.version must fail");
            assert!(
                err.to_string().contains("schema_meta"),
                "error should mention schema_meta: {err}"
            );
        }
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute(
                "UPDATE schema_meta SET version = ?1 WHERE id = 1",
                [crate::SCHEMA_VERSION],
            )
            .expect("restore schema_meta.version");
        }
        assert!(read_core_state_readonly(&db_path).is_ok());

        // 4. Negative integer in a u64/u32 column must propagate a conversion error instead of clamping to 0.
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute("UPDATE bindings SET delivered_seq = -7", [])
                .expect("set negative delivered_seq");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "negative delivered_seq must fail conversion"
        );
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute("UPDATE bindings SET delivered_seq = 0", [])
                .expect("restore delivered_seq");
            conn.execute("UPDATE workers SET attempt_count = -1", [])
                .expect("set negative attempt_count");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "negative attempt_count must fail conversion"
        );
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute("UPDATE workers SET attempt_count = 0", [])
                .expect("restore attempt_count");
        }

        // 5. Missing domain table (`DROP TABLE workers`) must fail rather than returning partial tables.
        {
            let conn = rusqlite::Connection::open(&db_path).expect("open rw");
            conn.execute_batch("DROP TABLE workers;")
                .expect("drop workers table");
        }
        assert!(
            read_core_state_readonly(&db_path).is_err(),
            "missing workers table must fail instead of returning partial snapshot"
        );
    }

    #[test]
    fn user_owned_0700_private_dir_and_journal_file_pass_proc_self_owner_validation_for_append_and_read(
    ) {
        let dir = ScopedPrivateDir::new("owner-validation");
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            let dir_meta = fs::symlink_metadata(&dir.path).expect("stat test dir");
            assert_eq!(dir_meta.permissions().mode() & 0o777, 0o700);
            if let Ok(proc_target_meta) = fs::metadata("/proc/self") {
                assert_eq!(
                    dir_meta.uid(),
                    proc_target_meta.uid(),
                    "real 0700 temp dir must be owned by the current process owner resolved via fs::metadata(\"/proc/self\")"
                );
            }
        }

        validate_existing_private_dir_readonly(&dir.path)
            .expect("user-owned 0700 temp dir must pass validate_existing_private_dir_readonly");

        let rec = sample_journal_record(
            "obsrec_owner_check_1",
            "obsreq_owner_check_1",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );
        append_journal_record(&dir.path, &rec)
            .expect("append_journal_record must succeed for user-owned 0700 directory");

        let journal_path = dir.path.join(OBSERVATION_JOURNAL_FILE_NAME);
        let file_meta =
            validate_existing_private_file_readonly(&journal_path, MAX_OBSERVATION_JOURNAL_BYTES)
                .expect(
                "user-owned 0600 journal file must pass validate_existing_private_file_readonly",
            );
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            assert_eq!(file_meta.permissions().mode() & 0o777, 0o600);
            if let Ok(proc_target_meta) = fs::metadata("/proc/self") {
                assert_eq!(file_meta.uid(), proc_target_meta.uid());
            }
        }

        let (records, warnings) = read_journal_records_readonly(&dir.path)
            .expect("read_journal_records_readonly must succeed for user-owned 0700 directory");
        assert!(warnings.is_empty());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, "obsrec_owner_check_1");
    }

    #[test]
    fn process_proof_requires_live_matching_instance_and_never_trusts_pid_alone_or_mismatched_proof(
    ) {
        let dir = ScopedPrivateDir::new("process-proof");
        let live_inst =
            current_process_instance().expect("Linux /proc should provide process instance");

        // 1. Bare PID without `process_instance` must never prove Running, even after journal round-trip.
        let mut bare_pid_rec = sample_journal_record(
            "obsrec_bare_pid",
            "obsreq_bare_pid",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );
        bare_pid_rec.pid = Some(std::process::id());
        bare_pid_rec.process_instance = None;
        assert!(!bare_pid_rec.has_live_process_instance());
        assert_eq!(
            bare_pid_rec.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        append_journal_record(&dir.path, &bare_pid_rec).expect("append bare_pid_rec");
        let (read_back, warnings) = read_journal_records_readonly(&dir.path).expect("read journal");
        assert!(warnings.is_empty());
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].process_instance, None);
        assert!(!read_back[0].has_live_process_instance());
        assert_eq!(
            read_back[0].resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        // 2. Mismatched record.pid vs process_instance.pid must resolve to Unknown.
        let mut pid_mismatch = bare_pid_rec.clone();
        pid_mismatch.pid = Some(live_inst.pid.saturating_add(1));
        pid_mismatch.process_instance = Some(live_inst.clone());
        assert!(!pid_mismatch.has_live_process_instance());
        assert_eq!(
            pid_mismatch.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        // 3. Reused PID with wrong boot_id must resolve to Unknown.
        let mut wrong_boot = bare_pid_rec.clone();
        wrong_boot.pid = Some(live_inst.pid);
        wrong_boot.process_instance = Some(ObservationProcessInstance {
            pid: live_inst.pid,
            boot_id: "00000000-0000-0000-0000-000000000000".to_string(),
            start_ticks: live_inst.start_ticks,
        });
        assert!(!wrong_boot.has_live_process_instance());
        assert_eq!(
            wrong_boot.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        // 4. Reused PID with wrong start_ticks must resolve to Unknown.
        let mut wrong_ticks = bare_pid_rec.clone();
        wrong_ticks.pid = Some(live_inst.pid);
        wrong_ticks.process_instance = Some(ObservationProcessInstance {
            pid: live_inst.pid,
            boot_id: live_inst.boot_id.clone(),
            start_ticks: live_inst.start_ticks.saturating_add(1),
        });
        assert!(!wrong_ticks.has_live_process_instance());
        assert_eq!(
            wrong_ticks.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        // 5. Dead/non-existent PID must resolve to Unknown.
        let mut dead_pid = bare_pid_rec.clone();
        dead_pid.pid = Some(u32::MAX);
        dead_pid.process_instance = Some(ObservationProcessInstance {
            pid: u32::MAX,
            boot_id: live_inst.boot_id.clone(),
            start_ticks: live_inst.start_ticks,
        });
        assert!(!dead_pid.has_live_process_instance());
        assert_eq!(
            dead_pid.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );

        // 6. Genuinely live matching process instance resolves as Running.
        let mut live_rec = bare_pid_rec.clone();
        live_rec.pid = Some(live_inst.pid);
        live_rec.process_instance = Some(live_inst);
        assert!(live_rec.has_live_process_instance());
        assert_eq!(
            live_rec.resolve_in_flight_state(false),
            ObservationCallState::Running
        );

        // A live MCP server does not prove an individual request future is active.
        let mut mcp_rec = live_rec.clone();
        mcp_rec.operation = "mcp.paired.start_approved_task".to_string();
        assert!(mcp_rec.has_live_process_instance());
        assert_eq!(
            mcp_rec.resolve_in_flight_state(false),
            ObservationCallState::Unknown
        );
        assert_eq!(
            mcp_rec.resolve_in_flight_state(true),
            ObservationCallState::Running
        );

        // 7. Absent/dead process proof with a separately passed active worker receipt resolves as active.
        assert_eq!(
            bare_pid_rec.resolve_in_flight_state(true),
            ObservationCallState::Running
        );
        assert_eq!(
            dead_pid.resolve_in_flight_state(true),
            ObservationCallState::Running
        );
    }

    #[test]
    fn concurrent_cross_thread_journal_appends_preserve_every_complete_parseable_record() {
        let dir = ScopedPrivateDir::new("concurrent-append");
        let dir_path = Arc::new(dir.path.clone());
        let threads = 8usize;
        let per_thread = 25usize;

        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let target_dir = Arc::clone(&dir_path);
            handles.push(std::thread::spawn(move || {
                for i in 0..per_thread {
                    let rec_id = format!("obsrec_t{t}_i{i}");
                    let req_id = format!("obsreq_t{t}_i{i}");
                    let mut rec = sample_journal_record(
                        &rec_id,
                        &req_id,
                        ObservationRecordPhase::Entry,
                        ObservationCallState::Running,
                    );
                    rec.summary =
                        Some(format!("Thread {t} record {i} padding {}", "x".repeat(120)));
                    append_journal_record(&target_dir, &rec).expect("concurrent append");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("join writer thread");
        }

        let journal_path = dir.path.join(OBSERVATION_JOURNAL_FILE_NAME);
        let raw = fs::read_to_string(&journal_path).expect("read journal file");
        let raw_lines: Vec<&str> = raw.lines().collect();
        assert_eq!(raw_lines.len(), threads * per_thread);
        for line in &raw_lines {
            serde_json::from_str::<ObservationJournalRecord>(line)
                .expect("every raw JSONL line must parse cleanly");
        }

        let (records, warnings) =
            read_journal_records_readonly(&dir.path).expect("read_journal_records_readonly");
        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
        assert_eq!(records.len(), threads * per_thread);
        let unique_ids: BTreeSet<String> = records.into_iter().map(|r| r.record_id).collect();
        assert_eq!(unique_ids.len(), threads * per_thread);
    }

    #[test]
    fn malformed_and_truncated_journal_lines_emit_warnings_preserve_valid_rows_and_allow_later_append(
    ) {
        let dir = ScopedPrivateDir::new("malformed-truncated");
        let rec1 = sample_journal_record(
            "obsrec_valid_1",
            "obsreq_1",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );
        let rec2 = sample_journal_record(
            "obsrec_valid_2",
            "obsreq_1",
            ObservationRecordPhase::Finish,
            ObservationCallState::Completed,
        );
        let rec3 = sample_journal_record(
            "obsrec_valid_3",
            "obsreq_2",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );

        append_journal_record(&dir.path, &rec1).expect("append rec1");

        let journal_path = dir.path.join(OBSERVATION_JOURNAL_FILE_NAME);
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .expect("open journal for malformed line");
            file.write_all(b"{\"record_id\":\"malformed_line\",bad_json}\n")
                .expect("write malformed line");
        }

        append_journal_record(&dir.path, &rec2).expect("append rec2");

        // Append a truncated JSON line at EOF without a trailing newline.
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .expect("open journal for truncated line");
            file.write_all(b"{\"record_id\":\"truncated_eof\",\"request_id\":\"obsreq_trunc\"")
                .expect("write truncated line without newline");
        }

        let (records_before, warnings_before) =
            read_journal_records_readonly(&dir.path).expect("read with malformed + truncated");
        assert_eq!(records_before.len(), 2);
        assert_eq!(records_before[0].record_id, "obsrec_valid_1");
        assert_eq!(records_before[1].record_id, "obsrec_valid_2");
        assert_eq!(warnings_before.len(), 2);

        // Appending after a truncated EOF line (missing `\n`) must repair the newline boundary
        // so `rec3` is written on its own complete line and preserved.
        append_journal_record(&dir.path, &rec3).expect("append rec3 after truncated EOF line");

        let (records_after, warnings_after) =
            read_journal_records_readonly(&dir.path).expect("read after later append");
        assert_eq!(records_after.len(), 3);
        assert_eq!(records_after[0].record_id, "obsrec_valid_1");
        assert_eq!(records_after[1].record_id, "obsrec_valid_2");
        assert_eq!(records_after[2].record_id, "obsrec_valid_3");
        assert_eq!(warnings_after.len(), 2);
    }

    #[test]
    fn oversized_journal_over_16_mib_reads_newest_complete_tail_records_and_reports_partial_history(
    ) {
        let dir = ScopedPrivateDir::new("oversized-16mib");
        let journal_path = dir.path.join(OBSERVATION_JOURNAL_FILE_NAME);

        let early_rec = sample_journal_record(
            "obsrec_early_omitted",
            "obsreq_early",
            ObservationRecordPhase::Entry,
            ObservationCallState::Completed,
        );
        append_journal_record(&dir.path, &early_rec).expect("append early record");

        let mut filler_rec = sample_journal_record(
            "obsrec_filler",
            "obsreq_filler",
            ObservationRecordPhase::Progress,
            ObservationCallState::Running,
        );
        filler_rec.summary = Some("p".repeat(MAX_OBSERVATION_SUMMARY_CHARS));
        let mut filler_line = serde_json::to_vec(&filler_rec).expect("serialize filler");
        filler_line.push(b'\n');

        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .expect("open journal for filler");
            let mut written = fs::metadata(&journal_path).expect("meta").len();
            let target = MAX_OBSERVATION_JOURNAL_BYTES + 128 * 1024;
            let mut chunk = Vec::with_capacity(filler_line.len() * 64);
            for _ in 0..64 {
                chunk.extend_from_slice(&filler_line);
            }
            while written < target {
                file.write_all(&chunk).expect("write filler chunk");
                written += chunk.len() as u64;
            }
            file.flush().expect("flush filler");
        }

        let tail_rec_1 = sample_journal_record(
            "obsrec_tail_newest_1",
            "obsreq_tail",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );
        let tail_rec_2 = sample_journal_record(
            "obsrec_tail_newest_2",
            "obsreq_tail",
            ObservationRecordPhase::Finish,
            ObservationCallState::Completed,
        );
        append_journal_record(&dir.path, &tail_rec_1).expect("append tail 1");
        append_journal_record(&dir.path, &tail_rec_2).expect("append tail 2");

        let size_before = fs::metadata(&journal_path).expect("meta before").len();
        assert!(size_before > MAX_OBSERVATION_JOURNAL_BYTES);

        let (records, warnings) =
            read_journal_records_readonly(&dir.path).expect("read oversized journal");
        let size_after = fs::metadata(&journal_path).expect("meta after").len();
        assert_eq!(
            size_before, size_after,
            "oversized read must not rotate, truncate, or delete the journal file"
        );
        assert_eq!(
            warnings.len(),
            1,
            "expected exactly 1 partial-history warning, got {warnings:?}"
        );
        assert!(
            warnings[0].contains("exceeds maximum read window")
                && warnings[0].contains("earlier history was omitted"),
            "unexpected warning text: {}",
            warnings[0]
        );
        assert!(
            !records
                .iter()
                .any(|r| r.record_id == "obsrec_early_omitted"),
            "earliest record outside the trailing 16 MiB window must be omitted"
        );
        assert!(records.len() > 2);
        let last_two = &records[records.len() - 2..];
        assert_eq!(last_two[0].record_id, "obsrec_tail_newest_1");
        assert_eq!(last_two[1].record_id, "obsrec_tail_newest_2");
    }

    #[test]
    fn cli_argv_metadata_redacts_secrets_prompts_unknown_flags_and_request_paths_preserving_only_structural_ids(
    ) {
        let raw_secret_1 = "sk-live-super-secret-key-987654321";
        let raw_secret_2 = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secret";
        let raw_prompt = "Write a parser and include secret token xyz-999";
        let raw_instruction = "Internal worker instruction that must never be logged";
        let raw_summary = "Confidential plan summary text";
        let raw_reason = "Operator cancellation reason prose";
        let raw_answer = "Private user answer text";
        let raw_req_path_1 = "/home/operator/.private/requests/propose-secret.json";
        let raw_req_path_2 = "/tmp/inline-advice-request-payload.json";
        let raw_req_text = "{\"task_summary\":\"raw inline json request body\"}";
        let unknown_flag = "--unrecognized-custom-flag";
        let unknown_val = "unrecognized_flag_sensitive_value";

        let args = vec![
            "propose".to_string(),
            "--plan-id".to_string(),
            "plan_valid_101".to_string(),
            "--approval-id".to_string(),
            "apr_valid_202".to_string(),
            "--task-id=task_valid_303".to_string(),
            "--binding-id".to_string(),
            "bnd_valid_404".to_string(),
            "--parent-request-id".to_string(),
            "obsreq_parent_505".to_string(),
            "--role".to_string(),
            "coder".to_string(),
            "--difficulty=hard".to_string(),
            "--expected-revision".to_string(),
            "7".to_string(),
            "--request-file".to_string(),
            raw_req_path_1.to_string(),
            format!("--request={raw_req_path_2}"),
            "--prompt".to_string(),
            raw_prompt.to_string(),
            "--instruction".to_string(),
            raw_instruction.to_string(),
            "--summary".to_string(),
            raw_summary.to_string(),
            "--reason".to_string(),
            raw_reason.to_string(),
            "--answer".to_string(),
            raw_answer.to_string(),
            unknown_flag.to_string(),
            unknown_val.to_string(),
            format!("--api-header={raw_secret_2}"),
            raw_secret_1.to_string(),
            raw_req_text.to_string(),
        ];

        let meta = sanitize_cli_argv_metadata("context", "workflow", &args);
        assert_eq!(meta.operation, "context.workflow.propose");
        assert_eq!(meta.title, "workflow propose");
        assert_eq!(meta.plan_id.as_deref(), Some("plan_valid_101"));
        assert_eq!(meta.approval_id.as_deref(), Some("apr_valid_202"));
        assert_eq!(meta.task_id.as_deref(), Some("task_valid_303"));
        assert_eq!(meta.binding_id.as_deref(), Some("bnd_valid_404"));
        assert_eq!(meta.parent_request_id.as_deref(), Some("obsreq_parent_505"));
        assert_eq!(meta.role.as_deref(), Some("coder"));
        assert_eq!(meta.difficulty.as_deref(), Some("hard"));
        assert!(meta.safe_summary.contains("request_file=[redacted_path]"));
        assert!(meta.safe_summary.contains("plan_id=plan_valid_101"));
        assert!(meta.safe_summary.contains("approval_id=apr_valid_202"));
        assert!(meta.safe_summary.contains("task_id=task_valid_303"));
        assert!(meta.safe_summary.contains("binding_id=bnd_valid_404"));
        assert!(meta.safe_summary.contains("role=coder"));
        assert!(meta.safe_summary.contains("difficulty=hard"));
        assert!(meta.safe_summary.contains("expected_revision=7"));

        let combined_debug = format!("{meta:?}");
        for forbidden in [
            raw_secret_1,
            raw_secret_2,
            raw_prompt,
            raw_instruction,
            raw_summary,
            raw_reason,
            raw_answer,
            raw_req_path_1,
            raw_req_path_2,
            raw_req_text,
            unknown_flag,
            unknown_val,
        ] {
            assert!(
                !meta.safe_summary.contains(forbidden),
                "safe_summary leaked forbidden value: {forbidden}"
            );
            assert!(
                !combined_debug.contains(forbidden),
                "SafeArgvMetadata leaked forbidden value: {forbidden}"
            );
        }

        // Positional secret at index 0 and secret passed to `--plan-id` must also be rejected.
        let secret_pos_args = vec![
            "sk-positional-secret-000".to_string(),
            "--plan-id".to_string(),
            "sk-secret-plan-id".to_string(),
            "--task-id".to_string(),
            "task_clean_9".to_string(),
        ];
        let meta_pos = sanitize_cli_argv_metadata("context", "advice", &secret_pos_args);
        assert_eq!(meta_pos.operation, "context.advice.invoke");
        assert_eq!(meta_pos.plan_id, None);
        assert_eq!(meta_pos.task_id.as_deref(), Some("task_clean_9"));
        assert!(!format!("{meta_pos:?}").contains("sk-"));
    }

    #[test]
    fn append_journal_record_creates_reopens_and_recovers_journal_without_extra_files() {
        let dir = ScopedPrivateDir::new("append-reopen-recovery");
        assert!(list_dir_names(&dir.path).is_empty());

        let journal_path = dir.path.join(OBSERVATION_JOURNAL_FILE_NAME);
        let rec1 = sample_journal_record(
            "obsrec_reopen_1",
            "obsreq_reopen_1",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );
        let rec2 = sample_journal_record(
            "obsrec_reopen_2",
            "obsreq_reopen_1",
            ObservationRecordPhase::Progress,
            ObservationCallState::Running,
        );
        let rec3 = sample_journal_record(
            "obsrec_reopen_3",
            "obsreq_reopen_1",
            ObservationRecordPhase::Finish,
            ObservationCallState::Completed,
        );

        append_journal_record(&dir.path, &rec1).expect("initial create and dir sync");
        assert_eq!(
            list_dir_names(&dir.path),
            BTreeSet::from([OBSERVATION_JOURNAL_FILE_NAME.to_string()])
        );
        let first_fp = capture_file_fingerprint(&journal_path);
        assert!(first_fp.exists);
        #[cfg(unix)]
        {
            assert_eq!(first_fp.mode, 0o600);
            assert_ne!(first_fp.ino, 0);
        }

        append_journal_record(&dir.path, &rec2).expect("reopen append and dir sync");
        let second_fp = capture_file_fingerprint(&journal_path);
        assert_eq!(second_fp.dev, first_fp.dev);
        assert_eq!(second_fp.ino, first_fp.ino);
        assert!(second_fp.len > first_fp.len);

        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&journal_path)
                .expect("open journal for partial tail");
            file.write_all(b"{\"record_id\":\"partial_tail\"")
                .expect("write partial tail without newline");
        }

        append_journal_record(&dir.path, &rec3).expect("recover after partial tail and sync");
        let third_fp = capture_file_fingerprint(&journal_path);
        assert_eq!(third_fp.dev, first_fp.dev);
        assert_eq!(third_fp.ino, first_fp.ino);
        assert_eq!(
            list_dir_names(&dir.path),
            BTreeSet::from([OBSERVATION_JOURNAL_FILE_NAME.to_string()])
        );

        let (records, warnings) =
            read_journal_records_readonly(&dir.path).expect("read recovered journal");
        assert_eq!(warnings.len(), 1);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record_id, "obsrec_reopen_1");
        assert_eq!(records[1].record_id, "obsrec_reopen_2");
        assert_eq!(records[2].record_id, "obsrec_reopen_3");
    }

    #[test]
    fn append_journal_record_refuses_symlinks_and_dir_substitution_without_unrelated_writes() {
        let workspace = ScopedPrivateDir::new("dir-substitution");
        let target_dir = ScopedPrivateDir::new("symlink-target");
        let rec = sample_journal_record(
            "obsrec_sec_1",
            "obsreq_sec_1",
            ObservationRecordPhase::Entry,
            ObservationCallState::Running,
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::{symlink, PermissionsExt};

            let symlink_dir = workspace.path.join("store-symlink");
            symlink(&target_dir.path, &symlink_dir).expect("create directory symlink");
            let target_meta =
                validate_private_dir_metadata(&target_dir.path).expect("valid target dir");
            assert!(matches!(
                open_verified_private_dir(&symlink_dir, &target_meta),
                Err(ObservationJournalError::SecurityViolation(_))
            ));
            assert!(matches!(
                append_journal_record(&symlink_dir, &rec),
                Err(ObservationJournalError::SecurityViolation(_))
            ));
            assert!(list_dir_names(&target_dir.path).is_empty());

            let nested_real = target_dir.path.join("nested");
            fs::create_dir(&nested_real).expect("create nested target dir");
            fs::set_permissions(&nested_real, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 nested target dir");
            assert!(matches!(
                append_journal_record(&symlink_dir.join("nested"), &rec),
                Err(ObservationJournalError::SecurityViolation(_))
            ));
            assert!(list_dir_names(&nested_real).is_empty());
            fs::remove_dir(&nested_real).expect("remove nested target dir");
            fs::remove_file(&symlink_dir).expect("remove directory symlink");

            let outside_file = target_dir.path.join("outside.jsonl");
            fs::write(&outside_file, b"preserve-me\n").expect("seed outside file");
            fs::set_permissions(&outside_file, fs::Permissions::from_mode(0o600))
                .expect("chmod 0600 outside file");
            let outside_before = capture_file_fingerprint(&outside_file);
            let journal_symlink = workspace.path.join(OBSERVATION_JOURNAL_FILE_NAME);
            symlink(&outside_file, &journal_symlink).expect("create journal file symlink");
            assert!(matches!(
                append_journal_record(&workspace.path, &rec),
                Err(ObservationJournalError::SecurityViolation(_))
            ));
            assert_eq!(capture_file_fingerprint(&outside_file), outside_before);
            fs::remove_file(&journal_symlink).expect("remove journal file symlink");

            let active_dir = workspace.path.join("active-store");
            let replacement_dir = workspace.path.join("replacement-store");
            let displaced_dir = workspace.path.join("displaced-store");
            fs::create_dir(&active_dir).expect("create active_dir");
            fs::set_permissions(&active_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 active_dir");
            fs::create_dir(&replacement_dir).expect("create replacement_dir");
            fs::set_permissions(&replacement_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 replacement_dir");

            let dir_meta = validate_private_dir_metadata(&active_dir).expect("active_dir meta");
            let dir_file =
                open_verified_private_dir(&active_dir, &dir_meta).expect("open active_dir");
            revalidate_private_dir_identity(&active_dir, &dir_meta, Some(&dir_file))
                .expect("initial directory identity revalidation");

            fs::rename(&active_dir, &displaced_dir).expect("displace active_dir");
            fs::rename(&replacement_dir, &active_dir).expect("substitute replacement_dir");
            assert!(matches!(
                revalidate_private_dir_identity(&active_dir, &dir_meta, Some(&dir_file)),
                Err(ObservationJournalError::SecurityViolation(_))
            ));
            assert!(list_dir_names(&active_dir).is_empty());
            assert!(list_dir_names(&displaced_dir).is_empty());
        }
        #[cfg(not(unix))]
        {
            let _ = (&workspace, &target_dir, &rec);
        }
    }

    #[test]
    fn verified_mcp_context_preserves_scope_and_declared_role_evidence() {
        let exec = ObservationExecutor::local_mcp();
        assert_eq!(exec.tool.as_deref(), Some("threadlane_mcp"));
        assert_eq!(exec.provider.as_deref(), Some("local"));
        assert_eq!(exec.role, None);
        assert_eq!(exec.requested_model, None);
        assert_eq!(exec.observed_model, None);
        assert_eq!(exec.requested_effort, None);
        assert_eq!(exec.observed_effort, None);

        let ctx = VerifiedMcpObservationContext::new(
            "bnd_0196ed00-0000-7000-8000-000000000001",
            "0196ed00-0000-7000-8000-00000000000a",
            "  Alpha\nChat\tTitle  ",
            Path::new("/tmp/workspace/alpha-repo"),
            Some("proj_alpha"),
        );
        assert_eq!(ctx.thread_label, "AlphaChatTitle");
        assert_eq!(ctx.project_label, "alpha-repo");
        assert_eq!(
            ctx.project_path.as_deref(),
            Some("/tmp/workspace/alpha-repo")
        );
        assert_eq!(ctx.actor.label.as_deref(), Some("Codex · AlphaChatTitle"));
        assert_eq!(ctx.actor.role.as_deref(), Some("coworker"));
        assert_eq!(ctx.actor.source.as_deref(), Some("paired_mcp"));
        assert_eq!(ctx.actor.model, None);
        assert_eq!(ctx.actor.effort, None);
        assert_eq!(ctx.actor.evidence, ObservationActorEvidence::Configured);
        assert_eq!(
            ObservationActor::paired_follower("AlphaChatTitle"),
            ctx.actor
        );

        let legacy_actor: ObservationActor = serde_json::from_str(
            r#"{"label":"Codex · Legacy Chat","role":"follower","source":"paired_mcp","model":null,"effort":null,"evidence":"configured"}"#,
        )
        .expect("historical follower actor must still deserialize");
        assert_eq!(legacy_actor.role.as_deref(), Some("follower"));
        assert_eq!(legacy_actor.evidence, ObservationActorEvidence::Configured);

        assert!(ctx
            .matching_scope(
                "bnd_0196ed00-0000-7000-8000-000000000001",
                "0196ed00-0000-7000-8000-00000000000a"
            )
            .is_some());
        assert!(ctx
            .matching_scope(
                "bnd_0196ed00-0000-7000-8000-000000000002",
                "0196ed00-0000-7000-8000-00000000000a"
            )
            .is_none());
        assert!(ctx
            .matching_scope(
                "bnd_0196ed00-0000-7000-8000-000000000001",
                "0196ed00-0000-7000-8000-00000000000b"
            )
            .is_none());
    }

    #[test]
    fn verified_mcp_context_preserves_path_whitespace_and_bounds_display_labels() {
        let path = Path::new("/tmp/workspace/ spaced-repo ");
        let title = "🦀".repeat(crate::MAX_LABEL_LEN);
        let context = VerifiedMcpObservationContext::new("binding", "thread", &title, path, None);
        assert_eq!(context.project_path.as_deref(), path.to_str());
        assert_eq!(context.project_label, "spaced-repo");
        assert!(!context.thread_label.is_empty());
        assert!(context.thread_label.len() <= crate::MAX_LABEL_LEN);
        assert!(!context.thread_label.chars().any(char::is_control));

        let invalid_path = Path::new("/tmp/workspace/control\npath");
        let context =
            VerifiedMcpObservationContext::new("binding", "thread", "Chat", invalid_path, None);
        assert!(context.project_path.is_none());
    }
}
