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
    /// Configured / selected conversational model from verified exact-thread native metadata; never treated as runtime execution telemetry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_model: Option<String>,
    /// Configured / selected conversational reasoning effort from verified exact-thread native metadata; never treated as runtime execution telemetry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_effort: Option<String>,
    /// Provenance level of the actor metadata.
    pub evidence: ObservationActorEvidence,
}

/// Sanitizes an optional configured model or effort token from verified native metadata,
/// stripping control characters, rejecting secret-like markers, and bounding length to
/// [`crate::MAX_LABEL_LEN`] bytes.
pub fn sanitize_observation_selection_token(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let mut out = String::new();
    for ch in raw.chars() {
        if !ch.is_control() && out.len() + ch.len_utf8() <= crate::MAX_LABEL_LEN {
            out.push(ch);
        }
    }
    let trimmed = out.trim();
    if trimmed.is_empty() || contains_sensitive_marker(&trimmed.to_ascii_lowercase()) {
        None
    } else {
        Some(trimmed.to_string())
    }
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
            selected_model: None,
            selected_effort: None,
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
            selected_model: None,
            selected_effort: None,
            evidence: ObservationActorEvidence::Configured,
        }
    }

    /// Returns a verified paired-MCP caller actor record carrying sanitized configured thread
    /// selection metadata while keeping runtime-observed `model` and `effort` `None`.
    pub fn paired_coworker_with_selection(
        thread_label: &str,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) -> Self {
        Self::paired_coworker(thread_label).with_selected_model(selected_model, selected_effort)
    }

    /// Attaches or replaces sanitized configured `selected_model` and `selected_effort` metadata
    /// while leaving runtime-observed `model` and `effort` untouched.
    pub fn with_selected_model(
        mut self,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) -> Self {
        self.selected_model = sanitize_observation_selection_token(selected_model);
        self.selected_effort = sanitize_observation_selection_token(selected_effort);
        self
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

    /// Attaches or replaces sanitized configured `selected_model` and `selected_effort` metadata
    /// from verified exact-thread native metadata while keeping runtime-observed `model` and
    /// `effort` `None`.
    pub fn with_selected_model(
        mut self,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) -> Self {
        self.actor = self
            .actor
            .with_selected_model(selected_model, selected_effort);
        self
    }

    /// Clears configured `selected_model` and `selected_effort` when a live metadata refresh is
    /// unavailable or fails validation so stale selections are never carried forward as current.
    pub fn clear_selected_model(&mut self) {
        self.actor.selected_model = None;
        self.actor.selected_effort = None;
    }

    /// Updates the sanitized thread title and configured `selected_model` / `selected_effort`
    /// from a verified exact-thread native metadata read.
    pub fn refresh_thread_metadata(
        &mut self,
        thread_title: Option<&str>,
        selected_model: Option<&str>,
        selected_effort: Option<&str>,
    ) {
        if let Some(title) = thread_title {
            let sanitized = sanitize_observation_label(title, "Codex chat");
            self.thread_label = sanitized.clone();
            self.actor.label = Some(format!("Codex · {sanitized}"));
        }
        self.actor.selected_model = sanitize_observation_selection_token(selected_model);
        self.actor.selected_effort = sanitize_observation_selection_token(selected_effort);
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

/// Structured provenance context for a single read-only observation warning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationWarningContext {
    /// Sanitized warning message matching an entry in [`ObservationSnapshot::warnings`].
    pub warning: String,
    /// Validated registry observation source identifier (`src_...`) when the warning originated from a specific source, or `None` for global/registry/snapshot warnings.
    pub source_id: Option<String>,
    /// Subsystem origin token (`registry`, `jobs`, `journal`, `requests`, or `snapshot`).
    pub origin: String,
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
    /// Optional structured provenance contexts for entries in [`Self::warnings`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warning_contexts: Vec<ObservationWarningContext>,
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

/// Errors returned by read-only SQLite state queries that must never create or mutate files.
#[derive(Debug, Error)]
pub enum ReadonlySqliteError {
    /// A clean immutable database snapshot is unavailable without recovery.
    #[error("clean immutable SQLite snapshot unavailable: {0}")]
    SnapshotUnavailable(String),
    /// Target directory or database file failed private path or symlink validation.
    #[error("sqlite read-only path failed security validation: {0}")]
    SecurityViolation(String),
    /// Target database or sidecar file exceeds the maximum allowed size.
    #[error("sqlite read-only file exceeds maximum size limit")]
    DatabaseTooLarge,
    /// Underlying filesystem or SQLite read-only I/O error.
    #[error("sqlite read-only I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Core domain or query validation error.
    #[error(transparent)]
    Core(#[from] crate::CoreError),
}

impl From<ObservationJournalError> for ReadonlySqliteError {
    fn from(err: ObservationJournalError) -> Self {
        match err {
            ObservationJournalError::SecurityViolation(reason) => Self::SecurityViolation(reason),
            ObservationJournalError::JournalTooLarge | ObservationJournalError::RecordTooLarge => {
                Self::DatabaseTooLarge
            }
            ObservationJournalError::Io(io_err) => Self::Io(io_err),
            ObservationJournalError::Json(json_err) => Self::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                json_err.to_string(),
            )),
        }
    }
}

/// Executes `f` inside a strictly read-only SQLite deferred transaction on an existing `state.db`
/// without creating directories, creating database or sidecar files, running schema migrations,
/// recovering in-flight state, or modifying any rows or filesystem metadata.
pub(crate) fn with_existing_readonly_sqlite_tx<T, F>(
    database_path: &Path,
    f: F,
) -> Result<T, ReadonlySqliteError>
where
    F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T, crate::CoreError>,
{
    const MAX_CORE_DB_BYTES: u64 = 64 * 1024 * 1024;

    let parent = database_path.parent().ok_or_else(|| {
        ReadonlySqliteError::SecurityViolation("database path has no parent directory".to_string())
    })?;
    let parent_pre_meta = validate_private_dir_metadata(parent)?;
    let db_pre_meta = validate_readonly_sqlite_file(
        database_path,
        &parent_pre_meta,
        MAX_CORE_DB_BYTES,
        "state.db",
    )?;

    let wal_path = sqlite_sidecar_path(database_path, "-wal");
    let shm_path = sqlite_sidecar_path(database_path, "-shm");
    // A stopped database must already be checkpointed. Even a read-only SQLite WAL
    // connection can write shared-memory read marks; do not open it or ignore WAL frames.
    for (path, label) in [(&wal_path, "state.db-wal"), (&shm_path, "state.db-shm")] {
        if inspect_readonly_sqlite_sidecar(path, &parent_pre_meta, MAX_CORE_DB_BYTES, label)?
            .is_some()
        {
            return Err(ReadonlySqliteError::SnapshotUnavailable(
                "residual WAL/SHM sidecars require recovery; stopped history cannot provide a clean immutable snapshot".to_string(),
            ));
        }
    }
    let uri = build_sqlite_readonly_immutable_uri(database_path)?;
    let mut conn = rusqlite::Connection::open_with_flags(
        &uri,
        rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|err| {
        ReadonlySqliteError::from(sqlite_readonly_error("immutable open failed", err))
    })?;

    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
        .map_err(|err| {
            ReadonlySqliteError::from(sqlite_readonly_error("begin read transaction failed", err))
        })?;

    let schema_version: i64 = tx
        .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|err| {
            ReadonlySqliteError::from(sqlite_readonly_error("schema_meta query failed", err))
        })?;
    if schema_version != i64::from(crate::SCHEMA_VERSION) {
        let found = u32::try_from(schema_version).unwrap_or(u32::MAX);
        return Err(ReadonlySqliteError::Core(
            crate::CoreError::IncompatibleSchemaVersion {
                found,
                expected: crate::SCHEMA_VERSION,
            },
        ));
    }

    let out = f(&tx)?;

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

    verify_sqlite_sidecar_absent(&wal_path, "state.db-wal sidecar")?;
    verify_sqlite_sidecar_absent(&shm_path, "state.db-shm sidecar")?;

    Ok(out)
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

/// Wire schema version for [`WorkspaceSnapshot`] envelopes (`1`).
pub const WORKSPACE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Default page limit for paginated [`ObservationRequest`] reads.
pub const DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT: usize = 50;

/// Maximum records returned per paginated [`ObservationRequest`] page (`200`).
pub const MAX_OBSERVATION_REQUEST_PAGE_LIMIT: usize = 200;

/// Maximum [`ObservationJob`] entries projected in a snapshot (`500`).
pub const MAX_OBSERVATION_JOBS: usize = 500;

/// Maximum [`ObservationRequest`] entries retained in the frontend request window (`1,000`).
pub const MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW: usize = 1_000;

/// Maximum serialized UTF-8 byte size (`64 KiB`) of a single native sample sidecar file.
pub const MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES: usize = 64 * 1024;

/// Maximum number of native sample sidecar files retained per observation source (`128`).
pub const MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE: usize = 128;

/// Maximum number of safe items retained in a [`WorkspaceMessagesWindow`] (`1,000`).
pub const MAX_WORKSPACE_MESSAGES_WINDOW: usize = 1_000;

/// Maximum structured job history records projected per `(source_id, binding_id)` (`64`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_JOBS_PER_BINDING: usize = 64;

/// Maximum tasks projected per [`WorkspaceJobHistory`] (`64`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_TASKS_PER_JOB: usize = 64;

/// Maximum attempts projected per [`WorkspaceTaskHistory`] (`16`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_ATTEMPTS_PER_TASK: usize = 16;

/// Maximum task candidates projected per [`WorkspaceTaskHistory`] (`16`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_CANDIDATES_PER_TASK: usize = 16;

/// Maximum run references projected per [`WorkspaceJobHistory`] (`256`).
pub const MAX_WORKSPACE_RUNS_PER_JOB: usize = 256;

/// Maximum outbox reservation/dispatch records projected per [`WorkspaceJobHistory`] (`256`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_OUTBOX_PER_JOB: usize = 256;

/// Maximum combined integration candidates projected per [`WorkspaceJobHistory`] (`32`),
/// matching the durable coworker ledger capacity.
pub const MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB: usize = 32;

/// Maximum correlated link identifiers retained per list inside [`WorkspaceRecordLinks`] (`64`).
pub const MAX_WORKSPACE_RECORD_LINKS_PER_LIST: usize = 64;

/// Initial native observation sampling interval in milliseconds (`2,000 ms` / `2 s`).
pub const WORKSPACE_SAMPLING_INTERVAL_MS: u64 = 2_000;

/// Native metadata read timeout in milliseconds (`750 ms`).
pub const WORKSPACE_METADATA_TIMEOUT_MS: u64 = 750;

/// Native sample staleness threshold in milliseconds (`10,000 ms` / `10 s`).
pub const WORKSPACE_SAMPLE_STALE_THRESHOLD_MS: u64 = 10_000;

const NIL_UUID_HYPHENATED: &str = "00000000-0000-0000-0000-000000000000";

/// Contract validation and bounding errors for workspace observation projections.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkspaceContractError {
    /// A [`WorkspaceChatIdentity`] component was missing, malformed, or substituted with a
    /// non-identity token (such as a chat title, `sessionId`, or `job_id`).
    #[error("invalid workspace chat identity field `{field}`: {reason}")]
    InvalidChatIdentity {
        /// Name of the invalid identity field.
        field: &'static str,
        /// Human-readable validation failure reason.
        reason: String,
    },
    /// A [`WorkspaceNativeSample`] failed structural validation.
    #[error("invalid workspace native sample: {0}")]
    InvalidNativeSample(String),
    /// A native sample sidecar exceeded [`MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES`].
    #[error("native sample sidecar size ({actual_bytes} bytes) exceeds {max_bytes}-byte limit")]
    SidecarTooLarge {
        /// Serialized byte length of the sidecar payload.
        actual_bytes: usize,
        /// Maximum permitted byte length (`65,536`).
        max_bytes: usize,
    },
    /// A `Messages` item attempted to use a forbidden or unrecognized source kind (such as a raw
    /// transcript or synthetic user message).
    #[error("forbidden or unrecognized workspace message source kind `{0}`")]
    ForbiddenMessageSource(String),
    /// Malformed UTF-8 or JSON payload when decoding a sidecar.
    #[error("invalid workspace JSON payload: {0}")]
    InvalidJson(String),
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn default_unknown_token() -> String {
    "unknown".to_string()
}

fn deserialize_non_empty_or_unknown<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(sanitize_status_token(raw.as_deref()))
}

fn default_sampling_interval_ms() -> u64 {
    WORKSPACE_SAMPLING_INTERVAL_MS
}

fn default_metadata_timeout_ms() -> u64 {
    WORKSPACE_METADATA_TIMEOUT_MS
}

fn default_stale_threshold_ms() -> u64 {
    WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
}

fn default_max_messages_window() -> usize {
    MAX_WORKSPACE_MESSAGES_WINDOW
}

fn default_max_summary_chars() -> usize {
    MAX_OBSERVATION_SUMMARY_CHARS
}

fn default_workspace_schema_version() -> u32 {
    WORKSPACE_SNAPSHOT_SCHEMA_VERSION
}

fn default_observation_job_state_unknown() -> ObservationJobState {
    ObservationJobState::Unknown
}

fn deserialize_workspace_job_state<'de, D>(deserializer: D) -> Result<ObservationJobState, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    let state = match raw.as_deref().map(str::trim) {
        Some("planning") => ObservationJobState::Planning,
        Some("ready") => ObservationJobState::Ready,
        Some("running") => ObservationJobState::Running,
        Some("reviewing") => ObservationJobState::Reviewing,
        Some("needs_user") => ObservationJobState::NeedsUser,
        Some("completed") => ObservationJobState::Completed,
        Some("failed") => ObservationJobState::Failed,
        _ => ObservationJobState::Unknown,
    };
    Ok(state)
}

fn default_observation_delivery_stage_unknown() -> ObservationDeliveryStage {
    ObservationDeliveryStage::Unknown
}

fn deserialize_workspace_delivery_stage<'de, D>(
    deserializer: D,
) -> Result<ObservationDeliveryStage, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    let stage = match raw.as_deref().map(str::trim) {
        Some("result_ready") => ObservationDeliveryStage::ResultReady,
        Some("notification_submitted") => ObservationDeliveryStage::NotificationSubmitted,
        Some("batch_delivered") => ObservationDeliveryStage::BatchDelivered,
        Some("batch_acknowledged") => ObservationDeliveryStage::BatchAcknowledged,
        Some("response_returned") => ObservationDeliveryStage::ResponseReturned,
        Some("failed") => ObservationDeliveryStage::Failed,
        _ => ObservationDeliveryStage::Unknown,
    };
    Ok(stage)
}

fn default_empty_observation_snapshot() -> ObservationSnapshot {
    ObservationSnapshot {
        schema_version: OBSERVATION_SCHEMA_VERSION,
        generated_at_ms: 0,
        sources: Vec::new(),
        jobs: Vec::new(),
        requests: Vec::new(),
        next_cursor: None,
        has_more: false,
        jobs_has_more: false,
        warnings: Vec::new(),
        warning_contexts: Vec::new(),
    }
}

fn sanitize_status_token(raw: Option<&str>) -> String {
    let Some(raw) = raw else {
        return default_unknown_token();
    };
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 64
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        || contains_sensitive_marker(&trimmed.to_ascii_lowercase())
    {
        default_unknown_token()
    } else {
        trimmed.to_string()
    }
}

fn clean_opt_id(raw: Option<&str>) -> Option<String> {
    raw.and_then(validate_correlation_id)
}

fn clean_opt_summary(raw: Option<&str>) -> Option<String> {
    raw.map(sanitize_workspace_summary)
        .filter(|s| !s.is_empty())
}

fn clean_id_list(items: &[String], max_items: usize) -> Vec<String> {
    items
        .iter()
        .filter_map(|id| validate_correlation_id(id))
        .take(max_items)
        .collect()
}

fn cap_tail<T>(mut items: Vec<T>, max_len: usize) -> (Vec<T>, usize) {
    let omitted = items.len().saturating_sub(max_len);
    if omitted > 0 {
        items.drain(0..omitted);
    }
    (items, omitted)
}

fn is_forbidden_chat_identity_substitute_token(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    lower.starts_with("job_")
        || lower.starts_with("job-")
        || lower.starts_with("job:")
        || lower.starts_with("plan_")
        || lower.starts_with("plan-")
        || lower.starts_with("plan:")
        || lower.starts_with("sess_")
        || lower.starts_with("sess-")
        || lower.starts_with("sess:")
        || lower.starts_with("session_")
        || lower.starts_with("session-")
        || lower.starts_with("session:")
        || lower.starts_with("sessionid")
}

fn validate_workspace_source_id(raw: &str) -> Result<String, WorkspaceContractError> {
    if raw.is_empty() || raw.len() > crate::MAX_LABEL_LEN || raw.trim() != raw {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "source_id",
            reason: "must be a non-empty trimmed identifier up to 128 bytes".to_string(),
        });
    }
    if !raw
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "source_id",
            reason: "must contain only ASCII [A-Za-z0-9_.-] characters".to_string(),
        });
    }
    if contains_sensitive_marker(&raw.to_ascii_lowercase()) {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "source_id",
            reason: "must not contain sensitive markers".to_string(),
        });
    }
    if is_forbidden_chat_identity_substitute_token(raw) {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "source_id",
            reason: "job_id, plan_id, or sessionId cannot replace source_id".to_string(),
        });
    }
    Ok(raw.to_string())
}

fn validate_workspace_binding_id(raw: &str) -> Result<String, WorkspaceContractError> {
    let Some(clean) = validate_correlation_id(raw) else {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "binding_id",
            reason: "must be a valid non-empty ASCII identifier up to 128 bytes".to_string(),
        });
    };
    if clean != raw {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "binding_id",
            reason: "must not contain leading or trailing whitespace".to_string(),
        });
    }
    if is_forbidden_chat_identity_substitute_token(&clean) {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "binding_id",
            reason: "job_id, plan_id, or sessionId cannot replace binding_id".to_string(),
        });
    }
    Ok(clean)
}

fn is_canonical_lowercase_non_nil_uuid(raw: &str) -> bool {
    if raw.len() != 36 || raw == NIL_UUID_HYPHENATED {
        return false;
    }
    for (idx, b) in raw.bytes().enumerate() {
        let is_hyphen = matches!(idx, 8 | 13 | 18 | 23);
        if is_hyphen {
            if b != b'-' {
                return false;
            }
        } else if !(b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return false;
        }
    }
    true
}

fn validate_workspace_thread_uuid(raw: &str) -> Result<String, WorkspaceContractError> {
    if !is_canonical_lowercase_non_nil_uuid(raw) {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "thread_uuid",
            reason: "must be a canonical lowercase hyphenated non-nil UUID".to_string(),
        });
    }
    Ok(raw.to_string())
}

fn validate_canonical_project_scope(raw: &str) -> Result<String, WorkspaceContractError> {
    if raw.is_empty()
        || raw.len() > crate::MAX_WORKER_PATH_BYTES
        || raw.trim() != raw
        || !raw.starts_with('/')
        || raw.chars().any(char::is_control)
    {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "canonical_project_scope",
            reason: "must be a trimmed absolute path up to 512 UTF-8 bytes".to_string(),
        });
    }
    if raw.contains("//")
        || raw == "/."
        || raw == "/.."
        || raw.contains("/./")
        || raw.contains("/../")
        || raw.ends_with("/.")
        || raw.ends_with("/..")
        || (raw.len() > 1 && raw.ends_with('/'))
    {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "canonical_project_scope",
            reason: "must not contain '//', '.', '..', or trailing slashes".to_string(),
        });
    }
    if contains_sk_prefix_marker(&raw.to_ascii_lowercase()) {
        return Err(WorkspaceContractError::InvalidChatIdentity {
            field: "canonical_project_scope",
            reason: "must not contain secret-like tokens".to_string(),
        });
    }
    Ok(raw.to_string())
}

/// Sanitized workspace summary text paired with Unicode-safe truncation and redaction metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSanitizedSummary {
    /// Sanitized text guaranteed to contain at most [`MAX_OBSERVATION_SUMMARY_CHARS`] Unicode scalar values.
    pub summary: String,
    /// `true` when the normalized text exceeded [`MAX_OBSERVATION_SUMMARY_CHARS`] and was truncated.
    pub truncated: bool,
    /// Original character count after control-character/whitespace normalization (or `None` when redacted).
    pub original_char_count: Option<usize>,
    /// `true` when sensitive marker detection replaced the text with `[redacted sensitive metadata]`.
    pub redacted: bool,
}

/// Sanitizes a workspace summary string, collapsing control characters and whitespace, redacting
/// secret-like markers, and enforcing a strict Unicode-safe [`MAX_OBSERVATION_SUMMARY_CHARS`]
/// (`240` scalar values, inclusive of `"..."`) character bound.
pub fn sanitize_workspace_summary_with_meta(raw: &str) -> WorkspaceSanitizedSummary {
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
        return WorkspaceSanitizedSummary {
            summary: "[redacted sensitive metadata]".to_string(),
            truncated: false,
            original_char_count: None,
            redacted: true,
        };
    }

    let char_count = collapsed.chars().count();
    if char_count <= MAX_OBSERVATION_SUMMARY_CHARS {
        WorkspaceSanitizedSummary {
            summary: collapsed,
            truncated: false,
            original_char_count: Some(char_count),
            redacted: false,
        }
    } else {
        let keep = MAX_OBSERVATION_SUMMARY_CHARS.saturating_sub(3);
        let mut out: String = collapsed.chars().take(keep).collect();
        out.push_str("...");
        WorkspaceSanitizedSummary {
            summary: out,
            truncated: true,
            original_char_count: Some(char_count),
            redacted: false,
        }
    }
}

/// Sanitizes a workspace summary string to at most [`MAX_OBSERVATION_SUMMARY_CHARS`] (`240`)
/// Unicode scalar values (including `"..."` when truncated).
pub fn sanitize_workspace_summary(raw: &str) -> String {
    sanitize_workspace_summary_with_meta(raw).summary
}

/// Authoritative observational identity for a native conversation.
///
/// Always requires all four verified components: `source_id`, `binding_id`, `thread_uuid`, and
/// `canonical_project_scope`. Display titles (`thread_label`), shared-fork `sessionId` tokens, and
/// `job_id` / `plan_id` identifiers must never substitute for chat identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct WorkspaceChatIdentity {
    /// Validated observation source identifier (`default`, `src_...`, `ctx_...`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_id: String,
    /// Validated scoped software `binding_id` (`bnd_...`, `apr_...`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub binding_id: String,
    /// Canonical lowercase hyphenated non-nil native `thread_uuid`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub thread_uuid: String,
    /// Verified canonical absolute project path scoping this conversation.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub canonical_project_scope: String,
}

impl WorkspaceChatIdentity {
    /// Constructs and validates an authoritative [`WorkspaceChatIdentity`].
    pub fn new(
        source_id: &str,
        binding_id: &str,
        thread_uuid: &str,
        canonical_project_scope: &str,
    ) -> Result<Self, WorkspaceContractError> {
        let identity = Self {
            source_id: validate_workspace_source_id(source_id)?,
            binding_id: validate_workspace_binding_id(binding_id)?,
            thread_uuid: validate_workspace_thread_uuid(thread_uuid)?,
            canonical_project_scope: validate_canonical_project_scope(canonical_project_scope)?,
        };
        Ok(identity)
    }

    /// Validates all four components of this [`WorkspaceChatIdentity`].
    pub fn validate(&self) -> Result<(), WorkspaceContractError> {
        let _ = validate_workspace_source_id(&self.source_id)?;
        let _ = validate_workspace_binding_id(&self.binding_id)?;
        let _ = validate_workspace_thread_uuid(&self.thread_uuid)?;
        let _ = validate_canonical_project_scope(&self.canonical_project_scope)?;
        Ok(())
    }

    /// Returns a collision-free length-prefixed UTF-8 canonical key (`wchat:v1:...`) when valid,
    /// or `None` if any identity component fails validation.
    pub fn canonical_key(&self) -> Option<String> {
        self.validate().ok()?;
        Some(format!(
            "wchat:v1:{}:{}:{}:{}:{}:{}:{}:{}",
            self.source_id.len(),
            self.source_id,
            self.binding_id.len(),
            self.binding_id,
            self.thread_uuid.len(),
            self.thread_uuid,
            self.canonical_project_scope.len(),
            self.canonical_project_scope,
        ))
    }
}

/// Native sampling and staleness policy carried by [`WorkspaceSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSamplingPolicy {
    /// Target background sampling interval in milliseconds (`2,000 ms` initial default).
    #[serde(default = "default_sampling_interval_ms")]
    pub interval_ms: u64,
    /// Exact-thread metadata read timeout in milliseconds (`750 ms` default).
    #[serde(default = "default_metadata_timeout_ms")]
    pub metadata_timeout_ms: u64,
    /// Sample age threshold in milliseconds beyond which a live sample degrades to `stale` (`10,000 ms` default).
    #[serde(default = "default_stale_threshold_ms")]
    pub stale_threshold_ms: u64,
    /// Structural invariant: assembling a [`WorkspaceSnapshot`] never resets native sample age (`false`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub snapshot_resets_sample_age: bool,
}

impl Default for WorkspaceSamplingPolicy {
    fn default() -> Self {
        Self {
            interval_ms: WORKSPACE_SAMPLING_INTERVAL_MS,
            metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
            stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
            snapshot_resets_sample_age: false,
        }
    }
}

impl WorkspaceSamplingPolicy {
    /// Normalizes invalid zero intervals/thresholds to contract defaults and forces
    /// `snapshot_resets_sample_age` to `false`.
    pub fn normalized(&self) -> Self {
        Self {
            interval_ms: if self.interval_ms == 0 {
                WORKSPACE_SAMPLING_INTERVAL_MS
            } else {
                self.interval_ms
            },
            metadata_timeout_ms: if self.metadata_timeout_ms == 0 {
                WORKSPACE_METADATA_TIMEOUT_MS
            } else {
                self.metadata_timeout_ms
            },
            stale_threshold_ms: if self.stale_threshold_ms == 0 {
                WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
            } else {
                self.stale_threshold_ms
            },
            snapshot_resets_sample_age: false,
        }
    }
}

/// Availability state of a native sample, chat projection, or workspace sub-window.
///
/// Strictly separated from [`WorkspaceActivityState`] (`working`, `idle`, `waiting_for_approval`,
/// `waiting_for_input`, `unknown`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAvailabilityState {
    /// Source/sample is connected, verified, and within the staleness threshold.
    Available,
    /// Native parent/transport route is disconnected.
    Disconnected,
    /// Last successful sample age exceeded the staleness threshold (`> 10,000 ms` by default).
    Stale,
    /// Sample or sub-window is missing, unverified, unreadable, or unrecognized.
    #[default]
    #[serde(other)]
    Unavailable,
}

impl WorkspaceAvailabilityState {
    /// Returns the canonical `snake_case` wire string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Disconnected => "disconnected",
            Self::Stale => "stale",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Fine-grained conversational activity state for a verified native chat sample.
///
/// Kept strictly separate from [`WorkspaceAvailabilityState`] (`available`, `disconnected`,
/// `stale`, `unavailable`). When availability is not [`WorkspaceAvailabilityState::Available`],
/// effective live activity degrades to [`Self::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceActivityState {
    /// Native chat is actively executing a turn or tool call.
    Working,
    /// Native chat is connected, verified, and idle between turns.
    Idle,
    /// Conversation or bound workflow is waiting on explicit human approval.
    WaitingForApproval,
    /// Conversation or bound workflow is waiting on human question/input response.
    WaitingForInput,
    /// Conversational activity detail is unknown, unverified, or masked by non-available status.
    #[default]
    #[serde(other)]
    Unknown,
}

impl WorkspaceActivityState {
    /// Returns the canonical `snake_case` wire string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Idle => "idle",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::WaitingForInput => "waiting_for_input",
            Self::Unknown => "unknown",
        }
    }
}

/// Transport connection state reported by a [`WorkspaceNativeSample`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceConnectionState {
    /// Verified native route is connected.
    Connected,
    /// Verified native route is disconnected.
    Disconnected,
    /// Route is reachable in degraded mode (for example transient metadata timeout).
    Degraded,
    /// Connection state is missing, unverified, or unrecognized.
    #[default]
    #[serde(other)]
    Unavailable,
}

/// Sample freshness state computed relative to `last_success_at_ms` and `stale_threshold_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceFreshnessState {
    /// Sample succeeded within `stale_threshold_ms`.
    Fresh,
    /// Sample age since `last_success_at_ms` exceeded `stale_threshold_ms`.
    Stale,
    /// Producer has recorded an attempt without any successful native read yet.
    NeverSucceeded,
    /// Freshness cannot be established (missing or invalid timestamps).
    #[default]
    #[serde(other)]
    Unknown,
}

/// Tri-state capability support indicator that degrades missing wire fields to `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceCapabilityState {
    /// Capability is verified as supported on the active route.
    Supported,
    /// Capability is verified as unsupported on the active route.
    Unsupported,
    /// Capability support is unknown or omitted.
    #[default]
    #[serde(other)]
    Unknown,
}

/// Evidence classification for [`WorkspaceConfiguredSelection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceSelectionEvidence {
    /// Read from verified exact-thread native configuration metadata; never runtime execution telemetry.
    VerifiedConfiguration,
    /// Configuration selection metadata is unavailable, unverified, or stale.
    #[default]
    #[serde(other)]
    Unavailable,
}

/// Configured model and reasoning effort from verified exact-thread native metadata.
///
/// Explicitly labeled as configuration evidence (`verified_configuration`) and never counted as
/// runtime execution telemetry (`counts_as_runtime_telemetry` is always `false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceConfiguredSelection {
    /// Configured / selected conversational model token, or `None` when unavailable.
    #[serde(default)]
    pub selected_model: Option<String>,
    /// Configured / selected conversational reasoning effort token, or `None` when unavailable.
    #[serde(default)]
    pub selected_effort: Option<String>,
    /// Evidence classification (`verified_configuration` or `unavailable`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub evidence: WorkspaceSelectionEvidence,
    /// Structural invariant flag: configured selection is never runtime execution telemetry (`false`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub counts_as_runtime_telemetry: bool,
}

impl WorkspaceConfiguredSelection {
    /// Constructs an unavailable selection record with `None` model/effort and `false` telemetry flag.
    pub const fn unavailable() -> Self {
        Self {
            selected_model: None,
            selected_effort: None,
            evidence: WorkspaceSelectionEvidence::Unavailable,
            counts_as_runtime_telemetry: false,
        }
    }

    /// Constructs a verified configuration selection from sanitized native metadata tokens.
    ///
    /// If both sanitized tokens are `None`, evidence degrades to [`WorkspaceSelectionEvidence::Unavailable`].
    pub fn verified(selected_model: Option<&str>, selected_effort: Option<&str>) -> Self {
        let clean_model = sanitize_observation_selection_token(selected_model);
        let clean_effort = sanitize_observation_selection_token(selected_effort);
        let evidence = if clean_model.is_some() || clean_effort.is_some() {
            WorkspaceSelectionEvidence::VerifiedConfiguration
        } else {
            WorkspaceSelectionEvidence::Unavailable
        };
        Self {
            selected_model: clean_model,
            selected_effort: clean_effort,
            evidence,
            counts_as_runtime_telemetry: false,
        }
    }

    /// Sanitizes tokens, clears unverified selections, and enforces `counts_as_runtime_telemetry = false`.
    pub fn normalized(&self) -> Self {
        match self.evidence {
            WorkspaceSelectionEvidence::VerifiedConfiguration => Self::verified(
                self.selected_model.as_deref(),
                self.selected_effort.as_deref(),
            ),
            WorkspaceSelectionEvidence::Unavailable => Self::unavailable(),
        }
    }
}

/// Connection and freshness metadata attached to a [`WorkspaceNativeSample`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceSampleConnection {
    /// Current route connection state.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub state: WorkspaceConnectionState,
    /// Sample freshness classification.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub freshness: WorkspaceFreshnessState,
    /// Optional verified route token (such as `managed_linux_desktop` or `paired_mcp`).
    #[serde(default)]
    pub route: Option<String>,
    /// Optional sanitized diagnostic summary (`<= 240` chars).
    #[serde(default)]
    pub diagnostic: Option<String>,
}

impl WorkspaceSampleConnection {
    /// Sanitizes optional `route` and `diagnostic` fields.
    pub fn normalized(&self) -> Self {
        let route = self.route.as_deref().and_then(validate_correlation_id);
        let diagnostic = self
            .diagnostic
            .as_deref()
            .map(sanitize_workspace_summary)
            .filter(|s| !s.is_empty());
        Self {
            state: self.state,
            freshness: self.freshness,
            route,
            diagnostic,
        }
    }
}

/// Route and backend capabilities reported on a [`WorkspaceNativeSample`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceNativeCapabilities {
    /// Whether exact-thread metadata read (`thread/read`) is supported.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub exact_thread_read: WorkspaceCapabilityState,
    /// Whether fine-grained conversational activity detail is supported.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub activity_detail: WorkspaceCapabilityState,
    /// Whether configured model/effort selection metadata is supported.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub configured_selection: WorkspaceCapabilityState,
    /// Whether wake-up notice delivery is supported on the bound route.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub wakeup_delivery: WorkspaceCapabilityState,
    /// Whether per-call native parent/thread identity verification is active.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub per_call_identity: WorkspaceCapabilityState,
}

/// Point-in-time native conversation sample persisted in a bounded (`<= 64 KiB`) sidecar or
/// embedded in a [`WorkspaceSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceNativeSample {
    /// Authoritative four-part chat identity (`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub chat_identity: WorkspaceChatIdentity,
    /// Producer epoch token minted on sampler/backend startup; resets `sequence` across restarts.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub producer_epoch: String,
    /// Verified OS process instance (`pid`, `boot_id`, `start_ticks`) of the backend/sampler when known.
    #[serde(default)]
    pub backend_process_instance: Option<ObservationProcessInstance>,
    /// Monotonic sequence number within `(producer_epoch, backend_process_instance)`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub sequence: u64,
    /// Timestamp (ms since Unix epoch) when this sample observation attempt occurred.
    #[serde(default, alias = "observed_at")]
    pub observed_at_ms: Option<i64>,
    /// Timestamp (ms since Unix epoch) of the most recent successful native read.
    #[serde(default, alias = "last_success_at")]
    pub last_success_at_ms: Option<i64>,
    /// Availability state (`available`, `disconnected`, `stale`, `unavailable`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub availability: WorkspaceAvailabilityState,
    /// Fine-grained activity state (`working`, `idle`, `waiting_for_approval`, `waiting_for_input`, `unknown`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub activity: WorkspaceActivityState,
    /// Connection and freshness details.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub connection: WorkspaceSampleConnection,
    /// Route and backend capability states.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub capabilities: WorkspaceNativeCapabilities,
    /// Verified configuration selection (`selected_model` / `selected_effort`), never runtime telemetry.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub configured_selection: WorkspaceConfiguredSelection,
}

/// Evaluated live status of a [`WorkspaceNativeSample`] at a reference timestamp without mutating
/// the underlying sample timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceEvaluatedNativeSample {
    /// Age in milliseconds since `last_success_at_ms`, or `None` when never succeeded or invalid.
    pub sample_age_ms: Option<i64>,
    /// Evaluated freshness state.
    pub freshness: WorkspaceFreshnessState,
    /// Effective availability (`available` degrades to `stale` once `sample_age_ms > stale_threshold_ms`).
    pub effective_availability: WorkspaceAvailabilityState,
    /// Effective live activity (`unknown` whenever `effective_availability != Available`).
    pub effective_activity: WorkspaceActivityState,
    /// Effective configured selection (`unavailable` whenever `effective_availability != Available`).
    pub effective_configured_selection: WorkspaceConfiguredSelection,
}

fn is_valid_process_instance_shape(instance: &ObservationProcessInstance) -> bool {
    instance.pid > 0
        && !instance.boot_id.is_empty()
        && instance.boot_id.len() <= crate::MAX_LABEL_LEN
        && instance.boot_id.trim() == instance.boot_id
        && instance.boot_id.bytes().all(|b| b.is_ascii_graphic())
}

impl WorkspaceNativeSample {
    /// Validates identity, `producer_epoch`, optional `backend_process_instance`, and timestamp ordering.
    pub fn validate(&self) -> Result<(), WorkspaceContractError> {
        self.chat_identity.validate()?;
        let Some(clean_epoch) = validate_correlation_id(&self.producer_epoch) else {
            return Err(WorkspaceContractError::InvalidNativeSample(
                "producer_epoch must be a valid non-empty ASCII token up to 128 bytes".to_string(),
            ));
        };
        if clean_epoch != self.producer_epoch {
            return Err(WorkspaceContractError::InvalidNativeSample(
                "producer_epoch must not contain leading or trailing whitespace".to_string(),
            ));
        }
        if let Some(instance) = &self.backend_process_instance {
            if !is_valid_process_instance_shape(instance) {
                return Err(WorkspaceContractError::InvalidNativeSample(
                    "backend_process_instance must carry non-zero pid and graphic ASCII boot_id"
                        .to_string(),
                ));
            }
        }
        if let (Some(obs), Some(succ)) = (self.observed_at_ms, self.last_success_at_ms) {
            if succ > obs {
                return Err(WorkspaceContractError::InvalidNativeSample(
                    "last_success_at_ms must not exceed observed_at_ms".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Normalizes untrusted or partial wire fields so invalid or non-available states safely
    /// degrade to `unknown` / `unavailable` without resetting `observed_at_ms` or `last_success_at_ms`.
    pub fn normalized(&self) -> Self {
        let valid_epoch = validate_correlation_id(&self.producer_epoch)
            .filter(|clean| clean == &self.producer_epoch)
            .unwrap_or_default();
        let valid_process = self
            .backend_process_instance
            .clone()
            .filter(is_valid_process_instance_shape);
        let identity_ok = self.chat_identity.validate().is_ok();
        let timestamps_consistent = match (self.observed_at_ms, self.last_success_at_ms) {
            (Some(obs), Some(succ)) => obs > 0 && succ > 0 && succ <= obs,
            (Some(obs), None) => obs > 0,
            (None, Some(_)) => false,
            (None, None) => true,
        };

        let mut availability = self.availability;
        if !identity_ok
            || valid_epoch.is_empty()
            || !timestamps_consistent
            || (availability == WorkspaceAvailabilityState::Available
                && (self.observed_at_ms.is_none() || self.last_success_at_ms.is_none()))
        {
            availability = WorkspaceAvailabilityState::Unavailable;
        }

        let activity = if availability == WorkspaceAvailabilityState::Available {
            self.activity
        } else {
            WorkspaceActivityState::Unknown
        };

        let configured_selection = if availability == WorkspaceAvailabilityState::Available {
            self.configured_selection.normalized()
        } else {
            WorkspaceConfiguredSelection::unavailable()
        };

        let mut connection = self.connection.normalized();
        if availability == WorkspaceAvailabilityState::Disconnected {
            connection.state = WorkspaceConnectionState::Disconnected;
        } else if availability == WorkspaceAvailabilityState::Unavailable
            && connection.state == WorkspaceConnectionState::Connected
        {
            connection.state = WorkspaceConnectionState::Unavailable;
        }

        Self {
            chat_identity: self.chat_identity.clone(),
            producer_epoch: valid_epoch,
            backend_process_instance: valid_process,
            sequence: self.sequence,
            observed_at_ms: self.observed_at_ms.filter(|&v| v > 0),
            last_success_at_ms: if timestamps_consistent {
                self.last_success_at_ms.filter(|&v| v > 0)
            } else {
                None
            },
            availability,
            activity,
            connection,
            capabilities: self.capabilities.clone(),
            configured_selection,
        }
    }

    /// Evaluates sample age, staleness, availability, and live activity against `reference_now_ms`
    /// without modifying `observed_at_ms` or `last_success_at_ms`.
    pub fn evaluate_at(
        &self,
        reference_now_ms: i64,
        stale_threshold_ms: u64,
    ) -> WorkspaceEvaluatedNativeSample {
        let threshold_ms = if stale_threshold_ms == 0 {
            WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
        } else {
            stale_threshold_ms
        };
        let norm = self.normalized();
        if self.validate().is_err() || norm.producer_epoch.is_empty() {
            return WorkspaceEvaluatedNativeSample {
                sample_age_ms: None,
                freshness: WorkspaceFreshnessState::Unknown,
                effective_availability: WorkspaceAvailabilityState::Unavailable,
                effective_activity: WorkspaceActivityState::Unknown,
                effective_configured_selection: WorkspaceConfiguredSelection::unavailable(),
            };
        }

        let Some(anchor_ms) = norm.last_success_at_ms else {
            let freshness = if norm.observed_at_ms.is_some() {
                WorkspaceFreshnessState::NeverSucceeded
            } else {
                WorkspaceFreshnessState::Unknown
            };
            let effective_availability = match norm.availability {
                WorkspaceAvailabilityState::Disconnected => {
                    WorkspaceAvailabilityState::Disconnected
                }
                _ => WorkspaceAvailabilityState::Unavailable,
            };
            return WorkspaceEvaluatedNativeSample {
                sample_age_ms: None,
                freshness,
                effective_availability,
                effective_activity: WorkspaceActivityState::Unknown,
                effective_configured_selection: WorkspaceConfiguredSelection::unavailable(),
            };
        };

        let delta_ms = reference_now_ms.saturating_sub(anchor_ms);
        let max_skew_ms = i64::try_from(threshold_ms).unwrap_or(i64::MAX);
        if delta_ms < -max_skew_ms {
            return WorkspaceEvaluatedNativeSample {
                sample_age_ms: None,
                freshness: WorkspaceFreshnessState::Unknown,
                effective_availability: WorkspaceAvailabilityState::Unavailable,
                effective_activity: WorkspaceActivityState::Unknown,
                effective_configured_selection: WorkspaceConfiguredSelection::unavailable(),
            };
        }

        let clamped_age_ms = delta_ms.max(0);
        let is_stale = u64::try_from(clamped_age_ms).unwrap_or(u64::MAX) > threshold_ms;
        let freshness = if is_stale {
            WorkspaceFreshnessState::Stale
        } else {
            WorkspaceFreshnessState::Fresh
        };

        let effective_availability = match norm.availability {
            WorkspaceAvailabilityState::Available => {
                if is_stale {
                    WorkspaceAvailabilityState::Stale
                } else {
                    WorkspaceAvailabilityState::Available
                }
            }
            WorkspaceAvailabilityState::Disconnected => WorkspaceAvailabilityState::Disconnected,
            WorkspaceAvailabilityState::Stale => WorkspaceAvailabilityState::Stale,
            WorkspaceAvailabilityState::Unavailable => WorkspaceAvailabilityState::Unavailable,
        };

        let effective_activity = if effective_availability == WorkspaceAvailabilityState::Available
        {
            norm.activity
        } else {
            WorkspaceActivityState::Unknown
        };

        let effective_configured_selection =
            if effective_availability == WorkspaceAvailabilityState::Available {
                norm.configured_selection
            } else {
                WorkspaceConfiguredSelection::unavailable()
            };

        WorkspaceEvaluatedNativeSample {
            sample_age_ms: Some(clamped_age_ms),
            freshness,
            effective_availability,
            effective_activity,
            effective_configured_selection,
        }
    }
}

/// Ordering and epoch reconciliation outcome for incoming [`WorkspaceNativeSample`] updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceSampleReconcileDecision {
    /// Accepted as the first valid sample for this chat identity.
    AcceptedInitial,
    /// Accepted because `sequence` strictly increased within the same producer epoch and process instance.
    AcceptedNextSequence,
    /// Accepted because `producer_epoch` rotated with a non-older `observed_at_ms`.
    AcceptedNewEpoch,
    /// Rejected because `incoming` failed identity, epoch, process-instance, or timestamp validation.
    RejectedInvalidSample,
    /// Rejected because `incoming.chat_identity` did not match `current.chat_identity`.
    RejectedMismatchedIdentity,
    /// Rejected because `incoming.sequence == current.sequence` within the same epoch.
    RejectedDuplicateSequence,
    /// Rejected because `incoming.sequence < current.sequence` within the same epoch.
    RejectedStaleSequence,
    /// Rejected because `producer_epoch` changed to an older or missing `observed_at_ms`.
    RejectedStaleEpoch,
    /// Rejected because `backend_process_instance` changed without minting a new `producer_epoch`.
    RejectedEpochProcessMismatch,
}

impl WorkspaceSampleReconcileDecision {
    /// Returns `true` when the incoming sample should replace the current sample.
    pub const fn is_accepted(self) -> bool {
        matches!(
            self,
            Self::AcceptedInitial | Self::AcceptedNextSequence | Self::AcceptedNewEpoch
        )
    }
}

/// Evaluates whether `incoming` supersedes `current` for the same [`WorkspaceChatIdentity`].
pub fn reconcile_workspace_native_sample(
    current: Option<&WorkspaceNativeSample>,
    incoming: &WorkspaceNativeSample,
) -> WorkspaceSampleReconcileDecision {
    if incoming.validate().is_err() {
        return WorkspaceSampleReconcileDecision::RejectedInvalidSample;
    }
    let Some(current) = current else {
        return WorkspaceSampleReconcileDecision::AcceptedInitial;
    };
    if current.validate().is_err() {
        return WorkspaceSampleReconcileDecision::AcceptedInitial;
    }
    if current.chat_identity != incoming.chat_identity {
        return WorkspaceSampleReconcileDecision::RejectedMismatchedIdentity;
    }
    if current.producer_epoch == incoming.producer_epoch {
        if current.backend_process_instance != incoming.backend_process_instance {
            return WorkspaceSampleReconcileDecision::RejectedEpochProcessMismatch;
        }
        return match incoming.sequence.cmp(&current.sequence) {
            std::cmp::Ordering::Greater => WorkspaceSampleReconcileDecision::AcceptedNextSequence,
            std::cmp::Ordering::Equal => {
                WorkspaceSampleReconcileDecision::RejectedDuplicateSequence
            }
            std::cmp::Ordering::Less => WorkspaceSampleReconcileDecision::RejectedStaleSequence,
        };
    }

    let Some(incoming_obs) = incoming.observed_at_ms else {
        return WorkspaceSampleReconcileDecision::RejectedStaleEpoch;
    };
    if let Some(current_obs) = current.observed_at_ms {
        if incoming_obs < current_obs {
            return WorkspaceSampleReconcileDecision::RejectedStaleEpoch;
        }
    }
    WorkspaceSampleReconcileDecision::AcceptedNewEpoch
}

/// Validates, normalizes, and serializes a [`WorkspaceNativeSample`] sidecar payload, enforcing
/// [`MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES`] (`64 KiB`).
pub fn serialize_workspace_native_sample_sidecar(
    sample: &WorkspaceNativeSample,
) -> Result<Vec<u8>, WorkspaceContractError> {
    sample.validate()?;
    let normalized = sample.normalized();
    let bytes = serde_json::to_vec(&normalized)
        .map_err(|err| WorkspaceContractError::InvalidJson(err.to_string()))?;
    if bytes.len() > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES {
        return Err(WorkspaceContractError::SidecarTooLarge {
            actual_bytes: bytes.len(),
            max_bytes: MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
        });
    }
    Ok(bytes)
}

/// Parses and normalizes a [`WorkspaceNativeSample`] sidecar byte slice after enforcing the
/// [`MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES`] (`64 KiB`) cap.
pub fn parse_workspace_native_sample_sidecar(
    bytes: &[u8],
) -> Result<WorkspaceNativeSample, WorkspaceContractError> {
    if bytes.len() > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES {
        return Err(WorkspaceContractError::SidecarTooLarge {
            actual_bytes: bytes.len(),
            max_bytes: MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
        });
    }
    let parsed: WorkspaceNativeSample = serde_json::from_slice(bytes)
        .map_err(|err| WorkspaceContractError::InvalidJson(err.to_string()))?;
    Ok(parsed.normalized())
}

/// Deduplicates native samples per chat via [`reconcile_workspace_native_sample`] and caps the
/// retained set at [`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`] (`128`) per `source_id`.
///
/// Returns `(retained_samples, omitted_count)`.
pub fn bound_workspace_native_samples_per_source(
    samples: Vec<WorkspaceNativeSample>,
) -> (Vec<WorkspaceNativeSample>, usize) {
    let mut omitted = 0usize;
    let mut latest_by_chat: std::collections::BTreeMap<String, WorkspaceNativeSample> =
        std::collections::BTreeMap::new();

    for sample in samples {
        let Some(chat_key) = sample.chat_identity.canonical_key() else {
            omitted += 1;
            continue;
        };
        if sample.validate().is_err() {
            omitted += 1;
            continue;
        }
        let normalized = sample.normalized();
        match latest_by_chat.get(&chat_key) {
            None => {
                latest_by_chat.insert(chat_key, normalized);
            }
            Some(existing) => {
                if reconcile_workspace_native_sample(Some(existing), &normalized).is_accepted() {
                    latest_by_chat.insert(chat_key, normalized);
                }
            }
        }
    }

    let mut by_source: std::collections::BTreeMap<String, Vec<(String, WorkspaceNativeSample)>> =
        std::collections::BTreeMap::new();
    for (chat_key, sample) in latest_by_chat {
        by_source
            .entry(sample.chat_identity.source_id.clone())
            .or_default()
            .push((chat_key, sample));
    }

    let mut retained = Vec::new();
    for (_source_id, mut group) in by_source {
        group.sort_by(|(left_key, left), (right_key, right)| {
            right
                .observed_at_ms
                .unwrap_or(i64::MIN)
                .cmp(&left.observed_at_ms.unwrap_or(i64::MIN))
                .then_with(|| right.sequence.cmp(&left.sequence))
                .then_with(|| left_key.cmp(right_key))
        });
        if group.len() > MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE {
            omitted += group.len() - MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE;
            group.truncate(MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE);
        }
        for (_key, sample) in group {
            retained.push(sample);
        }
    }

    (retained, omitted)
}

/// Summary projection of a canonical conversation in [`WorkspaceSnapshot::chats`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceChatSummary {
    /// Canonical four-part chat identity (`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub identity: WorkspaceChatIdentity,
    /// Display-only sanitized chat title (`<= 128` bytes); never part of chat identity.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub thread_label: String,
    /// Display-only sanitized project label (`<= 128` bytes); never part of chat identity.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub project_label: String,
    /// Evaluated availability state (`available`, `disconnected`, `stale`, `unavailable`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub availability: WorkspaceAvailabilityState,
    /// Evaluated live activity state (`working`, `idle`, `waiting_for_approval`, `waiting_for_input`, `unknown`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub activity: WorkspaceActivityState,
    /// Verified configured model/effort selection, or `unavailable`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub configured_selection: WorkspaceConfiguredSelection,
    /// Count of active jobs associated with this chat.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub active_job_count: usize,
    /// Count of jobs/tasks currently requiring attention on this chat.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub attention_count: usize,
    /// Most recent observed update timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
}

impl WorkspaceChatSummary {
    /// Sanitizes labels and enforces availability/activity/selection separation invariants.
    pub fn normalized(&self) -> Self {
        let identity_ok = self.identity.validate().is_ok();
        let availability = if identity_ok {
            self.availability
        } else {
            WorkspaceAvailabilityState::Unavailable
        };
        let activity = if availability == WorkspaceAvailabilityState::Available {
            self.activity
        } else {
            WorkspaceActivityState::Unknown
        };
        let configured_selection = if availability == WorkspaceAvailabilityState::Available {
            self.configured_selection.normalized()
        } else {
            WorkspaceConfiguredSelection::unavailable()
        };
        Self {
            identity: self.identity.clone(),
            thread_label: sanitize_observation_label(&self.thread_label, "Codex chat"),
            project_label: sanitize_observation_label(&self.project_label, "Project"),
            availability,
            activity,
            configured_selection,
            active_job_count: self.active_job_count,
            attention_count: self.attention_count,
            updated_at_ms: self.updated_at_ms.filter(|&v| v > 0),
        }
    }
}

/// Verification status of a [`WorkspaceAcceptanceProof`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAcceptanceStatus {
    /// Verified by structured review/closure receipts; never inferred from worker exit status.
    VerifiedAccepted,
    /// Candidate produced and awaiting review or closure verification.
    PendingReview,
    /// Candidate was explicitly rejected or requested changes.
    Rejected,
    /// No accepted candidate exists yet.
    NotAccepted,
    /// Acceptance state is missing, degraded, or lacks required receipt proof.
    #[default]
    #[serde(other)]
    Unknown,
}

/// Structured acceptance proof kept strictly separate from worker completion and job result prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceAcceptanceProof {
    /// Current acceptance verification status.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub status: WorkspaceAcceptanceStatus,
    /// Accepted task-level candidate identifier when applicable.
    #[serde(default)]
    pub accepted_candidate_id: Option<String>,
    /// Integrated job-level combined candidate identifier when applicable.
    #[serde(default)]
    pub combined_candidate_id: Option<String>,
    /// Structured Reviewer verdict identifier when recorded.
    #[serde(default)]
    pub review_verdict_id: Option<String>,
    /// Durable Reviewer receipt identifier when recorded.
    #[serde(default)]
    pub review_receipt_id: Option<String>,
    /// Durable integration receipt identifier when recorded.
    #[serde(default)]
    pub integration_receipt_id: Option<String>,
    /// Single-Closure reservation identifier when recorded.
    #[serde(default)]
    pub closure_reservation_id: Option<String>,
    /// Accepted Closure verdict identifier required for job-level verified acceptance.
    #[serde(default)]
    pub closure_verdict_id: Option<String>,
    /// Timestamp (ms since Unix epoch) when acceptance proof was durably recorded.
    #[serde(default)]
    pub verified_at_ms: Option<i64>,
}

impl WorkspaceAcceptanceProof {
    /// Returns an `unknown` acceptance proof with no receipt identifiers.
    pub const fn unknown() -> Self {
        Self {
            status: WorkspaceAcceptanceStatus::Unknown,
            accepted_candidate_id: None,
            combined_candidate_id: None,
            review_verdict_id: None,
            review_receipt_id: None,
            integration_receipt_id: None,
            closure_reservation_id: None,
            closure_verdict_id: None,
            verified_at_ms: None,
        }
    }

    fn cleaned_ids(&self) -> Self {
        Self {
            status: self.status,
            accepted_candidate_id: clean_opt_id(self.accepted_candidate_id.as_deref()),
            combined_candidate_id: clean_opt_id(self.combined_candidate_id.as_deref()),
            review_verdict_id: clean_opt_id(self.review_verdict_id.as_deref()),
            review_receipt_id: clean_opt_id(self.review_receipt_id.as_deref()),
            integration_receipt_id: clean_opt_id(self.integration_receipt_id.as_deref()),
            closure_reservation_id: clean_opt_id(self.closure_reservation_id.as_deref()),
            closure_verdict_id: clean_opt_id(self.closure_verdict_id.as_deref()),
            verified_at_ms: self.verified_at_ms.filter(|&v| v > 0),
        }
    }

    /// Normalizes a task-level acceptance proof: `verified_accepted` requires a valid
    /// `accepted_candidate_id` plus at least one of `review_verdict_id` or `review_receipt_id`.
    pub fn normalized_for_task(&self) -> Self {
        let mut out = self.cleaned_ids();
        if out.status == WorkspaceAcceptanceStatus::VerifiedAccepted {
            let has_candidate = out.accepted_candidate_id.is_some();
            let has_review_proof =
                out.review_verdict_id.is_some() || out.review_receipt_id.is_some();
            if !(has_candidate && has_review_proof) {
                out.status = WorkspaceAcceptanceStatus::Unknown;
            }
        }
        out
    }

    /// Normalizes a job-level acceptance proof: `verified_accepted` requires both a valid
    /// `combined_candidate_id` and an accepted `closure_verdict_id`.
    pub fn normalized_for_job(&self) -> Self {
        let mut out = self.cleaned_ids();
        if out.status == WorkspaceAcceptanceStatus::VerifiedAccepted
            && (out.combined_candidate_id.is_none() || out.closure_verdict_id.is_none())
        {
            out.status = WorkspaceAcceptanceStatus::Unknown;
        }
        out
    }
}

/// Historical failure record kept separate from live [`WorkspaceAttentionState`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceHistoricalFailure {
    /// Optional durable failure record identifier.
    #[serde(default)]
    pub failure_id: Option<String>,
    /// Lifecycle stage where the failure occurred (`worker_execution`, `verification`, `review`, `integration`, `closure`, `dispatch`, or `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub stage: String,
    /// Timestamp (ms since Unix epoch) when the failure was recorded.
    #[serde(default)]
    pub failed_at_ms: Option<i64>,
    /// Associated `task_id` when task-scoped.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Associated 1-based task attempt number when applicable.
    #[serde(default)]
    pub attempt_no: Option<u32>,
    /// Associated core `run_id` when applicable.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Associated `outbox_id` when applicable.
    #[serde(default)]
    pub outbox_id: Option<String>,
    /// Sanitized failure summary (`<= 240` chars).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub reason: String,
    /// Whether a subsequent retry, revision, or operator action moved past this failure while preserving its audit trail.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub superseded_or_resolved: bool,
}

impl WorkspaceHistoricalFailure {
    /// Sanitizes identifiers and bounds `reason` to [`MAX_OBSERVATION_SUMMARY_CHARS`].
    pub fn normalized(&self) -> Self {
        let clean_reason = sanitize_workspace_summary(&self.reason);
        Self {
            failure_id: clean_opt_id(self.failure_id.as_deref()),
            stage: sanitize_status_token(Some(&self.stage)),
            failed_at_ms: self.failed_at_ms.filter(|&v| v > 0),
            task_id: clean_opt_id(self.task_id.as_deref()),
            attempt_no: self.attempt_no,
            run_id: clean_opt_id(self.run_id.as_deref()),
            outbox_id: clean_opt_id(self.outbox_id.as_deref()),
            reason: if clean_reason.is_empty() {
                "Historical failure recorded".to_string()
            } else {
                clean_reason
            },
            superseded_or_resolved: self.superseded_or_resolved,
        }
    }
}

/// Classification of current live operator or coworker attention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAttentionKind {
    /// Waiting on explicit human plan/amendment approval.
    NeedsApproval,
    /// Waiting on human answer to a pending question.
    NeedsUserInput,
    /// Paused behind an active hold boundary.
    OnHold,
    /// Blocked on an unresolved outbox or interrupted effect requiring receipt reconciliation.
    UnresolvedEffect,
    /// Settled failure fence awaiting correction/replan decision.
    BlockedFailure,
    /// Candidate ready and awaiting review action.
    ReviewRequired,
    /// Explicitly no live attention needed.
    None,
    /// Attention state is unknown or unrecognized.
    #[default]
    #[serde(other)]
    Unknown,
}

/// Live attention state kept strictly separate from [`WorkspaceHistoricalFailure`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceAttentionState {
    /// Current attention category.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub kind: WorkspaceAttentionKind,
    /// Sanitized reason summary (`<= 240` chars).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub reason: String,
    /// Optional safe next-step guidance (`<= 240` chars).
    #[serde(default)]
    pub next_step: Option<String>,
    /// Active hold identifiers blocking advancement.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub blocking_hold_ids: Vec<String>,
    /// Unresolved outbox identifiers requiring reconciliation.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub blocking_outbox_ids: Vec<String>,
    /// Active question identifier when waiting on input.
    #[serde(default)]
    pub question_id: Option<String>,
    /// Timestamp (ms since Unix epoch) of the latest attention transition.
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
}

impl WorkspaceAttentionState {
    /// Sanitizes summaries and bounds blocking ID lists.
    pub fn normalized(&self) -> Self {
        Self {
            kind: self.kind,
            reason: sanitize_workspace_summary(&self.reason),
            next_step: clean_opt_summary(self.next_step.as_deref()),
            blocking_hold_ids: clean_id_list(
                &self.blocking_hold_ids,
                MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
            ),
            blocking_outbox_ids: clean_id_list(
                &self.blocking_outbox_ids,
                MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
            ),
            question_id: clean_opt_id(self.question_id.as_deref()),
            updated_at_ms: self.updated_at_ms.filter(|&v| v > 0),
        }
    }
}

/// Single task execution attempt projected under [`WorkspaceTaskHistory::attempts`] (`<= 16` per task).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceTaskAttempt {
    /// 1-based attempt number within the task.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub attempt_no: u32,
    /// Attempt status token (`queued`, `running`, `completed`, `failed`, `cancelled`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub status: String,
    /// Correlated core `run_id` when dispatched.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Correlated durable `worker_id` when dispatched.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Correlated `outbox_id` reservation for this attempt.
    #[serde(default)]
    pub outbox_id: Option<String>,
    /// Requested vs. runtime-observed executor telemetry for this attempt.
    #[serde(default)]
    pub executor: Option<ObservationExecutor>,
    /// Start timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub started_at_ms: Option<i64>,
    /// Finish timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub finished_at_ms: Option<i64>,
    /// Sanitized attempt summary (`<= 240` chars).
    #[serde(default)]
    pub summary: Option<String>,
}

impl WorkspaceTaskAttempt {
    /// Sanitizes attempt identifiers and summary text.
    pub fn normalized(&self) -> Self {
        Self {
            attempt_no: self.attempt_no,
            status: sanitize_status_token(Some(&self.status)),
            run_id: clean_opt_id(self.run_id.as_deref()),
            worker_id: clean_opt_id(self.worker_id.as_deref()),
            outbox_id: clean_opt_id(self.outbox_id.as_deref()),
            executor: self.executor.clone(),
            started_at_ms: self.started_at_ms.filter(|&v| v > 0),
            finished_at_ms: self.finished_at_ms.filter(|&v| v > 0),
            summary: clean_opt_summary(self.summary.as_deref()),
        }
    }
}

/// Task output candidate projected under [`WorkspaceTaskHistory::candidates`] (`<= 16` per task).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceTaskCandidate {
    /// Stable candidate identifier (`cand_...`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub candidate_id: String,
    /// Producing attempt number.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub attempt_no: u32,
    /// Producing durable `worker_id` when applicable.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Git commit SHA of the candidate worktree head when recorded.
    #[serde(default)]
    pub head_commit_sha: Option<String>,
    /// Git tree SHA of the candidate output when recorded.
    #[serde(default)]
    pub tree_sha: Option<String>,
    /// Whether automated verification gates passed (`None` when not yet run or unknown).
    #[serde(default)]
    pub verification_passed: Option<bool>,
    /// Review status token (`pending`, `accepted`, `changes_requested`, `invalidated`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub review_status: String,
    /// Linked Reviewer verdict identifier when reviewed.
    #[serde(default)]
    pub review_verdict_id: Option<String>,
    /// Linked Reviewer receipt identifier when reviewed.
    #[serde(default)]
    pub review_receipt_id: Option<String>,
    /// Timestamp (ms since Unix epoch) when the candidate was sealed.
    #[serde(default)]
    pub produced_at_ms: Option<i64>,
    /// Sanitized candidate summary (`<= 240` chars).
    #[serde(default)]
    pub summary: Option<String>,
}

impl WorkspaceTaskCandidate {
    /// Sanitizes candidate identifiers and summary text.
    pub fn normalized(&self) -> Self {
        Self {
            candidate_id: validate_correlation_id(&self.candidate_id)
                .unwrap_or_else(default_unknown_token),
            attempt_no: self.attempt_no,
            worker_id: clean_opt_id(self.worker_id.as_deref()),
            head_commit_sha: clean_opt_id(self.head_commit_sha.as_deref()),
            tree_sha: clean_opt_id(self.tree_sha.as_deref()),
            verification_passed: self.verification_passed,
            review_status: sanitize_status_token(Some(&self.review_status)),
            review_verdict_id: clean_opt_id(self.review_verdict_id.as_deref()),
            review_receipt_id: clean_opt_id(self.review_receipt_id.as_deref()),
            produced_at_ms: self.produced_at_ms.filter(|&v| v > 0),
            summary: clean_opt_summary(self.summary.as_deref()),
        }
    }
}

/// Structured task history entry inside [`WorkspaceJobHistory::tasks`] (`<= 64` per job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceTaskHistory {
    /// Stable task identifier within the job.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub task_id: String,
    /// Sanitized task title (`<= 128` bytes).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub title: String,
    /// Assigned workflow role (`coder`, `reviewer`, etc.).
    #[serde(default)]
    pub role: Option<String>,
    /// Declared task difficulty (`easy`, `medium`, `hard`).
    #[serde(default)]
    pub difficulty: Option<String>,
    /// Current task lifecycle stage (`pending`, `coding`, `verifying`, `reviewing`, `accepted`, `blocked`, `unknown`).
    #[serde(default)]
    pub stage: Option<String>,
    /// Truthful task status (`pending`, `running`, `ready_for_review`, `completed`, `accepted`, `failed`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub status: String,
    /// Upstream task IDs that this task depends on (`<= 64`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub depends_on: Vec<String>,
    /// Current 1-based attempt number (`0` before any attempt starts), kept separate from `attempts`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub current_attempt_no: u32,
    /// Bounded attempt history (`<= 16` attempts/task).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub attempts: Vec<WorkspaceTaskAttempt>,
    /// Bounded candidate history (`<= 16` candidates/task).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub candidates: Vec<WorkspaceTaskCandidate>,
    /// Active candidate awaiting verification/review when applicable.
    #[serde(default)]
    pub active_candidate_id: Option<String>,
    /// Accepted candidate identifier when verified.
    #[serde(default)]
    pub accepted_candidate_id: Option<String>,
    /// Historical task failure audit record (distinct from `current_attention`).
    #[serde(default)]
    pub historical_failure: Option<WorkspaceHistoricalFailure>,
    /// Current live task attention state (distinct from `historical_failure`).
    #[serde(default)]
    pub current_attention: Option<WorkspaceAttentionState>,
    /// Verified task acceptance proof (distinct from worker completion).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub acceptance_proof: WorkspaceAcceptanceProof,
}

impl WorkspaceTaskHistory {
    fn normalized_with_omissions(&self) -> (Self, usize, usize) {
        let all_attempts = self
            .attempts
            .iter()
            .map(WorkspaceTaskAttempt::normalized)
            .collect();
        let (attempts, attempts_omitted) = cap_tail(all_attempts, MAX_WORKSPACE_ATTEMPTS_PER_TASK);

        let all_candidates = self
            .candidates
            .iter()
            .map(WorkspaceTaskCandidate::normalized)
            .collect();
        let (candidates, candidates_omitted) =
            cap_tail(all_candidates, MAX_WORKSPACE_CANDIDATES_PER_TASK);

        let normalized = Self {
            task_id: validate_correlation_id(&self.task_id).unwrap_or_else(default_unknown_token),
            title: sanitize_observation_label(&self.title, "Task"),
            role: clean_opt_id(self.role.as_deref()),
            difficulty: clean_opt_id(self.difficulty.as_deref()),
            stage: self
                .stage
                .as_deref()
                .map(|s| sanitize_status_token(Some(s))),
            status: sanitize_status_token(Some(&self.status)),
            depends_on: clean_id_list(&self.depends_on, MAX_WORKSPACE_TASKS_PER_JOB),
            current_attempt_no: self.current_attempt_no,
            attempts,
            candidates,
            active_candidate_id: clean_opt_id(self.active_candidate_id.as_deref()),
            accepted_candidate_id: clean_opt_id(self.accepted_candidate_id.as_deref()),
            historical_failure: self
                .historical_failure
                .as_ref()
                .map(WorkspaceHistoricalFailure::normalized),
            current_attention: self
                .current_attention
                .as_ref()
                .map(WorkspaceAttentionState::normalized),
            acceptance_proof: self.acceptance_proof.normalized_for_task(),
        };
        (normalized, attempts_omitted, candidates_omitted)
    }
}

/// Core/worker run reference projected under [`WorkspaceJobHistory::runs`] (`<= 256` per job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceRunRecord {
    /// Core `run_id`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub run_id: String,
    /// Optional durable `worker_id` associated with this run.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Owning software `binding_id`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub binding_id: String,
    /// Correlated `task_id` when bound to a job task.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Correlated 1-based task attempt number when applicable.
    #[serde(default)]
    pub attempt_no: Option<u32>,
    /// Assigned workflow role (`coder`, `reviewer`, `architect`, `orchestrator`, `closure`).
    #[serde(default)]
    pub role: Option<String>,
    /// Truthful run/worker status (`queued`, `claimed`, `running`, `completed`, `failed`, `cancelled`, `timed_out`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub status: String,
    /// Requested vs. runtime-observed executor metadata.
    #[serde(default)]
    pub executor: Option<ObservationExecutor>,
    /// Run start timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub started_at_ms: Option<i64>,
    /// Run finish timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub finished_at_ms: Option<i64>,
    /// Latest run update timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
}

impl WorkspaceRunRecord {
    /// Sanitizes run identifiers and status token.
    pub fn normalized(&self) -> Self {
        Self {
            run_id: validate_correlation_id(&self.run_id).unwrap_or_else(default_unknown_token),
            worker_id: self.worker_id.as_deref().and_then(validate_correlation_id),
            binding_id: validate_correlation_id(&self.binding_id)
                .unwrap_or_else(default_unknown_token),
            task_id: self.task_id.as_deref().and_then(validate_correlation_id),
            attempt_no: self.attempt_no,
            role: self.role.as_deref().and_then(validate_correlation_id),
            status: sanitize_status_token(Some(&self.status)),
            executor: self.executor.clone(),
            started_at_ms: self.started_at_ms.filter(|&v| v > 0),
            finished_at_ms: self.finished_at_ms.filter(|&v| v > 0),
            updated_at_ms: self.updated_at_ms.filter(|&v| v > 0),
        }
    }
}

/// Durable outbox reservation/dispatch entry projected under [`WorkspaceJobHistory::outbox`] (`<= 256` per job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceOutboxRecord {
    /// Durable outbox record identifier (`outbox_...`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub outbox_id: String,
    /// Idempotent request/reservation key when recorded.
    #[serde(default)]
    pub request_key: Option<String>,
    /// Outbox effect kind (`coder_dispatch`, `reviewer_dispatch`, `closure_dispatch`, `notification`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub kind: String,
    /// Outbox lifecycle status (`reserved`, `dispatched`, `completed`, `failed`, `reconciled`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub status: String,
    /// Target `task_id` when task-scoped.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Target 1-based task attempt number when applicable.
    #[serde(default)]
    pub attempt_no: Option<u32>,
    /// Target workflow role when role-scoped.
    #[serde(default)]
    pub role: Option<String>,
    /// Linked `run_id` when dispatched.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Linked `worker_id` when dispatched.
    #[serde(default)]
    pub worker_id: Option<String>,
    /// Durable completion/reconciliation receipt ID when settled.
    #[serde(default)]
    pub receipt_id: Option<String>,
    /// Job revision at which the effect was reserved.
    #[serde(default)]
    pub reserved_at_job_revision: Option<u64>,
    /// Latest job revision that updated this outbox record.
    #[serde(default)]
    pub updated_at_job_revision: Option<u64>,
    /// Sanitized diagnostic summary (`<= 240` chars).
    #[serde(default)]
    pub diagnostic: Option<String>,
}

impl WorkspaceOutboxRecord {
    /// Sanitizes outbox identifiers and diagnostic text.
    pub fn normalized(&self) -> Self {
        Self {
            outbox_id: validate_correlation_id(&self.outbox_id)
                .unwrap_or_else(default_unknown_token),
            request_key: clean_opt_id(self.request_key.as_deref()),
            kind: sanitize_status_token(Some(&self.kind)),
            status: sanitize_status_token(Some(&self.status)),
            task_id: clean_opt_id(self.task_id.as_deref()),
            attempt_no: self.attempt_no,
            role: clean_opt_id(self.role.as_deref()),
            run_id: clean_opt_id(self.run_id.as_deref()),
            worker_id: clean_opt_id(self.worker_id.as_deref()),
            receipt_id: clean_opt_id(self.receipt_id.as_deref()),
            reserved_at_job_revision: self.reserved_at_job_revision,
            updated_at_job_revision: self.updated_at_job_revision,
            diagnostic: clean_opt_summary(self.diagnostic.as_deref()),
        }
    }
}

/// Combined integration candidate projected under [`WorkspaceJobHistory::combined_candidates`] (`<= 32` per job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceCombinedCandidate {
    /// Combined candidate identifier (`ccand_...`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub combined_candidate_id: String,
    /// Job revision under which integration ran.
    #[serde(default)]
    pub job_revision: Option<u64>,
    /// Combined candidate status (`pending_checks`, `ready_for_closure`, `accepted`, `failed`, `invalidated`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub status: String,
    /// Final integrated commit SHA when recorded.
    #[serde(default)]
    pub final_commit_sha: Option<String>,
    /// Final integrated tree SHA when recorded.
    #[serde(default)]
    pub final_tree_sha: Option<String>,
    /// Digest over integrated task candidates when recorded.
    #[serde(default)]
    pub candidate_digest: Option<String>,
    /// Reserved single-Closure identifier bound to this candidate.
    #[serde(default)]
    pub closure_reservation_id: Option<String>,
    /// Count of integrated validation checks recorded on this candidate.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub check_count: usize,
    /// Timestamp (ms since Unix epoch) when integrated.
    #[serde(default)]
    pub produced_at_ms: Option<i64>,
}

impl WorkspaceCombinedCandidate {
    /// Sanitizes combined candidate identifiers and status token.
    pub fn normalized(&self) -> Self {
        Self {
            combined_candidate_id: validate_correlation_id(&self.combined_candidate_id)
                .unwrap_or_else(default_unknown_token),
            job_revision: self.job_revision,
            status: sanitize_status_token(Some(&self.status)),
            final_commit_sha: clean_opt_id(self.final_commit_sha.as_deref()),
            final_tree_sha: clean_opt_id(self.final_tree_sha.as_deref()),
            candidate_digest: clean_opt_id(self.candidate_digest.as_deref()),
            closure_reservation_id: clean_opt_id(self.closure_reservation_id.as_deref()),
            check_count: self.check_count,
            produced_at_ms: self.produced_at_ms.filter(|&v| v > 0),
        }
    }
}

/// Safe job result summary kept separate from [`WorkspaceAcceptanceProof`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceJobResult {
    /// Outcome label (`completed`, `failed`, `stopped`, `ready_for_review`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub outcome: String,
    /// Sanitized result summary (`<= 240` chars).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub summary: String,
    /// Timestamp (ms since Unix epoch) when the result summary was produced.
    #[serde(default)]
    pub produced_at_ms: Option<i64>,
    /// Delivery/ACK stage of the result (submission/ACK never proves human reading).
    #[serde(
        default = "default_observation_delivery_stage_unknown",
        deserialize_with = "deserialize_workspace_delivery_stage"
    )]
    pub delivery_stage: ObservationDeliveryStage,
    /// Optional Closure verdict identifier that produced the final summary.
    #[serde(default)]
    pub closure_verdict_id: Option<String>,
}

impl Default for WorkspaceJobResult {
    fn default() -> Self {
        Self {
            outcome: default_unknown_token(),
            summary: String::new(),
            produced_at_ms: None,
            delivery_stage: ObservationDeliveryStage::Unknown,
            closure_verdict_id: None,
        }
    }
}

impl WorkspaceJobResult {
    /// Sanitizes outcome, summary, and optional `closure_verdict_id`.
    pub fn normalized(&self) -> Self {
        Self {
            outcome: sanitize_status_token(Some(&self.outcome)),
            summary: sanitize_workspace_summary(&self.summary),
            produced_at_ms: self.produced_at_ms.filter(|&v| v > 0),
            delivery_stage: self.delivery_stage,
            closure_verdict_id: clean_opt_id(self.closure_verdict_id.as_deref()),
        }
    }
}

/// Follow-up lineage and upstream source record links for a [`WorkspaceJobHistory`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceRecordLinks {
    /// Source `plan_id` from which this job was approved.
    #[serde(default)]
    pub source_plan_id: Option<String>,
    /// Source `plan_revision` frozen by the approval.
    #[serde(default)]
    pub source_plan_revision: Option<u64>,
    /// Exact `approval_id` authorizing execution of this job.
    #[serde(default)]
    pub source_approval_id: Option<String>,
    /// Upstream `work_brief_id` when planned from a structured work brief.
    #[serde(default)]
    pub source_work_brief_id: Option<String>,
    /// Standing `planning_grant_id` used during planning, if any.
    #[serde(default)]
    pub planning_grant_id: Option<String>,
    /// Planning reservation identifier, if any.
    #[serde(default)]
    pub planning_reservation_id: Option<String>,
    /// Prior `job_id` superseded by this job revision/replan.
    #[serde(default)]
    pub supersedes_job_id: Option<String>,
    /// Newer follow-up `job_id` that superseded this job.
    #[serde(default)]
    pub superseded_by_job_id: Option<String>,
    /// Optional parent `job_id` for decomposed follow-up work.
    #[serde(default)]
    pub parent_job_id: Option<String>,
    /// Follow-up `job_id`s linked forward from this job (`<= 64`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub follow_up_job_ids: Vec<String>,
    /// Bounded Architect advice record IDs consulted during planning (`<= 64`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub architect_advice_ids: Vec<String>,
    /// Correlated [`ObservationRequest`] IDs linked to this job (`<= 64`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_request_ids: Vec<String>,
}

impl WorkspaceRecordLinks {
    /// Sanitizes and bounds all link identifiers.
    pub fn normalized(&self) -> Self {
        let cap = MAX_WORKSPACE_RECORD_LINKS_PER_LIST;
        Self {
            source_plan_id: clean_opt_id(self.source_plan_id.as_deref()),
            source_plan_revision: self.source_plan_revision,
            source_approval_id: clean_opt_id(self.source_approval_id.as_deref()),
            source_work_brief_id: clean_opt_id(self.source_work_brief_id.as_deref()),
            planning_grant_id: clean_opt_id(self.planning_grant_id.as_deref()),
            planning_reservation_id: clean_opt_id(self.planning_reservation_id.as_deref()),
            supersedes_job_id: clean_opt_id(self.supersedes_job_id.as_deref()),
            superseded_by_job_id: clean_opt_id(self.superseded_by_job_id.as_deref()),
            parent_job_id: clean_opt_id(self.parent_job_id.as_deref()),
            follow_up_job_ids: clean_id_list(&self.follow_up_job_ids, cap),
            architect_advice_ids: clean_id_list(&self.architect_advice_ids, cap),
            source_request_ids: clean_id_list(&self.source_request_ids, cap),
        }
    }
}

/// Header summary of a job inside [`WorkspaceJobHistory::job`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceJobSummary {
    /// Observation source identifier scoping `job_id`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_id: String,
    /// Durable job identifier within `source_id`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub job_id: String,
    /// Scoped software `binding_id` when bound.
    #[serde(default)]
    pub binding_id: Option<String>,
    /// Canonical four-part chat identity when all verified components exist; never synthesized from `title`, `sessionId`, or `job_id`.
    #[serde(default)]
    pub chat_identity: Option<WorkspaceChatIdentity>,
    /// Sanitized human-readable job title (`<= 128` bytes).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub title: String,
    /// Monotonic job ledger revision number when backed by the coworker job ledger.
    #[serde(default)]
    pub job_revision: Option<u64>,
    /// High-level job lifecycle state (`unknown` when missing/unrecognized).
    #[serde(
        default = "default_observation_job_state_unknown",
        deserialize_with = "deserialize_workspace_job_state"
    )]
    pub state: ObservationJobState,
    /// Optional fine-grained ledger phase (`executing`, `integrating`, `closing`, `completed`, `stopped`, `failed`).
    #[serde(default)]
    pub phase: Option<String>,
    /// Approval status token (`approved`, `pending_approval`, `superseded`, `unknown`, etc.).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub approval_status: String,
    /// Execution status token (`not_started`, `running`, `ready_for_review`, `completed`, `accepted`, `failed`, `stopped`, `unknown`).
    #[serde(
        default = "default_unknown_token",
        deserialize_with = "deserialize_non_empty_or_unknown"
    )]
    pub execution_status: String,
    /// Review state token when applicable.
    #[serde(default)]
    pub review_state: Option<String>,
    /// Job creation timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub created_at_ms: Option<i64>,
    /// Latest job update timestamp in milliseconds since Unix epoch.
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
    /// Sanitized job summary (`<= 240` chars).
    #[serde(default)]
    pub summary: Option<String>,
}

impl Default for WorkspaceJobSummary {
    fn default() -> Self {
        Self {
            source_id: default_unknown_token(),
            job_id: default_unknown_token(),
            binding_id: None,
            chat_identity: None,
            title: "Job".to_string(),
            job_revision: None,
            state: ObservationJobState::Unknown,
            phase: None,
            approval_status: default_unknown_token(),
            execution_status: default_unknown_token(),
            review_state: None,
            created_at_ms: None,
            updated_at_ms: None,
            summary: None,
        }
    }
}

impl WorkspaceJobSummary {
    /// Sanitizes identifiers, drops invalid `chat_identity`, and bounds `title` / `summary`.
    pub fn normalized(&self) -> Self {
        let valid_source = validate_workspace_source_id(&self.source_id)
            .unwrap_or_else(|_| default_unknown_token());
        let valid_job_id =
            validate_correlation_id(&self.job_id).unwrap_or_else(default_unknown_token);
        let valid_binding = self
            .binding_id
            .as_deref()
            .and_then(|b| validate_workspace_binding_id(b).ok());
        let valid_chat = self
            .chat_identity
            .clone()
            .filter(|id| id.validate().is_ok() && id.source_id == valid_source);
        Self {
            source_id: valid_source,
            job_id: valid_job_id,
            binding_id: valid_binding,
            chat_identity: valid_chat,
            title: sanitize_observation_label(&self.title, "Job"),
            job_revision: self.job_revision,
            state: self.state,
            phase: self
                .phase
                .as_deref()
                .map(|p| sanitize_status_token(Some(p))),
            approval_status: sanitize_status_token(Some(&self.approval_status)),
            execution_status: sanitize_status_token(Some(&self.execution_status)),
            review_state: self
                .review_state
                .as_deref()
                .map(|r| sanitize_status_token(Some(r))),
            created_at_ms: self.created_at_ms.filter(|&v| v > 0),
            updated_at_ms: self.updated_at_ms.filter(|&v| v > 0),
            summary: self
                .summary
                .as_deref()
                .map(sanitize_workspace_summary)
                .filter(|s| !s.is_empty()),
        }
    }
}

/// Explicit per-collection omission accounting when [`WorkspaceJobHistory`] hits ledger bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceJobHistoryTruncation {
    /// Tasks omitted beyond [`MAX_WORKSPACE_TASKS_PER_JOB`] (`64`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tasks_omitted: usize,
    /// Task attempts omitted across tasks beyond [`MAX_WORKSPACE_ATTEMPTS_PER_TASK`] (`16`/task).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub attempts_omitted: usize,
    /// Task candidates omitted across tasks beyond [`MAX_WORKSPACE_CANDIDATES_PER_TASK`] (`16`/task).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub candidates_omitted: usize,
    /// Runs omitted beyond [`MAX_WORKSPACE_RUNS_PER_JOB`] (`256`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub runs_omitted: usize,
    /// Outbox records omitted beyond [`MAX_WORKSPACE_OUTBOX_PER_JOB`] (`256`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub outbox_omitted: usize,
    /// Combined candidates omitted beyond [`MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB`] (`32`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub combined_candidates_omitted: usize,
}

/// Complete bounded job history projection (`<= 64` jobs/binding, `<= 500` total) with separated
/// `job`, `tasks` (`current_attempt_no`, `attempts`, `candidates`), `runs`, `outbox`,
/// `combined_candidates`, `result`, `links`, `historical_failure`, `current_attention`, and
/// verified `acceptance_proof`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceJobHistory {
    /// Job header summary.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub job: WorkspaceJobSummary,
    /// Bounded task projections (`<= 64` tasks/job).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tasks: Vec<WorkspaceTaskHistory>,
    /// Bounded core/worker run references (`<= 256` runs/job).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub runs: Vec<WorkspaceRunRecord>,
    /// Bounded durable outbox records (`<= 256` outbox records/job).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub outbox: Vec<WorkspaceOutboxRecord>,
    /// Bounded combined integration candidates (`<= 32` combined candidates/job).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub combined_candidates: Vec<WorkspaceCombinedCandidate>,
    /// Safe job result summary (distinct from `acceptance_proof`).
    #[serde(default)]
    pub result: Option<WorkspaceJobResult>,
    /// Follow-up lineage and source record links.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub links: WorkspaceRecordLinks,
    /// Historical job-level failure record (distinct from `current_attention`).
    #[serde(default)]
    pub historical_failure: Option<WorkspaceHistoricalFailure>,
    /// Current live job-level attention state (distinct from `historical_failure`).
    #[serde(default)]
    pub current_attention: Option<WorkspaceAttentionState>,
    /// Verified job-level acceptance proof (requires `combined_candidate_id` + `closure_verdict_id`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub acceptance_proof: WorkspaceAcceptanceProof,
    /// Omission counts recorded when capping child collections.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub truncation: WorkspaceJobHistoryTruncation,
}

impl WorkspaceJobHistory {
    /// Enforces all ledger-aligned bounds (`64` tasks/job, `16` attempts/task, `16` candidates/task,
    /// `256` runs/job, `256` outbox/job, `32` combined candidates/job), sanitizes summaries, and
    /// validates acceptance proofs.
    pub fn normalized(&self) -> Self {
        let tasks_omitted_extra = self.tasks.len().saturating_sub(MAX_WORKSPACE_TASKS_PER_JOB);
        let mut tasks = Vec::with_capacity(self.tasks.len().min(MAX_WORKSPACE_TASKS_PER_JOB));
        let mut attempts_omitted_extra = 0usize;
        let mut candidates_omitted_extra = 0usize;
        for raw_task in self.tasks.iter().take(MAX_WORKSPACE_TASKS_PER_JOB) {
            let (norm_task, att_omitted, cand_omitted) = raw_task.normalized_with_omissions();
            attempts_omitted_extra += att_omitted;
            candidates_omitted_extra += cand_omitted;
            tasks.push(norm_task);
        }

        let all_runs = self
            .runs
            .iter()
            .map(WorkspaceRunRecord::normalized)
            .collect();
        let (runs, runs_omitted_extra) = cap_tail(all_runs, MAX_WORKSPACE_RUNS_PER_JOB);

        let all_outbox = self
            .outbox
            .iter()
            .map(WorkspaceOutboxRecord::normalized)
            .collect();
        let (outbox, outbox_omitted_extra) = cap_tail(all_outbox, MAX_WORKSPACE_OUTBOX_PER_JOB);

        let all_combined = self
            .combined_candidates
            .iter()
            .map(WorkspaceCombinedCandidate::normalized)
            .collect();
        let (combined_candidates, combined_omitted_extra) =
            cap_tail(all_combined, MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB);

        Self {
            job: self.job.normalized(),
            tasks,
            runs,
            outbox,
            combined_candidates,
            result: self.result.as_ref().map(WorkspaceJobResult::normalized),
            links: self.links.normalized(),
            historical_failure: self
                .historical_failure
                .as_ref()
                .map(WorkspaceHistoricalFailure::normalized),
            current_attention: self
                .current_attention
                .as_ref()
                .map(WorkspaceAttentionState::normalized),
            acceptance_proof: self.acceptance_proof.normalized_for_job(),
            truncation: WorkspaceJobHistoryTruncation {
                tasks_omitted: self
                    .truncation
                    .tasks_omitted
                    .saturating_add(tasks_omitted_extra),
                attempts_omitted: self
                    .truncation
                    .attempts_omitted
                    .saturating_add(attempts_omitted_extra),
                candidates_omitted: self
                    .truncation
                    .candidates_omitted
                    .saturating_add(candidates_omitted_extra),
                runs_omitted: self
                    .truncation
                    .runs_omitted
                    .saturating_add(runs_omitted_extra),
                outbox_omitted: self
                    .truncation
                    .outbox_omitted
                    .saturating_add(outbox_omitted_extra),
                combined_candidates_omitted: self
                    .truncation
                    .combined_candidates_omitted
                    .saturating_add(combined_omitted_extra),
            },
        }
    }
}

/// Bounds job history projections to at most [`MAX_WORKSPACE_JOBS_PER_BINDING`] (`64`) per
/// `(source_id, binding_id)` scope and at most [`MAX_OBSERVATION_JOBS`] (`500`) across the snapshot.
pub fn bound_workspace_job_histories(
    histories: Vec<WorkspaceJobHistory>,
) -> (Vec<WorkspaceJobHistory>, usize) {
    let mut normalized_all: Vec<WorkspaceJobHistory> = histories
        .iter()
        .map(WorkspaceJobHistory::normalized)
        .collect();

    normalized_all.sort_by(|left, right| {
        right
            .job
            .updated_at_ms
            .unwrap_or(i64::MIN)
            .cmp(&left.job.updated_at_ms.unwrap_or(i64::MIN))
            .then_with(|| left.job.source_id.cmp(&right.job.source_id))
            .then_with(|| left.job.job_id.cmp(&right.job.job_id))
    });

    let mut per_binding_counts: std::collections::BTreeMap<(String, String), usize> =
        std::collections::BTreeMap::new();
    let mut retained = Vec::new();
    let mut omitted = 0usize;

    for item in normalized_all {
        if let Some(binding_id) = &item.job.binding_id {
            let key = (item.job.source_id.clone(), binding_id.clone());
            let count = per_binding_counts.entry(key).or_insert(0);
            if *count >= MAX_WORKSPACE_JOBS_PER_BINDING {
                omitted += 1;
                continue;
            }
            *count += 1;
        }
        if retained.len() >= MAX_OBSERVATION_JOBS {
            omitted += 1;
            continue;
        }
        retained.push(item);
    }

    (retained, omitted)
}

/// Permitted safe source kinds for [`WorkspaceMessageItem`] entries.
///
/// `Messages` may only be derived from existing safe summary, result, and question records.
/// Raw provider transcripts and synthetic user messages are structurally excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceMessageSourceKind {
    /// Safe summary from a durable work brief.
    WorkBriefSummary,
    /// Safe summary from a proposed or approved plan revision.
    PlanSummary,
    /// Safe prompt summary from a structured run/workflow question.
    QuestionPrompt,
    /// Safe answer summary recorded on a structured run/workflow question.
    QuestionAnswer,
    /// Safe task candidate/attempt outcome summary.
    TaskResultSummary,
    /// Safe Reviewer verdict summary.
    ReviewVerdictSummary,
    /// Safe single-Closure completion summary.
    ClosureSummary,
    /// Safe job-level outcome/delivery summary.
    JobResultSummary,
    /// Safe settled-block or hold attention summary.
    AttentionSummary,
    /// Safe `240`-character observation journal summary.
    JournalSummary,
    /// Degraded fallback when deserializing an unrecognized wire token (rejected during projection).
    #[default]
    #[serde(other)]
    Unknown,
}

impl WorkspaceMessageSourceKind {
    /// Returns the canonical `snake_case` wire string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkBriefSummary => "work_brief_summary",
            Self::PlanSummary => "plan_summary",
            Self::QuestionPrompt => "question_prompt",
            Self::QuestionAnswer => "question_answer",
            Self::TaskResultSummary => "task_result_summary",
            Self::ReviewVerdictSummary => "review_verdict_summary",
            Self::ClosureSummary => "closure_summary",
            Self::JobResultSummary => "job_result_summary",
            Self::AttentionSummary => "attention_summary",
            Self::JournalSummary => "journal_summary",
            Self::Unknown => "unknown",
        }
    }

    /// Returns `true` only for the ten permitted safe summary/result/question sources.
    pub const fn is_permitted_safe_source(self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

/// Validates a wire string against the closed allowlist of safe `Messages` source kinds.
///
/// Explicitly rejects `"transcript"`, `"raw_transcript"`, `"provider_transcript"`,
/// `"synthetic_user_message"`, `"unknown"`, and any other non-allowlisted kind.
pub fn validate_safe_workspace_message_source(
    raw_kind: &str,
) -> Result<WorkspaceMessageSourceKind, WorkspaceContractError> {
    match raw_kind.trim() {
        "work_brief_summary" => Ok(WorkspaceMessageSourceKind::WorkBriefSummary),
        "plan_summary" => Ok(WorkspaceMessageSourceKind::PlanSummary),
        "question_prompt" => Ok(WorkspaceMessageSourceKind::QuestionPrompt),
        "question_answer" => Ok(WorkspaceMessageSourceKind::QuestionAnswer),
        "task_result_summary" => Ok(WorkspaceMessageSourceKind::TaskResultSummary),
        "review_verdict_summary" => Ok(WorkspaceMessageSourceKind::ReviewVerdictSummary),
        "closure_summary" => Ok(WorkspaceMessageSourceKind::ClosureSummary),
        "job_result_summary" => Ok(WorkspaceMessageSourceKind::JobResultSummary),
        "attention_summary" => Ok(WorkspaceMessageSourceKind::AttentionSummary),
        "journal_summary" => Ok(WorkspaceMessageSourceKind::JournalSummary),
        other => Err(WorkspaceContractError::ForbiddenMessageSource(
            other.to_string(),
        )),
    }
}

/// Deterministic source ordering tuple for a [`WorkspaceMessageItem`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceMessageSourceOrder {
    /// Upstream recorded timestamp in milliseconds since Unix epoch, if known.
    #[serde(default)]
    pub recorded_at_ms: Option<i64>,
    /// Monotonic sequence or revision number within the source ledger (`0` when unsequenced).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_seq: u64,
    /// Secondary sub-record index (for example `0` = question prompt, `1` = question answer).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub sub_index: u32,
}

/// Scope metadata binding a [`WorkspaceMessageItem`] to its exact source, chat, job, task, and run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceMessageScope {
    /// Observation source identifier.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_id: String,
    /// Optional scoped software `binding_id`.
    #[serde(default)]
    pub binding_id: Option<String>,
    /// Optional canonical `thread_uuid`.
    #[serde(default)]
    pub thread_uuid: Option<String>,
    /// Optional verified canonical project scope path.
    #[serde(default)]
    pub canonical_project_scope: Option<String>,
    /// Populated only when all four verified chat identity components are valid.
    #[serde(default)]
    pub chat_identity: Option<WorkspaceChatIdentity>,
    /// Optional correlated `job_id`.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Optional correlated `task_id`.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Optional correlated `run_id`.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Optional correlated 1-based task attempt number.
    #[serde(default)]
    pub attempt_no: Option<u32>,
}

impl WorkspaceMessageScope {
    /// Normalizes scope identifiers and derives `chat_identity` iff all four canonical fields pass validation.
    pub fn normalized(&self) -> (Self, Vec<String>) {
        let mut missing = Vec::new();
        let source_id = match validate_workspace_source_id(&self.source_id) {
            Ok(id) => id,
            Err(_) => {
                missing.push("source_id".to_string());
                default_unknown_token()
            }
        };
        let binding_id = match &self.binding_id {
            Some(raw) => match validate_workspace_binding_id(raw) {
                Ok(id) => Some(id),
                Err(_) => {
                    missing.push("binding_id".to_string());
                    None
                }
            },
            None => {
                missing.push("binding_id".to_string());
                None
            }
        };
        let thread_uuid = match &self.thread_uuid {
            Some(raw) => match validate_workspace_thread_uuid(raw) {
                Ok(id) => Some(id),
                Err(_) => {
                    missing.push("thread_uuid".to_string());
                    None
                }
            },
            None => {
                missing.push("thread_uuid".to_string());
                None
            }
        };
        let canonical_project_scope = match &self.canonical_project_scope {
            Some(raw) => match validate_canonical_project_scope(raw) {
                Ok(path) => Some(path),
                Err(_) => {
                    missing.push("canonical_project_scope".to_string());
                    None
                }
            },
            None => {
                missing.push("canonical_project_scope".to_string());
                None
            }
        };

        let explicit_valid = self.chat_identity.clone().filter(|id| {
            id.validate().is_ok()
                && id.source_id == source_id
                && binding_id.as_deref() == Some(id.binding_id.as_str())
                && thread_uuid.as_deref() == Some(id.thread_uuid.as_str())
                && canonical_project_scope.as_deref() == Some(id.canonical_project_scope.as_str())
        });
        let derived_chat = explicit_valid.or_else(|| {
            match (
                binding_id.as_deref(),
                thread_uuid.as_deref(),
                canonical_project_scope.as_deref(),
            ) {
                (Some(b), Some(t), Some(p)) => WorkspaceChatIdentity::new(&source_id, b, t, p).ok(),
                _ => None,
            }
        });

        let scope = Self {
            source_id,
            binding_id,
            thread_uuid,
            canonical_project_scope,
            chat_identity: derived_chat,
            job_id: self.job_id.as_deref().and_then(validate_correlation_id),
            task_id: self.task_id.as_deref().and_then(validate_correlation_id),
            run_id: self.run_id.as_deref().and_then(validate_correlation_id),
            attempt_no: self.attempt_no,
        };
        (scope, missing)
    }
}

/// Truncation, redaction, and missing-provenance metadata attached to a [`WorkspaceMessageItem`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessageCompleteness {
    /// Whether `summary` was truncated to fit [`MAX_OBSERVATION_SUMMARY_CHARS`] (`240` chars).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub summary_truncated: bool,
    /// Original character count prior to truncation when known.
    #[serde(default)]
    pub original_char_count: Option<usize>,
    /// Character cap enforced on `summary` (`240`).
    #[serde(default = "default_max_summary_chars")]
    pub max_summary_chars: usize,
    /// Whether `summary` was replaced due to sensitive marker detection.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub redacted: bool,
    /// Missing optional scope or ordering field names (`binding_id`, `recorded_at_ms`, etc.).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub missing_fields: Vec<String>,
    /// Optional safe explanation when the upstream record was partial or degraded (`<= 240` chars).
    #[serde(default)]
    pub incomplete_reason: Option<String>,
}

impl Default for WorkspaceMessageCompleteness {
    fn default() -> Self {
        Self {
            summary_truncated: false,
            original_char_count: None,
            max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
            redacted: false,
            missing_fields: Vec::new(),
            incomplete_reason: None,
        }
    }
}

/// Single safe summary/result/question item projected into [`WorkspaceMessagesWindow::items`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceMessageItem {
    /// Deterministic item identifier (`wmsg:v1:...`) derived from source scope, kind, and record ID.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub item_id: String,
    /// Permitted safe source kind (`work_brief_summary`, `question_prompt`, `closure_summary`, etc.).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_kind: WorkspaceMessageSourceKind,
    /// Stable upstream source record identifier (`brief_id`, `question_id`, `verdict_id`, `record_id`, etc.).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_record_id: String,
    /// Deterministic ordering coordinates within the source ledger.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source_order: WorkspaceMessageSourceOrder,
    /// Scoped provenance (`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`, `job_id`, `task_id`, `run_id`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub scope: WorkspaceMessageScope,
    /// Optional safe workflow/actor role token (`coworker`, `orchestrator`, `architect`, `coder`, `reviewer`, `closure`, `operator`); never fabricates a synthetic human transcript turn.
    #[serde(default)]
    pub actor_role: Option<String>,
    /// Sanitized safe summary (`<= 240` Unicode scalar values).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub summary: String,
    /// Truncation, redaction, and missing-field metadata.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub completeness: WorkspaceMessageCompleteness,
}

/// Input specification for projecting a validated [`WorkspaceMessageItem`] from a safe source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMessageCandidateInput {
    /// Wire source kind string (validated against [`validate_safe_workspace_message_source`]).
    pub source_kind: String,
    /// Stable upstream source record identifier.
    pub source_record_id: String,
    /// Ordering metadata from the source record.
    pub source_order: WorkspaceMessageSourceOrder,
    /// Scope metadata for this item.
    pub scope: WorkspaceMessageScope,
    /// Optional safe workflow/actor role token.
    pub actor_role: Option<String>,
    /// Raw summary text to sanitize and bound to `240` chars.
    pub raw_summary: String,
    /// Whether the upstream source text was already pre-truncated before reaching this projection.
    pub upstream_truncated: bool,
    /// Optional safe reason when upstream data was incomplete.
    pub incomplete_reason: Option<String>,
}

fn format_workspace_message_item_id(
    source_id: &str,
    source_kind: WorkspaceMessageSourceKind,
    source_record_id: &str,
    sub_index: u32,
) -> String {
    let kind_str = source_kind.as_str();
    format!(
        "wmsg:v1:{}:{}:{}:{}:{}:{}:{}",
        source_id.len(),
        source_id,
        kind_str.len(),
        kind_str,
        source_record_id.len(),
        source_record_id,
        sub_index,
    )
}

fn compare_workspace_messages(
    left: &WorkspaceMessageItem,
    right: &WorkspaceMessageItem,
) -> std::cmp::Ordering {
    let left_has_ts = left.source_order.recorded_at_ms.is_some();
    let right_has_ts = right.source_order.recorded_at_ms.is_some();
    left_has_ts
        .cmp(&right_has_ts)
        .then_with(|| {
            left.source_order
                .recorded_at_ms
                .unwrap_or(0)
                .cmp(&right.source_order.recorded_at_ms.unwrap_or(0))
        })
        .then_with(|| {
            left.source_order
                .source_seq
                .cmp(&right.source_order.source_seq)
        })
        .then_with(|| {
            left.source_order
                .sub_index
                .cmp(&right.source_order.sub_index)
        })
        .then_with(|| left.scope.source_id.cmp(&right.scope.source_id))
        .then_with(|| left.source_kind.as_str().cmp(right.source_kind.as_str()))
        .then_with(|| left.source_record_id.cmp(&right.source_record_id))
        .then_with(|| left.item_id.cmp(&right.item_id))
}

impl WorkspaceMessageItem {
    /// Constructs a validated [`WorkspaceMessageItem`] from a safe summary/result/question source.
    ///
    /// Rejects raw transcript and synthetic user message kinds as well as invalid `source_record_id`
    /// or `scope.source_id` values.
    pub fn from_safe_input(
        input: WorkspaceMessageCandidateInput,
    ) -> Result<Self, WorkspaceContractError> {
        let kind = validate_safe_workspace_message_source(&input.source_kind)?;
        let _ = validate_workspace_source_id(&input.scope.source_id)?;
        let Some(clean_record_id) = validate_correlation_id(&input.source_record_id) else {
            return Err(WorkspaceContractError::InvalidNativeSample(
                "message source_record_id must be a valid non-empty ASCII identifier".to_string(),
            ));
        };
        let (norm_scope, mut missing_fields) = input.scope.normalized();
        let recorded_at_ms = input.source_order.recorded_at_ms.filter(|&v| v > 0);
        if recorded_at_ms.is_none() {
            missing_fields.push("recorded_at_ms".to_string());
        }
        let sanitized = sanitize_workspace_summary_with_meta(&input.raw_summary);
        let order = WorkspaceMessageSourceOrder {
            recorded_at_ms,
            source_seq: input.source_order.source_seq,
            sub_index: input.source_order.sub_index,
        };
        let item_id = format_workspace_message_item_id(
            &norm_scope.source_id,
            kind,
            &clean_record_id,
            order.sub_index,
        );
        let incomplete_reason = input
            .incomplete_reason
            .as_deref()
            .map(sanitize_workspace_summary)
            .filter(|s| !s.is_empty());

        Ok(Self {
            item_id,
            source_kind: kind,
            source_record_id: clean_record_id,
            source_order: order,
            scope: norm_scope,
            actor_role: input
                .actor_role
                .as_deref()
                .and_then(validate_correlation_id),
            summary: sanitized.summary,
            completeness: WorkspaceMessageCompleteness {
                summary_truncated: sanitized.truncated || input.upstream_truncated,
                original_char_count: sanitized.original_char_count,
                max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
                redacted: sanitized.redacted,
                missing_fields,
                incomplete_reason,
            },
        })
    }

    /// Normalizes a deserialized [`WorkspaceMessageItem`], returning `None` if its `source_kind` is
    /// not a permitted safe source or its `source_record_id` / `scope.source_id` is invalid.
    pub fn normalized_safe(&self) -> Option<Self> {
        if !self.source_kind.is_permitted_safe_source() {
            return None;
        }
        let _ = validate_workspace_source_id(&self.scope.source_id).ok()?;
        let clean_record_id = validate_correlation_id(&self.source_record_id)?;
        let (norm_scope, mut missing_fields) = self.scope.normalized();
        for existing in &self.completeness.missing_fields {
            if let Some(clean_name) = validate_correlation_id(existing) {
                if !missing_fields.contains(&clean_name) {
                    missing_fields.push(clean_name);
                }
            }
        }
        let recorded_at_ms = self.source_order.recorded_at_ms.filter(|&v| v > 0);
        if recorded_at_ms.is_none() && !missing_fields.iter().any(|f| f == "recorded_at_ms") {
            missing_fields.push("recorded_at_ms".to_string());
        }
        let sanitized = sanitize_workspace_summary_with_meta(&self.summary);
        let order = WorkspaceMessageSourceOrder {
            recorded_at_ms,
            source_seq: self.source_order.source_seq,
            sub_index: self.source_order.sub_index,
        };
        let item_id = format_workspace_message_item_id(
            &norm_scope.source_id,
            self.source_kind,
            &clean_record_id,
            order.sub_index,
        );
        let original_char_count = match (
            self.completeness.original_char_count,
            sanitized.original_char_count,
        ) {
            (_, None) => None,
            (Some(prev), Some(curr)) => Some(prev.max(curr)),
            (None, Some(curr)) => Some(curr),
        };
        let incomplete_reason = clean_opt_summary(self.completeness.incomplete_reason.as_deref());
        Some(Self {
            item_id,
            source_kind: self.source_kind,
            source_record_id: clean_record_id,
            source_order: order,
            scope: norm_scope,
            actor_role: clean_opt_id(self.actor_role.as_deref()),
            summary: sanitized.summary,
            completeness: WorkspaceMessageCompleteness {
                summary_truncated: self.completeness.summary_truncated || sanitized.truncated,
                original_char_count,
                max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
                redacted: self.completeness.redacted || sanitized.redacted,
                missing_fields,
                incomplete_reason,
            },
        })
    }
}

/// Bounded (`<= 1,000` items) safe `Messages` window carried by [`WorkspaceSnapshot::messages`].
///
/// Never requires raw provider transcripts (`requires_transcript == false`) and never permits
/// synthetic user messages (`allows_synthetic_user_messages == false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMessagesWindow {
    /// Window availability (`unavailable` when omitted on legacy snapshots; `available` when projected).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub availability: WorkspaceAvailabilityState,
    /// Ordered safe message items (`<= 1,000`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub items: Vec<WorkspaceMessageItem>,
    /// Enforced window cap (`1,000`).
    #[serde(default = "default_max_messages_window")]
    pub max_items: usize,
    /// Count of older or invalid items omitted from `items`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub omitted_count: usize,
    /// Whether additional safe items exist outside this window.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub has_more: bool,
    /// Whether `items` was truncated at `max_items` or omitted unsafe entries.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub truncated: bool,
    /// Structural invariant: `Messages` never requires raw provider conversation transcripts (`false`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub requires_transcript: bool,
    /// Structural invariant: `Messages` never synthesizes human user messages (`false`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub allows_synthetic_user_messages: bool,
}

impl Default for WorkspaceMessagesWindow {
    fn default() -> Self {
        Self {
            availability: WorkspaceAvailabilityState::Unavailable,
            items: Vec::new(),
            max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
            omitted_count: 0,
            has_more: false,
            truncated: false,
            requires_transcript: false,
            allows_synthetic_user_messages: false,
        }
    }
}

impl WorkspaceMessagesWindow {
    /// Normalizes, deduplicates by `item_id`, deterministically orders, and bounds `items` to
    /// [`MAX_WORKSPACE_MESSAGES_WINDOW`] (`1,000`), forcing transcript/synthetic flags to `false`.
    pub fn normalized(&self) -> Self {
        let mut dropped_invalid = 0usize;
        let mut by_id: std::collections::BTreeMap<String, WorkspaceMessageItem> =
            std::collections::BTreeMap::new();
        for raw in &self.items {
            match raw.normalized_safe() {
                Some(clean) => {
                    by_id.insert(clean.item_id.clone(), clean);
                }
                None => {
                    dropped_invalid += 1;
                }
            }
        }
        let mut ordered: Vec<WorkspaceMessageItem> = by_id.into_values().collect();
        ordered.sort_by(compare_workspace_messages);

        let window_overflow = ordered.len().saturating_sub(MAX_WORKSPACE_MESSAGES_WINDOW);
        if window_overflow > 0 {
            ordered.drain(0..window_overflow);
        }

        let omitted_count = self
            .omitted_count
            .saturating_add(dropped_invalid)
            .saturating_add(window_overflow);
        let has_more = self.has_more || window_overflow > 0;
        let truncated = self.truncated || omitted_count > 0;

        Self {
            availability: self.availability,
            items: ordered,
            max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
            omitted_count,
            has_more,
            truncated,
            requires_transcript: false,
            allows_synthetic_user_messages: false,
        }
    }
}

/// Projects a deterministic, bounded (`<= 1,000` items) [`WorkspaceMessagesWindow`] strictly from
/// safe summary, result, and question sources.
pub fn project_safe_workspace_messages(
    inputs: Vec<WorkspaceMessageCandidateInput>,
) -> Result<WorkspaceMessagesWindow, WorkspaceContractError> {
    let mut items = Vec::with_capacity(inputs.len());
    for input in inputs {
        items.push(WorkspaceMessageItem::from_safe_input(input)?);
    }
    let raw_window = WorkspaceMessagesWindow {
        availability: WorkspaceAvailabilityState::Available,
        items,
        max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
        omitted_count: 0,
        has_more: false,
        truncated: false,
        requires_transcript: false,
        allows_synthetic_user_messages: false,
    };
    Ok(raw_window.normalized())
}

/// Top-level truncation and bound accounting on [`WorkspaceSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WorkspaceSnapshotBounds {
    /// Native samples omitted due to validation or the `128`/source cap.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub native_samples_omitted: usize,
    /// Job history projections omitted due to the `64`/binding or `500`/snapshot cap.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub job_histories_omitted: usize,
    /// Message items omitted from [`WorkspaceSnapshot::messages`] (`1,000` cap).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub messages_omitted: usize,
    /// Whether `observation.requests` was capped at [`MAX_OBSERVATION_REQUEST_PAGE_LIMIT`] (`200`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub requests_capped: bool,
    /// Whether `observation.jobs` was capped at [`MAX_OBSERVATION_JOBS`] (`500`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub jobs_capped: bool,
}

/// Separate read-only workspace observation envelope that embeds [`ObservationSnapshot`] without
/// altering [`ObservationSnapshot`] or [`ObservationActor`] wire shapes or struct literals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    /// Wire schema version (`1`).
    #[serde(default = "default_workspace_schema_version")]
    pub schema_version: u32,
    /// Timestamp (ms since Unix epoch) when this envelope was assembled.
    ///
    /// Assembling or regenerating a [`WorkspaceSnapshot`] never resets native sample age.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub generated_at_ms: i64,
    /// Native sampling and staleness policy (`2 s` interval, `750 ms` metadata timeout, `10 s` stale threshold).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub sampling_policy: WorkspaceSamplingPolicy,
    /// Preserved backward-compatible [`ObservationSnapshot`] payload.
    #[serde(default = "default_empty_observation_snapshot")]
    pub observation: ObservationSnapshot,
    /// Canonical chat summaries keyed by `(source_id, binding_id, thread_uuid, canonical_project_scope)`.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub chats: Vec<WorkspaceChatSummary>,
    /// Native conversation samples (`<= 128` per source, each sidecar `<= 64 KiB`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub native_samples: Vec<WorkspaceNativeSample>,
    /// Structured job history projections (`<= 64` per binding, `<= 500` total).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub job_histories: Vec<WorkspaceJobHistory>,
    /// Safe `Messages` window (`<= 1,000` items; never requires transcripts or synthetic user messages).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub messages: WorkspaceMessagesWindow,
    /// Explicit truncation and bound accounting across all projected windows.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub bounds: WorkspaceSnapshotBounds,
}

impl WorkspaceSnapshot {
    /// Creates a minimal normalized [`WorkspaceSnapshot`] around an existing [`ObservationSnapshot`].
    pub fn new(generated_at_ms: i64, observation: ObservationSnapshot) -> Self {
        Self {
            schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
            generated_at_ms,
            sampling_policy: WorkspaceSamplingPolicy::default(),
            observation,
            chats: Vec::new(),
            native_samples: Vec::new(),
            job_histories: Vec::new(),
            messages: WorkspaceMessagesWindow::default(),
            bounds: WorkspaceSnapshotBounds::default(),
        }
        .normalized()
    }

    /// Upgrades a legacy [`ObservationSnapshot`] into a [`WorkspaceSnapshot`] where missing new
    /// surfaces (`native_samples`, `messages`, unverified chat/job fields) degrade safely to
    /// `unknown` / `unavailable`.
    pub fn from_legacy_observation(observation: ObservationSnapshot) -> Self {
        let generated_at_ms = observation.generated_at_ms;
        let mut source_paths: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for src in &observation.sources {
            if let Some(path) = &src.project_path {
                if let Ok(clean_path) = validate_canonical_project_scope(path) {
                    source_paths.insert(src.id.clone(), clean_path);
                }
            }
        }

        let mut chats_by_key: std::collections::BTreeMap<String, WorkspaceChatSummary> =
            std::collections::BTreeMap::new();
        let mut histories = Vec::new();

        for job in &observation.jobs {
            let scope_path = job
                .project_path
                .as_deref()
                .and_then(|p| validate_canonical_project_scope(p).ok())
                .or_else(|| source_paths.get(&job.source_id).cloned());

            let maybe_identity = match (
                job.binding_id.as_deref(),
                job.thread_uuid.as_deref(),
                scope_path.as_deref(),
            ) {
                (Some(b), Some(t), Some(p)) => {
                    WorkspaceChatIdentity::new(&job.source_id, b, t, p).ok()
                }
                _ => None,
            };

            if let Some(identity) = &maybe_identity {
                if let Some(key) = identity.canonical_key() {
                    let is_active = matches!(
                        job.state,
                        ObservationJobState::Planning
                            | ObservationJobState::Ready
                            | ObservationJobState::Running
                            | ObservationJobState::Reviewing
                            | ObservationJobState::NeedsUser
                    ) || job.active_call_count > 0;
                    let needs_attention = job.state == ObservationJobState::NeedsUser;
                    let entry = chats_by_key
                        .entry(key)
                        .or_insert_with(|| WorkspaceChatSummary {
                            identity: identity.clone(),
                            thread_label: sanitize_observation_label(
                                job.thread_label.as_deref().unwrap_or("Codex chat"),
                                "Codex chat",
                            ),
                            project_label: sanitize_observation_label(
                                &job.project_label,
                                "Project",
                            ),
                            availability: WorkspaceAvailabilityState::Unavailable,
                            activity: WorkspaceActivityState::Unknown,
                            configured_selection: WorkspaceConfiguredSelection::unavailable(),
                            active_job_count: 0,
                            attention_count: 0,
                            updated_at_ms: job.updated_at_ms,
                        });
                    if is_active {
                        entry.active_job_count += 1;
                    }
                    if needs_attention {
                        entry.attention_count += 1;
                    }
                    if let Some(ts) = job.updated_at_ms {
                        if entry.updated_at_ms.is_none_or(|prev| ts > prev) {
                            entry.updated_at_ms = Some(ts);
                        }
                    }
                }
            }

            let tasks = job
                .tasks
                .iter()
                .map(|t| WorkspaceTaskHistory {
                    task_id: t.id.clone(),
                    title: t.title.clone(),
                    status: t.status.clone(),
                    depends_on: t.depends_on.clone(),
                    ..WorkspaceTaskHistory::default()
                })
                .collect();

            histories.push(WorkspaceJobHistory {
                job: WorkspaceJobSummary {
                    source_id: job.source_id.clone(),
                    job_id: job.id.clone(),
                    binding_id: job.binding_id.clone(),
                    chat_identity: maybe_identity,
                    title: job.title.clone(),
                    job_revision: None,
                    state: job.state,
                    phase: None,
                    approval_status: job.approval_status.clone(),
                    execution_status: job.execution_status.clone(),
                    review_state: job.review_state.clone(),
                    created_at_ms: None,
                    updated_at_ms: job.updated_at_ms,
                    summary: job.summary.clone(),
                },
                tasks,
                links: WorkspaceRecordLinks {
                    source_plan_id: job.plan_id.clone(),
                    source_plan_revision: job.plan_revision,
                    ..WorkspaceRecordLinks::default()
                },
                acceptance_proof: WorkspaceAcceptanceProof::unknown(),
                ..WorkspaceJobHistory::default()
            });
        }

        Self {
            schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
            generated_at_ms,
            sampling_policy: WorkspaceSamplingPolicy::default(),
            observation,
            chats: chats_by_key.into_values().collect(),
            native_samples: Vec::new(),
            job_histories: histories,
            messages: WorkspaceMessagesWindow::default(),
            bounds: WorkspaceSnapshotBounds::default(),
        }
        .normalized()
    }

    /// Normalizes all nested bounds, chat identities, sample freshness states (at `generated_at_ms`),
    /// job history projections, and safe `Messages` items without ever mutating
    /// `native_samples[*].observed_at_ms` or `native_samples[*].last_success_at_ms`.
    pub fn normalized(&self) -> Self {
        let policy = self.sampling_policy.normalized();

        let mut observation = self.observation.clone();
        let mut jobs_capped = false;
        if observation.jobs.len() > MAX_OBSERVATION_JOBS {
            observation.jobs.truncate(MAX_OBSERVATION_JOBS);
            observation.jobs_has_more = true;
            jobs_capped = true;
        }
        let mut requests_capped = false;
        if observation.requests.len() > MAX_OBSERVATION_REQUEST_PAGE_LIMIT {
            observation
                .requests
                .truncate(MAX_OBSERVATION_REQUEST_PAGE_LIMIT);
            observation.has_more = true;
            requests_capped = true;
        }

        let (native_samples, samples_omitted) =
            bound_workspace_native_samples_per_source(self.native_samples.clone());

        let mut evaluated_by_chat: std::collections::BTreeMap<
            String,
            (&WorkspaceNativeSample, WorkspaceEvaluatedNativeSample),
        > = std::collections::BTreeMap::new();
        for sample in &native_samples {
            if let Some(chat_key) = sample.chat_identity.canonical_key() {
                let eval = sample.evaluate_at(self.generated_at_ms, policy.stale_threshold_ms);
                evaluated_by_chat.insert(chat_key, (sample, eval));
            }
        }

        let mut chats_by_key: std::collections::BTreeMap<String, WorkspaceChatSummary> =
            std::collections::BTreeMap::new();
        for chat in &self.chats {
            let norm_chat = chat.normalized();
            let Some(chat_key) = norm_chat.identity.canonical_key() else {
                continue;
            };
            chats_by_key.insert(chat_key, norm_chat);
        }

        for (chat_key, (sample, eval)) in &evaluated_by_chat {
            let entry =
                chats_by_key
                    .entry(chat_key.clone())
                    .or_insert_with(|| WorkspaceChatSummary {
                        identity: sample.chat_identity.clone(),
                        thread_label: "Codex chat".to_string(),
                        project_label: "Project".to_string(),
                        availability: eval.effective_availability,
                        activity: eval.effective_activity,
                        configured_selection: eval.effective_configured_selection.clone(),
                        active_job_count: 0,
                        attention_count: 0,
                        updated_at_ms: sample.observed_at_ms,
                    });
            entry.availability = eval.effective_availability;
            entry.activity = eval.effective_activity;
            entry.configured_selection = eval.effective_configured_selection.clone();
            if let Some(obs_ms) = sample.observed_at_ms {
                if entry.updated_at_ms.is_none_or(|prev| obs_ms > prev) {
                    entry.updated_at_ms = Some(obs_ms);
                }
            }
        }

        let chats: Vec<WorkspaceChatSummary> = chats_by_key.into_values().collect();
        let (job_histories, jobs_hist_omitted) =
            bound_workspace_job_histories(self.job_histories.clone());
        let messages = self.messages.normalized();

        let bounds = WorkspaceSnapshotBounds {
            native_samples_omitted: self
                .bounds
                .native_samples_omitted
                .saturating_add(samples_omitted),
            job_histories_omitted: self
                .bounds
                .job_histories_omitted
                .saturating_add(jobs_hist_omitted),
            messages_omitted: messages.omitted_count,
            requests_capped: self.bounds.requests_capped || requests_capped,
            jobs_capped: self.bounds.jobs_capped || jobs_capped,
        };

        Self {
            schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
            generated_at_ms: self.generated_at_ms,
            sampling_policy: policy,
            observation,
            chats,
            native_samples,
            job_histories,
            messages,
            bounds,
        }
    }

    /// Re-assembles this [`WorkspaceSnapshot`] at `next_generated_at_ms`, updating `generated_at_ms`
    /// and re-evaluating sample staleness while preserving every sample's original `observed_at_ms`
    /// and `last_success_at_ms`.
    pub fn regenerate_at(&self, next_generated_at_ms: i64) -> Self {
        let mut next = self.clone();
        next.generated_at_ms = next_generated_at_ms;
        next.observation.generated_at_ms = next_generated_at_ms;
        next.normalized()
    }
}

/// Parses either a [`WorkspaceSnapshot`] JSON envelope or a bare legacy [`ObservationSnapshot`]
/// JSON payload, normalizing missing fields safely to `unknown` / `unavailable`.
pub fn parse_workspace_snapshot_json(
    raw_json: &str,
) -> Result<WorkspaceSnapshot, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_str(raw_json)?;
    if let serde_json::Value::Object(ref map) = value {
        let has_workspace_observation = map
            .get("observation")
            .is_some_and(serde_json::Value::is_object);
        let looks_like_legacy_observation = !has_workspace_observation
            && (map.contains_key("sources")
                || map.contains_key("requests")
                || (map.contains_key("jobs") && !map.contains_key("job_histories")));
        if looks_like_legacy_observation {
            let legacy: ObservationSnapshot = serde_json::from_value(value)?;
            return Ok(WorkspaceSnapshot::from_legacy_observation(legacy));
        }
    }
    let snapshot: WorkspaceSnapshot = serde_json::from_value(value)?;
    Ok(snapshot.normalized())
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
        assert_eq!(legacy_actor.selected_model, None);
        assert_eq!(legacy_actor.selected_effort, None);
        assert_eq!(legacy_actor.evidence, ObservationActorEvidence::Configured);
        assert_eq!(
            serde_json::to_string(&legacy_actor).expect("re-serialize legacy actor"),
            r#"{"label":"Codex · Legacy Chat","role":"follower","source":"paired_mcp","model":null,"effort":null,"evidence":"configured"}"#,
            "omitted selected_model/selected_effort must be skipped when None"
        );

        let selected_ctx = ctx
            .clone()
            .with_selected_model(Some("  gpt-5-sol\n "), Some(" xhigh\t"));
        assert_eq!(selected_ctx.actor.model, None);
        assert_eq!(selected_ctx.actor.effort, None);
        assert_eq!(
            selected_ctx.actor.selected_model.as_deref(),
            Some("gpt-5-sol")
        );
        assert_eq!(selected_ctx.actor.selected_effort.as_deref(), Some("xhigh"));
        assert_eq!(
            selected_ctx.actor.evidence,
            ObservationActorEvidence::Configured
        );

        let mut refreshable = selected_ctx.clone();
        refreshable.refresh_thread_metadata(
            Some("Updated Chat"),
            Some("gpt-6-astra"),
            Some("high"),
        );
        assert_eq!(refreshable.thread_label, "Updated Chat");
        assert_eq!(
            refreshable.actor.label.as_deref(),
            Some("Codex · Updated Chat")
        );
        assert_eq!(refreshable.actor.model, None);
        assert_eq!(refreshable.actor.effort, None);
        assert_eq!(
            refreshable.actor.selected_model.as_deref(),
            Some("gpt-6-astra")
        );
        assert_eq!(refreshable.actor.selected_effort.as_deref(), Some("high"));

        // Secret-like tokens or empty strings are rejected, and clear_selected_model drops stale data.
        assert_eq!(
            sanitize_observation_selection_token(Some("sk-secret-model-token")),
            None
        );
        assert_eq!(sanitize_observation_selection_token(Some("   \n\t ")), None);
        refreshable.clear_selected_model();
        assert_eq!(refreshable.actor.selected_model, None);
        assert_eq!(refreshable.actor.selected_effort, None);

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

    #[test]
    fn list_activity_readonly_reads_bounded_history_and_validates_scope_without_mutation() {
        let dir = ScopedPrivateDir::new("readonly-activity");
        let db_path = dir.path.join("state.db");
        let thread_uuid = "0196ed00-0000-7000-8000-00000000000a";

        let (binding_id, run_id) = {
            let mut core = crate::Core::open(&db_path).expect("open core");
            let binding = core
                .create_binding(thread_uuid, "Stopped History Test")
                .expect("create binding");
            let out = core
                .execute(
                    &binding.binding_id,
                    crate::Command::StartDemo {
                        command_key: "cmd-ro-start-1".to_string(),
                        instruction: "Test stopped activity read".to_string(),
                        step_delay_ms: Some(100),
                        now_ms: 1_000,
                    },
                )
                .expect("start run");
            let run = match out {
                crate::CommandOutput::Run(r) => *r,
                other => panic!("unexpected output: {other:?}"),
            };
            core.tick(1_200).expect("tick 1");
            core.tick(1_400).expect("tick 2");
            (binding.binding_id, run.run_id)
        };

        let pre_snap = read_core_state_readonly(&db_path).expect("pre snapshot");
        let pre_meta = fs::symlink_metadata(&db_path).expect("pre db meta");

        let page1 = crate::Core::list_activity_readonly(
            &db_path,
            &binding_id,
            thread_uuid,
            None,
            None,
            1,
            Some(&run_id),
            None,
        )
        .expect("list_activity_readonly page 1");
        assert_eq!(page1.binding_id, binding_id);
        assert_eq!(page1.events.len(), 1);
        assert!(page1.has_more);
        assert!(page1.next_cursor.is_some());

        let page2 = crate::Core::list_activity_readonly(
            &db_path,
            &binding_id,
            thread_uuid,
            None,
            page1.next_cursor.as_deref(),
            10,
            Some(&run_id),
            None,
        )
        .expect("list_activity_readonly page 2");
        assert!(!page2.events.is_empty());
        assert!(page2.events[0].seq > page1.events[0].seq);

        let wrong_binding_err = crate::Core::list_activity_readonly(
            &db_path,
            "bnd_00000000000000000000000000000000",
            thread_uuid,
            None,
            None,
            10,
            None,
            None,
        )
        .expect_err("wrong binding must fail");
        assert!(matches!(
            wrong_binding_err,
            ReadonlySqliteError::Core(crate::CoreError::BindingNotFound { .. })
        ));

        let wrong_thread_err = crate::Core::list_activity_readonly(
            &db_path,
            &binding_id,
            "0196ed00-0000-7000-8000-00000000000b",
            None,
            None,
            10,
            None,
            None,
        )
        .expect_err("wrong thread_uuid must fail");
        assert!(matches!(
            wrong_thread_err,
            ReadonlySqliteError::Core(crate::CoreError::Validation {
                field: "thread_uuid",
                ..
            })
        ));

        let post_snap = read_core_state_readonly(&db_path).expect("post snapshot");
        let post_meta = fs::symlink_metadata(&db_path).expect("post db meta");
        assert_eq!(pre_snap, post_snap);
        assert_eq!(pre_meta.len(), post_meta.len());
        verify_unchanged_file_metadata(&pre_meta, &post_meta, "state.db")
            .expect("state.db metadata must be unchanged");

        // Residual sidecars are rejected before SQLite opens them, preserving bytes and metadata.
        for suffix in ["-wal", "-shm"] {
            let sidecar = sqlite_sidecar_path(&db_path, suffix);
            fs::write(&sidecar, b"residual sidecar").expect("write isolated sidecar");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&sidecar, fs::Permissions::from_mode(0o600))
                    .expect("private sidecar");
            }
            let before = fs::symlink_metadata(&sidecar).expect("sidecar metadata");
            let err = crate::Core::list_activity_readonly(
                &db_path,
                &binding_id,
                thread_uuid,
                None,
                None,
                10,
                None,
                None,
            )
            .expect_err("residual sidecar must fail closed");
            assert!(matches!(err, ReadonlySqliteError::SnapshotUnavailable(_)));
            assert_eq!(
                fs::read(&sidecar).expect("sidecar bytes"),
                b"residual sidecar"
            );
            verify_unchanged_file_metadata(
                &before,
                &fs::symlink_metadata(&sidecar).expect("post sidecar"),
                "sidecar",
            )
            .expect("sidecar unchanged");
            fs::remove_file(sidecar).expect("remove isolated sidecar");
        }

        // A historical schema is rejected in place, without running migrations.
        let conn = rusqlite::Connection::open(&db_path).expect("open isolated schema fixture");
        conn.execute(
            "UPDATE schema_meta SET version = ?1 WHERE id = 1",
            [i64::from(crate::SCHEMA_VERSION) - 1],
        )
        .expect("set old schema");
        drop(conn);
        let old_schema_bytes = fs::read(&db_path).expect("old schema bytes");
        let old_schema_meta = fs::symlink_metadata(&db_path).expect("old schema metadata");
        let err = crate::Core::list_activity_readonly(
            &db_path,
            &binding_id,
            thread_uuid,
            None,
            None,
            10,
            None,
            None,
        )
        .expect_err("old schema must not be migrated");
        assert!(matches!(
            err,
            ReadonlySqliteError::Core(crate::CoreError::IncompatibleSchemaVersion { .. })
        ));
        assert_eq!(
            fs::read(&db_path).expect("unchanged old schema"),
            old_schema_bytes
        );
        verify_unchanged_file_metadata(
            &old_schema_meta,
            &fs::symlink_metadata(&db_path).expect("post schema metadata"),
            "state.db",
        )
        .expect("schema rejection leaves metadata unchanged");
    }

    #[test]
    fn observation_snapshot_deserializes_legacy_payload_without_warning_contexts() {
        let legacy_json = r#"{
            "schema_version": 1,
            "generated_at_ms": 1700000000000,
            "sources": [],
            "jobs": [],
            "requests": [],
            "next_cursor": null,
            "has_more": false,
            "jobs_has_more": false,
            "warnings": ["Skipped unreadable or invalid coworker workflow ledger in observation source."]
        }"#;

        let snapshot: ObservationSnapshot =
            serde_json::from_str(legacy_json).expect("legacy snapshot should deserialize");
        assert_eq!(snapshot.schema_version, OBSERVATION_SCHEMA_VERSION);
        assert_eq!(
            snapshot.warnings,
            vec!["Skipped unreadable or invalid coworker workflow ledger in observation source."]
        );
        assert!(snapshot.warning_contexts.is_empty());

        let serialized_empty =
            serde_json::to_value(&snapshot).expect("serialize snapshot with empty contexts");
        assert!(serialized_empty.get("warning_contexts").is_none());

        let mut with_context = snapshot;
        with_context
            .warning_contexts
            .push(ObservationWarningContext {
                warning:
                    "Skipped unreadable or invalid coworker workflow ledger in observation source."
                        .to_string(),
                source_id: Some("src_alpha".to_string()),
                origin: "jobs".to_string(),
            });
        let round_trip_json =
            serde_json::to_string(&with_context).expect("serialize snapshot with contexts");
        let round_trip: ObservationSnapshot =
            serde_json::from_str(&round_trip_json).expect("deserialize snapshot with contexts");
        assert_eq!(round_trip.warning_contexts, with_context.warning_contexts);
    }
}
