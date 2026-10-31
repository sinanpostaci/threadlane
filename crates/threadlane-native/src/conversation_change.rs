//! Scoped conversational change routing, durable change records, and capability boundaries.
//!
//! Implements Section C ("Conversation changes and Steer boundaries") of the workflow loop
//! control contract:
//! - Native Codex interprets the human's conversational input and submits a structured
//!   [`ConversationChangeRequest`]. Threadlane never monitors chat transcripts, never runs a
//!   background classifier, and never grants authority to worker prose or MCP payloads.
//! - Questions and status inquiries (`question_or_status`) take the `read_only_observation`
//!   route (`record_only`), never revising plans, cancelling tasks, dispatching workers, or
//!   blocking workflow advancement.
//! - Clarifications (`clarification`) take the `checkpoint_clarification` route
//!   (`pending_clarification_at_checkpoint`) and are held for a real workflow checkpoint.
//!   Attempts to smuggle write-scope expansions or model/profile/effort changes on the
//!   clarification route are rejected with `route_mismatch`.
//! - Scope changes (`scope_change`), model/profile changes (`model_or_profile_change`), and
//!   explicit stops (`stop_or_cancel`) take the `scope_or_model_replan` route
//!   (`cancel_replan_new_approval_required`).
//! - Because current worker providers (`antigravity`, `opencode_v2`) have no verified live
//!   in-flight input channel, pause, or resume proof, decisions always report
//!   `live_input_delivered: false`, `process_paused: false`, `provider_resumed: false`, and
//!   `frozen_approval_immutable: true`. Requesting `require_verified_live_input` fails closed
//!   with `unsupported_capability`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use threadlane_authority::validation::{
    validate_canonical_thread_uuid, validate_identifier,
    validate_revision as validate_authority_revision, validate_sha256_hex,
};
use threadlane_authority::{ApprovalRecord, ApprovedTaskRecord, AuthorityError, AuthorityStore};
use threadlane_host::{ApprovedHostClient, HostError, TaskExecutionStatus, TaskStateSnapshot};
use threadlane_workflow::{compile_plan_revision, PlanError, PlanStore, ToolAdapter};
use uuid::Uuid;

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, ensure_platform_supported,
    prepare_private_dir, read_private_file_bounded, validate_absolute_no_symlink_components,
    write_private_file_atomic,
};

/// Schema version for persisted per-binding conversation-change ledger files.
pub const CONVERSATION_CHANGE_SCHEMA_VERSION: u32 = 1;

/// Policy identifier embedded in every [`ConversationChangeDecision`].
pub const CONVERSATION_CHANGE_POLICY_VERSION: &str = "conversation_change_routing_v1";

/// Subdirectory name under `plans_dir` where per-binding conversation-change ledgers are stored.
pub const CONVERSATION_CHANGE_NAMESPACE_DIR: &str = "conversation-changes";

/// Maximum byte size (`1 MiB`) permitted for a single binding's conversation-change ledger file.
pub const MAX_CONVERSATION_CHANGE_STORE_BYTES: usize = 1_048_576;

/// Maximum number of change records retained in a single binding's conversation-change ledger.
pub const MAX_CONVERSATION_CHANGES_PER_BINDING: usize = 256;

/// Default page size for [`ConversationChangeStore::list_changes`].
pub const DEFAULT_CONVERSATION_CHANGE_LIST_LIMIT: usize = 20;

/// Maximum page size for [`ConversationChangeStore::list_changes`].
pub const MAX_CONVERSATION_CHANGE_LIST_LIMIT: usize = 20;

/// Maximum byte length for a pagination cursor passed to [`ConversationChangeStore::list_changes`].
pub const MAX_CONVERSATION_CHANGE_CURSOR_LEN: usize = 128;

/// Maximum byte length for [`ConversationChangeRequest::summary`].
pub const MAX_CONVERSATION_CHANGE_SUMMARY_BYTES: usize = 512;

/// Maximum byte length for [`ConversationChangeRequest::detail`].
pub const MAX_CONVERSATION_CHANGE_DETAIL_BYTES: usize = 4_096;

/// Maximum byte length for [`ResolveConversationChangeRequest::resolution_note`].
pub const MAX_CONVERSATION_CHANGE_RESOLUTION_NOTE_BYTES: usize = 1_024;

const CHANGE_ID_PREFIX: &str = "chg_";
const CHANGE_ID_HEX_LEN: usize = 32;
const MAX_CANCEL_REASON_BYTES: usize = 512;

static IN_PROCESS_CHANGE_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> =
    OnceLock::new();

/// Interpreted category of a conversational human message submitted by native Codex.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationChangeKind {
    /// Side-effect-free question or status inquiry (`read_only_observation`).
    QuestionOrStatus,
    /// Clarification within the existing task scope and model assignment (`checkpoint_clarification`).
    Clarification,
    /// Change to task scope, write paths, deliverables, or acceptance criteria (`scope_or_model_replan`).
    ScopeChange,
    /// Change to workflow profile, model, or reasoning effort (`scope_or_model_replan`).
    ModelOrProfileChange,
    /// Request to stop or cancel active/planned work and replan (`scope_or_model_replan`).
    StopOrCancel,
}

impl ConversationChangeKind {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuestionOrStatus => "question_or_status",
            Self::Clarification => "clarification",
            Self::ScopeChange => "scope_change",
            Self::ModelOrProfileChange => "model_or_profile_change",
            Self::StopOrCancel => "stop_or_cancel",
        }
    }

    /// Returns the required [`ConversationChangeRoute`] for this change kind.
    pub const fn default_route(self) -> ConversationChangeRoute {
        match self {
            Self::QuestionOrStatus => ConversationChangeRoute::ReadOnlyObservation,
            Self::Clarification => ConversationChangeRoute::CheckpointClarification,
            Self::ScopeChange | Self::ModelOrProfileChange | Self::StopOrCancel => {
                ConversationChangeRoute::ScopeOrModelReplan
            }
        }
    }
}

/// Distinct routing path taken for an interpreted conversational change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationChangeRoute {
    /// Read-only observation route for questions and status inquiries; never mutates plans or tasks.
    ReadOnlyObservation,
    /// Checkpoint clarification route; holds clarification for a real workflow checkpoint without scope or model changes.
    CheckpointClarification,
    /// Scope or model replan route; invalidates advancing superseded work until a new plan revision and fresh approval exist.
    ScopeOrModelReplan,
}

impl ConversationChangeRoute {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnlyObservation => "read_only_observation",
            Self::CheckpointClarification => "checkpoint_clarification",
            Self::ScopeOrModelReplan => "scope_or_model_replan",
        }
    }
}

/// Typed routing decision outcome for a conversational change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationChangeDecisionKind {
    /// Side-effect-free observation record (`question_or_status`).
    RecordOnly,
    /// Pending clarification held for the next real workflow checkpoint (`clarification`).
    PendingClarificationAtCheckpoint,
    /// Active or planned work must stop/replan and obtain a fresh human approval before replacement work runs.
    CancelReplanNewApprovalRequired,
}

impl ConversationChangeDecisionKind {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecordOnly => "record_only",
            Self::PendingClarificationAtCheckpoint => "pending_clarification_at_checkpoint",
            Self::CancelReplanNewApprovalRequired => "cancel_replan_new_approval_required",
        }
    }
}

/// Lifecycle status of a persisted [`ConversationChangeRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationChangeStatus {
    /// Recorded as a read-only question/status observation; never blocks work.
    RecordedOnly,
    /// Unresolved clarification waiting to be incorporated at a verified workflow checkpoint.
    PendingClarification,
    /// Unresolved scope/model/stop change requiring a new published plan revision and fresh approval.
    PendingReplan,
    /// Explicitly resolved against a verified checkpoint or newly published/approved plan revision.
    Resolved,
}

impl ConversationChangeStatus {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecordedOnly => "recorded_only",
            Self::PendingClarification => "pending_clarification",
            Self::PendingReplan => "pending_replan",
            Self::Resolved => "resolved",
        }
    }

    /// Returns `true` when this status blocks advancing or dispatching affected work.
    pub const fn is_unresolved_blocking(self) -> bool {
        matches!(self, Self::PendingClarification | Self::PendingReplan)
    }
}

/// Requested delivery mode for a conversational change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationDeliveryRequest {
    /// Route through a safe workflow checkpoint or replan/re-approval (default).
    #[default]
    SafeCheckpointOrReplan,
    /// Prefer live in-flight worker continuation if supported, falling back honestly when unsupported.
    LiveContinuationIfSupported,
    /// Require verified live in-flight worker input delivery, failing closed when unsupported.
    RequireVerifiedLiveInput,
}

impl ConversationDeliveryRequest {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SafeCheckpointOrReplan => "safe_checkpoint_or_replan",
            Self::LiveContinuationIfSupported => "live_continuation_if_supported",
            Self::RequireVerifiedLiveInput => "require_verified_live_input",
        }
    }
}

/// Truthful live-input capability classification for the targeted execution route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveInputSupportStatus {
    /// Not applicable because the request is a read-only question or status observation.
    NotApplicableReadOnly,
    /// Current worker providers have no verified live in-flight input, pause, or resume path.
    UnsupportedNoLiveInputPath,
}

impl LiveInputSupportStatus {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicableReadOnly => "not_applicable_read_only",
            Self::UnsupportedNoLiveInputPath => "unsupported_no_live_input_path",
        }
    }
}

/// Truthful stop-proof classification for an approved task.
///
/// Distinguishes `CancelRequested` (cancellation requested, worker not yet proven exited)
/// from `ProvenStopped` (`ReadyForReview`, `Failed`, or `Cancelled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservedTaskStopState {
    /// Task has not been dispatched yet (`not_started`, `native_route_required`, or `dependency_inputs_unverified`).
    NotStarted,
    /// Worker is currently running (`running`).
    Running,
    /// Cancellation has been requested (`cancel_requested`), but worker termination is not yet proven.
    CancelRequested,
    /// Worker is proven stopped (`ready_for_review`, `failed`, or `cancelled`).
    ProvenStopped,
    /// Live worker state could not be verified (`unknown`).
    Unknown,
}

impl ObservedTaskStopState {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::ProvenStopped => "proven_stopped",
            Self::Unknown => "unknown",
        }
    }

    /// Returns `true` only when the task is proven not to be running (`NotStarted` or `ProvenStopped`).
    pub const fn is_proven_stopped(self) -> bool {
        matches!(self, Self::NotStarted | Self::ProvenStopped)
    }

    /// Maps a [`TaskExecutionStatus`] to its truthful [`ObservedTaskStopState`].
    pub const fn from_execution_status(status: TaskExecutionStatus) -> Self {
        match status {
            TaskExecutionStatus::NotStarted
            | TaskExecutionStatus::DependencyInputsUnverified
            | TaskExecutionStatus::DependencyInputsReady
            | TaskExecutionStatus::NativeRouteRequired => Self::NotStarted,
            TaskExecutionStatus::Queued
            | TaskExecutionStatus::Claimed
            | TaskExecutionStatus::Running => Self::Running,
            TaskExecutionStatus::CancelRequested => Self::CancelRequested,
            TaskExecutionStatus::ReadyForReview
            | TaskExecutionStatus::Failed
            | TaskExecutionStatus::Cancelled
            | TaskExecutionStatus::TimedOut => Self::ProvenStopped,
            TaskExecutionStatus::Unknown => Self::Unknown,
        }
    }
}

/// Explicit resolution mechanism for a pending conversational change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationChangeResolutionKind {
    /// Resolved by publishing a newer active plan revision (and recording a fresh approval when required).
    LinkedPlanRevision,
    /// Resolved at a verified workflow checkpoint without requiring a new plan revision (valid only for clarifications).
    CheckpointHandled,
}

impl ConversationChangeResolutionKind {
    /// Returns the canonical `snake_case` string representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinkedPlanRevision => "linked_plan_revision",
            Self::CheckpointHandled => "checkpoint_handled",
        }
    }
}

/// Optional typed cancellation request attached to a non-question conversational change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationCancelAction {
    /// Expected current task revision (`>= 1`).
    pub expected_task_revision: u64,
    /// Lowercase 64-character SHA-256 hex digest matching the frozen approval `subject_digest`.
    pub confirm_approval_digest: String,
    /// Explicit operator confirmation (`true` required).
    pub operator_approved: bool,
    /// Optional bounded cancellation reason (`<= 512` bytes, no control characters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Structured conversational change request submitted by native Codex on behalf of the operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangeRequest {
    /// Client-supplied idempotency key for safe retries (`[a-zA-Z0-9_-]{1,128}`).
    pub request_key: String,
    /// Expected current per-binding conversation-change store revision (`0` before the first record).
    pub expected_change_revision: u64,
    /// Target approval identifier (`apr_<32hex>`), if a plan revision has already been approved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Target plan identifier, if referencing a specific scoped plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Expected plan revision when `plan_id` or `approval_id` is supplied (`>= 1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_plan_revision: Option<u64>,
    /// Target task identifier within the plan or approval, if task-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Expected live task revision when inspecting or affecting a specific approved task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_task_revision: Option<u64>,
    /// Interpreted conversational change category.
    pub kind: ConversationChangeKind,
    /// Optional explicit route override; if provided, must match the route required by `kind` and proposed changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<ConversationChangeRoute>,
    /// Bounded single-line summary of the operator's conversational input (`1..=512` bytes).
    pub summary: String,
    /// Optional bounded detail or clarification note (`<= 4096` bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Optional proposed task write-scope paths (`repo-relative`); any expansion beyond frozen approval requires `ScopeOrModelReplan`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_write_paths: Vec<String>,
    /// Optional proposed profile identifier; any change from the approved profile requires `ScopeOrModelReplan`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_profile_id: Option<String>,
    /// Optional proposed model identifier; any change from the approved task model requires `ScopeOrModelReplan`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_model_id: Option<String>,
    /// Optional proposed reasoning effort; any change from the approved task effort requires `ScopeOrModelReplan`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_effort: Option<String>,
    /// Requested delivery mode (defaults to `safe_checkpoint_or_replan`).
    #[serde(default)]
    pub requested_delivery: ConversationDeliveryRequest,
    /// Optional typed cancellation request for the targeted approved task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_action: Option<ConversationCancelAction>,
}

/// Typed routing and capability decision computed for a conversational change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangeDecision {
    /// Policy version string (`conversation_change_routing_v1`).
    pub policy_version: String,
    /// Interpreted change kind.
    pub kind: ConversationChangeKind,
    /// Selected routing path (`read_only_observation`, `checkpoint_clarification`, or `scope_or_model_replan`).
    pub route: ConversationChangeRoute,
    /// Decision outcome kind (`record_only`, `pending_clarification_at_checkpoint`, or `cancel_replan_new_approval_required`).
    pub decision_kind: ConversationChangeDecisionKind,
    /// Resulting change record lifecycle status (`recorded_only`, `pending_clarification`, `pending_replan`, or `resolved`).
    pub status: ConversationChangeStatus,
    /// Requested delivery mode.
    pub requested_delivery: ConversationDeliveryRequest,
    /// Truthful live-input capability status for the targeted worker adapter (`not_applicable_read_only` or `unsupported_no_live_input_path`).
    pub live_input_status: LiveInputSupportStatus,
    /// Target task adapter when a specific approved task was inspected (`codex_cli`, `antigravity`, or `opencode_v2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_adapter: Option<ToolAdapter>,
    /// Observed live task execution status at evaluation time, if a task was inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_task_status: Option<TaskExecutionStatus>,
    /// Observed live task revision at evaluation time, if a task was inspected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_task_revision: Option<u64>,
    /// Observed task stop-proof classification (`not_started`, `running`, `cancel_requested`, `proven_stopped`, or `unknown`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_stop_state: Option<ObservedTaskStopState>,
    /// Whether the targeted task is proven not to be running (`true` only for `not_started` or `proven_stopped`, or when no task is targeted).
    pub prior_task_proven_stopped: bool,
    /// Whether a typed cancellation was requested during this operation.
    pub cancel_requested: bool,
    /// Whether publishing a new plan revision is required before replacement execution.
    pub replan_required: bool,
    /// Whether recording a fresh operator approval (`threadlane-approve`) is required before replacement execution.
    pub new_approval_required: bool,
    /// Whether this change blocks advancing or dispatching the targeted task/approval until resolved.
    pub blocks_task_advancement: bool,
    /// Whether frozen `ApprovalRecord` prompts, write scopes, and model pins remain immutable (`always true`).
    pub frozen_approval_immutable: bool,
    /// Whether any instruction was delivered live to an in-flight worker (`always false` for current providers).
    pub live_input_delivered: bool,
    /// Whether any worker process was paused in place (`always false` for current providers).
    pub process_paused: bool,
    /// Whether any worker provider session was resumed in place (`always false` for current providers).
    pub provider_resumed: bool,
    /// Safe human-readable explanation of the routing decision and required next operator action.
    pub rationale: String,
}

/// Side-effect-free preview of how a [`ConversationChangeRequest`] will be routed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangePreview {
    /// Authoritative binding identifier (`binding_id`).
    pub binding_id: String,
    /// Authoritative native Codex thread UUID (`thread.id`).
    pub thread_uuid: String,
    /// Current per-binding conversation-change store revision before recording.
    pub current_change_revision: u64,
    /// Next per-binding conversation-change store revision if recorded (`current_change_revision + 1`, or existing record's revision on idempotent replay).
    pub next_change_revision: u64,
    /// Whether `request_key` already matches an existing persisted record (idempotent replay).
    pub idempotent_replay: bool,
    /// Resolved approval identifier, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Resolved plan identifier, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Resolved plan revision, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Resolved task identifier, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Whether the request includes a validated `cancel_action` that would invoke `ApprovedHostClient::cancel_task` on `record-change`.
    pub will_invoke_cancel_task: bool,
    /// Computed routing and capability decision.
    pub decision: ConversationChangeDecision,
}

/// Explicit resolution metadata attached to a resolved [`ConversationChangeRecord`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangeResolution {
    /// Client-supplied idempotency key that resolved this change.
    pub resolution_request_key: String,
    /// Resolution mechanism (`linked_plan_revision` or `checkpoint_handled`).
    pub resolution_kind: ConversationChangeResolutionKind,
    /// Store revision created by this resolution.
    pub resolved_change_revision: u64,
    /// Linked active published plan revision when `resolution_kind == LinkedPlanRevision` (or current plan revision for `CheckpointHandled`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_plan_revision: Option<u64>,
    /// Compiled plan digest of `target_plan_revision` when verified against the plan store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_plan_digest: Option<String>,
    /// Linked fresh operator approval identifier (`apr_<32hex>`) when verified against the authority store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_approval_id: Option<String>,
    /// Observed task stop state at resolution time, if the change targeted an approved task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_stop_state: Option<ObservedTaskStopState>,
    /// Bounded operator resolution note (`<= 1024` bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_note: Option<String>,
    /// Unix epoch timestamp in milliseconds when the change was resolved.
    pub resolved_at_ms: i64,
}

/// Request to explicitly resolve a pending conversational change (`pending_clarification` or `pending_replan`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveConversationChangeRequest {
    /// Client-supplied idempotency key (`[a-zA-Z0-9_-]{1,128}`).
    pub request_key: String,
    /// Identifier of the change record to resolve (`chg_<32hex>`).
    pub change_id: String,
    /// Expected current per-binding conversation-change store revision (`>= 1`).
    pub expected_change_revision: u64,
    /// Resolution mechanism (`linked_plan_revision` or `checkpoint_handled`).
    pub resolution_kind: ConversationChangeResolutionKind,
    /// Target published plan revision (`> original plan_revision` when `replan_required` or `LinkedPlanRevision`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_plan_revision: Option<u64>,
    /// Optional fresh approval identifier (`apr_<32hex>`) for `target_plan_revision`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_approval_id: Option<String>,
    /// Optional bounded resolution note (`<= 1024` bytes, no control characters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_note: Option<String>,
}

/// Durable, binding-scoped conversational change record persisted under `<plans_dir>/conversation-changes/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangeRecord {
    /// Unique change identifier (`chg_<32hex>`).
    pub change_id: String,
    /// Client-supplied idempotency key (`[a-zA-Z0-9_-]{1,128}`).
    pub request_key: String,
    /// Store revision when this record was created (`>= 1`).
    pub created_change_revision: u64,
    /// Latest store revision that touched this record (`>= created_change_revision`).
    pub updated_change_revision: u64,
    /// Authoritative binding identifier (`binding_id`).
    pub binding_id: String,
    /// Authoritative native Codex thread UUID (`thread.id`).
    pub thread_uuid: String,
    /// Associated approval identifier (`apr_<32hex>`), if scoped to an approval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Associated frozen approval `subject_digest`, if scoped to an approval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject_digest: Option<String>,
    /// Associated plan identifier, if scoped to a plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Associated plan revision at the time the change was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Targeted task identifier, if task-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Downstream dependent task identifiers in the frozen approval DAG that are also blocked by this change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_dependent_task_ids: Vec<String>,
    /// Bounded single-line summary of the conversational change.
    pub summary: String,
    /// Optional bounded detail or clarification note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Proposed task write-scope paths included in the change request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_write_paths: Vec<String>,
    /// Proposed profile identifier included in the change request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_profile_id: Option<String>,
    /// Proposed model identifier included in the change request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_model_id: Option<String>,
    /// Proposed reasoning effort included in the change request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_effort: Option<String>,
    /// Computed routing and capability decision (updated when task cancellation or resolution occurs).
    pub decision: ConversationChangeDecision,
    /// Task state snapshot returned by `ApprovedHostClient::cancel_task` when `cancel_action` was executed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_task_snapshot: Option<TaskStateSnapshot>,
    /// Explicit resolution metadata once the change has been resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ConversationChangeResolution>,
    /// Unix epoch timestamp in milliseconds when the change was recorded.
    pub created_at_ms: i64,
    /// Unix epoch timestamp in milliseconds when the change was last updated.
    pub updated_at_ms: i64,
}

/// Query parameters for bounded listing of scoped conversational change records.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListConversationChangesParams {
    /// Optional approval identifier filter (`apr_<32hex>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Optional task identifier filter (matches direct `task_id`, `affected_dependent_task_ids`, or approval-wide changes with `task_id == None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// When `true`, returns only unresolved blocking changes (`pending_clarification` and `pending_replan`).
    #[serde(default)]
    pub unresolved_only: bool,
    /// Optional opaque cursor (`change_id`) from a prior page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Maximum number of change records to return (`1..=20`, default `20`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Compact descriptor of a single unresolved conversational change that blocks task or workflow advancement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedBlockingChange {
    /// Unique change identifier (`chg_<32hex>`).
    pub change_id: String,
    /// Store revision when the change was last updated.
    pub change_revision: u64,
    /// Change kind (`clarification`, `scope_change`, `model_or_profile_change`, or `stop_or_cancel`).
    pub kind: ConversationChangeKind,
    /// Selected route (`checkpoint_clarification` or `scope_or_model_replan`).
    pub route: ConversationChangeRoute,
    /// Unresolved status (`pending_clarification` or `pending_replan`).
    pub status: ConversationChangeStatus,
    /// Associated approval identifier, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Associated plan identifier, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// Associated plan revision, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_revision: Option<u64>,
    /// Directly targeted task identifier (`None` means approval/plan-wide).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Downstream dependent tasks in the approval DAG also blocked by this change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_dependent_task_ids: Vec<String>,
    /// Whether a new plan revision is required to resolve this change.
    pub replan_required: bool,
    /// Whether a fresh operator approval is required to resolve this change.
    pub new_approval_required: bool,
    /// Observed task stop state when the change was recorded or cancelled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_stop_state: Option<ObservedTaskStopState>,
    /// Whether the prior task was proven stopped at record time.
    pub prior_task_proven_stopped: bool,
    /// Bounded single-line summary of the change.
    pub summary: String,
}

/// Durable evidence that a resolved conversational change superseded an older frozen approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersededApprovalChange {
    /// Unique change identifier (`chg_<32hex>`).
    pub change_id: String,
    /// Store revision when the change was resolved.
    pub change_revision: u64,
    /// Change kind (`scope_change`, `model_or_profile_change`, `stop_or_cancel`, or `clarification`).
    pub kind: ConversationChangeKind,
    /// Selected route (`scope_or_model_replan` or `checkpoint_clarification`).
    pub route: ConversationChangeRoute,
    /// Superseded approval identifier (`apr_<32hex>`), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_approval_id: Option<String>,
    /// Associated plan identifier, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_plan_id: Option<String>,
    /// Superseded plan revision, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_plan_revision: Option<u64>,
    /// Directly targeted task identifier (`None` means approval/plan-wide).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Downstream dependent tasks in the old approval DAG also invalidated by this change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_dependent_task_ids: Vec<String>,
    /// Newly published target plan revision linked during resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_plan_revision: Option<u64>,
    /// Fresh operator approval identifier linked during resolution, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_approval_id: Option<String>,
    /// Bounded single-line summary of the change that superseded the old approval.
    pub summary: String,
}

/// Read-only summary of unresolved blocking conversational changes and superseded-approval evidence
/// for a binding, approval, or task.
///
/// Used by workflow and `profile-flow` controllers to refuse advancing invalidated work while
/// a clarification or replan is pending, or after an older frozen approval has been superseded
/// by a resolved replan to a newer plan revision and approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedChangeBlockSummary {
    /// Authoritative binding identifier (`binding_id`).
    pub binding_id: String,
    /// Filter approval identifier, if supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    /// Filter task identifier, if supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Current per-binding conversation-change store revision.
    pub current_change_revision: u64,
    /// `true` when at least one unresolved blocking change (`pending_clarification` or `pending_replan`) applies to this scope or when `approval_superseded` is `true`.
    pub blocked: bool,
    /// `true` when the queried approval has been durably superseded by a resolved replan to a newer plan revision/approval.
    pub approval_superseded: bool,
    /// `true` when any matching unresolved blocking change requires publishing a new plan revision.
    pub replan_required: bool,
    /// `true` when any matching unresolved blocking change requires a fresh operator approval.
    pub new_approval_required: bool,
    /// Unresolved blocking changes ordered from newest to oldest.
    pub blocking_changes: Vec<UnresolvedBlockingChange>,
    /// Resolved replan changes that durably superseded the queried approval (newest to oldest).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded_by_changes: Vec<SupersededApprovalChange>,
}

/// Bounded page of scoped conversational change records for a binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationChangePage {
    /// Authoritative binding identifier (`binding_id`).
    pub binding_id: String,
    /// Current per-binding conversation-change store revision.
    pub current_change_revision: u64,
    /// Bounded page of change records (newest first).
    pub changes: Vec<ConversationChangeRecord>,
    /// Opaque cursor for the next page when more records exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Summary of unresolved blocking changes matching the filter scope.
    pub unresolved_block_summary: UnresolvedChangeBlockSummary,
}

/// Sanitized, static-reason error type for conversation-change operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationChangeError {
    /// Request or parameter failed syntax, length, or invariant validation.
    Validation {
        /// Static human-safe validation failure description.
        reason: &'static str,
    },
    /// Store directory or file violated private `0700`/`0600` or non-symlink security rules.
    SecurityViolation {
        /// Static human-safe security violation description.
        reason: &'static str,
    },
    /// Current platform does not support Unix private file permissions.
    UnsupportedPlatform,
    /// Requested live-input delivery capability is unsupported by current worker providers.
    UnsupportedCapability {
        /// Static human-safe capability boundary description.
        reason: &'static str,
    },
    /// Change kind, route, or proposed scope/model mutation violated route separation rules.
    RouteMismatch {
        /// Static human-safe route mismatch description.
        reason: &'static str,
    },
    /// Binding, thread UUID, approval, or plan ownership did not match the authenticated scope.
    ScopeMismatch {
        /// Static human-safe scope mismatch description.
        reason: &'static str,
    },
    /// Approval confirmation digest did not match the frozen `subject_digest`.
    SubjectMismatch,
    /// Referenced approval record was not found in the authority store.
    ApprovalNotFound,
    /// Referenced plan or plan revision was not found in the plan store.
    PlanNotFound,
    /// Referenced task was not found in the plan or approval record.
    UnknownTask,
    /// Referenced plan revision is not the active published revision.
    UnpublishedOrSupersededPlan,
    /// Expected change, plan, or task revision did not match current state.
    StaleRevision {
        /// Static human-safe stale revision description.
        reason: &'static str,
    },
    /// Request key was reused with a different payload or target revision.
    IdempotencyConflict {
        /// Static human-safe idempotency conflict description.
        reason: &'static str,
    },
    /// Requested `change_id` was not found in the binding's ledger.
    ChangeNotFound,
    /// Change record is not in a valid state for the requested operation.
    InvalidChangeState {
        /// Static human-safe invalid state description.
        reason: &'static str,
    },
    /// Affected task is still `running` or `cancel_requested` and is not proven stopped.
    TaskNotProvenStopped {
        /// Static human-safe stop-proof refusal description.
        reason: &'static str,
    },
    /// Affected task state is `unknown` and cannot be verified without the host session.
    AmbiguousTaskState {
        /// Static human-safe ambiguous task state description.
        reason: &'static str,
    },
    /// Resolution requires linking a newly published plan revision.
    ReplanRequired {
        /// Static human-safe replan requirement description.
        reason: &'static str,
    },
    /// Resolution requires linking a fresh operator approval record.
    NewApprovalRequired {
        /// Static human-safe new approval requirement description.
        reason: &'static str,
    },
    /// A prior mutating cancellation operation failed ambiguously and blocks blind replay.
    AmbiguousOutcomePendingReconciliation,
    /// A mutating operation crossed an effect boundary before failing with an uncertain outcome.
    OutcomeUnknown {
        /// Static human-safe uncertain outcome description.
        reason: &'static str,
    },
    /// Approved host session is not running or ready for live task cancellation.
    HostNotReady,
    /// Approved host session or store lock is currently busy.
    LockBusy,
    /// Task is not in a valid run state for cancellation.
    InvalidRunState {
        /// Static human-safe invalid run state description.
        reason: &'static str,
    },
    /// Per-binding conversation-change ledger reached its bounded entry or byte limit.
    StoreLimitExceeded {
        /// Static human-safe store limit description.
        reason: &'static str,
    },
    /// Persisted conversation-change ledger failed integrity validation.
    CorruptStore {
        /// Static human-safe corrupt store description.
        reason: &'static str,
    },
    /// Filesystem I/O error while reading or writing the conversation-change store.
    Io {
        /// Static human-safe I/O failure description.
        reason: &'static str,
    },
}

impl ConversationChangeError {
    /// Returns the stable `snake_case` machine-readable error code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "invalid_input",
            Self::SecurityViolation { .. } => "security_violation",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::UnsupportedCapability { .. } => "unsupported_capability",
            Self::RouteMismatch { .. } => "route_mismatch",
            Self::ScopeMismatch { .. } => "scope_mismatch",
            Self::SubjectMismatch => "subject_mismatch",
            Self::ApprovalNotFound => "approval_not_found",
            Self::PlanNotFound => "plan_not_found",
            Self::UnknownTask => "unknown_task",
            Self::UnpublishedOrSupersededPlan => "unpublished_or_superseded_plan",
            Self::StaleRevision { .. } => "stale_revision",
            Self::IdempotencyConflict { .. } => "idempotency_conflict",
            Self::ChangeNotFound => "change_not_found",
            Self::InvalidChangeState { .. } => "invalid_change_state",
            Self::TaskNotProvenStopped { .. } => "task_not_proven_stopped",
            Self::AmbiguousTaskState { .. } => "ambiguous_task_state",
            Self::ReplanRequired { .. } => "replan_required",
            Self::NewApprovalRequired { .. } => "new_approval_required",
            Self::AmbiguousOutcomePendingReconciliation => {
                "ambiguous_outcome_pending_reconciliation"
            }
            Self::OutcomeUnknown { .. } => "outcome_unknown",
            Self::HostNotReady => "host_not_ready",
            Self::LockBusy => "lock_busy",
            Self::InvalidRunState { .. } => "invalid_run_state",
            Self::StoreLimitExceeded { .. } => "store_limit_exceeded",
            Self::CorruptStore { .. } => "corrupt_store",
            Self::Io { .. } => "io_error",
        }
    }

    /// Returns a static, sanitized user-safe message describing this error.
    pub const fn safe_message(&self) -> &'static str {
        match self {
            Self::Validation { reason }
            | Self::SecurityViolation { reason }
            | Self::UnsupportedCapability { reason }
            | Self::RouteMismatch { reason }
            | Self::ScopeMismatch { reason }
            | Self::StaleRevision { reason }
            | Self::IdempotencyConflict { reason }
            | Self::InvalidChangeState { reason }
            | Self::TaskNotProvenStopped { reason }
            | Self::AmbiguousTaskState { reason }
            | Self::ReplanRequired { reason }
            | Self::NewApprovalRequired { reason }
            | Self::OutcomeUnknown { reason }
            | Self::InvalidRunState { reason }
            | Self::StoreLimitExceeded { reason }
            | Self::CorruptStore { reason }
            | Self::Io { reason } => reason,
            Self::UnsupportedPlatform => {
                "Conversation change routing is only supported on Unix platforms."
            }
            Self::SubjectMismatch => {
                "Approval confirmation digest does not match the frozen approval record's subject_digest."
            }
            Self::ApprovalNotFound => {
                "Referenced approval record was not found in the authority store."
            }
            Self::PlanNotFound => "Referenced workflow plan revision was not found.",
            Self::UnknownTask => {
                "Referenced task was not found in the scoped workflow plan or approval record."
            }
            Self::UnpublishedOrSupersededPlan => {
                "Referenced workflow plan revision is not the active published revision."
            }
            Self::ChangeNotFound => {
                "Requested conversation change record was not found in this binding scope."
            }
            Self::AmbiguousOutcomePendingReconciliation => {
                "A prior conversation-change cancellation had an ambiguous outcome; inspect task and change state before retrying."
            }
            Self::HostNotReady => {
                "Approved host session is not running or has not reached authenticated ready state."
            }
            Self::LockBusy => "Approved host session or conversation-change lock is currently busy.",
        }
    }
}

impl fmt::Display for ConversationChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.safe_message())
    }
}

impl std::error::Error for ConversationChangeError {}

impl From<ConversationChangeError> for NativeError {
    fn from(err: ConversationChangeError) -> Self {
        let code = match err {
            ConversationChangeError::SecurityViolation { .. } => NativeErrorCode::SecurityViolation,
            ConversationChangeError::UnsupportedPlatform => NativeErrorCode::UnsupportedPlatform,
            ConversationChangeError::UnsupportedCapability { .. } => {
                NativeErrorCode::UnsupportedCapability
            }
            ConversationChangeError::AmbiguousOutcomePendingReconciliation
            | ConversationChangeError::OutcomeUnknown { .. } => {
                NativeErrorCode::AmbiguousTransportFailure
            }
            ConversationChangeError::StoreLimitExceeded { .. } => NativeErrorCode::OutputOverflow,
            ConversationChangeError::Io { .. } => NativeErrorCode::IoError,
            _ => NativeErrorCode::ValidationFailed,
        };
        NativeError::new(code, err.safe_message())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingCancelEffect {
    request_key: String,
    request_digest: String,
    expected_change_revision: u64,
    approval_id: String,
    task_id: String,
    expected_task_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedChangeLedger {
    schema_version: u32,
    binding_id: String,
    current_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ambiguous_cancels: Vec<PendingCancelEffect>,
    records: Vec<ConversationChangeRecord>,
}

impl PersistedChangeLedger {
    fn new(binding_id: &str) -> Self {
        Self {
            schema_version: CONVERSATION_CHANGE_SCHEMA_VERSION,
            binding_id: binding_id.to_string(),
            current_revision: 0,
            ambiguous_cancels: Vec::new(),
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct ObservedTaskContext {
    adapter: ToolAdapter,
    status: TaskExecutionStatus,
    revision: Option<u64>,
    stop_state: ObservedTaskStopState,
    dependent_task_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct ValidatedChangeEvaluation {
    approval_record: Option<ApprovalRecord>,
    plan_id: Option<String>,
    plan_revision: Option<u64>,
    task_id: Option<String>,
    dependent_task_ids: Vec<String>,
    summary: String,
    detail: Option<String>,
    proposed_write_paths: Vec<String>,
    proposed_profile_id: Option<String>,
    proposed_model_id: Option<String>,
    proposed_effort: Option<String>,
    cancel_reason: Option<String>,
    decision: ConversationChangeDecision,
}

/// Durable, binding-scoped store for conversational change records under `<plans_dir>/conversation-changes/`.
#[derive(Debug, Clone)]
pub struct ConversationChangeStore {
    plans_dir: PathBuf,
    namespace_dir: PathBuf,
}

impl ConversationChangeStore {
    /// Opens or initializes the `<plans_dir>/conversation-changes/` private (`0700`) store directory.
    pub fn open(plans_dir: impl AsRef<Path>) -> Result<Self, ConversationChangeError> {
        ensure_platform_supported().map_err(|_| ConversationChangeError::UnsupportedPlatform)?;
        let clean_plans_dir =
            prepare_private_dir(plans_dir.as_ref(), "plans_dir").map_err(map_fs_error)?;
        let namespace_dir = clean_plans_dir.join(CONVERSATION_CHANGE_NAMESPACE_DIR);
        let clean_namespace_dir = prepare_private_dir(&namespace_dir, "conversation_changes_dir")
            .map_err(map_fs_error)?;
        Ok(Self {
            plans_dir: clean_plans_dir,
            namespace_dir: clean_namespace_dir,
        })
    }

    /// Opens a read-only handle to `<plans_dir>/conversation-changes/` without creating
    /// directories or lock files.
    ///
    /// Validates that `plans_dir` is an existing private (`0700`) non-symlink directory and, if
    /// `<plans_dir>/conversation-changes/` already exists, that it is also a private (`0700`)
    /// non-symlink directory.
    pub fn open_readonly(plans_dir: impl AsRef<Path>) -> Result<Self, ConversationChangeError> {
        ensure_platform_supported().map_err(|_| ConversationChangeError::UnsupportedPlatform)?;
        let clean_plans_dir = validate_existing_private_dir_readonly(plans_dir.as_ref())?;
        let namespace_dir = clean_plans_dir.join(CONVERSATION_CHANGE_NAMESPACE_DIR);
        let _ = validate_optional_namespace_dir_readonly(&namespace_dir)?;
        Ok(Self {
            plans_dir: clean_plans_dir,
            namespace_dir,
        })
    }

    /// Returns the validated `plans_dir` root path.
    pub fn plans_dir(&self) -> &Path {
        &self.plans_dir
    }

    /// Returns the validated `<plans_dir>/conversation-changes` directory path.
    pub fn namespace_dir(&self) -> &Path {
        &self.namespace_dir
    }

    /// Returns the current conversation-change store revision for `binding_id` (`0` when no changes exist).
    pub fn current_revision(&self, binding_id: &str) -> Result<u64, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        let ledger = self.read_ledger_readonly(&checked_binding)?;
        Ok(ledger.current_revision)
    }

    /// Looks up a single [`ConversationChangeRecord`] by `change_id` within `binding_id`.
    pub fn get_change(
        &self,
        binding_id: &str,
        change_id: &str,
    ) -> Result<Option<ConversationChangeRecord>, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        validate_change_id(change_id)?;
        let ledger = self.read_ledger_readonly(&checked_binding)?;
        Ok(ledger
            .records
            .into_iter()
            .find(|record| record.change_id == change_id))
    }

    /// Lists a bounded page of conversational change records for `binding_id` (newest first).
    pub fn list_changes(
        &self,
        binding_id: &str,
        params: &ListConversationChangesParams,
    ) -> Result<ConversationChangePage, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        if let Some(ref approval_id) = params.approval_id {
            validate_approval_id(approval_id)?;
        }
        if let Some(ref task_id) = params.task_id {
            validate_task_id(task_id)?;
        }
        let limit = params
            .limit
            .unwrap_or(DEFAULT_CONVERSATION_CHANGE_LIST_LIMIT);
        if !(1..=MAX_CONVERSATION_CHANGE_LIST_LIMIT).contains(&limit) {
            return Err(ConversationChangeError::Validation {
                reason: "Conversation change list limit must be between 1 and 20.",
            });
        }
        if let Some(ref cursor) = params.cursor {
            if cursor.is_empty()
                || cursor.trim() != cursor
                || cursor.len() > MAX_CONVERSATION_CHANGE_CURSOR_LEN
                || cursor.chars().any(char::is_control)
            {
                return Err(ConversationChangeError::Validation {
                    reason: "Invalid conversation change pagination cursor.",
                });
            }
            validate_change_id(cursor)?;
        }

        let ledger = self.read_ledger_readonly(&checked_binding)?;
        refuse_pending_cancel_observation(&ledger, params.approval_id.as_deref())?;
        let summary = build_unresolved_summary(
            &checked_binding,
            ledger.current_revision,
            &ledger.records,
            params.approval_id.as_deref(),
            params.task_id.as_deref(),
        );

        let filtered: Vec<ConversationChangeRecord> = ledger
            .records
            .iter()
            .rev()
            .filter(|record| {
                record_matches_scope(
                    record,
                    params.approval_id.as_deref(),
                    params.task_id.as_deref(),
                ) && (!params.unresolved_only || record.decision.status.is_unresolved_blocking())
            })
            .cloned()
            .collect();

        let start_idx = if let Some(ref cursor) = params.cursor {
            let pos = filtered.iter().position(|r| &r.change_id == cursor).ok_or(
                ConversationChangeError::Validation {
                    reason: "Pagination cursor was not found in the filtered change list.",
                },
            )?;
            pos + 1
        } else {
            0
        };

        let page_slice: Vec<ConversationChangeRecord> = filtered
            .iter()
            .skip(start_idx)
            .take(limit)
            .cloned()
            .collect();
        let next_cursor = if start_idx + page_slice.len() < filtered.len() {
            page_slice.last().map(|r| r.change_id.clone())
        } else {
            None
        };

        Ok(ConversationChangePage {
            binding_id: checked_binding,
            current_change_revision: ledger.current_revision,
            changes: page_slice,
            next_cursor,
            unresolved_block_summary: summary,
        })
    }

    /// Returns a read-only summary of unresolved blocking changes (`pending_clarification` and
    /// `pending_replan`) and superseded-approval evidence for `binding_id`, optionally filtered to
    /// `approval_id` and `task_id`.
    pub fn unresolved_blocking_changes(
        &self,
        binding_id: &str,
        approval_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<UnresolvedChangeBlockSummary, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        if let Some(apr_id) = approval_id {
            validate_approval_id(apr_id)?;
        }
        if let Some(tsk_id) = task_id {
            validate_task_id(tsk_id)?;
        }
        let ledger = self.read_ledger_readonly(&checked_binding)?;
        refuse_pending_cancel_observation(&ledger, approval_id)?;
        Ok(build_unresolved_summary(
            &checked_binding,
            ledger.current_revision,
            &ledger.records,
            approval_id,
            task_id,
        ))
    }

    /// Evaluates a [`ConversationChangeRequest`] against live binding, plan, authority, and host
    /// state without mutating stores or cancelling tasks.
    pub async fn preview_change(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        request: &ConversationChangeRequest,
        plans: &PlanStore,
        authority: &AuthorityStore,
        host_client: &ApprovedHostClient,
    ) -> Result<ConversationChangePreview, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        let checked_thread = validate_thread_uuid(thread_uuid)?;
        verify_client_scope(host_client, &checked_binding, &checked_thread)?;
        validate_request_key(&request.request_key)?;
        validate_revision("expected_change_revision", request.expected_change_revision)?;

        let request_digest = compute_request_digest(&checked_binding, &checked_thread, request)?;

        let ledger = self.read_ledger_readonly(&checked_binding)?;

        if ledger.ambiguous_cancels.iter().any(|p| {
            p.request_key == request.request_key
                || (request.task_id.is_some()
                    && request.task_id.as_deref() == Some(p.task_id.as_str()))
        }) {
            return Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation);
        }

        if let Some(existing) = ledger
            .records
            .iter()
            .find(|r| r.request_key == request.request_key)
        {
            verify_idempotent_record_match(
                existing,
                &checked_binding,
                &checked_thread,
                request,
                &request_digest,
                ledger.current_revision,
            )?;
            return Ok(ConversationChangePreview {
                binding_id: checked_binding,
                thread_uuid: checked_thread,
                current_change_revision: ledger.current_revision,
                next_change_revision: existing.created_change_revision,
                idempotent_replay: true,
                approval_id: existing.approval_id.clone(),
                plan_id: existing.plan_id.clone(),
                plan_revision: existing.plan_revision,
                task_id: existing.task_id.clone(),
                will_invoke_cancel_task: false,
                decision: existing.decision.clone(),
            });
        }

        if ledger.records.iter().any(|r| {
            r.resolution
                .as_ref()
                .map(|res| res.resolution_request_key.as_str())
                == Some(request.request_key.as_str())
        }) {
            return Err(ConversationChangeError::IdempotencyConflict {
                reason: "request_key was already used for a change resolution operation.",
            });
        }

        if request.expected_change_revision != ledger.current_revision {
            return Err(ConversationChangeError::StaleRevision {
                reason: "expected_change_revision does not match current conversation-change store revision.",
            });
        }

        let eval = evaluate_change_request(
            &checked_binding,
            &checked_thread,
            request,
            plans,
            authority,
            host_client,
        )
        .await?;

        Ok(ConversationChangePreview {
            binding_id: checked_binding,
            thread_uuid: checked_thread,
            current_change_revision: ledger.current_revision,
            next_change_revision: ledger.current_revision.saturating_add(1),
            idempotent_replay: false,
            approval_id: eval.approval_record.as_ref().map(|a| a.approval_id.clone()),
            plan_id: eval.plan_id,
            plan_revision: eval.plan_revision,
            task_id: eval.task_id,
            will_invoke_cancel_task: request.cancel_action.is_some(),
            decision: eval.decision,
        })
    }

    /// Persists a scoped [`ConversationChangeRecord`] with `expected_change_revision` compare-and-swap
    /// and `request_key` idempotency, optionally invoking `ApprovedHostClient::cancel_task` when
    /// `cancel_action` is present.
    pub async fn record_change(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        request: &ConversationChangeRequest,
        plans: &PlanStore,
        authority: &AuthorityStore,
        host_client: &ApprovedHostClient,
    ) -> Result<ConversationChangeRecord, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        let checked_thread = validate_thread_uuid(thread_uuid)?;
        verify_client_scope(host_client, &checked_binding, &checked_thread)?;
        validate_request_key(&request.request_key)?;
        validate_revision("expected_change_revision", request.expected_change_revision)?;

        let request_digest = compute_request_digest(&checked_binding, &checked_thread, request)?;

        let precheck = self.with_binding_lock(&checked_binding, |ledger_path| {
            let ledger = read_ledger_locked(ledger_path, &checked_binding)?;
            if ledger.ambiguous_cancels.iter().any(|p| {
                p.request_key == request.request_key
                    || (request.task_id.is_some()
                        && request.task_id.as_deref() == Some(p.task_id.as_str()))
            }) {
                return Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation);
            }
            if let Some(existing) = ledger
                .records
                .iter()
                .find(|r| r.request_key == request.request_key)
            {
                verify_idempotent_record_match(
                    existing,
                    &checked_binding,
                    &checked_thread,
                    request,
                    &request_digest,
                    ledger.current_revision,
                )?;
                return Ok(Some(existing.clone()));
            }
            if ledger.records.iter().any(|r| {
                r.resolution
                    .as_ref()
                    .map(|res| res.resolution_request_key.as_str())
                    == Some(request.request_key.as_str())
            }) {
                return Err(ConversationChangeError::IdempotencyConflict {
                    reason: "request_key was already used for a change resolution operation.",
                });
            }
            if request.expected_change_revision != ledger.current_revision {
                return Err(ConversationChangeError::StaleRevision {
                    reason: "expected_change_revision does not match current conversation-change store revision.",
                });
            }
            if ledger.records.len() >= MAX_CONVERSATION_CHANGES_PER_BINDING {
                return Err(ConversationChangeError::StoreLimitExceeded {
                    reason: "Conversation change ledger reached the maximum of 256 records for this binding.",
                });
            }
            Ok(None)
        })?;

        if let Some(idempotent_record) = precheck {
            return Ok(idempotent_record);
        }

        let mut eval = evaluate_change_request(
            &checked_binding,
            &checked_thread,
            request,
            plans,
            authority,
            host_client,
        )
        .await?;

        let mut cancelled_snapshot: Option<TaskStateSnapshot> = None;

        if let Some(ref cancel_action) = request.cancel_action {
            let approval_id = eval
                .approval_record
                .as_ref()
                .map(|a| a.approval_id.clone())
                .ok_or(ConversationChangeError::Validation {
                    reason: "cancel_action requires a valid approval_id.",
                })?;
            let task_id = eval
                .task_id
                .clone()
                .ok_or(ConversationChangeError::Validation {
                    reason: "cancel_action requires a valid task_id.",
                })?;

            // Persist a pending cancel effect marker before crossing the host RPC boundary so
            // ambiguous failures cannot be blindly replayed.
            self.with_binding_lock(&checked_binding, |ledger_path| {
                let mut ledger = read_ledger_locked(ledger_path, &checked_binding)?;
                if request.expected_change_revision != ledger.current_revision {
                    return Err(ConversationChangeError::StaleRevision {
                        reason: "expected_change_revision does not match current conversation-change store revision.",
                    });
                }
                // The earlier precheck precedes an await. Reserve the effect
                // under this same exclusive ledger lock before any cancel RPC.
                if ledger.ambiguous_cancels.iter().any(|pending| {
                    pending.request_key == request.request_key
                        || (pending.approval_id == approval_id && pending.task_id == task_id)
                }) {
                    return Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation);
                }
                ledger.ambiguous_cancels.push(PendingCancelEffect {
                    request_key: request.request_key.clone(),
                    request_digest: request_digest.clone(),
                    expected_change_revision: request.expected_change_revision,
                    approval_id: approval_id.clone(),
                    task_id: task_id.clone(),
                    expected_task_revision: cancel_action.expected_task_revision,
                });
                write_ledger_locked(&self.namespace_dir, ledger_path, &ledger)
            })?;

            match host_client
                .cancel_task(
                    &approval_id,
                    &task_id,
                    cancel_action.expected_task_revision,
                    eval.cancel_reason.clone(),
                )
                .await
            {
                Ok(snapshot) => {
                    if snapshot.binding_id != checked_binding
                        || snapshot.thread_uuid != checked_thread
                    {
                        return Err(ConversationChangeError::OutcomeUnknown {
                            reason: "Task cancellation completed, but returned snapshot scope failed validation.",
                        });
                    }
                    let post_stop = ObservedTaskStopState::from_execution_status(snapshot.status);
                    eval.decision.observed_task_status = Some(snapshot.status);
                    eval.decision.observed_task_revision =
                        snapshot.worker.as_ref().map(|w| w.revision);
                    eval.decision.observed_stop_state = Some(post_stop);
                    eval.decision.prior_task_proven_stopped = post_stop.is_proven_stopped();
                    eval.decision.cancel_requested = true;
                    eval.decision.rationale = build_decision_rationale(
                        eval.decision.kind,
                        eval.decision.route,
                        eval.decision.observed_stop_state,
                        true,
                    );
                    cancelled_snapshot = Some(snapshot);
                }
                Err(host_err) => {
                    if is_deterministic_pre_effect_host_error(&host_err) {
                        let _ = self.with_binding_lock(&checked_binding, |ledger_path| {
                            let mut ledger = read_ledger_locked(ledger_path, &checked_binding)?;
                            ledger
                                .ambiguous_cancels
                                .retain(|p| p.request_key != request.request_key);
                            write_ledger_locked(&self.namespace_dir, ledger_path, &ledger)
                        });
                        return Err(map_host_cancel_error(&host_err));
                    }
                    return Err(ConversationChangeError::OutcomeUnknown {
                        reason: "Task cancellation outcome is uncertain after a runtime or I/O failure; inspect task and change state before retrying.",
                    });
                }
            }
        }

        let now_ms = now_epoch_ms();
        let change_id = generate_change_id();

        self.with_binding_lock(&checked_binding, |ledger_path| {
            let mut ledger = read_ledger_locked(ledger_path, &checked_binding)?;
            ledger
                .ambiguous_cancels
                .retain(|p| p.request_key != request.request_key);

            if request.expected_change_revision != ledger.current_revision {
                if cancelled_snapshot.is_some() {
                    return Err(ConversationChangeError::OutcomeUnknown {
                        reason: "Task cancellation completed, but conversation-change store revision advanced concurrently.",
                    });
                }
                return Err(ConversationChangeError::StaleRevision {
                    reason: "expected_change_revision does not match current conversation-change store revision.",
                });
            }
            if ledger.records.len() >= MAX_CONVERSATION_CHANGES_PER_BINDING {
                return Err(ConversationChangeError::StoreLimitExceeded {
                    reason: "Conversation change ledger reached the maximum of 256 records for this binding.",
                });
            }

            let next_revision = ledger.current_revision.saturating_add(1);
            let record = ConversationChangeRecord {
                change_id,
                request_key: request.request_key.clone(),
                created_change_revision: next_revision,
                updated_change_revision: next_revision,
                binding_id: checked_binding.clone(),
                thread_uuid: checked_thread.clone(),
                approval_id: eval.approval_record.as_ref().map(|a| a.approval_id.clone()),
                approval_subject_digest: eval
                    .approval_record
                    .as_ref()
                    .map(|a| a.subject_digest.clone()),
                plan_id: eval.plan_id,
                plan_revision: eval.plan_revision,
                task_id: eval.task_id,
                affected_dependent_task_ids: eval.dependent_task_ids,
                summary: eval.summary,
                detail: eval.detail,
                proposed_write_paths: eval.proposed_write_paths,
                proposed_profile_id: eval.proposed_profile_id,
                proposed_model_id: eval.proposed_model_id,
                proposed_effort: eval.proposed_effort,
                decision: eval.decision,
                cancelled_task_snapshot: cancelled_snapshot.clone(),
                resolution: None,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
            };

            ledger.current_revision = next_revision;
            ledger.records.push(record.clone());
            if let Err(write_err) = write_ledger_locked(&self.namespace_dir, ledger_path, &ledger) {
                if cancelled_snapshot.is_some() {
                    return Err(ConversationChangeError::OutcomeUnknown {
                        reason: "Task cancellation completed, but persisting the conversation-change record failed.",
                    });
                }
                return Err(write_err);
            }
            Ok(record)
        })
    }

    /// Explicitly resolves a `pending_clarification` or `pending_replan` change after verifying
    /// that affected tasks are proven stopped and that any required new plan revision and fresh
    /// approval exist.
    pub async fn resolve_change(
        &self,
        binding_id: &str,
        thread_uuid: &str,
        request: &ResolveConversationChangeRequest,
        plans: &PlanStore,
        authority: &AuthorityStore,
        host_client: &ApprovedHostClient,
    ) -> Result<ConversationChangeRecord, ConversationChangeError> {
        let checked_binding = validate_binding_id(binding_id)?;
        let checked_thread = validate_thread_uuid(thread_uuid)?;
        verify_client_scope(host_client, &checked_binding, &checked_thread)?;
        validate_request_key(&request.request_key)?;
        validate_change_id(&request.change_id)?;
        if request.expected_change_revision == 0 {
            return Err(ConversationChangeError::Validation {
                reason: "expected_change_revision must be >= 1 when resolving a change.",
            });
        }
        validate_revision("expected_change_revision", request.expected_change_revision)?;
        if let Some(target_rev) = request.target_plan_revision {
            if target_rev == 0 {
                return Err(ConversationChangeError::Validation {
                    reason: "target_plan_revision must be >= 1 when provided.",
                });
            }
            validate_revision("target_plan_revision", target_rev)?;
        }
        if let Some(ref new_apr_id) = request.new_approval_id {
            validate_approval_id(new_apr_id)?;
        }
        let clean_note = validate_optional_text(
            request.resolution_note.as_deref(),
            MAX_CONVERSATION_CHANGE_RESOLUTION_NOTE_BYTES,
            "resolution_note exceeds 1024 bytes or contains control characters.",
        )?;

        let existing_record = self.with_binding_lock(&checked_binding, |ledger_path| {
            let ledger = read_ledger_locked(ledger_path, &checked_binding)?;
            if !ledger.ambiguous_cancels.is_empty() {
                return Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation);
            }
            let record = ledger
                .records
                .iter()
                .find(|r| r.change_id == request.change_id)
                .cloned()
                .ok_or(ConversationChangeError::ChangeNotFound)?;

            if record.binding_id != checked_binding || record.thread_uuid != checked_thread {
                return Err(ConversationChangeError::ScopeMismatch {
                    reason: "Conversation change record does not belong to the requested binding and thread scope.",
                });
            }

            if let Some(ref existing_res) = record.resolution {
                if existing_res.resolution_request_key == request.request_key {
                    let expected_matches = request.expected_change_revision
                        == existing_res.resolved_change_revision.saturating_sub(1)
                        || request.expected_change_revision == ledger.current_revision;
                    if expected_matches
                        && existing_res.resolution_kind == request.resolution_kind
                        && (request.target_plan_revision.is_none()
                            || existing_res.target_plan_revision == request.target_plan_revision)
                        && existing_res.new_approval_id == request.new_approval_id
                        && existing_res.resolution_note == clean_note
                    {
                        return Ok((true, record));
                    }
                    return Err(ConversationChangeError::IdempotencyConflict {
                        reason: "Resolution request_key was already used with different resolution parameters.",
                    });
                }
                return Err(ConversationChangeError::InvalidChangeState {
                    reason: "Conversation change record is already resolved.",
                });
            }

            if ledger.records.iter().any(|r| {
                r.request_key == request.request_key
                    || r.resolution
                        .as_ref()
                        .map(|res| res.resolution_request_key.as_str())
                        == Some(request.request_key.as_str())
            }) {
                return Err(ConversationChangeError::IdempotencyConflict {
                    reason: "request_key was already used by another conversation change operation.",
                });
            }

            if record.decision.status == ConversationChangeStatus::RecordedOnly {
                return Err(ConversationChangeError::InvalidChangeState {
                    reason: "Question or status observation records are read-only and cannot be resolved.",
                });
            }

            if request.expected_change_revision != ledger.current_revision {
                return Err(ConversationChangeError::StaleRevision {
                    reason: "expected_change_revision does not match current conversation-change store revision.",
                });
            }

            Ok((false, record))
        })?;

        if existing_record.0 {
            return Ok(existing_record.1);
        }
        let record = existing_record.1;

        // A plan-only record can also cover already-approved executions. Omitting the
        // approval ID must not hide those live tasks or their affected dependents.
        let approvals = if let Some(ref approval_id) = record.approval_id {
            vec![load_scoped_approval(
                authority,
                approval_id,
                &checked_binding,
                &checked_thread,
            )?]
        } else if let Some(ref plan_id) = record.plan_id {
            let summaries = authority
                .list_for_binding(&checked_binding)
                .map_err(|err| map_authority_to_change_error(&err))?;
            if summaries.len() > 256 {
                return Err(ConversationChangeError::Validation {
                    reason: "Approval scan exceeded the bounded resolution limit.",
                });
            }
            summaries
                .into_iter()
                .filter(|a| a.plan_id == *plan_id && Some(a.plan_revision) == record.plan_revision)
                .map(|a| {
                    load_scoped_approval(
                        authority,
                        &a.approval_id,
                        &checked_binding,
                        &checked_thread,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let mut aggregate_stop = ObservedTaskStopState::NotStarted;
        for approval in &approvals {
            for task in &approval.tasks {
                if let Some(ref root) = record.task_id {
                    if task.task_id != *root
                        && !record.affected_dependent_task_ids.contains(&task.task_id)
                    {
                        continue;
                    }
                }
                let task_ctx = observe_approved_task(
                    approval,
                    &task.task_id,
                    host_client,
                    &checked_binding,
                    &checked_thread,
                )
                .await?;
                ensure_task_proven_stopped_for_resolution(
                    &record,
                    request.resolution_kind,
                    task_ctx.status,
                    task_ctx.stop_state,
                )?;
                if task_ctx.stop_state == ObservedTaskStopState::ProvenStopped {
                    aggregate_stop = ObservedTaskStopState::ProvenStopped;
                }
            }
        }
        let verified_stop_state = (!approvals.is_empty()).then_some(aggregate_stop);

        let (resolved_plan_rev, resolved_plan_digest, verified_new_approval_id) = match request
            .resolution_kind
        {
            ConversationChangeResolutionKind::CheckpointHandled => {
                if record.decision.replan_required {
                    return Err(ConversationChangeError::ReplanRequired {
                            reason: "Scope, model, or stop changes require publishing a new plan revision; use linked_plan_revision.",
                        });
                }
                if record.decision.new_approval_required {
                    return Err(ConversationChangeError::NewApprovalRequired {
                            reason: "This change requires a fresh operator approval; use linked_plan_revision.",
                        });
                }
                if request.new_approval_id.is_some() {
                    return Err(ConversationChangeError::Validation {
                            reason: "checkpoint_handled resolution cannot specify new_approval_id; use linked_plan_revision.",
                        });
                }
                if let (Some(orig_rev), Some(target_rev)) =
                    (record.plan_revision, request.target_plan_revision)
                {
                    if target_rev != orig_rev {
                        return Err(ConversationChangeError::Validation {
                                reason: "checkpoint_handled resolution must not change plan_revision; use linked_plan_revision.",
                            });
                    }
                }
                (record.plan_revision, None, None)
            }
            ConversationChangeResolutionKind::LinkedPlanRevision => {
                let target_rev =
                    request
                        .target_plan_revision
                        .ok_or(ConversationChangeError::Validation {
                            reason:
                                "linked_plan_revision resolution requires target_plan_revision.",
                        })?;
                if let Some(orig_rev) = record.plan_revision {
                    if target_rev <= orig_rev {
                        return Err(ConversationChangeError::ReplanRequired {
                                reason: "target_plan_revision must be strictly greater than the superseded plan revision.",
                            });
                    }
                }

                let new_approval = if let Some(ref new_apr_id) = request.new_approval_id {
                    if record.approval_id.as_deref() == Some(new_apr_id.as_str()) {
                        return Err(ConversationChangeError::NewApprovalRequired {
                                reason: "new_approval_id must reference a fresh approval record, not the superseded approval_id.",
                            });
                    }
                    let loaded = load_scoped_approval(
                        authority,
                        new_apr_id,
                        &checked_binding,
                        &checked_thread,
                    )?;
                    Some(loaded)
                } else if record.decision.new_approval_required {
                    return Err(ConversationChangeError::NewApprovalRequired {
                            reason: "Resolving a scope or model replan requires new_approval_id for the newly approved plan revision.",
                        });
                } else {
                    None
                };

                let plan_id = record
                    .plan_id
                    .clone()
                    .or_else(|| new_approval.as_ref().map(|a| a.plan_id.clone()))
                    .ok_or(ConversationChangeError::Validation {
                        reason: "linked_plan_revision resolution requires a scoped plan_id.",
                    })?;

                let plan_rev = plans
                    .get_revision(&plan_id, target_rev)
                    .map_err(|err| map_plan_store_error(&err))?
                    .ok_or(ConversationChangeError::PlanNotFound)?;
                let latest_rev = plans
                    .latest_revision(&plan_id)
                    .map_err(|err| map_plan_store_error(&err))?
                    .ok_or(ConversationChangeError::PlanNotFound)?;
                if plan_rev.binding_id != checked_binding
                    || latest_rev.binding_id != checked_binding
                {
                    return Err(ConversationChangeError::ScopeMismatch {
                        reason: "Target workflow plan does not belong to the requested binding.",
                    });
                }
                if !plan_rev.status.is_active_published() || latest_rev.revision != target_rev {
                    return Err(ConversationChangeError::UnpublishedOrSupersededPlan);
                }

                let plan_digest = if let Some(ref apr) = new_approval {
                    if apr.plan_id != plan_id || apr.plan_revision != target_rev {
                        return Err(ConversationChangeError::Validation {
                                reason: "new_approval_id does not match the target plan_id and target_plan_revision.",
                            });
                    }
                    let reg_snapshot = authority.registry().snapshot().map_err(|_| {
                            ConversationChangeError::CorruptStore {
                                reason: "Failed to read workflow registry snapshot during approval verification.",
                            }
                        })?;
                    if apr.registry_revision != plan_rev.profile_revision
                        || reg_snapshot.revision != apr.registry_revision
                    {
                        return Err(ConversationChangeError::StaleRevision {
                                reason: "new_approval_id registry_revision does not match the active registry and plan revision.",
                            });
                    }
                    let compiled = compile_plan_revision(&plan_rev, &reg_snapshot)
                        .map_err(|err| map_plan_store_error(&err))?;
                    if compiled.plan_digest != apr.plan_digest {
                        return Err(ConversationChangeError::StaleRevision {
                                reason: "new_approval_id plan_digest does not match the compiled target plan revision.",
                            });
                    }
                    Some(apr.plan_digest.clone())
                } else {
                    Some(compute_plan_revision_digest(&plan_rev)?)
                };

                (
                    Some(target_rev),
                    plan_digest,
                    new_approval.map(|a| a.approval_id),
                )
            }
        };

        let now_ms = now_epoch_ms();
        self.with_binding_lock(&checked_binding, |ledger_path| {
            let mut ledger = read_ledger_locked(ledger_path, &checked_binding)?;
            if request.expected_change_revision != ledger.current_revision {
                return Err(ConversationChangeError::StaleRevision {
                    reason: "expected_change_revision does not match current conversation-change store revision.",
                });
            }
            let next_revision = ledger.current_revision.saturating_add(1);
            let target = ledger
                .records
                .iter_mut()
                .find(|r| r.change_id == request.change_id)
                .ok_or(ConversationChangeError::ChangeNotFound)?;

            if target.resolution.is_some() {
                return Err(ConversationChangeError::InvalidChangeState {
                    reason: "Conversation change record is already resolved.",
                });
            }

            target.updated_change_revision = next_revision;
            target.updated_at_ms = now_ms;
            target.decision.status = ConversationChangeStatus::Resolved;
            target.decision.blocks_task_advancement = false;
            if let Some(stop_state) = verified_stop_state {
                target.decision.observed_stop_state = Some(stop_state);
                target.decision.prior_task_proven_stopped = stop_state.is_proven_stopped();
            }
            target.resolution = Some(ConversationChangeResolution {
                resolution_request_key: request.request_key.clone(),
                resolution_kind: request.resolution_kind,
                resolved_change_revision: next_revision,
                target_plan_revision: resolved_plan_rev,
                target_plan_digest: resolved_plan_digest,
                new_approval_id: verified_new_approval_id,
                verified_stop_state,
                resolution_note: clean_note,
                resolved_at_ms: now_ms,
            });

            let resolved_clone = target.clone();
            ledger.current_revision = next_revision;
            write_ledger_locked(&self.namespace_dir, ledger_path, &ledger)?;
            Ok(resolved_clone)
        })
    }

    fn read_ledger_readonly(
        &self,
        binding_id: &str,
    ) -> Result<PersistedChangeLedger, ConversationChangeError> {
        let _ = validate_existing_private_dir_readonly(&self.plans_dir)?;
        if !validate_optional_namespace_dir_readonly(&self.namespace_dir)? {
            return Ok(PersistedChangeLedger::new(binding_id));
        }
        let (ledger_path, _) = ledger_paths_for_binding(&self.namespace_dir, binding_id)?;
        read_ledger_locked(&ledger_path, binding_id)
    }

    fn with_binding_lock<T, F>(&self, binding_id: &str, f: F) -> Result<T, ConversationChangeError>
    where
        F: FnOnce(&Path) -> Result<T, ConversationChangeError>,
    {
        let _ = prepare_private_dir(&self.namespace_dir, "conversation_changes_dir")
            .map_err(map_fs_error)?;
        let (ledger_path, lock_filename) =
            ledger_paths_for_binding(&self.namespace_dir, binding_id)?;
        let lock_key = self.namespace_dir.join(&lock_filename);
        let arc_mutex = {
            let map_mutex = IN_PROCESS_CHANGE_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
            let mut map = map_mutex
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Arc::clone(
                map.entry(lock_key)
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _in_proc_guard = arc_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file_lock_guard = acquire_cross_process_lock(
            &self.namespace_dir,
            &lock_filename,
            "conversation_change_lock",
        )
        .map_err(map_fs_error)?;
        f(&ledger_path)
    }
}

/// Serializes role advancement with the binding's actual conversation-change mutations.
/// The guard must remain alive through dispatch; preflight and observation never acquire it.
pub(crate) async fn acquire_conversation_advancement_guard(
    plans_dir: &Path,
    binding_id: &str,
) -> Result<crate::fs_safety::CrossProcessLockGuard, NativeError> {
    let plans_dir = plans_dir.to_path_buf();
    let binding_id = binding_id.to_string();
    tokio::task::spawn_blocking(move || -> Result<_, NativeError> {
        let store = ConversationChangeStore::open(&plans_dir)?;
        validate_binding_id(&binding_id)?;
        let (_, filename) = ledger_paths_for_binding(&store.namespace_dir, &binding_id)?;
        acquire_cross_process_lock(
            &store.namespace_dir,
            &filename,
            "conversation_advancement_lock",
        )
    })
    .await
    .map_err(|_| {
        NativeError::new(
            NativeErrorCode::IoError,
            "Conversation advancement lock acquisition failed.",
        )
    })?
}

/// Reads blocking changes without creating a namespace or acquiring a mutation lock.
pub fn inspect_unresolved_blocking_changes(
    plans_dir: &Path,
    binding_id: &str,
    approval_id: Option<&str>,
    task_id: Option<&str>,
) -> Result<UnresolvedChangeBlockSummary, NativeError> {
    let store = ConversationChangeStore::open_readonly(plans_dir)?;
    Ok(store.unresolved_blocking_changes(binding_id, approval_id, task_id)?)
}

fn validate_existing_private_dir_readonly(dir: &Path) -> Result<PathBuf, ConversationChangeError> {
    if dir.as_os_str().is_empty() {
        return Err(ConversationChangeError::Validation {
            reason: "plans_dir path must not be empty.",
        });
    }
    let resolved = if dir.is_absolute() {
        dir.to_path_buf()
    } else {
        let cwd = std::env::current_dir().map_err(|_| ConversationChangeError::Io {
            reason: "Failed to resolve current working directory for plans_dir.",
        })?;
        cwd.join(dir)
    };
    let clean =
        validate_absolute_no_symlink_components(&resolved, "plans_dir").map_err(map_fs_error)?;
    let meta = std::fs::symlink_metadata(&clean).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            ConversationChangeError::Validation {
                reason: "Required plans_dir directory does not exist.",
            }
        } else {
            ConversationChangeError::Io {
                reason: "Failed to inspect plans_dir directory metadata.",
            }
        }
    })?;
    if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
        return Err(ConversationChangeError::SecurityViolation {
            reason: "plans_dir must be a non-symlink private directory.",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o077) != 0 {
            return Err(ConversationChangeError::SecurityViolation {
                reason: "plans_dir has insecure permissions (expected 0700).",
            });
        }
    }
    Ok(clean)
}

fn validate_optional_namespace_dir_readonly(
    namespace_dir: &Path,
) -> Result<bool, ConversationChangeError> {
    let _ = validate_absolute_no_symlink_components(namespace_dir, "conversation_changes_dir")
        .map_err(map_fs_error)?;
    match std::fs::symlink_metadata(namespace_dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(ConversationChangeError::SecurityViolation {
                    reason: "conversation-changes path must be a non-symlink private directory.",
                });
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if (meta.permissions().mode() & 0o077) != 0 {
                    return Err(ConversationChangeError::SecurityViolation {
                        reason: "conversation-changes directory has insecure permissions (expected 0700).",
                    });
                }
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(ConversationChangeError::Io {
            reason: "Failed to inspect conversation-changes directory metadata.",
        }),
    }
}

async fn evaluate_change_request(
    binding_id: &str,
    thread_uuid: &str,
    request: &ConversationChangeRequest,
    plans: &PlanStore,
    authority: &AuthorityStore,
    host_client: &ApprovedHostClient,
) -> Result<ValidatedChangeEvaluation, ConversationChangeError> {
    let clean_summary = validate_required_summary(&request.summary)?;
    let clean_detail = validate_optional_text(
        request.detail.as_deref(),
        MAX_CONVERSATION_CHANGE_DETAIL_BYTES,
        "detail exceeds 4096 bytes or contains control characters.",
    )?;

    let mut clean_write_paths = Vec::with_capacity(request.proposed_write_paths.len());
    for raw_path in &request.proposed_write_paths {
        let normalized = validate_repo_relative_write_path("proposed_write_paths", raw_path)
            .map_err(|_| ConversationChangeError::Validation {
                reason: "proposed_write_paths entry is not a valid repo-relative path.",
            })?;
        if !clean_write_paths.contains(&normalized) {
            clean_write_paths.push(normalized);
        }
    }

    let clean_profile_id = match &request.proposed_profile_id {
        Some(raw) => {
            threadlane_workflow::validate_registry_identifier(raw).map_err(|_| {
                ConversationChangeError::Validation {
                    reason: "proposed_profile_id failed canonical registry validation.",
                }
            })?;
            Some(raw.clone())
        }
        None => None,
    };
    let clean_model_id = match &request.proposed_model_id {
        Some(raw) => {
            threadlane_workflow::validate_registry_identifier(raw).map_err(|_| {
                ConversationChangeError::Validation {
                    reason: "proposed_model_id failed canonical registry validation.",
                }
            })?;
            Some(raw.clone())
        }
        None => None,
    };
    let clean_effort = match request.proposed_effort {
        Some(ref raw) => Some(validate_identifier("proposed_effort", raw).map_err(|_| {
            ConversationChangeError::Validation {
                reason: "proposed_effort failed identifier syntax validation.",
            }
        })?),
        None => None,
    };

    if request.requested_delivery == ConversationDeliveryRequest::RequireVerifiedLiveInput {
        return Err(ConversationChangeError::UnsupportedCapability {
            reason: "Current worker providers have no verified live in-flight input path; use safe_checkpoint_or_replan.",
        });
    }

    let expected_route = request.kind.default_route();
    if let Some(explicit_route) = request.route {
        if explicit_route != expected_route {
            return Err(ConversationChangeError::RouteMismatch {
                reason: "Explicit route does not match the route required for the conversational change kind.",
            });
        }
    }

    // Enforce strict route separation before touching live worker cancellation.
    match request.kind {
        ConversationChangeKind::QuestionOrStatus => {
            if request.cancel_action.is_some()
                || !clean_write_paths.is_empty()
                || clean_profile_id.is_some()
                || clean_model_id.is_some()
                || clean_effort.is_some()
            {
                return Err(ConversationChangeError::RouteMismatch {
                    reason: "Question or status inquiries are read-only observations and cannot propose scope/model changes or cancel tasks.",
                });
            }
        }
        ConversationChangeKind::Clarification => {
            if clean_profile_id.is_some() || clean_model_id.is_some() || clean_effort.is_some() {
                return Err(ConversationChangeError::RouteMismatch {
                    reason: "Clarification route cannot change model, effort, or profile assignments; use model_or_profile_change on scope_or_model_replan.",
                });
            }
        }
        ConversationChangeKind::ScopeChange
        | ConversationChangeKind::ModelOrProfileChange
        | ConversationChangeKind::StopOrCancel => {}
    }

    let approval_record = match request.approval_id {
        Some(ref apr_id) => {
            validate_approval_id(apr_id)?;
            Some(load_scoped_approval(
                authority,
                apr_id,
                binding_id,
                thread_uuid,
            )?)
        }
        None => None,
    };

    if let Some(ref explicit_plan_id) = request.plan_id {
        validate_identifier("plan_id", explicit_plan_id).map_err(|_| {
            ConversationChangeError::Validation {
                reason: "plan_id failed identifier syntax validation.",
            }
        })?;
    }
    if let Some(expected_plan_rev) = request.expected_plan_revision {
        if expected_plan_rev == 0 {
            return Err(ConversationChangeError::Validation {
                reason: "expected_plan_revision must be >= 1 when provided.",
            });
        }
        validate_revision("expected_plan_revision", expected_plan_rev)?;
    }

    let (resolved_plan_id, resolved_plan_revision, plan_tasks) = if let Some(ref apr) =
        approval_record
    {
        if let Some(ref explicit_plan_id) = request.plan_id {
            if explicit_plan_id != &apr.plan_id {
                return Err(ConversationChangeError::ScopeMismatch {
                    reason: "plan_id does not match the referenced approval record's plan_id.",
                });
            }
        }
        if let Some(expected_plan_rev) = request.expected_plan_revision {
            if expected_plan_rev != apr.plan_revision {
                return Err(ConversationChangeError::StaleRevision {
                        reason: "expected_plan_revision does not match the referenced approval record's plan_revision.",
                    });
            }
        }
        let plan_rev = plans
            .get_revision(&apr.plan_id, apr.plan_revision)
            .map_err(|err| map_plan_store_error(&err))?
            .ok_or(ConversationChangeError::PlanNotFound)?;
        let latest_rev = plans
            .latest_revision(&apr.plan_id)
            .map_err(|err| map_plan_store_error(&err))?
            .ok_or(ConversationChangeError::PlanNotFound)?;
        if plan_rev.binding_id != binding_id || latest_rev.binding_id != binding_id {
            return Err(ConversationChangeError::ScopeMismatch {
                reason: "Referenced workflow plan does not belong to the requested binding.",
            });
        }
        if let Some(expected_plan_rev) = request.expected_plan_revision {
            if latest_rev.revision != expected_plan_rev {
                return Err(ConversationChangeError::StaleRevision {
                    reason:
                        "expected_plan_revision does not match the plan store's latest revision.",
                });
            }
        }
        (
            Some(apr.plan_id.clone()),
            Some(apr.plan_revision),
            Some(plan_rev.tasks),
        )
    } else if let Some(ref explicit_plan_id) = request.plan_id {
        let plan_rev = plans
            .latest_revision(explicit_plan_id)
            .map_err(|err| map_plan_store_error(&err))?
            .ok_or(ConversationChangeError::PlanNotFound)?;
        if plan_rev.binding_id != binding_id {
            return Err(ConversationChangeError::ScopeMismatch {
                reason: "Referenced workflow plan does not belong to the requested binding.",
            });
        }
        if let Some(expected_plan_rev) = request.expected_plan_revision {
            if plan_rev.revision != expected_plan_rev {
                return Err(ConversationChangeError::StaleRevision {
                    reason:
                        "expected_plan_revision does not match the current workflow plan revision.",
                });
            }
        }
        (
            Some(plan_rev.plan_id),
            Some(plan_rev.revision),
            Some(plan_rev.tasks),
        )
    } else {
        if request.expected_plan_revision.is_some() {
            return Err(ConversationChangeError::Validation {
                reason: "expected_plan_revision requires either plan_id or approval_id.",
            });
        }
        (None, None, None)
    };

    let resolved_task_id = match request.task_id {
        Some(ref tsk_id) => {
            validate_task_id(tsk_id)?;
            Some(tsk_id.clone())
        }
        None => None,
    };

    if request.expected_task_revision.is_some()
        && (approval_record.is_none() || resolved_task_id.is_none())
    {
        return Err(ConversationChangeError::Validation {
            reason: "expected_task_revision requires both approval_id and task_id.",
        });
    }

    let observed_task = if let Some(ref tsk_id) = resolved_task_id {
        if let Some(ref apr) = approval_record {
            let approved_task = apr
                .tasks
                .iter()
                .find(|t| &t.task_id == tsk_id)
                .ok_or(ConversationChangeError::UnknownTask)?;
            if request.kind == ConversationChangeKind::Clarification
                && !clean_write_paths.is_empty()
                && !write_paths_within_allowed(
                    &clean_write_paths,
                    &approved_task.allowed_write_paths,
                )
            {
                return Err(ConversationChangeError::RouteMismatch {
                    reason: "Clarification route cannot expand frozen approved task write scopes; use scope_change on scope_or_model_replan.",
                });
            }
            let ctx =
                observe_approved_task(apr, tsk_id, host_client, binding_id, thread_uuid).await?;
            Some(ctx)
        } else if let Some(ref tasks) = plan_tasks {
            let plan_task = tasks
                .iter()
                .find(|t| &t.id == tsk_id)
                .ok_or(ConversationChangeError::UnknownTask)?;
            if request.kind == ConversationChangeKind::Clarification
                && !clean_write_paths.is_empty()
                && !write_paths_within_allowed(&clean_write_paths, &plan_task.allowed_write_paths)
            {
                return Err(ConversationChangeError::RouteMismatch {
                    reason: "Clarification route cannot expand task write scopes; use scope_change on scope_or_model_replan.",
                });
            }
            None
        } else {
            return Err(ConversationChangeError::Validation {
                reason: "task_id requires either approval_id or plan_id.",
            });
        }
    } else {
        if request.kind == ConversationChangeKind::Clarification && !clean_write_paths.is_empty() {
            return Err(ConversationChangeError::RouteMismatch {
                reason: "Clarification route cannot propose write scope changes without an in-scope task; use scope_change.",
            });
        }
        None
    };

    if let Some(expected_task_rev) = request.expected_task_revision {
        validate_revision("expected_task_revision", expected_task_rev)?;
        let task_ctx = observed_task
            .as_ref()
            .ok_or(ConversationChangeError::Validation {
                reason: "expected_task_revision requires an approved task.",
            })?;
        if task_ctx.status == TaskExecutionStatus::Unknown || task_ctx.revision.is_none() {
            return Err(ConversationChangeError::AmbiguousTaskState {
                reason: "Live task state is unknown; cannot verify expected_task_revision.",
            });
        }
        if task_ctx.revision != Some(expected_task_rev) {
            return Err(ConversationChangeError::StaleRevision {
                reason: "Observed task revision does not match expected_task_revision.",
            });
        }
    }

    let clean_cancel_reason = if let Some(ref cancel_action) = request.cancel_action {
        let apr = approval_record
            .as_ref()
            .ok_or(ConversationChangeError::Validation {
                reason: "cancel_action requires approval_id.",
            })?;
        let _ = resolved_task_id
            .as_ref()
            .ok_or(ConversationChangeError::Validation {
                reason: "cancel_action requires task_id.",
            })?;
        if !cancel_action.operator_approved {
            return Err(ConversationChangeError::Validation {
                reason: "cancel_action requires operator_approved=true.",
            });
        }
        let checked_digest =
            validate_sha256_hex("confirm_approval_digest", &cancel_action.confirm_approval_digest)
                .map_err(|_| ConversationChangeError::Validation {
                    reason: "confirm_approval_digest must be a 64-character lowercase hexadecimal SHA-256 digest.",
                })?;
        if !constant_time_str_eq(&apr.subject_digest, &checked_digest) {
            return Err(ConversationChangeError::SubjectMismatch);
        }
        if cancel_action.expected_task_revision == 0 {
            return Err(ConversationChangeError::Validation {
                reason: "cancel_action.expected_task_revision must be >= 1.",
            });
        }
        validate_revision(
            "cancel_action.expected_task_revision",
            cancel_action.expected_task_revision,
        )?;
        if let Some(req_task_rev) = request.expected_task_revision {
            if req_task_rev != cancel_action.expected_task_revision {
                return Err(ConversationChangeError::Validation {
                    reason: "expected_task_revision and cancel_action.expected_task_revision must match.",
                });
            }
        }
        let task_ctx = observed_task
            .as_ref()
            .ok_or(ConversationChangeError::Validation {
                reason: "cancel_action requires an approved task.",
            })?;
        if task_ctx.adapter == ToolAdapter::CodexCli {
            return Err(ConversationChangeError::InvalidRunState {
                reason: "Task targets a manual native route and cannot be cancelled as a worker.",
            });
        }
        if task_ctx.status == TaskExecutionStatus::Unknown {
            return Err(ConversationChangeError::HostNotReady);
        }
        if task_ctx.stop_state == ObservedTaskStopState::NotStarted {
            return Err(ConversationChangeError::InvalidRunState {
                reason: "Requested approved task has not been started in this host session.",
            });
        }
        if task_ctx.revision != Some(cancel_action.expected_task_revision) {
            return Err(ConversationChangeError::StaleRevision {
                reason:
                    "Observed task revision does not match cancel_action.expected_task_revision.",
            });
        }
        validate_optional_text(
            cancel_action.reason.as_deref(),
            MAX_CANCEL_REASON_BYTES,
            "cancel_action.reason exceeds 512 bytes or contains control characters.",
        )?
    } else {
        None
    };

    let (decision_kind, status, replan_required, new_approval_required, blocks_task_advancement) =
        match expected_route {
            ConversationChangeRoute::ReadOnlyObservation => (
                ConversationChangeDecisionKind::RecordOnly,
                ConversationChangeStatus::RecordedOnly,
                false,
                false,
                false,
            ),
            ConversationChangeRoute::CheckpointClarification => (
                ConversationChangeDecisionKind::PendingClarificationAtCheckpoint,
                ConversationChangeStatus::PendingClarification,
                false,
                false,
                true,
            ),
            ConversationChangeRoute::ScopeOrModelReplan => (
                ConversationChangeDecisionKind::CancelReplanNewApprovalRequired,
                ConversationChangeStatus::PendingReplan,
                true,
                true,
                true,
            ),
        };

    let live_input_status = match expected_route {
        ConversationChangeRoute::ReadOnlyObservation => {
            LiveInputSupportStatus::NotApplicableReadOnly
        }
        ConversationChangeRoute::CheckpointClarification
        | ConversationChangeRoute::ScopeOrModelReplan => {
            LiveInputSupportStatus::UnsupportedNoLiveInputPath
        }
    };

    let observed_stop_state = observed_task.as_ref().map(|c| c.stop_state);
    let prior_task_proven_stopped =
        observed_stop_state.is_some_and(ObservedTaskStopState::is_proven_stopped);

    let decision = ConversationChangeDecision {
        policy_version: CONVERSATION_CHANGE_POLICY_VERSION.to_string(),
        kind: request.kind,
        route: expected_route,
        decision_kind,
        status,
        requested_delivery: request.requested_delivery,
        live_input_status,
        task_adapter: observed_task.as_ref().map(|c| c.adapter),
        observed_task_status: observed_task.as_ref().map(|c| c.status),
        observed_task_revision: observed_task.as_ref().and_then(|c| c.revision),
        observed_stop_state,
        prior_task_proven_stopped,
        cancel_requested: false,
        replan_required,
        new_approval_required,
        blocks_task_advancement,
        frozen_approval_immutable: true,
        live_input_delivered: false,
        process_paused: false,
        provider_resumed: false,
        rationale: build_decision_rationale(
            request.kind,
            expected_route,
            observed_stop_state,
            false,
        ),
    };

    let dependent_task_ids = observed_task
        .map(|c| c.dependent_task_ids)
        .unwrap_or_else(|| match (&plan_tasks, resolved_task_id.as_deref()) {
            (Some(tasks), Some(root)) => collect_dependent_ids(
                tasks
                    .iter()
                    .map(|t| (t.id.as_str(), t.depends_on.as_slice())),
                root,
            ),
            _ => Vec::new(),
        });
    Ok(ValidatedChangeEvaluation {
        approval_record,
        plan_id: resolved_plan_id,
        plan_revision: resolved_plan_revision,
        task_id: resolved_task_id,
        dependent_task_ids,
        summary: clean_summary,
        detail: clean_detail,
        proposed_write_paths: clean_write_paths,
        proposed_profile_id: clean_profile_id,
        proposed_model_id: clean_model_id,
        proposed_effort: clean_effort,
        cancel_reason: clean_cancel_reason,
        decision,
    })
}

fn build_decision_rationale(
    kind: ConversationChangeKind,
    route: ConversationChangeRoute,
    stop_state: Option<ObservedTaskStopState>,
    cancel_invoked: bool,
) -> String {
    match route {
        ConversationChangeRoute::ReadOnlyObservation => {
            "Read-only question or status observation recorded without revising plans, cancelling tasks, or consulting models.".to_string()
        }
        ConversationChangeRoute::CheckpointClarification => match stop_state {
            Some(ObservedTaskStopState::Running) => {
                "Clarification recorded for the next workflow checkpoint; live in-flight worker input is unsupported, so active task continues until a real checkpoint.".to_string()
            }
            Some(ObservedTaskStopState::CancelRequested) => {
                "Clarification recorded while task cancellation is in progress (`cancel_requested`); wait until the task is proven stopped before resolving.".to_string()
            }
            Some(ObservedTaskStopState::Unknown) => {
                "Clarification recorded while live task state is unknown; reconcile host state and verify the task is stopped before resolving.".to_string()
            }
            _ => {
                "Clarification recorded at a verified workflow checkpoint without expanding frozen write scopes or changing model assignments.".to_string()
            }
        },
        ConversationChangeRoute::ScopeOrModelReplan => {
            let prefix = match kind {
                ConversationChangeKind::ScopeChange => "Scope change",
                ConversationChangeKind::ModelOrProfileChange => "Model or profile change",
                ConversationChangeKind::StopOrCancel => "Stop or cancellation change",
                _ => "Conversational change",
            };
            match (cancel_invoked, stop_state) {
                (true, Some(ObservedTaskStopState::CancelRequested)) => format!(
                    "{prefix} requested task cancellation (`cancel_requested`); prior worker is not yet proven stopped. Wait for `cancelled`, publish a new plan revision, and record fresh operator approval."
                ),
                (true, Some(ObservedTaskStopState::ProvenStopped)) => format!(
                    "{prefix} cancelled the prior task (`cancelled`, proven stopped). Publish a new plan revision and record fresh operator approval before dispatching replacement work."
                ),
                (false, Some(ObservedTaskStopState::Running)) => format!(
                    "{prefix} invalidates the active task; cancel the running task and wait until it is proven stopped, then publish a new plan revision and record fresh operator approval."
                ),
                (false, Some(ObservedTaskStopState::CancelRequested)) => format!(
                    "{prefix} recorded while prior task is `cancel_requested` (not yet proven stopped). Wait until `cancelled`, then publish a new plan revision and record fresh operator approval."
                ),
                _ => format!(
                    "{prefix} requires publishing a new plan revision and recording fresh operator approval (`threadlane-approve`); frozen approved prompts and scopes remain immutable."
                ),
            }
        }
    }
}

async fn observe_approved_task(
    approval_record: &ApprovalRecord,
    task_id: &str,
    host_client: &ApprovedHostClient,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<ObservedTaskContext, ConversationChangeError> {
    let task_record = approval_record
        .tasks
        .iter()
        .find(|t| t.task_id == task_id)
        .ok_or(ConversationChangeError::UnknownTask)?;
    let dependent_task_ids = collect_transitive_dependents(&approval_record.tasks, task_id);

    match host_client
        .get_task(&approval_record.approval_id, task_id)
        .await
    {
        Ok(snapshot) => {
            if snapshot.binding_id != binding_id || snapshot.thread_uuid != thread_uuid {
                return Err(ConversationChangeError::ScopeMismatch {
                    reason: "Observed task snapshot does not belong to the requested binding and thread scope.",
                });
            }
            let stop_state = ObservedTaskStopState::from_execution_status(snapshot.status);
            let revision = snapshot.worker.as_ref().map(|w| w.revision).or_else(|| {
                if stop_state == ObservedTaskStopState::NotStarted {
                    Some(0)
                } else {
                    None
                }
            });
            Ok(ObservedTaskContext {
                adapter: task_record.adapter,
                status: snapshot.status,
                revision,
                stop_state,
                dependent_task_ids,
            })
        }
        Err(HostError::HostNotReady { phase: None, .. }) => {
            // Host session directory or manifest has not been created yet: task has never been started.
            let initial_status = if task_record.adapter == ToolAdapter::CodexCli
                || task_record.provider.is_none()
            {
                TaskExecutionStatus::NativeRouteRequired
            } else if !task_record.depends_on.is_empty() {
                TaskExecutionStatus::DependencyInputsUnverified
            } else {
                TaskExecutionStatus::NotStarted
            };
            Ok(ObservedTaskContext {
                adapter: task_record.adapter,
                status: initial_status,
                revision: Some(0),
                stop_state: ObservedTaskStopState::NotStarted,
                dependent_task_ids,
            })
        }
        Err(HostError::HostNotReady {
            phase: Some(_), ..
        }) => {
            if task_record.adapter == ToolAdapter::CodexCli || task_record.provider.is_none() {
                Ok(ObservedTaskContext {
                    adapter: task_record.adapter,
                    status: TaskExecutionStatus::NativeRouteRequired,
                    revision: Some(0),
                    stop_state: ObservedTaskStopState::NotStarted,
                    dependent_task_ids,
                })
            } else {
                Ok(ObservedTaskContext {
                    adapter: task_record.adapter,
                    status: TaskExecutionStatus::Unknown,
                    revision: None,
                    stop_state: ObservedTaskStopState::Unknown,
                    dependent_task_ids,
                })
            }
        }
        Err(HostError::ScopeMismatch { .. }) => Err(ConversationChangeError::ScopeMismatch {
            reason: "Approved host session does not belong to the requested binding and thread scope.",
        }),
        Err(HostError::SecurityViolation { .. }) => {
            Err(ConversationChangeError::SecurityViolation {
                reason: "Approved host session store violated private permission or symlink security checks.",
            })
        }
        Err(HostError::LockBusy { .. }) => Err(ConversationChangeError::LockBusy),
        Err(_) => Err(ConversationChangeError::AmbiguousTaskState {
            reason: "Failed to inspect live approved task state from the host session.",
        }),
    }
}

fn ensure_task_proven_stopped_for_resolution(
    record: &ConversationChangeRecord,
    resolution_kind: ConversationChangeResolutionKind,
    live_status: TaskExecutionStatus,
    stop_state: ObservedTaskStopState,
) -> Result<(), ConversationChangeError> {
    match stop_state {
        ObservedTaskStopState::Running | ObservedTaskStopState::CancelRequested => {
            Err(ConversationChangeError::TaskNotProvenStopped {
                reason: "Affected task is still running or cancel_requested and is not yet proven stopped.",
            })
        }
        ObservedTaskStopState::Unknown => Err(ConversationChangeError::AmbiguousTaskState {
            reason: "Affected task state is unknown and cannot be proven stopped.",
        }),
        ObservedTaskStopState::NotStarted | ObservedTaskStopState::ProvenStopped => {
            if resolution_kind == ConversationChangeResolutionKind::CheckpointHandled
                && record.decision.status == ConversationChangeStatus::PendingClarification
                && matches!(
                    live_status,
                    TaskExecutionStatus::Failed
                        | TaskExecutionStatus::Cancelled
                        | TaskExecutionStatus::TimedOut
                )
            {
                return Err(ConversationChangeError::ReplanRequired {
                    reason: "Task ended in failed, cancelled, or timed_out state; resolve via linked_plan_revision instead of checkpoint_handled.",
                });
            }
            Ok(())
        }
    }
}

fn collect_transitive_dependents(tasks: &[ApprovedTaskRecord], root_task_id: &str) -> Vec<String> {
    collect_dependent_ids(
        tasks
            .iter()
            .map(|t| (t.task_id.as_str(), t.depends_on.as_slice())),
        root_task_id,
    )
}

fn collect_dependent_ids<'a>(
    tasks: impl Iterator<Item = (&'a str, &'a [String])>,
    root_task_id: &str,
) -> Vec<String> {
    let graph: Vec<_> = tasks.collect();
    let mut visited = BTreeSet::new();
    let mut queue = vec![root_task_id.to_string()];
    while let Some(current) = queue.pop() {
        for (id, deps) in &graph {
            if deps.iter().any(|dep| dep == &current) && visited.insert((*id).to_string()) {
                queue.push((*id).to_string());
            }
        }
    }
    graph
        .into_iter()
        .filter(|(id, _)| visited.contains(*id))
        .map(|(id, _)| id.to_string())
        .collect()
}

fn write_paths_within_allowed(proposed: &[String], allowed: &[String]) -> bool {
    proposed.iter().all(|prop| {
        allowed
            .iter()
            .any(|allow| prop == allow || (allow.ends_with('/') && prop.starts_with(allow)))
    })
}

fn record_matches_scope(
    record: &ConversationChangeRecord,
    approval_filter: Option<&str>,
    task_filter: Option<&str>,
) -> bool {
    if let Some(apr_id) = approval_filter {
        if let Some(ref rec_apr) = record.approval_id {
            if rec_apr != apr_id {
                return false;
            }
        }
    }
    if let Some(tsk_id) = task_filter {
        if let Some(ref rec_tsk) = record.task_id {
            if rec_tsk != tsk_id
                && !record
                    .affected_dependent_task_ids
                    .iter()
                    .any(|dep| dep == tsk_id)
            {
                return false;
            }
        }
    }
    true
}

fn record_supersedes_approval(
    record: &ConversationChangeRecord,
    approval_filter: Option<&str>,
) -> bool {
    let Some(apr_id) = approval_filter else {
        return false;
    };
    if record.decision.status != ConversationChangeStatus::Resolved {
        return false;
    }
    let is_replan_resolution = record.decision.replan_required
        || record.decision.new_approval_required
        || record
            .resolution
            .as_ref()
            .map(|res| res.resolution_kind == ConversationChangeResolutionKind::LinkedPlanRevision)
            .unwrap_or(false);
    if !is_replan_resolution {
        return false;
    }
    record.approval_id.as_deref() == Some(apr_id)
}

fn refuse_pending_cancel_observation(
    ledger: &PersistedChangeLedger,
    approval_filter: Option<&str>,
) -> Result<(), ConversationChangeError> {
    if ledger
        .ambiguous_cancels
        .iter()
        .any(|pending| approval_filter.is_none_or(|approval_id| pending.approval_id == approval_id))
    {
        // Until a returned snapshot is durably recorded, all tasks in the
        // affected approval may depend on a change whose outcome is unknown.
        return Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation);
    }
    Ok(())
}

fn build_unresolved_summary(
    binding_id: &str,
    current_revision: u64,
    records: &[ConversationChangeRecord],
    approval_filter: Option<&str>,
    task_filter: Option<&str>,
) -> UnresolvedChangeBlockSummary {
    let mut blocking_changes = Vec::new();
    let mut superseded_by_changes = Vec::new();
    let mut replan_required = false;
    let mut new_approval_required = false;

    for record in records.iter().rev() {
        if record.decision.status.is_unresolved_blocking() {
            if !record_matches_scope(record, approval_filter, task_filter) {
                continue;
            }
            replan_required |= record.decision.replan_required;
            new_approval_required |= record.decision.new_approval_required;
            blocking_changes.push(UnresolvedBlockingChange {
                change_id: record.change_id.clone(),
                change_revision: record.updated_change_revision,
                kind: record.decision.kind,
                route: record.decision.route,
                status: record.decision.status,
                approval_id: record.approval_id.clone(),
                plan_id: record.plan_id.clone(),
                plan_revision: record.plan_revision,
                task_id: record.task_id.clone(),
                affected_dependent_task_ids: record.affected_dependent_task_ids.clone(),
                replan_required: record.decision.replan_required,
                new_approval_required: record.decision.new_approval_required,
                observed_stop_state: record.decision.observed_stop_state,
                prior_task_proven_stopped: record.decision.prior_task_proven_stopped,
                summary: record.summary.clone(),
            });
        } else if record_supersedes_approval(record, approval_filter) {
            let target_plan_revision = record
                .resolution
                .as_ref()
                .and_then(|res| res.target_plan_revision);
            let new_approval_id = record
                .resolution
                .as_ref()
                .and_then(|res| res.new_approval_id.clone());
            superseded_by_changes.push(SupersededApprovalChange {
                change_id: record.change_id.clone(),
                change_revision: record.updated_change_revision,
                kind: record.decision.kind,
                route: record.decision.route,
                superseded_approval_id: record.approval_id.clone(),
                superseded_plan_id: record.plan_id.clone(),
                superseded_plan_revision: record.plan_revision,
                task_id: record.task_id.clone(),
                affected_dependent_task_ids: record.affected_dependent_task_ids.clone(),
                target_plan_revision,
                new_approval_id,
                summary: record.summary.clone(),
            });
        }
    }

    let approval_superseded = !superseded_by_changes.is_empty();
    UnresolvedChangeBlockSummary {
        binding_id: binding_id.to_string(),
        approval_id: approval_filter.map(str::to_string),
        task_id: task_filter.map(str::to_string),
        current_change_revision: current_revision,
        blocked: !blocking_changes.is_empty() || approval_superseded,
        approval_superseded,
        replan_required,
        new_approval_required,
        blocking_changes,
        superseded_by_changes,
    }
}

fn verify_idempotent_record_match(
    existing: &ConversationChangeRecord,
    binding_id: &str,
    thread_uuid: &str,
    request: &ConversationChangeRequest,
    _request_digest: &str,
    current_store_revision: u64,
) -> Result<(), ConversationChangeError> {
    if existing.binding_id != binding_id || existing.thread_uuid != thread_uuid {
        return Err(ConversationChangeError::ScopeMismatch {
            reason: "Existing conversation change record belongs to a different binding or thread scope.",
        });
    }

    let mut clean_req_write_paths = Vec::with_capacity(request.proposed_write_paths.len());
    for raw_path in &request.proposed_write_paths {
        let normalized = validate_repo_relative_write_path("proposed_write_paths", raw_path)
            .map_err(|_| ConversationChangeError::IdempotencyConflict {
                reason:
                    "request_key was already used with a different conversational change payload.",
            })?;
        if !clean_req_write_paths.contains(&normalized) {
            clean_req_write_paths.push(normalized);
        }
    }

    let approval_matches = request.approval_id == existing.approval_id;
    let plan_matches = match (&request.plan_id, &request.approval_id) {
        (Some(req_plan), _) => existing.plan_id.as_ref() == Some(req_plan),
        (None, Some(_)) => true,
        (None, None) => existing.plan_id.is_none(),
    };
    let plan_rev_matches = match request.expected_plan_revision {
        Some(req_rev) => existing.plan_revision == Some(req_rev),
        None => true,
    };
    let route_matches = match request.route {
        Some(req_route) => req_route == existing.decision.route,
        None => existing.decision.kind.default_route() == existing.decision.route,
    };
    let cancel_matches = match &request.cancel_action {
        Some(cancel) => {
            existing.decision.cancel_requested
                && cancel.operator_approved
                && existing.approval_subject_digest.as_deref()
                    == Some(cancel.confirm_approval_digest.as_str())
        }
        None => !existing.decision.cancel_requested,
    };
    let revision_ok = request.expected_change_revision
        == existing.created_change_revision.saturating_sub(1)
        || request.expected_change_revision == existing.created_change_revision
        || request.expected_change_revision == current_store_revision;

    let payload_matches = approval_matches
        && plan_matches
        && plan_rev_matches
        && request.task_id == existing.task_id
        && request.kind == existing.decision.kind
        && route_matches
        && request.summary.trim() == existing.summary
        && request.detail.as_deref().map(str::trim) == existing.detail.as_deref()
        && clean_req_write_paths == existing.proposed_write_paths
        && request.proposed_profile_id == existing.proposed_profile_id
        && request.proposed_model_id == existing.proposed_model_id
        && request.proposed_effort == existing.proposed_effort
        && request.requested_delivery == existing.decision.requested_delivery
        && cancel_matches;

    if !revision_ok || !payload_matches {
        return Err(ConversationChangeError::IdempotencyConflict {
            reason: "request_key was already used with a different conversational change payload.",
        });
    }
    Ok(())
}

fn compute_request_digest(
    binding_id: &str,
    thread_uuid: &str,
    request: &ConversationChangeRequest,
) -> Result<String, ConversationChangeError> {
    let tuple = (
        "threadlane-native:conversation-change-request:v1",
        binding_id,
        thread_uuid,
        &request.approval_id,
        &request.plan_id,
        &request.task_id,
        request.kind.as_str(),
        request.summary.trim(),
        request.detail.as_deref().map(str::trim),
        &request.proposed_write_paths,
        &request.proposed_profile_id,
        &request.proposed_model_id,
        &request.proposed_effort,
        request.requested_delivery.as_str(),
        request.cancel_action.is_some(),
    );
    let encoded = serde_json::to_vec(&tuple).map_err(|_| ConversationChangeError::Validation {
        reason: "Failed to encode conversational change request digest.",
    })?;
    Ok(sha256_hex(&encoded))
}

fn compute_plan_revision_digest<T: Serialize>(
    plan_rev: &T,
) -> Result<String, ConversationChangeError> {
    let encoded =
        serde_json::to_vec(&("threadlane-native:plan-rev:v1", plan_rev)).map_err(|_| {
            ConversationChangeError::Validation {
                reason: "Failed to encode target plan revision digest.",
            }
        })?;
    Ok(sha256_hex(&encoded))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn binding_ledger_tag(binding_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"threadlane-native:conversation-changes:v1\0");
    hasher.update(binding_id.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(32);
    for byte in &digest[..16] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn ledger_paths_for_binding(
    namespace_dir: &Path,
    binding_id: &str,
) -> Result<(PathBuf, String), ConversationChangeError> {
    let tag = binding_ledger_tag(binding_id);
    let ledger_filename = format!("changes-{tag}.json");
    let lock_filename = format!("changes-{tag}.lock");
    Ok((namespace_dir.join(ledger_filename), lock_filename))
}

fn read_ledger_locked(
    ledger_path: &Path,
    expected_binding_id: &str,
) -> Result<PersistedChangeLedger, ConversationChangeError> {
    let Some(bytes) = read_private_file_bounded(
        ledger_path,
        MAX_CONVERSATION_CHANGE_STORE_BYTES,
        "conversation_change_ledger",
    )
    .map_err(map_fs_error)?
    else {
        return Ok(PersistedChangeLedger::new(expected_binding_id));
    };

    let ledger: PersistedChangeLedger =
        serde_json::from_slice(&bytes).map_err(|_| ConversationChangeError::CorruptStore {
            reason: "Persisted conversation-change ledger contains invalid JSON.",
        })?;
    validate_persisted_ledger(&ledger, expected_binding_id)?;
    Ok(ledger)
}

fn write_ledger_locked(
    namespace_dir: &Path,
    ledger_path: &Path,
    ledger: &PersistedChangeLedger,
) -> Result<(), ConversationChangeError> {
    validate_persisted_ledger(ledger, &ledger.binding_id)?;
    if ledger.records.len() > MAX_CONVERSATION_CHANGES_PER_BINDING {
        return Err(ConversationChangeError::StoreLimitExceeded {
            reason:
                "Conversation change ledger reached the maximum of 256 records for this binding.",
        });
    }
    let encoded =
        serde_json::to_vec_pretty(ledger).map_err(|_| ConversationChangeError::CorruptStore {
            reason: "Failed to serialize conversation-change ledger.",
        })?;
    if encoded.len() > MAX_CONVERSATION_CHANGE_STORE_BYTES {
        return Err(ConversationChangeError::StoreLimitExceeded {
            reason: "Conversation change ledger exceeded the 1 MiB size limit.",
        });
    }
    let filename = ledger_path.file_name().and_then(|n| n.to_str()).ok_or(
        ConversationChangeError::SecurityViolation {
            reason: "Invalid conversation-change ledger filename.",
        },
    )?;
    write_private_file_atomic(
        namespace_dir,
        filename,
        &encoded,
        "conversation_change_ledger",
    )
    .map_err(map_fs_error)?;
    Ok(())
}

fn validate_persisted_ledger(
    ledger: &PersistedChangeLedger,
    expected_binding_id: &str,
) -> Result<(), ConversationChangeError> {
    if ledger.schema_version != CONVERSATION_CHANGE_SCHEMA_VERSION {
        return Err(ConversationChangeError::CorruptStore {
            reason: "Unsupported conversation-change ledger schema version.",
        });
    }
    if ledger.binding_id != expected_binding_id {
        return Err(ConversationChangeError::CorruptStore {
            reason: "Conversation-change ledger binding_id mismatch.",
        });
    }
    if ledger.records.len() > MAX_CONVERSATION_CHANGES_PER_BINDING {
        return Err(ConversationChangeError::CorruptStore {
            reason: "Conversation-change ledger exceeds maximum record count.",
        });
    }
    if validate_authority_revision("current_revision", ledger.current_revision).is_err() {
        return Err(ConversationChangeError::CorruptStore {
            reason: "Conversation-change ledger contains an out-of-range revision number.",
        });
    }

    let mut seen_change_ids = BTreeSet::new();
    let mut seen_request_keys = BTreeSet::new();
    let mut max_seen_revision = 0u64;

    for record in &ledger.records {
        if validate_change_id(&record.change_id).is_err()
            || !seen_change_ids.insert(record.change_id.as_str())
        {
            return Err(ConversationChangeError::CorruptStore {
                reason: "Conversation-change ledger contains an invalid or duplicate change_id.",
            });
        }
        if validate_request_key(&record.request_key).is_err()
            || !seen_request_keys.insert(record.request_key.as_str())
        {
            return Err(ConversationChangeError::CorruptStore {
                reason: "Conversation-change ledger contains an invalid or duplicate request_key.",
            });
        }
        if record.binding_id != expected_binding_id
            || validate_thread_uuid(&record.thread_uuid).is_err()
        {
            return Err(ConversationChangeError::CorruptStore {
                reason: "Conversation-change record scope metadata failed validation.",
            });
        }
        if record.created_change_revision == 0
            || record.updated_change_revision < record.created_change_revision
            || record.updated_change_revision > ledger.current_revision
        {
            return Err(ConversationChangeError::CorruptStore {
                reason: "Conversation-change record revision bounds are inconsistent.",
            });
        }
        if let Some(ref res) = record.resolution {
            if validate_request_key(&res.resolution_request_key).is_err()
                || !seen_request_keys.insert(res.resolution_request_key.as_str())
                || res.resolved_change_revision != record.updated_change_revision
                || record.decision.status != ConversationChangeStatus::Resolved
            {
                return Err(ConversationChangeError::CorruptStore {
                    reason: "Conversation-change record resolution metadata is inconsistent.",
                });
            }
        }
        max_seen_revision = max_seen_revision.max(record.updated_change_revision);
    }

    if (ledger.records.is_empty() && ledger.current_revision != 0)
        || (!ledger.records.is_empty() && ledger.current_revision != max_seen_revision)
    {
        return Err(ConversationChangeError::CorruptStore {
            reason: "Conversation-change ledger current_revision does not match its records.",
        });
    }

    Ok(())
}

fn load_scoped_approval(
    authority: &AuthorityStore,
    approval_id: &str,
    expected_binding_id: &str,
    expected_thread_uuid: &str,
) -> Result<ApprovalRecord, ConversationChangeError> {
    let record = authority
        .get_for_operator(approval_id)
        .map_err(|err| map_authority_to_change_error(&err))?;
    if record.binding_id != expected_binding_id || record.thread_uuid != expected_thread_uuid {
        return Err(ConversationChangeError::ScopeMismatch {
            reason: "Approval record does not belong to the requested binding and thread scope.",
        });
    }
    Ok(record)
}

fn verify_client_scope(
    host_client: &ApprovedHostClient,
    binding_id: &str,
    thread_uuid: &str,
) -> Result<(), ConversationChangeError> {
    if host_client.control_binding_id() != binding_id
        || host_client.native_thread_uuid() != thread_uuid
    {
        return Err(ConversationChangeError::ScopeMismatch {
            reason: "ApprovedHostClient scope does not match the requested binding and thread.",
        });
    }
    Ok(())
}

fn validate_binding_id(binding_id: &str) -> Result<String, ConversationChangeError> {
    validate_identifier("binding_id", binding_id).map_err(|_| ConversationChangeError::Validation {
        reason: "binding_id failed identifier syntax validation.",
    })
}

fn validate_thread_uuid(thread_uuid: &str) -> Result<String, ConversationChangeError> {
    validate_canonical_thread_uuid("thread_uuid", thread_uuid).map_err(|_| {
        ConversationChangeError::Validation {
            reason: "thread_uuid must be a canonical lowercase hyphenated non-nil UUID.",
        }
    })
}

fn validate_request_key(request_key: &str) -> Result<String, ConversationChangeError> {
    validate_identifier("request_key", request_key).map_err(|_| {
        ConversationChangeError::Validation {
            reason: "request_key must be 1..=128 ASCII alphanumeric, `_`, or `-` characters.",
        }
    })
}

fn validate_task_id(task_id: &str) -> Result<String, ConversationChangeError> {
    validate_identifier("task_id", task_id).map_err(|_| ConversationChangeError::Validation {
        reason: "task_id failed identifier syntax validation.",
    })
}

fn validate_approval_id(approval_id: &str) -> Result<(), ConversationChangeError> {
    threadlane_host::storage::validate_approval_id(approval_id).map_err(|_| {
        ConversationChangeError::Validation {
            reason: "approval_id must match `apr_<32 lowercase hex characters>`.",
        }
    })
}

fn validate_change_id(change_id: &str) -> Result<(), ConversationChangeError> {
    if !change_id.starts_with(CHANGE_ID_PREFIX)
        || change_id.len() != CHANGE_ID_PREFIX.len() + CHANGE_ID_HEX_LEN
    {
        return Err(ConversationChangeError::Validation {
            reason: "change_id must match `chg_<32 lowercase hex characters>`.",
        });
    }
    let hex_part = &change_id[CHANGE_ID_PREFIX.len()..];
    if !hex_part
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ConversationChangeError::Validation {
            reason: "change_id must contain only lowercase hexadecimal characters after `chg_`.",
        });
    }
    Ok(())
}

fn generate_change_id() -> String {
    format!("{CHANGE_ID_PREFIX}{}", Uuid::new_v4().simple())
}

fn validate_revision(field: &'static str, value: u64) -> Result<(), ConversationChangeError> {
    validate_authority_revision(field, value).map_err(|_| ConversationChangeError::Validation {
        reason: "Revision number exceeds the maximum safe integer bound.",
    })?;
    Ok(())
}

fn validate_repo_relative_write_path(
    _field: &'static str,
    rel_path: &str,
) -> Result<String, ConversationChangeError> {
    if rel_path.is_empty() || rel_path.len() > 512 || rel_path.trim() != rel_path {
        return Err(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry is not a valid repo-relative path.",
        });
    }
    let stripped = rel_path.strip_suffix('/').unwrap_or(rel_path);
    if stripped.is_empty()
        || rel_path.starts_with('/')
        || stripped.ends_with('/')
        || rel_path.contains('\\')
        || rel_path.contains('\0')
        || rel_path.contains("//")
        || rel_path
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '*' | '?' | '[' | ']' | '{' | '}' | '!'))
        || stripped
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry is not a valid repo-relative path.",
        });
    }
    let path = Path::new(stripped);
    if path.is_absolute() {
        return Err(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry is not a valid repo-relative path.",
        });
    }
    let components: Vec<String> = path
        .components()
        .map(|comp| match comp {
            Component::Normal(part) => Some(part.to_string_lossy().to_ascii_lowercase()),
            _ => None,
        })
        .collect::<Option<Vec<String>>>()
        .ok_or(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry is not a valid repo-relative path.",
        })?;
    if components.is_empty() {
        return Err(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry is not a valid repo-relative path.",
        });
    }
    for comp in &components {
        if matches!(
            comp.as_str(),
            ".git"
                | ".gitmodules"
                | ".gitattributes"
                | ".gitignore"
                | "agents.md"
                | "claude.md"
                | ".codex"
                | ".gemini"
                | ".opencode"
                | ".agents"
                | ".claude"
        ) || comp == ".env"
            || comp.starts_with(".env.")
        {
            return Err(ConversationChangeError::Validation {
                reason:
                    "proposed_write_paths entry targets a protected control or configuration path.",
            });
        }
    }
    if components[0] == ".github"
        || (components[0] == ".cargo"
            && components
                .get(1)
                .is_some_and(|second| second == "config" || second == "config.toml"))
    {
        return Err(ConversationChangeError::Validation {
            reason: "proposed_write_paths entry targets a protected control or configuration path.",
        });
    }
    Ok(rel_path.to_string())
}

fn validate_required_summary(raw: &str) -> Result<String, ConversationChangeError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_CONVERSATION_CHANGE_SUMMARY_BYTES
        || trimmed.chars().any(char::is_control)
    {
        return Err(ConversationChangeError::Validation {
            reason: "summary must be 1..=512 bytes without control characters or newlines.",
        });
    }
    Ok(trimmed.to_string())
}

fn validate_optional_text(
    raw: Option<&str>,
    max_bytes: usize,
    err_reason: &'static str,
) -> Result<Option<String>, ConversationChangeError> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max_bytes || trimmed.chars().any(char::is_control) {
        return Err(ConversationChangeError::Validation { reason: err_reason });
    }
    Ok(Some(trimmed.to_string()))
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn map_fs_error(err: NativeError) -> ConversationChangeError {
    match err.error_code() {
        NativeErrorCode::SecurityViolation => ConversationChangeError::SecurityViolation {
            reason:
                "Conversation-change store path or file permissions failed security validation.",
        },
        NativeErrorCode::UnsupportedPlatform => ConversationChangeError::UnsupportedPlatform,
        NativeErrorCode::OutputOverflow => ConversationChangeError::StoreLimitExceeded {
            reason: "Conversation-change store file exceeded the 1 MiB size limit.",
        },
        _ => ConversationChangeError::Io {
            reason: "Filesystem I/O error while accessing the conversation-change store.",
        },
    }
}

fn map_plan_store_error(err: &PlanError) -> ConversationChangeError {
    match err {
        PlanError::PlanNotFound { .. } => ConversationChangeError::PlanNotFound,
        PlanError::StaleRevision { .. } | PlanError::ProfileRevisionMismatch { .. } => {
            ConversationChangeError::StaleRevision {
                reason: "Workflow plan store revision did not match expected revision.",
            }
        }
        PlanError::BindingMismatch { .. } => ConversationChangeError::ScopeMismatch {
            reason: "Referenced workflow plan does not belong to the requested binding.",
        },
        PlanError::UnpublishedPlan { .. } => ConversationChangeError::UnpublishedOrSupersededPlan,
        PlanError::SecurityViolation(_) => ConversationChangeError::SecurityViolation {
            reason: "Workflow plan store path or permissions failed security validation.",
        },
        PlanError::UnsupportedPlatform(_) => ConversationChangeError::UnsupportedPlatform,
        PlanError::CorruptStore(_) | PlanError::Serialization(_) => {
            ConversationChangeError::CorruptStore {
                reason: "Workflow plan store failed integrity validation.",
            }
        }
        PlanError::Io(_) => ConversationChangeError::Io {
            reason: "Filesystem I/O error while reading the workflow plan store.",
        },
        _ => ConversationChangeError::Validation {
            reason: "Workflow plan identifier or revision failed validation.",
        },
    }
}

fn map_authority_to_change_error(err: &AuthorityError) -> ConversationChangeError {
    map_authority_code_to_change_error(err.code())
}

fn map_authority_code_to_change_error(code: &str) -> ConversationChangeError {
    match code {
        "approval_not_found" => ConversationChangeError::ApprovalNotFound,
        "plan_not_found" => ConversationChangeError::PlanNotFound,
        "unknown_task" => ConversationChangeError::UnknownTask,
        "binding_mismatch" | "thread_uuid_mismatch" => ConversationChangeError::ScopeMismatch {
            reason: "Approval record does not belong to the requested binding and thread scope.",
        },
        "unpublished_or_superseded_plan" => ConversationChangeError::UnpublishedOrSupersededPlan,
        "stale_registry_revision" | "plan_digest_mismatch" => {
            ConversationChangeError::StaleRevision {
                reason:
                    "Approval snapshot no longer matches the active workflow registry or plan digest.",
            }
        }
        "security_violation" => ConversationChangeError::SecurityViolation {
            reason: "Authority store violated private permission or symlink security requirements.",
        },
        "corrupt_store" => ConversationChangeError::CorruptStore {
            reason: "Authority store ledger failed integrity verification.",
        },
        "io" => ConversationChangeError::Io {
            reason: "Filesystem I/O error while accessing the authority store.",
        },
        _ => ConversationChangeError::Validation {
            reason: "Authority store rejected the approval or plan verification.",
        },
    }
}

fn is_deterministic_pre_effect_host_error(err: &HostError) -> bool {
    matches!(
        err,
        HostError::Validation { .. }
            | HostError::ApprovalNotFound { .. }
            | HostError::UnknownTask { .. }
            | HostError::DependencyInputsUnverified { .. }
            | HostError::NativeRouteRequired { .. }
            | HostError::TaskNotStarted { .. }
            | HostError::UnpublishedOrSupersededPlan { .. }
            | HostError::LockBusy { .. }
            | HostError::HostNotReady { .. }
            | HostError::StaleRevision { .. }
            | HostError::IdempotencyConflict { .. }
            | HostError::InvalidRunState { .. }
            | HostError::SecurityViolation { .. }
            | HostError::Authority { .. }
    )
}

fn map_host_cancel_error(err: &HostError) -> ConversationChangeError {
    match err {
        HostError::Validation { .. } => ConversationChangeError::Validation {
            reason: "Task cancellation parameter failed validation.",
        },
        HostError::ApprovalNotFound { .. } => ConversationChangeError::ApprovalNotFound,
        HostError::UnknownTask { .. } => ConversationChangeError::UnknownTask,
        HostError::TaskNotStarted { .. }
        | HostError::NativeRouteRequired { .. }
        | HostError::DependencyInputsUnverified { .. }
        | HostError::InvalidRunState { .. } => ConversationChangeError::InvalidRunState {
            reason: "Target approved task is not in a running or cancellable worker state.",
        },
        HostError::UnpublishedOrSupersededPlan { .. } => {
            ConversationChangeError::UnpublishedOrSupersededPlan
        }
        HostError::LockBusy { .. } => ConversationChangeError::LockBusy,
        HostError::HostNotReady { .. } => ConversationChangeError::HostNotReady,
        HostError::StaleRevision { .. } => ConversationChangeError::StaleRevision {
            reason: "Expected task worker revision does not match the current worker revision.",
        },
        HostError::IdempotencyConflict { .. } => ConversationChangeError::IdempotencyConflict {
            reason: "Deterministic task cancellation command key conflicted with an existing operation.",
        },
        HostError::SecurityViolation { .. } => ConversationChangeError::SecurityViolation {
            reason: "Approved host session store violated private permission or symlink security checks.",
        },
        HostError::Authority { code, .. } => map_authority_code_to_change_error(code),
        _ => ConversationChangeError::OutcomeUnknown {
            reason: "Task cancellation outcome is uncertain after a runtime or I/O failure; inspect task and change state before retrying.",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncertain_cancel_marker_blocks_advancement_without_a_change_record() {
        let root = tempfile::tempdir().expect("temporary plans root");
        let store = ConversationChangeStore::open(root.path()).expect("open change store");
        let binding = "binding-uncertain";
        let affected = "apr_11111111111111111111111111111111";
        let unrelated = "apr_22222222222222222222222222222222";
        let mut ledger = PersistedChangeLedger::new(binding);
        ledger.ambiguous_cancels.push(PendingCancelEffect {
            request_key: "cancel-once".into(),
            request_digest: "a".repeat(64),
            expected_change_revision: 0,
            approval_id: affected.into(),
            task_id: "task-a".into(),
            expected_task_revision: 1,
        });
        store
            .with_binding_lock(binding, |path| {
                write_ledger_locked(store.namespace_dir(), path, &ledger)
            })
            .expect("persist before-effect marker");
        assert!(ledger.records.is_empty());
        assert!(matches!(
            store.unresolved_blocking_changes(binding, Some(affected), Some("dependent-task")),
            Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation)
        ));
        assert!(
            inspect_unresolved_blocking_changes(root.path(), binding, Some(affected), None)
                .is_err()
        );
        let unrelated_summary = store
            .unresolved_blocking_changes(binding, Some(unrelated), None)
            .expect("unrelated approval can be observed");
        assert!(!unrelated_summary.blocked);
        assert!(matches!(
            store.list_changes(
                binding,
                &ListConversationChangesParams {
                    approval_id: Some(affected.into()),
                    task_id: None,
                    unresolved_only: false,
                    cursor: None,
                    limit: None,
                }
            ),
            Err(ConversationChangeError::AmbiguousOutcomePendingReconciliation)
        ));
    }

    #[test]
    fn observed_stop_state_distinguishes_cancel_requested_from_proven_stopped() {
        assert_eq!(
            ObservedTaskStopState::from_execution_status(TaskExecutionStatus::NotStarted),
            ObservedTaskStopState::NotStarted
        );
        assert!(ObservedTaskStopState::NotStarted.is_proven_stopped());

        assert_eq!(
            ObservedTaskStopState::from_execution_status(TaskExecutionStatus::Running),
            ObservedTaskStopState::Running
        );
        assert!(!ObservedTaskStopState::Running.is_proven_stopped());

        assert_eq!(
            ObservedTaskStopState::from_execution_status(TaskExecutionStatus::CancelRequested),
            ObservedTaskStopState::CancelRequested
        );
        assert!(
            !ObservedTaskStopState::CancelRequested.is_proven_stopped(),
            "cancel_requested must never be treated as proven stopped"
        );

        assert_eq!(
            ObservedTaskStopState::from_execution_status(TaskExecutionStatus::Cancelled),
            ObservedTaskStopState::ProvenStopped
        );
        assert!(ObservedTaskStopState::ProvenStopped.is_proven_stopped());

        assert_eq!(
            ObservedTaskStopState::from_execution_status(TaskExecutionStatus::Unknown),
            ObservedTaskStopState::Unknown
        );
        assert!(!ObservedTaskStopState::Unknown.is_proven_stopped());
    }

    #[test]
    fn write_path_coverage_detects_expansions() {
        let allowed = vec!["src/lib.rs".to_string(), "crates/sub/".to_string()];
        assert!(write_paths_within_allowed(
            &["src/lib.rs".to_string(), "crates/sub/mod.rs".to_string()],
            &allowed
        ));
        assert!(!write_paths_within_allowed(
            &["src/extra.rs".to_string()],
            &allowed
        ));
        assert!(!write_paths_within_allowed(
            &["crates/sub_other/mod.rs".to_string()],
            &allowed
        ));
    }

    #[test]
    fn host_authority_error_maps_by_typed_code() {
        let approval_missing = HostError::Authority {
            code: "approval_not_found".to_string(),
            message: "missing approval".to_string(),
        };
        assert_eq!(
            map_host_cancel_error(&approval_missing),
            ConversationChangeError::ApprovalNotFound
        );

        let scope_mismatch = HostError::Authority {
            code: "binding_mismatch".to_string(),
            message: "wrong binding".to_string(),
        };
        assert_eq!(
            map_host_cancel_error(&scope_mismatch).code(),
            "scope_mismatch"
        );

        let superseded = HostError::Authority {
            code: "unpublished_or_superseded_plan".to_string(),
            message: "superseded".to_string(),
        };
        assert_eq!(
            map_host_cancel_error(&superseded),
            ConversationChangeError::UnpublishedOrSupersededPlan
        );
    }
}

#[cfg(test)]
mod write_scope_regressions {
    use super::write_paths_within_allowed;
    #[test]
    fn directory_markers_cannot_expand_frozen_literal_file_scopes() {
        assert!(!write_paths_within_allowed(
            &["src/item/child.rs".into()],
            &["src/item".into()]
        ));
        assert!(!write_paths_within_allowed(
            &["src/item/".into()],
            &["src/item".into()]
        ));
        assert!(write_paths_within_allowed(
            &["src/item/child.rs".into()],
            &["src/item/".into()]
        ));
        assert!(write_paths_within_allowed(
            &["src/item".into()],
            &["src/item".into()]
        ));
        assert!(!write_paths_within_allowed(
            &["src/item-other/child.rs".into()],
            &["src/item/".into()]
        ));
    }
}

#[cfg(test)]
mod advancement_serialization_regressions {
    use super::*;
    use std::time::Duration;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn advancement_guard_excludes_the_actual_change_mutation_lock() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let store = ConversationChangeStore::open(temp.path()).expect("change store");
        let binding = "bnd_advancement_lock";
        let guard = acquire_conversation_advancement_guard(temp.path(), binding)
            .await
            .expect("guard");
        let (tx, rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            store
                .with_binding_lock(binding, |_| {
                    tx.send(()).expect("send mutation lock acquired");
                    Ok(())
                })
                .expect("mutation lock");
        });
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        drop(guard);
        rx.recv_timeout(Duration::from_secs(3))
            .expect("mutation proceeds after advancement");
        writer.join().expect("writer joined");
    }
}
