use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{QuestionStatus, RunStatus, WakeupStatus};

/// Specific reason an event batch acknowledgement was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckErrorReason {
    /// The supplied `batch_id` does not exist in durable delivery state.
    UnknownBatch,
    /// The supplied `batch_id` belongs to a different binding.
    CrossBindingBatch,
    /// The batch references events beyond the binding's delivered high-water mark.
    UnseenOrFutureEvents {
        /// Highest sequence number referenced by the batch.
        batch_to_seq: u64,
        /// Highest sequence number delivered to the binding so far.
        delivered_seq: u64,
    },
    /// Acknowledging this batch would skip earlier unacknowledged events.
    SkippedEvents {
        /// Highest sequence number acknowledged on the binding so far.
        acked_seq: u64,
        /// Starting sequence number of the batch being acknowledged.
        batch_from_seq: u64,
    },
}

/// Domain and storage errors returned by `threadlane-core`.
#[derive(Debug, Error)]
pub enum CoreError {
    /// Input failed length, count, or format validation.
    #[error("validation failed on `{field}`: {reason}")]
    Validation {
        /// Name of the invalid field or parameter.
        field: &'static str,
        /// Human-readable validation failure reason.
        reason: String,
    },

    /// Database schema version does not match the engine version.
    #[error("incompatible database schema version {found} (expected {expected})")]
    IncompatibleSchemaVersion {
        /// Schema version found in the SQLite file.
        found: u32,
        /// Schema version expected by this crate.
        expected: u32,
    },

    /// The specified `binding_id` does not exist.
    #[error("binding `{binding_id}` not found")]
    BindingNotFound {
        /// Missing binding identifier.
        binding_id: String,
    },

    /// Multiple pre-existing bindings match `thread_uuid`, making idempotent binding resolution ambiguous.
    #[error(
        "ambiguous binding for thread `{thread_uuid}`: found {match_count} existing bindings ({})",
        binding_ids.join(", ")
    )]
    AmbiguousThreadBinding {
        /// Canonical native Codex thread UUID that matched multiple pre-existing bindings.
        thread_uuid: String,
        /// Number of pre-existing bindings found for `thread_uuid` (`>= 2`).
        match_count: usize,
        /// Ordered `binding_id` values of the conflicting pre-existing bindings.
        binding_ids: Vec<String>,
    },

    /// The specified `run_id` does not exist within `binding_id`.
    #[error("run `{run_id}` not found for binding `{binding_id}`")]
    RunNotFound {
        /// Binding scope used for the lookup.
        binding_id: String,
        /// Missing run identifier.
        run_id: String,
    },

    /// The specified `question_id` does not exist for `(binding_id, run_id)`.
    #[error("question `{question_id}` not found for run `{run_id}` on binding `{binding_id}`")]
    QuestionNotFound {
        /// Binding scope used for the lookup.
        binding_id: String,
        /// Run scope used for the lookup.
        run_id: String,
        /// Missing question identifier.
        question_id: String,
    },

    /// The specified worker (`run_id` or `worker_id`) does not exist within `binding_id`.
    #[error("worker `{worker_id}` not found for binding `{binding_id}`")]
    WorkerNotFound {
        /// Binding scope used for the lookup.
        binding_id: String,
        /// Missing worker or run identifier.
        worker_id: String,
    },

    /// A worker claim or callback did not match the live `(binding_id, run_id, worker_id, revision, claim_id, attempt_count, profile_digest)` state.
    #[error(
        "stale or mismatched worker claim `{claim_id}` for worker `{worker_id}` on run `{run_id}` (binding `{binding_id}`): {reason}"
    )]
    StaleWorkerClaim {
        /// Binding scope owning the worker.
        binding_id: String,
        /// Run identifier owning the worker.
        run_id: String,
        /// Worker identifier.
        worker_id: String,
        /// Claim identifier supplied by the caller.
        claim_id: String,
        /// Detailed mismatch or stale-state reason.
        reason: String,
    },

    /// `expected_revision` did not match the run's current revision.
    #[error(
        "stale revision for run `{run_id}`: expected revision {expected_revision}, actual revision is {actual_revision}"
    )]
    StaleRevision {
        /// Target run identifier.
        run_id: String,
        /// Revision supplied by the caller.
        expected_revision: u64,
        /// Current persisted revision of the run.
        actual_revision: u64,
    },

    /// Question was already superseded, answered, cancelled, or belongs to a prior revision.
    #[error(
        "question `{question_id}` on run `{run_id}` is not active (status: {question_status:?}, question_revision: {question_revision}, current_revision: {current_revision})"
    )]
    StaleQuestion {
        /// Target run identifier.
        run_id: String,
        /// Target question identifier.
        question_id: String,
        /// Persisted status of the question.
        question_status: QuestionStatus,
        /// Revision that emitted the question.
        question_revision: u64,
        /// Current persisted revision of the run.
        current_revision: u64,
    },

    /// Operation is invalid for the run's current lifecycle status.
    #[error("cannot perform `{action}` on run `{run_id}` while in status `{status:?}`")]
    InvalidRunState {
        /// Target run identifier.
        run_id: String,
        /// Current persisted status of the run.
        status: RunStatus,
        /// Attempted operation name.
        action: &'static str,
    },

    /// An idempotent `command_key` was reused on the same binding with different parameters or operation.
    #[error(
        "idempotency conflict on binding `{binding_id}` for command_key `{command_key}`: key already used with different payload"
    )]
    IdempotencyConflict {
        /// Binding scope where the key collision occurred.
        binding_id: String,
        /// Conflicting caller command key.
        command_key: String,
    },

    /// Event batch acknowledgement violated boundary or isolation invariants.
    #[error(
        "invalid event acknowledgement on binding `{binding_id}` for batch `{batch_id}`: {reason:?}"
    )]
    InvalidAck {
        /// Binding scope where acknowledgement was attempted.
        binding_id: String,
        /// Batch identifier supplied by the caller.
        batch_id: String,
        /// Detailed acknowledgement rejection reason.
        reason: AckErrorReason,
    },

    /// Caller attempted to pull events starting after an unseen sequence number.
    #[error(
        "cannot pull events after unseen sequence {requested_after_seq} on binding `{binding_id}` (delivered_seq is {delivered_seq})"
    )]
    UnseenEventCursor {
        /// Binding scope where the pull was attempted.
        binding_id: String,
        /// Requested `after_seq` cursor.
        requested_after_seq: u64,
        /// Highest sequence number delivered to the binding so far.
        delivered_seq: u64,
    },

    /// The specified wake-up `claim_id` does not match an active in-flight wake-up claim.
    #[error("active wake-up claim `{claim_id}` not found")]
    WakeupClaimNotFound {
        /// Unmatched claim identifier.
        claim_id: String,
    },

    /// Wake-up state transition is invalid for the current status.
    #[error(
        "cannot perform `{action}` on wake-up for binding `{binding_id}` while in status `{status:?}`"
    )]
    InvalidWakeupState {
        /// Binding identifier.
        binding_id: String,
        /// Current wake-up status.
        status: WakeupStatus,
        /// Attempted operation name.
        action: &'static str,
    },

    /// Serialized event payload exceeded the maximum bounded size.
    #[error("event payload size {size_bytes} bytes exceeds limit of {max_bytes} bytes")]
    EventPayloadTooLarge {
        /// Actual serialized byte size.
        size_bytes: usize,
        /// Maximum allowed byte size.
        max_bytes: usize,
    },

    /// Serialized response snapshot or page exceeded its configured byte budget.
    #[error("serialized response size {size_bytes} bytes exceeds budget of {max_bytes} bytes")]
    ResponseTooLarge {
        /// Actual serialized JSON byte size including reserved wrapper overhead.
        size_bytes: usize,
        /// Maximum allowed serialized JSON byte budget.
        max_bytes: usize,
    },

    /// Underlying SQLite error.
    #[error("database error: {0}")]
    Storage(#[from] rusqlite::Error),

    /// JSON serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Stored database value violated an internal invariant.
    #[error("corrupt database state: {0}")]
    CorruptState(String),
}
