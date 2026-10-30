#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-core`: transactional, persistent domain and storage engine for Threadlane Milestone 1.
//!
//! This crate owns the SQLite persistence layer, deterministic demo run state machine,
//! per-binding isolation, revision-checked answer/steer/cancel mutations, idempotent
//! command deduplication, ordered event batches with high-water-mark acknowledgements,
//! and persistent wake-up notice coalescing and recovery.

mod engine;
/// Domain and storage error types.
pub mod error;
/// Input validation limits and fixed protocol constants.
pub mod limits;
/// Serializable request, response, run, event, and wake-up types.
pub mod types;
/// Durable worker dispatch ledger, claims, progress, and outcome types.
pub mod worker;

pub use engine::Core;
pub use error::{AckErrorReason, CoreError};
pub use limits::{
    estimate_wire_response_bytes, DEFAULT_DEMO_STEP_DELAY_MS, DEFAULT_EVENT_BATCH_SIZE,
    DEFAULT_PAGE_LIMIT, FIXED_WAKEUP_MESSAGE, MAX_ANSWER_LEN, MAX_CANCEL_REASON_LEN,
    MAX_COMMAND_KEY_LEN, MAX_COMPACT_RESPONSE_BYTES, MAX_CURSOR_LEN, MAX_DEMO_STEP_DELAY_MS,
    MAX_EVENT_BATCH_BYTES, MAX_EVENT_BATCH_SIZE, MAX_EVENT_PAYLOAD_BYTES, MAX_ID_LEN,
    MAX_INSTRUCTION_LEN, MAX_LABEL_LEN, MAX_PAGE_LIMIT, MAX_PAGE_RESPONSE_BYTES,
    MAX_STATUS_RECENT_RUNS, MAX_SUMMARY_TEXT_PREVIEW_CHARS, MAX_WAKEUP_ERROR_LEN,
    MAX_WORKER_CHANGED_PATHS_PREVIEW, MAX_WORKER_DIGEST_LEN, MAX_WORKER_MAX_PARALLEL,
    MAX_WORKER_PATH_BYTES, MAX_WORKER_PROFILES, MAX_WORKER_PROFILE_ID_LEN,
    MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT, MAX_WORKER_SUMMARY_TEXT_BYTES,
    MAX_WORKER_VIOLATION_SUMMARIES, MIN_EVENT_BATCH_BYTES, MIN_WORKER_MAX_PARALLEL,
    RESPONSE_WRAPPER_OVERHEAD_BYTES, SCHEMA_VERSION,
};
pub use types::{
    AckResult, ActivityPage, Binding, Command, CommandOutput, Event, EventBatch, EventKind,
    EventPayload, Question, QuestionHistoryPage, QuestionStatus, RevisionHistoryPage, Run,
    RunListPage, RunRevision, RunStatus, RunSummary, TickReport, WakeupClaim, WakeupNotice,
    WakeupOutcome, WakeupRetryRecord, WakeupStatus,
};
pub use worker::{
    sanitize_worker_text, WorkerAcceptanceState, WorkerClaim, WorkerCompletionRecord,
    WorkerOutcomeSummary, WorkerProgressSummary, WorkerProgressUpdate, WorkerSnapshot,
    WorkerStatus, WorkerWorktreeSummary, WorkerWorktreeUpdate,
};
