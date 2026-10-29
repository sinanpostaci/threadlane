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

pub use engine::Core;
pub use error::{AckErrorReason, CoreError};
pub use limits::{
    DEFAULT_DEMO_STEP_DELAY_MS, DEFAULT_EVENT_BATCH_SIZE, FIXED_WAKEUP_MESSAGE, MAX_ANSWER_LEN,
    MAX_CANCEL_REASON_LEN, MAX_COMMAND_KEY_LEN, MAX_DEMO_STEP_DELAY_MS, MAX_EVENT_BATCH_SIZE,
    MAX_EVENT_PAYLOAD_BYTES, MAX_ID_LEN, MAX_INSTRUCTION_LEN, MAX_LABEL_LEN, MAX_WAKEUP_ERROR_LEN,
    SCHEMA_VERSION,
};
pub use types::{
    AckResult, Binding, Command, CommandOutput, Event, EventBatch, EventKind, EventPayload,
    Question, QuestionStatus, Run, RunRevision, RunStatus, TickReport, WakeupClaim, WakeupNotice,
    WakeupOutcome, WakeupStatus,
};
