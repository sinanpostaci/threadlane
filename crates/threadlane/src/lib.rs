#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane`: local single-owner supervisor, authenticated loopback IPC client,
//! CLI, binding-scoped stdio MCP server, and optional Codex wake-up queue bridge
//! for Threadlane Milestone 1.

/// CLI argument parsing and command execution.
pub mod cli;
/// Authenticated local HTTP client and single-binding scoped client.
pub mod client;
/// Optional Codex queue wake-up bridge and argument construction.
pub mod codex;
/// Runtime descriptor, process-lifetime state lock, and filesystem security checks.
pub mod descriptor;
/// Stable error codes, wire error envelopes, and runtime error conversions.
pub mod error;
/// Binding-scoped stdio MCP server built on official `rmcp`.
pub mod mcp;
/// Wire protocol requests, compact status snapshots, and responses.
pub mod protocol;
/// Single-owner SQLite, demo tick, and bounded worker dispatch supervisor over authenticated loopback HTTP.
pub mod supervisor;
/// Startup worker profile configuration, validation, and bounded worker execution adapter.
pub mod workers;

pub use client::{ScopedClient, SupervisorClient, MAX_CLIENT_RESPONSE_BYTES};
pub use codex::{build_codex_queue_args, CodexWakeupConfig, DEFAULT_CODEX_TIMEOUT_MS};
pub use descriptor::{
    prepare_state_dir, RuntimeDescriptor, SecretToken, StateLock, DATABASE_FILENAME,
    DESCRIPTOR_FILENAME, DESCRIPTOR_VERSION, LOCK_FILENAME, LOOPBACK_HOST,
};
pub use error::{ApiError, Error, ErrorCode};
pub use mcp::ThreadlaneMcpServer;
pub use protocol::{
    ActivityPage, BindingListResponse, CreateBindingRequest, Event, EventKind, EventPayload,
    RetryWakeupRequest, RunSummary, ScopedRequest, ScopedResponse, StatusResponse,
    WorkerAcceptanceState, WorkerClaim, WorkerCompletionRecord, WorkerOutcomeSummary,
    WorkerProgressSummary, WorkerProgressUpdate, WorkerSnapshot, WorkerStatus,
    WorkerWorktreeSummary, WorkerWorktreeUpdate, MAX_STATUS_RECENT_RUNS,
};
pub use supervisor::{
    Supervisor, SupervisorConfig, SupervisorHandle, DEFAULT_TICK_INTERVAL_MS, MAX_HTTP_BODY_BYTES,
};
pub use workers::{
    ValidatedWorkerProfile, ValidatedWorkersConfig, WorkerProfileConfig, WorkerSpecTemplate,
    WorkersConfig, DEFAULT_MAX_PARALLEL, MAX_MAX_PARALLEL, MAX_WORKERS_CONFIG_FILE_BYTES,
    MAX_WORKER_PROFILES, MIN_MAX_PARALLEL,
};
