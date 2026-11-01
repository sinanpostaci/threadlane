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
/// Explicit Linux desktop native endpoint discovery, process-tree proof, and peer validation.
pub mod native_endpoint;
/// Explicit operator-selected native tool-output wake-up configuration and transport.
pub mod native_tool_output;
/// Wire protocol requests, compact status snapshots, and responses.
pub mod protocol;
/// Single-owner SQLite, demo tick, and bounded worker dispatch supervisor over authenticated loopback HTTP.
pub mod supervisor;
/// Startup worker profile configuration, validation, and bounded worker execution adapter.
pub mod workers;

pub use client::{ScopedClient, SupervisorClient, MAX_CLIENT_RESPONSE_BYTES};
pub use codex::{
    build_codex_queue_args, invoke_codex_wakeup, ApprovedSessionWakeupContext, CodexWakeupConfig,
    DEFAULT_CODEX_TIMEOUT_MS, MAX_CODEX_TIMEOUT_MS,
};
pub use descriptor::{
    prepare_state_dir, RuntimeDescriptor, SecretToken, StateLock, DATABASE_FILENAME,
    DESCRIPTOR_FILENAME, DESCRIPTOR_VERSION, LOCK_FILENAME, LOOPBACK_HOST,
};
pub use error::{ApiError, Error, ErrorCode};
pub use mcp::ThreadlaneMcpServer;
pub use native_endpoint::NativeReconnectConfig;
pub use native_tool_output::NativeToolOutputConfig;
pub use protocol::{
    ActivityPage, BindingListResponse, CreateBindingRequest, Event, EventKind, EventPayload,
    HostControlBusyCounts, HostControlConfig, HostControlInfoRequest, HostControlOwnerScope,
    HostControlRuntimeState, HostControlStatusResponse, RetryWakeupRequest, RunSummary,
    ScopedRequest, ScopedResponse, StatusResponse, StopIdleHostRequest, WorkerAcceptanceState,
    WorkerClaim, WorkerCompletionRecord, WorkerExecutionBinding, WorkerOutcomeSummary,
    WorkerProgressSummary, WorkerProgressUpdate, WorkerSnapshot, WorkerStatus,
    WorkerWorktreeSummary, WorkerWorktreeUpdate, MAX_STATUS_RECENT_RUNS,
    WORKER_EXECUTION_BINDING_SCHEMA_VERSION,
};
pub use supervisor::{
    Supervisor, SupervisorConfig, SupervisorHandle, DEFAULT_TICK_INTERVAL_MS, MAX_HTTP_BODY_BYTES,
};
pub use workers::{
    dependency_input_lock_path, read_dependency_input, resolve_worker_execution,
    validate_dependency_input_policy, DependencyInputPolicy, DependencyInputSeal,
    DependencyPredecessorInput, ResolvedWorkerExecution, ValidatedWorkerProfile,
    ValidatedWorkersConfig, VerifiedDependencyInput, WorkerProfileConfig, WorkerSpecTemplate,
    WorkersConfig, DEFAULT_MAX_PARALLEL, DEPENDENCIES_MANIFEST_SUBDIR,
    DEPENDENCY_INPUT_SCHEMA_VERSION, MAX_DEPENDENCY_INPUT_BYTES, MAX_DEPENDENCY_PREDECESSORS,
    MAX_MAX_PARALLEL, MAX_WORKERS_CONFIG_FILE_BYTES, MAX_WORKER_PROFILES, MIN_MAX_PARALLEL,
};
