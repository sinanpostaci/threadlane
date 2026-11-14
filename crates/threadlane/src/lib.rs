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
/// Typed Coworker planning command wake-up context and bounded native Unix WebSocket notification API.
pub mod coworker_planning_wakeup;
/// Typed Coworker job status wake-up context and bounded native Unix WebSocket notification API.
pub mod coworker_wakeup;
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
/// Trusted-local operator transport for durable Worker backlog execution state.
pub mod operator_backlog;
/// Ephemeral claim-scoped worker process presence snapshots and bounded Linux verification.
pub mod presence;
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
pub use coworker_planning_wakeup::{
    format_coworker_planning_wakeup_message, invoke_coworker_planning_wakeup,
    validate_coworker_planning_command_id, CoworkerPlanningNotification,
    CoworkerPlanningWakeupAttempt, CoworkerPlanningWakeupContext,
    COWORKER_PLANNING_COMMAND_ID_HEX_LEN, COWORKER_PLANNING_COMMAND_ID_PREFIX,
};
pub use coworker_wakeup::{
    format_coworker_job_wakeup_message, invoke_coworker_job_wakeup, validate_coworker_job_id,
    validate_coworker_notification_id, validate_coworker_transition_id, CoworkerJobNotification,
    CoworkerJobWakeupAttempt, CoworkerJobWakeupContext, COWORKER_JOB_ID_HEX_LEN,
    COWORKER_JOB_ID_PREFIX, MAX_COWORKER_NOTIFICATION_ID_LEN, MAX_COWORKER_TRANSITION_ID_LEN,
};
pub use descriptor::{
    prepare_state_dir, RuntimeDescriptor, SecretToken, StateLock, DATABASE_FILENAME,
    DESCRIPTOR_FILENAME, DESCRIPTOR_VERSION, LOCK_FILENAME, LOOPBACK_HOST,
};
pub use error::{ApiError, Error, ErrorCode};
pub use mcp::{
    OperatingGuideParams, OperatingGuideResponse, ThreadlaneMcpServer, NATIVE_OPERATOR_DOC_PATH,
    OPERATING_GUIDE_CLI_COMMAND, OPERATING_GUIDE_DOC_PATH, OPERATING_GUIDE_MARKDOWN,
    OPERATING_GUIDE_TOOL, OPERATING_GUIDE_VERSION, PAIRED_MCP_STARTUP_INSTRUCTIONS,
    SCOPED_MCP_STARTUP_INSTRUCTIONS,
};
pub use native_endpoint::NativeReconnectConfig;
pub use native_tool_output::{NativeNotificationDelivery, NativeToolOutputConfig};
pub use operator_backlog::{OperatorWorkerBacklogRequest, OperatorWorkerBacklogResponse};
pub use protocol::{
    ActivityPage, BindingListResponse, ControlRuntimeBusyCounts, ControlRuntimeCapabilities,
    ControlRuntimeInfoResponse, ControlRuntimeRetireRequest, ControlRuntimeRetireResponse,
    ControlRuntimeRetireState, ControlRuntimeRole, ControlRuntimeRuntimeState,
    CreateBindingRequest, EnsureBindingOutcome, EnsureBindingRequest, EnsureBindingResponse, Event,
    EventKind, EventPayload, GetOrCreateBindingOutcome, GetOrCreateBindingRequest,
    GetOrCreateBindingResponse, HostControlBusyCounts, HostControlConfig, HostControlInfoRequest,
    HostControlOwnerScope, HostControlRuntimeState, HostControlStatusResponse,
    IdempotentBindingOutcome, RearmHostWakeupRequest, RearmHostWakeupResponse, RetryWakeupRequest,
    RunSummary, ScopedRequest, ScopedResponse, StatusResponse, StopIdleHostRequest,
    WorkerAcceptanceState, WorkerClaim, WorkerCompletionRecord, WorkerExecutionBinding,
    WorkerOutcomeSummary, WorkerPresenceAvailability, WorkerPresenceFreshness,
    WorkerPresenceSnapshot, WorkerPresenceUnavailableReason, WorkerProgressAttentionReason,
    WorkerProgressSummary, WorkerProgressUpdate, WorkerProgressWatchdogSnapshot,
    WorkerProgressWatchdogStatus, WorkerSnapshot, WorkerStatus, WorkerWorktreeSummary,
    WorkerWorktreeUpdate, CONTROL_RUNTIME_IDLE_RETIRE_MIN_PROTOCOL_VERSION,
    CONTROL_RUNTIME_INFO_PATH, CONTROL_RUNTIME_INSTANCE_ID_HEX_LEN,
    CONTROL_RUNTIME_PROTOCOL_VERSION, CONTROL_RUNTIME_RETIRE_PATH,
    MAX_CONTROL_RUNTIME_PACKAGE_VERSION_BYTES, MAX_STATUS_RECENT_RUNS, SUPERVISOR_RETIRING_MESSAGE,
    WORKER_EXECUTION_BINDING_SCHEMA_VERSION, WORKER_PRESENCE_MAX_SAMPLE_AGE_MS,
    WORKER_PRESENCE_SCHEMA_VERSION,
};
pub use supervisor::{
    Supervisor, SupervisorConfig, SupervisorHandle, DEFAULT_TICK_INTERVAL_MS, MAX_HTTP_BODY_BYTES,
};
pub use workers::{
    compute_frozen_profile_identity_digest, dependency_input_lock_path, read_dependency_input,
    resolve_worker_execution, validate_dependency_input_policy, DependencyInputPolicy,
    DependencyInputSeal, DependencyPredecessorInput, ResolvedWorkerExecution,
    ValidatedWorkerProfile, ValidatedWorkersConfig, VerifiedDependencyInput, WorkerProfileConfig,
    WorkerSpecTemplate, WorkersConfig, DEFAULT_MAX_PARALLEL, DEPENDENCIES_MANIFEST_SUBDIR,
    DEPENDENCY_INPUT_SCHEMA_VERSION, MAX_DEPENDENCY_INPUT_BYTES, MAX_DEPENDENCY_PREDECESSORS,
    MAX_MAX_PARALLEL, MAX_WORKERS_CONFIG_FILE_BYTES, MAX_WORKER_PROFILES, MIN_MAX_PARALLEL,
};
