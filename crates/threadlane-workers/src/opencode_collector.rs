//! Crate-private owned OpenCode v2 session allocation and post-run metadata collection.
//!
//! This module supervises the fixed public OpenCode v2 CLI commands required to allocate a
//! software-owned session before a worker run and to collect authoritative session metadata after
//! the worker exits:
//!
//! 1. **Allocation (`allocate`)**:
//!    `api --standalone session.create --data <JSON>`
//!    Parses only `{ "data": { "id", "location": { "directory" }, "time": { "created" } } }` and
//!    validates a safe bounded non-empty session identifier, an exact canonical working directory
//!    match, and a creation timestamp at or after `started_at_ms` (and not unreasonably in the
//!    future).
//! 2. **Collection (`collect`)**:
//!    - Phase 1: `api --standalone session.get --param sessionID=<exact software-owned ID>`
//!    - Phase 2: `session export --standalone --sanitize <exact software-owned ID>`
//!    Feeds the captured in-memory byte buffers directly to [`validate_opencode_metadata`],
//!    preserving finite static error codes without persisting or logging raw export content.
//!
//! Every metadata command runs without a shell in its own Unix process group (`process_group(0)`),
//! with `stdin` set to null, `kill_on_drop(true)`, the exact canonical working directory, and the
//! exact per-child environment overrides used by the model run. On Linux, each metadata command
//! prepares a dedicated `O_CLOEXEC` `stdout` pipe before spawning the child and sizes/verifies its
//! kernel pipe capacity via `F_SETPIPE_SZ`/`F_GETPIPE_SZ` to hold at least `max_stdout_bytes`
//! (`64 KiB` for `session.create`/`session.get` and `1 MiB` for `session export`). Both `stdout`
//! and `stderr` are drained concurrently under strict byte caps (`64 KiB` for
//! `session.create`/`session.get`, `1 MiB` for `session export`, and `context.max_stderr_bytes`
//! for `stderr` which is discarded).
//! Cancellation, timeouts (`<= 10s` per command and bounded by the overall run deadline), or
//! output overflows immediately terminate and reap the entire owned Unix process group. Any
//! lingering descendant process or cleanup uncertainty prevents evidence acceptance.

use std::fmt;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::fs_safety::ensure_platform_supported;
use crate::opencode_metadata::{
    validate_opencode_metadata_with_diagnostic, OpenCodeMetadataDiagnostic, OpenCodeMetadataError,
    OpenCodeMetadataEvidence, OpenCodeMetadataStage, OwnedOpenCodeSession,
    MAX_OPENCODE_EXPORT_BYTES, MAX_OPENCODE_INFO_BYTES,
};
use crate::spec::{ResolvedModelTarget, MAX_IDENTIFIER_LEN, MAX_STDERR_BYTES};
use crate::supervisor::{
    finish_or_terminate_surviving_descendants, is_isolated_process_group, is_process_group_alive,
    terminate_and_reap_group, terminate_process_group_members, wait_for_cancellation,
    wait_for_readers_or_signal_stop, ProcessGroupGuard, POST_EXIT_PIPE_DRAIN_TIMEOUT_MS,
};

/// Maximum per-command timeout for OpenCode metadata commands (`10 seconds`).
pub(crate) const MAX_METADATA_COMMAND_TIMEOUT_MS: u64 = 10_000;

/// Maximum accepted byte length of the `session.create` stdout payload (`64 KiB`).
pub(crate) const MAX_OPENCODE_CREATE_BYTES: usize = MAX_OPENCODE_INFO_BYTES;

/// Maximum permitted forward clock drift above `started_at_ms` for `session.create` (`5 minutes`).
pub(crate) const MAX_SESSION_CREATE_FUTURE_DRIFT_MS: u64 = 300_000;

/// Pre-validated execution context shared across OpenCode session allocation and collection.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeCollectorContext {
    /// Validated canonical executable path for the OpenCode CLI.
    pub(crate) executable: PathBuf,
    /// Validated canonical working directory for the worker run.
    pub(crate) canonical_workdir: PathBuf,
    /// Exact per-child environment overrides used by the worker model run.
    pub(crate) child_env: Vec<(String, String)>,
    /// Absolute overall deadline shared across the worker run and metadata commands.
    pub(crate) overall_deadline: Instant,
    /// Trusted wallclock timestamp (milliseconds since Unix epoch) when the run started.
    pub(crate) started_at_ms: u64,
    /// Maximum stderr bytes permitted per metadata command before overflow (`stderr` is dropped).
    pub(crate) max_stderr_bytes: usize,
}

/// Finite static error code returned by OpenCode session allocation or metadata collection.
///
/// Variants carry no raw output, filesystem paths, provider error prose, tokens, or JSON values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OpenCodeCollectorError {
    /// Current host platform does not support Unix process-group supervision.
    UnsupportedPlatform,
    /// Collector context or requested model target failed pre-launch validation.
    InvalidContext,
    /// Spawning the CLI process or isolating its Unix process group failed.
    SpawnFailed,
    /// Operation was cancelled via the cancellation channel before or during command execution.
    Cancelled,
    /// Per-command timeout (`<= 10s`) or overall run deadline expired.
    TimedOut,
    /// Command stdout/stderr byte cap or metadata message cap was exceeded.
    OutputOverflow,
    /// Lingering descendants or pipe-drain uncertainty prevented clean process-group exit.
    CleanupIncomplete,
    /// Metadata command exited with a non-zero exit code or termination signal.
    NonZeroExit,
    /// Required JSON shape, identifier, timestamp type, or field type was malformed.
    InvalidMetadata,
    /// Session identifier, working directory, or message-session scope did not match owned state.
    OwnershipMismatch,
    /// Session or assistant timestamp predates `started_at_ms` or is unreasonably in the future.
    StaleSession,
    /// No assistant model evidence was available in the sanitized session export.
    MissingModel,
    /// Assistant provider or model did not match the immutable requested target.
    ModelMismatch,
    /// Requested effort had no matching per-assistant variant evidence.
    MissingEffort,
    /// Observed assistant variant differed from the requested effort.
    EffortMismatch,
    /// Succeeded session outcome, assistant completion, final `stop`, or idle time was missing.
    MissingCompletion,
    /// Assistant message carried a non-null provider error.
    ProviderError,
}

impl OpenCodeCollectorError {
    /// Returns the finite `snake_case` error code.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::InvalidContext => "invalid_context",
            Self::SpawnFailed => "spawn_failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::OutputOverflow => "output_overflow",
            Self::CleanupIncomplete => "cleanup_incomplete",
            Self::NonZeroExit => "nonzero_exit",
            Self::InvalidMetadata => "invalid_metadata",
            Self::OwnershipMismatch => "ownership_mismatch",
            Self::StaleSession => "stale_session",
            Self::MissingModel => "missing_model",
            Self::ModelMismatch => "model_mismatch",
            Self::MissingEffort => "missing_effort",
            Self::EffortMismatch => "effort_mismatch",
            Self::MissingCompletion => "missing_completion",
            Self::ProviderError => "provider_error",
        }
    }
}

impl fmt::Display for OpenCodeCollectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for OpenCodeCollectorError {}

impl From<OpenCodeMetadataError> for OpenCodeCollectorError {
    fn from(error: OpenCodeMetadataError) -> Self {
        match error {
            OpenCodeMetadataError::OutputOverflow => Self::OutputOverflow,
            OpenCodeMetadataError::InvalidMetadata => Self::InvalidMetadata,
            OpenCodeMetadataError::OwnershipMismatch => Self::OwnershipMismatch,
            OpenCodeMetadataError::StaleSession => Self::StaleSession,
            OpenCodeMetadataError::MissingModel => Self::MissingModel,
            OpenCodeMetadataError::ModelMismatch => Self::ModelMismatch,
            OpenCodeMetadataError::MissingEffort => Self::MissingEffort,
            OpenCodeMetadataError::EffortMismatch => Self::EffortMismatch,
            OpenCodeMetadataError::MissingCompletion => Self::MissingCompletion,
            OpenCodeMetadataError::ProviderError => Self::ProviderError,
        }
    }
}

/// Privacy-safe OpenCode v2 collector failure pairing a coarse
/// [`OpenCodeCollectorError`] with an optional finite [`OpenCodeMetadataStage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OpenCodeCollectorDiagnostic {
    /// Coarse collector or metadata validation error category.
    pub(crate) error: OpenCodeCollectorError,
    /// Finite validation stage when available (`None` for transport errors or legacy undifferentiated `invalid_metadata`).
    pub(crate) stage: Option<OpenCodeMetadataStage>,
}

impl OpenCodeCollectorDiagnostic {
    /// Creates a collector diagnostic for `error` without a validation stage.
    pub(crate) const fn from_error(error: OpenCodeCollectorError) -> Self {
        Self { error, stage: None }
    }

    /// Creates an [`OpenCodeCollectorError::InvalidMetadata`] diagnostic qualified by `stage`.
    pub(crate) const fn invalid_metadata(stage: OpenCodeMetadataStage) -> Self {
        Self {
            error: OpenCodeCollectorError::InvalidMetadata,
            stage: Some(stage),
        }
    }

    /// Returns the finite machine-readable error code.
    ///
    /// Preserves existing semantic error codes (`output_overflow`, `ownership_mismatch`,
    /// `stale_session`, `missing_model`, `model_mismatch`, `missing_effort`, `effort_mismatch`,
    /// `missing_completion`, `provider_error`, `nonzero_exit`, `timed_out`, `cancelled`,
    /// `cleanup_incomplete`, `spawn_failed`, `invalid_context`, `unsupported_platform`),
    /// emits `"invalid_metadata:<stage>"` when `error == InvalidMetadata` and `stage` is `Some`,
    /// and emits `"invalid_metadata"` when `stage` is `None`.
    pub(crate) fn as_str(&self) -> &'static str {
        match (self.error, self.stage) {
            (OpenCodeCollectorError::InvalidMetadata, Some(stage)) => stage.qualified_error_code(),
            (error, _) => error.as_str(),
        }
    }

    /// Parses a stored collector/metadata error code into a structured [`OpenCodeCollectorDiagnostic`],
    /// preserving legacy undifferentiated `"invalid_metadata"` with `stage: None` and rejecting
    /// unrecognized strings.
    pub(crate) fn from_error_code(raw: &str) -> Option<Self> {
        if let Some(meta_diag) = OpenCodeMetadataDiagnostic::from_error_code(raw) {
            return Some(Self::from(meta_diag));
        }
        match raw {
            "unsupported_platform" => Some(Self::from_error(
                OpenCodeCollectorError::UnsupportedPlatform,
            )),
            "invalid_context" => Some(Self::from_error(OpenCodeCollectorError::InvalidContext)),
            "spawn_failed" => Some(Self::from_error(OpenCodeCollectorError::SpawnFailed)),
            "cancelled" => Some(Self::from_error(OpenCodeCollectorError::Cancelled)),
            "timed_out" => Some(Self::from_error(OpenCodeCollectorError::TimedOut)),
            "cleanup_incomplete" => {
                Some(Self::from_error(OpenCodeCollectorError::CleanupIncomplete))
            }
            "nonzero_exit" => Some(Self::from_error(OpenCodeCollectorError::NonZeroExit)),
            _ => None,
        }
    }
}

impl fmt::Display for OpenCodeCollectorDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for OpenCodeCollectorDiagnostic {}

impl From<OpenCodeMetadataDiagnostic> for OpenCodeCollectorDiagnostic {
    fn from(diagnostic: OpenCodeMetadataDiagnostic) -> Self {
        Self {
            error: OpenCodeCollectorError::from(diagnostic.error),
            stage: diagnostic.stage,
        }
    }
}

#[derive(Deserialize)]
struct SessionCreateEnvelope {
    data: SessionCreateData,
}

#[derive(Deserialize)]
struct SessionCreateData {
    id: String,
    location: SessionCreateLocation,
    time: SessionCreateTime,
}

#[derive(Deserialize)]
struct SessionCreateLocation {
    directory: String,
}

#[derive(Deserialize)]
struct SessionCreateTime {
    created: u64,
}

/// Allocates a fresh software-owned OpenCode v2 session before launching the worker run.
///
/// Executes `api --standalone session.create --data <JSON>` under Unix process-group supervision
/// and validates the returned session identifier, canonical working directory, and creation time.
#[cfg(test)]
pub(crate) async fn allocate(
    context: &OpenCodeCollectorContext,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OwnedOpenCodeSession, OpenCodeCollectorError> {
    allocate_with_diagnostic(context, resolved_model, cancel_rx)
        .await
        .map_err(|diagnostic| diagnostic.error)
}

/// Allocates a fresh software-owned OpenCode v2 session before launching the worker run,
/// returning a stage-qualified [`OpenCodeCollectorDiagnostic`] on failure.
pub(crate) async fn allocate_with_diagnostic(
    context: &OpenCodeCollectorContext,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OwnedOpenCodeSession, OpenCodeCollectorDiagnostic> {
    let workdir_str = validate_context(context).map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let argv = build_session_create_argv(workdir_str, resolved_model)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    let create_stdout = run_metadata_command(context, &argv, MAX_OPENCODE_CREATE_BYTES, cancel_rx)
        .await
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    let owned = parse_and_validate_created_session(&create_stdout, context, workdir_str)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    Ok(owned)
}

/// Collects and validates authoritative OpenCode v2 session metadata for an [`OwnedOpenCodeSession`].
///
/// Executes `api --standalone session.get --param sessionID=<ID>` followed by
/// `session export --standalone --sanitize <ID>`, checking cancellation and the overall deadline
/// before each phase, and validates both responses in memory.
#[cfg(test)]
pub(crate) async fn collect(
    context: &OpenCodeCollectorContext,
    owned: &OwnedOpenCodeSession,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OpenCodeMetadataEvidence, OpenCodeCollectorError> {
    collect_with_diagnostic(context, owned, resolved_model, cancel_rx)
        .await
        .map_err(|diagnostic| diagnostic.error)
}

/// Collects and validates authoritative OpenCode v2 session metadata for an [`OwnedOpenCodeSession`],
/// returning a stage-qualified [`OpenCodeCollectorDiagnostic`] on failure.
pub(crate) async fn collect_with_diagnostic(
    context: &OpenCodeCollectorContext,
    owned: &OwnedOpenCodeSession,
    resolved_model: &ResolvedModelTarget,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OpenCodeMetadataEvidence, OpenCodeCollectorDiagnostic> {
    let workdir_str = validate_context(context).map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let _ = split_and_validate_model_target(resolved_model)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    validate_owned_against_context(owned, context, workdir_str)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let get_argv = build_session_get_argv(&owned.session_id)?;
    let info_bytes = run_metadata_command(context, &get_argv, MAX_OPENCODE_INFO_BYTES, cancel_rx)
        .await
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let export_argv = build_session_export_argv(&owned.session_id)?;
    let export_bytes =
        run_metadata_command(context, &export_argv, MAX_OPENCODE_EXPORT_BYTES, cancel_rx)
            .await
            .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let evidence = validate_opencode_metadata_with_diagnostic(
        owned,
        &info_bytes,
        &export_bytes,
        resolved_model,
    )
    .map_err(OpenCodeCollectorDiagnostic::from)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    Ok(evidence)
}

/// Builds the fixed public argv for `api --standalone session.create --data <JSON>`.
pub(crate) fn build_session_create_argv(
    canonical_workdir: &str,
    resolved_model: &ResolvedModelTarget,
) -> Result<Vec<String>, OpenCodeCollectorError> {
    if !is_clean_absolute_path(canonical_workdir) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    let (provider_id, model_id) = split_and_validate_model_target(resolved_model)?;

    let mut model_map = Map::new();
    model_map.insert(
        "providerID".to_string(),
        Value::String(provider_id.to_string()),
    );
    model_map.insert("id".to_string(), Value::String(model_id.to_string()));
    if let Some(variant) = resolved_model.effort.as_deref() {
        model_map.insert("variant".to_string(), Value::String(variant.to_string()));
    }

    let mut location_map = Map::new();
    location_map.insert(
        "directory".to_string(),
        Value::String(canonical_workdir.to_string()),
    );

    let mut root_map = Map::new();
    root_map.insert("agent".to_string(), Value::String("build".to_string()));
    root_map.insert("model".to_string(), Value::Object(model_map));
    root_map.insert("location".to_string(), Value::Object(location_map));

    let data_json = serde_json::to_string(&Value::Object(root_map))
        .map_err(|_| OpenCodeCollectorError::InvalidContext)?;

    Ok(vec![
        "api".to_string(),
        "--standalone".to_string(),
        "session.create".to_string(),
        "--data".to_string(),
        data_json,
    ])
}

/// Builds the fixed public argv for `api --standalone session.get --param sessionID=<ID>`.
pub(crate) fn build_session_get_argv(
    session_id: &str,
) -> Result<Vec<String>, OpenCodeCollectorDiagnostic> {
    if !is_safe_identifier(session_id) {
        return Err(OpenCodeCollectorDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    Ok(vec![
        "api".to_string(),
        "--standalone".to_string(),
        "session.get".to_string(),
        "--param".to_string(),
        format!("sessionID={session_id}"),
    ])
}

/// Builds the fixed public argv for `session export --standalone --sanitize <ID>`.
pub(crate) fn build_session_export_argv(
    session_id: &str,
) -> Result<Vec<String>, OpenCodeCollectorDiagnostic> {
    if !is_safe_identifier(session_id) {
        return Err(OpenCodeCollectorDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    Ok(vec![
        "session".to_string(),
        "export".to_string(),
        "--standalone".to_string(),
        "--sanitize".to_string(),
        session_id.to_string(),
    ])
}

fn parse_and_validate_created_session(
    bytes: &[u8],
    context: &OpenCodeCollectorContext,
    expected_workdir: &str,
) -> Result<OwnedOpenCodeSession, OpenCodeCollectorDiagnostic> {
    if bytes.len() > MAX_OPENCODE_CREATE_BYTES {
        return Err(OpenCodeCollectorDiagnostic::from_error(
            OpenCodeCollectorError::OutputOverflow,
        ));
    }

    let root: Value = serde_json::from_slice(bytes).map_err(|_| {
        OpenCodeCollectorDiagnostic::invalid_metadata(OpenCodeMetadataStage::CreateResponseJson)
    })?;
    if !root.is_object() {
        return Err(OpenCodeCollectorDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CreateResponseJson,
        ));
    }

    let envelope: SessionCreateEnvelope = serde_json::from_slice(bytes).map_err(|_| {
        OpenCodeCollectorDiagnostic::invalid_metadata(OpenCodeMetadataStage::CreateResponseEnvelope)
    })?;

    if !is_safe_identifier(&envelope.data.id) {
        return Err(OpenCodeCollectorDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CreateResponseEnvelope,
        ));
    }

    if !is_clean_absolute_path(&envelope.data.location.directory)
        || envelope.data.location.directory != expected_workdir
    {
        return Err(OpenCodeCollectorDiagnostic::from_error(
            OpenCodeCollectorError::OwnershipMismatch,
        ));
    }

    let created_ms = envelope.data.time.created;
    let max_allowed_created_ms = context
        .started_at_ms
        .saturating_add(MAX_SESSION_CREATE_FUTURE_DRIFT_MS);
    if created_ms < context.started_at_ms
        || created_ms > max_allowed_created_ms
        || created_ms > i64::MAX as u64
    {
        return Err(OpenCodeCollectorDiagnostic::from_error(
            OpenCodeCollectorError::StaleSession,
        ));
    }

    Ok(OwnedOpenCodeSession {
        session_id: envelope.data.id,
        canonical_workdir: context.canonical_workdir.clone(),
        started_at_ms: context.started_at_ms,
    })
}

fn validate_context(context: &OpenCodeCollectorContext) -> Result<&str, OpenCodeCollectorError> {
    ensure_platform_supported().map_err(|_| OpenCodeCollectorError::UnsupportedPlatform)?;

    let exec_str = context
        .executable
        .to_str()
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_clean_absolute_path(exec_str) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    let workdir_str = context
        .canonical_workdir
        .to_str()
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_clean_absolute_path(workdir_str) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    if context.max_stderr_bytes == 0 || context.max_stderr_bytes > MAX_STDERR_BYTES {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    if context.started_at_ms > i64::MAX as u64 {
        return Err(OpenCodeCollectorError::InvalidContext);
    }

    for (key, value) in &context.child_env {
        if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
            return Err(OpenCodeCollectorError::InvalidContext);
        }
    }

    Ok(workdir_str)
}

fn validate_owned_against_context(
    owned: &OwnedOpenCodeSession,
    context: &OpenCodeCollectorContext,
    expected_workdir: &str,
) -> Result<(), OpenCodeCollectorDiagnostic> {
    if !is_safe_identifier(&owned.session_id) {
        return Err(OpenCodeCollectorDiagnostic::invalid_metadata(
            OpenCodeMetadataStage::CallerContract,
        ));
    }
    let owned_workdir = owned.canonical_workdir.to_str().ok_or_else(|| {
        OpenCodeCollectorDiagnostic::invalid_metadata(OpenCodeMetadataStage::CallerContract)
    })?;
    if !is_clean_absolute_path(owned_workdir) || owned_workdir != expected_workdir {
        return Err(OpenCodeCollectorDiagnostic::from_error(
            OpenCodeCollectorError::OwnershipMismatch,
        ));
    }
    if owned.started_at_ms < context.started_at_ms || owned.started_at_ms > i64::MAX as u64 {
        return Err(OpenCodeCollectorDiagnostic::from_error(
            OpenCodeCollectorError::StaleSession,
        ));
    }
    Ok(())
}

fn split_and_validate_model_target(
    resolved_model: &ResolvedModelTarget,
) -> Result<(&str, &str), OpenCodeCollectorError> {
    if resolved_model.base_model.len() > MAX_IDENTIFIER_LEN {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    let (provider_id, model_id) = resolved_model
        .base_model
        .split_once('/')
        .ok_or(OpenCodeCollectorError::InvalidContext)?;
    if !is_safe_identifier(provider_id) || !is_safe_model_identifier(model_id) {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    if resolved_model
        .effort
        .as_deref()
        .is_some_and(|effort| !is_safe_identifier(effort))
    {
        return Err(OpenCodeCollectorError::InvalidContext);
    }
    Ok((provider_id, model_id))
}

fn check_not_cancelled_or_expired(
    overall_deadline: Instant,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Instant, OpenCodeCollectorError> {
    if *cancel_rx.borrow_and_update() {
        return Err(OpenCodeCollectorError::Cancelled);
    }
    let now = Instant::now();
    if now >= overall_deadline {
        return Err(OpenCodeCollectorError::TimedOut);
    }
    let max_phase_deadline = now + Duration::from_millis(MAX_METADATA_COMMAND_TIMEOUT_MS);
    Ok(overall_deadline.min(max_phase_deadline))
}

async fn run_metadata_command(
    context: &OpenCodeCollectorContext,
    argv: &[String],
    max_stdout_bytes: usize,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Vec<u8>, OpenCodeCollectorError> {
    run_metadata_command_with_pipe_config(
        context,
        argv,
        max_stdout_bytes,
        max_stdout_bytes,
        #[cfg(all(test, target_os = "linux"))]
        None,
        cancel_rx,
    )
    .await
}

async fn run_metadata_command_with_pipe_config(
    context: &OpenCodeCollectorContext,
    argv: &[String],
    max_stdout_bytes: usize,
    pipe_capacity_bytes: usize,
    #[cfg(all(test, target_os = "linux"))] pre_read_sync_socket: Option<&tokio::net::UnixDatagram>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<Vec<u8>, OpenCodeCollectorError> {
    let command_deadline = check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)?;

    let (mut child, stdout_pipe) = spawn_isolated_child(context, argv, pipe_capacity_bytes).await?;
    let child_pid = match child.id() {
        Some(pid) => pid,
        None => {
            drop(stdout_pipe);
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    };
    let pgid = match i32::try_from(child_pid) {
        Ok(id) => id,
        Err(_) => {
            drop(stdout_pipe);
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    };

    if !is_isolated_process_group(pgid) {
        drop(stdout_pipe);
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    let mut pgroup_guard = ProcessGroupGuard::new(pgid);

    let Some(stderr_pipe) = child.stderr.take() else {
        drop(stdout_pipe);
        let _ = terminate_and_reap_group(pgid, &mut child).await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    };

    #[cfg(all(test, target_os = "linux"))]
    if let Some(socket) = pre_read_sync_socket {
        if wait_for_pre_read_sync(socket, &mut child, command_deadline)
            .await
            .is_err()
        {
            drop(stdout_pipe);
            drop(stderr_pipe);
            let _ = terminate_and_reap_group(pgid, &mut child).await;
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    }

    let (overflow_tx, mut overflow_rx) = mpsc::unbounded_channel::<()>();
    let (drain_stop_tx, drain_stop_rx) = watch::channel(false);

    let mut stdout_handle = tokio::spawn(drain_stdout_bounded(
        stdout_pipe,
        max_stdout_bytes,
        overflow_tx.clone(),
        drain_stop_rx.clone(),
    ));
    let mut stderr_handle = tokio::spawn(drain_stderr_bounded(
        stderr_pipe,
        context.max_stderr_bytes,
        overflow_tx,
        drain_stop_rx,
    ));

    let mut timed_out = false;
    let mut cancelled = false;
    let mut overflow_signaled = false;
    let mut descendants_terminated_after_exit = false;
    let exit_status: Option<ExitStatus>;

    if *cancel_rx.borrow_and_update() {
        cancelled = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else if Instant::now() >= command_deadline {
        timed_out = true;
        exit_status = terminate_and_reap_group(pgid, &mut child).await;
    } else {
        let timeout_sleep = tokio::time::sleep_until(command_deadline);
        tokio::pin!(timeout_sleep);

        tokio::select! {
            wait_res = child.wait() => {
                exit_status = wait_res.ok();
                descendants_terminated_after_exit =
                    finish_or_terminate_surviving_descendants(
                        pgid,
                        &stdout_handle,
                        &stderr_handle,
                    ).await;
            }
            Some(()) = overflow_rx.recv() => {
                overflow_signaled = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
            () = &mut timeout_sleep => {
                timed_out = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
            () = wait_for_cancellation(cancel_rx) => {
                cancelled = true;
                exit_status = terminate_and_reap_group(pgid, &mut child).await;
            }
        }
    }

    let pipe_drain_timed_out =
        wait_for_readers_or_signal_stop(&stdout_handle, &stderr_handle, &drain_stop_tx).await;

    if is_process_group_alive(pgid) {
        terminate_process_group_members(pgid).await;
    }

    let group_still_alive = is_process_group_alive(pgid);
    if !group_still_alive {
        pgroup_guard.disarm();
    }
    let mut cleanup_incomplete =
        pipe_drain_timed_out || group_still_alive || descendants_terminated_after_exit;

    let (stdout_bytes, stdout_overflow, stdout_read_error) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stdout_handle,
    )
    .await
    {
        Ok(Ok(triple)) => triple,
        Ok(Err(_)) => {
            cleanup_incomplete = true;
            (Vec::new(), false, true)
        }
        Err(_) => {
            stdout_handle.abort();
            cleanup_incomplete = true;
            (Vec::new(), false, true)
        }
    };

    let (stderr_overflow, stderr_read_error) = match tokio::time::timeout(
        Duration::from_millis(POST_EXIT_PIPE_DRAIN_TIMEOUT_MS),
        &mut stderr_handle,
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(_)) => {
            cleanup_incomplete = true;
            (false, true)
        }
        Err(_) => {
            stderr_handle.abort();
            cleanup_incomplete = true;
            (false, true)
        }
    };

    if stdout_read_error || stderr_read_error {
        cleanup_incomplete = true;
    }

    let final_overflow = overflow_signaled || stdout_overflow || stderr_overflow;

    if group_still_alive {
        return Err(OpenCodeCollectorError::CleanupIncomplete);
    }
    if cancelled {
        return Err(OpenCodeCollectorError::Cancelled);
    }
    if timed_out {
        return Err(OpenCodeCollectorError::TimedOut);
    }
    if final_overflow {
        return Err(OpenCodeCollectorError::OutputOverflow);
    }
    if cleanup_incomplete {
        return Err(OpenCodeCollectorError::CleanupIncomplete);
    }

    match exit_status {
        Some(status) if status.success() => Ok(stdout_bytes),
        _ => Err(OpenCodeCollectorError::NonZeroExit),
    }
}

#[cfg(all(test, target_os = "linux"))]
async fn wait_for_pre_read_sync(
    socket: &tokio::net::UnixDatagram,
    child: &mut tokio::process::Child,
    command_deadline: Instant,
) -> Result<(), OpenCodeCollectorError> {
    // Async socket readiness and child waiting are cancellation-safe: a timeout leaves no
    // blocking thread behind. Waiting for exit also proves that stdout reading is postponed
    // until after the producer's single nonblocking write and immediate exit.
    let deadline = command_deadline.min(Instant::now() + Duration::from_secs(5));
    let handshake = async {
        let mut marker = [0u8; 2];
        let len = socket
            .recv(&mut marker)
            .await
            .map_err(|_| OpenCodeCollectorError::SpawnFailed)?;
        if &marker[..len] != b"1" {
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
        let status = child
            .wait()
            .await
            .map_err(|_| OpenCodeCollectorError::SpawnFailed)?;
        if !status.success() {
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
        Ok(())
    };
    tokio::time::timeout_at(deadline, handshake)
        .await
        .map_err(|_| OpenCodeCollectorError::SpawnFailed)?
}

#[cfg(all(test, target_os = "linux"))]
async fn collect_with_export_transport_test_hook(
    context: &OpenCodeCollectorContext,
    owned: &OwnedOpenCodeSession,
    resolved_model: &ResolvedModelTarget,
    export_pipe_capacity_bytes: usize,
    export_pre_read_sync_socket: Option<&tokio::net::UnixDatagram>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> Result<OpenCodeMetadataEvidence, OpenCodeCollectorDiagnostic> {
    let workdir_str = validate_context(context).map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let _ = split_and_validate_model_target(resolved_model)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    validate_owned_against_context(owned, context, workdir_str)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let get_argv = build_session_get_argv(&owned.session_id)?;
    let info_bytes = run_metadata_command(context, &get_argv, MAX_OPENCODE_INFO_BYTES, cancel_rx)
        .await
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let export_argv = build_session_export_argv(&owned.session_id)?;
    let export_bytes = run_metadata_command_with_pipe_config(
        context,
        &export_argv,
        MAX_OPENCODE_EXPORT_BYTES,
        export_pipe_capacity_bytes,
        export_pre_read_sync_socket,
        cancel_rx,
    )
    .await
    .map_err(OpenCodeCollectorDiagnostic::from_error)?;

    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    let evidence = validate_opencode_metadata_with_diagnostic(
        owned,
        &info_bytes,
        &export_bytes,
        resolved_model,
    )
    .map_err(OpenCodeCollectorDiagnostic::from)?;
    check_not_cancelled_or_expired(context.overall_deadline, cancel_rx)
        .map_err(OpenCodeCollectorDiagnostic::from_error)?;
    Ok(evidence)
}

/// Creates a dedicated anonymous pipe for a metadata command's `stdout` on Linux and sizes/verifies
/// its kernel buffer to hold at least `min_capacity_bytes` before the child process is spawned.
///
/// Both descriptors are created atomically with `O_CLOEXEC` so the read end is never inherited by
/// the child across `execve`, and both are immediately wrapped in [`OwnedFd`] so any capacity or
/// `fcntl` error closes both descriptors before returning [`OpenCodeCollectorError::SpawnFailed`].
#[cfg(target_os = "linux")]
// Linux pipe sizing has no safe standard-library API; keep the FD FFI exception local.
#[allow(unsafe_code)]
fn prepare_linux_metadata_stdout_pipe(
    min_capacity_bytes: usize,
) -> Result<(OwnedFd, OwnedFd, usize), OpenCodeCollectorError> {
    if min_capacity_bytes == 0 {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }
    let requested_capacity = libc::c_int::try_from(min_capacity_bytes)
        .map_err(|_| OpenCodeCollectorError::SpawnFailed)?;

    let mut fds: [libc::c_int; 2] = [-1, -1];
    // SAFETY: `fds` is a valid 2-element `c_int` buffer; `O_CLOEXEC` atomically marks both ends close-on-exec.
    let pipe_rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if pipe_rc != 0 || fds[0] < 0 || fds[1] < 0 {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    // SAFETY: `pipe2` succeeded and returned two fresh, valid, unowned file descriptors.
    let read_fd = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: `pipe2` succeeded and returned two fresh, valid, unowned file descriptors.
    let write_fd = unsafe { OwnedFd::from_raw_fd(fds[1]) };

    // SAFETY: `write_fd` is a valid open pipe descriptor owned by `write_fd`.
    let set_rc =
        unsafe { libc::fcntl(write_fd.as_raw_fd(), libc::F_SETPIPE_SZ, requested_capacity) };
    if set_rc < 0 {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }
    let set_capacity = usize::try_from(set_rc).map_err(|_| OpenCodeCollectorError::SpawnFailed)?;
    if set_capacity < min_capacity_bytes {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    // SAFETY: `read_fd` is a valid open pipe descriptor owned by `read_fd`.
    let get_rc = unsafe { libc::fcntl(read_fd.as_raw_fd(), libc::F_GETPIPE_SZ) };
    if get_rc < 0 {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }
    let verified_capacity =
        usize::try_from(get_rc).map_err(|_| OpenCodeCollectorError::SpawnFailed)?;
    if verified_capacity < min_capacity_bytes {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    Ok((read_fd, write_fd, verified_capacity))
}

#[cfg(target_os = "linux")]
async fn spawn_isolated_child(
    context: &OpenCodeCollectorContext,
    argv: &[String],
    pipe_capacity_bytes: usize,
) -> Result<(tokio::process::Child, tokio::process::ChildStdout), OpenCodeCollectorError> {
    let (stdout_read_fd, stdout_write_fd, _verified_capacity) =
        prepare_linux_metadata_stdout_pipe(pipe_capacity_bytes)?;

    let mut cmd = Command::new(&context.executable);
    cmd.args(argv)
        .envs(
            context
                .child_env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .current_dir(&context.canonical_workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_write_fd))
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.process_group(0);

    let spawn_res = cmd.spawn();
    drop(cmd);
    let mut child = spawn_res.map_err(|_| OpenCodeCollectorError::SpawnFailed)?;

    let std_stdout = std::process::ChildStdout::from(stdout_read_fd);
    let stdout_pipe = match tokio::process::ChildStdout::from_std(std_stdout) {
        Ok(pipe) => pipe,
        Err(_) => {
            if let Some(pid) = child.id().and_then(|id| i32::try_from(id).ok()) {
                let _ = terminate_and_reap_group(pid, &mut child).await;
            } else {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
            return Err(OpenCodeCollectorError::SpawnFailed);
        }
    };

    Ok((child, stdout_pipe))
}

#[cfg(not(target_os = "linux"))]
async fn spawn_isolated_child(
    context: &OpenCodeCollectorContext,
    argv: &[String],
    pipe_capacity_bytes: usize,
) -> Result<(tokio::process::Child, tokio::process::ChildStdout), OpenCodeCollectorError> {
    if pipe_capacity_bytes == 0 {
        return Err(OpenCodeCollectorError::SpawnFailed);
    }

    let mut cmd = Command::new(&context.executable);
    cmd.args(argv)
        .envs(
            context
                .child_env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .current_dir(&context.canonical_workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        cmd.process_group(0);
    }

    let mut child = cmd
        .spawn()
        .map_err(|_| OpenCodeCollectorError::SpawnFailed)?;
    let Some(stdout_pipe) = child.stdout.take() else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(OpenCodeCollectorError::SpawnFailed);
    };

    Ok((child, stdout_pipe))
}

async fn drain_stdout_bounded(
    mut reader: tokio::process::ChildStdout,
    max_stdout_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<()>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (Vec<u8>, bool, bool) {
    let mut buf: Vec<u8> = Vec::with_capacity(4096.min(max_stdout_bytes));
    let mut chunk = [0u8; 4096];
    let mut overflow = false;
    let mut read_error = false;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_cancellation(&mut drain_stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            break;
        };
        match read_res {
            Ok(0) => break,
            Ok(n) => {
                if buf.len().saturating_add(n) > max_stdout_bytes {
                    buf.clear();
                    overflow = true;
                    let _ = overflow_tx.send(());
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => {
                buf.clear();
                read_error = true;
                break;
            }
        }
    }

    (buf, overflow, read_error)
}

async fn drain_stderr_bounded(
    mut reader: tokio::process::ChildStderr,
    max_stderr_bytes: usize,
    overflow_tx: mpsc::UnboundedSender<()>,
    mut drain_stop_rx: watch::Receiver<bool>,
) -> (bool, bool) {
    let mut total_bytes: usize = 0;
    let mut chunk = [0u8; 4096];
    let mut overflow = false;
    let mut read_error = false;

    loop {
        let read_res = tokio::select! {
            biased;
            res = reader.read(&mut chunk) => Some(res),
            () = wait_for_cancellation(&mut drain_stop_rx) => None,
        };
        let Some(read_res) = read_res else {
            break;
        };
        match read_res {
            Ok(0) => break,
            Ok(n) => {
                if total_bytes.saturating_add(n) > max_stderr_bytes {
                    overflow = true;
                    let _ = overflow_tx.send(());
                    break;
                }
                total_bytes += n;
            }
            Err(_) => {
                read_error = true;
                break;
            }
        }
    }

    (overflow, read_error)
}

fn is_clean_absolute_path(value: &str) -> bool {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
        || value.contains("//")
        || (value != "/" && value.ends_with('/'))
    {
        return false;
    }
    Path::new(value)
        .components()
        .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

fn is_safe_model_identifier(value: &str) -> bool {
    !value.is_empty() && value.split('/').all(is_safe_identifier)
}

fn is_safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_LEN
        && value.trim() == value
        && !value.starts_with('-')
        && !value.starts_with('.')
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::supervisor::is_process_alive;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    struct TestFixture {
        root_dir: PathBuf,
        workdir: PathBuf,
        bin_path: PathBuf,
        log_path: PathBuf,
    }

    impl TestFixture {
        fn new(script_body: &str) -> Self {
            let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let base = std::env::temp_dir()
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from("/tmp"));
            let root_dir = base.join(format!("threadlane-opencode-collector-test-{pid}-{id}"));
            let workdir = root_dir.join("work");
            fs::create_dir_all(&workdir).expect("create test workdir");
            fs::set_permissions(&root_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod root_dir");
            fs::set_permissions(&workdir, fs::Permissions::from_mode(0o700))
                .expect("chmod workdir");

            let staging_path = root_dir.join("fake-opencode.tmp");
            let bin_path = root_dir.join("fake-opencode");
            let log_path = root_dir.join("invocations.log");

            {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .mode(0o700)
                    .open(&staging_path)
                    .expect("open staging fake script");
                file.write_all(script_body.as_bytes())
                    .expect("write fake script");
                file.sync_all().expect("sync fake script");
            }
            fs::rename(&staging_path, &bin_path).expect("rename fake script into place");

            Self {
                root_dir,
                workdir,
                bin_path,
                log_path,
            }
        }

        fn context(&self, timeout: Duration, started_at_ms: u64) -> OpenCodeCollectorContext {
            OpenCodeCollectorContext {
                executable: self.bin_path.clone(),
                canonical_workdir: self.workdir.clone(),
                child_env: vec![(
                    "OPENCODE_CONFIG_CONTENT".to_string(),
                    r#"{"model":"opencode-go/deepseek-v4.1-flash"}"#.to_string(),
                )],
                overall_deadline: Instant::now() + timeout,
                started_at_ms,
                max_stderr_bytes: 4096,
            }
        }

        fn invocations(&self) -> Vec<String> {
            match fs::read_to_string(&self.log_path) {
                Ok(text) => text.lines().map(str::to_string).collect(),
                Err(_) => Vec::new(),
            }
        }
    }

    impl Drop for TestFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root_dir);
        }
    }

    fn expected_model_target() -> ResolvedModelTarget {
        ResolvedModelTarget {
            base_model: "opencode-go/deepseek-v4.1-flash".to_string(),
            effort: Some("max".to_string()),
            cli_model_arg: "opencode-go/deepseek-v4.1-flash#max".to_string(),
        }
    }

    #[tokio::test]
    async fn valid_create_info_and_sanitized_export_proves_exact_model_and_max_effort() {
        let script = r#"#!/bin/sh
set -eu
LOG_FILE="$(dirname "$0")/invocations.log"
printf '%s\n' "$*" >> "$LOG_FILE"

if [ "${OPENCODE_CONFIG_CONTENT:-}" != '{"model":"opencode-go/deepseek-v4.1-flash"}' ]; then
    echo "missing or mismatched OPENCODE_CONFIG_CONTENT" >&2
    exit 12
fi

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.create" ] && [ "$4" = "--data" ]; then
    printf '%s\n' "$5" > "$(dirname "$0")/create_payload.json"
    printf 'bounded stderr create noise\n' >&2
    cat <<EOF
{"data":{"id":"ses_owned_123","title":"ignored title","location":{"directory":"$(pwd)"},"time":{"created":1050}}}
EOF
    exit 0
fi

if [ "$1" = "api" ] && [ "$2" = "--standalone" ] && [ "$3" = "session.get" ] && [ "$4" = "--param" ] && [ "$5" = "sessionID=ses_owned_123" ]; then
    printf 'bounded stderr get noise\n' >&2
    cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
    exit 0
fi

if [ "$1" = "session" ] && [ "$2" = "export" ] && [ "$3" = "--standalone" ] && [ "$4" = "--sanitize" ] && [ "$5" = "ses_owned_123" ]; then
    printf 'bounded stderr export noise\n' >&2
    cat <<EOF
{"info":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}},"messages":[{"id":"msg_user_1","type":"user","sessionID":"ses_owned_123"},{"id":"msg_asst_1","type":"assistant","sessionID":"ses_owned_123","finish":"stop","time":{"created":1060,"completed":1150},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}
EOF
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        let owned = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect("allocate should succeed");
        assert_eq!(owned.session_id, "ses_owned_123");
        assert_eq!(owned.canonical_workdir, fixture.workdir);
        assert_eq!(owned.started_at_ms, 1_000);

        let create_payload_raw = fs::read_to_string(fixture.root_dir.join("create_payload.json"))
            .expect("read captured session.create payload");
        let create_payload: Value =
            serde_json::from_str(&create_payload_raw).expect("parse session.create payload");
        assert_eq!(create_payload["agent"], "build");
        assert_eq!(create_payload["model"]["providerID"], "opencode-go");
        assert_eq!(create_payload["model"]["id"], "deepseek-v4.1-flash");
        assert_eq!(create_payload["model"]["variant"], "max");
        assert_eq!(
            create_payload["location"]["directory"],
            fixture.workdir.to_str().unwrap()
        );

        let evidence = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect("collect should succeed");
        assert_eq!(evidence.source, "opencode_sanitized_session_export");
        assert_eq!(evidence.strength, "cli_reported_per_assistant");
        assert_eq!(evidence.session_id, "ses_owned_123");
        assert_eq!(evidence.observed_model, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(evidence.observed_effort.as_deref(), Some("max"));
        assert_eq!(evidence.assistant_message_count, 1);
        assert_eq!(evidence.terminal_message_id, "msg_asst_1");

        let invocations = fixture.invocations();
        assert_eq!(invocations.len(), 3);
        assert!(invocations[0].starts_with("api --standalone session.create --data "));
        assert_eq!(
            invocations[1],
            "api --standalone session.get --param sessionID=ses_owned_123"
        );
        assert_eq!(
            invocations[2],
            "session export --standalone --sanitize ses_owned_123"
        );
    }

    #[tokio::test]
    async fn wrong_create_identity_time_or_cwd_refuses_before_metadata() {
        let script = r#"#!/bin/sh
set -eu
MODE_FILE="$(dirname "$0")/mode.txt"
LOG_FILE="$(dirname "$0")/invocations.log"
printf '%s\n' "$*" >> "$LOG_FILE"
MODE="$(cat "$MODE_FILE")"

if [ "$1" = "api" ] && [ "$3" = "session.create" ]; then
    case "$MODE" in
        bad_id)
            printf '{"data":{"id":"../escaped","location":{"directory":"%s"},"time":{"created":1050}}}\n' "$(pwd)"
            ;;
        stale_time)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":999}}}\n' "$(pwd)"
            ;;
        future_time)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":999999999}}}\n' "$(pwd)"
            ;;
        wrong_cwd)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"/tmp/unrelated-workdir"},"time":{"created":1050}}}\n'
            ;;
    esac
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        fs::write(&mode_path, "bad_id").expect("write bad_id mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("unsafe session ID must fail");
        assert_eq!(err, OpenCodeCollectorError::InvalidMetadata);

        fs::write(&mode_path, "stale_time").expect("write stale_time mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("stale session creation timestamp must fail");
        assert_eq!(err, OpenCodeCollectorError::StaleSession);

        fs::write(&mode_path, "future_time").expect("write future_time mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("unreasonably future session creation timestamp must fail");
        assert_eq!(err, OpenCodeCollectorError::StaleSession);

        fs::write(&mode_path, "wrong_cwd").expect("write wrong_cwd mode");
        let err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("mismatched session directory must fail");
        assert_eq!(err, OpenCodeCollectorError::OwnershipMismatch);

        let before_collect_count = fixture.invocations().len();
        let mismatched_owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: PathBuf::from("/tmp/different-owned-workdir"),
            started_at_ms: 1_000,
        };
        let collect_err = collect(&context, &mismatched_owned, &target, &mut cancel_rx)
            .await
            .expect_err("collect with wrong owned cwd must refuse before spawning");
        assert_eq!(collect_err, OpenCodeCollectorError::OwnershipMismatch);
        assert_eq!(
            fixture.invocations().len(),
            before_collect_count,
            "collect must not spawn any CLI command when owned session state mismatches context"
        );
    }

    #[tokio::test]
    async fn metadata_error_never_echoes_sentinel() {
        let sentinel = "SENTINEL_SECRET_TOKEN_DO_NOT_ECHO_999";
        let script = r#"#!/bin/sh
set -eu
MODE="$(cat "$(dirname "$0")/mode.txt")"
SENTINEL="SENTINEL_SECRET_TOKEN_DO_NOT_ECHO_999"

if [ "$1" = "api" ] && [ "$3" = "session.create" ]; then
    printf 'stderr leak %s\n' "$SENTINEL" >&2
    printf 'not-json-%s\n' "$SENTINEL"
    exit 0
fi

if [ "$1" = "api" ] && [ "$3" = "session.get" ]; then
    if [ "$MODE" = "nonzero" ]; then
        printf 'stderr failure %s\n' "$SENTINEL" >&2
        printf '{"error":"%s"}\n' "$SENTINEL"
        exit 7
    fi
    cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
    exit 0
fi

if [ "$1" = "session" ] && [ "$2" = "export" ]; then
    cat <<EOF
{"info":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}},"messages":[{"id":"msg_asst_1","type":"assistant","sessionID":"ses_owned_123","finish":"stop","error":{"message":"$SENTINEL"},"time":{"created":1060,"completed":1150},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"}}]}
EOF
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let context = fixture.context(Duration::from_secs(5), 1_000);
        let target = expected_model_target();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);

        fs::write(&mode_path, "malformed_create").expect("write mode");
        let create_err = allocate(&context, &target, &mut cancel_rx)
            .await
            .expect_err("malformed create output must fail");
        assert_eq!(create_err, OpenCodeCollectorError::InvalidMetadata);
        assert!(!create_err.to_string().contains(sentinel));
        assert!(!format!("{create_err:?}").contains(sentinel));

        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };

        fs::write(&mode_path, "nonzero").expect("write mode");
        let nonzero_err = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("nonzero session.get must fail");
        assert_eq!(nonzero_err, OpenCodeCollectorError::NonZeroExit);
        assert!(!nonzero_err.to_string().contains(sentinel));
        assert!(!format!("{nonzero_err:?}").contains(sentinel));

        fs::write(&mode_path, "provider_error").expect("write mode");
        let provider_err = collect(&context, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("assistant provider error must fail");
        assert_eq!(provider_err, OpenCodeCollectorError::ProviderError);
        assert!(!provider_err.to_string().contains(sentinel));
        assert!(!format!("{provider_err:?}").contains(sentinel));
    }

    #[tokio::test]
    async fn timeout_cancel_overflow_and_lingering_descendant_kill_reap_with_no_later_phase() {
        let script = r#"#!/bin/sh
set -eu
ROOT_DIR="$(dirname "$0")"
MODE="$(cat "$ROOT_DIR/mode.txt")"
LOG_FILE="$ROOT_DIR/invocations.log"
PID_FILE="$ROOT_DIR/descendant.pid"
printf '%s\n' "$*" >> "$LOG_FILE"

if [ "$1" = "api" ] && [ "$3" = "session.get" ]; then
    case "$MODE" in
        timeout)
            sleep 60 &
            echo $! > "$PID_FILE"
            wait
            ;;
        cancel)
            sleep 60 &
            echo $! > "$PID_FILE"
            wait
            ;;
        overflow)
            sleep 60 &
            echo $! > "$PID_FILE"
            dd if=/dev/zero bs=1024 count=80 2>/dev/null
            wait
            ;;
        lingering)
            sleep 60 >/dev/null 2>&1 &
            echo $! > "$PID_FILE"
            cat <<EOF
{"data":{"id":"ses_owned_123","outcome":"succeeded","location":{"directory":"$(pwd)"},"time":{"created":1050,"idle":1200}}}
EOF
            exit 0
            ;;
    esac
fi

if [ "$1" = "session" ] && [ "$2" = "export" ]; then
    printf 'EXPORT_SHOULD_NOT_RUN\n' >> "$LOG_FILE"
    exit 0
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let pid_path = fixture.root_dir.join("descendant.pid");
        let target = expected_model_target();
        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };

        // 1. Timeout kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "timeout").expect("write timeout mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let timeout_ctx = fixture.context(Duration::from_millis(120), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&timeout_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("timeout must fail");
        assert_eq!(err, OpenCodeCollectorError::TimedOut);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on timeout"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 2. Cancellation kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "cancel").expect("write cancel mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let cancel_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let cancel_trigger = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = cancel_tx.send(true);
        });
        let err = collect(&cancel_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("cancellation must fail");
        let _ = cancel_trigger.await;
        assert_eq!(err, OpenCodeCollectorError::Cancelled);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on cancellation"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 3. Stdout overflow (>64KiB) kills and reaps descendant and does not launch export phase.
        fs::write(&mode_path, "overflow").expect("write overflow mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let overflow_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&overflow_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("stdout overflow must fail");
        assert_eq!(err, OpenCodeCollectorError::OutputOverflow);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "descendant must be killed and reaped on overflow"
        );
        assert_eq!(fixture.invocations().len(), 1);

        // 4. Lingering descendant after direct child exit 0 is terminated/reaped and prevents success.
        fs::write(&mode_path, "lingering").expect("write lingering mode");
        let _ = fs::remove_file(&fixture.log_path);
        let _ = fs::remove_file(&pid_path);
        let lingering_ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let err = collect(&lingering_ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("lingering descendant after exit 0 must fail cleanup check");
        assert_eq!(err, OpenCodeCollectorError::CleanupIncomplete);
        let desc_pid: u32 = fs::read_to_string(&pid_path)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse pid");
        assert!(
            !is_process_alive(desc_pid),
            "lingering descendant must be terminated and reaped"
        );
        assert_eq!(fixture.invocations().len(), 1);
    }

    #[tokio::test]
    async fn collector_diagnostics_distinguish_create_info_export_and_metadata_stages_without_leaking_sentinels(
    ) {
        let script = r#"#!/bin/sh
set -eu
LOG_FILE="$(dirname "$0")/invocations.log"
MODE_FILE="$(dirname "$0")/mode.txt"
printf '%s\n' "$*" >> "$LOG_FILE"
MODE="$(cat "$MODE_FILE")"

if [ "${1:-}" = "api" ] && [ "${2:-}" = "--standalone" ] && [ "${3:-}" = "session.create" ]; then
    case "$MODE" in
        create_bad_json)
            printf 'not-json SENTINEL_CREATE_RAW sk-secret-create\n'
            exit 0
            ;;
        create_bad_envelope)
            printf '{"data":{"id":"   ","title":"SENTINEL_CREATE_TITLE","location":{"directory":"%s"},"time":{"created":1500}}}\n' "$(pwd)"
            exit 0
            ;;
        create_wrong_dir)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"/tmp/SENTINEL_WRONG_PATH"},"time":{"created":1500}}}\n'
            exit 0
            ;;
    esac
fi

if [ "${1:-}" = "api" ] && [ "${2:-}" = "--standalone" ] && [ "${3:-}" = "session.get" ]; then
    case "$MODE" in
        info_bad_json)
            printf 'corrupt-info SENTINEL_INFO_RAW https://secret.example/internal\n'
            exit 0
            ;;
        info_bad_envelope)
            printf '{"unexpected":"SENTINEL_INFO_ENVELOPE"}\n'
            exit 0
            ;;
        export_bad_json|export_bad_envelope|terminal_bad_finish)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"outcome":"succeeded","time":{"created":1500,"updated":2600,"idle":2600}}}\n' "$(pwd)"
            exit 0
            ;;
    esac
fi

if [ "${1:-}" = "session" ] && [ "${2:-}" = "export" ]; then
    case "$MODE" in
        info_bad_json|info_bad_envelope)
            printf '{"info":{"id":"ses_owned_123","outcome":"succeeded","time":{"created":1500,"updated":2600,"idle":2600}},"messages":[{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":"stop","model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"},"time":{"created":1700,"completed":2500}}]}\n'
            exit 0
            ;;
        export_bad_json)
            printf '{corrupt export SENTINEL_EXPORT_RAW\n'
            exit 0
            ;;
        export_bad_envelope)
            printf '{"info":{"id":"ses_owned_123","outcome":"succeeded","time":{"created":1500,"updated":2600,"idle":2600}},"messages":"SENTINEL_EXPORT_ENVELOPE"}\n'
            exit 0
            ;;
        terminal_bad_finish)
            printf '{"info":{"id":"ses_owned_123","outcome":"succeeded","time":{"created":1500,"updated":2600,"idle":2600}},"messages":[{"id":"msg_a_1","sessionID":"ses_owned_123","type":"assistant","finish":{"reason":"SENTINEL_FINISH_OBJ"},"model":{"providerID":"opencode-go","id":"deepseek-v4.1-flash","variant":"max"},"time":{"created":1700,"completed":2500}}]}\n'
            exit 0
            ;;
    esac
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let target = expected_model_target();
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };
        let sentinels = [
            "SENTINEL_CREATE_RAW",
            "sk-secret-create",
            "SENTINEL_CREATE_TITLE",
            "SENTINEL_WRONG_PATH",
            "SENTINEL_INFO_RAW",
            "https://secret.example/internal",
            "SENTINEL_INFO_ENVELOPE",
            "SENTINEL_FINISH_OBJ",
            "SENTINEL_EXPORT_RAW",
            "SENTINEL_EXPORT_ENVELOPE",
            "ses_owned_123",
        ];

        // 1. Create response invalid JSON -> create_response_json stage.
        fs::write(&mode_path, "create_bad_json").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
            .await
            .expect_err("create bad json must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::CreateResponseJson));
        assert_eq!(diag.as_str(), "invalid_metadata:create_response_json");
        for sentinel in sentinels {
            assert!(!diag.as_str().contains(sentinel));
            assert!(!diag.to_string().contains(sentinel));
        }

        // 2. Create response invalid envelope -> create_response_envelope stage.
        fs::write(&mode_path, "create_bad_envelope").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
            .await
            .expect_err("create bad envelope must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(
            diag.stage,
            Some(OpenCodeMetadataStage::CreateResponseEnvelope)
        );
        assert_eq!(diag.as_str(), "invalid_metadata:create_response_envelope");

        // 3. Create wrong directory preserves semantic ownership_mismatch without stage suffix.
        fs::write(&mode_path, "create_wrong_dir").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
            .await
            .expect_err("create wrong dir must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::OwnershipMismatch);
        assert_eq!(diag.stage, None);
        assert_eq!(diag.as_str(), "ownership_mismatch");

        // 4. Collect caller contract rejection before subprocess launch.
        let _ = fs::remove_file(&fixture.log_path);
        let mut bad_owned = owned.clone();
        bad_owned.session_id = "-flag_injection".to_string();
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &bad_owned, &target, &mut cancel_rx)
            .await
            .expect_err("invalid session id must fail caller contract");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::CallerContract));
        assert_eq!(diag.as_str(), "invalid_metadata:caller_contract");
        assert!(fixture.invocations().is_empty());

        // 5. Collect info_response_json stage (runs both session.get and export before validation).
        fs::write(&mode_path, "info_bad_json").expect("write mode");
        let _ = fs::remove_file(&fixture.log_path);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("info bad json must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::InfoResponseJson));
        assert_eq!(diag.as_str(), "invalid_metadata:info_response_json");
        assert_eq!(fixture.invocations().len(), 2);
        for sentinel in sentinels {
            assert!(!diag.as_str().contains(sentinel));
            assert!(!diag.to_string().contains(sentinel));
        }

        // 6. Collect info_response_envelope stage.
        fs::write(&mode_path, "info_bad_envelope").expect("write mode");
        let _ = fs::remove_file(&fixture.log_path);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("info bad envelope must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(
            diag.stage,
            Some(OpenCodeMetadataStage::InfoResponseEnvelope)
        );
        assert_eq!(diag.as_str(), "invalid_metadata:info_response_envelope");
        assert_eq!(fixture.invocations().len(), 2);

        // 7. Collect export_json stage.
        fs::write(&mode_path, "export_bad_json").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("export bad json must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportJson));
        assert_eq!(diag.as_str(), "invalid_metadata:export_json");
        for sentinel in sentinels {
            assert!(!diag.as_str().contains(sentinel));
            assert!(!diag.to_string().contains(sentinel));
        }

        // 8. Collect export_envelope stage.
        fs::write(&mode_path, "export_bad_envelope").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("export bad envelope must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportEnvelope));
        assert_eq!(diag.as_str(), "invalid_metadata:export_envelope");

        // 9. Collect terminal_completion stage.
        fs::write(&mode_path, "terminal_bad_finish").expect("write mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("terminal bad finish must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::TerminalCompletion));
        assert_eq!(diag.as_str(), "invalid_metadata:terminal_completion");
        for sentinel in sentinels {
            assert!(!diag.as_str().contains(sentinel));
            assert!(!diag.to_string().contains(sentinel));
        }

        // 10. Legacy and stage-qualified collector error codes decode safely.
        assert_eq!(
            OpenCodeCollectorDiagnostic::from_error_code("invalid_metadata"),
            Some(OpenCodeCollectorDiagnostic {
                error: OpenCodeCollectorError::InvalidMetadata,
                stage: None,
            })
        );
        assert_eq!(
            OpenCodeCollectorDiagnostic::from_error_code("invalid_metadata:export_json"),
            Some(OpenCodeCollectorDiagnostic {
                error: OpenCodeCollectorError::InvalidMetadata,
                stage: Some(OpenCodeMetadataStage::ExportJson),
            })
        );
        assert_eq!(
            OpenCodeCollectorDiagnostic::from_error_code("invalid_metadata:unknown_stage"),
            None
        );
    }

    #[tokio::test]
    async fn create_session_rejects_duplicate_known_fields_from_raw_bytes_without_collapsing() {
        let script = r#"#!/bin/sh
set -eu
MODE="$(cat "$(dirname "$0")/mode.txt")"
WORKDIR="$(pwd)"

if [ "${1:-}" = "api" ] && [ "${2:-}" = "--standalone" ] && [ "${3:-}" = "session.create" ]; then
    case "$MODE" in
        dup_data)
            printf '{"data":{"id":"../bad_first_SENTINEL","location":{"directory":"/tmp/wrong_first_SENTINEL"},"time":{"created":100}},"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":1050}}}\n' "$WORKDIR"
            exit 0
            ;;
        dup_id)
            printf '{"data":{"id":"../bad_first_SENTINEL","id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":1050}}}\n' "$WORKDIR"
            exit 0
            ;;
        dup_location_directory)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"/tmp/wrong_first_SENTINEL","directory":"%s"},"time":{"created":1050}}}\n' "$WORKDIR"
            exit 0
            ;;
        dup_time_created)
            printf '{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":100,"created":1050}}}\n' "$WORKDIR"
            exit 0
            ;;
        non_object_root)
            printf '[{"data":{"id":"ses_owned_123","location":{"directory":"%s"},"time":{"created":1050}}}]\n' "$WORKDIR"
            exit 0
            ;;
        valid_with_unknown_fields)
            printf '{"extra_root":"ignored","data":{"id":"ses_owned_123","title":"ignored","extra_dup":1,"extra_dup":2,"location":{"directory":"%s","extra_loc":"ignored"},"time":{"created":1050,"updated":1050}}}\n' "$WORKDIR"
            exit 0
            ;;
    esac
fi

exit 99
"#;
        let fixture = TestFixture::new(script);
        let mode_path = fixture.root_dir.join("mode.txt");
        let target = expected_model_target();
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let workdir_str = fixture.workdir.to_str().expect("utf-8 workdir");

        let duplicate_fixtures = [
            (
                "dup_data",
                format!(
                    r#"{{"data":{{"id":"../bad_first_SENTINEL","location":{{"directory":"/tmp/wrong_first_SENTINEL"}},"time":{{"created":100}}}},"data":{{"id":"ses_owned_123","location":{{"directory":"{workdir_str}"}},"time":{{"created":1050}}}}}}"#
                ),
            ),
            (
                "dup_id",
                format!(
                    r#"{{"data":{{"id":"../bad_first_SENTINEL","id":"ses_owned_123","location":{{"directory":"{workdir_str}"}},"time":{{"created":1050}}}}}}"#
                ),
            ),
            (
                "dup_location_directory",
                format!(
                    r#"{{"data":{{"id":"ses_owned_123","location":{{"directory":"/tmp/wrong_first_SENTINEL","directory":"{workdir_str}"}},"time":{{"created":1050}}}}}}"#
                ),
            ),
            (
                "dup_time_created",
                format!(
                    r#"{{"data":{{"id":"ses_owned_123","location":{{"directory":"{workdir_str}"}},"time":{{"created":100,"created":1050}}}}}}"#
                ),
            ),
        ];

        for (mode, raw_json) in &duplicate_fixtures {
            let direct_diag =
                parse_and_validate_created_session(raw_json.as_bytes(), &ctx, workdir_str)
                    .expect_err("duplicate known field in raw JSON must fail");
            assert_eq!(direct_diag.error, OpenCodeCollectorError::InvalidMetadata);
            assert_eq!(
                direct_diag.stage,
                Some(OpenCodeMetadataStage::CreateResponseEnvelope)
            );
            assert_eq!(
                direct_diag.as_str(),
                "invalid_metadata:create_response_envelope"
            );

            fs::write(&mode_path, mode).expect("write duplicate mode");
            let (_cancel_tx, mut cancel_rx) = watch::channel(false);
            let diag = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
                .await
                .expect_err("allocate_with_diagnostic must reject duplicate known field");
            assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
            assert_eq!(
                diag.stage,
                Some(OpenCodeMetadataStage::CreateResponseEnvelope)
            );
            assert_eq!(diag.as_str(), "invalid_metadata:create_response_envelope");
            assert!(!diag.to_string().contains("duplicate field"));
            assert!(!format!("{diag:?}").contains("duplicate field"));
            assert!(!diag.to_string().contains("SENTINEL"));
            assert!(!format!("{diag:?}").contains("SENTINEL"));

            let (_cancel_tx, mut cancel_rx) = watch::channel(false);
            let coarse_err = allocate(&ctx, &target, &mut cancel_rx)
                .await
                .expect_err("allocate must reject duplicate known field");
            assert_eq!(coarse_err, OpenCodeCollectorError::InvalidMetadata);
        }

        // Non-object root JSON remains classified at CreateResponseJson.
        fs::write(&mode_path, "non_object_root").expect("write non_object_root mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let root_diag = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
            .await
            .expect_err("non-object root must fail at CreateResponseJson");
        assert_eq!(root_diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(
            root_diag.stage,
            Some(OpenCodeMetadataStage::CreateResponseJson)
        );
        assert_eq!(root_diag.as_str(), "invalid_metadata:create_response_json");

        // Unrelated unknown fields remain ignored on a valid create envelope.
        fs::write(&mode_path, "valid_with_unknown_fields")
            .expect("write valid_with_unknown_fields mode");
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let owned = allocate_with_diagnostic(&ctx, &target, &mut cancel_rx)
            .await
            .expect("valid create envelope with unrelated unknown fields should succeed");
        assert_eq!(owned.session_id, "ses_owned_123");
        assert_eq!(owned.canonical_workdir, fixture.workdir);
        assert_eq!(owned.started_at_ms, 1_000);
    }

    #[cfg(target_os = "linux")]
    fn count_open_fds_matching_stdout_pipe_inode() -> usize {
        use std::os::unix::fs::MetadataExt;
        let Ok(stdout_meta) = fs::metadata("/proc/self/fd/1") else {
            return 0;
        };
        let target_dev = stdout_meta.dev();
        let target_ino = stdout_meta.ino();
        let Ok(entries) = fs::read_dir("/proc/self/fd") else {
            return 0;
        };
        let mut matches = 0usize;
        for entry in entries.flatten() {
            if let Ok(meta) = fs::metadata(entry.path()) {
                if meta.dev() == target_dev && meta.ino() == target_ino {
                    matches += 1;
                }
            }
        }
        matches
    }

    #[cfg(target_os = "linux")]
    #[test]
    // Only this synthetic child needs raw descriptor operations and an unflushed exit.
    #[allow(unsafe_code)]
    fn abrupt_nonblocking_export_child_entrypoint() {
        let Some(payload_path) = std::env::var_os("THREADLANE_TEST_ABRUPT_PAYLOAD_PATH") else {
            return;
        };
        let result_path = std::env::var_os("THREADLANE_TEST_ABRUPT_RESULT_PATH")
            .expect("THREADLANE_TEST_ABRUPT_RESULT_PATH must be set");
        let ready_socket = std::env::var_os("THREADLANE_TEST_ABRUPT_READY_SOCKET");

        let payload = fs::read(&payload_path).expect("read abrupt payload file");

        // Restore the metadata stdout pipe from fd 3 onto STDOUT_FILENO (fd 1) and close fd 3.
        // SAFETY: fd 3 was duplicated from the child's initial stdout by the wrapper script.
        unsafe {
            if libc::dup2(3, libc::STDOUT_FILENO) < 0 {
                libc::_exit(91);
            }
            let _ = libc::close(3);
        }

        // Verify that the child's stdout pipe was already sized before spawn and that no extra
        // descriptor for this pipe (such as the parent's read end) leaked across execve.
        // SAFETY: `STDOUT_FILENO` is valid and open.
        let child_pipe_sz = unsafe { libc::fcntl(libc::STDOUT_FILENO, libc::F_GETPIPE_SZ) };
        let pipe_fds = count_open_fds_matching_stdout_pipe_inode();

        // SAFETY: `STDOUT_FILENO` is valid and open; set non-blocking mode and perform one write.
        let write_rc = unsafe {
            let flags = libc::fcntl(libc::STDOUT_FILENO, libc::F_GETFL);
            if flags < 0 {
                libc::_exit(92);
            }
            if libc::fcntl(libc::STDOUT_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                libc::_exit(93);
            }
            libc::write(libc::STDOUT_FILENO, payload.as_ptr().cast(), payload.len())
        };

        let _ = fs::write(
            &result_path,
            format!("write_rc={write_rc} pipe_sz={child_pipe_sz} pipe_fds={pipe_fds}\n"),
        );

        if let Some(socket_path) = ready_socket {
            let socket = std::os::unix::net::UnixDatagram::unbound().expect("child sync socket");
            socket
                .send_to(b"1", socket_path)
                .expect("signal completed nonblocking write");
        }

        // SAFETY: Immediate process exit without stdio flushing, matching abrupt CLI termination.
        unsafe {
            libc::_exit(0);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    // Inspect the kernel's FD flags and capacity without broadening the crate's unsafe boundary.
    #[allow(unsafe_code)]
    fn linux_metadata_stdout_pipe_presizes_verifies_cloexec_and_rejects_invalid_capacity() {
        for target_capacity in [
            MAX_OPENCODE_CREATE_BYTES,
            MAX_OPENCODE_INFO_BYTES,
            MAX_OPENCODE_EXPORT_BYTES,
        ] {
            let (read_fd, write_fd, verified_capacity) =
                prepare_linux_metadata_stdout_pipe(target_capacity)
                    .expect("prepare_linux_metadata_stdout_pipe should succeed");
            assert!(
                verified_capacity >= target_capacity,
                "verified capacity {verified_capacity} must be >= {target_capacity}"
            );

            // SAFETY: `read_fd` and `write_fd` are valid open descriptors owned by `OwnedFd`.
            unsafe {
                let read_sz = libc::fcntl(read_fd.as_raw_fd(), libc::F_GETPIPE_SZ);
                let write_sz = libc::fcntl(write_fd.as_raw_fd(), libc::F_GETPIPE_SZ);
                assert_eq!(usize::try_from(read_sz).unwrap(), verified_capacity);
                assert_eq!(usize::try_from(write_sz).unwrap(), verified_capacity);

                let read_fd_flags = libc::fcntl(read_fd.as_raw_fd(), libc::F_GETFD);
                let write_fd_flags = libc::fcntl(write_fd.as_raw_fd(), libc::F_GETFD);
                assert_ne!(read_fd_flags & libc::FD_CLOEXEC, 0);
                assert_ne!(write_fd_flags & libc::FD_CLOEXEC, 0);

                let read_fl_flags = libc::fcntl(read_fd.as_raw_fd(), libc::F_GETFL);
                let write_fl_flags = libc::fcntl(write_fd.as_raw_fd(), libc::F_GETFL);
                assert_eq!(read_fl_flags & libc::O_NONBLOCK, 0);
                assert_eq!(write_fl_flags & libc::O_NONBLOCK, 0);
            }
        }

        assert_eq!(
            prepare_linux_metadata_stdout_pipe(0).unwrap_err(),
            OpenCodeCollectorError::SpawnFailed
        );
        assert_eq!(
            prepare_linux_metadata_stdout_pipe(usize::try_from(libc::c_int::MAX).unwrap())
                .unwrap_err(),
            OpenCodeCollectorError::SpawnFailed
        );
        assert_eq!(
            prepare_linux_metadata_stdout_pipe((libc::c_int::MAX as usize).saturating_add(1))
                .unwrap_err(),
            OpenCodeCollectorError::SpawnFailed
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pre_read_sync_without_child_signal_is_bounded_and_reaps_child() {
        for child_action in ["exit 97", "exec sleep 60"] {
            let script = format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > \"$(dirname \"$0\")/child.pid\"\n{child_action}\n"
            );
            let fixture = TestFixture::new(&script);
            let socket = tokio::net::UnixDatagram::bind(fixture.root_dir.join("ready.sock"))
                .expect("bind private sync socket");
            let context = fixture.context(Duration::from_millis(300), 1_000);
            let (_cancel_tx, mut cancel_rx) = watch::channel(false);
            let started = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_secs(3),
                run_metadata_command_with_pipe_config(
                    &context,
                    &[],
                    MAX_OPENCODE_EXPORT_BYTES,
                    MAX_OPENCODE_EXPORT_BYTES,
                    Some(&socket),
                    &mut cancel_rx,
                ),
            )
            .await
            .expect("missing handshake must not hang collection or runtime shutdown");
            assert_eq!(result, Err(OpenCodeCollectorError::SpawnFailed));
            assert!(started.elapsed() < Duration::from_secs(3));
            let pid: u32 = fs::read_to_string(fixture.root_dir.join("child.pid"))
                .expect("child started before handshake timeout")
                .parse()
                .expect("numeric synthetic child pid");
            assert!(!is_process_alive(pid), "synthetic child must be reaped");
        }
    }

    #[cfg(target_os = "linux")]
    fn build_padded_sanitized_export_json(
        workdir_str: &str,
        session_id: &str,
        provider_id: &str,
        model_id: &str,
        variant: &str,
        sentinel: &str,
        target_len: usize,
    ) -> String {
        let prefix = format!(
            r#"{{"info":{{"id":"{session_id}","outcome":"succeeded","location":{{"directory":"{workdir_str}"}},"time":{{"created":1050,"updated":1200,"idle":1200}}}},"messages":[{{"id":"msg_user_1","type":"user","sessionID":"{session_id}","parts":[{{"type":"text","text":"{sentinel}:"#
        );
        let suffix = format!(
            r#""}}]}},{{"id":"msg_asst_1","type":"assistant","sessionID":"{session_id}","finish":"stop","time":{{"created":1060,"completed":1150}},"model":{{"providerID":"{provider_id}","id":"{model_id}","variant":"{variant}"}},"parts":[{{"type":"text","text":"sanitized assistant output"}}]}} ]}}"#
        );
        assert!(
            target_len > prefix.len() + suffix.len(),
            "target_len must exceed prefix + suffix length"
        );
        let pad_len = target_len - prefix.len() - suffix.len();
        let payload = format!("{prefix}{}{suffix}", "x".repeat(pad_len));
        assert_eq!(payload.len(), target_len);
        payload
    }

    #[cfg(target_os = "linux")]
    fn parse_abrupt_child_result(raw: &str) -> (isize, i32, usize) {
        let mut write_rc: Option<isize> = None;
        let mut pipe_sz: Option<i32> = None;
        let mut pipe_fds: Option<usize> = None;
        for token in raw.split_whitespace() {
            if let Some(val) = token.strip_prefix("write_rc=") {
                write_rc = val.parse().ok();
            } else if let Some(val) = token.strip_prefix("pipe_sz=") {
                pipe_sz = val.parse().ok();
            } else if let Some(val) = token.strip_prefix("pipe_fds=") {
                pipe_fds = val.parse().ok();
            }
        }
        (
            write_rc.expect("write_rc present"),
            pipe_sz.expect("pipe_sz present"),
            pipe_fds.expect("pipe_fds present"),
        )
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn large_sanitized_export_from_abrupt_nonblocking_producer_succeeds_with_presized_pipe_and_postponed_reader(
    ) {
        let test_bin = std::env::current_exe().expect("current test binary path");
        let test_bin_str = test_bin.to_str().expect("utf-8 test binary path");
        let sentinel = "SENTINEL_LARGE_EXPORT_PADDING_SECRET_777";
        let target_export_len: usize = 131_097;

        let script = format!(
            r#"#!/bin/sh
set -eu
ROOT_DIR="$(dirname "$0")"
LOG_FILE="$ROOT_DIR/invocations.log"
printf '%s\n' "$*" >> "$LOG_FILE"

if [ "${{1:-}}" = "api" ] && [ "${{2:-}}" = "--standalone" ] && [ "${{3:-}}" = "session.get" ]; then
    cat <<EOF
{{"data":{{"id":"ses_owned_123","outcome":"succeeded","location":{{"directory":"$(pwd)"}},"time":{{"created":1050,"updated":1200,"idle":1200}}}}}}
EOF
    exit 0
fi

if [ "${{1:-}}" = "session" ] && [ "${{2:-}}" = "export" ] && [ "${{3:-}}" = "--standalone" ] && [ "${{4:-}}" = "--sanitize" ]; then
    MODE="$(cat "$ROOT_DIR/mode.txt")"
    if [ "$MODE" = "overflow" ]; then
        dd if=/dev/zero bs=1024 count=1025 2>/dev/null | tr '\0' 'a'
        exit 0
    fi
    exec 3>&1 1>/dev/null 2>/dev/null
    export THREADLANE_TEST_ABRUPT_PAYLOAD_PATH="$ROOT_DIR/export_payload.json"
    export THREADLANE_TEST_ABRUPT_RESULT_PATH="$ROOT_DIR/abrupt_result.txt"
    if [ -S "$ROOT_DIR/ready.sock" ]; then
        export THREADLANE_TEST_ABRUPT_READY_SOCKET="$ROOT_DIR/ready.sock"
    fi
    exec "{test_bin_str}" abrupt_nonblocking_export_child_entrypoint --nocapture
fi

exit 99
"#
        );

        let fixture = TestFixture::new(&script);
        let workdir_str = fixture.workdir.to_str().expect("utf-8 workdir");
        let mode_path = fixture.root_dir.join("mode.txt");
        let payload_path = fixture.root_dir.join("export_payload.json");
        let result_path = fixture.root_dir.join("abrupt_result.txt");
        let socket_path = fixture.root_dir.join("ready.sock");

        let valid_export = build_padded_sanitized_export_json(
            workdir_str,
            "ses_owned_123",
            "opencode-go",
            "deepseek-v4.1-flash",
            "max",
            sentinel,
            target_export_len,
        );
        assert_eq!(valid_export.len(), 131_097);
        fs::write(&payload_path, valid_export.as_bytes()).expect("write valid_export");
        fs::write(&mode_path, "abrupt").expect("write abrupt mode");

        let sync_socket = tokio::net::UnixDatagram::bind(&socket_path)
            .expect("bind private pre-read sync socket");

        let target = expected_model_target();
        let owned = OwnedOpenCodeSession {
            session_id: "ses_owned_123".to_string(),
            canonical_workdir: fixture.workdir.clone(),
            started_at_ms: 1_000,
        };

        // 1. With pipe pre-sized to 65_536 before spawn and parent reading postponed until after
        // the child completes its single nonblocking write and exits 0, only 65_536 bytes fit in
        // the pipe buffer and the truncated JSON is rejected at `export_json`.
        let _ = fs::remove_file(&result_path);
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let truncated_diag = collect_with_export_transport_test_hook(
            &ctx,
            &owned,
            &target,
            65_536,
            Some(&sync_socket),
            &mut cancel_rx,
        )
        .await
        .expect_err("64 KiB pipe must truncate 131097-byte write when reader waits");
        assert_eq!(
            truncated_diag.error,
            OpenCodeCollectorError::InvalidMetadata
        );
        assert_eq!(
            truncated_diag.stage,
            Some(OpenCodeMetadataStage::ExportJson)
        );
        assert_eq!(truncated_diag.as_str(), "invalid_metadata:export_json");
        assert!(!truncated_diag.to_string().contains(sentinel));

        let (write_rc_64k, pipe_sz_64k, pipe_fds_64k) = parse_abrupt_child_result(
            &fs::read_to_string(&result_path).expect("read 64k abrupt result"),
        );
        assert_eq!(write_rc_64k, 65_536);
        assert_eq!(pipe_sz_64k, 65_536);
        assert_eq!(pipe_fds_64k, 1, "child must not inherit parent read fd");

        // 2. With pipe pre-sized to MAX_OPENCODE_EXPORT_BYTES (1_048_576) BEFORE spawn and the
        // exact same postponed reader handshake, the child's single nonblocking write buffers all
        // 131_097 bytes and exits 0 before the parent reads a single byte, and collection succeeds.
        let _ = fs::remove_file(&result_path);
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let evidence = collect_with_export_transport_test_hook(
            &ctx,
            &owned,
            &target,
            MAX_OPENCODE_EXPORT_BYTES,
            Some(&sync_socket),
            &mut cancel_rx,
        )
        .await
        .expect("1 MiB pre-sized pipe must capture full 131097-byte nonblocking export");
        assert_eq!(evidence.source, "opencode_sanitized_session_export");
        assert_eq!(evidence.strength, "cli_reported_per_assistant");
        assert_eq!(evidence.session_id, "ses_owned_123");
        assert_eq!(evidence.observed_model, "opencode-go/deepseek-v4.1-flash");
        assert_eq!(evidence.observed_effort.as_deref(), Some("max"));
        assert_eq!(evidence.assistant_message_count, 1);
        assert_eq!(evidence.terminal_message_id, "msg_asst_1");

        let (write_rc_1m, pipe_sz_1m, pipe_fds_1m) = parse_abrupt_child_result(
            &fs::read_to_string(&result_path).expect("read 1M abrupt result"),
        );
        assert_eq!(
            usize::try_from(write_rc_1m).unwrap(),
            target_export_len,
            "single nonblocking write must transfer all 131097 bytes into pre-sized pipe"
        );
        assert!(
            usize::try_from(pipe_sz_1m).unwrap() >= MAX_OPENCODE_EXPORT_BYTES,
            "child stdout pipe capacity at startup must be >= 1 MiB"
        );
        assert_eq!(pipe_fds_1m, 1, "child must not inherit parent read fd");

        // 3. Production `collect_with_diagnostic` and `collect` succeed directly against the
        // abrupt nonblocking producer and never retain padding or sentinel text.
        drop(sync_socket);
        fs::remove_file(&socket_path).expect("remove sync socket for normal production collect");
        let _ = fs::remove_file(&result_path);
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let prod_evidence = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect("production collect_with_diagnostic should succeed on 131097-byte export");
        assert_eq!(prod_evidence, evidence);
        let serialized = serde_json::to_string(&prod_evidence).expect("serialize evidence");
        assert!(!serialized.contains(sentinel));
        assert!(!serialized.contains("xxxx"));

        let (write_rc_prod, pipe_sz_prod, pipe_fds_prod) = parse_abrupt_child_result(
            &fs::read_to_string(&result_path).expect("read prod abrupt result"),
        );
        assert_eq!(usize::try_from(write_rc_prod).unwrap(), target_export_len);
        assert!(usize::try_from(pipe_sz_prod).unwrap() >= MAX_OPENCODE_EXPORT_BYTES);
        assert_eq!(pipe_fds_prod, 1);

        // 4. Truncated >64KiB payload (90_000 bytes) is still rejected at `export_json`.
        fs::write(&payload_path, &valid_export.as_bytes()[..90_000])
            .expect("write truncated 90k export");
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("truncated 90k export must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::InvalidMetadata);
        assert_eq!(diag.stage, Some(OpenCodeMetadataStage::ExportJson));
        assert!(!diag.to_string().contains(sentinel));

        // 5. >64KiB valid JSON exports with wrong session ID, wrong model, or wrong effort are
        // rejected with their exact semantic errors.
        let wrong_id_export = build_padded_sanitized_export_json(
            workdir_str,
            "ses_other_999",
            "opencode-go",
            "deepseek-v4.1-flash",
            "max",
            sentinel,
            target_export_len,
        );
        fs::write(&payload_path, wrong_id_export.as_bytes()).expect("write wrong_id_export");
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("wrong session id in >64KiB export must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::OwnershipMismatch);

        let wrong_model_export = build_padded_sanitized_export_json(
            workdir_str,
            "ses_owned_123",
            "opencode-go",
            "wrong-model",
            "max",
            sentinel,
            target_export_len,
        );
        fs::write(&payload_path, wrong_model_export.as_bytes()).expect("write wrong_model_export");
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("wrong model in >64KiB export must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::ModelMismatch);

        let wrong_effort_export = build_padded_sanitized_export_json(
            workdir_str,
            "ses_owned_123",
            "opencode-go",
            "deepseek-v4.1-flash",
            "low",
            sentinel,
            target_export_len,
        );
        fs::write(&payload_path, wrong_effort_export.as_bytes())
            .expect("write wrong_effort_export");
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err("wrong effort in >64KiB export must fail");
        assert_eq!(diag.error, OpenCodeCollectorError::EffortMismatch);

        // 6. Over-budget export (> 1 MiB) is rejected with `OutputOverflow`.
        fs::write(&mode_path, "overflow").expect("write overflow mode");
        let ctx = fixture.context(Duration::from_secs(5), 1_000);
        let (_cancel_tx, mut cancel_rx) = watch::channel(false);
        let diag = collect_with_diagnostic(&ctx, &owned, &target, &mut cancel_rx)
            .await
            .expect_err(">1 MiB export must fail with OutputOverflow");
        assert_eq!(diag.error, OpenCodeCollectorError::OutputOverflow);
        assert_eq!(diag.as_str(), "output_overflow");
    }
}
