//! Read-only, bounded native chat identity matcher for candidate `thread.id` discovery.
//!
//! ## Security Boundary (Candidate Detection vs. Proof)
//!
//! A locally running paired-MCP stdio process (`threadlane-native-mcp`) knows its own
//! process-generated [`ConnectionIdentity`] (`connection_nonce`, `epoch_id`, `server_pid`) and
//! startup `cwd`, but its environment does not provide a trustworthy Codex `thread.id`.
//!
//! Because `thread/list` filtered by `cwd` is not a complete ownership census (a reused MCP
//! connection could appear in another project and history filtering can omit active threads),
//! this matcher enumerates all loaded threads across **all** projects via bounded, complete,
//! de-duplicated pages of `thread/loaded/list`, inspects `thread/read` (`includeTurns: false`)
//! and `mcpServerStatus/list` (`threadlane`), and invokes `mcpServer/tool/call`
//! (`connection_identity`) on every loaded candidate with a running `threadlane` MCP server.
//!
//! - **Candidate only (`native_per_call_identity_verified: false`)**: Even when constrained to the
//!   2025-11-25 session-handshake MCP protocol, matching `ConnectionIdentity` to one loaded thread
//!   is a version-scoped detection measure, not universal proof of per-call native caller identity.
//! - **Inaccessible subagents fail closed**: Parent-owned multi-agent v2 subagents can appear in
//!   `thread/loaded/list` and return metadata from `thread/read` and `mcpServerStatus/list`, but
//!   reject `mcpServer/tool/call(connection_identity)` with JSON-RPC `-32600`
//!   (`direct app-server input is not allowed for multi-agent v2 sub-agents`). Without a separate
//!   trusted host guarantee, an inaccessible loaded candidate means exclusive ownership is not
//!   verified; the matcher fails closed with [`IdentityMatchErrorKind::UnprobeableCandidate`].
//! - **Post-discovery recheck**: After scanning all loaded candidates to completion, the matcher
//!   verifies the unique match's canonical `cwd`, re-reads `thread/read` and `connection_identity`,
//!   and re-reads the complete `thread/loaded/list` census to catch concurrent additions/removals.
//!
//! The optional [`verify_thread_hint_candidate`] path treats a request's `threadId` only as a
//! hint and independently checks the thread-scoped connection. It cannot prove connection
//! exclusivity, per-call caller identity, or grant enrollment by itself.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::{
    NativeClient, NativeLoadedThreadPage, NativeMcpServerStatus, NativeMcpToolCallResult,
    NativeThread,
};
use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    constant_time_str_eq, ensure_platform_supported, sanitize_diagnostic_text,
    validate_bounded_identifier, validate_optional_cwd_path,
};
use crate::pairing::{
    ConnectionEpochState, ConnectionIdentity, NativeVerificationStatus, CONNECTION_IDENTITY_TOOL,
    MCP_SERVER_NAME,
};

/// Default maximum `thread/loaded/list` pages scanned per census pass (`16`).
pub const DEFAULT_MAX_MATCH_PAGES: usize = 16;
/// Hard upper bound on `max_pages` (`64`).
pub const MAX_MATCH_PAGES_LIMIT: usize = 64;
/// Default maximum loaded thread IDs inspected per census pass (`128`).
pub const DEFAULT_MAX_MATCH_CANDIDATES: usize = 128;
/// Hard upper bound on `max_candidates` (`512`).
pub const MAX_MATCH_CANDIDATES_LIMIT: usize = 512;
/// Default maximum running MCP servers probed via `mcpServer/tool/call` (`64`).
pub const DEFAULT_MAX_PROBED_CANDIDATES: usize = 64;
/// Hard upper bound on `max_probed_candidates` (`256`).
pub const MAX_PROBED_CANDIDATES_LIMIT: usize = 256;
/// Default overall wall-clock timeout for a full identity matching pass (`15s`).
pub const DEFAULT_MATCH_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard upper bound on `timeout` (`60s`).
pub const MAX_MATCH_TIMEOUT: Duration = Duration::from_secs(60);

const MAX_CURSOR_LEN: usize = 256;
const MAX_IDENTIFIER_LEN: usize = 128;
const EXPECTED_PREFIXED_TOKEN_HEX_LEN: usize = 64;

/// Bounded scan and timeout budgets for [`NativeIdentityMatcher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityMatchBudget {
    /// Maximum `thread/loaded/list` pages allowed per census pass.
    pub max_pages: usize,
    /// Maximum loaded thread entries allowed per census pass.
    pub max_candidates: usize,
    /// Maximum running candidate threads on which `connection_identity` may be called.
    pub max_probed_candidates: usize,
    /// Maximum wall-clock duration for the entire discovery and re-verification pass.
    pub timeout: Duration,
}

impl Default for IdentityMatchBudget {
    fn default() -> Self {
        Self {
            max_pages: DEFAULT_MAX_MATCH_PAGES,
            max_candidates: DEFAULT_MAX_MATCH_CANDIDATES,
            max_probed_candidates: DEFAULT_MAX_PROBED_CANDIDATES,
            timeout: DEFAULT_MATCH_TIMEOUT,
        }
    }
}

impl IdentityMatchBudget {
    /// Validates that all budget limits are non-zero and within hard safety caps.
    pub fn validate(&self) -> Result<(), IdentityMatchError> {
        if self.max_pages == 0 || self.max_pages > MAX_MATCH_PAGES_LIMIT {
            return Err(err(
                IdentityMatchErrorKind::InvalidConfig,
                format!("matcher max_pages must be in 1..={MAX_MATCH_PAGES_LIMIT}"),
            ));
        }
        if self.max_candidates == 0 || self.max_candidates > MAX_MATCH_CANDIDATES_LIMIT {
            return Err(err(
                IdentityMatchErrorKind::InvalidConfig,
                format!("matcher max_candidates must be in 1..={MAX_MATCH_CANDIDATES_LIMIT}"),
            ));
        }
        if self.max_probed_candidates == 0
            || self.max_probed_candidates > MAX_PROBED_CANDIDATES_LIMIT
            || self.max_probed_candidates > self.max_candidates
        {
            return Err(err(
                IdentityMatchErrorKind::InvalidConfig,
                format!(
                    "matcher max_probed_candidates must be in 1..={MAX_PROBED_CANDIDATES_LIMIT} and <= max_candidates"
                ),
            ));
        }
        if self.timeout.is_zero() || self.timeout > MAX_MATCH_TIMEOUT {
            return Err(err(
                IdentityMatchErrorKind::InvalidConfig,
                format!(
                    "matcher timeout must be between 1ms and {}s",
                    MAX_MATCH_TIMEOUT.as_secs()
                ),
            ));
        }
        Ok(())
    }
}

/// Specific fail-closed classification for native chat identity matching failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityMatchErrorKind {
    /// Matcher budget or caller configuration is invalid.
    InvalidConfig,
    /// Project `cwd` failed symlink, traversal, or canonical directory validation.
    SecurityViolation,
    /// Expected or returned `ConnectionIdentity` payload failed format or state validation.
    InvalidIdentity,
    /// `thread/loaded/list` returned no loaded native threads.
    EmptyProject,
    /// Every reachable loaded thread was checked and none matched the expected identity.
    NoMatchingThread,
    /// A candidate partially matched nonce, epoch, or PID with conflicting identity fields.
    IdentityMismatch,
    /// A candidate matched `connection_nonce` with a stale/different `epoch_id`, or changed on re-read.
    StaleResult,
    /// The set of loaded threads changed during the scan or a loaded ID reported `not_loaded`.
    ChangingCensus,
    /// Multiple loaded threads (in the same or different projects) shared a nonce, epoch, or PID.
    AmbiguousSharedConnection,
    /// Pagination or candidate probing hit a budget cap, cursor cycle, or empty continuation page.
    IncompletePagination,
    /// A loaded candidate lacks a valid `threadlane` MCP server entry or `connection_identity` tool.
    AbsentOrInvalidMcpServer,
    /// A thread or its `threadlane` MCP server reported an unsupported or ambiguous status.
    UnsupportedStatus,
    /// A loaded candidate rejected `mcpServer/tool/call(connection_identity)` (e.g. `-32600` v2 subagent).
    UnprobeableCandidate,
    /// Returned thread ID or canonical `cwd` was invalid, changed on re-read, or differed from `project_cwd`.
    ThreadMetadataMismatch,
    /// Discovery or a client request exceeded the timeout budget.
    TimedOut,
    /// Underlying native client transport, protocol, or RPC call failed.
    ProviderError,
}

impl IdentityMatchErrorKind {
    /// Returns the static `snake_case` identifier for this failure kind.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::SecurityViolation => "security_violation",
            Self::InvalidIdentity => "invalid_identity",
            Self::EmptyProject => "empty_project",
            Self::NoMatchingThread => "no_matching_thread",
            Self::IdentityMismatch => "identity_mismatch",
            Self::StaleResult => "stale_result",
            Self::ChangingCensus => "changing_census",
            Self::AmbiguousSharedConnection => "ambiguous_shared_connection",
            Self::IncompletePagination => "incomplete_pagination",
            Self::AbsentOrInvalidMcpServer => "absent_or_invalid_mcp_server",
            Self::UnsupportedStatus => "unsupported_status",
            Self::UnprobeableCandidate => "unprobeable_candidate",
            Self::ThreadMetadataMismatch => "thread_metadata_mismatch",
            Self::TimedOut => "timed_out",
            Self::ProviderError => "provider_error",
        }
    }

    /// Maps this matcher error kind to the corresponding [`NativeErrorCode`].
    pub const fn native_error_code(self) -> NativeErrorCode {
        match self {
            Self::InvalidConfig => NativeErrorCode::InvalidConfig,
            Self::SecurityViolation => NativeErrorCode::SecurityViolation,
            Self::InvalidIdentity | Self::IdentityMismatch => NativeErrorCode::ValidationFailed,
            Self::EmptyProject | Self::NoMatchingThread => NativeErrorCode::ThreadNotFound,
            Self::StaleResult | Self::ChangingCensus => NativeErrorCode::ExpiredEpoch,
            Self::AmbiguousSharedConnection => NativeErrorCode::SharedConnectionRejected,
            Self::IncompletePagination => NativeErrorCode::OutputOverflow,
            Self::AbsentOrInvalidMcpServer => NativeErrorCode::NotConfigured,
            Self::UnsupportedStatus | Self::UnprobeableCandidate => {
                NativeErrorCode::UnsupportedCapability
            }
            Self::ThreadMetadataMismatch => NativeErrorCode::ProjectMismatch,
            Self::TimedOut => NativeErrorCode::TimedOut,
            Self::ProviderError => NativeErrorCode::TransportUnavailable,
        }
    }
}

/// Redacted, structured error returned by [`NativeIdentityMatcher`].
///
/// Never embeds nonces, epochs, operator tokens, chat titles, `sessionId`, or filesystem paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct IdentityMatchError {
    /// Specific matcher failure classification.
    pub kind: IdentityMatchErrorKind,
    /// Corresponding crate-level [`NativeErrorCode`].
    pub code: NativeErrorCode,
    /// Redacted human-readable diagnostic message.
    pub message: String,
    /// Optional JSON-RPC numeric error code when reported by the app-server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_code: Option<i64>,
}

impl IdentityMatchError {
    /// Constructs a new [`IdentityMatchError`] with a sanitized diagnostic message.
    fn new(kind: IdentityMatchErrorKind, message: impl Into<String>) -> Self {
        Self::with_rpc_code(kind, None, message)
    }

    /// Constructs a new [`IdentityMatchError`] preserving an optional JSON-RPC error code.
    fn with_rpc_code(
        kind: IdentityMatchErrorKind,
        rpc_code: Option<i64>,
        message: impl Into<String>,
    ) -> Self {
        let raw = message.into();
        Self {
            kind,
            code: kind.native_error_code(),
            message: sanitize_diagnostic_text(&raw),
            rpc_code,
        }
    }

    /// Returns the [`IdentityMatchErrorKind`].
    pub const fn kind(&self) -> IdentityMatchErrorKind {
        self.kind
    }

    /// Returns the static `snake_case` string for the matcher error kind.
    pub const fn kind_str(&self) -> &'static str {
        self.kind.as_str()
    }

    /// Returns the [`NativeErrorCode`].
    pub const fn error_code(&self) -> NativeErrorCode {
        self.code
    }

    /// Returns the static `snake_case` string for the [`NativeErrorCode`].
    pub const fn code(&self) -> &'static str {
        self.code.as_str()
    }

    /// Returns the sanitized diagnostic message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Converts this matcher error into a [`NativeError`].
    pub fn into_native_error(self) -> NativeError {
        NativeError {
            code: self.code,
            message: self.message,
            rpc_code: self.rpc_code,
        }
    }
}

impl From<IdentityMatchError> for NativeError {
    fn from(err: IdentityMatchError) -> Self {
        err.into_native_error()
    }
}

fn err(kind: IdentityMatchErrorKind, message: impl Into<String>) -> IdentityMatchError {
    IdentityMatchError::new(kind, message)
}

/// Verified candidate thread discovered by [`NativeIdentityMatcher`].
///
/// Represents a read-only candidate match across the current loaded-thread census, **not** proof
/// of per-call native caller identity (`native_per_call_identity_verified` is always `false`).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedThreadCandidate {
    /// Canonical UUID of the uniquely matched native thread (`thread.id`).
    pub thread_id: String,
    /// Canonical project working directory verified on both initial and post-discovery reads.
    pub cwd: PathBuf,
    /// Optional native `projectId` metadata from `thread/read`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Thread status observed on post-discovery re-read (`idle` or `active`).
    pub thread_status: String,
    /// Verified non-zero OS process ID (`server_pid`) of the paired MCP server.
    pub server_pid: u32,
    /// Verified connection epoch state (`unbound` or `enrolled`).
    pub epoch_state: ConnectionEpochState,
    /// Enrolled binding ID when `epoch_state` is [`ConnectionEpochState::Enrolled`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrolled_binding_id: Option<String>,
    /// Honest verification gate (always [`NativeVerificationStatus::NeedsVerification`]).
    pub verification_gate: NativeVerificationStatus,
    /// Always `false`: candidate discovery does not prove native per-call caller identity.
    pub native_per_call_identity_verified: bool,
    /// Number of `thread/loaded/list` pages scanned in the loaded-thread census.
    pub pages_scanned: usize,
    /// Number of unique loaded threads examined across all projects.
    pub candidates_examined: usize,
    /// Number of loaded threads probed via `mcpServer/tool/call` (`connection_identity`).
    pub candidates_probed: usize,
    /// Number of loaded threads skipped because their `threadlane` MCP server was not running.
    pub candidates_skipped_mcp_not_running: usize,
}

impl fmt::Debug for MatchedThreadCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MatchedThreadCandidate")
            .field("thread_id", &self.thread_id)
            .field("cwd", &"<redacted-canonical-cwd>")
            .field("project_id", &self.project_id)
            .field("thread_status", &self.thread_status)
            .field("server_pid", &self.server_pid)
            .field("epoch_state", &self.epoch_state)
            .field("enrolled_binding_id", &self.enrolled_binding_id)
            .field("verification_gate", &self.verification_gate)
            .field(
                "native_per_call_identity_verified",
                &self.native_per_call_identity_verified,
            )
            .field("pages_scanned", &self.pages_scanned)
            .field("candidates_examined", &self.candidates_examined)
            .field("candidates_probed", &self.candidates_probed)
            .field(
                "candidates_skipped_mcp_not_running",
                &self.candidates_skipped_mcp_not_running,
            )
            .finish()
    }
}

/// Narrow, read-only interface required by [`NativeIdentityMatcher`].
///
/// Only exposes `thread/loaded/list`, `thread/read` (`includeTurns: false`),
/// `mcpServerStatus/list`, and `mcpServer/tool/call` (`connection_identity`).
#[async_trait]
pub trait NativeIdentityProbeClient: Send + Sync {
    /// Lists a single page of currently loaded thread IDs across all projects (`thread/loaded/list`).
    async fn list_loaded_threads(
        &self,
        cursor: Option<String>,
    ) -> Result<NativeLoadedThreadPage, NativeError>;

    /// Reads metadata for a single thread (`thread/read` with `includeTurns: false`).
    async fn read_thread(&self, thread_id: &str) -> Result<NativeThread, NativeError>;

    /// Lists MCP server status entries for `thread_id` (`mcpServerStatus/list`).
    async fn list_mcp_server_status(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<Vec<NativeMcpServerStatus>, NativeError>;

    /// Invokes `connection_identity` on `server_name` in `thread_id` (`mcpServer/tool/call`).
    async fn call_connection_identity(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<NativeMcpToolCallResult, NativeError>;
}

#[async_trait]
impl NativeIdentityProbeClient for NativeClient {
    async fn list_loaded_threads(
        &self,
        cursor: Option<String>,
    ) -> Result<NativeLoadedThreadPage, NativeError> {
        self.loaded_thread_ids(cursor).await
    }

    async fn read_thread(&self, thread_id: &str) -> Result<NativeThread, NativeError> {
        self.thread(thread_id).await
    }

    async fn list_mcp_server_status(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<Vec<NativeMcpServerStatus>, NativeError> {
        self.mcp_server_status(thread_id, server_name).await
    }

    async fn call_connection_identity(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<NativeMcpToolCallResult, NativeError> {
        self.call_mcp_tool(thread_id, server_name, CONNECTION_IDENTITY_TOOL, json!({}))
            .await
    }
}

#[async_trait]
impl<T: NativeIdentityProbeClient + ?Sized> NativeIdentityProbeClient for &T {
    async fn list_loaded_threads(
        &self,
        cursor: Option<String>,
    ) -> Result<NativeLoadedThreadPage, NativeError> {
        (**self).list_loaded_threads(cursor).await
    }

    async fn read_thread(&self, thread_id: &str) -> Result<NativeThread, NativeError> {
        (**self).read_thread(thread_id).await
    }

    async fn list_mcp_server_status(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<Vec<NativeMcpServerStatus>, NativeError> {
        (**self)
            .list_mcp_server_status(thread_id, server_name)
            .await
    }

    async fn call_connection_identity(
        &self,
        thread_id: &str,
        server_name: &str,
    ) -> Result<NativeMcpToolCallResult, NativeError> {
        (**self)
            .call_connection_identity(thread_id, server_name)
            .await
    }
}

/// Reusable, read-only native chat identity matcher with bounded scan and probe budgets.
#[derive(Debug, Clone, Default)]
pub struct NativeIdentityMatcher {
    budget: IdentityMatchBudget,
}

impl NativeIdentityMatcher {
    /// Constructs a new [`NativeIdentityMatcher`] with default budgets.
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a [`NativeIdentityMatcher`] with an explicit validated [`IdentityMatchBudget`].
    pub fn with_budget(budget: IdentityMatchBudget) -> Result<Self, IdentityMatchError> {
        budget.validate()?;
        Ok(Self { budget })
    }

    /// Returns the configured [`IdentityMatchBudget`].
    pub const fn budget(&self) -> &IdentityMatchBudget {
        &self.budget
    }

    /// Discovers which loaded `thread.id`, if any, uniquely owns `expected_identity` and matches
    /// the validated canonical `project_cwd`.
    pub async fn match_candidate<C: NativeIdentityProbeClient + ?Sized>(
        &self,
        client: &C,
        expected_identity: &ConnectionIdentity,
        project_cwd: &Path,
    ) -> Result<MatchedThreadCandidate, IdentityMatchError> {
        match_native_thread_identity_with_budget(
            client,
            expected_identity,
            project_cwd,
            &self.budget,
        )
        .await
    }
}

/// Discovers a candidate `thread.id` for `expected_identity` in `project_cwd` using default budgets.
pub async fn match_native_thread_identity<C: NativeIdentityProbeClient + ?Sized>(
    client: &C,
    expected_identity: &ConnectionIdentity,
    project_cwd: &Path,
) -> Result<MatchedThreadCandidate, IdentityMatchError> {
    let budget = IdentityMatchBudget::default();
    match_native_thread_identity_with_budget(client, expected_identity, project_cwd, &budget).await
}

/// Checks an untrusted thread ID hint against a thread-scoped MCP connection twice.
///
/// This is a read-only candidate check: a match is **not** proof that the connection cannot
/// be shared with another thread, is not proof of the caller's identity, and does not authorize
/// enrollment. It does not scan unrelated chats, so inaccessible subagents do not prevent this
/// check. A hint targeting an inaccessible subagent itself fails closed.
pub async fn verify_thread_hint_candidate<C: NativeIdentityProbeClient + ?Sized>(
    client: &C,
    thread_id_hint: &str,
    expected_identity: &ConnectionIdentity,
    project_cwd: &Path,
) -> Result<MatchedThreadCandidate, IdentityMatchError> {
    validate_canonical_thread_uuid(thread_id_hint, "threadId hint")?;
    validate_connection_identity_fields(expected_identity, true)?;
    let cwd = validate_canonical_project_cwd(project_cwd)?;
    let cwd_str = cwd.to_str().ok_or_else(|| {
        err(
            IdentityMatchErrorKind::InvalidConfig,
            "project working directory is not valid UTF-8",
        )
    })?;
    let started = Instant::now();
    let timeout = DEFAULT_MATCH_TIMEOUT;
    let thread = run_with_deadline(started, timeout, client.read_thread(thread_id_hint))
        .await
        .map_err(|e| classify_probe_error(e, "thread/read"))?;
    validate_loaded_thread_metadata(&thread, thread_id_hint)?;
    if !ensure_thread_is_loaded(&thread.status)? || thread.cwd != cwd_str {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "hinted thread is not loaded in the expected project",
        ));
    }
    let statuses = run_with_deadline(
        started,
        timeout,
        client.list_mcp_server_status(thread_id_hint, MCP_SERVER_NAME),
    )
    .await
    .map_err(|e| classify_probe_error(e, "mcpServerStatus/list"))?;
    if !evaluate_threadlane_mcp_ready(&statuses)? {
        return Err(err(
            IdentityMatchErrorKind::AbsentOrInvalidMcpServer,
            "hinted thread's Threadlane MCP server is not running",
        ));
    }
    for _ in 0..2 {
        let result = run_with_deadline(
            started,
            timeout,
            client.call_connection_identity(thread_id_hint, MCP_SERVER_NAME),
        )
        .await
        .map_err(|e| classify_probe_error(e, "mcpServer/tool/call(connection_identity)"))?;
        let actual = parse_and_validate_connection_identity(&result)?;
        if !constant_time_str_eq(
            &actual.connection_nonce,
            &expected_identity.connection_nonce,
        ) || !constant_time_str_eq(&actual.epoch_id, &expected_identity.epoch_id)
            || actual.server_pid != expected_identity.server_pid
            || actual.state != expected_identity.state
            || actual.enrolled_binding_id != expected_identity.enrolled_binding_id
        {
            return Err(err(
                IdentityMatchErrorKind::IdentityMismatch,
                "hinted thread's MCP connection does not match this connection",
            ));
        }
    }
    let reread = run_with_deadline(started, timeout, client.read_thread(thread_id_hint))
        .await
        .map_err(|e| classify_probe_error(e, "post-discovery thread/read"))?;
    validate_loaded_thread_metadata(&reread, thread_id_hint)?;
    if !ensure_thread_is_loaded(&reread.status)?
        || reread.cwd != cwd_str
        || reread.project_id != thread.project_id
    {
        return Err(err(
            IdentityMatchErrorKind::StaleResult,
            "hinted thread changed during verification",
        ));
    }
    Ok(MatchedThreadCandidate {
        thread_id: reread.id,
        cwd,
        project_id: reread.project_id,
        thread_status: reread.status.trim().to_string(),
        server_pid: expected_identity.server_pid,
        epoch_state: expected_identity.state,
        enrolled_binding_id: expected_identity.enrolled_binding_id.clone(),
        verification_gate: NativeVerificationStatus::NeedsVerification,
        native_per_call_identity_verified: false,
        pages_scanned: 0,
        candidates_examined: 1,
        candidates_probed: 1,
        candidates_skipped_mcp_not_running: 0,
    })
}

/// Encodes a [`ConnectionIdentity`] into the standard `rmcp` [`NativeMcpToolCallResult`] envelope.
pub fn encode_connection_identity_tool_result(
    identity: &ConnectionIdentity,
) -> NativeMcpToolCallResult {
    let envelope = json!({ "data": identity });
    NativeMcpToolCallResult {
        content: vec![json!({ "type": "text", "text": envelope.to_string() })],
        structured_content: Some(envelope),
        is_error: false,
    }
}

/// Validates that `project_cwd` is an existing, canonical, non-symlink absolute directory.
pub fn validate_canonical_project_cwd(project_cwd: &Path) -> Result<PathBuf, IdentityMatchError> {
    ensure_platform_supported()
        .map_err(|e| err(IdentityMatchErrorKind::InvalidConfig, e.message()))?;
    let clean = match validate_optional_cwd_path(Some(project_cwd)) {
        Ok(Some(path)) => path,
        Ok(None) => {
            return Err(err(
                IdentityMatchErrorKind::InvalidConfig,
                "project working directory must not be empty",
            ));
        }
        Err(e) => {
            let kind = if e.error_code() == NativeErrorCode::SecurityViolation {
                IdentityMatchErrorKind::SecurityViolation
            } else {
                IdentityMatchErrorKind::InvalidConfig
            };
            return Err(err(kind, e.message()));
        }
    };

    let meta = fs::symlink_metadata(&clean).map_err(|_| {
        err(
            IdentityMatchErrorKind::InvalidConfig,
            "project working directory does not exist or cannot be inspected",
        )
    })?;
    if meta.file_type().is_symlink() {
        return Err(err(
            IdentityMatchErrorKind::SecurityViolation,
            "project working directory must not be a symlink",
        ));
    }
    if !meta.file_type().is_dir() {
        return Err(err(
            IdentityMatchErrorKind::InvalidConfig,
            "project working directory is not a directory",
        ));
    }
    let canonical = fs::canonicalize(&clean).map_err(|_| {
        err(
            IdentityMatchErrorKind::InvalidConfig,
            "project working directory could not be canonicalized",
        )
    })?;
    if canonical != clean {
        return Err(err(
            IdentityMatchErrorKind::SecurityViolation,
            "project working directory does not match its canonical path",
        ));
    }
    Ok(clean)
}

/// Discovers which loaded `thread.id`, if any, uniquely owns `expected_identity` across all loaded
/// projects and verifies its canonical `cwd` matches `project_cwd`.
pub async fn match_native_thread_identity_with_budget<C: NativeIdentityProbeClient + ?Sized>(
    client: &C,
    expected_identity: &ConnectionIdentity,
    project_cwd: &Path,
    budget: &IdentityMatchBudget,
) -> Result<MatchedThreadCandidate, IdentityMatchError> {
    budget.validate()?;
    let canonical_cwd = validate_canonical_project_cwd(project_cwd)?;
    let canonical_cwd_str = canonical_cwd.to_str().ok_or_else(|| {
        err(
            IdentityMatchErrorKind::InvalidConfig,
            "canonical project working directory is not valid UTF-8",
        )
    })?;
    validate_connection_identity_fields(expected_identity, true)?;

    let start = Instant::now();

    // 1. Enumerate the complete, bounded, de-duplicated `thread/loaded/list` census across ALL projects.
    let (initial_set, pages_scanned) = collect_loaded_thread_census(client, budget, start).await?;
    if initial_set.is_empty() {
        return Err(err(
            IdentityMatchErrorKind::EmptyProject,
            "thread/loaded/list returned no loaded native threads",
        ));
    }

    let mut candidates_examined = 0usize;
    let mut candidates_probed = 0usize;
    let mut candidates_skipped_mcp_not_running = 0usize;

    let mut seen_nonces: BTreeMap<String, String> = BTreeMap::new();
    let mut seen_epochs: BTreeMap<String, String> = BTreeMap::new();
    let mut seen_pids: BTreeMap<u32, String> = BTreeMap::new();
    let mut duplicate_reason: Option<&'static str> = None;
    let mut stale_epoch_reason: Option<&'static str> = None;
    let mut partial_mismatch_reason: Option<&'static str> = None;
    let mut matched_candidates: Vec<(NativeThread, ConnectionIdentity)> = Vec::new();

    // 2. Inspect every loaded thread across all projects to completion.
    for thread_id in &initial_set {
        candidates_examined += 1;

        let thread = run_with_deadline(start, budget.timeout, client.read_thread(thread_id))
            .await
            .map_err(|e| classify_probe_error(e, "thread/read"))?;
        validate_loaded_thread_metadata(&thread, thread_id)?;

        if !ensure_thread_is_loaded(&thread.status)? {
            return Err(err(
                IdentityMatchErrorKind::ChangingCensus,
                "loaded thread census changed during scan: candidate reported not_loaded on thread/read",
            ));
        }

        let statuses = run_with_deadline(
            start,
            budget.timeout,
            client.list_mcp_server_status(&thread.id, MCP_SERVER_NAME),
        )
        .await
        .map_err(|e| classify_probe_error(e, "mcpServerStatus/list"))?;

        if !evaluate_threadlane_mcp_ready(&statuses)? {
            candidates_skipped_mcp_not_running += 1;
            continue;
        }

        if candidates_probed >= budget.max_probed_candidates {
            return Err(err(
                IdentityMatchErrorKind::IncompletePagination,
                "probed candidate count exceeded matcher max_probed_candidates budget",
            ));
        }
        candidates_probed += 1;

        let tool_result = run_with_deadline(
            start,
            budget.timeout,
            client.call_connection_identity(&thread.id, MCP_SERVER_NAME),
        )
        .await
        .map_err(|e| classify_probe_error(e, "mcpServer/tool/call(connection_identity)"))?;

        let candidate_identity = parse_and_validate_connection_identity(&tool_result)?;

        if let Some(prev) = seen_nonces.insert(
            candidate_identity.connection_nonce.clone(),
            thread.id.clone(),
        ) {
            if prev != thread.id && duplicate_reason.is_none() {
                duplicate_reason =
                    Some("two distinct loaded threads reported the same MCP connection_nonce");
            }
        }
        if let Some(prev) =
            seen_epochs.insert(candidate_identity.epoch_id.clone(), thread.id.clone())
        {
            if prev != thread.id && duplicate_reason.is_none() {
                duplicate_reason =
                    Some("two distinct loaded threads reported the same MCP epoch_id");
            }
        }
        if let Some(prev) = seen_pids.insert(candidate_identity.server_pid, thread.id.clone()) {
            if prev != thread.id && duplicate_reason.is_none() {
                duplicate_reason =
                    Some("two distinct loaded threads reported the same MCP server_pid");
            }
        }

        let nonce_ok = constant_time_str_eq(
            &candidate_identity.connection_nonce,
            &expected_identity.connection_nonce,
        );
        let epoch_ok =
            constant_time_str_eq(&candidate_identity.epoch_id, &expected_identity.epoch_id);
        let pid_ok = candidate_identity.server_pid == expected_identity.server_pid;

        if nonce_ok && epoch_ok && pid_ok {
            if matches!(
                candidate_identity.state,
                ConnectionEpochState::Expired | ConnectionEpochState::QuarantinedSharedConnection
            ) {
                partial_mismatch_reason
                    .get_or_insert("matching candidate connection is expired or quarantined");
                continue;
            }
            if candidate_identity.state != expected_identity.state
                || candidate_identity.enrolled_binding_id != expected_identity.enrolled_binding_id
            {
                partial_mismatch_reason.get_or_insert(
                    "matching candidate connection state or enrolled_binding_id differed from expected identity",
                );
                continue;
            }
            matched_candidates.push((thread, candidate_identity));
        } else if nonce_ok && !epoch_ok {
            stale_epoch_reason.get_or_insert(
                "candidate matched connection_nonce but reported a different epoch_id",
            );
        } else if nonce_ok || epoch_ok || pid_ok {
            partial_mismatch_reason.get_or_insert(
                "candidate partially matched connection_nonce, epoch_id, or server_pid with conflicting identity fields",
            );
        }
    }

    if let Some(reason) = duplicate_reason {
        return Err(err(
            IdentityMatchErrorKind::AmbiguousSharedConnection,
            reason,
        ));
    }
    if matched_candidates.len() > 1 {
        return Err(err(
            IdentityMatchErrorKind::AmbiguousSharedConnection,
            "multiple loaded threads matched the expected MCP ConnectionIdentity",
        ));
    }
    if let Some(reason) = stale_epoch_reason {
        return Err(err(IdentityMatchErrorKind::StaleResult, reason));
    }
    if let Some(reason) = partial_mismatch_reason {
        return Err(err(IdentityMatchErrorKind::IdentityMismatch, reason));
    }

    let Some((selected_thread, _)) = matched_candidates.into_iter().next() else {
        return Err(err(
            IdentityMatchErrorKind::NoMatchingThread,
            "no loaded native thread matched the expected MCP ConnectionIdentity",
        ));
    };

    // 3. Ensure the uniquely matched thread belongs to the caller's validated canonical project_cwd.
    if selected_thread.cwd != canonical_cwd_str {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "uniquely matched loaded thread cwd does not match the expected canonical project cwd",
        ));
    }

    // 4. Re-read selected thread metadata and connection_identity to catch mid-scan changes.
    let reread_thread = run_with_deadline(
        start,
        budget.timeout,
        client.read_thread(&selected_thread.id),
    )
    .await
    .map_err(|e| classify_probe_error(e, "post-discovery thread/read"))?;
    validate_loaded_thread_metadata(&reread_thread, &selected_thread.id)?;

    if reread_thread.cwd != canonical_cwd_str {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "selected thread cwd changed or mismatched during post-discovery thread/read",
        ));
    }
    if reread_thread.project_id != selected_thread.project_id {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "selected thread projectId changed during post-discovery thread/read",
        ));
    }
    if !ensure_thread_is_loaded(&reread_thread.status)? {
        return Err(err(
            IdentityMatchErrorKind::StaleResult,
            "selected thread became not_loaded during post-discovery verification",
        ));
    }

    let reread_tool = run_with_deadline(
        start,
        budget.timeout,
        client.call_connection_identity(&selected_thread.id, MCP_SERVER_NAME),
    )
    .await
    .map_err(|e| {
        classify_probe_error(e, "post-discovery mcpServer/tool/call(connection_identity)")
    })?;
    let reread_identity = parse_and_validate_connection_identity(&reread_tool)?;

    let reread_exact =
        constant_time_str_eq(
            &reread_identity.connection_nonce,
            &expected_identity.connection_nonce,
        ) && constant_time_str_eq(&reread_identity.epoch_id, &expected_identity.epoch_id)
            && reread_identity.server_pid == expected_identity.server_pid
            && reread_identity.state == expected_identity.state
            && reread_identity.enrolled_binding_id == expected_identity.enrolled_binding_id;
    if !reread_exact {
        return Err(err(
            IdentityMatchErrorKind::StaleResult,
            "selected thread ConnectionIdentity changed during post-discovery verification",
        ));
    }

    // 5. Re-read the complete `thread/loaded/list` census to catch concurrent additions or removals.
    let (recheck_set, _) = collect_loaded_thread_census(client, budget, start).await?;
    if recheck_set != initial_set {
        return Err(err(
            IdentityMatchErrorKind::ChangingCensus,
            "loaded thread census changed between initial discovery and post-discovery recheck",
        ));
    }

    Ok(MatchedThreadCandidate {
        thread_id: reread_thread.id,
        cwd: canonical_cwd,
        project_id: reread_thread.project_id,
        thread_status: reread_thread.status.trim().to_string(),
        server_pid: reread_identity.server_pid,
        epoch_state: reread_identity.state,
        enrolled_binding_id: reread_identity.enrolled_binding_id,
        verification_gate: NativeVerificationStatus::NeedsVerification,
        native_per_call_identity_verified: false,
        pages_scanned,
        candidates_examined,
        candidates_probed,
        candidates_skipped_mcp_not_running,
    })
}

async fn collect_loaded_thread_census<C: NativeIdentityProbeClient + ?Sized>(
    client: &C,
    budget: &IdentityMatchBudget,
    start: Instant,
) -> Result<(BTreeSet<String>, usize), IdentityMatchError> {
    let mut pages_scanned = 0usize;
    let mut raw_entries = 0usize;
    let mut cursor: Option<String> = None;
    let mut seen_cursors: BTreeSet<String> = BTreeSet::new();
    let mut loaded_ids: BTreeSet<String> = BTreeSet::new();

    loop {
        if pages_scanned >= budget.max_pages {
            return Err(err(
                IdentityMatchErrorKind::IncompletePagination,
                "thread/loaded/list pagination exceeded matcher max_pages budget",
            ));
        }

        let page = run_with_deadline(
            start,
            budget.timeout,
            client.list_loaded_threads(cursor.clone()),
        )
        .await
        .map_err(|e| classify_probe_error(e, "thread/loaded/list"))?;
        pages_scanned += 1;

        if page.thread_ids.is_empty() && page.next_cursor.is_some() {
            return Err(err(
                IdentityMatchErrorKind::IncompletePagination,
                "thread/loaded/list returned an empty page with a continuation cursor",
            ));
        }

        for raw_id in page.thread_ids {
            raw_entries += 1;
            if raw_entries > budget.max_candidates {
                return Err(err(
                    IdentityMatchErrorKind::IncompletePagination,
                    "thread/loaded/list entry count exceeded matcher max_candidates budget",
                ));
            }
            validate_canonical_thread_uuid(&raw_id, "thread/loaded/list thread_id")?;
            loaded_ids.insert(raw_id);
        }

        let Some(next) = page.next_cursor else {
            break;
        };
        validate_bounded_identifier(&next, "next_cursor", MAX_CURSOR_LEN).map_err(|_| {
            err(
                IdentityMatchErrorKind::IncompletePagination,
                "thread/loaded/list returned an invalid pagination cursor",
            )
        })?;
        if !seen_cursors.insert(next.clone()) {
            return Err(err(
                IdentityMatchErrorKind::IncompletePagination,
                "thread/loaded/list pagination cursor cycle detected",
            ));
        }
        if pages_scanned >= budget.max_pages {
            return Err(err(
                IdentityMatchErrorKind::IncompletePagination,
                "thread/loaded/list reached max_pages with unread continuation pages remaining",
            ));
        }
        cursor = Some(next);
    }

    Ok((loaded_ids, pages_scanned))
}

fn ensure_thread_is_loaded(status: &str) -> Result<bool, IdentityMatchError> {
    match status.trim() {
        "idle" | "active" => Ok(true),
        "not_loaded" | "notLoaded" => Ok(false),
        "system_error" | "systemError" => Err(err(
            IdentityMatchErrorKind::UnsupportedStatus,
            "loaded thread reported system_error status",
        )),
        _ => Err(err(
            IdentityMatchErrorKind::UnsupportedStatus,
            "loaded thread reported an unsupported or ambiguous status",
        )),
    }
}

fn evaluate_threadlane_mcp_ready(
    statuses: &[NativeMcpServerStatus],
) -> Result<bool, IdentityMatchError> {
    let mut matching = statuses
        .iter()
        .filter(|entry| entry.name == MCP_SERVER_NAME);
    let Some(status) = matching.next() else {
        return Err(err(
            IdentityMatchErrorKind::AbsentOrInvalidMcpServer,
            "threadlane MCP server entry is absent from mcpServerStatus/list",
        ));
    };
    if matching.next().is_some() {
        return Err(err(
            IdentityMatchErrorKind::AbsentOrInvalidMcpServer,
            "mcpServerStatus/list returned duplicate threadlane MCP server entries",
        ));
    }
    if status.tools_error.is_some() {
        return Err(err(
            IdentityMatchErrorKind::AbsentOrInvalidMcpServer,
            "threadlane MCP server entry reported a tools enumeration error",
        ));
    }

    match status.runtime_status.as_deref().map(str::trim) {
        Some("notStarted" | "not_started" | "stopped" | "disabled") => Ok(false),
        Some("connected" | "running" | "ready") => {
            if !status
                .tool_names
                .iter()
                .any(|t| t == CONNECTION_IDENTITY_TOOL)
            {
                return Err(err(
                    IdentityMatchErrorKind::AbsentOrInvalidMcpServer,
                    "running threadlane MCP server does not advertise connection_identity",
                ));
            }
            Ok(true)
        }
        Some(_) | None => Err(err(
            IdentityMatchErrorKind::UnsupportedStatus,
            "threadlane MCP server reported an unsupported or ambiguous runtime status",
        )),
    }
}

fn validate_loaded_thread_metadata(
    thread: &NativeThread,
    expected_thread_id: &str,
) -> Result<(), IdentityMatchError> {
    validate_canonical_thread_uuid(&thread.id, "thread.id")?;
    if thread.id != expected_thread_id {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "thread/read returned a thread.id that does not match the requested threadId",
        ));
    }
    if thread.cwd.trim().is_empty() || thread.cwd.chars().any(char::is_control) {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            "thread/read returned an empty or invalid cwd",
        ));
    }
    if let Some(ref project_id) = thread.project_id {
        validate_bounded_identifier(project_id, "project_id", MAX_IDENTIFIER_LEN).map_err(
            |_| {
                err(
                    IdentityMatchErrorKind::ThreadMetadataMismatch,
                    "thread/read returned an invalid projectId identifier",
                )
            },
        )?;
    }
    Ok(())
}

fn validate_canonical_thread_uuid(
    raw: &str,
    field: &'static str,
) -> Result<(), IdentityMatchError> {
    validate_bounded_identifier(raw, field, MAX_IDENTIFIER_LEN).map_err(|_| {
        err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            format!("{field} is empty or contains invalid characters"),
        )
    })?;
    let parsed = Uuid::parse_str(raw).map_err(|_| {
        err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            format!("{field} is not a valid UUID"),
        )
    })?;
    if parsed.is_nil() || parsed.hyphenated().to_string() != raw {
        return Err(err(
            IdentityMatchErrorKind::ThreadMetadataMismatch,
            format!("{field} must be a canonical lowercase hyphenated non-nil UUID"),
        ));
    }
    Ok(())
}

fn validate_connection_identity_fields(
    identity: &ConnectionIdentity,
    reject_terminal_states: bool,
) -> Result<(), IdentityMatchError> {
    validate_prefixed_hex_token(&identity.connection_nonce, "nce_", "connection_nonce")?;
    validate_prefixed_hex_token(&identity.epoch_id, "epc_", "epoch_id")?;
    if identity.server_pid == 0 {
        return Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            "ConnectionIdentity server_pid must be non-zero",
        ));
    }
    match identity.state {
        ConnectionEpochState::Unbound if identity.enrolled_binding_id.is_some() => Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            "unbound ConnectionIdentity must not carry an enrolled_binding_id",
        )),
        ConnectionEpochState::Unbound => Ok(()),
        ConnectionEpochState::Enrolled => {
            let Some(ref binding_id) = identity.enrolled_binding_id else {
                return Err(err(
                    IdentityMatchErrorKind::InvalidIdentity,
                    "enrolled ConnectionIdentity must carry an enrolled_binding_id",
                ));
            };
            validate_bounded_identifier(binding_id, "enrolled_binding_id", MAX_IDENTIFIER_LEN)
                .map(|_| ())
                .map_err(|_| {
                    err(
                        IdentityMatchErrorKind::InvalidIdentity,
                        "ConnectionIdentity enrolled_binding_id is invalid",
                    )
                })
        }
        ConnectionEpochState::Expired | ConnectionEpochState::QuarantinedSharedConnection => {
            if reject_terminal_states {
                Err(err(
                    IdentityMatchErrorKind::InvalidIdentity,
                    "expected ConnectionIdentity cannot be expired or quarantined",
                ))
            } else {
                Ok(())
            }
        }
    }
}

fn validate_prefixed_hex_token(
    raw: &str,
    prefix: &'static str,
    label: &'static str,
) -> Result<(), IdentityMatchError> {
    let Some(hex_part) = raw.strip_prefix(prefix) else {
        return Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            format!("{label} is missing required prefix"),
        ));
    };
    if hex_part.len() != EXPECTED_PREFIXED_TOKEN_HEX_LEN
        || !hex_part
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            format!("{label} must be {EXPECTED_PREFIXED_TOKEN_HEX_LEN} lowercase hex characters"),
        ));
    }
    Ok(())
}

fn parse_and_validate_connection_identity(
    tool_result: &NativeMcpToolCallResult,
) -> Result<ConnectionIdentity, IdentityMatchError> {
    if tool_result.is_error {
        return Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            "connection_identity tool call returned isError=true",
        ));
    }

    let structured = tool_result
        .structured_content
        .as_ref()
        .map(extract_identity_from_envelope)
        .transpose()?;

    let text_based = match tool_result
        .content
        .first()
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
    {
        Some(raw_text) => {
            let parsed_val: Value = serde_json::from_str(raw_text).map_err(|_| {
                err(
                    IdentityMatchErrorKind::InvalidIdentity,
                    "connection_identity text content is not valid JSON",
                )
            })?;
            Some(extract_identity_from_envelope(&parsed_val)?)
        }
        None => None,
    };

    let identity = match (structured, text_based) {
        (Some(s), Some(t)) => {
            if !constant_time_str_eq(&s.connection_nonce, &t.connection_nonce)
                || !constant_time_str_eq(&s.epoch_id, &t.epoch_id)
                || s.server_pid != t.server_pid
                || s.state != t.state
                || s.enrolled_binding_id != t.enrolled_binding_id
            {
                return Err(err(
                    IdentityMatchErrorKind::InvalidIdentity,
                    "connection_identity structuredContent and text content disagreed",
                ));
            }
            s
        }
        (Some(s), None) => s,
        (None, Some(t)) => t,
        (None, None) => {
            return Err(err(
                IdentityMatchErrorKind::InvalidIdentity,
                "connection_identity tool result contained neither structuredContent nor text content",
            ));
        }
    };

    validate_connection_identity_fields(&identity, false)?;
    Ok(identity)
}

fn extract_identity_from_envelope(
    envelope: &Value,
) -> Result<ConnectionIdentity, IdentityMatchError> {
    if envelope.get("error").is_some() {
        return Err(err(
            IdentityMatchErrorKind::InvalidIdentity,
            "connection_identity payload contained an error object",
        ));
    }
    let data = envelope.get("data").unwrap_or(envelope);
    serde_json::from_value::<ConnectionIdentity>(data.clone()).map_err(|_| {
        err(
            IdentityMatchErrorKind::InvalidIdentity,
            "connection_identity payload did not match ConnectionIdentity schema",
        )
    })
}

async fn run_with_deadline<F, T>(
    start: Instant,
    timeout: Duration,
    fut: F,
) -> Result<T, NativeError>
where
    F: std::future::Future<Output = Result<T, NativeError>>,
{
    let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
        return Err(NativeError::new(
            NativeErrorCode::TimedOut,
            "identity matcher exceeded overall timeout budget",
        ));
    };
    if remaining.is_zero() {
        return Err(NativeError::new(
            NativeErrorCode::TimedOut,
            "identity matcher exceeded overall timeout budget",
        ));
    }
    tokio::time::timeout(remaining, fut).await.map_err(|_| {
        NativeError::new(
            NativeErrorCode::TimedOut,
            "identity matcher operation timed out",
        )
    })?
}

fn classify_probe_error(err: NativeError, context: &'static str) -> IdentityMatchError {
    if err.error_code() == NativeErrorCode::TimedOut {
        return IdentityMatchError::with_rpc_code(
            IdentityMatchErrorKind::TimedOut,
            err.rpc_code,
            format!("{context} timed out"),
        );
    }
    let lower = err.message().to_ascii_lowercase();
    if err.rpc_code == Some(-32600)
        || err.error_code() == NativeErrorCode::UnsupportedCapability
        || lower.contains("sub-agent")
        || lower.contains("subagent")
        || lower.contains("direct app-server input is not allowed")
    {
        return IdentityMatchError::with_rpc_code(
            IdentityMatchErrorKind::UnprobeableCandidate,
            err.rpc_code,
            format!(
                "loaded candidate rejected {context} probe (such as an active v2 subagent); ownership across all loaded threads cannot be verified"
            ),
        );
    }
    IdentityMatchError::with_rpc_code(
        IdentityMatchErrorKind::ProviderError,
        err.rpc_code,
        format!("{context} failed ({})", err.code()),
    )
}
