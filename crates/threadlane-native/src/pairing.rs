use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, constant_time_str_eq, generate_prefixed_random_id,
    prepare_private_dir, read_private_file_bounded, validate_bounded_identifier,
    write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::rpc::is_process_alive;

/// Canonical MCP server name configured in Codex for Threadlane.
pub const MCP_SERVER_NAME: &str = "threadlane";

/// Canonical bootstrap MCP tool name used by the trusted operator to query the connection nonce.
pub const CONNECTION_IDENTITY_TOOL: &str = "connection_identity";

const PAIRING_LOCK_FILENAME: &str = "pairing.lock";
const PAIRING_STATE_FILENAME: &str = "pairing_state.json";
const OPERATOR_TOKEN_FILENAME: &str = "operator.token";
const PAIRING_SCHEMA_VERSION: u32 = 1;
const MAX_TRACKED_SESSIONS: usize = 256;
const MAX_ID_CHARS: usize = 128;

/// Honest verification status for native multi-chat routing and busy-turn delivery gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeVerificationStatus {
    /// Live native two-chat/busy-turn behavior still requires an authorized host experiment.
    NeedsVerification,
    /// Native Codex executable or pairing store is not configured.
    NotConfigured,
    /// Blocked because a single MCP child connection was observed shared between two chats.
    BlockedSharedConnection,
    /// Blocked because a prior busy-turn wakeup attempt is in ambiguous `Unknown` state.
    BlockedAmbiguousWakeup,
    /// Exercised against local fake JSON-RPC fixtures only (not live multi-chat acceptance).
    FixtureVerifiedOnly,
}

impl NativeVerificationStatus {
    /// Returns the static `snake_case` representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NeedsVerification => "needs_verification",
            Self::NotConfigured => "not_configured",
            Self::BlockedSharedConnection => "blocked_shared_connection",
            Self::BlockedAmbiguousWakeup => "blocked_ambiguous_wakeup",
            Self::FixtureVerifiedOnly => "fixture_verified_only",
        }
    }
}

/// Lifecycle state of a single MCP stdio child connection epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionEpochState {
    /// Initial state at MCP startup; diagnostics and verified managed-desktop connection are available.
    Unbound,
    /// Enrolled by the trusted operator to a single `(binding_id, thread_id)` scope.
    Enrolled,
    /// Retired due to MCP process restart, replacement enrollment, or operator revocation.
    Expired,
    /// Permanently quarantined because two distinct native threads attempted to use this connection.
    QuarantinedSharedConnection,
}

impl ConnectionEpochState {
    /// Returns the static `snake_case` representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unbound => "unbound",
            Self::Enrolled => "enrolled",
            Self::Expired => "expired",
            Self::QuarantinedSharedConnection => "quarantined_shared_connection",
        }
    }
}

/// Public connection identity payload returned by the `connection_identity` MCP tool.
///
/// Knowing `connection_nonce` and `epoch_id` does not grant MCP callers enrollment authority;
/// only the local operator with access to the private `0600` operator token can enroll.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionIdentity {
    /// Fresh CSPRNG connection nonce generated at MCP child startup.
    pub connection_nonce: String,
    /// Fresh CSPRNG epoch identifier generated at MCP child startup.
    pub epoch_id: String,
    /// OS process ID of the MCP child server.
    pub server_pid: u32,
    /// Current connection epoch state.
    pub state: ConnectionEpochState,
    /// Bound Threadlane `binding_id`, if enrolled.
    pub enrolled_binding_id: Option<String>,
    /// Honest verification status (`needs_verification` until live two-chat host validation).
    pub verification_gate: NativeVerificationStatus,
}

/// Operator-only request to enroll a probed `(connection_nonce, epoch_id)` to a single native chat.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorEnrollRequest {
    /// Nonce returned by `mcpServer/tool/call` -> `connection_identity` for `thread_id`.
    pub connection_nonce: String,
    /// Epoch ID returned by `mcpServer/tool/call` -> `connection_identity` for `thread_id`.
    pub epoch_id: String,
    /// Scoped Threadlane `binding_id` to attach.
    pub binding_id: String,
    /// Authoritative native Codex `thread.id` (never `sessionId`).
    pub thread_id: String,
    /// Optional native `projectId` verified from `thread/read`.
    pub project_id: Option<String>,
}

/// Verified enrollment record stored in [`PairingStore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentRecord {
    /// Enrolled connection nonce.
    pub connection_nonce: String,
    /// Enrolled connection epoch ID.
    pub epoch_id: String,
    /// Owning MCP child process ID.
    pub server_pid: u32,
    /// Enrolled Threadlane `binding_id`.
    pub binding_id: String,
    /// Enrolled native Codex `thread.id` (never `sessionId`).
    pub thread_id: String,
    /// Enrolled native `projectId`, if present.
    pub project_id: Option<String>,
    /// Current state (`Enrolled`).
    pub state: ConnectionEpochState,
    /// Honest verification gate status.
    pub verification_gate: NativeVerificationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSessionEntry {
    connection_nonce: String,
    epoch_id: String,
    server_pid: u32,
    state: ConnectionEpochState,
    binding_id: Option<String>,
    thread_id: Option<String>,
    project_id: Option<String>,
    quarantined_second_thread_id: Option<String>,
}

impl StoredSessionEntry {
    fn to_identity(&self) -> ConnectionIdentity {
        let verification_gate = match self.state {
            ConnectionEpochState::QuarantinedSharedConnection => {
                NativeVerificationStatus::BlockedSharedConnection
            }
            _ => NativeVerificationStatus::NeedsVerification,
        };
        ConnectionIdentity {
            connection_nonce: self.connection_nonce.clone(),
            epoch_id: self.epoch_id.clone(),
            server_pid: self.server_pid,
            state: self.state,
            enrolled_binding_id: if self.state == ConnectionEpochState::Enrolled {
                self.binding_id.clone()
            } else {
                None
            },
            verification_gate,
        }
    }

    fn to_enrollment(&self) -> Option<EnrollmentRecord> {
        if self.state != ConnectionEpochState::Enrolled {
            return None;
        }
        Some(EnrollmentRecord {
            connection_nonce: self.connection_nonce.clone(),
            epoch_id: self.epoch_id.clone(),
            server_pid: self.server_pid,
            binding_id: self.binding_id.clone()?,
            thread_id: self.thread_id.clone()?,
            project_id: self.project_id.clone(),
            state: self.state,
            verification_gate: NativeVerificationStatus::NeedsVerification,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PairingStateFile {
    schema_version: u32,
    sessions: Vec<StoredSessionEntry>,
}

impl Default for PairingStateFile {
    fn default() -> Self {
        Self {
            schema_version: PAIRING_SCHEMA_VERSION,
            sessions: Vec::new(),
        }
    }
}

/// Private (`0700`/`0600`), cross-process locked store managing MCP connection-epoch pairing.
///
/// Enforces:
/// - MCP servers start `Unbound` with a unique CSPRNG `connection_nonce` and `epoch_id`.
/// - Only the local operator holding the private `0600` operator token can enroll a session.
/// - One MCP connection can never serve two native `thread.id`s; attempting to enroll a second
///   thread on the same connection immediately quarantines the connection for both threads.
/// - Process restarts or new enrollments for the same binding/thread expire prior epochs.
#[derive(Debug, Clone)]
pub struct PairingStore {
    dir: PathBuf,
}

impl PairingStore {
    /// Opens or initializes a private `0700` pairing directory and ensures the `0600` operator token exists.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        let clean_dir = prepare_private_dir(dir.as_ref(), "native MCP pairing")?;
        let store = Self { dir: clean_dir };
        let _lock =
            acquire_cross_process_lock(&store.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        let _ = store.ensure_operator_token_locked()?;
        let _ = store.load_state_locked()?;
        Ok(store)
    }

    /// Returns the canonical private directory backing this [`PairingStore`].
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Reads the private `0600` operator token required for trusted enrollment calls.
    pub fn operator_token(&self) -> Result<String, NativeError> {
        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        self.ensure_operator_token_locked()
    }

    /// Registers a brand-new `Unbound` connection epoch for an MCP server process (`server_pid`).
    ///
    /// Automatically expires any prior epochs owned by dead OS processes.
    pub fn register_unbound_epoch(
        &self,
        server_pid: u32,
    ) -> Result<ConnectionIdentity, NativeError> {
        self.register_epoch_internal(server_pid, None)
    }

    /// Registers a fresh `Unbound` connection epoch after an MCP server restart, immediately
    /// expiring `previous_epoch_id` even if the process ID was reused or restarted in-process.
    pub fn register_restarted_epoch(
        &self,
        previous_epoch_id: &str,
        server_pid: u32,
    ) -> Result<ConnectionIdentity, NativeError> {
        let prev =
            validate_bounded_identifier(previous_epoch_id, "previous_epoch_id", MAX_ID_CHARS)?;
        self.register_epoch_internal(server_pid, Some(&prev))
    }

    fn register_epoch_internal(
        &self,
        server_pid: u32,
        retire_epoch_id: Option<&str>,
    ) -> Result<ConnectionIdentity, NativeError> {
        if server_pid == 0 {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "server_pid must be non-zero",
            ));
        }

        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        let mut state = self.load_state_locked()?;

        for existing in &mut state.sessions {
            if matches!(
                existing.state,
                ConnectionEpochState::Unbound | ConnectionEpochState::Enrolled
            ) {
                let matches_retired = retire_epoch_id
                    .is_some_and(|prev| constant_time_str_eq(&existing.epoch_id, prev));
                if matches_retired || !is_process_alive(existing.server_pid) {
                    existing.state = ConnectionEpochState::Expired;
                }
            }
        }

        if state.sessions.len() >= MAX_TRACKED_SESSIONS {
            state
                .sessions
                .retain(|s| !matches!(s.state, ConnectionEpochState::Expired));
            if state.sessions.len() >= MAX_TRACKED_SESSIONS {
                state.sessions.remove(0);
            }
        }

        let entry = StoredSessionEntry {
            connection_nonce: generate_prefixed_random_id("nce"),
            epoch_id: generate_prefixed_random_id("epc"),
            server_pid,
            state: ConnectionEpochState::Unbound,
            binding_id: None,
            thread_id: None,
            project_id: None,
            quarantined_second_thread_id: None,
        };
        let identity = entry.to_identity();
        state.sessions.push(entry);
        self.save_state_locked(&state)?;

        Ok(identity)
    }

    /// Inspects the current [`ConnectionIdentity`] for `(connection_nonce, epoch_id)`.
    pub fn inspect_epoch(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
    ) -> Result<ConnectionIdentity, NativeError> {
        let _ = validate_bounded_identifier(connection_nonce, "connection_nonce", MAX_ID_CHARS)?;
        let _ = validate_bounded_identifier(epoch_id, "epoch_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        let mut state = self.load_state_locked()?;
        let mut mutated = false;

        let Some(entry) = state
            .sessions
            .iter_mut()
            .find(|s| constant_time_str_eq(&s.connection_nonce, connection_nonce))
        else {
            return Err(NativeError::new(
                NativeErrorCode::UnknownConnectionNonce,
                "connection nonce is not registered in pairing store",
            ));
        };

        if !constant_time_str_eq(&entry.epoch_id, epoch_id) {
            return Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "connection epoch ID does not match active session",
            ));
        }

        if matches!(
            entry.state,
            ConnectionEpochState::Unbound | ConnectionEpochState::Enrolled
        ) && !is_process_alive(entry.server_pid)
        {
            entry.state = ConnectionEpochState::Expired;
            mutated = true;
        }

        let identity = entry.to_identity();
        if mutated {
            self.save_state_locked(&state)?;
        }
        Ok(identity)
    }

    /// Verifies that `(connection_nonce, epoch_id)` is currently `Enrolled` and returns its [`EnrollmentRecord`].
    ///
    /// Called on every scoped MCP tool invocation before delegating to `ThreadlaneMcpServer`.
    pub fn verify_enrolled_binding(
        &self,
        connection_nonce: &str,
        epoch_id: &str,
    ) -> Result<EnrollmentRecord, NativeError> {
        let _ = validate_bounded_identifier(connection_nonce, "connection_nonce", MAX_ID_CHARS)?;
        let _ = validate_bounded_identifier(epoch_id, "epoch_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        let mut state = self.load_state_locked()?;

        let Some(idx) = state
            .sessions
            .iter()
            .position(|s| constant_time_str_eq(&s.connection_nonce, connection_nonce))
        else {
            return Err(NativeError::new(
                NativeErrorCode::UnknownConnectionNonce,
                "MCP connection nonce is unknown",
            ));
        };

        if !constant_time_str_eq(&state.sessions[idx].epoch_id, epoch_id) {
            return Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "MCP connection epoch has been superseded or expired",
            ));
        }

        if matches!(
            state.sessions[idx].state,
            ConnectionEpochState::Unbound | ConnectionEpochState::Enrolled
        ) && !is_process_alive(state.sessions[idx].server_pid)
        {
            state.sessions[idx].state = ConnectionEpochState::Expired;
            self.save_state_locked(&state)?;
        }

        match state.sessions[idx].state {
            ConnectionEpochState::Unbound => Err(NativeError::new(
                NativeErrorCode::UnboundConnection,
                "MCP connection is unbound; trusted operator enrollment is required before calling scoped tools",
            )),
            ConnectionEpochState::Expired => Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "MCP connection epoch has expired; refresh or re-enroll the native chat connection",
            )),
            ConnectionEpochState::QuarantinedSharedConnection => Err(NativeError::new(
                NativeErrorCode::SharedConnectionRejected,
                "MCP connection was quarantined because it was shared between multiple native threads",
            )),
            ConnectionEpochState::Enrolled => state.sessions[idx].to_enrollment().ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ProtocolError,
                    "enrolled session record is missing binding_id or thread_id",
                )
            }),
        }
    }

    /// Explicitly expires a connection epoch (trusted operator only).
    pub fn expire_epoch(&self, operator_token: &str, epoch_id: &str) -> Result<(), NativeError> {
        let _ = validate_bounded_identifier(epoch_id, "epoch_id", MAX_ID_CHARS)?;
        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        self.verify_operator_token_locked(operator_token)?;

        let mut state = self.load_state_locked()?;
        let mut found = false;
        for session in &mut state.sessions {
            if constant_time_str_eq(&session.epoch_id, epoch_id) {
                if session.state != ConnectionEpochState::QuarantinedSharedConnection {
                    session.state = ConnectionEpochState::Expired;
                }
                found = true;
            }
        }
        if !found {
            return Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "epoch_id not found in pairing store",
            ));
        }
        self.save_state_locked(&state)
    }

    /// Enrolls a probed `(connection_nonce, epoch_id)` session to `(binding_id, thread_id, project_id)`.
    ///
    /// Requires the private `operator_token`. Never callable by the model over MCP.
    /// If the connection is already enrolled to a different `thread_id`, the connection is
    /// permanently quarantined (`QuarantinedSharedConnection`) so neither thread can use it.
    pub fn enroll(
        &self,
        operator_token: &str,
        request: OperatorEnrollRequest,
    ) -> Result<EnrollmentRecord, NativeError> {
        let nonce = validate_bounded_identifier(
            &request.connection_nonce,
            "connection_nonce",
            MAX_ID_CHARS,
        )?;
        let epoch = validate_bounded_identifier(&request.epoch_id, "epoch_id", MAX_ID_CHARS)?;
        let binding_id =
            validate_bounded_identifier(&request.binding_id, "binding_id", MAX_ID_CHARS)?;
        let thread_id = validate_bounded_identifier(&request.thread_id, "thread_id", MAX_ID_CHARS)?;
        let project_id = match request.project_id {
            Some(ref p) => Some(validate_bounded_identifier(p, "project_id", MAX_ID_CHARS)?),
            None => None,
        };

        let _lock =
            acquire_cross_process_lock(&self.dir, PAIRING_LOCK_FILENAME, "native MCP pairing")?;
        self.verify_operator_token_locked(operator_token)?;

        let mut state = self.load_state_locked()?;
        let Some(target_idx) = state
            .sessions
            .iter()
            .position(|s| constant_time_str_eq(&s.connection_nonce, &nonce))
        else {
            return Err(NativeError::new(
                NativeErrorCode::UnknownConnectionNonce,
                "cannot enroll unknown MCP connection nonce",
            ));
        };

        if !constant_time_str_eq(&state.sessions[target_idx].epoch_id, &epoch) {
            return Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "cannot enroll stale MCP connection epoch",
            ));
        }

        if !is_process_alive(state.sessions[target_idx].server_pid) {
            state.sessions[target_idx].state = ConnectionEpochState::Expired;
            self.save_state_locked(&state)?;
            return Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "cannot enroll MCP connection epoch whose owning process has exited",
            ));
        }

        match state.sessions[target_idx].state {
            ConnectionEpochState::Expired => Err(NativeError::new(
                NativeErrorCode::ExpiredEpoch,
                "cannot enroll expired MCP connection epoch",
            )),
            ConnectionEpochState::QuarantinedSharedConnection => Err(NativeError::new(
                NativeErrorCode::SharedConnectionRejected,
                "MCP connection is quarantined because it was shared between two native threads",
            )),
            ConnectionEpochState::Enrolled => {
                let existing_thread = state.sessions[target_idx]
                    .thread_id
                    .as_deref()
                    .unwrap_or("");
                if existing_thread != thread_id {
                    // Two distinct native threads resolved to the same MCP connection child!
                    // Quarantine the connection immediately so neither thread can use it.
                    state.sessions[target_idx].state =
                        ConnectionEpochState::QuarantinedSharedConnection;
                    state.sessions[target_idx].quarantined_second_thread_id = Some(thread_id);
                    self.save_state_locked(&state)?;
                    return Err(NativeError::new(
                        NativeErrorCode::SharedConnectionRejected,
                        "rejected shared MCP connection observed across two distinct native threads; connection quarantined",
                    ));
                }

                let existing_binding = state.sessions[target_idx]
                    .binding_id
                    .as_deref()
                    .unwrap_or("");
                if existing_binding != binding_id
                    || state.sessions[target_idx].project_id != project_id
                {
                    return Err(NativeError::new(
                        NativeErrorCode::RebindRejected,
                        "cannot rebind an already-enrolled MCP connection to a different binding_id or project_id",
                    ));
                }

                state.sessions[target_idx].to_enrollment().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "corrupt enrollment state in pairing store",
                    )
                })
            }
            ConnectionEpochState::Unbound => {
                // Expire any older epochs previously enrolled to the same `binding_id` or `thread_id`.
                for (idx, session) in state.sessions.iter_mut().enumerate() {
                    if idx != target_idx
                        && session.state == ConnectionEpochState::Enrolled
                        && (session.binding_id.as_deref() == Some(binding_id.as_str())
                            || session.thread_id.as_deref() == Some(thread_id.as_str()))
                    {
                        session.state = ConnectionEpochState::Expired;
                    }
                }

                state.sessions[target_idx].state = ConnectionEpochState::Enrolled;
                state.sessions[target_idx].binding_id = Some(binding_id);
                state.sessions[target_idx].thread_id = Some(thread_id);
                state.sessions[target_idx].project_id = project_id;
                let record = state.sessions[target_idx].to_enrollment().ok_or_else(|| {
                    NativeError::new(
                        NativeErrorCode::ProtocolError,
                        "failed to build enrollment record",
                    )
                })?;
                self.save_state_locked(&state)?;
                Ok(record)
            }
        }
    }

    fn ensure_operator_token_locked(&self) -> Result<String, NativeError> {
        let token_path = self.dir.join(OPERATOR_TOKEN_FILENAME);
        if let Some(bytes) =
            read_private_file_bounded(&token_path, 256, "native pairing operator token")?
        {
            let token = String::from_utf8(bytes).map_err(|_| {
                NativeError::new(
                    NativeErrorCode::SecurityViolation,
                    "native pairing operator token is not valid UTF-8",
                )
            })?;
            let trimmed = token.trim().to_string();
            let _ = validate_bounded_identifier(&trimmed, "operator_token", MAX_ID_CHARS)?;
            return Ok(trimmed);
        }

        let fresh = generate_prefixed_random_id("op");
        write_private_file_atomic(
            &self.dir,
            OPERATOR_TOKEN_FILENAME,
            fresh.as_bytes(),
            "native pairing operator token",
        )?;
        Ok(fresh)
    }

    fn verify_operator_token_locked(&self, candidate: &str) -> Result<(), NativeError> {
        let expected = self.ensure_operator_token_locked()?;
        if !constant_time_str_eq(&expected, candidate) {
            return Err(NativeError::new(
                NativeErrorCode::UnauthorizedOperator,
                "invalid operator token for native MCP pairing enrollment",
            ));
        }
        Ok(())
    }

    fn load_state_locked(&self) -> Result<PairingStateFile, NativeError> {
        let state_path = self.dir.join(PAIRING_STATE_FILENAME);
        let Some(bytes) = read_private_file_bounded(
            &state_path,
            MAX_PRIVATE_STATE_FILE_BYTES,
            "native pairing state",
        )?
        else {
            return Ok(PairingStateFile::default());
        };

        let parsed: PairingStateFile = serde_json::from_slice(&bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "corrupt native MCP pairing state JSON",
            )
        })?;
        if parsed.schema_version != PAIRING_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported native MCP pairing schema version {}",
                    parsed.schema_version
                ),
            ));
        }
        Ok(parsed)
    }

    fn save_state_locked(&self, state: &PairingStateFile) -> Result<(), NativeError> {
        let encoded = serde_json::to_vec_pretty(state).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize native MCP pairing state",
            )
        })?;
        write_private_file_atomic(
            &self.dir,
            PAIRING_STATE_FILENAME,
            &encoded,
            "native pairing state",
        )?;
        Ok(())
    }
}
