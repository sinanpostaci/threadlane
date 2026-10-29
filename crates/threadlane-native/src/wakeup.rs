use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use threadlane_core::FIXED_WAKEUP_MESSAGE;

use crate::error::{NativeError, NativeErrorCode};
use crate::fs_safety::{
    acquire_cross_process_lock, generate_prefixed_random_id, prepare_private_dir,
    read_private_file_bounded, sanitize_diagnostic_text, validate_bounded_identifier,
    write_private_file_atomic, MAX_PRIVATE_STATE_FILE_BYTES,
};
use crate::pairing::NativeVerificationStatus;

const WAKEUP_LOCK_FILENAME: &str = "wakeup.lock";
const WAKEUP_STATE_FILENAME: &str = "wakeup_state.json";
const WAKEUP_SCHEMA_VERSION: u32 = 1;
const MAX_WAKEUP_RECORDS: usize = 256;
const MAX_ID_CHARS: usize = 128;

/// Durable lifecycle state for a native chat wake-up / `turn/steer` delivery.
///
/// Explicitly distinguishes `Pending`, `Submitted` (accepted by transport/RPC, not yet proven
/// handled by the model), `Unknown` (ambiguous timeout/disconnect after wire write), and `Received`
/// (reconciled via explicit MCP pull/ack).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeWakeupStatus {
    /// Wake-up is queued locally and has not yet been transmitted.
    Pending,
    /// `turn/steer` or `queue` returned success, but the model has not yet pulled events via MCP.
    Submitted,
    /// Transmission timed out, disconnected, or returned an ambiguous response after wire write.
    Unknown,
    /// Bound chat explicitly pulled/acknowledged the pending update over MCP.
    Received,
}

impl NativeWakeupStatus {
    /// Returns the static `snake_case` status string.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Submitted => "submitted",
            Self::Unknown => "unknown",
            Self::Received => "received",
        }
    }
}

/// Durable record for a native chat wake-up / `turn/steer` attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWakeupRecord {
    /// Scoped Threadlane `binding_id`.
    pub binding_id: String,
    /// Authoritative native Codex `thread.id` UUID (never `sessionId` or chat title).
    pub thread_id: String,
    /// Server-reported live turn UUID required by `turn/steer`, if known.
    pub expected_turn_id: Option<String>,
    /// Unique attempt identifier for CAS / reconciliation checks.
    pub attempt_id: String,
    /// Current durable wake-up status.
    pub status: NativeWakeupStatus,
    /// Fixed machine wake message (`FIXED_WAKEUP_MESSAGE`; never worker prose).
    pub fixed_message: String,
    /// Optional sanitized diagnostic note for `Unknown` or rejected attempts.
    pub diagnostic: Option<String>,
    /// Honest verification gate status (`needs_verification` or `blocked_ambiguous_wakeup`).
    pub verification_gate: NativeVerificationStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WakeupStateFile {
    schema_version: u32,
    records: Vec<NativeWakeupRecord>,
}

impl Default for WakeupStateFile {
    fn default() -> Self {
        Self {
            schema_version: WAKEUP_SCHEMA_VERSION,
            records: Vec::new(),
        }
    }
}

/// Builds the exact `turn/steer` JSON-RPC parameter object for `thread_id` and `expected_turn_id`.
///
/// Always uses [`FIXED_WAKEUP_MESSAGE`] and never includes model, effort, cwd, or approval overrides.
pub fn build_turn_steer_params(
    thread_id: &str,
    expected_turn_id: &str,
) -> Result<Value, NativeError> {
    let clean_thread = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
    let clean_turn =
        validate_bounded_identifier(expected_turn_id, "expected_turn_id", MAX_ID_CHARS)?;
    Ok(json!({
        "threadId": clean_thread,
        "expectedTurnId": clean_turn,
        "input": [
            {
                "type": "text",
                "text": FIXED_WAKEUP_MESSAGE
            }
        ]
    }))
}

/// Builds the exact 5-element fallback `codex queue` argument vector using `thread.id` UUID and
/// [`FIXED_WAKEUP_MESSAGE`].
pub fn build_native_queue_fallback_args(thread_id: &str) -> Result<[String; 5], NativeError> {
    let clean_thread = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
    Ok([
        "queue".to_string(),
        "--thread".to_string(),
        clean_thread,
        "--message".to_string(),
        FIXED_WAKEUP_MESSAGE.to_string(),
    ])
}

/// Private (`0700`/`0600`), cross-process locked durable ledger for native wake-up and `turn/steer` state.
///
/// Prevents blind resends when a prior submission is in `Unknown` or unacknowledged `Submitted` state.
#[derive(Debug, Clone)]
pub struct NativeWakeupStore {
    dir: PathBuf,
}

impl NativeWakeupStore {
    /// Opens or initializes the private `0700` native wake-up store directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, NativeError> {
        let clean_dir = prepare_private_dir(dir.as_ref(), "native wakeup store")?;
        let store = Self { dir: clean_dir };
        let _lock =
            acquire_cross_process_lock(&store.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let _ = store.load_locked()?;
        Ok(store)
    }

    /// Returns the canonical private directory backing this store.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns a snapshot of all recorded wake-up entries.
    pub fn snapshot(&self) -> Result<Vec<NativeWakeupRecord>, NativeError> {
        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        Ok(self.load_locked()?.records)
    }

    /// Fetches the current [`NativeWakeupRecord`] for `binding_id`, if any.
    pub fn get(&self, binding_id: &str) -> Result<Option<NativeWakeupRecord>, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let state = self.load_locked()?;
        Ok(state
            .records
            .into_iter()
            .find(|r| r.binding_id == clean_binding))
    }

    /// Queues or returns a `Pending` wake-up for `(binding_id, thread_id, expected_turn_id)`.
    ///
    /// Refuses with [`NativeErrorCode::AmbiguousWakeupPendingReconciliation`] if the binding has
    /// an unresolved `Unknown` or unacknowledged `Submitted` wake-up.
    pub fn prepare_pending(
        &self,
        binding_id: &str,
        thread_id: &str,
        expected_turn_id: Option<&str>,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let clean_thread = validate_bounded_identifier(thread_id, "thread_id", MAX_ID_CHARS)?;
        let clean_turn = match expected_turn_id {
            Some(t) => Some(validate_bounded_identifier(
                t,
                "expected_turn_id",
                MAX_ID_CHARS,
            )?),
            None => None,
        };

        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;

        if let Some(existing) = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
        {
            match existing.status {
                NativeWakeupStatus::Unknown => {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "prior native wakeup submission is in ambiguous `unknown` state; reconcile before resending",
                    ));
                }
                NativeWakeupStatus::Submitted => {
                    return Err(NativeError::new(
                        NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                        "prior native wakeup is already `submitted` and awaiting MCP receipt; refusing blind duplicate send",
                    ));
                }
                NativeWakeupStatus::Pending => {
                    existing.thread_id = clean_thread;
                    existing.expected_turn_id = clean_turn;
                    existing.fixed_message = FIXED_WAKEUP_MESSAGE.to_string();
                    existing.verification_gate = NativeVerificationStatus::NeedsVerification;
                    let updated = existing.clone();
                    self.save_locked(&state)?;
                    return Ok(updated);
                }
                NativeWakeupStatus::Received => {
                    existing.thread_id = clean_thread;
                    existing.expected_turn_id = clean_turn;
                    existing.attempt_id = generate_prefixed_random_id("wak");
                    existing.status = NativeWakeupStatus::Pending;
                    existing.fixed_message = FIXED_WAKEUP_MESSAGE.to_string();
                    existing.diagnostic = None;
                    existing.verification_gate = NativeVerificationStatus::NeedsVerification;
                    let updated = existing.clone();
                    self.save_locked(&state)?;
                    return Ok(updated);
                }
            }
        }

        if state.records.len() >= MAX_WAKEUP_RECORDS {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "maximum native wakeup record count exceeded",
            ));
        }

        let record = NativeWakeupRecord {
            binding_id: clean_binding,
            thread_id: clean_thread,
            expected_turn_id: clean_turn,
            attempt_id: generate_prefixed_random_id("wak"),
            status: NativeWakeupStatus::Pending,
            fixed_message: FIXED_WAKEUP_MESSAGE.to_string(),
            diagnostic: None,
            verification_gate: NativeVerificationStatus::NeedsVerification,
        };
        state.records.push(record.clone());
        self.save_locked(&state)?;
        Ok(record)
    }

    /// Records that a `Pending` wake-up attempt was accepted by `turn/steer` (`Submitted`).
    pub fn record_submitted(
        &self,
        binding_id: &str,
        expected_attempt_id: &str,
        verified_turn_id: &str,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let clean_attempt =
            validate_bounded_identifier(expected_attempt_id, "expected_attempt_id", MAX_ID_CHARS)?;
        let clean_turn =
            validate_bounded_identifier(verified_turn_id, "verified_turn_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "wakeup record not found for binding_id",
                )
            })?;

        if record.attempt_id != clean_attempt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "wakeup attempt_id mismatch when recording submitted status",
            ));
        }
        record.expected_turn_id = Some(clean_turn);
        record.status = NativeWakeupStatus::Submitted;
        record.diagnostic = None;
        record.verification_gate = NativeVerificationStatus::NeedsVerification;
        let out = record.clone();
        self.save_locked(&state)?;
        Ok(out)
    }

    /// Records that a `Pending` wake-up attempt encountered an ambiguous failure after wire write (`Unknown`).
    pub fn record_unknown(
        &self,
        binding_id: &str,
        expected_attempt_id: &str,
        reason: &str,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let clean_attempt =
            validate_bounded_identifier(expected_attempt_id, "expected_attempt_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "wakeup record not found for binding_id",
                )
            })?;

        if record.attempt_id != clean_attempt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "wakeup attempt_id mismatch when recording unknown status",
            ));
        }
        record.status = NativeWakeupStatus::Unknown;
        record.diagnostic = Some(sanitize_diagnostic_text(reason));
        record.verification_gate = NativeVerificationStatus::BlockedAmbiguousWakeup;
        let out = record.clone();
        self.save_locked(&state)?;
        Ok(out)
    }

    /// Records a pre-transmission or explicit server rejection diagnostic while keeping status `Pending`.
    pub fn record_rejected_pre_delivery(
        &self,
        binding_id: &str,
        expected_attempt_id: &str,
        reason: &str,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let clean_attempt =
            validate_bounded_identifier(expected_attempt_id, "expected_attempt_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "wakeup record not found for binding_id",
                )
            })?;

        if record.attempt_id != clean_attempt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "wakeup attempt_id mismatch",
            ));
        }
        record.status = NativeWakeupStatus::Pending;
        record.diagnostic = Some(sanitize_diagnostic_text(reason));
        let out = record.clone();
        self.save_locked(&state)?;
        Ok(out)
    }

    /// Reconciles a binding's wake-up record to `Received` when the bound chat pulls/acknowledges events via MCP.
    pub fn mark_received(&self, binding_id: &str) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "wakeup record not found for binding_id",
                )
            })?;

        record.status = NativeWakeupStatus::Received;
        record.diagnostic = None;
        record.verification_gate = NativeVerificationStatus::NeedsVerification;
        let out = record.clone();
        self.save_locked(&state)?;
        Ok(out)
    }

    /// Explicitly re-arms an `Unknown` (when `allow_unknown` is `true`) or `Submitted` wake-up
    /// back to `Pending` with a new `attempt_id` after operator verification.
    pub fn reconcile_for_retry(
        &self,
        binding_id: &str,
        expected_attempt_id: &str,
        allow_unknown: bool,
    ) -> Result<NativeWakeupRecord, NativeError> {
        let clean_binding = validate_bounded_identifier(binding_id, "binding_id", MAX_ID_CHARS)?;
        let clean_attempt =
            validate_bounded_identifier(expected_attempt_id, "expected_attempt_id", MAX_ID_CHARS)?;

        let _lock =
            acquire_cross_process_lock(&self.dir, WAKEUP_LOCK_FILENAME, "native wakeup store")?;
        let mut state = self.load_locked()?;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.binding_id == clean_binding)
            .ok_or_else(|| {
                NativeError::new(
                    NativeErrorCode::ValidationFailed,
                    "wakeup record not found for binding_id",
                )
            })?;

        if record.attempt_id != clean_attempt {
            return Err(NativeError::new(
                NativeErrorCode::ValidationFailed,
                "wakeup attempt_id mismatch during retry reconciliation",
            ));
        }
        if record.status == NativeWakeupStatus::Unknown && !allow_unknown {
            return Err(NativeError::new(
                NativeErrorCode::AmbiguousWakeupPendingReconciliation,
                "allow_unknown=true is required to re-arm an ambiguous `unknown` wakeup",
            ));
        }

        record.attempt_id = generate_prefixed_random_id("wak");
        record.status = NativeWakeupStatus::Pending;
        record.diagnostic = None;
        record.verification_gate = NativeVerificationStatus::NeedsVerification;
        let out = record.clone();
        self.save_locked(&state)?;
        Ok(out)
    }

    fn load_locked(&self) -> Result<WakeupStateFile, NativeError> {
        let path = self.dir.join(WAKEUP_STATE_FILENAME);
        let Some(bytes) =
            read_private_file_bounded(&path, MAX_PRIVATE_STATE_FILE_BYTES, "native wakeup state")?
        else {
            return Ok(WakeupStateFile::default());
        };
        let parsed: WakeupStateFile = serde_json::from_slice(&bytes).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "corrupt native wakeup state JSON",
            )
        })?;
        if parsed.schema_version != WAKEUP_SCHEMA_VERSION {
            return Err(NativeError::new(
                NativeErrorCode::ProtocolError,
                format!(
                    "unsupported native wakeup schema version {}",
                    parsed.schema_version
                ),
            ));
        }
        Ok(parsed)
    }

    fn save_locked(&self, state: &WakeupStateFile) -> Result<(), NativeError> {
        let encoded = serde_json::to_vec_pretty(state).map_err(|_| {
            NativeError::new(
                NativeErrorCode::ProtocolError,
                "failed to serialize native wakeup state",
            )
        })?;
        write_private_file_atomic(
            &self.dir,
            WAKEUP_STATE_FILENAME,
            &encoded,
            "native wakeup state",
        )?;
        Ok(())
    }
}
