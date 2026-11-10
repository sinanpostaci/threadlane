//! Bounded advisory provider diagnostic metadata, independent of execution state.
use serde::{Deserialize, Serialize};

/// Maximum encoded snapshot size admitted across the core boundary.
pub const MAX_PROVIDER_HEALTH_SNAPSHOT_BYTES: usize = 2048;
/// Maximum diagnostic records examined per invocation.
pub const MAX_PROVIDER_HEALTH_RECORDS: u32 = 4096;
/// Maximum cumulative update sequence, including coverage and completion updates.
pub const MAX_PROVIDER_HEALTH_UPDATES: u32 = 8192;
/// Largest exactly representable JSON integer.
pub const MAX_PROVIDER_HEALTH_ELAPSED_MS: u64 = 9_007_199_254_740_991;

/// Content trust class; owned transport does not verify logger semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthEvidenceClass {
    /// An untrusted log-text pattern, never proven transport failure or root cause.
    UnverifiedProviderDiagnosticHint,
}

/// Availability of the bounded diagnostic channel, never a health assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDiagnosticAvailability {
    /// An invocation-owned bounded source is being observed.
    Available,
    /// Diagnostics are unavailable or unsupported; silence is not healthy.
    Unavailable,
    /// A resource limit stopped classification; subsequent bytes are discarded.
    Truncated,
}

/// Finite coverage reason; contains no raw error, source path, or provider prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDiagnosticUnavailableReason {
    /// No classifier/capture is implemented for this provider.
    UnsupportedProvider,
    /// An owned private capture could not be established.
    CaptureSetupFailed,
    /// Source identity, ownership, permissions, or reading was lost.
    SourceLost,
    /// The invocation ended; absence of a hint does not establish health.
    SourceClosed,
    /// A byte, line, record, or update bound was reached.
    ResourceLimit,
}

/// Allowlisted text pattern, never a verified causal classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthCategory {
    /// A logger-shaped line reported stream-idle timeout and an internal retry.
    StreamIdleRetry,
}

/// Why a current advisory was closed; immutable observations remain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthRecovery {
    /// A later unverified hint closed an advisory; not inferred from silence.
    LaterDiagnosticHint,
    /// Existing independent execution evidence verified natural completion.
    VerifiedTerminalOutcome,
}

/// An immutable bounded observation made at a monotonic invocation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHealthObservation {
    /// Reported text category.
    pub category: ProviderHealthCategory,
    /// Monotonic elapsed invocation milliseconds; never parsed logger wall time.
    pub elapsed_ms: u64,
    /// Cumulative software update containing this observation.
    pub update_sequence: u32,
}

/// Cumulative, coalescible advisory snapshot with no lifecycle or acceptance authority.
///
/// Persistence and callbacks additionally require an independently verified invocation owner
/// and exact binding/thread/job/approval or command/reservation/attempt correlation. A decoded
/// DTO, watch update, or owned pipe alone supplies none of that authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHealthSnapshot {
    /// Version of this finite wire shape.
    pub schema_version: u8,
    /// Explicitly unverified content trust.
    pub evidence_class: ProviderHealthEvidenceClass,
    /// Diagnostic coverage, independent of process liveness and tool progress.
    pub availability: ProviderDiagnosticAvailability,
    /// Fixed explanation when coverage is unavailable or truncated.
    pub unavailable_reason: Option<ProviderDiagnosticUnavailableReason>,
    /// Last software observation's monotonic invocation time.
    pub elapsed_ms: u64,
    /// Bounded cumulative software revision; duplicates do not advance it.
    pub update_sequence: u32,
    /// Distinct observed provider-internal retry reports, never Threadlane task attempts.
    pub retry_count: u32,
    /// Immutable first text-pattern observation.
    pub first_failure: Option<ProviderHealthObservation>,
    /// Most recent text-pattern observation.
    pub last_failure: Option<ProviderHealthObservation>,
    /// Current advisory pattern, not an assertion of current provider failure.
    pub current_failure: Option<ProviderHealthCategory>,
    /// Explicit advisory closure, never inferred from quiet output.
    pub recovery: Option<ProviderHealthRecovery>,
}

impl ProviderHealthSnapshot {
    /// Checks bounded shape and internal consistency, without granting source authority.
    pub fn is_valid(&self) -> bool {
        let coverage_valid = match self.availability {
            ProviderDiagnosticAvailability::Available => self.unavailable_reason.is_none(),
            ProviderDiagnosticAvailability::Unavailable => {
                self.unavailable_reason.is_some()
                    && self.unavailable_reason
                        != Some(ProviderDiagnosticUnavailableReason::ResourceLimit)
            }
            ProviderDiagnosticAvailability::Truncated => {
                self.unavailable_reason == Some(ProviderDiagnosticUnavailableReason::ResourceLimit)
            }
        };
        if self.schema_version != 1
            || !coverage_valid
            || self.elapsed_ms > MAX_PROVIDER_HEALTH_ELAPSED_MS
            || self.update_sequence > MAX_PROVIDER_HEALTH_UPDATES
            || self.retry_count > MAX_PROVIDER_HEALTH_RECORDS
        {
            return false;
        }
        match (self.first_failure, self.last_failure) {
            (None, None) => {
                self.retry_count == 0 && self.current_failure.is_none() && self.recovery.is_none()
            }
            (Some(first), Some(last)) => {
                self.retry_count > 0
                    && first.update_sequence > 0
                    && first.update_sequence <= last.update_sequence
                    && last.update_sequence <= self.update_sequence
                    && self.retry_count <= last.update_sequence - first.update_sequence + 1
                    && first.elapsed_ms <= last.elapsed_ms
                    && last.elapsed_ms <= self.elapsed_ms
                    && ((self.retry_count == 1 && first == last)
                        || (self.retry_count > 1 && first.update_sequence < last.update_sequence))
                    && match self.current_failure {
                        Some(category) => category == last.category && self.recovery.is_none(),
                        None => self.recovery.is_some(),
                    }
            }
            _ => false,
        }
    }

    /// Rejects stale revisions and changed first observations during cumulative conversion.
    ///
    /// Exact duplicates are idempotent. Callers must compare within the same verified scope.
    pub fn is_valid_successor_of(&self, previous: &Self) -> bool {
        self.is_valid()
            && previous.is_valid()
            && (self == previous
                || (self.update_sequence > previous.update_sequence
                    && self.elapsed_ms >= previous.elapsed_ms
                    && self.retry_count >= previous.retry_count
                    && (previous.first_failure.is_none()
                        || self.first_failure == previous.first_failure)
                    && (self.retry_count > previous.retry_count
                        || self.last_failure == previous.last_failure)))
    }
}

/// Decodes and validates the provider-independent allowlist at an explicit core boundary.
///
/// This does not persist anything, authenticate a source, create a notice, or change execution.
/// The host must establish verified owner and exact scope before calling its durable APIs.
pub fn decode_provider_health_snapshot(
    bytes: &[u8],
) -> Result<ProviderHealthSnapshot, crate::CoreError> {
    let invalid = || crate::CoreError::Validation {
        field: "provider_health",
        reason: "invalid bounded unverified diagnostic snapshot".to_string(),
    };
    if bytes.len() > MAX_PROVIDER_HEALTH_SNAPSHOT_BYTES {
        return Err(invalid());
    }
    let snapshot: ProviderHealthSnapshot = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if !snapshot.is_valid() {
        return Err(invalid());
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_boundary_rejects_provenance_claims_arbitrary_fields_and_bad_history() {
        let raw = br#"{"schema_version":1,"evidence_class":"unverified_provider_diagnostic_hint","availability":"unavailable","unavailable_reason":"unsupported_provider","elapsed_ms":0,"update_sequence":0,"retry_count":0,"first_failure":null,"last_failure":null,"current_failure":null,"recovery":null}"#;
        let initial = decode_provider_health_snapshot(raw).unwrap();
        let mut bad = serde_json::to_value(&initial).unwrap();
        bad["raw_log"] = serde_json::json!("PRIVATE");
        assert!(decode_provider_health_snapshot(&serde_json::to_vec(&bad).unwrap()).is_err());
        bad.as_object_mut().unwrap().remove("raw_log");
        bad["evidence_class"] = serde_json::json!("verified_provider_transport_failure");
        assert!(decode_provider_health_snapshot(&serde_json::to_vec(&bad).unwrap()).is_err());
        bad = serde_json::to_value(&initial).unwrap();
        bad["retry_count"] = serde_json::json!(1);
        assert!(decode_provider_health_snapshot(&serde_json::to_vec(&bad).unwrap()).is_err());
        assert!(initial.is_valid_successor_of(&initial));
        assert!(
            decode_provider_health_snapshot(&[b' '; MAX_PROVIDER_HEALTH_SNAPSHOT_BYTES + 1])
                .is_err()
        );
    }
}
