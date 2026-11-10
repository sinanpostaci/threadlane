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
    /// Maximum attempt ordinal reported by matching text, independent of observed record count.
    /// Unknown in legacy snapshots; this is never a Threadlane dispatch attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_high_water: Option<u32>,
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
            || self.retry_high_water.is_some_and(|count| {
                count == 0 || count > MAX_PROVIDER_HEALTH_RECORDS || self.retry_count == 0
            })
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
                    && match (previous.retry_high_water, self.retry_high_water) {
                        (Some(prior), Some(next)) => {
                            next >= prior
                                && (next == prior || self.retry_count > previous.retry_count)
                        }
                        (Some(_), None) => false,
                        _ => true,
                    }
                    && (previous.first_failure.is_none()
                        || self.first_failure == previous.first_failure)
                    && (self.retry_count > previous.retry_count
                        || self.last_failure == previous.last_failure)))
    }
}

use tokio::sync::watch;

/// Lossless bounded cumulative health observer; each invocation needs its own channel.
///
/// A full stdout observer cannot drop the first/last hint in this channel. Consumers clone the
/// snapshot before awaiting persistence, then arrange their own one-notice durable deduplication.
pub type ProviderHealthObserver = watch::Sender<Option<ProviderHealthSnapshot>>;

pub(crate) fn initial_snapshot(supported: bool) -> ProviderHealthSnapshot {
    ProviderHealthSnapshot {
        schema_version: 1,
        evidence_class: ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint,
        availability: ProviderDiagnosticAvailability::Unavailable,
        unavailable_reason: Some(if supported {
            ProviderDiagnosticUnavailableReason::CaptureSetupFailed
        } else {
            ProviderDiagnosticUnavailableReason::UnsupportedProvider
        }),
        elapsed_ms: 0,
        update_sequence: 0,
        retry_count: 0,
        retry_high_water: None,
        first_failure: None,
        last_failure: None,
        current_failure: None,
        recovery: None,
    }
}

pub(crate) fn set_coverage(
    snapshot: &mut ProviderHealthSnapshot,
    availability: ProviderDiagnosticAvailability,
    reason: Option<ProviderDiagnosticUnavailableReason>,
    elapsed_ms: u64,
) {
    if snapshot.availability == availability && snapshot.unavailable_reason == reason {
        return;
    }
    snapshot.availability = availability;
    snapshot.unavailable_reason = reason;
    snapshot.elapsed_ms = elapsed_ms
        .max(snapshot.elapsed_ms)
        .min(MAX_PROVIDER_HEALTH_ELAPSED_MS);
    snapshot.update_sequence = (snapshot.update_sequence + 1).min(MAX_PROVIDER_HEALTH_UPDATES);
}

#[derive(Clone)]
pub(crate) struct HealthPublisher {
    latest: watch::Sender<ProviderHealthSnapshot>,
    observer: Option<ProviderHealthObserver>,
}

impl HealthPublisher {
    pub(crate) fn new(supported: bool, observer: Option<ProviderHealthObserver>) -> Self {
        let snapshot = initial_snapshot(supported);
        let (latest, _) = watch::channel(snapshot.clone());
        let this = Self { latest, observer };
        if let Some(observer) = &this.observer {
            observer.send_replace(Some(snapshot));
        }
        this
    }

    pub(crate) fn publish(&self, snapshot: ProviderHealthSnapshot) {
        debug_assert!(snapshot.is_valid());
        if !snapshot.is_valid() {
            return;
        }
        let changed = self.latest.send_if_modified(|previous| {
            if *previous == snapshot || !snapshot.is_valid_successor_of(previous) {
                return false;
            }
            *previous = snapshot.clone();
            true
        });
        if changed {
            if let Some(observer) = &self.observer {
                observer.send_if_modified(|previous| {
                    if previous
                        .as_ref()
                        .is_some_and(|p| !snapshot.is_valid_successor_of(p))
                    {
                        return false;
                    }
                    *previous = Some(snapshot.clone());
                    true
                });
            }
        }
    }

    pub(crate) fn snapshot(&self) -> ProviderHealthSnapshot {
        self.latest.borrow().clone()
    }

    pub(crate) fn finish(
        &self,
        elapsed_ms: u64,
        verified_completion: bool,
    ) -> ProviderHealthSnapshot {
        let previous = self.snapshot();
        let mut snapshot = previous.clone();
        if verified_completion && snapshot.current_failure.is_some() {
            snapshot.current_failure = None;
            snapshot.recovery = Some(ProviderHealthRecovery::VerifiedTerminalOutcome);
        }
        snapshot.elapsed_ms = elapsed_ms
            .max(snapshot.elapsed_ms)
            .min(MAX_PROVIDER_HEALTH_ELAPSED_MS);
        if snapshot != previous {
            snapshot.update_sequence =
                (snapshot.update_sequence + 1).min(MAX_PROVIDER_HEALTH_UPDATES);
        }
        self.publish(snapshot);
        self.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::antigravity_diagnostics::AntigravityDiagnosticParser;

    #[test]
    fn watch_coalesces_without_receiving_and_terminal_closure_keeps_immutable_hints() {
        let (observer, receiver) = watch::channel(None);
        let publisher = HealthPublisher::new(true, Some(observer));
        let mut parser = AntigravityDiagnosticParser::new();
        publisher.publish(parser.snapshot());
        let mut offset = 0;
        for attempt in 1..=5 {
            let line = format!("I0101 00:00:01.000000 1 run.go:315] Run: attempt {attempt} failed (request failed: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s\n");
            parser.ingest_at(offset, line.as_bytes(), attempt * 300_000);
            offset += line.len() as u64;
            publisher.publish(parser.snapshot());
        }
        let before = publisher.snapshot();
        assert_eq!(receiver.borrow().as_ref().unwrap().retry_count, 5);
        publisher.publish(initial_snapshot(true));
        assert_eq!(publisher.snapshot(), before);
        let after = publisher.finish(1_600_000, true);
        assert_eq!(after.first_failure, before.first_failure);
        assert_eq!(after.last_failure, before.last_failure);
        assert_eq!(after.retry_count, 5);
        assert!(after.current_failure.is_none());
        assert_eq!(
            after.recovery,
            Some(ProviderHealthRecovery::VerifiedTerminalOutcome)
        );
        assert!(after.is_valid_successor_of(&before));
        assert_eq!(receiver.borrow().as_ref(), Some(&after));
        drop(receiver);
        assert!(publisher.finish(1_600_001, true).is_valid());
    }

    #[test]
    fn unsupported_and_closed_quiet_sources_never_assert_health_or_recovery() {
        let publisher = HealthPublisher::new(false, None);
        let final_snapshot = publisher.finish(100, true);
        assert_eq!(
            final_snapshot.unavailable_reason,
            Some(ProviderDiagnosticUnavailableReason::UnsupportedProvider)
        );
        assert!(final_snapshot.current_failure.is_none());
        assert!(final_snapshot.recovery.is_none());
        assert!(final_snapshot.is_valid());
    }
}
