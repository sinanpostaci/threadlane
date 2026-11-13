//! Runner-origin owned-process presence observations, independent of provider output.
//!
//! The worker supervisor produces these snapshots for the child it actually spawned. They are
//! never parsed from provider stdout, stderr, tool messages, progress events, or model names.
//! A watch update is advisory evidence only: durable use additionally requires an independently
//! verified invocation owner plus exact claim/host/binding correlation and a freshness check.
//!
//! `Preparing` is published before a child exists and never means live. `Running` is published
//! only after the supervisor obtained a real child and validated its Unix process group, and is
//! refreshed from the supervision loop while no exit has been confirmed. `Stopped` confirms only
//! that the immediate child was observed exited/reaped; it makes no descendant-cleanup, result,
//! acceptance, or work-success claim. A canonical invocation UUID scopes revisions, and successor
//! checks pin the established child/PGID/Linux-start identity and heartbeat interval. No
//! owned-process observation is added to advisory APIs.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Finite wire version of [`OwnedProcessSnapshot`].
pub const OWNED_PROCESS_SCHEMA_VERSION: u8 = 1;
/// Default bounded presence heartbeat interval; supervision never refreshes faster than this.
pub const DEFAULT_OWNED_PROCESS_HEARTBEAT_MS: u64 = 1_000;
/// Minimum admitted heartbeat interval, kept at one second or longer.
pub const MIN_OWNED_PROCESS_HEARTBEAT_MS: u64 = 1_000;
/// Largest exactly representable JSON integer for cumulative update sequences.
pub const MAX_OWNED_PROCESS_SEQUENCE: u64 = 9_007_199_254_740_991;
/// Largest exactly representable JSON integer for monotonic invocation elapsed milliseconds.
pub const MAX_OWNED_PROCESS_ELAPSED_MS: u64 = 9_007_199_254_740_991;

/// Explicit provenance of a presence observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedProcessOrigin {
    /// Produced by the worker supervisor for its own child, never parsed from provider output.
    RunnerOwnedChild,
}

/// Lifecycle of the runner-owned worker child, independent of provider evidence or acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedProcessState {
    /// Invocation preparation is in progress and no owned child exists yet.
    Preparing,
    /// A real owned child was spawned and its process-group ownership was validated.
    Running,
    /// The immediate child was confirmed exited/reaped; no result-acceptance claim is made.
    Stopped,
    /// Ownership, reaping, reading, or the observing future became uncertain.
    Unknown,
}

/// Finite reason for an [`OwnedProcessState::Unknown`] observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedProcessUnknownReason {
    /// The invocation ended before a child was spawned, so no owned process existed.
    NoOwnedChild,
    /// A child exists, but process-group or pipe ownership checks did not complete.
    OwnershipUnconfirmed,
    /// The immediate child's exit/reap could not be confirmed.
    ExitUnconfirmed,
    /// The publisher was dropped before a confirmed terminal state; cleanup is uncertain.
    PublisherDropped,
}

/// Bounded runner-origin presence snapshot with no provider prose, argv, path, or secret.
///
/// The unique invocation ID and monotonic sequence distinguish separate invocations and stale
/// revisions. On Linux, `linux_process_start_ticks` is a best-effort `/proc/<pid>/stat` start-time
/// identity that helps distinguish PID reuse; it is `None` when unavailable or on other platforms
/// and is not a portable process-identity claim. A watch update alone grants no durable authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedProcessSnapshot {
    /// Finite wire version.
    pub schema_version: u8,
    /// Explicit runner-side provenance.
    pub origin: OwnedProcessOrigin,
    /// Software-generated canonical lower-case UUID identity for this invocation observation.
    pub invocation_id: String,
    /// Current lifecycle state.
    pub state: OwnedProcessState,
    /// Fixed reason present only when `state == Unknown`.
    pub unknown_reason: Option<OwnedProcessUnknownReason>,
    /// Monotonically increasing software update sequence within this invocation.
    pub sequence: u64,
    /// Monotonic elapsed invocation milliseconds, bounded for JSON exactness.
    pub elapsed_ms: u64,
    /// Bounded heartbeat interval; consumers use their own clock for freshness decisions.
    pub heartbeat_interval_ms: u64,
    /// Actual child PID, present only after the supervisor spawned and claimed the child.
    pub child_pid: Option<u32>,
    /// Owned Unix process group ID validated by the supervisor after spawn.
    pub process_group_id: Option<i32>,
    /// Best-effort Linux `/proc/<pid>/stat` start-time ticks for this spawned child.
    pub linux_process_start_ticks: Option<u64>,
}

impl OwnedProcessSnapshot {
    /// Returns `true` only for a validated [`OwnedProcessState::Running`] observation.
    pub fn is_live(&self) -> bool {
        self.state == OwnedProcessState::Running && self.is_valid()
    }

    /// Checks bounded shape and internal consistency without granting ownership or freshness.
    ///
    /// `Running` and `Stopped` require a concrete child PID greater than 1 plus the matching
    /// process-group ID. A snapshot that lost that identity is invalid and never live.
    pub fn is_valid(&self) -> bool {
        if self.schema_version != OWNED_PROCESS_SCHEMA_VERSION
            || self.origin != OwnedProcessOrigin::RunnerOwnedChild
            || !is_valid_invocation_id(&self.invocation_id)
            || self.sequence == 0
            || self.sequence > MAX_OWNED_PROCESS_SEQUENCE
            || self.elapsed_ms > MAX_OWNED_PROCESS_ELAPSED_MS
            || self.heartbeat_interval_ms < MIN_OWNED_PROCESS_HEARTBEAT_MS
            || self.heartbeat_interval_ms > MAX_OWNED_PROCESS_ELAPSED_MS
        {
            return false;
        }
        let concrete_identity = self.concrete_identity().is_some();
        let absent_identity = self.has_absent_identity();
        match self.state {
            OwnedProcessState::Preparing => self.unknown_reason.is_none() && absent_identity,
            OwnedProcessState::Running | OwnedProcessState::Stopped => {
                self.unknown_reason.is_none() && concrete_identity
            }
            OwnedProcessState::Unknown => {
                self.unknown_reason.is_some() && (absent_identity || concrete_identity)
            }
        }
    }

    /// Rejects stale revisions, lifecycle regressions, and identity mutation within one invocation.
    ///
    /// Once a concrete child/PGID/Linux-start identity is established, every later revision must
    /// preserve it exactly, including an absent optional Linux start value. Heartbeat interval,
    /// invocation ID, and terminal no-identity observations are pinned the same way. A coalesced
    /// `Preparing -> Running/Stopped/Unknown` observation may establish the identity it skipped.
    pub fn is_valid_successor_of(&self, previous: &Self) -> bool {
        if self.invocation_id != previous.invocation_id
            || !self.is_valid()
            || !previous.is_valid()
            || self.sequence <= previous.sequence
            || self.elapsed_ms < previous.elapsed_ms
            || self.heartbeat_interval_ms != previous.heartbeat_interval_ms
        {
            return false;
        }
        if !matches!(
            (previous.state, self.state),
            (
                OwnedProcessState::Preparing,
                OwnedProcessState::Running
                    | OwnedProcessState::Stopped
                    | OwnedProcessState::Unknown
            ) | (
                OwnedProcessState::Running,
                OwnedProcessState::Running
                    | OwnedProcessState::Stopped
                    | OwnedProcessState::Unknown
            ) | (OwnedProcessState::Stopped, OwnedProcessState::Stopped)
                | (OwnedProcessState::Unknown, OwnedProcessState::Unknown)
        ) {
            return false;
        }
        match previous.concrete_identity() {
            Some(identity) => self.concrete_identity() == Some(identity),
            None => {
                previous.state == OwnedProcessState::Preparing || self.concrete_identity().is_none()
            }
        }
    }

    fn has_absent_identity(&self) -> bool {
        self.child_pid.is_none()
            && self.process_group_id.is_none()
            && self.linux_process_start_ticks.is_none()
    }

    fn concrete_identity(&self) -> Option<(u32, i32, Option<u64>)> {
        match (self.child_pid, self.process_group_id) {
            (Some(child_pid), Some(process_group_id))
                if child_pid > 1
                    && i32::try_from(child_pid).is_ok_and(|pid| pid == process_group_id) =>
            {
                Some((child_pid, process_group_id, self.linux_process_start_ticks))
            }
            _ => None,
        }
    }
}

fn is_valid_invocation_id(value: &str) -> bool {
    value.len() <= 64
        && uuid::Uuid::parse_str(value).is_ok_and(|parsed| parsed.hyphenated().to_string() == value)
}

/// Coalescing watch observer for runner-origin owned-process presence.
///
/// Use one channel per invocation. Closure of the receiver is nonblocking and never changes the
/// runner outcome. Watch updates coalesce and are not durable authority on their own.
pub type OwnedProcessObserver = watch::Sender<Option<OwnedProcessSnapshot>>;

/// Best-effort Linux start-time ticks from `/proc/<pid>/stat` field 22.
#[cfg(target_os = "linux")]
pub(crate) fn linux_process_start_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = stat.rsplit_once(')')?.1;
    // The remainder begins at field 3 (state); starttime is field 22, index 19 here.
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// Process start identity is not read or claimed on unsupported platforms.
#[cfg(not(target_os = "linux"))]
pub(crate) fn linux_process_start_ticks(_pid: u32) -> Option<u64> {
    None
}

/// Invocation-owned publisher; dropping it before a terminal state publishes `Unknown`.
pub(crate) struct OwnedProcessPublisher {
    snapshot: OwnedProcessSnapshot,
    observer: Option<OwnedProcessObserver>,
    start_instant: Instant,
    child_created: bool,
    terminal: bool,
}

impl OwnedProcessPublisher {
    pub(crate) fn new(observer: Option<OwnedProcessObserver>, start_instant: Instant) -> Self {
        let mut publisher = Self {
            snapshot: OwnedProcessSnapshot {
                schema_version: OWNED_PROCESS_SCHEMA_VERSION,
                origin: OwnedProcessOrigin::RunnerOwnedChild,
                invocation_id: uuid::Uuid::new_v4().to_string(),
                state: OwnedProcessState::Preparing,
                unknown_reason: None,
                sequence: 0,
                elapsed_ms: 0,
                heartbeat_interval_ms: DEFAULT_OWNED_PROCESS_HEARTBEAT_MS,
                child_pid: None,
                process_group_id: None,
                linux_process_start_ticks: None,
            },
            observer,
            start_instant,
            child_created: false,
            terminal: false,
        };
        publisher.publish();
        publisher
    }

    pub(crate) fn note_child_created(&mut self) {
        self.child_created = true;
    }

    pub(crate) fn publish_running(&mut self, child_pid: u32, process_group_id: i32) {
        if self.terminal || self.snapshot.state != OwnedProcessState::Preparing {
            return;
        }
        self.snapshot.state = OwnedProcessState::Running;
        self.snapshot.unknown_reason = None;
        self.snapshot.child_pid = Some(child_pid);
        self.snapshot.process_group_id = Some(process_group_id);
        self.snapshot.linux_process_start_ticks = linux_process_start_ticks(child_pid);
        self.publish();
    }

    pub(crate) fn heartbeat(&mut self) {
        if self.terminal || self.snapshot.state != OwnedProcessState::Running {
            return;
        }
        self.publish();
    }

    pub(crate) fn publish_stopped(&mut self) {
        if self.terminal || self.snapshot.state != OwnedProcessState::Running {
            return;
        }
        self.snapshot.state = OwnedProcessState::Stopped;
        self.snapshot.unknown_reason = None;
        self.publish();
        self.terminal = true;
    }

    pub(crate) fn publish_unknown(&mut self, reason: OwnedProcessUnknownReason) {
        if self.terminal {
            return;
        }
        self.snapshot.state = OwnedProcessState::Unknown;
        self.snapshot.unknown_reason = Some(reason);
        self.publish();
        self.terminal = true;
    }

    fn publish(&mut self) {
        self.snapshot.elapsed_ms = self
            .snapshot
            .elapsed_ms
            .max(elapsed_ms(self.start_instant))
            .min(MAX_OWNED_PROCESS_ELAPSED_MS);
        self.snapshot.sequence = self
            .snapshot
            .sequence
            .saturating_add(1)
            .min(MAX_OWNED_PROCESS_SEQUENCE);
        if let Some(observer) = &self.observer {
            // Watch replacement is nonblocking; a closed receiver cannot change the run outcome.
            observer.send_replace(Some(self.snapshot.clone()));
        }
    }
}

impl Drop for OwnedProcessPublisher {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        let reason = if self.child_created {
            OwnedProcessUnknownReason::PublisherDropped
        } else {
            OwnedProcessUnknownReason::NoOwnedChild
        };
        self.publish_unknown(reason);
    }
}

fn elapsed_ms(start_instant: Instant) -> u64 {
    u64::try_from(start_instant.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .min(MAX_OWNED_PROCESS_ELAPSED_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_pid_metadata_still_has_distinct_invocation_identity() {
        let repeated_pid = std::process::id();
        let repeated_pgid = i32::try_from(repeated_pid).expect("test PID fits i32");

        let first_running = {
            let (observer, receiver) = watch::channel(None);
            let mut first = OwnedProcessPublisher::new(Some(observer), Instant::now());
            first.note_child_created();
            first.publish_running(repeated_pid, repeated_pgid);
            let snapshot = receiver.borrow().clone().expect("first running");
            drop(first);
            snapshot
        };
        let second_running = {
            let (observer, receiver) = watch::channel(None);
            let mut second = OwnedProcessPublisher::new(Some(observer), Instant::now());
            second.note_child_created();
            second.publish_running(repeated_pid, repeated_pgid);
            let snapshot = receiver.borrow().clone().expect("second running");
            snapshot
        };

        assert_eq!(first_running.child_pid, second_running.child_pid);
        assert_eq!(
            first_running.process_group_id,
            second_running.process_group_id
        );
        assert_eq!(
            first_running.linux_process_start_ticks,
            second_running.linux_process_start_ticks
        );
        assert_ne!(first_running.invocation_id, second_running.invocation_id);
        assert!(first_running.is_valid());
        assert!(second_running.is_valid());
        assert!(first_running.is_live());
        assert!(!first_running.is_valid_successor_of(&second_running));
    }

    #[test]
    fn transitions_are_monotonic_and_drop_is_unknown_never_live() {
        let (observer, receiver) = watch::channel(None);
        let mut publisher = OwnedProcessPublisher::new(Some(observer), Instant::now());
        let preparing = receiver.borrow().clone().expect("preparing");
        assert_eq!(preparing.state, OwnedProcessState::Preparing);
        assert!(!preparing.is_live());

        publisher.note_child_created();
        publisher.publish_running(1234, 1234);
        let running = receiver.borrow().clone().expect("running");
        assert!(running.is_live());
        assert!(running.is_valid_successor_of(&preparing));
        publisher.heartbeat();
        let heartbeat = receiver.borrow().clone().expect("heartbeat");
        assert!(heartbeat.sequence > running.sequence);
        assert!(heartbeat.is_valid_successor_of(&running));

        drop(publisher);
        let unknown = receiver.borrow().clone().expect("unknown");
        assert_eq!(unknown.state, OwnedProcessState::Unknown);
        assert_eq!(
            unknown.unknown_reason,
            Some(OwnedProcessUnknownReason::PublisherDropped)
        );
        assert!(!unknown.is_live());
        assert!(unknown.is_valid_successor_of(&heartbeat));
    }

    #[test]
    fn stopped_is_terminal_and_later_drop_or_heartbeat_does_not_replace_it() {
        let (observer, receiver) = watch::channel(None);
        let mut publisher = OwnedProcessPublisher::new(Some(observer), Instant::now());
        publisher.note_child_created();
        publisher.publish_running(1234, 1234);
        publisher.publish_stopped();
        let stopped = receiver.borrow().clone().expect("stopped");
        assert_eq!(stopped.state, OwnedProcessState::Stopped);
        publisher.heartbeat();
        assert_eq!(receiver.borrow().as_ref(), Some(&stopped));
        drop(publisher);
        assert_eq!(receiver.borrow().as_ref(), Some(&stopped));
        assert!(!stopped.is_live());
    }

    #[test]
    fn unsupported_pre_spawn_drop_is_unknown_without_partial_identity() {
        let (observer, receiver) = watch::channel(None);
        let publisher = OwnedProcessPublisher::new(Some(observer), Instant::now());
        drop(publisher);
        let unknown = receiver.borrow().clone().expect("unknown");
        assert_eq!(unknown.state, OwnedProcessState::Unknown);
        assert_eq!(
            unknown.unknown_reason,
            Some(OwnedProcessUnknownReason::NoOwnedChild)
        );
        assert!(!unknown.is_live());
        assert!(unknown.child_pid.is_none());
        assert!(unknown.process_group_id.is_none());
        assert!(unknown.is_valid());
    }

    #[test]
    fn validation_rejects_unbounded_or_inconsistent_snapshots() {
        let (observer, receiver) = watch::channel(None);
        let mut publisher = OwnedProcessPublisher::new(Some(observer), Instant::now());
        publisher.note_child_created();
        publisher.publish_running(1234, 1234);
        let running = receiver.borrow().clone().expect("running");

        let mut oversized = running.clone();
        oversized.elapsed_ms = MAX_OWNED_PROCESS_ELAPSED_MS + 1;
        assert!(!oversized.is_valid());
        let mut zero_sequence = running.clone();
        zero_sequence.sequence = 0;
        assert!(!zero_sequence.is_valid());
        let mut preparing_live = running.clone();
        preparing_live.state = OwnedProcessState::Preparing;
        assert!(!preparing_live.is_valid());
        let mut unknown_without_reason = running;
        unknown_without_reason.state = OwnedProcessState::Unknown;
        assert!(!unknown_without_reason.is_valid());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_start_identity_is_cleanly_available_when_proc_is_mounted() {
        if !std::path::Path::new("/proc/self/stat").exists() {
            return;
        }
        let pid = std::process::id();
        let first = linux_process_start_ticks(pid).expect("Linux start-time ticks");
        let second = linux_process_start_ticks(pid).expect("stable start-time ticks");
        assert!(first > 0);
        assert_eq!(first, second);
    }
}
