//! Ephemeral, claim-scoped worker process presence snapshots.
//!
//! Presence is derived from an in-memory runner-owned process observation channel and
//! independent Linux `/proc` verification. It is never persisted in the durable worker
//! record, never mutates scheduling state, and never creates a human request.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use threadlane_core::{WorkerClaim, WorkerExecutionBinding, WorkerSnapshot, WorkerStatus};
use threadlane_workers::{OwnedProcessSnapshot, OwnedProcessState};
use tokio::sync::watch;

use crate::protocol::HostControlConfig;

/// Schema version of the finite [`WorkerPresenceSnapshot`] wire DTO.
pub const WORKER_PRESENCE_SCHEMA_VERSION: u8 = 1;

/// Upper bound for a fresh process sample, independent of a runner-provided interval.
pub const WORKER_PRESENCE_MAX_SAMPLE_AGE_MS: u64 = 5_000;

/// A fresh sample may be at most this many heartbeat intervals old.
const WORKER_PRESENCE_HEARTBEATS: u64 = 3;

/// Bound for the single `/proc/<pid>/stat` read used by physical verification.
const MAX_PROC_STAT_BYTES: u64 = 4 * 1024;

/// Availability of an ephemeral worker process observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPresenceAvailability {
    /// A current runner sample and independent Linux process identity prove a live process.
    Live,
    /// The worker is terminal and no fresh running process is claimed.
    NonLive,
    /// Presence is unknown, unverified, stale, or unavailable; never a human request.
    Unavailable,
}

/// Freshness of the underlying process sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPresenceFreshness {
    /// The sample is within the bounded heartbeat age.
    Fresh,
    /// A running sample is older than the bounded age.
    Stale,
    /// No fresh running sample applies.
    NotApplicable,
}

/// Explicit reason why worker presence is unavailable or unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPresenceUnavailableReason {
    /// No active in-memory claim owns the requested worker run.
    InactiveClaim,
    /// The runner reported preparation before an owned child process was spawned.
    AwaitingProcessLaunch,
    /// No process observation has arrived for this claim.
    NoObservation,
    /// The runner observation channel closed before a terminal sample.
    ObserverClosed,
    /// A sample failed the producer's typed validity or successor invariants.
    InvalidObservation,
    /// A running sample exceeded the bounded heartbeat age.
    StaleObservation,
    /// The active claim does not match the exact scoped worker snapshot.
    ClaimMismatch,
    /// The observed child PID no longer exists.
    ProcessMissing,
    /// The observed child or process group is dead.
    ProcessExited,
    /// The observed child is a zombie or otherwise dead task.
    ProcessZombie,
    /// The Linux process start identity no longer matches the runner sample.
    ProcessIdentityMismatch,
    /// The Linux process group identity no longer matches the runner sample.
    ProcessGroupMismatch,
    /// The bounded Linux process inspection could not be completed.
    ProcessInspectionFailed,
    /// Physical process verification is not implemented on this platform.
    UnsupportedPlatform,
    /// The durable worker state is `Unknown`; it cannot be revived by presence.
    PersistedUnknown,
    /// The supervisor is stopping, so no fresh running presence is reported.
    SupervisorStopping,
    /// The supervisor retired or faulted, so no fresh running presence is reported.
    SupervisorRetired,
    /// A required supervisor state could not be read for this reply.
    SupervisorUnavailable,
    /// Presence changed while independent process inspection was in progress.
    ObservationChanged,
    /// The runner reported an unknown process state.
    RunnerUnknown,
}

/// Finite, authenticated, binding-scoped worker presence response.
///
/// Contains no filesystem paths, raw provider output, argv, environment, prompts, or secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerPresenceSnapshot {
    /// Schema version of this presence DTO.
    pub schema_version: u8,
    /// Owning binding identifier.
    pub binding_id: String,
    /// Target run identifier.
    pub run_id: String,
    /// Target worker identifier.
    pub worker_id: String,
    /// Current durable worker revision at the time of the correlated read.
    pub worker_revision: u64,
    /// Current durable worker status at the time of the correlated read.
    pub worker_status: WorkerStatus,
    /// Active or most recent dispatcher claim identifier.
    pub claim_id: Option<String>,
    /// Claimed 1-based attempt number.
    pub attempt_count: u32,
    /// Trusted profile identifier fixed at admission.
    pub profile_id: String,
    /// Immutable digest of the trusted execution profile.
    pub profile_digest: String,
    /// Optional durable dependency execution identity.
    pub execution_binding: Option<WorkerExecutionBinding>,
    /// Live, non-live, or unavailable availability.
    pub availability: WorkerPresenceAvailability,
    /// Freshness of the observed process sample.
    pub freshness: WorkerPresenceFreshness,
    /// Convenience boolean that is true only for a fresh, physically verified running process.
    pub live: bool,
    /// Supervisor wall-clock timestamp when the current sample sequence was observed.
    pub sampled_at_ms: Option<i64>,
    /// Monotonically measured age of the current sample sequence.
    pub sample_age_ms: Option<u64>,
    /// Bounded maximum age applied to the current sample.
    pub max_sample_age_ms: u64,
    /// Server-computed expiry timestamp for the current sample.
    pub expires_at_ms: Option<i64>,
    /// Optional validated runner-owned process observation.
    pub process: Option<OwnedProcessSnapshot>,
    /// Optional exact host-control owner tuple and instance installed on this supervisor.
    pub owner: Option<HostControlConfig>,
    /// Explicit unavailable or unknown reason when no fresh running evidence exists.
    pub unavailable_reason: Option<WorkerPresenceUnavailableReason>,
}

#[derive(Debug, Clone)]
pub(crate) struct PresenceCacheRead {
    snapshot: Option<OwnedProcessSnapshot>,
    sampled_at: Option<Instant>,
    sampled_at_ms: Option<i64>,
    closed: bool,
    invalid: bool,
}

#[derive(Debug)]
struct PresenceCacheInner {
    receiver: Option<watch::Receiver<Option<OwnedProcessSnapshot>>>,
    close_probe: watch::Receiver<Option<OwnedProcessSnapshot>>,
    snapshot: Option<OwnedProcessSnapshot>,
    sampled_at: Option<Instant>,
    sampled_at_ms: Option<i64>,
    closed: bool,
    invalid: bool,
}

/// Private per-claim observation cache. The receiver is never exposed outside this module.
#[derive(Debug)]
pub(crate) struct WorkerPresenceCache {
    inner: Mutex<PresenceCacheInner>,
}

impl WorkerPresenceCache {
    pub(crate) fn new(receiver: watch::Receiver<Option<OwnedProcessSnapshot>>) -> Self {
        let close_probe = receiver.clone();
        Self {
            inner: Mutex::new(PresenceCacheInner {
                receiver: Some(receiver),
                close_probe,
                snapshot: None,
                sampled_at: None,
                sampled_at_ms: None,
                closed: false,
                invalid: false,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PresenceCacheInner> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn take_receiver(&self) -> Option<watch::Receiver<Option<OwnedProcessSnapshot>>> {
        self.lock().receiver.take()
    }

    /// Drains the runner-owned watch channel until the sender closes.
    ///
    /// Only a changed sequence refreshes the sample timestamp. Repeated reads or repeated
    /// sends of the identical sequence never make an old sample look fresh.
    pub(crate) async fn watch_changes(self: Arc<Self>) {
        let Some(mut receiver) = self.take_receiver() else {
            self.mark_closed();
            return;
        };
        loop {
            match receiver.changed().await {
                Ok(()) => {
                    let incoming = {
                        let borrowed = receiver.borrow_and_update();
                        borrowed.clone()
                    };
                    self.record(incoming);
                }
                Err(_) => {
                    self.mark_closed();
                    break;
                }
            }
        }
    }

    fn record(&self, incoming: Option<OwnedProcessSnapshot>) {
        let mut inner = self.lock();
        if inner.closed {
            return;
        }
        let Some(incoming) = incoming else {
            if inner.snapshot.is_some() {
                inner.invalid = true;
            }
            return;
        };
        if !incoming.is_valid() {
            inner.invalid = true;
            inner.snapshot = Some(incoming);
            inner.sampled_at = None;
            inner.sampled_at_ms = None;
            return;
        }
        if let Some(previous) = inner.snapshot.as_ref() {
            if incoming.sequence == previous.sequence {
                if incoming != *previous {
                    inner.invalid = true;
                }
                return;
            }
            if incoming.sequence < previous.sequence || !incoming.is_valid_successor_of(previous) {
                inner.invalid = true;
                return;
            }
        }
        inner.snapshot = Some(incoming);
        inner.sampled_at = Some(Instant::now());
        inner.sampled_at_ms = Some(unix_time_ms());
    }

    fn mark_closed(&self) {
        self.lock().closed = true;
    }

    pub(crate) fn read(&self) -> PresenceCacheRead {
        let mut inner = self.lock();
        if !inner.closed {
            match inner.close_probe.has_changed() {
                Err(_) => inner.closed = true,
                Ok(true) => {
                    inner.close_probe.borrow_and_update();
                    if inner.close_probe.has_changed().is_err() {
                        inner.closed = true;
                    }
                }
                Ok(false) => {}
            }
        }
        PresenceCacheRead {
            snapshot: inner.snapshot.clone(),
            sampled_at: inner.sampled_at,
            sampled_at_ms: inner.sampled_at_ms,
            closed: inner.closed,
            invalid: inner.invalid,
        }
    }
}

#[derive(Debug)]
pub(crate) enum PresenceEvaluation {
    Final(WorkerPresenceSnapshot),
    RequiresPhysicalCheck(WorkerPresenceSnapshot, OwnedProcessSnapshot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessVerification {
    Running,
    Missing,
    Exited,
    Zombie,
    IdentityMismatch,
    GroupMismatch,
    InspectionFailed,
    #[cfg(not(target_os = "linux"))]
    UnsupportedPlatform,
}

pub(crate) fn evaluate_cached_presence(
    worker: &WorkerSnapshot,
    claim: Option<&WorkerClaim>,
    cached: Option<&PresenceCacheRead>,
    owner: Option<&HostControlConfig>,
    stopping: bool,
    retired: bool,
) -> PresenceEvaluation {
    let mut response = base_snapshot(worker, owner);

    if let Some(cached) = cached {
        if let Some(sample) = cached.snapshot.as_ref() {
            response.max_sample_age_ms = max_sample_age_ms(sample);
            if sample.is_valid() {
                response.process = Some(sample.clone());
                response.sampled_at_ms = cached.sampled_at_ms;
                if let (Some(sampled_at), Some(sampled_at_ms)) =
                    (cached.sampled_at, cached.sampled_at_ms)
                {
                    let age_ms =
                        u64::try_from(sampled_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                    response.sample_age_ms = Some(age_ms);
                    let expiry_offset =
                        i64::try_from(response.max_sample_age_ms).unwrap_or(i64::MAX);
                    response.expires_at_ms = Some(sampled_at_ms.saturating_add(expiry_offset));
                }
            }
        }
    }

    if retired {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::SupervisorRetired,
        ));
    }
    if stopping {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::SupervisorStopping,
        ));
    }
    if worker.status == WorkerStatus::Unknown {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::PersistedUnknown,
        ));
    }
    if worker.status.is_terminal() {
        response.availability = WorkerPresenceAvailability::NonLive;
        response.freshness = WorkerPresenceFreshness::NotApplicable;
        response.live = false;
        response.unavailable_reason = None;
        return PresenceEvaluation::Final(response);
    }

    let Some(claim) = claim else {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::InactiveClaim,
        ));
    };
    if !claim_matches_worker(claim, worker) {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::ClaimMismatch,
        ));
    }

    let Some(cached) = cached else {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::NoObservation,
        ));
    };
    if cached.invalid {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::InvalidObservation,
        ));
    }
    let Some(sample) = cached.snapshot.as_ref() else {
        let reason = if cached.closed {
            WorkerPresenceUnavailableReason::ObserverClosed
        } else {
            WorkerPresenceUnavailableReason::NoObservation
        };
        return PresenceEvaluation::Final(unavailable(response, reason));
    };
    if !sample.is_valid() {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::InvalidObservation,
        ));
    }
    if cached.closed && sample.state != OwnedProcessState::Stopped {
        return PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::ObserverClosed,
        ));
    }

    match &sample.state {
        OwnedProcessState::Preparing => PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::AwaitingProcessLaunch,
        )),
        OwnedProcessState::Stopped => {
            response.availability = WorkerPresenceAvailability::NonLive;
            response.freshness = WorkerPresenceFreshness::NotApplicable;
            response.live = false;
            response.unavailable_reason = None;
            PresenceEvaluation::Final(response)
        }
        OwnedProcessState::Unknown => PresenceEvaluation::Final(unavailable(
            response,
            WorkerPresenceUnavailableReason::RunnerUnknown,
        )),
        OwnedProcessState::Running => {
            if !sample.is_live() {
                return PresenceEvaluation::Final(unavailable(
                    response,
                    WorkerPresenceUnavailableReason::InvalidObservation,
                ));
            }
            let Some(sampled_at) = cached.sampled_at else {
                return PresenceEvaluation::Final(unavailable(
                    response,
                    WorkerPresenceUnavailableReason::NoObservation,
                ));
            };
            let age_ms = u64::try_from(sampled_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            response.sample_age_ms = Some(age_ms);
            if age_ms > response.max_sample_age_ms {
                response.freshness = WorkerPresenceFreshness::Stale;
                return PresenceEvaluation::Final(unavailable(
                    response,
                    WorkerPresenceUnavailableReason::StaleObservation,
                ));
            }
            response.freshness = WorkerPresenceFreshness::Fresh;
            response.unavailable_reason = None;
            PresenceEvaluation::RequiresPhysicalCheck(response, sample.clone())
        }
    }
}

pub(crate) fn apply_physical_verification(
    mut response: WorkerPresenceSnapshot,
    verification: ProcessVerification,
) -> WorkerPresenceSnapshot {
    match verification {
        ProcessVerification::Running => {
            response.availability = WorkerPresenceAvailability::Live;
            response.freshness = WorkerPresenceFreshness::Fresh;
            response.live = true;
            response.unavailable_reason = None;
        }
        ProcessVerification::Missing => {
            response = unavailable(response, WorkerPresenceUnavailableReason::ProcessMissing)
        }
        ProcessVerification::Exited => {
            response = unavailable(response, WorkerPresenceUnavailableReason::ProcessExited)
        }
        ProcessVerification::Zombie => {
            response = unavailable(response, WorkerPresenceUnavailableReason::ProcessZombie)
        }
        ProcessVerification::IdentityMismatch => {
            response = unavailable(
                response,
                WorkerPresenceUnavailableReason::ProcessIdentityMismatch,
            )
        }
        ProcessVerification::GroupMismatch => {
            response = unavailable(
                response,
                WorkerPresenceUnavailableReason::ProcessGroupMismatch,
            )
        }
        ProcessVerification::InspectionFailed => {
            response = unavailable(
                response,
                WorkerPresenceUnavailableReason::ProcessInspectionFailed,
            )
        }
        #[cfg(not(target_os = "linux"))]
        ProcessVerification::UnsupportedPlatform => {
            response = unavailable(
                response,
                WorkerPresenceUnavailableReason::UnsupportedPlatform,
            )
        }
    }
    response
}

pub(crate) fn unavailable_from_worker(
    worker: &WorkerSnapshot,
    owner: Option<&HostControlConfig>,
    reason: WorkerPresenceUnavailableReason,
) -> WorkerPresenceSnapshot {
    unavailable(base_snapshot(worker, owner), reason)
}

pub(crate) fn same_process_identity(
    left: &OwnedProcessSnapshot,
    right: &OwnedProcessSnapshot,
) -> bool {
    left.invocation_id == right.invocation_id
        && left.child_pid == right.child_pid
        && left.process_group_id == right.process_group_id
        && left.linux_process_start_ticks == right.linux_process_start_ticks
}

fn unavailable(
    mut response: WorkerPresenceSnapshot,
    reason: WorkerPresenceUnavailableReason,
) -> WorkerPresenceSnapshot {
    response.availability = WorkerPresenceAvailability::Unavailable;
    if response.freshness != WorkerPresenceFreshness::Stale {
        response.freshness = WorkerPresenceFreshness::NotApplicable;
    }
    response.live = false;
    response.unavailable_reason = Some(reason);
    response
}

fn base_snapshot(
    worker: &WorkerSnapshot,
    owner: Option<&HostControlConfig>,
) -> WorkerPresenceSnapshot {
    WorkerPresenceSnapshot {
        schema_version: WORKER_PRESENCE_SCHEMA_VERSION,
        binding_id: worker.binding_id.clone(),
        run_id: worker.run_id.clone(),
        worker_id: worker.worker_id.clone(),
        worker_revision: worker.revision,
        worker_status: worker.status,
        claim_id: worker.claim_id.clone(),
        attempt_count: worker.attempt_count,
        profile_id: worker.profile_id.clone(),
        profile_digest: worker.profile_digest.clone(),
        execution_binding: worker.execution_binding.clone(),
        availability: WorkerPresenceAvailability::Unavailable,
        freshness: WorkerPresenceFreshness::NotApplicable,
        live: false,
        sampled_at_ms: None,
        sample_age_ms: None,
        max_sample_age_ms: WORKER_PRESENCE_MAX_SAMPLE_AGE_MS,
        expires_at_ms: None,
        process: None,
        owner: owner.cloned(),
        unavailable_reason: None,
    }
}

fn claim_matches_worker(claim: &WorkerClaim, worker: &WorkerSnapshot) -> bool {
    claim.binding_id == worker.binding_id
        && claim.run_id == worker.run_id
        && claim.worker_id == worker.worker_id
        && worker.claim_id.as_deref() == Some(claim.claim_id.as_str())
        && claim.attempt_count == worker.attempt_count
        && claim.profile_id == worker.profile_id
        && claim.profile_digest == worker.profile_digest
        && claim.provider == worker.provider
        && claim.requested_model == worker.requested_model
        && claim.requested_effort == worker.requested_effort
        && claim.base_commit == worker.worktree.base_commit
        && claim.execution_binding == worker.execution_binding
        && claim.instruction == worker.instruction
}

fn max_sample_age_ms(sample: &OwnedProcessSnapshot) -> u64 {
    sample
        .heartbeat_interval_ms
        .saturating_mul(WORKER_PRESENCE_HEARTBEATS)
        .clamp(1, WORKER_PRESENCE_MAX_SAMPLE_AGE_MS)
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
pub(crate) fn verify_linux_process(snapshot: &OwnedProcessSnapshot) -> ProcessVerification {
    let (Some(pid), Some(process_group_id), Some(expected_start_ticks)) = (
        snapshot.child_pid,
        snapshot.process_group_id,
        snapshot.linux_process_start_ticks,
    ) else {
        return ProcessVerification::Missing;
    };
    if pid == 0 || expected_start_ticks == 0 {
        return ProcessVerification::Missing;
    }

    let stat_path = format!("/proc/{pid}/stat");
    let file = match std::fs::File::open(stat_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return ProcessVerification::Missing;
        }
        Err(_) => return ProcessVerification::InspectionFailed,
    };
    let mut contents = String::new();
    if file
        .take(MAX_PROC_STAT_BYTES)
        .read_to_string(&mut contents)
        .is_err()
    {
        return ProcessVerification::InspectionFailed;
    }
    let Some(command_end) = contents.rfind(')') else {
        return ProcessVerification::InspectionFailed;
    };
    let fields: Vec<&str> = contents[command_end + 1..].split_whitespace().collect();
    // `/proc/<pid>/stat` fields after the command are state, ppid, pgrp, session, ...
    // and starttime is field 22 overall (index 19 after the command).
    if fields.len() <= 19 {
        return ProcessVerification::InspectionFailed;
    }
    let state = fields[0].chars().next().unwrap_or('?');
    if matches!(state, 'Z') {
        return ProcessVerification::Zombie;
    }
    if matches!(state, 'X' | 'x') {
        return ProcessVerification::Exited;
    }
    let Ok(actual_process_group_id) = fields[2].parse::<i32>() else {
        return ProcessVerification::InspectionFailed;
    };
    let Ok(actual_start_ticks) = fields[19].parse::<u64>() else {
        return ProcessVerification::InspectionFailed;
    };
    if actual_process_group_id != process_group_id {
        return ProcessVerification::GroupMismatch;
    }
    if actual_start_ticks != expected_start_ticks {
        return ProcessVerification::IdentityMismatch;
    }
    ProcessVerification::Running
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn verify_linux_process(_snapshot: &OwnedProcessSnapshot) -> ProcessVerification {
    ProcessVerification::UnsupportedPlatform
}

#[cfg(test)]
mod tests {
    use super::*;
    use threadlane_workers::OwnedProcessOrigin;

    fn sample(sequence: u64, state: OwnedProcessState) -> OwnedProcessSnapshot {
        OwnedProcessSnapshot {
            schema_version: 1,
            origin: OwnedProcessOrigin::RunnerOwnedChild,
            invocation_id: "00000000-0000-4000-8000-000000000001".to_string(),
            state,
            unknown_reason: None,
            sequence,
            elapsed_ms: sequence.saturating_mul(1_000),
            heartbeat_interval_ms: 1_000,
            child_pid: Some(std::process::id()),
            process_group_id: i32::try_from(std::process::id()).ok(),
            linux_process_start_ticks: Some(1),
        }
    }

    #[test]
    fn unchanged_sequence_never_refreshes_the_sample_time() {
        let (_sender, receiver) = watch::channel::<Option<OwnedProcessSnapshot>>(None);
        let cache = WorkerPresenceCache::new(receiver);
        let snapshot = sample(1, OwnedProcessState::Running);
        cache.record(Some(snapshot.clone()));
        let first = cache.read().sampled_at.expect("first sample time");

        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.record(Some(snapshot));

        let second = cache.read().sampled_at.expect("second sample time");
        assert_eq!(first, second);
        assert!(!cache.read().invalid);
    }

    #[test]
    fn sequence_regression_and_same_sequence_mutation_fail_closed() {
        let (_sender, receiver) = watch::channel::<Option<OwnedProcessSnapshot>>(None);
        let cache = WorkerPresenceCache::new(receiver);

        let mut second = sample(2, OwnedProcessState::Running);
        cache.record(Some(second.clone()));
        cache.record(Some(sample(1, OwnedProcessState::Running)));
        assert!(cache.read().invalid);

        let (_sender, receiver) = watch::channel::<Option<OwnedProcessSnapshot>>(None);
        let cache = WorkerPresenceCache::new(receiver);
        cache.record(Some(second.clone()));
        second.elapsed_ms += 1;
        cache.record(Some(second));
        assert!(cache.read().invalid);
    }

    #[tokio::test]
    async fn closed_observer_keeps_the_last_stopped_sample_but_marks_the_channel_closed() {
        let (sender, receiver) = watch::channel::<Option<OwnedProcessSnapshot>>(None);
        let cache = Arc::new(WorkerPresenceCache::new(receiver));
        let task = tokio::spawn(Arc::clone(&cache).watch_changes());
        sender
            .send(Some(sample(1, OwnedProcessState::Stopped)))
            .expect("send stopped sample");
        drop(sender);
        task.await.expect("observer task");

        let read = cache.read();
        assert!(read.closed);
        assert!(matches!(
            read.snapshot.as_ref().map(|sample| &sample.state),
            Some(OwnedProcessState::Stopped)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn physical_identity_check_rejects_reused_pid_pgid_and_start_ticks() {
        let pid = std::process::id();
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .expect("read current process stat");
        let command_end = stat.rfind(')').expect("command terminator");
        let fields: Vec<&str> = stat[command_end + 1..].split_whitespace().collect();
        let process_group_id = fields[2].parse::<i32>().expect("current pgrp");
        let start_ticks = fields[19].parse::<u64>().expect("current start ticks");

        let mut observation = sample(1, OwnedProcessState::Running);
        observation.child_pid = Some(pid);
        observation.process_group_id = Some(process_group_id);
        observation.linux_process_start_ticks = Some(start_ticks);
        assert_eq!(
            verify_linux_process(&observation),
            ProcessVerification::Running
        );

        observation.linux_process_start_ticks = Some(start_ticks.saturating_add(1));
        assert_eq!(
            verify_linux_process(&observation),
            ProcessVerification::IdentityMismatch
        );

        observation.linux_process_start_ticks = Some(start_ticks);
        observation.process_group_id = Some(process_group_id.saturating_add(1));
        assert_eq!(
            verify_linux_process(&observation),
            ProcessVerification::GroupMismatch
        );

        observation.child_pid = Some(u32::MAX);
        assert_eq!(
            verify_linux_process(&observation),
            ProcessVerification::Missing
        );
    }
}
