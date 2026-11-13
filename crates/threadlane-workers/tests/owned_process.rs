#![cfg(unix)]
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use threadlane_workers::{
    run_worker_with_process_observer, ExecutionBudgets, OwnedProcessObserver, OwnedProcessOrigin,
    OwnedProcessSnapshot, OwnedProcessState, OwnedProcessUnknownReason, WorkerError,
    WorkerExecutionReport, WorkerFilePermissions, WorkerProvider, WorkerRunStatus, WorkerSpec,
    DEFAULT_OWNED_PROCESS_HEARTBEAT_MS, OWNED_PROCESS_SCHEMA_VERSION,
};
use tokio::sync::watch;

struct Fake {
    _root: TempDir,
    spec: WorkerSpec,
    workdir: PathBuf,
}

impl Fake {
    fn new(body: &str) -> Self {
        let root = tempfile::tempdir().expect("create temp dir");
        let path = fs::canonicalize(root.path()).expect("canonicalize temp dir");
        let repository_path = path.join("repo");
        let workdir = path.join("workdir");
        fs::create_dir(&repository_path).expect("create repository path");
        fs::create_dir(&workdir).expect("create workdir");
        let executable = path.join("worker");
        let script = format!(
            "#!/bin/sh\nset -eu\nprintf '%s\\n' '{{\"type\":\"init\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n{body}\nprintf '%s\\n' '{{\"type\":\"result\",\"subtype\":\"success\",\"model\":\"gemini-4-argon-high\",\"effort\":\"high\"}}'\n"
        );
        fs::write(&executable, script).expect("write fake worker");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("chmod fake worker");
        Self {
            _root: root,
            spec: WorkerSpec {
                provider: WorkerProvider::Antigravity,
                executable,
                model: "gemini-4-argon-high".to_owned(),
                effort: Some("high".to_owned()),
                allowed_models: BTreeSet::new(),
                session_id: None,
                standalone: false,
                repository_path,
                base_commit: "HEAD".to_owned(),
                work_area_root: path.join("work"),
                allowed_write_paths: BTreeSet::from(["result.txt".to_owned()]),
                prompt: "PRIVATE_PROCESS_PROMPT_SENTINEL".to_owned(),
                file_permissions: WorkerFilePermissions::Inherit,
                budgets: ExecutionBudgets {
                    timeout_ms: 5_000,
                    ..ExecutionBudgets::default()
                },
            },
            workdir,
        }
    }

    fn release_file(&self) -> PathBuf {
        self.workdir.join("release-owned-process")
    }
}

fn start_run<'a>(
    fake: &'a Fake,
    cancel_rx: &'a mut watch::Receiver<bool>,
    process_tx: Option<OwnedProcessObserver>,
) -> impl std::future::Future<Output = Result<WorkerExecutionReport, WorkerError>> + 'a {
    run_worker_with_process_observer(&fake.spec, &fake.workdir, cancel_rx, None, None, process_tx)
}

fn push_latest(
    snapshots: &mut Vec<OwnedProcessSnapshot>,
    receiver: &watch::Receiver<Option<OwnedProcessSnapshot>>,
) {
    if let Some(snapshot) = receiver.borrow().clone() {
        if snapshots.last() != Some(&snapshot) {
            snapshots.push(snapshot);
        }
    }
}

const FORGED_INVOCATION_ID: &str = "00000000-0000-4000-8000-000000000001";
const FORGED_CHILD_PID: u32 = 4_242;
const FORGED_PROCESS_GROUP_ID: i32 = 4_242;
const FORGED_START_TICKS: u64 = 12_345;
const TEST_WALL_CLOCK_LIMIT: Duration = Duration::from_secs(10);

fn forged_preparing_snapshot() -> OwnedProcessSnapshot {
    OwnedProcessSnapshot {
        schema_version: OWNED_PROCESS_SCHEMA_VERSION,
        origin: OwnedProcessOrigin::RunnerOwnedChild,
        invocation_id: FORGED_INVOCATION_ID.to_owned(),
        state: OwnedProcessState::Preparing,
        unknown_reason: None,
        sequence: 1,
        elapsed_ms: 0,
        heartbeat_interval_ms: DEFAULT_OWNED_PROCESS_HEARTBEAT_MS,
        child_pid: None,
        process_group_id: None,
        linux_process_start_ticks: None,
    }
}

fn forged_running_snapshot() -> OwnedProcessSnapshot {
    OwnedProcessSnapshot {
        schema_version: OWNED_PROCESS_SCHEMA_VERSION,
        origin: OwnedProcessOrigin::RunnerOwnedChild,
        invocation_id: FORGED_INVOCATION_ID.to_owned(),
        state: OwnedProcessState::Running,
        unknown_reason: None,
        sequence: 2,
        elapsed_ms: 10,
        heartbeat_interval_ms: DEFAULT_OWNED_PROCESS_HEARTBEAT_MS,
        child_pid: Some(FORGED_CHILD_PID),
        process_group_id: Some(FORGED_PROCESS_GROUP_ID),
        linux_process_start_ticks: Some(FORGED_START_TICKS),
    }
}

fn next_revision(mut snapshot: OwnedProcessSnapshot) -> OwnedProcessSnapshot {
    snapshot.sequence += 1;
    snapshot.elapsed_ms += 1;
    snapshot
}

#[tokio::test]
async fn pre_validation_failure_never_emits_running_and_closes_the_observer() {
    let mut fake = Fake::new("exit 0");
    fake.spec.prompt = "   ".to_owned();
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);

    let error = tokio::time::timeout(
        TEST_WALL_CLOCK_LIMIT,
        run_worker_with_process_observer(
            &fake.spec,
            &fake.workdir,
            &mut cancel_rx,
            None,
            None,
            Some(process_tx),
        ),
    )
    .await
    .expect("pre-validation failure must not hang")
    .expect_err("blank prompt must fail before launch");
    assert!(matches!(error, WorkerError::InvalidSpec(_)));
    assert!(process_rx.borrow().is_none());
    let closed = tokio::time::timeout(Duration::from_secs(1), process_rx.changed()).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "pre-validation failure must close the observation watch"
    );
}

#[test]
fn running_and_stopped_require_concrete_matching_child_identity() {
    let running = forged_running_snapshot();
    assert!(running.is_valid());
    assert!(running.is_live());

    let mut missing_running = running.clone();
    missing_running.child_pid = None;
    missing_running.process_group_id = None;
    missing_running.linux_process_start_ticks = None;
    assert!(!missing_running.is_valid());
    assert!(!missing_running.is_live());

    let mut mismatched_running = running.clone();
    mismatched_running.process_group_id = Some(FORGED_PROCESS_GROUP_ID + 1);
    assert!(!mismatched_running.is_valid());
    assert!(!mismatched_running.is_live());

    let mut missing_stopped = running.clone();
    missing_stopped.state = OwnedProcessState::Stopped;
    missing_stopped.child_pid = None;
    missing_stopped.process_group_id = None;
    missing_stopped.linux_process_start_ticks = None;
    assert!(!missing_stopped.is_valid());
    assert!(!missing_stopped.is_live());

    let preparing = forged_preparing_snapshot();
    assert!(preparing.is_valid());
    assert!(!preparing.is_live());
}

#[test]
fn successor_pins_child_identity_heartbeat_and_linux_start_ticks() {
    let running = forged_running_snapshot();
    let heartbeat = next_revision(running.clone());
    assert!(heartbeat.is_valid());
    assert!(heartbeat.is_valid_successor_of(&running));

    let mut changed_child = next_revision(heartbeat.clone());
    changed_child.child_pid = Some(FORGED_CHILD_PID + 1);
    changed_child.process_group_id = Some(FORGED_PROCESS_GROUP_ID + 1);
    changed_child.linux_process_start_ticks = Some(FORGED_START_TICKS + 1);
    assert!(changed_child.is_valid());
    assert!(!changed_child.is_valid_successor_of(&heartbeat));

    let mut changed_start_identity = next_revision(heartbeat.clone());
    changed_start_identity.linux_process_start_ticks = Some(FORGED_START_TICKS + 1);
    assert!(changed_start_identity.is_valid());
    assert!(!changed_start_identity.is_valid_successor_of(&heartbeat));

    let mut cleared_start_identity = next_revision(heartbeat.clone());
    cleared_start_identity.linux_process_start_ticks = None;
    assert!(cleared_start_identity.is_valid());
    assert!(!cleared_start_identity.is_valid_successor_of(&heartbeat));

    let mut changed_heartbeat = next_revision(heartbeat.clone());
    changed_heartbeat.heartbeat_interval_ms = DEFAULT_OWNED_PROCESS_HEARTBEAT_MS + 1;
    assert!(changed_heartbeat.is_valid());
    assert!(!changed_heartbeat.is_valid_successor_of(&heartbeat));

    let mut stopped = next_revision(heartbeat.clone());
    stopped.state = OwnedProcessState::Stopped;
    assert!(stopped.is_valid());
    assert!(stopped.is_valid_successor_of(&heartbeat));

    let mut stopped_changed = next_revision(stopped.clone());
    stopped_changed.child_pid = Some(FORGED_CHILD_PID + 1);
    stopped_changed.process_group_id = Some(FORGED_PROCESS_GROUP_ID + 1);
    assert!(stopped_changed.is_valid());
    assert!(!stopped_changed.is_valid_successor_of(&stopped));

    let mut unknown = next_revision(heartbeat.clone());
    unknown.state = OwnedProcessState::Unknown;
    unknown.unknown_reason = Some(OwnedProcessUnknownReason::ExitUnconfirmed);
    assert!(unknown.is_valid());
    assert!(unknown.is_valid_successor_of(&heartbeat));

    let mut disappeared = next_revision(heartbeat.clone());
    disappeared.state = OwnedProcessState::Unknown;
    disappeared.unknown_reason = Some(OwnedProcessUnknownReason::ExitUnconfirmed);
    disappeared.child_pid = None;
    disappeared.process_group_id = None;
    disappeared.linux_process_start_ticks = None;
    assert!(disappeared.is_valid());
    assert!(!disappeared.is_valid_successor_of(&heartbeat));

    let mut without_start_identity = running.clone();
    without_start_identity.linux_process_start_ticks = None;
    assert!(without_start_identity.is_valid());
    let without_start_heartbeat = next_revision(without_start_identity.clone());
    assert!(without_start_heartbeat.is_valid_successor_of(&without_start_identity));

    let mut late_start_identity = next_revision(without_start_identity.clone());
    late_start_identity.linux_process_start_ticks = Some(FORGED_START_TICKS);
    assert!(late_start_identity.is_valid());
    assert!(!late_start_identity.is_valid_successor_of(&without_start_identity));
}

#[test]
fn coalesced_preparing_to_stopped_establishes_but_never_invents_identity() {
    let preparing = forged_preparing_snapshot();
    let mut running = forged_running_snapshot();
    running.sequence = preparing.sequence + 1;
    running.elapsed_ms = preparing.elapsed_ms + 1;
    assert!(running.is_valid_successor_of(&preparing));

    let mut stopped = next_revision(running.clone());
    stopped.state = OwnedProcessState::Stopped;
    assert!(stopped.is_valid_successor_of(&running));

    let mut coalesced_stopped = stopped.clone();
    coalesced_stopped.sequence = preparing.sequence + 1;
    coalesced_stopped.elapsed_ms = preparing.elapsed_ms + 1;
    assert!(coalesced_stopped.is_valid());
    assert!(coalesced_stopped.is_valid_successor_of(&preparing));

    let mut stopped_without_identity = coalesced_stopped.clone();
    stopped_without_identity.child_pid = None;
    stopped_without_identity.process_group_id = None;
    stopped_without_identity.linux_process_start_ticks = None;
    assert!(!stopped_without_identity.is_valid());
    assert!(!stopped_without_identity.is_valid_successor_of(&preparing));
}

#[test]
fn invocation_identity_must_be_a_bounded_canonical_uuid() {
    let valid = forged_running_snapshot();
    assert!(valid.is_valid());

    for invalid in [
        "",
        "spoofed",
        "00000000000040008000000000000001",
        "{00000000-0000-4000-8000-000000000001}",
        "urn:uuid:00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000001-extra-long",
        "00000000-0000-4000-8000-00000000000G",
        "00000000-0000-4000-8000-00000000000A",
    ] {
        let mut candidate = valid.clone();
        candidate.invocation_id = invalid.to_owned();
        assert!(
            !candidate.is_valid(),
            "accepted non-canonical invocation identity {invalid:?}"
        );
    }
}

#[tokio::test]
async fn pre_spawn_refusal_never_publishes_running() {
    let fake = Fake::new("exit 0");
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    cancel_tx.send_replace(true);
    let (process_tx, mut process_rx) = watch::channel(None);

    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));
    let mut snapshots = Vec::new();
    let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => break result.expect("pre-spawn cancellation run"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    snapshots.push(snapshot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for pre-spawn cancellation");
    push_latest(&mut snapshots, &process_rx);

    assert_eq!(report.status, WorkerRunStatus::Cancelled);
    assert!(snapshots.iter().all(|snapshot| !snapshot.is_live()));
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.state != OwnedProcessState::Running));
    let last = snapshots.last().expect("terminal presence snapshot");
    assert_eq!(last.state, OwnedProcessState::Unknown);
    assert_eq!(
        last.unknown_reason,
        Some(OwnedProcessUnknownReason::NoOwnedChild)
    );
    assert!(last.child_pid.is_none());
    assert!(last.process_group_id.is_none());
}

#[tokio::test]
async fn running_heartbeat_and_stopped_share_identity_and_increasing_sequence() {
    let fake = Fake::new("while [ ! -f release-owned-process ]; do sleep 0.02; done");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);
    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

    let mut snapshots = Vec::new();
    let running = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => panic!("worker exited before Running: {result:?}"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    if snapshot.state == OwnedProcessState::Running {
                        snapshots.push(snapshot.clone());
                        break snapshot;
                    }
                    snapshots.push(snapshot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for Running presence");
    assert!(running.is_live());
    assert!(running.child_pid.is_some_and(|pid| pid > 1));
    assert_eq!(
        running.process_group_id,
        running.child_pid.and_then(|pid| i32::try_from(pid).ok())
    );
    assert_eq!(running.heartbeat_interval_ms, 1_000);
    let invocation_id = running.invocation_id.clone();

    let fresh_heartbeat = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => panic!("worker exited before fresh heartbeat: {result:?}"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    assert_eq!(snapshot.invocation_id, invocation_id);
                    assert!(snapshot.sequence > running.sequence);
                    let is_fresh_heartbeat = snapshot.state == OwnedProcessState::Running
                        && snapshot.elapsed_ms >= 1_000;
                    snapshots.push(snapshot.clone());
                    if is_fresh_heartbeat {
                        break snapshot;
                    }
                }
            }
        }
    })
    .await
    .expect("timed out waiting for a fresh post-spawn heartbeat");
    assert_eq!(fresh_heartbeat.state, OwnedProcessState::Running);
    assert!(fresh_heartbeat.elapsed_ms >= 1_000);
    assert!(fresh_heartbeat.sequence > running.sequence);
    fs::write(fake.release_file(), b"release").expect("release fake worker");

    let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => break result.expect("observed run"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    snapshots.push(snapshot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for observed run completion");
    push_latest(&mut snapshots, &process_rx);

    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.invocation_id == invocation_id));
    assert!(snapshots
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence));
    let last = snapshots.last().expect("stopped snapshot");
    assert_eq!(last.state, OwnedProcessState::Stopped);
    assert!(!last.is_live());
    assert_eq!(last.child_pid, running.child_pid);
    assert_eq!(last.process_group_id, running.process_group_id);
    assert_eq!(
        last.linux_process_start_ticks,
        running.linux_process_start_ticks
    );
    assert!(last.sequence > running.sequence);
    let serialized = serde_json::to_string(&snapshots).expect("serialize snapshots");
    assert!(!serialized.contains("PRIVATE_PROCESS_PROMPT_SENTINEL"));
    assert!(!serialized.contains("gemini-4-argon-high"));
}

#[tokio::test]
async fn confirmed_exit_clears_running_before_post_exit_pipe_cleanup() {
    let fake = Fake::new("sleep 30 &");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);
    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

    let mut snapshots = Vec::new();
    let mut saw_stopped_while_run_pending = false;
    let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => break result.expect("pipe-holder run"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    if snapshot.state == OwnedProcessState::Stopped {
                        saw_stopped_while_run_pending = true;
                    }
                    snapshots.push(snapshot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for pipe-holder run completion");
    push_latest(&mut snapshots, &process_rx);

    assert!(
        saw_stopped_while_run_pending,
        "Stopped must be published before post-exit pipe/cleanup work completes"
    );
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert!(report.descendants_terminated_after_exit);
    let last = snapshots.last().expect("stopped snapshot");
    assert_eq!(last.state, OwnedProcessState::Stopped);
    assert!(!last.is_live());
}

#[tokio::test]
async fn cancellation_and_timeout_never_leave_running_live() {
    for use_timeout in [false, true] {
        let mut fake = Fake::new("sleep 5");
        if use_timeout {
            fake.spec.budgets.timeout_ms = 1_000;
        }
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (process_tx, mut process_rx) = watch::channel(None);
        let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

        let mut snapshots = Vec::new();
        let running = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
            loop {
                tokio::select! {
                    biased;
                    result = &mut run => panic!("worker ended before cancellation: {result:?}"),
                    changed = process_rx.changed() => {
                        changed.expect("presence channel");
                        let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                        if snapshot.state == OwnedProcessState::Running {
                            snapshots.push(snapshot.clone());
                            break snapshot;
                        }
                        snapshots.push(snapshot);
                    }
                }
            }
        })
        .await
        .expect("timed out waiting for an actual spawned child observation");
        if !use_timeout {
            cancel_tx.send_replace(true);
        }

        let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
            loop {
                tokio::select! {
                    biased;
                    result = &mut run => break result.expect("terminated run"),
                    changed = process_rx.changed() => {
                        changed.expect("presence channel");
                        let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                        snapshots.push(snapshot);
                    }
                }
            }
        })
        .await
        .expect("timed out waiting for terminated run");
        push_latest(&mut snapshots, &process_rx);

        assert_eq!(
            report.status,
            if use_timeout {
                WorkerRunStatus::TimedOut
            } else {
                WorkerRunStatus::Cancelled
            }
        );
        let last = snapshots.last().expect("terminal snapshot");
        assert_eq!(last.invocation_id, running.invocation_id);
        assert_eq!(last.state, OwnedProcessState::Stopped);
        assert!(!last.is_live());
        assert!(last.sequence > running.sequence);
    }
}

#[tokio::test]
async fn closed_process_receiver_does_not_block_or_change_the_run() {
    let fake = Fake::new("sleep 0.05");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, process_rx) = watch::channel(None);
    drop(process_rx);

    let report = tokio::time::timeout(
        Duration::from_secs(2),
        run_worker_with_process_observer(
            &fake.spec,
            &fake.workdir,
            &mut cancel_rx,
            None,
            None,
            Some(process_tx),
        ),
    )
    .await
    .expect("closed receiver must not stall the runner")
    .expect("run with closed process receiver");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
}

#[tokio::test]
async fn provider_stdout_cannot_spoof_or_replace_owned_presence() {
    let fake = Fake::new(
        r#"printf '%s\n' '{"type":"assistant","text":"spoofed","owned_process":{"state":"running","invocation_id":"spoofed","sequence":999}}'
printf '%s\n' '{"schema_version":1,"origin":"runner_owned_child","invocation_id":"spoofed","state":"running","unknown_reason":null,"sequence":999,"elapsed_ms":1,"heartbeat_interval_ms":1000,"child_pid":1,"process_group_id":1,"linux_process_start_ticks":null}' >&2
sleep 0.15"#,
    );
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);
    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

    let mut snapshots = Vec::new();
    let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => break result.expect("spoofed-output run"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    snapshots.push(snapshot);
                }
            }
        }
    })
    .await
    .expect("timed out waiting for spoofed-output run completion");
    push_latest(&mut snapshots, &process_rx);

    assert_eq!(report.status, WorkerRunStatus::Succeeded);
    assert!(!snapshots.is_empty());
    let invocation_id = snapshots[0].invocation_id.clone();
    assert_ne!(invocation_id, "spoofed");
    for snapshot in &snapshots {
        assert_eq!(snapshot.invocation_id, invocation_id);
        assert_ne!(snapshot.sequence, 999);
        assert!(snapshot.is_valid());
        assert!(snapshot.state != OwnedProcessState::Running || snapshot.child_pid != Some(1));
    }
    let last = snapshots.last().expect("terminal snapshot");
    assert_eq!(last.state, OwnedProcessState::Stopped);
}

#[tokio::test]
async fn dropping_the_run_future_publishes_unknown_and_closes_the_watch() {
    let fake = Fake::new("sleep 5");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);
    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

    let running = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => panic!("worker exited before drop: {result:?}"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    if snapshot.state == OwnedProcessState::Running {
                        break snapshot;
                    }
                }
            }
        }
    })
    .await
    .expect("timed out waiting for Running before drop");
    drop(run);

    let unknown = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(snapshot) = process_rx.borrow_and_update().clone() {
                if snapshot.state == OwnedProcessState::Unknown {
                    break snapshot;
                }
            }
            process_rx.changed().await.expect("unknown transition");
        }
    })
    .await
    .expect("dropped future must publish Unknown");
    assert_eq!(unknown.invocation_id, running.invocation_id);
    assert_eq!(
        unknown.unknown_reason,
        Some(OwnedProcessUnknownReason::PublisherDropped)
    );
    assert!(!unknown.is_live());
    assert!(unknown.sequence > running.sequence);
    let closed = tokio::time::timeout(Duration::from_secs(1), process_rx.changed()).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "dropped future must close the observation watch"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_spawned_child_reports_proc_start_identity() {
    let fake = Fake::new("while [ ! -f release-owned-process ]; do sleep 0.02; done");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let (process_tx, mut process_rx) = watch::channel(None);
    let mut run = Box::pin(start_run(&fake, &mut cancel_rx, Some(process_tx)));

    let running = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, async {
        loop {
            tokio::select! {
                biased;
                result = &mut run => panic!("worker exited before Running: {result:?}"),
                changed = process_rx.changed() => {
                    changed.expect("presence channel");
                    let snapshot = process_rx.borrow_and_update().clone().expect("snapshot");
                    if snapshot.state == OwnedProcessState::Running {
                        break snapshot;
                    }
                }
            }
        }
    })
    .await
    .expect("timed out waiting for Linux Running presence");
    if std::path::Path::new("/proc/self/stat").exists() {
        assert!(running.linux_process_start_ticks.is_some());
    }
    fs::write(fake.release_file(), b"release").expect("release fake worker");
    let report = tokio::time::timeout(TEST_WALL_CLOCK_LIMIT, run)
        .await
        .expect("timed out waiting for observed run completion")
        .expect("finish observed run");
    assert_eq!(report.status, WorkerRunStatus::Succeeded);
}
