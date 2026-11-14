//! Focused authenticated control-runtime identity and atomic idle-retire tests.
//!
//! Every test runs against isolated private temporary directories and owned in-process
//! supervisors. No user runtime state, credentials, or external services are touched.

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane::descriptor::{read_and_validate_descriptor, SecretToken, DESCRIPTOR_FILENAME};
use threadlane::{
    CodexWakeupConfig, ControlRuntimeRetireRequest, ControlRuntimeRetireState, ControlRuntimeRole,
    ControlRuntimeRuntimeState, Error, ErrorCode, HostControlConfig, ScopedClient, Supervisor,
    SupervisorClient, SupervisorConfig, CONTROL_RUNTIME_PROTOCOL_VERSION,
};
use threadlane_core::{BacklogItemKind, BacklogStatus, Run, RunStatus};

const THREAD_UUID: &str = "018f1a2b-3c4d-7e00-8000-000000000201";
const DEMO_COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);

fn test_host_control() -> HostControlConfig {
    HostControlConfig {
        approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
        subject_digest: "a".repeat(64),
        control_binding_id: "bnd_control_001".to_string(),
        thread_uuid: THREAD_UUID.to_string(),
        session_binding_id: "bnd_session_001".to_string(),
        instance_id: "hst_0123456789abcdef0123456789abcdef".to_string(),
    }
}

/// Answers every active demo question at the run's current revision and waits for completion
/// within a bounded deadline. Demo runs pause at `WaitingInput` even with a zero step delay, so
/// polling alone can never reach `Completed`.
async fn complete_demo_run(scoped: &ScopedClient, started: Run, label: &str) -> Run {
    let deadline = Instant::now() + DEMO_COMPLETION_TIMEOUT;
    let mut run = started;
    let mut answer_index = 0usize;

    loop {
        match run.status {
            RunStatus::Completed => return run,
            RunStatus::WaitingInput => {
                let question_id = run.active_question_id.clone().unwrap_or_else(|| {
                    panic!(
                        "{label}: waiting demo run {} has no active question",
                        run.run_id
                    )
                });
                let run_id = run.run_id.clone();
                let command_key = format!("{label}-answer-{answer_index}");
                answer_index += 1;
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "{label}: demo run {run_id} did not complete before the deadline"
                );
                run = tokio::time::timeout(
                    remaining,
                    scoped.answer(
                        command_key,
                        run_id.clone(),
                        question_id,
                        run.revision,
                        format!("{label} answer {answer_index}"),
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("{label}: timed out answering demo run {run_id}"))
                .unwrap_or_else(|err| panic!("{label}: failed to answer demo run {run_id}: {err}"));
                continue;
            }
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::TimedOut | RunStatus::Unknown => {
                panic!(
                    "{label}: demo run {} entered terminal status {:?}",
                    run.run_id, run.status
                );
            }
            _ => {}
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: demo run {} did not complete before the deadline",
            run.run_id
        );
        tokio::time::sleep(Duration::from_millis(10).min(remaining)).await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: demo run {} did not complete before the deadline",
            run.run_id
        );
        let run_id = run.run_id.clone();
        run = tokio::time::timeout(remaining, scoped.get_run(run_id.clone()))
            .await
            .unwrap_or_else(|_| panic!("{label}: timed out polling demo run {run_id}"))
            .unwrap_or_else(|err| panic!("{label}: failed to poll demo run {run_id}: {err}"));
    }
}

fn write_executable_script(dir: &std::path::Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write fake executable");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).expect("chmod fake executable");
    path
}

async fn seed_binding_and_done_item(
    state_dir: &std::path::Path,
) -> (String, String, String, usize) {
    let handle = Supervisor::start(SupervisorConfig::new(state_dir))
        .await
        .expect("seed supervisor");
    let client = SupervisorClient::from_state_dir(state_dir).expect("seed client");
    let binding = client
        .create_binding(THREAD_UUID, "control-runtime-upgrade")
        .await
        .expect("seed binding");
    let scoped =
        ScopedClient::from_state_dir(state_dir, &binding.binding_id).expect("seed scoped client");

    let item = scoped
        .create_backlog_item(
            "seed-item-create".to_string(),
            "Durable Done item".to_string(),
            "Survives clean idle retirement".to_string(),
            "Durable backlog state".to_string(),
            BacklogItemKind::Coworker,
        )
        .await
        .expect("seed backlog item");
    let item = scoped
        .start_backlog_item(
            "seed-item-start".to_string(),
            item.item_id.clone(),
            item.revision,
        )
        .await
        .expect("start backlog item");
    let item = scoped
        .complete_backlog_item(
            "seed-item-complete".to_string(),
            item.item_id.clone(),
            item.revision,
            "Delivery checked".to_string(),
            "fixture evidence".to_string(),
        )
        .await
        .expect("complete backlog item");
    assert_eq!(item.status, BacklogStatus::Done);

    let run = scoped
        .start_demo(
            "seed-demo-run".to_string(),
            "Durable demo run".to_string(),
            Some(0),
        )
        .await
        .expect("seed demo run");
    let run = complete_demo_run(&scoped, run, "seed-demo-run").await;
    let activity = scoped
        .list_activity(None, None, Some(50), None, None)
        .await
        .expect("seed activity");
    let activity_count = activity.events.len();
    // Clear pending notices while retaining event history so the runtime is truly idle.
    let mut acked_events = 0usize;
    for round in 0..20 {
        let batch = scoped
            .pull_events(None, Some(50))
            .await
            .expect("seed pull events");
        if batch.events.is_empty() {
            break;
        }
        acked_events += batch.events.len();
        scoped
            .ack_events(format!("seed-ack-{round}"), batch.batch_id)
            .await
            .expect("seed ack events");
    }
    assert!(acked_events > 0, "seeded run must emit events");

    handle.shutdown().await;
    (binding.binding_id, item.item_id, run.run_id, activity_count)
}

#[tokio::test]
async fn baseline_info_reports_bounded_identity_role_capabilities_and_no_secret() {
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("state");
    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let info = client
        .control_runtime_info()
        .await
        .expect("authenticated control runtime info");

    assert_eq!(info.protocol_version, CONTROL_RUNTIME_PROTOCOL_VERSION);
    assert_eq!(info.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.role, ControlRuntimeRole::Baseline);
    assert_eq!(info.state, ControlRuntimeRuntimeState::Ready);
    assert_eq!(info.pid, handle.descriptor().pid);
    assert_eq!(info.instance_id.len(), 32);
    assert!(info
        .instance_id
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    assert!(!info.notifications_configured);
    assert!(!info.workers_configured);
    assert!(info.capabilities.bindings);
    assert!(info.capabilities.scoped_backlog);
    assert!(info.capabilities.event_pull_ack);
    assert!(info.capabilities.guarded_worker_backlog);
    assert!(info.capabilities.idle_retire);
    assert!(!info.capabilities.approved_host_control);

    let serialized = serde_json::to_string(&info).expect("serialize info");
    assert!(
        !serialized.contains(handle.descriptor().token.as_str()),
        "control info must never expose the bearer token"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn idle_retire_latches_clean_shutdown_and_preserves_data_without_replay() {
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("state");
    let (binding_id, item_id, run_id, activity_count) =
        seed_binding_and_done_item(&state_dir).await;

    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let info = client.control_runtime_info().await.expect("info");
    let request = ControlRuntimeRetireRequest {
        expected_protocol_version: info.protocol_version,
        expected_instance_id: info.instance_id.clone(),
        expected_pid: info.pid,
    };

    let response = client
        .retire_idle_control_runtime(&request)
        .await
        .expect("idle retire accepted");
    assert_eq!(response.state, ControlRuntimeRetireState::Retiring);
    assert_eq!(response.instance_id, info.instance_id);
    assert_eq!(response.protocol_version, info.protocol_version);

    tokio::time::timeout(Duration::from_secs(1), handle.wait_for_stop_request())
        .await
        .expect("stop latch must wake clean shutdown waiters");

    // Clean idle retirement must not fabricate a fatal error. The fatal-only waiter stays pending
    // and no fatal state is recorded.
    let fatal_wait =
        tokio::time::timeout(Duration::from_millis(150), handle.wait_for_fatal()).await;
    assert!(
        fatal_wait.is_err(),
        "clean idle retirement must not resolve the fatal-only waiter"
    );
    assert!(
        handle.fatal_error().is_none(),
        "clean retirement must not record a fatal error"
    );

    handle.shutdown().await;
    assert!(
        !state_dir.join(DESCRIPTOR_FILENAME).exists(),
        "clean shutdown must remove the owned descriptor"
    );
    assert!(
        read_and_validate_descriptor(&state_dir).is_err(),
        "descriptor must not be reusable after clean shutdown"
    );

    // Reopen the same durable state through normal locked startup and verify no replay.
    let restarted = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("restart after clean retirement");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("restarted client");
    let bindings = client.list_bindings().await.expect("bindings preserved");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].binding_id, binding_id);
    let scoped =
        ScopedClient::from_state_dir(&state_dir, &binding_id).expect("restarted scoped client");
    let item = scoped
        .get_backlog_item(item_id)
        .await
        .expect("Done item preserved");
    assert_eq!(item.status, BacklogStatus::Done);
    let run = scoped.get_run(run_id.clone()).await.expect("run preserved");
    assert_eq!(run.status, RunStatus::Completed);
    let runs = scoped.list_runs(None, Some(50)).await.expect("runs");
    assert_eq!(runs.runs.len(), 1, "restart must not replay runs");
    assert_eq!(runs.runs[0].run_id, run_id);
    let activity = scoped
        .list_activity(None, None, Some(50), None, None)
        .await
        .expect("activity");
    assert_eq!(
        activity.events.len(),
        activity_count,
        "restart must not replay or fabricate events"
    );

    restarted.shutdown().await;
}

#[tokio::test]
async fn retire_refuses_active_run_and_pending_notice_without_side_effects() {
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("state");
    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID, "active-refusal")
        .await
        .expect("binding");
    let scoped =
        ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("scoped client");
    let info = client.control_runtime_info().await.expect("info");
    let request = ControlRuntimeRetireRequest {
        expected_protocol_version: info.protocol_version,
        expected_instance_id: info.instance_id.clone(),
        expected_pid: info.pid,
    };

    let active = scoped
        .start_demo(
            "active-demo".to_string(),
            "Active work must block retirement".to_string(),
            Some(5_000),
        )
        .await
        .expect("start active demo");
    let err = client
        .retire_idle_control_runtime(&request)
        .await
        .expect_err("active run must refuse retirement");
    match err {
        Error::Remote(api_err) => {
            assert_eq!(api_err.code, ErrorCode::HostNotIdle);
            let details = api_err.details.expect("busy details");
            assert_eq!(details["non_terminal_runs"], 1);
        }
        other => panic!("unexpected retire error: {other:?}"),
    }
    assert_eq!(
        client
            .control_runtime_info()
            .await
            .expect("still live")
            .state,
        ControlRuntimeRuntimeState::Ready
    );
    assert!(client.status(&binding.binding_id).await.is_ok());
    let active_run = scoped
        .get_run(active.run_id.clone())
        .await
        .expect("active run");
    assert_ne!(active_run.status, RunStatus::Completed);

    // Complete a separate run without ACKing its important event so a pending notice remains,
    // then prove the pending notice alone blocks retirement.
    let pending = scoped
        .start_demo(
            "pending-demo".to_string(),
            "Pending notice must block retirement".to_string(),
            Some(0),
        )
        .await
        .expect("start pending demo");
    let pending = complete_demo_run(&scoped, pending, "pending-demo").await;
    assert_eq!(pending.status, RunStatus::Completed);
    // Cancel the long-running demo so only the terminal run's unacked notice remains.
    let cancelled = scoped
        .cancel(
            "cancel-active-demo".to_string(),
            active.run_id,
            Some("test cleanup".to_string()),
        )
        .await
        .expect("cancel active demo");
    assert_eq!(cancelled.status, RunStatus::Cancelled);

    let err = client
        .retire_idle_control_runtime(&request)
        .await
        .expect_err("pending notice must refuse retirement");
    match err {
        Error::Remote(api_err) => {
            assert_eq!(api_err.code, ErrorCode::HostNotIdle);
            let details = api_err.details.expect("busy details");
            assert!(details["pending_wakeups"].as_u64().unwrap_or(0) >= 1);
        }
        other => panic!("unexpected retire error: {other:?}"),
    }
    assert_eq!(
        client
            .control_runtime_info()
            .await
            .expect("still live after refusal")
            .state,
        ControlRuntimeRuntimeState::Ready
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn retire_rejects_stale_instance_and_protocol_identity() {
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("state");

    let first = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("first supervisor");
    let first_client = SupervisorClient::from_state_dir(&state_dir).expect("first client");
    let first_info = first_client
        .control_runtime_info()
        .await
        .expect("first info");
    first.shutdown().await;

    let second = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("second supervisor");
    let second_client = SupervisorClient::from_state_dir(&state_dir).expect("second client");
    let second_info = second_client
        .control_runtime_info()
        .await
        .expect("second info");
    assert_ne!(first_info.instance_id, second_info.instance_id);

    let stale = ControlRuntimeRetireRequest {
        expected_protocol_version: first_info.protocol_version,
        expected_instance_id: first_info.instance_id,
        expected_pid: first_info.pid,
    };
    match second_client
        .retire_idle_control_runtime(&stale)
        .await
        .expect_err("stale identity must be rejected")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::HostInstanceMismatch),
        other => panic!("unexpected stale identity error: {other:?}"),
    }

    let wrong_protocol = ControlRuntimeRetireRequest {
        expected_protocol_version: second_info.protocol_version + 1,
        expected_instance_id: second_info.instance_id.clone(),
        expected_pid: second_info.pid,
    };
    match second_client
        .retire_idle_control_runtime(&wrong_protocol)
        .await
        .expect_err("wrong protocol must be rejected")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::InvalidConfig),
        other => panic!("unexpected protocol error: {other:?}"),
    }

    let mut wrong_token_descriptor = second.descriptor().clone();
    wrong_token_descriptor.token = SecretToken::new("f".repeat(64));
    let wrong_token_client = SupervisorClient::from_descriptor(wrong_token_descriptor)
        .expect("wrong token descriptor is well formed");
    let exact = ControlRuntimeRetireRequest {
        expected_protocol_version: second_info.protocol_version,
        expected_instance_id: second_info.instance_id.clone(),
        expected_pid: second_info.pid,
    };
    match wrong_token_client
        .retire_idle_control_runtime(&exact)
        .await
        .expect_err("wrong bearer token must be rejected")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::Unauthorized),
        other => panic!("unexpected token error: {other:?}"),
    }

    assert_eq!(
        second_client
            .control_runtime_info()
            .await
            .expect("replacement still live")
            .instance_id,
        second_info.instance_id
    );
    second.shutdown().await;
}

#[tokio::test]
async fn retire_refuses_approved_host_and_configured_notification_runtime() {
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("approved-state");
    let handle =
        Supervisor::start_with_host_control(SupervisorConfig::new(&state_dir), test_host_control())
            .await
            .expect("approved host supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("approved client");
    let info = client.control_runtime_info().await.expect("approved info");
    assert_eq!(info.role, ControlRuntimeRole::ApprovedHost);
    assert!(info.capabilities.approved_host_control);
    assert!(!info.capabilities.idle_retire);
    let request = ControlRuntimeRetireRequest {
        expected_protocol_version: info.protocol_version,
        expected_instance_id: info.instance_id.clone(),
        expected_pid: info.pid,
    };
    match client
        .retire_idle_control_runtime(&request)
        .await
        .expect_err("approved host must never retire through the baseline API")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::InvalidConfig),
        other => panic!("unexpected approved host error: {other:?}"),
    }
    assert_eq!(
        client
            .control_runtime_info()
            .await
            .expect("still live")
            .state,
        ControlRuntimeRuntimeState::Ready
    );
    handle.shutdown().await;

    let notify_state_dir = temp.path().join("notify-state");
    let fake_codex = write_executable_script(temp.path(), "fake-codex");
    let mut notify_bindings = BTreeSet::new();
    notify_bindings.insert("bnd_notify_001".to_string());
    let config = SupervisorConfig {
        state_dir: notify_state_dir.clone(),
        tick_interval: Duration::from_millis(50),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex,
            notify_bindings,
            timeout: Duration::from_millis(100),
            approved_session: None,
        }),
        workers: None,
    };
    let handle = Supervisor::start(config)
        .await
        .expect("notification supervisor");
    let client = SupervisorClient::from_state_dir(&notify_state_dir).expect("notify client");
    let info = client.control_runtime_info().await.expect("notify info");
    assert_eq!(info.role, ControlRuntimeRole::Baseline);
    assert!(info.notifications_configured);
    assert!(!info.capabilities.idle_retire);
    let request = ControlRuntimeRetireRequest {
        expected_protocol_version: info.protocol_version,
        expected_instance_id: info.instance_id.clone(),
        expected_pid: info.pid,
    };
    match client
        .retire_idle_control_runtime(&request)
        .await
        .expect_err("configured notification runtime must never retire")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::InvalidConfig),
        other => panic!("unexpected notification runtime error: {other:?}"),
    }
    assert_eq!(
        client
            .control_runtime_info()
            .await
            .expect("still live")
            .state,
        ControlRuntimeRuntimeState::Ready
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn retire_and_mutation_have_exactly_one_winner() {
    // Retirement wins first: every later mutation must be refused without creating work.
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("retire-wins");
    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID, "atomicity")
        .await
        .expect("binding");
    let scoped =
        ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("scoped client");
    let info = client.control_runtime_info().await.expect("info");
    client
        .retire_idle_control_runtime(&ControlRuntimeRetireRequest {
            expected_protocol_version: info.protocol_version,
            expected_instance_id: info.instance_id.clone(),
            expected_pid: info.pid,
        })
        .await
        .expect("retire wins");
    match scoped
        .start_demo(
            "retire-wins-mutation".to_string(),
            "must be refused".to_string(),
            Some(0),
        )
        .await
        .expect_err("mutation after retirement must be refused")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::HostStopping),
        other => panic!("unexpected post-retire mutation error: {other:?}"),
    }
    let status = client.status(&binding.binding_id).await.expect("status");
    assert_eq!(status.total_runs, 0);
    handle.shutdown().await;

    // Activity wins first: retirement must refuse without stopping the runtime.
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("activity-wins");
    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID, "activity-wins")
        .await
        .expect("binding");
    let scoped =
        ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("scoped client");
    let info = client.control_runtime_info().await.expect("info");
    let run = scoped
        .start_demo(
            "activity-wins-mutation".to_string(),
            "must keep runtime active".to_string(),
            Some(5_000),
        )
        .await
        .expect("mutation wins");
    match client
        .retire_idle_control_runtime(&ControlRuntimeRetireRequest {
            expected_protocol_version: info.protocol_version,
            expected_instance_id: info.instance_id.clone(),
            expected_pid: info.pid,
        })
        .await
        .expect_err("retirement after activity must refuse")
    {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::HostNotIdle),
        other => panic!("unexpected active retirement error: {other:?}"),
    }
    assert_ne!(
        scoped.get_run(run.run_id).await.expect("run").status,
        RunStatus::Completed
    );
    assert_eq!(
        client
            .control_runtime_info()
            .await
            .expect("still live")
            .state,
        ControlRuntimeRuntimeState::Ready
    );
    handle.shutdown().await;

    // Concurrent race: exactly one side wins and no run is created when retirement wins.
    let temp = TempDir::new().expect("tempdir");
    let state_dir = temp.path().join("concurrent");
    let handle = Supervisor::start(SupervisorConfig::new(&state_dir))
        .await
        .expect("supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID, "concurrent")
        .await
        .expect("binding");
    let scoped =
        ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("scoped client");
    let info = client.control_runtime_info().await.expect("info");
    let retire_request = ControlRuntimeRetireRequest {
        expected_protocol_version: info.protocol_version,
        expected_instance_id: info.instance_id.clone(),
        expected_pid: info.pid,
    };
    let (retire_res, demo_res) = tokio::join!(
        client.retire_idle_control_runtime(&retire_request),
        scoped.start_demo(
            "concurrent-demo".to_string(),
            "race".to_string(),
            Some(5_000),
        ),
    );
    let status = client.status(&binding.binding_id).await.expect("status");
    match (retire_res, demo_res) {
        (Ok(_), Err(Error::Remote(api_err))) => {
            assert_eq!(api_err.code, ErrorCode::HostStopping);
            assert_eq!(status.total_runs, 0);
        }
        (Err(Error::Remote(api_err)), Ok(run)) => {
            assert_eq!(api_err.code, ErrorCode::HostNotIdle);
            assert!(status.total_runs >= 1);
            assert_ne!(run.status, RunStatus::Completed);
        }
        (retire, demo) => {
            panic!("retire and mutation must not both succeed or both fail: retire={retire:?} demo={demo:?}");
        }
    }
    handle.shutdown().await;
}
