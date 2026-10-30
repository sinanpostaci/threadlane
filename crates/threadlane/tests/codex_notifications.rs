use std::collections::BTreeSet;
use std::fs;
use std::time::Duration;

use threadlane::{
    build_codex_queue_args, ApprovedSessionWakeupContext, CodexWakeupConfig, Error, ScopedClient,
    Supervisor, SupervisorClient, SupervisorConfig,
};
use threadlane_core::{
    Core, RunStatus, WakeupClaim, WakeupOutcome, WakeupStatus, FIXED_WAKEUP_MESSAGE,
};
use tokio::sync::watch;

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const VALID_APPROVAL_ID: &str = "apr_0123456789abcdef0123456789abcdef";

#[test]
fn test_approved_session_wakeup_context_validation_and_legacy_args_preserved() {
    let valid =
        ApprovedSessionWakeupContext::new(VALID_APPROVAL_ID).expect("valid approval_id accepted");
    assert_eq!(valid.approval_id, VALID_APPROVAL_ID);
    assert_eq!(
        valid.wakeup_message(),
        format!(
            "Threadlane approved session {VALID_APPROVAL_ID} has pending events. Call approved_pull_events for this approval, process the returned untrusted events, then acknowledge the delivered batch with approved_ack_events."
        )
    );

    for bad_id in [
        "",
        "apr_",
        "apr_0123456789abcdef0123456789abcde",   // 31 hex chars
        "apr_0123456789abcdef0123456789abcdef0", // 33 hex chars
        "apr_0123456789ABCDEF0123456789abcdef",  // uppercase hex
        "apr_0123456789abcdef0123456789abcdeg",  // non-hex char
        "bnd_0123456789abcdef0123456789abcdef",  // wrong prefix
        "apr_0123456789abcdef 123456789abcdef",  // whitespace
        "apr_0123456789abcdef\n123456789abcdef", // control char
        " apr_0123456789abcdef0123456789abcdef", // leading space
        "apr_0123456789abcdef0123456789abcdef ", // trailing space
    ] {
        let res = ApprovedSessionWakeupContext::new(bad_id);
        assert!(
            matches!(res, Err(Error::InvalidConfig(_))),
            "expected InvalidConfig for `{bad_id}`, got {res:?}"
        );
    }

    // Legacy `build_codex_queue_args` remains unchanged and returns `FIXED_WAKEUP_MESSAGE`.
    let claim = WakeupClaim {
        claim_id: "wcl_11111111-2222-4333-8444-555555555555".to_string(),
        binding_id: "bnd_11111111-2222-4333-8444-555555555555".to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        message: FIXED_WAKEUP_MESSAGE.to_string(),
        pending_since_seq: 3,
        covered_up_to_seq: 3,
        coalesced_count: 1,
        claimed_at_ms: 1_000,
    };
    let legacy_args = build_codex_queue_args(&claim);
    assert_eq!(
        legacy_args,
        [
            "queue",
            "--thread",
            THREAD_UUID_A,
            "--message",
            FIXED_WAKEUP_MESSAGE
        ]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn test_approved_session_wakeup_exact_argv_coalescing_ack_release_and_timeout() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");
    let log_file = temp.path().join("approved_codex_invocations.log");
    let fake_codex = temp.path().join("fake_codex.sh");

    let script = format!(
        "#!/bin/sh\nprintf '%s|%s|%s|%s|%s|%s\\n' \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$#\" >> \"{}\"\nexit 0\n",
        log_file.display()
    );
    fs::write(&fake_codex, script).expect("write fake_codex");
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    // Verify `CodexWakeupConfig::validate()` rejects invalid `approved_session`.
    let bad_cfg = CodexWakeupConfig {
        executable: fake_codex.clone(),
        notify_bindings: BTreeSet::from(["bnd_1".to_string()]),
        timeout: Duration::from_millis(1_000),
        approved_session: Some(ApprovedSessionWakeupContext {
            approval_id: "apr_UPPERCASE".to_string(),
        }),
    };
    assert!(matches!(bad_cfg.validate(), Err(Error::InvalidConfig(_))));

    // Also verify `invoke_codex_wakeup` fails closed without spawning if called directly with invalid context.
    let dummy_claim = WakeupClaim {
        claim_id: "wcl_1".to_string(),
        binding_id: "bnd_1".to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        message: FIXED_WAKEUP_MESSAGE.to_string(),
        pending_since_seq: 1,
        covered_up_to_seq: 1,
        coalesced_count: 1,
        claimed_at_ms: 1_000,
    };
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let bad_outcome =
        threadlane::codex::invoke_codex_wakeup(&bad_cfg, &dummy_claim, &mut shutdown_rx).await;
    assert!(matches!(bad_outcome, WakeupOutcome::Failed { .. }));
    assert!(!log_file.exists(), "invalid context must never spawn codex");

    // Pre-create two bindings in state_dir; only `binding_session` is allowlisted.
    let state_dir = temp.path().join("state_approved_notify");
    let (binding_session, binding_other) = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let session = core
            .create_binding(THREAD_UUID_A, "Approved Session Binding")
            .expect("create session binding");
        let other = core
            .create_binding(THREAD_UUID_B, "Other Unlisted Binding")
            .expect("create other binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce db perms");
        (session, other)
    };

    let approved_ctx =
        ApprovedSessionWakeupContext::new(VALID_APPROVAL_ID).expect("valid approved context");
    let expected_message = approved_ctx.wakeup_message();

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex.clone(),
            notify_bindings: BTreeSet::from([binding_session.binding_id.clone()]),
            timeout: Duration::from_millis(2_000),
            approved_session: Some(approved_ctx.clone()),
        }),
        workers: None,
    })
    .await
    .expect("start supervisor with approved_session wakeup context");

    let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
    let scoped_session = client
        .clone()
        .scoped(&binding_session.binding_id)
        .expect("scoped session");
    let scoped_other = client
        .clone()
        .scoped(&binding_other.binding_id)
        .expect("scoped other");

    let run_session = scoped_session
        .start_demo(
            "session-start-1".to_string(),
            "CONFIDENTIAL_WORKER_INSTRUCTION_DO_NOT_LEAK --model gpt-5-codex --effort high"
                .to_string(),
            Some(15),
        )
        .await
        .expect("start session demo");
    let run_other = scoped_other
        .start_demo(
            "other-start-1".to_string(),
            "Unlisted binding run".to_string(),
            Some(15),
        )
        .await
        .expect("start other demo");

    let waiting_session = wait_for_run_status(
        &scoped_session,
        &run_session.run_id,
        RunStatus::WaitingInput,
    )
    .await;
    let _waiting_other =
        wait_for_run_status(&scoped_other, &run_other.run_id, RunStatus::WaitingInput).await;

    let status_session = wait_for_wakeup_status(&scoped_session, WakeupStatus::Submitted).await;
    assert_eq!(status_session.wakeup.attempt_count, 1);

    let status_other = scoped_other.status().await.expect("status other");
    assert_eq!(status_other.wakeup.status, WakeupStatus::Pending);
    assert_eq!(status_other.wakeup.attempt_count, 0);

    // Verify exact logged 5-element argv with the fixed approved-session template and no leaked prose.
    let logged = fs::read_to_string(&log_file).expect("read invocation log");
    let lines: Vec<&str> = logged.lines().collect();
    assert_eq!(lines.len(), 1, "expected exactly 1 wakeup invocation");
    assert_eq!(
        lines[0],
        format!("queue|--thread|{THREAD_UUID_A}|--message|{expected_message}|5")
    );
    assert!(!lines[0].contains("CONFIDENTIAL_WORKER_INSTRUCTION_DO_NOT_LEAK"));
    assert!(!lines[0].contains("Approved Session Binding"));
    assert!(!lines[0].contains("gpt-5-codex"));
    assert!(!lines[0].contains(FIXED_WAKEUP_MESSAGE));

    // Coalescing while unacknowledged: answer question so RunCompleted is emitted while first claim is Submitted.
    let q_id = waiting_session
        .active_question_id
        .expect("active question_id");
    scoped_session
        .answer(
            "session-ans-1".to_string(),
            run_session.run_id.clone(),
            q_id,
            1,
            "CONFIDENTIAL_OPERATOR_ANSWER_DO_NOT_LEAK".to_string(),
        )
        .await
        .expect("answer session question");
    let _ = wait_for_run_status(&scoped_session, &run_session.run_id, RunStatus::Completed).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let logged_coalesced = fs::read_to_string(&log_file).expect("read coalesced log");
    assert_eq!(
        logged_coalesced.lines().count(),
        1,
        "must not re-notify while prior Submitted claim is unacknowledged"
    );

    // Acknowledge only the first 3 events (covering QuestionAsked at seq 3, leaving RunCompleted at seq 5 unacked).
    // This releases the first claim and triggers a second wake-up for the remaining unacknowledged event.
    let batch_first = scoped_session
        .pull_events(None, Some(3))
        .await
        .expect("pull first 3 events");
    assert_eq!(batch_first.to_seq, 3);
    scoped_session
        .ack_events("session-ack-1".to_string(), batch_first.batch_id)
        .await
        .expect("ack first 3 events");

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let st = scoped_session.status().await.expect("status");
        if st.wakeup.attempt_count == 2 && st.wakeup.status == WakeupStatus::Submitted {
            break;
        }
    }
    let st_after_release = scoped_session
        .status()
        .await
        .expect("status after 2nd wake");
    assert_eq!(st_after_release.wakeup.attempt_count, 2);
    assert_eq!(st_after_release.wakeup.status, WakeupStatus::Submitted);

    let logged_second = fs::read_to_string(&log_file).expect("read second log");
    let lines_second: Vec<&str> = logged_second.lines().collect();
    assert_eq!(lines_second.len(), 2);
    assert_eq!(
        lines_second[1],
        format!("queue|--thread|{THREAD_UUID_A}|--message|{expected_message}|5")
    );
    assert!(!lines_second[1].contains("CONFIDENTIAL_OPERATOR_ANSWER_DO_NOT_LEAK"));

    // Acknowledge remaining events -> wakeup returns to Idle.
    let batch_rest = scoped_session
        .pull_events(None, Some(50))
        .await
        .expect("pull remaining events");
    scoped_session
        .ack_events("session-ack-2".to_string(), batch_rest.batch_id)
        .await
        .expect("ack remaining events");
    let st_idle = scoped_session.status().await.expect("status idle");
    assert_eq!(st_idle.wakeup.status, WakeupStatus::Idle);

    handle.shutdown().await;

    // Timeout transitions wakeup to `Unknown` and never automatically replays.
    let timeout_state_dir = temp.path().join("state_approved_timeout");
    let slow_codex = temp.path().join("slow_codex.sh");
    let timeout_log = temp.path().join("timeout_invocations.log");
    let slow_script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$5\" >> \"{}\"\nsleep 30\n",
        timeout_log.display()
    );
    fs::write(&slow_codex, slow_script).expect("write slow_codex");
    fs::set_permissions(&slow_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    let timeout_binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&timeout_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Approved Timeout Session")
            .expect("create timeout binding");
        threadlane::descriptor::enforce_database_permissions(&timeout_state_dir)
            .expect("enforce perms");
        b
    };

    let timeout_handle = Supervisor::start(SupervisorConfig {
        state_dir: timeout_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: slow_codex,
            notify_bindings: BTreeSet::from([timeout_binding.binding_id.clone()]),
            timeout: Duration::from_millis(80),
            approved_session: Some(approved_ctx),
        }),
        workers: None,
    })
    .await
    .expect("start timeout supervisor");

    let scoped_timeout = SupervisorClient::from_state_dir(&timeout_state_dir)
        .expect("timeout client")
        .scoped(&timeout_binding.binding_id)
        .expect("scoped timeout");
    let _ = scoped_timeout
        .start_demo(
            "timeout-start".to_string(),
            "Timeout run".to_string(),
            Some(10),
        )
        .await
        .expect("start timeout demo");

    let unknown_st = wait_for_wakeup_status(&scoped_timeout, WakeupStatus::Unknown).await;
    assert_eq!(unknown_st.wakeup.attempt_count, 1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    let unknown_after = scoped_timeout.status().await.expect("status after wait");
    assert_eq!(unknown_after.wakeup.status, WakeupStatus::Unknown);
    assert_eq!(unknown_after.wakeup.attempt_count, 1);
    let timeout_lines = fs::read_to_string(&timeout_log).expect("read timeout log");
    assert_eq!(timeout_lines.lines().count(), 1);
    assert_eq!(timeout_lines.trim(), expected_message);

    timeout_handle.shutdown().await;
}

async fn wait_for_run_status(
    scoped: &ScopedClient,
    run_id: &str,
    expected: RunStatus,
) -> threadlane_core::Run {
    for _ in 0..80 {
        let run = scoped.get_run(run_id.to_string()).await.expect("get_run");
        if run.status == expected {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("timed out waiting for run `{run_id}` to reach status {expected:?}");
}

async fn wait_for_wakeup_status(
    scoped: &ScopedClient,
    expected: WakeupStatus,
) -> threadlane::StatusResponse {
    for _ in 0..80 {
        let st = scoped.status().await.expect("status");
        if st.wakeup.status == expected {
            return st;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("timed out waiting for wakeup status {expected:?}");
}
