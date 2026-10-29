use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use threadlane::{
    prepare_state_dir, CodexWakeupConfig, Error, ErrorCode, RuntimeDescriptor, ScopedClient,
    ScopedRequest, SecretToken, Supervisor, SupervisorClient, SupervisorConfig,
    ThreadlaneMcpServer, DATABASE_FILENAME, DESCRIPTOR_FILENAME, DESCRIPTOR_VERSION, LOCK_FILENAME,
    LOOPBACK_HOST, MAX_HTTP_BODY_BYTES,
};
use threadlane_core::{
    estimate_wire_response_bytes, CommandOutput, Core, QuestionStatus, RunStatus, WakeupStatus,
    FIXED_WAKEUP_MESSAGE, MAX_COMPACT_RESPONSE_BYTES, MAX_INSTRUCTION_LEN, MAX_PAGE_LIMIT,
    MAX_PAGE_RESPONSE_BYTES,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

fn fast_supervisor_config(state_dir: &Path) -> SupervisorConfig {
    SupervisorConfig {
        state_dir: state_dir.to_path_buf(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: None,
    }
}

#[tokio::test]
async fn test_supervisor_http_auth_rejection_and_secret_redaction() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");

    let descriptor = handle.descriptor().clone();
    assert_eq!(descriptor.version, DESCRIPTOR_VERSION);
    assert_eq!(descriptor.host, LOOPBACK_HOST);
    assert!(descriptor.port > 0);
    assert!(descriptor.pid > 0);

    // Ensure Debug formatting of descriptor, token, handle, and client never leaks the secret.
    let raw_secret = descriptor.token.as_str().to_string();
    assert!(!format!("{descriptor:?}").contains(&raw_secret));
    assert!(!format!("{:?}", descriptor.token).contains(&raw_secret));
    assert!(!format!("{}", descriptor.token).contains(&raw_secret));
    assert!(!format!("{handle:?}").contains(&raw_secret));

    let client = SupervisorClient::from_state_dir(&state_dir).expect("client from state dir");
    assert!(!format!("{client:?}").contains(&raw_secret));

    let base_url = descriptor.base_url().expect("base url");
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("raw reqwest client");

    // 1. Missing Authorization header on GET /v1/bindings -> 401
    let resp_no_auth = raw_http
        .get(format!("{base_url}/v1/bindings"))
        .send()
        .await
        .expect("send no auth");
    assert_eq!(resp_no_auth.status(), reqwest::StatusCode::UNAUTHORIZED);
    let err_no_auth: threadlane::ApiError = resp_no_auth.json().await.expect("parse api error");
    assert_eq!(err_no_auth.code, ErrorCode::Unauthorized);

    // 2. Non-Bearer Authorization header on POST /v1/bindings -> 401
    let resp_basic = raw_http
        .post(format!("{base_url}/v1/bindings"))
        .header(reqwest::header::AUTHORIZATION, "Basic dXNlcjpwYXNz")
        .json(&json!({
            "thread_uuid": THREAD_UUID_A,
            "label": "Unauthorized"
        }))
        .send()
        .await
        .expect("send basic auth");
    assert_eq!(resp_basic.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 3. Wrong Bearer token on scoped command endpoint -> 401
    let wrong_token = SecretToken::generate();
    let resp_wrong = raw_http
        .post(format!("{base_url}/v1/bindings/bnd_fake/command"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", wrong_token.as_str()),
        )
        .json(&json!({ "op": "status" }))
        .send()
        .await
        .expect("send wrong token");
    assert_eq!(resp_wrong.status(), reqwest::StatusCode::UNAUTHORIZED);

    // 4. Unknown route without auth still requires auth -> 401
    let resp_unknown_unauth = raw_http
        .get(format!("{base_url}/unknown"))
        .send()
        .await
        .expect("send unknown unauth");
    assert_eq!(
        resp_unknown_unauth.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    // 5. Valid authenticated client succeeds.
    let binding = client
        .create_binding(THREAD_UUID_A, "Authorized Chat")
        .await
        .expect("create binding");
    assert_eq!(binding.thread_uuid, THREAD_UUID_A);
    assert_eq!(binding.label, "Authorized Chat");

    handle.shutdown().await;
    assert!(!state_dir.join(DESCRIPTOR_FILENAME).exists());
}

#[tokio::test]
async fn test_lock_ownership_race_prevents_second_supervisor() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle_1 = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("first supervisor starts");

    let client_1 = SupervisorClient::from_state_dir(&state_dir).expect("client 1");
    let created = client_1
        .create_binding(THREAD_UUID_A, "Primary Owner")
        .await
        .expect("create binding on owner 1");

    // Second start while first is active must fail with LockBusy and leave first owner intact.
    let second_attempt = Supervisor::start(fast_supervisor_config(&state_dir)).await;
    match second_attempt {
        Err(Error::LockBusy { .. }) => {}
        other => panic!("expected LockBusy, got {other:?}"),
    }

    // First owner still serves requests normally.
    let bindings = client_1.list_bindings().await.expect("list bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].binding_id, created.binding_id);

    handle_1.shutdown().await;

    // Also race multiple concurrent supervisor starts on a fresh state directory.
    let race_dir = temp.path().join("race_state");
    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let cfg = fast_supervisor_config(&race_dir);
        join_set.spawn(async move { Supervisor::start(cfg).await });
    }

    let mut winners = Vec::new();
    let mut lock_busy_count = 0usize;
    while let Some(res) = join_set.join_next().await {
        match res.expect("task join") {
            Ok(handle) => winners.push(handle),
            Err(Error::LockBusy { .. }) => lock_busy_count += 1,
            Err(other) => panic!("unexpected error in lock race: {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one supervisor may win the lock");
    assert_eq!(lock_busy_count, 5);
    for winner in winners {
        winner.shutdown().await;
    }

    // Reopening the original state_dir after clean shutdown succeeds and preserves state.
    let handle_2 = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("reopen supervisor after shutdown");
    let client_2 = SupervisorClient::from_state_dir(&state_dir).expect("client 2");
    let bindings_after = client_2.list_bindings().await.expect("list after reopen");
    assert_eq!(bindings_after.len(), 1);
    assert_eq!(bindings_after[0].binding_id, created.binding_id);
    handle_2.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_private_permissions_and_symlink_rejection() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");

    let dir_mode = fs::symlink_metadata(&state_dir)
        .expect("stat state_dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(dir_mode, 0o700);

    for filename in [LOCK_FILENAME, DATABASE_FILENAME, DESCRIPTOR_FILENAME] {
        let file_mode = fs::symlink_metadata(state_dir.join(filename))
            .unwrap_or_else(|e| panic!("stat {filename}: {e}"))
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "expected 0600 on {filename}");
    }

    // Client must reject if descriptor permissions are widened (e.g. 0644).
    let desc_path = state_dir.join(DESCRIPTOR_FILENAME);
    fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    match SupervisorClient::from_state_dir(&state_dir) {
        Err(Error::SecurityViolation(msg)) => assert!(msg.contains("insecure permissions")),
        other => panic!("expected SecurityViolation for 0644 descriptor, got {other:?}"),
    }
    fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o600)).expect("restore 0600");

    // Client must reject if state_dir permissions are widened (e.g. 0755).
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    match SupervisorClient::from_state_dir(&state_dir) {
        Err(Error::SecurityViolation(msg)) => assert!(msg.contains("insecure permissions")),
        other => panic!("expected SecurityViolation for 0755 state_dir, got {other:?}"),
    }
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).expect("restore 0700");

    // Client must reject a symlinked state_dir.
    let symlink_state_dir = temp.path().join("state_link");
    symlink(&state_dir, &symlink_state_dir).expect("create state_dir symlink");
    match SupervisorClient::from_state_dir(&symlink_state_dir) {
        Err(Error::SecurityViolation(msg)) => assert!(msg.contains("symlink")),
        other => panic!("expected SecurityViolation for symlinked state_dir, got {other:?}"),
    }

    // Client must also reject a state_dir whose parent directory is a symlink.
    let symlink_parent = temp.path().join("parent_link");
    symlink(temp.path(), &symlink_parent).expect("create parent symlink");
    match SupervisorClient::from_state_dir(symlink_parent.join("state")) {
        Err(Error::SecurityViolation(msg)) => assert!(msg.contains("symlink")),
        other => panic!("expected SecurityViolation for symlinked parent read, got {other:?}"),
    }

    handle.shutdown().await;

    // Supervisor::start must reject a symlinked state_dir.
    match Supervisor::start(fast_supervisor_config(&symlink_state_dir)).await {
        Err(Error::SecurityViolation(msg)) => assert!(msg.contains("symlink")),
        other => panic!("expected SecurityViolation for symlinked state_dir start, got {other:?}"),
    }

    // Supervisor::start must reject a symlinked database, lock, or descriptor file.
    for target_name in [LOCK_FILENAME, DATABASE_FILENAME, DESCRIPTOR_FILENAME] {
        let sub_dir = temp.path().join(format!("symlink_test_{target_name}"));
        fs::create_dir(&sub_dir).expect("create sub_dir");
        fs::set_permissions(&sub_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let outside_file = temp.path().join(format!("outside_{target_name}"));
        fs::write(&outside_file, b"outside").expect("write outside");
        symlink(&outside_file, sub_dir.join(target_name)).expect("symlink state file");

        match Supervisor::start(fast_supervisor_config(&sub_dir)).await {
            Err(Error::SecurityViolation(msg)) => assert!(msg.contains("symlink")),
            other => panic!("expected symlink rejection for {target_name}, got {other:?}"),
        }
    }

    // Supervisor::start must reject pre-existing state files with insecure permissions (0644).
    for target_name in [LOCK_FILENAME, DATABASE_FILENAME, DESCRIPTOR_FILENAME] {
        let sub_dir = temp.path().join(format!("insecure_file_{target_name}"));
        fs::create_dir(&sub_dir).expect("create sub_dir");
        fs::set_permissions(&sub_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let file_path = sub_dir.join(target_name);
        fs::write(&file_path, b"").expect("create file");
        fs::set_permissions(&file_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");

        match Supervisor::start(fast_supervisor_config(&sub_dir)).await {
            Err(Error::SecurityViolation(msg)) => assert!(msg.contains("insecure permissions")),
            other => panic!("expected insecure file rejection for {target_name}, got {other:?}"),
        }
    }

    // Descriptor validation must reject non-loopback host and unsupported version.
    let bad_host_desc = RuntimeDescriptor {
        version: DESCRIPTOR_VERSION,
        host: "0.0.0.0".to_string(),
        port: 9999,
        pid: 123,
        token: SecretToken::generate(),
    };
    assert!(matches!(
        SupervisorClient::from_descriptor(bad_host_desc),
        Err(Error::SecurityViolation(_))
    ));

    let bad_ver_desc = RuntimeDescriptor {
        version: 99,
        host: LOOPBACK_HOST.to_string(),
        port: 9999,
        pid: 123,
        token: SecretToken::generate(),
    };
    assert!(matches!(
        SupervisorClient::from_descriptor(bad_ver_desc),
        Err(Error::SecurityViolation(_))
    ));
}

#[tokio::test]
async fn test_two_binding_isolation_compact_status_and_idempotency_across_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let binding_a = client
        .create_binding(THREAD_UUID_A, "Chat A")
        .await
        .expect("bind A");
    let binding_b = client
        .create_binding(THREAD_UUID_B, "Chat B")
        .await
        .expect("bind B");

    let scoped_a = client
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped A");
    let scoped_b = client
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped B");

    // Start demo run on Binding A with short step delay.
    let started_run = scoped_a
        .start_demo(
            "cmd-start-a1".to_string(),
            "Implement module A safely".to_string(),
            Some(20),
        )
        .await
        .expect("start demo A");
    assert_eq!(started_run.status, RunStatus::Running);
    assert_eq!(started_run.revision, 1);

    // Wait for run on A to reach WaitingInput.
    let waiting_run =
        wait_for_run_status(&scoped_a, &started_run.run_id, RunStatus::WaitingInput).await;
    let question_id_rev1 = waiting_run
        .active_question_id
        .clone()
        .expect("active question on rev 1");

    // Verify compact status on A has untrusted_data = true, wake-up Pending, and RunSummary.
    let status_a = scoped_a.status().await.expect("status A");
    assert!(status_a.untrusted_data);
    assert_eq!(status_a.binding.binding_id, binding_a.binding_id);
    assert_eq!(status_a.wakeup.status, WakeupStatus::Pending);
    assert_eq!(status_a.total_runs, 1);
    assert_eq!(status_a.active_runs, 1);
    assert_eq!(status_a.recent_runs.len(), 1);
    assert!(status_a.recent_runs[0].untrusted_data);
    assert_eq!(
        status_a.recent_runs[0].active_question_id.as_deref(),
        Some(question_id_rev1.as_str())
    );

    // Cross-binding isolation: Binding B cannot read, answer, steer, or cancel Binding A's run.
    for bad_res in [
        scoped_b.get_run(started_run.run_id.clone()).await.err(),
        scoped_b
            .answer(
                "b-ans".to_string(),
                started_run.run_id.clone(),
                question_id_rev1.clone(),
                1,
                "cross-binding answer".to_string(),
            )
            .await
            .err(),
        scoped_b
            .steer(
                "b-steer".to_string(),
                started_run.run_id.clone(),
                1,
                "cross-binding steer".to_string(),
            )
            .await
            .err(),
        scoped_b
            .cancel("b-cancel".to_string(), started_run.run_id.clone(), None)
            .await
            .err(),
    ] {
        match bad_res {
            Some(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::RunNotFound),
            other => panic!("expected RunNotFound for cross-binding access, got {other:?}"),
        }
    }

    // Pull events on A and verify B cannot acknowledge A's batch_id.
    let batch_a = scoped_a.pull_events(None, Some(50)).await.expect("pull A");
    assert_eq!(batch_a.events.len(), 3); // RunStarted, RunProgress, QuestionAsked
    assert!(batch_a.events.iter().all(|e| e.payload.untrusted_data));

    match scoped_b
        .ack_events("b-ack-cross".to_string(), batch_a.batch_id.clone())
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::InvalidAck),
        other => panic!("expected InvalidAck for cross-binding batch, got {other:?}"),
    }

    // Steer with stale revision (e.g. revision 99) fails with StaleRevision.
    match scoped_a
        .steer(
            "a-steer-stale".to_string(),
            started_run.run_id.clone(),
            99,
            "wrong revision".to_string(),
        )
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::StaleRevision),
        other => panic!("expected StaleRevision, got {other:?}"),
    }

    // Valid steer at revision 1 advances to revision 2 and supersedes question_id_rev1.
    let steered_run = scoped_a
        .steer(
            "cmd-steer-a1".to_string(),
            started_run.run_id.clone(),
            1,
            "Revised instruction for rev 2".to_string(),
        )
        .await
        .expect("steer rev 1 -> 2");
    assert_eq!(steered_run.revision, 2);
    assert_eq!(steered_run.status, RunStatus::Running);
    assert!(steered_run.active_question.is_none());
    let qs_after_steer = scoped_a
        .list_run_questions(started_run.run_id.clone(), None, None)
        .await
        .expect("list questions after steer");
    assert_eq!(
        qs_after_steer.questions[0].status,
        QuestionStatus::Superseded
    );
    assert_eq!(qs_after_steer.questions[0].superseded_by_revision, Some(2));

    // Wait for revision 2 to reach WaitingInput and pose a new question.
    let waiting_rev2 =
        wait_for_run_status(&scoped_a, &started_run.run_id, RunStatus::WaitingInput).await;
    let question_id_rev2 = waiting_rev2
        .active_question_id
        .clone()
        .expect("active question on rev 2");
    assert_ne!(question_id_rev1, question_id_rev2);

    // Answering the superseded question_id_rev1 even with expected_revision = 2 fails with StaleQuestion.
    match scoped_a
        .answer(
            "a-ans-superseded".to_string(),
            started_run.run_id.clone(),
            question_id_rev1.clone(),
            2,
            "late answer".to_string(),
        )
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::StaleQuestion),
        other => panic!("expected StaleQuestion, got {other:?}"),
    }

    // Restart supervisor and verify idempotent retry of `cmd-start-a1` and `cmd-steer-a1`
    // returns the original responses, while reusing `cmd-start-a1` for different params fails.
    handle.shutdown().await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let handle_restarted = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("restart supervisor");

    let retry_start = scoped_a
        .start_demo(
            "cmd-start-a1".to_string(),
            "Implement module A safely".to_string(),
            Some(20),
        )
        .await
        .expect("idempotent retry start_demo after restart");
    assert_eq!(retry_start, started_run);

    let retry_steer = scoped_a
        .steer(
            "cmd-steer-a1".to_string(),
            started_run.run_id.clone(),
            1,
            "Revised instruction for rev 2".to_string(),
        )
        .await
        .expect("idempotent retry steer after restart");
    assert_eq!(retry_steer, steered_run);

    match scoped_a
        .start_demo(
            "cmd-start-a1".to_string(),
            "Different instruction with same key".to_string(),
            Some(20),
        )
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::IdempotencyConflict),
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }

    // Answer revision 2 question and verify run completes.
    let completing_run = scoped_a
        .answer(
            "cmd-ans-rev2".to_string(),
            started_run.run_id.clone(),
            question_id_rev2,
            2,
            "Confirmed for revision 2".to_string(),
        )
        .await
        .expect("answer rev 2");
    assert_eq!(completing_run.status, RunStatus::Completing);

    let completed_run =
        wait_for_run_status(&scoped_a, &started_run.run_id, RunStatus::Completed).await;
    assert_eq!(completed_run.revision, 2);
    assert!(completed_run.active_question.is_none());
    let completed_revs = scoped_a
        .list_run_revisions(started_run.run_id.clone(), None, None)
        .await
        .expect("list completed revisions");
    assert_eq!(completed_revs.revisions.len(), 2);
    let completed_qs = scoped_a
        .list_run_questions(started_run.run_id.clone(), None, None)
        .await
        .expect("list completed questions");
    assert_eq!(completed_qs.questions.len(), 2);

    handle_restarted.shutdown().await;
}

#[tokio::test]
async fn test_stdio_mcp_initialize_list_and_scoped_tool_calls() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let binding_a = client
        .create_binding(THREAD_UUID_A, "MCP Bound Chat A")
        .await
        .expect("bind A");
    let binding_b = client
        .create_binding(THREAD_UUID_B, "Other Chat B")
        .await
        .expect("bind B");

    // Start a run on Binding B to verify MCP scoped to A cannot access it.
    let run_on_b = client
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped B")
        .start_demo("b-key-1".to_string(), "Task on B".to_string(), Some(50))
        .await
        .expect("start demo on B");

    let mut mcp_child = spawn_mcp_child(&state_dir, &binding_a.binding_id);
    let mut stdin = mcp_child.stdin.take().expect("mcp stdin");
    let stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut reader = BufReader::new(stdout);

    // 1. MCP initialize handshake
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "threadlane-test-client", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let init_resp = read_json_rpc_response(&mut reader, 1).await;
    assert_eq!(
        init_resp["result"]["serverInfo"]["name"],
        json!("threadlane")
    );
    assert!(init_resp["result"]["capabilities"]["tools"].is_object());

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
    )
    .await;

    // 2. tools/list: verify exact 11 scoped tools, annotations, and no binding_id/thread_uuid/now_ms params.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await;
    let list_resp = read_json_rpc_response(&mut reader, 2).await;
    let tools = list_resp["result"]["tools"]
        .as_array()
        .expect("tools array");
    let tool_names: BTreeSet<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().expect("tool name"))
        .collect();
    let expected_names: BTreeSet<&str> = [
        "status",
        "get_run",
        "list_runs",
        "list_run_revisions",
        "list_run_questions",
        "list_activity",
        "start_demo",
        "pull_events",
        "ack_events",
        "answer",
        "steer",
        "cancel",
        "start_worker",
        "get_worker",
        "cancel_worker",
    ]
    .into_iter()
    .collect();
    assert_eq!(tool_names, expected_names);

    for tool_def in tools {
        assert!(
            tool_def["annotations"].is_object(),
            "tool {} missing annotations",
            tool_def["name"]
        );
        assert_eq!(
            tool_def["annotations"]["openWorldHint"],
            json!(false),
            "tool {} should have openWorldHint=false",
            tool_def["name"]
        );
        let props = &tool_def["inputSchema"]["properties"];
        if let Some(map) = props.as_object() {
            assert!(!map.contains_key("binding_id"));
            assert!(!map.contains_key("thread_uuid"));
            assert!(!map.contains_key("now_ms"));
        }
    }

    // 3. Unknown fields (such as injected `binding_id` or `now_ms`) must be rejected by
    // `#[serde(deny_unknown_fields)]`. In rmcp 3.8.0 (`router/tool.rs::into_tool_argument_error`),
    // `INVALID_PARAMS` from `Parameters<T>` deserialization is returned as a `CallToolResult`
    // with `isError: true` and `"failed to deserialize parameters: unknown field ..."` text,
    // without executing the tool handler or creating any run.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "start_demo",
                "arguments": {
                    "command_key": "k-bad",
                    "instruction": "hello",
                    "binding_id": binding_b.binding_id,
                    "now_ms": 12345
                }
            }
        }),
    )
    .await;
    let unknown_field_resp = read_json_rpc_response(&mut reader, 3).await;
    assert_eq!(unknown_field_resp["result"]["isError"], json!(true));
    let unknown_err_text = unknown_field_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("tool argument error text");
    assert!(
        unknown_err_text.contains("failed to deserialize parameters"),
        "expected rmcp parameter deserialization error, got: {unknown_err_text}"
    );
    assert!(
        unknown_err_text.contains("unknown field"),
        "expected unknown field error, got: {unknown_err_text}"
    );

    // Also verify `status` rejects injected `binding_id` or `now_ms`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {
                    "binding_id": binding_b.binding_id,
                    "now_ms": 99999
                }
            }
        }),
    )
    .await;
    let unknown_status_resp = read_json_rpc_response(&mut reader, 30).await;
    assert_eq!(unknown_status_resp["result"]["isError"], json!(true));
    assert!(unknown_status_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .contains("unknown field"));

    // Prove the rejected `start_demo` call with injected `binding_id`/`now_ms` had no effect
    // and created no run on either binding_a or binding_b.
    let status_a_before = client
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped A")
        .status()
        .await
        .expect("status A before");
    assert_eq!(
        status_a_before.total_runs, 0,
        "rejected tool call must not create a run on bound chat A"
    );
    let status_b_before = client
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped B")
        .status()
        .await
        .expect("status B before");
    assert_eq!(
        status_b_before.total_runs, 1,
        "rejected tool call must not create an extra run on chat B"
    );

    // 4. Cross-binding run lookup via MCP returns structured domain error with isError: true.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "get_run",
                "arguments": {
                    "run_id": run_on_b.run_id
                }
            }
        }),
    )
    .await;
    let cross_run_resp = read_json_rpc_response(&mut reader, 4).await;
    assert_eq!(cross_run_resp["result"]["isError"], json!(true));
    assert_eq!(
        cross_run_resp["result"]["structuredContent"]["error"]["code"],
        json!("run_not_found")
    );

    // 5. Full 8-tool lifecycle over stdio MCP:
    //    `start_demo` -> `get_run` -> `pull_events` -> `ack_events` -> `steer` -> `answer` -> `status` -> `cancel`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "start_demo",
                "arguments": {
                    "command_key": "mcp-start-1",
                    "instruction": "MCP simulated demo task",
                    "step_delay_ms": 15
                }
            }
        }),
    )
    .await;
    let start_resp = read_json_rpc_response(&mut reader, 5).await;
    assert_eq!(start_resp["result"]["isError"], json!(false));
    let start_sc = &start_resp["result"]["structuredContent"];
    assert_eq!(start_sc["untrusted_data"], json!(true));
    assert_eq!(start_sc["demo_only"], json!(true));
    let run_id = start_sc["data"]["run_id"]
        .as_str()
        .expect("run_id")
        .to_string();

    // Wait for WaitingInput (revision 1) via MCP `get_run`.
    let mut active_question_id_rev1 = None;
    for attempt in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let req_id = 100 + attempt;
        send_json_rpc(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "get_run",
                    "arguments": { "run_id": run_id }
                }
            }),
        )
        .await;
        let run_resp = read_json_rpc_response(&mut reader, req_id).await;
        let run_data = &run_resp["result"]["structuredContent"]["data"];
        if run_data["status"] == json!("waiting_input") {
            active_question_id_rev1 = run_data["active_question_id"].as_str().map(str::to_owned);
            break;
        }
    }
    let question_id_rev1 = active_question_id_rev1.expect("question asked on rev 1 via MCP");

    // Pull events and acknowledge them via MCP `pull_events` and `ack_events`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "pull_events",
                "arguments": { "limit": 20 }
            }
        }),
    )
    .await;
    let pull_resp = read_json_rpc_response(&mut reader, 6).await;
    let batch_id = pull_resp["result"]["structuredContent"]["data"]["batch_id"]
        .as_str()
        .expect("batch_id")
        .to_string();

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "ack_events",
                "arguments": {
                    "command_key": "mcp-ack-1",
                    "batch_id": batch_id
                }
            }
        }),
    )
    .await;
    let ack_resp = read_json_rpc_response(&mut reader, 7).await;
    assert_eq!(ack_resp["result"]["isError"], json!(false));
    assert_eq!(
        ack_resp["result"]["structuredContent"]["data"]["advanced"],
        json!(true)
    );

    // Steer the run via MCP `steer` from revision 1 to revision 2.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "steer",
                "arguments": {
                    "command_key": "mcp-steer-1",
                    "run_id": run_id,
                    "expected_revision": 1,
                    "instruction": "Steered MCP instruction rev 2"
                }
            }
        }),
    )
    .await;
    let steer_resp = read_json_rpc_response(&mut reader, 8).await;
    assert_eq!(steer_resp["result"]["isError"], json!(false));
    let steered_data = &steer_resp["result"]["structuredContent"]["data"];
    assert_eq!(steered_data["revision"], json!(2));
    assert_eq!(steered_data["status"], json!("running"));
    assert_eq!(steered_data["active_question"], json!(null));

    // Verify superseded question and revision history via MCP `list_run_questions` and `list_run_revisions`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 81,
            "method": "tools/call",
            "params": {
                "name": "list_run_questions",
                "arguments": { "run_id": run_id }
            }
        }),
    )
    .await;
    let list_qs_resp = read_json_rpc_response(&mut reader, 81).await;
    assert_eq!(list_qs_resp["result"]["isError"], json!(false));
    assert_eq!(
        list_qs_resp["result"]["structuredContent"]["data"]["questions"][0]["status"],
        json!("superseded")
    );

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 82,
            "method": "tools/call",
            "params": {
                "name": "list_run_revisions",
                "arguments": { "run_id": run_id }
            }
        }),
    )
    .await;
    let list_revs_resp = read_json_rpc_response(&mut reader, 82).await;
    assert_eq!(list_revs_resp["result"]["isError"], json!(false));
    assert_eq!(
        list_revs_resp["result"]["structuredContent"]["data"]["revisions"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 83,
            "method": "tools/call",
            "params": {
                "name": "list_runs",
                "arguments": { "limit": 10 }
            }
        }),
    )
    .await;
    let list_runs_resp = read_json_rpc_response(&mut reader, 83).await;
    assert_eq!(list_runs_resp["result"]["isError"], json!(false));
    assert_eq!(
        list_runs_resp["result"]["structuredContent"]["data"]["runs"][0]["run_id"],
        json!(run_id)
    );

    // Wait for revision 2 to pose a fresh question via MCP `get_run`.
    let mut active_question_id_rev2 = None;
    for attempt in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let req_id = 150 + attempt;
        send_json_rpc(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "get_run",
                    "arguments": { "run_id": run_id }
                }
            }),
        )
        .await;
        let run_resp = read_json_rpc_response(&mut reader, req_id).await;
        let run_data = &run_resp["result"]["structuredContent"]["data"];
        if run_data["status"] == json!("waiting_input") && run_data["revision"] == json!(2) {
            active_question_id_rev2 = run_data["active_question_id"].as_str().map(str::to_owned);
            break;
        }
    }
    let question_id_rev2 = active_question_id_rev2.expect("question asked on rev 2 via MCP");
    assert_ne!(question_id_rev1, question_id_rev2);

    // Answer the revision 2 question via MCP `answer`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "answer",
                "arguments": {
                    "command_key": "mcp-answer-2",
                    "run_id": run_id,
                    "question_id": question_id_rev2,
                    "expected_revision": 2,
                    "answer": "Approved in MCP test rev 2"
                }
            }
        }),
    )
    .await;
    let ans_resp = read_json_rpc_response(&mut reader, 9).await;
    assert_eq!(ans_resp["result"]["isError"], json!(false));

    // Wait for Completed via MCP `status`.
    let mut saw_completed = false;
    for attempt in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let req_id = 200 + attempt;
        send_json_rpc(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "status",
                    "arguments": {}
                }
            }),
        )
        .await;
        let st_resp = read_json_rpc_response(&mut reader, req_id).await;
        let st_data = &st_resp["result"]["structuredContent"]["data"];
        assert_eq!(
            st_data["binding"]["binding_id"],
            json!(binding_a.binding_id)
        );
        if st_data["recent_runs"][0]["status"] == json!("completed") {
            saw_completed = true;
            break;
        }
    }
    assert!(saw_completed, "expected MCP run to reach completed");

    // Start a second run and exercise MCP `cancel` to complete all 8 tools.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "start_demo",
                "arguments": {
                    "command_key": "mcp-start-cancel",
                    "instruction": "Run to be cancelled via MCP",
                    "step_delay_ms": 200
                }
            }
        }),
    )
    .await;
    let start_cancel_resp = read_json_rpc_response(&mut reader, 10).await;
    assert_eq!(start_cancel_resp["result"]["isError"], json!(false));
    let cancel_run_id = start_cancel_resp["result"]["structuredContent"]["data"]["run_id"]
        .as_str()
        .expect("cancel_run_id")
        .to_string();

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "cancel",
                "arguments": {
                    "command_key": "mcp-cancel-1",
                    "run_id": cancel_run_id,
                    "reason": "User cancelled via MCP"
                }
            }
        }),
    )
    .await;
    let cancel_resp = read_json_rpc_response(&mut reader, 11).await;
    assert_eq!(cancel_resp["result"]["isError"], json!(false));
    assert_eq!(
        cancel_resp["result"]["structuredContent"]["data"]["status"],
        json!("cancelled")
    );

    drop(stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_codex_wakeup_exact_args_allowlist_timeout_and_no_repeat_dispatch() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");

    // 1. Config validation checks: relative path and empty allowlist are rejected.
    let rel_cfg = CodexWakeupConfig {
        executable: PathBuf::from("relative/codex"),
        notify_bindings: BTreeSet::from(["bnd_123".to_string()]),
        timeout: Duration::from_millis(500),
    };
    assert!(matches!(rel_cfg.validate(), Err(Error::InvalidConfig(_))));

    // 2. Exact argument array, allowlist enforcement, and no repeat dispatch without ack.
    let state_dir = temp.path().join("state_codex");
    let log_file = temp.path().join("codex_invocations.jsonl");
    let fake_codex = temp.path().join("fake_codex.sh");

    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$1|$2|$3|$4|$5|$#\" >> \"{}\"\nexit 0\n",
        log_file.display()
    );
    fs::write(&fake_codex, script).expect("write fake_codex");
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    // Pre-create two bindings via Core before starting supervisor with allowlist = [binding_allowed].
    let (binding_allowed, binding_unlisted) = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let allowed = core
            .create_binding(THREAD_UUID_A, "Sensitive Allowed Label")
            .expect("create allowed");
        let unlisted = core
            .create_binding(THREAD_UUID_B, "Unlisted Label")
            .expect("create unlisted");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce db perms");
        (allowed, unlisted)
    };

    let sup_cfg = SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex.clone(),
            notify_bindings: BTreeSet::from([binding_allowed.binding_id.clone()]),
            timeout: Duration::from_millis(2_000),
        }),
        workers: None,
    };
    let handle = Supervisor::start(sup_cfg)
        .await
        .expect("start supervisor with codex");

    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped_allowed = client
        .clone()
        .scoped(&binding_allowed.binding_id)
        .expect("scoped allowed");
    let scoped_unlisted = client
        .clone()
        .scoped(&binding_unlisted.binding_id)
        .expect("scoped unlisted");

    // Start demo runs on both bindings.
    let run_allowed = scoped_allowed
        .start_demo(
            "allow-start".to_string(),
            "Secret instruction that must never appear in wakeup argv".to_string(),
            Some(15),
        )
        .await
        .expect("start allowed");
    let run_unlisted = scoped_unlisted
        .start_demo(
            "unlisted-start".to_string(),
            "Unlisted instruction".to_string(),
            Some(15),
        )
        .await
        .expect("start unlisted");

    let waiting_allowed = wait_for_run_status(
        &scoped_allowed,
        &run_allowed.run_id,
        RunStatus::WaitingInput,
    )
    .await;
    let _waiting_unlisted = wait_for_run_status(
        &scoped_unlisted,
        &run_unlisted.run_id,
        RunStatus::WaitingInput,
    )
    .await;

    // Wait for `binding_allowed` wakeup status to become `Submitted`.
    let status_allowed = wait_for_wakeup_status(&scoped_allowed, WakeupStatus::Submitted).await;
    assert_eq!(status_allowed.wakeup.attempt_count, 1);

    // Unlisted binding must remain `Pending` and never be claimed by the dispatcher.
    let status_unlisted = scoped_unlisted.status().await.expect("status unlisted");
    assert_eq!(status_unlisted.wakeup.status, WakeupStatus::Pending);
    assert_eq!(status_unlisted.wakeup.attempt_count, 0);

    // Verify exact logged argv for `binding_allowed`: 5 args, no model flags, no worker prose or label.
    let logged = fs::read_to_string(&log_file).expect("read codex log");
    let lines: Vec<&str> = logged.lines().collect();
    assert_eq!(lines.len(), 1, "expected exactly 1 wakeup invocation");
    assert_eq!(
        lines[0],
        format!("queue|--thread|{THREAD_UUID_A}|--message|{FIXED_WAKEUP_MESSAGE}|5")
    );
    assert!(!lines[0].contains("Sensitive Allowed Label"));
    assert!(!lines[0].contains("Secret instruction"));

    // Answer the question on `binding_allowed` WITHOUT acknowledging the QuestionAsked event yet.
    // The subsequent RunCompleted event must coalesce into the existing notice without re-invoking Codex!
    let q_id = waiting_allowed
        .active_question_id
        .expect("allowed question_id");
    scoped_allowed
        .answer(
            "allow-ans".to_string(),
            run_allowed.run_id.clone(),
            q_id,
            1,
            "Answer before ack".to_string(),
        )
        .await
        .expect("answer allowed");
    let _ = wait_for_run_status(&scoped_allowed, &run_allowed.run_id, RunStatus::Completed).await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    let logged_after_complete = fs::read_to_string(&log_file).expect("read codex log 2");
    assert_eq!(
        logged_after_complete.lines().count(),
        1,
        "must not re-dispatch while prior Submitted claim is unacknowledged"
    );

    // Pull only the first 3 events (covering QuestionAsked at seq 3, leaving RunCompleted at seq 5 unacked)
    // and acknowledge that batch. This releases the prior claim coverage and triggers exactly 1 new wake-up
    // for the remaining unacknowledged RunCompleted event!
    let batch_1 = scoped_allowed
        .pull_events(None, Some(3))
        .await
        .expect("pull first 3 events");
    assert_eq!(batch_1.to_seq, 3);
    scoped_allowed
        .ack_events("allow-ack-1".to_string(), batch_1.batch_id)
        .await
        .expect("ack first 3 events");

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let st = scoped_allowed.status().await.expect("status");
        if st.wakeup.attempt_count == 2 && st.wakeup.status == WakeupStatus::Submitted {
            break;
        }
    }
    let st_second = scoped_allowed
        .status()
        .await
        .expect("status after 2nd wake");
    assert_eq!(st_second.wakeup.attempt_count, 2);
    assert_eq!(st_second.wakeup.status, WakeupStatus::Submitted);

    // Acknowledge the remaining events and verify wakeup returns to Idle.
    let batch_2 = scoped_allowed
        .pull_events(None, Some(50))
        .await
        .expect("pull remaining events");
    scoped_allowed
        .ack_events("allow-ack-2".to_string(), batch_2.batch_id)
        .await
        .expect("ack remaining events");
    let st_idle = scoped_allowed.status().await.expect("status idle");
    assert_eq!(st_idle.wakeup.status, WakeupStatus::Idle);

    handle.shutdown().await;

    // 3. Timeout handling: slow fake Codex script is killed & reaped, recorded as `Unknown`, and never replayed.
    let timeout_state_dir = temp.path().join("state_timeout");
    let slow_codex = temp.path().join("slow_codex.sh");
    let timeout_count_file = temp.path().join("timeout_invocations.txt");
    let slow_script = format!(
        "#!/bin/sh\necho invoked >> \"{}\"\nsleep 30\n",
        timeout_count_file.display()
    );
    fs::write(&slow_codex, slow_script).expect("write slow_codex");
    fs::set_permissions(&slow_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    let binding_timeout = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&timeout_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Timeout Chat")
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
            notify_bindings: BTreeSet::from([binding_timeout.binding_id.clone()]),
            timeout: Duration::from_millis(80),
        }),
        workers: None,
    })
    .await
    .expect("start timeout supervisor");

    let scoped_timeout = SupervisorClient::from_state_dir(&timeout_state_dir)
        .expect("timeout client")
        .scoped(&binding_timeout.binding_id)
        .expect("scoped timeout");

    let _ = scoped_timeout
        .start_demo("t-start".to_string(), "Timeout test".to_string(), Some(10))
        .await
        .expect("start timeout demo");

    let unknown_status = wait_for_wakeup_status(&scoped_timeout, WakeupStatus::Unknown).await;
    assert_eq!(unknown_status.wakeup.attempt_count, 1);
    assert!(unknown_status
        .wakeup
        .last_error
        .as_deref()
        .unwrap_or_default()
        .contains("timed out"));

    // Wait several more ticks to confirm `Unknown` is never replayed.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let unknown_after = scoped_timeout.status().await.expect("status after wait");
    assert_eq!(unknown_after.wakeup.status, WakeupStatus::Unknown);
    assert_eq!(unknown_after.wakeup.attempt_count, 1);
    let timeout_invocations = fs::read_to_string(&timeout_count_file).expect("read timeout count");
    assert_eq!(timeout_invocations.lines().count(), 1);

    timeout_handle.shutdown().await;

    // 4. Spawn failure (non-executable 0600 file) records `Failed` and never replays.
    let fail_state_dir = temp.path().join("state_fail");
    let non_exec_codex = temp.path().join("non_exec_codex.sh");
    fs::write(&non_exec_codex, "#!/bin/sh\nexit 0\n").expect("write non_exec");
    fs::set_permissions(&non_exec_codex, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

    let binding_fail = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&fail_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Fail Chat")
            .expect("create fail binding");
        threadlane::descriptor::enforce_database_permissions(&fail_state_dir)
            .expect("enforce perms");
        b
    };

    let fail_handle = Supervisor::start(SupervisorConfig {
        state_dir: fail_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: non_exec_codex,
            notify_bindings: BTreeSet::from([binding_fail.binding_id.clone()]),
            timeout: Duration::from_millis(500),
        }),
        workers: None,
    })
    .await
    .expect("start fail supervisor");

    let scoped_fail = SupervisorClient::from_state_dir(&fail_state_dir)
        .expect("fail client")
        .scoped(&binding_fail.binding_id)
        .expect("scoped fail");
    let _ = scoped_fail
        .start_demo(
            "f-start".to_string(),
            "Spawn fail test".to_string(),
            Some(10),
        )
        .await
        .expect("start fail demo");

    let failed_status = wait_for_wakeup_status(&scoped_fail, WakeupStatus::Failed).await;
    assert_eq!(failed_status.wakeup.attempt_count, 1);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let failed_after = scoped_fail.status().await.expect("status after fail wait");
    assert_eq!(failed_after.wakeup.attempt_count, 1);

    fail_handle.shutdown().await;
}

#[tokio::test]
async fn test_cli_end_to_end_simulated_demo_loop() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("cli_state");
    let bin = env!("CARGO_BIN_EXE_threadlane");

    let mut serve_child = Command::new(bin)
        .arg("serve")
        .arg("--state-dir")
        .arg(&state_dir)
        .arg("--tick-interval-ms")
        .arg("15")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn threadlane serve");

    // Wait for supervisor.json to appear.
    let desc_path = state_dir.join(DESCRIPTOR_FILENAME);
    for _ in 0..100 {
        if desc_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(desc_path.exists(), "supervisor.json should be created");

    // 1. `threadlane bind`
    let bind_out = run_cli_cmd(
        bin,
        &[
            "bind",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--thread",
            THREAD_UUID_A,
            "--label",
            "CLI Smoke Chat",
        ],
    )
    .await;
    let binding_id = bind_out["binding_id"]
        .as_str()
        .expect("binding_id")
        .to_string();

    // 2. `threadlane call --request start_demo`
    let start_req = serde_json::to_string(&ScopedRequest::StartDemo {
        command_key: "cli-start-1".to_string(),
        instruction: "Simulated CLI task".to_string(),
        step_delay_ms: Some(15),
    })
    .unwrap();
    let start_out = run_cli_cmd(
        bin,
        &[
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding_id,
            "--request",
            &start_req,
        ],
    )
    .await;
    let run_id = start_out["data"]["run_id"]
        .as_str()
        .expect("run_id")
        .to_string();

    // 3. Poll `threadlane status` until active_question_id is present.
    let mut question_id = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let status_out = run_cli_cmd(
            bin,
            &[
                "status",
                "--state-dir",
                state_dir.to_str().unwrap(),
                "--binding",
                &binding_id,
            ],
        )
        .await;
        if let Some(q) = status_out["recent_runs"][0]["active_question_id"].as_str() {
            question_id = Some(q.to_string());
            break;
        }
    }
    let question_id = question_id.expect("question posed in CLI loop");

    // 4. Answer question via `threadlane call`.
    let ans_req = serde_json::to_string(&ScopedRequest::Answer {
        command_key: "cli-ans-1".to_string(),
        run_id: run_id.clone(),
        question_id,
        expected_revision: 1,
        answer: "Proceed with simulated completion".to_string(),
    })
    .unwrap();
    let _ = run_cli_cmd(
        bin,
        &[
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding_id,
            "--request",
            &ans_req,
        ],
    )
    .await;

    // 5. Wait for run completion via `threadlane status`.
    let mut completed = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let status_out = run_cli_cmd(
            bin,
            &[
                "status",
                "--state-dir",
                state_dir.to_str().unwrap(),
                "--binding",
                &binding_id,
            ],
        )
        .await;
        if status_out["recent_runs"][0]["status"] == json!("completed") {
            completed = true;
            break;
        }
    }
    assert!(completed, "CLI demo run should reach completed");

    cleanup_child(&mut serve_child).await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_persistence_failure_stops_mutations_and_does_not_retry_wakeup() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");

    // Case 1: Failure while persisting a wake-up result (`record_wakeup_result`) must
    // transition the supervisor into a faulted state, stop subsequent mutations, and
    // NEVER retry the uncertain external wake-up side effect.
    let wake_state_dir = temp.path().join("wake_fault_state");
    let count_file = temp.path().join("wake_fault_count.txt");
    let fake_codex = temp.path().join("fake_codex_fault.sh");
    let script = format!(
        "#!/bin/sh\necho invoked >> \"{}\"\nexit 0\n",
        count_file.display()
    );
    fs::write(&fake_codex, script).expect("write fake_codex");
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    let binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&wake_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Wake Fault Chat")
            .expect("create binding");
        drop(core);

        // Install a controlled SQLite trigger in the test database fixture so that
        // `claim_wakeup` (`status = 'in_flight'`) succeeds, the external fake Codex
        // process runs once, and `record_wakeup_result` (`resolved_at_ms IS NOT NULL`)
        // fails with a storage error.
        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite for trigger");
        conn.execute_batch(
            "CREATE TRIGGER fail_wakeup_result_write
             BEFORE UPDATE ON wakeups
             WHEN NEW.resolved_at_ms IS NOT NULL
             BEGIN
                 SELECT RAISE(ABORT, 'simulated disk failure on wakeup result');
             END;",
        )
        .expect("create trigger");
        drop(conn);
        threadlane::descriptor::enforce_database_permissions(&wake_state_dir)
            .expect("enforce perms");
        b
    };

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: wake_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex,
            notify_bindings: BTreeSet::from([binding.binding_id.clone()]),
            timeout: Duration::from_millis(500),
        }),
        workers: None,
    })
    .await
    .expect("start supervisor");

    let client = SupervisorClient::from_state_dir(&wake_state_dir).expect("client");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped client");

    let _ = scoped
        .start_demo(
            "wf-start".to_string(),
            "Trigger wakeup".to_string(),
            Some(10),
        )
        .await
        .expect("start demo");

    // Wait for the supervisor to record the fatal persistence failure.
    let mut saw_fatal = false;
    for _ in 0..80 {
        if let Some(err) = handle.fatal_error() {
            assert_eq!(err.code, ErrorCode::StorageError);
            saw_fatal = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        saw_fatal,
        "supervisor should report fatal persistence error"
    );

    // Wait additional time to prove the background loop exited and never retried the wake-up.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let invocations = fs::read_to_string(&count_file).expect("read invocation count");
    assert_eq!(
        invocations.lines().count(),
        1,
        "external wake-up must not be retried when result persistence fails"
    );

    // Subsequent HTTP requests must fail closed with StorageError rather than claiming healthy.
    match scoped.status().await {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::StorageError),
        other => {
            panic!("expected Remote(StorageError) after fatal persistence failure, got {other:?}")
        }
    }
    match client
        .create_binding(THREAD_UUID_B, "New Binding After Fault")
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::StorageError),
        other => panic!("expected Remote(StorageError) on mutation after fault, got {other:?}"),
    }

    handle.shutdown().await;

    // Case 2: Persistence failure during background `tick` faults the supervisor and stops new mutations.
    let tick_state_dir = temp.path().join("tick_fault_state");
    {
        let db_path =
            threadlane::descriptor::prepare_database_path(&tick_state_dir).expect("prep db");
        let core = Core::open(&db_path).expect("open core");
        drop(core);

        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite for tick trigger");
        conn.execute_batch(
            "CREATE TRIGGER fail_tick_question_insert
             BEFORE INSERT ON questions
             BEGIN
                 SELECT RAISE(ABORT, 'simulated disk failure during tick');
             END;",
        )
        .expect("create tick trigger");
        drop(conn);
        threadlane::descriptor::enforce_database_permissions(&tick_state_dir)
            .expect("enforce perms");
    }

    let tick_handle = Supervisor::start(fast_supervisor_config(&tick_state_dir))
        .await
        .expect("start tick supervisor");
    let tick_client = SupervisorClient::from_state_dir(&tick_state_dir).expect("tick client");
    let tick_binding = tick_client
        .create_binding(THREAD_UUID_A, "Tick Fault Chat")
        .await
        .expect("create tick binding");
    let tick_scoped = tick_client
        .clone()
        .scoped(&tick_binding.binding_id)
        .expect("scoped tick");

    let _ = tick_scoped
        .start_demo(
            "tf-start".to_string(),
            "Will fail on tick".to_string(),
            Some(10),
        )
        .await
        .expect("start demo before tick failure");

    let mut tick_faulted = false;
    for _ in 0..80 {
        if let Some(err) = tick_handle.fatal_error() {
            assert_eq!(err.code, ErrorCode::StorageError);
            tick_faulted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        tick_faulted,
        "supervisor should fault when background tick fails"
    );

    match tick_scoped
        .start_demo(
            "tf-start-2".to_string(),
            "Must be rejected".to_string(),
            Some(10),
        )
        .await
    {
        Err(Error::Remote(api_err)) => assert_eq!(api_err.code, ErrorCode::StorageError),
        other => panic!("expected Remote(StorageError) after tick failure, got {other:?}"),
    }

    tick_handle.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_stdio_mcp_offline_discovery_online_recovery_restart_and_descriptor_security() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let temp = tempfile::tempdir().expect("tempdir");
    let missing_state_dir = temp.path().join("missing_mcp_state");
    let state_dir = temp.path().join("offline_mcp_state");

    assert!(
        !missing_state_dir.exists(),
        "missing_state_dir must not exist before offline MCP discovery"
    );

    // 1. Eager CLI client constructors must still fail when supervisor/state_dir is missing.
    assert!(matches!(
        SupervisorClient::from_state_dir(&missing_state_dir),
        Err(Error::Transport(_))
    ));
    assert!(matches!(
        ScopedClient::from_state_dir(&missing_state_dir, "bnd_11111111222243338444555555555555"),
        Err(Error::Transport(_))
    ));

    // 2. Lazy MCP constructor validates binding_id format and static path safety immediately.
    assert!(matches!(
        ScopedClient::from_state_dir_lazy(&missing_state_dir, "bad/binding"),
        Err(Error::Core(_))
    ));
    assert!(matches!(
        ScopedClient::from_state_dir_lazy(
            temp.path().join("../escape"),
            "bnd_11111111222243338444555555555555"
        ),
        Err(Error::SecurityViolation(_))
    ));
    let real_parent = temp.path().join("real_parent");
    fs::create_dir(&real_parent).expect("create real_parent");
    let symlink_parent = temp.path().join("symlink_parent");
    symlink(&real_parent, &symlink_parent).expect("create symlink_parent");
    assert!(matches!(
        ScopedClient::from_state_dir_lazy(
            symlink_parent.join("state"),
            "bnd_11111111222243338444555555555555"
        ),
        Err(Error::SecurityViolation(_))
    ));

    // 3. Offline discovery when `missing_state_dir` does not exist on disk at all.
    {
        let mut missing_mcp =
            spawn_mcp_child(&missing_state_dir, "bnd_11111111222243338444555555555555");
        let mut missing_stdin = missing_mcp.stdin.take().expect("missing mcp stdin");
        let missing_stdout = missing_mcp.stdout.take().expect("missing mcp stdout");
        let mut missing_reader = BufReader::new(missing_stdout);

        send_json_rpc(
            &mut missing_stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": { "name": "missing-dir-mcp-client", "version": "0.1.0" }
                }
            }),
        )
        .await;
        let _ = read_json_rpc_response(&mut missing_reader, 1).await;
        send_json_rpc(
            &mut missing_stdin,
            &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .await;

        send_json_rpc(
            &mut missing_stdin,
            &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
        )
        .await;
        let missing_list = read_json_rpc_response(&mut missing_reader, 2).await;
        assert_eq!(
            missing_list["result"]["tools"].as_array().map(Vec::len),
            Some(15)
        );
        assert!(
            !missing_state_dir.exists(),
            "initialize and tools/list must not create or require state_dir"
        );

        send_json_rpc(
            &mut missing_stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": { "name": "status", "arguments": {} }
            }),
        )
        .await;
        let missing_status = read_json_rpc_response(&mut missing_reader, 3).await;
        assert_eq!(missing_status["result"]["isError"], json!(true));
        assert_eq!(
            missing_status["result"]["structuredContent"]["error"]["code"],
            json!("transport_error")
        );

        drop(missing_stdin);
        cleanup_child(&mut missing_mcp).await;
    }

    // 4. Seed a real generated binding in `state_dir` using `Core` (while no supervisor is running
    // and no `supervisor.json` exists), then launch a long-running MCP process offline.
    let seeded_binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Offline MCP Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };
    let desc_path = state_dir.join(DESCRIPTOR_FILENAME);
    assert!(
        !desc_path.exists(),
        "supervisor.json must not exist while supervisor is offline"
    );

    let mut mcp_child = spawn_mcp_child(&state_dir, &seeded_binding.binding_id);
    let mut stdin = mcp_child.stdin.take().expect("mcp stdin");
    let stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut reader = BufReader::new(stdout);

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "offline-discovery-client", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let init_resp = read_json_rpc_response(&mut reader, 1).await;
    assert_eq!(
        init_resp["result"]["serverInfo"]["name"],
        json!("threadlane")
    );

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
    )
    .await;

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await;
    let list_resp = read_json_rpc_response(&mut reader, 2).await;
    let tools = list_resp["result"]["tools"]
        .as_array()
        .expect("tools array");
    assert_eq!(tools.len(), 15, "MCP tool count must remain 15");

    // 5. Calling `status` while supervisor is offline returns a structured transport_error tool result.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        }),
    )
    .await;
    let offline_status_resp = read_json_rpc_response(&mut reader, 3).await;
    assert_eq!(offline_status_resp["result"]["isError"], json!(true));
    assert_eq!(
        offline_status_resp["result"]["structuredContent"]["error"]["code"],
        json!("transport_error")
    );

    // 6. Descriptor security is never weakened on tool calls: an insecure (0644) descriptor
    // placed in state_dir before supervisor starts is rejected with `security_violation`.
    fs::write(
        &desc_path,
        serde_json::to_vec(&RuntimeDescriptor {
            version: DESCRIPTOR_VERSION,
            host: LOOPBACK_HOST.to_string(),
            port: 6553,
            pid: 999,
            token: SecretToken::generate(),
        })
        .unwrap(),
    )
    .expect("write insecure descriptor");
    fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        }),
    )
    .await;
    let insecure_desc_resp = read_json_rpc_response(&mut reader, 4).await;
    assert_eq!(insecure_desc_resp["result"]["isError"], json!(true));
    assert_eq!(
        insecure_desc_resp["result"]["structuredContent"]["error"]["code"],
        json!("security_violation")
    );
    fs::remove_file(&desc_path).expect("remove insecure descriptor");

    // 7. Start the supervisor and verify `status` and `start_demo` succeed in the SAME
    // long-running MCP process.
    let handle_1 = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor 1");

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        }),
    )
    .await;
    let online_status_resp = read_json_rpc_response(&mut reader, 5).await;
    assert_eq!(online_status_resp["result"]["isError"], json!(false));
    assert_eq!(
        online_status_resp["result"]["structuredContent"]["data"]["binding"]["binding_id"],
        json!(seeded_binding.binding_id)
    );
    assert_eq!(
        online_status_resp["result"]["structuredContent"]["data"]["binding"]["thread_uuid"],
        json!(THREAD_UUID_A)
    );

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "start_demo",
                "arguments": {
                    "command_key": "offline-mcp-start-1",
                    "instruction": "Task started after supervisor came online",
                    "step_delay_ms": 25
                }
            }
        }),
    )
    .await;
    let start_resp = read_json_rpc_response(&mut reader, 6).await;
    assert_eq!(start_resp["result"]["isError"], json!(false));

    // 8. Restart supervisor (new port and bearer token) and verify the SAME MCP process refreshes
    // the descriptor and continues working.
    let old_token = handle_1.descriptor().token.as_str().to_string();
    handle_1.shutdown().await;

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        }),
    )
    .await;
    let mid_restart_resp = read_json_rpc_response(&mut reader, 7).await;
    assert_eq!(mid_restart_resp["result"]["isError"], json!(true));
    assert_eq!(
        mid_restart_resp["result"]["structuredContent"]["error"]["code"],
        json!("transport_error")
    );

    let handle_2 = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor 2");
    assert_ne!(handle_2.descriptor().token.as_str(), old_token);

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "status",
                "arguments": {}
            }
        }),
    )
    .await;
    let restarted_status_resp = read_json_rpc_response(&mut reader, 8).await;
    assert_eq!(restarted_status_resp["result"]["isError"], json!(false));
    assert_eq!(
        restarted_status_resp["result"]["structuredContent"]["data"]["total_runs"],
        json!(1)
    );

    drop(stdin);
    cleanup_child(&mut mcp_child).await;
    handle_2.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_arbitrary_non_empty_directory_is_rejected_without_chmod_or_writes() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");

    // 1. Pre-existing non-empty directory with 0755 permissions and unrelated contents must be
    // rejected before chmod or writing any Threadlane files.
    let arbitrary_dir = temp.path().join("arbitrary_project_dir");
    fs::create_dir(&arbitrary_dir).expect("create arbitrary_dir");
    fs::set_permissions(&arbitrary_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    let user_file = arbitrary_dir.join("README.txt");
    fs::write(&user_file, b"important user content").expect("write user_file");
    let user_subdir = arbitrary_dir.join("src");
    fs::create_dir(&user_subdir).expect("create user_subdir");

    match prepare_state_dir(&arbitrary_dir) {
        Err(Error::SecurityViolation(msg)) => {
            assert!(msg.contains("non-empty"), "unexpected message: {msg}");
        }
        other => panic!("expected SecurityViolation for arbitrary non-empty dir, got {other:?}"),
    }

    match Supervisor::start(fast_supervisor_config(&arbitrary_dir)).await {
        Err(Error::SecurityViolation(msg)) => {
            assert!(msg.contains("non-empty"), "unexpected message: {msg}");
        }
        other => panic!("expected SecurityViolation on Supervisor::start, got {other:?}"),
    }

    let preserved_mode = fs::symlink_metadata(&arbitrary_dir)
        .expect("stat arbitrary_dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        preserved_mode, 0o755,
        "arbitrary directory permissions must not be changed to 0700"
    );
    assert_eq!(
        fs::read(&user_file).expect("read user_file"),
        b"important user content"
    );
    assert!(user_subdir.is_dir());
    assert!(!arbitrary_dir.join(LOCK_FILENAME).exists());
    assert!(!arbitrary_dir.join(DATABASE_FILENAME).exists());
    assert!(!arbitrary_dir.join(DESCRIPTOR_FILENAME).exists());

    // 2. Even if a pre-existing non-empty directory already has 0700 permissions, if it contains
    // none of the known Threadlane state entries it must still be rejected without writing files.
    let private_unrelated = temp.path().join("private_unrelated");
    fs::create_dir(&private_unrelated).expect("create private_unrelated");
    fs::set_permissions(&private_unrelated, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    fs::write(private_unrelated.join("notes.txt"), b"private notes").expect("write notes");

    assert!(matches!(
        Supervisor::start(fast_supervisor_config(&private_unrelated)).await,
        Err(Error::SecurityViolation(_))
    ));
    assert!(!private_unrelated.join(LOCK_FILENAME).exists());
    assert!(!private_unrelated.join(DATABASE_FILENAME).exists());
    assert!(!private_unrelated.join(DESCRIPTOR_FILENAME).exists());

    // 3. A pre-existing EMPTY directory (even with 0755 permissions) may be initialized into a
    // 0700 private state directory, and then reopened as a genuine private state directory.
    let empty_state_dir = temp.path().join("empty_state_dir");
    fs::create_dir(&empty_state_dir).expect("create empty_state_dir");
    fs::set_permissions(&empty_state_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");

    let handle = Supervisor::start(fast_supervisor_config(&empty_state_dir))
        .await
        .expect("initialize empty state dir");
    let initialized_mode = fs::symlink_metadata(&empty_state_dir)
        .expect("stat empty_state_dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(initialized_mode, 0o700);
    handle.shutdown().await;

    let handle_reopen = Supervisor::start(fast_supervisor_config(&empty_state_dir))
        .await
        .expect("reopen genuine private state dir");
    handle_reopen.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_trusted_cli_retry_wakeup_failed_unknown_auth_and_mcp_rejection() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");
    let bin = env!("CARGO_BIN_EXE_threadlane");

    // Part 1: Failed wake-up -> CLI `retry-wakeup` -> exactly one more dispatch, unchanged ack cursor.
    let fail_state_dir = temp.path().join("retry_fail_state");
    let fail_log = temp.path().join("retry_fail_invocations.txt");
    let fake_codex = temp.path().join("retry_codex.sh");
    let script = format!(
        "#!/bin/sh\necho \"$1|$2|$3|$4|$5\" >> \"{}\"\nexit 0\n",
        fail_log.display()
    );
    fs::write(&fake_codex, script).expect("write fake_codex");
    // Start non-executable (0600) so the first wake-up attempt fails to spawn -> `Failed`.
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o600)).expect("chmod 0600");

    let binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&fail_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Retry Fail Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&fail_state_dir)
            .expect("enforce perms");
        b
    };

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: fail_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex.clone(),
            notify_bindings: BTreeSet::from([binding.binding_id.clone()]),
            timeout: Duration::from_millis(1_000),
        }),
        workers: None,
    })
    .await
    .expect("start supervisor");

    let client = SupervisorClient::from_state_dir(&fail_state_dir).expect("client");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped client");

    let _ = scoped
        .start_demo(
            "rf-start".to_string(),
            "Retry failed wake".to_string(),
            Some(10),
        )
        .await
        .expect("start demo");

    let failed_status = wait_for_wakeup_status(&scoped, WakeupStatus::Failed).await;
    assert_eq!(failed_status.wakeup.attempt_count, 1);
    let failed_claim_id = failed_status
        .wakeup
        .claim_id
        .clone()
        .expect("failed claim_id");

    // Pull 1 event (without acknowledging) so delivered_seq = 1, acked_seq = 0.
    let pulled = scoped
        .pull_events(None, Some(1))
        .await
        .expect("pull 1 event");
    assert_eq!(pulled.delivered_seq, 1);
    assert_eq!(pulled.acked_seq, 0);

    // Fix executable permissions to 0700 before manual retry.
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    // Wrong expected_claim_id via CLI must fail and not dispatch.
    let bad_cas_out = Command::new(bin)
        .args([
            "retry-wakeup",
            "--state-dir",
            fail_state_dir.to_str().unwrap(),
            "--binding",
            &binding.binding_id,
            "--expected-claim",
            "wcl_00000000000000000000000000000000",
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run bad CAS retry-wakeup");
    assert!(!bad_cas_out.status.success());

    // Valid CLI `retry-wakeup` re-arms the Failed notice and triggers exactly one more dispatch.
    let retry_out = run_cli_cmd(
        bin,
        &[
            "retry-wakeup",
            "--state-dir",
            fail_state_dir.to_str().unwrap(),
            "--binding",
            &binding.binding_id,
            "--expected-claim",
            &failed_claim_id,
        ],
    )
    .await;
    assert_eq!(retry_out["binding_id"], json!(binding.binding_id));

    let submitted_status = wait_for_wakeup_status(&scoped, WakeupStatus::Submitted).await;
    assert_eq!(submitted_status.wakeup.attempt_count, 2);
    assert_eq!(submitted_status.binding.delivered_seq, 1);
    assert_eq!(
        submitted_status.binding.acked_seq, 0,
        "retry-wakeup must not alter acked_seq cursor"
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    let logged = fs::read_to_string(&fail_log).expect("read fail_log");
    assert_eq!(
        logged.lines().count(),
        1,
        "expected exactly one dispatch after manual retry of Failed claim"
    );

    // Repeating retry-wakeup with the stale failed_claim_id or on Submitted state must fail.
    let repeat_out = Command::new(bin)
        .args([
            "retry-wakeup",
            "--state-dir",
            fail_state_dir.to_str().unwrap(),
            "--binding",
            &binding.binding_id,
            "--expected-claim",
            &failed_claim_id,
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run stale repeat retry-wakeup");
    assert!(!repeat_out.status.success());

    // Auth rejection and unknown-field rejection on POST /v1/bindings/{id}/retry-wakeup.
    let base_url = handle.descriptor().base_url().expect("base_url");
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("raw client");

    let unauth_resp = raw_http
        .post(format!(
            "{base_url}/v1/bindings/{}/retry-wakeup",
            binding.binding_id
        ))
        .json(&json!({ "expected_claim_id": failed_claim_id }))
        .send()
        .await
        .expect("send unauth retry-wakeup");
    assert_eq!(unauth_resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let injected_field_resp = raw_http
        .post(format!(
            "{base_url}/v1/bindings/{}/retry-wakeup",
            binding.binding_id
        ))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", handle.descriptor().token.as_str()),
        )
        .json(&json!({
            "expected_claim_id": failed_claim_id,
            "allow_unknown": false,
            "now_ms": 12345
        }))
        .send()
        .await
        .expect("send injected now_ms retry-wakeup");
    assert_eq!(
        injected_field_resp.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    // Scoped `call` CLI must reject `retry_wakeup` operation.
    let scoped_call_out = Command::new(bin)
        .args([
            "call",
            "--state-dir",
            fail_state_dir.to_str().unwrap(),
            "--binding",
            &binding.binding_id,
            "--request",
            r#"{"op":"retry_wakeup","expected_claim_id":"wcl_123"}"#,
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run scoped call retry_wakeup");
    assert!(!scoped_call_out.status.success());

    // MCP server must reject `retry_wakeup` tool call and any injected retry fields.
    let mut mcp_child = spawn_mcp_child(&fail_state_dir, &binding.binding_id);
    let mut stdin = mcp_child.stdin.take().expect("mcp stdin");
    let stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut reader = BufReader::new(stdout);

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "retry-check-mcp", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let _ = read_json_rpc_response(&mut reader, 1).await;
    send_json_rpc(
        &mut stdin,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "retry_wakeup",
                "arguments": { "expected_claim_id": failed_claim_id }
            }
        }),
    )
    .await;
    let mcp_retry_resp = read_json_rpc_response(&mut reader, 2).await;
    assert!(
        mcp_retry_resp.get("error").is_some() || mcp_retry_resp["result"]["isError"] == json!(true),
        "MCP must reject retry_wakeup tool call: {mcp_retry_resp}"
    );

    drop(stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;

    // Part 2: `Unknown` wake-up requires explicit `--allow-unknown` flag.
    let unknown_state_dir = temp.path().join("retry_unknown_state");
    let hang_marker = temp.path().join("hang_marker");
    let unknown_log = temp.path().join("retry_unknown_invocations.txt");
    let toggle_codex = temp.path().join("toggle_codex.sh");
    fs::write(&hang_marker, b"1").expect("write hang_marker");
    let toggle_script = format!(
        "#!/bin/sh\nif [ -f \"{}\" ]; then\n  exec sleep 30\nfi\necho invoked >> \"{}\"\nexit 0\n",
        hang_marker.display(),
        unknown_log.display()
    );
    fs::write(&toggle_codex, toggle_script).expect("write toggle_codex");
    fs::set_permissions(&toggle_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    let unknown_binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&unknown_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_B, "Retry Unknown Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&unknown_state_dir)
            .expect("enforce perms");
        b
    };

    let unknown_handle = Supervisor::start(SupervisorConfig {
        state_dir: unknown_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: toggle_codex,
            notify_bindings: BTreeSet::from([unknown_binding.binding_id.clone()]),
            timeout: Duration::from_millis(80),
        }),
        workers: None,
    })
    .await
    .expect("start unknown supervisor");

    let unknown_scoped = SupervisorClient::from_state_dir(&unknown_state_dir)
        .expect("unknown client")
        .scoped(&unknown_binding.binding_id)
        .expect("unknown scoped");

    let _ = unknown_scoped
        .start_demo(
            "ru-start".to_string(),
            "Trigger timeout".to_string(),
            Some(10),
        )
        .await
        .expect("start unknown demo");

    let unknown_st = wait_for_wakeup_status(&unknown_scoped, WakeupStatus::Unknown).await;
    assert_eq!(unknown_st.wakeup.attempt_count, 1);
    let unknown_claim_id = unknown_st
        .wakeup
        .claim_id
        .clone()
        .expect("unknown claim_id");

    // Remove hang_marker so future invocations succeed.
    fs::remove_file(&hang_marker).expect("remove hang_marker");

    // Without `--allow-unknown`, CLI `retry-wakeup` must be rejected.
    let without_flag = Command::new(bin)
        .args([
            "retry-wakeup",
            "--state-dir",
            unknown_state_dir.to_str().unwrap(),
            "--binding",
            &unknown_binding.binding_id,
            "--expected-claim",
            &unknown_claim_id,
        ])
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run retry-wakeup without --allow-unknown");
    assert!(
        !without_flag.status.success(),
        "retry-wakeup on Unknown must fail without --allow-unknown"
    );
    let st_still_unknown = unknown_scoped.status().await.expect("status still unknown");
    assert_eq!(st_still_unknown.wakeup.status, WakeupStatus::Unknown);
    assert_eq!(st_still_unknown.wakeup.attempt_count, 1);

    // With `--allow-unknown`, CLI `retry-wakeup` succeeds and dispatches once.
    let _ = run_cli_cmd(
        bin,
        &[
            "retry-wakeup",
            "--state-dir",
            unknown_state_dir.to_str().unwrap(),
            "--binding",
            &unknown_binding.binding_id,
            "--expected-claim",
            &unknown_claim_id,
            "--allow-unknown",
        ],
    )
    .await;

    let st_after_allow = wait_for_wakeup_status(&unknown_scoped, WakeupStatus::Submitted).await;
    assert_eq!(st_after_allow.wakeup.attempt_count, 2);
    let unknown_lines = fs::read_to_string(&unknown_log).expect("read unknown_log");
    assert_eq!(unknown_lines.lines().count(), 1);

    unknown_handle.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn test_serve_child_process_exits_nonzero_on_fatal_sqlite_error_and_releases_lock() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("child_fatal_state");
    let count_file = temp.path().join("child_fatal_count.txt");
    let fake_codex = temp.path().join("child_fake_codex.sh");
    let bin = env!("CARGO_BIN_EXE_threadlane");

    let script = format!(
        "#!/bin/sh\necho invoked >> \"{}\"\nexit 0\n",
        count_file.display()
    );
    fs::write(&fake_codex, script).expect("write fake_codex");
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Child Fatal Chat")
            .expect("create binding");
        drop(core);

        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite for trigger");
        conn.execute_batch(
            "CREATE TRIGGER fail_child_wakeup_result
             BEFORE UPDATE ON wakeups
             WHEN NEW.resolved_at_ms IS NOT NULL
             BEGIN
                 SELECT RAISE(ABORT, 'simulated child disk failure on wakeup result');
             END;",
        )
        .expect("create trigger");
        drop(conn);
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let serve_child = Command::new(bin)
        .arg("serve")
        .arg("--state-dir")
        .arg(&state_dir)
        .arg("--codex")
        .arg(&fake_codex)
        .arg("--notify-binding")
        .arg(&binding.binding_id)
        .arg("--tick-interval-ms")
        .arg("15")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn threadlane serve");

    let desc_path = state_dir.join(DESCRIPTOR_FILENAME);
    for _ in 0..100 {
        if desc_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(desc_path.exists(), "supervisor.json should appear");

    let scoped = ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("scoped");
    let _ = scoped
        .start_demo(
            "cf-start".to_string(),
            "Trigger fatal wakeup result failure in child serve".to_string(),
            Some(10),
        )
        .await
        .expect("start demo");

    // Wait for `threadlane serve` child process to exit on its own (without sending a signal).
    let output = tokio::time::timeout(Duration::from_secs(5), serve_child.wait_with_output())
        .await
        .expect("serve child must exit within bounded timeout after fatal error")
        .expect("wait_with_output");

    assert!(
        !output.status.success(),
        "serve must exit nonzero on fatal persistence error, got {:?}",
        output.status
    );
    let stderr_text = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr_text.contains("storage_error")
            && stderr_text.contains("simulated child disk failure"),
        "stderr must preserve the actual fatal diagnostic, got: {stderr_text}"
    );

    // Descriptor must be cleaned up and fake Codex must have been invoked only once.
    assert!(
        !desc_path.exists(),
        "supervisor.json must be removed on fatal shutdown"
    );
    let invocations = fs::read_to_string(&count_file).expect("read count_file");
    assert_eq!(
        invocations.lines().count(),
        1,
        "external wake-up must not be repeated on fatal error"
    );

    // Drop the test trigger and prove a new supervisor owner can immediately acquire the lock and start.
    {
        let conn =
            rusqlite::Connection::open(state_dir.join(DATABASE_FILENAME)).expect("open sqlite");
        conn.execute_batch("DROP TRIGGER fail_child_wakeup_result;")
            .expect("drop trigger");
    }

    let new_owner = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("new supervisor owner must be able to start after fatal exit");
    let new_scoped =
        ScopedClient::from_state_dir(&state_dir, &binding.binding_id).expect("new scoped client");
    let recovered_status = new_scoped.status().await.expect("status on new owner");
    assert_eq!(
        recovered_status.wakeup.status,
        WakeupStatus::Unknown,
        "in-flight claim left by faulted owner must recover to Unknown without auto-retry"
    );
    new_owner.shutdown().await;
}

#[tokio::test]
async fn test_oversized_derived_event_instruction_does_not_fault_supervisor_or_other_binding() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("oversized_derived_state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let binding_a = client
        .create_binding(THREAD_UUID_A, "Oversized Instruction Binding A")
        .await
        .expect("bind A");
    let binding_b = client
        .create_binding(THREAD_UUID_B, "Healthy Sibling Binding B")
        .await
        .expect("bind B");

    let scoped_a = client
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped A");
    let scoped_b = client
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped B");

    // Exact reproduced input: 2411 '\u{1}' bytes + 1685 'a' bytes = 4096 bytes.
    let exact_instruction = format!("{}{}", "\u{1}".repeat(2411), "a".repeat(1685));
    assert_eq!(exact_instruction.len(), 4096);

    let started = scoped_a
        .start_demo(
            "over-start-1".to_string(),
            exact_instruction.clone(),
            Some(15),
        )
        .await
        .expect("start_demo with 4096-byte control-heavy instruction");
    assert_eq!(
        started.instruction, exact_instruction,
        "accepted raw instruction must remain full in Run"
    );

    // Wait for tick to emit derived progress + question events and transition to WaitingInput.
    let waiting_a = wait_for_run_status(&scoped_a, &started.run_id, RunStatus::WaitingInput).await;
    assert!(
        handle.fatal_error().is_none(),
        "supervisor must not fault on tick with control-heavy 4096-byte instruction"
    );
    assert_eq!(waiting_a.instruction, exact_instruction);
    let revs_a = scoped_a
        .list_run_revisions(started.run_id.clone(), None, None)
        .await
        .expect("list revisions on A");
    assert_eq!(revs_a.revisions[0].instruction, exact_instruction);

    // Sibling binding B must remain completely functional.
    let run_b = scoped_b
        .start_demo(
            "b-start-1".to_string(),
            "Sibling task on B".to_string(),
            Some(15),
        )
        .await
        .expect("start demo on B after A ticked");
    let _waiting_b = wait_for_run_status(&scoped_b, &run_b.run_id, RunStatus::WaitingInput).await;

    // Restart supervisor and verify neither A nor B faults after restart, and A can complete.
    handle.shutdown().await;
    let handle_restarted = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("restart supervisor");

    let status_b = scoped_b.status().await.expect("status B after restart");
    assert_eq!(status_b.total_runs, 1);

    let q_id = waiting_a.active_question_id.expect("question_id on A");
    let _ = scoped_a
        .answer(
            "over-ans-1".to_string(),
            started.run_id.clone(),
            q_id,
            1,
            "Proceed after compact derived question".to_string(),
        )
        .await
        .expect("answer A after restart");

    let completed_a = wait_for_run_status(&scoped_a, &started.run_id, RunStatus::Completed).await;
    assert_eq!(completed_a.instruction, exact_instruction);
    assert!(handle_restarted.fatal_error().is_none());

    let batch_a = scoped_a
        .pull_events(None, Some(20))
        .await
        .expect("pull events on A");
    assert_eq!(batch_a.events.len(), 5);
    assert_eq!(
        batch_a.events[0].payload.instruction.as_deref(),
        Some(exact_instruction.as_str())
    );

    handle_restarted.shutdown().await;
}

#[tokio::test]
async fn test_m2a_rmcp_call_tool_result_wire_budgets_stdio_mcp_paging_and_http_body_cap() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("m2a_mcp_budget_state");
    let bin = env!("CARGO_BIN_EXE_threadlane");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");

    let binding_a = client
        .create_binding(THREAD_UUID_A, "M2A Budget Binding A")
        .await
        .expect("bind A");
    let binding_b = client
        .create_binding(THREAD_UUID_B, "M2A Sibling Binding B")
        .await
        .expect("bind B");

    let scoped_a = client
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped A");
    let scoped_b = client
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped B");

    let make_max_instruction = |rev: u64| -> String {
        let suffix = format!("mcp-rev-{rev:03}-");
        let control_prefix = "\u{1}".repeat(2_410);
        let pad_len = MAX_INSTRUCTION_LEN - control_prefix.len() - suffix.len();
        format!("{control_prefix}{suffix}{}", "z".repeat(pad_len))
    };

    // Start a run on A with 15 control-escaped maximum-length revisions (~47 KiB two-copy wire
    // bytes per revision -> ~700 KiB across 15 revisions, crossing multiple 256 KiB page boundaries).
    let total_revisions = 15_u64;
    let inst_1 = make_max_instruction(1);
    let started_a = scoped_a
        .start_demo("m2a-start-1".to_string(), inst_1.clone(), Some(10))
        .await
        .expect("start_demo rev 1");

    // Verify actual rmcp CallToolResult for compact Run response fits within MAX_COMPACT_RESPONSE_BYTES
    // and is conservatively bounded by `estimate_wire_response_bytes`.
    let compact_mcp_res = ThreadlaneMcpServer::build_structured_tool_result(
        &binding_a.binding_id,
        "start_demo",
        &started_a,
    );
    let compact_mcp_json =
        serde_json::to_string(&compact_mcp_res).expect("serialize CallToolResult");
    let compact_estimate =
        estimate_wire_response_bytes(&CommandOutput::Run(Box::new(started_a.clone())))
            .expect("estimate");
    assert!(
        compact_mcp_json.len() <= MAX_COMPACT_RESPONSE_BYTES,
        "serialized CallToolResult for compact Run was {} > {}",
        compact_mcp_json.len(),
        MAX_COMPACT_RESPONSE_BYTES
    );
    assert!(
        compact_mcp_json.len() <= compact_estimate,
        "actual CallToolResult {} exceeded conservative estimate {}",
        compact_mcp_json.len(),
        compact_estimate
    );

    let _ = wait_for_run_status(&scoped_a, &started_a.run_id, RunStatus::WaitingInput).await;

    for rev in 2..=total_revisions {
        let inst = make_max_instruction(rev);
        let steered = scoped_a
            .steer(
                format!("m2a-steer-{rev}"),
                started_a.run_id.clone(),
                rev - 1,
                inst,
            )
            .await
            .expect("steer to next rev");
        let steered_mcp_res = ThreadlaneMcpServer::build_structured_tool_result(
            &binding_a.binding_id,
            "steer",
            &steered,
        );
        let steered_mcp_json =
            serde_json::to_string(&steered_mcp_res).expect("serialize steered CallToolResult");
        assert!(steered_mcp_json.len() <= MAX_COMPACT_RESPONSE_BYTES);

        for _ in 0..80 {
            let r = scoped_a
                .get_run(started_a.run_id.clone())
                .await
                .expect("get_run");
            if r.revision == rev && r.status == RunStatus::WaitingInput {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    // Create a run on B as well for cross-binding cursor misuse tests.
    let run_b = scoped_b
        .start_demo("m2a-b-start".to_string(), "Run on B".to_string(), Some(10))
        .await
        .expect("start B");
    let _ = wait_for_run_status(&scoped_b, &run_b.run_id, RunStatus::WaitingInput).await;

    // Spawn real stdio MCP server for binding_a and page through `list_run_revisions` with `limit: 50`.
    let mut mcp_child = spawn_mcp_child(&state_dir, &binding_a.binding_id);
    let mut stdin = mcp_child.stdin.take().expect("mcp stdin");
    let stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut reader = BufReader::new(stdout);

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "m2a-budget-mcp-client", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let _ = read_json_rpc_response(&mut reader, 1).await;
    send_json_rpc(
        &mut stdin,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;

    let mut mcp_collected_revs = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_count = 0_usize;
    let mut req_id = 10_u64;

    loop {
        req_id += 1;
        send_json_rpc(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "list_run_revisions",
                    "arguments": {
                        "run_id": started_a.run_id,
                        "cursor": cursor,
                        "limit": MAX_PAGE_LIMIT
                    }
                }
            }),
        )
        .await;
        let resp = read_json_rpc_response(&mut reader, req_id).await;
        assert_eq!(resp["result"]["isError"], json!(false));

        // Measure the serialized `result` (the actual `rmcp::model::CallToolResult` JSON object,
        // which contains BOTH `content[0].text` and `structuredContent`).
        let call_tool_result_bytes = serde_json::to_string(&resp["result"])
            .expect("serialize actual MCP CallToolResult")
            .len();
        assert!(
            call_tool_result_bytes <= MAX_PAGE_RESPONSE_BYTES,
            "actual MCP CallToolResult was {call_tool_result_bytes} bytes > {MAX_PAGE_RESPONSE_BYTES}"
        );

        let page_data = &resp["result"]["structuredContent"]["data"];
        let page_revs = page_data["revisions"].as_array().expect("revisions array");
        assert!(!page_revs.is_empty());
        page_count += 1;
        for item in page_revs {
            mcp_collected_revs.push((
                item["revision"].as_u64().expect("rev u64"),
                item["instruction"].as_str().expect("inst str").to_string(),
            ));
        }

        if page_data["has_more"] == json!(true) {
            assert!(
                page_revs.len() < MAX_PAGE_LIMIT,
                "byte budget must split 50-row request into smaller pages"
            );
            cursor = Some(
                page_data["next_cursor"]
                    .as_str()
                    .expect("next_cursor")
                    .to_string(),
            );
        } else {
            assert!(page_data["next_cursor"].is_null());
            break;
        }
    }

    assert!(
        page_count >= 3,
        "15 max-escaped revisions must span at least 3 256KiB-bounded MCP pages, got {page_count}"
    );
    assert_eq!(mcp_collected_revs.len() as u64, total_revisions);
    for (idx, (rev, inst)) in mcp_collected_revs.iter().enumerate() {
        let expected_rev = (idx as u64) + 1;
        assert_eq!(
            *rev, expected_rev,
            "no revision may be skipped or duplicated when byte budget ends a page"
        );
        assert_eq!(*inst, make_max_instruction(expected_rev));
    }

    // Directly serialize a full byte-budget-split `rmcp::model::CallToolResult` from
    // `ThreadlaneMcpServer::build_structured_tool_result` and verify it is bounded by both
    // `estimate_wire_response_bytes` and `MAX_PAGE_RESPONSE_BYTES`.
    let max_page_revs = scoped_a
        .list_run_revisions(started_a.run_id.clone(), None, Some(MAX_PAGE_LIMIT))
        .await
        .expect("max page revs");
    assert!(max_page_revs.has_more);
    assert!(max_page_revs.revisions.len() < MAX_PAGE_LIMIT);
    let page_call_tool_result = ThreadlaneMcpServer::build_structured_tool_result(
        &binding_a.binding_id,
        "list_run_revisions",
        &max_page_revs,
    );
    let page_call_tool_json =
        serde_json::to_string(&page_call_tool_result).expect("serialize page CallToolResult");
    let page_estimate =
        estimate_wire_response_bytes(&CommandOutput::RevisionHistory(max_page_revs.clone()))
            .expect("estimate page wire bytes");
    assert!(page_call_tool_json.len() <= MAX_PAGE_RESPONSE_BYTES);
    assert!(
        page_call_tool_json.len() <= page_estimate,
        "actual page CallToolResult {} exceeded estimate {}",
        page_call_tool_json.len(),
        page_estimate
    );

    // Verify cross-kind cursor misuse over stdio MCP returns `validation_failed` with `isError: true`.
    let page_1_revs = scoped_a
        .list_run_revisions(started_a.run_id.clone(), None, Some(1))
        .await
        .expect("page 1 revs");
    let rev_cursor_1 = page_1_revs.next_cursor.expect("rev cursor 1");

    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 90,
            "method": "tools/call",
            "params": {
                "name": "list_run_questions",
                "arguments": {
                    "run_id": started_a.run_id,
                    "cursor": rev_cursor_1,
                    "limit": 5
                }
            }
        }),
    )
    .await;
    let misuse_resp = read_json_rpc_response(&mut reader, 90).await;
    assert_eq!(misuse_resp["result"]["isError"], json!(true));
    assert_eq!(
        misuse_resp["result"]["structuredContent"]["error"]["code"],
        json!("validation_failed")
    );

    drop(stdin);
    cleanup_child(&mut mcp_child).await;

    // Verify CLI `threadlane call` works for `list_runs`, `list_run_revisions`, and `list_run_questions`.
    let cli_list_runs = run_cli_cmd(
        bin,
        &[
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding_a.binding_id,
            "--request",
            r#"{"op":"list_runs","limit":5}"#,
        ],
    )
    .await;
    assert_eq!(cli_list_runs["kind"], json!("run_list"));
    assert_eq!(
        cli_list_runs["data"]["runs"][0]["run_id"],
        json!(started_a.run_id)
    );

    let cli_list_qs = run_cli_cmd(
        bin,
        &[
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding_a.binding_id,
            "--request",
            &serde_json::to_string(&ScopedRequest::ListRunQuestions {
                run_id: started_a.run_id.clone(),
                cursor: None,
                limit: Some(5),
            })
            .unwrap(),
        ],
    )
    .await;
    assert_eq!(cli_list_qs["kind"], json!("question_history"));
    assert_eq!(
        cli_list_qs["data"]["questions"].as_array().map(Vec::len),
        Some(5)
    );
    assert_eq!(cli_list_qs["data"]["has_more"], json!(true));

    // Verify HTTP request body size cap (> MAX_HTTP_BODY_BYTES = 64 KiB):
    // 1) Unauthenticated oversized request -> 401 Unauthorized.
    // 2) Authenticated oversized request -> 413 Payload Too Large with ErrorCode::PayloadTooLarge.
    let base_url = handle.descriptor().base_url().expect("base_url");
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("raw client");
    let oversized_body = serde_json::to_string(&json!({
        "op": "start_demo",
        "command_key": "huge-body",
        "instruction": "X".repeat(MAX_HTTP_BODY_BYTES + 1024),
    }))
    .unwrap();
    assert!(oversized_body.len() > MAX_HTTP_BODY_BYTES);

    let unauth_huge = raw_http
        .post(format!(
            "{base_url}/v1/bindings/{}/command",
            binding_a.binding_id
        ))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(oversized_body.clone())
        .send()
        .await
        .expect("send unauth huge");
    assert_eq!(unauth_huge.status(), reqwest::StatusCode::UNAUTHORIZED);

    let auth_huge = raw_http
        .post(format!(
            "{base_url}/v1/bindings/{}/command",
            binding_a.binding_id
        ))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", handle.descriptor().token.as_str()),
        )
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(oversized_body)
        .send()
        .await
        .expect("send auth huge");
    assert_eq!(auth_huge.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let huge_err_json: Value = auth_huge.json().await.expect("parse 413 error json");
    assert_eq!(huge_err_json["code"], json!("payload_too_large"));

    handle.shutdown().await;
}

async fn wait_for_run_status(
    scoped: &threadlane::ScopedClient,
    run_id: &str,
    expected: RunStatus,
) -> threadlane_core::Run {
    for _ in 0..60 {
        let run = scoped.get_run(run_id.to_string()).await.expect("get_run");
        if run.status == expected {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("timed out waiting for run `{run_id}` to reach status {expected:?}");
}

async fn wait_for_wakeup_status(
    scoped: &threadlane::ScopedClient,
    expected: WakeupStatus,
) -> threadlane::StatusResponse {
    for _ in 0..60 {
        let status = scoped.status().await.expect("status");
        if status.wakeup.status == expected {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("timed out waiting for wakeup status {expected:?}");
}

fn spawn_mcp_child(state_dir: &Path, binding_id: &str) -> Child {
    let bin = env!("CARGO_BIN_EXE_threadlane");
    Command::new(bin)
        .arg("mcp")
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--binding")
        .arg(binding_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn threadlane mcp")
}

async fn send_json_rpc<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, msg: &Value) {
    let line = serde_json::to_string(msg).expect("serialize json-rpc");
    writer.write_all(line.as_bytes()).await.expect("write line");
    writer.write_all(b"\n").await.expect("write newline");
    writer.flush().await.expect("flush");
}

async fn read_json_rpc_response<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    expected_id: u64,
) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for JSON-RPC response id {expected_id}"
        );
        let mut line = String::new();
        let bytes = tokio::time::timeout(remaining, reader.read_line(&mut line))
            .await
            .expect("json-rpc read timeout")
            .expect("json-rpc read line");
        assert!(bytes > 0, "unexpected EOF on MCP stdout");
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed: Value = serde_json::from_str(trimmed).expect("valid JSON-RPC frame on stdout");
        if parsed.get("id").and_then(Value::as_u64) == Some(expected_id) {
            return parsed;
        }
    }
}

async fn run_cli_cmd(bin: &str, args: &[&str]) -> Value {
    let output = Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run threadlane CLI command");
    assert!(
        output.status.success(),
        "CLI command {:?} failed: stderr={}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("parse CLI JSON stdout")
}

async fn cleanup_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
}
