use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use threadlane::{
    CodexWakeupConfig, Error, ErrorCode, RuntimeDescriptor, ScopedRequest, SecretToken, Supervisor,
    SupervisorClient, SupervisorConfig, DATABASE_FILENAME, DESCRIPTOR_FILENAME, DESCRIPTOR_VERSION,
    LOCK_FILENAME, LOOPBACK_HOST,
};
use threadlane_core::{Core, QuestionStatus, RunStatus, WakeupStatus, FIXED_WAKEUP_MESSAGE};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

fn fast_supervisor_config(state_dir: &Path) -> SupervisorConfig {
    SupervisorConfig {
        state_dir: state_dir.to_path_buf(),
        tick_interval: Duration::from_millis(15),
        codex: None,
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
    assert_eq!(steered_run.questions[0].status, QuestionStatus::Superseded);
    assert_eq!(steered_run.questions[0].superseded_by_revision, Some(2));

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
    assert_eq!(completed_run.revision_history.len(), 2);
    assert_eq!(completed_run.questions.len(), 2);

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

    // 2. tools/list: verify exact 8 scoped tools, annotations, and no binding_id/thread_uuid/now_ms params.
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
        "start_demo",
        "pull_events",
        "ack_events",
        "answer",
        "steer",
        "cancel",
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
    assert_eq!(steered_data["questions"][0]["status"], json!("superseded"));

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
