use std::time::Duration;

use serde_json::{json, Value};
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

use threadlane::{
    ErrorCode, EventKind, ScopedRequest, Supervisor, SupervisorClient, SupervisorConfig,
    ThreadlaneMcpServer,
};
use threadlane_core::{
    estimate_wire_response_bytes, CommandOutput, RunStatus, WakeupStatus, MAX_INSTRUCTION_LEN,
    MAX_PAGE_LIMIT, MAX_PAGE_RESPONSE_BYTES,
};

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
async fn test_list_activity_sdk_http_cli_mcp_pagination_filters_live_advance_and_follower_preservation(
) {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("activity_runtime_state");
    let bin = env!("CARGO_BIN_EXE_threadlane");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");

    let binding_a = client
        .create_binding(THREAD_UUID_A, "Runtime Activity Chat A")
        .await
        .expect("bind A");
    let binding_b = client
        .create_binding(THREAD_UUID_B, "Runtime Activity Chat B")
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

    // 1. Empty binding returns empty ActivityPage with high_water_mark_seq = 0 and after_seq = 0.
    let empty_page = scoped_a
        .list_activity(None, None, None, None, None)
        .await
        .expect("empty list_activity");
    assert_eq!(empty_page.binding_id, binding_a.binding_id);
    assert!(empty_page.events.is_empty());
    assert_eq!(empty_page.next_cursor, None);
    assert!(!empty_page.has_more);
    assert_eq!(empty_page.high_water_mark_seq, 0);
    assert_eq!(empty_page.after_seq, 0);

    // 2. Start 2 demo runs on binding A and 1 on binding B, wait for WaitingInput.
    let run_a1 = scoped_a
        .start_demo(
            "act-a1-start".to_string(),
            "Run A1 initial instruction".to_string(),
            Some(15),
        )
        .await
        .expect("start a1");
    let waiting_a1 = wait_for_run_status(&scoped_a, &run_a1.run_id, RunStatus::WaitingInput).await;

    let run_a2 = scoped_a
        .start_demo(
            "act-a2-start".to_string(),
            "Run A2 initial instruction".to_string(),
            Some(15),
        )
        .await
        .expect("start a2");
    let _waiting_a2 = wait_for_run_status(&scoped_a, &run_a2.run_id, RunStatus::WaitingInput).await;

    let run_b1 = scoped_b
        .start_demo(
            "act-b1-start".to_string(),
            "Run B1 initial instruction".to_string(),
            Some(15),
        )
        .await
        .expect("start b1");
    let _waiting_b1 = wait_for_run_status(&scoped_b, &run_b1.run_id, RunStatus::WaitingInput).await;

    // Verify follower cursors before list_activity are 0 and wakeup is Pending.
    let status_before = scoped_a.status().await.expect("status A before");
    assert_eq!(status_before.binding.delivered_seq, 0);
    assert_eq!(status_before.binding.acked_seq, 0);
    assert_eq!(status_before.wakeup.status, WakeupStatus::Pending);

    // 3. Page through binding A activity using ScopedClient::list_activity (limit = 2).
    let p1 = scoped_a
        .list_activity(None, None, Some(2), None, None)
        .await
        .expect("page 1");
    assert_eq!(p1.high_water_mark_seq, 6);
    assert_eq!(p1.after_seq, 0);
    assert!(p1.has_more);
    assert_eq!(
        p1.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let c1 = p1.next_cursor.clone().expect("c1");

    // Emit more events on A1 (answer + completion -> seq 7 and 8) while cursor c1 is open!
    let q_a1 = waiting_a1.active_question_id.expect("question_id a1");
    scoped_a
        .answer(
            "act-a1-ans".to_string(),
            run_a1.run_id.clone(),
            q_a1,
            1,
            "Approved A1".to_string(),
        )
        .await
        .expect("answer a1");
    let _completed_a1 = wait_for_run_status(&scoped_a, &run_a1.run_id, RunStatus::Completed).await;

    // Walking c1 must stay pinned to high_water_mark_seq = 6.
    let p2 = scoped_a
        .list_activity(None, Some(c1.clone()), Some(2), None, None)
        .await
        .expect("page 2");
    assert_eq!(p2.high_water_mark_seq, 6);
    assert_eq!(p2.after_seq, 2);
    assert!(p2.has_more);
    assert_eq!(
        p2.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3, 4]
    );

    let p3 = scoped_a
        .list_activity(None, p2.next_cursor.clone(), Some(2), None, None)
        .await
        .expect("page 3");
    assert_eq!(p3.high_water_mark_seq, 6);
    assert_eq!(p3.after_seq, 4);
    assert!(!p3.has_more);
    assert_eq!(p3.next_cursor, None);
    assert_eq!(
        p3.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![5, 6]
    );

    // 4. Live viewer advances from previous high_water_mark_seq = 6 -> receives seq 7..=8.
    let live_delta = scoped_a
        .list_activity(Some(p3.high_water_mark_seq), None, Some(10), None, None)
        .await
        .expect("live delta");
    assert_eq!(live_delta.after_seq, 6);
    assert_eq!(live_delta.high_water_mark_seq, 8);
    assert!(!live_delta.has_more);
    assert_eq!(live_delta.next_cursor, None);
    assert_eq!(
        live_delta.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![7, 8]
    );

    // Polling again at high_water_mark_seq = 8 when no new events exist returns empty page with high_water_mark_seq = 8.
    let live_caught_up = scoped_a
        .list_activity(
            Some(live_delta.high_water_mark_seq),
            None,
            Some(10),
            None,
            None,
        )
        .await
        .expect("live caught up");
    assert!(live_caught_up.events.is_empty());
    assert!(!live_caught_up.has_more);
    assert_eq!(live_caught_up.high_water_mark_seq, 8);
    assert_eq!(live_caught_up.after_seq, 8);

    // 5. CLI `threadlane call --request {"op":"list_activity",...}` works with filters.
    let cli_filtered = run_cli_cmd(
        bin,
        &[
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding_a.binding_id,
            "--request",
            &serde_json::to_string(&ScopedRequest::ListActivity {
                after_seq: None,
                cursor: None,
                limit: Some(10),
                run_id: Some(run_a1.run_id.clone()),
                kind: Some(EventKind::RunCompleted),
            })
            .unwrap(),
        ],
    )
    .await;
    assert_eq!(cli_filtered["kind"], json!("activity"));
    assert_eq!(cli_filtered["data"]["high_water_mark_seq"], json!(8));
    assert_eq!(cli_filtered["data"]["after_seq"], json!(0));
    assert_eq!(cli_filtered["data"]["has_more"], json!(false));
    let cli_events = cli_filtered["data"]["events"].as_array().expect("events");
    assert_eq!(cli_events.len(), 1);
    assert_eq!(cli_events[0]["seq"], json!(8));
    assert_eq!(cli_events[0]["kind"], json!("run_completed"));
    assert_eq!(cli_events[0]["run_id"], json!(run_a1.run_id));

    // 6. Stdio MCP `list_activity` tool discovery, schema, filtering, pagination, and error envelopes.
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
                "clientInfo": { "name": "activity-mcp-test", "version": "0.1.0" }
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
        &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
    )
    .await;
    let list_resp = read_json_rpc_response(&mut reader, 2).await;
    let tools = list_resp["result"]["tools"]
        .as_array()
        .expect("tools array");
    assert_eq!(
        tools.len(),
        24,
        "standalone MCP tool count includes the operating guide and eight ordinary backlog tools"
    );
    let activity_tool = tools
        .iter()
        .find(|t| t["name"] == json!("list_activity"))
        .expect("list_activity tool in tools/list");
    assert_eq!(activity_tool["annotations"]["readOnlyHint"], json!(true));
    assert_eq!(activity_tool["annotations"]["openWorldHint"], json!(false));
    let schema_props = activity_tool["inputSchema"]["properties"]
        .as_object()
        .expect("list_activity properties");
    assert!(schema_props.contains_key("after_seq"));
    assert!(schema_props.contains_key("cursor"));
    assert!(schema_props.contains_key("limit"));
    assert!(schema_props.contains_key("run_id"));
    assert!(schema_props.contains_key("kind"));
    assert!(!schema_props.contains_key("binding_id"));
    assert!(!schema_props.contains_key("now_ms"));

    // Call `list_activity` via MCP with `run_id` filter and `limit: 2`.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "list_activity",
                "arguments": {
                    "run_id": run_a1.run_id,
                    "limit": 2
                }
            }
        }),
    )
    .await;
    let mcp_page_1 = read_json_rpc_response(&mut reader, 3).await;
    assert_eq!(mcp_page_1["result"]["isError"], json!(false));
    let sc_1 = &mcp_page_1["result"]["structuredContent"];
    assert_eq!(sc_1["untrusted_data"], json!(true));
    assert_eq!(sc_1["demo_only"], json!(false));
    assert_eq!(sc_1["tool"], json!("list_activity"));
    assert_eq!(sc_1["binding_id"], json!(binding_a.binding_id));
    assert_eq!(sc_1["data"]["has_more"], json!(true));
    assert_eq!(sc_1["data"]["high_water_mark_seq"], json!(8));
    let mcp_cursor = sc_1["data"]["next_cursor"]
        .as_str()
        .expect("mcp next_cursor")
        .to_string();

    //Simultaneous cursor + after_seq over MCP returns structured validation_failed error.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "list_activity",
                "arguments": {
                    "after_seq": 1,
                    "cursor": mcp_cursor,
                    "run_id": run_a1.run_id,
                    "limit": 2
                }
            }
        }),
    )
    .await;
    let mcp_both_err = read_json_rpc_response(&mut reader, 4).await;
    assert_eq!(mcp_both_err["result"]["isError"], json!(true));
    assert_eq!(
        mcp_both_err["result"]["structuredContent"]["error"]["code"],
        json!("validation_failed")
    );

    // Wrong filter on cursor over MCP returns structured validation_failed error.
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "list_activity",
                "arguments": {
                    "cursor": mcp_cursor,
                    "run_id": run_a2.run_id,
                    "limit": 2
                }
            }
        }),
    )
    .await;
    let mcp_wrong_filter = read_json_rpc_response(&mut reader, 5).await;
    assert_eq!(mcp_wrong_filter["result"]["isError"], json!(true));
    assert_eq!(
        mcp_wrong_filter["result"]["structuredContent"]["error"]["code"],
        json!("validation_failed")
    );

    // Cross-binding cursor misuse over SDK/HTTP returns ErrorCode::ValidationFailed.
    let cross_bind_err = scoped_b
        .list_activity(
            None,
            Some(mcp_cursor.clone()),
            Some(2),
            Some(run_b1.run_id.clone()),
            None,
        )
        .await
        .expect_err("cross-binding cursor must fail");
    assert_eq!(
        cross_bind_err.to_api_error().code,
        ErrorCode::ValidationFailed
    );

    // Verify all read-only list_activity operations left follower cursors at 0 and wakeup Pending.
    let status_after = scoped_a.status().await.expect("status A after");
    assert_eq!(status_after.binding.delivered_seq, 0);
    assert_eq!(status_after.binding.acked_seq, 0);
    assert_eq!(status_after.wakeup.status, WakeupStatus::Pending);

    // Pull and acknowledge events on A to confirm follower delivery still starts from seq 1.
    let pulled = scoped_a
        .pull_events(None, Some(50))
        .await
        .expect("pull_events after list_activity");
    assert_eq!(pulled.from_seq, 1);
    assert_eq!(pulled.to_seq, 8);
    assert_eq!(pulled.delivered_seq, 8);
    assert_eq!(pulled.acked_seq, 0);
    let acked = scoped_a
        .ack_events("act-ack-all".to_string(), pulled.batch_id)
        .await
        .expect("ack_events");
    assert_eq!(acked.acked_seq, 8);
    assert!(acked.advanced);
    assert_eq!(acked.wakeup_status, WakeupStatus::Idle);

    // Reopen supervisor and verify list_activity still returns all 8 events while acked_seq = 8.
    drop(stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;

    let handle_reopened = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("restart supervisor");
    let scoped_a_reopened = SupervisorClient::from_state_dir(&state_dir)
        .expect("reopened client")
        .scoped(&binding_a.binding_id)
        .expect("reopened scoped A");
    let reopened_activity = scoped_a_reopened
        .list_activity(None, None, Some(20), None, None)
        .await
        .expect("list_activity after reopen");
    assert_eq!(reopened_activity.high_water_mark_seq, 8);
    assert_eq!(reopened_activity.events.len(), 8);
    let reopened_status = scoped_a_reopened.status().await.expect("reopened status");
    assert_eq!(reopened_status.binding.delivered_seq, 8);
    assert_eq!(reopened_status.binding.acked_seq, 8);

    handle_reopened.shutdown().await;
}

#[tokio::test]
async fn test_list_activity_mcp_256kib_wire_budget_splits_pages_without_truncating_payloads() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("activity_mcp_budget_state");

    let handle = Supervisor::start(fast_supervisor_config(&state_dir))
        .await
        .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID_A, "MCP Activity Budget Chat")
        .await
        .expect("create binding");
    let scoped = client.scoped(&binding.binding_id).expect("scoped");

    let make_max_instruction = |rev: u64| -> String {
        let suffix = format!("mcp-act-rev-{rev:03}-");
        let control_prefix = "\u{1}".repeat(2_410);
        let pad_len = MAX_INSTRUCTION_LEN - control_prefix.len() - suffix.len();
        format!("{control_prefix}{suffix}{}", "w".repeat(pad_len))
    };

    let started = scoped
        .start_demo(
            "act-budget-start".to_string(),
            make_max_instruction(1),
            Some(10),
        )
        .await
        .expect("start_demo");
    let _ = wait_for_run_status(&scoped, &started.run_id, RunStatus::WaitingInput).await;

    let total_revisions = 12_u64;
    for rev in 2..=total_revisions {
        scoped
            .steer(
                format!("act-budget-steer-{rev}"),
                started.run_id.clone(),
                rev - 1,
                make_max_instruction(rev),
            )
            .await
            .expect("steer");
        for _ in 0..80 {
            let r = scoped
                .get_run(started.run_id.clone())
                .await
                .expect("get_run");
            if r.revision == rev && r.status == RunStatus::WaitingInput {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    let mut mcp_child = spawn_mcp_child(&state_dir, &binding.binding_id);
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
                "clientInfo": { "name": "activity-budget-mcp", "version": "0.1.0" }
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

    let mut cursor: Option<String> = None;
    let mut req_id = 10_u64;
    let mut page_count = 0_usize;
    let mut collected_seqs = Vec::new();
    let mut saw_budget_split = false;
    let mut anchored_high_water = None;
    let mut instructions_seen = 0;

    loop {
        req_id += 1;
        send_json_rpc(
            &mut stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "list_activity",
                    "arguments": {
                        "cursor": cursor,
                        "limit": MAX_PAGE_LIMIT
                    }
                }
            }),
        )
        .await;
        let resp = read_json_rpc_response(&mut reader, req_id).await;
        assert_eq!(resp["result"]["isError"], json!(false));

        let call_tool_result_bytes = serde_json::to_string(&resp["result"])
            .expect("serialize MCP CallToolResult")
            .len();
        assert!(
            call_tool_result_bytes <= MAX_PAGE_RESPONSE_BYTES,
            "MCP CallToolResult for list_activity was {call_tool_result_bytes} bytes > {MAX_PAGE_RESPONSE_BYTES}"
        );

        let page_data = &resp["result"]["structuredContent"]["data"];
        let high_water = page_data["high_water_mark_seq"]
            .as_u64()
            .expect("high water");
        assert_eq!(*anchored_high_water.get_or_insert(high_water), high_water);
        let page_events = page_data["events"].as_array().expect("events array");
        assert!(!page_events.is_empty());
        page_count += 1;

        for ev in page_events {
            let seq = ev["seq"].as_u64().expect("seq u64");
            let rev = ev["revision"].as_u64().expect("revision u64");
            let kind = ev["kind"].as_str().expect("kind str");
            if kind == "run_started" || kind == "run_steered" {
                instructions_seen += 1;
                assert_eq!(
                    ev["payload"]["instruction"].as_str(),
                    Some(make_max_instruction(rev).as_str()),
                    "event payload at seq {seq} must never be truncated"
                );
            }
            collected_seqs.push(seq);
        }

        if page_data["has_more"] == json!(true) {
            if page_events.len() < MAX_PAGE_LIMIT {
                saw_budget_split = true;
            }
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
        saw_budget_split,
        "expected 256 KiB budget to split 50-item request"
    );
    assert!(
        page_count > 1,
        "expected budget pagination, got {page_count} page"
    );
    for (idx, seq) in collected_seqs.iter().enumerate() {
        assert_eq!(*seq, (idx as u64) + 1);
    }
    assert_eq!(collected_seqs.last().copied(), anchored_high_water);
    assert_eq!(instructions_seen, total_revisions);

    // Also verify build_structured_tool_result against estimate_wire_response_bytes directly.
    let first_page = scoped
        .list_activity(None, None, Some(MAX_PAGE_LIMIT), None, None)
        .await
        .expect("first page");
    let structured_res = ThreadlaneMcpServer::build_structured_tool_result(
        &binding.binding_id,
        "list_activity",
        &first_page,
    );
    let structured_json = serde_json::to_string(&structured_res).expect("serialize structured_res");
    let estimated =
        estimate_wire_response_bytes(&CommandOutput::Activity(first_page)).expect("estimate");
    assert!(structured_json.len() <= MAX_PAGE_RESPONSE_BYTES);
    assert!(structured_json.len() <= estimated);

    drop(stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;
}

async fn wait_for_run_status(
    scoped: &threadlane::ScopedClient,
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
