use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_native::{PairedMcpServer, PairingStore};
use tokio::process::Command;

const THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
const THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";

fn probe_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_threadlane-native-probe"))
}

fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake codex script");
    let mut perms = fs::metadata(&path).expect("stat script").permissions();
    perms.set_mode(0o700);
    fs::set_permissions(&path, perms).expect("chmod 0700 script");
    path
}

async fn run_probe(args: &[&str]) -> Output {
    Command::new(probe_bin())
        .args(args)
        .output()
        .await
        .expect("execute threadlane-native-probe")
}

fn status_response_template() -> String {
    let inner_text = serde_json::to_string(&json!({
        "untrusted_data": true,
        "demo_only": false,
        "binding_id": "__BINDING_ID__",
        "tool": "status",
        "data": {
            "untrusted_data": true,
            "binding": {
                "binding_id": "__BINDING_ID__",
                "thread_uuid": "__THREAD_UUID__"
            }
        }
    }))
    .expect("serialize status inner template");
    serde_json::to_string(&json!({
        "id": 2,
        "result": {
            "content": [{
                "type": "text",
                "text": inner_text
            }],
            "isError": false
        }
    }))
    .expect("serialize status response template")
}

fn assert_no_secret_leaks(output: &Output, secrets: &[&str]) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    for secret in secrets {
        assert!(
            !secret.is_empty(),
            "test secret to check must not be empty"
        );
        assert!(
            !stdout.contains(secret),
            "stdout leaked secret `{secret}`:\n{stdout}"
        );
        assert!(
            !stderr.contains(secret),
            "stderr leaked secret `{secret}`:\n{stderr}"
        );
    }
}

#[tokio::test]
async fn inspect_queries_exact_thread_and_mcp_rpcs_parses_content_text_without_enrolling_or_leaking_secrets(
) {
    let tmp = TempDir::new().expect("tempdir");
    let project_root = tmp.path().join("project-workspace");
    fs::create_dir_all(&project_root).expect("create project root");
    let canonical_root = fs::canonicalize(&project_root).expect("canonical project root");

    let state_dir = tmp.path().join("state-dir");
    let pairing_dir = tmp.path().join("pairing-dir");
    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let mcp = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create unbound mcp");

    let nonce = mcp.connection_nonce().to_string();
    let epoch = mcp.epoch_id().to_string();
    let secret_title = "Private User Chat Title Do Not Print";
    let secret_transcript = "Confidential user transcript prompt text that must never leak";

    // Build exact rmcp CallToolResult::structured JSON string inside `content[0].text`
    // (with `structuredContent` omitted) to verify `content[0].text` wrapper parsing.
    let rmcp_identity_wrapper = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": nonce,
            "epoch_id": epoch,
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let escaped_content_item = json!([{
        "type": "text",
        "text": serde_json::to_string(&rmcp_identity_wrapper).expect("serialize wrapper")
    }]);

    let req_log = tmp.path().join("inspect_requests.jsonl");
    let sock_path = tmp.path().join("inspect.sock");
    fs::write(&sock_path, b"").expect("write placeholder socket");

    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "${{1:-}}" != "app-server" || "${{2:-}}" != "proxy" || "${{3:-}}" != "--sock" ]]; then
  echo "unexpected argv: $*" >&2
  exit 2
fi
read -r init_line
printf '%s\n' "$init_line" >> "{log}"
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r initialized_line
printf '%s\n' "$initialized_line" >> "{log}"
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread}","sessionId":"shared-session-fork-ignored","projectId":"proj-inspect-1","name":"{title}","cwd":"{cwd}","status":{{"type":"idle"}},"preview":"{transcript}"}}}}}}'
elif [[ "$req_line" == *'"method":"mcpServerStatus/list"'* ]]; then
  printf '%s\n' '{{"id":2,"result":{{"data":[{{"name":"threadlane","authStatus":"unsupported","runtimeStatus":"connected","tools":{{"connection_identity":{{}},"status":{{}}}}}}]}}}}'
elif [[ "$req_line" == *'"method":"mcpServer/tool/call"'* ]]; then
  printf '%s\n' '{{"id":2,"result":{{"content":{content_json},"isError":false}}}}'
else
  printf '%s\n' '{{"id":2,"error":{{"code":-32601,"message":"unexpected method"}}}}'
fi
"#,
        log = req_log.display(),
        thread = THREAD_A,
        title = secret_title,
        cwd = canonical_root.display(),
        transcript = secret_transcript,
        content_json = serde_json::to_string(&escaped_content_item).expect("content json"),
    );
    let codex_exe = write_executable_script(tmp.path(), "fake-codex-inspect.sh", &script);

    let out = run_probe(&[
        "inspect",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--socket",
        sock_path.to_str().unwrap(),
        "--thread",
        THREAD_A,
        "--project-root",
        canonical_root.to_str().unwrap(),
    ])
    .await;

    assert!(
        out.status.success(),
        "inspect failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_no_secret_leaks(
        &out,
        &[
            &operator_token,
            &nonce,
            &epoch,
            secret_title,
            secret_transcript,
            "shared-session-fork-ignored",
        ],
    );

    let report: Value = serde_json::from_slice(&out.stdout).expect("parse InspectReport JSON");
    assert_eq!(report["mode"], "inspect");
    assert_eq!(report["configured"], true);
    assert_eq!(report["thread_id"], THREAD_A);
    assert_eq!(report["thread_id_matched"], true);
    assert_eq!(report["project_id"], "proj-inspect-1");
    assert_eq!(report["cwd"], canonical_root.to_string_lossy().as_ref());
    assert_eq!(report["cwd_match"], true);
    assert_eq!(report["mcp_server_name"], "threadlane");
    assert_eq!(report["mcp_present"], true);
    assert_eq!(report["mcp_runtime_status"], "connected");
    assert_eq!(report["connection_identity_tool_listed"], true);
    assert_eq!(report["identity_shape_valid"], true);
    assert_eq!(report["connection_state"], "unbound");
    assert_eq!(report["enrolled_binding_id"], Value::Null);
    assert_eq!(report["enrolled_on_inspect"], false);
    assert_eq!(report["verification_gate"], "needs_verification");
    assert_eq!(report["two_chat_routing_gate"], "needs_verification");
    assert_eq!(report["restart_persistence_gate"], "needs_verification");
    assert_eq!(report["busy_turn_delivery_gate"], "needs_verification");
    assert_eq!(report["overall_live_gate"], "needs_verification");

    // Verify `inspect` did not enroll the connection in `PairingStore`.
    let still_unbound = pairing_store
        .inspect_epoch(&nonce, &epoch)
        .expect("inspect epoch");
    assert_eq!(still_unbound.state.as_str(), "unbound");
    assert_eq!(still_unbound.enrolled_binding_id, None);

    // Verify exact JSON-RPC calls logged (`thread/read`, `mcpServerStatus/list`, `mcpServer/tool/call`).
    let logged: Vec<Value> = fs::read_to_string(&req_log)
        .expect("read request log")
        .lines()
        .map(|l| serde_json::from_str(l).expect("parse request line"))
        .collect();
    let rpc_calls: Vec<&Value> = logged
        .iter()
        .filter(|v| v["method"] != "initialize" && v["method"] != "initialized")
        .collect();
    assert_eq!(rpc_calls.len(), 3);
    assert_eq!(rpc_calls[0]["method"], "thread/read");
    assert_eq!(rpc_calls[0]["params"]["threadId"], THREAD_A);
    assert_eq!(rpc_calls[0]["params"]["includeTurns"], false);

    assert_eq!(rpc_calls[1]["method"], "mcpServerStatus/list");
    assert_eq!(rpc_calls[1]["params"]["threadId"], THREAD_A);
    assert_eq!(rpc_calls[1]["params"]["serverName"], "threadlane");
    assert_eq!(rpc_calls[1]["params"]["detail"], "toolsAndAuthOnly");

    assert_eq!(rpc_calls[2]["method"], "mcpServer/tool/call");
    assert_eq!(rpc_calls[2]["params"]["threadId"], THREAD_A);
    assert_eq!(rpc_calls[2]["params"]["server"], "threadlane");
    assert_eq!(rpc_calls[2]["params"]["tool"], "connection_identity");
    assert_eq!(rpc_calls[2]["params"]["arguments"], json!({}));
}

#[tokio::test]
async fn inspect_and_pair_check_reject_cwd_mismatch_missing_allow_enroll_and_duplicate_threads() {
    let tmp = TempDir::new().expect("tempdir");
    let expected_root = tmp.path().join("expected-project");
    let wrong_root = tmp.path().join("other-project");
    fs::create_dir_all(&expected_root).expect("create expected root");
    fs::create_dir_all(&wrong_root).expect("create wrong root");
    let canonical_expected = fs::canonicalize(&expected_root).expect("canonical expected");
    let canonical_wrong = fs::canonicalize(&wrong_root).expect("canonical wrong");

    let req_log = tmp.path().join("mismatch_requests.jsonl");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_a}","cwd":"{expected_cwd}","status":{{"type":"idle"}}}}}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_b}","cwd":"{wrong_cwd}","status":{{"type":"idle"}}}}}}}}'
  fi
else
  printf '%s\n' '{{"id":2,"error":{{"code":-32600,"message":"should not be called after cwd mismatch"}}}}'
fi
"#,
        log = req_log.display(),
        thread_a = THREAD_A,
        thread_b = THREAD_B,
        expected_cwd = canonical_expected.display(),
        wrong_cwd = canonical_wrong.display(),
    );
    let codex_exe = write_executable_script(tmp.path(), "fake-codex-mismatch.sh", &script);

    // 1. `inspect` on THREAD_B (whose cwd is `wrong_root`) must fail with `project_mismatch`
    //    before issuing any MCP status or tool call.
    let inspect_err_out = run_probe(&[
        "inspect",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--thread",
        THREAD_B,
        "--project-root",
        canonical_expected.to_str().unwrap(),
    ])
    .await;
    assert!(!inspect_err_out.status.success());
    let inspect_err: Value =
        serde_json::from_slice(&inspect_err_out.stderr).expect("parse error JSON");
    assert_eq!(inspect_err["code"], "project_mismatch");

    let logged_after_inspect: Vec<Value> = fs::read_to_string(&req_log)
        .expect("read log")
        .lines()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    assert_eq!(
        logged_after_inspect.len(),
        1,
        "only thread/read should be called when cwd mismatches"
    );
    assert_eq!(logged_after_inspect[0]["method"], "thread/read");

    // 2. Start a real disposable supervisor for `pair-check` validation checks.
    let state_dir = tmp.path().join("sup-state");
    let pairing_dir = tmp.path().join("pair-state");
    let sup_handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(50),
        codex: None,
        workers: None,
    })
    .await
    .expect("start supervisor");
    let sup_client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");

    // 2a. Missing `--allow-enroll` is rejected with `validation_failed`.
    let no_flag_out = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_expected.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
    ])
    .await;
    assert!(!no_flag_out.status.success());
    let no_flag_err: Value = serde_json::from_slice(&no_flag_out.stderr).expect("parse error JSON");
    assert_eq!(no_flag_err["code"], "validation_failed");
    assert!(sup_client
        .list_bindings()
        .await
        .expect("list bindings")
        .is_empty());

    // 2b. Identical `--thread-a` and `--thread-b` is rejected with `validation_failed`.
    let dup_thread_out = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_expected.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_A,
        "--allow-enroll",
    ])
    .await;
    assert!(!dup_thread_out.status.success());
    let dup_err: Value = serde_json::from_slice(&dup_thread_out.stderr).expect("parse error JSON");
    assert_eq!(dup_err["code"], "validation_failed");

    // 2c. Mismatched `cwd` on `thread-b` in `pair-check` fails with `project_mismatch`
    //     without creating any supervisor bindings.
    let pair_mismatch_out = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_expected.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
        "--allow-enroll",
    ])
    .await;
    assert!(!pair_mismatch_out.status.success());
    let pair_mismatch_err: Value =
        serde_json::from_slice(&pair_mismatch_out.stderr).expect("parse error JSON");
    assert_eq!(pair_mismatch_err["code"], "project_mismatch");
    assert!(
        sup_client
            .list_bindings()
            .await
            .expect("list bindings")
            .is_empty(),
        "no bindings may be created when thread cwd verification fails"
    );

    sup_handle.shutdown().await;
}

#[tokio::test]
async fn pair_check_quarantines_shared_child_connection_and_reports_blocked_for_both_threads() {
    let tmp = TempDir::new().expect("tempdir");
    let project_root = tmp.path().join("shared-project");
    fs::create_dir_all(&project_root).expect("create project root");
    let canonical_root = fs::canonicalize(&project_root).expect("canonical project root");

    let state_dir = tmp.path().join("sup-state");
    let pairing_dir = tmp.path().join("pair-state");

    let sup_handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(50),
        codex: None,
        workers: None,
    })
    .await
    .expect("start supervisor");

    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let shared_mcp =
        PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create shared mcp");
    let shared_nonce = shared_mcp.connection_nonce().to_string();
    let shared_epoch = shared_mcp.epoch_id().to_string();

    let rmcp_shared_identity = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": shared_nonce,
            "epoch_id": shared_epoch,
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let shared_content = json!([{
        "type": "text",
        "text": serde_json::to_string(&rmcp_shared_identity).expect("serialize wrapper")
    }]);

    let req_log = tmp.path().join("shared_requests.jsonl");
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_a}","projectId":"proj-shared","name":"Secret Chat A","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_b}","projectId":"proj-shared","name":"Secret Chat B","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  fi
elif [[ "$req_line" == *'"method":"mcpServer/tool/call"'* ]]; then
  printf '%s\n' '{{"id":2,"result":{{"content":{content_json},"structuredContent":{structured_json},"isError":false}}}}'
fi
"#,
        log = req_log.display(),
        thread_a = THREAD_A,
        thread_b = THREAD_B,
        cwd = canonical_root.display(),
        content_json = serde_json::to_string(&shared_content).expect("content"),
        structured_json = serde_json::to_string(&rmcp_shared_identity).expect("structured"),
    );
    let codex_exe = write_executable_script(tmp.path(), "fake-codex-shared.sh", &script);

    let out = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_root.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
        "--allow-enroll",
        "--verify-status",
    ])
    .await;

    assert!(
        !out.status.success(),
        "blocked shared connection must exit non-zero"
    );
    assert_no_secret_leaks(
        &out,
        &[
            &operator_token,
            &shared_nonce,
            &shared_epoch,
            "Secret Chat A",
            "Secret Chat B",
        ],
    );

    let report: Value =
        serde_json::from_slice(&out.stdout).expect("parse PairCheckReport JSON from stdout");
    assert_eq!(report["mode"], "pair_check");
    assert_eq!(report["outcome"], "blocked_shared_connection");
    assert_eq!(report["shared_connection_detected"], true);
    assert_eq!(report["quarantined_for_both"], true);
    assert_eq!(report["distinct_connection_nonces"], false);
    assert_eq!(report["distinct_epoch_ids"], false);
    assert_eq!(report["isolated_enrollment_observed"], false);
    assert_eq!(report["status_routing_verified"], false);
    assert_eq!(
        report["per_call_binding_routing"],
        "blocked_shared_connection"
    );

    assert_eq!(report["thread_a"]["thread_id"], THREAD_A);
    assert_eq!(
        report["thread_a"]["connection_state"],
        "quarantined_shared_connection"
    );
    assert_eq!(
        report["thread_a"]["verification_gate"],
        "blocked_shared_connection"
    );
    assert_eq!(report["thread_a"]["enrolled_binding_id"], Value::Null);
    assert_eq!(
        report["thread_a"]["status_verified_binding_id"],
        Value::Null
    );

    assert_eq!(report["thread_b"]["thread_id"], THREAD_B);
    assert_eq!(
        report["thread_b"]["connection_state"],
        "quarantined_shared_connection"
    );
    assert_eq!(
        report["thread_b"]["verification_gate"],
        "blocked_shared_connection"
    );
    assert_eq!(report["thread_b"]["enrolled_binding_id"], Value::Null);
    assert_eq!(
        report["thread_b"]["status_verified_binding_id"],
        Value::Null
    );

    assert_eq!(report["two_chat_routing_gate"], "blocked_shared_connection");
    assert_eq!(report["overall_live_gate"], "blocked_shared_connection");

    // Verify `status` was never called after shared connection quarantine.
    let logged = fs::read_to_string(&req_log).expect("read request log");
    assert!(
        !logged.contains(r#""tool":"status""#),
        "pair-check must never continue to status tool calls when shared connection is detected"
    );

    // Verify `shared_mcp` is permanently quarantined in `PairingStore` so neither thread can use it.
    let quarantined_err = shared_mcp
        .invoke_tool_json("status", json!({}), None)
        .await
        .expect_err("quarantined connection must reject status calls");
    assert_eq!(
        quarantined_err.structured_content.unwrap()["error"]["code"],
        "shared_connection_rejected"
    );

    sup_handle.shutdown().await;
}

#[tokio::test]
async fn pair_check_isolated_enrollment_and_verify_status_proof_preserve_honest_gates_and_redact_secrets(
) {
    let tmp = TempDir::new().expect("tempdir");
    let project_root = tmp.path().join("isolated-project");
    fs::create_dir_all(&project_root).expect("create project root");
    let canonical_root = fs::canonicalize(&project_root).expect("canonical project root");

    let state_dir = tmp.path().join("sup-state");
    let sup_handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(50),
        codex: None,
        workers: None,
    })
    .await
    .expect("start supervisor");
    let sup_client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");

    // --- Part 1: `pair-check` WITHOUT `--verify-status` ---
    let pairing_dir_1 = tmp.path().join("pair-state-1");
    let pairing_store_1 = PairingStore::open(&pairing_dir_1).expect("open pairing store 1");
    let op_token_1 = pairing_store_1.operator_token().expect("op token 1");
    let mcp_a1 = PairedMcpServer::new(&state_dir, pairing_store_1.clone()).expect("mcp a1");
    let mcp_b1 = PairedMcpServer::new(&state_dir, pairing_store_1.clone()).expect("mcp b1");

    let id_a1 = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_a1.connection_nonce(),
            "epoch_id": mcp_a1.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let id_b1 = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_b1.connection_nonce(),
            "epoch_id": mcp_b1.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let content_a1 = json!([{"type": "text", "text": serde_json::to_string(&id_a1).unwrap()}]);
    let content_b1 = json!([{"type": "text", "text": serde_json::to_string(&id_b1).unwrap()}]);

    let req_log_1 = tmp.path().join("isolated_1.jsonl");
    let script_1 = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_a}","projectId":"proj-iso","name":"Untrusted Title A","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_b}","projectId":"proj-iso","name":"Untrusted Title B","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  fi
elif [[ "$req_line" == *'"method":"mcpServer/tool/call"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"content":{content_a},"isError":false}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"content":{content_b},"isError":false}}}}'
  fi
fi
"#,
        log = req_log_1.display(),
        thread_a = THREAD_A,
        thread_b = THREAD_B,
        cwd = canonical_root.display(),
        content_a = serde_json::to_string(&content_a1).unwrap(),
        content_b = serde_json::to_string(&content_b1).unwrap(),
    );
    let codex_exe_1 = write_executable_script(tmp.path(), "fake-codex-iso1.sh", &script_1);

    let out_1 = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe_1.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir_1.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_root.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
        "--allow-enroll",
    ])
    .await;

    assert!(
        out_1.status.success(),
        "pair-check failed: {}",
        String::from_utf8_lossy(&out_1.stderr)
    );
    assert_no_secret_leaks(
        &out_1,
        &[
            &op_token_1,
            mcp_a1.connection_nonce(),
            mcp_a1.epoch_id(),
            mcp_b1.connection_nonce(),
            mcp_b1.epoch_id(),
            "Untrusted Title A",
            "Untrusted Title B",
        ],
    );

    let report_1: Value = serde_json::from_slice(&out_1.stdout).expect("parse report 1");
    assert_eq!(report_1["outcome"], "isolated_enrollment_observed");
    assert_eq!(report_1["isolated_enrollment_observed"], true);
    assert_eq!(report_1["status_routing_verified"], false);
    assert_eq!(report_1["per_call_binding_routing"], "needs_verification");
    assert_eq!(report_1["two_chat_routing_gate"], "needs_verification");
    assert_eq!(report_1["restart_persistence_gate"], "needs_verification");
    assert_eq!(report_1["busy_turn_delivery_gate"], "needs_verification");
    assert_eq!(report_1["overall_live_gate"], "needs_verification");
    assert_eq!(
        report_1["thread_a"]["status_verified_binding_id"],
        Value::Null
    );
    assert_eq!(
        report_1["thread_b"]["status_verified_binding_id"],
        Value::Null
    );

    let logged_1 = fs::read_to_string(&req_log_1).expect("read log 1");
    assert!(!logged_1.contains(r#""tool":"status""#));

    // --- Part 2: `pair-check` WITH `--verify-status` ---
    let pairing_dir_2 = tmp.path().join("pair-state-2");
    let pairing_store_2 = PairingStore::open(&pairing_dir_2).expect("open pairing store 2");
    let op_token_2 = pairing_store_2.operator_token().expect("op token 2");
    let mcp_a2 = PairedMcpServer::new(&state_dir, pairing_store_2.clone()).expect("mcp a2");
    let mcp_b2 = PairedMcpServer::new(&state_dir, pairing_store_2.clone()).expect("mcp b2");

    let id_a2 = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_a2.connection_nonce(),
            "epoch_id": mcp_a2.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let id_b2 = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_b2.connection_nonce(),
            "epoch_id": mcp_b2.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let content_a2 = json!([{"type": "text", "text": serde_json::to_string(&id_a2).unwrap()}]);
    let content_b2 = json!([{"type": "text", "text": serde_json::to_string(&id_b2).unwrap()}]);
    let pairing_state_path = pairing_dir_2.join("pairing_state.json");

    let req_log_2 = tmp.path().join("isolated_2.jsonl");
    let status_response_2 = status_response_template();
    let script_2 = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_a}","projectId":"proj-iso","name":"Secret Title A","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_b}","projectId":"proj-iso","name":"Secret Title B","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  fi
elif [[ "$req_line" == *'"tool":"connection_identity"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"content":{content_a},"isError":false}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"content":{content_b},"isError":false}}}}'
  fi
elif [[ "$req_line" == *'"tool":"status"'* ]]; then
  binding_ids=()
  while IFS= read -r line; do
    if [[ "$line" == *'"binding_id": "'* ]]; then
      val="${{line#*\"binding_id\": \"}}"
      val="${{val%%\"*}}"
      binding_ids+=("$val")
    fi
  done < "{pairing_state}"
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    bid="${{binding_ids[0]}}"
    tid="{thread_a}"
  else
    bid="${{binding_ids[1]}}"
    tid="{thread_b}"
  fi
  status_response='{status_response_2}'
  status_response="${{status_response//__BINDING_ID__/$bid}}"
  status_response="${{status_response//__THREAD_UUID__/$tid}}"
  printf '%s\n' "$status_response"
fi
"#,
        log = req_log_2.display(),
        thread_a = THREAD_A,
        thread_b = THREAD_B,
        cwd = canonical_root.display(),
        content_a = serde_json::to_string(&content_a2).unwrap(),
        content_b = serde_json::to_string(&content_b2).unwrap(),
        pairing_state = pairing_state_path.display(),
        status_response_2 = status_response_2,
    );
    let codex_exe_2 = write_executable_script(tmp.path(), "fake-codex-iso2.sh", &script_2);

    let out_2 = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe_2.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir_2.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_root.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
        "--allow-enroll",
        "--verify-status",
    ])
    .await;

    assert!(
        out_2.status.success(),
        "pair-check --verify-status failed: {}",
        String::from_utf8_lossy(&out_2.stderr)
    );
    assert_no_secret_leaks(
        &out_2,
        &[
            &op_token_2,
            mcp_a2.connection_nonce(),
            mcp_a2.epoch_id(),
            mcp_b2.connection_nonce(),
            mcp_b2.epoch_id(),
            "Secret Title A",
            "Secret Title B",
        ],
    );

    let report_2: Value = serde_json::from_slice(&out_2.stdout).expect("parse report 2");
    assert_eq!(report_2["outcome"], "isolated_status_verified");
    assert_eq!(report_2["shared_connection_detected"], false);
    assert_eq!(report_2["quarantined_for_both"], false);
    assert_eq!(report_2["distinct_connection_nonces"], true);
    assert_eq!(report_2["distinct_epoch_ids"], true);
    assert_eq!(report_2["isolated_enrollment_observed"], true);
    assert_eq!(report_2["status_routing_verified"], true);
    assert_eq!(report_2["per_call_binding_routing"], "status_binding_verified");

    let bnd_a = report_2["thread_a"]["created_binding_id"]
        .as_str()
        .expect("created_binding_id a");
    let bnd_b = report_2["thread_b"]["created_binding_id"]
        .as_str()
        .expect("created_binding_id b");
    assert_ne!(bnd_a, bnd_b);
    assert_eq!(
        report_2["thread_a"]["enrolled_binding_id"].as_str(),
        Some(bnd_a)
    );
    assert_eq!(
        report_2["thread_a"]["status_verified_binding_id"].as_str(),
        Some(bnd_a)
    );
    assert_eq!(
        report_2["thread_b"]["enrolled_binding_id"].as_str(),
        Some(bnd_b)
    );
    assert_eq!(
        report_2["thread_b"]["status_verified_binding_id"].as_str(),
        Some(bnd_b)
    );
    assert_eq!(report_2["thread_a"]["binding_label"], "native-probe-thread-a");
    assert_eq!(report_2["thread_b"]["binding_label"], "native-probe-thread-b");

    // Restart, busy-turn, and overall live gate must remain `needs_verification`.
    assert_eq!(report_2["two_chat_routing_gate"], "needs_verification");
    assert_eq!(report_2["restart_persistence_gate"], "needs_verification");
    assert_eq!(report_2["busy_turn_delivery_gate"], "needs_verification");
    assert_eq!(report_2["overall_live_gate"], "needs_verification");

    // Confirm real `PairedMcpServer` instances delegate `status` to their own created bindings.
    let direct_status_a = mcp_a2
        .invoke_tool_json("status", json!({}), None)
        .await
        .expect("direct status A")
        .structured_content
        .expect("structured A");
    assert_eq!(direct_status_a["binding_id"].as_str(), Some(bnd_a));

    let direct_status_b = mcp_b2
        .invoke_tool_json("status", json!({}), None)
        .await
        .expect("direct status B")
        .structured_content
        .expect("structured B");
    assert_eq!(direct_status_b["binding_id"].as_str(), Some(bnd_b));

    // Confirm supervisor bindings used safe static labels rather than untrusted native titles.
    let all_bindings = sup_client.list_bindings().await.expect("list bindings");
    for b in &all_bindings {
        assert!(
            b.label == "native-probe-thread-a" || b.label == "native-probe-thread-b",
            "unexpected binding label: {}",
            b.label
        );
    }

    sup_handle.shutdown().await;
}

#[tokio::test]
async fn pair_check_verify_status_rejects_cross_wired_binding_id_and_redacts_error_tokens() {
    let tmp = TempDir::new().expect("tempdir");
    let project_root = tmp.path().join("crosswire-project");
    fs::create_dir_all(&project_root).expect("create project root");
    let canonical_root = fs::canonicalize(&project_root).expect("canonical project root");

    let state_dir = tmp.path().join("sup-state");
    let pairing_dir = tmp.path().join("pair-state");

    let sup_handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(50),
        codex: None,
        workers: None,
    })
    .await
    .expect("start supervisor");

    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let op_token = pairing_store.operator_token().expect("op token");
    let mcp_a = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("mcp a");
    let mcp_b = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("mcp b");

    let id_a = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_a.connection_nonce(),
            "epoch_id": mcp_a.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let id_b = json!({
        "untrusted_data": false,
        "tool": "connection_identity",
        "data": {
            "connection_nonce": mcp_b.connection_nonce(),
            "epoch_id": mcp_b.epoch_id(),
            "server_pid": std::process::id(),
            "state": "unbound",
            "enrolled_binding_id": null,
            "verification_gate": "needs_verification"
        }
    });
    let content_a = json!([{"type": "text", "text": serde_json::to_string(&id_a).unwrap()}]);
    let content_b = json!([{"type": "text", "text": serde_json::to_string(&id_b).unwrap()}]);
    let pairing_state_path = pairing_dir.join("pairing_state.json");

    // Cross-wired script: BOTH thread_a and thread_b return thread_a's binding_id on `status`.
    let status_response = status_response_template();
    let script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r req_line
if [[ "$req_line" == *'"method":"thread/read"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_a}","projectId":"proj-1","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"{thread_b}","projectId":"proj-1","cwd":"{cwd}","status":{{"type":"idle"}}}}}}}}'
  fi
elif [[ "$req_line" == *'"tool":"connection_identity"'* ]]; then
  if [[ "$req_line" == *'{thread_a}'* ]]; then
    printf '%s\n' '{{"id":2,"result":{{"content":{content_a},"isError":false}}}}'
  else
    printf '%s\n' '{{"id":2,"result":{{"content":{content_b},"isError":false}}}}'
  fi
elif [[ "$req_line" == *'"tool":"status"'* ]]; then
  binding_ids=()
  while IFS= read -r line; do
    if [[ "$line" == *'"binding_id": "'* ]]; then
      val="${{line#*\"binding_id\": \"}}"
      val="${{val%%\"*}}"
      binding_ids+=("$val")
    fi
  done < "{pairing_state}"
  bid="${{binding_ids[0]}}"
  tid="{thread_a}"
  status_response='{status_response}'
  status_response="${{status_response//__BINDING_ID__/$bid}}"
  status_response="${{status_response//__THREAD_UUID__/$tid}}"
  printf '%s\n' "$status_response"
fi
"#,
        thread_a = THREAD_A,
        thread_b = THREAD_B,
        cwd = canonical_root.display(),
        content_a = serde_json::to_string(&content_a).unwrap(),
        content_b = serde_json::to_string(&content_b).unwrap(),
        pairing_state = pairing_state_path.display(),
        status_response = status_response,
    );
    let codex_exe = write_executable_script(tmp.path(), "fake-codex-crosswire.sh", &script);

    let out = run_probe(&[
        "pair-check",
        "--codex",
        codex_exe.to_str().unwrap(),
        "--pairing-dir",
        pairing_dir.to_str().unwrap(),
        "--state-dir",
        state_dir.to_str().unwrap(),
        "--project-root",
        canonical_root.to_str().unwrap(),
        "--thread-a",
        THREAD_A,
        "--thread-b",
        THREAD_B,
        "--allow-enroll",
        "--verify-status",
    ])
    .await;

    assert!(!out.status.success());
    assert_no_secret_leaks(
        &out,
        &[
            &op_token,
            mcp_a.connection_nonce(),
            mcp_a.epoch_id(),
            mcp_b.connection_nonce(),
            mcp_b.epoch_id(),
        ],
    );
    let err_json: Value = serde_json::from_slice(&out.stderr).expect("parse error JSON");
    assert_eq!(err_json["code"], "security_violation");

    // Verify that if an RPC error message attempts to echo `nce_`, `epc_`, or `op_` secrets,
    // `threadlane-native-probe` scrubs them to `[REDACTED]` in stderr.
    let leak_script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '%s\n' '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}'
read -r _initialized
read -r _req_line
printf '%s\n' '{{"id":2,"error":{{"code":-32000,"message":"upstream leaked {op} and {nce} and {epc}"}}}}'
"#,
        op = op_token,
        nce = mcp_a.connection_nonce(),
        epc = mcp_a.epoch_id(),
    );
    let leak_exe = write_executable_script(tmp.path(), "fake-codex-leak-err.sh", &leak_script);
    let leak_out = run_probe(&[
        "inspect",
        "--codex",
        leak_exe.to_str().unwrap(),
        "--thread",
        THREAD_A,
        "--project-root",
        canonical_root.to_str().unwrap(),
    ])
    .await;
    assert!(!leak_out.status.success());
    assert_no_secret_leaks(
        &leak_out,
        &[&op_token, mcp_a.connection_nonce(), mcp_a.epoch_id()],
    );
    let leak_err_json: Value =
        serde_json::from_slice(&leak_out.stderr).expect("parse scrubbed error JSON");
    assert_eq!(leak_err_json["code"], "rpc_error");
    assert!(leak_err_json["message"]
        .as_str()
        .unwrap()
        .contains("[REDACTED]"));

    sup_handle.shutdown().await;
}
