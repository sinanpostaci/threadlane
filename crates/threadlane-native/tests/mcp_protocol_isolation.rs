//! Ensure the paired server cannot silently accept stateless shared connections.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[tokio::test]
async fn paired_server_rejects_stateless_requests_and_negotiates_a_session_handshake() {
    let temp = tempfile::tempdir().expect("private temporary directory");
    let mut server = Command::new(env!("CARGO_BIN_EXE_threadlane-native-mcp"))
        .args([
            "--state-dir",
            temp.path().join("runtime").to_str().expect("runtime path"),
            "--pairing-dir",
            temp.path().join("pairing").to_str().expect("pairing path"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("start owned MCP server");
    let mut input = server.stdin.take().expect("MCP stdin");
    let mut output = BufReader::new(server.stdout.take().expect("MCP stdout")).lines();

    let stateless_request = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/list",
        "params": {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {}
        }}
    });
    input
        .write_all(format!("{stateless_request}\n").as_bytes())
        .await
        .expect("write stateless request");
    let rejected = tokio::time::timeout(Duration::from_secs(5), output.next_line())
        .await
        .expect("bounded stateless response")
        .expect("read response")
        .expect("server response");
    let rejected: Value = serde_json::from_str(&rejected).expect("JSON-RPC error");
    assert_eq!(rejected["id"], 1);
    assert!(rejected.get("error").is_some());

    let initialize = json!({
        "jsonrpc": "2.0", "id": 2, "method": "initialize",
        "params": {
            "protocolVersion": "2026-07-28",
            "capabilities": {},
            "clientInfo": {"name": "threadlane-test", "version": "0.1.0"}
        }
    });
    input
        .write_all(format!("{initialize}\n").as_bytes())
        .await
        .expect("write initialize");
    let initialized = tokio::time::timeout(Duration::from_secs(5), output.next_line())
        .await
        .expect("bounded initialize response")
        .expect("read response")
        .expect("server response");
    let initialized: Value = serde_json::from_str(&initialized).expect("JSON-RPC response");
    assert_eq!(initialized["id"], 2);
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    let instructions = initialized["result"]["instructions"]
        .as_str()
        .expect("handshake instructions");
    let first_512: String = instructions.chars().take(512).collect();
    assert!(first_512.contains("2026-11-02.v2"));
    assert!(first_512.contains("Follower"));
    assert!(first_512.contains("connect_current_chat"));
    assert!(first_512.contains("operating_guide"));

    input
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .expect("complete handshake");
    for (id, meta, expected_code) in [
        (3, json!({}), "needs_verification"),
        (
            4,
            json!({
                "threadId": "0196ed00-0000-7000-8000-00000000000a",
                "title": "Spoofed Chat Title",
                "model": "gpt-spoofed",
                "projectPath": "/tmp/spoofed"
            }),
            "not_configured",
        ),
    ] {
        let request = json!({"jsonrpc":"2.0", "id":id, "method":"tools/call",
            "params":{"name":"connect_current_chat", "arguments":{}, "_meta":meta}});
        input
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("request pairing");
        let response = tokio::time::timeout(Duration::from_secs(5), output.next_line())
            .await
            .expect("bounded pairing response")
            .expect("read pairing response")
            .expect("response exists");
        let response: Value = serde_json::from_str(&response).expect("pairing response JSON");
        assert_eq!(response["id"], id);
        assert_eq!(response["result"]["isError"], true);
        assert_eq!(
            response["result"]["structuredContent"]["error"]["code"],
            expected_code
        );
        assert!(
            !temp.path().join("runtime").exists(),
            "unverified metadata cannot start a runtime"
        );
    }

    drop(input);
    let _ = tokio::time::timeout(Duration::from_secs(5), server.wait())
        .await
        .expect("owned server exits after EOF");
}
