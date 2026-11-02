#![cfg(target_os = "linux")]

use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{symlink, DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::NativeReconnectConfig;
use threadlane_native::{
    execute_ws_rpc, validate_ws_readonly_request, ConnectionEpochState, NativeClient,
    NativeErrorCode, NativeTransport, NativeVerificationStatus, NativeWakeupStore, PairingStore,
    MAX_SKIPPED_NOTIFICATIONS,
};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
const TEST_THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";

fn read_ppid(pid: u32) -> u32 {
    let stat_path = format!("/proc/{pid}/stat");
    let raw = fs::read_to_string(&stat_path).expect("read /proc/<pid>/stat");
    let rparen = raw.rfind(')').expect("closing paren in /proc/<pid>/stat");
    let after = &raw[rparen + 1..];
    let mut fields = after.split_whitespace();
    let _state = fields.next().expect("process state field");
    let ppid_str = fields.next().expect("process ppid field");
    ppid_str.parse::<u32>().expect("numeric ppid")
}

struct WsTestEnv {
    _temp: tempfile::TempDir,
    root: PathBuf,
    project_root: PathBuf,
    sock_dir: PathBuf,
    descriptor_root: PathBuf,
    backend_pid: u32,
    wrapper_pid: u32,
    backend_executable: PathBuf,
    application_executable: PathBuf,
    staging_counter: AtomicUsize,
}

impl WsTestEnv {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let project_root = root.join("workspace-repo");
        fs::create_dir(&project_root).expect("create workspace-repo");

        let sock_dir = root.join("secret-sock-dir");
        DirBuilder::new()
            .mode(0o700)
            .create(&sock_dir)
            .expect("create sock_dir");

        let descriptor_root = root.join("secret-descriptors-root");
        DirBuilder::new()
            .mode(0o700)
            .create(&descriptor_root)
            .expect("create descriptor_root");

        let backend_pid = std::process::id();
        let wrapper_pid = read_ppid(backend_pid);
        let app_pid = read_ppid(wrapper_pid);
        let backend_executable =
            fs::canonicalize("/proc/self/exe").expect("canonical backend executable");
        let application_executable = fs::canonicalize(format!("/proc/{app_pid}/exe"))
            .expect("canonical application executable");

        Self {
            _temp: temp,
            root,
            project_root,
            sock_dir,
            descriptor_root,
            backend_pid,
            wrapper_pid,
            backend_executable,
            application_executable,
            staging_counter: AtomicUsize::new(0),
        }
    }

    fn reconnect_config(&self) -> NativeReconnectConfig {
        NativeReconnectConfig {
            descriptor_root: self.descriptor_root.clone(),
            backend_executable: self.backend_executable.clone(),
            application_executable: self.application_executable.clone(),
        }
    }

    fn client(&self) -> NativeClient {
        NativeClient::new_managed_websocket(self.reconnect_config())
            .expect("create managed WebSocket NativeClient")
    }

    fn publish_symlinked_session(&self, suffix: &str) -> (UnixListener, PathBuf) {
        let seq = self.staging_counter.fetch_add(1, Ordering::SeqCst);
        let physical_sock = self.sock_dir.join(format!("phys-{suffix}-{seq}.sock"));
        let listener = UnixListener::bind(&physical_sock).expect("bind physical unix socket");
        fs::set_permissions(&physical_sock, fs::Permissions::from_mode(0o600))
            .expect("secure physical socket");

        let staging_dir = self.root.join(format!("staging-{suffix}-{seq}"));
        DirBuilder::new()
            .mode(0o700)
            .create(&staging_dir)
            .expect("create staging dir");

        let final_session_dir = self.descriptor_root.join(format!("tl-codex-{suffix}"));
        let advertised_sock = final_session_dir.join("codex.sock");
        symlink(&physical_sock, staging_dir.join("codex.sock")).expect("symlink session socket");

        let descriptor_json = serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "wrapper_pid": self.wrapper_pid,
            "backend_pid": self.backend_pid,
            "socket_path": advertised_sock,
            "real_codex": self.backend_executable,
            "transport": "unix_websocket",
        }))
        .expect("serialize descriptor json");

        let mut desc_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(staging_dir.join("descriptor.json"))
            .expect("create descriptor.json");
        desc_file
            .write_all(&descriptor_json)
            .expect("write descriptor.json");
        desc_file.sync_all().expect("sync descriptor.json");
        drop(desc_file);

        fs::rename(&staging_dir, &final_session_dir).expect("atomically publish session dir");
        (listener, final_session_dir)
    }

    fn publish_direct_session(&self, suffix: &str) -> (UnixListener, PathBuf, PathBuf) {
        let session_dir = self.descriptor_root.join(format!("tl-codex-{suffix}"));
        DirBuilder::new()
            .mode(0o700)
            .create(&session_dir)
            .expect("create session dir");
        let sock_path = session_dir.join("codex.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind session socket");
        fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600))
            .expect("secure session socket");

        let descriptor_json = serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "wrapper_pid": self.wrapper_pid,
            "backend_pid": self.backend_pid,
            "socket_path": sock_path,
            "real_codex": self.backend_executable,
            "transport": "unix_websocket",
        }))
        .expect("serialize descriptor json");

        let mut desc_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(session_dir.join("descriptor.json"))
            .expect("create descriptor.json");
        desc_file
            .write_all(&descriptor_json)
            .expect("write descriptor.json");
        desc_file.sync_all().expect("sync descriptor.json");

        (listener, session_dir, sock_path)
    }

    fn retire_session(&self, session_dir: &Path) {
        let seq = self.staging_counter.fetch_add(1, Ordering::SeqCst);
        let retired_path = self.root.join(format!("retired-session-{seq}"));
        fs::rename(session_dir, retired_path).expect("atomically retire session dir");
    }
}

fn assert_redacted_diagnostic(text: &str, env: &WsTestEnv) {
    assert!(
        !text.contains("secret-sock-dir"),
        "diagnostic leaked socket directory name: {text}"
    );
    assert!(
        !text.contains("secret-descriptors-root"),
        "diagnostic leaked descriptor_root name: {text}"
    );
    assert!(
        !text.contains(env.root.to_string_lossy().as_ref()),
        "diagnostic leaked temp root path: {text}"
    );
    assert!(!text.contains("nce_"), "diagnostic leaked nonce: {text}");
    assert!(!text.contains("epc_"), "diagnostic leaked epoch: {text}");
    assert!(
        !text.contains("op_"),
        "diagnostic leaked operator token: {text}"
    );
}

#[tokio::test]
async fn managed_endpoint_can_be_pinned_to_the_mcp_parent_backend() {
    let env = WsTestEnv::new();
    let (listener, _) = env.publish_symlinked_session("parent-pin");
    let stream = env
        .reconnect_config()
        .connect_verified_stream_for_backend(Some(env.backend_pid))
        .await
        .expect("matching backend is accepted");
    let (peer, _) = listener.accept().await.expect("accept matching connection");
    drop(stream);
    drop(peer);
    let refused = env
        .reconnect_config()
        .connect_verified_stream_for_backend(Some(0))
        .await
        .expect_err("a different backend must be refused");
    assert!(matches!(refused, threadlane::Error::SecurityViolation(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn managed_ws_rejects_broad_status_and_unbounded_requests_before_connecting() {
    for params in [
        json!({"threadId": TEST_THREAD_A, "detail": "toolsAndAuthOnly"}),
        json!({"threadId": TEST_THREAD_A, "serverName": "other", "detail": "toolsAndAuthOnly"}),
        json!({"threadId": TEST_THREAD_A, "serverName": "threadlane"}),
        json!({"threadId": TEST_THREAD_A, "serverName": "threadlane", "detail": "toolsAndAuthOnly", "limit": 21}),
        json!({"threadId": TEST_THREAD_A, "serverName": "threadlane", "detail": "toolsAndAuthOnly", "cursor": "x".repeat(513)}),
    ] {
        assert!(validate_ws_readonly_request("mcpServerStatus/list", &params).is_err());
    }

    let env = WsTestEnv::new();
    let params = json!({"threadId": TEST_THREAD_A, "includeTurns": false});
    for (timeout, max_frame) in [
        (Duration::ZERO, 4096),
        (Duration::from_secs(121), 4096),
        (Duration::from_secs(1), 1023),
        (Duration::from_secs(1), 4 * 1024 * 1024 + 1),
    ] {
        let err = execute_ws_rpc::<Value>(
            &env.reconnect_config(),
            timeout,
            max_frame,
            "thread/read",
            params.clone(),
        )
        .await
        .expect_err("out-of-range transport settings must fail");
        assert_eq!(err.code, NativeErrorCode::InvalidConfig);
    }
}

#[tokio::test]
async fn managed_ws_executes_allowlisted_readonly_methods_correlates_ids_and_closes_cleanly() {
    let env = WsTestEnv::new();
    let (listener, _session_dir) = env.publish_symlinked_session("happy-path");

    let client = env
        .client()
        .with_cwd_filter(env.project_root.clone())
        .expect("set cwd filter");

    assert!(client.is_managed_websocket());
    assert_eq!(
        client.managed_websocket_config(),
        Some(&env.reconnect_config())
    );
    assert_eq!(
        client.transport(),
        &NativeTransport::ManagedWebSocket(env.reconnect_config())
    );

    let dbg = format!("{client:?}");
    assert_redacted_diagnostic(&dbg, &env);
    assert!(dbg.contains("[REDACTED]"));

    let (frames_tx, mut frames_rx) = mpsc::unbounded_channel::<(Vec<Value>, bool)>();
    let project_cwd_str = env.project_root.to_string_lossy().into_owned();

    let server_task = tokio::spawn(async move {
        for call_idx in 0..5usize {
            let (stream, _) = listener.accept().await.expect("accept unix stream");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept websocket handshake");

            let mut received = Vec::new();

            // 1. Read `initialize` (id = 1).
            let init_msg = ws
                .next()
                .await
                .expect("init frame")
                .expect("valid init frame");
            let Message::Text(init_text) = init_msg else {
                panic!("expected text frame for initialize");
            };
            let init_val: Value = serde_json::from_str(&init_text).expect("parse init json");
            received.push(init_val);

            // Emit an interleaved notification before the initialize response.
            ws.send(Message::Text(
                json!({
                    "method": "thread/status/changed",
                    "params": { "threadId": TEST_THREAD_A, "status": "idle" }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send pre-init notification");

            // Send `initialize` response (id = 1).
            ws.send(Message::Text(
                json!({
                    "id": 1u64,
                    "result": {
                        "userAgent": "codex-app-server/0.165.0",
                        "codexHome": "/tmp/codex-home",
                        "platformFamily": "unix",
                        "platformOs": "linux"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send init response");

            // 2. Read `initialized` notification.
            let initialized_msg = ws
                .next()
                .await
                .expect("initialized frame")
                .expect("valid initialized frame");
            let Message::Text(initialized_text) = initialized_msg else {
                panic!("expected text frame for initialized");
            };
            let initialized_val: Value =
                serde_json::from_str(&initialized_text).expect("parse initialized json");
            received.push(initialized_val);

            // 3. Read target request (id = 2).
            let req_msg = ws
                .next()
                .await
                .expect("request frame")
                .expect("valid request frame");
            let Message::Text(req_text) = req_msg else {
                panic!("expected text frame for request");
            };
            let req_val: Value = serde_json::from_str(&req_text).expect("parse request json");
            received.push(req_val);

            // Emit an interleaved notification and a Ping before the target response (id = 2).
            ws.send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .expect("send ping");
            ws.send(Message::Text(
                json!({
                    "method": "mcpServer/startupStatus/updated",
                    "params": { "name": "threadlane", "status": "ready" }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send pre-result notification");

            let resp_payload = match call_idx {
                0 => json!({
                    "id": 2u64,
                    "result": {
                        "data": [
                            {
                                "id": TEST_THREAD_A,
                                "sessionId": "shared-fork-session-999",
                                "projectId": "proj-alpha",
                                "name": "Alpha Conversation",
                                "cwd": project_cwd_str,
                                "status": { "type": "active", "activeFlags": ["waitingOnUserInput"] },
                                "model": "gpt-6-astra",
                                "reasoningEffort": "high"
                            },
                            {
                                "id": TEST_THREAD_B,
                                "sessionId": "shared-fork-session-999",
                                "projectId": "proj-beta",
                                "name": "Beta Conversation",
                                "cwd": project_cwd_str,
                                "status": { "type": "idle" },
                                "model": "gemini-4-argon-high",
                                "reasoningEffort": "high"
                            }
                        ],
                        "nextCursor": "cursor-next-2"
                    }
                }),
                1 => json!({
                    "id": 2u64,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_A,
                            "sessionId": "shared-fork-session-999",
                            "projectId": "proj-alpha",
                            "name": "Alpha Conversation",
                            "cwd": project_cwd_str,
                            "status": { "type": "idle" },
                            "canAcceptDirectInput": true,
                            "model": "gpt-6-astra",
                            "reasoningEffort": "high"
                        }
                    }
                }),
                2 => json!({
                    "id": 2u64,
                    "result": {
                        "data": [
                            {
                                "name": "threadlane",
                                "authStatus": "unsupported",
                                "runtimeStatus": "connected",
                                "tools": {
                                    "status": {},
                                    "connection_identity": {}
                                },
                                "toolsError": null
                            }
                        ]
                    }
                }),
                3 => {
                    let identity_data = json!({
                        "data": {
                            "connection_nonce": "nce_0123456789abcdef0123456789abcdef",
                            "epoch_id": "epc_abcdef0123456789abcdef0123456789",
                            "server_pid": 42424u32,
                            "state": "unbound",
                            "enrolled_binding_id": null,
                            "verification_gate": "needs_verification"
                        }
                    });
                    json!({
                        "id": 2u64,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": identity_data.to_string()
                                }
                            ],
                            "structuredContent": identity_data,
                            "isError": false
                        }
                    })
                }
                4 => json!({"id": 2u64, "result": {
                    "data": [TEST_THREAD_A, TEST_THREAD_B], "nextCursor": null
                }}),
                _ => unreachable!(),
            };

            ws.send(Message::Text(resp_payload.to_string().into()))
                .await
                .expect("send target response");

            // Verify client sends a clean Close frame after receiving response id = 2.
            let mut saw_close = false;
            while let Ok(Some(Ok(post_msg))) =
                tokio::time::timeout(Duration::from_millis(500), ws.next()).await
            {
                if matches!(post_msg, Message::Close(_)) {
                    saw_close = true;
                    break;
                }
            }
            frames_tx
                .send((received, saw_close))
                .expect("record call frames");
        }
    });

    // 1. `threads` (`thread/list`)
    let page = client
        .threads(
            Some("proj-alpha".to_string()),
            Some("cursor-page-1".to_string()),
        )
        .await
        .expect("threads over managed WebSocket");
    assert_eq!(page.next_cursor.as_deref(), Some("cursor-next-2"));
    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].id, TEST_THREAD_A);
    assert_eq!(page.threads[0].project_id.as_deref(), Some("proj-alpha"));
    assert_eq!(page.threads[0].status, "active");

    // 2. `thread` (`thread/read`)
    let thread_meta = client
        .thread(TEST_THREAD_A)
        .await
        .expect("thread over managed WebSocket");
    assert_eq!(thread_meta.id, TEST_THREAD_A);
    assert_eq!(thread_meta.status, "idle");
    assert_eq!(thread_meta.can_accept_direct_input, Some(true));

    // 3. `mcp_server_status` (`mcpServerStatus/list`)
    let statuses = client
        .mcp_server_status(TEST_THREAD_A, "threadlane")
        .await
        .expect("mcp_server_status over managed WebSocket");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].name, "threadlane");
    assert_eq!(
        statuses[0].tool_names,
        vec!["connection_identity".to_string(), "status".to_string()]
    );

    // 4. `probe_thread_connection_identity` (`mcpServer/tool/call` -> `connection_identity`)
    let identity = client
        .probe_thread_connection_identity(TEST_THREAD_A, "threadlane")
        .await
        .expect("probe_thread_connection_identity over managed WebSocket");
    assert_eq!(
        identity.connection_nonce,
        "nce_0123456789abcdef0123456789abcdef"
    );
    assert_eq!(identity.epoch_id, "epc_abcdef0123456789abcdef0123456789");
    assert_eq!(identity.server_pid, 42424);
    assert_eq!(identity.state, ConnectionEpochState::Unbound);
    assert_eq!(
        identity.verification_gate,
        NativeVerificationStatus::NeedsVerification
    );

    let loaded = client
        .loaded_thread_ids(None)
        .await
        .expect("loaded IDs over managed WebSocket");
    assert_eq!(loaded.thread_ids, vec![TEST_THREAD_A, TEST_THREAD_B]);
    assert!(loaded.next_cursor.is_none());

    server_task.await.expect("server task completed");

    let mut recorded = Vec::new();
    while let Ok(item) = frames_rx.try_recv() {
        recorded.push(item);
    }
    assert_eq!(recorded.len(), 5);

    for (frames, saw_close) in &recorded {
        assert!(
            *saw_close,
            "client must send a clean WebSocket Close frame after each RPC exchange"
        );
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0]["id"], 1);
        assert_eq!(frames[0]["method"], "initialize");
        assert_eq!(
            frames[0]["params"]["capabilities"]["experimentalApi"],
            false
        );
        assert_eq!(frames[1]["method"], "initialized");
        assert!(frames[1].get("id").is_none());
        assert_eq!(frames[2]["id"], 2);
    }

    // Verify wire request parameters for each allowlisted call.
    assert_eq!(recorded[0].0[2]["method"], "thread/list");
    assert!(recorded[0].0[2]["params"].get("projectId").is_none());
    assert_eq!(recorded[0].0[2]["params"]["useStateDbOnly"], true);
    assert_eq!(recorded[0].0[2]["params"]["cursor"], "cursor-page-1");

    assert_eq!(recorded[1].0[2]["method"], "thread/read");
    assert_eq!(recorded[1].0[2]["params"]["threadId"], TEST_THREAD_A);
    assert_eq!(recorded[1].0[2]["params"]["includeTurns"], false);

    assert_eq!(recorded[2].0[2]["method"], "mcpServerStatus/list");
    assert_eq!(recorded[2].0[2]["params"]["threadId"], TEST_THREAD_A);
    assert_eq!(recorded[2].0[2]["params"]["serverName"], "threadlane");
    assert_eq!(recorded[2].0[2]["params"]["detail"], "toolsAndAuthOnly");

    assert_eq!(recorded[3].0[2]["method"], "mcpServer/tool/call");
    assert_eq!(recorded[3].0[2]["params"]["threadId"], TEST_THREAD_A);
    assert_eq!(recorded[3].0[2]["params"]["server"], "threadlane");
    assert_eq!(recorded[3].0[2]["params"]["tool"], "connection_identity");
    assert_eq!(recorded[3].0[2]["params"]["arguments"], json!({}));
    assert!(recorded[3].0[2]["params"].get("_meta").is_none());
    assert_eq!(recorded[4].0[2]["method"], "thread/loaded/list");
    assert_eq!(recorded[4].0[2]["params"]["limit"], 100);
}

#[tokio::test]
async fn managed_ws_rejects_non_allowlisted_methods_tools_and_effects_before_any_frame_is_sent() {
    let env = WsTestEnv::new();
    let (listener, _session_dir) = env.publish_symlinked_session("allowlist-guard");
    let accept_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let accept_count_clone = Arc::clone(&accept_count);
    let stop_clone = Arc::clone(&stop);
    let watcher = tokio::spawn(async move {
        while !stop_clone.load(Ordering::SeqCst) {
            if let Ok(Ok((_stream, _))) =
                tokio::time::timeout(Duration::from_millis(20), listener.accept()).await
            {
                accept_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    let client = env.client();
    let reconnect = env.reconnect_config();

    // 1. Direct `validate_ws_readonly_request` & `execute_ws_rpc` checks for disallowed methods/params.
    let disallowed_rpc_cases: Vec<(&str, Value)> = vec![
        (
            "turn/start",
            json!({ "threadId": TEST_THREAD_A, "input": [] }),
        ),
        (
            "turn/steer",
            json!({ "threadId": TEST_THREAD_A, "expectedTurnId": "turn-1", "input": [] }),
        ),
        (
            "thread/resume",
            json!({ "threadId": TEST_THREAD_A, "excludeTurns": true }),
        ),
        ("thread/start", json!({ "cwd": "/tmp" })),
        (
            "thread/read",
            json!({ "threadId": TEST_THREAD_A, "includeTurns": true }),
        ),
        ("thread/read", json!({ "threadId": TEST_THREAD_A })),
        (
            "thread/read",
            json!({ "threadId": TEST_THREAD_A, "includeTurns": false, "extra": 1 }),
        ),
        (
            "thread/list",
            json!({ "projectId": "proj-experimental", "useStateDbOnly": true }),
        ),
        (
            "mcpServerStatus/list",
            json!({ "threadId": TEST_THREAD_A, "detail": "full" }),
        ),
        (
            "mcpServer/tool/call",
            json!({
                "threadId": TEST_THREAD_A,
                "server": "other_server",
                "tool": "connection_identity",
                "arguments": {}
            }),
        ),
        (
            "mcpServer/tool/call",
            json!({
                "threadId": TEST_THREAD_A,
                "server": "threadlane",
                "tool": "status",
                "arguments": {}
            }),
        ),
        (
            "mcpServer/tool/call",
            json!({
                "threadId": TEST_THREAD_A,
                "server": "threadlane",
                "tool": "enroll",
                "arguments": {}
            }),
        ),
        (
            "mcpServer/tool/call",
            json!({
                "threadId": TEST_THREAD_A,
                "server": "threadlane",
                "tool": "connection_identity",
                "arguments": { "binding_id": "bnd_1" }
            }),
        ),
        (
            "mcpServer/tool/call",
            json!({
                "threadId": TEST_THREAD_A,
                "server": "threadlane",
                "tool": "connection_identity",
                "arguments": {},
                "_meta": { "threadId": TEST_THREAD_A }
            }),
        ),
    ];

    for (method, params) in disallowed_rpc_cases {
        let val_err = validate_ws_readonly_request(method, &params)
            .expect_err("validate_ws_readonly_request must reject disallowed case");
        assert_eq!(
            val_err.code,
            NativeErrorCode::UnsupportedCapability,
            "unexpected error code for ({method}, {params}): {val_err:?}"
        );

        let rpc_err = execute_ws_rpc::<Value>(
            &reconnect,
            Duration::from_secs(2),
            64 * 1024,
            method,
            params.clone(),
        )
        .await
        .expect_err("execute_ws_rpc must reject disallowed case before connecting");
        assert_eq!(rpc_err.code, NativeErrorCode::UnsupportedCapability);
    }

    // 2. Generic `NativeClient::call_mcp_tool` and `probe_thread_connection_identity` with wrong server/tool/args.
    let wrong_server_err = client
        .call_mcp_tool(TEST_THREAD_A, "other", "connection_identity", json!({}))
        .await
        .expect_err("wrong server must be rejected");
    assert_eq!(
        wrong_server_err.code,
        NativeErrorCode::UnsupportedCapability
    );

    let probe_wrong_server_err = client
        .probe_thread_connection_identity(TEST_THREAD_A, "other")
        .await
        .expect_err("probe_thread_connection_identity with wrong server must be rejected");
    assert_eq!(
        probe_wrong_server_err.code,
        NativeErrorCode::UnsupportedCapability
    );

    let wrong_tool_err = client
        .call_mcp_tool(TEST_THREAD_A, "threadlane", "status", json!({}))
        .await
        .expect_err("status tool must be rejected on read-only managed WebSocket");
    assert_eq!(wrong_tool_err.code, NativeErrorCode::UnsupportedCapability);

    let non_empty_args_err = client
        .call_mcp_tool(
            TEST_THREAD_A,
            "threadlane",
            "connection_identity",
            json!({ "unexpected": true }),
        )
        .await
        .expect_err("non-empty arguments must be rejected");
    assert_eq!(
        non_empty_args_err.code,
        NativeErrorCode::UnsupportedCapability
    );

    // 3. `steer_active_turn` and `enroll_thread_connection` must fail closed before touching stores or socket.
    let wakeup_dir = env.root.join("wakeup-store");
    let wakeup_store = NativeWakeupStore::open(&wakeup_dir).expect("open wakeup store");
    let steer_err = client
        .steer_active_turn(&wakeup_store, "bnd_ws_1", TEST_THREAD_A, Some("turn-123"))
        .await
        .expect_err("steer_active_turn must be rejected on managed WebSocket");
    assert_eq!(steer_err.code, NativeErrorCode::UnsupportedCapability);
    assert!(
        wakeup_store
            .get("bnd_ws_1")
            .expect("query wakeup")
            .is_none(),
        "rejected steer on managed WebSocket must not write a pending wakeup record"
    );

    let pairing_dir = env.root.join("pairing-store");
    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let enroll_err = client
        .enroll_thread_connection(
            &pairing_store,
            &operator_token,
            TEST_THREAD_A,
            "bnd_ws_1",
            Some("proj-alpha"),
        )
        .await
        .expect_err("enroll_thread_connection must be rejected on managed WebSocket");
    assert_eq!(enroll_err.code, NativeErrorCode::UnsupportedCapability);

    stop.store(true, Ordering::SeqCst);
    let _ = watcher.await;
    assert_eq!(
        accept_count.load(Ordering::SeqCst),
        0,
        "zero Unix socket connections may be opened when allowlist rejects a request"
    );
}

#[tokio::test]
async fn managed_ws_rejects_bad_ids_malformed_frames_early_close_timeouts_and_overflows() {
    #[derive(Clone)]
    enum FaultScenario {
        MissingId,
        NullId,
        StringId,
        MismatchedId,
        ServerInitiatedRequest,
        NotificationWithResult,
        ResponseMissingResultAndError,
        ResponseBothResultAndError,
        MalformedJson,
        BinaryFrame,
        RpcErrorWithSecretInMessage,
        DisagreeingConnectionIdentity,
        DisagreeingIdentityTextBlocks,
        ToolIsErrorTrue,
        LoadedInvalidId,
        LoadedDuplicateId,
        EarlyCloseBeforeInit,
        EarlyCloseAfterInit,
        SingleFrameOverflow,
        AggregateNotificationOverflow,
        TooManySkippedNotifications,
        TimeoutStall,
    }

    let env = WsTestEnv::new();
    let (listener, _session_dir, _sock_path) = env.publish_direct_session("faults");

    let scenarios = vec![
        (FaultScenario::MissingId, NativeErrorCode::ProtocolError),
        (FaultScenario::NullId, NativeErrorCode::ProtocolError),
        (FaultScenario::StringId, NativeErrorCode::ProtocolError),
        (FaultScenario::MismatchedId, NativeErrorCode::ProtocolError),
        (
            FaultScenario::ServerInitiatedRequest,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::NotificationWithResult,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::ResponseMissingResultAndError,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::ResponseBothResultAndError,
            NativeErrorCode::ProtocolError,
        ),
        (FaultScenario::MalformedJson, NativeErrorCode::ProtocolError),
        (FaultScenario::BinaryFrame, NativeErrorCode::ProtocolError),
        (
            FaultScenario::RpcErrorWithSecretInMessage,
            NativeErrorCode::RpcError,
        ),
        (
            FaultScenario::DisagreeingConnectionIdentity,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::DisagreeingIdentityTextBlocks,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::ToolIsErrorTrue,
            NativeErrorCode::McpToolError,
        ),
        (
            FaultScenario::LoadedInvalidId,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::LoadedDuplicateId,
            NativeErrorCode::ProtocolError,
        ),
        (
            FaultScenario::EarlyCloseBeforeInit,
            NativeErrorCode::TransportUnavailable,
        ),
        (
            FaultScenario::EarlyCloseAfterInit,
            NativeErrorCode::TransportUnavailable,
        ),
        (
            FaultScenario::SingleFrameOverflow,
            NativeErrorCode::OutputOverflow,
        ),
        (
            FaultScenario::AggregateNotificationOverflow,
            NativeErrorCode::OutputOverflow,
        ),
        (
            FaultScenario::TooManySkippedNotifications,
            NativeErrorCode::ProtocolError,
        ),
        (FaultScenario::TimeoutStall, NativeErrorCode::TimedOut),
    ];

    let server_scenarios: Vec<FaultScenario> = scenarios.iter().map(|(s, _)| s.clone()).collect();
    let server_task = tokio::spawn(async move {
        for scenario in server_scenarios {
            let (stream, _) = listener.accept().await.expect("accept fault connection");
            let mut ws = tokio_tungstenite::accept_async(stream)
                .await
                .expect("accept ws handshake");

            // Wait for `initialize` request (id = 1).
            let _ = ws.next().await;

            if matches!(scenario, FaultScenario::EarlyCloseBeforeInit) {
                let _ = ws.send(Message::Close(None)).await;
                let _ = ws.close(None).await;
                continue;
            }
            if matches!(scenario, FaultScenario::TimeoutStall) {
                tokio::time::sleep(Duration::from_millis(400)).await;
                continue;
            }

            // Send valid `initialize` response (id = 1).
            ws.send(Message::Text(
                json!({
                    "id": 1u64,
                    "result": { "userAgent": "codex-app-server/0.165.0" }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send init response");

            // Read `initialized` and target request (id = 2).
            let _ = ws.next().await;
            let _ = ws.next().await;

            match scenario {
                FaultScenario::MissingId => {
                    let _ = ws
                        .send(Message::Text(json!({ "result": {} }).to_string().into()))
                        .await;
                }
                FaultScenario::NullId => {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": null, "result": {} }).to_string().into(),
                        ))
                        .await;
                }
                FaultScenario::StringId => {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": "2", "result": {} }).to_string().into(),
                        ))
                        .await;
                }
                FaultScenario::MismatchedId => {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 99u64, "result": {} }).to_string().into(),
                        ))
                        .await;
                }
                FaultScenario::ServerInitiatedRequest => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 50u64,
                                "method": "item/tool/call",
                                "params": {}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::NotificationWithResult => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "method": "thread/status/changed",
                                "result": {}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::ResponseMissingResultAndError => {
                    let _ = ws
                        .send(Message::Text(json!({ "id": 2u64 }).to_string().into()))
                        .await;
                }
                FaultScenario::ResponseBothResultAndError => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64,
                                "result": {},
                                "error": { "code": -32000, "message": "conflict" }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::MalformedJson => {
                    let _ = ws
                        .send(Message::Text("{malformed-json-payload".into()))
                        .await;
                }
                FaultScenario::BinaryFrame => {
                    let _ = ws
                        .send(Message::Binary(vec![0xff, 0x00, 0xfe].into()))
                        .await;
                }
                FaultScenario::RpcErrorWithSecretInMessage => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64,
                                "error": {
                                    "code": -32602,
                                    "message": "failed at /tmp/secret-sock-dir with nce_secret123 and op_secret456"
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::DisagreeingConnectionIdentity => {
                    let id_a = json!({
                        "data": {
                            "connection_nonce": "nce_aaaa",
                            "epoch_id": "epc_1111",
                            "server_pid": 100u32,
                            "state": "unbound",
                            "enrolled_binding_id": null,
                            "verification_gate": "needs_verification"
                        }
                    });
                    let id_b = json!({
                        "data": {
                            "connection_nonce": "nce_bbbb",
                            "epoch_id": "epc_1111",
                            "server_pid": 100u32,
                            "state": "unbound",
                            "enrolled_binding_id": null,
                            "verification_gate": "needs_verification"
                        }
                    });
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64,
                                "result": {
                                    "content": [{ "type": "text", "text": id_a.to_string() }],
                                    "structuredContent": id_b,
                                    "isError": false
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::DisagreeingIdentityTextBlocks => {
                    let identity = |nonce: &str| {
                        json!({"data": {
                            "connection_nonce": nonce, "epoch_id": "epc_1111",
                            "server_pid": 100u32, "state": "unbound",
                            "enrolled_binding_id": null, "verification_gate": "needs_verification"
                        }})
                        .to_string()
                    };
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64,
                                "result": {"content": [
                                    {"type": "text", "text": identity("nce_aaaa")},
                                    {"type": "text", "text": identity("nce_bbbb")}
                                ], "isError": false}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::ToolIsErrorTrue => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64,
                                "result": {
                                    "content": [],
                                    "structuredContent": {
                                        "error": {
                                            "message": "tool failed with nce_secret999"
                                        }
                                    },
                                    "isError": true
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::LoadedInvalidId | FaultScenario::LoadedDuplicateId => {
                    let data = if matches!(scenario, FaultScenario::LoadedInvalidId) {
                        vec!["not-a-uuid"]
                    } else {
                        vec![TEST_THREAD_A, TEST_THREAD_A]
                    };
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 2u64, "result": {"data": data, "nextCursor": null}
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                FaultScenario::EarlyCloseAfterInit => {
                    let _ = ws.send(Message::Close(None)).await;
                    let _ = ws.close(None).await;
                }
                FaultScenario::SingleFrameOverflow => {
                    let huge = "X".repeat(8192);
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 2u64, "result": { "pad": huge } })
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                FaultScenario::AggregateNotificationOverflow => {
                    // Client has max_frame_bytes = 2048; send 3 valid notifications of ~900 bytes each.
                    let chunk = "N".repeat(850);
                    for _ in 0..3 {
                        if ws
                            .send(Message::Text(
                                json!({
                                    "method": "thread/notification",
                                    "params": { "pad": chunk }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                FaultScenario::TooManySkippedNotifications => {
                    for _ in 0..=MAX_SKIPPED_NOTIFICATIONS {
                        if ws
                            .send(Message::Text(
                                json!({ "method": "thread/ping" }).to_string().into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                FaultScenario::EarlyCloseBeforeInit | FaultScenario::TimeoutStall => {
                    unreachable!()
                }
            }
        }
    });

    let client = env
        .client()
        .with_max_frame_bytes(2048)
        .expect("max_frame_bytes = 2048")
        .with_timeout(Duration::from_millis(150))
        .expect("timeout = 150ms");

    for (scenario, expected_code) in scenarios {
        // Give the count-limit case enough byte budget to reach the count limit first.
        let scenario_client = if matches!(&scenario, FaultScenario::TooManySkippedNotifications) {
            client
                .clone()
                .with_max_frame_bytes(8192)
                .expect("count-limit frame budget")
        } else {
            client.clone()
        };
        let err = match scenario {
            FaultScenario::LoadedInvalidId | FaultScenario::LoadedDuplicateId => scenario_client
                .loaded_thread_ids(None)
                .await
                .expect_err("invalid loaded-thread page must fail"),
            FaultScenario::DisagreeingConnectionIdentity
            | FaultScenario::DisagreeingIdentityTextBlocks
            | FaultScenario::ToolIsErrorTrue => scenario_client
                .probe_thread_connection_identity(TEST_THREAD_A, "threadlane")
                .await
                .expect_err("probe_thread_connection_identity fault must fail"),
            _ => scenario_client
                .thread(TEST_THREAD_A)
                .await
                .expect_err("thread read fault must fail"),
        };
        assert_eq!(
            err.code, expected_code,
            "unexpected error code for scenario; got {err:?}"
        );
        if matches!(scenario, FaultScenario::RpcErrorWithSecretInMessage) {
            assert_eq!(err.rpc_code, Some(-32602));
        }
        assert_redacted_diagnostic(&err.message, &env);
        assert_redacted_diagnostic(&format!("{err:?}"), &env);
    }

    server_task.await.expect("fault server finished");
}

#[tokio::test]
async fn managed_ws_refuses_mismatched_process_peer_and_non_private_endpoints() {
    let env = WsTestEnv::new();

    // 1. Empty descriptor_root (no live candidate) -> TransportUnavailable.
    let empty_err = env
        .client()
        .thread(TEST_THREAD_A)
        .await
        .expect_err("empty descriptor_root must fail with TransportUnavailable");
    assert_eq!(empty_err.code, NativeErrorCode::TransportUnavailable);
    assert_redacted_diagnostic(&empty_err.message, &env);

    // 2. Non-private descriptor_root (0755) is refused at construction time.
    fs::set_permissions(&env.descriptor_root, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 descriptor_root");
    let bad_root_err = NativeClient::new_managed_websocket(env.reconnect_config())
        .expect_err("0755 descriptor_root must be refused");
    assert_eq!(bad_root_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&bad_root_err.message, &env);
    fs::set_permissions(&env.descriptor_root, fs::Permissions::from_mode(0o700))
        .expect("restore 0700 descriptor_root");

    // 3. Publish a direct session and verify non-private session directory (0755),
    //    non-private descriptor.json (0644), and non-private socket (0666) are refused.
    let (listener, session_dir, sock_path) = env.publish_direct_session("perm-checks");
    let descriptor_path = session_dir.join("descriptor.json");
    let client = env.client();

    fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 session_dir");
    let dir_err = client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("0755 session_dir must be refused");
    assert_eq!(dir_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&dir_err.message, &env);
    fs::set_permissions(&session_dir, fs::Permissions::from_mode(0o700))
        .expect("restore 0700 session_dir");

    fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o644))
        .expect("chmod 0644 descriptor.json");
    let desc_err = client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("0644 descriptor.json must be refused");
    assert_eq!(desc_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&desc_err.message, &env);
    fs::set_permissions(&descriptor_path, fs::Permissions::from_mode(0o600))
        .expect("restore 0600 descriptor.json");

    fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o666)).expect("chmod 0666 socket");
    let sock_err = client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("0666 socket must be refused");
    assert_eq!(sock_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&sock_err.message, &env);
    fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600))
        .expect("restore 0600 socket");

    // 4. Mismatched `backend_executable` vs descriptor `real_codex` is refused.
    let other_exe = env.root.join("other-codex-bin");
    fs::write(&other_exe, b"#!/bin/sh\nexit 0\n").expect("write other_exe");
    fs::set_permissions(&other_exe, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    let mut wrong_backend_cfg = env.reconnect_config();
    wrong_backend_cfg.backend_executable = other_exe.clone();
    let wrong_backend_client =
        NativeClient::new_managed_websocket(wrong_backend_cfg).expect("valid static config");
    let mismatch_exe_err = wrong_backend_client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("mismatched backend_executable must be refused");
    // A descriptor for a different executable is not a matching endpoint.
    assert_eq!(mismatch_exe_err.code, NativeErrorCode::TransportUnavailable);
    assert_redacted_diagnostic(&mismatch_exe_err.message, &env);

    // 5. Multiple live matching candidates in descriptor_root are refused (ambiguous).
    let (listener_2, session_dir_2, _sock_2) = env.publish_direct_session("duplicate-live");
    let ambiguous_err = client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("multiple live candidates must be refused");
    assert_eq!(ambiguous_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&ambiguous_err.message, &env);

    drop(listener_2);
    env.retire_session(&session_dir_2);
    drop(listener);
    env.retire_session(&session_dir);

    // 6. Real kernel `SO_PEERCRED` PID mismatch:
    //    Spawn a real child process (`sleep 30`) whose parent is `self` (`wrapper_pid = self.backend_pid`)
    //    and whose grandparent is `self.wrapper_pid`. The `/proc` process chain
    //    `child -> self -> parent_of_self` is live and valid, but the UnixListener socket is bound by
    //    `self` (`peer_pid == self.backend_pid != child.id()`), so `validate_peer` must refuse!
    let sleep_exe = fs::canonicalize("/bin/sh").expect("canonical /bin/sh");
    let grandparent_exe =
        fs::canonicalize(format!("/proc/{}/exe", env.wrapper_pid)).expect("grandparent exe");
    let mut child = Command::new(&sleep_exe)
        .args(["-c", "sleep 30; true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleeping child process");
    let child_pid = child.id();

    let peer_session_dir = env.descriptor_root.join("tl-codex-peer-mismatch");
    DirBuilder::new()
        .mode(0o700)
        .create(&peer_session_dir)
        .expect("create peer_session_dir");
    let peer_sock_path = peer_session_dir.join("codex.sock");
    let _parent_bound_listener =
        UnixListener::bind(&peer_sock_path).expect("bind socket in parent process");
    fs::set_permissions(&peer_sock_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 peer_sock_path");

    let peer_desc_json = serde_json::to_vec_pretty(&json!({
        "schema_version": 1,
        "wrapper_pid": env.backend_pid,
        "backend_pid": child_pid,
        "socket_path": peer_sock_path,
        "real_codex": sleep_exe,
        "transport": "unix_websocket",
    }))
    .expect("serialize peer descriptor");
    let mut peer_desc_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(peer_session_dir.join("descriptor.json"))
        .expect("create peer descriptor.json");
    peer_desc_file
        .write_all(&peer_desc_json)
        .expect("write peer descriptor.json");
    peer_desc_file
        .sync_all()
        .expect("sync peer descriptor.json");
    drop(peer_desc_file);

    let peer_cfg = NativeReconnectConfig {
        descriptor_root: env.descriptor_root.clone(),
        backend_executable: sleep_exe,
        application_executable: grandparent_exe,
    };
    let peer_client =
        NativeClient::new_managed_websocket(peer_cfg).expect("create peer test client");

    let peer_err = peer_client
        .thread(TEST_THREAD_A)
        .await
        .expect_err("SO_PEERCRED PID mismatch between descriptor backend_pid and socket listener must be refused");

    let _ = child.kill();
    let _ = child.wait();

    assert_eq!(peer_err.code, NativeErrorCode::SecurityViolation);
    assert_redacted_diagnostic(&peer_err.message, &env);
}
