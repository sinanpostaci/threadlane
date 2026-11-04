#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::native_tool_output::NativeNotificationDelivery;
use threadlane::{
    format_coworker_planning_wakeup_message, invoke_codex_wakeup, invoke_coworker_planning_wakeup,
    ApprovedSessionWakeupContext, CodexWakeupConfig, CoworkerPlanningWakeupAttempt,
    CoworkerPlanningWakeupContext, NativeToolOutputConfig,
};
use threadlane_core::{WakeupClaim, WakeupOutcome};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_THREAD_ID: &str = "01968f52-8871-7d51-9914-79d2bc48eb5a";
const TEST_TURN_ID: &str = "01968f52-9999-7d51-9914-79d2bc48eb5b";
const TEST_APPROVAL_ID: &str = "apr_0123456789abcdef0123456789abcdef";
const TEST_BINDING_ID: &str = "bnd_test_native";

// The callback result is fixed by tungstenite's public handshake trait.
#[allow(clippy::result_large_err)]
fn validate_upgrade(
    req: &Request,
    resp: Response,
) -> Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
    let upgrade = req
        .headers()
        .get("Upgrade")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(upgrade.eq_ignore_ascii_case("websocket"));
    let host = req
        .headers()
        .get("Host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(host.starts_with("localhost"));
    Ok(resp)
}

struct TestEnv {
    _temp: tempfile::TempDir,
    project_root: PathBuf,
    sock_dir: PathBuf,
    socket_path: PathBuf,
    cfg_dir: PathBuf,
    queue_executable: PathBuf,
    queue_marker: PathBuf,
}

impl TestEnv {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let project_root = root.join("repo");
        fs::create_dir(&project_root).expect("create repo");

        let sock_dir = root.join("sock");
        fs::create_dir(&sock_dir).expect("create sock_dir");
        fs::set_permissions(&sock_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let socket_path = sock_dir.join("codex.sock");

        let cfg_dir = root.join("cfg");
        fs::create_dir(&cfg_dir).expect("create cfg_dir");
        fs::set_permissions(&cfg_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");

        let queue_marker = root.join("queue-ran.marker");
        let queue_executable = root.join("fake-codex-queue.sh");
        let script = format!(
            "#!/bin/sh\necho ran > \"{}\"\nexit 0\n",
            queue_marker.display()
        );
        fs::write(&queue_executable, script).expect("write fake queue executable");
        fs::set_permissions(&queue_executable, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 queue executable");

        Self {
            _temp: temp,
            project_root,
            sock_dir,
            socket_path,
            cfg_dir,
            queue_executable,
            queue_marker,
        }
    }

    fn base_native_config(&self) -> NativeToolOutputConfig {
        NativeToolOutputConfig {
            schema_version: 1,
            socket_path: self.socket_path.clone(),
            thread_id: TEST_THREAD_ID.to_string(),
            project_root: self.project_root.clone(),
            model: "gpt-5-codex".to_string(),
            effort: "high".to_string(),
            delivery: NativeNotificationDelivery::ToolOutput,
            reconnect: None,
        }
    }

    fn coworker_native_config(&self) -> NativeToolOutputConfig {
        NativeToolOutputConfig::new(
            self.socket_path.clone(),
            TEST_THREAD_ID.to_string(),
            self.project_root.clone(),
        )
    }

    fn sample_claim(&self) -> WakeupClaim {
        WakeupClaim {
            binding_id: TEST_BINDING_ID.to_string(),
            thread_uuid: TEST_THREAD_ID.to_string(),
            claim_id: "wcl_0123456789abcdef0123456789abcdef".to_string(),
            pending_since_seq: 3,
            covered_up_to_seq: 9,
            coalesced_count: 4,
            message: "UNTRUSTED_CLAIM_MESSAGE_NOT_FOR_TRANSPORT".to_string(),
            claimed_at_ms: 1_000,
        }
    }

    fn wakeup_config(
        &self,
        ctx: ApprovedSessionWakeupContext,
        timeout_ms: u64,
    ) -> CodexWakeupConfig {
        let mut notify_bindings = BTreeSet::new();
        notify_bindings.insert(TEST_BINDING_ID.to_string());
        CodexWakeupConfig {
            executable: self.queue_executable.clone(),
            notify_bindings,
            timeout: Duration::from_millis(timeout_ms),
            approved_session: Some(ctx),
        }
    }
}

#[derive(Clone)]
enum TurnBehavior {
    DisconnectWithoutReply,
    SleepTimeout,
    MalformedJson,
    BinaryFrame,
    MismatchedId,
    RpcError,
    TurnError,
    InvalidTurnId(String),
    OversizeFrame,
    TriggerShutdown(watch::Sender<bool>),
}

#[tokio::test]
async fn config_load_and_validate_enforce_permissions_symlinks_and_socket_rules() {
    let env = TestEnv::new();
    let _listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
    let cfg = env.base_native_config();
    cfg.validate().expect("valid base config");

    // Write valid private JSON and load it
    let cfg_path = env.cfg_dir.join("native.json");
    fs::write(&cfg_path, serde_json::to_vec_pretty(&cfg).expect("json")).expect("write cfg");
    fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    let loaded = NativeToolOutputConfig::load(&cfg_path).expect("load valid config");
    assert_eq!(loaded, cfg);

    // Reject wrong config file permissions (0644)
    fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    assert!(NativeToolOutputConfig::load(&cfg_path).is_err());
    fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).expect("restore 0600");

    // Reject wrong config parent permissions (0755)
    fs::set_permissions(&env.cfg_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    assert!(NativeToolOutputConfig::load(&cfg_path).is_err());
    fs::set_permissions(&env.cfg_dir, fs::Permissions::from_mode(0o700)).expect("restore 0700");

    // Reject symlink config file
    let symlink_cfg = env.cfg_dir.join("symlink.json");
    symlink(&cfg_path, &symlink_cfg).expect("symlink cfg");
    assert!(NativeToolOutputConfig::load(&symlink_cfg).is_err());

    // Reject oversized config (> 16 KiB)
    let huge_path = env.cfg_dir.join("huge.json");
    fs::write(&huge_path, vec![b' '; 16_385]).expect("write huge");
    fs::set_permissions(&huge_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    assert!(NativeToolOutputConfig::load(&huge_path).is_err());

    // Reject wrong socket parent permissions (0755)
    fs::set_permissions(&env.sock_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    assert!(cfg.validate().is_err());
    fs::set_permissions(&env.sock_dir, fs::Permissions::from_mode(0o700)).expect("restore 0700");

    // Reject non-socket regular file as socket_path
    let regular_file = env.sock_dir.join("not-a-socket");
    fs::write(&regular_file, b"hello").expect("write regular file");
    let mut bad_sock_cfg = cfg.clone();
    bad_sock_cfg.socket_path = regular_file;
    assert!(bad_sock_cfg.validate().is_err());

    // Reject symlink socket
    let sym_sock = env.sock_dir.join("sym.sock");
    symlink(&env.socket_path, &sym_sock).expect("symlink sock");
    let mut sym_sock_cfg = cfg.clone();
    sym_sock_cfg.socket_path = sym_sock;
    assert!(sym_sock_cfg.validate().is_err());
}

#[tokio::test]
async fn native_exchange_submits_exact_sequence_for_idle_and_active_without_running_queue() {
    for (status_json, use_operator_cli) in [
        (json!({ "type": "idle" }), false),
        (
            json!({ "type": "active", "activeFlags": ["turnRunning"] }),
            true,
        ),
    ] {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env.base_native_config();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);
        let expected_cwd = env.project_root.to_string_lossy().to_string();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept unix stream");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws handshake");

            // 1. initialize
            let msg1 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg1.clone());
            assert_eq!(msg1["method"], "initialize");
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send init resp");

            // 2. initialized notification
            let msg2 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg2.clone());
            assert_eq!(msg2["method"], "initialized");
            assert!(msg2.get("id").is_none());

            // 3. thread/read
            let msg3 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg3.clone());
            assert_eq!(msg3["method"], "thread/read");
            assert_eq!(
                msg3["params"],
                json!({ "threadId": TEST_THREAD_ID, "includeTurns": false })
            );
            // Send a Ping and an untrusted notification before replying to verify safe handling
            ws.send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .expect("send ping");
            ws.send(Message::Text(
                json!({ "method": "thread/updated", "params": { "untrusted": "ignored" } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send notification");
            ws.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": status_json,
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send read resp");

            // 4. thread/resume
            let msg4 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg4.clone());
            assert_eq!(msg4["method"], "thread/resume");
            assert_eq!(
                msg4["params"],
                json!({ "threadId": TEST_THREAD_ID, "excludeTurns": true })
            );
            ws.send(Message::Text(
                json!({
                    "id": 3,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": status_json,
                        },
                        "model": "gpt-5-codex",
                        "reasoningEffort": "high",
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send resume resp");

            // 5. turn/start
            let msg5 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg5.clone());
            assert_eq!(msg5["method"], "turn/start");
            ws.send(Message::Text(
                json!({
                    "id": 4,
                    "result": {
                        "turn": {
                            "id": TEST_TURN_ID,
                            "status": { "type": "inProgress" },
                            "error": null
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("send turn/start resp");
        });

        let mut base_ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID).expect("ctx");
        if use_operator_cli {
            base_ctx = base_ctx.with_operator_cli_route();
        }
        let expected_notice = base_ctx.wakeup_message();
        let ctx = base_ctx
            .with_tool_output_transport(cfg)
            .expect("attach tool_output");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        assert!(
            !env.queue_marker.exists(),
            "queue executable must never run"
        );

        server.await.expect("server task");
        let calls = recorded.lock().await;
        assert_eq!(calls.len(), 5);
        let turn_params = &calls[4]["params"];
        assert_eq!(turn_params["threadId"], TEST_THREAD_ID);
        assert_eq!(turn_params["input"], json!([]));
        assert!(turn_params.get("model").is_none());
        assert!(turn_params.get("effort").is_none());
        assert_eq!(
            turn_params["toolOutput"]["name"],
            "threadlane_events_available"
        );
        assert_eq!(turn_params["toolOutput"]["namespace"], "threadlane");
        let parsed_output: Value = serde_json::from_str(
            turn_params["toolOutput"]["output"]
                .as_str()
                .expect("output string"),
        )
        .expect("valid output json");
        assert_eq!(
            parsed_output,
            json!({
                "untrusted_data": true,
                "approval_id": TEST_APPROVAL_ID,
                "claim_id": claim.claim_id,
                "pending_since_seq": claim.pending_since_seq,
                "covered_up_to_seq": claim.covered_up_to_seq,
                "coalesced_count": claim.coalesced_count,
                "notice": expected_notice,
            })
        );
    }
}

#[tokio::test]
async fn scope_status_model_and_effort_mismatches_fail_before_any_turn_start_write() {
    let cases = [
        (
            "notLoaded status",
            json!({ "id": TEST_THREAD_ID, "status": { "type": "notLoaded" } }),
            json!({ "id": TEST_THREAD_ID, "model": "gpt-5-codex", "reasoningEffort": "high" }),
        ),
        (
            "thread.id mismatch in read",
            json!({ "id": "01968f52-0000-7d51-9914-79d2bc48eb5a", "status": { "type": "idle" } }),
            json!({ "id": TEST_THREAD_ID, "model": "gpt-5-codex", "reasoningEffort": "high" }),
        ),
        (
            "cwd mismatch in read",
            json!({ "id": TEST_THREAD_ID, "cwd": "/tmp/wrong-cwd", "status": { "type": "idle" } }),
            json!({ "id": TEST_THREAD_ID, "model": "gpt-5-codex", "reasoningEffort": "high" }),
        ),
        (
            "thread.id mismatch in resume despite model switch",
            json!({ "id": TEST_THREAD_ID, "status": { "type": "idle" } }),
            json!({ "id": "01968f52-0000-7d51-9914-79d2bc48eb5a", "model": "o4-mini", "reasoningEffort": "low" }),
        ),
        (
            "cwd mismatch in resume despite model switch",
            json!({ "id": TEST_THREAD_ID, "status": { "type": "idle" } }),
            json!({ "id": TEST_THREAD_ID, "cwd": "/tmp/wrong-cwd", "model": "o4-mini", "reasoningEffort": "low" }),
        ),
        (
            "outer and inner model disagreement in resume",
            json!({ "id": TEST_THREAD_ID, "status": { "type": "idle" } }),
            json!({
                "id": TEST_THREAD_ID,
                "model": "gpt-5-codex",
                "reasoningEffort": "high",
                "thread": { "id": TEST_THREAD_ID, "model": "o4-mini" }
            }),
        ),
        (
            "non-string model field in read",
            json!({ "id": TEST_THREAD_ID, "model": 42, "status": { "type": "idle" } }),
            json!({ "id": TEST_THREAD_ID, "model": "gpt-5-codex", "reasoningEffort": "high" }),
        ),
    ];

    for (label, mut read_thread, mut resume_obj) in cases {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let cfg = env.base_native_config();
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        if read_thread.get("cwd").is_none() {
            read_thread["cwd"] = Value::String(expected_cwd.clone());
        }
        if resume_obj.get("cwd").is_none() {
            resume_obj["cwd"] = Value::String(expected_cwd.clone());
        }

        let saw_turn_start = Arc::new(Mutex::new(false));
        let saw_turn_start_clone = Arc::clone(&saw_turn_start);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

            let _init = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _initialized = read_json(&mut ws).await;
            let _read = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 2, "result": { "thread": read_thread } })
                        .to_string()
                        .into(),
                ))
                .await;

            if let Some(Ok(Message::Text(text))) = ws.next().await {
                let val: Value = serde_json::from_str(&text).expect("json");
                if val["method"] == "thread/resume" {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 3, "result": resume_obj }).to_string().into(),
                        ))
                        .await;
                    if let Some(Ok(Message::Text(next_text))) = ws.next().await {
                        let next_val: Value = serde_json::from_str(&next_text).expect("json");
                        if next_val["method"] == "turn/start" {
                            *saw_turn_start_clone.lock().await = true;
                        }
                    }
                } else if val["method"] == "turn/start" {
                    *saw_turn_start_clone.lock().await = true;
                }
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Failed { .. }),
            "expected Failed for case {label}, got {outcome:?}"
        );
        server.await.expect("server");
        assert!(
            !*saw_turn_start.lock().await,
            "turn/start must not be sent for case {label}"
        );
        assert!(!env.queue_marker.exists());
    }
}

#[tokio::test]
async fn pre_write_failures_foreign_binding_claim_mismatch_and_initial_shutdown_return_failed() {
    let env = TestEnv::new();
    let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
    let cfg = env.base_native_config();
    let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
        .expect("ctx")
        .with_tool_output_transport(cfg)
        .expect("transport");
    let wakeup_cfg = env.wakeup_config(ctx, 5_000);

    // 1. Foreign binding rejected before any socket connect
    let mut bad_binding_claim = env.sample_claim();
    bad_binding_claim.binding_id = "bnd_foreign".to_string();
    let (_tx, mut rx) = watch::channel(false);
    assert!(matches!(
        invoke_codex_wakeup(&wakeup_cfg, &bad_binding_claim, &mut rx).await,
        WakeupOutcome::Failed { .. }
    ));

    // 2. Claim thread_uuid mismatch rejected before any socket connect
    let mut bad_thread_claim = env.sample_claim();
    bad_thread_claim.thread_uuid = "01968f52-0000-7d51-9914-79d2bc48eb5a".to_string();
    assert!(matches!(
        invoke_codex_wakeup(&wakeup_cfg, &bad_thread_claim, &mut rx).await,
        WakeupOutcome::Failed { .. }
    ));

    // 3. Initial shutdown == true rejected before any socket connect
    let (_tx_shut, mut rx_shut) = watch::channel(true);
    assert!(matches!(
        invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx_shut).await,
        WakeupOutcome::Failed { .. }
    ));

    // 4. Missing socket at invocation time returns Failed and never runs queue executable
    drop(listener);
    fs::remove_file(&env.socket_path).expect("remove socket");
    assert!(matches!(
        invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await,
        WakeupOutcome::Failed { .. }
    ));
    assert!(!env.queue_marker.exists());
}

#[tokio::test]
async fn post_turn_start_write_failures_and_shutdown_return_unknown_with_redacted_reason() {
    let (shut_tx, _) = watch::channel(false);
    let behaviors = vec![
        TurnBehavior::DisconnectWithoutReply,
        TurnBehavior::SleepTimeout,
        TurnBehavior::MalformedJson,
        TurnBehavior::BinaryFrame,
        TurnBehavior::MismatchedId,
        TurnBehavior::RpcError,
        TurnBehavior::TurnError,
        TurnBehavior::InvalidTurnId("00000000-0000-0000-0000-000000000000".to_string()),
        TurnBehavior::InvalidTurnId("not-a-uuid".to_string()),
        TurnBehavior::OversizeFrame,
        TurnBehavior::TriggerShutdown(shut_tx),
    ];

    for behavior in behaviors {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let cfg = env.base_native_config();
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let (turn_received_tx, mut turn_received_rx) = mpsc::channel::<()>(1);

        let (tx, mut rx) = match &behavior {
            TurnBehavior::TriggerShutdown(sender) => (sender.clone(), sender.subscribe()),
            _ => watch::channel(false),
        };
        let _ = tx;

        let behavior_clone = behavior.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "idle" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 3,
                        "result": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "model": "gpt-5-codex",
                            "reasoningEffort": "high"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;

            let turn_req = read_json(&mut ws).await;
            assert_eq!(turn_req["method"], "turn/start");
            let _ = turn_received_tx.send(()).await;

            match behavior_clone {
                TurnBehavior::DisconnectWithoutReply => {
                    drop(ws);
                }
                TurnBehavior::SleepTimeout => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                TurnBehavior::MalformedJson => {
                    let _ = ws.send(Message::Text("{not-json".into())).await;
                }
                TurnBehavior::BinaryFrame => {
                    let _ = ws.send(Message::Binary(vec![0xde, 0xad].into())).await;
                }
                TurnBehavior::MismatchedId => {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 99, "result": { "turn": { "id": TEST_TURN_ID } } })
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                TurnBehavior::RpcError => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 4,
                                "error": { "code": -32000, "message": "secret-rpc-error-detail" }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                TurnBehavior::TurnError => {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 4,
                                "result": {
                                    "turn": {
                                        "id": TEST_TURN_ID,
                                        "error": { "message": "secret-turn-error" }
                                    }
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                }
                TurnBehavior::InvalidTurnId(bad_id) => {
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 4, "result": { "turn": { "id": bad_id } } })
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                TurnBehavior::OversizeFrame => {
                    let huge = "x".repeat(1_048_600);
                    let _ = ws.send(Message::Text(huge.into())).await;
                }
                TurnBehavior::TriggerShutdown(sender) => {
                    let _ = sender.send(true);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let timeout_ms = match behavior {
            TurnBehavior::SleepTimeout => 120,
            _ => 5_000,
        };
        let wakeup_cfg = env.wakeup_config(ctx, timeout_ms);
        let claim = env.sample_claim();

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        turn_received_rx
            .recv()
            .await
            .expect("server received turn/start");
        match outcome {
            WakeupOutcome::Unknown { reason } => {
                assert!(!reason.contains("secret-rpc-error-detail"));
                assert!(!reason.contains("secret-turn-error"));
                assert!(!reason.contains("gpt-5-codex"));
                assert!(!reason.contains(TEST_THREAD_ID));
            }
            other => {
                panic!("expected WakeupOutcome::Unknown after turn/start write, got {other:?}")
            }
        }
        assert!(!env.queue_marker.exists());
        let _ = server.await;
    }
}

async fn read_json(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>) -> Value {
    loop {
        let msg = ws
            .next()
            .await
            .expect("frame present")
            .expect("valid frame");
        match msg {
            Message::Text(t) => return serde_json::from_str(&t).expect("valid json"),
            Message::Pong(_) | Message::Ping(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

#[tokio::test]
async fn user_message_steer_mode_routes_idle_to_turn_start_and_active_to_turn_steer() {
    // 1. Idle thread -> turn/start with fixed user text notice and no toolOutput or overrides.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env
            .base_native_config()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");

            let msg1 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg1);
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("init");

            let msg2 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg2);

            let msg3 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg3);
            ws.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "idle" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("read");

            let msg4 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg4);
            ws.send(Message::Text(
                json!({
                    "id": 3,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "idle" }
                        },
                        "model": "gpt-5-codex",
                        "reasoningEffort": "high"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resume");

            let msg5 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg5);
            ws.send(Message::Text(
                json!({
                    "id": 4,
                    "result": {
                        "turn": {
                            "id": TEST_TURN_ID,
                            "status": { "type": "inProgress" },
                            "error": null
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("turn/start");
        });

        let base_ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_operator_cli_route();
        let expected_notice = base_ctx.wakeup_message();
        let ctx = base_ctx
            .with_tool_output_transport(cfg)
            .expect("attach steer transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        assert!(!env.queue_marker.exists());

        server.await.expect("server");
        let calls = recorded.lock().await;
        assert_eq!(calls.len(), 5);
        assert_eq!(
            calls[0]["params"]["capabilities"],
            json!({ "experimentalApi": true })
        );
        assert_eq!(calls[4]["method"], "turn/start");
        let params = &calls[4]["params"];
        assert_eq!(params["threadId"], TEST_THREAD_ID);
        assert_eq!(
            params["input"],
            json!([{ "type": "text", "text": expected_notice }])
        );
        assert!(params.get("toolOutput").is_none());
        assert!(params.get("model").is_none());
        assert!(params.get("effort").is_none());
    }

    // 2. Active thread -> thread/turns/list (summary, limit 1, desc) + turn/steer with exact expectedTurnId.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env
            .base_native_config()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");

            let msg1 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg1);
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("init");

            let msg2 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg2);

            let msg3 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg3);
            ws.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "active", "activeFlags": ["turnRunning"] }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("read");

            let msg4 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg4);
            ws.send(Message::Text(
                json!({
                    "id": 3,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "active" }
                        },
                        "model": "gpt-5-codex",
                        "reasoningEffort": "high"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resume");

            let msg5 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg5);
            ws.send(Message::Text(
                json!({
                    "id": 4,
                    "result": {
                        "data": [
                            {
                                "id": TEST_TURN_ID,
                                "status": "inProgress",
                                "items": []
                            }
                        ],
                        "nextCursor": null
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("turns/list");

            let msg6 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg6);
            ws.send(Message::Text(
                json!({
                    "id": 5,
                    "result": {
                        "turnId": TEST_TURN_ID
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("turn/steer");
        });

        let base_ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID).expect("ctx");
        let expected_notice = base_ctx.wakeup_message();
        let ctx = base_ctx
            .with_tool_output_transport(cfg)
            .expect("attach steer transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        assert!(!env.queue_marker.exists());

        server.await.expect("server");
        let calls = recorded.lock().await;
        assert_eq!(calls.len(), 6);
        assert_eq!(calls[4]["method"], "thread/turns/list");
        assert_eq!(
            calls[4]["params"],
            json!({
                "threadId": TEST_THREAD_ID,
                "limit": 1,
                "sortDirection": "desc",
                "itemsView": "summary"
            })
        );
        assert_eq!(calls[5]["method"], "turn/steer");
        assert_eq!(
            calls[5]["params"],
            json!({
                "threadId": TEST_THREAD_ID,
                "expectedTurnId": TEST_TURN_ID,
                "input": [{ "type": "text", "text": expected_notice }]
            })
        );
    }
}

#[tokio::test]
async fn user_message_steer_refuses_missing_or_completed_active_turn_and_marks_mismatched_steer_ack_unknown(
) {
    // 1. Pre-effect refusal when active thread has no inProgress turn summary -> Failed, no turn/start or turn/steer fallback.
    for list_response in [
        json!({ "id": 4, "result": { "data": [], "nextCursor": null } }),
        json!({
            "id": 4,
            "result": {
                "data": [{ "id": TEST_TURN_ID, "status": "completed", "items": [] }],
                "nextCursor": null
            }
        }),
        json!({ "id": 4, "error": { "code": -32601, "message": "method not found" } }),
    ] {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env
            .base_native_config()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let saw_effect_write = Arc::new(Mutex::new(false));
        let saw_effect_write_clone = Arc::clone(&saw_effect_write);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "active" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 3,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "active" }
                            },
                            "model": "gpt-5-codex",
                            "reasoningEffort": "high"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;

            let list_req = read_json(&mut ws).await;
            assert_eq!(list_req["method"], "thread/turns/list");
            let _ = ws
                .send(Message::Text(list_response.to_string().into()))
                .await;

            if let Some(Ok(Message::Text(next_text))) = ws.next().await {
                let next_val: Value = serde_json::from_str(&next_text).expect("json");
                if matches!(
                    next_val["method"].as_str(),
                    Some("turn/start" | "turn/steer")
                ) {
                    *saw_effect_write_clone.lock().await = true;
                }
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Failed { .. }),
            "expected Failed before effect write, got {outcome:?}"
        );
        server.await.expect("server");
        assert!(!*saw_effect_write.lock().await);
        assert!(!env.queue_marker.exists());
    }

    // 2. Post-write mismatched turnId or RPC error on turn/steer -> Unknown and no retry.
    for steer_reply in [
        json!({
            "id": 5,
            "result": { "turnId": "01968f52-1111-7d51-9914-79d2bc48eb5c" }
        }),
        json!({
            "id": 5,
            "error": { "code": -32000, "message": "secret-active-turn-finished" }
        }),
    ] {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env
            .base_native_config()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let expected_cwd = env.project_root.to_string_lossy().to_string();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "active" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 3,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "active" }
                            },
                            "model": "gpt-5-codex",
                            "reasoningEffort": "high"
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 4,
                        "result": {
                            "data": [{ "id": TEST_TURN_ID, "status": { "type": "inProgress" } }],
                            "nextCursor": null
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let steer_req = read_json(&mut ws).await;
            assert_eq!(steer_req["method"], "turn/steer");
            let _ = ws.send(Message::Text(steer_reply.to_string().into())).await;
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        match outcome {
            WakeupOutcome::Unknown { reason } => {
                assert!(!reason.contains("secret-active-turn-finished"));
                assert!(!reason.contains(TEST_TURN_ID));
            }
            other => panic!("expected Unknown after turn/steer write, got {other:?}"),
        }
        server.await.expect("server");
        assert!(!env.queue_marker.exists());
    }
}

#[tokio::test]
async fn coworker_model_and_effort_switches_succeed_in_idle_and_busy_delivery_for_same_chat() {
    let cases = [
        (
            "idle tool_output with legacy context and switched live model",
            true,
            NativeNotificationDelivery::ToolOutput,
            false,
            Some("o4-mini"),
            Some("low"),
        ),
        (
            "busy tool_output with model-less coworker context and switched live model",
            false,
            NativeNotificationDelivery::ToolOutput,
            true,
            Some("gpt-5.4-codex"),
            Some("xhigh"),
        ),
        (
            "idle user_message_steer with legacy context and switched live model",
            true,
            NativeNotificationDelivery::UserMessageSteer,
            false,
            Some("o4-mini"),
            Some("medium"),
        ),
        (
            "busy user_message_steer with model-less coworker context and switched live model",
            false,
            NativeNotificationDelivery::UserMessageSteer,
            true,
            Some("gpt-5.4-codex"),
            Some("low"),
        ),
        (
            "busy user_message_steer with omitted live model metadata",
            false,
            NativeNotificationDelivery::UserMessageSteer,
            true,
            None,
            None,
        ),
    ];

    for (label, use_legacy_cfg, delivery, is_busy, live_model, live_effort) in cases {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let base_cfg = if use_legacy_cfg {
            env.base_native_config()
        } else {
            env.coworker_native_config()
        };
        let cfg = base_cfg.with_delivery(delivery);
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);
        let status_json = if is_busy {
            json!({ "type": "active", "activeFlags": ["turnRunning"] })
        } else {
            json!({ "type": "idle" })
        };

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");

            let msg1 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg1);
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("init");

            let msg2 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg2);

            let msg3 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg3);
            let mut read_thread = json!({
                "id": TEST_THREAD_ID,
                "cwd": expected_cwd,
                "status": status_json,
            });
            if let Some(m) = live_model {
                read_thread["model"] = json!(m);
            }
            if let Some(e) = live_effort {
                read_thread["reasoningEffort"] = json!(e);
            }
            ws.send(Message::Text(
                json!({ "id": 2, "result": { "thread": read_thread } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("read");

            let msg4 = read_json(&mut ws).await;
            recorded_clone.lock().await.push(msg4);
            let mut resume_result = json!({
                "thread": {
                    "id": TEST_THREAD_ID,
                    "cwd": expected_cwd,
                    "status": status_json,
                }
            });
            if let Some(m) = live_model {
                resume_result["model"] = json!(m);
                resume_result["thread"]["model"] = json!(m);
            }
            if let Some(e) = live_effort {
                resume_result["reasoningEffort"] = json!(e);
                resume_result["thread"]["reasoningEffort"] = json!(e);
            }
            ws.send(Message::Text(
                json!({ "id": 3, "result": resume_result })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("resume");

            match (delivery, is_busy) {
                (NativeNotificationDelivery::UserMessageSteer, true) => {
                    let msg5 = read_json(&mut ws).await;
                    recorded_clone.lock().await.push(msg5);
                    ws.send(Message::Text(
                        json!({
                            "id": 4,
                            "result": {
                                "data": [{ "id": TEST_TURN_ID, "status": "inProgress", "items": [] }],
                                "nextCursor": null
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("turns/list");

                    let msg6 = read_json(&mut ws).await;
                    recorded_clone.lock().await.push(msg6);
                    ws.send(Message::Text(
                        json!({ "id": 5, "result": { "turnId": TEST_TURN_ID } })
                            .to_string()
                            .into(),
                    ))
                    .await
                    .expect("turn/steer");
                }
                _ => {
                    let msg5 = read_json(&mut ws).await;
                    recorded_clone.lock().await.push(msg5);
                    ws.send(Message::Text(
                        json!({
                            "id": 4,
                            "result": {
                                "turn": {
                                    "id": TEST_TURN_ID,
                                    "status": { "type": "inProgress" },
                                    "error": null
                                }
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("turn/start");
                }
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(
            outcome,
            WakeupOutcome::Submitted,
            "expected Submitted for case {label}"
        );
        assert!(!env.queue_marker.exists());

        server.await.expect("server");
        let calls = recorded.lock().await;
        // Verify thread/resume never sends model or effort overrides.
        assert_eq!(calls[3]["method"], "thread/resume");
        assert!(calls[3]["params"].get("model").is_none());
        assert!(calls[3]["params"].get("effort").is_none());
        assert!(calls[3]["params"].get("reasoningEffort").is_none());

        let final_call = calls.last().expect("final call");
        assert!(final_call["params"].get("model").is_none());
        assert!(final_call["params"].get("effort").is_none());
        assert!(final_call["params"].get("reasoningEffort").is_none());
        if delivery == NativeNotificationDelivery::UserMessageSteer && is_busy {
            assert_eq!(final_call["method"], "turn/steer");
            assert_eq!(final_call["params"]["expectedTurnId"], TEST_TURN_ID);
        } else {
            assert_eq!(final_call["method"], "turn/start");
        }
    }
}

#[tokio::test]
async fn wrong_thread_project_or_active_turn_fails_before_effect_independent_of_model() {
    for (label, wrong_thread, wrong_cwd, completed_turn, live_model) in [
        (
            "wrong thread with switched model",
            true,
            false,
            false,
            Some("o4-mini"),
        ),
        ("wrong thread with omitted model", true, false, false, None),
        (
            "wrong project cwd with switched model",
            false,
            true,
            false,
            Some("gpt-5.4-codex"),
        ),
        (
            "wrong project cwd with omitted model",
            false,
            true,
            false,
            None,
        ),
        (
            "completed active turn with switched model",
            false,
            false,
            true,
            Some("o4-mini"),
        ),
        (
            "completed active turn with omitted model",
            false,
            false,
            true,
            None,
        ),
    ] {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env
            .coworker_native_config()
            .with_delivery(NativeNotificationDelivery::UserMessageSteer);
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let saw_effect_write = Arc::new(Mutex::new(false));
        let saw_effect_write_clone = Arc::clone(&saw_effect_write);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;

            let thread_id = if wrong_thread {
                "01968f52-0000-7d51-9914-79d2bc48eb5a"
            } else {
                TEST_THREAD_ID
            };
            let cwd = if wrong_cwd {
                "/tmp/wrong-project-root"
            } else {
                expected_cwd.as_str()
            };
            let mut read_thread = json!({
                "id": thread_id,
                "cwd": cwd,
                "status": { "type": "active" }
            });
            if let Some(m) = live_model {
                read_thread["model"] = json!(m);
            }
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 2, "result": { "thread": read_thread } })
                        .to_string()
                        .into(),
                ))
                .await;

            if let Some(Ok(Message::Text(text))) = ws.next().await {
                let val: Value = serde_json::from_str(&text).expect("json");
                if val["method"] == "thread/resume" {
                    let mut resume_res = json!({
                        "thread": {
                            "id": thread_id,
                            "cwd": cwd,
                            "status": { "type": "active" }
                        }
                    });
                    if let Some(m) = live_model {
                        resume_res["model"] = json!(m);
                    }
                    let _ = ws
                        .send(Message::Text(
                            json!({ "id": 3, "result": resume_res }).to_string().into(),
                        ))
                        .await;
                    if let Some(Ok(Message::Text(next_text))) = ws.next().await {
                        let next_val: Value = serde_json::from_str(&next_text).expect("json");
                        if next_val["method"] == "thread/turns/list" && completed_turn {
                            let _ = ws
                                .send(Message::Text(
                                    json!({
                                        "id": 4,
                                        "result": {
                                            "data": [{ "id": TEST_TURN_ID, "status": "completed", "items": [] }],
                                            "nextCursor": null
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await;
                            if let Some(Ok(Message::Text(after_list))) = ws.next().await {
                                let after_val: Value =
                                    serde_json::from_str(&after_list).expect("json");
                                if matches!(
                                    after_val["method"].as_str(),
                                    Some("turn/start" | "turn/steer")
                                ) {
                                    *saw_effect_write_clone.lock().await = true;
                                }
                            }
                        } else if matches!(
                            next_val["method"].as_str(),
                            Some("turn/start" | "turn/steer")
                        ) {
                            *saw_effect_write_clone.lock().await = true;
                        }
                    }
                } else if matches!(val["method"].as_str(), Some("turn/start" | "turn/steer")) {
                    *saw_effect_write_clone.lock().await = true;
                }
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Failed { .. }),
            "expected Failed for case {label}, got {outcome:?}"
        );
        server.await.expect("server");
        assert!(
            !*saw_effect_write.lock().await,
            "no effect write permitted for case {label}"
        );
        assert!(!env.queue_marker.exists());
    }
}

#[tokio::test]
async fn coworker_planning_wakeup_supports_idle_start_active_steer_disconnect_unknown_and_scope_rejection(
) {
    const TEST_COMMAND_ID: &str = "pcm_0123456789abcdef0123456789abcdef";
    const TEST_NOTIF_ID: &str = "ntf_0123456789abcdef0123456789abcdef";
    let expected_notice =
        format_coworker_planning_wakeup_message(TEST_COMMAND_ID).expect("expected notice");

    // 1. Idle thread -> turn/start with fixed planning notice and omitted model/effort/toolOutput.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env.coworker_native_config();
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("init");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "idle" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("read");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 3,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "idle" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resume");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 4,
                    "result": {
                        "turn": {
                            "id": TEST_TURN_ID,
                            "status": { "type": "inProgress" },
                            "error": null
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("turn/start");
        });

        let ctx = CoworkerPlanningWakeupContext::new(TEST_COMMAND_ID, cfg).expect("planning ctx");
        let attempt = CoworkerPlanningWakeupAttempt::new(TEST_COMMAND_ID, TEST_NOTIF_ID)
            .expect("planning attempt");
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_coworker_planning_wakeup(&ctx, &attempt, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        assert!(!env.queue_marker.exists());
        server.await.expect("server");

        let calls = recorded.lock().await;
        assert_eq!(calls.len(), 5);
        assert_eq!(calls[4]["method"], "turn/start");
        let params = &calls[4]["params"];
        assert_eq!(params["threadId"], TEST_THREAD_ID);
        assert_eq!(
            params["input"],
            json!([{ "type": "text", "text": expected_notice }])
        );
        assert!(params.get("toolOutput").is_none());
        assert!(params.get("model").is_none());
        assert!(params.get("effort").is_none());
    }

    // 2. Active thread -> thread/turns/list + turn/steer with live expectedTurnId.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env.coworker_native_config();
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let recorded = Arc::new(Mutex::new(Vec::<Value>::new()));
        let recorded_clone = Arc::clone(&recorded);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("init");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "active" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("read");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 3,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "active" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("resume");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({
                    "id": 4,
                    "result": {
                        "data": [{ "id": TEST_TURN_ID, "status": "inProgress", "items": [] }],
                        "nextCursor": null
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("turns/list");
            recorded_clone.lock().await.push(read_json(&mut ws).await);
            ws.send(Message::Text(
                json!({ "id": 5, "result": { "turnId": TEST_TURN_ID } })
                    .to_string()
                    .into(),
            ))
            .await
            .expect("turn/steer");
        });

        let ctx = CoworkerPlanningWakeupContext::new(TEST_COMMAND_ID, cfg).expect("planning ctx");
        let attempt = CoworkerPlanningWakeupAttempt::new(TEST_COMMAND_ID, TEST_NOTIF_ID)
            .expect("planning attempt");
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_coworker_planning_wakeup(&ctx, &attempt, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        server.await.expect("server");

        let calls = recorded.lock().await;
        assert_eq!(calls.len(), 6);
        assert_eq!(calls[5]["method"], "turn/steer");
        let params = &calls[5]["params"];
        assert_eq!(params["threadId"], TEST_THREAD_ID);
        assert_eq!(params["expectedTurnId"], TEST_TURN_ID);
        assert_eq!(
            params["input"],
            json!([{ "type": "text", "text": expected_notice }])
        );
        assert!(params.get("model").is_none());
        assert!(params.get("effort").is_none());
    }

    // 3. Disconnect after effect write -> WakeupOutcome::Unknown.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env.coworker_native_config();
        let expected_cwd = env.project_root.to_string_lossy().to_string();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "idle" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 3,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": "idle" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            let msg5 = read_json(&mut ws).await;
            assert_eq!(msg5["method"], "turn/start");
            let _ = ws.close(None).await;
        });

        let ctx = CoworkerPlanningWakeupContext::new(TEST_COMMAND_ID, cfg).expect("planning ctx");
        let attempt = CoworkerPlanningWakeupAttempt::new(TEST_COMMAND_ID, TEST_NOTIF_ID)
            .expect("planning attempt");
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_coworker_planning_wakeup(&ctx, &attempt, &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Unknown { .. }),
            "expected Unknown on disconnect after turn/start write, got {outcome:?}"
        );
        server.await.expect("server");
    }

    // 4. Mismatched command_id or cross-thread/project rejection -> WakeupOutcome::Failed before effect write.
    {
        let env = TestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind unix socket");
        let cfg = env.coworker_native_config();
        let ctx = CoworkerPlanningWakeupContext::new(TEST_COMMAND_ID, cfg).expect("planning ctx");
        let mismatched_attempt = CoworkerPlanningWakeupAttempt::new(
            "pcm_ffffffffffffffffffffffffffffffff",
            TEST_NOTIF_ID,
        )
        .expect("attempt");
        let (_tx, mut rx) = watch::channel(false);
        let outcome = invoke_coworker_planning_wakeup(&ctx, &mismatched_attempt, &mut rx).await;
        assert!(matches!(outcome, WakeupOutcome::Failed { .. }));

        let saw_effect_write = Arc::new(Mutex::new(false));
        let saw_effect_write_clone = Arc::clone(&saw_effect_write);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut ws = accept_hdr_async(stream, validate_upgrade)
                .await
                .expect("ws");
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": "01968f52-0000-7d51-9914-79d2bc48eb5a",
                                "cwd": "/tmp/wrong-project-root",
                                "status": { "type": "idle" }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
            if let Some(Ok(Message::Text(text))) = ws.next().await {
                let val: Value = serde_json::from_str(&text).expect("json");
                if matches!(val["method"].as_str(), Some("turn/start" | "turn/steer")) {
                    *saw_effect_write_clone.lock().await = true;
                }
            }
        });

        let attempt = CoworkerPlanningWakeupAttempt::new(TEST_COMMAND_ID, TEST_NOTIF_ID)
            .expect("planning attempt");
        let outcome = invoke_coworker_planning_wakeup(&ctx, &attempt, &mut rx).await;
        assert!(matches!(outcome, WakeupOutcome::Failed { .. }));
        server.await.expect("server");
        assert!(!*saw_effect_write.lock().await);
    }
}
