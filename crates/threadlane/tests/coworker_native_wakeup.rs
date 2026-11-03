#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::{
    format_coworker_job_wakeup_message, invoke_coworker_job_wakeup, CoworkerJobWakeupAttempt,
    CoworkerJobWakeupContext, NativeNotificationDelivery, NativeToolOutputConfig,
};
use threadlane_core::WakeupOutcome;
use tokio::net::UnixListener;
use tokio::sync::{watch, Mutex};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_THREAD_ID: &str = "01968f52-8871-7d51-9914-79d2bc48eb5a";
const TEST_IDLE_TURN_ID: &str = "01968f52-9999-7d51-9914-79d2bc48eb5b";
const TEST_ACTIVE_TURN_ID: &str = "01968f52-aaaa-7d51-9914-79d2bc48eb5c";
const STALE_PERSISTED_TURN_ID: &str = "01968f52-0001-7d51-9914-79d2bc48eb5d";
const TEST_JOB_ID: &str = "job_0123456789abcdef0123456789abcdef";
const OTHER_JOB_ID: &str = "job_fedcba9876543210fedcba9876543210";

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

struct CoworkerTestEnv {
    _temp: tempfile::TempDir,
    project_root: PathBuf,
    other_project_root: PathBuf,
    socket_path: PathBuf,
    queue_marker: PathBuf,
}

impl CoworkerTestEnv {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let project_root = root.join("repo");
        fs::create_dir(&project_root).expect("create repo");
        let other_project_root = root.join("other-repo");
        fs::create_dir(&other_project_root).expect("create other-repo");

        let sock_dir = root.join("sock");
        fs::create_dir(&sock_dir).expect("create sock_dir");
        fs::set_permissions(&sock_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
        let socket_path = sock_dir.join("codex.sock");
        let queue_marker = root.join("queue-ran.marker");

        Self {
            _temp: temp,
            project_root,
            other_project_root,
            socket_path,
            queue_marker,
        }
    }

    fn coworker_context(&self) -> CoworkerJobWakeupContext {
        CoworkerJobWakeupContext::from_target(
            TEST_JOB_ID,
            self.socket_path.clone(),
            TEST_THREAD_ID,
            self.project_root.clone(),
        )
        .expect("valid coworker context")
    }
}

async fn read_json<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = ws
        .next()
        .await
        .expect("expected ws frame")
        .expect("ws frame ok");
    match msg {
        Message::Text(text) => serde_json::from_str(&text).expect("valid json frame"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

#[tokio::test]
async fn coworker_idle_wakeup_sends_fixed_user_message_without_model_effort_or_fake_approval() {
    let env = CoworkerTestEnv::new();
    let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
    let expected_cwd = env.project_root.to_string_lossy().to_string();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut ws = accept_hdr_async(stream, validate_upgrade)
            .await
            .expect("ws handshake");

        let init = read_json(&mut ws).await;
        assert_eq!(init["id"], json!(1));
        assert_eq!(init["method"], json!("initialize"));
        assert_eq!(
            init["params"]["capabilities"]["experimentalApi"],
            json!(true)
        );
        ws.send(Message::Text(
            json!({ "id": 1, "result": { "serverInfo": { "name": "codex" } } })
                .to_string()
                .into(),
        ))
        .await
        .expect("send init resp");

        let initialized = read_json(&mut ws).await;
        assert_eq!(initialized, json!({ "method": "initialized" }));

        let read_req = read_json(&mut ws).await;
        assert_eq!(read_req["id"], json!(2));
        assert_eq!(read_req["method"], json!("thread/read"));
        assert_eq!(
            read_req["params"],
            json!({
                "threadId": TEST_THREAD_ID,
                "includeTurns": false,
            })
        );
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
        .expect("send read resp");

        let resume_req = read_json(&mut ws).await;
        assert_eq!(resume_req["id"], json!(3));
        assert_eq!(resume_req["method"], json!("thread/resume"));
        assert_eq!(
            resume_req["params"],
            json!({
                "threadId": TEST_THREAD_ID,
                "excludeTurns": true,
            })
        );
        assert!(resume_req["params"].get("model").is_none());
        assert!(resume_req["params"].get("effort").is_none());
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
        .expect("send resume resp");

        let turn_req = read_json(&mut ws).await;
        ws.send(Message::Text(
            json!({
                "id": 4,
                "result": {
                    "turn": {
                        "id": TEST_IDLE_TURN_ID,
                        "status": "inProgress"
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send turn/start resp");

        turn_req
    });

    let ctx = env.coworker_context();
    assert_eq!(ctx.delivery(), NativeNotificationDelivery::UserMessageSteer);
    assert!(!ctx.transport().has_legacy_model_metadata());

    let attempt = CoworkerJobWakeupAttempt::new(TEST_JOB_ID, "notif_job_status_001")
        .expect("valid attempt")
        .with_status_revision(2)
        .expect("valid revision")
        .with_transition("running:completed")
        .expect("valid transition");
    let (_tx, mut rx) = watch::channel(false);

    let outcome = invoke_coworker_job_wakeup(&ctx, &attempt, &mut rx).await;
    assert_eq!(outcome, WakeupOutcome::Submitted);

    let turn_req = server.await.expect("server join");
    let expected_notice = format_coworker_job_wakeup_message(TEST_JOB_ID).expect("expected notice");
    assert_eq!(turn_req["id"], json!(4));
    assert_eq!(turn_req["method"], json!("turn/start"));
    assert_eq!(
        turn_req["params"],
        json!({
            "threadId": TEST_THREAD_ID,
            "input": [
                {
                    "type": "text",
                    "text": expected_notice,
                }
            ]
        })
    );
    assert!(turn_req["params"].get("toolOutput").is_none());
    assert!(turn_req["params"].get("model").is_none());
    assert!(turn_req["params"].get("effort").is_none());

    let serialized_turn = turn_req.to_string();
    assert!(serialized_turn.contains(TEST_JOB_ID));
    assert!(serialized_turn.contains("execution_status"));
    assert!(serialized_turn.contains("untrusted"));
    assert!(!serialized_turn.contains("apr_"));
    assert!(!serialized_turn.contains("wcl_"));
    assert!(!serialized_turn.to_ascii_lowercase().contains("batch"));
    assert!(!serialized_turn.to_ascii_lowercase().contains("pull"));
    assert!(!serialized_turn.to_ascii_lowercase().contains("ack"));
    assert!(!env.queue_marker.exists());
}

#[tokio::test]
async fn coworker_busy_wakeup_uses_live_in_progress_expected_turn_id_ignores_old_turns_and_never_queues(
) {
    let env = CoworkerTestEnv::new();
    let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
    let expected_cwd = env.project_root.to_string_lossy().to_string();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut ws = accept_hdr_async(stream, validate_upgrade)
            .await
            .expect("ws handshake");

        let init = read_json(&mut ws).await;
        assert_eq!(
            init["params"]["capabilities"]["experimentalApi"],
            json!(true)
        );
        ws.send(Message::Text(
            json!({ "id": 1, "result": {} }).to_string().into(),
        ))
        .await
        .expect("send init resp");

        let _initialized = read_json(&mut ws).await;
        let _read_req = read_json(&mut ws).await;
        // Include a stale persisted completed turn in thread/read to verify it is never used as expectedTurnId.
        ws.send(Message::Text(
            json!({
                "id": 2,
                "result": {
                    "thread": {
                        "id": TEST_THREAD_ID,
                        "cwd": expected_cwd,
                        "status": { "type": "active" },
                        "turns": [
                            {
                                "id": STALE_PERSISTED_TURN_ID,
                                "status": "completed"
                            }
                        ]
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send read resp");

        let _resume_req = read_json(&mut ws).await;
        ws.send(Message::Text(
            json!({
                "id": 3,
                "result": {
                    "thread": {
                        "id": TEST_THREAD_ID,
                        "cwd": expected_cwd,
                        "status": { "type": "active" },
                        "turns": [
                            {
                                "id": STALE_PERSISTED_TURN_ID,
                                "status": "completed"
                            }
                        ]
                    }
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send resume resp");

        let list_req = read_json(&mut ws).await;
        assert_eq!(
            list_req,
            json!({
                "id": 4,
                "method": "thread/turns/list",
                "params": {
                    "threadId": TEST_THREAD_ID,
                    "limit": 1,
                    "sortDirection": "desc",
                    "itemsView": "summary",
                }
            })
        );
        ws.send(Message::Text(
            json!({
                "id": 4,
                "result": {
                    "data": [
                        {
                            "id": TEST_ACTIVE_TURN_ID,
                            "status": "inProgress"
                        }
                    ]
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send turns/list resp");

        let steer_req = read_json(&mut ws).await;
        ws.send(Message::Text(
            json!({
                "id": 5,
                "result": {
                    "turnId": TEST_ACTIVE_TURN_ID
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send turn/steer resp");

        steer_req
    });

    let ctx = env.coworker_context();
    let attempt = CoworkerJobWakeupAttempt::new(TEST_JOB_ID, "notif_busy_steer_001")
        .expect("valid attempt")
        .with_status_revision(5)
        .expect("valid revision");
    let (_tx, mut rx) = watch::channel(false);

    let outcome = invoke_coworker_job_wakeup(&ctx, &attempt, &mut rx).await;
    assert_eq!(outcome, WakeupOutcome::Submitted);

    let steer_req = server.await.expect("server join");
    let expected_notice = format_coworker_job_wakeup_message(TEST_JOB_ID).expect("expected notice");
    assert_eq!(
        steer_req,
        json!({
            "id": 5,
            "method": "turn/steer",
            "params": {
                "threadId": TEST_THREAD_ID,
                "expectedTurnId": TEST_ACTIVE_TURN_ID,
                "input": [
                    {
                        "type": "text",
                        "text": expected_notice,
                    }
                ]
            }
        })
    );
    assert!(steer_req["params"].get("model").is_none());
    assert!(steer_req["params"].get("effort").is_none());
    assert!(!env.queue_marker.exists());
}

#[tokio::test]
async fn coworker_busy_wakeup_refuses_completed_turn_without_sending_steer() {
    let env = CoworkerTestEnv::new();
    let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
    let expected_cwd = env.project_root.to_string_lossy().to_string();
    let saw_steer = Arc::new(Mutex::new(false));
    let saw_steer_clone = Arc::clone(&saw_steer);

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
                    "id": 4,
                    "result": {
                        "data": [
                            {
                                "id": STALE_PERSISTED_TURN_ID,
                                "status": "completed"
                            }
                        ]
                    }
                })
                .to_string()
                .into(),
            ))
            .await;

        if let Some(Ok(Message::Text(text))) = ws.next().await {
            let val: Value = serde_json::from_str(&text).expect("json");
            if val["method"] == "turn/steer" || val["method"] == "turn/start" {
                *saw_steer_clone.lock().await = true;
            }
        }
    });

    let ctx = env.coworker_context();
    let attempt =
        CoworkerJobWakeupAttempt::new(TEST_JOB_ID, "notif_busy_completed_turn").expect("attempt");
    let (_tx, mut rx) = watch::channel(false);

    let outcome = invoke_coworker_job_wakeup(&ctx, &attempt, &mut rx).await;
    assert!(
        matches!(outcome, WakeupOutcome::Failed { .. }),
        "expected Failed when active thread has only completed turn, got {outcome:?}"
    );
    server.await.expect("server join");
    assert!(!*saw_steer.lock().await);
}

#[tokio::test]
async fn coworker_rejects_wrong_thread_or_project_or_job_mismatch_before_sending_notice() {
    // 1. Legacy model/effort metadata on transport is rejected by CoworkerJobWakeupContext::new.
    {
        let env = CoworkerTestEnv::new();
        let _listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let mut legacy_transport = NativeToolOutputConfig::new_user_message_steer(
            env.socket_path.clone(),
            TEST_THREAD_ID.to_string(),
            env.project_root.clone(),
        );
        legacy_transport.model = "gpt-5-codex".to_string();
        assert!(CoworkerJobWakeupContext::new(TEST_JOB_ID, legacy_transport).is_err());
    }

    // 2. Job ID mismatch between context and attempt is rejected before connecting to the socket.
    {
        let env = CoworkerTestEnv::new();
        let _listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let ctx = env.coworker_context();
        let mismatched_attempt =
            CoworkerJobWakeupAttempt::new(OTHER_JOB_ID, "notif_mismatch").expect("valid attempt");
        let (_tx, mut rx) = watch::channel(false);
        let outcome = invoke_coworker_job_wakeup(&ctx, &mismatched_attempt, &mut rx).await;
        assert!(matches!(outcome, WakeupOutcome::Failed { .. }));
    }

    // 3. Wrong thread.id or wrong canonical project_root (cwd) on thread/read or thread/resume is rejected before send.
    let cases = vec![
        (
            "wrong_thread_id_on_read",
            "01968f52-0000-7d51-9914-79d2bc48eb5a",
            true,
            TEST_THREAD_ID,
            true,
        ),
        (
            "wrong_project_root_on_read",
            TEST_THREAD_ID,
            false,
            TEST_THREAD_ID,
            true,
        ),
        (
            "wrong_thread_id_on_resume",
            TEST_THREAD_ID,
            true,
            "01968f52-0000-7d51-9914-79d2bc48eb5a",
            true,
        ),
        (
            "wrong_project_root_on_resume",
            TEST_THREAD_ID,
            true,
            TEST_THREAD_ID,
            false,
        ),
    ];

    for (label, read_thread_id, read_matches_project, resume_thread_id, resume_matches_project) in
        cases
    {
        let env = CoworkerTestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let read_cwd = if read_matches_project {
            env.project_root.to_string_lossy().to_string()
        } else {
            env.other_project_root.to_string_lossy().to_string()
        };
        let resume_cwd = if resume_matches_project {
            env.project_root.to_string_lossy().to_string()
        } else {
            env.other_project_root.to_string_lossy().to_string()
        };

        let saw_effect = Arc::new(Mutex::new(false));
        let saw_effect_clone = Arc::clone(&saw_effect);

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
                                "id": read_thread_id,
                                "cwd": read_cwd,
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
                if val["method"] == "thread/resume" {
                    let _ = ws
                        .send(Message::Text(
                            json!({
                                "id": 3,
                                "result": {
                                    "thread": {
                                        "id": resume_thread_id,
                                        "cwd": resume_cwd,
                                        "status": { "type": "idle" }
                                    }
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                    if let Some(Ok(Message::Text(next_text))) = ws.next().await {
                        let next_val: Value = serde_json::from_str(&next_text).expect("json");
                        if next_val["method"] == "turn/start" || next_val["method"] == "turn/steer"
                        {
                            *saw_effect_clone.lock().await = true;
                        }
                    }
                } else if val["method"] == "turn/start" || val["method"] == "turn/steer" {
                    *saw_effect_clone.lock().await = true;
                }
            }
        });

        let ctx = env.coworker_context();
        let attempt =
            CoworkerJobWakeupAttempt::new(TEST_JOB_ID, "notif_verify_target").expect("attempt");
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_coworker_job_wakeup(&ctx, &attempt, &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Failed { .. }),
            "expected Failed for {label}, got {outcome:?}"
        );
        server.await.expect("server join");
        assert!(
            !*saw_effect.lock().await,
            "effect frame must never be written for {label}"
        );
    }
}

#[tokio::test]
async fn coworker_disconnect_after_write_returns_unknown_without_resending_notice() {
    for is_active in [false, true] {
        let env = CoworkerTestEnv::new();
        let listener = UnixListener::bind(&env.socket_path).expect("bind socket");
        let expected_cwd = env.project_root.to_string_lossy().to_string();
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let effect_writes = Arc::new(AtomicUsize::new(0));
        let accepted_clone = Arc::clone(&accepted_connections);
        let effect_clone = Arc::clone(&effect_writes);

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            accepted_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

            let _ = read_json(&mut ws).await;
            let _ = ws
                .send(Message::Text(
                    json!({ "id": 1, "result": {} }).to_string().into(),
                ))
                .await;
            let _ = read_json(&mut ws).await;
            let _ = read_json(&mut ws).await;
            let status_str = if is_active { "active" } else { "idle" };
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": 2,
                        "result": {
                            "thread": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "status": { "type": status_str }
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
                                "status": { "type": status_str }
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;

            if is_active {
                let list_req = read_json(&mut ws).await;
                assert_eq!(list_req["method"], json!("thread/turns/list"));
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": 4,
                            "result": {
                                "data": [
                                    {
                                        "id": TEST_ACTIVE_TURN_ID,
                                        "status": "inProgress"
                                    }
                                ]
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                let steer_req = read_json(&mut ws).await;
                assert_eq!(steer_req["method"], json!("turn/steer"));
                effect_clone.fetch_add(1, Ordering::SeqCst);
            } else {
                let turn_req = read_json(&mut ws).await;
                assert_eq!(turn_req["method"], json!("turn/start"));
                effect_clone.fetch_add(1, Ordering::SeqCst);
            }

            // Drop the WebSocket immediately after receiving the notification write without replying.
            drop(ws);
        });

        // Descriptor refresh is covered separately by the existing native_reconnect integration
        // suites; this test directly exercises real post-write ambiguity on the fixed explicitly
        // selected private socket.
        let ctx = env.coworker_context();

        let attempt = CoworkerJobWakeupAttempt::new(TEST_JOB_ID, "notif_disconnect_after_write")
            .expect("attempt");
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_coworker_job_wakeup(&ctx, &attempt, &mut rx).await;
        match outcome {
            WakeupOutcome::Unknown { ref reason } => {
                assert!(
                    !reason.contains(&env.project_root.to_string_lossy().to_string()),
                    "unknown reason must not leak filesystem paths: {reason}"
                );
            }
            other => panic!(
                "expected WakeupOutcome::Unknown after disconnect post-write (is_active={is_active}), got {other:?}"
            ),
        }

        server.await.expect("server join");
        assert_eq!(accepted_connections.load(Ordering::SeqCst), 1);
        assert_eq!(effect_writes.load(Ordering::SeqCst), 1);
    }
}
