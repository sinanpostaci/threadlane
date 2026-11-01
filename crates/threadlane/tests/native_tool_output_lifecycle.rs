#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::{
    ApprovedSessionWakeupContext, CodexWakeupConfig, NativeToolOutputConfig, ScopedClient,
    Supervisor, SupervisorClient, SupervisorConfig,
};
use threadlane_core::{Core, RunStatus, WakeupStatus};
use tokio::net::UnixListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

const THREAD: &str = "11111111-2222-4333-8444-555555555555";
const OTHER_THREAD: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const TURN: &str = "01900000-0000-7000-8000-000000000001";
const APPROVAL: &str = "apr_0123456789abcdef0123456789abcdef";

struct Peer {
    socket: PathBuf,
    starts: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    async fn start(root: &Path, disconnect_after_write: bool) -> Self {
        let socket = root.join("native.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        let observed = starts.clone();
        let cwd = root.to_string_lossy().into_owned();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                // A real RFC6455 handshake is required. A raw JSONL proxy cannot
                // satisfy this fixture.
                let mut ws = accept_async(stream).await.unwrap();
                while let Some(frame) = ws.next().await {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(_) => break,
                    };
                    let text = match frame {
                        Message::Text(text) => text,
                        Message::Close(_) => break,
                        Message::Ping(bytes) => {
                            let _ = ws.send(Message::Pong(bytes)).await;
                            continue;
                        }
                        _ => panic!("unexpected non-text request"),
                    };
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let method = request["method"].as_str().unwrap();
                    if method == "initialized" {
                        continue;
                    }
                    let result = match method {
                        "initialize" => json!({"userAgent": "owned-lifecycle-fixture"}),
                        "thread/read" => {
                            assert_eq!(request["params"]["threadId"], THREAD);
                            assert_eq!(request["params"]["includeTurns"], false);
                            json!({"thread": {"id": THREAD, "cwd": cwd, "status": {"type": "idle"}}})
                        }
                        "thread/resume" => {
                            let p = request["params"].as_object().unwrap();
                            assert_eq!(p.len(), 2, "resume must not change settings");
                            assert_eq!(p["threadId"], THREAD);
                            assert_eq!(p["excludeTurns"], true);
                            json!({"thread": {"id": THREAD, "cwd": cwd}, "cwd": cwd,
                                   "model": "gpt-6-luna", "reasoningEffort": "max"})
                        }
                        "turn/start" => {
                            let p = request["params"].as_object().unwrap();
                            assert_eq!(p.len(), 3, "no model or permission overrides");
                            assert_eq!(p["threadId"], THREAD);
                            assert_eq!(p["input"], json!([]));
                            assert_eq!(p["toolOutput"]["name"], "threadlane_events_available");
                            assert_eq!(p["toolOutput"]["namespace"], "threadlane");
                            let body = p["toolOutput"]["output"].as_str().unwrap();
                            let notice: Value = serde_json::from_str(body).unwrap();
                            assert_eq!(notice["untrusted_data"], true);
                            assert_eq!(notice["approval_id"], APPROVAL);
                            assert!(notice["claim_id"].as_str().unwrap().starts_with("wcl_"));
                            assert!(!body.contains("PRIVATE_TASK_TEXT"));
                            assert!(!body.contains("PRIVATE_ANSWER"));
                            observed.fetch_add(1, Ordering::SeqCst);
                            if disconnect_after_write {
                                let _ = ws.close(None).await;
                                break;
                            }
                            json!({"turn": {"id": TURN, "status": "inProgress", "error": null, "items": []}})
                        }
                        _ => panic!("unexpected effect or RPC: {method}"),
                    };
                    if ws
                        .send(Message::Text(
                            json!({"id": request["id"], "result": result})
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
        });
        Self {
            socket,
            starts,
            task,
        }
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

fn root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    root
}

fn config(root: &Path, socket: &Path, binding_id: &str) -> CodexWakeupConfig {
    let marker = root.join("queue-executed");
    let executable = root.join("must-never-run.sh");
    fs::write(
        &executable,
        format!("#!/bin/sh\nprintf bad > '{}'\nexit 29\n", marker.display()),
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let native = NativeToolOutputConfig {
        schema_version: 1,
        socket_path: socket.to_path_buf(),
        delivery: Default::default(),
        reconnect: None,
        thread_id: THREAD.to_string(),
        project_root: root.to_path_buf(),
        model: "gpt-6-luna".to_string(),
        effort: "max".to_string(),
    };
    CodexWakeupConfig {
        executable,
        notify_bindings: BTreeSet::from([binding_id.to_string()]),
        timeout: Duration::from_secs(2),
        approved_session: Some(
            ApprovedSessionWakeupContext::new(APPROVAL)
                .unwrap()
                .with_operator_cli_route()
                .with_tool_output_transport(native)
                .unwrap(),
        ),
    }
}

fn bindings(state: &Path) -> (String, String) {
    let db = threadlane::descriptor::prepare_database_path(state).unwrap();
    let mut core = Core::open(&db).unwrap();
    let a = core.create_binding(THREAD, "session").unwrap();
    let b = core.create_binding(OTHER_THREAD, "foreign").unwrap();
    threadlane::descriptor::enforce_database_permissions(state).unwrap();
    (a.binding_id, b.binding_id)
}

async fn status(client: &ScopedClient, expected: WakeupStatus) -> threadlane::StatusResponse {
    for _ in 0..150 {
        let value = client.status().await.unwrap();
        if value.wakeup.status == expected {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected wakeup status {expected:?}");
}

async fn run_status(client: &ScopedClient, run: &str, expected: RunStatus) -> threadlane_core::Run {
    for _ in 0..150 {
        let value = client.get_run(run.to_string()).await.unwrap();
        if value.status == expected {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("expected run status {expected:?}");
}

#[tokio::test]
async fn native_notice_coalesces_and_only_actual_batch_ack_rearms_it() {
    let temp = root();
    let root = temp.path().canonicalize().unwrap();
    let peer = Peer::start(&root, false).await;
    let state = root.join("state");
    let (binding, foreign) = bindings(&state);
    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(config(&root, &peer.socket, &binding)),
        workers: None,
    })
    .await
    .unwrap();
    let client = SupervisorClient::from_state_dir(&state).unwrap();
    let scoped = client.clone().scoped(&binding).unwrap();
    let other = client.scoped(&foreign).unwrap();
    let run = scoped
        .start_demo("start".into(), "PRIVATE_TASK_TEXT".into(), Some(15))
        .await
        .unwrap();
    other
        .start_demo("foreign-start".into(), "foreign".into(), Some(15))
        .await
        .unwrap();
    let waiting = run_status(&scoped, &run.run_id, RunStatus::WaitingInput).await;
    assert_eq!(
        status(&scoped, WakeupStatus::Submitted)
            .await
            .wakeup
            .attempt_count,
        1
    );
    assert_eq!(other.status().await.unwrap().wakeup.attempt_count, 0);
    scoped
        .answer(
            "answer".into(),
            run.run_id.clone(),
            waiting.active_question_id.unwrap(),
            1,
            "PRIVATE_ANSWER".into(),
        )
        .await
        .unwrap();
    run_status(&scoped, &run.run_id, RunStatus::Completed).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(peer.starts.load(Ordering::SeqCst), 1);
    let first = scoped.pull_events(None, Some(3)).await.unwrap();
    assert_eq!(first.to_seq, 3);
    assert!(other
        .ack_events("foreign-ack".into(), first.batch_id.clone())
        .await
        .is_err());
    scoped
        .ack_events("ack-first".into(), first.batch_id)
        .await
        .unwrap();
    for _ in 0..150 {
        if scoped.status().await.unwrap().wakeup.attempt_count == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        status(&scoped, WakeupStatus::Submitted)
            .await
            .wakeup
            .attempt_count,
        2
    );
    assert_eq!(peer.starts.load(Ordering::SeqCst), 2);
    let rest = scoped.pull_events(None, Some(50)).await.unwrap();
    scoped
        .ack_events("ack-rest".into(), rest.batch_id)
        .await
        .unwrap();
    status(&scoped, WakeupStatus::Idle).await;
    assert!(!root.join("queue-executed").exists());
    handle.shutdown().await;
    peer.stop().await;
}

#[tokio::test]
async fn unknown_native_write_survives_supervisor_restart_without_replay() {
    let temp = root();
    let root = temp.path().canonicalize().unwrap();
    let peer = Peer::start(&root, true).await;
    let state = root.join("state");
    let (binding, _) = bindings(&state);
    let notification = config(&root, &peer.socket, &binding);
    let supervisor_config = || SupervisorConfig {
        state_dir: state.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(notification.clone()),
        workers: None,
    };
    let handle = Supervisor::start(supervisor_config()).await.unwrap();
    let scoped = SupervisorClient::from_state_dir(&state)
        .unwrap()
        .scoped(&binding)
        .unwrap();
    scoped
        .start_demo("start".into(), "PRIVATE_TASK_TEXT".into(), Some(15))
        .await
        .unwrap();
    assert_eq!(
        status(&scoped, WakeupStatus::Unknown)
            .await
            .wakeup
            .attempt_count,
        1
    );
    handle.shutdown().await;
    let restarted = Supervisor::start(supervisor_config()).await.unwrap();
    let scoped = SupervisorClient::from_state_dir(&state)
        .unwrap()
        .scoped(&binding)
        .unwrap();
    assert_eq!(
        status(&scoped, WakeupStatus::Unknown)
            .await
            .wakeup
            .attempt_count,
        1
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(peer.starts.load(Ordering::SeqCst), 1);
    assert!(!root.join("queue-executed").exists());
    restarted.shutdown().await;
    peer.stop().await;
}
