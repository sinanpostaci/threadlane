#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::native_tool_output::NativeNotificationDelivery;
use threadlane::{
    invoke_codex_wakeup, ApprovedSessionWakeupContext, CodexWakeupConfig, NativeToolOutputConfig,
};
use threadlane_core::{WakeupClaim, WakeupOutcome};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};

const THREAD: &str = "01968f52-8871-7d51-9914-79d2bc48eb5a";
const APPROVAL: &str = "apr_0123456789abcdef0123456789abcdef";
const BINDING: &str = "bnd_status_edges_fixture";
const MODEL: &str = "gpt-6-luna";
const EFFORT: &str = "max";

#[derive(Clone, Copy, Debug)]
enum ResumeStatus {
    Missing,
    Idle,
}

async fn read_json(ws: &mut WebSocketStream<UnixStream>) -> Value {
    let message = ws.next().await.expect("request").expect("valid frame");
    serde_json::from_str(message.to_text().expect("text frame")).expect("JSON request")
}

async fn reply(ws: &mut WebSocketStream<UnixStream>, id: &Value, result: Value) {
    ws.send(Message::Text(
        json!({"id": id, "result": result}).to_string().into(),
    ))
    .await
    .expect("reply");
}

async fn assert_no_effect(ws: &mut WebSocketStream<UnixStream>) {
    let next = tokio::time::timeout(Duration::from_secs(1), ws.next())
        .await
        .expect("client must close after refusal");
    if let Some(Ok(Message::Text(text))) = next {
        panic!("unexpected post-refusal request: {text}");
    }
}

async fn run_case(status: ResumeStatus) {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = fs::canonicalize(temp.path()).expect("canonical root");
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("private root");
    let socket_path = root.join("control.sock");
    let listener = UnixListener::bind(&socket_path).expect("listener");
    let project_root = root.join("repository");
    fs::create_dir(&project_root).expect("repository");
    let queue_marker = root.join("queue-used");
    let executable = root.join("fake-codex");
    fs::write(
        &executable,
        format!("#!/bin/sh\ntouch '{}'\n", queue_marker.display()),
    )
    .expect("fake executable");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).expect("executable mode");
    let native = NativeToolOutputConfig {
        schema_version: 1,
        socket_path,
        thread_id: THREAD.to_owned(),
        project_root: project_root.clone(),
        model: MODEL.to_owned(),
        effort: EFFORT.to_owned(),
        delivery: NativeNotificationDelivery::UserMessageSteer,
        reconnect: None,
    };
    let context = ApprovedSessionWakeupContext::new(APPROVAL)
        .expect("context")
        .with_operator_cli_route()
        .with_tool_output_transport(native)
        .expect("native context");
    let config = CodexWakeupConfig {
        executable,
        notify_bindings: BTreeSet::from([BINDING.to_owned()]),
        timeout: Duration::from_secs(5),
        approved_session: Some(context),
    };
    let claim = WakeupClaim {
        binding_id: BINDING.to_owned(),
        thread_uuid: THREAD.to_owned(),
        claim_id: "wcl_0123456789abcdef0123456789abcdef".to_owned(),
        pending_since_seq: 1,
        covered_up_to_seq: 3,
        coalesced_count: 2,
        message: "UNTRUSTED WORKER PROSE MUST NEVER BE TRANSMITTED".to_owned(),
        claimed_at_ms: 1000,
    };
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut ws = accept_async(stream).await.expect("websocket");
        let init = read_json(&mut ws).await;
        assert_eq!(init["method"], "initialize");
        assert_eq!(init["params"]["capabilities"]["experimentalApi"], true);
        reply(&mut ws, &init["id"], json!({})).await;
        assert_eq!(read_json(&mut ws).await["method"], "initialized");

        let read = read_json(&mut ws).await;
        assert_eq!(read["method"], "thread/read");
        assert_eq!(
            read["params"],
            json!({"threadId":THREAD,"includeTurns":false})
        );
        reply(
            &mut ws,
            &read["id"],
            json!({"thread":{"id":THREAD,"cwd":project_root,"status":{"type":"active"},
                    "model":MODEL,"reasoningEffort":EFFORT}}),
        )
        .await;

        let resume = read_json(&mut ws).await;
        assert_eq!(resume["method"], "thread/resume");
        assert_eq!(
            resume["params"],
            json!({"threadId":THREAD,"excludeTurns":true})
        );
        let mut thread = json!({"id":THREAD,"cwd":project_root,
                                "model":MODEL,"reasoningEffort":EFFORT});
        if matches!(status, ResumeStatus::Idle) {
            thread["status"] = json!({"type":"idle"});
        }
        reply(
            &mut ws,
            &resume["id"],
            json!({"thread":thread,"model":MODEL,"reasoningEffort":EFFORT}),
        )
        .await;

        assert_no_effect(&mut ws).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "client retried after rejected thread status in {status:?}"
        );
    });

    let (_sender, mut shutdown) = watch::channel(false);
    let outcome = invoke_codex_wakeup(&config, &claim, &mut shutdown).await;
    assert!(
        matches!(outcome, WakeupOutcome::Failed { .. }),
        "case {status:?}: {outcome:?}"
    );
    assert!(!queue_marker.exists(), "queue fallback in {status:?}");
    server.await.expect("server assertions");
}

#[tokio::test]
async fn user_message_steer_refuses_missing_or_changed_resume_status_before_any_effect() {
    for status in [ResumeStatus::Missing, ResumeStatus::Idle] {
        run_case(status).await;
    }
}
