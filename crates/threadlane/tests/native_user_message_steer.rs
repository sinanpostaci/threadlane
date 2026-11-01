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
const TURN: &str = "01968f52-9999-7d51-9914-79d2bc48eb5b";
const APPROVAL: &str = "apr_0123456789abcdef0123456789abcdef";
const BINDING: &str = "bnd_steer_fixture";

#[derive(Clone, Copy, Debug)]
enum Case {
    Idle,
    Busy,
    MissingTurn,
    CompletedTurn,
    MalformedTurn,
    UnsupportedList,
    WrongAck,
    DisconnectAfterWrite,
    WrongModel,
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

async fn run_case(case: Case) {
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
        model: "gpt-6-luna".to_owned(),
        effort: "max".to_owned(),
        delivery: NativeNotificationDelivery::UserMessageSteer,
        reconnect: None,
    };
    let base = ApprovedSessionWakeupContext::new(APPROVAL)
        .expect("context")
        .with_operator_cli_route();
    let expected_notice = base.wakeup_message();
    let context = base
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
        let status = if matches!(case, Case::Idle) {
            "idle"
        } else {
            "active"
        };
        let thread = json!({"id":THREAD,"cwd":project_root,"status":{"type":status},
                            "model":"gpt-6-luna","reasoningEffort":"max"});
        reply(&mut ws, &read["id"], json!({"thread":thread})).await;
        let resume = read_json(&mut ws).await;
        assert_eq!(resume["method"], "thread/resume");
        assert_eq!(
            resume["params"],
            json!({"threadId":THREAD,"excludeTurns":true})
        );
        let model = if matches!(case, Case::WrongModel) {
            "unapproved-model"
        } else {
            "gpt-6-luna"
        };
        reply(
            &mut ws,
            &resume["id"],
            json!({"thread":thread,"model":model,"reasoningEffort":"max"}),
        )
        .await;
        if matches!(case, Case::WrongModel) {
            assert_no_effect(&mut ws).await;
            return;
        }
        if !matches!(case, Case::Idle) {
            let list = read_json(&mut ws).await;
            assert_eq!(list["method"], "thread/turns/list");
            assert_eq!(
                list["params"],
                json!({"threadId":THREAD,"limit":1,"sortDirection":"desc","itemsView":"summary"})
            );
            if matches!(case, Case::UnsupportedList) {
                ws.send(Message::Text(
                    json!({"id":list["id"],"error":{"code":-32601,"message":"unsupported"}})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("error response");
                assert_no_effect(&mut ws).await;
                return;
            }
            let id = if matches!(case, Case::MalformedTurn) {
                "not-a-live-turn-id"
            } else {
                TURN
            };
            let status = if matches!(case, Case::CompletedTurn) {
                "completed"
            } else {
                "inProgress"
            };
            let data = if matches!(case, Case::MissingTurn) {
                json!([])
            } else {
                json!([{"id":id,"status":status,"itemsView":"summary","items":[]}])
            };
            reply(&mut ws, &list["id"], json!({"data":data,"nextCursor":null})).await;
            if matches!(
                case,
                Case::MissingTurn | Case::CompletedTurn | Case::MalformedTurn
            ) {
                assert_no_effect(&mut ws).await;
                return;
            }
        }
        let effect = read_json(&mut ws).await;
        assert_eq!(
            effect["method"],
            if matches!(case, Case::Idle) {
                "turn/start"
            } else {
                "turn/steer"
            }
        );
        assert_eq!(effect["params"]["threadId"], THREAD);
        assert_eq!(
            effect["params"]["input"],
            json!([{"type":"text","text":expected_notice}])
        );
        assert!(effect["params"].get("toolOutput").is_none());
        assert!(effect["params"].get("model").is_none());
        assert!(effect["params"].get("effort").is_none());
        if !matches!(case, Case::Idle) {
            assert_eq!(effect["params"]["expectedTurnId"], TURN);
        }
        if matches!(case, Case::DisconnectAfterWrite) {
            return;
        }
        let result = if matches!(case, Case::Idle) {
            json!({"turn":{"id":TURN,"status":"inProgress","error":null}})
        } else {
            json!({"turnId": if matches!(case, Case::WrongAck) {THREAD} else {TURN}})
        };
        reply(&mut ws, &effect["id"], result).await;
        assert_no_effect(&mut ws).await;
    });
    let (_sender, mut shutdown) = watch::channel(false);
    let outcome = invoke_codex_wakeup(&config, &claim, &mut shutdown).await;
    let expected = match case {
        Case::Idle | Case::Busy => matches!(outcome, WakeupOutcome::Submitted),
        Case::WrongAck | Case::DisconnectAfterWrite => {
            matches!(outcome, WakeupOutcome::Unknown { .. })
        }
        _ => matches!(outcome, WakeupOutcome::Failed { .. }),
    };
    assert!(expected, "case {case:?}: {outcome:?}");
    assert!(!queue_marker.exists(), "queue fallback in {case:?}");
    server.await.expect("server assertions");
}

#[tokio::test]
async fn direct_notifications_use_idle_start_or_exact_busy_steer_without_queue_or_model_override() {
    for case in [Case::Idle, Case::Busy] {
        run_case(case).await;
    }
}

#[tokio::test]
async fn direct_notifications_refuse_missing_stale_unsupported_or_unapproved_metadata_before_send()
{
    for case in [
        Case::MissingTurn,
        Case::CompletedTurn,
        Case::MalformedTurn,
        Case::UnsupportedList,
        Case::WrongModel,
    ] {
        run_case(case).await;
    }
}

#[tokio::test]
async fn direct_notifications_do_not_replay_mismatched_or_disconnected_submissions() {
    for case in [Case::WrongAck, Case::DisconnectAfterWrite] {
        run_case(case).await;
    }
}
