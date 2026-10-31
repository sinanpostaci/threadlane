use tempfile::TempDir;
use threadlane_core::{
    Command, Core, WakeupOutcome, WakeupStatus, FIXED_WAKEUP_MESSAGE, WAKEUP_NOTICE_STATUS_MESSAGE,
};

const THREAD_UUID: &str = "11111111-2222-4333-8444-555555555555";
const LEGACY_WAKEUP_MESSAGE: &str =
    "Threadlane has pending events. Pull and process Threadlane events via MCP.";
const ROUTE_NEUTRAL_NOTICE_MESSAGE: &str =
    "Threadlane has pending events. Use the configured event interface to pull and acknowledge them.";

#[test]
fn notice_message_stays_route_neutral_before_and_after_ack_while_claim_keeps_legacy_text() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("wakeup_status_message.db");
    let mut core = Core::open(&db_path).expect("open core");
    let binding = core
        .create_binding(THREAD_UUID, "Route-neutral notice")
        .expect("create binding");

    assert_eq!(FIXED_WAKEUP_MESSAGE, LEGACY_WAKEUP_MESSAGE);
    assert_eq!(WAKEUP_NOTICE_STATUS_MESSAGE, ROUTE_NEUTRAL_NOTICE_MESSAGE);

    core.execute(
        &binding.binding_id,
        Command::StartDemo {
            command_key: "start".to_string(),
            instruction: "Exercise route-neutral notice status".to_string(),
            step_delay_ms: Some(100),
            now_ms: 1_000,
        },
    )
    .expect("start demo");
    core.tick(1_100).expect("tick to question");

    let pending = core
        .get_wakeup(&binding.binding_id)
        .expect("pending notice");
    assert_eq!(pending.status, WakeupStatus::Pending);
    assert_eq!(pending.message, ROUTE_NEUTRAL_NOTICE_MESSAGE);
    assert_eq!(pending.message, WAKEUP_NOTICE_STATUS_MESSAGE);
    assert_ne!(pending.message, FIXED_WAKEUP_MESSAGE);
    assert!(!pending.message.contains("MCP"));

    let claim = core
        .claim_wakeup(1_200)
        .expect("claim wakeup")
        .expect("pending claim");
    assert_eq!(claim.message, LEGACY_WAKEUP_MESSAGE);
    assert_eq!(claim.message, FIXED_WAKEUP_MESSAGE);

    let in_flight = core
        .get_wakeup(&binding.binding_id)
        .expect("in-flight notice");
    assert_eq!(in_flight.status, WakeupStatus::InFlight);
    assert_eq!(in_flight.message, ROUTE_NEUTRAL_NOTICE_MESSAGE);

    core.record_wakeup_result(&claim.claim_id, WakeupOutcome::Submitted, 1_250)
        .expect("record submitted");

    let submitted = core
        .get_wakeup(&binding.binding_id)
        .expect("submitted notice");
    assert_eq!(submitted.status, WakeupStatus::Submitted);
    assert_eq!(submitted.message, ROUTE_NEUTRAL_NOTICE_MESSAGE);

    let batch = core
        .execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 1_300,
            },
        )
        .expect("pull events")
        .into_event_batch()
        .expect("event batch");
    assert_eq!(batch.to_seq, 3);

    let ack = core
        .execute(
            &binding.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack".to_string(),
                batch_id: batch.batch_id,
                now_ms: 1_400,
            },
        )
        .expect("ack events")
        .into_ack_result()
        .expect("ack result");
    assert_eq!(ack.wakeup_status, WakeupStatus::Idle);

    let idle = core.get_wakeup(&binding.binding_id).expect("idle notice");
    assert_eq!(idle.status, WakeupStatus::Idle);
    assert_eq!(idle.message, ROUTE_NEUTRAL_NOTICE_MESSAGE);
    assert_eq!(idle.message, pending.message);
    assert_eq!(claim.message, LEGACY_WAKEUP_MESSAGE);

    drop(core);
    let reopened = Core::open(&db_path).expect("reopen core");
    let reopened_notice = reopened
        .get_wakeup(&binding.binding_id)
        .expect("reopened notice");
    assert_eq!(reopened_notice.status, WakeupStatus::Idle);
    assert_eq!(reopened_notice.message, ROUTE_NEUTRAL_NOTICE_MESSAGE);
    assert_eq!(reopened_notice.message, submitted.message);
}
