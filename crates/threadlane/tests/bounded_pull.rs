use std::time::Duration;

use rusqlite::{params, Connection};
use tempfile::tempdir;
use threadlane::{Error, ErrorCode, ScopedRequest, Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_core::{
    Event, EventBatch, EventKind, EventPayload, MAX_EVENT_BATCH_BYTES, MIN_EVENT_BATCH_BYTES,
};

const UUID_1: &str = "0195f1c0-7c10-7000-8000-000000000001";
const UUID_2: &str = "0195f1c0-7c10-7000-8000-000000000002";

fn count_event_batches(db_path: &std::path::Path, binding_id: &str) -> i64 {
    let conn = Connection::open(db_path).expect("open sqlite db");
    conn.query_row(
        "SELECT COUNT(*) FROM event_batches WHERE binding_id = ?1",
        params![binding_id],
        |row| row.get(0),
    )
    .expect("count event_batches")
}

#[allow(clippy::too_many_arguments)]
fn insert_custom_event(
    db_path: &std::path::Path,
    binding_id: &str,
    run_id: &str,
    seq: u64,
    revision: u64,
    step_seq: u64,
    detail: &str,
    created_at_ms: i64,
) -> Event {
    let event_id = format!("evt_00000000-0000-4000-8000-{seq:012}");
    let payload = EventPayload {
        untrusted_data: true,
        summary: detail.to_string(),
        question_id: None,
        question_prompt: None,
        answer: None,
        instruction: None,
        cancel_reason: None,
    };
    let payload_json = serde_json::to_string(&payload).expect("serialize payload");

    let conn = Connection::open(db_path).expect("open sqlite db");
    conn.execute(
        "INSERT INTO events (
            event_id, binding_id, seq, run_id, revision, step_seq,
            kind, important, payload_json, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9)",
        params![
            event_id,
            binding_id,
            seq as i64,
            run_id,
            revision as i64,
            step_seq as i64,
            EventKind::RunProgress.as_str(),
            payload_json,
            created_at_ms,
        ],
    )
    .expect("insert custom event");
    conn.execute(
        "UPDATE bindings
         SET next_event_seq = CASE WHEN next_event_seq <= ?1 THEN ?1 + 1 ELSE next_event_seq END
         WHERE binding_id = ?2",
        params![seq as i64, binding_id],
    )
    .expect("advance next_event_seq");

    Event {
        event_id,
        binding_id: binding_id.to_string(),
        seq,
        run_id: run_id.to_string(),
        revision,
        step_seq,
        kind: EventKind::RunProgress,
        important: false,
        payload,
        created_at_ms,
    }
}

#[tokio::test]
async fn scoped_client_pull_events_bounded_48kib_boundary_rollback_and_isolation() {
    let dir = tempdir().expect("create tempdir");
    let runtime_dir = dir.path().join("runtime");
    let db_path = runtime_dir.join("state.db");

    let mut cfg = SupervisorConfig::new(&runtime_dir);
    cfg.tick_interval = Duration::from_secs(60);

    let supervisor = Supervisor::start(cfg).await.expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&runtime_dir).expect("create client");
    let alpha_binding = client
        .create_binding(UUID_1, "alpha")
        .await
        .expect("create alpha binding");
    let beta_binding = client
        .create_binding(UUID_2, "beta")
        .await
        .expect("create beta binding");

    let alpha_id = alpha_binding.binding_id;
    let beta_id = beta_binding.binding_id;

    let bindings = client.list_bindings().await.expect("list bindings");
    assert_eq!(bindings.len(), 2);

    let alpha = client.clone().scoped(&alpha_id).expect("scoped alpha");
    let beta = client.scoped(&beta_id).expect("scoped beta");

    // Verify wire serialization of ScopedRequest::PullEventsBounded.
    let wire_json = serde_json::to_value(ScopedRequest::PullEventsBounded {
        after_seq: Some(2),
        limit: Some(5),
        max_batch_bytes: 49_152,
    })
    .expect("serialize ScopedRequest::PullEventsBounded");
    assert_eq!(wire_json["op"], "pull_events_bounded");
    assert_eq!(wire_json["after_seq"], 2);
    assert_eq!(wire_json["limit"], 5);
    assert_eq!(wire_json["max_batch_bytes"], 49_152);

    // Start demo runs on both bindings.
    let run_alpha = alpha
        .start_demo(
            "cmd-alpha-start".to_string(),
            "alpha task".to_string(),
            Some(500),
        )
        .await
        .expect("start alpha demo");
    let _run_beta = beta
        .start_demo(
            "cmd-beta-start".to_string(),
            "beta task".to_string(),
            Some(500),
        )
        .await
        .expect("start beta demo");

    // Pull and ACK seq 1 on alpha.
    let initial = alpha
        .pull_events_bounded(None, Some(10), 4_096)
        .await
        .expect("pull initial alpha batch");
    assert_eq!(initial.events.len(), 1);
    assert_eq!(initial.from_seq, 1);
    assert_eq!(initial.to_seq, 1);
    alpha
        .ack_events("cmd-alpha-ack-1".to_string(), initial.batch_id)
        .await
        .expect("ack initial alpha batch");

    // Construct seq 2 on alpha so the candidate EventBatch at seq 2 (with has_more = true)
    // is exactly 48 KiB (49_152 bytes), and append escaped-payload events at seq 3 and seq 4.
    let dummy_event = Event {
        event_id: format!("evt_00000000-0000-4000-8000-{:012}", 2),
        binding_id: alpha_id.clone(),
        seq: 2,
        run_id: run_alpha.run_id.clone(),
        revision: 1,
        step_seq: 2,
        kind: EventKind::RunProgress,
        important: false,
        payload: EventPayload {
            untrusted_data: true,
            summary: String::new(),
            question_id: None,
            question_prompt: None,
            answer: None,
            instruction: None,
            cancel_reason: None,
        },
        created_at_ms: 5_000,
    };
    let dummy_batch = EventBatch {
        batch_id: "bat_00000000000000000000000000000000".to_string(),
        binding_id: alpha_id.clone(),
        events: vec![dummy_event],
        from_seq: 2,
        to_seq: 2,
        delivered_seq: 2,
        acked_seq: 1,
        has_more: true,
    };
    let base_len = serde_json::to_vec(&dummy_batch)
        .expect("serialize dummy batch")
        .len();
    let budget_48k = 48 * 1024;
    let exact_detail = "A".repeat(budget_48k - base_len);

    insert_custom_event(
        &db_path,
        &alpha_id,
        &run_alpha.run_id,
        2,
        1,
        2,
        &exact_detail,
        5_000,
    );
    let escaped_chunk = "\"quote\"\\backslash\\\n\r\t\u{0007}".repeat(45);
    let event_3 = insert_custom_event(
        &db_path,
        &alpha_id,
        &run_alpha.run_id,
        3,
        1,
        3,
        &format!("seq3-{escaped_chunk}"),
        5_010,
    );
    let event_4 = insert_custom_event(
        &db_path,
        &alpha_id,
        &run_alpha.run_id,
        4,
        1,
        4,
        &format!("seq4-{escaped_chunk}"),
        5_020,
    );

    // Compute exact one-event EventBatch budgets with the 36-character batch ID that the
    // supervisor generates, so each follow-up pull trims the next event by bytes.
    let seq3_probe = EventBatch {
        batch_id: "bat_00000000000000000000000000000000".to_string(),
        binding_id: alpha_id.clone(),
        events: vec![event_3],
        from_seq: 3,
        to_seq: 3,
        delivered_seq: 3,
        acked_seq: 1,
        has_more: true,
    };
    let seq3_budget = serde_json::to_vec(&seq3_probe)
        .expect("serialize seq3 probe")
        .len();
    let seq4_probe = EventBatch {
        batch_id: "bat_00000000000000000000000000000000".to_string(),
        binding_id: alpha_id.clone(),
        events: vec![event_4],
        from_seq: 4,
        to_seq: 4,
        delivered_seq: 4,
        acked_seq: 3,
        has_more: false,
    };
    let seq4_budget = serde_json::to_vec(&seq4_probe)
        .expect("serialize seq4 probe")
        .len();

    let batches_before = count_event_batches(&db_path, &alpha_id);
    assert_eq!(batches_before, 1);

    // 1. Invalid budgets fail with ValidationFailed and do not mutate SQLite state.
    for bad_budget in [MIN_EVENT_BATCH_BYTES - 1, MAX_EVENT_BATCH_BYTES + 1] {
        let err = alpha
            .pull_events_bounded(None, Some(10), bad_budget)
            .await
            .expect_err("invalid budget must fail");
        match err {
            Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::ValidationFailed),
            other => panic!("expected Remote(ValidationFailed), got {other:?}"),
        }
        let status = alpha.status().await.expect("alpha status");
        assert_eq!(status.binding.delivered_seq, 1);
        assert_eq!(status.binding.acked_seq, 1);
        assert_eq!(count_event_batches(&db_path, &alpha_id), batches_before);
    }

    // 2. Budget of 49_151 bytes (1 byte below 48 KiB) fails with PayloadTooLarge and rolls back.
    let too_small_err = alpha
        .pull_events_bounded(None, Some(10), budget_48k - 1)
        .await
        .expect_err("49_151 budget must fail when first event is 49_152 bytes");
    match too_small_err {
        Error::Remote(api_err) => assert_eq!(api_err.code, ErrorCode::PayloadTooLarge),
        other => panic!("expected Remote(PayloadTooLarge), got {other:?}"),
    }
    let status = alpha.status().await.expect("alpha status");
    assert_eq!(status.binding.delivered_seq, 1);
    assert_eq!(status.binding.acked_seq, 1);
    assert_eq!(count_event_batches(&db_path, &alpha_id), batches_before);

    // 3. Budget of 49_152 bytes (exact 48 KiB) trims seqs 3..=4 and returns seq 2 with has_more = true.
    let batch_seq2 = alpha
        .pull_events_bounded(None, Some(10), budget_48k)
        .await
        .expect("48 KiB budget must fit seq 2");
    assert_eq!(
        serde_json::to_vec(&batch_seq2)
            .expect("serialize batch_seq2")
            .len(),
        budget_48k
    );
    assert_eq!(batch_seq2.events.len(), 1);
    assert_eq!(batch_seq2.from_seq, 2);
    assert_eq!(batch_seq2.to_seq, 2);
    assert_eq!(batch_seq2.delivered_seq, 2);
    assert_eq!(batch_seq2.acked_seq, 1);
    assert!(batch_seq2.has_more);

    // 4. Follow-up pull for seqs 3..=4 with a budget that fits seq 3 but trims seq 4.
    let batch_seq3 = alpha
        .pull_events_bounded(Some(batch_seq2.to_seq), Some(10), seq3_budget)
        .await
        .expect("pull seq 3 bounded");
    assert_eq!(
        serde_json::to_vec(&batch_seq3).expect("json").len(),
        seq3_budget
    );
    assert_eq!(batch_seq3.events.len(), 1);
    assert_eq!(batch_seq3.from_seq, 3);
    assert_eq!(batch_seq3.to_seq, 3);
    assert_eq!(batch_seq3.delivered_seq, 3);
    assert_eq!(batch_seq3.acked_seq, 1);
    assert!(batch_seq3.has_more);

    // ACK seq 2 and seq 3 in order, then pull and ACK seq 4.
    let ack2 = alpha
        .ack_events("cmd-alpha-ack-2".to_string(), batch_seq2.batch_id.clone())
        .await
        .expect("ack seq 2");
    assert!(ack2.advanced);
    assert_eq!(ack2.acked_seq, 2);

    // Duplicate ACK of seq 2 with a new command_key is a safe no-op.
    let ack2_dup = alpha
        .ack_events("cmd-alpha-ack-2-dup".to_string(), batch_seq2.batch_id)
        .await
        .expect("duplicate ack seq 2");
    assert!(!ack2_dup.advanced);
    assert_eq!(ack2_dup.acked_seq, 2);

    let ack3 = alpha
        .ack_events("cmd-alpha-ack-3".to_string(), batch_seq3.batch_id)
        .await
        .expect("ack seq 3");
    assert!(ack3.advanced);
    assert_eq!(ack3.acked_seq, 3);

    let batch_seq4 = alpha
        .pull_events_bounded(None, Some(10), seq4_budget)
        .await
        .expect("pull seq 4 bounded");
    assert_eq!(
        serde_json::to_vec(&batch_seq4).expect("json").len(),
        seq4_budget
    );
    assert_eq!(batch_seq4.events.len(), 1);
    assert_eq!(batch_seq4.from_seq, 4);
    assert_eq!(batch_seq4.to_seq, 4);
    assert_eq!(batch_seq4.delivered_seq, 4);
    assert_eq!(batch_seq4.acked_seq, 3);
    assert!(!batch_seq4.has_more);

    let ack4 = alpha
        .ack_events("cmd-alpha-ack-4".to_string(), batch_seq4.batch_id)
        .await
        .expect("ack seq 4");
    assert!(ack4.advanced);
    assert_eq!(ack4.acked_seq, 4);

    // 5. Legacy pull_events replay returns all 4 events (including the 48 KiB event) without byte trimming.
    let legacy_all = alpha
        .pull_events(Some(0), Some(10))
        .await
        .expect("legacy pull_events replay");
    assert_eq!(legacy_all.events.len(), 4);
    assert_eq!(legacy_all.from_seq, 1);
    assert_eq!(legacy_all.to_seq, 4);
    assert!(!legacy_all.has_more);

    // 6. Beta binding remained completely isolated throughout alpha's bounded pulls and failures.
    let beta_status = beta.status().await.expect("beta status");
    assert_eq!(beta_status.binding.delivered_seq, 0);
    assert_eq!(beta_status.binding.acked_seq, 0);
    let beta_batch = beta
        .pull_events_bounded(None, Some(10), 4_096)
        .await
        .expect("beta bounded pull");
    assert_eq!(beta_batch.events.len(), 1);
    assert_eq!(beta_batch.from_seq, 1);
    assert_eq!(beta_batch.to_seq, 1);

    supervisor.shutdown().await;
}
