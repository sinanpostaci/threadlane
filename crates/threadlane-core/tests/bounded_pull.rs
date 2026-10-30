use rusqlite::{params, Connection};
use tempfile::tempdir;
use threadlane_core::{
    AckErrorReason, Command, CommandOutput, Core, CoreError, Event, EventBatch, EventKind,
    EventPayload, MAX_EVENT_BATCH_BYTES, MAX_EVENT_BATCH_SIZE, MIN_EVENT_BATCH_BYTES,
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

fn load_event_batch_row(
    db_path: &std::path::Path,
    batch_id: &str,
) -> Option<(String, i64, i64, i64)> {
    let conn = Connection::open(db_path).expect("open sqlite db");
    conn.query_row(
        "SELECT binding_id, from_seq, to_seq, event_count
         FROM event_batches
         WHERE batch_id = ?1",
        params![batch_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .ok()
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

#[test]
fn pull_events_bounded_validates_budget_and_rolls_back_when_first_event_cannot_fit() {
    let dir = tempdir().expect("create tempdir");
    let db_path = dir.path().join("state.sqlite3");
    let mut core = Core::open(&db_path).expect("open core");
    let binding = core
        .create_binding(UUID_1, "bounded-rollback")
        .expect("create binding");

    let run = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "cmd-start".to_string(),
                instruction: "seed run for bounded rollback".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("run output");

    // Drain and acknowledge the initial RunStarted event so seq 2 is the first eligible event.
    let initial_batch = core
        .execute(
            &binding.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: 10,
                max_batch_bytes: 4_096,
                now_ms: 1_010,
            },
        )
        .expect("pull initial batch")
        .into_event_batch()
        .expect("event batch");
    assert_eq!(initial_batch.events.len(), 1);
    assert_eq!(initial_batch.from_seq, 1);
    assert_eq!(initial_batch.to_seq, 1);

    core.execute(
        &binding.binding_id,
        Command::AcknowledgeEvents {
            command_key: "cmd-ack-initial".to_string(),
            batch_id: initial_batch.batch_id,
            now_ms: 1_020,
        },
    )
    .expect("ack initial batch");

    // Insert a large escaped payload event at seq 2 (~3.5 KiB JSON) followed by a small event at seq 3.
    let escaped_detail = "\"quote\"\\slash\\\nnewline\t\u{0007}bell-🔥-".repeat(120);
    insert_custom_event(
        &db_path,
        &binding.binding_id,
        &run.run_id,
        2,
        1,
        2,
        &escaped_detail,
        1_030,
    );
    insert_custom_event(
        &db_path,
        &binding.binding_id,
        &run.run_id,
        3,
        1,
        3,
        "small-followup",
        1_040,
    );

    let batches_before = count_event_batches(&db_path, &binding.binding_id);
    assert_eq!(batches_before, 1);

    // 1. Invalid budgets (< 1024 or > 131072) must fail validation before any SQLite mutation.
    for invalid_budget in [0, MIN_EVENT_BATCH_BYTES - 1, MAX_EVENT_BATCH_BYTES + 1] {
        let err = core
            .execute(
                &binding.binding_id,
                Command::PullEventsBounded {
                    after_seq: None,
                    limit: 10,
                    max_batch_bytes: invalid_budget,
                    now_ms: 1_050,
                },
            )
            .expect_err("invalid budget must be rejected");
        assert!(
            matches!(
                err,
                CoreError::Validation {
                    field: "max_batch_bytes",
                    ..
                }
            ),
            "expected max_batch_bytes validation error, got: {err:?}"
        );

        let b = core.get_binding(&binding.binding_id).expect("get binding");
        assert_eq!(b.delivered_seq, 1);
        assert_eq!(b.acked_seq, 1);
        assert_eq!(
            count_event_batches(&db_path, &binding.binding_id),
            batches_before
        );
    }

    // 2. When the first eligible event (seq 2) exceeds max_batch_bytes, PullEventsBounded must
    // fail with ResponseTooLarge before advancing delivered_seq or inserting into event_batches.
    let err = core
        .execute(
            &binding.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: 10,
                max_batch_bytes: MIN_EVENT_BATCH_BYTES,
                now_ms: 1_060,
            },
        )
        .expect_err("oversized first event must be rejected");
    match err {
        CoreError::ResponseTooLarge {
            size_bytes,
            max_bytes,
        } => {
            assert_eq!(max_bytes, MIN_EVENT_BATCH_BYTES);
            assert!(size_bytes > MIN_EVENT_BATCH_BYTES);
        }
        other => panic!("expected ResponseTooLarge, got: {other:?}"),
    }

    let b = core.get_binding(&binding.binding_id).expect("get binding");
    assert_eq!(b.delivered_seq, 1, "delivered_seq must not advance");
    assert_eq!(b.acked_seq, 1, "acked_seq must not change");
    assert_eq!(
        count_event_batches(&db_path, &binding.binding_id),
        batches_before,
        "no event_batches row may be inserted when first event cannot fit"
    );
}

#[test]
fn pull_events_bounded_exact_48kib_boundary_and_escaped_prefix_trimming() {
    let dir = tempdir().expect("create tempdir");
    let db_path = dir.path().join("state.sqlite3");
    let mut core = Core::open(&db_path).expect("open core");
    let binding = core
        .create_binding(UUID_1, "exact-48k")
        .expect("create binding");

    let run = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "cmd-start".to_string(),
                instruction: "seed run for 48k boundary".to_string(),
                step_delay_ms: Some(100),
                now_ms: 2_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("run output");

    // Drain and ack seq 1 so seq 2 is the first eligible event.
    let first = core
        .execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 2_010,
            },
        )
        .expect("pull seq 1")
        .into_event_batch()
        .expect("event batch");
    core.execute(
        &binding.binding_id,
        Command::AcknowledgeEvents {
            command_key: "cmd-ack-1".to_string(),
            batch_id: first.batch_id,
            now_ms: 2_020,
        },
    )
    .expect("ack seq 1");

    // Measure the exact serialized EventBatch overhead with an empty detail string for seq 2
    // when has_more is true (because seq 3 will also be present).
    let dummy_event = Event {
        event_id: format!("evt_00000000-0000-4000-8000-{:012}", 2),
        binding_id: binding.binding_id.clone(),
        seq: 2,
        run_id: run.run_id.clone(),
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
        created_at_ms: 2_030,
    };
    let dummy_batch = EventBatch {
        batch_id: "bat_00000000000000000000000000000000".to_string(),
        binding_id: binding.binding_id.clone(),
        events: vec![dummy_event],
        from_seq: 2,
        to_seq: 2,
        delivered_seq: 2,
        acked_seq: 1,
        has_more: true,
    };
    let base_bytes = serde_json::to_vec(&dummy_batch)
        .expect("serialize dummy batch")
        .len();
    let target_budget = 48 * 1024; // 49_152 bytes
    let filler_len = target_budget - base_bytes;
    let exact_detail = "x".repeat(filler_len);

    insert_custom_event(
        &db_path,
        &binding.binding_id,
        &run.run_id,
        2,
        1,
        2,
        &exact_detail,
        2_030,
    );
    insert_custom_event(
        &db_path,
        &binding.binding_id,
        &run.run_id,
        3,
        1,
        3,
        &"\"escaped\"\\payload\\\n".repeat(150),
        2_040,
    );

    // At 49_151 bytes (one byte below 48 KiB), even the single trimmed event at seq 2 cannot fit.
    let err = core
        .execute(
            &binding.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: 10,
                max_batch_bytes: target_budget - 1,
                now_ms: 2_050,
            },
        )
        .expect_err("49_151 budget must reject 49_152-byte single-event batch");
    assert!(matches!(
        err,
        CoreError::ResponseTooLarge {
            size_bytes: 49_152,
            max_bytes: 49_151,
        }
    ));
    assert_eq!(
        core.get_binding(&binding.binding_id)
            .expect("binding")
            .delivered_seq,
        1
    );
    assert_eq!(count_event_batches(&db_path, &binding.binding_id), 1);

    // At exactly 49_152 bytes (48 KiB), seq 3 is byte-trimmed and seq 2 fits at the exact byte limit.
    let batch = core
        .execute(
            &binding.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: 10,
                max_batch_bytes: target_budget,
                now_ms: 2_060,
            },
        )
        .expect("49_152 budget must fit seq 2 after trimming seq 3")
        .into_event_batch()
        .expect("event batch");

    let serialized_len = serde_json::to_vec(&batch).expect("serialize batch").len();
    assert_eq!(serialized_len, target_budget);
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].seq, 2);
    assert_eq!(batch.from_seq, 2);
    assert_eq!(batch.to_seq, 2);
    assert_eq!(batch.delivered_seq, 2);
    assert_eq!(batch.acked_seq, 1);
    assert!(
        batch.has_more,
        "has_more must be true when byte-trimmed from 2 events to 1"
    );

    // Verify the persisted event_batches row matches the trimmed batch.
    let persisted =
        load_event_batch_row(&db_path, &batch.batch_id).expect("persisted event_batches row");
    assert_eq!(
        persisted,
        (binding.binding_id.clone(), 2, 2, 1),
        "event_batches row must record trimmed from_seq, to_seq, and event_count"
    );
}

#[test]
fn pull_events_bounded_cursor_ack_replay_empty_and_legacy_semantics() {
    let dir = tempdir().expect("create tempdir");
    let db_path = dir.path().join("state.sqlite3");
    let mut core = Core::open(&db_path).expect("open core");
    let binding_a = core
        .create_binding(UUID_1, "binding-a")
        .expect("create binding a");
    let binding_b = core
        .create_binding(UUID_2, "binding-b")
        .expect("create binding b");

    let run_a = core
        .execute(
            &binding_a.binding_id,
            Command::StartDemo {
                command_key: "cmd-start-a".to_string(),
                instruction: "instruction a".to_string(),
                step_delay_ms: Some(100),
                now_ms: 3_000,
            },
        )
        .expect("start demo a")
        .into_run()
        .expect("run a");

    let _run_b = core
        .execute(
            &binding_b.binding_id,
            Command::StartDemo {
                command_key: "cmd-start-b".to_string(),
                instruction: "instruction b".to_string(),
                step_delay_ms: Some(100),
                now_ms: 3_000,
            },
        )
        .expect("start demo b")
        .into_run()
        .expect("run b");

    // Append 4 escaped-payload events on binding_a (seqs 2..=5).
    let chunk = "\"escaped\"\\segment\\\n\t".repeat(45); // ~1.1 KiB before JSON escaping
    let mut appended_events = Vec::new();
    for seq in 2..=5 {
        appended_events.push(insert_custom_event(
            &db_path,
            &binding_a.binding_id,
            &run_a.run_id,
            seq,
            1,
            seq,
            &format!("seq-{seq}-{chunk}"),
            3_000 + (seq as i64) * 10,
        ));
    }

    // Compute the exact serialized size of the intended second batch (seqs 3 and 4) with the
    // 36-character batch ID that PullEventsBounded generates, so the budget exercises a two-event
    // ordered prefix while still byte-trimming seq 5.
    let two_event_probe = EventBatch {
        batch_id: "bat_00000000000000000000000000000000".to_string(),
        binding_id: binding_a.binding_id.clone(),
        events: vec![appended_events[1].clone(), appended_events[2].clone()],
        from_seq: 3,
        to_seq: 4,
        delivered_seq: 4,
        acked_seq: 0,
        has_more: true,
    };
    let batch2_budget = serde_json::to_vec(&two_event_probe)
        .expect("serialize two-event probe")
        .len();

    // Legacy PullEvents on a separate check: verify it is not byte-trimmed if called with after_seq.
    // First, do bounded pulls on binding_a with a 2_600-byte budget for the first batch.
    let batch1 = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: MAX_EVENT_BATCH_SIZE,
                max_batch_bytes: 2_600,
                now_ms: 3_100,
            },
        )
        .expect("pull batch 1")
        .into_event_batch()
        .expect("batch 1");
    assert!(serde_json::to_vec(&batch1).expect("json").len() <= 2_600);
    assert_eq!(batch1.from_seq, 1);
    assert_eq!(batch1.to_seq, 2);
    assert_eq!(batch1.events.len(), 2);
    assert_eq!(batch1.delivered_seq, 2);
    assert_eq!(batch1.acked_seq, 0);
    assert!(batch1.has_more);

    // Unseen cursor (> delivered_seq) must be rejected before advancing state.
    let unseen_err = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: Some(3),
                limit: 10,
                max_batch_bytes: 2_600,
                now_ms: 3_110,
            },
        )
        .expect_err("after_seq > delivered_seq must fail");
    assert!(matches!(
        unseen_err,
        CoreError::UnseenEventCursor {
            requested_after_seq: 3,
            delivered_seq: 2,
            ..
        }
    ));

    // Follow-up pull using after_seq = Some(batch1.to_seq) before ACKing batch 1.
    let batch2 = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: Some(batch1.to_seq),
                limit: MAX_EVENT_BATCH_SIZE,
                max_batch_bytes: batch2_budget,
                now_ms: 3_120,
            },
        )
        .expect("pull batch 2")
        .into_event_batch()
        .expect("batch 2");
    assert_eq!(
        serde_json::to_vec(&batch2).expect("json").len(),
        batch2_budget
    );
    assert_eq!(batch2.from_seq, 3);
    assert_eq!(batch2.to_seq, 4);
    assert_eq!(batch2.events.len(), 2);
    assert_eq!(batch2.delivered_seq, 4);
    assert_eq!(batch2.acked_seq, 0);
    assert!(batch2.has_more);

    // Out-of-order ACK of batch 2 before batch 1 must fail with SkippedEvents.
    let skip_err = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-batch2-early".to_string(),
                batch_id: batch2.batch_id.clone(),
                now_ms: 3_130,
            },
        )
        .expect_err("ack batch 2 before batch 1 must fail");
    assert!(matches!(
        skip_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::SkippedEvents { .. },
            ..
        }
    ));

    // Cross-binding isolation: binding_b cannot ACK binding_a's batch, and binding_b cursors are 0.
    let cross_err = core
        .execute(
            &binding_b.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-cross".to_string(),
                batch_id: batch1.batch_id.clone(),
                now_ms: 3_135,
            },
        )
        .expect_err("cross-binding ack must fail");
    assert!(matches!(
        cross_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::CrossBindingBatch,
            ..
        }
    ));
    let b_state = core.get_binding(&binding_b.binding_id).expect("binding b");
    assert_eq!(b_state.delivered_seq, 0);
    assert_eq!(b_state.acked_seq, 0);

    // ACK batch 1, then verify idempotent retry and duplicate ACK with a new command_key.
    let ack1 = match core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-1".to_string(),
                batch_id: batch1.batch_id.clone(),
                now_ms: 3_140,
            },
        )
        .expect("ack batch 1")
    {
        CommandOutput::AckResult(ack) => ack,
        other => panic!("expected AckResult, got {other:?}"),
    };
    assert!(ack1.advanced);
    assert_eq!(ack1.acked_seq, 2);

    let ack1_retry = match core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-1".to_string(),
                batch_id: batch1.batch_id.clone(),
                now_ms: 3_145,
            },
        )
        .expect("retry ack batch 1")
    {
        CommandOutput::AckResult(ack) => ack,
        other => panic!("expected AckResult, got {other:?}"),
    };
    assert!(ack1_retry.advanced);
    assert_eq!(ack1_retry.acked_seq, 2);

    let ack1_dup = match core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-1-second-key".to_string(),
                batch_id: batch1.batch_id.clone(),
                now_ms: 3_150,
            },
        )
        .expect("duplicate ack batch 1 with new key")
    {
        CommandOutput::AckResult(ack) => ack,
        other => panic!("expected AckResult, got {other:?}"),
    };
    assert!(!ack1_dup.advanced);
    assert_eq!(ack1_dup.acked_seq, 2);

    // ACK batch 2, then pull remaining event 5.
    let ack2 = match core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cmd-ack-2".to_string(),
                batch_id: batch2.batch_id,
                now_ms: 3_160,
            },
        )
        .expect("ack batch 2")
    {
        CommandOutput::AckResult(ack) => ack,
        other => panic!("expected AckResult, got {other:?}"),
    };
    assert!(ack2.advanced);
    assert_eq!(ack2.acked_seq, 4);

    let batch3 = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: MAX_EVENT_BATCH_SIZE,
                max_batch_bytes: 2_600,
                now_ms: 3_170,
            },
        )
        .expect("pull batch 3")
        .into_event_batch()
        .expect("batch 3");
    assert_eq!(batch3.from_seq, 5);
    assert_eq!(batch3.to_seq, 5);
    assert_eq!(batch3.events.len(), 1);
    assert_eq!(batch3.delivered_seq, 5);
    assert_eq!(batch3.acked_seq, 4);
    assert!(!batch3.has_more);

    core.execute(
        &binding_a.binding_id,
        Command::AcknowledgeEvents {
            command_key: "cmd-ack-3".to_string(),
            batch_id: batch3.batch_id,
            now_ms: 3_180,
        },
    )
    .expect("ack batch 3");

    // Empty batch semantics when all events are acknowledged.
    let empty_batch = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: None,
                limit: 10,
                max_batch_bytes: MIN_EVENT_BATCH_BYTES,
                now_ms: 3_190,
            },
        )
        .expect("pull empty bounded batch")
        .into_event_batch()
        .expect("empty batch");
    assert!(empty_batch.events.is_empty());
    assert_eq!(empty_batch.from_seq, 5);
    assert_eq!(empty_batch.to_seq, 5);
    assert_eq!(empty_batch.delivered_seq, 5);
    assert_eq!(empty_batch.acked_seq, 5);
    assert!(!empty_batch.has_more);

    // Replay from after_seq = Some(0) with bounded pull trims to prefix without rewinding cursors.
    let replay_bounded = core
        .execute(
            &binding_a.binding_id,
            Command::PullEventsBounded {
                after_seq: Some(0),
                limit: 10,
                max_batch_bytes: 2_600,
                now_ms: 3_200,
            },
        )
        .expect("replay bounded")
        .into_event_batch()
        .expect("replay batch");
    assert_eq!(replay_bounded.from_seq, 1);
    assert_eq!(replay_bounded.to_seq, 2);
    assert_eq!(replay_bounded.delivered_seq, 5);
    assert_eq!(replay_bounded.acked_seq, 5);
    assert!(replay_bounded.has_more);

    // Legacy PullEvents from after_seq = Some(0) returns all 5 events without byte trimming.
    let legacy_batch = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: Some(0),
                limit: 10,
                now_ms: 3_210,
            },
        )
        .expect("legacy pull events")
        .into_event_batch()
        .expect("legacy batch");
    assert_eq!(legacy_batch.events.len(), 5);
    assert_eq!(legacy_batch.from_seq, 1);
    assert_eq!(legacy_batch.to_seq, 5);
    assert!(!legacy_batch.has_more);
}
