//! Independent regression cases from the primary review.
use threadlane_core::{Command, Core, WakeupOutcome, WakeupStatus};

const THREAD: &str = "93f2029c-f20c-45b9-b1ec-36be3a55c811";

fn start(core: &mut Core, binding: &str) {
    core.execute(
        binding,
        Command::StartDemo {
            command_key: "start".into(),
            instruction: "Demo only".into(),
            step_delay_ms: Some(0),
            now_ms: 0,
        },
    )
    .unwrap();
}

fn acknowledge_available(core: &mut Core, binding: &str) {
    let batch = core
        .execute(
            binding,
            Command::PullEvents {
                after_seq: None,
                limit: 50,
                now_ms: 1,
            },
        )
        .unwrap()
        .into_event_batch()
        .unwrap();
    core.execute(
        binding,
        Command::AcknowledgeEvents {
            command_key: "ack".into(),
            batch_id: batch.batch_id,
            now_ms: 2,
        },
    )
    .unwrap();
}

#[test]
fn acknowledgement_before_queue_result_releases_the_notice() {
    let temp = tempfile::tempdir().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let binding = core.create_binding(THREAD, "review").unwrap().binding_id;
    start(&mut core, &binding);
    core.tick(0).unwrap();
    let claim = core.claim_wakeup(1).unwrap().unwrap();
    acknowledge_available(&mut core, &binding);
    core.record_wakeup_result(&claim.claim_id, WakeupOutcome::Submitted, 3)
        .unwrap();
    assert_eq!(
        core.get_wakeup(&binding).unwrap().status,
        WakeupStatus::Idle
    );
}

#[test]
fn acknowledged_inflight_notice_does_not_block_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("state.sqlite");
    let mut core = Core::open(&db).unwrap();
    let binding = core.create_binding(THREAD, "review").unwrap().binding_id;
    start(&mut core, &binding);
    core.tick(0).unwrap();
    core.claim_wakeup(1).unwrap().unwrap();
    acknowledge_available(&mut core, &binding);
    drop(core);
    let core = Core::open(&db).unwrap();
    assert_eq!(
        core.get_wakeup(&binding).unwrap().status,
        WakeupStatus::Idle
    );
}

#[test]
fn an_empty_batch_cannot_acknowledge_a_different_nonempty_batch() {
    let temp = tempfile::tempdir().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let binding = core.create_binding(THREAD, "review").unwrap().binding_id;
    start(&mut core, &binding);
    let seen = core
        .execute(
            &binding,
            Command::PullEvents {
                after_seq: None,
                limit: 50,
                now_ms: 1,
            },
        )
        .unwrap()
        .into_event_batch()
        .unwrap();
    let empty = core
        .execute(
            &binding,
            Command::PullEvents {
                after_seq: Some(seen.to_seq),
                limit: 50,
                now_ms: 1,
            },
        )
        .unwrap()
        .into_event_batch()
        .unwrap();
    assert!(empty.events.is_empty());
    core.execute(
        &binding,
        Command::AcknowledgeEvents {
            command_key: "empty-ack".into(),
            batch_id: empty.batch_id,
            now_ms: 2,
        },
    )
    .unwrap();
    assert_eq!(core.get_binding(&binding).unwrap().acked_seq, 0);
}

#[test]
fn inflight_ack_of_old_coverage_schedules_newer_events_without_resending_old() {
    let temp = tempfile::tempdir().unwrap();
    let mut core = Core::open(temp.path().join("state.sqlite")).unwrap();
    let binding = core.create_binding(THREAD, "review").unwrap().binding_id;

    // Old coverage: run 1 emits QuestionAsked at seq 3.
    start(&mut core, &binding);
    core.tick(0).unwrap();
    let claim_1 = core.claim_wakeup(1).unwrap().unwrap();
    assert_eq!(claim_1.pending_since_seq, 3);
    assert_eq!(claim_1.covered_up_to_seq, 3);

    // Acknowledge old coverage (1..=3) while claim_1 is still InFlight.
    acknowledge_available(&mut core, &binding);

    // New important event arrives at seq 6 while claim_1 is still InFlight.
    core.execute(
        &binding,
        Command::StartDemo {
            command_key: "start-2".into(),
            instruction: "Second run".into(),
            step_delay_ms: Some(0),
            now_ms: 2,
        },
    )
    .unwrap();
    core.tick(2).unwrap();

    // InFlight ownership remains intact until result is recorded.
    let inflight = core.get_wakeup(&binding).unwrap();
    assert_eq!(inflight.status, WakeupStatus::InFlight);
    assert_eq!(
        inflight.claim_id.as_deref(),
        Some(claim_1.claim_id.as_str())
    );

    // Recording result reconciles old ACK (<= 3) and schedules only new coverage (seq 6) as Pending.
    let notice = core
        .record_wakeup_result(&claim_1.claim_id, WakeupOutcome::Submitted, 3)
        .unwrap();
    assert_eq!(notice.status, WakeupStatus::Pending);
    assert_eq!(notice.pending_since_seq, Some(6));
    assert_eq!(notice.covered_up_to_seq, 0);
    assert_eq!(notice.coalesced_count, 1);

    let claim_2 = core.claim_wakeup(4).unwrap().unwrap();
    assert_eq!(claim_2.pending_since_seq, 6);
    assert_eq!(claim_2.covered_up_to_seq, 6);
    assert_eq!(claim_2.coalesced_count, 1);
}

#[test]
fn reopen_reconciles_acked_old_coverage_to_pending_for_new_events_and_keeps_unacked_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("state.sqlite");
    let mut core = Core::open(&db).unwrap();
    let binding_acked_old = core.create_binding(THREAD, "acked-old").unwrap().binding_id;
    let binding_unacked = core
        .create_binding("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "unacked")
        .unwrap()
        .binding_id;

    // binding_acked_old: claim seq 3, ack 1..=3, then emit new important event at seq 6 while InFlight.
    start(&mut core, &binding_acked_old);
    core.tick(0).unwrap();
    core.claim_wakeup_for_binding(&binding_acked_old, 1)
        .unwrap()
        .unwrap();
    acknowledge_available(&mut core, &binding_acked_old);
    core.execute(
        &binding_acked_old,
        Command::StartDemo {
            command_key: "start-2".into(),
            instruction: "Second run".into(),
            step_delay_ms: Some(0),
            now_ms: 2,
        },
    )
    .unwrap();
    core.tick(2).unwrap();

    // binding_unacked: claim seq 3, do not acknowledge.
    start(&mut core, &binding_unacked);
    core.tick(0).unwrap();
    core.claim_wakeup_for_binding(&binding_unacked, 1)
        .unwrap()
        .unwrap();

    drop(core);
    let mut core = Core::open(&db).unwrap();

    // Unacknowledged InFlight claim stays Unknown and never auto-retries.
    assert_eq!(
        core.get_wakeup(&binding_unacked).unwrap().status,
        WakeupStatus::Unknown
    );
    assert_eq!(
        core.claim_wakeup_for_binding(&binding_unacked, 5).unwrap(),
        None
    );

    // Acknowledged old coverage with new unread important event (seq 6) reconciles to Pending for seq 6 only.
    let notice = core.get_wakeup(&binding_acked_old).unwrap();
    assert_eq!(notice.status, WakeupStatus::Pending);
    assert_eq!(notice.pending_since_seq, Some(6));
    assert_eq!(notice.coalesced_count, 1);

    let claim_new = core
        .claim_wakeup_for_binding(&binding_acked_old, 5)
        .unwrap()
        .unwrap();
    assert_eq!(claim_new.pending_since_seq, 6);
    assert_eq!(claim_new.covered_up_to_seq, 6);
    assert_eq!(claim_new.coalesced_count, 1);
}

#[test]
fn v1_database_without_wakeup_retries_upgrades_additively_for_retry_and_escaped_ticks() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("state.sqlite");

    let instruction = "\u{1}".repeat(2_411) + &"a".repeat(1_685);
    let (binding_id, run_id, claim_id) = {
        let mut core = Core::open(&db).unwrap();
        let binding = core.create_binding(THREAD, "v1-compat").unwrap().binding_id;
        let run = core
            .execute(
                &binding,
                Command::StartDemo {
                    command_key: "start-v1".into(),
                    instruction: instruction.clone(),
                    step_delay_ms: Some(10),
                    now_ms: 100,
                },
            )
            .unwrap()
            .into_run()
            .unwrap();
        core.tick(110).unwrap();
        let claim = core.claim_wakeup(115).unwrap().unwrap();
        core.record_wakeup_result(
            &claim.claim_id,
            WakeupOutcome::Failed {
                error: "initial failure".into(),
            },
            120,
        )
        .unwrap();
        (binding, run.run_id, claim.claim_id)
    };

    // Simulate a database from the earlier v1 build before `wakeup_retries` existed.
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute("DROP TABLE wakeup_retries", []).unwrap();
    }

    // Reopening at SCHEMA_VERSION = 1 recreates `wakeup_retries` via CREATE TABLE IF NOT EXISTS.
    let mut core = Core::open(&db).unwrap();
    let rearmed = core
        .retry_wakeup(&binding_id, &claim_id, false, 130)
        .unwrap();
    assert_eq!(rearmed.status, WakeupStatus::Pending);
    assert_eq!(rearmed.claim_id, None);
    assert_eq!(core.list_wakeup_retries(&binding_id).unwrap().len(), 1);

    let run = core.get_run(&binding_id, &run_id).unwrap();
    assert_eq!(run.instruction, instruction);
}
