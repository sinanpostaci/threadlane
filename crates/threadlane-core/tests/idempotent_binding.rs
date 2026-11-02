use std::sync::{Arc, Barrier};
use std::thread;

use threadlane_core::{
    Command, Core, CoreError, GetOrCreateBindingOutcome, WakeupStatus, MAX_LABEL_LEN,
};

const THREAD_A: &str = "11111111-1111-4111-8111-11111111111a";
const THREAD_B: &str = "22222222-2222-4222-8222-22222222222b";
const THREAD_C: &str = "33333333-3333-4333-8333-33333333333c";

#[test]
fn none_to_create_and_repeat_preserves_binding_cursors_and_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open core");

    // 1. None -> create returns created=true with initialized cursors and Idle wakeup.
    let first = core
        .get_or_create_binding(THREAD_A, "  primary operator session  ")
        .expect("initial get_or_create_binding");
    assert!(first.created);
    assert!(first.binding.binding_id.starts_with("bnd_"));
    assert_eq!(first.binding.thread_uuid, THREAD_A);
    assert_eq!(first.binding.label, "primary operator session");
    assert_eq!(first.binding.delivered_seq, 0);
    assert_eq!(first.binding.acked_seq, 0);

    let wakeup = core
        .get_wakeup(&first.binding.binding_id)
        .expect("initial wakeup");
    assert_eq!(wakeup.status, WakeupStatus::Idle);
    assert_eq!(wakeup.thread_uuid, THREAD_A);

    // 2. Advance state on the binding: start a demo run, emit events, pull and ack.
    let run = core
        .execute(
            &first.binding.binding_id,
            Command::StartDemo {
                command_key: "demo-start-1".to_string(),
                instruction: "verify cursor preservation".to_string(),
                step_delay_ms: Some(50),
                now_ms: 1_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("demo run");
    core.tick(1_100).expect("tick demo to waiting_input");

    let batch = core
        .execute(
            &first.binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 1_110,
            },
        )
        .expect("pull events")
        .into_event_batch()
        .expect("event batch");
    assert!(batch.delivered_seq >= 2);

    let ack = core
        .execute(
            &first.binding.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-batch-1".to_string(),
                batch_id: batch.batch_id.clone(),
                now_ms: 1_120,
            },
        )
        .expect("ack events")
        .into_ack_result()
        .expect("ack result");
    assert_eq!(ack.acked_seq, batch.to_seq);

    // Emit another event so delivered_seq / pending events remain non-trivial.
    let waiting_run = core
        .get_run(&first.binding.binding_id, &run.run_id)
        .expect("load waiting run");
    let question_id = waiting_run
        .active_question
        .as_ref()
        .expect("active question")
        .question_id
        .clone();
    core.execute(
        &first.binding.binding_id,
        Command::Answer {
            command_key: "answer-1".to_string(),
            run_id: run.run_id.clone(),
            question_id,
            expected_revision: waiting_run.revision,
            answer: "proceed".to_string(),
            now_ms: 1_130,
        },
    )
    .expect("answer question");
    core.tick(1_200).expect("tick demo to completion");

    let pull_unacked = core
        .execute(
            &first.binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 1,
                now_ms: 1_210,
            },
        )
        .expect("pull partial")
        .into_event_batch()
        .expect("partial batch");
    assert!(pull_unacked.delivered_seq > ack.acked_seq);

    let expected_binding = core
        .get_binding(&first.binding.binding_id)
        .expect("load binding before repeat");
    let expected_wakeup = core
        .get_wakeup(&first.binding.binding_id)
        .expect("load wakeup before repeat");
    assert_eq!(expected_wakeup.status, WakeupStatus::Pending);

    // 3. Repeat get_or_create_binding (and ensure_binding) with a different valid label:
    //    must return created=false and preserve binding_id, original label, cursors, and wakeup.
    let second = core
        .get_or_create_binding(THREAD_A, "reconnect label should not overwrite")
        .expect("repeat get_or_create_binding");
    assert!(!second.created);
    assert_eq!(second.binding, expected_binding);
    assert_eq!(second.binding.label, "primary operator session");
    assert_eq!(second.binding.delivered_seq, pull_unacked.delivered_seq);
    assert_eq!(second.binding.acked_seq, ack.acked_seq);

    // 4. Reopen Core from disk and verify idempotent reconnect across restart.
    drop(core);
    let mut reopened = Core::open(&db_path).expect("reopen core");
    let third = reopened
        .ensure_binding(THREAD_A, "post-restart reconnect")
        .expect("post-restart ensure_binding");
    assert!(!third.created);
    assert_eq!(third.binding, expected_binding);
    assert_eq!(
        reopened
            .get_wakeup(&first.binding.binding_id)
            .expect("wakeup after reopen"),
        expected_wakeup
    );
    assert_eq!(
        reopened.list_bindings().expect("list bindings"),
        vec![expected_binding]
    );
}

#[test]
fn concurrent_get_or_create_calls_create_only_one_binding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");

    // Initialize database schema first so concurrent connections race directly on get_or_create_binding.
    let initial = Core::open(&db_path).expect("init core");
    drop(initial);

    let worker_count = 8usize;
    let barrier = Arc::new(Barrier::new(worker_count));
    let mut handles = Vec::with_capacity(worker_count);

    for idx in 0..worker_count {
        // Open before spawning: a failed initialization must not strand peers at the barrier.
        let mut conn_core = Core::open(&db_path).expect("open worker core");
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> GetOrCreateBindingOutcome {
            barrier.wait();
            conn_core
                .get_or_create_binding(THREAD_A, &format!("concurrent-caller-{idx}"))
                .expect("concurrent get_or_create_binding must succeed")
        }));
    }

    let outcomes: Vec<GetOrCreateBindingOutcome> = handles
        .into_iter()
        .map(|h| h.join().expect("join worker thread"))
        .collect();

    let created_outcomes: Vec<&GetOrCreateBindingOutcome> =
        outcomes.iter().filter(|o| o.created).collect();
    assert_eq!(
        created_outcomes.len(),
        1,
        "exactly one concurrent caller must create the binding: {outcomes:?}"
    );

    let winner = &created_outcomes[0].binding;
    for outcome in &outcomes {
        assert_eq!(&outcome.binding, winner);
    }

    let verify_core = Core::open(&db_path).expect("open verify core");
    let all_bindings = verify_core.list_bindings().expect("list bindings");
    assert_eq!(all_bindings, vec![winner.clone()]);
}

#[test]
fn multiple_pre_existing_bindings_return_ambiguity_error_without_mutations() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open core");

    // Pre-existing duplicate bindings created via legacy/explicit create_binding.
    let b1 = core
        .create_binding(THREAD_A, "duplicate-first")
        .expect("b1");
    let b2 = core
        .create_binding(THREAD_A, "duplicate-second")
        .expect("b2");
    let other = core
        .create_binding(THREAD_B, "other-thread")
        .expect("other");

    // Advance state on b1 and other to verify nothing gets reset or mutated on ambiguity error.
    core.execute(
        &b1.binding_id,
        Command::StartDemo {
            command_key: "b1-demo".to_string(),
            instruction: "b1 work".to_string(),
            step_delay_ms: Some(50),
            now_ms: 2_000,
        },
    )
    .expect("start b1 demo");
    core.execute(
        &other.binding_id,
        Command::StartDemo {
            command_key: "other-demo".to_string(),
            instruction: "other work".to_string(),
            step_delay_ms: Some(50),
            now_ms: 2_000,
        },
    )
    .expect("start other demo");
    core.tick(2_100).expect("tick both");

    let b1_batch = core
        .execute(
            &b1.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 2_110,
            },
        )
        .expect("pull b1")
        .into_event_batch()
        .expect("b1 batch");
    core.execute(
        &b1.binding_id,
        Command::AcknowledgeEvents {
            command_key: "ack-b1".to_string(),
            batch_id: b1_batch.batch_id,
            now_ms: 2_120,
        },
    )
    .expect("ack b1");

    let before_bindings = core.list_bindings().expect("bindings before ambiguity");
    let before_b1_wakeup = core.get_wakeup(&b1.binding_id).expect("b1 wakeup");
    let before_b2_wakeup = core.get_wakeup(&b2.binding_id).expect("b2 wakeup");
    let before_other_wakeup = core.get_wakeup(&other.binding_id).expect("other wakeup");

    let err = core
        .get_or_create_binding(THREAD_A, "duplicate-first")
        .expect_err("multiple pre-existing bindings must fail closed");
    match err {
        CoreError::AmbiguousThreadBinding {
            thread_uuid,
            match_count,
            binding_ids,
        } => {
            assert_eq!(thread_uuid, THREAD_A);
            assert_eq!(match_count, 2);
            assert_eq!(
                binding_ids,
                vec![b1.binding_id.clone(), b2.binding_id.clone()]
            );
        }
        other_err => panic!("expected AmbiguousThreadBinding, got {other_err:?}"),
    }

    // Verify no bindings, cursors, labels, or wakeups were altered.
    assert_eq!(
        core.list_bindings().expect("bindings after ambiguity"),
        before_bindings
    );
    assert_eq!(
        core.get_wakeup(&b1.binding_id).expect("b1 wakeup after"),
        before_b1_wakeup
    );
    assert_eq!(
        core.get_wakeup(&b2.binding_id).expect("b2 wakeup after"),
        before_b2_wakeup
    );
    assert_eq!(
        core.get_wakeup(&other.binding_id)
            .expect("other wakeup after"),
        before_other_wakeup
    );

    // Other threads can still use get_or_create_binding normally.
    let other_outcome = core
        .get_or_create_binding(THREAD_B, "other-thread-reconnect")
        .expect("THREAD_B has a single binding and must succeed");
    assert!(!other_outcome.created);
    assert_eq!(other_outcome.binding.binding_id, other.binding_id);

    let new_c = core
        .get_or_create_binding(THREAD_C, "fresh-thread-c")
        .expect("THREAD_C has no bindings and must be created");
    assert!(new_c.created);
    assert_eq!(new_c.binding.thread_uuid, THREAD_C);
}

#[test]
fn validates_all_arguments_even_when_matching_binding_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open core");

    let overlong_label = "x".repeat(MAX_LABEL_LEN + 1);
    let uppercase_uuid = "11111111-1111-4111-8111-11111111111A";
    let simple_uuid = "11111111111141118111111111111111";

    // 1. Validation failures when no binding exists must not insert anything.
    for bad_uuid in [
        "",
        "   ",
        "not-a-uuid",
        "00000000-0000-0000-0000-000000000000",
        " 11111111-1111-4111-8111-11111111111a",
        "11111111-1111-4111-8111-11111111111a ",
        uppercase_uuid,
        simple_uuid,
    ] {
        let err = core
            .get_or_create_binding(bad_uuid, "valid-label")
            .expect_err("invalid thread_uuid must be rejected");
        match err {
            CoreError::Validation { field, .. } => assert_eq!(field, "thread_uuid"),
            other => panic!("expected Validation on thread_uuid for `{bad_uuid}`, got {other:?}"),
        }
    }

    for bad_label in [
        "",
        "   ",
        "bad\nlabel",
        "bad\0label",
        overlong_label.as_str(),
    ] {
        let err = core
            .get_or_create_binding(THREAD_A, bad_label)
            .expect_err("invalid label must be rejected");
        match err {
            CoreError::Validation { field, .. } => assert_eq!(field, "label"),
            other => panic!("expected Validation on label, got {other:?}"),
        }
    }

    assert!(core
        .list_bindings()
        .expect("no bindings created")
        .is_empty());

    // 2. Create a valid binding for THREAD_A, then verify invalid label or non-canonical UUID
    //    is STILL rejected on the existing-match path.
    let created = core
        .get_or_create_binding(THREAD_A, "valid-initial-label")
        .expect("create valid binding");
    assert!(created.created);

    for bad_label in [
        "",
        "   ",
        "bad\nlabel",
        "bad\t\rlabel",
        overlong_label.as_str(),
    ] {
        let err = core
            .get_or_create_binding(THREAD_A, bad_label)
            .expect_err("invalid label must be rejected even when binding exists");
        match err {
            CoreError::Validation { field, .. } => assert_eq!(field, "label"),
            other => panic!("expected Validation on label on existing path, got {other:?}"),
        }
    }

    for non_canonical in [uppercase_uuid, simple_uuid] {
        let err = core
            .get_or_create_binding(non_canonical, "valid-label")
            .expect_err("non-canonical UUID must be rejected even when canonical match exists");
        match err {
            CoreError::Validation { field, .. } => assert_eq!(field, "thread_uuid"),
            other => panic!("expected Validation on thread_uuid, got {other:?}"),
        }
    }

    // Existing binding remains intact.
    assert_eq!(
        core.list_bindings().expect("list bindings"),
        vec![created.binding]
    );
}
