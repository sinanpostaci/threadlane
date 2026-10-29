use tempfile::TempDir;
use threadlane_core::{
    AckErrorReason, Command, Core, CoreError, EventKind, QuestionStatus, RunStatus, WakeupOutcome,
    WakeupStatus, FIXED_WAKEUP_MESSAGE, MAX_ANSWER_LEN, MAX_CANCEL_REASON_LEN, MAX_COMMAND_KEY_LEN,
    MAX_DEMO_STEP_DELAY_MS, MAX_EVENT_BATCH_SIZE, MAX_INSTRUCTION_LEN, MAX_LABEL_LEN,
    MAX_WAKEUP_ERROR_LEN,
};

const UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

#[test]
fn full_demo_loop_with_events_and_wakeups() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding("11111111-2222-4333-8444-555555555555", "Primary Demo Chat")
        .expect("create binding");
    assert!(binding.binding_id.starts_with("bnd_"));
    assert_eq!(binding.thread_uuid, UUID_A);
    assert_eq!(binding.label, "Primary Demo Chat");
    assert_eq!(binding.delivered_seq, 0);
    assert_eq!(binding.acked_seq, 0);

    // 1. Start -> Running
    let started = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "start-1".to_string(),
                instruction: "Implement bounded conversation loop".to_string(),
                step_delay_ms: Some(500),
                now_ms: 1_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("run output");

    assert_eq!(started.status, RunStatus::Running);
    assert_eq!(started.revision, 1);
    assert_eq!(started.step_seq, 1);
    assert_eq!(started.next_step_at_ms, Some(1_500));
    assert_eq!(started.active_question_id, None);
    assert_eq!(started.revision_history.len(), 1);
    assert_eq!(
        started.revision_history[0].instruction,
        "Implement bounded conversation loop"
    );

    // Start alone is not an important wake-up event.
    let wakeup = core.get_wakeup(&binding.binding_id).expect("get wakeup");
    assert_eq!(wakeup.status, WakeupStatus::Idle);
    assert_eq!(core.claim_wakeup(1_100).expect("claim wakeup"), None);

    // Tick before due time does not advance.
    let early_tick = core.tick(1_499).expect("early tick");
    assert!(early_tick.advanced_runs.is_empty());
    assert_eq!(early_tick.emitted_events, 0);

    // 2. Tick at due time: Running -> WaitingInput (emits RunProgress + QuestionAsked).
    let tick_to_question = core.tick(1_500).expect("tick to question");
    assert_eq!(tick_to_question.advanced_runs, vec![started.run_id.clone()]);
    assert_eq!(tick_to_question.emitted_events, 2);

    let waiting = core
        .get_run(&binding.binding_id, &started.run_id)
        .expect("get waiting run");
    assert_eq!(waiting.status, RunStatus::WaitingInput);
    assert_eq!(waiting.revision, 1);
    assert_eq!(waiting.step_seq, 3);
    assert_eq!(waiting.next_step_at_ms, None);
    assert_eq!(waiting.questions.len(), 1);
    let question_id = waiting
        .active_question_id
        .clone()
        .expect("active question id");
    assert_eq!(waiting.questions[0].question_id, question_id);
    assert_eq!(waiting.questions[0].status, QuestionStatus::Pending);
    assert_eq!(waiting.questions[0].revision, 1);

    // QuestionAsked is important -> creates Pending wake-up notice.
    let wakeup = core.get_wakeup(&binding.binding_id).expect("get wakeup");
    assert_eq!(wakeup.status, WakeupStatus::Pending);
    assert_eq!(wakeup.pending_since_seq, Some(3));
    assert_eq!(wakeup.latest_important_seq, 3);
    assert_eq!(wakeup.coalesced_count, 1);

    // Claim wake-up persists InFlight intent with fixed message containing no worker text or label.
    let claim = core
        .claim_wakeup(1_510)
        .expect("claim wakeup")
        .expect("wakeup claim present");
    assert_eq!(claim.binding_id, binding.binding_id);
    assert_eq!(claim.thread_uuid, UUID_A);
    assert_eq!(claim.message, FIXED_WAKEUP_MESSAGE);
    assert!(!claim.message.contains("Primary Demo Chat"));
    assert!(!claim.message.contains("Implement bounded"));
    assert_eq!(claim.covered_up_to_seq, 3);

    // Second claim while InFlight returns None.
    assert_eq!(core.claim_wakeup(1_515).expect("second claim"), None);

    // Record Submitted outcome.
    let submitted_notice = core
        .record_wakeup_result(&claim.claim_id, WakeupOutcome::Submitted, 1_520)
        .expect("record submitted");
    assert_eq!(submitted_notice.status, WakeupStatus::Submitted);
    assert_eq!(core.claim_wakeup(1_525).expect("claim after submit"), None);

    // Pull events (1..=3): RunStarted, RunProgress, QuestionAsked.
    let batch_1 = core
        .execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 1_530,
            },
        )
        .expect("pull events")
        .into_event_batch()
        .expect("event batch");

    assert_eq!(batch_1.events.len(), 3);
    assert_eq!(batch_1.from_seq, 1);
    assert_eq!(batch_1.to_seq, 3);
    assert_eq!(batch_1.delivered_seq, 3);
    assert_eq!(batch_1.acked_seq, 0);
    assert!(!batch_1.has_more);
    assert_eq!(batch_1.events[0].kind, EventKind::RunStarted);
    assert_eq!(batch_1.events[1].kind, EventKind::RunProgress);
    assert_eq!(batch_1.events[2].kind, EventKind::QuestionAsked);
    assert!(batch_1.events.iter().all(|e| e.payload.untrusted_data));

    // Reading events is NOT acknowledgement: wake-up remains Submitted until acknowledged.
    let wakeup_after_read = core.get_wakeup(&binding.binding_id).expect("wakeup");
    assert_eq!(wakeup_after_read.status, WakeupStatus::Submitted);

    // Acknowledge batch_1 -> releases notification coverage back to Idle.
    let ack_1 = core
        .execute(
            &binding.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-1".to_string(),
                batch_id: batch_1.batch_id.clone(),
                now_ms: 1_540,
            },
        )
        .expect("ack batch 1")
        .into_ack_result()
        .expect("ack result");

    assert_eq!(ack_1.acked_seq, 3);
    assert!(ack_1.advanced);
    assert_eq!(ack_1.wakeup_status, WakeupStatus::Idle);

    // 3. Answer: WaitingInput -> Completing
    let completing = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-1".to_string(),
                run_id: started.run_id.clone(),
                question_id: question_id.clone(),
                expected_revision: 1,
                answer: "Approved for execution".to_string(),
                now_ms: 2_000,
            },
        )
        .expect("answer question")
        .into_run()
        .expect("completing run");

    assert_eq!(completing.status, RunStatus::Completing);
    assert_eq!(completing.active_question_id, None);
    assert_eq!(completing.next_step_at_ms, Some(2_500));
    assert_eq!(completing.questions[0].status, QuestionStatus::Answered);
    assert_eq!(
        completing.questions[0].answer.as_deref(),
        Some("Approved for execution")
    );

    // 4. Tick at completion time: Completing -> Completed
    assert!(core
        .tick(2_499)
        .expect("pre-complete tick")
        .advanced_runs
        .is_empty());
    let tick_complete = core.tick(2_500).expect("complete tick");
    assert_eq!(tick_complete.advanced_runs, vec![started.run_id.clone()]);
    assert_eq!(tick_complete.emitted_events, 1);

    let completed = core
        .get_run(&binding.binding_id, &started.run_id)
        .expect("completed run");
    assert_eq!(completed.status, RunStatus::Completed);
    assert_eq!(completed.completed_at_ms, Some(2_500));
    assert_eq!(completed.next_step_at_ms, None);

    // RunCompleted is important -> creates new Pending wake-up notice at seq 5.
    let wakeup_complete = core.get_wakeup(&binding.binding_id).expect("wakeup");
    assert_eq!(wakeup_complete.status, WakeupStatus::Pending);
    assert_eq!(wakeup_complete.pending_since_seq, Some(5));
    assert_eq!(wakeup_complete.latest_important_seq, 5);

    let claim_2 = core
        .claim_wakeup(2_510)
        .expect("claim completion wakeup")
        .expect("claim 2");
    assert_eq!(claim_2.covered_up_to_seq, 5);
    core.record_wakeup_result(&claim_2.claim_id, WakeupOutcome::Submitted, 2_520)
        .expect("submit claim 2");

    let batch_2 = core
        .execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 2_530,
            },
        )
        .expect("pull completion events")
        .into_event_batch()
        .expect("batch 2");

    assert_eq!(batch_2.from_seq, 4);
    assert_eq!(batch_2.to_seq, 5);
    assert_eq!(batch_2.events.len(), 2);
    assert_eq!(batch_2.events[0].kind, EventKind::QuestionAnswered);
    assert_eq!(batch_2.events[1].kind, EventKind::RunCompleted);

    let ack_2 = core
        .execute(
            &binding.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-2".to_string(),
                batch_id: batch_2.batch_id,
                now_ms: 2_540,
            },
        )
        .expect("ack 2")
        .into_ack_result()
        .expect("ack result 2");
    assert_eq!(ack_2.acked_seq, 5);
    assert_eq!(ack_2.wakeup_status, WakeupStatus::Idle);
}

#[test]
fn duplicate_retry_and_reused_key_conflict_persist_across_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");

    let (binding_id, original_run) = {
        let mut core = Core::open(&db_path).expect("open");
        let binding = core
            .create_binding(UUID_A, "Idempotency Chat")
            .expect("binding");

        let run = core
            .execute(
                &binding.binding_id,
                Command::StartDemo {
                    command_key: "dup-key-1".to_string(),
                    instruction: "Initial task".to_string(),
                    step_delay_ms: Some(300),
                    now_ms: 1_000,
                },
            )
            .expect("first start")
            .into_run()
            .expect("run");

        // Identical retry with advancing clock returns original result without duplicating state.
        let retry_same_session = core
            .execute(
                &binding.binding_id,
                Command::StartDemo {
                    command_key: "dup-key-1".to_string(),
                    instruction: "Initial task".to_string(),
                    step_delay_ms: Some(300),
                    now_ms: 1_200,
                },
            )
            .expect("retry start")
            .into_run()
            .expect("run");

        assert_eq!(run, retry_same_session);
        (binding.binding_id, run)
    };

    // Reopen database and verify retry still returns original result and conflicts are rejected.
    let mut core = Core::open(&db_path).expect("reopen");
    let retry_after_reopen = core
        .execute(
            &binding_id,
            Command::StartDemo {
                command_key: "dup-key-1".to_string(),
                instruction: "Initial task".to_string(),
                step_delay_ms: Some(300),
                now_ms: 2_000,
            },
        )
        .expect("retry after reopen")
        .into_run()
        .expect("run");
    assert_eq!(original_run, retry_after_reopen);

    // Verify only 1 run and 1 event were ever created.
    let runs = core
        .execute(&binding_id, Command::ListRuns)
        .expect("list runs")
        .into_run_list()
        .expect("run list");
    assert_eq!(runs.len(), 1);

    let batch = core
        .execute(
            &binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 2_010,
            },
        )
        .expect("pull")
        .into_event_batch()
        .expect("batch");
    assert_eq!(batch.events.len(), 1);

    // Reusing the same command_key with changed instruction is rejected.
    let err_changed_instruction = core
        .execute(
            &binding_id,
            Command::StartDemo {
                command_key: "dup-key-1".to_string(),
                instruction: "Different instruction".to_string(),
                step_delay_ms: Some(300),
                now_ms: 2_020,
            },
        )
        .expect_err("conflict on changed instruction");
    assert!(matches!(
        err_changed_instruction,
        CoreError::IdempotencyConflict { .. }
    ));

    // Reusing the same command_key with a different operation is also rejected.
    let err_different_op = core
        .execute(
            &binding_id,
            Command::Cancel {
                command_key: "dup-key-1".to_string(),
                run_id: original_run.run_id.clone(),
                reason: Some("abort".to_string()),
                now_ms: 2_030,
            },
        )
        .expect_err("conflict on different operation");
    assert!(matches!(
        err_different_op,
        CoreError::IdempotencyConflict { .. }
    ));
}

#[test]
fn persistence_after_reopen_preserves_step_progression_without_duplication() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");

    let (binding_id, run_id) = {
        let mut core = Core::open(&db_path).expect("open 1");
        let binding = core.create_binding(UUID_A, "Reopen Test").expect("binding");
        let run = core
            .execute(
                &binding.binding_id,
                Command::StartDemo {
                    command_key: "start-reopen".to_string(),
                    instruction: "Survive restarts".to_string(),
                    step_delay_ms: Some(400),
                    now_ms: 1_000,
                },
            )
            .expect("start")
            .into_run()
            .expect("run");
        (binding.binding_id, run.run_id)
    };

    // Reopen before first tick and advance to WaitingInput.
    let question_id = {
        let mut core = Core::open(&db_path).expect("open 2");
        let report = core.tick(1_400).expect("tick 1");
        assert_eq!(report.advanced_runs, vec![run_id.clone()]);
        let run = core.get_run(&binding_id, &run_id).expect("get run");
        assert_eq!(run.status, RunStatus::WaitingInput);
        run.active_question_id.expect("question id")
    };

    // Reopen while in WaitingInput; repeated tick must not emit another question.
    {
        let mut core = Core::open(&db_path).expect("open 3");
        let repeat_tick = core.tick(2_000).expect("repeat tick");
        assert!(repeat_tick.advanced_runs.is_empty());

        let answered = core
            .execute(
                &binding_id,
                Command::Answer {
                    command_key: "ans-reopen".to_string(),
                    run_id: run_id.clone(),
                    question_id,
                    expected_revision: 1,
                    answer: "Continue".to_string(),
                    now_ms: 2_000,
                },
            )
            .expect("answer")
            .into_run()
            .expect("run");
        assert_eq!(answered.status, RunStatus::Completing);
        assert_eq!(answered.next_step_at_ms, Some(2_400));
    }

    // Reopen while in Completing and finish via tick.
    {
        let mut core = Core::open(&db_path).expect("open 4");
        let report = core.tick(2_400).expect("tick complete");
        assert_eq!(report.advanced_runs, vec![run_id.clone()]);
        assert_eq!(report.emitted_events, 1);

        // Subsequent tick does not repeat completion.
        let after_complete = core.tick(3_000).expect("tick after complete");
        assert!(after_complete.advanced_runs.is_empty());

        let final_run = core.get_run(&binding_id, &run_id).expect("final run");
        assert_eq!(final_run.status, RunStatus::Completed);
        assert_eq!(final_run.completed_at_ms, Some(2_400));
    }
}

#[test]
fn two_binding_isolation_across_runs_questions_events_batches_and_wakeups() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding_a = core.create_binding(UUID_A, "Chat A").expect("binding a");
    let binding_b = core.create_binding(UUID_B, "Chat B").expect("binding b");

    // Reusing the same command_key across different bindings is isolated and allowed.
    let run_a = core
        .execute(
            &binding_a.binding_id,
            Command::StartDemo {
                command_key: "shared-key".to_string(),
                instruction: "Task for A".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start a")
        .into_run()
        .expect("run a");

    let run_b = core
        .execute(
            &binding_b.binding_id,
            Command::StartDemo {
                command_key: "shared-key".to_string(),
                instruction: "Task for B".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start b")
        .into_run()
        .expect("run b");

    core.tick(1_100).expect("tick both");

    let run_a = core
        .get_run(&binding_a.binding_id, &run_a.run_id)
        .expect("get a");
    let run_b = core
        .get_run(&binding_b.binding_id, &run_b.run_id)
        .expect("get b");
    let q_a = run_a.active_question_id.clone().expect("q_a");
    let q_b = run_b.active_question_id.clone().expect("q_b");

    // Cross-binding GetRun is rejected.
    assert!(matches!(
        core.get_run(&binding_b.binding_id, &run_a.run_id),
        Err(CoreError::RunNotFound { .. })
    ));

    // Cross-binding Answer (using binding_b with run_a, or binding_b with run_b + q_a) is rejected.
    assert!(matches!(
        core.execute(
            &binding_b.binding_id,
            Command::Answer {
                command_key: "cross-ans-1".to_string(),
                run_id: run_a.run_id.clone(),
                question_id: q_a.clone(),
                expected_revision: 1,
                answer: "intrude".to_string(),
                now_ms: 1_200,
            }
        ),
        Err(CoreError::RunNotFound { .. })
    ));
    assert!(matches!(
        core.execute(
            &binding_b.binding_id,
            Command::Answer {
                command_key: "cross-ans-2".to_string(),
                run_id: run_b.run_id.clone(),
                question_id: q_a,
                expected_revision: 1,
                answer: "intrude question".to_string(),
                now_ms: 1_200,
            }
        ),
        Err(CoreError::QuestionNotFound { .. })
    ));

    // Cross-binding Steer and Cancel are rejected.
    assert!(matches!(
        core.execute(
            &binding_b.binding_id,
            Command::Steer {
                command_key: "cross-steer".to_string(),
                run_id: run_a.run_id.clone(),
                expected_revision: 1,
                instruction: "hijack".to_string(),
                now_ms: 1_200,
            }
        ),
        Err(CoreError::RunNotFound { .. })
    ));
    assert!(matches!(
        core.execute(
            &binding_b.binding_id,
            Command::Cancel {
                command_key: "cross-cancel".to_string(),
                run_id: run_a.run_id.clone(),
                reason: None,
                now_ms: 1_200,
            }
        ),
        Err(CoreError::RunNotFound { .. })
    ));

    // Events and batches are strictly isolated per binding.
    let batch_a = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 1_210,
            },
        )
        .expect("pull a")
        .into_event_batch()
        .expect("batch a");
    let batch_b = core
        .execute(
            &binding_b.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 1_210,
            },
        )
        .expect("pull b")
        .into_event_batch()
        .expect("batch b");

    assert_eq!(batch_a.events.len(), 3);
    assert!(batch_a.events.iter().all(|e| e.run_id == run_a.run_id));
    assert_eq!(batch_b.events.len(), 3);
    assert!(batch_b.events.iter().all(|e| e.run_id == run_b.run_id));

    // Cross-binding batch acknowledgement is rejected.
    let cross_ack_err = core
        .execute(
            &binding_b.binding_id,
            Command::AcknowledgeEvents {
                command_key: "cross-ack".to_string(),
                batch_id: batch_a.batch_id.clone(),
                now_ms: 1_220,
            },
        )
        .expect_err("cross binding ack must fail");
    assert!(matches!(
        cross_ack_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::CrossBindingBatch,
            ..
        }
    ));

    // Binding B can still answer its own question `q_b` unaffected.
    let answered_b = core
        .execute(
            &binding_b.binding_id,
            Command::Answer {
                command_key: "valid-ans-b".to_string(),
                run_id: run_b.run_id,
                question_id: q_b,
                expected_revision: 1,
                answer: "Valid B answer".to_string(),
                now_ms: 1_230,
            },
        )
        .expect("answer b")
        .into_run()
        .expect("run b");
    assert_eq!(answered_b.status, RunStatus::Completing);
}

#[test]
fn steer_increments_revision_supersedes_question_and_rejects_stale_revisions_and_questions() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");
    let binding = core.create_binding(UUID_A, "Steer Chat").expect("binding");

    let run = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "start-steer".to_string(),
                instruction: "Plan v1".to_string(),
                step_delay_ms: Some(200),
                now_ms: 1_000,
            },
        )
        .expect("start")
        .into_run()
        .expect("run");

    core.tick(1_200).expect("tick to v1 question");
    let run_v1 = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("get v1");
    let q1_id = run_v1.active_question_id.clone().expect("q1");

    // Steer with wrong expected_revision is rejected.
    let stale_steer_err = core
        .execute(
            &binding.binding_id,
            Command::Steer {
                command_key: "bad-steer".to_string(),
                run_id: run.run_id.clone(),
                expected_revision: 2,
                instruction: "Premature v2".to_string(),
                now_ms: 1_250,
            },
        )
        .expect_err("stale steer must fail");
    assert!(matches!(
        stale_steer_err,
        CoreError::StaleRevision {
            expected_revision: 2,
            actual_revision: 1,
            ..
        }
    ));

    // Valid Steer at expected_revision = 1 -> advances to revision 2 and supersedes q1.
    let steered_v2 = core
        .execute(
            &binding.binding_id,
            Command::Steer {
                command_key: "steer-to-2".to_string(),
                run_id: run.run_id.clone(),
                expected_revision: 1,
                instruction: "Plan v2".to_string(),
                now_ms: 1_300,
            },
        )
        .expect("steer to v2")
        .into_run()
        .expect("run v2");

    assert_eq!(steered_v2.revision, 2);
    assert_eq!(steered_v2.status, RunStatus::Running);
    assert_eq!(steered_v2.instruction, "Plan v2");
    assert_eq!(steered_v2.active_question_id, None);
    assert_eq!(steered_v2.next_step_at_ms, Some(1_500));
    assert_eq!(steered_v2.revision_history.len(), 2);
    assert_eq!(steered_v2.revision_history[0].revision, 1);
    assert_eq!(steered_v2.revision_history[0].superseded_at_ms, Some(1_300));
    assert_eq!(steered_v2.revision_history[1].revision, 2);
    assert_eq!(steered_v2.revision_history[1].instruction, "Plan v2");
    assert_eq!(steered_v2.questions.len(), 1);
    assert_eq!(steered_v2.questions[0].status, QuestionStatus::Superseded);
    assert_eq!(steered_v2.questions[0].superseded_by_revision, Some(2));

    // Answering superseded q1 with stale revision 1 fails with StaleRevision.
    let ans_stale_rev = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-q1-rev1".to_string(),
                run_id: run.run_id.clone(),
                question_id: q1_id.clone(),
                expected_revision: 1,
                answer: "Too late rev 1".to_string(),
                now_ms: 1_350,
            },
        )
        .expect_err("stale rev answer");
    assert!(matches!(
        ans_stale_rev,
        CoreError::StaleRevision {
            expected_revision: 1,
            actual_revision: 2,
            ..
        }
    ));

    // Answering superseded q1 even with current expected_revision = 2 fails with StaleQuestion.
    let ans_stale_q = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-q1-rev2".to_string(),
                run_id: run.run_id.clone(),
                question_id: q1_id.clone(),
                expected_revision: 2,
                answer: "Trying old question with new rev".to_string(),
                now_ms: 1_360,
            },
        )
        .expect_err("stale question answer");
    assert!(matches!(
        ans_stale_q,
        CoreError::StaleQuestion {
            question_status: QuestionStatus::Superseded,
            question_revision: 1,
            current_revision: 2,
            ..
        }
    ));

    // Advance v2 to its own question q2.
    core.tick(1_500).expect("tick v2 question");
    let waiting_v2 = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("get waiting v2");
    let q2_id = waiting_v2.active_question_id.clone().expect("q2");
    assert_ne!(q1_id, q2_id);
    assert_eq!(waiting_v2.questions.len(), 2);

    // Answering q2 with expected_revision = 2 transitions to Completing.
    let completing_v2 = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-q2-rev2".to_string(),
                run_id: run.run_id.clone(),
                question_id: q2_id,
                expected_revision: 2,
                answer: "Approve v2".to_string(),
                now_ms: 1_600,
            },
        )
        .expect("answer q2")
        .into_run()
        .expect("completing v2");
    assert_eq!(completing_v2.status, RunStatus::Completing);
    assert_eq!(completing_v2.next_step_at_ms, Some(1_800));

    // Steer while in Completing supersedes the completing step so revision 2 cannot complete!
    let steered_v3 = core
        .execute(
            &binding.binding_id,
            Command::Steer {
                command_key: "steer-to-3".to_string(),
                run_id: run.run_id.clone(),
                expected_revision: 2,
                instruction: "Plan v3 before completion".to_string(),
                now_ms: 1_700,
            },
        )
        .expect("steer to v3")
        .into_run()
        .expect("run v3");
    assert_eq!(steered_v3.revision, 3);
    assert_eq!(steered_v3.status, RunStatus::Running);
    assert_eq!(steered_v3.next_step_at_ms, Some(1_900));

    // Tick at 1_800 (the old v2 completion time) must NOT complete the run.
    assert!(core
        .tick(1_800)
        .expect("tick at old v2 time")
        .advanced_runs
        .is_empty());
    let still_running = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("still running");
    assert_eq!(still_running.status, RunStatus::Running);
}

#[test]
fn cancel_is_terminal_and_prevents_later_completion_or_mutations() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");
    let binding = core.create_binding(UUID_A, "Cancel Chat").expect("binding");

    let run = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "start-cancel-test".to_string(),
                instruction: "Will be cancelled during completing".to_string(),
                step_delay_ms: Some(200),
                now_ms: 1_000,
            },
        )
        .expect("start")
        .into_run()
        .expect("run");

    core.tick(1_200).expect("tick to question");
    let waiting = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("waiting");
    let q_id = waiting.active_question_id.expect("q_id");

    let completing = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-before-cancel".to_string(),
                run_id: run.run_id.clone(),
                question_id: q_id.clone(),
                expected_revision: 1,
                answer: "Go ahead".to_string(),
                now_ms: 1_300,
            },
        )
        .expect("answer")
        .into_run()
        .expect("completing");
    assert_eq!(completing.status, RunStatus::Completing);
    assert_eq!(completing.next_step_at_ms, Some(1_500));

    // Cancel before 1_500.
    let cancelled = core
        .execute(
            &binding.binding_id,
            Command::Cancel {
                command_key: "cancel-1".to_string(),
                run_id: run.run_id.clone(),
                reason: Some("User stopped task".to_string()),
                now_ms: 1_400,
            },
        )
        .expect("cancel")
        .into_run()
        .expect("cancelled");

    assert_eq!(cancelled.status, RunStatus::Cancelled);
    assert_eq!(cancelled.cancelled_at_ms, Some(1_400));
    assert_eq!(
        cancelled.cancel_reason.as_deref(),
        Some("User stopped task")
    );
    assert_eq!(cancelled.next_step_at_ms, None);

    // Tick at and after 1_500 must NEVER transition a cancelled run to Completed.
    let tick_report = core.tick(1_500).expect("tick at 1500");
    assert!(tick_report.advanced_runs.is_empty());
    assert_eq!(tick_report.emitted_events, 0);

    let after_tick = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("after tick");
    assert_eq!(after_tick.status, RunStatus::Cancelled);
    assert_eq!(after_tick.completed_at_ms, None);

    // Steer, Answer, and a new Cancel on the terminal run are all rejected.
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::Steer {
                command_key: "steer-after-cancel".to_string(),
                run_id: run.run_id.clone(),
                expected_revision: 1,
                instruction: "Revive".to_string(),
                now_ms: 1_600,
            }
        ),
        Err(CoreError::InvalidRunState {
            status: RunStatus::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "ans-after-cancel".to_string(),
                run_id: run.run_id.clone(),
                question_id: q_id,
                expected_revision: 1,
                answer: "Late".to_string(),
                now_ms: 1_600,
            }
        ),
        Err(CoreError::InvalidRunState {
            status: RunStatus::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::Cancel {
                command_key: "cancel-2".to_string(),
                run_id: run.run_id,
                reason: None,
                now_ms: 1_600,
            }
        ),
        Err(CoreError::InvalidRunState {
            status: RunStatus::Cancelled,
            ..
        })
    ));
}

#[test]
fn unseen_future_skipped_stale_and_cross_binding_acknowledgements() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding_a = core.create_binding(UUID_A, "Ack A").expect("binding a");
    let binding_b = core.create_binding(UUID_B, "Ack B").expect("binding b");

    // Emit 3 events on binding_a (RunStarted, RunProgress, QuestionAsked).
    core.execute(
        &binding_a.binding_id,
        Command::StartDemo {
            command_key: "start-ack".to_string(),
            instruction: "Generate events".to_string(),
            step_delay_ms: Some(100),
            now_ms: 1_000,
        },
    )
    .expect("start");
    core.tick(1_100).expect("tick");

    // 1. Pulling after an unseen sequence number (delivered_seq == 0) is rejected.
    let unseen_cursor_err = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: Some(1),
                limit: 1,
                now_ms: 1_110,
            },
        )
        .expect_err("unseen cursor must fail");
    assert!(matches!(
        unseen_cursor_err,
        CoreError::UnseenEventCursor {
            requested_after_seq: 1,
            delivered_seq: 0,
            ..
        }
    ));

    // 2. Acknowledging an unknown/future batch ID is rejected.
    let unknown_batch_err = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-unknown".to_string(),
                batch_id: "bat_future_nonexistent".to_string(),
                now_ms: 1_120,
            },
        )
        .expect_err("unknown batch must fail");
    assert!(matches!(
        unknown_batch_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::UnknownBatch,
            ..
        }
    ));

    // Pull batch 1 covering only seq 1..=1 (limit = 1).
    let batch_1 = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 1,
                now_ms: 1_130,
            },
        )
        .expect("pull 1")
        .into_event_batch()
        .expect("batch 1");
    assert_eq!(batch_1.from_seq, 1);
    assert_eq!(batch_1.to_seq, 1);
    assert_eq!(batch_1.delivered_seq, 1);
    assert!(batch_1.has_more);

    // Pull batch 2 covering seq 2..=3 (after_seq = Some(1), limit = 2).
    let batch_2 = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: Some(1),
                limit: 2,
                now_ms: 1_140,
            },
        )
        .expect("pull 2")
        .into_event_batch()
        .expect("batch 2");
    assert_eq!(batch_2.from_seq, 2);
    assert_eq!(batch_2.to_seq, 3);
    assert_eq!(batch_2.delivered_seq, 3);
    assert!(!batch_2.has_more);

    // 3. Acknowledging batch_2 (2..=3) before batch_1 (1..=1) skips unacknowledged event 1 -> rejected.
    let skip_err = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-skip".to_string(),
                batch_id: batch_2.batch_id.clone(),
                now_ms: 1_150,
            },
        )
        .expect_err("skipping event 1 must fail");
    assert!(matches!(
        skip_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::SkippedEvents {
                acked_seq: 0,
                batch_from_seq: 2
            },
            ..
        }
    ));

    // 4. Cross-binding acknowledgement of batch_1 on binding_b is rejected.
    let cross_err = core
        .execute(
            &binding_b.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-cross".to_string(),
                batch_id: batch_1.batch_id.clone(),
                now_ms: 1_160,
            },
        )
        .expect_err("cross binding ack must fail");
    assert!(matches!(
        cross_err,
        CoreError::InvalidAck {
            reason: AckErrorReason::CrossBindingBatch,
            ..
        }
    ));

    // 5. Acknowledge batch_1 (1..=1) then batch_2 (2..=3) in order -> succeeds.
    let ack_1 = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b1".to_string(),
                batch_id: batch_1.batch_id.clone(),
                now_ms: 1_170,
            },
        )
        .expect("ack b1")
        .into_ack_result()
        .expect("ack result 1");
    assert_eq!(ack_1.acked_seq, 1);
    assert!(ack_1.advanced);

    let ack_2 = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b2".to_string(),
                batch_id: batch_2.batch_id,
                now_ms: 1_180,
            },
        )
        .expect("ack b2")
        .into_ack_result()
        .expect("ack result 2");
    assert_eq!(ack_2.acked_seq, 3);
    assert!(ack_2.advanced);

    // 6. Stale/repeated acknowledgement of batch_1 (1..=1) with a new key is harmless and never moves acked_seq backwards.
    let stale_ack = core
        .execute(
            &binding_a.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b1-stale".to_string(),
                batch_id: batch_1.batch_id,
                now_ms: 1_190,
            },
        )
        .expect("stale ack is harmless")
        .into_ack_result()
        .expect("stale ack result");
    assert_eq!(stale_ack.acked_seq, 3);
    assert!(!stale_ack.advanced);
}

#[test]
fn limit_validation_rejects_malformed_and_oversized_inputs() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");

    // Invalid UUIDs
    assert!(matches!(
        core.create_binding("not-a-uuid", "Label"),
        Err(CoreError::Validation {
            field: "thread_uuid",
            ..
        })
    ));
    assert!(matches!(
        core.create_binding("00000000-0000-0000-0000-000000000000", "Label"),
        Err(CoreError::Validation {
            field: "thread_uuid",
            ..
        })
    ));

    // Invalid labels
    assert!(matches!(
        core.create_binding(UUID_A, "   "),
        Err(CoreError::Validation { field: "label", .. })
    ));
    assert!(matches!(
        core.create_binding(UUID_A, "bad\nlabel"),
        Err(CoreError::Validation { field: "label", .. })
    ));
    let long_label = "x".repeat(MAX_LABEL_LEN + 1);
    assert!(matches!(
        core.create_binding(UUID_A, &long_label),
        Err(CoreError::Validation { field: "label", .. })
    ));

    let binding = core.create_binding(UUID_A, "Valid Label").expect("binding");

    // Oversized command_key
    let long_key = "k".repeat(MAX_COMMAND_KEY_LEN + 1);
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: long_key,
                instruction: "ok".to_string(),
                step_delay_ms: None,
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation {
            field: "command_key",
            ..
        })
    ));

    // Oversized instruction
    let long_instruction = "i".repeat(MAX_INSTRUCTION_LEN + 1);
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "k1".to_string(),
                instruction: long_instruction,
                step_delay_ms: None,
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation {
            field: "instruction",
            ..
        })
    ));

    // Invalid step_delay_ms and now_ms
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "k2".to_string(),
                instruction: "ok".to_string(),
                step_delay_ms: Some(MAX_DEMO_STEP_DELAY_MS + 1),
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation {
            field: "step_delay_ms",
            ..
        })
    ));
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "k3".to_string(),
                instruction: "ok".to_string(),
                step_delay_ms: Some(100),
                now_ms: -1,
            }
        ),
        Err(CoreError::Validation {
            field: "now_ms",
            ..
        })
    ));

    // PullEvents batch size limits
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 0,
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation { field: "limit", .. })
    ));
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: MAX_EVENT_BATCH_SIZE + 1,
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation { field: "limit", .. })
    ));

    // Oversized Answer and Cancel reason
    let long_answer = "a".repeat(MAX_ANSWER_LEN + 1);
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "k4".to_string(),
                run_id: "run_1".to_string(),
                question_id: "qst_1".to_string(),
                expected_revision: 1,
                answer: long_answer,
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation {
            field: "answer",
            ..
        })
    ));

    let long_reason = "r".repeat(MAX_CANCEL_REASON_LEN + 1);
    assert!(matches!(
        core.execute(
            &binding.binding_id,
            Command::Cancel {
                command_key: "k5".to_string(),
                run_id: "run_1".to_string(),
                reason: Some(long_reason),
                now_ms: 100,
            }
        ),
        Err(CoreError::Validation {
            field: "reason",
            ..
        })
    ));

    // Oversized wake-up error detail
    let long_err = "e".repeat(MAX_WAKEUP_ERROR_LEN + 1);
    assert!(matches!(
        core.record_wakeup_result("wcl_1", WakeupOutcome::Failed { error: long_err }, 100),
        Err(CoreError::Validation { field: "error", .. })
    ));
}

#[test]
fn wakeup_coalescing_partial_ack_coverage_and_inflight_recovery_to_unknown() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");

    let (binding_a_id, binding_b_id, claim_a_id) = {
        let mut core = Core::open(&db_path).expect("open");
        let binding_a = core
            .create_binding(UUID_A, "Coalesce A")
            .expect("binding a");
        let binding_b = core.create_binding(UUID_B, "Fail B").expect("binding b");

        // Start two runs on binding_a so two important QuestionAsked events coalesce into one Pending notice.
        core.execute(
            &binding_a.binding_id,
            Command::StartDemo {
                command_key: "r1".to_string(),
                instruction: "Run 1".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start r1");
        core.execute(
            &binding_a.binding_id,
            Command::StartDemo {
                command_key: "r2".to_string(),
                instruction: "Run 2".to_string(),
                step_delay_ms: Some(200),
                now_ms: 1_000,
            },
        )
        .expect("start r2");

        // At 1_100, r1 emits RunProgress (seq 3) and QuestionAsked (seq 4, important).
        core.tick(1_100).expect("tick r1");
        let w1 = core.get_wakeup(&binding_a.binding_id).expect("w1");
        assert_eq!(w1.status, WakeupStatus::Pending);
        assert_eq!(w1.pending_since_seq, Some(4));
        assert_eq!(w1.latest_important_seq, 4);
        assert_eq!(w1.coalesced_count, 1);

        // At 1_200, r2 emits RunProgress (seq 5) and QuestionAsked (seq 6, important).
        // Coalesces into the existing single Pending notice for binding_a!
        core.tick(1_200).expect("tick r2");
        let w2 = core.get_wakeup(&binding_a.binding_id).expect("w2");
        assert_eq!(w2.status, WakeupStatus::Pending);
        assert_eq!(w2.pending_since_seq, Some(4));
        assert_eq!(w2.latest_important_seq, 6);
        assert_eq!(w2.coalesced_count, 2);

        // Also trigger and fail a wake-up on binding_b to verify Failed is not retried.
        core.execute(
            &binding_b.binding_id,
            Command::StartDemo {
                command_key: "rb".to_string(),
                instruction: "Run B".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start rb");
        core.tick(1_100).expect("tick rb");
        let claim_b = core
            .claim_wakeup_for_binding(&binding_b.binding_id, 1_210)
            .expect("claim b")
            .expect("claim_b");
        let failed_b = core
            .record_wakeup_result(
                &claim_b.claim_id,
                WakeupOutcome::Failed {
                    error: "codex binary missing".to_string(),
                },
                1_220,
            )
            .expect("record failed b");
        assert_eq!(failed_b.status, WakeupStatus::Failed);
        assert_eq!(
            core.claim_wakeup_for_binding(&binding_b.binding_id, 1_230)
                .expect("retry b"),
            None
        );

        // Claim binding_a's coalesced wake-up -> persists InFlight intent before external call.
        let claim_a = core
            .claim_wakeup_for_binding(&binding_a.binding_id, 1_250)
            .expect("claim a")
            .expect("claim_a");
        assert_eq!(claim_a.pending_since_seq, 4);
        assert_eq!(claim_a.covered_up_to_seq, 6);
        assert_eq!(claim_a.coalesced_count, 2);

        let inflight_a = core.get_wakeup(&binding_a.binding_id).expect("inflight a");
        assert_eq!(inflight_a.status, WakeupStatus::InFlight);

        (binding_a.binding_id, binding_b.binding_id, claim_a.claim_id)
    };

    // Reopen database while binding_a's claim was InFlight -> recovers to Unknown without automatic retry.
    let mut core = Core::open(&db_path).expect("reopen after crash");
    let recovered_a = core.get_wakeup(&binding_a_id).expect("recovered a");
    assert_eq!(recovered_a.status, WakeupStatus::Unknown);
    assert_eq!(recovered_a.claim_id.as_deref(), Some(claim_a_id.as_str()));
    assert!(recovered_a.last_error.is_some());

    // Neither Unknown (binding_a) nor Failed (binding_b) is automatically retried by claim_wakeup.
    assert_eq!(core.claim_wakeup(2_000).expect("claim any"), None);
    assert_eq!(
        core.claim_wakeup_for_binding(&binding_b_id, 2_000)
            .expect("claim b"),
        None
    );

    // Start a 3rd run on binding_a and tick so a new important event arrives at seq = 9
    // while seq <= 6 is still unacknowledged.
    core.execute(
        &binding_a_id,
        Command::StartDemo {
            command_key: "r3".to_string(),
            instruction: "Run 3".to_string(),
            step_delay_ms: Some(100),
            now_ms: 2_000,
        },
    )
    .expect("start r3");
    core.tick(2_100).expect("tick r3");

    let still_unknown = core.get_wakeup(&binding_a_id).expect("still unknown");
    assert_eq!(still_unknown.status, WakeupStatus::Unknown);
    assert_eq!(still_unknown.covered_up_to_seq, 6);
    assert_eq!(still_unknown.latest_important_seq, 9);
    // Still cannot claim a second wake-up before the prior covered events (<= 6) are acknowledged.
    assert_eq!(core.claim_wakeup(2_150).expect("claim blocked"), None);

    // Partial pull & ack of seq 1..=2 (non-important RunStarted events) does NOT release coverage for seq <= 6.
    let batch_1_2 = core
        .execute(
            &binding_a_id,
            Command::PullEvents {
                after_seq: None,
                limit: 2,
                now_ms: 2_200,
            },
        )
        .expect("pull 1..2")
        .into_event_batch()
        .expect("batch 1..2");
    assert_eq!(batch_1_2.to_seq, 2);

    let ack_1_2 = core
        .execute(
            &binding_a_id,
            Command::AcknowledgeEvents {
                command_key: "ack-1-2".to_string(),
                batch_id: batch_1_2.batch_id,
                now_ms: 2_210,
            },
        )
        .expect("ack 1..2")
        .into_ack_result()
        .expect("ack result 1..2");
    assert_eq!(ack_1_2.wakeup_status, WakeupStatus::Unknown);

    // Now pull & acknowledge seq 3..=6 (covering covered_up_to_seq = 6, while seq 7..=9 remain unread).
    let batch_3_6 = core
        .execute(
            &binding_a_id,
            Command::PullEvents {
                after_seq: None,
                limit: 4,
                now_ms: 2_220,
            },
        )
        .expect("pull 3..6")
        .into_event_batch()
        .expect("batch 3..6");
    assert_eq!(batch_3_6.from_seq, 3);
    assert_eq!(batch_3_6.to_seq, 6);
    assert!(batch_3_6.has_more);

    let ack_3_6 = core
        .execute(
            &binding_a_id,
            Command::AcknowledgeEvents {
                command_key: "ack-3-6".to_string(),
                batch_id: batch_3_6.batch_id,
                now_ms: 2_230,
            },
        )
        .expect("ack 3..6")
        .into_ack_result()
        .expect("ack result 3..6");

    // Prior notification coverage (<= 6) is now released, and unread important event 9 remains Pending!
    assert_eq!(ack_3_6.acked_seq, 6);
    assert_eq!(ack_3_6.wakeup_status, WakeupStatus::Pending);

    let pending_for_9 = core.get_wakeup(&binding_a_id).expect("pending for 9");
    assert_eq!(pending_for_9.status, WakeupStatus::Pending);
    assert_eq!(pending_for_9.pending_since_seq, Some(9));
    assert_eq!(pending_for_9.latest_important_seq, 9);
    assert_eq!(pending_for_9.coalesced_count, 1);

    let claim_9 = core.claim_wakeup(2_240).expect("claim 9").expect("claim_9");
    assert_eq!(claim_9.pending_since_seq, 9);
    assert_eq!(claim_9.covered_up_to_seq, 9);
}
