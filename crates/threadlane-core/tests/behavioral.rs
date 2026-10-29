use tempfile::TempDir;
use threadlane_core::{
    estimate_wire_response_bytes, AckErrorReason, Command, CommandOutput, Core, CoreError,
    EventKind, QuestionStatus, RunStatus, WakeupOutcome, WakeupRetryRecord, WakeupStatus,
    DEFAULT_PAGE_LIMIT, FIXED_WAKEUP_MESSAGE, MAX_ANSWER_LEN, MAX_CANCEL_REASON_LEN,
    MAX_COMMAND_KEY_LEN, MAX_COMPACT_RESPONSE_BYTES, MAX_CURSOR_LEN, MAX_DEMO_STEP_DELAY_MS,
    MAX_EVENT_BATCH_SIZE, MAX_EVENT_PAYLOAD_BYTES, MAX_INSTRUCTION_LEN, MAX_LABEL_LEN,
    MAX_PAGE_LIMIT, MAX_PAGE_RESPONSE_BYTES, MAX_STATUS_RECENT_RUNS,
    MAX_SUMMARY_TEXT_PREVIEW_CHARS, MAX_WAKEUP_ERROR_LEN, SCHEMA_VERSION,
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

    assert!(started.untrusted_data);
    assert_eq!(started.status, RunStatus::Running);
    assert_eq!(started.revision, 1);
    assert_eq!(started.step_seq, 1);
    assert_eq!(started.next_step_at_ms, Some(1_500));
    assert_eq!(started.active_question_id, None);
    assert_eq!(started.active_question, None);
    let started_revs = core
        .list_run_revisions(
            &binding.binding_id,
            &started.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list started revisions");
    assert_eq!(started_revs.revisions.len(), 1);
    assert!(started_revs.revisions[0].untrusted_data);
    assert_eq!(
        started_revs.revisions[0].instruction,
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
    let question_id = waiting
        .active_question_id
        .clone()
        .expect("active question id");
    let active_q = waiting
        .active_question
        .as_ref()
        .expect("active question embedded in waiting run");
    assert!(active_q.untrusted_data);
    assert_eq!(active_q.question_id, question_id);
    assert_eq!(active_q.status, QuestionStatus::Pending);
    assert_eq!(active_q.revision, 1);
    let waiting_questions = core
        .list_run_questions(
            &binding.binding_id,
            &started.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list waiting questions");
    assert_eq!(waiting_questions.questions.len(), 1);
    assert_eq!(waiting_questions.questions[0].question_id, question_id);
    assert_eq!(
        waiting_questions.questions[0].status,
        QuestionStatus::Pending
    );
    assert_eq!(waiting_questions.questions[0].revision, 1);

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
    assert_eq!(
        batch_1.events[0].payload.instruction.as_deref(),
        Some("Implement bounded conversation loop")
    );
    assert_eq!(batch_1.events[1].kind, EventKind::RunProgress);
    assert_eq!(batch_1.events[1].payload.instruction, None);
    assert_eq!(batch_1.events[2].kind, EventKind::QuestionAsked);
    assert_eq!(batch_1.events[2].payload.instruction, None);
    let asked_prompt = batch_1.events[2]
        .payload
        .question_prompt
        .as_deref()
        .expect("question prompt");
    assert!(asked_prompt.contains(&started.run_id));
    assert!(asked_prompt.contains("revision 1"));
    assert!(asked_prompt.contains("confirm the current instruction"));
    assert!(asked_prompt.contains("get_run"));
    assert!(!asked_prompt.contains("Implement bounded conversation loop"));
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
    assert_eq!(completing.active_question, None);
    assert_eq!(completing.next_step_at_ms, Some(2_500));
    let completing_questions = core
        .list_run_questions(
            &binding.binding_id,
            &started.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list completing questions");
    assert_eq!(
        completing_questions.questions[0].status,
        QuestionStatus::Answered
    );
    assert_eq!(
        completing_questions.questions[0].answer.as_deref(),
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
    assert_eq!(batch_2.events[0].payload.instruction, None);
    assert_eq!(
        batch_2.events[0].payload.answer.as_deref(),
        Some("Approved for execution")
    );
    assert_eq!(batch_2.events[1].kind, EventKind::RunCompleted);
    assert_eq!(batch_2.events[1].payload.instruction, None);
    assert_eq!(batch_2.events[1].payload.answer, None);

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
    let runs_page = core
        .execute(
            &binding_id,
            Command::ListRuns {
                cursor: None,
                limit: DEFAULT_PAGE_LIMIT,
            },
        )
        .expect("list runs")
        .into_run_list()
        .expect("run list");
    assert_eq!(runs_page.runs.len(), 1);
    assert!(!runs_page.has_more);
    assert_eq!(runs_page.next_cursor, None);

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
    assert_eq!(steered_v2.active_question, None);
    assert_eq!(steered_v2.next_step_at_ms, Some(1_500));

    let v2_revs = core
        .list_run_revisions(&binding.binding_id, &run.run_id, None, DEFAULT_PAGE_LIMIT)
        .expect("list v2 revisions");
    assert_eq!(v2_revs.revisions.len(), 2);
    assert_eq!(v2_revs.revisions[0].revision, 1);
    assert_eq!(v2_revs.revisions[0].superseded_at_ms, Some(1_300));
    assert_eq!(v2_revs.revisions[1].revision, 2);
    assert_eq!(v2_revs.revisions[1].instruction, "Plan v2");

    let v2_qs = core
        .list_run_questions(&binding.binding_id, &run.run_id, None, DEFAULT_PAGE_LIMIT)
        .expect("list v2 questions");
    assert_eq!(v2_qs.questions.len(), 1);
    assert_eq!(v2_qs.questions[0].status, QuestionStatus::Superseded);
    assert_eq!(v2_qs.questions[0].superseded_by_revision, Some(2));

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
    assert_eq!(
        waiting_v2
            .active_question
            .as_ref()
            .expect("active q2")
            .question_id,
        q2_id
    );
    let waiting_v2_qs = core
        .list_run_questions(&binding.binding_id, &run.run_id, None, DEFAULT_PAGE_LIMIT)
        .expect("list waiting v2 questions");
    assert_eq!(waiting_v2_qs.questions.len(), 2);

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

#[test]
fn escaped_instruction_start_steer_completion_and_oversized_rejection() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding_a = core
        .create_binding(UUID_A, "Escaped Instruction A")
        .expect("binding a");
    let binding_b = core
        .create_binding(UUID_B, "Companion Binding B")
        .expect("binding b");

    // 1. Exact reproduction of the 4,096-byte control-character instruction that previously
    // passed `start_demo` (16,344 escaped JSON bytes <= 16,384) and then failed forever on
    // `tick` when `QuestionAsked` expanded to 16,420 bytes > 16,384.
    let repro_instruction = "\u{1}".repeat(2_411) + &"a".repeat(1_685);
    assert_eq!(repro_instruction.len(), MAX_INSTRUCTION_LEN);

    let started_a = core
        .execute(
            &binding_a.binding_id,
            Command::StartDemo {
                command_key: "repro-start".to_string(),
                instruction: repro_instruction.clone(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start repro instruction")
        .into_run()
        .expect("run a");
    assert_eq!(started_a.instruction, repro_instruction);
    let started_a_revs = core
        .list_run_revisions(
            &binding_a.binding_id,
            &started_a.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list started_a revs");
    assert_eq!(started_a_revs.revisions[0].instruction, repro_instruction);

    let started_b = core
        .execute(
            &binding_b.binding_id,
            Command::StartDemo {
                command_key: "b-start".to_string(),
                instruction: "Sibling binding run".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start b")
        .into_run()
        .expect("run b");

    // 2. Both bindings advance cleanly on tick without EventPayloadTooLarge.
    let tick_1 = core.tick(1_100).expect("tick 1 must succeed");
    assert_eq!(
        tick_1.advanced_runs,
        vec![started_a.run_id.clone(), started_b.run_id.clone()]
    );
    assert_eq!(tick_1.emitted_events, 4);

    let waiting_a_v1 = core
        .get_run(&binding_a.binding_id, &started_a.run_id)
        .expect("get waiting a v1");
    assert_eq!(waiting_a_v1.status, RunStatus::WaitingInput);
    assert_eq!(waiting_a_v1.instruction, repro_instruction);
    let waiting_a_v1_revs = core
        .list_run_revisions(
            &binding_a.binding_id,
            &started_a.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list waiting_a_v1 revs");
    assert_eq!(
        waiting_a_v1_revs.revisions[0].instruction,
        repro_instruction
    );
    let q1 = waiting_a_v1.active_question.as_ref().expect("active q1");
    assert!(q1.prompt.contains(&started_a.run_id));
    assert!(q1.prompt.contains("revision 1"));
    assert!(q1.prompt.contains("confirm the current instruction"));
    assert!(q1.prompt.contains("get_run"));
    assert!(!q1.prompt.contains('\u{1}'));

    // 3. Steer with another 4,096-byte control-character instruction and advance via tick.
    let steered_instruction = "\u{1}".repeat(2_411) + &"b".repeat(1_685);
    assert_eq!(steered_instruction.len(), MAX_INSTRUCTION_LEN);

    let steered_a = core
        .execute(
            &binding_a.binding_id,
            Command::Steer {
                command_key: "repro-steer".to_string(),
                run_id: started_a.run_id.clone(),
                expected_revision: 1,
                instruction: steered_instruction.clone(),
                now_ms: 1_200,
            },
        )
        .expect("steer repro instruction")
        .into_run()
        .expect("steered run a");
    assert_eq!(steered_a.revision, 2);
    assert_eq!(steered_a.instruction, steered_instruction);
    let steered_a_revs = core
        .list_run_revisions(
            &binding_a.binding_id,
            &started_a.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list steered_a revs");
    assert_eq!(steered_a_revs.revisions.len(), 2);
    assert_eq!(steered_a_revs.revisions[0].instruction, repro_instruction);
    assert_eq!(steered_a_revs.revisions[1].instruction, steered_instruction);

    let tick_2 = core.tick(1_300).expect("tick 2 after steer must succeed");
    assert_eq!(tick_2.advanced_runs, vec![started_a.run_id.clone()]);
    assert_eq!(tick_2.emitted_events, 2);

    let waiting_a_v2 = core
        .get_run(&binding_a.binding_id, &started_a.run_id)
        .expect("waiting a v2");
    assert_eq!(waiting_a_v2.status, RunStatus::WaitingInput);
    let q2_id = waiting_a_v2.active_question_id.clone().expect("q2 id");
    let q2_active = waiting_a_v2.active_question.as_ref().expect("active q2");
    assert!(q2_active.prompt.contains("revision 2"));
    assert!(!q2_active.prompt.contains('\u{1}'));

    // 4. Answer with a 4,096-byte escaped quote payload and tick to completion.
    let max_quote_answer = "\"".repeat(MAX_ANSWER_LEN);
    core.execute(
        &binding_a.binding_id,
        Command::Answer {
            command_key: "repro-answer".to_string(),
            run_id: started_a.run_id.clone(),
            question_id: q2_id,
            expected_revision: 2,
            answer: max_quote_answer.clone(),
            now_ms: 1_400,
        },
    )
    .expect("answer v2");

    let tick_3 = core.tick(1_500).expect("tick 3 to completed must succeed");
    assert_eq!(tick_3.advanced_runs, vec![started_a.run_id.clone()]);
    assert_eq!(tick_3.emitted_events, 1);

    let completed_a = core
        .get_run(&binding_a.binding_id, &started_a.run_id)
        .expect("completed a");
    assert_eq!(completed_a.status, RunStatus::Completed);
    assert_eq!(completed_a.instruction, steered_instruction);
    let completed_a_revs = core
        .list_run_revisions(
            &binding_a.binding_id,
            &started_a.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list completed_a revs");
    assert_eq!(completed_a_revs.revisions[0].instruction, repro_instruction);
    assert_eq!(
        completed_a_revs.revisions[1].instruction,
        steered_instruction
    );
    let completed_a_qs = core
        .list_run_questions(
            &binding_a.binding_id,
            &started_a.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("list completed_a questions");
    assert_eq!(
        completed_a_qs.questions[1].answer.as_deref(),
        Some(max_quote_answer.as_str())
    );

    let batch_a = core
        .execute(
            &binding_a.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 50,
                now_ms: 1_510,
            },
        )
        .expect("pull a events")
        .into_event_batch()
        .expect("batch a");
    assert_eq!(batch_a.events.len(), 9);
    for event in &batch_a.events {
        let json = serde_json::to_string(&event.payload).expect("serialize payload");
        assert!(json.len() <= MAX_EVENT_PAYLOAD_BYTES);
        match event.kind {
            EventKind::RunStarted => {
                assert_eq!(
                    event.payload.instruction.as_deref(),
                    Some(repro_instruction.as_str())
                );
            }
            EventKind::RunSteered => {
                assert_eq!(
                    event.payload.instruction.as_deref(),
                    Some(steered_instruction.as_str())
                );
            }
            EventKind::RunProgress
            | EventKind::QuestionAsked
            | EventKind::QuestionAnswered
            | EventKind::QuestionSuperseded
            | EventKind::RunCompleted
            | EventKind::RunCancelled => {
                assert_eq!(event.payload.instruction, None);
            }
            unexpected => panic!("demo run emitted unexpected non-demo event: {unexpected:?}"),
        }
    }

    // 5. Verify maximum quotes, backslashes, and multibyte Unicode instructions all progress to completion.
    let edge_instructions = [
        "\"".repeat(MAX_INSTRUCTION_LEN),
        "\\".repeat(MAX_INSTRUCTION_LEN),
        "🦀".repeat(MAX_INSTRUCTION_LEN / 4),
        "\u{2028}".repeat(MAX_INSTRUCTION_LEN / 3) + "z",
    ];
    for (idx, edge_inst) in edge_instructions.iter().enumerate() {
        assert_eq!(edge_inst.len(), MAX_INSTRUCTION_LEN);
        let run = core
            .execute(
                &binding_b.binding_id,
                Command::StartDemo {
                    command_key: format!("edge-start-{idx}"),
                    instruction: edge_inst.clone(),
                    step_delay_ms: Some(10),
                    now_ms: 2_000,
                },
            )
            .expect("start edge instruction")
            .into_run()
            .expect("edge run");
        core.tick(2_010).expect("tick edge to question");
        let waiting = core
            .get_run(&binding_b.binding_id, &run.run_id)
            .expect("waiting edge");
        assert_eq!(waiting.instruction, *edge_inst);
        let q_id = waiting.active_question_id.expect("edge q_id");
        core.execute(
            &binding_b.binding_id,
            Command::Answer {
                command_key: format!("edge-ans-{idx}"),
                run_id: run.run_id.clone(),
                question_id: q_id,
                expected_revision: 1,
                answer: "ok".to_string(),
                now_ms: 2_020,
            },
        )
        .expect("answer edge");
        core.tick(2_030).expect("tick edge to completion");
        let done = core
            .get_run(&binding_b.binding_id, &run.run_id)
            .expect("done edge");
        assert_eq!(done.status, RunStatus::Completed);
        assert_eq!(done.instruction, *edge_inst);
    }

    // 6. Oversized immediate event payload (<= 4,096 raw bytes, > 16,384 escaped JSON bytes)
    // is rejected transactionally before commit with NO partial write.
    let dir_reject = TempDir::new().expect("tempdir");
    let mut core_reject = Core::open(dir_reject.path().join("reject.db")).expect("open reject");
    let b_reject = core_reject
        .create_binding(UUID_A, "Reject Chat")
        .expect("binding reject");

    // 2,730 * 6 = 16,380 escaped bytes (+ envelope > 16,384), while raw byte length is 2,730 <= 4,096.
    let oversized_escaped = "\u{1}".repeat(2_730);
    assert!(oversized_escaped.len() <= MAX_INSTRUCTION_LEN);

    let start_err = core_reject
        .execute(
            &b_reject.binding_id,
            Command::StartDemo {
                command_key: "atomic-key-1".to_string(),
                instruction: oversized_escaped.clone(),
                step_delay_ms: Some(50),
                now_ms: 100,
            },
        )
        .expect_err("oversized immediate RunStarted event must fail");
    assert!(matches!(
        start_err,
        CoreError::EventPayloadTooLarge {
            max_bytes: MAX_EVENT_PAYLOAD_BYTES,
            ..
        }
    ));

    // Verify no partial write occurred on StartDemo failure.
    let runs_after_reject = core_reject
        .execute(
            &b_reject.binding_id,
            Command::ListRuns {
                cursor: None,
                limit: DEFAULT_PAGE_LIMIT,
            },
        )
        .expect("list runs")
        .into_run_list()
        .expect("runs");
    assert!(runs_after_reject.runs.is_empty());
    let events_after_reject = core_reject
        .execute(
            &b_reject.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 101,
            },
        )
        .expect("pull")
        .into_event_batch()
        .expect("batch");
    assert!(events_after_reject.events.is_empty());

    // Because the transaction rolled back, `atomic-key-1` was not committed to `command_dedup`
    // and can now be used for a valid StartDemo.
    let valid_run = core_reject
        .execute(
            &b_reject.binding_id,
            Command::StartDemo {
                command_key: "atomic-key-1".to_string(),
                instruction: "Valid instruction".to_string(),
                step_delay_ms: Some(50),
                now_ms: 105,
            },
        )
        .expect("valid start with same key")
        .into_run()
        .expect("valid run");
    core_reject.tick(155).expect("tick valid run");
    let waiting_valid = core_reject
        .get_run(&b_reject.binding_id, &valid_run.run_id)
        .expect("waiting valid");
    let valid_q_id = waiting_valid.active_question_id.clone().expect("q_id");

    // Oversized immediate Steer fails transactionally without superseding the pending question or advancing revision.
    let steer_err = core_reject
        .execute(
            &b_reject.binding_id,
            Command::Steer {
                command_key: "atomic-steer-key".to_string(),
                run_id: valid_run.run_id.clone(),
                expected_revision: 1,
                instruction: oversized_escaped.clone(),
                now_ms: 160,
            },
        )
        .expect_err("oversized immediate RunSteered event must fail");
    assert!(matches!(steer_err, CoreError::EventPayloadTooLarge { .. }));

    let after_failed_steer = core_reject
        .get_run(&b_reject.binding_id, &valid_run.run_id)
        .expect("after failed steer");
    assert_eq!(after_failed_steer.revision, 1);
    assert_eq!(after_failed_steer.status, RunStatus::WaitingInput);
    assert_eq!(after_failed_steer.instruction, "Valid instruction");
    let revs_after_failed_steer = core_reject
        .list_run_revisions(
            &b_reject.binding_id,
            &valid_run.run_id,
            None,
            DEFAULT_PAGE_LIMIT,
        )
        .expect("revs after failed steer");
    assert_eq!(revs_after_failed_steer.revisions.len(), 1);
    assert_eq!(
        after_failed_steer
            .active_question
            .as_ref()
            .expect("active q")
            .status,
        QuestionStatus::Pending
    );

    // Oversized immediate Answer fails transactionally without marking the question Answered.
    let answer_err = core_reject
        .execute(
            &b_reject.binding_id,
            Command::Answer {
                command_key: "atomic-answer-key".to_string(),
                run_id: valid_run.run_id.clone(),
                question_id: valid_q_id.clone(),
                expected_revision: 1,
                answer: oversized_escaped,
                now_ms: 165,
            },
        )
        .expect_err("oversized immediate QuestionAnswered event must fail");
    assert!(matches!(answer_err, CoreError::EventPayloadTooLarge { .. }));

    let after_failed_answer = core_reject
        .get_run(&b_reject.binding_id, &valid_run.run_id)
        .expect("after failed answer");
    assert_eq!(after_failed_answer.status, RunStatus::WaitingInput);
    assert_eq!(
        after_failed_answer.active_question_id.as_deref(),
        Some(valid_q_id.as_str())
    );
    let q_after_failed_answer = after_failed_answer
        .active_question
        .as_ref()
        .expect("active q after failed answer");
    assert_eq!(q_after_failed_answer.status, QuestionStatus::Pending);
    assert_eq!(q_after_failed_answer.answer, None);
}

#[test]
fn restart_with_already_persisted_raw_control_instructions_advances_cleanly() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("legacy_state.db");

    let repro_instruction = "\u{1}".repeat(2_411) + &"a".repeat(1_685);
    let all_control_instruction = "\u{1}".repeat(MAX_INSTRUCTION_LEN);

    let (binding_a_id, binding_b_id, run_1_id, run_2_id, run_b_id) = {
        let mut core = Core::open(&db_path).expect("open");
        let b_a = core.create_binding(UUID_A, "Legacy A").expect("b_a");
        let b_b = core.create_binding(UUID_B, "Legacy B").expect("b_b");

        let r1 = core
            .execute(
                &b_a.binding_id,
                Command::StartDemo {
                    command_key: "r1".to_string(),
                    instruction: "placeholder 1".to_string(),
                    step_delay_ms: Some(100),
                    now_ms: 1_000,
                },
            )
            .expect("r1")
            .into_run()
            .expect("r1");
        let r2 = core
            .execute(
                &b_a.binding_id,
                Command::StartDemo {
                    command_key: "r2".to_string(),
                    instruction: "placeholder 2".to_string(),
                    step_delay_ms: Some(100),
                    now_ms: 1_000,
                },
            )
            .expect("r2")
            .into_run()
            .expect("r2");
        let rb = core
            .execute(
                &b_b.binding_id,
                Command::StartDemo {
                    command_key: "rb".to_string(),
                    instruction: "normal companion".to_string(),
                    step_delay_ms: Some(100),
                    now_ms: 1_000,
                },
            )
            .expect("rb")
            .into_run()
            .expect("rb");
        (
            b_a.binding_id,
            b_b.binding_id,
            r1.run_id,
            r2.run_id,
            rb.run_id,
        )
    };

    // Simulate a database persisted by the previous build containing both the 2,411-control
    // instruction and a full 4,096-control instruction (24,576 escaped bytes) in `runs` and `run_revisions`.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open raw sqlite");
        conn.execute(
            "UPDATE runs SET instruction = ?1 WHERE run_id = ?2",
            rusqlite::params![repro_instruction, run_1_id],
        )
        .expect("update r1 instruction");
        conn.execute(
            "UPDATE run_revisions SET instruction = ?1 WHERE run_id = ?2 AND revision = 1",
            rusqlite::params![repro_instruction, run_1_id],
        )
        .expect("update r1 revision");
        conn.execute(
            "UPDATE runs SET instruction = ?1 WHERE run_id = ?2",
            rusqlite::params![all_control_instruction, run_2_id],
        )
        .expect("update r2 instruction");
        conn.execute(
            "UPDATE run_revisions SET instruction = ?1 WHERE run_id = ?2 AND revision = 1",
            rusqlite::params![all_control_instruction, run_2_id],
        )
        .expect("update r2 revision");
    }

    // Reopen and tick: all runs across both bindings advance to WaitingInput and then Completed!
    let mut core = Core::open(&db_path).expect("reopen");
    let tick_report = core.tick(1_100).expect("tick legacy runs");
    assert_eq!(
        tick_report.advanced_runs,
        vec![run_1_id.clone(), run_2_id.clone(), run_b_id.clone()]
    );
    assert_eq!(tick_report.emitted_events, 6);

    let r1 = core.get_run(&binding_a_id, &run_1_id).expect("get r1");
    assert_eq!(r1.status, RunStatus::WaitingInput);
    assert_eq!(r1.instruction, repro_instruction);
    let r1_revs = core
        .list_run_revisions(&binding_a_id, &run_1_id, None, DEFAULT_PAGE_LIMIT)
        .expect("list r1 revs");
    assert_eq!(r1_revs.revisions[0].instruction, repro_instruction);
    let q1_id = r1.active_question_id.expect("q1");

    let r2 = core.get_run(&binding_a_id, &run_2_id).expect("get r2");
    assert_eq!(r2.status, RunStatus::WaitingInput);
    assert_eq!(r2.instruction, all_control_instruction);
    let r2_revs = core
        .list_run_revisions(&binding_a_id, &run_2_id, None, DEFAULT_PAGE_LIMIT)
        .expect("list r2 revs");
    assert_eq!(r2_revs.revisions[0].instruction, all_control_instruction);
    let q2_id = r2.active_question_id.expect("q2");

    let rb = core.get_run(&binding_b_id, &run_b_id).expect("get rb");
    assert_eq!(rb.status, RunStatus::WaitingInput);

    // Answer both legacy runs on binding_a and tick to Completed across another reopen.
    core.execute(
        &binding_a_id,
        Command::Answer {
            command_key: "ans-r1".to_string(),
            run_id: run_1_id.clone(),
            question_id: q1_id,
            expected_revision: 1,
            answer: "confirm r1".to_string(),
            now_ms: 1_200,
        },
    )
    .expect("answer r1");
    core.execute(
        &binding_a_id,
        Command::Answer {
            command_key: "ans-r2".to_string(),
            run_id: run_2_id.clone(),
            question_id: q2_id,
            expected_revision: 1,
            answer: "confirm r2".to_string(),
            now_ms: 1_200,
        },
    )
    .expect("answer r2");

    drop(core);
    let mut core = Core::open(&db_path).expect("reopen before completion");
    let complete_report = core.tick(1_300).expect("complete legacy runs");
    assert_eq!(
        complete_report.advanced_runs,
        vec![run_1_id.clone(), run_2_id.clone()]
    );

    let r1_done = core.get_run(&binding_a_id, &run_1_id).expect("r1 done");
    let r2_done = core.get_run(&binding_a_id, &run_2_id).expect("r2 done");
    assert_eq!(r1_done.status, RunStatus::Completed);
    assert_eq!(r1_done.instruction, repro_instruction);
    assert_eq!(r2_done.status, RunStatus::Completed);
    assert_eq!(r2_done.instruction, all_control_instruction);
}

#[test]
fn manual_retry_wakeup_lifecycle_audit_isolation_and_ack_reconciliation() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding_a = core.create_binding(UUID_A, "Retry A").expect("binding a");
    let binding_b = core.create_binding(UUID_B, "Retry B").expect("binding b");

    // 1. Reject Idle state.
    let err_idle = core
        .retry_wakeup(&binding_a.binding_id, "wcl_nonexistent", true, 900)
        .expect_err("Idle wakeup cannot be retried");
    assert!(matches!(
        err_idle,
        CoreError::InvalidWakeupState {
            status: WakeupStatus::Idle,
            action: "retry_wakeup",
            ..
        }
    ));

    // Emit an important event on binding_a (seq = 3).
    core.execute(
        &binding_a.binding_id,
        Command::StartDemo {
            command_key: "start-a1".to_string(),
            instruction: "Run A1".to_string(),
            step_delay_ms: Some(100),
            now_ms: 1_000,
        },
    )
    .expect("start a1");
    core.tick(1_100).expect("tick a1");

    // 2. Reject Pending state.
    let err_pending = core
        .retry_wakeup(&binding_a.binding_id, "wcl_nonexistent", true, 1_105)
        .expect_err("Pending wakeup cannot be retried");
    assert!(matches!(
        err_pending,
        CoreError::InvalidWakeupState {
            status: WakeupStatus::Pending,
            action: "retry_wakeup",
            ..
        }
    ));

    // Claim wake-up -> InFlight (claim_1, attempt_count = 1).
    let claim_1 = core
        .claim_wakeup_for_binding(&binding_a.binding_id, 1_110)
        .expect("claim 1")
        .expect("claim_1");
    assert_eq!(claim_1.pending_since_seq, 3);
    assert_eq!(claim_1.covered_up_to_seq, 3);
    assert_eq!(claim_1.coalesced_count, 1);

    // 3. Reject InFlight state (with matching claim_id) and stale claim_id while InFlight.
    let err_inflight = core
        .retry_wakeup(&binding_a.binding_id, &claim_1.claim_id, true, 1_115)
        .expect_err("InFlight wakeup cannot be retried");
    assert!(matches!(
        err_inflight,
        CoreError::InvalidWakeupState {
            status: WakeupStatus::InFlight,
            action: "retry_wakeup",
            ..
        }
    ));
    let err_stale_inflight = core
        .retry_wakeup(&binding_a.binding_id, "wcl_stale", true, 1_115)
        .expect_err("Unmatched claim while InFlight must fail");
    assert!(matches!(
        err_stale_inflight,
        CoreError::WakeupClaimNotFound { .. }
    ));

    // Record Failed outcome on claim_1.
    let failed_notice = core
        .record_wakeup_result(
            &claim_1.claim_id,
            WakeupOutcome::Failed {
                error: "codex binary missing".to_string(),
            },
            1_120,
        )
        .expect("record failed");
    assert_eq!(failed_notice.status, WakeupStatus::Failed);
    assert_eq!(failed_notice.attempt_count, 1);
    assert_eq!(
        failed_notice.claim_id.as_deref(),
        Some(claim_1.claim_id.as_str())
    );
    assert_eq!(
        core.claim_wakeup(1_125).expect("no auto retry"),
        None,
        "Failed wake-up must never be automatically retried"
    );

    // 4. Cross-binding and stale claim rejections when Failed.
    let err_missing_binding = core
        .retry_wakeup("bnd_missing", &claim_1.claim_id, false, 1_130)
        .expect_err("missing binding");
    assert!(matches!(
        err_missing_binding,
        CoreError::BindingNotFound { .. }
    ));

    // Cross-binding: binding_b is Idle, claim_1 belongs to binding_a.
    let err_cross_idle = core
        .retry_wakeup(&binding_b.binding_id, &claim_1.claim_id, true, 1_130)
        .expect_err("cross-binding retry on Idle binding_b must fail");
    assert!(matches!(
        err_cross_idle,
        CoreError::WakeupClaimNotFound { .. }
    ));

    // Stale/wrong expected_claim_id on binding_a does not alter Failed state.
    let err_wrong_claim = core
        .retry_wakeup(&binding_a.binding_id, "wcl_wrong_id", false, 1_130)
        .expect_err("wrong expected_claim_id must fail");
    assert!(matches!(
        err_wrong_claim,
        CoreError::WakeupClaimNotFound { .. }
    ));
    let still_failed = core
        .get_wakeup(&binding_a.binding_id)
        .expect("still failed");
    assert_eq!(still_failed.status, WakeupStatus::Failed);

    // 5. Rearm Failed wake-up with matching expected_claim_id (allow_unknown = false).
    let rearmed_1 = core
        .retry_wakeup(&binding_a.binding_id, &claim_1.claim_id, false, 1_140)
        .expect("rearm failed claim_1");
    assert_eq!(rearmed_1.status, WakeupStatus::Pending);
    assert_eq!(rearmed_1.claim_id, None);
    assert_eq!(rearmed_1.covered_up_to_seq, 0);
    assert_eq!(rearmed_1.pending_since_seq, Some(3));
    assert_eq!(rearmed_1.latest_important_seq, 3);
    assert_eq!(rearmed_1.coalesced_count, 1);
    assert_eq!(rearmed_1.attempt_count, 1);
    assert_eq!(rearmed_1.last_error, None);
    assert_eq!(rearmed_1.claimed_at_ms, None);
    assert_eq!(rearmed_1.resolved_at_ms, None);

    // Binding delivered_seq and acked_seq are untouched.
    let b_a_state = core.get_binding(&binding_a.binding_id).expect("binding a");
    assert_eq!(b_a_state.delivered_seq, 0);
    assert_eq!(b_a_state.acked_seq, 0);

    // 6. Repeated retry with the same expected_claim_id causes no second side effect.
    let err_repeat = core
        .retry_wakeup(&binding_a.binding_id, &claim_1.claim_id, false, 1_145)
        .expect_err("repeated retry must fail");
    assert!(matches!(
        err_repeat,
        CoreError::InvalidWakeupState {
            status: WakeupStatus::Pending,
            action: "retry_wakeup",
            ..
        }
    ));
    let retries_after_repeat = core
        .list_wakeup_retries(&binding_a.binding_id)
        .expect("retries");
    assert_eq!(retries_after_repeat.len(), 1);

    // 7. Durability across reopen while Pending, then claim exactly one additional claim (claim_2).
    drop(core);
    let mut core = Core::open(&db_path).expect("reopen while pending");
    let pending_after_reopen = core
        .get_wakeup(&binding_a.binding_id)
        .expect("pending after reopen");
    assert_eq!(pending_after_reopen.status, WakeupStatus::Pending);
    assert_eq!(pending_after_reopen.attempt_count, 1);
    assert_eq!(pending_after_reopen.pending_since_seq, Some(3));

    let claim_2 = core.claim_wakeup(1_150).expect("claim 2").expect("claim_2");
    assert_ne!(claim_2.claim_id, claim_1.claim_id);
    assert_eq!(claim_2.pending_since_seq, 3);
    assert_eq!(claim_2.covered_up_to_seq, 3);
    assert_eq!(claim_2.coalesced_count, 1);
    assert_eq!(
        core.claim_wakeup(1_155).expect("no extra claim"),
        None,
        "One retry must allow only one additional claim"
    );
    let inflight_2 = core.get_wakeup(&binding_a.binding_id).expect("inflight 2");
    assert_eq!(inflight_2.attempt_count, 2);

    // Old retry with claim_1 while claim_2 is InFlight must never release claim_2!
    let err_old_on_inflight = core
        .retry_wakeup(&binding_a.binding_id, &claim_1.claim_id, true, 1_160)
        .expect_err("stale retry must not release newer InFlight claim");
    assert!(matches!(
        err_old_on_inflight,
        CoreError::WakeupClaimNotFound { .. }
    ));
    let still_inflight_2 = core
        .get_wakeup(&binding_a.binding_id)
        .expect("still inflight 2");
    assert_eq!(
        still_inflight_2.claim_id.as_deref(),
        Some(claim_2.claim_id.as_str())
    );

    // 8. Reopen while claim_2 is InFlight -> recovers to Unknown.
    drop(core);
    let mut core = Core::open(&db_path).expect("reopen to unknown");
    let unknown_notice = core
        .get_wakeup(&binding_a.binding_id)
        .expect("unknown notice");
    assert_eq!(unknown_notice.status, WakeupStatus::Unknown);
    assert_eq!(
        unknown_notice.claim_id.as_deref(),
        Some(claim_2.claim_id.as_str())
    );

    // Stale claim_1 is still rejected when claim_2 is Unknown.
    assert!(matches!(
        core.retry_wakeup(&binding_a.binding_id, &claim_1.claim_id, true, 1_200),
        Err(CoreError::WakeupClaimNotFound { .. })
    ));

    // Unknown is refused without explicit `allow_unknown == true`.
    let err_unknown_disallowed = core
        .retry_wakeup(&binding_a.binding_id, &claim_2.claim_id, false, 1_205)
        .expect_err("Unknown wakeup must be refused when allow_unknown is false");
    assert!(matches!(
        err_unknown_disallowed,
        CoreError::InvalidWakeupState {
            status: WakeupStatus::Unknown,
            action: "retry_wakeup",
            ..
        }
    ));
    let still_unknown = core
        .get_wakeup(&binding_a.binding_id)
        .expect("still unknown");
    assert_eq!(still_unknown.status, WakeupStatus::Unknown);
    let retries_still_1 = core
        .list_wakeup_retries(&binding_a.binding_id)
        .expect("retries still 1");
    assert_eq!(retries_still_1.len(), 1);

    // Unknown is rearmed when `allow_unknown == true`.
    let rearmed_2 = core
        .retry_wakeup(&binding_a.binding_id, &claim_2.claim_id, true, 1_210)
        .expect("rearm unknown with allow_unknown=true");
    assert_eq!(rearmed_2.status, WakeupStatus::Pending);
    assert_eq!(rearmed_2.claim_id, None);
    assert_eq!(rearmed_2.covered_up_to_seq, 0);
    assert_eq!(rearmed_2.pending_since_seq, Some(3));
    assert_eq!(rearmed_2.coalesced_count, 1);
    assert_eq!(rearmed_2.attempt_count, 2);
    assert_eq!(rearmed_2.last_error, None);
    assert_eq!(rearmed_2.claimed_at_ms, None);
    assert_eq!(rearmed_2.resolved_at_ms, None);

    // Claim claim_3 and record Submitted -> Reject retry on Submitted.
    let claim_3 = core.claim_wakeup(1_220).expect("claim 3").expect("claim_3");
    core.record_wakeup_result(&claim_3.claim_id, WakeupOutcome::Submitted, 1_230)
        .expect("submit claim 3");
    assert!(matches!(
        core.retry_wakeup(&binding_a.binding_id, &claim_3.claim_id, true, 1_235),
        Err(CoreError::InvalidWakeupState {
            status: WakeupStatus::Submitted,
            action: "retry_wakeup",
            ..
        })
    ));

    // 9. Verify audit retention and immutable historical wakeup_claims across restarts.
    drop(core);
    let mut core = Core::open(&db_path).expect("reopen for audit check");
    let retries: Vec<WakeupRetryRecord> = core
        .list_wakeup_retries(&binding_a.binding_id)
        .expect("list retries");
    assert_eq!(retries.len(), 2);
    assert!(retries[0].retry_id.starts_with("wrt_"));
    assert_eq!(retries[0].binding_id, binding_a.binding_id);
    assert_eq!(retries[0].prior_claim_id, claim_1.claim_id);
    assert_eq!(retries[0].prior_status, WakeupStatus::Failed);
    assert!(!retries[0].allow_unknown);
    assert_eq!(retries[0].retried_at_ms, 1_140);

    assert!(retries[1].retry_id.starts_with("wrt_"));
    assert_eq!(retries[1].binding_id, binding_a.binding_id);
    assert_eq!(retries[1].prior_claim_id, claim_2.claim_id);
    assert_eq!(retries[1].prior_status, WakeupStatus::Unknown);
    assert!(retries[1].allow_unknown);
    assert_eq!(retries[1].retried_at_ms, 1_210);

    // Check historical `wakeup_claims` rows in SQLite are preserved immutably.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("raw sqlite");
        let mut stmt = conn
            .prepare(
                "SELECT claim_id, status, error_detail
                 FROM wakeup_claims
                 WHERE binding_id = ?1
                 ORDER BY rowid ASC",
            )
            .expect("prepare claims query");
        let rows: Vec<(String, String, Option<String>)> = stmt
            .query_map(rusqlite::params![binding_a.binding_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .expect("query claims")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, claim_1.claim_id);
        assert_eq!(rows[0].1, "failed");
        assert_eq!(rows[0].2.as_deref(), Some("codex binary missing"));
        assert_eq!(rows[1].0, claim_2.claim_id);
        assert_eq!(rows[1].1, "unknown");
        assert!(rows[1].2.is_some());
        assert_eq!(rows[2].0, claim_3.claim_id);
        assert_eq!(rows[2].1, "submitted");
        assert_eq!(rows[2].2, None);
    }

    // 10. Verify ACK after Unknown transitions to Idle when no newer important events exist,
    // and to Pending when newer important events exist.
    core.execute(
        &binding_b.binding_id,
        Command::StartDemo {
            command_key: "b-run-1".to_string(),
            instruction: "Run B1".to_string(),
            step_delay_ms: Some(50),
            now_ms: 2_000,
        },
    )
    .expect("start b1");
    core.tick(2_050).expect("tick b1");
    assert!(matches!(
        core.retry_wakeup(&binding_b.binding_id, &claim_1.claim_id, true, 2_055),
        Err(CoreError::WakeupClaimNotFound { .. })
    ));
    let claim_b1 = core
        .claim_wakeup_for_binding(&binding_b.binding_id, 2_060)
        .expect("claim b1")
        .expect("claim_b1");
    core.record_wakeup_result(
        &claim_b1.claim_id,
        WakeupOutcome::Unknown {
            reason: "codex queue timed out".to_string(),
        },
        2_070,
    )
    .expect("unknown b1");
    assert!(matches!(
        core.retry_wakeup(&binding_b.binding_id, &claim_1.claim_id, true, 2_075),
        Err(CoreError::WakeupClaimNotFound { .. })
    ));

    // Pull and ACK 1..=3 on binding_b with no newer important events -> Unknown transitions to Idle.
    let batch_b1 = core
        .execute(
            &binding_b.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 10,
                now_ms: 2_080,
            },
        )
        .expect("pull b1")
        .into_event_batch()
        .expect("batch b1");
    let ack_b1 = core
        .execute(
            &binding_b.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b1".to_string(),
                batch_id: batch_b1.batch_id,
                now_ms: 2_090,
            },
        )
        .expect("ack b1")
        .into_ack_result()
        .expect("ack result b1");
    assert_eq!(ack_b1.wakeup_status, WakeupStatus::Idle);
    let idle_b = core.get_wakeup(&binding_b.binding_id).expect("idle b");
    assert_eq!(idle_b.status, WakeupStatus::Idle);
    assert_eq!(idle_b.claim_id, None);
    assert_eq!(idle_b.claimed_at_ms, None);
    assert_eq!(idle_b.resolved_at_ms, None);

    // Now emit important event at seq 6 (Run B2), claim and mark Unknown, then emit newer
    // important event at seq 9 (Run B3) before acknowledging 4..=6.
    core.execute(
        &binding_b.binding_id,
        Command::StartDemo {
            command_key: "b-run-2".to_string(),
            instruction: "Run B2".to_string(),
            step_delay_ms: Some(50),
            now_ms: 2_100,
        },
    )
    .expect("start b2");
    core.tick(2_150).expect("tick b2");
    let claim_b2 = core
        .claim_wakeup_for_binding(&binding_b.binding_id, 2_160)
        .expect("claim b2")
        .expect("claim_b2");
    assert_eq!(claim_b2.covered_up_to_seq, 6);
    core.record_wakeup_result(
        &claim_b2.claim_id,
        WakeupOutcome::Unknown {
            reason: "timeout 2".to_string(),
        },
        2_170,
    )
    .expect("unknown b2");

    // Emit newer important event at seq 9 while binding_b is in Unknown (covered_up_to_seq = 6).
    core.execute(
        &binding_b.binding_id,
        Command::StartDemo {
            command_key: "b-run-3".to_string(),
            instruction: "Run B3".to_string(),
            step_delay_ms: Some(50),
            now_ms: 2_200,
        },
    )
    .expect("start b3");
    core.tick(2_250).expect("tick b3");

    // Pull and ACK 4..=6 (covering covered_up_to_seq = 6) -> Unknown transitions to Pending for seq 9!
    let batch_b2 = core
        .execute(
            &binding_b.binding_id,
            Command::PullEvents {
                after_seq: None,
                limit: 3,
                now_ms: 2_260,
            },
        )
        .expect("pull 4..6")
        .into_event_batch()
        .expect("batch 4..6");
    assert_eq!(batch_b2.from_seq, 4);
    assert_eq!(batch_b2.to_seq, 6);
    let ack_b2 = core
        .execute(
            &binding_b.binding_id,
            Command::AcknowledgeEvents {
                command_key: "ack-b2".to_string(),
                batch_id: batch_b2.batch_id,
                now_ms: 2_270,
            },
        )
        .expect("ack 4..6")
        .into_ack_result()
        .expect("ack result 4..6");
    assert_eq!(ack_b2.wakeup_status, WakeupStatus::Pending);
    let pending_b3 = core.get_wakeup(&binding_b.binding_id).expect("pending b3");
    assert_eq!(pending_b3.status, WakeupStatus::Pending);
    assert_eq!(pending_b3.claim_id, None);
    assert_eq!(pending_b3.pending_since_seq, Some(9));
    assert_eq!(pending_b3.covered_up_to_seq, 0);
    assert_eq!(pending_b3.coalesced_count, 1);
    assert_eq!(pending_b3.claimed_at_ms, None);
    assert_eq!(pending_b3.resolved_at_ms, None);
}

#[test]
fn m2a_hundreds_of_revisions_and_questions_max_escaped_byte_budget_paging() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane_m2a_scale.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding = core
        .create_binding(UUID_A, "M2A Scale Paging")
        .expect("binding");

    // Helper to build a 4,096-byte control-escaped instruction that fits within the 16 KiB event
    // payload budget (~16,344 first-level JSON bytes, ~47 KiB two-copy wire estimate).
    let make_max_instruction = |rev: u64| -> String {
        let suffix = format!("rev-{rev:04}-");
        let control_prefix = "\u{1}".repeat(2_410);
        let pad_len = MAX_INSTRUCTION_LEN - control_prefix.len() - suffix.len();
        format!("{control_prefix}{suffix}{}", "x".repeat(pad_len))
    };

    let inst_1 = make_max_instruction(1);
    assert_eq!(inst_1.len(), MAX_INSTRUCTION_LEN);

    let mut now_ms = 10_000_i64;
    let started_out = core
        .execute(
            &binding.binding_id,
            Command::StartDemo {
                command_key: "scale-start".to_string(),
                instruction: inst_1.clone(),
                step_delay_ms: Some(10),
                now_ms,
            },
        )
        .expect("start scale run");
    assert!(
        estimate_wire_response_bytes(&started_out).expect("wire bytes")
            <= MAX_COMPACT_RESPONSE_BYTES
    );
    let run = started_out.into_run().expect("run");

    // Advance revision 1 to WaitingInput so it creates question 1.
    now_ms += 10;
    core.tick(now_ms).expect("tick rev 1");

    let total_revisions = 210_u64;
    for rev in 2..=total_revisions {
        let inst = make_max_instruction(rev);
        assert_eq!(inst.len(), MAX_INSTRUCTION_LEN);

        now_ms += 10;
        let steer_out = core
            .execute(
                &binding.binding_id,
                Command::Steer {
                    command_key: format!("scale-steer-{rev}"),
                    run_id: run.run_id.clone(),
                    expected_revision: rev - 1,
                    instruction: inst.clone(),
                    now_ms,
                },
            )
            .expect("steer scale run");
        let steer_wire_bytes = estimate_wire_response_bytes(&steer_out).expect("steer wire bytes");
        assert!(
            steer_wire_bytes <= MAX_COMPACT_RESPONSE_BYTES,
            "compact steer snapshot at rev {rev} was {steer_wire_bytes} bytes > {MAX_COMPACT_RESPONSE_BYTES}"
        );

        now_ms += 10;
        core.tick(now_ms).expect("tick after steer");
    }

    // Answer the final pending question at revision 210 with a 4,096-byte quote-heavy answer.
    let waiting_final = core
        .get_run(&binding.binding_id, &run.run_id)
        .expect("get waiting final");
    assert_eq!(waiting_final.revision, total_revisions);
    assert_eq!(waiting_final.status, RunStatus::WaitingInput);
    let final_q = waiting_final
        .active_question
        .as_ref()
        .expect("final active question");
    assert_eq!(final_q.revision, total_revisions);
    assert_eq!(final_q.status, QuestionStatus::Pending);

    let get_wire_bytes =
        estimate_wire_response_bytes(&CommandOutput::Run(Box::new(waiting_final.clone())))
            .expect("get_run wire bytes");
    assert!(
        get_wire_bytes <= MAX_COMPACT_RESPONSE_BYTES,
        "GetRun wire bytes {get_wire_bytes} exceeded {MAX_COMPACT_RESPONSE_BYTES}"
    );

    let max_answer = "\"a".repeat(MAX_ANSWER_LEN / 2);
    assert_eq!(max_answer.len(), MAX_ANSWER_LEN);
    now_ms += 10;
    let answer_out = core
        .execute(
            &binding.binding_id,
            Command::Answer {
                command_key: "scale-final-answer".to_string(),
                run_id: run.run_id.clone(),
                question_id: final_q.question_id.clone(),
                expected_revision: total_revisions,
                answer: max_answer.clone(),
                now_ms,
            },
        )
        .expect("answer final question");
    assert!(
        estimate_wire_response_bytes(&answer_out).expect("answer wire bytes")
            <= MAX_COMPACT_RESPONSE_BYTES
    );

    // Page through all 210 revisions requesting `limit = MAX_PAGE_LIMIT (50)`.
    // Because each escaped revision takes ~47 KiB in the two-copy wire estimate, the byte budget
    // (`MAX_PAGE_RESPONSE_BYTES = 256 KiB`) must split pages early (around 5 items per page)
    // without skipping the first candidate that would exceed the budget.
    let mut collected_revisions = Vec::new();
    let mut rev_cursor: Option<String> = None;
    let mut saw_budget_split = false;
    loop {
        let page = core
            .list_run_revisions(
                &binding.binding_id,
                &run.run_id,
                rev_cursor.as_deref(),
                MAX_PAGE_LIMIT,
            )
            .expect("list_run_revisions page");
        let page_wire_bytes =
            estimate_wire_response_bytes(&CommandOutput::RevisionHistory(page.clone()))
                .expect("revision page wire bytes");
        assert!(
            page_wire_bytes <= MAX_PAGE_RESPONSE_BYTES,
            "revision page wire bytes {page_wire_bytes} exceeded {MAX_PAGE_RESPONSE_BYTES}"
        );
        assert!(!page.revisions.is_empty());
        if page.has_more && page.revisions.len() < MAX_PAGE_LIMIT {
            saw_budget_split = true;
        }
        collected_revisions.extend(page.revisions);
        if page.has_more {
            rev_cursor = Some(page.next_cursor.expect("next_cursor when has_more"));
        } else {
            assert!(page.next_cursor.is_none());
            break;
        }
    }

    assert!(
        saw_budget_split,
        "expected byte budget to split 50-row request into smaller pages"
    );
    assert_eq!(collected_revisions.len() as u64, total_revisions);
    for (idx, rev_entry) in collected_revisions.iter().enumerate() {
        let expected_rev = (idx as u64) + 1;
        assert_eq!(rev_entry.revision, expected_rev);
        assert_eq!(rev_entry.instruction, make_max_instruction(expected_rev));
        assert!(rev_entry.untrusted_data);
        if expected_rev < total_revisions {
            assert!(rev_entry.superseded_at_ms.is_some());
        } else {
            assert_eq!(rev_entry.superseded_at_ms, None);
        }
    }

    // Page through all 210 questions and verify exact ascending creation order and no missing/duplicate rows.
    let mut collected_questions = Vec::new();
    let mut q_cursor: Option<String> = None;
    loop {
        let page = core
            .list_run_questions(
                &binding.binding_id,
                &run.run_id,
                q_cursor.as_deref(),
                MAX_PAGE_LIMIT,
            )
            .expect("list_run_questions page");
        let page_wire_bytes =
            estimate_wire_response_bytes(&CommandOutput::QuestionHistory(page.clone()))
                .expect("question page wire bytes");
        assert!(
            page_wire_bytes <= MAX_PAGE_RESPONSE_BYTES,
            "question page wire bytes {page_wire_bytes} exceeded {MAX_PAGE_RESPONSE_BYTES}"
        );
        assert!(!page.questions.is_empty());
        collected_questions.extend(page.questions);
        if page.has_more {
            q_cursor = Some(page.next_cursor.expect("next_cursor when has_more"));
        } else {
            assert!(page.next_cursor.is_none());
            break;
        }
    }

    assert_eq!(collected_questions.len() as u64, total_revisions);
    for (idx, q_entry) in collected_questions.iter().enumerate() {
        let expected_rev = (idx as u64) + 1;
        assert_eq!(q_entry.revision, expected_rev);
        assert!(q_entry.untrusted_data);
        if expected_rev == total_revisions {
            assert_eq!(q_entry.status, QuestionStatus::Answered);
            assert_eq!(q_entry.answer.as_deref(), Some(max_answer.as_str()));
        } else {
            assert_eq!(q_entry.status, QuestionStatus::Superseded);
            assert_eq!(q_entry.superseded_by_revision, Some(expected_rev + 1));
        }
    }
}

#[test]
fn m2a_cursor_validation_scope_kind_misuse_and_overflow_rejection() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane_m2a_cursors.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding_a = core.create_binding(UUID_A, "Binding A").expect("b_a");
    let binding_b = core.create_binding(UUID_B, "Binding B").expect("b_b");

    // Create 2 runs in binding A and 2 runs in binding B, each with 2 revisions and 2 questions.
    let mut create_steered_run = |binding_id: &str, prefix: &str, base_ms: i64| -> String {
        let r = core
            .execute(
                binding_id,
                Command::StartDemo {
                    command_key: format!("{prefix}-start"),
                    instruction: format!("{prefix} v1"),
                    step_delay_ms: Some(10),
                    now_ms: base_ms,
                },
            )
            .expect("start")
            .into_run()
            .expect("run");
        core.tick(base_ms + 10).expect("tick 1");
        core.execute(
            binding_id,
            Command::Steer {
                command_key: format!("{prefix}-steer"),
                run_id: r.run_id.clone(),
                expected_revision: 1,
                instruction: format!("{prefix} v2"),
                now_ms: base_ms + 20,
            },
        )
        .expect("steer");
        core.tick(base_ms + 30).expect("tick 2");
        r.run_id
    };

    let run_a1 = create_steered_run(&binding_a.binding_id, "a1", 1_000);
    let run_a2 = create_steered_run(&binding_a.binding_id, "a2", 2_000);
    let run_b1 = create_steered_run(&binding_b.binding_id, "b1", 3_000);
    let _run_b2 = create_steered_run(&binding_b.binding_id, "b2", 4_000);

    // Obtain valid cursors from binding A (limit = 1).
    let runs_page_a = core
        .list_runs(&binding_a.binding_id, None, 1)
        .expect("runs page a");
    let runs_cursor_a = runs_page_a.next_cursor.expect("runs cursor a");

    let revs_page_a1 = core
        .list_run_revisions(&binding_a.binding_id, &run_a1, None, 1)
        .expect("revs page a1");
    let revs_cursor_a1 = revs_page_a1.next_cursor.expect("revs cursor a1");

    let qs_page_a1 = core
        .list_run_questions(&binding_a.binding_id, &run_a1, None, 1)
        .expect("qs page a1");
    let qs_cursor_a1 = qs_page_a1.next_cursor.expect("qs cursor a1");

    // 1. Cross-binding cursor misuse must fail with Validation.
    assert!(matches!(
        core.list_runs(&binding_b.binding_id, Some(&runs_cursor_a), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_revisions(&binding_b.binding_id, &run_b1, Some(&revs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_questions(&binding_b.binding_id, &run_b1, Some(&qs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));

    // 2. Same-binding different-run cursor misuse must fail with Validation.
    assert!(matches!(
        core.list_run_revisions(&binding_a.binding_id, &run_a2, Some(&revs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_questions(&binding_a.binding_id, &run_a2, Some(&qs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));

    // 3. Cross-kind cursor misuse must fail with Validation.
    assert!(matches!(
        core.list_runs(&binding_a.binding_id, Some(&revs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_runs(&binding_a.binding_id, Some(&qs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_revisions(&binding_a.binding_id, &run_a1, Some(&runs_cursor_a), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_revisions(&binding_a.binding_id, &run_a1, Some(&qs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_questions(&binding_a.binding_id, &run_a1, Some(&runs_cursor_a), 10),
        Err(CoreError::Validation { .. })
    ));
    assert!(matches!(
        core.list_run_questions(&binding_a.binding_id, &run_a1, Some(&revs_cursor_a1), 10),
        Err(CoreError::Validation { .. })
    ));

    // 4. Forged, malformed, leading-zero, out-of-order, non-existent anchor, and overflowing cursors.
    let bid = &binding_a.binding_id;
    let bad_runs_cursors = vec![
        "".to_string(),
        "   ".to_string(),
        "x".repeat(MAX_CURSOR_LEN + 1),
        format!("tlc2:runs:{bid}:1:2"),
        format!("tlc1:runs:{bid}:0:2"),
        format!("tlc1:runs:{bid}:1:0"),
        format!("tlc1:runs:{bid}:01:2"),
        format!("tlc1:runs:{bid}:1:02"),
        format!("tlc1:runs:{bid}:2:2"),
        format!("tlc1:runs:{bid}:2:1"),
        format!("tlc1:runs:{bid}:999:1000"),
        format!("tlc1:runs:{bid}:1:999"),
        format!("tlc1:runs:{bid}:1:{}", u64::MAX),
        format!("tlc1:runs:{bid}:1:{}", (i64::MAX as u64) + 1),
        format!("tlc1:runs:{bid}:1:2:extra"),
    ];
    for bad in &bad_runs_cursors {
        assert!(
            matches!(
                core.list_runs(bid, Some(bad), 10),
                Err(CoreError::Validation { .. })
            ),
            "expected invalid runs cursor to fail: {bad}"
        );
    }

    let bad_revs_cursors = vec![
        format!("tlc1:revisions:{bid}:{run_a1}:0:2"),
        format!("tlc1:revisions:{bid}:{run_a1}:1:0"),
        format!("tlc1:revisions:{bid}:{run_a1}:01:2"),
        format!("tlc1:revisions:{bid}:{run_a1}:2:2"),
        format!("tlc1:revisions:{bid}:{run_a1}:2:1"),
        format!("tlc1:revisions:{bid}:{run_a1}:1:99"),
        format!("tlc1:revisions:{bid}:{run_a1}:1:{}", u64::MAX),
        format!("tlc1:revisions:{bid}:{run_a1}:1:{}", (i64::MAX as u64) + 1),
    ];
    for bad in &bad_revs_cursors {
        assert!(
            matches!(
                core.list_run_revisions(bid, &run_a1, Some(bad), 10),
                Err(CoreError::Validation { .. })
            ),
            "expected invalid revisions cursor to fail: {bad}"
        );
    }

    let bad_qs_cursors = vec![
        format!("tlc1:questions:{bid}:{run_a1}:0:2"),
        format!("tlc1:questions:{bid}:{run_a1}:1:0"),
        format!("tlc1:questions:{bid}:{run_a1}:01:2"),
        format!("tlc1:questions:{bid}:{run_a1}:2:2"),
        format!("tlc1:questions:{bid}:{run_a1}:2:1"),
        format!("tlc1:questions:{bid}:{run_a1}:1:999"),
        format!("tlc1:questions:{bid}:{run_a1}:1:{}", u64::MAX),
        format!("tlc1:questions:{bid}:{run_a1}:1:{}", (i64::MAX as u64) + 1),
    ];
    for bad in &bad_qs_cursors {
        assert!(
            matches!(
                core.list_run_questions(bid, &run_a1, Some(bad), 10),
                Err(CoreError::Validation { .. })
            ),
            "expected invalid questions cursor to fail: {bad}"
        );
    }

    // 5. Invalid page limits (0, MAX_PAGE_LIMIT + 1, usize::MAX) must fail with Validation.
    for bad_limit in [0_usize, MAX_PAGE_LIMIT + 1, usize::MAX] {
        assert!(matches!(
            core.list_runs(bid, None, bad_limit),
            Err(CoreError::Validation { .. })
        ));
        assert!(matches!(
            core.list_run_revisions(bid, &run_a1, None, bad_limit),
            Err(CoreError::Validation { .. })
        ));
        assert!(matches!(
            core.list_run_questions(bid, &run_a1, None, bad_limit),
            Err(CoreError::Validation { .. })
        ));
    }
}

#[test]
fn m2a_stable_high_water_mark_window_under_concurrent_appends_and_live_view_semantics() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane_m2a_hwm.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding = core.create_binding(UUID_A, "HWM Binding").expect("binding");
    let bid = &binding.binding_id;

    // Create 4 initial runs r1..r4.
    let mut run_ids = Vec::new();
    for i in 1..=4 {
        let r = core
            .execute(
                bid,
                Command::StartDemo {
                    command_key: format!("hwm-run-{i}"),
                    instruction: format!("Run {i} rev 1"),
                    step_delay_ms: Some(10),
                    now_ms: 1_000 + i * 10,
                },
            )
            .expect("start")
            .into_run()
            .expect("run");
        run_ids.push(r.run_id);
    }
    core.tick(1_100).expect("tick all 4 to waiting");

    // Advance run_ids[0] through 4 revisions and 4 questions so we can page its history too.
    let target_run = run_ids[0].clone();
    for rev in 2..=4 {
        core.execute(
            bid,
            Command::Steer {
                command_key: format!("hwm-steer-{rev}"),
                run_id: target_run.clone(),
                expected_revision: rev - 1,
                instruction: format!("Run 1 rev {rev}"),
                now_ms: 1_100 + (rev as i64) * 20,
            },
        )
        .expect("steer target");
        core.tick(1_110 + (rev as i64) * 20).expect("tick target");
    }

    // Fetch Page 1 (limit = 2) of runs, revisions, and questions (ascending creation/revision order).
    let runs_p1 = core.list_runs(bid, None, 2).expect("runs p1");
    assert_eq!(runs_p1.runs.len(), 2);
    assert_eq!(runs_p1.runs[0].run_id, run_ids[0]);
    assert_eq!(runs_p1.runs[1].run_id, run_ids[1]);
    assert!(runs_p1.has_more);
    let runs_cursor = runs_p1.next_cursor.expect("runs cursor");

    let revs_p1 = core
        .list_run_revisions(bid, &target_run, None, 2)
        .expect("revs p1");
    assert_eq!(revs_p1.revisions.len(), 2);
    assert_eq!(revs_p1.revisions[0].revision, 1);
    assert_eq!(revs_p1.revisions[1].revision, 2);
    assert!(revs_p1.has_more);
    let revs_cursor = revs_p1.next_cursor.expect("revs cursor");

    let qs_p1 = core
        .list_run_questions(bid, &target_run, None, 2)
        .expect("qs p1");
    assert_eq!(qs_p1.questions.len(), 2);
    assert_eq!(qs_p1.questions[0].revision, 1);
    assert_eq!(qs_p1.questions[1].revision, 2);
    assert!(qs_p1.has_more);
    let qs_cursor = qs_p1.next_cursor.expect("qs cursor");

    // Now append new runs (r5, r6), steer target_run to rev 5 & 6 (which supersedes rev 4's question
    // and creates questions for rev 5 & 6), and cancel run_ids[2] (r3) which will appear on Page 2!
    for i in 5..=6 {
        let r = core
            .execute(
                bid,
                Command::StartDemo {
                    command_key: format!("hwm-run-{i}"),
                    instruction: format!("Run {i} rev 1"),
                    step_delay_ms: Some(10),
                    now_ms: 2_000 + i * 10,
                },
            )
            .expect("start new run")
            .into_run()
            .expect("new run");
        run_ids.push(r.run_id);
    }
    for rev in 5..=6 {
        core.execute(
            bid,
            Command::Steer {
                command_key: format!("hwm-steer-{rev}"),
                run_id: target_run.clone(),
                expected_revision: rev - 1,
                instruction: format!("Run 1 rev {rev}"),
                now_ms: 2_100 + (rev as i64) * 20,
            },
        )
        .expect("steer target new");
        core.tick(2_110 + (rev as i64) * 20)
            .expect("tick target new");
    }
    core.execute(
        bid,
        Command::Cancel {
            command_key: "hwm-cancel-r3".to_string(),
            run_id: run_ids[2].clone(),
            reason: Some("cancelled between pages".to_string()),
            now_ms: 2_300,
        },
    )
    .expect("cancel r3");

    // Fetch Page 2 using the cursors from Page 1:
    // - Membership must be strictly pinned to the initial high-water-mark window (r3, r4; rev 3, 4; q 3, 4)
    //   with NO duplicates and NO missing rows.
    // - Mutable status fields reflect live state (e.g. r3 is Cancelled, rev 4 is superseded, q 4 is Superseded).
    let runs_p2 = core
        .list_runs(bid, Some(&runs_cursor), 10)
        .expect("runs p2");
    assert_eq!(runs_p2.runs.len(), 2);
    assert_eq!(runs_p2.runs[0].run_id, run_ids[2]);
    assert_eq!(runs_p2.runs[0].status, RunStatus::Cancelled);
    assert_eq!(runs_p2.runs[1].run_id, run_ids[3]);
    assert_eq!(runs_p2.runs[1].status, RunStatus::WaitingInput);
    assert!(!runs_p2.has_more);
    assert!(runs_p2.next_cursor.is_none());

    let revs_p2 = core
        .list_run_revisions(bid, &target_run, Some(&revs_cursor), 10)
        .expect("revs p2");
    assert_eq!(revs_p2.revisions.len(), 2);
    assert_eq!(revs_p2.revisions[0].revision, 3);
    assert_eq!(revs_p2.revisions[1].revision, 4);
    assert!(revs_p2.revisions[1].superseded_at_ms.is_some());
    assert!(!revs_p2.has_more);
    assert!(revs_p2.next_cursor.is_none());

    let qs_p2 = core
        .list_run_questions(bid, &target_run, Some(&qs_cursor), 10)
        .expect("qs p2");
    assert_eq!(qs_p2.questions.len(), 2);
    assert_eq!(qs_p2.questions[0].revision, 3);
    assert_eq!(qs_p2.questions[1].revision, 4);
    assert_eq!(qs_p2.questions[1].status, QuestionStatus::Superseded);
    assert_eq!(qs_p2.questions[1].superseded_by_revision, Some(5));
    assert!(!qs_p2.has_more);
    assert!(qs_p2.next_cursor.is_none());

    // Starting a fresh Page 1 (cursor: None) now includes the newly appended items (6 runs, 6 revisions, 6 questions).
    let fresh_runs = core.list_runs(bid, None, 10).expect("fresh runs");
    assert_eq!(fresh_runs.runs.len(), 6);
    assert_eq!(fresh_runs.runs[5].run_id, run_ids[5]);

    let fresh_revs = core
        .list_run_revisions(bid, &target_run, None, 10)
        .expect("fresh revs");
    assert_eq!(fresh_revs.revisions.len(), 6);
    assert_eq!(fresh_revs.revisions[5].revision, 6);

    let fresh_qs = core
        .list_run_questions(bid, &target_run, None, 10)
        .expect("fresh qs");
    assert_eq!(fresh_qs.questions.len(), 6);
    assert_eq!(fresh_qs.questions[5].revision, 6);
}

#[test]
fn m2a_multi_run_bounded_status_counts_and_paged_list_runs() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane_m2a_status.db");
    let mut core = Core::open(&db_path).expect("open");

    let binding = core
        .create_binding(UUID_A, "Multi-Run Status")
        .expect("binding");
    let bid = &binding.binding_id;

    let total_runs = 65_usize;
    let long_instruction = "L".repeat(300);
    let mut created_ids = Vec::with_capacity(total_runs);

    for idx in 0..total_runs {
        let r = core
            .execute(
                bid,
                Command::StartDemo {
                    command_key: format!("status-run-{idx}"),
                    instruction: format!("Run-{idx:02}-{long_instruction}"),
                    step_delay_ms: Some(10),
                    now_ms: 1_000 + (idx as i64),
                },
            )
            .expect("start")
            .into_run()
            .expect("run");
        created_ids.push(r.run_id);
    }

    // Cancel 15 runs so 50 remain active (`Running`).
    for (idx, run_id) in created_ids.iter().take(15).enumerate() {
        core.execute(
            bid,
            Command::Cancel {
                command_key: format!("status-cancel-{idx}"),
                run_id: run_id.clone(),
                reason: Some("done".to_string()),
                now_ms: 2_000 + (idx as i64),
            },
        )
        .expect("cancel");
    }

    let (count_total, count_active, recent) = core
        .get_status_run_summaries(bid, MAX_STATUS_RECENT_RUNS)
        .expect("get_status_run_summaries");
    assert_eq!(count_total, 65);
    assert_eq!(count_active, 50);
    assert_eq!(recent.len(), MAX_STATUS_RECENT_RUNS);
    // Status recent_runs are newest first: indices 64 down to 55.
    for (offset, summary) in recent.iter().enumerate() {
        let expected_idx = total_runs - 1 - offset;
        assert_eq!(summary.run_id, created_ids[expected_idx]);
        assert!(summary.instruction_truncated);
        assert!(summary.instruction_preview.ends_with("..."));
        assert_eq!(
            summary.instruction_preview.chars().count(),
            MAX_SUMMARY_TEXT_PREVIEW_CHARS + 3
        );
        assert!(summary.untrusted_data);
    }

    // Page through all 65 runs via `list_runs` with `limit = 25` (25 + 25 + 15 = 65, ascending creation order).
    let mut paged_ids = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_lengths = Vec::new();
    loop {
        let page = core
            .list_runs(bid, cursor.as_deref(), 25)
            .expect("list_runs page");
        page_lengths.push(page.runs.len());
        paged_ids.extend(page.runs.into_iter().map(|r| r.run_id));
        if page.has_more {
            cursor = Some(page.next_cursor.expect("next_cursor"));
        } else {
            assert!(page.next_cursor.is_none());
            break;
        }
    }
    assert_eq!(page_lengths, vec![25, 25, 15]);
    assert_eq!(paged_ids, created_ids);
}

#[test]
fn m2a_compact_idempotency_original_snapshot_replay_and_legacy_v1_migration() {
    const LEGACY_V1_VERSION: u32 = 1;

    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("threadlane_m2a_dedup.db");

    // Part 1: Verify that retrying `start_demo`, `steer`, `answer`, and `cancel` returns the
    // ORIGINAL mutation snapshot (not today's mutated state) both in-memory and across reopen.
    let (binding_id, run_1_id, q1_id) = {
        let mut core = Core::open(&db_path).expect("open");
        let b = core.create_binding(UUID_A, "Dedup Replay").expect("b");

        let started = core
            .execute(
                &b.binding_id,
                Command::StartDemo {
                    command_key: "key-start-1".to_string(),
                    instruction: "Original v1 instruction".to_string(),
                    step_delay_ms: Some(10),
                    now_ms: 1_000,
                },
            )
            .expect("start")
            .into_run()
            .expect("run");
        assert_eq!(started.revision, 1);
        assert_eq!(started.status, RunStatus::Running);

        core.tick(1_010).expect("tick 1");
        let waiting_v1 = core
            .get_run(&b.binding_id, &started.run_id)
            .expect("waiting v1");
        let q1_id = waiting_v1.active_question_id.clone().expect("q1");

        let steered = core
            .execute(
                &b.binding_id,
                Command::Steer {
                    command_key: "key-steer-2".to_string(),
                    run_id: started.run_id.clone(),
                    expected_revision: 1,
                    instruction: "Steered v2 instruction".to_string(),
                    now_ms: 1_020,
                },
            )
            .expect("steer")
            .into_run()
            .expect("steered");
        assert_eq!(steered.revision, 2);
        assert_eq!(steered.status, RunStatus::Running);

        core.tick(1_030).expect("tick 2");
        let waiting_v2 = core
            .get_run(&b.binding_id, &started.run_id)
            .expect("waiting v2");
        let q2_id = waiting_v2.active_question_id.clone().expect("q2");

        let answered = core
            .execute(
                &b.binding_id,
                Command::Answer {
                    command_key: "key-answer-2".to_string(),
                    run_id: started.run_id.clone(),
                    question_id: q2_id.clone(),
                    expected_revision: 2,
                    answer: "Approved v2".to_string(),
                    now_ms: 1_040,
                },
            )
            .expect("answer")
            .into_run()
            .expect("answered");
        assert_eq!(answered.revision, 2);
        assert_eq!(answered.status, RunStatus::Completing);

        core.tick(1_050).expect("tick 3 to completed");
        let done = core.get_run(&b.binding_id, &started.run_id).expect("done");
        assert_eq!(done.status, RunStatus::Completed);

        // Retry `key-start-1`, `key-steer-2`, and `key-answer-2` now that the run is Completed:
        // each retry must return its ORIGINAL mutation snapshot!
        let replay_start = core
            .execute(
                &b.binding_id,
                Command::StartDemo {
                    command_key: "key-start-1".to_string(),
                    instruction: "Original v1 instruction".to_string(),
                    step_delay_ms: Some(10),
                    now_ms: 9_999,
                },
            )
            .expect("replay start")
            .into_run()
            .expect("replay start run");
        assert_eq!(replay_start.run_id, started.run_id);
        assert_eq!(replay_start.revision, 1);
        assert_eq!(replay_start.status, RunStatus::Running);
        assert_eq!(replay_start.instruction, "Original v1 instruction");

        let replay_steer = core
            .execute(
                &b.binding_id,
                Command::Steer {
                    command_key: "key-steer-2".to_string(),
                    run_id: started.run_id.clone(),
                    expected_revision: 1,
                    instruction: "Steered v2 instruction".to_string(),
                    now_ms: 9_999,
                },
            )
            .expect("replay steer")
            .into_run()
            .expect("replay steer run");
        assert_eq!(replay_steer.revision, 2);
        assert_eq!(replay_steer.status, RunStatus::Running);
        assert_eq!(replay_steer.instruction, "Steered v2 instruction");

        let replay_answer = core
            .execute(
                &b.binding_id,
                Command::Answer {
                    command_key: "key-answer-2".to_string(),
                    run_id: started.run_id.clone(),
                    question_id: q2_id,
                    expected_revision: 2,
                    answer: "Approved v2".to_string(),
                    now_ms: 9_999,
                },
            )
            .expect("replay answer")
            .into_run()
            .expect("replay answer run");
        assert_eq!(replay_answer.revision, 2);
        assert_eq!(replay_answer.status, RunStatus::Completing);

        (b.binding_id, started.run_id, q1_id)
    };

    // Part 2: Simulate a legacy v1 SQLite database (`schema_meta.version = 1`) where multiple
    // `command_dedup` rows (`key-start-1` and `key-steer-2`) contain legacy v1 `CommandOutput::Run`
    // JSON with 150 `revision_history` and 150 `questions` entries (> 130 KiB each), while the live
    // `runs` row has already moved on to Completed.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open raw sqlite");
        conn.execute(
            "UPDATE schema_meta SET version = ?1 WHERE id = 1",
            rusqlite::params![LEGACY_V1_VERSION],
        )
        .expect("downgrade schema_meta to v1");

        let mut legacy_revisions = Vec::new();
        let mut legacy_questions = Vec::new();
        for rev in 1..=150_u64 {
            legacy_revisions.push(serde_json::json!({
                "revision": rev,
                "instruction": format!("Legacy revision {rev} {}", "R".repeat(400)),
                "created_at_ms": 1_000 + (rev as i64),
                "superseded_at_ms": if rev < 150 { Some(1_001 + (rev as i64)) } else { None },
            }));
            let qid = if rev == 1 {
                q1_id.clone()
            } else {
                format!("legacy-q-{rev}")
            };
            legacy_questions.push(serde_json::json!({
                "question_id": qid,
                "binding_id": binding_id,
                "run_id": run_1_id,
                "revision": rev,
                "status": if rev == 1 { "pending" } else { "superseded" },
                "prompt": format!("Legacy question {rev} {}", "Q".repeat(400)),
                "answer": serde_json::Value::Null,
                "superseded_by_revision": if rev == 1 { None } else { Some(rev + 1) },
                "created_at_ms": 1_000 + (rev as i64),
                "resolved_at_ms": serde_json::Value::Null,
            }));
        }

        let legacy_v1_start_output = serde_json::json!({
            "kind": "run",
            "data": {
                "run_id": run_1_id,
                "binding_id": binding_id,
                "revision": 1,
                "status": "waiting_input",
                "instruction": "Original v1 instruction",
                "step_seq": 3,
                "step_delay_ms": 10,
                "next_step_at_ms": serde_json::Value::Null,
                "active_question_id": q1_id,
                "started_at_ms": 1_000,
                "updated_at_ms": 1_010,
                "completed_at_ms": serde_json::Value::Null,
                "cancelled_at_ms": serde_json::Value::Null,
                "cancel_reason": serde_json::Value::Null,
                "revision_history": legacy_revisions,
                "questions": legacy_questions,
            }
        });
        let legacy_start_json =
            serde_json::to_string(&legacy_v1_start_output).expect("serialize legacy v1 start");
        assert!(legacy_start_json.len() > MAX_COMPACT_RESPONSE_BYTES);

        let legacy_v1_steer_output = serde_json::json!({
            "kind": "run",
            "data": {
                "run_id": run_1_id,
                "binding_id": binding_id,
                "revision": 2,
                "status": "running",
                "instruction": "Steered v2 instruction",
                "step_seq": 4,
                "step_delay_ms": 10,
                "next_step_at_ms": 1_030,
                "active_question_id": serde_json::Value::Null,
                "started_at_ms": 1_000,
                "updated_at_ms": 1_020,
                "completed_at_ms": serde_json::Value::Null,
                "cancelled_at_ms": serde_json::Value::Null,
                "cancel_reason": serde_json::Value::Null,
                "revision_history": legacy_revisions,
                "questions": legacy_questions,
            }
        });
        let legacy_steer_json =
            serde_json::to_string(&legacy_v1_steer_output).expect("serialize legacy v1 steer");
        assert!(legacy_steer_json.len() > MAX_COMPACT_RESPONSE_BYTES);

        conn.execute(
            "UPDATE command_dedup SET response_json = ?1 WHERE binding_id = ?2 AND command_key = 'key-start-1'",
            rusqlite::params![legacy_start_json, binding_id],
        )
        .expect("inject legacy v1 start dedup json");
        conn.execute(
            "UPDATE command_dedup SET response_json = ?1 WHERE binding_id = ?2 AND command_key = 'key-steer-2'",
            rusqlite::params![legacy_steer_json, binding_id],
        )
        .expect("inject legacy v1 steer dedup json");
    }

    // Reopen with `Core::open`: v1 -> v2 migration must compact all legacy `command_dedup` rows
    // in-place and bump `schema_meta.version` to `SCHEMA_VERSION` (2).
    let mut reopened = Core::open(&db_path).expect("reopen and migrate v1 -> v2");
    {
        let conn = rusqlite::Connection::open(&db_path).expect("inspect migrated db");
        let version: i64 = conn
            .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |r| {
                r.get(0)
            })
            .expect("read schema version");
        assert_eq!(version, i64::from(SCHEMA_VERSION));

        for key in ["key-start-1", "key-steer-2"] {
            let migrated_json: String = conn
                .query_row(
                    "SELECT response_json FROM command_dedup WHERE binding_id = ?1 AND command_key = ?2",
                    rusqlite::params![binding_id, key],
                    |r| r.get(0),
                )
                .expect("read migrated response_json");
            assert!(!migrated_json.contains("revision_history"));
            assert!(!migrated_json.contains("\"questions\""));
            assert!(migrated_json.len() < 4_096);
        }
    }

    // Replay `key-start-1` and `key-steer-2` after migration: must return their compacted original
    // snapshots (including reconstructed `active_question` on `key-start-1`) within
    // `MAX_COMPACT_RESPONSE_BYTES`, while payload mismatch still fails with `IdempotencyConflict`.
    let migrated_replay_out = reopened
        .execute(
            &binding_id,
            Command::StartDemo {
                command_key: "key-start-1".to_string(),
                instruction: "Original v1 instruction".to_string(),
                step_delay_ms: Some(10),
                now_ms: 20_000,
            },
        )
        .expect("replay migrated key-start-1");
    assert!(
        estimate_wire_response_bytes(&migrated_replay_out).expect("wire bytes")
            <= MAX_COMPACT_RESPONSE_BYTES
    );
    let migrated_run = migrated_replay_out.into_run().expect("migrated run");
    assert_eq!(migrated_run.run_id, run_1_id);
    assert_eq!(migrated_run.revision, 1);
    assert_eq!(migrated_run.status, RunStatus::WaitingInput);
    assert!(migrated_run.untrusted_data);
    let migrated_active_q = migrated_run
        .active_question
        .expect("reconstructed active_question from legacy v1 questions array");
    assert_eq!(migrated_active_q.question_id, q1_id);
    assert_eq!(migrated_active_q.revision, 1);
    assert!(migrated_active_q.untrusted_data);

    let migrated_steer_out = reopened
        .execute(
            &binding_id,
            Command::Steer {
                command_key: "key-steer-2".to_string(),
                run_id: run_1_id.clone(),
                expected_revision: 1,
                instruction: "Steered v2 instruction".to_string(),
                now_ms: 20_000,
            },
        )
        .expect("replay migrated key-steer-2")
        .into_run()
        .expect("migrated steer run");
    assert_eq!(migrated_steer_out.revision, 2);
    assert_eq!(migrated_steer_out.status, RunStatus::Running);
    assert_eq!(migrated_steer_out.active_question, None);

    let conflict_err = reopened
        .execute(
            &binding_id,
            Command::StartDemo {
                command_key: "key-start-1".to_string(),
                instruction: "Different instruction".to_string(),
                step_delay_ms: Some(10),
                now_ms: 20_001,
            },
        )
        .expect_err("payload mismatch must still fail after v1->v2 migration");
    assert!(matches!(
        conflict_err,
        CoreError::IdempotencyConflict { .. }
    ));

    drop(reopened);

    // Part 3: Verify v2 reopen does NOT rescan `command_dedup` rows on every `Core::open`.
    // Inject a marker into an uncalled `command_dedup` row while `schema_meta.version == 2`:
    // reopening a v2 database must not rewrite or scan `command_dedup`.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("open raw sqlite v2");
        conn.execute(
            "INSERT INTO command_dedup (binding_id, command_key, operation, payload_hash, response_json, created_at_ms)
             VALUES (?1, 'uncalled-v2-marker', 'start_demo', 'deadbeef', '{\"unscanned_v2_marker\":true}', 30000)",
            rusqlite::params![binding_id],
        )
        .expect("insert uncalled v2 marker row");
    }
    let reopened_v2 = Core::open(&db_path).expect("v2 reopen must not rescan command_dedup");
    let _ = reopened_v2
        .get_run(&binding_id, &run_1_id)
        .expect("get_run on v2 reopen");
}
