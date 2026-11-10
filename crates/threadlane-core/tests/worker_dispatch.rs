use tempfile::TempDir;
use threadlane_core::{
    estimate_wire_response_bytes, Command, CommandOutput, Core, CoreError, EventKind, RunStatus,
    WakeupOutcome, WakeupStatus, WorkerAcceptanceState, WorkerCompletionRecord,
    WorkerExecutionBinding, WorkerOutcomeSummary, WorkerProgressUpdate, WorkerStatus,
    WorkerWorktreeUpdate, FIXED_WAKEUP_MESSAGE, MAX_COMPACT_RESPONSE_BYTES, MAX_INSTRUCTION_LEN,
    MAX_WORKER_CHANGED_PATHS_PREVIEW, MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT,
    MAX_WORKER_SUMMARY_TEXT_BYTES, MAX_WORKER_VIOLATION_SUMMARIES, SCHEMA_VERSION,
    WORKER_EXECUTION_BINDING_SCHEMA_VERSION,
};

const UUID_A: &str = "11111111-1111-4111-8111-111111111111";
const UUID_B: &str = "22222222-2222-4222-8222-222222222222";
const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn sample_worktree_update(run_id: &str, worker_id: &str) -> WorkerWorktreeUpdate {
    WorkerWorktreeUpdate {
        run_worktree_id: format!("run_{run_id}"),
        run_worktree_path: format!("/tmp/work-area/runs/run_{run_id}"),
        run_branch_name: format!("threadlane/run/{run_id}"),
        task_worktree_id: format!("task_{run_id}_{worker_id}"),
        task_worktree_path: format!("/tmp/work-area/tasks/task_{run_id}_{worker_id}"),
        task_branch_name: format!("threadlane/task/{run_id}/{worker_id}"),
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
    }
}

fn sample_ready_outcome() -> WorkerOutcomeSummary {
    WorkerOutcomeSummary {
        provider_retry_high_water: None,
        provider_health: None,
        exit_code: Some(0),
        signal: None,
        duration_ms: Some(420),
        timed_out: false,
        cancelled: false,
        cancel_requested_before_completion: false,
        missing_model_evidence: false,
        missing_effort_evidence: false,
        provider_error: false,
        incomplete_cleanup: false,
        rejected_by_diff_gate: false,
        stdout_truncated: false,
        stderr_truncated: false,
        acceptance_state: WorkerAcceptanceState::ReadyForReview,
        changed_paths_count: 1,
        changed_paths_preview: vec!["src/lib.rs".to_string()],
        violation_count: 0,
        violation_summaries: Vec::new(),
        final_summary: Some("Implemented requested module changes.".to_string()),
        failure_reason: None,
        report_artifact_id: Some("art_sample_01.json".to_string()),
    }
}

#[test]
fn worker_lifecycle_start_claim_progress_complete_and_wake_ack() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_lifecycle.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Worker Lifecycle")
        .expect("create binding");
    let bid = &binding.binding_id;

    // 1. Admit and queue worker via Command::StartWorker.
    let start_out = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "wk-start-1".to_string(),
                profile_id: "codex-default".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Implement worker dispatch ledger".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("start_worker");
    assert!(
        estimate_wire_response_bytes(&start_out).expect("wire bytes") <= MAX_COMPACT_RESPONSE_BYTES
    );
    let queued = start_out.into_worker().expect("into_worker");
    assert!(queued.untrusted_data);
    assert_eq!(queued.status, WorkerStatus::Queued);
    assert_eq!(queued.revision, 1);
    assert_eq!(queued.profile_id, "codex-default");
    assert_eq!(queued.profile_digest, DIGEST_A);
    assert_eq!(queued.attempt_count, 0);
    assert!(queued.claim_id.is_none());

    // Routine WorkerQueued event must not trigger a wake-up notice.
    let wakeup = core.get_wakeup(bid).expect("wakeup after queue");
    assert_eq!(wakeup.status, WakeupStatus::Idle);

    // Linked run is visible in get_run and get_status_run_summaries as Queued (active).
    let run_snap = core.get_run(bid, &queued.run_id).expect("get_run");
    assert_eq!(run_snap.status, RunStatus::Queued);
    let (total_runs, active_runs, recent) = core
        .get_status_run_summaries(bid, 10)
        .expect("status summaries");
    assert_eq!(total_runs, 1);
    assert_eq!(active_runs, 1);
    assert_eq!(recent[0].status, RunStatus::Queued);

    // Lookup via get_worker works by both run_id and worker_id.
    let by_run = core.get_worker(bid, &queued.run_id).expect("by run_id");
    let by_worker = core
        .get_worker(bid, &queued.worker_id)
        .expect("by worker_id");
    assert_eq!(by_run, queued);
    assert_eq!(by_worker, queued);

    // 2. Claim the queued worker.
    let queued_list = core.list_queued_workers(10).expect("list_queued_workers");
    assert_eq!(queued_list.len(), 1);
    assert_eq!(queued_list[0].worker_id, queued.worker_id);

    let claim = core
        .claim_worker(bid, &queued.run_id, 1, DIGEST_A, 1_010)
        .expect("claim_worker");
    assert_eq!(claim.worker_id, queued.worker_id);
    assert_eq!(claim.run_id, queued.run_id);
    assert_eq!(claim.revision, 1);
    assert_eq!(claim.attempt_count, 1);
    assert_eq!(claim.profile_digest, DIGEST_A);
    assert!(core
        .list_queued_workers(10)
        .expect("queued empty")
        .is_empty());

    // 3. Record running state with owned per-run and per-task worktrees.
    let running = core
        .record_worker_running(
            &claim,
            sample_worktree_update(&queued.run_id, &queued.worker_id),
            1_020,
        )
        .expect("record_worker_running");
    assert_eq!(running.status, WorkerStatus::Running);
    assert_eq!(running.started_at_ms, Some(1_020));
    assert_eq!(
        running.worktree.run_worktree_id.as_deref(),
        Some(format!("run_{}", queued.run_id).as_str())
    );
    assert_eq!(
        running.worktree.task_worktree_id.as_deref(),
        Some(format!("task_{}_{}", queued.run_id, queued.worker_id).as_str())
    );

    // 4. Record live progress before completion; routine progress must not wake Codex.
    let progressed = core
        .record_worker_progress(
            &claim,
            &[
                WorkerProgressUpdate {
                    provider_retry_high_water: None,
                    provider_health: None,
                    watchdog_attention: None,
                    event_kind: "system_init".to_string(),
                    summary: "Initialized provider session".to_string(),
                },
                WorkerProgressUpdate {
                    provider_retry_high_water: None,
                    provider_health: None,
                    watchdog_attention: None,
                    event_kind: "assistant_message".to_string(),
                    summary: "Inspecting workspace files".to_string(),
                },
            ],
            1_030,
        )
        .expect("record_worker_progress");
    assert_eq!(progressed.progress.durable_progress_events, 2);
    assert_eq!(progressed.progress.coalesced_progress_events, 0);
    assert_eq!(
        progressed.progress.last_event_kind.as_deref(),
        Some("assistant_message")
    );
    assert_eq!(
        progressed.progress.last_summary.as_deref(),
        Some("Inspecting workspace files")
    );
    assert_eq!(
        core.get_wakeup(bid).expect("wakeup during progress").status,
        WakeupStatus::Idle
    );

    // 5. Complete the worker; terminal WorkerCompleted event is important and wakes the binding.
    let completed = core
        .record_worker_outcome(
            &claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: Some("fedcba9876543210fedcba9876543210fedcba98".to_string()),
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            1_050,
        )
        .expect("record_worker_outcome");
    assert_eq!(completed.status, WorkerStatus::Completed);
    assert_eq!(completed.observed_model.as_deref(), Some("gpt-5.4"));
    assert_eq!(completed.observed_effort.as_deref(), Some("high"));
    assert_eq!(
        completed.worktree.head_commit.as_deref(),
        Some("fedcba9876543210fedcba9876543210fedcba98")
    );
    let outcome = completed.outcome.as_ref().expect("outcome present");
    assert_eq!(
        outcome.acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );
    assert!(!outcome.cancel_requested_before_completion);

    // Never sets an `accepted` flag in serialized JSON.
    let completed_json = serde_json::to_string(&completed).expect("serialize worker");
    assert!(!completed_json.contains("\"accepted\""));

    // Active run count drops to 0 once terminal.
    let (total_after, active_after, _) = core
        .get_status_run_summaries(bid, 10)
        .expect("summaries after complete");
    assert_eq!(total_after, 1);
    assert_eq!(active_after, 0);

    // Important WorkerCompleted event scheduled a pending wake-up notice with fixed text.
    let wake_claim = core
        .claim_wakeup_for_binding(bid, 1_060)
        .expect("claim wakeup")
        .expect("wake claim present");
    assert_eq!(wake_claim.message, FIXED_WAKEUP_MESSAGE);
    assert_eq!(wake_claim.coalesced_count, 1);
    core.record_wakeup_result(&wake_claim.claim_id, WakeupOutcome::Submitted, 1_065)
        .expect("record wakeup submitted");

    // Pull and acknowledge the event batch; wake-up notice returns to Idle.
    let batch = core
        .execute(
            bid,
            Command::PullEvents {
                after_seq: None,
                limit: 20,
                now_ms: 1_070,
            },
        )
        .expect("pull_events")
        .into_event_batch()
        .expect("event batch");
    let kinds: Vec<EventKind> = batch.events.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            EventKind::WorkerQueued,
            EventKind::WorkerStarted,
            EventKind::WorkerProgress,
            EventKind::WorkerProgress,
            EventKind::WorkerCompleted,
        ]
    );
    let ack = core
        .execute(
            bid,
            Command::AcknowledgeEvents {
                command_key: "ack-worker-1".to_string(),
                batch_id: batch.batch_id,
                now_ms: 1_080,
            },
        )
        .expect("ack")
        .into_ack_result()
        .expect("ack result");
    assert!(ack.advanced);
    assert_eq!(ack.wakeup_status, WakeupStatus::Idle);
}

#[test]
fn worker_demo_isolation_and_refusal_of_answer_steer_and_generic_cancel() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_demo_refusal.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Refusal Binding")
        .expect("binding");
    let bid = &binding.binding_id;

    // Start a worker run and a demo run side-by-side on the same binding.
    let worker = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-1".to_string(),
                profile_id: "prof-a".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "claude_code".to_string(),
                requested_model: "claude-sonnet-4-6".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Worker task".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("start_worker")
        .into_worker()
        .expect("worker");

    let demo = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "d-1".to_string(),
                instruction: "Demo task".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_000,
            },
        )
        .expect("start_demo")
        .into_run()
        .expect("demo");

    // Claim and start the worker so its status is Running.
    let claim = core
        .claim_worker(bid, &worker.run_id, 1, DIGEST_A, 1_005)
        .expect("claim");
    core.record_worker_running(
        &claim,
        sample_worktree_update(&worker.run_id, &worker.worker_id),
        1_006,
    )
    .expect("running");

    // `tick` must advance ONLY the demo run and never touch the Running worker run.
    let tick_report = core.tick(2_000).expect("tick");
    assert_eq!(tick_report.advanced_runs, vec![demo.run_id.clone()]);

    let worker_after_tick = core
        .get_worker(bid, &worker.run_id)
        .expect("worker after tick");
    assert_eq!(worker_after_tick.status, WorkerStatus::Running);
    assert_eq!(worker_after_tick.revision, 1);

    // Calling Steer, Answer, or generic demo Cancel on the worker run must fail with
    // InvalidRunState BEFORE any mutation.
    assert!(matches!(
        core.execute(
            bid,
            Command::Steer {
                command_key: "forbid-steer".to_string(),
                run_id: worker.run_id.clone(),
                expected_revision: 1,
                instruction: "Attempted steer".to_string(),
                now_ms: 2_010,
            },
        ),
        Err(CoreError::InvalidRunState {
            action: "steer",
            ..
        })
    ));

    assert!(matches!(
        core.execute(
            bid,
            Command::Answer {
                command_key: "forbid-answer".to_string(),
                run_id: worker.run_id.clone(),
                question_id: "qst_fake".to_string(),
                expected_revision: 1,
                answer: "Attempted answer".to_string(),
                now_ms: 2_011,
            },
        ),
        Err(CoreError::InvalidRunState {
            action: "answer",
            ..
        })
    ));

    assert!(matches!(
        core.execute(
            bid,
            Command::Cancel {
                command_key: "forbid-generic-cancel".to_string(),
                run_id: worker.run_id.clone(),
                reason: Some("wrong cancel tool".to_string()),
                now_ms: 2_012,
            },
        ),
        Err(CoreError::InvalidRunState {
            action: "cancel",
            ..
        })
    ));

    // Conversely, calling CancelWorker on the demo run must also fail with InvalidRunState.
    assert!(matches!(
        core.execute(
            bid,
            Command::CancelWorker {
                command_key: "forbid-cancel-worker-on-demo".to_string(),
                run_id: demo.run_id.clone(),
                expected_revision: 1,
                reason: None,
                now_ms: 2_013,
            },
        ),
        Err(CoreError::InvalidRunState {
            action: "cancel_worker",
            ..
        })
    ));

    // Verify the worker run was not mutated at all by any of the rejected commands.
    let unchanged = core.get_worker(bid, &worker.run_id).expect("unchanged");
    assert_eq!(unchanged, worker_after_tick);
}

#[test]
fn worker_idempotency_changed_profile_conflict_and_binding_isolation() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_dedup.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding_a = core.create_binding(UUID_A, "Binding A").expect("a");
    let binding_b = core.create_binding(UUID_B, "Binding B").expect("b");
    let bid_a = &binding_a.binding_id;
    let bid_b = &binding_b.binding_id;

    let initial = core
        .execute(
            bid_a,
            Command::StartWorker {
                command_key: "start-key-1".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Original worker instruction".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("initial start")
        .into_worker()
        .expect("worker");
    assert_eq!(initial.status, WorkerStatus::Queued);

    // Claim and complete the worker so its live status moves to Completed.
    let claim = core
        .claim_worker(bid_a, &initial.run_id, 1, DIGEST_A, 1_010)
        .expect("claim");
    core.record_worker_outcome(
        &claim,
        WorkerCompletionRecord {
            status: WorkerStatus::Completed,
            observed_model: Some("gpt-5.4".to_string()),
            observed_effort: Some("high".to_string()),
            head_commit: None,
            dropped_live_events: 0,
            outcome: sample_ready_outcome(),
        },
        1_020,
    )
    .expect("complete");

    // Retrying StartWorker with the same command_key and same (profile_id, profile_digest, instruction)
    // must return the ORIGINAL Queued snapshot and never queue a second worker.
    let replay = core
        .execute(
            bid_a,
            Command::StartWorker {
                command_key: "start-key-1".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Original worker instruction".to_string(),
                now_ms: 9_999,
            },
        )
        .expect("replay start")
        .into_worker()
        .expect("replay worker");
    assert_eq!(replay, initial);
    assert!(core
        .list_queued_workers(10)
        .expect("no duplicate queued")
        .is_empty());

    // Reusing `start-key-1` with a DIFFERENT profile_digest (simulating changed profile config!)
    // must fail with IdempotencyConflict.
    assert!(matches!(
        core.execute(
            bid_a,
            Command::StartWorker {
                command_key: "start-key-1".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_B.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Original worker instruction".to_string(),
                now_ms: 10_000,
            },
        ),
        Err(CoreError::IdempotencyConflict { .. })
    ));

    // Reusing `start-key-1` with a different instruction or different operation also conflicts.
    assert!(matches!(
        core.execute(
            bid_a,
            Command::StartWorker {
                command_key: "start-key-1".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Modified instruction".to_string(),
                now_ms: 10_001,
            },
        ),
        Err(CoreError::IdempotencyConflict { .. })
    ));

    // Binding B cannot read or cancel Binding A's worker, and can use `start-key-1` independently.
    assert!(matches!(
        core.get_worker(bid_b, &initial.run_id),
        Err(CoreError::WorkerNotFound { .. })
    ));
    assert!(matches!(
        core.execute(
            bid_b,
            Command::CancelWorker {
                command_key: "cancel-cross".to_string(),
                run_id: initial.run_id.clone(),
                expected_revision: 1,
                reason: None,
                now_ms: 10_002,
            },
        ),
        Err(CoreError::WorkerNotFound { .. })
    ));

    let b_worker = core
        .execute(
            bid_b,
            Command::StartWorker {
                command_key: "start-key-1".to_string(),
                profile_id: "prof-b".to_string(),
                profile_digest: DIGEST_B.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "medium".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Binding B instruction".to_string(),
                now_ms: 10_003,
            },
        )
        .expect("binding B start with same key")
        .into_worker()
        .expect("b_worker");
    assert_ne!(b_worker.worker_id, initial.worker_id);
}

#[test]
fn worker_atomic_claim_correlation_and_stale_callback_rejection() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_stale_claims.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core.create_binding(UUID_A, "Claim Correlation").expect("b");
    let bid = &binding.binding_id;

    let queued = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "claim-test-1".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Check claim correlation".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("start")
        .into_worker()
        .expect("worker");

    // 1. Attempting to claim with a mismatched profile_digest must fail with StaleWorkerClaim
    //    and leave the worker Queued.
    assert!(matches!(
        core.claim_worker(bid, &queued.run_id, 1, DIGEST_B, 1_005),
        Err(CoreError::StaleWorkerClaim { .. })
    ));
    // Attempting to claim with a stale revision must fail with StaleRevision.
    assert!(matches!(
        core.claim_worker(bid, &queued.run_id, 2, DIGEST_A, 1_006),
        Err(CoreError::StaleRevision {
            expected_revision: 2,
            actual_revision: 1,
            ..
        })
    ));

    // Valid claim succeeds.
    let valid_claim = core
        .claim_worker(bid, &queued.run_id, 1, DIGEST_A, 1_010)
        .expect("valid claim");

    // Second claim on an already-claimed worker must fail with InvalidRunState.
    assert!(matches!(
        core.claim_worker(bid, &queued.run_id, 1, DIGEST_A, 1_011),
        Err(CoreError::InvalidRunState {
            action: "claim_worker",
            ..
        })
    ));

    // 2. Forged/stale claims with wrong claim_id, revision, attempt_count, worker_id, or profile_digest
    //    must be rejected by record_worker_running, record_worker_progress, and record_worker_outcome.
    let mut wrong_claim_id = valid_claim.clone();
    wrong_claim_id.claim_id = "wclm_forged".to_string();

    let mut wrong_revision = valid_claim.clone();
    wrong_revision.revision = 99;

    let mut wrong_attempt = valid_claim.clone();
    wrong_attempt.attempt_count = 2;

    let mut wrong_digest = valid_claim.clone();
    wrong_digest.profile_digest = DIGEST_B.to_string();

    for bad_claim in [
        &wrong_claim_id,
        &wrong_revision,
        &wrong_attempt,
        &wrong_digest,
    ] {
        assert!(matches!(
            core.record_worker_running(
                bad_claim,
                sample_worktree_update(&queued.run_id, &queued.worker_id),
                1_020,
            ),
            Err(CoreError::StaleWorkerClaim { .. })
        ));
        assert!(matches!(
            core.record_worker_progress(
                bad_claim,
                &[WorkerProgressUpdate {
                    provider_retry_high_water: None,
                    provider_health: None,
                    watchdog_attention: None,
                    event_kind: "tool_use".to_string(),
                    summary: "forged progress".to_string(),
                }],
                1_021,
            ),
            Err(CoreError::StaleWorkerClaim { .. })
        ));
        assert!(matches!(
            core.record_worker_outcome(
                bad_claim,
                WorkerCompletionRecord {
                    status: WorkerStatus::Failed,
                    observed_model: None,
                    observed_effort: None,
                    head_commit: None,
                    dropped_live_events: 0,
                    outcome: sample_ready_outcome(),
                },
                1_022,
            ),
            Err(CoreError::StaleWorkerClaim { .. })
        ));
    }

    // 3. Complete the worker legitimately, then verify that a late/duplicate callback with `valid_claim`
    //    cannot overwrite the terminal Completed outcome.
    let completed = core
        .record_worker_outcome(
            &valid_claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: Some("aaaa1111".to_string()),
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            1_050,
        )
        .expect("legitimate completion");
    assert_eq!(completed.status, WorkerStatus::Completed);

    let mut failed_outcome = sample_ready_outcome();
    failed_outcome.acceptance_state = WorkerAcceptanceState::ExecutionFailed;
    failed_outcome.failure_reason = Some("late stale failure".to_string());

    assert!(matches!(
        core.record_worker_outcome(
            &valid_claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Failed,
                observed_model: None,
                observed_effort: None,
                head_commit: Some("bbbb2222".to_string()),
                dropped_live_events: 9,
                outcome: failed_outcome,
            },
            1_060,
        ),
        Err(CoreError::StaleWorkerClaim { .. })
    ));

    let after_stale = core.get_worker(bid, &queued.run_id).expect("after stale");
    assert_eq!(after_stale, completed);
}

#[test]
fn worker_cancel_semantics_queued_vs_running_and_natural_completion_race() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_cancel_race.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core.create_binding(UUID_A, "Cancel Semantics").expect("b");
    let bid = &binding.binding_id;

    // Case 1: Cancelling an unclaimed Queued worker transitions immediately to Cancelled.
    let w_queued = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-queued".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Cancel while queued".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("start w_queued")
        .into_worker()
        .expect("w_queued");

    // Stale revision on CancelWorker must fail with StaleRevision.
    assert!(matches!(
        core.execute(
            bid,
            Command::CancelWorker {
                command_key: "cancel-stale-rev".to_string(),
                run_id: w_queued.run_id.clone(),
                expected_revision: 2,
                reason: None,
                now_ms: 1_005,
            },
        ),
        Err(CoreError::StaleRevision {
            expected_revision: 2,
            actual_revision: 1,
            ..
        })
    ));

    let cancelled_queued = core
        .execute(
            bid,
            Command::CancelWorker {
                command_key: "cancel-queued-1".to_string(),
                run_id: w_queued.run_id.clone(),
                expected_revision: 1,
                reason: Some("no longer needed".to_string()),
                now_ms: 1_010,
            },
        )
        .expect("cancel queued")
        .into_worker()
        .expect("cancelled_queued");
    assert_eq!(cancelled_queued.status, WorkerStatus::Cancelled);
    assert_eq!(
        cancelled_queued.cancel_reason.as_deref(),
        Some("no longer needed")
    );
    assert!(
        cancelled_queued
            .outcome
            .as_ref()
            .expect("outcome")
            .cancelled
    );

    // Case 2: Cancelling a Running worker records CancelRequested first, then transitions to
    // proven Cancelled when the supervisor reports termination.
    let w_stop = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-stop".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Cancel while running".to_string(),
                now_ms: 2_000,
            },
        )
        .expect("start w_stop")
        .into_worker()
        .expect("w_stop");
    let claim_stop = core
        .claim_worker(bid, &w_stop.run_id, 1, DIGEST_A, 2_005)
        .expect("claim stop");
    core.record_worker_running(
        &claim_stop,
        sample_worktree_update(&w_stop.run_id, &w_stop.worker_id),
        2_010,
    )
    .expect("running stop");

    let cancel_req = core
        .execute(
            bid,
            Command::CancelWorker {
                command_key: "cancel-running-1".to_string(),
                run_id: w_stop.run_id.clone(),
                expected_revision: 1,
                reason: Some("user requested stop".to_string()),
                now_ms: 2_020,
            },
        )
        .expect("cancel running")
        .into_worker()
        .expect("cancel_req");
    assert_eq!(cancel_req.status, WorkerStatus::CancelRequested);
    assert_eq!(cancel_req.cancel_requested_at_ms, Some(2_020));
    assert_eq!(
        cancel_req.cancel_reason.as_deref(),
        Some("user requested stop")
    );
    assert!(cancel_req.outcome.is_none());

    // Idempotent retry of `cancel-running-1` returns the exact CancelRequested snapshot.
    let cancel_req_replay = core
        .execute(
            bid,
            Command::CancelWorker {
                command_key: "cancel-running-1".to_string(),
                run_id: w_stop.run_id.clone(),
                expected_revision: 1,
                reason: Some("user requested stop".to_string()),
                now_ms: 2_025,
            },
        )
        .expect("replay cancel")
        .into_worker()
        .expect("replay");
    assert_eq!(cancel_req_replay, cancel_req);

    // A second distinct cancel command while already CancelRequested is rejected with InvalidRunState.
    assert!(matches!(
        core.execute(
            bid,
            Command::CancelWorker {
                command_key: "cancel-running-duplicate-new-key".to_string(),
                run_id: w_stop.run_id.clone(),
                expected_revision: 1,
                reason: Some("second cancel".to_string()),
                now_ms: 2_026,
            },
        ),
        Err(CoreError::InvalidRunState {
            action: "cancel_worker",
            ..
        })
    ));

    // Supervisor proves process stopped due to cancellation -> transitions to Cancelled.
    let mut cancelled_outcome = sample_ready_outcome();
    cancelled_outcome.exit_code = None;
    cancelled_outcome.signal = Some(9);
    cancelled_outcome.cancelled = true;
    cancelled_outcome.acceptance_state = WorkerAcceptanceState::ExecutionFailed;
    let proven_cancelled = core
        .record_worker_outcome(
            &claim_stop,
            WorkerCompletionRecord {
                status: WorkerStatus::Cancelled,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: None,
                dropped_live_events: 0,
                outcome: cancelled_outcome,
            },
            2_040,
        )
        .expect("proven cancelled");
    assert_eq!(proven_cancelled.status, WorkerStatus::Cancelled);
    let stop_out = proven_cancelled.outcome.expect("stop outcome");
    assert!(stop_out.cancelled);
    assert!(stop_out.cancel_requested_before_completion);

    // Case 3: Cancellation races with natural completion!
    // Worker is CancelRequested, but the process exits 0 naturally before the kill signal arrives.
    // Core must preserve the genuine Completed result AND retain the stop-request evidence.
    let w_race = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-race".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: "Finish just as cancel arrives".to_string(),
                now_ms: 3_000,
            },
        )
        .expect("start w_race")
        .into_worker()
        .expect("w_race");
    let claim_race = core
        .claim_worker(bid, &w_race.run_id, 1, DIGEST_A, 3_005)
        .expect("claim race");
    core.record_worker_running(
        &claim_race,
        sample_worktree_update(&w_race.run_id, &w_race.worker_id),
        3_010,
    )
    .expect("running race");

    core.execute(
        bid,
        Command::CancelWorker {
            command_key: "cancel-race-1".to_string(),
            run_id: w_race.run_id.clone(),
            expected_revision: 1,
            reason: Some("too late to stop".to_string()),
            now_ms: 3_020,
        },
    )
    .expect("request cancel on w_race");

    let raced_done = core
        .record_worker_outcome(
            &claim_race,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: Some("112233445566".to_string()),
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            3_025,
        )
        .expect("record natural completion after cancel request");

    assert_eq!(raced_done.status, WorkerStatus::Completed);
    assert_eq!(raced_done.cancel_requested_at_ms, Some(3_020));
    assert_eq!(
        raced_done.cancel_reason.as_deref(),
        Some("too late to stop")
    );
    let raced_outcome = raced_done.outcome.expect("raced outcome");
    assert!(!raced_outcome.cancelled);
    assert!(raced_outcome.cancel_requested_before_completion);
    assert_eq!(
        raced_outcome.acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );
    assert_eq!(
        core.get_run(bid, &w_race.run_id).expect("run").status,
        RunStatus::Completed
    );
}

#[test]
fn worker_reopen_recovery_marks_in_flight_unknown_without_replay_and_migrates_v1_v2() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_reopen_migration.db");

    // Step 1: Create 4 in-flight workers (Queued, Claimed, Running, CancelRequested) and 1 Completed worker.
    let (bid, run_queued, run_claimed, run_running, run_cancel_req, run_completed) = {
        let mut core = Core::open(&db_path).expect("open core");
        let b = core.create_binding(UUID_A, "Reopen Recovery").expect("b");
        let bid = b.binding_id;

        let start = |core: &mut Core, key: &str, ts: i64| {
            core.execute(
                &bid,
                Command::StartWorker {
                    command_key: key.to_string(),
                    profile_id: "prof-1".to_string(),
                    profile_digest: DIGEST_A.to_string(),
                    provider: "codex_cli".to_string(),
                    requested_model: "gpt-5.4".to_string(),
                    requested_effort: "high".to_string(),
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                    execution_binding: None,
                    instruction: format!("Task for {key}"),
                    now_ms: ts,
                },
            )
            .expect("start")
            .into_worker()
            .expect("worker")
        };

        let w1 = start(&mut core, "w-queued", 1_000);

        let w2 = start(&mut core, "w-claimed", 1_010);
        let _c2 = core
            .claim_worker(&bid, &w2.run_id, 1, DIGEST_A, 1_011)
            .expect("claim w2");

        let w3 = start(&mut core, "w-running", 1_020);
        let c3 = core
            .claim_worker(&bid, &w3.run_id, 1, DIGEST_A, 1_021)
            .expect("claim w3");
        core.record_worker_running(
            &c3,
            sample_worktree_update(&w3.run_id, &w3.worker_id),
            1_022,
        )
        .expect("running w3");

        let w4 = start(&mut core, "w-cancel-req", 1_030);
        let c4 = core
            .claim_worker(&bid, &w4.run_id, 1, DIGEST_A, 1_031)
            .expect("claim w4");
        core.record_worker_running(
            &c4,
            sample_worktree_update(&w4.run_id, &w4.worker_id),
            1_032,
        )
        .expect("running w4");
        core.execute(
            &bid,
            Command::CancelWorker {
                command_key: "cancel-w4".to_string(),
                run_id: w4.run_id.clone(),
                expected_revision: 1,
                reason: Some("shutting down".to_string()),
                now_ms: 1_033,
            },
        )
        .expect("cancel_req w4");

        let w5 = start(&mut core, "w-done", 1_040);
        let c5 = core
            .claim_worker(&bid, &w5.run_id, 1, DIGEST_A, 1_041)
            .expect("claim w5");
        core.record_worker_outcome(
            &c5,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: None,
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            1_042,
        )
        .expect("complete w5");

        (bid, w1.run_id, w2.run_id, w3.run_id, w4.run_id, w5.run_id)
    };

    // Simulate a v2 database upgrade at the same time by setting schema_meta.version = 2.
    {
        let conn = rusqlite::Connection::open(&db_path).expect("raw sqlite");
        conn.execute("UPDATE schema_meta SET version = 2 WHERE id = 1", [])
            .expect("set version 2");
    }

    // Step 2: Reopen Core. Schema must upgrade to SCHEMA_VERSION (3), and all 4 non-terminal
    // workers must become Unknown with important events and wake-up coalescing, while w5 stays Completed.
    let reopened = Core::open(&db_path).expect("reopen core");
    {
        let conn = rusqlite::Connection::open(&db_path).expect("inspect version");
        let v: u32 = conn
            .query_row("SELECT version FROM schema_meta WHERE id = 1", [], |r| {
                r.get(0)
            })
            .expect("version");
        assert_eq!(v, SCHEMA_VERSION);
    }

    assert!(
        reopened
            .list_queued_workers(10)
            .expect("no queued workers after reopen")
            .is_empty(),
        "uncertain workers must never remain queued for automatic re-execution"
    );

    for run_id in [&run_queued, &run_claimed, &run_running, &run_cancel_req] {
        let snap = reopened
            .get_worker(&bid, run_id)
            .expect("get recovered worker");
        assert_eq!(snap.status, WorkerStatus::Unknown);
        let out = snap.outcome.expect("unknown outcome populated");
        assert_eq!(out.acceptance_state, WorkerAcceptanceState::Unknown);
        assert!(out
            .failure_reason
            .as_deref()
            .unwrap_or_default()
            .contains("marked unknown without automatic retry"));
        if run_id == &run_cancel_req {
            assert!(out.cancel_requested_before_completion);
            assert_eq!(snap.cancel_reason.as_deref(), Some("shutting down"));
        }
        assert_eq!(
            reopened.get_run(&bid, run_id).expect("run").status,
            RunStatus::Unknown
        );
    }

    let done_snap = reopened
        .get_worker(&bid, &run_completed)
        .expect("completed untouched");
    assert_eq!(done_snap.status, WorkerStatus::Completed);

    // Wakeup notice has coalesced the 1 Completed + 4 WorkerUnknown important events (5 total).
    let wakeup = reopened.get_wakeup(&bid).expect("wakeup after reopen");
    assert_eq!(wakeup.status, WakeupStatus::Pending);
    assert_eq!(wakeup.coalesced_count, 5);
    assert_eq!(
        wakeup.message,
        threadlane_core::WAKEUP_NOTICE_STATUS_MESSAGE
    );
}

#[test]
fn worker_bounded_progress_cap_secret_redaction_and_wire_budget_enforcement() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_bounds.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core.create_binding(UUID_A, "Bounded Progress").expect("b");
    let bid = &binding.binding_id;

    let max_instruction = "I".repeat(MAX_INSTRUCTION_LEN);
    let started = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "bounded-worker-1".to_string(),
                profile_id: "prof-bounded".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "opencode".to_string(),
                requested_model: "openai/gpt-5".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: None,
                instruction: max_instruction,
                now_ms: 1_000,
            },
        )
        .expect("start")
        .into_worker()
        .expect("worker");

    let claim = core
        .claim_worker(bid, &started.run_id, 1, DIGEST_A, 1_005)
        .expect("claim");

    // Emit 50 live progress updates containing secrets and oversized text.
    let total_updates = (MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT as usize) + 18;
    let mut updates = Vec::with_capacity(total_updates);
    for idx in 0..total_updates {
        updates.push(WorkerProgressUpdate {
            provider_retry_high_water: None,
            provider_health: None,
                    watchdog_attention: None,
            event_kind: "assistant_message".to_string(),
            summary: format!(
                "Step {idx} using sk-secret-token-value-123456 and Bearer eyJhbGciOiJIUzI1NiJ9 and OPENAI_API_KEY=supersecret {}",
                "P".repeat(MAX_WORKER_SUMMARY_TEXT_BYTES * 2)
            ),
        });
    }

    let after_progress = core
        .record_worker_progress(&claim, &updates, 1_020)
        .expect("record progress batch");
    assert_eq!(
        after_progress.progress.durable_progress_events,
        MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT
    );
    assert_eq!(after_progress.progress.coalesced_progress_events, 18);

    let last_summary = after_progress
        .progress
        .last_summary
        .as_deref()
        .expect("last_summary");
    assert!(last_summary.len() <= MAX_WORKER_SUMMARY_TEXT_BYTES);
    assert!(!last_summary.contains("sk-secret-token-value-123456"));
    assert!(!last_summary.contains("eyJhbGciOiJIUzI1NiJ9"));
    assert!(!last_summary.contains("supersecret"));
    assert!(last_summary.contains("[REDACTED]"));

    // Record a failure outcome with oversized changed paths, violation summaries, and secret-bearing diagnostics.
    let many_paths: Vec<String> = (0..(MAX_WORKER_CHANGED_PATHS_PREVIEW + 10))
        .map(|i| format!("src/module_{i}/file_{}.rs", "x".repeat(200)))
        .collect();
    let many_violations: Vec<String> = (0..(MAX_WORKER_VIOLATION_SUMMARIES + 6))
        .map(|i| {
            format!(
                "forbidden_path: secret_{i} token=my_secret_value {}",
                "V".repeat(400)
            )
        })
        .collect();

    let outcome = WorkerOutcomeSummary {
        provider_retry_high_water: None,
        provider_health: None,
        exit_code: Some(1),
        signal: None,
        duration_ms: Some(1_500),
        timed_out: false,
        cancelled: false,
        cancel_requested_before_completion: false,
        missing_model_evidence: true,
        missing_effort_evidence: true,
        provider_error: true,
        incomplete_cleanup: true,
        rejected_by_diff_gate: true,
        stdout_truncated: true,
        stderr_truncated: true,
        acceptance_state: WorkerAcceptanceState::RejectedByDiffGate,
        changed_paths_count: many_paths.len() as u32,
        changed_paths_preview: many_paths,
        violation_count: many_violations.len() as u32,
        violation_summaries: many_violations,
        final_summary: Some(format!(
            "Summary with secret sk-abcdef1234567890 {}",
            "S".repeat(1_000)
        )),
        failure_reason: Some(format!(
            "Failed with api_key=raw_secret_key {}",
            "F".repeat(1_000)
        )),
        report_artifact_id: Some("art_large_report.json".to_string()),
    };

    let finished = core
        .record_worker_outcome(
            &claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Failed,
                observed_model: None,
                observed_effort: None,
                head_commit: Some("deadbeef".to_string()),
                dropped_live_events: 7,
                outcome,
            },
            1_050,
        )
        .expect("record failed outcome");

    assert_eq!(finished.status, WorkerStatus::Failed);
    assert_eq!(finished.progress.dropped_live_events, 7);
    let out = finished.outcome.as_ref().expect("outcome");
    assert!(out.missing_model_evidence);
    assert!(out.missing_effort_evidence);
    assert!(out.provider_error);
    assert!(out.incomplete_cleanup);
    assert!(out.rejected_by_diff_gate);
    assert_eq!(
        out.changed_paths_preview.len(),
        MAX_WORKER_CHANGED_PATHS_PREVIEW
    );
    assert_eq!(
        out.violation_summaries.len(),
        MAX_WORKER_VIOLATION_SUMMARIES
    );
    assert!(!out
        .final_summary
        .as_deref()
        .unwrap_or_default()
        .contains("sk-abcdef1234567890"));
    assert!(!out
        .failure_reason
        .as_deref()
        .unwrap_or_default()
        .contains("raw_secret_key"));

    let wire_bytes =
        estimate_wire_response_bytes(&CommandOutput::Worker(Box::new(finished))).expect("wire");
    assert!(
        wire_bytes <= MAX_COMPACT_RESPONSE_BYTES,
        "worker snapshot wire bytes {wire_bytes} exceeded {MAX_COMPACT_RESPONSE_BYTES}"
    );

    // Pull events and verify only MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT progress events were persisted.
    let batch = core
        .execute(
            bid,
            Command::PullEvents {
                after_seq: None,
                limit: 100,
                now_ms: 1_060,
            },
        )
        .expect("pull events")
        .into_event_batch()
        .expect("batch");
    let progress_event_count = batch
        .events
        .iter()
        .filter(|e| e.kind == EventKind::WorkerProgress)
        .count();
    assert_eq!(
        progress_event_count,
        MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT as usize
    );
    for ev in &batch.events {
        assert!(!ev.payload.summary.contains("sk-secret-token-value-123456"));
        assert!(!ev.payload.summary.contains("supersecret"));
    }
}

#[test]
fn worker_shutdown_mark_active_unknown_and_timeout_outcome() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_shutdown_timeout.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Shutdown & Timeout")
        .expect("b");
    let bid = &binding.binding_id;

    // 1. Worker that times out emits WorkerTimedOut (important = true) and transitions to TimedOut.
    let w_timeout = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-timeout".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Slow worker task".to_string(),
                now_ms: 1_000,
            },
        )
        .expect("start w_timeout")
        .into_worker()
        .expect("w_timeout");
    let claim_timeout = core
        .claim_worker(bid, &w_timeout.run_id, 1, DIGEST_A, 1_005)
        .expect("claim timeout");

    let mut timeout_outcome = sample_ready_outcome();
    timeout_outcome.exit_code = None;
    timeout_outcome.timed_out = true;
    timeout_outcome.acceptance_state = WorkerAcceptanceState::ExecutionFailed;
    timeout_outcome.failure_reason = Some("exceeded 30s timeout".to_string());

    let timed_out_snap = core
        .record_worker_outcome(
            &claim_timeout,
            WorkerCompletionRecord {
                status: WorkerStatus::TimedOut,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: None,
                dropped_live_events: 0,
                outcome: timeout_outcome,
            },
            31_005,
        )
        .expect("record timeout");
    assert_eq!(timed_out_snap.status, WorkerStatus::TimedOut);
    assert_eq!(
        core.get_run(bid, &w_timeout.run_id).expect("run").status,
        RunStatus::TimedOut
    );

    // 2. Active worker interrupted by supervisor shutdown via `mark_active_workers_unknown`
    //    becomes Unknown and rejects any late callback from the aborted task.
    let w_active = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "w-active-shutdown".to_string(),
                profile_id: "prof-1".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "codex_cli".to_string(),
                requested_model: "gpt-5.4".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "HEAD".to_string(),
                execution_binding: None,
                instruction: "Interrupted by shutdown".to_string(),
                now_ms: 32_000,
            },
        )
        .expect("start w_active")
        .into_worker()
        .expect("w_active");
    let claim_active = core
        .claim_worker(bid, &w_active.run_id, 1, DIGEST_A, 32_005)
        .expect("claim active");

    let marked_count = core
        .mark_active_workers_unknown(32_500, "supervisor shutdown aborted active worker")
        .expect("mark_active_workers_unknown");
    assert_eq!(marked_count, 1);

    let unknown_snap = core
        .get_worker(bid, &w_active.run_id)
        .expect("unknown snap");
    assert_eq!(unknown_snap.status, WorkerStatus::Unknown);
    assert_eq!(
        unknown_snap
            .outcome
            .as_ref()
            .expect("outcome")
            .acceptance_state,
        WorkerAcceptanceState::Unknown
    );

    // Late callback after shutdown uncertainty must be rejected as stale.
    assert!(matches!(
        core.record_worker_outcome(
            &claim_active,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gpt-5.4".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: None,
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            32_600,
        ),
        Err(CoreError::StaleWorkerClaim { .. })
    ));
}

#[test]
fn worker_execution_binding_reject_queued_worker_and_schema_v3_migration() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("worker_exec_binding.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Execution Binding & Rejection")
        .expect("create binding");
    let bid = &binding.binding_id;

    let exec_base = "1111111111111111111111111111111111111111";
    let input_digest = "22".repeat(32);
    let seal_file_digest = "33".repeat(32);
    let instruction = "Implement dependent task B";

    let exec_binding = WorkerExecutionBinding::new(
        DIGEST_A,
        instruction,
        &input_digest,
        &seal_file_digest,
        exec_base,
    )
    .expect("construct execution binding");
    assert_eq!(
        exec_binding.schema_version,
        WORKER_EXECUTION_BINDING_SCHEMA_VERSION
    );
    exec_binding
        .verify_against(DIGEST_A, instruction, exec_base)
        .expect("verify against matching inputs");
    assert!(exec_binding
        .verify_against(DIGEST_B, instruction, exec_base)
        .is_err());
    assert!(exec_binding
        .verify_against(DIGEST_A, "wrong instruction", exec_base)
        .is_err());
    assert!(exec_binding
        .verify_against(
            DIGEST_A,
            instruction,
            "4444444444444444444444444444444444444444"
        )
        .is_err());

    // StartWorker rejects a mismatched execution_binding before mutating state.
    assert!(matches!(
        core.execute(
            bid,
            Command::StartWorker {
                command_key: "dep-bad-base".to_string(),
                profile_id: "dep-prof".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                execution_binding: Some(exec_binding.clone()),
                instruction: instruction.to_string(),
                now_ms: 1_000,
            },
        ),
        Err(CoreError::Validation {
            field: "execution_binding",
            ..
        })
    ));

    // Admit dependent worker with valid execution_binding.
    let dep_worker = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "dep-start-1".to_string(),
                profile_id: "dep-prof".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: exec_base.to_string(),
                execution_binding: Some(exec_binding.clone()),
                instruction: instruction.to_string(),
                now_ms: 1_010,
            },
        )
        .expect("start dependent worker")
        .into_worker()
        .expect("worker");
    assert_eq!(dep_worker.execution_binding, Some(exec_binding.clone()));
    assert_eq!(dep_worker.worktree.base_commit, exec_base);

    // Reusing the same command_key with a different seal_file_digest must conflict.
    let alt_binding = WorkerExecutionBinding::new(
        DIGEST_A,
        instruction,
        &input_digest,
        &"55".repeat(32),
        exec_base,
    )
    .expect("alt binding");
    assert!(matches!(
        core.execute(
            bid,
            Command::StartWorker {
                command_key: "dep-start-1".to_string(),
                profile_id: "dep-prof".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: exec_base.to_string(),
                execution_binding: Some(alt_binding.clone()),
                instruction: instruction.to_string(),
                now_ms: 1_015,
            },
        ),
        Err(CoreError::IdempotencyConflict { .. })
    ));

    // Claiming without execution_binding or with a mismatched execution_binding must fail with StaleWorkerClaim.
    assert!(matches!(
        core.claim_worker(bid, &dep_worker.run_id, 1, DIGEST_A, 1_020),
        Err(CoreError::StaleWorkerClaim { .. })
    ));
    assert!(matches!(
        core.claim_worker_with_execution_binding(
            bid,
            &dep_worker.run_id,
            1,
            DIGEST_A,
            Some(&alt_binding),
            1_021
        ),
        Err(CoreError::StaleWorkerClaim { .. })
    ));

    let claim = core
        .claim_worker_with_execution_binding(
            bid,
            &dep_worker.run_id,
            1,
            DIGEST_A,
            Some(&exec_binding),
            1_025,
        )
        .expect("claim dependent worker");
    assert_eq!(claim.execution_binding, Some(exec_binding));
    assert_eq!(claim.base_commit, exec_base);

    // record_worker_running must reject a worktree_update with a mismatched base_commit.
    assert!(matches!(
        core.record_worker_running(
            &claim,
            sample_worktree_update(&dep_worker.run_id, &dep_worker.worker_id),
            1_030,
        ),
        Err(CoreError::StaleWorkerClaim { .. })
    ));

    // reject_queued_worker refuses already-claimed workers.
    assert!(matches!(
        core.reject_queued_worker(
            bid,
            &dep_worker.run_id,
            1,
            DIGEST_A,
            "cannot reject claimed worker",
            1_031,
        ),
        Err(CoreError::InvalidRunState {
            action: "reject_queued_worker",
            ..
        })
    ));

    // Admit a second dependent worker and reject it while still Queued.
    let dep_worker_2 = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "dep-start-2".to_string(),
                profile_id: "dep-prof".to_string(),
                profile_digest: DIGEST_A.to_string(),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: exec_base.to_string(),
                execution_binding: Some(alt_binding),
                instruction: instruction.to_string(),
                now_ms: 1_040,
            },
        )
        .expect("start dep_worker_2")
        .into_worker()
        .expect("worker 2");

    let rejected = core
        .reject_queued_worker(
            bid,
            &dep_worker_2.run_id,
            1,
            DIGEST_A,
            "dependency seal changed before claim with sk-secret-token-999999",
            1_050,
        )
        .expect("reject queued worker");
    assert_eq!(rejected.status, WorkerStatus::Failed);
    assert_eq!(rejected.attempt_count, 0);
    assert!(rejected.claim_id.is_none());
    let rej_out = rejected.outcome.as_ref().expect("rejected outcome");
    assert_eq!(
        rej_out.acceptance_state,
        WorkerAcceptanceState::ExecutionFailed
    );
    assert!(!rej_out
        .failure_reason
        .as_deref()
        .unwrap_or_default()
        .contains("sk-secret-token-999999"));
    assert_eq!(
        core.get_run(bid, &dep_worker_2.run_id).expect("run").status,
        RunStatus::Failed
    );
    assert_eq!(
        core.get_wakeup(bid).expect("wakeup").status,
        WakeupStatus::Pending
    );

    // Verify v3 schema migration when `execution_binding_json` is absent.
    let v3_db_path = dir.path().join("legacy_v3.db");
    {
        let conn = rusqlite::Connection::open(&v3_db_path).expect("open v3 sqlite");
        conn.execute_batch(
            "CREATE TABLE schema_meta (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
             INSERT INTO schema_meta (id, version) VALUES (1, 3);
             CREATE TABLE bindings (
                 binding_id TEXT PRIMARY KEY,
                 thread_uuid TEXT NOT NULL,
                 label TEXT NOT NULL,
                 delivered_seq INTEGER NOT NULL DEFAULT 0,
                 acked_seq INTEGER NOT NULL DEFAULT 0,
                 next_event_seq INTEGER NOT NULL DEFAULT 1
             );
             INSERT INTO bindings (binding_id, thread_uuid, label, delivered_seq, acked_seq, next_event_seq)
             VALUES ('bnd_v3', '11111111-1111-4111-8111-111111111111', 'v3', 0, 0, 2);
             CREATE TABLE wakeups (
                 binding_id TEXT PRIMARY KEY REFERENCES bindings(binding_id),
                 thread_uuid TEXT NOT NULL,
                 status TEXT NOT NULL,
                 claim_id TEXT,
                 pending_since_seq INTEGER,
                 latest_important_seq INTEGER NOT NULL DEFAULT 0,
                 covered_up_to_seq INTEGER NOT NULL DEFAULT 0,
                 coalesced_count INTEGER NOT NULL DEFAULT 0,
                 attempt_count INTEGER NOT NULL DEFAULT 0,
                 last_error TEXT,
                 claimed_at_ms INTEGER,
                 resolved_at_ms INTEGER
             );
             INSERT INTO wakeups (binding_id, thread_uuid, status)
             VALUES ('bnd_v3', '11111111-1111-4111-8111-111111111111', 'idle');
             CREATE TABLE runs (
                 run_id TEXT PRIMARY KEY,
                 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                 revision INTEGER NOT NULL,
                 status TEXT NOT NULL,
                 instruction TEXT NOT NULL,
                 step_seq INTEGER NOT NULL,
                 step_delay_ms INTEGER NOT NULL,
                 next_step_at_ms INTEGER,
                 active_question_id TEXT,
                 started_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 completed_at_ms INTEGER,
                 cancelled_at_ms INTEGER,
                 cancel_reason TEXT
             );
             INSERT INTO runs (run_id, binding_id, revision, status, instruction, step_seq, step_delay_ms, started_at_ms, updated_at_ms, completed_at_ms)
             VALUES ('run_v3', 'bnd_v3', 1, 'completed', 'legacy v3 task', 2, 0, 100, 200, 200);
             CREATE TABLE workers (
                 worker_id TEXT PRIMARY KEY,
                 run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id),
                 binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
                 revision INTEGER NOT NULL,
                 status TEXT NOT NULL,
                 profile_id TEXT NOT NULL,
                 profile_digest TEXT NOT NULL,
                 provider TEXT NOT NULL,
                 requested_model TEXT NOT NULL,
                 requested_effort TEXT NOT NULL,
                 observed_model TEXT,
                 observed_effort TEXT,
                 instruction TEXT NOT NULL,
                 claim_id TEXT,
                 attempt_count INTEGER NOT NULL DEFAULT 0,
                 worktree_json TEXT NOT NULL,
                 progress_json TEXT NOT NULL,
                 outcome_json TEXT,
                 created_at_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 started_at_ms INTEGER,
                 completed_at_ms INTEGER,
                 cancel_requested_at_ms INTEGER,
                 cancel_reason TEXT
             );
             INSERT INTO workers (
                 worker_id, run_id, binding_id, revision, status, profile_id,
                 profile_digest, provider, requested_model, requested_effort,
                 instruction, attempt_count, worktree_json, progress_json,
                 created_at_ms, updated_at_ms, completed_at_ms
             ) VALUES (
                 'wrk_v3', 'run_v3', 'bnd_v3', 1, 'completed', 'prof-v3',
                 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 'codex_cli', 'gpt-5.4', 'high', 'legacy v3 task', 1,
                 '{\"base_commit\":\"0123456789abcdef0123456789abcdef01234567\"}',
                 '{\"durable_progress_events\":0,\"coalesced_progress_events\":0,\"dropped_live_events\":0}',
                 100, 200, 200
             );",
        )
        .expect("create v3 schema");
    }

    let migrated = Core::open(&v3_db_path).expect("migrate v3 to v4");
    let v3_worker = migrated
        .get_worker("bnd_v3", "run_v3")
        .expect("load migrated v3 worker");
    assert_eq!(v3_worker.status, WorkerStatus::Completed);
    assert!(v3_worker.execution_binding.is_none());
}
