use tempfile::TempDir;
use threadlane_core::{
    estimate_wire_response_bytes, Command, CommandOutput, Core, CoreError, EventKind, WakeupStatus,
    WorkerProgressUpdate, WorkerWorktreeUpdate, DEFAULT_PAGE_LIMIT, MAX_CURSOR_LEN, MAX_ID_LEN,
    MAX_INSTRUCTION_LEN, MAX_PAGE_LIMIT, MAX_PAGE_RESPONSE_BYTES,
};

const UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

#[test]
fn empty_binding_and_read_only_invariants_across_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("activity_readonly.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Read-Only Activity Binding")
        .expect("create binding");
    let bid = &binding.binding_id;

    // 1. Empty binding returns empty page with high_water_mark_seq = 0 and after_seq = 0.
    let empty_page = core
        .list_activity(bid, None, None, DEFAULT_PAGE_LIMIT, None, None)
        .expect("empty list_activity");
    assert_eq!(empty_page.binding_id, *bid);
    assert!(empty_page.events.is_empty());
    assert_eq!(empty_page.next_cursor, None);
    assert!(!empty_page.has_more);
    assert_eq!(empty_page.high_water_mark_seq, 0);
    assert_eq!(empty_page.after_seq, 0);

    // 2. Emit demo lifecycle events and worker lifecycle events without calling pull_events.
    let demo_run = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "ro-demo-start".to_string(),
                instruction: "Demo task instruction".to_string(),
                step_delay_ms: Some(20),
                now_ms: 1_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("demo run");
    core.tick(1_020).expect("tick demo to waiting_input");

    let worker_snap = core
        .execute(
            bid,
            Command::StartWorker {
                command_key: "ro-worker-start".to_string(),
                profile_id: "prof_ro".to_string(),
                profile_digest: "a".repeat(64),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
                instruction: "Worker task instruction".to_string(),
                now_ms: 1_030,
            },
        )
        .expect("start worker")
        .into_worker()
        .expect("worker snapshot");
    let claim = core
        .claim_worker(
            bid,
            &worker_snap.run_id,
            worker_snap.revision,
            &"a".repeat(64),
            1_040,
        )
        .expect("claim worker");
    core.record_worker_running(
        &claim,
        WorkerWorktreeUpdate {
            run_worktree_id: "wt-run-1".to_string(),
            run_worktree_path: "/tmp/wt-run-1".to_string(),
            run_branch_name: "tl/run/1".to_string(),
            task_worktree_id: "wt-task-1".to_string(),
            task_worktree_path: "/tmp/wt-task-1".to_string(),
            task_branch_name: "tl/task/1".to_string(),
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_string(),
        },
        1_050,
    )
    .expect("record worker running");
    core.record_worker_progress(
        &claim,
        &[WorkerProgressUpdate {
            event_kind: "assistant".to_string(),
            summary: "Inspecting crate".to_string(),
        }],
        1_060,
    )
    .expect("record worker progress");

    let wakeup_before = core.get_wakeup(bid).expect("wakeup before");
    assert_eq!(wakeup_before.status, WakeupStatus::Pending);
    let binding_before = core.get_binding(bid).expect("binding before");
    assert_eq!(binding_before.delivered_seq, 0);
    assert_eq!(binding_before.acked_seq, 0);

    // 3. Repeated list_activity calls via both Core::list_activity and Command::ListActivity.
    let page_direct = core
        .list_activity(bid, None, None, DEFAULT_PAGE_LIMIT, None, None)
        .expect("direct list_activity");
    let page_cmd = core
        .execute(
            bid,
            Command::ListActivity {
                after_seq: None,
                cursor: None,
                limit: DEFAULT_PAGE_LIMIT,
                run_id: None,
                kind: None,
            },
        )
        .expect("execute ListActivity")
        .into_activity()
        .expect("into_activity");
    assert_eq!(page_direct, page_cmd);
    assert_eq!(page_direct.events.len(), 6);
    assert_eq!(page_direct.high_water_mark_seq, 6);
    assert_eq!(page_direct.after_seq, 0);
    assert!(!page_direct.has_more);
    assert_eq!(page_direct.next_cursor, None);
    for (idx, ev) in page_direct.events.iter().enumerate() {
        assert_eq!(ev.seq, (idx as u64) + 1);
        assert!(ev.payload.untrusted_data);
    }
    assert_eq!(page_direct.events[0].run_id, demo_run.run_id);
    assert_eq!(page_direct.events[0].kind, EventKind::RunStarted);
    assert_eq!(page_direct.events[3].run_id, worker_snap.run_id);
    assert_eq!(page_direct.events[3].kind, EventKind::WorkerQueued);
    assert_eq!(page_direct.events[4].kind, EventKind::WorkerStarted);
    assert_eq!(page_direct.events[5].kind, EventKind::WorkerProgress);

    // 4. Follower cursors, wakeup notice, and event_batches must be completely untouched.
    let binding_after = core.get_binding(bid).expect("binding after");
    assert_eq!(binding_after.delivered_seq, 0);
    assert_eq!(binding_after.acked_seq, 0);
    let wakeup_after = core.get_wakeup(bid).expect("wakeup after");
    assert_eq!(wakeup_after, wakeup_before);

    // 5. Reopen database and verify activity history persists while follower cursors stay 0.
    drop(core);
    let core_reopened = Core::open(&db_path).expect("reopen core");
    let reopened_binding = core_reopened.get_binding(bid).expect("reopened binding");
    assert_eq!(reopened_binding.delivered_seq, 0);
    assert_eq!(reopened_binding.acked_seq, 0);

    // Note: reopening transitions in-flight worker to WorkerUnknown (emitting event seq 7).
    let reopened_page = core_reopened
        .list_activity(bid, None, None, DEFAULT_PAGE_LIMIT, None, None)
        .expect("reopened list_activity");
    assert_eq!(reopened_page.high_water_mark_seq, 7);
    assert_eq!(reopened_page.events.len(), 7);
    assert_eq!(reopened_page.events[6].kind, EventKind::WorkerUnknown);
}

#[test]
fn pagination_freezes_high_water_mark_and_filters_under_concurrent_appends() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("activity_paging.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Paging Binding")
        .expect("binding");
    let bid = &binding.binding_id;

    // Create 2 runs and advance both to WaitingInput (3 events per run = 6 events total).
    let run_1 = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "p-run-1".to_string(),
                instruction: "Run 1 v1".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_000,
            },
        )
        .expect("start run 1")
        .into_run()
        .expect("run 1");
    core.tick(1_010).expect("tick run 1");

    let run_2 = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "p-run-2".to_string(),
                instruction: "Run 2 v1".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_020,
            },
        )
        .expect("start run 2")
        .into_run()
        .expect("run 2");
    core.tick(1_030).expect("tick run 2");

    // Page 1 (unfiltered, limit = 2): snapshots high_water_mark_seq = 6.
    let page_1 = core
        .list_activity(bid, None, None, 2, None, None)
        .expect("page 1");
    assert_eq!(page_1.high_water_mark_seq, 6);
    assert_eq!(page_1.after_seq, 0);
    assert!(page_1.has_more);
    assert_eq!(
        page_1.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let cursor_1 = page_1.next_cursor.clone().expect("cursor 1");

    // Also open Page 1 filtered by `run_id = Some(run_1)` with `limit = 2` (run_1 has events at seq 1, 2, 3).
    let run1_page_1 = core
        .list_activity(bid, None, None, 2, Some(&run_1.run_id), None)
        .expect("run1 page 1");
    assert_eq!(run1_page_1.high_water_mark_seq, 6);
    assert_eq!(run1_page_1.after_seq, 0);
    assert!(run1_page_1.has_more);
    assert_eq!(
        run1_page_1.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    let run1_cursor_1 = run1_page_1.next_cursor.clone().expect("run1 cursor 1");

    // Also open Page 1 filtered by `kind = Some(EventKind::QuestionAsked)` with `limit = 1` (matches seq 3 and seq 6).
    let kind_page_1 = core
        .list_activity(bid, None, None, 1, None, Some(EventKind::QuestionAsked))
        .expect("kind page 1");
    assert_eq!(kind_page_1.high_water_mark_seq, 6);
    assert_eq!(kind_page_1.after_seq, 0);
    assert!(kind_page_1.has_more);
    assert_eq!(
        kind_page_1.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3]
    );
    let kind_cursor_1 = kind_page_1.next_cursor.clone().expect("kind cursor 1");

    // Now steer `run_1` and tick so 4 new events (seq 7, 8, 9, 10) are emitted AFTER page 1 was opened!
    core.execute(
        bid,
        Command::Steer {
            command_key: "p-steer-1".to_string(),
            run_id: run_1.run_id.clone(),
            expected_revision: 1,
            instruction: "Run 1 v2".to_string(),
            now_ms: 1_040,
        },
    )
    .expect("steer run 1");
    core.tick(1_050).expect("tick run 1 v2");

    // Verify new high water mark on fresh page is 10, while walking the existing cursors stays pinned to 6!
    let fresh = core
        .list_activity(bid, None, None, DEFAULT_PAGE_LIMIT, None, None)
        .expect("fresh");
    assert_eq!(fresh.high_water_mark_seq, 10);
    assert_eq!(fresh.events.len(), 10);

    // Finish walking unfiltered cursor chain (should yield [3, 4] then [5, 6] and stop at 6).
    let page_2 = core
        .list_activity(bid, None, Some(&cursor_1), 2, None, None)
        .expect("page 2");
    assert_eq!(page_2.high_water_mark_seq, 6);
    assert_eq!(page_2.after_seq, 2);
    assert!(page_2.has_more);
    assert_eq!(
        page_2.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3, 4]
    );

    let page_3 = core
        .list_activity(bid, None, page_2.next_cursor.as_deref(), 2, None, None)
        .expect("page 3");
    assert_eq!(page_3.high_water_mark_seq, 6);
    assert_eq!(page_3.after_seq, 4);
    assert!(!page_3.has_more);
    assert_eq!(page_3.next_cursor, None);
    assert_eq!(
        page_3.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![5, 6]
    );

    // Finish walking `run_1` filtered cursor (should yield [3] and stop despite seq 4..6 belonging to run_2 and seq 7..10 belonging to run_1!).
    let run1_page_2 = core
        .list_activity(
            bid,
            None,
            Some(&run1_cursor_1),
            2,
            Some(&run_1.run_id),
            None,
        )
        .expect("run1 page 2");
    assert_eq!(run1_page_2.high_water_mark_seq, 6);
    assert_eq!(run1_page_2.after_seq, 2);
    assert!(!run1_page_2.has_more);
    assert_eq!(run1_page_2.next_cursor, None);
    assert_eq!(
        run1_page_2.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3]
    );

    // Finish walking `QuestionAsked` filtered cursor (should yield [6] and stop despite seq 10 also being QuestionAsked).
    let kind_page_2 = core
        .list_activity(
            bid,
            None,
            Some(&kind_cursor_1),
            1,
            None,
            Some(EventKind::QuestionAsked),
        )
        .expect("kind page 2");
    assert_eq!(kind_page_2.high_water_mark_seq, 6);
    assert_eq!(kind_page_2.after_seq, 3);
    assert!(!kind_page_2.has_more);
    assert_eq!(kind_page_2.next_cursor, None);
    assert_eq!(
        kind_page_2.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![6]
    );

    // Combined `run_id` + `kind` filter with no matches still reports `high_water_mark_seq = 10`
    // and `has_more = false` so the caller can advance its live cursor.
    let no_matches = core
        .list_activity(
            bid,
            None,
            None,
            DEFAULT_PAGE_LIMIT,
            Some(&run_2.run_id),
            Some(EventKind::RunSteered),
        )
        .expect("no filtered matches");
    assert!(no_matches.events.is_empty());
    assert!(!no_matches.has_more);
    assert_eq!(no_matches.next_cursor, None);
    assert_eq!(no_matches.high_water_mark_seq, 10);
    assert_eq!(no_matches.after_seq, 0);
}

#[test]
fn live_viewer_after_seq_advancement_and_follower_cursor_independence() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("activity_live.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Live Viewer Binding")
        .expect("binding");
    let bid = &binding.binding_id;

    let run = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "lv-start".to_string(),
                instruction: "Live viewer demo".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_000,
            },
        )
        .expect("start")
        .into_run()
        .expect("run");
    core.tick(1_010).expect("tick 1");

    // Initial live viewer fetch gets events 1..=3 and high_water_mark_seq = 3.
    let initial = core
        .list_activity(bid, None, None, DEFAULT_PAGE_LIMIT, None, None)
        .expect("initial live fetch");
    assert_eq!(initial.high_water_mark_seq, 3);
    assert_eq!(initial.after_seq, 0);
    assert_eq!(initial.events.len(), 3);

    // Polling with after_seq = Some(3) before new events arrive returns empty batch with high_water_mark_seq = 3.
    let poll_empty = core
        .list_activity(
            bid,
            Some(initial.high_water_mark_seq),
            None,
            DEFAULT_PAGE_LIMIT,
            None,
            None,
        )
        .expect("poll empty at hwm");
    assert!(poll_empty.events.is_empty());
    assert!(!poll_empty.has_more);
    assert_eq!(poll_empty.next_cursor, None);
    assert_eq!(poll_empty.high_water_mark_seq, 3);
    assert_eq!(poll_empty.after_seq, 3);

    // Filtered poll with no matches in new window still advances high_water_mark_seq.
    let waiting = core.get_run(bid, &run.run_id).expect("get waiting");
    let q_id = waiting.active_question_id.expect("question_id");
    core.execute(
        bid,
        Command::Answer {
            command_key: "lv-ans".to_string(),
            run_id: run.run_id.clone(),
            question_id: q_id,
            expected_revision: 1,
            answer: "Approve".to_string(),
            now_ms: 1_020,
        },
    )
    .expect("answer");
    core.tick(1_030).expect("tick complete");

    // Events 4 (QuestionAnswered) and 5 (RunCompleted) are now emitted.
    // Poll after_seq = Some(3) with limit = 1 and walk cursor.
    let delta_1 = core
        .list_activity(bid, Some(3), None, 1, None, None)
        .expect("delta page 1");
    assert_eq!(delta_1.after_seq, 3);
    assert_eq!(delta_1.high_water_mark_seq, 5);
    assert!(delta_1.has_more);
    assert_eq!(delta_1.events.len(), 1);
    assert_eq!(delta_1.events[0].seq, 4);
    assert_eq!(delta_1.events[0].kind, EventKind::QuestionAnswered);

    let delta_2 = core
        .list_activity(bid, None, delta_1.next_cursor.as_deref(), 1, None, None)
        .expect("delta page 2");
    assert_eq!(delta_2.after_seq, 4);
    assert_eq!(delta_2.high_water_mark_seq, 5);
    assert!(!delta_2.has_more);
    assert_eq!(delta_2.next_cursor, None);
    assert_eq!(delta_2.events.len(), 1);
    assert_eq!(delta_2.events[0].seq, 5);
    assert_eq!(delta_2.events[0].kind, EventKind::RunCompleted);

    // Interleave with PullEvents / AcknowledgeEvents: follower cursors start at 0 and advance independently.
    let b_state = core.get_binding(bid).expect("binding before pull");
    assert_eq!(b_state.delivered_seq, 0);
    assert_eq!(b_state.acked_seq, 0);

    let batch_1 = core
        .execute(
            bid,
            Command::PullEvents {
                after_seq: None,
                limit: 2,
                now_ms: 1_040,
            },
        )
        .expect("pull 2")
        .into_event_batch()
        .expect("batch 1");
    assert_eq!(batch_1.from_seq, 1);
    assert_eq!(batch_1.to_seq, 2);
    assert_eq!(batch_1.delivered_seq, 2);
    assert_eq!(batch_1.acked_seq, 0);

    // list_activity can still read seq 1..=5 or after_seq = 4 even though delivered_seq is 2 and acked_seq is 0.
    let activity_beyond_delivered = core
        .list_activity(bid, Some(2), None, 10, None, None)
        .expect("list_activity beyond delivered_seq");
    assert_eq!(
        activity_beyond_delivered
            .events
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );

    let b_state_after = core.get_binding(bid).expect("binding after activity read");
    assert_eq!(b_state_after.delivered_seq, 2);
    assert_eq!(b_state_after.acked_seq, 0);

    let ack_1 = core
        .execute(
            bid,
            Command::AcknowledgeEvents {
                command_key: "lv-ack-1".to_string(),
                batch_id: batch_1.batch_id,
                now_ms: 1_050,
            },
        )
        .expect("ack 1")
        .into_ack_result()
        .expect("ack result");
    assert_eq!(ack_1.acked_seq, 2);
    assert!(ack_1.advanced);
}

#[test]
fn cursor_filter_scope_and_parameter_validation_rejections() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("activity_validation.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding_a = core.create_binding(UUID_A, "Binding A").expect("b_a");
    let binding_b = core.create_binding(UUID_B, "Binding B").expect("b_b");
    let bid_a = &binding_a.binding_id;
    let bid_b = &binding_b.binding_id;

    // Seed 2 runs on A and 1 run on B.
    let run_a1 = core
        .execute(
            bid_a,
            Command::StartDemo {
                command_key: "va-1".to_string(),
                instruction: "A1".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_000,
            },
        )
        .expect("start a1")
        .into_run()
        .expect("a1")
        .run_id;
    core.tick(1_010).expect("tick a1");

    let run_a2 = core
        .execute(
            bid_a,
            Command::StartDemo {
                command_key: "va-2".to_string(),
                instruction: "A2".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_020,
            },
        )
        .expect("start a2")
        .into_run()
        .expect("a2")
        .run_id;
    core.tick(1_030).expect("tick a2");

    let run_b1 = core
        .execute(
            bid_b,
            Command::StartDemo {
                command_key: "vb-1".to_string(),
                instruction: "B1".to_string(),
                step_delay_ms: Some(10),
                now_ms: 1_040,
            },
        )
        .expect("start b1")
        .into_run()
        .expect("b1")
        .run_id;
    core.tick(1_050).expect("tick b1");

    let unfiltered_cursor_a = core
        .list_activity(bid_a, None, None, 1, None, None)
        .expect("unfiltered page 1")
        .next_cursor
        .expect("unfiltered cursor a");
    let run_filtered_cursor_a1 = core
        .list_activity(bid_a, None, None, 1, Some(&run_a1), None)
        .expect("run filtered page 1")
        .next_cursor
        .expect("run filtered cursor a1");
    let kind_filtered_cursor_a = core
        .list_activity(bid_a, None, None, 1, None, Some(EventKind::RunStarted))
        .expect("kind filtered page 1")
        .next_cursor
        .expect("kind filtered cursor a");

    // 1. Simultaneous cursor and after_seq must be rejected.
    assert!(matches!(
        core.list_activity(bid_a, Some(1), Some(&unfiltered_cursor_a), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(bid_a, Some(0), Some(&unfiltered_cursor_a), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));

    // 2. Future after_seq (> high_water_mark_seq) must be rejected.
    assert!(matches!(
        core.list_activity(bid_a, Some(7), None, 10, None, None),
        Err(CoreError::Validation {
            field: "after_seq",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(bid_a, Some(u64::MAX), None, 10, None, None),
        Err(CoreError::Validation {
            field: "after_seq",
            ..
        })
    ));

    // 3. Invalid limits (0, MAX_PAGE_LIMIT + 1, usize::MAX) must be rejected.
    for bad_limit in [0_usize, MAX_PAGE_LIMIT + 1, usize::MAX] {
        assert!(matches!(
            core.list_activity(bid_a, None, None, bad_limit, None, None),
            Err(CoreError::Validation { field: "limit", .. })
        ));
    }

    // 4. Invalid run_id filters must be rejected via core limits.
    for bad_run_id in [
        "".to_string(),
        "   ".to_string(),
        " leading".to_string(),
        "trailing ".to_string(),
        "bad\0nul".to_string(),
        "bad\nctrl".to_string(),
        "-".to_string(),
        "run:colon".to_string(),
        "r".repeat(MAX_ID_LEN + 1),
    ] {
        assert!(
            matches!(
                core.list_activity(bid_a, None, None, 10, Some(&bad_run_id), None),
                Err(CoreError::Validation {
                    field: "run_id",
                    ..
                })
            ),
            "expected invalid run_id `{bad_run_id}` to be rejected"
        );
    }

    // 5. Cross-binding cursor misuse must be rejected.
    assert!(matches!(
        core.list_activity(bid_b, None, Some(&unfiltered_cursor_a), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(
            bid_b,
            None,
            Some(&run_filtered_cursor_a1),
            10,
            Some(&run_b1),
            None
        ),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));

    // 6. Filter mismatch between cursor and request must be rejected.
    assert!(matches!(
        core.list_activity(
            bid_a,
            None,
            Some(&unfiltered_cursor_a),
            10,
            Some(&run_a1),
            None
        ),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(
            bid_a,
            None,
            Some(&unfiltered_cursor_a),
            10,
            None,
            Some(EventKind::RunStarted)
        ),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(
            bid_a,
            None,
            Some(&run_filtered_cursor_a1),
            10,
            Some(&run_a2),
            None
        ),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(bid_a, None, Some(&run_filtered_cursor_a1), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(
            bid_a,
            None,
            Some(&kind_filtered_cursor_a),
            10,
            None,
            Some(EventKind::RunProgress)
        ),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_activity(bid_a, None, Some(&kind_filtered_cursor_a), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));

    // 7. Cross-resource cursor misuse in both directions must be rejected.
    let runs_cursor = core
        .list_runs(bid_a, None, 1)
        .expect("list_runs")
        .next_cursor
        .expect("runs cursor");
    let questions_cursor = core
        .list_run_questions(bid_a, &run_a1, None, 1)
        .expect("list_run_questions")
        .next_cursor;
    assert!(matches!(
        core.list_activity(bid_a, None, Some(&runs_cursor), 10, None, None),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_runs(bid_a, Some(&unfiltered_cursor_a), 10),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_run_revisions(bid_a, &run_a1, Some(&unfiltered_cursor_a), 10),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    assert!(matches!(
        core.list_run_questions(bid_a, &run_a1, Some(&unfiltered_cursor_a), 10),
        Err(CoreError::Validation {
            field: "cursor",
            ..
        })
    ));
    if let Some(qc) = questions_cursor {
        assert!(matches!(
            core.list_activity(bid_a, None, Some(&qc), 10, None, None),
            Err(CoreError::Validation {
                field: "cursor",
                ..
            })
        ));
    }

    // 8. Malformed, out-of-order, leading-zero, future, and overflowing activity cursors.
    for bad_cursor in [
        "".to_string(),
        "   ".to_string(),
        "x".repeat(MAX_CURSOR_LEN + 1),
        format!("tlc2:activity:{bid_a}:-:-:1:6"),
        format!("tlc1:activity:{bid_a}:-:1:6"),
        format!("tlc1:activity:{bid_a}:-:-:1:6:extra"),
        format!("tlc1:activity:{bid_a}:-:-:0:6"),
        format!("tlc1:activity:{bid_a}:-:-:01:6"),
        format!("tlc1:activity:{bid_a}:-:-:1:06"),
        format!("tlc1:activity:{bid_a}:-:-:6:6"),
        format!("tlc1:activity:{bid_a}:-:-:6:5"),
        format!("tlc1:activity:{bid_a}:-:-:1:7"),
        format!("tlc1:activity:{bid_a}:-:-:1:999"),
        format!("tlc1:activity:{bid_a}:-:-:998:999"),
        format!("tlc1:activity:{bid_a}:-:unknown_kind:1:6"),
        format!("tlc1:activity:{bid_a}:-:-:1:{}", u64::MAX),
        format!("tlc1:activity:{bid_a}:-:-:1:{}", (i64::MAX as u64) + 1),
    ] {
        assert!(
            matches!(
                core.list_activity(bid_a, None, Some(&bad_cursor), 10, None, None),
                Err(CoreError::Validation {
                    field: "cursor",
                    ..
                })
            ),
            "expected invalid activity cursor `{bad_cursor}` to be rejected"
        );
    }
}

#[test]
fn page_response_256kib_budget_reduces_page_without_truncating_event_payloads() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("activity_budget.db");
    let mut core = Core::open(&db_path).expect("open core");

    let binding = core
        .create_binding(UUID_A, "Budget Binding")
        .expect("binding");
    let bid = &binding.binding_id;

    let make_max_instruction = |rev: u64| -> String {
        let suffix = format!("act-rev-{rev:03}-");
        let control_prefix = "\u{1}".repeat(2_410);
        let pad_len = MAX_INSTRUCTION_LEN - control_prefix.len() - suffix.len();
        format!("{control_prefix}{suffix}{}", "z".repeat(pad_len))
    };

    let inst_1 = make_max_instruction(1);
    let mut now_ms = 5_000_i64;
    let run = core
        .execute(
            bid,
            Command::StartDemo {
                command_key: "budget-start".to_string(),
                instruction: inst_1.clone(),
                step_delay_ms: Some(10),
                now_ms,
            },
        )
        .expect("start budget run")
        .into_run()
        .expect("run");
    now_ms += 10;
    core.tick(now_ms).expect("tick 1");

    let total_revisions = 15_u64;
    for rev in 2..=total_revisions {
        now_ms += 10;
        core.execute(
            bid,
            Command::Steer {
                command_key: format!("budget-steer-{rev}"),
                run_id: run.run_id.clone(),
                expected_revision: rev - 1,
                instruction: make_max_instruction(rev),
                now_ms,
            },
        )
        .expect("steer budget run");
        now_ms += 10;
        core.tick(now_ms).expect("tick after steer");
    }

    // Walk all events with limit = MAX_PAGE_LIMIT (50) and verify the 256 KiB budget splits pages
    // cleanly without truncating any event's instruction payload or skipping any sequence number.
    let mut collected_events = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_count = 0_usize;
    let mut expected_after_seq = 0_u64;

    loop {
        let page = core
            .list_activity(bid, None, cursor.as_deref(), MAX_PAGE_LIMIT, None, None)
            .expect("list_activity budget page");
        let wire_bytes = estimate_wire_response_bytes(&CommandOutput::Activity(page.clone()))
            .expect("estimate wire bytes");
        assert!(
            wire_bytes <= MAX_PAGE_RESPONSE_BYTES,
            "activity page wire bytes {wire_bytes} exceeded {MAX_PAGE_RESPONSE_BYTES}"
        );
        assert!(!page.events.is_empty());
        assert_eq!(page.after_seq, expected_after_seq);
        page_count += 1;

        expected_after_seq = page.events.last().expect("last event").seq;
        collected_events.extend(page.events);

        if page.has_more {
            cursor = Some(page.next_cursor.expect("next_cursor when has_more"));
        } else {
            assert!(page.next_cursor.is_none());
            break;
        }
    }

    assert!(
        page_count >= 3,
        "15 max-escaped revisions of activity events must span at least 3 256KiB-bounded pages, got {page_count}"
    );
    for (idx, ev) in collected_events.iter().enumerate() {
        assert_eq!(ev.seq, (idx as u64) + 1);
        if ev.kind == EventKind::RunStarted || ev.kind == EventKind::RunSteered {
            assert_eq!(
                ev.payload.instruction.as_deref(),
                Some(make_max_instruction(ev.revision).as_str()),
                "event payload instruction at seq {} rev {} must never be truncated",
                ev.seq,
                ev.revision
            );
        }
    }
}
