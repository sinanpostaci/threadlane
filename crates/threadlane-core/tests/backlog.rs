use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_core::{
    BacklogHistoryAction, BacklogHistoryEntry, BacklogItemEdit, BacklogItemKind, BacklogStatus,
    Command, Core, CoreError, NewBacklogItem, WakeupStatus, MAX_BACKLOG_ACCEPTANCE_LEN,
    MAX_BACKLOG_BLOCKED_REASON_LEN, MAX_BACKLOG_COMPLETION_EVIDENCE_LEN,
    MAX_BACKLOG_COMPLETION_NOTE_LEN, MAX_BACKLOG_DELIVERY_LEN, MAX_BACKLOG_TITLE_LEN,
    MAX_COMMAND_KEY_LEN, MAX_CURSOR_LEN, MAX_ID_LEN, MAX_PAGE_LIMIT,
};

const THREAD_A: &str = "11111111-1111-4111-8111-111111111111";
const THREAD_B: &str = "22222222-2222-4222-8222-222222222222";

fn open_temp_core() -> (TempDir, PathBuf, Core) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("threadlane.db");
    let core = Core::open(&db_path).expect("open core");
    (dir, db_path, core)
}

fn create_binding(core: &mut Core, thread_uuid: &str, label: &str) -> String {
    core.create_binding(thread_uuid, label)
        .expect("create binding")
        .binding_id
}

fn coworker_item(title: &str) -> NewBacklogItem {
    NewBacklogItem {
        title: title.to_string(),
        acceptance_condition: format!("{title} satisfies its acceptance condition"),
        delivery_description: format!("Deliver {title}"),
        kind: BacklogItemKind::Coworker,
    }
}

fn worker_item(title: &str) -> NewBacklogItem {
    NewBacklogItem {
        title: title.to_string(),
        acceptance_condition: format!("{title} satisfies its acceptance condition"),
        delivery_description: format!("Deliver {title}"),
        kind: BacklogItemKind::Worker,
    }
}

fn history_entries(core: &Core, binding_id: &str, item_id: &str) -> Vec<BacklogHistoryEntry> {
    let mut all_entries = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = core
            .list_backlog_history(binding_id, item_id, cursor.as_deref(), MAX_PAGE_LIMIT)
            .expect("list backlog history");
        all_entries.extend(page.entries);
        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }
    all_entries
}

fn history_count(core: &Core, binding_id: &str, item_id: &str) -> usize {
    history_entries(core, binding_id, item_id).len()
}

fn assert_validation(err: CoreError, expected_field: &str) {
    match err {
        CoreError::Validation { field, reason } => {
            assert_eq!(
                field, expected_field,
                "unexpected validation field (reason: {reason})"
            );
        }
        other => panic!("expected validation error for `{expected_field}`, got {other:?}"),
    }
}

fn assert_stale_revision(err: CoreError, expected_revision: u64, actual_revision: u64) {
    match err {
        CoreError::StaleRevision {
            expected_revision: expected,
            actual_revision: actual,
            ..
        } => {
            assert_eq!(expected, expected_revision);
            assert_eq!(actual, actual_revision);
        }
        other => panic!("expected stale revision error, got {other:?}"),
    }
}

fn assert_idempotency_conflict(err: CoreError) {
    match err {
        CoreError::IdempotencyConflict { .. } => {}
        other => panic!("expected idempotency conflict, got {other:?}"),
    }
}

#[test]
fn jobless_create_and_reads_do_not_create_runs_workers_events_or_notices() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "jobless backlog");

    let created = core
        .create_backlog_item(
            &binding_id,
            "create-worker-item",
            worker_item("Storage slice"),
            1_000,
        )
        .expect("create backlog item");

    assert!(created.item_id.starts_with("blg_"));
    assert_eq!(created.binding_id, binding_id);
    assert_eq!(created.revision, 1);
    assert_eq!(created.title, "Storage slice");
    assert_eq!(created.kind, BacklogItemKind::Worker);
    assert_eq!(created.status, BacklogStatus::Todo);
    assert!(created.blocked_reason.is_none());
    assert!(created.current_execution_ref.is_none());
    assert!(created.completion_note.is_none());
    assert!(created.completion_evidence.is_none());
    assert!(created.completed_at_ms.is_none());
    assert_eq!(created.created_at_ms, 1_000);
    assert_eq!(created.updated_at_ms, 1_000);

    let loaded = core
        .get_backlog_item(&binding_id, &created.item_id)
        .expect("read backlog item");
    assert_eq!(loaded, created);

    let page = core
        .list_backlog_items(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("list backlog items");
    assert_eq!(page.binding_id, binding_id);
    assert_eq!(page.items, vec![created.clone()]);
    assert!(!page.has_more);
    assert!(page.next_cursor.is_none());
    assert!(page.high_water_mark_rowid > 0);

    let entries = history_entries(&core, &binding_id, &created.item_id);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].action, BacklogHistoryAction::Created);
    assert_eq!(entries[0].revision, 1);
    assert_eq!(entries[0].item, created);
    assert_eq!(entries[0].recorded_at_ms, 1_000);

    assert!(core
        .list_runs(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("list runs")
        .runs
        .is_empty());
    assert!(core
        .list_activity(&binding_id, None, None, MAX_PAGE_LIMIT, None, None)
        .expect("list activity")
        .events
        .is_empty());
    assert_eq!(
        core.get_wakeup(&binding_id).expect("get wakeup").status,
        WakeupStatus::Idle
    );
    let idle = core.inspect_runtime_idle().expect("inspect idle");
    assert_eq!(idle.non_terminal_runs, 0);
    assert_eq!(idle.non_terminal_workers, 0);
    assert_eq!(idle.pending_wakeups, 0);
    assert_eq!(idle.in_flight_wakeups, 0);
}

#[test]
fn reopen_persists_items_and_idempotent_replay_adds_no_revisions_or_history() {
    let (_dir, db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "reopen backlog");

    let created_coworker = core
        .create_backlog_item(
            &binding_id,
            "create-coworker",
            coworker_item("Attested work"),
            1_000,
        )
        .expect("create coworker item");
    let started = core
        .start_backlog_item(
            &binding_id,
            "start-coworker",
            &created_coworker.item_id,
            1,
            1_010,
        )
        .expect("start coworker item");
    assert_eq!(started.status, BacklogStatus::Doing);
    assert_eq!(started.revision, 2);
    let completed = core
        .complete_backlog_item(
            &binding_id,
            "complete-coworker",
            &created_coworker.item_id,
            2,
            "Delivered the storage slice",
            "Reviewed behavior and focused tests",
            1_020,
        )
        .expect("complete coworker item");
    assert_eq!(completed.status, BacklogStatus::Done);
    assert_eq!(completed.revision, 3);

    let created_worker = core
        .create_backlog_item(
            &binding_id,
            "create-worker",
            worker_item("Manual edit"),
            1_030,
        )
        .expect("create worker item");
    let edited = core
        .edit_backlog_item(
            &binding_id,
            "edit-worker",
            &created_worker.item_id,
            1,
            BacklogItemEdit {
                title: Some("Manual edit clarified".to_string()),
                ..BacklogItemEdit::default()
            },
            1_040,
        )
        .expect("edit worker item");
    assert_eq!(edited.revision, 2);
    assert_eq!(edited.title, "Manual edit clarified");

    assert_eq!(
        history_count(&core, &binding_id, &created_coworker.item_id),
        3
    );
    assert_eq!(
        history_count(&core, &binding_id, &created_worker.item_id),
        2
    );

    drop(core);
    let mut reopened = Core::open(&db_path).expect("reopen core");
    assert_eq!(
        reopened
            .get_backlog_item(&binding_id, &created_coworker.item_id)
            .expect("read completed item after reopen"),
        completed
    );
    assert_eq!(
        reopened
            .get_backlog_item(&binding_id, &created_worker.item_id)
            .expect("read edited item after reopen"),
        edited
    );

    let replayed_create = reopened
        .create_backlog_item(
            &binding_id,
            "create-coworker",
            coworker_item("Attested work"),
            9_999,
        )
        .expect("replay create after reopen");
    assert_eq!(replayed_create, created_coworker);
    assert_eq!(
        reopened
            .get_backlog_item(&binding_id, &created_coworker.item_id)
            .expect("completed item remains done"),
        completed
    );
    assert_idempotency_conflict(
        reopened
            .create_backlog_item(
                &binding_id,
                "create-coworker",
                coworker_item("Changed payload after reopen"),
                9_999,
            )
            .expect_err("changed payload after reopen must conflict"),
    );
    assert_eq!(
        history_count(&reopened, &binding_id, &created_coworker.item_id),
        3
    );

    let replayed_start = reopened
        .start_backlog_item(
            &binding_id,
            "start-coworker",
            &created_coworker.item_id,
            1,
            9_999,
        )
        .expect("replay start after reopen");
    assert_eq!(replayed_start, started);

    let replayed_complete = reopened
        .complete_backlog_item(
            &binding_id,
            "complete-coworker",
            &created_coworker.item_id,
            2,
            "Delivered the storage slice",
            "Reviewed behavior and focused tests",
            9_999,
        )
        .expect("replay complete after reopen");
    assert_eq!(replayed_complete, completed);

    let replayed_edit = reopened
        .edit_backlog_item(
            &binding_id,
            "edit-worker",
            &created_worker.item_id,
            1,
            BacklogItemEdit {
                title: Some("Manual edit clarified".to_string()),
                ..BacklogItemEdit::default()
            },
            9_999,
        )
        .expect("replay edit after reopen");
    assert_eq!(replayed_edit, edited);

    assert_eq!(
        history_count(&reopened, &binding_id, &created_coworker.item_id),
        3
    );
    assert_eq!(
        history_count(&reopened, &binding_id, &created_worker.item_id),
        2
    );
    assert_eq!(
        reopened
            .get_backlog_item(&binding_id, &created_coworker.item_id)
            .expect("completed item unchanged by replay")
            .revision,
        3
    );

    let blocked = reopened
        .block_backlog_item(
            &binding_id,
            "block-worker-after-reopen",
            &created_worker.item_id,
            2,
            "waiting for the Worker reservation slice",
            1_050,
        )
        .expect("block after reopen");
    assert_eq!(blocked.status, BacklogStatus::Blocked);
    assert_eq!(blocked.revision, 3);
    let replayed_block = reopened
        .block_backlog_item(
            &binding_id,
            "block-worker-after-reopen",
            &created_worker.item_id,
            2,
            "waiting for the Worker reservation slice",
            9_999,
        )
        .expect("replay block after reopen");
    assert_eq!(replayed_block, blocked);
    assert_eq!(
        history_count(&reopened, &binding_id, &created_worker.item_id),
        3
    );
}

#[test]
fn bindings_isolate_items_history_cursors_and_reused_idempotency_keys() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_a = create_binding(&mut core, THREAD_A, "binding A");
    let binding_b = create_binding(&mut core, THREAD_B, "binding B");

    let item_a = core
        .create_backlog_item(
            &binding_a,
            "shared-create-key",
            worker_item("Only in A"),
            1_000,
        )
        .expect("create A item");
    let item_b = core
        .create_backlog_item(
            &binding_b,
            "shared-create-key",
            coworker_item("Only in B"),
            1_010,
        )
        .expect("create B item with the same command key");
    assert_eq!(item_a.binding_id, binding_a);
    assert_eq!(item_b.binding_id, binding_b);
    assert_ne!(item_a.item_id, item_b.item_id);

    assert_validation(
        core.get_backlog_item(&binding_b, &item_a.item_id)
            .expect_err("A item must not be visible from B"),
        "item_id",
    );
    assert_validation(
        core.get_backlog_item(&binding_a, &item_b.item_id)
            .expect_err("B item must not be visible from A"),
        "item_id",
    );
    assert_validation(
        core.list_backlog_history(&binding_b, &item_a.item_id, None, 1)
            .expect_err("A item history must not be visible from B"),
        "item_id",
    );
    assert_eq!(
        core.get_backlog_item(&binding_a, &item_a.item_id)
            .expect("A item unchanged"),
        item_a
    );
    assert_eq!(
        core.get_backlog_item(&binding_b, &item_b.item_id)
            .expect("B item unchanged"),
        item_b
    );
    assert_eq!(history_count(&core, &binding_a, &item_a.item_id), 1);
    assert_eq!(history_count(&core, &binding_b, &item_b.item_id), 1);

    let page_a = core
        .list_backlog_items(&binding_a, None, MAX_PAGE_LIMIT)
        .expect("list A items");
    assert_eq!(page_a.items, vec![item_a.clone()]);
    let page_b = core
        .list_backlog_items(&binding_b, None, MAX_PAGE_LIMIT)
        .expect("list B items");
    assert_eq!(page_b.items, vec![item_b.clone()]);

    let second_a = core
        .create_backlog_item(&binding_a, "second-a", worker_item("Second A item"), 1_020)
        .expect("create second A item");
    let first_page_a = core
        .list_backlog_items(&binding_a, None, 1)
        .expect("first A page");
    assert_eq!(first_page_a.items.len(), 1);
    let cursor_from_a = first_page_a.next_cursor.expect("A continuation cursor");
    assert_validation(
        core.list_backlog_items(&binding_b, Some(cursor_from_a.as_str()), 1)
            .expect_err("A item cursor must not be usable for B"),
        "cursor",
    );
    assert_validation(
        core.list_backlog_history(
            &binding_a,
            &second_a.item_id,
            Some(cursor_from_a.as_str()),
            1,
        )
        .expect_err("item cursor must not be usable as history cursor"),
        "cursor",
    );

    let edited_b = core
        .edit_backlog_item(
            &binding_b,
            "edit-b-item",
            &item_b.item_id,
            1,
            BacklogItemEdit {
                title: Some("Still only in B".to_string()),
                ..BacklogItemEdit::default()
            },
            1_030,
        )
        .expect("edit B item");
    let first_history_page_b = core
        .list_backlog_history(&binding_b, &item_b.item_id, None, 1)
        .expect("first B history page");
    let history_cursor_b = first_history_page_b
        .next_cursor
        .expect("B history continuation cursor");
    assert_validation(
        core.list_backlog_history(
            &binding_a,
            &item_a.item_id,
            Some(history_cursor_b.as_str()),
            1,
        )
        .expect_err("B history cursor must not be usable for A"),
        "cursor",
    );
    assert_eq!(edited_b.revision, 2);

    assert_idempotency_conflict(
        core.create_backlog_item(
            &binding_a,
            "shared-create-key",
            worker_item("Changed payload in A"),
            1_040,
        )
        .expect_err("changed payload with reused key must conflict"),
    );
    assert_eq!(
        core.get_backlog_item(&binding_a, &item_a.item_id)
            .expect("A item unaffected by conflict"),
        item_a
    );
    assert_eq!(history_count(&core, &binding_a, &item_a.item_id), 1);

    let replayed_b = core
        .create_backlog_item(
            &binding_b,
            "shared-create-key",
            coworker_item("Only in B"),
            1_050,
        )
        .expect("same key and payload in another binding is independent");
    assert_eq!(replayed_b, item_b);

    assert_validation(
        core.edit_backlog_item(
            &binding_b,
            "cross-binding-edit",
            &item_a.item_id,
            1,
            BacklogItemEdit {
                title: Some("must not apply".to_string()),
                ..BacklogItemEdit::default()
            },
            1_060,
        )
        .expect_err("A item must not be mutable through B"),
        "item_id",
    );
    assert_eq!(
        core.get_backlog_item(&binding_a, &item_a.item_id)
            .expect("A item remains unchanged"),
        item_a
    );
    assert_eq!(history_count(&core, &binding_a, &item_a.item_id), 1);
}

#[test]
fn changed_payloads_and_reused_keys_conflict_without_side_effects() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "conflicts");
    let created = core
        .create_backlog_item(
            &binding_id,
            "create",
            coworker_item("Conflict target"),
            1_000,
        )
        .expect("create item");

    let edited = core
        .edit_backlog_item(
            &binding_id,
            "edit",
            &created.item_id,
            1,
            BacklogItemEdit {
                title: Some("First edit".to_string()),
                ..BacklogItemEdit::default()
            },
            1_010,
        )
        .expect("first edit");
    assert_eq!(edited.revision, 2);

    assert_idempotency_conflict(
        core.edit_backlog_item(
            &binding_id,
            "edit",
            &created.item_id,
            1,
            BacklogItemEdit {
                title: Some("Changed edit payload".to_string()),
                ..BacklogItemEdit::default()
            },
            1_020,
        )
        .expect_err("changed edit payload must conflict"),
    );
    let unchanged = core
        .get_backlog_item(&binding_id, &created.item_id)
        .expect("read unchanged item");
    assert_eq!(unchanged, edited);
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 2);

    let started = core
        .start_backlog_item(&binding_id, "start", &created.item_id, 2, 1_030)
        .expect("start item");
    assert_eq!(started.revision, 3);
    assert_idempotency_conflict(
        core.start_backlog_item(&binding_id, "start", &created.item_id, 3, 1_040)
            .expect_err("changed start revision must conflict"),
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("started item unchanged"),
        started
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 3);

    let blocked = core
        .block_backlog_item(
            &binding_id,
            "block",
            &created.item_id,
            3,
            "waiting for review",
            1_050,
        )
        .expect("block item");
    assert_eq!(blocked.revision, 4);
    assert_idempotency_conflict(
        core.block_backlog_item(
            &binding_id,
            "block",
            &created.item_id,
            3,
            "different reason",
            1_060,
        )
        .expect_err("changed block reason must conflict"),
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("blocked item unchanged"),
        blocked
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 4);

    let restarted = core
        .start_backlog_item(&binding_id, "restart", &created.item_id, 4, 1_070)
        .expect("restart blocked item");
    assert_eq!(restarted.revision, 5);
    let completed = core
        .complete_backlog_item(
            &binding_id,
            "complete",
            &created.item_id,
            5,
            "Delivered revision five",
            "Checked acceptance evidence",
            1_080,
        )
        .expect("complete item");
    assert_eq!(completed.revision, 6);
    assert_idempotency_conflict(
        core.complete_backlog_item(
            &binding_id,
            "complete",
            &created.item_id,
            5,
            "Changed completion note",
            "Checked acceptance evidence",
            1_090,
        )
        .expect_err("changed completion payload must conflict"),
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("completed item unchanged"),
        completed
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 6);

    assert_idempotency_conflict(
        core.create_backlog_item(&binding_id, "create", worker_item("Conflict target"), 1_100)
            .expect_err("changed create kind must conflict"),
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 6);
}

#[test]
fn stale_edits_and_invalid_transitions_do_not_mutate_revision_or_history() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "transitions");
    let created = core
        .create_backlog_item(
            &binding_id,
            "create",
            coworker_item("Transition target"),
            1_000,
        )
        .expect("create item");

    assert_stale_revision(
        core.edit_backlog_item(
            &binding_id,
            "stale-edit",
            &created.item_id,
            2,
            BacklogItemEdit {
                title: Some("stale".to_string()),
                ..BacklogItemEdit::default()
            },
            1_010,
        )
        .expect_err("stale edit must fail"),
        2,
        1,
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("item after stale edit"),
        created
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 1);

    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "complete-from-todo",
            &created.item_id,
            1,
            "not allowed",
            "not allowed",
            1_020,
        )
        .expect_err("completion from Todo must fail"),
        "status",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("item after invalid completion"),
        created
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 1);

    let blocked = core
        .block_backlog_item(
            &binding_id,
            "block",
            &created.item_id,
            1,
            "waiting for input",
            1_030,
        )
        .expect("block item");
    assert_eq!(blocked.status, BacklogStatus::Blocked);
    assert_eq!(blocked.revision, 2);
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "complete-from-blocked",
            &created.item_id,
            2,
            "not allowed",
            "not allowed",
            1_040,
        )
        .expect_err("completion from Blocked must fail"),
        "status",
    );
    assert_validation(
        core.edit_backlog_item(
            &binding_id,
            "edit-blocked",
            &created.item_id,
            2,
            BacklogItemEdit {
                title: Some("blocked edit".to_string()),
                ..BacklogItemEdit::default()
            },
            1_050,
        )
        .expect_err("ordinary edit from Blocked must fail"),
        "status",
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 2);

    let started = core
        .start_backlog_item(&binding_id, "start", &created.item_id, 2, 1_060)
        .expect("start blocked item");
    assert_eq!(started.status, BacklogStatus::Doing);
    assert_eq!(started.revision, 3);
    assert!(started.blocked_reason.is_none());

    assert_validation(
        core.start_backlog_item(&binding_id, "start-again", &created.item_id, 3, 1_070)
            .expect_err("starting a Doing item must fail"),
        "status",
    );
    assert_validation(
        core.edit_backlog_item(
            &binding_id,
            "edit-acceptance-doing",
            &created.item_id,
            3,
            BacklogItemEdit {
                acceptance_condition: Some("changed acceptance".to_string()),
                ..BacklogItemEdit::default()
            },
            1_080,
        )
        .expect_err("acceptance edit during Doing must fail"),
        "acceptance_condition",
    );
    assert_validation(
        core.edit_backlog_item(
            &binding_id,
            "edit-delivery-doing",
            &created.item_id,
            3,
            BacklogItemEdit {
                delivery_description: Some("changed delivery".to_string()),
                ..BacklogItemEdit::default()
            },
            1_085,
        )
        .expect_err("delivery edit during Doing must fail"),
        "delivery_description",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("item after frozen Doing edits"),
        started
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 3);

    let clarified = core
        .edit_backlog_item(
            &binding_id,
            "edit-title-doing",
            &created.item_id,
            3,
            BacklogItemEdit {
                title: Some("Clarified while doing".to_string()),
                ..BacklogItemEdit::default()
            },
            1_090,
        )
        .expect("title clarification during Doing is allowed");
    assert_eq!(clarified.revision, 4);
    assert_eq!(clarified.status, BacklogStatus::Doing);

    let blocked_again = core
        .block_backlog_item(
            &binding_id,
            "block-again",
            &created.item_id,
            4,
            "needs a decision",
            1_100,
        )
        .expect("block doing item");
    assert_eq!(blocked_again.revision, 5);
    let restarted = core
        .start_backlog_item(&binding_id, "restart", &created.item_id, 5, 1_110)
        .expect("restart blocked item");
    assert_eq!(restarted.revision, 6);
    let completed = core
        .complete_backlog_item(
            &binding_id,
            "complete",
            &created.item_id,
            6,
            "Delivered after decision",
            "Checked acceptance",
            1_120,
        )
        .expect("complete doing item");
    assert_eq!(completed.status, BacklogStatus::Done);
    assert_eq!(completed.revision, 7);
    assert_eq!(
        history_count(&core, &binding_id, &created.item_id),
        7,
        "created, blocked, started, edited, blocked, started, completed"
    );

    for (key, expected_validation_field) in [
        ("block-done", "status"),
        ("start-done", "status"),
        ("edit-done", "status"),
        ("complete-done", "status"),
    ] {
        let error = match key {
            "block-done" => core
                .block_backlog_item(&binding_id, key, &created.item_id, 7, "cannot block", 1_130)
                .expect_err("blocking a Done item must fail"),
            "start-done" => core
                .start_backlog_item(&binding_id, key, &created.item_id, 7, 1_130)
                .expect_err("starting a Done item must fail"),
            "edit-done" => core
                .edit_backlog_item(
                    &binding_id,
                    key,
                    &created.item_id,
                    7,
                    BacklogItemEdit {
                        title: Some("cannot edit".to_string()),
                        ..BacklogItemEdit::default()
                    },
                    1_130,
                )
                .expect_err("editing a Done item must fail"),
            "complete-done" => core
                .complete_backlog_item(
                    &binding_id,
                    key,
                    &created.item_id,
                    7,
                    "cannot complete twice",
                    "cannot complete twice",
                    1_130,
                )
                .expect_err("completing a Done item must fail"),
            _ => unreachable!("known key"),
        };
        assert_validation(error, expected_validation_field);
    }
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("Done item unchanged"),
        completed
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 7);
}

#[test]
fn worker_items_reject_manual_start_completion_and_kind_bypass() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "worker guards");
    let created = core
        .create_backlog_item(
            &binding_id,
            "create-worker",
            worker_item("Worker guarded item"),
            1_000,
        )
        .expect("create worker item");
    assert_eq!(created.kind, BacklogItemKind::Worker);

    assert_validation(
        core.start_backlog_item(&binding_id, "start-worker", &created.item_id, 1, 1_010)
            .expect_err("Worker start must be rejected"),
        "kind",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "complete-worker",
            &created.item_id,
            1,
            "fake worker success",
            "fake worker evidence",
            1_020,
        )
        .expect_err("Worker completion must be rejected"),
        "kind",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("Worker item after rejected transitions"),
        created
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 1);

    let edited = core
        .edit_backlog_item(
            &binding_id,
            "edit-worker-todo",
            &created.item_id,
            1,
            BacklogItemEdit {
                title: Some("Worker title clarified".to_string()),
                acceptance_condition: Some("Worker acceptance clarified".to_string()),
                delivery_description: Some("Worker delivery clarified".to_string()),
            },
            1_030,
        )
        .expect("Worker edit while Todo is allowed");
    assert_eq!(edited.kind, BacklogItemKind::Worker);
    assert_eq!(edited.status, BacklogStatus::Todo);
    assert_eq!(edited.revision, 2);

    let blocked = core
        .block_backlog_item(
            &binding_id,
            "block-worker",
            &created.item_id,
            2,
            "needs a verified Worker reservation",
            1_040,
        )
        .expect("Worker block while Todo is allowed");
    assert_eq!(blocked.status, BacklogStatus::Blocked);
    assert_eq!(
        blocked.blocked_reason.as_deref(),
        Some("needs a verified Worker reservation")
    );
    assert_eq!(blocked.kind, BacklogItemKind::Worker);
    assert_eq!(blocked.revision, 3);

    assert_validation(
        core.start_backlog_item(
            &binding_id,
            "start-blocked-worker",
            &created.item_id,
            3,
            1_050,
        )
        .expect_err("blocked Worker start must be rejected"),
        "kind",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "complete-blocked-worker",
            &created.item_id,
            3,
            "fake",
            "fake",
            1_060,
        )
        .expect_err("blocked Worker completion must be rejected"),
        "kind",
    );
    assert_validation(
        core.edit_backlog_item(
            &binding_id,
            "edit-blocked-worker",
            &created.item_id,
            3,
            BacklogItemEdit {
                title: Some("must not apply".to_string()),
                ..BacklogItemEdit::default()
            },
            1_070,
        )
        .expect_err("Worker edit after Todo must be rejected"),
        "status",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("blocked Worker unchanged"),
        blocked
    );
    assert_eq!(blocked.kind, BacklogItemKind::Worker);
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 3);
}

#[test]
fn coworker_completion_requires_note_and_evidence_and_locks_final_done_state() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "completion evidence");
    let created = core
        .create_backlog_item(
            &binding_id,
            "create",
            coworker_item("Completion evidence"),
            1_000,
        )
        .expect("create item");
    let started = core
        .start_backlog_item(&binding_id, "start", &created.item_id, 1, 1_010)
        .expect("start item");
    assert_eq!(started.revision, 2);

    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "empty-note",
            &created.item_id,
            2,
            "   ",
            "checked evidence",
            1_020,
        )
        .expect_err("blank completion note must fail"),
        "note",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "empty-evidence",
            &created.item_id,
            2,
            "delivered",
            "",
            1_030,
        )
        .expect_err("empty completion evidence must fail"),
        "evidence",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "oversized-note",
            &created.item_id,
            2,
            &"n".repeat(MAX_BACKLOG_COMPLETION_NOTE_LEN + 1),
            "checked evidence",
            1_040,
        )
        .expect_err("oversized completion note must fail"),
        "note",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "oversized-evidence",
            &created.item_id,
            2,
            "delivered",
            &"e".repeat(MAX_BACKLOG_COMPLETION_EVIDENCE_LEN + 1),
            1_050,
        )
        .expect_err("oversized completion evidence must fail"),
        "evidence",
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 2);
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("item after rejected completion"),
        started
    );

    let completed = core
        .complete_backlog_item(
            &binding_id,
            "complete",
            &created.item_id,
            2,
            "Delivered the bounded backlog storage",
            "Reviewed creation, edits, history, and rejection tests",
            1_060,
        )
        .expect("complete item");
    assert_eq!(completed.status, BacklogStatus::Done);
    assert_eq!(completed.revision, 3);
    assert_eq!(
        completed.completion_note.as_deref(),
        Some("Delivered the bounded backlog storage")
    );
    assert_eq!(
        completed.completion_evidence.as_deref(),
        Some("Reviewed creation, edits, history, and rejection tests")
    );
    assert_eq!(completed.completed_at_ms, Some(1_060));
    assert!(completed.blocked_reason.is_none());
    assert!(completed.current_execution_ref.is_none());

    assert_validation(
        core.edit_backlog_item(
            &binding_id,
            "edit-done",
            &created.item_id,
            3,
            BacklogItemEdit {
                title: Some("too late".to_string()),
                ..BacklogItemEdit::default()
            },
            1_070,
        )
        .expect_err("editing Done must fail"),
        "status",
    );
    assert_validation(
        core.complete_backlog_item(
            &binding_id,
            "complete-again",
            &created.item_id,
            3,
            "changed note",
            "changed evidence",
            1_080,
        )
        .expect_err("completing Done again must fail"),
        "status",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("Done item remains immutable"),
        completed
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 3);
}

#[test]
fn blocked_reasons_are_recorded_and_coworker_items_resume() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "blocking");
    let created = core
        .create_backlog_item(&binding_id, "create", coworker_item("Blocked work"), 1_000)
        .expect("create item");
    let started = core
        .start_backlog_item(&binding_id, "start", &created.item_id, 1, 1_010)
        .expect("start item");

    assert_validation(
        core.block_backlog_item(&binding_id, "empty-block", &created.item_id, 2, "  ", 1_020)
            .expect_err("blank block reason must fail"),
        "reason",
    );
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("item after blank block reason"),
        started
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 2);

    let blocked = core
        .block_backlog_item(
            &binding_id,
            "block",
            &created.item_id,
            2,
            "waiting for the human decision",
            1_030,
        )
        .expect("block item");
    assert_eq!(blocked.status, BacklogStatus::Blocked);
    assert_eq!(
        blocked.blocked_reason.as_deref(),
        Some("waiting for the human decision")
    );
    assert_eq!(blocked.revision, 3);
    assert_validation(
        core.block_backlog_item(
            &binding_id,
            "block-again",
            &created.item_id,
            3,
            "still blocked",
            1_040,
        )
        .expect_err("blocking a Blocked item must fail"),
        "status",
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 3);

    let resumed = core
        .start_backlog_item(&binding_id, "resume", &created.item_id, 3, 1_050)
        .expect("resume blocked item");
    assert_eq!(resumed.status, BacklogStatus::Doing);
    assert!(resumed.blocked_reason.is_none());
    assert_eq!(resumed.revision, 4);
    let completed = core
        .complete_backlog_item(
            &binding_id,
            "complete",
            &created.item_id,
            4,
            "Delivered after the decision",
            "Checked acceptance after the decision",
            1_060,
        )
        .expect("complete resumed item");
    assert_eq!(completed.status, BacklogStatus::Done);
    assert_eq!(completed.revision, 5);
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 5);
}

#[test]
fn item_and_history_pagination_are_bounded_stable_and_honest() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "pagination");

    let mut created_items = Vec::new();
    for index in 0..7 {
        created_items.push(
            core.create_backlog_item(
                &binding_id,
                &format!("create-item-{index}"),
                worker_item(&format!("Item {index}")),
                1_000 + index as i64,
            )
            .expect("create paginated item"),
        );
    }

    let first_page = core
        .list_backlog_items(&binding_id, None, 2)
        .expect("first item page");
    assert_eq!(
        first_page
            .items
            .iter()
            .map(|item| item.item_id.clone())
            .collect::<Vec<_>>(),
        vec![
            created_items[0].item_id.clone(),
            created_items[1].item_id.clone()
        ]
    );
    assert!(first_page.has_more);
    let high_water_mark = first_page.high_water_mark_rowid;
    let mut cursor = first_page.next_cursor.expect("first item cursor");

    let late_item = core
        .create_backlog_item(
            &binding_id,
            "create-late-item",
            worker_item("Late item"),
            2_000,
        )
        .expect("create late item");

    let mut collected_ids = first_page
        .items
        .iter()
        .map(|item| item.item_id.clone())
        .collect::<Vec<_>>();
    let mut page_count = 1;
    loop {
        let page = core
            .list_backlog_items(&binding_id, Some(cursor.as_str()), 2)
            .expect("continue item page");
        assert_eq!(page.high_water_mark_rowid, high_water_mark);
        page_count += 1;
        assert!(page_count <= 8, "pagination must terminate");
        collected_ids.extend(page.items.iter().map(|item| item.item_id.clone()));
        if !page.has_more {
            assert!(page.next_cursor.is_none());
            break;
        }
        cursor = page.next_cursor.expect("next item cursor");
    }
    assert_eq!(
        collected_ids,
        created_items
            .iter()
            .map(|item| item.item_id.clone())
            .collect::<Vec<_>>()
    );
    assert!(!collected_ids.contains(&late_item.item_id));

    let fresh_page = core
        .list_backlog_items(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("fresh item page");
    assert!(fresh_page
        .items
        .iter()
        .any(|item| item.item_id == late_item.item_id));

    let target = &created_items[0];
    for index in 0..5 {
        core.edit_backlog_item(
            &binding_id,
            &format!("history-edit-{index}"),
            &target.item_id,
            index + 1,
            BacklogItemEdit {
                title: Some(format!("History title {index}")),
                ..BacklogItemEdit::default()
            },
            3_000 + index as i64,
        )
        .expect("edit item for history");
    }
    assert_eq!(history_count(&core, &binding_id, &target.item_id), 6);

    let first_history_page = core
        .list_backlog_history(&binding_id, &target.item_id, None, 2)
        .expect("first history page");
    assert_eq!(
        first_history_page
            .entries
            .iter()
            .map(|entry| entry.revision)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        first_history_page.entries[0].action,
        BacklogHistoryAction::Created
    );
    assert_eq!(
        first_history_page.entries[1].action,
        BacklogHistoryAction::Edited
    );
    assert!(first_history_page.has_more);
    let history_high_water_mark = first_history_page.high_water_mark_rowid;
    let mut history_cursor = first_history_page
        .next_cursor
        .expect("first history cursor");

    core.edit_backlog_item(
        &binding_id,
        "history-edit-after-anchor",
        &target.item_id,
        6,
        BacklogItemEdit {
            title: Some("Late history title".to_string()),
            ..BacklogItemEdit::default()
        },
        4_000,
    )
    .expect("edit after history anchor");

    let mut collected_revisions = first_history_page
        .entries
        .iter()
        .map(|entry| entry.revision)
        .collect::<Vec<_>>();
    loop {
        let page = core
            .list_backlog_history(
                &binding_id,
                &target.item_id,
                Some(history_cursor.as_str()),
                2,
            )
            .expect("continue history page");
        assert_eq!(page.high_water_mark_rowid, history_high_water_mark);
        collected_revisions.extend(page.entries.iter().map(|entry| entry.revision));
        if !page.has_more {
            assert!(page.next_cursor.is_none());
            break;
        }
        history_cursor = page.next_cursor.expect("next history cursor");
    }
    assert_eq!(collected_revisions, vec![1, 2, 3, 4, 5, 6]);

    let fresh_history = history_entries(&core, &binding_id, &target.item_id);
    assert_eq!(
        fresh_history
            .iter()
            .map(|entry| entry.revision)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );

    for invalid_limit in [0, MAX_PAGE_LIMIT + 1] {
        assert_validation(
            core.list_backlog_items(&binding_id, None, invalid_limit)
                .expect_err("invalid item page limit"),
            "limit",
        );
        assert_validation(
            core.list_backlog_history(&binding_id, &target.item_id, None, invalid_limit)
                .expect_err("invalid history page limit"),
            "limit",
        );
    }
}

#[test]
fn utf8_and_oversized_inputs_are_bounded_without_side_effects() {
    let (_dir, _db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "bounded text");

    let unicode_item = coworker_item("计划 ✅ café");
    let unicode_item = NewBacklogItem {
        acceptance_condition: "交付条件已检查 ✅".to_string(),
        delivery_description: "交付有界文本存储 ✅".to_string(),
        ..unicode_item
    };
    let created = core
        .create_backlog_item(&binding_id, "create-unicode", unicode_item, 1_000)
        .expect("create Unicode item");
    assert_eq!(created.title, "计划 ✅ café");
    assert_eq!(created.acceptance_condition, "交付条件已检查 ✅");
    assert_eq!(created.delivery_description, "交付有界文本存储 ✅");

    let exact_limit_title = "é".repeat(MAX_BACKLOG_TITLE_LEN / 2);
    assert_eq!(exact_limit_title.len(), MAX_BACKLOG_TITLE_LEN);
    core.create_backlog_item(
        &binding_id,
        "create-exact-title-limit",
        NewBacklogItem {
            title: exact_limit_title,
            ..coworker_item("unused")
        },
        1_010,
    )
    .expect("UTF-8 title at the byte limit is allowed");

    let before_rejections = core
        .list_backlog_items(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("list before rejected inputs");
    assert_eq!(before_rejections.items.len(), 2);

    let oversized_title = "t".repeat(MAX_BACKLOG_TITLE_LEN + 1);
    assert_validation(
        core.create_backlog_item(
            &binding_id,
            "oversized-title",
            NewBacklogItem {
                title: oversized_title,
                ..coworker_item("unused")
            },
            1_020,
        )
        .expect_err("oversized title must fail"),
        "title",
    );
    assert_validation(
        core.create_backlog_item(
            &binding_id,
            "oversized-acceptance",
            NewBacklogItem {
                acceptance_condition: "a".repeat(MAX_BACKLOG_ACCEPTANCE_LEN + 1),
                ..coworker_item("unused")
            },
            1_030,
        )
        .expect_err("oversized acceptance condition must fail"),
        "acceptance_condition",
    );
    assert_validation(
        core.create_backlog_item(
            &binding_id,
            "oversized-delivery",
            NewBacklogItem {
                delivery_description: "d".repeat(MAX_BACKLOG_DELIVERY_LEN + 1),
                ..coworker_item("unused")
            },
            1_040,
        )
        .expect_err("oversized delivery description must fail"),
        "delivery_description",
    );
    assert_validation(
        core.create_backlog_item(
            &binding_id,
            &"k".repeat(MAX_COMMAND_KEY_LEN + 1),
            coworker_item("unused"),
            1_050,
        )
        .expect_err("oversized command key must fail"),
        "command_key",
    );

    assert_validation(
        core.get_backlog_item(&binding_id, &"i".repeat(MAX_ID_LEN + 1))
            .expect_err("oversized item id must fail"),
        "item_id",
    );
    let oversized_cursor = "c".repeat(MAX_CURSOR_LEN + 1);
    assert_validation(
        core.list_backlog_items(&binding_id, Some(oversized_cursor.as_str()), 1)
            .expect_err("oversized cursor must fail"),
        "cursor",
    );
    assert_validation(
        core.list_backlog_items(&binding_id, Some("not-a-cursor"), 1)
            .expect_err("malformed cursor must fail"),
        "cursor",
    );
    assert_validation(
        core.block_backlog_item(
            &binding_id,
            "oversized-block",
            &created.item_id,
            1,
            &"r".repeat(MAX_BACKLOG_BLOCKED_REASON_LEN + 1),
            1_060,
        )
        .expect_err("oversized block reason must fail"),
        "reason",
    );

    let after_rejections = core
        .list_backlog_items(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("list after rejected inputs");
    assert_eq!(after_rejections.items.len(), 2);
    assert_eq!(
        core.get_backlog_item(&binding_id, &created.item_id)
            .expect("created item after rejected inputs"),
        created
    );
    assert_eq!(history_count(&core, &binding_id, &created.item_id), 1);
}

#[test]
fn legacy_binding_run_and_event_functionality_remains_available() {
    let (_dir, db_path, mut core) = open_temp_core();
    let binding_id = create_binding(&mut core, THREAD_A, "legacy run compatibility");

    let run = core
        .execute(
            &binding_id,
            Command::StartDemo {
                command_key: "legacy-demo".to_string(),
                instruction: "Exercise the existing demo runner".to_string(),
                step_delay_ms: Some(100),
                now_ms: 1_000,
            },
        )
        .expect("start demo")
        .into_run()
        .expect("demo run");
    assert_eq!(run.binding_id, binding_id);
    let waiting = core.tick(1_100).expect("tick demo to question");
    assert_eq!(waiting.advanced_runs, vec![run.run_id.clone()]);

    let backlog_item = core
        .create_backlog_item(
            &binding_id,
            "backlog-alongside-run",
            coworker_item("Backlog alongside run"),
            1_200,
        )
        .expect("create backlog alongside run");
    assert_eq!(backlog_item.status, BacklogStatus::Todo);

    let loaded_run = core
        .get_run(&binding_id, &run.run_id)
        .expect("read demo run");
    assert!(loaded_run.revision >= 1);
    assert!(!core
        .list_runs(&binding_id, None, MAX_PAGE_LIMIT)
        .expect("list runs")
        .runs
        .is_empty());
    assert!(!core
        .list_run_revisions(&binding_id, &run.run_id, None, MAX_PAGE_LIMIT)
        .expect("list run revisions")
        .revisions
        .is_empty());
    assert!(!core
        .list_activity(&binding_id, None, None, MAX_PAGE_LIMIT, None, None)
        .expect("list activity")
        .events
        .is_empty());
    assert_eq!(
        core.get_wakeup(&binding_id).expect("legacy wakeup").status,
        WakeupStatus::Pending
    );

    drop(core);
    let reopened = Core::open(&db_path).expect("reopen after mixed legacy and backlog state");
    assert_eq!(
        reopened
            .get_run(&binding_id, &run.run_id)
            .expect("run after reopen")
            .status,
        loaded_run.status
    );
    assert_eq!(
        reopened
            .get_backlog_item(&binding_id, &backlog_item.item_id)
            .expect("backlog after reopen"),
        backlog_item
    );
    assert!(!reopened
        .list_run_revisions(&binding_id, &run.run_id, None, MAX_PAGE_LIMIT)
        .expect("revisions after reopen")
        .revisions
        .is_empty());
    assert!(!reopened
        .list_activity(&binding_id, None, None, MAX_PAGE_LIMIT, None, None)
        .expect("activity after reopen")
        .events
        .is_empty());
}
