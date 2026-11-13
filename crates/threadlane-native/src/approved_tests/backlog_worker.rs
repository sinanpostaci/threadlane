//! Focused guarded Worker backlog facade tests.
//!
//! These tests reuse the disposable approved-host fixture and exercise only local fake
//! providers. They cover exact reservation/start/result/completion, duplicate start before and
//! after a recorded outcome and after durable completion with the old checkout removed,
//! stale/cross-binding/unapproved/wrong-thread rejection, stopped-host result reads, changed
//! same-length delivery refusal, durable completion replay after checkout removal, recoverable
//! pre-effect reservation, and unresolved durable dispatch intent refusal without redispatch.
//! Provider invocation counting uses the existing fake-provider ledger. These local fake-provider
//! tests do not prove native callback delivery or production provider behavior.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use threadlane_core::{BacklogItem, BacklogStatus, WorkerBacklogExecution, WorkerBacklogOutcome};
use threadlane_host::{
    record_task_dispatch_intent, ApprovedHost, ApprovedHostConfig, ApprovedHostHandle,
    TaskDispatchIntent, TaskExecutionStatus, TaskStateSnapshot,
};

use super::*;

async fn create_worker_item(
    mcp: &PairedMcpServer,
    command_key: &str,
    title: &str,
) -> (String, u64) {
    let response = mcp
        .invoke_tool_json(
            "backlog_create",
            json!({
                "command_key": command_key,
                "title": title,
                "acceptance_condition": "The exact delivered patch is accepted.",
                "delivery_description": "A bounded owned-checkout delivery.",
                "kind": "worker"
            }),
            None,
        )
        .await
        .expect("create Worker backlog item");
    let item: BacklogItem =
        serde_json::from_value(response.structured_content.unwrap()["data"].clone())
            .expect("decode created item");
    (item.item_id, item.revision)
}

async fn get_item(mcp: &PairedMcpServer, item_id: &str) -> BacklogItem {
    let response = mcp
        .invoke_tool_json("backlog_get", json!({ "item_id": item_id }), None)
        .await
        .expect("read backlog item");
    serde_json::from_value(response.structured_content.unwrap()["data"].clone())
        .expect("decode backlog item")
}

async fn start_fixture_host(fixture: &ApprovedMcpFixture, approval_id: &str) -> ApprovedHostHandle {
    ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        approval_id,
    ))
    .await
    .expect("start fixture host")
}

fn live_mcp(fixture: &ApprovedMcpFixture) -> PairedMcpServer {
    PairedMcpServer::new(&fixture.state_dir, fixture.pairing_store.clone())
        .expect("create MCP server")
        .with_host_stores(Arc::clone(&fixture.authority), &fixture.sessions_dir)
}

async fn wait_for_terminal_task(
    mcp: &PairedMcpServer,
    approval_id: &str,
    task_id: &str,
) -> TaskStateSnapshot {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let response = mcp
            .invoke_tool_json(
                "approved_task",
                json!({ "approval_id": approval_id, "task_id": task_id }),
                None,
            )
            .await
            .expect("read approved task");
        let snapshot: TaskStateSnapshot =
            serde_json::from_value(response.structured_content.unwrap()["data"].clone())
                .expect("decode task snapshot");
        if matches!(
            snapshot.status,
            TaskExecutionStatus::ReadyForReview
                | TaskExecutionStatus::Failed
                | TaskExecutionStatus::Cancelled
                | TaskExecutionStatus::TimedOut
        ) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "test Worker did not reach a terminal status in time"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn write_failing_agy(fixture: &ApprovedMcpFixture) {
    let script = format!(
        r#"#!/bin/sh
set -eu
COUNT_FILE="{}"
c=0
if [ -f "$COUNT_FILE" ]; then
    c=$(cat "$COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$COUNT_FILE"
exit 1
"#,
        fixture.agy_count_file.display()
    );
    fs::write(&fixture.fake_agy_exe, script).expect("write failing fake provider");
    fs::set_permissions(&fixture.fake_agy_exe, fs::Permissions::from_mode(0o755))
        .expect("chmod failing fake provider");
}

fn assert_worker_backlog_schema(tool: &rmcp::model::Tool, expected: &[&str]) {
    let schema = serde_json::to_value(&tool.input_schema).expect("schema json");
    let properties = schema["properties"]
        .as_object()
        .expect("schema properties object");
    assert_eq!(properties.len(), expected.len());
    for property in expected {
        assert!(
            properties.contains_key(*property),
            "missing schema property `{property}`"
        );
    }
    assert_eq!(schema["additionalProperties"], Value::Bool(false));
    for forbidden in [
        "binding_id",
        "thread_id",
        "thread_uuid",
        "session_binding_id",
        "model",
        "effort",
        "executable",
        "executable_path",
        "path",
        "scope",
        "allowed_write_paths",
        "profile_id",
        "success",
        "worker_snapshot",
        "authority",
        "provider",
    ] {
        assert!(
            !properties.contains_key(forbidden),
            "tool must not accept `{forbidden}`"
        );
    }
}

#[tokio::test]
async fn guarded_worker_backlog_tools_are_registered_with_strict_bounded_inputs() {
    let fixture = setup_approved_mcp_fixture().await;
    let mcp = live_mcp(&fixture);
    let tools = mcp.list_tools();

    for (name, expected, read_only) in [
        (
            "backlog_worker_start",
            &[
                "item_id",
                "expected_revision",
                "command_key",
                "approval_id",
                "task_id",
            ][..],
            false,
        ),
        (
            "backlog_worker_result",
            &["item_id", "execution_ref"][..],
            false,
        ),
        (
            "backlog_worker_complete",
            &[
                "item_id",
                "execution_ref",
                "command_key",
                "expected_revision",
                "expected_result_digest",
                "expected_delivery_digest",
                "acceptance_note",
                "acceptance_evidence",
            ][..],
            false,
        ),
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing tool `{name}`"));
        let annotations = tool.annotations.as_ref().expect("annotations");
        assert_eq!(annotations.read_only_hint, Some(read_only));
        assert_eq!(annotations.destructive_hint, Some(false));
        assert_eq!(annotations.idempotent_hint, Some(true));
        assert_eq!(annotations.open_world_hint, Some(false));
        assert_worker_backlog_schema(tool, expected);
    }

    // Result reconciliation is explicitly mutating even though it is exact-idempotent.
    let result_tool = tools
        .iter()
        .find(|tool| tool.name == "backlog_worker_result")
        .expect("result tool");
    assert_eq!(
        result_tool
            .annotations
            .as_ref()
            .expect("result annotations")
            .read_only_hint,
        Some(false)
    );
}

#[tokio::test]
async fn guarded_worker_backlog_start_result_complete_duplicate_stopped_and_replay() {
    let fixture = setup_approved_mcp_fixture().await;
    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-worker-backlog-happy",
        &fixture.binding_a,
        default_plan_tasks("Guarded Worker backlog happy path"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-worker-backlog-happy",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string(), "task-root-oc".to_string()],
        1_700_002_000_000,
    );
    let host = start_fixture_host(&fixture, &approval.approval_id).await;
    let mcp = live_mcp(&fixture);
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;
    let other_mcp = live_mcp(&fixture);
    enroll_server(&fixture, &other_mcp, &fixture.binding_b, THREAD_UUID_B).await;

    let (item_id, revision) = create_worker_item(&mcp, "create-happy", "Happy Worker item").await;

    // Stale caller revision reserves nothing and invokes no provider.
    let stale = mcp
        .invoke_tool_json(
            "backlog_worker_start",
            json!({
                "item_id": item_id,
                "expected_revision": revision + 1,
                "command_key": "start-stale",
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect_err("stale revision must fail");
    assert_eq!(
        stale.structured_content.unwrap()["error"]["code"],
        "stale_revision"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // An unapproved task is rejected before any reservation or provider effect.
    let unapproved = mcp
        .invoke_tool_json(
            "backlog_worker_start",
            json!({
                "item_id": item_id,
                "expected_revision": revision,
                "command_key": "start-unapproved",
                "approval_id": approval.approval_id,
                "task_id": "task-native-codex"
            }),
            None,
        )
        .await
        .expect_err("unapproved task must fail before effect");
    assert_eq!(
        unapproved.structured_content.unwrap()["error"]["code"],
        "unknown_task"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Todo);

    // Cross-binding scope is rejected by the existing advancement guard.
    let cross_binding = other_mcp
        .invoke_tool_json(
            "backlog_worker_start",
            json!({
                "item_id": item_id,
                "expected_revision": revision,
                "command_key": "start-cross-binding",
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect_err("cross-binding start must fail");
    assert_eq!(
        cross_binding.structured_content.unwrap()["error"]["code"],
        "scope_mismatch"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // A frozen approval for another native thread is rejected under this enrollment.
    let alt_thread_approval = approve_published_plan(
        &fixture,
        "plan-worker-backlog-happy",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A_ALT,
        vec!["task-root-agy".to_string()],
        1_700_002_000_100,
    );
    let wrong_thread = mcp
        .invoke_tool_json(
            "backlog_worker_start",
            json!({
                "item_id": item_id,
                "expected_revision": revision,
                "command_key": "start-wrong-thread",
                "approval_id": alt_thread_approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect_err("wrong-thread approval must fail");
    assert_eq!(
        wrong_thread.structured_content.unwrap()["error"]["code"],
        "scope_mismatch"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Todo);

    let start_params = json!({
        "item_id": item_id,
        "expected_revision": revision,
        "command_key": "start-happy",
        "approval_id": approval.approval_id,
        "task_id": "task-root-agy"
    });
    let start = mcp
        .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
        .await
        .expect("guarded Worker backlog start");
    let start_data = start.structured_content.expect("start data")["data"].clone();
    let first_execution: WorkerBacklogExecution =
        serde_json::from_value(start_data["execution"].clone()).expect("decode execution");
    assert!(first_execution.dispatch_receipt.is_some());
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Doing);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // Exact duplicate reuses the same reservation and invocation. This is also the lost
    // start-response path: the durable current execution is reconciled, not the original
    // reservation serialization returned by core replay.
    let duplicate = mcp
        .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
        .await
        .expect("duplicate start is idempotent");
    let duplicate_data = duplicate.structured_content.expect("duplicate data")["data"].clone();
    let duplicate_execution: WorkerBacklogExecution =
        serde_json::from_value(duplicate_data["execution"].clone())
            .expect("decode duplicate execution");
    assert_eq!(
        duplicate_execution.execution_ref,
        first_execution.execution_ref
    );
    assert_eq!(
        duplicate_execution.dispatch_receipt,
        first_execution.dispatch_receipt
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let terminal = wait_for_terminal_task(&mcp, &approval.approval_id, "task-root-agy").await;
    assert_eq!(terminal.status, TaskExecutionStatus::ReadyForReview);

    // The result is usable after the host has cleanly stopped.
    host.shutdown().await.expect("stop fixture host");
    let result = mcp
        .invoke_tool_json(
            "backlog_worker_result",
            json!({
                "item_id": item_id,
                "execution_ref": first_execution.execution_ref
            }),
            None,
        )
        .await
        .expect("read stopped host Worker result");
    let result_data = result.structured_content.expect("result data")["data"].clone();
    let executed: WorkerBacklogExecution =
        serde_json::from_value(result_data["execution"].clone()).expect("decode result execution");
    let outcome = executed.outcome.as_ref().expect("successful outcome");
    assert_eq!(outcome.outcome, WorkerBacklogOutcome::Success);
    let result_digest = outcome
        .result_digest
        .clone()
        .expect("successful result digest");
    let delivery = result_data["delivery"]
        .as_object()
        .expect("path-free delivery inspection");
    let delivery_digest = delivery["delivery_digest"]
        .as_str()
        .expect("delivery digest")
        .to_string();
    let worktree_id = delivery["worktree_id"]
        .as_str()
        .expect("worktree identity")
        .to_string();
    let item_after_result = get_item(&mcp, &item_id).await;
    assert_eq!(
        item_after_result.status,
        BacklogStatus::Doing,
        "a successful result alone must remain Doing pending Coworker acceptance"
    );

    // Re-reading the known outcome reuses/compares durable evidence without another history entry.
    let replayed_result = mcp
        .invoke_tool_json(
            "backlog_worker_result",
            json!({
                "item_id": item_id,
                "execution_ref": first_execution.execution_ref
            }),
            None,
        )
        .await
        .expect("re-read stored successful outcome");
    let replayed_execution: WorkerBacklogExecution = serde_json::from_value(
        replayed_result.structured_content.unwrap()["data"]["execution"].clone(),
    )
    .expect("decode replayed result execution");
    assert_eq!(
        replayed_execution
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.result_digest.as_deref()),
        Some(result_digest.as_str())
    );
    assert_eq!(
        get_item(&mcp, &item_id).await.revision,
        item_after_result.revision,
        "known outcome re-read must not append another backlog revision"
    );

    // A duplicate start after the outcome was recorded reconciles the same durable invocation
    // without dispatching again or reusing the original reservation serialization.
    let duplicate_after_outcome = mcp
        .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
        .await
        .expect("duplicate start after outcome");
    let after_outcome_execution: WorkerBacklogExecution = serde_json::from_value(
        duplicate_after_outcome.structured_content.unwrap()["data"]["execution"].clone(),
    )
    .expect("decode duplicate-after-outcome execution");
    assert_eq!(
        after_outcome_execution.execution_ref,
        first_execution.execution_ref
    );
    assert_eq!(
        after_outcome_execution.dispatch_receipt,
        first_execution.dispatch_receipt
    );
    let after_outcome = after_outcome_execution
        .outcome
        .as_ref()
        .expect("durable outcome after duplicate start");
    assert_eq!(after_outcome.outcome, WorkerBacklogOutcome::Success);
    assert_eq!(
        after_outcome.result_digest.as_deref(),
        Some(result_digest.as_str())
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(
        get_item(&mcp, &item_id).await.revision,
        item_after_result.revision,
        "duplicate start after an outcome must not append another backlog revision"
    );

    let item_before_completion = item_after_result;
    let work_area_root = fixture
        .sessions_dir
        .join(&approval.approval_id)
        .join(WORK_AREA_SUBDIR);
    let checkout = threadlane_workers::load_and_verify_manifest(&work_area_root, &worktree_id)
        .expect("load verified task worktree manifest")
        .checkout_path;
    let delivered_file = checkout.join("src/lib.rs");
    let original_bytes = fs::read(&delivered_file).expect("read delivered file");
    let mut changed_bytes = original_bytes.clone();
    changed_bytes[0] = if changed_bytes[0] == b'/' { b'#' } else { b'/' };
    assert_eq!(changed_bytes.len(), original_bytes.len());
    fs::write(&delivered_file, &changed_bytes).expect("write same-length changed delivery");

    let complete_params = json!({
        "item_id": item_id,
        "execution_ref": first_execution.execution_ref,
        "command_key": "complete-happy",
        "expected_revision": item_before_completion.revision,
        "expected_result_digest": result_digest,
        "expected_delivery_digest": delivery_digest,
        "acceptance_note": "Accepted the exact verified Worker delivery.",
        "acceptance_evidence": "Compared the result and path-free delivery digests."
    });
    let changed_bytes_err = mcp
        .invoke_tool_json("backlog_worker_complete", complete_params.clone(), None)
        .await
        .expect_err("changed same-length delivery must refuse Done");
    assert_eq!(
        changed_bytes_err.structured_content.unwrap()["error"]["code"],
        "delivery_digest_mismatch"
    );
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Doing);

    fs::write(&delivered_file, &original_bytes).expect("restore exact delivered bytes");
    let completed = mcp
        .invoke_tool_json("backlog_worker_complete", complete_params.clone(), None)
        .await
        .expect("complete exact verified delivery");
    let completed_data = completed.structured_content.expect("completion data")["data"].clone();
    assert_eq!(
        completed_data["completion"]["item"]["status"],
        Value::String("done".to_string())
    );
    let completed_at_ms =
        completed_data["completion"]["execution"]["completion"]["completed_at_ms"].clone();
    assert!(!completed_at_ms.is_null());
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Done);

    // Exact durable replay does not require the old mutable checkout.
    fs::remove_dir_all(&checkout).expect("remove completed task checkout");
    let replayed = mcp
        .invoke_tool_json("backlog_worker_complete", complete_params.clone(), None)
        .await
        .expect("exact durable completion replay");
    let replayed_data = replayed.structured_content.expect("replay data")["data"].clone();
    assert_eq!(
        replayed_data["completion"]["execution"]["completion"]["completed_at_ms"],
        completed_at_ms
    );

    // A duplicate start after Done reads the durable completion and does not require the removed
    // checkout or another provider invocation.
    let duplicate_after_done = mcp
        .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
        .await
        .expect("duplicate start after Done");
    let after_done_execution: WorkerBacklogExecution = serde_json::from_value(
        duplicate_after_done.structured_content.unwrap()["data"]["execution"].clone(),
    )
    .expect("decode duplicate-after-done execution");
    assert_eq!(
        after_done_execution.execution_ref,
        first_execution.execution_ref
    );
    assert_eq!(
        after_done_execution.dispatch_receipt,
        first_execution.dispatch_receipt
    );
    assert_eq!(
        after_done_execution
            .completion
            .as_ref()
            .expect("durable completion after duplicate start")
            .completed_at_ms,
        completed_at_ms.as_i64().expect("completion timestamp")
    );
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Done);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // A changed completion payload cannot reopen or overwrite Done.
    let mut changed_payload = complete_params;
    changed_payload["acceptance_note"] = json!("A different acceptance note.");
    let conflict = mcp
        .invoke_tool_json("backlog_worker_complete", changed_payload, None)
        .await
        .expect_err("changed Done payload must fail");
    assert_eq!(
        conflict.structured_content.unwrap()["error"]["code"],
        "completion_conflict"
    );
    assert_eq!(get_item(&mcp, &item_id).await.status, BacklogStatus::Done);
}

#[tokio::test]
async fn guarded_worker_backlog_failure_is_recorded_and_never_completed() {
    let fixture = setup_approved_mcp_fixture().await;
    write_failing_agy(&fixture);

    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-worker-backlog-failure",
        &fixture.binding_a,
        default_plan_tasks("Guarded Worker backlog failure"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-worker-backlog-failure",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_002_100_000,
    );
    let host = start_fixture_host(&fixture, &approval.approval_id).await;
    let mcp = live_mcp(&fixture);
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;
    let (item_id, revision) =
        create_worker_item(&mcp, "create-failure", "Failing Worker item").await;

    let start = mcp
        .invoke_tool_json(
            "backlog_worker_start",
            json!({
                "item_id": item_id,
                "expected_revision": revision,
                "command_key": "start-failure",
                "approval_id": approval.approval_id,
                "task_id": "task-root-agy"
            }),
            None,
        )
        .await
        .expect("start failing Worker");
    let execution: WorkerBacklogExecution =
        serde_json::from_value(start.structured_content.unwrap()["data"]["execution"].clone())
            .expect("decode failing execution");

    let terminal = wait_for_terminal_task(&mcp, &approval.approval_id, "task-root-agy").await;
    assert_eq!(terminal.status, TaskExecutionStatus::Failed);
    let result = mcp
        .invoke_tool_json(
            "backlog_worker_result",
            json!({
                "item_id": item_id,
                "execution_ref": execution.execution_ref
            }),
            None,
        )
        .await
        .expect("record failed Worker outcome");
    let result_data = result.structured_content.expect("result data")["data"].clone();
    let failed: WorkerBacklogExecution =
        serde_json::from_value(result_data["execution"].clone()).expect("failed execution");
    let outcome = failed.outcome.as_ref().expect("failure outcome");
    assert_eq!(outcome.outcome, WorkerBacklogOutcome::Failure);
    let digest = outcome.result_digest.clone().expect("failure digest");
    assert_eq!(digest.len(), 64);
    assert_ne!(
        outcome.note, "verified terminal worker failure",
        "failure must carry a concrete software-owned category"
    );
    assert!(
        matches!(
            outcome.note.as_str(),
            "verified worker timeout"
                | "verified worker cancellation"
                | "verified worker diff scope rejection"
                | "verified worker missing model evidence"
                | "verified worker missing effort evidence"
                | "verified worker cleanup incomplete"
                | "verified worker nonzero exit"
                | "verified worker signal termination"
                | "verified worker provider error"
                | "verified worker terminal failure"
        ),
        "failure note must be a software-owned bounded category: {}",
        outcome.note
    );
    let blocked_item = get_item(&mcp, &item_id).await;
    assert_eq!(blocked_item.status, BacklogStatus::Blocked);
    assert!(
        blocked_item
            .blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(outcome.note.as_str())),
        "blocked reason must expose the concrete failure category: {:?}",
        blocked_item.blocked_reason
    );

    // Re-reading the same failed result does not rewrite its stable outcome or status.
    let repeated = mcp
        .invoke_tool_json(
            "backlog_worker_result",
            json!({
                "item_id": item_id,
                "execution_ref": execution.execution_ref
            }),
            None,
        )
        .await
        .expect("re-read failed Worker outcome");
    let repeated_execution: WorkerBacklogExecution =
        serde_json::from_value(repeated.structured_content.unwrap()["data"]["execution"].clone())
            .expect("repeated failed execution");
    assert_eq!(
        repeated_execution.outcome.as_ref().map(|stored| (
            stored.outcome,
            stored.note.as_str(),
            stored.result_digest.as_deref()
        )),
        Some((
            WorkerBacklogOutcome::Failure,
            outcome.note.as_str(),
            Some(digest.as_str())
        ))
    );
    assert_eq!(
        get_item(&mcp, &item_id).await.revision,
        blocked_item.revision
    );

    let completion_err = mcp
        .invoke_tool_json(
            "backlog_worker_complete",
            json!({
                "item_id": item_id,
                "execution_ref": execution.execution_ref,
                "command_key": "complete-failure",
                "expected_revision": get_item(&mcp, &item_id).await.revision,
                "expected_result_digest": digest,
                "expected_delivery_digest": "0".repeat(64),
                "acceptance_note": "Must not complete.",
                "acceptance_evidence": "Failure is not acceptance evidence."
            }),
            None,
        )
        .await
        .expect_err("failed Worker cannot complete");
    assert_eq!(
        completion_err.structured_content.unwrap()["error"]["code"],
        "outcome_required"
    );
    assert_eq!(
        get_item(&mcp, &item_id).await.status,
        BacklogStatus::Blocked
    );

    host.shutdown().await.expect("stop fixture host");
}

#[tokio::test]
async fn guarded_worker_backlog_unresolved_dispatch_intent_is_not_redispatched() {
    let fixture = setup_approved_mcp_fixture().await;
    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-worker-backlog-unknown-intent",
        &fixture.binding_a,
        default_plan_tasks("Guarded Worker backlog unknown intent"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-worker-backlog-unknown-intent",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_002_150_000,
    );
    let host = start_fixture_host(&fixture, &approval.approval_id).await;
    let mcp = live_mcp(&fixture);
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;
    let (item_id, revision) = create_worker_item(
        &mcp,
        "create-unknown-intent",
        "Unresolved dispatch intent Worker item",
    )
    .await;

    let dispatch = host
        .materialized_plan()
        .task_dispatch
        .iter()
        .find(|dispatch| dispatch.task_id == "task-root-agy")
        .expect("frozen dispatch for task-root-agy")
        .clone();
    record_task_dispatch_intent(
        host.session_dir(),
        host.approval(),
        host.session_binding_id(),
        TaskDispatchIntent {
            task_id: dispatch.task_id,
            task_digest: dispatch.task_digest,
            profile_id: dispatch.profile_id,
            command_key: dispatch.command_key,
            execution_binding: None,
            recorded_at_ms: 1_700_002_150_100,
        },
    )
    .expect("persist unresolved dispatch intent");

    let start_params = json!({
        "item_id": item_id,
        "expected_revision": revision,
        "command_key": "start-unknown-intent",
        "approval_id": approval.approval_id,
        "task_id": "task-root-agy"
    });
    for _ in 0..2 {
        let err = mcp
            .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
            .await
            .expect_err("unresolved dispatch intent must refuse a new dispatch");
        assert_eq!(
            err.structured_content.unwrap()["error"]["code"],
            "worker_effect_unknown"
        );
        assert_eq!(
            read_invocation_count(&fixture.agy_count_file),
            0,
            "an unresolved dispatch intent must never trigger a provider invocation"
        );
    }
    let reserved = get_item(&mcp, &item_id).await;
    assert_eq!(reserved.status, BacklogStatus::Doing);
    assert!(reserved.current_execution_ref.is_some());

    host.shutdown().await.expect("stop fixture host");
}

#[tokio::test]
async fn guarded_worker_backlog_pre_effect_refusal_preserves_exact_reservation() {
    let fixture = setup_approved_mcp_fixture().await;
    let (plan_revision, plan_digest) = create_published_plan(
        &fixture,
        "plan-worker-backlog-prelaunch",
        &fixture.binding_a,
        default_plan_tasks("Guarded Worker backlog prelaunch"),
    );
    let approval = approve_published_plan(
        &fixture,
        "plan-worker-backlog-prelaunch",
        plan_revision,
        &plan_digest,
        &fixture.binding_a,
        THREAD_UUID_A,
        vec!["task-root-agy".to_string()],
        1_700_002_200_000,
    );
    let mcp = live_mcp(&fixture);
    enroll_server(&fixture, &mcp, &fixture.binding_a, THREAD_UUID_A).await;
    let (item_id, revision) =
        create_worker_item(&mcp, "create-prelaunch", "Prelaunch Worker item").await;
    let start_params = json!({
        "item_id": item_id,
        "expected_revision": revision,
        "command_key": "start-prelaunch",
        "approval_id": approval.approval_id,
        "task_id": "task-root-agy"
    });

    let missing_host = mcp
        .invoke_tool_json("backlog_worker_start", start_params.clone(), None)
        .await
        .expect_err("missing host is a recoverable pre-effect refusal");
    assert_eq!(
        missing_host.structured_content.unwrap()["error"]["code"],
        "host_not_ready"
    );
    let reserved = get_item(&mcp, &item_id).await;
    assert_eq!(reserved.status, BacklogStatus::Doing);
    let execution_ref = reserved
        .current_execution_ref
        .clone()
        .expect("original reservation remains recoverable");
    let result = mcp
        .invoke_tool_json(
            "backlog_worker_result",
            json!({ "item_id": item_id, "execution_ref": execution_ref }),
            None,
        )
        .await
        .expect("prelaunch result read is state-only");
    let prelaunch: WorkerBacklogExecution =
        serde_json::from_value(result.structured_content.unwrap()["data"]["execution"].clone())
            .expect("prelaunch execution");
    assert!(prelaunch.outcome.is_none());
    assert!(prelaunch.dispatch_receipt.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let host = start_fixture_host(&fixture, &approval.approval_id).await;
    let retried = mcp
        .invoke_tool_json("backlog_worker_start", start_params, None)
        .await
        .expect("exact retry resumes the original reservation");
    let resumed: WorkerBacklogExecution =
        serde_json::from_value(retried.structured_content.unwrap()["data"]["execution"].clone())
            .expect("resumed execution");
    assert_eq!(resumed.execution_ref, execution_ref);
    assert!(resumed.dispatch_receipt.is_some());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    host.shutdown().await.expect("stop fixture host");
}
