use std::path::Path;
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};

use threadlane::descriptor::read_and_validate_descriptor;
use threadlane::operator_backlog::{
    OperatorWorkerBacklogRequest, OperatorWorkerBacklogResponse, OPERATOR_WORKER_BACKLOG_PATH,
};
use threadlane::{
    ErrorCode, ScopedClient, ScopedRequest, Supervisor, SupervisorClient, SupervisorConfig,
    ThreadlaneMcpServer, MAX_CLIENT_RESPONSE_BYTES,
};
use threadlane_core::{
    estimate_wire_response_bytes, BacklogItemKind, BacklogStatus, WorkerBacklogAcceptanceRejection,
    WorkerBacklogAcceptanceRejectionReceipt, WorkerBacklogAuthority, WorkerBacklogCompletion,
    WorkerBacklogDispatchReceipt, WorkerBacklogExecution, WorkerBacklogOutcome,
    WorkerBacklogOutcomeReport, MAX_COMPACT_RESPONSE_BYTES,
};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const FIXTURE_TIMEOUT: Duration = Duration::from_secs(10);

const HEX64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEX64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HEX64_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const HEX64_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const APPROVAL_ID_A: &str = "apr_11111111111111111111111111111111";
const APPROVAL_ID_B: &str = "apr_22222222222222222222222222222222";
const TASK_ID_A: &str = "task_operator_backlog_one";
const TASK_ID_B: &str = "task_operator_backlog_two";
const RUN_ID_A: &str = "run_11111111-2222-4333-8444-555555555555";
const RUN_ID_B: &str = "run_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const WORKER_ID_A: &str = "wrk_11111111-2222-4333-8444-555555555555";
const WORKER_ID_B: &str = "wrk_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const PROFILE_ID_A: &str = "profile_operator_backlog_one";
const PROFILE_ID_B: &str = "profile_operator_backlog_two";

async fn bounded<F, T>(label: &str, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(FIXTURE_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
}

fn value_of<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("serialize operator backlog value")
}

fn fast_supervisor_config(state_dir: &Path) -> SupervisorConfig {
    SupervisorConfig {
        state_dir: state_dir.to_path_buf(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: None,
    }
}

async fn start_fixture(
    state_dir: &Path,
) -> (
    threadlane::SupervisorHandle,
    SupervisorClient,
    ScopedClient,
    ScopedClient,
) {
    let handle = bounded(
        "start operator backlog supervisor",
        Supervisor::start(fast_supervisor_config(state_dir)),
    )
    .await
    .expect("start operator backlog supervisor");
    let supervisor = SupervisorClient::from_state_dir(state_dir).expect("supervisor client");
    let binding_a = bounded(
        "create operator backlog binding A",
        supervisor.create_binding(THREAD_UUID_A, "Operator Backlog A"),
    )
    .await
    .expect("create operator backlog binding A");
    let binding_b = bounded(
        "create operator backlog binding B",
        supervisor.create_binding(THREAD_UUID_B, "Operator Backlog B"),
    )
    .await
    .expect("create operator backlog binding B");
    let scoped_a = supervisor
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped operator binding A");
    let scoped_b = supervisor
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped operator binding B");
    (handle, supervisor, scoped_a, scoped_b)
}

async fn create_worker_item(scoped: &ScopedClient, key: &str, title: &str) -> (String, u64) {
    create_worker_item_with_details(
        scoped,
        key,
        title,
        "Operator acceptance",
        "Operator delivery",
    )
    .await
}

async fn create_worker_item_with_details(
    scoped: &ScopedClient,
    key: &str,
    title: &str,
    acceptance_condition: &str,
    delivery_description: &str,
) -> (String, u64) {
    let item = bounded(
        "create operator worker item",
        scoped.create_backlog_item(
            key.to_string(),
            title.to_string(),
            acceptance_condition.to_string(),
            delivery_description.to_string(),
            BacklogItemKind::Worker,
        ),
    )
    .await
    .expect("create operator worker item");
    (item.item_id, item.revision)
}

async fn prepare_worker_item_for_completion(
    supervisor: &SupervisorClient,
    scoped: &ScopedClient,
    binding_id: &str,
    command_prefix: &str,
    title: &str,
    authority: &WorkerBacklogAuthority,
    receipt: &WorkerBacklogDispatchReceipt,
) -> (String, String, u64) {
    let acceptance_condition = "a".repeat(2048);
    let delivery_description = "b".repeat(2048);
    let (item_id, revision) = create_worker_item_with_details(
        scoped,
        &format!("{command_prefix}-create"),
        title,
        &acceptance_condition,
        &delivery_description,
    )
    .await;

    let reserved = call_operator(
        supervisor,
        reserve_body(
            binding_id,
            THREAD_UUID_A,
            &format!("{command_prefix}-reserve"),
            &item_id,
            revision,
            authority,
        ),
    )
    .await
    .expect("reserve budget fixture item");
    let execution_ref = execution_of(&reserved).execution_ref.clone();

    let attach_revision = item_snapshot(scoped, &item_id).await.revision;
    let attached = call_operator(
        supervisor,
        attach_body(
            binding_id,
            THREAD_UUID_A,
            &format!("{command_prefix}-attach"),
            &item_id,
            &execution_ref,
            attach_revision,
            receipt,
        ),
    )
    .await
    .expect("attach budget fixture receipt");
    assert_eq!(execution_of(&attached).execution_ref, execution_ref);

    let record_revision = item_snapshot(scoped, &item_id).await.revision;
    let recorded = call_operator(
        supervisor,
        record_body(
            binding_id,
            THREAD_UUID_A,
            &format!("{command_prefix}-record"),
            &item_id,
            &execution_ref,
            record_revision,
            &WorkerBacklogOutcomeReport {
                outcome: WorkerBacklogOutcome::Success,
                note: "verified".to_string(),
                result_digest: Some(HEX64_C.to_string()),
            },
        ),
    )
    .await
    .expect("record budget fixture success");
    assert_eq!(execution_of(&recorded).execution_ref, execution_ref);

    let before_complete = item_snapshot(scoped, &item_id).await;
    assert!(before_complete.revision > record_revision);
    assert_eq!(before_complete.status, BacklogStatus::Doing);
    (item_id, execution_ref, before_complete.revision)
}

async fn backlog_history_count(scoped: &ScopedClient, item_id: &str) -> usize {
    bounded(
        "operator backlog history count",
        scoped.list_backlog_history(item_id.to_string(), None, Some(50)),
    )
    .await
    .expect("operator backlog history count")
    .entries
    .len()
}

async fn call_operator(
    supervisor: &SupervisorClient,
    body: Value,
) -> Result<OperatorWorkerBacklogResponse, threadlane::Error> {
    let request: OperatorWorkerBacklogRequest =
        serde_json::from_value(body).expect("operator request must be strict and bounded");
    bounded(
        "operator worker backlog request",
        supervisor.worker_backlog_operator_attested(&request),
    )
    .await
}

fn execution_of(response: &OperatorWorkerBacklogResponse) -> &WorkerBacklogExecution {
    match response {
        OperatorWorkerBacklogResponse::Execution { execution } => execution,
        other => panic!("expected operator execution response, got {other:?}"),
    }
}

fn completion_of(response: &OperatorWorkerBacklogResponse) {
    match response {
        OperatorWorkerBacklogResponse::Completion { .. } => {}
        other => panic!("expected operator completion response, got {other:?}"),
    }
}

fn rejection_of(
    response: &OperatorWorkerBacklogResponse,
) -> &WorkerBacklogAcceptanceRejectionReceipt {
    match response {
        OperatorWorkerBacklogResponse::AcceptanceRejection { rejection } => rejection,
        other => panic!("expected operator acceptance rejection response, got {other:?}"),
    }
}

fn attempt_count_of(response: &OperatorWorkerBacklogResponse) -> usize {
    match response {
        OperatorWorkerBacklogResponse::AttemptPage { page } => page.attempts.len(),
        other => panic!("expected operator attempt page response, got {other:?}"),
    }
}

fn reserve_body(
    binding_id: &str,
    thread_uuid: &str,
    command_key: &str,
    item_id: &str,
    expected_revision: u64,
    authority: &WorkerBacklogAuthority,
) -> Value {
    json!({
        "op": "reserve",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "command_key": command_key,
        "item_id": item_id,
        "expected_revision": expected_revision,
        "authority": authority,
    })
}

fn attach_body(
    binding_id: &str,
    thread_uuid: &str,
    command_key: &str,
    item_id: &str,
    execution_ref: &str,
    expected_revision: u64,
    receipt: &WorkerBacklogDispatchReceipt,
) -> Value {
    json!({
        "op": "attach_dispatch_receipt",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "command_key": command_key,
        "item_id": item_id,
        "execution_ref": execution_ref,
        "expected_revision": expected_revision,
        "receipt": receipt,
    })
}

fn record_body(
    binding_id: &str,
    thread_uuid: &str,
    command_key: &str,
    item_id: &str,
    execution_ref: &str,
    expected_revision: u64,
    report: &WorkerBacklogOutcomeReport,
) -> Value {
    json!({
        "op": "record_outcome",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "command_key": command_key,
        "item_id": item_id,
        "execution_ref": execution_ref,
        "expected_revision": expected_revision,
        "report": report,
    })
}

fn complete_body(
    binding_id: &str,
    thread_uuid: &str,
    command_key: &str,
    item_id: &str,
    expected_revision: u64,
    completion: &WorkerBacklogCompletion,
) -> Value {
    json!({
        "op": "complete",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "command_key": command_key,
        "item_id": item_id,
        "expected_revision": expected_revision,
        "completion": completion,
    })
}

fn reject_body(
    binding_id: &str,
    thread_uuid: &str,
    command_key: &str,
    item_id: &str,
    expected_revision: u64,
    rejection: &WorkerBacklogAcceptanceRejection,
) -> Value {
    json!({
        "op": "reject_acceptance",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "command_key": command_key,
        "item_id": item_id,
        "expected_revision": expected_revision,
        "rejection": rejection,
    })
}

fn get_execution_body(
    binding_id: &str,
    thread_uuid: &str,
    item_id: &str,
    execution_ref: &str,
) -> Value {
    json!({
        "op": "get_execution",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "item_id": item_id,
        "execution_ref": execution_ref,
    })
}

fn list_attempts_body(binding_id: &str, thread_uuid: &str, item_id: &str, limit: usize) -> Value {
    json!({
        "op": "list_attempts",
        "binding_id": binding_id,
        "thread_uuid": thread_uuid,
        "item_id": item_id,
        "limit": limit,
    })
}

fn authority_fixture(approval_id: &str, task_id: &str) -> WorkerBacklogAuthority {
    WorkerBacklogAuthority {
        approval_id: approval_id.to_string(),
        subject_digest: HEX64_A.to_string(),
        task_id: task_id.to_string(),
        task_digest: HEX64_B.to_string(),
    }
}

fn receipt_fixture(
    session_binding_id: &str,
    run_id: &str,
    worker_id: &str,
    profile_id: &str,
    execution_binding_digest: &str,
) -> WorkerBacklogDispatchReceipt {
    WorkerBacklogDispatchReceipt {
        session_binding_id: session_binding_id.to_string(),
        run_id: run_id.to_string(),
        worker_id: worker_id.to_string(),
        profile_id: profile_id.to_string(),
        execution_binding_digest: Some(execution_binding_digest.to_string()),
    }
}

fn success_report_fixture(digest: &str) -> WorkerBacklogOutcomeReport {
    WorkerBacklogOutcomeReport {
        outcome: WorkerBacklogOutcome::Success,
        note: "bounded operator evidence".to_string(),
        result_digest: Some(digest.to_string()),
    }
}

fn unknown_report_fixture() -> WorkerBacklogOutcomeReport {
    WorkerBacklogOutcomeReport {
        outcome: WorkerBacklogOutcome::Unknown,
        note: "bounded operator evidence".to_string(),
        result_digest: None,
    }
}

fn completion_fixture(
    execution_ref: &str,
    result_digest: &str,
    note: &str,
) -> WorkerBacklogCompletion {
    WorkerBacklogCompletion {
        execution_ref: execution_ref.to_string(),
        result_digest: result_digest.to_string(),
        acceptance_note: note.to_string(),
        acceptance_evidence: "bounded operator evidence".to_string(),
        delivery_digest: HEX64_C.to_string(),
    }
}

fn rejection_fixture(
    execution_ref: &str,
    result_digest: &str,
    delivery_digest: &str,
    reason: &str,
) -> WorkerBacklogAcceptanceRejection {
    WorkerBacklogAcceptanceRejection {
        execution_ref: execution_ref.to_string(),
        result_digest: result_digest.to_string(),
        delivery_digest: delivery_digest.to_string(),
        reason: reason.to_string(),
        evidence: "bounded operator evidence".to_string(),
    }
}

async fn item_snapshot(scoped: &ScopedClient, item_id: &str) -> threadlane_core::BacklogItem {
    bounded(
        "operator backlog item snapshot",
        scoped.get_backlog_item(item_id.to_string()),
    )
    .await
    .expect("operator backlog item snapshot")
}

#[tokio::test]
async fn operator_worker_backlog_is_attested_idempotent_isolated_and_non_scheduling() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("operator_backlog_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();

    let status_before = bounded("status before operator backlog", scoped_a.status())
        .await
        .expect("status before operator backlog");
    assert_eq!(status_before.total_runs, 0);
    assert_eq!(status_before.binding.delivered_seq, 0);
    assert_eq!(status_before.binding.acked_seq, 0);

    let tools = value_of(&ThreadlaneMcpServer::new(scoped_a.clone()).list_tools());
    let tools = tools.as_array().expect("MCP tools array");
    assert_eq!(tools.len(), 24, "operator transport adds no MCP tools");
    assert!(tools.iter().all(|tool| {
        let name = tool["name"].as_str().unwrap_or_default();
        !name.contains("operator") && !name.contains("worker_backlog")
    }));

    let (item_one, revision_one) =
        create_worker_item(&scoped_a, "create-item-one", "Item one").await;
    let (item_two, revision_two) =
        create_worker_item(&scoped_a, "create-item-two", "Item two").await;
    let authority_one = authority_fixture(APPROVAL_ID_A, TASK_ID_A);
    let authority_two = authority_fixture(APPROVAL_ID_B, TASK_ID_B);
    let receipt_one = receipt_fixture(&binding_id, RUN_ID_A, WORKER_ID_A, PROFILE_ID_A, HEX64_A);
    let receipt_two = receipt_fixture(&binding_id, RUN_ID_B, WORKER_ID_B, PROFILE_ID_B, HEX64_B);

    let reserved_one = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reserve-item-one",
            &item_one,
            revision_one,
            &authority_one,
        ),
    )
    .await
    .expect("reserve item one");
    let execution_one = execution_of(&reserved_one).execution_ref.clone();
    let after_reserve_one = item_snapshot(&scoped_a, &item_one).await;
    assert!(
        after_reserve_one.revision > revision_one,
        "reserve must advance the backlog item revision"
    );
    let attach_revision_one = after_reserve_one.revision;

    // Exact reserve replay keeps its original request revision and execution.
    let replayed_one = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reserve-item-one",
            &item_one,
            revision_one,
            &authority_one,
        ),
    )
    .await
    .expect("replay reserve item one");
    assert_eq!(execution_of(&replayed_one).execution_ref, execution_one);
    let after_replayed_reserve = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_replayed_reserve.revision, attach_revision_one);

    // Stale backlog item revisions are rejected without attaching the receipt.
    let stale_attach = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "attach-stale",
            &item_one,
            &execution_one,
            attach_revision_one + 9,
            &receipt_one,
        ),
    )
    .await;
    assert!(stale_attach.is_err(), "stale attach must fail");
    let after_stale_attach = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_stale_attach.revision, attach_revision_one);
    assert_eq!(after_stale_attach.status, after_reserve_one.status);

    let attached_one = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "attach-item-one",
            &item_one,
            &execution_one,
            attach_revision_one,
            &receipt_one,
        ),
    )
    .await
    .expect("attach item one");
    assert_eq!(execution_of(&attached_one).execution_ref, execution_one);

    let after_attach_one = item_snapshot(&scoped_a, &item_one).await;
    assert!(after_attach_one.revision > attach_revision_one);
    let record_revision_one = after_attach_one.revision;

    // Exact attach replay keeps its original request revision and execution.
    let replayed_attach = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "attach-item-one",
            &item_one,
            &execution_one,
            attach_revision_one,
            &receipt_one,
        ),
    )
    .await
    .expect("replay attach item one");
    assert_eq!(execution_of(&replayed_attach).execution_ref, execution_one);
    let after_replayed_attach = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_replayed_attach.revision, record_revision_one);

    // Ordinary Worker completion stays rejected even after a trusted operator reservation.
    let before_manual = item_snapshot(&scoped_a, &item_one).await;
    let manual_complete = bounded(
        "ordinary worker completion refusal",
        scoped_a.complete_backlog_item(
            "manual-worker-complete".to_string(),
            item_one.clone(),
            before_manual.revision,
            "manual note".to_string(),
            "manual evidence".to_string(),
        ),
    )
    .await;
    assert!(
        manual_complete.is_err(),
        "ordinary Worker completion must fail"
    );
    let after_manual = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_manual.revision, before_manual.revision);

    let success_one = success_report_fixture(HEX64_A);
    let recorded_one = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one",
            &item_one,
            &execution_one,
            record_revision_one,
            &success_one,
        ),
    )
    .await
    .expect("record item one success");
    assert_eq!(execution_of(&recorded_one).execution_ref, execution_one);
    let after_record_one = item_snapshot(&scoped_a, &item_one).await;
    assert!(after_record_one.revision > record_revision_one);

    // An exact outcome replay keeps its original request revision and result.
    let replayed_outcome = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one",
            &item_one,
            &execution_one,
            record_revision_one,
            &success_one,
        ),
    )
    .await
    .expect("replay item one success");
    assert_eq!(execution_of(&replayed_outcome).execution_ref, execution_one);
    let after_replayed_outcome = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_replayed_outcome.revision, after_record_one.revision);

    // A recorded terminal result cannot be changed by a new command.
    let result_change = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one-change-result",
            &item_one,
            &execution_one,
            after_record_one.revision,
            &success_report_fixture(HEX64_B),
        ),
    )
    .await;
    assert!(result_change.is_err(), "a known result must not change");
    let after_result_change = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_result_change.revision, after_record_one.revision);

    // A changed result digest under the same key is an idempotency conflict.
    let digest_conflict = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one",
            &item_one,
            &execution_one,
            record_revision_one,
            &success_report_fixture(HEX64_B),
        ),
    )
    .await
    .expect_err("changed result digest must be refused");
    assert_eq!(
        digest_conflict.to_api_error().code,
        ErrorCode::IdempotencyConflict
    );
    let after_digest_conflict = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_digest_conflict.revision, after_record_one.revision);

    // Malformed success evidence is rejected before completion.
    let malformed_digest = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one-malformed",
            &item_one,
            &execution_one,
            after_record_one.revision,
            &success_report_fixture("not-a-sha256"),
        ),
    )
    .await;
    assert!(
        malformed_digest.is_err(),
        "malformed result digest must fail"
    );

    let missing_success_digest = WorkerBacklogOutcomeReport {
        outcome: WorkerBacklogOutcome::Success,
        note: "bounded operator evidence".to_string(),
        result_digest: None,
    };
    let missing_digest = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-one-missing-digest",
            &item_one,
            &execution_one,
            after_record_one.revision,
            &missing_success_digest,
        ),
    )
    .await;
    assert!(
        missing_digest.is_err(),
        "successful outcome requires a canonical result digest"
    );

    let before_complete = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(before_complete.revision, after_record_one.revision);
    let completion_one = completion_fixture(&execution_one, HEX64_A, "Item one verified");
    let completed_one = call_operator(
        &supervisor,
        complete_body(
            &binding_id,
            THREAD_UUID_A,
            "complete-item-one",
            &item_one,
            before_complete.revision,
            &completion_one,
        ),
    )
    .await
    .expect("complete item one");
    completion_of(&completed_one);
    let completed_snapshot = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(completed_snapshot.status, BacklogStatus::Done);
    assert!(completed_snapshot.revision > before_complete.revision);

    let replayed_completion = call_operator(
        &supervisor,
        complete_body(
            &binding_id,
            THREAD_UUID_A,
            "complete-item-one",
            &item_one,
            before_complete.revision,
            &completion_one,
        ),
    )
    .await
    .expect("replay complete item one");
    completion_of(&replayed_completion);
    let after_replayed_completion = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(
        after_replayed_completion.revision,
        completed_snapshot.revision
    );
    assert_eq!(after_replayed_completion.status, BacklogStatus::Done);

    // The second item remains independent and can complete on its own execution.
    let second_snapshot = item_snapshot(&scoped_a, &item_two).await;
    assert_eq!(second_snapshot.status, BacklogStatus::Todo);
    assert_eq!(second_snapshot.revision, revision_two);

    // One approval/task authority may be reserved only once across all items.
    let duplicate_authority = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reserve-item-two-duplicate-authority",
            &item_two,
            revision_two,
            &authority_one,
        ),
    )
    .await;
    assert!(
        duplicate_authority.is_err(),
        "duplicate authority across different items must fail"
    );
    let after_duplicate_authority = item_snapshot(&scoped_a, &item_two).await;
    assert_eq!(after_duplicate_authority.revision, revision_two);
    assert_eq!(after_duplicate_authority.status, BacklogStatus::Todo);
    let attempts_after_duplicate = call_operator(
        &supervisor,
        list_attempts_body(&binding_id, THREAD_UUID_A, &item_two, 10),
    )
    .await
    .expect("attempts after duplicate authority");
    assert_eq!(attempt_count_of(&attempts_after_duplicate), 0);

    let reserved_two = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reserve-item-two",
            &item_two,
            revision_two,
            &authority_two,
        ),
    )
    .await
    .expect("reserve item two");
    let execution_two = execution_of(&reserved_two).execution_ref.clone();
    assert_ne!(execution_one, execution_two);

    let after_reserve_two = item_snapshot(&scoped_a, &item_two).await;
    assert!(after_reserve_two.revision > revision_two);
    let attach_revision_two = after_reserve_two.revision;
    let attached_two = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "attach-item-two",
            &item_two,
            &execution_two,
            attach_revision_two,
            &receipt_two,
        ),
    )
    .await
    .expect("attach item two");
    assert_eq!(execution_of(&attached_two).execution_ref, execution_two);

    let after_attach_two = item_snapshot(&scoped_a, &item_two).await;
    assert!(after_attach_two.revision > attach_revision_two);
    let record_revision_two = after_attach_two.revision;
    let recorded_two = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-item-two",
            &item_two,
            &execution_two,
            record_revision_two,
            &success_report_fixture(HEX64_B),
        ),
    )
    .await
    .expect("record item two success");
    assert_eq!(execution_of(&recorded_two).execution_ref, execution_two);

    let before_complete_two = item_snapshot(&scoped_a, &item_two).await;
    assert!(before_complete_two.revision > record_revision_two);
    let completion_two = completion_fixture(&execution_two, HEX64_B, "Item two verified");
    let completed_two = call_operator(
        &supervisor,
        complete_body(
            &binding_id,
            THREAD_UUID_A,
            "complete-item-two",
            &item_two,
            before_complete_two.revision,
            &completion_two,
        ),
    )
    .await
    .expect("complete item two");
    completion_of(&completed_two);
    let after_complete_two = item_snapshot(&scoped_a, &item_two).await;
    assert_eq!(after_complete_two.status, BacklogStatus::Done);
    assert!(after_complete_two.revision > before_complete_two.revision);
    assert_eq!(
        item_snapshot(&scoped_a, &item_one).await.status,
        BacklogStatus::Done
    );

    // Operator storage/control never creates runs, events, wake-ups, or worker records.
    let status_after = bounded("status after operator backlog", scoped_a.status())
        .await
        .expect("status after operator backlog");
    assert_eq!(status_after.total_runs, 0);
    assert_eq!(status_after.active_runs, 0);
    assert!(status_after.recent_runs.is_empty());
    assert_eq!(status_after.binding.delivered_seq, 0);
    assert_eq!(status_after.binding.acked_seq, 0);
    assert_eq!(status_after.wakeup.status, status_before.wakeup.status);
    let activity = bounded(
        "activity after operator backlog",
        scoped_a.list_activity(None, None, Some(20), None, None),
    )
    .await
    .expect("activity after operator backlog");
    assert!(activity.events.is_empty());
    let runs = bounded(
        "runs after operator backlog",
        scoped_a.list_runs(None, Some(10)),
    )
    .await
    .expect("runs after operator backlog");
    assert!(runs.runs.is_empty());
    let worker_error = bounded(
        "worker lookup after operator backlog",
        scoped_a.get_worker(RUN_ID_A.to_string()),
    )
    .await;
    assert!(
        worker_error.is_err(),
        "no runtime worker record may be created"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn operator_worker_backlog_acceptance_rejection_preserves_success_and_replays_after_reopen() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp
        .path()
        .join("operator_backlog_acceptance_rejection_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();

    let status_before = bounded("rejection status before", scoped_a.status())
        .await
        .expect("rejection status before");
    assert_eq!(status_before.total_runs, 0);

    let (item_id, revision) = create_worker_item(
        &scoped_a,
        "create-rejection-item",
        "Rejected Worker delivery",
    )
    .await;
    let authority = authority_fixture(APPROVAL_ID_A, TASK_ID_A);
    let receipt = receipt_fixture(&binding_id, RUN_ID_A, WORKER_ID_A, PROFILE_ID_A, HEX64_A);

    let reserved = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-reserve",
            &item_id,
            revision,
            &authority,
        ),
    )
    .await
    .expect("reserve rejection item");
    let execution_ref = execution_of(&reserved).execution_ref.clone();

    let attach_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    let attached = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-attach",
            &item_id,
            &execution_ref,
            attach_revision,
            &receipt,
        ),
    )
    .await
    .expect("attach rejection receipt");
    assert_eq!(execution_of(&attached).execution_ref, execution_ref);

    let record_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    let recorded = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-record",
            &item_id,
            &execution_ref,
            record_revision,
            &success_report_fixture(HEX64_B),
        ),
    )
    .await
    .expect("record rejection success");
    assert_eq!(execution_of(&recorded).execution_ref, execution_ref);

    let before_rejection = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(before_rejection.status, BacklogStatus::Doing);
    let before_rejection_revision = before_rejection.revision;
    let before_execution = call_operator(
        &supervisor,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("execution before rejection");
    let history_before_rejection = backlog_history_count(&scoped_a, &item_id).await;

    let rejection = rejection_fixture(
        &execution_ref,
        HEX64_B,
        HEX64_C,
        "Delivered artifact did not satisfy the checked acceptance condition",
    );

    // Caller-supplied time is still rejected by the strict request contract.
    let mut injected_time = reject_body(
        &binding_id,
        THREAD_UUID_A,
        "reject-injected-time",
        &item_id,
        before_rejection_revision,
        &rejection,
    );
    injected_time["now_ms"] = json!(1);
    assert!(
        serde_json::from_value::<OperatorWorkerBacklogRequest>(injected_time).is_err(),
        "rejection requests must reject caller time"
    );

    // Cross-thread scope is validated before any idempotency replay or mutation.
    let cross_thread = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_B,
            "reject-cross-thread",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await
    .expect_err("foreign thread UUID must fail");
    assert_eq!(
        cross_thread.to_api_error().code,
        ErrorCode::ValidationFailed
    );
    assert_eq!(item_snapshot(&scoped_a, &item_id).await, before_rejection);
    let after_cross_thread_execution = call_operator(
        &supervisor,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("execution after cross-thread rejection");
    assert_eq!(
        value_of(&after_cross_thread_execution),
        value_of(&before_execution)
    );
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before_rejection
    );

    // Stale revisions are refused without mutating the item or its execution.
    let stale = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-stale",
            &item_id,
            before_rejection_revision - 1,
            &rejection,
        ),
    )
    .await
    .expect_err("stale rejection must fail");
    assert_eq!(stale.to_api_error().code, ErrorCode::StaleRevision);
    assert_eq!(item_snapshot(&scoped_a, &item_id).await, before_rejection);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before_rejection
    );

    // A different, structurally valid result digest is refused.
    let mut bad_result_digest = rejection.clone();
    bad_result_digest.result_digest = HEX64_D.to_string();
    let bad_result = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-bad-result-digest",
            &item_id,
            before_rejection_revision,
            &bad_result_digest,
        ),
    )
    .await
    .expect_err("mismatched result digest must fail");
    assert_eq!(bad_result.to_api_error().code, ErrorCode::ValidationFailed);
    assert_eq!(item_snapshot(&scoped_a, &item_id).await, before_rejection);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before_rejection
    );

    // A malformed delivery digest is refused before any record is committed.
    let mut bad_delivery_digest = rejection.clone();
    bad_delivery_digest.delivery_digest = "B".repeat(64);
    let bad_delivery = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-bad-delivery-digest",
            &item_id,
            before_rejection_revision,
            &bad_delivery_digest,
        ),
    )
    .await
    .expect_err("malformed delivery digest must fail");
    assert_eq!(
        bad_delivery.to_api_error().code,
        ErrorCode::ValidationFailed
    );
    assert_eq!(item_snapshot(&scoped_a, &item_id).await, before_rejection);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before_rejection
    );
    let after_refusals_execution = call_operator(
        &supervisor,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("execution after refused rejections");
    assert_eq!(
        value_of(&after_refusals_execution),
        value_of(&before_execution)
    );

    let rejected_response = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-acceptance",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await
    .expect("reject Worker acceptance");
    let rejected = rejection_of(&rejected_response);
    assert_eq!(rejected.item.status, BacklogStatus::Blocked);
    assert_eq!(rejected.item.revision, before_rejection_revision + 1);
    assert!(rejected.item.completion_note.is_none());
    assert!(rejected.item.completion_evidence.is_none());
    assert!(rejected.item.completed_at_ms.is_none());
    assert_eq!(
        rejected.item.current_execution_ref.as_deref(),
        Some(execution_ref.as_str())
    );
    assert!(rejected
        .item
        .blocked_reason
        .as_deref()
        .is_some_and(|reason| {
            reason.starts_with("worker acceptance rejected: ") && reason.contains(&rejection.reason)
        }));
    let original_outcome = rejected
        .execution
        .outcome
        .as_ref()
        .expect("original successful outcome");
    assert_eq!(original_outcome.outcome, WorkerBacklogOutcome::Success);
    assert_eq!(original_outcome.result_digest.as_deref(), Some(HEX64_B));
    assert!(rejected.execution.completion.is_none());
    let stored_rejection = rejected
        .execution
        .acceptance_rejection
        .as_ref()
        .expect("stored acceptance rejection");
    assert_eq!(stored_rejection.result_digest, HEX64_B);
    assert_eq!(stored_rejection.delivery_digest, HEX64_C);
    assert_eq!(stored_rejection.reason, rejection.reason);
    assert_eq!(stored_rejection.evidence, rejection.evidence);
    assert!(stored_rejection.rejected_at_ms > 0);

    let rejected_value = value_of(&rejected_response);
    assert_eq!(rejected_value["kind"], json!("acceptance_rejection"));
    let rejected_item = rejected.item.clone();
    let rejected_execution = rejected.execution.clone();
    let rejected_revision = rejected_item.revision;
    let history_after_rejection = backlog_history_count(&scoped_a, &item_id).await;
    assert_eq!(history_after_rejection, history_before_rejection + 1);

    // The exact command key remains bound to the current thread identity.
    let cross_thread_replay = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_B,
            "reject-acceptance",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await;
    assert!(
        cross_thread_replay.is_err(),
        "thread scope must be checked before command replay"
    );
    assert_eq!(
        item_snapshot(&scoped_a, &item_id).await.revision,
        rejected_revision
    );

    // An exact replay returns the original rejection receipt without a new revision,
    // history entry, rejection record, or fabricated runtime state.
    let replayed_response = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-acceptance",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await
    .expect("replay acceptance rejection");
    assert_eq!(value_of(&replayed_response), rejected_value);
    assert_eq!(item_snapshot(&scoped_a, &item_id).await, rejected_item);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_after_rejection
    );

    let status_after = bounded("rejection status after", scoped_a.status())
        .await
        .expect("rejection status after");
    assert_eq!(status_after.total_runs, 0);
    assert_eq!(status_after.active_runs, 0);
    assert!(status_after.recent_runs.is_empty());
    assert_eq!(status_after.binding.delivered_seq, 0);
    assert_eq!(status_after.binding.acked_seq, 0);
    assert_eq!(status_after.wakeup.status, status_before.wakeup.status);
    let activity = bounded(
        "rejection activity after",
        scoped_a.list_activity(None, None, Some(20), None, None),
    )
    .await
    .expect("rejection activity after");
    assert!(activity.events.is_empty());
    let runs = bounded("rejection runs after", scoped_a.list_runs(None, Some(10)))
        .await
        .expect("rejection runs after");
    assert!(runs.runs.is_empty());
    let worker_error = bounded(
        "rejection worker lookup",
        scoped_a.get_worker(RUN_ID_A.to_string()),
    )
    .await;
    assert!(
        worker_error.is_err(),
        "acceptance rejection must not create a runtime worker record"
    );

    handle.shutdown().await;

    let reopened_handle = bounded(
        "reopen rejection supervisor",
        Supervisor::start(fast_supervisor_config(&state_dir)),
    )
    .await
    .expect("reopen rejection supervisor");
    let reopened = SupervisorClient::from_state_dir(&state_dir).expect("reopened rejection client");
    let reopened_scoped = reopened
        .clone()
        .scoped(&binding_id)
        .expect("reopened rejection scoped binding");

    let reopened_replay = call_operator(
        &reopened,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-acceptance",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await
    .expect("replayed rejection after reopen");
    assert_eq!(value_of(&reopened_replay), rejected_value);
    assert_eq!(
        item_snapshot(&reopened_scoped, &item_id).await,
        rejected_item
    );
    let retained_execution = call_operator(
        &reopened,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("reopened rejected execution");
    assert_eq!(execution_of(&retained_execution), &rejected_execution);
    assert_eq!(
        backlog_history_count(&reopened_scoped, &item_id).await,
        history_after_rejection
    );
    let attempts = call_operator(
        &reopened,
        list_attempts_body(&binding_id, THREAD_UUID_A, &item_id, 10),
    )
    .await
    .expect("reopened rejection attempts");
    assert_eq!(attempt_count_of(&attempts), 1);

    // A blocked item cannot be rejected a second time by a new command key.
    let after_reopen_rejection = call_operator(
        &reopened,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-after-reopen",
            &item_id,
            rejected_revision,
            &rejection,
        ),
    )
    .await;
    assert!(
        after_reopen_rejection.is_err(),
        "a blocked item cannot be rejected again"
    );
    assert_eq!(
        item_snapshot(&reopened_scoped, &item_id).await,
        rejected_item
    );
    assert_eq!(
        backlog_history_count(&reopened_scoped, &item_id).await,
        history_after_rejection
    );

    reopened_handle.shutdown().await;
}

#[tokio::test]
async fn operator_worker_backlog_rejects_unauthorized_foreign_scope_and_unknown_replay() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("operator_backlog_reject_state");
    let (handle, supervisor, scoped_a, scoped_b) = start_fixture(&state_dir).await;
    let binding_a = scoped_a.binding_id().to_string();
    let binding_b = scoped_b.binding_id().to_string();

    // Raw loopback HTTP without the private bearer token is rejected before parsing.
    let descriptor = read_and_validate_descriptor(&state_dir).expect("operator descriptor");
    let url = format!(
        "{}{OPERATOR_WORKER_BACKLOG_PATH}",
        descriptor.base_url().expect("operator base URL")
    );
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(FIXTURE_TIMEOUT)
        .build()
        .expect("operator raw HTTP client");
    let unauthorized = bounded(
        "unauthorized operator request",
        http.post(&url).json(&json!({ "op": "reserve" })).send(),
    )
    .await
    .expect("unauthorized operator response");
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
    let unauthorized: Value = bounded("unauthorized operator body", unauthorized.json())
        .await
        .expect("unauthorized operator body");
    assert_eq!(unauthorized["code"], json!("unauthorized"));

    let (item_one, revision_one) =
        create_worker_item(&scoped_a, "create-reject-item", "Reject item").await;
    let authority = authority_fixture(APPROVAL_ID_A, TASK_ID_A);
    let receipt = receipt_fixture(&binding_a, RUN_ID_A, WORKER_ID_A, PROFILE_ID_A, HEX64_A);

    // The real HTTP route rejects caller-supplied time even with a valid bearer token.
    let mut http_injection = reserve_body(
        &binding_a,
        THREAD_UUID_A,
        "http-injected-time",
        &item_one,
        revision_one,
        &authority,
    );
    http_injection["now_ms"] = json!(1);
    let injected_response = bounded(
        "authenticated operator injection",
        http.post(&url)
            .bearer_auth(descriptor.token.as_str())
            .json(&http_injection)
            .send(),
    )
    .await
    .expect("authenticated operator injection response");
    assert_eq!(injected_response.status(), reqwest::StatusCode::BAD_REQUEST);
    let injected_error: Value = bounded(
        "authenticated operator injection body",
        injected_response.json(),
    )
    .await
    .expect("authenticated operator injection body");
    assert_eq!(injected_error["code"], json!("invalid_request"));

    // A mismatched thread UUID is rejected before any reservation effect.
    let thread_mismatch = call_operator(
        &supervisor,
        reserve_body(
            &binding_a,
            THREAD_UUID_B,
            "reserve-thread-mismatch",
            &item_one,
            revision_one,
            &authority,
        ),
    )
    .await;
    assert!(thread_mismatch.is_err(), "foreign thread UUID must fail");
    let after_thread_mismatch = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_thread_mismatch.revision, revision_one);
    assert_eq!(after_thread_mismatch.status, BacklogStatus::Todo);

    // A foreign binding cannot mutate another chat's item.
    let foreign_binding = call_operator(
        &supervisor,
        reserve_body(
            &binding_b,
            THREAD_UUID_B,
            "reserve-foreign-binding",
            &item_one,
            revision_one,
            &authority,
        ),
    )
    .await;
    assert!(foreign_binding.is_err(), "foreign binding must fail");
    let after_foreign = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_foreign.revision, revision_one);
    assert_eq!(after_foreign.status, BacklogStatus::Todo);

    let attempts_after_reject = call_operator(
        &supervisor,
        list_attempts_body(&binding_a, THREAD_UUID_A, &item_one, 10),
    )
    .await
    .expect("attempts after foreign reject");
    assert_eq!(attempt_count_of(&attempts_after_reject), 0);

    let reserved = call_operator(
        &supervisor,
        reserve_body(
            &binding_a,
            THREAD_UUID_A,
            "reserve-unknown-item",
            &item_one,
            revision_one,
            &authority,
        ),
    )
    .await
    .expect("reserve unknown item");
    let reserved = execution_of(&reserved);
    let execution_ref = reserved.execution_ref.clone();
    let after_reserve = item_snapshot(&scoped_a, &item_one).await;
    assert!(after_reserve.revision > revision_one);

    // Scope is checked before an exact command replay can observe reserved state.
    let replay_foreign_binding = call_operator(
        &supervisor,
        reserve_body(
            &binding_b,
            THREAD_UUID_B,
            "reserve-unknown-item",
            &item_one,
            revision_one,
            &authority,
        ),
    )
    .await;
    assert!(
        replay_foreign_binding.is_err(),
        "foreign scope must fail before command replay"
    );
    let after_replay_foreign = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(after_replay_foreign.revision, after_reserve.revision);
    assert_eq!(after_replay_foreign.status, after_reserve.status);

    let attach_revision = after_reserve.revision;
    let attached = call_operator(
        &supervisor,
        attach_body(
            &binding_a,
            THREAD_UUID_A,
            "attach-unknown-item",
            &item_one,
            &execution_ref,
            attach_revision,
            &receipt,
        ),
    )
    .await
    .expect("attach unknown item");
    assert_eq!(execution_of(&attached).execution_ref, execution_ref);
    let after_attach = item_snapshot(&scoped_a, &item_one).await;
    assert!(after_attach.revision > attach_revision);

    let record_revision = after_attach.revision;
    let unknown = call_operator(
        &supervisor,
        record_body(
            &binding_a,
            THREAD_UUID_A,
            "record-unknown-item",
            &item_one,
            &execution_ref,
            record_revision,
            &unknown_report_fixture(),
        ),
    )
    .await
    .expect("record unknown outcome");
    assert_eq!(execution_of(&unknown).execution_ref, execution_ref);
    let after_unknown = item_snapshot(&scoped_a, &item_one).await;
    assert!(after_unknown.revision > record_revision);
    assert_ne!(after_unknown.status, BacklogStatus::Done);

    let before_unknown_complete = item_snapshot(&scoped_a, &item_one).await;
    let complete_unknown = call_operator(
        &supervisor,
        complete_body(
            &binding_a,
            THREAD_UUID_A,
            "complete-unknown-item",
            &item_one,
            before_unknown_complete.revision,
            &completion_fixture(&execution_ref, HEX64_A, "Unknown outcome must not complete"),
        ),
    )
    .await;
    assert!(
        complete_unknown.is_err(),
        "unknown outcome must not complete"
    );
    let after_unknown_complete = item_snapshot(&scoped_a, &item_one).await;
    assert_eq!(
        after_unknown_complete.revision,
        before_unknown_complete.revision
    );
    assert_ne!(after_unknown_complete.status, BacklogStatus::Done);

    // An unknown execution is not silently replayed by a new reservation key.
    let unknown_replay = call_operator(
        &supervisor,
        reserve_body(
            &binding_a,
            THREAD_UUID_A,
            "reserve-after-unknown",
            &item_one,
            before_unknown_complete.revision,
            &authority,
        ),
    )
    .await;
    assert!(unknown_replay.is_err(), "unknown outcome must not replay");
    let attempts_after_unknown = call_operator(
        &supervisor,
        list_attempts_body(&binding_a, THREAD_UUID_A, &item_one, 10),
    )
    .await
    .expect("attempts after unknown");
    assert_eq!(attempt_count_of(&attempts_after_unknown), 1);
    let fetched_unknown = call_operator(
        &supervisor,
        get_execution_body(&binding_a, THREAD_UUID_A, &item_one, &execution_ref),
    )
    .await
    .expect("get unknown execution");
    let fetched_unknown = execution_of(&fetched_unknown);
    assert_eq!(fetched_unknown.execution_ref, execution_ref);
    assert_eq!(
        fetched_unknown.reservation_revision,
        reserved.reservation_revision
    );
    assert_ne!(
        item_snapshot(&scoped_a, &item_one).await.status,
        BacklogStatus::Done
    );

    // Caller-supplied time, success, and ordinary scoped variants are rejected.
    let mut injected_time = reserve_body(
        &binding_a,
        THREAD_UUID_A,
        "reserve-injected-time",
        &item_one,
        before_unknown_complete.revision,
        &authority,
    );
    injected_time["now_ms"] = json!(1);
    assert!(
        serde_json::from_value::<OperatorWorkerBacklogRequest>(injected_time).is_err(),
        "operator requests must reject caller time"
    );
    let mut injected_success = reserve_body(
        &binding_a,
        THREAD_UUID_A,
        "reserve-injected-success",
        &item_one,
        before_unknown_complete.revision,
        &authority,
    );
    injected_success["success"] = json!(true);
    assert!(
        serde_json::from_value::<OperatorWorkerBacklogRequest>(injected_success).is_err(),
        "operator requests must reject caller success flags"
    );
    for op in [
        "reserve",
        "attach_dispatch_receipt",
        "record_outcome",
        "complete",
        "reject_acceptance",
        "get_execution",
        "list_attempts",
    ] {
        let injected = json!({
            "op": op,
            "binding_id": binding_a,
            "thread_uuid": THREAD_UUID_A,
        });
        let error = serde_json::from_value::<ScopedRequest>(injected)
            .expect_err("ScopedRequest must not deserialize operator ops");
        assert!(
            error.to_string().contains("unknown variant"),
            "ScopedRequest must reject operator op `{op}` by variant: {error}"
        );
    }
    assert!(
        serde_json::from_value::<OperatorWorkerBacklogRequest>(json!({
            "op": "backlog_create",
            "binding_id": binding_a,
            "thread_uuid": THREAD_UUID_A,
        }))
        .is_err(),
        "operator request must not accept ordinary scoped ops"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn operator_worker_backlog_reads_are_retained_across_restart() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("operator_backlog_restart_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();
    let (item_id, revision) =
        create_worker_item(&scoped_a, "create-restart-item", "Restart item").await;
    let authority = authority_fixture(APPROVAL_ID_A, TASK_ID_A);
    let receipt = receipt_fixture(&binding_id, RUN_ID_A, WORKER_ID_A, PROFILE_ID_A, HEX64_A);

    let reserved = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reserve-restart-item",
            &item_id,
            revision,
            &authority,
        ),
    )
    .await
    .expect("reserve restart item");
    let reserved = execution_of(&reserved);
    let execution_ref = reserved.execution_ref.clone();

    let attach_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    assert!(attach_revision > revision);
    let attached = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "attach-restart-item",
            &item_id,
            &execution_ref,
            attach_revision,
            &receipt,
        ),
    )
    .await
    .expect("attach restart item");
    assert_eq!(execution_of(&attached).execution_ref, execution_ref);

    let record_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    assert!(record_revision > attach_revision);
    let recorded = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "record-restart-item",
            &item_id,
            &execution_ref,
            record_revision,
            &success_report_fixture(HEX64_A),
        ),
    )
    .await
    .expect("record restart item");
    assert_eq!(execution_of(&recorded).execution_ref, execution_ref);

    let before_complete = item_snapshot(&scoped_a, &item_id).await;
    assert!(before_complete.revision > record_revision);
    let completion = completion_fixture(&execution_ref, HEX64_A, "Restart item verified");
    let completed = call_operator(
        &supervisor,
        complete_body(
            &binding_id,
            THREAD_UUID_A,
            "complete-restart-item",
            &item_id,
            before_complete.revision,
            &completion,
        ),
    )
    .await
    .expect("complete restart item");
    completion_of(&completed);
    let completed_snapshot = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(completed_snapshot.status, BacklogStatus::Done);
    assert!(completed_snapshot.revision > before_complete.revision);
    let completed_revision = completed_snapshot.revision;

    handle.shutdown().await;

    let reopened_handle = bounded(
        "restart operator backlog supervisor",
        Supervisor::start(fast_supervisor_config(&state_dir)),
    )
    .await
    .expect("restart operator backlog supervisor");
    let reopened = SupervisorClient::from_state_dir(&state_dir).expect("reopened operator client");
    let reopened_scoped = reopened
        .clone()
        .scoped(&binding_id)
        .expect("reopened operator scoped binding");

    let retained = call_operator(
        &reopened,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("reopened get execution");
    let retained = execution_of(&retained);
    assert_eq!(retained.execution_ref, execution_ref);
    assert_eq!(retained.reservation_revision, reserved.reservation_revision);
    let attempts = call_operator(
        &reopened,
        list_attempts_body(&binding_id, THREAD_UUID_A, &item_id, 10),
    )
    .await
    .expect("reopened list attempts");
    assert_eq!(attempt_count_of(&attempts), 1);
    let retained_item = item_snapshot(&reopened_scoped, &item_id).await;
    assert_eq!(retained_item.revision, completed_revision);
    assert_eq!(retained_item.status, BacklogStatus::Done);

    let status = bounded("reopened status", reopened_scoped.status())
        .await
        .expect("reopened status");
    assert_eq!(status.total_runs, 0);
    assert_eq!(status.active_runs, 0);
    assert!(status.recent_runs.is_empty());
    assert_eq!(status.binding.delivered_seq, 0);
    assert_eq!(status.binding.acked_seq, 0);
    let activity = bounded(
        "reopened activity",
        reopened_scoped.list_activity(None, None, Some(20), None, None),
    )
    .await
    .expect("reopened activity");
    assert!(activity.events.is_empty());
    let worker_error = bounded(
        "reopened worker lookup",
        reopened_scoped.get_worker(RUN_ID_A.to_string()),
    )
    .await;
    assert!(
        worker_error.is_err(),
        "restart must not create runtime worker records"
    );

    reopened_handle.shutdown().await;
}

#[tokio::test]
async fn operator_worker_backlog_completion_http_budget_applies_to_inner_payload() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("operator_backlog_budget_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();
    let descriptor = read_and_validate_descriptor(&state_dir).expect("operator descriptor");
    let url = format!(
        "{}{OPERATOR_WORKER_BACKLOG_PATH}",
        descriptor.base_url().expect("operator base URL")
    );
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(FIXTURE_TIMEOUT)
        .build()
        .expect("operator budget HTTP client");

    let authority = WorkerBacklogAuthority {
        approval_id: APPROVAL_ID_A.to_string(),
        subject_digest: HEX64_A.to_string(),
        task_id: "task-one".to_string(),
        task_digest: HEX64_B.to_string(),
    };
    let dispatch_receipt = WorkerBacklogDispatchReceipt {
        session_binding_id: binding_id.clone(),
        run_id: "run_budget_proof".to_string(),
        worker_id: "wrk_budget_proof".to_string(),
        profile_id: "default".to_string(),
        execution_binding_digest: None,
    };
    let (item_id, execution_ref, before_complete_revision) = prepare_worker_item_for_completion(
        &supervisor,
        &scoped_a,
        &binding_id,
        "budget-complete",
        "Task",
        &authority,
        &dispatch_receipt,
    )
    .await;
    let history_before_complete = backlog_history_count(&scoped_a, &item_id).await;

    let proof_text = format!("x{}", "\u{1}".repeat(1011));
    assert_eq!(proof_text.len(), 1012);
    let completion = WorkerBacklogCompletion {
        execution_ref: execution_ref.clone(),
        result_digest: HEX64_C.to_string(),
        acceptance_note: proof_text.clone(),
        acceptance_evidence: proof_text,
        delivery_digest: HEX64_D.to_string(),
    };
    let body = complete_body(
        &binding_id,
        THREAD_UUID_A,
        "budget-complete-complete",
        &item_id,
        before_complete_revision,
        &completion,
    );

    let first = bounded(
        "budget completion HTTP response",
        http.post(&url)
            .bearer_auth(descriptor.token.as_str())
            .json(&body)
            .send(),
    )
    .await
    .expect("budget completion HTTP response");
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first_bytes = bounded("budget completion HTTP body", first.bytes())
        .await
        .expect("budget completion HTTP body");
    assert!(
        first_bytes.len() <= MAX_CLIENT_RESPONSE_BYTES,
        "actual serialized HTTP reply {} exceeds client cap {MAX_CLIENT_RESPONSE_BYTES}",
        first_bytes.len()
    );
    let first_response: OperatorWorkerBacklogResponse =
        serde_json::from_slice(&first_bytes).expect("budget completion response JSON");
    let completion_receipt = match &first_response {
        OperatorWorkerBacklogResponse::Completion { completion } => completion,
        other => panic!("expected completion response, got {other:?}"),
    };
    // Measure the actual typed receipt with the production estimate; do not guess
    // schema-specific byte counts or assume a literal envelope size.
    let inner_bytes =
        estimate_wire_response_bytes(completion_receipt).expect("inner receipt estimate");
    let envelope_bytes =
        estimate_wire_response_bytes(&first_response).expect("operator envelope estimate");
    assert!(
        inner_bytes <= MAX_COMPACT_RESPONSE_BYTES,
        "inner completion receipt {inner_bytes} exceeds compact budget {MAX_COMPACT_RESPONSE_BYTES}"
    );
    assert!(
        envelope_bytes > MAX_COMPACT_RESPONSE_BYTES,
        "fixture must place the operator envelope above the compact budget: inner {inner_bytes}, envelope {envelope_bytes}"
    );
    let first_value = value_of(&first_response);

    let after_complete = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(after_complete.status, BacklogStatus::Done);
    assert!(after_complete.revision > before_complete_revision);
    let completed_revision = after_complete.revision;
    let history_after_complete = backlog_history_count(&scoped_a, &item_id).await;
    assert_eq!(history_after_complete, history_before_complete + 1);

    let replay = bounded(
        "budget completion replay HTTP response",
        http.post(&url)
            .bearer_auth(descriptor.token.as_str())
            .json(&body)
            .send(),
    )
    .await
    .expect("budget completion replay HTTP response");
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    let replay_response: OperatorWorkerBacklogResponse =
        bounded("budget completion replay HTTP body", replay.json())
            .await
            .expect("budget completion replay HTTP body");
    assert_eq!(value_of(&replay_response), first_value);
    let after_replay = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(after_replay.revision, completed_revision);
    assert_eq!(after_replay.status, BacklogStatus::Done);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_after_complete
    );

    // A completion whose inner payload still exceeds the compact budget is refused
    // by the core before any item mutation or history append.
    let oversize_authority = WorkerBacklogAuthority {
        approval_id: APPROVAL_ID_B.to_string(),
        subject_digest: HEX64_A.to_string(),
        task_id: "task-two".to_string(),
        task_digest: HEX64_B.to_string(),
    };
    let oversize_receipt = WorkerBacklogDispatchReceipt {
        session_binding_id: binding_id.clone(),
        run_id: "run_budget_oversize".to_string(),
        worker_id: "wrk_budget_oversize".to_string(),
        profile_id: "default".to_string(),
        execution_binding_digest: None,
    };
    let oversize_title = "T".repeat(256);
    let (oversize_item_id, oversize_execution_ref, oversize_revision) =
        prepare_worker_item_for_completion(
            &supervisor,
            &scoped_a,
            &binding_id,
            "budget-oversize",
            &oversize_title,
            &oversize_authority,
            &oversize_receipt,
        )
        .await;
    let oversize_history = backlog_history_count(&scoped_a, &oversize_item_id).await;
    // The known-permitted completion text plus a maximum-length title still pushes
    // the inner receipt over the compact budget.
    let oversize_text = format!("x{}", "\u{1}".repeat(1011));
    let oversize_completion = WorkerBacklogCompletion {
        execution_ref: oversize_execution_ref,
        result_digest: HEX64_C.to_string(),
        acceptance_note: oversize_text.clone(),
        acceptance_evidence: oversize_text,
        delivery_digest: HEX64_D.to_string(),
    };
    let oversize_body = complete_body(
        &binding_id,
        THREAD_UUID_A,
        "budget-oversize-complete",
        &oversize_item_id,
        oversize_revision,
        &oversize_completion,
    );
    let oversize = bounded(
        "oversize budget completion HTTP response",
        http.post(&url)
            .bearer_auth(descriptor.token.as_str())
            .json(&oversize_body)
            .send(),
    )
    .await
    .expect("oversize budget completion HTTP response");
    assert_eq!(oversize.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let oversize_error: Value = bounded("oversize budget completion body", oversize.json())
        .await
        .expect("oversize budget completion body");
    assert_eq!(oversize_error["code"], json!("payload_too_large"));
    assert_eq!(
        oversize_error["details"]["max_bytes"].as_u64(),
        Some(MAX_COMPACT_RESPONSE_BYTES as u64)
    );
    let oversize_size = oversize_error["details"]["size_bytes"]
        .as_u64()
        .expect("oversize size_bytes");
    assert!(oversize_size > MAX_COMPACT_RESPONSE_BYTES as u64);
    let after_oversize = item_snapshot(&scoped_a, &oversize_item_id).await;
    assert_eq!(after_oversize.revision, oversize_revision);
    assert_eq!(after_oversize.status, BacklogStatus::Doing);
    assert_eq!(
        backlog_history_count(&scoped_a, &oversize_item_id).await,
        oversize_history
    );

    handle.shutdown().await;

    let reopened_handle = bounded(
        "reopen budget supervisor",
        Supervisor::start(fast_supervisor_config(&state_dir)),
    )
    .await
    .expect("reopen budget supervisor");
    let reopened = SupervisorClient::from_state_dir(&state_dir).expect("reopened budget client");
    let reopened_scoped = reopened
        .clone()
        .scoped(&binding_id)
        .expect("reopened budget scoped binding");
    let retained = item_snapshot(&reopened_scoped, &item_id).await;
    assert_eq!(retained.status, BacklogStatus::Done);
    assert_eq!(retained.revision, completed_revision);
    reopened_handle.shutdown().await;
}

#[tokio::test]
async fn operator_worker_backlog_acceptance_rejection_http_budget_applies_before_commit() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("operator_backlog_rejection_budget_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();
    let descriptor = read_and_validate_descriptor(&state_dir).expect("operator descriptor");
    let url = format!(
        "{}{OPERATOR_WORKER_BACKLOG_PATH}",
        descriptor.base_url().expect("operator base URL")
    );
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(FIXTURE_TIMEOUT)
        .build()
        .expect("operator rejection budget HTTP client");

    let control = "\u{1}";
    let title = control.repeat(256);
    let acceptance_condition = control.repeat(2_048);
    let delivery_description = "b".repeat(2_048);
    let (item_id, revision) = create_worker_item_with_details(
        &scoped_a,
        "reject-budget-create",
        &title,
        &acceptance_condition,
        &delivery_description,
    )
    .await;
    let authority = authority_fixture(APPROVAL_ID_A, TASK_ID_A);
    let receipt = receipt_fixture(&binding_id, RUN_ID_A, WORKER_ID_A, PROFILE_ID_A, HEX64_A);
    let reserved = call_operator(
        &supervisor,
        reserve_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-budget-reserve",
            &item_id,
            revision,
            &authority,
        ),
    )
    .await
    .expect("reserve rejection budget item");
    let execution_ref = execution_of(&reserved).execution_ref.clone();

    let attach_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    let attached = call_operator(
        &supervisor,
        attach_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-budget-attach",
            &item_id,
            &execution_ref,
            attach_revision,
            &receipt,
        ),
    )
    .await
    .expect("attach rejection budget receipt");
    assert_eq!(execution_of(&attached).execution_ref, execution_ref);

    let record_revision = item_snapshot(&scoped_a, &item_id).await.revision;
    let recorded = call_operator(
        &supervisor,
        record_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-budget-record",
            &item_id,
            &execution_ref,
            record_revision,
            &success_report_fixture(HEX64_C),
        ),
    )
    .await
    .expect("record rejection budget success");
    assert_eq!(execution_of(&recorded).execution_ref, execution_ref);

    let before_rejection = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(before_rejection.status, BacklogStatus::Doing);
    let before_rejection_revision = before_rejection.revision;
    let history_before = backlog_history_count(&scoped_a, &item_id).await;

    // Maximum-length control-heavy item and rejection texts push the inner
    // receipt over the compact budget even though each field is valid.
    let oversize_rejection = WorkerBacklogAcceptanceRejection {
        execution_ref: execution_ref.clone(),
        result_digest: HEX64_C.to_string(),
        delivery_digest: HEX64_D.to_string(),
        reason: "\u{1}".repeat(768),
        evidence: "\u{1}".repeat(2_048),
    };
    let oversize_body = reject_body(
        &binding_id,
        THREAD_UUID_A,
        "reject-budget",
        &item_id,
        before_rejection_revision,
        &oversize_rejection,
    );
    let oversize = bounded(
        "oversize rejection HTTP response",
        http.post(&url)
            .bearer_auth(descriptor.token.as_str())
            .json(&oversize_body)
            .send(),
    )
    .await
    .expect("oversize rejection HTTP response");
    assert_eq!(oversize.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let oversize_error: Value = bounded("oversize rejection body", oversize.json())
        .await
        .expect("oversize rejection body");
    assert_eq!(oversize_error["code"], json!("payload_too_large"));
    assert_eq!(
        oversize_error["details"]["max_bytes"].as_u64(),
        Some(MAX_COMPACT_RESPONSE_BYTES as u64)
    );
    let oversize_size = oversize_error["details"]["size_bytes"]
        .as_u64()
        .expect("oversize rejection size_bytes");
    assert!(oversize_size > MAX_COMPACT_RESPONSE_BYTES as u64);

    // The refused response is not committed, so the item, history, and execution
    // remain unchanged and the command key is not poisoned.
    let after_oversize = item_snapshot(&scoped_a, &item_id).await;
    assert_eq!(after_oversize, before_rejection);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before
    );
    let execution_after_oversize = call_operator(
        &supervisor,
        get_execution_body(&binding_id, THREAD_UUID_A, &item_id, &execution_ref),
    )
    .await
    .expect("execution after oversize rejection");
    assert!(execution_of(&execution_after_oversize)
        .acceptance_rejection
        .is_none());

    let rejection = rejection_fixture(
        &execution_ref,
        HEX64_C,
        HEX64_D,
        "Bounded failed acceptance after the oversized refusal",
    );
    let recovered = call_operator(
        &supervisor,
        reject_body(
            &binding_id,
            THREAD_UUID_A,
            "reject-budget",
            &item_id,
            before_rejection_revision,
            &rejection,
        ),
    )
    .await
    .expect("rejection after oversized refusal");
    let recovered = rejection_of(&recovered);
    assert_eq!(recovered.item.status, BacklogStatus::Blocked);
    assert_eq!(
        backlog_history_count(&scoped_a, &item_id).await,
        history_before + 1
    );

    handle.shutdown().await;
}
