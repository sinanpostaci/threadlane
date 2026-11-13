use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use rmcp::handler::server::wrapper::Parameters;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

use threadlane::descriptor::read_and_validate_descriptor;
use threadlane::mcp::{
    BacklogBlockParams, BacklogCompleteParams, BacklogCreateParams, BacklogEditParams,
    BacklogGetParams, BacklogHistoryParams, BacklogKindParam, BacklogListParams,
    BacklogStartParams,
};
use threadlane::{
    ErrorCode, HostControlConfig, HostControlOwnerScope, HostControlRuntimeState, ScopedClient,
    ScopedRequest, Supervisor, SupervisorClient, SupervisorConfig, ThreadlaneMcpServer,
};
use threadlane_core::{BacklogItemKind, WakeupStatus};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const FIXTURE_TIMEOUT: Duration = Duration::from_secs(10);

fn fast_supervisor_config(state_dir: &Path) -> SupervisorConfig {
    SupervisorConfig {
        state_dir: state_dir.to_path_buf(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: None,
    }
}

async fn bounded<F, T>(label: &str, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(FIXTURE_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
}

fn value_of<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("serialize backlog transport value")
}

fn item_id_of(value: &Value) -> String {
    for key in ["item_id", "id"] {
        if let Some(item_id) = value.get(key).and_then(Value::as_str) {
            return item_id.to_string();
        }
    }
    panic!("backlog item identifier missing from {value}");
}

fn revision_of(value: &Value) -> u64 {
    value
        .get("revision")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| panic!("backlog item revision missing from {value}"))
}

fn object_array(value: &Value) -> &Vec<Value> {
    for key in [
        "items",
        "backlog_items",
        "entries",
        "records",
        "history",
        "history_records",
        "runs",
        "events",
    ] {
        if let Some(array) = value.get(key).and_then(Value::as_array) {
            return array;
        }
    }
    if let Some(map) = value.as_object() {
        for candidate in map.values() {
            if let Some(array) = candidate.as_array() {
                return array;
            }
        }
    }
    panic!("bounded object array missing from {value}");
}

fn cursor_of(value: &Value) -> Option<String> {
    let map = value.as_object()?;
    for (key, candidate) in map {
        if key.contains("cursor") {
            if let Some(cursor) = candidate.as_str() {
                return Some(cursor.to_string());
            }
        }
    }
    None
}

fn json_contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(values) => values.iter().any(|value| json_contains_text(value, needle)),
        Value::Object(map) => map.values().any(|value| json_contains_text(value, needle)),
        _ => false,
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
        "start supervisor",
        Supervisor::start(fast_supervisor_config(state_dir)),
    )
    .await
    .expect("start backlog supervisor");
    let supervisor = SupervisorClient::from_state_dir(state_dir).expect("supervisor client");
    let binding_a = bounded(
        "create binding A",
        supervisor.create_binding(THREAD_UUID_A, "Backlog Chat A"),
    )
    .await
    .expect("create binding A");
    let binding_b = bounded(
        "create binding B",
        supervisor.create_binding(THREAD_UUID_B, "Backlog Chat B"),
    )
    .await
    .expect("create binding B");
    let scoped_a = supervisor
        .clone()
        .scoped(&binding_a.binding_id)
        .expect("scoped binding A");
    let scoped_b = supervisor
        .clone()
        .scoped(&binding_b.binding_id)
        .expect("scoped binding B");
    (handle, supervisor, scoped_a, scoped_b)
}

#[test]
fn backlog_requests_are_non_scheduling_with_server_owned_time_and_scope() {
    let read = ScopedRequest::BacklogGet {
        item_id: "itm_test".to_string(),
    };
    assert!(read.is_backlog());
    assert!(!read.is_mutating());
    assert!(!read.may_affect_scheduling());
    assert_eq!(
        serde_json::to_value(&read).expect("serialize backlog get")["op"],
        json!("backlog_get")
    );

    let mutation = ScopedRequest::BacklogCreate {
        command_key: "create".to_string(),
        title: "Title".to_string(),
        acceptance_condition: "Acceptance".to_string(),
        delivery_description: "Delivery".to_string(),
        kind: BacklogItemKind::Coworker,
    };
    assert!(mutation.is_backlog());
    assert!(mutation.is_mutating());
    assert!(!mutation.may_affect_scheduling());

    for injected in [
        json!({ "op": "backlog_get", "item_id": "itm_test", "binding_id": "bnd_other" }),
        json!({ "op": "backlog_get", "item_id": "itm_test", "now_ms": 1 }),
        json!({ "op": "backlog_list", "cursor": null, "limit": 1, "status": "completed" }),
    ] {
        assert!(
            serde_json::from_value::<ScopedRequest>(injected.clone()).is_err(),
            "ScopedRequest must reject caller-supplied scope/time/status fields: {injected}"
        );
    }
}

#[tokio::test]
async fn backlog_transport_is_scoped_idempotent_attested_and_persistent() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("backlog_state");
    let (handle, supervisor, scoped_a, scoped_b) = start_fixture(&state_dir).await;

    let status_before = bounded("status before backlog", scoped_a.status())
        .await
        .expect("status before backlog");
    assert_eq!(status_before.total_runs, 0);
    // A fresh binding has no unacknowledged important events yet.
    assert_eq!(status_before.wakeup.status, WakeupStatus::Idle);

    let created = bounded(
        "create backlog item",
        scoped_a.create_backlog_item(
            "create-main".to_string(),
            "First item".to_string(),
            "Acceptance one".to_string(),
            "Delivery one".to_string(),
            BacklogItemKind::Coworker,
        ),
    )
    .await
    .expect("create coworker backlog item");
    let created_json = value_of(&created);
    let item_id = item_id_of(&created_json);
    let revision_one = revision_of(&created_json);
    assert_eq!(created_json["kind"], json!("coworker"));
    assert_eq!(created_json["title"], json!("First item"));

    // Same command key and payload is an atomic replay, not a second item or revision.
    let replayed = bounded(
        "replay create backlog item",
        scoped_a.create_backlog_item(
            "create-main".to_string(),
            "First item".to_string(),
            "Acceptance one".to_string(),
            "Delivery one".to_string(),
            BacklogItemKind::Coworker,
        ),
    )
    .await
    .expect("replay coworker backlog item");
    let replayed_json = value_of(&replayed);
    assert_eq!(item_id_of(&replayed_json), item_id);
    assert_eq!(revision_of(&replayed_json), revision_one);
    let list_after_create = value_of(
        &bounded(
            "list after create",
            scoped_a.list_backlog_items(None, Some(10)),
        )
        .await
        .expect("list after create"),
    );
    assert_eq!(object_array(&list_after_create).len(), 1);

    let fetched = value_of(
        &bounded(
            "get backlog item",
            scoped_a.get_backlog_item(item_id.clone()),
        )
        .await
        .expect("get backlog item"),
    );
    assert_eq!(item_id_of(&fetched), item_id);
    let history_initial = value_of(
        &bounded(
            "initial backlog history",
            scoped_a.list_backlog_history(item_id.clone(), None, Some(20)),
        )
        .await
        .expect("initial backlog history"),
    );
    assert!(!object_array(&history_initial).is_empty());

    // Stale edits are rejected without changing the item.
    let _stale_error = match bounded(
        "stale backlog edit",
        scoped_a.edit_backlog_item(
            "edit-stale".to_string(),
            item_id.clone(),
            revision_one + 9,
            Some("Stale title".to_string()),
            None,
            None,
        ),
    )
    .await
    {
        Ok(_) => panic!("stale backlog edit must fail"),
        Err(error) => error,
    };

    let edited = value_of(
        &bounded(
            "edit backlog item",
            scoped_a.edit_backlog_item(
                "edit-main".to_string(),
                item_id.clone(),
                revision_one,
                Some("First item edited".to_string()),
                None,
                Some("Delivery edited".to_string()),
            ),
        )
        .await
        .expect("edit backlog item"),
    );
    let revision_two = revision_of(&edited);
    assert!(revision_two > revision_one);
    assert_eq!(edited["title"], json!("First item edited"));

    // Replaying the same edit key/payload returns the recorded result even with the old revision.
    let edit_replay = value_of(
        &bounded(
            "replay backlog edit",
            scoped_a.edit_backlog_item(
                "edit-main".to_string(),
                item_id.clone(),
                revision_one,
                Some("First item edited".to_string()),
                None,
                Some("Delivery edited".to_string()),
            ),
        )
        .await
        .expect("replay backlog edit"),
    );
    assert_eq!(revision_of(&edit_replay), revision_two);

    let conflict_error = match bounded(
        "changed-payload backlog edit",
        scoped_a.edit_backlog_item(
            "edit-main".to_string(),
            item_id.clone(),
            revision_one,
            Some("Changed payload".to_string()),
            None,
            None,
        ),
    )
    .await
    {
        Ok(_) => panic!("changed payload under the same command key must fail"),
        Err(error) => error,
    };
    assert_eq!(
        conflict_error.to_api_error().code,
        ErrorCode::IdempotencyConflict
    );
    let history_after_edit = value_of(
        &bounded(
            "backlog history after edit",
            scoped_a.list_backlog_history(item_id.clone(), None, Some(20)),
        )
        .await
        .expect("backlog history after edit"),
    );
    assert!(object_array(&history_after_edit).len() > object_array(&history_initial).len());

    // Worker items are never Coworker-startable or Coworker-completable.
    let worker = value_of(
        &bounded(
            "create worker backlog item",
            scoped_a.create_backlog_item(
                "create-worker".to_string(),
                "Worker item".to_string(),
                "Worker acceptance".to_string(),
                "Worker delivery".to_string(),
                BacklogItemKind::Worker,
            ),
        )
        .await
        .expect("create worker backlog item"),
    );
    let worker_id = item_id_of(&worker);
    let worker_revision = revision_of(&worker);
    let _worker_start_error = match bounded(
        "start worker backlog item",
        scoped_a.start_backlog_item(
            "worker-start".to_string(),
            worker_id.clone(),
            worker_revision,
        ),
    )
    .await
    {
        Ok(_) => panic!("Worker backlog start must be rejected"),
        Err(error) => error,
    };
    let _worker_complete_error = match bounded(
        "complete worker backlog item",
        scoped_a.complete_backlog_item(
            "worker-complete".to_string(),
            worker_id.clone(),
            worker_revision,
            "Worker note".to_string(),
            "Worker evidence".to_string(),
        ),
    )
    .await
    {
        Ok(_) => panic!("Worker backlog completion must be rejected"),
        Err(error) => error,
    };
    let worker_after = value_of(
        &bounded(
            "get worker backlog item after refusal",
            scoped_a.get_backlog_item(worker_id.clone()),
        )
        .await
        .expect("get worker backlog item after refusal"),
    );
    assert_eq!(revision_of(&worker_after), worker_revision);
    assert_eq!(worker_after["kind"], json!("worker"));

    let started = value_of(
        &bounded(
            "start coworker backlog item",
            scoped_a.start_backlog_item(
                "coworker-start".to_string(),
                item_id.clone(),
                revision_two,
            ),
        )
        .await
        .expect("start coworker backlog item"),
    );
    let revision_three = revision_of(&started);
    assert!(revision_three > revision_two);
    let completed = value_of(
        &bounded(
            "complete coworker backlog item",
            scoped_a.complete_backlog_item(
                "coworker-complete".to_string(),
                item_id.clone(),
                revision_three,
                "Coworker verified note".to_string(),
                "evidence:unit".to_string(),
            ),
        )
        .await
        .expect("complete coworker backlog item"),
    );
    assert!(revision_of(&completed) > revision_three);
    assert!(json_contains_text(&completed, "Coworker verified note"));
    assert!(json_contains_text(&completed, "evidence:unit"));
    let history_after_complete = value_of(
        &bounded(
            "backlog history after completion",
            scoped_a.list_backlog_history(item_id.clone(), None, Some(20)),
        )
        .await
        .expect("backlog history after completion"),
    );
    assert!(object_array(&history_after_complete).len() > object_array(&history_after_edit).len());
    let history_page_one = value_of(
        &bounded(
            "bounded backlog history page one",
            scoped_a.list_backlog_history(item_id.clone(), None, Some(1)),
        )
        .await
        .expect("bounded backlog history page one"),
    );
    let history_cursor = cursor_of(&history_page_one).expect("bounded backlog history cursor");

    let blocked_item = value_of(
        &bounded(
            "create blocked backlog item",
            scoped_a.create_backlog_item(
                "create-blocked".to_string(),
                "Blocked item".to_string(),
                "Blocked acceptance".to_string(),
                "Blocked delivery".to_string(),
                BacklogItemKind::Coworker,
            ),
        )
        .await
        .expect("create blocked backlog item"),
    );
    let blocked_item_id = item_id_of(&blocked_item);
    let blocked = value_of(
        &bounded(
            "block backlog item",
            scoped_a.block_backlog_item(
                "block-main".to_string(),
                blocked_item_id,
                revision_of(&blocked_item),
                "blocked by dependency".to_string(),
            ),
        )
        .await
        .expect("block backlog item"),
    );
    assert!(revision_of(&blocked) > revision_of(&blocked_item));

    for index in 0..3 {
        bounded(
            "create paged backlog item",
            scoped_a.create_backlog_item(
                format!("create-page-{index}"),
                format!("Paged item {index}"),
                "Paged acceptance".to_string(),
                "Paged delivery".to_string(),
                BacklogItemKind::Coworker,
            ),
        )
        .await
        .expect("create paged backlog item");
    }
    let page_one = value_of(
        &bounded(
            "bounded backlog page one",
            scoped_a.list_backlog_items(None, Some(2)),
        )
        .await
        .expect("bounded backlog page one"),
    );
    assert_eq!(object_array(&page_one).len(), 2);
    let cursor = cursor_of(&page_one).expect("bounded backlog page cursor");
    let page_two = value_of(
        &bounded(
            "bounded backlog page two",
            scoped_a.list_backlog_items(Some(cursor.clone()), Some(2)),
        )
        .await
        .expect("bounded backlog page two"),
    );
    assert!(!object_array(&page_two).is_empty());

    // Cross-chat item, history, and cursor access is denied.
    let _cross_get_error = match bounded(
        "cross-chat backlog get",
        scoped_b.get_backlog_item(item_id.clone()),
    )
    .await
    {
        Ok(_) => panic!("cross-chat backlog get must fail"),
        Err(error) => error,
    };
    let _cross_history_error = match bounded(
        "cross-chat backlog history",
        scoped_b.list_backlog_history(item_id.clone(), Some(history_cursor), Some(1)),
    )
    .await
    {
        Ok(_) => panic!("cross-chat backlog history must fail"),
        Err(error) => error,
    };
    let _cross_cursor_error = match bounded(
        "cross-chat backlog cursor",
        scoped_b.list_backlog_items(Some(cursor), Some(2)),
    )
    .await
    {
        Ok(_) => panic!("cross-chat backlog cursor must fail"),
        Err(error) => error,
    };

    let binding_b_empty = value_of(
        &bounded(
            "binding B empty backlog list",
            scoped_b.list_backlog_items(None, Some(10)),
        )
        .await
        .expect("binding B empty backlog list"),
    );
    assert!(object_array(&binding_b_empty).is_empty());
    let binding_b_item = value_of(
        &bounded(
            "binding B backlog create",
            scoped_b.create_backlog_item(
                "create-b".to_string(),
                "Chat B item".to_string(),
                "Chat B acceptance".to_string(),
                "Chat B delivery".to_string(),
                BacklogItemKind::Coworker,
            ),
        )
        .await
        .expect("binding B backlog create"),
    );
    let binding_b_item_id = item_id_of(&binding_b_item);
    assert_ne!(binding_b_item_id, item_id);
    let binding_a_page = value_of(
        &bounded(
            "binding A full backlog list",
            scoped_a.list_backlog_items(None, Some(50)),
        )
        .await
        .expect("binding A full backlog list"),
    );
    assert!(object_array(&binding_a_page)
        .iter()
        .all(|item| item_id_of(item) != binding_b_item_id));

    // Ordinary backlog work creates no runs, events, wake-ups, or execution scheduling.
    let status_after = bounded("status after backlog", scoped_a.status())
        .await
        .expect("status after backlog");
    assert_eq!(status_after.total_runs, 0);
    assert_eq!(status_after.active_runs, 0);
    assert!(status_after.recent_runs.is_empty());
    assert_eq!(status_after.binding.delivered_seq, 0);
    assert_eq!(status_after.binding.acked_seq, 0);
    assert_eq!(status_after.wakeup.status, status_before.wakeup.status);
    let runs_after = bounded(
        "list runs after backlog",
        scoped_a.list_runs(None, Some(10)),
    )
    .await
    .expect("list runs after backlog");
    assert!(runs_after.runs.is_empty());
    let activity_after = bounded(
        "list activity after backlog",
        scoped_a.list_activity(None, None, Some(20), None, None),
    )
    .await
    .expect("list activity after backlog");
    assert!(activity_after.events.is_empty());

    // Restart persistence and reconnect.
    handle.shutdown().await;
    let reopened_handle = bounded(
        "restart supervisor",
        Supervisor::start(fast_supervisor_config(&state_dir)),
    )
    .await
    .expect("restart backlog supervisor");
    let reopened_client =
        SupervisorClient::from_state_dir(&state_dir).expect("reopened supervisor client");
    let reopened_scoped_a = reopened_client
        .scoped(scoped_a.binding_id())
        .expect("reopened scoped binding A");
    let reopened_item = value_of(
        &bounded(
            "reopened backlog get",
            reopened_scoped_a.get_backlog_item(item_id.clone()),
        )
        .await
        .expect("reopened backlog get"),
    );
    assert_eq!(item_id_of(&reopened_item), item_id);
    assert!(json_contains_text(&reopened_item, "First item edited"));
    let reopened_list = value_of(
        &bounded(
            "reopened backlog list",
            reopened_scoped_a.list_backlog_items(None, Some(50)),
        )
        .await
        .expect("reopened backlog list"),
    );
    assert!(object_array(&reopened_list).len() >= 6);
    drop(supervisor);
    reopened_handle.shutdown().await;
}

#[tokio::test]
async fn backlog_mcp_handlers_schemas_and_protocol_injection_are_bounded() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("backlog_mcp_state");
    let (handle, supervisor, scoped_a, _scoped_b) = start_fixture(&state_dir).await;
    let binding_id = scoped_a.binding_id().to_string();
    let server = ThreadlaneMcpServer::new(scoped_a.clone());

    let tools_json = value_of(&server.list_tools());
    let tools = tools_json.as_array().expect("tools array");
    assert_eq!(tools.len(), 24);
    let backlog_tools = [
        "backlog_create",
        "backlog_get",
        "backlog_list",
        "backlog_edit",
        "backlog_block",
        "backlog_start",
        "backlog_complete",
        "backlog_history",
    ];
    for tool_name in backlog_tools {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == json!(tool_name))
            .unwrap_or_else(|| panic!("{tool_name} missing from tools/list"));
        let annotations = &tool["annotations"];
        assert!(annotations.is_object(), "{tool_name} annotations missing");
        let read_only = matches!(
            tool_name,
            "backlog_get" | "backlog_list" | "backlog_history"
        );
        assert_eq!(annotations["readOnlyHint"], json!(read_only));
        assert_eq!(annotations["idempotentHint"], json!(true));
        assert_eq!(annotations["openWorldHint"], json!(false));
        let schema = &tool["inputSchema"];
        assert_eq!(schema["additionalProperties"], json!(false));
        let properties = schema["properties"].as_object().expect("schema properties");
        for forbidden in ["binding_id", "thread_uuid", "thread_id", "now_ms", "status"] {
            assert!(
                !properties.contains_key(forbidden),
                "{tool_name} must not accept caller-supplied {forbidden}"
            );
        }
    }
    let create_schema = tools
        .iter()
        .find(|tool| tool["name"] == json!("backlog_create"))
        .expect("backlog_create schema");
    for expected in [
        "command_key",
        "title",
        "acceptance_condition",
        "delivery_description",
        "kind",
    ] {
        assert!(create_schema["inputSchema"]["properties"]
            .as_object()
            .expect("create properties")
            .contains_key(expected));
    }

    let created = bounded(
        "mcp backlog_create",
        server.backlog_create(Parameters(BacklogCreateParams {
            command_key: "mcp-create".to_string(),
            title: "MCP item".to_string(),
            acceptance_condition: "MCP acceptance".to_string(),
            delivery_description: "MCP delivery".to_string(),
            kind: BacklogKindParam::Coworker,
        })),
    )
    .await
    .expect("MCP backlog_create");
    let created_structured = created
        .structured_content
        .expect("created structured content");
    assert_eq!(created_structured["tool"], json!("backlog_create"));
    assert_eq!(created_structured["untrusted_data"], json!(true));
    assert_eq!(created_structured["demo_only"], json!(false));
    assert_eq!(created_structured["binding_id"], json!(binding_id));
    let item_id = item_id_of(&created_structured["data"]);
    let revision_one = revision_of(&created_structured["data"]);

    let fetched = bounded(
        "mcp backlog_get",
        server.backlog_get(Parameters(BacklogGetParams {
            item_id: item_id.clone(),
        })),
    )
    .await
    .expect("MCP backlog_get");
    assert_eq!(
        item_id_of(&fetched.structured_content.expect("get structured")["data"]),
        item_id
    );

    let listed = bounded(
        "mcp backlog_list",
        server.backlog_list(Parameters(BacklogListParams {
            cursor: None,
            limit: Some(2),
        })),
    )
    .await
    .expect("MCP backlog_list");
    assert!(
        object_array(&listed.structured_content.expect("list structured")["data"])
            .iter()
            .any(|item| item_id_of(item) == item_id)
    );

    let edited = bounded(
        "mcp backlog_edit",
        server.backlog_edit(Parameters(BacklogEditParams {
            command_key: "mcp-edit".to_string(),
            item_id: item_id.clone(),
            expected_revision: revision_one,
            title: Some("MCP item edited".to_string()),
            acceptance_condition: None,
            delivery_description: None,
        })),
    )
    .await
    .expect("MCP backlog_edit");
    let edited_data = edited.structured_content.expect("edit structured")["data"].clone();
    assert_eq!(edited_data["title"], json!("MCP item edited"));
    let revision_two = revision_of(&edited_data);

    let block_item = bounded(
        "mcp backlog_create block target",
        server.backlog_create(Parameters(BacklogCreateParams {
            command_key: "mcp-create-block".to_string(),
            title: "MCP blocked item".to_string(),
            acceptance_condition: "Blocked acceptance".to_string(),
            delivery_description: "Blocked delivery".to_string(),
            kind: BacklogKindParam::Coworker,
        })),
    )
    .await
    .expect("MCP block target create");
    let block_data = block_item
        .structured_content
        .expect("block target structured")["data"]
        .clone();
    let blocked = bounded(
        "mcp backlog_block",
        server.backlog_block(Parameters(BacklogBlockParams {
            command_key: "mcp-block".to_string(),
            item_id: item_id_of(&block_data),
            expected_revision: revision_of(&block_data),
            reason: "blocked in MCP test".to_string(),
        })),
    )
    .await
    .expect("MCP backlog_block");
    let blocked_data = blocked.structured_content.expect("block structured")["data"].clone();
    assert!(revision_of(&blocked_data) > revision_of(&block_data));

    let worker_item = bounded(
        "mcp backlog_create worker",
        server.backlog_create(Parameters(BacklogCreateParams {
            command_key: "mcp-create-worker".to_string(),
            title: "MCP worker item".to_string(),
            acceptance_condition: "Worker acceptance".to_string(),
            delivery_description: "Worker delivery".to_string(),
            kind: BacklogKindParam::Worker,
        })),
    )
    .await
    .expect("MCP worker create");
    let worker_data = worker_item.structured_content.expect("worker structured")["data"].clone();
    let worker_id = item_id_of(&worker_data);
    let worker_revision = revision_of(&worker_data);
    let worker_start_error = match bounded(
        "mcp backlog_start worker refusal",
        server.backlog_start(Parameters(BacklogStartParams {
            command_key: "mcp-worker-start".to_string(),
            item_id: worker_id.clone(),
            expected_revision: worker_revision,
        })),
    )
    .await
    {
        Ok(_) => panic!("MCP Worker backlog start must be rejected"),
        Err(error) => error,
    };
    assert_eq!(worker_start_error.is_error, Some(true));
    let worker_complete_error = match bounded(
        "mcp backlog_complete worker refusal",
        server.backlog_complete(Parameters(BacklogCompleteParams {
            command_key: "mcp-worker-complete".to_string(),
            item_id: worker_id,
            expected_revision: worker_revision,
            note: "Worker note".to_string(),
            evidence: "Worker evidence".to_string(),
        })),
    )
    .await
    {
        Ok(_) => panic!("MCP Worker backlog completion must be rejected"),
        Err(error) => error,
    };
    assert_eq!(worker_complete_error.is_error, Some(true));

    let started = bounded(
        "mcp backlog_start",
        server.backlog_start(Parameters(BacklogStartParams {
            command_key: "mcp-start".to_string(),
            item_id: item_id.clone(),
            expected_revision: revision_two,
        })),
    )
    .await
    .expect("MCP backlog_start");
    let started_data = started.structured_content.expect("start structured")["data"].clone();
    let revision_three = revision_of(&started_data);
    let completed = bounded(
        "mcp backlog_complete",
        server.backlog_complete(Parameters(BacklogCompleteParams {
            command_key: "mcp-complete".to_string(),
            item_id: item_id.clone(),
            expected_revision: revision_three,
            note: "MCP coworker note".to_string(),
            evidence: "MCP evidence".to_string(),
        })),
    )
    .await
    .expect("MCP backlog_complete");
    let completed_data = completed.structured_content.expect("complete structured")["data"].clone();
    assert!(json_contains_text(&completed_data, "MCP coworker note"));
    assert!(json_contains_text(&completed_data, "MCP evidence"));

    let history = bounded(
        "mcp backlog_history",
        server.backlog_history(Parameters(BacklogHistoryParams {
            item_id: item_id.clone(),
            cursor: None,
            limit: Some(20),
        })),
    )
    .await
    .expect("MCP backlog_history");
    assert!(
        !object_array(&history.structured_content.expect("history structured")["data"]).is_empty()
    );

    // Unknown caller-supplied authority/scope/time fields are rejected by the real HTTP protocol.
    let descriptor = read_and_validate_descriptor(&state_dir).expect("runtime descriptor");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("local HTTP client");
    let url = format!(
        "{}/v1/bindings/{binding_id}/command",
        descriptor.base_url().expect("descriptor base URL")
    );
    let base_body = json!({
        "op": "backlog_create",
        "command_key": "http-injection",
        "title": "Injected",
        "acceptance_condition": "Injected acceptance",
        "delivery_description": "Injected delivery",
        "kind": "coworker"
    });
    for (field, injected) in [
        ("binding_id", json!("bnd_other")),
        ("thread_uuid", json!(THREAD_UUID_B)),
        ("now_ms", json!(123)),
        ("status", json!("completed")),
    ] {
        let mut body = base_body.clone();
        body[field] = injected;
        let response = bounded(
            "HTTP injection response",
            http.post(&url)
                .bearer_auth(descriptor.token.as_str())
                .json(&body)
                .send(),
        )
        .await
        .expect("HTTP injection request");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "HTTP must reject injected {field}"
        );
        let error: Value = bounded("HTTP injection body", response.json())
            .await
            .expect("HTTP injection error body");
        assert_eq!(error["code"], json!("invalid_request"));
    }

    // The real stdio MCP protocol also rejects unknown scope/authority fields before dispatch.
    let mut child = spawn_mcp_child(&state_dir, &binding_id);
    let mut stdin = child.stdin.take().expect("mcp stdin");
    let stdout = child.stdout.take().expect("mcp stdout");
    let mut reader = BufReader::new(stdout);
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "backlog-transport-test", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let _ = read_json_rpc_response(&mut reader, 1).await;
    send_json_rpc(
        &mut stdin,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    send_json_rpc(
        &mut stdin,
        &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} }),
    )
    .await;
    let list_response = read_json_rpc_response(&mut reader, 2).await;
    assert_eq!(
        list_response["result"]["tools"]
            .as_array()
            .expect("MCP tools array")
            .len(),
        24
    );
    send_json_rpc(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "backlog_create",
                "arguments": {
                    "command_key": "mcp-injection",
                    "title": "Injected",
                    "acceptance_condition": "Injected acceptance",
                    "delivery_description": "Injected delivery",
                    "kind": "coworker",
                    "binding_id": "bnd_other",
                    "now_ms": 123
                }
            }
        }),
    )
    .await;
    let injection_response = read_json_rpc_response(&mut reader, 3).await;
    assert_eq!(
        injection_response["result"]["isError"],
        json!(true),
        "MCP must reject injected fields: {injection_response}"
    );
    let injection_text = injection_response["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP injection text");
    assert!(
        injection_text.contains("failed to deserialize parameters")
            && injection_text.contains("unknown field"),
        "unexpected MCP injection error: {injection_text}"
    );
    drop(stdin);
    cleanup_child(&mut child).await;

    drop(supervisor);
    handle.shutdown().await;
}

#[tokio::test]
async fn stopping_host_rejects_backlog_mutations_but_serves_reads() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("backlog_stopping_state");
    let control = HostControlConfig {
        approval_id: "apr_0123456789abcdef0123456789abcdef".to_string(),
        subject_digest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_string(),
        control_binding_id: "bnd_control_backlog".to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        session_binding_id: "bnd_session_backlog".to_string(),
        instance_id: "hst_0123456789abcdef0123456789abcdef".to_string(),
    };
    let handle = bounded(
        "start host-control supervisor",
        Supervisor::start_with_host_control(fast_supervisor_config(&state_dir), control.clone()),
    )
    .await
    .expect("start host-control supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
    let binding = bounded(
        "create stopping binding",
        client.create_binding(THREAD_UUID_A, "Backlog Stopping Chat"),
    )
    .await
    .expect("create stopping binding");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped stopping binding");
    let created = bounded(
        "create pre-stop backlog item",
        scoped.create_backlog_item(
            "pre-stop-create".to_string(),
            "Pre-stop item".to_string(),
            "Pre-stop acceptance".to_string(),
            "Pre-stop delivery".to_string(),
            BacklogItemKind::Coworker,
        ),
    )
    .await
    .expect("create pre-stop backlog item");
    let created_json = value_of(&created);
    let item_id = item_id_of(&created_json);
    let started = value_of(
        &bounded(
            "start pre-stop backlog item",
            scoped.start_backlog_item(
                "pre-stop-start".to_string(),
                item_id.clone(),
                revision_of(&created_json),
            ),
        )
        .await
        .expect("start pre-stop backlog item"),
    );
    let completed = value_of(
        &bounded(
            "complete pre-stop backlog item",
            scoped.complete_backlog_item(
                "pre-stop-complete".to_string(),
                item_id.clone(),
                revision_of(&started),
                "Pre-stop note".to_string(),
                "Pre-stop evidence".to_string(),
            ),
        )
        .await
        .expect("complete pre-stop backlog item"),
    );
    let revision = revision_of(&completed);

    let scope = HostControlOwnerScope {
        approval_id: control.approval_id.clone(),
        subject_digest: control.subject_digest.clone(),
        control_binding_id: control.control_binding_id.clone(),
        thread_uuid: control.thread_uuid.clone(),
        session_binding_id: control.session_binding_id.clone(),
        expected_instance_id: control.instance_id.clone(),
    };
    let stopped = bounded("stop idle host", client.stop_idle_host(&scope))
        .await
        .expect("stop idle host");
    assert_eq!(stopped.state, HostControlRuntimeState::Stopping);

    let create_error = match bounded(
        "stopped backlog create",
        scoped.create_backlog_item(
            "post-stop-create".to_string(),
            "Post-stop item".to_string(),
            "Post-stop acceptance".to_string(),
            "Post-stop delivery".to_string(),
            BacklogItemKind::Coworker,
        ),
    )
    .await
    {
        Ok(_) => panic!("backlog create must be rejected while stopping"),
        Err(error) => error,
    };
    assert_eq!(create_error.to_api_error().code, ErrorCode::HostStopping);
    let start_error = match bounded(
        "stopped backlog start",
        scoped.start_backlog_item("post-stop-start".to_string(), item_id.clone(), revision),
    )
    .await
    {
        Ok(_) => panic!("backlog start must be rejected while stopping"),
        Err(error) => error,
    };
    assert_eq!(start_error.to_api_error().code, ErrorCode::HostStopping);

    let read_item = value_of(
        &bounded(
            "stopped backlog get",
            scoped.get_backlog_item(item_id.clone()),
        )
        .await
        .expect("stopped backlog get"),
    );
    assert_eq!(item_id_of(&read_item), item_id);
    bounded(
        "stopped backlog list",
        scoped.list_backlog_items(None, Some(10)),
    )
    .await
    .expect("stopped backlog list");
    bounded(
        "stopped backlog history",
        scoped.list_backlog_history(item_id, None, Some(10)),
    )
    .await
    .expect("stopped backlog history");

    handle.shutdown().await;
}

fn spawn_mcp_child(state_dir: &Path, binding_id: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_threadlane"))
        .arg("mcp")
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--binding")
        .arg(binding_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn threadlane mcp")
}

async fn send_json_rpc<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, message: &Value) {
    let line = serde_json::to_string(message).expect("serialize JSON-RPC");
    writer
        .write_all(line.as_bytes())
        .await
        .expect("write JSON-RPC");
    writer.write_all(b"\n").await.expect("write newline");
    writer.flush().await.expect("flush JSON-RPC");
}

async fn read_json_rpc_response<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    expected_id: u64,
) -> Value {
    let deadline = tokio::time::Instant::now() + FIXTURE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for JSON-RPC response id {expected_id}"
        );
        let mut line = String::new();
        let bytes = tokio::time::timeout(remaining, reader.read_line(&mut line))
            .await
            .expect("JSON-RPC read timeout")
            .expect("read JSON-RPC line");
        if bytes == 0 {
            panic!("MCP child closed stdout before response id {expected_id}");
        }
        let Ok(response) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if response.get("id").and_then(Value::as_u64) == Some(expected_id) {
            return response;
        }
    }
}

async fn cleanup_child(child: &mut Child) {
    if tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}
