use super::*;
use serde_json::{json, Value};
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_core::observation::{read_journal_records_readonly, ObservationRecordPhase};
use threadlane_native::coworker_driver::{
    CoworkerDerivedCancellationBoundary, CoworkerDriverEnsureDisposition, CoworkerDriverOwnerState,
    CoworkerDriverPipelineBoundary,
};
use threadlane_native::coworker_workflow::CoworkerJobLockState;
use threadlane_native::{
    ExecutionPreviewResponse, ExecutionReadinessStatus, ExecutionStatusResponse,
    OperatorEnrollRequest, PairedMcpServer, PairingStore, StartExecutionResponse,
    StopExecutionResponse,
};

fn write_gated_fake_agy_script_for_mcp(
    script_path: &Path,
    coder_count_file: &Path,
    planning_count_file: &Path,
    prompt_file: &Path,
    plan_response_file: &Path,
    coder_started_file: &Path,
    gate_release_file: &Path,
) {
    let script = format!(
        r#"#!/bin/sh
set -eu
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"
CODER_STARTED_FILE="{coder_started_file}"
GATE_RELEASE_FILE="{gate_release_file}"

MODEL=""
EFFORT=""
AGY_MODE=""
PROMPT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --effort)
            EFFORT="$2"
            shift 2
            ;;
        --mode)
            AGY_MODE="$2"
            shift 2
            ;;
        --print|--prompt|-p)
            PROMPT="$2"
            shift 2
            ;;
        *)
            shift 1
            ;;
    esac
done

if [ "$AGY_MODE" = "plan" ]; then
    pc=0
    if [ -f "$PLANNING_COUNT_FILE" ]; then
        pc=$(cat "$PLANNING_COUNT_FILE")
    fi
    pc=$((pc + 1))
    printf "%s" "$pc" > "$PLANNING_COUNT_FILE"
    printf '{{"event":"init","init":{{"session_id":"sess-agy-plan-%s","model":"%s"}}}}\n' "$pc" "$MODEL"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$PLAN_RESPONSE_FILE" | tr '\n' ' ')
    printf '{{"event":"result","result":{{"status":"SUCCESS","response":"%s"}}}}\n' "$esc"
    exit 0
fi

c=0
if [ -f "$CODER_COUNT_FILE" ]; then
    c=$(cat "$CODER_COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$CODER_COUNT_FILE"

printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{{"type":"init","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
printf "started\n" > "$CODER_STARTED_FILE"

while [ ! -f "$GATE_RELEASE_FILE" ]; do
    sleep 0.05
done

if [ -d "src" ]; then
    case "$PROMPT" in
        *CTRL_TASK_A*)
            printf "pub fn a() -> u32 {{ 100 }}\n" > "src/a.rs"
            ;;
        *)
            printf "pub fn a() -> u32 {{ 42 }}\n" > "src/a.rs"
            ;;
    esac
fi

printf '{{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
        coder_count_file = coder_count_file.display(),
        planning_count_file = planning_count_file.display(),
        prompt_file = prompt_file.display(),
        plan_response_file = plan_response_file.display(),
        coder_started_file = coder_started_file.display(),
        gate_release_file = gate_release_file.display(),
    );
    let tmp_script = script_path.with_extension("mcp.tmp");
    write_executable_script(&tmp_script, &script);
    fs::rename(&tmp_script, script_path).expect("atomically replace fake-agy script");
}

fn create_enrolled_mcp(
    state_dir: &Path,
    pairing_dir: &Path,
    fixture: &ControllerTestFixture,
    binding_id: &str,
    thread_uuid: &str,
) -> PairedMcpServer {
    let pairing_store = PairingStore::open(pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let mcp = PairedMcpServer::new(state_dir, pairing_store.clone())
        .expect("create PairedMcpServer")
        .with_workflow_dirs(&fixture.registry_dir, &fixture.plans_dir)
        .expect("with_workflow_dirs")
        .with_host_dirs(&fixture.authority_dir, &fixture.sessions_dir)
        .expect("with_host_dirs")
        .with_sibling_anchor_exe(PathBuf::from(env!("CARGO_BIN_EXE_threadlane-flow")));
    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: mcp.connection_nonce().to_string(),
                epoch_id: mcp.epoch_id().to_string(),
                binding_id: binding_id.to_string(),
                thread_id: thread_uuid.to_string(),
                project_id: None,
            },
        )
        .expect("enroll PairedMcpServer");
    mcp
}

async fn invoke_mcp_ok<T: serde::de::DeserializeOwned>(
    mcp: &PairedMcpServer,
    tool_name: &str,
    args: Value,
) -> T {
    let res = mcp
        .invoke_tool_json(tool_name, args, None)
        .await
        .unwrap_or_else(|err| {
            panic!("expected MCP tool {tool_name} to succeed, got error: {err:?}")
        });
    assert_ne!(res.is_error, Some(true));
    let structured = res.structured_content.expect("structured_content");
    assert_eq!(structured["untrusted_data"], json!(true));
    assert_eq!(structured["tool"], json!(tool_name));
    serde_json::from_value(structured["data"].clone()).expect("deserialize tool response data")
}

fn execution_gateway_guard_args(tool_name: &str, job_id: &str) -> Value {
    if tool_name == "start_execution" {
        json!({
            "job_id": job_id,
            "expected_envelope_digest": "a".repeat(64),
        })
    } else {
        json!({ "job_id": job_id })
    }
}

async fn invoke_mcp_err(mcp: &PairedMcpServer, tool_name: &str, args: Value) -> (String, String) {
    let err = mcp
        .invoke_tool_json(tool_name, args, None)
        .await
        .expect_err("expected MCP tool call to fail");
    let structured = err.structured_content.expect("error structured_content");
    let code = structured["error"]["code"]
        .as_str()
        .expect("error.code")
        .to_string();
    let message = structured["error"]["message"]
        .as_str()
        .expect("error.message")
        .to_string();
    (code, message)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn execution_gateway_mcp_schemas_enrollment_scope_isolation_unapproved_refusal_and_pure_preview(
) {
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("mcp-state");
    let pairing_dir = root.join("mcp-pairing");
    for dir in [&state_dir, &pairing_dir] {
        fs::create_dir_all(dir).expect("create dir");
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    }

    // 1. Verify all 4 execution tools are advertised (40 tools total) with strict schemas and annotations.
    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let mcp_unbound =
        PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create unbound mcp");
    let tools = mcp_unbound.list_tools();
    assert_eq!(tools.len(), 40);

    for (
        tool_name,
        expected_ro,
        expected_destructive,
        expected_idempotent,
        expected_props,
        expected_required,
    ) in [
        (
            "preview_execution",
            true,
            false,
            true,
            &["job_id"][..],
            &["job_id"][..],
        ),
        (
            "start_execution",
            false,
            false,
            true,
            &["expected_envelope_digest", "job_id"][..],
            &["expected_envelope_digest", "job_id"][..],
        ),
        (
            "execution_status",
            true,
            false,
            true,
            &["job_id"][..],
            &["job_id"][..],
        ),
        (
            "stop_execution",
            false,
            true,
            true,
            &["job_id", "reason"][..],
            &["job_id"][..],
        ),
    ] {
        let tool = tools
            .iter()
            .find(|t| t.name == tool_name)
            .unwrap_or_else(|| panic!("missing tool {tool_name}"));
        let ann = tool.annotations.as_ref().expect("tool annotations");
        assert_eq!(ann.read_only_hint, Some(expected_ro));
        assert_eq!(ann.destructive_hint, Some(expected_destructive));
        assert_eq!(ann.idempotent_hint, Some(expected_idempotent));
        assert_eq!(ann.open_world_hint, Some(false));

        let schema_val = serde_json::to_value(&tool.input_schema).expect("serialize input_schema");
        assert_eq!(
            schema_val.get("additionalProperties"),
            Some(&json!(false)),
            "tool {tool_name} must enforce deny_unknown_fields"
        );
        let props_obj = schema_val["properties"]
            .as_object()
            .expect("schema properties object");
        let mut actual_props: Vec<&str> = props_obj.keys().map(String::as_str).collect();
        actual_props.sort_unstable();
        assert_eq!(actual_props, expected_props);

        let req_arr = schema_val["required"]
            .as_array()
            .expect("schema required array");
        let mut actual_req: Vec<&str> = req_arr.iter().filter_map(Value::as_str).collect();
        actual_req.sort_unstable();
        assert_eq!(actual_req, expected_required);

        let (code, _) = invoke_mcp_err(
            &mcp_unbound,
            tool_name,
            execution_gateway_guard_args(tool_name, "job_1"),
        )
        .await;
        assert_eq!(code, "unbound_connection");
    }

    // 2. Enrolled server without workflow/host store configuration fails closed with `not_configured`.
    let operator_token = pairing_store.operator_token().expect("operator token");
    let mcp_no_stores =
        PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("mcp_no_stores");
    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: mcp_no_stores.connection_nonce().to_string(),
                epoch_id: mcp_no_stores.epoch_id().to_string(),
                binding_id: BINDING_A.to_string(),
                thread_id: THREAD_UUID_A.to_string(),
                project_id: None,
            },
        )
        .expect("enroll mcp_no_stores");
    for tool_name in [
        "preview_execution",
        "start_execution",
        "execution_status",
        "stop_execution",
    ] {
        let (code, _) = invoke_mcp_err(
            &mcp_no_stores,
            tool_name,
            execution_gateway_guard_args(tool_name, "job_1"),
        )
        .await;
        assert_eq!(code, "not_configured");
    }

    let mcp_workflow_only = PairedMcpServer::new(&state_dir, pairing_store.clone())
        .expect("mcp_workflow_only")
        .with_workflow_dirs(&fixture.registry_dir, &fixture.plans_dir)
        .expect("with_workflow_dirs");
    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: mcp_workflow_only.connection_nonce().to_string(),
                epoch_id: mcp_workflow_only.epoch_id().to_string(),
                binding_id: BINDING_A.to_string(),
                thread_id: THREAD_UUID_A.to_string(),
                project_id: None,
            },
        )
        .expect("enroll mcp_workflow_only");
    for tool_name in [
        "preview_execution",
        "start_execution",
        "execution_status",
        "stop_execution",
    ] {
        let (code, _) = invoke_mcp_err(
            &mcp_workflow_only,
            tool_name,
            execution_gateway_guard_args(tool_name, "job_1"),
        )
        .await;
        assert_eq!(code, "not_configured");
    }

    // 3. Configured enrolled server rejects caller-supplied identity/rebind keys and extra model/path fields.
    let mcp_a = create_enrolled_mcp(&state_dir, &pairing_dir, &fixture, BINDING_A, THREAD_UUID_A);
    let mcp_b = create_enrolled_mcp(&state_dir, &pairing_dir, &fixture, BINDING_B, THREAD_UUID_B);

    for forbidden in [
        "binding_id",
        "control_binding_id",
        "thread_id",
        "thread_uuid",
        "native_thread_uuid",
        "session_id",
        "connection_nonce",
        "epoch_id",
    ] {
        for tool_name in [
            "preview_execution",
            "start_execution",
            "execution_status",
            "stop_execution",
        ] {
            let mut obj = serde_json::Map::new();
            obj.insert("job_id".to_string(), json!("job_1"));
            obj.insert(forbidden.to_string(), json!("forged"));
            let (code, _) = invoke_mcp_err(&mcp_a, tool_name, Value::Object(obj)).await;
            assert_eq!(code, "rebind_rejected");
        }
    }

    for extra_field in [
        "model_id",
        "tool_id",
        "profile_id",
        "repository_path",
        "flow_binary_path",
        "state_dir",
        "plans_dir",
    ] {
        let (code, _) = invoke_mcp_err(
            &mcp_a,
            "start_execution",
            json!({
                "job_id": "job_1",
                "expected_envelope_digest": "a".repeat(64),
                extra_field: "forged"
            }),
        )
        .await;
        assert_eq!(code, "validation_failed");
    }

    // 4. Create an unapproved job in BINDING_A: preview reports `approved: false` / `needs_approval`,
    //    and `start_execution` / `execution_status` / `stop_execution` refuse before any launch or counter change.
    let unapproved_job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req_unapproved_job_1".to_string(),
            expected_ledger_revision: None,
            work_brief: sample_brief(&fixture, "brief-unapproved-1"),
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create unapproved job");

    let unapproved_preview: ExecutionPreviewResponse = invoke_mcp_ok(
        &mcp_a,
        "preview_execution",
        json!({ "job_id": unapproved_job.job_id }),
    )
    .await;
    assert!(!unapproved_preview.approved);
    assert_eq!(
        unapproved_preview.readiness,
        ExecutionReadinessStatus::NeedsApproval
    );
    assert!(!unapproved_preview.operator_attested);
    assert!(!unapproved_preview.native_per_call_identity_verified);
    assert!(!unapproved_preview.saved_profiles_grant_execution_authority);
    assert!(!unapproved_preview.cryptographic_human_identity_proven);
    assert_eq!(
        unapproved_preview.pipeline_boundary,
        CoworkerDriverPipelineBoundary::Phase1InitialDispatchAndReconcileOnly
    );
    assert!(unapproved_preview.envelope_digest.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 0);

    let (unapproved_start_code, unapproved_start_msg) = invoke_mcp_err(
        &mcp_a,
        "start_execution",
        json!({
            "job_id": unapproved_job.job_id,
            "expected_envelope_digest": "a".repeat(64)
        }),
    )
    .await;
    assert_eq!(unapproved_start_code, "unauthorized_operator");
    assert!(unapproved_start_msg.contains("needs_approval"));

    let (unapproved_status_code, unapproved_status_msg) = invoke_mcp_err(
        &mcp_a,
        "execution_status",
        json!({ "job_id": unapproved_job.job_id }),
    )
    .await;
    assert_eq!(unapproved_status_code, "unauthorized_operator");
    assert!(unapproved_status_msg.contains("needs_approval"));

    let (unapproved_stop_code, unapproved_stop_msg) = invoke_mcp_err(
        &mcp_a,
        "stop_execution",
        json!({ "job_id": unapproved_job.job_id }),
    )
    .await;
    assert_eq!(unapproved_stop_code, "unauthorized_operator");
    assert!(unapproved_stop_msg.contains("needs_approval"));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // 5. Create a planned, approved, and frozen job in BINDING_A and verify pure `preview_execution`,
    //    prompt/path redaction, mismatched digest refusal, and cross-binding isolation.
    let secret_prompt_marker = "SECRET_RAW_TASK_INSTRUCTION_DO_NOT_LEAK_9912";
    let tasks = vec![PlanTask {
        id: "task-preview-a".to_string(),
        title: "Preview Task A".to_string(),
        instruction: format!("CTRL_TASK_A {secret_prompt_marker} update src/a.rs"),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];

    let (approved_job, approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        BINDING_A,
        THREAD_UUID_A,
        "brief-exec-preview-1",
        "plan-exec-preview-1",
        tasks,
        BTreeMap::new(),
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let preview: ExecutionPreviewResponse = invoke_mcp_ok(
        &mcp_a,
        "preview_execution",
        json!({ "job_id": approved_job.job_id }),
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(preview.approved);
    assert_eq!(
        preview.readiness,
        ExecutionReadinessStatus::ApprovedAndFrozen
    );
    assert!(preview.operator_attested);
    assert!(!preview.native_per_call_identity_verified);
    assert!(!preview.saved_profiles_grant_execution_authority);
    assert!(!preview.cryptographic_human_identity_proven);
    assert_eq!(preview.binding_id, BINDING_A);
    assert_eq!(preview.thread_uuid, THREAD_UUID_A);
    assert_eq!(preview.job_id, approved_job.job_id);
    assert_eq!(preview.approval_id.as_deref(), Some(approval_id.as_str()));
    assert_eq!(preview.project_base, project_base(&fixture));
    assert_eq!(preview.max_parallel, Some(1));
    assert_eq!(preview.approved_write_paths, vec!["src/a.rs".to_string()]);
    assert_eq!(preview.tasks.len(), 1);
    assert_eq!(preview.tasks[0].task_id, "task-preview-a");
    assert_eq!(preview.tasks[0].title, "Preview Task A");
    assert_eq!(
        preview.tasks[0].selected_choice,
        headless_argon_choice(Difficulty::Hard)
    );
    assert_eq!(
        preview.reviewer_choice,
        Some(headless_argon_choice(Difficulty::Hard))
    );
    assert_eq!(
        preview.closure_choice,
        Some(headless_argon_choice(Difficulty::Extreme))
    );
    assert_eq!(preview.allowed_check_ids, vec!["cargo_test".to_string()]);
    assert_eq!(preview.check_commands.len(), 1);
    assert_eq!(preview.check_commands[0].check_id, "cargo_test");
    assert_eq!(preview.check_commands[0].executable_name, "fake-cargo");
    assert_eq!(preview.check_commands[0].argv, vec!["test".to_string()]);
    assert_eq!(
        preview.pipeline_boundary,
        CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
    );

    let serialized_preview = serde_json::to_string(&preview).expect("serialize preview");
    assert!(
        !serialized_preview.contains(secret_prompt_marker),
        "preview_execution must never leak raw task prompt instructions"
    );
    assert!(
        !serialized_preview.contains(&fixture.fake_cargo_exe.to_string_lossy().to_string()),
        "preview_execution must never leak full host check executable paths"
    );
    assert!(
        !serialized_preview.contains(&fixture.fake_agy_exe.to_string_lossy().to_string()),
        "preview_execution must never leak registry tool executable paths"
    );

    // Mismatched expected_envelope_digest must fail closed before spawning any process or touching provider counters.
    let (mismatch_code, mismatch_msg) = invoke_mcp_err(
        &mcp_a,
        "start_execution",
        json!({
            "job_id": approved_job.job_id,
            "expected_envelope_digest": "0".repeat(64)
        }),
    )
    .await;
    assert_eq!(mismatch_code, "validation_failed");
    assert!(mismatch_msg.contains("envelope_digest_mismatch"));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
    assert!(!fixture.sessions_dir.join(&approval_id).exists());

    // Cross-binding enrolled server `mcp_b` cannot preview, start, inspect, or stop `BINDING_A`'s job.
    let real_digest = preview
        .envelope_digest
        .clone()
        .expect("frozen envelope_digest");
    for (tool_name, payload) in [
        (
            "preview_execution",
            json!({ "job_id": approved_job.job_id }),
        ),
        (
            "start_execution",
            json!({
                "job_id": approved_job.job_id,
                "expected_envelope_digest": real_digest
            }),
        ),
        ("execution_status", json!({ "job_id": approved_job.job_id })),
        ("stop_execution", json!({ "job_id": approved_job.job_id })),
    ] {
        let (cross_code, _) = invoke_mcp_err(&mcp_b, tool_name, payload).await;
        assert_eq!(cross_code, "validation_failed");
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn execution_gateway_mcp_detached_start_duplicate_reuse_status_full_mode_and_stop() {
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("runtime");
    let pairing_dir = root.join("pairing");
    for dir in [&state_dir, &pairing_dir] {
        fs::create_dir_all(dir).expect("create runtime/pairing dir");
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    }

    let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
        .await
        .expect("start disposable supervisor");
    let supervisor_client =
        SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
    let binding = supervisor_client
        .create_binding(THREAD_UUID_A, "execution-gateway-mcp-test")
        .await
        .expect("create supervisor binding");
    let binding_id = binding.binding_id;
    let thread_uuid = binding.thread_uuid;

    let tasks = vec![PlanTask {
        id: "task-mcp-exec-a".to_string(),
        title: "Task MCP Exec A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs via MCP execution gateway".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];

    let (job, approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_id,
        &thread_uuid,
        "brief-mcp-exec-1",
        "plan-mcp-exec-1",
        tasks,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let coder_started_file = root.join("mcp_coder_started.flag");
    let gate_release_file = root.join("mcp_coder_gate_release.flag");
    write_gated_fake_agy_script_for_mcp(
        &fixture.fake_agy_exe,
        &fixture.agy_count_file,
        &fixture.agy_planning_count_file,
        &fixture.agy_prompt_file,
        &fixture.agy_plan_response_file,
        &coder_started_file,
        &gate_release_file,
    );

    let mcp = create_enrolled_mcp(
        &state_dir,
        &pairing_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
    );

    // 1. Preview returns approved scope and exact frozen envelope_digest without spawning workers.
    let preview: ExecutionPreviewResponse =
        invoke_mcp_ok(&mcp, "preview_execution", json!({ "job_id": job.job_id })).await;
    assert!(preview.approved);
    assert_eq!(
        preview.readiness,
        ExecutionReadinessStatus::ApprovedAndFrozen
    );
    assert_eq!(
        preview.pipeline_boundary,
        threadlane_native::coworker_driver::CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
    );
    let expected_envelope_digest = preview
        .envelope_digest
        .clone()
        .expect("preview envelope_digest");
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    // 2. First `start_execution` spawns a detached `threadlane-flow` owner and returns while Coder is held on gate.
    let start_first: StartExecutionResponse = invoke_mcp_ok(
        &mcp,
        "start_execution",
        json!({
            "job_id": job.job_id,
            "expected_envelope_digest": expected_envelope_digest
        }),
    )
    .await;
    assert!(!gate_release_file.exists());
    assert_eq!(start_first.binding_id, binding_id);
    assert_eq!(start_first.thread_uuid, thread_uuid);
    assert_eq!(start_first.job_id, job.job_id);
    assert_eq!(start_first.approval_id, approval_id);
    assert_eq!(start_first.envelope_digest, expected_envelope_digest);
    assert_eq!(
        start_first.disposition,
        CoworkerDriverEnsureDisposition::SpawnedNewOwner
    );
    assert_eq!(
        start_first.owner_state,
        CoworkerDriverOwnerState::LiveVerifiedOwner
    );
    assert_eq!(
        start_first.pipeline_boundary,
        CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
    );
    let owner_instance_id = start_first
        .status
        .verified_owner_summary
        .as_ref()
        .expect("verified_owner_summary")
        .instance_id
        .clone();

    let started_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_started_file.exists() {
        assert!(
            Instant::now() < started_deadline,
            "timed out waiting for gated fake Coder to start"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 3. Concurrent duplicate `start_execution` calls reuse the live owner without duplicate provider invocations.
    let start_args = json!({
        "job_id": job.job_id,
        "expected_envelope_digest": expected_envelope_digest
    });
    let (dup_a, dup_b): (StartExecutionResponse, StartExecutionResponse) = tokio::join!(
        invoke_mcp_ok(&mcp, "start_execution", start_args.clone()),
        invoke_mcp_ok(&mcp, "start_execution", start_args),
    );
    for dup in [&dup_a, &dup_b] {
        assert_eq!(
            dup.disposition,
            CoworkerDriverEnsureDisposition::ReusedLiveOwner
        );
        assert_eq!(dup.owner_state, CoworkerDriverOwnerState::LiveVerifiedOwner);
        assert_eq!(
            dup.status
                .verified_owner_summary
                .as_ref()
                .expect("dup owner summary")
                .instance_id,
            owner_instance_id
        );
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 4. Poll `execution_status` until the full-mode owner reports the gated Coder task Running,
    //    while the same live owner remains responsible and the job is not prematurely complete.
    let status_readiness_deadline = Instant::now() + Duration::from_secs(5);
    let status_running: ExecutionStatusResponse = loop {
        let current: ExecutionStatusResponse =
            invoke_mcp_ok(&mcp, "execution_status", json!({ "job_id": job.job_id })).await;
        let owner_is_live = matches!(
            current.verified_owner_summary.as_ref(),
            Some(owner) if owner.instance_id.as_str() == owner_instance_id.as_str()
        );
        if current.tasks.len() == 1
            && current.tasks[0].stage == CoworkerTaskStage::Running
            && owner_is_live
            && current.job_phase != CoworkerJobPhase::Completed
        {
            break current;
        }
        assert!(
            Instant::now() < status_readiness_deadline,
            "timed out waiting for full-mode status readiness while Coder is held; last status: {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert!(!gate_release_file.exists());
    assert_eq!(status_running.tasks.len(), 1);
    assert_eq!(status_running.tasks[0].stage, CoworkerTaskStage::Running);
    assert_ne!(status_running.job_phase, CoworkerJobPhase::Completed);
    assert_eq!(
        status_running.pipeline_boundary,
        CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
    );
    assert_eq!(
        status_running
            .verified_owner_summary
            .as_ref()
            .expect("full-mode owner summary")
            .instance_id,
        owner_instance_id
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // 5. `stop_execution` records durable stop first, notifies the live owner, and releases locks cleanly.
    let stop_resp: StopExecutionResponse = invoke_mcp_ok(
        &mcp,
        "stop_execution",
        json!({
            "job_id": job.job_id,
            "reason": "mcp_execution_gateway_stop"
        }),
    )
    .await;
    assert!(stop_resp.durable_stop_recorded_before_host_cancel);
    assert!(stop_resp.notified_live_owner);
    assert_eq!(stop_resp.reason, "mcp_execution_gateway_stop");
    assert_eq!(
        stop_resp.derived_attempt_cancellation_boundary,
        CoworkerDerivedCancellationBoundary::OriginalAndDerivedSignalledDirectCheckReviewerClosureDrainedFenced
    );
    assert_eq!(
        stop_resp.pipeline_boundary,
        CoworkerDriverPipelineBoundary::FullAutomaticPipelineThroughSingleClosure
    );
    assert!(stop_resp.status.stop_requested);
    assert_eq!(
        stop_resp.status.overall_status,
        CoworkerControllerPhase1Status::Stopped
    );
    assert!(!stop_resp.status.lease_active);
    assert!(!stop_resp.status.host_lock_held);

    let driver_ownership_store = CoworkerJobOwnershipStore::new(
        threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir),
    )
    .expect("open driver ownership store");
    let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id)
        .expect("construct ownership key");

    let stop_deadline = Instant::now() + Duration::from_secs(10);
    while matches!(
        driver_ownership_store
            .inspect_job_lock(&ownership_key)
            .expect("inspect driver lock while waiting for stop release"),
        CoworkerJobLockState::HeldByActiveProcess { .. }
    ) {
        assert!(
            Instant::now() < stop_deadline,
            "timed out waiting for physical job lock release after stop_execution"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        !matches!(
            driver_ownership_store
                .inspect_job_lock(&ownership_key)
                .expect("inspect driver lock after stop_execution"),
            CoworkerJobLockState::HeldByActiveProcess { .. }
        ),
        "physical job lock must not be held after stop_execution"
    );

    // 6. Verify observation journal recorded paired Entry + Finish records for all 4 execution tools
    //    with the real binding_id, thread_uuid, job_id, and approval_id.
    let (journal, _journal_warnings) =
        read_journal_records_readonly(&fixture.plans_dir).expect("read observation journal");
    for op in [
        "mcp.paired.preview_execution",
        "mcp.paired.start_execution",
        "mcp.paired.execution_status",
        "mcp.paired.stop_execution",
    ] {
        let entries: Vec<_> = journal
            .iter()
            .filter(|r| r.operation == op && r.phase == ObservationRecordPhase::Entry)
            .collect();
        let finishes: Vec<_> = journal
            .iter()
            .filter(|r| r.operation == op && r.phase == ObservationRecordPhase::Finish)
            .collect();
        assert!(!entries.is_empty(), "missing Entry observation for {op}");
        assert!(!finishes.is_empty(), "missing Finish observation for {op}");
        for rec in entries.iter().chain(finishes.iter()) {
            assert_eq!(rec.binding_id.as_deref(), Some(binding_id.as_str()));
            assert_eq!(rec.thread_uuid.as_deref(), Some(thread_uuid.as_str()));
            assert_eq!(rec.job_id.as_deref(), Some(job.job_id.as_str()));
        }
        assert!(
            finishes
                .iter()
                .all(|r| r.approval_id.as_deref() == Some(approval_id.as_str())),
            "Finish observation for {op} must record correlated approval_id"
        );
    }

    supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn execution_gateway_mcp_superseded_job_status_and_stop_after_amendment_without_new_dispatch()
{
    let fixture = setup_fixture();
    let root = fs::canonicalize(fixture._tmp.path()).expect("canonicalize temp root");
    let state_dir = root.join("runtime-amend-stop");
    let pairing_dir = root.join("pairing-amend-stop");
    for dir in [&state_dir, &pairing_dir] {
        fs::create_dir_all(dir).expect("create runtime/pairing dir");
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    }

    let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
        .await
        .expect("start disposable supervisor");
    let supervisor_client =
        SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
    let binding = supervisor_client
        .create_binding(THREAD_UUID_A, "execution-gateway-amend-stop-test")
        .await
        .expect("create supervisor binding");
    let binding_id = binding.binding_id;
    let thread_uuid = binding.thread_uuid;

    let tasks = vec![PlanTask {
        id: "task-amend-stop-a".to_string(),
        title: "Task Amend Stop A".to_string(),
        instruction: "CTRL_TASK_A update src/a.rs before amendment supersession".to_string(),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Hard,
        depends_on: Vec::new(),
        allowed_write_paths: vec!["src/a.rs".to_string()],
    }];

    let (old_job, approval_id) = create_planned_approved_and_frozen_job_for_scope(
        &fixture,
        &binding_id,
        &thread_uuid,
        "brief-amend-stop-1",
        "plan-amend-stop-1",
        tasks,
        BTreeMap::new(),
        vec!["src/".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);

    let coder_started_file = root.join("mcp_amend_coder_started.flag");
    let gate_release_file = root.join("mcp_amend_coder_gate_release.flag");
    write_gated_fake_agy_script_for_mcp(
        &fixture.fake_agy_exe,
        &fixture.agy_count_file,
        &fixture.agy_planning_count_file,
        &fixture.agy_prompt_file,
        &fixture.agy_plan_response_file,
        &coder_started_file,
        &gate_release_file,
    );

    let mcp_a = create_enrolled_mcp(
        &state_dir,
        &pairing_dir,
        &fixture,
        &binding_id,
        &thread_uuid,
    );
    let mcp_b = create_enrolled_mcp(&state_dir, &pairing_dir, &fixture, BINDING_B, THREAD_UUID_B);

    let preview: ExecutionPreviewResponse = invoke_mcp_ok(
        &mcp_a,
        "preview_execution",
        json!({ "job_id": old_job.job_id }),
    )
    .await;
    let expected_envelope_digest = preview
        .envelope_digest
        .clone()
        .expect("preview envelope_digest");

    let start_resp: StartExecutionResponse = invoke_mcp_ok(
        &mcp_a,
        "start_execution",
        json!({
            "job_id": old_job.job_id,
            "expected_envelope_digest": expected_envelope_digest
        }),
    )
    .await;
    assert_eq!(
        start_resp.disposition,
        CoworkerDriverEnsureDisposition::SpawnedNewOwner
    );

    let started_deadline = Instant::now() + Duration::from_secs(5);
    while !coder_started_file.exists() {
        assert!(
            Instant::now() < started_deadline,
            "timed out waiting for gated fake Coder to start before amendment"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    // Record a real WorkBriefKind::Amendment superseding `old_job` while its gated worker is running.
    let ledger_before_amend = fixture
        .workflow_store
        .read_binding_ledger(&binding_id)
        .expect("read binding ledger before amendment");
    let current_old_job = ledger_before_amend
        .jobs
        .iter()
        .find(|j| j.job_id == old_job.job_id)
        .expect("find running old job in ledger");
    let mut amendment_brief = current_old_job.work_brief.clone();
    amendment_brief.brief_id = "brf_amend_running_job_1".to_string();
    amendment_brief.summary = "Amend running job scope while worker is active".to_string();
    amendment_brief.kind = threadlane_native::coworker_workflow::WorkBriefKind::Amendment {
        target_job_id: current_old_job.job_id.clone(),
        target_job_revision: current_old_job.job_revision,
        amendment_reason: "operator amended scope while previous job was running".to_string(),
    };
    let amended_job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: binding_id.clone(),
            thread_uuid: thread_uuid.clone(),
            request_key: "req_amend_running_job_1".to_string(),
            expected_ledger_revision: None,
            work_brief: amendment_brief,
            amended_target_task_ids: vec!["task-amend-stop-a".to_string()],
            amended_write_paths: vec!["src/a.rs".to_string()],
            amended_interfaces: Vec::new(),
        })
        .expect("create amendment job superseding running job");
    assert_eq!(
        amended_job.supersedes_job_id.as_deref(),
        Some(old_job.job_id.as_str())
    );

    let snapshot_before_drift = fixture
        .registry
        .snapshot()
        .expect("read registry snapshot before label update");
    let mut drifted_tool = snapshot_before_drift
        .tools
        .first()
        .expect("first registered tool")
        .clone();
    drifted_tool.label = format!("{} (Updated After Steering)", drifted_tool.label);
    let snapshot_after_drift = fixture
        .registry
        .apply(
            snapshot_before_drift.revision,
            RegistryChange::UpsertTool { tool: drifted_tool },
        )
        .expect("apply registry label update after worker started");
    assert!(
        snapshot_after_drift.revision
            > old_job
                .approval_linkage
                .as_ref()
                .expect("old job approval linkage")
                .registry_revision
    );

    // Starting the superseded job must still refuse with validation_failed.
    let (superseded_start_code, superseded_start_msg) = invoke_mcp_err(
        &mcp_a,
        "start_execution",
        json!({
            "job_id": old_job.job_id,
            "expected_envelope_digest": expected_envelope_digest
        }),
    )
    .await;
    assert_eq!(superseded_start_code, "validation_failed");
    assert!(superseded_start_msg.contains("superseded"));

    // Cross-scope enrolled server `mcp_b` must still be rejected across all tools.
    for (tool_name, payload) in [
        ("preview_execution", json!({ "job_id": old_job.job_id })),
        (
            "start_execution",
            json!({
                "job_id": old_job.job_id,
                "expected_envelope_digest": expected_envelope_digest
            }),
        ),
        ("execution_status", json!({ "job_id": old_job.job_id })),
        ("stop_execution", json!({ "job_id": old_job.job_id })),
    ] {
        let (cross_code, _) = invoke_mcp_err(&mcp_b, tool_name, payload).await;
        assert_eq!(cross_code, "validation_failed");
    }

    // `execution_status` remains available on the superseded job without new provider calls.
    let status_before_stop: ExecutionStatusResponse = invoke_mcp_ok(
        &mcp_a,
        "execution_status",
        json!({ "job_id": old_job.job_id }),
    )
    .await;
    assert_eq!(status_before_stop.binding_id, binding_id);
    assert_eq!(status_before_stop.thread_uuid, thread_uuid);
    assert_eq!(status_before_stop.job_id, old_job.job_id);
    assert_eq!(status_before_stop.approval_id, approval_id);
    assert_eq!(status_before_stop.envelope_digest, expected_envelope_digest);
    assert_ne!(status_before_stop.job_phase, CoworkerJobPhase::Completed);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

    // `stop_execution` records durable stop first, cancels the active original host task, releases
    // the lease and physical lock, and triggers zero new provider calls. The initial stop response
    // does not promise immediate lease or host release, so teardown is verified through the
    // read-only status endpoint before asserting final ownership state.
    let stop_resp: StopExecutionResponse = invoke_mcp_ok(
        &mcp_a,
        "stop_execution",
        json!({
            "job_id": old_job.job_id,
            "reason": "stop_superseded_after_amendment"
        }),
    )
    .await;
    assert!(stop_resp.durable_stop_recorded_before_host_cancel);
    assert_eq!(stop_resp.reason, "stop_superseded_after_amendment");
    assert!(stop_resp.status.stop_requested);
    assert_eq!(
        stop_resp.status.overall_status,
        CoworkerControllerPhase1Status::Stopped
    );
    assert_ne!(stop_resp.status.job_phase, CoworkerJobPhase::Completed);

    let readiness_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let settled_status: ExecutionStatusResponse = loop {
        let status: ExecutionStatusResponse = invoke_mcp_ok(
            &mcp_a,
            "execution_status",
            json!({ "job_id": old_job.job_id }),
        )
        .await;
        if !status.lease_active
            && !status.host_lock_held
            && status.owner_state != CoworkerDriverOwnerState::LiveVerifiedOwner
        {
            break status;
        }
        if std::time::Instant::now() >= readiness_deadline {
            panic!(
                "timed out waiting for superseded job teardown; last status: {:?}",
                status
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };

    let driver_ownership_store = CoworkerJobOwnershipStore::new(
        threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir),
    )
    .expect("open driver ownership store");
    let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &old_job.job_id)
        .expect("construct ownership key");
    let lock_state_after_stop = driver_ownership_store
        .inspect_job_lock(&ownership_key)
        .expect("inspect driver lock after stopping superseded job");
    assert!(
        !matches!(
            lock_state_after_stop,
            CoworkerJobLockState::HeldByActiveProcess { .. }
        ),
        "physical job lock must not be held after stopping superseded job"
    );

    assert!(settled_status.stop_requested);
    assert_eq!(
        settled_status.overall_status,
        CoworkerControllerPhase1Status::Stopped
    );
    assert_ne!(settled_status.job_phase, CoworkerJobPhase::Completed);
    assert!(!settled_status.lease_active);
    assert!(!settled_status.host_lock_held);

    if stop_resp.narrow_host_cancellation_attempted {
        assert!(
            !stop_resp.cancelled_host_tasks.is_empty(),
            "cancelled_host_tasks must be non-empty when narrow host cancellation is attempted"
        );
    } else {
        assert!(
            stop_resp.cancelled_host_tasks.is_empty()
                && !settled_status.lease_active
                && !settled_status.host_lock_held
                && !matches!(
                    lock_state_after_stop,
                    CoworkerJobLockState::HeldByActiveProcess { .. }
                ),
            "when narrow host cancellation is not attempted, no active host task, lease, or physical lock may remain"
        );
    }

    let status_after_stop: ExecutionStatusResponse = invoke_mcp_ok(
        &mcp_a,
        "execution_status",
        json!({ "job_id": old_job.job_id }),
    )
    .await;
    assert!(status_after_stop.stop_requested);
    assert_eq!(
        status_after_stop.overall_status,
        CoworkerControllerPhase1Status::Stopped
    );
    assert_ne!(status_after_stop.job_phase, CoworkerJobPhase::Completed);
    assert!(!status_after_stop.lease_active);
    assert!(!status_after_stop.host_lock_held);

    let (start_after_stop_code, start_after_stop_msg) = invoke_mcp_err(
        &mcp_a,
        "start_execution",
        json!({
            "job_id": old_job.job_id,
            "expected_envelope_digest": expected_envelope_digest
        }),
    )
    .await;
    assert_eq!(start_after_stop_code, "validation_failed");
    assert!(start_after_stop_msg.contains("superseded"));

    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

    supervisor.shutdown().await;
}
