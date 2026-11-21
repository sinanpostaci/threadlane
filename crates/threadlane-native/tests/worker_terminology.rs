use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use rmcp::ServerHandler;
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{ScopedClient, ThreadlaneMcpServer};
use threadlane_native::codex_skill::THREADLANE_SKILL_VERSION;
use threadlane_native::{
    open_workflow_stores, CatalogPage, CatalogRoleAssignment, Difficulty, GetPlanParams,
    OperatingGuideParams, OperatingGuideResponse, OperatorEnrollRequest, PairedMcpServer,
    PairingStore, PlanPage, PlanSummary, PlanTask, PlanTaskParams, PublishParams,
    PublishedPlanView, TaskExecutionRoute, ToolAdapter, WorkflowProfile, WorkflowRole,
    OPERATING_GUIDE_MARKDOWN, OPERATING_GUIDE_TOOL, OPERATING_GUIDE_VERSION,
    PAIRED_MCP_STARTUP_INSTRUCTIONS, SCOPED_MCP_STARTUP_INSTRUCTIONS,
};
use threadlane_workflow::{
    ModelChoice, ModelDefinition, RegistryChange, RoleAssignment, ScopedProfileDependencyProof,
    ScopedProfileDependencySpec, ToolDefinition,
};

fn sample_tool(id: &str, adapter: ToolAdapter, enabled: bool) -> ToolDefinition {
    ToolDefinition {
        id: id.to_string(),
        label: format!("Tool {id}"),
        adapter,
        executable_path: if adapter.is_cli_adapter() {
            Some(PathBuf::from(format!("/opt/secret-cli-paths/{id}-bin")))
        } else {
            None
        },
        enabled,
    }
}

fn sample_model(
    id: &str,
    tool_id: &str,
    model_arg: &str,
    efforts: &[&str],
    allow_no_effort: bool,
) -> ModelDefinition {
    ModelDefinition {
        id: id.to_string(),
        tool_id: tool_id.to_string(),
        label: format!("Model {id}"),
        model_arg: model_arg.to_string(),
        efforts: efforts.iter().map(|s| (*s).to_string()).collect(),
        allow_no_effort,
    }
}

#[test]
fn workflow_role_display_label_from_str_and_no_global_serde_map_key_alias() {
    // 1. Canonical wire/storage strings and Display stay unchanged.
    assert_eq!(WorkflowRole::Follower.as_str(), "follower");
    assert_eq!(WorkflowRole::Orchestrator.as_str(), "orchestrator");
    assert_eq!(WorkflowRole::Advisor.as_str(), "advisor");
    assert_eq!(WorkflowRole::Architect.as_str(), "architect");
    assert_eq!(WorkflowRole::Coder.as_str(), "coder");
    assert_eq!(WorkflowRole::Reviewer.as_str(), "reviewer");
    assert_eq!(WorkflowRole::Closure.as_str(), "closure");
    assert_eq!(WorkflowRole::Coder.to_string(), "coder");

    // 2. Central display_label() maps Coder -> Worker and both advisory spellings -> Advisor.
    assert_eq!(WorkflowRole::Follower.display_label(), "Follower");
    assert_eq!(WorkflowRole::Orchestrator.display_label(), "Orchestrator");
    assert_eq!(WorkflowRole::Advisor.display_label(), "Advisor");
    assert_eq!(WorkflowRole::Architect.display_label(), "Advisor");
    assert_eq!(WorkflowRole::Coder.display_label(), "Worker");
    assert_eq!(WorkflowRole::Reviewer.display_label(), "Reviewer");
    assert_eq!(WorkflowRole::Closure.display_label(), "Closure");

    // 3. CLI / FromStr accepts "worker" and "coder" while returning canonical WorkflowRole::Coder.
    assert_eq!("coder".parse::<WorkflowRole>(), Ok(WorkflowRole::Coder));
    assert_eq!("worker".parse::<WorkflowRole>(), Ok(WorkflowRole::Coder));
    for unknown in ["Worker", "CODER", "WORKER", "builder", "task_worker", ""] {
        assert!(
            unknown.parse::<WorkflowRole>().is_err(),
            "FromStr must reject unknown role `{unknown}`"
        );
    }

    // 4. Core WorkflowRole serde stays strictly "coder" and rejects "worker" so map keys cannot collide.
    assert_eq!(
        serde_json::to_string(&WorkflowRole::Coder).expect("serialize WorkflowRole::Coder"),
        "\"coder\""
    );
    assert_eq!(
        serde_json::from_str::<WorkflowRole>("\"coder\"").expect("deserialize canonical coder"),
        WorkflowRole::Coder
    );
    assert!(
        serde_json::from_str::<WorkflowRole>("\"worker\"").is_err(),
        "core WorkflowRole serde must reject \"worker\""
    );
    assert!(
        serde_json::from_str::<BTreeMap<WorkflowRole, String>>(
            r#"{"coder":"model-a","worker":"model-b"}"#
        )
        .is_err(),
        "BTreeMap<WorkflowRole, _> must reject \"worker\" key to prevent silent map overwrite"
    );
    assert!(
        serde_json::from_str::<PlanTask>(
            r#"{
                "id": "task-1",
                "title": "Core PlanTask",
                "instruction": "Persisted PlanTask must require canonical coder",
                "role": "worker",
                "difficulty": "easy",
                "depends_on": [],
                "allowed_write_paths": ["README.md"]
            }"#
        )
        .is_err(),
        "persisted PlanTask serde must require canonical \"coder\""
    );
}

#[tokio::test]
async fn mcp_plan_task_params_schema_and_deserializer_normalize_worker_to_canonical_coder() {
    use rmcp::handler::server::wrapper::Parameters;

    let tmp = TempDir::new().expect("tempdir");
    let state_dir = tmp.path().join("runtime-state");
    let pairing_dir = tmp.path().join("pairing-state");
    let reg_dir = tmp.path().join("workflow-registry");
    let plans_dir = tmp.path().join("workflow-plans");

    let (registry, plans) = open_workflow_stores(Some(&reg_dir), Some(&plans_dir))
        .expect("open workflow stores")
        .expect("workflow stores configured");

    let s1 = registry
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: sample_tool("agy", ToolAdapter::Antigravity, true),
            },
        )
        .expect("upsert agy");
    let s2 = registry
        .apply(
            s1.revision,
            RegistryChange::UpsertTool {
                tool: sample_tool("opencode", ToolAdapter::OpencodeV2, true),
            },
        )
        .expect("upsert opencode");
    let s3 = registry
        .apply(
            s2.revision,
            RegistryChange::UpsertModels {
                models: vec![
                    sample_model(
                        "argon-high",
                        "agy",
                        "gemini-4-argon-high",
                        &["medium", "high"],
                        false,
                    ),
                    sample_model(
                        "deepseek-flash",
                        "opencode",
                        "opencode-go/deepseek-v4.1-flash",
                        &["max"],
                        false,
                    ),
                ],
            },
        )
        .expect("upsert models");

    let profile = WorkflowProfile {
        id: "default-profile".to_string(),
        name: "Default Worker Terminology Profile".to_string(),
        assignments: vec![
            RoleAssignment {
                role: WorkflowRole::Orchestrator,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Architect,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Easy),
                choice: ModelChoice {
                    model_id: "deepseek-flash".to_string(),
                    effort: Some("max".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Reviewer,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Closure,
                difficulty: None,
                choice: ModelChoice {
                    model_id: "argon-high".to_string(),
                    effort: Some("high".to_string()),
                },
            },
        ],
        decision_advisor: None,
    };
    let s4 = registry
        .apply(s3.revision, RegistryChange::UpsertProfile { profile })
        .expect("upsert profile");
    let s5 = registry
        .apply(
            s4.revision,
            RegistryChange::SetDefaultProfile {
                profile_id: Some("default-profile".to_string()),
            },
        )
        .expect("set default profile");
    let reg_rev = s5.revision;

    // Verify ScopedProfileDependencyProof digest stability.
    let dep_spec = ScopedProfileDependencySpec::from_snapshot(&s5, "default-profile")
        .expect("scoped profile dependency spec");
    let dep_proof = ScopedProfileDependencyProof::from_snapshot(&s5, "default-profile")
        .expect("scoped profile dependency proof");
    assert_eq!(
        serde_json::to_value(&dep_spec).expect("serialize dep_spec")["assignments"][2]["role"],
        "coder"
    );
    dep_proof.validate().expect("validate dep_proof");

    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let mcp = PairedMcpServer::new(&state_dir, pairing_store.clone())
        .expect("create paired mcp")
        .with_workflow_stores(Arc::clone(&registry), Arc::clone(&plans));

    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: mcp.connection_nonce().to_string(),
                epoch_id: mcp.epoch_id().to_string(),
                binding_id: "bnd_worker_term_1".to_string(),
                thread_id: "0196ed00-0000-7000-8000-000000000701".to_string(),
                project_id: Some("proj-worker-term".to_string()),
            },
        )
        .expect("enroll paired mcp");

    // 1. Verify the current combined paired MCP tool count and that `save_draft`
    //    schema and `PlanTaskParams` deserializer agree.
    let tools = mcp.list_tools();
    assert_eq!(tools.len(), 59);
    let save_draft_tool = tools
        .iter()
        .find(|t| t.name == "save_draft")
        .expect("save_draft tool registered");
    let schema_val = serde_json::to_value(&save_draft_tool.input_schema).expect("schema json");
    let schema_str = serde_json::to_string(&schema_val).expect("schema string");
    for advertised_role in [
        "follower",
        "orchestrator",
        "advisor",
        "architect",
        "coder",
        "worker",
        "reviewer",
        "closure",
    ] {
        assert!(
            schema_str.contains(&format!("\"{advertised_role}\"")),
            "save_draft schema must advertise `{advertised_role}`"
        );
        let task_json = json!({
            "id": "t1",
            "title": "Role check",
            "role": advertised_role,
            "difficulty": "easy",
            "instruction": "Check role deserialization",
            "depends_on": [],
            "allowed_write_paths": ["README.md"]
        });
        let parsed: PlanTaskParams = serde_json::from_value(task_json)
            .unwrap_or_else(|e| panic!("PlanTaskParams must accept `{advertised_role}`: {e}"));
        if matches!(advertised_role, "coder" | "worker") {
            assert_eq!(parsed.role, WorkflowRole::Coder);
            let reserialized = serde_json::to_value(&parsed).expect("reserialize PlanTaskParams");
            assert_eq!(reserialized["role"], "coder");
        } else if advertised_role == "advisor" {
            assert_eq!(parsed.role, WorkflowRole::Advisor);
            let reserialized = serde_json::to_value(&parsed).expect("reserialize PlanTaskParams");
            assert_eq!(reserialized["role"], "advisor");
        } else if advertised_role == "architect" {
            assert_eq!(parsed.role, WorkflowRole::Architect);
            let reserialized = serde_json::to_value(&parsed).expect("reserialize PlanTaskParams");
            assert_eq!(reserialized["role"], "architect");
        }
    }

    for invalid_role in ["Worker", "CODER", "builder", "executor", ""] {
        let bad_task_json = json!({
            "id": "t1",
            "title": "Invalid role check",
            "role": invalid_role,
            "difficulty": "easy",
            "instruction": "Must fail",
            "depends_on": [],
            "allowed_write_paths": ["README.md"]
        });
        assert!(
            serde_json::from_value::<PlanTaskParams>(bad_task_json).is_err(),
            "PlanTaskParams must reject unknown role `{invalid_role}`"
        );
    }

    // 2. Verify `catalog` derives `role_label: "Worker"` for canonical `coder` while preserving
    //    assignment count, availability, and canonical `role: "coder"`.
    let cat_res = mcp
        .invoke_tool_json("catalog", json!({}), None)
        .await
        .expect("catalog call")
        .structured_content
        .expect("catalog structured content");
    let cat_raw_assignments = cat_res["data"]["profiles"][0]["assignments"]
        .as_array()
        .expect("raw assignments array");
    assert_eq!(cat_raw_assignments.len(), 6);
    for raw_entry in cat_raw_assignments {
        let role_str = raw_entry["role"].as_str().expect("role string");
        let label_str = raw_entry["role_label"].as_str().expect("role_label string");
        let role: WorkflowRole = role_str.parse().expect("parse canonical role");
        assert_eq!(role.as_str(), role_str);
        assert_eq!(label_str, role.display_label());
        if role == WorkflowRole::Coder {
            assert_eq!(role_str, "coder");
            assert_eq!(label_str, "Worker");
        }
    }
    let cat_page: CatalogPage =
        serde_json::from_value(cat_res["data"].clone()).expect("decode CatalogPage");
    let coder_assignments: Vec<&CatalogRoleAssignment> = cat_page.profiles[0]
        .assignments
        .iter()
        .filter(|a| a.role == WorkflowRole::Coder)
        .collect();
    assert_eq!(coder_assignments.len(), 2);
    for assignment in coder_assignments {
        assert_eq!(assignment.role_label, "Worker");
        assert!(assignment.available);
        assert_eq!(assignment.route, TaskExecutionRoute::HeadlessWorker);
    }

    // Old catalog JSON without `role_label` defaults `role_label` from canonical `role`.
    let mut legacy_assignment_json: Value = cat_raw_assignments
        .iter()
        .find(|entry| entry["role"] == "coder")
        .expect("coder assignment json")
        .clone();
    legacy_assignment_json
        .as_object_mut()
        .expect("object")
        .remove("role_label");
    let decoded_legacy_assignment: CatalogRoleAssignment =
        serde_json::from_value(legacy_assignment_json)
            .expect("deserialize legacy catalog assignment without role_label");
    assert_eq!(decoded_legacy_assignment.role, WorkflowRole::Coder);
    assert_eq!(decoded_legacy_assignment.role_label, "Worker");

    // 3. Save draft using `"role": "worker"` and verify persisted record, `get_plan`, `publish`,
    //    and compiled `plan_digest` are byte-identical to canonical `"role": "coder"`.
    let worker_draft_args = json!({
        "command_key": "cmd-worker-plan",
        "plan_id": "plan-worker-input",
        "expected_revision": 0,
        "profile_id": "default-profile",
        "profile_revision": reg_rev,
        "summary": "Compatibility test plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Execute scoped task",
                "role": "worker",
                "difficulty": "hard",
                "instruction": "Perform bounded repository task.",
                "depends_on": [],
                "allowed_write_paths": ["README.md"]
            }
        ]
    });
    let saved_worker: PlanSummary = serde_json::from_value(
        mcp.invoke_tool_json("save_draft", worker_draft_args, None)
            .await
            .expect("save_draft with worker role")
            .structured_content
            .expect("structured")["data"]
            .clone(),
    )
    .expect("decode worker PlanSummary");

    // Idempotent retry of the same command_key with `"role": "coder"` must succeed without conflict.
    let coder_retry_args = json!({
        "command_key": "cmd-worker-plan",
        "plan_id": "plan-worker-input",
        "expected_revision": 0,
        "profile_id": "default-profile",
        "profile_revision": reg_rev,
        "summary": "Compatibility test plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Execute scoped task",
                "role": "coder",
                "difficulty": "hard",
                "instruction": "Perform bounded repository task.",
                "depends_on": [],
                "allowed_write_paths": ["README.md"]
            }
        ]
    });
    let retry_coder: PlanSummary = serde_json::from_value(
        mcp.invoke_tool_json("save_draft", coder_retry_args, None)
            .await
            .expect("idempotent retry with canonical coder role")
            .structured_content
            .expect("structured")["data"]
            .clone(),
    )
    .expect("decode retry PlanSummary");
    assert_eq!(saved_worker, retry_coder);

    // Verify `get_plan` wire output and `PlanStore` persisted snapshot store canonical `"coder"`.
    let get_res = mcp
        .get_plan(Parameters(GetPlanParams {
            plan_id: "plan-worker-input".to_string(),
            revision: Some(1),
            task_cursor: None,
            task_limit: None,
        }))
        .await
        .expect("get_plan")
        .structured_content
        .expect("get_plan structured content");
    assert_eq!(get_res["data"]["tasks"][0]["role"], "coder");
    let plan_page: PlanPage =
        serde_json::from_value(get_res["data"].clone()).expect("decode PlanPage");
    assert_eq!(plan_page.tasks[0].role, WorkflowRole::Coder);

    let persisted_rev = plans
        .get_revision("plan-worker-input", 1)
        .expect("read revision")
        .expect("revision exists");
    assert_eq!(persisted_rev.tasks[0].role, WorkflowRole::Coder);
    let raw_plans_file =
        std::fs::read_to_string(plans.dir().join("plans.json")).expect("read plans.json");
    assert!(
        raw_plans_file.contains("\"role\": \"coder\"")
            || raw_plans_file.contains("\"role\":\"coder\"")
    );
    assert!(!raw_plans_file.contains("\"worker\""));

    // Current advisor task input persists the actual current advisor identity.
    let advisor_draft_args = json!({
        "command_key": "cmd-advisor-plan",
        "plan_id": "plan-advisor-input",
        "expected_revision": 0,
        "profile_id": "default-profile",
        "profile_revision": reg_rev,
        "summary": "Advisor identity plan",
        "tasks": [
            {
                "id": "task-advisor",
                "title": "Consult the advisor",
                "role": "advisor",
                "difficulty": "hard",
                "instruction": "Read-only advisory task.",
                "depends_on": [],
                "allowed_write_paths": ["README.md"]
            }
        ]
    });
    let _saved_advisor: PlanSummary = serde_json::from_value(
        mcp.invoke_tool_json("save_draft", advisor_draft_args, None)
            .await
            .expect("save_draft with advisor role")
            .structured_content
            .expect("structured")["data"]
            .clone(),
    )
    .expect("decode advisor PlanSummary");
    let advisor_page_res = mcp
        .get_plan(Parameters(GetPlanParams {
            plan_id: "plan-advisor-input".to_string(),
            revision: Some(1),
            task_cursor: None,
            task_limit: None,
        }))
        .await
        .expect("get_plan advisor")
        .structured_content
        .expect("advisor get_plan structured content");
    assert_eq!(advisor_page_res["data"]["tasks"][0]["role"], "advisor");
    let advisor_page: PlanPage =
        serde_json::from_value(advisor_page_res["data"].clone()).expect("decode advisor PlanPage");
    assert_eq!(advisor_page.tasks[0].role, WorkflowRole::Advisor);
    let persisted_advisor = plans
        .get_revision("plan-advisor-input", 1)
        .expect("read advisor revision")
        .expect("advisor revision exists");
    assert_eq!(persisted_advisor.tasks[0].role, WorkflowRole::Advisor);
    let raw_plans_file =
        std::fs::read_to_string(plans.dir().join("plans.json")).expect("read plans.json");
    assert!(
        raw_plans_file.contains("\"role\": \"advisor\"")
            || raw_plans_file.contains("\"role\":\"advisor\"")
    );

    let pub_worker: PublishedPlanView = serde_json::from_value(
        mcp.publish(Parameters(PublishParams {
            plan_id: "plan-worker-input".to_string(),
            revision: 1,
        }))
        .await
        .expect("publish plan")
        .structured_content
        .expect("publish structured")["data"]
            .clone(),
    )
    .expect("decode PublishedPlanView");
    assert_eq!(pub_worker.tasks[0].role, WorkflowRole::Coder);
    assert_eq!(pub_worker.header.plan_digest, saved_worker.plan_digest);

    // Compare against a plan created directly with `"role": "coder"` using the same plan_id in a fresh store.
    let plans_dir_2 = tmp.path().join("workflow-plans-canonical");
    let (_, plans_canonical) = open_workflow_stores(Some(&reg_dir), Some(&plans_dir_2))
        .expect("open second workflow stores")
        .expect("configured");
    let mcp_canonical = PairedMcpServer::new(&state_dir, pairing_store.clone())
        .expect("create second paired mcp")
        .with_workflow_stores(Arc::clone(&registry), Arc::clone(&plans_canonical));
    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: mcp_canonical.connection_nonce().to_string(),
                epoch_id: mcp_canonical.epoch_id().to_string(),
                binding_id: "bnd_worker_term_1".to_string(),
                thread_id: "0196ed00-0000-7000-8000-000000000702".to_string(),
                project_id: Some("proj-worker-term".to_string()),
            },
        )
        .expect("enroll second paired mcp");
    let saved_coder: PlanSummary = serde_json::from_value(
        mcp_canonical
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "cmd-worker-plan",
                    "plan_id": "plan-worker-input",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Compatibility test plan",
                    "tasks": [
                        {
                            "id": "task-1",
                            "title": "Execute scoped task",
                            "role": "coder",
                            "difficulty": "hard",
                            "instruction": "Perform bounded repository task.",
                            "depends_on": [],
                            "allowed_write_paths": ["README.md"]
                        }
                    ]
                }),
                None,
            )
            .await
            .expect("save_draft with canonical coder")
            .structured_content
            .expect("structured")["data"]
            .clone(),
    )
    .expect("decode canonical PlanSummary");
    assert_eq!(
        saved_worker.plan_digest, saved_coder.plan_digest,
        "worker input must compile to the exact same v1 plan_digest as canonical coder"
    );

    // 4. Unknown role at MCP `save_draft` boundary fails closed with `validation_failed` on the
    //    currently enrolled server, while the superseded first server reports `expired_epoch`.
    let expired_first_err = mcp
        .invoke_tool_json("catalog", json!({}), None)
        .await
        .expect_err("superseded first server epoch must be expired");
    assert_eq!(
        expired_first_err.structured_content.expect("expired err")["error"]["code"],
        "expired_epoch"
    );

    let invalid_mcp_err = mcp_canonical
        .invoke_tool_json(
            "save_draft",
            json!({
                "command_key": "cmd-invalid-role",
                "expected_revision": 0,
                "profile_id": "default-profile",
                "profile_revision": reg_rev,
                "summary": "Invalid role test",
                "tasks": [
                    {
                        "id": "task-1",
                        "title": "Invalid role",
                        "role": "builder",
                        "difficulty": "easy",
                        "instruction": "Should be rejected",
                        "depends_on": [],
                        "allowed_write_paths": ["README.md"]
                    }
                ]
            }),
            None,
        )
        .await
        .expect_err("unknown task role must fail save_draft");
    assert_eq!(
        invalid_mcp_err.structured_content.expect("structured err")["error"]["code"],
        "validation_failed"
    );
}

#[tokio::test]
async fn operating_guide_and_startup_instructions_report_current_version_and_worker_compatibility()
{
    use rmcp::handler::server::wrapper::Parameters;

    assert_eq!(OPERATING_GUIDE_VERSION, "2026-11-19.v5");
    assert_eq!(THREADLANE_SKILL_VERSION, "2026-11-19.v5");
    assert!(SCOPED_MCP_STARTUP_INSTRUCTIONS.contains(OPERATING_GUIDE_VERSION));
    assert!(PAIRED_MCP_STARTUP_INSTRUCTIONS.contains(OPERATING_GUIDE_VERSION));
    assert!(OPERATING_GUIDE_MARKDOWN.contains(OPERATING_GUIDE_VERSION));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("Worker"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("coder"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("role_label"));

    let current = OperatingGuideResponse::current();
    assert_eq!(current.version, "2026-11-19.v5");
    assert_eq!(current.guide_markdown, OPERATING_GUIDE_MARKDOWN);

    let tmp = TempDir::new().expect("tempdir");
    let state_dir = tmp.path().join("runtime-state");
    let pairing_dir = tmp.path().join("pairing-state");

    let scoped =
        ScopedClient::from_state_dir_lazy(&state_dir, "bnd_0196ed00-0000-7000-8000-000000000703")
            .expect("lazy scoped client");
    let scoped_server = ThreadlaneMcpServer::new(scoped);
    let scoped_tools = scoped_server.list_tools();
    // Current combined scoped surface: 24 registered tools.
    assert_eq!(scoped_tools.len(), 24);
    let scoped_guide_tool = scoped_tools
        .iter()
        .find(|t| t.name == OPERATING_GUIDE_TOOL)
        .expect("scoped operating_guide tool");
    let scoped_desc = scoped_guide_tool
        .description
        .as_deref()
        .expect("scoped operating_guide description");
    assert!(scoped_desc.contains("2026-11-19.v5"));
    assert!(scoped_desc.contains("Worker (coder)"));
    assert!(scoped_desc.contains("complete job admission"));

    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let paired_server =
        PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("paired server");
    let paired_tools = paired_server.list_tools();
    // Current combined paired surface: 59 registered tools.
    assert_eq!(paired_tools.len(), 59);
    assert!(paired_tools
        .iter()
        .any(|tool| tool.name == "ordinary_review_notice"));
    let paired_guide_tool = paired_tools
        .iter()
        .find(|t| t.name == OPERATING_GUIDE_TOOL)
        .expect("paired operating_guide tool");
    let paired_desc = paired_guide_tool
        .description
        .as_deref()
        .expect("paired operating_guide description");
    assert!(paired_desc.contains("2026-11-19.v5"));
    assert!(paired_desc.contains("Worker (coder)"));
    assert!(paired_desc.contains("complete job admission"));

    assert_eq!(
        scoped_server.get_info().instructions.as_deref(),
        Some(SCOPED_MCP_STARTUP_INSTRUCTIONS)
    );
    assert_eq!(
        paired_server.get_info().instructions.as_deref(),
        Some(PAIRED_MCP_STARTUP_INSTRUCTIONS)
    );

    pairing_store
        .enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: paired_server.connection_nonce().to_string(),
                epoch_id: paired_server.epoch_id().to_string(),
                binding_id: "bnd_0196ed00-0000-7000-8000-000000000703".to_string(),
                thread_id: "0196ed00-0000-7000-8000-000000000703".to_string(),
                project_id: None,
            },
        )
        .expect("enroll paired server");

    let guide_res = paired_server
        .operating_guide(Parameters(OperatingGuideParams::default()))
        .await
        .expect("paired operating_guide")
        .structured_content
        .expect("structured guide");
    assert_eq!(guide_res["data"]["version"], "2026-11-19.v5");
    assert_eq!(
        guide_res["data"]["guide_markdown"],
        OPERATING_GUIDE_MARKDOWN
    );
    assert!(OPERATING_GUIDE_MARKDOWN.contains("ordinary_closure_client"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("bootstrap_verified"));
    assert!(threadlane::CLOSURE_TEMPLATE_JAVASCRIPT
        .contains("tools.mcp__threadlane__approved_pull_events"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("closure-evidence.json"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("cross-page continuity"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("new Function(bootstrap)"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("APPROVAL_ID"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("const decision = REVIEW_DECISION;"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("client.accept(decision)"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("verdict: \"accept\""));
    assert!(OPERATING_GUIDE_MARKDOWN
        .contains("ORDINARY-WORKER-CLOSURE.md#legacy-completion-for-an-existing-read"));
    let legacy_closure = include_str!("../../../docs/ORDINARY-WORKER-CLOSURE.md");
    assert!(legacy_closure.contains("const acceptance = ACCEPTANCE_CONTEXT"));
    assert!(legacy_closure.contains("client.complete(acceptance)"));
    assert!(!OPERATING_GUIDE_MARKDOWN.contains("<ACTUAL_"));
    let bad_recipe = ["text(", "await tools.exec_command"].concat();
    assert!(!OPERATING_GUIDE_MARKDOWN.contains(bad_recipe.as_str()));
    let forbidden_log = ["con", "sole"].concat();
    assert!(!OPERATING_GUIDE_MARKDOWN.contains(forbidden_log.as_str()));
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn approval_fallback_docs_and_js_example_align_legacy_and_v2_record_request_contracts() {
    use threadlane_authority::{
        ApprovedTaskRecord, OperatorApprovalPreview, OperatorApprovalRequest,
        AUTHORITY_REQUEST_V2_SCHEMA_VERSION,
    };
    use threadlane_workers::{ExecutionBudgets, WorkerFilePermissions, WorkerProvider};

    const NATIVE_OPERATOR_MD: &str = include_str!("../../../docs/NATIVE-OPERATOR.md");
    const SKILL_MD: &str = include_str!("../../../skills/threadlane/SKILL.md");

    // Preserve guide/skill version pins, byte budgets, and safety hygiene.
    assert_eq!(OPERATING_GUIDE_VERSION, "2026-11-19.v5");
    assert_eq!(THREADLANE_SKILL_VERSION, "2026-11-19.v5");
    assert!(
        OPERATING_GUIDE_MARKDOWN.len() <= 16_384,
        "canonical MCP guide must stay within 16384 UTF-8 bytes, got {}",
        OPERATING_GUIDE_MARKDOWN.len()
    );
    assert!(
        SKILL_MD.len() <= 64 * 1024,
        "bundled SKILL.md must stay within 65536 UTF-8 bytes, got {}",
        SKILL_MD.len()
    );
    assert!(OPERATING_GUIDE_MARKDOWN.contains(OPERATING_GUIDE_VERSION));
    assert!(NATIVE_OPERATOR_MD.contains(OPERATING_GUIDE_VERSION));
    assert!(SKILL_MD.contains(THREADLANE_SKILL_VERSION));

    let normalized_guide = normalize_whitespace(OPERATING_GUIDE_MARKDOWN);
    let normalized_operator = normalize_whitespace(NATIVE_OPERATOR_MD);
    let normalized_skill = normalize_whitespace(SKILL_MD);

    // Regression guard: none of the three docs may unconditionally tell fallback users
    // to serialize `preview.request` regardless of frozen `required_changed_paths`.
    assert!(!normalized_guide
        .contains("Legacy `approve prepare --request` records unchanged `preview.request`."));
    assert!(!normalized_skill.contains("saves ONLY the unchanged normalized `preview.request`"));
    assert!(!normalized_operator.contains("save ONLY the unchanged `preview.request`"));
    assert!(!NATIVE_OPERATOR_MD
        .contains("Example functions.exec fallback for saving unchanged preview.request (0600)"));

    for required in [
        "Plain legacy shape for `None`; typed v2 `record_request` only for `Some(required_changed_paths)`",
        "Prefer `approve record --preview` (`None`/`Some`",
        "Fallback `--request` (`0600`, no overwrite, consent/origin/digest checks): unchanged emitted v2 `record_request` (`Some`) or legacy `preview.request` (`None`)",
        "fail closed if frozen required paths lack `record_request` (never rebuild, drop data, or authorize a run)",
    ] {
        assert!(
            normalized_guide.contains(required),
            "MCP operating guide is missing fallback DTO alignment fragment `{required}`"
        );
    }

    for required in [
        "plain legacy output shape for `None`, adding top-level typed v2 `record_request` only when `required_changed_paths` is `Some(R)`",
        "preferred `approve record --preview` (which works for both legacy `None` and `Some(required_changed_paths)`)",
        "save ONLY the actual emitted unchanged typed v2 `record_request` (`OperatorApprovalRequestV2`) when any task in `preview.tasks` freezes `required_changed_paths`, or the complete unchanged legacy `preview.request` (`OperatorApprovalRequest`, including `expected_plan_digest`) when all tasks leave `required_changed_paths` unset (`None`, rejecting any unexpected `record_request`)",
        "Fail closed if a frozen `required_changed_paths` set exists on any task while the actual emitted v2 `record_request` is absent or invalid",
        "never rebuild it, silently drop `required_changed_paths` or `expected_plan_digest`, or authorize/start a new run",
    ] {
        assert!(
            normalized_skill.contains(required),
            "bundled SKILL.md is missing fallback DTO alignment fragment `{required}`"
        );
    }

    for required in [
        "preserves the plain legacy `OperatorApprovalPreview` output shape",
        "emit a top-level typed v2 `record_request` (`OperatorApprovalRequestV2`: `{schema_version: 2, request, task_required_changed_paths}`)",
        "`record --preview` works for both legacy `None` and `Some(required_changed_paths)`",
        "When any task in `preview.tasks` freezes `required_changed_paths`, save ONLY the actual emitted top-level typed v2 `record_request`",
        "Fail closed if a frozen `required_changed_paths` entry exists on any task while the actual emitted v2 `record_request` is absent or malformed",
        "never rebuild `record_request` manually, never silently drop `required_changed_paths` by falling back to `preview.request`, and never authorize or start a new run",
        "When all tasks leave `required_changed_paths` unset (`None`, plain legacy output shape), reject any unexpected `record_request` and save ONLY the complete unchanged legacy `preview.request`",
    ] {
        assert!(
            normalized_operator.contains(required),
            "docs/NATIVE-OPERATOR.md is missing fallback DTO alignment fragment `{required}`"
        );
    }

    // Verify the executable functions.exec JS fallback snippet handles both CLI output shapes,
    // selects emitted v2 `record_request` or legacy `preview.request`, fails closed on missing or
    // mismatched v2 DTOs, and preserves every 0600/no-overwrite/digest/path safeguard.
    let js_fallback = NATIVE_OPERATOR_MD
        .split("```js")
        .skip(1)
        .filter_map(|fragment| fragment.split_once("```").map(|(code, _)| code))
        .find(|code| code.contains("Example functions.exec fallback"))
        .expect("executable functions.exec fallback JS snippet in docs/NATIVE-OPERATOR.md");
    for required_js in [
        "const isPrivateReceipt = Object.hasOwn(prepareOut, \"preview\");",
        "if (isPrivateReceipt && Object.hasOwn(preview, \"record_request\"))",
        "Object.hasOwn(task, \"required_changed_paths\")",
        "if (requiredByTask.size > 0)",
        "Object.keys(v2).length !== 3 || v2.schema_version !== 2",
        "v2.request.expected_plan_digest !== preview.request.expected_plan_digest",
        "v2.task_required_changed_paths",
        "frozen required_changed_paths require actual emitted v2 record_request",
        "unexpected record_request on legacy preview without required_changed_paths",
        "recordDto = v2;",
        "recordDto = preview.request;",
        "const rawJson = JSON.stringify(recordDto);",
        "umask 077; set -C;",
        "CDPATH= cd -P -- \"$dir\" && pwd -P",
        "printf '%s'",
        "shQuote(rawJson)",
        "!reqPath.endsWith(\"/request.json\")",
    ] {
        assert!(
            js_fallback.contains(required_js),
            "docs/NATIVE-OPERATOR.md JS fallback snippet is missing `{required_js}`"
        );
    }
    assert!(!js_fallback.contains("JSON.stringify(preview.request)"));

    // Confirm real authority DTO wire shapes match the documented legacy vs v2 contract.
    assert_eq!(AUTHORITY_REQUEST_V2_SCHEMA_VERSION, 2);
    let base_request = OperatorApprovalRequest {
        binding_id: "bnd_0196ed00-0000-7000-8000-000000000704".to_string(),
        thread_uuid: "0196ed00-0000-7000-8000-000000000704".to_string(),
        plan_id: "pln_0196ed00-0000-7000-8000-000000000704".to_string(),
        plan_revision: 1,
        expected_registry_revision: 1,
        expected_plan_digest: "a".repeat(64),
        repository_path: PathBuf::from("/tmp/example-repo"),
        base_commit: "1".repeat(40),
        approved_task_ids: vec!["task-1".to_string()],
        approved_write_paths: vec!["README.md".to_string(), "STATUS.txt".to_string()],
        max_parallel: 1,
        budgets: ExecutionBudgets::default(),
    };
    let make_task = |required_changed_paths: Option<Vec<String>>| ApprovedTaskRecord {
        task_id: "task-1".to_string(),
        title: "Sample task".to_string(),
        instruction: "Update README.md".to_string(),
        instruction_digest: "b".repeat(64),
        role: WorkflowRole::Coder,
        difficulty: Difficulty::Easy,
        tool_id: "opencode".to_string(),
        adapter: ToolAdapter::OpencodeV2,
        provider: Some(WorkerProvider::OpenCodeV2),
        canonical_executable: Some(PathBuf::from("/usr/bin/opencode")),
        model_id: "model-fast".to_string(),
        model_arg: "provider/model-fast".to_string(),
        base_model: "provider/model-fast".to_string(),
        effort: Some("high".to_string()),
        cli_model_arg: Some("provider/model-fast#high".to_string()),
        standalone: Some(true),
        file_permissions: Some(WorkerFilePermissions::TaskFiles),
        depends_on: vec![],
        allowed_write_paths: vec!["README.md".to_string(), "STATUS.txt".to_string()],
        required_changed_paths,
        task_digest: "c".repeat(64),
        profile_id: Some("worker-profile-1".to_string()),
        command_key: Some("cmd-task-1".to_string()),
    };

    let legacy_preview = OperatorApprovalPreview {
        request: base_request.clone(),
        subject_digest: "d".repeat(64),
        tasks: vec![make_task(None)],
    };
    assert!(legacy_preview
        .task_required_changed_paths()
        .expect("legacy preview map")
        .is_empty());
    let legacy_json = serde_json::to_value(&legacy_preview).expect("serialize legacy preview");
    let legacy_task_obj = legacy_json["tasks"][0]
        .as_object()
        .expect("legacy task object");
    assert!(!legacy_task_obj.contains_key("required_changed_paths"));
    assert_eq!(
        legacy_json["request"]["expected_plan_digest"],
        "a".repeat(64)
    );

    let v2_preview = OperatorApprovalPreview {
        request: base_request,
        subject_digest: "e".repeat(64),
        tasks: vec![make_task(Some(vec!["README.md".to_string()]))],
    };
    let v2_request = v2_preview
        .to_request_v2()
        .expect("derive typed v2 record_request");
    let v2_json = serde_json::to_value(&v2_request).expect("serialize v2 record_request");
    let v2_obj = v2_json.as_object().expect("v2 object");
    assert_eq!(v2_obj.len(), 3);
    assert_eq!(v2_obj["schema_version"], 2);
    assert_eq!(v2_obj["request"]["expected_plan_digest"], "a".repeat(64));
    assert_eq!(
        v2_obj["task_required_changed_paths"],
        json!({ "task-1": ["README.md"] })
    );
}

#[test]
fn readonly_role_routing_docs_and_live_mcp_schemas_cover_job_reuse_and_prepare_work() {
    const NATIVE_OPERATOR_MD: &str = include_str!("../../../docs/NATIVE-OPERATOR.md");
    const SKILL_MD: &str = include_str!("../../../skills/threadlane/SKILL.md");

    let normalized_guide = normalize_whitespace(OPERATING_GUIDE_MARKDOWN);
    let normalized_operator = normalize_whitespace(NATIVE_OPERATOR_MD);
    let normalized_skill = normalize_whitespace(SKILL_MD);

    for required in [
        "Optional read-only roles (not mandatory; no new job for ordinary work)",
        "plan/dependency/ownership changes need `Orchestrator`; architectural ambiguity uses `Advisor`",
        "Reuse a suitable scoped job (`coworker_status`) or call metadata-only `prepare_work` (exact binding/project/base/requirements/profile under live guide; no model calls/grants/auto-approval/dispatch) instead of asking for `job_id`",
        "pass actual `job_id` + matching live role grant to `request_technical_plan`/`consult_advisor` (legacy `architect`: `consult_architect`)",
        "Changed authority, missing user-owned requirements, or prior ambiguity stay explicit (no invented IDs/implicit grants)",
    ] {
        assert!(
            normalized_guide.contains(required),
            "MCP operating guide is missing read-only role routing fragment `{required}`"
        );
    }

    for (doc_name, normalized_doc) in [
        ("docs/NATIVE-OPERATOR.md", &normalized_operator),
        ("skills/threadlane/SKILL.md", &normalized_skill),
    ] {
        for required in [
            "Read-only `Orchestrator` and `Advisor` stages remain optional (never mandatory, and no new job is required for ordinary single-task Worker work)",
            "inspect live `coworker_status` first: reuse an existing suitable scoped job whose binding, project, base/tree, requirements, and profile match",
            "call metadata-only `prepare_work` rather than asking the human for a `job_id`, then pass the actual returned `job_id` together with the matching live role grant (`orchestrator` for `request_technical_plan`; explicit `advisor` for `consult_advisor`, or retained `architect` for pre-transition `consult_architect`) to `request_technical_plan` or `consult_advisor`",
            "`prepare_work` makes no model calls, creates no grants, grants no execution approval, and dispatches nothing",
            "missing necessary user-owned requirements, or prior ambiguity remain explicit with the human; never invent a `job_id`, infer implicit grants, auto-dispatch, or auto-approve",
        ] {
            assert!(
                normalized_doc.contains(required),
                "{doc_name} is missing read-only role routing fragment `{required}`"
            );
        }
    }

    // Verify live paired MCP tool schemas match the documented read-only role routing contract.
    let tmp = TempDir::new().expect("tempdir");
    let state_dir = tmp.path().join("runtime-state");
    let pairing_dir = tmp.path().join("pairing-state");
    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let paired_server =
        PairedMcpServer::new(&state_dir, pairing_store).expect("create paired server");
    let paired_tools = paired_server.list_tools();

    let tool_schema = |name: &str| -> Value {
        let tool = paired_tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("paired MCP surface must expose `{name}`"));
        serde_json::to_value(&tool.input_schema).expect("serialize MCP tool input schema")
    };
    let required_fields = |schema: &Value| -> Vec<String> {
        schema
            .get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };

    let status_schema = tool_schema("coworker_status");
    assert_eq!(status_schema["additionalProperties"], json!(false));
    assert!(required_fields(&status_schema).is_empty());

    let prepare_schema = tool_schema("prepare_work");
    assert_eq!(prepare_schema["additionalProperties"], json!(false));
    let prepare_required = required_fields(&prepare_schema);
    for field in [
        "grant_id",
        "request_key",
        "summary",
        "confirmed_requirements",
    ] {
        assert!(
            prepare_required.iter().any(|item| item == field),
            "prepare_work schema must require `{field}`"
        );
    }
    let prepare_props = prepare_schema["properties"]
        .as_object()
        .expect("prepare_work properties");
    for server_derived in [
        "binding_id",
        "thread_uuid",
        "job_id",
        "project_base",
        "profile_id",
    ] {
        assert!(
            !prepare_props.contains_key(server_derived),
            "prepare_work must derive `{server_derived}` from live server/grant state, not caller args"
        );
    }

    let plan_schema = tool_schema("request_technical_plan");
    assert_eq!(plan_schema["additionalProperties"], json!(false));
    let plan_required = required_fields(&plan_schema);
    for field in ["grant_id", "job_id", "request_key"] {
        assert!(
            plan_required.iter().any(|item| item == field),
            "request_technical_plan schema must require `{field}`"
        );
    }

    for advisor_tool in ["consult_advisor", "consult_architect"] {
        let advisor_schema = tool_schema(advisor_tool);
        assert_eq!(advisor_schema["additionalProperties"], json!(false));
        let advisor_required = required_fields(&advisor_schema);
        for field in [
            "grant_id",
            "job_id",
            "request_key",
            "question",
            "dependency_files",
        ] {
            assert!(
                advisor_required.iter().any(|item| item == field),
                "{advisor_tool} schema must require `{field}`"
            );
        }
    }
}
