use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use rmcp::ServerHandler;
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{ScopedClient, ThreadlaneMcpServer};
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
    assert_eq!(WorkflowRole::Architect.as_str(), "architect");
    assert_eq!(WorkflowRole::Coder.as_str(), "coder");
    assert_eq!(WorkflowRole::Reviewer.as_str(), "reviewer");
    assert_eq!(WorkflowRole::Closure.as_str(), "closure");
    assert_eq!(WorkflowRole::Coder.to_string(), "coder");

    // 2. Central display_label() maps Coder -> Worker and leaves other roles unchanged.
    assert_eq!(WorkflowRole::Follower.display_label(), "Follower");
    assert_eq!(WorkflowRole::Orchestrator.display_label(), "Orchestrator");
    assert_eq!(WorkflowRole::Architect.display_label(), "Architect");
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
    assert_eq!(tools.len(), 51);
    let save_draft_tool = tools
        .iter()
        .find(|t| t.name == "save_draft")
        .expect("save_draft tool registered");
    let schema_val = serde_json::to_value(&save_draft_tool.input_schema).expect("schema json");
    let schema_str = serde_json::to_string(&schema_val).expect("schema string");
    for advertised_role in [
        "follower",
        "orchestrator",
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

    assert_eq!(OPERATING_GUIDE_VERSION, "2026-11-13.v1");
    assert!(SCOPED_MCP_STARTUP_INSTRUCTIONS.contains(OPERATING_GUIDE_VERSION));
    assert!(PAIRED_MCP_STARTUP_INSTRUCTIONS.contains(OPERATING_GUIDE_VERSION));
    assert!(OPERATING_GUIDE_MARKDOWN.contains(OPERATING_GUIDE_VERSION));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("Worker"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("coder"));
    assert!(OPERATING_GUIDE_MARKDOWN.contains("role_label"));

    let current = OperatingGuideResponse::current();
    assert_eq!(current.version, "2026-11-13.v1");
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
    assert!(scoped_desc.contains("2026-11-13.v1"));
    assert!(scoped_desc.contains("Worker (coder)"));
    assert!(scoped_desc.contains("complete job admission"));

    let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
    let operator_token = pairing_store.operator_token().expect("operator token");
    let paired_server =
        PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("paired server");
    let paired_tools = paired_server.list_tools();
    // Current combined paired surface: 51 registered tools.
    assert_eq!(paired_tools.len(), 51);
    let paired_guide_tool = paired_tools
        .iter()
        .find(|t| t.name == OPERATING_GUIDE_TOOL)
        .expect("paired operating_guide tool");
    let paired_desc = paired_guide_tool
        .description
        .as_deref()
        .expect("paired operating_guide description");
    assert!(paired_desc.contains("2026-11-13.v1"));
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
    assert_eq!(guide_res["data"]["version"], "2026-11-13.v1");
    assert_eq!(
        guide_res["data"]["guide_markdown"],
        OPERATING_GUIDE_MARKDOWN
    );
}
