use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use threadlane::{
    format_coworker_job_wakeup_message, NativeToolOutputConfig, Supervisor, SupervisorClient,
    SupervisorConfig,
};
use threadlane_native::coworker_driver::{CoworkerCodeDriver, CoworkerDriverEnsureDisposition};
use threadlane_native::coworker_notice::{
    coworker_notice_dir, CoworkerNoticeConfig, CoworkerNoticeStore, NoticeDeliveryStatus,
};
use threadlane_native::coworker_workflow::{
    read_persisted_derived_task_execution_report, read_persisted_task_review_report,
    CoworkerJobLockState, CoworkerJobOwnershipKey, OutboxDispatchStatus,
};
use threadlane_native::planning_driver::planning_artifacts_dir;
use tokio::net::UnixListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;

async fn run_flow_cli_pipeline_output(
    state_dir: &Path,
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    subcommand_args: &[&str],
) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--registry-dir")
        .arg(&fixture.registry_dir)
        .arg("--plans-dir")
        .arg(&fixture.plans_dir)
        .arg("--authority-dir")
        .arg(&fixture.authority_dir)
        .arg("--sessions-dir")
        .arg(&fixture.sessions_dir)
        .arg("--binding")
        .arg(binding_id)
        .arg("--thread")
        .arg(thread_uuid)
        .args(subcommand_args)
        .output()
        .await
        .expect("spawn threadlane-flow subprocess")
}

async fn run_flow_cli_pipeline(
    state_dir: &Path,
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    subcommand_args: &[&str],
) -> Value {
    let output =
        run_flow_cli_pipeline_output(state_dir, fixture, binding_id, thread_uuid, subcommand_args)
            .await;
    assert!(
        output.status.success(),
        "threadlane-flow {:?} failed with status {:?}\nstdout: {}\nstderr: {}",
        subcommand_args,
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let envelope: Value =
        serde_json::from_slice(&output.stdout).expect("parse threadlane-flow stdout JSON envelope");
    assert_eq!(envelope["untrusted_data"], Value::Bool(true));
    assert_eq!(
        envelope["route"],
        Value::String("trusted_local_operator".to_string())
    );
    assert_eq!(
        envelope["binding_id"],
        Value::String(binding_id.to_string())
    );
    envelope["data"].clone()
}

async fn read_ws_json<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = ws
        .next()
        .await
        .expect("expected ws frame")
        .expect("ws frame ok");
    match msg {
        Message::Text(text) => serde_json::from_str(&text).expect("valid json frame"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

fn spawn_single_coworker_notice_ws_server(
    fixture: &PipelineTestFixture,
    thread_uuid: &str,
) -> (PathBuf, Arc<AtomicUsize>, tokio::task::JoinHandle<Value>) {
    let sock_dir = fixture.root.join("sock");
    fs::create_dir_all(&sock_dir).expect("create sock_dir");
    fs::set_permissions(&sock_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 sock_dir");
    let socket_path = sock_dir.join("notice.sock");
    assert!(
        socket_path.as_os_str().len() < 108,
        "socket path must be < 108 bytes"
    );
    let listener = UnixListener::bind(&socket_path).expect("bind notice socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 notice socket");

    let expected_cwd = fs::canonicalize(&fixture.repo_dir)
        .expect("canonical repo_dir")
        .to_string_lossy()
        .to_string();
    let expected_thread = thread_uuid.to_string();
    let effect_count = Arc::new(AtomicUsize::new(0));
    let effect_count_srv = Arc::clone(&effect_count);

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept notice ws");
        drop(listener);
        let mut ws = accept_async(stream).await.expect("ws handshake");

        let init = read_ws_json(&mut ws).await;
        assert_eq!(init["id"], json!(1));
        assert_eq!(init["method"], json!("initialize"));
        assert_eq!(
            init["params"]["capabilities"]["experimentalApi"],
            json!(true)
        );
        ws.send(Message::Text(
            json!({ "id": 1, "result": { "serverInfo": { "name": "codex" } } })
                .to_string()
                .into(),
        ))
        .await
        .expect("send init resp");

        let initialized = read_ws_json(&mut ws).await;
        assert_eq!(initialized, json!({ "method": "initialized" }));

        let read_req = read_ws_json(&mut ws).await;
        assert_eq!(read_req["id"], json!(2));
        assert_eq!(read_req["method"], json!("thread/read"));
        assert_eq!(
            read_req["params"],
            json!({ "threadId": expected_thread, "includeTurns": false })
        );
        let idle_thread = json!({
            "thread": { "id": expected_thread, "cwd": expected_cwd, "status": { "type": "idle" } }
        });
        ws.send(Message::Text(
            json!({ "id": 2, "result": idle_thread }).to_string().into(),
        ))
        .await
        .expect("send read resp");

        let resume_req = read_ws_json(&mut ws).await;
        assert_eq!(resume_req["id"], json!(3));
        assert_eq!(resume_req["method"], json!("thread/resume"));
        assert_eq!(
            resume_req["params"],
            json!({ "threadId": expected_thread, "excludeTurns": true })
        );
        assert!(resume_req["params"].get("model").is_none());
        assert!(resume_req["params"].get("effort").is_none());
        ws.send(Message::Text(
            json!({ "id": 3, "result": idle_thread }).to_string().into(),
        ))
        .await
        .expect("send resume resp");

        let turn_req = read_ws_json(&mut ws).await;
        effect_count_srv.fetch_add(1, Ordering::SeqCst);
        ws.send(Message::Text(
            json!({
                "id": 4,
                "result": { "turn": { "id": "01968f52-9999-7d51-9914-79d2bc48eb5b", "status": "inProgress" } }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send turn/start resp");
        turn_req
    });

    (socket_path, effect_count, server)
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_pipeline_job_for_scope(
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    correction_cap: u32,
) -> (CoworkerJobRecord, String) {
    create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
        fixture,
        binding_id,
        thread_uuid,
        brief_id,
        plan_id,
        tasks,
        task_interfaces,
        approved_write_paths,
        max_parallel,
        approval_budgets,
        coder_budget,
        correction_cap,
        headless_argon_choice(Difficulty::Hard),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    brief_id: &str,
    plan_id: &str,
    tasks: Vec<PlanTask>,
    task_interfaces: BTreeMap<String, TaskInterfaceOwnershipSpec>,
    approved_write_paths: Vec<String>,
    max_parallel: usize,
    approval_budgets: ExecutionBudgets,
    coder_budget: ControllerRoleExecutionBudget,
    correction_cap: u32,
    reviewer_choice: SelectedRoleChoice,
) -> (CoworkerJobRecord, String) {
    Box::pin(async move {
        let pb = project_base(fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let scoped_workflow = ScopedWorkflow::new(
            Arc::clone(&fixture.registry),
            Arc::clone(&fixture.plans),
            binding_id.to_string(),
        )
        .expect("create scoped_workflow for binding");
        let driver_planning_artifacts_dir = planning_artifacts_dir(&fixture.plans_dir);
        let planning_service = CoworkerPlanningService::new(
            fixture.workflow_store.clone(),
            &fixture.registry_dir,
            &driver_planning_artifacts_dir,
        )
        .expect("create planning_service at planning_artifacts_dir");

        let brief = WorkBrief {
            brief_id: brief_id.to_string(),
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            project_base: pb.clone(),
            kind: WorkBriefKind::NewJob,
            summary: format!("Full code-driver pipeline test brief {brief_id}"),
            confirmed_requirements: vec![
                "Dispatch ready roots up to max_parallel".to_string(),
                "Verify, review, correct in scope, integrate, seal downstream inputs, and complete job once".to_string(),
            ],
            proposed_additions: Vec::new(),
            assumptions: vec!["Linux local workspace".to_string()],
            open_questions: Vec::new(),
            accepted_architect_decisions: Vec::new(),
        };

        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                request_key: format!("req_job_{brief_id}"),
                expected_ledger_revision: None,
                work_brief: brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create job");

        let grant = fixture
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                request_key: format!("req_grant_{brief_id}"),
                expected_ledger_revision: None,
                project_base: pb,
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                policy: planning_only_policy(),
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 4,
                    max_orchestrator_calls: 2,
                    max_architect_calls: 2,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms: 1_000,
                expires_at_ms: 60_000,
                operator_attested: true,
            })
            .expect("create planning grant");

        let planned_tasks: Vec<StructuredPlannedTask> = tasks
            .iter()
            .map(|t| {
                let iface = task_interfaces
                    .get(&t.id)
                    .cloned()
                    .unwrap_or(TaskInterfaceOwnershipSpec {
                        owned_interfaces: Vec::new(),
                        consumed_interfaces: Vec::new(),
                    });
                StructuredPlannedTask {
                    task_id: t.id.clone(),
                    title: t.title.clone(),
                    description: t.instruction.clone(),
                    role: t.role,
                    difficulty: t.difficulty,
                    depends_on: t.depends_on.clone(),
                    allowed_write_paths: t.allowed_write_paths.clone(),
                    owned_interfaces: iface.owned_interfaces,
                    consumed_interfaces: iface.consumed_interfaces,
                    model_override: None,
                    tool_override: None,
                }
            })
            .collect();
        let structured_plan = StructuredOrchestratorOutput::Plan {
            schema_version: 1,
            summary: format!(
                "Orchestrator technical implementation plan `{plan_id}` for brief `{brief_id}` with verified read-only supervision and structured task DAG."
            ),
            tasks: planned_tasks,
            check_suggestions: vec![PlannedCheckSuggestion {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test --workspace".to_string(),
                required: true,
            }],
            unconfirmed_proposal_notes: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
        };
        let plan_json = serde_json::to_string(&structured_plan)
            .expect("serialize StructuredOrchestratorOutput::Plan");
        fs::write(&fixture.agy_plan_response_file, &plan_json)
            .expect("write agy_plan_response_file");

        let orch_req = OrchestrateWorkBriefPlanRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            grant_id: grant.grant_id.clone(),
            job_id: job.job_id.clone(),
            request_key: format!("req_orch_{brief_id}"),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            architect_difficulty: Some(Difficulty::Hard),
            plan_id: Some(plan_id.to_string()),
            expected_plan_revision: 0,
            architect_advice_ids: Vec::new(),
            max_architect_rounds: 0,
            current_time_ms: 2_000,
        };
        let plan_outcome = Box::pin(
            planning_service.orchestrate_brief_to_plan(&scoped_workflow, &orch_req),
        )
        .await
        .expect("orchestrate_brief_to_plan via real planning service");
        assert_eq!(
            plan_outcome.reservation.status,
            PlanningReservationStatus::Completed
        );
        let published_outcome = plan_outcome
            .require_published_plan()
            .expect("require published plan from Orchestrator");
        let _verified_artifact = planning_service
            .read_planning_artifact(&published_outcome.planning_result.report_artifact_id)
            .expect("verify persisted planning artifact on disk");

        let job = fixture
            .workflow_store
            .read_binding_ledger(binding_id)
            .expect("read ledger after orchestrate_brief_to_plan")
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find job after orchestrate_brief_to_plan");
        assert_eq!(job.phase, CoworkerJobPhase::AwaitingApproval);

        let approved_task_ids = tasks.iter().map(|t| t.id.clone()).collect();
        let approval_summary = fixture
            .authority
            .approve_at(
                OperatorApprovalRequest {
                    binding_id: binding_id.to_string(),
                    thread_uuid: thread_uuid.to_string(),
                    plan_id: published_outcome.planning_result.plan_id.clone(),
                    plan_revision: published_outcome.planning_result.plan_revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: published_outcome.planning_result.plan_digest.clone(),
                    repository_path: fixture.repo_dir.clone(),
                    base_commit: fixture.base_commit.clone(),
                    approved_task_ids,
                    approved_write_paths,
                    max_parallel,
                    budgets: approval_budgets,
                },
                1_700_000_001_000,
            )
            .expect("approve plan");

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![reviewer_choice.clone()],
            allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: correction_cap,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };

        let bound_job = fixture
            .workflow_store
            .bind_job_approval(
                &BindJobApprovalRequest {
                    binding_id: binding_id.to_string(),
                    thread_uuid: thread_uuid.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    approval_id: approval_summary.approval_id.clone(),
                    policy: pipeline_execution_policy(fixture),
                    controller_envelope: Some(cap_env),
                    task_interfaces,
                },
                &fixture.authority,
            )
            .expect("bind job approval");

        let completeness_receipt = build_independence_completeness_receipt(&bound_job);

        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(WorkflowRole::Coder, coder_budget);
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 6),
        );
        role_budgets.insert(
            WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
        );

        let preview_req = FreezeControllerExecutionEnvelopeRequest {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            job_id: bound_job.job_id.clone(),
            expected_job_revision: bound_job.job_revision,
            policy: pipeline_execution_policy(fixture),
            reviewer_choice,
            closure_choice: headless_argon_choice(Difficulty::Extreme),
            role_budgets,
            max_in_scope_corrections_per_task: correction_cap,
            max_total_corrections_per_job: correction_cap.saturating_mul(2),
            max_total_task_attempts_per_job: 6,
            check_commands: default_check_commands(fixture),
            allow_task_worktree_commits: true,
            allow_run_integration_execution: true,
            default_integration_strategy: IntegrationStrategy::MergeNoFf,
            independence_completeness_receipt: Some(completeness_receipt),
            shadow_jev_policy: None,
            created_at_ms: 1_700_000_100_000,
            operator_attested: false,
            confirm_approval_subject_digest: None,
            confirm_envelope_digest: None,
        };
        let preview: ControllerConfigurationPreview = fixture
            .policy_store
            .preview_execution_envelope(&preview_req, &bound_job, &fixture.authority)
            .expect("preview execution envelope");
        let mut freeze_req = preview_req;
        freeze_req.operator_attested = true;
        freeze_req.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
        freeze_req.confirm_envelope_digest = Some(preview.envelope_digest);
        fixture
            .policy_store
            .record_execution_envelope(&freeze_req, &bound_job, &fixture.authority)
            .expect("record execution envelope");

        (bound_job, approval_summary.approval_id)
    })
    .await
}

fn configure_codex_coder(fixture: &PipelineTestFixture) {
    let snapshot = fixture.registry.snapshot().unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-codex".to_string(),
                    label: "Offline Codex Coder".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: Some(fixture.fake_agy_exe.clone()),
                    enabled: true,
                },
            },
        )
        .unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision + 1,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-luna".to_string(),
                    tool_id: "tool-codex".to_string(),
                    label: "Luna fixture".to_string(),
                    model_arg: "gpt-6-luna".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .unwrap();
    let mut profile = snapshot.profiles[0].clone();
    for assignment in &mut profile.assignments {
        if assignment.role == WorkflowRole::Coder {
            assignment.choice = ModelChoice {
                model_id: "model-luna".to_string(),
                effort: Some("max".to_string()),
            };
        }
    }
    fixture
        .registry
        .apply(
            snapshot.revision + 2,
            RegistryChange::UpsertProfile { profile },
        )
        .unwrap();
}

fn headless_codex_reviewer_choice(difficulty: Difficulty) -> SelectedRoleChoice {
    SelectedRoleChoice {
        tool_id: "tool-codex".to_string(),
        adapter: ToolAdapter::CodexCli,
        route: RoleExecutionRoute::HeadlessWorker,
        model_id: "model-codex-sol".to_string(),
        model_arg: "gpt-6.1-sol".to_string(),
        effort: Some("high".to_string()),
        matched_difficulty: Some(difficulty),
    }
}

/// Pins a fake read-only Codex advisory Reviewer backed by the same deterministic Node provider
/// fixture while leaving the profile Coder choice unchanged. This is the real mixed provider
/// combination when the Coder is the OpenCode v2 Go/max fixture.
fn configure_codex_reviewer(fixture: &PipelineTestFixture) {
    let snapshot = fixture.registry.snapshot().unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-codex".to_string(),
                    label: "Offline Codex Reviewer".to_string(),
                    adapter: ToolAdapter::CodexCli,
                    executable_path: Some(fixture.fake_agy_exe.clone()),
                    enabled: true,
                },
            },
        )
        .unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision + 1,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-codex-sol".to_string(),
                    tool_id: "tool-codex".to_string(),
                    label: "Codex Sol fixture".to_string(),
                    model_arg: "gpt-6.1-sol".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .unwrap();
    let mut profile = snapshot.profiles[0].clone();
    for assignment in &mut profile.assignments {
        if assignment.role == WorkflowRole::Reviewer {
            assignment.difficulty = Some(Difficulty::Hard);
            assignment.choice = ModelChoice {
                model_id: "model-codex-sol".to_string(),
                effort: Some("high".to_string()),
            };
        }
    }
    fixture
        .registry
        .apply(
            snapshot.revision + 2,
            RegistryChange::UpsertProfile { profile },
        )
        .unwrap();
}

/// Registers the exact frozen OpenCode v2 Go/max Coder choice while leaving the profile Reviewer
/// untouched. The fake Node role provider interprets `api --standalone session.create`,
/// `session export --standalone --sanitize`, and `--standalone run ... --format json`, emitting
/// provider-observed telemetry and owned-session metadata for the same Coder calls.
fn configure_go_coder(fixture: &PipelineTestFixture) {
    let snapshot = fixture.registry.snapshot().unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-opencode".to_string(),
                    label: "Offline OpenCode Go Coder".to_string(),
                    adapter: ToolAdapter::OpencodeV2,
                    executable_path: Some(fixture.fake_agy_exe.clone()),
                    enabled: true,
                },
            },
        )
        .unwrap();
    fixture
        .registry
        .apply(
            snapshot.revision + 1,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-deepseek".to_string(),
                    tool_id: "tool-opencode".to_string(),
                    label: "DeepSeek Flash fixture".to_string(),
                    model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                    efforts: vec!["max".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .unwrap();
    let mut profile = snapshot.profiles[0].clone();
    for assignment in &mut profile.assignments {
        if assignment.role == WorkflowRole::Coder {
            assignment.difficulty = Some(Difficulty::Hard);
            assignment.choice = ModelChoice {
                model_id: "model-deepseek".to_string(),
                effort: Some("max".to_string()),
            };
        }
    }
    fixture
        .registry
        .apply(
            snapshot.revision + 2,
            RegistryChange::UpsertProfile { profile },
        )
        .unwrap();
}

fn pipeline_execution_policy(fixture: &PipelineTestFixture) -> ExecutionPolicy {
    let mut policy = execution_policy();
    if fixture
        .registry
        .snapshot()
        .unwrap()
        .models
        .iter()
        .any(|m| m.id == "model-luna")
    {
        policy.allow_rules.push(PolicyAllowRule {
            tool_id: "tool-codex".to_string(),
            model_id: "model-luna".to_string(),
            efforts: vec!["max".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Coder],
        });
    }
    if fixture
        .registry
        .snapshot()
        .unwrap()
        .tools
        .iter()
        .any(|t| t.id == "tool-opencode")
    {
        policy.allow_rules.push(PolicyAllowRule {
            tool_id: "tool-opencode".to_string(),
            model_id: "model-deepseek".to_string(),
            efforts: vec!["max".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Coder],
        });
    }
    if fixture
        .registry
        .snapshot()
        .unwrap()
        .profiles
        .iter()
        .any(|profile| {
            profile.assignments.iter().any(|assignment| {
                assignment.role == WorkflowRole::Reviewer
                    && assignment.choice.model_id == "model-codex-sol"
            })
        })
    {
        policy.allow_rules.push(PolicyAllowRule {
            tool_id: "tool-codex".to_string(),
            model_id: "model-codex-sol".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![WorkflowRole::Reviewer],
        });
    }
    policy
}

// Commits a tracked ignore rule before approval and leaves an ignored primary cache artifact
// in place for every pipeline transition through post-Closure source verification.
fn install_tracked_primary_cache_ignore_and_artifact(fixture: &mut PipelineTestFixture) -> PathBuf {
    fs::write(fixture.repo_dir.join(".gitignore"), "target/\n.local/\n")
        .expect("write tracked primary cache ignore rule");
    run_sanitized_git(
        &fixture.repo_dir,
        &["add", ".gitignore"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("stage primary cache ignore rule");
    run_sanitized_git(
        &fixture.repo_dir,
        &["commit", "-m", "track primary cache ignore rule", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("commit primary cache ignore rule");
    fixture.base_commit =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary base commit with tracked ignore rule");
    fixture.base_tree_sha = resolve_exact_tree_sha(
        &fixture.repo_dir,
        &fixture.base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve primary base tree with tracked ignore rule");

    let cache_file = fixture.repo_dir.join("target/cache.bin");
    fs::create_dir_all(cache_file.parent().expect("primary cache parent"))
        .expect("create ignored primary cache dir");
    fs::write(&cache_file, b"cached-primary-build-output")
        .expect("write ignored primary cache artifact");
    cache_file
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_code_driver_full_automatic_pipeline_runs_parallel_roots_reviewer_correction_dependent_and_single_closure_to_completed(
) {
    assert_full_pipeline(false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codex_luna_full_frozen_pipeline_parallel_roots_correction_dependent_and_single_closure() {
    assert_full_pipeline(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn go_coder_codex_sol_reviewer_full_frozen_pipeline_parallel_roots_correction_dependent_and_single_closure(
) {
    assert_full_pipeline(false, true, true).await;
}

#[allow(clippy::too_many_lines)]
async fn assert_full_pipeline(codex_coder: bool, codex_reviewer: bool, go_coder: bool) {
    assert!(
        !codex_coder || !go_coder,
        "the legacy Codex Coder and OpenCode Go Coder fixtures are mutually exclusive"
    );
    Box::pin(async {
        let mut fixture = setup_fixture();
        let primary_cache_file = install_tracked_primary_cache_ignore_and_artifact(&mut fixture);
        if codex_coder { configure_codex_coder(&fixture); }
        if go_coder { configure_go_coder(&fixture); }
        if codex_reviewer { configure_codex_reviewer(&fixture); }
        let reviewer_choice = if codex_reviewer {
            headless_codex_reviewer_choice(Difficulty::Hard)
        } else {
            headless_argon_choice(Difficulty::Hard)
        };
        let state_dir = fixture.root.join("runtime");
        fs::create_dir_all(&state_dir).expect("create runtime state_dir");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 runtime state_dir");

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
        let binding = supervisor_client
            .create_binding(THREAD_UUID_A, "code-driver-full-pipeline-cli-test")
            .await
            .expect("create actual supervisor binding");
        let binding_id = binding.binding_id;
        let thread_uuid = binding.thread_uuid;

        let tasks = vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Root task A with Reviewer correction".to_string(),
                instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-root-b".to_string(),
                title: "Root task B independent".to_string(),
                instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-c".to_string(),
                title: "Dependent task C requiring corrected A101 and B200".to_string(),
                instruction:
                    "CTRL_TASK_C_REQUIRES_A101 implement src/c.rs after verifying corrected 101 in src/a.rs and 200 in src/b.rs"
                        .to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".to_string(), "task-root-b".to_string()],
                allowed_write_paths: vec!["src/c.rs".to_string()],
            },
        ];

        let mut ifaces = BTreeMap::new();
        ifaces.insert(
            "task-root-a".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_a".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-root-b".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_b".to_string()],
                consumed_interfaces: Vec::new(),
            },
        );
        ifaces.insert(
            "task-dep-c".to_string(),
            TaskInterfaceOwnershipSpec {
                owned_interfaces: vec!["iface_c".to_string()],
                consumed_interfaces: vec!["iface_a".to_string(), "iface_b".to_string()],
            },
        );

        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
            &fixture,
            &binding_id,
            &thread_uuid,
            "brf_full_drv_dag_3",
            "plan-full-drv-dag-3",
            tasks,
            ifaces,
            vec![
                "src/a.rs".to_string(),
                "src/b.rs".to_string(),
                "src/c.rs".to_string(),
            ],
            2, // max_parallel = 2
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 5),
            1,
            reviewer_choice.clone(),
        )
        .await;

        let (notice_socket_path, notice_effect_count, notice_server) =
            spawn_single_coworker_notice_ws_server(&fixture, &thread_uuid);
        let notice_root = coworker_notice_dir(&fixture.plans_dir);
        assert_eq!(notice_root, fixture.plans_dir.join("coworker-code-driver/native-notices"));
        let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id)
            .expect("construct ownership key");
        let envelope_digest = fixture
            .policy_store
            .get_job_runtime_readonly(&binding_id, &job.job_id)
            .expect("read job runtime readonly")
            .expect("frozen job runtime present")
            .envelope
            .envelope_digest;
        let notice_config = CoworkerNoticeConfig::new(
            ownership_key.clone(),
            envelope_digest.clone(),
            NativeToolOutputConfig::new_user_message_steer(
                notice_socket_path,
                thread_uuid.clone(),
                fixture.repo_dir.clone(),
            ),
        )
        .expect("build scoped coworker notice config");
        CoworkerNoticeStore::new(notice_root.clone())
            .expect("open coworker notice store")
            .configure(notice_config)
            .expect("configure coworker notice target");

        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

        // Hold `task-root-a` attempt 1 Reviewer on a gate and configure its verdict to
        // `correct_in_scope`, while `task-root-b` runs in parallel to `Integrated`.
        let hold_gate_a = fixture.root.join("hold_reviewer_task-root-a.gate");
        let entered_marker_a = fixture.root.join("reviewer_entered_task-root-a.marker");
        let exited_marker_a = fixture.root.join("reviewer_exited_task-root-a.marker");
        fs::write(&hold_gate_a, "{\"fallback_timeout_ms\":180000}\n")
            .expect("create hold gate for task-root-a Reviewer");

        let attempt_1_override_path = fixture
            .root
            .join("reviewer_override_task-root-a_attempt_1.json");
        fs::write(
            &attempt_1_override_path,
            serde_json::to_string(&serde_json::json!({
                "outcome": "correct_in_scope",
                "findings": ["a() in src/a.rs returns 100 instead of 101"],
                "required_in_scope_fixes": ["Update pub fn a() in src/a.rs to return 101"],
                "summary": "Attempt 1 requires in-scope correction in src/a.rs from 100 to 101."
            }))
            .expect("serialize attempt 1 reviewer override"),
        )
        .expect("write reviewer_override_task-root-a_attempt_1.json");

        // 1. Default `code-driver ensure` (no mode flag) selects `full_automatic_pipeline`
        // and returns while the detached owner process continues driving the pipeline.
        let ensure_first = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(ensure_first["disposition"], "spawned_new_owner");
        assert_eq!(ensure_first["status"]["owner_state"], "live_verified_owner");
        assert_eq!(
            ensure_first["status"]["execution_mode"],
            "full_automatic_pipeline"
        );
        assert_eq!(
            ensure_first["status"]["pipeline_boundary"],
            "full_automatic_pipeline_through_single_closure"
        );
        // Readiness proves physical ownership; the logical lease is checked below at the actual in-flight Reviewer.
        let owner_instance_id = ensure_first["status"]["verified_owner_summary"]["instance_id"]
            .as_str()
            .expect("verified_owner_summary.instance_id")
            .to_string();
        assert!(owner_instance_id.starts_with("cwo_"));

        // 2. Wait until `task-root-a` Reviewer is held in flight AND `task-root-b` has
        // independently completed Coder -> check -> Reviewer -> per-task integration.
        let mid_deadline = Instant::now() + Duration::from_secs(90);
        let status_mid = loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            let b_integrated = current["tasks"]
                .as_array()
                .expect("status.tasks array")
                .iter()
                .any(|t| t["task_id"] == "task-root-b" && t["stage"] == "integrated");
            let a_in_review = current["pipeline_report"]["in_flight_stages"]
                .as_array()
                .is_some_and(|stages| {
                    stages.iter().any(|s| {
                        s["task_id"] == "task-root-a"
                            && s["attempt_no"] == 1
                            && s["kind"] == "task_review"
                    })
                });
            if entered_marker_a.is_file() && b_integrated && a_in_review {
                break current;
            }
            assert!(
                Instant::now() < mid_deadline,
                "timed out waiting for held task-root-a Reviewer + integrated task-root-b; {}; last status: {current}",
                format_fixture_counter_diagnostics(&fixture),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert!(
            !exited_marker_a.is_file(),
            "held task-root-a Reviewer must still be held before gate removal"
        );
        assert_eq!(status_mid["owner_state"], "live_verified_owner");
        assert_eq!(status_mid["lease_active"], true);
        assert_eq!(
            status_mid["verified_owner_summary"]["instance_id"],
            owner_instance_id
        );
        assert_eq!(status_mid["execution_mode"], "full_automatic_pipeline");
        assert_eq!(status_mid["pipeline_status"], "running");
        assert_ne!(status_mid["job_phase"], "completed");

        // 3. Repeated status polling while Reviewer is held returns compact cached evidence
        // without charging or invoking additional models/checks.
        let coder_mid = read_invocation_count(&fixture.agy_coder_count_file);
        let reviewer_mid = read_invocation_count(&fixture.agy_reviewer_count_file);
        let check_mid = read_invocation_count(&fixture.check_count_file);
        assert_eq!(coder_mid, 2);
        assert_eq!(reviewer_mid, 2);
        assert_eq!(check_mid, 2);

        let status_mid_poll_2 = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(status_mid_poll_2["owner_state"], "live_verified_owner");
        assert_eq!(
            status_mid_poll_2["verified_owner_summary"]["instance_id"],
            owner_instance_id
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), coder_mid);
        assert_eq!(
            read_invocation_count(&fixture.agy_reviewer_count_file),
            reviewer_mid
        );
        assert_eq!(read_invocation_count(&fixture.check_count_file), check_mid);

        // 4. Duplicate CLI `ensure` and programmatic `driver.ensure_owner(..., None)` reuse
        // the exact same live owner without spawning a second owner or calling providers.
        let dup_cli = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(dup_cli["disposition"], "reused_live_owner");
        assert_eq!(
            dup_cli["status"]["verified_owner_summary"]["instance_id"],
            owner_instance_id
        );

        let programmatic_driver = CoworkerCodeDriver::open_from_dirs(
            &state_dir,
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
        )
        .expect("open programmatic CoworkerCodeDriver");
        let dup_api = Box::pin(programmatic_driver.ensure_owner(
            &binding_id,
            &thread_uuid,
            &job.job_id,
            None,
        ))
        .await
        .expect("programmatic ensure_owner(None) while live owner active");
        assert_eq!(
            dup_api.disposition,
            CoworkerDriverEnsureDisposition::ReusedLiveOwner
        );
        assert_eq!(
            dup_api
                .status
                .verified_owner_summary
                .as_ref()
                .map(|s| s.instance_id.as_str()),
            Some(owner_instance_id.as_str())
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), coder_mid);
        assert_eq!(
            read_invocation_count(&fixture.agy_reviewer_count_file),
            reviewer_mid
        );

        // 5. Release the held Reviewer gate on `task-root-a` and wait for the detached owner
        // to drive bounded correction (attempt 2), original integration, dependency seal,
        // dependent `task-dep-c`, combined checks, and ONE Closure to `Completed`.
        fs::remove_file(&hold_gate_a).expect("release hold_reviewer_task-root-a.gate");

        let complete_deadline = Instant::now() + Duration::from_secs(300);
        let status_completed = loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            if current["job_phase"] == "completed" && current["owner_state"] == "no_active_owner" {
                break current;
            }
            assert!(
                Instant::now() < complete_deadline,
                "timed out waiting for detached full pipeline owner to reach completed + no_active_owner; {}; last status: {current}",
                format_fixture_counter_diagnostics(&fixture),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_eq!(status_completed["job_phase"], "completed");
        assert_eq!(status_completed["pipeline_status"], "completed");
        // CoworkerControllerPhase1Status has no `completed` variant; its terminal integrated
        // value is `all_tasks_integrated`, while job_phase/pipeline_status prove Closure completion.
        assert_eq!(status_completed["overall_status"], "all_tasks_integrated");
        assert_eq!(
            status_completed["execution_mode"],
            "full_automatic_pipeline"
        );
        assert_eq!(
            status_completed["pipeline_boundary"],
            "full_automatic_pipeline_through_single_closure"
        );
        assert_eq!(status_completed["owner_state"], "no_active_owner");
        assert_eq!(status_completed["lease_active"], false);
        assert_eq!(status_completed["host_lock_held"], false);
        assert_eq!(status_completed["blocked_on_unknown_effect"], false);
        assert_eq!(
            status_completed["pipeline_report"]["status"],
            "completed"
        );
        assert_eq!(
            status_completed["pipeline_report"]["completion_summary"]["status"],
            "completed"
        );
        assert_eq!(
            status_completed["pipeline_report"]["completion_summary"]["closure_verdict_kind"],
            "accepted"
        );
        assert!(
            status_completed["pipeline_report"]["completion_summary"]["combined_candidate_id"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
        );

        // Verify exact role and check counts:
        // - 1 Orchestrator call
        // - 4 Coder calls (A1, B1, derived A2, dependent C1)
        // - 4 Reviewer calls (A1 CorrectInScope, B1 Accept, A2 Accept, C1 Accept)
        // - 1 Closure call (Accepted)
        // - 5 check runs (A1, B1, A2, C1, + 1 combined candidate check)
        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 4);
        assert_eq!(
            read_invocation_count(&fixture.root.join("agy_derived_coder_count.txt")),
            1
        );
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 4);
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-a.txt")),
            2
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-root-b.txt")),
            1
        );
        assert_eq!(
            read_invocation_count(&fixture.root.join("reviewer_count_task-dep-c.txt")),
            1
        );
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 5);

        // Verify physical FD lock and disposable host lock were released on completion.
        let actual_driver_ownership_store = CoworkerJobOwnershipStore::new(
            threadlane_native::coworker_driver::coworker_driver_dir(&fixture.plans_dir),
        )
        .expect("open actual CLI code-driver ownership store");
        assert!(
            !matches!(
                actual_driver_ownership_store
                    .inspect_job_lock(&ownership_key)
                    .expect("inspect driver lock after completion"),
                CoworkerJobLockState::HeldByActiveProcess { .. }
            ),
            "physical job lock must be released after completion"
        );
        assert!(
            !threadlane_host::is_host_lock_held(&fixture.sessions_dir.join(&approval_id))
                .expect("inspect host lock after completion"),
            "disposable host lock must be released after completion"
        );

        let turn_req = tokio::time::timeout(Duration::from_secs(20), notice_server)
            .await
            .expect("wait for coworker completion notice ws callback")
            .expect("join coworker notice ws server");
        assert_eq!(notice_effect_count.load(Ordering::SeqCst), 1);
        let expected_notice =
            format_coworker_job_wakeup_message(&job.job_id).expect("expected notice");
        assert_eq!(turn_req["id"], json!(4));
        assert_eq!(turn_req["method"], json!("turn/start"));
        assert_eq!(
            turn_req["params"],
            json!({
                "threadId": thread_uuid,
                "input": [{ "type": "text", "text": expected_notice }]
            })
        );
        assert!(turn_req["params"].get("toolOutput").is_none());
        assert!(turn_req["params"].get("model").is_none());
        assert!(turn_req["params"].get("effort").is_none());
        let serialized_turn = turn_req.to_string();
        assert!(serialized_turn.contains(&job.job_id));
        assert!(serialized_turn.contains("execution_status"));
        assert!(serialized_turn.contains("untrusted"));
        assert!(!serialized_turn.contains("Full code-driver pipeline test brief"));
        assert!(!serialized_turn.contains("CTRL_TASK_A"));
        assert!(!serialized_turn.contains("apr_"));
        assert!(!serialized_turn.contains("wcl_"));
        assert!(!serialized_turn.to_ascii_lowercase().contains("batch"));

        let notice_inspected =
            CoworkerNoticeStore::inspect(&notice_root, &ownership_key, &envelope_digest)
                .expect("inspect coworker notice after completion");
        assert!(notice_inspected.configured);
        assert_eq!(notice_inspected.attempt_count, 1);
        assert_eq!(
            notice_inspected.latest.as_ref().expect("latest notice").status,
            NoticeDeliveryStatus::Submitted
        );

        // Verify final integrated checkout has corrected 101 in src/a.rs, 200 in src/b.rs, and 300 in src/c.rs,
        // while source repo remains untouched at base_commit.
        let final_wt = PathBuf::from(
            status_completed["pipeline_report"]["completion_summary"]["integration_checkout_path"]
                .as_str()
                .expect("integration_checkout_path string"),
        );
        assert_eq!(
            fs::read_to_string(final_wt.join("src/a.rs")).expect("read aggregated src/a.rs"),
            "pub fn a() -> u32 { 101 }\n"
        );
        assert_eq!(
            fs::read_to_string(final_wt.join("src/b.rs")).expect("read aggregated src/b.rs"),
            "pub fn b() -> u32 { 200 }\n"
        );
        assert_eq!(
            fs::read_to_string(final_wt.join("src/c.rs")).expect("read aggregated src/c.rs"),
            "pub fn c() -> u32 { 300 }\n"
        );
        assert_eq!(
            resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("source repo HEAD"),
            fixture.base_commit
        );
        assert_eq!(
            fs::read(&primary_cache_file).expect("read ignored primary cache artifact"),
            b"cached-primary-build-output"
        );

        // 6. Post-completion idempotent `ensure`, `ensure_owner(None)`, and `reconcile`
        // return `Completed` from persisted evidence without starting the host or charging models.
        let ensure_after_complete = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(
            ensure_after_complete["disposition"],
            "already_quiescent_verified"
        );
        assert_eq!(
            ensure_after_complete["status"]["job_phase"],
            "completed"
        );
        assert_eq!(
            ensure_after_complete["status"]["pipeline_status"],
            "completed"
        );
        assert_eq!(
            ensure_after_complete["status"]["overall_status"],
            "all_tasks_integrated"
        );
        assert_eq!(ensure_after_complete["status"]["host_lock_held"], false);

        let api_after_complete = Box::pin(programmatic_driver.ensure_owner(
            &binding_id,
            &thread_uuid,
            &job.job_id,
            None,
        ))
        .await
        .expect("programmatic ensure_owner(None) after completion");
        assert_eq!(
            api_after_complete.disposition,
            CoworkerDriverEnsureDisposition::AlreadyQuiescentVerified
        );
        assert_eq!(api_after_complete.status.job_phase, CoworkerJobPhase::Completed);

        let reconcile_after_complete = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "reconcile", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(reconcile_after_complete["job_phase"], "completed");
        assert_eq!(reconcile_after_complete["pipeline_status"], "completed");
        assert_eq!(reconcile_after_complete["overall_status"], "all_tasks_integrated");
        assert_eq!(reconcile_after_complete["host_lock_held"], false);

        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 4);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 4);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 5);
        let notice_after_reconcile =
            CoworkerNoticeStore::inspect(&notice_root, &ownership_key, &envelope_digest)
                .expect("inspect coworker notice after post-completion ensure/reconcile");
        assert!(notice_after_reconcile.configured);
        assert_eq!(notice_after_reconcile.attempt_count, 1);
        assert_eq!(
            notice_after_reconcile
                .latest
                .as_ref()
                .expect("latest notice after reconcile")
                .status,
            NoticeDeliveryStatus::Submitted
        );
        assert_eq!(notice_effect_count.load(Ordering::SeqCst), 1);

        if codex_coder {
            let approval = fixture.authority.get_for_operator(&approval_id).unwrap();
            assert!(approval.tasks.iter().all(|t| t.is_frozen_headless_worker() && t.provider == Some(threadlane_workers::WorkerProvider::CodexCli)));
            let runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id).unwrap().unwrap();
            assert_eq!(runtime.attempt_artifacts.len(), 4);
            let session_dir = fixture.sessions_dir.join(&approval_id);
            let state = threadlane_core::observation::read_core_state_readonly(&session_dir.join("runtime-state/state.db")).unwrap();
            for row in &state.workers {
                let worker = row.to_worker_snapshot().expect("complete worker snapshot");
                let task = approval.tasks.iter().find(|t| t.profile_id.as_deref() == Some(worker.profile_id.as_str())).unwrap();
                threadlane_host::load_verified_codex_task_report(&approval, &session_dir, task, &worker).unwrap();
                let mut foreign = approval.clone(); foreign.repository_path = fixture.root.clone();
                assert!(threadlane_host::load_verified_codex_task_report(&foreign, &session_dir, task, &worker).is_err());
                let mut wrong_worker = worker.clone(); wrong_worker.run_id = "run_foreign".to_string();
                assert!(threadlane_host::load_verified_codex_task_report(&approval, &session_dir, task, &wrong_worker).is_err());
                let mut wrong_worker = worker.clone(); wrong_worker.profile_digest = "a".repeat(64);
                assert!(threadlane_host::load_verified_codex_task_report(&approval, &session_dir, task, &wrong_worker).is_err());
                let mut wrong_scope = task.clone(); wrong_scope.allowed_write_paths = vec!["src/".to_string()];
                assert!(threadlane_host::load_verified_codex_task_report(&approval, &session_dir, &wrong_scope, &worker).is_err());
                let mut missing = worker.clone(); missing.outcome.as_mut().unwrap().report_artifact_id = None;
                assert!(threadlane_host::load_verified_codex_task_report(&approval, &session_dir, task, &missing).is_err());
            }
            for artifact in runtime.attempt_artifacts.values() {
                assert_eq!(artifact.observed_model, None);
                assert_eq!(artifact.observed_effort, None);
                let report = artifact.codex_launch_report.as_ref().expect("separate launch evidence");
                assert!(report.evidence.retained_events.is_empty());
                assert!(report.redacted_stderr_summary.is_none());
                let task = approval.tasks.iter().find(|t| t.task_id == artifact.task_id).unwrap();
                let choice = runtime.envelope.task_coder_choices.get(&artifact.task_id).unwrap();
                assert!(artifact.has_matching_coder_evidence(task, choice));
                let subject = artifact.review_subject.as_ref().expect("reviewed task subject");
                assert!(subject.policy.has_verified_codex_task_launch());
                assert_eq!(subject.policy.observed_model, None);
                subject.verify_integrity().unwrap();
                if let Some(receipt) = artifact.review_receipt.as_ref() {
                    assert!(receipt.policy.has_verified_codex_task_launch());
                    assert_eq!(receipt.policy.observed_model, None);
                    receipt.verify_integrity().unwrap();
                } else {
                    assert_eq!((&*artifact.task_id, artifact.attempt_no), ("task-root-a", 1));
                }
                let manifest = threadlane_workers::load_and_verify_manifest(&subject.work_area_root, &artifact.task_worktree_id).unwrap();
                artifact.verify_coder_manifest_evidence(task, &manifest, &fixture.plans_dir).unwrap();
                let mut wrong_source = artifact.clone(); wrong_source.codex_source_report_sha256 = Some("a".repeat(64));
                assert!(wrong_source.verify_coder_manifest_evidence(task, &manifest, &fixture.plans_dir).is_err());
                let mut missing = artifact.clone(); missing.codex_launch_report = None;
                assert!(!missing.has_matching_coder_evidence(task, choice));
                let mut wrong = artifact.clone(); wrong.codex_launch_report.as_mut().unwrap().requested_model.base_model = "gpt-wrong".to_string();
                assert!(!wrong.has_matching_coder_evidence(task, choice));
                let mut wrong = artifact.clone(); wrong.codex_launch_report.as_mut().unwrap().executed_executable = Some(PathBuf::from("/wrong/executable"));
                assert!(!wrong.has_matching_coder_evidence(task, choice));
                let mut wrong = artifact.clone(); wrong.codex_launch_report.as_mut().unwrap().constructed_argv[17] = report.constructed_argv[17].replace("src/a.rs", "src/");
                if wrong.codex_launch_report != artifact.codex_launch_report { assert!(!wrong.has_matching_coder_evidence(task, choice)); }
            }
        }

        if go_coder {
            let approval = fixture.authority.get_for_operator(&approval_id).unwrap();
            assert!(approval.tasks.iter().all(|t| t.is_frozen_headless_worker()
                && t.provider == Some(threadlane_workers::WorkerProvider::OpenCodeV2)
                && t.adapter == ToolAdapter::OpencodeV2
                && t.model_arg == "opencode-go/deepseek-v4.1-flash"
                && t.effort.as_deref() == Some("max")));
            let runtime = fixture
                .policy_store
                .get_job_runtime_readonly(&binding_id, &job.job_id)
                .unwrap()
                .unwrap();
            assert_eq!(runtime.attempt_artifacts.len(), 4);
            for artifact in runtime.attempt_artifacts.values() {
                assert_eq!(artifact.observed_provider, "opencode_v2");
                assert_eq!(
                    artifact.observed_model.as_deref(),
                    Some("opencode-go/deepseek-v4.1-flash")
                );
                assert_eq!(artifact.observed_effort.as_deref(), Some("max"));
                assert!(artifact.codex_launch_report.is_none());
                let task = approval
                    .tasks
                    .iter()
                    .find(|t| t.task_id == artifact.task_id)
                    .unwrap();
                let choice = runtime
                    .envelope
                    .task_coder_choices
                    .get(&artifact.task_id)
                    .unwrap();
                assert_eq!(choice.adapter, ToolAdapter::OpencodeV2);
                assert_eq!(choice.model_arg, "opencode-go/deepseek-v4.1-flash");
                assert_eq!(choice.effort.as_deref(), Some("max"));
                assert!(artifact.has_matching_coder_evidence(task, choice));
                let subject = artifact.review_subject.as_ref().expect("reviewed task subject");
                assert_eq!(subject.policy.requested_provider.as_deref(), Some("opencode_v2"));
                assert_eq!(
                    subject.policy.observed_model.as_deref(),
                    Some("opencode-go/deepseek-v4.1-flash")
                );
                assert_eq!(subject.policy.observed_effort.as_deref(), Some("max"));
                subject.verify_integrity().unwrap();
            }
            assert_eq!(runtime.derived_attempts.len(), 1);
            let receipt = &runtime.derived_attempts[0];
            assert_eq!(receipt.task_id, "task-root-a");
            assert_eq!(receipt.attempt_no, 2);
            let frozen_choice = runtime
                .envelope
                .task_coder_choices
                .get(&receipt.task_id)
                .expect("frozen derived Coder choice");
            assert_eq!(&receipt.coder_choice, frozen_choice);
            let derived_effect = runtime
                .pending_effects
                .values()
                .find(|effect| {
                    effect.kind == PendingControllerEffectKind::CoderDispatch
                        && effect.task_id.as_deref() == Some("task-root-a")
                        && effect.attempt_no == Some(2)
                })
                .expect("derived CoderDispatch effect for task-root-a attempt 2");
            assert_eq!(
                derived_effect.status,
                PendingControllerEffectStatus::EffectConfirmed
            );
            let report = read_persisted_derived_task_execution_report(
                &fixture.plans_dir,
                &derived_effect.effect_id,
            )
            .expect("read persisted derived Go execution report")
            .expect("persisted derived Go execution report exists");
            assert_eq!(report.attempt_no, 2);
            assert_eq!(report.coder_choice, receipt.coder_choice);
            assert_eq!(report.observed_provider, "opencode_v2");
            assert_eq!(
                report.observed_model.as_deref(),
                Some("opencode-go/deepseek-v4.1-flash")
            );
            assert_eq!(report.observed_effort.as_deref(), Some("max"));
            let worker_report = &report.execution_report.worker_report;
            assert_eq!(
                worker_report.provider,
                threadlane_workers::WorkerProvider::OpenCodeV2
            );
            assert_eq!(
                worker_report.requested_model.base_model,
                "opencode-go/deepseek-v4.1-flash"
            );
            assert_eq!(worker_report.requested_model.effort.as_deref(), Some("max"));
            assert_eq!(worker_report.requested_session_id, None);
            let metadata = worker_report
                .validated_opencode_metadata()
                .expect("validated owned OpenCode metadata for derived attempt");
            assert_eq!(metadata.observed_model, "opencode-go/deepseek-v4.1-flash");
            assert_eq!(metadata.observed_effort.as_deref(), Some("max"));
            assert_eq!(metadata.assistant_message_count, 1);
            assert_eq!(
                worker_report.allocated_session_id.as_deref(),
                Some(metadata.session_id.as_str())
            );
            assert!(worker_report.opencode_metadata_error.is_none());
        }

        if codex_reviewer {
            let runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id).unwrap().unwrap();
            let mut review_effect_count = 0;
            for effect in runtime.pending_effects.values() {
                if effect.kind == PendingControllerEffectKind::ReviewerInvocation
                    && effect.status == PendingControllerEffectStatus::EffectConfirmed
                {
                    review_effect_count += 1;
                    let report = read_persisted_task_review_report(&fixture.plans_dir, &effect.effect_id)
                        .expect("read persisted Codex Reviewer report")
                        .expect("persisted Codex Reviewer report exists");
                    assert_eq!(report.provider, threadlane_workers::WorkerProvider::CodexCli);
                    assert_eq!(report.requested_model.base_model, "gpt-6.1-sol");
                    assert_eq!(report.requested_model.effort.as_deref(), Some("high"));
                    assert_eq!(report.observed_model, None);
                    assert_eq!(report.observed_effort, None);
                    assert!(report.execution().has_pinned_codex_read_only_launch_config());
                }
            }
            assert_eq!(review_effect_count, 4, "one persisted report per Reviewer call");
        }

        // Reconstruct only the dependent task's fenced, pre-verification boundary from its
        // genuine retained initial Coder and dependency-preparation records. Never persist it.
        let mut dependent_job = fixture.workflow_store.read_binding_ledger(&binding_id).unwrap()
            .jobs.into_iter().find(|record| record.job_id == job.job_id).unwrap();
        let mut dependent_runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id).unwrap().unwrap();
        dependent_job.phase = CoworkerJobPhase::Held;
        let task = dependent_job.tasks.iter_mut().find(|task| task.task_id == "task-dep-c").unwrap();
        assert_eq!(task.current_attempt_no, 1);
        assert!(!task.accepted_predecessor_receipts.is_empty());
        task.stage = CoworkerTaskStage::Held;
        task.stage_before_hold = Some(CoworkerTaskStage::Running);
        task.active_candidate_id = None;
        task.accepted_candidate_id = None;
        task.candidates.clear();
        task.attempts.retain(|attempt| attempt.role == WorkflowRole::Coder);
        let coder = task.attempts.last().unwrap();
        let artifacts = dependent_runtime.attempt_artifacts.get_mut(&ControllerJobRuntimeState::artifact_key("task-dep-c", 1)).unwrap();
        assert!(!artifacts.is_derived_attempt);
        artifacts.committed_head_commit = coder.head_commit.clone();
        artifacts.committed_tree_sha = coder.tree_sha.clone();
        assert!(artifacts.committed_head_commit.is_some());
        assert!(artifacts.committed_tree_sha.is_some());
        artifacts.candidate_id = None;
        artifacts.check_evidence.clear();
        artifacts.test_evidence_summary = None;
        artifacts.review_subject = None;
        artifacts.reviewer_response_digest = None;
        artifacts.review_receipt = None;
        artifacts.integration_receipt = None;
        dependent_job.outbox.retain(|outbox| outbox.task_id != "task-dep-c" || matches!(outbox.kind,
            threadlane_native::coworker_workflow::OutboxDispatchKind::DependencyPreparation
                | threadlane_native::coworker_workflow::OutboxDispatchKind::TaskAttemptDispatch));
        dependent_runtime.pending_effects.retain(|_, effect| effect.task_id.as_deref() != Some("task-dep-c") || effect.kind == PendingControllerEffectKind::CoderDispatch);
        let prep_index = dependent_job.outbox.iter().position(|outbox| outbox.task_id == "task-dep-c" && outbox.kind == threadlane_native::coworker_workflow::OutboxDispatchKind::DependencyPreparation).expect("retained genuine dependency preparation");
        assert_eq!(dependent_job.outbox[prep_index].attempt_no, 1);
        assert_eq!(dependent_job.outbox[prep_index].status, OutboxDispatchStatus::DispatchedEffectConfirmed);
        let projection_owner: threadlane_native::coworker_driver::CoworkerDriverStatusReport = serde_json::from_value(status_mid.clone()).unwrap();
        let projection_approval = fixture.authority.get_for_operator(&approval_id).unwrap();
        let project_dependent = |snapshot_job: &CoworkerJobRecord| {
            threadlane_native::coworker_driver::evaluate_live_coder_projection_snapshot_from_job_and_runtime(
                snapshot_job, &dependent_runtime, &projection_approval, &ownership_key,
                projection_owner.verified_owner_summary.as_ref().unwrap(), projection_owner.execution_mode,
                Some(HostPhase::Stopped), false, &[], &[], &fixture.plans_dir, None, &[],
            )
        };
        assert!(!project_dependent(&dependent_job), "completed pre-Coder dependency preparation must not invent Running/Unknown for a fenced terminal dependent Coder");
        for status in [OutboxDispatchStatus::ReservedBeforeEffect, OutboxDispatchStatus::EffectUnknown, OutboxDispatchStatus::ReconciledNotExecuted] {
            let mut unproven_preparation = dependent_job.clone();
            unproven_preparation.outbox[prep_index].status = status;
            assert!(project_dependent(&unproven_preparation), "unproven dependency preparation must remain blocked: {status:?}");
        }
        let mut later_preparation = dependent_job.clone();
        later_preparation.outbox[prep_index].updated_at_job_revision = later_preparation.job_revision;
        assert!(project_dependent(&later_preparation), "post-Coder preparation history must not exempt stale cached Coder state");
        let mut future_preparation = dependent_job.clone();
        future_preparation.outbox[prep_index].attempt_no = 2;
        assert!(project_dependent(&future_preparation), "future-attempt preparation must not exempt the current Coder");

        supervisor.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_cancel_during_owned_in_flight_reviewer_drains_fences_promotion_and_prevents_replay(
) {
    Box::pin(async {
        let fixture = setup_fixture();
        let state_dir = fixture.root.join("runtime");
        fs::create_dir_all(&state_dir).expect("create runtime state_dir");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 runtime state_dir");

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
        let binding = supervisor_client
            .create_binding(THREAD_UUID_A, "code-driver-cancel-in-flight-reviewer-test")
            .await
            .expect("create actual supervisor binding");
        let binding_id = binding.binding_id;
        let thread_uuid = binding.thread_uuid;

        let tasks = vec![PlanTask {
            id: "task-root-a".to_string(),
            title: "Root task A cancelled during in-flight Reviewer".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }];

        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
            &fixture,
            &binding_id,
            &thread_uuid,
            "brf_full_drv_cancel_rev",
            "plan-full-drv-cancel-rev",
            tasks,
            BTreeMap::new(),
            vec!["src/a.rs".to_string()],
            1,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            1,
            headless_argon_choice(Difficulty::Hard),
        )
        .await;

        let hold_gate_a = fixture.root.join("hold_reviewer_task-root-a.gate");
        let entered_marker_a = fixture.root.join("reviewer_entered_task-root-a.marker");
        let exited_marker_a = fixture.root.join("reviewer_exited_task-root-a.marker");
        fs::write(&hold_gate_a, "{\"fallback_timeout_ms\":180000}\n")
            .expect("create hold gate for task-root-a Reviewer");

        let ensure_first = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(ensure_first["disposition"], "spawned_new_owner");
        assert_eq!(
            ensure_first["status"]["execution_mode"],
            "full_automatic_pipeline"
        );

        // Wait until `task-root-a` is held inside its in-flight `Reviewer` call.
        let review_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            let a_in_review = current["pipeline_report"]["in_flight_stages"]
                .as_array()
                .is_some_and(|stages| {
                    stages
                        .iter()
                        .any(|s| s["task_id"] == "task-root-a" && s["kind"] == "task_review")
                });
            if entered_marker_a.is_file() && a_in_review {
                break;
            }
            assert!(
                Instant::now() < review_deadline,
                "timed out waiting for task-root-a to enter held Reviewer; last status: {current}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Cancel while the Reviewer call is held on `hold_reviewer_task-root-a.gate`.
        // Direct Reviewer futures have no mid-flight kill API, so the owner must record durable
        // stop, fence late promotion, and remain alive draining the finite future under the held
        // guard while truthfully reporting `stopping_draining_in_flight == true`.
        let cancel_report = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &[
                "code-driver",
                "cancel",
                "--job-id",
                &job.job_id,
                "--reason",
                "operator_cancel_during_in_flight_reviewer",
            ],
        )
        .await;
        assert_eq!(
            cancel_report["durable_stop_recorded_before_host_cancel"],
            true
        );
        assert_eq!(cancel_report["notified_live_owner"], true);
        assert_eq!(
            cancel_report["derived_attempt_cancellation_boundary"],
            "original_and_derived_signalled_direct_check_reviewer_closure_drained_fenced"
        );
        assert_eq!(cancel_report["status"]["stop_requested"], true);

        // While the Reviewer gate is still closed, the owner still holds the guard and reports
        // `stopping_draining_in_flight == true` (never dropping the future or falsely claiming
        // remote inference terminated).
        assert!(
            !exited_marker_a.is_file(),
            "held Reviewer must still be running while gate is closed"
        );
        let status_draining = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(status_draining["owner_state"], "live_verified_owner");
        assert_eq!(status_draining["stop_requested"], true);
        assert_eq!(status_draining["stopping_draining_in_flight"], true);
        assert_eq!(
            status_draining["pipeline_report"]["draining_in_flight_after_stop"],
            true
        );

        // Release the Reviewer gate so the finite in-flight Reviewer call finishes draining,
        // and verify the owner fences late promotion and releases all locks cleanly.
        fs::remove_file(&hold_gate_a).expect("release hold_reviewer_task-root-a.gate after cancel");

        let drain_deadline = Instant::now() + Duration::from_secs(30);
        let status_stopped = loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            if current["owner_state"] == "no_active_owner" {
                break current;
            }
            assert!(
                Instant::now() < drain_deadline,
                "timed out waiting for cancelled owner to finish draining Reviewer and exit; last status: {current}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_eq!(status_stopped["owner_state"], "no_active_owner");
        assert_eq!(status_stopped["lease_active"], false);
        assert_eq!(status_stopped["host_lock_held"], false);
        assert_eq!(status_stopped["stop_requested"], true);
        assert_eq!(status_stopped["stopping_draining_in_flight"], false);
        assert_eq!(status_stopped["pipeline_status"], "stopped");
        assert_eq!(status_stopped["overall_status"], "stopped");
        assert_ne!(status_stopped["job_phase"], "completed");

        // Even though the drained Reviewer process returned a valid response, stop fencing
        // prevented promoting the review receipt or running per-task integration / Closure.
        let task_a = &status_stopped["tasks"][0];
        assert_eq!(task_a["task_id"], "task-root-a");
        assert_ne!(task_a["stage"], "accepted");
        assert_ne!(task_a["stage"], "integrated");
        assert!(
            status_stopped["pipeline_report"]["tasks"][0]["review_receipt_id"].is_null(),
            "late Reviewer output after stop must be fenced from recording a review receipt"
        );
        assert!(
            status_stopped["pipeline_report"]["tasks"][0]["integration_receipt_id"].is_null(),
            "stopped task must not integrate"
        );
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 1);

        // Subsequent `ensure` is a quiescent no-op with zero new model or check calls.
        let ensure_after_stop = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(
            ensure_after_stop["disposition"],
            "already_quiescent_verified"
        );
        assert_eq!(ensure_after_stop["status"]["stop_requested"], true);
        assert_eq!(ensure_after_stop["status"]["pipeline_status"], "stopped");
        assert_eq!(ensure_after_stop["status"]["overall_status"], "stopped");
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 1);

        let _ = approval_id;
        supervisor.shutdown().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_code_driver_settled_check_and_correction_blocks_send_one_attention_notice() {
    Box::pin(async {
        // Check-failure corrections are not authorized in this fixture. Reviewer corrections
        // cover both absent authority and an exhausted Coder budget.
        for case in ["task_check", "final_check", "correction_no_authority", "correction_budget"] {
            let fixture = setup_fixture();
            let state_dir = fixture.root.join("runtime");
            let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone())).await.unwrap();
            let client = SupervisorClient::from_state_dir(&state_dir).unwrap();
            let binding = client.create_binding(THREAD_UUID_A, "settled-attention-fixture").await.unwrap();
            let binding_id = binding.binding_id;
            let thread_uuid = binding.thread_uuid;
            let correction = case.starts_with("correction");
            let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
                &fixture, &binding_id, &thread_uuid, "brf_notice_block", "plan-notice-block",
                vec![PlanTask {
                    id: "task-notice-a".into(), title: "Attention fixture task".into(),
                    instruction: "CTRL_TASK_A implement src/a.rs".into(), role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard, depends_on: Vec::new(), allowed_write_paths: vec!["src/a.rs".into()],
                }], BTreeMap::new(), vec!["src/a.rs".into()], 1, ExecutionBudgets::default(),
                ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, if case == "correction_budget" { 1 } else { 4 }),
                if case == "correction_no_authority" { 0 } else { 1 },
                headless_argon_choice(Difficulty::Hard),
            ).await;
            if correction {
                fs::write(fixture.root.join("reviewer_override_task-notice-a.json"), json!({
                    "outcome": "correct_in_scope", "findings": ["Return value requires adjustment"],
                    "required_in_scope_fixes": ["Update a() to return 101"], "summary": "Requires in-scope correction"
                }).to_string()).unwrap();
            }

            // Hold the first real frozen check so notice absence is observable while work runs.
            let gate = fixture.root.join("notice-check.gate");
            let entered = fixture.root.join("notice-check.entered");
            fs::write(&gate, "hold").unwrap();
            let original_check = fs::read_to_string(&fixture.fake_cargo_exe).unwrap();
            let fail = match case { "task_check" => "exit 1", "final_check" => "if [ \"$c\" -ge 2 ]; then exit 1; fi", _ => "" };
            let held_check = original_check.replace("exit 0\n", &format!(
                "touch '{}'\nwhile [ -f '{}' ]; do sleep 0.02; done\n{fail}\nexit 0\n", entered.display(), gate.display()));
            write_executable_script(&fixture.fake_cargo_exe, &held_check);
            let (socket, effect_count, server) = spawn_single_coworker_notice_ws_server(&fixture, &thread_uuid);
            let key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id).unwrap();
            let runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id).unwrap().unwrap();
            let digest = runtime.envelope.envelope_digest;
            let notice_root = coworker_notice_dir(&fixture.plans_dir);
            CoworkerNoticeStore::new(&notice_root).unwrap().configure(CoworkerNoticeConfig::new(
                key.clone(), digest.clone(), NativeToolOutputConfig::new_user_message_steer(socket, thread_uuid.clone(), fixture.repo_dir.clone())
            ).unwrap()).unwrap();
            let ensure = run_flow_cli_pipeline(&state_dir, &fixture, &binding_id, &thread_uuid,
                &["code-driver", "ensure", "--job-id", &job.job_id]).await;
            assert_eq!(ensure["disposition"], "spawned_new_owner");
            let deadline = Instant::now() + Duration::from_secs(60);
            while !entered.is_file() {
                assert!(Instant::now() < deadline, "check did not start for {case}");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let running = run_flow_cli_pipeline(&state_dir, &fixture, &binding_id, &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id]).await;
            assert_eq!(running["pipeline_status"], "running", "{case}: {running}");
            assert_eq!(running["blocked_on_unknown_effect"], false);
            assert_eq!(CoworkerNoticeStore::inspect(&notice_root, &key, &digest).unwrap().attempt_count, 0);
            assert_eq!(effect_count.load(Ordering::SeqCst), 0);
            fs::remove_file(&gate).unwrap();

            let settled = loop {
                let current = run_flow_cli_pipeline(&state_dir, &fixture, &binding_id, &thread_uuid,
                    &["code-driver", "status", "--job-id", &job.job_id]).await;
                if current["owner_state"] == "no_active_owner" { break current; }
                assert!(Instant::now() < deadline, "owner did not settle for {case}: {current}");
                tokio::time::sleep(Duration::from_millis(30)).await;
            };
            let expected_status = if correction { "awaiting_correction_execution" } else { "blocked_by_failed_checks" };
            assert_eq!(settled["pipeline_status"], expected_status, "{case}: {settled}");
            assert!(settled["fatal_error"].is_null(), "ordinary settled block is not fatal: {case}: {settled}");
            assert_eq!(settled["host_lock_held"], false);
            assert_ne!(settled["job_phase"], "completed");
            let turn = tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap();
            assert_eq!(turn["params"]["threadId"], thread_uuid);
            assert_eq!(turn["params"]["input"][0]["text"], format_coworker_job_wakeup_message(&job.job_id).unwrap());
            let metadata = CoworkerNoticeStore::inspect(&notice_root, &key, &digest).unwrap();
            assert_eq!(metadata.attempt_count, 1, "{case}");
            let latest = metadata.latest.unwrap();
            assert_eq!(latest.kind, threadlane_native::coworker_notice::NoticeKind::Attention);
            assert_eq!(latest.status, NoticeDeliveryStatus::Submitted);
            assert_eq!(effect_count.load(Ordering::SeqCst), 1);
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            let calls = read_invocation_count(&fixture.agy_reviewer_count_file);
            let checks = read_invocation_count(&fixture.check_count_file);
            run_flow_cli_pipeline(&state_dir, &fixture, &binding_id, &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id]).await;
            let approval = fixture.authority.get_for_operator(&approval_id).unwrap();
            let session_dir = fixture.sessions_dir.join(&approval_id);
            let stopped_host = threadlane_host::read_host_manifest_optional(&session_dir, &approval).unwrap().unwrap();
            assert_eq!(stopped_host.phase, HostPhase::Stopped);
            assert!(!threadlane_host::is_host_lock_held(&session_dir).unwrap());
            for command in ["ensure", "reconcile", "run-owner"] {
                let refused = run_flow_cli_pipeline_output(&state_dir, &fixture, &binding_id, &thread_uuid,
                    &["code-driver", command, "--job-id", &job.job_id]).await;
                assert!(!refused.status.success(), "{command} must preserve the stopped-host gate for {case}");
                assert!(refused.stdout.is_empty(), "refusal must not return a successful result");
                let error: Value = serde_json::from_slice(&refused.stderr).expect("structured CLI refusal");
                assert_eq!(error["code"], "validation_failed", "unexpected {command} refusal for {case}: {error}");
                let notices_after = CoworkerNoticeStore::inspect(&notice_root, &key, &digest).unwrap();
                assert_eq!(notices_after.attempt_count, 1,
                    "{case}: refused {command} created a notice: {notices_after:?}; error={error}; fatal_fp={}",
                    threadlane_native::coworker_notice::derive_fatal_notice_fingerprint(&key).unwrap());
                let after_refusal = run_flow_cli_pipeline(&state_dir, &fixture, &binding_id, &thread_uuid,
                    &["code-driver", "status", "--job-id", &job.job_id]).await;
                assert!(after_refusal["fatal_error"].is_null(), "{case}: refused {command} poisoned first-fatal diagnostics: {after_refusal}");
                let host_after = threadlane_host::read_host_manifest_optional(&session_dir, &approval).unwrap().unwrap();
                assert_eq!(host_after.phase, HostPhase::Stopped);
                assert_eq!(host_after.instance_id, stopped_host.instance_id);
                assert_eq!(host_after.updated_at_ms, stopped_host.updated_at_ms);
                assert!(!threadlane_host::is_host_lock_held(&session_dir).unwrap());
            }
            assert_eq!(CoworkerNoticeStore::inspect(&notice_root, &key, &digest).unwrap().attempt_count, 1);
            assert_eq!(effect_count.load(Ordering::SeqCst), 1);
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), calls);
            assert_eq!(read_invocation_count(&fixture.check_count_file), checks);
            supervisor.shutdown().await;
        }
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_code_driver_full_pipeline_live_held_coders_report_running_not_unknown_and_reject_stale_correlation(
) {
    Box::pin(async {
        let fixture = setup_fixture();
        let state_dir = fixture.root.join("runtime-held-coders");
        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start supervisor");
        let client = SupervisorClient::from_state_dir(&state_dir).expect("supervisor client");
        let binding = client
            .create_binding(THREAD_UUID_A, "live-held-coders-fixture")
            .await
            .expect("create binding");
        let binding_id = binding.binding_id;
        let thread_uuid = binding.thread_uuid;

        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
            &fixture,
            &binding_id,
            &thread_uuid,
            "brf_held_coders",
            "plan-held-coders",
            vec![
                PlanTask {
                    id: "task-root-a".to_string(),
                    title: "Implement module A".to_string(),
                    instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/a.rs".to_string()],
                },
                PlanTask {
                    id: "task-root-b".to_string(),
                    title: "Implement module B".to_string(),
                    instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                    role: WorkflowRole::Coder,
                    difficulty: Difficulty::Hard,
                    depends_on: Vec::new(),
                    allowed_write_paths: vec!["src/b.rs".to_string()],
                },
            ],
            BTreeMap::new(),
            vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            1,
            headless_argon_choice(Difficulty::Hard),
        )
        .await;

        // Select a real Reviewer- or failed-check-based correction before the owner starts.
        let override_a_path = fixture.root.join("reviewer_override_task-root-a.json");
        fs::write(
            &override_a_path,
            serde_json::to_string_pretty(&json!({
                "outcome": "correct_in_scope",
                "findings": ["Return value in src/a.rs should be 101 instead of 100."],
                "required_in_scope_fixes": ["Update pub fn a() -> u32 in src/a.rs to return 101."],
                "summary": "Requires bounded in-scope correction in src/a.rs."
            }))
            .expect("serialize reviewer override"),
        )
        .expect("write reviewer_override_task-root-a.json");
        let hold_initial_a = fixture.root.join("hold_initial_coder_a.gate");
        let hold_initial_b = fixture.root.join("hold_initial_coder_b.gate");
        let hold_derived_a = fixture.root.join("hold_derived_coder_a.gate");
        let entered_initial_a = fixture.root.join("entered_initial_coder_a.marker");
        let entered_initial_b = fixture.root.join("entered_initial_coder_b.marker");
        let entered_derived_a = fixture.root.join("entered_derived_coder_a.marker");
        fs::write(&hold_initial_a, "hold\n").expect("create hold_initial_a");
        fs::write(&hold_initial_b, "hold\n").expect("create hold_initial_b");
        fs::write(&hold_derived_a, "hold\n").expect("create hold_derived_a");

        let original_agy = fs::read_to_string(&fixture.fake_agy_exe).expect("read fake_agy_exe");
        let exec_line = original_agy
            .lines()
            .find(|line| line.starts_with("exec node "))
            .expect("find exec node line in fake_agy_exe")
            .to_string();
        let gated_agy = format!(
            "#!/bin/sh\nset -eu\ncase \"$*\" in\n  *\"--mode plan\"*)\n    ;;\n  *\"IN-SCOPE CORRECTION DIRECTIVE\"*)\n    if [ -f \"{hold_derived}\" ]; then\n      : > \"{entered_derived}\"\n      while [ -f \"{hold_derived}\" ]; do sleep 0.02; done\n    fi\n    ;;\n  *\"CTRL_TASK_A\"*)\n    if [ -f \"{hold_a}\" ]; then\n      : > \"{entered_a}\"\n      while [ -f \"{hold_a}\" ]; do sleep 0.02; done\n    fi\n    ;;\n  *\"CTRL_TASK_B\"*)\n    if [ -f \"{hold_b}\" ]; then\n      : > \"{entered_b}\"\n      while [ -f \"{hold_b}\" ]; do sleep 0.02; done\n    fi\n    ;;\nesac\n{exec_line}\n",
            hold_derived = hold_derived_a.display(),
            entered_derived = entered_derived_a.display(),
            hold_a = hold_initial_a.display(),
            entered_a = entered_initial_a.display(),
            hold_b = hold_initial_b.display(),
            entered_b = entered_initial_b.display(),
        );
        write_executable_script(&fixture.fake_agy_exe, &gated_agy);

        let ensure_initial = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(ensure_initial["disposition"], "spawned_new_owner");

        let initial_deadline = Instant::now() + Duration::from_secs(20);
        while !entered_initial_a.is_file() || !entered_initial_b.is_file() {
            assert!(
                Instant::now() < initial_deadline,
                "timed out waiting for both parallel initial Coders to enter gates"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let status_initial_running = loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            let running_tasks = current["tasks"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter(|t| t["stage"] == "running" && t["host_status"] == "running")
                        .count()
                })
                .unwrap_or(0);
            if current["step_report"]["active_running_count"] == 2 && running_tasks == 2 {
                break current;
            }
            assert!(
                Instant::now() < initial_deadline,
                "timed out waiting for live owner status to observe 2 running initial Coders; last: {current}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };

        assert_eq!(status_initial_running["owner_state"], "live_verified_owner");
        assert_eq!(
            status_initial_running["execution_mode"],
            "full_automatic_pipeline"
        );
        assert_eq!(status_initial_running["overall_status"], "running_tasks");
        assert_eq!(status_initial_running["pipeline_status"], "running");
        assert_eq!(
            status_initial_running["pipeline_report"]["status"],
            "running"
        );
        assert_eq!(
            status_initial_running["pipeline_report"]["unknown_effect_task_ids"],
            json!([])
        );
        assert_eq!(status_initial_running["blocked_on_unknown_effect"], false);

        // Durable PendingControllerEffect records remain ReservedBeforeEffect on disk with actual run_id correlations.
        let runtime_initial = fixture
            .policy_store
            .get_job_runtime_readonly(&binding_id, &job.job_id)
            .expect("read runtime while initial Coders held")
            .expect("runtime exists");
        assert_eq!(runtime_initial.unresolved_effects().len(), 2);
        for eff in runtime_initial.unresolved_effects() {
            assert_eq!(eff.kind, PendingControllerEffectKind::CoderDispatch);
            assert_eq!(eff.status, PendingControllerEffectStatus::ReservedBeforeEffect);
            assert_eq!(eff.attempt_no, Some(1));
            assert!(
                eff.external_correlation_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("run_")),
                "expected run_id correlation on ReservedBeforeEffect initial CoderDispatch: {eff:?}"
            );
        }

        // Duplicate ensure while held reuses the live owner with zero extra provider calls.
        let dup_ensure = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(dup_ensure["disposition"], "reused_live_owner");
        assert_eq!(dup_ensure["status"]["pipeline_status"], "running");
        assert_eq!(dup_ensure["status"]["blocked_on_unknown_effect"], false);

        // Negative cases use isolated snapshots; never rewrite a ledger owned by the driver.
        let approval = fixture.authority.get_for_operator(&approval_id).expect("approval");
        let session_dir = fixture.sessions_dir.join(&approval_id);
        let manifest = threadlane_host::read_host_manifest_optional(&session_dir, &approval)
            .expect("manifest").expect("manifest exists");
        let (intents, receipts) = threadlane_host::read_session_task_ledger_readonly(
            &session_dir, &approval, manifest.session_binding_id.as_deref().expect("session binding"),
        ).expect("read task ledger").expect("task ledger exists");
        let key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id).expect("key");
        let project_with_plans = |snapshot_job: &CoworkerJobRecord, snapshot_runtime: &ControllerJobRuntimeState, status: &Value, plans_dir: &Path| {
            let report: threadlane_native::coworker_driver::CoworkerDriverStatusReport =
                serde_json::from_value(status.clone()).expect("status report");
            threadlane_native::coworker_driver::evaluate_live_coder_projection_snapshot_from_job_and_runtime(
                snapshot_job, snapshot_runtime, &approval, &key,
                report.verified_owner_summary.as_ref().expect("verified owner"),
                report.execution_mode, report.host_phase, report.host_lock_held,
                &intents, &receipts, plans_dir,
                report.step_report.as_ref(), &report.in_flight_stages,
            )
        };
        let project = |snapshot_job: &CoworkerJobRecord, snapshot_runtime: &ControllerJobRuntimeState, status: &Value| {
            project_with_plans(snapshot_job, snapshot_runtime, status, &fixture.plans_dir)
        };
        let initial_job = fixture.workflow_store.read_binding_ledger(&binding_id).expect("ledger")
            .jobs.into_iter().find(|j| j.job_id == job.job_id).expect("job");
        assert!(!project(&initial_job, &runtime_initial, &status_initial_running));
        let mut stale_initial = initial_job.clone();
        stale_initial.outbox.iter_mut().find(|o| o.task_id == "task-root-a" && o.attempt_no == 1)
            .expect("outbox").run_id = Some("run_00000000-0000-4000-8000-000000000099".to_string());
        assert!(project(&stale_initial, &runtime_initial, &status_initial_running));
        let mut orphan_initial = runtime_initial.clone();
        orphan_initial.lease.as_mut().expect("lease").instance_id = "stale-owner".to_string();
        assert!(project(&initial_job, &orphan_initial, &status_initial_running));
        let mut expired_initial = runtime_initial.clone();
        expired_initial.lease.as_mut().expect("lease").expires_at_ms = 1;
        assert!(project(&initial_job, &expired_initial, &status_initial_running));
        let mut reopened_initial = initial_job.clone();
        reopened_initial.outbox.iter_mut().find(|o| o.task_id == "task-root-a" && o.attempt_no == 1).expect("outbox")
            .status = OutboxDispatchStatus::ReservedBeforeEffect;
        assert!(project(&reopened_initial, &runtime_initial, &status_initial_running));
        let mut unknown_initial = runtime_initial.clone();
        unknown_initial.pending_effects.values_mut().next().expect("effect").status = PendingControllerEffectStatus::EffectUnknown;
        assert!(project(&initial_job, &unknown_initial, &status_initial_running));

        // Release both initial Coder gates so task-root-b integrates and task-root-a enters
        // Reviewer -> CorrectInScopeRequired -> derived Coder attempt 2 (held on hold_derived_a).
        fs::remove_file(&hold_initial_a).expect("release hold_initial_a");
        fs::remove_file(&hold_initial_b).expect("release hold_initial_b");

        let derived_deadline = Instant::now() + Duration::from_secs(30);
        while !entered_derived_a.is_file() {
            assert!(
                Instant::now() < derived_deadline,
                "timed out waiting for derived correction Coder attempt 2 to enter gate"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Clear the Reviewer override before derived attempt 2 finishes so attempt 2 is accepted.
        fs::remove_file(&override_a_path).expect("remove reviewer_override_task-root-a.json");

        let status_derived_running = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(status_derived_running["owner_state"], "live_verified_owner");
        assert_eq!(status_derived_running["pipeline_status"], "running");
        assert_eq!(
            status_derived_running["pipeline_report"]["status"],
            "running"
        );
        assert_eq!(
            status_derived_running["pipeline_report"]["unknown_effect_task_ids"],
            json!([])
        );
        assert_eq!(status_derived_running["blocked_on_unknown_effect"], false);
        assert!(
            status_derived_running["in_flight_stages"]
                .as_array()
                .is_some_and(|stages| stages.iter().any(|s| {
                    s["kind"] == "task_correction"
                        && s["task_id"] == "task-root-a"
                        && s["attempt_no"] == 2
                })),
            "expected in_flight_stages to contain task_correction for task-root-a attempt 2: {status_derived_running}"
        );

        let runtime_derived = fixture
            .policy_store
            .get_job_runtime_readonly(&binding_id, &job.job_id)
            .expect("read runtime while derived Coder held")
            .expect("runtime exists");
        assert_eq!(runtime_derived.unresolved_effects().len(), 1);
        let derived_eff = runtime_derived.unresolved_effects()[0];
        assert_eq!(derived_eff.kind, PendingControllerEffectKind::CoderDispatch);
        assert_eq!(
            derived_eff.status,
            PendingControllerEffectStatus::ReservedBeforeEffect
        );
        assert_eq!(derived_eff.task_id.as_deref(), Some("task-root-a"));
        assert_eq!(derived_eff.attempt_no, Some(2));
        assert!(
            derived_eff
                .external_correlation_id
                .as_deref()
                .is_some_and(|id| id.starts_with("dar_")),
            "expected DerivedTaskAttemptReceipt correlation on derived CoderDispatch: {derived_eff:?}"
        );

        let derived_job = fixture.workflow_store.read_binding_ledger(&binding_id).expect("ledger")
            .jobs.into_iter().find(|j| j.job_id == job.job_id).expect("derived job");
        assert!(!project(&derived_job, &runtime_derived, &status_derived_running));
        // Decompose original-host lifecycle from the retained direct execution: this saved
        // genuine owner/lease/receipt/inflight context must still prove the same derived
        // attempt when the original host is stopped. The private manifest test supplies
        // actual stopped/unlocked filesystem evidence for the shared observation rule.
        let mut stopped_original = status_derived_running.clone();
        stopped_original["host_phase"] = json!("stopped");
        stopped_original["host_lock_held"] = json!(false);
        assert!(!project(&derived_job, &runtime_derived, &stopped_original));
        let mut unrelated_stale = derived_job.clone();
        unrelated_stale.tasks.iter_mut().find(|task| task.task_id == "task-root-b").unwrap().stage = CoworkerTaskStage::Running;
        assert!(project(&unrelated_stale, &runtime_derived, &stopped_original), "live derived proof must not exempt an unrelated stale original task");
        let mut unrelated_stale_held = derived_job.clone();
        let stale_held_b = unrelated_stale_held.tasks.iter_mut().find(|task| task.task_id == "task-root-b").unwrap();
        stale_held_b.stage = CoworkerTaskStage::Held;
        stale_held_b.stage_before_hold = Some(CoworkerTaskStage::Running);
        assert!(project(&unrelated_stale_held, &runtime_derived, &stopped_original), "promoted candidate state must not exempt a stale Held running task on a stopped host");
        let mut stale_derived = derived_job.clone();
        stale_derived.outbox.iter_mut().find(|o| o.task_id == "task-root-a" && o.attempt_no == 2)
            .expect("derived outbox").receipt_id = Some("dar_00000000000000000000000000000099".to_string());
        assert!(project(&stale_derived, &runtime_derived, &status_derived_running));
        assert!(project(&stale_derived, &runtime_derived, &stopped_original));
        let mut unknown_derived = derived_job.clone();
        unknown_derived.outbox.iter_mut().find(|o| o.task_id == "task-root-a" && o.attempt_no == 2)
            .expect("derived outbox").status = OutboxDispatchStatus::EffectUnknown;
        assert!(project(&unknown_derived, &runtime_derived, &status_derived_running));
        let mut stale_flight = status_derived_running.clone();
        stale_flight["in_flight_stages"] = json!([]);
        assert!(project(&derived_job, &runtime_derived, &stale_flight));
        let mut stopped_stale_flight = stopped_original.clone();
        stopped_stale_flight["in_flight_stages"] = json!([]);
        assert!(project(&derived_job, &runtime_derived, &stopped_stale_flight));

        // Only an actual NotFound proves absence; an inaccessible report directory stays blocked.
        let denied_dir = fixture.root.join("denied-projection-reports");
        fs::create_dir(&denied_dir).expect("create isolated report directory");
        fs::set_permissions(&denied_dir, fs::Permissions::from_mode(0o000)).expect("deny report access");
        let denied_path = threadlane_native::coworker_workflow::derived_execution_report_file_path(&denied_dir, &derived_eff.effect_id);
        assert_eq!(fs::symlink_metadata(&denied_path).expect_err("must exercise denied access").kind(), std::io::ErrorKind::PermissionDenied);
        assert!(project_with_plans(&derived_job, &runtime_derived, &status_derived_running, &denied_dir));
        fs::set_permissions(&denied_dir, fs::Permissions::from_mode(0o700)).expect("restore isolated directory");
        fs::write(&denied_path, b"existing report").expect("create isolated report marker");
        assert!(project_with_plans(&derived_job, &runtime_derived, &status_derived_running, &denied_dir));

        // The check-failure execution route remains unsupported. Exercise its read-only
        // receipt projection using isolated typed snapshots; never persist or dispatch them.
        let mut check_job = derived_job.clone();
        let mut check_runtime = runtime_derived.clone();
        check_runtime.envelope.allow_check_failure_corrections = true;
        check_runtime.envelope.envelope_digest = check_runtime.envelope.compute_digest().expect("snapshot envelope digest");
        let task = check_job.tasks.iter_mut().find(|t| t.task_id == "task-root-a").expect("snapshot task");
        let prior = task.candidates.iter_mut().find(|c| c.attempt_no == 1).expect("prior candidate");
        prior.review_verdict = None;
        // Align the synthetic failed-check attempt evidence with its candidate lineage.
        for attempt in task.attempts.iter_mut().filter(|a| a.role == WorkflowRole::Coder && a.attempt_no == 1) {
            attempt.head_commit = Some(prior.head_commit.clone());
            attempt.tree_sha = Some(prior.tree_sha.clone());
        }
        let receipt = check_runtime.derived_attempts.iter_mut().find(|d| d.task_id == task.task_id && d.attempt_no == 2).expect("derived receipt");
        let spec = check_runtime.envelope.check_commands.get("cargo_test").expect("check spec");
        let basis = threadlane_native::coworker_workflow::CheckFailureCorrectionBasis {
            task_id: task.task_id.clone(), task_manifest_id: "w_projection_snapshot".to_string(),
            prior_attempt_no: 1, prior_candidate_id: prior.candidate_id.clone(),
            execution_base_commit: receipt.execution_base_commit.clone(),
            committed_head_commit: prior.head_commit.clone(), committed_tree_sha: prior.tree_sha.clone(),
            check_subject_digest: sha256_hex(b"snapshot check subject"),
            prior_review_subject_digest: receipt.prior_review_subject_digest.clone().expect("prior review subject"),
            failed_checks: vec![threadlane_native::coworker_workflow::CheckFailureReportRef {
                check_id: spec.check_id.clone(), kind: spec.kind, spec_digest: spec.spec_digest().expect("snapshot check digest"),
                effect_id: "eff_projection_snapshot".to_string(),
                report_path: fixture.root.join("snapshot-failed-check.json").display().to_string(),
                evidence_digest: sha256_hex(b"snapshot failure evidence"),
                status: threadlane_native::coworker_workflow::FrozenCheckExecutionStatus::NonZeroExit,
                exit_code: Some(1), signal: None, stdout_sha256: sha256_hex(b""), stderr_sha256: sha256_hex(b"check failed"),
                post_worktree_clean: true, subject_drifted: false, diagnostic_summary: "Snapshot check failure".to_string(),
            }], diagnostic_summary: "Snapshot required check failure".to_string(),
        };
        receipt.envelope_digest = check_runtime.envelope.envelope_digest.clone();
        receipt.prior_review_verdict_id = None;
        receipt.prior_check_failure_basis = Some(basis);
        receipt.receipt_digest = receipt.compute_digest().expect("snapshot receipt digest");
        receipt.validate_against_task(task, &check_runtime.envelope).expect("valid check-basis snapshot receipt");
        let digest = receipt.receipt_digest.clone();
        check_job.outbox.iter_mut().find(|o| o.task_id == "task-root-a" && o.attempt_no == 2).expect("outbox").subject_digest = digest.clone();
        check_runtime.pending_effects.values_mut().find(|e| e.task_id.as_deref() == Some("task-root-a") && e.attempt_no == Some(2)).expect("effect").subject_digest = digest;
        assert!(!project(&check_job, &check_runtime, &status_derived_running));
        let mut unauthorized_check = check_runtime.clone();
        unauthorized_check.envelope.allow_check_failure_corrections = false;
        assert!(project(&check_job, &unauthorized_check, &status_derived_running));
        let mut stale_check = check_runtime.clone();
        stale_check.derived_attempts.iter_mut().find(|d| d.task_id == "task-root-a" && d.attempt_no == 2).expect("receipt")
            .prior_check_failure_basis.as_mut().expect("basis").committed_head_commit = "f".repeat(40);
        assert!(project(&check_job, &stale_check, &status_derived_running));

        // Release derived Coder gate and verify clean completion with zero duplicate provider calls.
        fs::remove_file(&hold_derived_a).expect("release hold_derived_a");

        // Match the existing full-pipeline fixture's bound for real Git/provenance checks.
        let complete_deadline = Instant::now() + Duration::from_secs(300);
        let status_completed = loop {
            let current = run_flow_cli_pipeline(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            if current["pipeline_status"] == "completed"
                && current["owner_state"] == "no_active_owner"
            {
                break current;
            }
            if Instant::now() >= complete_deadline || (current["pipeline_status"] == "blocked_or_failed" && current["owner_state"] == "no_active_owner") {
                let runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id)
                    .expect("failure runtime").expect("runtime exists");
                let diagnostics: Vec<_> = runtime.closure_artifacts.values().map(|r| {
                    let loaded = fixture.policy_store.load_closure_execution_artifact_for_record(&binding_id, &thread_uuid, &job.job_id, r)
                        .expect("load fake Closure artifact");
                    (loaded.diagnostic, loaded.response_error, loaded.observed_model, loaded.observed_effort)
                }).collect();
                panic!("pipeline did not complete; Closure calls: {}; diagnostics: {diagnostics:?}; last: {current}",
                    read_invocation_count(&fixture.agy_closure_count_file));
            }
            // Bound diagnostic traffic between the owner's existing 50ms ticks.
            tokio::time::sleep(Duration::from_millis(200)).await;
        };

        assert_eq!(status_completed["job_phase"], "completed");
        assert_eq!(status_completed["pipeline_status"], "completed");
        assert_eq!(status_completed["blocked_on_unknown_effect"], false);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 3);
        assert_eq!(read_invocation_count(&fixture.root.join("agy_derived_coder_count.txt")), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 3);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);

        // Isolate the derived post-verification/pre-Reviewer boundary from genuine completed
        // artifacts. Preserve its terminal Coder receipt/report correlation and prior lineage.
        // These copies are never persisted, accepted, integrated, or dispatched.
        let mut held_derived_job = fixture.workflow_store.read_binding_ledger(&binding_id).unwrap()
            .jobs.into_iter().find(|record| record.job_id == job.job_id).unwrap();
        let mut held_derived_runtime = fixture.policy_store.get_job_runtime_readonly(&binding_id, &job.job_id).unwrap().unwrap();
        let task = held_derived_job.tasks.iter_mut().find(|task| task.task_id == "task-root-a").unwrap();
        task.stage = CoworkerTaskStage::Held;
        task.stage_before_hold = Some(CoworkerTaskStage::ReadyForReview);
        task.active_candidate_id = None;
        task.accepted_candidate_id = None;
        task.candidates.retain(|candidate| candidate.attempt_no != 2);
        task.attempts.retain(|attempt| attempt.attempt_no != 2 || attempt.role == WorkflowRole::Coder);
        held_derived_job.outbox.retain(|outbox| outbox.task_id != "task-root-a" || outbox.attempt_no != 2 || outbox.kind == threadlane_native::coworker_workflow::OutboxDispatchKind::TaskAttemptDispatch);
        held_derived_runtime.pending_effects.retain(|_, effect| {
            effect.task_id.as_deref() != Some("task-root-a") || effect.attempt_no != Some(2)
                || matches!(effect.kind, PendingControllerEffectKind::CoderDispatch | PendingControllerEffectKind::TaskCommit | PendingControllerEffectKind::CheckExecution)
        });
        let artifacts = held_derived_runtime.attempt_artifacts.get_mut(&ControllerJobRuntimeState::artifact_key("task-root-a", 2)).unwrap();
        assert!(artifacts.is_derived_attempt);
        assert!(artifacts.candidate_id.is_some());
        assert!(!artifacts.check_evidence.is_empty());
        assert!(artifacts.review_subject.is_some());
        artifacts.reviewer_response_digest = None;
        artifacts.review_receipt = None;
        artifacts.integration_receipt = None;
        assert!(!project(&held_derived_job, &held_derived_runtime, &stopped_original), "a verified derived Coder held before Reviewer is physically terminal on a stopped original host");
        let mut before_derived_commit = held_derived_runtime.clone();
        let artifacts = before_derived_commit.attempt_artifacts.get_mut(&ControllerJobRuntimeState::artifact_key("task-root-a", 2)).unwrap();
        artifacts.committed_head_commit = None;
        artifacts.committed_tree_sha = None;
        artifacts.candidate_id = None;
        artifacts.check_evidence.clear();
        artifacts.test_evidence_summary = None;
        artifacts.review_subject = None;
        before_derived_commit.pending_effects.retain(|_, effect| effect.task_id.as_deref() != Some("task-root-a") || effect.attempt_no != Some(2) || effect.kind == PendingControllerEffectKind::CoderDispatch);
        assert!(!project(&held_derived_job, &before_derived_commit, &stopped_original), "a correlated terminal derived report precedes TaskCommit and must not invent physical Coder liveness");
        let mut mismatched_terminal = held_derived_runtime.clone();
        mismatched_terminal.pending_effects.values_mut().find(|effect| effect.task_id.as_deref() == Some("task-root-a") && effect.attempt_no == Some(2) && effect.kind == PendingControllerEffectKind::CoderDispatch).unwrap()
            .external_correlation_id = Some("unrelated_terminal_report".into());
        assert!(project(&held_derived_job, &mismatched_terminal, &stopped_original), "an unrelated report must not prove a derived Coder terminal");
        // TaskCommit and checks follow physical Coder completion; their pending/unknown effects
        // still fence advancement without inventing a live Coder from the held workflow stage.
        let mut pending_commit = held_derived_runtime.clone();
        pending_commit.pending_effects.values_mut().find(|effect| effect.task_id.as_deref() == Some("task-root-a") && effect.attempt_no == Some(2) && effect.kind == PendingControllerEffectKind::TaskCommit).unwrap()
            .status = PendingControllerEffectStatus::EffectUnknown;
        assert!(project(&held_derived_job, &pending_commit, &stopped_original), "terminal Coder proof must not clear an unknown TaskCommit effect");

        let _ = approval_id;
        supervisor.shutdown().await;
    })
    .await;
}

#[tokio::test]
async fn explicit_unauthorized_driver_mode_refuses_without_fatal_record_or_notice() {
    Box::pin(async {
        let fixture = setup_fixture();
        let (job, _) = create_planned_approved_and_frozen_job(
            &fixture, "brf_mode_refusal", "plan-mode-refusal",
            vec![PlanTask {
                id: "task-mode".into(), title: "Mode refusal fixture".into(),
                instruction: "CTRL_TASK_A implement src/a.rs".into(), role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard, depends_on: Vec::new(), allowed_write_paths: vec!["src/a.rs".into()],
            }], BTreeMap::new(), vec!["src/a.rs".into()], 1, ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 1),
            IntegrationStrategy::MergeNoFf, true,
        ).await;
        // Narrow this offline operator fixture to initial dispatch only. Preserve its exact
        // approval/job linkage and recompute the frozen envelope digest; admission must still
        // validate, while the requested full pipeline remains outside this envelope.
        fixture.policy_store.mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |runtime| {
            runtime.envelope.allow_run_integration_execution = false;
            runtime.envelope.envelope_digest = runtime.envelope.compute_digest()?;
            Ok(())
        }).unwrap();
        let driver = CoworkerCodeDriver::from_controller(
            fixture.root.join("runtime"), &fixture.registry_dir, &fixture.plans_dir,
            &fixture.authority_dir, &fixture.sessions_dir, Arc::clone(&fixture.authority), fixture.controller.clone(),
        ).unwrap();
        let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &job.job_id).unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let session = fixture.controller.open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, now).unwrap();
        let admission = session.verify_admission().expect("narrowed fixture retains real admission");
        session.release().unwrap();
        let (socket, count, peer) = spawn_single_coworker_notice_ws_server(&fixture, THREAD_UUID_A);
        let notices = coworker_notice_dir(&fixture.plans_dir);
        let digest = admission.runtime_state.envelope.envelope_digest;
        CoworkerNoticeStore::new(&notices).unwrap().configure(CoworkerNoticeConfig::new(
            key.clone(), digest.clone(), NativeToolOutputConfig::new_user_message_steer(socket, THREAD_UUID_A.into(), fixture.repo_dir.clone()),
        ).unwrap()).unwrap();
        let options = threadlane_native::coworker_driver::CoworkerDriverRunOptions {
            execution_mode: Some(threadlane_native::coworker_driver::CoworkerDriverExecutionMode::FullAutomaticPipeline),
            max_steps: Some(1), ..Default::default()
        };
        for operation in ["run-owner", "reconcile", "ensure"] {
            let error = match operation {
                "run-owner" => driver.run_owner(BINDING_A, THREAD_UUID_A, &job.job_id, options).await.unwrap_err(),
                "reconcile" => driver.reconcile_with_options(BINDING_A, THREAD_UUID_A, &job.job_id, Some(options)).await.unwrap_err(),
                _ => driver.ensure_owner(BINDING_A, THREAD_UUID_A, &job.job_id, Some(options)).await.unwrap_err(),
            };
            assert_eq!(error.code, NativeErrorCode::ValidationFailed);
            assert!(error.message.contains("does not authorize full_automatic_pipeline"));
            assert!(driver.read_owner_failure_optional(&key).unwrap().is_none(), "{operation} refusal is not a runtime failure");
            assert_eq!(CoworkerNoticeStore::inspect(&notices, &key, &digest).unwrap().attempt_count, 0);
            assert_eq!(count.load(Ordering::SeqCst), 0);
            assert_eq!(fs::read_dir(&fixture.sessions_dir).unwrap().count(), 0, "{operation} must not start a host");
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
        }
        peer.abort();
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_host_cached_running_is_unknown_without_mutating_historical_state() {
    Box::pin(async {
        fn json_records(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
            let mut result = BTreeMap::new();
            if !root.exists() { return result; }
            for entry in fs::read_dir(root).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() { result.extend(json_records(&path)); }
                else if path.extension().is_some_and(|ext| ext == "json") {
                    result.insert(path.clone(), fs::read(path).unwrap());
                }
            }
            result
        }
        let fixture = setup_fixture();
        let state_dir = fixture.root.join("inactive-control");
        let control = Supervisor::start(SupervisorConfig::new(state_dir.clone())).await.unwrap();
        let client = SupervisorClient::from_state_dir(&state_dir).unwrap();
        let thread = uuid::Uuid::new_v4().to_string();
        let binding = client.create_binding(&thread, "inactive-status-fixture").await.unwrap();
        let tasks = [
            ("task-inactive-a", "CTRL_TASK_A implement src/a.rs", "src/a.rs", Vec::new()),
            ("task-complete-b", "CTRL_TASK_B implement src/b.rs", "src/b.rs", Vec::new()),
            ("task-pending-c", "Implement src/c.rs after both accepted roots", "src/c.rs", vec!["task-inactive-a".into(), "task-complete-b".into()]),
        ].into_iter().map(|(id, instruction, path, depends_on)| PlanTask {
            id: id.into(), title: format!("Fixture {id}"), instruction: instruction.into(),
            role: WorkflowRole::Coder, difficulty: Difficulty::Hard, depends_on,
            allowed_write_paths: vec![path.into()],
        }).collect();
        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope_with_reviewer(
            &fixture, &binding.binding_id, &thread, "brf_inactive_projection", "plan-inactive-projection",
            tasks, BTreeMap::new(), vec!["src/a.rs".into(), "src/b.rs".into(), "src/c.rs".into()],
            2, ExecutionBudgets::default(), ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4), 1,
            headless_argon_choice(Difficulty::Hard),
        ).await;
        let original = fs::read_to_string(&fixture.fake_agy_exe).unwrap();
        let exec = original.lines().find(|line| line.starts_with("exec node ")).unwrap();
        let gate = fixture.root.join("inactive-coder.gate");
        fs::write(&gate, "hold").unwrap();
        write_executable_script(&fixture.fake_agy_exe, &format!(
            "#!/bin/sh\nset -eu\ncase \"$*\" in *CTRL_TASK_A*) while [ -f '{}' ]; do sleep 0.02; done ;; esac\n{exec}\n", gate.display()));
        let host = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir, &fixture.plans_dir, &fixture.authority_dir, &fixture.sessions_dir, &approval_id,
        ))).await.unwrap();
        let driver = CoworkerCodeDriver::open_from_dirs(&state_dir, &fixture.registry_dir, &fixture.plans_dir,
            &fixture.authority_dir, &fixture.sessions_dir).unwrap();
        let now = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let session = driver.controller().open_owned_session(&binding.binding_id, &thread, &job.job_id, now()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let step = session.step(now()).await.unwrap();
            if step.tasks.iter().any(|task| task.task_id == "task-complete-b" && task.stage == CoworkerTaskStage::ReadyForReview) { break; }
            assert!(Instant::now() < deadline, "completed sibling did not reach verified ReadyForReview");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // A genuine Ready host is not declared inactive merely because this fixture's held
        // controller guard has no owner RPC socket. Live workers still report Running.
        let live = driver.status(&binding.binding_id, &thread, &job.job_id).await.unwrap();
        assert_eq!(live.host_phase, Some(HostPhase::Ready)); assert!(live.host_lock_held);
        assert_eq!(live.tasks.iter().find(|task| task.task_id == "task-inactive-a").unwrap().host_status, TaskExecutionStatus::Running);
        let host_client = ApprovedHostClient::new(Arc::clone(&fixture.authority), &fixture.sessions_dir, &binding.binding_id, &thread).unwrap();
        let completed_snapshot = host_client.get_task(&approval_id, "task-complete-b").await.unwrap();
        assert_eq!(completed_snapshot.status, TaskExecutionStatus::ReadyForReview);
        assert!(!completed_snapshot.accepted, "verified completion is not review acceptance");
        session.release().unwrap();
        host.shutdown().await.unwrap();
        let before: Vec<_> = [&fixture.plans_dir, &fixture.registry_dir, &fixture.authority_dir, &fixture.sessions_dir]
            .into_iter().map(|dir| json_records(dir)).collect();
        let status = driver.status(&binding.binding_id, &thread, &job.job_id).await.unwrap();
        assert_eq!(status.host_phase, Some(HostPhase::Stopped)); assert!(!status.host_lock_held); assert!(!status.lease_active);
        let inactive = status.tasks.iter().find(|task| task.task_id == "task-inactive-a").unwrap();
        assert_eq!(inactive.stage, CoworkerTaskStage::Running, "historical stage must be preserved");
        assert_eq!(inactive.host_status, TaskExecutionStatus::Unknown, "stopped host cannot prove a cached Running task is active");
        assert_eq!(status.overall_status, threadlane_native::coworker_workflow::CoworkerControllerPhase1Status::BlockedOnUnknownEffect);
        assert!(status.blocked_on_unknown_effect);
        let complete = status.tasks.iter().find(|task| task.task_id == "task-complete-b").unwrap();
        assert_eq!(complete.stage, CoworkerTaskStage::ReadyForReview); assert_eq!(complete.host_status, TaskExecutionStatus::ReadyForReview);
        assert_eq!(status.tasks.iter().find(|task| task.task_id == "task-pending-c").unwrap().host_status, TaskExecutionStatus::DependencyInputsUnverified);
        let pipeline = status.pipeline_report.as_ref().unwrap();
        assert_eq!(pipeline.status, CoworkerPipelineStatus::BlockedOnUnknownEffect);
        assert_eq!(pipeline.unknown_effect_task_ids, vec!["task-inactive-a"]);
        assert_eq!(pipeline.tasks.iter().find(|task| task.task_id == "task-inactive-a").unwrap().host_status, TaskExecutionStatus::Unknown);
        assert_eq!(driver.status(&binding.binding_id, &thread, &job.job_id).await.unwrap(), status);
        let after: Vec<_> = [&fixture.plans_dir, &fixture.registry_dir, &fixture.authority_dir, &fixture.sessions_dir]
            .into_iter().map(|dir| json_records(dir)).collect();
        assert_eq!(after, before, "status reads must not mutate authoritative fixture records");
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1, "only completed sibling reaches fake provider body");
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0); assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);

        // Finish actual TaskCommit/check verification before applying an actual workflow hold.
        // Reviewer has not run, so the verified candidate exists only in runtime artifacts.
        let verification_session = driver.controller().open_owned_session(&binding.binding_id, &thread, &job.job_id, now()).unwrap();
        threadlane_native::coworker_workflow::CoworkerTaskVerificationService::new(driver.controller().clone())
            .verify_and_prepare_task_attempt(verification_session.guard(), &threadlane_native::coworker_workflow::CoworkerTaskVerificationRequest {
                binding_id: binding.binding_id.clone(), thread_uuid: thread.clone(), job_id: job.job_id.clone(),
                task_id: "task-complete-b".into(), attempt_no: 1, now_ms: now(),
            }).await.expect("verify completed task before Reviewer");
        let verified = verification_session.verify_admission().unwrap();
        let verified_task = verified.job.tasks.iter().find(|task| task.task_id == "task-complete-b").unwrap();
        assert_eq!(verified_task.stage, CoworkerTaskStage::ReadyForReview);
        assert!(verified_task.candidates.is_empty());
        let artifacts = verified.runtime_state.get_attempt_artifacts("task-complete-b", 1).unwrap();
        assert!(artifacts.candidate_id.is_some());
        assert!(!artifacts.check_evidence.is_empty());
        assert!(artifacts.review_subject.is_some());
        assert!(artifacts.review_receipt.is_none());
        assert!(artifacts.integration_receipt.is_none());
        assert!(verified.runtime_state.pending_effects.values().any(|effect| {
            effect.task_id.as_deref() == Some("task-complete-b")
                && effect.kind == PendingControllerEffectKind::TaskCommit
                && effect.status == PendingControllerEffectStatus::EffectConfirmed
        }));
        let (held_job, _) = fixture.workflow_store.apply_workflow_hold(&threadlane_native::coworker_workflow::ApplyWorkflowHoldRequest {
            binding_id: binding.binding_id.clone(), thread_uuid: thread.clone(), job_id: job.job_id.clone(),
            expected_job_revision: verified.job.job_revision, expected_ledger_revision: None,
            request_key: "hold_after_verified_task_commit".into(),
            reason: threadlane_native::coworker_workflow::WorkflowHoldReason::ConversationClarificationPending,
            detail: "Hold verified task before Reviewer dispatch".into(),
            directly_affected_task_ids: vec!["task-complete-b".into()], affected_write_paths: vec!["src/b.rs".into()],
            affected_interfaces: Vec::new(), holds_entire_job: false,
        }).unwrap();
        let held_task = held_job.tasks.iter().find(|task| task.task_id == "task-complete-b").unwrap();
        assert_eq!(held_task.stage, CoworkerTaskStage::Held);
        assert_eq!(held_task.stage_before_hold, Some(CoworkerTaskStage::ReadyForReview));
        assert!(held_task.candidates.is_empty());
        assert!(held_task.accepted_candidate_id.is_none());
        verification_session.release().unwrap();
        let held_before: Vec<_> = [&fixture.plans_dir, &fixture.registry_dir, &fixture.authority_dir, &fixture.sessions_dir]
            .into_iter().map(|dir| json_records(dir)).collect();
        let held_status = driver.status(&binding.binding_id, &thread, &job.job_id).await.unwrap();
        assert_eq!(held_status.host_phase, Some(HostPhase::Stopped));
        assert!(!held_status.host_lock_held);
        let held_observation = held_status.tasks.iter().find(|task| task.task_id == "task-complete-b").unwrap();
        assert_eq!(held_observation.stage, CoworkerTaskStage::Held);
        assert_eq!(held_observation.host_status, TaskExecutionStatus::ReadyForReview);
        assert!(held_observation.fenced);
        let held_pipeline = held_status.pipeline_report.as_ref().unwrap();
        assert_eq!(held_pipeline.unknown_effect_task_ids, vec!["task-inactive-a"]);
        assert_eq!(held_pipeline.tasks.iter().find(|task| task.task_id == "task-complete-b").unwrap().host_status, TaskExecutionStatus::ReadyForReview);
        assert_ne!(held_status.job_phase, CoworkerJobPhase::Completed);
        assert_eq!(driver.status(&binding.binding_id, &thread, &job.job_id).await.unwrap(), held_status);
        let held_after: Vec<_> = [&fixture.plans_dir, &fixture.registry_dir, &fixture.authority_dir, &fixture.sessions_dir]
            .into_iter().map(|dir| json_records(dir)).collect();
        assert_eq!(held_before, held_after, "held verification inspection must not dispatch or promote work");
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        control.shutdown().await;
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_preflight_rejects_oversized_later_task_before_host_startup_and_preserves_status_cancel_and_quiescent_paths(
) {
    Box::pin(async {
        let fixture = setup_fixture();
        let pb = project_base(&fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let brief_id = "brf_drv_oversized_later";
        let plan_id = "plan-drv-oversized-later";

        let valid_root_a_instr = format!("CTRL_TASK_A {}", "a".repeat(3_953));
        let oversized_task_b_instr = format!("CTRL_TASK_B {}", "b".repeat(4_353));
        assert_eq!(valid_root_a_instr.len(), 3_965);
        assert_eq!(oversized_task_b_instr.len(), 4_365);

        let tasks = vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Valid First Root A".to_string(),
                instruction: valid_root_a_instr,
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-b".to_string(),
                title: "Oversized Later Task B".to_string(),
                instruction: oversized_task_b_instr,
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: vec!["task-root-a".to_string()],
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
        ];

        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: format!("req_job_{brief_id}"),
                expected_ledger_revision: None,
                work_brief: sample_brief(&fixture, brief_id),
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create job");
        let grant = fixture
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: format!("req_grant_{brief_id}"),
                expected_ledger_revision: None,
                project_base: pb,
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                policy: planning_only_policy(),
                capabilities: PlanningReadOnlyCapabilities::strict_read_only(),
                budget: PlanningCallBudget {
                    max_total_calls: 4,
                    max_orchestrator_calls: 2,
                    max_architect_calls: 2,
                    max_timeout_ms_per_call: 30_000,
                    max_output_bytes_per_call: 65_536,
                },
                created_at_ms: 1_000,
                expires_at_ms: 60_000,
                operator_attested: true,
            })
            .expect("create planning grant");

        let structured_plan = StructuredOrchestratorOutput::Plan {
            schema_version: 1,
            summary: format!("Orchestrator plan `{plan_id}` for `{brief_id}`."),
            tasks: tasks
                .iter()
                .map(|t| StructuredPlannedTask {
                    task_id: t.id.clone(),
                    title: t.title.clone(),
                    description: t.instruction.clone(),
                    role: t.role,
                    difficulty: t.difficulty,
                    depends_on: t.depends_on.clone(),
                    allowed_write_paths: t.allowed_write_paths.clone(),
                    owned_interfaces: Vec::new(),
                    consumed_interfaces: Vec::new(),
                    model_override: None,
                    tool_override: None,
                })
                .collect(),
            check_suggestions: vec![PlannedCheckSuggestion {
                check_id: "cargo_test".to_string(),
                command_summary: "cargo test --workspace".to_string(),
                required: true,
            }],
            unconfirmed_proposal_notes: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
        };
        fs::write(
            &fixture.agy_plan_response_file,
            serde_json::to_string(&structured_plan).expect("serialize structured_plan"),
        )
        .expect("write agy_plan_response_file");

        let orch_req = OrchestrateWorkBriefPlanRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            grant_id: grant.grant_id.clone(),
            job_id: job.job_id.clone(),
            request_key: format!("req_orch_{brief_id}"),
            expected_ledger_revision: None,
            difficulty: Some(Difficulty::Hard),
            architect_difficulty: Some(Difficulty::Hard),
            plan_id: Some(plan_id.to_string()),
            expected_plan_revision: 0,
            architect_advice_ids: Vec::new(),
            max_architect_rounds: 0,
            current_time_ms: 2_000,
        };
        let fresh_plan_err = Box::pin(
            fixture
                .planning_service
                .orchestrate_brief_to_plan(&fixture.scoped_workflow, &orch_req),
        )
        .await
        .expect_err("fresh Orchestrator planning must reject oversized later task");
        assert_eq!(fresh_plan_err.code, NativeErrorCode::ValidationFailed);
        assert!(fresh_plan_err.message.contains("task-b"));
        assert!(fresh_plan_err.message.contains("4096 bytes"));
        assert!(!fixture
            .plans
            .snapshot()
            .expect("snapshot plans after fresh refusal")
            .iter()
            .any(|rev| rev.binding_id == BINDING_A));

        let ledger_after = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read binding ledger after fresh refusal");
        let grant_after = ledger_after
            .planning_grants
            .iter()
            .find(|g| g.grant_id == grant.grant_id)
            .expect("grant after fresh refusal");
        let completed_res = &grant_after.reservations[0];
        assert_eq!(
            completed_res.status,
            threadlane_native::coworker_workflow::PlanningReservationStatus::Completed
        );

        let draft = fixture
            .plans
            .change_at(
                0,
                threadlane_workflow::PlanChange::SaveDraft {
                    plan_id: plan_id.to_string(),
                    binding_id: BINDING_A.to_string(),
                    profile_id: "profile-coworker".to_string(),
                    profile_revision: reg_snap.revision,
                    summary: format!("Historical Orchestrator plan `{plan_id}`"),
                    tasks: tasks.clone(),
                },
                1_700_000_000_000,
            )
            .expect("save historical oversized draft via generic 65K PlanStore");
        let published_plan = fixture
            .plans
            .change(
                draft.revision,
                threadlane_workflow::PlanChange::Publish {
                    plan_id: plan_id.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish historical oversized plan");
        let compiled = threadlane_workflow::compile_plan_revision(&published_plan, &reg_snap)
            .expect("compile historical oversized plan");

        let seeded_pr = threadlane_native::coworker_workflow::contracts::PlanningResultRef {
            grant_id: grant_after.grant_id.clone(),
            reservation_id: completed_res.reservation_id.clone(),
            request_key: completed_res.request_key.clone(),
            run_id: completed_res.run_id.clone().expect("run_id"),
            report_artifact_id: completed_res
                .report_artifact_id
                .clone()
                .expect("report_artifact_id"),
            output_digest: completed_res.output_digest.clone().expect("output_digest"),
            work_brief_digest: job.work_brief_digest.clone(),
            project_base: project_base(&fixture),
            profile_id: grant_after.profile_id.clone(),
            registry_revision: grant_after.registry_revision,
            policy_digest: grant_after.policy_digest.clone(),
            selected_choice: completed_res.selected_choice.clone(),
            plan_id: published_plan.plan_id.clone(),
            plan_revision: published_plan.revision,
            plan_digest: compiled.plan_digest.clone(),
            plan_status: threadlane_workflow::PlanStatus::Published,
            task_interfaces: BTreeMap::new(),
            architect_advice_ids: Vec::new(),
            resolved_open_questions: Vec::new(),
            deferred_proposals: Vec::new(),
            recorded_at_ms: 2_500,
        };
        let job = fixture
            .workflow_store
            .record_job_planning_result(
                &threadlane_native::coworker_workflow::store::RecordJobPlanningResultRequest {
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: Some(job.job_revision),
                    expected_ledger_revision: None,
                    planning_result: seeded_pr,
                },
            )
            .expect("record historical PlanningResultRef");
        assert_eq!(job.phase, CoworkerJobPhase::AwaitingApproval);

        let approved_task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        let approved_write_paths = vec!["src/a.rs".to_string(), "src/b.rs".to_string()];
        let new_approval_err = fixture
            .authority
            .approve_at(
                OperatorApprovalRequest {
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    plan_id: published_plan.plan_id.clone(),
                    plan_revision: published_plan.revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: compiled.plan_digest.clone(),
                    repository_path: fixture.repo_dir.clone(),
                    base_commit: fixture.base_commit.clone(),
                    approved_task_ids: approved_task_ids.clone(),
                    approved_write_paths: approved_write_paths.clone(),
                    max_parallel: 1,
                    budgets: ExecutionBudgets::default(),
                },
                1_700_000_001_000,
            )
            .expect_err("new approval must reject oversized later task");
        assert!(new_approval_err.to_string().contains("task-b"));

        let canonical_repo = fs::canonicalize(&fixture.repo_dir).expect("canonical repo");
        let canonical_agy = fs::canonicalize(&fixture.fake_agy_exe).expect("canonical agy");

        let mut legacy_record = threadlane_authority::ApprovalRecord {
            approval_id: String::new(),
            subject_digest: String::new(),
            policy_version: threadlane_authority::AUTHORITY_POLICY_VERSION,
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            plan_id: published_plan.plan_id.clone(),
            plan_revision: published_plan.revision,
            profile_id: "profile-coworker".to_string(),
            registry_revision: reg_snap.revision,
            plan_digest: compiled.plan_digest,
            repository_path: canonical_repo,
            base_commit: fixture.base_commit.clone(),
            approved_task_ids,
            approved_write_paths,
            max_parallel: 1,
            budgets: ExecutionBudgets::default(),
            tasks: compiled
                .tasks
                .iter()
                .map(|ct| threadlane_authority::ApprovedTaskRecord {
                    task_id: ct.id.clone(),
                    title: ct.title.clone(),
                    instruction: ct.instruction.clone(),
                    instruction_digest: ct.instruction_digest.clone(),
                    role: ct.role,
                    difficulty: ct.difficulty,
                    tool_id: ct.tool_id.clone(),
                    adapter: ct.adapter,
                    provider: Some(threadlane_workers::WorkerProvider::Antigravity),
                    canonical_executable: Some(canonical_agy.clone()),
                    model_id: ct.choice.model_id.clone(),
                    model_arg: ct.model_arg.clone(),
                    base_model: ct.model_arg.clone(),
                    effort: ct.choice.effort.clone(),
                    cli_model_arg: Some(ct.model_arg.clone()),
                    standalone: Some(false),
                    file_permissions: Some(threadlane_workers::WorkerFilePermissions::TaskFiles),
                    depends_on: ct.depends_on.clone(),
                    allowed_write_paths: ct.allowed_write_paths.clone(),
                    task_digest: ct.task_digest.clone(),
                    profile_id: None,
                    command_key: None,
                })
                .collect(),
            created_at_ms: 1_700_000_001_000,
        };
        let core_digest =
            threadlane_authority::compute_authority_core_digest(&legacy_record).expect("core");
        for task in &mut legacy_record.tasks {
            task.profile_id = Some(threadlane_authority::derive_profile_id(
                &core_digest,
                &task.task_id,
                &task.task_digest,
            ));
            task.command_key = Some(threadlane_authority::derive_command_key(
                &core_digest,
                &task.task_id,
                &task.task_digest,
            ));
        }
        legacy_record.subject_digest =
            threadlane_authority::compute_record_subject_digest(&legacy_record).expect("subject");
        legacy_record.approval_id =
            threadlane_authority::derive_approval_id(&legacy_record.subject_digest);
        threadlane_authority::verify_approval_record_integrity(
            &legacy_record,
            fixture.authority.approvals_path(),
        )
        .expect("historical sealed approval integrity");

        let ledger_json = json!({
            "schema_version": threadlane_authority::AUTHORITY_SCHEMA_VERSION,
            "approvals": [&legacy_record],
        });
        fs::write(
            fixture.authority.approvals_path(),
            serde_json::to_vec(&ledger_json).expect("serialize approvals ledger"),
        )
        .expect("write legacy approvals.json");
        fs::set_permissions(
            fixture.authority.approvals_path(),
            fs::Permissions::from_mode(0o600),
        )
        .expect("chmod 0600 approvals.json");

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![headless_argon_choice(Difficulty::Hard)],
            allowed_closure_choices: vec![headless_argon_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: 1,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };
        let bound_job = fixture
            .workflow_store
            .bind_job_approval(
                &BindJobApprovalRequest {
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    approval_id: legacy_record.approval_id.clone(),
                    policy: execution_policy(),
                    controller_envelope: Some(cap_env),
                    task_interfaces: BTreeMap::new(),
                },
                &fixture.authority,
            )
            .expect("bind legacy approval");
        let completeness_receipt = build_independence_completeness_receipt(&bound_job);
        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(
            WorkflowRole::Coder,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        );
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 4),
        );
        role_budgets.insert(
            WorkflowRole::Closure,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Closure, 2),
        );
        let preview_req = FreezeControllerExecutionEnvelopeRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: bound_job.job_id.clone(),
            expected_job_revision: bound_job.job_revision,
            policy: execution_policy(),
            reviewer_choice: headless_argon_choice(Difficulty::Hard),
            closure_choice: headless_argon_choice(Difficulty::Extreme),
            role_budgets,
            max_in_scope_corrections_per_task: 1,
            max_total_corrections_per_job: 2,
            max_total_task_attempts_per_job: 6,
            check_commands: default_check_commands(&fixture),
            allow_task_worktree_commits: true,
            allow_run_integration_execution: true,
            default_integration_strategy: IntegrationStrategy::MergeNoFf,
            independence_completeness_receipt: Some(completeness_receipt),
            shadow_jev_policy: None,
            created_at_ms: 1_700_000_100_000,
            operator_attested: false,
            confirm_approval_subject_digest: None,
            confirm_envelope_digest: None,
        };
        let preview = fixture
            .policy_store
            .preview_execution_envelope(&preview_req, &bound_job, &fixture.authority)
            .expect("preview execution envelope");
        let mut freeze_req = preview_req;
        freeze_req.operator_attested = true;
        freeze_req.confirm_approval_subject_digest = Some(preview.approval_subject_digest);
        freeze_req.confirm_envelope_digest = Some(preview.envelope_digest);
        fixture
            .policy_store
            .record_execution_envelope(&freeze_req, &bound_job, &fixture.authority)
            .expect("record execution envelope");

        let driver = CoworkerCodeDriver::from_controller(
            fixture.root.join("runtime"),
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            Arc::clone(&fixture.authority),
            fixture.controller.clone(),
        )
        .expect("create driver");
        let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, &bound_job.job_id)
            .expect("ownership key");
        let (socket, ws_count, peer) =
            spawn_single_coworker_notice_ws_server(&fixture, THREAD_UUID_A);
        let notices = coworker_notice_dir(&fixture.plans_dir);
        let digest = preview.envelope.envelope_digest;
        CoworkerNoticeStore::new(&notices)
            .expect("open notice store")
            .configure(
                CoworkerNoticeConfig::new(
                    key.clone(),
                    digest.clone(),
                    NativeToolOutputConfig::new_user_message_steer(
                        socket,
                        THREAD_UUID_A.into(),
                        fixture.repo_dir.clone(),
                    ),
                )
                .expect("notice config"),
            )
            .expect("configure notice store");

        // 1. Every effect-capable driver entrypoint (`run-owner`, `reconcile`, `ensure`) across
        // both execution modes must refuse BEFORE `ensure_host` or first dispatch!
        use threadlane_native::coworker_driver::{
            CoworkerDriverExecutionMode, CoworkerDriverRunOptions,
        };
        use threadlane_native::coworker_workflow::CoworkerControllerPhase1Status;
        for mode in [
            CoworkerDriverExecutionMode::Phase1InitialDispatchOnly,
            CoworkerDriverExecutionMode::FullAutomaticPipeline,
        ] {
            let options = CoworkerDriverRunOptions {
                execution_mode: Some(mode),
                max_steps: Some(1),
                ..Default::default()
            };
            for operation in ["run-owner", "reconcile", "ensure"] {
                let error = match operation {
                    "run-owner" => driver
                        .run_owner(BINDING_A, THREAD_UUID_A, &bound_job.job_id, options)
                        .await
                        .expect_err("run_owner preflight must refuse oversized later task"),
                    "reconcile" => driver
                        .reconcile_with_options(
                            BINDING_A,
                            THREAD_UUID_A,
                            &bound_job.job_id,
                            Some(options),
                        )
                        .await
                        .expect_err("reconcile preflight must refuse oversized later task"),
                    _ => driver
                        .ensure_owner(
                            BINDING_A,
                            THREAD_UUID_A,
                            &bound_job.job_id,
                            Some(options),
                        )
                        .await
                        .expect_err("ensure preflight must refuse oversized later task"),
                };
                assert_eq!(error.code, NativeErrorCode::ValidationFailed);
                assert!(error.message.contains("task-b"));
                assert!(error.message.contains("4096 bytes"));
                assert!(
                    driver.read_owner_failure_optional(&key).unwrap().is_none(),
                    "{operation}/{mode:?} refusal must not record fatal owner failure"
                );
                assert!(
                    driver.read_driver_state_optional(&key).unwrap().is_none(),
                    "{operation}/{mode:?} refusal must not persist driver state"
                );
                assert_eq!(
                    CoworkerNoticeStore::inspect(&notices, &key, &digest)
                        .unwrap()
                        .attempt_count,
                    0
                );
                assert_eq!(ws_count.load(Ordering::SeqCst), 0);
                assert_eq!(
                    fs::read_dir(&fixture.sessions_dir).unwrap().count(),
                    0,
                    "{operation}/{mode:?} must fail before ensure_host creates session dir"
                );
                let rt = fixture
                    .policy_store
                    .get_job_runtime_readonly(BINDING_A, &bound_job.job_id)
                    .unwrap()
                    .unwrap();
                assert!(rt.lease.is_none());
                assert_eq!(rt.total_task_attempts_reserved, 0);
                assert!(rt.pending_effects.is_empty());
                assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
                assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
            }
        }

        // 2. Read-only `status` and durable `cancel` succeed on the legacy oversized job before any
        // host exists; a bounded in-process stopped `run_owner` then establishes genuine quiescent
        // stopped proof (preparing and cleanly stopping the local host with zero task dispatches or
        // provider invocations), and subsequent `ensure_owner`, `reconcile_with_options`, and
        // `run_owner` reuse that stopped proof without starting another host.
        let status_before = driver
            .status(BINDING_A, THREAD_UUID_A, &bound_job.job_id)
            .await
            .expect("status must remain readable on legacy oversized job");
        assert!(status_before.host_phase.is_none());
        assert!(!status_before.host_lock_held);
        assert!(!status_before.stop_requested);
        assert_eq!(fs::read_dir(&fixture.sessions_dir).unwrap().count(), 0);

        let cancel_report = driver
            .cancel(
                BINDING_A,
                THREAD_UUID_A,
                &bound_job.job_id,
                Some("operator_stop_legacy_oversized".to_string()),
            )
            .await
            .expect("cancel must succeed on legacy oversized job without starting host");
        assert!(cancel_report.durable_stop_recorded_before_host_cancel);
        assert!(cancel_report.status.stop_requested);
        assert_eq!(
            cancel_report.status.overall_status,
            CoworkerControllerPhase1Status::Stopped
        );
        assert_eq!(fs::read_dir(&fixture.sessions_dir).unwrap().count(), 0);

        // Establish genuine bounded in-process stopped owner proof (`max_steps: 1`, `idle_linger_ms: 0`)
        // on the durably cancelled legacy job (which prepares and cleanly stops the local host with
        // zero task dispatch intents, receipts, or provider runs), then verify `ensure_owner`,
        // `reconcile_with_options`, and subsequent `run_owner` reuse `QuiescentNoHostRequired` /
        // `AlreadyQuiescentVerified` across both execution modes without starting another host.
        let initial_stopped_owner = driver
            .run_owner(
                BINDING_A,
                THREAD_UUID_A,
                &bound_job.job_id,
                CoworkerDriverRunOptions {
                    execution_mode: Some(CoworkerDriverExecutionMode::Phase1InitialDispatchOnly),
                    max_steps: Some(1),
                    idle_linger_ms: Some(0),
                    ..Default::default()
                },
            )
            .await
            .expect("bounded in-process stopped run_owner must establish quiescent proof");
        assert_eq!(
            initial_stopped_owner.overall_status,
            CoworkerControllerPhase1Status::Stopped
        );
        assert!(initial_stopped_owner.stop_requested);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 0);

        fn snapshot_session_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
            let mut out = BTreeMap::new();
            for entry in fs::read_dir(root).expect("read_dir session tree") {
                let path = entry.expect("session tree entry").path();
                let meta = fs::symlink_metadata(&path).expect("symlink_metadata session entry");
                if meta.file_type().is_dir() {
                    out.extend(snapshot_session_tree(&path));
                } else if meta.file_type().is_file() {
                    out.insert(path.clone(), Some(fs::read(&path).expect("read session file")));
                } else {
                    out.insert(path, None);
                }
            }
            out
        }

        let stopped_session_dir = fixture.sessions_dir.join(&legacy_record.approval_id);
        let stopped_manifest =
            threadlane_host::read_host_manifest_optional(&stopped_session_dir, &legacy_record)
                .expect("read stopped host manifest")
                .expect("stopped host manifest must exist");
        assert_eq!(stopped_manifest.phase, HostPhase::Stopped);
        assert!(
            !threadlane_host::is_host_lock_held(&stopped_session_dir).expect("inspect host lock")
        );
        let (stopped_intents, stopped_receipts) = threadlane_host::read_session_task_ledger_readonly(
            &stopped_session_dir,
            &legacy_record,
            stopped_manifest
                .session_binding_id
                .as_deref()
                .expect("stopped session_binding_id"),
        )
        .expect("read stopped host task ledger")
        .unwrap_or_default();
        assert!(stopped_intents.is_empty(), "zero host dispatch intents");
        assert!(stopped_receipts.is_empty(), "zero host task receipts");
        let stopped_manifest_path = stopped_session_dir.join(threadlane_host::HOST_MANIFEST_FILENAME);
        let stopped_manifest_bytes =
            fs::read(&stopped_manifest_path).expect("snapshot stopped host-manifest.json");
        let stopped_sessions_snapshot = snapshot_session_tree(&fixture.sessions_dir);

        for mode in [
            CoworkerDriverExecutionMode::Phase1InitialDispatchOnly,
            CoworkerDriverExecutionMode::FullAutomaticPipeline,
        ] {
            let mode_opts = CoworkerDriverRunOptions {
                execution_mode: Some(mode),
                max_steps: Some(1),
                idle_linger_ms: Some(0),
                ..Default::default()
            };

            let ensure_quiescent = driver
                .ensure_owner(
                    BINDING_A,
                    THREAD_UUID_A,
                    &bound_job.job_id,
                    Some(mode_opts),
                )
                .await
                .expect("quiescent stopped ensure_owner must reuse stopped proof without starting another host");
            assert_eq!(
                ensure_quiescent.disposition,
                CoworkerDriverEnsureDisposition::AlreadyQuiescentVerified
            );
            assert_eq!(
                ensure_quiescent.status.overall_status,
                CoworkerControllerPhase1Status::Stopped
            );
            assert_eq!(
                ensure_quiescent.status.host_mode,
                threadlane_native::coworker_driver::CoworkerDriverHostMode::QuiescentNoHostRequired
            );
            assert!(
                !threadlane_host::is_host_lock_held(&stopped_session_dir)
                    .expect("inspect host lock after ensure")
            );
            assert_eq!(
                fs::read(&stopped_manifest_path).expect("read manifest after ensure"),
                stopped_manifest_bytes
            );
            assert_eq!(
                snapshot_session_tree(&fixture.sessions_dir),
                stopped_sessions_snapshot
            );

            let reconcile_quiescent = driver
                .reconcile_with_options(
                    BINDING_A,
                    THREAD_UUID_A,
                    &bound_job.job_id,
                    Some(mode_opts),
                )
                .await
                .expect("quiescent stopped reconcile must reuse stopped proof without starting another host");
            assert_eq!(
                reconcile_quiescent.overall_status,
                CoworkerControllerPhase1Status::Stopped
            );
            assert_eq!(
                reconcile_quiescent.host_mode,
                threadlane_native::coworker_driver::CoworkerDriverHostMode::QuiescentNoHostRequired
            );
            assert!(
                !threadlane_host::is_host_lock_held(&stopped_session_dir)
                    .expect("inspect host lock after reconcile")
            );
            assert_eq!(
                fs::read(&stopped_manifest_path).expect("read manifest after reconcile"),
                stopped_manifest_bytes
            );
            assert_eq!(
                snapshot_session_tree(&fixture.sessions_dir),
                stopped_sessions_snapshot
            );

            let run_quiescent = driver
                .run_owner(BINDING_A, THREAD_UUID_A, &bound_job.job_id, mode_opts)
                .await
                .expect("quiescent stopped run_owner must reuse stopped proof without starting another host");
            assert_eq!(
                run_quiescent.overall_status,
                CoworkerControllerPhase1Status::Stopped
            );
            assert_eq!(
                run_quiescent.host_mode,
                threadlane_native::coworker_driver::CoworkerDriverHostMode::QuiescentNoHostRequired
            );
            assert!(
                !threadlane_host::is_host_lock_held(&stopped_session_dir)
                    .expect("inspect host lock after run_owner")
            );
            assert_eq!(
                fs::read(&stopped_manifest_path).expect("read manifest after run_owner"),
                stopped_manifest_bytes
            );
            assert_eq!(
                snapshot_session_tree(&fixture.sessions_dir),
                stopped_sessions_snapshot
            );
            assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
            assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
        }

        peer.abort();
    })
    .await;
}

#[path = "coworker_registry_drift.rs"]
mod coworker_registry_drift;
