use super::*;
use serde_json::Value;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
use threadlane_native::coworker_driver::{CoworkerCodeDriver, CoworkerDriverEnsureDisposition};
use threadlane_native::coworker_workflow::{CoworkerJobLockState, CoworkerJobOwnershipKey};
use threadlane_native::planning_driver::planning_artifacts_dir;

async fn run_flow_cli_pipeline(
    state_dir: &Path,
    fixture: &PipelineTestFixture,
    binding_id: &str,
    thread_uuid: &str,
    subcommand_args: &[&str],
) -> Value {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_threadlane-flow"))
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
        .expect("spawn threadlane-flow subprocess");
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
                    binding_id: binding_id.to_string(),
                    thread_uuid: thread_uuid.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    approval_id: approval_summary.approval_id.clone(),
                    policy: execution_policy(),
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
            policy: execution_policy(),
            reviewer_choice: headless_argon_choice(Difficulty::Hard),
            closure_choice: headless_argon_choice(Difficulty::Extreme),
            role_budgets,
            max_in_scope_corrections_per_task: 1,
            max_total_corrections_per_job: 2,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_full_automatic_pipeline_runs_parallel_roots_reviewer_correction_dependent_and_single_closure_to_completed(
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

        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope(
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
        )
        .await;

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
        let ownership_key = CoworkerJobOwnershipKey::new(&binding_id, &thread_uuid, &job.job_id)
            .expect("construct ownership key");
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

        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope(
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
