use super::*;
use threadlane_native::coworker_notice::NoticeKind;
use threadlane_native::coworker_workflow::candidate::{
    resolve_tool_executable, resolve_verified_closure_tool_executable,
};
use threadlane_native::coworker_workflow::{
    read_persisted_task_review_report, verify_frozen_runtime_registry_or_apply_config_hold,
    CombinedCandidateStatus, CoworkerLedgerMutationOutcome, ResolveWorkflowHoldRequest,
    TaskReviewExecutionStatus, WorkflowHoldRecord,
};
use threadlane_workflow::RegistrySnapshot;

fn is_registry_configuration_changed_hold(hold: &WorkflowHoldRecord) -> bool {
    hold.reason == WorkflowHoldReason::RegistryConfigurationChanged
        && serde_json::to_value(hold.reason).expect("serialize WorkflowHoldReason")
            == json!("registry_configuration_changed")
}

fn apply_registry_change(
    fixture: &PipelineTestFixture,
    change: RegistryChange,
) -> RegistrySnapshot {
    let snap = fixture.registry.snapshot().expect("registry snapshot");
    fixture
        .registry
        .apply(snap.revision, change)
        .expect("apply registry change")
}

fn default_two_root_tasks() -> (
    Vec<PlanTask>,
    BTreeMap<String, TaskInterfaceOwnershipSpec>,
    Vec<String>,
) {
    let tasks = vec![
        PlanTask {
            id: "task-root-a".to_string(),
            title: "Implement root module A".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        },
        PlanTask {
            id: "task-root-b".to_string(),
            title: "Implement root module B".to_string(),
            instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/b.rs".to_string()],
        },
    ];
    let mut ifaces = BTreeMap::new();
    ifaces.insert(
        "task-root-a".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_root_a".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    ifaces.insert(
        "task-root-b".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_root_b".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let write_paths = vec!["src/a.rs".to_string(), "src/b.rs".to_string()];
    (tasks, ifaces, write_paths)
}

async fn create_two_task_frozen_job(
    fixture: &PipelineTestFixture,
    brief_id: &str,
    plan_id: &str,
) -> (CoworkerJobRecord, String, ControllerJobRuntimeState) {
    create_two_task_frozen_job_with_policy(fixture, brief_id, plan_id, &execution_policy()).await
}

async fn create_two_task_frozen_job_with_policy(
    fixture: &PipelineTestFixture,
    brief_id: &str,
    plan_id: &str,
    policy: &ExecutionPolicy,
) -> (CoworkerJobRecord, String, ControllerJobRuntimeState) {
    Box::pin(async move {
        let (tasks, task_interfaces, approved_write_paths) = default_two_root_tasks();
        let pb = project_base(fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let brief = sample_brief(fixture, brief_id);

        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
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
        let plan_outcome = Box::pin(
            fixture
                .planning_service
                .orchestrate_brief_to_plan(&fixture.scoped_workflow, &orch_req),
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
        let _verified_artifact = fixture
            .planning_service
            .read_planning_artifact(&published_outcome.planning_result.report_artifact_id)
            .expect("verify persisted planning artifact on disk");

        let job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
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
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    plan_id: published_outcome.planning_result.plan_id.clone(),
                    plan_revision: published_outcome.planning_result.plan_revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: published_outcome.planning_result.plan_digest.clone(),
                    repository_path: fixture.repo_dir.clone(),
                    base_commit: fixture.base_commit.clone(),
                    approved_task_ids,
                    approved_write_paths,
                    max_parallel: 2,
                    budgets: ExecutionBudgets::default(),
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
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    job_id: job.job_id,
                    expected_job_revision: job.job_revision,
                    expected_ledger_revision: None,
                    approval_id: approval_summary.approval_id.clone(),
                    policy: policy.clone(),
                    controller_envelope: Some(cap_env),
                    task_interfaces,
                },
                &fixture.authority,
            )
            .expect("bind job approval");

        let completeness_receipt = Some(build_independence_completeness_receipt(&bound_job));

        let mut role_budgets = BTreeMap::new();
        role_budgets.insert(
            WorkflowRole::Coder,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        );
        role_budgets.insert(
            WorkflowRole::Reviewer,
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Reviewer, 6),
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
            policy: policy.clone(),
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
            independence_completeness_receipt: completeness_receipt,
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
        let runtime_state = fixture
            .policy_store
            .record_execution_envelope(&freeze_req, &bound_job, &fixture.authority)
            .expect("record execution envelope");

        (bound_job, approval_summary.approval_id, runtime_state)
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn cli_code_driver_live_owner_registry_drift_holds_without_aborting_active_futures_and_emits_single_attention_notice(
) {
    Box::pin(async {
        let fixture = setup_fixture();
        let state_dir = fixture.root.join("runtime-registry-drift");
        fs::create_dir_all(&state_dir).expect("create runtime state_dir");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 runtime state_dir");

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start disposable supervisor");
        let supervisor_client =
            SupervisorClient::from_state_dir(&state_dir).expect("create supervisor client");
        let binding = supervisor_client
            .create_binding(THREAD_UUID_A, "live-owner-registry-drift-test")
            .await
            .expect("create actual supervisor binding");
        let binding_id = binding.binding_id;
        let thread_uuid = binding.thread_uuid;

        let (tasks, ifaces, write_paths) = default_two_root_tasks();
        let (job, approval_id) = create_planned_approved_and_frozen_pipeline_job_for_scope(
            &fixture,
            &binding_id,
            &thread_uuid,
            "brf_live_owner_reg_drift",
            "plan-live-owner-reg-drift",
            tasks,
            ifaces,
            write_paths,
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            1,
        )
        .await;

        let (notice_socket_path, notice_effect_count, notice_server) =
            spawn_single_coworker_notice_ws_server(&fixture, &thread_uuid);
        let notice_root = coworker_notice_dir(&fixture.plans_dir);
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

        let hold_initial_b_gate = fixture.root.join("hold_initial_coder_b.gate");
        let entered_initial_b_marker = fixture.root.join("entered_initial_coder_b.marker");
        let hold_reviewer_a_gate = fixture.root.join("hold_reviewer_task-root-a.gate");
        let entered_reviewer_a_marker = fixture.root.join("reviewer_entered_task-root-a.marker");
        let exited_reviewer_a_marker = fixture.root.join("reviewer_exited_task-root-a.marker");
        fs::write(&hold_initial_b_gate, "hold\n").expect("create hold_initial_coder_b.gate");
        fs::write(
            &hold_reviewer_a_gate,
            "{\"fallback_timeout_ms\":180000}\n",
        )
        .expect("create hold_reviewer_task-root-a.gate");

        let orig_agy = fs::read_to_string(&fixture.fake_agy_exe).expect("read fake_agy_exe");
        let exec_line = orig_agy
            .lines()
            .find(|l| l.starts_with("exec node "))
            .expect("find exec node line in fake_agy_exe")
            .to_string();
        write_executable_script(
            &fixture.fake_agy_exe,
            &format!(
                "#!/bin/sh\nset -eu\n\
                 case \"$*\" in\n\
                   *\"--mode plan\"*) ;;\n\
                   *\"CTRL_TASK_B\"*)\n\
                     if [ -f '{gate}' ]; then\n\
                       printf 'entered\\n' > '{entered}'\n\
                       waited=0\n\
                       while [ -f '{gate}' ] && [ \"$waited\" -lt 1500 ]; do\n\
                         sleep 0.02\n\
                         waited=$((waited + 1))\n\
                       done\n\
                     fi\n\
                     ;;\n\
                 esac\n\
                 {exec_line}\n",
                gate = hold_initial_b_gate.display(),
                entered = entered_initial_b_marker.display(),
            ),
        );

        let ensure_val = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "ensure", "--job-id", &job.job_id],
        )
        .await;
        assert_eq!(ensure_val["disposition"], "spawned_new_owner");

        // Wait with a finite deadline until Task B is gated inside its live Worker process and Task A
        // has finished verification and is gated inside its live Reviewer process.
        // Do NOT panic inside the wait loop while gates are still held.
        let phase_deadline = Instant::now() + Duration::from_secs(30);
        let mut pre_drift_status = Value::Null;
        let mut last_status_diag = String::from("not_polled");
        let mut phase_timeout_diag: Option<String> = None;
        loop {
            if entered_initial_b_marker.is_file()
                && entered_reviewer_a_marker.is_file()
                && read_invocation_count(&fixture.agy_coder_count_file) == 1
                && read_invocation_count(&fixture.agy_reviewer_count_file) == 1
                && read_invocation_count(&fixture.check_count_file) == 1
            {
                let status_out = run_flow_cli_pipeline_output(
                    &state_dir,
                    &fixture,
                    &binding_id,
                    &thread_uuid,
                    &["code-driver", "status", "--job-id", &job.job_id],
                )
                .await;
                if status_out.status.success() {
                    let envelope: Value = serde_json::from_slice(&status_out.stdout)
                        .expect("parse pre-drift status envelope");
                    let status_val = envelope["data"].clone();
                    let task_stages: Vec<String> = status_val["tasks"]
                        .as_array()
                        .map(|tasks| {
                            tasks
                                .iter()
                                .map(|t| {
                                    format!(
                                        "{}:{}/{}",
                                        t["task_id"].as_str().unwrap_or("?"),
                                        t["stage"].as_str().unwrap_or("?"),
                                        t["host_status"].as_str().unwrap_or("?")
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    last_status_diag = format!(
                        "owner_state={}, job_phase={}, tasks={:?}, in_flight={}",
                        status_val["owner_state"],
                        status_val["job_phase"],
                        task_stages,
                        status_val["pipeline_report"]["in_flight_stages"],
                    );
                    let task_a_in_review = status_val["pipeline_report"]["in_flight_stages"]
                        .as_array()
                        .is_some_and(|stages| {
                            stages.iter().any(|s| {
                                s["task_id"] == "task-root-a" && s["kind"] == "task_review"
                            })
                        });
                    let task_b_running = status_val["tasks"]
                        .as_array()
                        .is_some_and(|tasks| {
                            tasks.iter().any(|t| {
                                t["task_id"] == "task-root-b" && t["stage"] == "running"
                            })
                        });
                    if status_val["owner_state"] == "live_verified_owner"
                        && task_a_in_review
                        && task_b_running
                    {
                        pre_drift_status = status_val;
                        break;
                    }
                } else {
                    last_status_diag = format!("rpc_exit={:?}", status_out.status.code());
                }
            }
            if Instant::now() >= phase_deadline {
                phase_timeout_diag = Some(format!(
                    "timed out waiting for gated Worker B + gated Reviewer A boundary (entered_b={}, entered_rev_a={}, coder={}, reviewer={}, check={}, last_status=[{}])",
                    entered_initial_b_marker.is_file(),
                    entered_reviewer_a_marker.is_file(),
                    read_invocation_count(&fixture.agy_coder_count_file),
                    read_invocation_count(&fixture.agy_reviewer_count_file),
                    read_invocation_count(&fixture.check_count_file),
                    last_status_diag,
                ));
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut fence_timeout_diag: Option<String> = None;
        let mut in_flight_observation: Option<(
            CoworkerJobRecord,
            ControllerJobRuntimeState,
            u32,
            u32,
            bool,
            Option<Value>,
        )> = None;

        if phase_timeout_diag.is_none() {
            // Disable the selected tool through the real RegistryStore at the live in-flight boundary.
            apply_registry_change(
                &fixture,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-agy".to_string(),
                        label: "Antigravity CLI".to_string(),
                        adapter: ToolAdapter::Antigravity,
                        executable_path: Some(fixture.fake_agy_exe.clone()),
                        enabled: false,
                    },
                },
            );

            // Wait for the live owner's periodic tick to observe the registry change while B and
            // Reviewer A are still physically gated.
            let fence_deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let ledger = fixture
                    .workflow_store
                    .read_binding_ledger(&binding_id)
                    .expect("read binding ledger during drift");
                let cur_job = ledger
                    .jobs
                    .iter()
                    .find(|j| j.job_id == job.job_id)
                    .expect("find job")
                    .clone();
                let cur_runtime = fixture
                    .policy_store
                    .get_job_runtime_readonly(&binding_id, &job.job_id)
                    .expect("read runtime during drift")
                    .expect("runtime exists");
                if cur_job.phase == CoworkerJobPhase::Held
                    || !cur_job.holds.is_empty()
                    || cur_runtime.stop_requested
                {
                    let in_flight_coder_count =
                        read_invocation_count(&fixture.agy_coder_count_file);
                    let in_flight_reviewer_count =
                        read_invocation_count(&fixture.agy_reviewer_count_file);
                    let in_flight_exited_reviewer_a = exited_reviewer_a_marker.is_file();
                    let in_flight_status = if !cur_runtime.stop_requested {
                        let out = run_flow_cli_pipeline_output(
                            &state_dir,
                            &fixture,
                            &binding_id,
                            &thread_uuid,
                            &["code-driver", "status", "--job-id", &job.job_id],
                        )
                        .await;
                        serde_json::from_slice::<Value>(&out.stdout)
                            .ok()
                            .and_then(|env| env.get("data").cloned())
                    } else {
                        None
                    };
                    in_flight_observation = Some((
                        cur_job,
                        cur_runtime,
                        in_flight_coder_count,
                        in_flight_reviewer_count,
                        in_flight_exited_reviewer_a,
                        in_flight_status,
                    ));
                    break;
                }
                if Instant::now() >= fence_deadline {
                    fence_timeout_diag = Some(
                        "timed out waiting for live owner to observe registry drift while futures are gated"
                            .to_string(),
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        // Always release both owned gates and drain/stop the owner process + supervisor before any
        // assertion or timeout panic so failures never leak background processes or hang.
        let _ = fs::remove_file(&hold_initial_b_gate);
        let _ = fs::remove_file(&hold_reviewer_a_gate);

        if phase_timeout_diag.is_some() || fence_timeout_diag.is_some() {
            let _ = run_flow_cli_pipeline_output(
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
                    "test_timeout_cleanup",
                ],
            )
            .await;
        }

        let settle_deadline = Instant::now() + Duration::from_secs(30);
        let mut settled_status = Value::Null;
        let mut settle_timeout = false;
        loop {
            let out = run_flow_cli_pipeline_output(
                &state_dir,
                &fixture,
                &binding_id,
                &thread_uuid,
                &["code-driver", "status", "--job-id", &job.job_id],
            )
            .await;
            if out.status.success() {
                let envelope: Value =
                    serde_json::from_slice(&out.stdout).expect("parse settled status envelope");
                let val = envelope["data"].clone();
                if val["owner_state"] == "no_active_owner" {
                    settled_status = val;
                    break;
                }
            }
            if Instant::now() >= settle_deadline {
                settle_timeout = true;
                let _ = run_flow_cli_pipeline_output(
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
                        "test_settle_timeout_cleanup",
                    ],
                )
                .await;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        if let Some(diag) = phase_timeout_diag {
            notice_server.abort();
            let _ = notice_server.await;
            supervisor.shutdown().await;
            panic!("{diag}");
        }
        if let Some(diag) = fence_timeout_diag {
            notice_server.abort();
            let _ = notice_server.await;
            supervisor.shutdown().await;
            panic!("{diag}");
        }
        if settle_timeout {
            notice_server.abort();
            let _ = notice_server.await;
            supervisor.shutdown().await;
            panic!(
                "timed out waiting for owner to settle after releasing gates (coder={}, reviewer={}, check={})",
                read_invocation_count(&fixture.agy_coder_count_file),
                read_invocation_count(&fixture.agy_reviewer_count_file),
                read_invocation_count(&fixture.check_count_file),
            );
        }

        let final_ledger = fixture
            .workflow_store
            .read_binding_ledger(&binding_id)
            .expect("read final ledger");
        let final_job = final_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find final job")
            .clone();
        let final_runtime = fixture
            .policy_store
            .get_job_runtime_readonly(&binding_id, &job.job_id)
            .expect("read final runtime")
            .expect("final runtime exists");

        let repeat_status = run_flow_cli_pipeline(
            &state_dir,
            &fixture,
            &binding_id,
            &thread_uuid,
            &["code-driver", "status", "--job-id", &job.job_id],
        )
        .await;

        let reopened_driver = CoworkerCodeDriver::open_from_dirs(
            &state_dir,
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
        )
        .expect("reopen CoworkerCodeDriver");
        let reopened_status = reopened_driver
            .status(&binding_id, &thread_uuid, &job.job_id)
            .await
            .expect("reopened driver status");
        let notice_metadata =
            CoworkerNoticeStore::inspect(&notice_root, &ownership_key, &envelope_digest)
                .expect("inspect notice metadata");

        let _ = tokio::time::timeout(Duration::from_secs(5), notice_server).await;
        supervisor.shutdown().await;
        let _ = approval_id;

        assert_eq!(pre_drift_status["owner_state"], "live_verified_owner");
        let (
            in_flight_job,
            in_flight_runtime,
            in_flight_coder_count,
            in_flight_reviewer_count,
            in_flight_exited_reviewer_a,
            in_flight_status,
        ) = in_flight_observation.expect("in-flight observation recorded");

        // 1. Verify in-flight boundary behavior: configuration hold minted without stop_requested or
        // aborting active B / Reviewer A futures on their original frozen pins.
        assert!(
            !in_flight_runtime.stop_requested,
            "registry drift must not set stop_requested while futures are active"
        );
        assert_eq!(in_flight_job.phase, CoworkerJobPhase::Held);
        assert_eq!(in_flight_job.holds.len(), 1);
        let active_hold = &in_flight_job.holds[0];
        assert!(active_hold.active);
        assert!(active_hold.holds_entire_job);
        assert!(
            is_registry_configuration_changed_hold(active_hold),
            "expected hold reason `registry_configuration_changed`, got {:?}",
            active_hold.reason
        );
        assert_eq!(
            in_flight_coder_count, 1,
            "Worker B must not increment provider counter until its gate is released"
        );
        assert_eq!(in_flight_reviewer_count, 1);
        assert!(
            !in_flight_exited_reviewer_a,
            "Reviewer A future must remain physically active until its gate is released"
        );
        if let Some(ref live_status) = in_flight_status {
            assert_eq!(live_status["fatal_error"], Value::Null);
            assert_eq!(live_status["stop_requested"], false);
            assert_eq!(live_status["job_phase"], "held");
        }

        let trunc_diag = |opt: &Option<String>| -> Option<String> {
            opt.as_ref()
                .map(|s| s.chars().take(200).collect::<String>())
        };
        let diag_effects: Vec<_> = final_runtime
            .pending_effects
            .values()
            .take(8)
            .map(|e| (e.kind, e.status, e.task_id.clone(), trunc_diag(&e.diagnostic)))
            .collect();
        let diag_outbox: Vec<_> = final_job
            .outbox
            .iter()
            .take(8)
            .map(|o| (o.kind, o.status, o.task_id.clone(), trunc_diag(&o.diagnostic)))
            .collect();
        eprintln!(
            "settled_diag: pipeline_status={}, fatal_error={}, pending_effects={:?}, outbox={:?}",
            settled_status["pipeline_status"],
            settled_status["fatal_error"],
            diag_effects,
            diag_outbox,
        );

        // 2. Verify settled Held state after releasing gates: no fatal_error, stop_requested=false,
        // both actual results retained on original frozen pins, and neither accepted nor integrated.
        assert_eq!(settled_status["owner_state"], "no_active_owner");
        assert_eq!(settled_status["job_phase"], "held");
        assert_eq!(settled_status["overall_status"], "held");
        assert_eq!(settled_status["pipeline_status"], "held");
        assert_eq!(settled_status["fatal_error"], Value::Null);
        assert_eq!(settled_status["stop_requested"], false);
        assert!(!final_runtime.stop_requested);
        assert_eq!(final_job.phase, CoworkerJobPhase::Held);
        assert!(final_job.combined_candidates.is_empty());

        let task_a = final_job
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .expect("task-root-a");
        let task_b = final_job
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .expect("task-root-b");
        assert_eq!(task_a.stage, CoworkerTaskStage::Held);
        assert_eq!(task_b.stage, CoworkerTaskStage::Held);
        assert!(task_a.accepted_candidate_id.is_none());
        assert!(task_b.accepted_candidate_id.is_none());
        assert!(
            task_b.candidates.is_empty(),
            "held Worker B result must not advance to task verification/review candidate promotion"
        );
        assert!(
            task_a
                .candidates
                .iter()
                .all(|c| c.review_receipt_id.is_none() && c.integration_receipt_id.is_none()),
            "held Reviewer A output must not mint a review receipt or integration receipt"
        );

        let art_a = final_runtime
            .get_attempt_artifacts("task-root-a", 1)
            .expect("retained artifacts for task-root-a attempt 1");
        assert_eq!(
            art_a.observed_model.as_deref(),
            Some("gemini-4-argon-high"),
            "task-root-a must execute on the original frozen model pin"
        );
        assert_eq!(art_a.observed_effort.as_deref(), Some("high"));
        assert!(
            art_a.reviewer_response_digest.is_some(),
            "Reviewer A response digest must be retained after draining under configuration hold"
        );
        assert!(art_a.review_receipt.is_none());
        assert!(art_a.integration_receipt.is_none());
        let review_eff = final_runtime
            .pending_effects
            .values()
            .find(|e| {
                e.kind == PendingControllerEffectKind::ReviewerInvocation
                    && e.task_id.as_deref() == Some("task-root-a")
                    && e.attempt_no == Some(1)
            })
            .expect("ReviewerInvocation effect for task-root-a");
        assert_eq!(
            review_eff.status,
            PendingControllerEffectStatus::EffectConfirmed
        );
        let persisted_review_report =
            read_persisted_task_review_report(&fixture.plans_dir, &review_eff.effect_id)
                .expect("read persisted task review report")
                .expect("persisted task review report must exist");
        assert_eq!(
            persisted_review_report.status,
            TaskReviewExecutionStatus::VerifiedVerdict
        );
        assert!(!persisted_review_report.fenced_from_promotion);
        assert_eq!(
            persisted_review_report.observed_model.as_deref(),
            Some("gemini-4-argon-high")
        );
        assert_eq!(
            persisted_review_report.observed_effort.as_deref(),
            Some("high")
        );

        let art_b = final_runtime
            .get_attempt_artifacts("task-root-b", 1)
            .expect("retained artifacts for task-root-b attempt 1");
        assert_eq!(
            art_b.observed_model.as_deref(),
            Some("gemini-4-argon-high"),
            "task-root-b must execute on the original frozen model pin"
        );
        assert_eq!(art_b.observed_effort.as_deref(), Some("high"));
        assert!(art_b.committed_head_commit.is_some());
        assert!(art_b.committed_tree_sha.is_some());
        assert!(
            art_b.review_subject.is_none(),
            "task-root-b must not enter task verification/review after configuration hold"
        );
        assert!(art_b.review_receipt.is_none());
        assert!(art_b.integration_receipt.is_none());

        // 3. Verify exact provider/check invocation counts and single native attention notice.
        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 2);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 4);

        assert_eq!(repeat_status["job_phase"], "held");
        assert_eq!(repeat_status["fatal_error"], Value::Null);
        assert_eq!(reopened_status.job_phase, CoworkerJobPhase::Held);
        assert!(reopened_status.fatal_error.is_none());

        assert!(notice_metadata.configured);
        assert_eq!(
            notice_metadata.attempt_count, 1,
            "configuration hold attention notice must be emitted exactly once across terminal receipt revisions"
        );
        let latest_notice = notice_metadata
            .latest
            .expect("configuration hold must record a latest notice report");
        assert_eq!(latest_notice.kind, NoticeKind::Attention);
        assert_eq!(latest_notice.status, NoticeDeliveryStatus::Submitted);
        assert_eq!(
            notice_effect_count.load(Ordering::SeqCst),
            1,
            "repeat status and reopened stores must not duplicate the configuration hold notice"
        );
    })
    .await;
}

#[tokio::test]
async fn unrelated_equivalent_registry_edits_preserve_admission_without_configuration_hold() {
    let fixture = setup_fixture();
    let (job, _approval_id, runtime_before) =
        create_two_task_frozen_job(&fixture, "brf_unrel_equiv_adm", "plan-unrel-equiv-adm").await;
    let frozen_revision = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .registry_revision;

    // 1. Add an unused profile, rename it, change the global default profile, and re-upsert the
    // selected profile and canonical tool definition with identical values.
    let unused_assignments = vec![
        RoleAssignment {
            role: WorkflowRole::Orchestrator,
            difficulty: Some(Difficulty::Hard),
            choice: ModelChoice {
                model_id: "model-argon".to_string(),
                effort: Some("high".to_string()),
            },
        },
        RoleAssignment {
            role: WorkflowRole::Coder,
            difficulty: Some(Difficulty::Hard),
            choice: ModelChoice {
                model_id: "model-argon".to_string(),
                effort: Some("high".to_string()),
            },
        },
    ];
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertProfile {
            profile: WorkflowProfile {
                id: "profile-unused".to_string(),
                name: "Unused Profile".to_string(),
                decision_advisor: None,
                assignments: unused_assignments.clone(),
            },
        },
    );
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertProfile {
            profile: WorkflowProfile {
                id: "profile-unused".to_string(),
                name: "Unused Profile Renamed".to_string(),
                decision_advisor: None,
                assignments: unused_assignments,
            },
        },
    );
    apply_registry_change(
        &fixture,
        RegistryChange::SetDefaultProfile {
            profile_id: Some("profile-unused".to_string()),
        },
    );

    let snap_mid = fixture.registry.snapshot().expect("registry snapshot");
    let existing_selected_profile = snap_mid
        .profiles
        .iter()
        .find(|p| p.id == "profile-coworker")
        .expect("profile-coworker")
        .clone();
    let existing_selected_tool = snap_mid
        .tools
        .iter()
        .find(|t| t.id == "tool-agy")
        .expect("tool-agy")
        .clone();
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertProfile {
            profile: existing_selected_profile,
        },
    );
    let live_snapshot = apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: existing_selected_tool,
        },
    );
    assert!(
        live_snapshot.revision > frozen_revision,
        "unrelated registry edits must advance global RegistryStore revision"
    );

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("unrelated equivalent registry edit must remain admissible");
    let adm = session
        .verify_admission()
        .expect("verify_admission must succeed without configuration hold");
    assert_eq!(adm.job.phase, CoworkerJobPhase::Executing);
    assert!(
        adm.job.holds.is_empty(),
        "unrelated registry edits must not mint a configuration hold: {:?}",
        adm.job.holds
    );
    assert!(adm
        .job
        .tasks
        .iter()
        .all(|t| t.stage != CoworkerTaskStage::Held && t.active_hold_ids.is_empty()));
    assert!(!adm.runtime_state.stop_requested);
    assert_eq!(adm.runtime_state.envelope, runtime_before.envelope);
    assert_eq!(
        adm.runtime_state.envelope.task_coder_choices,
        runtime_before.envelope.task_coder_choices
    );
    session.release().expect("release owned session");

    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
}

#[tokio::test]
async fn verified_closure_tool_executable_guard_accepts_equivalent_revision_and_rejects_tool_drift()
{
    let fixture = setup_fixture();
    let (job, _approval_id, runtime) = create_two_task_frozen_job(
        &fixture,
        "brf_closure_guard_drift",
        "plan-closure-guard-drift",
    )
    .await;
    let frozen_revision = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .registry_revision;
    let closure_choice = &runtime.envelope.closure_choice;

    let snap_initial = fixture.registry.snapshot().expect("registry snapshot");
    let mut relabeled_profile = snap_initial
        .profiles
        .iter()
        .find(|p| p.id == "profile-coworker")
        .expect("profile-coworker")
        .clone();
    relabeled_profile.name = "Coworker Headless Profile Relabeled".to_string();
    let snap_equiv = apply_registry_change(
        &fixture,
        RegistryChange::UpsertProfile {
            profile: relabeled_profile,
        },
    );
    assert!(snap_equiv.revision > frozen_revision);

    let verified_exe = resolve_verified_closure_tool_executable(
        &fixture.authority,
        &job,
        &runtime.envelope,
        closure_choice,
    )
    .expect("equivalent registry snapshot must accept frozen closure executable");
    assert_eq!(
        verified_exe,
        fs::canonicalize(&fixture.fake_agy_exe).expect("canonical fake_agy_exe")
    );

    let alt_exe = fixture.root.join("fake-agy-closure-alt.sh");
    write_executable_script(&alt_exe, "#!/usr/bin/env bash\nexit 0\n");
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(alt_exe.clone()),
                enabled: true,
            },
        },
    );

    let unguarded_exe = resolve_tool_executable(&fixture.authority, &closure_choice.tool_id)
        .expect("unguarded resolve_tool_executable resolves unproved alternate path");
    assert_eq!(
        unguarded_exe,
        fs::canonicalize(&alt_exe).expect("canonical alt_exe")
    );
    assert_ne!(unguarded_exe, verified_exe);

    let path_drift_err = resolve_verified_closure_tool_executable(
        &fixture.authority,
        &job,
        &runtime.envelope,
        closure_choice,
    )
    .expect_err("closure guard must reject unproved executable path change");
    assert_eq!(path_drift_err.code, NativeErrorCode::ValidationFailed);

    apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(fixture.fake_agy_exe.clone()),
                enabled: false,
            },
        },
    );
    let disabled_err = resolve_verified_closure_tool_executable(
        &fixture.authority,
        &job,
        &runtime.envelope,
        closure_choice,
    )
    .expect_err("closure guard must reject disabled tool");
    assert_eq!(disabled_err.code, NativeErrorCode::ValidationFailed);

    apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(fixture.fake_agy_exe.clone()),
                enabled: true,
            },
        },
    );
    let mut mismatched_adapter_choice = closure_choice.clone();
    mismatched_adapter_choice.adapter = ToolAdapter::OpencodeV2;
    let adapter_err = resolve_verified_closure_tool_executable(
        &fixture.authority,
        &job,
        &runtime.envelope,
        &mismatched_adapter_choice,
    )
    .expect_err("closure guard must reject adapter drift");
    assert_eq!(adapter_err.code, NativeErrorCode::ValidationFailed);

    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
}

#[tokio::test]
async fn stale_job_revision_cas_reloads_and_establishes_idempotent_registry_config_hold() {
    let fixture = setup_fixture();
    let (mut stale_job, approval_id, mut stale_runtime) =
        create_two_task_frozen_job(&fixture, "brf_stale_cas_hold", "plan-stale-cas-hold").await;
    let initial_job_revision = stale_job.job_revision;
    let frozen_envelope = stale_runtime.envelope.clone();

    let (job_with_scope_hold, scope_hold) = fixture
        .workflow_store
        .apply_workflow_hold(&ApplyWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: stale_job.job_id.clone(),
            expected_job_revision: initial_job_revision,
            expected_ledger_revision: None,
            request_key: "req_transient_scope_hold".to_string(),
            reason: WorkflowHoldReason::ReviewScopeConflict,
            detail: "transient scope hold to advance durable job_revision".to_string(),
            directly_affected_task_ids: vec!["task-root-a".to_string()],
            affected_write_paths: vec!["src/a.rs".to_string()],
            affected_interfaces: Vec::new(),
            holds_entire_job: false,
        })
        .expect("apply transient ReviewScopeConflict hold");
    assert!(scope_hold.active);
    assert!(job_with_scope_hold.job_revision > initial_job_revision);

    let (job_after_scope_resolve, resolved_scope_hold) = fixture
        .workflow_store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: stale_job.job_id.clone(),
            expected_job_revision: job_with_scope_hold.job_revision,
            expected_ledger_revision: None,
            hold_id: scope_hold.hold_id.clone(),
            resolution_note: "resolved transient scope hold before config hold check".to_string(),
        })
        .expect("resolve transient ReviewScopeConflict hold");
    assert!(!resolved_scope_hold.active);
    assert_eq!(job_after_scope_resolve.phase, CoworkerJobPhase::Executing);
    assert!(stale_job.job_revision < job_after_scope_resolve.job_revision);

    let alt_exe = fixture.root.join("fake-agy-stale-cas-alt.sh");
    write_executable_script(&alt_exe, "#!/usr/bin/env bash\nexit 0\n");
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(alt_exe),
                enabled: true,
            },
        },
    );

    let approval = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load approval for operator");
    verify_frozen_runtime_registry_or_apply_config_hold(
        &approval,
        &mut stale_job,
        &mut stale_runtime,
        &fixture.authority,
        &fixture.workflow_store,
        &fixture.policy_store,
    )
    .expect("stale job_revision CAS must reload ledger/runtime and apply config hold");

    assert_eq!(stale_job.phase, CoworkerJobPhase::Held);
    assert!(stale_job.job_revision > job_after_scope_resolve.job_revision);
    assert!(!stale_runtime.stop_requested);
    assert!(stale_runtime.stop_or_block_reason.is_some());
    assert_eq!(stale_runtime.envelope, frozen_envelope);

    let active_config_holds: Vec<&WorkflowHoldRecord> = stale_job
        .holds
        .iter()
        .filter(|h| h.active && is_registry_configuration_changed_hold(h))
        .collect();
    assert_eq!(active_config_holds.len(), 1);
    assert!(active_config_holds[0].holds_entire_job);
    assert!(stale_job.holds.iter().any(|h| {
        h.hold_id == scope_hold.hold_id
            && !h.active
            && h.reason == WorkflowHoldReason::ReviewScopeConflict
    }));
    assert_eq!(stale_job.holds.len(), 2);

    let ledger_after_first = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after first config hold call");
    let persisted_job_after_first = ledger_after_first
        .jobs
        .iter()
        .find(|j| j.job_id == stale_job.job_id)
        .expect("find persisted job");
    assert_eq!(persisted_job_after_first, &stale_job);

    let job_rev_after_first = stale_job.job_revision;
    let runtime_rev_after_first = stale_runtime.runtime_revision;
    let ledger_json_after_first =
        serde_json::to_value(&ledger_after_first).expect("serialize ledger_after_first");

    verify_frozen_runtime_registry_or_apply_config_hold(
        &approval,
        &mut stale_job,
        &mut stale_runtime,
        &fixture.authority,
        &fixture.workflow_store,
        &fixture.policy_store,
    )
    .expect("repeat call with active config hold must be idempotent");

    let ledger_after_second = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after repeat config hold call");
    let ledger_json_after_second =
        serde_json::to_value(&ledger_after_second).expect("serialize ledger_after_second");
    assert_eq!(stale_job.job_revision, job_rev_after_first);
    assert_eq!(stale_runtime.runtime_revision, runtime_rev_after_first);
    assert_eq!(ledger_json_after_second, ledger_json_after_first);
    assert_eq!(stale_job.holds.len(), 2);
    assert!(!stale_runtime.stop_requested);
    assert_eq!(stale_runtime.envelope, frozen_envelope);

    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.check_count_file), 0);
}

#[tokio::test]
async fn relevant_tool_changes_hold_with_stable_revisions_and_manual_resolve_immediately_refences()
{
    // Part A: Canonical executable path change on the selected tool holds even when model choices
    // resolve identically; repeated admission reuses the active hold with stable revisions; manual
    // hold resolution alone immediately re-fences without reopening the resolved hold.
    let fixture = setup_fixture();
    let (job, approval_id, _runtime_before) = create_two_task_frozen_job(
        &fixture,
        "brf_rel_tool_exe_drift",
        "plan-rel-tool-exe-drift",
    )
    .await;

    let host_control = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    )))
    .await
    .expect("start approved host");

    let alt_agy_exe = fixture.root.join("fake-agy-alt");
    write_executable_script(&alt_agy_exe, "#!/bin/sh\nexit 0\n");

    let snap_after_exe_change = apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(alt_agy_exe),
                enabled: true,
            },
        },
    );

    // Resolved model choices remain equal, but canonical tool executable path changed.
    let report = threadlane_native::profile_flow::preflight(
        &snap_after_exe_change,
        &threadlane_native::profile_flow::PreflightRequest {
            profile_id: Some("profile-coworker".to_string()),
            roles: vec![threadlane_native::profile_flow::RolePreflightRequest {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
            }],
        },
        &execution_policy(),
    )
    .expect("preflight Coder choice after executable_path change");
    assert!(report.ready);
    let live_coder_choice = threadlane_native::profile_flow::normalize_coder_choice(
        report.roles[0]
            .selected
            .as_ref()
            .expect("selected Coder choice"),
    )
    .expect("normalize Coder choice");
    assert_eq!(live_coder_choice, headless_argon_choice(Difficulty::Hard));

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("open_owned_session must fence instead of failing on tool executable path drift");
    let adm1 = session
        .verify_admission()
        .expect("verify_admission after tool executable path drift");
    assert_eq!(adm1.job.phase, CoworkerJobPhase::Held);
    assert!(!adm1.runtime_state.stop_requested);
    assert_eq!(adm1.job.holds.len(), 1);
    let first_hold = adm1.job.holds[0].clone();
    assert!(first_hold.active);
    assert!(first_hold.holds_entire_job);
    assert!(is_registry_configuration_changed_hold(&first_hold));

    let persisted_rt1 = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read persisted runtime state")
        .expect("persisted runtime state exists");
    assert_eq!(
        adm1.runtime_state.runtime_revision, persisted_rt1.runtime_revision,
        "returned runtime_revision must equal persisted state"
    );

    // Repeated admission reuses the active hold with stable job_revision and runtime_revision.
    let adm2 = session
        .verify_admission()
        .expect("repeated verify_admission while held");
    assert_eq!(adm2.job.job_revision, adm1.job.job_revision);
    assert_eq!(
        adm2.runtime_state.runtime_revision,
        adm1.runtime_state.runtime_revision
    );
    assert_eq!(adm2.job.holds, adm1.job.holds);
    assert!(!adm2.runtime_state.stop_requested);

    let step_while_held = session
        .step(50_000)
        .await
        .expect("controller step while held by registry configuration drift");
    assert_eq!(step_while_held.job_revision, adm1.job.job_revision);
    assert!(step_while_held.dispatched_task_ids.is_empty());
    let persisted_rt2 = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read persisted runtime state after step")
        .expect("persisted runtime state exists");
    assert!(!persisted_rt2.stop_requested);
    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);

    // Explicitly resolving the hold alone while settings still differ must immediately re-fence
    // with no dispatch; inactive idempotent hold reuse cannot reopen the resolved hold or leave the
    // job executing.
    let (job_after_resolve, resolved_hold) = fixture
        .workflow_store
        .resolve_workflow_hold(&ResolveWorkflowHoldRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            expected_job_revision: adm2.job.job_revision,
            expected_ledger_revision: None,
            hold_id: first_hold.hold_id.clone(),
            resolution_note: "manual resolve while tool executable path still differs".to_string(),
        })
        .expect("resolve configuration hold");
    assert!(!resolved_hold.active);
    assert!(resolved_hold.resolved_at_job_revision.is_some());
    assert_eq!(job_after_resolve.phase, CoworkerJobPhase::Executing);

    let step_after_resolve = session
        .step(51_000)
        .await
        .expect("step after manual hold resolution while registry still differs");
    assert!(
        step_after_resolve.dispatched_task_ids.is_empty(),
        "re-fencing after manual hold resolution must not dispatch any tasks"
    );
    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);

    let refenced_adm = session
        .verify_admission()
        .expect("verify_admission after re-fencing");
    assert_eq!(refenced_adm.job.phase, CoworkerJobPhase::Held);
    assert!(!refenced_adm.runtime_state.stop_requested);
    assert!(refenced_adm
        .job
        .tasks
        .iter()
        .all(|t| t.stage == CoworkerTaskStage::Held && !t.active_hold_ids.is_empty()));
    let historical_hold = refenced_adm
        .job
        .holds
        .iter()
        .find(|h| h.hold_id == first_hold.hold_id)
        .expect("resolved hold record must remain in job.holds");
    assert!(
        !historical_hold.active && historical_hold.resolved_at_job_revision.is_some(),
        "inactive idempotent hold reuse must not reopen a resolved hold"
    );
    let new_active_holds: Vec<_> = refenced_adm.job.holds.iter().filter(|h| h.active).collect();
    assert_eq!(new_active_holds.len(), 1);
    assert_ne!(new_active_holds[0].hold_id, first_hold.hold_id);
    assert!(new_active_holds[0].holds_entire_job);
    assert!(is_registry_configuration_changed_hold(new_active_holds[0]));
    session.release().expect("release session");
    host_control.shutdown().await.expect("shutdown host");

    // Part B: Selected tool disable also holds cleanly with stop_requested=false and stable state.
    let fixture_disabled = setup_fixture();
    let (job_b, _approval_b, _rt_b) = create_two_task_frozen_job(
        &fixture_disabled,
        "brf_rel_tool_disable",
        "plan-rel-tool-disable",
    )
    .await;
    apply_registry_change(
        &fixture_disabled,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(fixture_disabled.fake_agy_exe.clone()),
                enabled: false,
            },
        },
    );

    let session_b = fixture_disabled
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job_b.job_id, 20_000)
        .expect("open_owned_session when selected tool is disabled");
    let adm_b1 = session_b.verify_admission().expect("verify_admission B1");
    let adm_b2 = session_b.verify_admission().expect("verify_admission B2");
    assert_eq!(adm_b1.job.phase, CoworkerJobPhase::Held);
    assert_eq!(adm_b1.job.job_revision, adm_b2.job.job_revision);
    assert_eq!(
        adm_b1.runtime_state.runtime_revision,
        adm_b2.runtime_state.runtime_revision
    );
    assert!(!adm_b2.runtime_state.stop_requested);
    assert_eq!(adm_b2.job.holds.len(), 1);
    assert!(adm_b2.job.holds[0].active);
    assert!(is_registry_configuration_changed_hold(&adm_b2.job.holds[0]));
    session_b.release().expect("release session B");
}

async fn assert_advisory_role_only_profile_change_holds(
    target_role: WorkflowRole,
    preauthorize_alt_model_in_policy: bool,
) {
    let fixture = setup_fixture();
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertModels {
            models: vec![ModelDefinition {
                id: "model-argon-alt".to_string(),
                tool_id: "tool-agy".to_string(),
                label: "Gemini 4 Argon High Alt".to_string(),
                model_arg: "gemini-4-argon-high".to_string(),
                efforts: vec!["medium".to_string(), "high".to_string()],
                allow_no_effort: false,
            }],
        },
    );

    let mut policy = execution_policy();
    if preauthorize_alt_model_in_policy {
        policy.allow_rules.push(PolicyAllowRule {
            tool_id: "tool-agy".to_string(),
            model_id: "model-argon-alt".to_string(),
            efforts: vec!["high".to_string()],
            allow_no_effort: false,
            roles: vec![target_role],
        });
    }

    let suffix = format!(
        "{}_{}",
        target_role.as_str(),
        if preauthorize_alt_model_in_policy {
            "preauth"
        } else {
            "strict"
        }
    );
    let (job, _approval_id, _runtime_before) = create_two_task_frozen_job_with_policy(
        &fixture,
        &format!("brf_adv_{suffix}"),
        &format!("plan-adv-{suffix}"),
        &policy,
    )
    .await;

    let role_model = |role: WorkflowRole| {
        if role == target_role {
            "model-argon-alt".to_string()
        } else {
            "model-argon".to_string()
        }
    };
    apply_registry_change(
        &fixture,
        RegistryChange::UpsertProfile {
            profile: WorkflowProfile {
                id: "profile-coworker".to_string(),
                name: "Coworker Headless Profile".to_string(),
                decision_advisor: None,
                assignments: vec![
                    RoleAssignment {
                        role: WorkflowRole::Orchestrator,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: role_model(WorkflowRole::Orchestrator),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Architect,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: role_model(WorkflowRole::Architect),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: role_model(WorkflowRole::Coder),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Reviewer,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: role_model(WorkflowRole::Reviewer),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Closure,
                        difficulty: Some(Difficulty::Extreme),
                        choice: ModelChoice {
                            model_id: role_model(WorkflowRole::Closure),
                            effort: Some("high".to_string()),
                        },
                    },
                    RoleAssignment {
                        role: WorkflowRole::Follower,
                        difficulty: None,
                        choice: ModelChoice {
                            model_id: "model-codex-native".to_string(),
                            effort: Some("high".to_string()),
                        },
                    },
                ],
            },
        },
    );

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect("open_owned_session after advisory-role-only profile assignment change");
    let adm = session
        .verify_admission()
        .expect("verify_admission after advisory-role-only profile assignment change");
    assert_eq!(
        adm.job.phase,
        CoworkerJobPhase::Held,
        "changing only {target_role:?} profile assignment (preauthorize={preauthorize_alt_model_in_policy}) must fence the job"
    );
    assert!(!adm.runtime_state.stop_requested);
    assert_eq!(adm.job.holds.len(), 1);
    assert!(adm.job.holds[0].active);
    assert!(adm.job.holds[0].holds_entire_job);
    assert!(is_registry_configuration_changed_hold(&adm.job.holds[0]));
    assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 0);
    session.release().expect("release session");
}

#[tokio::test]
async fn reviewer_or_closure_only_profile_assignment_change_fences_job() {
    for target_role in [WorkflowRole::Reviewer, WorkflowRole::Closure] {
        for preauthorize in [true, false] {
            assert_advisory_role_only_profile_change_holds(target_role, preauthorize).await;
        }
    }
}

#[tokio::test]
async fn corrupt_frozen_task_choice_evidence_errors_before_minting_configuration_hold() {
    // Case 1: Mutate frozen task_coder_choices in ControllerJobRuntimeState (recomputing
    // envelope_digest) via mutate_job_runtime_for_operator, together with a registry change.
    let fixture = setup_fixture();
    let (job, _approval_id, _runtime_state) = create_two_task_frozen_job(
        &fixture,
        "brf_corrupt_env_choice",
        "plan-corrupt-env-choice",
    )
    .await;

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            let choice = state
                .envelope
                .task_coder_choices
                .get_mut("task-root-a")
                .expect("task-root-a choice in envelope");
            choice.effort = Some("medium".to_string());
            state.envelope.envelope_digest = state.envelope.compute_digest()?;
            Ok(())
        })
        .expect("mutate frozen task_coder_choices with recomputed envelope_digest");

    apply_registry_change(
        &fixture,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(fixture.fake_agy_exe.clone()),
                enabled: false,
            },
        },
    );

    let err1 = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
        .expect_err("corrupt frozen task_coder_choices must fail admission before holding");
    assert_eq!(err1.code, NativeErrorCode::ValidationFailed);
    let ledger1 = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after Case 1 failure");
    let job1 = ledger1
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("find job1");
    assert_eq!(job1.phase, CoworkerJobPhase::Executing);
    assert!(
        job1.holds.is_empty(),
        "no configuration hold may be minted when frozen task-choice validation fails"
    );

    // Case 2: Both job.tasks[0].selected_choice and runtime_state.envelope.task_coder_choices carry
    // a structurally valid choice (with recomputed envelope_digest) that passes envelope-vs-job
    // validation but mismatches the frozen ApprovalRecord task pin, alongside a registry change.
    // Complete frozen evidence validation against ApprovalRecord must still ERR before minting any
    // configuration hold.
    let fixture2 = setup_fixture();
    let (job2, _approval_id2, _runtime_state2) = create_two_task_frozen_job(
        &fixture2,
        "brf_corrupt_appr_choice",
        "plan-corrupt-appr-choice",
    )
    .await;

    fixture2
        .workflow_store
        .transact_binding_ledger(BINDING_A, None, |ledger| {
            let stored_job = ledger
                .jobs
                .iter_mut()
                .find(|j| j.job_id == job2.job_id)
                .expect("find stored job2");
            stored_job.tasks[0].selected_choice.effort = Some("medium".to_string());
            Ok(CoworkerLedgerMutationOutcome::Mutated(()))
        })
        .expect("mutate stored job task selected_choice");
    fixture2
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job2.job_id, |state| {
            let choice = state
                .envelope
                .task_coder_choices
                .get_mut("task-root-a")
                .expect("task-root-a choice in envelope");
            choice.effort = Some("medium".to_string());
            state.envelope.envelope_digest = state.envelope.compute_digest()?;
            Ok(())
        })
        .expect("mutate runtime envelope task_coder_choices to match mutated job task");

    apply_registry_change(
        &fixture2,
        RegistryChange::UpsertTool {
            tool: ToolDefinition {
                id: "tool-agy".to_string(),
                label: "Antigravity CLI".to_string(),
                adapter: ToolAdapter::Antigravity,
                executable_path: Some(fixture2.fake_agy_exe.clone()),
                enabled: false,
            },
        },
    );

    let err2 = fixture2
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job2.job_id, 20_000)
        .expect_err("task choice mismatch against ApprovalRecord must fail before minting a hold");
    assert_eq!(err2.code, NativeErrorCode::ValidationFailed);
    assert!(
        !err2.message.contains("RegistryStore revision drifted"),
        "complete frozen task-choice validation against ApprovalRecord must fail before registry drift evaluation, got: {}",
        err2.message
    );
    let ledger2 = fixture2
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after Case 2 failure");
    let stored_job2 = ledger2
        .jobs
        .iter()
        .find(|j| j.job_id == job2.job_id)
        .expect("find stored_job2");
    assert_eq!(stored_job2.phase, CoworkerJobPhase::Executing);
    assert!(
        stored_job2.holds.is_empty(),
        "no configuration hold may be minted before complete frozen task-choice validation succeeds"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_pipeline_completes_through_aggregate_and_closure_after_unrelated_registry_revision_change(
) {
    Box::pin(async {
        let fixture = setup_fixture();

        let tasks = vec![
            PlanTask {
                id: "task-root-a".to_string(),
                title: "Root task A".to_string(),
                instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/a.rs".to_string()],
            },
            PlanTask {
                id: "task-root-b".to_string(),
                title: "Root task B".to_string(),
                instruction: "CTRL_TASK_B implement src/b.rs".to_string(),
                role: WorkflowRole::Coder,
                difficulty: Difficulty::Hard,
                depends_on: Vec::new(),
                allowed_write_paths: vec!["src/b.rs".to_string()],
            },
            PlanTask {
                id: "task-dep-c".to_string(),
                title: "Dependent task C".to_string(),
                instruction: "CTRL_TASK_C implement src/c.rs".to_string(),
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
            BINDING_A,
            THREAD_UUID_A,
            "brf_unrel_drift_full_pipe",
            "plan-unrel-drift-full-pipe",
            tasks,
            ifaces,
            vec![
                "src/a.rs".to_string(),
                "src/b.rs".to_string(),
                "src/c.rs".to_string(),
            ],
            2,
            ExecutionBudgets::default(),
            ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
            1,
        )
        .await;

        let frozen_revision = job
            .approval_linkage
            .as_ref()
            .expect("approval linkage")
            .registry_revision;

        // Mutate unrelated registry entries after freezing authority and the controller envelope.
        // Because all Worker tasks, Reviewer, and Closure share the same frozen `tool-agy` fake
        // executable, the conservative equivalence check can prove every active dependency
        // unchanged across Worker -> Reviewer -> integration -> aggregate -> Closure.
        let unused_assignments = vec![
            RoleAssignment {
                role: WorkflowRole::Orchestrator,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "model-argon".to_string(),
                    effort: Some("high".to_string()),
                },
            },
            RoleAssignment {
                role: WorkflowRole::Coder,
                difficulty: Some(Difficulty::Hard),
                choice: ModelChoice {
                    model_id: "model-argon".to_string(),
                    effort: Some("high".to_string()),
                },
            },
        ];
        apply_registry_change(
            &fixture,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-unused".to_string(),
                    name: "Unused Profile".to_string(),
                    decision_advisor: None,
                    assignments: unused_assignments.clone(),
                },
            },
        );
        apply_registry_change(
            &fixture,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-unused".to_string(),
                    name: "Unused Profile Renamed".to_string(),
                    decision_advisor: None,
                    assignments: unused_assignments,
                },
            },
        );
        let live_snapshot = apply_registry_change(
            &fixture,
            RegistryChange::SetDefaultProfile {
                profile_id: Some("profile-unused".to_string()),
            },
        );
        assert!(
            live_snapshot.revision > frozen_revision,
            "unrelated registry edits must advance global RegistryStore revision"
        );

        let host_control = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )))
        .await
        .expect("start approved host");

        let code_driver = CoworkerCodeDriver::open_from_dirs(
            fixture.root.join("runtime-unrel-full"),
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
        )
        .expect("open CoworkerCodeDriver");
        let pipeline = CoworkerPipelineService::new(code_driver.controller().clone())
            .expect("create CoworkerPipelineService");

        let session = pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
            .expect("open owned pipeline session after unrelated registry drift");
        let drive_timeout_res = {
            let mut drive_fut = Box::pin(session.drive_until_settled(21_000));
            let mut progress_interval = tokio::time::interval(Duration::from_secs(15));
            progress_interval.tick().await;
            let overall_sleep = tokio::time::sleep(Duration::from_secs(300));
            tokio::pin!(overall_sleep);

            loop {
                tokio::select! {
                    res = &mut drive_fut => break Ok(res),
                    () = &mut overall_sleep => break Err(()),
                    _ = progress_interval.tick() => {
                        if let Ok(progress_ledger) = fixture.workflow_store.read_binding_ledger(BINDING_A) {
                            if let Some(progress_job) = progress_ledger.jobs.iter().find(|j| j.job_id == job.job_id) {
                                let task_stages: Vec<_> = progress_job
                                    .tasks
                                    .iter()
                                    .take(8)
                                    .map(|t| (t.task_id.clone(), t.stage))
                                    .collect();
                                eprintln!(
                                    "full_pipeline_progress_diag: phase={:?}, tasks={:?}, counts=(coder={}, reviewer={}, closure={}, check={})",
                                    progress_job.phase,
                                    task_stages,
                                    read_invocation_count(&fixture.agy_coder_count_file),
                                    read_invocation_count(&fixture.agy_reviewer_count_file),
                                    read_invocation_count(&fixture.agy_closure_count_file),
                                    read_invocation_count(&fixture.check_count_file),
                                );
                            }
                        }
                    }
                }
            }
        };
        if drive_timeout_res.is_err() {
            let timeout_ledger = fixture
                .workflow_store
                .read_binding_ledger(BINDING_A)
                .expect("read binding ledger on timeout");
            let timeout_job = timeout_ledger
                .jobs
                .iter()
                .find(|j| j.job_id == job.job_id)
                .expect("find job on timeout");
            let timeout_rt = fixture
                .policy_store
                .get_job_runtime_readonly(BINDING_A, &job.job_id)
                .expect("read runtime on timeout")
                .expect("runtime exists on timeout");
            let task_summary: Vec<_> = timeout_job
                .tasks
                .iter()
                .take(8)
                .map(|t| (t.task_id.clone(), t.stage, t.current_attempt_no))
                .collect();
            let effect_summary: Vec<_> = timeout_rt
                .pending_effects
                .values()
                .take(12)
                .map(|e| (e.kind, e.status, e.task_id.clone()))
                .collect();
            let outbox_summary: Vec<_> = timeout_job
                .outbox
                .iter()
                .take(12)
                .map(|o| (o.kind, o.status, o.task_id.clone()))
                .collect();
            eprintln!(
                "full_pipeline_timeout_diag: phase={:?}, tasks={:?}, pending_effects={:?}, outbox={:?}, counts=(plan={}, coder={}, reviewer={}, closure={}, check={})",
                timeout_job.phase,
                task_summary,
                effect_summary,
                outbox_summary,
                read_invocation_count(&fixture.agy_planning_count_file),
                read_invocation_count(&fixture.agy_coder_count_file),
                read_invocation_count(&fixture.agy_reviewer_count_file),
                read_invocation_count(&fixture.agy_closure_count_file),
                read_invocation_count(&fixture.check_count_file),
            );
        }
        let _ = session.release();
        host_control.shutdown().await.expect("shutdown host");

        let final_report = drive_timeout_res
            .expect("drive_until_settled must complete within 300s timeout")
            .expect("full pipeline must complete through aggregate and Closure after unrelated registry drift");
        assert_eq!(final_report.status, CoworkerPipelineStatus::Completed);
        assert_eq!(final_report.job_phase, CoworkerJobPhase::Completed);
        let completion = final_report
            .completion_outcome
            .as_ref()
            .expect("completion_outcome must be present on Completed report");
        assert_eq!(completion.status, CoworkerJobCompletionStatus::Completed);
        let closure_verdict = completion
            .closure_verdict
            .as_ref()
            .expect("closure_verdict must be present");
        assert_eq!(closure_verdict.kind, ClosureVerdictKind::Accepted);

        let completed_ledger = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read completed binding ledger");
        let completed_job = completed_ledger
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("find completed job");
        let completed_runtime = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .expect("read completed runtime")
            .expect("completed runtime exists");

        assert_eq!(completed_job.phase, CoworkerJobPhase::Completed);
        assert!(
            completed_job.holds.is_empty(),
            "unrelated registry drift must not mint any configuration hold during full pipeline execution: {:?}",
            completed_job.holds
        );
        assert!(!completed_runtime.stop_requested);
        for task in &completed_job.tasks {
            assert_eq!(task.stage, CoworkerTaskStage::Integrated);
            assert!(task.accepted_candidate_id.is_some());
            assert!(task.active_hold_ids.is_empty());
        }

        assert_eq!(completed_job.combined_candidates.len(), 1);
        let combined = &completed_job.combined_candidates[0];
        assert_eq!(combined.status, CombinedCandidateStatus::ClosureAccepted);
        assert!(combined.closure_reservation_id.is_some());

        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 3);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 3);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 4);
    })
    .await;
}

fn populate_isolated_system_path_dir_for_drift(isolated_bin: &Path) {
    fs::create_dir_all(isolated_bin).expect("create isolated_bin");
    fs::set_permissions(isolated_bin, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 isolated_bin");
    let Some(parent_path) = std::env::var_os("PATH") else {
        return;
    };
    for util in [
        "git", "node", "sh", "mkdir", "cat", "rmdir", "sleep", "rm", "cp", "mv", "chmod", "printf",
    ] {
        for dir in std::env::split_paths(&parent_path) {
            if !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join(util);
            if let Ok(meta) = fs::metadata(&candidate) {
                if meta.is_file() && (meta.permissions().mode() & 0o111 != 0) {
                    let link_path = isolated_bin.join(util);
                    if !link_path.exists() {
                        std::os::unix::fs::symlink(&candidate, &link_path)
                            .expect("symlink basic utility into isolated_bin");
                    }
                    break;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn system_mode_agy_worker_registry_drift_child_probe() {
    let Some(isolated_bin_os) = std::env::var_os("THREADLANE_DRIFT_SYSTEM_MODE_AGY_BIN") else {
        return;
    };
    let isolated_bin = PathBuf::from(isolated_bin_os);
    Box::pin(async move {
        use threadlane_native::coworker_admission::{
            AdmissionConfirmation, AdmissionRequest, AdmissionStage, CoworkerAdmissionService,
        };
        use threadlane_native::CoworkerCodeDriver;

        let fixture = setup_fixture();
        let role_script_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake-coworker-roles.mjs");
        let fake_codex_mjs = fixture.root.join("bin/fake-codex-advisory.mjs");
        fs::write(
            &fake_codex_mjs,
            r#"import { execFileSync } from "node:child_process";
const roleScript = process.argv[2];
const stateDir = process.argv[3];
const args = process.argv.slice(4);
if (args.includes("--ignore-rules")) {
  execFileSync(
    process.execPath,
    [roleScript, stateDir, ...args],
    { stdio: ["ignore", "inherit", "inherit"] },
  );
  process.exit(0);
}
let model = "gpt-6-astra";
for (let i = 0; i < args.length; i += 1) {
  if (args[i] === "--model" && i + 1 < args.length) {
    model = args[i + 1];
    i += 1;
  }
}
const prompt = args[args.length - 1] || "";
const out = execFileSync(
  process.execPath,
  [roleScript, stateDir, "--model", model, "--effort", "high", "--mode", "plan", "--print", prompt],
  { encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] },
);
const lines = out.trim().split("\n");
const last = JSON.parse(lines[lines.length - 1]);
const responseText = last.result.response;
process.stdout.write(`${JSON.stringify({ type: "thread.started", thread_id: "th-codex-advisory" })}\n`);
process.stdout.write(`${JSON.stringify({ type: "turn.started" })}\n`);
process.stdout.write(`${JSON.stringify({ type: "item.completed", item: { id: "msg-1", type: "agent_message", text: responseText } })}\n`);
process.stdout.write(`${JSON.stringify({ type: "turn.completed", usage: { input_tokens: 10, output_tokens: 5 } })}\n`);
"#,
        )
        .expect("write fake-codex-advisory.mjs");
        let fake_codex_exe = fixture.root.join("bin/fake-codex");
        write_executable_script(
            &fake_codex_exe,
            &format!(
                "#!/bin/sh\nset -eu\nexec node \"{}\" \"{}\" \"{}\" \"$@\"\n",
                fake_codex_mjs.display(),
                role_script_path.display(),
                fixture.root.display(),
            ),
        );
        let fake_agy_drifted_exe = fixture.root.join("bin/fake-agy-drifted");
        write_executable_script(
            &fake_agy_drifted_exe,
            &format!(
                "#!/bin/sh\nset -eu\nexec node \"{}\" \"{}\" \"$@\"\n",
                role_script_path.display(),
                fixture.root.display(),
            ),
        );
        let canonical_agy_v1 = fs::canonicalize(&fixture.fake_agy_exe).unwrap();
        let canonical_agy_v2 = fs::canonicalize(&fake_agy_drifted_exe).unwrap();
        let canonical_codex_v1 = fs::canonicalize(&fake_codex_exe).unwrap();

        let agy_link = isolated_bin.join("agy");
        let codex_link = isolated_bin.join("codex");
        let _ = fs::remove_file(&agy_link);
        let _ = fs::remove_file(&codex_link);
        std::os::unix::fs::symlink(&canonical_agy_v1, &agy_link).unwrap();
        std::os::unix::fs::symlink(&canonical_codex_v1, &codex_link).unwrap();

        // Configure tool-agy with executable_path: None (System mode) for Hard Coder Worker, and
        // tool-codex (System mode) for Medium Coder Worker plus Orchestrator, Architect, Reviewer, Closure.
        apply_registry_change(
            &fixture,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity CLI (System Mode)".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: None,
                    enabled: true,
                },
            },
        );
        apply_registry_change(
            &fixture,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-coworker".to_string(),
                    name: "Coworker System AGY Worker + Codex Worker/Advisory".to_string(),
                    assignments: vec![
                        RoleAssignment {
                            role: WorkflowRole::Orchestrator,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Architect,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-argon".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Medium),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Reviewer,
                            difficulty: Some(Difficulty::Hard),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                        RoleAssignment {
                            role: WorkflowRole::Closure,
                            difficulty: Some(Difficulty::Extreme),
                            choice: ModelChoice {
                                model_id: "model-codex-native".to_string(),
                                effort: Some("high".to_string()),
                            },
                        },
                    ],
                },
            },
        );

        let codex_choice = |difficulty: Difficulty| SelectedRoleChoice {
            tool_id: "tool-codex".to_string(),
            adapter: ToolAdapter::CodexCli,
            route: RoleExecutionRoute::HeadlessWorker,
            model_id: "model-codex-native".to_string(),
            model_arg: "gpt-6-astra".to_string(),
            effort: Some("high".to_string()),
            matched_difficulty: Some(difficulty),
        };
        let sys_planning_policy = ExecutionPolicy {
            schema_version: 1,
            policy_id: "policy-sys-agy-plan".to_string(),
            allow_rules: vec![PolicyAllowRule {
                tool_id: "tool-codex".to_string(),
                model_id: "model-codex-native".to_string(),
                efforts: vec!["high".to_string()],
                allow_no_effort: false,
                roles: vec![WorkflowRole::Orchestrator, WorkflowRole::Architect],
            }],
        };
        let sys_exec_policy = ExecutionPolicy {
            schema_version: 1,
            policy_id: "policy-sys-agy-exec".to_string(),
            allow_rules: vec![
                PolicyAllowRule {
                    tool_id: "tool-agy".to_string(),
                    model_id: "model-argon".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![WorkflowRole::Coder],
                },
                PolicyAllowRule {
                    tool_id: "tool-codex".to_string(),
                    model_id: "model-codex-native".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                    roles: vec![
                        WorkflowRole::Orchestrator,
                        WorkflowRole::Architect,
                        WorkflowRole::Coder,
                        WorkflowRole::Reviewer,
                        WorkflowRole::Closure,
                    ],
                },
            ],
        };

        let (mut tasks, task_interfaces, approved_write_paths) = default_two_root_tasks();
        tasks[0].difficulty = Difficulty::Hard;
        tasks[1].difficulty = Difficulty::Medium;
        let pb = project_base(&fixture);
        let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
        let brief = sample_brief(&fixture, "brf_sys_agy_unrel");
        let job = fixture
            .workflow_store
            .create_or_amend_job(&CreateCoworkerJobRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req_job_brf_sys_agy_unrel".to_string(),
                expected_ledger_revision: None,
                work_brief: brief,
                amended_target_task_ids: Vec::new(),
                amended_write_paths: Vec::new(),
                amended_interfaces: Vec::new(),
            })
            .expect("create job");

        let driver_planning_artifacts_dir =
            threadlane_native::planning_driver::planning_artifacts_dir(&fixture.plans_dir);
        let driver_planning_service = CoworkerPlanningService::new(
            fixture.workflow_store.clone(),
            &fixture.registry_dir,
            &driver_planning_artifacts_dir,
        )
        .expect("create driver_planning_service");

        let grant = fixture
            .workflow_store
            .create_planning_grant(&CreatePlanningGrantRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                request_key: "req_grant_brf_sys_agy_unrel".to_string(),
                expected_ledger_revision: None,
                project_base: pb,
                profile_id: "profile-coworker".to_string(),
                registry_revision: reg_snap.revision,
                policy: sys_planning_policy,
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
            summary: "System-mode AGY Worker + Codex Worker/Advisory two-task plan".to_string(),
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
        fs::write(
            &fixture.agy_plan_response_file,
            serde_json::to_string(&structured_plan).unwrap(),
        )
        .unwrap();

        let plan_outcome = Box::pin(driver_planning_service.orchestrate_brief_to_plan(
            &fixture.scoped_workflow,
            &OrchestrateWorkBriefPlanRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                grant_id: grant.grant_id.clone(),
                job_id: job.job_id.clone(),
                request_key: "req_orch_brf_sys_agy_unrel".to_string(),
                expected_ledger_revision: None,
                difficulty: Some(Difficulty::Hard),
                architect_difficulty: Some(Difficulty::Hard),
                plan_id: Some("plan-sys-agy-unrel".to_string()),
                expected_plan_revision: 0,
                architect_advice_ids: Vec::new(),
                max_architect_rounds: 0,
                current_time_ms: 2_000,
            },
        ))
        .await
        .expect("orchestrate_brief_to_plan via Codex advisory");
        let published_outcome = plan_outcome.require_published_plan().unwrap();

        let job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .unwrap()
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .unwrap();
        let approval_summary = fixture
            .authority
            .approve_at(
                OperatorApprovalRequest {
                    binding_id: BINDING_A.to_string(),
                    thread_uuid: THREAD_UUID_A.to_string(),
                    plan_id: published_outcome.planning_result.plan_id.clone(),
                    plan_revision: published_outcome.planning_result.plan_revision,
                    expected_registry_revision: reg_snap.revision,
                    expected_plan_digest: published_outcome.planning_result.plan_digest.clone(),
                    repository_path: fixture.repo_dir.clone(),
                    base_commit: fixture.base_commit.clone(),
                    approved_task_ids: tasks.iter().map(|t| t.id.clone()).collect(),
                    approved_write_paths,
                    max_parallel: 2,
                    budgets: ExecutionBudgets::default(),
                },
                1_700_000_001_000,
            )
            .expect("approve System-mode AGY + Codex Worker plan");
        let approval_record = fixture
            .authority
            .get_for_operator_readonly(&approval_summary.approval_id)
            .unwrap();
        assert_eq!(approval_record.tasks.len(), 2);
        let task_a_approved = approval_record
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-a")
            .unwrap();
        assert_eq!(task_a_approved.tool_id, "tool-agy");
        assert_eq!(
            task_a_approved.canonical_executable.as_ref(),
            Some(&canonical_agy_v1)
        );
        let task_b_approved = approval_record
            .tasks
            .iter()
            .find(|t| t.task_id == "task-root-b")
            .unwrap();
        assert_eq!(task_b_approved.tool_id, "tool-codex");
        assert_eq!(
            task_b_approved.canonical_executable.as_ref(),
            Some(&canonical_codex_v1)
        );

        let mut task_entries = BTreeMap::new();
        for approved_task in &approval_record.tasks {
            let iface = task_interfaces
                .get(&approved_task.task_id)
                .cloned()
                .unwrap_or_default();
            task_entries.insert(
                approved_task.task_id.clone(),
                TaskIndependenceCompletenessEntry {
                    task_id: approved_task.task_id.clone(),
                    task_digest: approved_task.task_digest.clone(),
                    owned_interfaces: iface.owned_interfaces,
                    consumed_interfaces: iface.consumed_interfaces,
                    explicitly_no_interfaces: false,
                    semantic_independence_attested: true,
                },
            );
        }
        let mut completeness = DeclaredIndependenceCompletenessReceipt {
            receipt_id: format!("icr_{}", job.job_id),
            job_id: job.job_id.clone(),
            approval_id: approval_summary.approval_id.clone(),
            plan_digest: approval_record.plan_digest.clone(),
            task_entries,
            hold_attestations: BTreeMap::new(),
            recorded_at_ms: 1_700_000_050_000,
            operator_attested: true,
            receipt_digest: String::new(),
        };
        completeness.receipt_digest = completeness.compute_digest().unwrap();

        let cap_env = FrozenControllerCapabilityEnvelope {
            allowed_reviewer_choices: vec![codex_choice(Difficulty::Hard)],
            allowed_closure_choices: vec![codex_choice(Difficulty::Extreme)],
            max_in_scope_correction_attempts: 1,
            allowed_check_ids: vec!["cargo_test".to_string()],
            local_git_integration_allowed: true,
            allow_check_failure_corrections: false,
        };
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
        let admission_req = AdmissionRequest {
            request_key: "admit_brf_sys_agy_unrel".to_string(),
            binding: BindJobApprovalRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                expected_ledger_revision: Some(
                    fixture
                        .workflow_store
                        .read_binding_ledger(BINDING_A)
                        .unwrap()
                        .ledger_revision,
                ),
                approval_id: approval_summary.approval_id.clone(),
                policy: sys_exec_policy.clone(),
                controller_envelope: Some(cap_env),
                task_interfaces,
            },
            planning_result: published_outcome.planning_result.clone(),
            work_brief_digest: job.work_brief_digest.clone(),
            execution: FreezeControllerExecutionEnvelopeRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                expected_job_revision: job.job_revision,
                policy: sys_exec_policy,
                reviewer_choice: codex_choice(Difficulty::Hard),
                closure_choice: codex_choice(Difficulty::Extreme),
                role_budgets,
                max_in_scope_corrections_per_task: 1,
                max_total_corrections_per_job: 2,
                max_total_task_attempts_per_job: 4,
                check_commands: default_check_commands(&fixture),
                allow_task_worktree_commits: true,
                allow_run_integration_execution: true,
                default_integration_strategy: IntegrationStrategy::MergeNoFf,
                independence_completeness_receipt: Some(completeness),
                shadow_jev_policy: None,
                created_at_ms: 1_700_000_100_000,
                operator_attested: false,
                confirm_approval_subject_digest: None,
                confirm_envelope_digest: None,
            },
            reviewer_difficulty: Some(Difficulty::Hard),
            closure_difficulty: Some(Difficulty::Extreme),
        };

        let admission_api = CoworkerAdmissionService::new(
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.registry_dir,
        )
        .unwrap();
        let count_before_preview = read_invocation_count(&fixture.agy_count_file);
        let preview = admission_api
            .preview(&admission_req)
            .expect("System-mode AGY Worker + Codex Worker/Advisory admission preview must succeed");
        assert_eq!(
            read_invocation_count(&fixture.agy_count_file),
            count_before_preview,
            "admission preview must never invoke or probe provider"
        );
        assert_eq!(
            preview.executable_pins.workers["task-root-a"].canonical_executable,
            canonical_agy_v1
        );
        assert_eq!(
            preview.executable_pins.workers["task-root-b"].canonical_executable,
            canonical_codex_v1
        );
        assert_eq!(
            preview.executable_pins.reviewer.canonical_executable,
            canonical_codex_v1
        );
        assert_eq!(
            preview.executable_pins.closure.canonical_executable,
            canonical_codex_v1
        );
        let receipt = admission_api
            .record(
                &admission_req,
                &AdmissionConfirmation {
                    trusted_local_attested: true,
                    preview_digest: preview.preview_digest.clone(),
                    approval_subject_digest: preview.approval_subject_digest.clone(),
                    envelope_digest: preview.envelope.envelope_digest.clone(),
                },
            )
            .expect("admission record must succeed");
        assert_eq!(receipt.stage, AdmissionStage::Complete);

        // Apply unrelated registry edits advancing revision beyond frozen authority.
        let frozen_revision = approval_record.registry_revision;
        let unused_assignments = fixture.registry.snapshot().unwrap().profiles[0]
            .assignments
            .clone();
        apply_registry_change(
            &fixture,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    id: "profile-unused".to_string(),
                    name: "Unused Profile".to_string(),
                    decision_advisor: None,
                    assignments: unused_assignments,
                },
            },
        );
        let live_snapshot = apply_registry_change(
            &fixture,
            RegistryChange::SetDefaultProfile {
                profile_id: Some("profile-unused".to_string()),
            },
        );
        assert!(live_snapshot.revision > frozen_revision);

        let workflow_ledger_path = fixture
            .workflow_store
            .ledger_path_for_binding(BINDING_A)
            .unwrap();
        let policy_ledger_path = fixture
            .policy_store
            .ledger_path_for_binding(BINDING_A)
            .unwrap();
        let saved_workflow_bytes = fs::read(&workflow_ledger_path).unwrap();
        let saved_policy_bytes = fs::read(&policy_ledger_path).unwrap();

        let host_control = Box::pin(ApprovedHost::start(ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_summary.approval_id,
        )))
        .await
        .expect("start approved host");

        let code_driver = CoworkerCodeDriver::open_from_dirs(
            fixture.root.join("runtime-sys-agy-unrel"),
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
        )
        .expect("open CoworkerCodeDriver");
        let pipeline = CoworkerPipelineService::new(code_driver.controller().clone())
            .expect("create CoworkerPipelineService");

        let session = pipeline
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 20_000)
            .expect("open owned pipeline session");
        let final_report = tokio::time::timeout(
            Duration::from_secs(180),
            session.drive_until_settled(21_000),
        )
        .await
        .expect("drive_until_settled must complete within timeout")
        .expect("pipeline with System-mode AGY Worker + Codex Worker/Advisory must complete across unrelated registry edit");
        let _ = session.release();

        assert_eq!(final_report.status, CoworkerPipelineStatus::Completed);
        assert_eq!(final_report.job_phase, CoworkerJobPhase::Completed);
        let completed_job = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .unwrap()
            .jobs
            .into_iter()
            .find(|j| j.job_id == job.job_id)
            .unwrap();
        let completed_runtime = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .unwrap()
            .unwrap();
        assert!(
            completed_job.holds.is_empty(),
            "unrelated registry edit must not mint a hold for System-mode AGY Worker + Codex Worker/Advisory: {:?}",
            completed_job.holds
        );
        assert!(!completed_runtime.stop_requested);
        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.agy_coder_count_file), 2);
        assert_eq!(read_invocation_count(&fixture.agy_reviewer_count_file), 2);
        assert_eq!(read_invocation_count(&fixture.agy_closure_count_file), 1);
        assert_eq!(read_invocation_count(&fixture.check_count_file), 3);

        // Now restore the post-admission Executing job state and verify that changing the discovered
        // PATH executable target to canonical_agy_v2 (or removing it) while registry revision >
        // frozen_revision fences the job with a persisted RegistryConfigurationChanged hold and
        // stop_requested == false.
        fs::write(&workflow_ledger_path, &saved_workflow_bytes).unwrap();
        fs::write(&policy_ledger_path, &saved_policy_bytes).unwrap();
        fs::remove_file(&agy_link).unwrap();
        std::os::unix::fs::symlink(&canonical_agy_v2, &agy_link).unwrap();
        let coder_before_hold = read_invocation_count(&fixture.agy_coder_count_file);
        let drift_session = code_driver
            .controller()
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 30_000)
            .expect("open_owned_session must fence instead of failing on Worker PATH target drift");
        let adm_drifted = drift_session
            .verify_admission()
            .expect("verify_admission after Worker PATH target drift");
        assert_eq!(adm_drifted.job.phase, CoworkerJobPhase::Held);
        assert!(!adm_drifted.runtime_state.stop_requested);
        assert_eq!(adm_drifted.job.holds.len(), 1);
        assert!(adm_drifted.job.holds[0].active);
        assert!(adm_drifted.job.holds[0].holds_entire_job);
        assert!(is_registry_configuration_changed_hold(&adm_drifted.job.holds[0]));
        let step_drifted = drift_session
            .step(31_000)
            .await
            .expect("controller step while held by Worker PATH target drift");
        assert!(step_drifted.dispatched_task_ids.is_empty());
        assert_eq!(
            read_invocation_count(&fixture.agy_coder_count_file),
            coder_before_hold
        );
        drift_session.release().expect("release drift_session");

        // Missing PATH executable for System-mode AGY Worker must also fence the job with
        // RegistryConfigurationChanged and stop_requested == false.
        fs::write(&workflow_ledger_path, &saved_workflow_bytes).unwrap();
        fs::write(&policy_ledger_path, &saved_policy_bytes).unwrap();
        fs::remove_file(&agy_link).unwrap();
        let missing_session = code_driver
            .controller()
            .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 40_000)
            .expect("open_owned_session must fence when Worker PATH executable is missing");
        let adm_missing = missing_session
            .verify_admission()
            .expect("verify_admission when Worker PATH executable is missing");
        assert_eq!(adm_missing.job.phase, CoworkerJobPhase::Held);
        assert!(!adm_missing.runtime_state.stop_requested);
        assert_eq!(adm_missing.job.holds.len(), 1);
        assert!(adm_missing.job.holds[0].active);
        assert!(adm_missing.job.holds[0].holds_entire_job);
        assert!(is_registry_configuration_changed_hold(&adm_missing.job.holds[0]));
        missing_session.release().expect("release missing_session");
        host_control.shutdown().await.expect("shutdown host");
    })
    .await;
}

#[tokio::test]
async fn system_mode_agy_worker_with_codex_advisory_continues_on_unrelated_registry_edit_and_holds_on_path_drift(
) {
    use threadlane_workers::{
        run_bounded_command, BoundedCommandBudgets, BoundedCommandSpec, BoundedCommandStatus,
    };

    let tmp = TempDir::new().expect("tempdir for isolated PATH");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
    let isolated_bin = root.join("isolated-system-bin");
    populate_isolated_system_path_dir_for_drift(&isolated_bin);

    let exe = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");
    let isolated_bin_str = isolated_bin
        .to_str()
        .expect("utf8 isolated_bin")
        .to_string();
    let report = run_bounded_command(&BoundedCommandSpec {
        executable: exe,
        argv: vec![
            "--exact".to_string(),
            "coworker_full_driver::coworker_registry_drift::system_mode_agy_worker_registry_drift_child_probe"
                .to_string(),
            "--nocapture".to_string(),
        ],
        working_directory: root,
        env_overrides: vec![
            ("PATH".to_string(), isolated_bin_str.clone()),
            (
                "THREADLANE_DRIFT_SYSTEM_MODE_AGY_BIN".to_string(),
                isolated_bin_str,
            ),
        ],
        strip_git_env: true,
        budgets: BoundedCommandBudgets {
            timeout_ms: 180_000,
            max_stdout_bytes: 262_144,
            max_stderr_bytes: 262_144,
        },
    })
    .await
    .expect("run bounded registry drift child probe");
    assert_eq!(
        report.status,
        BoundedCommandStatus::Succeeded,
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        report.status,
        String::from_utf8_lossy(&report.stdout_bytes),
        String::from_utf8_lossy(&report.stderr_bytes)
    );
    assert_eq!(
        report.exit_code,
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&report.stdout_bytes),
        String::from_utf8_lossy(&report.stderr_bytes)
    );
}
