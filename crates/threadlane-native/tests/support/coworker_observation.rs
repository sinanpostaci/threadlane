use super::*;
use std::process::Command;
use threadlane_core::observation::{
    current_process_instance, is_process_instance_alive, read_journal_records_readonly,
    ObservationActorEvidence, ObservationCallState, ObservationJournalRecord,
    ObservationProcessInstance, ObservationRecordPhase, VerifiedMcpObservationContext,
    OBSERVATION_JOURNAL_FILE_NAME,
};
use threadlane_native::coworker_observation::{
    with_verified_coworker_observation_context, COWORKER_OBS_OP_CHECKS_CANDIDATE,
    COWORKER_OBS_OP_CHECKS_TASK, COWORKER_OBS_OP_CLOSURE_CANDIDATE,
    COWORKER_OBS_OP_CODER_CORRECTION, COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION,
    COWORKER_OBS_OP_INTEGRATION_TASK, COWORKER_OBS_OP_PLANNING_ARCHITECT,
    COWORKER_OBS_OP_PLANNING_ORCHESTRATOR, COWORKER_OBS_OP_REVIEW_TASK,
};
use threadlane_native::coworker_workflow::{
    ArchitectContextDependencies, RunArchitectAdviceRequest, StructuredArchitectAdviceOutput,
};

fn read_coworker_journal_records(fixture: &ControllerTestFixture) -> Vec<ObservationJournalRecord> {
    let (records, warnings) =
        read_journal_records_readonly(&fixture.plans_dir).expect("read journal records readonly");
    assert!(
        warnings.is_empty(),
        "unexpected journal read warnings: {warnings:?}"
    );
    records
        .into_iter()
        .filter(|rec| rec.operation.starts_with("coworker."))
        .collect()
}

fn assert_matching_entry_and_finish<'a>(
    records: &'a [ObservationJournalRecord],
    operation: &str,
    expected_final_state: ObservationCallState,
) -> (&'a ObservationJournalRecord, &'a ObservationJournalRecord) {
    let matching: Vec<&ObservationJournalRecord> = records
        .iter()
        .filter(|r| r.operation == operation)
        .collect();
    assert!(
        matching.len() >= 2,
        "expected at least Entry + Finish for `{operation}`, found {}: {matching:?}",
        matching.len()
    );
    let entry = matching
        .iter()
        .copied()
        .find(|r| r.phase == ObservationRecordPhase::Entry)
        .unwrap_or_else(|| panic!("missing Entry record for `{operation}`"));
    let finish = matching
        .iter()
        .copied()
        .rev()
        .find(|r| r.phase == ObservationRecordPhase::Finish && r.call_id == entry.call_id)
        .unwrap_or_else(|| {
            panic!(
                "missing Finish record for `{operation}` call `{:?}`",
                entry.call_id
            )
        });

    assert_eq!(entry.request_id, finish.request_id);
    assert_eq!(entry.call_id, finish.call_id);
    assert!(entry.call_id.is_some());
    assert_eq!(entry.state, ObservationCallState::Running);
    assert!(
        entry.process_instance.is_some(),
        "Entry record for `{operation}` must bind a live local ObservationProcessInstance"
    );
    assert_eq!(finish.state, expected_final_state);
    assert!(
        finish.at_ms >= entry.at_ms,
        "Finish timestamp must be >= Entry timestamp for `{operation}`"
    );
    assert!(
        finish
            .summary
            .as_deref()
            .is_some_and(|s| s.contains("duration_ms=")),
        "Finish summary for `{operation}` must include measured duration_ms: {:?}",
        finish.summary
    );
    assert!(
        entry.usage.is_none() && finish.usage.is_none(),
        "unreported token/cost usage for `{operation}` must remain None (never 0 or estimated)"
    );
    (entry, finish)
}

fn process_instance_for_child_pid(pid: u32) -> ObservationProcessInstance {
    let base = current_process_instance().expect("host Linux process instance available");
    let stat =
        fs::read_to_string(format!("/proc/{pid}/stat")).expect("read child /proc/<pid>/stat");
    let rparen = stat.rfind(')').expect("stat closing paren");
    let after = stat[rparen + 1..].trim_start();
    let mut fields = after.split_whitespace();
    let _state = fields.next().expect("process state");
    let start_ticks = fields
        .nth(18)
        .expect("starttime field 22")
        .parse::<u64>()
        .expect("parse start_ticks");
    ObservationProcessInstance {
        pid,
        boot_id: base.boot_id,
        start_ticks,
    }
}

#[tokio::test]
async fn coworker_observation_records_entry_and_finish_across_planning_checks_reviewer_correction_integration_and_closure(
) {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"RAW_CHECK_STDOUT_BODY_DO_NOT_LEAK_888\\nrunning 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    // 1. Create an unrelated job in BINDING_B with a sensitive marker to prove cross-job and
    // raw-prompt isolation in observation-journal.jsonl.
    let mut unrelated_brief =
        sample_brief_for_scope(&fixture, BINDING_B, THREAD_UUID_B, "brf_obs_unrelated_b");
    unrelated_brief.summary = "UNRELATED_JOB_SECRET_SUMMARY_999".to_string();
    let unrelated_job = fixture
        .workflow_store
        .create_or_amend_job(&CreateCoworkerJobRequest {
            binding_id: BINDING_B.to_string(),
            thread_uuid: THREAD_UUID_B.to_string(),
            request_key: "req_job_obs_unrelated_b".to_string(),
            expected_ledger_revision: None,
            work_brief: unrelated_brief,
            amended_target_task_ids: Vec::new(),
            amended_write_paths: Vec::new(),
            amended_interfaces: Vec::new(),
        })
        .expect("create unrelated job in BINDING_B");

    // 2. Obtain read-only Architect advice under a verified MCP enrollment context and verify:
    //    - `COWORKER_OBS_OP_PLANNING_ARCHITECT` emits one Entry (`Running`) and one Finish (`Completed`)
    //    - `actor` has `Configured` evidence (`paired_mcp`) with `model: None` (never inferred from profile's Follower)
    //    - `requested_model`/`requested_effort` come from frozen `SelectedRoleChoice` while
    //      `observed_model`/`observed_effort` come strictly from provider runtime metadata
    //    - exact dependency-bound advice reuse appends zero new journal rows.
    let pb = project_base(&fixture);
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let arch_grant = fixture
        .workflow_store
        .create_planning_grant(&CreatePlanningGrantRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            request_key: "req_arch_obs_grant".to_string(),
            expected_ledger_revision: None,
            project_base: pb.clone(),
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
        .expect("create architect planning grant");

    let arch_output = StructuredArchitectAdviceOutput {
        schema_version: 1,
        decision_summary: "Keep src/a.rs ownership isolated to task-obs-main.".to_string(),
        rationale: "Single-module boundary avoids cross-task merge contention.".to_string(),
        recommended_write_paths: vec!["src/a.rs".to_string()],
        interface_contracts: BTreeMap::new(),
        open_questions: Vec::new(),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&arch_output).expect("serialize StructuredArchitectAdviceOutput"),
    )
    .expect("write architect response");

    let dep_blobs =
        CoworkerPlanningService::compute_dependency_file_blobs(&pb, &["src/a.rs".to_string()])
            .expect("compute dependency file blobs");
    let brief_for_arch = sample_brief(&fixture, "brf_obs_arch");
    let arch_deps = ArchitectContextDependencies {
        project_base: pb.clone(),
        work_brief_digest: brief_for_arch.digest().expect("brief digest"),
        plan_digest: None,
        policy_digest: planning_only_policy().digest().expect("policy digest"),
        dependency_file_blobs: dep_blobs,
        interface_ownership_digests: BTreeMap::new(),
        accepted_predecessor_trees: BTreeMap::new(),
    };
    let verified_ctx = VerifiedMcpObservationContext::new(
        BINDING_A,
        THREAD_UUID_A,
        "Observation Native Chat",
        &fixture.repo_dir,
        Some("proj-obs"),
    );
    let arch_req = RunArchitectAdviceRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        grant_id: arch_grant.grant_id.clone(),
        request_key: "req_arch_obs_1".to_string(),
        expected_ledger_revision: None,
        difficulty: Some(Difficulty::Hard),
        context_dependencies: arch_deps,
        question_summary: "How should src/a.rs write ownership be partitioned?".to_string(),
        prior_advice_context: Vec::new(),
        accept_into_brief: false,
        current_time_ms: 1_500,
    };
    let arch_outcome = with_verified_coworker_observation_context(
        Some(verified_ctx),
        fixture.planning_service.obtain_architect_advice(&arch_req),
    )
    .await
    .expect("obtain_architect_advice");
    assert!(!arch_outcome.reused_existing);

    let records_after_arch = read_coworker_journal_records(&fixture);
    assert_eq!(
        records_after_arch.len(),
        2,
        "single Architect call must append exactly Entry + Finish"
    );
    let (arch_entry, arch_finish) = assert_matching_entry_and_finish(
        &records_after_arch,
        COWORKER_OBS_OP_PLANNING_ARCHITECT,
        ObservationCallState::Completed,
    );
    assert_eq!(arch_entry.binding_id.as_deref(), Some(BINDING_A));
    assert_eq!(arch_entry.thread_uuid.as_deref(), Some(THREAD_UUID_A));
    assert_eq!(
        arch_entry.thread_label.as_deref(),
        Some("Observation Native Chat")
    );
    assert_eq!(
        arch_entry.project_path.as_deref(),
        Some(fixture.repo_dir.to_string_lossy().as_ref())
    );
    let arch_actor = arch_entry.actor.as_ref().expect("arch_entry actor");
    assert_eq!(arch_actor.role.as_deref(), Some("coworker"));
    assert_eq!(arch_actor.source.as_deref(), Some("paired_mcp"));
    assert_eq!(arch_actor.evidence, ObservationActorEvidence::Configured);
    assert_eq!(
        arch_actor.model, None,
        "verified Coworker actor must never infer active Codex model from profile Follower assignment"
    );
    assert_eq!(arch_actor.effort, None);

    let arch_exec_entry = arch_entry.executor.as_ref().expect("arch_entry executor");
    assert_eq!(arch_exec_entry.observed_model, None);
    assert_eq!(arch_exec_entry.observed_effort, None);

    let arch_exec_finish = arch_finish.executor.as_ref().expect("arch_finish executor");
    assert_eq!(arch_exec_finish.role.as_deref(), Some("architect"));
    assert_eq!(
        arch_exec_finish.requested_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(arch_exec_finish.requested_effort.as_deref(), Some("high"));
    assert_eq!(
        arch_exec_finish.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(
        arch_exec_finish.observed_effort.as_deref(),
        Some("high"),
        "high comes from the runtime init's allowlisted concrete Antigravity model variant, not an explicit effort field or independent reasoning budget"
    );

    // Exact dependency-bound Architect cache hit must not append any new journal records.
    let arch_cached = fixture
        .planning_service
        .obtain_architect_advice(&RunArchitectAdviceRequest {
            request_key: "req_arch_obs_2_cached".to_string(),
            current_time_ms: 1_600,
            ..arch_req
        })
        .await
        .expect("cached obtain_architect_advice");
    assert!(arch_cached.reused_existing);
    assert_eq!(
        read_coworker_journal_records(&fixture).len(),
        records_after_arch.len(),
        "cached Architect advice reuse must not append new observation journal records"
    );

    // 3. Run full workflow lifecycle on `job`:
    //    - Orchestrator planning (`COWORKER_OBS_OP_PLANNING_ORCHESTRATOR`)
    //    - Original ApprovedHost Coder task (`CTRL_TASK_A` + secret prompt marker)
    //    - Per-task verification check (`COWORKER_OBS_OP_CHECKS_TASK`)
    //    - Task Reviewer returning `CorrectInScope` (`COWORKER_OBS_OP_REVIEW_TASK`)
    //    - Derived Coder correction execution (`COWORKER_OBS_OP_CODER_CORRECTION`)
    //    - Attempt 2 verification check (`COWORKER_OBS_OP_CHECKS_TASK`)
    //    - Attempt 2 Task Reviewer returning `Accept` (`COWORKER_OBS_OP_REVIEW_TASK`)
    //    - Per-task original `RunIntegration` (`COWORKER_OBS_OP_INTEGRATION_TASK`)
    //    - Final job completion (`COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION`,
    //      `COWORKER_OBS_OP_CHECKS_CANDIDATE`, `COWORKER_OBS_OP_CLOSURE_CANDIDATE`)
    let task_id = "task-obs-main";
    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        task_id.to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_obs_main".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_obs_main",
        "plan-obs-main",
        vec![PlanTask {
            id: task_id.to_string(),
            title: "Observation Main Task".to_string(),
            instruction: "CTRL_TASK_A SECRET_RAW_PROMPT_MARKER_DO_NOT_LEAK_777 implement src/a.rs"
                .to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        task_interfaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let records_after_plan = read_coworker_journal_records(&fixture);
    let (orch_entry, orch_finish) = assert_matching_entry_and_finish(
        &records_after_plan,
        COWORKER_OBS_OP_PLANNING_ORCHESTRATOR,
        ObservationCallState::Completed,
    );
    assert_eq!(orch_entry.binding_id.as_deref(), Some(BINDING_A));
    assert_eq!(orch_entry.thread_uuid.as_deref(), Some(THREAD_UUID_A));
    assert_eq!(orch_entry.job_id.as_deref(), Some(job.job_id.as_str()));
    assert_eq!(orch_finish.plan_id.as_deref(), Some("plan-obs-main"));
    let orch_actor = orch_entry.actor.as_ref().expect("orch_entry actor");
    assert_eq!(
        orch_actor.evidence,
        ObservationActorEvidence::Unknown,
        "unverified local planning call without MCP context must report Unknown actor evidence"
    );
    assert_eq!(orch_actor.model, None);
    assert_eq!(
        orch_finish
            .executor
            .as_ref()
            .and_then(|e| e.role.as_deref()),
        Some("orchestrator")
    );

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 200_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 200_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .expect("task observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {task_id} ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Verify controller `step()` polling and original ApprovedHost Coder execution did not invent
    // a duplicate `coworker.coder.*` journal record for attempt 1.
    assert!(
        read_coworker_journal_records(&fixture)
            .iter()
            .all(|r| r.operation != COWORKER_OBS_OP_CODER_CORRECTION),
        "original ApprovedHost Coder attempt 1 must not emit a duplicate coworker.coder.correction record"
    );

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_1 = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify attempt 1");
    assert_eq!(
        verify_1.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    let review_subject_1 = verify_1
        .review_subject
        .as_ref()
        .expect("review_subject_1")
        .clone();
    let policy_digest = job
        .approval_linkage
        .as_ref()
        .expect("approval linkage")
        .policy_digest
        .clone();

    let correct_in_scope_resp = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: task_id.to_string(),
        attempt_no: 1,
        candidate_id: verify_1.candidate_id.clone(),
        review_subject_digest: review_subject_1.subject_digest.clone(),
        base_commit: review_subject_1.base_commit.clone(),
        task_head_commit: review_subject_1.task_head_commit.clone(),
        task_tree_sha: review_subject_1.task_tree_sha.clone(),
        diff_digest: review_subject_1.diff_digest.clone(),
        policy_digest: policy_digest.clone(),
        outcome: ReviewVerdictOutcome::CorrectInScope,
        findings: vec!["src/a.rs::a() returns 100 instead of 101".to_string()],
        required_in_scope_fixes: vec![
            "Update src/a.rs::a() to return 101 without changing signature".to_string(),
        ],
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("Requires bounded in-scope fix in src/a.rs.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&correct_in_scope_resp).expect("serialize review 1 response"),
    )
    .expect("write review 1 response");

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let review_1 = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 1,
                now_ms: step_ts + 100,
                untrusted_candidate_context: None,
            },
        )
        .await
        .expect("review attempt 1");
    assert_eq!(
        review_1.status,
        CoworkerTaskReviewStageStatus::CorrectInScopeRequired
    );

    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let prep_corr = correction_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 200,
            },
        )
        .await
        .expect("prepare_task_correction");
    assert_eq!(
        prep_corr.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_corr_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: task_id.to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };
    let exec_corr = correction_exec_service
        .execute_task_correction(session.guard(), &exec_corr_req)
        .await
        .expect("execute_task_correction");
    assert_eq!(
        exec_corr.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );

    let verify_2 = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 2,
                now_ms: step_ts + 400,
            },
        )
        .await
        .expect("verify attempt 2");
    assert_eq!(
        verify_2.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    let review_subject_2 = verify_2
        .review_subject
        .as_ref()
        .expect("review_subject_2")
        .clone();

    let accept_resp_2 = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: task_id.to_string(),
        attempt_no: 2,
        candidate_id: verify_2.candidate_id.clone(),
        review_subject_digest: review_subject_2.subject_digest.clone(),
        base_commit: review_subject_2.base_commit.clone(),
        task_head_commit: review_subject_2.task_head_commit.clone(),
        task_tree_sha: review_subject_2.task_tree_sha.clone(),
        diff_digest: review_subject_2.diff_digest.clone(),
        policy_digest,
        outcome: ReviewVerdictOutcome::Accept,
        findings: Vec::new(),
        required_in_scope_fixes: Vec::new(),
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("Accepted derived attempt 2.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&accept_resp_2).expect("serialize review 2 response"),
    )
    .expect("write review 2 response");

    let review_2_req = CoworkerTaskReviewStageRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: task_id.to_string(),
        attempt_no: 2,
        now_ms: step_ts + 500,
        untrusted_candidate_context: None,
    };
    let review_2 = review_service
        .review_task_attempt(session.guard(), &review_2_req)
        .await
        .expect("review attempt 2");
    assert_eq!(review_2.status, CoworkerTaskReviewStageStatus::Accepted);

    let int_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let int_req = CoworkerTaskIntegrationRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: task_id.to_string(),
        attempt_no: 2,
        now_ms: step_ts + 600,
    };
    let int_out = int_service
        .integrate_accepted_task(session.guard(), &int_req)
        .expect("integrate_accepted_task");
    assert_eq!(int_out.status, CoworkerTaskIntegrationStatus::Integrated);

    let completion_service =
        CoworkerJobCompletionService::from_controller(fixture.controller.clone())
            .expect("construct CoworkerJobCompletionService");
    let comp_req = CoworkerJobCompletionRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        now_ms: step_ts + 700,
    };
    let comp_out = completion_service
        .complete_job(session.guard(), &comp_req)
        .await
        .expect("complete_job");
    assert_eq!(comp_out.status, CoworkerJobCompletionStatus::Completed);

    // 4. Inspect all Coworker observation journal records and verify every stage boundary,
    //    scope correlation, role/model/effort provenance, and absence of fabricated usage.
    let all_records = read_coworker_journal_records(&fixture);
    for op in [
        COWORKER_OBS_OP_PLANNING_ARCHITECT,
        COWORKER_OBS_OP_PLANNING_ORCHESTRATOR,
        COWORKER_OBS_OP_CHECKS_TASK,
        COWORKER_OBS_OP_REVIEW_TASK,
        COWORKER_OBS_OP_CODER_CORRECTION,
        COWORKER_OBS_OP_INTEGRATION_TASK,
        COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION,
        COWORKER_OBS_OP_CHECKS_CANDIDATE,
        COWORKER_OBS_OP_CLOSURE_CANDIDATE,
    ] {
        let (entry, finish) =
            assert_matching_entry_and_finish(&all_records, op, ObservationCallState::Completed);
        assert_eq!(entry.binding_id.as_deref(), Some(BINDING_A));
        assert_eq!(entry.thread_uuid.as_deref(), Some(THREAD_UUID_A));
        if op != COWORKER_OBS_OP_PLANNING_ARCHITECT {
            assert_eq!(
                entry.job_id.as_deref(),
                Some(job.job_id.as_str()),
                "operation `{op}` must bind job_id"
            );
            assert_eq!(
                finish.plan_id.as_deref(),
                Some("plan-obs-main"),
                "operation `{op}` must bind plan_id"
            );
        }
        if matches!(
            op,
            COWORKER_OBS_OP_CHECKS_TASK
                | COWORKER_OBS_OP_REVIEW_TASK
                | COWORKER_OBS_OP_CODER_CORRECTION
                | COWORKER_OBS_OP_INTEGRATION_TASK
        ) {
            assert_eq!(
                entry.task_id.as_deref(),
                Some(task_id),
                "task operation `{op}` must bind task_id"
            );
            assert_eq!(
                entry.approval_id.as_deref(),
                Some(approval_id.as_str()),
                "post-approval task operation `{op}` must bind approval_id"
            );
        }
        if matches!(
            op,
            COWORKER_OBS_OP_INTEGRATION_JOB_AGGREGATION
                | COWORKER_OBS_OP_CHECKS_CANDIDATE
                | COWORKER_OBS_OP_CLOSURE_CANDIDATE
        ) {
            assert_eq!(
                finish.approval_id.as_deref(),
                Some(approval_id.as_str()),
                "job completion operation `{op}` must bind approval_id"
            );
        }
    }

    // Verify derived Coder correction specifically captures both requested and runtime-observed
    // model + effort without inventing token/cost usage.
    let (corr_entry, corr_finish) = assert_matching_entry_and_finish(
        &all_records,
        COWORKER_OBS_OP_CODER_CORRECTION,
        ObservationCallState::Completed,
    );
    assert_eq!(
        corr_entry
            .executor
            .as_ref()
            .and_then(|e| e.observed_model.as_deref()),
        None
    );
    let corr_exec = corr_finish.executor.as_ref().expect("corr_finish executor");
    assert_eq!(corr_exec.role.as_deref(), Some("coder"));
    assert_eq!(
        corr_exec.requested_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(corr_exec.requested_effort.as_deref(), Some("high"));
    assert_eq!(
        corr_exec.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(corr_exec.observed_effort.as_deref(), Some("high"));
    assert!(corr_finish.usage.is_none());

    // Verify Reviewer and Closure role attribution.
    let (_, rev_finish) = assert_matching_entry_and_finish(
        &all_records,
        COWORKER_OBS_OP_REVIEW_TASK,
        ObservationCallState::Completed,
    );
    assert_eq!(
        rev_finish.executor.as_ref().and_then(|e| e.role.as_deref()),
        Some("reviewer")
    );
    let (_, closure_finish) = assert_matching_entry_and_finish(
        &all_records,
        COWORKER_OBS_OP_CLOSURE_CANDIDATE,
        ObservationCallState::Completed,
    );
    assert_eq!(
        closure_finish
            .executor
            .as_ref()
            .and_then(|e| e.role.as_deref()),
        Some("closure")
    );

    // Verify raw prompt secrets, check stdout bodies, and unrelated jobs never leak into
    // `observation-journal.jsonl`.
    let raw_journal = fs::read_to_string(fixture.plans_dir.join(OBSERVATION_JOURNAL_FILE_NAME))
        .expect("read raw observation-journal.jsonl");
    assert!(!raw_journal.contains("SECRET_RAW_PROMPT_MARKER_DO_NOT_LEAK_777"));
    assert!(!raw_journal.contains("<untrusted_required_in_scope_fixes>"));
    assert!(!raw_journal.contains("RAW_CHECK_STDOUT_BODY_DO_NOT_LEAK_888"));
    assert!(!raw_journal.contains("UNRELATED_JOB_SECRET_SUMMARY_999"));
    assert!(!raw_journal.contains(&unrelated_job.job_id));

    // 5. Verify idempotent replays and read-only polls append ZERO new journal rows and consume
    //    ZERO additional role budgets.
    let count_before_replays = all_records.len();
    let runtime_before_replays = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime before replays")
        .expect("runtime exists");

    // Terminal no-effect refusals: the Completed job rejects correction and
    // review replay through the executing-phase guard; these are not cache hits.
    let correction_replay_error = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 800,
                ..exec_corr_req
            },
        )
        .await
        .expect_err("Completed job must reject correction/review replay");
    assert_eq!(
        correction_replay_error.code,
        threadlane_native::NativeErrorCode::ValidationFailed
    );
    assert!(
        correction_replay_error.message.contains("`Completed`"),
        "correction replay error should identify the `Completed` phase: {}",
        correction_replay_error.message
    );

    let review_replay_error = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                now_ms: step_ts + 850,
                ..review_2_req
            },
        )
        .await
        .expect_err("Completed job must reject correction/review replay");
    assert_eq!(
        review_replay_error.code,
        threadlane_native::NativeErrorCode::ValidationFailed
    );
    assert!(
        review_replay_error.message.contains("`Completed`"),
        "review replay error should identify the `Completed` phase: {}",
        review_replay_error.message
    );

    let replay_int = int_service
        .integrate_accepted_task(
            session.guard(),
            &CoworkerTaskIntegrationRequest {
                now_ms: step_ts + 900,
                ..int_req
            },
        )
        .expect("idempotent replay integrate_accepted_task");
    assert_eq!(
        replay_int.status,
        CoworkerTaskIntegrationStatus::ReopenedIntegrated
    );

    let replay_comp = completion_service
        .complete_job(
            session.guard(),
            &CoworkerJobCompletionRequest {
                now_ms: step_ts + 950,
                ..comp_req
            },
        )
        .await
        .expect("idempotent replay complete_job");
    assert_eq!(replay_comp.status, CoworkerJobCompletionStatus::Completed);

    // Read-only status/ledger/runtime polls:
    let _ = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("readonly ledger poll");
    let runtime_after_replays = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("readonly runtime poll")
        .expect("runtime exists");

    assert_eq!(
        read_coworker_journal_records(&fixture).len(),
        count_before_replays,
        "idempotent replays and read-only polls must not append any new observation journal rows"
    );
    assert_eq!(
        runtime_after_replays.role_usage, runtime_before_replays.role_usage,
        "idempotent replays and read-only polls must not charge additional role budgets"
    );

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn coworker_observation_degrades_failed_checks_and_lost_processes_without_staying_running() {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\necho \"compile error in src/a.rs\" >&2\nexit 1\n",
    );

    let task_id = "task-obs-fail";
    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        task_id.to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_obs_fail".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        &fixture,
        "brf_obs_fail",
        "plan-obs-fail",
        vec![PlanTask {
            id: task_id.to_string(),
            title: "Observation Fail Check Task".to_string(),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        task_interfaces,
        vec!["src/a.rs".to_string()],
        1,
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
    )
    .await;

    let host_handle = ApprovedHost::start(ApprovedHostConfig::new(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        &approval_id,
    ))
    .await
    .expect("start ApprovedHost");

    let session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 300_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 300_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == task_id)
            .expect("task observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {task_id} ReadyForReview: {rep:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 1. Failing verification check must emit Entry (`Running`) and terminal Finish (`Failed`)
    //    with `non_zero_exit` diagnostic rather than staying `Running` or fabricating `Completed`.
    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_fail = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt with failing cargo test");
    assert_eq!(
        verify_fail.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );

    let records = read_coworker_journal_records(&fixture);
    let (check_entry, check_finish) = assert_matching_entry_and_finish(
        &records,
        COWORKER_OBS_OP_CHECKS_TASK,
        ObservationCallState::Failed,
    );
    assert!(
        check_finish
            .summary
            .as_deref()
            .is_some_and(|s| s.contains("non_zero_exit")),
        "failed check Finish summary must report non_zero_exit: {:?}",
        check_finish.summary
    );

    // 2. Spawn a real local child process, capture its `ObservationProcessInstance` (`pid`,
    //    `boot_id`, and `start_ticks`), attach it to an in-flight Entry record, verify it resolves
    //    as `Running` while alive, then kill and reap the process and verify `resolve_in_flight_state`
    //    degrades to `ObservationCallState::Unknown` so crashed/lost processes never remain `Running`.
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 30")
        .spawn()
        .expect("spawn temporary sleep process");
    let pid = child.id();
    let proc_inst = process_instance_for_child_pid(pid);
    assert!(
        is_process_instance_alive(&proc_inst),
        "live child process must verify as alive with matching boot_id and start_ticks"
    );

    let mut in_flight_entry = check_entry.clone();
    in_flight_entry.pid = Some(pid);
    in_flight_entry.process_instance = Some(proc_inst.clone());
    assert!(in_flight_entry.has_live_process_instance());
    assert_eq!(
        in_flight_entry.resolve_in_flight_state(false),
        ObservationCallState::Running
    );

    child.kill().expect("kill child process");
    let _ = child.wait().expect("wait for killed child process");
    assert!(
        !is_process_instance_alive(&proc_inst),
        "killed and reaped process must not report is_process_instance_alive == true"
    );
    assert!(!in_flight_entry.has_live_process_instance());
    assert_eq!(
        in_flight_entry.resolve_in_flight_state(false),
        ObservationCallState::Unknown,
        "in-flight Entry record whose OS process died must degrade to Unknown rather than staying Running"
    );

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}
