use super::*;
use threadlane_native::coworker_workflow::controller_policy::{
    CheckFailureCorrectionBasis, CheckFailureReportRef,
};

struct PreparedCorrectionFixtureTask {
    job: CoworkerJobRecord,
    approval_id: String,
    approval_before: threadlane_authority::ApprovalRecord,
    approved_task_before: threadlane_authority::ApprovedTaskRecord,
    host_handle: threadlane_host::ApprovedHostHandle,
    session: threadlane_native::coworker_workflow::OwnedCoworkerControllerSession,
    step_ts: u64,
    verify_1: CoworkerTaskVerificationOutcome,
    review_subject_1: ReviewSubject,
    policy_digest: String,
    review_1: threadlane_native::coworker_workflow::CoworkerTaskReviewStageOutcome,
    art_key_1: String,
    art_1_before: threadlane_native::coworker_workflow::TaskAttemptExecutionArtifacts,
    task_manifest: threadlane_workers::WorktreeManifest,
    prep_outcome: threadlane_native::coworker_workflow::CoworkerTaskCorrectionPrepareOutcome,
    receipt: threadlane_native::coworker_workflow::DerivedTaskAttemptReceipt,
    expected_req_key: String,
    expected_eff_id: String,
    expected_report_id: String,
}

fn write_codex_reviewer_override(
    state_dir: &Path,
    task_id: &str,
    attempt_no: u32,
    spec: serde_json::Value,
) {
    fs::write(
        state_dir.join(format!(
            "reviewer_override_{task_id}_attempt_{attempt_no}.json"
        )),
        serde_json::to_string(&spec).expect("serialize Codex Reviewer override"),
    )
    .expect("write Codex Reviewer override");
}

fn replace_argv_arg(argv: &mut [String], from: &str, to: &str) {
    let index = argv
        .iter()
        .position(|arg| arg == from)
        .unwrap_or_else(|| panic!("missing argv entry `{from}`"));
    argv[index] = to.to_string();
}

async fn assert_codex_reviewer_report_rejected(
    service: &CoworkerTaskCorrectionService,
    session: &threadlane_native::coworker_workflow::OwnedCoworkerControllerSession,
    job: &CoworkerJobRecord,
    task_id: &str,
    now_ms: u64,
    report_path: &Path,
    report: &threadlane_native::coworker_workflow::TaskReviewExecutionReport,
) {
    fs::write(
        report_path,
        serde_json::to_vec_pretty(report).expect("serialize tampered Reviewer report"),
    )
    .expect("write tampered Reviewer report");
    fs::set_permissions(report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 tampered Reviewer report");
    let err = service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                prior_attempt_no: 1,
                now_ms,
            },
        )
        .await
        .expect_err("tampered Codex Reviewer report must be rejected");
    assert_eq!(
        err.code(),
        NativeErrorCode::ValidationFailed.as_str(),
        "tampered Codex Reviewer report rejection code"
    );
}

async fn prepare_task_for_derived_correction(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    task_id: &str,
    required_fixes: Vec<String>,
) -> PreparedCorrectionFixtureTask {
    prepare_task_for_derived_correction_with_budgets(
        fixture,
        brief_id,
        plan_id,
        task_id,
        required_fixes,
        ExecutionBudgets::default(),
    )
    .await
}

async fn prepare_task_for_derived_correction_with_reviewer(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    task_id: &str,
    required_fixes: Vec<String>,
    reviewer_choice: SelectedRoleChoice,
) -> PreparedCorrectionFixtureTask {
    prepare_task_for_derived_correction_with_reviewer_and_budgets(
        fixture,
        brief_id,
        plan_id,
        task_id,
        required_fixes,
        reviewer_choice,
        ExecutionBudgets::default(),
    )
    .await
}

async fn prepare_task_for_derived_correction_with_budgets(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    task_id: &str,
    required_fixes: Vec<String>,
    approval_budgets: ExecutionBudgets,
) -> PreparedCorrectionFixtureTask {
    prepare_task_for_derived_correction_with_reviewer_and_budgets(
        fixture,
        brief_id,
        plan_id,
        task_id,
        required_fixes,
        headless_argon_choice(Difficulty::Hard),
        approval_budgets,
    )
    .await
}

async fn prepare_task_for_derived_correction_with_reviewer_and_budgets(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    task_id: &str,
    required_fixes: Vec<String>,
    reviewer_choice: SelectedRoleChoice,
    approval_budgets: ExecutionBudgets,
) -> PreparedCorrectionFixtureTask {
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    install_owned_health_fixture(fixture);

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        task_id.to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec![format!("iface_{brief_id}")],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_for_scope_with_reviewer(
        fixture,
        BINDING_A,
        THREAD_UUID_A,
        brief_id,
        plan_id,
        vec![PlanTask {
            id: task_id.to_string(),
            title: format!("Correction Task {task_id}"),
            instruction: "CTRL_TASK_A implement src/a.rs".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/a.rs".to_string()],
        }],
        task_interfaces,
        vec!["src/a.rs".to_string()],
        1,
        approval_budgets,
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
        false,
        1,
        2,
        6,
        reviewer_choice.clone(),
    )
    .await;
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);

    let approval_before = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load original ApprovalRecord");
    let approved_task_before = approval_before
        .tasks
        .iter()
        .find(|t| t.task_id == task_id)
        .expect("approved task record")
        .clone();

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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 110_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 110_100u64;
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
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

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
        .expect("verify_and_prepare_task_attempt attempt 1");
    assert_eq!(
        verify_1.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    let review_subject_1 = verify_1
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject attempt 1")
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
        required_in_scope_fixes: required_fixes,
        conflicting_paths_or_interfaces: Vec::new(),
        summary: Some("Requires bounded in-scope fix in src/a.rs to return 101.".to_string()),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&correct_in_scope_resp)
            .expect("serialize CorrectInScope reviewer response"),
    )
    .expect("write CorrectInScope reviewer response");

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
                untrusted_candidate_context: Some(
                    "Implemented initial src/a.rs candidate".to_string(),
                ),
            },
        )
        .await
        .expect("review_task_attempt attempt 1 CorrectInScope");
    assert_eq!(
        review_1.status,
        CoworkerTaskReviewStageStatus::CorrectInScopeRequired
    );
    assert_eq!(
        review_1.task_stage,
        CoworkerTaskStage::CorrectInScopeRequired
    );
    assert!(review_1.review_receipt.is_none());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    if reviewer_choice.adapter == ToolAdapter::CodexCli {
        // Real `codex exec --json` emits no runtime model/effort telemetry. The report must carry
        // the exact pinned read-only launch configuration instead, without inventing observations.
        assert_eq!(review_1.report.provider, WorkerProvider::CodexCli);
        assert_eq!(
            review_1.report.requested_model.base_model,
            reviewer_choice.model_arg
        );
        assert_eq!(
            review_1.report.requested_model.effort,
            reviewer_choice.effort
        );
        assert_eq!(review_1.report.observed_model, None);
        assert_eq!(review_1.report.observed_effort, None);
        let review_execution = review_1.report.execution();
        assert_eq!(review_execution.provider, WorkerProvider::CodexCli);
        assert_eq!(
            review_execution.requested_model.base_model,
            reviewer_choice.model_arg
        );
        assert_eq!(
            review_execution.requested_model.effort,
            reviewer_choice.effort
        );
        assert!(review_execution.has_pinned_codex_read_only_launch_config());
    } else {
        assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);
    }

    let runtime_before_corr = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get job runtime before correction")
        .expect("runtime exists");
    let art_key_1 = ControllerJobRuntimeState::artifact_key(task_id, 1);
    let art_1_before = runtime_before_corr
        .attempt_artifacts
        .get(&art_key_1)
        .expect("attempt 1 artifacts")
        .clone();
    let task_manifest = threadlane_workers::load_and_verify_manifest(
        &review_subject_1.work_area_root,
        &art_1_before.task_worktree_id,
    )
    .expect("load task manifest");
    assert_ne!(art_1_before.run_id, task_manifest.run_id);
    assert_eq!(review_subject_1.run_id, task_manifest.run_id);

    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let prep_outcome = correction_service
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
        prep_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert_eq!(prep_outcome.next_attempt_no, 2);
    assert_eq!(prep_outcome.task_stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    let receipt = prep_outcome
        .require_receipt()
        .expect("require DerivedTaskAttemptReceipt")
        .clone();

    let expected_req_key = compute_derived_coder_request_key(
        &job.job_id,
        task_id,
        2,
        &verify_1.candidate_id,
        &review_1.verdict.verdict_id,
        &review_subject_1.subject_digest,
        &runtime_before_corr.envelope.envelope_digest,
        &headless_argon_choice(Difficulty::Hard),
    );
    let expected_eff_id =
        compute_derived_coder_effect_id(&job.job_id, task_id, 2, &expected_req_key);
    let expected_report_id =
        compute_derived_coder_report_artifact_id(&job.job_id, task_id, 2, &expected_req_key);

    PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1,
        policy_digest,
        review_1,
        art_key_1,
        art_1_before,
        task_manifest,
        prep_outcome,
        receipt,
        expected_req_key,
        expected_eff_id,
        expected_report_id,
    }
}

#[tokio::test]
async fn task_correction_executes_derived_coder_attempt_reopens_idempotently_verifies_reviews_and_integrates(
) {
    // Heap-allocate nested futures to bound the poll frame of this stack-pinned test.
    let fixture = setup_fixture();
    let required_fixes = vec![
        "Update src/a.rs::a() so it returns 101 instead of 100".to_string(),
        "Preserve function signature in src/a.rs".to_string(),
    ];
    let PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1,
        policy_digest,
        review_1,
        art_key_1,
        art_1_before,
        task_manifest,
        prep_outcome,
        receipt,
        expected_req_key,
        expected_eff_id,
        expected_report_id,
    } = Box::pin(prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_exec",
        "plan-corr-exec",
        "task-corr-exec",
        required_fixes,
    ))
    .await;

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());

    // Execute derived Coder attempt 2 via CoworkerTaskCorrectionExecutionService.
    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-exec".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };
    let exec_outcome =
        Box::pin(correction_exec_service.execute_task_correction(session.guard(), &exec_req))
            .await
            .expect("execute_task_correction");

    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(exec_outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert_eq!(exec_outcome.prior_attempt_no, 1);
    assert_eq!(exec_outcome.attempt_no, 2);
    assert!(!exec_outcome.reused_existing);
    assert_eq!(exec_outcome.prior_candidate_id, verify_1.candidate_id);
    assert_eq!(
        exec_outcome.prior_review_verdict_id.as_deref(),
        Some(review_1.verdict.verdict_id.as_str())
    );
    assert_eq!(
        exec_outcome.prior_review_subject_digest,
        review_subject_1.subject_digest
    );
    assert_eq!(
        exec_outcome.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(
        exec_outcome.effect_id.as_deref(),
        Some(expected_eff_id.as_str())
    );
    assert_eq!(exec_outcome.outbox_id, prep_outcome.outbox_id);
    assert_eq!(
        exec_outcome.report_artifact_id.as_deref(),
        Some(expected_report_id.as_str())
    );
    assert_eq!(exec_outcome.receipt.as_ref(), Some(&receipt));
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        2,
        "derived Coder execution must launch a real second headless Coder process"
    );

    let expected_report_path =
        derived_execution_report_file_path(&fixture.plans_dir, &expected_eff_id);
    assert_eq!(
        exec_outcome.report_path.as_deref(),
        Some(expected_report_path.to_string_lossy().as_ref())
    );
    let report_meta =
        fs::symlink_metadata(&expected_report_path).expect("derived execution report metadata");
    assert!(report_meta.file_type().is_file());
    assert_eq!(report_meta.permissions().mode() & 0o777, 0o600);

    let report = exec_outcome
        .report
        .as_ref()
        .expect("PersistedDerivedTaskExecutionArtifact present");
    report
        .validate_integrity()
        .expect("validate PersistedDerivedTaskExecutionArtifact integrity");
    assert_eq!(
        report
            .compute_digest()
            .expect("compute derived execution report digest"),
        report.artifact_digest
    );
    let persisted_report =
        read_persisted_derived_task_execution_report(&fixture.plans_dir, &expected_eff_id)
            .expect("read persisted derived execution report")
            .expect("persisted derived execution report exists");
    assert_eq!(&persisted_report, report);

    assert_eq!(report.report_artifact_id, expected_report_id);
    assert_eq!(report.receipt_id, receipt.receipt_id);
    assert_eq!(report.receipt_digest, receipt.receipt_digest);
    assert_eq!(report.request_key, expected_req_key);
    assert_eq!(report.effect_id, expected_eff_id);
    assert_eq!(report.prior_attempt_no, 1);
    assert_eq!(report.attempt_no, 2);
    assert_eq!(report.original_run_id, art_1_before.run_id);
    assert_eq!(report.original_worker_id, art_1_before.worker_id);
    assert_eq!(report.manifest_run_id, task_manifest.run_id);
    assert_eq!(
        Some(report.manifest_task_id.as_str()),
        task_manifest.task_id.as_deref()
    );
    assert_ne!(
        report.original_run_id, report.manifest_run_id,
        "original host run_id UUID must remain distinct from compact manifest run_id tag"
    );
    assert_ne!(
        report.task_id, report.manifest_task_id,
        "logical task_id must remain distinct from compact manifest task_id tag"
    );
    assert_eq!(report.task_worktree_id, art_1_before.task_worktree_id);
    assert_eq!(
        report.integration_worktree_id,
        art_1_before.integration_worktree_id
    );
    assert_eq!(report.execution_base_commit, fixture.base_commit);
    assert_eq!(
        report.pre_execution_head_commit,
        verify_1.committed_head_commit
    );
    assert_eq!(report.pre_execution_tree_sha, verify_1.committed_tree_sha);
    assert_eq!(
        report.post_execution_head_commit,
        verify_1.committed_head_commit
    );
    assert_eq!(report.post_execution_tree_sha, verify_1.committed_tree_sha);
    assert!(report.has_changes_from_prior_head);
    assert_eq!(report.coder_choice, headless_argon_choice(Difficulty::Hard));
    assert_eq!(report.observed_provider, "antigravity");
    assert_eq!(
        report.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(report.observed_effort.as_deref(), Some("high"));
    assert_eq!(
        report.execution_report.worker_report.requested_session_id, None,
        "derived Coder execution must never reuse a provider session"
    );
    assert_eq!(
        report.execution_report.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert!(report
        .execution_report
        .worker_report
        .has_complete_natural_completion_evidence());
    assert!(report.execution_report.diff_gate.passed);
    assert_eq!(
        report.execution_report.manifest.allowed_write_paths,
        approved_task_before
            .allowed_write_paths
            .iter()
            .cloned()
            .collect()
    );

    let captured_agy_prompt =
        fs::read_to_string(&fixture.agy_prompt_file).expect("read agy prompt file");
    const AGY_TASK_INSTRUCTION_DELIMITER: &str = "[THREADLANE_TASK_INSTRUCTION] ";
    assert!(
        captured_agy_prompt.starts_with("[THREADLANE_AGY_TASK_FILES_CONTRACT] "),
        "captured AGY prompt must start with the TaskFiles execution contract"
    );
    assert_eq!(
        captured_agy_prompt
            .matches(AGY_TASK_INSTRUCTION_DELIMITER)
            .count(),
        1,
        "captured AGY prompt must contain exactly one task instruction delimiter"
    );
    let (agy_contract, retained_instruction) = captured_agy_prompt
        .split_once(AGY_TASK_INSTRUCTION_DELIMITER)
        .expect("captured AGY prompt must contain the task instruction delimiter");
    assert_eq!(retained_instruction, receipt.derived_prompt.as_str());
    assert!(
        agy_contract.contains(
            "Persist changes only within declared task write scopes: [`src/a.rs` (exact file only; no descendant `/**`)]."
        ),
        "contract must scope writes to the exact allowed `src/a.rs` file"
    );
    assert!(
        agy_contract.contains(
            "Never use or request `command`, `run_command`, `bash`, `shell`, `execute`, `run_in_terminal`, or any terminal/shell tool"
        ),
        "contract must forbid terminal/shell execution"
    );
    assert!(
        agy_contract.contains(
            "Do not write provider or control configs (`.gemini`, `.codex`, `.opencode`, `.claude`, `.agents`, `.git`, `AGENTS.md`, `.env*`)."
        ),
        "contract must forbid provider/control configuration writes"
    );
    assert_eq!(
        fs::read_to_string(task_manifest.checkout_path.join("src/a.rs"))
            .expect("read owned task worktree src/a.rs after correction"),
        "pub fn a() -> u32 { 101 }\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("read primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );

    // No second budget charge after explicit preparation.
    let runtime_after_exec = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get runtime after derived execution")
        .expect("runtime exists");
    let coder_usage_after_exec = runtime_after_exec
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder usage after derived execution");
    assert_eq!(coder_usage_after_exec.calls_reserved, 2);
    assert_eq!(coder_usage_after_exec.calls_completed, 2);
    assert_eq!(runtime_after_exec.total_task_attempts_reserved, 2);
    assert_eq!(runtime_after_exec.total_corrections_reserved, 1);
    assert_eq!(runtime_after_exec.derived_attempts, vec![receipt.clone()]);
    let art_key_2 = ControllerJobRuntimeState::artifact_key("task-corr-exec", 2);
    let art_2_after_exec = runtime_after_exec
        .attempt_artifacts
        .get(&art_key_2)
        .expect("attempt 2 TaskAttemptExecutionArtifacts")
        .clone();
    assert!(art_2_after_exec.is_derived_attempt);
    assert_eq!(art_2_after_exec.run_id, art_1_before.run_id);
    assert_eq!(art_2_after_exec.worker_id, art_1_before.worker_id);
    assert_eq!(
        art_2_after_exec.task_worktree_id,
        art_1_before.task_worktree_id
    );
    assert_eq!(
        art_2_after_exec.integration_worktree_id,
        art_1_before.integration_worktree_id
    );
    assert_eq!(art_2_after_exec.execution_base_commit, fixture.base_commit);

    // Repeating execute_task_correction on same session and after session reopen is idempotent
    // without launching another worker process or charging counters.
    let repeat_same = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 400,
            ..exec_req.clone()
        },
    ))
    .await
    .expect("repeat execute_task_correction on same session");
    assert_eq!(
        repeat_same.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert!(repeat_same.reused_existing);
    assert_eq!(repeat_same.attempt_no, 2);
    assert_eq!(repeat_same.report, exec_outcome.report);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 500)
        .expect("reopen session");
    let reopened_exec = Box::pin(correction_exec_service.execute_task_correction(
        reopened_session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 600,
            ..exec_req
        },
    ))
    .await
    .expect("reopen execute_task_correction");
    assert_eq!(
        reopened_exec.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert!(reopened_exec.reused_existing);
    assert_eq!(reopened_exec.attempt_no, 2);
    assert_eq!(reopened_exec.report, exec_outcome.report);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    // Verify derived attempt 2 -> ReadyForReviewer with new committed HEAD, ReviewSubject, and check evidence.
    let verify_2 = Box::pin(verifier.verify_and_prepare_task_attempt(
        reopened_session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: "task-corr-exec".to_string(),
            attempt_no: 2,
            now_ms: step_ts + 700,
        },
    ))
    .await
    .expect("verify_and_prepare_task_attempt attempt 2");
    assert_eq!(
        verify_2.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    assert_eq!(verify_2.attempt_no, 2);
    assert_ne!(
        verify_2.committed_head_commit,
        verify_1.committed_head_commit
    );
    assert_ne!(verify_2.committed_tree_sha, verify_1.committed_tree_sha);
    assert_ne!(verify_2.candidate_id, verify_1.candidate_id);
    assert_eq!(verify_2.check_evidence.len(), 1);
    assert_eq!(verify_2.check_evidence[0].check_id, "cargo_test");
    let review_subject_2 = verify_2
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject attempt 2");
    assert_ne!(
        review_subject_2.subject_digest,
        review_subject_1.subject_digest
    );
    assert_eq!(review_subject_2.base_commit, fixture.base_commit);
    assert_eq!(
        review_subject_2.task_head_commit,
        verify_2.committed_head_commit
    );
    assert_eq!(review_subject_2.task_tree_sha, verify_2.committed_tree_sha);
    assert_eq!(review_subject_2.run_id, task_manifest.run_id);
    assert_eq!(
        Some(review_subject_2.task_id.as_str()),
        task_manifest.task_id.as_deref()
    );

    // Review derived attempt 2 -> Accepted + real ReviewReceipt.
    let accept_resp_2 = StructuredTaskReviewModelResponse {
        schema_version: TASK_REVIEW_RESPONSE_SCHEMA_VERSION,
        task_id: "task-corr-exec".to_string(),
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
        summary: Some(
            "Derived attempt 2 satisfies required in-scope fixes, policy, and passing check evidence."
                .to_string(),
        ),
    };
    fs::write(
        &fixture.agy_plan_response_file,
        serde_json::to_string(&accept_resp_2).expect("serialize Accept reviewer response"),
    )
    .expect("write Accept reviewer response");

    let review_gate = fixture.plans_dir.join("health-review.gate");
    fs::write(&review_gate, b"hold").expect("hold reviewer");
    let inspect_review = async {
        let health = wait_for_owned_role_health(
            &fixture,
            reopened_session.guard(),
            &job.job_id,
            WorkflowRole::Reviewer,
            Some("task-corr-exec"),
            Some(2),
        )
        .await;
        fs::remove_file(&review_gate).expect("release reviewer");
        health
    };
    let health_review_req = CoworkerTaskReviewStageRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-exec".to_string(),
        attempt_no: 2,
        now_ms: step_ts + 800,
        untrusted_candidate_context: Some("Updated src/a.rs::a() to return 101".to_string()),
    };
    let review_future =
        Box::pin(review_service.review_task_attempt(reopened_session.guard(), &health_review_req));
    let (review_result, live_review_health) =
        Box::pin(async { tokio::join!(review_future, inspect_review) }).await;
    let review_2 = review_result.expect("review_task_attempt attempt 2 Accept");
    let terminal_review_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("terminal Reviewer runtime")
        .expect("runtime");
    let terminal_review_health = terminal_review_runtime
        .provider_health
        .get(live_review_health.invocation.key())
        .expect("terminal Reviewer health");
    assert!(terminal_review_health.terminal_observed);
    assert_eq!(
        terminal_review_health.snapshot.unavailable_reason,
        Some(
            threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed
        )
    );
    assert_eq!(
        terminal_review_health.snapshot.first_failure,
        live_review_health.snapshot.first_failure
    );
    assert_eq!(
        terminal_review_health.snapshot,
        review_2
            .report
            .structured_execution
            .execution
            .provider_health
            .clone()
            .expect("Reviewer report health")
    );
    assert_eq!(review_2.status, CoworkerTaskReviewStageStatus::Accepted);
    assert_eq!(review_2.task_stage, CoworkerTaskStage::Accepted);
    assert_eq!(review_2.attempt_no, 2);
    assert_eq!(review_2.candidate_id, verify_2.candidate_id);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 3);
    let review_receipt_2 = review_2
        .review_receipt
        .as_ref()
        .expect("real ReviewReceipt for attempt 2");
    assert_eq!(
        review_receipt_2.subject_digest,
        review_subject_2.subject_digest
    );
    assert_eq!(
        review_receipt_2.task_head_commit,
        verify_2.committed_head_commit
    );

    // Integrate accepted attempt 2 -> Integrated + real IntegrationReceipt.
    let int_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let int_outcome = int_service
        .integrate_accepted_task(
            reopened_session.guard(),
            &CoworkerTaskIntegrationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-exec".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 900,
            },
        )
        .expect("integrate_accepted_task attempt 2");
    assert_eq!(
        int_outcome.status,
        CoworkerTaskIntegrationStatus::Integrated
    );
    assert_eq!(int_outcome.task_stage, CoworkerTaskStage::Integrated);
    assert_eq!(int_outcome.attempt_no, 2);
    assert_eq!(int_outcome.candidate_id, verify_2.candidate_id);
    assert_eq!(int_outcome.review_receipt_id, review_receipt_2.receipt_id);
    assert_eq!(
        int_outcome.review_subject_digest,
        review_subject_2.subject_digest
    );
    assert_eq!(
        int_outcome.previous_integration_head_commit,
        fixture.base_commit
    );
    assert_eq!(
        int_outcome.integrated_target_commit,
        verify_2.committed_head_commit
    );
    assert_eq!(int_outcome.integrated_tree_sha, verify_2.committed_tree_sha);
    assert_eq!(int_outcome.strategy, IntegrationStrategy::FastForwardOnly);

    let loaded_rev_receipt =
        load_review_receipt(host_handle.work_area_dir(), &review_receipt_2.receipt_id)
            .expect("load ReviewReceipt from work-area");
    assert_eq!(&loaded_rev_receipt, review_receipt_2);
    let loaded_int_receipt = load_integration_receipt(
        host_handle.work_area_dir(),
        &int_outcome.integration_receipt_id,
    )
    .expect("load IntegrationReceipt from work-area");
    assert_eq!(loaded_int_receipt, int_outcome.integration_receipt);
    assert_eq!(
        fs::read_to_string(
            int_outcome
                .integration_receipt
                .integration_checkout_path
                .join("src/a.rs")
        )
        .expect("read integrated src/a.rs"),
        "pub fn a() -> u32 { 101 }\n"
    );

    // Repeated controller step must not reset the derived attempt 2 state back to initial host attempt 1.
    let post_int_step = Box::pin(reopened_session.step(step_ts + 1_000))
        .await
        .expect("controller step after attempt 2 integration");
    assert!(post_int_step.dispatched_task_ids.is_empty());
    assert!(post_int_step
        .reconciled_ready_for_review_task_ids
        .is_empty());
    let obs_final = post_int_step
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-exec")
        .expect("final task observation");
    assert_eq!(obs_final.stage, CoworkerTaskStage::Integrated);
    let ledger_final = fixture
        .controller
        .workflow_store()
        .read_binding_ledger(BINDING_A)
        .expect("read binding ledger after post-integration step");
    let job_final = ledger_final
        .jobs
        .iter()
        .find(|j| j.job_id == job.job_id)
        .expect("final job record");
    let task_final = job_final
        .tasks
        .iter()
        .find(|t| t.task_id == "task-corr-exec")
        .expect("final task record");
    assert_eq!(task_final.stage, CoworkerTaskStage::Integrated);
    assert_eq!(task_final.current_attempt_no, 2);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let runtime_final = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("get final runtime")
        .expect("final runtime exists");
    let original_health = runtime_final
        .original_worker_provider_health
        .get("task-corr-exec")
        .expect("original host health");
    assert_eq!(original_health.run_id, art_1_before.run_id);
    assert_eq!(original_health.worker_id, art_1_before.worker_id);
    assert_eq!(original_health.snapshot.retry_count, 1);
    assert!(runtime_final
        .provider_health
        .values()
        .any(|health| matches!(
            &health.invocation,
            threadlane_native::provider_health::ProviderHealthInvocation::Controller {
                role: WorkflowRole::Coder,
                attempt_no: Some(2),
                ..
            }
        )));

    let stored_art_1 = runtime_final
        .attempt_artifacts
        .get(&art_key_1)
        .expect("attempt 1 artifacts preserved");
    let stored_art_2 = runtime_final
        .attempt_artifacts
        .get(&art_key_2)
        .expect("attempt 2 artifacts preserved");
    assert_eq!(stored_art_1.run_id, art_1_before.run_id);
    assert_eq!(stored_art_2.run_id, art_1_before.run_id);
    assert_eq!(stored_art_2.review_receipt.as_ref(), Some(review_receipt_2));
    assert_eq!(
        stored_art_2.integration_receipt.as_ref(),
        Some(&int_outcome.integration_receipt)
    );

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord after full correction lifecycle");
    assert_eq!(approval_after, approval_before);

    let primary_head =
        resolve_exact_commit_sha(&fixture.repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve primary HEAD");
    assert_eq!(primary_head, fixture.base_commit);
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("read primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );

    let closure_gate = fixture.plans_dir.join("health-closure.gate");
    fs::write(&closure_gate, b"hold").expect("hold job Closure");
    let completion_service =
        CoworkerJobCompletionService::from_controller(fixture.controller.clone())
            .expect("completion service");
    let completion_req = CoworkerJobCompletionRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        now_ms: step_ts + 1_100,
    };
    // Match the other nested stages: join only the pinned handle, not the completion state.
    let completion =
        Box::pin(completion_service.complete_job(reopened_session.guard(), &completion_req));
    let inspect_closure = async {
        let health = wait_for_owned_role_health(
            &fixture,
            reopened_session.guard(),
            &job.job_id,
            WorkflowRole::Closure,
            None,
            None,
        )
        .await;
        fs::remove_file(&closure_gate).expect("release job Closure");
        health
    };
    let (completion, live_closure_health) =
        Box::pin(async { tokio::join!(completion, inspect_closure) }).await;
    let completion = completion.expect("complete job with scoped live Closure diagnostics");
    assert_eq!(completion.status, CoworkerJobCompletionStatus::Completed);
    let closure_health = completion
        .provider_health
        .iter()
        .find(|view| view.record.invocation.key() == live_closure_health.invocation.key())
        .expect("final Closure health");
    assert!(!closure_health.live);
    assert!(closure_health.record.terminal_observed);
    assert_eq!(
        closure_health.record.snapshot.unavailable_reason,
        Some(
            threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed
        )
    );
    assert_eq!(
        closure_health.record.snapshot.first_failure,
        live_closure_health.snapshot.first_failure
    );
    assert_eq!(
        completion
            .original_worker_provider_health
            .get("task-corr-exec"),
        Some(original_health)
    );
    assert_eq!(
        fixture
            .authority
            .get_for_operator(&approval_id)
            .expect("unchanged approval after closure"),
        approval_before
    );
    assert_eq!(
        threadlane_native::provider_health::controller_health_views(&runtime_final, None)
            .iter()
            .filter(|view| view.live)
            .count(),
        0
    );

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_accepts_pinned_codex_reviewer_without_runtime_telemetry_and_completes_derived_go_attempt(
) {
    let fixture = setup_fixture();
    let (reviewer_choice, reviewer_state_dir) = configure_codex_reviewer(&fixture);
    write_codex_reviewer_override(
        &reviewer_state_dir,
        "task-corr-codex-rev",
        1,
        serde_json::json!({
            "outcome": "correct_in_scope",
            "findings": ["src/a.rs::a() returns 100 instead of 101"],
            "required_in_scope_fixes": ["Update src/a.rs::a() so it returns 101"],
            "summary": "Attempt 1 requires a bounded in-scope src/a.rs correction."
        }),
    );
    let task_id = "task-corr-codex-rev";
    let PreparedCorrectionFixtureTask {
        job,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1,
        review_1,
        task_manifest,
        prep_outcome,
        receipt,
        expected_req_key,
        expected_eff_id,
        expected_report_id,
        ..
    } = Box::pin(prepare_task_for_derived_correction_with_reviewer(
        &fixture,
        "brf_corr_codex_rev",
        "plan-corr-codex-rev",
        task_id,
        vec!["Update src/a.rs::a() so it returns 101".to_string()],
        reviewer_choice.clone(),
    ))
    .await;

    // A Codex Reviewer report has no runtime model/effort telemetry; preparation must accept the
    // verified pinned launch configuration instead of demanding fabricated observations.
    assert_eq!(
        prep_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert_eq!(prep_outcome.next_attempt_no, 2);
    assert_eq!(prep_outcome.task_stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(prep_outcome.receipt.as_ref(), Some(&receipt));
    assert!(prep_outcome.outbox_id.is_some());
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(
        read_invocation_count(&reviewer_state_dir.join("agy_reviewer_count.txt")),
        1
    );

    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let prep_req = CoworkerTaskCorrectionPrepareRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: task_id.to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 250,
    };
    let reopened_prep = correction_service
        .prepare_task_correction(session.guard(), &prep_req)
        .await
        .expect("repeat correction preparation must reopen");
    assert_eq!(
        reopened_prep.status,
        CoworkerTaskCorrectionPrepareStatus::Reopened
    );
    assert!(reopened_prep.reused_existing);
    assert_eq!(reopened_prep.receipt.as_ref(), Some(&receipt));
    assert_eq!(reopened_prep.outbox_id, prep_outcome.outbox_id);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: task_id.to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };
    let exec_outcome =
        Box::pin(correction_exec_service.execute_task_correction(session.guard(), &exec_req))
            .await
            .expect("execute derived Go correction attempt");
    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(exec_outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert_eq!(exec_outcome.attempt_no, 2);
    assert!(!exec_outcome.reused_existing);
    assert_eq!(exec_outcome.receipt.as_ref(), Some(&receipt));
    assert_eq!(
        exec_outcome.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(
        exec_outcome.effect_id.as_deref(),
        Some(expected_eff_id.as_str())
    );
    assert_eq!(exec_outcome.outbox_id, prep_outcome.outbox_id);
    assert_eq!(
        exec_outcome.report_artifact_id.as_deref(),
        Some(expected_report_id.as_str())
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        2,
        "exactly one derived Go Coder attempt must run"
    );
    assert_eq!(
        read_invocation_count(&reviewer_state_dir.join("agy_reviewer_count.txt")),
        1,
        "preparing/executing a correction must not call the Reviewer"
    );

    let repeat_exec = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 350,
            ..exec_req.clone()
        },
    ))
    .await
    .expect("repeat correction execution must reopen");
    assert!(repeat_exec.reused_existing);
    assert_eq!(repeat_exec.report, exec_outcome.report);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_2 = Box::pin(verifier.verify_and_prepare_task_attempt(
        session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: task_id.to_string(),
            attempt_no: 2,
            now_ms: step_ts + 400,
        },
    ))
    .await
    .expect("verify derived attempt 2");
    assert_eq!(
        verify_2.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    assert_ne!(verify_2.candidate_id, verify_1.candidate_id);
    assert_ne!(
        verify_2.committed_head_commit,
        verify_1.committed_head_commit
    );
    let review_subject_2 = verify_2
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject attempt 2");
    assert_eq!(review_subject_2.base_commit, fixture.base_commit);
    assert_eq!(review_subject_2.run_id, task_manifest.run_id);
    assert_eq!(review_subject_2.task_id, review_subject_1.task_id);

    write_codex_reviewer_override(
        &reviewer_state_dir,
        task_id,
        2,
        serde_json::json!({
            "outcome": "accept",
            "findings": [],
            "required_in_scope_fixes": [],
            "conflicting_paths_or_interfaces": [],
            "summary": "Derived attempt 2 satisfies the bounded correction and passing checks."
        }),
    );

    let review_service = CoworkerTaskReviewStageService::new(fixture.controller.clone());
    let review_2 = review_service
        .review_task_attempt(
            session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 2,
                now_ms: step_ts + 500,
                untrusted_candidate_context: Some(
                    "Updated src/a.rs::a() to return 101".to_string(),
                ),
            },
        )
        .await
        .expect("Codex Reviewer accepts derived attempt 2");
    assert_eq!(review_2.status, CoworkerTaskReviewStageStatus::Accepted);
    assert_eq!(review_2.task_stage, CoworkerTaskStage::Accepted);
    assert_eq!(review_2.candidate_id, verify_2.candidate_id);
    assert_eq!(review_2.report.provider, WorkerProvider::CodexCli);
    assert_eq!(review_2.report.observed_model, None);
    assert_eq!(review_2.report.observed_effort, None);
    assert!(review_2
        .report
        .execution()
        .has_pinned_codex_read_only_launch_config());
    let review_receipt = review_2
        .review_receipt
        .as_ref()
        .expect("real ReviewReceipt from Codex Accept");
    assert_eq!(
        review_receipt.subject_digest,
        review_subject_2.subject_digest
    );
    assert_eq!(
        review_receipt.task_head_commit,
        verify_2.committed_head_commit
    );
    assert_eq!(
        read_invocation_count(&reviewer_state_dir.join("agy_reviewer_count.txt")),
        2
    );

    let int_service = CoworkerTaskIntegrationService::new(fixture.controller.clone());
    let int_outcome = int_service
        .integrate_accepted_task(
            session.guard(),
            &CoworkerTaskIntegrationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: task_id.to_string(),
                attempt_no: 2,
                now_ms: step_ts + 600,
            },
        )
        .expect("integrate Codex-accepted derived attempt");
    assert_eq!(
        int_outcome.status,
        CoworkerTaskIntegrationStatus::Integrated
    );
    assert_eq!(int_outcome.task_stage, CoworkerTaskStage::Integrated);
    assert_eq!(int_outcome.review_receipt_id, review_receipt.receipt_id);
    assert_eq!(
        int_outcome.review_subject_digest,
        review_subject_2.subject_digest
    );
    assert_eq!(
        int_outcome.integrated_target_commit,
        verify_2.committed_head_commit
    );

    let loaded_review_receipt =
        load_review_receipt(host_handle.work_area_dir(), &review_receipt.receipt_id)
            .expect("load durable ReviewReceipt");
    assert_eq!(&loaded_review_receipt, review_receipt);
    let loaded_integration_receipt = load_integration_receipt(
        host_handle.work_area_dir(),
        &int_outcome.integration_receipt_id,
    )
    .expect("load durable IntegrationReceipt");
    assert_eq!(loaded_integration_receipt, int_outcome.integration_receipt);

    let runtime_final = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read final correction runtime")
        .expect("final runtime exists");
    assert_eq!(runtime_final.total_task_attempts_reserved, 2);
    assert_eq!(runtime_final.total_corrections_reserved, 1);
    assert_eq!(runtime_final.derived_attempts, vec![receipt]);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);
    assert_eq!(
        read_invocation_count(&reviewer_state_dir.join("agy_reviewer_count.txt")),
        2
    );
    assert_eq!(
        fs::read_to_string(
            int_outcome
                .integration_receipt
                .integration_checkout_path
                .join("src/a.rs")
        )
        .expect("read integrated src/a.rs"),
        "pub fn a() -> u32 { 101 }\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_dir.join("src/a.rs")).expect("read primary src/a.rs"),
        "pub fn a() -> u32 { 1 }\n"
    );
    assert_eq!(
        review_1.report.requested_model.base_model,
        reviewer_choice.model_arg
    );
    assert_eq!(
        review_1.report.requested_model.effort,
        reviewer_choice.effort
    );

    session.release().expect("release controller session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_codex_reviewer_rejects_tampered_identity_and_launch_evidence_without_new_reservation(
) {
    let fixture = setup_fixture();
    let (reviewer_choice, reviewer_state_dir) = configure_codex_reviewer(&fixture);
    let task_id = "task-corr-codex-neg";
    write_codex_reviewer_override(
        &reviewer_state_dir,
        task_id,
        1,
        serde_json::json!({
            "outcome": "correct_in_scope",
            "findings": ["src/a.rs::a() returns 100 instead of 101"],
            "required_in_scope_fixes": ["Update src/a.rs::a() so it returns 101"],
            "summary": "Attempt 1 requires a bounded in-scope src/a.rs correction."
        }),
    );
    let PreparedCorrectionFixtureTask {
        job,
        host_handle,
        session,
        step_ts,
        review_1,
        prep_outcome,
        receipt,
        ..
    } = Box::pin(prepare_task_for_derived_correction_with_reviewer(
        &fixture,
        "brf_corr_codex_neg",
        "plan-corr-codex-neg",
        task_id,
        vec!["Update src/a.rs::a() so it returns 101".to_string()],
        reviewer_choice,
    ))
    .await;
    assert_eq!(
        prep_outcome.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert_eq!(prep_outcome.receipt.as_ref(), Some(&receipt));

    // Reviewer report verification runs before the idempotent reopen path. Each tampered report
    // must therefore be refused without minting another reservation or invoking any provider.
    let report_path = PathBuf::from(&review_1.report_path);
    let valid_report_bytes = fs::read(&report_path).expect("read valid Codex Reviewer report");
    let base_report = review_1.report.clone();
    let correction_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let baseline_ledger = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read baseline ledger");
    let baseline_outbox_count = baseline_ledger
        .jobs
        .iter()
        .find(|record| record.job_id == job.job_id)
        .expect("baseline job")
        .outbox
        .len();
    let baseline_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read baseline runtime")
        .expect("baseline runtime exists");
    let baseline_effect_count = baseline_runtime.pending_effects.len();
    let assert_no_new_reservation = || {
        let runtime = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .expect("read runtime after rejected tamper")
            .expect("runtime exists");
        assert_eq!(runtime.total_task_attempts_reserved, 2);
        assert_eq!(runtime.total_corrections_reserved, 1);
        assert_eq!(runtime.derived_attempts, vec![receipt.clone()]);
        assert_eq!(
            runtime.pending_effects.len(),
            baseline_effect_count,
            "rejected tamper must not persist a new effect"
        );
        let ledger = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger after rejected tamper");
        let current_job = ledger
            .jobs
            .iter()
            .find(|record| record.job_id == job.job_id)
            .expect("job after rejected tamper");
        assert_eq!(
            current_job.outbox.len(),
            baseline_outbox_count,
            "rejected tamper must not persist a new outbox record"
        );
        assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
        assert_eq!(
            read_invocation_count(&reviewer_state_dir.join("agy_reviewer_count.txt")),
            1
        );
    };

    let mut wrong_outer_provider = base_report.clone();
    wrong_outer_provider.provider = WorkerProvider::Antigravity;
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 300,
        &report_path,
        &wrong_outer_provider,
    )
    .await;
    assert_no_new_reservation();

    let mut wrong_outer_model = base_report.clone();
    wrong_outer_model.requested_model.base_model = "gpt-6-other".to_string();
    wrong_outer_model.requested_model.cli_model_arg = "gpt-6-other".to_string();
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 350,
        &report_path,
        &wrong_outer_model,
    )
    .await;
    assert_no_new_reservation();

    let mut wrong_outer_effort = base_report.clone();
    wrong_outer_effort.requested_model.effort = Some("max".to_string());
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 400,
        &report_path,
        &wrong_outer_effort,
    )
    .await;
    assert_no_new_reservation();

    let mut wrong_embedded_provider = base_report.clone();
    wrong_embedded_provider
        .structured_execution
        .execution
        .provider = WorkerProvider::Antigravity;
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 450,
        &report_path,
        &wrong_embedded_provider,
    )
    .await;
    assert_no_new_reservation();

    let mut wrong_embedded_model = base_report.clone();
    wrong_embedded_model
        .structured_execution
        .execution
        .requested_model
        .base_model = "gpt-6-other".to_string();
    wrong_embedded_model
        .structured_execution
        .execution
        .requested_model
        .cli_model_arg = "gpt-6-other".to_string();
    replace_argv_arg(
        &mut wrong_embedded_model
            .structured_execution
            .execution
            .constructed_argv,
        "gpt-6.1-sol",
        "gpt-6-other",
    );
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 500,
        &report_path,
        &wrong_embedded_model,
    )
    .await;
    assert_no_new_reservation();

    let mut wrong_embedded_effort = base_report.clone();
    wrong_embedded_effort
        .structured_execution
        .execution
        .requested_model
        .effort = Some("max".to_string());
    replace_argv_arg(
        &mut wrong_embedded_effort
            .structured_execution
            .execution
            .constructed_argv,
        "model_reasoning_effort=\"high\"",
        "model_reasoning_effort=\"max\"",
    );
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 550,
        &report_path,
        &wrong_embedded_effort,
    )
    .await;
    assert_no_new_reservation();

    let mut changed_read_only_argv = base_report.clone();
    replace_argv_arg(
        &mut changed_read_only_argv
            .structured_execution
            .execution
            .constructed_argv,
        "approval_policy=\"never\"",
        "approval_policy=\"on-request\"",
    );
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 600,
        &report_path,
        &changed_read_only_argv,
    )
    .await;
    assert_no_new_reservation();

    // Stream telemetry never substitutes for the pinned read-only Codex launch configuration.
    let mut unpinned_stream_telemetry = base_report.clone();
    let unpinned_execution = &mut unpinned_stream_telemetry.structured_execution.execution;
    unpinned_execution.constructed_argv = vec!["--no-daemon".to_string(), "exec".to_string()];
    unpinned_execution.evidence.model_evidence = threadlane_workers::FieldEvidence::VerifiedMatch {
        value: "gpt-6.1-sol".to_string(),
    };
    unpinned_execution.evidence.effort_evidence =
        threadlane_workers::FieldEvidence::VerifiedMatch {
            value: "high".to_string(),
        };
    unpinned_stream_telemetry.observed_model = Some("gpt-6.1-sol".to_string());
    unpinned_stream_telemetry.observed_effort = Some("high".to_string());
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 625,
        &report_path,
        &unpinned_stream_telemetry,
    )
    .await;
    assert_no_new_reservation();

    let mut contradictory_observations = base_report.clone();
    contradictory_observations.observed_model =
        Some(base_report.requested_model.base_model.clone());
    contradictory_observations.observed_effort = Some("high".to_string());
    assert_codex_reviewer_report_rejected(
        &correction_service,
        &session,
        &job,
        task_id,
        step_ts + 650,
        &report_path,
        &contradictory_observations,
    )
    .await;
    assert_no_new_reservation();

    fs::write(&report_path, &valid_report_bytes).expect("restore valid Codex Reviewer report");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 restored Codex Reviewer report");

    session.release().expect("release controller session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_held_provider_rejects_duplicate_claim_without_marking_unknown_and_observes_stop_cancellation(
) {
    let fixture = setup_fixture();
    let required_fixes = vec![
        "HOLD_CORR_PROVIDER Hold correction provider process until stop_requested".to_string(),
        "Keep src/a.rs in-scope fix bounded".to_string(),
    ];
    let PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before: _,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1,
        policy_digest: _,
        review_1,
        art_key_1,
        art_1_before,
        task_manifest,
        prep_outcome,
        receipt,
        expected_req_key,
        expected_eff_id,
        expected_report_id,
    } = prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_held",
        "plan-corr-held",
        "task-corr-held",
        required_fixes,
    )
    .await;

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-held".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };

    let guard = session.guard();
    let primary_exec_fut = correction_exec_service.execute_task_correction(guard, &exec_req);

    let duplicate_and_cancel_fut = async {
        // Wait until the fake AGY provider process for derived attempt 2 has actually started.
        let start_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if read_invocation_count(&fixture.agy_count_file) == 2 {
                break;
            }
            assert!(
                Instant::now() < start_deadline,
                "timed out waiting for held derived Coder process to start"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let live_health = wait_for_owned_role_health(
            &fixture,
            guard,
            &job.job_id,
            WorkflowRole::Coder,
            Some("task-corr-held"),
            Some(2),
        )
        .await;
        assert!(!live_health.terminal_observed);
        assert_eq!(live_health.invocation.key(), expected_eff_id);

        // Verify the task is currently Running on attempt 2 with a single reservation.
        let ledger_running = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger while derived Coder is held");
        let job_running = ledger_running
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("job while running");
        let task_running = job_running
            .tasks
            .iter()
            .find(|t| t.task_id == "task-corr-held")
            .expect("task while running");
        assert_eq!(task_running.stage, CoworkerTaskStage::Running);
        assert_eq!(task_running.current_attempt_no, 2);

        // Issue a duplicate execute_task_correction call while the provider process is held.
        let dup_err = correction_exec_service
            .execute_task_correction(
                guard,
                &CoworkerTaskCorrectionExecuteRequest {
                    now_ms: step_ts + 350,
                    ..exec_req.clone()
                },
            )
            .await
            .expect_err("duplicate in-flight execute_task_correction must be refused");
        assert_eq!(
            dup_err.code(),
            NativeErrorCode::AmbiguousWakeupPendingReconciliation.as_str()
        );
        assert_eq!(
            read_invocation_count(&fixture.agy_count_file),
            2,
            "duplicate call must not launch a second correction process"
        );

        // Verify the duplicate call did not mark the legitimate running effect or task EffectUnknown
        // and did not charge a second reservation.
        let rt_during_hold = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, &job.job_id)
            .expect("read runtime after duplicate call")
            .expect("runtime exists");
        let coder_usage_hold = rt_during_hold
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage during hold");
        assert_eq!(coder_usage_hold.calls_reserved, 2);
        assert_eq!(coder_usage_hold.calls_completed, 1);
        assert_eq!(rt_during_hold.total_task_attempts_reserved, 2);
        assert_eq!(rt_during_hold.total_corrections_reserved, 1);
        let eff_during_hold = rt_during_hold
            .pending_effects
            .get(&expected_eff_id)
            .expect("running CoderDispatch effect");
        assert_eq!(
            eff_during_hold.status,
            PendingControllerEffectStatus::ReservedBeforeEffect,
            "duplicate call must not mark the legitimate running effect EffectUnknown"
        );

        let ledger_after_dup = fixture
            .workflow_store
            .read_binding_ledger(BINDING_A)
            .expect("read ledger after duplicate call");
        let task_after_dup = ledger_after_dup
            .jobs
            .iter()
            .find(|j| j.job_id == job.job_id)
            .expect("job after duplicate")
            .tasks
            .iter()
            .find(|t| t.task_id == "task-corr-held")
            .expect("task after duplicate");
        assert_eq!(task_after_dup.stage, CoworkerTaskStage::Running);

        // Set the real durable stop_requested flag while the provider is held.
        fixture
            .policy_store
            .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
                state.stop_requested = true;
                Ok(())
            })
            .expect("set durable stop_requested while provider is held");
    };

    let (primary_res, ()) = tokio::join!(primary_exec_fut, duplicate_and_cancel_fut);
    let cancelled_outcome = primary_res.expect("held correction completes with CancelledTerminal");

    assert_eq!(
        cancelled_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::CancelledTerminal
    );
    assert_eq!(cancelled_outcome.task_stage, CoworkerTaskStage::Cancelled);
    assert_ne!(
        cancelled_outcome.task_stage,
        CoworkerTaskStage::ReadyForReview
    );
    assert_eq!(cancelled_outcome.prior_attempt_no, 1);
    assert_eq!(cancelled_outcome.attempt_no, 2);
    assert!(!cancelled_outcome.reused_existing);
    assert_eq!(cancelled_outcome.prior_candidate_id, verify_1.candidate_id);
    assert_eq!(
        cancelled_outcome.prior_review_verdict_id.as_deref(),
        Some(review_1.verdict.verdict_id.as_str())
    );
    assert_eq!(
        cancelled_outcome.prior_review_subject_digest,
        review_subject_1.subject_digest
    );
    assert_eq!(
        cancelled_outcome.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(
        cancelled_outcome.effect_id.as_deref(),
        Some(expected_eff_id.as_str())
    );
    assert_eq!(cancelled_outcome.outbox_id, prep_outcome.outbox_id);
    assert_eq!(
        cancelled_outcome.report_artifact_id.as_deref(),
        Some(expected_report_id.as_str())
    );
    assert_eq!(cancelled_outcome.receipt.as_ref(), Some(&receipt));
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let expected_report_path =
        derived_execution_report_file_path(&fixture.plans_dir, &expected_eff_id);
    let report_meta =
        fs::symlink_metadata(&expected_report_path).expect("cancelled report file metadata");
    assert!(report_meta.file_type().is_file());
    assert_eq!(report_meta.permissions().mode() & 0o777, 0o600);

    let cancelled_report = cancelled_outcome
        .report
        .as_ref()
        .expect("PersistedDerivedTaskExecutionArtifact for cancelled run");
    cancelled_report
        .validate_integrity()
        .expect("validate cancelled PersistedDerivedTaskExecutionArtifact integrity");
    assert_eq!(
        cancelled_report
            .compute_digest()
            .expect("compute cancelled report digest"),
        cancelled_report.artifact_digest
    );
    let persisted_cancelled =
        read_persisted_derived_task_execution_report(&fixture.plans_dir, &expected_eff_id)
            .expect("read persisted cancelled report")
            .expect("persisted cancelled report exists");
    assert_eq!(&persisted_cancelled, cancelled_report);
    assert_eq!(
        cancelled_report.status,
        CoworkerTaskCorrectionExecuteStatus::CancelledTerminal
    );
    assert_eq!(
        cancelled_report.execution_report.worker_report.status,
        WorkerRunStatus::Cancelled
    );
    assert!(
        !cancelled_report
            .execution_report
            .worker_report
            .cleanup_incomplete,
        "supervisor must collect local process-group cleanup on cancellation"
    );
    assert_eq!(cancelled_report.prior_attempt_no, 1);
    assert_eq!(cancelled_report.attempt_no, 2);
    assert_eq!(cancelled_report.original_run_id, art_1_before.run_id);
    assert_eq!(cancelled_report.original_worker_id, art_1_before.worker_id);
    assert_eq!(cancelled_report.manifest_run_id, task_manifest.run_id);
    assert_eq!(
        Some(cancelled_report.manifest_task_id.as_str()),
        task_manifest.task_id.as_deref()
    );
    assert_ne!(
        cancelled_report.original_run_id,
        cancelled_report.manifest_run_id
    );
    assert_ne!(cancelled_report.task_id, cancelled_report.manifest_task_id);

    let rt_after_cancel = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after cancellation")
        .expect("runtime exists");
    let health_after_cancel = rt_after_cancel
        .provider_health
        .get(&expected_eff_id)
        .expect("terminal correction health");
    assert!(health_after_cancel.terminal_observed);
    assert_eq!(
        health_after_cancel.snapshot,
        cancelled_report
            .execution_report
            .worker_report
            .provider_health
            .clone()
            .expect("terminal report health")
    );
    assert!(health_after_cancel.snapshot.current_failure.is_some());
    assert!(health_after_cancel.snapshot.recovery.is_none());
    assert_eq!(
        health_after_cancel.snapshot.unavailable_reason,
        Some(
            threadlane_workers::provider_health::ProviderDiagnosticUnavailableReason::SourceClosed
        )
    );

    let coder_usage_cancel = rt_after_cancel
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder usage after cancellation");
    assert_eq!(coder_usage_cancel.calls_reserved, 2);
    assert_eq!(coder_usage_cancel.calls_completed, 2);
    assert_eq!(rt_after_cancel.total_task_attempts_reserved, 2);
    assert_eq!(rt_after_cancel.total_corrections_reserved, 1);
    assert_eq!(
        rt_after_cancel
            .pending_effects
            .get(&expected_eff_id)
            .expect("cancelled CoderDispatch effect")
            .status,
        PendingControllerEffectStatus::EffectFailedTerminal
    );
    assert!(
        !rt_after_cancel
            .attempt_artifacts
            .contains_key(&ControllerJobRuntimeState::artifact_key(
                "task-corr-held",
                2
            )),
        "cancelled derived attempt must not promote attempt 2 TaskAttemptExecutionArtifacts"
    );
    assert_eq!(
        rt_after_cancel.attempt_artifacts.get(&art_key_1),
        Some(&art_1_before)
    );

    // Another execute_task_correction request while stop_requested is true (and after clearing
    // stop_requested) must not promote to ReadyForReview, charge budgets, or relaunch the provider.
    let while_stopped_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 500,
                ..exec_req.clone()
            },
        )
        .await
        .expect_err("request while stop_requested is true must be refused");
    assert_eq!(
        while_stopped_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.stop_requested = false;
            Ok(())
        })
        .expect("clear stop_requested");

    let reopened_cancelled = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 600,
                ..exec_req
            },
        )
        .await
        .expect("reopen cancelled derived attempt after clearing stop_requested");
    assert_eq!(
        reopened_cancelled.status,
        CoworkerTaskCorrectionExecuteStatus::CancelledTerminal
    );
    assert_eq!(reopened_cancelled.task_stage, CoworkerTaskStage::Cancelled);
    assert!(reopened_cancelled.reused_existing);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let rt_after_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after reopen")
        .expect("runtime exists");
    assert_eq!(
        rt_after_reopen
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        2
    );
    assert_eq!(rt_after_reopen.total_task_attempts_reserved, 2);
    assert_eq!(rt_after_reopen.total_corrections_reserved, 1);

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord");
    assert_eq!(approval_after, approval_before);

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_artifact_recovery_fails_closed_on_corrupt_missing_or_symlink_and_preserves_conflicting_files(
) {
    // Heap-allocate nested futures to bound the poll frame of this stack-pinned test.
    let fixture = setup_fixture();
    let required_fixes = vec![
        "Update src/a.rs::a() so it returns 101 instead of 100".to_string(),
        "Preserve function signature in src/a.rs".to_string(),
    ];
    let PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before: _,
        host_handle,
        session,
        step_ts,
        verify_1: _,
        review_subject_1: _,
        policy_digest: _,
        review_1: _,
        art_key_1: _,
        art_1_before: _,
        task_manifest: _,
        prep_outcome: _,
        receipt: _,
        expected_req_key,
        expected_eff_id,
        expected_report_id: _,
    } = Box::pin(prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_recov",
        "plan-corr-recov",
        "task-corr-recov",
        required_fixes,
    ))
    .await;

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-recov".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };

    let report_path = derived_execution_report_file_path(&fixture.plans_dir, &expected_eff_id);
    let disposable_symlink_target = fixture.plans_dir.join("disposable-symlink-target.json");
    let symlink_target_bytes = b"{\"disposable_symlink_target\":true}";
    fs::write(&disposable_symlink_target, symlink_target_bytes)
        .expect("write disposable symlink target");
    fs::set_permissions(
        &disposable_symlink_target,
        fs::Permissions::from_mode(0o600),
    )
    .expect("chmod 0600 disposable symlink target");

    // 1. Pre-existing symlink at the derived execution report path must be rejected with
    // SecurityViolation without following/overwriting the symlink or launching the provider.
    std::os::unix::fs::symlink(&disposable_symlink_target, &report_path)
        .expect("create pre-existing symlink at report_path");
    let pre_symlink_err =
        Box::pin(correction_exec_service.execute_task_correction(session.guard(), &exec_req))
            .await
            .expect_err("pre-existing symlink at derived report path must fail closed");
    assert_eq!(
        pre_symlink_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        1,
        "pre-existing symlink must not launch a second Coder process"
    );
    assert!(
        fs::symlink_metadata(&report_path)
            .expect("symlink metadata")
            .file_type()
            .is_symlink(),
        "pre-existing symlink must remain intact"
    );
    assert_eq!(
        fs::read(&disposable_symlink_target).expect("read symlink target"),
        symlink_target_bytes
    );
    fs::remove_file(&report_path).expect("remove disposable pre-existing symlink");

    // 2. Pre-existing conflicting regular file at the derived execution report path must fail
    // closed without overwriting the conflicting bytes, launching the provider, or charging budgets.
    let pre_conflict_bytes = b"{\"conflicting\":\"preexisting-derived-report-bytes\"}";
    fs::write(&report_path, pre_conflict_bytes).expect("write pre-existing conflicting file");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 pre-existing conflicting file");
    let pre_conflict_err = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 320,
            ..exec_req.clone()
        },
    ))
    .await
    .expect_err("pre-existing conflicting file at derived report path must fail closed");
    assert_eq!(
        pre_conflict_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(
        fs::read(&report_path).expect("read pre-existing conflicting file after rejection"),
        pre_conflict_bytes,
        "pre-existing conflicting file bytes must never be overwritten"
    );
    let rt_after_pre_conflict = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after pre-conflict")
        .expect("runtime exists");
    assert_eq!(
        rt_after_pre_conflict
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("Coder usage")
            .calls_reserved,
        2
    );
    assert_eq!(rt_after_pre_conflict.total_task_attempts_reserved, 2);
    assert_eq!(rt_after_pre_conflict.total_corrections_reserved, 1);
    fs::remove_file(&report_path).expect("remove disposable pre-existing conflicting file");

    // 3. Execute the real derived correction so the legitimate 0600 report is persisted on disk.
    let exec_outcome = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 350,
            ..exec_req.clone()
        },
    ))
    .await
    .expect("execute_task_correction succeeds once conflicting file is removed");
    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);
    let valid_report_bytes = fs::read(&report_path).expect("read valid persisted report bytes");

    // 3b. Verify PersistedDerivedTaskExecutionArtifact preserves v1 Reviewer digest byte identity,
    // enforces XOR between `prior_review_verdict_id` and `prior_check_failure_basis`, and validates
    // standalone `CheckFailureCorrectionBasis` structural + lineage invariants at the artifact boundary.
    {
        let valid_report = exec_outcome
            .report
            .as_ref()
            .expect("PersistedDerivedTaskExecutionArtifact from execution")
            .clone();
        assert!(valid_report.prior_review_verdict_id.is_some());
        assert!(valid_report.prior_check_failure_basis.is_none());
        let valid_report_str =
            String::from_utf8(valid_report_bytes.clone()).expect("UTF-8 report JSON");
        assert!(!valid_report_str.contains("prior_check_failure_basis"));

        let mut neither_basis_report = valid_report.clone();
        neither_basis_report.prior_review_verdict_id = None;
        neither_basis_report.prior_check_failure_basis = None;
        assert!(neither_basis_report.compute_digest().is_err());
        assert!(neither_basis_report.validate_integrity().is_err());

        let sample_check_ref = CheckFailureReportRef {
            check_id: "cargo_test".to_string(),
            kind: FrozenCheckKind::Tests,
            spec_digest: sha256_hex(b"sample-spec-digest-v1"),
            effect_id: "eff_0123456789abcdef0123456789abcdef".to_string(),
            report_path: fixture
                .plans_dir
                .join("coworker-check-report-eff_0123456789abcdef0123456789abcdef.json")
                .to_string_lossy()
                .to_string(),
            evidence_digest: sha256_hex(b"sample-evidence-digest-v1"),
            status: FrozenCheckExecutionStatus::NonZeroExit,
            exit_code: Some(101),
            signal: None,
            stdout_sha256: sha256_hex(b"sample-stdout-v1"),
            stderr_sha256: sha256_hex(b"sample-stderr-v1"),
            post_worktree_clean: true,
            subject_drifted: false,
            diagnostic_summary: "cargo_test exited with status 101".to_string(),
        };
        let valid_check_basis = CheckFailureCorrectionBasis {
            task_id: valid_report.task_id.clone(),
            task_manifest_id: valid_report.manifest_task_id.clone(),
            prior_attempt_no: valid_report.prior_attempt_no,
            prior_candidate_id: valid_report.prior_candidate_id.clone(),
            execution_base_commit: valid_report.execution_base_commit.clone(),
            committed_head_commit: valid_report.pre_execution_head_commit.clone(),
            committed_tree_sha: valid_report.pre_execution_tree_sha.clone(),
            check_subject_digest: sha256_hex(b"sample-check-subject-digest-v1"),
            prior_review_subject_digest: valid_report.prior_review_subject_digest.clone(),
            failed_checks: vec![sample_check_ref],
            diagnostic_summary: "Blocked by failed required task-review check `cargo_test`"
                .to_string(),
        };

        let mut both_basis_report = valid_report.clone();
        both_basis_report.prior_check_failure_basis = Some(valid_check_basis.clone());
        assert!(both_basis_report.compute_digest().is_err());
        assert!(both_basis_report.validate_integrity().is_err());

        let mut check_basis_report = valid_report.clone();
        check_basis_report.prior_review_verdict_id = None;
        check_basis_report.prior_check_failure_basis = Some(valid_check_basis);
        check_basis_report.artifact_digest = check_basis_report
            .compute_digest()
            .expect("compute v2 check-failure artifact digest");
        assert_ne!(
            check_basis_report.artifact_digest,
            valid_report.artifact_digest
        );
        check_basis_report
            .validate_integrity()
            .expect("valid check-failure artifact passes validate_integrity");

        let mut malformed_basis_report = check_basis_report.clone();
        malformed_basis_report
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .failed_checks[0]
            .status = FrozenCheckExecutionStatus::TimedOut;
        malformed_basis_report.artifact_digest = malformed_basis_report
            .compute_digest()
            .expect("recompute digest on malformed basis");
        assert_eq!(
            malformed_basis_report
                .validate_integrity()
                .expect_err(
                    "malformed check-failure basis in artifact must fail validate_integrity"
                )
                .code(),
            NativeErrorCode::ValidationFailed.as_str()
        );

        let mut mismatched_lineage_report = check_basis_report;
        mismatched_lineage_report
            .prior_check_failure_basis
            .as_mut()
            .expect("basis")
            .task_manifest_id = "w_mismatched_manifest_id".to_string();
        mismatched_lineage_report.artifact_digest = mismatched_lineage_report
            .compute_digest()
            .expect("recompute digest on lineage-mismatched basis");
        assert_eq!(
            mismatched_lineage_report
                .validate_integrity()
                .expect_err("lineage-mismatched check-failure basis must fail validate_integrity")
                .code(),
            NativeErrorCode::ProtocolError.as_str()
        );
    }

    // 4. Corrupt the persisted report on disk with conflicting bytes: repeated execution and
    // verification requests must fail closed, preserve the conflicting bytes, and retain budgets
    // without another provider call or a new request key.
    let corrupt_bytes = b"{\"corrupt_persisted_derived_report\":true}";
    fs::write(&report_path, corrupt_bytes).expect("write corrupt bytes to report_path");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt report_path");

    let repeat_corrupt_err = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 400,
            ..exec_req.clone()
        },
    ))
    .await
    .expect_err("repeated execution with corrupt persisted report must fail closed");
    assert_eq!(
        repeat_corrupt_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(
        fs::read(&report_path).expect("read corrupt report after repeat execution"),
        corrupt_bytes,
        "corrupt report bytes must be preserved untouched"
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let verify_corrupt_err = Box::pin(verifier.verify_and_prepare_task_attempt(
        session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: "task-corr-recov".to_string(),
            attempt_no: 2,
            now_ms: step_ts + 450,
        },
    ))
    .await
    .expect_err("verification with corrupt persisted report must fail closed");
    assert_eq!(
        verify_corrupt_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(
        fs::read(&report_path).expect("read corrupt report after verification"),
        corrupt_bytes
    );

    // 5. Tamper with a field inside the persisted JSON report without updating artifact_digest:
    // both execution reopen and verification must fail closed and preserve the tampered bytes.
    let mut tampered_json: serde_json::Value =
        serde_json::from_slice(&valid_report_bytes).expect("parse valid report JSON");
    tampered_json["worktree_change_set_digest"] = serde_json::Value::String("f".repeat(64));
    let tampered_bytes =
        serde_json::to_vec_pretty(&tampered_json).expect("serialize tampered report JSON");
    fs::write(&report_path, &tampered_bytes).expect("write tampered report JSON");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 tampered report");

    let repeat_tampered_err = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 500,
            ..exec_req.clone()
        },
    ))
    .await
    .expect_err("repeated execution with tampered report digest must fail closed");
    assert_eq!(
        repeat_tampered_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(
        fs::read(&report_path).expect("read tampered report after execution"),
        tampered_bytes
    );

    let verify_tampered_err = Box::pin(verifier.verify_and_prepare_task_attempt(
        session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: "task-corr-recov".to_string(),
            attempt_no: 2,
            now_ms: step_ts + 550,
        },
    ))
    .await
    .expect_err("verification with tampered report digest must fail closed");
    assert_eq!(
        verify_tampered_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );
    assert_eq!(
        fs::read(&report_path).expect("read tampered report after verification"),
        tampered_bytes
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    // 6. Replace the persisted report with a symlink: both execution reopen and verification must
    // fail closed with SecurityViolation and preserve the symlink target.
    fs::remove_file(&report_path).expect("remove tampered report before symlink check");
    std::os::unix::fs::symlink(&disposable_symlink_target, &report_path)
        .expect("create symlink at report_path");

    let repeat_symlink_err = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 600,
            ..exec_req.clone()
        },
    ))
    .await
    .expect_err("repeated execution with symlink report must fail closed");
    assert_eq!(
        repeat_symlink_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
    let verify_symlink_err = Box::pin(verifier.verify_and_prepare_task_attempt(
        session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: "task-corr-recov".to_string(),
            attempt_no: 2,
            now_ms: step_ts + 650,
        },
    ))
    .await
    .expect_err("verification with symlink report must fail closed");
    assert_eq!(
        verify_symlink_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
    assert_eq!(
        fs::read(&disposable_symlink_target).expect("read symlink target after rejection"),
        symlink_target_bytes
    );

    // 7. Remove the persisted report completely: repeated execution and verification must fail
    // closed without launching another provider process, minting a new key, or charging budgets.
    fs::remove_file(&report_path).expect("remove symlink so report_path is missing");
    fs::remove_file(&disposable_symlink_target).expect("remove disposable symlink target");

    let repeat_missing_err = Box::pin(correction_exec_service.execute_task_correction(
        session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 700,
            ..exec_req
        },
    ))
    .await
    .expect_err("repeated execution with missing persisted report must fail closed");
    assert_eq!(
        repeat_missing_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );

    let verify_missing_err = Box::pin(verifier.verify_and_prepare_task_attempt(
        session.guard(),
        &CoworkerTaskVerificationRequest {
            binding_id: BINDING_A.to_string(),
            thread_uuid: THREAD_UUID_A.to_string(),
            job_id: job.job_id.clone(),
            task_id: "task-corr-recov".to_string(),
            attempt_no: 2,
            now_ms: step_ts + 750,
        },
    ))
    .await
    .expect_err("verification with missing persisted report must fail closed");
    assert_eq!(
        verify_missing_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    assert_eq!(
        read_invocation_count(&fixture.agy_count_file),
        2,
        "missing/corrupt report recovery checks must never launch another Coder process"
    );

    let rt_final = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read final runtime")
        .expect("runtime exists");
    let coder_usage_final = rt_final
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder usage");
    assert_eq!(coder_usage_final.calls_reserved, 2);
    assert_eq!(coder_usage_final.calls_completed, 2);
    assert_eq!(rt_final.total_task_attempts_reserved, 2);
    assert_eq!(rt_final.total_corrections_reserved, 1);
    assert_eq!(rt_final.derived_attempts.len(), 1);
    let coder_effects: Vec<_> = rt_final
        .pending_effects
        .values()
        .filter(|e| e.kind == PendingControllerEffectKind::CoderDispatch && e.attempt_no == Some(2))
        .collect();
    assert_eq!(coder_effects.len(), 1);
    assert_eq!(coder_effects[0].effect_id, expected_eff_id);
    assert_eq!(coder_effects[0].request_key, expected_req_key);

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord");
    assert_eq!(approval_after, approval_before);

    session.release().expect("release session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_large_persisted_report_exceeds_one_mib_while_policy_ledger_stays_bounded_reopens_and_verifies(
) {
    let fixture = setup_fixture();
    let required_fixes = vec![
        "Update src/a.rs::a() so it returns 101 instead of 100".to_string(),
        "Emit full structured progress stream during bounded correction".to_string(),
    ];
    let default_budgets = ExecutionBudgets::default();
    let PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before: _,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1,
        policy_digest: _,
        review_1: _,
        art_key_1,
        art_1_before,
        task_manifest,
        prep_outcome: _,
        receipt,
        expected_req_key,
        expected_eff_id,
        expected_report_id,
    } = prepare_task_for_derived_correction_with_budgets(
        &fixture,
        "brf_corr_large",
        "plan-corr-large",
        "task-corr-large",
        required_fixes,
        ExecutionBudgets {
            max_events: threadlane_workers::MAX_EVENTS,
            ..default_budgets.clone()
        },
    )
    .await;

    // Configure only this disposable test's fake executable before the derived attempt so the
    // actual derived Coder process generates a ~1.4 MiB valid NDJSON event stream (2,402 lines,
    // each line ~615 bytes << max_line_bytes = 256 KiB, total stdout < default 4 MiB budget)
    // and writes valid single-brace Rust contents `pub fn a() -> u32 { 101 }\n` to src/a.rs.
    let expected_summary_chunk = format!(
        "derived_coder_large_report_progress_payload_chunk_{}",
        "x".repeat(190)
    );
    let padding_chunk = "p".repeat(250);
    let large_corr_header = format!(
        r#"#!/bin/sh
set -eu
CODER_COUNT_FILE="{coder_count_file}"
PLANNING_COUNT_FILE="{planning_count_file}"
PROMPT_FILE="{prompt_file}"
PLAN_RESPONSE_FILE="{plan_response_file}"
SUMMARY_CHUNK="{expected_summary_chunk}"
PADDING_CHUNK="{padding_chunk}"
"#,
        coder_count_file = fixture.agy_count_file.display(),
        planning_count_file = fixture.agy_planning_count_file.display(),
        prompt_file = fixture.agy_prompt_file.display(),
        plan_response_file = fixture.agy_plan_response_file.display(),
    );
    let large_corr_body = r#"
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
    printf '{"event":"init","init":{"session_id":"sess-agy-plan-%s","model":"%s"}}\n' "$pc" "$MODEL"
    esc=$(sed 's/\\/\\\\/g; s/"/\\"/g' "$PLAN_RESPONSE_FILE" | tr '\n' ' ')
    printf '{"event":"result","result":{"status":"SUCCESS","response":"%s"}}\n' "$esc"
    exit 0
fi

c=0
if [ -f "$CODER_COUNT_FILE" ]; then
    c=$(cat "$CODER_COUNT_FILE")
fi
c=$((c + 1))
printf "%s" "$c" > "$CODER_COUNT_FILE"
printf "%s" "$PROMPT" > "$PROMPT_FILE"
printf '{"type":"init","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"

if ! grep -q "100" "src/a.rs"; then
    echo "expected prior attempt 1 contents in src/a.rs before large-report correction" >&2
    exit 3
fi
printf "pub fn a() -> u32 { 101 }\n" > "src/a.rs"

nl='
'
line=$(printf '{"type":"assistant","subtype":"progress_chunk_detail_stream_item","session_id":"sess-agy-ctrl","model":"%s","effort":"%s","summary":"%s","detail_padding":"%s"}' "$MODEL" "$EFFORT" "$SUMMARY_CHUNK" "$PADDING_CHUNK")
blk10="${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}${line}${nl}"
i=0
while [ "$i" -lt 240 ]; do
    printf "%s" "$blk10"
    i=$((i + 1))
done

printf '{"type":"result","subtype":"success","session_id":"sess-agy-ctrl","model":"%s","effort":"%s"}\n' "$MODEL" "$EFFORT"
exit 0
"#;
    write_executable_script(
        &fixture.fake_agy_exe,
        &format!("{large_corr_header}{large_corr_body}"),
    );

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-large".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };
    let exec_outcome = correction_exec_service
        .execute_task_correction(session.guard(), &exec_req)
        .await
        .expect("execute_task_correction with >1MiB persisted execution report");

    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(exec_outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert_eq!(exec_outcome.attempt_no, 2);
    assert!(!exec_outcome.reused_existing);
    assert_eq!(
        exec_outcome.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(
        exec_outcome.effect_id.as_deref(),
        Some(expected_eff_id.as_str())
    );
    assert_eq!(
        exec_outcome.report_artifact_id.as_deref(),
        Some(expected_report_id.as_str())
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let report = exec_outcome
        .report
        .as_ref()
        .expect("PersistedDerivedTaskExecutionArtifact present");
    report
        .validate_integrity()
        .expect("validate large PersistedDerivedTaskExecutionArtifact integrity");
    assert_eq!(
        report.execution_report.worker_report.status,
        WorkerRunStatus::Succeeded
    );
    assert_eq!(report.execution_report.worker_report.overflow, None);
    assert!(report
        .execution_report
        .worker_report
        .has_complete_natural_completion_evidence());
    assert!(
        report.execution_report.worker_report.stdout_bytes_captured >= 1_258_291,
        "expected >= 1.2 MiB captured stdout from real derived Coder process, got {}",
        report.execution_report.worker_report.stdout_bytes_captured
    );
    assert!(
        report.execution_report.worker_report.stdout_bytes_captured
            < default_budgets.max_stdout_bytes
    );
    assert_eq!(
        report.execution_report.worker_report.evidence.total_lines,
        2_402
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .valid_json_events,
        2_402
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .malformed_lines_count,
        0
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .retained_events
            .len(),
        2_402
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .dropped_events_count,
        0
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .retained_events[1]
            .summary
            .as_deref(),
        Some(expected_summary_chunk.as_str())
    );
    assert_eq!(
        report
            .execution_report
            .worker_report
            .evidence
            .retained_events[2_400]
            .summary
            .as_deref(),
        Some(expected_summary_chunk.as_str())
    );

    // Persisted private 0600 report artifact exceeds 1 MiB on disk while controller policy and
    // workflow ledgers remain compact and well below the 1 MiB ledger bound.
    let one_mib: u64 = 1_048_576;
    let expected_report_path =
        derived_execution_report_file_path(&fixture.plans_dir, &expected_eff_id);
    let report_meta =
        fs::symlink_metadata(&expected_report_path).expect("large derived report metadata");
    assert!(report_meta.file_type().is_file());
    assert_eq!(report_meta.permissions().mode() & 0o777, 0o600);
    assert!(
        report_meta.len() > one_mib,
        "expected persisted derived execution report file > 1 MiB ({one_mib} bytes), got {} bytes",
        report_meta.len()
    );

    let policy_ledger_path = fixture
        .policy_store
        .ledger_path_for_binding(BINDING_A)
        .expect("policy ledger path");
    let policy_ledger_meta =
        fs::symlink_metadata(&policy_ledger_path).expect("policy ledger metadata");
    assert!(
        policy_ledger_meta.len() < one_mib && policy_ledger_meta.len() < 128 * 1024,
        "controller policy ledger must remain well within its 1 MiB bound, got {} bytes",
        policy_ledger_meta.len()
    );
    let workflow_ledger_path = fixture
        .workflow_store
        .ledger_path_for_binding(BINDING_A)
        .expect("workflow ledger path");
    let workflow_ledger_meta =
        fs::symlink_metadata(&workflow_ledger_path).expect("workflow ledger metadata");
    assert!(
        workflow_ledger_meta.len() < one_mib && workflow_ledger_meta.len() < 128 * 1024,
        "workflow binding ledger must remain well within its 1 MiB bound, got {} bytes",
        workflow_ledger_meta.len()
    );

    // Reopen session and verify no new provider invocation, no duplicate budget charge, and full
    // >1MiB output retained through the real immutable report loader.
    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 400)
        .expect("reopen session");
    let reopened_exec = correction_exec_service
        .execute_task_correction(
            reopened_session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 500,
                ..exec_req
            },
        )
        .await
        .expect("reopen execute_task_correction with >1MiB report");
    assert_eq!(
        reopened_exec.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert!(reopened_exec.reused_existing);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let loaded_report =
        read_persisted_derived_task_execution_report(&fixture.plans_dir, &expected_eff_id)
            .expect("load >1MiB persisted derived report via immutable loader")
            .expect("persisted derived report exists");
    assert_eq!(&loaded_report, report);
    assert_eq!(
        reopened_exec.report.as_ref(),
        Some(&loaded_report),
        "reopen must return the full persisted >1MiB report"
    );

    let rt_after_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after reopen")
        .expect("runtime exists");
    let coder_usage = rt_after_reopen
        .role_usage
        .get(&WorkflowRole::Coder)
        .expect("Coder usage");
    assert_eq!(coder_usage.calls_reserved, 2);
    assert_eq!(coder_usage.calls_completed, 2);
    assert_eq!(rt_after_reopen.total_task_attempts_reserved, 2);
    assert_eq!(rt_after_reopen.total_corrections_reserved, 1);
    assert_eq!(rt_after_reopen.derived_attempts, vec![receipt]);

    // Verify attempt 2 consumes the actual derived report and worktree (`101`) rather than stale
    // original-host attempt 1 output (`100`).
    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_2 = verifier
        .verify_and_prepare_task_attempt(
            reopened_session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-large".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 600,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt attempt 2 from >1MiB derived report");
    assert_eq!(
        verify_2.status,
        CoworkerTaskVerificationStatus::ReadyForReviewer
    );
    assert_eq!(verify_2.attempt_no, 2);
    assert_ne!(
        verify_2.committed_head_commit,
        verify_1.committed_head_commit
    );
    assert_ne!(verify_2.committed_tree_sha, verify_1.committed_tree_sha);
    assert_ne!(verify_2.candidate_id, verify_1.candidate_id);
    let review_subject_2 = verify_2
        .review_subject
        .as_ref()
        .expect("promoted ReviewSubject for attempt 2");
    assert_ne!(
        review_subject_2.subject_digest,
        review_subject_1.subject_digest
    );
    assert_eq!(
        review_subject_2.task_head_commit,
        verify_2.committed_head_commit
    );
    assert_eq!(review_subject_2.task_tree_sha, verify_2.committed_tree_sha);
    assert_eq!(
        fs::read_to_string(task_manifest.checkout_path.join("src/a.rs"))
            .expect("read verified attempt 2 src/a.rs"),
        "pub fn a() -> u32 { 101 }\n"
    );

    let rt_after_verify = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime after verify")
        .expect("runtime exists");
    let art_key_2 = ControllerJobRuntimeState::artifact_key("task-corr-large", 2);
    assert_eq!(
        rt_after_verify.attempt_artifacts.get(&art_key_1),
        Some(&art_1_before)
    );
    let art_2 = rt_after_verify
        .attempt_artifacts
        .get(&art_key_2)
        .expect("attempt 2 artifacts");
    assert!(art_2.is_derived_attempt);
    assert_eq!(
        art_2.committed_head_commit.as_deref(),
        Some(verify_2.committed_head_commit.as_str())
    );
    assert_eq!(
        art_2.candidate_id.as_deref(),
        Some(verify_2.candidate_id.as_str())
    );

    let policy_ledger_after_verify =
        fs::symlink_metadata(&policy_ledger_path).expect("policy ledger metadata after verify");
    assert!(policy_ledger_after_verify.len() < one_mib);

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord");
    assert_eq!(approval_after, approval_before);

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn task_correction_reconciles_over_64kib_tracked_diff_and_reopens_idempotently() {
    let fixture = setup_fixture();
    let root_dir = fixture.agy_count_file.parent().expect("fixture root dir");
    let large_corr_path = root_dir.join("large_corr_src_a.rs");
    let corr_source = build_large_rust_source(3_000, "a", 101);
    fs::write(&large_corr_path, corr_source.as_bytes())
        .expect("write >64KiB correction src/a.rs payload");

    let orig_script = fs::read_to_string(&fixture.fake_agy_exe).expect("read fake-agy");
    let replacement = [
        "            case \"$PROMPT\" in".to_string(),
        "                *LARGE_CORR_DIFF_180K*)".to_string(),
        format!(
            "                    cp \"{}\" \"src/a.rs\"",
            large_corr_path.display(),
        ),
        "                    ;;".to_string(),
        "                *)".to_string(),
        "                    printf \"pub fn a() -> u32 { 101 }\\n\" > \"src/a.rs\"".to_string(),
        "                    ;;".to_string(),
        "            esac".to_string(),
    ]
    .join("\n");
    let updated_script = orig_script.replacen(
        "            printf \"pub fn a() -> u32 { 101 }\\n\" > \"src/a.rs\"",
        &replacement,
        1,
    );
    assert_ne!(updated_script, orig_script);
    write_executable_script(&fixture.fake_agy_exe, &updated_script);

    let required_fixes = vec![
        "LARGE_CORR_DIFF_180K Expand src/a.rs in-scope definitions".to_string(),
        "Update src/a.rs::a() to return 101".to_string(),
    ];
    let PreparedCorrectionFixtureTask {
        job,
        approval_id,
        approval_before,
        approved_task_before: _,
        host_handle,
        session,
        step_ts,
        verify_1,
        review_subject_1: _,
        policy_digest: _,
        review_1: _,
        art_key_1: _,
        art_1_before: _,
        task_manifest,
        prep_outcome: _,
        receipt: _,
        expected_req_key: _,
        expected_eff_id,
        expected_report_id: _,
    } = Box::pin(prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_diff_180k",
        "plan-corr-diff-180k",
        "task-corr-diff-180k",
        required_fixes,
    ))
    .await;

    let correction_exec_service =
        CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_req = CoworkerTaskCorrectionExecuteRequest {
        binding_id: BINDING_A.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        job_id: job.job_id.clone(),
        task_id: "task-corr-diff-180k".to_string(),
        prior_attempt_no: 1,
        now_ms: step_ts + 300,
    };
    let exec_outcome =
        Box::pin(correction_exec_service.execute_task_correction(session.guard(), &exec_req))
            .await
            .expect("execute >64KiB derived correction");

    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(exec_outcome.task_stage, CoworkerTaskStage::ReadyForReview);
    assert!(!exec_outcome.reused_existing);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let legacy_inc_diff = run_sanitized_git(
        &task_manifest.checkout_path,
        &["diff", "--binary", &verify_1.committed_head_commit, "--"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    );
    assert!(legacy_inc_diff.is_err());
    let legacy_base_diff = run_sanitized_git(
        &task_manifest.checkout_path,
        &["diff", "--binary", &fixture.base_commit, "--"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    );
    assert!(legacy_base_diff.is_err());

    let raw_inc_diff = run_sanitized_git_bytes(
        &task_manifest.checkout_path,
        &["diff", "--binary", &verify_1.committed_head_commit, "--"],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("4 MiB raw incremental diff");
    let raw_base_diff = run_sanitized_git_bytes(
        &task_manifest.checkout_path,
        &["diff", "--binary", &fixture.base_commit, "--"],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("4 MiB raw base diff");
    assert!(raw_inc_diff.stdout_bytes.len() > 180_000);
    assert!(raw_base_diff.stdout_bytes.len() > 180_000);

    let report = exec_outcome
        .report
        .as_ref()
        .expect("persisted derived execution report");
    report
        .validate_integrity()
        .expect("validate persisted derived report integrity");
    let loaded_report =
        read_persisted_derived_task_execution_report(&fixture.plans_dir, &expected_eff_id)
            .expect("load persisted derived report")
            .expect("persisted derived report exists");
    assert_eq!(&loaded_report, report);

    session.release().expect("release session");
    let reopened_session = fixture
        .controller
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, step_ts + 400)
        .expect("reopen session");
    let reopened_outcome = Box::pin(correction_exec_service.execute_task_correction(
        reopened_session.guard(),
        &CoworkerTaskCorrectionExecuteRequest {
            now_ms: step_ts + 500,
            ..exec_req
        },
    ))
    .await
    .expect("reopen >64KiB derived correction idempotently");
    assert_eq!(
        reopened_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert!(reopened_outcome.reused_existing);
    assert_eq!(reopened_outcome.report, exec_outcome.report);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load ApprovalRecord");
    assert_eq!(approval_after, approval_before);

    reopened_session
        .release()
        .expect("release reopened session");
    host_handle.shutdown().await.expect("shutdown host");
}

/// Adds only a deterministic unverified diagnostic fixture to an already-created fake executable.
fn install_owned_health_fixture(fixture: &ControllerTestFixture) {
    let original = fs::read_to_string(&fixture.fake_agy_exe).expect("read disposable role fixture");
    if original.contains("THREADLANE_OWNED_HEALTH_FIXTURE") {
        return;
    }
    let prefix = format!(
        r#"
# THREADLANE_OWNED_HEALTH_FIXTURE: synthetic text is not authenticated provider semantics.
health_previous=''
health_role=''
for health_argument in "$@"; do
    if [ "$health_previous" = '--log-file' ]; then
        printf '%s\n' 'I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.' > "$health_argument"
    fi
    case "$health_argument" in
        *StructuredTaskReviewModelResponse*) health_role='review' ;;
        *StructuredClosureModelResponse*) health_role='closure' ;;
    esac
    health_previous="$health_argument"
done
health_wait=0
while [ -n "$health_role" ] && [ -f '{plans_dir}/health-'"$health_role"'.gate' ] && [ "$health_wait" -lt 1000 ]; do
    sleep 0.01
    health_wait=$((health_wait + 1))
done
"#,
        plans_dir = fixture.plans_dir.display()
    );
    let updated = original.replacen("set -eu\n", &format!("set -eu\n{prefix}"), 1);
    assert_ne!(updated, original, "fixture shell prefix must be present");
    write_executable_script(&fixture.fake_agy_exe, &updated);
}

/// Live status must be durably readable before release, with exact effect/task/attempt/subject checks.
async fn wait_for_owned_role_health(
    fixture: &ControllerTestFixture,
    guard: &CoworkerJobOwnershipGuard,
    job_id: &str,
    role: WorkflowRole,
    task_id: Option<&str>,
    attempt_no: Option<u32>,
) -> threadlane_native::provider_health::ScopedProviderHealth {
    use threadlane_native::provider_health::{
        controller_health_views, ProviderHealthInvocation, ScopedHealthSink,
    };
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let state = fixture
            .policy_store
            .get_job_runtime_readonly(BINDING_A, job_id)
            .expect("read scoped live health")
            .expect("runtime");
        let owner = guard
            .validate_before_effect(&guard.key())
            .expect("actual owner");
        let found = controller_health_views(&state, Some(owner)).into_iter().find(|view| {
            view.live && view.record.snapshot.retry_count > 0 && matches!(
                &view.record.invocation,
                ProviderHealthInvocation::Controller { role: actual_role, task_id: actual_task, attempt_no: actual_attempt, .. }
                    if *actual_role == role && actual_task.as_deref() == task_id && *actual_attempt == attempt_no
            )
        });
        if let Some(view) = found {
            let ProviderHealthInvocation::Controller {
                effect_id,
                subject_digest,
                ..
            } = &view.record.invocation
            else {
                unreachable!()
            };
            assert!(!view.record.terminal_observed);
            assert_eq!(
                view.record.snapshot.availability,
                threadlane_workers::provider_health::ProviderDiagnosticAvailability::Available
            );
            assert!(view.record.snapshot.unavailable_reason.is_none());
            assert_eq!(view.record.snapshot.evidence_class,
                threadlane_workers::provider_health::ProviderHealthEvidenceClass::UnverifiedProviderDiagnosticHint);
            let before = fs::read(
                fixture
                    .policy_store
                    .ledger_path_for_binding(BINDING_A)
                    .expect("runtime path"),
            )
            .expect("runtime bytes");
            assert!(ScopedHealthSink::controller(
                &fixture.controller,
                guard,
                "eff_wrong_reservation",
                role,
                task_id,
                attempt_no,
                subject_digest
            )
            .is_err());
            assert!(ScopedHealthSink::controller(
                &fixture.controller,
                guard,
                effect_id,
                role,
                task_id,
                attempt_no,
                &"f".repeat(64)
            )
            .is_err());
            if attempt_no.is_some() {
                assert!(ScopedHealthSink::controller(
                    &fixture.controller,
                    guard,
                    effect_id,
                    role,
                    task_id,
                    Some(99),
                    subject_digest
                )
                .is_err());
                assert!(ScopedHealthSink::controller(
                    &fixture.controller,
                    guard,
                    effect_id,
                    role,
                    Some("wrong_task"),
                    attempt_no,
                    subject_digest
                )
                .is_err());
            }
            let after = fs::read(
                fixture
                    .policy_store
                    .ledger_path_for_binding(BINDING_A)
                    .expect("runtime path"),
            )
            .expect("runtime bytes");
            assert_eq!(
                before, after,
                "rejected scope must not mint metadata or reserve execution"
            );
            return view.record;
        }
        assert!(
            Instant::now() < deadline,
            "missing live health for {role:?} while provider is held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
