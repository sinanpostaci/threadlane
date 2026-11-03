use super::*;

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

async fn prepare_task_for_derived_correction(
    fixture: &ControllerTestFixture,
    brief_id: &str,
    plan_id: &str,
    task_id: &str,
    required_fixes: Vec<String>,
) -> PreparedCorrectionFixtureTask {
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... ok\\n\"\nexit 0\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        task_id.to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec![format!("iface_{brief_id}")],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job(
        fixture,
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
        ExecutionBudgets::default(),
        ControllerRoleExecutionBudget::default_for_role(WorkflowRole::Coder, 4),
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
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 2);

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
    } = prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_exec",
        "plan-corr-exec",
        "task-corr-exec",
        required_fixes,
    )
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
    let exec_outcome = correction_exec_service
        .execute_task_correction(session.guard(), &exec_req)
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
        exec_outcome.prior_review_verdict_id,
        review_1.verdict.verdict_id
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

    assert_eq!(
        fs::read_to_string(&fixture.agy_prompt_file).expect("read agy prompt file"),
        receipt.derived_prompt
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
    let repeat_same = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 400,
                ..exec_req.clone()
            },
        )
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
    let reopened_exec = correction_exec_service
        .execute_task_correction(
            reopened_session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 600,
                ..exec_req
            },
        )
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
    let verify_2 = verifier
        .verify_and_prepare_task_attempt(
            reopened_session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-exec".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 700,
            },
        )
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

    let review_2 = review_service
        .review_task_attempt(
            reopened_session.guard(),
            &CoworkerTaskReviewStageRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-exec".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 800,
                untrusted_candidate_context: Some(
                    "Updated src/a.rs::a() to return 101".to_string(),
                ),
            },
        )
        .await
        .expect("review_task_attempt attempt 2 Accept");
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
    let post_int_step = reopened_session
        .step(step_ts + 1_000)
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

    reopened_session
        .release()
        .expect("release reopened session");
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
        cancelled_outcome.prior_review_verdict_id,
        review_1.verdict.verdict_id
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
    } = prepare_task_for_derived_correction(
        &fixture,
        "brf_corr_recov",
        "plan-corr-recov",
        "task-corr-recov",
        required_fixes,
    )
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
    let pre_symlink_err = correction_exec_service
        .execute_task_correction(session.guard(), &exec_req)
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
    let pre_conflict_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 320,
                ..exec_req.clone()
            },
        )
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
    let exec_outcome = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 350,
                ..exec_req.clone()
            },
        )
        .await
        .expect("execute_task_correction succeeds once conflicting file is removed");
    assert_eq!(
        exec_outcome.status,
        CoworkerTaskCorrectionExecuteStatus::ReadyForVerification
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 2);
    let valid_report_bytes = fs::read(&report_path).expect("read valid persisted report bytes");

    // 4. Corrupt the persisted report on disk with conflicting bytes: repeated execution and
    // verification requests must fail closed, preserve the conflicting bytes, and retain budgets
    // without another provider call or a new request key.
    let corrupt_bytes = b"{\"corrupt_persisted_derived_report\":true}";
    fs::write(&report_path, corrupt_bytes).expect("write corrupt bytes to report_path");
    fs::set_permissions(&report_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt report_path");

    let repeat_corrupt_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 400,
                ..exec_req.clone()
            },
        )
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

    let verify_corrupt_err = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-recov".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 450,
            },
        )
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

    let repeat_tampered_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 500,
                ..exec_req.clone()
            },
        )
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

    let verify_tampered_err = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-recov".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 550,
            },
        )
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

    let repeat_symlink_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 600,
                ..exec_req.clone()
            },
        )
        .await
        .expect_err("repeated execution with symlink report must fail closed");
    assert_eq!(
        repeat_symlink_err.code(),
        NativeErrorCode::SecurityViolation.as_str()
    );
    let verify_symlink_err = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-recov".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 650,
            },
        )
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

    let repeat_missing_err = correction_exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                now_ms: step_ts + 700,
                ..exec_req
            },
        )
        .await
        .expect_err("repeated execution with missing persisted report must fail closed");
    assert_eq!(
        repeat_missing_err.code(),
        NativeErrorCode::ProtocolError.as_str()
    );

    let verify_missing_err = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-corr-recov".to_string(),
                attempt_no: 2,
                now_ms: step_ts + 750,
            },
        )
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
