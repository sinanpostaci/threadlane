use super::*;
use threadlane_native::coworker_workflow::controller_policy::compute_capability_envelope_digest;
use threadlane_native::coworker_workflow::task_correction::{
    compute_check_failure_derived_coder_request_key,
    compute_check_failure_derived_task_attempt_receipt_id,
};
use threadlane_workers::BoundedCommandOverflow;

#[tokio::test]
async fn check_failure_correction_capability_defaults_false_preserves_legacy_v1_identity_and_refuses_when_disabled(
) {
    let fixture = setup_fixture();

    // 1. Legacy JSON without `allow_check_failure_corrections` deserializes as `false`,
    //    omits the field on serialization, and preserves the exact v1 capability digest.
    let legacy_cap_json = serde_json::json!({
        "allowed_reviewer_choices": [headless_argon_choice(Difficulty::Hard)],
        "allowed_closure_choices": [headless_argon_choice(Difficulty::Extreme)],
        "max_in_scope_correction_attempts": 2,
        "allowed_check_ids": ["cargo_test"],
        "local_git_integration_allowed": true
    });
    let decoded_cap: FrozenControllerCapabilityEnvelope =
        serde_json::from_value(legacy_cap_json).expect("deserialize legacy capability envelope");
    assert!(!decoded_cap.allow_check_failure_corrections);
    let reserialized_cap = serde_json::to_string(&decoded_cap).expect("serialize default-off cap");
    assert!(!reserialized_cap.contains("allow_check_failure_corrections"));

    let expected_v1_seed = (
        "threadlane-native:frozen_controller_capability_envelope:v1",
        &decoded_cap.allowed_reviewer_choices,
        &decoded_cap.allowed_closure_choices,
        decoded_cap.max_in_scope_correction_attempts,
        &decoded_cap.allowed_check_ids,
        decoded_cap.local_git_integration_allowed,
    );
    let expected_v1_cap_digest =
        sha256_hex(&serde_json::to_vec(&expected_v1_seed).expect("serialize v1 seed"));
    let actual_v1_cap_digest =
        compute_capability_envelope_digest(&decoded_cap).expect("compute v1 cap digest");
    assert_eq!(actual_v1_cap_digest, expected_v1_cap_digest);

    let mut opt_in_cap = decoded_cap.clone();
    opt_in_cap.allow_check_failure_corrections = true;
    let opt_in_cap_json = serde_json::to_string(&opt_in_cap).expect("serialize opt-in cap");
    assert!(opt_in_cap_json.contains("\"allow_check_failure_corrections\":true"));
    let opt_in_cap_digest =
        compute_capability_envelope_digest(&opt_in_cap).expect("compute v2 cap digest");
    assert_ne!(opt_in_cap_digest, actual_v1_cap_digest);

    // Enabling `allow_check_failure_corrections` with `max_in_scope_correction_attempts == 0` is rejected.
    let mut invalid_cap = opt_in_cap.clone();
    invalid_cap.max_in_scope_correction_attempts = 0;
    assert_eq!(
        invalid_cap
            .validate(Some(&execution_policy()))
            .expect_err("allow_check_failure_corrections with 0 max attempts must fail")
            .code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // 2. Default-off job with a clean natural nonzero check failure refuses `prepare_task_correction`
    //    with `UnsupportedCapability` before recording a candidate or charging any counters.
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... FAILED\\n\"\nprintf \"error: test failed\\n\" >&2\nexit 101\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-default-off".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_default_off".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_with_options(
        &fixture,
        "brf_chk_default_off",
        "plan-chk-default-off",
        vec![PlanTask {
            id: "task-default-off".to_string(),
            title: "Default Off Check Failure Task".to_string(),
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
        false,
        2,
        2,
        4,
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
            .find(|t| t.task_id == "task-default-off")
            .expect("task observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_1 = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-default-off".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt attempt 1");
    assert_eq!(
        verify_1.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );
    assert!(verify_1.review_subject.is_none());

    let corr_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let disabled_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-default-off".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 100,
            },
        )
        .await
        .expect_err("default-off capability must refuse check-failure correction preparation");
    assert_eq!(
        disabled_err.code(),
        NativeErrorCode::UnsupportedCapability.as_str()
    );

    let post_runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(post_runtime.total_corrections_reserved, 0);
    assert_eq!(post_runtime.total_task_attempts_reserved, 1);
    assert!(post_runtime.derived_attempts.is_empty());

    let post_job = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists");
    assert!(post_job.tasks[0].candidates.is_empty());
    assert_eq!(post_job.tasks[0].stage, CoworkerTaskStage::ReadyForReview);

    drop(session);
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn check_failure_correction_prepares_from_clean_natural_nonzero_check_reopens_idempotently_and_fences_execution_boundary(
) {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... FAILED\\n\"\nprintf \"error: assertion failed in src_a\\n\" >&2\nexit 101\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-chk-prep".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_chk_prep".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_with_options(
        &fixture,
        "brf_chk_prep",
        "plan-chk-prep",
        vec![PlanTask {
            id: "task-chk-prep".to_string(),
            title: "Opt-In Check Failure Correction Task".to_string(),
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
        true,
        2,
        2,
        4,
    )
    .await;

    let approval_before = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("load original ApprovalRecord");

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
            .find(|t| t.task_id == "task-chk-prep")
            .expect("task observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ReadyForReview"
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
                task_id: "task-chk-prep".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt attempt 1");
    assert_eq!(
        verify_1.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );
    assert!(verify_1.review_subject.is_none());
    assert!(verify_1.check_evidence.is_empty());
    assert_eq!(verify_1.check_reports.len(), 1);

    let job_before_prep = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger before prepare")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists before prepare");
    let att_1_before = job_before_prep.tasks[0]
        .attempts
        .iter()
        .find(|a| a.attempt_no == 1 && a.role == WorkflowRole::Coder)
        .expect("attempt 1 Coder TaskAttemptRef")
        .clone();
    assert_eq!(
        att_1_before.head_commit.as_deref(),
        Some(att_1_before.base_commit.as_str())
    );
    assert_ne!(
        att_1_before.head_commit.as_deref(),
        Some(verify_1.committed_head_commit.as_str())
    );

    let corr_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());
    let prep_1 = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-prep".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 200,
            },
        )
        .await
        .expect("prepare check-failure correction attempt 2");

    assert_eq!(prep_1.status, CoworkerTaskCorrectionPrepareStatus::Prepared);
    assert!(!prep_1.reused_existing);
    assert_eq!(prep_1.task_stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(prep_1.prior_attempt_no, 1);
    assert_eq!(prep_1.next_attempt_no, 2);
    assert_eq!(prep_1.prior_candidate_id, verify_1.candidate_id);
    assert!(prep_1.prior_review_verdict_id.is_none());

    let basis = prep_1
        .prior_check_failure_basis
        .as_ref()
        .expect("prior_check_failure_basis must be populated");
    assert_eq!(basis.task_id, "task-chk-prep");
    assert_eq!(basis.prior_attempt_no, 1);
    assert_eq!(basis.prior_candidate_id, verify_1.candidate_id);
    assert_eq!(basis.committed_head_commit, verify_1.committed_head_commit);
    assert_eq!(basis.committed_tree_sha, verify_1.committed_tree_sha);
    assert_eq!(basis.failed_checks.len(), 1);
    assert_eq!(basis.failed_checks[0].check_id, "cargo_test");
    assert_eq!(
        basis.failed_checks[0].status,
        FrozenCheckExecutionStatus::NonZeroExit
    );
    assert_eq!(basis.failed_checks[0].exit_code, Some(101));

    let receipt = prep_1
        .require_receipt()
        .expect("Prepared outcome must carry DerivedTaskAttemptReceipt");
    assert!(receipt.prior_review_verdict_id.is_none());
    assert_eq!(receipt.prior_check_failure_basis.as_ref(), Some(basis));
    assert!(receipt
        .derived_prompt
        .contains("THREADLANE CHECK-FAILURE IN-SCOPE CORRECTION DIRECTIVE (ATTEMPT 2)"));
    assert!(receipt
        .derived_prompt
        .contains("prior_review_verdict_id: none (unreviewed frozen check failure)"));
    assert!(receipt
        .derived_prompt
        .contains("error: assertion failed in src_a"));

    let basis_digest = basis.compute_digest().expect("basis digest");
    let runtime = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    let expected_receipt_id = compute_check_failure_derived_task_attempt_receipt_id(
        &job.job_id,
        "task-chk-prep",
        2,
        &verify_1.candidate_id,
        &basis_digest,
        &prep_1.prior_review_subject_digest,
        &runtime.envelope.envelope_digest,
    );
    let expected_req_key = compute_check_failure_derived_coder_request_key(
        &job.job_id,
        "task-chk-prep",
        2,
        &verify_1.candidate_id,
        &basis_digest,
        &prep_1.prior_review_subject_digest,
        &runtime.envelope.envelope_digest,
        &job.tasks[0].selected_choice,
    );
    let expected_eff_id =
        compute_derived_coder_effect_id(&job.job_id, "task-chk-prep", 2, &expected_req_key);
    assert_eq!(receipt.receipt_id, expected_receipt_id);
    assert_eq!(
        prep_1.request_key.as_deref(),
        Some(expected_req_key.as_str())
    );
    assert_eq!(prep_1.effect_id.as_deref(), Some(expected_eff_id.as_str()));

    // Verify no extra model call occurred and Reviewer was never invoked.
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);
    assert_eq!(read_invocation_count(&fixture.agy_planning_count_file), 1);
    assert_eq!(runtime.total_task_attempts_reserved, 2);
    assert_eq!(runtime.total_corrections_reserved, 1);
    assert_eq!(
        runtime
            .role_usage
            .get(&WorkflowRole::Coder)
            .expect("coder usage")
            .calls_reserved,
        2
    );
    assert_eq!(
        runtime
            .role_usage
            .get(&WorkflowRole::Reviewer)
            .expect("reviewer usage")
            .calls_reserved,
        0
    );

    // Prior attempt artifacts must NOT have a promoted `review_subject` (`ReadyForReviewer`).
    let art_1 = runtime
        .attempt_artifacts
        .get(&ControllerJobRuntimeState::artifact_key("task-chk-prep", 1))
        .expect("attempt 1 artifacts");
    assert!(art_1.review_subject.is_none());

    // Task candidate in workflow ledger is unreviewed (`review_verdict == None`).
    let updated_job = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists");
    let updated_task = &updated_job.tasks[0];
    assert_eq!(updated_task.stage, CoworkerTaskStage::DispatchReserved);
    assert_eq!(updated_task.current_attempt_no, 2);
    assert_eq!(updated_task.candidates.len(), 1);
    assert_eq!(
        updated_task.candidates[0].candidate_id,
        verify_1.candidate_id
    );
    assert!(updated_task.candidates[0].review_verdict.is_none());
    assert!(updated_task.candidates[0].review_receipt_id.is_none());
    assert!(updated_task.candidates[0].check_evidence_digests.is_empty());
    assert_eq!(
        updated_task.candidates[0].review_subject_digest.as_deref(),
        Some(prep_1.prior_review_subject_digest.as_str())
    );
    let att_1_after = updated_task
        .attempts
        .iter()
        .find(|a| a.attempt_no == 1 && a.role == WorkflowRole::Coder)
        .expect("attempt 1 Coder TaskAttemptRef after prepare");
    assert_eq!(
        att_1_after.head_commit.as_deref(),
        Some(verify_1.committed_head_commit.as_str())
    );
    assert_eq!(
        att_1_after.tree_sha.as_deref(),
        Some(verify_1.committed_tree_sha.as_str())
    );
    assert_eq!(att_1_after.base_commit, att_1_before.base_commit);
    assert_eq!(att_1_after.run_id, att_1_before.run_id);
    assert_eq!(att_1_after.worker_id, att_1_before.worker_id);
    assert_eq!(att_1_after.task_worktree_id, att_1_before.task_worktree_id);
    assert_eq!(att_1_after.outbox_id, att_1_before.outbox_id);
    assert_eq!(att_1_after.request_key, att_1_before.request_key);
    assert_eq!(att_1_after.diff_digest, att_1_before.diff_digest);
    assert_eq!(
        att_1_after.report_artifact_id,
        att_1_before.report_artifact_id
    );
    assert_eq!(att_1_after.observed_model, att_1_before.observed_model);
    assert_eq!(att_1_after.observed_effort, att_1_before.observed_effort);

    // Reopening the same check-failure correction reservation is idempotent and charges no extra counters.
    let reopened = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-prep".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 400,
            },
        )
        .await
        .expect("reopen check-failure correction attempt 2");
    assert_eq!(
        reopened.status,
        CoworkerTaskCorrectionPrepareStatus::Reopened
    );
    assert!(reopened.reused_existing);
    assert_eq!(reopened.receipt, prep_1.receipt);
    assert_eq!(reopened.request_key, prep_1.request_key);
    assert_eq!(reopened.effect_id, prep_1.effect_id);
    assert_eq!(reopened.outbox_id, prep_1.outbox_id);

    let runtime_after_reopen = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(runtime_after_reopen.total_task_attempts_reserved, 2);
    assert_eq!(runtime_after_reopen.total_corrections_reserved, 1);

    // Executing a check-failure-derived reservation is explicitly fenced in this slice (`UnsupportedCapability`).
    let exec_service = CoworkerTaskCorrectionExecutionService::new(fixture.controller.clone());
    let exec_err = exec_service
        .execute_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionExecuteRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-prep".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 500,
            },
        )
        .await
        .expect_err(
            "check-failure correction execution must remain explicitly fenced in this slice",
        );
    assert_eq!(
        exec_err.code(),
        NativeErrorCode::UnsupportedCapability.as_str()
    );
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 1);

    let approval_after = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("reload original ApprovalRecord");
    assert_eq!(approval_after, approval_before);

    drop(session);
    host_handle.shutdown().await.expect("shutdown host");
}

#[tokio::test]
async fn check_failure_correction_refuses_tampered_missing_overflow_descendant_and_dirty_evidence_and_shares_caps(
) {
    let fixture = setup_fixture();
    write_executable_script(
        &fixture.fake_cargo_exe,
        "#!/bin/sh\nset -eu\nprintf \"running 1 test\\ntest src_a ... FAILED\\n\"\nprintf \"error: test failed\\n\" >&2\nexit 101\n",
    );

    let mut task_interfaces = BTreeMap::new();
    task_interfaces.insert(
        "task-chk-guards".to_string(),
        TaskInterfaceOwnershipSpec {
            owned_interfaces: vec!["iface_chk_guards".to_string()],
            consumed_interfaces: Vec::new(),
        },
    );
    let (job, approval_id) = create_planned_approved_and_frozen_job_with_options(
        &fixture,
        "brf_chk_guards",
        "plan-chk-guards",
        vec![PlanTask {
            id: "task-chk-guards".to_string(),
            title: "Check Failure Guards Task".to_string(),
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
        true,
        1,
        1,
        3,
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
        .open_owned_session(BINDING_A, THREAD_UUID_A, &job.job_id, 400_000)
        .expect("open controller session");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut step_ts = 400_100u64;
    loop {
        let rep = session.step(step_ts).await.expect("controller step");
        step_ts += 100;
        let obs = rep
            .tasks
            .iter()
            .find(|t| t.task_id == "task-chk-guards")
            .expect("task observation");
        if obs.stage == CoworkerTaskStage::ReadyForReview {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ReadyForReview"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let verifier = CoworkerTaskVerificationService::new(fixture.controller.clone());
    let verify_1 = verifier
        .verify_and_prepare_task_attempt(
            session.guard(),
            &CoworkerTaskVerificationRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                attempt_no: 1,
                now_ms: step_ts,
            },
        )
        .await
        .expect("verify_and_prepare_task_attempt attempt 1");
    assert_eq!(
        verify_1.status,
        CoworkerTaskVerificationStatus::BlockedByFailedChecks
    );

    let check_ref = &verify_1.check_reports[0];
    let report_path = PathBuf::from(&check_ref.report_path);
    let original_report_bytes = fs::read(&report_path).expect("read original check report bytes");
    let original_report = read_persisted_check_report(&fixture.plans_dir, &check_ref.effect_id)
        .expect("read persisted check report")
        .expect("report exists");

    let job_before_guards = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger before guards")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists before guards");
    let att_1_before_guards = job_before_guards.tasks[0]
        .attempts
        .iter()
        .find(|a| a.attempt_no == 1 && a.role == WorkflowRole::Coder)
        .expect("attempt 1 Coder TaskAttemptRef before guards")
        .clone();
    assert_eq!(
        att_1_before_guards.head_commit.as_deref(),
        Some(att_1_before_guards.base_commit.as_str())
    );

    let corr_service = CoworkerTaskCorrectionService::new(fixture.controller.clone());

    // 1. Missing private FrozenCheckExecutionReport file fails closed with ProtocolError.
    fs::remove_file(&report_path).expect("remove check report file");
    let missing_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 100,
            },
        )
        .await
        .expect_err("missing private check report must fail");
    assert_eq!(missing_err.code(), NativeErrorCode::ProtocolError.as_str());
    assert!(missing_err
        .message()
        .contains("missing persisted private 0600 FrozenCheckExecutionReport"));

    // 2. Non-natural / incomplete check statuses (OutputOverflow, DescendantsTerminatedAfterExit, TimedOut)
    //    are refused before any candidate or counter reservation.
    let mut overflow_report = original_report.clone();
    overflow_report.status = FrozenCheckExecutionStatus::OutputOverflow;
    overflow_report.stdout_overflowed = true;
    overflow_report.overflow = Some(BoundedCommandOverflow::StdoutExceeded {
        max_stdout_bytes: overflow_report.max_stdout_bytes,
    });
    overflow_report.evidence_digest = overflow_report
        .compute_evidence_digest()
        .expect("compute overflow digest");
    write_private_0600(
        &report_path,
        &serde_json::to_vec_pretty(&overflow_report).expect("serialize overflow report"),
    );
    let overflow_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 200,
            },
        )
        .await
        .expect_err("OutputOverflow check report must be refused");
    assert_eq!(
        overflow_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let mut descendants_report = original_report.clone();
    descendants_report.descendants_terminated_after_exit = true;
    descendants_report.evidence_digest = descendants_report
        .compute_evidence_digest()
        .expect("compute descendants digest");
    write_private_0600(
        &report_path,
        &serde_json::to_vec_pretty(&descendants_report).expect("serialize descendants report"),
    );
    let descendants_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 300,
            },
        )
        .await
        .expect_err("forced descendants check report must be refused");
    assert_eq!(
        descendants_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    // Restore valid report and verify dirty task worktree is refused before any reservation.
    write_private_0600(&report_path, &original_report_bytes);
    let dirty_file = original_report
        .checkout_path
        .join("src/untracked_dirty.txt");
    fs::write(&dirty_file, b"dirty").expect("write dirty file in task worktree");
    let dirty_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 400,
            },
        )
        .await
        .expect_err("dirty task worktree must be refused");
    assert_eq!(dirty_err.code(), NativeErrorCode::ValidationFailed.as_str());
    fs::remove_file(&dirty_file).expect("clean up dirty file");

    // Ignored artifacts inside the owned task worktree remain strict dirt.
    let task_cache_dir = original_report.checkout_path.join("target");
    fs::create_dir_all(&task_cache_dir).expect("create ignored task worktree cache dir");
    fs::write(
        task_cache_dir.join("cache.bin"),
        b"cached-task-worktree-output",
    )
    .expect("write ignored task worktree cache artifact");
    let ignored_task_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 410,
            },
        )
        .await
        .expect_err("ignored artifact in owned task worktree must be refused");
    assert_eq!(
        ignored_task_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    fs::remove_dir_all(&task_cache_dir).expect("clean up ignored task worktree cache dir");

    // Primary source cleanliness permits ignored caches but still rejects tracked, staged,
    // and non-ignored untracked source changes before any reservation.
    let primary_source_file = fixture.repo_dir.join("src/b.rs");
    let original_primary_source =
        fs::read_to_string(&primary_source_file).expect("read primary source file");
    fs::write(&primary_source_file, "pub fn b() -> u32 { 202 }\n")
        .expect("modify tracked primary source file");
    let tracked_primary_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 420,
            },
        )
        .await
        .expect_err("tracked primary source change must be refused");
    assert_eq!(
        tracked_primary_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    fs::write(&primary_source_file, &original_primary_source)
        .expect("restore tracked primary source file");

    fs::write(&primary_source_file, "pub fn b() -> u32 { 203 }\n")
        .expect("modify staged primary source file");
    run_sanitized_git(
        &fixture.repo_dir,
        &["add", "src/b.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("stage primary source change");
    let staged_primary_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 430,
            },
        )
        .await
        .expect_err("staged primary source change must be refused");
    assert_eq!(
        staged_primary_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    run_sanitized_git(
        &fixture.repo_dir,
        &["reset", "--quiet", "HEAD", "--", "src/b.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("unstage primary source change");
    fs::write(&primary_source_file, &original_primary_source)
        .expect("restore staged primary source file");

    let untracked_primary_file = fixture.repo_dir.join("primary-untracked.txt");
    fs::write(&untracked_primary_file, b"not ignored\n")
        .expect("write non-ignored untracked primary file");
    let untracked_primary_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 440,
            },
        )
        .await
        .expect_err("non-ignored untracked primary file must be refused");
    assert_eq!(
        untracked_primary_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );
    fs::remove_file(&untracked_primary_file).expect("remove non-ignored untracked primary file");

    // Verify zero counters or candidates were recorded across all refusals above.
    let runtime_before = fixture
        .policy_store
        .get_job_runtime_readonly(BINDING_A, &job.job_id)
        .expect("read runtime")
        .expect("runtime exists");
    assert_eq!(runtime_before.total_corrections_reserved, 0);
    assert_eq!(runtime_before.total_task_attempts_reserved, 1);
    assert!(runtime_before.derived_attempts.is_empty());

    // 3. Shared job-wide correction budget (`max_total_corrections_per_job == 1`): if already exhausted,
    //    check-failure correction refuses without a separate independent allowance.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.total_corrections_reserved = 1;
            Ok(())
        })
        .expect("exhaust shared total_corrections_reserved");

    let cap_exhausted_err = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 500,
            },
        )
        .await
        .expect_err(
            "exhausted shared max_total_corrections_per_job must refuse check-failure correction",
        );
    assert_eq!(
        cap_exhausted_err.code(),
        NativeErrorCode::ValidationFailed.as_str()
    );

    let job_after_refusals = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after refusals")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists after refusals");
    assert_eq!(
        job_after_refusals.tasks[0].stage,
        CoworkerTaskStage::ReadyForReview
    );
    assert!(job_after_refusals.tasks[0].candidates.is_empty());
    let att_1_after_refusals = job_after_refusals.tasks[0]
        .attempts
        .iter()
        .find(|a| a.attempt_no == 1 && a.role == WorkflowRole::Coder)
        .expect("attempt 1 Coder TaskAttemptRef after refusals");
    assert_eq!(att_1_after_refusals, &att_1_before_guards);

    // Restore shared counter and verify clean preparation succeeds once even while the primary
    // source checkout contains ignored build caches.
    fixture
        .policy_store
        .mutate_job_runtime_for_operator(BINDING_A, &job.job_id, |state| {
            state.total_corrections_reserved = 0;
            Ok(())
        })
        .expect("restore total_corrections_reserved");

    let primary_cache_file = fixture.repo_dir.join("target/cache.bin");
    fs::create_dir_all(primary_cache_file.parent().expect("primary cache parent"))
        .expect("create ignored primary cache dir");
    fs::write(&primary_cache_file, b"cached-primary-build-output")
        .expect("write ignored primary cache artifact");

    let prepared = corr_service
        .prepare_task_correction(
            session.guard(),
            &CoworkerTaskCorrectionPrepareRequest {
                binding_id: BINDING_A.to_string(),
                thread_uuid: THREAD_UUID_A.to_string(),
                job_id: job.job_id.clone(),
                task_id: "task-chk-guards".to_string(),
                prior_attempt_no: 1,
                now_ms: step_ts + 600,
            },
        )
        .await
        .expect("prepare check-failure correction after restoring shared budget");
    assert_eq!(
        prepared.status,
        CoworkerTaskCorrectionPrepareStatus::Prepared
    );
    assert_eq!(
        fs::read(&primary_cache_file).expect("read ignored primary cache artifact"),
        b"cached-primary-build-output"
    );

    let job_after_prep = fixture
        .workflow_store
        .read_binding_ledger(BINDING_A)
        .expect("read ledger after prepare")
        .jobs
        .into_iter()
        .find(|j| j.job_id == job.job_id)
        .expect("job exists after prepare");
    let att_1_after_prep = job_after_prep.tasks[0]
        .attempts
        .iter()
        .find(|a| a.attempt_no == 1 && a.role == WorkflowRole::Coder)
        .expect("attempt 1 Coder TaskAttemptRef after prepare");
    assert_eq!(
        att_1_after_prep.head_commit.as_deref(),
        Some(verify_1.committed_head_commit.as_str())
    );
    assert_eq!(
        att_1_after_prep.tree_sha.as_deref(),
        Some(verify_1.committed_tree_sha.as_str())
    );

    drop(session);
    host_handle.shutdown().await.expect("shutdown host");
}

fn write_private_0600(path: &Path, bytes: &[u8]) {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, bytes).expect("write private file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("set 0600 permissions");
}
