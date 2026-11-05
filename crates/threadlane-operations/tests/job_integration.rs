use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    execute_job_integration, inspect_final_candidate_readonly,
    inspect_job_integration_intent_readonly, inspect_review_subject, load_integration_receipt,
    prepare_and_seal_dependency_input, propose_job_integration,
    record_job_integration_intent_before_effect, verify_dependency_predecessor_proof,
    verify_final_candidate_subject_readonly, verify_legacy_base_closure_insufficient,
    ClosureAttemptState, ClosureExecutionEvidence, ClosureVerdictKind, CommitTaskChangesRequest,
    DependencyPredecessorCandidate, ExecuteIntegrationRequest, ExecuteJobIntegrationRequest,
    FinalCandidateClosureStore, FinalCandidatePolicyBinding, FinalCandidateTaskInput,
    InspectFinalCandidateRequest, InspectReviewSubjectRequest, IntegrationStrategy,
    JobIntegrationIntentState, JobIntegrationScopeProvenance, JobIntegrationTaskInput,
    JobTaskDependencyExecutionProof, MarkClosureSubmittedRequest, OperationsError,
    ProposeJobIntegrationRequest, RecordClosureVerdictRequest, ReserveClosureAttemptRequest,
    ReviewAcceptanceRequest, ReviewPolicyBinding, ReviewReceipt, ReviewSubject,
    TestEvidenceSummary, DEPENDENCIES_SUBDIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, DependencyInputPolicy,
    WorkerFilePermissions, WorkerRunStatus, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS, MANIFESTS_SUBDIR,
};

struct JobIntegrationFixture {
    _temp: TempDir,
    repo_path: PathBuf,
    work_area_root: PathBuf,
    approved_base_commit: String,
    approved_base_tree_sha: String,
    target_integration_run_id: String,
    integration_manifest: WorktreeManifest,
}

struct PreparedWorkerTask {
    original_run_id: String,
    task_id: String,
    task_digest: String,
    task_manifest: WorktreeManifest,
    subject: ReviewSubject,
    review_receipt: ReviewReceipt,
}

fn init_job_integration_fixture() -> JobIntegrationFixture {
    let temp = TempDir::new().expect("create tempdir");
    let repo_path = temp.path().join("repo");
    let work_area_root = temp.path().join("work-area");
    fs::create_dir_all(&repo_path).expect("create repo dir");
    fs::create_dir_all(&work_area_root).expect("create work_area dir");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&work_area_root, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 work_area_root");
    }

    run_sanitized_git(
        &repo_path,
        &["init", "-b", "main"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init");
    fs::create_dir_all(repo_path.join("src")).expect("create src dir");
    fs::write(
        repo_path.join("src/alpha.rs"),
        "pub fn alpha() -> u32 {\n    1\n}\n",
    )
    .expect("write alpha.rs");
    fs::write(
        repo_path.join("src/beta.rs"),
        "pub fn beta() -> u32 {\n    2\n}\n",
    )
    .expect("write beta.rs");
    fs::write(
        repo_path.join("src/gamma.rs"),
        "pub fn gamma() -> u32 {\n    3\n}\n",
    )
    .expect("write gamma.rs");

    run_sanitized_git(&repo_path, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git add");
    run_sanitized_git(
        &repo_path,
        &[
            "-c",
            "user.name=Test User",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "initial base commit",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");

    let canonical_repo = fs::canonicalize(&repo_path).expect("canonical repo");
    let canonical_work_area = fs::canonicalize(&work_area_root).expect("canonical work_area");
    let approved_base_commit =
        resolve_exact_commit_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD");
    let approved_base_tree_sha =
        resolve_exact_tree_sha(&canonical_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("resolve HEAD tree");

    let target_integration_run_id = "run_job_agg_target_1".to_string();
    let manager =
        WorktreeManager::new(&canonical_repo, &canonical_work_area, &approved_base_commit)
            .expect("WorktreeManager::new");
    let integration_manifest = manager
        .create_run_worktree_with_token(&target_integration_run_id, "int01")
        .expect("create target RunIntegration worktree");

    JobIntegrationFixture {
        _temp: temp,
        repo_path: canonical_repo,
        work_area_root: canonical_work_area,
        approved_base_commit,
        approved_base_tree_sha,
        target_integration_run_id,
        integration_manifest,
    }
}

fn default_task_policy() -> ReviewPolicyBinding {
    ReviewPolicyBinding {
        policy_id: "pol_task_impl".to_string(),
        requested_provider: Some("antigravity".to_string()),
        requested_model: Some("gemini-4-argon-high".to_string()),
        requested_effort: Some("high".to_string()),
        observed_model: Some("gemini-4-argon-high".to_string()),
        observed_effort: Some("high".to_string()),
        file_permissions: WorkerFilePermissions::TaskFiles,
        require_effort_evidence: true,
        require_passing_tests: true,
    }
}

fn default_test_evidence(summary: &str, passed_count: u32) -> TestEvidenceSummary {
    TestEvidenceSummary {
        formatting_passed: true,
        clippy_passed: true,
        tests_passed: true,
        passed_test_count: passed_count,
        failed_test_count: 0,
        evidence_summary: summary.to_string(),
    }
}

fn default_candidate_policy() -> FinalCandidatePolicyBinding {
    FinalCandidatePolicyBinding {
        policy_id: "pol_final_closure".to_string(),
        policy_digest: "11".repeat(32),
        scope_revision: 1,
        registry_revision: 1,
        approval_id: "app_cycle_1".to_string(),
        approval_subject_digest: "22".repeat(32),
        closure_task_id: "task_closing_review".to_string(),
        closure_task_digest: "33".repeat(32),
        requested_provider: "codex".to_string(),
        requested_model: "gpt-6-astra".to_string(),
        requested_effort: Some("max".to_string()),
        require_effort_evidence: true,
        require_passing_checks: true,
    }
}

fn default_execution_evidence(handoff_receipt_id: &str) -> ClosureExecutionEvidence {
    ClosureExecutionEvidence {
        evidence_id: format!("cev_{handoff_receipt_id}"),
        producer_run_id: "run_closure_eval_1".to_string(),
        producer_worker_id: Some("wrk_closure_1".to_string()),
        handoff_receipt_id: handoff_receipt_id.to_string(),
        observed_provider: "codex".to_string(),
        observed_model: Some("gpt-6-astra".to_string()),
        observed_effort: Some("max".to_string()),
        transcript_or_event_digest: "44".repeat(32),
        grants_merge_or_push_authority: false,
        substitutes_for_required_checks: false,
        pinned_launch_configuration: None,
    }
}

fn default_job_scope(target_integration_run_id: &str) -> JobIntegrationScopeProvenance {
    let mut aggregate_allowed_write_paths = BTreeSet::new();
    aggregate_allowed_write_paths.insert("src/alpha.rs".to_string());
    aggregate_allowed_write_paths.insert("src/beta.rs".to_string());
    JobIntegrationScopeProvenance {
        job_id: "job_multi_worker_1".to_string(),
        job_revision: 1,
        approval_id: "app_cycle_1".to_string(),
        approval_subject_digest: "22".repeat(32),
        target_integration_run_id: target_integration_run_id.to_string(),
        aggregate_allowed_write_paths,
    }
}

#[allow(clippy::too_many_arguments)]
fn create_and_accept_worker_task(
    fixture: &JobIntegrationFixture,
    original_run_id: &str,
    task_id: &str,
    token: &str,
    task_digest_byte: &str,
    rel_file: &str,
    new_contents: &str,
    approved_at_ms: i64,
) -> PreparedWorkerTask {
    let manager = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
    )
    .expect("WorktreeManager::new");
    let mut allowed = BTreeSet::new();
    allowed.insert(rel_file.to_string());

    let task_manifest = manager
        .create_task_worktree_with_token(original_run_id, task_id, token, &allowed)
        .expect("create task worktree in distinct worker run");
    fs::write(task_manifest.checkout_path.join(rel_file), new_contents)
        .expect("write task modification");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        commit_message: format!("Implement {task_id} in {original_run_id}"),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");

    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        policy: default_task_policy(),
        test_evidence: default_test_evidence("cargo fmt, clippy, and unit tests passed", 18),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_primary".to_string(),
        review_note: format!("Approved {task_id} from {original_run_id}"),
        approved_at_ms,
        receipt_id: Some(format!("rev_{task_id}_{token}")),
    })
    .expect("accept review subject");

    PreparedWorkerTask {
        original_run_id: original_run_id.to_string(),
        task_id: task_id.to_string(),
        task_digest: task_digest_byte.repeat(32),
        task_manifest,
        subject,
        review_receipt,
    }
}

fn to_job_task_input(
    task: &PreparedWorkerTask,
    strategy: IntegrationStrategy,
    receipt_id: Option<&str>,
) -> JobIntegrationTaskInput {
    JobIntegrationTaskInput {
        original_run_id: task.original_run_id.clone(),
        task_id: task.task_id.clone(),
        task_digest: task.task_digest.clone(),
        task_worktree_id: task.task_manifest.worktree_id.clone(),
        subject: task.subject.clone(),
        review_receipt: task.review_receipt.clone(),
        expected_task_head_commit: task.review_receipt.task_head_commit.clone(),
        dependency_input: None,
        strategy,
        receipt_id: receipt_id.map(str::to_string),
    }
}

fn assert_primary_checkout_untouched(fixture: &JobIntegrationFixture) {
    let head = resolve_exact_commit_sha(&fixture.repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve primary HEAD");
    assert_eq!(
        head, fixture.approved_base_commit,
        "primary checkout HEAD must never move"
    );
    let status = run_sanitized_git(
        &fixture.repo_path,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("primary git status");
    assert!(
        status.trim().is_empty(),
        "primary checkout must remain completely clean, got: {status}"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_path.join("src/alpha.rs")).expect("read primary alpha.rs"),
        "pub fn alpha() -> u32 {\n    1\n}\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.repo_path.join("src/beta.rs")).expect("read primary beta.rs"),
        "pub fn beta() -> u32 {\n    2\n}\n"
    );
}

#[test]
fn multi_worker_distinct_runs_aggregate_into_target_run_integration_and_pass_final_candidate_closure(
) {
    let fixture = init_job_integration_fixture();

    let task_a = create_and_accept_worker_task(
        &fixture,
        "run_worker_alpha_1",
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    101\n}\n",
        1_000,
    );
    let task_b = create_and_accept_worker_task(
        &fixture,
        "run_worker_beta_2",
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    202\n}\n",
        2_000,
    );

    assert_ne!(task_a.original_run_id, task_b.original_run_id);
    assert_ne!(task_a.original_run_id, fixture.target_integration_run_id);
    assert_ne!(task_b.original_run_id, fixture.target_integration_run_id);

    // 1. Legacy same-run execute_integration still rejects cross-run integration without job provenance.
    let legacy_err = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        task_worktree_id: task_a.task_manifest.worktree_id.clone(),
        subject: task_a.subject.clone(),
        review_receipt: task_a.review_receipt.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        expected_task_head_commit: task_a.review_receipt.task_head_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        receipt_id: Some("int_legacy_cross_run".to_string()),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_100,
    })
    .expect_err("legacy same-run execute_integration must reject cross-run task");
    assert!(matches!(legacy_err, OperationsError::WorktreeNotOwned(_)));

    // 2. Read-only job integration proposal predicts clean merges and does not mutate RunIntegration.
    let job_scope = default_job_scope(&fixture.target_integration_run_id);
    let tasks = vec![
        to_job_task_input(
            &task_a,
            IntegrationStrategy::FastForwardOnly,
            Some("int_job_alpha"),
        ),
        to_job_task_input(
            &task_b,
            IntegrationStrategy::MergeNoFf,
            Some("int_job_beta"),
        ),
    ];

    let proposal = propose_job_integration(&ProposeJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: tasks.clone(),
    })
    .expect("propose_job_integration");
    assert!(proposal.read_only);
    assert!(proposal.all_clean_merges_predicted);
    assert_eq!(proposal.task_proposals.len(), 2);
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.integration_manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("resolve integration HEAD after proposal"),
        fixture.approved_base_commit
    );

    // 3. Execute job integration across the two distinct worker runs into the target RunIntegration worktree.
    let report = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks,
        command_key: "cmd_job_integrate_1".to_string(),
        intent_id: Some("jint_batch_1".to_string()),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_500,
    })
    .expect("execute_job_integration");

    assert!(!report.reused_existing_receipts);
    assert_eq!(report.receipts.len(), 2);
    assert_eq!(report.intent.state, JobIntegrationIntentState::Completed);
    assert_ne!(
        report.final_integrated_target_commit,
        fixture.approved_base_commit
    );
    assert_ne!(
        report.final_integrated_tree_sha,
        fixture.approved_base_tree_sha
    );

    // Verify both changes exist in the owned RunIntegration checkout.
    assert_eq!(
        fs::read_to_string(
            fixture
                .integration_manifest
                .checkout_path
                .join("src/alpha.rs")
        )
        .expect("read integrated alpha.rs"),
        "pub fn alpha() -> u32 {\n    101\n}\n"
    );
    assert_eq!(
        fs::read_to_string(
            fixture
                .integration_manifest
                .checkout_path
                .join("src/beta.rs")
        )
        .expect("read integrated beta.rs"),
        "pub fn beta() -> u32 {\n    202\n}\n"
    );

    // Verify persisted IntegrationReceipts bind both original task run ID and target integration run ID + job scope.
    let rcp_a = load_integration_receipt(&fixture.work_area_root, "int_job_alpha")
        .expect("load int_job_alpha");
    let rcp_b = load_integration_receipt(&fixture.work_area_root, "int_job_beta")
        .expect("load int_job_beta");
    assert_eq!(rcp_a.run_id, "run_worker_alpha_1");
    assert_eq!(rcp_a.target_integration_run_id(), "run_job_agg_target_1");
    assert_eq!(rcp_b.run_id, "run_worker_beta_2");
    assert_eq!(rcp_b.target_integration_run_id(), "run_job_agg_target_1");

    let agg_a = rcp_a
        .job_aggregation
        .as_ref()
        .expect("rcp_a job_aggregation");
    assert_eq!(agg_a.original_task_run_id, "run_worker_alpha_1");
    assert_eq!(agg_a.target_integration_run_id, "run_job_agg_target_1");
    assert_eq!(agg_a.job_id, "job_multi_worker_1");
    assert!(!agg_a.cryptographically_proves_user_authority);
    assert!(!agg_a.native_chat_identity_verified);
    assert!(!agg_a.grants_main_merge_or_push_authority);
    rcp_a
        .verify_against_job_scope(
            &task_a.subject,
            &task_a.review_receipt,
            &job_scope,
            &task_a.task_id,
            &task_a.task_digest,
        )
        .expect("verify rcp_a against job scope");
    rcp_b
        .verify_against_job_scope(
            &task_b.subject,
            &task_b.review_receipt,
            &job_scope,
            &task_b.task_id,
            &task_b.task_digest,
        )
        .expect("verify rcp_b against job scope");

    // 4. Inspect final combined candidate across the distinct predecessor original run IDs.
    let candidate_subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.target_integration_run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: Some(report.final_integrated_target_commit.clone()),
        expected_candidate_tree_sha: Some(report.final_integrated_tree_sha.clone()),
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: task_b.task_id.clone(),
                task_digest: task_b.task_digest.clone(),
                task_worktree_id: task_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: rcp_b.receipt_id.clone(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(
                    task_b.review_receipt.allowed_write_paths.clone(),
                ),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
            FinalCandidateTaskInput {
                task_id: task_a.task_id.clone(),
                task_digest: task_a.task_digest.clone(),
                task_worktree_id: task_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: rcp_a.receipt_id.clone(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(
                    task_a.review_receipt.allowed_write_paths.clone(),
                ),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
        ],
        expected_aggregate_write_paths: Some(job_scope.aggregate_allowed_write_paths.clone()),
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence(
            "combined candidate fmt, clippy, and tests passed",
            40,
        ),
    })
    .expect("inspect_final_candidate_readonly across distinct worker runs");

    assert_eq!(candidate_subject.run_id, "run_job_agg_target_1");
    assert_eq!(candidate_subject.predecessors.len(), 2);
    assert_eq!(candidate_subject.predecessors[0].task_id, "task_alpha");
    assert_eq!(
        candidate_subject.predecessors[0].run_id,
        "run_worker_alpha_1"
    );
    assert_eq!(candidate_subject.predecessors[1].task_id, "task_beta");
    assert_eq!(
        candidate_subject.predecessors[1].run_id,
        "run_worker_beta_2"
    );
    assert_eq!(
        candidate_subject
            .changed_paths
            .iter()
            .map(|p| p.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/alpha.rs", "src/beta.rs"]
    );

    verify_final_candidate_subject_readonly(&candidate_subject)
        .expect("verify_final_candidate_subject_readonly");
    verify_legacy_base_closure_insufficient(&fixture.approved_base_tree_sha, &candidate_subject)
        .expect("verify_legacy_base_closure_insufficient");

    // 5. Complete verified closure attempt lifecycle on the aggregated FinalCandidateSubject.
    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open closure store");
    let res = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_closure_job_1".to_string(),
            attempt_id: Some("cat_job_1".to_string()),
            subject: candidate_subject.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 3_000,
        })
        .expect("reserve closure attempt");
    assert_eq!(res.ledger_revision(), 1);
    assert_eq!(res.attempt().state, ClosureAttemptState::Reserved);

    let (rev2, _) = store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_job_1".to_string(),
            command_key: "cmd_closure_job_1".to_string(),
            expected_subject_digest: candidate_subject.subject_digest.clone(),
            external_request_id: "req_closure_1".to_string(),
            submitted_at_ms: 3_100,
        })
        .expect("mark closure submitted");
    assert_eq!(rev2, 2);

    let (rev3, _, verdict) = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 2,
            attempt_id: "cat_job_1".to_string(),
            command_key: "cmd_closure_job_1".to_string(),
            verdict_id: Some("cvd_job_1".to_string()),
            subject: candidate_subject.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "Aggregated multi-worker candidate verified and accepted.".to_string(),
            execution_evidence: default_execution_evidence("hnd_job_closure_1"),
            recorded_at_ms: 3_200,
        })
        .expect("record closure verdict");
    assert_eq!(rev3, 3);
    store
        .verify_accepted_closure_verdict_readonly(&candidate_subject, &verdict)
        .expect("verify accepted closure verdict");

    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn duplicate_job_integration_reuses_persisted_receipts_without_new_commits_and_rejects_conflicting_key_or_mutated_receipt(
) {
    let fixture = init_job_integration_fixture();

    let task_a = create_and_accept_worker_task(
        &fixture,
        "run_worker_alpha_1",
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    11\n}\n",
        1_000,
    );
    let task_b = create_and_accept_worker_task(
        &fixture,
        "run_worker_beta_2",
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    22\n}\n",
        2_000,
    );

    let job_scope = default_job_scope(&fixture.target_integration_run_id);
    let request = ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![
            to_job_task_input(
                &task_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_dup_alpha"),
            ),
            to_job_task_input(
                &task_b,
                IntegrationStrategy::MergeNoFf,
                Some("int_dup_beta"),
            ),
        ],
        command_key: "cmd_dup_1".to_string(),
        intent_id: Some("jint_dup_1".to_string()),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_500,
    };

    let report1 = execute_job_integration(&request).expect("first execute_job_integration");
    assert!(!report1.reused_existing_receipts);
    let count1 = run_sanitized_git(
        &fixture.integration_manifest.checkout_path,
        &["rev-list", "--count", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("rev-list count after first integration");

    // 1. Exact duplicate call with the same command_key reuses persisted receipts without new commits.
    let report2 = execute_job_integration(&request).expect("duplicate execute_job_integration");
    assert!(report2.reused_existing_receipts);
    assert_eq!(
        report2.final_integrated_target_commit,
        report1.final_integrated_target_commit
    );
    assert_eq!(report2.receipts, report1.receipts);
    let count2 = run_sanitized_git(
        &fixture.integration_manifest.checkout_path,
        &["rev-list", "--count", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("rev-list count after second integration");
    assert_eq!(count1.trim(), count2.trim());

    // 2. Duplicate call with a different command_key for the identical completed batch also reuses receipts.
    let mut request_new_key = request.clone();
    request_new_key.command_key = "cmd_dup_2".to_string();
    let report3 =
        execute_job_integration(&request_new_key).expect("duplicate payload with new command_key");
    assert!(report3.reused_existing_receipts);
    assert_eq!(report3.receipts, report1.receipts);

    // 3. Reusing the original command_key with a different payload (e.g. different job_revision) is rejected.
    let mut conflicting_key_req = request.clone();
    conflicting_key_req.job_scope.job_revision = 2;
    let conflict_key_err = execute_job_integration(&conflicting_key_req)
        .expect_err("reusing command_key with different payload must fail");
    assert!(matches!(
        conflict_key_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // 4. Mutating a persisted IntegrationReceipt on disk causes both duplicate reuse and candidate inspection to fail closed.
    let receipt_path = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join("receipts")
        .join("integration-int_dup_beta.json");
    let raw_json = fs::read_to_string(&receipt_path).expect("read integration receipt JSON");
    let mutated_json = raw_json.replace("\"job_multi_worker_1\"", "\"job_tampered_99\"");
    assert_ne!(raw_json, mutated_json);
    fs::write(&receipt_path, mutated_json).expect("write mutated receipt JSON");

    let mutated_reuse_err = execute_job_integration(&request)
        .expect_err("mutated integration receipt on disk must fail duplicate reuse");
    assert!(matches!(
        mutated_reuse_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    let mutated_candidate_err = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.target_integration_run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: task_a.task_id.clone(),
                task_digest: task_a.task_digest.clone(),
                task_worktree_id: task_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_dup_alpha".to_string(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
            FinalCandidateTaskInput {
                task_id: task_b.task_id.clone(),
                task_digest: task_b.task_digest.clone(),
                task_worktree_id: task_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_dup_beta".to_string(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
        ],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("passed", 20),
    })
    .expect_err("mutated integration receipt on disk must fail final candidate inspection");
    assert!(matches!(
        mutated_candidate_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn job_integration_rejects_unrelated_job_scope_repo_base_wrong_target_run_stale_review_changed_heads_conflict_and_unaccepted_task(
) {
    let fixture = init_job_integration_fixture();

    let task_a = create_and_accept_worker_task(
        &fixture,
        "run_worker_alpha_1",
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    10\n}\n",
        1_000,
    );
    let task_b = create_and_accept_worker_task(
        &fixture,
        "run_worker_beta_2",
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    20\n}\n",
        2_000,
    );

    let valid_request = ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: default_job_scope(&fixture.target_integration_run_id),
        tasks: vec![
            to_job_task_input(&task_a, IntegrationStrategy::FastForwardOnly, None),
            to_job_task_input(&task_b, IntegrationStrategy::MergeNoFf, None),
        ],
        command_key: "cmd_reject_check".to_string(),
        intent_id: None,
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_500,
    };

    // 1. Wrong target_integration_run_id -> WorktreeNotOwned.
    let mut wrong_target_req = valid_request.clone();
    wrong_target_req.job_scope.target_integration_run_id = "run_wrong_target_99".to_string();
    let wrong_target_err = execute_job_integration(&wrong_target_req)
        .expect_err("wrong target_integration_run_id must be rejected");
    assert!(matches!(
        wrong_target_err,
        OperationsError::WorktreeNotOwned(_)
    ));

    // 2. Out-of-scope task (job scope only allows src/alpha.rs, but task_b touches src/beta.rs) -> DiffGateRejected.
    let mut narrow_scope_req = valid_request.clone();
    let mut narrow_paths = BTreeSet::new();
    narrow_paths.insert("src/alpha.rs".to_string());
    narrow_scope_req.job_scope.aggregate_allowed_write_paths = narrow_paths;
    let scope_err = execute_job_integration(&narrow_scope_req)
        .expect_err("task outside job aggregate_allowed_write_paths must be rejected");
    assert!(matches!(
        scope_err,
        OperationsError::DiffGateRejected { .. }
    ));

    // 3. Mismatched approved_base_commit -> SubjectMismatch.
    let mut wrong_base_req = valid_request.clone();
    wrong_base_req.approved_base_commit = task_a.review_receipt.task_head_commit.clone();
    let base_err = execute_job_integration(&wrong_base_req)
        .expect_err("mismatched approved_base_commit must be rejected");
    assert!(matches!(base_err, OperationsError::SubjectMismatch(_)));

    // 4. Changed / mismatched expected_source_head_commit, expected_integration_head_commit, expected_task_head_commit -> HeadMoved.
    let mut wrong_src_head_req = valid_request.clone();
    wrong_src_head_req.expected_source_head_commit = task_a.review_receipt.task_head_commit.clone();
    assert!(matches!(
        execute_job_integration(&wrong_src_head_req).expect_err("wrong source head"),
        OperationsError::HeadMoved { .. }
    ));

    let mut wrong_int_head_req = valid_request.clone();
    wrong_int_head_req.expected_integration_head_commit =
        task_a.review_receipt.task_head_commit.clone();
    assert!(matches!(
        execute_job_integration(&wrong_int_head_req).expect_err("wrong integration head"),
        OperationsError::HeadMoved { .. }
    ));

    let mut wrong_task_head_req = valid_request.clone();
    wrong_task_head_req.tasks[0].expected_task_head_commit = fixture.approved_base_commit.clone();
    assert!(matches!(
        execute_job_integration(&wrong_task_head_req).expect_err("wrong task head"),
        OperationsError::HeadMoved { .. }
    ));

    // 5. Foreign repository -> WorktreeNotOwned.
    let foreign_temp = TempDir::new().expect("foreign tempdir");
    let foreign_repo = foreign_temp.path().join("foreign-repo");
    fs::create_dir_all(&foreign_repo).expect("create foreign repo");
    run_sanitized_git(
        &foreign_repo,
        &["init", "-b", "main"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init foreign");
    fs::write(foreign_repo.join("README.md"), "foreign\n").expect("write foreign");
    run_sanitized_git(&foreign_repo, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("git add foreign");
    run_sanitized_git(
        &foreign_repo,
        &[
            "-c",
            "user.name=Foreign",
            "-c",
            "user.email=foreign@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "foreign init",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit foreign");
    let canonical_foreign = fs::canonicalize(&foreign_repo).expect("canonical foreign");
    let foreign_head =
        resolve_exact_commit_sha(&canonical_foreign, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("foreign head");
    let mut foreign_req = valid_request.clone();
    foreign_req.repository_path = canonical_foreign;
    foreign_req.expected_source_head_commit = foreign_head;
    assert!(matches!(
        execute_job_integration(&foreign_req).expect_err("foreign repo must fail"),
        OperationsError::WorktreeNotOwned(_)
    ));

    // 6. Unaccepted task (persisted review receipt absent from disk) -> ReceiptInvalidated.
    let rev_a_path = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join("receipts")
        .join(format!("review-{}.json", task_a.review_receipt.receipt_id));
    let saved_rev_a = fs::read(&rev_a_path).expect("read rev_a");
    fs::remove_file(&rev_a_path).expect("remove rev_a from disk");
    let unaccepted_err = execute_job_integration(&valid_request)
        .expect_err("missing persisted review receipt must be rejected");
    assert!(matches!(
        unaccepted_err,
        OperationsError::ReceiptInvalidated(_)
    ));
    fs::write(&rev_a_path, &saved_rev_a).expect("restore rev_a");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&rev_a_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 restored rev_a");
    }

    // 7. Superseded review receipt -> ReceiptInvalidated.
    let newer_rev_a = accept_review_subject(&ReviewAcceptanceRequest {
        subject: task_a.subject.clone(),
        expected_subject_digest: task_a.subject.subject_digest.clone(),
        reviewer_id: "op_primary".to_string(),
        review_note: "Newer superseding approval".to_string(),
        approved_at_ms: 1_500,
        receipt_id: Some("rev_task_alpha_newer".to_string()),
    })
    .expect("create superseding review receipt");
    let superseded_err = execute_job_integration(&valid_request)
        .expect_err("superseded review receipt must be rejected");
    assert!(matches!(
        superseded_err,
        OperationsError::ReceiptInvalidated(_)
    ));
    let newer_rev_a_path = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join("receipts")
        .join(format!("review-{}.json", newer_rev_a.receipt_id));
    fs::remove_file(&newer_rev_a_path).expect("remove newer_rev_a");

    // 8. Merge conflict between two worker tasks touching the same file -> refused before mutating RunIntegration.
    let task_conflict = create_and_accept_worker_task(
        &fixture,
        "run_worker_conflict_3",
        "task_conflict",
        "t03",
        "cc",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    999\n}\n",
        2_100,
    );
    let mut conflict_req = valid_request.clone();
    conflict_req.tasks = vec![
        to_job_task_input(&task_a, IntegrationStrategy::FastForwardOnly, None),
        to_job_task_input(&task_conflict, IntegrationStrategy::MergeNoFf, None),
    ];
    let conflict_err = execute_job_integration(&conflict_req)
        .expect_err("conflicting tasks must be refused before mutating RunIntegration");
    assert!(matches!(conflict_err, OperationsError::MergeConflict(_)));
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.integration_manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("RunIntegration HEAD after conflict refusal"),
        fixture.approved_base_commit,
        "RunIntegration HEAD must remain untouched at approved_base_commit when merge conflict is refused"
    );

    // 9. Stale review receipt (task worktree HEAD moves after review receipt was issued) -> ReceiptInvalidated.
    let task_stale = create_and_accept_worker_task(
        &fixture,
        "run_worker_stale_4",
        "task_stale",
        "t04",
        "dd",
        "src/gamma.rs",
        "pub fn gamma() -> u32 {\n    30\n}\n",
        2_200,
    );
    fs::write(
        task_stale.task_manifest.checkout_path.join("src/gamma.rs"),
        "pub fn gamma() -> u32 {\n    300\n}\n",
    )
    .expect("modify task_stale after review");
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_stale.task_manifest.worktree_id.clone(),
        commit_message: "unreviewed post-acceptance commit".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit post-review modification");

    let mut stale_req = valid_request.clone();
    stale_req
        .job_scope
        .aggregate_allowed_write_paths
        .insert("src/gamma.rs".to_string());
    stale_req.tasks = vec![to_job_task_input(
        &task_stale,
        IntegrationStrategy::FastForwardOnly,
        None,
    )];
    let stale_err = execute_job_integration(&stale_req)
        .expect_err("stale review receipt after task HEAD moved must fail");
    assert!(matches!(
        stale_err,
        OperationsError::ReceiptInvalidated(_) | OperationsError::HeadMoved { .. }
    ));

    // 10. Valid integration succeeds, and inspect_final_candidate_readonly rejects unrelated approval_id.
    let report = execute_job_integration(&valid_request).expect("valid job integration");
    let mut unrelated_policy = default_candidate_policy();
    unrelated_policy.approval_id = "app_unrelated_job_99".to_string();
    let unrelated_err = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.target_integration_run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: Some(report.final_integrated_target_commit),
        expected_candidate_tree_sha: Some(report.final_integrated_tree_sha),
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: task_a.task_id.clone(),
                task_digest: task_a.task_digest.clone(),
                task_worktree_id: task_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: report.receipts[0].receipt_id.clone(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
            FinalCandidateTaskInput {
                task_id: task_b.task_id.clone(),
                task_digest: task_b.task_digest.clone(),
                task_worktree_id: task_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: report.receipts[1].receipt_id.clone(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
        ],
        expected_aggregate_write_paths: None,
        policy: unrelated_policy,
        check_evidence: default_test_evidence("passed", 20),
    })
    .expect_err("unrelated approval_id in candidate policy must be rejected");
    assert!(matches!(
        unrelated_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn unknown_or_interrupted_job_integration_intent_blocks_blind_replay_and_recovers_only_when_actual_receipts_exist(
) {
    // Part 1: Interrupted InProgress intent without persisted receipts transitions to OutcomeUnknown
    // and blocks replay across all command keys.
    let fixture = init_job_integration_fixture();
    let task_a = create_and_accept_worker_task(
        &fixture,
        "run_worker_alpha_1",
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    55\n}\n",
        1_000,
    );
    let task_b = create_and_accept_worker_task(
        &fixture,
        "run_worker_beta_2",
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    66\n}\n",
        2_000,
    );

    let request = ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: default_job_scope(&fixture.target_integration_run_id),
        tasks: vec![
            to_job_task_input(
                &task_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_unk_alpha"),
            ),
            to_job_task_input(
                &task_b,
                IntegrationStrategy::MergeNoFf,
                Some("int_unk_beta"),
            ),
        ],
        command_key: "cmd_unknown_1".to_string(),
        intent_id: Some("jint_unknown_1".to_string()),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_500,
    };

    let recorded_intent =
        record_job_integration_intent_before_effect(&request).expect("record intent before effect");
    assert_eq!(recorded_intent.state, JobIntegrationIntentState::InProgress);

    let inspected = inspect_job_integration_intent_readonly(
        &fixture.work_area_root,
        &fixture.integration_manifest.worktree_id,
    )
    .expect("inspect intent readonly")
    .expect("intent must exist");
    assert_eq!(inspected.state, JobIntegrationIntentState::InProgress);

    // Calling execute_job_integration when an InProgress intent has no completed receipts on disk
    // must NOT blindly replay; it transitions the intent to OutcomeUnknown and fails closed.
    let replay_err = execute_job_integration(&request)
        .expect_err("incomplete InProgress intent must not be blindly replayed");
    assert!(matches!(
        replay_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));

    let after_unknown = inspect_job_integration_intent_readonly(
        &fixture.work_area_root,
        &fixture.integration_manifest.worktree_id,
    )
    .expect("inspect intent after unknown")
    .expect("intent must exist");
    assert_eq!(
        after_unknown.state,
        JobIntegrationIntentState::OutcomeUnknown
    );

    // A new command_key cannot bypass OutcomeUnknown either.
    let mut bypass_req = request.clone();
    bypass_req.command_key = "cmd_unknown_bypass_2".to_string();
    let bypass_err = execute_job_integration(&bypass_req)
        .expect_err("new command_key must not bypass OutcomeUnknown intent");
    assert!(matches!(
        bypass_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.integration_manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .expect("RunIntegration HEAD"),
        fixture.approved_base_commit
    );
    assert_primary_checkout_untouched(&fixture);

    // Part 2: If all actual IntegrationReceipts were persisted and RunIntegration HEAD matches,
    // an InProgress intent is deterministically recovered to Completed without creating new commits.
    let fixture2 = init_job_integration_fixture();
    let task2_a = create_and_accept_worker_task(
        &fixture2,
        "run_worker_alpha_1",
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    77\n}\n",
        1_000,
    );
    let task2_b = create_and_accept_worker_task(
        &fixture2,
        "run_worker_beta_2",
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    88\n}\n",
        2_000,
    );
    let req2 = ExecuteJobIntegrationRequest {
        repository_path: fixture2.repo_path.clone(),
        work_area_root: fixture2.work_area_root.clone(),
        integration_worktree_id: fixture2.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture2.approved_base_commit.clone(),
        expected_source_head_commit: fixture2.approved_base_commit.clone(),
        expected_integration_head_commit: fixture2.approved_base_commit.clone(),
        job_scope: default_job_scope(&fixture2.target_integration_run_id),
        tasks: vec![
            to_job_task_input(
                &task2_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_rec_alpha"),
            ),
            to_job_task_input(
                &task2_b,
                IntegrationStrategy::MergeNoFf,
                Some("int_rec_beta"),
            ),
        ],
        command_key: "cmd_recover_1".to_string(),
        intent_id: Some("jint_recover_1".to_string()),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 2_500,
    };

    let completed_report = execute_job_integration(&req2).expect("initial execute_job_integration");
    assert!(!completed_report.reused_existing_receipts);

    // Simulate a crash right before the final intent state transition by writing the InProgress
    // intent while the actual merged commits and IntegrationReceipt files remain on disk.
    let mut simulated_in_progress = completed_report.intent.clone();
    simulated_in_progress.state = JobIntegrationIntentState::InProgress;
    simulated_in_progress.final_integrated_target_commit = None;
    simulated_in_progress.final_integrated_tree_sha = None;
    simulated_in_progress.intent_digest = simulated_in_progress
        .compute_digest()
        .expect("compute_digest");
    let intent_path = fixture2
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join("job-integrations")
        .join(format!(
            "job-integration-intent-{}.json",
            fixture2.integration_manifest.worktree_id
        ));
    fs::write(
        &intent_path,
        serde_json::to_vec_pretty(&simulated_in_progress).expect("serialize intent"),
    )
    .expect("write simulated InProgress intent");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&intent_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 intent_path");
    }

    // While the intent on disk is InProgress, inspect_final_candidate_readonly refuses until recovered.
    let candidate_blocked_err = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture2.repo_path.clone(),
        work_area_root: fixture2.work_area_root.clone(),
        run_id: fixture2.target_integration_run_id.clone(),
        integration_worktree_id: fixture2.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture2.approved_base_commit.clone(),
        expected_source_head_commit: fixture2.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: task2_a.task_id.clone(),
                task_digest: task2_a.task_digest.clone(),
                task_worktree_id: task2_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task2_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_rec_alpha".to_string(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
            FinalCandidateTaskInput {
                task_id: task2_b.task_id.clone(),
                task_digest: task2_b.task_digest.clone(),
                task_worktree_id: task2_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task2_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_rec_beta".to_string(),
                expected_execution_base_commit: None,
                expected_allowed_write_paths: None,
                expected_provider: None,
                expected_model: None,
                expected_effort: None,
            },
        ],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("passed", 20),
    })
    .expect_err("unresolved InProgress intent must block candidate freeze");
    assert!(matches!(
        candidate_blocked_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));

    // Re-invoking execute_job_integration verifies the persisted receipts and live HEAD and recovers to Completed.
    let recovered_report =
        execute_job_integration(&req2).expect("recover completed receipts from InProgress intent");
    assert!(recovered_report.reused_existing_receipts);
    assert_eq!(
        recovered_report.intent.state,
        JobIntegrationIntentState::Completed
    );
    assert_eq!(
        recovered_report.final_integrated_target_commit,
        completed_report.final_integrated_target_commit
    );
    assert_eq!(recovered_report.receipts, completed_report.receipts);
    assert_primary_checkout_untouched(&fixture2);
}

#[test]
fn three_stage_dag_root_a_root_b_and_sealed_dependent_c_aggregates_and_verifies_candidate_closure()
{
    let fixture = init_job_integration_fixture();

    // Stage 1a: Root worker A (`rtag_alpha_1` / compact manifest tag `wt_alpha`) modifies `src/alpha.rs` and is accepted.
    // Its logical plan task ID (`plan_task_alpha`) and host runtime run ID (`run_550e8400-e29b-41d4-a716-446655440000`)
    // are intentionally distinct from the worktree manifest tags (`rtag_alpha_1` / `wt_alpha`).
    let mut task_a = create_and_accept_worker_task(
        &fixture,
        "rtag_alpha_1",
        "wt_alpha",
        "tokA1",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    10\n}\n",
        1_710_100_001_000,
    );
    let logical_a_id = "plan_task_alpha";
    let runtime_run_a = "run_550e8400-e29b-41d4-a716-446655440000";
    let worker_id_a = "wrk_550e8400-e29b-41d4-a716-446655440000";
    task_a.task_id = logical_a_id.to_string();
    assert_ne!(
        Some(task_a.task_id.as_str()),
        task_a.task_manifest.task_id.as_deref(),
        "logical plan task_id must be distinct from compact worktree manifest task_id"
    );
    assert_ne!(
        runtime_run_a, task_a.original_run_id,
        "host runtime_run_id must be distinct from compact worktree manifest run_id"
    );

    // Dual-receipt strategy step 1: Create A's own same-run integration worktree and same-run IntegrationReceipt
    // so downstream dependent sealing (`verify_dependency_predecessor_proof` + `prepare_and_seal_dependency_input`)
    // works through the existing unchanged dependency API.
    let root_manager = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
    )
    .expect("root WorktreeManager");
    let run_a_int_manifest = root_manager
        .create_run_worktree_with_token(&task_a.original_run_id, "intA1")
        .expect("create run A same-run integration worktree");
    let same_run_receipt_a = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: run_a_int_manifest.worktree_id.clone(),
        task_worktree_id: task_a.task_manifest.worktree_id.clone(),
        subject: task_a.subject.clone(),
        review_receipt: task_a.review_receipt.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        expected_task_head_commit: task_a.review_receipt.task_head_commit.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        operator_id: "op_controller".to_string(),
        integrated_at_ms: 1_710_100_001_500,
        receipt_id: Some("int_samerun_alpha".to_string()),
    })
    .expect("execute same-run integration for root A");
    assert!(same_run_receipt_a.job_aggregation.is_none());
    assert_eq!(same_run_receipt_a.task_id, "wt_alpha");
    assert_eq!(same_run_receipt_a.run_id, "rtag_alpha_1");

    // Stage 1b: Independent root worker B (`run_worker_beta_2` / `task_beta`) modifies `src/beta.rs` at root base.
    let task_b = create_and_accept_worker_task(
        &fixture,
        "run_worker_beta_2",
        "task_beta",
        "tokB1",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    20\n}\n",
        1_710_100_002_000,
    );

    // Stage 2: Prepare and seal dependent task C (`plan_task_gamma` in `rtag_gamma_3` / `wt_gamma`) depending on root A.
    let pred_proof_a = verify_dependency_predecessor_proof(&DependencyPredecessorCandidate {
        logical_task_id: task_a.task_id.clone(),
        logical_task_digest: task_a.task_digest.clone(),
        runtime_run_id: runtime_run_a.to_string(),
        worker_id: worker_id_a.to_string(),
        integration_worktree_id: run_a_int_manifest.worktree_id.clone(),
        task_worktree_id: task_a.task_manifest.worktree_id.clone(),
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_execution_base_commit: fixture.approved_base_commit.clone(),
        allowed_write_paths: task_a.subject.allowed_write_paths.clone(),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: task_a.review_receipt.receipt_id.clone(),
        integration_receipt_id: same_run_receipt_a.receipt_id.clone(),
    })
    .expect("verify predecessor proof for root A");
    assert_eq!(pred_proof_a.task_id, logical_a_id);
    assert_eq!(pred_proof_a.runtime_run_id, runtime_run_a);

    let mut allowed_c = BTreeSet::new();
    allowed_c.insert("src/gamma.rs".to_string());
    let task_c_id = "plan_task_gamma";
    let task_c_manifest_tag = "wt_gamma";
    let task_c_digest = "cc".repeat(32);
    let instruction_c = "Implement gamma() building on sealed predecessor alpha()";
    let instruction_c_digest = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(instruction_c.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let seal_c_path = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(DEPENDENCIES_SUBDIR)
        .join(format!("{task_c_id}.json"));
    let dep_policy_c = DependencyInputPolicy {
        approval_id: "app_cycle_1".to_string(),
        approval_subject_digest: "22".repeat(32),
        task_id: task_c_id.to_string(),
        task_digest: task_c_digest.clone(),
        instruction_digest: instruction_c_digest,
        seal_path: seal_c_path.clone(),
        command_key: "cmd_prep_dep_gamma_1".to_string(),
    };

    let seal_c = prepare_and_seal_dependency_input(
        &dep_policy_c,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
        instruction_c,
        vec![pred_proof_a],
    )
    .expect("prepare and seal dependency input for dependent task C");
    assert_ne!(
        seal_c.execution_base_commit, fixture.approved_base_commit,
        "sealed execution_base_commit for dependent C must differ from root approved_base_commit"
    );

    // Stage 3: Create dependent worker C's actual task worktree at `seal_c.execution_base_commit`.
    let dep_manager_c = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &seal_c.execution_base_commit,
    )
    .expect("dependent WorktreeManager at sealed execution base");
    let task_c_manifest = dep_manager_c
        .create_task_worktree_with_token("rtag_gamma_3", task_c_manifest_tag, "tokC1", &allowed_c)
        .expect("create dependent task C worktree at sealed execution_base_commit");
    assert_eq!(task_c_manifest.base_commit, seal_c.execution_base_commit);
    assert_ne!(
        Some(task_c_id),
        task_c_manifest.task_id.as_deref(),
        "dependent C logical task_id must be distinct from compact worktree manifest task_id"
    );
    // Verify C's worktree already sees predecessor A's committed `src/alpha.rs` from the sealed base!
    assert_eq!(
        fs::read_to_string(task_c_manifest.checkout_path.join("src/alpha.rs"))
            .expect("read predecessor alpha.rs in C checkout"),
        "pub fn alpha() -> u32 {\n    10\n}\n"
    );
    // C modifies only its own allowed scope (`src/gamma.rs`).
    fs::write(
        task_c_manifest.checkout_path.join("src/gamma.rs"),
        "pub fn gamma() -> u32 {\n    crate::alpha::alpha() + 30\n}\n",
    )
    .expect("write gamma.rs in C worktree");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_c_manifest.worktree_id.clone(),
        commit_message: "Implement task_gamma on sealed predecessor base".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit dependent C changes");

    let subject_c = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_c_manifest.worktree_id.clone(),
        policy: default_task_policy(),
        test_evidence: default_test_evidence("cargo fmt, clippy, and DAG tests passed", 24),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect dependent C review subject");
    assert_eq!(subject_c.base_commit, seal_c.execution_base_commit);

    let review_receipt_c = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject_c.clone(),
        expected_subject_digest: subject_c.subject_digest.clone(),
        reviewer_id: "op_primary".to_string(),
        review_note: "Approved dependent task_gamma".to_string(),
        approved_at_ms: 1_710_100_003_000,
        receipt_id: Some("rev_task_gamma_tokC1".to_string()),
    })
    .expect("accept dependent C review subject");
    assert_eq!(review_receipt_c.base_commit, seal_c.execution_base_commit);

    // Also execute same-run legacy integration for C in C's own run worktree to verify dual-receipt coexistence.
    let run_c_int_manifest = dep_manager_c
        .create_run_worktree_with_token("rtag_gamma_3", "intC1")
        .expect("create run C same-run integration worktree");
    let same_run_receipt_c = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: run_c_int_manifest.worktree_id.clone(),
        task_worktree_id: task_c_manifest.worktree_id.clone(),
        subject: subject_c.clone(),
        review_receipt: review_receipt_c.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: seal_c.execution_base_commit.clone(),
        expected_task_head_commit: review_receipt_c.task_head_commit.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        operator_id: "op_controller".to_string(),
        integrated_at_ms: 1_710_100_003_500,
        receipt_id: Some("int_samerun_gamma".to_string()),
    })
    .expect("execute same-run integration for dependent C");
    assert!(same_run_receipt_c.job_aggregation.is_none());

    let mut job_scope = default_job_scope(&fixture.target_integration_run_id);
    job_scope
        .aggregate_allowed_write_paths
        .insert("src/gamma.rs".to_string());

    let dep_proof_c = JobTaskDependencyExecutionProof {
        policy: dep_policy_c.clone(),
        instruction: instruction_c.to_string(),
    };
    let task_c_input = JobIntegrationTaskInput {
        original_run_id: "rtag_gamma_3".to_string(),
        task_id: task_c_id.to_string(),
        task_digest: task_c_digest.clone(),
        task_worktree_id: task_c_manifest.worktree_id.clone(),
        subject: subject_c.clone(),
        review_receipt: review_receipt_c.clone(),
        expected_task_head_commit: review_receipt_c.task_head_commit.clone(),
        dependency_input: Some(dep_proof_c.clone()),
        strategy: IntegrationStrategy::MergeNoFf,
        receipt_id: Some("int_job_gamma".to_string()),
    };

    // Negative check 1: Missing `dependency_input` proof for non-root task C must fail closed.
    let mut missing_proof_c = task_c_input.clone();
    missing_proof_c.dependency_input = None;
    let missing_proof_err = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![
            to_job_task_input(
                &task_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_job_alpha"),
            ),
            to_job_task_input(
                &task_b,
                IntegrationStrategy::MergeNoFf,
                Some("int_job_beta"),
            ),
            missing_proof_c,
        ],
        command_key: "cmd_missing_dep_proof".to_string(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_710_100_004_000,
        intent_id: None,
    })
    .expect_err("non-root task without dependency_input proof must be rejected");
    assert!(matches!(
        missing_proof_err,
        OperationsError::SubjectMismatch(_)
    ));

    // Negative check 2: Wrong task_digest or instruction in dependency proof must fail closed.
    let mut bad_inst_c = task_c_input.clone();
    bad_inst_c
        .dependency_input
        .as_mut()
        .expect("dep_input")
        .instruction = "tampered instruction".to_string();
    let bad_inst_err = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![
            to_job_task_input(
                &task_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_job_alpha"),
            ),
            bad_inst_c,
        ],
        command_key: "cmd_bad_inst_dep_proof".to_string(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_710_100_004_100,
        intent_id: None,
    })
    .expect_err("tampered instruction in dependency proof must be rejected");
    assert!(matches!(
        bad_inst_err,
        OperationsError::InvalidInput(_) | OperationsError::SubjectMismatch(_)
    ));

    // Negative check 2b: Tampered predecessor logical task_id or task_digest in batch must fail closed.
    let mut tampered_pred_a = to_job_task_input(
        &task_a,
        IntegrationStrategy::FastForwardOnly,
        Some("int_job_alpha"),
    );
    tampered_pred_a.task_digest = "ee".repeat(32);
    let tampered_pred_digest_err = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![tampered_pred_a, task_c_input.clone()],
        command_key: "cmd_tampered_pred_digest".to_string(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_710_100_004_150,
        intent_id: None,
    })
    .expect_err("tampered predecessor task_digest must be rejected against DependencyInputSeal");
    assert!(matches!(
        tampered_pred_digest_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // Negative check 3: Omitting sealed predecessor A from the job integration batch must fail closed.
    let missing_pred_err = execute_job_integration(&ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![
            to_job_task_input(
                &task_b,
                IntegrationStrategy::FastForwardOnly,
                Some("int_job_beta"),
            ),
            task_c_input.clone(),
        ],
        command_key: "cmd_missing_pred_in_batch".to_string(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_710_100_004_200,
        intent_id: None,
    })
    .expect_err("dependent task whose sealed predecessor is missing from batch must be rejected");
    assert!(matches!(
        missing_pred_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // Negative check 4: Tampering with persisted DependencyInputSeal on disk must fail closed.
    let original_seal_bytes = fs::read(&seal_c_path).expect("read seal_c_path");
    let mut tampered_seal_json: serde_json::Value =
        serde_json::from_slice(&original_seal_bytes).expect("parse seal json");
    tampered_seal_json["seal_digest"] = serde_json::Value::String("ff".repeat(32));
    fs::write(
        &seal_c_path,
        serde_json::to_vec_pretty(&tampered_seal_json).expect("serialize tampered seal"),
    )
    .expect("write tampered seal");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&seal_c_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 seal_c_path");
    }
    let tampered_seal_err = propose_job_integration(&ProposeJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: vec![
            to_job_task_input(
                &task_a,
                IntegrationStrategy::FastForwardOnly,
                Some("int_job_alpha"),
            ),
            to_job_task_input(
                &task_b,
                IntegrationStrategy::MergeNoFf,
                Some("int_job_beta"),
            ),
            task_c_input.clone(),
        ],
    });
    assert!(
        tampered_seal_err.is_err(),
        "tampered DependencyInputSeal on disk must be rejected"
    );
    // Restore authentic seal bytes.
    fs::write(&seal_c_path, &original_seal_bytes).expect("restore seal_c_path");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&seal_c_path, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 seal_c_path");
    }

    // Positive 3-stage DAG job aggregation of [A, B, C] into the root-based target RunIntegration worktree.
    let dag_tasks = vec![
        to_job_task_input(
            &task_a,
            IntegrationStrategy::FastForwardOnly,
            Some("int_job_alpha"),
        ),
        to_job_task_input(
            &task_b,
            IntegrationStrategy::MergeNoFf,
            Some("int_job_beta"),
        ),
        task_c_input.clone(),
    ];

    let proposal = propose_job_integration(&ProposeJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: dag_tasks.clone(),
    })
    .expect("propose 3-stage DAG job integration");
    assert!(proposal.all_clean_merges_predicted);
    assert_eq!(
        proposal.task_proposals[0].base_commit,
        fixture.approved_base_commit
    );
    assert_eq!(proposal.task_proposals[0].dependency_seal_digest, None);
    assert_eq!(
        proposal.task_proposals[2].approved_base_commit,
        fixture.approved_base_commit
    );
    assert_eq!(
        proposal.task_proposals[2].base_commit,
        seal_c.execution_base_commit
    );
    assert_eq!(
        proposal.task_proposals[2].dependency_seal_digest.as_deref(),
        Some(seal_c.seal_digest.as_str())
    );

    let exec_req = ExecuteJobIntegrationRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        job_scope: job_scope.clone(),
        tasks: dag_tasks,
        command_key: "cmd_exec_3stage_dag_1".to_string(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_710_100_005_000,
        intent_id: Some("jint_3stage_dag_1".to_string()),
    };
    let report = execute_job_integration(&exec_req).expect("execute 3-stage DAG job integration");
    assert!(!report.reused_existing_receipts);
    assert_eq!(report.receipts.len(), 3);
    assert_eq!(report.receipts[0].task_id, "wt_alpha");
    assert_eq!(
        report.receipts[0]
            .job_aggregation
            .as_ref()
            .expect("agg_a")
            .logical_task_id,
        logical_a_id
    );
    assert_eq!(report.receipts[0].base_commit, fixture.approved_base_commit);
    assert_eq!(report.receipts[1].base_commit, fixture.approved_base_commit);
    assert_eq!(report.receipts[2].task_id, task_c_manifest_tag);
    assert_eq!(
        report.receipts[2].base_commit, seal_c.execution_base_commit,
        "non-root aggregation receipt.base_commit must bind dependent task's actual execution base"
    );
    let agg_c = report.receipts[2]
        .job_aggregation
        .as_ref()
        .expect("job_aggregation on receipt C");
    assert_eq!(agg_c.logical_task_id, task_c_id);
    assert_eq!(agg_c.approved_base_commit, fixture.approved_base_commit);
    assert_eq!(
        agg_c.dependency_seal_digest.as_deref(),
        Some(seal_c.seal_digest.as_str())
    );
    assert_eq!(
        agg_c.dependency_snapshot_ref.as_deref(),
        Some(seal_c.snapshot_ref.as_str())
    );

    // Original same-run integration receipts for A and C remain intact on disk.
    let reloaded_samerun_a =
        load_integration_receipt(&fixture.work_area_root, &same_run_receipt_a.receipt_id)
            .expect("reload original same-run receipt A");
    assert_eq!(reloaded_samerun_a, same_run_receipt_a);
    let reloaded_samerun_c =
        load_integration_receipt(&fixture.work_area_root, &same_run_receipt_c.receipt_id)
            .expect("reload original same-run receipt C");
    assert_eq!(reloaded_samerun_c, same_run_receipt_c);

    // Idempotent replay of the completed 3-stage DAG batch succeeds even after job-aggregation receipts exist.
    let replay_report = execute_job_integration(&exec_req)
        .expect("idempotent replay of 3-stage DAG job integration");
    assert!(replay_report.reused_existing_receipts);
    assert_eq!(replay_report.receipts, report.receipts);

    // Single-final-batch enforcement: attempting a second batch with a different payload (even with a new command_key)
    // must fail closed without overwriting the completed intent.
    let mut second_wave_req = exec_req.clone();
    second_wave_req.command_key = "cmd_second_wave_attempt".to_string();
    second_wave_req.tasks.pop();
    let second_wave_err = execute_job_integration(&second_wave_req)
        .expect_err("second batch with different payload on completed target must be rejected");
    assert!(matches!(
        second_wave_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // Negative check on candidate inspection: tampered logical task_id on root A is rejected.
    let tampered_cand_err = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.target_integration_run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: Some(report.final_integrated_target_commit.clone()),
        expected_candidate_tree_sha: Some(report.final_integrated_tree_sha.clone()),
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: "wt_alpha".to_string(),
                task_digest: task_a.task_digest.clone(),
                task_worktree_id: task_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_job_alpha".to_string(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(task_a.subject.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
            FinalCandidateTaskInput {
                task_id: task_b.task_id.clone(),
                task_digest: task_b.task_digest.clone(),
                task_worktree_id: task_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_job_beta".to_string(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(task_b.subject.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
            FinalCandidateTaskInput {
                task_id: task_c_id.to_string(),
                task_digest: task_c_digest.clone(),
                task_worktree_id: task_c_manifest.worktree_id.clone(),
                review_receipt_id: review_receipt_c.receipt_id.clone(),
                integration_receipt_id: "int_job_gamma".to_string(),
                expected_execution_base_commit: Some(seal_c.execution_base_commit.clone()),
                expected_allowed_write_paths: Some(subject_c.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
        ],
        expected_aggregate_write_paths: Some(job_scope.aggregate_allowed_write_paths.clone()),
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence(
            "cargo fmt, clippy, and combined 3-stage DAG behavioral tests passed",
            42,
        ),
    })
    .expect_err("passing compact manifest tag instead of bound logical_task_id must fail");
    assert!(matches!(
        tampered_cand_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // Final candidate inspection and closure lifecycle succeed with all 3 tasks (including dependent C's sealed base).
    let candidate_subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.target_integration_run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: Some(report.final_integrated_target_commit.clone()),
        expected_candidate_tree_sha: Some(report.final_integrated_tree_sha.clone()),
        predecessor_tasks: vec![
            FinalCandidateTaskInput {
                task_id: task_a.task_id.clone(),
                task_digest: task_a.task_digest.clone(),
                task_worktree_id: task_a.task_manifest.worktree_id.clone(),
                review_receipt_id: task_a.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_job_alpha".to_string(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(task_a.subject.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
            FinalCandidateTaskInput {
                task_id: task_b.task_id.clone(),
                task_digest: task_b.task_digest.clone(),
                task_worktree_id: task_b.task_manifest.worktree_id.clone(),
                review_receipt_id: task_b.review_receipt.receipt_id.clone(),
                integration_receipt_id: "int_job_beta".to_string(),
                expected_execution_base_commit: Some(fixture.approved_base_commit.clone()),
                expected_allowed_write_paths: Some(task_b.subject.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
            FinalCandidateTaskInput {
                task_id: task_c_id.to_string(),
                task_digest: task_c_digest,
                task_worktree_id: task_c_manifest.worktree_id.clone(),
                review_receipt_id: review_receipt_c.receipt_id.clone(),
                integration_receipt_id: "int_job_gamma".to_string(),
                expected_execution_base_commit: Some(seal_c.execution_base_commit.clone()),
                expected_allowed_write_paths: Some(subject_c.allowed_write_paths.clone()),
                expected_provider: Some("antigravity".to_string()),
                expected_model: Some("gemini-4-argon-high".to_string()),
                expected_effort: Some("high".to_string()),
            },
        ],
        expected_aggregate_write_paths: Some(job_scope.aggregate_allowed_write_paths.clone()),
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence(
            "cargo fmt, clippy, and combined 3-stage DAG behavioral tests passed",
            42,
        ),
    })
    .expect("inspect final candidate for 3-stage DAG");

    // Predecessors in FinalCandidateSubject are sorted by logical task_id:
    // ["plan_task_alpha", "plan_task_gamma", "task_beta"] -> index 1 is `plan_task_gamma`!
    assert_eq!(candidate_subject.predecessors.len(), 3);
    let gamma_evidence = candidate_subject
        .predecessors
        .iter()
        .find(|p| p.task_id == task_c_id)
        .expect("find plan_task_gamma in candidate_subject.predecessors");
    assert_eq!(
        gamma_evidence.task_base_commit,
        seal_c.execution_base_commit
    );

    verify_final_candidate_subject_readonly(&candidate_subject)
        .expect("verify_final_candidate_subject_readonly for 3-stage DAG");
    verify_legacy_base_closure_insufficient(&fixture.approved_base_tree_sha, &candidate_subject)
        .expect("verify_legacy_base_closure_insufficient for 3-stage DAG");

    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open closure store");
    let res = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_close_3stage_dag_1".to_string(),
            attempt_id: Some("cat_3stage_dag_1".to_string()),
            subject: candidate_subject.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 1_710_100_006_000,
        })
        .expect("reserve closure attempt for 3-stage DAG");
    assert_eq!(res.ledger_revision(), 1);
    assert_eq!(res.attempt().state, ClosureAttemptState::Reserved);

    let (rev2, _) = store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_3stage_dag_1".to_string(),
            command_key: "cmd_close_3stage_dag_1".to_string(),
            expected_subject_digest: candidate_subject.subject_digest.clone(),
            external_request_id: "req_close_3stage_dag_1".to_string(),
            submitted_at_ms: 1_710_100_007_000,
        })
        .expect("mark closure submitted");
    assert_eq!(rev2, 2);

    let (rev3, _, verdict) = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 2,
            attempt_id: "cat_3stage_dag_1".to_string(),
            command_key: "cmd_close_3stage_dag_1".to_string(),
            verdict_id: Some("cvd_3stage_dag_1".to_string()),
            subject: candidate_subject.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "3-stage DAG candidate accepted".to_string(),
            execution_evidence: default_execution_evidence("hnd_close_3stage_dag_1"),
            recorded_at_ms: 1_710_100_008_000,
        })
        .expect("record closure verdict");
    assert_eq!(rev3, 3);
    store
        .verify_accepted_closure_verdict_readonly(&candidate_subject, &verdict)
        .expect("verify accepted closure verdict for 3-stage DAG");

    assert_primary_checkout_untouched(&fixture);
}
