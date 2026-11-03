use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, execute_integration,
    inspect_final_candidate_readonly, inspect_review_subject,
    verify_final_candidate_subject_readonly, verify_legacy_base_closure_insufficient,
    ClosureAttemptState, ClosureExecutionEvidence, ClosureReservationOutcome,
    ClosureTerminalFailure, ClosureVerdictKind, CommitTaskChangesRequest,
    ExecuteIntegrationRequest, FinalCandidateClosureStore, FinalCandidatePolicyBinding,
    FinalCandidateSubject, FinalCandidateTaskInput, InspectFinalCandidateRequest,
    InspectReviewSubjectRequest, IntegrationReceipt, IntegrationStrategy,
    MarkClosureSubmittedRequest, MarkClosureUnknownRequest, OperationsError,
    ReconcileClosureAttemptRequest, RecordClosureFailureRequest, RecordClosureVerdictRequest,
    ReserveClosureAttemptRequest, ReviewAcceptanceRequest, ReviewPolicyBinding, ReviewReceipt,
    TestEvidenceSummary,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git, WorkerFilePermissions,
    WorkerRunStatus, WorktreeManager, WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    MANIFESTS_SUBDIR,
};

struct CandidateTestFixture {
    _temp: TempDir,
    repo_path: PathBuf,
    work_area_root: PathBuf,
    approved_base_commit: String,
    approved_base_tree_sha: String,
    run_id: String,
    integration_manifest: WorktreeManifest,
}

struct AcceptedIntegratedTask {
    task_id: String,
    task_digest: String,
    task_manifest: WorktreeManifest,
    review_receipt: ReviewReceipt,
    integration_receipt: IntegrationReceipt,
}

fn init_candidate_fixture() -> CandidateTestFixture {
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

    let run_id = "run_cycle_1".to_string();
    let manager =
        WorktreeManager::new(&canonical_repo, &canonical_work_area, &approved_base_commit)
            .expect("WorktreeManager::new");
    let integration_manifest = manager
        .create_run_worktree_with_token(&run_id, "int01")
        .expect("create RunIntegration worktree");

    CandidateTestFixture {
        _temp: temp,
        repo_path: canonical_repo,
        work_area_root: canonical_work_area,
        approved_base_commit,
        approved_base_tree_sha,
        run_id,
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
        observed_model: "gpt-6-astra".to_string(),
        observed_effort: Some("max".to_string()),
        transcript_or_event_digest: "44".repeat(32),
        grants_merge_or_push_authority: false,
        substitutes_for_required_checks: false,
    }
}

#[allow(clippy::too_many_arguments)]
fn create_accept_and_integrate_task(
    fixture: &CandidateTestFixture,
    task_id: &str,
    token: &str,
    task_digest_byte: &str,
    rel_file: &str,
    new_contents: &str,
    strategy: IntegrationStrategy,
    timestamp_ms: i64,
) -> AcceptedIntegratedTask {
    let manager = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
    )
    .expect("WorktreeManager::new");
    let mut allowed = BTreeSet::new();
    allowed.insert(rel_file.to_string());

    let task_manifest = manager
        .create_task_worktree_with_token(&fixture.run_id, task_id, token, &allowed)
        .expect("create task worktree");
    fs::write(task_manifest.checkout_path.join(rel_file), new_contents)
        .expect("write task modification");

    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        commit_message: format!("Implement {task_id}"),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");

    let policy = default_task_policy();
    let test_evidence = default_test_evidence("cargo fmt, clippy, and unit tests passed", 14);
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        policy,
        test_evidence,
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");

    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        reviewer_id: "op_primary".to_string(),
        review_note: format!("Approved {task_id}"),
        approved_at_ms: timestamp_ms,
        receipt_id: Some(format!("rev_{task_id}_{token}")),
    })
    .expect("accept review subject");

    let current_int_head = resolve_exact_commit_sha(
        &fixture.integration_manifest.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve current integration HEAD");

    let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        task_worktree_id: task_manifest.worktree_id.clone(),
        subject,
        review_receipt: review_receipt.clone(),
        strategy,
        expected_integration_head_commit: current_int_head,
        expected_task_head_commit: review_receipt.task_head_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: timestamp_ms + 10,
        receipt_id: Some(format!("int_{task_id}_{token}")),
    })
    .expect("execute integration");

    AcceptedIntegratedTask {
        task_id: task_id.to_string(),
        task_digest: task_digest_byte.repeat(32),
        task_manifest,
        review_receipt,
        integration_receipt,
    }
}

fn to_task_input(task: &AcceptedIntegratedTask) -> FinalCandidateTaskInput {
    FinalCandidateTaskInput {
        task_id: task.task_id.clone(),
        task_digest: task.task_digest.clone(),
        task_worktree_id: task.task_manifest.worktree_id.clone(),
        review_receipt_id: task.review_receipt.receipt_id.clone(),
        integration_receipt_id: task.integration_receipt.receipt_id.clone(),
        expected_execution_base_commit: Some(task.review_receipt.base_commit.clone()),
        expected_allowed_write_paths: Some(task.review_receipt.allowed_write_paths.clone()),
        expected_provider: Some("antigravity".to_string()),
        expected_model: Some("gemini-4-argon-high".to_string()),
        expected_effort: Some("high".to_string()),
    }
}

fn assert_primary_checkout_untouched(fixture: &CandidateTestFixture) {
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
}

#[test]
fn final_candidate_inspect_and_identical_subject_deduplication_cache_hit() {
    let fixture = init_candidate_fixture();
    let task_a = create_accept_and_integrate_task(
        &fixture,
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    10\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );
    let task_b = create_accept_and_integrate_task(
        &fixture,
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    20\n}\n",
        IntegrationStrategy::MergeNoFf,
        2_000,
    );

    // Remove any worktree `.lock` files created during task commit/integration so we can verify
    // that `inspect_final_candidate_readonly` leaves missing `.lock` files absent.
    let int_lock = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join(format!("{}.lock", fixture.integration_manifest.worktree_id));
    let _ = fs::remove_file(&int_lock);
    assert!(!int_lock.exists());

    // Supply predecessors in reverse order `[task_beta, task_alpha]` and forward order `[task_alpha, task_beta]`.
    let subject_reverse = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: Some(
            task_b.integration_receipt.integrated_target_commit.clone(),
        ),
        expected_candidate_tree_sha: Some(task_b.integration_receipt.integrated_tree_sha.clone()),
        predecessor_tasks: vec![to_task_input(&task_b), to_task_input(&task_a)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("workspace fmt, clippy, and tests passed", 42),
    })
    .expect("inspect final candidate (reverse order)");

    assert!(
        !int_lock.exists(),
        "inspect_final_candidate_readonly must not create a missing worktree .lock file"
    );

    let subject_forward = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![to_task_input(&task_a), to_task_input(&task_b)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("workspace fmt, clippy, and tests passed", 42),
    })
    .expect("inspect final candidate (forward order)");

    assert_eq!(subject_reverse, subject_forward);
    assert_eq!(
        subject_forward.subject_digest,
        subject_reverse.subject_digest
    );
    assert_eq!(subject_forward.predecessors.len(), 2);
    assert_eq!(subject_forward.predecessors[0].task_id, "task_alpha");
    assert_eq!(subject_forward.predecessors[1].task_id, "task_beta");
    assert_eq!(
        subject_forward
            .changed_paths
            .iter()
            .map(|p| p.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/alpha.rs", "src/beta.rs"]
    );
    verify_final_candidate_subject_readonly(&subject_forward)
        .expect("verify_final_candidate_subject_readonly");

    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");
    let initial_ledger = store
        .inspect_ledger_readonly()
        .expect("inspect empty ledger");
    assert_eq!(initial_ledger.ledger_revision, 0);
    assert!(initial_ledger.attempts.is_empty());
    assert!(
        !store.closures_dir().exists(),
        "inspect_ledger_readonly must not create a missing closures directory"
    );
    assert!(
        !store.lock_path().exists(),
        "inspect_ledger_readonly must not create a missing lock file"
    );

    // 1. Reserve closure attempt (`ledger_revision` 0 -> 1).
    let res1 = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_closure_1".to_string(),
            attempt_id: Some("cat_attempt_1".to_string()),
            subject: subject_forward.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 3_000,
        })
        .expect("reserve closure attempt");
    assert!(!res1.is_cache_hit());
    assert_eq!(res1.ledger_revision(), 1);
    assert_eq!(res1.attempt().state, ClosureAttemptState::Reserved);

    // 2. Idempotent retry with the same `command_key` returns `IdempotentReserved` at revision 1.
    let res_idem = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_closure_1".to_string(),
            attempt_id: Some("cat_attempt_1".to_string()),
            subject: subject_forward.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 3_005,
        })
        .expect("idempotent reserve retry");
    assert!(matches!(
        res_idem,
        ClosureReservationOutcome::IdempotentReserved {
            ledger_revision: 1,
            ..
        }
    ));

    // 3. Mark attempt `Submitted` (`ledger_revision` 1 -> 2).
    let (rev_after_submit, submitted_record) = store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_attempt_1".to_string(),
            command_key: "cmd_closure_1".to_string(),
            expected_subject_digest: subject_forward.subject_digest.clone(),
            external_request_id: "req_provider_1".to_string(),
            submitted_at_ms: 3_100,
        })
        .expect("mark submitted");
    assert_eq!(rev_after_submit, 2);
    assert_eq!(submitted_record.state, ClosureAttemptState::Submitted);

    // 4. Record terminal `Accepted` verdict (`ledger_revision` 2 -> 3).
    let (rev_after_verdict, terminal_record, verdict) = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 2,
            attempt_id: "cat_attempt_1".to_string(),
            command_key: "cmd_closure_1".to_string(),
            verdict_id: Some("cvd_verdict_1".to_string()),
            subject: subject_forward.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "Final combined candidate verified and accepted.".to_string(),
            execution_evidence: default_execution_evidence("hnd_closure_1"),
            recorded_at_ms: 3_200,
        })
        .expect("record closure verdict");
    assert_eq!(rev_after_verdict, 3);
    assert_eq!(terminal_record.state, ClosureAttemptState::Terminal);
    assert_eq!(verdict.kind, ClosureVerdictKind::Accepted);
    assert!(!verdict.execution_evidence.grants_merge_or_push_authority);
    assert!(!verdict.execution_evidence.substitutes_for_required_checks);

    // 5. Verify accepted verdict against live disk state.
    let verified_record = store
        .verify_accepted_closure_verdict_readonly(&subject_forward, &verdict)
        .expect("verify accepted verdict readonly");
    assert_eq!(verified_record.attempt_id, "cat_attempt_1");

    // 6. Calling `reserve_closure_attempt` for the identical subject with a new command_key returns
    // an explicit `CacheHit` without incrementing `ledger_revision` or creating another attempt!
    let cache_decision = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 3,
            command_key: "cmd_closure_2".to_string(),
            attempt_id: None,
            subject: subject_forward,
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 3_500,
        })
        .expect("cache hit on identical subject");
    assert!(cache_decision.is_cache_hit());
    assert_eq!(cache_decision.cached_verdict(), Some(&verdict));
    match cache_decision {
        ClosureReservationOutcome::CacheHit {
            ledger_revision,
            attempt,
            verdict: cached_verdict,
        } => {
            assert_eq!(ledger_revision, 3);
            assert_eq!(attempt.attempt_id, "cat_attempt_1");
            assert_eq!(*cached_verdict, verdict);
        }
        other => panic!("expected CacheHit, got {other:?}"),
    }

    let final_ledger = store
        .inspect_ledger_readonly()
        .expect("inspect final ledger");
    assert_eq!(final_ledger.ledger_revision, 3);
    assert_eq!(final_ledger.attempts.len(), 1);
    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn final_candidate_invalidates_on_changed_tree_scope_policy_checks_and_predecessor_evidence() {
    let fixture = init_candidate_fixture();
    let task_a = create_accept_and_integrate_task(
        &fixture,
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    10\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );
    let task_b = create_accept_and_integrate_task(
        &fixture,
        "task_beta",
        "t02",
        "bb",
        "src/beta.rs",
        "pub fn beta() -> u32 {\n    20\n}\n",
        IntegrationStrategy::MergeNoFf,
        2_000,
    );

    let base_request = InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![to_task_input(&task_a), to_task_input(&task_b)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("all checks green", 30),
    };

    let subject_v1 =
        inspect_final_candidate_readonly(&base_request).expect("inspect baseline subject_v1");

    // 1. Changed aggregate write scope ceiling -> distinct `subject_digest`.
    let mut wider_scope = BTreeSet::new();
    wider_scope.insert("src/alpha.rs".to_string());
    wider_scope.insert("src/beta.rs".to_string());
    wider_scope.insert("src/gamma.rs".to_string());
    let mut scope_req = base_request.clone();
    scope_req.expected_aggregate_write_paths = Some(wider_scope);
    let subject_wider_scope =
        inspect_final_candidate_readonly(&scope_req).expect("inspect wider scope subject");
    assert_ne!(
        subject_v1.subject_digest,
        subject_wider_scope.subject_digest
    );

    // 2. Changed policy/scope revision -> distinct `subject_digest`.
    let mut policy_req = base_request.clone();
    policy_req.policy.scope_revision = 2;
    let subject_new_scope_rev =
        inspect_final_candidate_readonly(&policy_req).expect("inspect new scope_revision subject");
    assert_ne!(
        subject_v1.subject_digest,
        subject_new_scope_rev.subject_digest
    );

    // 3. Changed check evidence -> distinct `subject_digest`.
    let mut checks_req = base_request.clone();
    checks_req.check_evidence.passed_test_count = 35;
    let subject_new_checks =
        inspect_final_candidate_readonly(&checks_req).expect("inspect new checks subject");
    assert_ne!(subject_v1.subject_digest, subject_new_checks.subject_digest);

    // 4. Changed predecessor task digest -> distinct `subject_digest`.
    let mut pred_req = base_request.clone();
    pred_req.predecessor_tasks[0].task_digest = "99".repeat(32);
    let subject_new_pred =
        inspect_final_candidate_readonly(&pred_req).expect("inspect new predecessor digest");
    assert_ne!(subject_v1.subject_digest, subject_new_pred.subject_digest);

    // Record an Accepted verdict on `subject_v1`.
    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");
    store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_v1".to_string(),
            attempt_id: Some("cat_v1".to_string()),
            subject: subject_v1.clone(),
            legacy_base_tree_digest: None,
            reserved_at_ms: 3_000,
        })
        .expect("reserve v1");
    let (_, _, verdict_v1) = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_v1".to_string(),
            command_key: "cmd_v1".to_string(),
            verdict_id: Some("cvd_v1".to_string()),
            subject: subject_v1.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "Accepted v1".to_string(),
            execution_evidence: default_execution_evidence("hnd_v1"),
            recorded_at_ms: 3_100,
        })
        .expect("record v1 verdict");

    // 5. Integrate a third accepted task `task_gamma` into `RunIntegration`, changing the final candidate tree.
    let task_c = create_accept_and_integrate_task(
        &fixture,
        "task_gamma",
        "t03",
        "cc",
        "src/gamma.rs",
        "pub fn gamma() -> u32 {\n    30\n}\n",
        IntegrationStrategy::MergeNoFf,
        4_000,
    );

    // Omitting `task_gamma` from `predecessor_tasks` must now fail because `RunIntegration` has an
    // unlisted integration receipt and a newer HEAD!
    let stale_err = inspect_final_candidate_readonly(&base_request)
        .expect_err("omitting integrated task_gamma must fail");
    assert!(matches!(stale_err, OperationsError::ReceiptInvalidated(_)));

    // Re-verifying `subject_v1` or `verdict_v1` against live disk must fail because the candidate advanced.
    let verify_old_err = store
        .verify_accepted_closure_verdict_readonly(&subject_v1, &verdict_v1)
        .expect_err("old verdict must not validate after candidate tree changed");
    assert!(matches!(
        verify_old_err,
        OperationsError::ReceiptInvalidated(_) | OperationsError::HeadMoved { .. }
    ));

    // Inspect the updated candidate `subject_v2` including `task_gamma`.
    let mut v2_request = base_request;
    v2_request.predecessor_tasks.push(to_task_input(&task_c));
    let subject_v2 =
        inspect_final_candidate_readonly(&v2_request).expect("inspect updated subject_v2");
    assert_ne!(subject_v1.candidate_tree_sha, subject_v2.candidate_tree_sha);
    assert_ne!(subject_v1.subject_digest, subject_v2.subject_digest);

    // Reserving for `subject_v2` must NOT inherit `verdict_v1`; it reserves a new attempt (`Reserved`).
    let res_v2 = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 2,
            command_key: "cmd_v2".to_string(),
            attempt_id: Some("cat_v2".to_string()),
            subject: subject_v2,
            legacy_base_tree_digest: None,
            reserved_at_ms: 5_000,
        })
        .expect("reserve v2");
    assert!(
        !res_v2.is_cache_hit(),
        "changed final candidate must never inherit old Accepted verdict"
    );
    assert_eq!(res_v2.ledger_revision(), 3);
    assert_eq!(res_v2.attempt().attempt_id, "cat_v2");
    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn final_candidate_refuses_dirty_moved_detached_symlink_out_of_scope_and_foreign_worktrees() {
    let fixture = init_candidate_fixture();
    let task_a = create_accept_and_integrate_task(
        &fixture,
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    100\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );

    let valid_request = InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![to_task_input(&task_a)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("checks green", 10),
    };

    // Baseline succeeds.
    let baseline_subject =
        inspect_final_candidate_readonly(&valid_request).expect("baseline inspect");

    // 1. Dirty RunIntegration checkout (untracked file) -> refused with `DirtyWorktree`.
    let dirty_file = fixture
        .integration_manifest
        .checkout_path
        .join("src/untracked.tmp");
    fs::write(&dirty_file, "dirty").expect("write dirty file");
    let dirty_err =
        inspect_final_candidate_readonly(&valid_request).expect_err("dirty worktree must fail");
    assert!(matches!(dirty_err, OperationsError::DirtyWorktree { .. }));
    fs::remove_file(&dirty_file).expect("remove dirty file");

    // 2. Active writer lock held on RunIntegration worktree -> refused with `ActiveJob`.
    let manager = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
    )
    .expect("manager");
    let guard = manager
        .begin_job(&fixture.integration_manifest.worktree_id)
        .expect("begin_job");
    let active_err =
        inspect_final_candidate_readonly(&valid_request).expect_err("active job must fail");
    assert!(matches!(active_err, OperationsError::ActiveJob { .. }));
    guard.finish().expect("finish job");

    // 3. Out-of-scope `expected_aggregate_write_paths` -> refused with `DiffGateRejected`.
    let mut wrong_scope_req = valid_request.clone();
    let mut wrong_scope = BTreeSet::new();
    wrong_scope.insert("src/beta.rs".to_string());
    wrong_scope_req.expected_aggregate_write_paths = Some(wrong_scope);
    let scope_err = inspect_final_candidate_readonly(&wrong_scope_req)
        .expect_err("mismatched aggregate write scope must fail");
    assert!(matches!(
        scope_err,
        OperationsError::DiffGateRejected { .. }
    ));

    // 4. Detached HEAD on RunIntegration worktree -> refused with `WorktreeNotOwned`.
    run_sanitized_git(
        &fixture.integration_manifest.checkout_path,
        &["checkout", "--detach", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("detach HEAD");
    let detached_err =
        inspect_final_candidate_readonly(&valid_request).expect_err("detached HEAD must fail");
    assert!(matches!(detached_err, OperationsError::WorktreeNotOwned(_)));
    run_sanitized_git(
        &fixture.integration_manifest.checkout_path,
        &["checkout", &fixture.integration_manifest.branch_name],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("re-attach branch");

    // 5. Foreign repository path -> refused with `WorktreeNotOwned`.
    let foreign_temp = TempDir::new().expect("foreign tempdir");
    let foreign_repo = foreign_temp.path().join("foreign-repo");
    fs::create_dir_all(&foreign_repo).expect("create foreign repo");
    run_sanitized_git(
        &foreign_repo,
        &["init", "-b", "main"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init foreign");
    fs::write(foreign_repo.join("README.md"), "foreign\n").expect("write foreign readme");
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
    let foreign_err = inspect_final_candidate_readonly(&foreign_req)
        .expect_err("foreign repository must be refused");
    assert!(matches!(foreign_err, OperationsError::WorktreeNotOwned(_)));

    // 5b. Tampered/insecure closures directory or ledger file is refused fail-closed on read-only inspect.
    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");
    fs::create_dir_all(store.closures_dir()).expect("create closures dir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(store.closures_dir(), fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 closures_dir");
        let mode_err = store
            .inspect_ledger_readonly()
            .expect_err("insecure closures_dir permissions must fail closed");
        assert!(matches!(mode_err, OperationsError::SecurityViolation(_)));
        fs::set_permissions(store.closures_dir(), fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 closures_dir");
    }
    fs::create_dir_all(store.ledger_path()).expect("create directory at ledger_path");
    let non_file_err = store
        .inspect_ledger_readonly()
        .expect_err("non-regular ledger file must not be treated as empty");
    assert!(matches!(
        non_file_err,
        OperationsError::SecurityViolation(_)
    ));
    fs::remove_dir(store.ledger_path()).expect("remove ledger_path dir");
    fs::remove_dir(store.closures_dir()).expect("remove closures_dir");

    // 6. Unreviewed manual commit on RunIntegration branch (moving HEAD past IntegrationReceipt) -> refused!
    fs::write(
        fixture
            .integration_manifest
            .checkout_path
            .join("src/alpha.rs"),
        "pub fn alpha() -> u32 {\n    999\n}\n",
    )
    .expect("write unreviewed change");
    run_sanitized_git(
        &fixture.integration_manifest.checkout_path,
        &[
            "-c",
            "user.name=Rogue",
            "-c",
            "user.email=rogue@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-am",
            "unreviewed manual commit on integration branch",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("commit unreviewed change");

    let moved_err = inspect_final_candidate_readonly(&valid_request)
        .expect_err("unreviewed commit on integration branch must fail");
    assert!(matches!(moved_err, OperationsError::ReceiptInvalidated(_)));
    let verify_moved_err = verify_final_candidate_subject_readonly(&baseline_subject)
        .expect_err("frozen baseline_subject must fail after integration HEAD moved");
    assert!(matches!(
        verify_moved_err,
        OperationsError::HeadMoved { .. } | OperationsError::ReceiptInvalidated(_)
    ));

    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn final_candidate_enforces_cas_and_blocks_conflicting_command_keys_and_unknown_outcomes() {
    let fixture = init_candidate_fixture();
    let task_a = create_accept_and_integrate_task(
        &fixture,
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    11\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );

    let subject: FinalCandidateSubject =
        inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
            repository_path: fixture.repo_path.clone(),
            work_area_root: fixture.work_area_root.clone(),
            run_id: fixture.run_id.clone(),
            integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
            approved_base_commit: fixture.approved_base_commit.clone(),
            expected_source_head_commit: fixture.approved_base_commit.clone(),
            expected_candidate_head_commit: None,
            expected_candidate_tree_sha: None,
            predecessor_tasks: vec![to_task_input(&task_a)],
            expected_aggregate_write_paths: None,
            policy: default_candidate_policy(),
            check_evidence: default_test_evidence("all checks passed", 20),
        })
        .expect("inspect candidate");

    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");

    // 1. Stale CAS on initial reservation (`expected_ledger_revision = 5` when ledger is at `0`) -> refused.
    let stale_res_err = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 5,
            command_key: "cmd_1".to_string(),
            attempt_id: Some("cat_1".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: None,
            reserved_at_ms: 2_000,
        })
        .expect_err("stale CAS on reserve must fail");
    assert!(matches!(
        stale_res_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // 2. Valid reservation with `expected_ledger_revision = 0` -> revision becomes `1`.
    let res = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_1".to_string(),
            attempt_id: Some("cat_1".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: None,
            reserved_at_ms: 2_000,
        })
        .expect("reserve cat_1");
    assert_eq!(res.ledger_revision(), 1);

    // 3. Conflicting `command_key` (`"cmd_2"`) cannot bypass an unresolved `Reserved` attempt for the same subject!
    let bypass_err = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 1,
            command_key: "cmd_2".to_string(),
            attempt_id: Some("cat_2".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: None,
            reserved_at_ms: 2_010,
        })
        .expect_err("different command_key must not bypass unresolved Reserved attempt");
    assert!(matches!(
        bypass_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));

    // 4. Stale CAS on `mark_closure_attempt_submitted` (`expected_ledger_revision = 0` when actual is `1`) -> refused.
    let stale_sub_err = store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 0,
            attempt_id: "cat_1".to_string(),
            command_key: "cmd_1".to_string(),
            expected_subject_digest: subject.subject_digest.clone(),
            external_request_id: "ext_1".to_string(),
            submitted_at_ms: 2_100,
        })
        .expect_err("stale CAS on submit must fail");
    assert!(matches!(
        stale_sub_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // 5. Valid transition to `Submitted` (`ledger_revision` 1 -> 2).
    let (rev2, _) = store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_1".to_string(),
            command_key: "cmd_1".to_string(),
            expected_subject_digest: subject.subject_digest.clone(),
            external_request_id: "ext_1".to_string(),
            submitted_at_ms: 2_100,
        })
        .expect("mark submitted");
    assert_eq!(rev2, 2);

    // 6. Transition to `OutcomeUnknown` (`ledger_revision` 2 -> 3) when provider/persistence outcome is uncertain.
    let (rev3, unknown_rec) = store
        .mark_closure_attempt_unknown(&MarkClosureUnknownRequest {
            expected_ledger_revision: 2,
            attempt_id: "cat_1".to_string(),
            expected_subject_digest: subject.subject_digest.clone(),
            reason: "connection reset while awaiting closure receipt".to_string(),
            recorded_at_ms: 2_200,
        })
        .expect("mark unknown");
    assert_eq!(rev3, 3);
    assert_eq!(unknown_rec.state, ClosureAttemptState::OutcomeUnknown);

    // 7. While `OutcomeUnknown`, both the original `command_key` ("cmd_1") and any new `command_key` ("cmd_3")
    // are blocked from reserving or re-dispatching, and `record_closure_verdict` is also blocked!
    for key in ["cmd_1", "cmd_3"] {
        let blocked_res = store
            .reserve_closure_attempt(&ReserveClosureAttemptRequest {
                expected_ledger_revision: 3,
                command_key: key.to_string(),
                attempt_id: None,
                subject: subject.clone(),
                legacy_base_tree_digest: None,
                reserved_at_ms: 2_300,
            })
            .expect_err("OutcomeUnknown must block replay across all command keys");
        assert!(matches!(
            blocked_res,
            OperationsError::UncertainWorkerOutcome(_)
        ));
    }

    let direct_verdict_err = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 3,
            attempt_id: "cat_1".to_string(),
            command_key: "cmd_1".to_string(),
            verdict_id: None,
            subject: subject.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "Attempt direct record on unknown".to_string(),
            execution_evidence: default_execution_evidence("hnd_1"),
            recorded_at_ms: 2_350,
        })
        .expect_err("record_closure_verdict must refuse OutcomeUnknown attempts");
    assert!(matches!(
        direct_verdict_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));

    // 8. Reconciling with mismatched receipt ID vs execution_evidence.handoff_receipt_id is refused.
    let bad_reconcile_err = store
        .reconcile_closure_attempt(&ReconcileClosureAttemptRequest {
            expected_ledger_revision: 3,
            attempt_id: "cat_1".to_string(),
            reconciliation_receipt_id: "hnd_wrong".to_string(),
            reconciliation_receipt_digest: "55".repeat(32),
            subject: subject.clone(),
            resolved_kind: ClosureVerdictKind::Accepted,
            summary: "Mismatched receipt id".to_string(),
            execution_evidence: default_execution_evidence("hnd_actual"),
            reconciled_at_ms: 2_400,
        })
        .expect_err("mismatched reconciliation receipt id must fail");
    assert!(matches!(
        bad_reconcile_err,
        OperationsError::ReceiptInvalidated(_)
    ));

    // 9. Explicit receipt-bound reconciliation transitions `OutcomeUnknown -> Terminal` (`ledger_revision` 3 -> 4).
    let (rev4, reconciled_rec, reconciled_verdict) = store
        .reconcile_closure_attempt(&ReconcileClosureAttemptRequest {
            expected_ledger_revision: 3,
            attempt_id: "cat_1".to_string(),
            reconciliation_receipt_id: "hnd_reconciled_1".to_string(),
            reconciliation_receipt_digest: "55".repeat(32),
            subject: subject.clone(),
            resolved_kind: ClosureVerdictKind::Accepted,
            summary: "Reconciled from durable handoff receipt.".to_string(),
            execution_evidence: default_execution_evidence("hnd_reconciled_1"),
            reconciled_at_ms: 2_500,
        })
        .expect("reconcile closure attempt");
    assert_eq!(rev4, 4);
    assert_eq!(reconciled_rec.state, ClosureAttemptState::Terminal);
    assert_eq!(
        reconciled_rec.reconciliation_receipt_id.as_deref(),
        Some("hnd_reconciled_1")
    );
    assert_eq!(reconciled_verdict.kind, ClosureVerdictKind::Accepted);

    // 10. Subsequent reservation for the same subject now returns `CacheHit`.
    let post_reconcile = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 4,
            command_key: "cmd_4".to_string(),
            attempt_id: None,
            subject,
            legacy_base_tree_digest: None,
            reserved_at_ms: 2_600,
        })
        .expect("cache hit after reconciliation");
    assert!(post_reconcile.is_cache_hit());
    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn final_candidate_legacy_base_tree_reservation_never_satisfies_final_closure_and_verdict_grants_no_merge_or_push_authority(
) {
    let fixture = init_candidate_fixture();

    // 1. A RunIntegration checkout still sitting at `approved_base_commit` / `approved_base_tree_sha`
    // is rejected when attempting to freeze a final combined candidate.
    let manager = WorktreeManager::new(
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.approved_base_commit,
    )
    .expect("manager");
    let mut allowed = BTreeSet::new();
    allowed.insert("src/alpha.rs".to_string());
    let unintegrated_task = manager
        .create_task_worktree_with_token(&fixture.run_id, "task_alpha", "t01", &allowed)
        .expect("create task");
    fs::write(
        unintegrated_task.checkout_path.join("src/alpha.rs"),
        "pub fn alpha() -> u32 {\n    7\n}\n",
    )
    .expect("write alpha");
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: unintegrated_task.worktree_id.clone(),
        commit_message: "task_alpha commit".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task");
    let task_subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: fixture.work_area_root.clone(),
        task_worktree_id: unintegrated_task.worktree_id.clone(),
        policy: default_task_policy(),
        test_evidence: default_test_evidence("passed", 5),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect task");
    let task_rev = accept_review_subject(&ReviewAcceptanceRequest {
        subject: task_subject.clone(),
        expected_subject_digest: task_subject.subject_digest.clone(),
        reviewer_id: "op_primary".to_string(),
        review_note: "Approved".to_string(),
        approved_at_ms: 1_000,
        receipt_id: Some("rev_unintegrated".to_string()),
    })
    .expect("accept task");

    // Before integration, RunIntegration is still at `approved_base_commit`.
    let base_only_err = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![FinalCandidateTaskInput {
            task_id: "task_alpha".to_string(),
            task_digest: "aa".repeat(32),
            task_worktree_id: unintegrated_task.worktree_id.clone(),
            review_receipt_id: task_rev.receipt_id.clone(),
            integration_receipt_id: "int_missing".to_string(),
            expected_execution_base_commit: None,
            expected_allowed_write_paths: None,
            expected_provider: None,
            expected_model: None,
            expected_effort: None,
        }],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("passed", 5),
    })
    .expect_err("unintegrated RunIntegration at base tree must fail");
    assert!(matches!(base_only_err, OperationsError::SubjectMismatch(_)));

    // Now integrate `task_alpha` into `RunIntegration`.
    let int_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: fixture.work_area_root.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        task_worktree_id: unintegrated_task.worktree_id.clone(),
        subject: task_subject,
        review_receipt: task_rev.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        expected_integration_head_commit: fixture.approved_base_commit.clone(),
        expected_task_head_commit: task_rev.task_head_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        operator_id: "op_primary".to_string(),
        integrated_at_ms: 1_100,
        receipt_id: Some("int_alpha".to_string()),
    })
    .expect("integrate task_alpha");

    let integrated_task = AcceptedIntegratedTask {
        task_id: "task_alpha".to_string(),
        task_digest: "aa".repeat(32),
        task_manifest: unintegrated_task,
        review_receipt: task_rev,
        integration_receipt: int_receipt,
    };

    let subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![to_task_input(&integrated_task)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("passed", 10),
    })
    .expect("inspect integrated candidate");

    // 2. `verify_legacy_base_closure_insufficient` confirms `approved_base_tree_sha` is distinct
    // and rejects any attempt to pass `candidate_tree_sha` or `subject_digest` as a legacy base digest.
    verify_legacy_base_closure_insufficient(&fixture.approved_base_tree_sha, &subject)
        .expect("approved_base_tree_sha is distinct from final candidate");
    let spoofed_legacy_err =
        verify_legacy_base_closure_insufficient(&subject.candidate_tree_sha, &subject)
            .expect_err("candidate_tree_sha must not be accepted as a legacy base-tree digest");
    assert!(matches!(
        spoofed_legacy_err,
        OperationsError::SubjectMismatch(_)
    ));

    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");
    store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_authority_check".to_string(),
            attempt_id: Some("cat_auth_1".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 2_000,
        })
        .expect("reserve attempt");

    // 3. Execution evidence claiming merge/push authority or check substitution is strictly rejected!
    let mut bad_merge_auth = default_execution_evidence("hnd_auth_1");
    bad_merge_auth.grants_merge_or_push_authority = true;
    let auth_err = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_auth_1".to_string(),
            command_key: "cmd_authority_check".to_string(),
            verdict_id: None,
            subject: subject.clone(),
            kind: ClosureVerdictKind::Accepted,
            summary: "Try to grant merge authority".to_string(),
            execution_evidence: bad_merge_auth,
            recorded_at_ms: 2_100,
        })
        .expect_err("grants_merge_or_push_authority = true must fail");
    assert!(matches!(auth_err, OperationsError::SecurityViolation(_)));

    let mut bad_check_sub = default_execution_evidence("hnd_auth_1");
    bad_check_sub.substitutes_for_required_checks = true;
    let sub_err = store
        .record_closure_verdict(&RecordClosureVerdictRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_auth_1".to_string(),
            command_key: "cmd_authority_check".to_string(),
            verdict_id: None,
            subject,
            kind: ClosureVerdictKind::Accepted,
            summary: "Try to substitute for checks".to_string(),
            execution_evidence: bad_check_sub,
            recorded_at_ms: 2_100,
        })
        .expect_err("substitutes_for_required_checks = true must fail");
    assert!(matches!(sub_err, OperationsError::SecurityViolation(_)));

    assert_primary_checkout_untouched(&fixture);
}

#[test]
fn known_local_terminal_closure_failure_blocks_blind_retry_and_supports_reconciliation() {
    let fixture = init_candidate_fixture();
    let task_a = create_accept_and_integrate_task(
        &fixture,
        "task_alpha",
        "t01",
        "aa",
        "src/alpha.rs",
        "pub fn alpha() -> u32 {\n    42\n}\n",
        IntegrationStrategy::FastForwardOnly,
        1_000,
    );
    let subject = inspect_final_candidate_readonly(&InspectFinalCandidateRequest {
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        run_id: fixture.run_id.clone(),
        integration_worktree_id: fixture.integration_manifest.worktree_id.clone(),
        approved_base_commit: fixture.approved_base_commit.clone(),
        expected_source_head_commit: fixture.approved_base_commit.clone(),
        expected_candidate_head_commit: None,
        expected_candidate_tree_sha: None,
        predecessor_tasks: vec![to_task_input(&task_a)],
        expected_aggregate_write_paths: None,
        policy: default_candidate_policy(),
        check_evidence: default_test_evidence("passed", 10),
    })
    .expect("inspect candidate");

    let store = FinalCandidateClosureStore::open(&fixture.repo_path, &fixture.work_area_root)
        .expect("open store");
    store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 0,
            command_key: "cmd_fail_1".to_string(),
            attempt_id: Some("cat_fail_1".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: Some(fixture.approved_base_tree_sha.clone()),
            reserved_at_ms: 1_000,
        })
        .expect("reserve");
    store
        .mark_closure_attempt_submitted(&MarkClosureSubmittedRequest {
            expected_ledger_revision: 1,
            attempt_id: "cat_fail_1".to_string(),
            command_key: "cmd_fail_1".to_string(),
            expected_subject_digest: subject.subject_digest.clone(),
            external_request_id: "req_fail_1".to_string(),
            submitted_at_ms: 1_050,
        })
        .expect("submit");
    let (rev, failed) = store
        .record_closure_failure(&RecordClosureFailureRequest {
            expected_ledger_revision: 2,
            attempt_id: "cat_fail_1".to_string(),
            command_key: "cmd_fail_1".to_string(),
            expected_subject_digest: subject.subject_digest.clone(),
            failure: ClosureTerminalFailure {
                failure_code: "response_validation_failed".to_string(),
                reason: "malformed closure JSON output".to_string(),
                exit_code: Some(0),
                observed_provider: Some("gpt".to_string()),
                observed_model: None,
                observed_effort: None,
                response_sha256_digest: Some("ab".repeat(32)),
                fenced_late_output: false,
                recorded_at_ms: 1_100,
            },
        })
        .expect("record failure");
    assert_eq!(rev, 3);
    assert_eq!(failed.state, ClosureAttemptState::Terminal);
    assert!(failed.verdict.is_none());
    assert!(failed.terminal_failure.is_some());

    // New command key cannot bypass the failed terminal record on the same subject.
    let bypass_err = store
        .reserve_closure_attempt(&ReserveClosureAttemptRequest {
            expected_ledger_revision: 3,
            command_key: "cmd_fail_2".to_string(),
            attempt_id: Some("cat_fail_2".to_string()),
            subject: subject.clone(),
            legacy_base_tree_digest: None,
            reserved_at_ms: 1_200,
        })
        .expect_err("must block new-key bypass on failed subject");
    assert!(matches!(
        bypass_err,
        OperationsError::UncertainWorkerOutcome(_)
    ));

    // Explicit receipt-bound reconciliation can resolve the terminal failure.
    let (rec_rev, rec_attempt, rec_verdict) = store
        .reconcile_closure_attempt(&ReconcileClosureAttemptRequest {
            expected_ledger_revision: 3,
            attempt_id: "cat_fail_1".to_string(),
            reconciliation_receipt_id: "hnd_rec_fail_1".to_string(),
            reconciliation_receipt_digest: "cd".repeat(32),
            subject: subject.clone(),
            resolved_kind: ClosureVerdictKind::Accepted,
            summary: "Reconciled with receipt".to_string(),
            execution_evidence: default_execution_evidence("hnd_rec_fail_1"),
            reconciled_at_ms: 1_300,
        })
        .expect("reconcile failed attempt");
    assert_eq!(rec_rev, 4);
    assert_eq!(rec_attempt.state, ClosureAttemptState::Terminal);
    assert_eq!(rec_verdict.kind, ClosureVerdictKind::Accepted);
}
