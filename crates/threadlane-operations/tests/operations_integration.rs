use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tempfile::TempDir;
use threadlane_operations::{
    accept_review_subject, build_tmux_readonly_descriptor, cleanup_integrated_task_worktree,
    commit_task_worktree_changes, execute_integration, execute_push, execute_remote_branch_delete,
    inspect_codegraph_worktree, inspect_review_subject, inspect_tool_capabilities_passive,
    load_integration_receipt, load_review_receipt, probe_tool_capabilities_explicit,
    propose_codegraph_index_plan, propose_integration, propose_push, propose_remote_branch_delete,
    record_codegraph_index_receipt, verify_review_receipt, AuxiliaryToolKind,
    CodeGraphIndexFreshness, CommitTaskChangesRequest, ExecuteIntegrationRequest,
    ExecutePushRequest, ExecuteRemoteDeleteRequest, InspectReviewSubjectRequest,
    IntegrationProposalRequest, IntegrationStrategy, OperationsErrorCode, PushProposalRequest,
    RemoteDeleteProposalRequest, ReviewAcceptanceRequest, ReviewPolicyBinding, TestEvidenceSummary,
    TmuxDescriptorRequest, ToolPathsConfig, ToolSupportStatus, VerifiedIntegratedCleanupRequest,
};
use threadlane_workers::{
    cleanup_worktree, resolve_exact_commit_sha, run_sanitized_git, CleanupRequest,
    WorkerFilePermissions, WorkerRunStatus, WorktreeKind, WorktreeManager,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

fn create_private_tempdir() -> TempDir {
    let dir = tempfile::tempdir().expect("create tempdir");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("chmod 0700 tempdir");
    dir
}

fn init_disposable_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir).expect("create repo dir");
    fs::set_permissions(repo_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 repo");

    run_sanitized_git(
        repo_dir,
        &["init", "-q", "-b", "main"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init");
    fs::create_dir_all(repo_dir.join("src")).expect("create src dir");
    fs::create_dir_all(repo_dir.join("docs")).expect("create docs dir");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    10\n}\n",
    )
    .expect("write src/lib.rs");
    fs::write(repo_dir.join("docs/README.md"), "# Disposable Repo\n")
        .expect("write docs/README.md");

    run_sanitized_git(
        repo_dir,
        &["add", "--", "src/lib.rs", "docs/README.md"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add");
    run_sanitized_git(
        repo_dir,
        &[
            "-c",
            "user.name=Threadlane Fixture",
            "-c",
            "user.email=fixture@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "Initial commit",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");

    resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve initial HEAD")
}

fn sample_policy() -> ReviewPolicyBinding {
    ReviewPolicyBinding {
        policy_id: "m4-argon-high-policy".to_string(),
        codex_launch_report: None,
        codex_source_report_sha256: None,
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

fn sample_test_evidence() -> TestEvidenceSummary {
    TestEvidenceSummary {
        formatting_passed: true,
        clippy_passed: true,
        tests_passed: true,
        passed_test_count: 18,
        failed_test_count: 0,
        evidence_summary: "cargo fmt, clippy -D warnings, and 18 focused behavioral tests passed"
            .to_string(),
    }
}

#[test]
fn review_subject_digest_invalidates_receipt_on_any_head_base_scope_policy_or_evidence_change() {
    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let work_area = root.path().join("work_area");
    let base_commit = init_disposable_repo(&repo_dir);

    let manager = WorktreeManager::new(&repo_dir, &work_area, &base_commit).expect("manager");
    let mut allowed = BTreeSet::new();
    allowed.insert("src".to_string());
    let task_wt = manager
        .create_task_worktree_with_token("run-rev", "task-rev", "tokrev01", &allowed)
        .expect("create task worktree");

    // 1. Write an in-scope edit in the task worktree; before committing, `inspect_review_subject`
    // must refuse because uncommitted changes cannot be frozen into a commit-bound ReviewSubject.
    fs::write(
        task_wt.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    25\n}\n",
    )
    .expect("edit src/lib.rs");

    let dirty_err = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        policy: sample_policy(),
        test_evidence: sample_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect_err("uncommitted changes must be rejected by inspect_review_subject");
    assert_eq!(dirty_err.error_code(), OperationsErrorCode::DirtyWorktree);

    // 2. Uncertain or failed worker status (`TimedOut`, `MissingEffortEvidence` when required)
    // must be rejected before committing or inspecting.
    let uncertain_err = commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        commit_message: "Should not commit".to_string(),
        worker_status: Some(WorkerRunStatus::MissingEffortEvidence),
        allow_missing_effort_evidence: false,
    })
    .expect_err("must reject MissingEffortEvidence when not allowed");
    assert_eq!(
        uncertain_err.error_code(),
        OperationsErrorCode::UncertainWorkerOutcome
    );

    // 3. Explicit operator commit succeeds for validated in-scope changes.
    let commit_report = commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        commit_message: "Update compute() to return 25".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");
    assert_ne!(commit_report.task_head_commit, base_commit);
    assert_eq!(commit_report.changed_paths_count, 1);

    // 4. Inspect review subject and verify deterministic digest.
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        policy: sample_policy(),
        test_evidence: sample_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");
    assert_eq!(subject.compute_digest().unwrap(), subject.subject_digest);

    // 5. Policy requiring effort evidence rejects acceptance if `observed_effort` is missing.
    let mut missing_effort_policy = sample_policy();
    missing_effort_policy.observed_effort = None;
    let unaccepted_subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        policy: missing_effort_policy,
        test_evidence: sample_test_evidence(),
        worker_status: None,
    })
    .expect("inspect subject with missing observed_effort");
    let effort_err = accept_review_subject(&ReviewAcceptanceRequest {
        expected_subject_digest: unaccepted_subject.subject_digest.clone(),
        subject: unaccepted_subject,
        receipt_id: Some("rev-missing-effort".to_string()),
        reviewer_id: "primary-codex".to_string(),
        review_note: "Attempted approval".to_string(),
        approved_at_ms: 1_700_000_000_000,
    })
    .expect_err("must reject missing observed_effort when require_effort_evidence is true");
    assert_eq!(
        effort_err.error_code(),
        OperationsErrorCode::UncertainWorkerOutcome
    );

    // 6. Explicit operator acceptance with satisfied policy and test evidence issues a frozen ReviewReceipt.
    let receipt = accept_review_subject(&ReviewAcceptanceRequest {
        expected_subject_digest: subject.subject_digest.clone(),
        subject: subject.clone(),
        receipt_id: Some("rev-valid-01".to_string()),
        reviewer_id: "primary-codex".to_string(),
        review_note: "Reviewed diff, policy, and 18 passing tests".to_string(),
        approved_at_ms: 1_700_000_000_100,
    })
    .expect("accept review subject");

    let loaded_receipt =
        load_review_receipt(&work_area, "rev-valid-01").expect("load review receipt");
    assert_eq!(loaded_receipt, receipt);
    verify_review_receipt(&subject, &receipt).expect("valid receipt must pass verification");

    // 7. Mutating `base_commit` invalidates the receipt.
    let mut mutated_base = subject.clone();
    mutated_base.base_commit = "1".repeat(40);
    mutated_base.subject_digest = mutated_base.compute_digest().unwrap();
    let err = verify_review_receipt(&mutated_base, &receipt)
        .expect_err("base_commit change must invalidate receipt");
    assert_eq!(err.error_code(), OperationsErrorCode::ReceiptInvalidated);

    // 8. Mutating `task_head_commit` invalidates the receipt.
    let mut mutated_head = subject.clone();
    mutated_head.task_head_commit = "2".repeat(40);
    mutated_head.subject_digest = mutated_head.compute_digest().unwrap();
    let err = verify_review_receipt(&mutated_head, &receipt)
        .expect_err("task_head_commit change must invalidate receipt");
    assert_eq!(err.error_code(), OperationsErrorCode::ReceiptInvalidated);

    // 9. Mutating `allowed_write_paths` (scope) invalidates the receipt.
    let mut mutated_scope = subject.clone();
    mutated_scope.allowed_write_paths.insert("docs".to_string());
    mutated_scope.subject_digest = mutated_scope.compute_digest().unwrap();
    let err = verify_review_receipt(&mutated_scope, &receipt)
        .expect_err("scope change must invalidate receipt");
    assert_eq!(err.error_code(), OperationsErrorCode::ReceiptInvalidated);

    // 10. Mutating `policy` invalidates the receipt.
    let mut mutated_policy = subject.clone();
    mutated_policy.policy.policy_id = "different-policy".to_string();
    mutated_policy.subject_digest = mutated_policy.compute_digest().unwrap();
    let err = verify_review_receipt(&mutated_policy, &receipt)
        .expect_err("policy change must invalidate receipt");
    assert_eq!(err.error_code(), OperationsErrorCode::ReceiptInvalidated);

    // 11. Mutating `test_evidence` invalidates the receipt.
    let mut mutated_evidence = subject.clone();
    mutated_evidence.test_evidence.passed_test_count = 99;
    mutated_evidence.subject_digest = mutated_evidence.compute_digest().unwrap();
    let err = verify_review_receipt(&mutated_evidence, &receipt)
        .expect_err("test_evidence change must invalidate receipt");
    assert_eq!(err.error_code(), OperationsErrorCode::ReceiptInvalidated);

    // 12. Adding a new commit in the task worktree on disk invalidates the previously issued receipt
    // even when the caller passes the original `subject` and `receipt` structs!
    fs::write(
        task_wt.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    999\n}\n",
    )
    .expect("write unreviewed edit");
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area,
        task_worktree_id: task_wt.worktree_id,
        commit_message: "Unreviewed follow-up commit".to_string(),
        worker_status: None,
        allow_missing_effort_evidence: false,
    })
    .expect("commit follow-up");

    let live_err = verify_review_receipt(&subject, &receipt)
        .expect_err("live task HEAD advancing on disk must invalidate receipt");
    assert_eq!(
        live_err.error_code(),
        OperationsErrorCode::ReceiptInvalidated
    );
}

#[test]
fn full_workflow_propose_integrate_cleanup_push_and_leased_remote_delete_on_disposable_repos() {
    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let bare_remote = root.path().join("bare_remote.git");
    let work_area = root.path().join("work_area");
    let base_commit = init_disposable_repo(&repo_dir);

    // Initialize a local disposable bare repository to act as `"origin"` (no network access).
    fs::create_dir_all(&bare_remote).expect("create bare_remote");
    fs::set_permissions(&bare_remote, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 bare_remote");
    run_sanitized_git(
        &bare_remote,
        &["init", "--bare", "-q"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init --bare");
    run_sanitized_git(
        &repo_dir,
        &["remote", "add", "origin", bare_remote.to_str().unwrap()],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("add remote origin");
    run_sanitized_git(
        &repo_dir,
        &[
            "push",
            "-q",
            "origin",
            &format!("{base_commit}:refs/heads/main"),
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("push initial main to bare remote");

    let manager = WorktreeManager::new(&repo_dir, &work_area, &base_commit).expect("manager");
    let int_wt = manager
        .create_run_worktree_with_token("run-full", "tokint01")
        .expect("create integration worktree");
    let mut allowed = BTreeSet::new();
    allowed.insert("src".to_string());
    let task_wt = manager
        .create_task_worktree_with_token("run-full", "task-full", "toktsk01", &allowed)
        .expect("create task worktree");

    // Edit and commit in task worktree.
    fs::write(
        task_wt.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    77\n}\n",
    )
    .expect("write task change");
    let task_commit = commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        commit_message: "Implement compute() -> 77".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task changes");

    // Push the task branch to the disposable bare remote so we can test remote branch deletion gates.
    run_sanitized_git(
        &repo_dir,
        &[
            "push",
            "-q",
            "origin",
            &format!(
                "{}:refs/heads/{}",
                task_commit.task_head_commit, task_wt.branch_name
            ),
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("push task branch to bare remote");

    // Inspect and accept the review subject.
    let subject = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        policy: sample_policy(),
        test_evidence: sample_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect review subject");
    let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
        expected_subject_digest: subject.subject_digest.clone(),
        subject: subject.clone(),
        receipt_id: Some("rev-full-01".to_string()),
        reviewer_id: "primary-codex".to_string(),
        review_note: "Approved for integration".to_string(),
        approved_at_ms: 1_700_000_010_000,
    })
    .expect("accept review subject");

    // 1. Read-only integration proposal (`propose_integration`) must not mutate the integration worktree.
    let proposal = propose_integration(&IntegrationProposalRequest {
        work_area_root: work_area.clone(),
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        strategy: IntegrationStrategy::MergeNoFf,
    })
    .expect("propose integration");
    assert!(proposal.read_only);
    assert!(proposal.can_fast_forward);
    assert!(!proposal.already_integrated);
    assert!(proposal.clean_merge_predicted);
    assert_eq!(
        resolve_exact_commit_sha(
            &int_wt.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .unwrap(),
        base_commit,
        "propose_integration must not mutate integration worktree HEAD"
    );

    // 2. `execute_integration` with a stale `expected_integration_head_commit` is refused with `HeadMoved`.
    let stale_head_err = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area.clone(),
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        expected_integration_head_commit: "f".repeat(40),
        expected_task_head_commit: subject.task_head_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        strategy: IntegrationStrategy::MergeNoFf,
        receipt_id: Some("int-stale".to_string()),
        operator_id: "primary-codex".to_string(),
        integrated_at_ms: 1_700_000_020_000,
    })
    .expect_err("stale integration head must be refused");
    assert_eq!(stale_head_err.error_code(), OperationsErrorCode::HeadMoved);

    // 3. `execute_integration` with matching heads merges into the owned integration checkout,
    // preserves the primary source checkout at `base_commit`, and freezes an `IntegrationReceipt`.
    let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area.clone(),
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        expected_integration_head_commit: base_commit.clone(),
        expected_task_head_commit: subject.task_head_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        strategy: IntegrationStrategy::MergeNoFf,
        receipt_id: Some("int-full-01".to_string()),
        operator_id: "primary-codex".to_string(),
        integrated_at_ms: 1_700_000_020_000,
    })
    .expect("execute integration");

    let loaded_int_receipt =
        load_integration_receipt(&work_area, "int-full-01").expect("load integration receipt");
    assert_eq!(loaded_int_receipt, integration_receipt);
    assert_ne!(integration_receipt.integrated_target_commit, base_commit);
    assert_eq!(
        resolve_exact_commit_sha(&repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).unwrap(),
        base_commit,
        "primary source checkout HEAD must remain untouched at base_commit"
    );

    // 4. Attempting `execute_remote_branch_delete` BEFORE pushing the integrated commit to
    // `origin/main` MUST be refused with `RemoteAncestryMissing` (never blindly delete a remote
    // branch on local merge only!).
    let premature_remote_del = execute_remote_branch_delete(&ExecuteRemoteDeleteRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        remote_source_branch: task_wt.branch_name.clone(),
        expected_remote_source_head: subject.task_head_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        operator_id: "primary-codex".to_string(),
        deleted_at_ms: 1_700_000_030_000,
    })
    .expect_err("must refuse remote branch deletion before remote target contains accepted head");
    assert_eq!(
        premature_remote_del.error_code(),
        OperationsErrorCode::RemoteAncestryMissing
    );

    // 5. Read-only `propose_push` builds the push plan without mutating the remote.
    let push_plan = propose_push(&PushProposalRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        expected_remote_target_head: Some(base_commit.clone()),
    })
    .expect("propose push");
    assert!(push_plan.read_only);
    assert_eq!(
        resolve_exact_commit_sha(
            &bare_remote,
            "refs/heads/main",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .unwrap(),
        base_commit
    );

    // 6. `execute_push` with a wrong `expected_remote_target_head` lease is refused with `RemoteLeaseMismatch`.
    let bad_lease_push = execute_push(&ExecutePushRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        expected_remote_target_head: Some("e".repeat(40)),
        expected_source_head_commit: base_commit.clone(),
        operator_id: "primary-codex".to_string(),
        pushed_at_ms: 1_700_000_035_000,
    })
    .expect_err("wrong remote lease must be refused");
    assert_eq!(
        bad_lease_push.error_code(),
        OperationsErrorCode::RemoteLeaseMismatch
    );

    // 7. `execute_push` with matching lease pushes `integrated_target_commit` to `origin/main`.
    let push_receipt = execute_push(&ExecutePushRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        expected_remote_target_head: Some(base_commit.clone()),
        expected_source_head_commit: base_commit.clone(),
        operator_id: "primary-codex".to_string(),
        pushed_at_ms: 1_700_000_040_000,
    })
    .expect("execute push");
    assert_eq!(
        push_receipt.verified_remote_target_head,
        integration_receipt.integrated_target_commit
    );

    // 8. `execute_remote_branch_delete` with a wrong `expected_remote_source_head` is refused.
    let bad_del_plan = propose_remote_branch_delete(&RemoteDeleteProposalRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        remote_source_branch: task_wt.branch_name.clone(),
        expected_remote_source_head: "d".repeat(40),
    })
    .expect_err("must reject expected_remote_source_head != accepted_task_head_commit");
    assert_eq!(
        bad_del_plan.error_code(),
        OperationsErrorCode::ReceiptInvalidated
    );

    // 9. Now that `origin/main` contains `accepted_task_head_commit` and `origin/<task_branch>`
    // matches `expected_remote_source_head`, `execute_remote_branch_delete` succeeds!
    let remote_del_receipt = execute_remote_branch_delete(&ExecuteRemoteDeleteRequest {
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        integration_receipt: integration_receipt.clone(),
        remote_name: "origin".to_string(),
        remote_target_branch: "main".to_string(),
        remote_source_branch: task_wt.branch_name.clone(),
        expected_remote_source_head: subject.task_head_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        operator_id: "primary-codex".to_string(),
        deleted_at_ms: 1_700_000_045_000,
    })
    .expect("execute remote branch delete");
    assert_eq!(
        remote_del_receipt.deleted_remote_source_branch,
        task_wt.branch_name
    );

    // 10. Clean up the integrated task worktree via `cleanup_integrated_task_worktree`.
    let cleanup_report = cleanup_integrated_task_worktree(&VerifiedIntegratedCleanupRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        subject,
        review_receipt,
        integration_receipt,
        expected_source_head_commit: base_commit,
    })
    .expect("cleanup integrated task worktree");
    assert!(cleanup_report.checkout_removed);
    assert!(cleanup_report.partial_cleanup);
    assert!(!task_wt.checkout_path.exists());
}

#[test]
fn conflicting_merge_is_detected_by_proposal_and_refused_without_dirtying_integration_checkout() {
    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let work_area = root.path().join("work_area");
    let base_commit = init_disposable_repo(&repo_dir);

    let manager = WorktreeManager::new(&repo_dir, &work_area, &base_commit).expect("manager");
    let int_wt = manager
        .create_run_worktree_with_token("run-conf", "tokintc1")
        .expect("create integration worktree");
    let mut allowed = BTreeSet::new();
    allowed.insert("src".to_string());
    let task_wt_a = manager
        .create_task_worktree_with_token("run-conf", "task-a", "toktska1", &allowed)
        .expect("create task A worktree");
    let task_wt_b = manager
        .create_task_worktree_with_token("run-conf", "task-b", "toktskb1", &allowed)
        .expect("create task B worktree");

    // Task A edits src/lib.rs to 100 and integrates cleanly.
    fs::write(
        task_wt_a.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    100\n}\n",
    )
    .expect("write task A");
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt_a.worktree_id.clone(),
        commit_message: "Task A update".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task A");
    let subj_a = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt_a.worktree_id.clone(),
        policy: sample_policy(),
        test_evidence: sample_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect A");
    let rev_a = accept_review_subject(&ReviewAcceptanceRequest {
        expected_subject_digest: subj_a.subject_digest.clone(),
        subject: subj_a.clone(),
        receipt_id: Some("rev-a".to_string()),
        reviewer_id: "primary-codex".to_string(),
        review_note: "Approve A".to_string(),
        approved_at_ms: 1_700_000_100_000,
    })
    .expect("accept A");
    let int_a = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area.clone(),
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt_a.worktree_id.clone(),
        subject: subj_a,
        review_receipt: rev_a,
        expected_integration_head_commit: base_commit.clone(),
        expected_task_head_commit: resolve_exact_commit_sha(
            &task_wt_a.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .unwrap(),
        expected_source_head_commit: base_commit.clone(),
        strategy: IntegrationStrategy::FastForwardOnly,
        receipt_id: Some("int-a".to_string()),
        operator_id: "primary-codex".to_string(),
        integrated_at_ms: 1_700_000_101_000,
    })
    .expect("integrate A");

    // Task B edits the exact same line in src/lib.rs to 200 (conflicting with Task A).
    fs::write(
        task_wt_b.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    200\n}\n",
    )
    .expect("write task B");
    commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt_b.worktree_id.clone(),
        commit_message: "Task B conflicting update".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit task B");
    let subj_b = inspect_review_subject(&InspectReviewSubjectRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt_b.worktree_id.clone(),
        policy: sample_policy(),
        test_evidence: sample_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    })
    .expect("inspect B");
    let rev_b = accept_review_subject(&ReviewAcceptanceRequest {
        expected_subject_digest: subj_b.subject_digest.clone(),
        subject: subj_b.clone(),
        receipt_id: Some("rev-b".to_string()),
        reviewer_id: "primary-codex".to_string(),
        review_note: "Approve B".to_string(),
        approved_at_ms: 1_700_000_102_000,
    })
    .expect("accept B");

    // `propose_integration` predicts `clean_merge_predicted == false`.
    let prop_b = propose_integration(&IntegrationProposalRequest {
        work_area_root: work_area.clone(),
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt_b.worktree_id.clone(),
        subject: subj_b.clone(),
        review_receipt: rev_b.clone(),
        strategy: IntegrationStrategy::MergeNoFf,
    })
    .expect("propose B");
    assert!(!prop_b.can_fast_forward);
    assert!(!prop_b.clean_merge_predicted);

    // `execute_integration` refuses with `MergeConflict` and leaves `int_wt` clean at `int_a.integrated_target_commit`.
    let err_b = execute_integration(&ExecuteIntegrationRequest {
        work_area_root: work_area,
        integration_worktree_id: int_wt.worktree_id.clone(),
        task_worktree_id: task_wt_b.worktree_id,
        subject: subj_b.clone(),
        review_receipt: rev_b,
        expected_integration_head_commit: int_a.integrated_target_commit.clone(),
        expected_task_head_commit: subj_b.task_head_commit,
        expected_source_head_commit: base_commit,
        strategy: IntegrationStrategy::MergeNoFf,
        receipt_id: Some("int-b".to_string()),
        operator_id: "primary-codex".to_string(),
        integrated_at_ms: 1_700_000_103_000,
    })
    .expect_err("conflicting merge must be refused");
    assert_eq!(err_b.error_code(), OperationsErrorCode::MergeConflict);
    assert_eq!(
        resolve_exact_commit_sha(
            &int_wt.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS
        )
        .unwrap(),
        int_a.integrated_target_commit
    );
}

#[test]
fn capabilities_tmux_readonly_descriptor_and_codegraph_owned_worktree_freshness() {
    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let work_area = root.path().join("work_area");
    let base_commit = init_disposable_repo(&repo_dir);

    let manager = WorktreeManager::new(&repo_dir, &work_area, &base_commit).expect("manager");
    let mut allowed = BTreeSet::new();
    allowed.insert("src".to_string());
    let task_wt = manager
        .create_task_worktree_with_token("run-cap", "task-cap", "tokcap01", &allowed)
        .expect("create task worktree");

    // 1. Passive and explicit capability checks with fake disposable executables.
    let fake_bin_dir = root.path().join("bin");
    fs::create_dir_all(&fake_bin_dir).expect("create fake_bin_dir");
    fs::set_permissions(&fake_bin_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
    let fake_git = fake_bin_dir.join("fake-git");
    fs::write(&fake_git, "#!/bin/sh\necho 'git version 2.44.0'\n").expect("write fake-git");
    fs::set_permissions(&fake_git, fs::Permissions::from_mode(0o755)).expect("chmod 0755");

    let cfg = ToolPathsConfig {
        git_executable: Some(fake_git.clone()),
        tmux_executable: None,
        codegraph_executable: Some(root.path().join("missing-codegraph")),
    };
    let passive = inspect_tool_capabilities_passive(&cfg);
    assert_eq!(passive.git.tool, AuxiliaryToolKind::Git);
    assert_eq!(passive.git.status, ToolSupportStatus::AvailableUnverified);
    assert!(passive.git.version.is_none());
    assert_eq!(passive.tmux.status, ToolSupportStatus::OptionalUnavailable);
    assert!(!passive.tmux.mandatory_os_dependency);
    assert_eq!(
        passive.codegraph.status,
        ToolSupportStatus::InvalidExecutable
    );
    assert!(!passive.codegraph.mandatory_os_dependency);
    assert!(!passive.codegraph_surface.mcp_is_cli);
    assert!(!passive.codegraph_surface.auto_invoked);

    let probed = probe_tool_capabilities_explicit(&cfg);
    assert_eq!(probed.git.status, ToolSupportStatus::SupportedAndVerified);
    assert_eq!(probed.git.version.as_deref(), Some("git version 2.44.0"));
    assert_eq!(probed.tmux.status, ToolSupportStatus::OptionalUnavailable);

    // 2. Read-only `tmux` attachment descriptor verifies ownership and sets `-r` and `mandatory_supervisor = false`.
    let socket_path = root.path().join("tmux.sock");
    let tmux_desc = build_tmux_readonly_descriptor(&TmuxDescriptorRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
        session_name: Some("session_task_01".to_string()),
        socket_path: Some(socket_path.clone()),
        tmux_executable: None,
    })
    .expect("build tmux readonly descriptor");
    assert!(tmux_desc.read_only);
    assert!(!tmux_desc.mandatory_supervisor);
    assert_eq!(tmux_desc.worktree_kind, WorktreeKind::Task);
    assert_eq!(
        tmux_desc.attach_argv,
        vec![
            "tmux".to_string(),
            "-S".to_string(),
            socket_path.to_str().unwrap().to_string(),
            "attach-session".to_string(),
            "-r".to_string(),
            "-t".to_string(),
            "session_task_01".to_string(),
        ]
    );

    // 3. CodeGraph project path MUST be the actual owned worktree checkout; passing `repo_dir`
    // (the primary source checkout) or an unowned directory is refused!
    let unowned_err = inspect_codegraph_worktree(&work_area, &task_wt.worktree_id, &repo_dir)
        .expect_err("must refuse primary source repo as worktree codegraph project_path");
    assert_eq!(
        unowned_err.error_code(),
        OperationsErrorCode::WorktreeNotOwned
    );

    // 4. Initial CodeGraph freshness on the owned worktree checkout is `Missing`.
    let status_missing =
        inspect_codegraph_worktree(&work_area, &task_wt.worktree_id, &task_wt.checkout_path)
            .expect("inspect codegraph worktree");
    assert_eq!(status_missing.freshness, CodeGraphIndexFreshness::Missing);
    assert!(!status_missing.surface_distinction.mcp_is_cli);

    // Read-only index plan works with a validated CLI executable without running it.
    let fake_cg = fake_bin_dir.join("fake-codegraph");
    fs::write(&fake_cg, "#!/bin/sh\necho 'codegraph 0.1.0'\n").expect("write fake-codegraph");
    fs::set_permissions(&fake_cg, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    let cg_plan = propose_codegraph_index_plan(
        &work_area,
        &task_wt.worktree_id,
        &task_wt.checkout_path,
        &fake_cg,
    )
    .expect("propose codegraph index plan");
    assert!(cg_plan.read_only);
    assert_eq!(cg_plan.current_freshness, CodeGraphIndexFreshness::Missing);

    // 5. Record index receipt -> status becomes `Fresh`.
    let cg_receipt = record_codegraph_index_receipt(
        &work_area,
        &task_wt.worktree_id,
        &task_wt.checkout_path,
        "primary-codex",
        1_700_000_200_000,
    )
    .expect("record codegraph index receipt");
    let status_fresh =
        inspect_codegraph_worktree(&work_area, &task_wt.worktree_id, &task_wt.checkout_path)
            .expect("inspect fresh codegraph worktree");
    assert_eq!(
        status_fresh.freshness,
        CodeGraphIndexFreshness::Fresh {
            indexed_commit: cg_receipt.indexed_commit.clone(),
            indexed_tree_sha: cg_receipt.indexed_tree_sha.clone(),
            indexed_at_ms: 1_700_000_200_000,
        }
    );

    // 6. Modifying a file in the worktree immediately marks CodeGraph freshness `Stale` (`dirty_worktree = true`).
    fs::write(
        task_wt.checkout_path.join("src/lib.rs"),
        "pub fn compute() -> u32 {\n    333\n}\n",
    )
    .expect("modify src/lib.rs");
    let status_dirty_stale =
        inspect_codegraph_worktree(&work_area, &task_wt.worktree_id, &task_wt.checkout_path)
            .expect("inspect dirty stale codegraph worktree");
    match status_dirty_stale.freshness {
        CodeGraphIndexFreshness::Stale {
            dirty_worktree,
            indexed_commit,
            ..
        } => {
            assert!(dirty_worktree);
            assert_eq!(indexed_commit, base_commit);
        }
        other => panic!("expected Stale, got {other:?}"),
    }

    // 7. Committing the change cleans the worktree, but `HEAD` moved beyond `indexed_commit`,
    // so CodeGraph freshness remains `Stale` (`dirty_worktree = false`, `current_head_commit != indexed_commit`).
    let commit_rep = commit_task_worktree_changes(&CommitTaskChangesRequest {
        work_area_root: work_area.clone(),
        task_worktree_id: task_wt.worktree_id.clone(),
        commit_message: "Advance HEAD for CodeGraph staleness check".to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    })
    .expect("commit change");
    let status_head_stale =
        inspect_codegraph_worktree(&work_area, &task_wt.worktree_id, &task_wt.checkout_path)
            .expect("inspect committed stale codegraph worktree");
    match status_head_stale.freshness {
        CodeGraphIndexFreshness::Stale {
            dirty_worktree,
            indexed_commit,
            current_head_commit,
            ..
        } => {
            assert!(!dirty_worktree);
            assert_eq!(indexed_commit, base_commit);
            assert_eq!(current_head_commit, commit_rep.task_head_commit);
        }
        other => panic!("expected Stale after commit, got {other:?}"),
    }

    // Clean up an unused clean worktree via legacy `cleanup_worktree` to confirm existing behavior is preserved.
    let clean_wt = manager
        .create_task_worktree_with_token("run-cap", "task-clean", "tokcap02", &allowed)
        .expect("create clean task worktree");
    let legacy_cleanup = cleanup_worktree(&CleanupRequest {
        work_area_root: work_area,
        worktree_id: clean_wt.worktree_id,
    })
    .expect("legacy cleanup of unchanged worktree");
    assert!(!legacy_cleanup.removed_checkout_path.exists());
}
