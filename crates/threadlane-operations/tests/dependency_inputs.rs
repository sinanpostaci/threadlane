use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use threadlane_operations::{
    accept_review_subject, commit_task_worktree_changes, dependency_input_intent_path,
    dependency_input_snapshot_ref, execute_integration, inspect_dependency_input_seal_readonly,
    inspect_review_subject, prepare_and_seal_dependency_input, verify_dependency_predecessor_proof,
    verify_dependency_predecessor_receipts, CommitTaskChangesRequest,
    DependencyPredecessorCandidate, DependencyPreparationIntent, ExecuteIntegrationRequest,
    InspectReviewSubjectRequest, IntegrationReceipt, IntegrationStrategy, OperationsError,
    ReviewAcceptanceRequest, ReviewPolicyBinding, ReviewReceipt, TestEvidenceSummary,
    DEPENDENCIES_SUBDIR,
};
use threadlane_workers::{
    resolve_exact_commit_sha, resolve_exact_tree_sha, run_sanitized_git,
    run_sanitized_git_allow_nonzero, DependencyInputPolicy, WorkerFilePermissions, WorkerRunStatus,
    WorktreeManager, DEFAULT_GIT_COMMAND_TIMEOUT_MS, DEFAULT_MAX_GIT_OUTPUT_BYTES,
    DEPENDENCY_INPUT_SCHEMA_VERSION, MANIFESTS_SUBDIR,
};

struct OpsDependencyFixture {
    _tmp: TempDir,
    repo_path: PathBuf,
    work_area_root: PathBuf,
    base_commit: String,
}

impl OpsDependencyFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize root");
        let repo_path = root.join("repo");
        let work_area_root = root.join("work-area");
        fs::create_dir_all(repo_path.join("src")).expect("create repo src");

        run_sanitized_git(
            &repo_path,
            &["init", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git init");
        run_sanitized_git(
            &repo_path,
            &["config", "user.name", "Threadlane Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.name");
        run_sanitized_git(
            &repo_path,
            &["config", "user.email", "test@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.email");

        fs::write(
            repo_path.join("src/base.rs"),
            "pub fn base() -> u32 { 1 }\n",
        )
        .expect("write src/base.rs");
        run_sanitized_git(&repo_path, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            &repo_path,
            &["commit", "-m", "initial commit", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        let base_commit =
            resolve_exact_commit_sha(&repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("resolve base commit");

        let _manager = WorktreeManager::new(&repo_path, &work_area_root, &base_commit)
            .expect("init work-area");

        Self {
            _tmp: tmp,
            repo_path,
            work_area_root,
            base_commit,
        }
    }

    #[allow(clippy::too_many_arguments)] // Keep explicit execution and receipt timestamps in fixtures.
    fn create_accepted_integrated_task(
        &self,
        run_id: &str,
        logical_task_id: &str,
        execution_base_commit: &str,
        rel_path: &str,
        content: &str,
        approved_at_ms: i64,
        integrated_at_ms: i64,
    ) -> (String, String, ReviewReceipt, IntegrationReceipt) {
        let manager =
            WorktreeManager::new(&self.repo_path, &self.work_area_root, execution_base_commit)
                .expect("open worktree manager");
        let allowed_write_paths = BTreeSet::from([rel_path.to_string()]);
        let int_wt = manager
            .create_run_worktree(run_id)
            .expect("create integration worktree");
        let task_wt = manager
            .create_task_worktree(run_id, logical_task_id, &allowed_write_paths)
            .expect("create task worktree");

        let file_path = task_wt.checkout_path.join(rel_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).expect("create parent dir");
        }
        fs::write(&file_path, content).expect("write task file");

        let commit_report = commit_task_worktree_changes(&CommitTaskChangesRequest {
            work_area_root: self.work_area_root.clone(),
            task_worktree_id: task_wt.worktree_id.clone(),
            commit_message: format!("Implement {logical_task_id}"),
            worker_status: Some(WorkerRunStatus::Succeeded),
            allow_missing_effort_evidence: false,
        })
        .expect("commit task changes");

        let policy = ReviewPolicyBinding {
            policy_id: "m5b-policy".to_string(),
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
        };
        let test_evidence = TestEvidenceSummary {
            formatting_passed: true,
            clippy_passed: true,
            tests_passed: true,
            passed_test_count: 3,
            failed_test_count: 0,
            evidence_summary: "cargo test passed".to_string(),
        };

        let subject = inspect_review_subject(&InspectReviewSubjectRequest {
            work_area_root: self.work_area_root.clone(),
            task_worktree_id: task_wt.worktree_id.clone(),
            policy,
            test_evidence,
            worker_status: None,
        })
        .expect("inspect review subject");

        let review_receipt = accept_review_subject(&ReviewAcceptanceRequest {
            expected_subject_digest: subject.subject_digest.clone(),
            subject: subject.clone(),
            receipt_id: None,
            reviewer_id: "operator-1".to_string(),
            review_note: "Accepted for dependency chain".to_string(),
            approved_at_ms,
        })
        .expect("accept review subject");

        let integration_receipt = execute_integration(&ExecuteIntegrationRequest {
            work_area_root: self.work_area_root.clone(),
            integration_worktree_id: int_wt.worktree_id.clone(),
            task_worktree_id: task_wt.worktree_id.clone(),
            subject,
            review_receipt: review_receipt.clone(),
            expected_integration_head_commit: execution_base_commit.to_string(),
            expected_task_head_commit: commit_report.task_head_commit,
            expected_source_head_commit: self.base_commit.clone(),
            strategy: IntegrationStrategy::MergeNoFf,
            receipt_id: None,
            operator_id: "operator-1".to_string(),
            integrated_at_ms,
        })
        .expect("execute integration");

        (
            task_wt.worktree_id,
            int_wt.worktree_id,
            review_receipt,
            integration_receipt,
        )
    }
}

fn sha256_hex_str(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    format!("{:x}", h.finalize())
}

fn sample_policy(
    work_area_root: &Path,
    approval_id: &str,
    task_id: &str,
    instruction: &str,
) -> DependencyInputPolicy {
    DependencyInputPolicy {
        seal_path: work_area_root
            .join(MANIFESTS_SUBDIR)
            .join(DEPENDENCIES_SUBDIR)
            .join(format!("{task_id}.json")),
        approval_id: approval_id.to_string(),
        approval_subject_digest: "a".repeat(64),
        task_id: task_id.to_string(),
        task_digest: sha256_hex_str(&format!("task-digest:{task_id}")),
        instruction_digest: sha256_hex_str(instruction),
        command_key: format!("cmd_{task_id}"),
    }
}

#[test]
fn prepare_and_seal_composes_accepted_task_heads_only_and_supports_fan_in_and_chains() {
    let fixture = OpsDependencyFixture::new();
    let approval_id = "apr_0123456789abcdef0123456789abcdef";

    let (wt_a, int_a, rev_a, int_rec_a) = fixture.create_accepted_integrated_task(
        "run_11111111111111111111111111111111",
        "task-a",
        &fixture.base_commit,
        "src/a.rs",
        "pub fn a() -> u32 { 10 }\n",
        1_700_000_001_000,
        1_700_000_002_000,
    );
    let (wt_b, int_b, rev_b, int_rec_b) = fixture.create_accepted_integrated_task(
        "run_22222222222222222222222222222222",
        "task-b",
        &fixture.base_commit,
        "src/b.rs",
        "pub fn b() -> u32 { 20 }\n",
        1_700_000_003_000,
        1_700_000_004_000,
    );

    // Add an extra unrelated commit on integration checkout A and update its manifest/receipt
    // to prove composition uses ONLY `accepted_task_head_commit`, never `integrated_target_commit`.
    let int_a_checkout = int_rec_a.integration_checkout_path.clone();
    fs::write(
        int_a_checkout.join("src/unrelated_integration_only.rs"),
        "pub fn unrelated() {}\n",
    )
    .expect("write unrelated file in integration checkout");
    run_sanitized_git(
        &int_a_checkout,
        &["add", "."],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add unrelated");
    run_sanitized_git(
        &int_a_checkout,
        &["commit", "-m", "unrelated integration commit", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit unrelated");

    // Because integration checkout A moved away from `int_rec_a.integrated_target_commit`,
    // `verify_dependency_predecessor_proof` must reject it with `HeadMoved`!
    let cand_a = DependencyPredecessorCandidate {
        logical_task_id: "task-a".to_string(),
        logical_task_digest: sha256_hex_str("task-digest:task-a"),
        runtime_run_id: "run_11111111111111111111111111111111".to_string(),
        worker_id: "wrk_11111111111111111111111111111111".to_string(),
        integration_worktree_id: int_a.clone(),
        task_worktree_id: wt_a.clone(),
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: fixture.base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/a.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_a.receipt_id.clone(),
        integration_receipt_id: int_rec_a.receipt_id.clone(),
    };
    let head_moved_err = verify_dependency_predecessor_proof(&cand_a)
        .expect_err("integration worktree HEAD drift must fail");
    assert!(matches!(head_moved_err, OperationsError::HeadMoved { .. }));

    // Reset integration checkout A back to `int_rec_a.integrated_target_commit`.
    run_sanitized_git(
        &int_a_checkout,
        &[
            "reset",
            "--hard",
            &int_rec_a.integrated_target_commit,
            "--quiet",
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("reset integration checkout A");

    let proof_a = verify_dependency_predecessor_proof(&cand_a).expect("verify proof A");
    assert_eq!(proof_a.accepted_task_head_commit, rev_a.task_head_commit);
    assert_ne!(
        proof_a.accepted_task_head_commit, int_rec_a.integrated_target_commit,
        "MergeNoFf creates a distinct integration commit; proof must pin accepted_task_head_commit"
    );

    let cand_b = DependencyPredecessorCandidate {
        logical_task_id: "task-b".to_string(),
        logical_task_digest: sha256_hex_str("task-digest:task-b"),
        runtime_run_id: "run_22222222222222222222222222222222".to_string(),
        worker_id: "wrk_22222222222222222222222222222222".to_string(),
        integration_worktree_id: int_b,
        task_worktree_id: wt_b,
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: fixture.base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/b.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_b.receipt_id.clone(),
        integration_receipt_id: int_rec_b.receipt_id.clone(),
    };
    let proof_b = verify_dependency_predecessor_proof(&cand_b).expect("verify proof B");

    let instr_c = "Implement task-c combining A and B";
    let policy_c = sample_policy(&fixture.work_area_root, approval_id, "task-c", instr_c);

    // Read-only inspection before preparation must return None and leave disk clean.
    assert_eq!(
        inspect_dependency_input_seal_readonly(
            &policy_c,
            &fixture.repo_path,
            &fixture.work_area_root,
            &fixture.base_commit,
            instr_c,
            &policy_c.command_key,
        )
        .expect("readonly inspect before prepare"),
        None
    );
    assert!(!policy_c.seal_path.exists());

    // Seal fan-in inputs for task-c (passing B then A to verify deterministic sorting by task_id).
    let seal_c = prepare_and_seal_dependency_input(
        &policy_c,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_c,
        vec![proof_b.clone(), proof_a.clone()],
    )
    .expect("prepare and seal task-c");

    assert_eq!(seal_c.predecessors.len(), 2);
    assert_eq!(seal_c.predecessors[0].task_id, "task-a");
    assert_eq!(seal_c.predecessors[1].task_id, "task-b");
    assert_eq!(
        seal_c.snapshot_ref,
        dependency_input_snapshot_ref(approval_id, "task-c").unwrap()
    );

    // Verify `verify_dependency_predecessor_receipts` succeeds on the sealed input.
    verify_dependency_predecessor_receipts(&seal_c)
        .expect("verify_dependency_predecessor_receipts must succeed on valid seal");

    // Exact idempotent retry returns the same seal without mutating it.
    let seal_c_retry = prepare_and_seal_dependency_input(
        &policy_c,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_c,
        vec![proof_a.clone(), proof_b.clone()],
    )
    .expect("idempotent retry prepare task-c");
    assert_eq!(seal_c_retry, seal_c);

    // Read-only inspection returns the sealed DependencyInputSeal.
    let inspected_c = inspect_dependency_input_seal_readonly(
        &policy_c,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_c,
        &policy_c.command_key,
    )
    .expect("readonly inspect after prepare")
    .expect("seal exists");
    assert_eq!(inspected_c, seal_c);

    // Observation must neither recreate a missing lease lock nor report that input as ready.
    let lock_path = threadlane_workers::dependency_input_lock_path(&policy_c);
    let lock_bytes = fs::read(&lock_path).expect("read existing dependency lock");
    let lock_permissions = fs::metadata(&lock_path)
        .expect("lock metadata")
        .permissions();
    fs::remove_file(&lock_path).expect("remove dependency lock");
    assert!(inspect_dependency_input_seal_readonly(
        &policy_c,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_c,
        &policy_c.command_key,
    )
    .is_err());
    assert!(
        !lock_path.exists(),
        "read-only inspection recreated the lock"
    );
    fs::write(&lock_path, lock_bytes).expect("restore owned fixture lock");
    fs::set_permissions(&lock_path, lock_permissions).expect("restore lock permissions");

    // Multi-hop chain: task-c runs on `seal_c.execution_base_commit`, gets accepted and integrated,
    // and then task-d prepares against task-c!
    let (wt_c, int_c, rev_c, int_rec_c) = fixture.create_accepted_integrated_task(
        "run_33333333333333333333333333333333",
        "task-c",
        &seal_c.execution_base_commit,
        "src/c.rs",
        "pub fn c() -> u32 { 30 }\n",
        1_700_000_005_000,
        1_700_000_006_000,
    );
    let cand_c = DependencyPredecessorCandidate {
        logical_task_id: "task-c".to_string(),
        logical_task_digest: policy_c.task_digest.clone(),
        runtime_run_id: "run_33333333333333333333333333333333".to_string(),
        worker_id: "wrk_33333333333333333333333333333333".to_string(),
        integration_worktree_id: int_c,
        task_worktree_id: wt_c,
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: seal_c.execution_base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/c.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_c.receipt_id.clone(),
        integration_receipt_id: int_rec_c.receipt_id.clone(),
    };
    let proof_c = verify_dependency_predecessor_proof(&cand_c).expect("verify proof C");

    let instr_d = "Implement task-d on top of task-c";
    let policy_d = sample_policy(&fixture.work_area_root, approval_id, "task-d", instr_d);
    let seal_d = prepare_and_seal_dependency_input(
        &policy_d,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_d,
        vec![proof_c],
    )
    .expect("prepare and seal task-d");
    verify_dependency_predecessor_receipts(&seal_d)
        .expect("verify_dependency_predecessor_receipts for task-d");

    // Verify `seal_d.execution_base_commit` contains `src/base.rs`, `src/a.rs`, `src/b.rs`, and `src/c.rs`.
    let tree_files = run_sanitized_git(
        &fixture.repo_path,
        &[
            "ls-tree",
            "-r",
            "--name-only",
            &seal_d.execution_base_commit,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("ls-tree on seal_d");
    let file_set: BTreeSet<&str> = tree_files.lines().collect();
    assert!(file_set.contains("src/base.rs"));
    assert!(file_set.contains("src/a.rs"));
    assert!(file_set.contains("src/b.rs"));
    assert!(file_set.contains("src/c.rs"));
    assert!(!file_set.contains("src/unrelated_integration_only.rs"));
}

#[test]
fn prepare_and_seal_rejects_merge_conflict_conflicting_reseal_and_reconciles_interrupted_intent() {
    let fixture = OpsDependencyFixture::new();
    let approval_id = "apr_fedcba9876543210fedcba9876543210";

    let (wt_left, int_left, rev_left, int_rec_left) = fixture.create_accepted_integrated_task(
        "run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "task-left",
        &fixture.base_commit,
        "src/base.rs",
        "pub fn base() -> u32 { 111 }\n",
        1_700_000_010_000,
        1_700_000_011_000,
    );
    let (wt_right, int_right, rev_right, int_rec_right) = fixture.create_accepted_integrated_task(
        "run_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "task-right",
        &fixture.base_commit,
        "src/base.rs",
        "pub fn base() -> u32 { 222 }\n",
        1_700_000_012_000,
        1_700_000_013_000,
    );

    let proof_left = verify_dependency_predecessor_proof(&DependencyPredecessorCandidate {
        logical_task_id: "task-left".to_string(),
        logical_task_digest: sha256_hex_str("task-digest:task-left"),
        runtime_run_id: "run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        worker_id: "wrk_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        integration_worktree_id: int_left,
        task_worktree_id: wt_left,
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: fixture.base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/base.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_left.receipt_id.clone(),
        integration_receipt_id: int_rec_left.receipt_id.clone(),
    })
    .expect("proof left");

    let proof_right = verify_dependency_predecessor_proof(&DependencyPredecessorCandidate {
        logical_task_id: "task-right".to_string(),
        logical_task_digest: sha256_hex_str("task-digest:task-right"),
        runtime_run_id: "run_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        worker_id: "wrk_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        integration_worktree_id: int_right,
        task_worktree_id: wt_right,
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: fixture.base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/base.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_right.receipt_id.clone(),
        integration_receipt_id: int_rec_right.receipt_id.clone(),
    })
    .expect("proof right");

    let instr_conflict = "Combine conflicting left and right";
    let policy_conflict = sample_policy(
        &fixture.work_area_root,
        approval_id,
        "task-conflict",
        instr_conflict,
    );
    let conflict_err = prepare_and_seal_dependency_input(
        &policy_conflict,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_conflict,
        vec![proof_left.clone(), proof_right.clone()],
    )
    .expect_err("conflicting fan-in must fail closed with MergeConflict");
    assert!(matches!(conflict_err, OperationsError::MergeConflict(_)));
    assert!(!policy_conflict.seal_path.exists());
    assert!(!dependency_input_intent_path(&policy_conflict).exists());

    // Seal a valid single-predecessor child, then verify that attempting to re-seal with a
    // different predecessor fails closed without overwriting the seal.
    let instr_single = "Single predecessor child";
    let policy_single = sample_policy(
        &fixture.work_area_root,
        approval_id,
        "task-single",
        instr_single,
    );
    let original_seal = prepare_and_seal_dependency_input(
        &policy_single,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_single,
        vec![proof_left.clone()],
    )
    .expect("seal single");

    let reseal_err = prepare_and_seal_dependency_input(
        &policy_single,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_single,
        vec![proof_right],
    )
    .expect_err("conflicting re-seal must fail closed");
    assert!(matches!(reseal_err, OperationsError::ReceiptInvalidated(_)));

    // Simulate an interrupted preparation where `.intent.json` and the snapshot ref exist,
    // but the final `.json` seal file was not yet linked.
    fs::remove_file(&policy_single.seal_path).expect("remove seal to simulate crash after ref CAS");
    assert_eq!(
        inspect_dependency_input_seal_readonly(
            &policy_single,
            &fixture.repo_path,
            &fixture.work_area_root,
            &fixture.base_commit,
            instr_single,
            &policy_single.command_key,
        )
        .expect("readonly inspect with intent only must return None without reconciling"),
        None
    );
    assert!(
        !policy_single.seal_path.exists(),
        "readonly inspect must never reconcile or create seal file"
    );

    let reconciled_seal = prepare_and_seal_dependency_input(
        &policy_single,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr_single,
        vec![proof_left],
    )
    .expect("reconcile interrupted intent on explicit retry");
    assert_eq!(reconciled_seal, original_seal);
    assert!(policy_single.seal_path.exists());

    // Verify that tampering with the predecessor's review receipt on disk causes
    // `verify_dependency_predecessor_receipts` to fail closed!
    let rev_left_path = fixture
        .work_area_root
        .join(MANIFESTS_SUBDIR)
        .join("receipts")
        .join(format!("review-{}.json", rev_left.receipt_id));
    let original_rev_bytes = fs::read(&rev_left_path).expect("read review receipt");
    let tampered_rev = String::from_utf8(original_rev_bytes.clone())
        .expect("utf8")
        .replace("operator-1", "tampered-operator");
    fs::write(&rev_left_path, tampered_rev.as_bytes()).expect("write tampered review receipt");

    assert!(
        verify_dependency_predecessor_receipts(&reconciled_seal).is_err(),
        "verify_dependency_predecessor_receipts must reject tampered predecessor review receipt"
    );

    // Restore valid review receipt bytes and confirm verification succeeds again.
    fs::write(&rev_left_path, &original_rev_bytes).expect("restore review receipt");
    let _ = resolve_exact_tree_sha(
        &fixture.repo_path,
        &reconciled_seal.execution_base_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve tree");
    verify_dependency_predecessor_receipts(&reconciled_seal)
        .expect("restored review receipt must pass verification");
    let _intent: DependencyPreparationIntent = serde_json::from_slice(
        &fs::read(dependency_input_intent_path(&policy_single)).expect("read intent"),
    )
    .expect("parse intent");
    assert_eq!(_intent.schema_version, DEPENDENCY_INPUT_SCHEMA_VERSION);
}

fn assert_ref_absent(repo_path: &Path, ref_name: &str) {
    let out = run_sanitized_git_allow_nonzero(
        repo_path,
        &["show-ref", "--verify", "--quiet", "--", ref_name],
        DEFAULT_MAX_GIT_OUTPUT_BYTES,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("probe ref existence");
    assert_ne!(
        out.exit_code,
        Some(0),
        "expected foreign ref `{ref_name}` to remain absent"
    );
}

fn assert_primary_checkout_unchanged(
    repo_path: &Path,
    expected_branch: &str,
    expected_head: &str,
    expected_status: &str,
    expected_base_rs: &str,
) {
    let branch_now = run_sanitized_git(
        repo_path,
        &["symbolic-ref", "--quiet", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve primary HEAD symbolic-ref");
    assert_eq!(branch_now.trim(), expected_branch);

    let head_now = resolve_exact_commit_sha(repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve primary HEAD commit");
    assert_eq!(head_now, expected_head);

    let status_now = run_sanitized_git(
        repo_path,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("primary git status");
    assert_eq!(status_now, expected_status);

    let base_rs_now =
        fs::read_to_string(repo_path.join("src/base.rs")).expect("read primary src/base.rs");
    assert_eq!(base_rs_now, expected_base_rs);
}

#[test]
fn prepare_and_reconcile_reject_symbolic_snapshot_refs_and_preserve_foreign_refs() {
    let fixture = OpsDependencyFixture::new();
    let approval_id = "apr_99887766554433221100ffeeddccbbaa";

    let primary_branch_before = run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", "--quiet", "HEAD"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("primary branch before")
    .trim()
    .to_string();
    let primary_status_before = run_sanitized_git(
        &fixture.repo_path,
        &["status", "--porcelain=v1", "-uall"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("primary status before");
    let primary_base_rs_before = fs::read_to_string(fixture.repo_path.join("src/base.rs"))
        .expect("read primary src/base.rs before");

    let (wt_pred, int_pred, rev_pred, int_rec_pred) = fixture.create_accepted_integrated_task(
        "run_12121212121212121212121212121212",
        "task-pred",
        &fixture.base_commit,
        "src/pred.rs",
        "pub fn pred() -> u32 { 42 }\n",
        1_700_000_020_000,
        1_700_000_021_000,
    );

    let proof_pred = verify_dependency_predecessor_proof(&DependencyPredecessorCandidate {
        logical_task_id: "task-pred".to_string(),
        logical_task_digest: sha256_hex_str("task-digest:task-pred"),
        runtime_run_id: "run_12121212121212121212121212121212".to_string(),
        worker_id: "wrk_12121212121212121212121212121212".to_string(),
        integration_worktree_id: int_pred,
        task_worktree_id: wt_pred,
        repository_path: fixture.repo_path.clone(),
        work_area_root: fixture.work_area_root.clone(),
        approved_base_commit: fixture.base_commit.clone(),
        expected_execution_base_commit: fixture.base_commit.clone(),
        allowed_write_paths: BTreeSet::from(["src/pred.rs".to_string()]),
        expected_provider: "antigravity".to_string(),
        expected_model: "gemini-4-argon-high".to_string(),
        expected_effort: Some("high".to_string()),
        review_receipt_id: rev_pred.receipt_id,
        integration_receipt_id: int_rec_pred.receipt_id,
    })
    .expect("verify predecessor proof");

    let instr = "Implement dependent task-symref";
    let policy = sample_policy(&fixture.work_area_root, approval_id, "task-symref", instr);
    let intent_path = dependency_input_intent_path(&policy);
    let snapshot_ref =
        dependency_input_snapshot_ref(approval_id, "task-symref").expect("snapshot ref");

    let dangling_foreign_ref = "refs/heads/foreign-branch";
    let populated_foreign_ref = "refs/heads/populated-foreign-branch";
    let matching_foreign_ref = "refs/heads/matching-foreign-branch";

    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);

    // 1. Initial preparation with a dangling symbolic owned snapshot ref MUST fail closed
    //    without creating `refs/heads/foreign-branch`, writing `.intent.json`, or sealing.
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", &snapshot_ref, dangling_foreign_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("create dangling symbolic snapshot_ref");

    let readonly_dangling_err = inspect_dependency_input_seal_readonly(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        &policy.command_key,
    )
    .expect_err("readonly inspect must reject dangling symbolic snapshot_ref");
    assert!(matches!(
        readonly_dangling_err,
        OperationsError::SecurityViolation(_)
    ));

    let init_dangling_err = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect_err("initial prepare must reject dangling symbolic snapshot_ref");
    assert!(matches!(
        init_dangling_err,
        OperationsError::SecurityViolation(_)
    ));
    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);
    assert!(!intent_path.exists());
    assert!(!policy.seal_path.exists());
    assert_primary_checkout_unchanged(
        &fixture.repo_path,
        &primary_branch_before,
        &fixture.base_commit,
        &primary_status_before,
        &primary_base_rs_before,
    );

    // 2. Initial preparation with a populated symbolic owned snapshot ref MUST fail closed
    //    without moving `refs/heads/populated-foreign-branch`.
    run_sanitized_git(
        &fixture.repo_path,
        &[
            "update-ref",
            "--no-deref",
            "--",
            populated_foreign_ref,
            &fixture.base_commit,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("create populated foreign branch at base_commit");
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", &snapshot_ref, populated_foreign_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("point snapshot_ref to populated foreign branch");

    let init_populated_err = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect_err("initial prepare must reject populated symbolic snapshot_ref");
    assert!(matches!(
        init_populated_err,
        OperationsError::SecurityViolation(_)
    ));
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.repo_path,
            populated_foreign_ref,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("resolve populated foreign branch"),
        fixture.base_commit
    );
    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);
    assert!(!intent_path.exists());
    assert!(!policy.seal_path.exists());
    assert_primary_checkout_unchanged(
        &fixture.repo_path,
        &primary_branch_before,
        &fixture.base_commit,
        &primary_status_before,
        &primary_base_rs_before,
    );

    // Remove the symbolic ref and perform a valid preparation so `.intent.json` is recorded,
    // then remove the seal file to exercise interrupted-journal reconciliation.
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", "--delete", &snapshot_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("delete symbolic snapshot_ref");

    let expected_seal = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect("clean initial prepare after deleting symbolic ref");
    assert!(intent_path.exists());
    assert!(policy.seal_path.exists());
    fs::remove_file(&policy.seal_path).expect("remove seal to simulate interrupted prepare");

    // 3. Interrupted-journal reconciliation with a dangling symbolic owned snapshot ref MUST
    //    fail closed without creating `refs/heads/foreign-branch` or sealing.
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", &snapshot_ref, dangling_foreign_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("replace snapshot_ref with dangling symbolic ref before reconcile");
    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);

    let reconcile_dangling_err = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect_err("interrupted-journal reconcile must reject dangling symbolic snapshot_ref");
    assert!(matches!(
        reconcile_dangling_err,
        OperationsError::SecurityViolation(_)
    ));
    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);
    assert!(!policy.seal_path.exists());
    assert_primary_checkout_unchanged(
        &fixture.repo_path,
        &primary_branch_before,
        &fixture.base_commit,
        &primary_status_before,
        &primary_base_rs_before,
    );

    // 4. Interrupted-journal reconciliation with a populated symbolic target (both at `base_commit`
    //    and at `expected_seal.execution_base_commit`) MUST fail closed without moving the foreign
    //    target or treating the symbolic ref as a valid existing recipe.
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", &snapshot_ref, populated_foreign_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("point snapshot_ref to populated foreign branch for reconcile");

    let reconcile_populated_err = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect_err("interrupted-journal reconcile must reject populated symbolic snapshot_ref");
    assert!(matches!(
        reconcile_populated_err,
        OperationsError::SecurityViolation(_)
    ));
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.repo_path,
            populated_foreign_ref,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("resolve populated foreign branch after rejected reconcile"),
        fixture.base_commit
    );
    assert!(!policy.seal_path.exists());

    run_sanitized_git(
        &fixture.repo_path,
        &[
            "update-ref",
            "--no-deref",
            "--",
            matching_foreign_ref,
            &expected_seal.execution_base_commit,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("create matching foreign branch at execution_base_commit");
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", &snapshot_ref, matching_foreign_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("point snapshot_ref to matching foreign branch for reconcile");

    let reconcile_matching_sym_err = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred.clone()],
    )
    .expect_err("reconcile must reject symbolic ref at execution_base_commit");
    assert!(matches!(
        reconcile_matching_sym_err,
        OperationsError::SecurityViolation(_)
    ));
    let verify_sym_err = verify_dependency_predecessor_receipts(&expected_seal)
        .expect_err("receipt verification must also reject symbolic snapshot_ref");
    assert!(matches!(
        verify_sym_err,
        OperationsError::SecurityViolation(_)
    ));
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.repo_path,
            matching_foreign_ref,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("resolve matching foreign branch"),
        expected_seal.execution_base_commit
    );
    assert!(!policy.seal_path.exists());

    // 5. Once the bogus symbolic ref is removed, interrupted-journal reconciliation succeeds
    //    via `--no-deref` compare-and-swap creation and leaves foreign refs and primary checkout untouched.
    run_sanitized_git(
        &fixture.repo_path,
        &["symbolic-ref", "--delete", &snapshot_ref],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("delete symbolic snapshot_ref before final reconcile");

    let reconciled = prepare_and_seal_dependency_input(
        &policy,
        &fixture.repo_path,
        &fixture.work_area_root,
        &fixture.base_commit,
        instr,
        vec![proof_pred],
    )
    .expect("reconcile interrupted intent after symbolic ref removal");
    assert_eq!(reconciled, expected_seal);
    assert!(policy.seal_path.exists());
    assert_ref_absent(&fixture.repo_path, dangling_foreign_ref);
    assert_eq!(
        resolve_exact_commit_sha(
            &fixture.repo_path,
            populated_foreign_ref,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("populated foreign branch still at base_commit"),
        fixture.base_commit
    );
    assert_primary_checkout_unchanged(
        &fixture.repo_path,
        &primary_branch_before,
        &fixture.base_commit,
        &primary_status_before,
        &primary_base_rs_before,
    );
}
