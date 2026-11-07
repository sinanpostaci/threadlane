#![cfg(unix)]
//! End-to-end behavioral tests for the `threadlane-operator` CLI.
//!
//! These tests exercise the real binary against private, disposable repositories and owned
//! `WorktreeManager` fixtures. They never contact a provider or perform inference.

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane_operations::{
    load_integration_receipt, load_review_receipt, CommitTaskChangesRequest,
    ExecuteIntegrationRequest, InspectReviewSubjectRequest, IntegrationProposal,
    IntegrationProposalRequest, IntegrationReceipt, IntegrationStrategy, ReviewAcceptanceRequest,
    ReviewPolicyBinding, ReviewReceipt, ReviewSubject, TaskCommitReport, TestEvidenceSummary,
};
use threadlane_workers::{
    is_commit_ancestor, is_worktree_clean, resolve_exact_commit_sha, resolve_exact_tree_sha,
    run_sanitized_git, DiffChangeKind, WorkerFilePermissions, WorkerRunStatus, WorktreeManager,
    WorktreeManifest, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

const RUN_ID: &str = "run-cli";
const TASK_ID: &str = "task-cli";
const TASK_LIB_PATH: &str = "src/lib.rs";
const ORIGINAL_LIB: &str = "pub fn compute() -> u32 {\n    10\n}\n";
const UPDATED_LIB: &str = "pub fn compute() -> u32 {\n    77\n}\n";
const ADVANCED_LIB: &str = "pub fn compute() -> u32 {\n    101\n}\n";
const ORIGINAL_README: &str = "# Disposable Repo\n";
const SECRET_SENTINEL: &str = "THREADLANE_OPERATOR_SECRET_SENTINEL_7d1f6e4a";
const MAX_REQUEST_FILE_BYTES: u64 = 1_048_576;

struct Fixture {
    root: TempDir,
    repository_path: PathBuf,
    work_area_root: PathBuf,
    base_commit: String,
    integration: WorktreeManifest,
    task: WorktreeManifest,
}

impl Fixture {
    fn new() -> Self {
        let root = create_private_tempdir();
        let repository_path = root.path().join("repo");
        let base_commit = init_disposable_repo(&repository_path);
        let work_area_path = root.path().join("work_area");

        let manager = WorktreeManager::new(&repository_path, &work_area_path, &base_commit)
            .expect("create worktree manager");
        let integration = manager
            .create_run_worktree_with_token(RUN_ID, "tokint01")
            .expect("create integration worktree");
        let mut allowed = BTreeSet::new();
        allowed.insert("src".to_string());
        let task = manager
            .create_task_worktree_with_token(RUN_ID, TASK_ID, "toktask01", &allowed)
            .expect("create task worktree");

        Self {
            repository_path: manager.repository_path().to_path_buf(),
            work_area_root: manager.work_area_root().to_path_buf(),
            base_commit,
            integration,
            task,
            root,
        }
    }

    fn requests_dir(&self) -> PathBuf {
        self.root.path().join("requests")
    }

    fn write_request<T: Serialize>(&self, name: &str, request: &T) -> PathBuf {
        write_private_request(&self.requests_dir(), name, request)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct WorkspaceSnapshot {
    source_head: String,
    task_head: String,
    integration_head: String,
    source_status: String,
    task_status: String,
    integration_status: String,
    source_lib: String,
    source_readme: String,
    task_lib: String,
    integration_lib: String,
    task_clean: bool,
    receipt_files: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CliFailure {
    code: String,
    message: String,
}

fn create_private_tempdir() -> TempDir {
    let dir = tempfile::tempdir().expect("create tempdir");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("chmod 0700 tempdir");
    dir
}

fn init_disposable_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir).expect("create repo dir");
    fs::set_permissions(repo_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 repo");
    run_git(repo_dir, &["init", "-q", "-b", "main"]);

    fs::create_dir_all(repo_dir.join("src")).expect("create src dir");
    fs::create_dir_all(repo_dir.join("docs")).expect("create docs dir");
    fs::write(repo_dir.join(TASK_LIB_PATH), ORIGINAL_LIB).expect("write src/lib.rs");
    fs::write(repo_dir.join("docs/README.md"), ORIGINAL_README).expect("write docs/README.md");
    run_git(repo_dir, &["add", "--", TASK_LIB_PATH, "docs/README.md"]);
    commit_git(repo_dir, "Initial commit");

    resolve_head(repo_dir)
}

fn write_private_bytes(path: &Path, bytes: &[u8]) {
    let parent = path.parent().expect("request path has a parent");
    fs::create_dir_all(parent).expect("create request parent");
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 request parent");
    fs::write(path, bytes).expect("write private request file");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600 request file");
}

fn write_private_request<T: Serialize>(dir: &Path, name: &str, request: &T) -> PathBuf {
    let path = dir.join(name);
    assert!(path.is_absolute(), "request path must be absolute");
    let bytes = serde_json::to_vec(request).expect("serialize request");
    write_private_bytes(&path, &bytes);
    path
}

fn run_git(dir: &Path, args: &[&str]) -> String {
    run_sanitized_git(dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("git {args:?} in {} failed: {err}", dir.display()))
}

fn commit_git(dir: &Path, message: &str) {
    run_git(
        dir,
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
            message,
        ],
    );
}

fn commit_worktree_file(
    checkout_path: &Path,
    relative_path: &str,
    contents: &str,
    message: &str,
) -> String {
    let path = checkout_path.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create file parent");
    }
    fs::write(&path, contents).expect("write worktree file");
    run_git(checkout_path, &["add", "--", relative_path]);
    commit_git(checkout_path, message);
    resolve_head(checkout_path)
}

fn resolve_head(dir: &Path) -> String {
    resolve_exact_commit_sha(dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("resolve HEAD in {}: {err}", dir.display()))
}

fn resolve_tree(dir: &Path) -> String {
    resolve_exact_tree_sha(dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("resolve HEAD tree in {}: {err}", dir.display()))
}

fn is_clean(dir: &Path) -> bool {
    is_worktree_clean(dir, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .unwrap_or_else(|err| panic!("inspect cleanliness of {}: {err}", dir.display()))
}

fn receipt_file_names(work_area_root: &Path) -> Vec<String> {
    let receipts_dir = work_area_root.join("manifests").join("receipts");
    match fs::read_dir(&receipts_dir) {
        Ok(entries) => {
            let mut names = entries
                .map(|entry| {
                    entry
                        .expect("read receipt directory entry")
                        .file_name()
                        .into_string()
                        .expect("receipt filename must be UTF-8")
                })
                .collect::<Vec<_>>();
            names.sort();
            names
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("read receipt directory {}: {err}", receipts_dir.display()),
    }
}

fn worktree_status(dir: &Path) -> String {
    run_git(dir, &["status", "--porcelain=v1", "--untracked-files=all"])
}

fn snapshot_workspace(fx: &Fixture) -> WorkspaceSnapshot {
    WorkspaceSnapshot {
        source_head: resolve_head(&fx.repository_path),
        task_head: resolve_head(&fx.task.checkout_path),
        integration_head: resolve_head(&fx.integration.checkout_path),
        source_status: worktree_status(&fx.repository_path),
        task_status: worktree_status(&fx.task.checkout_path),
        integration_status: worktree_status(&fx.integration.checkout_path),
        source_lib: fs::read_to_string(fx.repository_path.join(TASK_LIB_PATH))
            .expect("read primary src/lib.rs"),
        source_readme: fs::read_to_string(fx.repository_path.join("docs/README.md"))
            .expect("read primary docs/README.md"),
        task_lib: fs::read_to_string(fx.task.checkout_path.join(TASK_LIB_PATH))
            .expect("read task src/lib.rs"),
        integration_lib: fs::read_to_string(fx.integration.checkout_path.join(TASK_LIB_PATH))
            .expect("read integration src/lib.rs"),
        task_clean: is_clean(&fx.task.checkout_path),
        receipt_files: receipt_file_names(&fx.work_area_root),
    }
}

fn assert_workspace_unchanged(fx: &Fixture, before: &WorkspaceSnapshot, context: &str) {
    let after = snapshot_workspace(fx);
    assert_eq!(
        after, *before,
        "{context}: rejected CLI request mutated the repository, worktrees, or receipts"
    );
}

fn strict_policy() -> ReviewPolicyBinding {
    ReviewPolicyBinding {
        policy_id: "m5a-operator-cli-strict-policy".to_string(),
        codex_launch_report: None,
        codex_source_report_sha256: None,
        requested_provider: Some("fixture-provider".to_string()),
        requested_model: Some("fixture-model-1".to_string()),
        requested_effort: Some("high".to_string()),
        observed_model: Some("fixture-model-1".to_string()),
        observed_effort: Some("high".to_string()),
        file_permissions: WorkerFilePermissions::TaskFiles,
        require_effort_evidence: true,
        require_passing_tests: true,
    }
}

// The evidence summary is an operator attestation for the fixture. It is not new reviewer
// telemetry, and the CLI must carry it through unchanged rather than inventing evidence.
fn operator_attested_test_evidence() -> TestEvidenceSummary {
    TestEvidenceSummary {
        formatting_passed: true,
        clippy_passed: true,
        tests_passed: true,
        passed_test_count: 7,
        failed_test_count: 0,
        evidence_summary: "Operator-attested prior validation summary for this fixture; no new reviewer telemetry was collected.".to_string(),
    }
}

fn commit_request(fx: &Fixture, message: &str) -> CommitTaskChangesRequest {
    CommitTaskChangesRequest {
        work_area_root: fx.work_area_root.clone(),
        task_worktree_id: fx.task.worktree_id.clone(),
        commit_message: message.to_string(),
        worker_status: Some(WorkerRunStatus::Succeeded),
        allow_missing_effort_evidence: false,
    }
}

fn inspect_request(fx: &Fixture) -> InspectReviewSubjectRequest {
    InspectReviewSubjectRequest {
        work_area_root: fx.work_area_root.clone(),
        task_worktree_id: fx.task.worktree_id.clone(),
        policy: strict_policy(),
        test_evidence: operator_attested_test_evidence(),
        worker_status: Some(WorkerRunStatus::Succeeded),
    }
}

fn review_acceptance_request(subject: &ReviewSubject, receipt_id: &str) -> ReviewAcceptanceRequest {
    ReviewAcceptanceRequest {
        subject: subject.clone(),
        expected_subject_digest: subject.subject_digest.clone(),
        receipt_id: Some(receipt_id.to_string()),
        reviewer_id: "operator-attestation".to_string(),
        review_note: "Operator reviewed the committed diff and attested evidence.".to_string(),
        approved_at_ms: 1_700_100_000_000,
    }
}

fn integration_proposal_request(
    fx: &Fixture,
    subject: &ReviewSubject,
    review_receipt: &ReviewReceipt,
    strategy: IntegrationStrategy,
) -> IntegrationProposalRequest {
    IntegrationProposalRequest {
        work_area_root: fx.work_area_root.clone(),
        integration_worktree_id: fx.integration.worktree_id.clone(),
        task_worktree_id: fx.task.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        strategy,
    }
}

fn execute_integration_request(
    fx: &Fixture,
    subject: &ReviewSubject,
    review_receipt: &ReviewReceipt,
    expected_integration_head: &str,
    strategy: IntegrationStrategy,
    receipt_id: &str,
) -> ExecuteIntegrationRequest {
    ExecuteIntegrationRequest {
        work_area_root: fx.work_area_root.clone(),
        integration_worktree_id: fx.integration.worktree_id.clone(),
        task_worktree_id: fx.task.worktree_id.clone(),
        subject: subject.clone(),
        review_receipt: review_receipt.clone(),
        expected_integration_head_commit: expected_integration_head.to_string(),
        expected_task_head_commit: subject.task_head_commit.clone(),
        expected_source_head_commit: fx.base_commit.clone(),
        strategy,
        receipt_id: Some(receipt_id.to_string()),
        operator_id: "operator-attestation".to_string(),
        integrated_at_ms: 1_700_100_010_000,
    }
}

fn operator_command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_threadlane-operator"));
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}

fn run_operator(args: &[&str]) -> Output {
    operator_command()
        .args(args)
        .output()
        .expect("run threadlane-operator")
}

fn run_operator_in(current_dir: &Path, args: &[&str]) -> Output {
    operator_command()
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("run threadlane-operator in fixture directory")
}

fn commit_task_cli(request_path: &Path, confirm_task: &str) -> Output {
    let request = request_path.to_str().expect("request path must be UTF-8");
    run_operator(&[
        "commit-task",
        "--request",
        request,
        "--confirm-task",
        confirm_task,
    ])
}

fn inspect_review_cli(request_path: &Path) -> Output {
    let request = request_path.to_str().expect("request path must be UTF-8");
    run_operator(&["inspect-review", "--request", request])
}

fn accept_review_cli(request_path: &Path, confirm_subject: &str) -> Output {
    let request = request_path.to_str().expect("request path must be UTF-8");
    run_operator(&[
        "accept-review",
        "--request",
        request,
        "--confirm-subject",
        confirm_subject,
    ])
}

fn propose_integration_cli(request_path: &Path) -> Output {
    let request = request_path.to_str().expect("request path must be UTF-8");
    run_operator(&["propose-integration", "--request", request])
}

fn integrate_cli(request_path: &Path, confirm_subject: &str, confirm_target: &str) -> Output {
    let request = request_path.to_str().expect("request path must be UTF-8");
    run_operator(&[
        "integrate",
        "--request",
        request,
        "--confirm-subject",
        confirm_subject,
        "--confirm-target",
        confirm_target,
    ])
}

fn parse_success<T: DeserializeOwned>(output: &Output, context: &str) -> T {
    assert!(
        output.status.success(),
        "{context} failed with status {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!(
            "{context}: stdout must contain exactly one DTO JSON document: {err}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn expect_cli_failure(output: &Output, expected_code: &str, context: &str) -> CliFailure {
    assert!(
        !output.status.success(),
        "{context}: expected a nonzero exit, got success\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        output.stdout.is_empty(),
        "{context}: failures must not write a DTO to stdout\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let failure: CliFailure = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "{context}: stderr must contain exactly one safe JSON error envelope: {err}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(
        failure.code,
        expected_code,
        "{context}: unexpected error code\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !failure.message.trim().is_empty(),
        "{context}: error message must be non-empty"
    );
    failure
}

fn assert_secret_not_echoed(output: &Output, context: &str) {
    for (stream, bytes) in [
        ("stdout", output.stdout.as_slice()),
        ("stderr", output.stderr.as_slice()),
    ] {
        let text = String::from_utf8_lossy(bytes);
        assert!(
            !text.contains(SECRET_SENTINEL),
            "{context}: request secret was echoed on {stream}: {text}"
        );
    }
}

fn assert_sha40(value: &str, label: &str) {
    assert_eq!(value.len(), 40, "{label} must be a 40-hex SHA: {value}");
    assert!(
        value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{label} must be hexadecimal: {value}"
    );
}

fn assert_sha256(value: &str, label: &str) {
    assert_eq!(value.len(), 64, "{label} must be a 64-hex digest: {value}");
    assert!(
        value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{label} must be hexadecimal: {value}"
    );
}

fn different_digest(actual: &str) -> String {
    let candidate = "0".repeat(64);
    if candidate.as_str() == actual {
        "1".repeat(64)
    } else {
        candidate
    }
}

fn different_commit(actual: &str) -> String {
    let candidate = "0".repeat(40);
    if candidate.as_str() == actual {
        "1".repeat(40)
    } else {
        candidate
    }
}

fn commit_task_ok(fx: &Fixture, message: &str, file_name: &str) -> TaskCommitReport {
    let request = commit_request(fx, message);
    let path = fx.write_request(file_name, &request);
    parse_success(&commit_task_cli(&path, &fx.task.worktree_id), "commit-task")
}

fn inspect_review_ok(fx: &Fixture, file_name: &str) -> ReviewSubject {
    let request = inspect_request(fx);
    let path = fx.write_request(file_name, &request);
    parse_success(&inspect_review_cli(&path), "inspect-review")
}

fn accept_review_ok(
    fx: &Fixture,
    subject: &ReviewSubject,
    receipt_id: &str,
    file_name: &str,
) -> ReviewReceipt {
    let request = review_acceptance_request(subject, receipt_id);
    let path = fx.write_request(file_name, &request);
    parse_success(
        &accept_review_cli(&path, &subject.subject_digest),
        "accept-review",
    )
}

#[test]
fn full_operator_cli_flow_preserves_primary_and_integrates_the_exact_reviewed_tree() {
    let fx = Fixture::new();
    let primary_before = snapshot_workspace(&fx);
    assert_eq!(primary_before.source_head, fx.base_commit);
    assert!(receipt_file_names(&fx.work_area_root).is_empty());

    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let commit_report: TaskCommitReport =
        commit_task_ok(&fx, "Update compute() to 77", "commit-e2e.json");
    assert_sha40(&commit_report.task_head_commit, "task_head_commit");
    assert_sha40(&commit_report.task_tree_sha, "task_tree_sha");
    assert_eq!(commit_report.task_worktree_id, fx.task.worktree_id);
    assert_eq!(commit_report.branch_name, fx.task.branch_name);
    assert_eq!(commit_report.base_commit, fx.base_commit);
    assert_ne!(commit_report.task_head_commit, fx.base_commit);
    assert_eq!(commit_report.changed_paths_count, 1);
    assert_eq!(
        resolve_head(&fx.task.checkout_path),
        commit_report.task_head_commit
    );
    assert_eq!(
        resolve_tree(&fx.task.checkout_path),
        commit_report.task_tree_sha
    );
    assert!(is_clean(&fx.task.checkout_path));
    assert_eq!(
        fs::read_to_string(fx.task.checkout_path.join(TASK_LIB_PATH))
            .expect("read committed task file"),
        UPDATED_LIB
    );
    assert_eq!(resolve_head(&fx.repository_path), fx.base_commit);
    assert_eq!(
        fs::read_to_string(fx.repository_path.join(TASK_LIB_PATH))
            .expect("read primary file after commit"),
        ORIGINAL_LIB
    );
    assert_eq!(
        fs::read_to_string(fx.repository_path.join("docs/README.md"))
            .expect("read primary README after commit"),
        ORIGINAL_README
    );
    assert!(
        receipt_file_names(&fx.work_area_root).is_empty(),
        "commit-task must not automatically approve or integrate work"
    );

    let subject: ReviewSubject = inspect_review_ok(&fx, "inspect-e2e.json");
    assert_sha256(&subject.subject_digest, "subject_digest");
    assert_eq!(
        subject.subject_digest,
        subject.compute_digest().expect("compute subject digest")
    );
    subject
        .verify_integrity()
        .expect("review subject integrity");
    assert_eq!(subject.run_id, RUN_ID);
    assert_eq!(subject.task_id, TASK_ID);
    assert_eq!(subject.task_worktree_id, fx.task.worktree_id);
    assert_eq!(subject.repository_path, fx.repository_path);
    assert_eq!(subject.work_area_root, fx.work_area_root);
    assert_eq!(subject.task_checkout_path, fx.task.checkout_path);
    assert_eq!(subject.base_commit, fx.base_commit);
    assert_eq!(subject.task_head_commit, commit_report.task_head_commit);
    assert_eq!(subject.task_tree_sha, commit_report.task_tree_sha);
    assert_eq!(subject.allowed_write_paths, fx.task.allowed_write_paths);
    assert_eq!(subject.policy, strict_policy());
    assert_eq!(subject.test_evidence, operator_attested_test_evidence());
    assert_eq!(subject.changed_paths.len(), 1);
    let changed = &subject.changed_paths[0];
    assert_eq!(changed.path, TASK_LIB_PATH);
    assert_eq!(changed.change_kind, DiffChangeKind::Modified);
    assert!(!changed.is_binary);
    assert!(!changed.is_symlink);
    assert!(
        receipt_file_names(&fx.work_area_root).is_empty(),
        "inspect-review must not automatically approve work"
    );
    assert_eq!(resolve_head(&fx.integration.checkout_path), fx.base_commit);
    assert_eq!(resolve_head(&fx.repository_path), fx.base_commit);
    assert_eq!(
        fs::read_to_string(fx.repository_path.join(TASK_LIB_PATH))
            .expect("read primary file after inspect"),
        ORIGINAL_LIB
    );

    let review_receipt: ReviewReceipt =
        accept_review_ok(&fx, &subject, "rev-cli-e2e", "accept-e2e.json");
    assert_eq!(review_receipt.receipt_id, "rev-cli-e2e");
    assert_eq!(review_receipt.subject_digest, subject.subject_digest);
    assert_eq!(review_receipt.base_commit, fx.base_commit);
    assert_eq!(review_receipt.task_head_commit, subject.task_head_commit);
    assert_eq!(review_receipt.task_tree_sha, subject.task_tree_sha);
    assert_eq!(review_receipt.policy, strict_policy());
    assert_eq!(
        review_receipt.test_evidence,
        operator_attested_test_evidence()
    );
    assert_eq!(review_receipt.reviewer_id, "operator-attestation");
    review_receipt
        .verify_integrity()
        .expect("review receipt integrity");
    review_receipt
        .verify_against_subject(&subject)
        .expect("review receipt must bind the reviewed subject");
    assert_eq!(
        load_review_receipt(&fx.work_area_root, "rev-cli-e2e")
            .expect("load persisted review receipt"),
        review_receipt
    );
    assert_eq!(
        receipt_file_names(&fx.work_area_root),
        vec!["review-rev-cli-e2e.json".to_string()]
    );
    assert_eq!(
        resolve_head(&fx.repository_path),
        fx.base_commit,
        "explicit review acceptance must not merge the primary checkout"
    );

    let proposal_request = integration_proposal_request(
        &fx,
        &subject,
        &review_receipt,
        IntegrationStrategy::MergeNoFf,
    );
    let proposal_path = fx.write_request("propose-e2e.json", &proposal_request);
    let proposal: IntegrationProposal = parse_success(
        &propose_integration_cli(&proposal_path),
        "propose-integration",
    );
    assert!(proposal.read_only);
    assert!(!proposal.already_integrated);
    assert!(proposal.can_fast_forward);
    assert!(proposal.clean_merge_predicted);
    assert_eq!(proposal.base_commit, fx.base_commit);
    assert_eq!(proposal.source_head_commit, fx.base_commit);
    assert_eq!(proposal.integration_head_commit, fx.base_commit);
    assert_eq!(proposal.task_head_commit, subject.task_head_commit);
    assert_eq!(proposal.task_tree_sha, subject.task_tree_sha);
    assert_eq!(proposal.review_subject_digest, subject.subject_digest);
    assert_eq!(proposal.review_receipt_id, review_receipt.receipt_id);
    assert_eq!(
        proposal.review_receipt_digest,
        review_receipt.receipt_digest
    );
    assert_eq!(proposal.strategy, IntegrationStrategy::MergeNoFf);
    assert!(
        proposal
            .planned_git_argv
            .iter()
            .any(|arg| arg == &subject.task_head_commit),
        "proposal must plan to integrate the reviewed task commit"
    );
    assert_eq!(
        proposal.proposal_digest,
        proposal.compute_digest().expect("compute proposal digest")
    );
    assert_eq!(
        resolve_head(&fx.integration.checkout_path),
        fx.base_commit,
        "propose-integration must be read-only"
    );
    assert_eq!(
        fs::read_to_string(fx.integration.checkout_path.join(TASK_LIB_PATH))
            .expect("read integration file after proposal"),
        ORIGINAL_LIB
    );
    assert_eq!(
        receipt_file_names(&fx.work_area_root),
        vec!["review-rev-cli-e2e.json".to_string()],
        "propose-integration must not create an integration receipt"
    );

    let integration_request = execute_integration_request(
        &fx,
        &subject,
        &review_receipt,
        &proposal.integration_head_commit,
        IntegrationStrategy::MergeNoFf,
        "int-cli-e2e",
    );
    assert_eq!(
        integration_request.expected_integration_head_commit,
        proposal.integration_head_commit
    );
    assert_eq!(
        integration_request.expected_task_head_commit,
        subject.task_head_commit
    );
    assert_eq!(
        integration_request.expected_source_head_commit,
        fx.base_commit
    );
    let integration_path = fx.write_request("integrate-e2e.json", &integration_request);
    let integration_receipt: IntegrationReceipt = parse_success(
        &integrate_cli(
            &integration_path,
            &subject.subject_digest,
            &proposal.integration_head_commit,
        ),
        "integrate",
    );
    assert_sha40(
        &integration_receipt.integrated_target_commit,
        "integrated_target_commit",
    );
    assert_sha40(
        &integration_receipt.integrated_tree_sha,
        "integrated_tree_sha",
    );
    assert_eq!(integration_receipt.receipt_id, "int-cli-e2e");
    assert_eq!(
        integration_receipt.previous_integration_head_commit,
        integration_request.expected_integration_head_commit
    );
    assert_eq!(
        integration_receipt.accepted_task_head_commit,
        integration_request.expected_task_head_commit
    );
    assert_eq!(
        integration_receipt.source_head_commit,
        integration_request.expected_source_head_commit
    );
    assert_eq!(
        integration_receipt.review_subject_digest,
        integration_request.subject.subject_digest
    );
    assert_eq!(
        integration_receipt.review_receipt_id,
        integration_request.review_receipt.receipt_id
    );
    assert_eq!(
        integration_receipt.review_receipt_digest,
        integration_request.review_receipt.receipt_digest
    );
    assert_ne!(integration_receipt.integrated_target_commit, fx.base_commit);
    assert_ne!(
        integration_receipt.integrated_target_commit, subject.task_head_commit,
        "merge_no_ff must produce a distinct integration merge commit"
    );
    assert_eq!(
        integration_receipt.integrated_tree_sha, subject.task_tree_sha,
        "integrated tree must be exactly the reviewed task tree"
    );
    assert_eq!(integration_receipt.source_head_commit, fx.base_commit);
    assert_eq!(
        integration_receipt.review_subject_digest,
        subject.subject_digest
    );
    assert_eq!(
        integration_receipt.review_receipt_id,
        review_receipt.receipt_id
    );
    assert_eq!(
        integration_receipt.review_receipt_digest,
        review_receipt.receipt_digest
    );
    assert_eq!(integration_receipt.strategy, IntegrationStrategy::MergeNoFf);
    assert_eq!(integration_receipt.operator_id, "operator-attestation");
    assert_eq!(integration_receipt.integrated_at_ms, 1_700_100_010_000);
    integration_receipt
        .verify_integrity()
        .expect("integration receipt integrity");
    integration_receipt
        .verify_against_review(&subject, &review_receipt)
        .expect("integration receipt must bind the accepted review");
    assert_eq!(
        load_integration_receipt(&fx.work_area_root, "int-cli-e2e")
            .expect("load persisted integration receipt"),
        integration_receipt
    );

    assert_eq!(
        resolve_head(&fx.integration.checkout_path),
        integration_receipt.integrated_target_commit
    );
    assert_eq!(
        resolve_tree(&fx.integration.checkout_path),
        subject.task_tree_sha
    );
    assert!(is_clean(&fx.integration.checkout_path));
    assert_eq!(
        fs::read_to_string(fx.integration.checkout_path.join(TASK_LIB_PATH))
            .expect("read integrated file"),
        UPDATED_LIB
    );
    assert!(
        is_commit_ancestor(
            &fx.integration.checkout_path,
            &fx.base_commit,
            &integration_receipt.integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("check base ancestry"),
        "integrated target must descend from the base commit"
    );
    assert!(
        is_commit_ancestor(
            &fx.integration.checkout_path,
            &subject.task_head_commit,
            &integration_receipt.integrated_target_commit,
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("check accepted task ancestry"),
        "integrated target must contain the accepted task head"
    );

    assert_eq!(
        resolve_head(&fx.repository_path),
        fx.base_commit,
        "primary source HEAD must never be merged automatically"
    );
    assert!(is_clean(&fx.repository_path));
    assert_eq!(
        fs::read_to_string(fx.repository_path.join(TASK_LIB_PATH))
            .expect("read primary file after integration"),
        ORIGINAL_LIB
    );
    assert_eq!(
        fs::read_to_string(fx.repository_path.join("docs/README.md"))
            .expect("read primary README after integration"),
        ORIGINAL_README
    );
    assert_eq!(
        resolve_head(&fx.task.checkout_path),
        subject.task_head_commit
    );
    assert!(is_clean(&fx.task.checkout_path));
    assert_eq!(
        fs::read_to_string(fx.task.checkout_path.join(TASK_LIB_PATH))
            .expect("read task file after integration"),
        UPDATED_LIB
    );

    let mut expected_receipts = vec![
        "integration-int-cli-e2e.json".to_string(),
        "review-rev-cli-e2e.json".to_string(),
    ];
    expected_receipts.sort();
    assert_eq!(receipt_file_names(&fx.work_area_root), expected_receipts);
}

#[test]
fn wrong_commit_review_and_integration_confirmations_are_rejected_without_effects() {
    let fx = Fixture::new();
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let dirty_before = snapshot_workspace(&fx);
    assert!(!dirty_before.task_clean);

    let commit_request_value =
        commit_request(&fx, "Must not be committed under a wrong confirmation");
    let commit_path = fx.write_request("wrong-commit.json", &commit_request_value);
    let wrong_commit = commit_task_cli(&commit_path, &format!("{}-wrong", fx.task.worktree_id));
    expect_cli_failure(
        &wrong_commit,
        "subject_mismatch",
        "wrong commit-task confirmation",
    );
    assert_workspace_unchanged(&fx, &dirty_before, "wrong commit-task confirmation");

    let commit_report: TaskCommitReport =
        commit_task_ok(&fx, "Update compute() to 77", "commit-ok.json");
    assert_eq!(
        resolve_head(&fx.task.checkout_path),
        commit_report.task_head_commit
    );

    let subject: ReviewSubject = inspect_review_ok(&fx, "inspect-ok.json");
    let before_wrong_accept = snapshot_workspace(&fx);
    let accept_request_value = review_acceptance_request(&subject, "rev-cli-guard");
    let accept_path = fx.write_request("wrong-accept.json", &accept_request_value);
    let wrong_accept = accept_review_cli(&accept_path, &different_digest(&subject.subject_digest));
    expect_cli_failure(
        &wrong_accept,
        "subject_mismatch",
        "wrong accept-review confirmation",
    );
    assert_workspace_unchanged(
        &fx,
        &before_wrong_accept,
        "wrong accept-review confirmation",
    );
    assert!(
        !fx.work_area_root
            .join("manifests/receipts/review-rev-cli-guard.json")
            .exists(),
        "wrong accept-review confirmation must not persist a receipt"
    );

    let review_receipt: ReviewReceipt =
        accept_review_ok(&fx, &subject, "rev-cli-guard", "accept-ok.json");
    let before_wrong_integrate = snapshot_workspace(&fx);
    let integration_request_value = execute_integration_request(
        &fx,
        &subject,
        &review_receipt,
        &fx.base_commit,
        IntegrationStrategy::MergeNoFf,
        "int-cli-guard",
    );
    let integration_path = fx.write_request("integrate-guard.json", &integration_request_value);

    let wrong_subject = integrate_cli(
        &integration_path,
        &different_digest(&subject.subject_digest),
        &fx.base_commit,
    );
    expect_cli_failure(
        &wrong_subject,
        "subject_mismatch",
        "wrong integrate subject confirmation",
    );
    assert_workspace_unchanged(
        &fx,
        &before_wrong_integrate,
        "wrong integrate subject confirmation",
    );

    let wrong_target = integrate_cli(
        &integration_path,
        &subject.subject_digest,
        &different_commit(&fx.base_commit),
    );
    expect_cli_failure(
        &wrong_target,
        "subject_mismatch",
        "wrong integrate target confirmation",
    );
    assert_workspace_unchanged(
        &fx,
        &before_wrong_integrate,
        "wrong integrate target confirmation",
    );

    assert_eq!(resolve_head(&fx.integration.checkout_path), fx.base_commit);
    assert_eq!(
        fs::read_to_string(fx.integration.checkout_path.join(TASK_LIB_PATH))
            .expect("read integration file after wrong confirmations"),
        ORIGINAL_LIB
    );
    assert!(
        !fx.work_area_root
            .join("manifests/receipts/integration-int-cli-guard.json")
            .exists(),
        "wrong integration confirmations must not persist an integration receipt"
    );
    assert_eq!(
        receipt_file_names(&fx.work_area_root),
        vec!["review-rev-cli-guard.json".to_string()]
    );
}

#[test]
fn loader_rejects_symlink_leaf_symlink_ancestor_and_relative_request_paths() {
    let fx = Fixture::new();
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let before = snapshot_workspace(&fx);
    let requests_dir = fx.requests_dir();

    let valid_request = commit_request(
        &fx,
        &format!("Loader guard must prevent this commit {SECRET_SENTINEL}"),
    );
    let valid_path = fx.write_request("loader-valid.json", &valid_request);

    let leaf_link = requests_dir.join("loader-leaf-link.json");
    symlink(&valid_path, &leaf_link).expect("create symlink request leaf");
    assert!(fs::symlink_metadata(&leaf_link)
        .expect("read symlink leaf metadata")
        .file_type()
        .is_symlink());
    let leaf_output = commit_task_cli(&leaf_link, &fx.task.worktree_id);
    expect_cli_failure(&leaf_output, "invalid_input", "symlink request leaf");
    assert_secret_not_echoed(&leaf_output, "symlink request leaf");
    assert_workspace_unchanged(&fx, &before, "symlink request leaf");

    let real_ancestor = requests_dir.join("real-ancestor");
    fs::create_dir_all(&real_ancestor).expect("create real ancestor directory");
    fs::set_permissions(&real_ancestor, fs::Permissions::from_mode(0o700))
        .expect("chmod real ancestor directory");
    write_private_request(&real_ancestor, "request.json", &valid_request);
    let ancestor_link = requests_dir.join("linked-ancestor");
    symlink(&real_ancestor, &ancestor_link).expect("create symlink request ancestor");
    assert!(fs::symlink_metadata(&ancestor_link)
        .expect("read symlink ancestor metadata")
        .file_type()
        .is_symlink());
    let linked_request_path = ancestor_link.join("request.json");
    assert!(linked_request_path.is_absolute());
    let ancestor_output = commit_task_cli(&linked_request_path, &fx.task.worktree_id);
    expect_cli_failure(
        &ancestor_output,
        "invalid_input",
        "symlink request ancestor",
    );
    assert_secret_not_echoed(&ancestor_output, "symlink request ancestor");
    assert_workspace_unchanged(&fx, &before, "symlink request ancestor");

    let relative_name = "relative-loader-request.json";
    let relative_path = fx.root.path().join(relative_name);
    write_private_bytes(
        &relative_path,
        &serde_json::to_vec(&valid_request).expect("serialize relative request"),
    );
    let task_id = fx.task.worktree_id.as_str();
    let relative_output = run_operator_in(
        fx.root.path(),
        &[
            "commit-task",
            "--request",
            relative_name,
            "--confirm-task",
            task_id,
        ],
    );
    expect_cli_failure(&relative_output, "invalid_input", "relative request path");
    assert_secret_not_echoed(&relative_output, "relative request path");
    assert_workspace_unchanged(&fx, &before, "relative request path");
}

#[test]
fn loader_rejects_insecure_oversize_and_malformed_request_files() {
    let fx = Fixture::new();
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let before = snapshot_workspace(&fx);
    let requests_dir = fx.requests_dir();

    let valid_request = commit_request(
        &fx,
        &format!("Loader guard must prevent this commit {SECRET_SENTINEL}"),
    );
    let valid_bytes = serde_json::to_vec(&valid_request).expect("serialize valid request");

    let insecure_path = requests_dir.join("loader-insecure.json");
    write_private_bytes(&insecure_path, &valid_bytes);
    fs::set_permissions(&insecure_path, fs::Permissions::from_mode(0o644))
        .expect("chmod insecure request file");
    let insecure_output = commit_task_cli(&insecure_path, &fx.task.worktree_id);
    expect_cli_failure(&insecure_output, "invalid_input", "insecure request mode");
    assert_secret_not_echoed(&insecure_output, "insecure request mode");
    assert_workspace_unchanged(&fx, &before, "insecure request mode");

    let oversize_path = requests_dir.join("loader-oversize.json");
    let mut oversize_bytes = valid_bytes.clone();
    oversize_bytes.resize(MAX_REQUEST_FILE_BYTES as usize + 1, b' ');
    write_private_bytes(&oversize_path, &oversize_bytes);
    assert!(
        fs::metadata(&oversize_path)
            .expect("read oversize request metadata")
            .len()
            > MAX_REQUEST_FILE_BYTES
    );
    let oversize_output = commit_task_cli(&oversize_path, &fx.task.worktree_id);
    expect_cli_failure(&oversize_output, "invalid_input", "oversize request file");
    assert_secret_not_echoed(&oversize_output, "oversize request file");
    assert_workspace_unchanged(&fx, &before, "oversize request file");

    let malformed_path = requests_dir.join("loader-malformed.json");
    let malformed = format!("this is not a request JSON document {SECRET_SENTINEL}");
    write_private_bytes(&malformed_path, malformed.as_bytes());
    let malformed_output = commit_task_cli(&malformed_path, &fx.task.worktree_id);
    expect_cli_failure(&malformed_output, "invalid_input", "malformed request file");
    assert_secret_not_echoed(&malformed_output, "malformed request file");
    assert_workspace_unchanged(&fx, &before, "malformed request file");

    let wrong_shape_path = requests_dir.join("loader-wrong-shape.json");
    let wrong_shape = format!("{{\"unexpected\":\"{SECRET_SENTINEL}\"}}");
    write_private_bytes(&wrong_shape_path, wrong_shape.as_bytes());
    let wrong_shape_output = commit_task_cli(&wrong_shape_path, &fx.task.worktree_id);
    expect_cli_failure(&wrong_shape_output, "invalid_input", "wrong request shape");
    assert_secret_not_echoed(&wrong_shape_output, "wrong request shape");
    assert_workspace_unchanged(&fx, &before, "wrong request shape");
}

#[test]
fn changed_review_subject_changed_task_and_stale_integration_head_cannot_integrate() {
    let fx = Fixture::new();
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let commit_report: TaskCommitReport =
        commit_task_ok(&fx, "Update compute() to 77", "commit-stale.json");
    let subject: ReviewSubject = inspect_review_ok(&fx, "inspect-stale.json");
    let review_receipt: ReviewReceipt =
        accept_review_ok(&fx, &subject, "rev-cli-stale", "accept-stale.json");
    assert_eq!(commit_report.task_head_commit, subject.task_head_commit);

    // Advance the integration branch after review acceptance. A request that expects the old
    // integration head must be refused without moving the advanced target.
    let advanced_integration_head = commit_worktree_file(
        &fx.integration.checkout_path,
        "docs/integration-note.txt",
        "integration branch advanced before the operator request\n",
        "Advance integration branch during fixture setup",
    );
    assert_ne!(advanced_integration_head, fx.base_commit);
    let after_advance = snapshot_workspace(&fx);
    assert_eq!(after_advance.integration_head, advanced_integration_head);
    assert_eq!(
        after_advance.receipt_files,
        vec!["review-rev-cli-stale.json".to_string()]
    );

    let stale_request = execute_integration_request(
        &fx,
        &subject,
        &review_receipt,
        &fx.base_commit,
        IntegrationStrategy::MergeNoFf,
        "int-cli-stale",
    );
    let stale_path = fx.write_request("integrate-stale.json", &stale_request);
    let stale_output = integrate_cli(&stale_path, &subject.subject_digest, &fx.base_commit);
    expect_cli_failure(&stale_output, "head_moved", "stale integration HEAD");
    assert_workspace_unchanged(&fx, &after_advance, "stale integration HEAD");
    assert!(!fx
        .work_area_root
        .join("manifests/receipts/integration-int-cli-stale.json")
        .exists());

    // A changed review subject must fail receipt verification even though its own digest is
    // internally consistent.
    let mut changed_subject = subject.clone();
    changed_subject.test_evidence.passed_test_count += 1;
    changed_subject.subject_digest = changed_subject
        .compute_digest()
        .expect("compute changed subject digest");
    changed_subject
        .verify_integrity()
        .expect("changed subject remains structurally valid");
    let changed_subject_request = execute_integration_request(
        &fx,
        &changed_subject,
        &review_receipt,
        &advanced_integration_head,
        IntegrationStrategy::MergeNoFf,
        "int-cli-changed-review",
    );
    let changed_subject_path =
        fx.write_request("integrate-changed-review.json", &changed_subject_request);
    let changed_subject_output = integrate_cli(
        &changed_subject_path,
        &changed_subject.subject_digest,
        &advanced_integration_head,
    );
    expect_cli_failure(
        &changed_subject_output,
        "receipt_invalidated",
        "changed review subject",
    );
    assert_workspace_unchanged(&fx, &after_advance, "changed review subject");
    assert!(!fx
        .work_area_root
        .join("manifests/receipts/integration-int-cli-changed-review.json")
        .exists());

    // Advancing the live task worktree after review acceptance invalidates the old receipt.
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), ADVANCED_LIB).expect("advance task edit");
    let advanced_task = commit_task_ok(
        &fx,
        "Advance task after review acceptance",
        "commit-stale-task.json",
    );
    assert_ne!(advanced_task.task_head_commit, subject.task_head_commit);
    let after_task_advance = snapshot_workspace(&fx);
    let changed_task_request = execute_integration_request(
        &fx,
        &subject,
        &review_receipt,
        &advanced_integration_head,
        IntegrationStrategy::MergeNoFf,
        "int-cli-changed-task",
    );
    let changed_task_path = fx.write_request("integrate-changed-task.json", &changed_task_request);
    let changed_task_output = integrate_cli(
        &changed_task_path,
        &subject.subject_digest,
        &advanced_integration_head,
    );
    expect_cli_failure(
        &changed_task_output,
        "receipt_invalidated",
        "changed task subject",
    );
    assert_workspace_unchanged(&fx, &after_task_advance, "changed task subject");
    assert!(!fx
        .work_area_root
        .join("manifests/receipts/integration-int-cli-changed-task.json")
        .exists());

    assert_eq!(
        resolve_head(&fx.task.checkout_path),
        advanced_task.task_head_commit
    );
    assert_eq!(
        resolve_head(&fx.integration.checkout_path),
        advanced_integration_head
    );
    assert_eq!(resolve_head(&fx.repository_path), fx.base_commit);
    assert_eq!(
        fs::read_to_string(fx.repository_path.join(TASK_LIB_PATH))
            .expect("read primary file after rejected integrations"),
        ORIGINAL_LIB
    );
}

#[test]
fn standard_help_and_version_usage_works() {
    let help = run_operator(&["--help"]);
    assert!(
        help.status.success(),
        "--help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let help_text = String::from_utf8_lossy(&help.stdout);
    for command in [
        "commit-task",
        "inspect-review",
        "accept-review",
        "propose-integration",
        "integrate",
    ] {
        assert!(
            help_text.contains(command),
            "top-level help must list `{command}`:\n{help_text}"
        );
    }

    let version = run_operator(&["--version"]);
    assert!(
        version.status.success(),
        "--version failed: {}",
        String::from_utf8_lossy(&version.stderr)
    );
    assert!(
        !version.stdout.is_empty(),
        "--version must print a version string"
    );
}

#[test]
fn broken_stdout_after_commit_reports_unknown_without_replaying_the_mutation() {
    use std::os::fd::OwnedFd;
    use std::os::unix::net::UnixStream;
    use std::process::Stdio;

    let fx = Fixture::new();
    fs::write(fx.task.checkout_path.join(TASK_LIB_PATH), UPDATED_LIB).expect("edit task");
    let request = commit_request(&fx, "Commit once despite a broken output consumer");
    let request_path = fx.write_request("broken-stdout.json", &request);
    let before_count = run_git(&fx.task.checkout_path, &["rev-list", "--count", "HEAD"])
        .trim()
        .parse::<u32>()
        .expect("commit count");

    // Close the peer before spawn so the first stdout write deterministically fails.
    let (writer, reader) = UnixStream::pair().expect("create output socket pair");
    drop(reader);
    let stdout_fd: OwnedFd = writer.into();
    let output = operator_command()
        .args([
            "commit-task",
            "--request",
            request_path.to_str().expect("request UTF-8"),
            "--confirm-task",
            &fx.task.worktree_id,
        ])
        .stdout(Stdio::from(stdout_fd))
        .output()
        .expect("run with broken stdout");
    expect_cli_failure(
        &output,
        "outcome_unknown",
        "commit result consumer disconnected",
    );
    assert_ne!(resolve_head(&fx.task.checkout_path), fx.base_commit);
    assert!(is_clean(&fx.task.checkout_path));
    assert_eq!(
        fs::read_to_string(fx.task.checkout_path.join(TASK_LIB_PATH)).expect("task contents"),
        UPDATED_LIB
    );
    let after_count = run_git(&fx.task.checkout_path, &["rev-list", "--count", "HEAD"])
        .trim()
        .parse::<u32>()
        .expect("commit count");
    assert_eq!(after_count, before_count + 1);
    assert_eq!(resolve_head(&fx.repository_path), fx.base_commit);
    assert_eq!(resolve_head(&fx.integration.checkout_path), fx.base_commit);
    assert!(receipt_file_names(&fx.work_area_root).is_empty());
}
