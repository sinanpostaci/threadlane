use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;
use threadlane_workers::{
    evaluate_diff_gate, evaluate_diff_gate_with_limits, inspect_base_commit_path_kind,
    is_path_allowed, is_path_allowed_by_scopes, is_path_allowed_syntactic,
    resolve_allowed_write_scopes_at_base, resolve_exact_commit_sha, run_sanitized_git,
    BaseCommitPathKind, DiffViolationKind, ExecutionBudgets, WorkAcceptanceState,
    WorkerFilePermissions, WorkerProvider, WorkerSpec, WorktreeManager, WriteScopeKind,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

struct RepoFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    work_area_root: PathBuf,
    base_commit: String,
    manager: WorktreeManager,
}

fn setup_repo_fixture() -> RepoFixture {
    let tmp = TempDir::new().expect("create tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
    let repo_dir = root.join("repo");
    let work_area_root = root.join("work-area");

    fs::create_dir_all(repo_dir.join("src/sub")).expect("create src/sub");
    fs::create_dir_all(repo_dir.join("unapproved")).expect("create unapproved");
    fs::write(
        repo_dir.join("foo"),
        "exact file foo line 1\nexact file foo line 2\nexact file foo line 3\n",
    )
    .expect("write foo");
    fs::write(
        repo_dir.join("bar"),
        "exact file bar line 1\nexact file bar line 2\n",
    )
    .expect("write bar");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn existing() -> u32 { 1 }\n",
    )
    .expect("write src/lib.rs");
    fs::write(repo_dir.join("src/sub/mod.rs"), "pub mod nested;\n").expect("write src/sub/mod.rs");
    fs::write(
        repo_dir.join("unapproved/secret.rs"),
        "secret line 1\nsecret line 2\nsecret line 3\n",
    )
    .expect("write unapproved/secret.rs");

    run_sanitized_git(
        &repo_dir,
        &["init", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git init");
    run_sanitized_git(
        &repo_dir,
        &["config", "user.email", "test@threadlane.local"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config email");
    run_sanitized_git(
        &repo_dir,
        &["config", "user.name", "Threadlane Test"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config name");
    run_sanitized_git(&repo_dir, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git add");
    run_sanitized_git(
        &repo_dir,
        &["commit", "-m", "baseline commit", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");

    let base_commit = resolve_exact_commit_sha(&repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve base_commit");
    let manager =
        WorktreeManager::new(&repo_dir, &work_area_root, &base_commit).expect("create manager");

    RepoFixture {
        _tmp: tmp,
        repo_dir,
        work_area_root,
        base_commit,
        manager,
    }
}

fn create_task_worktree_with_scopes(
    fixture: &RepoFixture,
    run_id: &str,
    task_id: &str,
    scopes: &[&str],
) -> (String, PathBuf) {
    let allowed: BTreeSet<String> = scopes.iter().map(|s| (*s).to_string()).collect();
    let manifest = fixture
        .manager
        .create_task_worktree(run_id, task_id, &allowed)
        .expect("create task worktree");
    (manifest.worktree_id, manifest.checkout_path)
}

#[test]
fn approved_existing_bare_file_replaced_with_directory_child_is_rejected() {
    let fixture = setup_repo_fixture();

    // 1. Untracked child after deleting existing file `foo`.
    let (wt_untracked, checkout_untracked) =
        create_task_worktree_with_scopes(&fixture, "run-existing-foo", "task-untracked", &["foo"]);
    fs::remove_file(checkout_untracked.join("foo")).expect("remove file foo");
    fs::create_dir_all(checkout_untracked.join("foo")).expect("create dir foo");
    fs::write(
        checkout_untracked.join("foo/child"),
        "unauthorized descendant under bare file grant\n",
    )
    .expect("write foo/child");

    let report_untracked =
        evaluate_diff_gate(&fixture.work_area_root, &wt_untracked).expect("evaluate diff gate");
    assert!(!report_untracked.passed);
    assert_eq!(
        report_untracked.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(
        report_untracked
            .violations
            .iter()
            .any(|v| { v.path == "foo/child" && v.kind == DiffViolationKind::OutsideAllowedPaths }),
        "expected OutsideAllowedPaths violation for foo/child, got {:?}",
        report_untracked.violations
    );

    // 2. Staged/committed replacement of `foo` with `foo/child`.
    let (wt_committed, checkout_committed) =
        create_task_worktree_with_scopes(&fixture, "run-existing-foo", "task-committed", &["foo"]);
    fs::remove_file(checkout_committed.join("foo")).expect("remove file foo");
    fs::create_dir_all(checkout_committed.join("foo")).expect("create dir foo");
    fs::write(
        checkout_committed.join("foo/child"),
        "committed unauthorized child\n",
    )
    .expect("write foo/child");
    run_sanitized_git(
        &checkout_committed,
        &["add", "-A"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add -A");
    run_sanitized_git(
        &checkout_committed,
        &["commit", "-m", "replace foo with foo/child", "--quiet"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit");

    let report_committed =
        evaluate_diff_gate(&fixture.work_area_root, &wt_committed).expect("evaluate diff gate");
    assert!(!report_committed.passed);
    assert_eq!(
        report_committed.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(
        report_committed
            .violations
            .iter()
            .any(|v| { v.path == "foo/child" && v.kind == DiffViolationKind::OutsideAllowedPaths }),
        "expected OutsideAllowedPaths violation for committed foo/child, got {:?}",
        report_committed.violations
    );

    // 3. Modifying `foo` in place as a regular file is still permitted.
    let (wt_valid, checkout_valid) =
        create_task_worktree_with_scopes(&fixture, "run-existing-foo", "task-valid", &["foo"]);
    fs::write(checkout_valid.join("foo"), "updated exact file foo\n").expect("update foo");
    let report_valid =
        evaluate_diff_gate(&fixture.work_area_root, &wt_valid).expect("evaluate diff gate");
    assert!(
        report_valid.passed,
        "modifying exact file foo must pass, violations: {:?}",
        report_valid.violations
    );
    assert_eq!(
        report_valid.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
}

#[test]
fn approved_absent_bare_file_creating_directory_child_is_rejected_while_exact_file_passes() {
    let fixture = setup_repo_fixture();

    // 1. Bare absent `new_target` -> creating `new_target/child` must be rejected.
    let (wt_bad, checkout_bad) =
        create_task_worktree_with_scopes(&fixture, "run-absent-foo", "task-child", &["new_target"]);
    fs::create_dir_all(checkout_bad.join("new_target")).expect("create dir new_target");
    fs::write(
        checkout_bad.join("new_target/child"),
        "unauthorized child under bare absent scope\n",
    )
    .expect("write new_target/child");

    let report_bad =
        evaluate_diff_gate(&fixture.work_area_root, &wt_bad).expect("evaluate diff gate");
    assert!(!report_bad.passed);
    assert_eq!(
        report_bad.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(
        report_bad.violations.iter().any(|v| {
            v.path == "new_target/child" && v.kind == DiffViolationKind::OutsideAllowedPaths
        }),
        "expected OutsideAllowedPaths for new_target/child, got {:?}",
        report_bad.violations
    );

    // 2. Bare absent `new_target` -> creating the exact file `new_target` passes.
    let (wt_ok, checkout_ok) =
        create_task_worktree_with_scopes(&fixture, "run-absent-foo", "task-exact", &["new_target"]);
    fs::write(checkout_ok.join("new_target"), "created exact file\n").expect("write new_target");

    let report_ok =
        evaluate_diff_gate(&fixture.work_area_root, &wt_ok).expect("evaluate diff gate");
    assert!(
        report_ok.passed,
        "creating exact file new_target must pass, violations: {:?}",
        report_ok.violations
    );
    assert_eq!(
        report_ok.acceptance_state,
        WorkAcceptanceState::DiffGatePassedPendingReview
    );
}

#[test]
fn explicit_trailing_slash_and_legacy_bare_directory_at_base_permit_descendants_while_rejecting_siblings_and_controls(
) {
    let fixture = setup_repo_fixture();

    // 1. Explicit `src/` and explicit absent `new_dir/` permit nested descendants.
    let (wt_explicit, checkout_explicit) = create_task_worktree_with_scopes(
        &fixture,
        "run-dirs",
        "task-explicit",
        &["src/", "new_dir/"],
    );
    fs::write(
        checkout_explicit.join("src/child.rs"),
        "pub fn child() {}\n",
    )
    .expect("write src/child.rs");
    fs::create_dir_all(checkout_explicit.join("new_dir/deep")).expect("create new_dir/deep");
    fs::write(
        checkout_explicit.join("new_dir/deep/item.rs"),
        "pub fn item() {}\n",
    )
    .expect("write new_dir/deep/item.rs");

    let report_explicit =
        evaluate_diff_gate(&fixture.work_area_root, &wt_explicit).expect("evaluate diff gate");
    assert!(
        report_explicit.passed,
        "explicit trailing-slash scopes must permit descendants: {:?}",
        report_explicit.violations
    );

    // 2. Legacy bare `src` (which is a `040000 tree` directory at `base_commit`) preserves
    // directory subtree semantics and startup WorkerSpec validation/identity.
    let canonical_sh = fs::canonicalize("/bin/sh").expect("canonicalize /bin/sh");
    let legacy_spec = WorkerSpec {
        provider: WorkerProvider::Antigravity,
        executable: canonical_sh,
        model: "gemini-4-argon-high".to_string(),
        effort: Some("high".to_string()),
        allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
        session_id: None,
        standalone: false,
        repository_path: fixture.repo_dir.clone(),
        base_commit: fixture.base_commit.clone(),
        work_area_root: fixture.work_area_root.clone(),
        allowed_write_paths: BTreeSet::from(["src".to_string()]),
        prompt: "Update files under src".to_string(),
        file_permissions: WorkerFilePermissions::Inherit,
        budgets: ExecutionBudgets::default(),
    };
    let validation_report = legacy_spec
        .validate()
        .expect("legacy bare directory spec must validate");
    assert_eq!(
        validation_report.resolved_model.cli_model_arg,
        "gemini-4-argon-high"
    );

    let (wt_legacy_dir, checkout_legacy_dir) =
        create_task_worktree_with_scopes(&fixture, "run-dirs", "task-legacy-bare-src", &["src"]);
    fs::write(
        checkout_legacy_dir.join("src/sub/new_child.rs"),
        "pub fn legacy_ok() {}\n",
    )
    .expect("write src/sub/new_child.rs");
    let report_legacy_dir =
        evaluate_diff_gate(&fixture.work_area_root, &wt_legacy_dir).expect("evaluate diff gate");
    assert!(
        report_legacy_dir.passed,
        "legacy bare directory at base_commit must still permit descendants: {:?}",
        report_legacy_dir.violations
    );

    // 3. Unrelated sibling prefix `src-evil/attack.rs` must be rejected!
    let (wt_sibling, checkout_sibling) =
        create_task_worktree_with_scopes(&fixture, "run-dirs", "task-sibling", &["src"]);
    fs::create_dir_all(checkout_sibling.join("src-evil")).expect("create src-evil");
    fs::write(
        checkout_sibling.join("src-evil/attack.rs"),
        "pub fn evil() {}\n",
    )
    .expect("write src-evil/attack.rs");
    let report_sibling =
        evaluate_diff_gate(&fixture.work_area_root, &wt_sibling).expect("evaluate diff gate");
    assert!(!report_sibling.passed);
    assert!(
        report_sibling.violations.iter().any(|v| {
            v.path == "src-evil/attack.rs" && v.kind == DiffViolationKind::OutsideAllowedPaths
        }),
        "sibling prefix src-evil/attack.rs must be rejected: {:?}",
        report_sibling.violations
    );

    // 4. Protected nested control configuration inside allowed `src/` (`src/AGENTS.md`, `src/.env.local`)
    // must still be rejected with `ControlConfigModified`!
    let (wt_control, checkout_control) =
        create_task_worktree_with_scopes(&fixture, "run-dirs", "task-control", &["src/"]);
    fs::write(checkout_control.join("src/AGENTS.md"), "# hijacked\n").expect("write src/AGENTS.md");
    fs::write(checkout_control.join("src/.env.local"), "SECRET=1\n").expect("write src/.env.local");
    let report_control =
        evaluate_diff_gate(&fixture.work_area_root, &wt_control).expect("evaluate diff gate");
    assert!(!report_control.passed);
    assert!(report_control.violations.iter().any(|v| {
        v.path == "src/AGENTS.md" && v.kind == DiffViolationKind::ControlConfigModified
    }));
    assert!(report_control.violations.iter().any(|v| {
        v.path == "src/.env.local" && v.kind == DiffViolationKind::ControlConfigModified
    }));
}

#[test]
fn rename_origin_and_destination_are_both_enforced_under_strict_scopes() {
    let fixture = setup_repo_fixture();

    // 1. Rename approved exact file `foo` into `foo/child` (staged so Git detects R100 rename) -> rejected!
    let (wt_rename_into_child, checkout_rename_into_child) =
        create_task_worktree_with_scopes(&fixture, "run-renames", "task-rename-child", &["foo"]);
    let foo_contents =
        fs::read_to_string(checkout_rename_into_child.join("foo")).expect("read foo");
    fs::remove_file(checkout_rename_into_child.join("foo")).expect("remove foo");
    fs::create_dir_all(checkout_rename_into_child.join("foo")).expect("create dir foo");
    fs::write(checkout_rename_into_child.join("foo/child"), foo_contents)
        .expect("write identical content to foo/child");
    run_sanitized_git(
        &checkout_rename_into_child,
        &["add", "-A"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add -A");

    let report_rename_child = evaluate_diff_gate(&fixture.work_area_root, &wt_rename_into_child)
        .expect("evaluate diff gate");
    assert!(!report_rename_child.passed);
    assert!(
        report_rename_child
            .violations
            .iter()
            .any(|v| { v.path == "foo/child" && v.kind == DiffViolationKind::OutsideAllowedPaths }),
        "renaming foo to foo/child under exact file grant must fail: {:?}",
        report_rename_child.violations
    );

    // 2. Rename unapproved `unapproved/secret.rs` to approved exact file `foo` -> rejected on `orig_path`!
    let (wt_rename_orig, checkout_rename_orig) =
        create_task_worktree_with_scopes(&fixture, "run-renames", "task-rename-orig", &["foo"]);
    let secret_contents =
        fs::read_to_string(checkout_rename_orig.join("unapproved/secret.rs")).expect("read secret");
    fs::remove_file(checkout_rename_orig.join("unapproved/secret.rs")).expect("remove secret");
    fs::write(checkout_rename_orig.join("foo"), secret_contents).expect("overwrite foo");
    run_sanitized_git(
        &checkout_rename_orig,
        &["add", "-A"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add -A");

    let report_rename_orig =
        evaluate_diff_gate(&fixture.work_area_root, &wt_rename_orig).expect("evaluate diff gate");
    assert!(!report_rename_orig.passed);
    assert!(
        report_rename_orig.violations.iter().any(|v| {
            v.path == "unapproved/secret.rs" && v.kind == DiffViolationKind::OutsideAllowedPaths
        }),
        "renaming unapproved/secret.rs to foo must fail on origin path: {:?}",
        report_rename_orig.violations
    );

    // 3. Rename approved exact file `foo` to another approved exact file `bar` -> passes!
    let (wt_rename_both_ok, checkout_rename_both_ok) = create_task_worktree_with_scopes(
        &fixture,
        "run-renames",
        "task-rename-both-ok",
        &["foo", "bar"],
    );
    let foo_text = fs::read_to_string(checkout_rename_both_ok.join("foo")).expect("read foo");
    fs::remove_file(checkout_rename_both_ok.join("foo")).expect("remove foo");
    fs::write(checkout_rename_both_ok.join("bar"), foo_text).expect("write bar");
    run_sanitized_git(
        &checkout_rename_both_ok,
        &["add", "-A"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add -A");

    let report_rename_ok = evaluate_diff_gate(&fixture.work_area_root, &wt_rename_both_ok)
        .expect("evaluate diff gate");
    assert!(
        report_rename_ok.passed,
        "renaming between two approved exact files must pass: {:?}",
        report_rename_ok.violations
    );
}

#[test]
fn git_evidence_budget_exhaustion_fails_closed_without_prefix_fallback() {
    let fixture = setup_repo_fixture();
    let (wt_id, checkout) =
        create_task_worktree_with_scopes(&fixture, "run-budget", "task-budget", &["foo"]);
    fs::write(checkout.join("foo"), "modified foo\n").expect("modify foo");

    // Tiny `max_git_output_bytes` (8 bytes) causes baseline `git ls-tree` to overflow.
    // The diff gate must fail closed (`evidence_complete == false`, `passed == false`,
    // `RejectedByDiffGate`) and must never accept the worktree.
    let report = evaluate_diff_gate_with_limits(&fixture.work_area_root, &wt_id, 64, 8)
        .expect("evaluate_diff_gate_with_limits");
    assert!(!report.evidence_complete);
    assert!(!report.passed);
    assert_eq!(
        report.acceptance_state,
        WorkAcceptanceState::RejectedByDiffGate
    );
    assert!(
        report
            .violations
            .iter()
            .any(|v| v.kind == DiffViolationKind::DiffBudgetExceeded),
        "expected DiffBudgetExceeded violation, got {:?}",
        report.violations
    );
}

#[test]
fn scope_resolution_and_matching_helpers_distinguish_exact_files_from_directories() {
    let fixture = setup_repo_fixture();

    assert_eq!(
        inspect_base_commit_path_kind(
            &fixture.repo_dir,
            &fixture.base_commit,
            "foo",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("inspect foo"),
        BaseCommitPathKind::RegularFile
    );
    assert_eq!(
        inspect_base_commit_path_kind(
            &fixture.repo_dir,
            &fixture.base_commit,
            "src",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("inspect src"),
        BaseCommitPathKind::Directory
    );
    assert_eq!(
        inspect_base_commit_path_kind(
            &fixture.repo_dir,
            &fixture.base_commit,
            "nonexistent/path.rs",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("inspect nonexistent"),
        BaseCommitPathKind::Missing
    );

    let scopes = resolve_allowed_write_scopes_at_base(
        &fixture.repo_dir,
        &fixture.base_commit,
        &BTreeSet::from([
            "foo".to_string(),
            "absent_file.rs".to_string(),
            "src".to_string(),
            "new_dir/".to_string(),
        ]),
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve scopes at base_commit");

    assert_eq!(
        scopes,
        BTreeMap::from([
            ("absent_file.rs".to_string(), WriteScopeKind::ExactFile),
            ("foo".to_string(), WriteScopeKind::ExactFile),
            ("new_dir".to_string(), WriteScopeKind::DirectorySubtree),
            ("src".to_string(), WriteScopeKind::DirectorySubtree),
        ])
    );

    assert!(is_path_allowed_by_scopes("foo", &scopes));
    assert!(!is_path_allowed_by_scopes("foo/child", &scopes));
    assert!(is_path_allowed_by_scopes("absent_file.rs", &scopes));
    assert!(!is_path_allowed_by_scopes("absent_file.rs/child", &scopes));
    assert!(is_path_allowed_by_scopes("src/lib.rs", &scopes));
    assert!(is_path_allowed_by_scopes("src/sub/mod.rs", &scopes));
    assert!(!is_path_allowed_by_scopes("src-evil/lib.rs", &scopes));
    assert!(!is_path_allowed_by_scopes("src/AGENTS.md", &scopes));
    assert!(is_path_allowed_by_scopes("new_dir/child.rs", &scopes));

    // Pure syntactic helper treats bare entries as ExactFile and trailing-`/` as DirectorySubtree.
    let raw_set = BTreeSet::from(["foo".to_string(), "src/".to_string()]);
    assert!(is_path_allowed_syntactic("foo", &raw_set));
    assert!(!is_path_allowed_syntactic("foo/child", &raw_set));
    assert!(is_path_allowed_syntactic("src/lib.rs", &raw_set));
    assert!(!is_path_allowed_syntactic("src-other/lib.rs", &raw_set));

    // Legacy helper remains intact for callers without baseline commit context.
    let legacy_set = BTreeSet::from(["src".to_string()]);
    assert!(is_path_allowed("src/lib.rs", &legacy_set));
}
