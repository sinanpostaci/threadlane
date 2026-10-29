use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;
use threadlane_workers::{
    begin_worktree_job, cleanup_integrated_worktree, cleanup_worktree, evaluate_diff_gate,
    is_commit_ancestor, is_worktree_clean, load_and_verify_manifest, resolve_exact_commit_sha,
    run_sanitized_git, sanitize_git_command_env_from, CleanupRefusalReason, CleanupRequest,
    IntegratedCleanupRequest, WorkerError, WorktreeManager, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

fn create_private_tempdir() -> TempDir {
    let dir = tempfile::tempdir().expect("create tempdir");
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).expect("chmod 0700 tempdir");
    dir
}

fn init_disposable_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir).expect("create repo dir");
    fs::set_permissions(repo_dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 repo");

    run_sanitized_git(repo_dir, &["init", "-q"], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init");
    fs::create_dir_all(repo_dir.join("src")).expect("create src dir");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn initial() -> u32 { 1 }\n",
    )
    .expect("write initial file");
    run_sanitized_git(
        repo_dir,
        &["add", "--", "src/lib.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add");
    run_sanitized_git(
        repo_dir,
        &[
            "-c",
            "user.name=Threadlane Test",
            "-c",
            "user.email=test@threadlane.local",
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

fn commit_file_in_worktree(checkout: &Path, rel_path: &str, content: &str, msg: &str) -> String {
    let full = checkout.join(rel_path);
    if let Some(parent) = full.parent() {
        fs::create_dir_all(parent).expect("create parent dir");
    }
    fs::write(&full, content).expect("write file");
    run_sanitized_git(
        checkout,
        &["add", "--", rel_path],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add in worktree");
    run_sanitized_git(
        checkout,
        &[
            "-c",
            "user.name=Threadlane Worker",
            "-c",
            "user.email=worker@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            msg,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit in worktree");
    resolve_exact_commit_sha(checkout, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve new HEAD")
}

#[test]
fn sanitize_git_command_env_strips_all_poisoned_git_variables_and_preserves_victim_repo() {
    let root = create_private_tempdir();
    let victim_repo = root.path().join("victim_repo");
    let target_repo = root.path().join("target_repo");
    let work_area = root.path().join("work_area");
    let poison_hooks = root.path().join("poison_hooks");
    let hook_sentinel = root.path().join("hook_executed_sentinel");

    let victim_base = init_disposable_repo(&victim_repo);
    let target_base = init_disposable_repo(&target_repo);

    // Stage a sensitive modification in victim_repo that must never be committed or altered.
    fs::write(
        victim_repo.join("src/lib.rs"),
        "pub fn victim_staged() {}\n",
    )
    .expect("write victim staged file");
    run_sanitized_git(
        &victim_repo,
        &["add", "--", "src/lib.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("stage victim file");
    let victim_index_before =
        fs::read(victim_repo.join(".git/index")).expect("read victim index before");
    let victim_config_before =
        fs::read(victim_repo.join(".git/config")).expect("read victim config before");

    // Create a poison Git hook directory that writes a sentinel if any Git command executes hooks.
    fs::create_dir_all(&poison_hooks).expect("create poison_hooks");
    for hook_name in ["post-checkout", "pre-commit", "post-commit", "post-merge"] {
        let hook_path = poison_hooks.join(hook_name);
        fs::write(
            &hook_path,
            format!(
                "#!/bin/sh\necho pwned > {}\nexit 1\n",
                hook_sentinel.display()
            ),
        )
        .expect("write poison hook");
        fs::set_permissions(&hook_path, fs::Permissions::from_mode(0o755))
            .expect("chmod 0755 hook");
    }

    // Verify `sanitize_git_command_env_from` removes all case variants of `GIT_*` and installs safe defaults.
    let mut probe_cmd = Command::new("env");
    let poisoned_vars = vec![
        ("GIT_DIR", victim_repo.join(".git").display().to_string()),
        ("GIT_WORK_TREE", victim_repo.display().to_string()),
        (
            "GIT_INDEX_FILE",
            victim_repo.join(".git/index").display().to_string(),
        ),
        (
            "git_common_dir",
            victim_repo.join(".git").display().to_string(),
        ),
        ("GIT_CONFIG_COUNT", "1".to_string()),
        ("GIT_CONFIG_KEY_0", "core.hooksPath".to_string()),
        ("GIT_CONFIG_VALUE_0", poison_hooks.display().to_string()),
        ("NON_GIT_SENTINEL", "preserved_value_123".to_string()),
    ];
    for (k, v) in &poisoned_vars {
        probe_cmd.env(k, v);
    }
    sanitize_git_command_env_from(&mut probe_cmd, poisoned_vars);
    let env_out = probe_cmd.output().expect("run env command");
    let env_text = String::from_utf8_lossy(&env_out.stdout);
    assert!(env_text.contains("NON_GIT_SENTINEL=preserved_value_123"));
    assert!(env_text.contains("GIT_TERMINAL_PROMPT=0"));
    assert!(env_text.contains("GIT_CONFIG_NOSYSTEM=1"));
    assert!(env_text.contains("GIT_CONFIG_SYSTEM=/dev/null"));
    assert!(env_text.contains("GIT_CONFIG_GLOBAL=/dev/null"));
    assert!(!env_text.contains("GIT_DIR="));
    assert!(!env_text.contains("GIT_WORK_TREE="));
    assert!(!env_text.contains("GIT_INDEX_FILE="));
    assert!(!env_text.contains("git_common_dir="));
    assert!(!env_text.contains("GIT_CONFIG_COUNT="));

    // Also spawn the actual `threadlane-worker` binary with a poisoned process environment
    // (`GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_CONFIG_*`) to prove real OS-inherited
    // environment sanitization across worktree creation, diff-gate, and cleanup.
    let spec_path = root.path().join("spec.json");
    let spec_json = serde_json::json!({
        "provider": "antigravity",
        "executable": fs::canonicalize("/bin/sh").expect("canonical fixture shell"),
        "model": "gemini-4-argon-high",
        "effort": "high",
        "repository_path": target_repo,
        "base_commit": target_base,
        "work_area_root": work_area,
        "allowed_write_paths": ["src"],
        "prompt": "Diagnostic spec"
    });
    fs::write(&spec_path, serde_json::to_vec_pretty(&spec_json).unwrap()).expect("write spec");

    let worker_bin = PathBuf::from(env!("CARGO_BIN_EXE_threadlane-worker"));
    let create_out = Command::new(&worker_bin)
        .args([
            "create-worktrees",
            "--spec",
            spec_path.to_str().unwrap(),
            "--run-id",
            "run-env-test",
            "--task-id",
            "task-env-test",
        ])
        .env("GIT_DIR", victim_repo.join(".git"))
        .env("GIT_WORK_TREE", &victim_repo)
        .env("GIT_INDEX_FILE", victim_repo.join(".git/index"))
        .env("GIT_COMMON_DIR", victim_repo.join(".git"))
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", &poison_hooks)
        .output()
        .expect("spawn threadlane-worker create-worktrees with poisoned GIT_* env");
    assert!(
        create_out.status.success(),
        "create-worktrees failed under poisoned GIT_* env: {}",
        String::from_utf8_lossy(&create_out.stderr)
    );

    // Verify victim repository HEAD, index, config, and staged content are 100% untouched.
    let victim_head_after =
        resolve_exact_commit_sha(&victim_repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("victim HEAD");
    assert_eq!(victim_head_after, victim_base);
    assert_eq!(
        fs::read(victim_repo.join(".git/index")).expect("read victim index after"),
        victim_index_before
    );
    assert_eq!(
        fs::read(victim_repo.join(".git/config")).expect("read victim config after"),
        victim_config_before
    );
    assert!(
        !hook_sentinel.exists(),
        "poison git hook must never execute"
    );
}

#[test]
fn cleanup_integrated_worktree_verifies_ancestry_and_records_partial_then_complete_cleanup() {
    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let work_area = root.path().join("work_area");
    let base_commit = init_disposable_repo(&repo_dir);

    let manager = WorktreeManager::new(&repo_dir, &work_area, &base_commit).expect("manager");
    let run_wt = manager
        .create_run_worktree_with_token("run-01", "tokrun01")
        .expect("create run worktree");
    let mut allowed = BTreeSet::new();
    allowed.insert("src".to_string());
    let task_wt = manager
        .create_task_worktree_with_token("run-01", "task-01", "toktask01", &allowed)
        .expect("create task worktree");

    // Commit a productive change in the task worktree.
    let task_head = commit_file_in_worktree(
        &task_wt.checkout_path,
        "src/lib.rs",
        "pub fn initial() -> u32 { 42 }\n",
        "Task update",
    );
    assert_ne!(task_head, base_commit);
    assert!(
        evaluate_diff_gate(&work_area, &task_wt.worktree_id)
            .expect("diff gate")
            .passed
    );

    // 1. Existing `cleanup_worktree` MUST refuse because HEAD != base_commit, and must not rewrite base_commit.
    let legacy_err = cleanup_worktree(&CleanupRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
    })
    .expect_err("existing cleanup_worktree must refuse productive worktree");
    match legacy_err {
        WorkerError::CleanupRefused { reason, .. } => {
            assert_eq!(reason, CleanupRefusalReason::DirtyWorktree);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    let reloaded_manifest =
        load_and_verify_manifest(&work_area, &task_wt.worktree_id).expect("reload manifest");
    assert_eq!(reloaded_manifest.base_commit, base_commit);

    // 2. `cleanup_integrated_worktree` MUST refuse before `task_head` is merged into `run_wt`
    // because `task_head` is not an ancestor of `run_wt`'s current HEAD (`base_commit`).
    let subject_digest = "a".repeat(64);
    let unmerged_err = cleanup_integrated_worktree(&IntegratedCleanupRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
        expected_base_commit: base_commit.clone(),
        accepted_head_commit: task_head.clone(),
        expected_target_commit: base_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        review_subject_digest: subject_digest.clone(),
        review_receipt_id: "rev-01".to_string(),
        integration_receipt_id: "int-01".to_string(),
    })
    .expect_err("must refuse when accepted_head is not in expected_target_commit ancestry");
    match unmerged_err {
        WorkerError::CleanupRefused { reason, detail, .. } => {
            assert_eq!(reason, CleanupRefusalReason::DirtyWorktree);
            assert!(detail.contains("not an ancestor"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(task_wt.checkout_path.exists());

    // 3. Merge `task_head` into the integration worktree (`run_wt`) with `--no-ff`, keeping
    // the primary source checkout (`repo_dir`) at `base_commit`.
    run_sanitized_git(
        &run_wt.checkout_path,
        &[
            "-c",
            "user.name=Threadlane Operator",
            "-c",
            "user.email=operator@threadlane.local",
            "-c",
            "commit.gpgsign=false",
            "merge",
            "--no-ff",
            "--no-edit",
            "-m",
            "Merge task-01 into run-01",
            "--",
            &task_head,
        ],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("merge into integration worktree");
    let target_commit = resolve_exact_commit_sha(
        &run_wt.checkout_path,
        "HEAD",
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("resolve integration HEAD");
    assert!(is_commit_ancestor(
        &repo_dir,
        &task_head,
        &target_commit,
        DEFAULT_GIT_COMMAND_TIMEOUT_MS
    )
    .expect("ancestry check"));
    assert_eq!(
        resolve_exact_commit_sha(&repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS).unwrap(),
        base_commit,
        "primary source checkout HEAD must remain at base_commit"
    );

    // 4. Refuse if the task worktree has an untracked/dirty file (preserve dirty artifacts!).
    let dirty_file = task_wt.checkout_path.join("src/untracked_note.txt");
    fs::write(&dirty_file, "preserve me").expect("write dirty file");
    let dirty_err = cleanup_integrated_worktree(&IntegratedCleanupRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
        expected_base_commit: base_commit.clone(),
        accepted_head_commit: task_head.clone(),
        expected_target_commit: target_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        review_subject_digest: subject_digest.clone(),
        review_receipt_id: "rev-01".to_string(),
        integration_receipt_id: "int-01".to_string(),
    })
    .expect_err("must refuse dirty checkout");
    match dirty_err {
        WorkerError::CleanupRefused { reason, .. } => {
            assert_eq!(reason, CleanupRefusalReason::DirtyWorktree);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(dirty_file.exists(), "dirty artifact must be preserved");
    fs::remove_file(&dirty_file).expect("clean up test dirty file");
    assert!(is_worktree_clean(&task_wt.checkout_path, DEFAULT_GIT_COMMAND_TIMEOUT_MS).unwrap());

    // 5. Refuse if an active job lock is held on the worktree.
    {
        let job_guard = begin_worktree_job(&work_area, &task_wt.worktree_id).expect("begin job");
        let active_err = cleanup_integrated_worktree(&IntegratedCleanupRequest {
            work_area_root: work_area.clone(),
            worktree_id: task_wt.worktree_id.clone(),
            expected_base_commit: base_commit.clone(),
            accepted_head_commit: task_head.clone(),
            expected_target_commit: target_commit.clone(),
            expected_source_head_commit: base_commit.clone(),
            review_subject_digest: subject_digest.clone(),
            review_receipt_id: "rev-01".to_string(),
            integration_receipt_id: "int-01".to_string(),
        })
        .expect_err("must refuse active job");
        match active_err {
            WorkerError::CleanupRefused { reason, .. } => {
                assert_eq!(reason, CleanupRefusalReason::ActiveJob);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        job_guard.finish().expect("finish job");
    }

    // 6. Now execute `cleanup_integrated_worktree` while primary source repo HEAD is still `base_commit`.
    // The checkout directory is cleanly removed, while safe `git branch -d` refuses to delete the
    // branch because `repo_dir` HEAD does not yet contain `task_head`. Partial cleanup is recorded!
    let partial_report = cleanup_integrated_worktree(&IntegratedCleanupRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
        expected_base_commit: base_commit.clone(),
        accepted_head_commit: task_head.clone(),
        expected_target_commit: target_commit.clone(),
        expected_source_head_commit: base_commit.clone(),
        review_subject_digest: subject_digest.clone(),
        review_receipt_id: "rev-01".to_string(),
        integration_receipt_id: "int-01".to_string(),
    })
    .expect("partial integrated cleanup");
    assert!(partial_report.checkout_removed);
    assert!(!partial_report.branch_removed);
    assert!(!partial_report.manifest_removed);
    assert!(partial_report.partial_cleanup);
    assert!(partial_report.branch_preserved_reason.is_some());
    assert!(!task_wt.checkout_path.exists());
    assert!(task_wt.manifest_path.exists());

    // 7. Fast-forward primary source repo to `target_commit` and re-run `cleanup_integrated_worktree`
    // to complete safe `git branch -d` and manifest removal.
    run_sanitized_git(
        &repo_dir,
        &["merge", "--ff-only", "--", &target_commit],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("fast-forward primary repo in disposable test");

    let final_report = cleanup_integrated_worktree(&IntegratedCleanupRequest {
        work_area_root: work_area.clone(),
        worktree_id: task_wt.worktree_id.clone(),
        expected_base_commit: base_commit,
        accepted_head_commit: task_head,
        expected_target_commit: target_commit.clone(),
        expected_source_head_commit: target_commit,
        review_subject_digest: subject_digest,
        review_receipt_id: "rev-01".to_string(),
        integration_receipt_id: "int-01".to_string(),
    })
    .expect("complete integrated cleanup");
    assert!(final_report.checkout_removed);
    assert!(final_report.branch_removed);
    assert!(final_report.manifest_removed);
    assert!(!final_report.partial_cleanup);
    assert!(!task_wt.manifest_path.exists());
}

#[test]
fn concurrent_worktree_manager_constructors_on_same_new_root_serialize_and_create_valid_worktrees()
{
    use std::sync::{Arc, Barrier};
    use std::thread;
    use threadlane_workers::git::{WORK_AREA_LOCK_FILENAME, WORK_AREA_MARKER_FILENAME};

    let root = create_private_tempdir();
    let repo_dir = root.path().join("repo");
    let base_commit = init_disposable_repo(&repo_dir);

    // Exercise both brand-new non-existent roots and pre-existing empty 0700 roots across multiple rounds.
    for round in 0..8 {
        let work_area = root.path().join(format!("work_area_round_{round}"));
        if round % 2 == 1 {
            fs::create_dir_all(&work_area).expect("precreate empty work_area");
            fs::set_permissions(&work_area, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 work_area");
        }

        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for worker_idx in 0..2 {
            let barrier = Arc::clone(&barrier);
            let repo_clone = repo_dir.clone();
            let area_clone = work_area.clone();
            let base_clone = base_commit.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();
                let manager = WorktreeManager::new(&repo_clone, &area_clone, &base_clone)
                    .expect("concurrent WorktreeManager::new must serialize cleanly");
                let mut allowed = BTreeSet::new();
                allowed.insert("src".to_string());
                let run_id = format!("run-r{round}");
                let task_id = format!("task-w{worker_idx}");
                let wt = manager
                    .create_task_worktree(&run_id, &task_id, &allowed)
                    .expect("create owned task worktree after concurrent init");
                let verified = load_and_verify_manifest(&area_clone, &wt.worktree_id)
                    .expect("verify manifest");
                assert_eq!(verified.worktree_id, wt.worktree_id);
                assert_eq!(verified.base_commit, base_clone);
                let gate =
                    evaluate_diff_gate(&area_clone, &wt.worktree_id).expect("evaluate diff gate");
                assert!(gate.passed);
                wt.worktree_id
            }));
        }

        let mut created_ids = BTreeSet::new();
        for handle in handles {
            let wt_id = handle.join().expect("worker thread panicked");
            assert!(created_ids.insert(wt_id));
        }
        assert_eq!(created_ids.len(), 2);

        let lock_meta =
            fs::symlink_metadata(work_area.join(WORK_AREA_LOCK_FILENAME)).expect("lock metadata");
        assert!(lock_meta.file_type().is_file());
        assert!(!lock_meta.file_type().is_symlink());
        assert_eq!(lock_meta.permissions().mode() & 0o777, 0o600);
        assert_eq!(lock_meta.len(), 0);

        let marker_meta = fs::symlink_metadata(work_area.join(WORK_AREA_MARKER_FILENAME))
            .expect("marker metadata");
        assert!(marker_meta.file_type().is_file());
        assert!(!marker_meta.file_type().is_symlink());
        assert_eq!(marker_meta.permissions().mode() & 0o777, 0o600);
    }
}

#[test]
fn work_area_root_rejects_unrelated_nonempty_mismatched_repo_and_invalid_lock_files() {
    use std::os::unix::fs::symlink;
    use threadlane_workers::git::{WORK_AREA_LOCK_FILENAME, WORK_AREA_MARKER_FILENAME};

    let root = create_private_tempdir();
    let repo_a = root.path().join("repo_a");
    let repo_b = root.path().join("repo_b");
    let base_a = init_disposable_repo(&repo_a);
    let base_b = init_disposable_repo(&repo_b);

    // 1. Reject unrelated non-empty directory and preserve its contents without leaving a lockfile behind.
    let unrelated_dir = root.path().join("unrelated_nonempty");
    fs::create_dir_all(&unrelated_dir).expect("create unrelated_dir");
    fs::set_permissions(&unrelated_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 unrelated_dir");
    let unrelated_file = unrelated_dir.join("unrelated.txt");
    fs::write(&unrelated_file, "user data").expect("write unrelated file");

    let err = WorktreeManager::new(&repo_a, &unrelated_dir, &base_a)
        .expect_err("must reject unrelated non-empty work_area_root");
    match err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("without valid Threadlane ownership marker"));
            assert!(msg.contains("unrelated.txt"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(unrelated_file.exists());
    assert!(!unrelated_dir.join(WORK_AREA_LOCK_FILENAME).exists());
    assert!(!unrelated_dir.join(WORK_AREA_MARKER_FILENAME).exists());

    // 2. Reject a directory containing an unpersisted/stray temporary marker file (`..threadlane-work-area.json.tmp.*`).
    let stray_tmp_dir = root.path().join("stray_tmp_dir");
    fs::create_dir_all(&stray_tmp_dir).expect("create stray_tmp_dir");
    fs::set_permissions(&stray_tmp_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 stray_tmp_dir");
    fs::write(
        stray_tmp_dir.join("..threadlane-work-area.json.tmp.VTss7m"),
        "{}",
    )
    .expect("write stray tempfile");

    let stray_err = WorktreeManager::new(&repo_a, &stray_tmp_dir, &base_a)
        .expect_err("must reject stray tempfile without ownership marker");
    match stray_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("without valid Threadlane ownership marker"));
            assert!(msg.contains("..threadlane-work-area.json.tmp.VTss7m"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // 3. Reject reusing an owned work_area_root for a different repository (`repository_common_git_dir` mismatch).
    let owned_area = root.path().join("owned_area");
    let _manager_a =
        WorktreeManager::new(&repo_a, &owned_area, &base_a).expect("init owned_area for repo_a");
    let mismatch_err = WorktreeManager::new(&repo_b, &owned_area, &base_b)
        .expect_err("must reject mismatched repository");
    match mismatch_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("is owned by repository"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // 4. Reject symlinked lock file (both pointing to a real file and dangling).
    let symlink_lock_area = root.path().join("symlink_lock_area");
    fs::create_dir_all(&symlink_lock_area).expect("create symlink_lock_area");
    fs::set_permissions(&symlink_lock_area, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 symlink_lock_area");
    let symlink_target = root.path().join("lock_target.txt");
    fs::write(&symlink_target, "").expect("write symlink target");
    fs::set_permissions(&symlink_target, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 symlink target");
    symlink(
        &symlink_target,
        symlink_lock_area.join(WORK_AREA_LOCK_FILENAME),
    )
    .expect("create symlink lock");

    let sym_err = WorktreeManager::new(&repo_a, &symlink_lock_area, &base_a)
        .expect_err("must reject symlinked work_area lock");
    match sym_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("symlink"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let dangling_lock_area = root.path().join("dangling_lock_area");
    fs::create_dir_all(&dangling_lock_area).expect("create dangling_lock_area");
    fs::set_permissions(&dangling_lock_area, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 dangling_lock_area");
    symlink(
        root.path().join("nonexistent_lock_target"),
        dangling_lock_area.join(WORK_AREA_LOCK_FILENAME),
    )
    .expect("create dangling symlink lock");

    let dangling_err = WorktreeManager::new(&repo_a, &dangling_lock_area, &base_a)
        .expect_err("must reject dangling symlinked work_area lock");
    match dangling_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("symlink"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // 5. Reject non-private (0644) or non-empty pre-existing lock file.
    let insecure_lock_area = root.path().join("insecure_lock_area");
    fs::create_dir_all(&insecure_lock_area).expect("create insecure_lock_area");
    fs::set_permissions(&insecure_lock_area, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 insecure_lock_area");
    let insecure_lock_path = insecure_lock_area.join(WORK_AREA_LOCK_FILENAME);
    fs::write(&insecure_lock_path, "").expect("write insecure lock");
    fs::set_permissions(&insecure_lock_path, fs::Permissions::from_mode(0o644))
        .expect("chmod 0644 lock");

    let perm_err = WorktreeManager::new(&repo_a, &insecure_lock_area, &base_a)
        .expect_err("must reject 0644 work_area lock");
    match perm_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("insecure permissions"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    fs::set_permissions(&insecure_lock_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 lock");
    fs::write(&insecure_lock_path, "non-empty lock payload").expect("write non-empty lock");
    let nonempty_lock_err = WorktreeManager::new(&repo_a, &insecure_lock_area, &base_a)
        .expect_err("must reject non-empty 0600 lock file");
    match nonempty_lock_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("0-byte lock file"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    fs::remove_file(&insecure_lock_path).expect("remove non-empty lock file");
    fs::create_dir(&insecure_lock_path).expect("create directory at lock path");
    fs::set_permissions(&insecure_lock_path, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 lock dir");
    let nonregular_lock_err = WorktreeManager::new(&repo_a, &insecure_lock_area, &base_a)
        .expect_err("must reject directory at lock path");
    match nonregular_lock_err {
        WorkerError::SecurityViolation(msg) => {
            assert!(msg.contains("regular file"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
