#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::PathBuf;
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::watch;

use threadlane_native::coworker_workflow::controller_policy::{
    FrozenCheckCommandSpec, FrozenCheckKind,
};
use threadlane_native::coworker_workflow::{
    run_frozen_check, run_frozen_check_with_cancel, run_frozen_check_with_claim, sha256_hex,
    verify_and_promote_check_reports, verify_candidate_check_evidence, CandidateCheckEvidence,
    CoworkerJobOwnershipKey, CoworkerJobOwnershipStore, FrozenCheckExecutionReport,
    FrozenCheckExecutionStatus, FrozenCheckSubject, FrozenCheckTargetKind,
};
use threadlane_workers::{
    begin_worktree_job, is_process_alive, is_process_group_alive, resolve_exact_commit_sha,
    resolve_exact_tree_sha, run_sanitized_git, WorktreeManager, WorktreeManifest,
    DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

const TEST_THREAD_UUID: &str = "123e4567-e89b-42d3-a456-426614174000";
const OTHER_THREAD_UUID: &str = "223e4567-e89b-42d3-a456-426614174111";

struct CheckTestFixture {
    _tmp: TempDir,
    root: PathBuf,
    repo_path: PathBuf,
    work_area_root: PathBuf,
    bin_dir: PathBuf,
    manager: WorktreeManager,
    ownership_store: CoworkerJobOwnershipStore,
}

impl CheckTestFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        let repo_path = root.join("repo");
        let work_area_root = root.join("work-area");
        let lock_root_dir = root.join("ownership-locks");
        let bin_dir = root.join("bin");

        fs::create_dir_all(repo_path.join("crates/sub/src")).expect("create repo subdir");
        fs::create_dir_all(&bin_dir).expect("create bin_dir");
        fs::create_dir_all(&lock_root_dir).expect("create lock_root_dir");
        fs::set_permissions(&lock_root_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 lock_root_dir");

        run_sanitized_git(
            &repo_path,
            &["init", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git init");
        run_sanitized_git(
            &repo_path,
            &["config", "user.name", "Threadlane Check Test"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.name");
        run_sanitized_git(
            &repo_path,
            &["config", "user.email", "checks@threadlane.local"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git config user.email");

        fs::write(
            repo_path.join("crates/sub/src/lib.rs"),
            "pub fn answer() -> u32 { 42 }\n",
        )
        .expect("write baseline file");
        run_sanitized_git(&repo_path, &["add", "."], DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("git add");
        run_sanitized_git(
            &repo_path,
            &["commit", "-m", "initial", "--quiet"],
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("git commit");

        let base_commit =
            resolve_exact_commit_sha(&repo_path, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
                .expect("resolve HEAD commit");
        let manager =
            WorktreeManager::new(&repo_path, &work_area_root, &base_commit).expect("worktree mgr");
        let ownership_store =
            CoworkerJobOwnershipStore::new(&lock_root_dir).expect("ownership store");

        Self {
            _tmp: tmp,
            root,
            repo_path,
            work_area_root: manager.work_area_root().to_path_buf(),
            bin_dir,
            manager,
            ownership_store,
        }
    }

    fn write_executable(&self, name: &str, body: &str) -> PathBuf {
        let staging = self.bin_dir.join(format!(".{name}.tmp"));
        let final_path = self.bin_dir.join(name);
        {
            let mut file = File::create(&staging).expect("create staging script");
            file.write_all(body.as_bytes()).expect("write script");
            file.sync_all().expect("sync script");
        }
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
        fs::rename(&staging, &final_path).expect("rename executable");
        fs::canonicalize(&final_path).expect("canonicalize executable")
    }

    fn create_task_worktree(&self, run_id: &str, task_id: &str, token: &str) -> WorktreeManifest {
        let allowed = BTreeSet::from(["crates/sub/src/lib.rs".to_string()]);
        self.manager
            .create_task_worktree_with_token(run_id, task_id, token, &allowed)
            .expect("create task worktree")
    }

    fn create_run_worktree(&self, run_id: &str, token: &str) -> WorktreeManifest {
        self.manager
            .create_run_worktree_with_token(run_id, token)
            .expect("create run integration worktree")
    }

    fn build_task_subject(
        &self,
        manifest: &WorktreeManifest,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> FrozenCheckSubject {
        let head_commit = resolve_exact_commit_sha(
            &manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("head commit");
        let tree_sha = resolve_exact_tree_sha(
            &manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("tree sha");
        FrozenCheckSubject {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            job_id: job_id.to_string(),
            target_kind: FrozenCheckTargetKind::TaskWorktree,
            repository_path: self.repo_path.clone(),
            work_area_root: self.work_area_root.clone(),
            worktree_id: manifest.worktree_id.clone(),
            expected_run_id: manifest.run_id.clone(),
            expected_task_id: manifest.task_id.clone(),
            expected_base_commit: manifest.base_commit.clone(),
            expected_head_commit: head_commit,
            expected_tree_sha: tree_sha,
        }
    }

    fn build_run_integration_subject(
        &self,
        manifest: &WorktreeManifest,
        binding_id: &str,
        thread_uuid: &str,
        job_id: &str,
    ) -> FrozenCheckSubject {
        let head_commit = resolve_exact_commit_sha(
            &manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("head commit");
        let tree_sha = resolve_exact_tree_sha(
            &manifest.checkout_path,
            "HEAD",
            DEFAULT_GIT_COMMAND_TIMEOUT_MS,
        )
        .expect("tree sha");
        FrozenCheckSubject {
            binding_id: binding_id.to_string(),
            thread_uuid: thread_uuid.to_string(),
            job_id: job_id.to_string(),
            target_kind: FrozenCheckTargetKind::RunIntegrationWorktree,
            repository_path: self.repo_path.clone(),
            work_area_root: self.work_area_root.clone(),
            worktree_id: manifest.worktree_id.clone(),
            expected_run_id: manifest.run_id.clone(),
            expected_task_id: None,
            expected_base_commit: manifest.base_commit.clone(),
            expected_head_commit: head_commit,
            expected_tree_sha: tree_sha,
        }
    }
}

#[tokio::test]
async fn real_frozen_check_executes_in_owned_task_and_run_worktrees_and_promotes_verified_evidence()
{
    let fixture = CheckTestFixture::new();
    let task_wt = fixture.create_task_worktree("run-alpha", "task-1", "tok01");
    let run_wt = fixture.create_run_worktree("run-alpha", "tok02");

    let key = CoworkerJobOwnershipKey::new("bind-alpha", TEST_THREAD_UUID, "job-alpha")
        .expect("ownership key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_000)
        .expect("try_acquire_job")
        .into_guard()
        .expect("acquired guard");
    let claim = guard.issue_driver_claim();

    let sentinel = fixture.root.join("NEVER_TOUCHED");
    let check_bin = fixture.write_executable(
        "cargo-check-fake",
        r#"#!/bin/sh
set -eu
printf 'CWD=%s\n' "$(pwd -P)"
printf 'ARGS=%s\n' "$*"
i=0
while [ "$i" -lt 10 ]; do
    printf 'UTF8_LINE_%02d=çğıöşü-日本語-🦀-verified\n' "$i"
    i=$((i + 1))
done
printf 'STDERR_OK=clean-run\n' >&2
exit 0
"#,
    );

    let literal_arg = format!("--filter=literal; touch {} | $(id)", sentinel.display());
    let spec = FrozenCheckCommandSpec {
        check_id: "cargo_test_sub".to_string(),
        kind: FrozenCheckKind::Tests,
        executable: check_bin,
        argv: vec!["test".to_string(), literal_arg.clone()],
        repo_relative_cwd: "crates/sub".to_string(),
        timeout_ms: 5_000,
        max_stdout_bytes: 4_096,
        max_stderr_bytes: 1_024,
        required_for_task_review: true,
        required_for_candidate_closure: true,
    };

    // 1. Execute on Task worktree with claim & summary verification.
    let task_subject =
        fixture.build_task_subject(&task_wt, "bind-alpha", TEST_THREAD_UUID, "job-alpha");
    let (_cancel_tx, mut cancel_rx) = watch::channel(false);
    let task_report = run_frozen_check_with_claim(
        &guard,
        Some(&claim),
        Some(guard.summary()),
        &task_subject,
        &spec,
        &mut cancel_rx,
    )
    .await
    .expect("run frozen check on task worktree");

    assert!(
        task_report.passed(),
        "expected passed report: {task_report:?}"
    );
    assert_eq!(task_report.status, FrozenCheckExecutionStatus::Passed);
    assert_eq!(task_report.exit_code, Some(0));
    assert_eq!(task_report.signal, None);
    assert!(task_report.stdout_bytes_captured > 256);
    assert!(task_report.post_worktree_clean);
    assert!(!task_report.subject_drifted);
    assert!(task_report.ownership_verified_post_run);
    assert_eq!(
        task_report.resolved_working_directory,
        task_wt.checkout_path.join("crates/sub")
    );
    assert!(
        !sentinel.exists(),
        "literal metacharacters must not execute in a shell"
    );

    // Serde round-trip recovery verification + promotion to CandidateCheckEvidence.
    let json = serde_json::to_string(&task_report).expect("serialize report");
    let recovered: FrozenCheckExecutionReport =
        serde_json::from_str(&json).expect("deserialize report");
    recovered
        .validate_integrity()
        .expect("validate_integrity after serde");
    recovered
        .verify_against_context(&task_subject, &spec)
        .expect("verify_against_context after serde");

    let evidence = recovered
        .to_candidate_check_evidence(&task_subject, &spec)
        .expect("promote to CandidateCheckEvidence");
    assert_eq!(evidence.check_id, "cargo_test_sub");
    assert!(evidence.passed);
    assert_eq!(evidence.exit_code, 0);
    assert_eq!(evidence.evidence_digest, recovered.evidence_digest);
    verify_candidate_check_evidence(&evidence, &recovered, &task_subject, &spec)
        .expect("verify_candidate_check_evidence");

    // 2. Execute on RunIntegration worktree and promote via verify_and_promote_check_reports.
    let run_subject =
        fixture.build_run_integration_subject(&run_wt, "bind-alpha", TEST_THREAD_UUID, "job-alpha");
    let run_report = run_frozen_check(&guard, &run_subject, &spec)
        .await
        .expect("run frozen check on run integration worktree");
    assert!(run_report.passed());

    let mut check_map = BTreeMap::new();
    check_map.insert(spec.check_id.clone(), spec.clone());
    let promoted_list = verify_and_promote_check_reports(
        &run_subject,
        &check_map,
        std::slice::from_ref(&run_report),
    )
    .expect("verify_and_promote_check_reports");
    assert_eq!(promoted_list.len(), 1);
    assert_eq!(promoted_list[0].evidence_digest, run_report.evidence_digest);
}

#[tokio::test]
async fn nonzero_timeout_cancel_and_stdout_stderr_overflow_never_pass_or_promote() {
    let fixture = CheckTestFixture::new();
    let task_wt = fixture.create_task_worktree("run-beta", "task-1", "tok01");
    let subject = fixture.build_task_subject(&task_wt, "bind-beta", TEST_THREAD_UUID, "job-beta");

    let key = subject.ownership_key().expect("key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_100)
        .expect("acquire")
        .into_guard()
        .expect("guard");

    let pid_file = fixture.root.join("grandchild.pid");
    let mode_bin = fixture.write_executable(
        "cargo-modes-fake",
        r#"#!/bin/sh
set -eu
mode="$1"
pid_out="${2:-}"
if [ "$mode" = "nonzero" ]; then
    printf 'lint error\n' >&2
    exit 9
elif [ "$mode" = "sleep" ]; then
    sleep 30 &
    wait
elif [ "$mode" = "stdout_over" ]; then
    head -c 65 /dev/zero | tr '\0' 'X'
    exit 0
elif [ "$mode" = "stderr_over" ]; then
    head -c 33 /dev/zero | tr '\0' 'Y' >&2
    exit 0
elif [ "$mode" = "waited_descendant_exit0" ]; then
    (sleep 0.02; printf 'sub-ok\n') &
    child_pid=$!
    if [ -n "$pid_out" ]; then
        echo "$child_pid" > "$pid_out"
    fi
    wait "$child_pid"
    printf 'parent-ok\n'
    exit 0
elif [ "$mode" = "lingering_descendant_exit0" ]; then
    sleep 30 &
    if [ -n "$pid_out" ]; then
        echo $! > "$pid_out"
    fi
    printf 'parent-exited-early\n'
    exit 0
fi
"#,
    );

    let make_spec = |mode: &str, timeout_ms: u64| FrozenCheckCommandSpec {
        check_id: format!("check_{mode}"),
        kind: FrozenCheckKind::Clippy,
        executable: mode_bin.clone(),
        argv: vec![mode.to_string()],
        repo_relative_cwd: ".".to_string(),
        timeout_ms,
        max_stdout_bytes: 64,
        max_stderr_bytes: 32,
        required_for_task_review: true,
        required_for_candidate_closure: false,
    };

    // 1. Non-zero exit
    let nz_spec = make_spec("nonzero", 5_000);
    let nz_report = run_frozen_check(&guard, &subject, &nz_spec)
        .await
        .expect("nonzero check");
    assert!(!nz_report.passed());
    assert_eq!(nz_report.status, FrozenCheckExecutionStatus::NonZeroExit);
    assert_eq!(nz_report.exit_code, Some(9));
    assert!(nz_report
        .to_candidate_check_evidence(&subject, &nz_spec)
        .is_err());

    // 2. Timeout
    let to_spec = make_spec("sleep", 120);
    let to_report = run_frozen_check(&guard, &subject, &to_spec)
        .await
        .expect("timeout check");
    assert!(!to_report.passed());
    assert_eq!(to_report.status, FrozenCheckExecutionStatus::TimedOut);
    assert!(!is_process_group_alive(
        to_report.process_group_id.expect("pgid")
    ));
    assert!(to_report
        .to_candidate_check_evidence(&subject, &to_spec)
        .is_err());

    // 3. Cancellation
    let cancel_spec = make_spec("sleep", 10_000);
    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = cancel_tx.send(true);
    });
    let cancel_report =
        run_frozen_check_with_cancel(&guard, &subject, &cancel_spec, &mut cancel_rx)
            .await
            .expect("cancelled check");
    assert!(!cancel_report.passed());
    assert_eq!(cancel_report.status, FrozenCheckExecutionStatus::Cancelled);
    assert!(!is_process_group_alive(
        cancel_report.process_group_id.expect("pgid")
    ));
    assert!(cancel_report
        .to_candidate_check_evidence(&subject, &cancel_spec)
        .is_err());

    // 4. Stdout overflow (65 bytes vs 64 cap)
    let out_spec = make_spec("stdout_over", 5_000);
    let out_report = run_frozen_check(&guard, &subject, &out_spec)
        .await
        .expect("stdout overflow check");
    assert!(!out_report.passed());
    assert_eq!(
        out_report.status,
        FrozenCheckExecutionStatus::OutputOverflow
    );
    assert!(out_report.stdout_overflowed);
    assert_eq!(out_report.stdout_bytes_captured, 64);
    assert!(out_report
        .to_candidate_check_evidence(&subject, &out_spec)
        .is_err());

    // 5. Stderr overflow (33 bytes vs 32 cap)
    let err_spec = make_spec("stderr_over", 5_000);
    let err_report = run_frozen_check(&guard, &subject, &err_spec)
        .await
        .expect("stderr overflow check");
    assert!(!err_report.passed());
    assert_eq!(
        err_report.status,
        FrozenCheckExecutionStatus::OutputOverflow
    );
    assert!(err_report.stderr_overflowed);
    assert_eq!(err_report.stderr_bytes_captured, 32);
    assert!(err_report
        .to_candidate_check_evidence(&subject, &err_spec)
        .is_err());

    // 6. Naturally waited child subprocess before parent exit 0 passes and promotes cleanly
    let _ = fs::remove_file(&pid_file);
    let mut waited_spec = make_spec("waited_descendant_exit0", 5_000);
    waited_spec.argv = vec![
        "waited_descendant_exit0".to_string(),
        pid_file.to_string_lossy().to_string(),
    ];
    let waited_report = run_frozen_check(&guard, &subject, &waited_spec)
        .await
        .expect("waited descendant check");
    assert!(waited_report.passed());
    assert_eq!(waited_report.status, FrozenCheckExecutionStatus::Passed);
    assert_eq!(waited_report.exit_code, Some(0));
    assert!(!waited_report.descendants_terminated_after_exit);
    assert!(!waited_report.cleanup_incomplete);
    assert!(waited_report
        .to_candidate_check_evidence(&subject, &waited_spec)
        .is_ok());

    // 7. Surviving descendant force-killed after parent exit 0 reaps process group, preserves
    // exit_code == Some(0) and cleanup_incomplete == false, classifies as
    // DescendantsTerminatedAfterExit, and never passes or promotes to CandidateCheckEvidence.
    let _ = fs::remove_file(&pid_file);
    let mut lingering_spec = make_spec("lingering_descendant_exit0", 5_000);
    lingering_spec.argv = vec![
        "lingering_descendant_exit0".to_string(),
        pid_file.to_string_lossy().to_string(),
    ];
    let lingering_report = run_frozen_check(&guard, &subject, &lingering_spec)
        .await
        .expect("lingering descendant check");
    assert!(!lingering_report.passed());
    assert_eq!(
        lingering_report.status,
        FrozenCheckExecutionStatus::DescendantsTerminatedAfterExit
    );
    assert_eq!(lingering_report.exit_code, Some(0));
    assert!(lingering_report.descendants_terminated_after_exit);
    assert!(!lingering_report.cleanup_incomplete);
    assert!(lingering_report.post_worktree_clean);
    assert!(!lingering_report.subject_drifted);
    assert!(!is_process_group_alive(
        lingering_report.process_group_id.expect("pgid")
    ));
    let raw_pid = fs::read_to_string(&pid_file).expect("read grandchild pid");
    let grandchild_pid: u32 = raw_pid.trim().parse().expect("parse grandchild pid");
    assert!(!is_process_alive(grandchild_pid));
    assert!(lingering_report
        .to_candidate_check_evidence(&subject, &lingering_spec)
        .is_err());
}

#[tokio::test]
async fn wrong_guard_unowned_main_checkout_symlink_cwd_active_job_and_subject_mutation_refuse_passing_evidence(
) {
    let fixture = CheckTestFixture::new();
    let task_wt = fixture.create_task_worktree("run-gamma", "task-1", "tok01");
    let subject = fixture.build_task_subject(&task_wt, "bind-gamma", TEST_THREAD_UUID, "job-gamma");

    let key = subject.ownership_key().expect("key");
    let first_guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_200)
        .expect("acquire first")
        .into_guard()
        .expect("first guard");
    let stale_summary = first_guard.summary().clone();
    drop(first_guard);

    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_300)
        .expect("reacquire")
        .into_guard()
        .expect("active guard");

    let ok_bin = fixture.write_executable("cargo-ok", "#!/bin/sh\nprintf 'ok\\n'\nexit 0\n");
    let base_spec = FrozenCheckCommandSpec {
        check_id: "cargo_fmt".to_string(),
        kind: FrozenCheckKind::Formatting,
        executable: ok_bin.clone(),
        argv: vec!["--check".to_string()],
        repo_relative_cwd: ".".to_string(),
        timeout_ms: 5_000,
        max_stdout_bytes: 512,
        max_stderr_bytes: 512,
        required_for_task_review: true,
        required_for_candidate_closure: false,
    };

    // 1. Wrong ownership guard scope (different thread_uuid / job_id) is rejected before spawning.
    let wrong_key = CoworkerJobOwnershipKey::new("bind-gamma", OTHER_THREAD_UUID, "job-other")
        .expect("wrong key");
    let wrong_guard = fixture
        .ownership_store
        .try_acquire_job(&wrong_key, 1_700_000_000_301)
        .expect("acquire wrong")
        .into_guard()
        .expect("wrong guard");
    assert!(run_frozen_check(&wrong_guard, &subject, &base_spec)
        .await
        .is_err());

    // 2. Stale ownership summary from a prior lock acquisition is rejected before spawning.
    let (_tx, mut rx) = watch::channel(false);
    assert!(run_frozen_check_with_claim(
        &guard,
        None,
        Some(&stale_summary),
        &subject,
        &base_spec,
        &mut rx
    )
    .await
    .is_err());

    // 3. Unowned / main repository checkout or missing worktree manifest is rejected.
    let mut unowned_subject = subject.clone();
    unowned_subject.worktree_id = "wt-main-unowned".to_string();
    assert!(run_frozen_check(&guard, &unowned_subject, &base_spec)
        .await
        .is_err());

    // 4. Active worktree job lock is rejected without clobbering the held lock.
    let active_wt_guard =
        begin_worktree_job(&fixture.work_area_root, &task_wt.worktree_id).expect("begin job");
    assert!(run_frozen_check(&guard, &subject, &base_spec)
        .await
        .is_err());
    active_wt_guard.finish().expect("finish active job");

    // 5. Symlink repo_relative_cwd inside the worktree checkout is rejected before spawning.
    let outside_dir = fixture.root.join("outside-target");
    fs::create_dir_all(&outside_dir).expect("create outside_dir");
    let symlink_cwd_path = task_wt.checkout_path.join("crates/symlink_dir");
    symlink(&outside_dir, &symlink_cwd_path).expect("create symlink cwd");
    let mut symlink_spec = base_spec.clone();
    symlink_spec.repo_relative_cwd = "crates/symlink_dir".to_string();
    assert!(run_frozen_check(&guard, &subject, &symlink_spec)
        .await
        .is_err());
    fs::remove_file(&symlink_cwd_path).expect("remove symlink cwd");

    // 6. Check command that mutates the worktree (creates untracked file or modifies tracked file)
    // preserves outputs and worktree changes, records SubjectMutatedOrDrifted, and refuses promotion!
    let mutating_bin = fixture.write_executable(
        "cargo-mutating-check",
        r#"#!/bin/sh
set -eu
printf 'check ran but wrote side effect\n'
printf 'mutated by check\n' > check_side_effect.txt
exit 0
"#,
    );
    let mut mutating_spec = base_spec.clone();
    mutating_spec.executable = mutating_bin;
    let drift_report = run_frozen_check(&guard, &subject, &mutating_spec)
        .await
        .expect("mutating check returns report");
    assert!(!drift_report.passed());
    assert_eq!(
        drift_report.status,
        FrozenCheckExecutionStatus::SubjectMutatedOrDrifted
    );
    assert!(!drift_report.post_worktree_clean);
    assert!(drift_report.subject_drifted);
    assert!(drift_report.drift_reason.is_some());
    assert_eq!(
        String::from_utf8_lossy(&drift_report.stdout_bytes),
        "check ran but wrote side effect\n"
    );
    assert!(
        task_wt.checkout_path.join("check_side_effect.txt").exists(),
        "worktree changes must be preserved on disk and never discarded"
    );
    assert!(drift_report
        .to_candidate_check_evidence(&subject, &mutating_spec)
        .is_err());

    // 7. Subsequent check on the now-dirty worktree is refused pre-effect while preserving the file.
    assert!(run_frozen_check(&guard, &subject, &base_spec)
        .await
        .is_err());
    assert!(task_wt.checkout_path.join("check_side_effect.txt").exists());
}

#[tokio::test]
async fn tampered_report_bytes_changed_spec_changed_tree_and_model_prose_fail_verification() {
    let fixture = CheckTestFixture::new();
    let task_wt = fixture.create_task_worktree("run-delta", "task-1", "tok01");
    let subject = fixture.build_task_subject(&task_wt, "bind-delta", TEST_THREAD_UUID, "job-delta");

    let key = subject.ownership_key().expect("key");
    let guard = fixture
        .ownership_store
        .try_acquire_job(&key, 1_700_000_000_400)
        .expect("acquire")
        .into_guard()
        .expect("guard");

    let check_bin = fixture.write_executable(
        "cargo-verify-fake",
        "#!/bin/sh\nprintf 'verified-output\\n'\nexit 0\n",
    );
    let spec = FrozenCheckCommandSpec {
        check_id: "cargo_clippy".to_string(),
        kind: FrozenCheckKind::Clippy,
        executable: check_bin,
        argv: vec!["--all-targets".to_string()],
        repo_relative_cwd: ".".to_string(),
        timeout_ms: 5_000,
        max_stdout_bytes: 512,
        max_stderr_bytes: 512,
        required_for_task_review: true,
        required_for_candidate_closure: true,
    };

    let report = run_frozen_check(&guard, &subject, &spec)
        .await
        .expect("run passing check");
    assert!(report.passed());

    // 1. Mutating stdout_bytes fails validate_integrity (stdout_sha256 mismatch).
    let mut tampered_stdout = report.clone();
    tampered_stdout.stdout_bytes.push(b'!');
    tampered_stdout.stdout_bytes_captured = tampered_stdout.stdout_bytes.len();
    assert!(tampered_stdout.validate_integrity().is_err());

    // 2. Mutating stdout_bytes + stdout_sha256 without updating evidence_digest fails validate_integrity.
    tampered_stdout.stdout_sha256 = sha256_hex(&tampered_stdout.stdout_bytes);
    assert!(tampered_stdout.validate_integrity().is_err());

    // 3. Forging Passed status on a non-zero exit report (even if recomputing evidence_digest) fails validate_integrity.
    let mut forged_passed = report.clone();
    forged_passed.status = FrozenCheckExecutionStatus::Passed;
    forged_passed.exit_code = Some(1);
    forged_passed.evidence_digest = forged_passed
        .compute_evidence_digest()
        .expect("compute digest");
    assert!(forged_passed.validate_integrity().is_err());
    assert!(!forged_passed.passed());

    // 3b. Forging Passed status when descendants_terminated_after_exit is true (even with exit 0
    // and recomputed evidence_digest after serde round-trip) fails validate_integrity and promotion.
    let mut forged_descendants = report.clone();
    forged_descendants.status = FrozenCheckExecutionStatus::Passed;
    forged_descendants.descendants_terminated_after_exit = true;
    forged_descendants.evidence_digest = forged_descendants
        .compute_evidence_digest()
        .expect("compute digest");
    let forged_json = serde_json::to_string(&forged_descendants).expect("serialize forged");
    let recovered_forged: FrozenCheckExecutionReport =
        serde_json::from_str(&forged_json).expect("deserialize forged");
    assert!(recovered_forged.validate_integrity().is_err());
    assert!(!recovered_forged.passed());
    assert!(recovered_forged
        .to_candidate_check_evidence(&subject, &spec)
        .is_err());

    // 4. Changed FrozenCheckCommandSpec (different argv or timeout) fails verify_against_context and promotion.
    let mut changed_spec = spec.clone();
    changed_spec.argv = vec!["--lib".to_string()];
    assert!(report
        .verify_against_context(&subject, &changed_spec)
        .is_err());
    assert!(report
        .to_candidate_check_evidence(&subject, &changed_spec)
        .is_err());

    // 5. Changed subject tree SHA fails verify_against_context and promotion.
    let mut changed_tree_subject = subject.clone();
    changed_tree_subject.expected_tree_sha = "f".repeat(40);
    assert!(report
        .verify_against_context(&changed_tree_subject, &spec)
        .is_err());
    assert!(report
        .to_candidate_check_evidence(&changed_tree_subject, &spec)
        .is_err());

    // 6. Model prose or caller-fabricated CandidateCheckEvidence digest is rejected by verify_candidate_check_evidence.
    let fabricated_evidence = CandidateCheckEvidence {
        check_id: "cargo_clippy".to_string(),
        command_summary: report.command_summary.clone(),
        passed: true,
        exit_code: 0,
        evidence_digest: sha256_hex(b"model prose: all checks passed"),
    };
    assert!(
        verify_candidate_check_evidence(&fabricated_evidence, &report, &subject, &spec).is_err()
    );
}
