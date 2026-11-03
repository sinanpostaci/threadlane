use std::env;
use std::fs;
use std::hint::black_box;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use threadlane_native::coworker_workflow::{
    CoworkerJobLockState, CoworkerJobOwnershipKey, CoworkerJobOwnershipStore,
    CoworkerOwnershipAcquireOutcome, CoworkerOwnershipSummary, COWORKER_OWNERSHIP_CHALLENGE_PREFIX,
    COWORKER_OWNERSHIP_INSTANCE_PREFIX, OWNERSHIP_GRANTS_NO_EXECUTION_AUTHORITY,
    OWNERSHIP_REQUIRES_RECEIPT_RECONCILIATION_BEFORE_INFERENCE,
};
use threadlane_native::NativeErrorCode;

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_coworker_alpha";
const BINDING_B: &str = "bnd_coworker_beta";

const HELPER_MODE_ENV: &str = "THREADLANE_COWORKER_OWNERSHIP_TEST_HELPER";
const HELPER_ROOT_ENV: &str = "THREADLANE_COWORKER_OWNERSHIP_TEST_ROOT";
const HELPER_BINDING_ENV: &str = "THREADLANE_COWORKER_OWNERSHIP_TEST_BINDING";
const HELPER_THREAD_ENV: &str = "THREADLANE_COWORKER_OWNERSHIP_TEST_THREAD";
const HELPER_JOB_ENV: &str = "THREADLANE_COWORKER_OWNERSHIP_TEST_JOB";
const HELPER_TEST_NAME: &str =
    "second_os_process_cannot_acquire_same_job_even_with_copied_metadata_while_other_jobs_proceed";
const CHILD_HELPER_TIMEOUT: Duration = Duration::from_secs(10);

struct OwnedTestChild {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout_rx: Receiver<std::io::Result<String>>,
}

impl OwnedTestChild {
    fn spawn_helper(mode: &str, root: &Path, key: &CoworkerJobOwnershipKey) -> Self {
        let exe = env::current_exe().expect("resolve current test binary");
        let mut child = Command::new(exe)
            .args(["--exact", HELPER_TEST_NAME, "--nocapture"])
            .env(HELPER_MODE_ENV, mode)
            .env(HELPER_ROOT_ENV, root.as_os_str())
            .env(HELPER_BINDING_ENV, &key.binding_id)
            .env(HELPER_THREAD_ENV, &key.thread_uuid)
            .env(HELPER_JOB_ENV, &key.job_id)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn owned test child helper");

        let stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");
        let (tx, stdout_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if tx.send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err));
                        break;
                    }
                }
            }
        });

        Self {
            child,
            stdin: Some(stdin),
            stdout_rx,
        }
    }

    fn wait_for_marker(&mut self, marker: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                self.kill_and_reap();
                panic!("timed out waiting for child marker `{marker}`");
            };
            match self.stdout_rx.recv_timeout(remaining) {
                Ok(Ok(line)) => {
                    if let Some(pos) = line.find(marker) {
                        return line[pos + marker.len()..].trim().to_string();
                    }
                }
                Ok(Err(err)) => {
                    self.kill_and_reap();
                    panic!("failed reading child stdout for `{marker}`: {err}");
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.kill_and_reap();
                    panic!("child stdout closed before emitting marker `{marker}`");
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.kill_and_reap();
                    panic!("timed out waiting for child marker `{marker}`");
                }
            }
        }
    }

    fn wait_for_clean_exit(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait().expect("poll child exit status") {
                Some(status) => {
                    assert!(status.success(), "child helper must exit 0, got {status}");
                    return;
                }
                None => {
                    if Instant::now() >= deadline {
                        self.kill_and_reap();
                        panic!("timed out waiting for child helper exit");
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    fn spawn_holder(
        root: &Path,
        key: &CoworkerJobOwnershipKey,
    ) -> (Self, CoworkerOwnershipSummary) {
        let mut handle = Self::spawn_helper("hold_until_stdin_eof", root, key);
        let summary_json = handle.wait_for_marker("ACQUIRED:", CHILD_HELPER_TIMEOUT);
        let summary: CoworkerOwnershipSummary =
            serde_json::from_str(&summary_json).expect("parse child CoworkerOwnershipSummary");
        (handle, summary)
    }

    fn verify_contended_with_copied_summary(
        root: &Path,
        key: &CoworkerJobOwnershipKey,
        copied: &CoworkerOwnershipSummary,
    ) {
        let mut handle = Self::spawn_helper("verify_busy_with_copied_summary", root, key);
        {
            let mut child_stdin = handle.stdin.take().expect("child B stdin");
            let summary_json = serde_json::to_string(copied).expect("serialize copied summary");
            child_stdin
                .write_all(summary_json.as_bytes())
                .expect("write copied summary to child B");
        }
        let _ = handle.wait_for_marker("CONTENDED_BUSY_CONFIRMED", CHILD_HELPER_TIMEOUT);
        handle.wait_for_clean_exit(CHILD_HELPER_TIMEOUT);
    }

    fn release_gracefully_and_wait(&mut self) {
        drop(self.stdin.take());
        let _ = self.wait_for_marker("RELEASED", CHILD_HELPER_TIMEOUT);
        self.wait_for_clean_exit(CHILD_HELPER_TIMEOUT);
    }

    fn kill_and_reap(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stdin.take();
    }
}

impl Drop for OwnedTestChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn read_stdin_bounded(timeout: Duration) -> String {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut input = String::new();
        let res = std::io::stdin().read_to_string(&mut input).map(|_| input);
        let _ = tx.send(res);
    });
    rx.recv_timeout(timeout)
        .expect("child helper stdin read timed out")
        .expect("child helper stdin read failed")
}

fn maybe_run_child_helper() -> bool {
    let Ok(mode) = env::var(HELPER_MODE_ENV) else {
        return false;
    };
    let root = PathBuf::from(env::var(HELPER_ROOT_ENV).expect("helper root env"));
    let key = CoworkerJobOwnershipKey::new(
        env::var(HELPER_BINDING_ENV).expect("helper binding env"),
        env::var(HELPER_THREAD_ENV).expect("helper thread env"),
        env::var(HELPER_JOB_ENV).expect("helper job env"),
    )
    .expect("valid helper key");
    let store = CoworkerJobOwnershipStore::new(&root).expect("open helper store");

    match mode.as_str() {
        "hold_until_stdin_eof" => {
            let guard = store
                .try_acquire_job_option(&key, 1_700_000_000_000)
                .expect("helper try_acquire_job")
                .expect("helper must acquire uncontended job lock");
            let json = serde_json::to_string(guard.summary()).expect("serialize summary");
            println!("ACQUIRED:{json}");
            std::io::stdout().flush().expect("flush stdout");

            let _ = read_stdin_bounded(CHILD_HELPER_TIMEOUT);
            drop(guard);
            println!("RELEASED");
            std::io::stdout()
                .flush()
                .expect("flush stdout after release");
        }
        "verify_busy_with_copied_summary" => {
            let input = read_stdin_bounded(CHILD_HELPER_TIMEOUT);
            let copied: CoworkerOwnershipSummary =
                serde_json::from_str(input.trim()).expect("parse copied summary");
            let direct = store
                .try_acquire_job(&key, 1_700_000_000_050)
                .expect("direct try_acquire_job in child B");
            assert!(direct.is_busy(), "child B direct acquire must be Busy");
            let with_copied = store
                .try_acquire_job_with_copied_summary(&key, &copied, 1_700_000_000_051)
                .expect("try_acquire_job_with_copied_summary in child B");
            assert!(
                with_copied.is_busy(),
                "child B acquire with copied summary/instance_id must still be Busy"
            );
            println!("CONTENDED_BUSY_CONFIRMED");
            std::io::stdout().flush().expect("flush child B stdout");
        }
        other => panic!("unknown helper mode: {other}"),
    }
    true
}

fn create_private_lock_root(tmp: &TempDir, name: &str) -> PathBuf {
    let base = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
    let dir = base.join(name);
    fs::create_dir(&dir).expect("create lock root dir");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).expect("chmod 0700 lock root");
    dir
}

#[test]
fn second_os_process_cannot_acquire_same_job_even_with_copied_metadata_while_other_jobs_proceed() {
    if maybe_run_child_helper() {
        return;
    }

    let tmp = TempDir::new().expect("tempdir");
    let root = create_private_lock_root(&tmp, "job-locks");
    let store = CoworkerJobOwnershipStore::new(&root).expect("open ownership store");

    let key_alpha =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_alpha").expect("key_alpha");
    let key_beta =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_beta").expect("key_beta");
    let key_other_binding =
        CoworkerJobOwnershipKey::new(BINDING_B, THREAD_UUID_B, "job_alpha").expect("key_other");

    let (mut child_a, summary_a) = OwnedTestChild::spawn_holder(&root, &key_alpha);
    assert!(summary_a
        .instance_id
        .starts_with(&format!("{COWORKER_OWNERSHIP_INSTANCE_PREFIX}_")));

    // Parent process sees the lock held by child A and cannot acquire it even with copied metadata.
    match store
        .try_acquire_job(&key_alpha, 1_700_000_000_100)
        .expect("non-blocking try_acquire_job while held")
    {
        CoworkerOwnershipAcquireOutcome::Busy { observed_holder } => {
            assert_eq!(observed_holder.as_ref(), Some(&summary_a));
        }
        CoworkerOwnershipAcquireOutcome::Acquired(_) => {
            panic!("parent must not acquire lock held by child A");
        }
    }
    assert!(store
        .try_acquire_job_option(&key_alpha, 1_700_000_000_101)
        .expect("try_acquire_job_option")
        .is_none());
    assert!(store
        .try_acquire_job_with_copied_summary(&key_alpha, &summary_a, 1_700_000_000_102)
        .expect("try_acquire_job_with_copied_summary")
        .is_busy());

    // A second OS child process presenting child A's copied summary/instance_id also gets Busy.
    OwnedTestChild::verify_contended_with_copied_summary(&root, &key_alpha, &summary_a);

    // Different job in same binding and same job name in different binding proceed independently.
    let guard_beta = store
        .try_acquire_job_option(&key_beta, 1_700_000_000_200)
        .expect("acquire key_beta")
        .expect("key_beta must not be blocked by key_alpha");
    let guard_other_binding = store
        .try_acquire_job_option(&key_other_binding, 1_700_000_000_201)
        .expect("acquire key_other_binding")
        .expect("key_other_binding must not be blocked by key_alpha");
    assert_ne!(guard_beta.instance_id(), summary_a.instance_id);
    assert_ne!(guard_other_binding.instance_id(), summary_a.instance_id);
    assert_ne!(
        guard_beta.lock_file_identity(),
        summary_a.lock_file_identity
    );

    drop(guard_beta);
    drop(guard_other_binding);
    child_a.release_gracefully_and_wait();
}

#[test]
fn graceful_guard_drop_and_owned_child_crash_release_os_lock_with_distinct_identity_and_reject_stale_owner(
) {
    let tmp = TempDir::new().expect("tempdir");
    let root = create_private_lock_root(&tmp, "crash-recovery-locks");
    let store = CoworkerJobOwnershipStore::new(&root).expect("open store");
    let key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_recover").expect("valid key");

    // 1. Graceful drop in child process releases OS lock without unlinking the lockfile.
    let (mut child_1, summary_1) = OwnedTestChild::spawn_holder(&root, &key);
    child_1.release_gracefully_and_wait();

    match store
        .inspect_job_lock(&key)
        .expect("inspect after graceful drop")
    {
        CoworkerJobLockState::UnlockedStaleFile {
            recorded_summary,
            lock_file_identity,
        } => {
            assert_eq!(recorded_summary.as_ref(), Some(&summary_1));
            assert_eq!(lock_file_identity, summary_1.lock_file_identity);
        }
        other => panic!("expected UnlockedStaleFile after graceful drop, got {other:?}"),
    }

    let guard_2 = store
        .try_acquire_job_option(&key, 1_700_000_000_300)
        .expect("reacquire after graceful drop")
        .expect("must acquire after graceful drop");
    assert_eq!(guard_2.lock_file_identity(), summary_1.lock_file_identity);
    assert_ne!(guard_2.instance_id(), summary_1.instance_id);
    assert_ne!(guard_2.challenge_digest(), summary_1.challenge_digest);
    assert_eq!(
        guard_2
            .validate_summary_before_effect(&key, &summary_1)
            .expect_err("stale summary_1 must not authorize guard_2")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    drop(guard_2);

    // 2. Abrupt crash of owned child process releases kernel lock; stale lockfile does not block
    //    restart and never authorizes the crashed owner's summary or instance_id.
    let (mut crashed_child, crashed_summary) = OwnedTestChild::spawn_holder(&root, &key);
    assert_eq!(
        store.inspect_job_lock(&key).expect("inspect while held"),
        CoworkerJobLockState::HeldByActiveProcess {
            recorded_summary: Some(crashed_summary.clone()),
            lock_file_identity: crashed_summary.lock_file_identity,
        }
    );
    crashed_child.kill_and_reap();

    match store
        .inspect_job_lock(&key)
        .expect("inspect after child crash")
    {
        CoworkerJobLockState::UnlockedStaleFile {
            recorded_summary,
            lock_file_identity,
        } => {
            assert_eq!(recorded_summary.as_ref(), Some(&crashed_summary));
            assert_eq!(lock_file_identity, crashed_summary.lock_file_identity);
        }
        other => panic!("expected UnlockedStaleFile after crash, got {other:?}"),
    }

    let guard_after_crash = match store
        .try_acquire_job_with_copied_summary(&key, &crashed_summary, 1_700_000_000_400)
        .expect("reacquire after child crash")
    {
        CoworkerOwnershipAcquireOutcome::Acquired(guard) => guard,
        CoworkerOwnershipAcquireOutcome::Busy { .. } => {
            panic!("stale lockfile from crashed child must not block restart")
        }
    };
    assert_eq!(
        guard_after_crash.lock_file_identity(),
        crashed_summary.lock_file_identity
    );
    assert_ne!(guard_after_crash.instance_id(), crashed_summary.instance_id);
    assert_ne!(
        guard_after_crash.challenge_digest(),
        crashed_summary.challenge_digest
    );

    // Old crashed owner's instance_id and summary are rejected before later effects.
    assert_eq!(
        guard_after_crash
            .validate_claim_before_effect(&key, &crashed_summary.instance_id, None)
            .expect_err("crashed owner instance_id must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        guard_after_crash
            .validate_summary_before_effect(&key, &crashed_summary)
            .expect_err("crashed owner summary must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );

    // Current guard and its issued driver claim validate cleanly.
    let active_claim = guard_after_crash.issue_driver_claim();
    let verified = store
        .verify_live_claim(&guard_after_crash, &key, &active_claim)
        .expect("active claim must validate");
    assert_eq!(verified, guard_after_crash.summary());
}

#[test]
fn wrong_scope_inode_drift_symlink_hardlink_nonprivate_permissions_and_secret_redaction_enforced() {
    assert!(black_box(OWNERSHIP_GRANTS_NO_EXECUTION_AUTHORITY));
    assert!(black_box(
        OWNERSHIP_REQUIRES_RECEIPT_RECONCILIATION_BEFORE_INFERENCE
    ));

    let tmp = TempDir::new().expect("tempdir");
    let root_a = create_private_lock_root(&tmp, "root-a");
    let root_b = create_private_lock_root(&tmp, "root-b");
    let store_a = CoworkerJobOwnershipStore::new(&root_a).expect("store_a");
    let store_b = CoworkerJobOwnershipStore::new(&root_b).expect("store_b");

    let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_1").expect("key");
    let wrong_job_key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_2").expect("wrong_job_key");
    let wrong_binding_key =
        CoworkerJobOwnershipKey::new(BINDING_B, THREAD_UUID_A, "job_1").expect("wrong_binding_key");
    let wrong_thread_key =
        CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_B, "job_1").expect("wrong_thread_key");

    let guard = store_a
        .try_acquire_job_option(&key, 1_700_000_000_500)
        .expect("acquire")
        .expect("guard");
    let claim = guard.issue_driver_claim();

    // Secret redaction in Debug and public summary.
    let guard_debug = format!("{guard:?}");
    let claim_debug = format!("{claim:?}");
    let summary_json = serde_json::to_string(guard.summary()).expect("serialize summary");
    assert!(guard_debug.contains("[REDACTED]"));
    assert!(claim_debug.contains("[REDACTED]"));
    assert!(!guard_debug.contains(&format!("{COWORKER_OWNERSHIP_CHALLENGE_PREFIX}_")));
    assert!(!claim_debug.contains(&format!("{COWORKER_OWNERSHIP_CHALLENGE_PREFIX}_")));
    assert!(!summary_json.contains(&format!("{COWORKER_OWNERSHIP_CHALLENGE_PREFIX}_")));
    assert!(!guard.summary().grants_execution_authority);

    // Wrong scope key or wrong store root rejects.
    for bad_key in [&wrong_job_key, &wrong_binding_key, &wrong_thread_key] {
        assert_eq!(
            guard
                .validate_before_effect(bad_key)
                .expect_err("wrong scope key must fail")
                .error_code(),
            NativeErrorCode::SecurityViolation
        );
        assert_eq!(
            guard
                .validate_driver_claim(bad_key, &claim)
                .expect_err("wrong claim scope must fail")
                .error_code(),
            NativeErrorCode::SecurityViolation
        );
    }
    assert_eq!(
        store_b
            .verify_live_guard(&guard, &key)
            .expect_err("guard from store_a must be rejected by store_b")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        guard
            .verify_challenge(
                "cwc_0000000000000000000000000000000000000000000000000000000000000000"
            )
            .expect_err("wrong challenge must fail")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );

    // Unexpected hardlink on active lockfile is rejected before later effect.
    let hardlink_path = root_a.join("unexpected-hardlink.lock");
    fs::hard_link(guard.lock_path(), &hardlink_path).expect("create hardlink");
    assert_eq!(
        guard
            .validate_before_effect(&key)
            .expect_err("hardlinked lockfile (nlink > 1) must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    fs::remove_file(&hardlink_path).expect("remove hardlink");
    guard
        .validate_before_effect(&key)
        .expect("removing hardlink restores valid single-link state");

    // Non-private lockfile permissions (0644) or root permissions (0755) reject before effect.
    fs::set_permissions(guard.lock_path(), fs::Permissions::from_mode(0o644))
        .expect("weaken lockfile mode to 0644");
    assert_eq!(
        guard
            .validate_before_effect(&key)
            .expect_err("0644 lockfile must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    fs::set_permissions(guard.lock_path(), fs::Permissions::from_mode(0o600))
        .expect("restore lockfile 0600");

    fs::set_permissions(&root_a, fs::Permissions::from_mode(0o755))
        .expect("weaken root_a mode to 0755");
    assert_eq!(
        guard
            .validate_before_effect(&key)
            .expect_err("0755 lock root must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    fs::set_permissions(&root_a, fs::Permissions::from_mode(0o700)).expect("restore root_a 0700");

    // Replacing or unlinking the lockfile inode while guard is held rejects before effect.
    let lock_path = guard.lock_path().to_path_buf();
    fs::remove_file(&lock_path).expect("unlink lockfile while guard held");
    assert_eq!(
        guard
            .validate_before_effect(&key)
            .expect_err("unlinked lockfile must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    fs::write(&lock_path, b"{}").expect("recreate replacement inode at same path");
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    assert_eq!(
        guard
            .validate_before_effect(&key)
            .expect_err("replacement inode at lock_path must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    drop(guard);
    fs::remove_file(&lock_path).expect("cleanup replacement file");

    // Symlink lock root directory and non-private lock root directory are rejected on Store::new.
    let base = fs::canonicalize(tmp.path()).expect("canonicalize");
    let symlink_root = base.join("symlink-root");
    symlink(&root_a, &symlink_root).expect("create symlink root");
    assert_eq!(
        CoworkerJobOwnershipStore::new(&symlink_root)
            .expect_err("symlink lock root must fail")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );

    let insecure_root = base.join("insecure-root");
    fs::create_dir(&insecure_root).expect("create insecure root");
    fs::set_permissions(&insecure_root, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
    assert_eq!(
        CoworkerJobOwnershipStore::new(&insecure_root)
            .expect_err("0755 lock root must fail")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );

    // Pre-existing symlink lockfile and non-private lockfile are rejected on acquire/inspect.
    let target_file = base.join("outside.txt");
    fs::write(&target_file, b"outside").expect("write target");
    symlink(&target_file, &lock_path).expect("symlink lockfile");
    assert_eq!(
        store_a
            .try_acquire_job(&key, 1_700_000_000_600)
            .expect_err("symlink lockfile must be rejected on acquire")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    assert_eq!(
        store_a
            .inspect_job_lock(&key)
            .expect_err("symlink lockfile must be rejected on inspect")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
    fs::remove_file(&lock_path).expect("remove symlink lockfile");

    fs::write(&lock_path, b"").expect("create pre-existing lockfile");
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    assert_eq!(
        store_a
            .try_acquire_job(&key, 1_700_000_000_601)
            .expect_err("pre-existing 0644 lockfile must be rejected")
            .error_code(),
        NativeErrorCode::SecurityViolation
    );
}

#[test]
fn readonly_inspection_and_path_queries_never_create_files_or_mutate_lockfiles() {
    let tmp = TempDir::new().expect("tempdir");
    let root = create_private_lock_root(&tmp, "readonly-locks");
    let store = CoworkerJobOwnershipStore::new(&root).expect("open store");
    let key = CoworkerJobOwnershipKey::new(BINDING_A, THREAD_UUID_A, "job_readonly").expect("key");

    let expected_path = store.lock_path_for_job(&key).expect("lock_path_for_job");
    assert!(!expected_path.exists());
    assert_eq!(
        store.inspect_job_lock(&key).expect("inspect absent lock"),
        CoworkerJobLockState::Absent
    );
    assert!(
        fs::read_dir(&root).expect("read_dir").next().is_none(),
        "inspect_job_lock and lock_path_for_job must never create lockfiles"
    );

    let guard = store
        .try_acquire_job_option(&key, 1_700_000_000_700)
        .expect("acquire")
        .expect("guard");
    let expected_summary = guard.summary().clone();
    drop(guard);

    let before_bytes = fs::read(&expected_path).expect("read stale lockfile bytes before inspect");
    let state = store
        .inspect_job_lock(&key)
        .expect("inspect stale lockfile");
    let after_bytes = fs::read(&expected_path).expect("read stale lockfile bytes after inspect");
    assert_eq!(before_bytes, after_bytes);
    assert_eq!(
        state,
        CoworkerJobLockState::UnlockedStaleFile {
            recorded_summary: Some(expected_summary.clone()),
            lock_file_identity: expected_summary.lock_file_identity,
        }
    );
}
