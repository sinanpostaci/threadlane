use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use threadlane_core::observation::ReadonlySqliteError;
use threadlane_core::{
    Command, Core, CoreError, WorkerAcceptanceState, WorkerCompletionRecord, WorkerOutcomeSummary,
    WorkerSnapshot, WorkerStatus, WorkerWorktreeUpdate,
};

const UUID_A: &str = "11111111-1111-4111-8111-111111111111";
const UUID_B: &str = "22222222-2222-4222-8222-222222222222";
const PROFILE_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

fn set_private_dir(path: &Path) {
    fs::create_dir_all(path).expect("create private fixture directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("chmod private fixture directory 0700");
    }
}

fn private_tempdir() -> TempDir {
    let dir = TempDir::new().expect("tempdir");
    set_private_dir(dir.path());
    dir
}

fn set_private_file(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .expect("chmod private fixture file 0600");
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn private_file(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write private fixture file");
    set_private_file(path);
}

fn dir_entry_names(path: &Path) -> Vec<String> {
    let mut names = fs::read_dir(path)
        .expect("read fixture directory")
        .map(|entry| {
            entry
                .expect("read fixture directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn sample_worktree_update(run_id: &str, worker_id: &str) -> WorkerWorktreeUpdate {
    WorkerWorktreeUpdate {
        run_worktree_id: format!("wt-run-r_fixture-{run_id}"),
        run_worktree_path: format!("/tmp/work-area/checkouts/runs/wt-run-r_fixture-{run_id}"),
        run_branch_name: format!("threadlane/run/r_fixture/{run_id}"),
        task_worktree_id: format!("wt-task-r_fixture-w_fixture-{worker_id}"),
        task_worktree_path: format!(
            "/tmp/work-area/checkouts/tasks/wt-task-r_fixture-w_fixture-{worker_id}"
        ),
        task_branch_name: format!("threadlane/task/r_fixture/w_fixture/{worker_id}"),
        base_commit: BASE_COMMIT.to_string(),
    }
}

fn sample_ready_outcome() -> WorkerOutcomeSummary {
    WorkerOutcomeSummary {
        provider_retry_high_water: None,
        provider_health: None,
        exit_code: Some(0),
        signal: None,
        duration_ms: Some(420),
        timed_out: false,
        cancelled: false,
        cancel_requested_before_completion: false,
        missing_model_evidence: false,
        missing_effort_evidence: false,
        provider_error: false,
        incomplete_cleanup: false,
        rejected_by_diff_gate: false,
        stdout_truncated: false,
        stderr_truncated: false,
        acceptance_state: WorkerAcceptanceState::ReadyForReview,
        changed_paths_count: 1,
        changed_paths_preview: vec!["src/lib.rs".to_string()],
        violation_count: 0,
        violation_summaries: Vec::new(),
        final_summary: Some("Completed bounded fixture work.".to_string()),
        failure_reason: None,
        report_artifact_id: None,
    }
}

fn seed_completed_worker(core: &mut Core, binding_id: &str, command_key: &str) -> WorkerSnapshot {
    let queued = core
        .execute(
            binding_id,
            Command::StartWorker {
                command_key: command_key.to_string(),
                profile_id: "profile-readonly".to_string(),
                profile_digest: PROFILE_DIGEST.to_string(),
                provider: "antigravity".to_string(),
                requested_model: "gemini-4-argon-high".to_string(),
                requested_effort: "high".to_string(),
                base_commit: BASE_COMMIT.to_string(),
                execution_binding: None,
                instruction: "Produce a durable read-only result.".to_string(),
                now_ms: 1_700_000_000_000,
            },
        )
        .expect("queue worker")
        .into_worker()
        .expect("worker snapshot");

    let claim = core
        .claim_worker(
            binding_id,
            &queued.run_id,
            1,
            PROFILE_DIGEST,
            1_700_000_000_010,
        )
        .expect("claim worker");
    core.record_worker_running(
        &claim,
        sample_worktree_update(&queued.run_id, &queued.worker_id),
        1_700_000_000_020,
    )
    .expect("record worker running");
    let completed = core
        .record_worker_outcome(
            &claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: Some("fedcba9876543210fedcba9876543210fedcba98".to_string()),
                dropped_live_events: 0,
                outcome: sample_ready_outcome(),
            },
            1_700_000_000_030,
        )
        .expect("record completed outcome");
    assert_eq!(completed.status, WorkerStatus::Completed);
    completed
}

struct SeededStoppedDb {
    db_dir: PathBuf,
    db_path: PathBuf,
    binding_id: String,
    worker: WorkerSnapshot,
}

fn create_stopped_db(
    root: &Path,
    name: &str,
    thread_uuid: &str,
    command_key: &str,
) -> SeededStoppedDb {
    let db_dir = root.join(name);
    set_private_dir(&db_dir);
    let db_path = db_dir.join("state.db");

    let mut core = Core::open(&db_path).expect("open Core");
    let binding = core
        .create_binding(thread_uuid, name)
        .expect("create binding");
    let worker = seed_completed_worker(&mut core, &binding.binding_id, command_key);
    drop(core);

    assert!(!db_dir.join("state.db-wal").exists());
    assert!(!db_dir.join("state.db-shm").exists());
    set_private_file(&db_path);

    SeededStoppedDb {
        db_dir,
        db_path,
        binding_id: binding.binding_id,
        worker,
    }
}

fn expect_readonly_core_error(result: Result<WorkerSnapshot, ReadonlySqliteError>) -> CoreError {
    match result {
        Err(ReadonlySqliteError::Core(err)) => err,
        other => panic!("expected read-only Core error, got {other:?}"),
    }
}

#[test]
fn readonly_worker_snapshot_is_exact_scoped_and_survives_closed_db_without_sidecars() {
    let root = private_tempdir();
    let worker_a = create_stopped_db(root.path(), "db-a", UUID_A, "worker-a");
    let worker_b = create_stopped_db(root.path(), "db-b", UUID_B, "worker-b");
    assert_ne!(worker_a.binding_id, worker_b.binding_id);

    let dir_names_before = dir_entry_names(&worker_a.db_dir);
    let db_before = fs::read(&worker_a.db_path).expect("read closed fixture DB");

    let by_run = Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_A,
        &worker_a.worker.run_id,
    )
    .expect("read worker A by exact run from closed DB");
    assert_eq!(by_run, worker_a.worker);
    assert_eq!(by_run.status, WorkerStatus::Completed);
    assert_eq!(
        by_run
            .outcome
            .as_ref()
            .expect("completed outcome")
            .acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );

    let by_worker = Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_A,
        &worker_a.worker.worker_id,
    )
    .expect("read worker A by exact worker id from closed DB");
    assert_eq!(by_worker, worker_a.worker);
    assert_ne!(
        by_worker, worker_b.worker,
        "a separate stopped DB must not select another session's worker"
    );

    let other_db = Core::get_worker_readonly(
        &worker_b.db_path,
        &worker_b.binding_id,
        UUID_B,
        &worker_b.worker.run_id,
    )
    .expect("read worker B from its own separate stopped DB");
    assert_eq!(other_db, worker_b.worker);

    // Wrong binding/thread and malformed scopes fail before an arbitrary worker can be returned.
    let wrong_thread = expect_readonly_core_error(Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_B,
        &worker_a.worker.run_id,
    ));
    assert!(matches!(
        wrong_thread,
        CoreError::Validation {
            field: "thread_uuid",
            ..
        }
    ));
    let other_binding = expect_readonly_core_error(Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_b.binding_id,
        UUID_A,
        &worker_a.worker.run_id,
    ));
    assert!(matches!(other_binding, CoreError::BindingNotFound { .. }));
    let missing_worker = expect_readonly_core_error(Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_A,
        "run_00000000000000000000000000000001",
    ));
    assert!(matches!(missing_worker, CoreError::WorkerNotFound { .. }));
    let malformed_thread = expect_readonly_core_error(Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        "not-a-thread-uuid",
        &worker_a.worker.run_id,
    ));
    assert!(matches!(
        malformed_thread,
        CoreError::Validation {
            field: "thread_uuid",
            ..
        }
    ));

    assert_eq!(dir_entry_names(&worker_a.db_dir), dir_names_before);
    assert_eq!(
        fs::read(&worker_a.db_path).expect("read DB after readonly"),
        db_before
    );
    assert!(!worker_a.db_dir.join("state.db-wal").exists());
    assert!(!worker_a.db_dir.join("state.db-shm").exists());

    // Residual WAL frames cannot be ignored or silently recovered by the stopped read.
    let wal_path = worker_a.db_dir.join("state.db-wal");
    let shm_path = worker_a.db_dir.join("state.db-shm");
    private_file(&wal_path, b"");
    let residual_wal = Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_A,
        &worker_a.worker.run_id,
    )
    .expect_err("residual WAL must refuse the frozen snapshot");
    assert!(matches!(
        residual_wal,
        ReadonlySqliteError::SnapshotUnavailable(_)
    ));
    assert!(!shm_path.exists(), "must not create a missing SHM sidecar");
    assert_eq!(
        fs::read(&worker_a.db_path).expect("read DB after WAL rejection"),
        db_before
    );
    fs::remove_file(&wal_path).expect("remove residual WAL fixture");

    let recovered = Core::get_worker_readonly(
        &worker_a.db_path,
        &worker_a.binding_id,
        UUID_A,
        &worker_a.worker.run_id,
    )
    .expect("clean checkpointed history is readable again");
    assert_eq!(recovered, worker_a.worker);
    assert_eq!(dir_entry_names(&worker_a.db_dir), dir_names_before);
    assert_eq!(
        fs::read(&worker_a.db_path).expect("read DB after recovery"),
        db_before
    );
    assert!(!wal_path.exists() && !shm_path.exists());
}

#[test]
fn readonly_worker_snapshot_rejects_multi_binding_session_db_without_writes() {
    let root = private_tempdir();
    let db_dir = root.path().join("db-multi");
    set_private_dir(&db_dir);
    let db_path = db_dir.join("state.db");

    let mut core = Core::open(&db_path).expect("open Core");
    let binding_a = core
        .create_binding(UUID_A, "multi binding A")
        .expect("create binding A");
    let _binding_b = core
        .create_binding(UUID_B, "multi binding B")
        .expect("create binding B");
    let worker = seed_completed_worker(&mut core, &binding_a.binding_id, "multi-worker");
    drop(core);
    set_private_file(&db_path);

    let dir_names_before = dir_entry_names(&db_dir);
    let db_before = fs::read(&db_path).expect("read multi-binding fixture DB");

    let err = expect_readonly_core_error(Core::get_worker_readonly(
        &db_path,
        &binding_a.binding_id,
        UUID_A,
        &worker.run_id,
    ));
    match err {
        CoreError::CorruptState(reason) => assert!(
            reason.contains("expected 1 binding"),
            "unexpected multi-binding rejection reason: {reason}"
        ),
        other => panic!("expected CorruptState for multi-binding session DB, got {other:?}"),
    }

    assert_eq!(dir_entry_names(&db_dir), dir_names_before);
    assert_eq!(
        fs::read(&db_path).expect("read DB after multi-binding rejection"),
        db_before
    );
    assert!(!db_dir.join("state.db-wal").exists());
    assert!(!db_dir.join("state.db-shm").exists());
}
