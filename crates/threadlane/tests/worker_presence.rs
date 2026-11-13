#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::Duration;

use threadlane::client::{ScopedClient, SupervisorClient};
use threadlane::error::ErrorCode;
use threadlane::protocol::{
    HostControlConfig, WorkerPresenceAvailability, WorkerPresenceFreshness,
    WorkerPresenceUnavailableReason, WorkerSnapshot, WorkerStatus,
};
use threadlane::supervisor::{Supervisor, SupervisorConfig};
use threadlane::workers::{WorkerProfileConfig, WorkerSpecTemplate, WorkersConfig};
use threadlane_core::Core;
use threadlane_workers::{
    ExecutionBudgets, OwnedProcessState, WorkerFilePermissions, WorkerProvider,
};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const APPROVAL_ID: &str = "apr_11111111111111111111111111111111";
const SUBJECT_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const INSTANCE_ID: &str = "hst_55555555555555555555555555555555";

struct GitRepoFixture {
    repo_dir: PathBuf,
    base_commit: String,
}

fn init_git_repo(root: &Path) -> GitRepoFixture {
    let repo_dir = root.join("source_repo");
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src dir");
    fs::write(
        repo_dir.join("src/lib.rs"),
        "pub fn value() -> u32 {\n    1\n}\n",
    )
    .expect("write initial src/lib.rs");
    fs::write(repo_dir.join("README.md"), "# Presence Test Repo\n").expect("write README.md");

    run_git(&repo_dir, &["init"]);
    run_git(&repo_dir, &["config", "user.email", "test@example.com"]);
    run_git(&repo_dir, &["config", "user.name", "Threadlane Test"]);
    run_git(&repo_dir, &["add", "."]);
    run_git(&repo_dir, &["commit", "-m", "Initial commit"]);

    let output = StdCommand::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&repo_dir)
        .output()
        .expect("git rev-parse HEAD");
    assert!(output.status.success(), "git rev-parse HEAD failed");
    let base_commit = String::from_utf8(output.stdout)
        .expect("utf8 commit")
        .trim()
        .to_string();
    assert_eq!(base_commit.len(), 40);

    GitRepoFixture {
        repo_dir,
        base_commit,
    }
}

fn run_git(repo_dir: &Path, args: &[&str]) {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_executable_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
}

fn default_test_budgets(timeout_ms: u64) -> ExecutionBudgets {
    ExecutionBudgets {
        progress_watchdog: None,
        timeout_ms,
        max_line_bytes: 32 * 1024,
        max_stdout_bytes: 256 * 1024,
        max_stderr_bytes: 64 * 1024,
        max_events: 256,
    }
}

fn make_profile(
    profile_id: &str,
    binding_id: &str,
    executable: &Path,
    repo: &GitRepoFixture,
    work_area_root: &Path,
) -> WorkerProfileConfig {
    WorkerProfileConfig {
        profile_id: profile_id.to_string(),
        allowed_binding_ids: BTreeSet::from([binding_id.to_string()]),
        spec: WorkerSpecTemplate {
            provider: WorkerProvider::Antigravity,
            executable: executable.to_path_buf(),
            model: "gemini-4-argon-high".to_string(),
            effort: Some("high".to_string()),
            allowed_models: BTreeSet::from(["gemini-4-argon-high".to_string()]),
            standalone: false,
            session_id: None,
            repository_path: repo.repo_dir.clone(),
            base_commit: repo.base_commit.clone(),
            work_area_root: work_area_root.to_path_buf(),
            allowed_write_paths: BTreeSet::from(["src".to_string()]),
            file_permissions: WorkerFilePermissions::Inherit,
            budgets: default_test_budgets(10_000),
        },
        dependency_input: None,
    }
}

fn seed_binding(state_dir: &Path, thread_uuid: &str, label: &str) -> threadlane_core::Binding {
    let db_path = threadlane::descriptor::prepare_database_path(state_dir).expect("prepare db");
    let mut core = Core::open(&db_path).expect("open core");
    let binding = core
        .create_binding(thread_uuid, label)
        .expect("create binding");
    threadlane::descriptor::enforce_database_permissions(state_dir).expect("enforce db perms");
    binding
}

fn blocking_provider_script(
    marker: &Path,
    allow_exit: &Path,
    spawn_count: &Path,
    init_payload: &str,
) -> String {
    let result_payload = r#"{"type":"result","subtype":"success","status":"completed","summary":"Presence fixture completed"}"#;
    format!(
        r#"#!/bin/sh
echo spawned >> "{count}"
touch "{marker}"
printf '%s\n' '{init}'
while [ ! -f "{allow}" ]; do
  sleep 0.02
done
printf '%s\n' '{result}'
exit 0
"#,
        count = spawn_count.display(),
        marker = marker.display(),
        init = init_payload,
        allow = allow_exit.display(),
        result = result_payload,
    )
}

fn init_payload(session_id: &str) -> String {
    format!(
        r#"{{"type":"system","subtype":"init","session_id":"{session_id}","model":"gemini-4-argon-high","effort":"high"}}"#
    )
}

async fn wait_for_presence_live(
    scoped: &ScopedClient,
    run_id: &str,
) -> threadlane::protocol::WorkerPresenceSnapshot {
    for _ in 0..400 {
        let presence = scoped
            .get_worker_presence(run_id.to_string())
            .await
            .expect("get_worker_presence poll");
        if presence.live {
            return presence;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let last = scoped
        .get_worker_presence(run_id.to_string())
        .await
        .expect("final get_worker_presence");
    panic!("timed out waiting for live presence; last snapshot: {last:#?}");
}

async fn wait_for_terminal(scoped: &ScopedClient, run_id: &str) -> WorkerSnapshot {
    for _ in 0..400 {
        let snapshot = scoped
            .get_worker(run_id.to_string())
            .await
            .expect("get_worker poll");
        if snapshot.status.is_terminal() {
            return snapshot;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let last = scoped
        .get_worker(run_id.to_string())
        .await
        .expect("final get_worker");
    panic!("timed out waiting for terminal worker; last snapshot: {last:#?}");
}

fn sample_host_control(binding_id: &str) -> HostControlConfig {
    HostControlConfig {
        approval_id: APPROVAL_ID.to_string(),
        subject_digest: SUBJECT_DIGEST.to_string(),
        control_binding_id: binding_id.to_string(),
        thread_uuid: THREAD_UUID_A.to_string(),
        session_binding_id: binding_id.to_string(),
        instance_id: INSTANCE_ID.to_string(),
    }
}

#[tokio::test]
async fn queued_work_is_not_live_and_an_actual_silent_child_is_fresh_running() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let work_area = temp.path().join("work_area");
    let repo = init_git_repo(temp.path());
    let binding = seed_binding(&state_dir, THREAD_UUID_A, "Presence Queue Chat");

    let allow_a = temp.path().join("allow_a.flag");
    let allow_b = temp.path().join("allow_b.flag");
    let marker_a = temp.path().join("spawned_a.flag");
    let marker_b = temp.path().join("spawned_b.flag");
    let count_a = temp.path().join("spawn_count_a.txt");
    let count_b = temp.path().join("spawn_count_b.txt");
    let script_a = temp.path().join("provider_a.sh");
    let script_b = temp.path().join("provider_b.sh");
    write_executable_script(
        &script_a,
        &blocking_provider_script(&marker_a, &allow_a, &count_a, &init_payload("sess-a")),
    );
    write_executable_script(
        &script_b,
        &blocking_provider_script(&marker_b, &allow_b, &count_b, &init_payload("sess-b")),
    );

    let workers_cfg = WorkersConfig {
        max_parallel: 1,
        profiles: vec![
            make_profile(
                "prof_presence_a",
                &binding.binding_id,
                &script_a,
                &repo,
                &work_area,
            ),
            make_profile(
                "prof_presence_b",
                &binding.binding_id,
                &script_b,
                &repo,
                &work_area,
            ),
        ],
    };
    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(workers_cfg),
    })
    .await
    .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped = client
        .scoped(&binding.binding_id)
        .expect("scoped presence client");

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let first = scoped
            .start_worker(
                "presence-sequential-a".to_string(),
                "prof_presence_a".to_string(),
                "Run the first presence fixture".to_string(),
            )
            .await
            .expect("start first worker");
        let first_presence = wait_for_presence_live(&scoped, &first.run_id).await;
        // `start_worker` may return while the worker is still queued without a claim. Presence
        // must match the current durable snapshot rather than that earlier admission response.
        let first_current = scoped
            .get_worker(first.run_id.clone())
            .await
            .expect("current durable first worker");
        assert_eq!(
            first_presence.availability,
            WorkerPresenceAvailability::Live
        );
        assert_eq!(first_presence.freshness, WorkerPresenceFreshness::Fresh);
        assert_eq!(first_presence.binding_id, first_current.binding_id);
        assert_eq!(first_presence.run_id, first_current.run_id);
        assert_eq!(first_presence.worker_id, first_current.worker_id);
        assert_eq!(first_presence.claim_id, first_current.claim_id);
        assert_eq!(first_presence.attempt_count, first_current.attempt_count);
        assert_eq!(first_presence.profile_id, first_current.profile_id);
        assert_eq!(first_presence.profile_digest, first_current.profile_digest);
        assert!(first_current.claim_id.is_some());
        assert_eq!(first_presence.worker_status, WorkerStatus::Running);
        let first_process = first_presence
            .process
            .as_ref()
            .expect("validated process observation");
        assert!(first_process.state == OwnedProcessState::Running);
        assert!(first_process.child_pid.is_some());
        assert!(first_process.process_group_id.is_some());
        assert!(first_process.linux_process_start_ticks.is_some());
        assert!(first_presence.sample_age_ms.is_some());
        assert!(first_presence.expires_at_ms.is_some());

        // The second worker is admitted but still queued behind the first claim. No process
        // observation exists yet, so presence must not become live merely from durable state.
        let second = scoped
            .start_worker(
                "presence-sequential-b".to_string(),
                "prof_presence_b".to_string(),
                "Run the second presence fixture".to_string(),
            )
            .await
            .expect("start second worker");
        let queued = scoped
            .get_worker_presence(second.run_id.clone())
            .await
            .expect("queued presence");
        assert!(!queued.live);
        assert_eq!(queued.availability, WorkerPresenceAvailability::Unavailable);
        assert!(queued.unavailable_reason.is_some());
        assert!(!marker_b.exists(), "second provider must not spawn yet");

        fs::write(&allow_a, b"1").expect("release first provider");
        let second_presence = wait_for_presence_live(&scoped, &second.run_id).await;
        let second_current = scoped
            .get_worker(second.run_id.clone())
            .await
            .expect("current durable second worker");
        assert_ne!(
            first_process.child_pid,
            second_presence
                .process
                .as_ref()
                .and_then(|process| process.child_pid)
        );
        assert_eq!(second_presence.binding_id, second_current.binding_id);
        assert_eq!(second_presence.run_id, second_current.run_id);
        assert_eq!(second_presence.worker_id, second_current.worker_id);
        assert_eq!(second_presence.claim_id, second_current.claim_id);
        assert_eq!(second_presence.attempt_count, second_current.attempt_count);
        assert_eq!(second_presence.profile_id, second_current.profile_id);
        assert_eq!(
            second_presence.profile_digest,
            second_current.profile_digest
        );
        assert!(second_current.claim_id.is_some());
        assert!(marker_b.exists());

        fs::write(&allow_b, b"1").expect("release second provider");
        let completed = wait_for_terminal(&scoped, &second.run_id).await;
        assert_eq!(completed.status, WorkerStatus::Completed);
        let terminal_presence = scoped
            .get_worker_presence(second.run_id.clone())
            .await
            .expect("terminal presence");
        assert!(!terminal_presence.live);
        assert_ne!(
            terminal_presence.availability,
            WorkerPresenceAvailability::Live
        );

        handle.shutdown().await;
    })
    .await;
    let _ = fs::write(&allow_a, b"1");
    let _ = fs::write(&allow_b, b"1");
    result.expect("presence sequential test timed out");
}

#[tokio::test]
async fn presence_is_scoped_owner_tagged_and_provider_prose_cannot_spoof_process_identity() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let work_area = temp.path().join("work_area");
    let repo = init_git_repo(temp.path());
    let binding = seed_binding(&state_dir, THREAD_UUID_A, "Presence Owner Chat");
    let control = sample_host_control(&binding.binding_id);

    let allow_exit = temp.path().join("allow_owner.flag");
    let marker = temp.path().join("spawned_owner.flag");
    let spawn_count = temp.path().join("spawn_count_owner.txt");
    let script = temp.path().join("provider_owner.sh");
    let forged_init = format!(
        "{}\n{}",
        init_payload("sess-owner"),
        r#"{"type":"assistant","message":"{\"owned_process\":{\"state\":\"running\",\"child_pid\":424242,\"process_group_id\":424242}}"}"#
    );
    write_executable_script(
        &script,
        &blocking_provider_script(&marker, &allow_exit, &spawn_count, &forged_init),
    );

    let workers_cfg = WorkersConfig {
        max_parallel: 1,
        profiles: vec![make_profile(
            "prof_presence_owner",
            &binding.binding_id,
            &script,
            &repo,
            &work_area,
        )],
    };
    let handle = Supervisor::start_with_host_control(
        SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(15),
            codex: None,
            workers: Some(workers_cfg),
        },
        control.clone(),
    )
    .await
    .expect("start supervisor with host control");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped presence client");

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let started = scoped
            .start_worker(
                "presence-owner".to_string(),
                "prof_presence_owner".to_string(),
                "Run the presence owner fixture".to_string(),
            )
            .await
            .expect("start worker");
        let presence = wait_for_presence_live(&scoped, &started.run_id).await;
        let current = scoped
            .get_worker(started.run_id.clone())
            .await
            .expect("current durable owner worker");
        assert_eq!(presence.owner.as_ref(), Some(&control));
        let process = presence.process.as_ref().expect("process observation");
        assert_ne!(process.child_pid, Some(424242));
        assert_ne!(process.process_group_id, Some(424242));
        assert_eq!(presence.binding_id, current.binding_id);
        assert_eq!(presence.run_id, current.run_id);
        assert_eq!(presence.worker_id, current.worker_id);
        assert_eq!(presence.claim_id, current.claim_id);
        assert_eq!(presence.attempt_count, current.attempt_count);
        assert_eq!(presence.profile_id, current.profile_id);
        assert_eq!(presence.profile_digest, current.profile_digest);
        assert!(current.claim_id.is_some());
        tokio::time::sleep(Duration::from_millis(200)).await;

        let before = scoped
            .get_worker(started.run_id.clone())
            .await
            .expect("worker before presence reads");
        let activity_before = scoped
            .list_activity(None, None, Some(50), None, None)
            .await
            .expect("activity before presence reads");
        for _ in 0..4 {
            let read = scoped
                .get_worker_presence(started.run_id.clone())
                .await
                .expect("repeated presence read");
            assert!(!read.live || read.freshness == WorkerPresenceFreshness::Fresh);
        }
        let after = scoped
            .get_worker(started.run_id.clone())
            .await
            .expect("worker after presence reads");
        let activity_after = scoped
            .list_activity(None, None, Some(50), None, None)
            .await
            .expect("activity after presence reads");
        assert_eq!(before.revision, after.revision);
        assert_eq!(before.status, after.status);
        assert_eq!(before.claim_id, after.claim_id);
        assert_eq!(
            activity_before.high_water_mark_seq,
            activity_after.high_water_mark_seq
        );
        assert_eq!(activity_before.events.len(), activity_after.events.len());
        let spawns = fs::read_to_string(&spawn_count).expect("spawn count");
        assert_eq!(spawns.lines().count(), 1);
        assert!(marker.exists());

        let other_binding = client
            .create_binding(THREAD_UUID_B, "Other Presence Chat")
            .await
            .expect("create other binding");
        let other_scoped = client
            .scoped(&other_binding.binding_id)
            .expect("other scoped client");
        let foreign = other_scoped
            .get_worker_presence(started.run_id.clone())
            .await
            .expect_err("other binding must not read this presence");
        assert_eq!(foreign.to_api_error().code, ErrorCode::WorkerNotFound);

        let missing = scoped
            .get_worker_presence("run_00000000-0000-4000-8000-000000000000".to_string())
            .await
            .expect_err("missing run must return a normal scoped error");
        assert_eq!(missing.to_api_error().code, ErrorCode::WorkerNotFound);

        fs::write(&allow_exit, b"1").expect("release owner provider");
        let completed = wait_for_terminal(&scoped, &started.run_id).await;
        assert_eq!(completed.status, WorkerStatus::Completed);
        let terminal = scoped
            .get_worker_presence(started.run_id.clone())
            .await
            .expect("terminal presence");
        assert!(!terminal.live);
        assert_ne!(terminal.availability, WorkerPresenceAvailability::Live);

        handle.shutdown().await;
    })
    .await;
    let _ = fs::write(&allow_exit, b"1");
    result.expect("presence owner test timed out");
}

#[tokio::test]
async fn parallel_claims_have_independent_process_presence() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let work_area = temp.path().join("work_area");
    let repo = init_git_repo(temp.path());
    let binding = seed_binding(&state_dir, THREAD_UUID_A, "Presence Parallel Chat");

    let allow_a = temp.path().join("allow_parallel_a.flag");
    let allow_b = temp.path().join("allow_parallel_b.flag");
    let marker_a = temp.path().join("spawned_parallel_a.flag");
    let marker_b = temp.path().join("spawned_parallel_b.flag");
    let count_a = temp.path().join("spawn_count_parallel_a.txt");
    let count_b = temp.path().join("spawn_count_parallel_b.txt");
    let script_a = temp.path().join("provider_parallel_a.sh");
    let script_b = temp.path().join("provider_parallel_b.sh");
    write_executable_script(
        &script_a,
        &blocking_provider_script(
            &marker_a,
            &allow_a,
            &count_a,
            &init_payload("sess-parallel-a"),
        ),
    );
    write_executable_script(
        &script_b,
        &blocking_provider_script(
            &marker_b,
            &allow_b,
            &count_b,
            &init_payload("sess-parallel-b"),
        ),
    );

    let workers_cfg = WorkersConfig {
        max_parallel: 2,
        profiles: vec![
            make_profile(
                "prof_parallel_a",
                &binding.binding_id,
                &script_a,
                &repo,
                &work_area,
            ),
            make_profile(
                "prof_parallel_b",
                &binding.binding_id,
                &script_b,
                &repo,
                &work_area,
            ),
        ],
    };
    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(workers_cfg),
    })
    .await
    .expect("start supervisor");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped = client
        .scoped(&binding.binding_id)
        .expect("scoped presence client");

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let first = scoped
            .start_worker(
                "presence-parallel-a".to_string(),
                "prof_parallel_a".to_string(),
                "Run parallel presence A".to_string(),
            )
            .await
            .expect("start first parallel worker");
        let second = scoped
            .start_worker(
                "presence-parallel-b".to_string(),
                "prof_parallel_b".to_string(),
                "Run parallel presence B".to_string(),
            )
            .await
            .expect("start second parallel worker");

        let first_presence = wait_for_presence_live(&scoped, &first.run_id).await;
        let second_presence = wait_for_presence_live(&scoped, &second.run_id).await;
        assert_ne!(first_presence.claim_id, second_presence.claim_id);
        let first_pid = first_presence
            .process
            .as_ref()
            .and_then(|process| process.child_pid);
        let second_pid = second_presence
            .process
            .as_ref()
            .and_then(|process| process.child_pid);
        assert!(first_pid.is_some());
        assert!(second_pid.is_some());
        assert_ne!(first_pid, second_pid);

        fs::write(&allow_a, b"1").expect("release parallel A");
        fs::write(&allow_b, b"1").expect("release parallel B");
        let _ = wait_for_terminal(&scoped, &first.run_id).await;
        let _ = wait_for_terminal(&scoped, &second.run_id).await;

        assert!(
            !scoped
                .get_worker_presence(first.run_id.clone())
                .await
                .expect("first terminal presence")
                .live
        );
        assert!(
            !scoped
                .get_worker_presence(second.run_id.clone())
                .await
                .expect("second terminal presence")
                .live
        );

        handle.shutdown().await;
    })
    .await;
    let _ = fs::write(&allow_a, b"1");
    let _ = fs::write(&allow_b, b"1");
    result.expect("presence parallel test timed out");
}

#[tokio::test]
async fn restart_and_shutdown_never_revive_persisted_unknown_presence() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let work_area = temp.path().join("work_area");
    let repo = init_git_repo(temp.path());
    let binding = seed_binding(&state_dir, THREAD_UUID_A, "Presence Restart Chat");

    let allow_exit = temp.path().join("allow_restart.flag");
    let marker = temp.path().join("spawned_restart.flag");
    let spawn_count = temp.path().join("spawn_count_restart.txt");
    let script = temp.path().join("provider_restart.sh");
    write_executable_script(
        &script,
        &blocking_provider_script(
            &marker,
            &allow_exit,
            &spawn_count,
            &init_payload("sess-restart"),
        ),
    );
    let workers_cfg = WorkersConfig {
        max_parallel: 1,
        profiles: vec![make_profile(
            "prof_presence_restart",
            &binding.binding_id,
            &script,
            &repo,
            &work_area,
        )],
    };
    let config = SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(workers_cfg),
    };
    let handle = Supervisor::start(config.clone())
        .await
        .expect("start supervisor for restart");
    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped = client
        .scoped(&binding.binding_id)
        .expect("scoped presence client");

    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let started = scoped
            .start_worker(
                "presence-restart".to_string(),
                "prof_presence_restart".to_string(),
                "Run the restart presence fixture".to_string(),
            )
            .await
            .expect("start worker");
        let live = wait_for_presence_live(&scoped, &started.run_id).await;
        assert!(live.live);

        handle.shutdown().await;

        let restarted = Supervisor::start(config)
            .await
            .expect("restart supervisor on the same state");
        let restarted_client =
            SupervisorClient::from_state_dir(&state_dir).expect("restarted client");
        let restarted_scoped = restarted_client
            .scoped(&binding.binding_id)
            .expect("restarted scoped client");
        let presence = restarted_scoped
            .get_worker_presence(started.run_id.clone())
            .await
            .expect("presence after restart");
        assert!(!presence.live);
        assert_eq!(presence.worker_status, WorkerStatus::Unknown);
        assert_eq!(
            presence.availability,
            WorkerPresenceAvailability::Unavailable
        );
        assert_eq!(
            presence.unavailable_reason,
            Some(WorkerPresenceUnavailableReason::PersistedUnknown)
        );
        assert!(presence.process.is_none());

        // A stopped-session read remains a normal scoped error for a missing or foreign run.
        let missing = restarted_scoped
            .get_worker_presence("run_00000000-0000-4000-8000-000000000000".to_string())
            .await
            .expect_err("missing presence after restart");
        assert_eq!(missing.to_api_error().code, ErrorCode::WorkerNotFound);

        restarted.shutdown().await;

        // A restart must not revive the old durable worker, and the original process marker
        // proves the fixture was real rather than a fabricated observation.
        assert!(marker.exists());
        let spawns = fs::read_to_string(&spawn_count).expect("spawn count");
        assert_eq!(spawns.lines().count(), 1);
    })
    .await;
    let _ = fs::write(&allow_exit, b"1");
    result.expect("presence restart test timed out");
}
