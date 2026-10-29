#![cfg(unix)]

use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use threadlane::client::{ScopedClient, SupervisorClient};
use threadlane::codex::CodexWakeupConfig;
use threadlane::error::ErrorCode;
use threadlane::protocol::{ScopedRequest, WorkerAcceptanceState, WorkerSnapshot, WorkerStatus};
use threadlane::supervisor::{Supervisor, SupervisorConfig};
use threadlane::workers::{
    WorkerProfileConfig, WorkerSpecTemplate, WorkersConfig, MAX_WORKERS_CONFIG_FILE_BYTES,
};
use threadlane_core::{
    estimate_wire_response_bytes, Core, CoreError, EventKind, WakeupStatus, WorkerCompletionRecord,
    WorkerOutcomeSummary, WorkerProgressUpdate, WorkerWorktreeUpdate, FIXED_WAKEUP_MESSAGE,
    MAX_COMPACT_RESPONSE_BYTES, MAX_PAGE_RESPONSE_BYTES, MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT,
};
use threadlane_workers::{ExecutionBudgets, WorkerFilePermissions, WorkerProvider};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

const THREAD_UUID_A: &str = "11111111-2222-4333-8444-555555555555";
const THREAD_UUID_B: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

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
    fs::write(repo_dir.join("README.md"), "# Test Repo\n").expect("write README.md");

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
    let out = StdCommand::new("git")
        .args(args)
        .current_dir(repo_dir)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn write_executable_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("chmod 0700");
}

fn write_private_workers_config(path: &Path, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize workers config");
    fs::write(path, bytes).expect("write workers config");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
}

fn default_test_budgets(timeout_ms: u64) -> ExecutionBudgets {
    ExecutionBudgets {
        timeout_ms,
        max_line_bytes: 32 * 1024,
        max_stdout_bytes: 256 * 1024,
        max_stderr_bytes: 64 * 1024,
        max_events: 256,
    }
}

fn make_profile_config(
    profile_id: &str,
    allowed_bindings: &[String],
    executable: &Path,
    repo: &GitRepoFixture,
    work_area_root: &Path,
    timeout_ms: u64,
) -> WorkerProfileConfig {
    WorkerProfileConfig {
        profile_id: profile_id.to_string(),
        allowed_binding_ids: allowed_bindings.iter().cloned().collect(),
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
            budgets: default_test_budgets(timeout_ms),
        },
    }
}

async fn wait_for_worker_status<F>(
    scoped: &ScopedClient,
    run_or_worker_id: &str,
    predicate: F,
) -> WorkerSnapshot
where
    F: Fn(&WorkerSnapshot) -> bool,
{
    for _ in 0..250 {
        let snapshot = scoped
            .get_worker(run_or_worker_id.to_string())
            .await
            .expect("get_worker poll");
        if predicate(&snapshot) {
            return snapshot;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let last = scoped
        .get_worker(run_or_worker_id.to_string())
        .await
        .expect("final get_worker");
    panic!("timed out waiting for worker status condition; last snapshot: {last:#?}");
}

fn spawn_mcp_child(state_dir: &Path, binding_id: &str) -> Child {
    let bin = env!("CARGO_BIN_EXE_threadlane");
    Command::new(bin)
        .arg("mcp")
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--binding")
        .arg(binding_id)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn threadlane mcp")
}

async fn send_json_rpc(stdin: &mut tokio::process::ChildStdin, msg: &Value) {
    let line = format!("{}\n", serde_json::to_string(msg).unwrap());
    stdin.write_all(line.as_bytes()).await.expect("write stdin");
    stdin.flush().await.expect("flush stdin");
}

async fn read_json_rpc_response(
    reader: &mut BufReader<tokio::process::ChildStdout>,
    expected_id: u64,
) -> Value {
    let mut line = String::new();
    loop {
        line.clear();
        let n = tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
            .await
            .expect("timeout reading MCP stdout")
            .expect("read MCP line");
        assert!(n > 0, "unexpected EOF from MCP stdout");
        let parsed: Value = serde_json::from_str(line.trim()).expect("valid JSON-RPC line");
        if parsed.get("id") == Some(&json!(expected_id)) {
            return parsed;
        }
    }
}

async fn cleanup_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[tokio::test]
async fn test_workers_disabled_by_default_and_startup_config_validation_side_effect_free() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_disabled");
    let repo = init_git_repo(temp.path());

    // 1. Worker dispatch is disabled by default (`workers: None`).
    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: None,
    })
    .await
    .expect("start supervisor with workers disabled");

    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let binding = client
        .create_binding(THREAD_UUID_A, "Disabled Workers Chat")
        .await
        .expect("create binding");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped client");

    let err = scoped
        .start_worker(
            "cmd-disabled-1".to_string(),
            "default-profile".to_string(),
            "Attempt worker when disabled".to_string(),
        )
        .await
        .expect_err("start_worker must fail when workers are disabled");
    let api_err = err.to_api_error();
    assert_eq!(api_err.code, ErrorCode::InvalidConfig);

    // Verify no run was admitted when worker dispatch is disabled.
    let status = scoped.status().await.expect("status");
    assert_eq!(status.total_runs, 0);
    handle.shutdown().await;

    // 2. Startup `--workers-config` file validation and side-effect freedom.
    let fake_bin = temp.path().join("fake_agy_validate.sh");
    let marker_file = temp.path().join("must_never_be_created.txt");
    write_executable_script(
        &fake_bin,
        &format!(
            "#!/bin/sh\necho spawned > \"{}\"\nexit 0\n",
            marker_file.display()
        ),
    );

    let non_existent_work_area = temp.path().join("uncreated_work_area");
    let valid_profile_json = json!({
        "profile_id": "prof_main",
        "allowed_binding_ids": [binding.binding_id],
        "spec": {
            "provider": "antigravity",
            "executable": fake_bin,
            "model": "gemini-4-argon-high",
            "effort": "high",
            "allowed_models": ["gemini-4-argon-high"],
            "standalone": false,
            "repository_path": repo.repo_dir,
            "base_commit": repo.base_commit,
            "work_area_root": non_existent_work_area,
            "allowed_write_paths": ["src"]
        }
    });

    let cfg_path = temp.path().join("workers.json");
    write_private_workers_config(
        &cfg_path,
        &json!({
            "max_parallel": 2,
            "profiles": [valid_profile_json]
        }),
    );

    // Valid load succeeds without creating `work_area_root`, without creating worktrees, and without spawning.
    let loaded = WorkersConfig::load_from_path(&cfg_path).expect("load valid workers.json");
    assert_eq!(loaded.max_parallel, 2);
    assert_eq!(loaded.profiles.len(), 1);
    assert!(
        !non_existent_work_area.exists(),
        "config validation must not create work_area_root"
    );
    assert!(
        !marker_file.exists(),
        "config validation must not spawn worker executable"
    );

    // Non-0600 permissions must be rejected.
    fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    let perm_err = WorkersConfig::load_from_path(&cfg_path).expect_err("reject 0644 config");
    assert!(perm_err.to_string().contains("0600"), "err: {perm_err}");
    fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).expect("restore 0600");

    // Symlink config file must be rejected.
    let symlink_cfg = temp.path().join("workers_link.json");
    std::os::unix::fs::symlink(&cfg_path, &symlink_cfg).expect("create symlink");
    let sym_err = WorkersConfig::load_from_path(&symlink_cfg).expect_err("reject symlink config");
    assert!(sym_err.to_string().contains("symlink"), "err: {sym_err}");

    // Oversized config file must be rejected without unbounded read.
    let oversized_cfg = temp.path().join("oversized_workers.json");
    fs::write(
        &oversized_cfg,
        vec![b' '; MAX_WORKERS_CONFIG_FILE_BYTES + 16],
    )
    .expect("write oversized");
    fs::set_permissions(&oversized_cfg, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    let over_err =
        WorkersConfig::load_from_path(&oversized_cfg).expect_err("reject oversized config");
    assert!(
        over_err.to_string().contains("exceeds maximum"),
        "err: {over_err}"
    );

    // Unknown fields at root, profile, spec, and budgets levels must be rejected.
    for bad_json in [
        json!({
            "max_parallel": 2,
            "unknown_root_field": true,
            "profiles": [valid_profile_json]
        }),
        json!({
            "max_parallel": 2,
            "profiles": [{
                "profile_id": "prof_main",
                "allowed_binding_ids": [binding.binding_id],
                "unexpected_profile_key": 1,
                "spec": valid_profile_json["spec"]
            }]
        }),
        json!({
            "max_parallel": 2,
            "profiles": [{
                "profile_id": "prof_main",
                "allowed_binding_ids": [binding.binding_id],
                "spec": {
                    "provider": "antigravity",
                    "executable": fake_bin,
                    "model": "gemini-4-argon-high",
                    "effort": "high",
                    "standalone": false,
                    "repository_path": repo.repo_dir,
                    "base_commit": repo.base_commit,
                    "work_area_root": non_existent_work_area,
                    "allowed_write_paths": ["src"],
                    "extra_spec_field": "forbidden"
                }
            }]
        }),
        json!({
            "max_parallel": 2,
            "profiles": [{
                "profile_id": "prof_main",
                "allowed_binding_ids": [binding.binding_id],
                "spec": {
                    "provider": "antigravity",
                    "executable": fake_bin,
                    "model": "gemini-4-argon-high",
                    "effort": "high",
                    "standalone": false,
                    "repository_path": repo.repo_dir,
                    "base_commit": repo.base_commit,
                    "work_area_root": non_existent_work_area,
                    "allowed_write_paths": ["src"],
                    "budgets": {
                        "timeout_ms": 5000,
                        "unknown_budget_field": 99
                    }
                }
            }]
        }),
    ] {
        write_private_workers_config(&cfg_path, &bad_json);
        assert!(
            WorkersConfig::load_from_path(&cfg_path).is_err(),
            "expected unknown field rejection for {bad_json}"
        );
    }

    // Embedded credentials or environment blocks must be rejected.
    write_private_workers_config(
        &cfg_path,
        &json!({
            "max_parallel": 2,
            "profiles": [{
                "profile_id": "prof_main",
                "allowed_binding_ids": [binding.binding_id],
                "spec": valid_profile_json["spec"],
                "api_key": "secret"
            }]
        }),
    );
    let cred_err = WorkersConfig::load_from_path(&cfg_path).expect_err("reject api_key field");
    assert!(
        cred_err.to_string().contains("credentials"),
        "err: {cred_err}"
    );

    // Mutable refs like "HEAD" or short SHAs in `base_commit` must be rejected.
    for bad_commit in ["HEAD", "main", "refs/heads/main", &repo.base_commit[..12]] {
        let mut bad_prof = valid_profile_json.clone();
        bad_prof["spec"]["base_commit"] = json!(bad_commit);
        write_private_workers_config(
            &cfg_path,
            &json!({
                "max_parallel": 2,
                "profiles": [bad_prof]
            }),
        );
        let commit_err =
            WorkersConfig::load_from_path(&cfg_path).expect_err("reject non-40-hex base_commit");
        assert!(
            commit_err.to_string().contains("40-character hexadecimal"),
            "unexpected error for {bad_commit}: {commit_err}"
        );
    }

    // `max_parallel` bounds (1..=8) and profile count bounds (1..=32).
    for bad_parallel in [0usize, 9usize] {
        write_private_workers_config(
            &cfg_path,
            &json!({
                "max_parallel": bad_parallel,
                "profiles": [valid_profile_json]
            }),
        );
        assert!(WorkersConfig::load_from_path(&cfg_path).is_err());
    }

    // Existing `work_area_root` with non-private permissions (0755) must be rejected without chmodding it.
    let insecure_work_area = temp.path().join("insecure_work_area");
    fs::create_dir_all(&insecure_work_area).expect("create insecure_work_area");
    fs::set_permissions(&insecure_work_area, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755");
    let mut insecure_prof = valid_profile_json.clone();
    insecure_prof["spec"]["work_area_root"] = json!(insecure_work_area);
    write_private_workers_config(
        &cfg_path,
        &json!({
            "max_parallel": 2,
            "profiles": [insecure_prof]
        }),
    );
    assert!(WorkersConfig::load_from_path(&cfg_path).is_err());
    let mode_after = fs::symlink_metadata(&insecure_work_area)
        .expect("stat insecure_work_area")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode_after, 0o755,
        "config validation must never chmod existing directories"
    );
}

#[tokio::test]
async fn test_worker_end_to_end_live_progress_mcp_cli_and_wakeup_integration() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_e2e");
    let work_area = temp.path().join("work_area_e2e");
    let repo = init_git_repo(temp.path());

    let progress_ready_file = temp.path().join("progress_ready.flag");
    let allow_exit_file = temp.path().join("allow_exit.flag");
    let codex_log = temp.path().join("codex_wakeup.log");
    let fake_codex = temp.path().join("fake_codex.sh");
    write_executable_script(
        &fake_codex,
        &format!(
            "#!/bin/sh\nprintf '%s|%s|%s|%s|%s|%d\\n' \"$1\" \"$2\" \"$3\" \"$4\" \"$5\" \"$#\" >> \"{}\"\nexit 0\n",
            codex_log.display()
        ),
    );

    let fake_worker = temp.path().join("fake_agy_e2e.sh");
    write_executable_script(
        &fake_worker,
        &format!(
            r#"#!/bin/sh
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"sess-e2e","model":"gemini-4-argon-high","effort":"high"}}'
printf '%s\n' '{{"type":"assistant","message":"Inspecting src/lib.rs with token sk-secret1234567890abcdef"}}'
printf '%s\n' '{{"type":"tool_use","tool_name":"edit_file","summary":"Modifying src/lib.rs"}}'
touch "{progress_ready}"
while [ ! -f "{allow_exit}" ]; do
  sleep 0.02
done
cat << 'EOF' > src/lib.rs
pub fn value() -> u32 {{
    42
}}
EOF
printf '%s\n' '{{"type":"result","subtype":"success","status":"completed","summary":"Updated value() to 42"}}'
exit 0
"#,
            progress_ready = progress_ready_file.display(),
            allow_exit = allow_exit_file.display(),
        ),
    );

    // Seed binding before starting supervisor so both `codex` wakeup and `workers` profile can reference it.
    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "E2E Worker Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let workers_cfg = WorkersConfig {
        max_parallel: 2,
        profiles: vec![make_profile_config(
            "prof_rust",
            std::slice::from_ref(&binding.binding_id),
            &fake_worker,
            &repo,
            &work_area,
            10_000,
        )],
    };

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: Some(CodexWakeupConfig {
            executable: fake_codex,
            notify_bindings: BTreeSet::from([binding.binding_id.clone()]),
            timeout: Duration::from_millis(2_000),
        }),
        workers: Some(workers_cfg),
    })
    .await
    .expect("start supervisor");

    let client = SupervisorClient::from_state_dir(&state_dir).expect("client");
    let scoped = client
        .clone()
        .scoped(&binding.binding_id)
        .expect("scoped client");

    // 1. Start worker via MCP `start_worker` tool and verify `demo_only == false`.
    let mut mcp_child = spawn_mcp_child(&state_dir, &binding.binding_id);
    let mut mcp_stdin = mcp_child.stdin.take().expect("mcp stdin");
    let mcp_stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut mcp_reader = BufReader::new(mcp_stdout);

    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "worker-e2e-mcp", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let _ = read_json_rpc_response(&mut mcp_reader, 1).await;
    send_json_rpc(
        &mut mcp_stdin,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;

    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "start_worker",
                "arguments": {
                    "command_key": "e2e-start-1",
                    "profile_id": "prof_rust",
                    "instruction": "Update value() in src/lib.rs to return 42"
                }
            }
        }),
    )
    .await;
    let start_mcp_resp = read_json_rpc_response(&mut mcp_reader, 2).await;
    assert_ne!(start_mcp_resp["result"]["isError"], json!(true));
    assert_eq!(
        start_mcp_resp["result"]["structuredContent"]["demo_only"],
        json!(false),
        "real worker MCP responses must never be labeled demo_only=true"
    );
    let started_data = &start_mcp_resp["result"]["structuredContent"]["data"];
    let run_id = started_data["run_id"].as_str().expect("run_id").to_string();
    let worker_id = started_data["worker_id"]
        .as_str()
        .expect("worker_id")
        .to_string();
    assert_eq!(started_data["profile_id"], json!("prof_rust"));
    assert_eq!(
        started_data["requested_model"],
        json!("gemini-4-argon-high")
    );
    assert_eq!(started_data["requested_effort"], json!("high"));
    assert_eq!(
        started_data["profile_digest"].as_str().map(str::len),
        Some(64)
    );

    // 2. Wait until the fake worker has emitted live progress lines BEFORE exiting.
    for _ in 0..200 {
        if progress_ready_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        progress_ready_file.exists(),
        "fake worker should reach progress_ready before exit"
    );

    // Poll `get_worker` (using `worker_id` alias!) until live progress is recorded in SQLite while still `Running`.
    let running_snapshot = wait_for_worker_status(&scoped, &worker_id, |s| {
        s.status == WorkerStatus::Running && s.progress.durable_progress_events >= 2
    })
    .await;
    assert_eq!(running_snapshot.run_id, run_id);
    assert_eq!(running_snapshot.worker_id, worker_id);
    assert_eq!(running_snapshot.status, WorkerStatus::Running);
    assert!(running_snapshot.worktree.run_worktree_id.is_some());
    assert!(running_snapshot.worktree.task_worktree_id.is_some());
    let last_summary = running_snapshot
        .progress
        .last_summary
        .as_deref()
        .unwrap_or_default();
    assert!(
        !last_summary.contains("sk-secret1234567890abcdef"),
        "live progress summary must redact secrets: {last_summary}"
    );

    // Verify durable `WorkerProgress` event is visible in `pull_events` before worker process exit.
    let mid_events = scoped
        .pull_events(None, Some(20))
        .await
        .expect("pull mid events");
    assert!(
        mid_events
            .events
            .iter()
            .any(|e| e.kind == EventKind::WorkerStarted),
        "expected WorkerStarted event while running"
    );
    assert!(
        mid_events
            .events
            .iter()
            .any(|e| e.kind == EventKind::WorkerProgress),
        "expected durable WorkerProgress event before process exit"
    );
    for ev in &mid_events.events {
        let serialized = serde_json::to_string(ev).unwrap();
        assert!(
            !serialized.contains("sk-secret1234567890abcdef"),
            "durable event must never leak secret token"
        );
    }

    // 3. Interactive demo operations (`answer`, `steer`, and generic `cancel`) must explicitly reject on worker runs
    // before mutating state.
    let pre_rev = running_snapshot.revision;
    let ans_err = scoped
        .answer(
            "reject-worker-ans".to_string(),
            run_id.clone(),
            "q_none".to_string(),
            pre_rev,
            "interactive answer".to_string(),
        )
        .await
        .expect_err("answer on worker run must fail");
    assert_eq!(ans_err.to_api_error().code, ErrorCode::InvalidRunState);

    let steer_err = scoped
        .steer(
            "reject-worker-steer".to_string(),
            run_id.clone(),
            pre_rev,
            "new instruction".to_string(),
        )
        .await
        .expect_err("steer on worker run must fail");
    assert_eq!(steer_err.to_api_error().code, ErrorCode::InvalidRunState);

    let demo_cancel_err = scoped
        .cancel(
            "reject-demo-cancel".to_string(),
            run_id.clone(),
            Some("wrong cancel op".to_string()),
        )
        .await
        .expect_err("generic demo cancel on worker run must fail");
    assert_eq!(
        demo_cancel_err.to_api_error().code,
        ErrorCode::InvalidRunState
    );

    let still_running = scoped.get_worker(run_id.clone()).await.expect("get_worker");
    assert_eq!(still_running.status, WorkerStatus::Running);
    assert_eq!(still_running.revision, pre_rev);

    // 4. Release the worker to finish writing `src/lib.rs` and exit 0.
    fs::write(&allow_exit_file, b"ok").expect("write allow_exit");

    let completed = wait_for_worker_status(&scoped, &run_id, |s| s.status.is_terminal()).await;
    assert_eq!(completed.status, WorkerStatus::Completed);
    assert_eq!(
        completed.observed_model.as_deref(),
        Some("gemini-4-argon-high")
    );
    assert_eq!(completed.observed_effort.as_deref(), Some("high"));

    let outcome = completed.outcome.as_ref().expect("outcome summary");
    assert_eq!(outcome.exit_code, Some(0));
    assert!(!outcome.timed_out);
    assert!(!outcome.cancelled);
    assert!(!outcome.cancel_requested_before_completion);
    assert!(!outcome.missing_model_evidence);
    assert!(!outcome.missing_effort_evidence);
    assert!(!outcome.provider_error);
    assert!(!outcome.incomplete_cleanup);
    assert!(!outcome.rejected_by_diff_gate);
    assert_eq!(
        outcome.acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );
    assert_eq!(outcome.changed_paths_count, 1);
    assert_eq!(
        outcome.changed_paths_preview,
        vec!["src/lib.rs".to_string()]
    );
    assert_eq!(
        outcome.final_summary.as_deref(),
        Some("Updated value() to 42")
    );

    // Verify private local report artifact exists with 0600 permissions and redacted argv.
    let artifact_id = outcome
        .report_artifact_id
        .as_deref()
        .expect("report_artifact_id");
    let artifact_path = state_dir
        .join("worker_reports")
        .join(format!("{artifact_id}.json"));
    let artifact_meta = fs::symlink_metadata(&artifact_path).expect("stat artifact");
    assert_eq!(artifact_meta.permissions().mode() & 0o777, 0o600);
    let artifact_json: Value =
        serde_json::from_slice(&fs::read(&artifact_path).expect("read artifact"))
            .expect("parse artifact json");
    assert_eq!(
        artifact_json["worker_report"]["constructed_argv"],
        json!([]),
        "private local report artifact must clear raw prompt argv"
    );

    // Verify owned task worktree still has the modified file, while source repo is untouched.
    let task_wt_path = PathBuf::from(
        completed
            .worktree
            .task_worktree_path
            .as_deref()
            .expect("task_worktree_path"),
    );
    assert!(fs::read_to_string(task_wt_path.join("src/lib.rs"))
        .expect("read task worktree lib.rs")
        .contains("42"));
    assert_eq!(
        fs::read_to_string(repo.repo_dir.join("src/lib.rs")).expect("read source lib.rs"),
        "pub fn value() -> u32 {\n    1\n}\n",
        "source repository checkout must remain untouched"
    );

    // Verify MCP `get_worker` returns the completed snapshot with `demo_only == false`.
    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "get_worker",
                "arguments": {
                    "run_id": worker_id
                }
            }
        }),
    )
    .await;
    let get_mcp_resp = read_json_rpc_response(&mut mcp_reader, 3).await;
    assert_eq!(
        get_mcp_resp["result"]["structuredContent"]["demo_only"],
        json!(false)
    );
    assert_eq!(
        get_mcp_resp["result"]["structuredContent"]["data"]["status"],
        json!("completed")
    );

    // Verify CLI `threadlane call --request get_worker` also works end to end.
    let bin = env!("CARGO_BIN_EXE_threadlane");
    let cli_get_out = Command::new(bin)
        .args([
            "call",
            "--state-dir",
            state_dir.to_str().unwrap(),
            "--binding",
            &binding.binding_id,
            "--request",
            &serde_json::to_string(&ScopedRequest::GetWorker {
                run_id: run_id.clone(),
            })
            .unwrap(),
        ])
        .output()
        .await
        .expect("run CLI get_worker");
    assert!(cli_get_out.status.success());
    let cli_json: Value = serde_json::from_slice(&cli_get_out.stdout).expect("parse CLI json");
    assert_eq!(cli_json["data"]["status"], json!("completed"));

    // Verify important `WorkerCompleted` event triggered the Codex wake-up sender with `FIXED_WAKEUP_MESSAGE`.
    for _ in 0..100 {
        let st = scoped.status().await.expect("status");
        if st.wakeup.status == WakeupStatus::Submitted {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let st_final = scoped.status().await.expect("final status");
    assert_eq!(st_final.wakeup.status, WakeupStatus::Submitted);
    let wake_log = fs::read_to_string(&codex_log).expect("read codex wakeup log");
    assert_eq!(
        wake_log.lines().next(),
        Some(format!("queue|--thread|{THREAD_UUID_A}|--message|{FIXED_WAKEUP_MESSAGE}|5").as_str())
    );

    drop(mcp_stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_forbidden_overrides_and_cross_binding_profile_isolation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_isolation");
    let work_area = temp.path().join("work_area_isolation");
    let repo = init_git_repo(temp.path());

    let fake_worker = temp.path().join("fake_agy_iso.sh");
    write_executable_script(&fake_worker, "#!/bin/sh\nexit 0\n");

    let (binding_a, binding_b) = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let a = core
            .create_binding(THREAD_UUID_A, "Authorized Binding A")
            .expect("create A");
        let b = core
            .create_binding(THREAD_UUID_B, "Unauthorized Binding B")
            .expect("create B");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        (a, b)
    };

    let workers_cfg = WorkersConfig {
        max_parallel: 2,
        profiles: vec![make_profile_config(
            "prof_only_a",
            std::slice::from_ref(&binding_a.binding_id),
            &fake_worker,
            &repo,
            &work_area,
            5_000,
        )],
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
    let scoped_a = client.clone().scoped(&binding_a.binding_id).unwrap();
    let scoped_b = client.clone().scoped(&binding_b.binding_id).unwrap();

    // 1. Binding B cannot start a worker with `prof_only_a`.
    let b_err = scoped_b
        .start_worker(
            "b-forbidden-start".to_string(),
            "prof_only_a".to_string(),
            "Try unauthorized profile".to_string(),
        )
        .await
        .expect_err("binding B must be rejected for prof_only_a");
    assert_eq!(b_err.to_api_error().code, ErrorCode::ValidationFailed);
    assert_eq!(
        scoped_b.status().await.expect("status B").total_runs,
        0,
        "rejected profile check must not admit a run"
    );

    // 2. Unknown profile_id must be rejected before admitting a run.
    let unknown_prof_err = scoped_a
        .start_worker(
            "a-unknown-prof".to_string(),
            "nonexistent_profile".to_string(),
            "Task".to_string(),
        )
        .await
        .expect_err("unknown profile must fail");
    assert_eq!(
        unknown_prof_err.to_api_error().code,
        ErrorCode::ValidationFailed
    );
    assert_eq!(scoped_a.status().await.expect("status A").total_runs, 0);

    // 3. Forbidden caller overrides over raw HTTP and MCP must be rejected (`deny_unknown_fields`).
    let base_url = handle.descriptor().base_url().expect("base_url");
    let raw_http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("raw http");

    for injected_payload in [
        json!({
            "op": "start_worker",
            "command_key": "inj-1",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "model": "gpt-4o"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-2",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "effort": "low"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-3",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "executable": "/bin/sh"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-4",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "repository_path": "/tmp"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-5",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "allowed_write_paths": ["*"]
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-6",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "budgets": { "timeout_ms": 999999 }
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-7",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "base_commit": "HEAD"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-8",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "binding_id": binding_a.binding_id
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-9",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "now_ms": 100
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-10",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "file_permissions": "task_files"
        }),
        json!({
            "op": "start_worker",
            "command_key": "inj-11",
            "profile_id": "prof_only_a",
            "instruction": "Task",
            "settings": { "permissions": { "allow": ["read_file(**)"] } }
        }),
    ] {
        let resp = raw_http
            .post(format!(
                "{base_url}/v1/bindings/{}/command",
                binding_a.binding_id
            ))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", handle.descriptor().token.as_str()),
            )
            .json(&injected_payload)
            .send()
            .await
            .expect("send injected HTTP start_worker");
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "injected field must be rejected: {injected_payload}"
        );
    }

    // Verify MCP also rejects injected override fields and forbidden profile registration tools.
    let mut mcp_child = spawn_mcp_child(&state_dir, &binding_a.binding_id);
    let mut mcp_stdin = mcp_child.stdin.take().expect("mcp stdin");
    let mcp_stdout = mcp_child.stdout.take().expect("mcp stdout");
    let mut mcp_reader = BufReader::new(mcp_stdout);

    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "override-check-mcp", "version": "0.1.0" }
            }
        }),
    )
    .await;
    let _ = read_json_rpc_response(&mut mcp_reader, 1).await;
    send_json_rpc(
        &mut mcp_stdin,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;

    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "start_worker",
                "arguments": {
                    "command_key": "mcp-inj-1",
                    "profile_id": "prof_only_a",
                    "instruction": "Task",
                    "model": "override-model"
                }
            }
        }),
    )
    .await;
    let mcp_inj_resp = read_json_rpc_response(&mut mcp_reader, 2).await;
    assert_eq!(mcp_inj_resp["result"]["isError"], json!(true));
    // rmcp rejects unknown arguments during Parameters<T> deserialization,
    // before the handler can produce a structured application error.
    let injection_error = mcp_inj_resp["result"]["content"][0]["text"]
        .as_str()
        .expect("MCP parameter error text");
    assert!(injection_error.contains("failed to deserialize parameters"));
    assert!(injection_error.contains("unknown field") && injection_error.contains("model"));

    for (req_id, field_name, field_val) in [
        (21, "file_permissions", json!("task_files")),
        (
            22,
            "settings",
            json!({ "permissions": { "allow": ["read_file(**)"] } }),
        ),
    ] {
        send_json_rpc(
            &mut mcp_stdin,
            &json!({
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "tools/call",
                "params": {
                    "name": "start_worker",
                    "arguments": {
                        "command_key": format!("mcp-inj-{req_id}"),
                        "profile_id": "prof_only_a",
                        "instruction": "Task",
                        field_name: field_val
                    }
                }
            }),
        )
        .await;
        let mcp_field_resp = read_json_rpc_response(&mut mcp_reader, req_id).await;
        assert_eq!(mcp_field_resp["result"]["isError"], json!(true));
        let field_err = mcp_field_resp["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP parameter error text");
        assert!(field_err.contains("failed to deserialize parameters"));
        assert!(
            field_err.contains("unknown field") && field_err.contains(field_name),
            "expected unknown field `{field_name}` rejection in: {field_err}"
        );
    }

    // Profile registration via MCP is forbidden.
    send_json_rpc(
        &mut mcp_stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "register_worker_profile",
                "arguments": { "profile_id": "evil" }
            }
        }),
    )
    .await;
    let mcp_reg_resp = read_json_rpc_response(&mut mcp_reader, 3).await;
    assert!(
        mcp_reg_resp.get("error").is_some() || mcp_reg_resp["result"]["isError"] == json!(true),
        "profile registration via MCP must be rejected"
    );

    assert_eq!(scoped_a.status().await.expect("status A").total_runs, 0);

    // 4. Cross-binding `get_worker` and `cancel_worker`: Binding B must not be able to inspect or cancel Binding A's worker.
    let worker_a = scoped_a
        .start_worker(
            "a-valid-start".to_string(),
            "prof_only_a".to_string(),
            "Authorized task for A".to_string(),
        )
        .await
        .expect("start worker A");
    let get_cross_err = scoped_b
        .get_worker(worker_a.run_id.clone())
        .await
        .expect_err("binding B must not get_worker of binding A");
    assert_eq!(get_cross_err.to_api_error().code, ErrorCode::WorkerNotFound);
    let cancel_cross_err = scoped_b
        .cancel_worker(
            "b-cross-cancel".to_string(),
            worker_a.run_id.clone(),
            worker_a.revision,
            None,
        )
        .await
        .expect_err("binding B must not cancel_worker of binding A");
    assert_eq!(
        cancel_cross_err.to_api_error().code,
        ErrorCode::WorkerNotFound
    );

    drop(mcp_stdin);
    cleanup_child(&mut mcp_child).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_idempotency_retries_queue_capacity_and_profile_digest_conflict() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_idemp");
    let work_area = temp.path().join("work_area_idemp");
    let repo = init_git_repo(temp.path());

    let block_file = temp.path().join("block_worker.flag");
    fs::write(&block_file, b"1").expect("write block_file");
    let count_file = temp.path().join("spawn_count.txt");
    let fake_worker = temp.path().join("fake_agy_idemp.sh");
    write_executable_script(
        &fake_worker,
        &format!(
            r#"#!/bin/sh
echo spawned >> "{count}"
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"s-idemp","model":"gemini-4-argon-high","effort":"high"}}'
while [ -f "{block}" ]; do
  sleep 0.02
done
printf '%s\n' '{{"type":"result","subtype":"success","status":"completed","summary":"Done"}}'
exit 0
"#,
            count = count_file.display(),
            block = block_file.display(),
        ),
    );

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Idempotency Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    // Configure `max_parallel = 1` so the first worker occupies the only execution slot.
    let workers_cfg = WorkersConfig {
        max_parallel: 1,
        profiles: vec![make_profile_config(
            "prof_1",
            std::slice::from_ref(&binding.binding_id),
            &fake_worker,
            &repo,
            &work_area,
            10_000,
        )],
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
    let scoped = client.scoped(&binding.binding_id).expect("scoped");

    let first = scoped
        .start_worker(
            "idemp-key-1".to_string(),
            "prof_1".to_string(),
            "First task".to_string(),
        )
        .await
        .expect("first start_worker");

    // Wait until the first worker is actively `Running`, occupying the sole `max_parallel = 1` slot.
    let _ = wait_for_worker_status(&scoped, &first.run_id, |s| {
        s.status == WorkerStatus::Running
    })
    .await;

    // Duplicate `start_worker` retry with the same `command_key` while the queue/slot is full
    // must succeed immediately with the original snapshot and never queue a second run!
    let retry = scoped
        .start_worker(
            "idemp-key-1".to_string(),
            "prof_1".to_string(),
            "First task".to_string(),
        )
        .await
        .expect("idempotent retry while slot is full");
    assert_eq!(retry.run_id, first.run_id);
    assert_eq!(retry.worker_id, first.worker_id);
    assert_eq!(scoped.status().await.expect("status").total_runs, 1);

    // Reusing `idemp-key-1` with a different instruction or on `cancel_worker` must return `IdempotencyConflict`.
    let diff_instr_err = scoped
        .start_worker(
            "idemp-key-1".to_string(),
            "prof_1".to_string(),
            "Different instruction".to_string(),
        )
        .await
        .expect_err("different instruction with same command_key must conflict");
    assert_eq!(
        diff_instr_err.to_api_error().code,
        ErrorCode::IdempotencyConflict
    );

    let cross_op_err = scoped
        .cancel_worker("idemp-key-1".to_string(), first.run_id.clone(), 1, None)
        .await
        .expect_err("reusing start_worker command_key on cancel_worker must conflict");
    assert_eq!(
        cross_op_err.to_api_error().code,
        ErrorCode::IdempotencyConflict
    );

    fs::remove_file(&block_file).expect("unblock worker");
    let _ = wait_for_worker_status(&scoped, &first.run_id, |s| {
        s.status == WorkerStatus::Completed
    })
    .await;
    assert_eq!(
        fs::read_to_string(&count_file)
            .expect("read count")
            .lines()
            .count(),
        1,
        "idempotent retry must never spawn a duplicate process"
    );

    handle.shutdown().await;

    // Restart supervisor with a modified profile definition under the same `profile_id` ("prof_1"),
    // changing `allowed_write_paths` (and thus `profile_digest`).
    // Retrying `idemp-key-1` must detect the `profile_digest` mismatch and return `IdempotencyConflict`!
    let mut modified_profile = make_profile_config(
        "prof_1",
        std::slice::from_ref(&binding.binding_id),
        &fake_worker,
        &repo,
        &work_area,
        10_000,
    );
    modified_profile
        .spec
        .allowed_write_paths
        .insert("README.md".to_string());

    let handle_2 = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 1,
            profiles: vec![modified_profile],
        }),
    })
    .await
    .expect("start supervisor 2");

    let scoped_2 = SupervisorClient::from_state_dir(&state_dir)
        .expect("client 2")
        .scoped(&binding.binding_id)
        .expect("scoped 2");

    let changed_digest_err = scoped_2
        .start_worker(
            "idemp-key-1".to_string(),
            "prof_1".to_string(),
            "First task".to_string(),
        )
        .await
        .expect_err("retrying command_key after profile_digest change must conflict");
    assert_eq!(
        changed_digest_err.to_api_error().code,
        ErrorCode::IdempotencyConflict
    );

    handle_2.shutdown().await;
}

#[tokio::test]
async fn test_bounded_concurrency_cap_and_isolated_worktrees() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_concurrency");
    let work_area = temp.path().join("work_area_concurrency");
    let repo = init_git_repo(temp.path());

    let gate_file = temp.path().join("release_workers.flag");
    let active_dir = temp.path().join("active_pids");
    fs::create_dir_all(&active_dir).expect("create active_dir");

    let fake_worker = temp.path().join("fake_agy_conc.sh");
    write_executable_script(
        &fake_worker,
        &format!(
            r#"#!/bin/sh
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"s-conc","model":"gemini-4-argon-high","effort":"high"}}'
touch "{active_dir}/$$"
while [ ! -f "{gate}" ]; do
  sleep 0.02
done
rm -f "{active_dir}/$$"
printf '%s\n' '{{"type":"result","subtype":"success","status":"completed","summary":"Done"}}'
exit 0
"#,
            active_dir = active_dir.display(),
            gate = gate_file.display(),
        ),
    );

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Concurrency Cap Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 2,
            profiles: vec![make_profile_config(
                "prof_conc",
                std::slice::from_ref(&binding.binding_id),
                &fake_worker,
                &repo,
                &work_area,
                10_000,
            )],
        }),
    })
    .await
    .expect("start supervisor");

    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .expect("client")
        .scoped(&binding.binding_id)
        .expect("scoped");

    let w1 = scoped
        .start_worker(
            "c-1".to_string(),
            "prof_conc".to_string(),
            "Job 1".to_string(),
        )
        .await
        .expect("start w1");
    let w2 = scoped
        .start_worker(
            "c-2".to_string(),
            "prof_conc".to_string(),
            "Job 2".to_string(),
        )
        .await
        .expect("start w2");
    let w3 = scoped
        .start_worker(
            "c-3".to_string(),
            "prof_conc".to_string(),
            "Job 3".to_string(),
        )
        .await
        .expect("start w3");

    // Wait for w1 and w2 to reach `Running`.
    let r1 =
        wait_for_worker_status(&scoped, &w1.run_id, |s| s.status == WorkerStatus::Running).await;
    let r2 =
        wait_for_worker_status(&scoped, &w2.run_id, |s| s.status == WorkerStatus::Running).await;

    // Give the background loop several ticks and verify w3 stays `Queued` while 2 workers are active.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let q3 = scoped.get_worker(w3.run_id.clone()).await.expect("get w3");
    assert_eq!(
        q3.status,
        WorkerStatus::Queued,
        "3rd worker must remain Queued while max_parallel=2 slots are occupied"
    );

    let active_count = fs::read_dir(&active_dir).expect("read active_dir").count();
    assert!(
        active_count <= 2,
        "at most 2 worker processes may run concurrently, found {active_count}"
    );

    // Verify w1 and w2 have distinct isolated run and task worktrees.
    assert_ne!(r1.worktree.run_worktree_path, r2.worktree.run_worktree_path);
    assert_ne!(
        r1.worktree.task_worktree_path,
        r2.worktree.task_worktree_path
    );

    // Release the gate and verify all 3 workers complete with distinct worktrees.
    fs::write(&gate_file, b"go").expect("release gate");
    let c1 =
        wait_for_worker_status(&scoped, &w1.run_id, |s| s.status == WorkerStatus::Completed).await;
    let c2 =
        wait_for_worker_status(&scoped, &w2.run_id, |s| s.status == WorkerStatus::Completed).await;
    let c3 =
        wait_for_worker_status(&scoped, &w3.run_id, |s| s.status == WorkerStatus::Completed).await;

    let task_paths: BTreeSet<String> = [
        c1.worktree.task_worktree_path.unwrap(),
        c2.worktree.task_worktree_path.unwrap(),
        c3.worktree.task_worktree_path.unwrap(),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        task_paths.len(),
        3,
        "all 3 runs must have distinct task worktrees"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn test_cancel_worker_revision_check_and_completion_race() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_cancel");
    let work_area = temp.path().join("work_area_cancel");
    let repo = init_git_repo(temp.path());

    let running_flag = temp.path().join("cancel_target_running.flag");
    let slow_worker = temp.path().join("slow_agy.sh");
    write_executable_script(
        &slow_worker,
        &format!(
            r#"#!/bin/sh
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"s-cancel","model":"gemini-4-argon-high","effort":"high"}}'
touch "{running}"
sleep 30
"#,
            running = running_flag.display(),
        ),
    );

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Cancel Worker Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 2,
            profiles: vec![make_profile_config(
                "prof_slow",
                std::slice::from_ref(&binding.binding_id),
                &slow_worker,
                &repo,
                &work_area,
                20_000,
            )],
        }),
    })
    .await
    .expect("start supervisor");

    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .expect("client")
        .scoped(&binding.binding_id)
        .expect("scoped");

    let started = scoped
        .start_worker(
            "cancel-start-1".to_string(),
            "prof_slow".to_string(),
            "Long running job".to_string(),
        )
        .await
        .expect("start slow worker");

    let running = wait_for_worker_status(&scoped, &started.run_id, |s| {
        s.status == WorkerStatus::Running
    })
    .await;
    for _ in 0..100 {
        if running_flag.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // 1. Stale `expected_revision` on `cancel_worker` must be rejected with `StaleRevision`.
    let stale_err = scoped
        .cancel_worker(
            "cancel-stale".to_string(),
            started.run_id.clone(),
            running.revision + 99,
            Some("wrong rev".to_string()),
        )
        .await
        .expect_err("stale revision must fail");
    assert_eq!(stale_err.to_api_error().code, ErrorCode::StaleRevision);

    // 2. Valid `cancel_worker` transitions to `CancelRequested` and then `Cancelled` once terminated.
    let cancel_req_snap = scoped
        .cancel_worker(
            "cancel-valid-1".to_string(),
            started.run_id.clone(),
            running.revision,
            Some("User requested stop".to_string()),
        )
        .await
        .expect("valid cancel_worker");
    assert!(matches!(
        cancel_req_snap.status,
        WorkerStatus::CancelRequested | WorkerStatus::Cancelled
    ));
    assert_eq!(
        cancel_req_snap.cancel_reason.as_deref(),
        Some("User requested stop")
    );

    let cancelled = wait_for_worker_status(&scoped, &started.run_id, |s| {
        s.status == WorkerStatus::Cancelled
    })
    .await;
    let outcome = cancelled.outcome.as_ref().expect("cancelled outcome");
    assert!(outcome.cancelled);
    assert!(outcome.cancel_requested_before_completion);
    assert_eq!(
        outcome.acceptance_state,
        WorkerAcceptanceState::ExecutionFailed
    );

    // 3. Cancellation racing with natural completion:
    // When a worker finishes with full valid completion evidence and exit code 0 right as
    // cancellation is signaled, the genuine `Completed` result and `ReadyForReview` state
    // are retained while recording `cancel_requested_before_completion = true` and `cancelled = false`.
    let race_ready_flag = temp.path().join("race_ready.flag");
    let race_worker = temp.path().join("race_agy.sh");
    write_executable_script(
        &race_worker,
        &format!(
            r#"#!/bin/sh
trap 'printf "%s\n" "{{\"type\":\"result\",\"subtype\":\"success\",\"status\":\"completed\",\"summary\":\"Natural completion won race\"}}"; exit 0' TERM INT
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"s-race","model":"gemini-4-argon-high","effort":"high"}}'
touch "{ready}"
while :; do
  sleep 0.05
done
"#,
            ready = race_ready_flag.display(),
        ),
    );

    handle.shutdown().await;

    let handle_race = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 2,
            profiles: vec![make_profile_config(
                "prof_race",
                std::slice::from_ref(&binding.binding_id),
                &race_worker,
                &repo,
                &work_area,
                20_000,
            )],
        }),
    })
    .await
    .expect("start race supervisor");

    let scoped_race = SupervisorClient::from_state_dir(&state_dir)
        .expect("race client")
        .scoped(&binding.binding_id)
        .expect("race scoped");

    let race_started = scoped_race
        .start_worker(
            "race-start-1".to_string(),
            "prof_race".to_string(),
            "Finish right as cancel arrives".to_string(),
        )
        .await
        .expect("start race worker");

    let race_running = wait_for_worker_status(&scoped_race, &race_started.run_id, |s| {
        s.status == WorkerStatus::Running
    })
    .await;
    for _ in 0..100 {
        if race_ready_flag.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    let _ = scoped_race
        .cancel_worker(
            "race-cancel-1".to_string(),
            race_started.run_id.clone(),
            race_running.revision,
            Some("Late cancel request".to_string()),
        )
        .await
        .expect("cancel_worker on racing worker");

    let race_final = wait_for_worker_status(&scoped_race, &race_started.run_id, |s| {
        s.status.is_terminal()
    })
    .await;
    assert_eq!(
        race_final.status,
        WorkerStatus::Completed,
        "natural completion winning the race must retain Completed status"
    );
    let race_outcome = race_final.outcome.as_ref().expect("race outcome");
    assert!(!race_outcome.cancelled);
    assert!(race_outcome.cancel_requested_before_completion);
    assert_eq!(
        race_outcome.acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );
    assert_eq!(
        race_outcome.final_summary.as_deref(),
        Some("Natural completion won race")
    );

    handle_race.shutdown().await;
}

#[tokio::test]
async fn test_stale_callbacks_crash_recovery_unknown_and_shutdown_drop() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_recovery");
    let work_area = temp.path().join("work_area_recovery");
    let repo = init_git_repo(temp.path());

    // 1. Direct Core verification: stale claim/revision/digest callbacks cannot overwrite terminal or Unknown state,
    // and reopening `Supervisor` recovers active workers to `Unknown` without invoking the provider executable.
    let never_spawn_marker = temp.path().join("must_not_spawn_on_reopen.txt");
    let fake_worker = temp.path().join("fake_agy_recover.sh");
    write_executable_script(
        &fake_worker,
        &format!(
            "#!/bin/sh\necho spawned >> \"{}\"\nexit 0\n",
            never_spawn_marker.display()
        ),
    );

    let (binding, seeded_run_id, stale_claim) = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Recovery Chat")
            .expect("create binding");

        let profile = make_profile_config(
            "prof_rec",
            std::slice::from_ref(&b.binding_id),
            &fake_worker,
            &repo,
            &work_area,
            10_000,
        );
        let validated = WorkersConfig {
            max_parallel: 1,
            profiles: vec![profile],
        }
        .validate()
        .expect("validate config");
        let vprof = validated.profiles.get("prof_rec").unwrap();

        let out = core
            .execute(
                &b.binding_id,
                threadlane_core::Command::StartWorker {
                    command_key: "seed-w-1".to_string(),
                    profile_id: vprof.profile_id.clone(),
                    profile_digest: vprof.profile_digest.clone(),
                    provider: vprof.provider_str().to_string(),
                    requested_model: vprof.requested_model_str().to_string(),
                    requested_effort: vprof.requested_effort.clone(),
                    base_commit: vprof.template.base_commit.clone(),
                    instruction: "Interrupted before completion".to_string(),
                    now_ms: 1_000,
                },
            )
            .expect("start_worker in core");
        let snap = match out {
            threadlane_core::CommandOutput::Worker(w) => w,
            other => panic!("unexpected output: {other:?}"),
        };
        let claim = core
            .claim_worker(
                &b.binding_id,
                &snap.run_id,
                snap.revision,
                &vprof.profile_digest,
                1_010,
            )
            .expect("claim_worker");
        let _ = core
            .record_worker_running(
                &claim,
                WorkerWorktreeUpdate {
                    run_worktree_id: "wt-run-seed".to_string(),
                    run_worktree_path: "/tmp/wt-run-seed".to_string(),
                    run_branch_name: "tl/run/seed".to_string(),
                    task_worktree_id: "wt-task-seed".to_string(),
                    task_worktree_path: "/tmp/wt-task-seed".to_string(),
                    task_branch_name: "tl/task/seed".to_string(),
                    base_commit: repo.base_commit.clone(),
                },
                1_020,
            )
            .expect("record_worker_running");

        // Verify a forged claim with wrong `claim_id` or `profile_digest` is rejected!
        let mut forged_claim = claim.clone();
        forged_claim.claim_id = "wclm_00000000000000000000000000000000".to_string();
        assert!(matches!(
            core.record_worker_progress(
                &forged_claim,
                &[WorkerProgressUpdate {
                    event_kind: "assistant_message".to_string(),
                    summary: "forged".to_string(),
                }],
                1_025,
            ),
            Err(CoreError::StaleWorkerClaim { .. })
        ));
        let after_forged = core
            .get_worker(&b.binding_id, &snap.run_id)
            .expect("read worker after forged callback");
        assert_eq!(after_forged.status, WorkerStatus::Running);
        assert_eq!(after_forged.progress.durable_progress_events, 0);

        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        (b, snap.run_id.clone(), claim)
    };

    // Start supervisor on the database containing the interrupted `Running` worker.
    // Startup recovery must mark it `Unknown`, emit an important `WorkerUnknown` event, and NEVER spawn `fake_worker`.
    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 1,
            profiles: vec![make_profile_config(
                "prof_rec",
                std::slice::from_ref(&binding.binding_id),
                &fake_worker,
                &repo,
                &work_area,
                10_000,
            )],
        }),
    })
    .await
    .expect("start supervisor after crash");

    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .expect("client")
        .scoped(&binding.binding_id)
        .expect("scoped");

    let recovered = scoped
        .get_worker(seeded_run_id.clone())
        .await
        .expect("get recovered worker");
    assert_eq!(recovered.status, WorkerStatus::Unknown);
    assert_eq!(
        recovered.outcome.as_ref().map(|o| o.acceptance_state),
        Some(WorkerAcceptanceState::Unknown)
    );

    let events = scoped
        .pull_events(None, Some(20))
        .await
        .expect("pull events");
    assert!(
        events
            .events
            .iter()
            .any(|e| e.kind == EventKind::WorkerUnknown),
        "startup recovery must emit durable WorkerUnknown event"
    );
    let status = scoped.status().await.expect("status");
    assert_eq!(
        status.wakeup.status,
        WakeupStatus::Pending,
        "WorkerUnknown must mark binding wake-up Pending"
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        !never_spawn_marker.exists(),
        "uncertain recovered worker must never be re-invoked"
    );

    handle.shutdown().await;

    // Verify a late stale completion callback after the worker is already `Unknown` is rejected by `Core`!
    {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let late_res = core.record_worker_outcome(
            &stale_claim,
            WorkerCompletionRecord {
                status: WorkerStatus::Completed,
                observed_model: Some("gemini-4-argon-high".to_string()),
                observed_effort: Some("high".to_string()),
                head_commit: Some(repo.base_commit.clone()),
                dropped_live_events: 0,
                outcome: WorkerOutcomeSummary {
                    exit_code: Some(0),
                    signal: None,
                    duration_ms: Some(10),
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
                    changed_paths_count: 0,
                    changed_paths_preview: Vec::new(),
                    violation_count: 0,
                    violation_summaries: Vec::new(),
                    final_summary: Some("Late stale completion".to_string()),
                    failure_reason: None,
                    report_artifact_id: None,
                },
            },
            2_000,
        );
        assert!(
            late_res.is_err(),
            "stale completion must not overwrite Unknown state"
        );
        let snap_after = core
            .get_worker(&binding.binding_id, &seeded_run_id)
            .expect("get_worker after stale callback");
        assert_eq!(snap_after.status, WorkerStatus::Unknown);
    }

    // 2. Dropping / shutting down supervisor while a worker is actively running terminates the worker
    // and leaves durable `Unknown` state rather than `Completed` or user `Cancelled`.
    let drop_state_dir = temp.path().join("state_shutdown_active");
    let drop_work_area = temp.path().join("work_area_shutdown_active");
    let active_flag = temp.path().join("active_during_shutdown.flag");
    let hang_worker = temp.path().join("hang_agy.sh");
    write_executable_script(
        &hang_worker,
        &format!(
            r#"#!/bin/sh
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"s-hang","model":"gemini-4-argon-high","effort":"high"}}'
touch "{active}"
sleep 30
"#,
            active = active_flag.display(),
        ),
    );

    let drop_binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&drop_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_B, "Shutdown Active Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&drop_state_dir)
            .expect("enforce perms");
        b
    };

    let drop_handle = Supervisor::start(SupervisorConfig {
        state_dir: drop_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 1,
            profiles: vec![make_profile_config(
                "prof_hang",
                std::slice::from_ref(&drop_binding.binding_id),
                &hang_worker,
                &repo,
                &drop_work_area,
                20_000,
            )],
        }),
    })
    .await
    .expect("start drop_handle");

    let drop_scoped = SupervisorClient::from_state_dir(&drop_state_dir)
        .expect("client")
        .scoped(&drop_binding.binding_id)
        .expect("scoped");
    let active_w = drop_scoped
        .start_worker(
            "hang-1".to_string(),
            "prof_hang".to_string(),
            "Hang during shutdown".to_string(),
        )
        .await
        .expect("start hang-1");

    let _ = wait_for_worker_status(&drop_scoped, &active_w.run_id, |s| {
        s.status == WorkerStatus::Running
    })
    .await;
    for _ in 0..100 {
        if active_flag.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Shut down supervisor while the worker process is mid-flight.
    drop_handle.shutdown().await;

    let db_path = threadlane::descriptor::prepare_database_path(&drop_state_dir).expect("prep db");
    let core = Core::open(&db_path).expect("open core after shutdown");
    let after_shutdown = core
        .get_worker(&drop_binding.binding_id, &active_w.run_id)
        .expect("get_worker after shutdown");
    assert_eq!(
        after_shutdown.status,
        WorkerStatus::Unknown,
        "worker interrupted by supervisor shutdown must be recorded as Unknown"
    );

    // 3. Fatal SQLite error during worker outcome transitions the supervisor into a faulted state,
    // cancels other active workers, and stops new dispatch.
    let fault_state_dir = temp.path().join("state_sqlite_fault");
    let fault_work_area = temp.path().join("work_area_sqlite_fault");
    let fast_worker = temp.path().join("fast_fault_agy.sh");
    write_executable_script(
        &fast_worker,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-fast","model":"gemini-4-argon-high","effort":"high"}'
printf '%s\n' '{"type":"result","subtype":"success","status":"completed","summary":"Fast completion"}'
exit 0
"#,
    );

    let fault_binding = {
        let db_path =
            threadlane::descriptor::prepare_database_path(&fault_state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "SQLite Fault Chat")
            .expect("create binding");
        drop(core);

        let conn = rusqlite::Connection::open(&db_path).expect("open sqlite for trigger");
        conn.execute_batch(
            "CREATE TRIGGER fail_worker_completed_event
             BEFORE INSERT ON events
             WHEN NEW.kind = 'worker_completed'
             BEGIN
                 SELECT RAISE(ABORT, 'simulated disk fault on worker_completed event');
             END;",
        )
        .expect("create fault trigger");
        drop(conn);
        threadlane::descriptor::enforce_database_permissions(&fault_state_dir)
            .expect("enforce perms");
        b
    };

    let fault_handle = Supervisor::start(SupervisorConfig {
        state_dir: fault_state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 2,
            profiles: vec![make_profile_config(
                "prof_fault",
                std::slice::from_ref(&fault_binding.binding_id),
                &fast_worker,
                &repo,
                &fault_work_area,
                10_000,
            )],
        }),
    })
    .await
    .expect("start fault supervisor");

    let fault_scoped = SupervisorClient::from_state_dir(&fault_state_dir)
        .expect("fault client")
        .scoped(&fault_binding.binding_id)
        .expect("fault scoped");

    let _ = fault_scoped
        .start_worker(
            "fault-w-1".to_string(),
            "prof_fault".to_string(),
            "Trigger SQLite fault on completion".to_string(),
        )
        .await
        .expect("start fault-w-1");

    // Wait for the fatal SQLite error to be recorded by the supervisor.
    for _ in 0..200 {
        if fault_handle.fatal_error().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        fault_handle.fatal_error().is_some(),
        "expected fatal supervisor error when SQLite trigger aborts worker outcome"
    );

    // Subsequent requests must preserve the exact recorded fatal storage error.
    let post_fault_err = fault_scoped
        .start_worker(
            "fault-w-2".to_string(),
            "prof_fault".to_string(),
            "Should be rejected after fatal SQLite fault".to_string(),
        )
        .await
        .expect_err("faulted supervisor must reject new worker dispatch");
    assert_eq!(
        post_fault_err.to_api_error().code,
        fault_handle
            .fatal_error()
            .expect("recorded fatal error")
            .code
    );

    fault_handle.shutdown().await;
}

#[tokio::test]
async fn test_high_volume_progress_cap_redaction_wire_budgets_and_failure_outcomes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state_outcomes");
    let work_area = temp.path().join("work_area_outcomes");
    let repo = init_git_repo(temp.path());

    // 1. High-volume progress emitter (80 progress lines with secrets).
    let spam_worker = temp.path().join("spam_agy.sh");
    write_executable_script(
        &spam_worker,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-spam","model":"gemini-4-argon-high","effort":"high"}'
i=1
while [ "$i" -le 80 ]; do
  printf '{"type":"assistant","message":"Progress step %d with secret ghp_1234567890abcdefghijklmnopqrstuv and Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.secret"}\n' "$i"
  i=$((i + 1))
done
printf '%s\n' '{"type":"result","subtype":"success","status":"completed","summary":"Finished 80 steps"}'
exit 0
"#,
    );

    // 2. Missing model evidence emitter.
    let missing_model_worker = temp.path().join("missing_model_agy.sh");
    write_executable_script(
        &missing_model_worker,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-nomodel","effort":"high"}'
printf '%s\n' '{"type":"result","subtype":"success","status":"completed","summary":"No model telemetry"}'
exit 0
"#,
    );

    // 3. Diff-gate violation emitter (writes `forbidden.txt` outside `allowed_write_paths = ["src"]`).
    let diff_viol_worker = temp.path().join("diff_viol_agy.sh");
    write_executable_script(
        &diff_viol_worker,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-diff","model":"gemini-4-argon-high","effort":"high"}'
echo "unauthorized write" > forbidden.txt
printf '%s\n' '{"type":"result","subtype":"success","status":"completed","summary":"Wrote forbidden.txt"}'
exit 0
"#,
    );

    // 4. Provider error emitter.
    let provider_err_worker = temp.path().join("provider_err_agy.sh");
    write_executable_script(
        &provider_err_worker,
        r#"#!/bin/sh
printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-err","model":"gemini-4-argon-high","effort":"high"}'
printf '%s\n' '{"type":"error","code":"rate_limit_exceeded","message":"Provider quota exceeded for key sk-live-99999999999999999999"}'
exit 1
"#,
    );

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "Outcomes & Budgets Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let workers_cfg = WorkersConfig {
        max_parallel: 2,
        profiles: vec![
            make_profile_config(
                "prof_spam",
                std::slice::from_ref(&binding.binding_id),
                &spam_worker,
                &repo,
                &work_area,
                10_000,
            ),
            make_profile_config(
                "prof_missing_model",
                std::slice::from_ref(&binding.binding_id),
                &missing_model_worker,
                &repo,
                &work_area,
                10_000,
            ),
            make_profile_config(
                "prof_diff_viol",
                std::slice::from_ref(&binding.binding_id),
                &diff_viol_worker,
                &repo,
                &work_area,
                10_000,
            ),
            make_profile_config(
                "prof_provider_err",
                std::slice::from_ref(&binding.binding_id),
                &provider_err_worker,
                &repo,
                &work_area,
                10_000,
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

    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .expect("client")
        .scoped(&binding.binding_id)
        .expect("scoped");

    // Case 1: High-volume progress stream completes cleanly, caps durable progress events, and redacts secrets.
    let spam_start = scoped
        .start_worker(
            "out-spam".to_string(),
            "prof_spam".to_string(),
            "Run 80 steps".to_string(),
        )
        .await
        .expect("start spam worker");
    let spam_done =
        wait_for_worker_status(&scoped, &spam_start.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(spam_done.status, WorkerStatus::Completed);
    assert!(
        spam_done.progress.durable_progress_events <= MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT,
        "durable progress events ({}) must not exceed cap ({MAX_WORKER_PROGRESS_EVENTS_PER_ATTEMPT})",
        spam_done.progress.durable_progress_events
    );
    assert!(
        spam_done.progress.coalesced_progress_events > 0
            || spam_done.progress.dropped_live_events > 0,
        "excess progress events must be coalesced or counted in dropped_live_events"
    );
    assert!(
        estimate_wire_response_bytes(&spam_done).expect("wire estimate")
            <= MAX_COMPACT_RESPONSE_BYTES
    );

    let batch = scoped
        .pull_events(None, Some(50))
        .await
        .expect("pull events");
    assert!(
        estimate_wire_response_bytes(&batch).expect("batch wire estimate")
            <= MAX_PAGE_RESPONSE_BYTES
    );
    let batch_str = serde_json::to_string(&batch).unwrap();
    assert!(!batch_str.contains("ghp_1234567890abcdefghijklmnopqrstuv"));
    assert!(!batch_str.contains("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"));

    // Case 2: Missing model telemetry -> `Failed` with `missing_model_evidence: true` (exit zero is never enough!).
    let mm_start = scoped
        .start_worker(
            "out-mm".to_string(),
            "prof_missing_model".to_string(),
            "Run without model telemetry".to_string(),
        )
        .await
        .expect("start missing_model worker");
    let mm_done =
        wait_for_worker_status(&scoped, &mm_start.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(mm_done.status, WorkerStatus::Failed);
    let mm_outcome = mm_done.outcome.as_ref().expect("mm outcome");
    assert_eq!(mm_outcome.exit_code, Some(0));
    assert!(mm_outcome.missing_model_evidence);
    assert_eq!(
        mm_outcome.acceptance_state,
        WorkerAcceptanceState::ExecutionFailed
    );

    // Case 3: Diff-gate violation -> `Failed` with `rejected_by_diff_gate: true` and dirty task worktree preserved.
    let dv_start = scoped
        .start_worker(
            "out-dv".to_string(),
            "prof_diff_viol".to_string(),
            "Modify forbidden.txt".to_string(),
        )
        .await
        .expect("start diff_viol worker");
    let dv_done =
        wait_for_worker_status(&scoped, &dv_start.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(dv_done.status, WorkerStatus::Failed);
    let dv_outcome = dv_done.outcome.as_ref().expect("dv outcome");
    assert!(dv_outcome.rejected_by_diff_gate);
    assert_eq!(
        dv_outcome.acceptance_state,
        WorkerAcceptanceState::RejectedByDiffGate
    );
    assert!(dv_outcome.violation_count >= 1);
    assert!(!dv_outcome.violation_summaries.is_empty());

    let dv_task_wt = PathBuf::from(
        dv_done
            .worktree
            .task_worktree_path
            .as_deref()
            .expect("dv task worktree"),
    );
    assert!(
        dv_task_wt.join("forbidden.txt").exists(),
        "dirty task worktree must be preserved on diff-gate rejection"
    );
    assert!(
        !repo.repo_dir.join("forbidden.txt").exists(),
        "source repository must not contain forbidden.txt"
    );

    // Case 4: Provider error event -> `Failed` with `provider_error: true` and redacted failure_reason.
    let pe_start = scoped
        .start_worker(
            "out-pe".to_string(),
            "prof_provider_err".to_string(),
            "Trigger provider error".to_string(),
        )
        .await
        .expect("start provider_err worker");
    let pe_done =
        wait_for_worker_status(&scoped, &pe_start.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(pe_done.status, WorkerStatus::Failed);
    let pe_outcome = pe_done.outcome.as_ref().expect("pe outcome");
    assert!(pe_outcome.provider_error);
    let pe_reason = pe_outcome
        .failure_reason
        .as_deref()
        .expect("pe failure_reason");
    assert!(!pe_reason.contains("sk-live-99999999999999999999"));

    handle.shutdown().await;
}

#[tokio::test]
async fn test_report_artifacts_preserve_existing_paths_and_genuine_outcome() {
    for case in [
        "directory_symlink",
        "file_symlink",
        "insecure_directory",
        "existing_file",
        "fresh",
    ] {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        let work_area = temp.path().join("work_area");
        let repo = init_git_repo(temp.path());
        let ready = temp.path().join("worker_ready");
        let allow_exit = temp.path().join("allow_exit");
        let fake_worker = temp.path().join("fake_artifact_worker.sh");
        write_executable_script(
            &fake_worker,
            &format!(
                r#"#!/bin/sh
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"artifact-test","model":"gemini-4-argon-high","effort":"high"}}'
touch "{ready}"
while [ ! -f "{allow_exit}" ]; do sleep 0.02; done
printf '%s\n' '{{"type":"result","subtype":"success","status":"completed","summary":"Artifact fixture completed"}}'
"#,
                ready = ready.display(),
                allow_exit = allow_exit.display()
            ),
        );
        let binding = {
            let db = threadlane::descriptor::prepare_database_path(&state_dir).expect("database");
            let mut core = Core::open(&db).expect("core");
            let binding = core
                .create_binding(THREAD_UUID_A, "Artifact review fixture")
                .expect("binding");
            threadlane::descriptor::enforce_database_permissions(&state_dir).expect("permissions");
            binding
        };
        let handle = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(10),
            codex: None,
            workers: Some(WorkersConfig {
                max_parallel: 1,
                profiles: vec![make_profile_config(
                    "artifact_fixture",
                    std::slice::from_ref(&binding.binding_id),
                    &fake_worker,
                    &repo,
                    &work_area,
                    10_000,
                )],
            }),
        })
        .await
        .expect("supervisor");
        let scoped = SupervisorClient::from_state_dir(&state_dir)
            .expect("client")
            .scoped(&binding.binding_id)
            .expect("scoped client");
        let reports = state_dir.join("worker_reports");
        let external = temp.path().join("external_directory");
        fs::create_dir(&external).expect("external directory");
        fs::set_permissions(&external, fs::Permissions::from_mode(0o755)).expect("external mode");
        let precious = temp.path().join("precious.txt");
        fs::write(&precious, "PRESERVE THIS FILE").expect("sentinel");
        fs::set_permissions(&precious, fs::Permissions::from_mode(0o644)).expect("sentinel mode");
        match case {
            "directory_symlink" => {
                std::os::unix::fs::symlink(&external, &reports).expect("directory symlink")
            }
            "insecure_directory" => {
                fs::create_dir(&reports).expect("insecure reports");
                fs::set_permissions(&reports, fs::Permissions::from_mode(0o755))
                    .expect("directory mode");
                fs::write(reports.join("keep.txt"), "PRESERVE DIRECTORY")
                    .expect("directory sentinel");
            }
            "file_symlink" | "existing_file" => {
                fs::create_dir(&reports).expect("reports");
                fs::set_permissions(&reports, fs::Permissions::from_mode(0o700))
                    .expect("private reports");
            }
            "fresh" => {}
            _ => unreachable!(),
        }
        let started = scoped
            .start_worker(
                "artifact-test".into(),
                "artifact_fixture".into(),
                "Private instruction marker".into(),
            )
            .await
            .expect("start");
        for _ in 0..250 {
            if ready.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready.exists(), "worker did not reach exit barrier: {case}");
        let artifact = reports.join(format!("art_{}.json", started.worker_id));
        if case == "file_symlink" {
            std::os::unix::fs::symlink(&precious, &artifact).expect("artifact symlink");
        } else if case == "existing_file" {
            fs::write(&artifact, "PRESERVE EXISTING ARTIFACT").expect("existing artifact");
            fs::set_permissions(&artifact, fs::Permissions::from_mode(0o644))
                .expect("existing artifact mode");
        }
        fs::write(&allow_exit, "exit").expect("release worker");
        let terminal =
            wait_for_worker_status(&scoped, &started.run_id, |w| w.status.is_terminal()).await;
        assert_eq!(
            terminal.status,
            WorkerStatus::Completed,
            "genuine worker outcome: {case}"
        );
        let outcome = terminal.outcome.expect("terminal outcome");
        assert_eq!(
            outcome.acceptance_state,
            WorkerAcceptanceState::ReadyForReview
        );
        assert_eq!(fs::read_to_string(&precious).unwrap(), "PRESERVE THIS FILE");
        assert_eq!(
            fs::metadata(&precious).unwrap().permissions().mode() & 0o777,
            0o644
        );
        if case == "fresh" {
            assert_eq!(
                outcome.report_artifact_id,
                Some(format!("art_{}", started.worker_id))
            );
            assert_eq!(
                fs::metadata(&reports).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(&artifact).unwrap().permissions().mode() & 0o777,
                0o600
            );
            let bytes = fs::read(&artifact).expect("read artifact");
            assert!(bytes.len() <= 128 * 1024);
            let report: Value = serde_json::from_slice(&bytes).expect("report JSON");
            assert_eq!(report["worker_report"]["constructed_argv"], json!([]));
            assert!(!String::from_utf8(bytes)
                .unwrap()
                .contains("Private instruction marker"));
        } else {
            assert!(
                outcome.report_artifact_id.is_none(),
                "unsafe path must omit artifact: {case}"
            );
            match case {
                "directory_symlink" => {
                    assert!(fs::symlink_metadata(&reports)
                        .unwrap()
                        .file_type()
                        .is_symlink());
                    assert_eq!(
                        fs::metadata(&external).unwrap().permissions().mode() & 0o777,
                        0o755
                    );
                    assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
                }
                "insecure_directory" => {
                    assert_eq!(
                        fs::metadata(&reports).unwrap().permissions().mode() & 0o777,
                        0o755
                    );
                    assert_eq!(
                        fs::read_to_string(reports.join("keep.txt")).unwrap(),
                        "PRESERVE DIRECTORY"
                    );
                    assert!(!artifact.exists());
                }
                "file_symlink" => assert!(fs::symlink_metadata(&artifact)
                    .unwrap()
                    .file_type()
                    .is_symlink()),
                "existing_file" => {
                    assert_eq!(
                        fs::read_to_string(&artifact).unwrap(),
                        "PRESERVE EXISTING ARTIFACT"
                    );
                    assert_eq!(
                        fs::metadata(&artifact).unwrap().permissions().mode() & 0o777,
                        0o644
                    );
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(
            fs::read_to_string(repo.repo_dir.join("src/lib.rs")).unwrap(),
            "pub fn value() -> u32 {\n    1\n}\n"
        );
        handle.shutdown().await;
    }
}

#[tokio::test]
async fn test_worker_file_permissions_profile_digest_validation_and_task_files_dispatch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let repo = init_git_repo(temp.path());

    let never_spawn_marker = temp.path().join("validate_must_not_spawn.flag");
    let fake_validate_bin = temp.path().join("fake_agy_perm_validate.sh");
    write_executable_script(
        &fake_validate_bin,
        &format!(
            "#!/bin/sh\necho spawned > \"{}\"\nexit 0\n",
            never_spawn_marker.display()
        ),
    );

    let uncreated_work_area = temp.path().join("uncreated_perm_work_area");
    let cfg_omitted_path = temp.path().join("workers_omitted.json");
    let cfg_explicit_path = temp.path().join("workers_explicit_inherit.json");
    let cfg_task_files_path = temp.path().join("workers_task_files.json");

    // 1. Omitted vs explicit `inherit` equality and default preservation.
    assert_eq!(
        WorkerFilePermissions::default(),
        WorkerFilePermissions::Inherit
    );

    let omitted_profile_json = json!({
        "profile_id": "prof_perm",
        "allowed_binding_ids": ["bind_perm_1"],
        "spec": {
            "provider": "antigravity",
            "executable": fake_validate_bin,
            "model": "gemini-4-argon-high",
            "effort": "high",
            "allowed_models": ["gemini-4-argon-high"],
            "standalone": false,
            "repository_path": repo.repo_dir,
            "base_commit": repo.base_commit,
            "work_area_root": uncreated_work_area,
            "allowed_write_paths": ["src"]
        }
    });

    let mut explicit_inherit_profile_json = omitted_profile_json.clone();
    explicit_inherit_profile_json["spec"]["file_permissions"] = json!("inherit");

    let mut task_files_profile_json = omitted_profile_json.clone();
    task_files_profile_json["spec"]["file_permissions"] = json!("task_files");

    write_private_workers_config(
        &cfg_omitted_path,
        &json!({
            "max_parallel": 2,
            "profiles": [omitted_profile_json]
        }),
    );
    write_private_workers_config(
        &cfg_explicit_path,
        &json!({
            "max_parallel": 2,
            "profiles": [explicit_inherit_profile_json]
        }),
    );
    write_private_workers_config(
        &cfg_task_files_path,
        &json!({
            "max_parallel": 2,
            "profiles": [task_files_profile_json]
        }),
    );

    let loaded_omitted =
        WorkersConfig::load_from_path(&cfg_omitted_path).expect("load omitted file_permissions");
    let loaded_explicit =
        WorkersConfig::load_from_path(&cfg_explicit_path).expect("load explicit inherit");
    let loaded_task_files =
        WorkersConfig::load_from_path(&cfg_task_files_path).expect("load task_files");

    assert_eq!(
        loaded_omitted.profiles[0].spec.file_permissions,
        WorkerFilePermissions::Inherit
    );
    assert_eq!(loaded_omitted, loaded_explicit);

    let val_omitted = loaded_omitted.validate().expect("validate omitted");
    let val_explicit = loaded_explicit
        .validate()
        .expect("validate explicit inherit");
    let val_task_files = loaded_task_files.validate().expect("validate task_files");

    let prof_omitted = val_omitted.profiles.get("prof_perm").expect("prof_perm");
    let prof_explicit = val_explicit.profiles.get("prof_perm").expect("prof_perm");
    let prof_task_files = val_task_files.profiles.get("prof_perm").expect("prof_perm");

    assert_eq!(
        prof_omitted.template.file_permissions,
        WorkerFilePermissions::Inherit
    );
    assert_eq!(
        prof_omitted
            .build_spec("Check".to_string())
            .file_permissions,
        WorkerFilePermissions::Inherit
    );
    let legacy_canonical = json!({
        "profile_id": prof_omitted.profile_id,
        "allowed_binding_ids": prof_omitted.allowed_binding_ids,
        "provider": prof_omitted.template.provider.as_str(),
        "executable": prof_omitted.template.executable.to_string_lossy(),
        "base_model": prof_omitted.resolved_model.base_model,
        "requested_effort": prof_omitted.requested_effort,
        "cli_model_arg": prof_omitted.resolved_model.cli_model_arg,
        "allowed_models": prof_omitted.template.allowed_models,
        "standalone": prof_omitted.template.standalone,
        "session_id": prof_omitted.template.session_id,
        "repository_path": prof_omitted.template.repository_path.to_string_lossy(),
        "base_commit": prof_omitted.template.base_commit,
        "work_area_root": prof_omitted.template.work_area_root.to_string_lossy(),
        "allowed_write_paths": prof_omitted.template.allowed_write_paths,
        "budgets": {
            "timeout_ms": prof_omitted.template.budgets.timeout_ms,
            "max_line_bytes": prof_omitted.template.budgets.max_line_bytes,
            "max_stdout_bytes": prof_omitted.template.budgets.max_stdout_bytes,
            "max_stderr_bytes": prof_omitted.template.budgets.max_stderr_bytes,
            "max_events": prof_omitted.template.budgets.max_events,
        }
    });
    let legacy_digest = {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        let bytes = serde_json::to_vec(&legacy_canonical).expect("serialize legacy canonical");
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let mut hex = String::with_capacity(64);
        for byte in hasher.finalize() {
            let _ = write!(&mut hex, "{byte:02x}");
        }
        hex
    };
    assert_eq!(
        prof_omitted.profile_digest, legacy_digest,
        "inherit profile_digest must match legacy M2B canonical digest without file_permissions"
    );
    assert_eq!(
        prof_omitted.profile_digest, prof_explicit.profile_digest,
        "omitted file_permissions must produce the exact same profile_digest as explicit inherit"
    );

    // 2. `TaskFiles` preserves canonical template field, copies into `WorkerSpec`, and changes `profile_digest`.
    assert_eq!(
        prof_task_files.template.file_permissions,
        WorkerFilePermissions::TaskFiles
    );
    assert_eq!(
        prof_task_files
            .build_spec("Check".to_string())
            .file_permissions,
        WorkerFilePermissions::TaskFiles
    );
    assert_ne!(
        prof_task_files.profile_digest, prof_omitted.profile_digest,
        "task_files must change the immutable profile_digest"
    );

    // 3. Strict variant validation and rejection of arbitrary `settings` in startup profile config.
    let bad_cfg_path = temp.path().join("workers_bad_perm.json");
    for invalid_spec_patch in [
        json!({ "file_permissions": "all" }),
        json!({ "file_permissions": "Inherit" }),
        json!({ "file_permissions": "task-files" }),
        json!({ "file_permissions": true }),
        json!({ "settings": { "permissions": { "allow": ["read_file(**)"] } } }),
    ] {
        let mut bad_prof = omitted_profile_json.clone();
        for (k, v) in invalid_spec_patch.as_object().unwrap() {
            bad_prof["spec"][k] = v.clone();
        }
        write_private_workers_config(
            &bad_cfg_path,
            &json!({
                "max_parallel": 1,
                "profiles": [bad_prof]
            }),
        );
        assert!(
            WorkersConfig::load_from_path(&bad_cfg_path).is_err(),
            "expected startup config rejection for patch {invalid_spec_patch}"
        );
    }

    // 4. Unsupported OpenCode policy (`opencode_v2` with `task_files`) is rejected before side effects.
    write_private_workers_config(
        &bad_cfg_path,
        &json!({
            "max_parallel": 1,
            "profiles": [{
                "profile_id": "prof_opencode_bad",
                "allowed_binding_ids": ["bind_perm_1"],
                "spec": {
                    "provider": "opencode_v2",
                    "executable": fake_validate_bin,
                    "model": "openai/gpt-5",
                    "effort": "high",
                    "allowed_models": ["openai/gpt-5"],
                    "standalone": true,
                    "repository_path": repo.repo_dir,
                    "base_commit": repo.base_commit,
                    "work_area_root": uncreated_work_area,
                    "allowed_write_paths": ["src"],
                    "file_permissions": "task_files"
                }
            }]
        }),
    );
    assert!(
        WorkersConfig::load_from_path(&bad_cfg_path).is_err(),
        "task_files must be rejected for provider opencode_v2"
    );

    // 5. Unsafe permission-pattern metacharacters in `allowed_write_paths` and `work_area_root`
    // are rejected before any filesystem or process side effects when `file_permissions` is `task_files`.
    for unsafe_write_path in [
        "src/*",
        "src/file?.rs",
        "src/[a-z].rs",
        "src/lib].rs",
        "src/{a,b}.rs",
        "src/lib}.rs",
        "src/(group).rs",
        "src/lib).rs",
        "src\\lib.rs",
        "src/bad\x07ctrl.rs",
    ] {
        let mut bad_prof = task_files_profile_json.clone();
        bad_prof["spec"]["allowed_write_paths"] = json!([unsafe_write_path]);
        write_private_workers_config(
            &bad_cfg_path,
            &json!({
                "max_parallel": 1,
                "profiles": [bad_prof]
            }),
        );
        assert!(
            WorkersConfig::load_from_path(&bad_cfg_path).is_err(),
            "expected rejection of unsafe write path `{unsafe_write_path}` under task_files"
        );
    }

    for unsafe_work_area_name in [
        "work_area_*",
        "work_area_?",
        "work_area_[1]",
        "work_area_{a}",
        "work_area_(x)",
        "work_area_\\bad",
    ] {
        let mut bad_prof = task_files_profile_json.clone();
        bad_prof["spec"]["work_area_root"] = json!(temp.path().join(unsafe_work_area_name));
        write_private_workers_config(
            &bad_cfg_path,
            &json!({
                "max_parallel": 1,
                "profiles": [bad_prof]
            }),
        );
        assert!(
            WorkersConfig::load_from_path(&bad_cfg_path).is_err(),
            "expected rejection of unsafe work_area_root `{unsafe_work_area_name}` under task_files"
        );
    }

    // Clean Unicode and spaces in paths must remain valid with `task_files` without side effects.
    let unicode_work_area = temp.path().join("work area café Δ");
    let mut unicode_prof = task_files_profile_json.clone();
    unicode_prof["spec"]["work_area_root"] = json!(unicode_work_area);
    unicode_prof["spec"]["allowed_write_paths"] = json!(["src", "docs/notes café.md"]);
    write_private_workers_config(
        &bad_cfg_path,
        &json!({
            "max_parallel": 1,
            "profiles": [unicode_prof]
        }),
    );
    let loaded_unicode = WorkersConfig::load_from_path(&bad_cfg_path)
        .expect("clean Unicode and spaces must be accepted with task_files");
    assert_eq!(
        loaded_unicode.profiles[0].spec.file_permissions,
        WorkerFilePermissions::TaskFiles
    );

    assert!(
        !uncreated_work_area.exists() && !unicode_work_area.exists(),
        "startup validation must remain side-effect free and never create work_area_root"
    );
    assert!(
        !never_spawn_marker.exists(),
        "startup validation must never spawn worker executable"
    );

    // 6. End-to-end supervisor dispatch with `Inherit` vs `TaskFiles` and idempotency digest conflict.
    let state_dir = temp.path().join("state_perm_e2e");
    let work_area = temp.path().join("work_area_perm_e2e");
    let argv_log_dir = temp.path().join("argv_logs");
    fs::create_dir_all(&argv_log_dir).expect("create argv_log_dir");

    let fake_perm_worker = temp.path().join("fake_agy_perm_e2e.sh");
    write_executable_script(
        &fake_perm_worker,
        &format!(
            r#"#!/bin/sh
out_file="{log_dir}/$(basename "$PWD").argv"
: > "$out_file"
for arg in "$@"; do
  printf '%s\n' "$arg" >> "$out_file"
done
printf '%s\n' '{{"type":"system","subtype":"init","session_id":"sess-perm","model":"gemini-4-argon-high","effort":"high"}}'
cat << 'EOF' > src/lib.rs
pub fn value() -> u32 {{
    99
}}
EOF
printf '%s\n' '{{"type":"result","subtype":"success","status":"completed","summary":"Updated src/lib.rs under scoped permissions"}}'
exit 0
"#,
            log_dir = argv_log_dir.display(),
        ),
    );

    let binding = {
        let db_path = threadlane::descriptor::prepare_database_path(&state_dir).expect("prep db");
        let mut core = Core::open(&db_path).expect("open core");
        let b = core
            .create_binding(THREAD_UUID_A, "File Permissions E2E Chat")
            .expect("create binding");
        threadlane::descriptor::enforce_database_permissions(&state_dir).expect("enforce perms");
        b
    };

    let prof_inherit_cfg = make_profile_config(
        "prof_inherit",
        std::slice::from_ref(&binding.binding_id),
        &fake_perm_worker,
        &repo,
        &work_area,
        10_000,
    );
    let mut prof_task_files_cfg = make_profile_config(
        "prof_task_files",
        std::slice::from_ref(&binding.binding_id),
        &fake_perm_worker,
        &repo,
        &work_area,
        10_000,
    );
    prof_task_files_cfg.spec.file_permissions = WorkerFilePermissions::TaskFiles;

    let handle = Supervisor::start(SupervisorConfig {
        state_dir: state_dir.clone(),
        tick_interval: Duration::from_millis(15),
        codex: None,
        workers: Some(WorkersConfig {
            max_parallel: 2,
            profiles: vec![prof_inherit_cfg, prof_task_files_cfg],
        }),
    })
    .await
    .expect("start supervisor");

    let scoped = SupervisorClient::from_state_dir(&state_dir)
        .expect("client")
        .scoped(&binding.binding_id)
        .expect("scoped");

    let run_inherit = scoped
        .start_worker(
            "perm-inherit-1".to_string(),
            "prof_inherit".to_string(),
            "Run with inherit permissions".to_string(),
        )
        .await
        .expect("start inherit worker");
    let done_inherit =
        wait_for_worker_status(&scoped, &run_inherit.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(done_inherit.status, WorkerStatus::Completed);

    let inherit_wt = PathBuf::from(
        done_inherit
            .worktree
            .task_worktree_path
            .as_deref()
            .expect("inherit task_worktree_path"),
    );
    let inherit_argv_lines: Vec<String> = fs::read_to_string(argv_log_dir.join(format!(
        "{}.argv",
        inherit_wt.file_name().unwrap().to_string_lossy()
    )))
    .expect("read inherit argv log")
    .lines()
    .map(str::to_string)
    .collect();
    assert!(
        !inherit_argv_lines.iter().any(|a| a == "--settings"),
        "inherit profile must not pass --settings: {inherit_argv_lines:?}"
    );

    let run_tf = scoped
        .start_worker(
            "perm-tf-1".to_string(),
            "prof_task_files".to_string(),
            "Run with task_files permissions".to_string(),
        )
        .await
        .expect("start task_files worker");
    let done_tf = wait_for_worker_status(&scoped, &run_tf.run_id, |s| s.status.is_terminal()).await;
    assert_eq!(done_tf.status, WorkerStatus::Completed);
    let tf_outcome = done_tf.outcome.as_ref().expect("tf outcome");
    assert_eq!(
        tf_outcome.acceptance_state,
        WorkerAcceptanceState::ReadyForReview
    );

    let tf_wt = PathBuf::from(
        done_tf
            .worktree
            .task_worktree_path
            .as_deref()
            .expect("task_files task_worktree_path"),
    );
    let tf_argv_lines: Vec<String> = fs::read_to_string(argv_log_dir.join(format!(
        "{}.argv",
        tf_wt.file_name().unwrap().to_string_lossy()
    )))
    .expect("read task_files argv log")
    .lines()
    .map(str::to_string)
    .collect();
    let settings_idx = tf_argv_lines
        .iter()
        .position(|a| a == "--settings")
        .expect("task_files profile must pass --settings");
    let settings_json: Value =
        serde_json::from_str(&tf_argv_lines[settings_idx + 1]).expect("valid --settings JSON");
    let allow_rules: Vec<&str> = settings_json["permissions"]["allow"]
        .as_array()
        .expect("permissions.allow array")
        .iter()
        .map(|v| v.as_str().expect("string rule"))
        .collect();
    let canonical_tf_wt = fs::canonicalize(&tf_wt).expect("canonicalize tf_wt");
    let expected_read_rule = format!("read_file({}/**)", canonical_tf_wt.display());
    let expected_edit_exact = format!("edit_file({}/src)", canonical_tf_wt.display());
    let expected_edit_desc = format!("edit_file({}/src/**)", canonical_tf_wt.display());
    assert!(
        allow_rules.contains(&expected_read_rule.as_str()),
        "expected `{expected_read_rule}` in {allow_rules:?}"
    );
    assert!(
        allow_rules.contains(&expected_edit_exact.as_str())
            && allow_rules.contains(&expected_edit_desc.as_str()),
        "expected exact + descendant edit_file rules in {allow_rules:?}"
    );
    assert!(
        !tf_argv_lines
            .iter()
            .any(|a| a == "--auto" || a == "--dangerously-skip-permissions"),
        "task_files must never pass --auto or --dangerously-skip-permissions"
    );

    handle.shutdown().await;
}
