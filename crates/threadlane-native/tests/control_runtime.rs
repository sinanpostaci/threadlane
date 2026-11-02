//! Focused synthetic integration tests for `threadlane_native::control_runtime`.
//!
//! All tests run against isolated `0700` temporary directories and owned synthetic/in-process
//! supervisor processes. No user state, credentials, or ambient system services are touched.

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use clap::Parser;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane::cli::{
    build_supervisor_config_with_workers, Cli, Subcommands as ThreadlaneCliSubcommands,
};
use threadlane::descriptor::{
    RuntimeDescriptor, SecretToken, StateLock, DATABASE_FILENAME, DESCRIPTOR_FILENAME,
    LOCK_FILENAME,
};
use threadlane::{
    ScopedClient, Supervisor, SupervisorClient, SupervisorConfig, DEFAULT_CODEX_TIMEOUT_MS,
    DEFAULT_TICK_INTERVAL_MS,
};
use threadlane_core::{
    Command as CoreCommand, CommandOutput, Core, RunStatus, WorkerExecutionBinding, WorkerStatus,
};
use threadlane_native::{
    build_control_runtime_serve_args, ensure_control_runtime, is_process_alive,
    is_process_group_alive, validate_control_runtime_executable_path,
    validate_control_runtime_state_dir, validate_control_runtime_timeout,
    ControlRuntimeEnsureOutcome, ControlRuntimeEnsureStatus, ControlRuntimeError,
    ControlRuntimeErrorCode, ControlRuntimePolicy, CONTROL_RUNTIME_IN_FLIGHT_RECOVERY_POLICY,
    CONTROL_RUNTIME_LIFETIME_POLICY,
};

const CHILD_MODE_ENV: &str = "THREADLANE_TEST_CONTROL_RUNTIME_CHILD_MODE";
const CHILD_ARGV_FILE_ENV: &str = "THREADLANE_TEST_CHILD_ARGV_FILE";
const CHILD_KEEP_ALIVE_FILE_ENV: &str = "THREADLANE_TEST_KEEP_ALIVE_FILE";
const CHILD_DELAY_MS_ENV: &str = "THREADLANE_TEST_CHILD_DELAY_MS";
const CHILD_CALLER_STATE_DIR_ENV: &str = "THREADLANE_TEST_CALLER_STATE_DIR";
const CHILD_CALLER_EXE_ENV: &str = "THREADLANE_TEST_CALLER_EXE";
const CHILD_CALLER_TIMEOUT_MS_ENV: &str = "THREADLANE_TEST_CALLER_TIMEOUT_MS";
const CHILD_CALLER_OUT_FILE_ENV: &str = "THREADLANE_TEST_CALLER_OUT_FILE";
const CHILD_LEADER_PID_FILE_ENV: &str = "THREADLANE_TEST_LEADER_PID_FILE";
const CHILD_DESCENDANT_PID_FILE_ENV: &str = "THREADLANE_TEST_DESCENDANT_PID_FILE";
const CHILD_IGNORE_SIGTERM_ENV: &str = "THREADLANE_TEST_IGNORE_SIGTERM";

const TEST_THREAD_UUID_A: &str = "018f1a2b-3c4d-7e00-8000-000000000101";
const TEST_THREAD_UUID_B: &str = "018f1a2b-3c4d-7e00-8000-000000000102";

#[derive(Debug, Serialize, Deserialize)]
enum CallerChildResult {
    Ok(ControlRuntimeEnsureOutcome),
    Err(ControlRuntimeError),
}

fn assert_wire_safe_deserialize_owned<T: Serialize + DeserializeOwned>() {}

/// Self-invoking child entrypoint used by synthetic `bin/threadlane` wrapper scripts inside tests.
///
/// During normal `cargo test` execution (`CHILD_MODE_ENV` unset), this returns immediately as a
/// no-op. When `exec`-ed by a test wrapper script in a child OS process, it runs one of the
/// deterministic synthetic child modes without relying on ambient `PATH` utilities:
/// - `"real_serve"`: validates the exact `threadlane serve --state-dir <dir>` CLI arguments via
///   [`Cli::try_parse_from`] and runs a real [`Supervisor::start`] inside the child process until
///   its keep-alive file is removed (or a 25s safety cap expires),
/// - `"ensure_caller"`: invokes [`ensure_control_runtime`] from an independent OS process to test
///   cross-process startup races and caller-exit survival,
/// - `"exit_with_descendant"`: spawns a background child in the same process group, records both
///   PIDs, and exits non-zero to test premature-exit group cleanup,
/// - `"hang_with_descendant"`: optionally ignores `SIGTERM`, spawns a background child in the same
///   process group, records both PIDs, and sleeps to test timeout and cancellation cleanup, or
/// - `"sleep_only"`: sleeps quietly as a benign helper process.
#[test]
#[allow(unsafe_code)]
fn synthetic_control_runtime_child_entrypoint() {
    assert_wire_safe_deserialize_owned::<ControlRuntimeEnsureOutcome>();
    assert_wire_safe_deserialize_owned::<ControlRuntimeError>();
    assert_wire_safe_deserialize_owned::<CallerChildResult>();

    let Some(mode_os) = std::env::var_os(CHILD_MODE_ENV) else {
        return;
    };
    let mode = mode_os.to_string_lossy().into_owned();

    match mode.as_str() {
        "sleep_only" => {
            std::thread::sleep(Duration::from_secs(25));
            std::process::exit(0);
        }
        "exit_with_descendant" => {
            let leader_pid_file = PathBuf::from(
                std::env::var_os(CHILD_LEADER_PID_FILE_ENV)
                    .expect("CHILD_LEADER_PID_FILE_ENV must be set"),
            );
            let descendant_pid_file = PathBuf::from(
                std::env::var_os(CHILD_DESCENDANT_PID_FILE_ENV)
                    .expect("CHILD_DESCENDANT_PID_FILE_ENV must be set"),
            );
            let test_bin = std::env::current_exe().expect("current_exe");
            let descendant = std::process::Command::new(&test_bin)
                .args([
                    "--exact",
                    "synthetic_control_runtime_child_entrypoint",
                    "--nocapture",
                ])
                .env(CHILD_MODE_ENV, "sleep_only")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn sleep_only descendant");
            fs::write(&leader_pid_file, std::process::id().to_string())
                .expect("write leader_pid_file");
            fs::write(&descendant_pid_file, descendant.id().to_string())
                .expect("write descendant_pid_file");
            std::thread::sleep(Duration::from_millis(30));
            std::process::exit(1);
        }
        "hang_with_descendant" => {
            if std::env::var(CHILD_IGNORE_SIGTERM_ENV).as_deref() == Ok("1") {
                unsafe {
                    libc::signal(libc::SIGTERM, libc::SIG_IGN);
                }
            }
            let leader_pid_file = PathBuf::from(
                std::env::var_os(CHILD_LEADER_PID_FILE_ENV)
                    .expect("CHILD_LEADER_PID_FILE_ENV must be set"),
            );
            let descendant_pid_file = PathBuf::from(
                std::env::var_os(CHILD_DESCENDANT_PID_FILE_ENV)
                    .expect("CHILD_DESCENDANT_PID_FILE_ENV must be set"),
            );
            let test_bin = std::env::current_exe().expect("current_exe");
            let descendant = std::process::Command::new(&test_bin)
                .args([
                    "--exact",
                    "synthetic_control_runtime_child_entrypoint",
                    "--nocapture",
                ])
                .env(CHILD_MODE_ENV, "sleep_only")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn sleep_only descendant");
            fs::write(&leader_pid_file, std::process::id().to_string())
                .expect("write leader_pid_file");
            fs::write(&descendant_pid_file, descendant.id().to_string())
                .expect("write descendant_pid_file");
            std::thread::sleep(Duration::from_secs(25));
            std::process::exit(0);
        }
        "real_serve" => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build child tokio runtime");
            let argv_file = PathBuf::from(
                std::env::var_os(CHILD_ARGV_FILE_ENV).expect("CHILD_ARGV_FILE_ENV must be set"),
            );
            let keep_alive_file = PathBuf::from(
                std::env::var_os(CHILD_KEEP_ALIVE_FILE_ENV)
                    .expect("CHILD_KEEP_ALIVE_FILE_ENV must be set"),
            );
            let delay_ms: u64 = std::env::var(CHILD_DELAY_MS_ENV)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            let raw_argv = fs::read_to_string(&argv_file).expect("read recorded child argv");
            let recorded_args: Vec<String> = raw_argv.lines().map(str::to_string).collect();
            let mut full_cli = Vec::with_capacity(recorded_args.len() + 1);
            full_cli.push("threadlane".to_string());
            full_cli.extend(recorded_args);

            let parsed = Cli::try_parse_from(&full_cli)
                .expect("spawned argv must parse as valid threadlane::cli::Cli");
            let config = match parsed.command {
                ThreadlaneCliSubcommands::Serve {
                    state_dir,
                    codex,
                    notify_bindings,
                    tick_interval_ms,
                    codex_timeout_ms,
                    workers_config,
                } => {
                    assert!(codex.is_none(), "codex must be None");
                    assert!(notify_bindings.is_empty(), "notify_bindings must be empty");
                    assert_eq!(tick_interval_ms, DEFAULT_TICK_INTERVAL_MS);
                    assert_eq!(codex_timeout_ms, DEFAULT_CODEX_TIMEOUT_MS);
                    assert!(workers_config.is_none(), "workers_config must be None");
                    build_supervisor_config_with_workers(
                        state_dir,
                        codex,
                        notify_bindings,
                        tick_interval_ms,
                        codex_timeout_ms,
                        workers_config,
                    )
                    .expect("build validated SupervisorConfig from parsed CLI")
                }
                other => {
                    panic!(
                        "unexpected threadlane CLI command in child_entrypoint: {:?}",
                        other
                    );
                }
            };

            rt.block_on(async move {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                let handle = match Supervisor::start(config).await {
                    Ok(h) => h,
                    Err(_) => {
                        std::process::exit(1);
                    }
                };
                let safety_deadline = Instant::now() + Duration::from_secs(25);
                while Instant::now() < safety_deadline && keep_alive_file.exists() {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                handle.shutdown().await;
            });
            std::process::exit(0);
        }
        "ensure_caller" => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("build child tokio runtime");
            let state_dir = PathBuf::from(
                std::env::var_os(CHILD_CALLER_STATE_DIR_ENV)
                    .expect("CHILD_CALLER_STATE_DIR_ENV must be set"),
            );
            let exe = PathBuf::from(
                std::env::var_os(CHILD_CALLER_EXE_ENV).expect("CHILD_CALLER_EXE_ENV must be set"),
            );
            let timeout_ms: u64 = std::env::var(CHILD_CALLER_TIMEOUT_MS_ENV)
                .expect("CHILD_CALLER_TIMEOUT_MS_ENV must be set")
                .parse()
                .expect("parse timeout_ms");
            let out_file = PathBuf::from(
                std::env::var_os(CHILD_CALLER_OUT_FILE_ENV)
                    .expect("CHILD_CALLER_OUT_FILE_ENV must be set"),
            );

            let res = rt.block_on(async move {
                match ensure_control_runtime(&state_dir, &exe, Duration::from_millis(timeout_ms))
                    .await
                {
                    Ok(outcome) => CallerChildResult::Ok(outcome),
                    Err(err) => CallerChildResult::Err(err),
                }
            });
            let encoded = serde_json::to_vec_pretty(&res).expect("serialize CallerChildResult");
            fs::write(&out_file, encoded).expect("write CallerChildResult");
            std::process::exit(0);
        }
        other => panic!("unknown CHILD_MODE_ENV `{other}`"),
    }
}

/// Owned test fixture that cleans up any spawned synthetic supervisor or helper processes on drop.
struct ControlRuntimeTestFixture {
    _tmp: TempDir,
    root: PathBuf,
    state_dir: PathBuf,
    bin_dir: PathBuf,
    keep_alive_file: PathBuf,
    pids_file: PathBuf,
    tracked_pids: Vec<u32>,
}

impl ControlRuntimeTestFixture {
    fn new() -> Self {
        let tmp = TempDir::new().expect("create private TempDir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize TempDir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 root TempDir");

        let state_dir = root.join("control-state");
        fs::create_dir(&state_dir).expect("create state_dir");
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 state_dir");

        let bin_dir = root.join("bin");
        fs::create_dir(&bin_dir).expect("create bin_dir");
        fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 bin_dir");

        let keep_alive_file = root.join("keep-alive.flag");
        fs::write(&keep_alive_file, b"1\n").expect("write keep-alive flag");

        let pids_file = root.join("spawned-pids.txt");

        Self {
            _tmp: tmp,
            root,
            state_dir,
            bin_dir,
            keep_alive_file,
            pids_file,
            tracked_pids: Vec::new(),
        }
    }

    fn track_pid(&mut self, pid: u32) {
        if pid > 1 {
            self.tracked_pids.push(pid);
        }
    }

    /// Writes a synthetic `threadlane` executable script in `self.bin_dir` that records its
    /// invocation count, PID, and exact argv using only POSIX `/bin/sh` builtins (compatible with
    /// `cmd.env_clear()` on NixOS), and then `exec`s this test binary's
    /// `synthetic_control_runtime_child_entrypoint` in `"real_serve"` mode so the child process
    /// keeps the exact spawned PID and PGID and runs a real `Supervisor::start`.
    fn write_real_serve_wrapper(
        &self,
        name: &str,
        count_file: &Path,
        argv_file: &Path,
        delay_ms: u64,
        require_preexisting_stale_files: bool,
    ) -> PathBuf {
        let script_path = self.bin_dir.join(name);
        let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
            .expect("canonicalize current_exe");
        let stale_assertion = if require_preexisting_stale_files {
            format!(
                r#"if [ ! -f "{state_dir}/supervisor.json" ] || [ ! -f "{state_dir}/state.db" ] || [ ! -f "{state_dir}/supervisor.lock" ]; then
    echo "stale files were deleted before threadlane serve startup" >&2
    exit 91
fi
"#,
                state_dir = self.state_dir.display()
            )
        } else {
            String::new()
        };

        let script = format!(
            r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
ARGV_FILE="{argv_file}"
PIDS_FILE="{pids_file}"
c=0
if [ -f "$COUNT_FILE" ]; then
    read -r c < "$COUNT_FILE" || c=0
fi
c=$((c + 1))
printf "%s\n" "$c" > "$COUNT_FILE"
printf "%s\n" "$$" >> "$PIDS_FILE"
: > "$ARGV_FILE"
for arg in "$@"; do
    printf "%s\n" "$arg" >> "$ARGV_FILE"
done
{stale_assertion}export {mode_env}="real_serve"
export {argv_env}="$ARGV_FILE"
export {keep_env}="{keep_alive}"
export {delay_env}="{delay_ms}"
exec "{test_bin}" --exact synthetic_control_runtime_child_entrypoint --nocapture >/dev/null 2>&1
"#,
            count_file = count_file.display(),
            argv_file = argv_file.display(),
            pids_file = self.pids_file.display(),
            stale_assertion = stale_assertion,
            mode_env = CHILD_MODE_ENV,
            argv_env = CHILD_ARGV_FILE_ENV,
            keep_env = CHILD_KEEP_ALIVE_FILE_ENV,
            keep_alive = self.keep_alive_file.display(),
            delay_env = CHILD_DELAY_MS_ENV,
            delay_ms = delay_ms,
            test_bin = test_bin.display(),
        );

        write_executable_script(&script_path, &script);
        script_path
    }

    /// Writes a synthetic executable script that `exec`s `synthetic_control_runtime_child_entrypoint`
    /// in `"exit_with_descendant"` or `"hang_with_descendant"` mode without requiring `sleep` in `PATH`.
    fn write_group_behavior_wrapper(
        &self,
        name: &str,
        mode: &str,
        leader_pid_file: &Path,
        descendant_pid_file: &Path,
        ignore_sigterm: bool,
    ) -> PathBuf {
        let script_path = self.bin_dir.join(name);
        let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
            .expect("canonicalize current_exe");
        let ignore_val = if ignore_sigterm { "1" } else { "0" };
        let script = format!(
            r#"#!/bin/sh
set -eu
printf "%s\n" "$$" >> "{pids_file}"
export {mode_env}="{mode}"
export {leader_env}="{leader_file}"
export {desc_env}="{desc_file}"
export {ignore_env}="{ignore_val}"
exec "{test_bin}" --exact synthetic_control_runtime_child_entrypoint --nocapture >/dev/null 2>&1
"#,
            pids_file = self.pids_file.display(),
            mode_env = CHILD_MODE_ENV,
            mode = mode,
            leader_env = CHILD_LEADER_PID_FILE_ENV,
            leader_file = leader_pid_file.display(),
            desc_env = CHILD_DESCENDANT_PID_FILE_ENV,
            desc_file = descendant_pid_file.display(),
            ignore_env = CHILD_IGNORE_SIGTERM_ENV,
            ignore_val = ignore_val,
            test_bin = test_bin.display(),
        );
        write_executable_script(&script_path, &script);
        script_path
    }
}

impl Drop for ControlRuntimeTestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.keep_alive_file);
        let mut all_pids = self.tracked_pids.clone();
        if let Ok(raw) = fs::read_to_string(&self.pids_file) {
            for line in raw.lines() {
                if let Ok(pid) = line.trim().parse::<u32>() {
                    if pid > 1 {
                        all_pids.push(pid);
                    }
                }
            }
        }
        for pid in all_pids {
            kill_owned_test_pid_and_group(pid);
        }
    }
}

fn write_executable_script(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create script parent");
    }
    let mut file = File::create(path).expect("create script");
    file.write_all(body.as_bytes()).expect("write script");
    file.sync_all().expect("sync script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod 0755 script");
}

fn read_invocation_count(path: &Path) -> usize {
    match fs::read_to_string(path) {
        Ok(s) => s.trim().parse::<usize>().unwrap_or(0),
        Err(_) => 0,
    }
}

#[allow(unsafe_code)]
fn kill_owned_test_pid_and_group(pid: u32) {
    let Ok(pid_i32) = i32::try_from(pid) else {
        return;
    };
    if pid_i32 <= 1 || pid == std::process::id() {
        return;
    }
    unsafe {
        let pgid = libc::getpgid(pid_i32);
        let caller_pgid = libc::getpgrp();
        if pgid > 1 && pgid != caller_pgid {
            let _ = libc::kill(-pgid, libc::SIGTERM);
        }
        let _ = libc::kill(pid_i32, libc::SIGTERM);
    }
    for _ in 0..15 {
        if !is_process_alive(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    unsafe {
        let pgid = libc::getpgid(pid_i32);
        let caller_pgid = libc::getpgrp();
        if pgid > 1 && pgid != caller_pgid {
            let _ = libc::kill(-pgid, libc::SIGKILL);
        }
        let _ = libc::kill(pid_i32, libc::SIGKILL);
    }
}

#[allow(unsafe_code)]
fn process_pgid(pid: u32) -> i32 {
    let pid_i32 = i32::try_from(pid).expect("pid fits i32");
    unsafe { libc::getpgid(pid_i32) }
}

/// 1. Existing healthy authenticated supervisor is reused without spawning another process, and
///    reports `None` (unverified) for `notifications_enabled` and `workers_enabled`.
#[tokio::test]
async fn existing_healthy_supervisor_reused_without_spawning_and_preserves_state() {
    let fixture = ControlRuntimeTestFixture::new();

    let handle = Supervisor::start(SupervisorConfig::new(&fixture.state_dir))
        .await
        .expect("start in-process Supervisor");
    let client =
        SupervisorClient::from_state_dir(&fixture.state_dir).expect("client from state_dir");
    let _b1 = client
        .create_binding(TEST_THREAD_UUID_A, "chat-alpha")
        .await
        .expect("create binding alpha");
    let _b2 = client
        .create_binding(TEST_THREAD_UUID_B, "chat-beta")
        .await
        .expect("create binding beta");

    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    let descriptor_before = fs::read(&descriptor_path).expect("read supervisor.json before");

    let count_file = fixture.root.join("reuse-count.txt");
    let argv_file = fixture.root.join("reuse-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    let outcome = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("reuse existing healthy supervisor");

    assert_eq!(outcome.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(outcome.status.as_str(), "already_running");
    assert_eq!(outcome.state_dir, fixture.state_dir);
    assert_eq!(outcome.pid, std::process::id());
    assert_eq!(outcome.port, handle.descriptor().port);
    assert_eq!(outcome.bindings_count, 2);
    assert!(!outcome.stale_descriptor_recovered);
    assert_eq!(
        outcome.notifications_enabled, None,
        "reused existing supervisor must report None (unverified) for notifications_enabled"
    );
    assert_eq!(
        outcome.workers_enabled, None,
        "reused existing supervisor must report None (unverified) for workers_enabled"
    );
    assert!(!outcome.approved_hosts_resumed);
    assert_eq!(
        outcome.in_flight_runs_recovery_policy,
        CONTROL_RUNTIME_IN_FLIGHT_RECOVERY_POLICY
    );
    assert_eq!(
        outcome.process_lifetime_policy,
        CONTROL_RUNTIME_LIFETIME_POLICY
    );
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "healthy existing supervisor must never spawn a new process"
    );
    assert_eq!(
        fs::read(&descriptor_path).expect("read supervisor.json after"),
        descriptor_before,
        "supervisor.json must remain byte-for-byte unchanged on reuse"
    );

    handle.shutdown().await;
}

/// 2. Fresh start spawns exact `serve --state-dir <dir>` scope with a cleared environment
///    (`env_clear()`), reports `Some(false)` for `notifications_enabled` and `workers_enabled`,
///    runs in an isolated background process group, and subsequent calls reuse it (`None`).
#[tokio::test]
async fn fresh_start_exact_serve_scope_env_scrubbing_and_background_survival() {
    let mut fixture = ControlRuntimeTestFixture::new();

    let count_file = fixture.root.join("fresh-count.txt");
    let argv_file = fixture.root.join("fresh-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    let policy = ControlRuntimePolicy::new(&fixture.state_dir, &spy_exe, Duration::from_secs(5))
        .expect("construct ControlRuntimePolicy");
    assert_eq!(
        policy.serve_args(),
        build_control_runtime_serve_args(&fixture.state_dir)
    );

    let started = policy.ensure().await.expect("start fresh control runtime");
    fixture.track_pid(started.pid);

    assert_eq!(started.status, ControlRuntimeEnsureStatus::Started);
    assert_eq!(started.status.as_str(), "started");
    assert_eq!(started.state_dir, fixture.state_dir);
    assert_ne!(started.pid, std::process::id());
    assert!(started.port > 0);
    assert_eq!(started.bindings_count, 0);
    assert!(!started.stale_descriptor_recovered);
    assert_eq!(started.notifications_enabled, Some(false));
    assert_eq!(started.workers_enabled, Some(false));
    assert!(!started.approved_hosts_resumed);
    assert_eq!(read_invocation_count(&count_file), 1);

    let recorded_args: Vec<String> = fs::read_to_string(&argv_file)
        .expect("read recorded argv")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(
        recorded_args,
        vec![
            "serve".to_string(),
            "--state-dir".to_string(),
            fixture.state_dir.display().to_string(),
        ],
        "must pass only `serve --state-dir <dir>` with notifications, workers, and approved hosts disabled"
    );

    // Verify `cmd.env_clear()` scrubbed ambient parent environment variables from the spawned child.
    assert!(
        std::env::var_os("CARGO_MANIFEST_DIR").is_some(),
        "parent test process has CARGO_MANIFEST_DIR in its environment"
    );
    let child_environ_bytes = fs::read(format!("/proc/{}/environ", started.pid))
        .expect("read /proc/<started.pid>/environ");
    let child_environ = String::from_utf8_lossy(&child_environ_bytes);
    assert!(
        !child_environ.contains("CARGO_MANIFEST_DIR="),
        "spawned supervisor must not inherit ambient parent environment variables: {child_environ:?}"
    );

    // Verify isolated process group (`pgid == started.pid != caller_pgid`).
    let child_pgid = process_pgid(started.pid);
    let caller_pgid = process_pgid(std::process::id());
    assert_eq!(
        child_pgid, started.pid as i32,
        "started control supervisor must be leader of its own process group"
    );
    assert_ne!(child_pgid, caller_pgid);

    // Dropping `policy` leaves the dedicated background control process running and authenticated.
    drop(policy);
    assert!(is_process_alive(started.pid));

    let client = SupervisorClient::from_state_dir(&fixture.state_dir)
        .expect("connect to started supervisor");
    let created = client
        .create_binding(TEST_THREAD_UUID_A, "persisted-after-start")
        .await
        .expect("create binding on started supervisor");
    assert_eq!(created.thread_uuid, TEST_THREAD_UUID_A);

    // A second `ensure_control_runtime` call reuses the running background process without spawning.
    let reused = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("second ensure reuses started supervisor");
    assert_eq!(reused.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(reused.pid, started.pid);
    assert_eq!(reused.port, started.port);
    assert_eq!(reused.bindings_count, 1);
    assert_eq!(reused.notifications_enabled, None);
    assert_eq!(reused.workers_enabled, None);
    assert_eq!(read_invocation_count(&count_file), 1);
}

/// 3. Stale descriptor (dead PID, unlocked `supervisor.lock`) is recovered strictly via normal
///    locked `Supervisor` startup without deleting `supervisor.json`, `state.db`, or
///    `supervisor.lock`, and pre-existing in-flight worker runs are marked `Unknown` without
///    re-dispatch.
#[tokio::test]
async fn stale_descriptor_recovered_via_locked_startup_and_marks_in_flight_workers_unknown() {
    let mut fixture = ControlRuntimeTestFixture::new();

    // 1. Seed `state.db` and `supervisor.lock` with a binding and a non-terminal worker run.
    let initial_handle = Supervisor::start(SupervisorConfig::new(&fixture.state_dir))
        .await
        .expect("start initial supervisor to create state.db and supervisor.lock");
    let initial_port = initial_handle.descriptor().port;
    initial_handle.shutdown().await;

    let db_path = fixture.state_dir.join(DATABASE_FILENAME);
    let lock_path = fixture.state_dir.join(LOCK_FILENAME);
    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);

    let (binding_id, worker_run_id) = {
        let mut core = Core::open(&db_path).expect("open state.db directly");
        let binding = core
            .create_binding(TEST_THREAD_UUID_A, "stale-recovery-binding")
            .expect("create binding");
        let profile_digest = "a".repeat(64);
        let base_commit = "b".repeat(40);
        let instruction = "Do not re-dispatch after supervisor restart";
        let execution_binding = WorkerExecutionBinding::new(
            &profile_digest,
            instruction,
            &"c".repeat(64),
            &"d".repeat(64),
            &base_commit,
        )
        .expect("build valid WorkerExecutionBinding");
        let out = core
            .execute(
                &binding.binding_id,
                CoreCommand::StartWorker {
                    command_key: "cmd-seed-worker-1".to_string(),
                    profile_id: "agy-reviewer".to_string(),
                    profile_digest,
                    provider: "antigravity".to_string(),
                    requested_model: "gemini-4-argon-high".to_string(),
                    requested_effort: "high".to_string(),
                    base_commit,
                    execution_binding: Some(execution_binding),
                    instruction: instruction.to_string(),
                    now_ms: 1_700_000_000_100,
                },
            )
            .expect("seed queued worker run");
        let CommandOutput::Worker(worker_snap) = out else {
            panic!("expected CommandOutput::Worker");
        };
        assert_eq!(worker_snap.status, WorkerStatus::Queued);
        (binding.binding_id, worker_snap.run_id)
    };
    threadlane::descriptor::enforce_database_permissions(&fixture.state_dir)
        .expect("enforce 0600 on state.db");

    // Spawn and immediately reap a short-lived process to obtain a provably dead OS PID.
    let mut dead_child = std::process::Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .spawn()
        .expect("spawn short-lived process");
    let dead_pid = dead_child.id();
    let _ = dead_child.wait().expect("reap short-lived process");
    assert!(!is_process_alive(dead_pid));

    // Write a stale `supervisor.json` with `dead_pid` and `0600` permissions.
    let stale_descriptor = RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".to_string(),
        port: initial_port,
        pid: dead_pid,
        token: SecretToken::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
    };
    threadlane::descriptor::write_descriptor_atomic(&fixture.state_dir, &stale_descriptor)
        .expect("write stale supervisor.json");

    let db_meta_before = fs::symlink_metadata(&db_path).expect("stat state.db before");
    let lock_meta_before = fs::symlink_metadata(&lock_path).expect("stat supervisor.lock before");
    assert!(descriptor_path.exists());

    // The wrapper script asserts (`require_preexisting_stale_files = true`) that `supervisor.json`,
    // `state.db`, and `supervisor.lock` all still exist on disk when `threadlane serve` starts.
    let count_file = fixture.root.join("stale-count.txt");
    let argv_file = fixture.root.join("stale-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, true);

    let outcome = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(5))
        .await
        .expect("recover from stale supervisor.json via locked startup");
    fixture.track_pid(outcome.pid);

    assert_eq!(outcome.status, ControlRuntimeEnsureStatus::Started);
    assert!(outcome.stale_descriptor_recovered);
    assert_eq!(outcome.notifications_enabled, Some(false));
    assert_eq!(outcome.workers_enabled, Some(false));
    assert_ne!(outcome.pid, dead_pid);
    assert!(is_process_alive(outcome.pid));
    assert_eq!(outcome.bindings_count, 1);
    assert_eq!(read_invocation_count(&count_file), 1);

    // Verify `state.db` and `supervisor.lock` were never deleted/recreated (`(dev, ino)` preserved).
    let db_meta_after = fs::symlink_metadata(&db_path).expect("stat state.db after");
    let lock_meta_after = fs::symlink_metadata(&lock_path).expect("stat supervisor.lock after");
    assert_eq!(
        (db_meta_after.dev(), db_meta_after.ino()),
        (db_meta_before.dev(), db_meta_before.ino()),
        "state.db must never be unlinked during stale descriptor recovery"
    );
    assert_eq!(
        (lock_meta_after.dev(), lock_meta_after.ino()),
        (lock_meta_before.dev(), lock_meta_before.ino()),
        "supervisor.lock must never be unlinked during stale descriptor recovery"
    );

    // Verify the pre-existing in-flight worker run was marked `Unknown` by normal Supervisor
    // startup and was never re-dispatched.
    let scoped = ScopedClient::from_state_dir(&fixture.state_dir, &binding_id)
        .expect("scoped client on recovered supervisor");
    let worker_after = scoped
        .get_worker(worker_run_id.clone())
        .await
        .expect("query worker after recovery");
    assert_eq!(
        worker_after.status,
        WorkerStatus::Unknown,
        "pre-existing non-terminal worker run must be marked Unknown on supervisor startup"
    );
    let run_after = scoped
        .get_run(worker_run_id)
        .await
        .expect("query run after recovery");
    assert_eq!(run_after.status, RunStatus::Unknown);
}

/// 4. Concurrent callers racing at startup (including first-use creation of a missing `state_dir`,
///    in-process callers, and cross-process callers) converge on a single authenticated supervisor
///    owner and never fail a benign directory creation race or produce two owners.
#[tokio::test]
async fn concurrent_ensure_callers_in_process_cross_process_and_first_use_missing_dir_race() {
    // Part A: First-use race on a missing `state_dir` across in-process and cross-process callers.
    let mut fixture_a = ControlRuntimeTestFixture::new();
    let missing_state_dir = fixture_a.root.join("first-use-missing-state");
    assert!(
        !missing_state_dir.exists(),
        "first-use state_dir must not exist before concurrent callers start"
    );

    let count_a = fixture_a.root.join("conc-a-count.txt");
    let argv_a = fixture_a.root.join("conc-a-argv.txt");
    let spy_exe_a = fixture_a.write_real_serve_wrapper("threadlane", &count_a, &argv_a, 90, false);

    let caller_out_a = fixture_a.root.join("caller-child-first-use.json");
    let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");

    let mut caller_child_a = tokio::process::Command::new(&test_bin)
        .args([
            "--exact",
            "synthetic_control_runtime_child_entrypoint",
            "--nocapture",
        ])
        .env(CHILD_MODE_ENV, "ensure_caller")
        .env(CHILD_CALLER_STATE_DIR_ENV, &missing_state_dir)
        .env(CHILD_CALLER_EXE_ENV, &spy_exe_a)
        .env(CHILD_CALLER_TIMEOUT_MS_ENV, "5000")
        .env(CHILD_CALLER_OUT_FILE_ENV, &caller_out_a)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn cross-process first-use ensure caller");

    let (r1, r2, r3, r4) = tokio::join!(
        ensure_control_runtime(&missing_state_dir, &spy_exe_a, Duration::from_secs(5)),
        ensure_control_runtime(&missing_state_dir, &spy_exe_a, Duration::from_secs(5)),
        ensure_control_runtime(&missing_state_dir, &spy_exe_a, Duration::from_secs(5)),
        ensure_control_runtime(&missing_state_dir, &spy_exe_a, Duration::from_secs(5)),
    );
    let child_exit_a = caller_child_a
        .wait()
        .await
        .expect("wait for caller_child_a");
    assert!(child_exit_a.success(), "caller_child_a must exit 0");

    let child_res_a: CallerChildResult =
        serde_json::from_slice(&fs::read(&caller_out_a).expect("read caller_out_a"))
            .expect("deserialize CallerChildResult");
    let CallerChildResult::Ok(child_outcome_a) = child_res_a else {
        panic!("expected cross-process first-use caller to succeed, got {child_res_a:?}");
    };

    let outcomes = [
        r1.expect("caller 1"),
        r2.expect("caller 2"),
        r3.expect("caller 3"),
        r4.expect("caller 4"),
        child_outcome_a,
    ];
    let winner_pid = outcomes[0].pid;
    let winner_port = outcomes[0].port;
    fixture_a.track_pid(winner_pid);

    let created_meta = fs::symlink_metadata(&missing_state_dir).expect("stat created state_dir");
    assert_eq!(
        created_meta.mode() & 0o777,
        0o700,
        "concurrently created missing state_dir must have 0700 permissions"
    );

    let started_count = outcomes
        .iter()
        .filter(|o| o.status == ControlRuntimeEnsureStatus::Started)
        .count();
    let reused_count = outcomes
        .iter()
        .filter(|o| o.status == ControlRuntimeEnsureStatus::AlreadyRunning)
        .count();
    assert_eq!(started_count, 1, "exactly one caller must report Started");
    assert_eq!(
        reused_count, 4,
        "remaining callers must report AlreadyRunning"
    );
    for o in &outcomes {
        assert_eq!(o.pid, winner_pid);
        assert_eq!(o.port, winner_port);
        if o.status == ControlRuntimeEnsureStatus::Started {
            assert_eq!(o.notifications_enabled, Some(false));
            assert_eq!(o.workers_enabled, Some(false));
        } else {
            assert_eq!(o.notifications_enabled, None);
            assert_eq!(o.workers_enabled, None);
        }
    }

    // Part B: Direct OS thread race on `validate_control_runtime_state_dir` for a missing directory.
    let race_dir = fixture_a.root.join("thread-race-missing-dir");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(6));
    let mut handles = Vec::new();
    for _ in 0..6 {
        let dir_clone = race_dir.clone();
        let bar_clone = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar_clone.wait();
            validate_control_runtime_state_dir(&dir_clone)
        }));
    }
    for h in handles {
        let validated = h
            .join()
            .expect("join thread")
            .expect("concurrent validate_control_runtime_state_dir must handle AlreadyExists");
        assert_eq!(validated, race_dir);
    }
}

/// 5. Ambiguous live process, `401 Unauthorized`, and occupied `supervisor.lock` refuse takeover
///    without killing pre-existing processes or spawning a duplicate supervisor.
#[tokio::test]
async fn ambiguous_live_process_unauthorized_and_occupied_lock_refuse_takeover() {
    let mut fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("refuse-count.txt");
    let argv_file = fixture.root.join("refuse-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    // 5a. Live PID in `supervisor.json`, but `supervisor.lock` is NOT held -> `AmbiguousLiveSupervisor`.
    let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");
    let mut sleeper = std::process::Command::new(&test_bin)
        .args([
            "--exact",
            "synthetic_control_runtime_child_entrypoint",
            "--nocapture",
        ])
        .env(CHILD_MODE_ENV, "sleep_only")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn benign live process");
    let live_pid = sleeper.id();
    fixture.track_pid(live_pid);

    let unlocked_live_desc = RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".to_string(),
        port: 65_431,
        pid: live_pid,
        token: SecretToken::new("1111111111111111111111111111111111111111111111111111111111111111"),
    };
    threadlane::descriptor::write_descriptor_atomic(&fixture.state_dir, &unlocked_live_desc)
        .expect("write unlocked live descriptor");

    let err_unlocked =
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(400))
            .await
            .expect_err("unlocked live PID must refuse takeover");
    assert_eq!(
        err_unlocked.error_code(),
        ControlRuntimeErrorCode::AmbiguousLiveSupervisor
    );
    assert_eq!(read_invocation_count(&count_file), 0);
    assert!(
        is_process_alive(live_pid),
        "must never kill a pre-existing process on takeover refusal"
    );
    let _ = sleeper.kill();
    let _ = sleeper.wait();

    // 5b. Live PID holding `supervisor.lock`, but TCP port is unreachable/closed -> `AmbiguousLiveSupervisor`.
    let closed_port = {
        let ephem = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        ephem.local_addr().expect("local_addr").port()
    };
    let held_lock = StateLock::acquire(&fixture.state_dir).expect("acquire supervisor.lock");
    let unreachable_desc = RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".to_string(),
        port: closed_port,
        pid: std::process::id(),
        token: SecretToken::new("2222222222222222222222222222222222222222222222222222222222222222"),
    };
    threadlane::descriptor::write_descriptor_atomic(&fixture.state_dir, &unreachable_desc)
        .expect("write unreachable live descriptor");

    let err_unreachable =
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(400))
            .await
            .expect_err("live locked PID with unreachable port must refuse takeover");
    assert_eq!(
        err_unreachable.error_code(),
        ControlRuntimeErrorCode::AmbiguousLiveSupervisor
    );
    assert_eq!(read_invocation_count(&count_file), 0);
    drop(held_lock);

    // 5c. Occupied `supervisor.lock` with no `supervisor.json` that never becomes ready -> `LockBusy`.
    fs::remove_file(fixture.state_dir.join(DESCRIPTOR_FILENAME)).expect("remove descriptor");
    let occupied_lock = StateLock::acquire(&fixture.state_dir).expect("acquire occupied lock");
    let started_wait = Instant::now();
    let err_busy = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(250))
        .await
        .expect_err("occupied supervisor.lock must time out with LockBusy");
    assert_eq!(err_busy.error_code(), ControlRuntimeErrorCode::LockBusy);
    assert!(started_wait.elapsed() < Duration::from_millis(1_500));
    assert_eq!(read_invocation_count(&count_file), 0);
    drop(occupied_lock);

    // 5d. Live authenticated supervisor rejects mismatched bearer token with `401 Unauthorized` ->
    //     `UnauthorizedSupervisor` (and never kills the running supervisor).
    let handle = Supervisor::start(SupervisorConfig::new(&fixture.state_dir))
        .await
        .expect("start real supervisor for 401 test");
    let wrong_token_desc = RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".to_string(),
        port: handle.descriptor().port,
        pid: handle.descriptor().pid,
        token: SecretToken::new("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
    };
    threadlane::descriptor::write_descriptor_atomic(&fixture.state_dir, &wrong_token_desc)
        .expect("overwrite supervisor.json with wrong token");

    let err_unauth =
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(500))
            .await
            .expect_err("mismatched bearer token must fail with UnauthorizedSupervisor");
    assert_eq!(
        err_unauth.error_code(),
        ControlRuntimeErrorCode::UnauthorizedSupervisor
    );
    assert!(
        !err_unauth
            .message()
            .contains("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        "error message must never leak bearer tokens"
    );
    assert_eq!(read_invocation_count(&count_file), 0);
    handle.shutdown().await;

    // 5e. Hanging TCP listener on a live locked `supervisor.json` enforces the bounded caller
    //     timeout and fails closed with `AmbiguousLiveSupervisor` and zero spawns.
    let hanging_listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind hanging listener");
    let hanging_port = hanging_listener.local_addr().expect("local_addr").port();
    hanging_listener
        .set_nonblocking(true)
        .expect("set_nonblocking");
    let tokio_listener =
        tokio::net::TcpListener::from_std(hanging_listener).expect("tokio TcpListener");
    let hold_task = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = tokio_listener.accept().await {
            held.push(sock);
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    let hanging_lock = StateLock::acquire(&fixture.state_dir).expect("acquire hanging lock");
    let hanging_desc = RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".to_string(),
        port: hanging_port,
        pid: std::process::id(),
        token: SecretToken::new("3333333333333333333333333333333333333333333333333333333333333333"),
    };
    threadlane::descriptor::write_descriptor_atomic(&fixture.state_dir, &hanging_desc)
        .expect("write hanging descriptor");

    let hang_probe_started = Instant::now();
    let err_hang_probe =
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(250))
            .await
            .expect_err("hanging live supervisor probe must time out and refuse takeover");
    assert_eq!(
        err_hang_probe.error_code(),
        ControlRuntimeErrorCode::AmbiguousLiveSupervisor
    );
    assert!(hang_probe_started.elapsed() < Duration::from_millis(1_500));
    assert_eq!(read_invocation_count(&count_file), 0);

    drop(hanging_lock);
    hold_task.abort();
}

/// 6. Invalid permissions, untrusted writable/symlink ancestors, symlinks, relative/traversal
///    paths, invalid executables, and corrupt descriptors fail closed with zero process spawns.
#[tokio::test]
async fn invalid_permissions_untrusted_ancestors_symlinks_executables_and_descriptors_fail_closed()
{
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("sec-count.txt");
    let argv_file = fixture.root.join("sec-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    // 6a. Timeout bounds.
    assert_eq!(
        validate_control_runtime_timeout(Duration::ZERO)
            .expect_err("zero timeout")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );
    assert_eq!(
        validate_control_runtime_timeout(Duration::from_secs(61))
            .expect_err("excessive timeout")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );

    // 6b. Relative and traversal `state_dir`.
    assert_eq!(
        validate_control_runtime_state_dir(Path::new("relative-state"))
            .expect_err("relative state_dir")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );
    assert_eq!(
        validate_control_runtime_state_dir(&fixture.root.join("a/../control-state"))
            .expect_err("traversal state_dir")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    // 6c. Symlink `state_dir` and symlink ancestor component.
    let symlink_state = fixture.root.join("symlink-state");
    symlink(&fixture.state_dir, &symlink_state).expect("create symlink state_dir");
    assert_eq!(
        ensure_control_runtime(&symlink_state, &spy_exe, Duration::from_secs(1))
            .await
            .expect_err("symlink state_dir must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    let symlink_parent = fixture.root.join("symlink-parent");
    symlink(&fixture.root, &symlink_parent).expect("create symlink parent");
    assert_eq!(
        validate_control_runtime_state_dir(&symlink_parent.join("control-state"))
            .expect_err("symlink ancestor component must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    // 6d. Untrusted group/world-writable non-sticky ancestor directory (`0777` parent) and
    //     missing child directly inside a sticky world-writable directory (`01777` parent).
    let writable_ancestor = fixture.root.join("writable-ancestor");
    fs::create_dir(&writable_ancestor).expect("create writable_ancestor");
    let child_under_writable = writable_ancestor.join("private-state");
    fs::create_dir(&child_under_writable).expect("create child_under_writable");
    fs::set_permissions(&child_under_writable, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 child_under_writable");
    fs::set_permissions(&writable_ancestor, fs::Permissions::from_mode(0o777))
        .expect("chmod 0777 writable_ancestor");
    assert_eq!(
        validate_control_runtime_state_dir(&child_under_writable)
            .expect_err("0777 non-sticky ancestor must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    fs::set_permissions(&writable_ancestor, fs::Permissions::from_mode(0o700))
        .expect("restore 0700 on writable_ancestor for cleanup");

    let sticky_parent = fixture.root.join("sticky-tmp-like");
    fs::create_dir(&sticky_parent).expect("create sticky_parent");
    fs::set_permissions(&sticky_parent, fs::Permissions::from_mode(0o1777))
        .expect("chmod 01777 sticky_parent");
    assert_eq!(
        validate_control_runtime_state_dir(&sticky_parent.join("missing-direct-child"))
            .expect_err("creating missing state_dir directly inside 01777 sticky parent must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    let existing_sticky_child = sticky_parent.join("existing-owned-child");
    fs::create_dir(&existing_sticky_child).expect("create existing_sticky_child");
    fs::set_permissions(&existing_sticky_child, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 existing_sticky_child");
    assert_eq!(
        validate_control_runtime_state_dir(&existing_sticky_child)
            .expect("existing 0700 child inside 01777 sticky parent must succeed"),
        existing_sticky_child
    );
    fs::set_permissions(&sticky_parent, fs::Permissions::from_mode(0o700))
        .expect("restore 0700 on sticky_parent for cleanup");

    // 6e. Insecure `0755` permissions on pre-existing `state_dir`.
    let insecure_dir = fixture.root.join("insecure-state");
    fs::create_dir(&insecure_dir).expect("create insecure_dir");
    fs::set_permissions(&insecure_dir, fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 insecure_dir");
    assert_eq!(
        ensure_control_runtime(&insecure_dir, &spy_exe, Duration::from_secs(1))
            .await
            .expect_err("0755 state_dir must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    // 6f. Non-empty `state_dir` with unrelated file and no Threadlane entries.
    let foreign_dir = fixture.root.join("foreign-state");
    fs::create_dir(&foreign_dir).expect("create foreign_dir");
    fs::set_permissions(&foreign_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 foreign_dir");
    fs::write(foreign_dir.join("unexpected.txt"), b"foreign").expect("write foreign file");
    assert_eq!(
        ensure_control_runtime(&foreign_dir, &spy_exe, Duration::from_secs(1))
            .await
            .expect_err("foreign non-empty state_dir must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    // 6g. Insecure `0644` permissions or symlink on `supervisor.json`.
    let desc_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    fs::write(&desc_path, b"{}").expect("write desc_path");
    fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o644))
        .expect("chmod 0644 supervisor.json");
    assert_eq!(
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(1))
            .await
            .expect_err("0644 supervisor.json must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    fs::remove_file(&desc_path).expect("remove 0644 supervisor.json");

    let target_file = fixture.root.join("outside-target.json");
    fs::write(&target_file, b"{}").expect("write outside target");
    symlink(&target_file, &desc_path).expect("symlink supervisor.json");
    assert_eq!(
        ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(1))
            .await
            .expect_err("symlink supervisor.json must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    fs::remove_file(&desc_path).expect("remove symlink supervisor.json");

    // 6h. Corrupt `0600` `supervisor.json` fails with `InvalidDescriptor` and is preserved untouched.
    let corrupt_bytes = b"{not-valid-descriptor-json\n";
    fs::write(&desc_path, corrupt_bytes).expect("write corrupt supervisor.json");
    fs::set_permissions(&desc_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 corrupt supervisor.json");
    let corrupt_err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(1))
        .await
        .expect_err("corrupt supervisor.json must fail closed");
    assert_eq!(
        corrupt_err.error_code(),
        ControlRuntimeErrorCode::InvalidDescriptor
    );
    assert_eq!(
        fs::read(&desc_path).expect("read corrupt supervisor.json after failure"),
        corrupt_bytes,
        "corrupt supervisor.json must never be deleted or overwritten"
    );
    fs::remove_file(&desc_path).expect("clean up corrupt supervisor.json");

    // 6i. Invalid executable paths (relative, traversal, missing, directory, non-executable,
    //     world-writable, untrusted writable ancestor, symlink).
    assert_eq!(
        validate_control_runtime_executable_path(Path::new("threadlane"))
            .expect_err("relative executable")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );
    assert_eq!(
        validate_control_runtime_executable_path(&fixture.bin_dir.join("../bin/threadlane"))
            .expect_err("traversal executable")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    assert_eq!(
        validate_control_runtime_executable_path(&fixture.bin_dir.join("does-not-exist"))
            .expect_err("missing executable")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );
    assert_eq!(
        validate_control_runtime_executable_path(&fixture.bin_dir)
            .expect_err("directory as executable")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );

    let non_exec = fixture.bin_dir.join("non-exec-threadlane");
    fs::write(&non_exec, b"#!/bin/sh\nexit 0\n").expect("write non_exec");
    fs::set_permissions(&non_exec, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
    assert_eq!(
        validate_control_runtime_executable_path(&non_exec)
            .expect_err("0600 non-executable")
            .error_code(),
        ControlRuntimeErrorCode::InvalidConfig
    );

    let world_writable_exec = fixture.bin_dir.join("world-writable-threadlane");
    fs::write(&world_writable_exec, b"#!/bin/sh\nexit 0\n").expect("write world_writable_exec");
    fs::set_permissions(&world_writable_exec, fs::Permissions::from_mode(0o777))
        .expect("chmod 0777");
    assert_eq!(
        validate_control_runtime_executable_path(&world_writable_exec)
            .expect_err("0777 world-writable executable")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    let writable_bin_dir = fixture.root.join("writable-bin");
    fs::create_dir(&writable_bin_dir).expect("create writable_bin_dir");
    let exe_in_writable_dir = writable_bin_dir.join("threadlane");
    write_executable_script(&exe_in_writable_dir, "#!/bin/sh\nexit 0\n");
    fs::set_permissions(&writable_bin_dir, fs::Permissions::from_mode(0o777))
        .expect("chmod 0777 writable_bin_dir");
    assert_eq!(
        validate_control_runtime_executable_path(&exe_in_writable_dir)
            .expect_err("executable inside 0777 directory must fail")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );
    fs::set_permissions(&writable_bin_dir, fs::Permissions::from_mode(0o700))
        .expect("restore 0700 writable_bin_dir");

    let symlink_exec = fixture.bin_dir.join("symlink-threadlane");
    symlink(&spy_exe, &symlink_exec).expect("create symlink executable");
    assert_eq!(
        validate_control_runtime_executable_path(&symlink_exec)
            .expect_err("symlink executable")
            .error_code(),
        ControlRuntimeErrorCode::SecurityViolation
    );

    assert_eq!(read_invocation_count(&count_file), 0);
}

/// 7. OS spawn failure, premature child exit, startup timeout, and caller future cancellation
///    (including cancellation mid-`terminate_and_reap`) terminate + reap the spawned child process
///    group (including descendants) with zero orphan/zombie leaks.
#[tokio::test]
async fn startup_timeout_failed_spawn_premature_exit_and_cancellation_cleanup_process_group() {
    let mut fixture = ControlRuntimeTestFixture::new();

    // 7a. OS-level `SpawnFailed` (missing script interpreter -> `ENOENT` at `execve`).
    let bad_interp_exe = fixture.bin_dir.join("bad-interp-threadlane");
    write_executable_script(
        &bad_interp_exe,
        "#!/nonexistent/threadlane/interpreter\nexit 0\n",
    );
    let spawn_err =
        ensure_control_runtime(&fixture.state_dir, &bad_interp_exe, Duration::from_secs(1))
            .await
            .expect_err("missing interpreter must fail at spawn");
    assert_eq!(spawn_err.error_code(), ControlRuntimeErrorCode::SpawnFailed);

    // 7b. `StartupProcessExited` with a background descendant in the spawned process group:
    //     both leader and descendant must be terminated and reaped before `ensure_control_runtime` returns.
    let exit_leader_pid_file = fixture.root.join("exit-leader-pid.txt");
    let exit_descendant_pid_file = fixture.root.join("exit-descendant-pid.txt");
    let exit_exe = fixture.write_group_behavior_wrapper(
        "exit-threadlane",
        "exit_with_descendant",
        &exit_leader_pid_file,
        &exit_descendant_pid_file,
        false,
    );

    let exit_err = ensure_control_runtime(&fixture.state_dir, &exit_exe, Duration::from_secs(2))
        .await
        .expect_err("prematurely exiting child must fail with StartupProcessExited");
    assert_eq!(
        exit_err.error_code(),
        ControlRuntimeErrorCode::StartupProcessExited
    );

    let exit_leader_pid: u32 = fs::read_to_string(&exit_leader_pid_file)
        .expect("read exit_leader_pid")
        .trim()
        .parse()
        .expect("parse exit_leader_pid");
    let exit_desc_pid: u32 = fs::read_to_string(&exit_descendant_pid_file)
        .expect("read exit_desc_pid")
        .trim()
        .parse()
        .expect("parse exit_desc_pid");
    fixture.track_pid(exit_leader_pid);
    fixture.track_pid(exit_desc_pid);

    assert!(
        !is_process_alive(exit_leader_pid),
        "exited leader must be reaped"
    );
    assert!(
        !is_process_alive(exit_desc_pid),
        "descendant in failed startup process group must be terminated (no orphan leak)"
    );
    assert!(
        !is_process_group_alive(exit_leader_pid as i32),
        "failed startup process group must have zero remaining live processes"
    );

    // 7c. `StartupTimedOut` when child hangs without publishing `supervisor.json` and spawns a
    //     descendant: both child and descendant must be killed and reaped on timeout.
    let hang_leader_pid_file = fixture.root.join("hang-leader-pid.txt");
    let hang_descendant_pid_file = fixture.root.join("hang-descendant-pid.txt");
    let hang_exe = fixture.write_group_behavior_wrapper(
        "hang-threadlane",
        "hang_with_descendant",
        &hang_leader_pid_file,
        &hang_descendant_pid_file,
        false,
    );

    let started = Instant::now();
    let timeout_err =
        ensure_control_runtime(&fixture.state_dir, &hang_exe, Duration::from_millis(250))
            .await
            .expect_err("hanging child must time out with StartupTimedOut");
    let elapsed = started.elapsed();

    assert_eq!(
        timeout_err.error_code(),
        ControlRuntimeErrorCode::StartupTimedOut
    );
    assert!(
        elapsed < Duration::from_millis(1_800),
        "timeout must be bounded (elapsed: {elapsed:?})"
    );

    let hang_leader_pid: u32 = fs::read_to_string(&hang_leader_pid_file)
        .expect("read hang_leader_pid")
        .trim()
        .parse()
        .expect("parse hang_leader_pid");
    let hang_desc_pid: u32 = fs::read_to_string(&hang_descendant_pid_file)
        .expect("read hang_desc_pid")
        .trim()
        .parse()
        .expect("parse hang_desc_pid");
    fixture.track_pid(hang_leader_pid);
    fixture.track_pid(hang_desc_pid);

    assert!(
        !is_process_alive(hang_leader_pid),
        "timed-out leader process must be terminated and reaped"
    );
    assert!(
        !is_process_alive(hang_desc_pid),
        "timed-out descendant process must be terminated and reaped"
    );
    assert!(
        !is_process_group_alive(hang_leader_pid as i32),
        "timed-out process group must have zero remaining live processes"
    );
    assert!(
        !Path::new(&format!("/proc/{hang_leader_pid}/stat")).exists(),
        "timed-out leader process must be fully reaped (no zombie entry in /proc)"
    );

    // 7d. Caller future cancellation during `terminate_and_reap` (child ignores SIGTERM so
    //     `terminate_and_reap` suspends at `.await` during its 150ms grace period; outer timeout
    //     drops the `ensure_control_runtime` future mid-cleanup): `SpawnedChildGuard::drop` must
    //     NOT be disarmed before `.await` and must still SIGKILL and reap both leader and descendant.
    let cancel_leader_pid_file = fixture.root.join("cancel-leader-pid.txt");
    let cancel_descendant_pid_file = fixture.root.join("cancel-descendant-pid.txt");
    let cancel_exe = fixture.write_group_behavior_wrapper(
        "cancel-threadlane",
        "hang_with_descendant",
        &cancel_leader_pid_file,
        &cancel_descendant_pid_file,
        true,
    );

    let outer_res = tokio::time::timeout(
        Duration::from_millis(410),
        ensure_control_runtime(&fixture.state_dir, &cancel_exe, Duration::from_millis(350)),
    )
    .await;
    assert!(
        outer_res.is_err(),
        "outer timeout must cancel ensure_control_runtime while terminate_and_reap is awaiting"
    );

    let cancel_leader_pid: u32 = fs::read_to_string(&cancel_leader_pid_file)
        .expect("read cancel_leader_pid")
        .trim()
        .parse()
        .expect("parse cancel_leader_pid");
    let cancel_desc_pid: u32 = fs::read_to_string(&cancel_descendant_pid_file)
        .expect("read cancel_desc_pid")
        .trim()
        .parse()
        .expect("parse cancel_desc_pid");
    fixture.track_pid(cancel_leader_pid);
    fixture.track_pid(cancel_desc_pid);

    // Drop sends SIGKILL synchronously; process exit and asynchronous reaping are observed
    // separately, so allow a bounded interval for the kernel/runtime to finish both.
    let cleanup_deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < cleanup_deadline
        && (is_process_alive(cancel_leader_pid) || is_process_alive(cancel_desc_pid))
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !is_process_alive(cancel_leader_pid),
        "Drop cleanup on future cancellation during terminate_and_reap must kill and reap leader"
    );
    assert!(
        !is_process_alive(cancel_desc_pid),
        "Drop cleanup on future cancellation during terminate_and_reap must kill descendant"
    );
    assert!(
        !is_process_group_alive(cancel_leader_pid as i32),
        "cancelled startup process group must have zero remaining live processes"
    );
}

/// 8. Deterministic caller-exit survival: a dedicated control supervisor started by a separate
///    caller OS process survives after that caller OS process exits, remains in its own process
///    group, and continues serving authenticated requests.
#[tokio::test]
async fn started_control_supervisor_survives_caller_process_exit() {
    let mut fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("survive-count.txt");
    let argv_file = fixture.root.join("survive-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    let caller_out_file = fixture.root.join("survive-caller-out.json");
    let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");

    let mut caller_child = tokio::process::Command::new(&test_bin)
        .args([
            "--exact",
            "synthetic_control_runtime_child_entrypoint",
            "--nocapture",
        ])
        .env(CHILD_MODE_ENV, "ensure_caller")
        .env(CHILD_CALLER_STATE_DIR_ENV, &fixture.state_dir)
        .env(CHILD_CALLER_EXE_ENV, &spy_exe)
        .env(CHILD_CALLER_TIMEOUT_MS_ENV, "5000")
        .env(CHILD_CALLER_OUT_FILE_ENV, &caller_out_file)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn independent ensure_caller process");
    let caller_pid = caller_child.id().expect("caller_child pid");

    let caller_exit = caller_child.wait().await.expect("wait for caller_child");
    assert!(caller_exit.success(), "caller_child must exit 0");
    assert!(
        !is_process_alive(caller_pid),
        "caller OS process must be completely exited and reaped"
    );

    let caller_res: CallerChildResult =
        serde_json::from_slice(&fs::read(&caller_out_file).expect("read caller_out_file"))
            .expect("deserialize CallerChildResult");
    let CallerChildResult::Ok(started) = caller_res else {
        panic!("expected caller_child ensure_control_runtime to succeed, got {caller_res:?}");
    };
    fixture.track_pid(started.pid);

    assert_eq!(started.status, ControlRuntimeEnsureStatus::Started);
    assert_eq!(started.notifications_enabled, Some(false));
    assert_eq!(started.workers_enabled, Some(false));
    assert_ne!(started.pid, caller_pid);
    assert!(
        is_process_alive(started.pid),
        "started control supervisor must survive after its caller OS process exits"
    );
    assert_eq!(
        process_pgid(started.pid),
        started.pid as i32,
        "surviving control supervisor remains leader of its own process group"
    );

    // Verify the surviving supervisor serves authenticated requests after caller exit.
    let client = SupervisorClient::from_state_dir(&fixture.state_dir)
        .expect("connect to surviving supervisor");
    let binding = client
        .create_binding(TEST_THREAD_UUID_A, "survived-caller-exit")
        .await
        .expect("create binding on surviving supervisor");
    assert_eq!(binding.thread_uuid, TEST_THREAD_UUID_A);

    // Subsequent `ensure_control_runtime` reuses the surviving supervisor without spawning again.
    let reused = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("reuse surviving supervisor");
    assert_eq!(reused.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(reused.pid, started.pid);
    assert_eq!(reused.port, started.port);
    assert_eq!(reused.bindings_count, 1);
    assert_eq!(reused.notifications_enabled, None);
    assert_eq!(reused.workers_enabled, None);
    assert_eq!(read_invocation_count(&count_file), 1);
}
