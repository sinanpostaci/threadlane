//! Focused synthetic integration tests for `threadlane_native::control_runtime`.
//!
//! All tests run against isolated `0700` temporary directories and owned synthetic/in-process
//! supervisor processes. No user state, credentials, or ambient system services are touched.

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use clap::Parser;
use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use threadlane::cli::{
    build_supervisor_config_with_workers, Cli, Subcommands as ThreadlaneCliSubcommands,
};
use threadlane::descriptor::{
    remove_owned_descriptor, write_descriptor_atomic, RuntimeDescriptor, SecretToken, StateLock,
    DATABASE_FILENAME, DESCRIPTOR_FILENAME, LOCK_FILENAME,
};
use threadlane::protocol::{
    ControlRuntimeCapabilities, ControlRuntimeInfoResponse, ControlRuntimeRetireRequest,
    ControlRuntimeRetireState, ControlRuntimeRole, ControlRuntimeRuntimeState,
    CONTROL_RUNTIME_PROTOCOL_VERSION,
};
use threadlane::{
    ApiError, ErrorCode as ThreadlaneErrorCode, ScopedClient, Supervisor, SupervisorClient,
    SupervisorConfig, DEFAULT_CODEX_TIMEOUT_MS, DEFAULT_TICK_INTERVAL_MS,
};
use threadlane_core::{
    BacklogItemKind, BacklogStatus, Command as CoreCommand, CommandOutput, Core, Run, RunStatus,
    WorkerExecutionBinding, WorkerStatus,
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

const DEMO_COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Answers every active demo question at the run's current revision and waits for completion
/// within a bounded deadline. Demo runs pause at `WaitingInput` even with a zero step delay, so
/// polling alone can never reach `Completed`.
async fn complete_demo_run(scoped: &ScopedClient, started: Run, label: &str) -> Run {
    let deadline = Instant::now() + DEMO_COMPLETION_TIMEOUT;
    let mut run = started;
    let mut answer_index = 0usize;

    loop {
        match run.status {
            RunStatus::Completed => return run,
            RunStatus::WaitingInput => {
                let question_id = run.active_question_id.clone().unwrap_or_else(|| {
                    panic!(
                        "{label}: waiting demo run {} has no active question",
                        run.run_id
                    )
                });
                let run_id = run.run_id.clone();
                let command_key = format!("{label}-answer-{answer_index}");
                answer_index += 1;
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "{label}: demo run {run_id} did not complete before the deadline"
                );
                run = tokio::time::timeout(
                    remaining,
                    scoped.answer(
                        command_key,
                        run_id.clone(),
                        question_id,
                        run.revision,
                        format!("{label} answer {answer_index}"),
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("{label}: timed out answering demo run {run_id}"))
                .unwrap_or_else(|err| panic!("{label}: failed to answer demo run {run_id}: {err}"));
                continue;
            }
            RunStatus::Failed | RunStatus::Cancelled | RunStatus::TimedOut | RunStatus::Unknown => {
                panic!(
                    "{label}: demo run {} entered terminal status {:?}",
                    run.run_id, run.status
                );
            }
            _ => {}
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: demo run {} did not complete before the deadline",
            run.run_id
        );
        tokio::time::sleep(Duration::from_millis(10).min(remaining)).await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: demo run {} did not complete before the deadline",
            run.run_id
        );
        let run_id = run.run_id.clone();
        run = tokio::time::timeout(remaining, scoped.get_run(run_id.clone()))
            .await
            .unwrap_or_else(|_| panic!("{label}: timed out polling demo run {run_id}"))
            .unwrap_or_else(|err| panic!("{label}: failed to poll demo run {run_id}: {err}"));
    }
}

/// Pulls and acknowledges every returned event batch within a bounded deadline so the seeded
/// runtime is genuinely idle before an upgrade assertion.
async fn drain_and_ack_events(scoped: &ScopedClient, label: &str) -> usize {
    let deadline = Instant::now() + EVENT_DRAIN_TIMEOUT;
    let mut acked_events = 0usize;
    let mut round = 0usize;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: timed out draining event batches"
        );
        let batch = tokio::time::timeout(remaining, scoped.pull_events(None, Some(50)))
            .await
            .unwrap_or_else(|_| panic!("{label}: timed out pulling event batch {round}"))
            .unwrap_or_else(|err| panic!("{label}: failed to pull event batch {round}: {err}"));

        let is_empty = batch.events.is_empty();
        acked_events += batch.events.len();
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "{label}: timed out acknowledging event batch {round}"
        );
        tokio::time::timeout(
            remaining,
            scoped.ack_events(format!("{label}-ack-{round}"), batch.batch_id),
        )
        .await
        .unwrap_or_else(|_| panic!("{label}: timed out acknowledging event batch {round}"))
        .unwrap_or_else(|err| panic!("{label}: failed to acknowledge event batch {round}: {err}"));

        if is_empty {
            return acked_events;
        }
        round += 1;
    }
}

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
                tokio::select! {
                    _ = handle.wait_for_stop_request() => {}
                    _ = async {
                        let safety_deadline = Instant::now() + Duration::from_secs(25);
                        while Instant::now() < safety_deadline && keep_alive_file.exists() {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    } => {}
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

    /// Writes a synthetic `threadlane` executable script in `self.bin_dir` that records each
    /// invocation with an atomic append, records that invocation's exact argv in a PID-scoped file,
    /// and then `exec`s this test binary's `synthetic_control_runtime_child_entrypoint` in
    /// `"real_serve"` mode so the child process keeps the exact spawned PID and PGID and runs a
    /// real `Supervisor::start`. PID-scoped argv files live under the fixture `TempDir` and are
    /// removed with it, so cleanup stays bounded and owned.
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
ARGV_INDEX_FILE="${{ARGV_FILE}}.index"
PIDS_FILE="{pids_file}"
umask 077
printf "launch\n" >> "$COUNT_FILE"
printf "%s\n" "$$" >> "$PIDS_FILE"
invocation_argv="${{ARGV_FILE}}.$$"
printf "%s\n" "$invocation_argv" >> "$ARGV_INDEX_FILE"
: > "$invocation_argv"
for arg in "$@"; do
    printf "%s\n" "$arg" >> "$invocation_argv"
done
{stale_assertion}export {mode_env}="real_serve"
export {argv_env}="$invocation_argv"
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
        Ok(contents) => contents
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        Err(_) => 0,
    }
}

/// Reads the exact recorded argv of the most recent synthetic wrapper invocation.
fn read_recorded_argv(argv_file: &Path) -> Vec<String> {
    let mut index_os = argv_file.as_os_str().to_os_string();
    index_os.push(".index");
    let index_path = PathBuf::from(index_os);
    let index = fs::read_to_string(&index_path)
        .unwrap_or_else(|err| panic!("read argv index `{}`: {err}", index_path.display()));
    let latest = index
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| panic!("argv index `{}` is empty", index_path.display()));
    let recorded = fs::read_to_string(latest)
        .unwrap_or_else(|err| panic!("read recorded child argv `{latest}`: {err}"));
    recorded.lines().map(str::to_string).collect()
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

#[derive(Debug, Clone, Copy)]
struct FakeInfoSpec {
    protocol_version: u32,
    package_version: &'static str,
    role: ControlRuntimeRole,
    notifications_configured: bool,
    workers_configured: bool,
    idle_retire: bool,
    scoped_backlog: bool,
    malformed_instance_id: bool,
}

impl FakeInfoSpec {
    fn compatible() -> Self {
        Self {
            protocol_version: CONTROL_RUNTIME_PROTOCOL_VERSION,
            package_version: env!("CARGO_PKG_VERSION"),
            role: ControlRuntimeRole::Baseline,
            notifications_configured: false,
            workers_configured: false,
            idle_retire: true,
            scoped_backlog: true,
            malformed_instance_id: false,
        }
    }

    fn incompatible_missing_backlog() -> Self {
        Self {
            scoped_backlog: false,
            package_version: "0.0.9-old",
            ..Self::compatible()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FakeRetireMode {
    Clean,
    Busy,
    Hang,
}

struct FakeControlRuntime {
    descriptor: RuntimeDescriptor,
    info: ControlRuntimeInfoResponse,
    stop_tx: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    retire_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl FakeControlRuntime {
    async fn start(
        state_dir: &Path,
        spec: FakeInfoSpec,
        retire_mode: FakeRetireMode,
        include_info_endpoint: bool,
    ) -> Self {
        use tokio::net::TcpListener;

        let lock = StateLock::acquire(state_dir).expect("acquire synthetic runtime lock");
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind synthetic runtime listener");
        let port = listener.local_addr().expect("synthetic local_addr").port();
        let token = SecretToken::generate();
        let descriptor = RuntimeDescriptor {
            version: 1,
            host: "127.0.0.1".to_string(),
            port,
            pid: std::process::id(),
            token,
        };
        write_descriptor_atomic(state_dir, &descriptor).expect("write synthetic descriptor");

        let info = ControlRuntimeInfoResponse {
            protocol_version: spec.protocol_version,
            package_version: spec.package_version.to_string(),
            role: spec.role,
            state: ControlRuntimeRuntimeState::Ready,
            instance_id: if spec.malformed_instance_id {
                "not-a-valid-instance".to_string()
            } else {
                uuid::Uuid::new_v4().simple().to_string()
            },
            pid: std::process::id(),
            notifications_configured: spec.notifications_configured,
            workers_configured: spec.workers_configured,
            capabilities: ControlRuntimeCapabilities {
                bindings: true,
                scoped_backlog: spec.scoped_backlog,
                event_pull_ack: true,
                guarded_worker_backlog: true,
                idle_retire: spec.idle_retire,
                approved_host_control: spec.role == ControlRuntimeRole::ApprovedHost,
            },
        };

        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let retire_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task_retire_requests = std::sync::Arc::clone(&retire_requests);
        let task_info = info.clone();
        let task_token = descriptor.token.clone();
        let task_descriptor = descriptor.clone();
        let task_state_dir = state_dir.to_path_buf();
        let task_stop_tx = stop_tx.clone();
        let task = tokio::spawn(async move {
            let _guard = FakeRuntimeGuard {
                state_dir: task_state_dir,
                descriptor: task_descriptor,
                _lock: lock,
            };
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else { break; };
                        let info = task_info.clone();
                        let token = task_token.clone();
                        let stop_tx = task_stop_tx.clone();
                        let retire_requests = std::sync::Arc::clone(&task_retire_requests);
                        tokio::spawn(async move {
                            handle_fake_control_connection(
                                &mut socket,
                                &info,
                                &token,
                                include_info_endpoint,
                                retire_mode,
                                &stop_tx,
                                &retire_requests,
                            )
                            .await;
                        });
                    }
                }
            }
        });

        Self {
            descriptor,
            info,
            stop_tx,
            task,
            retire_requests,
        }
    }

    fn info(&self) -> &ControlRuntimeInfoResponse {
        &self.info
    }

    fn retire_requests(&self) -> usize {
        self.retire_requests
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn is_serving(&self) -> bool {
        let client = SupervisorClient::from_descriptor(self.descriptor.clone())
            .expect("synthetic runtime descriptor");
        tokio::time::timeout(Duration::from_millis(300), client.list_bindings())
            .await
            .is_ok_and(|result| result.is_ok())
    }
}

impl Drop for FakeControlRuntime {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        self.task.abort();
    }
}

struct FakeRuntimeGuard {
    state_dir: PathBuf,
    descriptor: RuntimeDescriptor,
    _lock: StateLock,
}

impl Drop for FakeRuntimeGuard {
    fn drop(&mut self) {
        remove_owned_descriptor(&self.state_dir, &self.descriptor);
    }
}

async fn handle_fake_control_connection(
    socket: &mut tokio::net::TcpStream,
    info: &ControlRuntimeInfoResponse,
    token: &SecretToken,
    include_info_endpoint: bool,
    retire_mode: FakeRetireMode,
    stop_tx: &tokio::sync::watch::Sender<bool>,
    retire_requests: &std::sync::atomic::AtomicUsize,
) {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        let read = match socket.read(&mut chunk).await {
            Ok(read) => read,
            Err(_) => return,
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        if buffer.len() > 16 * 1024 {
            return;
        }
    };

    let header_text = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = header_text.split("\r\n");
    let Some(request_line) = lines.next() else {
        return;
    };
    let mut request_parts = request_line.split_whitespace();
    let Some(method) = request_parts.next() else {
        return;
    };
    let Some(path) = request_parts.next() else {
        return;
    };

    let mut authorization = None;
    let mut content_length = 0usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("authorization") {
            authorization = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().unwrap_or(0);
        }
    }

    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        match socket.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
            Err(_) => return,
        }
    }
    body.truncate(content_length);

    let expected_auth = format!("Bearer {}", token.as_str());
    if authorization.as_deref() != Some(expected_auth.as_str()) {
        write_fake_json(
            socket,
            "401 Unauthorized",
            &ApiError::new(ThreadlaneErrorCode::Unauthorized, "invalid bearer token"),
        )
        .await;
        return;
    }

    match (method, path) {
        ("GET", "/v1/bindings") => {
            write_fake_json(
                socket,
                "200 OK",
                &threadlane::protocol::BindingListResponse {
                    bindings: Vec::new(),
                },
            )
            .await;
        }
        ("GET", "/v1/control-runtime/info") if include_info_endpoint => {
            write_fake_json(socket, "200 OK", info).await;
        }
        ("POST", "/v1/control-runtime/retire") if include_info_endpoint => {
            retire_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let request: ControlRuntimeRetireRequest = match serde_json::from_slice(&body) {
                Ok(request) => request,
                Err(err) => {
                    write_fake_json(
                        socket,
                        "400 Bad Request",
                        &ApiError::new(
                            ThreadlaneErrorCode::InvalidRequest,
                            format!("invalid retire request: {err}"),
                        ),
                    )
                    .await;
                    return;
                }
            };
            if request.expected_pid != info.pid || request.expected_instance_id != info.instance_id
            {
                write_fake_json(
                    socket,
                    "409 Conflict",
                    &ApiError::with_details(
                        ThreadlaneErrorCode::HostInstanceMismatch,
                        "synthetic instance mismatch",
                        serde_json::json!({ "expected_instance_id": request.expected_instance_id.clone() }),
                    ),
                )
                .await;
                return;
            }
            if request.expected_protocol_version != info.protocol_version {
                write_fake_json(
                    socket,
                    "409 Conflict",
                    &ApiError::new(
                        ThreadlaneErrorCode::InvalidConfig,
                        "control runtime protocol mismatch: synthetic",
                    ),
                )
                .await;
                return;
            }
            match retire_mode {
                FakeRetireMode::Busy => {
                    write_fake_json(
                        socket,
                        "409 Conflict",
                        &ApiError::with_details(
                            ThreadlaneErrorCode::HostNotIdle,
                            "synthetic runtime is busy",
                            serde_json::json!({
                                "non_terminal_runs": 1,
                                "non_terminal_workers": 1,
                                "live_worker_tasks": 1,
                                "in_flight_wakeups": 0,
                                "pending_wakeups": 1,
                            }),
                        ),
                    )
                    .await;
                }
                FakeRetireMode::Hang => {
                    write_fake_json(
                        socket,
                        "200 OK",
                        &threadlane::protocol::ControlRuntimeRetireResponse {
                            protocol_version: info.protocol_version,
                            instance_id: info.instance_id.clone(),
                            state: ControlRuntimeRetireState::Retiring,
                        },
                    )
                    .await;
                }
                FakeRetireMode::Clean => {
                    write_fake_json(
                        socket,
                        "200 OK",
                        &threadlane::protocol::ControlRuntimeRetireResponse {
                            protocol_version: info.protocol_version,
                            instance_id: info.instance_id.clone(),
                            state: ControlRuntimeRetireState::Retiring,
                        },
                    )
                    .await;
                    let _ = stop_tx.send(true);
                }
            }
        }
        _ => {
            write_fake_json(
                socket,
                "404 Not Found",
                &ApiError::new(ThreadlaneErrorCode::InvalidRequest, "endpoint not found"),
            )
            .await;
        }
    }
}

async fn write_fake_json<T: Serialize>(
    socket: &mut tokio::net::TcpStream,
    status: &str,
    value: &T,
) {
    use tokio::io::AsyncWriteExt;

    let body = serde_json::to_vec(value).expect("serialize synthetic response");
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(header.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}

/// 1. Existing healthy authenticated supervisor is reused without spawning another process, and
///    its authenticated control advertisement reports truthful configuration and role.
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
        outcome.notifications_enabled,
        Some(false),
        "verified reuse must truthfully report notifications disabled"
    );
    assert_eq!(
        outcome.workers_enabled,
        Some(false),
        "verified reuse must truthfully report workers disabled"
    );
    assert_eq!(
        outcome.runtime_protocol_version,
        threadlane::CONTROL_RUNTIME_PROTOCOL_VERSION
    );
    assert_eq!(
        outcome.runtime_role,
        threadlane::ControlRuntimeRole::Baseline
    );
    assert_eq!(outcome.runtime_instance_id.len(), 32);
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

    let recorded_args = read_recorded_argv(&argv_file);
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
    assert_eq!(reused.notifications_enabled, Some(false));
    assert_eq!(reused.workers_enabled, Some(false));
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
    assert_eq!(
        read_invocation_count(&count_a),
        1,
        "cross-process startup coordination must launch exactly one supervisor"
    );
    for o in &outcomes {
        assert_eq!(o.pid, winner_pid);
        assert_eq!(o.port, winner_port);
        assert_eq!(o.notifications_enabled, Some(false));
        assert_eq!(o.workers_enabled, Some(false));
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

/// 9. A baseline runtime whose package version differs but whose authenticated capability
///    contract covers every required operation is reused truthfully without a restart.
#[tokio::test]
async fn compatible_older_package_runtime_is_reused_by_capability_not_version() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("compat-count.txt");
    let argv_file = fixture.root.join("compat-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    let mut spec = FakeInfoSpec::compatible();
    spec.package_version = "0.0.9-old";
    let fake =
        FakeControlRuntime::start(&fixture.state_dir, spec, FakeRetireMode::Clean, true).await;

    let outcome = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("compatible capability contract must be reused");

    assert_eq!(outcome.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(outcome.pid, std::process::id());
    assert_eq!(
        outcome.runtime_protocol_version,
        CONTROL_RUNTIME_PROTOCOL_VERSION
    );
    assert_eq!(outcome.runtime_package_version, "0.0.9-old");
    assert_eq!(outcome.runtime_instance_id, fake.info().instance_id);
    assert_eq!(outcome.runtime_role, ControlRuntimeRole::Baseline);
    assert_eq!(outcome.notifications_enabled, Some(false));
    assert_eq!(outcome.workers_enabled, Some(false));
    assert_eq!(outcome.retired_runtime, None);
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "package version difference alone must never spawn a replacement"
    );
    assert_eq!(fake.retire_requests(), 0);
    assert!(fake.is_serving().await);
}

/// 10. A legacy authenticated runtime without the control-info/idle-retire handshake fails early
///     with precise upgrade guidance, never spawns, and is never signalled or reused.
#[tokio::test]
async fn unsupported_legacy_runtime_fails_early_without_spawn_or_signal() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("legacy-count.txt");
    let argv_file = fixture.root.join("legacy-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);

    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::compatible(),
        FakeRetireMode::Clean,
        false,
    )
    .await;

    let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(2))
        .await
        .expect_err("legacy runtime must fail early");
    assert_eq!(err.error_code(), ControlRuntimeErrorCode::UpgradeRequired);
    assert!(
        err.message().contains("legacy") && err.message().contains("Stop"),
        "upgrade guidance must be actionable: {}",
        err.message()
    );
    assert_eq!(read_invocation_count(&count_file), 0);
    assert_eq!(fake.retire_requests(), 0);
    assert!(
        fake.is_serving().await,
        "legacy runtime must remain untouched and alive"
    );
}

/// 11. An incompatible idle baseline that advertises safe retirement is atomically retired and
///     replaced by the current executable with bindings, backlog Done state, and run history
///     preserved. Startup records no replay.
#[tokio::test]
async fn inactive_incompatible_baseline_upgrades_preserving_data_without_replay() {
    let mut fixture = ControlRuntimeTestFixture::new();

    let (binding_id, backlog_item_id, run_id, activity_count_before) = {
        let handle = Supervisor::start(SupervisorConfig::new(&fixture.state_dir))
            .await
            .expect("seed supervisor");
        let client = SupervisorClient::from_state_dir(&fixture.state_dir).expect("seed client");
        let binding = client
            .create_binding(TEST_THREAD_UUID_A, "upgrade-preserve")
            .await
            .expect("seed binding");
        let scoped = ScopedClient::from_state_dir(&fixture.state_dir, &binding.binding_id)
            .expect("seed scoped client");

        let item = scoped
            .create_backlog_item(
                "seed-item-create".to_string(),
                "Preserve Done item".to_string(),
                "Item survives idle runtime upgrade".to_string(),
                "Durable backlog state".to_string(),
                BacklogItemKind::Coworker,
            )
            .await
            .expect("seed backlog item");
        let item = scoped
            .start_backlog_item(
                "seed-item-start".to_string(),
                item.item_id.clone(),
                item.revision,
            )
            .await
            .expect("start backlog item");
        let item = scoped
            .complete_backlog_item(
                "seed-item-complete".to_string(),
                item.item_id.clone(),
                item.revision,
                "Coworker delivery checked".to_string(),
                "fixture evidence".to_string(),
            )
            .await
            .expect("complete backlog item");
        assert_eq!(item.status, BacklogStatus::Done);

        let run = scoped
            .start_demo(
                "seed-demo-run".to_string(),
                "Durable completed run".to_string(),
                Some(0),
            )
            .await
            .expect("seed demo run");
        let run = complete_demo_run(&scoped, run, "seed-demo-run").await;
        let acked_events = drain_and_ack_events(&scoped, "seed-demo").await;
        assert!(acked_events > 0, "seeded run must emit events");
        let activity = scoped
            .list_activity(None, None, Some(50), None, None)
            .await
            .expect("seed activity");
        let activity_count = activity.events.len();
        assert!(activity_count > 0);

        handle.shutdown().await;
        (binding.binding_id, item.item_id, run.run_id, activity_count)
    };

    let count_file = fixture.root.join("upgrade-count.txt");
    let argv_file = fixture.root.join("upgrade-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::incompatible_missing_backlog(),
        FakeRetireMode::Clean,
        true,
    )
    .await;
    let fake_instance = fake.info().instance_id.clone();
    let fake_package = fake.info().package_version.clone();

    let outcome = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(8))
        .await
        .expect("idle incompatible baseline must upgrade safely");
    fixture.track_pid(outcome.pid);

    assert_eq!(outcome.status, ControlRuntimeEnsureStatus::Upgraded);
    assert_ne!(outcome.pid, std::process::id());
    assert_eq!(outcome.bindings_count, 1);
    let retired = outcome
        .retired_runtime
        .as_ref()
        .expect("upgrade must report the retired runtime");
    assert_eq!(retired.pid, std::process::id());
    assert_eq!(retired.instance_id, fake_instance);
    assert_eq!(retired.package_version, fake_package);
    assert_eq!(read_invocation_count(&count_file), 1);

    let client = SupervisorClient::from_state_dir(&fixture.state_dir).expect("successor client");
    let bindings = client
        .list_bindings()
        .await
        .expect("list successor bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].binding_id, binding_id);
    let scoped =
        ScopedClient::from_state_dir(&fixture.state_dir, &binding_id).expect("successor scoped");
    let item = scoped
        .get_backlog_item(backlog_item_id)
        .await
        .expect("Done item preserved");
    assert_eq!(item.status, BacklogStatus::Done);
    assert_eq!(
        item.completion_note.as_deref(),
        Some("Coworker delivery checked")
    );

    let run = scoped.get_run(run_id.clone()).await.expect("run preserved");
    assert_eq!(run.status, RunStatus::Completed);
    let runs = scoped
        .list_runs(None, Some(50))
        .await
        .expect("runs preserved");
    assert_eq!(runs.runs.len(), 1, "startup must not replay or create runs");
    assert_eq!(runs.runs[0].run_id, run_id);
    let activity = scoped
        .list_activity(None, None, Some(50), None, None)
        .await
        .expect("activity preserved");
    assert_eq!(
        activity.events.len(),
        activity_count_before,
        "startup must not replay or fabricate events"
    );
}

/// 12. An incompatible runtime whose atomic retire is refused because activity won the race
///     surfaces a precise busy error, starts no replacement, and leaves the runtime serving.
#[tokio::test]
async fn incompatible_busy_runtime_refuses_upgrade_without_side_effects() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("busy-count.txt");
    let argv_file = fixture.root.join("busy-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::incompatible_missing_backlog(),
        FakeRetireMode::Busy,
        true,
    )
    .await;

    let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect_err("busy runtime must refuse upgrade");
    assert_eq!(err.error_code(), ControlRuntimeErrorCode::RuntimeBusy);
    assert!(
        err.message().contains("non_terminal_runs=1")
            && err.message().contains("pending_wakeups=1"),
        "busy refusal must report bounded counters: {}",
        err.message()
    );
    assert_eq!(read_invocation_count(&count_file), 0);
    assert_eq!(fake.retire_requests(), 1);
    assert!(fake.is_serving().await);
}

/// 13. Two independent cross-process ensure callers racing against one incompatible idle
///     baseline converge on exactly one successor without a second retirement or spawn.
#[tokio::test]
async fn concurrent_cross_process_ensures_produce_one_successor() {
    let mut fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("race-count.txt");
    let argv_file = fixture.root.join("race-argv.txt");
    let spy_exe =
        fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 120, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::incompatible_missing_backlog(),
        FakeRetireMode::Clean,
        true,
    )
    .await;

    let test_bin = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonicalize current_exe");
    let out_a = fixture.root.join("race-a.json");
    let out_b = fixture.root.join("race-b.json");

    let spawn_caller = |out_file: PathBuf| {
        tokio::process::Command::new(&test_bin)
            .args([
                "--exact",
                "synthetic_control_runtime_child_entrypoint",
                "--nocapture",
            ])
            .env(CHILD_MODE_ENV, "ensure_caller")
            .env(CHILD_CALLER_STATE_DIR_ENV, &fixture.state_dir)
            .env(CHILD_CALLER_EXE_ENV, &spy_exe)
            .env(CHILD_CALLER_TIMEOUT_MS_ENV, "8000")
            .env(CHILD_CALLER_OUT_FILE_ENV, out_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn race ensure caller")
    };

    let mut caller_a = spawn_caller(out_a.clone());
    let mut caller_b = spawn_caller(out_b.clone());
    let (exit_a, exit_b) = tokio::join!(caller_a.wait(), caller_b.wait());
    assert!(exit_a.expect("wait caller A").success());
    assert!(exit_b.expect("wait caller B").success());

    let result_a: CallerChildResult =
        serde_json::from_slice(&fs::read(&out_a).expect("read race A")).expect("parse race A");
    let result_b: CallerChildResult =
        serde_json::from_slice(&fs::read(&out_b).expect("read race B")).expect("parse race B");
    let CallerChildResult::Ok(outcome_a) = result_a else {
        panic!("race caller A failed: {result_a:?}");
    };
    let CallerChildResult::Ok(outcome_b) = result_b else {
        panic!("race caller B failed: {result_b:?}");
    };

    assert_eq!(
        read_invocation_count(&count_file),
        1,
        "bounded cross-process startup coordination must launch exactly one successor"
    );
    // The surviving process/port and the one-launched/one-reused outcomes prove a single
    // successor; the truthful invocation count must match that launch count.
    assert_eq!(outcome_a.pid, outcome_b.pid, "one successor pid");
    assert_eq!(outcome_a.port, outcome_b.port, "one successor port");
    let launched = [&outcome_a, &outcome_b]
        .iter()
        .filter(|outcome| {
            matches!(
                outcome.status,
                ControlRuntimeEnsureStatus::Upgraded | ControlRuntimeEnsureStatus::Started
            )
        })
        .count();
    let reused = [&outcome_a, &outcome_b]
        .iter()
        .filter(|outcome| outcome.status == ControlRuntimeEnsureStatus::AlreadyRunning)
        .count();
    assert_eq!(launched, 1, "exactly one caller must launch the successor");
    assert_eq!(reused, 1, "the other caller must reuse the successor");
    assert_eq!(
        read_invocation_count(&count_file),
        launched,
        "launch invocation count must match exactly one successful successor launch"
    );
    fixture.track_pid(outcome_a.pid);
    assert!(is_process_alive(outcome_a.pid));
    assert!(!fake.is_serving().await, "retired fake must stop serving");
}

/// 14. A runtime that accepts retirement but never releases its owned lock causes a bounded
///     retire timeout with no forced kill and no replacement spawn.
#[tokio::test]
async fn retire_timeout_does_not_force_kill_or_spawn_duplicate() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("retire-timeout-count.txt");
    let argv_file = fixture.root.join("retire-timeout-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::incompatible_missing_backlog(),
        FakeRetireMode::Hang,
        true,
    )
    .await;

    let started = Instant::now();
    let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(400))
        .await
        .expect_err("retirement timeout must fail closed");
    let elapsed = started.elapsed();
    assert_eq!(err.error_code(), ControlRuntimeErrorCode::RetireTimedOut);
    assert!(elapsed < Duration::from_millis(2_000));
    assert_eq!(read_invocation_count(&count_file), 0);
    assert_eq!(fake.retire_requests(), 1);
    assert!(
        fake.is_serving().await,
        "retire timeout must not signal or kill the runtime"
    );
}

/// 15. Approved host roles and newer/unknown protocols are never retired or downgraded by the
///     baseline ensure helper.
#[tokio::test]
async fn approved_host_and_newer_protocol_refuse_automatic_upgrade() {
    for spec in [
        FakeInfoSpec {
            role: ControlRuntimeRole::ApprovedHost,
            idle_retire: false,
            ..FakeInfoSpec::compatible()
        },
        FakeInfoSpec {
            protocol_version: CONTROL_RUNTIME_PROTOCOL_VERSION + 99,
            idle_retire: true,
            ..FakeInfoSpec::compatible()
        },
    ] {
        let fixture = ControlRuntimeTestFixture::new();
        let count_file = fixture.root.join("foreign-count.txt");
        let argv_file = fixture.root.join("foreign-argv.txt");
        let spy_exe =
            fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
        let fake =
            FakeControlRuntime::start(&fixture.state_dir, spec, FakeRetireMode::Clean, true).await;

        let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(2))
            .await
            .expect_err("foreign or newer runtime must be refused");
        assert_eq!(
            err.error_code(),
            ControlRuntimeErrorCode::IncompatibleRuntime
        );
        assert_eq!(read_invocation_count(&count_file), 0);
        assert_eq!(fake.retire_requests(), 0);
        assert!(fake.is_serving().await);
    }
}

/// 16. Malformed authenticated control-runtime identity is never reused, retired, or replaced.
#[tokio::test]
async fn malformed_control_info_refuses_reuse_and_retirement() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("malformed-count.txt");
    let argv_file = fixture.root.join("malformed-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let mut spec = FakeInfoSpec::compatible();
    spec.malformed_instance_id = true;
    let fake =
        FakeControlRuntime::start(&fixture.state_dir, spec, FakeRetireMode::Clean, true).await;

    let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(2))
        .await
        .expect_err("malformed control info must fail closed");
    assert_eq!(err.error_code(), ControlRuntimeErrorCode::ProtocolError);
    assert_eq!(read_invocation_count(&count_file), 0);
    assert_eq!(fake.retire_requests(), 0);
    assert!(fake.is_serving().await);
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
    assert_eq!(reused.notifications_enabled, Some(false));
    assert_eq!(reused.workers_enabled, Some(false));
    assert_eq!(read_invocation_count(&count_file), 1);
}

/// Prefix of the persistent cross-process startup guard kept beside `state_dir`.
///
/// Tests discover the single guard produced by production code by this prefix instead of
/// recomputing the production filename hash.
const STARTUP_GUARD_PREFIX: &str = ".threadlane-control-runtime-startup-";

fn discover_startup_guard_path(fixture_root: &Path) -> PathBuf {
    let mut found = Vec::new();
    for entry in fs::read_dir(fixture_root).expect("read fixture root for startup guard") {
        let entry = entry.expect("read fixture root entry");
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(STARTUP_GUARD_PREFIX) && name.ends_with(".lock") {
            found.push(entry.path());
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected exactly one persistent startup guard beside state_dir, found {found:?}"
    );
    found.pop().expect("single discovered startup guard")
}

fn assert_private_empty_guard_file(path: &Path) {
    let meta = fs::symlink_metadata(path)
        .unwrap_or_else(|err| panic!("stat startup guard `{}`: {err}", path.display()));
    assert!(
        meta.file_type().is_file(),
        "startup guard `{}` must be a regular file",
        path.display()
    );
    assert_eq!(
        meta.len(),
        0,
        "startup guard `{}` must be empty",
        path.display()
    );
    assert_eq!(
        meta.mode() & 0o777,
        0o600,
        "startup guard `{}` must stay private 0600",
        path.display()
    );
}

fn hold_startup_guard(path: &Path) -> File {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap_or_else(|err| panic!("open startup guard `{}` for locking: {err}", path.display()));
    FileExt::lock_exclusive(&file)
        .unwrap_or_else(|err| panic!("flock startup guard `{}`: {err}", path.display()));
    file
}

fn create_private_empty_file(path: &Path) {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap_or_else(|err| panic!("create private empty file `{}`: {err}", path.display()));
    file.sync_all()
        .unwrap_or_else(|err| panic!("sync private empty file `{}`: {err}", path.display()));
}

fn open_fd_count_for_inode(dev: u64, ino: u64) -> usize {
    fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .filter_map(Result::ok)
        .filter(|entry| {
            fs::metadata(entry.path()).is_ok_and(|meta| meta.dev() == dev && meta.ino() == ino)
        })
        .count()
}

async fn wait_for_guard_fd_count(guard_path: &Path, expected: usize, label: &str) {
    let meta = fs::symlink_metadata(guard_path).unwrap_or_else(|err| {
        panic!(
            "{label}: stat startup guard `{}`: {err}",
            guard_path.display()
        )
    });
    let (dev, ino) = (meta.dev(), meta.ino());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let observed = open_fd_count_for_inode(dev, ino);
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: expected {expected} open descriptor(s) for startup guard inode ({dev}, {ino}), observed {observed}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn create_valid_startup_guard_against_fake(
    fixture: &ControlRuntimeTestFixture,
    fake: &FakeControlRuntime,
    spy_exe: &Path,
    count_file: &Path,
) -> PathBuf {
    let outcome = ensure_control_runtime(&fixture.state_dir, spy_exe, Duration::from_secs(3))
        .await
        .expect("create valid startup guard against compatible fake runtime");
    assert_eq!(outcome.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(outcome.pid, std::process::id());
    assert_eq!(outcome.bindings_count, 0);
    assert_eq!(outcome.runtime_instance_id, fake.info().instance_id);
    assert_eq!(
        read_invocation_count(count_file),
        0,
        "creating the startup guard must reuse the compatible owned runtime without spawning"
    );
    assert_eq!(fake.retire_requests(), 0);
    assert!(fake.is_serving().await);

    let guard_path = discover_startup_guard_path(&fixture.root);
    assert_private_empty_guard_file(&guard_path);
    guard_path
}

async fn assert_invalid_startup_guard_rejected(
    fixture: &ControlRuntimeTestFixture,
    spy_exe: &Path,
    case: &str,
) {
    let outcome = ensure_control_runtime(&fixture.state_dir, spy_exe, Duration::from_secs(1)).await;
    let err = match outcome {
        Ok(outcome) => panic!("{case}: expected startup guard rejection, got {outcome:?}"),
        Err(err) => err,
    };
    assert_eq!(
        err.error_code(),
        ControlRuntimeErrorCode::SecurityViolation,
        "{case}: {}",
        err.message()
    );
}

/// 17. A held cross-process startup guard bounds an ensure call with `LockBusy` without startup,
///     retirement, descriptor change, or runtime signalling; releasing it lets the next ensure
///     reuse the same compatible runtime without spawning.
#[tokio::test]
async fn held_startup_guard_bounds_ensure_with_lock_busy_and_release_reuses_runtime() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("guard-busy-count.txt");
    let argv_file = fixture.root.join("guard-busy-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::compatible(),
        FakeRetireMode::Clean,
        true,
    )
    .await;
    let guard_path =
        create_valid_startup_guard_against_fake(&fixture, &fake, &spy_exe, &count_file).await;

    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    let descriptor_before =
        fs::read(&descriptor_path).expect("read fake descriptor before refusal");
    let descriptor_meta_before =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor before refusal");
    let guard_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat startup guard before hold");
    let held = hold_startup_guard(&guard_path);

    let started = Instant::now();
    let err = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_millis(200))
        .await
        .expect_err("held startup guard must time out with LockBusy");
    let elapsed = started.elapsed();

    assert_eq!(err.error_code(), ControlRuntimeErrorCode::LockBusy);
    assert!(
        err.message().contains("startup guard"),
        "LockBusy must identify the startup guard: {}",
        err.message()
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "held startup guard must bound the wait (elapsed: {elapsed:?})"
    );
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "guard contention must never start a supervisor"
    );
    assert_eq!(
        fake.retire_requests(),
        0,
        "guard contention must never retire the runtime"
    );
    assert!(
        fake.is_serving().await,
        "guard contention must not stop the runtime"
    );

    let descriptor_after = fs::read(&descriptor_path).expect("read fake descriptor after refusal");
    let descriptor_meta_after =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor after refusal");
    assert_eq!(
        descriptor_after, descriptor_before,
        "guard contention must not rewrite supervisor.json"
    );
    assert_eq!(
        (descriptor_meta_after.dev(), descriptor_meta_after.ino()),
        (descriptor_meta_before.dev(), descriptor_meta_before.ino()),
        "guard contention must not replace supervisor.json"
    );
    assert_private_empty_guard_file(&guard_path);
    let guard_meta_after =
        fs::symlink_metadata(&guard_path).expect("stat startup guard after refusal");
    assert_eq!(
        (guard_meta_after.dev(), guard_meta_after.ino()),
        (guard_meta_before.dev(), guard_meta_before.ino()),
        "a contended startup guard must remain the same persistent file"
    );

    FileExt::unlock(&held).expect("release startup guard flock");
    drop(held);

    let reused = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("released startup guard must allow legitimate reuse");
    assert_eq!(reused.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(reused.pid, std::process::id());
    assert_eq!(reused.port, fake.descriptor.port);
    assert_eq!(reused.runtime_instance_id, fake.info().instance_id);
    assert_eq!(reused.bindings_count, 0);
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "legitimate reuse after guard release must not spawn"
    );
    assert_eq!(fake.retire_requests(), 0);
    assert!(fake.is_serving().await);
    assert_private_empty_guard_file(&guard_path);
    let guard_meta_reused =
        fs::symlink_metadata(&guard_path).expect("stat startup guard after reuse");
    assert_eq!(
        (guard_meta_reused.dev(), guard_meta_reused.ino()),
        (guard_meta_before.dev(), guard_meta_before.ino()),
        "reuse must keep the same persistent startup guard inode"
    );
}

/// 18. Cancelling a genuinely blocked ensure future releases its opened startup guard descriptor
///     without unlinking the persistent guard or stopping the runtime; a later ensure succeeds.
#[tokio::test]
async fn cancelled_blocked_ensure_releases_guard_descriptor_without_unlinking_runtime() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("guard-cancel-count.txt");
    let argv_file = fixture.root.join("guard-cancel-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::compatible(),
        FakeRetireMode::Clean,
        true,
    )
    .await;
    let guard_path =
        create_valid_startup_guard_against_fake(&fixture, &fake, &spy_exe, &count_file).await;

    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    let descriptor_before =
        fs::read(&descriptor_path).expect("read fake descriptor before cancellation");
    let descriptor_meta_before =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor before cancellation");
    let guard_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat startup guard before hold");
    let held = hold_startup_guard(&guard_path);

    let mut ensure_future = Box::pin(ensure_control_runtime(
        &fixture.state_dir,
        &spy_exe,
        Duration::from_secs(5),
    ));
    let pending = tokio::time::timeout(Duration::from_millis(500), &mut ensure_future).await;
    assert!(
        pending.is_err(),
        "the ensure future must remain pending while the startup guard is held"
    );
    wait_for_guard_fd_count(&guard_path, 2, "blocked ensure").await;
    drop(ensure_future);
    wait_for_guard_fd_count(&guard_path, 1, "cancelled ensure").await;

    assert_private_empty_guard_file(&guard_path);
    let guard_meta_after =
        fs::symlink_metadata(&guard_path).expect("stat startup guard after cancellation");
    assert_eq!(
        (guard_meta_after.dev(), guard_meta_after.ino()),
        (guard_meta_before.dev(), guard_meta_before.ino()),
        "cancellation must not unlink or replace the persistent startup guard"
    );
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "cancellation during guard contention must not start a supervisor"
    );
    assert_eq!(fake.retire_requests(), 0);
    assert!(
        fake.is_serving().await,
        "cancellation must not stop the runtime"
    );
    let descriptor_after =
        fs::read(&descriptor_path).expect("read fake descriptor after cancellation");
    let descriptor_meta_after =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor after cancellation");
    assert_eq!(descriptor_after, descriptor_before);
    assert_eq!(
        (descriptor_meta_after.dev(), descriptor_meta_after.ino()),
        (descriptor_meta_before.dev(), descriptor_meta_before.ino())
    );

    FileExt::unlock(&held).expect("release startup guard flock");
    drop(held);

    let reused = ensure_control_runtime(&fixture.state_dir, &spy_exe, Duration::from_secs(3))
        .await
        .expect("subsequent ensure after cancellation must succeed");
    assert_eq!(reused.status, ControlRuntimeEnsureStatus::AlreadyRunning);
    assert_eq!(reused.pid, std::process::id());
    assert_eq!(reused.port, fake.descriptor.port);
    assert_eq!(reused.runtime_instance_id, fake.info().instance_id);
    assert_eq!(read_invocation_count(&count_file), 0);
    assert_eq!(fake.retire_requests(), 0);
    assert!(fake.is_serving().await);
}

/// 19. Symlink, world-readable `0644`, nonempty, and directory startup guard forms are rejected
///     without modifying the target, descriptor, or owned runtime and without any spawn/retire.
#[tokio::test]
async fn invalid_startup_guard_forms_are_rejected_without_touching_runtime_or_descriptor() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("guard-invalid-count.txt");
    let argv_file = fixture.root.join("guard-invalid-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::compatible(),
        FakeRetireMode::Clean,
        true,
    )
    .await;
    let guard_path =
        create_valid_startup_guard_against_fake(&fixture, &fake, &spy_exe, &count_file).await;

    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    let descriptor_before =
        fs::read(&descriptor_path).expect("read fake descriptor before invalid guards");
    let descriptor_meta_before =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor before invalid guards");

    // 19a. Symlink guard must be rejected without touching its owned target.
    fs::remove_file(&guard_path).expect("remove valid guard before symlink case");
    let symlink_target = fixture.root.join("startup-guard-symlink-target");
    let symlink_target_bytes: &[u8] = b"owned startup guard symlink target\n";
    fs::write(&symlink_target, symlink_target_bytes).expect("write symlink target");
    fs::set_permissions(&symlink_target, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 symlink target");
    let symlink_target_meta_before =
        fs::symlink_metadata(&symlink_target).expect("stat symlink target before");
    symlink(&symlink_target, &guard_path).expect("create symlink startup guard");
    assert_invalid_startup_guard_rejected(&fixture, &spy_exe, "symlink guard").await;
    assert_eq!(
        fs::read(&symlink_target)
            .expect("read symlink target after")
            .as_slice(),
        symlink_target_bytes,
        "symlink guard rejection must not modify the target"
    );
    let symlink_target_meta_after =
        fs::symlink_metadata(&symlink_target).expect("stat symlink target after");
    assert_eq!(
        (
            symlink_target_meta_after.dev(),
            symlink_target_meta_after.ino()
        ),
        (
            symlink_target_meta_before.dev(),
            symlink_target_meta_before.ino()
        ),
        "symlink guard rejection must not replace the target"
    );
    assert!(
        fs::symlink_metadata(&guard_path)
            .expect("stat symlink guard")
            .file_type()
            .is_symlink(),
        "symlink guard must remain a symlink"
    );
    fs::remove_file(&guard_path).expect("remove symlink guard");

    // 19b. World-readable empty `0644` guard must be rejected without rewriting it.
    fs::write(&guard_path, b"").expect("create world-readable empty guard");
    fs::set_permissions(&guard_path, fs::Permissions::from_mode(0o644))
        .expect("chmod 0644 startup guard");
    let world_readable_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat world-readable guard before");
    assert_invalid_startup_guard_rejected(&fixture, &spy_exe, "world-readable guard").await;
    let world_readable_meta_after =
        fs::symlink_metadata(&guard_path).expect("stat world-readable guard after");
    assert_eq!(
        (
            world_readable_meta_after.dev(),
            world_readable_meta_after.ino()
        ),
        (
            world_readable_meta_before.dev(),
            world_readable_meta_before.ino()
        ),
        "world-readable guard rejection must not replace the file"
    );
    assert_eq!(world_readable_meta_after.mode() & 0o777, 0o644);
    assert_eq!(world_readable_meta_after.len(), 0);
    assert!(
        fs::read(&guard_path)
            .expect("read world-readable guard after")
            .is_empty(),
        "world-readable guard rejection must not write contents"
    );
    fs::remove_file(&guard_path).expect("remove world-readable guard");

    // 19c. Nonempty `0600` guard must be rejected without truncating or rewriting it.
    let nonempty_bytes: &[u8] = b"not a valid startup guard\n";
    fs::write(&guard_path, nonempty_bytes).expect("create nonempty guard");
    fs::set_permissions(&guard_path, fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 nonempty guard");
    let nonempty_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat nonempty guard before");
    assert_invalid_startup_guard_rejected(&fixture, &spy_exe, "nonempty guard").await;
    let nonempty_meta_after = fs::symlink_metadata(&guard_path).expect("stat nonempty guard after");
    assert_eq!(
        (nonempty_meta_after.dev(), nonempty_meta_after.ino()),
        (nonempty_meta_before.dev(), nonempty_meta_before.ino()),
        "nonempty guard rejection must not replace the file"
    );
    assert_eq!(nonempty_meta_after.mode() & 0o777, 0o600);
    assert_eq!(
        fs::read(&guard_path)
            .expect("read nonempty guard after")
            .as_slice(),
        nonempty_bytes,
        "nonempty guard rejection must preserve contents"
    );
    fs::remove_file(&guard_path).expect("remove nonempty guard");

    // 19d. Directory guard must be rejected without replacing or modifying it.
    fs::create_dir(&guard_path).expect("create directory guard");
    fs::set_permissions(&guard_path, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 directory guard");
    let directory_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat directory guard before");
    assert_invalid_startup_guard_rejected(&fixture, &spy_exe, "directory guard").await;
    let directory_meta_after =
        fs::symlink_metadata(&guard_path).expect("stat directory guard after");
    assert_eq!(
        (directory_meta_after.dev(), directory_meta_after.ino()),
        (directory_meta_before.dev(), directory_meta_before.ino()),
        "directory guard rejection must not replace the directory"
    );
    assert!(directory_meta_after.file_type().is_dir());
    assert_eq!(directory_meta_after.mode() & 0o777, 0o700);
    fs::remove_dir(&guard_path).expect("remove directory guard");

    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "invalid startup guard forms must never start a supervisor"
    );
    assert_eq!(
        fake.retire_requests(),
        0,
        "invalid startup guard forms must never retire the runtime"
    );
    assert!(
        fake.is_serving().await,
        "invalid startup guard forms must not stop the runtime"
    );
    let descriptor_after =
        fs::read(&descriptor_path).expect("read fake descriptor after invalid guards");
    let descriptor_meta_after =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor after invalid guards");
    assert_eq!(descriptor_after, descriptor_before);
    assert_eq!(
        (descriptor_meta_after.dev(), descriptor_meta_after.ino()),
        (descriptor_meta_before.dev(), descriptor_meta_before.ino()),
        "invalid startup guards must not replace supervisor.json"
    );
}

/// 20. Replacing the guard pathname while a future waits on the original inode must fail that
///     same future with `StateChanged` after the original lock is released, never enter startup,
///     and keep both owned files intact.
#[tokio::test]
async fn replaced_startup_guard_path_fails_waiting_ensure_with_state_changed() {
    let fixture = ControlRuntimeTestFixture::new();
    let count_file = fixture.root.join("guard-replace-count.txt");
    let argv_file = fixture.root.join("guard-replace-argv.txt");
    let spy_exe = fixture.write_real_serve_wrapper("threadlane", &count_file, &argv_file, 0, false);
    let fake = FakeControlRuntime::start(
        &fixture.state_dir,
        FakeInfoSpec::compatible(),
        FakeRetireMode::Clean,
        true,
    )
    .await;
    let guard_path =
        create_valid_startup_guard_against_fake(&fixture, &fake, &spy_exe, &count_file).await;

    let descriptor_path = fixture.state_dir.join(DESCRIPTOR_FILENAME);
    let descriptor_before =
        fs::read(&descriptor_path).expect("read fake descriptor before replacement");
    let descriptor_meta_before =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor before replacement");
    let original_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat original startup guard");
    let original = hold_startup_guard(&guard_path);

    let mut ensure_future = Box::pin(ensure_control_runtime(
        &fixture.state_dir,
        &spy_exe,
        Duration::from_secs(5),
    ));
    let pending = tokio::time::timeout(Duration::from_millis(500), &mut ensure_future).await;
    assert!(
        pending.is_err(),
        "ensure must remain pending on the original guard inode"
    );
    wait_for_guard_fd_count(&guard_path, 2, "ensure waiting on original guard").await;

    let replacement_path = fixture.root.join("startup-guard-replacement");
    create_private_empty_file(&replacement_path);
    fs::rename(&replacement_path, &guard_path).expect("atomically replace startup guard pathname");
    let replacement_meta_before =
        fs::symlink_metadata(&guard_path).expect("stat replacement startup guard");
    assert_ne!(
        (replacement_meta_before.dev(), replacement_meta_before.ino()),
        (original_meta_before.dev(), original_meta_before.ino()),
        "replacement guard must be a different inode"
    );
    assert_private_empty_guard_file(&guard_path);

    FileExt::unlock(&original).expect("release original startup guard after replacement");
    // Keep `original` open through the assertions so its inode remains an owned file.

    let resumed = tokio::time::timeout(Duration::from_secs(5), &mut ensure_future)
        .await
        .expect("waiting ensure must complete after guard replacement and unlock");
    let err =
        resumed.expect_err("replaced startup guard must fail StateChanged, not start a runtime");
    assert_eq!(err.error_code(), ControlRuntimeErrorCode::StateChanged);
    assert!(
        err.message().contains("changed"),
        "StateChanged must explain the startup guard identity change: {}",
        err.message()
    );
    assert_eq!(
        read_invocation_count(&count_file),
        0,
        "a replaced startup guard must never enter startup"
    );
    assert_eq!(fake.retire_requests(), 0);
    assert!(
        fake.is_serving().await,
        "guard replacement must not stop the runtime"
    );

    let descriptor_after =
        fs::read(&descriptor_path).expect("read fake descriptor after replacement");
    let descriptor_meta_after =
        fs::symlink_metadata(&descriptor_path).expect("stat fake descriptor after replacement");
    assert_eq!(descriptor_after, descriptor_before);
    assert_eq!(
        (descriptor_meta_after.dev(), descriptor_meta_after.ino()),
        (descriptor_meta_before.dev(), descriptor_meta_before.ino())
    );

    let replacement_meta_after =
        fs::symlink_metadata(&guard_path).expect("stat replacement startup guard after resume");
    assert_eq!(
        (replacement_meta_after.dev(), replacement_meta_after.ino()),
        (replacement_meta_before.dev(), replacement_meta_before.ino()),
        "the replacement guard must remain at the pathname"
    );
    assert_private_empty_guard_file(&guard_path);
    let original_meta_after = original
        .metadata()
        .expect("stat held original startup guard");
    assert_eq!(
        (original_meta_after.dev(), original_meta_after.ino()),
        (original_meta_before.dev(), original_meta_before.ino()),
        "the original inode must remain the same held owned file"
    );
    assert_eq!(original_meta_after.len(), 0);
    assert_eq!(original_meta_after.mode() & 0o777, 0o600);
    drop(original);
}
