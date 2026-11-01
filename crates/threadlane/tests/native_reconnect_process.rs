#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Synthetic Linux OS-process integration fixture for [`NativeToolOutputConfig::reconnect`].
//!
//! Scope:
//! - Spawns real local child processes (`parent test -> wrapper child -> backend-1 child`, then
//!   `backend-2 child`) using the current test binary (`std::env::current_exe()`) and an ignored
//!   helper entrypoint selected by exact test arguments and environment role variables.
//! - Exercises real `/proc` ancestry and executable verification, `SO_PEERCRED` peer-credential
//!   checks, stale-PID descriptor ignore, and pre-`turn/start` WebSocket reconnect across an actual
//!   backend PID and Unix domain socket change.
//! - Never invokes a real Codex binary, external server, or live model/provider API.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::parent_id;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use threadlane::native_endpoint::{
    ENDPOINT_DESCRIPTOR_FILENAME, ENDPOINT_DESCRIPTOR_SCHEMA_VERSION,
    ENDPOINT_TRANSPORT_UNIX_WEBSOCKET, SESSION_DIR_PREFIX,
};
use threadlane::native_tool_output::{
    NATIVE_TOOL_OUTPUT_CONFIG_SCHEMA_VERSION, NATIVE_TOOL_OUTPUT_NAME, NATIVE_TOOL_OUTPUT_NAMESPACE,
};
use threadlane::{
    invoke_codex_wakeup, ApprovedSessionWakeupContext, CodexWakeupConfig, NativeReconnectConfig,
    NativeToolOutputConfig,
};
use threadlane_core::{WakeupClaim, WakeupOutcome};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::watch;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;

const ENV_FIXTURE_ROLE: &str = "THREADLANE_RECONNECT_FIXTURE_ROLE";
const ENV_FIXTURE_SPEC: &str = "THREADLANE_RECONNECT_FIXTURE_SPEC";
const ENV_WRAPPER_PID: &str = "THREADLANE_RECONNECT_FIXTURE_WRAPPER_PID";
const HELPER_TEST_NAME: &str = "native_reconnect_process_fixture_entrypoint";

const TEST_THREAD_ID: &str = "01968f52-8871-7d51-9914-79d2bc48eb5a";
const TEST_TURN_ID: &str = "01968f52-9999-7d51-9914-79d2bc48eb5b";
const TEST_APPROVAL_ID: &str = "apr_0123456789abcdef0123456789abcdef";
const TEST_BINDING_ID: &str = "bnd_test_native_reconnect_proc";
const FAKE_MODEL_ID: &str = "fake-local-codex-model";
const FAKE_EFFORT: &str = "high";

const CHILD_STEP_TIMEOUT: Duration = Duration::from_secs(10);
const WAKEUP_BUDGET_MS: u64 = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FixtureSpec {
    descriptor_root: PathBuf,
    records_dir: PathBuf,
    project_root: PathBuf,
    real_codex: PathBuf,
    thread_id: String,
    turn_id: String,
    model: String,
    effort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendReadyRecord {
    backend_pid: u32,
    wrapper_pid: u32,
    session_dir: PathBuf,
    socket_path: PathBuf,
    descriptor_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackendSessionRecord {
    backend_pid: u32,
    ppid: u32,
    wrapper_pid: u32,
    session_dir: PathBuf,
    socket_path: PathBuf,
    descriptor_path: PathBuf,
    handshakes: usize,
    methods_seen: Vec<String>,
    turn_start_count: usize,
    turn_start_requests: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WrapperRecord {
    wrapper_pid: u32,
    app_pid: u32,
    backend_1_pid: u32,
    backend_1_reaped: bool,
    backend_1_exit_ok: bool,
    backend_2_pid: u32,
    backend_2_reaped: bool,
    backend_2_exit_ok: bool,
}

// The callback result is fixed by tungstenite's public handshake trait.
#[allow(clippy::result_large_err)]
fn validate_upgrade(
    req: &Request,
    resp: Response,
) -> Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
    let upgrade = req
        .headers()
        .get("Upgrade")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(upgrade.eq_ignore_ascii_case("websocket"));
    let host = req
        .headers()
        .get("Host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(host.starts_with("localhost"));
    Ok(resp)
}

fn create_private_dir(path: &Path) {
    fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .expect("create private 0700 directory");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("enforce 0700 directory permissions");
}

fn write_private_file_atomic(path: &Path, bytes: &[u8]) {
    let parent = path
        .parent()
        .expect("private file path must have a parent directory");
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("valid UTF-8 file name");
    let tmp_path = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
    let _ = fs::remove_file(&tmp_path);

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)
        .expect("create private 0600 temp file");
    file.write_all(bytes).expect("write private temp file");
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .expect("enforce 0600 temp file permissions");
    file.sync_all().expect("sync private temp file");
    drop(file);

    fs::rename(&tmp_path, path).expect("atomically publish private 0600 file");
}

fn write_private_json<T: Serialize>(path: &Path, value: &T) {
    let encoded = serde_json::to_vec_pretty(value).expect("serialize JSON record");
    write_private_file_atomic(path, &encoded);
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> T {
    let bytes = fs::read(path).expect("read JSON file");
    serde_json::from_slice(&bytes).expect("parse JSON file")
}

async fn read_ws_json(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
) -> Value {
    tokio::time::timeout(CHILD_STEP_TIMEOUT, async {
        loop {
            let msg = ws
                .next()
                .await
                .expect("expected WebSocket frame")
                .expect("valid WebSocket frame");
            match msg {
                Message::Text(text) => {
                    return serde_json::from_str(&text).expect("valid JSON-RPC frame");
                }
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected WebSocket frame in fake backend: {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for WebSocket JSON frame")
}

async fn send_ws_json(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    payload: &Value,
) {
    let text = payload.to_string();
    tokio::time::timeout(CHILD_STEP_TIMEOUT, ws.send(Message::Text(text.into())))
        .await
        .expect("timed out sending WebSocket JSON frame")
        .expect("send WebSocket JSON frame");
}

fn setup_session_endpoint(
    spec: &FixtureSpec,
    label: &str,
    wrapper_pid: u32,
    backend_pid: u32,
) -> (PathBuf, PathBuf, PathBuf, UnixListener) {
    let session_dir = spec
        .descriptor_root
        .join(format!("{SESSION_DIR_PREFIX}{label}-{backend_pid}"));
    create_private_dir(&session_dir);

    let socket_path = session_dir.join("s.sock");
    let listener = UnixListener::bind(&socket_path).expect("bind session Unix domain socket");
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .expect("enforce 0600 permissions on Unix domain socket");

    let descriptor_path = session_dir.join(ENDPOINT_DESCRIPTOR_FILENAME);
    let descriptor_json = json!({
        "schema_version": ENDPOINT_DESCRIPTOR_SCHEMA_VERSION,
        "wrapper_pid": wrapper_pid,
        "backend_pid": backend_pid,
        "socket_path": socket_path,
        "real_codex": spec.real_codex,
        "transport": ENDPOINT_TRANSPORT_UNIX_WEBSOCKET,
    });
    write_private_json(&descriptor_path, &descriptor_json);

    (session_dir, socket_path, descriptor_path, listener)
}

async fn run_backend_1(spec: FixtureSpec, wrapper_pid: u32) {
    let backend_pid = std::process::id();
    let ppid = parent_id();
    assert_eq!(
        ppid, wrapper_pid,
        "backend-1 parent PID must equal wrapper PID"
    );

    let (session_dir, socket_path, descriptor_path, listener) =
        setup_session_endpoint(&spec, "session-1", wrapper_pid, backend_pid);

    let ready_record = BackendReadyRecord {
        backend_pid,
        wrapper_pid,
        session_dir: session_dir.clone(),
        socket_path: socket_path.clone(),
        descriptor_path: descriptor_path.clone(),
    };
    write_private_json(
        &spec.records_dir.join("backend-1-ready.json"),
        &ready_record,
    );

    let (stream, _) = tokio::time::timeout(CHILD_STEP_TIMEOUT, listener.accept())
        .await
        .expect("backend-1 timed out waiting for client connection")
        .expect("backend-1 accept Unix stream");

    // Drop the listener immediately after accepting the first connection so no second connection
    // can queue on session-1 while leaving the session directory and descriptor file in place to
    // exercise stale-PID candidate ignore after backend-1 exits.
    drop(listener);

    let mut ws = tokio::time::timeout(
        CHILD_STEP_TIMEOUT,
        accept_hdr_async(stream, validate_upgrade),
    )
    .await
    .expect("backend-1 timed out during WebSocket handshake")
    .expect("backend-1 WebSocket handshake");

    let init_msg = read_ws_json(&mut ws).await;
    assert_eq!(init_msg["method"], "initialize");
    assert_eq!(init_msg["id"], 1);

    let record = BackendSessionRecord {
        backend_pid,
        ppid,
        wrapper_pid,
        session_dir,
        socket_path,
        descriptor_path,
        handshakes: 1,
        methods_seen: vec!["initialize".to_string()],
        turn_start_count: 0,
        turn_start_requests: Vec::new(),
    };
    write_private_json(&spec.records_dir.join("backend-1-record.json"), &record);

    // Close before replying to initialize (prior to any turn/start) and exit immediately.
    let _ = ws.close(None).await;
    drop(ws);
}

async fn run_backend_2(spec: FixtureSpec, wrapper_pid: u32) {
    let backend_pid = std::process::id();
    let ppid = parent_id();
    assert_eq!(
        ppid, wrapper_pid,
        "backend-2 parent PID must equal wrapper PID"
    );

    let (session_dir, socket_path, descriptor_path, listener) =
        setup_session_endpoint(&spec, "session-2", wrapper_pid, backend_pid);

    let (stream, _) = tokio::time::timeout(CHILD_STEP_TIMEOUT, listener.accept())
        .await
        .expect("backend-2 timed out waiting for reconnect client connection")
        .expect("backend-2 accept Unix stream");

    let mut ws = tokio::time::timeout(
        CHILD_STEP_TIMEOUT,
        accept_hdr_async(stream, validate_upgrade),
    )
    .await
    .expect("backend-2 timed out during WebSocket handshake")
    .expect("backend-2 WebSocket handshake");

    let mut methods_seen = Vec::new();
    let mut turn_start_requests = Vec::new();
    let expected_cwd = spec.project_root.to_string_lossy().to_string();

    // 1. initialize
    let msg1 = read_ws_json(&mut ws).await;
    assert_eq!(msg1["method"], "initialize");
    assert_eq!(msg1["id"], 1);
    methods_seen.push("initialize".to_string());
    send_ws_json(
        &mut ws,
        &json!({
            "id": 1,
            "result": {
                "serverInfo": { "name": "fake-local-codex-backend-2" }
            }
        }),
    )
    .await;

    // 2. initialized notification
    let msg2 = read_ws_json(&mut ws).await;
    assert_eq!(msg2["method"], "initialized");
    assert!(msg2.get("id").is_none());
    methods_seen.push("initialized".to_string());

    // 3. thread/read
    let msg3 = read_ws_json(&mut ws).await;
    assert_eq!(msg3["method"], "thread/read");
    assert_eq!(msg3["id"], 2);
    assert_eq!(
        msg3["params"],
        json!({ "threadId": spec.thread_id, "includeTurns": false })
    );
    methods_seen.push("thread/read".to_string());
    send_ws_json(
        &mut ws,
        &json!({
            "id": 2,
            "result": {
                "thread": {
                    "id": spec.thread_id,
                    "cwd": expected_cwd,
                    "model": spec.model,
                    "reasoningEffort": spec.effort,
                    "status": { "type": "idle" }
                }
            }
        }),
    )
    .await;

    // 4. thread/resume
    let msg4 = read_ws_json(&mut ws).await;
    assert_eq!(msg4["method"], "thread/resume");
    assert_eq!(msg4["id"], 3);
    assert_eq!(
        msg4["params"],
        json!({ "threadId": spec.thread_id, "excludeTurns": true })
    );
    methods_seen.push("thread/resume".to_string());
    send_ws_json(
        &mut ws,
        &json!({
            "id": 3,
            "result": {
                "thread": {
                    "id": spec.thread_id,
                    "cwd": expected_cwd,
                    "status": { "type": "idle" }
                },
                "model": spec.model,
                "reasoningEffort": spec.effort
            }
        }),
    )
    .await;

    // 5. turn/start
    let msg5 = read_ws_json(&mut ws).await;
    assert_eq!(msg5["method"], "turn/start");
    assert_eq!(msg5["id"], 4);
    methods_seen.push("turn/start".to_string());
    turn_start_requests.push(msg5);
    send_ws_json(
        &mut ws,
        &json!({
            "id": 4,
            "result": {
                "turn": {
                    "id": spec.turn_id,
                    "status": { "type": "inProgress" },
                    "error": null
                }
            }
        }),
    )
    .await;
    let _ = tokio::time::timeout(CHILD_STEP_TIMEOUT, ws.flush()).await;

    // Wait for the client to finish reading the turn/start response and close its connection
    // before backend-2 exits.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame_res) = ws.next().await {
            match frame_res {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    })
    .await;

    let record = BackendSessionRecord {
        backend_pid,
        ppid,
        wrapper_pid,
        session_dir,
        socket_path,
        descriptor_path,
        handshakes: 1,
        methods_seen,
        turn_start_count: turn_start_requests.len(),
        turn_start_requests,
    };
    write_private_json(&spec.records_dir.join("backend-2-record.json"), &record);
    drop(ws);
    drop(listener);
}

fn spawn_backend_child(
    spec_path: &Path,
    spec: &FixtureSpec,
    role: &str,
    wrapper_pid: u32,
) -> Child {
    let mut cmd = Command::new(&spec.real_codex);
    cmd.args(["--ignored", "--exact", HELPER_TEST_NAME, "--nocapture"])
        .env(ENV_FIXTURE_ROLE, role)
        .env(ENV_FIXTURE_SPEC, spec_path)
        .env(ENV_WRAPPER_PID, wrapper_pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    cmd.spawn().expect("spawn backend child process")
}

async fn wait_for_parent_disconnect(parent_eof_rx: &mut watch::Receiver<bool>) {
    loop {
        if *parent_eof_rx.borrow_and_update() {
            return;
        }
        if parent_eof_rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn wait_and_reap_backend_child(
    child: &mut Child,
    parent_eof_rx: &mut watch::Receiver<bool>,
) -> (bool, bool) {
    tokio::select! {
        biased;
        _ = wait_for_parent_disconnect(parent_eof_rx) => {
            let _ = child.kill().await;
            let reaped = child.wait().await.is_ok();
            (reaped, false)
        }
        timed = tokio::time::timeout(CHILD_STEP_TIMEOUT, child.wait()) => {
            match timed {
                Ok(Ok(status)) => (true, status.success()),
                Ok(Err(_)) | Err(_) => {
                    let _ = child.kill().await;
                    let reaped = child.wait().await.is_ok();
                    (reaped, false)
                }
            }
        }
    }
}

async fn run_wrapper(spec_path: PathBuf, spec: FixtureSpec) {
    let wrapper_pid = std::process::id();
    let app_pid = parent_id();

    let (parent_eof_tx, mut parent_eof_rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 16];
        loop {
            match stdin.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = parent_eof_tx.send(true);
                    break;
                }
                Ok(_) => continue,
            }
        }
    });

    // 1. Start first backend child, wait for it to close during initialize and exit, and reap it.
    let mut child_1 = spawn_backend_child(&spec_path, &spec, "backend-1", wrapper_pid);
    let backend_1_pid = child_1.id().expect("backend-1 OS PID");
    let (backend_1_reaped, backend_1_exit_ok) =
        wait_and_reap_backend_child(&mut child_1, &mut parent_eof_rx).await;
    assert!(
        backend_1_reaped && backend_1_exit_ok,
        "backend-1 did not exit cleanly"
    );

    // 2. Start second backend child in a new session directory after reaping the first child.
    let mut child_2 = spawn_backend_child(&spec_path, &spec, "backend-2", wrapper_pid);
    let backend_2_pid = child_2.id().expect("backend-2 OS PID");
    assert_ne!(
        backend_1_pid, backend_2_pid,
        "restarted backend child must have a distinct OS PID"
    );
    let (backend_2_reaped, backend_2_exit_ok) =
        wait_and_reap_backend_child(&mut child_2, &mut parent_eof_rx).await;

    let wrapper_record = WrapperRecord {
        wrapper_pid,
        app_pid,
        backend_1_pid,
        backend_1_reaped,
        backend_1_exit_ok,
        backend_2_pid,
        backend_2_reaped,
        backend_2_exit_ok,
    };
    write_private_json(
        &spec.records_dir.join("wrapper-record.json"),
        &wrapper_record,
    );

    assert!(
        backend_2_reaped && backend_2_exit_ok,
        "backend-2 did not exit cleanly"
    );
}

#[test]
#[ignore = "internal child-process role entrypoint for native_reconnect_process integration test"]
fn native_reconnect_process_fixture_entrypoint() {
    let Some(role_os) = std::env::var_os(ENV_FIXTURE_ROLE) else {
        return;
    };
    let role = role_os.to_string_lossy().into_owned();
    let spec_path = PathBuf::from(
        std::env::var_os(ENV_FIXTURE_SPEC).expect("missing THREADLANE_RECONNECT_FIXTURE_SPEC"),
    );
    let spec: FixtureSpec = read_json_file(&spec_path);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime for fixture child process");

    rt.block_on(async move {
        match role.as_str() {
            "wrapper" => run_wrapper(spec_path, spec).await,
            "backend-1" => {
                let wrapper_pid: u32 = std::env::var(ENV_WRAPPER_PID)
                    .expect("missing wrapper PID env")
                    .parse()
                    .expect("valid wrapper PID");
                run_backend_1(spec, wrapper_pid).await;
            }
            "backend-2" => {
                let wrapper_pid: u32 = std::env::var(ENV_WRAPPER_PID)
                    .expect("missing wrapper PID env")
                    .parse()
                    .expect("valid wrapper PID");
                run_backend_2(spec, wrapper_pid).await;
            }
            other => panic!("unknown fixture role: {other}"),
        }
    });
}

struct WrapperProcessGuard {
    child: Child,
    stdin: Option<ChildStdin>,
    reaped: bool,
}

impl WrapperProcessGuard {
    fn spawn(current_exe: &Path, spec_path: &Path) -> Self {
        let mut cmd = Command::new(current_exe);
        cmd.args(["--ignored", "--exact", HELPER_TEST_NAME, "--nocapture"])
            .env(ENV_FIXTURE_ROLE, "wrapper")
            .env(ENV_FIXTURE_SPEC, spec_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = cmd.spawn().expect("spawn wrapper child process");
        let stdin = child.stdin.take();
        Self {
            child,
            stdin,
            reaped: false,
        }
    }

    fn id(&self) -> u32 {
        self.child.id().expect("wrapper child must have OS PID")
    }

    async fn wait_and_reap(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let _ = self.stdin.take();
        match tokio::time::timeout(timeout, self.child.wait()).await {
            Ok(Ok(status)) => {
                self.reaped = true;
                status
            }
            Ok(Err(err)) => {
                let _ = self.child.kill().await;
                let _ = self.child.wait().await;
                self.reaped = true;
                panic!("failed waiting on wrapper child process: {err}");
            }
            Err(_) => {
                let _ = self.child.kill().await;
                let _ = self.child.wait().await;
                self.reaped = true;
                panic!("timed out waiting for wrapper child process to exit");
            }
        }
    }
}

impl Drop for WrapperProcessGuard {
    fn drop(&mut self) {
        // Close stdin first so the wrapper observes EOF and kills/reaps any active backend child.
        let _ = self.stdin.take();
        if self.reaped {
            return;
        }

        let grace_deadline = Instant::now() + Duration::from_millis(750);
        while Instant::now() < grace_deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                self.reaped = true;
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let _ = self.child.start_kill();
        let kill_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < kill_deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                self.reaped = true;
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

async fn wait_for_file(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for fixture file {}", path.display());
}

fn assert_child_reaped_in_proc(pid: u32, parent_pid: u32) {
    let status_path = PathBuf::from(format!("/proc/{pid}/status"));
    let Ok(status_text) = fs::read_to_string(&status_path) else {
        return;
    };
    let ppid = status_text.lines().find_map(|line| {
        line.strip_prefix("PPid:")
            .and_then(|rest| rest.trim().parse::<u32>().ok())
    });
    assert_ne!(
        ppid,
        Some(parent_pid),
        "process {pid} is still present under parent {parent_pid} after reap"
    );
}

#[tokio::test]
async fn native_reconnect_recovers_across_real_child_backend_restart_under_wrapper() {
    let temp = tempfile::tempdir().expect("create tempdir");
    let root = fs::canonicalize(temp.path()).expect("canonicalize tempdir");

    let project_root = root.join("repo");
    let descriptor_root = root.join("runtime");
    let cfg_dir = root.join("cfg");
    let records_dir = root.join("records");
    for dir in [&project_root, &descriptor_root, &cfg_dir, &records_dir] {
        create_private_dir(dir);
    }

    let queue_marker = root.join("queue-ran.marker");
    let queue_executable = root.join("fake-codex-queue.sh");
    let queue_script = format!(
        "#!/bin/sh\necho ran > \"{}\"\nexit 0\n",
        queue_marker.display()
    );
    write_private_file_atomic(&queue_executable, queue_script.as_bytes());
    fs::set_permissions(&queue_executable, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 fake queue executable");

    let current_exe = fs::canonicalize(std::env::current_exe().expect("current_exe"))
        .expect("canonical current_exe");
    let app_pid = std::process::id();

    let spec = FixtureSpec {
        descriptor_root: descriptor_root.clone(),
        records_dir: records_dir.clone(),
        project_root: project_root.clone(),
        real_codex: current_exe.clone(),
        thread_id: TEST_THREAD_ID.to_string(),
        turn_id: TEST_TURN_ID.to_string(),
        model: FAKE_MODEL_ID.to_string(),
        effort: FAKE_EFFORT.to_string(),
    };
    let spec_path = records_dir.join("fixture-spec.json");
    write_private_json(&spec_path, &spec);

    let mut wrapper_guard = WrapperProcessGuard::spawn(&current_exe, &spec_path);
    let wrapper_pid = wrapper_guard.id();
    assert_ne!(wrapper_pid, app_pid, "wrapper must have its own OS PID");

    let ready_path = records_dir.join("backend-1-ready.json");
    wait_for_file(&ready_path, Duration::from_secs(5)).await;
    let backend_1_ready: BackendReadyRecord = read_json_file(&ready_path);
    assert_eq!(backend_1_ready.wrapper_pid, wrapper_pid);
    assert_ne!(backend_1_ready.backend_pid, wrapper_pid);
    assert_ne!(backend_1_ready.backend_pid, app_pid);

    let native_cfg = NativeToolOutputConfig {
        schema_version: NATIVE_TOOL_OUTPUT_CONFIG_SCHEMA_VERSION,
        socket_path: backend_1_ready.socket_path.clone(),
        thread_id: TEST_THREAD_ID.to_string(),
        project_root: project_root.clone(),
        model: FAKE_MODEL_ID.to_string(),
        effort: FAKE_EFFORT.to_string(),
        delivery: Default::default(),
        reconnect: Some(NativeReconnectConfig {
            descriptor_root: descriptor_root.clone(),
            backend_executable: current_exe.clone(),
            application_executable: current_exe.clone(),
        }),
    };
    native_cfg
        .validate()
        .expect("validate initial native reconnect config");

    let cfg_path = cfg_dir.join("native-tool-output.json");
    write_private_json(&cfg_path, &native_cfg);
    let loaded_cfg = NativeToolOutputConfig::load(&cfg_path).expect("load private native config");
    assert_eq!(loaded_cfg, native_cfg);
    let initial_digest = loaded_cfg.digest();

    let base_ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
        .expect("valid approval_id")
        .with_operator_cli_route();
    let expected_notice = base_ctx.wakeup_message();
    let approved_ctx = base_ctx
        .with_tool_output_transport(loaded_cfg.clone())
        .expect("attach native tool-output transport");
    let initial_ctx_snapshot = approved_ctx.clone();

    let mut notify_bindings = BTreeSet::new();
    notify_bindings.insert(TEST_BINDING_ID.to_string());
    let wakeup_cfg = CodexWakeupConfig {
        executable: queue_executable,
        notify_bindings,
        timeout: Duration::from_millis(WAKEUP_BUDGET_MS),
        approved_session: Some(approved_ctx),
    };
    wakeup_cfg.validate().expect("validate wakeup config");

    let claim = WakeupClaim {
        binding_id: TEST_BINDING_ID.to_string(),
        thread_uuid: TEST_THREAD_ID.to_string(),
        claim_id: "wcl_0123456789abcdef0123456789abcdef".to_string(),
        pending_since_seq: 5,
        covered_up_to_seq: 11,
        coalesced_count: 3,
        message: "UNTRUSTED_CLAIM_MESSAGE_NOT_FOR_TRANSPORT".to_string(),
        claimed_at_ms: 2_000,
    };

    let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Invoke wake-up exactly once across the child backend crash/exit and wrapper-managed restart.
    let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut shutdown_rx).await;
    assert_eq!(outcome, WakeupOutcome::Submitted);
    assert!(
        !queue_marker.exists(),
        "fallback queue executable must never run when native tool-output transport is configured"
    );

    // Keep the parent's liveness pipe open until both children have exited and
    // been reaped. Closing it earlier intentionally asks the wrapper to cancel them.
    wait_for_file(&records_dir.join("wrapper-record.json"), CHILD_STEP_TIMEOUT).await;
    let wrapper_status = wrapper_guard.wait_and_reap(CHILD_STEP_TIMEOUT).await;
    assert!(
        wrapper_status.success(),
        "wrapper child process must exit with zero status, got {wrapper_status}"
    );

    let backend_1_record: BackendSessionRecord =
        read_json_file(&records_dir.join("backend-1-record.json"));
    let backend_2_record: BackendSessionRecord =
        read_json_file(&records_dir.join("backend-2-record.json"));
    let wrapper_record: WrapperRecord = read_json_file(&records_dir.join("wrapper-record.json"));

    // 1. Verify actual OS process ancestry, distinct backend PIDs, and clean child reaping.
    assert_eq!(wrapper_record.app_pid, app_pid);
    assert_eq!(wrapper_record.wrapper_pid, wrapper_pid);
    assert_eq!(wrapper_record.backend_1_pid, backend_1_ready.backend_pid);
    assert_eq!(backend_1_record.backend_pid, wrapper_record.backend_1_pid);
    assert_eq!(backend_2_record.backend_pid, wrapper_record.backend_2_pid);
    assert_eq!(backend_1_record.ppid, wrapper_pid);
    assert_eq!(backend_2_record.ppid, wrapper_pid);
    assert_ne!(
        backend_1_record.backend_pid, backend_2_record.backend_pid,
        "restarted backend must run in a new child OS process with a different PID"
    );
    assert_ne!(backend_1_record.backend_pid, wrapper_pid);
    assert_ne!(backend_2_record.backend_pid, wrapper_pid);
    assert_ne!(backend_1_record.backend_pid, app_pid);
    assert_ne!(backend_2_record.backend_pid, app_pid);
    assert!(wrapper_record.backend_1_reaped && wrapper_record.backend_1_exit_ok);
    assert!(wrapper_record.backend_2_reaped && wrapper_record.backend_2_exit_ok);
    assert_child_reaped_in_proc(backend_1_record.backend_pid, wrapper_pid);
    assert_child_reaped_in_proc(backend_2_record.backend_pid, wrapper_pid);
    assert_child_reaped_in_proc(wrapper_pid, app_pid);

    // 2. Verify new session directory / socket and preserved stale first descriptor.
    assert_ne!(backend_1_record.session_dir, backend_2_record.session_dir);
    assert_ne!(backend_1_record.socket_path, backend_2_record.socket_path);
    assert!(
        backend_1_record.descriptor_path.exists(),
        "first backend descriptor must remain on disk to exercise stale-PID candidate ignore"
    );
    assert!(backend_2_record.descriptor_path.exists());

    for dir in [
        &descriptor_root,
        &backend_1_record.session_dir,
        &backend_2_record.session_dir,
    ] {
        let mode = fs::symlink_metadata(dir)
            .expect("stat private directory")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "directory {} must be 0700", dir.display());
    }
    for file in [
        &backend_1_record.descriptor_path,
        &backend_2_record.descriptor_path,
        &backend_1_record.socket_path,
        &backend_2_record.socket_path,
    ] {
        let mode = fs::symlink_metadata(file)
            .expect("stat private descriptor or socket")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "path {} must be 0600", file.display());
    }

    // 3. Verify >= 2 connection handshakes and exact protocol progression across both backends.
    assert_eq!(backend_1_record.handshakes, 1);
    assert_eq!(backend_2_record.handshakes, 1);
    let total_handshakes = backend_1_record.handshakes + backend_2_record.handshakes;
    assert!(
        total_handshakes >= 2,
        "expected at least 2 WebSocket connection handshakes across restart, got {total_handshakes}"
    );
    assert_eq!(
        backend_1_record.methods_seen,
        vec!["initialize".to_string()]
    );
    assert_eq!(
        backend_2_record.methods_seen,
        vec![
            "initialize".to_string(),
            "initialized".to_string(),
            "thread/read".to_string(),
            "thread/resume".to_string(),
            "turn/start".to_string(),
        ]
    );

    // 4. Verify exactly ONE turn/start across both backends and validate its payload.
    assert_eq!(backend_1_record.turn_start_count, 0);
    assert!(backend_1_record.turn_start_requests.is_empty());
    assert_eq!(backend_2_record.turn_start_count, 1);
    assert_eq!(backend_2_record.turn_start_requests.len(), 1);
    assert_eq!(
        backend_1_record.turn_start_count + backend_2_record.turn_start_count,
        1
    );

    let turn_params = &backend_2_record.turn_start_requests[0]["params"];
    assert_eq!(turn_params["threadId"], TEST_THREAD_ID);
    assert_eq!(turn_params["input"], json!([]));
    assert!(turn_params.get("model").is_none());
    assert!(turn_params.get("effort").is_none());
    assert_eq!(turn_params["toolOutput"]["name"], NATIVE_TOOL_OUTPUT_NAME);
    assert_eq!(
        turn_params["toolOutput"]["namespace"],
        NATIVE_TOOL_OUTPUT_NAMESPACE
    );
    let parsed_output: Value = serde_json::from_str(
        turn_params["toolOutput"]["output"]
            .as_str()
            .expect("toolOutput.output string"),
    )
    .expect("parse toolOutput.output JSON");
    assert_eq!(
        parsed_output,
        json!({
            "untrusted_data": true,
            "approval_id": TEST_APPROVAL_ID,
            "claim_id": claim.claim_id,
            "pending_since_seq": claim.pending_since_seq,
            "covered_up_to_seq": claim.covered_up_to_seq,
            "coalesced_count": claim.coalesced_count,
            "notice": expected_notice,
        })
    );

    // 5. Verify original notification context and configuration digest remained unchanged.
    let post_ctx = wakeup_cfg
        .approved_session
        .as_ref()
        .expect("approved_session context present");
    assert_eq!(post_ctx, &initial_ctx_snapshot);
    assert_eq!(post_ctx.wakeup_message(), expected_notice);
    let post_transport = post_ctx
        .tool_output_transport()
        .expect("native tool-output transport present");
    assert_eq!(post_transport, &loaded_cfg);
    assert_eq!(post_transport.socket_path, backend_1_ready.socket_path);
    assert_eq!(post_transport.digest(), initial_digest);

    let reloaded_cfg =
        NativeToolOutputConfig::load(&cfg_path).expect("reload persisted native config");
    assert_eq!(reloaded_cfg, loaded_cfg);
    assert_eq!(reloaded_cfg.digest(), initial_digest);
}
