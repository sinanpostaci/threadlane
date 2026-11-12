#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{symlink, DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::{NativeReconnectConfig, Supervisor, SupervisorConfig, SupervisorHandle};
use threadlane_core::observation::{
    serialize_workspace_native_sample_sidecar, ObservationProcessInstance, WorkspaceActivityState,
    WorkspaceAvailabilityState, WorkspaceCapabilityState, WorkspaceChatIdentity,
    WorkspaceConfiguredSelection, WorkspaceConnectionState, WorkspaceFreshnessState,
    WorkspaceNativeCapabilities, WorkspaceNativeSample, WorkspaceSampleConnection,
    WorkspaceSampleReconcileDecision, WorkspaceSamplingPolicy, WorkspaceSelectionEvidence,
    MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE, MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
    WORKSPACE_METADATA_TIMEOUT_MS, WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
    WORKSPACE_SAMPLING_INTERVAL_MS,
};
use threadlane_native::{
    compute_observation_source_id, generate_sample_producer_epoch, native_chat_sample_path,
    native_chat_samples_dir, prove_native_thread_activity, read_backend_process_instance,
    read_workspace_native_sample_readonly, read_workspace_native_samples_readonly,
    write_workspace_native_sample_sidecar, ManagedParentConnectionConfig, NativeErrorCode,
    NativeThreadStatusDetail, OperatorEnrollRequest, PairedMcpServer, PairingStore,
    NATIVE_CHAT_SAMPLES_LOCK_FILENAME, NATIVE_CHAT_SAMPLE_ROUTE,
};
use tokio::net::UnixListener;
use tokio_tungstenite::tungstenite::protocol::Message;

const THREAD_A: &str = "0196ed00-0000-7000-8000-00000000000a";
const THREAD_B: &str = "0196ed00-0000-7000-8000-00000000000b";
const THREAD_C: &str = "0196ed00-0000-7000-8000-00000000000c";

fn read_ppid(pid: u32) -> u32 {
    let stat_path = format!("/proc/{pid}/stat");
    let raw = fs::read_to_string(&stat_path).expect("read /proc/<pid>/stat");
    let rparen = raw.rfind(')').expect("closing paren in /proc/<pid>/stat");
    let after = &raw[rparen + 1..];
    let mut fields = after.split_whitespace();
    let _state = fields.next().expect("process state field");
    let ppid_str = fields.next().expect("process ppid field");
    ppid_str.parse::<u32>().expect("numeric ppid")
}

#[derive(Clone)]
struct MockThreadState {
    reported_id: String,
    project_id: Option<String>,
    title: String,
    cwd: String,
    status_json: Value,
    model: Option<String>,
    effort: Option<String>,
    artificial_delay: Duration,
    disconnect_immediately: bool,
}

#[derive(Clone)]
struct BoundMcpIdentity {
    pairing_store: PairingStore,
    connection_nonce: String,
    epoch_id: String,
}

struct MockParentState {
    threads: BTreeMap<String, MockThreadState>,
    mcp_identities: BTreeMap<String, BoundMcpIdentity>,
    thread_read_counts: BTreeMap<String, usize>,
    saw_include_turns_true: bool,
}

struct ObservationTestHarness {
    _temp: tempfile::TempDir,
    root: PathBuf,
    project_root: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    anchor_exe: PathBuf,
    descriptor_root: PathBuf,
    session_dir: PathBuf,
    backend_pid: u32,
    backend_executable: PathBuf,
    application_executable: PathBuf,
    pairing_store: PairingStore,
    _supervisor: SupervisorHandle,
    mock_state: Arc<Mutex<MockParentState>>,
    server_task: tokio::task::JoinHandle<()>,
    stop_accepting: Arc<AtomicBool>,
}

impl Drop for ObservationTestHarness {
    fn drop(&mut self) {
        self.stop_accepting.store(true, Ordering::Release);
        self.server_task.abort();
    }
}

impl ObservationTestHarness {
    async fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");

        let project_root = root.join("workspace-repo");
        fs::create_dir(&project_root).expect("create workspace-repo");

        let mk_private = |name: &str| -> PathBuf {
            let p = root.join(name);
            DirBuilder::new()
                .mode(0o700)
                .create(&p)
                .expect("create private dir");
            p
        };

        let state_dir = mk_private("runtime-state");
        let pairing_dir = mk_private("pairing-state");
        let registry_dir = mk_private("registry-store");
        let plans_dir = mk_private("plans-store");
        let authority_dir = mk_private("authority-store");
        let sessions_dir = mk_private("sessions-store");
        let sock_dir = mk_private("sock-dir");
        let descriptor_root = mk_private("descriptor-root");
        let bin_dir = mk_private("fake-bin");

        // Write a closed 0700 placeholder sibling `threadlane` executable so `ControlRuntimePolicy`
        // validates executable path safety while reusing the already-running in-process `Supervisor`.
        let fake_threadlane = bin_dir.join("threadlane");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o700)
                .open(&fake_threadlane)
                .expect("create fake threadlane binary");
            f.write_all(b"#!/bin/sh\nexit 0\n")
                .expect("write fake threadlane binary");
            f.sync_all().expect("sync fake threadlane binary");
        }
        let anchor_exe = bin_dir.join("threadlane-native-mcp");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o700)
                .open(&anchor_exe)
                .expect("create anchor binary");
            f.write_all(b"#!/bin/sh\nexit 0\n")
                .expect("write anchor binary");
            f.sync_all().expect("sync anchor binary");
        }

        let supervisor = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(40),
            codex: None,
            workers: None,
        })
        .await
        .expect("start in-process supervisor");

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");

        let backend_pid = std::process::id();
        let wrapper_pid = read_ppid(backend_pid);
        let app_pid = read_ppid(wrapper_pid);
        let backend_executable =
            fs::canonicalize("/proc/self/exe").expect("canonical backend executable");
        let application_executable = fs::canonicalize(format!("/proc/{app_pid}/exe"))
            .expect("canonical application executable");

        let physical_sock = sock_dir.join("codex-parent.sock");
        let listener = UnixListener::bind(&physical_sock).expect("bind unix socket");
        fs::set_permissions(&physical_sock, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 socket");

        let staging_dir = root.join("staging-session");
        DirBuilder::new()
            .mode(0o700)
            .create(&staging_dir)
            .expect("create staging dir");
        let session_dir = descriptor_root.join("tl-codex-main");
        let advertised_sock = session_dir.join("codex.sock");
        symlink(&physical_sock, staging_dir.join("codex.sock")).expect("symlink socket");

        let descriptor_json = serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "wrapper_pid": wrapper_pid,
            "backend_pid": backend_pid,
            "socket_path": advertised_sock,
            "real_codex": backend_executable,
            "transport": "unix_websocket",
        }))
        .expect("serialize descriptor");
        {
            let mut desc_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(staging_dir.join("descriptor.json"))
                .expect("create descriptor.json");
            desc_file
                .write_all(&descriptor_json)
                .expect("write descriptor.json");
            desc_file.sync_all().expect("sync descriptor.json");
        }
        fs::rename(&staging_dir, &session_dir).expect("publish session dir");

        let mock_state = Arc::new(Mutex::new(MockParentState {
            threads: BTreeMap::new(),
            mcp_identities: BTreeMap::new(),
            thread_read_counts: BTreeMap::new(),
            saw_include_turns_true: false,
        }));
        let state_for_loop = Arc::clone(&mock_state);
        let stop_accepting = Arc::new(AtomicBool::new(false));
        let stop_for_loop = Arc::clone(&stop_accepting);

        let server_task = tokio::spawn(async move {
            loop {
                if stop_for_loop.load(Ordering::Acquire) {
                    break;
                }
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                if stop_for_loop.load(Ordering::Acquire) {
                    drop(stream);
                    break;
                }
                let conn_state = Arc::clone(&state_for_loop);
                tokio::spawn(async move {
                    handle_mock_ws_connection(stream, conn_state).await;
                });
            }
        });

        Self {
            _temp: temp,
            root,
            project_root,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            anchor_exe,
            descriptor_root,
            session_dir,
            backend_pid,
            backend_executable,
            application_executable,
            pairing_store,
            _supervisor: supervisor,
            mock_state,
            server_task,
            stop_accepting,
        }
    }

    fn reconnect_config(&self) -> NativeReconnectConfig {
        NativeReconnectConfig {
            descriptor_root: self.descriptor_root.clone(),
            backend_executable: self.backend_executable.clone(),
            application_executable: self.application_executable.clone(),
        }
    }

    fn build_full_mcp_server(&self, parent_cfg: ManagedParentConnectionConfig) -> PairedMcpServer {
        PairedMcpServer::new(&self.state_dir, self.pairing_store.clone())
            .expect("create PairedMcpServer")
            .with_sibling_anchor_exe(&self.anchor_exe)
            .with_workflow_dirs(&self.registry_dir, &self.plans_dir)
            .expect("configure workflow dirs")
            .with_host_dirs(&self.authority_dir, &self.sessions_dir)
            .expect("configure host dirs")
            .with_managed_parent_config(parent_cfg)
    }

    fn register_thread(&self, thread_id: &str, state: MockThreadState, server: &PairedMcpServer) {
        let mut guard = self.mock_state.lock().expect("lock mock state");
        guard.threads.insert(thread_id.to_string(), state);
        guard.mcp_identities.insert(
            thread_id.to_string(),
            BoundMcpIdentity {
                pairing_store: self.pairing_store.clone(),
                connection_nonce: server.connection_nonce().to_string(),
                epoch_id: server.epoch_id().to_string(),
            },
        );
    }

    fn update_thread<F>(&self, thread_id: &str, mutate: F)
    where
        F: FnOnce(&mut MockThreadState),
    {
        let mut guard = self.mock_state.lock().expect("lock mock state");
        let entry = guard
            .threads
            .get_mut(thread_id)
            .expect("registered thread state");
        mutate(entry);
    }

    fn update_mcp_identity(&self, thread_id: &str, server: &PairedMcpServer) {
        let mut guard = self.mock_state.lock().expect("lock mock state");
        guard.mcp_identities.insert(
            thread_id.to_string(),
            BoundMcpIdentity {
                pairing_store: self.pairing_store.clone(),
                connection_nonce: server.connection_nonce().to_string(),
                epoch_id: server.epoch_id().to_string(),
            },
        );
    }

    fn thread_read_count(&self, thread_id: &str) -> usize {
        let guard = self.mock_state.lock().expect("lock mock state");
        guard
            .thread_read_counts
            .get(thread_id)
            .copied()
            .unwrap_or(0)
    }

    fn saw_include_turns_true(&self) -> bool {
        self.mock_state
            .lock()
            .expect("lock mock state")
            .saw_include_turns_true
    }

    fn retire_parent_endpoint(&self) {
        self.stop_accepting.store(true, Ordering::Release);
        self.server_task.abort();
        let retired = self.root.join("retired-parent-endpoint");
        if self.session_dir.exists() {
            let _ = fs::rename(&self.session_dir, retired);
        }
    }
}

async fn handle_mock_ws_connection(
    stream: tokio::net::UnixStream,
    mock_state: Arc<Mutex<MockParentState>>,
) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };

    let Some(Ok(Message::Text(init_text))) = ws.next().await else {
        return;
    };
    let Ok(init_val) = serde_json::from_str::<Value>(&init_text) else {
        return;
    };
    let init_id = init_val.get("id").cloned().unwrap_or_else(|| json!(1));
    if ws
        .send(Message::Text(
            json!({
                "id": init_id,
                "result": {
                    "userAgent": "codex-app-server/0.165.0",
                    "codexHome": "/tmp/codex-home",
                    "platformFamily": "unix",
                    "platformOs": "linux"
                }
            })
            .to_string()
            .into(),
        ))
        .await
        .is_err()
    {
        return;
    }

    // Consume `initialized` notification.
    let Some(Ok(Message::Text(_))) = ws.next().await else {
        return;
    };

    // Read target RPC request.
    let Some(Ok(Message::Text(req_text))) = ws.next().await else {
        return;
    };
    let Ok(req_val) = serde_json::from_str::<Value>(&req_text) else {
        return;
    };
    let req_id = req_val.get("id").cloned().unwrap_or_else(|| json!(2));
    let method = req_val
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = req_val.get("params").cloned().unwrap_or_else(|| json!({}));

    match method {
        "thread/read" => {
            let thread_id = params
                .get("threadId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let include_turns = params
                .get("includeTurns")
                .and_then(Value::as_bool)
                .unwrap_or(true);

            let snapshot = {
                let mut guard = mock_state.lock().expect("lock mock state");
                if include_turns {
                    guard.saw_include_turns_true = true;
                }
                *guard
                    .thread_read_counts
                    .entry(thread_id.clone())
                    .or_insert(0) += 1;
                guard.threads.get(&thread_id).cloned()
            };

            let Some(state) = snapshot else {
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": req_id,
                            "error": { "code": -32602, "message": "thread not found" }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                return;
            };

            if state.disconnect_immediately {
                return;
            }
            if !state.artificial_delay.is_zero() {
                tokio::time::sleep(state.artificial_delay).await;
            }

            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": req_id,
                        "result": {
                            "thread": {
                                "id": state.reported_id,
                                "projectId": state.project_id,
                                "name": state.title,
                                "cwd": state.cwd,
                                "status": state.status_json,
                                "model": state.model,
                                "reasoningEffort": state.effort,
                                "canAcceptDirectInput": true
                            }
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
        }
        "mcpServerStatus/list" => {
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": req_id,
                        "result": {
                            "data": [{
                                "name": "threadlane",
                                "authStatus": "notLoggedIn",
                                "runtimeStatus": "connected",
                                "tools": {
                                    "connection_identity": {}
                                }
                            }]
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
        }
        "mcpServer/tool/call" => {
            let thread_id = params
                .get("threadId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let bound = {
                let guard = mock_state.lock().expect("lock mock state");
                guard.mcp_identities.get(thread_id).cloned()
            };
            let Some(bound) = bound else {
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": req_id,
                            "error": { "code": -32602, "message": "unknown thread MCP server" }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                return;
            };
            let live_identity = bound
                .pairing_store
                .inspect_epoch(&bound.connection_nonce, &bound.epoch_id)
                .expect("inspect bound epoch");
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": req_id,
                        "result": {
                            "content": [],
                            "structuredContent": {
                                "untrusted_data": false,
                                "tool": "connection_identity",
                                "data": live_identity
                            },
                            "isError": false
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
        }
        _ => {
            let _ = ws
                .send(Message::Text(
                    json!({
                        "id": req_id,
                        "error": { "code": -32601, "message": "unsupported method" }
                    })
                    .to_string()
                    .into(),
                ))
                .await;
        }
    }

    let _ = ws.next().await;
}

async fn wait_for_sample<F>(
    state_dir: &Path,
    identity: &WorkspaceChatIdentity,
    timeout: Duration,
    predicate: F,
) -> WorkspaceNativeSample
where
    F: Fn(&WorkspaceNativeSample) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let now_ms = threadlane_core::observation::current_observation_time_ms();
        if let Ok(Some(entry)) = read_workspace_native_sample_readonly(state_dir, identity, now_ms)
        {
            if predicate(&entry.sample) {
                return entry.sample;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for expected WorkspaceNativeSample for {identity:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn sampler_requires_verified_connect_current_chat_and_deduplicates_repeat_connect() {
    let harness = ObservationTestHarness::new().await;
    let parent_cfg = ManagedParentConnectionConfig::new(
        harness.reconnect_config(),
        harness.backend_pid,
        &harness.project_root,
    )
    .with_sampling_policy(WorkspaceSamplingPolicy {
        interval_ms: 150,
        metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
        stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
        snapshot_resets_sample_age: false,
    });

    let server = harness.build_full_mcp_server(parent_cfg);
    harness.register_thread(
        THREAD_A,
        MockThreadState {
            reported_id: THREAD_A.to_string(),
            project_id: Some("proj_main".to_string()),
            title: "Primary Chat".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            status_json: json!({ "type": "idle" }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server,
    );

    // 1. Metadata hints (`_meta.threadId`) on `connection_identity` or unbound tool calls must NEVER start a sampler.
    let _ = server
        .invoke_tool_json(
            "connection_identity",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("connection_identity succeeds");
    let _ = server
        .invoke_tool_json("status", json!({}), Some(json!({ "threadId": THREAD_A })))
        .await
        .expect_err("unbound status rejected");
    assert!(!server.has_active_native_chat_sampler());
    assert!(!native_chat_samples_dir(&harness.state_dir).exists());

    // 2. Failed `connect_current_chat` (wrong thread hint not matching MCP connection) must NEVER start a sampler.
    let wrong_connect = server
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_C })),
        )
        .await;
    assert!(wrong_connect.is_err());
    assert!(!server.has_active_native_chat_sampler());
    assert!(!native_chat_samples_dir(&harness.state_dir).exists());

    // 3. Manual `PairingStore::enroll` on a separate server without `connect_current_chat` must NEVER start a sampler.
    let manual_server = PairedMcpServer::new(&harness.state_dir, harness.pairing_store.clone())
        .expect("manual server");
    let op_token = harness.pairing_store.operator_token().expect("op token");
    harness
        .pairing_store
        .enroll(
            &op_token,
            OperatorEnrollRequest {
                connection_nonce: manual_server.connection_nonce().to_string(),
                epoch_id: manual_server.epoch_id().to_string(),
                binding_id: "bnd_manual_only".to_string(),
                thread_id: THREAD_C.to_string(),
                project_id: Some("proj_main".to_string()),
            },
        )
        .expect("manual enroll");
    assert!(!manual_server.has_active_native_chat_sampler());
    assert!(!native_chat_samples_dir(&harness.state_dir).exists());

    // 4. Verified `connect_current_chat` starts ONE sampler and writes initial `0600` sample in `0700` dir.
    let connect_res = server
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("verified connect_current_chat")
        .structured_content
        .expect("structured connect output");
    let binding_id = connect_res["data"]["binding_id"]
        .as_str()
        .expect("binding_id")
        .to_string();

    assert!(server.has_active_native_chat_sampler());
    let first_epoch = server
        .active_native_chat_producer_epoch()
        .expect("active producer epoch");

    let expected_source_id = compute_observation_source_id(
        &harness.plans_dir,
        &harness.authority_dir,
        &harness.sessions_dir,
    )
    .expect("compute source_id");
    assert!(expected_source_id.starts_with("src_"));
    assert_eq!(expected_source_id.len(), 68);
    assert_eq!(
        server
            .resolve_observation_source_id(&harness.project_root)
            .expect("server source_id"),
        expected_source_id
    );

    let identity_a = WorkspaceChatIdentity::new(
        &expected_source_id,
        &binding_id,
        THREAD_A,
        harness.project_root.to_str().expect("utf8 root"),
    )
    .expect("chat identity A");

    let initial_sample = wait_for_sample(
        &harness.state_dir,
        &identity_a,
        Duration::from_secs(2),
        |s| s.sequence >= 1,
    )
    .await;
    assert_eq!(initial_sample.producer_epoch, first_epoch);
    assert_eq!(
        initial_sample.availability,
        WorkspaceAvailabilityState::Available
    );
    assert_eq!(initial_sample.activity, WorkspaceActivityState::Idle);
    assert_eq!(
        initial_sample
            .configured_selection
            .selected_model
            .as_deref(),
        Some("gpt-5.4")
    );
    assert_eq!(
        initial_sample
            .configured_selection
            .selected_effort
            .as_deref(),
        Some("high")
    );
    assert_eq!(
        initial_sample.configured_selection.evidence,
        WorkspaceSelectionEvidence::VerifiedConfiguration
    );
    assert!(
        !initial_sample
            .configured_selection
            .counts_as_runtime_telemetry
    );

    let samples_dir = native_chat_samples_dir(&harness.state_dir);
    let sample_file =
        native_chat_sample_path(&harness.state_dir, &identity_a).expect("sample path");
    assert_eq!(
        fs::symlink_metadata(&samples_dir)
            .expect("stat samples_dir")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::symlink_metadata(&sample_file)
            .expect("stat sample_file")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    // 5. Re-running `connect_current_chat` reuses the single running sampler without rotating `producer_epoch`.
    let seq_before_repeat = initial_sample.sequence;
    let repeat_res = server
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("repeat connect_current_chat succeeds");
    let repeat_binding = repeat_res.structured_content.unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(repeat_binding, binding_id);
    assert_eq!(
        server.active_native_chat_producer_epoch().as_deref(),
        Some(first_epoch.as_str())
    );

    let after_repeat = wait_for_sample(
        &harness.state_dir,
        &identity_a,
        Duration::from_secs(2),
        |s| s.sequence > seq_before_repeat,
    )
    .await;
    assert_eq!(after_repeat.producer_epoch, first_epoch);
    assert!(!harness.saw_include_turns_true());

    server.stop_native_chat_sampler();
}

#[tokio::test]
async fn co_scoped_two_chats_update_live_between_mcp_calls_and_separate_proven_waiting_flags() {
    let harness = ObservationTestHarness::new().await;
    let fast_policy = WorkspaceSamplingPolicy {
        interval_ms: 120,
        metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
        stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
        snapshot_resets_sample_age: false,
    };

    let server_a = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_sampling_policy(fast_policy.clone()),
    );
    let server_b = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_sampling_policy(fast_policy),
    );

    harness.register_thread(
        THREAD_A,
        MockThreadState {
            reported_id: THREAD_A.to_string(),
            project_id: Some("proj_shared".to_string()),
            title: "Co-Scoped Chat A".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            status_json: json!({ "type": "active", "activeFlags": [] }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server_a,
    );
    harness.register_thread(
        THREAD_B,
        MockThreadState {
            reported_id: THREAD_B.to_string(),
            project_id: Some("proj_shared".to_string()),
            title: "Co-Scoped Chat B".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            // Plain "active" lacks activeFlags -> must degrade activity to `Unknown`, never `Working`!
            status_json: json!("active"),
            model: Some("gpt-5.4-mini".to_string()),
            effort: Some("low".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server_b,
    );

    let binding_a = server_a
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("connect chat A")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();

    let binding_b = server_b
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_B })),
        )
        .await
        .expect("connect chat B")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(binding_a, binding_b);

    let source_id = compute_observation_source_id(
        &harness.plans_dir,
        &harness.authority_dir,
        &harness.sessions_dir,
    )
    .expect("source_id");
    let cwd_str = harness.project_root.to_str().unwrap();
    let id_a = WorkspaceChatIdentity::new(&source_id, &binding_a, THREAD_A, cwd_str).unwrap();
    let id_b = WorkspaceChatIdentity::new(&source_id, &binding_b, THREAD_B, cwd_str).unwrap();

    // Initial states: Chat A is proven `Working`; Chat B with plain `"active"` is `Unknown`.
    let sample_a1 = wait_for_sample(&harness.state_dir, &id_a, Duration::from_secs(2), |s| {
        s.activity == WorkspaceActivityState::Working
    })
    .await;
    assert_eq!(
        sample_a1.configured_selection.selected_model.as_deref(),
        Some("gpt-5.4")
    );
    assert!(!sample_a1.configured_selection.counts_as_runtime_telemetry);

    let sample_b1 = wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
        s.sequence >= 1
    })
    .await;
    assert_eq!(
        sample_b1.availability,
        WorkspaceAvailabilityState::Available
    );
    assert_eq!(sample_b1.activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        sample_b1.configured_selection.selected_model.as_deref(),
        Some("gpt-5.4-mini")
    );

    // Without ANY MCP calls, switch Chat A model + waitingOnApproval, and Chat B to unknown flag.
    harness.update_thread(THREAD_A, |st| {
        st.model = Some("gpt-5.4-pro".to_string());
        st.effort = Some("xhigh".to_string());
        st.status_json = json!({ "type": "active", "activeFlags": ["waitingOnApproval"] });
    });
    harness.update_thread(THREAD_B, |st| {
        st.status_json = json!({ "type": "active", "activeFlags": ["mysteryFutureFlag"] });
    });

    let sample_a2 = wait_for_sample(&harness.state_dir, &id_a, Duration::from_secs(2), |s| {
        s.activity == WorkspaceActivityState::WaitingForApproval
            && s.configured_selection.selected_model.as_deref() == Some("gpt-5.4-pro")
    })
    .await;
    assert_eq!(
        sample_a2.configured_selection.selected_effort.as_deref(),
        Some("xhigh")
    );
    assert_eq!(
        sample_a2.configured_selection.evidence,
        WorkspaceSelectionEvidence::VerifiedConfiguration
    );
    assert!(!sample_a2.configured_selection.counts_as_runtime_telemetry);
    assert!(sample_a2.sequence > sample_a1.sequence);

    let sample_b2 = wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
        s.sequence > sample_b1.sequence
    })
    .await;
    assert_eq!(sample_b2.activity, WorkspaceActivityState::Unknown);

    // Next, switch Chat A -> `waitingOnUserInput` and Chat B -> conflicting flags (must remain `Unknown`).
    harness.update_thread(THREAD_A, |st| {
        st.status_json = json!({ "type": "active", "activeFlags": ["waitingOnUserInput"] });
    });
    harness.update_thread(THREAD_B, |st| {
        st.status_json =
            json!({ "type": "active", "activeFlags": ["waitingOnApproval", "waitingOnUserInput"] });
    });

    let sample_a3 = wait_for_sample(&harness.state_dir, &id_a, Duration::from_secs(2), |s| {
        s.activity == WorkspaceActivityState::WaitingForInput
    })
    .await;
    assert!(sample_a3.sequence > sample_a2.sequence);

    let sample_b3 = wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
        s.sequence > sample_b2.sequence
    })
    .await;
    assert_eq!(sample_b3.activity, WorkspaceActivityState::Unknown);

    // Switch Chat B -> `idle`, verify both chats coexist cleanly in `read_workspace_native_samples_readonly`.
    harness.update_thread(THREAD_B, |st| {
        st.status_json = json!({ "type": "idle" });
    });
    let _ = wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
        s.activity == WorkspaceActivityState::Idle
    })
    .await;

    let report = read_workspace_native_samples_readonly(
        &harness.state_dir,
        Some(&source_id),
        threadlane_core::observation::current_observation_time_ms(),
    )
    .expect("read co-scoped samples");
    assert_eq!(report.entries.len(), 2);
    assert_eq!(report.omitted_count, 0);
    assert!(report.warnings.is_empty());
    assert!(!harness.saw_include_turns_true());

    server_a.stop_native_chat_sampler();
    server_b.stop_native_chat_sampler();
}

#[tokio::test]
async fn wrong_thread_or_project_scope_marks_unavailable_and_suppresses_stale_model() {
    let harness = ObservationTestHarness::new().await;
    let server = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_sampling_policy(WorkspaceSamplingPolicy {
            interval_ms: 120,
            metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
            stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
            snapshot_resets_sample_age: false,
        }),
    );

    harness.register_thread(
        THREAD_A,
        MockThreadState {
            reported_id: THREAD_A.to_string(),
            project_id: Some("proj_alpha".to_string()),
            title: "Scoped Chat".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            status_json: json!({ "type": "active", "activeFlags": [] }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server,
    );

    let binding_id = server
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("connect")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();

    let source_id = compute_observation_source_id(
        &harness.plans_dir,
        &harness.authority_dir,
        &harness.sessions_dir,
    )
    .unwrap();
    let identity = WorkspaceChatIdentity::new(
        &source_id,
        &binding_id,
        THREAD_A,
        harness.project_root.to_str().unwrap(),
    )
    .unwrap();

    let good = wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
        s.availability == WorkspaceAvailabilityState::Available
    })
    .await;
    let initial_last_success = good.last_success_at_ms.expect("initial last_success_at_ms");

    // 1. Wrong `project_id` on subsequent metadata poll must mark `Unavailable`, `Unknown`, and clear model.
    harness.update_thread(THREAD_A, |st| {
        st.project_id = Some("proj_intruder".to_string());
        st.model = Some("spoofed-model".to_string());
    });
    let mismatch_proj =
        wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
            s.sequence > good.sequence && s.availability == WorkspaceAvailabilityState::Unavailable
        })
        .await;
    assert_eq!(mismatch_proj.activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        mismatch_proj.configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );
    assert_eq!(mismatch_proj.last_success_at_ms, Some(initial_last_success));

    // 2. Wrong `cwd` (different project directory) also stays `Unavailable` with `Unknown` activity and `None` model.
    harness.update_thread(THREAD_A, |st| {
        st.project_id = Some("proj_alpha".to_string());
        st.cwd = "/tmp/unrelated-checkout".to_string();
    });
    let mismatch_cwd =
        wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
            s.sequence > mismatch_proj.sequence
        })
        .await;
    assert_eq!(
        mismatch_cwd.availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(mismatch_cwd.activity, WorkspaceActivityState::Unknown);
    assert_eq!(mismatch_cwd.configured_selection.selected_model, None);

    // 3. Wrong `reported_id` (`ThreadNotFound` mismatch in `NativeClient::thread_metadata`) also degrades cleanly.
    harness.update_thread(THREAD_A, |st| {
        st.cwd = harness.project_root.to_string_lossy().into_owned();
        st.reported_id = THREAD_B.to_string();
    });
    let mismatch_thread =
        wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
            s.sequence > mismatch_cwd.sequence
        })
        .await;
    assert_eq!(
        mismatch_thread.availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(mismatch_thread.activity, WorkspaceActivityState::Unknown);
    assert_eq!(mismatch_thread.configured_selection.selected_model, None);

    server.stop_native_chat_sampler();
}

#[tokio::test]
async fn metadata_timeout_750ms_disconnect_and_10000ms_staleness_clear_activity_and_selection() {
    let default_policy = WorkspaceSamplingPolicy::default();
    assert_eq!(default_policy.interval_ms, 2_000);
    assert_eq!(default_policy.interval_ms, WORKSPACE_SAMPLING_INTERVAL_MS);
    assert_eq!(default_policy.metadata_timeout_ms, 750);
    assert_eq!(
        default_policy.metadata_timeout_ms,
        WORKSPACE_METADATA_TIMEOUT_MS
    );
    assert_eq!(default_policy.stale_threshold_ms, 10_000);
    assert_eq!(
        default_policy.stale_threshold_ms,
        WORKSPACE_SAMPLE_STALE_THRESHOLD_MS
    );

    let harness = ObservationTestHarness::new().await;
    // Exercise the exact 750ms metadata timeout!
    let server = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_sampling_policy(WorkspaceSamplingPolicy {
            interval_ms: 150,
            metadata_timeout_ms: 750,
            stale_threshold_ms: 10_000,
            snapshot_resets_sample_age: false,
        }),
    );

    harness.register_thread(
        THREAD_A,
        MockThreadState {
            reported_id: THREAD_A.to_string(),
            project_id: Some("proj_timeout".to_string()),
            title: "Timeout Chat".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            status_json: json!({ "type": "active", "activeFlags": [] }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server,
    );

    let binding_id = server
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("connect")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();

    let source_id = compute_observation_source_id(
        &harness.plans_dir,
        &harness.authority_dir,
        &harness.sessions_dir,
    )
    .unwrap();
    let identity = WorkspaceChatIdentity::new(
        &source_id,
        &binding_id,
        THREAD_A,
        harness.project_root.to_str().unwrap(),
    )
    .unwrap();

    let initial = wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
        s.availability == WorkspaceAvailabilityState::Available
    })
    .await;
    assert_eq!(initial.activity, WorkspaceActivityState::Working);
    assert_eq!(
        initial.configured_selection.selected_model.as_deref(),
        Some("gpt-5.4")
    );

    // 1. Stall `thread/read` for 1,100ms (> 750ms timeout).
    harness.update_thread(THREAD_A, |st| {
        st.artificial_delay = Duration::from_millis(1_100);
    });
    let timed_out = wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(3), |s| {
        s.sequence > initial.sequence && s.connection.state == WorkspaceConnectionState::Degraded
    })
    .await;
    assert_eq!(
        timed_out.availability,
        WorkspaceAvailabilityState::Unavailable
    );
    assert_eq!(timed_out.activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        timed_out.configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );
    assert_eq!(timed_out.last_success_at_ms, initial.last_success_at_ms);

    // 2. Restore normal latency and verify live recovery to `Available` (`Idle`).
    harness.update_thread(THREAD_A, |st| {
        st.artificial_delay = Duration::ZERO;
        st.status_json = json!({ "type": "idle" });
    });
    let recovered = wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(3), |s| {
        s.sequence > timed_out.sequence && s.availability == WorkspaceAvailabilityState::Available
    })
    .await;
    assert_eq!(recovered.activity, WorkspaceActivityState::Idle);
    assert_eq!(
        recovered.configured_selection.selected_model.as_deref(),
        Some("gpt-5.4")
    );

    // 3. Evaluate the `Available` sample past the 10,000ms staleness threshold without mutating timestamps:
    //    `effective_availability` must degrade to `Stale`, `effective_activity` to `Unknown`, and
    //    `effective_configured_selection` to `unavailable()`.
    let recovered_success_ms = recovered
        .last_success_at_ms
        .expect("recovered success timestamp");
    let stale_read = read_workspace_native_sample_readonly(
        &harness.state_dir,
        &identity,
        recovered_success_ms + 10_001,
    )
    .expect("readonly read")
    .expect("sample exists");
    assert_eq!(
        stale_read.evaluated.effective_availability,
        WorkspaceAvailabilityState::Stale
    );
    assert_eq!(
        stale_read.evaluated.freshness,
        WorkspaceFreshnessState::Stale
    );
    assert_eq!(
        stale_read.evaluated.effective_activity,
        WorkspaceActivityState::Unknown
    );
    assert_eq!(
        stale_read.evaluated.effective_configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );
    assert_eq!(
        stale_read.sample.last_success_at_ms,
        Some(recovered_success_ms)
    );

    // 4. Retire the parent endpoint so the verified connection disconnects -> `Disconnected` + `Unknown` + `None` model.
    harness.retire_parent_endpoint();
    let disconnected =
        wait_for_sample(&harness.state_dir, &identity, Duration::from_secs(2), |s| {
            s.availability == WorkspaceAvailabilityState::Disconnected
        })
        .await;
    assert_eq!(disconnected.activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        disconnected.configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );
}

#[tokio::test]
async fn quarantine_pid_reuse_epoch_restart_and_drop_invalidate_old_producer() {
    let harness = ObservationTestHarness::new().await;
    let fast_policy = WorkspaceSamplingPolicy {
        interval_ms: 100,
        metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
        stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
        snapshot_resets_sample_age: false,
    };

    // --- Part A: Shared-connection quarantine invalidates and stops the active producer ---
    let server_q = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_sampling_policy(fast_policy.clone()),
    );
    harness.register_thread(
        THREAD_A,
        MockThreadState {
            reported_id: THREAD_A.to_string(),
            project_id: Some("proj_lifecycle".to_string()),
            title: "Quarantine Chat".to_string(),
            cwd: harness.project_root.to_string_lossy().into_owned(),
            status_json: json!({ "type": "active", "activeFlags": [] }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server_q,
    );

    let binding_a = server_q
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_A })),
        )
        .await
        .expect("connect server_q")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(server_q.has_active_native_chat_sampler());

    let source_id = compute_observation_source_id(
        &harness.plans_dir,
        &harness.authority_dir,
        &harness.sessions_dir,
    )
    .unwrap();
    let cwd_str = harness.project_root.to_str().unwrap();
    let id_a = WorkspaceChatIdentity::new(&source_id, &binding_a, THREAD_A, cwd_str).unwrap();

    let _ = wait_for_sample(&harness.state_dir, &id_a, Duration::from_secs(2), |s| {
        s.availability == WorkspaceAvailabilityState::Available
    })
    .await;

    // Trigger `QuarantinedSharedConnection` on `server_q`'s epoch by attempting cross-thread enrollment.
    let op_token = harness.pairing_store.operator_token().unwrap();
    let quarantine_err = harness
        .pairing_store
        .enroll(
            &op_token,
            OperatorEnrollRequest {
                connection_nonce: server_q.connection_nonce().to_string(),
                epoch_id: server_q.epoch_id().to_string(),
                binding_id: "bnd_other".to_string(),
                thread_id: THREAD_C.to_string(),
                project_id: Some("proj_lifecycle".to_string()),
            },
        )
        .expect_err("shared connection must quarantine");
    assert_eq!(quarantine_err.code(), "shared_connection_rejected");

    let quarantined_sample =
        wait_for_sample(&harness.state_dir, &id_a, Duration::from_secs(2), |s| {
            s.availability == WorkspaceAvailabilityState::Unavailable
        })
        .await;
    assert_eq!(quarantined_sample.activity, WorkspaceActivityState::Unknown);
    assert_eq!(
        quarantined_sample.configured_selection,
        WorkspaceConfiguredSelection::unavailable()
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while server_q.has_active_native_chat_sampler() {
        assert!(
            Instant::now() < deadline,
            "quarantined sampler must terminate"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // --- Part B: Backend PID reuse invalidates old producer and rejects epoch-process mismatch ---
    let real_instance =
        read_backend_process_instance(harness.backend_pid).expect("real backend process instance");
    let simulated_ticks = Arc::new(AtomicU64::new(real_instance.start_ticks));
    let ticks_for_reader = Arc::clone(&simulated_ticks);
    let boot_id_for_reader = real_instance.boot_id.clone();

    let server_pid_reuse = harness.build_full_mcp_server(
        ManagedParentConnectionConfig::new(
            harness.reconnect_config(),
            harness.backend_pid,
            &harness.project_root,
        )
        .with_process_instance_reader(move |pid| {
            Some(ObservationProcessInstance {
                pid,
                boot_id: boot_id_for_reader.clone(),
                start_ticks: ticks_for_reader.load(Ordering::Acquire),
            })
        })
        .with_sampling_policy(fast_policy.clone()),
    );

    harness.register_thread(
        THREAD_B,
        MockThreadState {
            reported_id: THREAD_B.to_string(),
            project_id: Some("proj_lifecycle".to_string()),
            title: "PID Reuse Chat".to_string(),
            cwd: cwd_str.to_string(),
            status_json: json!({ "type": "active", "activeFlags": [] }),
            model: Some("gpt-5.4".to_string()),
            effort: Some("high".to_string()),
            artificial_delay: Duration::ZERO,
            disconnect_immediately: false,
        },
        &server_pid_reuse,
    );

    let binding_b = server_pid_reuse
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_B })),
        )
        .await
        .expect("connect server_pid_reuse")
        .structured_content
        .unwrap()["data"]["binding_id"]
        .as_str()
        .unwrap()
        .to_string();
    let id_b = WorkspaceChatIdentity::new(&source_id, &binding_b, THREAD_B, cwd_str).unwrap();

    let live_b = wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
        s.availability == WorkspaceAvailabilityState::Available
    })
    .await;
    let epoch_before_pid_reuse = live_b.producer_epoch.clone();

    // Simulate OS PID reuse: same `backend_pid`, different `start_ticks`!
    let reused_ticks = real_instance.start_ticks.saturating_add(77_777);
    simulated_ticks.store(reused_ticks, Ordering::Release);

    let pid_reused_sample =
        wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
            s.availability == WorkspaceAvailabilityState::Unavailable
        })
        .await;
    assert_eq!(pid_reused_sample.activity, WorkspaceActivityState::Unknown);
    assert_eq!(pid_reused_sample.configured_selection.selected_model, None);

    let deadline = Instant::now() + Duration::from_secs(2);
    while server_pid_reuse.has_active_native_chat_sampler() {
        assert!(
            Instant::now() < deadline,
            "PID-reused sampler must terminate"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Attempting to persist a sample with the new `ObservationProcessInstance` under the old
    // `producer_epoch` is rejected with `RejectedEpochProcessMismatch`.
    let forged_same_epoch = WorkspaceNativeSample {
        chat_identity: id_b.clone(),
        producer_epoch: epoch_before_pid_reuse.clone(),
        backend_process_instance: Some(ObservationProcessInstance {
            pid: harness.backend_pid,
            boot_id: real_instance.boot_id.clone(),
            start_ticks: reused_ticks,
        }),
        sequence: pid_reused_sample.sequence + 10,
        observed_at_ms: pid_reused_sample.observed_at_ms.map(|t| t + 50),
        last_success_at_ms: pid_reused_sample.observed_at_ms.map(|t| t + 50),
        availability: WorkspaceAvailabilityState::Available,
        activity: WorkspaceActivityState::Working,
        connection: WorkspaceSampleConnection {
            state: WorkspaceConnectionState::Connected,
            freshness: WorkspaceFreshnessState::Fresh,
            route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
            diagnostic: None,
        },
        capabilities: WorkspaceNativeCapabilities::default(),
        configured_selection: WorkspaceConfiguredSelection::verified(Some("gpt-5.4"), Some("high")),
    };
    let mismatch_write =
        write_workspace_native_sample_sidecar(&harness.state_dir, &forged_same_epoch)
            .expect("reconcile outcome");
    assert_eq!(
        mismatch_write.decision,
        WorkspaceSampleReconcileDecision::RejectedEpochProcessMismatch
    );

    // --- Part C: `restart_epoch` stops old producer, rotates `producer_epoch`, and `Drop` stops polling ---
    let mut server_restart = server_pid_reuse;
    server_restart
        .restart_epoch()
        .expect("restart paired MCP epoch");
    assert!(!server_restart.has_active_native_chat_sampler());
    harness.update_mcp_identity(THREAD_B, &server_restart);

    let _ = server_restart
        .invoke_tool_json(
            "connect_current_chat",
            json!({}),
            Some(json!({ "threadId": THREAD_B })),
        )
        .await
        .expect("reconnect after restart_epoch");
    assert!(server_restart.has_active_native_chat_sampler());
    let restarted_epoch = server_restart
        .active_native_chat_producer_epoch()
        .expect("new producer epoch");
    assert_ne!(restarted_epoch, epoch_before_pid_reuse);

    let restarted_sample =
        wait_for_sample(&harness.state_dir, &id_b, Duration::from_secs(2), |s| {
            s.producer_epoch == restarted_epoch
                && s.availability == WorkspaceAvailabilityState::Available
        })
        .await;
    assert!(restarted_sample.sequence >= 1);

    drop(server_restart);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let count_after_drop = harness.thread_read_count(THREAD_B);
    tokio::time::sleep(Duration::from_millis(260)).await;
    assert_eq!(
        harness.thread_read_count(THREAD_B),
        count_after_drop,
        "dropping PairedMcpServer must immediately stop background sampling"
    );
}

#[test]
fn sample_sidecar_enforces_reconciliation_repo_disjointness_64kib_and_128_per_source_bounds() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = fs::canonicalize(temp.path()).expect("canonical root");
    let state_dir = root.join("private-state");
    DirBuilder::new()
        .mode(0o700)
        .create(&state_dir)
        .expect("create state_dir");
    let repo_dir = root.join("project-repo");
    fs::create_dir(&repo_dir).expect("create repo_dir");
    let repo_str = repo_dir.to_str().unwrap();

    // 1. Direct activity proof helper rejects missing/invalid/unknown flags and accepts proven ones.
    assert_eq!(
        prove_native_thread_activity(&NativeThreadStatusDetail {
            status: "active".to_string(),
            tagged: false,
            active_flags_present: false,
            active_flags: Vec::new(),
            has_invalid_active_flags: false,
        }),
        (
            WorkspaceActivityState::Unknown,
            WorkspaceCapabilityState::Unknown
        )
    );
    assert_eq!(
        prove_native_thread_activity(&NativeThreadStatusDetail {
            status: "active".to_string(),
            tagged: true,
            active_flags_present: false,
            active_flags: Vec::new(),
            has_invalid_active_flags: false,
        }),
        (
            WorkspaceActivityState::Unknown,
            WorkspaceCapabilityState::Unknown
        )
    );
    assert_eq!(
        prove_native_thread_activity(&NativeThreadStatusDetail {
            status: "active".to_string(),
            tagged: true,
            active_flags_present: true,
            active_flags: Vec::new(),
            has_invalid_active_flags: false,
        }),
        (
            WorkspaceActivityState::Working,
            WorkspaceCapabilityState::Supported
        )
    );

    // 2. Repo-overlapping `state_dir` is rejected with `SecurityViolation`.
    let inside_repo_state = repo_dir.join("illegal-state");
    DirBuilder::new()
        .mode(0o700)
        .create(&inside_repo_state)
        .expect("create overlapping dir");
    let overlap_identity =
        WorkspaceChatIdentity::new("default", "bnd_overlap", THREAD_A, repo_str).unwrap();
    let base_sample = |identity: WorkspaceChatIdentity,
                       epoch: &str,
                       seq: u64,
                       at_ms: i64|
     -> WorkspaceNativeSample {
        WorkspaceNativeSample {
            chat_identity: identity,
            producer_epoch: epoch.to_string(),
            backend_process_instance: Some(ObservationProcessInstance {
                pid: 4242,
                boot_id: "boot-test-1".to_string(),
                start_ticks: 9001,
            }),
            sequence: seq,
            observed_at_ms: Some(at_ms),
            last_success_at_ms: Some(at_ms),
            availability: WorkspaceAvailabilityState::Available,
            activity: WorkspaceActivityState::Working,
            connection: WorkspaceSampleConnection {
                state: WorkspaceConnectionState::Connected,
                freshness: WorkspaceFreshnessState::Fresh,
                route: Some(NATIVE_CHAT_SAMPLE_ROUTE.to_string()),
                diagnostic: None,
            },
            capabilities: WorkspaceNativeCapabilities::default(),
            configured_selection: WorkspaceConfiguredSelection::verified(
                Some("gpt-5.4"),
                Some("high"),
            ),
        }
    };

    let overlap_err = write_workspace_native_sample_sidecar(
        &inside_repo_state,
        &base_sample(
            overlap_identity.clone(),
            &generate_sample_producer_epoch(),
            1,
            1_000,
        ),
    )
    .expect_err("repo overlap must fail closed");
    assert_eq!(overlap_err.error_code(), NativeErrorCode::SecurityViolation);

    // 2b. Read-only inspection on an uninitialized `state_dir` never creates directories or locks.
    assert_eq!(
        read_workspace_native_sample_readonly(&state_dir, &overlap_identity, 1_500)
            .expect("readonly empty lookup"),
        None
    );
    let empty_report = read_workspace_native_samples_readonly(&state_dir, None, 1_500)
        .expect("readonly empty scan");
    assert!(empty_report.entries.is_empty());
    assert!(!native_chat_samples_dir(&state_dir).exists());

    // 3. Stale and duplicate sequences are rejected without overwriting newer sample on disk.
    let epoch = generate_sample_producer_epoch();
    let s2 = base_sample(overlap_identity.clone(), &epoch, 2, 2_000);
    let w_initial = write_workspace_native_sample_sidecar(&state_dir, &s2).expect("write seq 2");
    assert_eq!(
        w_initial.decision,
        WorkspaceSampleReconcileDecision::AcceptedInitial
    );

    let s1_stale = base_sample(overlap_identity.clone(), &epoch, 1, 2_500);
    let w_stale =
        write_workspace_native_sample_sidecar(&state_dir, &s1_stale).expect("reconcile seq 1");
    assert_eq!(
        w_stale.decision,
        WorkspaceSampleReconcileDecision::RejectedStaleSequence
    );

    let s2_dup = base_sample(overlap_identity.clone(), &epoch, 2, 2_500);
    let w_dup = write_workspace_native_sample_sidecar(&state_dir, &s2_dup).expect("reconcile dup");
    assert_eq!(
        w_dup.decision,
        WorkspaceSampleReconcileDecision::RejectedDuplicateSequence
    );

    // Read-only paths must remain lock-free and mutation-free even when evaluating stale samples.
    let lock_path = native_chat_samples_dir(&state_dir).join(NATIVE_CHAT_SAMPLES_LOCK_FILENAME);
    let _ = fs::remove_file(&lock_path);
    let raw_before_readonly = fs::read(&w_initial.path).expect("read persisted sidecar bytes");

    let persisted = read_workspace_native_sample_readonly(&state_dir, &overlap_identity, 15_000)
        .expect("read")
        .expect("present");
    assert_eq!(persisted.sample.sequence, 2);
    assert_eq!(persisted.sample.observed_at_ms, Some(2_000));
    assert_eq!(
        persisted.evaluated.effective_availability,
        WorkspaceAvailabilityState::Stale
    );
    let _ = read_workspace_native_samples_readonly(&state_dir, None, 15_000)
        .expect("readonly scan without lock recreation");
    assert!(!lock_path.exists());
    assert_eq!(
        fs::read(&w_initial.path).expect("re-read persisted sidecar bytes"),
        raw_before_readonly
    );

    // 4. Oversized (> 64 KiB) sidecar on disk is skipped/rejected safely by readonly inspection.
    let fake_id = WorkspaceChatIdentity::new("default", "bnd_huge", THREAD_B, repo_str).unwrap();
    let huge_path = native_chat_sample_path(&state_dir, &fake_id).unwrap();
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&huge_path)
            .expect("open huge sidecar");
        let big = vec![b' '; MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES + 128];
        f.write_all(&big).expect("write > 64 KiB");
    }
    let single_huge_err = read_workspace_native_sample_readonly(&state_dir, &fake_id, 3_000)
        .expect_err("oversized sidecar must error on direct read");
    assert_eq!(
        single_huge_err.error_code(),
        NativeErrorCode::OutputOverflow
    );
    let _ = fs::remove_file(&huge_path);

    // 5. Per-source 128 sample cap (`MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE`) prunes older chats on disk
    //    without discarding unrelated files/transcripts or samples belonging to other sources.
    let samples_dir = native_chat_samples_dir(&state_dir);
    let unrelated_transcript = samples_dir.join("raw-transcript.jsonl");
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&unrelated_transcript)
            .expect("create unrelated transcript file");
        f.write_all(b"{\"turn\":\"never-discard\"}\n")
            .expect("write unrelated transcript bytes");
    }
    let other_source = "src_2222222222222222222222222222222222222222222222222222222222222222";
    let other_source_id =
        WorkspaceChatIdentity::new(other_source, "bnd_other_src", THREAD_C, repo_str)
            .expect("other source identity");
    write_workspace_native_sample_sidecar(
        &state_dir,
        &base_sample(
            other_source_id.clone(),
            &generate_sample_producer_epoch(),
            1,
            500,
        ),
    )
    .expect("write other source sample");

    let bounded_source = "src_1111111111111111111111111111111111111111111111111111111111111111";
    let mut first_chat_epoch = String::new();
    let mut first_chat_id = None;
    for idx in 0..132u32 {
        let thread_uuid = format!("0196ed00-0000-7000-8000-{:012x}", idx + 1);
        let binding_id = format!("bnd_cap_{idx}");
        let chat_id =
            WorkspaceChatIdentity::new(bounded_source, &binding_id, &thread_uuid, repo_str)
                .expect("valid cap identity");
        let epoch_i = generate_sample_producer_epoch();
        if idx == 0 {
            first_chat_epoch = epoch_i.clone();
            first_chat_id = Some(chat_id.clone());
        }
        let sample = base_sample(chat_id, &epoch_i, 1, 10_000 + i64::from(idx) * 10);
        let outcome =
            write_workspace_native_sample_sidecar(&state_dir, &sample).expect("write cap sample");
        if idx >= MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE as u32 {
            assert_eq!(outcome.pruned_for_source, 1);
        } else {
            assert_eq!(outcome.pruned_for_source, 0);
        }
    }

    let report = read_workspace_native_samples_readonly(&state_dir, Some(bounded_source), 12_000)
        .expect("read bounded source");
    assert_eq!(
        report.entries.len(),
        MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE
    );
    assert_eq!(report.omitted_count, 0);
    assert_eq!(report.warnings.len(), 1);
    // Earliest 4 chats (idx 0..4, observed_at_ms 10_000..=10_030) must have been pruned on disk!
    for pruned_idx in 0..4u32 {
        let pruned_uuid = format!("0196ed00-0000-7000-8000-{:012x}", pruned_idx + 1);
        let pruned_id = WorkspaceChatIdentity::new(
            bounded_source,
            &format!("bnd_cap_{pruned_idx}"),
            &pruned_uuid,
            repo_str,
        )
        .expect("pruned chat identity");
        let pruned_path = native_chat_sample_path(&state_dir, &pruned_id).expect("pruned path");
        assert!(!pruned_path.exists());
    }
    let oldest_retained_obs = report
        .entries
        .iter()
        .filter_map(|e| e.sample.observed_at_ms)
        .min()
        .expect("min observed_at_ms");
    assert_eq!(oldest_retained_obs, 10_040);

    // Unrelated transcript file and other-source sample remain untouched on disk.
    assert_eq!(
        fs::read(&unrelated_transcript).expect("read preserved unrelated transcript"),
        b"{\"turn\":\"never-discard\"}\n"
    );
    let other_report =
        read_workspace_native_samples_readonly(&state_dir, Some(other_source), 12_000)
            .expect("read other source");
    assert_eq!(other_report.entries.len(), 1);
    assert_eq!(
        other_report.entries[0].sample.chat_identity,
        other_source_id
    );

    // 6. If a leftover 129th sidecar for `idx == 0` exists on disk and receives an accepted newer
    //    sequence that still ranks older than the retained 128 chats, its stale sidecar is removed.
    let first_id = first_chat_id.expect("first chat id");
    let first_path = native_chat_sample_path(&state_dir, &first_id).expect("first path");
    let leftover_seed = base_sample(first_id.clone(), &first_chat_epoch, 1, 10_000);
    let leftover_bytes =
        serialize_workspace_native_sample_sidecar(&leftover_seed).expect("serialize leftover");
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&first_path)
            .expect("seed leftover sidecar");
        f.write_all(&leftover_bytes)
            .expect("write leftover sidecar");
    }
    assert!(first_path.exists());

    let outranked_update = base_sample(first_id, &first_chat_epoch, 2, 10_005);
    let outranked_outcome = write_workspace_native_sample_sidecar(&state_dir, &outranked_update)
        .expect("reconcile and prune outranked leftover");
    assert_eq!(
        outranked_outcome.decision,
        WorkspaceSampleReconcileDecision::AcceptedNextSequence
    );
    assert_eq!(outranked_outcome.pruned_for_source, 1);
    assert!(!first_path.exists());
}
