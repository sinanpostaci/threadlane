#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{symlink, DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use threadlane::native_tool_output::NativeNotificationDelivery;
use threadlane::{
    invoke_codex_wakeup, ApprovedSessionWakeupContext, CodexWakeupConfig, NativeReconnectConfig,
    NativeToolOutputConfig,
};
use threadlane_core::{WakeupClaim, WakeupOutcome};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::protocol::Message;

const TEST_THREAD_ID: &str = "01968f52-8871-7d51-9914-79d2bc48eb5a";
const TEST_TURN_ID: &str = "01968f52-9999-7d51-9914-79d2bc48eb5b";
const TEST_APPROVAL_ID: &str = "apr_0123456789abcdef0123456789abcdef";
const TEST_BINDING_ID: &str = "bnd_test_native_reconnect";

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

struct ReconnectTestEnv {
    _temp: tempfile::TempDir,
    root: PathBuf,
    project_root: PathBuf,
    sock_dir: PathBuf,
    initial_socket_path: PathBuf,
    cfg_dir: PathBuf,
    descriptor_root: PathBuf,
    backend_pid: u32,
    wrapper_pid: u32,
    backend_executable: PathBuf,
    application_executable: PathBuf,
    queue_executable: PathBuf,
    queue_marker: PathBuf,
    staging_counter: AtomicUsize,
}

impl ReconnectTestEnv {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = fs::canonicalize(temp.path()).expect("canonical root");
        let project_root = root.join("repo");
        fs::create_dir(&project_root).expect("create repo");

        let sock_dir = root.join("sock");
        DirBuilder::new()
            .mode(0o700)
            .create(&sock_dir)
            .expect("create sock_dir");
        let initial_socket_path = sock_dir.join("initial-codex.sock");

        let cfg_dir = root.join("cfg");
        DirBuilder::new()
            .mode(0o700)
            .create(&cfg_dir)
            .expect("create cfg_dir");

        let descriptor_root = root.join("descriptors");
        DirBuilder::new()
            .mode(0o700)
            .create(&descriptor_root)
            .expect("create descriptor_root");

        let backend_pid = std::process::id();
        let wrapper_pid = read_ppid(backend_pid);
        let app_pid = read_ppid(wrapper_pid);
        let backend_executable =
            fs::canonicalize("/proc/self/exe").expect("canonical backend executable");
        let application_executable = fs::canonicalize(format!("/proc/{app_pid}/exe"))
            .expect("canonical application executable");

        let queue_marker = root.join("queue-ran.marker");
        let queue_executable = root.join("fake-codex-queue.sh");
        let script = format!(
            "#!/bin/sh\necho ran > \"{}\"\nexit 0\n",
            queue_marker.display()
        );
        fs::write(&queue_executable, script).expect("write fake queue executable");
        fs::set_permissions(&queue_executable, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 queue executable");

        Self {
            _temp: temp,
            root,
            project_root,
            sock_dir,
            initial_socket_path,
            cfg_dir,
            descriptor_root,
            backend_pid,
            wrapper_pid,
            backend_executable,
            application_executable,
            queue_executable,
            queue_marker,
            staging_counter: AtomicUsize::new(0),
        }
    }

    fn reconnect_config(&self) -> NativeReconnectConfig {
        NativeReconnectConfig {
            descriptor_root: self.descriptor_root.clone(),
            backend_executable: self.backend_executable.clone(),
            application_executable: self.application_executable.clone(),
        }
    }

    fn native_config_with_reconnect(&self) -> NativeToolOutputConfig {
        NativeToolOutputConfig {
            schema_version: 1,
            socket_path: self.initial_socket_path.clone(),
            thread_id: TEST_THREAD_ID.to_string(),
            project_root: self.project_root.clone(),
            model: "gpt-5-codex".to_string(),
            effort: "high".to_string(),
            delivery: NativeNotificationDelivery::ToolOutput,
            reconnect: Some(self.reconnect_config()),
        }
    }

    fn sample_claim(&self) -> WakeupClaim {
        WakeupClaim {
            binding_id: TEST_BINDING_ID.to_string(),
            thread_uuid: TEST_THREAD_ID.to_string(),
            claim_id: "wcl_0123456789abcdef0123456789abcdef".to_string(),
            pending_since_seq: 3,
            covered_up_to_seq: 9,
            coalesced_count: 4,
            message: "UNTRUSTED_CLAIM_MESSAGE_NOT_FOR_TRANSPORT".to_string(),
            claimed_at_ms: 1_000,
        }
    }

    fn wakeup_config(
        &self,
        ctx: ApprovedSessionWakeupContext,
        timeout_ms: u64,
    ) -> CodexWakeupConfig {
        let mut notify_bindings = BTreeSet::new();
        notify_bindings.insert(TEST_BINDING_ID.to_string());
        CodexWakeupConfig {
            executable: self.queue_executable.clone(),
            notify_bindings,
            timeout: Duration::from_millis(timeout_ms),
            approved_session: Some(ctx),
        }
    }

    /// Binds a physical Unix socket inside `self.sock_dir` (`0700`) and atomically publishes
    /// a `tl-codex-<suffix>` directory in `self.descriptor_root` whose `codex.sock` is a symlink
    /// to that physical socket, matching the launcher's session layout.
    fn publish_symlinked_session(&self, suffix: &str) -> (UnixListener, PathBuf) {
        let seq = self.staging_counter.fetch_add(1, Ordering::SeqCst);
        let physical_sock = self.sock_dir.join(format!("phys-{suffix}-{seq}.sock"));
        let listener = UnixListener::bind(&physical_sock).expect("bind physical unix socket");
        fs::set_permissions(&physical_sock, fs::Permissions::from_mode(0o600))
            .expect("secure physical socket");

        let staging_dir = self.root.join(format!("staging-{suffix}-{seq}"));
        DirBuilder::new()
            .mode(0o700)
            .create(&staging_dir)
            .expect("create staging dir");

        let final_session_dir = self.descriptor_root.join(format!("tl-codex-{suffix}"));
        let advertised_sock = final_session_dir.join("codex.sock");
        symlink(&physical_sock, staging_dir.join("codex.sock")).expect("symlink session socket");

        let descriptor_json = serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "wrapper_pid": self.wrapper_pid,
            "backend_pid": self.backend_pid,
            "socket_path": advertised_sock,
            "real_codex": self.backend_executable,
            "transport": "unix_websocket",
        }))
        .expect("serialize descriptor json");

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
        drop(desc_file);

        fs::rename(&staging_dir, &final_session_dir).expect("atomically publish session dir");
        (listener, final_session_dir)
    }

    /// Creates a `tl-codex-<suffix>` directory with a physical Unix socket bound directly inside it.
    fn publish_direct_session(&self, suffix: &str) -> (UnixListener, PathBuf) {
        let session_dir = self.descriptor_root.join(format!("tl-codex-{suffix}"));
        DirBuilder::new()
            .mode(0o700)
            .create(&session_dir)
            .expect("create session dir");
        let sock_path = session_dir.join("codex.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind session socket");
        fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600))
            .expect("secure session socket");

        let descriptor_json = serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "wrapper_pid": self.wrapper_pid,
            "backend_pid": self.backend_pid,
            "socket_path": sock_path,
            "real_codex": self.backend_executable,
            "transport": "unix_websocket",
        }))
        .expect("serialize descriptor json");

        let mut desc_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(session_dir.join("descriptor.json"))
            .expect("create descriptor.json");
        desc_file
            .write_all(&descriptor_json)
            .expect("write descriptor.json");
        desc_file.sync_all().expect("sync descriptor.json");

        (listener, session_dir)
    }

    /// Atomically retires a published session directory out of `descriptor_root`.
    fn retire_session(&self, session_dir: &Path) {
        let seq = self.staging_counter.fetch_add(1, Ordering::SeqCst);
        let retired_path = self.root.join(format!("retired-session-{seq}"));
        fs::rename(session_dir, retired_path).expect("atomically retire session dir");
    }
}

#[tokio::test]
async fn reconnect_config_serialization_digest_and_missing_initial_socket_validation() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let env = ReconnectTestEnv::new();

        // 1. When reconnect is None, initial socket must exist.
        let mut legacy_cfg = env.native_config_with_reconnect();
        legacy_cfg.reconnect = None;
        assert!(
            legacy_cfg.validate().is_err(),
            "legacy mode must require initial socket to exist"
        );

        let listener = UnixListener::bind(&env.initial_socket_path).expect("bind initial socket");
        legacy_cfg
            .validate()
            .expect("legacy config valid when socket exists");
        let legacy_json = serde_json::to_string(&legacy_cfg).expect("serialize legacy");
        assert!(!legacy_json.contains("reconnect"));
        let legacy_digest = legacy_cfg.digest();

        // Remove the initial socket to simulate post-restart state.
        drop(listener);
        fs::remove_file(&env.initial_socket_path).expect("remove initial socket");
        assert!(legacy_cfg.validate().is_err());

        // 2. With reconnect Some, validation and loading succeed even when initial socket_path is missing.
        let reconnect_cfg = env.native_config_with_reconnect();
        assert!(!env.initial_socket_path.exists());
        reconnect_cfg
            .validate()
            .expect("reconnect mode must not require initial socket to still exist");

        let cfg_path = env.cfg_dir.join("native-reconnect.json");
        fs::write(
            &cfg_path,
            serde_json::to_vec_pretty(&reconnect_cfg).expect("json"),
        )
        .expect("write reconnect config");
        fs::set_permissions(&cfg_path, fs::Permissions::from_mode(0o600)).expect("chmod 0600");
        let loaded = NativeToolOutputConfig::load(&cfg_path).expect("load reconnect config");
        assert_eq!(loaded, reconnect_cfg);

        // 3. Digests bind the reconnect selector and differ from legacy None.
        let reconnect_digest = reconnect_cfg.digest();
        assert_ne!(reconnect_digest, legacy_digest);

        let mut modified_reconnect = reconnect_cfg.clone();
        modified_reconnect
            .reconnect
            .as_mut()
            .expect("reconnect some")
            .descriptor_root = env.project_root.clone();
        assert_ne!(modified_reconnect.digest(), reconnect_digest);

        // 4. Relative or traversal initial socket_path is still rejected in reconnect mode.
        let mut bad_sock = reconnect_cfg.clone();
        bad_sock.socket_path = PathBuf::from("relative/codex.sock");
        assert!(bad_sock.validate().is_err());

        let mut traversal_sock = reconnect_cfg;
        traversal_sock.socket_path = env.sock_dir.join("../sock/codex.sock");
        assert!(traversal_sock.validate().is_err());
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn reconnect_recovers_across_missing_endpoint_handshake_drop_and_not_loaded_status() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let env = ReconnectTestEnv::new();
        assert!(!env.initial_socket_path.exists());
        let cfg = env.native_config_with_reconnect();
        let expected_cwd = env.project_root.to_string_lossy().to_string();

        let (ready_tx, mut ready_rx) = mpsc::channel::<UnixListener>(1);
        let env_arc = Arc::new(env);
        let env_publisher = Arc::clone(&env_arc);

        // Exceed the former ~3s premature-attempt exhaustion while staying inside the 5s budget.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(3_500)).await;
            let (listener, _session_dir) = env_publisher.publish_symlinked_session("restarted");
            let _ = ready_tx.send(listener).await;
        });

        let connection_count = Arc::new(AtomicUsize::new(0));
        let connection_count_clone = Arc::clone(&connection_count);
        let turn_start_count = Arc::new(AtomicUsize::new(0));
        let turn_start_count_clone = Arc::clone(&turn_start_count);

        let server = tokio::spawn(async move {
            let listener = ready_rx.recv().await.expect("receive published listener");

            // Attempt 1 on socket: drop stream immediately before WebSocket handshake completes.
            let (stream1, _) = listener.accept().await.expect("accept attempt 1");
            connection_count_clone.fetch_add(1, Ordering::SeqCst);
            drop(stream1);

            // Attempt 2 on socket: return status = notLoaded in thread/read.
            let (stream2, _) = listener.accept().await.expect("accept attempt 2");
            connection_count_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws2 = tokio_tungstenite::accept_async(stream2)
                .await
                .expect("ws2 handshake");
            let _ = read_json(&mut ws2).await;
            ws2.send(Message::Text(
                json!({ "id": 1, "result": {} }).to_string().into(),
            ))
            .await
            .expect("ws2 init resp");
            let _ = read_json(&mut ws2).await;
            let _ = read_json(&mut ws2).await;
            ws2.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "notLoaded" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ws2 read notLoaded resp");
            drop(ws2);

            // Attempt 3 on socket: thread/read succeeds (idle), then connection drops before thread/resume reply.
            let (stream3, _) = listener.accept().await.expect("accept attempt 3");
            connection_count_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws3 = tokio_tungstenite::accept_async(stream3)
                .await
                .expect("ws3 handshake");
            let _ = read_json(&mut ws3).await;
            ws3.send(Message::Text(
                json!({ "id": 1, "result": {} }).to_string().into(),
            ))
            .await
            .expect("ws3 init resp");
            let _ = read_json(&mut ws3).await;
            let _ = read_json(&mut ws3).await;
            ws3.send(Message::Text(
                json!({
                    "id": 2,
                    "result": {
                        "thread": {
                            "id": TEST_THREAD_ID,
                            "cwd": expected_cwd,
                            "status": { "type": "idle" }
                        }
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("ws3 read idle resp");
            let _ = read_json(&mut ws3).await;
            drop(ws3);

            // Attempt 4 on socket: full valid exchange -> Submitted.
            let (stream4, _) = listener.accept().await.expect("accept attempt 4");
            connection_count_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws4 = tokio_tungstenite::accept_async(stream4)
                .await
                .expect("ws4 handshake");
            complete_valid_exchange(&mut ws4, &expected_cwd, &turn_start_count_clone).await;
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("attach reconnect transport");
        let wakeup_cfg = env_arc.wakeup_config(ctx, 5_000);
        let claim = env_arc.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        server.await.expect("server task");
        assert_eq!(connection_count.load(Ordering::SeqCst), 4);
        assert_eq!(turn_start_count.load(Ordering::SeqCst), 1);
        assert!(!env_arc.queue_marker.exists());
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn reconnect_discovers_replaced_session_directory_after_pre_effect_disconnect() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let env = Arc::new(ReconnectTestEnv::new());
        let (listener1, session_dir1) = env.publish_direct_session("first");
        let cfg = env.native_config_with_reconnect();
        let expected_cwd = env.project_root.to_string_lossy().to_string();

        let turn_start_count = Arc::new(AtomicUsize::new(0));
        let turn_start_count_clone = Arc::clone(&turn_start_count);
        let env_server = Arc::clone(&env);

        let server = tokio::spawn(async move {
            // Accept on first session, read initialize, then replace session 1 with session 2 and drop.
            let (stream1, _) = listener1.accept().await.expect("accept session 1");
            let mut ws1 = tokio_tungstenite::accept_async(stream1)
                .await
                .expect("ws1 handshake");
            let init_msg = read_json(&mut ws1).await;
            assert_eq!(init_msg["method"], "initialize");

            env_server.retire_session(&session_dir1);
            let (listener2, _session_dir2) = env_server.publish_symlinked_session("second");
            drop(ws1);
            drop(listener1);

            let (stream2, _) = listener2.accept().await.expect("accept session 2");
            let mut ws2 = tokio_tungstenite::accept_async(stream2)
                .await
                .expect("ws2 handshake");
            complete_valid_exchange(&mut ws2, &expected_cwd, &turn_start_count_clone).await;
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(cfg)
            .expect("attach reconnect transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let claim = env.sample_claim();
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &claim, &mut rx).await;
        assert_eq!(outcome, WakeupOutcome::Submitted);
        server.await.expect("server task");
        assert_eq!(turn_start_count.load(Ordering::SeqCst), 1);
        assert!(!env.queue_marker.exists());
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn terminal_refused_metadata_and_protocol_errors_do_not_retry() {
    tokio::time::timeout(Duration::from_secs(15), async {
        // 1. Multiple live matching candidates -> Refused -> immediate Failed without connecting.
        {
            let env = ReconnectTestEnv::new();
            let (_l1, _s1) = env.publish_symlinked_session("dup-a");
            let (_l2, _s2) = env.publish_symlinked_session("dup-b");
            let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
                .expect("ctx")
                .with_tool_output_transport(env.native_config_with_reconnect())
                .expect("transport");
            let wakeup_cfg = env.wakeup_config(ctx, 5_000);
            let (_tx, mut rx) = watch::channel(false);

            let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
            assert!(
                matches!(outcome, WakeupOutcome::Failed { .. }),
                "expected Failed when multiple live candidates exist, got {outcome:?}"
            );
        }

        // 2. Metadata mismatch (wrong model in thread/resume) -> terminal Failed on first attempt without retry.
        {
            let env = ReconnectTestEnv::new();
            let (listener, _session_dir) = env.publish_symlinked_session("bad-model");
            let expected_cwd = env.project_root.to_string_lossy().to_string();
            let connections = Arc::new(AtomicUsize::new(0));
            let connections_clone = Arc::clone(&connections);

            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                connections_clone.fetch_add(1, Ordering::SeqCst);
                let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({ "id": 1, "result": {} }).to_string().into(),
                    ))
                    .await;
                let _ = read_json(&mut ws).await;
                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": 2,
                            "result": {
                                "thread": {
                                    "id": TEST_THREAD_ID,
                                    "cwd": expected_cwd,
                                    "status": { "type": "idle" }
                                }
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": 3,
                            "result": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "model": "unexpected-model",
                                "reasoningEffort": "high"
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;

                // Watch briefly to ensure no second connection attempt occurs.
                if tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_ok()
                {
                    connections_clone.fetch_add(1, Ordering::SeqCst);
                }
            });

            let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
                .expect("ctx")
                .with_tool_output_transport(env.native_config_with_reconnect())
                .expect("transport");
            let wakeup_cfg = env.wakeup_config(ctx, 5_000);
            let (_tx, mut rx) = watch::channel(false);

            let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
            assert!(matches!(outcome, WakeupOutcome::Failed { .. }));
            server.await.expect("server");
            assert_eq!(
                connections.load(Ordering::SeqCst),
                1,
                "terminal model mismatch must not retry"
            );
        }

        // 3. Protocol violation (binary frame during initialize) -> terminal Failed on first attempt without retry.
        {
            let env = ReconnectTestEnv::new();
            let (listener, _session_dir) = env.publish_symlinked_session("bad-proto");
            let connections = Arc::new(AtomicUsize::new(0));
            let connections_clone = Arc::clone(&connections);

            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                connections_clone.fetch_add(1, Ordering::SeqCst);
                let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

                let _ = read_json(&mut ws).await;
                let _ = ws.send(Message::Binary(vec![0xde, 0xad].into())).await;

                if tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_ok()
                {
                    connections_clone.fetch_add(1, Ordering::SeqCst);
                }
            });

            let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
                .expect("ctx")
                .with_tool_output_transport(env.native_config_with_reconnect())
                .expect("transport");
            let wakeup_cfg = env.wakeup_config(ctx, 5_000);
            let (_tx, mut rx) = watch::channel(false);

            let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
            assert!(matches!(outcome, WakeupOutcome::Failed { .. }));
            server.await.expect("server");
            assert_eq!(
                connections.load(Ordering::SeqCst),
                1,
                "terminal protocol error must not retry"
            );
        }
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn shared_cumulative_frame_budget_spans_across_reconnect_attempts() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let env = ReconnectTestEnv::new();
        let (listener, _session_dir) = env.publish_symlinked_session("frame-budget");
        let connections = Arc::new(AtomicUsize::new(0));
        let connections_clone = Arc::clone(&connections);

        let server = tokio::spawn(async move {
            // Attempt 1: send 300 untrusted notifications, then disconnect before replying to initialize.
            let (stream1, _) = listener.accept().await.expect("accept 1");
            connections_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws1 = tokio_tungstenite::accept_async(stream1).await.expect("ws1");
            let _ = read_json(&mut ws1).await;
            for _ in 0..300 {
                ws1.send(Message::Text(
                    json!({ "method": "thread/updated", "params": {} })
                        .to_string()
                        .into(),
                ))
                .await
                .expect("send notif 1");
            }
            drop(ws1);

            // Attempt 2: send another 220 notifications (300 + 220 > 512 cumulative frame budget).
            let (stream2, _) = listener.accept().await.expect("accept 2");
            connections_clone.fetch_add(1, Ordering::SeqCst);
            let mut ws2 = tokio_tungstenite::accept_async(stream2).await.expect("ws2");
            let _ = read_json(&mut ws2).await;
            for _ in 0..220 {
                if ws2
                    .send(Message::Text(
                        json!({ "method": "thread/updated", "params": {} })
                            .to_string()
                            .into(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            if tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_ok()
            {
                connections_clone.fetch_add(1, Ordering::SeqCst);
            }
        });

        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(env.native_config_with_reconnect())
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 5_000);
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
        match outcome {
            WakeupOutcome::Failed { error } => {
                assert!(
                    error.contains("exceeded maximum frame count"),
                    "unexpected error: {error}"
                );
            }
            other => panic!("expected Failed due to cumulative frame count, got {other:?}"),
        }
        server.await.expect("server");
        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "cumulative frame budget exhaustion must terminate retries on second attempt"
        );
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn post_turn_start_failure_returns_unknown_and_never_retries_or_replays() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for disconnect_instead_of_invalid_json in [true, false] {
            let env = ReconnectTestEnv::new();
            let (listener, _session_dir) = env.publish_symlinked_session("post-effect");
            let expected_cwd = env.project_root.to_string_lossy().to_string();
            let connections = Arc::new(AtomicUsize::new(0));
            let connections_clone = Arc::clone(&connections);
            let turn_starts = Arc::new(AtomicUsize::new(0));
            let turn_starts_clone = Arc::clone(&turn_starts);

            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                connections_clone.fetch_add(1, Ordering::SeqCst);
                let mut ws = tokio_tungstenite::accept_async(stream).await.expect("ws");

                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({ "id": 1, "result": {} }).to_string().into(),
                    ))
                    .await;
                let _ = read_json(&mut ws).await;
                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": 2,
                            "result": {
                                "thread": {
                                    "id": TEST_THREAD_ID,
                                    "cwd": expected_cwd,
                                    "status": { "type": "idle" }
                                }
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;
                let _ = read_json(&mut ws).await;
                let _ = ws
                    .send(Message::Text(
                        json!({
                            "id": 3,
                            "result": {
                                "id": TEST_THREAD_ID,
                                "cwd": expected_cwd,
                                "model": "gpt-5-codex",
                                "reasoningEffort": "high"
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await;

                let turn_req = read_json(&mut ws).await;
                assert_eq!(turn_req["method"], "turn/start");
                turn_starts_clone.fetch_add(1, Ordering::SeqCst);

                if disconnect_instead_of_invalid_json {
                    drop(ws);
                } else {
                    let _ = ws.send(Message::Text("{malformed-json".into())).await;
                }

                if tokio::time::timeout(Duration::from_millis(60), listener.accept())
                    .await
                    .is_ok()
                {
                    connections_clone.fetch_add(1, Ordering::SeqCst);
                }
            });

            let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
                .expect("ctx")
                .with_tool_output_transport(env.native_config_with_reconnect())
                .expect("transport");
            let wakeup_cfg = env.wakeup_config(ctx, 5_000);
            let (_tx, mut rx) = watch::channel(false);

            let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
            assert!(
                matches!(outcome, WakeupOutcome::Unknown { .. }),
                "expected Unknown after turn/start write, got {outcome:?}"
            );
            server.await.expect("server");
            assert_eq!(turn_starts.load(Ordering::SeqCst), 1);
            assert_eq!(
                connections.load(Ordering::SeqCst),
                1,
                "must never retry or replay once turn/start may have been written"
            );
            assert!(!env.queue_marker.exists());
        }
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

#[tokio::test]
async fn timeout_before_effect_with_unavailable_endpoint_returns_failed() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let env = ReconnectTestEnv::new();
        let ctx = ApprovedSessionWakeupContext::new(TEST_APPROVAL_ID)
            .expect("ctx")
            .with_tool_output_transport(env.native_config_with_reconnect())
            .expect("transport");
        let wakeup_cfg = env.wakeup_config(ctx, 120);
        let (_tx, mut rx) = watch::channel(false);

        let outcome = invoke_codex_wakeup(&wakeup_cfg, &env.sample_claim(), &mut rx).await;
        assert!(
            matches!(outcome, WakeupOutcome::Failed { .. }),
            "expected Failed when timeout expires before turn/start, got {outcome:?}"
        );
        assert!(!env.queue_marker.exists());
    })
    .await
    .expect("synthetic reconnect fixture exceeded its deadline");
}

async fn complete_valid_exchange(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>,
    expected_cwd: &str,
    turn_start_count: &AtomicUsize,
) {
    let msg1 = read_json(ws).await;
    assert_eq!(msg1["method"], "initialize");
    ws.send(Message::Text(
        json!({ "id": 1, "result": { "serverInfo": { "name": "fake" } } })
            .to_string()
            .into(),
    ))
    .await
    .expect("send init resp");

    let msg2 = read_json(ws).await;
    assert_eq!(msg2["method"], "initialized");

    let msg3 = read_json(ws).await;
    assert_eq!(msg3["method"], "thread/read");
    ws.send(Message::Text(
        json!({
            "id": 2,
            "result": {
                "thread": {
                    "id": TEST_THREAD_ID,
                    "cwd": expected_cwd,
                    "status": { "type": "idle" }
                }
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send read resp");

    let msg4 = read_json(ws).await;
    assert_eq!(msg4["method"], "thread/resume");
    ws.send(Message::Text(
        json!({
            "id": 3,
            "result": {
                "thread": {
                    "id": TEST_THREAD_ID,
                    "cwd": expected_cwd,
                    "status": { "type": "idle" }
                },
                "model": "gpt-5-codex",
                "reasoningEffort": "high"
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send resume resp");

    let msg5 = read_json(ws).await;
    assert_eq!(msg5["method"], "turn/start");
    turn_start_count.fetch_add(1, Ordering::SeqCst);
    ws.send(Message::Text(
        json!({
            "id": 4,
            "result": {
                "turn": {
                    "id": TEST_TURN_ID,
                    "status": { "type": "inProgress" },
                    "error": null
                }
            }
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send turn/start resp");
}

async fn read_json(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::UnixStream>) -> Value {
    loop {
        let msg = ws
            .next()
            .await
            .expect("frame present")
            .expect("valid frame");
        match msg {
            Message::Text(t) => return serde_json::from_str(&t).expect("valid json"),
            Message::Pong(_) | Message::Ping(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}
