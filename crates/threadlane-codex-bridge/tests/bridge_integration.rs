#![forbid(unsafe_code)]

//! Transport-only integration and subprocess tests for `threadlane-codex-bridge`.
//!
//! All tests in this file use a synthetic owned fake backend executable and local Unix domain
//! sockets only. They never invoke any real Codex binary, model provider, or desktop application.

#[cfg(unix)]
mod unix_tests {
    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};
    use std::os::unix::net::UnixListener as StdUnixListener;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Output, Stdio};
    use std::time::{Duration, Instant};

    use futures_util::{SinkExt, StreamExt};
    use rustix::process::{getpgrp, kill_process, Pid, Signal};
    use serde_json::{json, Value};
    use tempfile::TempDir;
    use threadlane_codex_bridge::{
        BridgeDescriptor, DESCRIPTOR_STDERR_PREFIX, FIXED_ERROR_DIAGNOSTIC, MAX_FRAME_BYTES,
        REAL_CODEX_ENV, RUNTIME_DIR_ENV,
    };
    use tokio::net::UnixListener as TokioUnixListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

    const BRIDGE_BIN: &str = env!("CARGO_BIN_EXE_threadlane-codex-bridge");

    fn pid_exists(pid: u32) -> bool {
        let Ok(raw_pid) = i32::try_from(pid) else {
            return false;
        };
        if raw_pid <= 1 {
            return false;
        }
        let Some(rpid) = Pid::from_raw(raw_pid) else {
            return false;
        };
        if rustix::process::test_kill_process(rpid).is_err() {
            return false;
        }
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            if let Some(rparen) = stat.rfind(')') {
                let rest = stat[rparen + 1..].trim_start();
                if rest.starts_with('Z') || rest.starts_with('X') || rest.starts_with('x') {
                    return false;
                }
            }
        }
        true
    }

    fn wait_for_pid_exit(pid: u32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if !pid_exists(pid) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        !pid_exists(pid)
    }

    fn record_fake_event(payload: Value) {
        let Ok(log_path) = std::env::var("FAKE_CODEX_LOG_FILE") else {
            return;
        };
        if log_path.is_empty() {
            return;
        }
        let mut obj = match payload {
            Value::Object(map) => map,
            _ => return,
        };
        obj.insert("transport_only".to_string(), Value::Bool(true));
        if let Ok(mut file) = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            if let Ok(line) = serde_json::to_string(&Value::Object(obj)) {
                let _ = writeln!(file, "{line}");
                let _ = file.flush();
            }
        }
    }

    fn run_raw_byte_relay(advertised_sock: &Path) {
        let _ = rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
        let Ok(listener) = StdUnixListener::bind(advertised_sock) else {
            return;
        };
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
            let mut buf = [0u8; 256];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"{\"raw_byte_relay\":true,\"not_websocket\":true}\n");
            let _ = stream.flush();
        }
    }

    async fn run_fake_ws_server(advertised_sock: PathBuf, mode: String) {
        use tokio::signal::unix::{signal, SignalKind};

        let bind_sock = if mode == "ws_symlink" {
            let ext_dir = std::env::var("FAKE_CODEX_EXTERNAL_SOCK_DIR")
                .expect("FAKE_CODEX_EXTERNAL_SOCK_DIR required for ws_symlink");
            PathBuf::from(ext_dir).join("physical.sock")
        } else {
            advertised_sock.clone()
        };

        if let Ok(desc_pid_file) = std::env::var("FAKE_CODEX_DESCENDANT_PID_FILE") {
            if !desc_pid_file.is_empty() {
                if let Ok(child) = Command::new("/bin/sh")
                    .args(["-c", "sleep 60"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    let _ = fs::write(desc_pid_file, format!("{}\n", child.id()));
                }
            }
        }

        let mut sigterm = signal(SignalKind::terminate()).expect("sigterm");
        let mut sigint = signal(SignalKind::interrupt()).expect("sigint");
        let mut sighup = signal(SignalKind::hangup()).expect("sighup");

        let _ = rustix::process::umask(rustix::fs::Mode::from_raw_mode(0o077));
        let listener = TokioUnixListener::bind(&bind_sock).expect("bind unix ws listener");

        if mode == "ws_symlink" {
            symlink(&bind_sock, &advertised_sock).expect("symlink advertised socket");
        }

        let accept_res = tokio::select! {
            _ = sigterm.recv() => return,
            _ = sigint.recv() => return,
            _ = sighup.recv() => return,
            res = listener.accept() => res,
        };

        let Ok((stream, _)) = accept_res else {
            return;
        };

        let mut captured_headers: BTreeMap<String, String> = BTreeMap::new();
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_FRAME_BYTES);
        ws_config.max_frame_size = Some(MAX_FRAME_BYTES);

        // Tungstenite fixes this callback's error type to an HTTP response.
        #[allow(clippy::result_large_err)]
        let callback = |req: &Request, response: Response| {
            for key in [
                "Upgrade",
                "Connection",
                "Sec-WebSocket-Key",
                "Sec-WebSocket-Version",
            ] {
                if let Some(val) = req.headers().get(key).and_then(|v| v.to_str().ok()) {
                    captured_headers.insert(key.to_ascii_lowercase(), val.to_string());
                }
            }
            Ok(response)
        };

        let Ok(mut ws) =
            tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(ws_config))
                .await
        else {
            return;
        };

        record_fake_event(json!({
            "event": "handshake",
            "pid": std::process::id(),
            "headers": captured_headers,
        }));

        if mode == "ws_send_binary" {
            let _ = ws.next().await;
            let _ = ws
                .send(Message::Binary(vec![0x00, 0x01, 0x02, 0x03].into()))
                .await;
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
                _ = sighup.recv() => {}
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            return;
        }

        if mode == "ws_send_newline" {
            let _ = ws.next().await;
            let _ = ws
                .send(Message::Text(
                    "{\"jsonrpc\":\"2.0\",\"method\":\"bad\"}\n{\"injected\":true}".into(),
                ))
                .await;
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
                _ = sighup.recv() => {}
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            return;
        }

        if let Ok(notif) = std::env::var("FAKE_CODEX_SERVER_NOTIFICATION") {
            if !notif.is_empty() {
                let _ = ws.send(Message::Text(notif.into())).await;
            }
        }

        loop {
            tokio::select! {
                _ = sigterm.recv() => break,
                _ = sigint.recv() => break,
                _ = sighup.recv() => break,
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            let s = text.as_str().to_string();
                            record_fake_event(json!({
                                "event": "ws_frame_received",
                                "frame": s,
                                "is_text": true,
                            }));
                            if ws.send(Message::Text(s.into())).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            let _ = ws.close(None).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) => break,
                    }
                }
            }
        }

        // Wait until terminated by the bridge's process-group cleanup.
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
            _ = sighup.recv() => {}
            _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
    }

    #[test]
    fn fake_codex_subprocess_entry() {
        if std::env::var("FAKE_CODEX_SUBPROCESS").as_deref() != Ok("1") {
            return;
        }

        let argc: usize = std::env::var("FAKE_CODEX_ARGC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let mut args = Vec::with_capacity(argc);
        for i in 0..argc {
            let val = std::env::var(format!("FAKE_CODEX_ARG_{i}")).unwrap_or_default();
            args.push(val);
        }

        let mode = std::env::var("FAKE_CODEX_MODE").unwrap_or_else(|_| "ws_echo".to_string());
        record_fake_event(json!({
            "event": "spawn",
            "pid": std::process::id(),
            "pgid": getpgrp().as_raw_nonzero().get(),
            "argv": args,
        }));

        if mode != "passthrough"
            && args.iter().any(|a| a == "app-server")
            && args.len() >= 3
            && args[args.len() - 2] == "--listen"
            && args[args.len() - 1].starts_with("unix://")
        {
            let sock_path = PathBuf::from(&args[args.len() - 1]["unix://".len()..]);
            if mode == "early_exit" {
                let code: i32 = std::env::var("FAKE_CODEX_EXIT_CODE")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(42);
                std::process::exit(code);
            }
            if mode == "raw_byte_relay" {
                run_raw_byte_relay(&sock_path);
                std::process::exit(0);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build fake codex tokio runtime");
            rt.block_on(run_fake_ws_server(sock_path, mode));
            std::process::exit(0);
        }

        record_fake_event(json!({
            "event": "passthrough",
            "pid": std::process::id(),
            "argv": args,
        }));

        let out_line = serde_json::to_string(&json!({
            "passthrough_argv": args,
            "transport_only": true,
        }))
        .expect("serialize passthrough json");

        if let Ok(mut real_stdout) = fs::OpenOptions::new()
            .write(true)
            .open("/proc/self/fd/3")
            .or_else(|_| fs::OpenOptions::new().write(true).open("/dev/fd/3"))
        {
            let _ = writeln!(real_stdout, "{out_line}");
            let _ = real_stdout.flush();
        }
        std::process::exit(0);
    }

    struct BridgeTestFixture {
        _tmp: TempDir,
        test_root: PathBuf,
        runtime_dir: PathBuf,
        fake_codex: PathBuf,
        log_file: PathBuf,
        descendant_pid_file: PathBuf,
    }

    impl BridgeTestFixture {
        fn new() -> Self {
            // Use /tmp directly to guarantee short Unix socket paths (< 100 bytes).
            let raw_tmp = tempfile::Builder::new()
                .prefix("tl-b1-")
                .tempdir_in("/tmp")
                .expect("create short tempdir in /tmp");
            let test_root = fs::canonicalize(raw_tmp.path()).expect("canonicalize test_root");
            fs::set_permissions(&test_root, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 test_root");

            let runtime_dir = test_root.join("rt");
            fs::create_dir(&runtime_dir).expect("create runtime_dir");
            fs::set_permissions(&runtime_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 runtime_dir");

            let bin_dir = test_root.join("bin");
            fs::create_dir(&bin_dir).expect("create bin_dir");
            fs::set_permissions(&bin_dir, fs::Permissions::from_mode(0o700))
                .expect("chmod 0700 bin_dir");

            let fake_codex = bin_dir.join("fake-codex");
            let test_exe = std::env::current_exe().expect("current test executable");
            let script = format!(
                "#!/bin/sh\n\
                 exec 3>&1 1>/dev/null\n\
                 export FAKE_CODEX_SUBPROCESS=1\n\
                 export FAKE_CODEX_ARGC=\"$#\"\n\
                 i=0\n\
                 for arg in \"$@\"; do\n\
                     export \"FAKE_CODEX_ARG_$i=$arg\"\n\
                     i=$((i + 1))\n\
                 done\n\
                 exec \"{}\" unix_tests::fake_codex_subprocess_entry --exact --nocapture\n",
                test_exe.display()
            );

            let staged = bin_dir.join(".fake-codex.tmp");
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o755)
                .open(&staged)
                .expect("open staged fake-codex");
            file.write_all(script.as_bytes()).expect("write fake-codex");
            file.sync_all().expect("sync fake-codex");
            drop(file);
            fs::rename(&staged, &fake_codex).expect("rename fake-codex");

            let log_file = test_root.join("fake_events.jsonl");
            let descendant_pid_file = test_root.join("descendant.pid");

            Self {
                _tmp: raw_tmp,
                test_root,
                runtime_dir,
                fake_codex,
                log_file,
                descendant_pid_file,
            }
        }

        fn base_command(&self, args: &[&str]) -> Command {
            let mut cmd = Command::new(BRIDGE_BIN);
            cmd.args(args)
                .env(REAL_CODEX_ENV, &self.fake_codex)
                .env(RUNTIME_DIR_ENV, &self.runtime_dir)
                .env("FAKE_CODEX_LOG_FILE", &self.log_file)
                .env_remove("FAKE_CODEX_SUBPROCESS")
                .env_remove("FAKE_CODEX_MODE")
                .env_remove("FAKE_CODEX_EXIT_CODE")
                .env_remove("FAKE_CODEX_SERVER_NOTIFICATION")
                .env_remove("FAKE_CODEX_EXTERNAL_SOCK_DIR")
                .env_remove("FAKE_CODEX_DESCENDANT_PID_FILE");
            cmd
        }

        fn read_events(&self) -> Vec<Value> {
            let Ok(content) = fs::read_to_string(&self.log_file) else {
                return Vec::new();
            };
            content
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).expect("parse fake event json"))
                .collect()
        }

        fn clear_events(&self) {
            let _ = fs::remove_file(&self.log_file);
        }

        fn runtime_entries(&self) -> Vec<PathBuf> {
            let Ok(rd) = fs::read_dir(&self.runtime_dir) else {
                return Vec::new();
            };
            rd.filter_map(Result::ok).map(|e| e.path()).collect()
        }
    }

    fn wait_for_descriptor_line<R: BufRead>(stderr: &mut R) -> PathBuf {
        let mut line = String::new();
        let bytes = stderr
            .read_line(&mut line)
            .expect("read descriptor line from stderr");
        assert!(bytes > 0, "unexpected EOF waiting for descriptor line");
        let trimmed = line.trim();
        let path_str = trimmed
            .strip_prefix(DESCRIPTOR_STDERR_PREFIX)
            .unwrap_or_else(|| panic!("unexpected stderr line: {trimmed:?}"));
        PathBuf::from(path_str)
    }

    fn kill_and_reap(child: &mut Child) {
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    #[test]
    fn offline_help_and_version_subprocesses_work_without_real_codex() {
        for flag in ["--version", "-V", "--threadlane-bridge-version"] {
            let out = Command::new(BRIDGE_BIN)
                .arg(flag)
                .env_remove(REAL_CODEX_ENV)
                .env_remove(RUNTIME_DIR_ENV)
                .output()
                .expect("run bridge version");
            assert!(out.status.success(), "flag {flag} failed");
            assert!(out.stderr.is_empty());
            let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
            assert_eq!(stdout, threadlane_codex_bridge::version_text());
        }

        for flag in ["--help", "-h", "--threadlane-bridge-help"] {
            let out = Command::new(BRIDGE_BIN)
                .arg(flag)
                .env_remove(REAL_CODEX_ENV)
                .env_remove(RUNTIME_DIR_ENV)
                .output()
                .expect("run bridge help");
            assert!(out.status.success(), "flag {flag} failed");
            assert!(out.stderr.is_empty());
            let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
            assert_eq!(stdout, threadlane_codex_bridge::help_text());
        }
    }

    #[test]
    fn websocket_handshake_roundtrip_descriptor_and_process_group_cleanup() {
        let fx = BridgeTestFixture::new();
        let server_notif = "{\"jsonrpc\":\"2.0\",\"method\":\"codex/event\",\"params\":{\"transport_only\":true,\"kind\":\"server_notification\",\"seq\":1}}";

        let mut cmd = fx.base_command(&["app-server"]);
        cmd.env("FAKE_CODEX_SERVER_NOTIFICATION", server_notif)
            .env("FAKE_CODEX_DESCENDANT_PID_FILE", &fx.descendant_pid_file)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut proc = cmd.spawn().expect("spawn bridge");
        let mut stdin = proc.stdin.take().expect("stdin");
        let stdout = proc.stdout.take().expect("stdout");
        let stderr = proc.stderr.take().expect("stderr");
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);

        let descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
        assert!(descriptor_path.exists());
        let desc_meta = fs::symlink_metadata(&descriptor_path).expect("lstat descriptor");
        assert_eq!(desc_meta.permissions().mode() & 0o7777, 0o600);

        let descriptor: BridgeDescriptor = serde_json::from_str(
            &fs::read_to_string(&descriptor_path).expect("read descriptor.json"),
        )
        .expect("parse descriptor.json");

        assert_eq!(descriptor.schema_version, 1);
        assert_eq!(descriptor.wrapper_pid, proc.id());
        assert_eq!(Path::new(&descriptor.real_codex), fx.fake_codex.as_path());
        assert_eq!(descriptor.transport, "unix_websocket");

        let backend_pid = descriptor.backend_pid;
        let socket_path = PathBuf::from(&descriptor.socket_path);
        assert!(socket_path.starts_with(&fx.runtime_dir));
        assert!(socket_path.as_os_str().as_bytes().len() < 100);
        assert!(pid_exists(backend_pid));

        let descendant_pid: u32 = fs::read_to_string(&fx.descendant_pid_file)
            .expect("read descendant pid")
            .trim()
            .parse()
            .expect("parse descendant pid");
        assert!(pid_exists(descendant_pid));

        let frames_to_send = [
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"clientInfo\":{\"name\":\"chatgpt-native\",\"version\":\"26.1027.30824\"},\"capabilities\":{}}}",
            "{\"jsonrpc\":\"2.0\",\"method\":\"initialized\",\"params\":{}}",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"serverInfo\":{\"name\":\"codex-fake\",\"version\":\"0.166.0-alpha.5.1\"}}}",
            "{\"jsonrpc\":\"2.0\",\"id\":\"req-2\",\"method\":\"thread/sendUserMessage\",\"params\":{\"text\":\"hello\",\"transport_only\":true}}",
            "{\"jsonrpc\":\"2.0\",\"id\":\"req-2\",\"result\":{\"accepted\":true,\"transport_only\":true}}",
            "{\"jsonrpc\":\"2.0\",\"method\":\"client/userNotification\",\"params\":{\"transport_only\":true,\"active\":true}}",
        ];

        for frame in frames_to_send {
            writeln!(stdin, "{frame}").expect("write frame to stdin");
        }
        stdin.flush().expect("flush stdin");

        let expected_count = 1 + frames_to_send.len();
        let mut received_lines = Vec::with_capacity(expected_count);
        for _ in 0..expected_count {
            let mut raw = String::new();
            let n = stdout_reader
                .read_line(&mut raw)
                .expect("read line from bridge stdout");
            assert!(n > 0, "unexpected EOF on bridge stdout");
            assert!(raw.ends_with('\n'));
            received_lines.push(raw.trim_end_matches('\n').to_string());
        }

        let mut expected_lines = vec![server_notif.to_string()];
        expected_lines.extend(frames_to_send.iter().map(|s| (*s).to_string()));
        assert_eq!(received_lines, expected_lines);

        drop(stdin);
        let status = proc.wait().expect("wait for bridge exit");
        assert_eq!(status.code(), Some(0));

        let mut extra_stdout = String::new();
        stdout_reader
            .read_to_string(&mut extra_stdout)
            .expect("read remaining stdout");
        assert!(extra_stdout.is_empty());

        assert!(wait_for_pid_exit(backend_pid, Duration::from_secs(3)));
        assert!(wait_for_pid_exit(descendant_pid, Duration::from_secs(3)));
        assert!(!descriptor_path.exists());
        assert!(fs::symlink_metadata(&socket_path).is_err());
        assert!(fx.runtime_entries().is_empty());

        let events = fx.read_events();
        let spawns: Vec<&Value> = events.iter().filter(|e| e["event"] == "spawn").collect();
        assert_eq!(spawns.len(), 1);
        assert_eq!(
            spawns[0]["argv"],
            json!([
                "app-server",
                "--listen",
                format!("unix://{}", socket_path.display())
            ])
        );

        let handshakes: Vec<&Value> = events
            .iter()
            .filter(|e| e["event"] == "handshake")
            .collect();
        assert_eq!(handshakes.len(), 1);
        let headers = &handshakes[0]["headers"];
        assert_eq!(
            headers["upgrade"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase(),
            "websocket"
        );
        assert!(headers["connection"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("upgrade"));
        assert_eq!(headers["sec-websocket-version"], "13");
        assert!(!headers["sec-websocket-key"]
            .as_str()
            .unwrap_or_default()
            .is_empty());
    }

    #[test]
    fn stdio_listen_variants_intercepted() {
        let fx = BridgeTestFixture::new();
        let variants: &[&[&str]] = &[
            &["app-server", "--listen", "stdio://"],
            &["app-server", "--listen=stdio://"],
            &["app-server", "--stdio"],
        ];

        for &variant in variants {
            fx.clear_events();
            let mut cmd = fx.base_command(variant);
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            let mut proc = cmd.spawn().expect("spawn variant");
            let mut stdin = proc.stdin.take().expect("stdin");
            let stdout = proc.stdout.take().expect("stdout");
            let stderr = proc.stderr.take().expect("stderr");
            let mut stdout_reader = BufReader::new(stdout);
            let mut stderr_reader = BufReader::new(stderr);

            let _desc = wait_for_descriptor_line(&mut stderr_reader);
            let frame = "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"ping\"}";
            writeln!(stdin, "{frame}").expect("write ping");
            stdin.flush().expect("flush ping");

            let mut echoed = String::new();
            stdout_reader.read_line(&mut echoed).expect("read echo");
            assert_eq!(echoed, format!("{frame}\n"));

            drop(stdin);
            let status = proc.wait().expect("wait variant");
            assert_eq!(status.code(), Some(0));
            assert!(fx.runtime_entries().is_empty());

            let events = fx.read_events();
            let spawns: Vec<&Value> = events.iter().filter(|e| e["event"] == "spawn").collect();
            assert_eq!(spawns.len(), 1);
        }
    }

    #[test]
    fn prefixed_native_shape_roundtrip_and_argv_preservation() {
        let fx = BridgeTestFixture::new();
        let opaque_1 = "synthetic_opaque_1=\"alpha:1=true\"";
        let opaque_2 = "synthetic_opaque_2={nested=[\"app-server\",\"--stdio\"],count=2}";
        let native_argv = [
            "-c",
            opaque_1,
            "-c",
            opaque_2,
            "app-server",
            "--analytics-default-enabled",
        ];
        let server_notif = "{\"jsonrpc\":\"2.0\",\"method\":\"codex/event\",\"params\":{\"transport_only\":true,\"kind\":\"prefixed_native_notif\"}}";

        let mut cmd = fx.base_command(&native_argv);
        cmd.env("FAKE_CODEX_SERVER_NOTIFICATION", server_notif)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut proc = cmd.spawn().expect("spawn prefixed native shape");
        let mut stdin = proc.stdin.take().expect("stdin");
        let stdout = proc.stdout.take().expect("stdout");
        let stderr = proc.stderr.take().expect("stderr");
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);

        let descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
        let descriptor: BridgeDescriptor =
            serde_json::from_str(&fs::read_to_string(&descriptor_path).expect("read descriptor"))
                .expect("parse descriptor");
        let backend_pid = descriptor.backend_pid;
        let socket_path = PathBuf::from(&descriptor.socket_path);
        assert!(pid_exists(backend_pid));

        let req_frame =
            "{\"jsonrpc\":\"2.0\",\"id\":101,\"method\":\"initialize\",\"params\":{\"transport_only\":true}}";
        writeln!(stdin, "{req_frame}").expect("write request");
        stdin.flush().expect("flush request");

        let mut line1 = String::new();
        let mut line2 = String::new();
        stdout_reader.read_line(&mut line1).expect("read line 1");
        stdout_reader.read_line(&mut line2).expect("read line 2");
        assert_eq!(line1.trim_end_matches('\n'), server_notif);
        assert_eq!(line2.trim_end_matches('\n'), req_frame);

        drop(stdin);
        let status = proc.wait().expect("wait prefixed native");
        assert_eq!(status.code(), Some(0));
        assert!(wait_for_pid_exit(backend_pid, Duration::from_secs(3)));
        assert!(!descriptor_path.exists());
        assert!(fs::symlink_metadata(&socket_path).is_err());
        assert!(fx.runtime_entries().is_empty());

        let events = fx.read_events();
        let spawns: Vec<&Value> = events.iter().filter(|e| e["event"] == "spawn").collect();
        assert_eq!(spawns.len(), 1);
        assert_eq!(
            spawns[0]["argv"],
            json!([
                "-c",
                opaque_1,
                "-c",
                opaque_2,
                "app-server",
                "--analytics-default-enabled",
                "--listen",
                format!("unix://{}", socket_path.display())
            ])
        );
    }

    #[test]
    fn negative_and_unrelated_passthrough_cases_exec_unchanged_without_sockets() {
        let fx = BridgeTestFixture::new();
        let sample_sock = fx.test_root.join("neg-passthrough.sock");
        let sample_sock_str = sample_sock.to_str().expect("utf8 sock").to_string();
        let unix_sock_arg = format!("unix://{sample_sock_str}");
        let eq_unix_sock_arg = format!("--listen=unix://{sample_sock_str}");
        let missing_rt = fx.test_root.join("neg-nonexistent-rt");

        let passthrough_cases: Vec<Vec<&str>> = vec![
            vec!["--version"],
            vec!["exec", "--model", "o4-mini", "--full-auto", "echo hi"],
            vec!["-c", "app-server"],
            vec!["--config", "app-server"],
            vec!["--enable", "app-server"],
            vec!["--disable", "app-server"],
            vec!["-c", "app-server", "exec", "noop"],
            vec!["-c"],
            vec!["--config"],
            vec!["--config="],
            vec!["--enable"],
            vec!["--disable"],
            vec!["app-server", "-c"],
            vec!["app-server", "--config="],
            vec!["app-server", "--listen"],
            vec!["--unknown-global", "app-server"],
            vec!["-c", "k=v", "app-server", "--unknown-flag"],
            vec!["app-server", "--model", "o4-mini"],
            vec!["app-server", "--help"],
            vec!["app-server", "-h"],
            vec!["app-server", "--print-startup-config"],
            vec!["app-server", "startup-sign-out"],
            vec!["app-server", "proxy", "--sock", &sample_sock_str],
            vec!["app-server", "--listen", &unix_sock_arg],
            vec!["app-server", &eq_unix_sock_arg],
            vec!["app-server", "--listen", "ws://127.0.0.1:9999"],
            vec!["app-server", "--stdio", "--stdio"],
            vec!["app-server", "--stdio", "--listen", "stdio://"],
            vec!["app-server", "--listen", "stdio://", "--listen=stdio://"],
        ];

        for argv in passthrough_cases {
            fx.clear_events();
            let out: Output = fx
                .base_command(&argv)
                .env(RUNTIME_DIR_ENV, &missing_rt)
                .env("FAKE_CODEX_MODE", "passthrough")
                .stdin(Stdio::null())
                .output()
                .expect("run passthrough case");

            assert_eq!(out.status.code(), Some(0), "failed for argv: {argv:?}");
            assert!(out.stderr.is_empty(), "non-empty stderr for argv: {argv:?}");

            let payload: Value =
                serde_json::from_slice(&out.stdout).expect("parse passthrough stdout json");
            assert_eq!(payload["passthrough_argv"], json!(argv));
            assert!(!missing_rt.exists());
            assert!(fs::symlink_metadata(&sample_sock).is_err());
            assert!(fx.runtime_entries().is_empty());

            let events = fx.read_events();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0]["event"], "spawn");
            assert_eq!(events[0]["argv"], json!(argv));
            assert_eq!(events[1]["event"], "passthrough");
            assert_eq!(events[1]["argv"], json!(argv));
        }
    }

    #[test]
    fn raw_non_websocket_socket_rejected() {
        let fx = BridgeTestFixture::new();
        let mut cmd = fx.base_command(&["app-server"]);
        cmd.env("FAKE_CODEX_MODE", "raw_byte_relay")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut proc = cmd.spawn().expect("spawn raw byte relay case");
        if let Some(mut stdin) = proc.stdin.take() {
            let _ = writeln!(
                stdin,
                "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}}"
            );
        }
        let out = proc.wait_with_output().expect("wait raw byte relay");
        assert_ne!(out.status.code(), Some(0));
        assert!(out.stdout.is_empty());
        assert_eq!(String::from_utf8_lossy(&out.stderr), FIXED_ERROR_DIAGNOSTIC);
        assert!(fx.runtime_entries().is_empty());

        let events = fx.read_events();
        let spawns: Vec<&Value> = events.iter().filter(|e| e["event"] == "spawn").collect();
        assert_eq!(spawns.len(), 1);
    }

    #[test]
    fn signal_cleanup_terminates_backend_and_removes_socket() {
        let fx = BridgeTestFixture::new();
        for (sig, sig_num) in [(Signal::TERM, 15), (Signal::INT, 2), (Signal::HUP, 1)] {
            let _ = fs::remove_file(&fx.descendant_pid_file);
            let mut cmd = fx.base_command(&["app-server"]);
            cmd.env("FAKE_CODEX_DESCENDANT_PID_FILE", &fx.descendant_pid_file)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            let mut proc = cmd.spawn().expect("spawn bridge for signal test");
            let stderr = proc.stderr.take().expect("stderr");
            let mut stderr_reader = BufReader::new(stderr);

            let descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
            let descriptor: BridgeDescriptor = serde_json::from_str(
                &fs::read_to_string(&descriptor_path).expect("read descriptor"),
            )
            .expect("parse descriptor");
            let backend_pid = descriptor.backend_pid;
            let socket_path = PathBuf::from(&descriptor.socket_path);
            assert!(pid_exists(backend_pid));

            let descendant_pid: u32 = fs::read_to_string(&fx.descendant_pid_file)
                .expect("read descendant pid")
                .trim()
                .parse()
                .expect("parse descendant pid");
            assert!(pid_exists(descendant_pid));

            let wrapper_pid =
                Pid::from_raw(i32::try_from(proc.id()).expect("pid i32")).expect("non-zero pid");
            kill_process(wrapper_pid, sig).expect("send signal to bridge");

            let status = proc.wait().expect("wait for bridge after signal");
            assert_eq!(status.code(), Some(128 + sig_num));

            let mut rest_stderr = String::new();
            stderr_reader
                .read_to_string(&mut rest_stderr)
                .expect("read remaining stderr");
            assert!(
                rest_stderr.is_empty(),
                "signal termination must not emit error diagnostic on stderr"
            );

            assert!(wait_for_pid_exit(backend_pid, Duration::from_secs(3)));
            assert!(wait_for_pid_exit(descendant_pid, Duration::from_secs(3)));
            assert!(!descriptor_path.exists());
            assert!(fs::symlink_metadata(&socket_path).is_err());
            assert!(fx.runtime_entries().is_empty());
        }
    }

    #[test]
    fn early_backend_error_truthful_nonzero_without_retry() {
        let fx = BridgeTestFixture::new();
        let out = fx
            .base_command(&["app-server"])
            .env("FAKE_CODEX_MODE", "early_exit")
            .env("FAKE_CODEX_EXIT_CODE", "42")
            .stdin(Stdio::null())
            .output()
            .expect("run early_exit case");

        assert_eq!(out.status.code(), Some(42));
        assert!(out.stdout.is_empty());
        assert_eq!(String::from_utf8_lossy(&out.stderr), FIXED_ERROR_DIAGNOSTIC);
        assert!(fx.runtime_entries().is_empty());

        let events = fx.read_events();
        let spawns: Vec<&Value> = events.iter().filter(|e| e["event"] == "spawn").collect();
        assert_eq!(spawns.len(), 1);
    }

    #[test]
    fn unsafe_runtime_root_rejected_before_process_spawn() {
        let fx = BridgeTestFixture::new();

        let open_dir = fx.test_root.join("rt-0755");
        fs::create_dir(&open_dir).expect("create open_dir");
        fs::set_permissions(&open_dir, fs::Permissions::from_mode(0o755)).expect("chmod 0755");

        let symlink_dir = fx.test_root.join("rt-symlink");
        symlink(&fx.runtime_dir, &symlink_dir).expect("create symlink_dir");

        let regular_file = fx.test_root.join("rt-file");
        fs::write(&regular_file, b"not a dir\n").expect("write regular_file");
        fs::set_permissions(&regular_file, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 regular_file");

        let long_dir = fx.test_root.join("a".repeat(95));
        fs::create_dir(&long_dir).expect("create long_dir");
        fs::set_permissions(&long_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 long_dir");

        let missing_dir = fx.test_root.join("does-not-exist");

        let bad_roots: Vec<Option<PathBuf>> = vec![
            None,
            Some(PathBuf::from("")),
            Some(PathBuf::from("relative/runtime/dir")),
            Some(missing_dir),
            Some(open_dir),
            Some(symlink_dir),
            Some(regular_file),
            Some(long_dir),
        ];

        for bad_root in bad_roots {
            fx.clear_events();
            let mut cmd = fx.base_command(&["app-server"]);
            match bad_root {
                Some(ref p) => {
                    cmd.env(RUNTIME_DIR_ENV, p);
                }
                None => {
                    cmd.env_remove(RUNTIME_DIR_ENV);
                }
            }
            let out = cmd
                .stdin(Stdio::null())
                .output()
                .expect("run unsafe runtime root case");

            assert_eq!(out.status.code(), Some(1), "failed for {bad_root:?}");
            assert!(out.stdout.is_empty());
            assert_eq!(String::from_utf8_lossy(&out.stderr), FIXED_ERROR_DIAGNOSTIC);
            assert!(fx.read_events().is_empty());
        }
    }

    #[test]
    fn invalid_real_codex_rejected_before_spawn() {
        let fx = BridgeTestFixture::new();

        let non_exec = fx.test_root.join("non-exec-codex");
        fs::write(&non_exec, b"#!/bin/sh\nexit 0\n").expect("write non_exec");
        fs::set_permissions(&non_exec, fs::Permissions::from_mode(0o600))
            .expect("chmod 0600 non_exec");

        let self_symlink = fx.test_root.join("bridge-self-symlink");
        symlink(BRIDGE_BIN, &self_symlink).expect("symlink to bridge binary");

        let invalid_paths: Vec<Option<PathBuf>> = vec![
            None,
            Some(PathBuf::from("")),
            Some(PathBuf::from("relative/codex")),
            Some(fx.test_root.join("missing-codex")),
            Some(fx.runtime_dir.clone()),
            Some(non_exec),
            Some(PathBuf::from(BRIDGE_BIN)),
            Some(self_symlink),
        ];

        for bad_codex in invalid_paths {
            fx.clear_events();
            let mut cmd = fx.base_command(&["app-server"]);
            match bad_codex {
                Some(ref p) => {
                    cmd.env(REAL_CODEX_ENV, p);
                }
                None => {
                    cmd.env_remove(REAL_CODEX_ENV);
                }
            }
            let out = cmd
                .stdin(Stdio::null())
                .output()
                .expect("run invalid real codex case");

            assert_eq!(out.status.code(), Some(1), "failed for {bad_codex:?}");
            assert!(out.stdout.is_empty());
            assert_eq!(String::from_utf8_lossy(&out.stderr), FIXED_ERROR_DIAGNOSTIC);
            assert!(fx.read_events().is_empty());
        }
    }

    #[test]
    fn oversized_stdin_and_malformed_utf8_rejected_without_leak() {
        let fx = BridgeTestFixture::new();
        let mut oversized = vec![b'x'; MAX_FRAME_BYTES + 4096];
        oversized.push(b'\n');
        let malformed_utf8 = b"{\"bad\":\"\xff\xfe\"}\n".to_vec();

        for (label, bad_bytes) in [("oversized", oversized), ("malformed_utf8", malformed_utf8)] {
            fx.clear_events();
            let mut cmd = fx.base_command(&["app-server"]);
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            let mut proc = cmd.spawn().expect("spawn bridge for bad stdin test");
            let mut stdin = proc.stdin.take().expect("stdin");
            let mut stdout = proc.stdout.take().expect("stdout");
            let stderr = proc.stderr.take().expect("stderr");
            let mut stderr_reader = BufReader::new(stderr);

            let descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
            let descriptor: BridgeDescriptor = serde_json::from_str(
                &fs::read_to_string(&descriptor_path).expect("read descriptor"),
            )
            .expect("parse descriptor");
            let backend_pid = descriptor.backend_pid;

            let _ = stdin.write_all(&bad_bytes);
            let _ = stdin.flush();
            drop(stdin);

            let status = proc.wait().expect("wait bridge on bad stdin");
            assert_ne!(
                status.code(),
                Some(0),
                "case {label} unexpectedly succeeded"
            );

            let mut out_bytes = Vec::new();
            stdout.read_to_end(&mut out_bytes).expect("read stdout");
            assert!(out_bytes.is_empty(), "case {label} leaked stdout");

            let mut stderr_rest = String::new();
            stderr_reader
                .read_to_string(&mut stderr_rest)
                .expect("read stderr");
            assert_eq!(stderr_rest, FIXED_ERROR_DIAGNOSTIC);

            assert!(wait_for_pid_exit(backend_pid, Duration::from_secs(3)));
            assert!(fx.runtime_entries().is_empty());
            kill_and_reap(&mut proc);
        }
    }

    #[test]
    fn invalid_ws_frames_rejected_without_stdout_leak() {
        let fx = BridgeTestFixture::new();

        for mode in ["ws_send_binary", "ws_send_newline"] {
            fx.clear_events();
            let mut cmd = fx.base_command(&["app-server"]);
            cmd.env("FAKE_CODEX_MODE", mode)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            let mut proc = cmd.spawn().expect("spawn bridge for invalid ws frame test");
            let mut stdin = proc.stdin.take().expect("stdin");
            let mut stdout = proc.stdout.take().expect("stdout");
            let stderr = proc.stderr.take().expect("stderr");
            let mut stderr_reader = BufReader::new(stderr);

            let descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
            let descriptor: BridgeDescriptor = serde_json::from_str(
                &fs::read_to_string(&descriptor_path).expect("read descriptor"),
            )
            .expect("parse descriptor");
            let backend_pid = descriptor.backend_pid;

            writeln!(
                stdin,
                "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"trigger\"}}"
            )
            .expect("write trigger");
            stdin.flush().expect("flush trigger");

            let status = proc.wait().expect("wait bridge on invalid ws frame");
            assert_ne!(status.code(), Some(0), "mode {mode} unexpectedly succeeded");

            let mut out_bytes = Vec::new();
            stdout.read_to_end(&mut out_bytes).expect("read stdout");
            assert!(out_bytes.is_empty(), "mode {mode} leaked stdout");

            let mut stderr_rest = String::new();
            stderr_reader
                .read_to_string(&mut stderr_rest)
                .expect("read stderr");
            assert_eq!(stderr_rest, FIXED_ERROR_DIAGNOSTIC);

            assert!(wait_for_pid_exit(backend_pid, Duration::from_secs(3)));
            assert!(fx.runtime_entries().is_empty());
            kill_and_reap(&mut proc);
            drop(stdin);
        }
    }

    #[test]
    fn symlink_socket_supported_and_external_target_preserved() {
        let fx = BridgeTestFixture::new();
        let ext_dir = fx.test_root.join("ext-sock-dir");
        fs::create_dir(&ext_dir).expect("create ext_dir");
        fs::set_permissions(&ext_dir, fs::Permissions::from_mode(0o700))
            .expect("chmod 0700 ext_dir");
        let marker_file = ext_dir.join("keep.txt");
        fs::write(&marker_file, b"preserve\n").expect("write marker_file");

        let mut cmd = fx.base_command(&["app-server"]);
        cmd.env("FAKE_CODEX_MODE", "ws_symlink")
            .env("FAKE_CODEX_EXTERNAL_SOCK_DIR", &ext_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut proc = cmd.spawn().expect("spawn ws_symlink bridge");
        let mut stdin = proc.stdin.take().expect("stdin");
        let stdout = proc.stdout.take().expect("stdout");
        let stderr = proc.stderr.take().expect("stderr");
        let mut stdout_reader = BufReader::new(stdout);
        let mut stderr_reader = BufReader::new(stderr);

        let _descriptor_path = wait_for_descriptor_line(&mut stderr_reader);
        let frame = "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"ping\"}";
        writeln!(stdin, "{frame}").expect("write ping");
        stdin.flush().expect("flush ping");

        let mut echoed = String::new();
        stdout_reader
            .read_line(&mut echoed)
            .expect("read echoed ping");
        assert_eq!(echoed, format!("{frame}\n"));

        drop(stdin);
        let status = proc.wait().expect("wait ws_symlink bridge");
        assert_eq!(status.code(), Some(0));
        assert!(fx.runtime_entries().is_empty());
        assert!(ext_dir.is_dir());
        assert!(marker_file.is_file());
        let _ = File::open(&marker_file).expect("marker file still readable");
    }
}
