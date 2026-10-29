#![deny(unsafe_code)]
#![deny(missing_docs)]

//! `threadlane-native`: native Codex `app-server proxy` metadata client, connection-epoch
//! paired MCP server (`threadlane-native-mcp`), and durable `expectedTurnId`-checked busy
//! wake-up adapter for Threadlane Milestone 4.
//!
//! ## Security and Protocol Invariants
//!
//! - **Public proxy only**: Spawns `codex app-server proxy [--sock <path>]` in an isolated Unix
//!   process group with bounded frame reading and timeouts; never starts a second server, never
//!   reads conversation turns/transcripts (`includeTurns: false`), and never calls `thread/resume`.
//! - **Stable discovery**: Never sends experimental `ThreadListParams.projectId` on the wire;
//!   uses stable `cwd` filtering on the wire and client-side `projectId` metadata filtering.
//! - **Authoritative thread identity**: Binds `thread.id` (UUIDv7), never shared-fork `sessionId`
//!   or non-unique chat titles.
//! - **Unbound MCP connection-epoch pairing**: Each MCP stdio child starts `Unbound` with a fresh
//!   CSPRNG `connection_nonce` and `epoch_id`, allowing only `connection_identity`. The trusted
//!   local operator queries the nonce via thread-scoped `mcpServer/tool/call` and enrolls
//!   `(connection_nonce, epoch_id) -> (binding_id, thread.id, projectId)` through the private
//!   `0700`/`0600` [`PairingStore`]. Connections shared across two threads are immediately
//!   quarantined (`QuarantinedSharedConnection`).
//! - **Honest verification gates**: Live two-chat and busy-turn routing report
//!   [`NativeVerificationStatus::NeedsVerification`] until an authorized live host experiment.

/// Public `codex app-server proxy` client and native thread metadata types.
pub mod client;
/// Structured error codes and redacted error types.
pub mod error;
/// Path validation, private `0700`/`0600` persistence, cross-process locking, and sanitization.
pub mod fs_safety;
/// Paired MCP stdio server delegating to [`threadlane::ThreadlaneMcpServer`] after enrollment.
pub mod mcp_pair;
/// Private operator connection-epoch pairing store and verification gate types.
pub mod pairing;
/// Bounded newline-delimited JSON-RPC proxy session and Unix process-group cleanup.
pub mod rpc;
/// Durable `Pending`/`Submitted`/`Unknown`/`Received` busy-turn wake-up store and `turn/steer` builder.
pub mod wakeup;

pub use client::{
    NativeClient, NativeConfig, NativeDiagnosticReport, NativeMcpServerStatus,
    NativeMcpToolCallResult, NativeThread, NativeThreadPage,
};
pub use error::{NativeError, NativeErrorCode};
pub use mcp_pair::{ConnectionIdentityParams, PairedMcpServer};
pub use pairing::{
    ConnectionEpochState, ConnectionIdentity, EnrollmentRecord, NativeVerificationStatus,
    OperatorEnrollRequest, PairingStore, CONNECTION_IDENTITY_TOOL, MCP_SERVER_NAME,
};
pub use rpc::{
    build_proxy_args, is_process_alive, is_process_group_alive, InitializeResult, ProxyCallError,
    DEFAULT_PROXY_TIMEOUT, MAX_RPC_FRAME_BYTES, MAX_RPC_STDERR_BYTES, MAX_RPC_TOTAL_STDOUT_BYTES,
};
pub use wakeup::{
    build_native_queue_fallback_args, build_turn_steer_params, NativeWakeupRecord,
    NativeWakeupStatus, NativeWakeupStore,
};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use serde_json::{json, Value};
    use tempfile::TempDir;
    use threadlane::{Supervisor, SupervisorClient, SupervisorConfig};
    use threadlane_core::FIXED_WAKEUP_MESSAGE;

    use super::*;

    fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, body).expect("write fake codex script");
        let mut perms = fs::metadata(&path).expect("stat script").permissions();
        perms.set_mode(0o700);
        fs::set_permissions(&path, perms).expect("chmod 0700 script");
        path
    }

    #[tokio::test]
    async fn threads_uses_stable_cwd_and_clientside_project_filter_without_wire_project_id() {
        let tmp = TempDir::new().expect("tempdir");
        let req_log = tmp.path().join("requests.jsonl");
        let project_cwd = tmp.path().join("workspace");
        fs::create_dir_all(&project_cwd).expect("create workspace dir");

        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
if [[ "${{1:-}}" != "app-server" || "${{2:-}}" != "proxy" ]]; then
  echo "unexpected argv: $*" >&2
  exit 2
fi
read -r init_line
printf '%s\n' "$init_line" >> "{log}"
printf '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0","codexHome":"/tmp","platformFamily":"unix","platformOs":"linux"}}}}\n'
read -r initialized_line
printf '%s\n' "$initialized_line" >> "{log}"
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
printf '{{"id":2,"result":{{"data":[{{"id":"0196ed00-0000-7000-8000-000000000001","sessionId":"shared-session-fork-999","projectId":"proj-alpha","name":"Alpha Chat","cwd":"{cwd}","status":{{"type":"active","activeFlags":["waitingOnUserInput"]}},"model":"gpt-6-astra","reasoningEffort":"high","preview":"secret user transcript text that must never be stored"}},{{"id":"0196ed00-0000-7000-8000-000000000002","sessionId":"shared-session-fork-999","projectId":"proj-beta","name":"Beta Chat","cwd":"{cwd}","status":{{"type":"idle"}},"model":"gemini-4-argon-high","reasoningEffort":"high"}}],"nextCursor":"cursor-page-2"}}}}\n'
"#,
            log = req_log.display(),
            cwd = project_cwd.display()
        );
        let exe = write_executable_script(tmp.path(), "fake-codex.sh", &script);
        let sock_path = tmp.path().join("proxy.sock");
        fs::write(&sock_path, b"").expect("create placeholder file");

        let client = NativeClient::new(NativeConfig {
            executable: exe,
            socket: Some(sock_path),
        })
        .expect("create NativeClient")
        .with_cwd_filter(project_cwd.clone())
        .expect("set cwd filter");

        let page = client
            .threads(
                Some("proj-alpha".to_string()),
                Some("cursor-page-1".to_string()),
            )
            .await
            .expect("list threads");

        assert_eq!(page.next_cursor.as_deref(), Some("cursor-page-2"));
        assert_eq!(page.threads.len(), 1);
        let t = &page.threads[0];
        // Must bind `thread.id`, never `sessionId`.
        assert_eq!(t.id, "0196ed00-0000-7000-8000-000000000001");
        assert_ne!(t.id, "shared-session-fork-999");
        assert_eq!(t.project_id.as_deref(), Some("proj-alpha"));
        assert_eq!(t.title.as_deref(), Some("Alpha Chat"));
        assert_eq!(t.cwd, project_cwd.to_string_lossy());
        assert_eq!(t.status, "active");
        assert_eq!(t.model.as_deref(), Some("gpt-6-astra"));
        assert_eq!(t.effort.as_deref(), Some("high"));

        let logged = fs::read_to_string(&req_log).expect("read request log");
        let lines: Vec<Value> = logged
            .lines()
            .map(|l| serde_json::from_str(l).expect("parse logged json"))
            .collect();
        assert_eq!(lines.len(), 3);
        assert!(
            lines[0].get("jsonrpc").is_none(),
            "proxy wire omits jsonrpc"
        );
        assert_eq!(lines[0]["method"], "initialize");
        assert_eq!(lines[0]["params"]["capabilities"]["experimentalApi"], false);
        assert_eq!(lines[1]["method"], "initialized");
        assert_eq!(lines[2]["method"], "thread/list");
        // Experimental `projectId` must never be emitted on the wire in `thread/list`.
        assert!(lines[2]["params"].get("projectId").is_none());
        assert_eq!(
            lines[2]["params"]["cwd"],
            project_cwd.to_string_lossy().as_ref()
        );
        assert_eq!(lines[2]["params"]["cursor"], "cursor-page-1");
        assert_eq!(lines[2]["params"]["useStateDbOnly"], true);
    }

    #[tokio::test]
    async fn thread_read_enforces_include_turns_false_and_authoritative_id_match() {
        let tmp = TempDir::new().expect("tempdir");
        let req_log = tmp.path().join("read_req.jsonl");

        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}\n'
read -r _initialized
read -r req_line
printf '%s\n' "$req_line" >> "{log}"
printf '{{"id":2,"result":{{"thread":{{"id":"0196ed00-0000-7000-8000-000000000099","sessionId":"shared-session-abc","projectId":"proj-1","name":null,"cwd":"/tmp","status":{{"type":"notLoaded"}},"model":null,"reasoningEffort":null}}}}}}\n'
"#,
            log = req_log.display()
        );
        let exe = write_executable_script(tmp.path(), "fake-codex-read.sh", &script);
        let client = NativeClient::new(NativeConfig {
            executable: exe,
            socket: None,
        })
        .expect("client");

        let thread = client
            .thread("0196ed00-0000-7000-8000-000000000099")
            .await
            .expect("thread read");
        assert_eq!(thread.id, "0196ed00-0000-7000-8000-000000000099");
        assert_eq!(thread.status, "not_loaded");
        assert_eq!(thread.title, None);

        let logged: Value =
            serde_json::from_str(fs::read_to_string(&req_log).expect("log").trim()).expect("json");
        assert_eq!(logged["method"], "thread/read");
        assert_eq!(logged["params"]["includeTurns"], false);
        assert_eq!(
            logged["params"]["threadId"],
            "0196ed00-0000-7000-8000-000000000099"
        );

        // Requesting a different thread ID than returned must fail with `thread_not_found`.
        let mismatch_err = client
            .thread("0196ed00-0000-7000-8000-000000000777")
            .await
            .expect_err("mismatched thread.id must be rejected");
        assert_eq!(mismatch_err.code(), "thread_not_found");
    }

    #[tokio::test]
    async fn proxy_overflow_and_timeout_terminate_and_reap_descendant_process_group() {
        let tmp = TempDir::new().expect("tempdir");
        let child_pid_file = tmp.path().join("descendant.pid");

        let script = format!(
            r#"#!/usr/bin/env bash
sleep 60 &
echo $! > "{pid_file}"
read -r _init
python3 -c 'import sys; sys.stdout.write("A" * 8192); sys.stdout.flush()' 2>/dev/null || printf '%08192d' 0
sleep 60
"#,
            pid_file = child_pid_file.display()
        );
        let exe = write_executable_script(tmp.path(), "fake-codex-overflow.sh", &script);
        let client = NativeClient::new(NativeConfig {
            executable: exe,
            socket: None,
        })
        .expect("client")
        .with_max_frame_bytes(2048)
        .expect("small frame cap")
        .with_timeout(Duration::from_millis(500))
        .expect("timeout");

        let err = client
            .thread("0196ed00-0000-7000-8000-000000000001")
            .await
            .expect_err("overflow or timeout expected");
        assert!(matches!(err.code(), "output_overflow" | "timed_out"));

        if let Ok(pid_str) = fs::read_to_string(&child_pid_file) {
            if let Ok(desc_pid) = pid_str.trim().parse::<u32>() {
                for _ in 0..20 {
                    if !is_process_alive(desc_pid) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert!(
                    !is_process_alive(desc_pid),
                    "spawned descendant inside proxy process group must be terminated and reaped"
                );
            }
        }
    }

    #[tokio::test]
    async fn paired_mcp_enforces_unbound_lockout_two_chat_scope_isolation_and_restart_expiry() {
        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");

        // Start a real local Threadlane supervisor for end-to-end delegation verification.
        let sup_handle = Supervisor::start(SupervisorConfig {
            state_dir: state_dir.clone(),
            tick_interval: Duration::from_millis(50),
            codex: None,
            workers: None,
        })
        .await
        .expect("start supervisor");

        let sup_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect supervisor client");
        let binding_a = sup_client
            .create_binding("0196ed00-0000-7000-8000-00000000000a", "Chat A")
            .await
            .expect("create binding A");
        let binding_b = sup_client
            .create_binding("0196ed00-0000-7000-8000-00000000000b", "Chat B")
            .await
            .expect("create binding B");

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");

        let mut mcp_a =
            PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create mcp A");
        let mcp_b = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create mcp B");

        // 1. Before enrollment, only `connection_identity` is allowed; scoped tools and model-driven
        //    enrollment/rebinding attempts are rejected even when `_meta` supplies fake IDs.
        let id_res = mcp_a
            .invoke_tool_json(
                "connection_identity",
                json!({}),
                Some(
                    json!({"threadId": binding_a.thread_uuid, "binding_id": binding_a.binding_id}),
                ),
            )
            .await
            .expect("connection_identity allowed while unbound");
        let id_payload = id_res.structured_content.expect("structured identity");
        assert_eq!(id_payload["data"]["state"], "unbound");
        assert_eq!(
            id_payload["data"]["verification_gate"],
            "needs_verification"
        );

        let unbound_status_err = mcp_a
            .invoke_tool_json(
                "status",
                json!({}),
                Some(
                    json!({"threadId": binding_a.thread_uuid, "binding_id": binding_a.binding_id}),
                ),
            )
            .await
            .expect_err("status must fail while unbound");
        assert_eq!(
            unbound_status_err.structured_content.unwrap()["error"]["code"],
            "unbound_connection"
        );

        let model_enroll_err = mcp_a
            .invoke_tool_json("enroll", json!({"binding_id": binding_a.binding_id}), None)
            .await
            .expect_err("model cannot call enroll over MCP");
        assert_eq!(
            model_enroll_err.structured_content.unwrap()["error"]["code"],
            "rebind_rejected"
        );

        // 2. Trusted operator enrolls Connection A -> Chat A and Connection B -> Chat B via
        //    thread-scoped `mcpServer/tool/call` (`connection_identity`).
        let fake_proxy_script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
read -r _init
printf '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}\n'
read -r _initialized
read -r req
if [[ "$req" == *'"method":"thread/read"'* ]]; then
  if [[ "$req" == *'0196ed00-0000-7000-8000-00000000000a'* ]]; then
    printf '{{"id":2,"result":{{"thread":{{"id":"0196ed00-0000-7000-8000-00000000000a","sessionId":"shared-fork","projectId":"proj-shared","name":"Chat A","cwd":"/tmp","status":{{"type":"idle"}}}}}}}}\n'
  else
    printf '{{"id":2,"result":{{"thread":{{"id":"0196ed00-0000-7000-8000-00000000000b","sessionId":"shared-fork","projectId":"proj-shared","name":"Chat B","cwd":"/tmp","status":{{"type":"idle"}}}}}}}}\n'
  fi
elif [[ "$req" == *'"method":"mcpServer/tool/call"'* ]]; then
  if [[ "$req" == *'0196ed00-0000-7000-8000-00000000000a'* ]]; then
    printf '{{"id":2,"result":{{"content":[],"structuredContent":{{"data":{{"connection_nonce":"{nonce_a}","epoch_id":"{epoch_a}","server_pid":{pid},"state":"unbound","enrolled_binding_id":null,"verification_gate":"needs_verification"}}}},"isError":false}}}}\n'
  else
    printf '{{"id":2,"result":{{"content":[],"structuredContent":{{"data":{{"connection_nonce":"{nonce_b}","epoch_id":"{epoch_b}","server_pid":{pid},"state":"unbound","enrolled_binding_id":null,"verification_gate":"needs_verification"}}}},"isError":false}}}}\n'
  fi
fi
"#,
            nonce_a = mcp_a.connection_nonce(),
            epoch_a = mcp_a.epoch_id(),
            nonce_b = mcp_b.connection_nonce(),
            epoch_b = mcp_b.epoch_id(),
            pid = std::process::id(),
        );
        let exe = write_executable_script(tmp.path(), "fake-codex-pair.sh", &fake_proxy_script);
        let native_client = NativeClient::new(NativeConfig {
            executable: exe,
            socket: None,
        })
        .expect("native client");

        let enrolled_a = native_client
            .enroll_thread_connection(
                &pairing_store,
                &operator_token,
                &binding_a.thread_uuid,
                &binding_a.binding_id,
                Some("proj-shared"),
            )
            .await
            .expect("enroll chat A");
        assert_eq!(enrolled_a.binding_id, binding_a.binding_id);

        let enrolled_b = native_client
            .enroll_thread_connection(
                &pairing_store,
                &operator_token,
                &binding_b.thread_uuid,
                &binding_b.binding_id,
                Some("proj-shared"),
            )
            .await
            .expect("enroll chat B");
        assert_eq!(enrolled_b.binding_id, binding_b.binding_id);

        // 3. Enrolled Chat A starts a demo run; Chat B cannot read or cancel Chat A's run.
        let start_a = mcp_a
            .invoke_tool_json(
                "start_demo",
                json!({
                    "command_key": "cmd-a-1",
                    "instruction": "Task scoped to Chat A",
                    "step_delay_ms": 5000
                }),
                None,
            )
            .await
            .expect("start_demo on Chat A")
            .structured_content
            .expect("structured start_demo");
        let run_a_id = start_a["data"]["run_id"]
            .as_str()
            .expect("run_id")
            .to_string();

        let cross_scope_err = mcp_b
            .invoke_tool_json("get_run", json!({ "run_id": run_a_id }), None)
            .await
            .expect_err("Chat B must not read Chat A's run");
        assert_eq!(
            cross_scope_err.structured_content.unwrap()["error"]["code"],
            "run_not_found"
        );

        // Passing `binding_id` in tool arguments to override scope is rejected.
        let smuggle_err = mcp_b
            .invoke_tool_json(
                "get_run",
                json!({
                    "run_id": run_a_id,
                    "binding_id": binding_a.binding_id
                }),
                None,
            )
            .await
            .expect_err("caller binding_id argument must be rejected");
        assert_eq!(
            smuggle_err.structured_content.unwrap()["error"]["code"],
            "rebind_rejected"
        );

        // 4. Restarting MCP server A expires its previous epoch and returns it to `Unbound`.
        let old_nonce_a = mcp_a.connection_nonce().to_string();
        let old_epoch_a = mcp_a.epoch_id().to_string();
        let restarted = mcp_a.restart_epoch().expect("restart epoch A");
        assert_eq!(restarted.state, ConnectionEpochState::Unbound);
        assert_ne!(restarted.epoch_id, old_epoch_a);

        let stale_check = pairing_store
            .verify_enrolled_binding(&old_nonce_a, &old_epoch_a)
            .expect_err("old epoch must be expired after restart");
        assert_eq!(stale_check.code(), "expired_epoch");

        let post_restart_call = mcp_a
            .invoke_tool_json("status", json!({}), None)
            .await
            .expect_err("restarted connection is unbound until re-enrolled");
        assert_eq!(
            post_restart_call.structured_content.unwrap()["error"]["code"],
            "unbound_connection"
        );

        sup_handle.shutdown().await;
    }

    #[tokio::test]
    async fn executable_validation_canonicalizes_symlink_chains_and_rejects_invalid_targets() {
        use std::os::unix::fs::symlink;

        let tmp = TempDir::new().expect("tempdir");
        let invoked_path_file = tmp.path().join("invoked_argv0.txt");
        let store_dir = tmp.path().join("nix-store-like/bin");
        fs::create_dir_all(&store_dir).expect("create store dir");

        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$0" > "{invoked}"
read -r _init
printf '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}\n'
read -r _initialized
read -r _req
printf '{{"id":2,"result":{{"thread":{{"id":"0196ed00-0000-7000-8000-000000000055","sessionId":"shared-session","projectId":"proj-nix","name":"Nix Chat","cwd":"/tmp","status":{{"type":"idle"}}}}}}}}\n'
"#,
            invoked = invoked_path_file.display()
        );
        let real_exe = write_executable_script(&store_dir, "codex-real", &script);
        let canonical_real_exe = fs::canonicalize(&real_exe).expect("canonical real exe");

        // Build a multi-hop symlink chain (directory symlink + leaf symlink, like NixOS profiles).
        let intermediate_dir = tmp.path().join("intermediate-profile");
        fs::create_dir_all(&intermediate_dir).expect("create intermediate dir");
        let leaf_link_in_inter = intermediate_dir.join("codex-link");
        symlink(&real_exe, &leaf_link_in_inter).expect("create leaf symlink");

        let profile_dir_link = tmp.path().join("per-user-profile-bin");
        symlink(&intermediate_dir, &profile_dir_link).expect("create directory symlink");
        let chained_exe_path = profile_dir_link.join("codex-link");

        let client = NativeClient::new(NativeConfig {
            executable: chained_exe_path,
            socket: None,
        })
        .expect("symlinked CLI executable should resolve to canonical target");
        assert_eq!(client.config().executable, canonical_real_exe);

        let thread = client
            .thread("0196ed00-0000-7000-8000-000000000055")
            .await
            .expect("invoke canonical target via NativeClient");
        assert_eq!(thread.id, "0196ed00-0000-7000-8000-000000000055");

        let launched_argv0 = fs::read_to_string(&invoked_path_file).expect("read invoked argv0");
        assert_eq!(PathBuf::from(launched_argv0), canonical_real_exe);

        // Non-executable file (even via symlink) must be rejected.
        let non_exec = store_dir.join("codex-non-exec");
        fs::write(&non_exec, "#!/usr/bin/env bash\n").expect("write non-exec");
        let mut perms = fs::metadata(&non_exec).expect("stat").permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&non_exec, perms).expect("chmod 0600");
        let non_exec_link = intermediate_dir.join("codex-non-exec-link");
        symlink(&non_exec, &non_exec_link).expect("symlink non-exec");
        let non_exec_err = NativeClient::new(NativeConfig {
            executable: non_exec_link,
            socket: None,
        })
        .expect_err("non-executable target must be rejected");
        assert_eq!(non_exec_err.code(), "invalid_config");

        // Directory target must be rejected.
        let dir_err = NativeClient::new(NativeConfig {
            executable: store_dir.clone(),
            socket: None,
        })
        .expect_err("directory executable path must be rejected");
        assert_eq!(dir_err.code(), "invalid_config");

        // Missing path and broken symlink must be rejected with `not_configured`.
        let missing_err = NativeClient::new(NativeConfig {
            executable: tmp.path().join("does-not-exist"),
            socket: None,
        })
        .expect_err("missing executable must be rejected");
        assert_eq!(missing_err.code(), "not_configured");

        let broken_link = intermediate_dir.join("broken-codex-link");
        symlink(tmp.path().join("missing-target"), &broken_link).expect("broken symlink");
        let broken_err = NativeClient::new(NativeConfig {
            executable: broken_link,
            socket: None,
        })
        .expect_err("broken executable symlink must be rejected");
        assert_eq!(broken_err.code(), "not_configured");

        // Private socket and pairing directories must still reject symlinks.
        let real_sock = tmp.path().join("real.sock");
        fs::write(&real_sock, b"").expect("write real sock");
        let symlink_sock = tmp.path().join("symlink.sock");
        symlink(&real_sock, &symlink_sock).expect("symlink sock");
        let sock_err = NativeClient::new(NativeConfig {
            executable: real_exe,
            socket: Some(symlink_sock),
        })
        .expect_err("symlink socket path must still be rejected");
        assert_eq!(sock_err.code(), "security_violation");
    }

    #[tokio::test]
    async fn shared_connection_between_two_threads_is_quarantined_for_both_chats() {
        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");
        let shared_mcp =
            PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create shared mcp");

        // Enroll shared_mcp for Thread 1.
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: shared_mcp.connection_nonce().to_string(),
                    epoch_id: shared_mcp.epoch_id().to_string(),
                    binding_id: "bnd_thread_1".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000001".to_string(),
                    project_id: Some("proj-1".to_string()),
                },
            )
            .expect("initial enrollment for Thread 1 succeeds");

        // Now Thread 2 attempts to enroll the SAME MCP connection nonce/epoch.
        let shared_err = pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: shared_mcp.connection_nonce().to_string(),
                    epoch_id: shared_mcp.epoch_id().to_string(),
                    binding_id: "bnd_thread_2".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000002".to_string(),
                    project_id: Some("proj-1".to_string()),
                },
            )
            .expect_err("shared connection across two threads must be rejected");
        assert_eq!(shared_err.code(), "shared_connection_rejected");

        // Verify the connection is now permanently quarantined so Thread 1 can no longer use it either!
        let thread_1_err = shared_mcp
            .invoke_tool_json("status", json!({}), None)
            .await
            .expect_err("quarantined shared connection must reject subsequent tool calls");
        assert_eq!(
            thread_1_err.structured_content.unwrap()["error"]["code"],
            "shared_connection_rejected"
        );
    }

    #[tokio::test]
    async fn busy_turn_steer_tracks_pending_submitted_unknown_received_and_blocks_blind_resend() {
        let tmp = TempDir::new().expect("tempdir");
        let wakeup_dir = tmp.path().join("wakeups");
        let wakeup_store = NativeWakeupStore::open(&wakeup_dir).expect("open wakeup store");

        let mode_file = tmp.path().join("proxy_mode.txt");
        let steer_log = tmp.path().join("steer_req.jsonl");
        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
mode="$(cat "{mode_file}")"
read -r _init
printf '{{"id":1,"result":{{"userAgent":"codex-cli/0.165.0"}}}}\n'
read -r _initialized
read -r req
printf '%s\n' "$req" >> "{steer_log}"
if [[ "$mode" == "disconnect_after_read" ]]; then
  exit 1
elif [[ "$mode" == "ok" ]]; then
  printf '{{"id":2,"result":{{"turnId":"turn-live-100"}}}}\n'
fi
"#,
            mode_file = mode_file.display(),
            steer_log = steer_log.display(),
        );
        let exe = write_executable_script(tmp.path(), "fake-codex-steer.sh", &script);
        let client = NativeClient::new(NativeConfig {
            executable: exe,
            socket: None,
        })
        .expect("client");

        // 1. Missing `expectedTurnId` is rejected without touching the wire.
        let missing_turn = client
            .steer_active_turn(
                &wakeup_store,
                "bnd_busy_1",
                "0196ed00-0000-7000-8000-000000000010",
                None,
            )
            .await
            .expect_err("missing expectedTurnId must be rejected");
        assert_eq!(missing_turn.code(), "missing_live_turn_id");
        assert!(!steer_log.exists());

        // 2. Ambiguous failure after writing `turn/steer` to the wire marks the wakeup `Unknown`.
        fs::write(&mode_file, "disconnect_after_read").expect("set mode");
        let ambig_err = client
            .steer_active_turn(
                &wakeup_store,
                "bnd_busy_1",
                "0196ed00-0000-7000-8000-000000000010",
                Some("turn-live-100"),
            )
            .await
            .expect_err("disconnect after write must fail");
        assert_eq!(ambig_err.code(), "ambiguous_transport_failure");

        let unknown_rec = wakeup_store
            .get("bnd_busy_1")
            .expect("get wakeup")
            .expect("wakeup record exists");
        assert_eq!(unknown_rec.status, NativeWakeupStatus::Unknown);
        assert_eq!(
            unknown_rec.verification_gate,
            NativeVerificationStatus::BlockedAmbiguousWakeup
        );

        // 3. Blind resend while `Unknown` is blocked!
        fs::write(&mode_file, "ok").expect("set mode ok");
        let resend_err = client
            .steer_active_turn(
                &wakeup_store,
                "bnd_busy_1",
                "0196ed00-0000-7000-8000-000000000010",
                Some("turn-live-100"),
            )
            .await
            .expect_err("blind resend of unknown wakeup must be refused");
        assert_eq!(resend_err.code(), "ambiguous_wakeup_pending_reconciliation");

        // 4. Reconciling without `allow_unknown = true` is refused; with `allow_unknown = true` it re-arms to `Pending`.
        let disallow_err = wakeup_store
            .reconcile_for_retry("bnd_busy_1", &unknown_rec.attempt_id, false)
            .expect_err("allow_unknown=false must fail");
        assert_eq!(
            disallow_err.code(),
            "ambiguous_wakeup_pending_reconciliation"
        );

        let rearmed = wakeup_store
            .reconcile_for_retry("bnd_busy_1", &unknown_rec.attempt_id, true)
            .expect("reconcile with allow_unknown=true");
        assert_eq!(rearmed.status, NativeWakeupStatus::Pending);

        // 5. Retry now succeeds and transitions to `Submitted` (not `Received` until MCP pull/ack).
        let submitted = client
            .steer_active_turn(
                &wakeup_store,
                "bnd_busy_1",
                "0196ed00-0000-7000-8000-000000000010",
                Some("turn-live-100"),
            )
            .await
            .expect("steer_active_turn succeeds");
        assert_eq!(submitted.status, NativeWakeupStatus::Submitted);
        assert_eq!(submitted.fixed_message, FIXED_WAKEUP_MESSAGE);

        let logged_lines: Vec<Value> = fs::read_to_string(&steer_log)
            .expect("read steer log")
            .lines()
            .map(|l| serde_json::from_str(l).expect("parse steer json"))
            .collect();
        let last_req = logged_lines.last().expect("last steer request");
        assert_eq!(last_req["method"], "turn/steer");
        assert_eq!(
            last_req["params"]["threadId"],
            "0196ed00-0000-7000-8000-000000000010"
        );
        assert_eq!(last_req["params"]["expectedTurnId"], "turn-live-100");
        assert_eq!(last_req["params"]["input"][0]["text"], FIXED_WAKEUP_MESSAGE);
        assert!(last_req["params"].get("model").is_none());
        assert!(last_req["params"].get("effort").is_none());

        // 6. Explicit MCP receipt transitions `Submitted` -> `Received`.
        let received = wakeup_store
            .mark_received("bnd_busy_1")
            .expect("mark_received");
        assert_eq!(received.status, NativeWakeupStatus::Received);
    }
}
