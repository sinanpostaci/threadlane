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

/// Sealed approved-task MCP bridge parameters, response envelopes, and host/authority helpers.
pub mod approved_mcp;
/// Public `codex app-server proxy` client and native thread metadata types.
pub mod client;
/// Structured error codes and redacted error types.
pub mod error;
/// Path validation, private `0700`/`0600` persistence, cross-process locking, and sanitization.
pub mod fs_safety;
/// Validated operator launcher policy, CLI flags, and initial fresh-only host auto-start coordinator.
pub mod host_launcher;
/// Paired MCP stdio server delegating to [`threadlane::ThreadlaneMcpServer`] after enrollment.
pub mod mcp_pair;
/// Private operator connection-epoch pairing store and verification gate types.
pub mod pairing;
/// Bounded newline-delimited JSON-RPC proxy session and Unix process-group cleanup.
pub mod rpc;
/// Durable `Pending`/`Submitted`/`Unknown`/`Received` busy-turn wake-up store and `turn/steer` builder.
pub mod wakeup;

pub use approved_mcp::{
    open_host_stores, open_host_stores_with_known_dirs, ApprovalSummaryPage,
    ApprovedAckEventsParams, ApprovedActivityPage, ApprovedActivityParams, ApprovedEventAck,
    ApprovedEventBatch, ApprovedPullEventsParams, ApprovedSessionParams, ApprovedTaskParams,
    CancelApprovedTaskParams, HostStoreHandles, ListApprovalsParams, StartApprovedTaskParams,
    DEFAULT_APPROVAL_LIST_LIMIT, DEFAULT_APPROVED_ACTIVITY_LIMIT,
    DEFAULT_APPROVED_EVENT_BATCH_LIMIT, HOST_NOT_CONFIGURED_MESSAGE, MAX_APPROVAL_CURSOR_LEN,
    MAX_APPROVAL_LIST_LIMIT, MAX_APPROVED_ACTIVITY_LIMIT, MAX_APPROVED_CANCEL_REASON_LEN,
    MAX_APPROVED_EVENT_BATCH_LIMIT,
};
pub use client::{
    NativeClient, NativeConfig, NativeDiagnosticReport, NativeMcpServerStatus,
    NativeMcpToolCallResult, NativeThread, NativeThreadPage,
};
pub use error::{NativeError, NativeErrorCode};
pub use host_launcher::{
    validate_host_executable_path, ApprovedHostLauncherPolicy, HostLauncherPolicy, NativeMcpCli,
    APPROVED_HOST_STARTUP_POLL_INTERVAL, APPROVED_HOST_STARTUP_TIMEOUT,
    DEFAULT_HOST_EXECUTABLE_NAME,
};
pub use mcp_pair::{
    open_workflow_stores, CatalogParams, ConnectionIdentityParams, DraftTaskParams, GetPlanParams,
    ListPlansParams, PairedMcpServer, PlanTaskParams, PublishParams, PublishPlanParams,
    SaveDraftParams, SavePlanDraftParams, WorkflowStoreHandles, WorkflowTaskParams,
};
pub use pairing::{
    ConnectionEpochState, ConnectionIdentity, EnrollmentRecord, NativeVerificationStatus,
    OperatorEnrollRequest, PairingStore, CONNECTION_IDENTITY_TOOL, MCP_SERVER_NAME,
};
pub use rpc::{
    build_proxy_args, is_process_alive, is_process_group_alive, InitializeResult, ProxyCallError,
    DEFAULT_PROXY_TIMEOUT, MAX_RPC_FRAME_BYTES, MAX_RPC_STDERR_BYTES, MAX_RPC_TOTAL_STDOUT_BYTES,
};
pub use threadlane_authority::{
    ApprovalRecord, ApprovalSummary, AuthorityError, AuthorityStore, MaterializedPlan,
    OperatorApprovalRequest,
};
pub use threadlane_host::{
    ApprovedHostClient, HostError, HostExecutionGates, HostPhase, HostSessionManifest,
    HostSessionSummary, TaskExecutionStatus, TaskRunReceipt, TaskStateSnapshot,
    APPROVED_EVENT_BATCH_MAX_BYTES, APPROVED_EVENT_BATCH_MAX_EVENTS,
};
pub use threadlane_workflow::{
    CatalogAssignment, CatalogPage, CatalogProfile, CatalogRoleAssignment, DeliveryReceipt,
    Difficulty, PlanHeader, PlanPage, PlanStatus, PlanStore, PlanSummary, PlanSummaryPage,
    PlanTask, PublicTaskRoute, PublishedPlanView, RegistryStore, SavePlanDraft, ScopedWorkflow,
    TaskExecutionRoute, TaskRoute, ToolAdapter, WorkflowError, WorkflowProfile, WorkflowRole,
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
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let path = dir.join(name);
        let staging = dir.join(format!(".{name}.tmp"));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o700)
            .open(&staging)
            .expect("create staged fake script");
        file.write_all(body.as_bytes()).expect("write fake script");
        file.sync_all().expect("sync fake script");
        drop(file);
        fs::rename(staging, &path).expect("publish closed fake executable");
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

    #[tokio::test]
    async fn paired_mcp_list_activity_enforces_unbound_lockout_same_binding_scope_and_readonly_cursors(
    ) {
        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");

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
            .create_binding("0196ed00-0000-7000-8000-00000000010a", "Chat A")
            .await
            .expect("create binding A");
        let binding_b = sup_client
            .create_binding("0196ed00-0000-7000-8000-00000000010b", "Chat B")
            .await
            .expect("create binding B");

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");

        let mcp_a = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create mcp A");
        let mcp_b = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create mcp B");

        // Verify capability count: `connection_identity` + 15 delegated runtime tools + 5 workflow tools + 8 approved-task tools = 29 tools.
        let tools = mcp_a.list_tools();
        assert_eq!(tools.len(), 29, "PairedMcpServer must expose 29 tools");
        let list_activity_tool = tools
            .iter()
            .find(|tool| tool.name == "list_activity")
            .expect("list_activity tool registered in PairedMcpServer router");
        let annotations = list_activity_tool
            .annotations
            .as_ref()
            .expect("list_activity annotations");
        assert_eq!(annotations.read_only_hint, Some(true));
        assert_eq!(annotations.destructive_hint, Some(false));
        assert_eq!(annotations.idempotent_hint, Some(true));
        assert_eq!(annotations.open_world_hint, Some(false));

        // 1. Unbound `list_activity` is blocked even if untrusted `_meta` supplies a binding/thread ID.
        let unbound_err = mcp_a
            .invoke_tool_json(
                "list_activity",
                json!({}),
                Some(json!({
                    "threadId": binding_a.thread_uuid,
                    "binding_id": binding_a.binding_id
                })),
            )
            .await
            .expect_err("unbound list_activity must be blocked");
        assert_eq!(
            unbound_err.structured_content.unwrap()["error"]["code"],
            "unbound_connection"
        );

        // 2. Enroll Connection A -> Chat A and Connection B -> Chat B.
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp_a.connection_nonce().to_string(),
                    epoch_id: mcp_a.epoch_id().to_string(),
                    binding_id: binding_a.binding_id.clone(),
                    thread_id: binding_a.thread_uuid.clone(),
                    project_id: Some("proj-history".to_string()),
                },
            )
            .expect("enroll chat A");
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp_b.connection_nonce().to_string(),
                    epoch_id: mcp_b.epoch_id().to_string(),
                    binding_id: binding_b.binding_id.clone(),
                    thread_id: binding_b.thread_uuid.clone(),
                    project_id: Some("proj-history".to_string()),
                },
            )
            .expect("enroll chat B");

        // 3. Start a demo run on Chat A to generate durable activity events.
        let start_a = mcp_a
            .invoke_tool_json(
                "start_demo",
                json!({
                    "command_key": "cmd-history-a-1",
                    "instruction": "History test task for Chat A",
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

        // 4. Caller-supplied identity fields on `list_activity` are rejected with `rebind_rejected`.
        for forbidden_key in [
            "binding_id",
            "thread_id",
            "session_id",
            "connection_nonce",
            "epoch_id",
        ] {
            let mut bad_args = serde_json::Map::new();
            bad_args.insert(
                forbidden_key.to_string(),
                Value::String(binding_a.binding_id.clone()),
            );
            let smuggle_err = mcp_b
                .invoke_tool_json("list_activity", Value::Object(bad_args), None)
                .await
                .expect_err("caller identity argument must be rejected");
            assert_eq!(
                smuggle_err.structured_content.unwrap()["error"]["code"],
                "rebind_rejected",
                "forbidden key {forbidden_key} must trigger rebind_rejected"
            );
        }

        // 5. Authorized same-binding `list_activity` on Chat A returns Chat A's activity page
        //    without advancing `delivered_seq` or `acked_seq`.
        let page_a = mcp_a
            .invoke_tool_json(
                "list_activity",
                json!({
                    "run_id": run_a_id,
                    "kind": "run_started",
                    "limit": 10
                }),
                None,
            )
            .await
            .expect("list_activity on Chat A")
            .structured_content
            .expect("structured list_activity");
        assert_eq!(page_a["tool"], "list_activity");
        assert_eq!(page_a["binding_id"], binding_a.binding_id);
        assert_eq!(page_a["data"]["binding_id"], binding_a.binding_id);
        let events_a = page_a["data"]["events"].as_array().expect("events array");
        assert_eq!(events_a.len(), 1);
        assert_eq!(events_a[0]["run_id"], run_a_id);
        assert_eq!(events_a[0]["kind"], "run_started");
        assert!(page_a["data"]["high_water_mark_seq"].as_u64().unwrap_or(0) >= 1);

        let status_a = mcp_a
            .invoke_tool_json("status", json!({}), None)
            .await
            .expect("status on Chat A")
            .structured_content
            .expect("structured status");
        assert_eq!(status_a["data"]["binding"]["delivered_seq"], 0);
        assert_eq!(status_a["data"]["binding"]["acked_seq"], 0);

        // 6. Cross-binding isolation: Chat B calling `list_activity` with Chat A's `run_id`
        //    only queries Chat B's binding and sees zero events from Chat A.
        let page_b = mcp_b
            .invoke_tool_json(
                "list_activity",
                json!({
                    "run_id": run_a_id,
                    "limit": 10
                }),
                Some(json!({
                    "threadId": binding_a.thread_uuid,
                    "binding_id": binding_a.binding_id
                })),
            )
            .await
            .expect("list_activity on Chat B")
            .structured_content
            .expect("structured list_activity on Chat B");
        assert_eq!(page_b["binding_id"], binding_b.binding_id);
        assert_eq!(page_b["data"]["binding_id"], binding_b.binding_id);
        assert_eq!(
            page_b["data"]["events"].as_array().expect("events").len(),
            0,
            "Chat B must never see Chat A's activity events"
        );

        sup_handle.shutdown().await;
    }

    fn workflow_sample_tool(
        id: &str,
        adapter: ToolAdapter,
        enabled: bool,
    ) -> threadlane_workflow::ToolDefinition {
        threadlane_workflow::ToolDefinition {
            id: id.to_string(),
            label: format!("Tool {id}"),
            adapter,
            executable_path: if adapter.is_cli_adapter() {
                Some(PathBuf::from(format!("/opt/secret-cli-paths/{id}-bin")))
            } else {
                None
            },
            enabled,
        }
    }

    fn workflow_sample_model(
        id: &str,
        tool_id: &str,
        model_arg: &str,
        efforts: &[&str],
        allow_no_effort: bool,
    ) -> threadlane_workflow::ModelDefinition {
        threadlane_workflow::ModelDefinition {
            id: id.to_string(),
            tool_id: tool_id.to_string(),
            label: format!("Model {id}"),
            model_arg: model_arg.to_string(),
            efforts: efforts.iter().map(|s| (*s).to_string()).collect(),
            allow_no_effort,
        }
    }

    fn setup_workflow_stores(
        tmp: &TempDir,
    ) -> (
        std::sync::Arc<RegistryStore>,
        std::sync::Arc<PlanStore>,
        u64,
    ) {
        use std::sync::Arc;
        use threadlane_workflow::{ModelChoice, RegistryChange, RoleAssignment};

        let reg_dir = tmp.path().join("workflow-registry");
        let plans_dir = tmp.path().join("workflow-plans");

        let (registry, plans) = open_workflow_stores(Some(&reg_dir), Some(&plans_dir))
            .expect("open workflow stores")
            .expect("stores configured");

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: workflow_sample_tool("agy", ToolAdapter::Antigravity, true),
                },
            )
            .expect("upsert agy");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertTool {
                    tool: workflow_sample_tool("opencode", ToolAdapter::OpencodeV2, true),
                },
            )
            .expect("upsert opencode");
        let s3 = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertTool {
                    tool: workflow_sample_tool("codex", ToolAdapter::CodexCli, true),
                },
            )
            .expect("upsert codex");
        let s4 = registry
            .apply(
                s3.revision,
                RegistryChange::UpsertModels {
                    models: vec![
                        workflow_sample_model(
                            "argon-high",
                            "agy",
                            "gemini-4-argon-high",
                            &["medium", "high"],
                            false,
                        ),
                        workflow_sample_model(
                            "deepseek-flash",
                            "opencode",
                            "opencode-go/deepseek-v4.1-flash",
                            &["max"],
                            false,
                        ),
                        workflow_sample_model(
                            "astra-max",
                            "codex",
                            "gpt-6-astra",
                            &["high", "max"],
                            false,
                        ),
                    ],
                },
            )
            .expect("upsert models");

        let profile = WorkflowProfile {
            id: "default-profile".to_string(),
            name: "Default M4 Workflow Profile".to_string(),
            assignments: vec![
                RoleAssignment {
                    role: WorkflowRole::Follower,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "astra-max".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Easy),
                    choice: ModelChoice {
                        model_id: "deepseek-flash".to_string(),
                        effort: Some("max".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Coder,
                    difficulty: Some(Difficulty::Hard),
                    choice: ModelChoice {
                        model_id: "argon-high".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
                RoleAssignment {
                    role: WorkflowRole::Reviewer,
                    difficulty: None,
                    choice: ModelChoice {
                        model_id: "argon-high".to_string(),
                        effort: Some("high".to_string()),
                    },
                },
            ],
        };

        let s5 = registry
            .apply(s4.revision, RegistryChange::UpsertProfile { profile })
            .expect("upsert profile");
        let s6 = registry
            .apply(
                s5.revision,
                RegistryChange::SetDefaultProfile {
                    profile_id: Some("default-profile".to_string()),
                },
            )
            .expect("set default profile");

        (Arc::clone(&registry), Arc::clone(&plans), s6.revision)
    }

    #[tokio::test]
    async fn paired_mcp_workflow_registered_tools_and_strict_json_schemas() {
        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");
        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let mcp = PairedMcpServer::new(&state_dir, pairing_store).expect("create mcp");

        let tools = mcp.list_tools();
        assert_eq!(tools.len(), 29);

        for (tool_name, expected_read_only, expected_props) in [
            ("catalog", true, &["cursor", "limit"][..]),
            ("list_plans", true, &["cursor", "limit"][..]),
            (
                "get_plan",
                true,
                &["plan_id", "revision", "task_cursor", "task_limit"][..],
            ),
            (
                "save_draft",
                false,
                &[
                    "command_key",
                    "plan_id",
                    "expected_revision",
                    "profile_id",
                    "profile_revision",
                    "summary",
                    "tasks",
                ][..],
            ),
            ("publish", false, &["plan_id", "revision"][..]),
        ] {
            let tool = tools
                .iter()
                .find(|t| t.name == tool_name)
                .unwrap_or_else(|| panic!("missing workflow tool `{tool_name}`"));
            let ann = tool.annotations.as_ref().expect("tool annotations");
            assert_eq!(ann.read_only_hint, Some(expected_read_only));
            assert_eq!(ann.destructive_hint, Some(false));
            assert_eq!(ann.idempotent_hint, Some(true));
            assert_eq!(ann.open_world_hint, Some(false));

            let schema_val = serde_json::to_value(&tool.input_schema).expect("schema json");
            let schema_str = serde_json::to_string(&schema_val).expect("schema string");
            let props = schema_val["properties"]
                .as_object()
                .unwrap_or_else(|| panic!("{tool_name} schema must have properties object"));
            for &prop in expected_props {
                assert!(
                    props.contains_key(prop),
                    "{tool_name} schema missing expected property `{prop}`"
                );
            }
            for forbidden in [
                "binding_id",
                "thread_id",
                "session_id",
                "connection_nonce",
                "epoch_id",
                "registry_dir",
                "plans_dir",
                "executable",
                "executable_path",
                "approved",
                "operator_approved",
            ] {
                assert!(
                    !props.contains_key(forbidden),
                    "{tool_name} schema must not expose `{forbidden}`"
                );
                assert!(
                    !schema_str.contains(&format!("\"{forbidden}\"")),
                    "{tool_name} schema definition must not mention `{forbidden}`"
                );
            }
            assert_eq!(
                schema_val["additionalProperties"],
                Value::Bool(false),
                "{tool_name} schema must enforce deny_unknown_fields"
            );

            if tool_name == "save_draft" {
                for task_prop in [
                    "id",
                    "title",
                    "role",
                    "difficulty",
                    "instruction",
                    "depends_on",
                    "allowed_write_paths",
                ] {
                    assert!(
                        schema_str.contains(&format!("\"{task_prop}\"")),
                        "save_draft task schema must include `{task_prop}`"
                    );
                }
                for enum_val in [
                    "follower",
                    "orchestrator",
                    "architect",
                    "coder",
                    "reviewer",
                    "closure",
                    "easy",
                    "medium",
                    "hard",
                    "extreme",
                ] {
                    assert!(
                        schema_str.contains(&format!("\"{enum_val}\"")),
                        "save_draft schema must include enum value `{enum_val}`"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn paired_mcp_workflow_unbound_expired_quarantined_and_not_configured_guards() {
        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");
        let (registry, plans, reg_rev) = setup_workflow_stores(&tmp);

        // Verify bootstrap store opening requires both --registry-dir and --plans-dir together.
        assert!(open_workflow_stores(None, None)
            .expect("none stores")
            .is_none());
        let only_reg_err = open_workflow_stores(Some(registry.config_dir()), None)
            .expect_err("registry-dir without plans-dir must fail");
        assert_eq!(only_reg_err.code(), "invalid_config");
        let only_plans_err = open_workflow_stores(None, Some(plans.dir()))
            .expect_err("plans-dir without registry-dir must fail");
        assert_eq!(only_plans_err.code(), "invalid_config");

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");

        let mcp_base = PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("create mcp");
        let orig_nonce = mcp_base.connection_nonce().to_string();
        let orig_epoch = mcp_base.epoch_id().to_string();
        assert!(!mcp_base.has_workflow_stores());

        let mut mcp = mcp_base.with_workflow_stores(
            std::sync::Arc::clone(&registry),
            std::sync::Arc::clone(&plans),
        );
        assert!(mcp.has_workflow_stores());
        assert_eq!(mcp.connection_nonce(), orig_nonce);
        assert_eq!(mcp.epoch_id(), orig_epoch);

        let spoofed_meta = Some(json!({
            "threadId": "0196ed00-0000-7000-8000-000000000001",
            "binding_id": "bnd_spoofed"
        }));

        // 1. Unbound server rejects all 5 workflow tools before accessing registry/plan data,
        //    ignoring `_meta`.
        for (tool, args) in [
            ("catalog", json!({})),
            ("list_plans", json!({})),
            ("get_plan", json!({ "plan_id": "plan-1" })),
            (
                "save_draft",
                json!({
                    "command_key": "cmd-1",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Unbound attempt",
                    "tasks": [{
                        "id": "t1",
                        "title": "Task 1",
                        "role": "coder",
                        "difficulty": "easy",
                        "instruction": "Should not be saved",
                        "depends_on": [],
                        "allowed_write_paths": ["src/lib.rs"]
                    }]
                }),
            ),
            ("publish", json!({ "plan_id": "plan-1", "revision": 1 })),
        ] {
            let err = mcp
                .invoke_tool_json(tool, args, spoofed_meta.clone())
                .await
                .expect_err("unbound workflow call must fail");
            let err_json = err.structured_content.expect("structured error");
            assert_eq!(err_json["error"]["code"], "unbound_connection");
            let serialized = serde_json::to_string(&err_json).expect("serialize err");
            assert!(!serialized.contains("default-profile"));
            assert!(!serialized.contains("gemini-4-argon-high"));
            assert!(!serialized.contains("/opt/secret-cli-paths"));
        }
        assert!(plans.snapshot().expect("plans snapshot").is_empty());

        // 2. Enrolled server WITHOUT workflow stores fails honestly with `workflow_not_configured`.
        let mcp_unconfigured =
            PairedMcpServer::new(&state_dir, pairing_store.clone()).expect("unconfigured mcp");
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp_unconfigured.connection_nonce().to_string(),
                    epoch_id: mcp_unconfigured.epoch_id().to_string(),
                    binding_id: "bnd_unconfigured".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000090".to_string(),
                    project_id: Some("proj-wf".to_string()),
                },
            )
            .expect("enroll unconfigured");

        for (tool, args) in [
            ("catalog", json!({})),
            ("list_plans", json!({})),
            ("get_plan", json!({ "plan_id": "plan-1" })),
            (
                "save_draft",
                json!({
                    "command_key": "cmd-1",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Unconfigured attempt",
                    "tasks": [{
                        "id": "t1",
                        "title": "Task 1",
                        "role": "coder",
                        "difficulty": "easy",
                        "instruction": "Should fail not configured",
                        "depends_on": [],
                        "allowed_write_paths": ["src/lib.rs"]
                    }]
                }),
            ),
            ("publish", json!({ "plan_id": "plan-1", "revision": 1 })),
        ] {
            let err = mcp_unconfigured
                .invoke_tool_json(tool, args, None)
                .await
                .expect_err("unconfigured workflow tool must fail");
            let err_json = err.structured_content.expect("structured error");
            assert_eq!(err_json["binding_id"], "bnd_unconfigured");
            assert_eq!(err_json["tool"], tool);
            assert_eq!(err_json["error"]["code"], "workflow_not_configured");
        }

        // 3. Enroll `mcp`, verify `catalog` works, then restart epoch and expire/quarantine connections.
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp.connection_nonce().to_string(),
                    epoch_id: mcp.epoch_id().to_string(),
                    binding_id: "bnd_active".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000091".to_string(),
                    project_id: Some("proj-wf".to_string()),
                },
            )
            .expect("enroll active");

        let ok_cat = mcp
            .invoke_tool_json("catalog", json!({}), None)
            .await
            .expect("enrolled catalog succeeds");
        assert_eq!(
            ok_cat.structured_content.unwrap()["binding_id"],
            "bnd_active"
        );

        // Restarting the connection expires the prior epoch and returns `mcp` to `Unbound`.
        let old_nonce = mcp.connection_nonce().to_string();
        let old_epoch = mcp.epoch_id().to_string();
        mcp.restart_epoch().expect("restart epoch");
        assert_eq!(
            pairing_store
                .verify_enrolled_binding(&old_nonce, &old_epoch)
                .unwrap_err()
                .code(),
            "expired_epoch"
        );
        let post_restart_err = mcp
            .invoke_tool_json("catalog", json!({}), None)
            .await
            .expect_err("restarted connection must not inherit enrollment");
        assert_eq!(
            post_restart_err.structured_content.unwrap()["error"]["code"],
            "unbound_connection"
        );

        // Explicitly expiring an enrolled connection rejects all workflow tools with `expired_epoch`.
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp.connection_nonce().to_string(),
                    epoch_id: mcp.epoch_id().to_string(),
                    binding_id: "bnd_active".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000091".to_string(),
                    project_id: Some("proj-wf".to_string()),
                },
            )
            .expect("re-enroll");
        pairing_store
            .expire_epoch(&operator_token, mcp.epoch_id())
            .expect("expire epoch");
        let expired_err = mcp
            .invoke_tool_json("list_plans", json!({}), None)
            .await
            .expect_err("expired connection must fail");
        assert_eq!(
            expired_err.structured_content.unwrap()["error"]["code"],
            "expired_epoch"
        );

        // Shared connection across two threads is quarantined and rejects workflow tools.
        let shared_mcp = PairedMcpServer::new(&state_dir, pairing_store.clone())
            .expect("shared mcp")
            .with_workflow_stores(registry, plans);
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: shared_mcp.connection_nonce().to_string(),
                    epoch_id: shared_mcp.epoch_id().to_string(),
                    binding_id: "bnd_shared_1".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-000000000092".to_string(),
                    project_id: Some("proj-wf".to_string()),
                },
            )
            .expect("enroll thread 1");
        let _ = pairing_store.enroll(
            &operator_token,
            OperatorEnrollRequest {
                connection_nonce: shared_mcp.connection_nonce().to_string(),
                epoch_id: shared_mcp.epoch_id().to_string(),
                binding_id: "bnd_shared_2".to_string(),
                thread_id: "0196ed00-0000-7000-8000-000000000093".to_string(),
                project_id: Some("proj-wf".to_string()),
            },
        );
        for tool in ["catalog", "list_plans", "get_plan", "save_draft", "publish"] {
            let q_err = shared_mcp
                .invoke_tool_json(tool, json!({}), None)
                .await
                .expect_err("quarantined connection must reject workflow tool");
            assert_eq!(
                q_err.structured_content.unwrap()["error"]["code"],
                "shared_connection_rejected"
            );
        }
    }

    #[tokio::test]
    async fn paired_mcp_workflow_catalog_draft_retry_publish_and_disabled_tool_honesty() {
        use rmcp::handler::server::wrapper::Parameters;
        use threadlane_workflow::RegistryChange;

        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");
        let (registry, plans, reg_rev) = setup_workflow_stores(&tmp);

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");

        let mcp = PairedMcpServer::new(&state_dir, pairing_store.clone())
            .expect("create mcp")
            .with_workflow_stores(
                std::sync::Arc::clone(&registry),
                std::sync::Arc::clone(&plans),
            );

        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp.connection_nonce().to_string(),
                    epoch_id: mcp.epoch_id().to_string(),
                    binding_id: "bnd_workflow_a".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-0000000000a1".to_string(),
                    project_id: Some("proj-wf".to_string()),
                },
            )
            .expect("enroll workflow A");

        // 1. Enrolled `catalog` returns safe full model/effort assignments without executable paths or tokens.
        let cat_res = mcp
            .invoke_tool_json("catalog", json!({ "limit": 10 }), None)
            .await
            .expect("catalog call");
        let cat_raw = serde_json::to_string(&cat_res).expect("serialize catalog result");
        assert!(!cat_raw.contains("/opt/secret-cli-paths"));
        assert!(!cat_raw.contains("executable_path"));
        assert!(!cat_raw.contains(&operator_token));

        let cat_env = cat_res.structured_content.expect("catalog structured");
        assert_eq!(cat_env["untrusted_data"], true);
        assert_eq!(cat_env["demo_only"], false);
        assert_eq!(cat_env["binding_id"], "bnd_workflow_a");
        assert_eq!(cat_env["tool"], "catalog");

        let cat_page: CatalogPage =
            serde_json::from_value(cat_env["data"].clone()).expect("decode CatalogPage");
        assert_eq!(cat_page.registry_revision, reg_rev);
        assert_eq!(
            cat_page.default_profile_id.as_deref(),
            Some("default-profile")
        );
        assert_eq!(cat_page.profiles.len(), 1);
        let prof = &cat_page.profiles[0];
        assert!(prof.is_default);
        assert!(prof.available);
        assert_eq!(prof.assignments.len(), 4);
        let hard_coder = prof
            .assignments
            .iter()
            .find(|a| a.role == WorkflowRole::Coder && a.difficulty == Some(Difficulty::Hard))
            .expect("hard coder assignment");
        assert_eq!(hard_coder.model_id, "argon-high");
        assert_eq!(hard_coder.model_arg, "gemini-4-argon-high");
        assert_eq!(hard_coder.effort.as_deref(), Some("high"));
        assert_eq!(hard_coder.adapter, ToolAdapter::Antigravity);
        assert_eq!(hard_coder.route, TaskExecutionRoute::HeadlessWorker);
        assert!(hard_coder.tool_enabled);
        assert!(hard_coder.available);

        // 2. `save_draft` creates rev 1 and supports idempotent retry without creating rev 2.
        let draft_args = json!({
            "command_key": "cmd-save-1",
            "expected_revision": 0,
            "profile_id": "default-profile",
            "profile_revision": reg_rev,
            "summary": "Initial M4 workflow plan",
            "tasks": [
                {
                    "id": "task-easy",
                    "title": "Easy slice",
                    "role": "coder",
                    "difficulty": "easy",
                    "instruction": "Implement easy helper",
                    "depends_on": [],
                    "allowed_write_paths": ["crates/threadlane-native/src/error.rs"]
                },
                {
                    "id": "task-hard",
                    "title": "Hard slice",
                    "role": "coder",
                    "difficulty": "hard",
                    "instruction": "Implement paired MCP workflow handler",
                    "depends_on": ["task-easy"],
                    "allowed_write_paths": ["crates/threadlane-native/src/mcp_pair.rs"]
                }
            ]
        });

        let saved_env = mcp
            .invoke_tool_json("save_draft", draft_args.clone(), None)
            .await
            .expect("save_draft rev 1")
            .structured_content
            .expect("structured save_draft");
        assert_eq!(saved_env["binding_id"], "bnd_workflow_a");
        assert_eq!(saved_env["tool"], "save_draft");
        let saved_summary: PlanSummary =
            serde_json::from_value(saved_env["data"].clone()).expect("decode PlanSummary");
        assert_eq!(saved_summary.binding_id, "bnd_workflow_a");
        assert_eq!(saved_summary.revision, 1);
        assert_eq!(saved_summary.status, PlanStatus::Draft);
        assert_eq!(saved_summary.task_count, 2);
        assert!(saved_summary.plan_digest.is_some());

        // Exact retry at expected_revision 0 and expected_revision 1 returns rev 1 idempotently.
        let retry_0: PlanSummary = serde_json::from_value(
            mcp.invoke_tool_json("save_draft", draft_args.clone(), None)
                .await
                .expect("retry at expected_revision 0")
                .structured_content
                .unwrap()["data"]
                .clone(),
        )
        .expect("decode retry 0");
        assert_eq!(retry_0, saved_summary);
        assert_eq!(
            plans
                .plan_revisions(&saved_summary.plan_id)
                .expect("revs")
                .len(),
            1
        );

        let mut retry_1_args = draft_args.clone();
        retry_1_args["expected_revision"] = json!(1);
        let retry_1: PlanSummary = serde_json::from_value(
            mcp.invoke_tool_json("save_draft", retry_1_args, None)
                .await
                .expect("retry at expected_revision 1")
                .structured_content
                .unwrap()["data"]
                .clone(),
        )
        .expect("decode retry 1");
        assert_eq!(retry_1, saved_summary);

        // Reusing `cmd-save-1` with a different summary fails with `idempotency_conflict`.
        let mut conflict_args = draft_args.clone();
        conflict_args["summary"] = json!("Mutated summary with same command_key");
        let conflict_err = mcp
            .invoke_tool_json("save_draft", conflict_args, None)
            .await
            .expect_err("reused command_key with different payload must fail");
        assert_eq!(
            conflict_err.structured_content.unwrap()["error"]["code"],
            "idempotency_conflict"
        );

        // 3. `publish` transitions Draft -> Published (never Received or Applied).
        //    Also exercise direct `#[tool]` handler method on `PairedMcpServer`.
        let pub_res = mcp
            .publish(Parameters(PublishParams {
                plan_id: saved_summary.plan_id.clone(),
                revision: 1,
            }))
            .await
            .expect("publish rev 1");
        let pub_raw = serde_json::to_string(&pub_res).expect("serialize publish");
        assert!(!pub_raw.contains("/opt/secret-cli-paths"));
        assert!(!pub_raw.contains("executable_path"));
        assert!(!pub_raw.contains("Implement paired MCP workflow handler"));

        let pub_view: PublishedPlanView = serde_json::from_value(
            pub_res.structured_content.expect("structured publish")["data"].clone(),
        )
        .expect("decode PublishedPlanView");
        assert_eq!(pub_view.header.status, PlanStatus::Published);
        assert_eq!(pub_view.receipt.state, PlanStatus::Published);
        assert_eq!(pub_view.header.plan_digest, saved_summary.plan_digest);
        assert_eq!(pub_view.tasks.len(), 2);
        assert_eq!(pub_view.tasks[0].model_id, "deepseek-flash");
        assert_eq!(pub_view.tasks[0].effort.as_deref(), Some("max"));
        assert_eq!(pub_view.tasks[1].model_id, "argon-high");
        assert_eq!(pub_view.tasks[1].effort.as_deref(), Some("high"));

        // `list_plans` and `get_plan` must NOT advance receipt to `received` or `applied`.
        let list_res = mcp
            .list_plans(Parameters(ListPlansParams::default()))
            .await
            .expect("list_plans");
        let list_page: PlanSummaryPage =
            serde_json::from_value(list_res.structured_content.unwrap()["data"].clone())
                .expect("decode PlanSummaryPage");
        assert_eq!(list_page.plans.len(), 1);
        assert_eq!(list_page.plans[0].status, PlanStatus::Published);

        let get_res = mcp
            .get_plan(Parameters(GetPlanParams {
                plan_id: saved_summary.plan_id.clone(),
                revision: Some(1),
                task_cursor: None,
                task_limit: None,
            }))
            .await
            .expect("get_plan");
        let plan_page: PlanPage =
            serde_json::from_value(get_res.structured_content.unwrap()["data"].clone())
                .expect("decode PlanPage");
        assert_eq!(plan_page.header.status, PlanStatus::Published);
        assert_eq!(plan_page.tasks.len(), 2);
        assert_eq!(
            plans
                .delivery_receipt(&saved_summary.plan_id, 1)
                .expect("receipt")
                .expect("exists")
                .state,
            PlanStatus::Published
        );

        // 4. Disabling `opencode` tool in `RegistryStore` is honestly reported in `catalog`
        //    (fresh snapshot per call, no silent caching of old registry revision) and blocks `save_draft`.
        let disabled_snap = registry
            .apply(
                reg_rev,
                RegistryChange::UpsertTool {
                    tool: workflow_sample_tool("opencode", ToolAdapter::OpencodeV2, false),
                },
            )
            .expect("disable opencode");

        let refreshed_cat: CatalogPage = serde_json::from_value(
            mcp.invoke_tool_json("catalog", json!({}), None)
                .await
                .expect("refreshed catalog")
                .structured_content
                .unwrap()["data"]
                .clone(),
        )
        .expect("decode refreshed catalog");
        assert_eq!(refreshed_cat.registry_revision, disabled_snap.revision);
        assert!(!refreshed_cat.profiles[0].available);
        let easy_assign = refreshed_cat.profiles[0]
            .assignments
            .iter()
            .find(|a| a.role == WorkflowRole::Coder && a.difficulty == Some(Difficulty::Easy))
            .expect("easy coder assignment");
        assert_eq!(easy_assign.model_id, "deepseek-flash");
        assert!(!easy_assign.tool_enabled);
        assert!(!easy_assign.available);

        let disabled_draft_err = mcp
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "cmd-disabled-tool",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": disabled_snap.revision,
                    "summary": "Plan referencing disabled opencode tool",
                    "tasks": [{
                        "id": "t-easy",
                        "title": "Uses disabled easy coder",
                        "role": "coder",
                        "difficulty": "easy",
                        "instruction": "Should fail resolution",
                        "depends_on": [],
                        "allowed_write_paths": ["src/lib.rs"]
                    }]
                }),
                None,
            )
            .await
            .expect_err("disabled tool must fail draft compilation");
        assert_eq!(
            disabled_draft_err.structured_content.unwrap()["error"]["code"],
            "registry_resolution_failed"
        );
    }

    #[tokio::test]
    async fn paired_mcp_workflow_two_chat_isolation_spoof_rejection_and_256kib_budget() {
        use threadlane_workflow::MAX_MCP_RESPONSE_BYTES;

        let tmp = TempDir::new().expect("tempdir");
        let state_dir = tmp.path().join("runtime-state");
        let pairing_dir = tmp.path().join("pairing-state");
        let (registry, plans, reg_rev) = setup_workflow_stores(&tmp);

        let pairing_store = PairingStore::open(&pairing_dir).expect("open pairing store");
        let operator_token = pairing_store.operator_token().expect("operator token");

        let mcp_a = PairedMcpServer::new(&state_dir, pairing_store.clone())
            .expect("mcp A")
            .with_workflow_stores(
                std::sync::Arc::clone(&registry),
                std::sync::Arc::clone(&plans),
            );
        let mcp_b = PairedMcpServer::new(&state_dir, pairing_store.clone())
            .expect("mcp B")
            .with_workflow_stores(
                std::sync::Arc::clone(&registry),
                std::sync::Arc::clone(&plans),
            );

        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp_a.connection_nonce().to_string(),
                    epoch_id: mcp_a.epoch_id().to_string(),
                    binding_id: "bnd_chat_alpha".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-0000000000b1".to_string(),
                    project_id: Some("proj-shared".to_string()),
                },
            )
            .expect("enroll alpha");
        pairing_store
            .enroll(
                &operator_token,
                OperatorEnrollRequest {
                    connection_nonce: mcp_b.connection_nonce().to_string(),
                    epoch_id: mcp_b.epoch_id().to_string(),
                    binding_id: "bnd_chat_beta".to_string(),
                    thread_id: "0196ed00-0000-7000-8000-0000000000b2".to_string(),
                    project_id: Some("proj-shared".to_string()),
                },
            )
            .expect("enroll beta");

        // 1. Both chats save a draft with `plan_id: None` and the same `command_key`.
        let shared_key_draft = json!({
            "command_key": "shared-cmd-key",
            "expected_revision": 0,
            "profile_id": "default-profile",
            "profile_revision": reg_rev,
            "summary": "Per-binding isolated plan",
            "tasks": [{
                "id": "t1",
                "title": "Task 1",
                "role": "coder",
                "difficulty": "hard",
                "instruction": "Isolated task instruction",
                "depends_on": [],
                "allowed_write_paths": ["crates/threadlane-native/src/lib.rs"]
            }]
        });

        let sum_a: PlanSummary = serde_json::from_value(
            mcp_a
                .invoke_tool_json("save_draft", shared_key_draft.clone(), None)
                .await
                .expect("save on alpha")
                .structured_content
                .unwrap()["data"]
                .clone(),
        )
        .expect("sum_a");
        let sum_b: PlanSummary = serde_json::from_value(
            mcp_b
                .invoke_tool_json(
                    "save_draft",
                    shared_key_draft.clone(),
                    Some(json!({
                        "binding_id": "bnd_chat_alpha",
                        "threadId": "0196ed00-0000-7000-8000-0000000000b1"
                    })),
                )
                .await
                .expect("save on beta ignoring _meta")
                .structured_content
                .unwrap()["data"]
                .clone(),
        )
        .expect("sum_b");
        assert_ne!(sum_a.plan_id, sum_b.plan_id);
        assert_eq!(sum_a.binding_id, "bnd_chat_alpha");
        assert_eq!(sum_b.binding_id, "bnd_chat_beta");

        // 2. Chat B cannot get, update, or publish Chat A's plan; receives metadata-safe `not_found`
        //    identical to a completely nonexistent plan.
        let baseline_missing_err = mcp_b
            .invoke_tool_json(
                "get_plan",
                json!({ "plan_id": "plan-does-not-exist" }),
                None,
            )
            .await
            .expect_err("missing plan on beta")
            .structured_content
            .expect("structured missing error");
        assert_eq!(baseline_missing_err["error"]["code"], "not_found");

        let cross_get_latest = mcp_b
            .invoke_tool_json(
                "get_plan",
                json!({ "plan_id": sum_a.plan_id }),
                Some(json!({ "binding_id": "bnd_chat_alpha" })),
            )
            .await
            .expect_err("cross get latest")
            .structured_content
            .unwrap();
        let cross_get_rev1 = mcp_b
            .invoke_tool_json(
                "get_plan",
                json!({ "plan_id": sum_a.plan_id, "revision": 1 }),
                None,
            )
            .await
            .expect_err("cross get rev 1")
            .structured_content
            .unwrap();
        let cross_save_rev0 = mcp_b
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "hijack-0",
                    "plan_id": sum_a.plan_id,
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Hijack rev 0",
                    "tasks": shared_key_draft["tasks"]
                }),
                None,
            )
            .await
            .expect_err("cross save rev 0")
            .structured_content
            .unwrap();
        let cross_save_rev1 = mcp_b
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "hijack-1",
                    "plan_id": sum_a.plan_id,
                    "expected_revision": 1,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Hijack rev 1",
                    "tasks": shared_key_draft["tasks"]
                }),
                None,
            )
            .await
            .expect_err("cross save rev 1")
            .structured_content
            .unwrap();
        let cross_publish = mcp_b
            .invoke_tool_json(
                "publish",
                json!({ "plan_id": sum_a.plan_id, "revision": 1 }),
                None,
            )
            .await
            .expect_err("cross publish")
            .structured_content
            .unwrap();

        for cross_err in [
            &cross_get_latest,
            &cross_get_rev1,
            &cross_save_rev0,
            &cross_save_rev1,
            &cross_publish,
        ] {
            assert_eq!(cross_err["error"], baseline_missing_err["error"]);
            let s = serde_json::to_string(cross_err).expect("serialize cross err");
            assert!(!s.contains("bnd_chat_alpha"));
            assert!(!s.contains(&sum_a.plan_id));
        }

        // 3. Caller-supplied spoofed identity, path, executable, and approval fields are denied.
        for rebind_key in [
            "binding_id",
            "thread_id",
            "session_id",
            "connection_nonce",
            "epoch_id",
        ] {
            let mut bad_top = serde_json::Map::new();
            bad_top.insert(rebind_key.to_string(), json!("bnd_chat_alpha"));
            let top_err = mcp_b
                .invoke_tool_json("list_plans", Value::Object(bad_top), None)
                .await
                .expect_err("top-level rebind key must fail");
            assert_eq!(
                top_err.structured_content.unwrap()["error"]["code"],
                "rebind_rejected"
            );

            let mut bad_task = shared_key_draft["tasks"][0].clone();
            bad_task
                .as_object_mut()
                .unwrap()
                .insert(rebind_key.to_string(), json!("bnd_chat_alpha"));
            let mut nested_draft = shared_key_draft.clone();
            nested_draft["tasks"] = json!([bad_task]);
            let nested_err = mcp_b
                .invoke_tool_json("save_draft", nested_draft, None)
                .await
                .expect_err("nested task rebind key must fail");
            assert_eq!(
                nested_err.structured_content.unwrap()["error"]["code"],
                "rebind_rejected"
            );
        }

        for unknown_key in [
            "registry_dir",
            "plans_dir",
            "state_dir",
            "pairing_dir",
            "executable",
            "executable_path",
            "approved",
            "operator_approved",
            "status",
            "receipt",
        ] {
            let mut bad_top = serde_json::Map::new();
            bad_top.insert(unknown_key.to_string(), json!("/tmp/evil"));
            let top_err = mcp_a
                .invoke_tool_json("catalog", Value::Object(bad_top), None)
                .await
                .expect_err("unknown top-level field must fail");
            assert_eq!(
                top_err.structured_content.unwrap()["error"]["code"],
                "validation_failed"
            );

            let mut bad_task = shared_key_draft["tasks"][0].clone();
            bad_task
                .as_object_mut()
                .unwrap()
                .insert(unknown_key.to_string(), json!("/tmp/evil"));
            let mut nested_draft = shared_key_draft.clone();
            nested_draft["command_key"] = json!(format!("bad-nested-{unknown_key}"));
            nested_draft["tasks"] = json!([bad_task]);
            let nested_err = mcp_a
                .invoke_tool_json("save_draft", nested_draft, None)
                .await
                .expect_err("unknown nested task field must fail");
            assert_eq!(
                nested_err.structured_content.unwrap()["error"]["code"],
                "validation_failed"
            );
        }

        for bad_write_path in [
            "/etc/nixos/configuration.nix",
            "../outside.rs",
            "AGENTS.md",
            ".git/config",
        ] {
            let mut bad_scope_draft = shared_key_draft.clone();
            bad_scope_draft["command_key"] = json!(format!("bad-scope-{}", bad_write_path.len()));
            bad_scope_draft["tasks"][0]["allowed_write_paths"] = json!([bad_write_path]);
            let scope_err = mcp_a
                .invoke_tool_json("save_draft", bad_scope_draft, None)
                .await
                .expect_err("disallowed write path must fail");
            assert_eq!(
                scope_err.structured_content.unwrap()["error"]["code"],
                "invalid_write_scope"
            );
        }

        // 4. 256 KiB wire budget: multi-page untruncated instructions, single-item overflow,
        //    and oversized publish rejection without mutating `PlanStore`.
        let large_tasks: Vec<Value> = (1..=5)
            .map(|idx| {
                json!({
                    "id": format!("big-task-{idx}"),
                    "title": format!("Big Task {idx}"),
                    "role": "coder",
                    "difficulty": "hard",
                    "instruction": format!("Instruction-{idx}:{}", "x".repeat(45_000)),
                    "depends_on": if idx == 1 { vec![] } else { vec![format!("big-task-{}", idx - 1)] },
                    "allowed_write_paths": [format!("crates/threadlane-native/src/part_{idx}.rs")]
                })
            })
            .collect();

        mcp_a
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "cmd-big-paged",
                    "plan_id": "plan-big-paged",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Multi-page 45 KiB task instructions",
                    "tasks": large_tasks
                }),
                None,
            )
            .await
            .expect("save big paged plan");

        let mut paged_tasks = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        loop {
            let res = mcp_a
                .invoke_tool_json(
                    "get_plan",
                    json!({
                        "plan_id": "plan-big-paged",
                        "task_cursor": cursor,
                        "task_limit": 10
                    }),
                    None,
                )
                .await
                .expect("get_plan page within 256 KiB");
            let wire_bytes = serde_json::to_vec(&res).expect("encode CallToolResult");
            assert!(
                wire_bytes.len() <= MAX_MCP_RESPONSE_BYTES,
                "page wire bytes {} exceeded {}",
                wire_bytes.len(),
                MAX_MCP_RESPONSE_BYTES
            );
            pages += 1;
            let page: PlanPage =
                serde_json::from_value(res.structured_content.unwrap()["data"].clone())
                    .expect("decode PlanPage");
            paged_tasks.extend(page.tasks);
            if let Some(next) = page.next_task_cursor {
                assert!(page.has_more);
                cursor = Some(next);
            } else {
                assert!(!page.has_more);
                break;
            }
        }
        assert!(pages >= 2, "5 x 45 KiB tasks must span multiple pages");
        assert_eq!(paged_tasks.len(), 5);
        for (idx, task) in paged_tasks.iter().enumerate() {
            let expected_inst = format!("Instruction-{}:{}", idx + 1, "x".repeat(45_000));
            assert_eq!(
                task.instruction, expected_inst,
                "task instruction must never be truncated"
            );
        }

        // Single task whose double-encoded MCP envelope exceeds 256 KiB fails `get_plan` with `response_too_large`.
        mcp_a
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "cmd-quote-heavy",
                    "plan_id": "plan-quote-heavy",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Single task > 256 KiB MCP envelope",
                    "tasks": [{
                        "id": "quote-task",
                        "title": "Quote heavy task",
                        "role": "coder",
                        "difficulty": "easy",
                        "instruction": "\"".repeat(50_000),
                        "depends_on": [],
                        "allowed_write_paths": ["src/lib.rs"]
                    }]
                }),
                None,
            )
            .await
            .expect("save quote-heavy draft");

        let get_overbudget_err = mcp_a
            .invoke_tool_json(
                "get_plan",
                json!({ "plan_id": "plan-quote-heavy", "task_limit": 1 }),
                None,
            )
            .await
            .expect_err("single task over 256 KiB must fail");
        assert_eq!(
            get_overbudget_err.structured_content.unwrap()["error"]["code"],
            "response_too_large"
        );

        // Draft whose `PublishedPlanView` exceeds 256 KiB fails `publish` with `response_too_large`
        // BEFORE mutating `PlanStore` (revision stays `Draft`).
        let many_route_tasks: Vec<Value> = (1..=60)
            .map(|idx| {
                let paths: Vec<String> = (1..=8)
                    .map(|p_idx| {
                        format!(
                            "crates/threadlane-native/src/deep_mod_{idx:02}/sub_{p_idx:02}/{}",
                            "segment_".repeat(45)
                        )
                    })
                    .collect();
                json!({
                    "id": format!("route-task-{idx:02}"),
                    "title": format!("Route Task {idx}"),
                    "role": "coder",
                    "difficulty": "hard",
                    "instruction": "Short instruction",
                    "depends_on": [],
                    "allowed_write_paths": paths
                })
            })
            .collect();

        mcp_a
            .invoke_tool_json(
                "save_draft",
                json!({
                    "command_key": "cmd-oversized-publish",
                    "plan_id": "plan-oversized-publish",
                    "expected_revision": 0,
                    "profile_id": "default-profile",
                    "profile_revision": reg_rev,
                    "summary": "Oversized publish view",
                    "tasks": many_route_tasks
                }),
                None,
            )
            .await
            .expect("save draft with large route view");

        let pub_overbudget_err = mcp_a
            .invoke_tool_json(
                "publish",
                json!({ "plan_id": "plan-oversized-publish", "revision": 1 }),
                None,
            )
            .await
            .expect_err("oversized publish must fail before mutation");
        assert_eq!(
            pub_overbudget_err.structured_content.unwrap()["error"]["code"],
            "response_too_large"
        );
        assert_eq!(
            plans
                .latest_revision("plan-oversized-publish")
                .expect("latest")
                .expect("exists")
                .status,
            PlanStatus::Draft
        );
    }
}

#[cfg(test)]
mod approved_tests;
