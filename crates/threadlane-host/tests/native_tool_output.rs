#![cfg(unix)]
#![forbid(unsafe_code)]

use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::ScopedClient;
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_core::WakeupStatus;
use threadlane_host::{
    prepare_session_dir, read_and_verify_host_manifest, write_host_manifest, ApprovedHost,
    ApprovedHostClient, ApprovedHostConfig, ApprovedHostNotificationConfig, HostCli, HostCommand,
    HostError, HostNotificationRoute, HostNotificationSummary, HostNotificationTransport,
    HostPhase, HostSessionManifest, NativeNotificationDelivery, NativeToolOutputConfig, ServeArgs,
    TaskExecutionStatus, APPROVED_EVENT_BATCH_MAX_EVENTS, HOST_LOCK_FILENAME,
    HOST_MANIFEST_FILENAME, RUNTIME_STATE_SUBDIR, WORK_AREA_SUBDIR,
};
use threadlane_plans::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanStore,
    PlanTask, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
use threadlane_registry::{RegistryChange, RegistryStore};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use tokio::net::{UnixListener, UnixStream};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

const THREAD_UUID_A: &str = "550e8400-e29b-41d4-a716-446655440000";
const THREAD_UUID_B: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
const BINDING_A: &str = "bnd_control_alpha";
const ACCEPTED_TURN_ID: &str = "0196ed60-1111-7000-8000-000000000001";
const NATIVE_MODEL: &str = "luna-max";
const NATIVE_EFFORT: &str = "high";

struct NativeHostTestFixture {
    _tmp: TempDir,
    repo_dir: PathBuf,
    other_repo_dir: PathBuf,
    base_commit: String,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    private_dir: PathBuf,
    socket_path: PathBuf,
    fake_codex_exe: PathBuf,
    codex_count_file: PathBuf,
    agy_count_file: PathBuf,
    registry: Arc<RegistryStore>,
    plans: Arc<PlanStore>,
    authority: Arc<AuthorityStore>,
}

fn write_executable_script(script_path: &Path, body: &str) {
    if let Some(parent) = script_path.parent() {
        fs::create_dir_all(parent).expect("create script parent dir");
    }
    let mut f = File::create(script_path).expect("create script");
    f.write_all(body.as_bytes()).expect("write script");
    f.sync_all().expect("sync script");
    fs::set_permissions(script_path, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
}

fn init_disposable_git_repo(repo_dir: &Path) -> String {
    fs::create_dir_all(repo_dir.join("src")).expect("create repo src");
    fs::write(repo_dir.join("src/lib.rs"), "pub fn alpha() -> u32 { 1 }\n")
        .expect("write src/lib.rs");
    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial commit", "--quiet"][..],
    ] {
        run_sanitized_git(repo_dir, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init step");
    }
    resolve_exact_commit_sha(repo_dir, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD")
}

fn setup_fixture() -> NativeHostTestFixture {
    let tmp = TempDir::new().expect("tempdir");
    let root = fs::canonicalize(tmp.path()).expect("canonicalize temp root");

    let repo_dir = root.join("repo");
    let base_commit = init_disposable_git_repo(&repo_dir);
    let other_repo_dir = root.join("other-repo");
    let _ = init_disposable_git_repo(&other_repo_dir);

    let bin_dir = root.join("bin");
    let fake_agy_exe = bin_dir.join("fake-agy");
    let fake_codex_exe = bin_dir.join("fake-codex");
    let agy_count_file = root.join("agy-invocations.txt");
    let codex_count_file = root.join("codex-queue-invocations.txt");

    write_executable_script(
        &fake_agy_exe,
        &format!(
            r#"#!/bin/sh
set -eu
COUNT_FILE="{count_file}"
c=0
if [ -f "$COUNT_FILE" ]; then c=$(cat "$COUNT_FILE"); fi
printf "%s" "$((c + 1))" > "$COUNT_FILE"
MODEL=""
EFFORT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --model) MODEL="$2"; shift 2 ;;
        --effort) EFFORT="$2"; shift 2 ;;
        *) shift 1 ;;
    esac
done
printf '{{"type":"init","session_id":"sess-agy-1","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
if [ -d "src" ]; then
    printf "// updated by fake-agy\npub fn alpha() -> u32 {{ 42 }}\n" > "src/lib.rs"
fi
printf '{{"type":"result","subtype":"success","session_id":"sess-agy-1","model":"%s","effort":"%s"}}\n' "$MODEL" "$EFFORT"
exit 0
"#,
            count_file = agy_count_file.display(),
        ),
    );

    write_executable_script(
        &fake_codex_exe,
        &format!(
            "#!/bin/sh\nset -eu\nCOUNT_FILE=\"{}\"\nc=0\nif [ -f \"$COUNT_FILE\" ]; then c=$(cat \"$COUNT_FILE\"); fi\nprintf \"%s\" \"$((c + 1))\" > \"$COUNT_FILE\"\nexit 0\n",
            codex_count_file.display()
        ),
    );

    let private_dir = root.join("private-native");
    fs::create_dir_all(&private_dir).expect("create private_dir");
    fs::set_permissions(&private_dir, fs::Permissions::from_mode(0o700))
        .expect("chmod 0700 private_dir");
    let socket_path = private_dir.join("codex.sock");

    let registry_dir = root.join("registry-store");
    let plans_dir = root.join("plans-store");
    let authority_dir = root.join("authority-store");
    let sessions_dir = root.join("sessions-store");

    let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
    let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

    registry
        .apply(
            0,
            RegistryChange::UpsertTool {
                tool: ToolDefinition {
                    id: "tool-agy".to_string(),
                    label: "Antigravity CLI".to_string(),
                    adapter: ToolAdapter::Antigravity,
                    executable_path: Some(fake_agy_exe),
                    enabled: true,
                },
            },
        )
        .expect("upsert tool-agy");
    registry
        .apply(
            1,
            RegistryChange::UpsertModels {
                models: vec![ModelDefinition {
                    id: "model-argon".to_string(),
                    tool_id: "tool-agy".to_string(),
                    label: "Gemini 4 Argon High".to_string(),
                    model_arg: "gemini-4-argon-high".to_string(),
                    efforts: vec!["high".to_string()],
                    allow_no_effort: false,
                }],
            },
        )
        .expect("upsert models");
    registry
        .apply(
            2,
            RegistryChange::UpsertProfile {
                profile: WorkflowProfile {
                    decision_advisor: None,
                    id: "profile-main".to_string(),
                    name: "Main Profile".to_string(),
                    assignments: vec![RoleAssignment {
                        role: WorkflowRole::Coder,
                        difficulty: Some(Difficulty::Hard),
                        choice: ModelChoice {
                            model_id: "model-argon".to_string(),
                            effort: Some("high".to_string()),
                        },
                    }],
                },
            },
        )
        .expect("upsert profile");

    let authority = Arc::new(
        AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
            .expect("open authority"),
    );

    NativeHostTestFixture {
        _tmp: tmp,
        repo_dir,
        other_repo_dir,
        base_commit,
        registry_dir,
        plans_dir,
        authority_dir,
        sessions_dir,
        private_dir,
        socket_path,
        fake_codex_exe,
        codex_count_file,
        agy_count_file,
        registry,
        plans,
        authority,
    }
}

fn create_approved_plan(
    fixture: &NativeHostTestFixture,
    plan_id: &str,
    binding_id: &str,
    thread_uuid: &str,
) -> String {
    let tasks: Vec<PlanTask> = (1..=2)
        .map(|number| PlanTask {
            id: format!("task-root-{number}"),
            title: "Root task".to_string(),
            instruction: "CONFIDENTIAL_WORKER_INSTRUCTION_DO_NOT_LEAK".to_string(),
            role: WorkflowRole::Coder,
            difficulty: Difficulty::Hard,
            depends_on: Vec::new(),
            allowed_write_paths: vec!["src/lib.rs".to_string()],
        })
        .collect();
    let reg_snap = fixture.registry.snapshot().expect("registry snapshot");
    let draft = fixture
        .plans
        .change_at(
            0,
            PlanChange::SaveDraft {
                plan_id: plan_id.to_string(),
                binding_id: binding_id.to_string(),
                profile_id: "profile-main".to_string(),
                profile_revision: reg_snap.revision,
                summary: format!("Plan {plan_id}"),
                tasks: tasks.clone(),
            },
            1_700_000_000_000,
        )
        .expect("save draft");
    let published = fixture
        .plans
        .change(
            draft.revision,
            PlanChange::Publish {
                plan_id: plan_id.to_string(),
                revision: draft.revision,
            },
        )
        .expect("publish plan");
    let compiled = compile_plan_revision(&published, &reg_snap).expect("compile published plan");

    fixture
        .authority
        .approve_at(
            OperatorApprovalRequest {
                binding_id: binding_id.to_string(),
                thread_uuid: thread_uuid.to_string(),
                plan_id: plan_id.to_string(),
                plan_revision: published.revision,
                expected_registry_revision: reg_snap.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: fixture.repo_dir.clone(),
                base_commit: fixture.base_commit.clone(),
                approved_task_ids: tasks.iter().map(|task| task.id.clone()).collect(),
                approved_write_paths: vec!["src/".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            },
            1_700_000_001_000,
        )
        .expect("approve plan")
        .approval_id
}

fn read_invocation_count(path: &Path) -> u32 {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(0)
}

fn write_private_json_context(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).expect("chmod 0700 parent");
    }
    fs::write(path, serde_json::to_vec_pretty(value).expect("serialize")).expect("write context");
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod 0600 context");
}

fn sample_native_config(
    socket_path: &Path,
    thread_id: &str,
    project_root: &Path,
) -> NativeToolOutputConfig {
    NativeToolOutputConfig {
        schema_version: 1,
        socket_path: socket_path.to_path_buf(),
        delivery: NativeNotificationDelivery::ToolOutput,
        reconnect: None,
        thread_id: thread_id.to_string(),
        project_root: project_root.to_path_buf(),
        model: NATIVE_MODEL.to_string(),
        effort: NATIVE_EFFORT.to_string(),
    }
}

#[test]
fn notification_summary_wire_compatibility_validation_and_redaction() {
    let fixture = setup_fixture();
    let approval_id =
        create_approved_plan(&fixture, "plan-summary-compat", BINDING_A, THREAD_UUID_A);
    let _sock_guard = StdUnixListener::bind(&fixture.socket_path).expect("bind unix socket");

    let native_cfg = sample_native_config(&fixture.socket_path, THREAD_UUID_A, &fixture.repo_dir);
    native_cfg.validate().expect("valid native config");

    let dbg_str = format!("{native_cfg:?}");
    for secret in [
        THREAD_UUID_A,
        NATIVE_MODEL,
        fixture.socket_path.to_str().unwrap(),
        fixture.repo_dir.to_str().unwrap(),
    ] {
        assert!(
            !dbg_str.contains(secret),
            "Debug output must redact {secret}"
        );
    }

    // 1. Disabled and default-queue summaries remain byte-for-byte identical to legacy manifests.
    let disabled = HostNotificationSummary::disabled();
    assert_eq!(disabled.transport, HostNotificationTransport::Queue);
    assert_eq!(disabled.native_context_digest, None);
    disabled.validate().expect("valid disabled summary");
    assert_eq!(
        serde_json::to_string(&disabled).expect("serialize disabled"),
        r#"{"enabled":false,"executable_digest":null,"timeout_ms":null}"#
    );

    let queue_cfg = ApprovedHostNotificationConfig::new(fixture.fake_codex_exe.clone(), 12_000)
        .expect("new queue notification config");
    assert_eq!(queue_cfg.tool_output, None);
    let queue_summary = queue_cfg.summary();
    queue_summary.validate().expect("valid queue summary");
    let queue_json: Value = serde_json::to_value(&queue_summary).expect("serialize queue summary");
    assert!(queue_json.get("transport").is_none());
    assert!(queue_json.get("native_context_digest").is_none());
    assert!(queue_json.get("route").is_none());

    // 2. Enabled native summary includes `unix_tool_output` and 64-hex `native_context_digest` without leaking paths/model/thread.
    let host_native_cfg = queue_cfg
        .clone()
        .with_route(HostNotificationRoute::OperatorCli)
        .with_tool_output(native_cfg.clone())
        .expect("with_tool_output");
    let native_summary = host_native_cfg.summary();
    assert_eq!(
        native_summary.executable_digest,
        queue_summary.executable_digest
    );
    assert_eq!(
        native_summary.transport,
        HostNotificationTransport::UnixToolOutput
    );
    assert_eq!(
        native_summary.native_context_digest.as_deref(),
        Some(native_cfg.digest().as_str())
    );
    assert_eq!(native_cfg.digest().len(), 64);
    native_summary.validate().expect("valid native summary");

    let serialized_native = serde_json::to_string(&native_summary).expect("serialize native");
    assert!(serialized_native.contains(r#""transport":"unix_tool_output""#));
    assert!(serialized_native.contains(&native_cfg.digest()));
    for secret in [
        THREAD_UUID_A,
        NATIVE_MODEL,
        fixture.socket_path.to_str().unwrap(),
        fixture.repo_dir.to_str().unwrap(),
    ] {
        assert!(!serialized_native.contains(secret));
    }
    assert_eq!(
        serde_json::from_str::<HostNotificationSummary>(&serialized_native).expect("roundtrip"),
        native_summary
    );

    // 2b. Direct user-message steer mode reports `unix_user_message_steer` and a distinct digest.
    let steer_native_cfg = native_cfg
        .clone()
        .with_delivery(NativeNotificationDelivery::UserMessageSteer);
    assert_ne!(steer_native_cfg.digest(), native_cfg.digest());
    let host_steer_cfg = queue_cfg
        .clone()
        .with_route(HostNotificationRoute::OperatorCli)
        .with_steer_context(native_cfg.clone())
        .expect("with_steer_context");
    let steer_summary = host_steer_cfg.summary();
    assert_eq!(
        steer_summary.transport,
        HostNotificationTransport::UnixUserMessageSteer
    );
    assert_eq!(
        steer_summary.native_context_digest.as_deref(),
        Some(steer_native_cfg.digest().as_str())
    );
    steer_summary.validate().expect("valid steer summary");
    let serialized_steer = serde_json::to_string(&steer_summary).expect("serialize steer");
    assert!(serialized_steer.contains(r#""transport":"unix_user_message_steer""#));
    assert_eq!(
        serde_json::from_str::<HostNotificationSummary>(&serialized_steer).expect("roundtrip"),
        steer_summary
    );

    // 3. Invalid summary combinations fail validation and manifest verification.
    for invalid in [
        HostNotificationSummary {
            enabled: false,
            executable_digest: None,
            timeout_ms: None,
            route: HostNotificationRoute::Mcp,
            transport: HostNotificationTransport::UnixToolOutput,
            native_context_digest: None,
        },
        HostNotificationSummary {
            enabled: false,
            executable_digest: None,
            timeout_ms: None,
            route: HostNotificationRoute::Mcp,
            transport: HostNotificationTransport::Queue,
            native_context_digest: Some(native_cfg.digest()),
        },
        HostNotificationSummary {
            enabled: true,
            executable_digest: queue_summary.executable_digest.clone(),
            timeout_ms: Some(12_000),
            route: HostNotificationRoute::Mcp,
            transport: HostNotificationTransport::Queue,
            native_context_digest: Some(native_cfg.digest()),
        },
        HostNotificationSummary {
            enabled: true,
            executable_digest: queue_summary.executable_digest.clone(),
            timeout_ms: Some(12_000),
            route: HostNotificationRoute::Mcp,
            transport: HostNotificationTransport::UnixToolOutput,
            native_context_digest: None,
        },
    ] {
        assert_eq!(
            invalid.validate().expect_err("must fail").code(),
            "validation"
        );
    }

    let record = fixture
        .authority
        .get_for_operator(&approval_id)
        .expect("get_for_operator");
    let session_dir =
        prepare_session_dir(&fixture.sessions_dir, &approval_id).expect("prepare_session_dir");
    let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
    let valid_manifest = HostSessionManifest::from_approval_record(
        &record,
        Some("bnd_session_native_test".to_string()),
        HostPhase::Stopped,
        None,
        None,
        1_700_000_000_000,
        1_700_000_001_000,
    )
    .with_notifications(native_summary.clone());
    write_host_manifest(&session_dir, &valid_manifest).expect("write valid native manifest");
    assert_eq!(
        read_and_verify_host_manifest(&manifest_path, &record)
            .expect("verify valid native manifest")
            .notifications,
        native_summary
    );

    let mut corrupt_val: Value = serde_json::to_value(&valid_manifest).expect("to_value");
    corrupt_val["notifications"] = json!({
        "enabled": true,
        "executable_digest": queue_summary.executable_digest,
        "timeout_ms": 12_000,
        "transport": "queue",
        "native_context_digest": native_cfg.digest(),
    });
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&corrupt_val).unwrap(),
    )
    .unwrap();
    fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        read_and_verify_host_manifest(&manifest_path, &record)
            .expect_err("manifest with queue + native_context_digest must be rejected")
            .code(),
        "corrupt_manifest"
    );
}

#[test]
fn cli_notify_tool_output_context_requires_notify_codex_and_validates_private_file() {
    let fixture = setup_fixture();
    let approval_id = create_approved_plan(&fixture, "plan-cli-context", BINDING_A, THREAD_UUID_A);
    let _sock_guard = StdUnixListener::bind(&fixture.socket_path).expect("bind unix socket");

    let context_path = fixture.private_dir.join("context.json");
    write_private_json_context(
        &context_path,
        &json!({
            "schema_version": 1,
            "socket_path": fixture.socket_path,
            "thread_id": THREAD_UUID_A,
            "project_root": fixture.repo_dir,
            "model": NATIVE_MODEL,
            "effort": NATIVE_EFFORT
        }),
    );

    // 1. `--notify-tool-output-context` without `--notify-codex` is rejected.
    let cli_missing_codex = HostCli::try_parse_from([
        "threadlane-host",
        "serve",
        "--registry-dir",
        fixture.registry_dir.to_str().unwrap(),
        "--plans-dir",
        fixture.plans_dir.to_str().unwrap(),
        "--authority-dir",
        fixture.authority_dir.to_str().unwrap(),
        "--sessions-dir",
        fixture.sessions_dir.to_str().unwrap(),
        "--approval-id",
        &approval_id,
        "--notify-tool-output-context",
        context_path.to_str().unwrap(),
    ])
    .expect("parse CLI");
    let HostCommand::Serve(args_missing_codex) = cli_missing_codex.command else {
        panic!("expected Serve");
    };
    assert_eq!(
        args_missing_codex
            .into_host_config()
            .expect_err("--notify-tool-output-context without --notify-codex must fail")
            .code(),
        "validation"
    );

    // 2. Valid `--notify-tool-output-context` with `--notify-codex`, `--notify-route`, and `--notify-timeout-ms`.
    let cli_valid = HostCli::try_parse_from([
        "threadlane-host",
        "serve",
        "--registry-dir",
        fixture.registry_dir.to_str().unwrap(),
        "--plans-dir",
        fixture.plans_dir.to_str().unwrap(),
        "--authority-dir",
        fixture.authority_dir.to_str().unwrap(),
        "--sessions-dir",
        fixture.sessions_dir.to_str().unwrap(),
        "--approval-id",
        &approval_id,
        "--notify-codex",
        fixture.fake_codex_exe.to_str().unwrap(),
        "--notify-timeout-ms",
        "9000",
        "--notify-route",
        "operator-cli",
        "--notify-tool-output-context",
        context_path.to_str().unwrap(),
    ])
    .expect("parse valid CLI");
    let HostCommand::Serve(args_valid) = cli_valid.command else {
        panic!("expected Serve");
    };
    let notify_cfg = args_valid
        .into_host_config()
        .expect("into_host_config")
        .notifications
        .expect("notifications enabled");
    assert_eq!(notify_cfg.timeout_ms, 9_000);
    assert_eq!(notify_cfg.route, HostNotificationRoute::OperatorCli);
    assert_eq!(
        notify_cfg.tool_output,
        Some(sample_native_config(
            &fixture.socket_path,
            THREAD_UUID_A,
            &fixture.repo_dir
        ))
    );

    // 3. Symlinked context file, non-0600 permissions, and unknown JSON fields fail closed.
    let symlink_context = fixture.private_dir.join("context-link.json");
    symlink(&context_path, &symlink_context).expect("symlink context");
    let base_args = |ctx_path: PathBuf| ServeArgs {
        registry_dir: fixture.registry_dir.clone(),
        plans_dir: fixture.plans_dir.clone(),
        authority_dir: fixture.authority_dir.clone(),
        sessions_dir: fixture.sessions_dir.clone(),
        approval_id: approval_id.clone(),
        notify_codex: Some(fixture.fake_codex_exe.clone()),
        notify_timeout_ms: None,
        notify_route: None,
        notify_tool_output_context: Some(ctx_path),
        restart_from_instance_id: None,
        start_fresh_only: false,
    };
    assert!(base_args(symlink_context).into_host_config().is_err());

    fs::set_permissions(&context_path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
    assert!(base_args(context_path.clone()).into_host_config().is_err());
    fs::set_permissions(&context_path, fs::Permissions::from_mode(0o600)).expect("restore 0600");

    let unknown_field_path = fixture.private_dir.join("unknown-field.json");
    write_private_json_context(
        &unknown_field_path,
        &json!({
            "schema_version": 1,
            "socket_path": fixture.socket_path,
            "thread_id": THREAD_UUID_A,
            "project_root": fixture.repo_dir,
            "model": NATIVE_MODEL,
            "effort": NATIVE_EFFORT,
            "unexpected": true
        }),
    );
    assert!(base_args(unknown_field_path).into_host_config().is_err());
}

#[tokio::test]
async fn mismatched_thread_or_project_root_rejected_before_any_bootstrap_or_restart_effect() {
    let fixture = setup_fixture();
    let approval_id = create_approved_plan(
        &fixture,
        "plan-early-scope-reject",
        BINDING_A,
        THREAD_UUID_A,
    );
    let _sock_guard = StdUnixListener::bind(&fixture.socket_path).expect("bind unix socket");

    let make_notify = |thread_id: &str, repo: &Path| {
        ApprovedHostNotificationConfig::new(fixture.fake_codex_exe.clone(), 5_000)
            .expect("new notify config")
            .with_tool_output(sample_native_config(&fixture.socket_path, thread_id, repo))
            .expect("valid syntax for native config")
    };
    let wrong_thread_cfg = make_notify(THREAD_UUID_B, &fixture.repo_dir);
    let wrong_repo_cfg = make_notify(THREAD_UUID_A, &fixture.other_repo_dir);
    let session_dir = fixture.sessions_dir.join(&approval_id);

    let host_cfg = |notify: Option<ApprovedHostNotificationConfig>| {
        let mut cfg = ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        );
        if let Some(n) = notify {
            cfg = cfg.with_notifications(n);
        }
        cfg
    };

    // 1. `start` and `start_fresh_only` reject mismatched `thread_id` and `project_root` before creating
    //    `<sessions_dir>/<approval_id>`, `host.lock`, `host-manifest.json`, `runtime-state`, or `work-area`.
    let start_err = ApprovedHost::start(host_cfg(Some(wrong_thread_cfg.clone())))
        .await
        .expect_err("mismatched thread_id must fail on start");
    assert_eq!(start_err.code(), "scope_mismatch");
    assert!(matches!(
        &start_err,
        HostError::ScopeMismatch { field, .. } if field == "thread_id"
    ));
    assert!(!start_err.to_string().contains(THREAD_UUID_A));
    assert!(!start_err.to_string().contains(THREAD_UUID_B));
    assert!(!session_dir.exists());

    let fresh_err = ApprovedHost::start_fresh_only(host_cfg(Some(wrong_repo_cfg.clone())))
        .await
        .expect_err("mismatched project_root must fail on start_fresh_only");
    assert_eq!(fresh_err.code(), "scope_mismatch");
    assert!(matches!(
        &fresh_err,
        HostError::ScopeMismatch { field, .. } if field == "project_root"
    ));
    assert!(!fresh_err
        .to_string()
        .contains(fixture.repo_dir.to_str().unwrap()));
    assert!(!fresh_err
        .to_string()
        .contains(fixture.other_repo_dir.to_str().unwrap()));
    assert!(!session_dir.exists());
    for sub in [
        HOST_LOCK_FILENAME,
        HOST_MANIFEST_FILENAME,
        RUNTIME_STATE_SUBDIR,
        WORK_AREA_SUBDIR,
    ] {
        assert!(!session_dir.join(sub).exists());
    }

    // 2. Start valid native session, verify Ready summary, and shut down cleanly.
    let matching_native_cfg =
        sample_native_config(&fixture.socket_path, THREAD_UUID_A, &fixture.repo_dir);
    let valid_notify_cfg = make_notify(THREAD_UUID_A, &fixture.repo_dir);
    let handle = ApprovedHost::start_fresh_only(host_cfg(Some(valid_notify_cfg)))
        .await
        .expect("start_fresh_only with matching native config");
    assert_eq!(
        handle.summary().notifications.transport,
        HostNotificationTransport::UnixToolOutput
    );
    assert_eq!(
        handle
            .summary()
            .notifications
            .native_context_digest
            .as_deref(),
        Some(matching_native_cfg.digest().as_str())
    );
    let stopped_instance_id = handle.instance_id().to_string();
    handle.shutdown().await.expect("clean shutdown");

    let manifest_path = session_dir.join(HOST_MANIFEST_FILENAME);
    let stopped_bytes = fs::read(&manifest_path).expect("read Stopped manifest");

    // 3. `restart_stopped` with mismatched `thread_id` or `project_root` fails before touching Stopped manifest.
    for bad_cfg in [wrong_thread_cfg, wrong_repo_cfg] {
        let err = ApprovedHost::restart_stopped(host_cfg(Some(bad_cfg)), &stopped_instance_id)
            .await
            .expect_err("restart_stopped with mismatched native scope must fail");
        assert_eq!(err.code(), "scope_mismatch");
        assert_eq!(
            fs::read(&manifest_path).expect("read manifest"),
            stopped_bytes
        );
    }

    // 4. Restarting without explicit notification opt-in resets notifications to disabled.
    let restarted_disabled = ApprovedHost::restart_stopped(host_cfg(None), &stopped_instance_id)
        .await
        .expect("restart_stopped with notifications disabled");
    assert_eq!(
        restarted_disabled.summary().notifications,
        HostNotificationSummary::disabled()
    );
    restarted_disabled.shutdown().await.expect("shutdown");
    assert_eq!(read_invocation_count(&fixture.codex_count_file), 0);
    assert_eq!(read_invocation_count(&fixture.agy_count_file), 0);
}

// The callback result is fixed by tungstenite's public handshake trait.
#[allow(clippy::result_large_err)]
fn validate_upgrade(
    req: &Request,
    resp: Response,
) -> Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
    assert_eq!(req.uri().path(), "/");
    Ok(resp)
}

async fn recv_json_rpc(ws: &mut WebSocketStream<UnixStream>) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timed out waiting for client WebSocket frame")
        .expect("WebSocket stream ended unexpectedly")
        .expect("WebSocket frame error");
    match msg {
        Message::Text(text) => serde_json::from_str(text.as_ref()).expect("parse JSON-RPC text"),
        other => panic!("expected Text WebSocket frame, got {other:?}"),
    }
}

async fn send_json_rpc(ws: &mut WebSocketStream<UnixStream>, value: &Value) {
    let text = serde_json::to_string(value).expect("serialize JSON-RPC response");
    tokio::time::timeout(Duration::from_secs(5), ws.send(Message::Text(text.into())))
        .await
        .expect("timed out sending WebSocket frame")
        .expect("failed to send WebSocket frame");
}

async fn handle_one_native_tool_output_exchange(
    listener: &UnixListener,
    thread_id: &str,
    project_root: &Path,
    thread_status_type: &str,
) -> Value {
    let (stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("timed out waiting for Unix WebSocket connection")
        .expect("accept unix ws client");
    let mut ws = tokio::time::timeout(
        Duration::from_secs(5),
        accept_hdr_async(stream, validate_upgrade),
    )
    .await
    .expect("timed out during WebSocket HTTP Upgrade handshake")
    .expect("WebSocket handshake failed");

    // 1. initialize
    let init_req = recv_json_rpc(&mut ws).await;
    assert_eq!(init_req["method"], "initialize");
    assert!(init_req["params"]["clientInfo"].is_object());
    send_json_rpc(
        &mut ws,
        &json!({
            "id": init_req["id"],
            "result": { "userAgent": "fake-codex-native/1.0" }
        }),
    )
    .await;

    // 2. initialized notification
    let initialized_notif = recv_json_rpc(&mut ws).await;
    assert_eq!(initialized_notif["method"], "initialized");
    assert!(initialized_notif.get("id").is_none());

    // 3. thread/read with includeTurns: false
    let read_req = recv_json_rpc(&mut ws).await;
    assert_eq!(read_req["method"], "thread/read");
    assert_eq!(
        read_req["params"],
        json!({
            "threadId": thread_id,
            "includeTurns": false
        })
    );
    let cwd_str = project_root.to_string_lossy();
    send_json_rpc(
        &mut ws,
        &json!({
            "id": read_req["id"],
            "result": {
                "thread": {
                    "id": thread_id,
                    "cwd": cwd_str,
                    "status": { "type": thread_status_type }
                }
            }
        }),
    )
    .await;

    // 4. thread/resume with ONLY threadId and excludeTurns: true
    let resume_req = recv_json_rpc(&mut ws).await;
    assert_eq!(resume_req["method"], "thread/resume");
    assert_eq!(
        resume_req["params"],
        json!({
            "threadId": thread_id,
            "excludeTurns": true
        })
    );
    send_json_rpc(
        &mut ws,
        &json!({
            "id": resume_req["id"],
            "result": {
                "thread": {
                    "id": thread_id,
                    "cwd": cwd_str,
                    "status": { "type": thread_status_type }
                },
                "cwd": cwd_str,
                "model": NATIVE_MODEL,
                "reasoningEffort": NATIVE_EFFORT
            }
        }),
    )
    .await;

    // 5. turn/start with input: [] and toolOutput
    let turn_req = recv_json_rpc(&mut ws).await;
    assert_eq!(turn_req["method"], "turn/start");
    let params = &turn_req["params"];
    assert_eq!(params["threadId"], thread_id);
    assert_eq!(params["input"], json!([]));
    for forbidden in [
        "model",
        "effort",
        "reasoningEffort",
        "approvalPolicy",
        "sandbox",
    ] {
        assert!(
            params.get(forbidden).is_none(),
            "unexpected override {forbidden}"
        );
    }
    assert_eq!(params["toolOutput"]["name"], "threadlane_events_available");
    assert_eq!(params["toolOutput"]["namespace"], "threadlane");

    send_json_rpc(
        &mut ws,
        &json!({
            "id": turn_req["id"],
            "result": {
                "turn": {
                    "id": ACCEPTED_TURN_ID,
                    "status": "inProgress",
                    "error": null
                }
            }
        }),
    )
    .await;

    let output_str = params["toolOutput"]["output"]
        .as_str()
        .expect("toolOutput.output string");
    serde_json::from_str(output_str).expect("parse toolOutput.output JSON")
}

async fn wait_for_ready(client: &ApprovedHostClient, approval: &str, task: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snap = client.get_task(approval, task).await.expect("get_task");
        if snap.status == TaskExecutionStatus::ReadyForReview {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task did not become ReadyForReview: {:?}",
            snap.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_submitted(scoped: &ScopedClient, attempts: u64) -> threadlane::StatusResponse {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let st = scoped.status().await.expect("scoped status");
        if st.wakeup.status == WakeupStatus::Submitted && st.wakeup.attempt_count == attempts {
            return st;
        }
        assert!(
            Instant::now() < deadline,
            "notification did not reach Submitted: {:?}",
            st.wakeup
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn drain_actual_batches(client: &ApprovedHostClient, approval: &str) {
    loop {
        let batch = client
            .pull_events(approval, None, Some(APPROVED_EVENT_BATCH_MAX_EVENTS))
            .await
            .expect("pull actual event batch");
        assert!(!batch.events.is_empty());
        let ack = client
            .ack_events(approval, batch.batch_id)
            .await
            .expect("ack returned batch_id");
        assert_eq!(ack.acked_seq, batch.to_seq);
        if !batch.has_more {
            return;
        }
    }
}

#[tokio::test]
async fn host_native_tool_output_end_to_end_submits_without_queue_and_rearms_after_ack() {
    let fixture = setup_fixture();
    let approval_id = create_approved_plan(&fixture, "plan-e2e-native", BINDING_A, THREAD_UUID_A);
    let listener = UnixListener::bind(&fixture.socket_path).expect("bind async unix listener");
    let native_cfg = sample_native_config(&fixture.socket_path, THREAD_UUID_A, &fixture.repo_dir);
    let notify_cfg = ApprovedHostNotificationConfig::new(fixture.fake_codex_exe.clone(), 5_000)
        .expect("new notify config")
        .with_route(HostNotificationRoute::OperatorCli)
        .with_tool_output(native_cfg)
        .expect("with_tool_output");
    let host = ApprovedHost::start_fresh_only(
        ApprovedHostConfig::new(
            &fixture.registry_dir,
            &fixture.plans_dir,
            &fixture.authority_dir,
            &fixture.sessions_dir,
            &approval_id,
        )
        .with_tick_interval(Duration::from_millis(20))
        .with_notifications(notify_cfg),
    )
    .await
    .expect("start native notification host");
    let repo_for_server = fixture.repo_dir.clone();
    let server_task = tokio::spawn(async move {
        let first = handle_one_native_tool_output_exchange(
            &listener,
            THREAD_UUID_A,
            &repo_for_server,
            "idle",
        )
        .await;
        let second = handle_one_native_tool_output_exchange(
            &listener,
            THREAD_UUID_A,
            &repo_for_server,
            "active",
        )
        .await;
        (first, second)
    });
    let client = ApprovedHostClient::open(
        &fixture.registry_dir,
        &fixture.plans_dir,
        &fixture.authority_dir,
        &fixture.sessions_dir,
        BINDING_A,
        THREAD_UUID_A,
    )
    .expect("approved host client");
    let scoped = ScopedClient::from_state_dir(host.runtime_state_dir(), host.session_binding_id())
        .expect("runtime scoped client");

    // Worker terminal events trigger notices. RunStarted itself is not an
    // important event; do not gate worker completion on a nonexistent start notice.
    client
        .dispatch_task(&approval_id, "task-root-1")
        .await
        .expect("dispatch first task");
    wait_for_ready(&client, &approval_id, "task-root-1").await;
    let first = wait_for_submitted(&scoped, 1).await;
    let first_claim = first.wakeup.claim_id.clone().expect("first claim");
    assert_eq!(read_invocation_count(&fixture.codex_count_file), 0);
    drain_actual_batches(&client, &approval_id).await;
    assert_eq!(
        scoped.status().await.unwrap().wakeup.status,
        WakeupStatus::Idle
    );

    client
        .dispatch_task(&approval_id, "task-root-2")
        .await
        .expect("dispatch second task");
    wait_for_ready(&client, &approval_id, "task-root-2").await;
    let second = wait_for_submitted(&scoped, 2).await;
    assert_ne!(
        second.wakeup.claim_id.as_deref(),
        Some(first_claim.as_str())
    );
    assert!(second.wakeup.covered_up_to_seq > first.wakeup.covered_up_to_seq);
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server completion timeout")
        .expect("server join");
    assert_eq!(a["claim_id"], json!(first_claim));
    assert_ne!(a["claim_id"], b["claim_id"]);
    for payload in [&a, &b] {
        assert_eq!(payload["untrusted_data"], true);
        assert_eq!(payload["approval_id"], json!(approval_id));
        assert!(payload["coalesced_count"].as_u64().unwrap() >= 1);
        let notice = payload["notice"].as_str().expect("notice string");
        assert!(notice.contains("threadlane-session pull"));
        assert!(!notice.contains("CONFIDENTIAL_WORKER_INSTRUCTION_DO_NOT_LEAK"));
    }
    drain_actual_batches(&client, &approval_id).await;
    assert_eq!(
        scoped.status().await.unwrap().wakeup.status,
        WakeupStatus::Idle
    );
    assert_eq!(read_invocation_count(&fixture.codex_count_file), 0);
    host.shutdown().await.expect("shutdown host");
}
