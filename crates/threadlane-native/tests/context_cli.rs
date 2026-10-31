#![cfg(unix)]
//! End-to-end behavioral tests for the packaged `threadlane-context` CLI.
//!
//! Covers outer help/version without context or runtime, sanitized syntax errors, existing private
//! v1 context validation with legacy installation metadata, `check` + `workflow catalog` +
//! `session` missing-host refusal, foreign/offline bindings, protected scope/context flag override
//! refusals with zero side effects, bad schema/route/identity/duplicate fields, package-pin and
//! store guards, symlink/insecure/oversized context files, missing or unsafe sibling binaries, and
//! child stdio/exit/signal (`outcome_unknown`) handling.

use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{symlink, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::Arc;

use serde_json::{json, Value};
use tempfile::TempDir;
use threadlane::{Supervisor, SupervisorClient, SupervisorConfig, SupervisorHandle};
use threadlane_authority::{AuthorityStore, OperatorApprovalRequest};
use threadlane_native::{PlanStore, RegistryStore};
use threadlane_workers::{
    resolve_exact_commit_sha, run_sanitized_git, ExecutionBudgets, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};
use threadlane_workflow::{
    compile_plan_revision, Difficulty, ModelChoice, ModelDefinition, PlanChange, PlanTask,
    RegistryChange, RoleAssignment, ToolAdapter, ToolDefinition, WorkflowProfile, WorkflowRole,
};
use tokio::process::Command;

const THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000a1";
const OTHER_THREAD_UUID: &str = "0196ed00-0000-7000-8000-0000000000b2";
const BINDING_LABEL: &str = "context-cli-chat";
const PROFILE_ID: &str = "profile-context-cli";
const PLAN_ID: &str = "plan-context-cli";
const MAX_IO_BYTES: usize = 1_048_576;
const SECRET_SENTINEL: &str = "THREADLANE_CONTEXT_SECRET_SENTINEL_9f4b2c1e";

struct ContextFixture {
    _tmp: TempDir,
    root: PathBuf,
    project_root: PathBuf,
    pkg_dir: PathBuf,
    pkg_bin_dir: PathBuf,
    context_bin: PathBuf,
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
    contexts_dir: PathBuf,
    supervisor: Option<SupervisorHandle>,
    binding_id: String,
    approval_id: String,
}

impl ContextFixture {
    async fn start() -> Self {
        let tmp = TempDir::new().expect("create context cli tempdir");
        let root = fs::canonicalize(tmp.path()).expect("canonicalize tempdir");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).expect("chmod root 0700");

        // Project root is an existing canonical non-symlink directory with normal shared 0755 permissions.
        let project_root = root.join("project-repo");
        let base_commit = init_disposable_repo(&project_root);
        fs::set_permissions(&project_root, fs::Permissions::from_mode(0o755))
            .expect("chmod project_root 0755");

        // Stage a package layout `<root>/pkg/bin` with real compiled binaries.
        let pkg_dir = root.join("pkg");
        let pkg_bin_dir = pkg_dir.join("bin");
        fs::create_dir_all(&pkg_bin_dir).expect("create pkg/bin");
        fs::set_permissions(&pkg_dir, fs::Permissions::from_mode(0o755)).expect("chmod pkg 0755");
        fs::set_permissions(&pkg_bin_dir, fs::Permissions::from_mode(0o755))
            .expect("chmod pkg/bin 0755");

        let context_bin = copy_executable_atomic(
            Path::new(env!("CARGO_BIN_EXE_threadlane-context")),
            &pkg_bin_dir,
            "threadlane-context",
        );
        copy_executable_atomic(
            Path::new(env!("CARGO_BIN_EXE_threadlane-workflow")),
            &pkg_bin_dir,
            "threadlane-workflow",
        );
        copy_executable_atomic(
            Path::new(env!("CARGO_BIN_EXE_threadlane-session")),
            &pkg_bin_dir,
            "threadlane-session",
        );

        let fake_worker = write_executable_script(
            &pkg_bin_dir,
            "fake-opencode",
            "#!/usr/bin/env sh\nexit 0\n",
            0o700,
        );

        let state_dir = root.join("state");
        let registry_dir = root.join("registry");
        let plans_dir = root.join("plans");
        let authority_dir = root.join("authority");
        let sessions_dir = root.join("sessions");
        let contexts_dir = root.join("contexts");

        for dir in [
            &state_dir,
            &registry_dir,
            &plans_dir,
            &authority_dir,
            &sessions_dir,
            &contexts_dir,
        ] {
            fs::create_dir_all(dir).expect("create private dir");
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
                .expect("chmod private dir 0700");
        }

        let supervisor = Supervisor::start(SupervisorConfig::new(state_dir.clone()))
            .await
            .expect("start supervisor");
        let sup_client =
            SupervisorClient::from_state_dir(&state_dir).expect("connect supervisor client");
        let binding = sup_client
            .create_binding(THREAD_UUID, BINDING_LABEL)
            .await
            .expect("create binding");

        let registry = Arc::new(RegistryStore::open(&registry_dir).expect("open registry"));
        let plans = Arc::new(PlanStore::open(&plans_dir).expect("open plans"));

        let s1 = registry
            .apply(
                0,
                RegistryChange::UpsertTool {
                    tool: ToolDefinition {
                        id: "tool-opencode".to_string(),
                        label: "OpenCode v2 fixture".to_string(),
                        adapter: ToolAdapter::OpencodeV2,
                        executable_path: Some(fake_worker),
                        enabled: true,
                    },
                },
            )
            .expect("upsert tool");
        let s2 = registry
            .apply(
                s1.revision,
                RegistryChange::UpsertModel {
                    model: ModelDefinition {
                        id: "model-deepseek-flash".to_string(),
                        tool_id: "tool-opencode".to_string(),
                        label: "DeepSeek v4.1 Flash Max".to_string(),
                        model_arg: "opencode-go/deepseek-v4.1-flash".to_string(),
                        efforts: vec!["max".to_string()],
                        allow_no_effort: false,
                    },
                },
            )
            .expect("upsert model");
        let registry_snapshot = registry
            .apply(
                s2.revision,
                RegistryChange::UpsertProfile {
                    profile: WorkflowProfile {
                        id: PROFILE_ID.to_string(),
                        name: "Context CLI Fixture Profile".to_string(),
                        assignments: vec![RoleAssignment {
                            role: WorkflowRole::Coder,
                            difficulty: Some(Difficulty::Easy),
                            choice: ModelChoice {
                                model_id: "model-deepseek-flash".to_string(),
                                effort: Some("max".to_string()),
                            },
                        }],
                    },
                },
            )
            .expect("upsert profile");

        let draft = plans
            .change(
                0,
                PlanChange::SaveDraft {
                    plan_id: PLAN_ID.to_string(),
                    binding_id: binding.binding_id.clone(),
                    profile_id: PROFILE_ID.to_string(),
                    profile_revision: registry_snapshot.revision,
                    summary: "Context CLI fixture plan".to_string(),
                    tasks: vec![PlanTask {
                        id: "task-1".to_string(),
                        title: "Verify context routing".to_string(),
                        instruction: "Run bounded context check".to_string(),
                        role: WorkflowRole::Coder,
                        difficulty: Difficulty::Easy,
                        depends_on: Vec::new(),
                        allowed_write_paths: vec!["src/lib.rs".to_string()],
                    }],
                },
            )
            .expect("save plan draft");
        let published = plans
            .change(
                draft.revision,
                PlanChange::Publish {
                    plan_id: PLAN_ID.to_string(),
                    revision: draft.revision,
                },
            )
            .expect("publish plan");
        let compiled =
            compile_plan_revision(&published, &registry_snapshot).expect("compile published plan");

        let authority =
            AuthorityStore::open(&authority_dir, Arc::clone(&registry), Arc::clone(&plans))
                .expect("open authority store");
        let approval = authority
            .approve(OperatorApprovalRequest {
                binding_id: binding.binding_id.clone(),
                thread_uuid: THREAD_UUID.to_string(),
                plan_id: published.plan_id,
                plan_revision: published.revision,
                expected_registry_revision: registry_snapshot.revision,
                expected_plan_digest: compiled.plan_digest,
                repository_path: project_root.clone(),
                base_commit,
                approved_task_ids: vec!["task-1".to_string()],
                approved_write_paths: vec!["src/lib.rs".to_string()],
                max_parallel: 1,
                budgets: ExecutionBudgets::default(),
            })
            .expect("record approval");

        Self {
            _tmp: tmp,
            root,
            project_root,
            pkg_dir,
            pkg_bin_dir,
            context_bin,
            state_dir,
            registry_dir,
            plans_dir,
            authority_dir,
            sessions_dir,
            contexts_dir,
            supervisor: Some(supervisor),
            binding_id: binding.binding_id,
            approval_id: approval.approval_id,
        }
    }

    fn valid_context_json(&self) -> Value {
        json!({
            "schema_version": 1,
            "project_root": self.project_root,
            "binding_id": self.binding_id,
            "thread_uuid": THREAD_UUID,
            "route": "trusted_local_operator",
            "native_per_call_identity_verified": false,
            "stores": {
                "state_dir": self.state_dir,
                "registry_dir": self.registry_dir,
                "plans_dir": self.plans_dir,
                "authority_dir": self.authority_dir,
                "sessions_dir": self.sessions_dir
            },
            "cli_package": self.pkg_dir,
            "desktop_package": "/nix/store/00000000000000000000000000000000-threadlane-desktop",
            "xdg_config_home": "/tmp/non-operational-xdg-config",
            "xdg_data_home": "/tmp/non-operational-xdg-data",
            "xdg_state_home": "/tmp/non-operational-xdg-state",
            "legacy_metadata_note": "ignored non-operational metadata"
        })
    }

    fn write_private_context(&self, name: &str, value: &Value) -> PathBuf {
        let bytes = serde_json::to_vec_pretty(value).expect("serialize context JSON");
        self.write_private_context_bytes(name, &bytes, 0o600)
    }

    fn write_private_context_bytes(&self, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
        let path = self.contexts_dir.join(name);
        fs::write(&path, bytes).expect("write context file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("chmod context file");
        path
    }

    async fn shutdown(&mut self) {
        if let Some(sup) = self.supervisor.take() {
            sup.shutdown().await;
        }
    }
}

fn init_disposable_repo(repo: &Path) -> String {
    fs::create_dir_all(repo.join("src")).expect("create repo/src");
    fs::write(repo.join("src/lib.rs"), "pub fn value() -> u32 { 42 }\n").expect("write src/lib.rs");

    for args in [
        &["init", "--quiet"][..],
        &["config", "user.email", "context-test@threadlane.local"][..],
        &["config", "user.name", "Threadlane Context Test"][..],
        &["add", "."][..],
        &["commit", "-m", "initial fixture commit", "--quiet"][..],
    ] {
        run_sanitized_git(repo, args, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("run git setup");
    }

    resolve_exact_commit_sha(repo, "HEAD", DEFAULT_GIT_COMMAND_TIMEOUT_MS)
        .expect("resolve HEAD commit")
}

fn copy_executable_atomic(src: &Path, dst_dir: &Path, name: &str) -> PathBuf {
    let bytes = fs::read(src).expect("read compiled binary");
    let dst = dst_dir.join(name);
    let staging = dst_dir.join(format!(".{name}.tmp"));
    let _ = fs::remove_file(&staging);
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o755)
        .open(&staging)
        .expect("open staged executable");
    file.write_all(&bytes).expect("write staged executable");
    file.sync_all().expect("sync staged executable");
    drop(file);
    fs::rename(&staging, &dst).expect("publish staged executable");
    dst
}

fn write_executable_script(dir: &Path, name: &str, body: &str, mode: u32) -> PathBuf {
    let dst = dir.join(name);
    let staging = dir.join(format!(".{name}.tmp"));
    let _ = fs::remove_file(&staging);
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(mode)
        .open(&staging)
        .expect("open staged script");
    file.write_all(body.as_bytes()).expect("write script");
    file.sync_all().expect("sync script");
    drop(file);
    fs::rename(&staging, &dst).expect("publish script");
    dst
}

async fn run_bin(bin: &Path, args: &[&str]) -> Output {
    Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("execute CLI binary")
}

async fn run_bin_os(bin: &Path, args: &[OsString]) -> Output {
    Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("execute CLI binary with OsString args")
}

fn assert_error_code(output: &Output, expected_code: &str) -> Value {
    assert!(
        !output.status.success(),
        "expected non-zero exit status, got {:?}; stdout={}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.is_empty(),
        "expected empty stdout on error, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let err_json: Value = serde_json::from_slice(&output.stderr).unwrap_or_else(|err| {
        panic!(
            "stderr was not valid JSON ({err}): {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(err_json["code"], expected_code, "stderr JSON: {err_json}");
    let stderr_str = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr_str.contains(SECRET_SENTINEL),
        "stderr leaked secret sentinel"
    );
    err_json
}

#[tokio::test]
async fn outer_help_and_version_succeed_without_context_or_runtime_and_invalid_syntax_is_sanitized()
{
    let bin = Path::new(env!("CARGO_BIN_EXE_threadlane-context"));

    for help_flag in ["--help", "-h"] {
        let out = run_bin(bin, &[help_flag]).await;
        assert!(out.status.success(), "{help_flag} must exit 0");
        assert!(out.stderr.is_empty(), "{help_flag} must not write stderr");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("threadlane-context"));
        assert!(stdout.contains("--context <ABS_PRIVATE_JSON>"));
    }

    for ver_flag in ["--version", "-V"] {
        let out = run_bin(bin, &[ver_flag]).await;
        assert!(out.status.success(), "{ver_flag} must exit 0");
        assert!(out.stderr.is_empty(), "{ver_flag} must not write stderr");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.starts_with("threadlane-context "));
    }

    // Missing arguments or missing --context.
    let no_args = run_bin(bin, &[]).await;
    assert_error_code(&no_args, "invalid_input");

    let missing_ctx = run_bin(bin, &["check"]).await;
    assert_error_code(&missing_ctx, "invalid_input");

    // Unsupported command families (approve, host, operator) must be rejected without pass-through.
    for family in ["approve", "host", "operator", "unknown-family"] {
        let out = run_bin(
            bin,
            &["--context", "/tmp/nonexistent.json", family, "status"],
        )
        .await;
        assert_error_code(&out, "invalid_input");
    }

    // Extra arguments after `check` or missing arguments after `workflow`/`session`.
    let extra_check = run_bin(
        bin,
        &[
            "--context",
            "/tmp/nonexistent.json",
            "check",
            SECRET_SENTINEL,
        ],
    )
    .await;
    assert_error_code(&extra_check, "invalid_input");

    let empty_workflow = run_bin(bin, &["--context", "/tmp/nonexistent.json", "workflow"]).await;
    assert_error_code(&empty_workflow, "invalid_input");

    let empty_session = run_bin(bin, &["--context", "/tmp/nonexistent.json", "session"]).await;
    assert_error_code(&empty_session, "invalid_input");

    // Non-UTF8 CLI arguments are rejected safely without panic.
    let non_utf8 = run_bin_os(
        bin,
        &[
            OsString::from("--context"),
            OsString::from("/tmp/nonexistent.json"),
            OsString::from("workflow"),
            OsString::from_vec(vec![0xff, 0xfe, 0xfd]),
        ],
    )
    .await;
    assert_error_code(&non_utf8, "invalid_input");
}

#[tokio::test]
async fn existing_private_context_check_workflow_catalog_and_session_missing_host_end_to_end() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path = fixture.write_private_context("context-v1.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().expect("utf8 context path");

    // 1. `check` validates schema, 0755 project_root, 0700 stores, cli_package pin, and live binding.
    let check_out = run_bin(&fixture.context_bin, &["--context", ctx_str, "check"]).await;
    assert!(
        check_out.status.success(),
        "check failed: {}",
        String::from_utf8_lossy(&check_out.stderr)
    );
    assert!(check_out.stderr.is_empty());
    let check_env: Value = serde_json::from_slice(&check_out.stdout).expect("parse check JSON");
    assert_eq!(check_env["untrusted_data"], true);
    assert_eq!(check_env["route"], "trusted_local_operator");
    assert_eq!(check_env["native_per_call_identity_verified"], false);
    assert_eq!(check_env["binding_id"], fixture.binding_id);
    assert_eq!(check_env["data"]["schema_version"], 1);
    assert_eq!(check_env["data"]["binding_id"], fixture.binding_id);
    assert_eq!(check_env["data"]["thread_uuid"], THREAD_UUID);
    assert_eq!(check_env["data"]["route"], "trusted_local_operator");
    assert_eq!(
        check_env["data"]["native_per_call_identity_verified"],
        false
    );
    assert_eq!(check_env["data"]["live_binding_verified"], true);
    assert_eq!(check_env["data"]["human_approval_verified"], false);
    assert_eq!(check_env["data"]["cli_package_pinned"], true);

    // Project root permissions must remain 0755 (not required 0700 and never mutated).
    let project_mode = fs::symlink_metadata(&fixture.project_root)
        .expect("stat project_root")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(project_mode, 0o755);

    // 2. `workflow catalog` invokes sibling `threadlane-workflow` with context-bound flags.
    let wf_out = run_bin(
        &fixture.context_bin,
        &[
            &format!("--context={ctx_str}"),
            "workflow",
            "catalog",
            "--limit",
            "5",
        ],
    )
    .await;
    assert!(
        wf_out.status.success(),
        "workflow catalog failed: {}",
        String::from_utf8_lossy(&wf_out.stderr)
    );
    let wf_env: Value = serde_json::from_slice(&wf_out.stdout).expect("parse workflow JSON");
    assert_eq!(wf_env["untrusted_data"], true);
    assert_eq!(wf_env["route"], "trusted_local_operator");
    assert_eq!(wf_env["native_per_call_identity_verified"], false);
    assert_eq!(wf_env["binding_id"], fixture.binding_id);
    let profiles = wf_env["data"]["profiles"]
        .as_array()
        .expect("profiles array");
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0]["profile_id"], PROFILE_ID);

    // 3. `session session --approval-id <id>` invokes sibling `threadlane-session`; with no running
    //    host, it fails closed with `host_not_ready` without starting a host.
    let sess_out = run_bin(
        &fixture.context_bin,
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
        ],
    )
    .await;
    assert_error_code(&sess_out, "host_not_ready");

    let sessions_entries: Vec<_> = fs::read_dir(&fixture.sessions_dir)
        .expect("read sessions_dir")
        .collect();
    assert!(
        sessions_entries.is_empty(),
        "session read must never create session dirs or auto-start a host"
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn foreign_and_offline_binding_refusals_across_check_workflow_and_session() {
    let mut fixture = ContextFixture::start().await;

    // Context with unknown binding_id fails with binding_not_found.
    let mut unknown_binding_json = fixture.valid_context_json();
    unknown_binding_json["binding_id"] = json!("bnd_missing_999");
    let unknown_ctx = fixture.write_private_context("unknown-binding.json", &unknown_binding_json);
    let unknown_str = unknown_ctx.to_str().unwrap();

    for args in [
        &["--context", unknown_str, "check"][..],
        &["--context", unknown_str, "workflow", "catalog"][..],
    ] {
        let out = run_bin(&fixture.context_bin, args).await;
        assert_error_code(&out, "binding_not_found");
    }

    // Context with foreign thread_uuid fails with thread_uuid_mismatch.
    let mut foreign_thread_json = fixture.valid_context_json();
    foreign_thread_json["thread_uuid"] = json!(OTHER_THREAD_UUID);
    let foreign_ctx = fixture.write_private_context("foreign-thread.json", &foreign_thread_json);
    let foreign_str = foreign_ctx.to_str().unwrap();

    for args in [
        &["--context", foreign_str, "check"][..],
        &["--context", foreign_str, "workflow", "catalog"][..],
        &[
            "--context",
            foreign_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
        ][..],
    ] {
        let out = run_bin(&fixture.context_bin, args).await;
        assert_error_code(&out, "thread_uuid_mismatch");
    }

    // Offline supervisor fails with transport_unavailable.
    let valid_ctx =
        fixture.write_private_context("valid-before-offline.json", &fixture.valid_context_json());
    let valid_str = valid_ctx.to_str().unwrap();
    fixture.shutdown().await;

    for args in [
        &["--context", valid_str, "check"][..],
        &["--context", valid_str, "workflow", "catalog"][..],
        &[
            "--context",
            valid_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
        ][..],
    ] {
        let out = run_bin(&fixture.context_bin, args).await;
        assert_error_code(&out, "transport_unavailable");
    }
}

#[tokio::test]
async fn protected_flag_overrides_and_repeated_context_fail_with_no_side_effects() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path =
        fixture.write_private_context("context-protected.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().unwrap();

    // Replace sibling binaries with marker scripts that record if they are ever spawned.
    let marker_file = fixture.root.join("child-spawned.marker");
    let marker_script = format!(
        "#!/usr/bin/env sh\nprintf 'spawned' > \"{}\"\nexit 0\n",
        marker_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &marker_script,
        0o755,
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-session",
        &marker_script,
        0o755,
    );

    let override_cases: &[&[&str]] = &[
        &["--context", ctx_str, "--context", ctx_str, "check"],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--state-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--state-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--registry-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--registry-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--plans-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--plans-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
            "--authority-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
            "--authority-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
            "--sessions-dir",
            "/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "session",
            "session",
            "--approval-id",
            &fixture.approval_id,
            "--sessions-dir=/tmp/other",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--binding",
            "bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--binding=bnd_override",
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--thread",
            OTHER_THREAD_UUID,
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            &format!("--thread={OTHER_THREAD_UUID}"),
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            "--context",
            ctx_str,
        ],
        &[
            "--context",
            ctx_str,
            "workflow",
            "catalog",
            &format!("--context={ctx_str}"),
        ],
    ];

    for case in override_cases {
        let out = run_bin(&fixture.context_bin, case).await;
        assert_error_code(&out, "invalid_input");
        assert!(
            !marker_file.exists(),
            "protected flag override must never spawn child executable: {case:?}"
        );
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn bad_schema_route_identity_duplicate_fields_store_guards_and_package_pin_refusals() {
    let mut fixture = ContextFixture::start().await;

    // 1. Bad schema_version (2 instead of 1).
    let mut bad_schema = fixture.valid_context_json();
    bad_schema["schema_version"] = json!(2);
    let p = fixture.write_private_context("bad-schema.json", &bad_schema);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 2. Bad route.
    let mut bad_route = fixture.valid_context_json();
    bad_route["route"] = json!("mcp");
    let p = fixture.write_private_context("bad-route.json", &bad_route);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 3. Bad native_per_call_identity_verified (true instead of false).
    let mut bad_id = fixture.valid_context_json();
    bad_id["native_per_call_identity_verified"] = json!(true);
    let p = fixture.write_private_context("bad-identity.json", &bad_id);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 4. Duplicate operational field in raw JSON object.
    let dup_top_raw = format!(
        r#"{{
            "schema_version": 1,
            "project_root": "{}",
            "binding_id": "{}",
            "binding_id": "bnd_duplicate_override",
            "thread_uuid": "{}",
            "route": "trusted_local_operator",
            "native_per_call_identity_verified": false,
            "stores": {{
                "state_dir": "{}",
                "registry_dir": "{}",
                "plans_dir": "{}",
                "authority_dir": "{}",
                "sessions_dir": "{}"
            }}
        }}"#,
        fixture.project_root.display(),
        fixture.binding_id,
        THREAD_UUID,
        fixture.state_dir.display(),
        fixture.registry_dir.display(),
        fixture.plans_dir.display(),
        fixture.authority_dir.display(),
        fixture.sessions_dir.display()
    );
    let dup_top_path =
        fixture.write_private_context_bytes("dup-top.json", dup_top_raw.as_bytes(), 0o600);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", dup_top_path.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 5. Duplicate field inside `stores`.
    let dup_store_raw = format!(
        r#"{{
            "schema_version": 1,
            "project_root": "{}",
            "binding_id": "{}",
            "thread_uuid": "{}",
            "route": "trusted_local_operator",
            "native_per_call_identity_verified": false,
            "stores": {{
                "state_dir": "{}",
                "state_dir": "{}",
                "registry_dir": "{}",
                "plans_dir": "{}",
                "authority_dir": "{}",
                "sessions_dir": "{}"
            }}
        }}"#,
        fixture.project_root.display(),
        fixture.binding_id,
        THREAD_UUID,
        fixture.state_dir.display(),
        fixture.state_dir.display(),
        fixture.registry_dir.display(),
        fixture.plans_dir.display(),
        fixture.authority_dir.display(),
        fixture.sessions_dir.display()
    );
    let dup_store_path =
        fixture.write_private_context_bytes("dup-store.json", dup_store_raw.as_bytes(), 0o600);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", dup_store_path.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 6. Top-level store override field is rejected.
    let mut top_override = fixture.valid_context_json();
    top_override["state_dir"] = json!(fixture.state_dir);
    let p = fixture.write_private_context("top-override.json", &top_override);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // 7. Package pin mismatch: foreign package directory or executable file path in cli_package.
    let other_pkg = fixture.root.join("other-pkg");
    fs::create_dir_all(other_pkg.join("bin")).expect("create other-pkg/bin");
    let mut wrong_pkg = fixture.valid_context_json();
    wrong_pkg["cli_package"] = json!(other_pkg);
    let p = fixture.write_private_context("wrong-pkg.json", &wrong_pkg);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );

    let mut file_pkg = fixture.valid_context_json();
    file_pkg["cli_package"] = json!(fixture.context_bin);
    let p = fixture.write_private_context("file-pkg.json", &file_pkg);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );

    // 8. Missing store directory is rejected without creating it.
    let missing_store = fixture.root.join("missing-sessions-store");
    let mut missing_store_json = fixture.valid_context_json();
    missing_store_json["stores"]["sessions_dir"] = json!(missing_store);
    let p = fixture.write_private_context("missing-store.json", &missing_store_json);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );
    assert!(
        !missing_store.exists(),
        "threadlane-context must never mkdir missing store directories"
    );

    // 9. Non-0700 store directory is rejected without repairing permissions.
    let open_store = fixture.root.join("open-sessions-store");
    fs::create_dir_all(&open_store).expect("create open store");
    fs::set_permissions(&open_store, fs::Permissions::from_mode(0o755))
        .expect("chmod open store 0755");
    let mut open_store_json = fixture.valid_context_json();
    open_store_json["stores"]["sessions_dir"] = json!(open_store);
    let p = fixture.write_private_context("open-store.json", &open_store_json);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );
    let mode_after = fs::symlink_metadata(&open_store)
        .expect("stat open store")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode_after, 0o755,
        "threadlane-context must never chmod/repair store permissions"
    );

    // 10. Overlapping store directories are rejected.
    let mut overlap_json = fixture.valid_context_json();
    overlap_json["stores"]["sessions_dir"] = json!(fixture.authority_dir);
    let p = fixture.write_private_context("overlap-store.json", &overlap_json);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", p.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn symlink_insecure_and_oversized_context_files_fail_closed_and_redact_contents() {
    let mut fixture = ContextFixture::start().await;
    let valid_ctx = fixture.write_private_context("valid.json", &fixture.valid_context_json());

    // Symlink context path is rejected.
    let link_ctx = fixture.contexts_dir.join("symlink-context.json");
    symlink(&valid_ctx, &link_ctx).expect("create symlink context");
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", link_ctx.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );

    // Insecure 0644 context file is rejected.
    let open_ctx = fixture.write_private_context_bytes(
        "open-0644.json",
        &serde_json::to_vec(&fixture.valid_context_json()).unwrap(),
        0o644,
    );
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", open_ctx.to_str().unwrap(), "check"],
        )
        .await,
        "security_violation",
    );

    // Oversized (> 1 MiB) 0600 context file is rejected.
    let oversized_ctx =
        fixture.write_private_context_bytes("oversized.json", &vec![b' '; MAX_IO_BYTES + 1], 0o600);
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", oversized_ctx.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    // Malformed JSON containing a secret sentinel never leaks the sentinel.
    let secret_payload = format!(r#"{{"broken_json": "{SECRET_SENTINEL}""#);
    let secret_ctx = fixture.write_private_context_bytes(
        "secret-malformed.json",
        secret_payload.as_bytes(),
        0o600,
    );
    assert_error_code(
        &run_bin(
            &fixture.context_bin,
            &["--context", secret_ctx.to_str().unwrap(), "check"],
        )
        .await,
        "invalid_input",
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn missing_or_unsafe_sibling_and_child_help_failure_and_signal_outcome_unknown() {
    let mut fixture = ContextFixture::start().await;
    let ctx_path =
        fixture.write_private_context("context-sibling.json", &fixture.valid_context_json());
    let ctx_str = ctx_path.to_str().unwrap();

    // 1. Inner `--help` is forwarded to the real sibling and succeeds with child help output.
    let inner_help = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "--help"],
    )
    .await;
    assert!(inner_help.status.success());
    let help_stdout = String::from_utf8_lossy(&inner_help.stdout);
    assert!(help_stdout.contains("threadlane-workflow"));

    // 2. Missing sibling executable fails with `not_configured`.
    let wf_sibling = fixture.pkg_bin_dir.join("threadlane-workflow");
    fs::remove_file(&wf_sibling).expect("remove threadlane-workflow sibling");
    let missing_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_error_code(&missing_out, "not_configured");

    // 3. Symlink sibling executable is rejected with `security_violation`.
    symlink(
        Path::new(env!("CARGO_BIN_EXE_threadlane-workflow")),
        &wf_sibling,
    )
    .expect("symlink threadlane-workflow");
    let symlink_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_error_code(&symlink_out, "security_violation");
    fs::remove_file(&wf_sibling).expect("remove symlink sibling");

    // 4. Directory in place of sibling executable is rejected with `security_violation`.
    fs::create_dir(&wf_sibling).expect("create directory sibling");
    let dir_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_error_code(&dir_out, "security_violation");
    fs::remove_dir(&wf_sibling).expect("remove directory sibling");

    // 5. Non-executable (0600) regular file sibling is rejected with `security_violation`.
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        "#!/usr/bin/env sh\nexit 0\n",
        0o600,
    );
    let non_exec_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_error_code(&non_exec_out, "security_violation");
    fs::remove_file(&wf_sibling).expect("remove non-exec sibling");

    // 6. Child non-zero exit code and stderr output are preserved transparently without replay.
    let count_file = fixture.root.join("child-count.txt");
    let failing_child_script = format!(
        "#!/usr/bin/env sh\nprintf 'x' >> \"{}\"\nprintf '{{\"code\":\"custom_child_refusal\",\"message\":\"child refused request\"}}\\n' >&2\nexit 19\n",
        count_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &failing_child_script,
        0o755,
    );
    let child_fail_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_eq!(child_fail_out.status.code(), Some(19));
    let child_err = assert_error_code(&child_fail_out, "custom_child_refusal");
    assert_eq!(child_err["message"], "child refused request");
    assert_eq!(
        fs::read_to_string(&count_file).expect("read count").len(),
        1,
        "failing child must be invoked exactly once without replay"
    );
    fs::remove_file(&wf_sibling).expect("remove failing sibling");

    // 7. Signal termination after child spawn emits `outcome_unknown` and never replays.
    let sig_count_file = fixture.root.join("sig-child-count.txt");
    let signal_child_script = format!(
        "#!/usr/bin/env sh\nprintf 'x' >> \"{}\"\nkill -TERM $$\n",
        sig_count_file.display()
    );
    write_executable_script(
        &fixture.pkg_bin_dir,
        "threadlane-workflow",
        &signal_child_script,
        0o755,
    );
    let sig_out = run_bin(
        &fixture.context_bin,
        &["--context", ctx_str, "workflow", "catalog"],
    )
    .await;
    assert_error_code(&sig_out, "outcome_unknown");
    assert_eq!(
        fs::read_to_string(&sig_count_file)
            .expect("read sig count")
            .len(),
        1,
        "signal-terminated child must be invoked exactly once without replay"
    );

    fixture.shutdown().await;
}
